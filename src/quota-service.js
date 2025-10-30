import { applyVerifyHeaders } from './utils.js';

const DEFAULT_BLOCK_TYPES = ['file_quota', 'global_quota'];
const QUOTA_TRANSACTIONS_TABLE = 'quota_transactions';
const QUOTA_BLOCKS_TABLE = 'quota_blocks';

const ensuredD1Bindings = new WeakSet();
const ensuredD1Rest = new Set();

const SQLITE_NOW_MS = "CAST(strftime('%s','now') AS INTEGER) * 1000";

const LOG_PREFIX = '[QuotaService]';

const isNonEmptyArray = (value) => Array.isArray(value) && value.length > 0;

const sanitizePositiveInteger = (value) => {
  const number = Number.parseInt(value, 10);
  if (!Number.isFinite(number) || number < 0) {
    return 0;
  }
  return number;
};

const sanitizeBigInt = (value) => {
  if (value === null || value === undefined) {
    return 0;
  }
  if (typeof value === 'bigint') {
    return Number(value);
  }
  const number = Number.parseInt(value, 10);
  if (!Number.isFinite(number)) {
    return 0;
  }
  return number;
};

const buildReason = (scope, projected, maxBytes, windowSeconds) => (
  `Quota exceeded (${scope}): ${projected} > ${maxBytes} bytes within ${windowSeconds} seconds`
);

const normalizePostgrestUrl = (url) => {
  if (!url) {
    return '';
  }
  return url.endsWith('/') ? url.slice(0, -1) : url;
};

const callPostgrestRpc = async (dbConfig, functionName, body) => {
  const baseUrl = normalizePostgrestUrl(dbConfig?.postgrestUrl);
  if (!baseUrl) {
    throw new Error(`${LOG_PREFIX} PostgREST URL missing for ${functionName}`);
  }

  const targetUrl = `${baseUrl}/rpc/${functionName}`;
  const headers = { 'Content-Type': 'application/json' };
  applyVerifyHeaders(headers, dbConfig?.verifyHeader, dbConfig?.verifySecret);

  const response = await fetch(targetUrl, {
    method: 'POST',
    headers,
    body: JSON.stringify(body ?? {}),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${LOG_PREFIX} PostgREST RPC ${functionName} failed (${response.status}): ${text}`);
  }

  try {
    return await response.json();
  } catch (_error) {
    return [];
  }
};

const executeD1RestStatement = async (dbConfig, sql, params = []) => {
  const { accountId, databaseId, apiToken } = dbConfig || {};

  if (!accountId || !databaseId || !apiToken) {
    throw new Error(`${LOG_PREFIX} D1 REST configuration incomplete`);
  }

  const endpoint = `https://api.cloudflare.com/client/v4/accounts/${accountId}/d1/database/${databaseId}/query`;

  const payload = params.length > 0 ? { sql, params } : { sql };

  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${apiToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${LOG_PREFIX} D1 REST request failed (${response.status}): ${text}`);
  }

  const result = await response.json();
  if (!result.success) {
    const errors = Array.isArray(result.errors) ? result.errors.map((err) => err.message || String(err)).join(', ') : 'Unknown error';
    throw new Error(`${LOG_PREFIX} D1 REST query failed: ${errors}`);
  }

  return result.result?.[0] ?? null;
};

const getD1RestRows = (statementResult) => {
  if (!statementResult || !Array.isArray(statementResult.results)) {
    return [];
  }
  return statementResult.results;
};

const getD1Binding = (dbConfig) => {
  const bindingName = dbConfig?.databaseBinding;
  const env = dbConfig?.env;

  if (!bindingName || !env) {
    throw new Error(`${LOG_PREFIX} D1 binding configuration incomplete`);
  }

  const db = env[bindingName];
  if (!db) {
    throw new Error(`${LOG_PREFIX} D1 binding "${bindingName}" not found`);
  }
  return db;
};

const ensureD1QuotaTables = async (dbConfig) => {
  const db = getD1Binding(dbConfig);
  if (ensuredD1Bindings.has(db)) {
    return db;
  }

  const statements = [
    db.prepare(`
      CREATE TABLE IF NOT EXISTS ${QUOTA_TRANSACTIONS_TABLE} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id TEXT NOT NULL UNIQUE,
        ip_range_hash TEXT NOT NULL,
        filepath_hash TEXT NOT NULL,
        transaction_type TEXT NOT NULL CHECK (transaction_type IN ('debit', 'settlement')),
        estimated_bytes INTEGER,
        actual_bytes INTEGER,
        amount INTEGER NOT NULL,
        window_start_time INTEGER NOT NULL,
        created_at INTEGER DEFAULT (${SQLITE_NOW_MS}),
        CHECK ((transaction_type = 'debit' AND amount >= 0) OR (transaction_type = 'settlement' AND amount <= 0)),
        CHECK (estimated_bytes IS NULL OR estimated_bytes >= 0),
        CHECK (actual_bytes IS NULL OR actual_bytes >= 0),
        CHECK (window_start_time >= 0)
      )
    `),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_quota_transactions_ip_window ON ${QUOTA_TRANSACTIONS_TABLE} (ip_range_hash, window_start_time)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_quota_transactions_file_ip ON ${QUOTA_TRANSACTIONS_TABLE} (filepath_hash, ip_range_hash)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_quota_transactions_created_at ON ${QUOTA_TRANSACTIONS_TABLE} (created_at)`),
    db.prepare(`
      CREATE TABLE IF NOT EXISTS ${QUOTA_BLOCKS_TABLE} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ip_range_hash TEXT NOT NULL,
        filepath_hash TEXT,
        block_type TEXT NOT NULL CHECK (block_type IN ('file_quota', 'global_quota', 'rate_limit')),
        blocked_until INTEGER NOT NULL,
        reason TEXT,
        created_at INTEGER DEFAULT (${SQLITE_NOW_MS}),
        filepath_hash_key TEXT GENERATED ALWAYS AS (COALESCE(filepath_hash, '')) STORED,
        CHECK (blocked_until >= 0)
      )
    `),
    db.prepare(`CREATE UNIQUE INDEX IF NOT EXISTS idx_quota_blocks_unique ON ${QUOTA_BLOCKS_TABLE} (ip_range_hash, block_type, filepath_hash_key)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_quota_blocks_blocked_until ON ${QUOTA_BLOCKS_TABLE} (blocked_until)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_quota_blocks_created_at ON ${QUOTA_BLOCKS_TABLE} (created_at)`),
  ];

  await db.batch(statements);
  ensuredD1Bindings.add(db);
  return db;
};

const ensureD1RestQuotaTables = async (dbConfig) => {
  const key = `${dbConfig?.accountId || ''}:${dbConfig?.databaseId || ''}`;
  if (ensuredD1Rest.has(key)) {
    return;
  }

  const statements = [
    `CREATE TABLE IF NOT EXISTS ${QUOTA_TRANSACTIONS_TABLE} (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      request_id TEXT NOT NULL UNIQUE,
      ip_range_hash TEXT NOT NULL,
      filepath_hash TEXT NOT NULL,
      transaction_type TEXT NOT NULL CHECK (transaction_type IN ('debit', 'settlement')),
      estimated_bytes INTEGER,
      actual_bytes INTEGER,
      amount INTEGER NOT NULL,
      window_start_time INTEGER NOT NULL,
      created_at INTEGER DEFAULT (${SQLITE_NOW_MS}),
      CHECK ((transaction_type = 'debit' AND amount >= 0) OR (transaction_type = 'settlement' AND amount <= 0)),
      CHECK (estimated_bytes IS NULL OR estimated_bytes >= 0),
      CHECK (actual_bytes IS NULL OR actual_bytes >= 0),
      CHECK (window_start_time >= 0)
    )`,
    `CREATE INDEX IF NOT EXISTS idx_quota_transactions_ip_window ON ${QUOTA_TRANSACTIONS_TABLE} (ip_range_hash, window_start_time)` ,
    `CREATE INDEX IF NOT EXISTS idx_quota_transactions_file_ip ON ${QUOTA_TRANSACTIONS_TABLE} (filepath_hash, ip_range_hash)` ,
    `CREATE INDEX IF NOT EXISTS idx_quota_transactions_created_at ON ${QUOTA_TRANSACTIONS_TABLE} (created_at)` ,
    `CREATE TABLE IF NOT EXISTS ${QUOTA_BLOCKS_TABLE} (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ip_range_hash TEXT NOT NULL,
      filepath_hash TEXT,
      block_type TEXT NOT NULL CHECK (block_type IN ('file_quota', 'global_quota', 'rate_limit')),
      blocked_until INTEGER NOT NULL,
      reason TEXT,
      created_at INTEGER DEFAULT (${SQLITE_NOW_MS}),
      filepath_hash_key TEXT GENERATED ALWAYS AS (COALESCE(filepath_hash, '')) STORED,
      CHECK (blocked_until >= 0)
    )`,
    `CREATE UNIQUE INDEX IF NOT EXISTS idx_quota_blocks_unique ON ${QUOTA_BLOCKS_TABLE} (ip_range_hash, block_type, filepath_hash_key)` ,
    `CREATE INDEX IF NOT EXISTS idx_quota_blocks_blocked_until ON ${QUOTA_BLOCKS_TABLE} (blocked_until)` ,
    `CREATE INDEX IF NOT EXISTS idx_quota_blocks_created_at ON ${QUOTA_BLOCKS_TABLE} (created_at)` ,
  ];

  for (const statement of statements) {
    await executeD1RestStatement(dbConfig, statement);
  }

  ensuredD1Rest.add(key);
};

const sanitizeDeductBytes = (value) => {
  const number = Number.parseInt(value, 10);
  if (!Number.isFinite(number) || number < 0) {
    return 0;
  }
  return number;
};

const computeCutoffMs = (nowSeconds, windowSeconds) => {
  if (!windowSeconds || windowSeconds <= 0) {
    return 0;
  }
  const cutoff = (nowSeconds - windowSeconds) * 1000;
  return cutoff > 0 ? cutoff : 0;
};

const mapBalanceRow = (row = {}) => {
  const fileUsage = sanitizeBigInt(row.file_usage);
  const fileProjected = sanitizeBigInt(row.file_projected);
  const fileLimit = row.file_limit === null || row.file_limit === undefined ? null : sanitizeBigInt(row.file_limit);
  const fileRemaining = row.file_remaining === null || row.file_remaining === undefined ? null : sanitizeBigInt(row.file_remaining);
  const fileInsufficient = Boolean(row.file_insufficient);
  const fileBlockedUntil = row.file_blocked_until === null || row.file_blocked_until === undefined ? null : sanitizeBigInt(row.file_blocked_until);

  const globalUsage = sanitizeBigInt(row.global_usage);
  const globalProjected = sanitizeBigInt(row.global_projected);
  const globalLimit = row.global_limit === null || row.global_limit === undefined ? null : sanitizeBigInt(row.global_limit);
  const globalRemaining = row.global_remaining === null || row.global_remaining === undefined ? null : sanitizeBigInt(row.global_remaining);
  const globalInsufficient = Boolean(row.global_insufficient);
  const globalBlockedUntil = row.global_blocked_until === null || row.global_blocked_until === undefined ? null : sanitizeBigInt(row.global_blocked_until);

  return {
    fileUsage,
    fileProjected,
    fileLimit,
    fileRemaining,
    fileInsufficient,
    fileBlockedUntil,
    globalUsage,
    globalProjected,
    globalLimit,
    globalRemaining,
    globalInsufficient,
    globalBlockedUntil,
    insufficient: fileInsufficient || globalInsufficient,
  };
};

const defaultBalance = () => ({
  fileUsage: 0,
  fileProjected: 0,
  fileLimit: null,
  fileRemaining: null,
  fileInsufficient: false,
  fileBlockedUntil: null,
  globalUsage: 0,
  globalProjected: 0,
  globalLimit: null,
  globalRemaining: null,
  globalInsufficient: false,
  globalBlockedUntil: null,
  insufficient: false,
});

const mapBlockRows = (rows = []) => rows.map((row) => ({
  blockType: row.block_type,
  blockedUntil: sanitizeBigInt(row.blocked_until),
  reason: row.reason ?? null,
})).filter((entry) => Number.isFinite(entry.blockedUntil) && entry.blockedUntil > 0);

export const checkQuotaBlock = async (dbConfig, params = {}) => {
  const {
    ipRangeHash,
    filepathHash = null,
    blockTypes = DEFAULT_BLOCK_TYPES,
    nowSeconds = Math.floor(Date.now() / 1000),
  } = params;

  if (!ipRangeHash) {
    return { blocked: false, blocks: [] };
  }

  if (dbConfig?.dbMode === 'custom-pg-rest') {
    const result = await callPostgrestRpc(dbConfig, 'check_quota_block', {
      p_ip_range_hash: ipRangeHash,
      p_filepath_hash: filepathHash,
      p_now: nowSeconds,
      p_block_types: isNonEmptyArray(blockTypes) ? blockTypes : DEFAULT_BLOCK_TYPES,
    });
    const blocks = mapBlockRows(result);
    return { blocked: blocks.length > 0, blocks };
  }

  if (dbConfig?.dbMode === 'd1') {
    const db = await ensureD1QuotaTables(dbConfig);
    const activeBlockTypes = isNonEmptyArray(blockTypes) ? blockTypes : DEFAULT_BLOCK_TYPES;
    const placeholders = activeBlockTypes.map(() => '?').join(', ');
    const sql = `
      SELECT block_type, blocked_until, reason
        FROM ${QUOTA_BLOCKS_TABLE}
       WHERE ip_range_hash = ?
         AND blocked_until > ?
         AND block_type IN (${placeholders})
         AND (${filepathHash ? '(filepath_hash IS NULL OR filepath_hash = ?)' : 'filepath_hash IS NULL'})
    `;

    const bindings = [ipRangeHash, nowSeconds, ...activeBlockTypes];
    if (filepathHash) {
      bindings.push(filepathHash);
    }

    const statement = db.prepare(sql);
    const rows = await statement.bind(...bindings).all();
    const blocks = mapBlockRows(rows.results || rows);
    return { blocked: blocks.length > 0, blocks };
  }

  if (dbConfig?.dbMode === 'd1-rest') {
    await ensureD1RestQuotaTables(dbConfig);
    const activeBlockTypes = isNonEmptyArray(blockTypes) ? blockTypes : DEFAULT_BLOCK_TYPES;
    const placeholders = activeBlockTypes.map(() => '?').join(', ');
    const sql = `
      SELECT block_type, blocked_until, reason
        FROM ${QUOTA_BLOCKS_TABLE}
       WHERE ip_range_hash = ?
         AND blocked_until > ?
         AND block_type IN (${placeholders})
         AND (${filepathHash ? '(filepath_hash IS NULL OR filepath_hash = ?)' : 'filepath_hash IS NULL'})
    `;

    const paramsList = [ipRangeHash, nowSeconds, ...activeBlockTypes];
    if (filepathHash) {
      paramsList.push(filepathHash);
    }

    const statementResult = await executeD1RestStatement(dbConfig, sql, paramsList);
    const rows = getD1RestRows(statementResult);
    const blocks = mapBlockRows(rows);
    return { blocked: blocks.length > 0, blocks };
  }

  return { blocked: false, blocks: [] };
};

const computeBalanceForD1 = async (dbConfig, params) => {
  const {
    ipRangeHash,
    filepathHash,
    deductBytes,
    nowSeconds,
    fileWindowSeconds,
    fileMaxBytes,
    fileBlockSeconds,
    globalWindowSeconds,
    globalMaxBytes,
    globalBlockSeconds,
  } = params;

  const db = await ensureD1QuotaTables(dbConfig);
  const sanitizedDeduct = sanitizeDeductBytes(deductBytes);
  const windowStartFile = computeCutoffMs(nowSeconds, fileWindowSeconds);
  const windowStartGlobal = computeCutoffMs(nowSeconds, globalWindowSeconds);

  let fileUsage = 0;
  if (fileWindowSeconds && fileWindowSeconds > 0 && fileMaxBytes && fileMaxBytes > 0) {
    const fileResult = await db.prepare(`
      SELECT COALESCE(SUM(amount), 0) AS usage
        FROM ${QUOTA_TRANSACTIONS_TABLE}
       WHERE ip_range_hash = ?
         AND filepath_hash = ?
         AND window_start_time >= ?
    `).bind(ipRangeHash, filepathHash, windowStartFile).first();
    fileUsage = sanitizeBigInt(fileResult?.usage);
  }

  let globalUsage = 0;
  if (globalWindowSeconds && globalWindowSeconds > 0 && globalMaxBytes && globalMaxBytes > 0) {
    const globalResult = await db.prepare(`
      SELECT COALESCE(SUM(amount), 0) AS usage
        FROM ${QUOTA_TRANSACTIONS_TABLE}
       WHERE ip_range_hash = ?
         AND window_start_time >= ?
    `).bind(ipRangeHash, windowStartGlobal).first();
    globalUsage = sanitizeBigInt(globalResult?.usage);
  }

  const response = defaultBalance();
  response.fileUsage = fileUsage;
  response.globalUsage = globalUsage;
  response.fileProjected = fileUsage + sanitizedDeduct;
  response.globalProjected = globalUsage + sanitizedDeduct;

  if (fileWindowSeconds && fileWindowSeconds > 0 && fileMaxBytes && fileMaxBytes > 0) {
    response.fileLimit = fileMaxBytes;
    response.fileRemaining = Math.max(fileMaxBytes - fileUsage, 0);
    response.fileInsufficient = response.fileProjected > fileMaxBytes;
    if (response.fileInsufficient && fileBlockSeconds && fileBlockSeconds > 0) {
      const blockUntil = nowSeconds + fileBlockSeconds;
      const reason = buildReason('file', response.fileProjected, fileMaxBytes, fileWindowSeconds);
      await db.prepare(`
        INSERT INTO ${QUOTA_BLOCKS_TABLE} (ip_range_hash, filepath_hash, block_type, blocked_until, reason)
        VALUES (?, ?, 'file_quota', ?, ?)
        ON CONFLICT(ip_range_hash, block_type, filepath_hash_key) DO UPDATE SET
          blocked_until = MAX(${QUOTA_BLOCKS_TABLE}.blocked_until, excluded.blocked_until),
          reason = excluded.reason
      `).bind(ipRangeHash, filepathHash, blockUntil, reason).run();

      const blockRow = await db.prepare(`
        SELECT blocked_until
          FROM ${QUOTA_BLOCKS_TABLE}
         WHERE ip_range_hash = ?
           AND block_type = 'file_quota'
           AND filepath_hash_key = COALESCE(?, '')
      `).bind(ipRangeHash, filepathHash ?? '').first();
      response.fileBlockedUntil = sanitizeBigInt(blockRow?.blocked_until);
    }
  }

  if (globalWindowSeconds && globalWindowSeconds > 0 && globalMaxBytes && globalMaxBytes > 0) {
    response.globalLimit = globalMaxBytes;
    response.globalRemaining = Math.max(globalMaxBytes - globalUsage, 0);
    response.globalInsufficient = response.globalProjected > globalMaxBytes;
    if (response.globalInsufficient && globalBlockSeconds && globalBlockSeconds > 0) {
      const blockUntil = nowSeconds + globalBlockSeconds;
      const reason = buildReason('global', response.globalProjected, globalMaxBytes, globalWindowSeconds);
      await db.prepare(`
        INSERT INTO ${QUOTA_BLOCKS_TABLE} (ip_range_hash, filepath_hash, block_type, blocked_until, reason)
        VALUES (?, NULL, 'global_quota', ?, ?)
        ON CONFLICT(ip_range_hash, block_type, filepath_hash_key) DO UPDATE SET
          blocked_until = MAX(${QUOTA_BLOCKS_TABLE}.blocked_until, excluded.blocked_until),
          reason = excluded.reason
      `).bind(ipRangeHash, blockUntil, reason).run();

      const blockRow = await db.prepare(`
        SELECT blocked_until
          FROM ${QUOTA_BLOCKS_TABLE}
         WHERE ip_range_hash = ?
           AND block_type = 'global_quota'
           AND filepath_hash IS NULL
      `).bind(ipRangeHash).first();
      response.globalBlockedUntil = sanitizeBigInt(blockRow?.blocked_until);
    }
  }

  response.insufficient = response.fileInsufficient || response.globalInsufficient;
  return response;
};

const computeBalanceForD1Rest = async (dbConfig, params) => {
  const {
    ipRangeHash,
    filepathHash,
    deductBytes,
    nowSeconds,
    fileWindowSeconds,
    fileMaxBytes,
    fileBlockSeconds,
    globalWindowSeconds,
    globalMaxBytes,
    globalBlockSeconds,
  } = params;

  await ensureD1RestQuotaTables(dbConfig);

  const sanitizedDeduct = sanitizeDeductBytes(deductBytes);
  const windowStartFile = computeCutoffMs(nowSeconds, fileWindowSeconds);
  const windowStartGlobal = computeCutoffMs(nowSeconds, globalWindowSeconds);

  const response = defaultBalance();
  const usageSqlFile = `SELECT COALESCE(SUM(amount), 0) AS usage FROM ${QUOTA_TRANSACTIONS_TABLE} WHERE ip_range_hash = ? AND filepath_hash = ? AND window_start_time >= ?`;
  const usageSqlGlobal = `SELECT COALESCE(SUM(amount), 0) AS usage FROM ${QUOTA_TRANSACTIONS_TABLE} WHERE ip_range_hash = ? AND window_start_time >= ?`;

  if (fileWindowSeconds && fileWindowSeconds > 0 && fileMaxBytes && fileMaxBytes > 0) {
    const statement = await executeD1RestStatement(dbConfig, usageSqlFile, [ipRangeHash, filepathHash, windowStartFile]);
    const usageRows = getD1RestRows(statement);
    const usage = sanitizeBigInt(usageRows?.[0]?.usage);
    response.fileUsage = usage;
    response.fileProjected = usage + sanitizedDeduct;
    response.fileLimit = fileMaxBytes;
    response.fileRemaining = Math.max(fileMaxBytes - usage, 0);
    response.fileInsufficient = response.fileProjected > fileMaxBytes;

    if (response.fileInsufficient && fileBlockSeconds && fileBlockSeconds > 0) {
      const blockUntil = nowSeconds + fileBlockSeconds;
      const reason = buildReason('file', response.fileProjected, fileMaxBytes, fileWindowSeconds);
      const insertSql = `
        INSERT INTO ${QUOTA_BLOCKS_TABLE} (ip_range_hash, filepath_hash, block_type, blocked_until, reason)
        VALUES (?, ?, 'file_quota', ?, ?)
        ON CONFLICT(ip_range_hash, block_type, filepath_hash_key) DO UPDATE SET
          blocked_until = MAX(${QUOTA_BLOCKS_TABLE}.blocked_until, excluded.blocked_until),
          reason = excluded.reason
      `;
      await executeD1RestStatement(dbConfig, insertSql, [ipRangeHash, filepathHash, blockUntil, reason]);

      const selectSql = `
        SELECT blocked_until FROM ${QUOTA_BLOCKS_TABLE}
         WHERE ip_range_hash = ? AND block_type = 'file_quota' AND filepath_hash_key = COALESCE(?, '')
      `;
      const blockRowResult = await executeD1RestStatement(dbConfig, selectSql, [ipRangeHash, filepathHash ?? '']);
      const blockRows = getD1RestRows(blockRowResult);
      response.fileBlockedUntil = sanitizeBigInt(blockRows?.[0]?.blocked_until);
    }
  }

  if (globalWindowSeconds && globalWindowSeconds > 0 && globalMaxBytes && globalMaxBytes > 0) {
    const statement = await executeD1RestStatement(dbConfig, usageSqlGlobal, [ipRangeHash, windowStartGlobal]);
    const usageRows = getD1RestRows(statement);
    const usage = sanitizeBigInt(usageRows?.[0]?.usage);
    response.globalUsage = usage;
    response.globalProjected = usage + sanitizedDeduct;
    response.globalLimit = globalMaxBytes;
    response.globalRemaining = Math.max(globalMaxBytes - usage, 0);
    response.globalInsufficient = response.globalProjected > globalMaxBytes;

    if (response.globalInsufficient && globalBlockSeconds && globalBlockSeconds > 0) {
      const blockUntil = nowSeconds + globalBlockSeconds;
      const reason = buildReason('global', response.globalProjected, globalMaxBytes, globalWindowSeconds);
      const insertSql = `
        INSERT INTO ${QUOTA_BLOCKS_TABLE} (ip_range_hash, filepath_hash, block_type, blocked_until, reason)
        VALUES (?, NULL, 'global_quota', ?, ?)
        ON CONFLICT(ip_range_hash, block_type, filepath_hash_key) DO UPDATE SET
          blocked_until = MAX(${QUOTA_BLOCKS_TABLE}.blocked_until, excluded.blocked_until),
          reason = excluded.reason
      `;
      await executeD1RestStatement(dbConfig, insertSql, [ipRangeHash, blockUntil, reason]);

      const selectSql = `
        SELECT blocked_until FROM ${QUOTA_BLOCKS_TABLE}
         WHERE ip_range_hash = ? AND block_type = 'global_quota' AND filepath_hash IS NULL
      `;
      const blockRowResult = await executeD1RestStatement(dbConfig, selectSql, [ipRangeHash]);
      const blockRows = getD1RestRows(blockRowResult);
      response.globalBlockedUntil = sanitizeBigInt(blockRows?.[0]?.blocked_until);
    }
  }

  response.insufficient = response.fileInsufficient || response.globalInsufficient;
  return response;
};

export const checkQuotaBalance = async (dbConfig, params = {}) => {
  const {
    ipRangeHash,
    filepathHash,
    deductBytes = 0,
    nowSeconds = Math.floor(Date.now() / 1000),
    fileWindowSeconds,
    fileMaxBytes,
    fileBlockSeconds,
    globalWindowSeconds,
    globalMaxBytes,
    globalBlockSeconds,
  } = params;

  if (!ipRangeHash || !filepathHash) {
    return defaultBalance();
  }

  if (dbConfig?.dbMode === 'custom-pg-rest') {
    const result = await callPostgrestRpc(dbConfig, 'check_quota_balance', {
      p_ip_range_hash: ipRangeHash,
      p_filepath_hash: filepathHash,
      p_deduct_bytes: deductBytes,
      p_now: nowSeconds,
      p_file_window_seconds: fileWindowSeconds,
      p_file_max_bytes: fileMaxBytes,
      p_file_block_seconds: fileBlockSeconds,
      p_global_window_seconds: globalWindowSeconds,
      p_global_max_bytes: globalMaxBytes,
      p_global_block_seconds: globalBlockSeconds,
    });

    const row = Array.isArray(result) && result.length > 0 ? result[0] : {};
    return mapBalanceRow(row);
  }

  if (dbConfig?.dbMode === 'd1') {
    return computeBalanceForD1(dbConfig, {
      ipRangeHash,
      filepathHash,
      deductBytes,
      nowSeconds,
      fileWindowSeconds,
      fileMaxBytes,
      fileBlockSeconds,
      globalWindowSeconds,
      globalMaxBytes,
      globalBlockSeconds,
    });
  }

  if (dbConfig?.dbMode === 'd1-rest') {
    return computeBalanceForD1Rest(dbConfig, {
      ipRangeHash,
      filepathHash,
      deductBytes,
      nowSeconds,
      fileWindowSeconds,
      fileMaxBytes,
      fileBlockSeconds,
      globalWindowSeconds,
      globalMaxBytes,
      globalBlockSeconds,
    });
  }

  return defaultBalance();
};

const appendRequestSuffix = (requestId, suffix) => `${requestId}::${suffix}`;

export const insertQuotaDebit = async (dbConfig, params = {}) => {
  const {
    requestId,
    ipRangeHash,
    filepathHash,
    estimatedBytes = 0,
    amount = 0,
    windowStartTime = Date.now(),
  } = params;

  if (!requestId || !ipRangeHash || !filepathHash) {
    return { success: false };
  }

  const sanitizedAmount = sanitizeDeductBytes(amount);
  const sanitizedEstimated = sanitizeDeductBytes(estimatedBytes);
  const sanitizedWindow = sanitizeDeductBytes(windowStartTime);
  const fullRequestId = appendRequestSuffix(requestId, 'debit');

  if (dbConfig?.dbMode === 'custom-pg-rest') {
    const result = await callPostgrestRpc(dbConfig, 'insert_quota_debit', {
      p_request_id: requestId,
      p_ip_range_hash: ipRangeHash,
      p_filepath_hash: filepathHash,
      p_estimated_bytes: sanitizedEstimated,
      p_amount: sanitizedAmount,
      p_window_start_time: sanitizedWindow,
    });
    const row = Array.isArray(result) && result.length > 0 ? result[0] : {};
    return { success: Boolean(row.success) };
  }

  if (dbConfig?.dbMode === 'd1') {
    const db = await ensureD1QuotaTables(dbConfig);
    const statement = db.prepare(`
      INSERT INTO ${QUOTA_TRANSACTIONS_TABLE} (request_id, ip_range_hash, filepath_hash, transaction_type, estimated_bytes, actual_bytes, amount, window_start_time)
      VALUES (?, ?, ?, 'debit', ?, NULL, ?, ?)
      ON CONFLICT(request_id) DO NOTHING
    `);
    const result = await statement.bind(fullRequestId, ipRangeHash, filepathHash, sanitizedEstimated, sanitizedAmount, sanitizedWindow).run();
    return { success: Boolean(result?.meta?.changes) };
  }

  if (dbConfig?.dbMode === 'd1-rest') {
    await ensureD1RestQuotaTables(dbConfig);
    const sql = `
      INSERT INTO ${QUOTA_TRANSACTIONS_TABLE} (request_id, ip_range_hash, filepath_hash, transaction_type, estimated_bytes, actual_bytes, amount, window_start_time)
      VALUES (?, ?, ?, 'debit', ?, NULL, ?, ?)
      ON CONFLICT(request_id) DO NOTHING
    `;
    const statementResult = await executeD1RestStatement(dbConfig, sql, [fullRequestId, ipRangeHash, filepathHash, sanitizedEstimated, sanitizedAmount, sanitizedWindow]);
    const changes = statementResult?.meta?.changes ?? 0;
    return { success: changes > 0 };
  }

  return { success: false };
};

export const insertQuotaSettlement = async (dbConfig, params = {}) => {
  const {
    requestId,
    ipRangeHash,
    filepathHash,
    actualBytes = 0,
    refundAmount = 0,
    windowStartTime = Date.now(),
  } = params;

  if (!requestId || !ipRangeHash || !filepathHash) {
    return { success: false };
  }

  const sanitizedActual = sanitizeDeductBytes(actualBytes);
  const sanitizedWindow = sanitizeDeductBytes(windowStartTime);
  const normalizedRefund = -Math.abs(sanitizeDeductBytes(refundAmount));
  const fullRequestId = appendRequestSuffix(requestId, 'settlement');

  if (dbConfig?.dbMode === 'custom-pg-rest') {
    const result = await callPostgrestRpc(dbConfig, 'insert_quota_settlement', {
      p_request_id: requestId,
      p_ip_range_hash: ipRangeHash,
      p_filepath_hash: filepathHash,
      p_actual_bytes: sanitizedActual,
      p_refund_amount: normalizedRefund,
      p_window_start_time: sanitizedWindow,
    });
    const row = Array.isArray(result) && result.length > 0 ? result[0] : {};
    return { success: Boolean(row.success) };
  }

  if (dbConfig?.dbMode === 'd1') {
    const db = await ensureD1QuotaTables(dbConfig);
    const statement = db.prepare(`
      INSERT INTO ${QUOTA_TRANSACTIONS_TABLE} (request_id, ip_range_hash, filepath_hash, transaction_type, estimated_bytes, actual_bytes, amount, window_start_time)
      VALUES (?, ?, ?, 'settlement', NULL, ?, ?, ?)
      ON CONFLICT(request_id) DO NOTHING
    `);
    const result = await statement.bind(fullRequestId, ipRangeHash, filepathHash, sanitizedActual, normalizedRefund, sanitizedWindow).run();
    return { success: Boolean(result?.meta?.changes) };
  }

  if (dbConfig?.dbMode === 'd1-rest') {
    await ensureD1RestQuotaTables(dbConfig);
    const sql = `
      INSERT INTO ${QUOTA_TRANSACTIONS_TABLE} (request_id, ip_range_hash, filepath_hash, transaction_type, estimated_bytes, actual_bytes, amount, window_start_time)
      VALUES (?, ?, ?, 'settlement', NULL, ?, ?, ?)
      ON CONFLICT(request_id) DO NOTHING
    `;
    const statementResult = await executeD1RestStatement(dbConfig, sql, [fullRequestId, ipRangeHash, filepathHash, sanitizedActual, normalizedRefund, sanitizedWindow]);
    const changes = statementResult?.meta?.changes ?? 0;
    return { success: changes > 0 };
  }

  return { success: false };
};

export const deleteQuotaBlock = async (dbConfig, params = {}) => {
  const {
    ipRangeHash,
    filepathHash = null,
    blockType,
  } = params;

  if (!ipRangeHash || !blockType) {
    return { deleted: 0 };
  }

  if (dbConfig?.dbMode === 'custom-pg-rest') {
    const result = await callPostgrestRpc(dbConfig, 'delete_quota_block', {
      p_ip_range_hash: ipRangeHash,
      p_filepath_hash: filepathHash,
      p_block_type: blockType,
    });
    const row = Array.isArray(result) && result.length > 0 ? result[0] : {};
    return { deleted: sanitizeBigInt(row.delete_quota_block) };
  }

  if (dbConfig?.dbMode === 'd1') {
    const db = await ensureD1QuotaTables(dbConfig);
    const statement = db.prepare(`
      DELETE FROM ${QUOTA_BLOCKS_TABLE}
       WHERE ip_range_hash = ?
         AND block_type = ?
         AND (${filepathHash ? 'filepath_hash = ?' : 'filepath_hash IS NULL'})
    `);
    const bindings = filepathHash ? [ipRangeHash, blockType, filepathHash] : [ipRangeHash, blockType];
    const result = await statement.bind(...bindings).run();
    return { deleted: result?.meta?.changes ?? 0 };
  }

  if (dbConfig?.dbMode === 'd1-rest') {
    await ensureD1RestQuotaTables(dbConfig);
    const sql = `
      DELETE FROM ${QUOTA_BLOCKS_TABLE}
       WHERE ip_range_hash = ?
         AND block_type = ?
         AND (${filepathHash ? 'filepath_hash = ?' : 'filepath_hash IS NULL'})
    `;
    const bindings = filepathHash ? [ipRangeHash, blockType, filepathHash] : [ipRangeHash, blockType];
    const statementResult = await executeD1RestStatement(dbConfig, sql, bindings);
    const changes = statementResult?.meta?.changes ?? 0;
    return { deleted: changes };
  }

  return { deleted: 0 };
};
