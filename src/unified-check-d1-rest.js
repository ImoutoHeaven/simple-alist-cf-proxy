import { sha256Hash, calculateIPSubnet } from './utils.js';

/**
 * Execute a single SQL statement via the D1 REST API.
 */
const executeQuery = async (accountId, databaseId, apiToken, sql, params = []) => {
  const endpoint = `https://api.cloudflare.com/client/v4/accounts/${accountId}/d1/database/${databaseId}/query`;

  const statement = { sql };
  if (Array.isArray(params) && params.length > 0) {
    statement.params = params;
  }

  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${apiToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ sql: [statement] }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`D1 REST API error (${response.status}): ${errorText}`);
  }

  const result = await response.json();

  if (!result.success) {
    throw new Error(`D1 REST API query failed: ${JSON.stringify(result.errors || 'Unknown error')}`);
  }

  return result.result?.[0] || { results: [], success: true };
};

/**
 * Execute multiple SQL statements via a single D1 REST API batch call.
 * @param {string} accountId
 * @param {string} databaseId
 * @param {string} apiToken
 * @param {Array<{sql: string, params?: unknown[]}>} statements
 * @returns {Promise<Array<Object>>}
 */
const executeBatchQueries = async (accountId, databaseId, apiToken, statements) => {
  if (!Array.isArray(statements) || statements.length === 0) {
    throw new Error('[Unified Check D1-REST] Batch execution requires at least one statement');
  }

  const endpoint = `https://api.cloudflare.com/client/v4/accounts/${accountId}/d1/database/${databaseId}/query`;

  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${apiToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ sql: statements }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`D1 REST API batch error (${response.status}): ${errorText}`);
  }

  const result = await response.json();
  if (!result.success) {
    throw new Error(`D1 REST API batch failed: ${JSON.stringify(result.errors || 'Unknown error')}`);
  }

  if (!Array.isArray(result.result) || result.result.length !== statements.length) {
    throw new Error('[Unified Check D1-REST] Batch result length mismatch');
  }

  return result.result;
};

/**
 * Ensure cache, rate limit, throttle, and session tables exist before issuing queries.
 */
const ensureAllTables = async (accountId, databaseId, apiToken, { cacheTableName, rateLimitTableName, throttleTableName, sessionTableName }) => {
  await executeQuery(accountId, databaseId, apiToken, `
    CREATE TABLE IF NOT EXISTS ${cacheTableName} (
      PATH_HASH TEXT PRIMARY KEY,
      PATH TEXT NOT NULL,
      LINK_DATA TEXT NOT NULL,
      TIMESTAMP INTEGER NOT NULL,
      HOSTNAME_HASH TEXT
    )
  `);
  await executeQuery(accountId, databaseId, apiToken, `CREATE INDEX IF NOT EXISTS idx_cache_hostname ON ${cacheTableName}(HOSTNAME_HASH)`);

  await executeQuery(accountId, databaseId, apiToken, `
    CREATE TABLE IF NOT EXISTS ${rateLimitTableName} (
      IP_HASH TEXT PRIMARY KEY,
      IP_RANGE TEXT NOT NULL,
      ACCESS_COUNT INTEGER NOT NULL,
      LAST_WINDOW_TIME INTEGER NOT NULL,
      BLOCK_UNTIL INTEGER
    )
  `);
  await executeQuery(accountId, databaseId, apiToken, `CREATE INDEX IF NOT EXISTS idx_rate_limit_window ON ${rateLimitTableName}(LAST_WINDOW_TIME)`);
  await executeQuery(accountId, databaseId, apiToken, `CREATE INDEX IF NOT EXISTS idx_rate_limit_block ON ${rateLimitTableName}(BLOCK_UNTIL) WHERE BLOCK_UNTIL IS NOT NULL`);

  await executeQuery(accountId, databaseId, apiToken, `
    CREATE TABLE IF NOT EXISTS ${throttleTableName} (
      HOSTNAME_HASH TEXT PRIMARY KEY,
      HOSTNAME TEXT NOT NULL,
      ERROR_TIMESTAMP INTEGER,
      IS_PROTECTED INTEGER,
      LAST_ERROR_CODE INTEGER
    )
  `);
  await executeQuery(accountId, databaseId, apiToken, `CREATE INDEX IF NOT EXISTS idx_throttle_timestamp ON ${throttleTableName}(ERROR_TIMESTAMP)`);

  if (sessionTableName) {
    await executeBatchQueries(accountId, databaseId, apiToken, [
      {
        sql: `
          CREATE TABLE IF NOT EXISTS ${sessionTableName} (
            SESSION_TICKET TEXT PRIMARY KEY,
            FILE_PATH TEXT NOT NULL,
            FILE_PATH_HASH TEXT NOT NULL,
            IP_SUBNET TEXT NOT NULL,
            WORKER_ADDRESS TEXT NOT NULL,
            EXPIRE_AT INTEGER NOT NULL,
            CREATED_AT INTEGER NOT NULL
          )
        `,
      },
      {
        sql: `CREATE INDEX IF NOT EXISTS idx_session_expire ON ${sessionTableName}(EXPIRE_AT)`,
      },
    ]);
  }
};

const createDefaultSessionResult = (shouldCheckSession) => ({
  found: shouldCheckSession ? false : null,
  filePath: null,
  filePathHash: null,
  ipSubnet: null,
  workerAddress: null,
  expireAt: null,
  error: null,
});

const createDefaultCacheResult = () => ({
  hit: false,
  linkData: null,
  timestamp: null,
  hostnameHash: null,
});

const createDefaultThrottleResult = () => ({
  status: 'normal_operation',
  recordExists: false,
  isProtected: null,
  errorTimestamp: null,
  errorCode: null,
  retryAfter: 0,
});

const parseSessionFromRow = (row, sessionTicket, now) => {
  const sessionResult = createDefaultSessionResult(true);

  if (!row || row.SESSION_TICKET == null) {
    sessionResult.error = 'session not found';
    console.log('[Unified Check D1-REST] Session found: false');
    return sessionResult;
  }

  const expireAtRaw = row.EXPIRE_AT != null ? Number(row.EXPIRE_AT) : null;

  sessionResult.found = true;
  sessionResult.filePath = row.FILE_PATH || null;
  sessionResult.filePathHash = row.FILE_PATH_HASH || null;
  sessionResult.ipSubnet = row.IP_SUBNET || null;
  sessionResult.workerAddress = row.WORKER_ADDRESS || null;
  sessionResult.expireAt = Number.isFinite(expireAtRaw) ? expireAtRaw : null;
  sessionResult.error = null;

  if (sessionResult.expireAt !== null && sessionResult.expireAt < now) {
    sessionResult.found = false;
    sessionResult.error = 'session expired';
    console.log('[Unified Check D1-REST] Session expired for ticket:', sessionTicket);
  } else if (sessionResult.expireAt === null && row.EXPIRE_AT != null) {
    sessionResult.found = false;
    sessionResult.error = 'session expired';
    console.warn('[Unified Check D1-REST] Session record missing valid EXPIRE_AT, treating as expired');
  }

  console.log('[Unified Check D1-REST] Session found:', sessionResult.found);
  return sessionResult;
};

const parseCacheFromRow = (row, now, cacheTTL) => {
  const cacheResult = createDefaultCacheResult();

  if (!row) {
    console.log('[Unified Check D1-REST] Cache MISS');
    return cacheResult;
  }

  const timestamp = Number.parseInt(row.TIMESTAMP, 10);
  const age = Number.isFinite(timestamp) ? now - timestamp : Number.POSITIVE_INFINITY;

  if (Number.isFinite(age) && age <= cacheTTL) {
    try {
      cacheResult.hit = true;
      cacheResult.linkData = JSON.parse(row.LINK_DATA);
      cacheResult.timestamp = row.TIMESTAMP;
      cacheResult.hostnameHash = row.HOSTNAME_HASH || null;
      console.log('[Unified Check D1-REST] Cache HIT');
    } catch (error) {
      console.error('[Unified Check D1-REST] Failed to parse cache:', error.message);
    }
  } else {
    console.log('[Unified Check D1-REST] Cache expired (age:', age, 's)');
  }

  return cacheResult;
};

const parseRateLimitResult = (row, now, limit, windowSeconds, ipSubnet) => {
  if (!row) {
    throw new Error('[Unified Check D1-REST] Rate limit UPSERT returned no rows');
  }

  const accessCount = Number.parseInt(row.ACCESS_COUNT, 10);
  const lastWindowTime = Number.parseInt(row.LAST_WINDOW_TIME, 10);
  const blockUntil = row.BLOCK_UNTIL ? Number.parseInt(row.BLOCK_UNTIL, 10) : null;

  let allowed = true;
  let retryAfter = 0;

  if (blockUntil && blockUntil > now) {
    allowed = false;
    retryAfter = blockUntil - now;
    console.log('[Unified Check D1-REST] Rate limit BLOCKED until:', new Date(blockUntil * 1000).toISOString());
  } else if (accessCount >= limit) {
    const diff = Number.isFinite(lastWindowTime) ? now - lastWindowTime : windowSeconds;
    retryAfter = Math.max(windowSeconds - diff, 0);
    allowed = false;
    console.log('[Unified Check D1-REST] Rate limit EXCEEDED:', accessCount, '>=', limit);
  } else {
    console.log('[Unified Check D1-REST] Rate limit OK:', accessCount, '/', limit);
  }

  return {
    allowed,
    accessCount,
    lastWindowTime,
    blockUntil,
    retryAfter,
    ipSubnet,
  };
};

const parseThrottleFromRow = (row, cacheResult, now, throttleTimeWindow) => {
  const throttleResult = createDefaultThrottleResult();

  if (!cacheResult.hit || !cacheResult.hostnameHash) {
    console.log('[Unified Check D1-REST] Skipping throttle check (no hostname_hash from cache)');
    return throttleResult;
  }

  if (!row) {
    console.log('[Unified Check D1-REST] Throttle normal_operation (no record found)');
    return throttleResult;
  }

  throttleResult.recordExists = true;
  throttleResult.isProtected = row.IS_PROTECTED;
  throttleResult.errorTimestamp = row.ERROR_TIMESTAMP;
  throttleResult.errorCode = row.LAST_ERROR_CODE ?? null;

  if (row.IS_PROTECTED === 1) {
    const errorTimestamp = Number.parseInt(row.ERROR_TIMESTAMP, 10);
    const timeSinceError = Number.isFinite(errorTimestamp) ? now - errorTimestamp : throttleTimeWindow;

    if (timeSinceError < throttleTimeWindow) {
      throttleResult.status = 'protected';
      throttleResult.retryAfter = throttleTimeWindow - timeSinceError;
      console.log('[Unified Check D1-REST] Throttle PROTECTED, retry after:', throttleResult.retryAfter);
    } else {
      throttleResult.status = 'resume_operation';
      throttleResult.retryAfter = 0;
      console.log('[Unified Check D1-REST] Throttle resume_operation');
    }
  } else if (row.IS_PROTECTED === 0) {
    console.log('[Unified Check D1-REST] Throttle normal_operation (IS_PROTECTED = 0)');
  } else {
    console.log('[Unified Check D1-REST] Throttle normal_operation (IS_PROTECTED = NULL, invalid state)');
  }

  return throttleResult;
};

/**
 * Unified check for D1-REST (RTT 3→1 optimization).
 * Executes a unified SELECT (session/cache/throttle) and rate limit upsert in a single batch.
 */
export const unifiedCheckD1Rest = async (path, clientIP, config, sessionTicket = null) => {
  if (!config.accountId || !config.databaseId || !config.apiToken) {
    throw new Error('[Unified Check D1-REST] Missing D1-REST configuration');
  }

  const { accountId, databaseId, apiToken } = config;
  const now = Math.floor(Date.now() / 1000);
  const cacheTTL = config.linkTTL ?? 1800;
  const windowSeconds = config.windowTimeSeconds ?? 86400;
  const limit = config.limit ?? 100;
  const blockSeconds = config.blockTimeSeconds ?? 600;
  const cacheTableName = config.cacheTableName || 'DOWNLOAD_CACHE_TABLE';
  const rateLimitTableName = config.rateLimitTableName || 'DOWNLOAD_IP_RATELIMIT_TABLE';
  const throttleTableName = config.throttleTableName || 'THROTTLE_PROTECTION';
  const throttleTimeWindow = config.throttleTimeWindow ?? 60;
  const ipv4Suffix = config.ipv4Suffix ?? '/32';
  const ipv6Suffix = config.ipv6Suffix ?? '/60';

  const sessionEnabled = config.sessionEnabled === true;
  const configuredSessionTable = config.sessionTableName || 'SESSION_MAPPING_TABLE';
  const shouldCheckSession = sessionEnabled && typeof sessionTicket === 'string' && sessionTicket.trim() !== '';

  if (config.initTables === true) {
    await ensureAllTables(accountId, databaseId, apiToken, {
      cacheTableName,
      rateLimitTableName,
      throttleTableName,
      sessionTableName: sessionEnabled ? configuredSessionTable : null,
    });
  }

  console.log('[Unified Check D1-REST] Starting unified check for path:', path);

  const pathHash = await sha256Hash(path);
  if (!pathHash) {
    throw new Error('[Unified Check D1-REST] Failed to calculate path hash');
  }

  const ipSubnet = calculateIPSubnet(clientIP, ipv4Suffix, ipv6Suffix);
  if (!ipSubnet) {
    throw new Error('[Unified Check D1-REST] Failed to calculate IP subnet');
  }

  const ipHash = await sha256Hash(ipSubnet);
  if (!ipHash) {
    throw new Error('[Unified Check D1-REST] Failed to calculate IP hash');
  }

  const sessionTicketParam = shouldCheckSession ? sessionTicket : null;

  const sessionJoinClause = shouldCheckSession
    ? `
    LEFT JOIN ${configuredSessionTable} s 
      ON s.SESSION_TICKET = params.session_ticket_param 
      AND params.session_ticket_param IS NOT NULL 
      AND params.session_ticket_param != ''
  `
    : `
    LEFT JOIN (
      SELECT 
        NULL AS SESSION_TICKET,
        NULL AS FILE_PATH,
        NULL AS FILE_PATH_HASH,
        NULL AS IP_SUBNET,
        NULL AS WORKER_ADDRESS,
        NULL AS EXPIRE_AT
    ) s ON 1=0
  `;

  const unifiedSql = `
    SELECT 
      s.SESSION_TICKET,
      s.FILE_PATH,
      s.FILE_PATH_HASH,
      s.IP_SUBNET,
      s.WORKER_ADDRESS,
      s.EXPIRE_AT,
      c.LINK_DATA,
      c.TIMESTAMP,
      c.HOSTNAME_HASH,
      t.IS_PROTECTED,
      t.ERROR_TIMESTAMP,
      t.LAST_ERROR_CODE
    FROM 
      (SELECT ? AS provided_path_hash, ? AS session_ticket_param) params
    ${sessionJoinClause}
    LEFT JOIN ${cacheTableName} c 
      ON c.PATH_HASH = COALESCE(s.FILE_PATH_HASH, params.provided_path_hash)
    LEFT JOIN ${throttleTableName} t 
      ON t.HOSTNAME_HASH = c.HOSTNAME_HASH
    LIMIT 1
  `;

  const rateLimitSql = `
    INSERT INTO ${rateLimitTableName} (IP_HASH, IP_RANGE, ACCESS_COUNT, LAST_WINDOW_TIME, BLOCK_UNTIL)
    VALUES (?, ?, 1, ?, NULL)
    ON CONFLICT (IP_HASH) DO UPDATE SET
      ACCESS_COUNT = CASE
        WHEN ? - ${rateLimitTableName}.LAST_WINDOW_TIME >= ? THEN 1
        WHEN ${rateLimitTableName}.BLOCK_UNTIL IS NOT NULL AND ${rateLimitTableName}.BLOCK_UNTIL <= ? THEN 1
        WHEN ${rateLimitTableName}.ACCESS_COUNT >= ? THEN ${rateLimitTableName}.ACCESS_COUNT
        ELSE ${rateLimitTableName}.ACCESS_COUNT + 1
      END,
      LAST_WINDOW_TIME = CASE
        WHEN ? - ${rateLimitTableName}.LAST_WINDOW_TIME >= ? THEN ?
        WHEN ${rateLimitTableName}.BLOCK_UNTIL IS NOT NULL AND ${rateLimitTableName}.BLOCK_UNTIL <= ? THEN ?
        ELSE ${rateLimitTableName}.LAST_WINDOW_TIME
      END,
      BLOCK_UNTIL = CASE
        WHEN ? - ${rateLimitTableName}.LAST_WINDOW_TIME >= ? THEN NULL
        WHEN ${rateLimitTableName}.BLOCK_UNTIL IS NOT NULL AND ${rateLimitTableName}.BLOCK_UNTIL <= ? THEN NULL
        WHEN ${rateLimitTableName}.ACCESS_COUNT >= ? AND ? > 0 THEN ? + ?
        ELSE ${rateLimitTableName}.BLOCK_UNTIL
      END
    RETURNING ACCESS_COUNT, LAST_WINDOW_TIME, BLOCK_UNTIL
  `;

  const rateLimitParams = [
    ipHash, ipSubnet, now,
    now, windowSeconds, now, limit,
    now, windowSeconds, now, now, now,
    now, windowSeconds, now, limit, blockSeconds, now, blockSeconds,
  ];

  const batchQueries = [
    { sql: unifiedSql, params: [pathHash, sessionTicketParam || null] },
    { sql: rateLimitSql, params: rateLimitParams },
  ];

  console.log('[Unified Check D1-REST] Executing batch (unified query + rate limit in 1 RTT)');
  const batchResults = await executeBatchQueries(accountId, databaseId, apiToken, batchQueries);

  if (!Array.isArray(batchResults) || batchResults.length !== batchQueries.length) {
    throw new Error('[Unified Check D1-REST] Unified batch returned incomplete results');
  }

  batchResults.forEach((statementResult, index) => {
    if (!statementResult) {
      throw new Error(`[Unified Check D1-REST] Batch result missing for statement #${index + 1}`);
    }
    if (statementResult.success === false) {
      throw new Error(`[Unified Check D1-REST] Batch statement #${index + 1} failed: ${JSON.stringify(statementResult.errors || 'Unknown error')}`);
    }
  });

  const unifiedRow = batchResults[0]?.results?.[0] || null;
  const rateLimitRow = batchResults[1]?.results?.[0] || null;

  let sessionResult = createDefaultSessionResult(shouldCheckSession);
  if (shouldCheckSession) {
    sessionResult = parseSessionFromRow(unifiedRow, sessionTicket, now);
  } else {
    console.log('[Unified Check D1-REST] Session check skipped');
  }

  const cacheRow = unifiedRow && unifiedRow.LINK_DATA != null && unifiedRow.TIMESTAMP != null
    ? {
        LINK_DATA: unifiedRow.LINK_DATA,
        TIMESTAMP: unifiedRow.TIMESTAMP,
        HOSTNAME_HASH: unifiedRow.HOSTNAME_HASH,
      }
    : null;

  const cacheResult = parseCacheFromRow(cacheRow, now, cacheTTL);

  const throttleRow = unifiedRow && (
    unifiedRow.IS_PROTECTED != null ||
    unifiedRow.ERROR_TIMESTAMP != null ||
    unifiedRow.LAST_ERROR_CODE != null
  )
    ? {
        IS_PROTECTED: unifiedRow.IS_PROTECTED,
        ERROR_TIMESTAMP: unifiedRow.ERROR_TIMESTAMP,
        LAST_ERROR_CODE: unifiedRow.LAST_ERROR_CODE,
      }
    : null;

  const throttleResult = parseThrottleFromRow(throttleRow, cacheResult, now, throttleTimeWindow);
  const rateLimitResult = parseRateLimitResult(rateLimitRow, now, limit, windowSeconds, ipSubnet);

  console.log('[Unified Check D1-REST] Completed successfully (1 RTT)');

  return {
    session: sessionResult,
    cache: cacheResult,
    rateLimit: rateLimitResult,
    throttle: throttleResult,
  };
};
