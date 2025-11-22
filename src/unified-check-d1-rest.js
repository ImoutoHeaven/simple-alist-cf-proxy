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
 * Ensure cache, rate limit, and throttle tables exist before issuing queries.
 */
const ensureAllTables = async (
  accountId,
  databaseId,
  apiToken,
  {
    cacheTableName,
    rateLimitTableName,
    throttleTableName,
    lastActiveTableName,
    fairQueueTableName = 'upstream_slot_pool',
    fairQueueCooldownTableName = 'upstream_ip_cooldown',
    fairQueueQueueDepthTableName = 'upstream_ip_queue_depth',
    fairQueueHostPacingTableName = 'worker_host_pacing',
  }
) => {
  const activeTableName = lastActiveTableName || 'DOWNLOAD_LAST_ACTIVE_TABLE';

  const baseStatements = [
    {
      sql: `
        CREATE TABLE IF NOT EXISTS ${cacheTableName} (
          PATH_HASH TEXT PRIMARY KEY,
          PATH TEXT NOT NULL,
          LINK_DATA TEXT NOT NULL,
          TIMESTAMP INTEGER NOT NULL,
          HOSTNAME_HASH TEXT
        )
      `,
    },
    { sql: `CREATE INDEX IF NOT EXISTS idx_cache_hostname ON ${cacheTableName}(HOSTNAME_HASH)` },
    {
      sql: `
        CREATE TABLE IF NOT EXISTS ${rateLimitTableName} (
          IP_HASH TEXT PRIMARY KEY,
          IP_RANGE TEXT NOT NULL,
          ACCESS_COUNT INTEGER NOT NULL,
          LAST_WINDOW_TIME INTEGER NOT NULL,
          BLOCK_UNTIL INTEGER
        )
      `,
    },
    { sql: `CREATE INDEX IF NOT EXISTS idx_rate_limit_window ON ${rateLimitTableName}(LAST_WINDOW_TIME)` },
    { sql: `CREATE INDEX IF NOT EXISTS idx_rate_limit_block ON ${rateLimitTableName}(BLOCK_UNTIL) WHERE BLOCK_UNTIL IS NOT NULL` },
    {
      sql: `
        CREATE TABLE IF NOT EXISTS ${throttleTableName} (
          HOSTNAME_HASH TEXT PRIMARY KEY,
          HOSTNAME TEXT NOT NULL,
          ERROR_TIMESTAMP INTEGER,
          IS_PROTECTED INTEGER,
          LAST_ERROR_CODE INTEGER
        )
      `,
    },
    { sql: `CREATE INDEX IF NOT EXISTS idx_throttle_timestamp ON ${throttleTableName}(ERROR_TIMESTAMP)` },
    {
      sql: `
        CREATE TABLE IF NOT EXISTS ${activeTableName} (
          IP_HASH TEXT NOT NULL,
          PATH_HASH TEXT NOT NULL,
          LAST_ACCESS_TIME INTEGER NOT NULL,
          TOTAL_ACCESS_COUNT INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY (IP_HASH, PATH_HASH)
        )
      `,
    },
    { sql: `CREATE INDEX IF NOT EXISTS idx_download_last_active_time ON ${activeTableName}(LAST_ACCESS_TIME)` },
    {
      sql: `
        CREATE TABLE IF NOT EXISTS ${fairQueueTableName} (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          hostname_pattern TEXT NOT NULL,
          slot_index INTEGER NOT NULL,
          status TEXT NOT NULL DEFAULT 'available',
          ip_hash TEXT,
          locked_at TEXT,
          created_at TEXT DEFAULT (datetime('now'))
        )
      `,
      params: [],
    },
    {
      sql: `CREATE UNIQUE INDEX IF NOT EXISTS idx_fair_queue_unique
            ON ${fairQueueTableName} (hostname_pattern, slot_index)`,
      params: [],
    },
    {
      sql: `CREATE INDEX IF NOT EXISTS idx_fair_queue_host_status
            ON ${fairQueueTableName} (hostname_pattern, status)`,
      params: [],
    },
    {
      sql: `CREATE TABLE IF NOT EXISTS ${fairQueueCooldownTableName} (
        hostname_pattern TEXT NOT NULL,
        ip_hash TEXT NOT NULL,
        last_release_at TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (hostname_pattern, ip_hash)
      )`,
    },
    {
      sql: `CREATE INDEX IF NOT EXISTS idx_ip_cooldown_ts
            ON ${fairQueueCooldownTableName} (last_release_at)`,
    },
    {
      sql: `CREATE TABLE IF NOT EXISTS ${fairQueueQueueDepthTableName} (
        hostname_pattern TEXT NOT NULL,
        ip_hash TEXT NOT NULL,
        waiting_count INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (hostname_pattern, ip_hash)
      )`,
    },
    {
      sql: `CREATE INDEX IF NOT EXISTS idx_ip_queue_depth_ts
            ON ${fairQueueQueueDepthTableName} (updated_at)`,
    },
    {
      sql: `CREATE TABLE IF NOT EXISTS ${fairQueueHostPacingTableName} (
        hostname_pattern TEXT PRIMARY KEY,
        last_acquired_at TEXT NOT NULL
      )`,
    },
  ];

  await executeBatchQueries(accountId, databaseId, apiToken, baseStatements);
};

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
 * Executes a unified SELECT (cache/throttle) and rate limit upsert in a single batch.
 */
export const unifiedCheckD1Rest = async (path, clientIP, config) => {
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
  const lastActiveTableName = config.lastActiveTableName || 'DOWNLOAD_LAST_ACTIVE_TABLE';
  const fairQueueQueueDepthTableName = config.fairQueueQueueDepthTableName || 'upstream_ip_queue_depth';
  const hostPacingTableName = config.fairQueueHostPacingTableName || 'worker_host_pacing';
  const throttleTimeWindow = config.throttleTimeWindow ?? 60;
  const ipv4Suffix = config.ipv4Suffix ?? '/32';
  const ipv6Suffix = config.ipv6Suffix ?? '/60';

  if (config.initTables === true) {
    await ensureAllTables(accountId, databaseId, apiToken, {
      cacheTableName,
      rateLimitTableName,
      throttleTableName,
      lastActiveTableName,
      fairQueueQueueDepthTableName,
      fairQueueHostPacingTableName: hostPacingTableName,
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

  const unifiedSql = `
    SELECT 
      c.LINK_DATA,
      c.TIMESTAMP,
      c.HOSTNAME_HASH,
      t.IS_PROTECTED,
      t.ERROR_TIMESTAMP,
      t.LAST_ERROR_CODE,
      active.LAST_ACCESS_TIME AS ACTIVE_LAST_ACCESS_TIME
    FROM 
      (SELECT ? AS provided_path_hash, ? AS ip_hash_param) params
    LEFT JOIN ${cacheTableName} c 
      ON c.PATH_HASH = params.provided_path_hash
    LEFT JOIN ${throttleTableName} t 
      ON t.HOSTNAME_HASH = c.HOSTNAME_HASH
    LEFT JOIN ${lastActiveTableName} active
      ON active.PATH_HASH = params.provided_path_hash
      AND active.IP_HASH = params.ip_hash_param
    LIMIT 1
  `;

  const rateLimitSql = `
    INSERT INTO ${rateLimitTableName} (IP_HASH, IP_RANGE, ACCESS_COUNT, LAST_WINDOW_TIME, BLOCK_UNTIL)
    VALUES (?, ?, 1, ?, NULL)
    ON CONFLICT (IP_HASH) DO UPDATE SET
      ACCESS_COUNT = CASE
        WHEN ${rateLimitTableName}.BLOCK_UNTIL IS NOT NULL AND ${rateLimitTableName}.BLOCK_UNTIL > ? THEN ${rateLimitTableName}.ACCESS_COUNT
        WHEN ? - ${rateLimitTableName}.LAST_WINDOW_TIME >= ? THEN 1
        WHEN ${rateLimitTableName}.BLOCK_UNTIL IS NOT NULL AND ${rateLimitTableName}.BLOCK_UNTIL <= ? THEN 1
        WHEN ${rateLimitTableName}.ACCESS_COUNT >= ? THEN ${rateLimitTableName}.ACCESS_COUNT
        ELSE ${rateLimitTableName}.ACCESS_COUNT + 1
      END,
      LAST_WINDOW_TIME = CASE
        WHEN ${rateLimitTableName}.BLOCK_UNTIL IS NOT NULL AND ${rateLimitTableName}.BLOCK_UNTIL > ? THEN ${rateLimitTableName}.LAST_WINDOW_TIME
        WHEN ? - ${rateLimitTableName}.LAST_WINDOW_TIME >= ? THEN ?
        WHEN ${rateLimitTableName}.BLOCK_UNTIL IS NOT NULL AND ${rateLimitTableName}.BLOCK_UNTIL <= ? THEN ?
        ELSE ${rateLimitTableName}.LAST_WINDOW_TIME
      END,
      BLOCK_UNTIL = CASE
        WHEN ${rateLimitTableName}.BLOCK_UNTIL IS NOT NULL AND ${rateLimitTableName}.BLOCK_UNTIL > ? THEN ${rateLimitTableName}.BLOCK_UNTIL
        WHEN ${rateLimitTableName}.BLOCK_UNTIL IS NOT NULL AND ${rateLimitTableName}.BLOCK_UNTIL <= ? THEN NULL
        WHEN (${rateLimitTableName}.BLOCK_UNTIL IS NULL OR ${rateLimitTableName}.BLOCK_UNTIL <= ?)
             AND (
               CASE
                 WHEN ? - ${rateLimitTableName}.LAST_WINDOW_TIME >= ? THEN 1
                 WHEN ${rateLimitTableName}.BLOCK_UNTIL IS NOT NULL AND ${rateLimitTableName}.BLOCK_UNTIL <= ? THEN 1
                 WHEN ${rateLimitTableName}.ACCESS_COUNT >= ? THEN ${rateLimitTableName}.ACCESS_COUNT
                 ELSE ${rateLimitTableName}.ACCESS_COUNT + 1
               END
             ) >= ?
             AND ? > 0 THEN ? + ?
        ELSE ${rateLimitTableName}.BLOCK_UNTIL
      END
    RETURNING ACCESS_COUNT, LAST_WINDOW_TIME, BLOCK_UNTIL
  `;

  const rateLimitParams = [
    ipHash, ipSubnet, now,
    now, now, windowSeconds, now, limit,
    now, now, windowSeconds, now, now, now,
    now, now, now,
    now, windowSeconds, now, limit, limit, blockSeconds, now, blockSeconds,
  ];

  const batchQueries = [
    { sql: unifiedSql, params: [pathHash, ipHash] },
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

  const activeLastAccessTime = (() => {
    if (!unifiedRow || unifiedRow.ACTIVE_LAST_ACCESS_TIME == null) {
      return null;
    }
    const parsed = Number(unifiedRow.ACTIVE_LAST_ACCESS_TIME);
    return Number.isFinite(parsed) ? parsed : null;
  })();

  const cacheRow = unifiedRow && unifiedRow.LINK_DATA != null && unifiedRow.TIMESTAMP != null
    ? {
        LINK_DATA: unifiedRow.LINK_DATA,
        TIMESTAMP: unifiedRow.TIMESTAMP,
        HOSTNAME_HASH: unifiedRow.HOSTNAME_HASH,
      }
    : null;

  let cacheResult = parseCacheFromRow(cacheRow, now, cacheTTL);

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

  let throttleResult = parseThrottleFromRow(throttleRow, cacheResult, now, throttleTimeWindow);
  const rateLimitResult = parseRateLimitResult(rateLimitRow, now, limit, windowSeconds, ipSubnet);

  const idleTimeout = Number.isFinite(Number(config.idleTimeout))
    ? Number(config.idleTimeout)
    : 0;

  const idleInfo = {
    expired: false,
    timeout: idleTimeout,
    lastAccessTime: activeLastAccessTime,
    idleDuration: activeLastAccessTime != null ? now - activeLastAccessTime : null,
  };

  const idleErrorMessage = 'Link expired due to inactivity';

  if (idleTimeout > 0 && activeLastAccessTime != null) {
    const idleDuration = idleInfo.idleDuration ?? 0;
    if (idleDuration > idleTimeout) {
      idleInfo.expired = true;
      idleInfo.reason = idleErrorMessage;
      console.log(
        `[Unified Check D1-REST] Idle timeout exceeded (idle ${idleDuration}s > ${idleTimeout}s)`
      );

      cacheResult = { ...createDefaultCacheResult() };
      throttleResult = { ...createDefaultThrottleResult() };
    }
  }

  console.log('[Unified Check D1-REST] Completed successfully (1 RTT)');

  return {
    cache: cacheResult,
    rateLimit: rateLimitResult,
    throttle: throttleResult,
    idle: idleInfo,
  };
};

const updateLastActive = async (config, ipHash, pathHash) => {
  if (!config || !config.accountId || !config.databaseId || !config.apiToken) {
    throw new Error('[LastActive] Missing D1 REST configuration');
  }

  const tableName = config.lastActiveTableName || 'DOWNLOAD_LAST_ACTIVE_TABLE';
  const now = Math.floor(Date.now() / 1000);

  const statements = [
    {
      sql: `
        INSERT INTO ${tableName} (IP_HASH, PATH_HASH, LAST_ACCESS_TIME, TOTAL_ACCESS_COUNT)
        VALUES (?, ?, ?, 1)
        ON CONFLICT(IP_HASH, PATH_HASH) DO UPDATE SET
          LAST_ACCESS_TIME = excluded.LAST_ACCESS_TIME,
          TOTAL_ACCESS_COUNT = ${tableName}.TOTAL_ACCESS_COUNT + 1
      `,
      params: [ipHash, pathHash, now],
    },
  ];

  return executeBatchQueries(config.accountId, config.databaseId, config.apiToken, statements);
};

// ========================================
// Fair Queue Functions (D1-REST Mode)
// ========================================

const ensureSlotPoolRest = async (
  accountId,
  databaseId,
  apiToken,
  hostname,
  globalLimit,
  tableName,
  cooldownTableName = 'upstream_ip_cooldown'
) => {
  await executeQuery(
    accountId,
    databaseId,
    apiToken,
    `CREATE TABLE IF NOT EXISTS ${cooldownTableName} (
      hostname_pattern TEXT NOT NULL,
      ip_hash TEXT NOT NULL,
      last_release_at TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (hostname_pattern, ip_hash)
    )`
  );

  await executeQuery(
    accountId,
    databaseId,
    apiToken,
    `CREATE INDEX IF NOT EXISTS idx_ip_cooldown_ts
     ON ${cooldownTableName} (last_release_at)`
  );

  const countResult = await executeQuery(
    accountId,
    databaseId,
    apiToken,
    `SELECT COUNT(*) AS c FROM ${tableName} WHERE hostname_pattern = ?`,
    [hostname]
  );

  const row = countResult.results?.[0] || null;
  const current = Number(row?.c ?? row?.C ?? 0);
  if (current >= globalLimit) {
    return;
  }

  const statements = [];
  for (let i = current + 1; i <= globalLimit; i++) {
    statements.push({
      sql: `INSERT OR IGNORE INTO ${tableName} (hostname_pattern, slot_index, status)
            VALUES (?, ?, 'available')`,
      params: [hostname, i],
    });
  }

  if (statements.length > 0) {
    await executeBatchQueries(accountId, databaseId, apiToken, statements);
  }
};

const ensureQueueDepthTableRest = async (
  accountId,
  databaseId,
  apiToken,
  tableName = 'upstream_ip_queue_depth'
) => {
  await executeQuery(
    accountId,
    databaseId,
    apiToken,
    `CREATE TABLE IF NOT EXISTS ${tableName} (
      hostname_pattern TEXT NOT NULL,
      ip_hash TEXT NOT NULL,
      waiting_count INTEGER NOT NULL DEFAULT 0,
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (hostname_pattern, ip_hash)
    )`
  );

  await executeQuery(
    accountId,
    databaseId,
    apiToken,
    `CREATE INDEX IF NOT EXISTS idx_ip_queue_depth_ts
     ON ${tableName} (updated_at)`
  );
};

const ensurePacingTableRest = async (
  accountId,
  databaseId,
  apiToken,
  tableName = 'worker_host_pacing'
) => {
  await executeQuery(
    accountId,
    databaseId,
    apiToken,
    `CREATE TABLE IF NOT EXISTS ${tableName} (
      hostname_pattern TEXT PRIMARY KEY,
      last_acquired_at TEXT NOT NULL
    )`
  );
};

const tryClaimPacingTokenRest = async (
  accountId,
  databaseId,
  apiToken,
  hostname,
  intervalMs,
  tableName = 'worker_host_pacing'
) => {
  if (!intervalMs || intervalMs <= 0) {
    return true;
  }

  const seconds = intervalMs / 1000;
  const modifier = `-${seconds} seconds`;

  try {
    const result = await executeQuery(
      accountId,
      databaseId,
      apiToken,
      `INSERT INTO ${tableName} (hostname_pattern, last_acquired_at)
       VALUES (?, strftime('%Y-%m-%d %H:%M:%f','now'))
       ON CONFLICT(hostname_pattern) DO UPDATE
       SET last_acquired_at = excluded.last_acquired_at
       WHERE ${tableName}.last_acquired_at <= strftime('%Y-%m-%d %H:%M:%f','now', ?)`,
      [hostname, modifier]
    );

    const changes = result.meta?.changes ?? 0;
    return changes > 0;
  } catch (error) {
    console.error('[D1-REST Pacing] Error:', error);
    return false;
  }
};

const cleanupHostPacing = async (config) => {
  if (!config?.accountId || !config.databaseId || !config.apiToken) {
    throw new Error('[Fair Queue D1-REST] Missing D1 REST configuration for pacing cleanup');
  }

  const { accountId, databaseId, apiToken } = config;
  const tableName = config.fairQueueHostPacingTableName || 'worker_host_pacing';
  const ttlSeconds = Number.isFinite(config?.hostPacingCleanupTtlSeconds)
    ? config.hostPacingCleanupTtlSeconds
    : 604800;

  if (ttlSeconds <= 0) {
    return 0;
  }

  await ensurePacingTableRest(accountId, databaseId, apiToken, tableName);

  const changes = await executeQuery(
    accountId,
    databaseId,
    apiToken,
    `DELETE FROM ${tableName}
     WHERE updated_at < datetime('now', ?)`,
    [`-${ttlSeconds} seconds`]
  );

  const deleted = changes.meta?.changes ?? 0;
  if (deleted > 0) {
    console.log(`[Fair Queue D1-REST] Cleaned up ${deleted} pacing records`);
  }
  return deleted;
};

const tryAcquireFairSlotOnceRest = async (
  accountId,
  databaseId,
  apiToken,
  hostname,
  ipHash,
  config
) => {
  const tableName = config.fairQueueTableName || 'upstream_slot_pool';
  const cooldownTableName = config.fairQueueCooldownTableName || 'upstream_ip_cooldown';
  const zombieCutoff = `-${config.zombieTimeoutSeconds} seconds`;
  const cooldownSeconds =
    config && config.ipCooldownEnabled ? Math.max(0, config.ipCooldownSeconds || 0) : 0;

  try {
    const ipCountRow = (
      await executeQuery(
        accountId,
        databaseId,
        apiToken,
        `SELECT COUNT(*) AS c FROM ${tableName}
         WHERE hostname_pattern = ?
           AND status = 'locked'
           AND ip_hash = ?
           AND (locked_at IS NULL OR locked_at >= datetime('now', ?))`,
        [hostname, ipHash, zombieCutoff]
      )
    ).results?.[0] || null;
    const ipCount = Number(ipCountRow?.c ?? ipCountRow?.C ?? 0);

    if (ipCount >= config.perIpLimit) {
      return 0;
    }

    if (
      cooldownSeconds > 0 &&
      ipCount > 0 &&
      ipCount < config.perIpLimit &&
      ipHash
    ) {
      const cooldownResult = await executeQuery(
        accountId,
        databaseId,
        apiToken,
        `SELECT 1 AS active FROM ${cooldownTableName}
         WHERE hostname_pattern = ?
           AND ip_hash = ?
           AND last_release_at > datetime('now', ?)`,
        [hostname, ipHash, `-${cooldownSeconds} seconds`]
      );

      const cooldownRow = cooldownResult.results?.[0] || null;
      if (cooldownRow) {
        return 0;
      }
    }

    const candidate = await executeQuery(
      accountId,
      databaseId,
      apiToken,
      `SELECT id FROM ${tableName}
       WHERE hostname_pattern = ?
         AND slot_index <= ?
         AND (
           status = 'available'
           OR (
             status = 'locked'
             AND locked_at IS NOT NULL
             AND locked_at < datetime('now', ?)
           )
         )
       ORDER BY slot_index
       LIMIT 1`,
      [hostname, config.globalLimit, zombieCutoff]
    );

    const slotId = candidate.results?.[0]?.id ?? candidate.results?.[0]?.ID ?? null;
    if (!slotId) {
      return -1;
    }

    const updateResult = await executeQuery(
      accountId,
      databaseId,
      apiToken,
      `UPDATE ${tableName}
       SET status = 'locked', ip_hash = ?, locked_at = datetime('now')
       WHERE id = ?
         AND (
           status = 'available'
           OR (
             status = 'locked'
             AND locked_at IS NOT NULL
             AND locked_at < datetime('now', ?)
           )
         )`,
      [ipHash, slotId, zombieCutoff]
    );
    const changes = updateResult.meta?.changes ?? 0;
    if (changes === 0) {
      return -1;
    }
    return slotId;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const wrapped = new Error(`[Fair Queue D1-REST] Database error: ${message}`);
    wrapped.name = 'FairQueueDbError';
    wrapped.originalError = error;
    throw wrapped;
  }
};

const tryAcquireWorkerSlotRest = async (hostname, ipHash, config) => {
  if (!config?.accountId || !config.databaseId || !config.apiToken) {
    throw new Error('[Fair Queue D1-REST] Missing D1 REST configuration');
  }

  const { accountId, databaseId, apiToken } = config;

  const hostIntervalMs =
    Number.isFinite(config.hostIntervalMs) && config.hostIntervalMs > 0
      ? config.hostIntervalMs
      : Number.isFinite(config.pacingIntervalMs) && config.pacingIntervalMs > 0
        ? config.pacingIntervalMs
        : 0;

  const tableName = config.fairQueueHostPacingTableName || 'worker_host_pacing';

  await ensurePacingTableRest(accountId, databaseId, apiToken, tableName);

  const allowed = await tryClaimPacingTokenRest(
    accountId,
    databaseId,
    apiToken,
    hostname,
    hostIntervalMs,
    tableName
  );

  if (!allowed) {
    return -1;
  }

  return tryAcquireFairSlotOnceRest(
    accountId,
    databaseId,
    apiToken,
    hostname,
    ipHash,
    config
  );
};

const tryAcquireFairSlot = async (hostname, ipHash, config) => {
  if (!config?.accountId || !config.databaseId || !config.apiToken) {
    throw new Error('[Fair Queue D1-REST] Missing D1 REST configuration');
  }

  const { accountId, databaseId, apiToken } = config;
  const tableName = config.fairQueueTableName || 'upstream_slot_pool';
  const cooldownTableName = config.fairQueueCooldownTableName || 'upstream_ip_cooldown';

  await ensureSlotPoolRest(
    accountId,
    databaseId,
    apiToken,
    hostname,
    config.globalLimit,
    tableName,
    cooldownTableName
  );

  return tryAcquireWorkerSlotRest(hostname, ipHash, config);
};

const releaseFairSlot = async (slotId, config) => {
  if (!config?.accountId || !config.databaseId || !config.apiToken) {
    throw new Error('[Fair Queue D1-REST] Missing D1 REST configuration');
  }

  const { accountId, databaseId, apiToken } = config;
  const tableName = config.fairQueueTableName || 'upstream_slot_pool';
  const cooldownTableName = config.fairQueueCooldownTableName || 'upstream_ip_cooldown';

  const slotRowResult = await executeQuery(
    accountId,
    databaseId,
    apiToken,
    `SELECT hostname_pattern, ip_hash FROM ${tableName} WHERE id = ?`,
    [slotId]
  );

  const slotRow = slotRowResult.results?.[0] || null;
  if (!slotRow) {
    return;
  }

  const slotHostname = slotRow.hostname_pattern ?? slotRow.HOSTNAME_PATTERN ?? null;
  const slotIpHash = slotRow.ip_hash ?? slotRow.IP_HASH ?? null;

  if (config.ipCooldownEnabled && slotHostname && slotIpHash) {
    await executeQuery(
      accountId,
      databaseId,
      apiToken,
      `INSERT INTO ${cooldownTableName} (hostname_pattern, ip_hash, last_release_at, created_at)
       VALUES (?, ?, datetime('now'), datetime('now'))
       ON CONFLICT(hostname_pattern, ip_hash)
       DO UPDATE SET last_release_at = excluded.last_release_at`,
      [slotHostname, slotIpHash]
    );
  }

  await executeQuery(
    accountId,
    databaseId,
    apiToken,
    `UPDATE ${tableName}
     SET status = 'available', ip_hash = NULL, locked_at = NULL
     WHERE id = ?`,
    [slotId]
  );

  console.log(`[Fair Queue D1-REST] Released slot ${slotId}`);
};

const cleanupZombieSlots = async (config) => {
  if (!config?.accountId || !config.databaseId || !config.apiToken) {
    throw new Error('[Fair Queue D1-REST] Missing D1 REST configuration');
  }

  const { accountId, databaseId, apiToken } = config;
  const tableName = config.fairQueueTableName || 'upstream_slot_pool';

  const result = await executeQuery(
    accountId,
    databaseId,
    apiToken,
    `UPDATE ${tableName}
     SET status = 'available', ip_hash = NULL, locked_at = NULL
     WHERE status = 'locked'
       AND locked_at IS NOT NULL
       AND locked_at < datetime('now', ?)`,
    [`-${config.zombieTimeoutSeconds} seconds`]
  );

  const recovered = result.meta?.changes ?? 0;
  if (recovered > 0) {
    console.log(`[Fair Queue D1-REST] Recovered ${recovered} zombie slots`);
  }
  return recovered;
};

const cleanupIpCooldown = async (config) => {
  if (!config?.accountId || !config.databaseId || !config.apiToken) {
    throw new Error('[Fair Queue D1-REST] Missing D1 REST configuration');
  }

  if (!config?.ipCooldownEnabled) {
    return 0;
  }

  const ttlSeconds = Number.isFinite(config?.ipCooldownCleanupTtlSeconds)
    ? config.ipCooldownCleanupTtlSeconds
    : Math.max((config?.ipCooldownSeconds || 0) * 10, 60);

  if (ttlSeconds <= 0) {
    return 0;
  }

  const { accountId, databaseId, apiToken } = config;
  const tableName = config.fairQueueCooldownTableName || 'upstream_ip_cooldown';

  const result = await executeQuery(
    accountId,
    databaseId,
    apiToken,
    `DELETE FROM ${tableName}
     WHERE last_release_at < datetime('now', ?)`,
    [`-${ttlSeconds} seconds`]
  );

  const deleted = result.meta?.changes ?? 0;
  if (deleted > 0) {
    console.log(`[Fair Queue D1-REST] Cleaned up ${deleted} cooldown records`);
  }
  return deleted;
};

const queueDepthEnforced = (config) =>
  Number.isFinite(config?.maxWaitersPerIp) && config.maxWaitersPerIp > 0;

const tryRegisterQueueWaiter = async (hostname, ipHash, config) => {
  if (!config?.accountId || !config.databaseId || !config.apiToken) {
    throw new Error('[Fair Queue D1-REST] Missing D1 REST configuration');
  }

  if (!queueDepthEnforced(config) || !ipHash) {
    return true;
  }

  const { accountId, databaseId, apiToken } = config;
  const tableName = config.fairQueueQueueDepthTableName || 'upstream_ip_queue_depth';
  await ensureQueueDepthTableRest(accountId, databaseId, apiToken, tableName);
  const zombieTtlSeconds = Number.isFinite(config?.queueDepthZombieTtlSeconds)
    ? Math.max(1, config.queueDepthZombieTtlSeconds)
    : 20;
  const zombieCutoff = `-${zombieTtlSeconds} seconds`;

  const reviveZombieQueue = async () => {
    const result = await executeQuery(
      accountId,
      databaseId,
      apiToken,
      `UPDATE ${tableName}
       SET waiting_count = 1,
           updated_at = datetime('now')
       WHERE hostname_pattern = ?
         AND ip_hash = ?
         AND updated_at < datetime('now', ?)`,
      [hostname, ipHash, zombieCutoff]
    );
    return result.meta?.changes ?? 0;
  };

  const attemptUpdate = async () => {
    const result = await executeQuery(
      accountId,
      databaseId,
      apiToken,
      `UPDATE ${tableName}
       SET waiting_count = waiting_count + 1,
           updated_at = datetime('now')
       WHERE hostname_pattern = ?
         AND ip_hash = ?
         AND waiting_count < ?`,
      [hostname, ipHash, config.maxWaitersPerIp]
    );
    return result.meta?.changes ?? 0;
  };

  let changes = await reviveZombieQueue();
  if (changes > 0) {
    return true;
  }

  changes = await attemptUpdate();
  if (changes > 0) {
    return true;
  }

  try {
    const insertResult = await executeQuery(
      accountId,
      databaseId,
      apiToken,
      `INSERT INTO ${tableName} (hostname_pattern, ip_hash, waiting_count, updated_at)
       VALUES (?, ?, 1, datetime('now'))`,
      [hostname, ipHash]
    );
    changes = insertResult.meta?.changes ?? 0;
    if (changes > 0) {
      return true;
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (!/UNIQUE/i.test(message)) {
      throw error;
    }
  }

  changes = await attemptUpdate();
  return changes > 0;
};

const releaseQueueWaiter = async (hostname, ipHash, config) => {
  if (!config?.accountId || !config.databaseId || !config.apiToken) {
    throw new Error('[Fair Queue D1-REST] Missing D1 REST configuration');
  }

  if (!ipHash) {
    return;
  }

  const { accountId, databaseId, apiToken } = config;
  const tableName = config.fairQueueQueueDepthTableName || 'upstream_ip_queue_depth';
  await ensureQueueDepthTableRest(accountId, databaseId, apiToken, tableName);

  await executeQuery(
    accountId,
    databaseId,
    apiToken,
    `UPDATE ${tableName}
     SET waiting_count = CASE
       WHEN waiting_count > 0 THEN waiting_count - 1
       ELSE 0
     END,
     updated_at = datetime('now')
     WHERE hostname_pattern = ?
       AND ip_hash = ?`,
    [hostname, ipHash]
  );
};

const cleanupQueueDepth = async (config) => {
  if (!config?.accountId || !config.databaseId || !config.apiToken) {
    throw new Error('[Fair Queue D1-REST] Missing D1 REST configuration');
  }

  const ttlSeconds = Number.isFinite(config?.queueDepthCleanupTtlSeconds)
    ? config.queueDepthCleanupTtlSeconds
    : Math.max((config?.ipCooldownSeconds || 0) * 10, 60);

  if (ttlSeconds <= 0) {
    return 0;
  }

  const { accountId, databaseId, apiToken } = config;
  const tableName = config.fairQueueQueueDepthTableName || 'upstream_ip_queue_depth';
  await ensureQueueDepthTableRest(accountId, databaseId, apiToken, tableName);

  const result = await executeQuery(
    accountId,
    databaseId,
    apiToken,
    `DELETE FROM ${tableName}
     WHERE updated_at < datetime('now', ?)`,
    [`-${ttlSeconds} seconds`]
  );

  const deleted = result.meta?.changes ?? 0;
  if (deleted > 0) {
    console.log(`[Fair Queue D1-REST] Cleaned up ${deleted} queue depth records`);
  }
  return deleted;
};

export {
  ensureAllTables,
  updateLastActive,
  tryAcquireFairSlot,
  releaseFairSlot,
  cleanupZombieSlots,
  cleanupIpCooldown,
  tryRegisterQueueWaiter,
  releaseQueueWaiter,
  cleanupQueueDepth,
  cleanupHostPacing,
};
