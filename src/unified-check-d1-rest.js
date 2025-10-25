import { sha256Hash, calculateIPSubnet } from './utils.js';

/**
 * Execute SQL query via D1 REST API
 */
const executeQuery = async (accountId, databaseId, apiToken, sql, params = []) => {
  const endpoint = `https://api.cloudflare.com/client/v4/accounts/${accountId}/d1/database/${databaseId}/query`;

  const body = { sql };
  if (params && params.length > 0) {
    body.params = params;
  }

  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${apiToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
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
 * Ensure cache, rate limit, and throttle tables exist before issuing queries
 * @param {string} accountId
 * @param {string} databaseId
 * @param {string} apiToken
 * @param {Object} tableNames
 * @param {string} tableNames.cacheTableName
 * @param {string} tableNames.rateLimitTableName
 * @param {string} tableNames.throttleTableName
 * @returns {Promise<void>}
 */
const ensureAllTables = async (accountId, databaseId, apiToken, { cacheTableName, rateLimitTableName, throttleTableName }) => {
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
};

/**
 * Unified check for D1-REST (RTT 3→1-2 optimization)
 * Uses D1 REST API to execute multiple queries
 * @param {string} path - File path
 * @param {string} clientIP - Client IP address
 * @param {Object} config - Configuration object
 * @returns {Promise<{cache, rateLimit, throttle}>}
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
  const throttleTimeWindow = config.throttleTimeWindow ?? 60;
  const ipv4Suffix = config.ipv4Suffix ?? '/32';
  const ipv6Suffix = config.ipv6Suffix ?? '/60';

  await ensureAllTables(accountId, databaseId, apiToken, { cacheTableName, rateLimitTableName, throttleTableName });

  console.log('[Unified Check D1-REST] Starting unified check for path:', path);

  // Calculate hashes
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

  // Execute queries (D1 REST API)
  console.log('[Unified Check D1-REST] Executing cache check');
  const cacheSql = `SELECT LINK_DATA, TIMESTAMP, HOSTNAME_HASH FROM ${cacheTableName} WHERE PATH_HASH = ?`;
  const cacheResult = await executeQuery(accountId, databaseId, apiToken, cacheSql, [pathHash]);

  console.log('[Unified Check D1-REST] Executing rate limit UPSERT');
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
    now, windowSeconds, now, limit, blockSeconds, now, blockSeconds
  ];
  const rateLimitResult = await executeQuery(accountId, databaseId, apiToken, rateLimitSql, rateLimitParams);

  // Parse cache
  let cacheData = {
    hit: false,
    linkData: null,
    timestamp: null,
    hostnameHash: null,
  };

  const cacheRow = cacheResult.results?.[0];
  if (cacheRow) {
    const age = now - parseInt(cacheRow.TIMESTAMP, 10);
    if (age <= cacheTTL) {
      try {
        cacheData.hit = true;
        cacheData.linkData = JSON.parse(cacheRow.LINK_DATA);
        cacheData.timestamp = cacheRow.TIMESTAMP;
        cacheData.hostnameHash = cacheRow.HOSTNAME_HASH || null;
        console.log('[Unified Check D1-REST] Cache HIT');
      } catch (error) {
        console.error('[Unified Check D1-REST] Failed to parse cache:', error.message);
      }
    } else {
      console.log('[Unified Check D1-REST] Cache expired (age:', age, 's)');
    }
  } else {
    console.log('[Unified Check D1-REST] Cache MISS');
  }

  // Parse rate limit
  const rateLimitRow = rateLimitResult.results?.[0];
  if (!rateLimitRow) {
    throw new Error('[Unified Check D1-REST] Rate limit UPSERT returned no rows');
  }

  const accessCount = parseInt(rateLimitRow.ACCESS_COUNT, 10);
  const lastWindowTime = parseInt(rateLimitRow.LAST_WINDOW_TIME, 10);
  const blockUntil = rateLimitRow.BLOCK_UNTIL ? parseInt(rateLimitRow.BLOCK_UNTIL, 10) : null;

  let rateLimitAllowed = true;
  let rateLimitRetryAfter = 0;

  if (blockUntil && blockUntil > now) {
    rateLimitAllowed = false;
    rateLimitRetryAfter = blockUntil - now;
    console.log('[Unified Check D1-REST] Rate limit BLOCKED until:', new Date(blockUntil * 1000).toISOString());
  } else if (accessCount >= limit) {
    rateLimitAllowed = false;
    rateLimitRetryAfter = windowSeconds - (now - lastWindowTime);
    console.log('[Unified Check D1-REST] Rate limit EXCEEDED:', accessCount, '>=', limit);
  } else {
    console.log('[Unified Check D1-REST] Rate limit OK:', accessCount, '/', limit);
  }

  const rateLimitData = {
    allowed: rateLimitAllowed,
    accessCount,
    lastWindowTime,
    blockUntil,
    retryAfter: rateLimitRetryAfter,
    ipSubnet,
  };

  // Throttle check (if cache hit with hostname_hash)
  let throttleData = {
    status: 'normal_operation',
    recordExists: false,
    isProtected: null,
    errorTimestamp: null,
    errorCode: null,
    retryAfter: 0,
  };

  if (cacheData.hostnameHash) {
    console.log('[Unified Check D1-REST] Executing throttle check');
    const throttleSql = `SELECT IS_PROTECTED, ERROR_TIMESTAMP, LAST_ERROR_CODE FROM ${throttleTableName} WHERE HOSTNAME_HASH = ?`;
    const throttleResult = await executeQuery(accountId, databaseId, apiToken, throttleSql, [cacheData.hostnameHash]);
    const throttleRow = throttleResult.results?.[0];

    if (throttleRow) {
      throttleData.recordExists = true;
      throttleData.isProtected = throttleRow.IS_PROTECTED;
      throttleData.errorTimestamp = throttleRow.ERROR_TIMESTAMP;
      throttleData.errorCode = throttleRow.LAST_ERROR_CODE;

      if (throttleRow.IS_PROTECTED === 1) {
        const timeSinceError = now - parseInt(throttleRow.ERROR_TIMESTAMP, 10);
        if (timeSinceError < throttleTimeWindow) {
          throttleData.status = 'protected';
          throttleData.retryAfter = throttleTimeWindow - timeSinceError;
          console.log('[Unified Check D1-REST] Throttle PROTECTED, retry after:', throttleData.retryAfter);
        } else {
          throttleData.status = 'resume_operation';
          console.log('[Unified Check D1-REST] Throttle resume_operation');
        }
      }
    }
  } else {
    console.log('[Unified Check D1-REST] Skipping throttle check (no hostname_hash from cache)');
  }

  console.log('[Unified Check D1-REST] Completed');

  return {
    cache: cacheData,
    rateLimit: rateLimitData,
    throttle: throttleData,
  };
};
