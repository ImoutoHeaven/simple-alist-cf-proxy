import { sha256Hash, calculateIPSubnet } from './utils.js';

const toSafeInteger = (value) => {
  if (value === null || value === undefined) {
    return null;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return null;
  }
  return Math.max(0, Math.trunc(numeric));
};

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
 * Execute multiple SQL queries via a single D1 REST API batch call.
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
    body: JSON.stringify(statements),
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
 * Ensure cache, rate limit, and throttle tables exist before issuing queries
 * @param {string} accountId
 * @param {string} databaseId
 * @param {string} apiToken
 * @param {Object} tableNames
 * @param {string} tableNames.cacheTableName
 * @param {string} tableNames.rateLimitTableName
 * @param {string} tableNames.throttleTableName
 * @param {string} tableNames.fileQuotaTableName
 * @param {string} tableNames.globalQuotaTableName
 * @returns {Promise<void>}
 */
const ensureAllTables = async (
  accountId,
  databaseId,
  apiToken,
  { cacheTableName, rateLimitTableName, throttleTableName, fileQuotaTableName, globalQuotaTableName }
) => {
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

  await executeQuery(accountId, databaseId, apiToken, `
    CREATE TABLE IF NOT EXISTS ${fileQuotaTableName} (
      ip_range_hash TEXT,
      filepath_hash TEXT,
      bytes_downloaded BIGINT DEFAULT 0,
      last_window_time INTEGER NOT NULL,
      blocked_until INTEGER,
      PRIMARY KEY (ip_range_hash, filepath_hash)
    )
  `);

  await executeQuery(accountId, databaseId, apiToken, `
    CREATE TABLE IF NOT EXISTS ${globalQuotaTableName} (
      ip_range_hash TEXT PRIMARY KEY,
      total_bytes_downloaded BIGINT DEFAULT 0,
      last_window_time INTEGER NOT NULL,
      blocked_until INTEGER
    )
  `);
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
  const fileQuotaTableName = config.fileQuotaTableName || 'file_ip_download_quota';
  const globalQuotaTableName = config.globalQuotaTableName || 'global_ip_download_quota';
  const throttleTimeWindow = config.throttleTimeWindow ?? 60;
  const ipv4Suffix = config.ipv4Suffix ?? '/32';
  const ipv6Suffix = config.ipv6Suffix ?? '/60';
  const filepathHash = typeof config.filepathHash === 'string' && config.filepathHash.length > 0 ? config.filepathHash : null;
  const deductBytes = toSafeInteger(config.deductBytes) ?? 0;
  const fileQuotaWindowSeconds = toSafeInteger(config.fileQuotaWindowSeconds);
  const fileQuotaMaxBytes = toSafeInteger(config.fileQuotaMaxBytes);
  const fileQuotaBlockSeconds = toSafeInteger(config.fileQuotaBlockSeconds);
  const fileQuotaEnabled = Boolean(
    filepathHash &&
    fileQuotaTableName &&
    fileQuotaWindowSeconds !== null &&
    fileQuotaWindowSeconds > 0 &&
    fileQuotaMaxBytes !== null &&
    fileQuotaMaxBytes > 0 &&
    fileQuotaBlockSeconds !== null
  );
  const globalQuotaWindowSeconds = toSafeInteger(config.globalQuotaWindowSeconds);
  const globalQuotaMaxBytes = toSafeInteger(config.globalQuotaMaxBytes);
  const globalQuotaBlockSeconds = toSafeInteger(config.globalQuotaBlockSeconds);
  const globalQuotaEnabled = Boolean(
    globalQuotaTableName &&
    globalQuotaWindowSeconds !== null &&
    globalQuotaWindowSeconds > 0 &&
    globalQuotaMaxBytes !== null &&
    globalQuotaMaxBytes > 0 &&
    globalQuotaBlockSeconds !== null
  );

  await ensureAllTables(accountId, databaseId, apiToken, {
    cacheTableName,
    rateLimitTableName,
    throttleTableName,
    fileQuotaTableName,
    globalQuotaTableName,
  });

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

  const cacheSql = `SELECT LINK_DATA, TIMESTAMP, HOSTNAME_HASH FROM ${cacheTableName} WHERE PATH_HASH = ?`;
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

  const throttleSql = `
    SELECT IS_PROTECTED, ERROR_TIMESTAMP, LAST_ERROR_CODE
    FROM ${throttleTableName} AS throttle
    WHERE throttle.HOSTNAME_HASH = (
      SELECT cache.HOSTNAME_HASH
      FROM ${cacheTableName} AS cache
      WHERE cache.PATH_HASH = ?
      LIMIT 1
    )
      AND throttle.HOSTNAME_HASH IS NOT NULL
  `;

  const statements = [];
  const statementLabels = [];

  const cacheIndex = statements.length;
  statements.push({ sql: cacheSql, params: [pathHash] });
  statementLabels.push('cache');

  const rateLimitIndex = statements.length;
  statements.push({ sql: rateLimitSql, params: rateLimitParams });
  statementLabels.push('rate limit');

  let fileQuotaIndex = null;
  if (fileQuotaEnabled) {
    const fileQuotaBlockUntil = fileQuotaBlockSeconds > 0 ? now + fileQuotaBlockSeconds : now;
    const fileQuotaSql = `
      INSERT INTO ${fileQuotaTableName} (ip_range_hash, filepath_hash, bytes_downloaded, last_window_time, blocked_until)
      VALUES (?, ?, ?, ?, CASE WHEN ? > ? THEN ? ELSE NULL END)
      ON CONFLICT (ip_range_hash, filepath_hash) DO UPDATE SET
        bytes_downloaded = CASE
          WHEN ${fileQuotaTableName}.blocked_until IS NOT NULL AND ${fileQuotaTableName}.blocked_until > ? THEN ${fileQuotaTableName}.bytes_downloaded
          WHEN ? - ${fileQuotaTableName}.last_window_time >= ? THEN excluded.bytes_downloaded
          ELSE ${fileQuotaTableName}.bytes_downloaded + excluded.bytes_downloaded
        END,
        last_window_time = CASE
          WHEN ${fileQuotaTableName}.blocked_until IS NOT NULL AND ${fileQuotaTableName}.blocked_until > ? THEN ${fileQuotaTableName}.last_window_time
          WHEN ? - ${fileQuotaTableName}.last_window_time >= ? THEN ?
          ELSE ${fileQuotaTableName}.last_window_time
        END,
        blocked_until = CASE
          WHEN ${fileQuotaTableName}.blocked_until IS NOT NULL AND ${fileQuotaTableName}.blocked_until > ? THEN ${fileQuotaTableName}.blocked_until
          WHEN ? - ${fileQuotaTableName}.last_window_time >= ? THEN
            CASE
              WHEN excluded.bytes_downloaded > ? THEN ?
              ELSE NULL
            END
          WHEN ${fileQuotaTableName}.bytes_downloaded + excluded.bytes_downloaded > ? THEN ?
          ELSE NULL
        END
      RETURNING
        bytes_downloaded AS BYTES_DOWNLOADED,
        last_window_time AS LAST_WINDOW_TIME,
        blocked_until AS BLOCKED_UNTIL
    `;
    const fileQuotaParams = [
      ipHash,
      filepathHash,
      deductBytes,
      now,
      deductBytes,
      fileQuotaMaxBytes,
      fileQuotaBlockUntil,
      now,
      now,
      fileQuotaWindowSeconds,
      now,
      now,
      fileQuotaWindowSeconds,
      now,
      now,
      now,
      fileQuotaWindowSeconds,
      fileQuotaMaxBytes,
      fileQuotaBlockUntil,
      fileQuotaMaxBytes,
      fileQuotaBlockUntil,
    ];
    fileQuotaIndex = statements.length;
    statements.push({ sql: fileQuotaSql, params: fileQuotaParams });
    statementLabels.push('file quota');
  }

  let globalQuotaIndex = null;
  if (globalQuotaEnabled) {
    const globalQuotaBlockUntil = globalQuotaBlockSeconds > 0 ? now + globalQuotaBlockSeconds : now;
    const globalQuotaSql = `
      INSERT INTO ${globalQuotaTableName} (ip_range_hash, total_bytes_downloaded, last_window_time, blocked_until)
      VALUES (?, ?, ?, CASE WHEN ? > ? THEN ? ELSE NULL END)
      ON CONFLICT (ip_range_hash) DO UPDATE SET
        total_bytes_downloaded = CASE
          WHEN ${globalQuotaTableName}.blocked_until IS NOT NULL AND ${globalQuotaTableName}.blocked_until > ? THEN ${globalQuotaTableName}.total_bytes_downloaded
          WHEN ? - ${globalQuotaTableName}.last_window_time >= ? THEN excluded.total_bytes_downloaded
          ELSE ${globalQuotaTableName}.total_bytes_downloaded + excluded.total_bytes_downloaded
        END,
        last_window_time = CASE
          WHEN ${globalQuotaTableName}.blocked_until IS NOT NULL AND ${globalQuotaTableName}.blocked_until > ? THEN ${globalQuotaTableName}.last_window_time
          WHEN ? - ${globalQuotaTableName}.last_window_time >= ? THEN ?
          ELSE ${globalQuotaTableName}.last_window_time
        END,
        blocked_until = CASE
          WHEN ${globalQuotaTableName}.blocked_until IS NOT NULL AND ${globalQuotaTableName}.blocked_until > ? THEN ${globalQuotaTableName}.blocked_until
          WHEN ? - ${globalQuotaTableName}.last_window_time >= ? THEN
            CASE
              WHEN excluded.total_bytes_downloaded > ? THEN ?
              ELSE NULL
            END
          WHEN ${globalQuotaTableName}.total_bytes_downloaded + excluded.total_bytes_downloaded > ? THEN ?
          ELSE NULL
        END
      RETURNING
        total_bytes_downloaded AS TOTAL_BYTES_DOWNLOADED,
        last_window_time AS LAST_WINDOW_TIME,
        blocked_until AS BLOCKED_UNTIL
    `;
    const globalQuotaParams = [
      ipHash,
      deductBytes,
      now,
      deductBytes,
      globalQuotaMaxBytes,
      globalQuotaBlockUntil,
      now,
      now,
      globalQuotaWindowSeconds,
      now,
      now,
      globalQuotaWindowSeconds,
      now,
      now,
      now,
      globalQuotaWindowSeconds,
      globalQuotaMaxBytes,
      globalQuotaBlockUntil,
      globalQuotaMaxBytes,
      globalQuotaBlockUntil,
    ];
    globalQuotaIndex = statements.length;
    statements.push({ sql: globalQuotaSql, params: globalQuotaParams });
    statementLabels.push('global quota');
  }

  const throttleIndex = statements.length;
  statements.push({ sql: throttleSql, params: [pathHash] });
  statementLabels.push('throttle');

  console.log(`[Unified Check D1-REST] Executing batch (${statements.length} queries in 1 RTT: ${statementLabels.join(' + ')})`);
  const batchResults = await executeBatchQueries(accountId, databaseId, apiToken, statements);

  const getStatementResult = (index, label) => {
    const statementResult = batchResults[index];
    if (!statementResult) {
      throw new Error(`[Unified Check D1-REST] Batch result missing for ${label}`);
    }
    if (statementResult.success === false) {
      throw new Error(`[Unified Check D1-REST] ${label} statement failed: ${JSON.stringify(statementResult.errors || 'Unknown error')}`);
    }
    return statementResult;
  };

  const cacheResult = getStatementResult(cacheIndex, 'cache');
  const rateLimitResult = getStatementResult(rateLimitIndex, 'rate limit');
  const throttleResult = getStatementResult(throttleIndex, 'throttle');
  const fileQuotaResult = fileQuotaIndex !== null ? getStatementResult(fileQuotaIndex, 'file quota') : null;
  const globalQuotaResult = globalQuotaIndex !== null ? getStatementResult(globalQuotaIndex, 'global quota') : null;

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

  let fileQuotaData = null;
  if (fileQuotaResult) {
    const quotaRow = fileQuotaResult.results?.[0];
    if (!quotaRow) {
      console.warn('[Unified Check D1-REST] File quota statement returned no rows');
    } else {
      const bytesDownloadedRaw = parseInt(quotaRow.BYTES_DOWNLOADED, 10);
      const lastWindowRaw = parseInt(quotaRow.LAST_WINDOW_TIME, 10);
      const blockedRaw = quotaRow.BLOCKED_UNTIL !== null && quotaRow.BLOCKED_UNTIL !== undefined
        ? parseInt(quotaRow.BLOCKED_UNTIL, 10)
        : null;
      const bytesDownloaded = Number.isNaN(bytesDownloadedRaw) ? 0 : bytesDownloadedRaw;
      const lastWindow = Number.isNaN(lastWindowRaw) ? now : lastWindowRaw;
      const blockedUntil = blockedRaw !== null && !Number.isNaN(blockedRaw) ? blockedRaw : null;
      fileQuotaData = {
        bytesDownloaded,
        lastWindowTime: lastWindow,
        blockedUntil,
        exceeded: blockedUntil !== null && blockedUntil > now,
      };
    }
  }

  let globalQuotaData = null;
  if (globalQuotaResult) {
    const quotaRow = globalQuotaResult.results?.[0];
    if (!quotaRow) {
      console.warn('[Unified Check D1-REST] Global quota statement returned no rows');
    } else {
      const totalBytesRaw = parseInt(quotaRow.TOTAL_BYTES_DOWNLOADED, 10);
      const lastWindowRaw = parseInt(quotaRow.LAST_WINDOW_TIME, 10);
      const blockedRaw = quotaRow.BLOCKED_UNTIL !== null && quotaRow.BLOCKED_UNTIL !== undefined
        ? parseInt(quotaRow.BLOCKED_UNTIL, 10)
        : null;
      const totalBytesDownloaded = Number.isNaN(totalBytesRaw) ? 0 : totalBytesRaw;
      const lastWindow = Number.isNaN(lastWindowRaw) ? now : lastWindowRaw;
      const blockedUntil = blockedRaw !== null && !Number.isNaN(blockedRaw) ? blockedRaw : null;
      globalQuotaData = {
        bytesDownloaded: totalBytesDownloaded,
        lastWindowTime: lastWindow,
        blockedUntil,
        exceeded: blockedUntil !== null && blockedUntil > now,
      };
    }
  }

  // Throttle check (if cache hit with hostname_hash)
  // BREAKING CHANGE: IS_PROTECTED semantics
  //   1 = protected (error detected)
  //   0 = normal operation (initialized or recovered)
  //   NULL = record does not exist
  let throttleData = {
    status: 'normal_operation',
    recordExists: false,
    isProtected: null,
    errorTimestamp: null,
    errorCode: null,
    retryAfter: 0,
  };

  if (cacheData.hostnameHash) {
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
      } else if (throttleRow.IS_PROTECTED === 0) {
        console.log('[Unified Check D1-REST] Throttle normal_operation (IS_PROTECTED = 0)');
      } else {
        console.log('[Unified Check D1-REST] Throttle normal_operation (IS_PROTECTED = NULL, invalid state)');
      }
    } else {
      console.log('[Unified Check D1-REST] Throttle normal_operation (no record found)');
    }
  } else {
    console.log('[Unified Check D1-REST] Skipping throttle check (no hostname_hash from cache)');
  }

  console.log('[Unified Check D1-REST] Completed');

  return {
    cache: cacheData,
    rateLimit: rateLimitData,
    throttle: throttleData,
    fileQuota: fileQuotaData,
    globalQuota: globalQuotaData,
  };
};

/**
 * Refund quota for non-GET or failed requests (D1-REST)
 * @param {string} accountId
 * @param {string} databaseId
 * @param {string} apiToken
 * @param {string} ipRangeHash
 * @param {string | null} filepathHash
 * @param {number} refundBytes
 * @param {Object} [config]
 * @returns {Promise<void>}
 */
export const refundQuotaD1Rest = async (
  accountId,
  databaseId,
  apiToken,
  ipRangeHash,
  filepathHash,
  refundBytes,
  config = {}
) => {
  if (!accountId || !databaseId || !apiToken || !ipRangeHash) {
    return;
  }

  const normalizedBytes = Number.isFinite(refundBytes) ? Math.max(0, Math.trunc(refundBytes)) : 0;
  if (normalizedBytes <= 0) {
    return;
  }

  const {
    fileQuotaTableName = 'file_ip_download_quota',
    globalQuotaTableName = 'global_ip_download_quota',
    fileQuotaEnabled,
    globalQuotaEnabled,
    fileQuotaWindowSeconds,
    fileQuotaMaxBytes,
    globalQuotaWindowSeconds,
    globalQuotaMaxBytes,
  } = config;

  const now = Math.floor(Date.now() / 1000);
  const statements = [];

  if (
    filepathHash &&
    fileQuotaEnabled &&
    fileQuotaWindowSeconds &&
    fileQuotaMaxBytes
  ) {
    const checkSql = `SELECT last_window_time FROM ${fileQuotaTableName} WHERE ip_range_hash = ? AND filepath_hash = ?`;
    const checkResult = await executeBatchQueries(accountId, databaseId, apiToken, [
      { sql: checkSql, params: [ipRangeHash, filepathHash] }
    ]);

    const record = checkResult[0]?.results?.[0];
    const lastWindowRaw = record ? record.last_window_time : null;
    const lastWindowTime = typeof lastWindowRaw === 'number'
      ? lastWindowRaw
      : Number.parseInt(lastWindowRaw, 10);

    if (Number.isFinite(lastWindowTime) && (now - lastWindowTime) < fileQuotaWindowSeconds) {
      const fileRefundSql = `
        UPDATE ${fileQuotaTableName}
        SET 
          bytes_downloaded = MAX(bytes_downloaded - ?, 0),
          blocked_until = CASE
            WHEN MAX(bytes_downloaded - ?, 0) < ? THEN NULL
            ELSE blocked_until
          END
        WHERE ip_range_hash = ? AND filepath_hash = ?
      `;
      statements.push({
        sql: fileRefundSql,
        params: [normalizedBytes, normalizedBytes, fileQuotaMaxBytes, ipRangeHash, filepathHash]
      });
    }
  }

  if (
    globalQuotaEnabled &&
    globalQuotaWindowSeconds &&
    globalQuotaMaxBytes
  ) {
    const checkSql = `SELECT last_window_time FROM ${globalQuotaTableName} WHERE ip_range_hash = ?`;
    const checkResult = await executeBatchQueries(accountId, databaseId, apiToken, [
      { sql: checkSql, params: [ipRangeHash] }
    ]);

    const record = checkResult[0]?.results?.[0];
    const lastWindowRaw = record ? record.last_window_time : null;
    const lastWindowTime = typeof lastWindowRaw === 'number'
      ? lastWindowRaw
      : Number.parseInt(lastWindowRaw, 10);

    if (Number.isFinite(lastWindowTime) && (now - lastWindowTime) < globalQuotaWindowSeconds) {
      const globalRefundSql = `
        UPDATE ${globalQuotaTableName}
        SET 
          total_bytes_downloaded = MAX(total_bytes_downloaded - ?, 0),
          blocked_until = CASE
            WHEN MAX(total_bytes_downloaded - ?, 0) < ? THEN NULL
            ELSE blocked_until
          END
        WHERE ip_range_hash = ?
      `;
      statements.push({
        sql: globalRefundSql,
        params: [normalizedBytes, normalizedBytes, globalQuotaMaxBytes, ipRangeHash]
      });
    }
  }

  if (statements.length === 0) {
    return;
  }

  await executeBatchQueries(accountId, databaseId, apiToken, statements);
  console.log(`[Quota Refund D1-REST] Refunded ${normalizedBytes} bytes for IP ${ipRangeHash}`);
};
