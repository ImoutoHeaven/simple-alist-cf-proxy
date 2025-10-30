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
 * Ensure cache, rate limit, and throttle tables exist using a single batch call
 * @param {D1Database} db
 * @param {Object} tableNames
 * @param {string} tableNames.cacheTableName
 * @param {string} tableNames.rateLimitTableName
 * @param {string} tableNames.throttleTableName
 * @param {string} tableNames.fileQuotaTableName
 * @param {string} tableNames.globalQuotaTableName
 * @returns {Promise<void>}
 */
const ensureAllTables = async (
  db,
  { cacheTableName, rateLimitTableName, throttleTableName, fileQuotaTableName, globalQuotaTableName }
) => {
  const statements = [
    db.prepare(`
      CREATE TABLE IF NOT EXISTS ${cacheTableName} (
        PATH_HASH TEXT PRIMARY KEY,
        PATH TEXT NOT NULL,
        LINK_DATA TEXT NOT NULL,
        TIMESTAMP INTEGER NOT NULL,
        HOSTNAME_HASH TEXT
      )
    `),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_cache_hostname ON ${cacheTableName}(HOSTNAME_HASH)`),
    db.prepare(`
      CREATE TABLE IF NOT EXISTS ${rateLimitTableName} (
        IP_HASH TEXT PRIMARY KEY,
        IP_RANGE TEXT NOT NULL,
        ACCESS_COUNT INTEGER NOT NULL,
        LAST_WINDOW_TIME INTEGER NOT NULL,
        BLOCK_UNTIL INTEGER
      )
    `),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_rate_limit_window ON ${rateLimitTableName}(LAST_WINDOW_TIME)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_rate_limit_block ON ${rateLimitTableName}(BLOCK_UNTIL) WHERE BLOCK_UNTIL IS NOT NULL`),
    db.prepare(`
      CREATE TABLE IF NOT EXISTS ${throttleTableName} (
        HOSTNAME_HASH TEXT PRIMARY KEY,
        HOSTNAME TEXT NOT NULL,
        ERROR_TIMESTAMP INTEGER,
        IS_PROTECTED INTEGER,
        LAST_ERROR_CODE INTEGER
      )
    `),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_throttle_timestamp ON ${throttleTableName}(ERROR_TIMESTAMP)`),
    db.prepare(`
      CREATE TABLE IF NOT EXISTS ${fileQuotaTableName} (
        ip_range_hash TEXT,
        filepath_hash TEXT,
        bytes_downloaded BIGINT DEFAULT 0,
        last_window_time INTEGER NOT NULL,
        blocked_until INTEGER,
        PRIMARY KEY (ip_range_hash, filepath_hash)
      )
    `),
    db.prepare(`
      CREATE TABLE IF NOT EXISTS ${globalQuotaTableName} (
        ip_range_hash TEXT PRIMARY KEY,
        total_bytes_downloaded BIGINT DEFAULT 0,
        last_window_time INTEGER NOT NULL,
        blocked_until INTEGER
      )
    `),
  ];

  await db.batch(statements);
};

/**
 * Unified check for D1 binding (RTT 3→1 optimization)
 * Uses db.batch() to execute Rate Limit + Cache + Throttle in a single transaction
 * @param {string} path - File path
 * @param {string} clientIP - Client IP address
 * @param {Object} config - Configuration object
 * @returns {Promise<{cache, rateLimit, throttle}>}
 */
export const unifiedCheckD1 = async (path, clientIP, config) => {
  if (!config.env || !config.databaseBinding) {
    throw new Error('[Unified Check D1] Missing D1 configuration');
  }

  const db = config.env[config.databaseBinding];
  if (!db) {
    throw new Error(`[Unified Check D1] D1 database binding '${config.databaseBinding}' not found`);
  }

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

  await ensureAllTables(db, {
    cacheTableName,
    rateLimitTableName,
    throttleTableName,
    fileQuotaTableName,
    globalQuotaTableName,
  });

  console.log('[Unified Check D1] Starting unified check for path:', path);

  // Calculate hashes
  const pathHash = await sha256Hash(path);
  if (!pathHash) {
    throw new Error('[Unified Check D1] Failed to calculate path hash');
  }

  const ipSubnet = calculateIPSubnet(clientIP, ipv4Suffix, ipv6Suffix);
  if (!ipSubnet) {
    throw new Error('[Unified Check D1] Failed to calculate IP subnet');
  }

  const ipHash = await sha256Hash(ipSubnet);
  if (!ipHash) {
    throw new Error('[Unified Check D1] Failed to calculate IP hash');
  }

  // Prepare batch queries
  const statements = [];
  const statementLabels = [];

  // 1. Cache SELECT
  const cacheSql = `SELECT LINK_DATA, TIMESTAMP, HOSTNAME_HASH FROM ${cacheTableName} WHERE PATH_HASH = ?`;
  statements.push(db.prepare(cacheSql).bind(pathHash));
  statementLabels.push('cache');

  // 2. Rate Limit UPSERT
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

  statements.push(
    db.prepare(rateLimitSql).bind(
      ipHash, ipSubnet, now,
      now, windowSeconds, now, limit,
      now, windowSeconds, now, now, now,
      now, windowSeconds, now, limit, blockSeconds, now, blockSeconds
    )
  );
  statementLabels.push('rate limit');

  // 3. File quota tracking
  let fileQuotaStatementIndex = null;
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
    fileQuotaStatementIndex = statements.length;
    statements.push(db.prepare(fileQuotaSql).bind(...fileQuotaParams));
    statementLabels.push('file quota');
  }

  // 4. Global quota tracking
  let globalQuotaStatementIndex = null;
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
    globalQuotaStatementIndex = statements.length;
    statements.push(db.prepare(globalQuotaSql).bind(...globalQuotaParams));
    statementLabels.push('global quota');
  }

  // Execute batch (single RTT!)
  console.log(`[Unified Check D1] Executing batch (${statements.length} queries in 1 RTT: ${statementLabels.join(' + ')})`);
  const results = await db.batch(statements);

  if (!results || results.length !== statements.length) {
    throw new Error('[Unified Check D1] Batch returned incomplete results');
  }

  // Parse cache result
  let cacheResult = {
    hit: false,
    linkData: null,
    timestamp: null,
    hostnameHash: null,
  };

  const cacheRow = results[0].results?.[0];
  if (cacheRow) {
    const age = now - parseInt(cacheRow.TIMESTAMP, 10);
    if (age <= cacheTTL) {
      try {
        cacheResult.hit = true;
        cacheResult.linkData = JSON.parse(cacheRow.LINK_DATA);
        cacheResult.timestamp = cacheRow.TIMESTAMP;
        cacheResult.hostnameHash = cacheRow.HOSTNAME_HASH || null;
        console.log('[Unified Check D1] Cache HIT for path:', path);
      } catch (error) {
        console.error('[Unified Check D1] Failed to parse cache link data:', error.message);
      }
    } else {
      console.log('[Unified Check D1] Cache expired (age:', age, 's)');
    }
  } else {
    console.log('[Unified Check D1] Cache MISS for path:', path);
  }

  // Parse rate limit result
  const rateLimitRow = results[1].results?.[0];
  if (!rateLimitRow) {
    throw new Error('[Unified Check D1] Rate limit UPSERT returned no rows');
  }

  const accessCount = parseInt(rateLimitRow.ACCESS_COUNT, 10);
  const lastWindowTime = parseInt(rateLimitRow.LAST_WINDOW_TIME, 10);
  const blockUntil = rateLimitRow.BLOCK_UNTIL ? parseInt(rateLimitRow.BLOCK_UNTIL, 10) : null;

  let rateLimitAllowed = true;
  let rateLimitRetryAfter = 0;

  if (blockUntil && blockUntil > now) {
    rateLimitAllowed = false;
    rateLimitRetryAfter = blockUntil - now;
    console.log('[Unified Check D1] Rate limit BLOCKED until:', new Date(blockUntil * 1000).toISOString());
  } else if (accessCount >= limit) {
    const diff = now - lastWindowTime;
    rateLimitRetryAfter = windowSeconds - diff;
    rateLimitAllowed = false;
    console.log('[Unified Check D1] Rate limit EXCEEDED:', accessCount, '>=', limit);
  } else {
    console.log('[Unified Check D1] Rate limit OK:', accessCount, '/', limit);
  }

  const rateLimitResult = {
    allowed: rateLimitAllowed,
    accessCount,
    lastWindowTime,
    blockUntil,
    retryAfter: rateLimitRetryAfter,
    ipSubnet,
  };

  let fileQuotaSummary = null;
  if (fileQuotaStatementIndex !== null) {
    const fileQuotaRow = results[fileQuotaStatementIndex].results?.[0];
    if (!fileQuotaRow) {
      console.warn('[Unified Check D1] File quota statement returned no rows');
    } else {
      const bytesDownloadedRaw = parseInt(fileQuotaRow.BYTES_DOWNLOADED, 10);
      const lastWindowRaw = parseInt(fileQuotaRow.LAST_WINDOW_TIME, 10);
      const blockedRaw = fileQuotaRow.BLOCKED_UNTIL !== null && fileQuotaRow.BLOCKED_UNTIL !== undefined
        ? parseInt(fileQuotaRow.BLOCKED_UNTIL, 10)
        : null;
      const bytesDownloaded = Number.isNaN(bytesDownloadedRaw) ? 0 : bytesDownloadedRaw;
      const lastWindow = Number.isNaN(lastWindowRaw) ? now : lastWindowRaw;
      const blockedUntil = blockedRaw !== null && !Number.isNaN(blockedRaw) ? blockedRaw : null;
      fileQuotaSummary = {
        bytesDownloaded,
        lastWindowTime: lastWindow,
        blockedUntil,
        exceeded: blockedUntil !== null && blockedUntil > now,
      };
    }
  }

  let globalQuotaSummary = null;
  if (globalQuotaStatementIndex !== null) {
    const globalQuotaRow = results[globalQuotaStatementIndex].results?.[0];
    if (!globalQuotaRow) {
      console.warn('[Unified Check D1] Global quota statement returned no rows');
    } else {
      const totalBytesRaw = parseInt(globalQuotaRow.TOTAL_BYTES_DOWNLOADED, 10);
      const lastWindowRaw = parseInt(globalQuotaRow.LAST_WINDOW_TIME, 10);
      const blockedRaw = globalQuotaRow.BLOCKED_UNTIL !== null && globalQuotaRow.BLOCKED_UNTIL !== undefined
        ? parseInt(globalQuotaRow.BLOCKED_UNTIL, 10)
        : null;
      const totalBytesDownloaded = Number.isNaN(totalBytesRaw) ? 0 : totalBytesRaw;
      const lastWindow = Number.isNaN(lastWindowRaw) ? now : lastWindowRaw;
      const blockedUntil = blockedRaw !== null && !Number.isNaN(blockedRaw) ? blockedRaw : null;
      globalQuotaSummary = {
        bytesDownloaded: totalBytesDownloaded,
        lastWindowTime: lastWindow,
        blockedUntil,
        exceeded: blockedUntil !== null && blockedUntil > now,
      };
    }
  }

  // Parse throttle result (only if cache hit with hostname_hash)
  // BREAKING CHANGE: IS_PROTECTED semantics
  //   1 = protected (error detected)
  //   0 = normal operation (initialized or recovered)
  //   NULL = record does not exist
  let throttleResult = {
    status: 'normal_operation',
    recordExists: false,
    isProtected: null,
    errorTimestamp: null,
    errorCode: null,
    retryAfter: 0,
  };

  if (cacheResult.hostnameHash) {
    // Throttle query (conditional - only when cache hit)
    const throttleSql = `SELECT IS_PROTECTED, ERROR_TIMESTAMP, LAST_ERROR_CODE FROM ${throttleTableName} WHERE HOSTNAME_HASH = ?`;
    const throttleQueryResult = await db.prepare(throttleSql).bind(cacheResult.hostnameHash).first();

    if (throttleQueryResult) {
      throttleResult.recordExists = true;
      throttleResult.isProtected = throttleQueryResult.IS_PROTECTED;
      throttleResult.errorTimestamp = throttleQueryResult.ERROR_TIMESTAMP;
      throttleResult.errorCode = throttleQueryResult.LAST_ERROR_CODE;

      if (throttleQueryResult.IS_PROTECTED === 1) {
        const errorTimestamp = parseInt(throttleQueryResult.ERROR_TIMESTAMP, 10);
        const timeSinceError = now - errorTimestamp;

        if (timeSinceError < throttleTimeWindow) {
          throttleResult.status = 'protected';
          throttleResult.retryAfter = throttleTimeWindow - timeSinceError;
          console.log('[Unified Check D1] Throttle PROTECTED, retry after:', throttleResult.retryAfter);
        } else {
          throttleResult.status = 'resume_operation';
          console.log('[Unified Check D1] Throttle resume_operation (time window expired)');
        }
      } else if (throttleQueryResult.IS_PROTECTED === 0) {
        console.log('[Unified Check D1] Throttle normal_operation (IS_PROTECTED = 0)');
      } else {
        console.log('[Unified Check D1] Throttle normal_operation (IS_PROTECTED = NULL, invalid state)');
      }
    } else {
      console.log('[Unified Check D1] Throttle normal_operation (no record found)');
    }
  } else {
    console.log('[Unified Check D1] Skipping throttle check (no hostname_hash from cache)');
  }

  console.log('[Unified Check D1] Completed successfully');

  return {
    cache: cacheResult,
    rateLimit: rateLimitResult,
    throttle: throttleResult,
    fileQuota: fileQuotaSummary,
    globalQuota: globalQuotaSummary,
  };
};
