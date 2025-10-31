import { sha256Hash, calculateIPSubnet } from './utils.js';

/**
 * Ensure cache, rate limit, and throttle tables exist using a single batch call
 * @param {D1Database} db
 * @param {Object} tableNames
 * @param {string} tableNames.cacheTableName
 * @param {string} tableNames.rateLimitTableName
 * @param {string} tableNames.throttleTableName
 * @returns {Promise<void>}
 */
const ensureAllTables = async (db, {
  cacheTableName,
  rateLimitTableName,
  throttleTableName,
  bandwidthIprangeTableName,
  bandwidthFilepathTableName,
  ensureBandwidthIprange,
  ensureBandwidthFilepath,
}) => {
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
  ];

  if (ensureBandwidthIprange && bandwidthIprangeTableName) {
    statements.push(
      db.prepare(`
        CREATE TABLE IF NOT EXISTS ${bandwidthIprangeTableName} (
          IP_HASH TEXT PRIMARY KEY,
          IP_RANGE TEXT NOT NULL,
          BYTES_USED INTEGER NOT NULL DEFAULT 0,
          WINDOW_START INTEGER NOT NULL,
          BLOCK_UNTIL INTEGER
        )
      `),
      db.prepare(`CREATE INDEX IF NOT EXISTS idx_${bandwidthIprangeTableName.toLowerCase()}_window ON ${bandwidthIprangeTableName}(WINDOW_START)`),
      db.prepare(`CREATE INDEX IF NOT EXISTS idx_${bandwidthIprangeTableName.toLowerCase()}_block ON ${bandwidthIprangeTableName}(BLOCK_UNTIL) WHERE BLOCK_UNTIL IS NOT NULL`),
    );
  }

  if (ensureBandwidthFilepath && bandwidthFilepathTableName) {
    statements.push(
      db.prepare(`
        CREATE TABLE IF NOT EXISTS ${bandwidthFilepathTableName} (
          COMPOSITE_HASH TEXT PRIMARY KEY,
          IP_RANGE TEXT NOT NULL,
          FILEPATH TEXT NOT NULL,
          BYTES_USED INTEGER NOT NULL DEFAULT 0,
          WINDOW_START INTEGER NOT NULL,
          BLOCK_UNTIL INTEGER
        )
      `),
      db.prepare(`CREATE INDEX IF NOT EXISTS idx_${bandwidthFilepathTableName.toLowerCase()}_window ON ${bandwidthFilepathTableName}(WINDOW_START)`),
      db.prepare(`CREATE INDEX IF NOT EXISTS idx_${bandwidthFilepathTableName.toLowerCase()}_block ON ${bandwidthFilepathTableName}(BLOCK_UNTIL) WHERE BLOCK_UNTIL IS NOT NULL`),
    );
  }

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
  const throttleTimeWindow = config.throttleTimeWindow ?? 60;
  const ipv4Suffix = config.ipv4Suffix ?? '/32';
  const ipv6Suffix = config.ipv6Suffix ?? '/60';
  const bandwidthIprangeQuota = config.bandwidthIprangeQuota ?? 0;
  const bandwidthFilepathQuota = config.bandwidthFilepathQuota ?? 0;
  const bandwidthWindowTotalSeconds = config.bandwidthWindowTotalSeconds ?? 0;
  const bandwidthWindowFilepathSeconds = config.bandwidthWindowFilepathSeconds ?? 0;
  const bandwidthBlockSeconds = config.bandwidthBlockSeconds ?? 0;
  const bandwidthIprangeTableName = config.bandwidthIprangeTableName || 'IPRANGE_BANDWIDTH_QUOTA_TABLE';
  const bandwidthFilepathTableName = config.bandwidthFilepathTableName || 'IPRANGE_FILEPATH_BANDWIDTH_QUOTA_TABLE';
  const shouldCheckBandwidthIprange = bandwidthIprangeQuota > 0 && bandwidthWindowTotalSeconds > 0;
  const shouldCheckBandwidthFilepath = bandwidthFilepathQuota > 0 && bandwidthWindowFilepathSeconds > 0;

  await ensureAllTables(db, {
    cacheTableName,
    rateLimitTableName,
    throttleTableName,
    bandwidthIprangeTableName,
    bandwidthFilepathTableName,
    ensureBandwidthIprange: shouldCheckBandwidthIprange,
    ensureBandwidthFilepath: shouldCheckBandwidthFilepath,
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

  let compositeHash = null;
  if (shouldCheckBandwidthFilepath) {
    compositeHash = await sha256Hash(`${ipSubnet}${path}`);
  }

  // Prepare batch queries
  const statements = [];
  const statementIndexes = {};

  // 1. Cache SELECT
  const cacheSql = `SELECT LINK_DATA, TIMESTAMP, HOSTNAME_HASH FROM ${cacheTableName} WHERE PATH_HASH = ?`;
  statementIndexes.cache = statements.length;
  statements.push(db.prepare(cacheSql).bind(pathHash));

  // 2. Rate Limit UPSERT (same complex logic as d1.js)
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

  statementIndexes.rateLimit = statements.length;
  statements.push(
    db.prepare(rateLimitSql).bind(
      ipHash, ipSubnet, now,
      now, windowSeconds, now, limit,
      now, windowSeconds, now, now, now,
      now, windowSeconds, now, limit, blockSeconds, now, blockSeconds
    )
  );

  if (shouldCheckBandwidthIprange) {
    const iprangeSql = `SELECT BYTES_USED, WINDOW_START, BLOCK_UNTIL FROM ${bandwidthIprangeTableName} WHERE IP_HASH = ?`;
    statementIndexes.bandwidthIprange = statements.length;
    statements.push(db.prepare(iprangeSql).bind(ipHash));
  }

  if (shouldCheckBandwidthFilepath && compositeHash) {
    const filepathSql = `SELECT BYTES_USED, WINDOW_START, BLOCK_UNTIL FROM ${bandwidthFilepathTableName} WHERE COMPOSITE_HASH = ?`;
    statementIndexes.bandwidthFilepath = statements.length;
    statements.push(db.prepare(filepathSql).bind(compositeHash));
  }

  // Execute batch (single RTT!)
  console.log('[Unified Check D1] Executing batch (cache + rate limit + optional bandwidth checks)');
  const results = await db.batch(statements);

  if (!results || results.length < 2) {
    throw new Error('[Unified Check D1] Batch returned incomplete results');
  }

  // Parse cache result
  let cacheResult = {
    hit: false,
    linkData: null,
    timestamp: null,
    hostnameHash: null,
  };

  const cacheRow = results[statementIndexes.cache]?.results?.[0];
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
  const rateLimitRow = results[statementIndexes.rateLimit]?.results?.[0];
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

  const bandwidthResult = {
    iprangeAllowed: true,
    iprangeBytesUsed: 0,
    iprangeBlockUntil: null,
    iprangeRetryAfter: 0,
    filepathAllowed: true,
    filepathBytesUsed: 0,
    filepathBlockUntil: null,
    filepathRetryAfter: 0,
  };

  if (shouldCheckBandwidthIprange && typeof statementIndexes.bandwidthIprange !== 'undefined') {
    const iprangeRow = results[statementIndexes.bandwidthIprange]?.results?.[0];
    if (iprangeRow) {
      const bytesUsed = parseInt(iprangeRow.BYTES_USED, 10);
      const windowStart = parseInt(iprangeRow.WINDOW_START, 10);
      const blockValue = iprangeRow.BLOCK_UNTIL ? parseInt(iprangeRow.BLOCK_UNTIL, 10) : null;

      bandwidthResult.iprangeBytesUsed = Number.isNaN(bytesUsed) ? 0 : bytesUsed;

      if (blockValue && blockValue > now) {
        bandwidthResult.iprangeAllowed = false;
        bandwidthResult.iprangeBlockUntil = blockValue;
        bandwidthResult.iprangeRetryAfter = blockValue - now;
      } else if (!Number.isNaN(windowStart)
        && (now - windowStart) < bandwidthWindowTotalSeconds
        && bandwidthResult.iprangeBytesUsed >= bandwidthIprangeQuota) {
        bandwidthResult.iprangeAllowed = false;
        const retryUntil = windowStart + bandwidthWindowTotalSeconds;
        bandwidthResult.iprangeBlockUntil = retryUntil;
        if (retryUntil > now) {
          bandwidthResult.iprangeRetryAfter = retryUntil - now;
        }
      }
    }
  }

  if (shouldCheckBandwidthFilepath && compositeHash && typeof statementIndexes.bandwidthFilepath !== 'undefined') {
    const filepathRow = results[statementIndexes.bandwidthFilepath]?.results?.[0];
    if (filepathRow) {
      const bytesUsed = parseInt(filepathRow.BYTES_USED, 10);
      const windowStart = parseInt(filepathRow.WINDOW_START, 10);
      const blockValue = filepathRow.BLOCK_UNTIL ? parseInt(filepathRow.BLOCK_UNTIL, 10) : null;

      bandwidthResult.filepathBytesUsed = Number.isNaN(bytesUsed) ? 0 : bytesUsed;

      if (blockValue && blockValue > now) {
        bandwidthResult.filepathAllowed = false;
        bandwidthResult.filepathBlockUntil = blockValue;
        bandwidthResult.filepathRetryAfter = blockValue - now;
      } else if (!Number.isNaN(windowStart)
        && (now - windowStart) < bandwidthWindowFilepathSeconds
        && bandwidthResult.filepathBytesUsed >= bandwidthFilepathQuota) {
        bandwidthResult.filepathAllowed = false;
        const retryUntil = windowStart + bandwidthWindowFilepathSeconds;
        bandwidthResult.filepathBlockUntil = retryUntil;
        if (retryUntil > now) {
          bandwidthResult.filepathRetryAfter = retryUntil - now;
        }
      }
    }
  }

  console.log('[Unified Check D1] Completed successfully');

  return {
    cache: cacheResult,
    rateLimit: rateLimitResult,
    throttle: throttleResult,
    bandwidth: bandwidthResult,
  };
};
