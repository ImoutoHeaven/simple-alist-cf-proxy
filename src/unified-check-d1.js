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
const ensureAllTables = async (
  db,
  {
    cacheTableName,
    rateLimitTableName,
    throttleTableName,
    lastActiveTableName,
    fairQueueTableName = 'upstream_slot_pool',
    fairQueueCooldownTableName = 'upstream_ip_cooldown',
  }
) => {
  const activeTableName = lastActiveTableName || 'DOWNLOAD_LAST_ACTIVE_TABLE';
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
      CREATE TABLE IF NOT EXISTS ${activeTableName} (
        IP_HASH TEXT NOT NULL,
        PATH_HASH TEXT NOT NULL,
        LAST_ACCESS_TIME INTEGER NOT NULL,
        TOTAL_ACCESS_COUNT INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (IP_HASH, PATH_HASH)
      )
    `),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_download_last_active_time ON ${activeTableName}(LAST_ACCESS_TIME)`),
  ];

  statements.push(
    db.prepare(`
      CREATE TABLE IF NOT EXISTS ${fairQueueTableName} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hostname_pattern TEXT NOT NULL,
        slot_index INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'available',
        ip_hash TEXT,
        locked_at TEXT,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `),
    db.prepare(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_fair_queue_unique
        ON ${fairQueueTableName} (hostname_pattern, slot_index)
    `),
    db.prepare(`
      CREATE INDEX IF NOT EXISTS idx_fair_queue_host_status
        ON ${fairQueueTableName} (hostname_pattern, status)
    `),
    db.prepare(`
      CREATE TABLE IF NOT EXISTS ${fairQueueCooldownTableName} (
        hostname_pattern TEXT NOT NULL,
        ip_hash TEXT NOT NULL,
        last_release_at TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (hostname_pattern, ip_hash)
      )
    `),
    db.prepare(`
      CREATE INDEX IF NOT EXISTS idx_ip_cooldown_ts
        ON ${fairQueueCooldownTableName} (last_release_at)
    `)
  );

  await db.batch(statements);
};

/**
 * Unified check for D1 binding (RTT 3→1 optimization)
 * Uses db.batch() to execute rate limit + cache logic in a single transaction
 * @param {string} path - File path
 * @param {string} clientIP - Client IP address
 * @param {Object} config - Configuration object
 * @returns {Promise<{cache, rateLimit, throttle, idle}>}
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
  const lastActiveTableName = config.lastActiveTableName || 'DOWNLOAD_LAST_ACTIVE_TABLE';
  const throttleTimeWindow = config.throttleTimeWindow ?? 60;
  const ipv4Suffix = config.ipv4Suffix ?? '/32';
  const ipv6Suffix = config.ipv6Suffix ?? '/60';

  if (config.initTables === true) {
    await ensureAllTables(db, {
      cacheTableName,
      rateLimitTableName,
      throttleTableName,
      lastActiveTableName,
    });
  }

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

  const defaultCacheResult = {
    hit: false,
    linkData: null,
    timestamp: null,
    hostnameHash: null,
  };

  const defaultThrottleResult = {
    status: 'normal_operation',
    recordExists: false,
    isProtected: null,
    errorTimestamp: null,
    errorCode: null,
    retryAfter: 0,
  };

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

  const createRateLimitStatement = () =>
    db.prepare(rateLimitSql).bind(
      ipHash, ipSubnet, now,
      now, now, windowSeconds, now, limit,
      now, now, windowSeconds, now, now, now,
      now, now, now,
      now, windowSeconds, now, limit, limit, blockSeconds, now, blockSeconds
    );

  const parseRateLimitResult = (rateLimitRow) => {
    if (!rateLimitRow) {
      throw new Error('[Unified Check D1] Rate limit UPSERT returned no rows');
    }

    const accessCount = parseInt(rateLimitRow.ACCESS_COUNT, 10);
    const lastWindowTime = parseInt(rateLimitRow.LAST_WINDOW_TIME, 10);
    const blockUntil = rateLimitRow.BLOCK_UNTIL ? parseInt(rateLimitRow.BLOCK_UNTIL, 10) : null;

    let allowed = true;
    let retryAfter = 0;

    if (blockUntil && blockUntil > now) {
      allowed = false;
      retryAfter = blockUntil - now;
      console.log('[Unified Check D1] Rate limit BLOCKED until:', new Date(blockUntil * 1000).toISOString());
    } else if (accessCount >= limit) {
      const diff = now - lastWindowTime;
      retryAfter = Math.max(windowSeconds - diff, 0);
      allowed = false;
      console.log('[Unified Check D1] Rate limit EXCEEDED:', accessCount, '>=', limit);
    } else {
      console.log('[Unified Check D1] Rate limit OK:', accessCount, '/', limit);
    }

    return {
      allowed,
      accessCount,
      lastWindowTime,
      blockUntil,
      retryAfter,
      ipSubnet,
      limit,
      error: null,
    };
  };

  const parseCacheRow = (cacheRow, context) => {
    const cacheResult = { ...defaultCacheResult };

    if (!cacheRow) {
      console.log('[Unified Check D1] Cache MISS for', context);
      return cacheResult;
    }

    const timestamp = parseInt(cacheRow.TIMESTAMP, 10);
    const age = now - timestamp;
    if (Number.isFinite(age) && age <= cacheTTL) {
      try {
        cacheResult.hit = true;
        cacheResult.linkData = JSON.parse(cacheRow.LINK_DATA);
        cacheResult.timestamp = cacheRow.TIMESTAMP;
        cacheResult.hostnameHash = cacheRow.HOSTNAME_HASH || null;
        console.log('[Unified Check D1] Cache HIT for', context);
      } catch (error) {
        console.error('[Unified Check D1] Failed to parse cache link data:', error.message);
      }
    } else {
      console.log('[Unified Check D1] Cache expired (age:', age, 's) for', context);
    }

    return cacheResult;
  };

  const parseThrottleRow = (throttleRow, cacheResult) => {
    const throttleResult = { ...defaultThrottleResult };

    if (!cacheResult.hit || !cacheResult.hostnameHash) {
      console.log('[Unified Check D1] Skipping throttle check (no hostname_hash from cache)');
      return throttleResult;
    }

    if (!throttleRow) {
      console.log('[Unified Check D1] Throttle normal_operation (no record found)');
      return throttleResult;
    }

    throttleResult.recordExists = true;
    throttleResult.isProtected = throttleRow.IS_PROTECTED;
    throttleResult.errorTimestamp = throttleRow.ERROR_TIMESTAMP;
    throttleResult.errorCode = throttleRow.LAST_ERROR_CODE ?? null;

    if (throttleRow.IS_PROTECTED === 1) {
      const errorTimestamp = parseInt(throttleRow.ERROR_TIMESTAMP, 10);
      const timeSinceError = now - errorTimestamp;

      if (timeSinceError < throttleTimeWindow) {
        throttleResult.status = 'protected';
        throttleResult.retryAfter = throttleTimeWindow - timeSinceError;
        console.log('[Unified Check D1] Throttle PROTECTED, retry after:', throttleResult.retryAfter);
      } else {
        throttleResult.status = 'resume_operation';
        throttleResult.retryAfter = 0;
        console.log('[Unified Check D1] Throttle resume_operation (time window expired)');
      }
    } else if (throttleRow.IS_PROTECTED === 0) {
      throttleResult.status = 'normal_operation';
      console.log('[Unified Check D1] Throttle normal_operation (IS_PROTECTED = 0)');
    } else {
      throttleResult.status = 'normal_operation';
      console.log('[Unified Check D1] Throttle normal_operation (IS_PROTECTED = NULL, invalid state)');
    }

    return throttleResult;
  };

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

  const batchStatements = [
    db.prepare(unifiedSql).bind(pathHash, ipHash),
    createRateLimitStatement(),
  ];

  console.log('[Unified Check D1] Executing batch (unified query + rate limit in 1 RTT)');
  const batchResults = await db.batch(batchStatements);

  if (!batchResults || batchResults.length !== batchStatements.length) {
    throw new Error('[Unified Check D1] Unified batch returned incomplete results');
  }

  const unifiedRow = batchResults[0]?.results?.[0] || null;
  const rateLimitStatementResult = batchResults[1]?.results?.[0] || null;

  const activeLastAccessTime = (() => {
    if (!unifiedRow || unifiedRow.ACTIVE_LAST_ACCESS_TIME == null) {
      return null;
    }
    const parsed = Number(unifiedRow.ACTIVE_LAST_ACCESS_TIME);
    return Number.isFinite(parsed) ? parsed : null;
  })();

  const cacheContext = `path ${path}`;

  const hasCacheData = unifiedRow && unifiedRow.TIMESTAMP != null && unifiedRow.LINK_DATA != null;
  const cacheRow = hasCacheData
    ? {
        LINK_DATA: unifiedRow.LINK_DATA,
        TIMESTAMP: unifiedRow.TIMESTAMP,
        HOSTNAME_HASH: unifiedRow.HOSTNAME_HASH,
      }
    : null;

  let cacheResult = parseCacheRow(cacheRow, cacheContext);

  const hasThrottleData = unifiedRow && (
    unifiedRow.IS_PROTECTED != null ||
    unifiedRow.ERROR_TIMESTAMP != null ||
    unifiedRow.LAST_ERROR_CODE != null
  );

  const throttleRow = hasThrottleData
    ? {
        IS_PROTECTED: unifiedRow.IS_PROTECTED,
        ERROR_TIMESTAMP: unifiedRow.ERROR_TIMESTAMP,
        LAST_ERROR_CODE: unifiedRow.LAST_ERROR_CODE,
      }
    : null;

  let throttleResult = parseThrottleRow(throttleRow, cacheResult);
  const rateLimitResult = parseRateLimitResult(rateLimitStatementResult);

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
        `[Unified Check D1] Idle timeout exceeded (idle ${idleDuration}s > ${idleTimeout}s)`
      );

      cacheResult = { ...defaultCacheResult };
      throttleResult = { ...defaultThrottleResult };
    }
  }

  console.log('[Unified Check D1] Completed successfully (1 RTT)');

  return {
    cache: cacheResult,
    rateLimit: rateLimitResult,
    throttle: throttleResult,
    idle: idleInfo,
  };
};

const updateLastActive = async (db, ipHash, pathHash, tableName = 'DOWNLOAD_LAST_ACTIVE_TABLE') => {
  if (!db || typeof db.prepare !== 'function') {
    throw new Error('[LastActive] Invalid D1 database binding');
  }

  const resolvedTable = tableName || 'DOWNLOAD_LAST_ACTIVE_TABLE';
  const now = Math.floor(Date.now() / 1000);

  const sql = `
    INSERT INTO ${resolvedTable} (IP_HASH, PATH_HASH, LAST_ACCESS_TIME, TOTAL_ACCESS_COUNT)
    VALUES (?, ?, ?, 1)
    ON CONFLICT(IP_HASH, PATH_HASH) DO UPDATE SET
      LAST_ACCESS_TIME = excluded.LAST_ACCESS_TIME,
      TOTAL_ACCESS_COUNT = ${resolvedTable}.TOTAL_ACCESS_COUNT + 1
  `;

  return db.prepare(sql).bind(ipHash, pathHash, now).run();
};

// ========================================
// Fair Queue Functions (D1 Mode)
// ========================================

const ensureSlotPool = async (
  db,
  hostname,
  globalLimit,
  tableName,
  cooldownTableName = 'upstream_ip_cooldown'
) => {
  await db
    .prepare(`
      CREATE TABLE IF NOT EXISTS ${cooldownTableName} (
        hostname_pattern TEXT NOT NULL,
        ip_hash TEXT NOT NULL,
        last_release_at TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (hostname_pattern, ip_hash)
      )
    `)
    .run();

  await db
    .prepare(`
      CREATE INDEX IF NOT EXISTS idx_ip_cooldown_ts
        ON ${cooldownTableName} (last_release_at)
    `)
    .run();

  const row = await db
    .prepare(`SELECT COUNT(*) AS c FROM ${tableName} WHERE hostname_pattern = ?`)
    .bind(hostname)
    .first();

  const current = row?.c ?? 0;
  if (current >= globalLimit) {
    return;
  }

  const statements = [];
  for (let i = current + 1; i <= globalLimit; i++) {
    statements.push(
      db
        .prepare(
          `INSERT OR IGNORE INTO ${tableName} (hostname_pattern, slot_index, status)
           VALUES (?, ?, 'available')`
        )
        .bind(hostname, i)
    );
  }

  if (statements.length > 0) {
    await db.batch(statements);
  }
};

const tryAcquireFairSlotOnce = async (db, hostname, ipHash, config) => {
  const tableName = config.fairQueueTableName || 'upstream_slot_pool';
  const cooldownTableName = config.fairQueueCooldownTableName || 'upstream_ip_cooldown';
  const zombieCutoff = `-${config.zombieTimeoutSeconds} seconds`;
  const cooldownSeconds =
    config && config.ipCooldownEnabled ? Math.max(0, config.ipCooldownSeconds || 0) : 0;

  try {
    await ensureSlotPool(db, hostname, config.globalLimit, tableName, cooldownTableName);

    const ipRow = await db
      .prepare(
        `SELECT COUNT(*) AS c FROM ${tableName}
         WHERE hostname_pattern = ?
           AND status = 'locked'
           AND ip_hash = ?
           AND (locked_at IS NULL OR locked_at >= datetime('now', ?))`
      )
      .bind(hostname, ipHash, zombieCutoff)
      .first();

    const currentIpSlots = Number(ipRow?.c ?? 0);

    if (currentIpSlots >= config.perIpLimit) {
      return 0;
    }

    if (
      cooldownSeconds > 0 &&
      currentIpSlots > 0 &&
      currentIpSlots < config.perIpLimit &&
      ipHash
    ) {
      const cooldownRow = await db
        .prepare(
          `SELECT 1 AS active FROM ${cooldownTableName}
           WHERE hostname_pattern = ?
             AND ip_hash = ?
             AND last_release_at > datetime('now', ?)`
        )
        .bind(hostname, ipHash, `-${cooldownSeconds} seconds`)
        .first();

      if (cooldownRow) {
        return 0;
      }
    }

    const candidate = await db
      .prepare(
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
         ORDER BY slot_index LIMIT 1`
      )
      .bind(hostname, config.globalLimit, zombieCutoff)
      .first();

    if (!candidate) {
      return -1;
    }
    const updateResult = await db
      .prepare(
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
           )`
      )
      .bind(ipHash, candidate.id, zombieCutoff)
      .run();
    const changes = updateResult.meta?.changes ?? 0;
    if (changes === 0) {
      return -1;
    }
    return candidate.id;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const wrapped = new Error(`[Fair Queue D1] Database error: ${message}`);
    wrapped.name = 'FairQueueDbError';
    wrapped.originalError = error;
    throw wrapped;
  }
};

const tryAcquireFairSlot = async (hostname, ipHash, config) => {
  if (!config?.env || !config.databaseBinding) {
    throw new Error('[Fair Queue D1] Missing D1 configuration');
  }

  const db = config.env[config.databaseBinding];
  if (!db) {
    throw new Error(`[Fair Queue D1] D1 binding '${config.databaseBinding}' not found`);
  }

  return tryAcquireFairSlotOnce(db, hostname, ipHash, config);
};

const releaseFairSlot = async (slotId, config) => {
  if (!config?.env || !config.databaseBinding) {
    throw new Error('[Fair Queue D1] Missing D1 configuration');
  }

  const db = config.env[config.databaseBinding];
  if (!db) {
    throw new Error(`[Fair Queue D1] D1 binding '${config.databaseBinding}' not found`);
  }

  const tableName = config.fairQueueTableName || 'upstream_slot_pool';
  const cooldownTableName = config.fairQueueCooldownTableName || 'upstream_ip_cooldown';

  const slotRow = await db
    .prepare(
      `SELECT hostname_pattern, ip_hash
       FROM ${tableName}
       WHERE id = ?`
    )
    .bind(slotId)
    .first();

  if (!slotRow) {
    return;
  }

  const slotHostname = slotRow.hostname_pattern ?? slotRow.HOSTNAME_PATTERN ?? null;
  const slotIpHash = slotRow.ip_hash ?? slotRow.IP_HASH ?? null;

  if (config.ipCooldownEnabled && slotHostname && slotIpHash) {
    await db
      .prepare(
        `INSERT INTO ${cooldownTableName} (hostname_pattern, ip_hash, last_release_at, created_at)
         VALUES (?, ?, datetime('now'), datetime('now'))
         ON CONFLICT(hostname_pattern, ip_hash)
         DO UPDATE SET last_release_at = excluded.last_release_at`
      )
      .bind(slotHostname, slotIpHash)
      .run();
  }

  await db
    .prepare(
      `UPDATE ${tableName}
       SET status = 'available', ip_hash = NULL, locked_at = NULL
       WHERE id = ?`
    )
    .bind(slotId)
    .run();

  console.log(`[Fair Queue D1] Released slot ${slotId}`);
};

const cleanupZombieSlots = async (config) => {
  if (!config?.env || !config.databaseBinding) {
    throw new Error('[Fair Queue D1] Missing D1 configuration');
  }

  const db = config.env[config.databaseBinding];
  if (!db) {
    throw new Error(`[Fair Queue D1] D1 binding '${config.databaseBinding}' not found`);
  }

  const tableName = config.fairQueueTableName || 'upstream_slot_pool';

  const result = await db
    .prepare(
      `UPDATE ${tableName}
       SET status = 'available', ip_hash = NULL, locked_at = NULL
       WHERE status = 'locked'
         AND locked_at IS NOT NULL
         AND locked_at < datetime('now', ?)`
    )
    .bind(`-${config.zombieTimeoutSeconds} seconds`)
    .run();

  const recovered = result.meta?.changes ?? 0;
  if (recovered > 0) {
    console.log(`[Fair Queue D1] Recovered ${recovered} zombie slots`);
  }
  return recovered;
};

const cleanupIpCooldown = async (config) => {
  if (!config?.env || !config.databaseBinding) {
    throw new Error('[Fair Queue D1] Missing D1 configuration');
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

  const db = config.env[config.databaseBinding];
  if (!db) {
    throw new Error(`[Fair Queue D1] D1 binding '${config.databaseBinding}' not found`);
  }

  const tableName = config.fairQueueCooldownTableName || 'upstream_ip_cooldown';
  const result = await db
    .prepare(
      `DELETE FROM ${tableName}
       WHERE last_release_at < datetime('now', ?)`
    )
    .bind(`-${ttlSeconds} seconds`)
    .run();

  const deleted = result.meta?.changes ?? 0;
  if (deleted > 0) {
    console.log(`[Fair Queue D1] Cleaned up ${deleted} cooldown records`);
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
};
