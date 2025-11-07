import { sha256Hash, calculateIPSubnet } from './utils.js';

/**
 * Ensure cache, rate limit, throttle, and session tables exist using a single batch call
 * @param {D1Database} db
 * @param {Object} tableNames
 * @param {string} tableNames.cacheTableName
 * @param {string} tableNames.rateLimitTableName
 * @param {string} tableNames.throttleTableName
 * @param {string} [tableNames.sessionTableName]
 * @returns {Promise<void>}
 */
const ensureAllTables = async (
  db,
  {
    cacheTableName,
    rateLimitTableName,
    throttleTableName,
    sessionTableName,
    lastActiveTableName,
    fairQueueTableName = 'upstream_slot_pool',
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

  if (sessionTableName) {
    statements.push(
      db.prepare(`
        CREATE TABLE IF NOT EXISTS ${sessionTableName} (
          SESSION_TICKET TEXT PRIMARY KEY,
          FILE_PATH TEXT NOT NULL,
          FILE_PATH_HASH TEXT NOT NULL,
          IP_SUBNET TEXT NOT NULL,
          WORKER_ADDRESS TEXT NOT NULL,
          EXPIRE_AT INTEGER NOT NULL,
          CREATED_AT INTEGER NOT NULL
        )
      `),
      db.prepare(`CREATE INDEX IF NOT EXISTS idx_session_expire ON ${sessionTableName}(EXPIRE_AT)`)
    );
  }

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
    `)
  );

  await db.batch(statements);
};

/**
 * Unified check for D1 binding (RTT 3→1 optimization)
 * Uses db.batch() to execute Session (optional) + Rate Limit + Cache in a single transaction
 * @param {string} path - File path
 * @param {string} clientIP - Client IP address
 * @param {Object} config - Configuration object
 * @param {string|null} sessionTicket - Optional session ticket
 * @returns {Promise<{session, cache, rateLimit, throttle}>}
 */
export const unifiedCheckD1 = async (path, clientIP, config, sessionTicket = null) => {
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

  const sessionEnabled = config.sessionEnabled === true;
  const sessionTableName = sessionEnabled ? (config.sessionTableName || 'SESSION_MAPPING_TABLE') : null;
  const shouldCheckSession = sessionEnabled && typeof sessionTicket === 'string' && sessionTicket.trim() !== '';

  if (config.initTables === true) {
    await ensureAllTables(db, {
      cacheTableName,
      rateLimitTableName,
      throttleTableName,
      sessionTableName,
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

  const defaultSessionResult = {
    found: shouldCheckSession ? false : null,
    filePath: null,
    filePathHash: null,
    ipSubnet: null,
    workerAddress: null,
    expireAt: null,
    error: null,
  };

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
        WHEN ${rateLimitTableName}.BLOCK_UNTIL IS NOT NULL AND ${rateLimitTableName}.BLOCK_UNTIL > ? THEN ${rateLimitTableName}.BLOCK_UNTIL
        WHEN (${rateLimitTableName}.BLOCK_UNTIL IS NULL OR ${rateLimitTableName}.BLOCK_UNTIL <= ?) AND ${rateLimitTableName}.ACCESS_COUNT >= ? AND ? > 0 THEN ? + ?
        ELSE ${rateLimitTableName}.BLOCK_UNTIL
      END
    RETURNING ACCESS_COUNT, LAST_WINDOW_TIME, BLOCK_UNTIL
  `;

  const createRateLimitStatement = () =>
    db.prepare(rateLimitSql).bind(
      ipHash, ipSubnet, now,
      now, windowSeconds, now, limit,
      now, windowSeconds, now, now, now,
      now, windowSeconds, now, now, now, limit, blockSeconds, now, blockSeconds
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

  const sessionTicketParam = shouldCheckSession ? sessionTicket : null;

  const sessionJoinClause = sessionTableName
    ? `
    LEFT JOIN ${sessionTableName} s 
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
      t.LAST_ERROR_CODE,
      active.LAST_ACCESS_TIME AS ACTIVE_LAST_ACCESS_TIME
    FROM 
      (SELECT ? AS provided_path_hash, ? AS session_ticket_param, ? AS ip_hash_param) params
    ${sessionJoinClause}
    LEFT JOIN ${cacheTableName} c 
      ON c.PATH_HASH = COALESCE(s.FILE_PATH_HASH, params.provided_path_hash)
    LEFT JOIN ${throttleTableName} t 
      ON t.HOSTNAME_HASH = c.HOSTNAME_HASH
    LEFT JOIN ${lastActiveTableName} active
      ON active.PATH_HASH = COALESCE(s.FILE_PATH_HASH, params.provided_path_hash)
      AND active.IP_HASH = params.ip_hash_param
    LIMIT 1
  `;

  const batchStatements = [
    db.prepare(unifiedSql).bind(pathHash, sessionTicketParam || null, ipHash),
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

  let sessionResult = { ...defaultSessionResult };

  if (shouldCheckSession) {
    const sessionFound = Boolean(unifiedRow?.SESSION_TICKET);
    const expireAtRaw = unifiedRow?.EXPIRE_AT != null ? Number(unifiedRow.EXPIRE_AT) : null;

    sessionResult = {
      found: sessionFound,
      filePath: unifiedRow?.FILE_PATH || null,
      filePathHash: unifiedRow?.FILE_PATH_HASH || null,
      ipSubnet: unifiedRow?.IP_SUBNET || null,
      workerAddress: unifiedRow?.WORKER_ADDRESS || null,
      expireAt: Number.isFinite(expireAtRaw) ? expireAtRaw : null,
      error: null,
    };

    if (!sessionFound) {
      sessionResult.error = 'session not found';
    } else if (sessionResult.expireAt !== null && sessionResult.expireAt < now) {
      sessionResult.found = false;
      sessionResult.error = 'session expired';
      console.log('[Unified Check D1] Session has expired');
    } else if (sessionResult.expireAt === null && unifiedRow?.EXPIRE_AT != null) {
      sessionResult.found = false;
      sessionResult.error = 'session expired';
      console.warn('[Unified Check D1] Session record missing valid EXPIRE_AT, treating as expired');
    }

    console.log('[Unified Check D1] Session found:', sessionResult.found);
  } else {
    console.log('[Unified Check D1] Session check skipped');
  }

  const cacheContextHash = sessionResult?.filePathHash || null;
  const cacheContext = cacheContextHash ? `session FILE_PATH_HASH ${cacheContextHash}` : `path ${path}`;

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

      const fallbackSessionResult = {
        ...defaultSessionResult,
        found: shouldCheckSession ? false : defaultSessionResult.found,
        error: idleErrorMessage,
      };

      sessionResult = fallbackSessionResult;
      cacheResult = { ...defaultCacheResult };
      throttleResult = { ...defaultThrottleResult };
    }
  }

  console.log('[Unified Check D1] Completed successfully (1 RTT)');

  return {
    session: sessionResult,
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

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const ensureSlotPool = async (db, hostname, globalLimit, tableName) => {
  console.log('[Fair Queue D1][ensureSlotPool] checking slots', {
    hostname,
    globalLimit,
    tableName,
  });
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
    console.log('[Fair Queue D1][ensureSlotPool] inserting missing slots', {
      hostname,
      statements: statements.length,
    });
    await db.batch(statements);
  } else {
    console.log('[Fair Queue D1][ensureSlotPool] slot pool already satisfied', {
      hostname,
      current,
    });
  }
};

const tryAcquireFairSlotOnce = async (db, hostname, ipHash, config) => {
  const tableName = config.fairQueueTableName || 'upstream_slot_pool';

  console.log('[Fair Queue D1][tryAcquire] attempt start', {
    hostname,
    tableName,
    ipHash,
    perIpLimit: config.perIpLimit,
    globalLimit: config.globalLimit,
    zombieTimeoutSeconds: config.zombieTimeoutSeconds,
  });

  try {
    await ensureSlotPool(db, hostname, config.globalLimit, tableName);

    await db
      .prepare(
        `UPDATE ${tableName}
         SET status = 'available', ip_hash = NULL, locked_at = NULL
         WHERE hostname_pattern = ?
           AND status = 'locked'
           AND locked_at < datetime('now', ?)`
      )
      .bind(hostname, `-${config.zombieTimeoutSeconds} seconds`)
      .run();

    console.log('[Fair Queue D1][tryAcquire] cleaned zombies', {
      hostname,
      tableName,
    });

    const ipRow = await db
      .prepare(
        `SELECT COUNT(*) AS c FROM ${tableName}
         WHERE hostname_pattern = ? AND status = 'locked' AND ip_hash = ?`
      )
      .bind(hostname, ipHash)
      .first();

    console.log('[Fair Queue D1][tryAcquire] locked slots for IP', {
      hostname,
      lockedCount: ipRow?.c ?? null,
      perIpLimit: config.perIpLimit,
    });

    if ((ipRow?.c ?? 0) >= config.perIpLimit) {
      console.warn('[Fair Queue D1][tryAcquire] per-IP limit hit', {
        hostname,
        ipHash,
      });
      return 0;
    }

    const candidate = await db
      .prepare(
        `SELECT id FROM ${tableName}
         WHERE hostname_pattern = ? AND status = 'available'
         ORDER BY slot_index LIMIT 1`
      )
      .bind(hostname)
      .first();

    console.log('[Fair Queue D1][tryAcquire] candidate slot', {
      hostname,
      candidateId: candidate?.id ?? null,
    });

    if (!candidate) {
      console.warn('[Fair Queue D1][tryAcquire] no available slot', { hostname });
      return -2;
    }
    const updateResult = await db
      .prepare(
        `UPDATE ${tableName}
         SET status = 'locked', ip_hash = ?, locked_at = datetime('now')
         WHERE id = ?
           AND status = 'available'`
      )
      .bind(ipHash, candidate.id)
      .run();
    const changes = updateResult.meta?.changes ?? 0;
    if (changes === 0) {
      console.warn('[Fair Queue D1][tryAcquire] slot already taken, retrying', {
        hostname,
        slotId: candidate.id,
      });
      return -2;
    }
    console.log('[Fair Queue D1][tryAcquire] slot locked', {
      hostname,
      slotId: candidate.id,
    });
    return candidate.id;
  } catch (error) {
    console.error('[Fair Queue D1][tryAcquire] error', {
      hostname,
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : null,
    });
    throw error;
  }
};

const acquireFairSlot = async (hostname, ipHash, config) => {
  if (!config?.env || !config.databaseBinding) {
    throw new Error('[Fair Queue D1] Missing D1 configuration');
  }

  const db = config.env[config.databaseBinding];
  if (!db) {
    throw new Error(`[Fair Queue D1] D1 binding '${config.databaseBinding}' not found`);
  }

  const deadline = Date.now() + config.queueWaitTimeoutMs;
  let attempts = 0;

  while (true) {
    attempts += 1;
    console.log('[Fair Queue D1][acquire] loop', {
      hostname,
      ipHash,
      attempt: attempts,
      deadline,
      now: Date.now(),
      pollIntervalMs: config.pollIntervalMs,
    });
    const result = await tryAcquireFairSlotOnce(db, hostname, ipHash, config);

    if (result > 0) {
      console.log(`[Fair Queue D1] Acquired slot ${result} for ${hostname}`);
      return result;
    }

    if (result === 0) {
      console.warn(`[Fair Queue D1] Per-IP limit reached: ${ipHash}`);
      const error = new Error(`Per-IP limit ${config.perIpLimit} reached`);
      error.name = 'PerIpLimitError';
      throw error;
    }

    if (Date.now() > deadline) {
      console.warn(`[Fair Queue D1] Queue timeout for ${hostname}`);
      const error = new Error(`Queue timeout after ${config.queueWaitTimeoutMs}ms`);
      error.name = 'QueueTimeoutError';
      throw error;
    }

    await sleep(config.pollIntervalMs);
  }
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
      `DELETE FROM ${tableName}
       WHERE status = 'locked'
         AND locked_at < datetime('now', ?)`
    )
    .bind(`-${config.zombieTimeoutSeconds} seconds`)
    .run();

  const deleted = result.meta?.changes ?? 0;
  if (deleted > 0) {
    console.log(`[Fair Queue D1] Cleaned up ${deleted} zombie slots`);
  }
  return deleted;
};

export {
  ensureAllTables,
  updateLastActive,
  acquireFairSlot,
  releaseFairSlot,
  cleanupZombieSlots,
};
