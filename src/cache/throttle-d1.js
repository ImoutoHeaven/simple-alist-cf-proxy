import { sha256Hash } from '../utils.js';

/**
 * Ensure the THROTTLE_PROTECTION table exists in the database
 * @param {D1Database} db - D1 Database instance
 * @param {string} tableName - Table name
 * @returns {Promise<void>}
 */
const ensureTable = async (db, tableName) => {
  const sql = `
    CREATE TABLE IF NOT EXISTS ${tableName} (
      HOSTNAME_HASH TEXT PRIMARY KEY,
      HOSTNAME TEXT NOT NULL,
      ERROR_TIMESTAMP INTEGER,
      IS_PROTECTED INTEGER,
      LAST_ERROR_CODE INTEGER
    )
  `;
  await db.prepare(sql).run();

  // Create index for cleanup optimization
  const indexSql = `CREATE INDEX IF NOT EXISTS idx_throttle_timestamp ON ${tableName}(ERROR_TIMESTAMP)`;
  await db.prepare(indexSql).run();
};

/**
 * Check throttle protection status for a hostname
 * @param {string} hostname - Hostname to check
 * @param {Object} config - Throttle configuration
 * @param {Object} config.env - Cloudflare Workers env object
 * @param {string} config.databaseBinding - D1 database binding name
 * @param {string} config.tableName - Table name (defaults to 'THROTTLE_PROTECTION')
 * @param {number} config.throttleTimeWindow - Time window in seconds
 * @returns {Promise<{status: 'normal_operation'|'resume_operation'|'protected', recordExists: boolean, errorCode?: number, retryAfter?: number} | null>}
 */
export const checkThrottle = async (hostname, config) => {
  if (!config.env || !config.databaseBinding || !config.throttleTimeWindow) {
    return null;
  }

  if (!hostname || typeof hostname !== 'string') {
    return null;
  }

  try {
    // Get D1 database instance from binding
    const db = config.env[config.databaseBinding];
    if (!db) {
      console.error(`[Throttle] D1 database binding '${config.databaseBinding}' not found in env`);
      return null;
    }

    const tableName = config.tableName || 'THROTTLE_PROTECTION';

    // Ensure table exists
    await ensureTable(db, tableName);

    // Calculate hostname hash
    const hostnameHash = await sha256Hash(hostname);
    if (!hostnameHash) {
      console.error('[Throttle] Failed to calculate hostname hash');
      return null;
    }

    // Query throttle protection status
    const selectSql = `SELECT HOSTNAME_HASH, HOSTNAME, ERROR_TIMESTAMP, IS_PROTECTED, LAST_ERROR_CODE FROM ${tableName} WHERE HOSTNAME_HASH = ?`;
    const result = await db.prepare(selectSql).bind(hostnameHash).first();

    if (!result) {
      // No record found - normal operation (first time)
      return { status: 'normal_operation', recordExists: false };
    }

    // Check protection status
    if (result.IS_PROTECTED !== 1) {
      // Not protected - normal operation (record exists)
      return { status: 'normal_operation', recordExists: true };
    }

    // Protected - check time window
    const now = Math.floor(Date.now() / 1000);
    const errorTimestamp = Number.parseInt(result.ERROR_TIMESTAMP, 10);
    const timeSinceError = now - errorTimestamp;

    if (timeSinceError >= config.throttleTimeWindow) {
      // Time window expired - resume operation
      return { status: 'resume_operation', recordExists: true };
    } else {
      // Still within time window - protected
      const retryAfter = config.throttleTimeWindow - timeSinceError;
      return {
        status: 'protected',
        recordExists: true,
        errorCode: result.LAST_ERROR_CODE || 503,
        retryAfter,
      };
    }
  } catch (error) {
    console.error('[Throttle] Check failed:', error.message);
    return null;
  }
};

/**
 * Update throttle protection status for a hostname
 * @param {string} hostname - Hostname
 * @param {Object} updateData - Update data
 * @param {number} updateData.errorTimestamp - Unix timestamp of error (or null to clear)
 * @param {number} updateData.isProtected - 1 to protect, null to clear protection
 * @param {number} updateData.errorCode - HTTP error code (or null)
 * @param {Object} config - Throttle configuration
 * @returns {Promise<void>}
 */
export const updateThrottle = async (hostname, updateData, config) => {
  if (!config.env || !config.databaseBinding) {
    return;
  }

  if (!hostname || typeof hostname !== 'string') {
    return;
  }

  try {
    // Get D1 database instance from binding
    const db = config.env[config.databaseBinding];
    if (!db) {
      console.error(`[Throttle] D1 database binding '${config.databaseBinding}' not found in env`);
      return;
    }

    const tableName = config.tableName || 'THROTTLE_PROTECTION';

    // Ensure table exists
    await ensureTable(db, tableName);

    // Calculate hostname hash
    const hostnameHash = await sha256Hash(hostname);
    if (!hostnameHash) {
      console.error('[Throttle] Failed to calculate hostname hash');
      return;
    }

    // Probabilistic cleanup helper
    const triggerCleanup = () => {
      const probability = config.cleanupProbability || 0.01;
      if (Math.random() < probability) {
        console.log(`[Throttle Cleanup] Triggered cleanup (probability: ${probability * 100}%)`);

        const cleanupPromise = cleanupExpiredThrottle(db, tableName, config.throttleTimeWindow)
          .then((deletedCount) => {
            console.log(`[Throttle Cleanup] Background cleanup finished: ${deletedCount} records deleted`);
            return deletedCount;
          })
          .catch((error) => {
            console.error('[Throttle Cleanup] Background cleanup failed:', error instanceof Error ? error.message : String(error));
          });

        if (config.ctx && config.ctx.waitUntil) {
          config.ctx.waitUntil(cleanupPromise);
          console.log(`[Throttle Cleanup] Cleanup scheduled in background (using ctx.waitUntil)`);
        } else {
          console.warn(`[Throttle Cleanup] No ctx.waitUntil available, cleanup may be interrupted`);
        }
      }
    };

    // Atomic UPSERT with RETURNING
    const upsertSql = `
      INSERT INTO ${tableName} (HOSTNAME_HASH, HOSTNAME, ERROR_TIMESTAMP, IS_PROTECTED, LAST_ERROR_CODE)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT (HOSTNAME_HASH) DO UPDATE SET
        HOSTNAME = excluded.HOSTNAME,
        ERROR_TIMESTAMP = excluded.ERROR_TIMESTAMP,
        IS_PROTECTED = excluded.IS_PROTECTED,
        LAST_ERROR_CODE = excluded.LAST_ERROR_CODE
      RETURNING HOSTNAME_HASH, ERROR_TIMESTAMP, IS_PROTECTED, LAST_ERROR_CODE
    `;

    const result = await db.prepare(upsertSql)
      .bind(
        hostnameHash,
        hostname,
        updateData.errorTimestamp,
        updateData.isProtected,
        updateData.errorCode
      )
      .first();

    if (!result) {
      console.error('[Throttle] D1 UPSERT returned no rows');
      return;
    }

    console.log(`[Throttle] Updated protection status for ${hostname}: IS_PROTECTED=${updateData.isProtected}, ERROR_CODE=${updateData.errorCode}`);

    // Trigger cleanup probabilistically
    triggerCleanup();
  } catch (error) {
    console.error('[Throttle] Update failed:', error.message);
    // Don't propagate error - throttle failure should not block downloads
  }
};

/**
 * Clean up expired records from the database
 * Removes records where IS_PROTECTED IS NULL and ERROR_TIMESTAMP is older than throttleTimeWindow * 2
 * @param {D1Database} db - D1 Database instance
 * @param {string} tableName - Table name
 * @param {number} throttleTimeWindow - Time window in seconds
 * @returns {Promise<number>} - Number of deleted records
 */
const cleanupExpiredThrottle = async (db, tableName, throttleTimeWindow) => {
  const now = Math.floor(Date.now() / 1000);
  const cutoffTime = now - (throttleTimeWindow * 2);

  try {
    console.log(`[Throttle Cleanup] Executing DELETE query (cutoff: ${cutoffTime}, timeWindow: ${throttleTimeWindow}s)`);

    // Delete records where IS_PROTECTED IS NULL and ERROR_TIMESTAMP is old or NULL
    const deleteSql = `
      DELETE FROM ${tableName}
      WHERE IS_PROTECTED IS NULL
        AND (ERROR_TIMESTAMP IS NULL OR ERROR_TIMESTAMP < ?)
    `;
    const result = await db.prepare(deleteSql).bind(cutoffTime).run();

    const deletedCount = result.meta?.changes || 0;
    console.log(`[Throttle Cleanup] DELETE completed: ${deletedCount} expired records deleted (older than ${throttleTimeWindow * 2}s)`);

    return deletedCount;
  } catch (error) {
    // Log error but don't propagate (cleanup failure shouldn't block requests)
    console.error('[Throttle Cleanup] DELETE failed:', error instanceof Error ? error.message : String(error));
    return 0;
  }
};
