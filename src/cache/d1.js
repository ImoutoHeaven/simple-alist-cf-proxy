import { sha256Hash } from '../utils.js';

/**
 * Ensure the DOWNLOAD_CACHE_TABLE exists in the database
 * @param {D1Database} db - D1 Database instance
 * @param {string} tableName - Table name
 * @returns {Promise<void>}
 */
const ensureTable = async (db, tableName) => {
  const sql = `
    CREATE TABLE IF NOT EXISTS ${tableName} (
      PATH_HASH TEXT PRIMARY KEY,
      PATH TEXT NOT NULL,
      LINK_DATA TEXT NOT NULL,
      TIMESTAMP INTEGER NOT NULL,
      HOSTNAME_HASH TEXT
    )
  `;
  await db.prepare(sql).run();

  // Create index for hostname lookups if it does not already exist
  const indexSql = `CREATE INDEX IF NOT EXISTS idx_cache_hostname ON ${tableName}(HOSTNAME_HASH)`;
  await db.prepare(indexSql).run();
};

/**
 * Check if a cached download link exists and is still valid
 * @param {string} path - File path
 * @param {Object} config - Cache configuration
 * @returns {Promise<{linkData?: Object} | null>}
 */
export const checkCache = async (path, config) => {
  if (!config.env || !config.databaseBinding || !config.linkTTL) {
    return null;
  }

  if (!path || typeof path !== 'string') {
    return null;
  }

  try {
    // Get D1 database instance from binding
    const db = config.env[config.databaseBinding];
    if (!db) {
      throw new Error(`D1 database binding '${config.databaseBinding}' not found in env`);
    }

    const tableName = config.tableName || 'DOWNLOAD_CACHE_TABLE';

    // Ensure table exists
    await ensureTable(db, tableName);

    // Calculate path hash
    const pathHash = await sha256Hash(path);
    if (!pathHash) {
      return null;
    }

    // Query cache
    const selectSql = `SELECT PATH_HASH, PATH, LINK_DATA, TIMESTAMP, HOSTNAME_HASH FROM ${tableName} WHERE PATH_HASH = ?`;
    const result = await db.prepare(selectSql).bind(pathHash).first();

    if (!result) {
      return null; // Cache miss
    }

    // Check TTL
    const now = Math.floor(Date.now() / 1000);
    const age = now - Number.parseInt(result.TIMESTAMP, 10);

    if (age > config.linkTTL) {
      return null; // Expired
    }

    // Parse and return link data
    try {
      const linkData = JSON.parse(result.LINK_DATA);
      return { linkData, hostnameHash: result.HOSTNAME_HASH || null };
    } catch (error) {
      console.error('[Cache] Failed to parse LINK_DATA:', error.message);
      return null;
    }
  } catch (error) {
    console.error('[Cache] Check failed:', error.message);
    return null;
  }
};

/**
 * Save download link data to cache with atomic UPSERT
 * @param {string} path - File path
 * @param {Object} linkData - Download link data
 * @param {Object} config - Cache configuration
 * @returns {Promise<void>}
 */
export const saveCache = async (path, linkData, config) => {
  if (!config.env || !config.databaseBinding || !config.linkTTL) {
    return;
  }

  if (!path || typeof path !== 'string' || !linkData) {
    return;
  }

  try {
    // Get D1 database instance from binding
    const db = config.env[config.databaseBinding];
    if (!db) {
      throw new Error(`D1 database binding '${config.databaseBinding}' not found in env`);
    }

    const tableName = config.tableName || 'DOWNLOAD_CACHE_TABLE';

    // Ensure table exists
    await ensureTable(db, tableName);

    // Calculate path hash
    const pathHash = await sha256Hash(path);
    if (!pathHash) {
      return;
    }

    // Calculate hostname hash from linkData.url when available
    let hostnameHash = null;
    if (linkData && linkData.url) {
      try {
        const { extractHostname } = await import('../utils.js');
        const hostname = extractHostname(linkData.url);
        if (hostname) {
          hostnameHash = await sha256Hash(hostname);
          console.log(`[Cache] Calculated hostname hash for ${hostname}: ${hostnameHash}`);
        } else {
          console.warn(`[Cache] Failed to extract hostname from URL: ${linkData.url}`);
        }
      } catch (error) {
        console.error('[Cache] Failed to calculate hostname hash:', error.message);
      }
    }

    const now = Math.floor(Date.now() / 1000);

    // Probabilistic cleanup helper
    const triggerCleanup = () => {
      const probability = config.cleanupProbability || 0.01;
      if (Math.random() < probability) {
        console.log(`[Cache Cleanup] Triggered cleanup (probability: ${probability * 100}%)`);

        const cleanupPromise = cleanupExpiredCache(db, tableName, config.linkTTL)
          .then((deletedCount) => {
            console.log(`[Cache Cleanup] Background cleanup finished: ${deletedCount} records deleted`);
            return deletedCount;
          })
          .catch((error) => {
            console.error('[Cache Cleanup] Background cleanup failed:', error instanceof Error ? error.message : String(error));
          });

        if (config.ctx && config.ctx.waitUntil) {
          config.ctx.waitUntil(cleanupPromise);
          console.log(`[Cache Cleanup] Cleanup scheduled in background (using ctx.waitUntil)`);
        } else {
          console.warn(`[Cache Cleanup] No ctx.waitUntil available, cleanup may be interrupted`);
        }
      }
    };

    // Atomic UPSERT with RETURNING
    const upsertSql = `
      INSERT INTO ${tableName} (PATH_HASH, PATH, LINK_DATA, TIMESTAMP, HOSTNAME_HASH)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT (PATH_HASH) DO UPDATE SET
        LINK_DATA = excluded.LINK_DATA,
        TIMESTAMP = excluded.TIMESTAMP,
        PATH = excluded.PATH,
        HOSTNAME_HASH = excluded.HOSTNAME_HASH
      RETURNING PATH_HASH, LINK_DATA, TIMESTAMP, HOSTNAME_HASH
    `;

    const result = await db.prepare(upsertSql)
      .bind(pathHash, path, JSON.stringify(linkData), now, hostnameHash)
      .first();

    if (!result) {
      throw new Error('D1 UPSERT returned no rows');
    }

    // Trigger cleanup probabilistically
    triggerCleanup();
  } catch (error) {
    console.error('[Cache] Save failed:', error.message);
    // Don't propagate error - cache failure should not block downloads
  }
};

/**
 * Clean up expired records from the database
 * Removes records older than linkTTL * 2 (double buffer)
 * @param {D1Database} db - D1 Database instance
 * @param {string} tableName - Table name
 * @param {number} linkTTL - Link TTL in seconds
 * @returns {Promise<number>} - Number of deleted records
 */
const cleanupExpiredCache = async (db, tableName, linkTTL) => {
  const now = Math.floor(Date.now() / 1000);
  const cutoffTime = now - (linkTTL * 2);

  try {
    console.log(`[Cache Cleanup] Executing DELETE query (cutoff: ${cutoffTime}, linkTTL: ${linkTTL}s)`);

    const deleteSql = `DELETE FROM ${tableName} WHERE TIMESTAMP < ?`;
    const result = await db.prepare(deleteSql).bind(cutoffTime).run();

    const deletedCount = result.meta?.changes || 0;
    console.log(`[Cache Cleanup] DELETE completed: ${deletedCount} expired records deleted (older than ${linkTTL * 2}s)`);

    return deletedCount;
  } catch (error) {
    // Log error but don't propagate (cleanup failure shouldn't block requests)
    console.error('[Cache Cleanup] DELETE failed:', error instanceof Error ? error.message : String(error));
    return 0;
  }
};
