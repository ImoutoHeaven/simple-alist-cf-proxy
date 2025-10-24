import { sha256Hash } from '../utils.js';

/**
 * Execute SQL query via D1 REST API
 * @param {string} accountId - Cloudflare account ID
 * @param {string} databaseId - D1 database ID
 * @param {string} apiToken - Cloudflare API token
 * @param {string} sql - SQL query string
 * @param {Array} params - Query parameters (optional)
 * @returns {Promise<Object>} - Query result
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

  // Cloudflare D1 REST API returns: { success: true, result: [{ results: [...], success: true }] }
  if (!result.success) {
    throw new Error(`D1 REST API query failed: ${JSON.stringify(result.errors || 'Unknown error')}`);
  }

  return result.result?.[0] || { results: [], success: true };
};

/**
 * Ensure the DOWNLOAD_CACHE_TABLE exists in the database
 * @param {string} accountId - Cloudflare account ID
 * @param {string} databaseId - D1 database ID
 * @param {string} apiToken - Cloudflare API token
 * @param {string} tableName - Table name
 * @returns {Promise<void>}
 */
const ensureTable = async (accountId, databaseId, apiToken, tableName) => {
  const sql = `
    CREATE TABLE IF NOT EXISTS ${tableName} (
      PATH_HASH TEXT PRIMARY KEY,
      PATH TEXT NOT NULL,
      LINK_DATA TEXT NOT NULL,
      TIMESTAMP INTEGER NOT NULL
    )
  `;
  await executeQuery(accountId, databaseId, apiToken, sql);
};

/**
 * Check if a cached download link exists and is still valid
 * @param {string} path - File path
 * @param {Object} config - Cache configuration
 * @returns {Promise<{linkData?: Object} | null>}
 */
export const checkCache = async (path, config) => {
  if (!config.accountId || !config.databaseId || !config.apiToken || !config.linkTTL) {
    return null;
  }

  if (!path || typeof path !== 'string') {
    return null;
  }

  try {
    const { accountId, databaseId, apiToken } = config;
    const tableName = config.tableName || 'DOWNLOAD_CACHE_TABLE';

    // Ensure table exists
    await ensureTable(accountId, databaseId, apiToken, tableName);

    // Calculate path hash
    const pathHash = await sha256Hash(path);
    if (!pathHash) {
      return null;
    }

    // Query cache
    const selectSql = `SELECT PATH_HASH, PATH, LINK_DATA, TIMESTAMP FROM ${tableName} WHERE PATH_HASH = ?`;
    const queryResult = await executeQuery(accountId, databaseId, apiToken, selectSql, [pathHash]);
    const records = queryResult.results || [];

    if (!records || records.length === 0) {
      return null; // Cache miss
    }

    const result = records[0];

    // Check TTL
    const now = Math.floor(Date.now() / 1000);
    const age = now - Number.parseInt(result.TIMESTAMP, 10);

    if (age > config.linkTTL) {
      return null; // Expired
    }

    // Parse and return link data
    try {
      const linkData = JSON.parse(result.LINK_DATA);
      return { linkData };
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
  if (!config.accountId || !config.databaseId || !config.apiToken || !config.linkTTL) {
    return;
  }

  if (!path || typeof path !== 'string' || !linkData) {
    return;
  }

  try {
    const { accountId, databaseId, apiToken } = config;
    const tableName = config.tableName || 'DOWNLOAD_CACHE_TABLE';

    // Ensure table exists
    await ensureTable(accountId, databaseId, apiToken, tableName);

    // Calculate path hash
    const pathHash = await sha256Hash(path);
    if (!pathHash) {
      return;
    }

    const now = Math.floor(Date.now() / 1000);

    // Probabilistic cleanup helper
    const triggerCleanup = () => {
      const probability = config.cleanupProbability || 0.01;
      if (Math.random() < probability) {
        console.log(`[Cache Cleanup] Triggered cleanup (probability: ${probability * 100}%)`);

        const cleanupPromise = cleanupExpiredCache(accountId, databaseId, apiToken, tableName, config.linkTTL)
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
      INSERT INTO ${tableName} (PATH_HASH, PATH, LINK_DATA, TIMESTAMP)
      VALUES (?, ?, ?, ?)
      ON CONFLICT (PATH_HASH) DO UPDATE SET
        LINK_DATA = excluded.LINK_DATA,
        TIMESTAMP = excluded.TIMESTAMP,
        PATH = excluded.PATH
      RETURNING PATH_HASH, LINK_DATA, TIMESTAMP
    `;

    const upsertParams = [pathHash, path, JSON.stringify(linkData), now];
    const queryResult = await executeQuery(accountId, databaseId, apiToken, upsertSql, upsertParams);
    const records = queryResult.results || [];

    if (!records || records.length === 0) {
      throw new Error('D1 REST UPSERT returned no rows');
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
 * @param {string} accountId - Cloudflare account ID
 * @param {string} databaseId - D1 database ID
 * @param {string} apiToken - Cloudflare API token
 * @param {string} tableName - Table name
 * @param {number} linkTTL - Link TTL in seconds
 * @returns {Promise<number>} - Number of deleted records
 */
const cleanupExpiredCache = async (accountId, databaseId, apiToken, tableName, linkTTL) => {
  const now = Math.floor(Date.now() / 1000);
  const cutoffTime = now - (linkTTL * 2);

  try {
    console.log(`[Cache Cleanup] Executing DELETE query (cutoff: ${cutoffTime}, linkTTL: ${linkTTL}s)`);

    const deleteSql = `DELETE FROM ${tableName} WHERE TIMESTAMP < ?`;
    const result = await executeQuery(accountId, databaseId, apiToken, deleteSql, [cutoffTime]);

    const deletedCount = result.meta?.changes || 0;
    console.log(`[Cache Cleanup] DELETE completed: ${deletedCount} expired records deleted (older than ${linkTTL * 2}s)`);

    return deletedCount;
  } catch (error) {
    // Log error but don't propagate (cleanup failure shouldn't block requests)
    console.error('[Cache Cleanup] DELETE failed:', error instanceof Error ? error.message : String(error));
    return 0;
  }
};
