import { sha256Hash } from '../utils.js';

/**
 * Execute SQL query via D1 REST API
 * @param {string} accountId - Cloudflare account ID
 * @param {string} databaseId - D1 database ID
 * @param {string} apiToken - Cloudflare API token
 * @param {string} sql - SQL query
 * @param {Array} params - Query parameters
 * @returns {Promise<Object>} - Query result
 */
const executeQuery = async (accountId, databaseId, apiToken, sql, params = []) => {
  const url = `https://api.cloudflare.com/client/v4/accounts/${accountId}/d1/database/${databaseId}/query`;

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${apiToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      sql,
      params,
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    console.error(`[Throttle] D1 REST API error (${response.status}): ${errorText}`);
    return { results: [], meta: {} };
  }

  const result = await response.json();

  if (!result.success) {
    const errors = result.errors || [];
    const errorMessage = errors.map(e => e.message).join(', ');
    console.error(`[Throttle] D1 REST API query failed: ${errorMessage}`);
    return { results: [], meta: {} };
  }

  return result.result?.[0] || { results: [], meta: {} };
};

/**
 * Check throttle protection status for a hostname
 * @param {string} hostname - Hostname to check
 * @param {Object} config - Throttle configuration
 * @param {string} config.accountId - Cloudflare account ID
 * @param {string} config.databaseId - D1 database ID
 * @param {string} config.apiToken - Cloudflare API token
 * @param {string} config.tableName - Table name (defaults to 'THROTTLE_PROTECTION')
 * @param {number} config.throttleTimeWindow - Time window in seconds
 * @returns {Promise<{status: 'normal_operation'|'resume_operation'|'protected', recordExists: boolean, errorCode?: number, retryAfter?: number} | null>}
 */
export const checkThrottle = async (hostname, config) => {
  if (!config.accountId || !config.databaseId || !config.apiToken || !config.throttleTimeWindow) {
    return null;
  }

  if (!hostname || typeof hostname !== 'string') {
    return null;
  }

  try {
    const { accountId, databaseId, apiToken } = config;
    const tableName = config.tableName || 'THROTTLE_PROTECTION';

    // Calculate hostname hash
    const hostnameHash = await sha256Hash(hostname);
    if (!hostnameHash) {
      console.error('[Throttle] Failed to calculate hostname hash');
      return null;
    }

    // Query throttle protection status
    const selectSql = `SELECT HOSTNAME_HASH, HOSTNAME, ERROR_TIMESTAMP, IS_PROTECTED, LAST_ERROR_CODE FROM ${tableName} WHERE HOSTNAME_HASH = ?`;
    const queryResult = await executeQuery(accountId, databaseId, apiToken, selectSql, [hostnameHash]);

    const results = queryResult.results || [];

    if (!results || results.length === 0) {
      // No record found - normal operation (first time)
      return { status: 'normal_operation', recordExists: false };
    }

    const result = results[0];

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
  if (!config.accountId || !config.databaseId || !config.apiToken) {
    return;
  }

  if (!hostname || typeof hostname !== 'string') {
    return;
  }

  try {
    const { accountId, databaseId, apiToken } = config;
    const tableName = config.tableName || 'THROTTLE_PROTECTION';

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

        const cleanupPromise = cleanupExpiredThrottle(accountId, databaseId, apiToken, tableName, config.throttleTimeWindow)
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

    // Atomic UPSERT - D1 REST API doesn't support RETURNING in the same way,
    // but the operation is still atomic
    const upsertSql = `
      INSERT INTO ${tableName} (HOSTNAME_HASH, HOSTNAME, ERROR_TIMESTAMP, IS_PROTECTED, LAST_ERROR_CODE)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT (HOSTNAME_HASH) DO UPDATE SET
        HOSTNAME = excluded.HOSTNAME,
        ERROR_TIMESTAMP = excluded.ERROR_TIMESTAMP,
        IS_PROTECTED = excluded.IS_PROTECTED,
        LAST_ERROR_CODE = excluded.LAST_ERROR_CODE
    `;

    await executeQuery(
      accountId,
      databaseId,
      apiToken,
      upsertSql,
      [hostnameHash, hostname, updateData.errorTimestamp, updateData.isProtected, updateData.errorCode]
    );

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
 * @param {string} accountId - Cloudflare account ID
 * @param {string} databaseId - D1 database ID
 * @param {string} apiToken - Cloudflare API token
 * @param {string} tableName - Table name
 * @param {number} throttleTimeWindow - Time window in seconds
 * @returns {Promise<number>} - Number of deleted records
 */
const cleanupExpiredThrottle = async (accountId, databaseId, apiToken, tableName, throttleTimeWindow) => {
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

    const result = await executeQuery(accountId, databaseId, apiToken, deleteSql, [cutoffTime]);

    const deletedCount = result.meta?.changes || 0;
    console.log(`[Throttle Cleanup] DELETE completed: ${deletedCount} expired records deleted (older than ${throttleTimeWindow * 2}s)`);

    return deletedCount;
  } catch (error) {
    // Log error but don't propagate (cleanup failure shouldn't block requests)
    console.error('[Throttle Cleanup] DELETE failed:', error instanceof Error ? error.message : String(error));
    return 0;
  }
};
