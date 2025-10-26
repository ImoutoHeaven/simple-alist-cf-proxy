import { sha256Hash } from '../utils.js';

/**
 * Execute query via PostgREST API
 * @param {string} postgrestUrl - PostgREST API base URL
 * @param {string} verifyHeader - Authentication header name
 * @param {string} verifySecret - Authentication header value
 * @param {string} tableName - Table name
 * @param {string} method - HTTP method (GET, POST, PATCH, DELETE)
 * @param {string} filters - URL query filters (for GET/PATCH/DELETE)
 * @param {Object} body - Request body (for POST/PATCH)
 * @param {Object} extraHeaders - Additional headers
 * @returns {Promise<Object>} - Query result
 */
const executeQuery = async (postgrestUrl, verifyHeader, verifySecret, tableName, method, filters = '', body = null, extraHeaders = {}) => {
  const url = `${postgrestUrl}/${tableName}${filters ? `?${filters}` : ''}`;

  const headers = {
    [verifyHeader]: verifySecret,
    'Content-Type': 'application/json',
    ...extraHeaders,
  };

  const options = {
    method,
    headers,
  };

  if (body) {
    options.body = JSON.stringify(body);
  }

  const response = await fetch(url, options);

  if (!response.ok) {
    const errorText = await response.text();

    // Check if table doesn't exist (PGRST205 error)
    if (response.status === 404 && errorText.includes('PGRST205')) {
      console.error(
        `[Throttle] PostgREST table not found: "${tableName}". ` +
        `Please create the table manually using init.sql. ` +
        `CREATE TABLE ${tableName} (...) (see init.sql for full schema)`
      );
      return { data: [], affectedRows: 0 };
    }

    console.error(`[Throttle] PostgREST API error (${response.status}): ${errorText}`);
    return { data: [], affectedRows: 0 };
  }

  // For POST/PATCH/DELETE, PostgREST returns the affected rows or empty
  // For GET, it returns an array of rows
  let result;
  const contentType = response.headers.get('content-type');
  if (contentType && contentType.includes('application/json')) {
    result = await response.json();
  } else {
    result = [];
  }

  // Get Content-Range header to determine affected rows count
  const contentRange = response.headers.get('content-range');
  let affectedRows = 0;
  if (contentRange) {
    // Content-Range format: "0-4/*" or "*/0" (no matches)
    const match = contentRange.match(/(\d+)-(\d+)|\*\/(\d+)/);
    if (match) {
      if (match[1] !== undefined && match[2] !== undefined) {
        affectedRows = parseInt(match[2], 10) - parseInt(match[1], 10) + 1;
      } else if (match[3] !== undefined) {
        affectedRows = parseInt(match[3], 10);
      }
    }
  } else if (method === 'POST' && response.status === 201) {
    // POST successful, assume 1 row inserted
    affectedRows = 1;
  } else if (method === 'PATCH' || method === 'DELETE') {
    // For PATCH/DELETE without Prefer: return=representation
    // We need to use Prefer: return=minimal and check if response is empty
    affectedRows = Array.isArray(result) ? result.length : 0;
  }

  return {
    data: Array.isArray(result) ? result : [],
    affectedRows,
  };
};

/**
 * Check throttle protection status for a hostname
 * @param {string} hostname - Hostname to check
 * @param {Object} config - Throttle configuration
 * @param {string} config.postgrestUrl - PostgREST API endpoint
 * @param {string} config.verifyHeader - Authentication header name
 * @param {string} config.verifySecret - Authentication header value
 * @param {string} config.tableName - Table name (defaults to 'THROTTLE_PROTECTION')
 * @param {number} config.throttleTimeWindow - Time window in seconds
 * @returns {Promise<{status: 'normal_operation'|'resume_operation'|'protected', recordExists: boolean, errorCode?: number, retryAfter?: number} | null>}
 */
export const checkThrottle = async (hostname, config) => {
  if (!config.postgrestUrl || !config.verifyHeader || !config.verifySecret || !config.throttleTimeWindow) {
    return null;
  }

  if (!hostname || typeof hostname !== 'string') {
    return null;
  }

  try {
    const { postgrestUrl, verifyHeader, verifySecret } = config;
    const tableName = config.tableName || 'THROTTLE_PROTECTION';

    // Calculate hostname hash
    const hostnameHash = await sha256Hash(hostname);
    if (!hostnameHash) {
      console.error('[Throttle] Failed to calculate hostname hash');
      return null;
    }

    // Query throttle protection status using PostgREST filter
    const filters = `HOSTNAME_HASH=eq.${hostnameHash}`;
    const queryResult = await executeQuery(
      postgrestUrl,
      verifyHeader,
      verifySecret,
      tableName,
      'GET',
      filters
    );

    const records = queryResult.data || [];

    if (!records || records.length === 0) {
      // No record found - normal operation (first time)
      return { status: 'normal_operation', recordExists: false };
    }

    const result = records[0];

    // BREAKING CHANGE: IS_PROTECTED semantics
    //   1 = protected (error detected)
    //   0 = normal operation (initialized or recovered)
    //   NULL = invalid state (should not exist in valid records)
    // Check protection status
    if (result.IS_PROTECTED === 0) {
      // Normal operation (record exists with IS_PROTECTED = 0)
      return { status: 'normal_operation', recordExists: true };
    } else if (result.IS_PROTECTED !== 1) {
      // IS_PROTECTED is NULL or other invalid value - treat as normal but log warning
      console.warn('[Throttle] Invalid IS_PROTECTED value:', result.IS_PROTECTED, 'for hostname:', hostname);
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
 * Update throttle protection status for a hostname using RPC stored procedure
 * @param {string} hostname - Hostname
 * @param {Object} updateData - Update data
 * @param {number} updateData.errorTimestamp - Unix timestamp of error (or null to clear)
 * @param {number} updateData.isProtected - 1 to protect, null to clear protection
 * @param {number} updateData.errorCode - HTTP error code (or null)
 * @param {Object} config - Throttle configuration
 * @returns {Promise<void>}
 */
export const updateThrottle = async (hostname, updateData, config) => {
  if (!config.postgrestUrl || !config.verifyHeader || !config.verifySecret) {
    return;
  }

  if (!hostname || typeof hostname !== 'string') {
    return;
  }

  try {
    const { postgrestUrl, verifyHeader, verifySecret } = config;
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

        const cleanupPromise = cleanupExpiredThrottle(postgrestUrl, verifyHeader, verifySecret, tableName, config.throttleTimeWindow)
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

    // Call atomic RPC stored procedure
    const rpcUrl = `${postgrestUrl}/rpc/download_upsert_throttle_protection`;
    const rpcBody = {
      p_hostname_hash: hostnameHash,
      p_hostname: hostname,
      p_error_timestamp: updateData.errorTimestamp,
      p_is_protected: updateData.isProtected,
      p_last_error_code: updateData.errorCode,
      p_table_name: tableName,
    };

    const rpcResponse = await fetch(rpcUrl, {
      method: 'POST',
      headers: {
        [verifyHeader]: verifySecret,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(rpcBody),
    });

    if (!rpcResponse.ok) {
      const errorText = await rpcResponse.text();
      console.error(`[Throttle] PostgREST RPC error (${rpcResponse.status}): ${errorText}`);
      return;
    }

    // Parse RPC result (returns array with single row)
    const rpcResult = await rpcResponse.json();
    if (!rpcResult || rpcResult.length === 0) {
      console.error('[Throttle] RPC download_upsert_throttle_protection returned no rows');
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
 * @param {string} postgrestUrl - PostgREST API base URL
 * @param {string} verifyHeader - Authentication header name
 * @param {string} verifySecret - Authentication header value
 * @param {string} tableName - Table name
 * @param {number} throttleTimeWindow - Time window in seconds
 * @returns {Promise<number>} - Number of deleted records
 */
const cleanupExpiredThrottle = async (postgrestUrl, verifyHeader, verifySecret, tableName, throttleTimeWindow) => {
  const now = Math.floor(Date.now() / 1000);
  const cutoffTime = now - (throttleTimeWindow * 2);

  try {
    console.log(`[Throttle Cleanup] Executing DELETE query (cutoff: ${cutoffTime}, timeWindow: ${throttleTimeWindow}s)`);

    // Use RPC function for cleanup to ensure proper NULL handling
    const rpcUrl = `${postgrestUrl}/rpc/download_cleanup_throttle_protection`;
    const rpcBody = {
      p_ttl_seconds: throttleTimeWindow * 2,
      p_table_name: tableName,
    };

    const rpcResponse = await fetch(rpcUrl, {
      method: 'POST',
      headers: {
        [verifyHeader]: verifySecret,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(rpcBody),
    });

    if (!rpcResponse.ok) {
      const errorText = await rpcResponse.text();
      console.error(`[Throttle Cleanup] PostgREST RPC error (${rpcResponse.status}): ${errorText}`);
      return 0;
    }

    const deletedCount = await rpcResponse.json();
    console.log(`[Throttle Cleanup] DELETE completed: ${deletedCount} expired records deleted (older than ${throttleTimeWindow * 2}s)`);

    return deletedCount;
  } catch (error) {
    // Log error but don't propagate (cleanup failure shouldn't block requests)
    console.error('[Throttle Cleanup] DELETE failed:', error instanceof Error ? error.message : String(error));
    return 0;
  }
};
