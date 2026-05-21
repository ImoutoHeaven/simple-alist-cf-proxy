import { sha256Hash, extractHostname, applyVerifyHeaders, hasVerifyCredentials } from '../utils.js';
import { logEvent } from '../logging.js';

const getErrorMessage = (error) => error instanceof Error ? error.message : String(error);
const getLogFields = (error, fallback = {}) => error?.logFields || { ...fallback, error: getErrorMessage(error) };

const createPostgrestError = (message, fields) => {
  const error = new Error(message);
  error.logFields = fields;
  return error;
};

/**
 * Execute query via PostgREST API
 * @param {string} postgrestUrl - PostgREST API base URL
 * @param {string|string[]} verifyHeader - Authentication header name(s)
 * @param {string|string[]} verifySecret - Authentication header value(s)
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
    'Content-Type': 'application/json',
    ...extraHeaders,
  };
  applyVerifyHeaders(headers, verifyHeader, verifySecret);

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
      throw new Error(
        `PostgREST table not found: "${tableName}". ` +
        `Please create the table manually using init.sql:\\n` +
        `CREATE TABLE ${tableName} (\\n` +
        `  PATH_HASH TEXT PRIMARY KEY,\\n` +
        `  PATH TEXT NOT NULL,\\n` +
        `  LINK_DATA TEXT NOT NULL,\\n` +
        `  TIMESTAMP INTEGER NOT NULL\\n` +
        `);\\n` +
        `\\nAlso run: CREATE OR REPLACE FUNCTION download_upsert_download_cache(...) (see init.sql)`
      );
    }

    throw createPostgrestError(`PostgREST API error (${response.status}): ${errorText}`, {
      status: response.status,
      operation: method,
      table: tableName,
    });
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
 * Check if a cached download link exists and is still valid
 * @param {string} path - File path
 * @param {Object} config - Cache configuration
 * @returns {Promise<{linkData?: Object} | null>}
 */
export const checkCache = async (path, config) => {
  if (!config.postgrestUrl || !hasVerifyCredentials(config.verifyHeader, config.verifySecret) || !config.linkTTL) {
    return null;
  }

  if (!path || typeof path !== 'string') {
    return null;
  }

  try {
    const { postgrestUrl, verifyHeader, verifySecret } = config;
    const tableName = config.tableName || 'DOWNLOAD_CACHE_TABLE';

    // Calculate path hash
    const pathHash = await sha256Hash(path);
    if (!pathHash) {
      return null;
    }

    // Query cache using PostgREST filter
    const filters = `PATH_HASH=eq.${pathHash}`;
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
      logEvent('error', 'Cache', 'parse_failed', { error: getErrorMessage(error) });
      return null;
    }
  } catch (error) {
    logEvent('error', 'Cache', 'check_failed', getLogFields(error, { operation: 'check' }));
    return null;
  }
};

/**
 * Save download link data to cache using RPC stored procedure
 * @param {string} path - File path
 * @param {Object} linkData - Download link data
 * @param {Object} config - Cache configuration
 * @returns {Promise<void>}
 */
export const saveCache = async (path, linkData, config) => {
  if (!config.postgrestUrl || !hasVerifyCredentials(config.verifyHeader, config.verifySecret) || !config.linkTTL) {
    return;
  }

  if (!path || typeof path !== 'string' || !linkData) {
    return;
  }

  try {
    const { postgrestUrl, verifyHeader, verifySecret } = config;
    const tableName = config.tableName || 'DOWNLOAD_CACHE_TABLE';

    // Calculate path hash
    const pathHash = await sha256Hash(path);
    if (!pathHash) {
      return;
    }

    // Calculate hostname hash from linkData.url
    let hostnameHash = null;
    if (linkData && linkData.url) {
      try {
        const hostname = extractHostname(linkData.url);
        if (hostname) {
          hostnameHash = await sha256Hash(hostname);
          logEvent('info', 'Cache', 'hostname_hash_calculated', { hostname, hostnameHash });
        } else {
          logEvent('warn', 'Cache', 'hostname_missing');
        }
      } catch (error) {
        logEvent('error', 'Cache', 'hostname_hash_failed', { error: getErrorMessage(error) });
      }
    }

    const now = Math.floor(Date.now() / 1000);

    // Probabilistic cleanup helper
    const triggerCleanup = () => {
      const probability = config.cleanupProbability || 0.01;
      if (Math.random() < probability) {
        logEvent('info', 'Cache', 'cleanup_triggered', { probability });

        const cleanupPromise = cleanupExpiredCache(postgrestUrl, verifyHeader, verifySecret, tableName, config.linkTTL)
          .then((deletedCount) => {
            logEvent('info', 'Cache', 'cleanup_done', { deletedCount });
            return deletedCount;
          })
          .catch((error) => {
            logEvent('error', 'Cache', 'cleanup_failed', { error: getErrorMessage(error) });
          });

        if (config.ctx && config.ctx.waitUntil) {
          config.ctx.waitUntil(cleanupPromise);
          logEvent('info', 'Cache', 'cleanup_scheduled', { waitUntil: true });
        } else {
          logEvent('warn', 'Cache', 'cleanup_scheduled', { waitUntil: false });
        }
      }
    };

    // Call atomic RPC stored procedure (with hostname_hash)
    const rpcUrl = `${postgrestUrl}/rpc/download_upsert_download_cache`;
    const rpcBody = {
      p_path_hash: pathHash,
      p_path: path,
      p_link_data: JSON.stringify(linkData),
      p_timestamp: now,
      p_hostname_hash: hostnameHash,
      p_table_name: tableName,
    };

    const rpcHeaders = { 'Content-Type': 'application/json' };
    applyVerifyHeaders(rpcHeaders, verifyHeader, verifySecret);

    const rpcResponse = await fetch(rpcUrl, {
      method: 'POST',
      headers: rpcHeaders,
      body: JSON.stringify(rpcBody),
    });

    if (!rpcResponse.ok) {
      const errorText = await rpcResponse.text();
      throw createPostgrestError(`PostgREST RPC error (${rpcResponse.status}): ${errorText}`, {
        status: rpcResponse.status,
        operation: 'save',
        rpc: 'download_upsert_download_cache',
        table: tableName,
      });
    }

    // Parse RPC result (returns array with single row)
    const rpcResult = await rpcResponse.json();
    if (!rpcResult || rpcResult.length === 0) {
      throw new Error('RPC download_upsert_download_cache returned no rows');
    }

    logEvent('info', 'Cache', 'save_success', { hostnameHash });

    // Trigger cleanup probabilistically
    triggerCleanup();
  } catch (error) {
    logEvent('error', 'Cache', 'save_failed', getLogFields(error, { operation: 'save' }));
    // Don't propagate error - cache failure should not block downloads
  }
};

/**
 * Clean up expired records from the database
 * Removes records older than linkTTL * 2 (double buffer)
 * @param {string} postgrestUrl - PostgREST API base URL
 * @param {string|string[]} verifyHeader - Authentication header name(s)
 * @param {string|string[]} verifySecret - Authentication header value(s)
 * @param {string} tableName - Table name
 * @param {number} linkTTL - Link TTL in seconds
 * @returns {Promise<number>} - Number of deleted records
 */
const cleanupExpiredCache = async (postgrestUrl, verifyHeader, verifySecret, tableName, linkTTL) => {
  const now = Math.floor(Date.now() / 1000);
  const cutoffTime = now - (linkTTL * 2);

  try {
    logEvent('info', 'Cache', 'cleanup_query_start', { cutoffTime, linkTTL });

    // Delete records where TIMESTAMP is older than cutoff
    const filters = `TIMESTAMP=lt.${cutoffTime}`;

    const result = await executeQuery(
      postgrestUrl,
      verifyHeader,
      verifySecret,
      tableName,
      'DELETE',
      filters,
      null,
      { 'Prefer': 'return=representation' }
    );

    const deletedCount = result.affectedRows || 0;
    logEvent('info', 'Cache', 'cleanup_query_done', { deletedCount, olderThanSeconds: linkTTL * 2 });

    return deletedCount;
  } catch (error) {
    // Log error but don't propagate (cleanup failure shouldn't block requests)
    logEvent('error', 'Cache', 'cleanup_query_failed', getLogFields(error, { operation: 'cleanup', table: tableName }));
    return 0;
  }
};
