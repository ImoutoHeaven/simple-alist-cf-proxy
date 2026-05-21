import { calculateIPSubnet, sha256Hash, applyVerifyHeaders, hasVerifyCredentials } from '../utils.js';
import { logEvent } from '../logging.js';

const DEFAULT_TABLE = 'DOWNLOAD_IP_RATELIMIT_TABLE';
const getErrorMessage = (error) => error instanceof Error ? error.message : String(error);
const getLogFields = (error, fallback = {}) => error?.logFields || { ...fallback, error: getErrorMessage(error) };

const createPostgrestError = (message, fields) => {
  const error = new Error(message);
  error.logFields = fields;
  return error;
};

/**
 * Execute query via PostgREST API.
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

    if (response.status === 404 && errorText.includes('PGRST205')) {
      throw new Error(
        `PostgREST table not found: "${tableName}". ` +
        'Please create the table and stored procedure using init.sql before enabling rate limiting.'
      );
    }

    throw createPostgrestError(`PostgREST API error (${response.status}): ${errorText}`, {
      status: response.status,
      operation: method,
      table: tableName,
    });
  }

  let result;
  const contentType = response.headers.get('content-type');
  if (contentType && contentType.includes('application/json')) {
    result = await response.json();
  } else {
    result = [];
  }

  const contentRange = response.headers.get('content-range');
  let affectedRows = 0;
  if (contentRange) {
    const match = contentRange.match(/(\d+)-(\d+)|\*\/(\d+)/);
    if (match) {
      if (match[1] !== undefined && match[2] !== undefined) {
        affectedRows = parseInt(match[2], 10) - parseInt(match[1], 10) + 1;
      } else if (match[3] !== undefined) {
        affectedRows = parseInt(match[3], 10);
      }
    }
  } else if (method === 'POST' && response.status === 201) {
    affectedRows = 1;
  } else if (method === 'PATCH' || method === 'DELETE') {
    affectedRows = Array.isArray(result) ? result.length : 0;
  }

  return {
    data: Array.isArray(result) ? result : [],
    affectedRows,
  };
};

/**
 * Rate limit check via PostgREST RPC.
 */
export const checkRateLimit = async (ip, config) => {
  if (!config.postgrestUrl || !hasVerifyCredentials(config.verifyHeader, config.verifySecret) || !config.windowTimeSeconds || !config.limit) {
    return { allowed: true };
  }

  if (!ip || typeof ip !== 'string') {
    return { allowed: true };
  }

  try {
    const { postgrestUrl, verifyHeader, verifySecret } = config;
    const tableName = config.tableName || DEFAULT_TABLE;

    const ipSubnet = calculateIPSubnet(ip, config.ipv4Suffix, config.ipv6Suffix);
    if (!ipSubnet) {
      return { allowed: true };
    }

    const ipHash = await sha256Hash(ipSubnet);
    if (!ipHash) {
      return { allowed: true };
    }

    const now = Math.floor(Date.now() / 1000);

    const triggerCleanup = () => {
      const probability = config.cleanupProbability || 0.01;
      if (Math.random() < probability) {
        logEvent('info', 'RateLimit', 'cleanup_triggered', { probability });

        const cleanupPromise = cleanupExpiredRecords(
          postgrestUrl,
          verifyHeader,
          verifySecret,
          tableName,
          config.windowTimeSeconds
        )
          .then((deletedCount) => {
            logEvent('info', 'RateLimit', 'cleanup_done', { deletedCount });
            return deletedCount;
          })
          .catch((error) => {
            logEvent('error', 'RateLimit', 'cleanup_failed', { error: getErrorMessage(error) });
          });

        if (config.ctx && config.ctx.waitUntil) {
          config.ctx.waitUntil(cleanupPromise);
          logEvent('info', 'RateLimit', 'cleanup_scheduled', { waitUntil: true });
        } else {
          logEvent('warn', 'RateLimit', 'cleanup_scheduled', { waitUntil: false });
        }
      }
    };

    const rpcUrl = `${postgrestUrl}/rpc/download_upsert_rate_limit`;
    const rpcBody = {
      p_ip_hash: ipHash,
      p_ip_range: ipSubnet,
      p_now: now,
      p_window_seconds: config.windowTimeSeconds,
      p_limit: config.limit,
      p_block_seconds: config.blockTimeSeconds || 0,
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
        operation: 'check',
        rpc: 'download_upsert_rate_limit',
        table: tableName,
      });
    }

    const rpcResult = await rpcResponse.json();
    if (!rpcResult || rpcResult.length === 0) {
      throw new Error('RPC download_upsert_rate_limit returned no rows');
    }

    const row = rpcResult[0];
    const accessCount = Number.parseInt(row.ACCESS_COUNT, 10);
    const lastWindowTime = Number.parseInt(row.LAST_WINDOW_TIME, 10);
    const blockUntil = row.BLOCK_UNTIL ? Number.parseInt(row.BLOCK_UNTIL, 10) : null;

    triggerCleanup();

    if (blockUntil && blockUntil > now) {
      const retryAfter = blockUntil - now;
      return {
        allowed: false,
        ipSubnet,
        retryAfter: Math.max(1, retryAfter),
      };
    }

    if (accessCount >= config.limit) {
      const diff = now - lastWindowTime;
      const retryAfter = config.windowTimeSeconds - diff;
      return {
        allowed: false,
        ipSubnet,
        retryAfter: Math.max(1, retryAfter),
      };
    }

    return { allowed: true };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);

    if (config.pgErrorHandle === 'fail-open') {
      logEvent('error', 'RateLimit', 'check_failed_fail_open', getLogFields(error, { operation: 'check' }));
      return { allowed: true };
    }

    return {
      allowed: false,
      error: `Rate limit check failed: ${errorMessage}`,
    };
  }
};

/**
 * Remove expired rate limit records.
 */
export const cleanupExpiredRecords = async (postgrestUrl, verifyHeader, verifySecret, tableName, windowTimeSeconds) => {
  const now = Math.floor(Date.now() / 1000);
  const cutoffTime = now - (windowTimeSeconds * 2);

  try {
    logEvent('info', 'RateLimit', 'cleanup_query_start', { cutoffTime, windowTimeSeconds });

    const filters = `LAST_WINDOW_TIME=lt.${cutoffTime}&and=(BLOCK_UNTIL.is.null,BLOCK_UNTIL.lt.${now})`;
    const result = await executeQuery(
      postgrestUrl,
      verifyHeader,
      verifySecret,
      tableName,
      'DELETE',
      filters,
      null,
      { Prefer: 'return=representation' }
    );

    const deletedCount = result.affectedRows || 0;
    logEvent('info', 'RateLimit', 'cleanup_query_done', { deletedCount });
    return deletedCount;
  } catch (error) {
    logEvent('error', 'RateLimit', 'cleanup_query_failed', getLogFields(error, { operation: 'cleanup', table: tableName }));
    return 0;
  }
};

export const formatWindowTime = (seconds) => {
  if (seconds % 3600 === 0) {
    return `${seconds / 3600}h`;
  }
  if (seconds % 60 === 0) {
    return `${seconds / 60}m`;
  }
  return `${seconds}s`;
};
