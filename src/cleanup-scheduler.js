import { applyVerifyHeaders } from './utils.js';

const DEFAULT_CLEANUP_PROBABILITY = 0.01;

const formatPercentageLabel = (probability) => {
  const percentage = probability * 100;
  return percentage % 1 === 0 ? `${percentage}` : percentage.toFixed(2);
};

const deriveCleanupProbability = (config) => {
  if (config && typeof config.cleanupPercentage === 'number' && !Number.isNaN(config.cleanupPercentage)) {
    const normalized = Math.min(Math.max(config.cleanupPercentage, 0), 100);
    return normalized / 100;
  }

  const candidates = [
    config?.cacheConfig?.cleanupProbability,
    config?.rateLimitConfig?.cleanupProbability,
    config?.throttleConfig?.cleanupProbability,
  ];

  for (const candidate of candidates) {
    if (typeof candidate === 'number' && candidate >= 0 && candidate <= 1) {
      return candidate;
    }
  }

  return DEFAULT_CLEANUP_PROBABILITY;
};

const executeD1RestStatement = async (accountId, databaseId, apiToken, sql, params = []) => {
  const endpoint = `https://api.cloudflare.com/client/v4/accounts/${accountId}/d1/database/${databaseId}/query`;
  const payload = params && params.length > 0 ? { sql, params } : { sql };

  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${apiToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`D1 REST cleanup request failed (${response.status}): ${errorText}`);
  }

  const result = await response.json();
  if (!result.success) {
    throw new Error(`D1 REST cleanup query failed: ${JSON.stringify(result.errors || 'Unknown error')}`);
  }

  const statementResult = result.result?.[0];
  return statementResult?.meta?.changes ?? 0;
};

const executeD1BindingStatement = async (db, sql, params = []) => {
  if (!db || typeof db.prepare !== 'function') {
    throw new Error('D1 binding unavailable for cleanup');
  }

  let statement = db.prepare(sql);
  if (params.length > 0) {
    statement = statement.bind(...params);
  }

  const result = await statement.run();
  return result?.meta?.changes ?? 0;
};

const normalizePostgrestUrl = (postgrestUrl) => {
  if (!postgrestUrl) {
    return '';
  }
  return postgrestUrl.endsWith('/') ? postgrestUrl.slice(0, -1) : postgrestUrl;
};

const parseContentRange = (contentRange) => {
  if (!contentRange) {
    return 0;
  }

  const rangeMatch = contentRange.match(/(?:(\d+)-(\d+)|\*\/(\d+))/);
  if (!rangeMatch) {
    return 0;
  }

  if (rangeMatch[1] !== undefined && rangeMatch[2] !== undefined) {
    const start = Number.parseInt(rangeMatch[1], 10);
    const end = Number.parseInt(rangeMatch[2], 10);
    if (Number.isInteger(start) && Number.isInteger(end) && end >= start) {
      return end - start + 1;
    }
    return 0;
  }

  if (rangeMatch[3] !== undefined) {
    const value = Number.parseInt(rangeMatch[3], 10);
    return Number.isInteger(value) ? value : 0;
  }

  return 0;
};

const executePostgrestDelete = async (postgrestUrl, verifyHeader, verifySecret, tableName, filters, extraHeaders = {}) => {
  const baseUrl = normalizePostgrestUrl(postgrestUrl);
  const targetUrl = `${baseUrl}/${tableName}${filters ? `?${filters}` : ''}`;

  const headers = {
    'Content-Type': 'application/json',
    ...extraHeaders,
  };
  applyVerifyHeaders(headers, verifyHeader, verifySecret);

  const response = await fetch(targetUrl, {
    method: 'DELETE',
    headers,
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`PostgREST cleanup request failed (${response.status}): ${errorText}`);
  }

  let payload = [];
  const contentType = response.headers.get('content-type');
  if (contentType && contentType.includes('application/json')) {
    try {
      payload = await response.json();
    } catch (_error) {
      payload = [];
    }
  }

  const contentRange = response.headers.get('content-range');
  const affectedRowsFromHeader = parseContentRange(contentRange);

  if (affectedRowsFromHeader > 0) {
    return affectedRowsFromHeader;
  }

  return Array.isArray(payload) ? payload.length : 0;
};

const executePostgrestRpc = async (postgrestUrl, verifyHeader, verifySecret, functionName, body) => {
  const baseUrl = normalizePostgrestUrl(postgrestUrl);
  const targetUrl = `${baseUrl}/rpc/${functionName}`;

  const headers = {
    'Content-Type': 'application/json',
  };
  applyVerifyHeaders(headers, verifyHeader, verifySecret);

  const response = await fetch(targetUrl, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`PostgREST RPC cleanup failed (${response.status}): ${errorText}`);
  }

  const contentType = response.headers.get('content-type');
  if (contentType && contentType.includes('application/json')) {
    const payload = await response.json();
    if (typeof payload === 'number') {
      return payload;
    }
  }

  return 0;
};

const buildD1RestCleanupTasks = (config) => {
  const tasks = [];

  if (config.cacheEnabled && config.cacheConfig) {
    const cacheConfig = config.cacheConfig;
    tasks.push({
      name: 'Cache',
      fn: async () => {
        const now = Math.floor(Date.now() / 1000);
        const cutoffTime = now - (cacheConfig.linkTTL * 2);
        const table = cacheConfig.tableName || 'DOWNLOAD_CACHE_TABLE';
        const sql = `DELETE FROM ${table} WHERE TIMESTAMP < ?`;
        return executeD1RestStatement(cacheConfig.accountId, cacheConfig.databaseId, cacheConfig.apiToken, sql, [cutoffTime]);
      },
    });
  }

  if (config.rateLimitEnabled && config.rateLimitConfig) {
    const rateLimitConfig = config.rateLimitConfig;
    tasks.push({
      name: 'RateLimit',
      fn: async () => {
        const now = Math.floor(Date.now() / 1000);
        const cutoffTime = now - (rateLimitConfig.windowTimeSeconds * 2);
        const table = rateLimitConfig.tableName || 'DOWNLOAD_IP_RATELIMIT_TABLE';
        const sql = `
          DELETE FROM ${table}
          WHERE LAST_WINDOW_TIME < ?
            AND (BLOCK_UNTIL IS NULL OR BLOCK_UNTIL < ?)
        `;
        return executeD1RestStatement(rateLimitConfig.accountId, rateLimitConfig.databaseId, rateLimitConfig.apiToken, sql, [cutoffTime, now]);
      },
    });
  }

  if (config.throttleEnabled && config.throttleConfig) {
    const throttleConfig = config.throttleConfig;
    tasks.push({
      name: 'Throttle',
      fn: async () => {
        const now = Math.floor(Date.now() / 1000);
        const cutoffTime = now - (throttleConfig.throttleTimeWindow * 2);
        const table = throttleConfig.tableName || 'THROTTLE_PROTECTION';
        const sql = `
          DELETE FROM ${table}
          WHERE IS_PROTECTED = 0
            AND (ERROR_TIMESTAMP IS NULL OR ERROR_TIMESTAMP < ?)
        `;
        return executeD1RestStatement(throttleConfig.accountId, throttleConfig.databaseId, throttleConfig.apiToken, sql, [cutoffTime]);
      },
    });
  }

  return tasks;
};

const buildD1CleanupTasks = (config, env) => {
  const tasks = [];

  const resolveBinding = (moduleConfig) => {
    const bindingName = moduleConfig?.databaseBinding;
    if (!bindingName) {
      throw new Error('D1 database binding name is missing');
    }
    const sourceEnv = moduleConfig.env || env;
    const db = sourceEnv?.[bindingName];
    if (!db) {
      throw new Error(`D1 database binding '${bindingName}' not found`);
    }
    return db;
  };

  if (config.cacheEnabled && config.cacheConfig) {
    const cacheConfig = config.cacheConfig;
    tasks.push({
      name: 'Cache',
      fn: async () => {
        const db = resolveBinding(cacheConfig);
        const now = Math.floor(Date.now() / 1000);
        const cutoffTime = now - (cacheConfig.linkTTL * 2);
        const table = cacheConfig.tableName || 'DOWNLOAD_CACHE_TABLE';
        const sql = `DELETE FROM ${table} WHERE TIMESTAMP < ?`;
        return executeD1BindingStatement(db, sql, [cutoffTime]);
      },
    });
  }

  if (config.rateLimitEnabled && config.rateLimitConfig) {
    const rateLimitConfig = config.rateLimitConfig;
    tasks.push({
      name: 'RateLimit',
      fn: async () => {
        const db = resolveBinding(rateLimitConfig);
        const now = Math.floor(Date.now() / 1000);
        const cutoffTime = now - (rateLimitConfig.windowTimeSeconds * 2);
        const table = rateLimitConfig.tableName || 'DOWNLOAD_IP_RATELIMIT_TABLE';
        const sql = `
          DELETE FROM ${table}
          WHERE LAST_WINDOW_TIME < ?
            AND (BLOCK_UNTIL IS NULL OR BLOCK_UNTIL < ?)
        `;
        return executeD1BindingStatement(db, sql, [cutoffTime, now]);
      },
    });
  }

  if (config.throttleEnabled && config.throttleConfig) {
    const throttleConfig = config.throttleConfig;
    tasks.push({
      name: 'Throttle',
      fn: async () => {
        const db = resolveBinding(throttleConfig);
        const now = Math.floor(Date.now() / 1000);
        const cutoffTime = now - (throttleConfig.throttleTimeWindow * 2);
        const table = throttleConfig.tableName || 'THROTTLE_PROTECTION';
        const sql = `
          DELETE FROM ${table}
          WHERE IS_PROTECTED = 0
            AND (ERROR_TIMESTAMP IS NULL OR ERROR_TIMESTAMP < ?)
        `;
        return executeD1BindingStatement(db, sql, [cutoffTime]);
      },
    });
  }

  return tasks;
};

const buildCustomPgRestCleanupTasks = (config) => {
  const tasks = [];

  if (config.cacheEnabled && config.cacheConfig) {
    const cacheConfig = config.cacheConfig;
    tasks.push({
      name: 'Cache',
      fn: async () => {
        const now = Math.floor(Date.now() / 1000);
        const cutoffTime = now - (cacheConfig.linkTTL * 2);
        const table = cacheConfig.tableName || 'DOWNLOAD_CACHE_TABLE';
        const filters = `TIMESTAMP=lt.${cutoffTime}`;
        return executePostgrestDelete(
          cacheConfig.postgrestUrl,
          cacheConfig.verifyHeader,
          cacheConfig.verifySecret,
          table,
          filters,
          { Prefer: 'return=representation' }
        );
      },
    });
  }

  if (config.rateLimitEnabled && config.rateLimitConfig) {
    const rateLimitConfig = config.rateLimitConfig;
    tasks.push({
      name: 'RateLimit',
      fn: async () => {
        const now = Math.floor(Date.now() / 1000);
        const cutoffTime = now - (rateLimitConfig.windowTimeSeconds * 2);
        const table = rateLimitConfig.tableName || 'DOWNLOAD_IP_RATELIMIT_TABLE';
        const filters = `LAST_WINDOW_TIME=lt.${cutoffTime}&and=(BLOCK_UNTIL.is.null,BLOCK_UNTIL.lt.${now})`;
        return executePostgrestDelete(
          rateLimitConfig.postgrestUrl,
          rateLimitConfig.verifyHeader,
          rateLimitConfig.verifySecret,
          table,
          filters,
          { Prefer: 'return=representation' }
        );
      },
    });
  }

  if (config.throttleEnabled && config.throttleConfig) {
    const throttleConfig = config.throttleConfig;
    tasks.push({
      name: 'Throttle',
      fn: async () => {
        const table = throttleConfig.tableName || 'THROTTLE_PROTECTION';
        const ttlSeconds = throttleConfig.throttleTimeWindow * 2;
        return executePostgrestRpc(
          throttleConfig.postgrestUrl,
          throttleConfig.verifyHeader,
          throttleConfig.verifySecret,
          'download_cleanup_throttle_protection',
          {
            p_ttl_seconds: ttlSeconds,
            p_table_name: table,
          }
        );
      },
    });
  }

  return tasks;
};

export async function scheduleAllCleanups(config, env, ctx) {
  if (!config || !config.dbMode) {
    return;
  }

  const cleanupProbability = deriveCleanupProbability(config);
  if (cleanupProbability <= 0) {
    return;
  }

  if (Math.random() >= cleanupProbability) {
    return;
  }

  console.log(
    `[Cleanup Scheduler] Triggered (${formatPercentageLabel(cleanupProbability)}% probability)`
  );

  const normalizedMode = String(config.dbMode || '').trim().toLowerCase();
  let cleanupTasks = [];

  if (normalizedMode === 'd1-rest') {
    cleanupTasks = buildD1RestCleanupTasks(config);
  } else if (normalizedMode === 'd1') {
    cleanupTasks = buildD1CleanupTasks(config, env);
  } else if (normalizedMode === 'custom-pg-rest') {
    cleanupTasks = buildCustomPgRestCleanupTasks(config);
  } else {
    console.warn(`[Cleanup Scheduler] Unknown dbMode: ${config.dbMode}`);
    return;
  }

  if (cleanupTasks.length === 0) {
    console.warn('[Cleanup Scheduler] No cleanup tasks scheduled (features disabled or misconfigured)');
    return;
  }

  const cleanupPromise = Promise.allSettled(
    cleanupTasks.map((task) =>
      task
        .fn()
        .then((deletedCount) => {
          console.log(`[${task.name} Cleanup] Deleted ${deletedCount} records`);
          return deletedCount;
        })
        .catch((error) => {
          const message = error instanceof Error ? error.message : String(error);
          console.error(`[${task.name} Cleanup] Failed:`, message);
          throw error;
        })
    )
  );

  if (ctx && typeof ctx.waitUntil === 'function') {
    ctx.waitUntil(cleanupPromise);
  } else {
    await cleanupPromise;
  }
}

