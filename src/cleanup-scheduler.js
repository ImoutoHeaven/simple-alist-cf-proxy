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

const cleanupLastActiveTable = async (config, env) => {
  const { dbMode, cacheConfig } = config || {};

  if (
    dbMode !== 'custom-pg-rest' ||
    !cacheConfig ||
    typeof cacheConfig.linkTTL !== 'number' ||
    cacheConfig.linkTTL <= 0
  ) {
    return { cleaned: 0 };
  }

  const now = Math.floor(Date.now() / 1000);
  const threshold = now - cacheConfig.linkTTL;
  const tableName = cacheConfig.lastActiveTableName || 'DOWNLOAD_LAST_ACTIVE_TABLE';

  try {
    const { postgrestUrl, verifyHeader, verifySecret } = cacheConfig;
    const filters = `LAST_ACCESS_TIME=lt.${threshold}`;
    const cleaned = await executePostgrestDelete(
      postgrestUrl,
      verifyHeader,
      verifySecret,
      tableName,
      filters,
      { Prefer: 'return=representation' }
    );
    return { cleaned };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error('[Cleanup] LastActive cleanup failed:', message);
    return { cleaned: 0, error: message };
  }

  return { cleaned: 0 };
};

export async function scheduleAllCleanups(config, env, ctx) {
  if (!config || config.dbMode !== 'custom-pg-rest') {
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

  const cleanupTasks = buildCustomPgRestCleanupTasks(config);

  if (
    config.cacheConfig &&
    typeof config.cacheConfig.linkTTL === 'number' &&
    config.cacheConfig.linkTTL > 0 &&
    config.cacheConfig.lastActiveTableName
  ) {
    cleanupTasks.push({
      name: 'LastActive',
      fn: async () => {
        const result = await cleanupLastActiveTable(config, env);
        return result.cleaned || 0;
      },
    });
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

  // Fair Queue zombie slot cleanup
  if (config.fairQueueEnabled) {
    const fairQueueCleanupProbability = 0.01; // 1%
    if (Math.random() < fairQueueCleanupProbability) {
      console.log('[Fair Queue Cleanup] Triggered cleanup');

      const cleanupPromise = (async () => {
        const fairQueueModule = await import('./fairqueue/custom-pg-rest.js');
        const fairQueueCleanupTasks = [];

        if (typeof fairQueueModule.cleanupZombieSlots === 'function') {
          fairQueueCleanupTasks.push(
            fairQueueModule.cleanupZombieSlots(config.fairQueueConfig)
          );
        }

        if (
          config.fairQueueConfig?.ipCooldownEnabled &&
          typeof fairQueueModule.cleanupIpCooldown === 'function'
        ) {
          fairQueueCleanupTasks.push(
            fairQueueModule.cleanupIpCooldown(config.fairQueueConfig)
          );
        }

        if (typeof fairQueueModule.cleanupQueueDepth === 'function') {
          fairQueueCleanupTasks.push(
            fairQueueModule.cleanupQueueDepth(config.fairQueueConfig)
          );
        }

        if (typeof fairQueueModule.cleanupHostPacing === 'function') {
          fairQueueCleanupTasks.push(
            fairQueueModule.cleanupHostPacing(config.fairQueueConfig)
          );
        }

        if (fairQueueCleanupTasks.length > 0) {
          await Promise.all(fairQueueCleanupTasks);
        }
      })().catch((error) => {
        const message = error instanceof Error ? error.message : String(error);
        console.error('[Fair Queue Cleanup] Failed:', message);
      });

      if (ctx && typeof ctx.waitUntil === 'function') {
        ctx.waitUntil(cleanupPromise);
      } else {
        await cleanupPromise;
      }
    }
  }
}

export { cleanupLastActiveTable };
