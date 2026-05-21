import { applyVerifyHeaders, hasVerifyCredentials } from './utils.js';
import { logEvent } from './logging.js';

const DEFAULT_CLEANUP_PROBABILITY = 0.01;
const getErrorMessage = (error) => error instanceof Error ? error.message : String(error);
const getLogFields = (error, fallback = {}) => error?.logFields || { ...fallback, error: getErrorMessage(error) };

const createPostgrestError = (message, fields) => {
  const error = new Error(message);
  error.logFields = fields;
  return error;
};

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

const normalizeStringValue = (value, fallback = '') => {
  if (typeof value !== 'string') {
    return fallback;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : fallback;
};

const normalizeHeaderValues = (values) => {
  if (!Array.isArray(values)) {
    return [];
  }
  return values.filter((value) => typeof value === 'string' && value.trim() !== '');
};

const resolveTicketStateCleanupConfig = (config) => {
  const cacheConfig = config?.cacheConfig && typeof config.cacheConfig === 'object'
    ? config.cacheConfig
    : {};
  const topLevelVerifyHeader = normalizeHeaderValues(config?.verifyHeader);
  const topLevelVerifySecret = normalizeHeaderValues(config?.verifySecret);
  const cacheVerifyHeader = normalizeHeaderValues(cacheConfig.verifyHeader);
  const cacheVerifySecret = normalizeHeaderValues(cacheConfig.verifySecret);
  const verifyHeader = cacheVerifyHeader.length > 0 ? cacheVerifyHeader : topLevelVerifyHeader;
  const verifySecret = cacheVerifySecret.length > 0 ? cacheVerifySecret : topLevelVerifySecret;
  const postgrestUrl = normalizeStringValue(
    cacheConfig.postgrestUrl,
    normalizeStringValue(config?.postgrestUrl)
  );
  const ticketStateTableName = normalizeStringValue(
    cacheConfig.ticketStateTableName,
    normalizeStringValue(config?.ticketStateTableName, 'DOWNLOAD_TICKET_STATE_TABLE')
  );

  if (
    !postgrestUrl
    || !ticketStateTableName
    || !hasVerifyCredentials(verifyHeader, verifySecret)
    || verifyHeader.length !== verifySecret.length
  ) {
    return null;
  }

  return {
    postgrestUrl,
    verifyHeader,
    verifySecret,
    ticketStateTableName,
  };
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
    throw createPostgrestError(`PostgREST cleanup request failed (${response.status}): ${errorText}`, {
      status: response.status,
      operation: 'cleanup',
      table: tableName,
    });
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

  return tasks;
};

const cleanupExpiredTicketState = async (config, env, ticketStateConfig = resolveTicketStateCleanupConfig(config)) => {
  if (!ticketStateConfig) {
    return { cleaned: 0 };
  }

  const now = Math.floor(Date.now() / 1000);
  const tableName = ticketStateConfig.ticketStateTableName || 'DOWNLOAD_TICKET_STATE_TABLE';

  try {
    const { postgrestUrl, verifyHeader, verifySecret } = ticketStateConfig;
    const headers = {
      'Content-Type': 'application/json',
    };
    applyVerifyHeaders(headers, verifyHeader, verifySecret);

    const response = await fetch(`${normalizePostgrestUrl(postgrestUrl)}/rpc/download_cleanup_expired_tickets`, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        p_now: now,
        p_table_name: tableName,
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw createPostgrestError(`cleanup RPC failed (${response.status}): ${errorText}`, {
        status: response.status,
        operation: 'cleanup',
        rpc: 'download_cleanup_expired_tickets',
        table: tableName,
      });
    }

    const payload = await response.json();
    return { cleaned: Number(payload?.deleted) || 0 };
  } catch (error) {
    const message = getErrorMessage(error);
    logEvent('error', 'CleanupScheduler', 'task_failed', { task: 'TicketState', ...getLogFields(error) });
    return { cleaned: 0, error: message };
  }

  return { cleaned: 0 };
};

export async function scheduleAllCleanups(config, env, ctx) {
  if (!config) {
    return;
  }

  const ticketStateEnabled = config.dbMode === 'custom-pg-rest';
  const cleanupTasks = ticketStateEnabled
    ? buildCustomPgRestCleanupTasks(config)
    : [];
  const ticketStateCleanupConfig = ticketStateEnabled
    ? resolveTicketStateCleanupConfig(config)
    : null;

  if (ticketStateCleanupConfig) {
    cleanupTasks.push({
      name: 'TicketState',
      fn: async () => {
        const result = await cleanupExpiredTicketState(config, env, ticketStateCleanupConfig);
        return result.cleaned || 0;
      },
    });
  }

  if (cleanupTasks.length === 0) {
    return;
  }

  const cleanupProbability = deriveCleanupProbability(config);
  if (cleanupProbability <= 0) {
    return;
  }

  if (Math.random() >= cleanupProbability) {
    return;
  }

  logEvent('info', 'CleanupScheduler', 'triggered', {
    probability: cleanupProbability,
    percentage: formatPercentageLabel(cleanupProbability),
  });

  const cleanupPromise = Promise.allSettled(
    cleanupTasks.map((task) =>
      task
        .fn()
        .then((deletedCount) => {
          logEvent('info', 'CleanupScheduler', 'task_deleted', { task: task.name, deletedCount });
          return deletedCount;
        })
        .catch((error) => {
          logEvent('error', 'CleanupScheduler', 'task_failed', { task: task.name, ...getLogFields(error) });
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

export { cleanupExpiredTicketState };
