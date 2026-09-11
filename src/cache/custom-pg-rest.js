import { sha256Hash, applyVerifyHeaders, hasVerifyCredentials, isUsableReadyLink, readResponseTextWithSignal } from '../utils.js';
import { logEvent } from '../logging.js';

const DEFAULT_CACHE_TABLE = 'DOWNLOAD_CACHE_TABLE';
const DEFAULT_CACHE_TTL = 1800;

const getErrorMessage = (error) => error instanceof Error ? error.message : String(error);
const getLogFields = (error, fallback = {}) => error?.logFields || { ...fallback, error: getErrorMessage(error) };
const isAbortError = (error) => error?.name === 'AbortError';
const createAbortError = () => new DOMException('The operation was aborted', 'AbortError');
const monotonicNow = () => typeof performance?.now === 'function' ? performance.now() : Date.now();

const createPostgrestError = (message, fields) => {
  const error = new Error(message);
  error.logFields = fields;
  return error;
};

const normalizePostgrestUrl = (url) => {
  if (!url || typeof url !== 'string') {
    return '';
  }
  return url.endsWith('/') ? url.slice(0, -1) : url;
};

const requireConfig = (config = {}) => {
  const postgrestUrl = normalizePostgrestUrl(config.postgrestUrl);
  if (!postgrestUrl || !hasVerifyCredentials(config.verifyHeader, config.verifySecret)) {
    throw createPostgrestError('PostgREST cache configuration is missing', { operation: 'cache' });
  }
  const linkTTL = Number(config.linkTTL);
  return {
    ...config,
    postgrestUrl,
    tableName: config.tableName || DEFAULT_CACHE_TABLE,
    linkTTL: Number.isFinite(linkTTL) && linkTTL > 0 ? linkTTL : DEFAULT_CACHE_TTL,
  };
};

const createHeaders = (config) => {
  const headers = { 'Content-Type': 'application/json' };
  applyVerifyHeaders(headers, config.verifyHeader, config.verifySecret);
  return headers;
};

const postRpc = async (config, rpcName, body, options = {}) => {
  const startedAt = monotonicNow();
  const response = await fetch(`${config.postgrestUrl}/rpc/${rpcName}`, {
    method: 'POST',
    headers: createHeaders(config),
    body: JSON.stringify(body),
    signal: options.signal,
  });
  if (!response.ok) {
    try {
      await readResponseTextWithSignal(response, options.signal);
    } catch (error) {
      if (isAbortError(error)) {
        throw error;
      }
      if (options.signal?.aborted) {
        throw createAbortError();
      }
    }
    throw createPostgrestError(`PostgREST RPC ${rpcName} failed (${response.status})`, {
      status: response.status,
      operation: 'rpc',
      rpc: rpcName,
    });
  }
  let payload;
  try {
    payload = JSON.parse(await readResponseTextWithSignal(response, options.signal));
  } catch (error) {
    if (isAbortError(error)) {
      throw error;
    }
    if (options.signal?.aborted) {
      throw createAbortError();
    }
    throw createPostgrestError(`PostgREST RPC ${rpcName} returned invalid JSON`, {
      operation: 'rpc',
      rpc: rpcName,
      error: 'invalid_json',
    });
  }
  if (options.scalar) {
    return { payload, elapsedMs: Math.max(0, monotonicNow() - startedAt) };
  }
  if (!Array.isArray(payload) || payload.length === 0 || !payload[0] || typeof payload[0] !== 'object') {
    throw createPostgrestError(`PostgREST RPC ${rpcName} returned no rows`, {
      operation: 'rpc',
      rpc: rpcName,
    });
  }
  return { payload: payload[0], elapsedMs: Math.max(0, monotonicNow() - startedAt) };
};

const parseNullableInteger = (value) => {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : null;
};

const parseDeadlineMs = (value) => {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value * 1000;
  }
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : null;
};

const parseRemainingMs = (deadline, observedAtMs, elapsedMs) => {
  const deadlineMs = parseDeadlineMs(deadline);
  if (deadlineMs === null || observedAtMs === null) {
    return null;
  }
  return Math.max(0, Math.trunc(deadlineMs - observedAtMs - Math.max(0, Number(elapsedMs) || 0)));
};

const parseRemainingSeconds = (remainingMs) => (
  remainingMs === null ? null : Math.max(0, Math.ceil(remainingMs / 1000))
);

const normalizeObservedState = (row, elapsedMs = 0) => {
  const observedAt = row?.observed_at;
  const observedAtMs = parseDeadlineMs(observedAt);
  const leaseUntil = row?.lease_until;
  const retryAfter = row?.retry_after;
  const leaseRemainingMs = parseRemainingMs(leaseUntil, observedAtMs, elapsedMs);
  const retryAfterRemainingMs = parseRemainingMs(retryAfter, observedAtMs, elapsedMs);
  return {
    observedAt,
    observedAtMs,
    leaseRemainingMs,
    retryAfterRemainingMs,
    leaseUntil,
    leaseUntilMs: parseDeadlineMs(leaseUntil),
    retryAfter,
    retryAfterMs: parseDeadlineMs(retryAfter),
    retryAfterSeconds: parseRemainingSeconds(retryAfterRemainingMs),
  };
};

const parseLinkData = (value) => {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  if (typeof value === 'object') {
    return value;
  }
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch {
    return null;
  }
};

const stateProtocolError = (message) => createPostgrestError(message, {
  operation: 'state',
  error: 'invalid_response',
});

const finishProtocolError = (message) => createPostgrestError(message, {
  operation: 'finish',
  error: 'invalid_response',
});

const normalizeState = (row, elapsedMs = 0) => {
  if (!row || typeof row !== 'object' || typeof row.result !== 'string') {
    throw createPostgrestError('PostgREST cache RPC returned an invalid state row', {
      operation: 'state',
      error: 'invalid_response',
    });
  }
  const result = row.result.trim().toLowerCase();
  if (!['ready', 'missing', 'wait', 'backoff', 'acquired'].includes(result)) {
    throw createPostgrestError('PostgREST cache RPC returned an unknown state', {
      operation: 'state',
      error: 'invalid_response',
    });
  }
  const observed = normalizeObservedState(row, elapsedMs);
  if (observed.observedAtMs === null) {
    throw stateProtocolError('PostgREST cache RPC returned no observation time');
  }
  if (result === 'ready' && (typeof row.version !== 'string' || !row.version.trim() || !isUsableReadyLink(parseLinkData(row.link_data)))) {
    throw stateProtocolError('PostgREST cache RPC returned an incomplete ready state');
  }
  if (['acquired', 'wait'].includes(result) && (
    !row.lease_id
    || observed.observedAtMs === null
    || observed.leaseUntilMs === null
  )) {
    throw stateProtocolError('PostgREST cache RPC returned an incomplete lease state');
  }
  if (result === 'backoff' && (observed.observedAtMs === null || observed.retryAfterMs === null)) {
    throw stateProtocolError('PostgREST cache RPC returned an incomplete backoff state');
  }
  return {
    result,
    pathHash: row.path_hash ?? null,
    path: row.path ?? null,
    linkData: parseLinkData(row.link_data),
    cacheTimestamp: parseNullableInteger(row.cache_timestamp),
    hostnameHash: row.hostname_hash ?? null,
    version: row.version ?? null,
    leaseId: row.lease_id ?? null,
    observedAt: observed.observedAt,
    observedAtMs: observed.observedAtMs,
    leaseRemainingMs: observed.leaseRemainingMs,
    retryAfterRemainingMs: observed.retryAfterRemainingMs,
    leaseUntil: observed.leaseUntil,
    leaseUntilMs: observed.leaseUntilMs,
    invalidVersion: row.invalid_version ?? null,
    retryAfter: observed.retryAfter,
    retryAfterMs: observed.retryAfterMs,
    retryAfterSeconds: observed.retryAfterSeconds,
    lastErrorCode: parseNullableInteger(row.last_error_code),
    updatedAt: row.updated_at ?? null,
  };
};

const normalizeFinishState = (row, elapsedMs = 0) => {
  if (!row || typeof row !== 'object' || typeof row.result !== 'string') {
    throw createPostgrestError('PostgREST cache RPC returned an invalid finish row', {
      operation: 'finish',
      error: 'invalid_response',
    });
  }
  const result = row.result.trim().toLowerCase();
  if (!['committed', 'duplicate', 'stale', 'invalid'].includes(result)) {
    throw createPostgrestError('PostgREST cache RPC returned an unknown finish result', {
      operation: 'finish',
      error: 'invalid_response',
    });
  }
  const observed = normalizeObservedState(row, elapsedMs);
  if (observed.observedAtMs === null) {
    throw createPostgrestError('PostgREST cache RPC returned no observation time', {
      operation: 'finish',
      error: 'invalid_response',
    });
  }
  return {
    result,
    version: row.version ?? null,
    invalidVersion: row.invalid_version ?? null,
    observedAt: observed.observedAt,
    observedAtMs: observed.observedAtMs,
    leaseRemainingMs: observed.leaseRemainingMs,
    retryAfterRemainingMs: observed.retryAfterRemainingMs,
    retryAfter: observed.retryAfter,
    retryAfterMs: observed.retryAfterMs,
    retryAfterSeconds: observed.retryAfterSeconds,
    lastErrorCode: parseNullableInteger(row.last_error_code),
  };
};

const cachePathHash = async (path) => {
  if (!path || typeof path !== 'string') {
    throw new Error('cache path must be a non-empty string');
  }
  const pathHash = await sha256Hash(path);
  if (!pathHash) {
    throw new Error('failed to calculate cache path hash');
  }
  return pathHash;
};

export const getCacheState = async (path, rawConfig = {}) => {
  const config = requireConfig(rawConfig);
  const pathHash = await cachePathHash(path);
  try {
    const rpcResponse = await postRpc(config, 'download_get_cache_state', {
      p_path_hash: pathHash,
      p_cache_ttl: config.linkTTL,
      p_cache_table_name: config.tableName,
    }, { signal: rawConfig.signal });
    return normalizeState(
      rpcResponse.payload,
      Number.isFinite(rpcResponse.elapsedMs) && rpcResponse.elapsedMs >= 0 ? rpcResponse.elapsedMs : 0,
    );
  } catch (error) {
    if (!error?.logFields) {
      error.logFields = { operation: 'state' };
    }
    logEvent('error', 'Cache', 'state_failed', getLogFields(error, { operation: 'state' }));
    throw error;
  }
};

export const acquireCacheRefresh = async (path, observedVersion = null, rawConfig = {}) => {
  const config = requireConfig(rawConfig);
  const pathHash = await cachePathHash(path);
  try {
    const rpcResponse = await postRpc(config, 'download_acquire_cache_refresh', {
      p_path_hash: pathHash,
      p_observed_version: observedVersion || null,
      p_cache_ttl: config.linkTTL,
      p_cache_table_name: config.tableName,
    }, { signal: rawConfig.signal });
    return normalizeState(
      rpcResponse.payload,
      Number.isFinite(rpcResponse.elapsedMs) && rpcResponse.elapsedMs >= 0 ? rpcResponse.elapsedMs : 0,
    );
  } catch (error) {
    if (!error?.logFields) {
      error.logFields = { operation: 'acquire' };
    }
    logEvent('error', 'Cache', 'acquire_failed', getLogFields(error, { operation: 'acquire' }));
    throw error;
  }
};

export const finishCacheRefresh = async (path, leaseId, options = {}, rawConfig = {}) => {
  const config = requireConfig(rawConfig);
  const pathHash = await cachePathHash(path);
  try {
    if (options?.linkData !== null && options?.linkData !== undefined && !isUsableReadyLink(options.linkData)) {
      throw finishProtocolError('PostgREST cache finish received an incomplete ready link');
    }
    const body = {
      p_path_hash: pathHash,
      p_lease_id: leaseId || null,
      p_link_data: options?.linkData ? JSON.stringify(options.linkData) : null,
      p_path: options?.path || path,
      p_hostname_hash: options?.hostnameHash || null,
      p_failed_version: options?.failedVersion || null,
      p_error_code: Number.isInteger(options?.errorCode) ? options.errorCode : null,
      p_cache_table_name: config.tableName,
    };
    const rpcResponse = await postRpc(config, 'download_finish_cache_refresh', body, { signal: rawConfig.signal });
    return normalizeFinishState(
      rpcResponse.payload,
      Number.isFinite(rpcResponse.elapsedMs) && rpcResponse.elapsedMs >= 0 ? rpcResponse.elapsedMs : 0,
    );
  } catch (error) {
    logEvent('error', 'Cache', 'finish_failed', getLogFields(error, { operation: 'finish' }));
    throw error;
  }
};

export const cleanupExpiredCache = async (rawConfig = {}) => {
  const config = requireConfig(rawConfig);
  try {
    const rpcResponse = await postRpc(config, 'download_cleanup_expired_cache', {
      p_ttl_seconds: config.linkTTL,
      p_table_name: config.tableName,
    }, { signal: rawConfig.signal, scalar: true });
    const payload = rpcResponse.payload;
    if (typeof payload === 'number') {
      return parseNullableInteger(payload) ?? 0;
    }
    if (payload && typeof payload === 'object' && Number.isFinite(Number(payload.deleted))) {
      return parseNullableInteger(payload.deleted) ?? 0;
    }
    throw new Error('PostgREST cleanup RPC returned an invalid result');
  } catch (error) {
    logEvent('error', 'Cache', 'cleanup_failed', getLogFields(error, { operation: 'cleanup' }));
    return 0;
  }
};
