import { sha256Hash, calculateIPSubnet, applyVerifyHeaders, hasVerifyCredentials, isUsableReadyLink, readResponseTextWithSignal } from './utils.js';
import { logEvent } from './logging.js';

const VALID_BREAKER_STATES = new Set(['closed', 'open', 'half_open']);
const VALID_MARK_USED_RESULTS = new Set(['transitioned', 'already_used', 'storage_error']);

const normalizePostgrestUrl = (url) => {
  if (!url) {
    return '';
  }
  return url.endsWith('/') ? url.slice(0, -1) : url;
};

const createRpcHeaders = (config) => {
  if (!config?.postgrestUrl || !hasVerifyCredentials(config.verifyHeader, config.verifySecret)) {
    throw new Error('[Unified Check] Missing PostgREST configuration');
  }

  const headers = { 'Content-Type': 'application/json' };
  applyVerifyHeaders(headers, config.verifyHeader, config.verifySecret);
  return headers;
};

const parseNullableInt = (value) => {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isNaN(parsed) ? null : parsed;
};

const normalizeBreakerRecordExists = (value) => {
  if (value === true || value === 1 || value === '1') return true;
  if (typeof value === 'string') {
    const lowered = value.trim().toLowerCase();
    if (lowered === 'true' || lowered === 't') return true;
    if (lowered === 'false' || lowered === 'f') return false;
  }
  return false;
};

const normalizeBreakerState = (value) => {
  if (typeof value !== 'string') {
    return null;
  }
  const state = value.trim().toLowerCase();
  return VALID_BREAKER_STATES.has(state) ? state : null;
};

const normalizeFound = (value) => {
  if (value === true || value === 1 || value === '1') return true;
  if (typeof value === 'string') {
    const lowered = value.trim().toLowerCase();
    if (lowered === 'true' || lowered === 't') return true;
    if (lowered === 'false' || lowered === 'f') return false;
  }
  return false;
};

const parseTimestampMs = (value) => {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value * 1000;
  }
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : null;
};

const createRpcError = (message, fields) => {
  const error = new Error(message);
  error.logFields = fields;
  return error;
};

const postRpc = async (config, rpcName, body, errorScope) => {
  const targetUrl = `${normalizePostgrestUrl(config.postgrestUrl)}/rpc/${rpcName}`;
  const response = await fetch(targetUrl, {
    method: 'POST',
    headers: createRpcHeaders(config),
    body: JSON.stringify(body),
    signal: config.signal,
  });

  if (!response.ok) {
    try {
      await readResponseTextWithSignal(response, config.signal);
    } catch (error) {
      if (config.signal?.aborted) {
        throw error;
      }
      // The response body is diagnostic input and is intentionally discarded.
    }
    throw createRpcError(`${errorScope} RPC error (${response.status})`, {
      operation: 'rpc',
      rpc: rpcName,
      status: response.status,
    });
  }

  let result;
  try {
    result = JSON.parse(await readResponseTextWithSignal(response, config.signal));
  } catch (error) {
    if (config.signal?.aborted) {
      throw error;
    }
    throw createRpcError(`${errorScope} RPC returned invalid JSON`, {
      operation: 'rpc',
      rpc: rpcName,
      error: 'invalid_json',
    });
  }
  return result;
};

const parseTicketStateRow = (row) => {
  const found = normalizeFound(row?.found);
  if (!found) {
    return {
      found: false,
      ticketHash: null,
      issuedAt: null,
      firstUsedAt: null,
      hardExpireAt: null,
      idleTimeoutSeconds: null,
      idlePolicy: null,
      idleLeaseExpiresAt: null,
      idleRenewOwnerLeaseId: null,
      idleRenewOwnerLastHeartbeatAt: null,
      ipHash: null,
      pathHash: null,
    };
  }

  return {
    found: true,
    ticketHash: typeof row?.ticket_hash === 'string' ? row.ticket_hash : null,
    issuedAt: parseNullableInt(row?.issued_at),
    firstUsedAt: parseNullableInt(row?.first_used_at),
    hardExpireAt: parseNullableInt(row?.hard_expire_at),
    idleTimeoutSeconds: parseNullableInt(row?.idle_timeout_seconds),
    idlePolicy: typeof row?.idle_policy === 'string' ? row.idle_policy : null,
    idleLeaseExpiresAt: parseNullableInt(row?.idle_lease_expires_at),
    idleRenewOwnerLeaseId: typeof row?.idle_renew_owner_lease_id === 'string' ? row.idle_renew_owner_lease_id : null,
    idleRenewOwnerLastHeartbeatAt: parseNullableInt(row?.idle_renew_owner_last_heartbeat_at),
    ipHash: typeof row?.ip_hash === 'string' ? row.ip_hash : null,
    pathHash: typeof row?.path_hash === 'string' ? row.path_hash : null,
  };
};

/**
 * Unified check that performs rate limit + cache + breaker snapshot lookup in one database RTT
 * @param {string} path - File path
 * @param {string} clientIP - Client IP address
 * @param {Object} config - Configuration object
 * @returns {Promise<{cache, rateLimit, throttle}>}
 */
export const unifiedCheck = async (path, clientIP, config) => {
  if (!config.postgrestUrl || !hasVerifyCredentials(config.verifyHeader, config.verifySecret)) {
    throw new Error('[Unified Check] Missing PostgREST configuration');
  }
  if (typeof config.cacheEnabled !== 'boolean') {
    throw new Error('[Unified Check] cacheEnabled must be boolean');
  }

  const now = Math.floor(Date.now() / 1000);
  const cacheTTL = config.linkTTL ?? 1800;
  const windowSeconds = config.windowTimeSeconds ?? 86400;
  const limit = config.limit ?? 100;
  const blockSeconds = config.blockTimeSeconds ?? 600;
  const cacheTableName = config.cacheTableName || 'DOWNLOAD_CACHE_TABLE';
  const rateLimitTableName = config.rateLimitTableName || 'DOWNLOAD_IP_RATELIMIT_TABLE';
  const ipv4Suffix = config.ipv4Suffix ?? '/32';
  const ipv6Suffix = config.ipv6Suffix ?? '/60';

  logEvent('info', 'UnifiedCheck', 'start', {
    operation: 'download_unified_check',
    cacheEnabled: config.cacheEnabled,
  });

  const pathHash = await sha256Hash(path);
  if (!pathHash) {
    throw new Error('[Unified Check] Failed to calculate path hash');
  }

  const ipSubnet = calculateIPSubnet(clientIP, ipv4Suffix, ipv6Suffix);
  if (!ipSubnet) {
    throw new Error('[Unified Check] Failed to calculate IP subnet');
  }

  const ipHash = await sha256Hash(ipSubnet);
  if (!ipHash) {
    throw new Error('[Unified Check] Failed to calculate IP hash');
  }

  const rpcUrl = `${config.postgrestUrl}/rpc/download_unified_check`;
  const rpcBody = {
    p_path_hash: pathHash,
    p_cache_enabled: config.cacheEnabled,
    p_cache_ttl: cacheTTL,
    p_cache_table_name: cacheTableName,
    p_ip_hash: ipHash,
    p_ip_range: ipSubnet,
    p_window_seconds: windowSeconds,
    p_limit: limit,
    p_block_seconds: blockSeconds,
    p_ratelimit_table_name: rateLimitTableName,
    p_throttle_hostname_hash: config.throttleHostnameHash ?? null,
    p_now: now,
  };

  logEvent('info', 'UnifiedCheck', 'rpc_start', {
    operation: 'download_unified_check',
    cacheEnabled: rpcBody.p_cache_enabled,
    cacheTtl: rpcBody.p_cache_ttl,
    windowSeconds: rpcBody.p_window_seconds,
    limit: rpcBody.p_limit,
    blockSeconds: rpcBody.p_block_seconds,
    pathHash,
    ipHash,
    throttleHostnameHash: rpcBody.p_throttle_hostname_hash,
  });

  const response = await fetch(rpcUrl, {
    method: 'POST',
    headers: createRpcHeaders(config),
    body: JSON.stringify(rpcBody),
    signal: config.signal,
  });

  if (!response.ok) {
    try {
      await readResponseTextWithSignal(response, config.signal);
    } catch (error) {
      if (config.signal?.aborted) {
        throw error;
      }
      // The response body is diagnostic input and is intentionally discarded.
    }
    logEvent('error', 'UnifiedCheck', 'rpc_error', {
      operation: 'download_unified_check',
      status: response.status,
    });
    throw createRpcError(`Unified check RPC error (${response.status})`, {
      operation: 'rpc',
      rpc: 'download_unified_check',
      status: response.status,
    });
  }

  let result;
  try {
    result = JSON.parse(await readResponseTextWithSignal(response, config.signal));
  } catch (error) {
    if (config.signal?.aborted) {
      throw error;
    }
    throw createRpcError('Unified check RPC returned invalid JSON', {
      operation: 'rpc',
      rpc: 'download_unified_check',
      error: 'invalid_json',
    });
  }
  if (!Array.isArray(result) || result.length === 0 || !result[0] || typeof result[0] !== 'object') {
    logEvent('error', 'UnifiedCheck', 'rpc_empty', {
      operation: 'download_unified_check',
    });
    throw new Error('Unified check returned no rows');
  }

  const row = result[0];
  const cacheObservedAtMs = parseTimestampMs(row.cache_observed_at);
  if (cacheObservedAtMs === null) {
    throw createRpcError('Unified check returned no cache observation time', {
      operation: 'state',
      rpc: 'download_unified_check',
      error: 'invalid_response',
    });
  }
  logEvent('info', 'UnifiedCheck', 'rpc_success', {
    operation: 'download_unified_check',
    rowCount: result.length,
    cacheHit: Boolean(config.cacheEnabled && row.cache_link_data),
    accessCount: row.rate_access_count,
    blockUntil: row.rate_block_until,
    breakerRecordExists: row.throttle_record_exists,
    breakerState: row.throttle_state,
    breakerVersion: row.throttle_version,
    lastErrorCode: row.throttle_last_error_code,
  });

  let cacheResult = {
    hit: false,
    linkData: null,
    timestamp: null,
    hostnameHash: null,
    version: null,
    observedAt: null,
    observedAtMs: null,
  };
  cacheResult.version = row.cache_version || null;
  cacheResult.observedAt = row.cache_observed_at ?? null;
  cacheResult.observedAtMs = cacheObservedAtMs;

  if (config.cacheEnabled && row.cache_link_data) {
    try {
      const parsedLinkData = JSON.parse(row.cache_link_data);
      if (typeof row.cache_version !== 'string' || !row.cache_version.trim() || !isUsableReadyLink(parsedLinkData)) {
        throw new Error('invalid cache link data');
      }
      cacheResult.hit = true;
      cacheResult.linkData = parsedLinkData;
      cacheResult.timestamp = row.cache_timestamp;
      cacheResult.hostnameHash = row.cache_hostname_hash;
      logEvent('info', 'UnifiedCheck', 'cache_result', {
        result: 'hit',
        timestamp: cacheResult.timestamp,
        hostnameHash: cacheResult.hostnameHash,
        pathHash,
      });
    } catch {
      logEvent('error', 'UnifiedCheck', 'cache_parse_failed', {
        error: 'invalid_response',
        pathHash,
      });
      throw createRpcError('Unified check returned an invalid ready cache state', {
        operation: 'state',
        rpc: 'download_unified_check',
        error: 'invalid_response',
      });
    }
  } else if (!config.cacheEnabled) {
    logEvent('info', 'UnifiedCheck', 'cache_result', {
      result: 'disabled',
      pathHash,
    });
  } else {
    logEvent('info', 'UnifiedCheck', 'cache_result', {
      result: 'miss',
      pathHash,
    });
  }

  const parsedAccessCount = Number.parseInt(row.rate_access_count, 10);
  const accessCount = Number.isNaN(parsedAccessCount) ? 0 : parsedAccessCount;
  const parsedLastWindowTime = Number.parseInt(row.rate_last_window_time, 10);
  const lastWindowTime = Number.isNaN(parsedLastWindowTime) ? now : parsedLastWindowTime;
  const blockUntil = row.rate_block_until ? Number.parseInt(row.rate_block_until, 10) : null;

  let rateLimitAllowed = true;
  let rateLimitRetryAfter = 0;

  if (blockUntil && blockUntil > now) {
    rateLimitAllowed = false;
    rateLimitRetryAfter = blockUntil - now;
    logEvent('info', 'UnifiedCheck', 'rate_limit_result', {
      result: 'blocked',
      accessCount,
      limit,
      retryAfter: rateLimitRetryAfter,
      blockUntil,
    });
  } else if (accessCount >= limit) {
    const diff = now - lastWindowTime;
    rateLimitRetryAfter = windowSeconds - diff;
    rateLimitAllowed = false;
    logEvent('info', 'UnifiedCheck', 'rate_limit_result', {
      result: 'exceeded',
      accessCount,
      limit,
      retryAfter: rateLimitRetryAfter,
    });
  } else {
    logEvent('info', 'UnifiedCheck', 'rate_limit_result', {
      result: 'ok',
      accessCount,
      limit,
    });
  }

  const rateLimitResult = {
    allowed: rateLimitAllowed,
    accessCount,
    lastWindowTime,
    blockUntil,
    retryAfter: Number.isFinite(rateLimitRetryAfter) ? Math.max(0, rateLimitRetryAfter) : 0,
    ipSubnet,
  };

  const throttleResult = {
    recordExists: normalizeBreakerRecordExists(row.throttle_record_exists),
    state: normalizeBreakerState(row.throttle_state),
    openUntil: parseNullableInt(row.throttle_open_until),
    reason: typeof row.throttle_reason === 'string' && row.throttle_reason.trim() !== ''
      ? row.throttle_reason
      : null,
    version: parseNullableInt(row.throttle_version),
    lastErrorCode: parseNullableInt(row.throttle_last_error_code),
  };

  if (throttleResult.recordExists) {
    logEvent('info', 'UnifiedCheck', 'breaker_snapshot', {
      recordExists: throttleResult.recordExists,
      state: throttleResult.state,
      openUntil: throttleResult.openUntil,
      reason: throttleResult.reason,
      version: throttleResult.version,
      lastErrorCode: throttleResult.lastErrorCode,
    });
  } else {
    logEvent('info', 'UnifiedCheck', 'breaker_snapshot', {
      recordExists: false,
    });
  }

  logEvent('info', 'UnifiedCheck', 'complete', {
    operation: 'download_unified_check',
    cacheHit: cacheResult.hit,
    rateLimitAllowed: rateLimitResult.allowed,
    accessCount: rateLimitResult.accessCount,
    retryAfter: rateLimitResult.retryAfter,
    breakerState: throttleResult.state,
    pathHash,
    ipHash,
  });

  return {
    cache: cacheResult,
    rateLimit: rateLimitResult,
    throttle: throttleResult,
  };
};

export const readTicketState = async (ticketHash, config) => {
  const result = await postRpc(
    config,
    'download_get_ticket_state',
    {
      p_ticket_hash: ticketHash,
      p_table_name: config.ticketStateTableName || 'DOWNLOAD_TICKET_STATE_TABLE',
    },
    '[TicketState] Read'
  );

  if (!Array.isArray(result) || result.length === 0) {
    throw new Error('[TicketState] Read returned no rows');
  }

  return parseTicketStateRow(result[0]);
};

export const markTicketUsed = async (ticketHash, config, nowSeconds = Math.floor(Date.now() / 1000)) => {
  const result = await postRpc(
    config,
    'download_mark_ticket_used',
    {
      p_ticket_hash: ticketHash,
      p_now: nowSeconds,
      p_table_name: config.ticketStateTableName || 'DOWNLOAD_TICKET_STATE_TABLE',
    },
    '[TicketState] MarkUsed'
  );

  const normalizedResult = typeof result?.result === 'string'
    ? result.result.trim().toLowerCase()
    : 'storage_error';

  return {
    result: VALID_MARK_USED_RESULTS.has(normalizedResult) ? normalizedResult : 'storage_error',
    firstUsedAt: parseNullableInt(result?.first_used_at),
  };
};
