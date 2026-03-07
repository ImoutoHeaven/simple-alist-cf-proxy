import { sha256Hash, calculateIPSubnet, applyVerifyHeaders, hasVerifyCredentials } from './utils.js';

const VALID_BREAKER_STATES = new Set(['closed', 'open', 'half_open']);

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
  const lastActiveTableName = config.lastActiveTableName || 'DOWNLOAD_LAST_ACTIVE_TABLE';
  const ipv4Suffix = config.ipv4Suffix ?? '/32';
  const ipv6Suffix = config.ipv6Suffix ?? '/60';
  const idleTimeoutValue = Number(config.idleTimeout);
  const idleTimeout = Number.isFinite(idleTimeoutValue) && idleTimeoutValue > 0 ? idleTimeoutValue : 0;
  
  console.log('[Unified Check] Starting unified check for path:', path);
  
  // Calculate hashes
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
  
  // Call unified RPC
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

    // NEW: Last active parameters
    p_idle_timeout: idleTimeout,
    p_last_active_table_name: lastActiveTableName,
  };
  
  console.log('[Unified Check] Calling RPC with params:', JSON.stringify(rpcBody, null, 2));
  
  const rpcHeaders = { 'Content-Type': 'application/json' };
  applyVerifyHeaders(rpcHeaders, config.verifyHeader, config.verifySecret);

  const response = await fetch(rpcUrl, {
    method: 'POST',
    headers: rpcHeaders,
    body: JSON.stringify(rpcBody),
  });
  
  if (!response.ok) {
    const errorText = await response.text();
    console.error('[Unified Check] RPC error:', response.status, errorText);
    throw new Error(`Unified check RPC error (${response.status}): ${errorText}`);
  }
  
  const result = await response.json();
  if (!result || result.length === 0) {
    console.error('[Unified Check] RPC returned no rows');
    throw new Error('Unified check returned no rows');
  }
  
  const row = result[0];
  console.log('[Unified Check] RPC result:', JSON.stringify(row, null, 2));

  // Parse cache result
  let cacheResult = {
    hit: false,
    linkData: null,
    timestamp: null,
    hostnameHash: null,
  };
  
  if (config.cacheEnabled && row.cache_link_data) {
    try {
      cacheResult.hit = true;
      cacheResult.linkData = JSON.parse(row.cache_link_data);
      cacheResult.timestamp = row.cache_timestamp;
      cacheResult.hostnameHash = row.cache_hostname_hash;
      console.log('[Unified Check] Cache HIT for path:', path);
    } catch (error) {
      console.error('[Unified Check] Failed to parse cache link data:', error.message);
    }
  } else if (!config.cacheEnabled) {
    console.log('[Unified Check] Cache disabled for path:', path);
  } else {
    console.log('[Unified Check] Cache MISS for path:', path);
  }
  
  // Parse rate limit result
  const parsedAccessCount = parseInt(row.rate_access_count, 10);
  const accessCount = Number.isNaN(parsedAccessCount) ? 0 : parsedAccessCount;
  const parsedLastWindowTime = parseInt(row.rate_last_window_time, 10);
  const lastWindowTime = Number.isNaN(parsedLastWindowTime) ? now : parsedLastWindowTime;
  const blockUntil = row.rate_block_until ? parseInt(row.rate_block_until, 10) : null;
  
  let rateLimitAllowed = true;
  let rateLimitRetryAfter = 0;
  
  if (blockUntil && blockUntil > now) {
    rateLimitAllowed = false;
    rateLimitRetryAfter = blockUntil - now;
    console.log('[Unified Check] Rate limit BLOCKED until:', new Date(blockUntil * 1000).toISOString());
  } else if (accessCount >= limit) {
    const diff = now - lastWindowTime;
    rateLimitRetryAfter = windowSeconds - diff;
    rateLimitAllowed = false;
    console.log('[Unified Check] Rate limit EXCEEDED:', accessCount, '>=', limit);
  } else {
    console.log('[Unified Check] Rate limit OK:', accessCount, '/', limit);
  }
  
  const rateLimitResult = {
    allowed: rateLimitAllowed,
    accessCount,
    lastWindowTime,
    blockUntil,
    retryAfter: Number.isFinite(rateLimitRetryAfter) ? Math.max(0, rateLimitRetryAfter) : 0,
    ipSubnet,
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

  const parseNullableInt = (value) => {
    if (value === null || value === undefined || value === '') {
      return null;
    }
    const parsed = Number.parseInt(value, 10);
    return Number.isNaN(parsed) ? null : parsed;
  };

  const throttleState = normalizeBreakerState(row.throttle_state);

  const throttleResult = {
    recordExists: normalizeBreakerRecordExists(row.throttle_record_exists),
    state: throttleState,
    openUntil: parseNullableInt(row.throttle_open_until),
    reason: typeof row.throttle_reason === 'string' && row.throttle_reason.trim() !== ''
      ? row.throttle_reason
      : null,
    version: parseNullableInt(row.throttle_version),
    lastErrorCode: parseNullableInt(row.throttle_last_error_code),
  };

  if (throttleResult.recordExists) {
    console.log('[Unified Check] Breaker snapshot:', JSON.stringify(throttleResult));
  } else {
    console.log('[Unified Check] Breaker snapshot unavailable (no record)');
  }

  let activeLastAccessTime = null;
  let totalAccessCount = null;

  if (row.active_last_access_time !== null && row.active_last_access_time !== undefined) {
    const parsedActiveLastAccessTime = Number(row.active_last_access_time);
    if (Number.isFinite(parsedActiveLastAccessTime)) {
      activeLastAccessTime = parsedActiveLastAccessTime;
    }
  }

  if (row.active_total_access_count !== null && row.active_total_access_count !== undefined) {
    const parsedTotalAccessCount = Number(row.active_total_access_count);
    if (Number.isFinite(parsedTotalAccessCount)) {
      totalAccessCount = parsedTotalAccessCount;
    }
  }

  const idleInfo = {
    expired: false,
    timeout: idleTimeout,
    lastAccessTime: activeLastAccessTime,
    totalAccessCount,
    idleDuration: activeLastAccessTime != null ? now - activeLastAccessTime : null,
  };

  const idleErrorMessage = 'Link expired due to inactivity';

  if (idleTimeout > 0 && activeLastAccessTime != null) {
    const idleDuration = idleInfo.idleDuration ?? 0;
    if (idleDuration > idleTimeout) {
      idleInfo.expired = true;
      idleInfo.reason = idleErrorMessage;
      console.log(
        `[Unified Check] Idle timeout exceeded (idle ${idleDuration}s > ${idleTimeout}s)`
      );

      cacheResult = {
        hit: false,
        linkData: null,
        timestamp: null,
        hostnameHash: null,
      };

      return {
        cache: cacheResult,
        rateLimit: rateLimitResult,
        throttle: {
          recordExists: false,
          state: null,
          openUntil: null,
          reason: null,
          version: null,
          lastErrorCode: null,
        },
        idle: idleInfo,
      };
    }
  }

  console.log('[Unified Check] Completed successfully');
  
  return {
    cache: cacheResult,
    rateLimit: rateLimitResult,
    throttle: throttleResult,
    idle: idleInfo,
  };
};

const normalizePostgrestUrl = (url) => {
  if (!url) {
    return '';
  }
  return url.endsWith('/') ? url.slice(0, -1) : url;
};

const updateLastActive = async (config, ipHash, pathHash) => {
  if (
    !config ||
    !config.postgrestUrl ||
    !hasVerifyCredentials(config.verifyHeader, config.verifySecret)
  ) {
    throw new Error('[LastActive] Missing PostgREST configuration');
  }

  const now = Math.floor(Date.now() / 1000);
  const tableName = config.lastActiveTableName || 'DOWNLOAD_LAST_ACTIVE_TABLE';
  const targetUrl = `${normalizePostgrestUrl(config.postgrestUrl)}/rpc/download_update_last_active`;

  const headers = {
    'Content-Type': 'application/json',
  };
  applyVerifyHeaders(headers, config.verifyHeader, config.verifySecret);

  const body = {
    p_ip_hash: ipHash,
    p_path_hash: pathHash,
    p_last_access_time: now,
    p_table_name: tableName,
  };

  const response = await fetch(targetUrl, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`[LastActive] PostgREST update failed (${response.status}): ${errorText}`);
  }

  const contentType = response.headers.get('content-type');
  if (contentType && contentType.includes('application/json')) {
    try {
      return await response.json();
    } catch (_error) {
      return null;
    }
  }

  return null;
};

export { updateLastActive };
