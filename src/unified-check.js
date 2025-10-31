import { sha256Hash, calculateIPSubnet, applyVerifyHeaders, hasVerifyCredentials } from './utils.js';

/**
 * Unified check that performs Rate Limit + Cache + Throttle in a single database RTT
 * @param {string} path - File path
 * @param {string} clientIP - Client IP address
 * @param {Object} config - Configuration object
 * @returns {Promise<{cache, rateLimit, throttle}>}
 */
export const unifiedCheck = async (path, clientIP, config) => {
  if (!config.postgrestUrl || !hasVerifyCredentials(config.verifyHeader, config.verifySecret)) {
    throw new Error('[Unified Check] Missing PostgREST configuration');
  }

  const now = Math.floor(Date.now() / 1000);
  const throttleWindow = config.throttleTimeWindow ?? 60;
  const cacheTTL = config.linkTTL ?? 1800;
  const windowSeconds = config.windowTimeSeconds ?? 86400;
  const limit = config.limit ?? 100;
  const blockSeconds = config.blockTimeSeconds ?? 600;
  const cacheTableName = config.cacheTableName || 'DOWNLOAD_CACHE_TABLE';
  const rateLimitTableName = config.rateLimitTableName || 'DOWNLOAD_IP_RATELIMIT_TABLE';
  const throttleTableName = config.throttleTableName || 'THROTTLE_PROTECTION';
  const ipv4Suffix = config.ipv4Suffix ?? '/32';
  const ipv6Suffix = config.ipv6Suffix ?? '/60';
  const bandwidthIprangeQuota = config.bandwidthIprangeQuota ?? 0;
  const bandwidthFilepathQuota = config.bandwidthFilepathQuota ?? 0;
  const bandwidthWindowTotalSeconds = config.bandwidthWindowTotalSeconds ?? 0;
  const bandwidthWindowFilepathSeconds = config.bandwidthWindowFilepathSeconds ?? 0;
  const bandwidthBlockSeconds = config.bandwidthBlockSeconds ?? 0;
  const bandwidthIprangeTable = config.bandwidthIprangeTableName || 'IPRANGE_BANDWIDTH_QUOTA_TABLE';
  const bandwidthFilepathTable = config.bandwidthFilepathTableName || 'IPRANGE_FILEPATH_BANDWIDTH_QUOTA_TABLE';
  const shouldProcessIprange = bandwidthIprangeQuota > 0 && bandwidthWindowTotalSeconds > 0;
  const shouldProcessFilepath = bandwidthFilepathQuota > 0 && bandwidthWindowFilepathSeconds > 0;
  
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

  let compositeHash = null;
  if (shouldProcessFilepath) {
    compositeHash = await sha256Hash(`${ipSubnet}${path}`);
  }
  
  // Call unified RPC
  const rpcUrl = `${config.postgrestUrl}/rpc/download_unified_check`;
  const rpcBody = {
    p_path_hash: pathHash,
    p_cache_ttl: cacheTTL,
    p_cache_table_name: cacheTableName,
    p_ip_hash: ipHash,
    p_ip_range: ipSubnet,
    p_now: now,
    p_window_seconds: windowSeconds,
    p_limit: limit,
    p_block_seconds: blockSeconds,
    p_ratelimit_table_name: rateLimitTableName,
    p_throttle_time_window: throttleWindow,
    p_throttle_table_name: throttleTableName,
    p_bandwidth_iprange_quota: bandwidthIprangeQuota,
    p_bandwidth_filepath_quota: bandwidthFilepathQuota,
    p_bandwidth_window_total_seconds: bandwidthWindowTotalSeconds,
    p_bandwidth_window_filepath_seconds: bandwidthWindowFilepathSeconds,
    p_bandwidth_block_seconds: bandwidthBlockSeconds,
    p_bandwidth_iprange_table: bandwidthIprangeTable,
    p_bandwidth_filepath_table: bandwidthFilepathTable,
    p_bandwidth_composite_hash: compositeHash,
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
  
  if (row.cache_link_data) {
    try {
      cacheResult.hit = true;
      cacheResult.linkData = JSON.parse(row.cache_link_data);
      cacheResult.timestamp = row.cache_timestamp;
      cacheResult.hostnameHash = row.cache_hostname_hash;
      console.log('[Unified Check] Cache HIT for path:', path);
    } catch (error) {
      console.error('[Unified Check] Failed to parse cache link data:', error.message);
    }
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
  
  // Parse throttle result
  // BREAKING CHANGE: IS_PROTECTED semantics
  //   1 = protected (error detected)
  //   0 = normal operation (initialized or recovered)
  //   NULL = record does not exist

  let throttleResult = {
    status: 'normal_operation',
    recordExists: row.throttle_record_exists === true,
    isProtected: row.throttle_is_protected,
    errorTimestamp: row.throttle_error_timestamp,
    errorCode: row.throttle_error_code,
    retryAfter: 0,
  };

  if (row.throttle_is_protected === 1) {
    const errorTimestamp = parseInt(row.throttle_error_timestamp, 10);
    if (Number.isNaN(errorTimestamp)) {
      throttleResult.status = 'protected';
      throttleResult.retryAfter = throttleWindow;
      console.log('[Unified Check] Throttle PROTECTED with unknown timestamp, default retry after:', throttleResult.retryAfter);
    } else {
      const timeSinceError = now - errorTimestamp;

      if (timeSinceError < throttleWindow) {
        throttleResult.status = 'protected';
        throttleResult.retryAfter = throttleWindow - timeSinceError;
        console.log('[Unified Check] Throttle PROTECTED, retry after:', throttleResult.retryAfter);
      } else {
        throttleResult.status = 'resume_operation';
        console.log('[Unified Check] Throttle resume_operation (time window expired)');
      }
    }
  } else if (row.throttle_is_protected === 0) {
    console.log('[Unified Check] Throttle normal_operation (IS_PROTECTED = 0)');
  } else {
    console.log('[Unified Check] Throttle normal_operation (no record)');
  }
  
  // Parse bandwidth quota result
  const bandwidthResult = {
    iprangeAllowed: true,
    iprangeBytesUsed: 0,
    iprangeBlockUntil: null,
    iprangeRetryAfter: 0,
    filepathAllowed: true,
    filepathBytesUsed: 0,
    filepathBlockUntil: null,
    filepathRetryAfter: 0,
  };

  if (shouldProcessIprange || shouldProcessFilepath) {
    if (shouldProcessIprange) {
      if (typeof row.bandwidth_iprange_allowed !== 'undefined') {
        bandwidthResult.iprangeAllowed = row.bandwidth_iprange_allowed !== false;
      }
      if (typeof row.bandwidth_iprange_bytes_used !== 'undefined' && row.bandwidth_iprange_bytes_used !== null) {
        const parsed = parseInt(row.bandwidth_iprange_bytes_used, 10);
        bandwidthResult.iprangeBytesUsed = Number.isNaN(parsed) ? 0 : parsed;
      }
      if (row.bandwidth_iprange_block_until !== null && row.bandwidth_iprange_block_until !== undefined) {
        const blockValue = parseInt(row.bandwidth_iprange_block_until, 10);
        if (!Number.isNaN(blockValue)) {
          bandwidthResult.iprangeBlockUntil = blockValue;
          if (blockValue > now) {
            bandwidthResult.iprangeRetryAfter = blockValue - now;
          }
        }
      }
    }

    if (shouldProcessFilepath) {
      if (typeof row.bandwidth_filepath_allowed !== 'undefined') {
        bandwidthResult.filepathAllowed = row.bandwidth_filepath_allowed !== false;
      }
      if (typeof row.bandwidth_filepath_bytes_used !== 'undefined' && row.bandwidth_filepath_bytes_used !== null) {
        const parsed = parseInt(row.bandwidth_filepath_bytes_used, 10);
        bandwidthResult.filepathBytesUsed = Number.isNaN(parsed) ? 0 : parsed;
      }
      if (row.bandwidth_filepath_block_until !== null && row.bandwidth_filepath_block_until !== undefined) {
        const blockValue = parseInt(row.bandwidth_filepath_block_until, 10);
        if (!Number.isNaN(blockValue)) {
          bandwidthResult.filepathBlockUntil = blockValue;
          if (blockValue > now) {
            bandwidthResult.filepathRetryAfter = blockValue - now;
          }
        }
      }
    }
  }

  console.log('[Unified Check] Completed successfully');
  
  return {
    cache: cacheResult,
    rateLimit: rateLimitResult,
    throttle: throttleResult,
    bandwidth: bandwidthResult,
  };
};
