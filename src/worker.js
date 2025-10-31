// Import cache/throttle managers, rate limiter, and utilities
import { createCacheManager } from './cache/factory.js';
import { createThrottleManager } from './cache/throttle-factory.js';
import { createRateLimiter } from './ratelimit/factory.js';
import { createBandwidthQuotaManager } from './bandwidth-quota/factory.js';
import { unifiedCheck } from './unified-check.js';
import { unifiedCheckD1 } from './unified-check-d1.js';
import { unifiedCheckD1Rest } from './unified-check-d1-rest.js';
import { scheduleAllCleanups } from './cleanup-scheduler.js';
import {
  parseBoolean,
  parseInteger,
  parseNumber,
  parseWindowTime,
  extractHostname,
  matchHostnamePattern,
  applyVerifyHeaders,
  calculateIPSubnet,
  sha256Hash,
  parseBandwidthSize,
  parseDynamicQuota,
} from './utils.js';

// Configuration constants
const REQUIRED_ENV = ['ADDRESS', 'TOKEN', 'WORKER_ADDRESS'];
const VALID_ACTIONS = new Set(['block', 'skip-sign', 'skip-hash', 'skip-worker', 'skip-ip', 'asis']);
const VALID_EXCEPT_ACTIONS = new Set(['block-except', 'skip-sign-except', 'skip-hash-except', 'skip-worker-except', 'skip-ip-except', 'asis-except']);

// Utility: Parse comma-separated prefix list
const parsePrefixList = (value) => {
  if (!value || typeof value !== 'string') return [];
  return value.split(',').map(p => p.trim()).filter(p => p.length > 0);
};

// Utility: Validate action values (supports comma-separated list)
const validateActions = (actions, paramName) => {
  if (!actions) return [];

  const actionList = String(actions)
    .split(',')
    .map(a => a.trim().toLowerCase())
    .filter(a => a.length > 0);

  // Validate each action
  for (const action of actionList) {
    if (!VALID_ACTIONS.has(action)) {
      throw new Error(
        `${paramName} contains invalid action '${action}'. Must be one of: ${Array.from(VALID_ACTIONS).join(', ')}`
      );
    }
  }

  // Validate combinations
  const hasBlock = actionList.includes('block');
  const hasAsis = actionList.includes('asis');

  if (hasBlock && actionList.length > 1) {
    throw new Error(`${paramName}: 'block' cannot be combined with other actions`);
  }

  if (hasAsis && actionList.length > 1) {
    throw new Error(`${paramName}: 'asis' cannot be combined with other actions`);
  }

  return actionList;
};

// Utility: Validate except action values (supports comma-separated list)
const validateExceptActions = (actions, paramName) => {
  if (!actions) return [];

  const actionList = String(actions)
    .split(',')
    .map(a => a.trim().toLowerCase())
    .filter(a => a.length > 0);

  // Validate each action
  for (const action of actionList) {
    // Must end with -except
    if (!action.endsWith('-except')) {
      throw new Error(
        `${paramName} contains invalid action '${action}'. All actions must use -except suffix (e.g., 'block-except', 'skip-sign-except')`
      );
    }

    // Check if it's a valid except action
    if (!VALID_EXCEPT_ACTIONS.has(action)) {
      throw new Error(
        `${paramName} contains invalid action '${action}'. Must be one of: ${Array.from(VALID_EXCEPT_ACTIONS).join(', ')}`
      );
    }
  }

  // Validate combinations (same rules as regular actions)
  const hasBlock = actionList.includes('block-except');
  const hasAsis = actionList.includes('asis-except');

  if (hasBlock && actionList.length > 1) {
    throw new Error(`${paramName}: 'block-except' cannot be combined with other actions`);
  }

  if (hasAsis && actionList.length > 1) {
    throw new Error(`${paramName}: 'asis-except' cannot be combined with other actions`);
  }

  return actionList;
};

// Ensure required environment variables are set
const ensureRequiredEnv = (env) => {
  REQUIRED_ENV.forEach((key) => {
    if (!env[key] || String(env[key]).trim() === '') {
      throw new Error(`environment variable ${key} is required`);
    }
  });
};

/**
 * Calculate filepath quota limit based on configuration and filesize.
 * @param {{type: 'static'|'dynamic', value: number}} config
 * @param {number} filesize
 * @returns {number}
 */
function calculateFilepathQuota(config, filesize) {
  if (!config || config.value <= 0) {
    return 0;
  }

  if (config.type === 'dynamic') {
    if (!filesize || filesize <= 0) {
      console.warn('[Bandwidth Quota] Dynamic quota requires filesize, returning 0');
      return 0;
    }
    const quota = Math.floor(filesize * config.value);
    console.log(`[Bandwidth Quota] Dynamic filepath quota: ${filesize} bytes * ${config.value} = ${quota} bytes (${(quota / (1024 * 1024 * 1024)).toFixed(2)}GB)`);
    return quota;
  }

  console.log(`[Bandwidth Quota] Static filepath quota: ${config.value} bytes (${(config.value / (1024 * 1024 * 1024)).toFixed(2)}GB)`);
  return config.value;
}

/**
 * Wrap a ReadableStream and track downstream byte usage.
 * Ensures finalize callback executes exactly once with transfer stats.
 */
function wrapReadableWithAccounting(input, finalize) {
  if (!input || typeof input.getReader !== 'function') {
    const finalizePromise = Promise.resolve();
    return {
      stream: input,
      finalizeOnce: async () => {
        try {
          await finalize({ status: 'complete', bytes: 0 });
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          console.error('[Bandwidth Accounting] Finalize failed:', message);
        }
      },
      finalizePromise,
    };
  }

  let bytes = 0;
  let finalized = false;
  let reader = null;
  let finalizeResolver;
  const finalizePromise = new Promise((resolve) => {
    finalizeResolver = resolve;
  });

  const finalizeOnce = async (status) => {
    if (finalized) {
      return;
    }
    finalized = true;
    try {
      await finalize({ status, bytes });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error('[Bandwidth Accounting] Finalize failed:', message);
    }
    finalizeResolver?.();
  };

  const stream = new ReadableStream({
    start() {
      reader = input.getReader();
    },
    async pull(controller) {
      if (!reader) {
        controller.close();
        await finalizeOnce('complete');
        return;
      }

      try {
        const { done, value } = await reader.read();
        if (done) {
          controller.close();
          await finalizeOnce('complete');
          return;
        }

        if (value) {
          bytes += value.byteLength ?? 0;
          controller.enqueue(value);
        } else {
          controller.enqueue(value);
        }
      } catch (error) {
        try {
          controller.error(error);
        } catch {
          // controller may already be closed
        }
        await finalizeOnce('errored');
      }
    },
    async cancel(reason) {
      try {
        await reader?.cancel(reason);
      } catch {
        // ignore cancel errors
      }
      await finalizeOnce('canceled');
    }
  });

  return { stream, finalizeOnce, finalizePromise };
}

function createBandwidthQuotaExceededResponse(origin, bandwidthResult) {
  const safeHeaders = new Headers();
  safeHeaders.set('content-type', 'application/json;charset=UTF-8');
  safeHeaders.set('Access-Control-Allow-Origin', origin);
  safeHeaders.append('Vary', 'Origin');

  let message = 'Bandwidth quota exceeded';
  let retryAfter = 0;

  if (bandwidthResult && bandwidthResult.iprangeAllowed === false) {
    const bytesUsedGB = ((bandwidthResult.iprangeBytesUsed || 0) / (1024 * 1024 * 1024)).toFixed(2);
    message = `IP range bandwidth quota exceeded (used: ${bytesUsedGB}GB)`;
    retryAfter = bandwidthResult.iprangeRetryAfter || 0;
  } else if (bandwidthResult && bandwidthResult.filepathAllowed === false) {
    const bytesUsedGB = ((bandwidthResult.filepathBytesUsed || 0) / (1024 * 1024 * 1024)).toFixed(2);
    message = `File bandwidth quota exceeded (used: ${bytesUsedGB}GB)`;
    retryAfter = bandwidthResult.filepathRetryAfter || 0;
  }

  if (retryAfter > 0) {
    safeHeaders.set('Retry-After', String(Math.max(1, Math.ceil(retryAfter))));
  }

  return new Response(
    JSON.stringify({ code: 429, message, 'retry-after': retryAfter }),
    { status: 429, headers: safeHeaders }
  );
}

// Resolve configuration from environment variables
const resolveConfig = (env = {}) => {
  ensureRequiredEnv(env);

  const normalizeString = (value, defaultValue = '') => {
    if (value === undefined || value === null) return defaultValue;
    if (typeof value !== 'string') return defaultValue;
    const trimmed = value.trim();
    return trimmed === '' ? defaultValue : trimmed;
  };

  const blacklistPrefixes = parsePrefixList(env.BLACKLIST_PREFIX);
  const whitelistPrefixes = parsePrefixList(env.WHITELIST_PREFIX);
  const exceptPrefixes = parsePrefixList(env.EXCEPT_PREFIX);
  const blacklistActions = validateActions(env.BLACKLIST_ACTION, 'BLACKLIST_ACTION');
  const whitelistActions = validateActions(env.WHITELIST_ACTION, 'WHITELIST_ACTION');
  const exceptActions = validateExceptActions(env.EXCEPT_ACTION, 'EXCEPT_ACTION');

  const dbMode = normalizeString(env.DB_MODE);
  const normalizedDbMode = dbMode ? dbMode.toLowerCase() : '';
  const enableCfRatelimiter = normalizeString(env.ENABLE_CF_RATELIMITER, 'false').toLowerCase() === 'true';
  const cfRatelimiterBinding = normalizeString(env.CF_RATELIMITER_BINDING, 'CF_RATE_LIMITER');

  const linkTTL = normalizeString(env.LINK_TTL, '30m');
  const linkTTLSeconds = parseWindowTime(linkTTL);
  const cleanupPercentage = parseNumber(env.CLEANUP_PERCENTAGE, 1);
  const cleanupProbability = Math.max(0, Math.min(100, cleanupPercentage)) / 100;

  const downloadCacheTableName = (() => {
    const explicit = normalizeString(env.DOWNLOAD_CACHE_TABLE);
    if (explicit) return explicit;
    const legacyD1 = normalizeString(env.D1_TABLE_NAME);
    if (legacyD1) return legacyD1;
    const legacyPg = normalizeString(env.POSTGREST_TABLE_NAME);
    if (legacyPg) return legacyPg;
    return 'DOWNLOAD_CACHE_TABLE';
  })();
  const throttleTableName = normalizeString(env.THROTTLE_PROTECTION_TABLE, 'THROTTLE_PROTECTION');
  const rateLimitTableName = normalizeString(env.DOWNLOAD_IP_RATELIMIT_TABLE, 'DOWNLOAD_IP_RATELIMIT_TABLE');

  const windowTime = normalizeString(env.WINDOW_TIME);
  const windowTimeSeconds = parseWindowTime(windowTime);
  const ipSubnetLimit = parseInteger(env.IPSUBNET_WINDOWTIME_LIMIT, 0);
  const ipv4Suffix = normalizeString(env.IPV4_SUFFIX, '/32');
  const ipv6Suffix = normalizeString(env.IPV6_SUFFIX, '/60');
  const pgErrorHandleRaw = normalizeString(env.PG_ERROR_HANDLE, 'fail-closed').toLowerCase();
  const pgErrorHandle = pgErrorHandleRaw === 'fail-open' ? 'fail-open' : 'fail-closed';
  const blockTime = normalizeString(env.BLOCK_TIME, '10m');
  const blockTimeSeconds = parseWindowTime(blockTime);

  const throttleProtectHostname = normalizeString(env.THROTTLE_PROTECT_HOSTNAME);
  const throttleTimeWindow = normalizeString(env.THROTTLE_TIME_WINDOW, '60s');
  const throttleTimeWindowSeconds = parseWindowTime(throttleTimeWindow);
  const throttleProtectHttpCodeRaw = normalizeString(env.THROTTLE_PROTECT_HTTP_CODE, '429,500,503');
  const throttleProtectHttpCodes = throttleProtectHttpCodeRaw
    ? throttleProtectHttpCodeRaw
        .split(',')
        .map((code) => code.trim())
        .filter((code) => code.length > 0)
        .map((code) => Number.parseInt(code, 10))
        .filter((code) => Number.isInteger(code) && code >= 100 && code <= 599)
    : [];

  const throttleHostnamePatterns = throttleProtectHostname
    ? throttleProtectHostname.split(',').map((p) => p.trim()).filter((p) => p.length > 0)
    : [];

  const parseVerifyValues = (value) => {
    if (!value || typeof value !== 'string') {
      return [];
    }
    return value
      .split(',')
      .map((entry) => entry.trim())
      .filter((entry) => entry.length > 0);
  };

  const verifyHeaders = parseVerifyValues(normalizeString(env.VERIFY_HEADER));
  const verifySecrets = parseVerifyValues(normalizeString(env.VERIFY_SECRET));

  if (verifyHeaders.length > 0 && verifySecrets.length > 0 && verifyHeaders.length !== verifySecrets.length) {
    throw new Error('VERIFY_HEADER and VERIFY_SECRET must have the same number of comma-separated entries');
  }

  const d1DatabaseBinding = normalizeString(env.D1_DATABASE_BINDING, 'DB');
  const d1AccountId = normalizeString(env.D1_ACCOUNT_ID);
  const d1DatabaseId = normalizeString(env.D1_DATABASE_ID);
  const d1ApiToken = normalizeString(env.D1_API_TOKEN);
  const postgrestUrl = normalizeString(env.POSTGREST_URL);

  const quotaLimitTotalEnabled = parseBoolean(env.QUOTA_LIMIT_TOTAL_ENABLED, false);
  const quotaLimitFilepathEnabled = parseBoolean(env.QUOTA_LIMIT_FILEPATH_ENABLED, false);

  const bandwidthIprangeLimitRaw = normalizeString(env.IPSUBNET_BANDWIDTH_LIMIT, '10GB');
  const bandwidthIprangeLimit = parseBandwidthSize(bandwidthIprangeLimitRaw);

  const bandwidthFilepathLimitRaw = normalizeString(env.IPSUBNET_FILEPATH_BANDWIDTH_LIMIT, '2.2x');
  const bandwidthFilepathLimitParsed = parseDynamicQuota(bandwidthFilepathLimitRaw);

  const bandwidthWindowTimeTotal = normalizeString(env.BANDWIDTH_WINDOW_TIME_TOTAL, '4h');
  const bandwidthWindowTimeTotalSeconds = parseWindowTime(bandwidthWindowTimeTotal);

  const bandwidthWindowTimeFilepath = normalizeString(env.BANDWIDTH_WINDOW_TIME_FILEPATH, '4h');
  const bandwidthWindowTimeFilepathSeconds = parseWindowTime(bandwidthWindowTimeFilepath);

  const bandwidthBlockTime = normalizeString(env.BANDWIDTH_BLOCK_TIME, '10m');
  const bandwidthBlockTimeSeconds = parseWindowTime(bandwidthBlockTime);

  const bandwidthIprangeTableName = normalizeString(env.BANDWIDTH_IPRANGE_TABLE, 'IPRANGE_BANDWIDTH_QUOTA_TABLE');
  const bandwidthFilepathTableName = normalizeString(env.BANDWIDTH_FILEPATH_TABLE, 'IPRANGE_FILEPATH_BANDWIDTH_QUOTA_TABLE');
  const bandwidthParamsProvided = [
    env.QUOTA_LIMIT_TOTAL_ENABLED,
    env.QUOTA_LIMIT_FILEPATH_ENABLED,
    env.IPSUBNET_BANDWIDTH_LIMIT,
    env.IPSUBNET_FILEPATH_BANDWIDTH_LIMIT,
    env.BANDWIDTH_WINDOW_TIME_TOTAL,
    env.BANDWIDTH_WINDOW_TIME_FILEPATH,
    env.BANDWIDTH_BLOCK_TIME,
    env.BANDWIDTH_IPRANGE_TABLE,
    env.BANDWIDTH_FILEPATH_TABLE,
  ].some((value) => value !== undefined);
  let bandwidthQuotaEnabled = false;
  let bandwidthQuotaConfig = null;

  let cacheEnabled = false;
  let cacheConfig = {};

  if (dbMode) {
    if (normalizedDbMode === 'd1') {
      if (d1DatabaseBinding && linkTTLSeconds > 0) {
        cacheEnabled = true;
        cacheConfig = {
          env,
          databaseBinding: d1DatabaseBinding,
          tableName: downloadCacheTableName,
          linkTTL: linkTTLSeconds,
          cleanupProbability,
        };
      } else {
        throw new Error('DB_MODE is set to "d1" but LINK_TTL is missing or invalid');
      }
    } else if (normalizedDbMode === 'd1-rest') {
      if (d1AccountId && d1DatabaseId && d1ApiToken && linkTTLSeconds > 0) {
        cacheEnabled = true;
        cacheConfig = {
          accountId: d1AccountId,
          databaseId: d1DatabaseId,
          apiToken: d1ApiToken,
          tableName: downloadCacheTableName,
          linkTTL: linkTTLSeconds,
          cleanupProbability,
        };
      } else {
        throw new Error('DB_MODE is set to "d1-rest" but required environment variables are missing: D1_ACCOUNT_ID, D1_DATABASE_ID, D1_API_TOKEN, LINK_TTL');
      }
    } else if (normalizedDbMode === 'custom-pg-rest') {
      if (postgrestUrl && verifyHeaders.length > 0 && verifySecrets.length > 0 && linkTTLSeconds > 0) {
        cacheEnabled = true;
        cacheConfig = {
          postgrestUrl,
          verifyHeader: verifyHeaders,
          verifySecret: verifySecrets,
          tableName: downloadCacheTableName,
          linkTTL: linkTTLSeconds,
          cleanupProbability,
        };
      } else {
        throw new Error('DB_MODE is set to "custom-pg-rest" but required environment variables are missing: POSTGREST_URL, VERIFY_HEADER, VERIFY_SECRET, LINK_TTL');
      }
    } else {
      throw new Error(`Invalid DB_MODE: "${dbMode}". Valid options are: "d1", "d1-rest", "custom-pg-rest"`);
    }
  }

  const throttleEnabled = throttleHostnamePatterns.length > 0 && Boolean(dbMode);
  let throttleConfig = {};
  if (throttleEnabled) {
    if (normalizedDbMode === 'd1') {
      throttleConfig = {
        env,
        databaseBinding: d1DatabaseBinding,
        tableName: throttleTableName,
        throttleTimeWindow: throttleTimeWindowSeconds,
        cleanupProbability,
        protectedHttpCodes: throttleProtectHttpCodes,
      };
    } else if (normalizedDbMode === 'd1-rest') {
      if (!d1AccountId || !d1DatabaseId || !d1ApiToken) {
        throw new Error('Throttle protection requires D1 account configuration when DB_MODE is "d1-rest"');
      }
      throttleConfig = {
        accountId: d1AccountId,
        databaseId: d1DatabaseId,
        apiToken: d1ApiToken,
        tableName: throttleTableName,
        throttleTimeWindow: throttleTimeWindowSeconds,
        cleanupProbability,
        protectedHttpCodes: throttleProtectHttpCodes,
      };
    } else if (normalizedDbMode === 'custom-pg-rest') {
      if (!postgrestUrl || verifyHeaders.length === 0 || verifySecrets.length === 0) {
        throw new Error('Throttle protection requires POSTGREST_URL, VERIFY_HEADER, and VERIFY_SECRET when DB_MODE is "custom-pg-rest"');
      }
      throttleConfig = {
        postgrestUrl,
        verifyHeader: verifyHeaders,
        verifySecret: verifySecrets,
        tableName: throttleTableName,
        throttleTimeWindow: throttleTimeWindowSeconds,
        cleanupProbability,
        protectedHttpCodes: throttleProtectHttpCodes,
      };
    }
  }

  const rateLimitParamsProvided = windowTime !== '' || env.IPSUBNET_WINDOWTIME_LIMIT !== undefined;
  let rateLimitEnabled = false;
  let rateLimitConfig = {};
  if (dbMode && windowTimeSeconds > 0 && ipSubnetLimit > 0) {
    if (normalizedDbMode === 'd1') {
      rateLimitEnabled = true;
      rateLimitConfig = {
        env,
        databaseBinding: d1DatabaseBinding,
        tableName: rateLimitTableName,
        windowTimeSeconds,
        limit: ipSubnetLimit,
        ipv4Suffix,
        ipv6Suffix,
        pgErrorHandle,
        cleanupProbability,
        blockTimeSeconds,
      };
    } else if (normalizedDbMode === 'd1-rest') {
      if (!d1AccountId || !d1DatabaseId || !d1ApiToken) {
        throw new Error('Rate limiting requires D1 account configuration when DB_MODE is "d1-rest"');
      }
      rateLimitEnabled = true;
      rateLimitConfig = {
        accountId: d1AccountId,
        databaseId: d1DatabaseId,
        apiToken: d1ApiToken,
        tableName: rateLimitTableName,
        windowTimeSeconds,
        limit: ipSubnetLimit,
        ipv4Suffix,
        ipv6Suffix,
        pgErrorHandle,
        cleanupProbability,
        blockTimeSeconds,
      };
    } else if (normalizedDbMode === 'custom-pg-rest') {
      if (!postgrestUrl || verifyHeaders.length === 0 || verifySecrets.length === 0) {
        throw new Error('Rate limiting requires POSTGREST_URL, VERIFY_HEADER, and VERIFY_SECRET when DB_MODE is "custom-pg-rest"');
      }
      rateLimitEnabled = true;
      rateLimitConfig = {
        postgrestUrl,
        verifyHeader: verifyHeaders,
        verifySecret: verifySecrets,
        tableName: rateLimitTableName,
        windowTimeSeconds,
        limit: ipSubnetLimit,
        ipv4Suffix,
        ipv6Suffix,
        pgErrorHandle,
        cleanupProbability,
        blockTimeSeconds,
      };
    }
  } else if (dbMode && rateLimitParamsProvided && (!windowTimeSeconds || !ipSubnetLimit)) {
    throw new Error('Rate limiting configuration is incomplete. Ensure WINDOW_TIME and IPSUBNET_WINDOWTIME_LIMIT are valid positive values.');
  }

  if (dbMode && (quotaLimitTotalEnabled || quotaLimitFilepathEnabled)) {
    if (normalizedDbMode === 'd1') {
      bandwidthQuotaEnabled = true;
      bandwidthQuotaConfig = {
        env,
        databaseBinding: d1DatabaseBinding,
        totalEnabled: quotaLimitTotalEnabled,
        filepathEnabled: quotaLimitFilepathEnabled,
        iprangeLimit: bandwidthIprangeLimit,
        filepathLimitConfig: bandwidthFilepathLimitParsed,
        windowTimeTotalSeconds: bandwidthWindowTimeTotalSeconds,
        windowTimeFilepathSeconds: bandwidthWindowTimeFilepathSeconds,
        blockTimeSeconds: bandwidthBlockTimeSeconds,
        iprangeTableName: bandwidthIprangeTableName,
        filepathTableName: bandwidthFilepathTableName,
        ipv4Suffix,
        ipv6Suffix,
        pgErrorHandle,
        cleanupProbability,
      };
    } else if (normalizedDbMode === 'd1-rest') {
      if (!d1AccountId || !d1DatabaseId || !d1ApiToken) {
        throw new Error('Bandwidth quota requires D1 account configuration when DB_MODE is "d1-rest"');
      }
      bandwidthQuotaEnabled = true;
      bandwidthQuotaConfig = {
        accountId: d1AccountId,
        databaseId: d1DatabaseId,
        apiToken: d1ApiToken,
        totalEnabled: quotaLimitTotalEnabled,
        filepathEnabled: quotaLimitFilepathEnabled,
        iprangeLimit: bandwidthIprangeLimit,
        filepathLimitConfig: bandwidthFilepathLimitParsed,
        windowTimeTotalSeconds: bandwidthWindowTimeTotalSeconds,
        windowTimeFilepathSeconds: bandwidthWindowTimeFilepathSeconds,
        blockTimeSeconds: bandwidthBlockTimeSeconds,
        iprangeTableName: bandwidthIprangeTableName,
        filepathTableName: bandwidthFilepathTableName,
        ipv4Suffix,
        ipv6Suffix,
        pgErrorHandle,
        cleanupProbability,
      };
    } else if (normalizedDbMode === 'custom-pg-rest') {
      if (!postgrestUrl || verifyHeaders.length === 0 || verifySecrets.length === 0) {
        throw new Error('Bandwidth quota requires POSTGREST_URL, VERIFY_HEADER, and VERIFY_SECRET when DB_MODE is "custom-pg-rest"');
      }
      bandwidthQuotaEnabled = true;
      bandwidthQuotaConfig = {
        postgrestUrl,
        verifyHeader: verifyHeaders,
        verifySecret: verifySecrets,
        totalEnabled: quotaLimitTotalEnabled,
        filepathEnabled: quotaLimitFilepathEnabled,
        iprangeLimit: bandwidthIprangeLimit,
        filepathLimitConfig: bandwidthFilepathLimitParsed,
        windowTimeTotalSeconds: bandwidthWindowTimeTotalSeconds,
        windowTimeFilepathSeconds: bandwidthWindowTimeFilepathSeconds,
        blockTimeSeconds: bandwidthBlockTimeSeconds,
        iprangeTableName: bandwidthIprangeTableName,
        filepathTableName: bandwidthFilepathTableName,
        ipv4Suffix,
        ipv6Suffix,
        pgErrorHandle,
        cleanupProbability,
      };
    }
  } else if (
    dbMode &&
    bandwidthParamsProvided &&
    !quotaLimitTotalEnabled &&
    !quotaLimitFilepathEnabled
  ) {
    console.warn('[Bandwidth Quota] Configuration provided but both QUOTA_LIMIT_TOTAL_ENABLED and QUOTA_LIMIT_FILEPATH_ENABLED are false');
  }

  if (enableCfRatelimiter) {
    const ratelimiter = env[cfRatelimiterBinding];
    if (!ratelimiter || typeof ratelimiter.limit !== 'function') {
      throw new Error(
        `ENABLE_CF_RATELIMITER is true but binding "${cfRatelimiterBinding}" not found or invalid. Please configure [[rate_limit]] binding in wrangler.toml with name="${cfRatelimiterBinding}".`
      );
    }
  }

  return {
    address: String(env.ADDRESS).trim(),
    token: String(env.TOKEN).trim(),
    workerAddress: String(env.WORKER_ADDRESS).trim(),
    verifyHeader: verifyHeaders,
    verifySecret: verifySecrets,
    signCheck: parseBoolean(env.SIGN_CHECK, true),
    hashCheck: parseBoolean(env.HASH_CHECK, true),
    workerCheck: parseBoolean(env.WORKER_CHECK, true),
    ipCheck: parseBoolean(env.IP_CHECK, true),
    additionCheck: parseBoolean(env.ADDITION_CHECK, true),
    additionExpireTimeCheck: parseBoolean(env.ADDITION_EXPIRETIME_CHECK, true),
    ipv4Only: parseBoolean(env.IPV4_ONLY, true),
    blacklistPrefixes,
    whitelistPrefixes,
    exceptPrefixes,
    blacklistActions,
    whitelistActions,
    exceptActions,
    dbMode,
    cacheEnabled,
    cacheConfig,
    throttleEnabled,
    throttleHostnamePatterns,
    throttleConfig,
    rateLimitEnabled,
    rateLimitConfig,
    bandwidthQuotaEnabled,
    bandwidthQuotaConfig,
    windowTime,
    ipSubnetLimit,
    enableCfRatelimiter,
    cfRatelimiterBinding,
    ipv4Suffix,
    ipv6Suffix,
  };
};

// Helper function to check if an IP is IPv6
function isIPv6(ip) {
  return ip && ip.includes(':');
}

function base64Encode(input) {
  const bytes = new TextEncoder().encode(input);
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary);
}

function base64DecodeToString(input) {
  if (!input) {
    return null;
  }
  try {
    const normalized = (() => {
      const remainder = input.length % 4;
      if (remainder === 0) return input;
      if (remainder === 1) return null;
      const padding = 4 - remainder;
      return `${input}${'='.repeat(padding)}`;
    })();
    if (normalized === null) {
      return null;
    }
    const binary = atob(normalized);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i += 1) {
      bytes[i] = binary.charCodeAt(i);
    }
    return new TextDecoder().decode(bytes);
  } catch (_error) {
    return null;
  }
}

async function sha256Hex(text) {
  const data = new TextEncoder().encode(text);
  const hashBuffer = await crypto.subtle.digest("SHA-256", data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map((b) => b.toString(16).padStart(2, "0")).join("");
}

// Check if a path matches blacklist or whitelist and return the actions array
const checkPathListAction = (path, config) => {
  let decodedPath;
  try {
    decodedPath = decodeURIComponent(path);
  } catch (error) {
    // If path cannot be decoded, use as-is
    decodedPath = path;
  }

  // Check blacklist first (highest priority)
  if (config.blacklistPrefixes.length > 0 && config.blacklistActions.length > 0) {
    for (const prefix of config.blacklistPrefixes) {
      if (decodedPath.startsWith(prefix)) {
        return config.blacklistActions;
      }
    }
  }

  // Check whitelist second (second priority)
  if (config.whitelistPrefixes.length > 0 && config.whitelistActions.length > 0) {
    for (const prefix of config.whitelistPrefixes) {
      if (decodedPath.startsWith(prefix)) {
        return config.whitelistActions;
      }
    }
  }

  // Check exception list third (third priority) - inverse matching logic
  if (config.exceptPrefixes.length > 0 && config.exceptActions.length > 0) {
    // Check if path matches any except prefix
    let matchesExceptPrefix = false;
    for (const prefix of config.exceptPrefixes) {
      if (decodedPath.startsWith(prefix)) {
        matchesExceptPrefix = true;
        break;
      }
    }

    // If path does NOT match except prefix, apply the actions (remove -except suffix)
    if (!matchesExceptPrefix) {
      return config.exceptActions.map(action => action.replace('-except', ''));
    }
    // If path matches except prefix, use default behavior (return empty array)
  }

  // No match - use default behavior
  return [];
};

// src/verify.ts
const verify = async (label, data, _sign, token) => {
  if (!_sign) {
    return `${label} missing`;
  }
  const signSlice = _sign.split(":");
  if (!signSlice[signSlice.length - 1]) {
    return `${label} expire missing`;
  }
  const expire = parseInt(signSlice[signSlice.length - 1]);
  if (isNaN(expire)) {
    return `${label} expire invalid`;
  }
  if (expire < Date.now() / 1e3 && expire > 0) {
    return `${label} expired`;
  }
  const right = await hmacSha256Sign(data, expire, token);
  if (_sign !== right) {
    return `${label} mismatch`;
  }
  return "";
};
const hmacSha256Sign = async (data, expire, token) => {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(token),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"]
  );
  const buf = await crypto.subtle.sign(
    {
      name: "HMAC",
      hash: "SHA-256"
    },
    key,
    new TextEncoder().encode(`${data}:${expire}`)
  );
  return btoa(String.fromCharCode(...new Uint8Array(buf))).replace(/\+/g, "-").replace(/\//g, "_") + ":" + expire;
};

function createErrorResponse(origin, status, message) {
  const safeHeaders = new Headers();
  safeHeaders.set("content-type", "application/json;charset=UTF-8");
  safeHeaders.set("Access-Control-Allow-Origin", origin);
  safeHeaders.append("Vary", "Origin");

  return new Response(
    JSON.stringify({
      code: status,
      message
    }),
    {
      status,
      headers: safeHeaders
    }
  );
}

function createUnauthorizedResponse(origin, message) {
  return createErrorResponse(origin, 401, message);
}

const formatRateLimitWindow = (windowLabel, windowSeconds) => {
  if (windowLabel) {
    return windowLabel;
  }
  if (!windowSeconds || windowSeconds <= 0) {
    return 'configured window';
  }
  if (windowSeconds % 3600 === 0) {
    return `${windowSeconds / 3600}h`;
  }
  if (windowSeconds % 60 === 0) {
    return `${windowSeconds / 60}m`;
  }
  return `${windowSeconds}s`;
};

function createRateLimitResponse(origin, ipSubnet, limit, windowLabel, retryAfterSeconds) {
  const safeHeaders = new Headers();
  safeHeaders.set("content-type", "application/json;charset=UTF-8");
  safeHeaders.set("Access-Control-Allow-Origin", origin);
  safeHeaders.append("Vary", "Origin");

  const sanitizedRetryAfter = retryAfterSeconds && retryAfterSeconds > 0
    ? Math.max(1, Math.ceil(retryAfterSeconds))
    : 0;
  if (sanitizedRetryAfter) {
    safeHeaders.set("Retry-After", String(sanitizedRetryAfter));
  }

  const payload = {
    code: 429,
    message: `${ipSubnet || 'current client'} exceeds the limit of ${limit} requests in ${windowLabel}`
  };
  if (sanitizedRetryAfter) {
    payload['retry-after'] = sanitizedRetryAfter;
  }

  return new Response(JSON.stringify(payload), {
    status: 429,
    headers: safeHeaders
  });
}

function safeDecodePathname(pathname) {
  try {
    return decodeURIComponent(pathname);
  } catch {
    return null;
  }
}
// src/handleDownload.ts
async function handleDownload(request, env, config, cacheManager, throttleManager, rateLimiter, ctx) {
  const origin = request.headers.get("origin") ?? "*";
  const url = new URL(request.url);
  const path = safeDecodePathname(url.pathname);
  if (path === null) {
    return createErrorResponse(origin, 400, "invalid path encoding");
  }

  // Check blacklist/whitelist
  const actions = checkPathListAction(path, config);

  // Handle block action
  if (actions.includes('block')) {
    return createErrorResponse(origin, 403, "access denied");
  }

  const clientIP = request.headers.get("CF-Connecting-IP") || "";

  const bandwidthConfig = config.bandwidthQuotaConfig || null;
  let bandwidthManager = null;
  if (config.bandwidthQuotaEnabled) {
    try {
      bandwidthManager = createBandwidthQuotaManager(config.dbMode);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error('[Bandwidth Quota] Manager initialization failed:', message);
    }
  }

  let filesize = 0;

  // CF Rate Limiter检查（第一道防线）
  if (config.enableCfRatelimiter) {
    try {
      const cfResult = await checkCfRatelimit(
        env,
        clientIP,
        config.ipv4Suffix,
        config.ipv6Suffix,
        config.cfRatelimiterBinding
      );

      if (!cfResult.allowed) {
        console.error(`[CF Rate Limiter] Blocked IP subnet: ${cfResult.ipSubnet}`);
        return new Response('429 Too Many Requests - Rate limit exceeded', {
          status: 429,
          headers: {
            'Content-Type': 'text/plain',
            'Retry-After': '60',
          },
        });
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error('[CF Rate Limiter] Error during check:', message);
      // fail-open: continue processing if rate limiter check fails
    }
  }

  // Initialize check flags from config (each *_CHECK only controls itself)
  let shouldCheckSign = config.signCheck;
  let shouldCheckHash = config.hashCheck;
  let shouldCheckWorker = config.workerCheck;
  let shouldCheckIP = config.ipCheck;

  // Apply action overrides (unless 'asis' is specified)
  // Each skip-* only affects its own check, completely decoupled
  if (!actions.includes('asis')) {
    if (actions.includes('skip-sign')) {
      shouldCheckSign = false;
    }
    if (actions.includes('skip-hash')) {
      shouldCheckHash = false;
    }
    if (actions.includes('skip-worker')) {
      shouldCheckWorker = false;
    }
    if (actions.includes('skip-ip')) {
      shouldCheckIP = false;
    }
  }

  // Sign verification
  const sign = url.searchParams.get("sign") ?? "";
  if (shouldCheckSign) {
    const verifyResult = await verify("sign", path, sign, config.token);
    if (verifyResult !== "") {
      return createUnauthorizedResponse(origin, verifyResult);
    }
  }

  // HashSign verification
  const hashSign = url.searchParams.get("hashSign") ?? "";
  if (shouldCheckHash) {
    const base64Path = base64Encode(path);
    const hashVerifyResult = await verify("hashSign", base64Path, hashSign, config.token);
    if (hashVerifyResult !== "") {
      return createUnauthorizedResponse(origin, hashVerifyResult);
    }
  }

  // WorkerSign verification
  const workerSign = url.searchParams.get("workerSign") ?? "";
  if (shouldCheckWorker) {
    const workerVerifyData = JSON.stringify({ path: path, worker_addr: config.workerAddress });
    const workerVerifyResult = await verify("workerSign", workerVerifyData, workerSign, config.token);
    if (workerVerifyResult !== "") {
      return createUnauthorizedResponse(origin, workerVerifyResult);
    }
  }

  // IpSign verification
  const ipSign = url.searchParams.get("ipSign") ?? "";
  if (shouldCheckIP) {
    if (!ipSign) {
      return createUnauthorizedResponse(origin, "ipSign missing");
    }
    if (!clientIP) {
      return createUnauthorizedResponse(origin, "client ip missing");
    }
    const ipVerifyData = JSON.stringify({ path: path, ip: clientIP });
    const ipVerifyResult = await verify("ipSign", ipVerifyData, ipSign, config.token);
    if (ipVerifyResult !== "") {
      return createUnauthorizedResponse(origin, ipVerifyResult);
    }
  }

  const additionalInfo = url.searchParams.get("additionalInfo") ?? "";
  const additionalInfoSign = url.searchParams.get("additionalInfoSign") ?? "";
  if (config.additionCheck) {
    if (!additionalInfo) {
      return createUnauthorizedResponse(origin, "additionalInfo missing");
    }
    if (!additionalInfoSign) {
      return createUnauthorizedResponse(origin, "additionalInfoSign missing");
    }

    const additionalVerifyResult = await verify("additionalInfoSign", additionalInfo, additionalInfoSign, config.token);
    if (additionalVerifyResult !== "") {
      return createUnauthorizedResponse(origin, additionalVerifyResult);
    }

    const decodedAdditional = base64DecodeToString(additionalInfo);
    if (!decodedAdditional) {
      return createUnauthorizedResponse(origin, "additionalInfo decode failed");
    }

    let additionalPayload;
    try {
      additionalPayload = JSON.parse(decodedAdditional);
    } catch (_error) {
      return createUnauthorizedResponse(origin, "additionalInfo invalid");
    }

    const expectedPathHash = await sha256Hex(path);
    if (typeof additionalPayload.pathHash !== "string" || additionalPayload.pathHash !== expectedPathHash) {
      return createUnauthorizedResponse(origin, "additionalInfo path mismatch");
    }

    filesize = 0;
    if (typeof additionalPayload.filesize === 'number') {
      filesize = Math.max(0, Math.trunc(additionalPayload.filesize));
    } else if (typeof additionalPayload.filesize === 'string') {
      const parsed = Number.parseInt(additionalPayload.filesize, 10);
      if (Number.isFinite(parsed) && parsed > 0) {
        filesize = parsed;
      }
    }

    if (filesize > 0) {
      console.log(`[Additional Info] Extracted filesize: ${filesize} bytes (${(filesize / (1024 * 1024 * 1024)).toFixed(2)}GB)`);
    }

    if (config.additionExpireTimeCheck) {
      let expireTimestamp = 0;
      if (typeof additionalPayload.expireTime === "number") {
        expireTimestamp = Math.trunc(additionalPayload.expireTime);
      } else if (typeof additionalPayload.expireTime === "string") {
        expireTimestamp = Number.parseInt(additionalPayload.expireTime, 10);
      }

      if (!Number.isFinite(expireTimestamp) || expireTimestamp <= 0) {
        return createUnauthorizedResponse(origin, "additionalInfo expire invalid");
      }

      const nowSeconds = Math.floor(Date.now() / 1000);
      if (nowSeconds > expireTimestamp) {
        return createUnauthorizedResponse(origin, "link expired");
      }
    }
  }

  // ========================================
  // UNIFIED CHECK (RTT 3→1 OPTIMIZATION)
  // ========================================
  let unifiedResult = null;
  let cacheHit = false;
  let linkData = null;
  
  // Use unified check for all database modes when rate limit is enabled
  const supportsUnifiedCheck = config.rateLimitEnabled && (
    config.dbMode === 'custom-pg-rest' ||
    config.dbMode === 'd1' ||
    config.dbMode === 'd1-rest'
  );

  const bandwidthWindowTotalSeconds = bandwidthConfig?.windowTimeTotalSeconds ?? 0;
  const bandwidthWindowFilepathSeconds = bandwidthConfig?.windowTimeFilepathSeconds ?? 0;
  const bandwidthBlockSeconds = bandwidthConfig?.blockTimeSeconds ?? 0;
  const bandwidthIprangeTableName = bandwidthConfig?.iprangeTableName || 'IPRANGE_BANDWIDTH_QUOTA_TABLE';
  const bandwidthFilepathTableName = bandwidthConfig?.filepathTableName || 'IPRANGE_FILEPATH_BANDWIDTH_QUOTA_TABLE';
  const bandwidthIprangeQuotaLimit = config.bandwidthQuotaEnabled && bandwidthConfig?.totalEnabled
    ? bandwidthConfig.iprangeLimit
    : 0;
  const bandwidthFilepathQuotaLimit = config.bandwidthQuotaEnabled && bandwidthConfig?.filepathEnabled
    ? calculateFilepathQuota(bandwidthConfig.filepathLimitConfig, filesize)
    : 0;

  if (supportsUnifiedCheck) {
    try {

      const rateLimitConfig = config.rateLimitConfig || {};
      const cacheConfig = config.cacheConfig || {};
      const throttleConfig = config.throttleConfig || {};
      const limitConfigValue = rateLimitConfig.limit ?? config.ipSubnetLimit;

      if (config.dbMode === 'custom-pg-rest') {
        unifiedResult = await unifiedCheck(path, clientIP, {
          postgrestUrl: rateLimitConfig.postgrestUrl,
          verifyHeader: rateLimitConfig.verifyHeader,
          verifySecret: rateLimitConfig.verifySecret,
          linkTTL: cacheConfig.linkTTL ?? 1800,
          cacheTableName: cacheConfig.tableName || 'DOWNLOAD_CACHE_TABLE',
          windowTimeSeconds: rateLimitConfig.windowTimeSeconds ?? 86400,
          limit: limitConfigValue ?? 100,
          blockTimeSeconds: rateLimitConfig.blockTimeSeconds ?? 600,
          ipv4Suffix: rateLimitConfig.ipv4Suffix ?? '/32',
          ipv6Suffix: rateLimitConfig.ipv6Suffix ?? '/60',
          rateLimitTableName: rateLimitConfig.tableName || 'DOWNLOAD_IP_RATELIMIT_TABLE',
          throttleTimeWindow: throttleConfig.throttleTimeWindow ?? 60,
          throttleTableName: throttleConfig.tableName || 'THROTTLE_PROTECTION',
          bandwidthIprangeQuota: bandwidthIprangeQuotaLimit,
          bandwidthFilepathQuota: bandwidthFilepathQuotaLimit,
          bandwidthWindowTotalSeconds,
          bandwidthWindowFilepathSeconds,
          bandwidthBlockSeconds,
          bandwidthIprangeTableName,
          bandwidthFilepathTableName,
        });
      } else if (config.dbMode === 'd1') {
        unifiedResult = await unifiedCheckD1(path, clientIP, {
          env: cacheConfig.env || rateLimitConfig.env,
          databaseBinding: cacheConfig.databaseBinding || rateLimitConfig.databaseBinding || 'DB',
          linkTTL: cacheConfig.linkTTL ?? 1800,
          cacheTableName: cacheConfig.tableName || 'DOWNLOAD_CACHE_TABLE',
          windowTimeSeconds: rateLimitConfig.windowTimeSeconds ?? 86400,
          limit: limitConfigValue ?? 100,
          blockTimeSeconds: rateLimitConfig.blockTimeSeconds ?? 600,
          ipv4Suffix: rateLimitConfig.ipv4Suffix ?? '/32',
          ipv6Suffix: rateLimitConfig.ipv6Suffix ?? '/60',
          rateLimitTableName: rateLimitConfig.tableName || 'DOWNLOAD_IP_RATELIMIT_TABLE',
          throttleTimeWindow: throttleConfig.throttleTimeWindow ?? 60,
          throttleTableName: throttleConfig.tableName || 'THROTTLE_PROTECTION',
          bandwidthIprangeQuota: bandwidthIprangeQuotaLimit,
          bandwidthFilepathQuota: bandwidthFilepathQuotaLimit,
          bandwidthWindowTotalSeconds,
          bandwidthWindowFilepathSeconds,
          bandwidthBlockSeconds,
          bandwidthIprangeTableName,
          bandwidthFilepathTableName,
        });
      } else if (config.dbMode === 'd1-rest') {
        unifiedResult = await unifiedCheckD1Rest(path, clientIP, {
          accountId: rateLimitConfig.accountId || throttleConfig.accountId || cacheConfig.accountId,
          databaseId: rateLimitConfig.databaseId || throttleConfig.databaseId || cacheConfig.databaseId,
          apiToken: rateLimitConfig.apiToken || throttleConfig.apiToken || cacheConfig.apiToken,
          linkTTL: cacheConfig.linkTTL ?? 1800,
          cacheTableName: cacheConfig.tableName || 'DOWNLOAD_CACHE_TABLE',
          windowTimeSeconds: rateLimitConfig.windowTimeSeconds ?? 86400,
          limit: limitConfigValue ?? 100,
          blockTimeSeconds: rateLimitConfig.blockTimeSeconds ?? 600,
          ipv4Suffix: rateLimitConfig.ipv4Suffix ?? '/32',
          ipv6Suffix: rateLimitConfig.ipv6Suffix ?? '/60',
          rateLimitTableName: rateLimitConfig.tableName || 'DOWNLOAD_IP_RATELIMIT_TABLE',
          throttleTimeWindow: throttleConfig.throttleTimeWindow ?? 60,
          throttleTableName: throttleConfig.tableName || 'THROTTLE_PROTECTION',
          bandwidthIprangeQuota: bandwidthIprangeQuotaLimit,
          bandwidthFilepathQuota: bandwidthFilepathQuotaLimit,
          bandwidthWindowTotalSeconds,
          bandwidthWindowFilepathSeconds,
          bandwidthBlockSeconds,
          bandwidthIprangeTableName,
          bandwidthFilepathTableName,
        });
      } else {
        throw new Error(`Unsupported database mode for unified check: ${config.dbMode}`);
      }
      
      // Check rate limit result
      if (!unifiedResult.rateLimit.allowed) {
        if (unifiedResult.rateLimit.error) {
          console.error('[Rate Limit] fail-closed error:', unifiedResult.rateLimit.error);
          return createErrorResponse(origin, 500, unifiedResult.rateLimit.error);
        }
        
        const windowLabel = formatRateLimitWindow(config.windowTime, config.rateLimitConfig?.windowTimeSeconds);
        console.warn(
          '[Rate Limit] Subnet blocked:',
          unifiedResult.rateLimit.ipSubnet || clientIP,
          `limit=${config.ipSubnetLimit}`,
          `window=${windowLabel}`,
          `retryAfter=${unifiedResult.rateLimit.retryAfter || 0}s`
        );
        return createRateLimitResponse(
          origin,
          unifiedResult.rateLimit.ipSubnet || clientIP,
          config.ipSubnetLimit,
          windowLabel,
          unifiedResult.rateLimit.retryAfter
        );
      }
      
      // Check cache result
      if (
        config.bandwidthQuotaEnabled &&
        unifiedResult.bandwidth &&
        (unifiedResult.bandwidth.iprangeAllowed === false || unifiedResult.bandwidth.filepathAllowed === false)
      ) {
        console.warn(
          '[Bandwidth Quota] Exceeded:',
          unifiedResult.bandwidth.iprangeAllowed === false
            ? `IP range bytes=${unifiedResult.bandwidth.iprangeBytesUsed}`
            : '',
          unifiedResult.bandwidth.filepathAllowed === false
            ? `filepath bytes=${unifiedResult.bandwidth.filepathBytesUsed}`
            : ''
        );
        return createBandwidthQuotaExceededResponse(origin, unifiedResult.bandwidth);
      }
      
      if (unifiedResult.cache.hit) {
        cacheHit = true;
        linkData = unifiedResult.cache.linkData;
      }
      
      // Check throttle result (if we have cache hit with hostname_hash)
      if (config.throttleEnabled && unifiedResult.throttle.status === 'protected') {
        console.log(`[Throttle] Protected from unified check, returning error ${unifiedResult.throttle.errorCode}, retry after ${unifiedResult.throttle.retryAfter}s`);
        
        const safeHeaders = new Headers();
        safeHeaders.set("content-type", "application/json;charset=UTF-8");
        safeHeaders.set("Access-Control-Allow-Origin", origin);
        safeHeaders.append("Vary", "Origin");
        safeHeaders.set("X-Throttle-Protected", "true");
        safeHeaders.set("X-Throttle-Retry-After", String(unifiedResult.throttle.retryAfter));
        
        return new Response(
          JSON.stringify({
            code: unifiedResult.throttle.errorCode || 503,
            message: `Service temporarily unavailable (throttle protected, retry after ${unifiedResult.throttle.retryAfter}s)`
          }),
          {
            status: unifiedResult.throttle.errorCode || 503,
            headers: safeHeaders
          }
        );
      }
      
      // Trigger probabilistic cleanup for rate limit
      if (rateLimiter && config.rateLimitConfig) {
        const probability = config.rateLimitConfig.cleanupProbability || 0.01;
        if (Math.random() < probability) {
          console.log(`[Rate Limit Cleanup] Triggered cleanup (probability: ${probability * 100}%)`);
          
          let cleanupPromise;
          
          if (config.dbMode === 'custom-pg-rest') {
            const { cleanupExpiredRecords } = await import('./ratelimit/custom-pg-rest.js');
            cleanupPromise = cleanupExpiredRecords(
              config.rateLimitConfig.postgrestUrl,
              config.rateLimitConfig.verifyHeader,
              config.rateLimitConfig.verifySecret,
              config.rateLimitConfig.tableName,
              config.rateLimitConfig.windowTimeSeconds
            ).catch((error) => {
              console.error('[Rate Limit Cleanup] Failed:', error instanceof Error ? error.message : String(error));
            });
          } else if (config.dbMode === 'd1') {
            // D1 cleanup is already triggered inside checkRateLimit's triggerCleanup
            // No need to trigger here
            console.log('[Rate Limit Cleanup] Skipped (D1 cleanup handled internally)');
          } else if (config.dbMode === 'd1-rest') {
            // D1-REST cleanup is already triggered inside checkRateLimit's triggerCleanup
            // No need to trigger here
            console.log('[Rate Limit Cleanup] Skipped (D1-REST cleanup handled internally)');
          }
          
          if (cleanupPromise && ctx && ctx.waitUntil) {
            ctx.waitUntil(cleanupPromise);
          }
        }
      }
      
  } catch (error) {
    console.error('[Unified Check] Failed:', error instanceof Error ? error.message : String(error));
    console.error('[Unified Check] Stack:', error.stack);

    // Respect PG_ERROR_HANDLE configuration
    const pgErrorHandle = config.rateLimitConfig?.pgErrorHandle || 'fail-closed';

    if (pgErrorHandle === 'fail-open') {
      console.warn('[Unified Check] Fail-open mode: allowing request despite error');
      // Reset unified check outputs so downstream logic can run without them
      unifiedResult = null;
      cacheHit = false;
      linkData = null;
      // Continue execution without returning
    } else {
      console.error('[Unified Check] Fail-closed mode: blocking request');
      return createErrorResponse(origin, 500, `Unified check failed: ${error.message}`);
    }
  }
} else {
    // Fallback to original logic when unified check is not supported
    
    if (rateLimiter && config.rateLimitEnabled && clientIP) {
      try {
        const rateLimitResult = await rateLimiter.checkRateLimit(clientIP, { ...config.rateLimitConfig, ctx });
        if (!rateLimitResult.allowed) {
          if (rateLimitResult.error) {
            console.error('[Rate Limit] fail-closed error:', rateLimitResult.error);
            return createErrorResponse(origin, 500, rateLimitResult.error);
          }
          const windowLabel = formatRateLimitWindow(config.windowTime, config.rateLimitConfig?.windowTimeSeconds);
          console.warn(
            '[Rate Limit] Subnet blocked:',
            rateLimitResult.ipSubnet || clientIP,
            `limit=${config.ipSubnetLimit}`,
            `window=${windowLabel}`,
            `retryAfter=${rateLimitResult.retryAfter || 0}s`
          );
          return createRateLimitResponse(
            origin,
            rateLimitResult.ipSubnet || clientIP,
            config.ipSubnetLimit,
            windowLabel,
            rateLimitResult.retryAfter
          );
        }
      } catch (error) {
        console.error('[Rate Limit] Unexpected error:', error instanceof Error ? error.message : String(error));
        if (config.rateLimitConfig?.pgErrorHandle === 'fail-closed') {
          return createErrorResponse(origin, 500, 'Rate limit check failed');
        }
      }
    }
  }

  // Check cache (if not already resolved by unified check)
  let res;
  if (cacheHit && linkData) {
    res = { code: 200, data: linkData };
  } else if (cacheManager && !unifiedResult) {
    try {
      const cached = await cacheManager.checkCache(path, { ...config.cacheConfig, ctx });
      if (cached && cached.linkData) {
        res = { code: 200, data: cached.linkData };
      }
    } catch (error) {
      console.error('[Cache] Check failed, fallback to API:', error instanceof Error ? error.message : String(error));
    }
  }

  // If cache miss or not enabled, call AList API
  if (!res) {
    // 发送请求到AList服务
    const headers = {
      "content-type": "application/json;charset=UTF-8",
      Authorization: config.token,
      "CF-Connecting-IP-WORKERS": clientIP, // Forward the client's IP address, since default CF-Connecting-IP will be overwritten by CF, we should include original CF-Connecting-IP and forward it into a new header.
    };
    applyVerifyHeaders(headers, config.verifyHeader, config.verifySecret);
    let resp = await fetch(`${config.address}/api/fs/link`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        path
      })
    });

    // 检查响应类型
    const contentType = resp.headers.get("content-type") || "";

    // 如果不是JSON格式，返回自定义错误响应
    if (!contentType.includes("application/json")) {
      // 获取原始响应的状态码
      const originalStatus = resp.status;
      // 创建一个简单的错误消息，不包含敏感信息
      const safeErrorMessage = JSON.stringify({
        code: originalStatus,
        message: `Request failed with status: ${originalStatus}`
      });

      // 创建全新的headers对象，只添加必要的安全headers
      const safeHeaders = new Headers();
      safeHeaders.set("content-type", "application/json;charset=UTF-8");
      safeHeaders.set("Access-Control-Allow-Origin", origin);
      safeHeaders.append("Vary", "Origin");

      const safeErrorResp = new Response(safeErrorMessage, {
        status: originalStatus,
        statusText: "Error",  // 使用通用状态文本
        headers: safeHeaders  // 使用安全的headers集合
      });

      return safeErrorResp;
    }

    // 如果是JSON，按原来的逻辑处理
    res = await resp.json();
    if (res.code !== 200) {
      // 将错误状态码也反映在HTTP响应中
      const httpStatus = res.code >= 100 && res.code < 600 ? res.code : 500;

      const safeHeaders = new Headers();
      safeHeaders.set("content-type", "application/json;charset=UTF-8");
      safeHeaders.set("Access-Control-Allow-Origin", origin);
      safeHeaders.append("Vary", "Origin");

      const errorResp = new Response(JSON.stringify(res), {
        status: httpStatus,
        headers: safeHeaders
      });
      return errorResp;
    }

    // API call successful, save to cache (if enabled)
    if (cacheManager && res.data) {
      ctx.waitUntil(
        cacheManager
          .saveCache(path, res.data, { ...config.cacheConfig, ctx })
          .catch((error) => {
            // Cache save failure should not block downloads
            console.error('[Cache] Save failed:', error instanceof Error ? error.message : String(error));
          })
      );
    }
  }

  // Use linkData from cache or API response
  const downloadUrl = res.data.url;

  // Throttle protection logic
  let throttleStatus = null;
  let throttleHostname = null;
  let operationMode = 'normal_operation'; // 'normal_operation' or 'resume_operation'

  const shouldCheckThrottle = config.throttleEnabled && throttleManager && (
    !unifiedResult ||
    (unifiedResult && (!unifiedResult.cache.hit || !unifiedResult.cache.hostnameHash))
  );

  if (shouldCheckThrottle) {
    try {
      // Extract hostname from download URL
      throttleHostname = extractHostname(downloadUrl);

      if (throttleHostname) {
        // Check if hostname matches any protection pattern
        let hostnameMatched = false;
        for (const pattern of config.throttleHostnamePatterns) {
          if (matchHostnamePattern(throttleHostname, pattern)) {
            hostnameMatched = true;
            break;
          }
        }

        if (hostnameMatched) {
          // Check throttle protection status
          throttleStatus = await throttleManager.checkThrottle(throttleHostname, { ...config.throttleConfig, ctx });

          if (throttleStatus) {
            if (throttleStatus.status === 'protected') {
              // Still in protection window - return error without fetching
              console.log(`[Throttle] Protected: ${throttleHostname}, returning error ${throttleStatus.errorCode}, retry after ${throttleStatus.retryAfter}s`);

              const safeHeaders = new Headers();
              safeHeaders.set("content-type", "application/json;charset=UTF-8");
              safeHeaders.set("Access-Control-Allow-Origin", origin);
              safeHeaders.append("Vary", "Origin");
              safeHeaders.set("X-Throttle-Protected", "true");
              safeHeaders.set("X-Throttle-Retry-After", String(throttleStatus.retryAfter));

              return new Response(
                JSON.stringify({
                  code: throttleStatus.errorCode,
                  message: `Service temporarily unavailable (throttle protected, retry after ${throttleStatus.retryAfter}s)`
                }),
                {
                  status: throttleStatus.errorCode,
                  headers: safeHeaders
                }
              );
            } else if (throttleStatus.status === 'resume_operation') {
              // Time window expired - resume operation (mark for potential state clear)
              operationMode = 'resume_operation';
              console.log(`[Throttle] Resume operation: ${throttleHostname}`);
            }
            // else: normal_operation (no protection)
          }
        }
      }
    } catch (error) {
      // Throttle check failure should not block downloads
      console.error('[Throttle] Check failed, proceeding with download:', error instanceof Error ? error.message : String(error));
    }
  } else if (
    unifiedResult &&
    unifiedResult.cache &&
    unifiedResult.cache.hit &&
    unifiedResult.cache.hostnameHash
  ) {
    throttleHostname = extractHostname(downloadUrl);
    throttleStatus = unifiedResult.throttle;
    if (unifiedResult.throttle.status === 'resume_operation') {
      operationMode = 'resume_operation';
    }
  }

  // Proceed with fetch
  request = new Request(downloadUrl, request);
  if (res.data.header) {
    for (const k in res.data.header) {
      for (const v of res.data.header[k]) {
        request.headers.set(k, v);
      }
    }
  }

  let response = await fetch(request);
  while (response.status >= 300 && response.status < 400) {
    const location = response.headers.get("Location");
    if (location) {
      if (location.startsWith(`${config.workerAddress}/`)) {
        request = new Request(location, request);
        return await handleRequest(request, config, cacheManager, throttleManager, rateLimiter, ctx);
      } else {
        request = new Request(location, request);
        response = await fetch(request);
      }
    } else {
      break;
    }
  }
  
  // Update throttle protection status based on fetch result
  if (config.throttleEnabled && throttleManager && throttleHostname) {
    try {
      const statusCode = response.status;
      const protectedHttpCodes = Array.isArray(config.throttleConfig?.protectedHttpCodes)
        ? config.throttleConfig.protectedHttpCodes
        : [];

      if (protectedHttpCodes.includes(statusCode)) {
        // 4xx or 5xx error - set protection
        console.log(`[Throttle] Error ${statusCode} from ${throttleHostname}, setting protection`);

        const now = Math.floor(Date.now() / 1000);
        // Don't await - async update (non-blocking)
        const updatePromise = throttleManager.updateThrottle(
          throttleHostname,
          {
            errorTimestamp: now,
            isProtected: 1,
            errorCode: statusCode,
          },
          { ...config.throttleConfig, ctx }
        );

        if (ctx && ctx.waitUntil) {
          ctx.waitUntil(updatePromise);
        }
      } else if (statusCode >= 200 && statusCode < 400) {
        // Success - check operation mode and record existence
        // BREAKING CHANGE: IS_PROTECTED semantics
        //   1 = protected (error detected)
        //   0 = normal operation (initialized or recovered)
        //   NULL = record does not exist
        if (operationMode === 'resume_operation') {
          // Clear protection status (was protected, now recovered)
          console.log(`[Throttle] Success from ${throttleHostname}, clearing protection (resume operation)`);

          // Don't await - async update (non-blocking)
          const updatePromise = throttleManager.updateThrottle(
            throttleHostname,
            {
              errorTimestamp: null,
              isProtected: 0,  // Changed from null to 0
              errorCode: null,
            },
            { ...config.throttleConfig, ctx }
          );

          if (ctx && ctx.waitUntil) {
            ctx.waitUntil(updatePromise);
          }
        } else if (operationMode === 'normal_operation' && throttleStatus && !throttleStatus.recordExists) {
          // First time accessing this hostname - create record with IS_PROTECTED = 0 (normal operation)
          console.log(`[Throttle] Success from ${throttleHostname}, creating initial record (first time)`);

          // Don't await - async update (non-blocking)
          const updatePromise = throttleManager.updateThrottle(
            throttleHostname,
            {
              errorTimestamp: null,
              isProtected: 0,  // Changed from null to 0
              errorCode: null,
            },
            { ...config.throttleConfig, ctx }
          );

          if (ctx && ctx.waitUntil) {
            ctx.waitUntil(updatePromise);
          }
        }
        // else: normal_operation with existing record (recordExists=true) - skip write
      }
    } catch (error) {
      // Throttle update failure should not block downloads
      console.error('[Throttle] Update failed:', error instanceof Error ? error.message : String(error));
    }
  }

  let responseBody = response.body;
  let bandwidthFinalizePromise = null;

  const shouldAccountBandwidth =
    config.bandwidthQuotaEnabled &&
    bandwidthManager &&
    bandwidthConfig &&
    responseBody &&
    request.method === 'GET' &&
    (response.status === 200 || response.status === 206);

  if (shouldAccountBandwidth) {
    try {
      const subnet = calculateIPSubnet(
        clientIP,
        bandwidthConfig.ipv4Suffix || config.ipv4Suffix || '/32',
        bandwidthConfig.ipv6Suffix || config.ipv6Suffix || '/60'
      );

      if (subnet) {
        const { stream, finalizePromise } = wrapReadableWithAccounting(response.body, async ({ bytes }) => {
          if (!Number.isFinite(bytes) || bytes <= 0) {
            return;
          }

          if (!bandwidthManager || typeof bandwidthManager.upsertBandwidthQuota !== 'function') {
            console.warn('[Bandwidth Quota] No quota manager available for accounting');
            return;
          }

          try {
            await bandwidthManager.upsertBandwidthQuota(bandwidthConfig, subnet, path, bytes, filesize);
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            console.error('[Bandwidth Quota] Failed to record usage:', message);
          }
        });

        responseBody = stream;
        bandwidthFinalizePromise = finalizePromise.catch((error) => {
          const message = error instanceof Error ? error.message : String(error);
          console.error('[Bandwidth Accounting] Finalize error:', message);
        });
      } else {
        console.warn('[Bandwidth Quota] Unable to determine subnet, skipping accounting');
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error('[Bandwidth Quota] Accounting setup failed:', message);
    }
  }

  if (bandwidthFinalizePromise && ctx && ctx.waitUntil) {
    ctx.waitUntil(bandwidthFinalizePromise);
  }

  // 创建仅包含安全必要headers的响应
  const safeHeaders = new Headers();

  // 保留重要的内容相关headers
  const preserveHeaders = [
  'content-type',
  'content-disposition',
  'content-length',
  'cache-control',
  'content-encoding',
  'accept-ranges',
  'content-range',    // Added for partial downloads
  'transfer-encoding', // Added for chunked transfers
  'content-language',  // Added for internationalization
  'expires',           // Added for cache control
  'pragma',            // Added for cache control
  'etag',
  'last-modified'
  ];

  // 仅复制必要的headers
  preserveHeaders.forEach(header => {
    const value = response.headers.get(header);
    if (value) {
      safeHeaders.set(header, value);
    }
  });

  // 设置CORS headers
  safeHeaders.set("Access-Control-Allow-Origin", origin);
  safeHeaders.append("Vary", "Origin");

  // 创建带有安全headers的新响应
  const safeResponse = new Response(responseBody, {
    status: response.status,
    statusText: response.statusText,
    headers: safeHeaders
  });

  return safeResponse;
}
// src/handleOptions.ts
function handleOptions(request) {
  const corsHeaders = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,HEAD,POST,OPTIONS",
    "Access-Control-Max-Age": "86400"
  };
  let headers = request.headers;
  if (headers.get("Origin") !== null && headers.get("Access-Control-Request-Method") !== null) {
    // 使用安全的响应头
    const safeHeaders = new Headers();
    safeHeaders.set("Access-Control-Allow-Origin", headers.get("Origin") || "*");
    safeHeaders.set("Access-Control-Allow-Methods", "GET,HEAD,POST,OPTIONS");
    safeHeaders.set("Access-Control-Max-Age", "86400");
    safeHeaders.set("Access-Control-Allow-Headers", request.headers.get("Access-Control-Request-Headers") || "");
    
    return new Response(null, {
      headers: safeHeaders
    });
  } else {
    const safeHeaders = new Headers();
    safeHeaders.set("Allow", "GET, HEAD, POST, OPTIONS");
    
    return new Response(null, {
      headers: safeHeaders
    });
  }
}

// src/handleRequest.ts - Modified to check IPv6 addresses
/**
 * Check Cloudflare Rate Limiter
 * @param {Object} env - Worker环境对象
 * @param {string} clientIP - 客户端IP
 * @param {string} ipv4Suffix - IPv4子网掩码
 * @param {string} ipv6Suffix - IPv6子网前缀
 * @param {string} bindingName - Rate Limiter绑定名称
 * @returns {Promise<{allowed: boolean, ipSubnet: string}>}
 */
async function checkCfRatelimit(env, clientIP, ipv4Suffix, ipv6Suffix, bindingName) {
  const ipSubnet = calculateIPSubnet(clientIP, ipv4Suffix, ipv6Suffix);

  if (!ipSubnet) {
    return { allowed: true, ipSubnet };
  }

  const ipHash = await sha256Hash(ipSubnet);
  const ratelimiter = env[bindingName];
  const { success } = await ratelimiter.limit({ key: ipHash });

  return { allowed: success, ipSubnet };
}

// src/handleRequest.ts - Modified to check IPv6 addresses
async function handleRequest(request, env, config, cacheManager, throttleManager, rateLimiter, ctx) {
  // Check for IPv6 access if IPv4_ONLY is enabled
  if (config.ipv4Only) {
    const clientIP = request.headers.get("CF-Connecting-IP") || "";
    if (isIPv6(clientIP)) {
      const safeHeaders = new Headers();
      safeHeaders.set("content-type", "application/json;charset=UTF-8");
      safeHeaders.set("Access-Control-Allow-Origin", request.headers.get("origin") ?? "*");
      safeHeaders.append("Vary", "Origin");

      return new Response(
        JSON.stringify({
          code: 403,
          message: "ipv6 access is prohibited"
        }),
        {
          status: 403,
          headers: safeHeaders
        }
      );
    }
  }

  // Continue with normal processing if not blocked
  if (request.method === "OPTIONS") {
    return handleOptions(request);
  }
  return await handleDownload(request, env, config, cacheManager, throttleManager, rateLimiter, ctx);
}
// src/index.ts
export default {
  async fetch(request, env, ctx) {
    try {
      const config = resolveConfig(env || {});
      // Create cache manager instance based on DB_MODE
      const cacheManager = config.cacheEnabled ? createCacheManager(config.dbMode) : null;
      // Create throttle manager instance based on DB_MODE (if throttle enabled)
      const throttleManager = config.throttleEnabled ? createThrottleManager(config.dbMode) : null;
      const rateLimiter = config.rateLimitEnabled ? createRateLimiter(config.dbMode) : null;
      const response = await handleRequest(request, env, config, cacheManager, throttleManager, rateLimiter, ctx);

      scheduleAllCleanups(config, env, ctx).catch((error) => {
        const message = error instanceof Error ? error.message : String(error);
        console.error('[Cleanup Scheduler] Error:', message);
      });

      return response;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return createErrorResponse("*", 500, message);
    }
  }
};
