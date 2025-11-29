// Import cache/throttle managers, rate limiter, and utilities
import { createCacheManager } from './cache/factory.js';
import { createThrottleManager } from './cache/throttle-factory.js';
import { createRateLimiter } from './ratelimit/factory.js';
import { unifiedCheck } from './unified-check.js';
import { scheduleAllCleanups } from './cleanup-scheduler.js';
import { parseBoolean, extractHostname, matchHostnamePattern, applyVerifyHeaders, calculateIPSubnet, sha256Hash } from './utils.js';
import { checkOriginMatch, decryptOriginSnapshot, getClientIp, parseCheckOriginEnv } from './origin-binding.js';
import { handleInternalApiIfAny } from './internal-api.js';
import { fetchControllerState } from './controller-adapter.js';
import { BootstrapDO } from './do/bootstrap-do.js';
import { MetricsDO } from './do/metrics-do.js';

// Configuration constants
const REQUIRED_ENV = [];
const VALID_ACTIONS = new Set(['block', 'skip-sign', 'skip-hash', 'skip-worker', 'skip-addition', 'skip-addition-expiretime', 'skip-origin', 'asis']);
const DEFAULT_LINK_TTL_SECONDS = 1800;
const DEFAULT_CLEANUP_PERCENTAGE = 1;
const DEFAULT_RATE_LIMIT_BLOCK_SECONDS = 600;
const DEFAULT_RATE_LIMIT_IPV4_SUFFIX = '/32';
const DEFAULT_RATE_LIMIT_IPV6_SUFFIX = '/60';
const DEFAULT_FQ_QUEUE_WAIT_MS = 15000;
const DEFAULT_SLOT_HANDLER_TIMEOUT_MS = 20000;
const DEFAULT_SLOT_HANDLER_PER_REQUEST_TIMEOUT_MS = 8000;
const DEFAULT_SLOT_HANDLER_MAX_ATTEMPTS = 35;

const DOWNLOAD_EXPOSE_HEADERS = 'Content-Length, Content-Range, X-Throttle-Status, X-Throttle-Retry-After, Accept-Ranges';
const DOWNLOAD_ALLOW_HEADERS = 'Range, Content-Type, X-Requested-With';

const applyDownloadCorsHeaders = (headers) => {
  if (!headers || typeof headers.set !== 'function') {
    return;
  }
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Expose-Headers', DOWNLOAD_EXPOSE_HEADERS);
};

const handleOptions = () => {
  const headers = new Headers();
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  headers.set('Access-Control-Allow-Headers', DOWNLOAD_ALLOW_HEADERS);
  headers.set('Access-Control-Max-Age', '86400');
  return new Response(null, { status: 204, headers });
};

const normalizePgErrorHandleConfig = (value) => {
  const lowered = typeof value === 'string' ? value.trim().toLowerCase() : '';
  return lowered === 'fail-open' ? 'fail-open' : 'fail-closed';
};

// Ensure required environment variables are set
const ensureRequiredEnv = (env) => {
  REQUIRED_ENV.forEach((key) => {
    if (!env[key] || String(env[key]).trim() === '') {
      throw new Error(`environment variable ${key} is required`);
    }
  });
};

// Resolve configuration from controller bootstrap/decision
const resolveConfig = (env = {}, bootstrap = null, decision = null) => {
  ensureRequiredEnv(env);

  const normalizeString = (value, defaultValue = '') => {
    if (value === undefined || value === null) return defaultValue;
    if (typeof value !== 'string') return defaultValue;
    const trimmed = value.trim();
    return trimmed === '' ? defaultValue : trimmed;
  };

  const commonBootstrap = bootstrap && typeof bootstrap === 'object'
    ? bootstrap.common || null
    : null;
  if (!commonBootstrap) {
    throw new Error('controller bootstrap.common is required');
  }

  const downloadBootstrap = bootstrap && typeof bootstrap === 'object'
    ? bootstrap.download || null
    : null;
  if (!downloadBootstrap) {
    throw new Error('controller bootstrap.download is required');
  }

  const downloadDecision = decision && typeof decision === 'object'
    ? decision.download || null
    : null;

  const token = normalizeString(commonBootstrap.tokenHmacKey);
  if (!token) {
    throw new Error('controller common.tokenHmacKey is required');
  }
  const signSecretFromController = normalizeString(commonBootstrap.signSecret) || token;

  const address = normalizeString(downloadBootstrap.address);
  if (!address) {
    throw new Error('controller download.address is required');
  }

  const authConfig = downloadBootstrap.auth && typeof downloadBootstrap.auth === 'object'
    ? downloadBootstrap.auth
    : {};
  const authSignSecret = normalizeString(authConfig.signSecret);
  const signSecret = authSignSecret || signSecretFromController;
  const signCheck = authConfig.signCheck !== false;
  const hashCheck = authConfig.hashCheck !== false;
  const workerCheck = authConfig.workerCheck !== false;
  const additionCheck = authConfig.additionCheck !== false;
  const additionExpireTimeCheck = authConfig.additionExpireTimeCheck !== false;
  const ipv4Only = authConfig.ipv4Only !== false;

  // DB & cache from controller
  const dbConfig = downloadBootstrap.db && typeof downloadBootstrap.db === 'object'
    ? downloadBootstrap.db
    : {};
  const dbModeRaw = normalizeString(dbConfig.mode);
  const dbMode = dbModeRaw ? dbModeRaw.toLowerCase() : '';
  if (dbMode && dbMode !== 'custom-pg-rest') {
    throw new Error(`Invalid controller download.db.mode: "${dbModeRaw}". Only "" or "custom-pg-rest" are supported.`);
  }
  const isCustomDb = dbMode === 'custom-pg-rest';

  const postgrestUrl = normalizeString(dbConfig.postgrestUrl);
  const verifyHeader = Array.isArray(dbConfig.verifyHeader)
    ? dbConfig.verifyHeader.map((v) => normalizeString(v)).filter((v) => v.length > 0)
    : [];
  const verifySecret = Array.isArray(dbConfig.verifySecret)
    ? dbConfig.verifySecret.map((v) => normalizeString(v)).filter((v) => v.length > 0)
    : [];

  if (verifyHeader.length > 0 && verifySecret.length > 0 && verifyHeader.length !== verifySecret.length) {
    throw new Error('controller download.db.verifyHeader and verifySecret must have the same length');
  }
  if (isCustomDb && (!postgrestUrl || verifyHeader.length === 0 || verifySecret.length === 0)) {
    throw new Error('controller download.db requires postgrestUrl, verifyHeader, and verifySecret when mode=custom-pg-rest');
  }

  const cleanupPercentRaw = Number.parseFloat(dbConfig.cleanupPercentage);
  const cleanupProbability = Number.isFinite(cleanupPercentRaw) && cleanupPercentRaw >= 0
    ? Math.min(100, cleanupPercentRaw) / 100
    : DEFAULT_CLEANUP_PERCENTAGE / 100;

  const linkTTLSecondsRaw = Number(dbConfig.linkTTLSeconds);
  const linkTTLSeconds = Number.isFinite(linkTTLSecondsRaw) && linkTTLSecondsRaw > 0
    ? linkTTLSecondsRaw
    : DEFAULT_LINK_TTL_SECONDS;

  const idleTimeoutSecondsRaw = Number(dbConfig.idleTimeoutSeconds);
  const idleTimeoutSeconds = Number.isFinite(idleTimeoutSecondsRaw) && idleTimeoutSecondsRaw >= 0
    ? idleTimeoutSecondsRaw
    : 0;

  const cacheTableName = normalizeString(dbConfig.cacheTable, 'DOWNLOAD_CACHE_TABLE');
  const lastActiveTableName = normalizeString(dbConfig.lastActiveTable, 'DOWNLOAD_LAST_ACTIVE_TABLE');

  let cacheEnabled = false;
  let cacheConfig = {};

  if (isCustomDb) {
    cacheEnabled = true;
    cacheConfig = {
      postgrestUrl,
      verifyHeader,
      verifySecret,
      tableName: cacheTableName,
      linkTTL: linkTTLSeconds,
      idleTimeout: idleTimeoutSeconds,
      lastActiveTableName,
      cleanupProbability,
    };
  }

  // rate limit from controller
  const rateLimit = dbConfig.rateLimit && typeof dbConfig.rateLimit === 'object'
    ? dbConfig.rateLimit
    : {};
  const rateLimitWindowSecondsRaw = Number(rateLimit.windowSeconds);
  const rateLimitWindowSeconds = Number.isFinite(rateLimitWindowSecondsRaw) && rateLimitWindowSecondsRaw > 0
    ? rateLimitWindowSecondsRaw
    : 0;
  const rateLimitLimitRaw = Number(rateLimit.limit);
  const ipSubnetLimit = Number.isFinite(rateLimitLimitRaw) && rateLimitLimitRaw > 0 ? rateLimitLimitRaw : 0;
  const rateLimitCleanupPercentRaw = Number.parseFloat(rateLimit.cleanupPercentage);
  const rateLimitCleanupProbability = Number.isFinite(rateLimitCleanupPercentRaw) && rateLimitCleanupPercentRaw >= 0
    ? Math.min(100, rateLimitCleanupPercentRaw) / 100
    : cleanupProbability;
  const rateLimitEnabled = Boolean(isCustomDb && rateLimit.enabled && rateLimitWindowSeconds > 0 && ipSubnetLimit > 0);
  const rateLimitConfig = {
    postgrestUrl,
    verifyHeader,
    verifySecret,
    tableName: normalizeString(rateLimit.tableName, 'DOWNLOAD_IP_RATELIMIT_TABLE'),
    windowTimeSeconds: rateLimitWindowSeconds > 0 ? rateLimitWindowSeconds : 86400,
    limit: ipSubnetLimit,
    ipv4Suffix: normalizeString(rateLimit.ipv4Suffix, DEFAULT_RATE_LIMIT_IPV4_SUFFIX) || DEFAULT_RATE_LIMIT_IPV4_SUFFIX,
    ipv6Suffix: normalizeString(rateLimit.ipv6Suffix, DEFAULT_RATE_LIMIT_IPV6_SUFFIX) || DEFAULT_RATE_LIMIT_IPV6_SUFFIX,
    pgErrorHandle: normalizePgErrorHandleConfig(rateLimit.pgErrorHandle || 'fail-closed'),
    cleanupProbability: rateLimitCleanupProbability,
    blockTimeSeconds: Number(rateLimit.blockSeconds) > 0 ? Number(rateLimit.blockSeconds) : DEFAULT_RATE_LIMIT_BLOCK_SECONDS,
  };
  const windowTime = rateLimitWindowSeconds > 0 ? `${rateLimitWindowSeconds}s` : '';

  // throttle profile from controller + decision
  const throttleProfiles = downloadBootstrap.throttleProfiles && typeof downloadBootstrap.throttleProfiles === 'object'
    ? downloadBootstrap.throttleProfiles
    : {};
  const throttleProfileName = typeof downloadDecision?.throttleProfile === 'string' && downloadDecision.throttleProfile.trim() !== ''
    ? downloadDecision.throttleProfile.trim()
    : 'default';
  const throttleProfile = throttleProfiles[throttleProfileName] || throttleProfiles.default || {};
  const throttleHostnamePatterns = Array.isArray(throttleProfile.hostPatterns)
    ? throttleProfile.hostPatterns.map((p) => normalizeString(p)).filter((p) => p.length > 0)
    : [];
  const throttleCleanupPercentRaw = Number.parseFloat(throttleProfile.cleanupPercentage);
  const throttleCleanupProbability = Number.isFinite(throttleCleanupPercentRaw) && throttleCleanupPercentRaw >= 0
    ? Math.min(100, throttleCleanupPercentRaw) / 100
    : cleanupProbability;
  const throttleEnabled = isCustomDb && throttleHostnamePatterns.length > 0;
  const throttleConfig = {
    postgrestUrl,
    verifyHeader,
    verifySecret,
    tableName: normalizeString(throttleProfile.tableName, 'download_throttle'),
    throttleTimeWindow: Number(throttleProfile.windowSeconds) > 0 ? Number(throttleProfile.windowSeconds) : 60,
    observeWindowSeconds: Number(throttleProfile.observeWindowSeconds) > 0 ? Number(throttleProfile.observeWindowSeconds) : 60,
    errorRatioPercent: Number(throttleProfile.errorRatioPercent) > 0 ? Number(throttleProfile.errorRatioPercent) : 20,
    consecutiveThreshold: Number.isFinite(Number(throttleProfile.consecutiveThreshold)) && Number(throttleProfile.consecutiveThreshold) > 0
      ? Number(throttleProfile.consecutiveThreshold)
      : 4,
    minSampleCount: Number.isFinite(Number(throttleProfile.minSampleCount)) && Number(throttleProfile.minSampleCount) > 0
      ? Number(throttleProfile.minSampleCount)
      : 8,
    fastErrorRatioPercent: Number(throttleProfile.fastErrorRatioPercent) > 0
      ? Number(throttleProfile.fastErrorRatioPercent)
      : undefined,
    fastMinSampleCount: Number.isFinite(Number(throttleProfile.fastMinSampleCount)) && Number(throttleProfile.fastMinSampleCount) >= 0
      ? Number(throttleProfile.fastMinSampleCount)
      : 4,
    cleanupProbability: throttleCleanupProbability,
    protectedHttpCodes: Array.isArray(throttleProfile.protectHttpCodes)
      ? throttleProfile.protectHttpCodes
          .map((code) => Number(code))
          .filter((code) => Number.isInteger(code) && code >= 100 && code <= 599)
      : [],
  };
  throttleConfig.fastErrorRatioPercent = throttleConfig.fastErrorRatioPercent
    || throttleConfig.errorRatioPercent;

  // fair queue from controller + decision
  const fairQueueConfigRaw = downloadBootstrap.fairQueue && typeof downloadBootstrap.fairQueue === 'object'
    ? downloadBootstrap.fairQueue
    : {};
  const fairQueueProfiles = fairQueueConfigRaw.profiles && typeof fairQueueConfigRaw.profiles === 'object'
    ? fairQueueConfigRaw.profiles
    : {};
  const fairQueueProfileName = typeof downloadDecision?.fairQueueProfile === 'string' && downloadDecision.fairQueueProfile.trim() !== ''
    ? downloadDecision.fairQueueProfile.trim()
    : 'default';
  const fairQueueProfile = fairQueueProfiles[fairQueueProfileName] || fairQueueProfiles.default || {};
  const fairQueueHostnamePatterns = Array.isArray(fairQueueConfigRaw.hostPatterns)
    ? fairQueueConfigRaw.hostPatterns.map((p) => normalizeString(p)).filter((p) => p.length > 0)
    : [];
  const fairQueueEnabled = Boolean(fairQueueConfigRaw.enabled) && fairQueueHostnamePatterns.length > 0;
  const queueWaitTimeoutMsRaw = Number(fairQueueConfigRaw.queueWaitTimeoutMs);
  const queueWaitTimeoutMs = Number.isFinite(queueWaitTimeoutMsRaw) && queueWaitTimeoutMsRaw > 0
    ? queueWaitTimeoutMsRaw
    : DEFAULT_FQ_QUEUE_WAIT_MS;
  const slotHandlerTimeoutMsRaw = Number(fairQueueConfigRaw.slotHandlerTimeoutMs);
  const slotHandlerTimeoutMs = Number.isFinite(slotHandlerTimeoutMsRaw) && slotHandlerTimeoutMsRaw > 0
    ? slotHandlerTimeoutMsRaw
    : DEFAULT_SLOT_HANDLER_TIMEOUT_MS;
  const perRequestTimeoutMsRaw = Number(fairQueueConfigRaw.perRequestTimeoutMs);
  const perRequestTimeoutMs = Number.isFinite(perRequestTimeoutMsRaw) && perRequestTimeoutMsRaw > 0
    ? perRequestTimeoutMsRaw
    : DEFAULT_SLOT_HANDLER_PER_REQUEST_TIMEOUT_MS;
  const maxAttemptsCapRaw = Number(fairQueueConfigRaw.maxAttemptsCap);
  const maxAttemptsCap = Number.isFinite(maxAttemptsCapRaw) && maxAttemptsCapRaw > 0
    ? maxAttemptsCapRaw
    : DEFAULT_SLOT_HANDLER_MAX_ATTEMPTS;
  const slotHandlerUrl = normalizeString(fairQueueConfigRaw.slotHandlerUrl);
  const slotHandlerAuthKey = normalizeString(fairQueueConfigRaw.slotHandlerAuthKey);
  if (fairQueueEnabled && !slotHandlerUrl) {
    throw new Error('controller fairQueue.slotHandlerUrl is required when fairQueue.enabled is true');
  }
  const fairQueueProfileNormalized = {
    maxWaitMs: Number(fairQueueProfile.maxWaitMs) > 0 ? Number(fairQueueProfile.maxWaitMs) : queueWaitTimeoutMs,
    maxSlotPerHost: Number(fairQueueProfile.maxSlotPerHost) > 0 ? Number(fairQueueProfile.maxSlotPerHost) : 8,
    maxSlotPerIp: Number(fairQueueProfile.maxSlotPerIp) > 0 ? Number(fairQueueProfile.maxSlotPerIp) : 3,
    maxWaitersPerIp: Number(fairQueueProfile.maxWaitersPerIp) > 0 ? Number(fairQueueProfile.maxWaitersPerIp) : 8,
  };
  const fairQueueConfig = {
    queueWaitTimeoutMs,
    profile: fairQueueProfileName,
    slotHandlerTimeoutMs,
    profileConfig: fairQueueProfileNormalized,
  };
  const slotHandlerConfig = {
    url: slotHandlerUrl,
    totalMaxWaitMs: slotHandlerTimeoutMs,
    perRequestTimeoutMs,
    maxAttemptsCap,
    authKey: slotHandlerAuthKey,
  };
  const fairQueueContext = {
    fairQueueEnabled,
    fairQueueHostnamePatterns,
    fairQueueConfig,
  };

  const enableCfRatelimiter = normalizeString(env.ENABLE_CF_RATELIMITER, 'false').toLowerCase() === 'true';
  const cfRatelimiterBinding = normalizeString(env.CF_RATELIMITER_BINDING, 'CF_RATE_LIMITER');

  if (enableCfRatelimiter) {
    const ratelimiter = env[cfRatelimiterBinding];
    if (!ratelimiter || typeof ratelimiter.limit !== 'function') {
      throw new Error(
        `ENABLE_CF_RATELIMITER is true but binding "${cfRatelimiterBinding}" not found or invalid. Please configure [[rate_limit]] binding in wrangler.toml with name="${cfRatelimiterBinding}".`
      );
    }
  }

  return {
    address,
    token,
    verifyHeader,
    verifySecret,
    signSecret,
    signCheck,
    hashCheck,
    workerCheck,
    additionCheck,
    additionExpireTimeCheck,
    ipv4Only,
    dbMode,
    cacheEnabled,
    cacheConfig,
    throttleEnabled,
    throttleHostnamePatterns,
    throttleConfig,
    rateLimitEnabled,
    rateLimitConfig,
    slotHandlerConfig,
    windowTime,
    ipSubnetLimit,
    enableCfRatelimiter,
    cfRatelimiterBinding,
    ipv4Suffix: rateLimitConfig.ipv4Suffix,
    ipv6Suffix: rateLimitConfig.ipv6Suffix,
    pgErrorHandle: rateLimitConfig.pgErrorHandle,
    idleTimeout: idleTimeoutSeconds,
    lastActiveTableName,
    fairQueueEnabled: fairQueueContext.fairQueueEnabled,
    fairQueueHostnamePatterns: fairQueueContext.fairQueueHostnamePatterns,
    fairQueueConfig: fairQueueContext.fairQueueConfig,
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

const sanitizeDispositionFileName = (value) => {
  if (!value) return 'download.bin';
  return value.replace(/["\\\r\n]/g, '_');
};

const encodeRFC5987Value = (value) =>
  encodeURIComponent(value)
    .replace(/['()*]/g, (character) => `%${character.charCodeAt(0).toString(16).toUpperCase()}`)
    .replace(/%(7C|60|5E)/g, (match) => match.toUpperCase());

const buildAttachmentContentDisposition = (fileName) => {
  const normalized = fileName && fileName.length > 0 ? fileName : 'download.bin';
  const safeName = sanitizeDispositionFileName(normalized);
  const encoded = encodeRFC5987Value(normalized);
  return `attachment; filename="${safeName}"; filename*=UTF-8''${encoded}`;
};

const deriveFileNameFromPath = (inputPath) => {
  if (typeof inputPath !== 'string' || inputPath.length === 0) {
    return '';
  }
  let decoded = '';
  try {
    decoded = decodeURIComponent(inputPath);
  } catch (_error) {
    decoded = inputPath;
  }
  const segments = decoded.split('/').filter((segment) => segment.length > 0);
  return segments.length > 0 ? segments[segments.length - 1] : '';
};

const ensureEncryptedFileName = (fileName) => {
  const normalized = fileName && fileName.length > 0 ? fileName : 'download.bin';
  return normalized.toLowerCase().endsWith('.enc') ? normalized : `${normalized}.enc`;
};

async function sha256Hex(text) {
  const data = new TextEncoder().encode(text);
  const hashBuffer = await crypto.subtle.digest("SHA-256", data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map((b) => b.toString(16).padStart(2, "0")).join("");
}

// Normalize controller decision.pathAction into VALID_ACTIONS; controller is source of truth.
const normalizeControllerPathActions = (downloadDecision) => {
  if (!downloadDecision || !Array.isArray(downloadDecision.pathAction)) {
    return [];
  }

  const normalized = [];
  const seen = new Set();

  for (const action of downloadDecision.pathAction) {
    const token = typeof action === 'string' ? action.trim().toLowerCase() : '';
    if (!token) {
      continue;
    }
    if (!VALID_ACTIONS.has(token)) {
      console.warn(`[controller] unsupported pathAction '${action}' ignored`);
      continue;
    }
    if (!seen.has(token)) {
      normalized.push(token);
      seen.add(token);
    }
  }

  return normalized;
};

// Extract origin check modes from controller decision; controller is source of truth.
const extractControllerOriginModes = (downloadDecision) => {
  if (!downloadDecision || typeof downloadDecision.checkOriginMode === 'undefined') {
    return [];
  }
  if (typeof downloadDecision.checkOriginMode !== 'string') {
    console.warn('[controller] checkOriginMode is not a string, ignore');
    return [];
  }
  return parseCheckOriginEnv(downloadDecision.checkOriginMode);
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

const verifySignature = async (secret, data, signature) => {
  if (!signature) return 'sign missing';
  const parts = signature.split(':');
  const expirePart = parts[parts.length - 1];
  if (!expirePart) return 'expire missing';
  const expire = Number.parseInt(expirePart, 10);
  if (Number.isNaN(expire)) return 'expire invalid';
  if (expire < Date.now() / 1e3 && expire > 0) return 'expire expired';
  const expected = await hmacSha256Sign(data, expire, secret);
  if (expected !== signature) return 'sign mismatch';
  return '';
};

function createErrorResponse(origin, status, message, extraHeaders) {
  const safeHeaders = new Headers();
  safeHeaders.set("content-type", "application/json;charset=UTF-8");
  safeHeaders.set("Access-Control-Allow-Origin", origin);
  safeHeaders.append("Vary", "Origin");
  if (extraHeaders && typeof extraHeaders === 'object') {
    for (const [headerName, headerValue] of Object.entries(extraHeaders)) {
      if (headerValue === undefined || headerValue === null) {
        continue;
      }
      safeHeaders.set(headerName, String(headerValue));
    }
  }

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

function createClientAbortResponse(origin) {
  return createErrorResponse(origin, 499, "client aborted request");
}

function isAbortError(error) {
  if (!error) {
    return false;
  }
  if (error.name === 'AbortError') {
    return true;
  }
  const message = error instanceof Error ? error.message : String(error);
  return typeof message === 'string' && message.toLowerCase().includes('aborted');
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

function createThrottleProtectedResponse(origin, throttleStatus) {
  const retryAfter =
    throttleStatus && Number.isFinite(throttleStatus.retryAfter) && throttleStatus.retryAfter > 0
      ? Math.max(1, Math.ceil(throttleStatus.retryAfter))
      : 0;
  const statusCode =
    throttleStatus && Number.isFinite(throttleStatus.errorCode) && throttleStatus.errorCode >= 100
      ? throttleStatus.errorCode
      : 503;
  const message =
    (throttleStatus && throttleStatus.message) ||
    (retryAfter
      ? `Service temporarily unavailable (throttle protected, retry after ${retryAfter}s)`
      : 'Service temporarily unavailable (throttle protected)');

  const safeHeaders = new Headers();
  safeHeaders.set("content-type", "application/json;charset=UTF-8");
  safeHeaders.set("Access-Control-Allow-Origin", origin);
  safeHeaders.append("Vary", "Origin");
  safeHeaders.set("X-Throttle-Protected", "true");
  if (retryAfter) {
    const retryAfterValue = String(retryAfter);
    safeHeaders.set("Retry-After", retryAfterValue);
    safeHeaders.set("X-Throttle-Retry-After", retryAfterValue);
  }

  return new Response(
    JSON.stringify({
      code: statusCode,
      message
    }),
    {
      status: statusCode,
      headers: safeHeaders
    }
  );
}

const normalizePostgrestBaseUrl = (url) => {
  if (!url || typeof url !== 'string') {
    return '';
  }
  return url.endsWith('/') ? url.slice(0, -1) : url;
};

const createFairQueueClient = (config) => createSlotHandlerClient(config);

const createSlotHandlerClient = (config) => {
  const slotCfg = config.slotHandlerConfig || {};
  const baseUrl = normalizePostgrestBaseUrl(slotCfg.url);
  const pgErrorHandle = (config.pgErrorHandle || 'fail-closed').toLowerCase();
  if (!baseUrl) {
    if (pgErrorHandle === 'fail-open') {
      return {
        async waitForSlot() {
          console.warn('[FQ] slot-handler URL missing, granting slot (fail-open)');
          return { kind: 'granted' };
        },
        async releaseSlot() {},
      };
    }
    throw new Error('[FQ] slot-handler backend enabled but FAIR_QUEUE_SLOT_HANDLER_URL is missing');
  }

  const acquireUrl = `${baseUrl}/api/v0/fairqueue/acquire`;
  const releaseUrl = `${baseUrl}/api/v0/fairqueue/release`;
  const cancelUrl = `${baseUrl}/api/v0/fairqueue/cancel`;
  const authKey = slotCfg.authKey || '';
  const throttleTimeWindowSeconds =
    Number(config.throttleConfig?.throttleTimeWindow) > 0
      ? Number(config.throttleConfig.throttleTimeWindow)
      : 60;
  const perRequestTimeoutMsRaw = Number(slotCfg.perRequestTimeoutMs);
  const perRequestTimeoutMs =
    Number.isFinite(perRequestTimeoutMsRaw) && perRequestTimeoutMsRaw > 0 ? perRequestTimeoutMsRaw : 8000;
  const totalMaxWaitMsRaw =
    Number(slotCfg.totalMaxWaitMs) ||
    Number(slotCfg.timeoutMs) || // legacy field
    Number(config.fairQueueConfig?.queueWaitTimeoutMs);
  const totalMaxWaitMs =
    Number.isFinite(totalMaxWaitMsRaw) && totalMaxWaitMsRaw > 0 ? totalMaxWaitMsRaw : 20000;
  const maxAttemptsCapRaw = Number(slotCfg.maxAttemptsCap);
  const maxAttemptsCap =
    Number.isFinite(maxAttemptsCapRaw) && maxAttemptsCapRaw > 0 ? maxAttemptsCapRaw : 35;

  const buildHeaders = () => {
    const headers = { 'Content-Type': 'application/json' };
    if (authKey) {
      headers['X-FQ-Auth'] = authKey;
    }
    return headers;
  };

  const fetchWithTimeout = async (url, payload, timeoutMs) => {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      return await fetch(url, {
        method: 'POST',
        body: JSON.stringify(payload),
        headers: buildHeaders(),
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timer);
    }
  };

  const computeMaxAttempts = () => {
    const attempts = Math.ceil(totalMaxWaitMs / perRequestTimeoutMs);
    const safeAttempts = Number.isFinite(attempts) && attempts > 0 ? attempts : 1;
    return Math.max(1, Math.min(maxAttemptsCap, safeAttempts));
  };

  return {
    async waitForSlot(ctx, fqContext) {
      const maxAttempts = computeMaxAttempts();
      let queryToken = null;

      for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        const payload = queryToken
          ? { queryToken, now: Date.now() }
          : {
              hostname: fqContext.hostname,
              hostnameHash: fqContext.hostnameHash,
              ipBucket: fqContext.ipBucket,
              now: fqContext.nowMs,
              throttleTimeWindowSeconds,
            };

        let res;
        try {
          res = await fetchWithTimeout(acquireUrl, payload, perRequestTimeoutMs);
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          console.error('[FQ] slot-handler acquire error:', message);
          if (attempt === maxAttempts) {
            return pgErrorHandle === 'fail-open'
              ? { kind: 'granted' }
              : { kind: 'timeout', reason: 'slot-handler-unreachable' };
          }
          continue;
        }

        if (!res.ok) {
          const message = `[FQ] slot-handler acquire failed: status ${res.status}`;
          console.error(message);
          if (pgErrorHandle === 'fail-open') {
            return { kind: 'granted' };
          }
          throw new Error(message);
        }

        let data;
        try {
          data = await res.json();
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          console.error('[FQ] slot-handler response parse error:', message);
          if (attempt === maxAttempts) {
            return pgErrorHandle === 'fail-open'
              ? { kind: 'granted' }
              : { kind: 'timeout', reason: 'slot-handler-invalid-response' };
          }
          continue;
        }

        if (data && data.queryToken) {
          queryToken = data.queryToken;
          fqContext.queryToken = data.queryToken;
        }

        switch (data?.result) {
          case 'granted':
            fqContext.slotToken = data.slotToken;
            fqContext.slotAcquiredAt = Date.now();
            console.log(`[FQ] slot granted via slot-handler host=${fqContext.hostname}`);
            return { kind: 'granted' };
          case 'throttled':
            const retryAfter =
              Number.isFinite(data?.throttleRetryAfter) && data.throttleRetryAfter > 0
                ? data.throttleRetryAfter
                : Number.isFinite(data?.throttle_retry_after) && data.throttle_retry_after > 0
                  ? data.throttle_retry_after
                  : Number.isFinite(data?.throttleWait) && data.throttleWait > 0
                    ? data.throttleWait
                    : Number.isFinite(data?.retryAfter) && data.retryAfter > 0
                      ? data.retryAfter
                      : null;
            return {
              kind: 'throttled',
              throttleCode: Number.isFinite(data.throttleCode) ? data.throttleCode : 503,
              retryAfter: retryAfter ?? undefined,
            };
          case 'timeout':
            return { kind: 'timeout' };
          case 'pending':
            continue;
          default: {
            const message = `[FQ] unexpected slot-handler result: ${data?.result}`;
            console.error(message);
            if (pgErrorHandle === 'fail-open') {
              return { kind: 'granted' };
            }
            throw new Error(message);
          }
        }
      }

      return pgErrorHandle === 'fail-open'
        ? { kind: 'granted' }
        : { kind: 'timeout', reason: 'slot-handler-loop-exhausted' };
    },

    async cancelSession(ctx, fqContext) {
      const token = fqContext?.queryToken;
      if (!token) {
        return;
      }

      const payload = { queryToken: token };

      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 5000);
      try {
        await fetch(cancelUrl, {
          method: 'POST',
          headers: buildHeaders(),
          body: JSON.stringify(payload),
          signal: controller.signal,
        });
        console.log(`[FQ] cancel session via slot-handler host=${fqContext.hostname}`);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.warn('[FQ] cancelSession error (slot-handler):', message);
      } finally {
        clearTimeout(timer);
      }
    },

    async releaseSlot(ctx, fqContext) {
      if (!fqContext.slotToken) {
        return;
      }

      const payload = {
        hostname: fqContext.hostname,
        hostnameHash: fqContext.hostnameHash,
        ipBucket: fqContext.ipBucket,
        slotToken: fqContext.slotToken,
        hitUpstreamAtMs: fqContext.hitUpstreamAtMs || fqContext.nowMs,
        now: Date.now(),
      };

      try {
        await fetch(releaseUrl, {
          method: 'POST',
          headers: buildHeaders(),
          body: JSON.stringify(payload),
        });
        console.log(`[FQ] slot released via slot-handler host=${fqContext.hostname}`);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.error('[FQ] releaseSlot error (slot-handler):', message);
      }
    },
  };
};

function safeDecodePathname(pathname) {
  try {
    return decodeURIComponent(pathname);
  } catch {
    return null;
  }
}

// Normalizes the request path the same way Landing uses it for hashing.
function normalizePath(pathname) {
  if (typeof pathname !== 'string') {
    return null;
  }
  const decoded = safeDecodePathname(pathname);
  if (decoded === null) {
    return null;
  }
  if (decoded.length === 0) {
    return '/';
  }
  return decoded.startsWith('/') ? decoded : `/${decoded}`;
}
// src/handleDownload.ts
async function handleDownload(request, env, config, cacheManager, throttleManager, rateLimiter, ctx) {
  const originalRequest = request;
  const origin = request.headers.get("origin") ?? "*";
  const url = new URL(request.url);
  const normalizedPath = normalizePath(url.pathname);
  let path = normalizedPath;
  let clientAborted = false;
  const clientSignal = request.signal;
  if (clientSignal && typeof clientSignal.addEventListener === 'function') {
    clientSignal.addEventListener('abort', () => {
      clientAborted = true;
    });
  }

  if (path === null || typeof path !== "string") {
    return createErrorResponse(origin, 400, "invalid path encoding");
  }

  const downloadDecision = ctx && ctx.controllerState ? ctx.controllerState?.decision?.download : null;
  if (!downloadDecision) {
    return createErrorResponse(origin, 503, "controller decision unavailable");
  }

  const actions = normalizeControllerPathActions(downloadDecision);
  const originCheckModes = extractControllerOriginModes(downloadDecision);

  // Handle block action
  if (actions.includes('block')) {
    return createErrorResponse(origin, 403, "access denied");
  }

  const skipOriginByAction = actions.includes('skip-origin');
  const needOriginCheck = !skipOriginByAction && originCheckModes.length > 0;

  const clientIpValue = getClientIp(request);
  const clientIP = clientIpValue || "";

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
  let shouldCheckAddition = config.additionCheck || needOriginCheck;
  let shouldCheckAdditionExpireTime = config.additionExpireTimeCheck || needOriginCheck;
  let dynamicIdleTimeout = null;

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
    if (actions.includes('skip-addition') && !needOriginCheck) {
      shouldCheckAddition = false;
    }
    if (actions.includes('skip-addition-expiretime') && !needOriginCheck) {
      shouldCheckAdditionExpireTime = false;
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
    const actualWorkerAddress = new URL(request.url).origin;
    const workerVerifyData = JSON.stringify({ path: path, worker_addr: actualWorkerAddress });
    const workerVerifyResult = await verify("workerSign", workerVerifyData, workerSign, config.token);
    if (workerVerifyResult !== "") {
      return createUnauthorizedResponse(origin, workerVerifyResult);
    }
  }

  const additionalInfo = url.searchParams.get("additionalInfo") ?? "";
  const additionalInfoSign = url.searchParams.get("additionalInfoSign") ?? "";
  let additionalPayload = null;
  if (shouldCheckAddition) {
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

    try {
      additionalPayload = JSON.parse(decodedAdditional);
    } catch (_error) {
      return createUnauthorizedResponse(origin, "additionalInfo invalid");
    }

    // Extract idle_timeout from additionalInfo (priority: additionalInfo > env)
    if (additionalPayload && typeof additionalPayload === "object") {
      const { idle_timeout: idleTimeoutOverride } = additionalPayload;

      if (typeof idleTimeoutOverride === "number" && Number.isFinite(idleTimeoutOverride) && idleTimeoutOverride >= 0) {
        dynamicIdleTimeout = Math.trunc(idleTimeoutOverride);
        console.log("[IDLE] Using idle_timeout from additionalInfo:", dynamicIdleTimeout);
      }
    }

    const normalizedPathForHash = normalizedPath ?? normalizePath(url.pathname);
    if (typeof normalizedPathForHash !== "string" || normalizedPathForHash.length === 0) {
      return createErrorResponse(origin, 400, "invalid path encoding");
    }
    const currentPathHash = await sha256Hex(normalizedPathForHash);
    if (typeof additionalPayload.pathHash !== "string" || additionalPayload.pathHash !== currentPathHash) {
      return createUnauthorizedResponse(origin, "additionalInfo path mismatch");
    }

    if (shouldCheckAdditionExpireTime) {
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

  if (needOriginCheck) {
    if (!additionalPayload || typeof additionalPayload !== 'object') {
      return createUnauthorizedResponse(origin, "origin payload missing");
    }
    const encryptedSnapshot = typeof additionalPayload.encrypt === 'string' ? additionalPayload.encrypt : '';
    if (!encryptedSnapshot) {
      return createUnauthorizedResponse(origin, "origin encrypt missing");
    }
    const snapshot = await decryptOriginSnapshot(encryptedSnapshot, config.token);
    if (!snapshot) {
      console.warn('[Origin Binding] Failed to decrypt snapshot');
      return createUnauthorizedResponse(origin, "origin decrypt failed");
    }
    const cf = request.cf || {};
    const currentOrigin = {
      ip_addr: clientIpValue || null,
      country: cf.country,
      continent: cf.continent,
      region: cf.region,
      city: cf.city,
      asn: typeof cf.asn === 'undefined' || cf.asn === null ? undefined : String(cf.asn),
    };
    const originResult = checkOriginMatch(snapshot, currentOrigin, originCheckModes, {
      ipv4Suffix: config.ipv4Suffix,
      ipv6Suffix: config.ipv6Suffix,
    });
    if (!originResult.ok) {
      console.warn('[Origin Binding] Mismatch:', originResult.failedFields);
      return createUnauthorizedResponse(origin, "origin mismatch");
    }
  }

  // ========================================
  // UNIFIED CHECK (RTT 3→1 OPTIMIZATION)
  // ========================================
  let unifiedResult = null;
  let cacheHit = false;
  let linkData = null;
  
  // Use unified check when rate limit is enabled and dbMode is custom-pg-rest
  const supportsUnifiedCheck = config.rateLimitEnabled && config.dbMode === 'custom-pg-rest';

  if (supportsUnifiedCheck) {
    try {
      const rateLimitConfig = config.rateLimitConfig || {};
      const cacheConfig = config.cacheConfig || {};
      const throttleConfig = config.throttleConfig || {};
      const limitConfigValue = rateLimitConfig.limit ?? config.ipSubnetLimit;
      const effectiveIdleTimeout =
        dynamicIdleTimeout ?? cacheConfig.idleTimeout ?? config.idleTimeout ?? 0;

      unifiedResult = await unifiedCheck(path, clientIP, {
        postgrestUrl: rateLimitConfig.postgrestUrl,
        verifyHeader: rateLimitConfig.verifyHeader,
        verifySecret: rateLimitConfig.verifySecret,
        linkTTL: cacheConfig.linkTTL ?? 1800,
        idleTimeout: effectiveIdleTimeout,
        cacheTableName: cacheConfig.tableName || 'DOWNLOAD_CACHE_TABLE',
        windowTimeSeconds: rateLimitConfig.windowTimeSeconds ?? 86400,
        limit: limitConfigValue ?? 100,
        blockTimeSeconds: rateLimitConfig.blockTimeSeconds ?? 600,
        ipv4Suffix: rateLimitConfig.ipv4Suffix ?? '/32',
        ipv6Suffix: rateLimitConfig.ipv6Suffix ?? '/60',
        rateLimitTableName: rateLimitConfig.tableName || 'DOWNLOAD_IP_RATELIMIT_TABLE',
        throttleTimeWindow: throttleConfig.throttleTimeWindow ?? 60,
        throttleTableName: throttleConfig.tableName || 'THROTTLE_PROTECTION',
        lastActiveTableName: cacheConfig.lastActiveTableName || config.lastActiveTableName,
      });
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
    if (unifiedResult) {
      console.log('[Idle Debug] Unified check idle payload:', unifiedResult.idle ?? null);
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

      if (unifiedResult.idle && unifiedResult.idle.expired) {
        const idleReason = unifiedResult.idle.reason || 'Link expired due to inactivity';
        const idleDuration = unifiedResult.idle.idleDuration ?? 'unknown';
        const idleTimeout = unifiedResult.idle.timeout ?? 'unknown';
        console.warn(
          `[Idle Timeout] Link expired (idle ${idleDuration}s, timeout ${idleTimeout}s)`
        );
        return createErrorResponse(origin, 410, idleReason);
      }

      if (unifiedResult.cache.hit) {
        cacheHit = true;
        linkData = unifiedResult.cache.linkData;
      }

      if (config.throttleEnabled && unifiedResult.throttle.status === 'protected') {
        console.log(`[Throttle] Protected from unified check, returning error ${unifiedResult.throttle.errorCode}, retry after ${unifiedResult.throttle.retryAfter}s`);

        return createThrottleProtectedResponse(origin, unifiedResult.throttle);
      }

      if (rateLimiter && config.rateLimitConfig) {
        const probability = config.rateLimitConfig.cleanupProbability || 0.01;
        if (Math.random() < probability) {
          console.log(`[Rate Limit Cleanup] Triggered cleanup (probability: ${probability * 100}%)`);

          const { cleanupExpiredRecords } = await import('./ratelimit/custom-pg-rest.js');
          const cleanupPromise = cleanupExpiredRecords(
            config.rateLimitConfig.postgrestUrl,
            config.rateLimitConfig.verifyHeader,
            config.rateLimitConfig.verifySecret,
            config.rateLimitConfig.tableName,
            config.rateLimitConfig.windowTimeSeconds
          ).catch((cleanupError) => {
            console.error('[Rate Limit Cleanup] Failed:', cleanupError instanceof Error ? cleanupError.message : String(cleanupError));
          });

          if (cleanupPromise && ctx && ctx.waitUntil) {
            ctx.waitUntil(cleanupPromise);
          }
        }
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

  const fetchLinkDataFromApi = async (options = {}) => {
    const { forceRefresh = false, linkType } = options;
    const headers = {
      "content-type": "application/json;charset=UTF-8",
      Authorization: config.token,
      "CF-Connecting-IP-WORKERS": clientIP,
    };
    applyVerifyHeaders(headers, config.verifyHeader, config.verifySecret);
    const requestUrl = new URL(`${config.address}/api/fs/link`);
    if (forceRefresh) {
      requestUrl.searchParams.set("refresh", "true");
      const typeVal =
        linkType ||
        (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function"
          ? crypto.randomUUID()
          : `refresh-${Date.now()}-${Math.random().toString(16).slice(2)}`);
      requestUrl.searchParams.set("type", typeVal);
    } else if (linkType) {
      requestUrl.searchParams.set("type", linkType);
    }
    const payload = forceRefresh ? { path, refresh: true } : { path };
    const resp = await fetch(requestUrl.toString(), {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });

    const contentType = resp.headers.get("content-type") || "";
    if (!contentType.includes("application/json")) {
      const originalStatus = resp.status;
      const safeErrorMessage = JSON.stringify({
        code: originalStatus,
        message: `Request failed with status: ${originalStatus}`,
      });
      const safeHeaders = new Headers();
      safeHeaders.set("content-type", "application/json;charset=UTF-8");
      safeHeaders.set("Access-Control-Allow-Origin", origin);
      safeHeaders.append("Vary", "Origin");

      return {
        errorResponse: new Response(safeErrorMessage, {
          status: originalStatus,
          statusText: "Error",
          headers: safeHeaders,
        }),
      };
    }

    const apiResult = await resp.json();
    if (apiResult.code !== 200) {
      const httpStatus = apiResult.code >= 100 && apiResult.code < 600 ? apiResult.code : 500;
      const safeHeaders = new Headers();
      safeHeaders.set("content-type", "application/json;charset=UTF-8");
      safeHeaders.set("Access-Control-Allow-Origin", origin);
      safeHeaders.append("Vary", "Origin");
      return {
        errorResponse: new Response(JSON.stringify(apiResult), {
          status: httpStatus,
          headers: safeHeaders,
        }),
      };
    }

    if (cacheManager && apiResult.data) {
      if (forceRefresh && shouldRetryAuthError(apiResult.code || 0)) {
        console.warn('[Cache] Skip cache save due to auth error during refresh');
      } else {
        ctx.waitUntil(
          cacheManager
            .saveCache(path, apiResult.data, { ...config.cacheConfig, ctx })
            .catch((error) => {
              console.error('[Cache] Save failed:', error instanceof Error ? error.message : String(error));
            })
        );
      }
    }

    return { res: apiResult };
  };

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

  if (!res) {
    const { res: apiResult, errorResponse } = await fetchLinkDataFromApi();
    if (errorResponse) {
      return errorResponse;
    }
    res = apiResult;
  }

  // Use linkData from cache or API response
  let downloadUrl = res.data.url;
  // ========================================
  // Throttle protection logic (pre-check)
  // ========================================
  let throttleStatus = null;
  let throttleHostname = null;

  const throttleCheckEnabled = config.throttleEnabled && throttleManager;
  if (throttleCheckEnabled) {
    throttleHostname = extractHostname(downloadUrl);

    const unifiedThrottleUsable =
      unifiedResult &&
      unifiedResult.cache &&
      unifiedResult.cache.hit &&
      unifiedResult.cache.hostnameHash &&
      unifiedResult.throttle;

    if (unifiedThrottleUsable) {
      throttleStatus = unifiedResult.throttle;
    } else if (throttleHostname) {
      let hostnameMatched = false;
      for (const pattern of config.throttleHostnamePatterns) {
        if (matchHostnamePattern(throttleHostname, pattern)) {
          hostnameMatched = true;
          break;
        }
      }

      if (hostnameMatched) {
        try {
          throttleStatus = await throttleManager.checkThrottle(throttleHostname, { ...config.throttleConfig, ctx });
        } catch (error) {
          // Throttle check failure should not block downloads
          console.error('[Throttle] Check failed, proceeding with download:', error instanceof Error ? error.message : String(error));
        }
      }
    }

    if (throttleStatus) {
      if (throttleStatus.status === 'protected') {
        console.log(
          `[Throttle] Protected: ${throttleHostname}, returning error ${throttleStatus.errorCode}, retry after ${throttleStatus.retryAfter}s`
        );
        return createThrottleProtectedResponse(origin, throttleStatus);
      } else if (throttleStatus.status === 'resume_operation') {
        console.log(`[Throttle] Resume operation: ${throttleHostname}`);
      }
    }
  }

  // ========================================
  // Fair Upstream Queue Integration
  // ========================================
  const upstreamHostname = extractHostname(downloadUrl);
  const needFairQueue =
    config.fairQueueEnabled &&
    upstreamHostname &&
    config.fairQueueHostnamePatterns.some((pattern) => matchHostnamePattern(upstreamHostname, pattern));

  let fairQueueClient = null;
  let fqContext = null;
  let shouldSendFQCancel = false;
  let earlyResponse = null;

  if (needFairQueue) {
    if (!config.slotHandlerConfig?.url) {
      console.error('[Fair Queue] enabled but slot-handler URL missing');
      return createErrorResponse(origin, 503, 'Fair queue misconfigured (slot-handler URL missing)');
    }

    const clientIpSubnet = calculateIPSubnet(clientIP, config.ipv4Suffix, config.ipv6Suffix);

    if (clientIpSubnet) {
      const clientIpSubnetHash = await sha256Hash(clientIpSubnet);
      const hostnameHash = await sha256Hash(upstreamHostname);
      try {
        fairQueueClient = createFairQueueClient(config);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.error('[Fair Queue] Failed to initialize client:', message);
        return createErrorResponse(origin, 503, 'Fair queue unavailable');
      }
      fqContext = {
        hostname: upstreamHostname,
        hostnameHash,
        ipBucket: clientIpSubnetHash,
        nowMs: Date.now(),
      };

      try {
        const fqResult = await fairQueueClient.waitForSlot(ctx, fqContext);
        if (fqResult.kind === 'throttled') {
          return createThrottleProtectedResponse(origin, {
            status: 'protected',
            errorCode: fqResult.throttleCode || 503,
            retryAfter: fqResult.retryAfter,
          });
        }

        if (fqResult.kind === 'timeout') {
          const safeHeaders = new Headers();
          safeHeaders.set("content-type", "application/json;charset=UTF-8");
          safeHeaders.set("Access-Control-Allow-Origin", origin);
          safeHeaders.append("Vary", "Origin");
          safeHeaders.set("Retry-After", "60");

          return new Response(
            JSON.stringify({
              code: 503,
              message: 'Upstream queue timeout, please retry later'
            }),
            {
              status: 503,
              headers: safeHeaders
            }
          );
        }
      } catch (error) {
        if (clientAborted && isAbortError(error)) {
          shouldSendFQCancel = true;
          earlyResponse = createClientAbortResponse(origin);
        } else {
          const message = error instanceof Error ? error.message : String(error);
          console.error('[Fair Queue] waitForSlot error:', message);
          return createErrorResponse(origin, 503, 'Fair queue unavailable');
        }
      }
    } else {
      console.warn('[Fair Queue] Skipped: unable to derive client subnet for queue enforcement');
    }
  }

  const buildUpstreamRequest = (urlValue, headerConfig) => {
    const upstreamRequest = new Request(urlValue, originalRequest);
    if (headerConfig && typeof headerConfig === 'object') {
      Object.keys(headerConfig).forEach((key) => {
        const entries = Array.isArray(headerConfig[key]) ? headerConfig[key] : [headerConfig[key]];
        entries.forEach((value) => {
          if (typeof value === 'string') {
            upstreamRequest.headers.set(key, value);
          }
        });
      });
    }
    return upstreamRequest;
  };
  const shouldRetryAuthError = (status) => status === 401 || status === 410;

  let retriedWithFreshLink = false;

  // Proceed with fetch
  try {
    if (earlyResponse) {
      return earlyResponse;
    }

    request = buildUpstreamRequest(downloadUrl, res.data.header);
    if (fqContext && !fqContext.hitUpstreamAtMs) {
      fqContext.hitUpstreamAtMs = Date.now();
    }
    let response = await fetch(request);
    while (response.status >= 300 && response.status < 400) {
      const location = response.headers.get("Location");
      if (location) {
        const currentOrigin = new URL(originalRequest.url).origin;
        if (location.startsWith(`${currentOrigin}/`)) {
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

    if (!retriedWithFreshLink && shouldRetryAuthError(response.status)) {
      retriedWithFreshLink = true;
      console.warn(`[Upstream] Auth error ${response.status} for ${path}, refreshing link from API`);
      const refreshType =
        typeof crypto !== "undefined" && typeof crypto.randomUUID === "function"
          ? crypto.randomUUID()
          : `refresh-${Date.now()}-${Math.random().toString(16).slice(2)}`;
      const { res: refreshedLink, errorResponse } = await fetchLinkDataFromApi({
        forceRefresh: true,
        linkType: refreshType,
      });
      if (errorResponse) {
        console.warn('[Upstream] Failed to refresh link due to API error, returning original response');
      } else if (refreshedLink && refreshedLink.data && refreshedLink.data.url) {
        downloadUrl = refreshedLink.data.url;
        res = refreshedLink;
        request = buildUpstreamRequest(downloadUrl, res.data.header);
        response = await fetch(request);
        while (response.status >= 300 && response.status < 400) {
          const location = response.headers.get("Location");
          if (location) {
            const currentOrigin = new URL(originalRequest.url).origin;
            if (location.startsWith(`${currentOrigin}/`)) {
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
      }
    }

    if (response.status !== 200 && response.status !== 206) {
      console.warn(
        `[Upstream] Unexpected status ${response.status} for ${path} (url ${downloadUrl})`
      );
    }
    
    // Update throttle protection status based on fetch result
    if (config.throttleEnabled && throttleManager && throttleHostname) {
      try {
        const statusCode = response.status;
        const protectedHttpCodes = Array.isArray(config.throttleConfig?.protectedHttpCodes)
          ? config.throttleConfig.protectedHttpCodes
          : [];

        const isProtectedError = protectedHttpCodes.includes(statusCode);
        const isSuccessStatus = statusCode >= 200 && statusCode < 400;

        if (isProtectedError || isSuccessStatus) {
          const eventType = isProtectedError ? 'error' : 'success';
          if (isProtectedError) {
            console.log(`[Throttle] Error ${statusCode} from ${throttleHostname}, reporting to throttle window`);
          }

          const updatePromise = throttleManager.updateThrottle(
            throttleHostname,
            {
              eventType,
              statusCode,
            },
            { ...config.throttleConfig, ctx }
          );

          if (ctx && ctx.waitUntil) {
            ctx.waitUntil(updatePromise);
          }
        }
      } catch (error) {
        // Throttle update failure should not block downloads
        console.error('[Throttle] Update failed:', error instanceof Error ? error.message : String(error));
      }
    }

    // 创建仅包含安全必要headers的响应
    const safeHeaders = new Headers();
    const isCryptedDownload = additionalPayload?.isCrypted === true;

    // 保留重要的内容相关headers
    const preserveHeaders = [
      'content-type',
      'content-disposition',
      'content-length',
      'cache-control',
      'content-encoding',
      'accept-ranges',
      'content-range', // Added for partial downloads
      'transfer-encoding', // Added for chunked transfers
      'content-language', // Added for internationalization
      'expires', // Added for cache control
      'pragma', // Added for cache control
      'etag',
      'last-modified'
    ];

    // 仅复制必要的headers
    preserveHeaders.forEach(header => {
      if (header === 'content-disposition' && isCryptedDownload) {
        return;
      }
      const value = response.headers.get(header);
      if (value) {
        safeHeaders.set(header, value);
      }
    });

    if (isCryptedDownload) {
      const derivedName = deriveFileNameFromPath(path);
      const encryptedFileName = ensureEncryptedFileName(derivedName);
      safeHeaders.set('content-disposition', buildAttachmentContentDisposition(encryptedFileName));
    }

    // 设置CORS headers
    applyDownloadCorsHeaders(safeHeaders);

    // 创建带有安全headers的新响应
    const safeResponse = new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers: safeHeaders
    });

    const shouldUpdateLastActive =
      config.cacheConfig &&
      typeof config.cacheConfig.idleTimeout === 'number' &&
      config.cacheConfig.idleTimeout > 0 &&
      config.cacheConfig.lastActiveTableName;

    if (
      shouldUpdateLastActive &&
      config.dbMode === 'custom-pg-rest' &&
      clientIP &&
      typeof path === 'string'
    ) {
      const updatePromise = (async () => {
        try {
          const ipSubnet = calculateIPSubnet(clientIP, config.ipv4Suffix, config.ipv6Suffix);
          if (!ipSubnet) {
            return;
          }

          const [ipHash, pathHash] = await Promise.all([sha256Hash(ipSubnet), sha256Hash(path)]);
          if (!ipHash || !pathHash) {
            return;
          }

          const { updateLastActive } = await import('./unified-check.js');
          await updateLastActive(config.cacheConfig, ipHash, pathHash);
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          console.error('[LastActive] Update failed:', message);
        }
      })();

      if (ctx && ctx.waitUntil) {
        ctx.waitUntil(updatePromise);
      }
    }

    return safeResponse;
  } finally {
    if (shouldSendFQCancel && fairQueueClient && fqContext && fqContext.queryToken) {
      const cancelPromise = fairQueueClient.cancelSession(ctx, fqContext);
      if (ctx && typeof ctx.waitUntil === 'function') {
        ctx.waitUntil(cancelPromise);
      } else {
        cancelPromise.catch((error) => {
          const message = error instanceof Error ? error.message : String(error);
          console.warn('[Fair Queue] cancelSession failed:', message);
        });
      }
    }

    if (fairQueueClient && fqContext && fqContext.slotToken) {
      const releasePromise = fairQueueClient.releaseSlot(ctx, fqContext);
      if (ctx && typeof ctx.waitUntil === 'function') {
        ctx.waitUntil(releasePromise);
      } else {
        await releasePromise;
      }
    }
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
  const origin = request.headers.get("origin") ?? "*";
  // Check for IPv6 access if IPv4_ONLY is enabled
  if (config.ipv4Only) {
    const clientIP = getClientIp(request) || "";
    if (isIPv6(clientIP)) {
      const safeHeaders = new Headers();
      safeHeaders.set("content-type", "application/json;charset=UTF-8");
      safeHeaders.set("Access-Control-Allow-Origin", origin);
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
export { BootstrapDO, MetricsDO };

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname || '/';

      if (pathname.startsWith('/api/v0/')) {
        const internalResponse = await handleInternalApiIfAny(request, env, ctx);
        if (internalResponse) {
          return internalResponse;
        }
      }

      let controllerState = null;
      try {
        controllerState = await fetchControllerState(request, env);
      } catch (error) {
        console.error('[controller] state fetch error:', error instanceof Error ? error.message : String(error));
      }
      if (!controllerState || !controllerState.bootstrap || !controllerState.decision) {
        return createErrorResponse("*", 503, "controller state unavailable");
      }

      const config = resolveConfig(env || {}, controllerState.bootstrap, controllerState.decision);
      // Create cache manager instance based on DB_MODE
      const cacheManager = config.cacheEnabled ? createCacheManager(config.dbMode) : null;
      // Create throttle manager instance based on DB_MODE (if throttle enabled)
      const throttleManager = config.throttleEnabled ? createThrottleManager(config.dbMode) : null;
      const rateLimiter = config.rateLimitEnabled ? createRateLimiter(config.dbMode) : null;

      ctx.controllerState = controllerState;

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
