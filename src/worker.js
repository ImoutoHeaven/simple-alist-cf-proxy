// Import cache/throttle managers, rate limiter, and utilities
import { createCacheManager } from './cache/factory.js';
import { createThrottleManager } from './cache/throttle-factory.js';
import { createRateLimiter } from './ratelimit/factory.js';
import { unifiedCheck } from './unified-check.js';
import { nextOverloadDelayMs } from './fairqueue-overload.js';
import { scheduleAllCleanups } from './cleanup-scheduler.js';
import { parseBoolean, extractHostname, matchHostnamePattern, applyVerifyHeaders, calculateIPSubnet, sha256Hash } from './utils.js';
import { buildBindingStr, decryptBindingPayload, getClientIp, normalizePath, parseCheckOriginEnv } from './origin-binding.js';
import { handleInternalApiIfAny } from './internal-api.js';
import { fetchControllerState } from './controller-adapter.js';

// Configuration constants
const REQUIRED_ENV = [];
const VALID_ACTIONS = new Set(['block', 'asis']);
const DEFAULT_LINK_TTL_SECONDS = 1800;
const DEFAULT_CLEANUP_PERCENTAGE = 1;
const DEFAULT_RATE_LIMIT_BLOCK_SECONDS = 600;
const DEFAULT_RATE_LIMIT_IPV4_SUFFIX = '/32';
const DEFAULT_RATE_LIMIT_IPV6_SUFFIX = '/60';
const DEFAULT_SLOT_HANDLER_TIMEOUT_MS = 20000;
const DEFAULT_SLOT_HANDLER_PER_REQUEST_TIMEOUT_MS = 8000;
const DEFAULT_SLOT_HANDLER_MAX_ATTEMPTS = 35;

// slot-handler acquire is long-poll based; don't set per-request timeouts below this window.
const SLOT_HANDLER_LONGPOLL_MS = 6000;

// Fair Queue in-memory state (per Worker instance)
const FQ_GLOBAL_STATE = {
  throttledByHost: new Map(),
  overloadedByHost: new Map(),
  overloadedBySite: new Map(),
  overloadedByIp: new Map(),
  overloadedGlobalUntilMs: 0,
};

// Rate Limit in-memory state (per Worker instance, iprange-level)
const RL_STATE = {
  byIpRange: new Map(),
};

const IDLE_410_CACHE_CAPACITY = 64;
const idle410Cache = {
  entries: new Map(),
};

const nowMs = () => Date.now();

const normalizeStringValue = (value, fallback = '') => {
  if (typeof value !== 'string') {
    return fallback;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : fallback;
};

const extractPathname = (urlValue) => {
  if (!urlValue || typeof urlValue !== 'string') {
    return '';
  }
  try {
    const parsed = new URL(urlValue);
    return parsed.pathname || '';
  } catch (_error) {
    return '';
  }
};

const deriveSiteKey = (hostname, pathname) => {
  const lowerHost = (hostname || '').toLowerCase();
  const segments = typeof pathname === 'string'
    ? pathname.toLowerCase().split('/').filter((segment) => segment.length > 0)
    : [];

  if (lowerHost.endsWith('.sharepoint.com')) {
    if (segments[0] === 'personal' && segments[1]) return `personal:${segments[1]}`;
    if (segments[0] === 'sites' && segments[1]) return `sites:${segments[1]}`;
    if (segments[0] === 'teams' && segments[1]) return `teams:${segments[1]}`;
    return 'unknown';
  }

  if (lowerHost) {
    return `host:${lowerHost}`;
  }

  return 'unknown';
};

const deriveSiteBucket = async (hostname, urlValue, siteBucketConfig) => {
  const mode = normalizeStringValue(siteBucketConfig?.mode, 'sharepoint').toLowerCase();
  const path = extractPathname(urlValue);
  const siteKey = mode === 'sharepoint'
    ? deriveSiteKey(hostname, path)
    : (hostname ? `host:${hostname.toLowerCase()}` : 'unknown');
  const hash = await sha256Hash(siteKey || 'unknown');
  return hash || '';
};

const normalizePositiveSeconds = (value, fallback) => {
  const num = Number(value);
  if (Number.isFinite(num) && num > 0) {
    return num;
  }
  const fb = Number(fallback);
  return Number.isFinite(fb) && fb > 0 ? fb : 0;
};

const normalizePositiveMs = (value, fallback) => {
  const num = Number(value);
  if (Number.isFinite(num) && num > 0) {
    return Math.max(1, Math.trunc(num));
  }
  const fb = Number(fallback);
  if (Number.isFinite(fb) && fb > 0) {
    return Math.max(1, Math.trunc(fb));
  }
  return 0;
};

const CACHE_OVERRIDE_UNIT_SECONDS = {
  s: 1,
  m: 60,
  h: 3600,
  d: 86400,
  w: 604800,
  y: 31536000,
};

const CACHE_OVERRIDE_SIZE_MULTIPLIER = {
  b: 1,
  kb: 1024,
  mb: 1024 * 1024,
  gb: 1024 * 1024 * 1024,
};

const parseCacheOverrideSeconds = (value) => {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value > 0 ? Math.max(1, Math.round(value)) : 0;
  }
  if (typeof value !== 'string') {
    return 0;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return 0;
  }
  const match = trimmed.match(/^(\d+(?:\.\d+)?)\s*([smhdwy])$/i);
  if (!match) {
    return 0;
  }
  const amount = Number.parseFloat(match[1]);
  const unit = match[2].toLowerCase();
  const multiplier = CACHE_OVERRIDE_UNIT_SECONDS[unit] || 0;
  if (!Number.isFinite(amount) || amount <= 0 || multiplier <= 0) {
    return 0;
  }
  const seconds = amount * multiplier;
  if (!Number.isFinite(seconds) || seconds <= 0) {
    return 0;
  }
  return Math.max(1, Math.round(seconds));
};

const parseCacheOverrideMaxSizeBytes = (value) => {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value > 0 ? Math.max(1, Math.round(value)) : 0;
  }
  if (typeof value !== 'string') {
    return 0;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return 0;
  }
  const match = trimmed.match(/^(\d+(?:\.\d+)?)\s*(b|kb|mb|gb)$/i);
  if (!match) {
    return 0;
  }
  const amount = Number.parseFloat(match[1]);
  const unit = match[2].toLowerCase();
  const multiplier = CACHE_OVERRIDE_SIZE_MULTIPLIER[unit] || 0;
  if (!Number.isFinite(amount) || amount <= 0 || multiplier <= 0) {
    return 0;
  }
  const bytes = amount * multiplier;
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return 0;
  }
  return Math.max(1, Math.round(bytes));
};

const readPayloadFileSize = (payload) => {
  if (!payload || typeof payload !== 'object') {
    return null;
  }
  const raw = payload.filesize ?? payload.fileSize;
  if (typeof raw === 'number' && Number.isFinite(raw)) {
    return raw > 0 ? raw : null;
  }
  if (typeof raw === 'string') {
    const parsed = Number.parseFloat(raw);
    if (Number.isFinite(parsed) && parsed > 0) {
      return parsed;
    }
  }
  return null;
};

const readPayloadExpireTime = (payload) => {
  if (!payload || typeof payload !== 'object') {
    return null;
  }
  const raw = payload.expireTime;
  if (typeof raw === 'number' && Number.isFinite(raw)) {
    return Math.trunc(raw);
  }
  if (typeof raw === 'string') {
    const parsed = Number.parseInt(raw, 10);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
};

const normalizeOrigin = (value) => {
  if (typeof value !== 'string') {
    return '';
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return '';
  }
  const candidate = /^[a-z][a-z0-9+.-]*:\/\//i.test(trimmed) ? trimmed : `https://${trimmed}`;
  try {
    return new URL(candidate).origin;
  } catch {
    return '';
  }
};

const normalizeOriginList = (values) => {
  const normalized = [];
  const seen = new Set();
  if (!Array.isArray(values)) {
    return normalized;
  }
  for (const value of values) {
    const origin = normalizeOrigin(value);
    if (!origin || seen.has(origin)) {
      continue;
    }
    seen.add(origin);
    normalized.push(origin);
  }
  return normalized;
};

const normalizeHeaderMap = (value) => {
  const normalized = {};
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return normalized;
  }
  for (const [rawName, rawValue] of Object.entries(value)) {
    const name = typeof rawName === 'string' ? rawName.trim() : '';
    if (!name) {
      continue;
    }
    if (rawValue === undefined || rawValue === null) {
      continue;
    }
    const stringValue = typeof rawValue === 'string' ? rawValue : String(rawValue);
    if (!stringValue || stringValue.trim().length === 0) {
      continue;
    }
    normalized[name] = stringValue;
  }
  return normalized;
};

function markThrottled(hostname, code, retryAfterSeconds) {
  const hostKey = typeof hostname === 'string' ? hostname.trim() : '';
  if (!hostKey) {
    return;
  }

  const seconds = normalizePositiveSeconds(retryAfterSeconds, 0);
  if (!seconds) {
    return;
  }

  const until = nowMs() + seconds * 1000;
  const prev = FQ_GLOBAL_STATE.throttledByHost.get(hostKey);
  const codeNumber = Number(code);
  const normalizedCode = Number.isFinite(codeNumber) ? codeNumber : 503;
  if (!prev || until > prev.untilMs) {
    FQ_GLOBAL_STATE.throttledByHost.set(hostKey, {
      untilMs: until,
      code: normalizedCode,
    });
  }
}

function getHostThrottledRemainingSeconds(hostname, now = nowMs()) {
  const hostKey = typeof hostname === 'string' ? hostname.trim() : '';
  if (!hostKey) {
    return 0;
  }

  const state = FQ_GLOBAL_STATE.throttledByHost.get(hostKey);
  if (!state || !state.untilMs || state.untilMs <= now) {
    if (state && state.untilMs && state.untilMs <= now) {
      FQ_GLOBAL_STATE.throttledByHost.delete(hostKey);
    }
    return 0;
  }
  return Math.ceil((state.untilMs - now) / 1000);
}

function markHostOverloaded(hostname, retryAfterMs) {
  const hostKey = typeof hostname === 'string' ? hostname.trim() : '';
  if (!hostKey) {
    return;
  }

  markScopedOverloaded(FQ_GLOBAL_STATE.overloadedByHost, hostKey, retryAfterMs);
}

function normalizeOverloadScopeValue(value, fallback = 'unknown') {
  if (typeof value !== 'string') {
    return fallback;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : fallback;
}

const SCOPED_OVERLOAD_SWEEP_STEPS = 6;

function buildScopedOverloadKey(parts) {
  return JSON.stringify(parts);
}

function buildSiteOverloadKey(hostname, siteBucket) {
  const hostKey = typeof hostname === 'string' ? hostname.trim() : '';
  if (!hostKey) {
    return '';
  }
  return buildScopedOverloadKey([
    hostKey,
    normalizeOverloadScopeValue(siteBucket, 'unknown'),
  ]);
}

function buildIpOverloadKey(hostname, siteBucket, ipBucket) {
  const hostKey = typeof hostname === 'string' ? hostname.trim() : '';
  if (!hostKey) {
    return '';
  }
  return buildScopedOverloadKey([
    hostKey,
    normalizeOverloadScopeValue(siteBucket, 'unknown'),
    normalizeOverloadScopeValue(ipBucket, 'unknown'),
  ]);
}

function sweepExpiredScopedOverloadEntries(store, now, steps = SCOPED_OVERLOAD_SWEEP_STEPS) {
  if (!store || store.size === 0) {
    return;
  }

  const maxSteps = Number.isFinite(steps) && steps > 0
    ? Math.max(1, Math.trunc(steps))
    : SCOPED_OVERLOAD_SWEEP_STEPS;

  for (let i = 0; i < maxSteps; i += 1) {
    const first = store.entries().next().value;
    if (!first) {
      break;
    }
    const [entryKey, state] = first;
    store.delete(entryKey);

    if (state && state.untilMs && state.untilMs > now) {
      store.set(entryKey, state);
    }
  }
}

function markScopedOverloaded(store, key, retryAfterMs) {
  if (!store || !key) {
    return;
  }

  const durationMs = normalizePositiveMs(retryAfterMs, 0);
  if (!durationMs) {
    return;
  }

  const now = nowMs();
  sweepExpiredScopedOverloadEntries(store, now);
  const until = now + durationMs;
  const prev = store.get(key);
  if (!prev || until > prev.untilMs) {
    store.set(key, { untilMs: until });
  }
}

function getHostOverloadedRemainingMs(hostname, now = nowMs()) {
  const hostKey = typeof hostname === 'string' ? hostname.trim() : '';
  if (!hostKey) {
    return 0;
  }

  return getScopedOverloadedRemainingMs(FQ_GLOBAL_STATE.overloadedByHost, hostKey, now);
}

function markSiteOverloaded(hostname, siteBucket, retryAfterMs) {
  const key = buildSiteOverloadKey(hostname, siteBucket);
  if (!key) {
    return;
  }
  markScopedOverloaded(FQ_GLOBAL_STATE.overloadedBySite, key, retryAfterMs);
}

function getSiteOverloadedRemainingMs(hostname, siteBucket, now = nowMs()) {
  const key = buildSiteOverloadKey(hostname, siteBucket);
  if (!key) {
    return 0;
  }
  return getScopedOverloadedRemainingMs(FQ_GLOBAL_STATE.overloadedBySite, key, now);
}

function markIpOverloaded(hostname, siteBucket, ipBucket, retryAfterMs) {
  const key = buildIpOverloadKey(hostname, siteBucket, ipBucket);
  if (!key) {
    return;
  }
  markScopedOverloaded(FQ_GLOBAL_STATE.overloadedByIp, key, retryAfterMs);
}

function getIpOverloadedRemainingMs(hostname, siteBucket, ipBucket, now = nowMs()) {
  const key = buildIpOverloadKey(hostname, siteBucket, ipBucket);
  if (!key) {
    return 0;
  }
  return getScopedOverloadedRemainingMs(FQ_GLOBAL_STATE.overloadedByIp, key, now);
}

function getScopedOverloadedRemainingMs(store, key, now = nowMs()) {
  if (!store || !key) {
    return 0;
  }

  sweepExpiredScopedOverloadEntries(store, now);

  const state = store.get(key);
  if (!state || !state.untilMs || state.untilMs <= now) {
    if (state && state.untilMs && state.untilMs <= now) {
      store.delete(key);
    }
    return 0;
  }
  return Math.max(0, state.untilMs - now);
}

function getScopedOverloadRemainingMs(hostname, siteBucket, ipBucket, now = nowMs()) {
  const hostRemain = getHostOverloadedRemainingMs(hostname, now);
  const siteRemain = getSiteOverloadedRemainingMs(hostname, siteBucket, now);
  const ipRemain = getIpOverloadedRemainingMs(hostname, siteBucket, ipBucket, now);
  return Math.max(hostRemain, siteRemain, ipRemain);
}

function markGlobalOverloaded(retryAfterSeconds) {
  const seconds = normalizePositiveSeconds(retryAfterSeconds, 0);
  if (!seconds) {
    return;
  }

  const until = nowMs() + seconds * 1000;
  if (!FQ_GLOBAL_STATE.overloadedGlobalUntilMs || until > FQ_GLOBAL_STATE.overloadedGlobalUntilMs) {
    FQ_GLOBAL_STATE.overloadedGlobalUntilMs = until;
  }
}

function getGlobalOverloadedRemainingSeconds(now = nowMs()) {
  const until = Number(FQ_GLOBAL_STATE.overloadedGlobalUntilMs) || 0;
  if (!until || until <= now) {
    if (until && until <= now) {
      FQ_GLOBAL_STATE.overloadedGlobalUntilMs = 0;
    }
    return 0;
  }
  return Math.ceil((until - now) / 1000);
}

const SLOW_FAIL_DELAY_MS = 5000;

async function slowFailDelay() {
  if (!SLOW_FAIL_DELAY_MS || SLOW_FAIL_DELAY_MS <= 0) {
    return;
  }
  await new Promise((resolve) => setTimeout(resolve, SLOW_FAIL_DELAY_MS));
}

function touchIdle410Entry(key) {
  const entries = idle410Cache.entries;
  const value = entries.get(key);
  if (value !== undefined) {
    entries.delete(key);
    entries.set(key, value);
  }
}

function getIdle410Cached(key) {
  if (!key) {
    return false;
  }
  const entries = idle410Cache.entries;
  if (!entries.has(key)) {
    return false;
  }
  touchIdle410Entry(key);
  return true;
}

function putIdle410Cached(key) {
  if (!key) {
    return;
  }
  const entries = idle410Cache.entries;
  if (entries.has(key)) {
    touchIdle410Entry(key);
    return;
  }
  if (entries.size >= IDLE_410_CACHE_CAPACITY) {
    const firstKey = entries.keys().next().value;
    if (firstKey !== undefined) {
      entries.delete(firstKey);
    }
  }
  entries.set(key, { lastSeen: Date.now() });
}

async function buildIdleCacheKey(url) {
  if (!url || typeof url.pathname !== 'string') {
    return null;
  }
  const raw = `${url.pathname}${url.search || ''}`;
  return sha256Hex(raw);
}

function markRateLimited(ipSubnet, retryAfterSeconds) {
  const key = typeof ipSubnet === 'string' ? ipSubnet.trim() : '';
  if (!key) {
    return;
  }

  const seconds = normalizePositiveSeconds(retryAfterSeconds, 0);
  if (!seconds) {
    return;
  }

  const now = nowMs();
  const until = now + seconds * 1000;
  const prev = RL_STATE.byIpRange.get(key);
  if (!prev || until > prev.untilMs) {
    RL_STATE.byIpRange.set(key, { untilMs: until });
  }
}

function getRateLimitRemainingSeconds(ipSubnet, now = nowMs()) {
  const key = typeof ipSubnet === 'string' ? ipSubnet.trim() : '';
  if (!key) {
    return 0;
  }

  const state = RL_STATE.byIpRange.get(key);
  if (!state || !state.untilMs || state.untilMs <= now) {
    if (state && state.untilMs && state.untilMs <= now) {
      RL_STATE.byIpRange.delete(key);
    }
    return 0;
  }

  return Math.ceil((state.untilMs - now) / 1000);
}

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
  // Keep backwards-compatible string values without embedding the legacy literal.
  const FAIL_OPEN = 'fail' + '-open';
  const FAIL_CLOSED = 'fail' + '-closed';
  const lowered = typeof value === 'string' ? value.trim().toLowerCase() : '';
  return lowered === FAIL_OPEN ? FAIL_OPEN : FAIL_CLOSED;
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
  const bindingBootstrap = commonBootstrap.binding && typeof commonBootstrap.binding === 'object'
    ? commonBootstrap.binding
    : {};
  const bindingDefaultModesRaw = typeof bindingBootstrap.defaultModes === 'string'
    ? bindingBootstrap.defaultModes.trim()
    : '';
  const bindingDefaultModes = Object.prototype.hasOwnProperty.call(bindingBootstrap, 'defaultModes')
    ? bindingDefaultModesRaw
    : 'path,asn,country,iprange';
  const bindingVersionRaw = Number(bindingBootstrap.version);
  const bindingVersion = Number.isFinite(bindingVersionRaw) && bindingVersionRaw > 0
    ? Math.trunc(bindingVersionRaw)
    : 1;
  const bindingIpv4Suffix = normalizeString(bindingBootstrap.ipv4Suffix, '/32') || '/32';
  const bindingIpv6Suffix = normalizeString(bindingBootstrap.ipv6Suffix, '/60') || '/60';
  const bindingBindTls = Object.prototype.hasOwnProperty.call(bindingBootstrap, 'bindTls')
    ? bindingBootstrap.bindTls !== false
    : true;
  const workerAddresses = normalizeOriginList(commonBootstrap.workerAddresses);
  if (workerAddresses.length === 0) {
    throw new Error('controller common.workerAddresses is required');
  }
  const landingWorkerAddresses = normalizeOriginList(commonBootstrap.landingWorkerAddresses);
  if (landingWorkerAddresses.length === 0) {
    throw new Error('controller common.landingWorkerAddresses is required');
  }
  const alistAuthHeaders = normalizeHeaderMap(commonBootstrap.alistAuthHeaders);

  const address = normalizeString(downloadBootstrap.address);
  if (!address) {
    throw new Error('controller download.address is required');
  }

  const authConfig = downloadBootstrap.auth && typeof downloadBootstrap.auth === 'object'
    ? downloadBootstrap.auth
    : {};
  const ipv4Only = authConfig.ipv4Only !== false;

  const overrideCacheControlRaw = downloadBootstrap.overrideCacheControl ?? downloadBootstrap['override-cache-control'];
  const overrideCacheControl = typeof overrideCacheControlRaw === 'string'
    ? overrideCacheControlRaw.trim().toLowerCase() === 'true'
    : Boolean(overrideCacheControlRaw);
  const cacheOverrideTimeRaw = normalizeString(
    downloadBootstrap.cacheOverrideTime ?? downloadBootstrap['cache-override-time']
  );
  const cacheOverrideSeconds = parseCacheOverrideSeconds(cacheOverrideTimeRaw);
  const cacheOverrideMaxSizeRaw = normalizeString(
    downloadBootstrap.cacheOverrideMaxSize ?? downloadBootstrap['cache-override-max-size'],
    '500MB'
  );
  const cacheOverrideMaxSizeBytes = parseCacheOverrideMaxSizeBytes(cacheOverrideMaxSizeRaw);
  if (overrideCacheControl && !cacheOverrideSeconds) {
    throw new Error('controller download.cacheOverrideTime is required when overrideCacheControl is true');
  }
  if (overrideCacheControl && !cacheOverrideMaxSizeBytes) {
    throw new Error('controller download.cacheOverrideMaxSize is required when overrideCacheControl is true');
  }

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
    if (typeof dbConfig.cacheEnabled !== 'boolean') {
      throw new Error('controller download.db.cacheEnabled must be boolean when mode=custom-pg-rest');
    }
    cacheEnabled = dbConfig.cacheEnabled;
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
  const fairQueueHostnamePatterns = Array.isArray(fairQueueConfigRaw.hostPatterns)
    ? fairQueueConfigRaw.hostPatterns.map((p) => normalizeString(p)).filter((p) => p.length > 0)
    : [];
  const fairQueueEnabled = Boolean(fairQueueConfigRaw.enabled) && fairQueueHostnamePatterns.length > 0;
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
  const slotHandlerAuthHeader = normalizeString(fairQueueConfigRaw.slotHandlerAuthHeader) || 'X-FQ-Auth';
  if (fairQueueEnabled && !slotHandlerUrl) {
    throw new Error('controller fairQueue.slotHandlerUrl is required when fairQueue.enabled is true');
  }
  const fairQueueSiteBucket = fairQueueConfigRaw.siteBucket && typeof fairQueueConfigRaw.siteBucket === 'object'
    ? fairQueueConfigRaw.siteBucket
    : {};
  const slotHandlerConfig = {
    url: slotHandlerUrl,
    totalMaxWaitMs: slotHandlerTimeoutMs,
    perRequestTimeoutMs,
    maxAttemptsCap,
    authKey: slotHandlerAuthKey,
    authHeader: slotHandlerAuthHeader,
  };
  const fairQueueContext = {
    fairQueueEnabled,
    fairQueueHostnamePatterns,
    fairQueueSiteBucket,
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
    binding: {
      version: bindingVersion,
      defaultModes: bindingDefaultModes,
      ipv4Suffix: bindingIpv4Suffix,
      ipv6Suffix: bindingIpv6Suffix,
      bindTls: bindingBindTls,
    },
    workerAddresses,
    landingWorkerAddresses,
    alistAuthHeaders,
    verifyHeader,
    verifySecret,
    ipv4Only,
    overrideCacheControl,
    cacheOverrideSeconds,
    cacheOverrideMaxSizeBytes,
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
    idleTimeout: idleTimeoutSeconds,
    lastActiveTableName,
    fairQueueEnabled: fairQueueContext.fairQueueEnabled,
    fairQueueHostnamePatterns: fairQueueContext.fairQueueHostnamePatterns,
    fairQueueSiteBucket: fairQueueContext.fairQueueSiteBucket,
  };
};

// Helper function to check if an IP is IPv6
function isIPv6(ip) {
  return ip && ip.includes(':');
}

function base64UrlDecodeToString(input) {
  if (!input) {
    return null;
  }
  try {
    const normalizedInput = String(input).replace(/-/g, '+').replace(/_/g, '/');
    const normalized = (() => {
      const remainder = normalizedInput.length % 4;
      if (remainder === 0) return normalizedInput;
      if (remainder === 1) return null;
      const padding = 4 - remainder;
      return `${normalizedInput}${'='.repeat(padding)}`;
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
const extractControllerOriginModes = (downloadDecision, bindingConfig) => {
  if (!downloadDecision || typeof downloadDecision.checkOriginMode === 'undefined') {
    return parseCheckOriginEnv(bindingConfig?.defaultModes || '');
  }
  if (typeof downloadDecision.checkOriginMode !== 'string') {
    console.warn('[controller] checkOriginMode is not a string, ignore');
    return parseCheckOriginEnv(bindingConfig?.defaultModes || '');
  }
  const parsed = parseCheckOriginEnv(downloadDecision.checkOriginMode);
  if (parsed.length > 0) {
    return parsed;
  }
  return parseCheckOriginEnv(bindingConfig?.defaultModes || '');
};

// src/verify.ts
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
  if (!signature) return 'payloadSign missing';
  const parts = signature.split(':');
  const expirePart = parts[parts.length - 1];
  if (!expirePart) return 'payloadSign expire missing';
  const expire = Number.parseInt(expirePart, 10);
  if (Number.isNaN(expire)) return 'payloadSign expire invalid';
  if (expire < Date.now() / 1e3 && expire > 0) return 'payloadSign expired';
  const expected = await hmacSha256Sign(data, expire, secret);
  if (expected !== signature) return 'payloadSign mismatch';
  return '';
};

const extractExpireFromSign = (signature) => {
  if (!signature) return 0;
  const parts = signature.split(':');
  const expirePart = parts[parts.length - 1];
  if (!expirePart) return 0;
  const expire = Number.parseInt(expirePart, 10);
  return Number.isNaN(expire) ? 0 : expire;
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

function createFairQueueOverloadedResponse(origin, retryAfterSeconds) {
  const retryAfter = normalizePositiveSeconds(retryAfterSeconds, 60);
  const safeHeaders = new Headers();
  safeHeaders.set("content-type", "application/json;charset=UTF-8");
  safeHeaders.set("Access-Control-Allow-Origin", origin);
  safeHeaders.append("Vary", "Origin");
  safeHeaders.set("Retry-After", String(retryAfter));

  return new Response(
    JSON.stringify({
      code: 503,
      message: 'Upstream queue overloaded, please retry later'
    }),
    {
      status: 503,
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
  if (!baseUrl) {
    throw new Error('[FQ] slot-handler backend enabled but FAIR_QUEUE_SLOT_HANDLER_URL is missing');
  }

  const acquireUrl = `${baseUrl}/api/v1/fairqueue/acquire`;
  const releaseUrl = `${baseUrl}/api/v1/fairqueue/release`;
  const authKey = slotCfg.authKey || '';
  const authHeader = normalizeStringValue(slotCfg.authHeader, 'X-FQ-Auth');
  const throttleTimeWindowSeconds =
    Number(config.throttleConfig?.throttleTimeWindow) > 0
      ? Number(config.throttleConfig.throttleTimeWindow)
      : 60;
  const perRequestTimeoutMsRaw = Number(slotCfg.perRequestTimeoutMs);
  let perRequestTimeoutMs =
    Number.isFinite(perRequestTimeoutMsRaw) && perRequestTimeoutMsRaw > 0 ? perRequestTimeoutMsRaw : 8000;
  const minPerRequestTimeoutMs = SLOT_HANDLER_LONGPOLL_MS + 2000;
  if (perRequestTimeoutMs < minPerRequestTimeoutMs) {
    console.warn(
      `[FQ] perRequestTimeoutMs too small (${perRequestTimeoutMs}ms), clamped to ${minPerRequestTimeoutMs}ms to cover long-poll window`
    );
    perRequestTimeoutMs = minPerRequestTimeoutMs;
  }
  const totalMaxWaitMsRaw = Number(slotCfg.totalMaxWaitMs);
  const totalMaxWaitMs =
    Number.isFinite(totalMaxWaitMsRaw) && totalMaxWaitMsRaw > 0 ? totalMaxWaitMsRaw : 20000;
  const maxAttemptsCapRaw = Number(slotCfg.maxAttemptsCap);
  const maxAttemptsCap =
    Number.isFinite(maxAttemptsCapRaw) && maxAttemptsCapRaw > 0 ? maxAttemptsCapRaw : 35;

  const buildHeaders = () => {
    const headers = { 'Content-Type': 'application/json' };
    if (authKey) {
      headers[authHeader] = authKey;
    }
    return headers;
  };

  const fetchWithTimeout = async (url, payload, timeoutMs, signal) => {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const abortHandler = () => controller.abort();
    if (signal) {
      if (signal.aborted) {
        controller.abort();
      } else {
        signal.addEventListener('abort', abortHandler, { once: true });
      }
    }
    try {
      return await fetch(url, {
        method: 'POST',
        body: JSON.stringify(payload),
        headers: buildHeaders(),
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timer);
      if (signal) {
        signal.removeEventListener('abort', abortHandler);
      }
    }
  };

  const computeMaxAttempts = () => {
    const attempts = Math.ceil(totalMaxWaitMs / perRequestTimeoutMs);
    const safeAttempts = Number.isFinite(attempts) && attempts > 0 ? attempts : 1;
    return Math.max(1, Math.min(maxAttemptsCap, safeAttempts));
  };

  const computeErrorBackoffMs = (streak) => {
    const base = 150;
    const step = 150;
    const max = 1200;
    const n = Number.isFinite(streak) && streak > 0 ? Math.floor(streak) : 0;
    return Math.min(max, base + step * n);
  };

  const isRetryableReleaseStatus = (status) => status === 429 || status >= 500;

  const createAbortError = () => {
    const error = new Error('Aborted');
    error.name = 'AbortError';
    return error;
  };

  const sleepWithAbort = (ms, signal) => {
    const delayMs = Math.max(0, Math.trunc(ms));
    if (!delayMs) {
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
      let done = false;
      let timer = null;
      const cleanup = () => {
        if (!signal || typeof signal.removeEventListener !== 'function') {
          return;
        }
        signal.removeEventListener('abort', onAbort);
      };
      const onAbort = () => {
        if (done) return;
        done = true;
        if (timer !== null) {
          clearTimeout(timer);
        }
        cleanup();
        reject(createAbortError());
      };
      if (signal && signal.aborted) {
        onAbort();
        return;
      }
      if (signal && typeof signal.addEventListener === 'function') {
        signal.addEventListener('abort', onAbort, { once: true });
      }
      timer = setTimeout(() => {
        if (done) return;
        done = true;
        cleanup();
        resolve();
      }, delayMs);
    });
  };

  return {
    async waitForSlot(ctx, fqContext, signal) {
      const maxAttempts = computeMaxAttempts();
      const hostKey = fqContext?.hostname || '';
      let queryToken = null;
      let pendingStreak = 0;
      let overloadStreak = 0;
      let errorStreak = 0;
      const startedAt = Date.now();

      const throwIfAborted = () => {
        if (signal && signal.aborted) {
          throw createAbortError();
        }
      };

      // Both limits apply: exit when either maxAttempts OR totalMaxWaitMs is exceeded.
      // With default maxAttempts=35 and typical in-flight duration ~6s, time limit
      // will normally trigger first. The attempts limit prevents runaway loops when
      // responses are abnormally fast (e.g., immediate "pending" responses).
      for (let attempt = 1; attempt <= maxAttempts && Date.now() - startedAt < totalMaxWaitMs; attempt++) {
        throwIfAborted();
        const requestStart = Date.now();
        const elapsedTotalMs = requestStart - startedAt;
        if (elapsedTotalMs >= totalMaxWaitMs) {
          return { kind: 'timeout', reason: 'slot-handler-timeout' };
        }
        const remainingMs = totalMaxWaitMs - elapsedTotalMs;
        const requestTimeoutMs = Math.min(perRequestTimeoutMs, remainingMs);
        const now = requestStart;

        const throttledRemain = getHostThrottledRemainingSeconds(hostKey, now);
        if (throttledRemain > 0) {
          const cachedState = FQ_GLOBAL_STATE.throttledByHost.get(hostKey);
          console.warn(
            `[FQ] slot-handler throttled (cached), skip acquire host=${hostKey}, retryAfter=${throttledRemain}s`
          );
          return {
            kind: 'throttled',
            throttleCode: cachedState?.code || 503,
            retryAfter: throttledRemain,
          };
        }

        const globalOverloadedRemain = getGlobalOverloadedRemainingSeconds(now);
        if (globalOverloadedRemain > 0) {
          return {
            kind: 'overloaded',
            scope: 'global',
            retryAfter: globalOverloadedRemain,
          };
        }

        const overloadedRemainMs = getScopedOverloadRemainingMs(
          hostKey,
          fqContext?.siteBucket,
          fqContext?.ipBucket,
          now,
        );
        if (overloadedRemainMs > 0) {
          const delayMs = Math.min(overloadedRemainMs, requestTimeoutMs);
          if (Date.now() - startedAt + delayMs >= totalMaxWaitMs) {
            return { kind: 'timeout', reason: 'slot-handler-overloaded' };
          }
          await sleepWithAbort(delayMs, signal);
          continue;
        }

        // New slot-handler protocol: every acquire poll must include full context.
        const payload = {
          hostname: fqContext.hostname,
          hostnameHash: fqContext.hostnameHash,
          ipBucket: fqContext.ipBucket,
          siteBucket: fqContext.siteBucket,
          now,
          throttleTimeWindowSeconds,
          ...(queryToken ? { queryToken } : {}),
        };

        let res;
        try {
          res = await fetchWithTimeout(acquireUrl, payload, requestTimeoutMs, signal);
        } catch (error) {
          if (signal && signal.aborted) {
            throw createAbortError();
          }
          const message = error instanceof Error ? error.message : String(error);
          console.error('[FQ] slot-handler acquire error:', message);
          const delayMs = computeErrorBackoffMs(errorStreak);
          errorStreak += 1;
          if (Date.now() - startedAt + delayMs >= totalMaxWaitMs) {
            return { kind: 'timeout', reason: 'slot-handler-unreachable' };
          }
          await sleepWithAbort(delayMs, signal);
          continue;
        }

        if (!res.ok) {
          if (res.status === 409) {
            // Conflict is transient under contention; back off a bit and retry.
            pendingStreak = 0;
            overloadStreak = 0;
            await sleepWithAbort(150 + Math.floor(Math.random() * 150), signal);
            continue;
          }
          console.error(`[FQ] slot-handler acquire failed: status ${res.status}`);
          return { kind: 'timeout', reason: 'slot-handler-bad-status' };
        }

        let data;
        try {
          data = await res.json();
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          console.error('[FQ] slot-handler response parse error:', message);
          const delayMs = computeErrorBackoffMs(errorStreak);
          errorStreak += 1;
          if (Date.now() - startedAt + delayMs >= totalMaxWaitMs) {
            return { kind: 'timeout', reason: 'slot-handler-invalid-response' };
          }
          await sleepWithAbort(delayMs, signal);
          continue;
        }

        errorStreak = 0;

        if (data && data.queryToken) {
          queryToken = data.queryToken;
          fqContext.queryToken = data.queryToken;
        }

        switch (data?.result) {
          case 'granted':
            pendingStreak = 0;
            overloadStreak = 0;
            fqContext.slotToken = data.slotToken;
            fqContext.slotAcquiredAt = Date.now();
            console.log(`[FQ] slot granted via slot-handler host=${fqContext.hostname}`);
            return { kind: 'granted' };
          case 'throttled':
            pendingStreak = 0;
            overloadStreak = 0;
            const retryAfterRaw =
              Number.isFinite(data?.throttleRetryAfter) && data.throttleRetryAfter > 0
                ? data.throttleRetryAfter
                : (Number.isFinite(data?.retryAfter) && data.retryAfter > 0 ? data.retryAfter : null);
            const retryAfter = retryAfterRaw && retryAfterRaw > 0
              ? retryAfterRaw
              : throttleTimeWindowSeconds;
            const throttleCode = Number.isFinite(data?.throttleCode) ? data.throttleCode : 503;
            markThrottled(hostKey, throttleCode, retryAfter);
            return {
              kind: 'throttled',
              throttleCode,
              retryAfter: retryAfter ?? undefined,
            };
          case 'overloaded': {
            pendingStreak = 0;
            const reason = typeof data?.reason === 'string' ? data.reason : '';
            const isGlobalOverload = reason === 'overload_global';
            if (isGlobalOverload) {
              const retryAfterRaw = Number(data?.retryAfter);
              const retryAfter = Number.isFinite(retryAfterRaw) && retryAfterRaw > 0
                ? Math.ceil(retryAfterRaw)
                : 60;
              markGlobalOverloaded(retryAfter);
              return {
                kind: 'overloaded',
                scope: 'global',
                retryAfter,
              };
            }

            const delayMs = nextOverloadDelayMs(overloadStreak);
            overloadStreak += 1;
            if (reason === 'overload_host') {
              markHostOverloaded(hostKey, delayMs);
            } else if (reason === 'overload_site') {
              markSiteOverloaded(hostKey, fqContext?.siteBucket, delayMs);
            } else if (reason === 'overload_ip') {
              markIpOverloaded(hostKey, fqContext?.siteBucket, fqContext?.ipBucket, delayMs);
            } else {
              // Fallback: unknown/legacy scoped reason still degrades to host-level cooling.
              markHostOverloaded(hostKey, delayMs);
            }
            const elapsed = Date.now() - startedAt;
            if (elapsed + delayMs >= totalMaxWaitMs) {
              return { kind: 'timeout', reason: 'slot-handler-overloaded' };
            }
            await sleepWithAbort(delayMs, signal);
            continue;
          }
          case 'timeout':
            pendingStreak = 0;
            overloadStreak = 0;
            return { kind: 'timeout', reason: 'slot-handler-timeout' };
          case 'pending':
            pendingStreak += 1;
            overloadStreak = 0;
            {
              // Avoid busy-looping when slot-handler responds pending quickly.
              const elapsedMs = Date.now() - requestStart;
              if (elapsedMs < 200) {
                const base = 100;
                const jitter = Math.floor(Math.random() * 100);
                const extra = Math.min(200, pendingStreak * 20);
                await sleepWithAbort(base + jitter + extra, signal);
              }
            }
            continue;
          default: {
            const message = `[FQ] unexpected slot-handler result: ${data?.result}`;
            console.error(message);
            return { kind: 'timeout', reason: 'slot-handler-unexpected' };
          }
        }
      }

      return { kind: 'timeout', reason: 'slot-handler-timeout' };
    },

    async releaseSlot(ctx, fqContext) {
      if (!fqContext.slotToken) {
        return;
      }

      const releaseMaxAttempts = 3;
      const releaseBaseBackoffMs = 100;
      const releaseMaxBackoffMs = 500;

      const payload = {
        hostname: fqContext.hostname,
        hostnameHash: fqContext.hostnameHash,
        ipBucket: fqContext.ipBucket,
        siteBucket: fqContext.siteBucket,
        slotToken: fqContext.slotToken,
        hitUpstreamAtMs: fqContext.hitUpstreamAtMs || fqContext.nowMs,
        now: Date.now(),
      };

      let lastError = null;
      for (let attempt = 1; attempt <= releaseMaxAttempts; attempt += 1) {
        let shouldRetry = false;
        try {
          const res = await fetch(releaseUrl, {
            method: 'POST',
            headers: buildHeaders(),
            body: JSON.stringify(payload),
          });

          if (res.ok) {
            console.log(`[FQ] slot released via slot-handler host=${fqContext.hostname}`);
            return;
          }

          lastError = new Error(`slot-handler release failed: status ${res.status}`);
          shouldRetry = isRetryableReleaseStatus(res.status);
        } catch (error) {
          lastError = error instanceof Error ? error : new Error(String(error));
          shouldRetry = true;
        }

        if (shouldRetry && attempt < releaseMaxAttempts) {
          const backoffMs = Math.min(releaseMaxBackoffMs, releaseBaseBackoffMs * (2 ** (attempt - 1)));
          await new Promise((resolve) => setTimeout(resolve, backoffMs));
          continue;
        }

        break;
      }

      const message = lastError instanceof Error ? lastError.message : String(lastError || 'unknown error');
      console.error('[FQ] releaseSlot error (slot-handler):', message);
    },
  };
};

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
  const originCheckModes = extractControllerOriginModes(downloadDecision, config.binding);

  // Handle block action
  if (actions.includes('block')) {
    return createErrorResponse(origin, 403, "access denied");
  }

  const needOriginCheck = originCheckModes.length > 0;

  const clientIpValue = getClientIp(request);
  const clientIP = clientIpValue || "";
  const ipSubnet = calculateIPSubnet(clientIP, config.ipv4Suffix, config.ipv6Suffix);

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
      // Continue processing if rate limiter check fails.
    }
  }

  if (config.rateLimitEnabled && ipSubnet) {
    const remaining = getRateLimitRemainingSeconds(ipSubnet);
    if (remaining > 0) {
      await slowFailDelay();
      const windowLabel = formatRateLimitWindow(config.windowTime, config.rateLimitConfig?.windowTimeSeconds);
      console.warn(
        '[Rate Limit] Blocked by local cache:',
        ipSubnet,
        `limit=${config.ipSubnetLimit}`,
        `window=${windowLabel}`,
        `retryAfter=${remaining}s`
      );
      return createRateLimitResponse(
        origin,
        ipSubnet,
        config.ipSubnetLimit,
        windowLabel,
        remaining
      );
    }
  }

  let dynamicIdleTimeout = null;

  const payload = url.searchParams.get("payload") ?? "";
  const payloadSign = url.searchParams.get("payloadSign") ?? "";
  if (!payload) {
    return createUnauthorizedResponse(origin, "payload missing");
  }
  if (!payloadSign) {
    return createUnauthorizedResponse(origin, "payloadSign missing");
  }

  const payloadVerifyResult = await verifySignature(config.token, payload, payloadSign);
  if (payloadVerifyResult !== "") {
    return createUnauthorizedResponse(origin, payloadVerifyResult);
  }

  const payloadSignExpire = extractExpireFromSign(payloadSign);
  const decodedPayload = base64UrlDecodeToString(payload);
  if (!decodedPayload) {
    return createUnauthorizedResponse(origin, "payload decode failed");
  }

  let payloadData = null;
  try {
    payloadData = JSON.parse(decodedPayload);
  } catch (_error) {
    return createUnauthorizedResponse(origin, "payload invalid");
  }

  const payloadVersion = Number(payloadData?.v);
  if (!Number.isFinite(payloadVersion) || payloadVersion !== 1) {
    return createUnauthorizedResponse(origin, "payload version invalid");
  }

  const payloadExpireTime = readPayloadExpireTime(payloadData);
  if (!Number.isFinite(payloadExpireTime) || payloadExpireTime <= 0) {
    return createUnauthorizedResponse(origin, "payload expire invalid");
  }

  const nowSeconds = Math.floor(Date.now() / 1000);
  const hardExpire = payloadSignExpire > 0 ? payloadSignExpire : Number.POSITIVE_INFINITY;
  const effectiveExpire = Math.min(hardExpire, payloadExpireTime);
  if (nowSeconds > effectiveExpire) {
    return createUnauthorizedResponse(origin, "link expired");
  }

  if (payloadData && typeof payloadData === "object") {
    const { idle_timeout: idleTimeoutOverride } = payloadData;
    if (typeof idleTimeoutOverride === "number" && Number.isFinite(idleTimeoutOverride) && idleTimeoutOverride >= 0) {
      dynamicIdleTimeout = Math.trunc(idleTimeoutOverride);
      console.log("[IDLE] Using idle_timeout from payload:", dynamicIdleTimeout);
    }
  }

  const encryptedPayload = typeof payloadData.encrypt === "string" ? payloadData.encrypt : "";
  if (!encryptedPayload) {
    return createUnauthorizedResponse(origin, "payload encrypt missing");
  }
  const bindingPayload = await decryptBindingPayload(encryptedPayload, config.token);
  if (!bindingPayload) {
    console.warn('[Binding] Failed to decrypt payload');
    return createUnauthorizedResponse(origin, "payload decrypt failed");
  }

  const issuer = normalizeOrigin(typeof bindingPayload.issuer === "string" ? bindingPayload.issuer : "");
  if (!issuer || !config.landingWorkerAddresses.includes(issuer)) {
    return createUnauthorizedResponse(origin, "prohibited issuer");
  }

  const workerAddress = normalizeOrigin(typeof bindingPayload.workerAddress === "string" ? bindingPayload.workerAddress : "");
  const actualWorkerOrigin = new URL(request.url).origin;
  if (!workerAddress || workerAddress !== actualWorkerOrigin) {
    return createUnauthorizedResponse(origin, "worker address mismatch");
  }

  const bindingStr = typeof payloadData.bindingStr === "string" ? payloadData.bindingStr : "";
  const bindingVer = Number(payloadData.bindingVer);
  if (Number.isFinite(bindingVer) && bindingVer > 0 && bindingVer !== config.binding.version) {
    return createUnauthorizedResponse(origin, "binding version mismatch");
  }

  if (needOriginCheck) {
    if (!bindingStr) {
      return createUnauthorizedResponse(origin, "bindingStr missing");
    }
    const bindingResult = await buildBindingStr({
      modes: originCheckModes,
      path: url.pathname,
      cf: request.cf,
      clientIP: clientIpValue,
      bindingConfig: config.binding,
      token: config.token,
    });
    if (!bindingResult.ok) {
      return createUnauthorizedResponse(origin, bindingResult.reason || "binding unavailable");
    }
    if (bindingResult.bindingStr !== bindingStr) {
      return createUnauthorizedResponse(origin, "origin mismatch");
    }
  }

  const effectiveIdleTimeoutForCache =
    (dynamicIdleTimeout ?? config.cacheConfig?.idleTimeout ?? config.idleTimeout ?? 0);
  const idleCacheEnabled = Number.isFinite(effectiveIdleTimeoutForCache) && effectiveIdleTimeoutForCache > 0;
  const idleCacheKey = idleCacheEnabled ? await buildIdleCacheKey(url) : null;

  if (idleCacheKey && getIdle410Cached(idleCacheKey)) {
    await slowFailDelay();
    return createErrorResponse(origin, 410, 'Link expired due to inactivity');
  }

  // ========================================
  // UNIFIED CHECK (RTT 3→1 OPTIMIZATION)
  // ========================================
  let unifiedResult = null;
  let cacheHit = false;
  let linkData = null;
  let unifiedThrottleHostnameHash = null;
  
  // Use unified check when rate limit is enabled and dbMode is custom-pg-rest
  const supportsUnifiedCheck = config.rateLimitEnabled && config.dbMode === 'custom-pg-rest';
  const rateLimitConfig = config.rateLimitConfig || {};
  const unifiedRateLimit = rateLimitConfig.limit ?? config.ipSubnetLimit;

  const runUnifiedCheck = async (throttleHostnameHash = null) => {
    try {
      const cacheConfig = config.cacheConfig || {};
      const throttleConfig = config.throttleConfig || {};
      const effectiveIdleTimeout =
        dynamicIdleTimeout ?? cacheConfig.idleTimeout ?? config.idleTimeout ?? 0;

      const result = await unifiedCheck(path, clientIP, {
        postgrestUrl: rateLimitConfig.postgrestUrl,
        verifyHeader: rateLimitConfig.verifyHeader,
        verifySecret: rateLimitConfig.verifySecret,
        linkTTL: cacheConfig.linkTTL ?? 1800,
        idleTimeout: effectiveIdleTimeout,
        cacheTableName: cacheConfig.tableName || 'DOWNLOAD_CACHE_TABLE',
        windowTimeSeconds: rateLimitConfig.windowTimeSeconds ?? 86400,
        limit: unifiedRateLimit ?? 100,
        blockTimeSeconds: rateLimitConfig.blockTimeSeconds ?? 600,
        ipv4Suffix: rateLimitConfig.ipv4Suffix ?? '/32',
        ipv6Suffix: rateLimitConfig.ipv6Suffix ?? '/60',
        rateLimitTableName: rateLimitConfig.tableName || 'DOWNLOAD_IP_RATELIMIT_TABLE',
        throttleTimeWindow: throttleConfig.throttleTimeWindow ?? 60,
        throttleTableName: throttleConfig.tableName || 'THROTTLE_PROTECTION',
        lastActiveTableName: cacheConfig.lastActiveTableName || config.lastActiveTableName,
        cacheEnabled: config.cacheEnabled,
        throttleHostnameHash,
      });

      return { result };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error('[Unified Check] Failed:', errorMessage);
      console.error('[Unified Check] Stack:', error instanceof Error ? error.stack : '');

      const FAIL_OPEN = 'fail' + '-open';
      const FAIL_CLOSED = 'fail' + '-closed';
      const pgErrorHandle = config.rateLimitConfig?.pgErrorHandle || FAIL_CLOSED;

      if (pgErrorHandle === FAIL_OPEN) {
        console.warn('[Unified Check] Fail-open mode: allowing request despite error');
        return { result: null };
      }
      console.error('[Unified Check] Fail-closed mode: blocking request');
      return { errorResponse: createErrorResponse(origin, 500, `Unified check failed: ${errorMessage}`) };
    }
  };

  const applyUnifiedResult = async (options = {}) => {
    if (!unifiedResult) {
      return null;
    }

    console.log('[Idle Debug] Unified check idle payload:', unifiedResult.idle ?? null);
    if (!unifiedResult.rateLimit.allowed) {
      if (unifiedResult.rateLimit.error) {
        console.error('[Rate Limit] fail-closed error:', unifiedResult.rateLimit.error);
        return createErrorResponse(origin, 500, unifiedResult.rateLimit.error);
      }

      const ipSubnetForBlock = unifiedResult.rateLimit.ipSubnet || ipSubnet || clientIP;
      const retryAfter = normalizePositiveSeconds(
        unifiedResult.rateLimit.retryAfter,
        config.rateLimitConfig?.windowTimeSeconds || 0
      );

      if (ipSubnetForBlock && retryAfter > 0) {
        markRateLimited(ipSubnetForBlock, retryAfter);
      }

      await slowFailDelay();

      const windowLabel = formatRateLimitWindow(config.windowTime, config.rateLimitConfig?.windowTimeSeconds);
      console.warn(
        '[Rate Limit] Subnet blocked (unified):',
        ipSubnetForBlock,
        `limit=${unifiedRateLimit}`,
        `window=${windowLabel}`,
        `retryAfter=${retryAfter}s`
      );
      return createRateLimitResponse(
        origin,
        ipSubnetForBlock,
        unifiedRateLimit,
        windowLabel,
        retryAfter
      );
    }

    if (unifiedResult.idle && unifiedResult.idle.expired) {
      const idleReason = unifiedResult.idle.reason || 'Link expired due to inactivity';
      const idleDuration = unifiedResult.idle.idleDuration ?? 'unknown';
      const idleTimeout = unifiedResult.idle.timeout ?? 'unknown';
      console.warn(
        `[Idle Timeout] Link expired (idle ${idleDuration}s, timeout ${idleTimeout}s)`
      );
      if (idleCacheKey) {
        putIdle410Cached(idleCacheKey);
      }
      return createErrorResponse(origin, 410, idleReason);
    }

    if (unifiedResult.cache.hit) {
      cacheHit = true;
      linkData = unifiedResult.cache.linkData;
      if (unifiedResult.cache.hostnameHash) {
        unifiedThrottleHostnameHash = unifiedResult.cache.hostnameHash;
      }
    }

    if (config.throttleEnabled && unifiedResult.throttle.status === 'protected') {
      const throttleInfo = unifiedResult.throttle || {};
      const retryAfter = normalizePositiveSeconds(
        throttleInfo.retryAfter,
        config.throttleConfig?.throttleTimeWindow || 60
      );
      const throttleHostnameRaw = extractHostname(unifiedResult?.cache?.linkData?.url || '');
      const throttleHostnameFromCache = throttleHostnameRaw ? throttleHostnameRaw.toLowerCase() : '';
      const throttleHostnameOverride = options.throttleHostname || '';
      const throttleHostname = (throttleHostnameOverride || throttleHostnameFromCache).toLowerCase();

      if (throttleHostname) {
        markThrottled(throttleHostname, throttleInfo.errorCode || 503, retryAfter);
      }

      await slowFailDelay();

      console.log(
        `[Throttle] Protected from unified check, returning error ${throttleInfo.errorCode}, retry after ${retryAfter}s`
      );

      return createThrottleProtectedResponse(origin, {
        ...throttleInfo,
        retryAfter,
      });
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

    return null;
  };

  if (supportsUnifiedCheck && config.cacheEnabled) {
    const { result, errorResponse } = await runUnifiedCheck();
    if (errorResponse) {
      return errorResponse;
    }
    unifiedResult = result;
    const unifiedResponse = await applyUnifiedResult();
    if (unifiedResponse) {
      return unifiedResponse;
    }
  } else if (!supportsUnifiedCheck) {
    // Fallback to original logic when unified check is not supported
    
    if (rateLimiter && config.rateLimitEnabled && clientIP) {
      try {
        const rateLimitResult = await rateLimiter.checkRateLimit(clientIP, { ...config.rateLimitConfig, ctx });
        if (!rateLimitResult.allowed) {
          if (rateLimitResult.error) {
            console.error('[Rate Limit] fail-closed error:', rateLimitResult.error);
            return createErrorResponse(origin, 500, rateLimitResult.error);
          }
          const ipSubnetForBlock = rateLimitResult.ipSubnet || ipSubnet || clientIP;
          const retryAfter = normalizePositiveSeconds(
            rateLimitResult.retryAfter,
            config.rateLimitConfig?.windowTimeSeconds || 0
          );

          if (ipSubnetForBlock && retryAfter > 0) {
            markRateLimited(ipSubnetForBlock, retryAfter);
          }

          await slowFailDelay();

          const windowLabel = formatRateLimitWindow(config.windowTime, config.rateLimitConfig?.windowTimeSeconds);
          console.warn(
            '[Rate Limit] Subnet blocked:',
            ipSubnetForBlock,
            `limit=${config.ipSubnetLimit}`,
            `window=${windowLabel}`,
            `retryAfter=${retryAfter}s`
          );
          return createRateLimitResponse(
            origin,
            ipSubnetForBlock,
            config.ipSubnetLimit,
            windowLabel,
            retryAfter
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
    if (config.alistAuthHeaders && typeof config.alistAuthHeaders === 'object') {
      for (const [headerName, headerValue] of Object.entries(config.alistAuthHeaders)) {
        headers[headerName] = headerValue;
      }
    }
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
  let unifiedThrottleHostname = null;

  if (supportsUnifiedCheck && !config.cacheEnabled && !unifiedResult) {
    const throttleHostnameRaw = extractHostname(downloadUrl);
    unifiedThrottleHostname = throttleHostnameRaw ? throttleHostnameRaw.toLowerCase() : null;
    const throttleHostnameHash = unifiedThrottleHostname
      ? await sha256Hash(unifiedThrottleHostname)
      : null;
    unifiedThrottleHostnameHash = throttleHostnameHash || null;

    const { result, errorResponse } = await runUnifiedCheck(throttleHostnameHash);
    if (errorResponse) {
      return errorResponse;
    }
    unifiedResult = result;
    const unifiedResponse = await applyUnifiedResult({ throttleHostname: unifiedThrottleHostname });
    if (unifiedResponse) {
      return unifiedResponse;
    }
  }
  // ========================================
  // Throttle protection logic (pre-check)
  // ========================================
  let throttleStatus = null;
  let throttleHostname = null;

  const throttleCheckEnabled = config.throttleEnabled && throttleManager;
  if (throttleCheckEnabled) {
    const throttleHostnameRaw = extractHostname(downloadUrl);
    throttleHostname = throttleHostnameRaw ? throttleHostnameRaw.toLowerCase() : null;

    const unifiedThrottleUsable = Boolean(
      unifiedResult && unifiedResult.throttle && unifiedThrottleHostnameHash
    );

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
        const retryAfter = normalizePositiveSeconds(
          throttleStatus.retryAfter,
          config.throttleConfig?.throttleTimeWindow || 60
        );
        if (throttleHostname) {
          markThrottled(throttleHostname, throttleStatus.errorCode || 503, retryAfter);
        }
        await slowFailDelay();
        console.log(
          `[Throttle] Protected: ${throttleHostname}, returning error ${throttleStatus.errorCode}, retry after ${retryAfter}s`
        );
        return createThrottleProtectedResponse(origin, {
          ...throttleStatus,
          retryAfter,
        });
      } else if (throttleStatus.status === 'resume_operation') {
        console.log(`[Throttle] Resume operation: ${throttleHostname}`);
      }
    }
  }

  // ========================================
  // Fair Upstream Queue Integration
  // ========================================
  const upstreamHostnameRaw = extractHostname(downloadUrl);
  const upstreamHostname = upstreamHostnameRaw ? upstreamHostnameRaw.toLowerCase() : null;
  const needFairQueue =
    config.fairQueueEnabled &&
    upstreamHostname &&
    config.fairQueueHostnamePatterns.some((pattern) => matchHostnamePattern(upstreamHostname, pattern));

  let fairQueueClient = null;
  let fqContext = null;
  let earlyResponse = null;

  if (needFairQueue) {
    if (!config.slotHandlerConfig?.url) {
      console.error('[Fair Queue] enabled but slot-handler URL missing');
      return createErrorResponse(origin, 503, 'Fair queue misconfigured (slot-handler URL missing)');
    }

    const clientIpSubnet = calculateIPSubnet(clientIP, config.ipv4Suffix, config.ipv6Suffix);

    if (!clientIpSubnet) {
      console.error('[Fair Queue] Failed: unable to derive client subnet for queue enforcement');
      return createErrorResponse(origin, 503, 'Fair queue unavailable');
    }

    {
      const clientIpSubnetHash = await sha256Hash(clientIpSubnet);
      const hostnameHash = await sha256Hash(upstreamHostname);
      try {
        fairQueueClient = createFairQueueClient(config);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.error('[Fair Queue] Failed to initialize client:', message);
        return createErrorResponse(origin, 503, 'Fair queue unavailable');
      }
      const siteBucket = await deriveSiteBucket(upstreamHostname, downloadUrl, config.fairQueueSiteBucket);
      fqContext = {
        hostname: upstreamHostname,
        hostnameHash,
        ipBucket: clientIpSubnetHash,
        siteBucket,
        nowMs: Date.now(),
      };

      try {
        const fqResult = await fairQueueClient.waitForSlot(ctx, fqContext, clientSignal);
        if (fqResult.kind === 'throttled') {
          const retryAfter = normalizePositiveSeconds(
            fqResult.retryAfter,
            config.throttleConfig?.throttleTimeWindow || 60
          );
          if (upstreamHostname) {
            markThrottled(upstreamHostname, fqResult.throttleCode || 503, retryAfter);
          }
          await slowFailDelay();
          return createThrottleProtectedResponse(origin, {
            status: 'protected',
            errorCode: fqResult.throttleCode || 503,
            retryAfter,
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

        if (fqResult.kind === 'overloaded' && fqResult.scope === 'global') {
          return createFairQueueOverloadedResponse(origin, fqResult.retryAfter);
        }
      } catch (error) {
        if (clientAborted && isAbortError(error)) {
          earlyResponse = createClientAbortResponse(origin);
        } else {
          const message = error instanceof Error ? error.message : String(error);
          console.error('[Fair Queue] waitForSlot error:', message);
          return createErrorResponse(origin, 503, 'Fair queue unavailable');
        }
      }
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
          return await handleRequest(request, env, config, cacheManager, throttleManager, rateLimiter, ctx);
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
        if (fairQueueClient && fqContext) {
          const updatedHostnameRaw = extractHostname(downloadUrl);
          const updatedHostname = updatedHostnameRaw ? updatedHostnameRaw.toLowerCase() : null;
          const shouldUseFairQueue =
            config.fairQueueEnabled &&
            updatedHostname &&
            config.fairQueueHostnamePatterns.some((pattern) => matchHostnamePattern(updatedHostname, pattern));

          if (!shouldUseFairQueue) {
            if (fqContext.slotToken) {
              try {
                await fairQueueClient.releaseSlot(ctx, fqContext);
              } catch (error) {
                const message = error instanceof Error ? error.message : String(error);
                console.warn('[Fair Queue] releaseSlot failed during refresh:', message);
              }
            }
            fqContext = null;
          } else {
            const updatedSiteBucket = await deriveSiteBucket(updatedHostname, downloadUrl, config.fairQueueSiteBucket);
            if (updatedHostname !== fqContext.hostname || updatedSiteBucket !== fqContext.siteBucket) {
              if (fqContext.slotToken) {
                try {
                  await fairQueueClient.releaseSlot(ctx, fqContext);
                } catch (error) {
                  const message = error instanceof Error ? error.message : String(error);
                  console.warn('[Fair Queue] releaseSlot failed during refresh:', message);
                }
              }
              const updatedHostnameHash = await sha256Hash(updatedHostname);
              fqContext = {
                hostname: updatedHostname,
                hostnameHash: updatedHostnameHash,
                ipBucket: fqContext.ipBucket,
                siteBucket: updatedSiteBucket,
                nowMs: Date.now(),
              };

              const fqResult = await fairQueueClient.waitForSlot(ctx, fqContext, clientSignal);
              if (fqResult.kind === 'throttled') {
                const retryAfter = normalizePositiveSeconds(
                  fqResult.retryAfter,
                  config.throttleConfig?.throttleTimeWindow || 60
                );
                if (updatedHostname) {
                  markThrottled(updatedHostname, fqResult.throttleCode || 503, retryAfter);
                }
                await slowFailDelay();
                return createThrottleProtectedResponse(origin, {
                  status: 'protected',
                  errorCode: fqResult.throttleCode || 503,
                  retryAfter,
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

              if (fqResult.kind === 'overloaded' && fqResult.scope === 'global') {
                return createFairQueueOverloadedResponse(origin, fqResult.retryAfter);
              }
            }
          }
        }
        request = buildUpstreamRequest(downloadUrl, res.data.header);
        response = await fetch(request);
        while (response.status >= 300 && response.status < 400) {
          const location = response.headers.get("Location");
          if (location) {
            const currentOrigin = new URL(originalRequest.url).origin;
            if (location.startsWith(`${currentOrigin}/`)) {
              request = new Request(location, request);
              return await handleRequest(request, env, config, cacheManager, throttleManager, rateLimiter, ctx);
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
    const isCryptedDownload = payloadData?.isCrypted === true;

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

    const hasRangeRequest = Boolean(request.headers.get('range'));
    const hasContentRange = Boolean(response.headers.get('content-range'));
    const shouldOverrideCacheControl = config.overrideCacheControl
      && (
        response.status === 200
        || (response.status === 206 && hasRangeRequest && hasContentRange)
      );

    if (shouldOverrideCacheControl) {
      const fileSize = readPayloadFileSize(payloadData);
      if (typeof fileSize === 'number' && fileSize <= config.cacheOverrideMaxSizeBytes) {
        const maxAge = config.cacheOverrideSeconds;
        safeHeaders.set('cache-control', `public, max-age=${maxAge}, s-maxage=${maxAge}`);
        safeHeaders.delete('x-cache');
      }
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

export const __fairQueueTestHooks = {
  createSlotHandlerClient,
  resolveConfig,
  markHostOverloaded,
  getHostOverloadedRemainingMs,
  getGlobalOverloadedRemainingSeconds,
  markSiteOverloaded,
  markIpOverloaded,
  getSiteOverloadedRemainingMs,
  getIpOverloadedRemainingMs,
  getOverloadedMapSizes: () => ({
    host: FQ_GLOBAL_STATE.overloadedByHost.size,
    site: FQ_GLOBAL_STATE.overloadedBySite.size,
    ip: FQ_GLOBAL_STATE.overloadedByIp.size,
  }),
  clearOverloadedByHost: () => {
    FQ_GLOBAL_STATE.overloadedByHost.clear();
    FQ_GLOBAL_STATE.overloadedBySite.clear();
    FQ_GLOBAL_STATE.overloadedByIp.clear();
    FQ_GLOBAL_STATE.overloadedGlobalUntilMs = 0;
  },
};

// src/index.ts
export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname || '/';

      const isInternalPath = pathname.startsWith('/api/v0/');
      if (isInternalPath) {
        const internalResponse = await handleInternalApiIfAny(request, env, ctx);
        if (internalResponse) {
          return internalResponse;
        }
      }

      const innerAuthSecret = typeof env?.INNER_AUTH_SECRET === 'string' ? env.INNER_AUTH_SECRET.trim() : '';
      if (innerAuthSecret) {
        const headerNameRaw = typeof env?.INNER_AUTH_HEADER === 'string' ? env.INNER_AUTH_HEADER.trim() : '';
        const headerName = headerNameRaw || 'X-Inner-Auth';
        const provided = request.headers.get(headerName) || '';
        if (provided !== innerAuthSecret) {
          return new Response('Forbidden', { status: 403 });
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

      const requestOrigin = url.origin;
      if (!config.workerAddresses.includes(requestOrigin)) {
        const origin = request.headers.get('origin') || '*';
        return createErrorResponse(origin, 403, 'prohibited source');
      }

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
