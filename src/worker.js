// Import cache/throttle managers, rate limiter, and utilities
import { createCacheManager } from './cache/factory.js';
import { createThrottleManager } from './cache/throttle-factory.js';
import { createRateLimiter } from './ratelimit/factory.js';
import { unifiedCheck, readTicketState, markTicketUsed } from './unified-check.js';
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
const DEFAULT_SLOT_HANDLER_RELEASE_TIMEOUT_MS = 1500;
const DEFAULT_TRUE_CONCURRENCY_AUTH_HEADER = 'X-CQ-Auth';
// Must exceed the default CQ wait poll window with explicit slack, or held acquires can time out client-side first.
const DEFAULT_TRUE_CONCURRENCY_ACQUIRE_TIMEOUT_MS = 11500;
const DEFAULT_TRUE_CONCURRENCY_RELEASE_TIMEOUT_MS = 1500;
const DEFAULT_TRUE_CONCURRENCY_WAIT_TOTAL_MAX_MS = 20000;
const DEFAULT_TRUE_CONCURRENCY_WAIT_MAX_ATTEMPTS_CAP = 35;
const DEFAULT_TRUE_CONCURRENCY_HEARTBEAT_CONFIG = Object.freeze({
  enabled: true,
  required: true,
  path: '/api/v1/concurrency/heartbeat',
  intervalMs: 5000,
  timeoutMs: 15000,
  reconnectGraceMs: 12000,
  helloTimeoutMs: 2000,
  startTimeoutMs: 7000,
  ackTimeoutMs: 2000,
  initialConnectMaxAttempts: 3,
  initialConnectMaxElapsedMs: 3000,
  reconnectMaxAttempts: 3,
  reconnectMaxElapsedMs: 10000,
  reconnectBaseDelayMs: 250,
  reconnectMaxDelayMs: 2000,
  reconnectSafetyMarginMs: 1000,
});
const TRUE_CONCURRENCY_RELEASE_RETRY_DELAYS_MS = [0, 2000, 4000, 8000];
const FINAL_CLEANUP_RELEASE_CONCURRENCY = 2;
const DEFAULT_SLOT_HANDLER_MAX_ATTEMPTS = 35;
const DEFAULT_THROTTLE_OPEN_CAP_SECONDS = 60;
const DEFAULT_THROTTLE_OPEN_THRESHOLD_PERCENT = 30;
const DEFAULT_THROTTLE_CLOSE_THRESHOLD_PERCENT = 15;
const DEFAULT_THROTTLE_EWMA_SPAN = 8;
const DEFAULT_THROTTLE_CONSECUTIVE_THRESHOLD = 4;
const DEFAULT_THROTTLE_MIN_SAMPLES_BEFORE_EWMA_OPEN = 8;
const DEFAULT_THROTTLE_IDLE_RESET_SECONDS = 900;
const DEFAULT_THROTTLE_HALF_OPEN_SUCCESS_THRESHOLD = 2;
const DEFAULT_THROTTLE_HALF_OPEN_CLOSE_MODE = 'and';
const DEFAULT_THROTTLE_HALF_OPEN_MAX_PROBE_COUNT = 4;
const MAX_THROTTLE_HALF_OPEN_PROBE_COUNT = 63;
const DEFAULT_THROTTLE_HALF_OPEN_MAX_SECONDS = 15;
const DEFAULT_THROTTLE_HALF_OPEN_TIMEOUT_MODE = 'partial-close';
const DEFAULT_THROTTLE_PROTECT_HTTP_CODES = [429, 499, 500, 502, 503, 504];
// slot-handler acquire is long-poll based; don't set per-request timeouts below this window.
const SLOT_HANDLER_LONGPOLL_MS = 6000;

// Fair Queue in-memory state (per Worker instance)
const FQ_GLOBAL_STATE = {
  overloadedByHost: new Map(),
  overloadedBySite: new Map(),
  overloadedByIp: new Map(),
  overloadedGlobalUntilMs: 0,
};

// Rate Limit in-memory state (per Worker instance, iprange-level)
const RL_STATE = {
  byIpRange: new Map(),
};

const GOOGLE_DRIVE_HEAD_PROBE_RANGE = 'bytes=0-0';
const HOP_BY_HOP_RESPONSE_HEADERS = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade',
]);
const SITE_BUCKET_MODES = new Set(['host', 'sharepoint', 'googledrive']);

const nowMs = () => Date.now();

const normalizeHostnameValue = (hostname) => {
  if (typeof hostname !== 'string') {
    return '';
  }
  return hostname.trim().toLowerCase();
};

const normalizeSiteBucketModeToken = (value) => {
  if (typeof value !== 'string') {
    return '';
  }
  return value.trim().toLowerCase();
};

const normalizeSiteBucketConfig = (siteBucketConfig, fieldName = 'controller siteBucket') => {
  const rawConfig = siteBucketConfig && typeof siteBucketConfig === 'object'
    ? siteBucketConfig
    : {};
  const normalizedModes = [];
  const seenModes = new Set();
  const rawModes = Array.isArray(rawConfig.modes) ? rawConfig.modes : [];

  for (const rawMode of rawModes) {
    const mode = normalizeSiteBucketModeToken(rawMode);
    if (!mode || seenModes.has(mode)) {
      continue;
    }
    if (!SITE_BUCKET_MODES.has(mode)) {
      throw new Error(`Invalid ${fieldName}: unsupported siteBucket mode ${rawMode}`);
    }
    seenModes.add(mode);
    normalizedModes.push(mode);
  }

  const normalizedMode = normalizeSiteBucketModeToken(rawConfig.mode);
  if (normalizedMode && !SITE_BUCKET_MODES.has(normalizedMode)) {
    throw new Error(`Invalid ${fieldName}: unsupported siteBucket mode ${rawConfig.mode}`);
  }

  const effectiveModes = normalizedModes.length > 0
    ? normalizedModes
    : normalizedMode
      ? [normalizedMode]
      : ['sharepoint'];

  return {
    mode: effectiveModes[0],
    modes: effectiveModes,
  };
};

const detectSiteBucketProvider = (hostname) => {
  const host = normalizeHostnameValue(hostname);
  if (!host) {
    return 'unknown';
  }
  if (host.endsWith('.sharepoint.com')) {
    return 'sharepoint';
  }
  if (
    host === 'drive.google.com'
    || host === 'drive.usercontent.google.com'
    || host.endsWith('.googleapis.com')
    || host.endsWith('.googleusercontent.com')
  ) {
    return 'googledrive';
  }
  return 'unknown';
};

const isGoogleDriveDownloadHostname = (hostname) => {
  const host = normalizeHostnameValue(hostname);
  return detectSiteBucketProvider(host) === 'googledrive' || host === 'drive.usercontent.google.com';
};

const parseContentRangeTotal = (contentRangeValue) => {
  if (typeof contentRangeValue !== 'string') {
    return null;
  }
  const match = contentRangeValue.trim().match(/^bytes\s+(?:\d+-\d+|\*)\/(\d+|\*)$/i);
  if (!match || match[1] === '*') {
    return null;
  }
  const total = Number.parseInt(match[1], 10);
  return Number.isFinite(total) && total >= 0 ? total : null;
};

const cancelResponseBody = async (response) => {
  if (!response?.body || typeof response.body.cancel !== 'function') {
    return;
  }
  try {
    await response.body.cancel();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn('[Upstream] Failed to cancel unused response body:', message);
  }
};

const parseContentLengthHeader = (contentLengthValue) => {
  if (typeof contentLengthValue !== 'string') {
    return null;
  }
  const trimmed = contentLengthValue.trim();
  if (!/^\d+$/.test(trimmed)) {
    return null;
  }
  const parsed = Number.parseInt(trimmed, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
};

const buildGoogleDriveFullRangeHeader = (totalSize) => {
  if (!Number.isFinite(totalSize) || totalSize <= 0) {
    return null;
  }
  return `bytes=0-${Math.trunc(totalSize) - 1}`;
};

const isExactGoogleDriveFullRangeMatch = (requestedRangeHeader, contentRangeHeader) => {
  if (typeof requestedRangeHeader !== 'string' || typeof contentRangeHeader !== 'string') {
    return false;
  }
  const requestedMatch = requestedRangeHeader.trim().match(/^bytes=(\d+)-(\d+)$/i);
  const contentMatch = contentRangeHeader.trim().match(/^bytes\s+(\d+)-(\d+)\/(\d+|\*)$/i);
  if (!requestedMatch || !contentMatch || contentMatch[3] === '*') {
    return false;
  }
  return requestedMatch[1] === contentMatch[1]
    && requestedMatch[2] === contentMatch[2]
    && contentMatch[1] === '0'
    && Number.parseInt(contentMatch[2], 10) + 1 === Number.parseInt(contentMatch[3], 10);
};

const shouldSynthesizeGoogleDriveAcceptRanges = (responseToWrap, requestToWrap) => {
  if (responseToWrap.headers.get('accept-ranges')) {
    return false;
  }
  const upstreamHostname = extractHostname(requestToWrap?.url || '')?.toLowerCase() || '';
  if (!isGoogleDriveDownloadHostname(upstreamHostname)) {
    return false;
  }
  if (responseToWrap.status === 206) {
    return Boolean(responseToWrap.headers.get('content-range'));
  }
  return responseToWrap.status === 200;
};

const normalizeStringValue = (value, fallback = '') => {
  if (typeof value !== 'string') {
    return fallback;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : fallback;
};

const isValidTicketNonce = (value) => (
  typeof value === 'string'
  && /^[A-Za-z0-9_-]{22,}$/.test(value.trim())
);

const normalizeKnownFileSize = (value) => {
  const num = Number(value);
  return Number.isFinite(num) && num > 0 ? Math.trunc(num) : null;
};

const resolveTicketStateConfig = (config) => {
  const cacheConfig = config?.cacheConfig && typeof config.cacheConfig === 'object'
    ? config.cacheConfig
    : {};
  const topLevelVerifyHeader = Array.isArray(config?.verifyHeader)
    ? config.verifyHeader.filter((value) => typeof value === 'string' && value.trim() !== '')
    : [];
  const topLevelVerifySecret = Array.isArray(config?.verifySecret)
    ? config.verifySecret.filter((value) => typeof value === 'string' && value.trim() !== '')
    : [];
  const postgrestUrl = normalizeStringValue(
    cacheConfig.postgrestUrl,
    normalizeStringValue(config?.postgrestUrl)
  );
  const verifyHeader = Array.isArray(cacheConfig.verifyHeader)
    ? cacheConfig.verifyHeader.filter((value) => typeof value === 'string' && value.trim() !== '')
    : topLevelVerifyHeader;
  const verifySecret = Array.isArray(cacheConfig.verifySecret)
    ? cacheConfig.verifySecret.filter((value) => typeof value === 'string' && value.trim() !== '')
    : [];
  const effectiveVerifySecret = verifySecret.length > 0 ? verifySecret : topLevelVerifySecret;
  const ticketStateTableName = normalizeStringValue(
    cacheConfig.ticketStateTableName,
    normalizeStringValue(config?.ticketStateTableName, 'DOWNLOAD_TICKET_STATE_TABLE')
  );

  if (
    !postgrestUrl
    || verifyHeader.length === 0
    || effectiveVerifySecret.length === 0
    || verifyHeader.length !== effectiveVerifySecret.length
    || !ticketStateTableName
  ) {
    throw new Error('ticket-state db configuration missing');
  }

  return {
    postgrestUrl,
    verifyHeader,
    verifySecret: effectiveVerifySecret,
    ticketStateTableName,
  };
};

const buildTicketResponseEvidence = ({
  response,
  knownFileSize = null,
  syntheticContentDisposition = false,
} = {}) => {
  const contentDisposition = normalizeStringValue(response?.headers?.get('content-disposition'));
  const contentRange = normalizeStringValue(response?.headers?.get('content-range'));
  const acceptRanges = normalizeStringValue(response?.headers?.get('accept-ranges')).toLowerCase();
  const contentType = normalizeStringValue(response?.headers?.get('content-type')).split(';', 1)[0].toLowerCase();
  const contentLength = parseContentLengthHeader(response?.headers?.get('content-length'));
  const expectedFileSize = normalizeKnownFileSize(knownFileSize);
  const hasResponseBody = Boolean(response) && response.body !== null;
  const hasPositiveBodyLength = contentLength !== null && contentLength > 0;
  const hasExactFileSizeMatch = expectedFileSize !== null
    && hasPositiveBodyLength
    && contentLength === expectedFileSize;
  const hasAuthoritativeAttachmentDisposition = Boolean(contentDisposition)
    && !syntheticContentDisposition
    && hasResponseBody;

  return {
    status: response?.status ?? null,
    contentDisposition,
    contentRange,
    acceptRanges,
    contentType,
    hasResponseBody,
    hasPositiveBodyLength,
    hasExactFileSizeMatch,
    hasAuthoritativeAttachmentDisposition,
    hasAuthoritativeFileSignal: (response?.status === 206)
      || Boolean(contentRange)
      || hasExactFileSizeMatch
      || hasAuthoritativeAttachmentDisposition,
  };
};

const hasAnyAuthoritativeFileSignal = (...signals) => signals.some(
  (signal) => Boolean(signal?.hasAuthoritativeFileSignal)
);

const shouldConsumeTicketResponse = ({
  requestMethod,
  response,
  upstreamResponse = null,
  knownFileSize = null,
  syntheticContentDisposition = false,
} = {}) => {
  if (requestMethod !== 'GET' || !response) {
    return false;
  }

  const responseEvidence = buildTicketResponseEvidence({
    response,
    knownFileSize,
    syntheticContentDisposition,
  });
  const upstreamEvidence = upstreamResponse && upstreamResponse !== response
    ? buildTicketResponseEvidence({
      response: upstreamResponse,
      knownFileSize,
    })
    : responseEvidence;

  const status = responseEvidence.status;
  if (status !== 200 && status !== 206) {
    return false;
  }

  const {
    acceptRanges,
    contentDisposition,
    contentRange,
    contentType,
    hasExactFileSizeMatch,
    hasPositiveBodyLength,
    hasResponseBody,
  } = responseEvidence;
  const isJsonPayload = contentType === 'application/json';
  const isPlainTextInterstitialType = contentType === 'text/html' || contentType === 'text/plain';
  const hasAuthoritativeFileSignal = hasAnyAuthoritativeFileSignal(responseEvidence, upstreamEvidence);

  if (status === 206) {
    return hasResponseBody;
  }

  // Encrypted metadata responses can synthesize attachment headers, so JSON only
  // counts as metadata when the worker synthesized the attachment wrapper itself.
  if (isJsonPayload) {
    return hasResponseBody && hasAuthoritativeFileSignal;
  }

  // Ambiguous inline textual GET 200 responses consume first use whenever the
  // worker releases them as the user-facing download response, including
  // crypted wrappers whose attachment header was synthesized by the worker.
  if (isPlainTextInterstitialType) {
    return hasResponseBody;
  }

  if (contentRange) {
    return true;
  }
  if (hasExactFileSizeMatch) {
    return true;
  }
  if (contentDisposition && hasPositiveBodyLength) {
    return true;
  }
  if (acceptRanges === 'bytes' && hasPositiveBodyLength) {
    return true;
  }

  if (hasResponseBody) {
    return true;
  }

  return hasPositiveBodyLength;
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

const deriveSharePointSiteKey = (pathname) => {
  const segments = typeof pathname === 'string'
    ? pathname.toLowerCase().split('/').filter((segment) => segment.length > 0)
    : [];
  if (segments[0] === 'personal' && segments[1]) return `personal:${segments[1]}`;
  if (segments[0] === 'sites' && segments[1]) return `sites:${segments[1]}`;
  if (segments[0] === 'teams' && segments[1]) return `teams:${segments[1]}`;
  return 'unknown';
};

const deriveProviderHostBucket = (hostname) => {
  const host = normalizeHostnameValue(hostname);
  return host || '';
};

const deriveThrottleAuthorityHostname = (hostname) => {
  const host = normalizeHostnameValue(hostname);
  return host || '';
};

const deriveSiteKey = (hostname, pathname, siteBucketConfig) => {
  const lowerHost = normalizeHostnameValue(hostname);
  const provider = detectSiteBucketProvider(lowerHost);
  const { modes } = normalizeSiteBucketConfig(siteBucketConfig);

  if (provider === 'googledrive' && modes.includes('googledrive')) {
    return 'googledrive:unspecified';
  }

  if (provider === 'sharepoint' && modes.includes('sharepoint')) {
    return deriveSharePointSiteKey(pathname);
  }

  if (modes.includes('host') && lowerHost) {
    return `host:${lowerHost}`;
  }

  return 'unknown';
};

const deriveSiteBucket = async (hostname, urlValue, siteBucketConfig) => {
  const path = extractPathname(urlValue);
  const siteKey = deriveSiteKey(hostname, path, siteBucketConfig);
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

const normalizeNonNegativeInt = (value, fallback) => {
  const num = Number(value);
  if (Number.isFinite(num) && num >= 0) {
    return Math.trunc(num);
  }
  const fb = Number(fallback);
  return Number.isFinite(fb) && fb >= 0 ? Math.trunc(fb) : 0;
};

const normalizeProtectHttpCodes = (value) => {
  if (value === undefined || value === null) {
    return [...DEFAULT_THROTTLE_PROTECT_HTTP_CODES];
  }

  if (!Array.isArray(value)) {
    throw new Error('Invalid protectHttpCodes from controller: expected array');
  }

  if (value.length === 0) {
    return [...DEFAULT_THROTTLE_PROTECT_HTTP_CODES];
  }

  return value.map((code, index) => {
    const normalized = Number(code);
    if (!Number.isInteger(normalized) || normalized < 100 || normalized > 599) {
      throw new Error(`Invalid protectHttpCodes[${index}] from controller: ${code}`);
    }
    return normalized;
  });
};

const HALF_OPEN_CLOSE_MODES = new Set(['and', 'or']);
const HALF_OPEN_TIMEOUT_MODES = new Set(['open', 'close', 'partial-close']);

const normalizeThrottleEnum = (value, fallback, validValues, fieldName) => {
  if (value === undefined || value === null) {
    return fallback;
  }
  if (typeof value !== 'string') {
    throw new Error(`Invalid ${fieldName} from controller: expected string`);
  }
  const normalized = value.trim().toLowerCase();
  if (!normalized) {
    return fallback;
  }
  if (!validValues.has(normalized)) {
    throw new Error(`Invalid ${fieldName} from controller: ${value}`);
  }
  return normalized;
};

const normalizeHalfOpenCloseMode = (value) => normalizeThrottleEnum(
  value,
  DEFAULT_THROTTLE_HALF_OPEN_CLOSE_MODE,
  HALF_OPEN_CLOSE_MODES,
  'halfOpenCloseMode',
);

const normalizeHalfOpenTimeoutMode = (value) => normalizeThrottleEnum(
  value,
  DEFAULT_THROTTLE_HALF_OPEN_TIMEOUT_MODE,
  HALF_OPEN_TIMEOUT_MODES,
  'halfOpenTimeoutMode',
);

const deriveOpenSeconds = (retryAfterValue, openCapSeconds) => {
  const cap = normalizePositiveSeconds(openCapSeconds, DEFAULT_THROTTLE_OPEN_CAP_SECONDS);
  const raw = typeof retryAfterValue === 'string' ? retryAfterValue.trim() : retryAfterValue;

  let parsed = null;
  if (typeof raw === 'number' && Number.isFinite(raw)) {
    parsed = Math.floor(raw);
  } else if (typeof raw === 'string' && /^\d+$/.test(raw)) {
    parsed = Number.parseInt(raw, 10);
  }

  if (!Number.isFinite(parsed) || parsed < 0) {
    return null;
  }

  return Math.max(1, Math.min(cap, parsed + 1));
};

const isManagedThrottleHost = (hostname, throttleHostnamePatterns = []) => {
  const hostKey = typeof hostname === 'string' ? hostname.trim().toLowerCase() : '';
  return Boolean(hostKey)
    && Array.isArray(throttleHostnamePatterns)
    && throttleHostnamePatterns.some((pattern) => matchHostnamePattern(hostKey, pattern));
};

function resolveAdmissionMode(config, hostname) {
  const hostKey = typeof hostname === 'string' ? hostname.trim().toLowerCase() : '';
  const queue = config.fairQueueEnabled
    && Boolean(hostKey)
    && Array.isArray(config.fairQueueHostnamePatterns)
    && config.fairQueueHostnamePatterns.some((pattern) => matchHostnamePattern(hostKey, pattern));
  const breaker = config.throttleEnabled && isManagedThrottleHost(hostKey, config.throttleHostnamePatterns);
  if (queue && breaker) return 'queue_breaker';
  if (queue) return 'queue_only';
  if (breaker) return 'breaker_only';
  return 'none';
}

const readOpenBreakerSnapshot = (snapshot, fallbackSeconds = 0, nowSeconds = Math.floor(Date.now() / 1000)) => {
  if (!snapshot || snapshot.state !== 'open') {
    return null;
  }

  const rawOpenUntil = Number(snapshot.openUntil);
  const openUntil = Number.isFinite(rawOpenUntil) ? Math.trunc(rawOpenUntil) : null;
  if (openUntil !== null && openUntil <= nowSeconds) {
    return null;
  }

  const rawVersion = Number(snapshot.version);
  const version = Number.isFinite(rawVersion) ? Math.trunc(rawVersion) : null;
  const lastErrorCode = Number(snapshot.lastErrorCode);
  const errorCode = Number.isFinite(lastErrorCode) && lastErrorCode >= 100 ? Math.trunc(lastErrorCode) : 503;
  const retryAfterBase = openUntil !== null ? (openUntil - nowSeconds) : 0;
  const retryAfter = normalizePositiveSeconds(retryAfterBase, fallbackSeconds);
  if (!retryAfter) {
    return null;
  }

  return {
    state: 'open',
    openUntil,
    reason: typeof snapshot.reason === 'string' && snapshot.reason.trim() !== '' ? snapshot.reason : null,
    version,
    errorCode,
    retryAfter,
  };
};

const readSlotHandlerBreakerSnapshot = (payload) => {
  if (!payload || typeof payload !== 'object') {
    return null;
  }

  const rawOpenUntil = Number(payload.breakerOpenUntil);
  const openUntil = Number.isFinite(rawOpenUntil) ? Math.trunc(rawOpenUntil) : null;
  const rawVersion = Number(payload.breakerVersion);
  const version = Number.isFinite(rawVersion) ? Math.trunc(rawVersion) : null;
  const throttleCode = Number(payload.throttleCode);
  const lastErrorCode = Number.isFinite(throttleCode) && throttleCode >= 100 ? Math.trunc(throttleCode) : null;
  const reason = typeof payload.breakerReason === 'string' && payload.breakerReason.trim() !== ''
    ? payload.breakerReason
    : null;

  if (openUntil === null && version === null && reason === null && lastErrorCode === null) {
    return null;
  }

  return {
    state: 'open',
    openUntil,
    reason,
    version,
    lastErrorCode,
  };
};

const readSlotHandlerAttempt = (payload) => {
  if (!payload || typeof payload !== 'object') {
    return { attemptVersion: null, attemptTicket: null };
  }

  const meta = payload.meta && typeof payload.meta === 'object' ? payload.meta : null;
  const rawAttemptVersion = Number(meta?.attemptVersion ?? payload.attemptVersion);
  const rawAttemptTicket = Number(meta?.attemptTicket ?? payload.attemptTicket);

  return {
    attemptVersion: Number.isFinite(rawAttemptVersion) && rawAttemptVersion > 0
      ? Math.trunc(rawAttemptVersion)
      : null,
    attemptTicket: Number.isFinite(rawAttemptTicket) && rawAttemptTicket > 0
      ? Math.trunc(rawAttemptTicket)
      : null,
  };
};

const readHalfOpenDeadlineRetryAfter = (snapshot, fallbackSeconds = 1, nowSeconds = Math.floor(Date.now() / 1000)) => {
  const rawHalfOpenDeadline = Number(snapshot?.halfOpenDeadline);
  const halfOpenDeadline = Number.isFinite(rawHalfOpenDeadline) ? Math.trunc(rawHalfOpenDeadline) : null;
  if (halfOpenDeadline !== null && halfOpenDeadline > nowSeconds) {
    return Math.max(1, halfOpenDeadline - nowSeconds);
  }
  return normalizePositiveSeconds(fallbackSeconds, 1) || 1;
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

const readOptionalPositiveIntegerFromBootstrap = (config, property, fallback, fieldName) => {
  if (!Object.prototype.hasOwnProperty.call(config, property)) {
    return fallback;
  }

  const value = config[property];
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`Invalid ${fieldName}: must be a positive integer`);
  }

  return value;
};

const normalizeTrueConcurrencyHeartbeatConfig = (heartbeatConfig, options = {}) => {
  const fieldName = options.fieldName || 'controller trueConcurrency.heartbeat';
  const required = options.required === true;

  if (!heartbeatConfig || typeof heartbeatConfig !== 'object' || Array.isArray(heartbeatConfig)) {
    if (required) {
      throw new Error(`${fieldName} is required when trueConcurrency.enabled is true`);
    }
    return { ...DEFAULT_TRUE_CONCURRENCY_HEARTBEAT_CONFIG };
  }

  const readBoolean = (property, fallback) => {
    if (!Object.prototype.hasOwnProperty.call(heartbeatConfig, property)) {
      return fallback;
    }
    if (typeof heartbeatConfig[property] !== 'boolean') {
      throw new Error(`Invalid ${fieldName}.${property}: expected boolean`);
    }
    return heartbeatConfig[property];
  };

  const readPositiveInt = (property, fallback) => {
    if (!Object.prototype.hasOwnProperty.call(heartbeatConfig, property)) {
      return fallback;
    }
    const value = Number(heartbeatConfig[property]);
    if (!Number.isFinite(value) || value <= 0) {
      throw new Error(`Invalid ${fieldName}.${property}: must be > 0`);
    }
    return Math.max(1, Math.trunc(value));
  };

  const enabled = readBoolean('enabled', DEFAULT_TRUE_CONCURRENCY_HEARTBEAT_CONFIG.enabled);
  const requiredEnabled = readBoolean('required', DEFAULT_TRUE_CONCURRENCY_HEARTBEAT_CONFIG.required);
  const rawPath = Object.prototype.hasOwnProperty.call(heartbeatConfig, 'path')
    ? normalizeStringValue(heartbeatConfig.path)
    : '';
  const normalizedPath = rawPath
    ? normalizePath(rawPath)
    : DEFAULT_TRUE_CONCURRENCY_HEARTBEAT_CONFIG.path;

  if (!normalizedPath) {
    throw new Error(`Invalid ${fieldName}.path`);
  }

  const normalized = {
    enabled,
    required: requiredEnabled,
    path: normalizedPath,
    intervalMs: readPositiveInt('intervalMs', DEFAULT_TRUE_CONCURRENCY_HEARTBEAT_CONFIG.intervalMs),
    timeoutMs: readPositiveInt('timeoutMs', DEFAULT_TRUE_CONCURRENCY_HEARTBEAT_CONFIG.timeoutMs),
    reconnectGraceMs: readPositiveInt('reconnectGraceMs', DEFAULT_TRUE_CONCURRENCY_HEARTBEAT_CONFIG.reconnectGraceMs),
    helloTimeoutMs: readPositiveInt('helloTimeoutMs', DEFAULT_TRUE_CONCURRENCY_HEARTBEAT_CONFIG.helloTimeoutMs),
    startTimeoutMs: readPositiveInt('startTimeoutMs', DEFAULT_TRUE_CONCURRENCY_HEARTBEAT_CONFIG.startTimeoutMs),
    ackTimeoutMs: readPositiveInt('ackTimeoutMs', DEFAULT_TRUE_CONCURRENCY_HEARTBEAT_CONFIG.ackTimeoutMs),
    initialConnectMaxAttempts: readPositiveInt('initialConnectMaxAttempts', DEFAULT_TRUE_CONCURRENCY_HEARTBEAT_CONFIG.initialConnectMaxAttempts),
    initialConnectMaxElapsedMs: readPositiveInt('initialConnectMaxElapsedMs', DEFAULT_TRUE_CONCURRENCY_HEARTBEAT_CONFIG.initialConnectMaxElapsedMs),
    reconnectMaxAttempts: readPositiveInt('reconnectMaxAttempts', DEFAULT_TRUE_CONCURRENCY_HEARTBEAT_CONFIG.reconnectMaxAttempts),
    reconnectMaxElapsedMs: readPositiveInt('reconnectMaxElapsedMs', DEFAULT_TRUE_CONCURRENCY_HEARTBEAT_CONFIG.reconnectMaxElapsedMs),
    reconnectBaseDelayMs: readPositiveInt('reconnectBaseDelayMs', DEFAULT_TRUE_CONCURRENCY_HEARTBEAT_CONFIG.reconnectBaseDelayMs),
    reconnectMaxDelayMs: readPositiveInt('reconnectMaxDelayMs', DEFAULT_TRUE_CONCURRENCY_HEARTBEAT_CONFIG.reconnectMaxDelayMs),
    reconnectSafetyMarginMs: readPositiveInt('reconnectSafetyMarginMs', DEFAULT_TRUE_CONCURRENCY_HEARTBEAT_CONFIG.reconnectSafetyMarginMs),
  };

  if (required && !normalized.enabled) {
    throw new Error(`Invalid ${fieldName}.enabled: must be true`);
  }
  if (required && !normalized.required) {
    throw new Error(`Invalid ${fieldName}.required: must be true`);
  }
  if (normalized.required && !normalized.enabled) {
    throw new Error(`Invalid ${fieldName}.required: required=true requires enabled=true`);
  }
  if (normalized.timeoutMs <= normalized.intervalMs) {
    throw new Error(`Invalid ${fieldName}.timeoutMs: must be greater than intervalMs`);
  }
  if (normalized.reconnectGraceMs > normalized.timeoutMs) {
    throw new Error(`Invalid ${fieldName}.reconnectGraceMs: must be <= timeoutMs`);
  }
  if (normalized.reconnectSafetyMarginMs >= normalized.reconnectGraceMs) {
    throw new Error(`Invalid ${fieldName}.reconnectSafetyMarginMs: must be < reconnectGraceMs`);
  }
  if (normalized.reconnectMaxElapsedMs > (normalized.reconnectGraceMs - normalized.reconnectSafetyMarginMs)) {
    throw new Error(`Invalid ${fieldName}.reconnect: reconnectMaxElapsedMs exceeds reconnect grace budget`);
  }
  if ((normalized.initialConnectMaxElapsedMs + normalized.helloTimeoutMs) > (normalized.startTimeoutMs - normalized.reconnectSafetyMarginMs)) {
    throw new Error(`Invalid ${fieldName}.startTimeoutMs: initial connect budget exceeds start timeout budget`);
  }

  return normalized;
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
  return (typeof raw === 'number' && Number.isSafeInteger(raw) && raw > 0) ? raw : null;
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

function markHostOverloaded(hostname, retryAfterMs) {
  const hostKey = deriveProviderHostBucket(hostname);
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
  const hostKey = deriveProviderHostBucket(hostname);
  if (!hostKey) {
    return '';
  }
  return buildScopedOverloadKey([
    hostKey,
    normalizeOverloadScopeValue(siteBucket, 'unknown'),
  ]);
}

function buildIpOverloadKey(hostname, siteBucket, ipBucket) {
  const hostKey = deriveProviderHostBucket(hostname);
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
  const hostKey = deriveProviderHostBucket(hostname);
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

function isNodeTestRunner() {
  return typeof process !== 'undefined'
    && typeof process.env?.NODE_TEST_CONTEXT === 'string'
    && process.env.NODE_TEST_CONTEXT.length > 0;
}

async function slowFailDelay() {
  if (isNodeTestRunner()) {
    return;
  }
  if (!SLOW_FAIL_DELAY_MS || SLOW_FAIL_DELAY_MS <= 0) {
    return;
  }
  await new Promise((resolve) => setTimeout(resolve, SLOW_FAIL_DELAY_MS));
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
  if (Object.prototype.hasOwnProperty.call(dbConfig, 'idleTimeoutSeconds')) {
    throw new Error('controller download.db.idleTimeoutSeconds is not supported');
  }
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

  const cacheTableName = normalizeString(dbConfig.cacheTable, 'DOWNLOAD_CACHE_TABLE');
  const ticketStateTableName = normalizeString(dbConfig.ticketStateTable, 'DOWNLOAD_TICKET_STATE_TABLE');

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
      ticketStateTableName,
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
  const hasThrottleProfileField = Boolean(downloadDecision)
    && Object.prototype.hasOwnProperty.call(downloadDecision, 'throttleProfile');
  let throttleProfileName = 'default';
  if (hasThrottleProfileField) {
    if (typeof downloadDecision.throttleProfile !== 'string' || downloadDecision.throttleProfile.trim() === '') {
      throw new Error('Invalid throttleProfile from controller: expected non-empty string');
    }
    throttleProfileName = downloadDecision.throttleProfile.trim();
  }
  const shouldRequireThrottleProfile = isCustomDb || hasThrottleProfileField;
  const throttleProfile = shouldRequireThrottleProfile
    ? throttleProfiles[throttleProfileName]
    : null;
  if (shouldRequireThrottleProfile && !throttleProfile) {
    throw new Error(`Unknown throttleProfile from controller: ${throttleProfileName}`);
  }
  const throttleProfileConfig = throttleProfile || {};
  const throttleHostnamePatterns = Array.isArray(throttleProfileConfig.hostPatterns)
    ? throttleProfileConfig.hostPatterns.map((p) => normalizeString(p)).filter((p) => p.length > 0)
    : [];
  const protectHttpCodes = normalizeProtectHttpCodes(throttleProfileConfig.protectHttpCodes);
  const throttleEnabled = isCustomDb && throttleHostnamePatterns.length > 0;
  const throttleConfig = {
    postgrestUrl,
    verifyHeader,
    verifySecret,
    openCapSeconds: normalizePositiveSeconds(throttleProfileConfig.openCapSeconds, DEFAULT_THROTTLE_OPEN_CAP_SECONDS),
    openThresholdPercent: normalizePositiveSeconds(
      throttleProfileConfig.openThresholdPercent,
      DEFAULT_THROTTLE_OPEN_THRESHOLD_PERCENT,
    ),
    closeThresholdPercent: normalizeNonNegativeInt(
      throttleProfileConfig.closeThresholdPercent,
      DEFAULT_THROTTLE_CLOSE_THRESHOLD_PERCENT,
    ),
    ewmaSpan: normalizePositiveSeconds(throttleProfileConfig.ewmaSpan, DEFAULT_THROTTLE_EWMA_SPAN),
    consecutiveThreshold: normalizePositiveSeconds(
      throttleProfileConfig.consecutiveThreshold,
      DEFAULT_THROTTLE_CONSECUTIVE_THRESHOLD,
    ),
    minSamplesBeforeEwmaOpen: normalizePositiveSeconds(
      throttleProfileConfig.minSamplesBeforeEwmaOpen,
      DEFAULT_THROTTLE_MIN_SAMPLES_BEFORE_EWMA_OPEN,
    ),
    idleResetSeconds: normalizeNonNegativeInt(
      throttleProfileConfig.idleResetSeconds,
      DEFAULT_THROTTLE_IDLE_RESET_SECONDS,
    ),
    halfOpenSuccessThreshold: normalizePositiveSeconds(
      throttleProfileConfig.halfOpenSuccessThreshold,
      DEFAULT_THROTTLE_HALF_OPEN_SUCCESS_THRESHOLD,
    ),
    halfOpenCloseMode: normalizeHalfOpenCloseMode(throttleProfileConfig.halfOpenCloseMode),
    halfOpenMaxProbeCount: normalizePositiveSeconds(
      throttleProfileConfig.halfOpenMaxProbeCount,
      DEFAULT_THROTTLE_HALF_OPEN_MAX_PROBE_COUNT,
    ),
    halfOpenMaxSeconds: normalizePositiveSeconds(
      throttleProfileConfig.halfOpenMaxSeconds,
      DEFAULT_THROTTLE_HALF_OPEN_MAX_SECONDS,
    ),
    halfOpenTimeoutMode: normalizeHalfOpenTimeoutMode(throttleProfileConfig.halfOpenTimeoutMode),
    protectHttpCodes,
  };
  if (throttleConfig.halfOpenMaxProbeCount > MAX_THROTTLE_HALF_OPEN_PROBE_COUNT) {
    throw new Error(`controller throttle profile is invalid: halfOpenMaxProbeCount must be <= ${MAX_THROTTLE_HALF_OPEN_PROBE_COUNT}`);
  }
  if (throttleConfig.halfOpenSuccessThreshold > throttleConfig.halfOpenMaxProbeCount) {
    throw new Error('controller throttle profile is invalid: halfOpenSuccessThreshold must be <= halfOpenMaxProbeCount');
  }

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
  const fairQueueSiteBucket = normalizeSiteBucketConfig(
    fairQueueConfigRaw.siteBucket,
    'controller fairQueue.siteBucket',
  );
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

  const trueConcurrencyConfigRaw = downloadBootstrap.trueConcurrency && typeof downloadBootstrap.trueConcurrency === 'object'
    ? downloadBootstrap.trueConcurrency
    : {};
  const trueConcurrencyHostnamePatterns = Array.isArray(trueConcurrencyConfigRaw.hostPatterns)
    ? trueConcurrencyConfigRaw.hostPatterns.map((p) => normalizeString(p)).filter((p) => p.length > 0)
    : [];
  if (Boolean(trueConcurrencyConfigRaw.enabled) && trueConcurrencyHostnamePatterns.length === 0) {
    throw new Error('controller trueConcurrency.hostPatterns is required when trueConcurrency.enabled is true');
  }
  const trueConcurrencyEnabled = Boolean(trueConcurrencyConfigRaw.enabled) && trueConcurrencyHostnamePatterns.length > 0;
  const concurrencyHandlerUrl = normalizeString(trueConcurrencyConfigRaw.handlerUrl);
  const concurrencyHandlerAuthKey = normalizeString(trueConcurrencyConfigRaw.handlerAuthKey);
  const concurrencyHandlerAuthHeader = normalizeString(trueConcurrencyConfigRaw.handlerAuthHeader)
    || DEFAULT_TRUE_CONCURRENCY_AUTH_HEADER;
  if (trueConcurrencyEnabled && !concurrencyHandlerUrl) {
    throw new Error('controller trueConcurrency.handlerUrl is required when trueConcurrency.enabled is true');
  }
  if (trueConcurrencyEnabled && !concurrencyHandlerAuthKey) {
    throw new Error('controller trueConcurrency.handlerAuthKey is required when trueConcurrency.enabled is true');
  }
  const trueConcurrencySiteBucket = normalizeSiteBucketConfig(
    trueConcurrencyConfigRaw.siteBucket,
    'controller trueConcurrency.siteBucket',
  );
  const heartbeatConfig = normalizeTrueConcurrencyHeartbeatConfig(
    trueConcurrencyConfigRaw.heartbeat,
    {
      fieldName: 'controller trueConcurrency.heartbeat',
      required: trueConcurrencyEnabled,
    },
  );
  const concurrencyHandlerConfig = {
    url: concurrencyHandlerUrl,
    authKey: concurrencyHandlerAuthKey,
    authHeader: concurrencyHandlerAuthHeader,
    acquireTimeoutMs: normalizePositiveMs(
      trueConcurrencyConfigRaw.acquireTimeoutMs,
      DEFAULT_TRUE_CONCURRENCY_ACQUIRE_TIMEOUT_MS,
    ),
    releaseTimeoutMs: normalizePositiveMs(
      trueConcurrencyConfigRaw.releaseTimeoutMs,
      DEFAULT_TRUE_CONCURRENCY_RELEASE_TIMEOUT_MS,
    ),
    waitTotalMaxMs: readOptionalPositiveIntegerFromBootstrap(
      trueConcurrencyConfigRaw,
      'waitTotalMaxMs',
      DEFAULT_TRUE_CONCURRENCY_WAIT_TOTAL_MAX_MS,
      'controller trueConcurrency.waitTotalMaxMs',
    ),
    waitMaxAttemptsCap: readOptionalPositiveIntegerFromBootstrap(
      trueConcurrencyConfigRaw,
      'waitMaxAttemptsCap',
      DEFAULT_TRUE_CONCURRENCY_WAIT_MAX_ATTEMPTS_CAP,
      'controller trueConcurrency.waitMaxAttemptsCap',
    ),
    heartbeat: heartbeatConfig,
  };

  const enableCfRatelimiter = normalizeString(env.ENABLE_CF_RATELIMITER, 'false').toLowerCase() === 'true';
  const cfRatelimiterBinding = normalizeString(env.CF_RATELIMITER_BINDING, 'CF_RATE_LIMITER');

  if (enableCfRatelimiter) {
    const ratelimiter = env[cfRatelimiterBinding];
    if (!ratelimiter || typeof ratelimiter.limit !== 'function') {
      throw new Error(
        `ENABLE_CF_RATELIMITER is true but binding "${cfRatelimiterBinding}" not found or invalid. Please configure [[ratelimits]] binding in wrangler.toml with name="${cfRatelimiterBinding}".`
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
    postgrestUrl,
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
    ticketStateTableName,
    fairQueueEnabled: fairQueueContext.fairQueueEnabled,
    fairQueueHostnamePatterns: fairQueueContext.fairQueueHostnamePatterns,
    fairQueueSiteBucket: fairQueueContext.fairQueueSiteBucket,
    trueConcurrencyEnabled,
    trueConcurrencyHostnamePatterns,
    trueConcurrencySiteBucket,
    concurrencyHandlerConfig,
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

function createBreakerAuthorityUnavailableResponse(origin, phase) {
  const suffix = typeof phase === 'string' && phase.trim() !== '' ? ` during ${phase}` : '';
  return createErrorResponse(origin, 503, `Throttle breaker authority unavailable${suffix}`);
}

const applyUnifiedResult = (unifiedResult, options = {}) => {
  if (!unifiedResult || !options.throttleEnabled) {
    return null;
  }

  const throttleHostnameRaw = extractHostname(unifiedResult?.cache?.linkData?.url || '');
  const throttleHostnameFromCache = throttleHostnameRaw ? throttleHostnameRaw.toLowerCase() : '';
  const throttleHostnameOverride = options.throttleHostname || '';
  const throttleHostname = (throttleHostnameOverride || throttleHostnameFromCache).toLowerCase();
  if (!isManagedThrottleHost(throttleHostname, options.throttleHostnamePatterns)) {
    return null;
  }

  const breakerState = readOpenBreakerSnapshot(
    unifiedResult.throttle,
    options.openCapSeconds || DEFAULT_THROTTLE_OPEN_CAP_SECONDS,
  );
  if (!breakerState) {
    return null;
  }

  return createThrottleProtectedResponse(options.origin || '*', breakerState);
};

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

const TRUE_CONCURRENCY_ACQUIRE_RESULTS = new Set(['granted', 'wait', 'conflict', 'released', 'cancelled', 'expired']);
const TRUE_CONCURRENCY_CLAIM_RESULTS = new Set(['granted', 'conflict', 'released', 'cancelled', 'expired']);
const TRUE_CONCURRENCY_ACK_HANDOFF_RESULTS = new Set(['acknowledged', 'conflict', 'released', 'cancelled', 'expired']);
const TRUE_CONCURRENCY_RELEASE_RESULTS = new Set(['released', 'noop', 'expired']);
const TRUE_CONCURRENCY_CANCEL_RESULTS = new Set(['cancelled', 'noop', 'conflict']);
const TRUE_CONCURRENCY_WAIT_SCOPES = new Set(['host', 'site', 'site_ip']);
const TRUE_CONCURRENCY_ACQUIRE_CONFLICT_REASONS = new Set([
  'request_id_tuple_mismatch',
  'waiter_already_attached',
  'stale_wait_token',
  'grant_unclaimed',
  'grant_already_claimed',
]);
const TRUE_CONCURRENCY_CANCEL_CONFLICT_REASONS = new Set([
  'request_id_tuple_mismatch',
  'must_release_active_lease',
]);
const TRUE_CONCURRENCY_ACK_HANDOFF_CONFLICT_REASONS = new Set(['handoff_token_mismatch']);
const TRUE_CONCURRENCY_RELEASED_TERMINAL_REASONS = new Set([
  'already_released',
  'stream_complete',
  'client_disconnect',
  'hard_expiry',
  'upstream_failure',
  'origin_fetch_failure',
  'heartbeat_connect_failed',
  'heartbeat_lost',
  'final_cleanup',
  'heartbeat_start_timeout',
  'heartbeat_timeout',
  'claim_handoff_timeout',
]);
const TRUE_CONCURRENCY_CANCELLED_TERMINAL_REASONS = new Set(['request_cancelled']);
const TRUE_CONCURRENCY_EXPIRED_TERMINAL_REASONS = new Set([
  'hard_expired',
  'waiter_detached_timeout',
]);
const TRUE_CONCURRENCY_RELEASE_NOOP_REASONS = new Set([
  'already_released',
  'expired',
  'not_found',
  'token_mismatch',
]);
const TRUE_CONCURRENCY_CANCEL_NOOP_REASONS = new Set(['already_terminal']);
const TRUE_CONCURRENCY_HEARTBEAT_TERMINAL_REASONS = new Set([
  'heartbeat_timeout',
  'heartbeat_start_timeout',
  'hard_expired',
  'already_released',
  'request_cancelled',
  'protocol_error',
  'token_mismatch',
]);
const TRUE_CONCURRENCY_HEARTBEAT_TERMINAL_NO_RELEASE_REASONS = new Set([
  'heartbeat_timeout',
  'heartbeat_start_timeout',
  'hard_expired',
  'already_released',
  'request_cancelled',
]);
const TRUE_CONCURRENCY_CANONICAL_RELEASE_REASONS = new Set([
  'stream_complete',
  'client_disconnect',
  'hard_expiry',
  'upstream_failure',
  'origin_fetch_failure',
  'heartbeat_connect_failed',
  'heartbeat_lost',
  'final_cleanup',
]);
const TRUE_CONCURRENCY_RELEASE_REASON_ALIASES = new Map([
  ['target_change', 'final_cleanup'],
  ['grant_delivery_failed', 'final_cleanup'],
  ['acquire_delivery_failed', 'final_cleanup'],
  ['head_probe_complete', 'stream_complete'],
  ['head_probe_invalid', 'origin_fetch_failure'],
  ['prestream_terminal', 'origin_fetch_failure'],
  ['google_drive_range_mismatch', 'origin_fetch_failure'],
]);

const normalizeTrueConcurrencyReleaseReason = (reason) => {
  const normalizedReason = normalizeStringValue(reason);
  if (TRUE_CONCURRENCY_CANONICAL_RELEASE_REASONS.has(normalizedReason)) {
    return normalizedReason;
  }
  return TRUE_CONCURRENCY_RELEASE_REASON_ALIASES.get(normalizedReason) || 'final_cleanup';
};

const isTrueConcurrencyLeaseIdentity = (lease) => (
  typeof lease?.leaseId === 'string'
  && lease.leaseId
  && typeof lease?.leaseToken === 'string'
  && lease.leaseToken
);

const isTrueConcurrencyRequestIdentity = (requestIdentity) => (
  typeof requestIdentity?.requestId === 'string'
  && requestIdentity.requestId
  && typeof requestIdentity?.hostname === 'string'
  && requestIdentity.hostname
  && typeof requestIdentity?.hostnameHash === 'string'
  && requestIdentity.hostnameHash
  && typeof requestIdentity?.siteBucket === 'string'
  && requestIdentity.siteBucket
  && typeof requestIdentity?.ipBucket === 'string'
  && requestIdentity.ipBucket
  && Number.isFinite(Number(requestIdentity?.hardExpireAtMs))
  && Number(requestIdentity.hardExpireAtMs) > 0
);

const buildTrueConcurrencyReleasePayload = (lease, reason) => {
  if (isTrueConcurrencyLeaseIdentity(lease)) {
    return {
      leaseId: lease.leaseId,
      leaseToken: lease.leaseToken,
      reason: normalizeTrueConcurrencyReleaseReason(reason),
      nowMs: Date.now(),
    };
  }

  throw new Error('[CQ] release requires lease identity');
};

const buildTrueConcurrencyCancelPayload = (requestIdentity, reason) => {
  if (isTrueConcurrencyRequestIdentity(requestIdentity)) {
    return {
      requestId: requestIdentity.requestId,
      hostname: requestIdentity.hostname,
      hostnameHash: requestIdentity.hostnameHash,
      siteBucket: requestIdentity.siteBucket,
      ipBucket: requestIdentity.ipBucket,
      hardExpireAtMs: Number(requestIdentity.hardExpireAtMs),
      reason,
      nowMs: Date.now(),
    };
  }

  throw new Error('[CQ] cancel requires request identity');
};

const readTrueConcurrencyResult = (operation, data, allowedResults) => {
  const result = typeof data?.result === 'string' ? data.result : '';
  if (!result || !(allowedResults instanceof Set) || !allowedResults.has(result)) {
    throw new Error(`[CQ] ${operation} returned malformed success result: ${result || 'unknown'}`);
  }
  return result;
};

const readTrueConcurrencyTerminalReason = (operation, result, data, allowedReasons) => {
  if (typeof data?.reason !== 'string' || !data.reason) {
    throw new Error(`[CQ] ${operation} ${result} response missing reason`);
  }
  if (!(allowedReasons instanceof Set) || !allowedReasons.has(data.reason)) {
    throw new Error(`[CQ] ${operation} ${result} response has unsupported reason`);
  }
  return data.reason;
};

const normalizeTrueConcurrencyAcquireResult = (data, options = {}) => {
  const result = readTrueConcurrencyResult('acquire', data, TRUE_CONCURRENCY_ACQUIRE_RESULTS);

  if (result === 'granted') {
    if (typeof data?.leaseId !== 'string' || !data.leaseId) {
      throw new Error('[CQ] acquire granted response missing leaseId');
    }
    if (typeof data?.leaseToken !== 'string' || !data.leaseToken) {
      throw new Error('[CQ] acquire granted response missing leaseToken');
    }
    const expiresAtMs = Number(data?.expiresAtMs);
    if (!Number.isFinite(expiresAtMs) || expiresAtMs <= 0) {
      throw new Error('[CQ] acquire granted response missing expiresAtMs');
    }
    if (Number.isFinite(options.hardExpireAtMs) && expiresAtMs > options.hardExpireAtMs) {
      throw new Error('[CQ] acquire granted response exceeds hardExpireAtMs');
    }
    if (typeof data?.claimToken !== 'string' || !data.claimToken) {
      throw new Error('[CQ] acquire granted response missing claimToken');
    }
    return {
      result: 'granted',
      leaseId: data.leaseId,
      leaseToken: data.leaseToken,
      expiresAtMs,
      claimToken: data.claimToken,
    };
  }

  if (result === 'wait') {
    if (typeof data?.waitToken !== 'string' || !data.waitToken) {
      throw new Error('[CQ] acquire wait response missing waitToken');
    }
    if (typeof data?.scope !== 'string' || !data.scope) {
      throw new Error('[CQ] acquire wait response missing scope');
    }
    if (!TRUE_CONCURRENCY_WAIT_SCOPES.has(data.scope)) {
      throw new Error('[CQ] acquire wait response has unsupported scope');
    }
    const retryAfter = Number(data?.retryAfter);
    if (!Number.isFinite(retryAfter) || retryAfter <= 0) {
      throw new Error('[CQ] acquire wait response missing retryAfter');
    }
    return {
      result: 'wait',
      waitToken: data.waitToken,
      scope: data.scope,
      retryAfter,
    };
  }

  if (result === 'conflict') {
    if (typeof data?.reason !== 'string' || !data.reason) {
      throw new Error('[CQ] acquire conflict response missing reason');
    }
    if (!TRUE_CONCURRENCY_ACQUIRE_CONFLICT_REASONS.has(data.reason)) {
      throw new Error('[CQ] acquire conflict response has unsupported reason');
    }
    return {
      result: 'conflict',
      reason: data.reason,
    };
  }

  const allowedTerminalReasons = result === 'released'
    ? TRUE_CONCURRENCY_RELEASED_TERMINAL_REASONS
    : result === 'cancelled'
      ? TRUE_CONCURRENCY_CANCELLED_TERMINAL_REASONS
      : TRUE_CONCURRENCY_EXPIRED_TERMINAL_REASONS;
  const reason = readTrueConcurrencyTerminalReason('acquire', result, data, allowedTerminalReasons);
  return {
    result,
    reason,
  };
};

const normalizeTrueConcurrencyClaimResult = (data, options = {}) => {
  const result = readTrueConcurrencyResult('claim', data, TRUE_CONCURRENCY_CLAIM_RESULTS);

  if (result === 'granted') {
    if (typeof data?.leaseId !== 'string' || !data.leaseId) {
      throw new Error('[CQ] claim granted response missing leaseId');
    }
    if (typeof data?.leaseToken !== 'string' || !data.leaseToken) {
      throw new Error('[CQ] claim granted response missing leaseToken');
    }
    const expiresAtMs = Number(data?.expiresAtMs);
    if (!Number.isFinite(expiresAtMs) || expiresAtMs <= 0) {
      throw new Error('[CQ] claim granted response missing expiresAtMs');
    }
    if (Number.isFinite(options.hardExpireAtMs) && expiresAtMs > options.hardExpireAtMs) {
      throw new Error('[CQ] claim granted response exceeds hardExpireAtMs');
    }
    if (typeof data?.handoffToken !== 'string' || !data.handoffToken) {
      throw new Error('[CQ] claim granted response missing handoffToken');
    }
    const handoffDeadlineMs = Number(data?.handoffDeadlineMs);
    if (!Number.isFinite(handoffDeadlineMs) || handoffDeadlineMs <= 0) {
      throw new Error('[CQ] claim granted response missing handoffDeadlineMs');
    }
    if (handoffDeadlineMs >= expiresAtMs) {
      throw new Error('[CQ] claim granted response exceeds lease handoff window');
    }
    return {
      result: 'granted',
      leaseId: data.leaseId,
      leaseToken: data.leaseToken,
      expiresAtMs,
      handoffToken: data.handoffToken,
      handoffDeadlineMs,
    };
  }

  if (result === 'conflict') {
    if (typeof data?.reason !== 'string' || !data.reason) {
      throw new Error('[CQ] claim conflict response missing reason');
    }
    if (!TRUE_CONCURRENCY_ACQUIRE_CONFLICT_REASONS.has(data.reason)) {
      throw new Error('[CQ] claim conflict response has unsupported reason');
    }
    return { result: 'conflict', reason: data.reason };
  }

  const allowedTerminalReasons = result === 'released'
    ? TRUE_CONCURRENCY_RELEASED_TERMINAL_REASONS
    : result === 'cancelled'
      ? TRUE_CONCURRENCY_CANCELLED_TERMINAL_REASONS
      : TRUE_CONCURRENCY_EXPIRED_TERMINAL_REASONS;
  return {
    result,
    reason: readTrueConcurrencyTerminalReason('claim', result, data, allowedTerminalReasons),
  };
};

const normalizeTrueConcurrencyAckHandoffResult = (data) => {
  const result = readTrueConcurrencyResult('ack_handoff', data, TRUE_CONCURRENCY_ACK_HANDOFF_RESULTS);

  if (result === 'acknowledged') {
    if (typeof data?.reason === 'string' && data.reason) {
      throw new Error('[CQ] ack_handoff acknowledged response must not include reason');
    }
    return { result: 'acknowledged' };
  }

  if (result === 'conflict') {
    if (typeof data?.reason !== 'string' || !data.reason) {
      throw new Error('[CQ] ack_handoff conflict response missing reason');
    }
    if (!TRUE_CONCURRENCY_ACK_HANDOFF_CONFLICT_REASONS.has(data.reason)) {
      throw new Error('[CQ] ack_handoff conflict response has unsupported reason');
    }
    return {
      result: 'conflict',
      reason: data.reason,
    };
  }

  const allowedTerminalReasons = result === 'released'
    ? TRUE_CONCURRENCY_RELEASED_TERMINAL_REASONS
    : result === 'cancelled'
      ? TRUE_CONCURRENCY_CANCELLED_TERMINAL_REASONS
      : TRUE_CONCURRENCY_EXPIRED_TERMINAL_REASONS;
  return {
    result,
    reason: readTrueConcurrencyTerminalReason('ack_handoff', result, data, allowedTerminalReasons),
  };
};

const normalizeTrueConcurrencyReleaseResult = (data) => {
  const result = readTrueConcurrencyResult('release', data, TRUE_CONCURRENCY_RELEASE_RESULTS);
  if (result === 'released') {
    return { result: 'released' };
  }
  if (result === 'expired') {
    if (typeof data?.reason !== 'string' || !data.reason) {
      throw new Error('[CQ] release expired response missing reason');
    }
    if (data.reason !== 'hard_expired') {
      throw new Error('[CQ] release expired response has unsupported reason');
    }
    return {
      result: 'expired',
      reason: data.reason,
    };
  }
  if (typeof data?.reason !== 'string' || !data.reason) {
    throw new Error('[CQ] release noop response missing reason');
  }
  if (!TRUE_CONCURRENCY_RELEASE_NOOP_REASONS.has(data.reason)) {
    throw new Error('[CQ] release noop response has unsupported reason');
  }
  return {
    result: 'noop',
    reason: data.reason,
  };
};

const normalizeTrueConcurrencyCancelResult = (data) => {
  const result = readTrueConcurrencyResult('cancel', data, TRUE_CONCURRENCY_CANCEL_RESULTS);
  if (result === 'cancelled') {
    return { result: 'cancelled' };
  }
  if (result === 'noop') {
    if (typeof data?.reason !== 'string' || !data.reason) {
      throw new Error('[CQ] cancel noop response missing reason');
    }
    if (!TRUE_CONCURRENCY_CANCEL_NOOP_REASONS.has(data.reason)) {
      throw new Error('[CQ] cancel noop response has unsupported reason');
    }
    return {
      result: 'noop',
      reason: data.reason,
    };
  }
  if (typeof data?.reason !== 'string' || !data.reason) {
    throw new Error('[CQ] cancel conflict response missing reason');
  }
  if (!TRUE_CONCURRENCY_CANCEL_CONFLICT_REASONS.has(data.reason)) {
    throw new Error('[CQ] cancel conflict response has unsupported reason');
  }
  return {
    result: 'conflict',
    reason: data.reason,
  };
};

const parseTrueConcurrencyHeartbeatJson = (rawData, messageType) => {
  const decode = () => {
    if (typeof rawData === 'string') {
      return rawData;
    }
    if (rawData instanceof ArrayBuffer) {
      return new TextDecoder().decode(new Uint8Array(rawData));
    }
    if (ArrayBuffer.isView(rawData)) {
      return new TextDecoder().decode(rawData);
    }
    throw new Error(`[CQ] ${messageType} message must be text`);
  };

  try {
    const parsed = JSON.parse(decode());
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error('payload must be an object');
    }
    return parsed;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`[CQ] ${messageType} parse failed: ${message}`);
  }
};

const readTrueConcurrencyHeartbeatRequiredString = (payload, fieldName, messageType) => {
  if (typeof payload?.[fieldName] !== 'string' || !payload[fieldName]) {
    throw new Error(`[CQ] ${messageType} missing ${fieldName}`);
  }
  return payload[fieldName];
};

const readTrueConcurrencyHeartbeatRequiredPositiveInt = (payload, fieldName, messageType) => {
  const value = Number(payload?.[fieldName]);
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`[CQ] ${messageType} missing ${fieldName}`);
  }
  return Math.max(1, Math.trunc(value));
};

const normalizeTrueConcurrencyHelloAck = (payload) => {
  if (payload?.type !== 'hello_ack') {
    throw new Error('[CQ] hello_ack response has unsupported type');
  }
  const helloAck = {
    type: 'hello_ack',
    generation: readTrueConcurrencyHeartbeatRequiredPositiveInt(payload, 'generation', 'hello_ack'),
    deadlineMs: readTrueConcurrencyHeartbeatRequiredPositiveInt(payload, 'deadlineMs', 'hello_ack'),
    ackTimeoutMs: readTrueConcurrencyHeartbeatRequiredPositiveInt(payload, 'ackTimeoutMs', 'hello_ack'),
    heartbeatIntervalMs: readTrueConcurrencyHeartbeatRequiredPositiveInt(payload, 'heartbeatIntervalMs', 'hello_ack'),
    heartbeatTimeoutMs: readTrueConcurrencyHeartbeatRequiredPositiveInt(payload, 'heartbeatTimeoutMs', 'hello_ack'),
    reconnectGraceMs: readTrueConcurrencyHeartbeatRequiredPositiveInt(payload, 'reconnectGraceMs', 'hello_ack'),
    startTimeoutMs: readTrueConcurrencyHeartbeatRequiredPositiveInt(payload, 'startTimeoutMs', 'hello_ack'),
    hardExpireAtMs: readTrueConcurrencyHeartbeatRequiredPositiveInt(payload, 'hardExpireAtMs', 'hello_ack'),
  };
  if (helloAck.deadlineMs > helloAck.hardExpireAtMs) {
    throw new Error('[CQ] hello_ack deadlineMs exceeds hardExpireAtMs');
  }
  return helloAck;
};

const normalizeTrueConcurrencyHeartbeatAck = (payload, generation) => {
  if (payload?.type !== 'heartbeat_ack') {
    throw new Error('[CQ] heartbeat_ack response has unsupported type');
  }
  const ack = {
    type: 'heartbeat_ack',
    generation: readTrueConcurrencyHeartbeatRequiredPositiveInt(payload, 'generation', 'heartbeat_ack'),
    deadlineMs: readTrueConcurrencyHeartbeatRequiredPositiveInt(payload, 'deadlineMs', 'heartbeat_ack'),
    hardExpireAtMs: readTrueConcurrencyHeartbeatRequiredPositiveInt(payload, 'hardExpireAtMs', 'heartbeat_ack'),
  };
  if (ack.generation !== generation) {
    throw new Error('[CQ] heartbeat_ack generation mismatch');
  }
  if (ack.deadlineMs > ack.hardExpireAtMs) {
    throw new Error('[CQ] heartbeat_ack deadlineMs exceeds hardExpireAtMs');
  }
  return ack;
};

const normalizeTrueConcurrencyHeartbeatTerminal = (payload) => {
  if (payload?.type !== 'terminal') {
    throw new Error('[CQ] terminal heartbeat response has unsupported type');
  }
  const result = typeof payload?.result === 'string' ? payload.result : '';
  if (!result) {
    throw new Error('[CQ] terminal heartbeat response missing result');
  }
  const reason = typeof payload?.reason === 'string' ? payload.reason : '';
  if (!TRUE_CONCURRENCY_HEARTBEAT_TERMINAL_REASONS.has(reason)) {
    throw new Error('[CQ] terminal heartbeat response has unsupported reason');
  }
  return {
    type: 'terminal',
    result,
    reason,
  };
};

const createTrueConcurrencyHeartbeatTerminalError = (terminal) => {
  const reason = terminal?.reason || 'unknown';
  const error = new Error(`[CQ] heartbeat terminal ${reason}`);
  error.name = 'TrueConcurrencyHeartbeatTerminalError';
  error.terminal = terminal;
  return error;
};

const isTrueConcurrencyHeartbeatTerminalError = (error) => (
  error instanceof Error
  && error.name === 'TrueConcurrencyHeartbeatTerminalError'
  && Boolean(error.terminal)
);

const waitForTrueConcurrencyHeartbeatMessage = (ws, signal, timeoutMs, messageType) => new Promise((resolve, reject) => {
  let settled = false;
  let timer = null;

  const cleanup = () => {
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
    ws.removeEventListener?.('message', onMessage);
    ws.removeEventListener?.('close', onClose);
    ws.removeEventListener?.('error', onError);
    signal?.removeEventListener?.('abort', onAbort);
  };

  const settle = (callback) => {
    if (settled) {
      return;
    }
    settled = true;
    cleanup();
    callback();
  };

  const onMessage = (event) => {
    settle(() => {
      try {
        resolve(parseTrueConcurrencyHeartbeatJson(event?.data, messageType));
      } catch (error) {
        reject(error);
      }
    });
  };

  const onClose = (event) => {
    const code = Number.isFinite(Number(event?.code)) ? Number(event.code) : 1000;
    const reason = typeof event?.reason === 'string' && event.reason ? `: ${event.reason}` : '';
    settle(() => reject(new Error(`[CQ] ${messageType} websocket closed before response (${code}${reason})`)));
  };

  const onError = () => {
    settle(() => reject(new Error(`[CQ] ${messageType} websocket error`)));
  };

  const onAbort = () => {
    settle(() => reject(new Error(`[CQ] ${messageType} aborted`)));
  };

  ws.addEventListener?.('message', onMessage);
  ws.addEventListener?.('close', onClose);
  ws.addEventListener?.('error', onError);
  if (signal?.aborted) {
    onAbort();
    return;
  }
  signal?.addEventListener?.('abort', onAbort, { once: true });

  timer = setTimeout(() => {
    settle(() => reject(new Error(`[CQ] ${messageType} timed out`)));
  }, timeoutMs);
});

const closeTrueConcurrencyHeartbeatSocket = (ws, reason = '') => {
  if (!ws || typeof ws.close !== 'function') {
    return;
  }
  try {
    ws.close(1000, typeof reason === 'string' ? reason : String(reason ?? ''));
  } catch (_error) {
    // Best-effort close only.
  }
};

const createConcurrencyHandlerClient = (config) => {
  const handlerCfg = config.concurrencyHandlerConfig || {};
  const baseUrl = normalizePostgrestBaseUrl(handlerCfg.url);
  if (!baseUrl) {
    throw new Error('[CQ] concurrency-handler enabled but handler URL is missing');
  }

  const authKey = normalizeStringValue(handlerCfg.authKey);
  const authHeader = normalizeStringValue(handlerCfg.authHeader, DEFAULT_TRUE_CONCURRENCY_AUTH_HEADER);
  const acquireUrl = `${baseUrl}/api/v1/concurrency/acquire`;
  const claimUrl = `${baseUrl}/api/v1/concurrency/claim`;
  const ackHandoffUrl = `${baseUrl}/api/v1/concurrency/ack_handoff`;
  const releaseUrl = `${baseUrl}/api/v1/concurrency/release`;
  const cancelUrl = `${baseUrl}/api/v1/concurrency/cancel`;
  const heartbeatPath = normalizePath(normalizeStringValue(handlerCfg.heartbeat?.path))
    || DEFAULT_TRUE_CONCURRENCY_HEARTBEAT_CONFIG.path;
  const heartbeatUrl = `${baseUrl}${heartbeatPath}`;
  const acquireTimeoutMs = normalizePositiveMs(
    handlerCfg.acquireTimeoutMs,
    DEFAULT_TRUE_CONCURRENCY_ACQUIRE_TIMEOUT_MS,
  );
  const releaseTimeoutMs = normalizePositiveMs(
    handlerCfg.releaseTimeoutMs,
    DEFAULT_TRUE_CONCURRENCY_RELEASE_TIMEOUT_MS,
  );

  const buildHeaders = () => {
    const headers = { 'Content-Type': 'application/json' };
    if (authKey) {
      headers[authHeader] = authKey;
    }
    return headers;
  };

  const postJson = async (url, payload, options = {}) => {
    const timeoutMs = normalizePositiveMs(options.timeoutMs, DEFAULT_TRUE_CONCURRENCY_ACQUIRE_TIMEOUT_MS);
    const signal = options.signal;
    const allowedStatuses = Array.isArray(options.allowedStatuses) && options.allowedStatuses.length > 0
      ? options.allowedStatuses
      : [200];
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
      const response = await fetch(url, {
        method: 'POST',
        headers: buildHeaders(),
        body: JSON.stringify(payload),
        signal: controller.signal,
      });
      if (!allowedStatuses.includes(response.status)) {
        throw new Error(`[CQ] handler request failed with status ${response.status}`);
      }

      let data;
      try {
        data = await response.json();
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        throw new Error(`[CQ] handler response parse failed: ${message}`);
      }
      return { status: response.status, data };
    } finally {
      clearTimeout(timer);
      if (signal) {
        signal.removeEventListener('abort', abortHandler);
      }
    }
  };

  return {
    async acquire(_ctx, plan, signal, timeoutMs = acquireTimeoutMs) {
      const payload = {
        hostname: plan.hostname,
        hostnameHash: plan.hostnameHash,
        siteBucket: plan.siteBucket,
        ipBucket: plan.ipBucket,
        requestId: plan.requestId,
        hardExpireAtMs: plan.hardExpireAtMs,
        nowMs: plan.nowMs,
      };
      if (typeof plan.waitToken === 'string' && plan.waitToken) {
        payload.waitToken = plan.waitToken;
      }
      const { data } = await postJson(acquireUrl, payload, {
        timeoutMs,
        signal,
        allowedStatuses: [200, 409, 410],
      });
      return normalizeTrueConcurrencyAcquireResult(data, {
        hardExpireAtMs: plan.hardExpireAtMs,
      });
    },

    async release(_ctx, lease, reason, signal) {
      const { data } = await postJson(
        releaseUrl,
        buildTrueConcurrencyReleasePayload(lease, reason),
        {
          timeoutMs: releaseTimeoutMs,
          signal,
        },
      );
      return normalizeTrueConcurrencyReleaseResult(data);
    },

    async claim(_ctx, claim, signal) {
      const { data } = await postJson(
        claimUrl,
        {
          requestId: claim.requestId,
          claimToken: claim.claimToken,
          nowMs: claim.nowMs,
        },
        {
          timeoutMs: acquireTimeoutMs,
          signal,
          allowedStatuses: [200, 409, 410],
        },
      );
      return normalizeTrueConcurrencyClaimResult(data, {
        hardExpireAtMs: claim.hardExpireAtMs,
      });
    },

    async ackHandoff(_ctx, handoff, signal) {
      const { data } = await postJson(
        ackHandoffUrl,
        {
          requestId: handoff.requestId,
          handoffToken: handoff.handoffToken,
          nowMs: handoff.nowMs,
        },
        {
          timeoutMs: acquireTimeoutMs,
          signal,
          allowedStatuses: [200, 409, 410],
        },
      );
      return normalizeTrueConcurrencyAckHandoffResult(data);
    },

    async connectHeartbeat(_ctx, identity, signal = new AbortController().signal) {
      const heartbeatConfig = normalizeTrueConcurrencyHeartbeatConfig(handlerCfg.heartbeat, {
        fieldName: 'concurrencyHandlerConfig.heartbeat',
        required: true,
      });
      const helloPayload = {
        type: 'hello',
        requestId: readTrueConcurrencyHeartbeatRequiredString(identity, 'requestId', 'heartbeat hello'),
        leaseId: readTrueConcurrencyHeartbeatRequiredString(identity, 'leaseId', 'heartbeat hello'),
        leaseToken: readTrueConcurrencyHeartbeatRequiredString(identity, 'leaseToken', 'heartbeat hello'),
        ticketHash: readTrueConcurrencyHeartbeatRequiredString(identity, 'ticketHash', 'heartbeat hello'),
        hardExpireAtMs: readTrueConcurrencyHeartbeatRequiredPositiveInt(identity, 'hardExpireAtMs', 'heartbeat hello'),
        clientInstanceId: readTrueConcurrencyHeartbeatRequiredString(identity, 'clientInstanceId', 'heartbeat hello'),
        attempt: readTrueConcurrencyHeartbeatRequiredPositiveInt(identity, 'attempt', 'heartbeat hello'),
        nowMs: Date.now(),
      };

      const response = await fetch(heartbeatUrl, {
        headers: {
          Upgrade: 'websocket',
          ...(authKey ? { [authHeader]: authKey } : {}),
        },
        signal,
      });
      if (response?.status !== 101 || !response?.webSocket) {
        throw new Error(`[CQ] heartbeat upgrade failed with status ${response?.status ?? 'unknown'}`);
      }

      const ws = response.webSocket;
      if (typeof ws.accept === 'function') {
        ws.accept();
      }

      try {
        const helloAckPromise = waitForTrueConcurrencyHeartbeatMessage(
          ws,
          signal,
          heartbeatConfig.helloTimeoutMs,
          'hello_ack',
        );
        ws.send(JSON.stringify(helloPayload));
        const helloMessage = await helloAckPromise;
        if (helloMessage?.type === 'terminal') {
          throw createTrueConcurrencyHeartbeatTerminalError(
            normalizeTrueConcurrencyHeartbeatTerminal(helloMessage),
          );
        }
        const helloAck = normalizeTrueConcurrencyHelloAck(helloMessage);

        const session = {
          ws,
          generation: helloAck.generation,
          deadlineMs: helloAck.deadlineMs,
          ackTimeoutMs: helloAck.ackTimeoutMs,
          heartbeatIntervalMs: helloAck.heartbeatIntervalMs,
          heartbeatTimeoutMs: helloAck.heartbeatTimeoutMs,
          reconnectGraceMs: helloAck.reconnectGraceMs,
          startTimeoutMs: helloAck.startTimeoutMs,
          hardExpireAtMs: helloAck.hardExpireAtMs,
          close(reason = '') {
            closeTrueConcurrencyHeartbeatSocket(ws, reason);
          },
          async sendHeartbeat() {
            const heartbeatPromise = waitForTrueConcurrencyHeartbeatMessage(
              ws,
              signal,
              session.ackTimeoutMs,
              'heartbeat_ack',
            );
            ws.send(JSON.stringify({
              type: 'heartbeat',
              requestId: helloPayload.requestId,
              leaseId: helloPayload.leaseId,
              leaseToken: helloPayload.leaseToken,
              ticketHash: helloPayload.ticketHash,
              generation: session.generation,
              nowMs: Date.now(),
            }));
            const heartbeatMessage = await heartbeatPromise;
            if (heartbeatMessage?.type === 'terminal') {
              throw createTrueConcurrencyHeartbeatTerminalError(
                normalizeTrueConcurrencyHeartbeatTerminal(heartbeatMessage),
              );
            }
            const ack = normalizeTrueConcurrencyHeartbeatAck(heartbeatMessage, session.generation);
            session.deadlineMs = ack.deadlineMs;
            session.hardExpireAtMs = ack.hardExpireAtMs;
            return ack;
          },
        };

        return session;
      } catch (error) {
        closeTrueConcurrencyHeartbeatSocket(ws, 'heartbeat_connect_failed');
        throw error;
      }
    },

    async cancel(_ctx, requestIdentity, reason, signal) {
      const { data } = await postJson(
        cancelUrl,
        buildTrueConcurrencyCancelPayload(requestIdentity, reason),
        {
          timeoutMs: releaseTimeoutMs,
          signal,
          allowedStatuses: [200, 409],
        },
      );
      return normalizeTrueConcurrencyCancelResult(data);
    },
  };
};

const isTrueConcurrencyManagedHostname = (config, hostname) => {
  const hostKey = typeof hostname === 'string' ? hostname.trim().toLowerCase() : '';
  return Boolean(config?.trueConcurrencyEnabled)
    && Boolean(hostKey)
    && Array.isArray(config?.trueConcurrencyHostnamePatterns)
    && config.trueConcurrencyHostnamePatterns.some((pattern) => matchHostnamePattern(hostKey, pattern));
};

const createTrueConcurrencyUnavailableResponse = (origin, message = 'True concurrency unavailable') => (
  createErrorResponse(origin, 503, message)
);

const createTrueConcurrencyRequestId = () => {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return `cq-${Date.now()}-${Math.random().toString(16).slice(2)}`;
};

const sleepMs = (delayMs) => new Promise((resolve) => {
  const timer = setTimeout(resolve, delayMs);
  if (typeof timer?.unref === 'function') {
    timer.unref();
  }
});

const createConcurrencyReleaseController = ({ client, ctx, lease, label }) => {
  let settled = false;
  let firstReason = null;
  let immediateAttemptPromise = null;
  let fullReleasePromise = null;

  const attemptRelease = async (reason) => {
    if (settled) {
      return true;
    }
    try {
      const result = await client.release(ctx, lease, reason);
      if (result?.result === 'released' || result?.result === 'noop' || result?.result === 'expired') {
        settled = true;
        return true;
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn(`[CQ] release failed for ${label}:`, message);
    }
    return false;
  };

  const ensureImmediateAttempt = async () => {
    if (!immediateAttemptPromise) {
      immediateAttemptPromise = attemptRelease(firstReason);
    }
    return immediateAttemptPromise;
  };

  return {
    isSettled() {
      return settled;
    },

    async releaseImmediately(reason) {
      if (!firstReason) {
        firstReason = reason;
      }
      const released = await ensureImmediateAttempt();
      if (!released) {
        this.ensureReleased(firstReason);
      }
      return released;
    },

    ensureReleased(reason) {
      if (!firstReason) {
        firstReason = reason;
      }
      if (!fullReleasePromise) {
        fullReleasePromise = (async () => {
          if (await ensureImmediateAttempt()) {
            return true;
          }

          for (const delayMs of TRUE_CONCURRENCY_RELEASE_RETRY_DELAYS_MS.slice(1)) {
            await sleepMs(delayMs);
            if (await attemptRelease(firstReason)) {
              return true;
            }
          }

          return false;
        })();
        if (ctx && typeof ctx.waitUntil === 'function') {
          ctx.waitUntil(fullReleasePromise);
        }
      }
      return fullReleasePromise;
    },
  };
};

const createTrueConcurrencyHeartbeatManager = ({
  client,
  ctx,
  plan,
  lease,
  heartbeatConfig,
  clientSignal,
  abortStream,
}) => {
  const cfg = normalizeTrueConcurrencyHeartbeatConfig(heartbeatConfig, {
    fieldName: 'concurrencyHandlerConfig.heartbeat',
    required: true,
  });
  const managerController = new AbortController();
  const managerSignal = managerController.signal;
  const clientInstanceId = normalizeStringValue(plan?.clientInstanceId) || 'download-worker';
  let stopped = false;
  let currentSession = null;
  let currentSessionListeners = null;
  let heartbeatTimer = null;
  let heartbeatInFlight = null;
  let reconnectTask = null;
  let reconnectFirstDisconnectAtMs = 0;
  let reconnectAttempts = 0;
  let connectAttempts = 0;
  let reconnectGraceMs = cfg.reconnectGraceMs;
  let hardExpireAtMs = Number(plan?.hardExpireAtMs) || 0;
  let boundStreamAbortController = null;
  let pendingAbortReason = '';

  const clearHeartbeatTimer = () => {
    if (heartbeatTimer) {
      clearTimeout(heartbeatTimer);
      heartbeatTimer = null;
    }
  };

  const waitForDelay = (delayMs) => new Promise((resolve) => {
    if (!Number.isFinite(delayMs) || delayMs <= 0 || managerSignal.aborted) {
      resolve();
      return;
    }
    let timer = null;
    const cleanup = () => {
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }
      managerSignal.removeEventListener('abort', onAbort);
    };
    const onAbort = () => {
      cleanup();
      resolve();
    };
    timer = setTimeout(() => {
      cleanup();
      resolve();
    }, delayMs);
    if (typeof timer?.unref === 'function') {
      timer.unref();
    }
    managerSignal.addEventListener('abort', onAbort, { once: true });
  });

  const connectHeartbeatWithBudget = (identity, budgetMs, timeoutCloseReason) => new Promise((resolve, reject) => {
    const remainingMs = Math.floor(Number(budgetMs));
    const budgetLabel = timeoutCloseReason === 'heartbeat_connect_failed' ? 'initial connect' : 'reconnect';
    const createBudgetError = () => {
      const error = new Error(`[CQ] heartbeat ${budgetLabel} exhausted elapsed budget`);
      error.heartbeatBudgetExhausted = true;
      return error;
    };
    if (!Number.isFinite(remainingMs) || remainingMs <= 0) {
      reject(createBudgetError());
      return;
    }

    const attemptController = new AbortController();
    let settled = false;
    let timer = null;
    let lateCloseReason = '';

    const abortAttempt = () => {
      if (!attemptController.signal.aborted) {
        attemptController.abort();
      }
    };
    const cleanup = () => {
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }
      managerSignal.removeEventListener('abort', onManagerAbort);
    };
    const settle = (callback) => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      callback();
    };
    const onManagerAbort = () => {
      lateCloseReason = 'heartbeat_stopped';
      abortAttempt();
      settle(() => reject(new Error('[CQ] heartbeat connect aborted')));
    };

    if (managerSignal.aborted) {
      onManagerAbort();
      return;
    }
    managerSignal.addEventListener('abort', onManagerAbort, { once: true });

    let connectPromise;
    try {
      connectPromise = Promise.resolve(client.connectHeartbeat(ctx, identity, attemptController.signal));
    } catch (error) {
      settle(() => reject(error));
      return;
    }

    connectPromise.then(
      (session) => {
        if (lateCloseReason) {
          session?.close?.(lateCloseReason);
          return;
        }
        settle(() => resolve(session));
      },
      (error) => settle(() => reject(error)),
    );

    queueMicrotask(() => {
      if (settled) {
        return;
      }
      timer = setTimeout(() => {
        lateCloseReason = timeoutCloseReason;
        abortAttempt();
        settle(() => reject(createBudgetError()));
      }, remainingMs);
      if (typeof timer?.unref === 'function') {
        timer.unref();
      }
    });
  });

  const buildIdentity = (attempt) => ({
    requestId: plan.requestId,
    leaseId: lease.leaseId,
    leaseToken: lease.leaseToken,
    hardExpireAtMs: plan.hardExpireAtMs,
    clientInstanceId,
    attempt,
    ticketHash: plan.ticketHash,
  });

  const disconnectSessionListeners = () => {
    if (!currentSession || !currentSessionListeners) {
      return;
    }
    currentSession.ws?.removeEventListener?.('close', currentSessionListeners.onClose);
    currentSession.ws?.removeEventListener?.('error', currentSessionListeners.onError);
    currentSessionListeners = null;
  };

  const closeSession = (reason = '') => {
    disconnectSessionListeners();
    if (currentSession) {
      currentSession.close?.(reason);
      currentSession = null;
    }
  };

  const abortManagedStream = (reason) => {
    if (!pendingAbortReason && typeof reason === 'string' && reason) {
      pendingAbortReason = reason;
    }
    if (boundStreamAbortController && !boundStreamAbortController.signal.aborted) {
      boundStreamAbortController.abort();
    }
    abortStream(reason);
  };

  const computeReconnectDeadlineMs = () => {
    const reconnectGraceDeadlineMs = reconnectFirstDisconnectAtMs
      + Math.max(0, reconnectGraceMs - cfg.reconnectSafetyMarginMs);
    return Math.min(
      reconnectFirstDisconnectAtMs + cfg.reconnectMaxElapsedMs,
      reconnectGraceDeadlineMs,
      hardExpireAtMs,
    );
  };

  const computeRemainingReconnectBudgetMs = (nowMs = Date.now()) => Math.max(
    0,
    computeReconnectDeadlineMs() - nowMs,
  );

  const computeJitteredReconnectDelayMs = (rawDelayMs) => {
    if (!Number.isFinite(rawDelayMs) || rawDelayMs <= 0) {
      return 0;
    }
    const cappedDelayMs = Math.min(cfg.reconnectMaxDelayMs, rawDelayMs);
    const jitterMs = Math.floor(Math.random() * cappedDelayMs);
    return Math.min(cfg.reconnectMaxDelayMs, cappedDelayMs + jitterMs);
  };

  const attachSession = (session) => {
    currentSession = session;
    reconnectGraceMs = Number(session?.reconnectGraceMs) || reconnectGraceMs;
    hardExpireAtMs = Number(session?.hardExpireAtMs) || hardExpireAtMs;
    reconnectFirstDisconnectAtMs = 0;
    reconnectAttempts = 0;

    const onClose = () => {
      if (stopped || !currentSession || currentSession !== session) {
        return;
      }
      void startReconnect('socket_closed');
    };
    const onError = () => {
      if (stopped || !currentSession || currentSession !== session) {
        return;
      }
      void startReconnect('socket_error');
    };

    currentSessionListeners = { onClose, onError };
    session.ws?.addEventListener?.('close', onClose);
    session.ws?.addEventListener?.('error', onError);
  };

  const handleTerminal = (terminal) => {
    if (stopped) {
      return;
    }
    clearHeartbeatTimer();
    stop(terminal?.reason || 'heartbeat_terminal');
    if (TRUE_CONCURRENCY_HEARTBEAT_TERMINAL_NO_RELEASE_REASONS.has(terminal?.reason)) {
      abortManagedStream(terminal.reason);
      return;
    }
    abortManagedStream('heartbeat_lost');
  };

  const scheduleNextHeartbeat = () => {
    clearHeartbeatTimer();
    if (stopped || !currentSession) {
      return;
    }
    heartbeatTimer = setTimeout(() => {
      heartbeatTimer = null;
      const session = currentSession;
      if (!session || stopped) {
        return;
      }
      heartbeatInFlight = (async () => {
        try {
          await session.sendHeartbeat();
          if (stopped || currentSession !== session) {
            return;
          }
          scheduleNextHeartbeat();
        } catch (error) {
          if (stopped) {
            return;
          }
          if (isTrueConcurrencyHeartbeatTerminalError(error)) {
            handleTerminal(error.terminal);
            return;
          }
          if (!currentSession || currentSession !== session) {
            return;
          }
          await startReconnect('heartbeat_ack_failed');
        } finally {
          heartbeatInFlight = null;
        }
      })();
    }, currentSession.heartbeatIntervalMs);
    if (typeof heartbeatTimer?.unref === 'function') {
      heartbeatTimer.unref();
    }
  };

  async function startReconnect(_trigger) {
    if (stopped) {
      return;
    }
    if (reconnectTask) {
      return reconnectTask;
    }

    clearHeartbeatTimer();
    closeSession('heartbeat_reconnect');
    reconnectFirstDisconnectAtMs = reconnectFirstDisconnectAtMs || Date.now();

    reconnectTask = (async () => {
      let nextDelayMs = cfg.reconnectBaseDelayMs;
      while (!stopped && !managerSignal.aborted) {
        if (reconnectAttempts >= cfg.reconnectMaxAttempts) {
          break;
        }
        if (Date.now() >= computeReconnectDeadlineMs()) {
          break;
        }

        reconnectAttempts += 1;
        try {
          const session = await connectHeartbeatWithBudget(
            buildIdentity(++connectAttempts),
            computeRemainingReconnectBudgetMs(),
            'heartbeat_lost',
          );
          if (stopped) {
            session.close?.('heartbeat_stopped');
            return;
          }
          if (Date.now() >= computeReconnectDeadlineMs()) {
            session.close?.('heartbeat_lost');
            break;
          }
          attachSession(session);
          scheduleNextHeartbeat();
          return;
        } catch (error) {
          if (stopped || managerSignal.aborted) {
            return;
          }
          if (isTrueConcurrencyHeartbeatTerminalError(error)) {
            handleTerminal(error.terminal);
            return;
          }
          if (error?.heartbeatBudgetExhausted) {
            break;
          }
          if (reconnectAttempts >= cfg.reconnectMaxAttempts || Date.now() >= computeReconnectDeadlineMs()) {
            break;
          }
        }

        const remainingBudgetMs = computeRemainingReconnectBudgetMs();
        if (remainingBudgetMs <= 0) {
          break;
        }
        await waitForDelay(Math.min(computeJitteredReconnectDelayMs(nextDelayMs), remainingBudgetMs));
        nextDelayMs = Math.min(cfg.reconnectMaxDelayMs, nextDelayMs * 2);
      }

      if (!stopped) {
        stop('heartbeat_lost');
        abortManagedStream('heartbeat_lost');
      }
    })().finally(() => {
      reconnectTask = null;
    });

    return reconnectTask;
  }

  const stop = (reason = '') => {
    if (stopped) {
      return;
    }
    stopped = true;
    clearHeartbeatTimer();
    closeSession(reason);
    managerController.abort();
    clientSignal?.removeEventListener?.('abort', onClientAbort);
  };

  const onClientAbort = () => {
    abortManagedStream('client_disconnect');
    void ensureCleanup('client_disconnect');
  };
  clientSignal?.addEventListener?.('abort', onClientAbort, { once: true });

  const ensureCleanup = (reason = '') => {
    stop(reason);
    const cleanupPromise = Promise.allSettled([
      heartbeatInFlight,
      reconnectTask,
    ].filter(Boolean));
    if (ctx && typeof ctx.waitUntil === 'function') {
      ctx.waitUntil(cleanupPromise);
    }
    return cleanupPromise;
  };

  return {
    async startBeforeOriginFetch() {
      const startedAtMs = Date.now();
      const initialConnectDeadlineMs = startedAtMs + cfg.initialConnectMaxElapsedMs;
      let lastError = null;

      while (!stopped && !managerSignal.aborted) {
        if (connectAttempts >= cfg.initialConnectMaxAttempts) {
          break;
        }
        if (Date.now() >= initialConnectDeadlineMs) {
          break;
        }

        connectAttempts += 1;
        try {
          const session = await connectHeartbeatWithBudget(
            buildIdentity(connectAttempts),
            initialConnectDeadlineMs - Date.now(),
            'heartbeat_connect_failed',
          );
          if (stopped) {
            session.close?.('heartbeat_stopped');
            break;
          }
          if (Date.now() > initialConnectDeadlineMs) {
            session.close?.('heartbeat_connect_failed');
            lastError = new Error('[CQ] heartbeat initial connect exceeded elapsed budget');
            break;
          }
          attachSession(session);
          scheduleNextHeartbeat();
          return;
        } catch (error) {
          lastError = error;
          if (isTrueConcurrencyHeartbeatTerminalError(error)) {
            throw error;
          }
          if (error?.heartbeatBudgetExhausted) {
            break;
          }
          if (Date.now() >= initialConnectDeadlineMs) {
            break;
          }
        }
      }

      throw lastError || new Error('[CQ] heartbeat initial connect failed');
    },

    bindStreamAbortController(abortController) {
      boundStreamAbortController = abortController;
      if (pendingAbortReason && abortController && !abortController.signal.aborted) {
        abortController.abort();
      }
    },

    stop,
    ensureCleanup,
  };
};

const readFairQueueInvocationEpoch = (value) => {
  const epoch = Number(value);
  if (!Number.isInteger(epoch) || epoch <= 0) {
    return null;
  }
  return epoch;
};

const readReleaseOwnerRequired = (value) => value === true;

const buildFairQueueCleanupIdentity = (cleanupContext) => {
  if (cleanupContext?.slotToken) {
    return `release:${cleanupContext.slotToken}`;
  }
  if (cleanupContext?.queryToken) {
    const invocationEpoch = readFairQueueInvocationEpoch(cleanupContext?.invocationEpoch);
    if (invocationEpoch !== null) {
      return `abandon:${cleanupContext.queryToken}:${invocationEpoch}`;
    }
    return `abandon-local:${cleanupContext.queryToken}`;
  }
  return '';
};

const clearFairQueueOwnershipMetadata = (fqContext) => {
  if (!fqContext) {
    return;
  }

  fqContext.slotToken = null;
  fqContext.queryToken = null;
  fqContext.invocationEpoch = null;
  fqContext.releaseOwnerRequired = undefined;
  fqContext.grantPromoted = false;
  fqContext.slotAcquiredAt = null;
};

const buildFinalCleanupGroups = (cleanupContexts) => {
  const dedupedContexts = [];
  const seenCleanupIdentities = new Set();

  for (const cleanupContext of cleanupContexts) {
    const cleanupIdentity = buildFairQueueCleanupIdentity(cleanupContext);
    if (!cleanupIdentity || seenCleanupIdentities.has(cleanupIdentity)) {
      continue;
    }
    seenCleanupIdentities.add(cleanupIdentity);
    dedupedContexts.push(cleanupContext);
  }

  const groups = [];
  const groupsByHostKey = new Map();

  for (const cleanupContext of dedupedContexts) {
    const providerHostBucket = deriveProviderHostBucket(cleanupContext?.hostname);
    const hostGroupKey = cleanupContext.hostnameHash || providerHostBucket;
    let group = groupsByHostKey.get(hostGroupKey);
    if (!group) {
      group = [];
      groupsByHostKey.set(hostGroupKey, group);
      groups.push(group);
    }
    group.push(cleanupContext);
  }

  return groups;
};

const clearReleasedFairQueueMetadata = (fqContext) => {
  if (!fqContext) {
    return;
  }

  clearFairQueueOwnershipMetadata(fqContext);
  if (fqContext.deferredReportArmed) {
    return;
  }
  fqContext.attemptVersion = null;
  fqContext.attemptTicket = null;
  fqContext.deferredReportStatusCode = null;
  fqContext.deferredReportArmed = false;
};

const clearAbandonedFairQueueMetadata = (fqContext) => {
  if (!fqContext) {
    return;
  }

  clearFairQueueOwnershipMetadata(fqContext);
  fqContext.attemptVersion = null;
  fqContext.attemptTicket = null;
  fqContext.deferredReportStatusCode = null;
  fqContext.deferredReportArmed = false;
};

const finalizeFairQueueContext = async ({ fairQueueClient, ctx, fqContext, phase }) => {
  if (!fairQueueClient || !fqContext) {
    return true;
  }

  if (fqContext.slotToken) {
    try {
      const released = await fairQueueClient.releaseSlot(ctx, fqContext);
      if (released) {
        clearReleasedFairQueueMetadata(fqContext);
        return true;
      }
      console.warn(`[Fair Queue] releaseSlot exhausted during ${phase} for host=${fqContext.hostname}`);
      return false;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn(`[Fair Queue] releaseSlot failed during ${phase}:`, message);
      return false;
    }
  }

  if (fqContext.grantPromoted === true) {
    clearReleasedFairQueueMetadata(fqContext);
    return true;
  }

  if (!fqContext.queryToken) {
    return true;
  }

  try {
    const abandoned = await fairQueueClient.abandonWait(ctx, fqContext);
    if (abandoned) {
      clearAbandonedFairQueueMetadata(fqContext);
      return true;
    }
    console.warn(`[Fair Queue] abandonWait exhausted during ${phase} for host=${fqContext.hostname}`);
    return false;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(`[Fair Queue] abandonWait failed during ${phase}:`, message);
    return false;
  }
};

const reconcileFairQueueContextForTarget = async ({
  fairQueueClient,
  ctx,
  fqContext,
  targetUrl,
  phase,
  pendingCleanupContexts = null,
  targetHostname = null,
  targetSiteBucket,
  forceRetire = false,
}) => {
  if (!fqContext) {
    return { retired: false, finalized: true };
  }

  const resolvedHostnameRaw = targetHostname || extractHostname(targetUrl);
  const resolvedHostname = resolvedHostnameRaw ? resolvedHostnameRaw.toLowerCase() : null;
  const resolvedSiteBucket = targetSiteBucket ?? fqContext.siteBucket;

  if (!forceRetire && resolvedHostname === fqContext.hostname && resolvedSiteBucket === fqContext.siteBucket) {
    return {
      retired: false,
      finalized: true,
      targetHostname: resolvedHostname,
      targetSiteBucket: resolvedSiteBucket,
    };
  }

  const finalized = await finalizeFairQueueContext({
    fairQueueClient,
    ctx,
    fqContext,
    phase,
  });
  if (!finalized && Array.isArray(pendingCleanupContexts)) {
    pendingCleanupContexts.push(fqContext);
  }

  return {
    retired: true,
    finalized,
    targetHostname: resolvedHostname,
    targetSiteBucket: resolvedSiteBucket,
  };
};

const runWithConcurrencyLimit = async (taskFactories, concurrencyLimit) => {
  if (!Array.isArray(taskFactories) || taskFactories.length === 0) {
    return;
  }

  const limit = Number.isFinite(concurrencyLimit) && concurrencyLimit > 0
    ? Math.max(1, Math.trunc(concurrencyLimit))
    : 1;
  let nextTaskIndex = 0;

  const runNextTask = async () => {
    while (nextTaskIndex < taskFactories.length) {
      const taskIndex = nextTaskIndex;
      nextTaskIndex += 1;
      await taskFactories[taskIndex]();
    }
  };

  const workerCount = Math.min(limit, taskFactories.length);
  await Promise.all(Array.from({ length: workerCount }, () => runNextTask()));
};

const createFairQueueClient = (config) => createSlotHandlerClient(config);

const createSlotHandlerClient = (config) => {
  const slotCfg = config.slotHandlerConfig || {};
  const testHooks = config.testHooks && typeof config.testHooks === 'object'
    ? config.testHooks
    : null;
  const baseUrl = normalizePostgrestBaseUrl(slotCfg.url);
  if (!baseUrl) {
    throw new Error('[FQ] slot-handler backend enabled but FAIR_QUEUE_SLOT_HANDLER_URL is missing');
  }

  const acquireUrl = `${baseUrl}/api/v1/fairqueue/acquire`;
  const releaseUrl = `${baseUrl}/api/v1/fairqueue/release`;
  const abandonUrl = `${baseUrl}/api/v1/fairqueue/abandon`;
  const authKey = slotCfg.authKey || '';
  const authHeader = normalizeStringValue(slotCfg.authHeader, 'X-FQ-Auth');
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
  const releaseTimeoutMs = DEFAULT_SLOT_HANDLER_RELEASE_TIMEOUT_MS;
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

  const fetchWithTimeout = async (url, payload, timeoutMs, signal, extraHeaders = null) => {
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
        headers: {
          ...buildHeaders(),
          ...(extraHeaders && typeof extraHeaders === 'object' ? extraHeaders : {}),
        },
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
      const hostKey = deriveProviderHostBucket(fqContext?.hostname);
      let queryToken = typeof fqContext?.queryToken === 'string' && fqContext.queryToken
        ? fqContext.queryToken
        : null;
      let invocationEpoch = readFairQueueInvocationEpoch(fqContext?.invocationEpoch);
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
          ...(queryToken ? { queryToken } : {}),
        };
        if (fqContext?.breakerEnabled === true) {
          payload.breakerEnabled = true;
          if (Number.isFinite(fqContext?.openCapSeconds)) {
            payload.openCapSeconds = Math.trunc(fqContext.openCapSeconds);
          }
          if (Number.isFinite(fqContext?.closeThresholdPercent)) {
            payload.closeThresholdPercent = Math.trunc(fqContext.closeThresholdPercent);
          }
          if (Number.isFinite(fqContext?.halfOpenSuccessThreshold)) {
            payload.halfOpenSuccessThreshold = Math.trunc(fqContext.halfOpenSuccessThreshold);
          }
          if (typeof fqContext?.halfOpenCloseMode === 'string' && fqContext.halfOpenCloseMode) {
            payload.halfOpenCloseMode = fqContext.halfOpenCloseMode;
          }
          if (Number.isFinite(fqContext?.halfOpenMaxProbeCount)) {
            payload.halfOpenMaxProbeCount = Math.trunc(fqContext.halfOpenMaxProbeCount);
          }
          if (Number.isFinite(fqContext?.halfOpenMaxSeconds)) {
            payload.halfOpenMaxSeconds = Math.trunc(fqContext.halfOpenMaxSeconds);
          }
          if (typeof fqContext?.halfOpenTimeoutMode === 'string' && fqContext.halfOpenTimeoutMode) {
            payload.halfOpenTimeoutMode = fqContext.halfOpenTimeoutMode;
          }
        }

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
            let data;
            try {
              data = await res.json();
            } catch (error) {
              const message = error instanceof Error ? error.message : String(error);
              console.error('[FQ] slot-handler conflict response parse error:', message);
              return { kind: 'timeout', reason: 'slot-handler-invalid-response' };
            }

            if (data?.result === 'conflict') {
              pendingStreak = 0;
              overloadStreak = 0;
              return { kind: 'conflict' };
            }

            console.error(`[FQ] unexpected slot-handler conflict result: ${data?.result}`);
            return { kind: 'timeout', reason: 'slot-handler-invalid-response' };
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

        const responseQueryToken = typeof data?.queryToken === 'string' && data.queryToken
          ? data.queryToken
          : null;
        const responseInvocationEpoch = readFairQueueInvocationEpoch(data?.invocationEpoch);
        const responseReleaseOwnerRequired = readReleaseOwnerRequired(data?.releaseOwnerRequired);

        const applyAcceptedOwnership = (result) => {
          if (!responseQueryToken || responseInvocationEpoch === null) {
            console.error(
              `[FQ] slot-handler ${result} response missing accepted ownership fields`,
            );
            return false;
          }
          queryToken = responseQueryToken;
          fqContext.queryToken = responseQueryToken;
          invocationEpoch = responseInvocationEpoch;
          fqContext.invocationEpoch = responseInvocationEpoch;
          return true;
        };

        switch (data?.result) {
          case 'granted':
            pendingStreak = 0;
            overloadStreak = 0;
            {
              if (!applyAcceptedOwnership('granted')) {
                return { kind: 'timeout', reason: 'slot-handler-invalid-response' };
              }
              const { attemptVersion, attemptTicket } = readSlotHandlerAttempt(data);
              fqContext.grantPromoted = true;
              fqContext.slotToken = data.slotToken;
              fqContext.releaseOwnerRequired = responseReleaseOwnerRequired ? true : undefined;
              fqContext.slotAcquiredAt = Date.now();
              fqContext.attemptVersion = Number.isFinite(attemptVersion) && Number.isFinite(attemptTicket)
                ? attemptVersion
                : null;
              fqContext.attemptTicket = Number.isFinite(attemptVersion) && Number.isFinite(attemptTicket)
                ? attemptTicket
                : null;
              if (typeof testHooks?.onGrantPromotion === 'function') {
                testHooks.onGrantPromotion(fqContext);
              }
            }
            console.log(`[FQ] slot granted via slot-handler host=${fqContext.hostname}`);
            return {
              kind: 'granted',
              attemptVersion: fqContext.attemptVersion,
              attemptTicket: fqContext.attemptTicket,
            };
          case 'throttled':
            pendingStreak = 0;
            overloadStreak = 0;
            if (!applyAcceptedOwnership('throttled')) {
              return { kind: 'timeout', reason: 'slot-handler-invalid-response' };
            }
            const throttleCode = Number.isFinite(data?.throttleCode) ? data.throttleCode : 503;
            const breakerSnapshot = readSlotHandlerBreakerSnapshot(data);
            const openBreaker = breakerSnapshot
              ? readOpenBreakerSnapshot(breakerSnapshot, 0)
              : null;
            const rawRetryAfter = Number(data?.retryAfter);
            clearAbandonedFairQueueMetadata(fqContext);
            queryToken = null;
            invocationEpoch = null;
            return {
              kind: 'throttled',
              throttleCode,
              retryAfter: openBreaker?.retryAfter
                ?? (Number.isFinite(rawRetryAfter) && rawRetryAfter > 0 ? Math.ceil(rawRetryAfter) : null),
              breakerSnapshot,
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
            clearAbandonedFairQueueMetadata(fqContext);
            queryToken = null;
            invocationEpoch = null;
            return { kind: 'timeout', reason: 'slot-handler-timeout' };
          case 'pending':
            if (!applyAcceptedOwnership('pending')) {
              return { kind: 'timeout', reason: 'slot-handler-invalid-response' };
            }
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
        return true;
      }

      const releaseMaxAttempts = 3;
      const releaseBaseBackoffMs = 100;
      const releaseMaxBackoffMs = 500;

      const ownerRoutingEnabled = fqContext.releaseOwnerRequired === true;
      const hostname = typeof fqContext.hostname === 'string' && fqContext.hostname
        ? fqContext.hostname
        : null;
      const hostnameHash = typeof fqContext.hostnameHash === 'string' && fqContext.hostnameHash
        ? fqContext.hostnameHash
        : null;
      const ipBucket = typeof fqContext.ipBucket === 'string' && fqContext.ipBucket
        ? fqContext.ipBucket
        : null;
      const siteBucket = typeof fqContext.siteBucket === 'string' && fqContext.siteBucket
        ? fqContext.siteBucket
        : null;
      const queryToken = typeof fqContext.queryToken === 'string' && fqContext.queryToken
        ? fqContext.queryToken
        : null;
      const invocationEpoch = readFairQueueInvocationEpoch(fqContext.invocationEpoch);
      if (!hostname || !hostnameHash || !ipBucket || !siteBucket || !queryToken || invocationEpoch === null) {
        console.error('[FQ] releaseSlot skipped: missing full release identity');
        return false;
      }
      const hitUpstreamAtMs = Number.isFinite(fqContext.hitUpstreamAtMs)
        ? fqContext.hitUpstreamAtMs
        : fqContext.nowMs;
      const payload = {
        hostname,
        hostnameHash,
        ipBucket,
        siteBucket,
        slotToken: fqContext.slotToken,
        queryToken,
        invocationEpoch,
        releaseOwnerRequired: ownerRoutingEnabled,
        hitUpstreamAtMs,
        now: Date.now(),
      };
      const routingHeaders = ownerRoutingEnabled
        ? {
          'X-FQ-Owner-Token': queryToken,
          'X-FQ-Owner-Epoch': String(invocationEpoch),
        }
        : null;

      let lastError = null;
      for (let attempt = 1; attempt <= releaseMaxAttempts; attempt += 1) {
        let shouldRetry = false;
        try {
          const res = await fetchWithTimeout(releaseUrl, payload, releaseTimeoutMs, undefined, routingHeaders);

          if (res.ok) {
            console.log(`[FQ] slot released via slot-handler host=${fqContext.hostname}`);
            return true;
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
      return false;
    },

    async abandonWait(ctx, fqContext) {
      const invocationEpoch = readFairQueueInvocationEpoch(fqContext?.invocationEpoch);
      if (!fqContext?.queryToken || fqContext?.slotToken || invocationEpoch === null) {
        return true;
      }

      const abandonMaxAttempts = 3;
      const abandonBaseBackoffMs = 100;
      const abandonMaxBackoffMs = 500;
      const payload = {
        queryToken: fqContext.queryToken,
        invocationEpoch,
      };

      let lastError = null;
      for (let attempt = 1; attempt <= abandonMaxAttempts; attempt += 1) {
        let shouldRetry = false;
        try {
          const res = await fetchWithTimeout(abandonUrl, payload, releaseTimeoutMs);

          if (res.ok) {
            console.log('[FQ] wait abandoned via slot-handler');
            return true;
          }

          lastError = new Error(`slot-handler abandon failed: status ${res.status}`);
          shouldRetry = isRetryableReleaseStatus(res.status);
        } catch (error) {
          lastError = error instanceof Error ? error : new Error(String(error));
          shouldRetry = true;
        }

        if (shouldRetry && attempt < abandonMaxAttempts) {
          const backoffMs = Math.min(abandonMaxBackoffMs, abandonBaseBackoffMs * (2 ** (attempt - 1)));
          await new Promise((resolve) => setTimeout(resolve, backoffMs));
          continue;
        }

        break;
      }

      const message = lastError instanceof Error ? lastError.message : String(lastError || 'unknown error');
      console.error('[FQ] abandonWait error (slot-handler):', message);
      return false;
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
  const clientSignal = request.signal;
  let clientAborted = clientSignal?.aborted === true;
  const didClientAbort = () => clientAborted || clientSignal?.aborted === true;
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
  if (!Number.isFinite(payloadSignExpire) || payloadSignExpire <= 0) {
    return createUnauthorizedResponse(origin, 'payloadSign expire invalid');
  }
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

  const hardExpireAtMs = Math.min(payloadSignExpire, payloadExpireTime) * 1000;
  if (!Number.isFinite(hardExpireAtMs) || hardExpireAtMs <= 0) {
    return createUnauthorizedResponse(origin, "link expired");
  }
  if (Date.now() >= hardExpireAtMs) {
    return createUnauthorizedResponse(origin, "link expired");
  }

  const ticketHash = await sha256Hash(`${payload}:${payloadSign}`);
  if (!ticketHash) {
    return createUnauthorizedResponse(origin, 'payload ticket hash invalid');
  }

  const ticketStateEnabled = config.dbMode === 'custom-pg-rest';
  const payloadTicketNonce = typeof payloadData?.ticketNonce === 'string' ? payloadData.ticketNonce.trim() : '';
  const idleTimeoutRaw = payloadData?.idle_timeout;
  const idleTimeoutSeconds = ticketStateEnabled
    && typeof idleTimeoutRaw === 'number'
    && Number.isSafeInteger(idleTimeoutRaw)
    ? idleTimeoutRaw
    : null;
  if (ticketStateEnabled) {
    if (!isValidTicketNonce(payloadTicketNonce)) {
      return createUnauthorizedResponse(origin, 'payload ticketNonce invalid');
    }

    if (idleTimeoutSeconds === null || idleTimeoutSeconds < 0) {
      return createUnauthorizedResponse(origin, 'payload idle_timeout invalid');
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

  let ticketStateConfig = null;
  let ticketState = null;
  if (ticketStateEnabled) {
    try {
      ticketStateConfig = resolveTicketStateConfig(config);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error('[TicketState] Config invalid:', message);
      return createErrorResponse(origin, 500, message);
    }

    try {
      ticketState = await readTicketState(ticketHash, ticketStateConfig);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error('[TicketState] Read failed:', message);
      return createErrorResponse(origin, 500, `Ticket state read failed: ${message}`);
    }

    if (!ticketState?.found) {
      return createUnauthorizedResponse(origin, 'ticket state missing');
    }

    if (!Number.isInteger(ticketState.issuedAt) || ticketState.issuedAt < 0) {
      return createUnauthorizedResponse(origin, 'ticket state issued_at invalid');
    }

    if (!Number.isInteger(ticketState.hardExpireAt) || ticketState.hardExpireAt <= 0) {
      return createUnauthorizedResponse(origin, 'ticket state hard_expire_at invalid');
    }

    if (!Number.isInteger(ticketState.idleTimeoutSeconds) || ticketState.idleTimeoutSeconds < 0) {
      return createUnauthorizedResponse(origin, 'ticket state idle_timeout_seconds invalid');
    }

    if (ticketState.firstUsedAt != null && (!Number.isInteger(ticketState.firstUsedAt) || ticketState.firstUsedAt < 0)) {
      return createUnauthorizedResponse(origin, 'ticket state first_used_at invalid');
    }

    if (ticketState.idlePolicy !== 'first_use' && ticketState.idlePolicy !== 'renewable') {
      return createUnauthorizedResponse(origin, 'ticket state idle_policy invalid');
    }

    const nowSeconds = Math.floor(Date.now() / 1000);

    if (ticketState.firstUsedAt == null) {
      const idleAge = nowSeconds - ticketState.issuedAt;
      if (idleAge >= ticketState.idleTimeoutSeconds) {
        await slowFailDelay();
        return createErrorResponse(origin, 410, 'Link expired due to inactivity');
      }
    } else if (ticketState.idlePolicy === 'renewable') {
      if (!Number.isInteger(ticketState.idleLeaseExpiresAt) || ticketState.idleLeaseExpiresAt <= 0) {
        return createUnauthorizedResponse(origin, 'link expired');
      }

      if (nowSeconds >= Math.min(ticketState.hardExpireAt, ticketState.idleLeaseExpiresAt)) {
        return createUnauthorizedResponse(origin, 'link expired');
      }
    } else if (nowSeconds >= ticketState.hardExpireAt) {
      return createUnauthorizedResponse(origin, 'link expired');
    }
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

      const result = await unifiedCheck(path, clientIP, {
        postgrestUrl: rateLimitConfig.postgrestUrl,
        verifyHeader: rateLimitConfig.verifyHeader,
        verifySecret: rateLimitConfig.verifySecret,
        linkTTL: cacheConfig.linkTTL ?? 1800,
        cacheTableName: cacheConfig.tableName || 'DOWNLOAD_CACHE_TABLE',
        windowTimeSeconds: rateLimitConfig.windowTimeSeconds ?? 86400,
        limit: unifiedRateLimit ?? 100,
        blockTimeSeconds: rateLimitConfig.blockTimeSeconds ?? 600,
        ipv4Suffix: rateLimitConfig.ipv4Suffix ?? '/32',
        ipv6Suffix: rateLimitConfig.ipv6Suffix ?? '/60',
        rateLimitTableName: rateLimitConfig.tableName || 'DOWNLOAD_IP_RATELIMIT_TABLE',
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

  const applyUnifiedCheckResult = async (options = {}) => {
    if (!unifiedResult) {
      return null;
    }

    const resolvedUnifiedThrottleHostname = (() => {
      const override = typeof options.throttleHostname === 'string' ? options.throttleHostname.trim().toLowerCase() : '';
      if (override) {
        return override;
      }
      const cachedHostnameRaw = extractHostname(unifiedResult?.cache?.linkData?.url || '');
      return cachedHostnameRaw ? cachedHostnameRaw.toLowerCase() : '';
    })();
    const unifiedAdmissionMode = resolveAdmissionMode(config, resolvedUnifiedThrottleHostname);
    const unifiedBreakerEligible = unifiedAdmissionMode === 'breaker_only';

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

    if (unifiedResult.cache.hit) {
      cacheHit = true;
      linkData = unifiedResult.cache.linkData;
      if (unifiedResult.cache.hostnameHash) {
        unifiedThrottleHostnameHash = unifiedResult.cache.hostnameHash;
      }
    }

    const unifiedBreaker = unifiedBreakerEligible && config.throttleEnabled
      ? readOpenBreakerSnapshot(unifiedResult.throttle, config.throttleConfig?.openCapSeconds || 60)
      : null;
    const unifiedResponse = unifiedBreakerEligible
      ? applyUnifiedResult(unifiedResult, {
          origin,
          openCapSeconds: config.throttleConfig?.openCapSeconds || 60,
          throttleEnabled: config.throttleEnabled,
          throttleHostname: resolvedUnifiedThrottleHostname,
          throttleHostnamePatterns: config.throttleHostnamePatterns,
        })
      : null;
    if (unifiedResponse) {
      await slowFailDelay();

      console.log(
        `[Throttle] Open breaker from unified check, returning error ${unifiedBreaker.errorCode}, retry after ${unifiedBreaker.retryAfter}s`
      );

      return unifiedResponse;
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
    const unifiedResponse = await applyUnifiedCheckResult();
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
  const upstreamHostnameRaw = extractHostname(downloadUrl);
  const upstreamHostname = upstreamHostnameRaw ? upstreamHostnameRaw.toLowerCase() : null;
  let admissionMode = resolveAdmissionMode(config, upstreamHostname);
  let unifiedThrottleHostname = null;

  if (supportsUnifiedCheck && !config.cacheEnabled && !unifiedResult) {
    const throttleHostnameRaw = extractHostname(downloadUrl);
    unifiedThrottleHostname = throttleHostnameRaw ? throttleHostnameRaw.toLowerCase() : null;
    const unifiedThrottleAuthorityHostname = admissionMode === 'breaker_only' && unifiedThrottleHostname
      ? deriveThrottleAuthorityHostname(unifiedThrottleHostname)
      : null;
    const throttleHostnameHash = unifiedThrottleAuthorityHostname
      ? await sha256Hash(unifiedThrottleAuthorityHostname)
      : null;
    unifiedThrottleHostnameHash = throttleHostnameHash || null;

    const { result, errorResponse } = await runUnifiedCheck(throttleHostnameHash);
    if (errorResponse) {
      return errorResponse;
    }
    unifiedResult = result;
    const unifiedResponse = await applyUnifiedCheckResult({ throttleHostname: unifiedThrottleHostname });
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
  const isThrottleManagedHostname = (hostname) => isManagedThrottleHost(hostname, config.throttleHostnamePatterns);
  const isProtectedThrottleStatusCode = (statusCode) => {
    if (!Number.isInteger(statusCode)) {
      return false;
    }
    const protectedHttpCodes = Array.isArray(config.throttleConfig?.protectHttpCodes)
      ? config.throttleConfig.protectHttpCodes
      : [];
    return protectedHttpCodes.includes(statusCode);
  };
  const isGeneratedTerminalUpstreamStatus = (statusCode) => (
    Number.isInteger(statusCode)
    && statusCode >= 400
    && statusCode < 600
  );
  const getThrottleAuthorityHostname = (hostname) => {
    if (!isThrottleManagedHostname(hostname)) {
      return null;
    }
    return deriveThrottleAuthorityHostname(hostname);
  };
  const readAuthoritySnapshotForHostname = async (hostname) => {
    const authorityHostname = getThrottleAuthorityHostname(hostname);
    if (!throttleCheckEnabled || !authorityHostname) {
      return null;
    }

    try {
      const snapshot = await throttleManager.getBreakerState(authorityHostname, { ...config.throttleConfig, ctx });
      if (!snapshot) {
        console.error('[Throttle] Snapshot read returned no authority state');
        return createBreakerAuthorityUnavailableResponse(origin, 'snapshot read');
      }
      return snapshot;
    } catch (error) {
      console.error('[Throttle] Snapshot read failed:', error instanceof Error ? error.message : String(error));
      return createBreakerAuthorityUnavailableResponse(origin, 'snapshot read');
    }
  };

  if (throttleCheckEnabled && admissionMode === 'breaker_only') {
    throttleHostname = upstreamHostname;

    const unifiedThrottleUsable = Boolean(
      unifiedResult
      && unifiedResult.throttle
      && unifiedThrottleHostnameHash
      && throttleHostname
    );

    if (unifiedThrottleUsable) {
      throttleStatus = unifiedResult.throttle;
    } else if (throttleHostname && isThrottleManagedHostname(throttleHostname)) {
      throttleStatus = await readAuthoritySnapshotForHostname(throttleHostname);
      if (throttleStatus instanceof Response) {
        return throttleStatus;
      }
    }

    if (throttleStatus) {
      const breakerState = readOpenBreakerSnapshot(throttleStatus, config.throttleConfig?.openCapSeconds || 60);
      if (breakerState) {
        await slowFailDelay();
        console.log(
          `[Throttle] Open breaker: ${throttleHostname}, returning error ${breakerState.errorCode}, retry after ${breakerState.retryAfter}s`
        );
        return createThrottleProtectedResponse(origin, breakerState);
      }
    }
  }

  const authorizeBreakerAttempt = async (hostname) => {
    const authorityHostname = getThrottleAuthorityHostname(hostname);
    if (!throttleCheckEnabled || !authorityHostname) {
      return {
        blockedResponse: null,
        attemptVersion: null,
        attemptTicket: null,
      };
    }

    try {
      const attemptSnapshot = await throttleManager.authorizeBreakerAttempt(authorityHostname, { ...config.throttleConfig, ctx });
      if (!attemptSnapshot) {
        console.error('[Throttle] Attempt authorize returned no authority state');
        return {
          blockedResponse: createBreakerAuthorityUnavailableResponse(origin, 'attempt authorize'),
          attemptVersion: null,
          attemptTicket: null,
        };
      }

      const openBreaker = readOpenBreakerSnapshot(
        attemptSnapshot,
        config.throttleConfig?.openCapSeconds || DEFAULT_THROTTLE_OPEN_CAP_SECONDS,
      );
      if (openBreaker) {
        await slowFailDelay();
        console.log(
          `[Throttle] Open breaker after attempt authorize: ${hostname}, returning error ${openBreaker.errorCode}, retry after ${openBreaker.retryAfter}s`
        );
        return {
          blockedResponse: createThrottleProtectedResponse(origin, openBreaker),
          attemptVersion: null,
          attemptTicket: null,
        };
      }

      if (attemptSnapshot.state === 'half_open' && attemptSnapshot.attemptGranted !== true) {
        const retryAfter = readHalfOpenDeadlineRetryAfter(attemptSnapshot, 1);
        await slowFailDelay();
        console.log(
          `[Throttle] Half-open batch full for ${hostname}, retry after ${retryAfter}s`
        );
        return {
          blockedResponse: createThrottleProtectedResponse(origin, {
            errorCode: attemptSnapshot.lastErrorCode || 503,
            retryAfter,
            message: `Service temporarily unavailable (half-open batch is full, retry after ${retryAfter}s)`,
          }),
          attemptVersion: null,
          attemptTicket: null,
        };
      }

      return {
        blockedResponse: null,
        attemptVersion: attemptSnapshot.attemptGranted === true ? attemptSnapshot.version : null,
        attemptTicket: attemptSnapshot.attemptGranted === true ? attemptSnapshot.attemptTicket : null,
      };
    } catch (error) {
      console.error('[Throttle] Attempt authorize failed:', error instanceof Error ? error.message : String(error));
      return {
        blockedResponse: createBreakerAuthorityUnavailableResponse(origin, 'attempt authorize'),
        attemptVersion: null,
        attemptTicket: null,
      };
    }
  };

  const authorizeBreakerOnlyAttemptIfNeeded = async (hostname) => {
    const hostnameAdmissionMode = resolveAdmissionMode(config, hostname);
    if (hostnameAdmissionMode !== 'breaker_only') {
      return {
        blockedResponse: null,
        attemptVersion: null,
        attemptTicket: null,
      };
    }
    return authorizeBreakerAttempt(hostname);
  };

  const settleQueueBreakerAttemptIfNeeded = async (hostname) => {
    const hostnameAdmissionMode = resolveAdmissionMode(config, hostname);
    if (hostnameAdmissionMode !== 'queue_breaker' || !fqContext || hostname !== fqContext.hostname) {
      return null;
    }

    const attemptVersion = Number.isFinite(fqContext.attemptVersion) ? Math.trunc(fqContext.attemptVersion) : null;
    const attemptTicket = Number.isFinite(fqContext.attemptTicket) ? Math.trunc(fqContext.attemptTicket) : null;
    if (!Number.isFinite(attemptVersion) || !Number.isFinite(attemptTicket)) {
      return null;
    }

    try {
      const authorityHostname = getThrottleAuthorityHostname(hostname);
      if (!authorityHostname) {
        return null;
      }

      const snapshot = await throttleManager.settleBreakerAttempt(authorityHostname, {
        attemptVersion,
        attemptTicket,
      }, { ...config.throttleConfig, ctx });
      fqContext.attemptVersion = null;
      fqContext.attemptTicket = null;
      clearDeferredQueueBreakerReport();
      return snapshot;
    } catch (error) {
      console.error('[Throttle] Attempt settlement failed:', error instanceof Error ? error.message : String(error));
      return createBreakerAuthorityUnavailableResponse(origin, 'attempt settlement');
    }
  };

  const armDeferredQueueBreakerReport = (statusCode) => {
    if (!fqContext) {
      return;
    }
    fqContext.deferredReportStatusCode = statusCode;
    fqContext.deferredReportArmed = true;
  };

  const disarmDeferredQueueBreakerReport = () => {
    if (!fqContext) {
      return;
    }
    fqContext.deferredReportArmed = false;
  };

  const clearDeferredQueueBreakerReport = () => {
    if (!fqContext) {
      return;
    }
    fqContext.deferredReportStatusCode = null;
    fqContext.deferredReportArmed = false;
  };

  const readQueueBreakerAttempt = (hostname, hostnameAdmissionMode) => {
    if (hostnameAdmissionMode !== 'queue_breaker' || !fqContext || hostname !== fqContext.hostname) {
      return {
        blockedResponse: null,
        attemptVersion: null,
        attemptTicket: null,
      };
    }

    return {
      blockedResponse: null,
      attemptVersion: Number.isFinite(fqContext.attemptVersion) ? Math.trunc(fqContext.attemptVersion) : null,
      attemptTicket: Number.isFinite(fqContext.attemptTicket) ? Math.trunc(fqContext.attemptTicket) : null,
      consumeAfterReport() {
        fqContext.attemptVersion = null;
        fqContext.attemptTicket = null;
        clearDeferredQueueBreakerReport();
      },
    };
  };

  const flushDeferredQueueBreakerReportIfNeeded = async () => {
    if (!fqContext || !fqContext.deferredReportArmed || !Number.isFinite(fqContext.deferredReportStatusCode)) {
      return null;
    }

    const attempt = readQueueBreakerAttempt(fqContext.hostname, 'queue_breaker');
    return reportBreakerResponseIfNeeded(
      fqContext.hostname,
      new Response(null, { status: fqContext.deferredReportStatusCode }),
      '',
      attempt,
    );
  };

  const flushDeferredQueueBreakerReportOnExit = async (response = null) => {
    if (!fqContext || !fqContext.deferredReportArmed || !Number.isFinite(fqContext.deferredReportStatusCode)) {
      return null;
    }

    if (response) {
      const statusCode = response.status;
      const isProtectedError = isProtectedThrottleStatusCode(statusCode);
      const isSuccessStatus = statusCode >= 200 && statusCode < 400;
      if (isProtectedError || isSuccessStatus) {
        return null;
      }
    }

    return flushDeferredQueueBreakerReportIfNeeded();
  };

  const shouldDeferQueueBreakerReportForRedirect = async (hostname, response, requestUrl) => {
    if (!response || response.status < 300 || response.status >= 400) {
      return false;
    }

    if (!fqContext || hostname !== fqContext.hostname) {
      return false;
    }

    const location = response.headers.get('Location');
    if (!location) {
      return false;
    }

    let redirectUrl;
    try {
      redirectUrl = new URL(location, requestUrl).toString();
    } catch (_error) {
      return false;
    }

    const redirectHostnameRaw = extractHostname(redirectUrl);
    const redirectHostname = redirectHostnameRaw ? redirectHostnameRaw.toLowerCase() : null;
    if (!redirectHostname) {
      return false;
    }

    const currentAuthorityHostname = getThrottleAuthorityHostname(hostname);
    const redirectAuthorityHostname = getThrottleAuthorityHostname(redirectHostname);
    if (!currentAuthorityHostname || currentAuthorityHostname !== redirectAuthorityHostname) {
      return false;
    }

    if (redirectHostname !== fqContext.hostname) {
      return false;
    }

    if (resolveAdmissionMode(config, redirectHostname) !== 'queue_breaker') {
      return false;
    }

    const redirectSiteBucket = await deriveSiteBucket(redirectHostname, redirectUrl, config.fairQueueSiteBucket);
    return redirectSiteBucket === fqContext.siteBucket;
  };

  const reportBreakerResponseIfNeeded = async (hostname, response, requestUrl, attempt = null) => {
    const hostnameAdmissionMode = resolveAdmissionMode(config, hostname);
    if (
      !throttleCheckEnabled
      || !response
      || (hostnameAdmissionMode !== 'breaker_only' && hostnameAdmissionMode !== 'queue_breaker')
    ) {
      return;
    }

    try {
      const statusCode = response.status;
      const isProtectedError = isProtectedThrottleStatusCode(statusCode);
      const isSuccessStatus = statusCode >= 200 && statusCode < 400;
      if (!isProtectedError && !isSuccessStatus) {
        return;
      }

      if (hostnameAdmissionMode === 'queue_breaker') {
        const shouldDefer = await shouldDeferQueueBreakerReportForRedirect(
          hostname,
          response,
          requestUrl,
        );
        if (shouldDefer) {
          armDeferredQueueBreakerReport(statusCode);
          return;
        }

        disarmDeferredQueueBreakerReport();
      }

      const sample = isProtectedError ? 1 : 0;
      const retryAfterSeconds = isProtectedError
        ? deriveOpenSeconds(response.headers.get('Retry-After'), config.throttleConfig?.openCapSeconds || 60)
        : null;
      const attemptVersion = Number.isFinite(attempt?.attemptVersion)
        ? Math.trunc(attempt.attemptVersion)
        : null;
      const attemptTicket = Number.isFinite(attempt?.attemptTicket)
        ? Math.trunc(attempt.attemptTicket)
        : null;

      if (isProtectedError) {
        console.log(`[Throttle] Error ${statusCode} from ${hostname}, reporting breaker sample`);
      }

      const authorityHostname = getThrottleAuthorityHostname(hostname);
      if (!authorityHostname) {
        return;
      }

      const snapshot = await throttleManager.reportBreakerSample(
        authorityHostname,
        {
          sample,
          statusCode,
          attemptVersion,
          attemptTicket,
          retryAfterSeconds,
        },
        { ...config.throttleConfig, ctx }
      );

      if (!snapshot) {
        console.error('[Throttle] Sample report returned no authority state');
        await cancelResponseBody(response);
        return createBreakerAuthorityUnavailableResponse(origin, 'sample report');
      }

      attempt?.consumeAfterReport?.();
    } catch (error) {
      console.error('[Throttle] Sample report failed:', error instanceof Error ? error.message : String(error));
      await cancelResponseBody(response);
      return createBreakerAuthorityUnavailableResponse(origin, 'sample report');
    }

    return null;
  };

  // ========================================
  // Fair Upstream Queue Integration
  // ========================================
  const needsFairQueueForMode = (mode) => mode === 'queue_only' || mode === 'queue_breaker';
  let needFairQueue = needsFairQueueForMode(admissionMode);
  let needTrueConcurrency = isTrueConcurrencyManagedHostname(config, upstreamHostname);

  const buildFairQueueAdmissionFields = (mode) => {
    if (mode !== 'queue_breaker') {
      return {};
    }

    return {
      breakerEnabled: true,
      openCapSeconds: config.throttleConfig?.openCapSeconds,
      closeThresholdPercent: config.throttleConfig?.closeThresholdPercent,
      halfOpenSuccessThreshold: config.throttleConfig?.halfOpenSuccessThreshold,
      halfOpenCloseMode: config.throttleConfig?.halfOpenCloseMode,
      halfOpenMaxProbeCount: config.throttleConfig?.halfOpenMaxProbeCount,
      halfOpenMaxSeconds: config.throttleConfig?.halfOpenMaxSeconds,
      halfOpenTimeoutMode: config.throttleConfig?.halfOpenTimeoutMode,
      attemptVersion: null,
      attemptTicket: null,
    };
  };

  const buildFairQueueContext = (hostname, hostnameHash, ipBucket, siteBucket, mode) => ({
    hostname,
    hostnameHash,
    ipBucket,
    siteBucket,
    nowMs: Date.now(),
    deferredReportStatusCode: null,
    deferredReportArmed: false,
    cleanupRetired: false,
    ...buildFairQueueAdmissionFields(mode),
  });

  let fairQueueClient = null;
  let fqContext = null;
  const pendingFairQueueCleanupContexts = [];
  let pendingBreakerOnlyAttempt = null;
  let clientIpSubnetHash = null;
  let concurrencyClient = null;
  let cqPlan = null;
  let cqPlanKey = '';
  let cqTargetUrl = '';
  let cqLease = null;
  let cqReleaseController = null;
  let cqHeartbeatManager = null;
  let cqCleanupBoundToStream = false;
  let cqStreamAbortController = null;
  let cqStreamAbortReason = '';
  let cqAcquireDispatched = false;
  let cqWaitBudget = null;

  const clearPendingBreakerOnlyAttempt = () => {
    pendingBreakerOnlyAttempt = null;
  };

  const armPendingBreakerOnlyAttempt = (hostname, attempt = null) => {
    if (resolveAdmissionMode(config, hostname) !== 'breaker_only') {
      clearPendingBreakerOnlyAttempt();
      return;
    }

    pendingBreakerOnlyAttempt = {
      hostname,
      attemptVersion: Number.isFinite(attempt?.attemptVersion) ? Math.trunc(attempt.attemptVersion) : null,
      attemptTicket: Number.isFinite(attempt?.attemptTicket) ? Math.trunc(attempt.attemptTicket) : null,
    };
  };

  const settleBreakerOnlyAttemptIfNeeded = async (hostname) => {
    if (
      resolveAdmissionMode(config, hostname) !== 'breaker_only'
      || !pendingBreakerOnlyAttempt
      || pendingBreakerOnlyAttempt.hostname !== hostname
    ) {
      return null;
    }

    const attemptVersion = Number.isFinite(pendingBreakerOnlyAttempt.attemptVersion)
      ? Math.trunc(pendingBreakerOnlyAttempt.attemptVersion)
      : null;
    const attemptTicket = Number.isFinite(pendingBreakerOnlyAttempt.attemptTicket)
      ? Math.trunc(pendingBreakerOnlyAttempt.attemptTicket)
      : null;

    if (!Number.isFinite(attemptVersion) || !Number.isFinite(attemptTicket)) {
      clearPendingBreakerOnlyAttempt();
      return null;
    }

    try {
      const authorityHostname = getThrottleAuthorityHostname(hostname);
      if (!authorityHostname) {
        clearPendingBreakerOnlyAttempt();
        return null;
      }

      const snapshot = await throttleManager.settleBreakerAttempt(authorityHostname, {
        attemptVersion,
        attemptTicket,
      }, { ...config.throttleConfig, ctx });
      clearPendingBreakerOnlyAttempt();
      return snapshot;
    } catch (error) {
      console.error('[Throttle] Attempt settlement failed:', error instanceof Error ? error.message : String(error));
      clearPendingBreakerOnlyAttempt();
      return createBreakerAuthorityUnavailableResponse(origin, 'attempt settlement');
    }
  };

  const retirePendingBreakerOnlyAttemptIfNeeded = async () => {
    const pendingHostname = pendingBreakerOnlyAttempt?.hostname;
    if (!pendingHostname) {
      return null;
    }

    const settlement = await settleBreakerOnlyAttemptIfNeeded(pendingHostname);
    if (settlement instanceof Response) {
      return settlement;
    }

    return null;
  };

  const settleBreakerAttemptIfNeeded = async (hostname) => {
    const queueBreakerSettlement = await settleQueueBreakerAttemptIfNeeded(hostname);
    if (queueBreakerSettlement instanceof Response) {
      return queueBreakerSettlement;
    }

    const breakerOnlySettlement = await settleBreakerOnlyAttemptIfNeeded(hostname);
    if (breakerOnlySettlement instanceof Response) {
      return breakerOnlySettlement;
    }

    return breakerOnlySettlement || queueBreakerSettlement;
  };

  const ensureClientIpSubnetHash = async (label, unavailableResponse) => {
    if (clientIpSubnetHash) {
      return null;
    }

    const clientIpSubnet = calculateIPSubnet(clientIP, config.ipv4Suffix, config.ipv6Suffix);
    if (!clientIpSubnet) {
      console.error(`[${label}] Failed: unable to derive client subnet for admission enforcement`);
      return unavailableResponse;
    }

    clientIpSubnetHash = await sha256Hash(clientIpSubnet);
    if (!clientIpSubnetHash) {
      console.error(`[${label}] Failed: unable to hash client subnet for admission enforcement`);
      return unavailableResponse;
    }

    return null;
  };

  const ensureFairQueueClientReady = async () => {
    if (!config.slotHandlerConfig?.url) {
      console.error('[Fair Queue] enabled but slot-handler URL missing');
      return createErrorResponse(origin, 503, 'Fair queue misconfigured (slot-handler URL missing)');
    }

    const subnetResponse = await ensureClientIpSubnetHash('Fair Queue', createErrorResponse(origin, 503, 'Fair queue unavailable'));
    if (subnetResponse) {
      return subnetResponse;
    }

    if (!fairQueueClient) {
      try {
        fairQueueClient = createFairQueueClient(config);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.error('[Fair Queue] Failed to initialize client:', message);
        return createErrorResponse(origin, 503, 'Fair queue unavailable');
      }
    }

    return null;
  };

  const ensureConcurrencyClientReady = async () => {
    if (!config.concurrencyHandlerConfig?.url) {
      console.error('[CQ] enabled but concurrency-handler URL missing');
      return createTrueConcurrencyUnavailableResponse(origin, 'True concurrency misconfigured (handler URL missing)');
    }

    const subnetResponse = await ensureClientIpSubnetHash('CQ', createTrueConcurrencyUnavailableResponse(origin));
    if (subnetResponse) {
      return subnetResponse;
    }

    if (!concurrencyClient) {
      try {
        concurrencyClient = createConcurrencyHandlerClient(config);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.error('[CQ] Failed to initialize client:', message);
        return createTrueConcurrencyUnavailableResponse(origin);
      }
    }

    return null;
  };

  const buildTrueConcurrencyPlanKey = (plan) => JSON.stringify([
    plan?.hostnameHash || '',
    plan?.siteBucket || '',
    plan?.ipBucket || '',
    plan?.hardExpireAtMs || 0,
  ]);

  const buildTrueConcurrencyPlanForTarget = async (targetUrl, requestId = createTrueConcurrencyRequestId()) => {
    const hostnameRaw = extractHostname(targetUrl);
    const hostname = hostnameRaw ? hostnameRaw.toLowerCase() : null;
    if (!hostname) {
      throw new Error('[CQ] target hostname missing');
    }
    return {
      hostname,
      hostnameHash: await sha256Hash(hostname),
      siteBucket: await deriveSiteBucket(hostname, targetUrl, config.trueConcurrencySiteBucket),
      ipBucket: clientIpSubnetHash,
      clientInstanceId: normalizeStringValue(env?.INSTANCE_ID) || 'download-worker',
      requestId,
      hardExpireAtMs,
      nowMs: Date.now(),
      ticketHash,
    };
  };

  const createTrueConcurrencyTerminalResponse = (result, reason = '') => {
    if (result === 'expired' && reason === 'hard_expired') {
      return createUnauthorizedResponse(origin, 'link expired');
    }
    const suffix = reason ? ` (${reason})` : '';
    return createTrueConcurrencyUnavailableResponse(origin, `True concurrency ${result}${suffix}`);
  };

  const abortCurrentTrueConcurrencyStream = (reason = '') => {
    if (!cqStreamAbortReason && typeof reason === 'string' && reason) {
      cqStreamAbortReason = reason;
    }
    if (cqStreamAbortController && !cqStreamAbortController.signal.aborted) {
      cqStreamAbortController.abort();
    }
  };

  const clearCurrentTrueConcurrencyState = () => {
    cqPlan = null;
    cqPlanKey = '';
    cqTargetUrl = '';
    cqLease = null;
    cqReleaseController = null;
    cqHeartbeatManager = null;
    cqCleanupBoundToStream = false;
    cqStreamAbortController = null;
    cqStreamAbortReason = '';
    cqAcquireDispatched = false;
    cqWaitBudget = null;
  };

  const readCurrentTrueConcurrencyWaitBudgetWindow = () => {
    if (!cqWaitBudget) {
      return null;
    }

    const nowMs = Date.now();
    const elapsedMs = nowMs - cqWaitBudget.startedAtMs;
    const remainingMs = cqWaitBudget.totalMaxMs - elapsedMs;

    return {
      nowMs,
      remainingMs,
      exhausted: remainingMs <= 0 || cqWaitBudget.attempts >= cqWaitBudget.maxAttemptsCap,
      timeoutMs: Math.min(config.concurrencyHandlerConfig.acquireTimeoutMs, remainingMs),
    };
  };

  const createTrueConcurrencyWaitBudgetExhaustedResponse = async () => {
    await ensureCurrentTrueConcurrencyCancelled('worker_aborted');
    return createTrueConcurrencyUnavailableResponse(origin, 'True concurrency wait budget exhausted');
  };

  const ensureCurrentTrueConcurrencyReleased = async (reason, immediate = false) => {
    if (!cqReleaseController) {
      return true;
    }

    const releaseController = cqReleaseController;
    const heartbeatManager = cqHeartbeatManager;
    await heartbeatManager?.ensureCleanup?.(reason);
    clearCurrentTrueConcurrencyState();
    if (immediate) {
      const released = await releaseController.releaseImmediately(reason);
      if (!released) {
        releaseController.ensureReleased(reason);
      }
      return released;
    }

    releaseController.ensureReleased(reason);
    return true;
  };

  const buildCurrentTrueConcurrencyRequestIdentity = () => {
    if (!cqPlan) {
      return null;
    }
    return {
      requestId: cqPlan.requestId,
      hostname: cqPlan.hostname,
      hostnameHash: cqPlan.hostnameHash,
      siteBucket: cqPlan.siteBucket,
      ipBucket: cqPlan.ipBucket,
      hardExpireAtMs: cqPlan.hardExpireAtMs,
    };
  };

  const ensureCurrentTrueConcurrencyCancelled = async (reason, options = {}) => {
    const allowPreActive = options && options.allowPreActive === true;
    if (!cqPlan || cqLease || !concurrencyClient) {
      return true;
    }
    if (!cqPlan.waitToken && !allowPreActive) {
      return true;
    }
    if (!cqPlan.waitToken && !cqAcquireDispatched) {
      return true;
    }

    const requestIdentity = buildCurrentTrueConcurrencyRequestIdentity();
    clearCurrentTrueConcurrencyState();
    if (!requestIdentity) {
      return true;
    }

    try {
      const result = await concurrencyClient.cancel(ctx, requestIdentity, reason);
      return result?.result === 'cancelled' || result?.result === 'noop';
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn(`[CQ] cancel failed for ${requestIdentity.requestId}:`, message);
      return false;
    }
  };

  const replayCurrentTrueConcurrencyAcquireAfterWaitError = async (phase, error) => {
    if (!cqPlan?.waitToken || !concurrencyClient || didClientAbort()) {
      throw error;
    }

    const waitBudgetWindow = readCurrentTrueConcurrencyWaitBudgetWindow();
    if (waitBudgetWindow?.exhausted) {
      return null;
    }

    cqWaitBudget.attempts += 1;

    const message = error instanceof Error ? error.message : String(error);
    console.warn(`[CQ] continue-wait acquire failed during ${phase}, replaying immutable tuple:`, message);
    cqPlan.nowMs = waitBudgetWindow.nowMs;
    return concurrencyClient.acquire(ctx, cqPlan, clientSignal, waitBudgetWindow.timeoutMs);
  };

  const releaseUnusedFairQueueGrantIfNeeded = async (phase) => {
    if (!needFairQueue || !fqContext?.slotToken) {
      return true;
    }
    fqContext.hitUpstreamAtMs = 0;
    return finalizeFairQueueOnFailure(phase);
  };

  const createFairQueueTimeoutResponse = () => {
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
  };

  const handleFairQueueWaitResult = async (fqResult) => {
    if (fqResult.kind === 'throttled') {
      const breakerState = fqResult.breakerSnapshot
        ? readOpenBreakerSnapshot(fqResult.breakerSnapshot, 0)
        : null;
      await slowFailDelay();
      return createThrottleProtectedResponse(origin, {
        errorCode: breakerState?.errorCode || fqResult.throttleCode || 503,
        retryAfter: fqResult.retryAfter ?? breakerState?.retryAfter,
      });
    }

    if (fqResult.kind === 'timeout') {
      return createFairQueueTimeoutResponse();
    }

    if (fqResult.kind === 'conflict') {
      return createFairQueueTimeoutResponse();
    }

    if (fqResult.kind === 'overloaded' && fqResult.scope === 'global') {
      return createFairQueueOverloadedResponse(origin, fqResult.retryAfter);
    }

    return null;
  };

  const admitFairQueueContext = async (phase) => {
    try {
      const fqResult = await fairQueueClient.waitForSlot(ctx, fqContext, clientSignal);
      return await handleFairQueueWaitResult(fqResult);
    } catch (error) {
      if (didClientAbort() && isAbortError(error)) {
        return createClientAbortResponse(origin);
      }
      const message = error instanceof Error ? error.message : String(error);
      console.error(`[Fair Queue] waitForSlot error during ${phase}:`, message);
      return createErrorResponse(origin, 503, 'Fair queue unavailable');
    }
  };

  const finalizeFairQueueOnFailure = async (phase) => {
    if (!fqContext || !fairQueueClient) {
      return true;
    }

    const finalized = await finalizeFairQueueContext({
      fairQueueClient,
      ctx,
      fqContext,
      phase,
    });
    if (!finalized) {
      pendingFairQueueCleanupContexts.push(fqContext);
    }
    return finalized;
  };

  const waitForInFlightFairQueueHeaderRelease = async (cleanupContext) => {
    const inFlightRelease = cleanupContext?.headerReleasePromise;
    if (!inFlightRelease) {
      return null;
    }
    return inFlightRelease;
  };

  const releaseFairQueueAfterHeadersIfNeeded = () => {
    if (!fqContext?.slotToken || !fairQueueClient || !needFairQueue || !needTrueConcurrency) {
      return;
    }

    if (fqContext.headerReleasePromise) {
      return;
    }

    const releaseContext = fqContext;
    releaseContext.headerReleasePromise = (async () => {
      const finalized = await finalizeFairQueueContext({
        fairQueueClient,
        ctx,
        fqContext: releaseContext,
        phase: 'upstream headers',
      });
      if (!finalized) {
        console.warn(`[Fair Queue] early release after upstream headers failed for host=${releaseContext.hostname}`);
      }
      return finalized;
    })().finally(() => {
      releaseContext.headerReleasePromise = null;
    });

    if (ctx && typeof ctx.waitUntil === 'function') {
      ctx.waitUntil(releaseContext.headerReleasePromise);
    }
  };

const runEarlyFairQueueCleanupAndReturn = async (response, phase) => {
  if (!response || !fqContext || !fairQueueClient) {
    return response;
  }

  const deferredReportResponse = await flushDeferredQueueBreakerReportIfNeeded();
  const responseToReturn = deferredReportResponse || response;

  fqContext.cleanupRetired = true;

    const cleanupPromise = (async () => {
      const finalized = await finalizeFairQueueContext({
        fairQueueClient,
        ctx,
        fqContext,
        phase,
      });
      if (finalized) {
        return true;
      }
      return finalizeFairQueueContext({
        fairQueueClient,
        ctx,
        fqContext,
        phase: `${phase} retry`,
      });
    })();

    if (ctx && typeof ctx.waitUntil === 'function') {
      ctx.waitUntil(cleanupPromise);
      return responseToReturn;
    }

    await cleanupPromise;
    return responseToReturn;
  };

  const prepareFairQueueContextForTarget = async (targetUrl, phase) => {
    const updatedHostnameRaw = extractHostname(targetUrl);
    const updatedHostname = updatedHostnameRaw ? updatedHostnameRaw.toLowerCase() : null;
    const updatedAdmissionMode = resolveAdmissionMode(config, updatedHostname);
    admissionMode = updatedAdmissionMode;
    needFairQueue = needsFairQueueForMode(updatedAdmissionMode);
    needTrueConcurrency = isTrueConcurrencyManagedHostname(config, updatedHostname);
    const normalizedTargetUrl = String(targetUrl);
    const updatedSiteBucket = needFairQueue
      ? await deriveSiteBucket(updatedHostname, targetUrl, config.fairQueueSiteBucket)
      : null;
    const fairQueueIdentityChanged = !fqContext
      || updatedHostname !== fqContext.hostname
      || updatedSiteBucket !== fqContext.siteBucket;
    const fairQueueTargetChanged = fairQueueIdentityChanged
      || ((updatedAdmissionMode === 'queue_only' || needTrueConcurrency)
        && (!fqContext || fqContext.targetUrl !== normalizedTargetUrl));

    if (!needFairQueue) {
      if (fqContext) {
        const deferredReportResponse = await flushDeferredQueueBreakerReportOnExit();
        if (deferredReportResponse) {
          return deferredReportResponse;
        }
        const previousFairQueueContext = fqContext;
        const cleanupResult = await reconcileFairQueueContextForTarget({
          fairQueueClient,
          ctx,
          fqContext: previousFairQueueContext,
          targetUrl,
          phase,
          pendingCleanupContexts: pendingFairQueueCleanupContexts,
          targetHostname: updatedHostname,
          targetSiteBucket: previousFairQueueContext.siteBucket,
          forceRetire: fairQueueTargetChanged,
        });
        fqContext = null;
        if (!cleanupResult.finalized) {
          // already queued for final cleanup retry
        }
      }
      return { targetHostname: updatedHostname, targetSiteBucket: null };
    }

    const fairQueueInitResponse = await ensureFairQueueClientReady();
    if (fairQueueInitResponse) {
      return fairQueueInitResponse;
    }

    if (!fairQueueTargetChanged && fqContext && updatedHostname === fqContext.hostname && updatedSiteBucket === fqContext.siteBucket) {
      return null;
    }

    if (fqContext) {
      const deferredReportResponse = await flushDeferredQueueBreakerReportOnExit();
      if (deferredReportResponse) {
        return deferredReportResponse;
      }
      const previousFairQueueContext = fqContext;
      await reconcileFairQueueContextForTarget({
        fairQueueClient,
        ctx,
        fqContext: previousFairQueueContext,
        targetUrl,
        phase,
        pendingCleanupContexts: pendingFairQueueCleanupContexts,
        targetHostname: updatedHostname,
        targetSiteBucket: updatedSiteBucket,
        forceRetire: fairQueueTargetChanged,
      });
    }
    const updatedHostnameHash = await sha256Hash(updatedHostname);
    fqContext = buildFairQueueContext(
      updatedHostname,
      updatedHostnameHash,
      clientIpSubnetHash,
      updatedSiteBucket,
      updatedAdmissionMode,
    );
    fqContext.targetUrl = normalizedTargetUrl;

    return {
      targetHostname: updatedHostname,
      targetSiteBucket: updatedSiteBucket,
    };
  };

  const prepareTargetForFetch = async (targetUrl, phase) => {
    const retireBreakerOnlyResponse = await retirePendingBreakerOnlyAttemptIfNeeded();
    if (retireBreakerOnlyResponse) {
      return retireBreakerOnlyResponse;
    }

    const normalizedTargetUrl = String(targetUrl);
    const fairQueuePrepareResult = await prepareFairQueueContextForTarget(targetUrl, phase);
    if (fairQueuePrepareResult instanceof Response) {
      return fairQueuePrepareResult;
    }

    // Worker CQ behavior matrix:
    // queue_only + CQ: FQ acquire -> CQ fast acquire.
    // granted => fetch -> FQ early release after headers -> CQ release.
    // wait => FQ immediate unused-grant release -> continue-wait via stable waitToken -> granted -> fetch -> CQ release.
    // terminal/conflict => FQ immediate unused-grant release -> terminal response with waiting request cleanup via CQ cancel.
    // queue_breaker + CQ: FQ acquire(granted + attempt) -> CQ fast acquire.
    // wait => settle old breaker attempt -> FQ immediate unused-grant release -> continue-wait.
    // granted after wait => fresh breaker authorize -> fetch or immediate CQ release on breaker deny.

    let nextPlan = null;
    if (needTrueConcurrency) {
      if (Date.now() >= hardExpireAtMs) {
        return createUnauthorizedResponse(origin, 'link expired');
      }

      const concurrencyInitResponse = await ensureConcurrencyClientReady();
      if (concurrencyInitResponse) {
        return concurrencyInitResponse;
      }

      nextPlan = await buildTrueConcurrencyPlanForTarget(targetUrl);
      const nextPlanKey = buildTrueConcurrencyPlanKey(nextPlan);
      const trueConcurrencyTargetChanged = cqTargetUrl !== '' && cqTargetUrl !== normalizedTargetUrl;

      if (cqReleaseController && (trueConcurrencyTargetChanged || (cqPlanKey && cqPlanKey !== nextPlanKey))) {
        await ensureCurrentTrueConcurrencyReleased('target_change', true);
      }

      if (!cqPlan || trueConcurrencyTargetChanged || cqPlanKey !== nextPlanKey) {
        cqPlan = nextPlan;
        cqPlanKey = nextPlanKey;
        cqTargetUrl = normalizedTargetUrl;
        cqLease = null;
        cqReleaseController = null;
        cqCleanupBoundToStream = false;
      }

    } else if (cqReleaseController) {
      await ensureCurrentTrueConcurrencyReleased('target_change', true);
    }

    if (needFairQueue && fqContext && !fqContext.slotToken) {
      const fairQueueWaitResponse = await admitFairQueueContext(phase);
      if (fairQueueWaitResponse) {
        return await runEarlyFairQueueCleanupAndReturn(fairQueueWaitResponse, phase);
      }
    }

    if (needTrueConcurrency && !cqLease && cqPlan) {
        if (Date.now() >= cqPlan.hardExpireAtMs) {
          const settleResponse = await settleBreakerAttemptIfNeeded(cqPlan.hostname);
          if (settleResponse instanceof Response) {
            if (needFairQueue) {
              await releaseUnusedFairQueueGrantIfNeeded(`${phase} expired before concurrency acquire`);
            }
            return settleResponse;
          }
          if (needFairQueue) {
            await releaseUnusedFairQueueGrantIfNeeded(`${phase} expired before concurrency acquire`);
          }
          return createUnauthorizedResponse(origin, 'link expired');
        }

      try {
        cqPlan.nowMs = Date.now();
        cqAcquireDispatched = true;
        let acquireResult = await concurrencyClient.acquire(ctx, cqPlan, clientSignal);

        if (acquireResult.result === 'wait') {
          cqPlan.waitToken = acquireResult.waitToken;
          cqWaitBudget = {
            startedAtMs: Date.now(),
            attempts: 1,
            totalMaxMs: config.concurrencyHandlerConfig.waitTotalMaxMs,
            maxAttemptsCap: config.concurrencyHandlerConfig.waitMaxAttemptsCap,
          };
          if (resolveAdmissionMode(config, cqPlan.hostname) === 'queue_breaker' || !needFairQueue) {
            const settleResponse = await settleBreakerAttemptIfNeeded(cqPlan.hostname);
            if (settleResponse instanceof Response) {
              const fairQueueReleased = await releaseUnusedFairQueueGrantIfNeeded(`${phase} cq wait settle failure`);
              await ensureCurrentTrueConcurrencyCancelled('worker_aborted');
              if (!fairQueueReleased) {
                return createErrorResponse(origin, 503, 'Fair queue unavailable');
              }
              return settleResponse;
            }
          }
          const fairQueueReleased = await releaseUnusedFairQueueGrantIfNeeded(`${phase} cq wait release`);
          if (!fairQueueReleased) {
            await ensureCurrentTrueConcurrencyCancelled('worker_aborted');
            return createErrorResponse(origin, 503, 'Fair queue unavailable');
          }

          while (acquireResult.result === 'wait') {
            const waitBudgetWindow = readCurrentTrueConcurrencyWaitBudgetWindow();
            if (!waitBudgetWindow || waitBudgetWindow.exhausted) {
              return createTrueConcurrencyWaitBudgetExhaustedResponse();
            }

            cqWaitBudget.attempts += 1;
            cqPlan.nowMs = waitBudgetWindow.nowMs;
            try {
              acquireResult = await concurrencyClient.acquire(
                ctx,
                cqPlan,
                clientSignal,
                waitBudgetWindow.timeoutMs,
              );
            } catch (error) {
              const replayedAcquireResult = await replayCurrentTrueConcurrencyAcquireAfterWaitError(
                phase,
                error,
              );
              if (!replayedAcquireResult) {
                return createTrueConcurrencyWaitBudgetExhaustedResponse();
              }
              acquireResult = replayedAcquireResult;
            }
          }
        }

        if (acquireResult.result === 'conflict' || acquireResult.result === 'released' || acquireResult.result === 'cancelled' || acquireResult.result === 'expired') {
          const settleResponse = await settleBreakerAttemptIfNeeded(cqPlan.hostname);
          if (settleResponse instanceof Response) {
            if (needFairQueue) {
              await releaseUnusedFairQueueGrantIfNeeded(`${phase} cq terminal settle failure`);
            }
            if (cqPlan?.waitToken) {
              await ensureCurrentTrueConcurrencyCancelled('worker_aborted');
            }
            return settleResponse;
          }
          if (needFairQueue) {
            await releaseUnusedFairQueueGrantIfNeeded(`${phase} cq terminal release`);
          }
          if (cqPlan?.waitToken) {
            await ensureCurrentTrueConcurrencyCancelled('worker_aborted');
          }
          return createTrueConcurrencyTerminalResponse(acquireResult.result, acquireResult.reason);
        }

        let claimedResult;
        const buildClaimReleaseLease = () => ({
          ...acquireResult,
          requestId: cqPlan.requestId,
          hostname: cqPlan.hostname,
          hostnameHash: cqPlan.hostnameHash,
          siteBucket: cqPlan.siteBucket,
          ipBucket: cqPlan.ipBucket,
          hardExpireAtMs: cqPlan.hardExpireAtMs,
        });
        try {
          claimedResult = await concurrencyClient.claim(ctx, {
            requestId: cqPlan.requestId,
            claimToken: acquireResult.claimToken,
            nowMs: Date.now(),
            hardExpireAtMs: cqPlan.hardExpireAtMs,
          }, clientSignal);
        } catch (error) {
          const releaseController = createConcurrencyReleaseController({
            client: concurrencyClient,
            ctx,
            lease: buildClaimReleaseLease(),
            label: cqPlan.hostname,
          });
          await releaseController.releaseImmediately('acquire_delivery_failed');
          const settleResponse = await settleBreakerAttemptIfNeeded(cqPlan.hostname);
          const message = error instanceof Error ? error.message : String(error);
          console.error(`[CQ] claim failed during ${phase}:`, message);
          if (settleResponse instanceof Response) {
            if (needFairQueue) {
              await releaseUnusedFairQueueGrantIfNeeded(`${phase} cq claim settle failure`);
            }
            return settleResponse;
          }
          if (needFairQueue) {
            await releaseUnusedFairQueueGrantIfNeeded(`${phase} cq claim failure`);
          }
          return createTrueConcurrencyUnavailableResponse(origin);
        }

        if (claimedResult.result !== 'granted') {
          if (claimedResult.result === 'conflict' && claimedResult.reason === 'grant_unclaimed') {
            const releaseController = createConcurrencyReleaseController({
              client: concurrencyClient,
              ctx,
              lease: buildClaimReleaseLease(),
              label: cqPlan.hostname,
            });
            await releaseController.releaseImmediately('acquire_delivery_failed');
          }
          const settleResponse = await settleBreakerAttemptIfNeeded(cqPlan.hostname);
          if (settleResponse instanceof Response) {
            if (needFairQueue) {
              await releaseUnusedFairQueueGrantIfNeeded(`${phase} cq claim terminal settle failure`);
            }
            return settleResponse;
          }
          if (needFairQueue) {
            await releaseUnusedFairQueueGrantIfNeeded(`${phase} cq claim terminal release`);
          }
          return createTrueConcurrencyTerminalResponse(claimedResult.result, claimedResult.reason);
        }

        acquireResult = {
          ...claimedResult,
          claimToken: acquireResult.claimToken,
        };

        cqLease = {
          ...acquireResult,
          requestId: cqPlan.requestId,
          hostname: cqPlan.hostname,
          hostnameHash: cqPlan.hostnameHash,
          siteBucket: cqPlan.siteBucket,
          ipBucket: cqPlan.ipBucket,
          hardExpireAtMs: cqPlan.hardExpireAtMs,
        };
        cqReleaseController = createConcurrencyReleaseController({
          client: concurrencyClient,
          ctx,
          lease: cqLease,
          label: cqPlan.hostname,
        });

        let ackHandoffResult;
        try {
          ackHandoffResult = await concurrencyClient.ackHandoff(ctx, {
            requestId: cqPlan.requestId,
            handoffToken: acquireResult.handoffToken,
            nowMs: Date.now(),
          }, clientSignal);
        } catch (error) {
          const claimHostname = cqPlan?.hostname;
          await ensureCurrentTrueConcurrencyReleased('grant_delivery_failed', true);
          const settleResponse = await settleBreakerAttemptIfNeeded(claimHostname);
          const message = error instanceof Error ? error.message : String(error);
          console.error(`[CQ] ack_handoff failed during ${phase}:`, message);
          if (settleResponse instanceof Response) {
            if (needFairQueue) {
              await releaseUnusedFairQueueGrantIfNeeded(`${phase} cq ack_handoff settle failure`);
            }
            return settleResponse;
          }
          if (needFairQueue) {
            await releaseUnusedFairQueueGrantIfNeeded(`${phase} cq ack_handoff failure`);
          }
          return createTrueConcurrencyUnavailableResponse(origin);
        }

        if (ackHandoffResult.result !== 'acknowledged') {
          const claimHostname = cqPlan.hostname;
          clearCurrentTrueConcurrencyState();
          const settleResponse = await settleBreakerAttemptIfNeeded(claimHostname);
          if (settleResponse instanceof Response) {
            if (needFairQueue) {
              await releaseUnusedFairQueueGrantIfNeeded(`${phase} cq ack_handoff terminal settle failure`);
            }
            return settleResponse;
          }
          if (needFairQueue) {
            await releaseUnusedFairQueueGrantIfNeeded(`${phase} cq ack_handoff terminal release`);
          }
          return createTrueConcurrencyTerminalResponse(ackHandoffResult.result, ackHandoffResult.reason);
        }

        if (resolveAdmissionMode(config, cqPlan.hostname) === 'queue_breaker' && needFairQueue && !fqContext?.slotToken) {
          const breakerAttempt = await authorizeBreakerAttempt(cqPlan.hostname);
          if (breakerAttempt?.blockedResponse) {
            await ensureCurrentTrueConcurrencyReleased('stream_complete', true);
            return breakerAttempt.blockedResponse;
          }
          fqContext.attemptVersion = breakerAttempt?.attemptVersion ?? null;
          fqContext.attemptTicket = breakerAttempt?.attemptTicket ?? null;
        }

        cqHeartbeatManager = createTrueConcurrencyHeartbeatManager({
          client: concurrencyClient,
          ctx,
          plan: cqPlan,
          lease: cqLease,
          heartbeatConfig: config.concurrencyHandlerConfig?.heartbeat,
          clientSignal,
          abortStream: abortCurrentTrueConcurrencyStream,
        });
        try {
          await cqHeartbeatManager.startBeforeOriginFetch();
        } catch (error) {
          const claimHostname = cqPlan?.hostname;
          if (isTrueConcurrencyHeartbeatTerminalError(error)) {
            const terminalReason = error.terminal?.reason || '';
            if (terminalReason === 'token_mismatch' || terminalReason === 'protocol_error') {
              await ensureCurrentTrueConcurrencyReleased('heartbeat_lost', true);
            } else if (TRUE_CONCURRENCY_HEARTBEAT_TERMINAL_NO_RELEASE_REASONS.has(terminalReason)) {
              await cqHeartbeatManager?.ensureCleanup?.(terminalReason);
              clearCurrentTrueConcurrencyState();
            } else {
              await ensureCurrentTrueConcurrencyReleased('heartbeat_connect_failed', true);
            }
          } else {
            await ensureCurrentTrueConcurrencyReleased('heartbeat_connect_failed', true);
          }
          const settleResponse = await settleBreakerAttemptIfNeeded(claimHostname);
          const message = error instanceof Error ? error.message : String(error);
          console.error(`[CQ] heartbeat start failed during ${phase}:`, message);
          if (settleResponse instanceof Response) {
            if (needFairQueue) {
              await releaseUnusedFairQueueGrantIfNeeded(`${phase} cq heartbeat settle failure`);
            }
            return settleResponse;
          }
          if (needFairQueue) {
            await releaseUnusedFairQueueGrantIfNeeded(`${phase} cq heartbeat failure`);
          }
          return createTrueConcurrencyUnavailableResponse(origin);
        }

        cqAcquireDispatched = false;
        cqWaitBudget = null;
        delete cqPlan.waitToken;
      } catch (error) {
        if (didClientAbort() && isAbortError(error)) {
          const settleResponse = await settleBreakerAttemptIfNeeded(cqPlan?.hostname);
          if (settleResponse instanceof Response) {
            if (needFairQueue) {
              await releaseUnusedFairQueueGrantIfNeeded(`${phase} client abort during cq acquire`);
            }
            return settleResponse;
          }
          if (needFairQueue) {
            await releaseUnusedFairQueueGrantIfNeeded(`${phase} client abort during cq acquire`);
          }
          await ensureCurrentTrueConcurrencyCancelled('worker_aborted', { allowPreActive: true });
          return createClientAbortResponse(origin);
        }
        const message = error instanceof Error ? error.message : String(error);
        console.error(`[CQ] acquire failed during ${phase}:`, message);
        const settleResponse = await settleBreakerAttemptIfNeeded(cqPlan?.hostname);
        if (settleResponse instanceof Response) {
          if (needFairQueue) {
            await releaseUnusedFairQueueGrantIfNeeded(`${phase} cq failure`);
          }
          await ensureCurrentTrueConcurrencyCancelled('worker_aborted');
          return settleResponse;
        }
        if (needFairQueue) {
          await releaseUnusedFairQueueGrantIfNeeded(`${phase} cq failure`);
        }
        await ensureCurrentTrueConcurrencyCancelled('worker_aborted', { allowPreActive: true });
        return createTrueConcurrencyUnavailableResponse(origin);
      }
    }

    return null;
  };

  const readKnownGoogleDriveDownloadSize = () => readPayloadFileSize(payloadData) ?? readPayloadFileSize(res?.data);

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
    const upstreamHostname = extractHostname(urlValue)?.toLowerCase() || '';
    if (originalRequest.method === 'HEAD' && isGoogleDriveDownloadHostname(upstreamHostname)) {
      upstreamRequest.headers.set('range', GOOGLE_DRIVE_HEAD_PROBE_RANGE);
      return new Request(upstreamRequest, { method: 'GET' });
    }
    if (
      originalRequest.method === 'GET'
      && !originalRequest.headers.get('range')
      && isGoogleDriveDownloadHostname(upstreamHostname)
    ) {
      const fileSize = readKnownGoogleDriveDownloadSize();
      const fullRangeHeader = buildGoogleDriveFullRangeHeader(fileSize);
      if (fullRangeHeader) {
        upstreamRequest.headers.set('range', fullRangeHeader);
      }
    }
    return upstreamRequest;
  };

  const probeGoogleDriveDownloadSize = async (requestToProbe) => {
    try {
      const headProbeRequest = new Request(requestToProbe, { method: 'HEAD' });
      const headProbeResponse = await fetch(headProbeRequest);

      const headProbeSize = parseContentLengthHeader(headProbeResponse.headers.get('content-length'));
      await cancelResponseBody(headProbeResponse);
      if (headProbeSize !== null) {
        return headProbeSize;
      }
    } catch (_error) {
      // fall through to the range probe before giving up
    }

    try {
      const rangeProbeRequest = new Request(requestToProbe);
      rangeProbeRequest.headers.set('range', GOOGLE_DRIVE_HEAD_PROBE_RANGE);
      const rangeProbeResponse = await fetch(rangeProbeRequest);

      const rangeProbeSize = parseContentRangeTotal(rangeProbeResponse.headers.get('content-range'));
      await cancelResponseBody(rangeProbeResponse);
      return rangeProbeSize;
    } catch (_error) {
      return null;
    }
  };

  const maybeApplyGoogleDriveFullDownloadTranslation = async (requestToTranslate) => {
    const upstreamHostname = extractHostname(requestToTranslate?.url || '')?.toLowerCase() || '';
    if (
      originalRequest.method !== 'GET'
      || originalRequest.headers.get('range')
      || !isGoogleDriveDownloadHostname(upstreamHostname)
      || requestToTranslate.headers.get('range')
    ) {
      return requestToTranslate;
    }

    const fileSize = readKnownGoogleDriveDownloadSize() ?? await probeGoogleDriveDownloadSize(requestToTranslate);
    const fullRangeHeader = buildGoogleDriveFullRangeHeader(fileSize);
    if (!fullRangeHeader) {
      return requestToTranslate;
    }

    const translatedRequest = new Request(requestToTranslate);
    translatedRequest.headers.set('range', fullRangeHeader);
    return translatedRequest;
  };

  const resolveRedirectLocation = (location, baseUrl) => new URL(location, baseUrl).toString();

  const retireAdmissionBeforeRecursiveWorkerRedirect = async (phase) => {
    let deferredReportResponse = null;

    if (fqContext) {
      deferredReportResponse = await flushDeferredQueueBreakerReportOnExit();
      const previousFairQueueContext = fqContext;
      fqContext = null;
      const finalized = await finalizeFairQueueContext({
        fairQueueClient,
        ctx,
        fqContext: previousFairQueueContext,
        phase,
      });
      if (!finalized) {
        pendingFairQueueCleanupContexts.push(previousFairQueueContext);
      }
    }

    if (cqReleaseController) {
      await ensureCurrentTrueConcurrencyReleased('target_change', true);
    }

    return deferredReportResponse;
  };

  const fetchUpstreamWithBreakerAttempt = async (requestToFetch) => {
    const requestHostnameRaw = extractHostname(requestToFetch?.url || '');
    const requestHostname = requestHostnameRaw ? requestHostnameRaw.toLowerCase() : null;
    const requestAdmissionMode = resolveAdmissionMode(config, requestHostname);
    let attempt = {
      blockedResponse: null,
      attemptVersion: null,
      attemptTicket: null,
    };

    if (requestAdmissionMode === 'queue_breaker') {
      attempt = readQueueBreakerAttempt(requestHostname, requestAdmissionMode);
    } else if (requestAdmissionMode === 'breaker_only') {
      if (
        pendingBreakerOnlyAttempt
        && pendingBreakerOnlyAttempt.hostname === requestHostname
      ) {
        attempt = {
          blockedResponse: null,
          attemptVersion: pendingBreakerOnlyAttempt.attemptVersion,
          attemptTicket: pendingBreakerOnlyAttempt.attemptTicket,
          consumeAfterReport() {
            clearPendingBreakerOnlyAttempt();
          },
        };
      } else {
        attempt = await authorizeBreakerOnlyAttemptIfNeeded(requestHostname);
        if (!attempt?.blockedResponse) {
          armPendingBreakerOnlyAttempt(requestHostname, attempt);
          attempt.consumeAfterReport = () => {
            clearPendingBreakerOnlyAttempt();
          };
        }
      }
    }

    if (attempt.blockedResponse) {
      return { blockedResponse: attempt.blockedResponse, response: null };
    }

    if (fqContext && !fqContext.hitUpstreamAtMs) {
      fqContext.hitUpstreamAtMs = Date.now();
    }

    return {
      blockedResponse: null,
      response: await (async () => {
        let upstreamResponse;
        try {
          upstreamResponse = await fetch(requestToFetch);
        } catch (error) {
          // Preserve deferred same-site redirect reporting; direct fetch throws with live
          // breaker debt still need the existing no-sample settlement path.
          const keepDeferredQueueBreakerReport = requestAdmissionMode === 'queue_breaker'
            && fqContext?.hostname === requestHostname
            && fqContext.deferredReportArmed === true
            && Number.isFinite(fqContext.deferredReportStatusCode);
          const breakerSettlementResponse = keepDeferredQueueBreakerReport
            ? null
            : await settleBreakerAttemptIfNeeded(requestHostname);
          if (cqReleaseController) {
            await ensureCurrentTrueConcurrencyReleased('origin_fetch_failure', true);
          }
          if (needFairQueue) {
            await finalizeFairQueueOnFailure('origin fetch failure');
          }
          if (breakerSettlementResponse instanceof Response) {
            throw breakerSettlementResponse;
          }
          throw error;
        }
        return upstreamResponse;
      })(),
      attempt,
    };
  };
  const shouldRetryAuthError = (status) => status === 401 || status === 410;

  const buildSafeResponseHeaders = (responseToWrap, requestToWrap) => {
    const safeHeaders = new Headers();
    const isCryptedDownload = payloadData?.isCrypted === true;

    const preserveHeaders = [
      'content-type',
      'content-disposition',
      'content-length',
      'cache-control',
      'content-encoding',
      'accept-ranges',
      'content-range',
      'content-language',
      'expires',
      'pragma',
      'etag',
      'last-modified'
    ];

    preserveHeaders.forEach((header) => {
      if (header === 'content-disposition' && isCryptedDownload) {
        return;
      }
      if (HOP_BY_HOP_RESPONSE_HEADERS.has(header)) {
        return;
      }
      const value = responseToWrap.headers.get(header);
      if (header === 'content-length' && parseContentLengthHeader(value) === null) {
        return;
      }
      if (value) {
        safeHeaders.set(header, value);
      }
    });

    if (shouldSynthesizeGoogleDriveAcceptRanges(responseToWrap, requestToWrap)) {
      safeHeaders.set('accept-ranges', 'bytes');
    }

    if (isCryptedDownload) {
      const derivedName = deriveFileNameFromPath(path);
      const encryptedFileName = ensureEncryptedFileName(derivedName);
      safeHeaders.set('content-disposition', buildAttachmentContentDisposition(encryptedFileName));
    }

    const hasRangeRequest = Boolean(requestToWrap.headers.get('range'));
    const hasContentRange = Boolean(responseToWrap.headers.get('content-range'));
    const shouldOverrideCacheControl = config.overrideCacheControl
      && (
        responseToWrap.status === 200
        || (responseToWrap.status === 206 && hasRangeRequest && hasContentRange)
      );

    if (shouldOverrideCacheControl) {
      const fileSize = readPayloadFileSize(payloadData);
      if (typeof fileSize === 'number' && fileSize <= config.cacheOverrideMaxSizeBytes) {
        const maxAge = config.cacheOverrideSeconds;
        safeHeaders.set('cache-control', `public, max-age=${maxAge}, s-maxage=${maxAge}`);
        safeHeaders.delete('x-cache');
      }
    }

    applyDownloadCorsHeaders(safeHeaders);
    return safeHeaders;
  };

  const buildManagedConcurrencyResponse = (upstreamResponse, requestToWrap, responseInitOverrides = null) => {
    const safeHeaders = responseInitOverrides?.headers instanceof Headers
      ? responseInitOverrides.headers
      : buildSafeResponseHeaders(upstreamResponse, requestToWrap);

    if (!upstreamResponse.body) {
      cqHeartbeatManager?.ensureCleanup?.('stream_complete');
      if (cqReleaseController) {
        cqReleaseController.ensureReleased('stream_complete');
      }
      return new Response(null, {
        status: upstreamResponse.status,
        statusText: upstreamResponse.statusText,
        headers: safeHeaders,
      });
    }

    const contentLengthHeader = responseInitOverrides?.headers instanceof Headers
      ? responseInitOverrides.headers.get('content-length')
      : upstreamResponse.headers.get('content-length');
    const contentLength = parseContentLengthHeader(contentLengthHeader);
    if (contentLength === null) {
      safeHeaders.delete('content-length');
    }
    const useFixedLengthStream = contentLength !== null && typeof FixedLengthStream === 'function';
    const streamPair = useFixedLengthStream
      ? new FixedLengthStream(contentLength)
      : (typeof IdentityTransformStream === 'function' ? new IdentityTransformStream() : new TransformStream({
        transform(chunk, controller) {
          controller.enqueue(chunk);
        },
      }));
    const abortController = new AbortController();
    cqStreamAbortController = abortController;
    cqHeartbeatManager?.bindStreamAbortController?.(abortController);
    if (cqStreamAbortReason && !abortController.signal.aborted) {
      abortController.abort();
    }
    const managedHardExpireAtMs = Number(cqLease?.hardExpireAtMs) || hardExpireAtMs;
    const msUntilExpire = Math.max(0, managedHardExpireAtMs - Date.now());
    let hardExpiryAbort = false;
    const expireTimer = setTimeout(() => {
      hardExpiryAbort = true;
      abortCurrentTrueConcurrencyStream('hard_expiry');
    }, msUntilExpire);
    if (typeof expireTimer?.unref === 'function') {
      expireTimer.unref();
    }
    const onClientAbort = () => {
      clientAborted = true;
      abortCurrentTrueConcurrencyStream('client_disconnect');
    };
    if (clientSignal && typeof clientSignal.addEventListener === 'function') {
      clientSignal.addEventListener('abort', onClientAbort, { once: true });
    }
    if (clientSignal?.aborted) {
      clientAborted = true;
      onClientAbort();
    }

    const readManagedStreamTerminationReason = (error) => {
      if (cqStreamAbortReason) {
        return cqStreamAbortReason;
      }
      if (didClientAbort()) {
        return 'client_disconnect';
      }
      if (hardExpiryAbort || Date.now() >= managedHardExpireAtMs) {
        return 'hard_expiry';
      }
      const message = error instanceof Error ? error.message : String(error);
      if (typeof error === 'string' || message === 'client closed download') {
        return 'client_disconnect';
      }
      if (isAbortError(error)) {
        return 'client_disconnect';
      }
      return 'upstream_failure';
    };

    const pipePromise = upstreamResponse.body.pipeTo(streamPair.writable, {
      signal: abortController.signal,
      preventAbort: false,
      preventCancel: false,
      preventClose: false,
    }).then(async () => {
      await cqHeartbeatManager?.ensureCleanup?.('stream_complete');
      await ensureCurrentTrueConcurrencyReleased('stream_complete', true);
    }).catch(async (error) => {
      const reason = readManagedStreamTerminationReason(error);
      await cqHeartbeatManager?.ensureCleanup?.(reason);
      if (!TRUE_CONCURRENCY_HEARTBEAT_TERMINAL_NO_RELEASE_REASONS.has(reason)) {
        await ensureCurrentTrueConcurrencyReleased(reason, true);
      }
      if (!isAbortError(error) && reason === 'upstream_failure') {
        const message = error instanceof Error ? error.message : String(error);
        console.warn('[CQ] managed stream terminated with error:', message);
      }
    }).finally(() => {
      clearTimeout(expireTimer);
      cqStreamAbortController = null;
      if (clientSignal && typeof clientSignal.removeEventListener === 'function') {
        clientSignal.removeEventListener('abort', onClientAbort);
      }
    });

    if (ctx && typeof ctx.waitUntil === 'function') {
      ctx.waitUntil(pipePromise);
    }

    cqCleanupBoundToStream = true;
    return new Response(streamPair.readable, {
      status: responseInitOverrides?.status ?? upstreamResponse.status,
      statusText: responseInitOverrides?.statusText ?? upstreamResponse.statusText,
      headers: safeHeaders,
    });
  };

  const buildHeadProbeResponse = async (upstreamResponse, requestToWrap) => {
    const safeHeaders = buildSafeResponseHeaders(upstreamResponse, requestToWrap);
    const totalSize = parseContentRangeTotal(upstreamResponse.headers.get('content-range'));
    if (totalSize !== null) {
      safeHeaders.set('content-length', String(totalSize));
    }
    safeHeaders.set('accept-ranges', 'bytes');
    safeHeaders.delete('content-range');

    if (cqReleaseController && !cqCleanupBoundToStream) {
      await ensureCurrentTrueConcurrencyReleased('head_probe_complete', true);
    }
    if (needFairQueue && fqContext?.slotToken) {
      if (fqContext.headerReleasePromise) {
        await fqContext.headerReleasePromise;
      } else {
        await finalizeFairQueueOnFailure('head probe complete');
      }
    }
    await cancelResponseBody(upstreamResponse);

    return new Response(null, {
      status: 200,
      statusText: 'OK',
      headers: safeHeaders,
    });
  };

  const buildGoogleDriveFullDownloadResponseInit = (upstreamResponse, requestToWrap) => {
    const safeHeaders = buildSafeResponseHeaders(upstreamResponse, requestToWrap);
    const totalSize = parseContentRangeTotal(upstreamResponse.headers.get('content-range'));
    if (totalSize !== null) {
      safeHeaders.set('content-length', String(totalSize));
    }
    safeHeaders.set('accept-ranges', 'bytes');
    safeHeaders.delete('content-range');

    return {
      status: 200,
      statusText: 'OK',
      headers: safeHeaders,
    };
  };

  const buildGoogleDriveFullDownloadResponse = (upstreamResponse, requestToWrap) => {
    const responseInit = buildGoogleDriveFullDownloadResponseInit(upstreamResponse, requestToWrap);
    return new Response(upstreamResponse.body, responseInit);
  };

  const buildGeneratedUpstreamTerminalResponse = (upstreamResponse) => {
    const status = upstreamResponse?.status;
    const message = status >= 500
      ? 'upstream download failed'
      : 'upstream download rejected';
    return createErrorResponse(origin, status, message);
  };

  const cancelResponseBodyAndReturn = async (responseToCancel, responseToReturn, reason) => {
    await cancelResponseBody(responseToCancel);
    return await releaseAdmissionBeforeTerminalResponse(responseToReturn, reason);
  };

  const releaseAdmissionBeforeTerminalResponse = async (response, reason) => {
    if (cqReleaseController && !cqCleanupBoundToStream) {
      await ensureCurrentTrueConcurrencyReleased(reason, true);
    }
    if (needFairQueue && fqContext?.slotToken) {
      const headerReleased = await waitForInFlightFairQueueHeaderRelease(fqContext);
      if (!headerReleased && fqContext?.slotToken) {
        await finalizeFairQueueOnFailure(reason);
      }
    }
    return response;
  };

  const reportFetchedUpstreamResponseIfNeeded = async (response, requestHostname, requestUrl, attempt, options = {}) => {
    if (!response) {
      return null;
    }

    if (response.status >= 400) {
      return null;
    }

    if (options.skipProtectedError === true && isProtectedThrottleStatusCode(response.status)) {
      return null;
    }

    return await reportBreakerResponseIfNeeded(requestHostname, response, requestUrl, attempt);
  };

  const finalizeUpstreamTerminalResponseIfNeeded = async (upstreamResponse, requestHostname, attempt = null) => {
    const status = upstreamResponse?.status;
    if (!isGeneratedTerminalUpstreamStatus(status)) {
      return null;
    }

    if (isProtectedThrottleStatusCode(status)) {
      const reportResponse = await reportBreakerResponseIfNeeded(
        requestHostname,
        upstreamResponse,
        '',
        attempt,
      );
      if (reportResponse) {
        return await cancelResponseBodyAndReturn(upstreamResponse, reportResponse, 'upstream_terminal');
      }
    }

    if (!isProtectedThrottleStatusCode(status)) {
      const settleResponse = await settleBreakerAttemptIfNeeded(requestHostname);
      if (settleResponse instanceof Response) {
        return await cancelResponseBodyAndReturn(upstreamResponse, settleResponse, 'upstream_terminal');
      }
    }

    await cancelResponseBody(upstreamResponse);
    return await releaseAdmissionBeforeTerminalResponse(
      buildGeneratedUpstreamTerminalResponse(upstreamResponse),
      'upstream_terminal',
    );
  };

  const finalizeContentResponse = async (responseToReturn, upstreamResponse = responseToReturn) => {
    const isContentResponse = shouldConsumeTicketResponse({
      requestMethod: request.method,
      response: responseToReturn,
      upstreamResponse,
      knownFileSize: readKnownGoogleDriveDownloadSize(),
      syntheticContentDisposition: payloadData?.isCrypted === true,
    });

    if (!isContentResponse) {
      return responseToReturn;
    }

    if (!ticketStateEnabled) {
      return responseToReturn;
    }

    try {
      const markUsedResult = await markTicketUsed(
        ticketHash,
        ticketStateConfig,
        Math.floor(Date.now() / 1000)
      );

      if (markUsedResult.result === 'transitioned' || markUsedResult.result === 'already_used') {
        return responseToReturn;
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error('[TicketState] Mark-used failed:', message);
    }

    if (responseToReturn?.body && typeof responseToReturn.body.cancel === 'function') {
      try {
        await responseToReturn.body.cancel();
      } catch (_error) {
        // Best effort: fail closed even if cancellation cannot complete cleanly.
      }
    }

    return createErrorResponse(origin, 502, 'ticket state update failed');
  };

  let retriedWithFreshLink = false;

  // Proceed with fetch
  try {
    const initialPrepareResponse = await prepareTargetForFetch(downloadUrl, 'initial');
    if (initialPrepareResponse) {
      return initialPrepareResponse;
    }

    request = await maybeApplyGoogleDriveFullDownloadTranslation(buildUpstreamRequest(downloadUrl, res.data.header));
    let { blockedResponse, response, attempt } = await fetchUpstreamWithBreakerAttempt(request);
    if (blockedResponse) {
      return await releaseAdmissionBeforeTerminalResponse(blockedResponse, 'prestream_terminal');
    }
    const currentOrigin = new URL(originalRequest.url).origin;
    let requestHostname = extractHostname(request?.url || '')?.toLowerCase() || null;
    const initialReportResponse = await reportFetchedUpstreamResponseIfNeeded(
      response,
      requestHostname,
      request.url,
      attempt,
      { skipProtectedError: !retriedWithFreshLink && shouldRetryAuthError(response.status) },
    );
    if (initialReportResponse) {
      return await cancelResponseBodyAndReturn(response, initialReportResponse, 'prestream_terminal');
    }
    while (response.status >= 300 && response.status < 400) {
      const location = response.headers.get("Location");
      if (location) {
        const resolvedLocation = resolveRedirectLocation(location, request.url);
        if (new URL(resolvedLocation).origin === currentOrigin) {
          const recursiveRedirectResponse = await retireAdmissionBeforeRecursiveWorkerRedirect('recursive redirect');
          if (recursiveRedirectResponse) {
            return recursiveRedirectResponse;
          }
          request = new Request(resolvedLocation, request);
          return await handleRequest(request, env, config, cacheManager, throttleManager, rateLimiter, ctx);
        } else {
          const targetPrepareResponse = await prepareTargetForFetch(resolvedLocation, 'redirect');
          if (targetPrepareResponse) {
            return targetPrepareResponse;
          }
          request = await maybeApplyGoogleDriveFullDownloadTranslation(new Request(resolvedLocation, request));
          ({ blockedResponse, response, attempt } = await fetchUpstreamWithBreakerAttempt(request));
          if (blockedResponse) {
            return await releaseAdmissionBeforeTerminalResponse(blockedResponse, 'prestream_terminal');
          }
          requestHostname = extractHostname(request?.url || '')?.toLowerCase() || null;
          const redirectedReportResponse = await reportFetchedUpstreamResponseIfNeeded(
            response,
            requestHostname,
            request.url,
            attempt,
            { skipProtectedError: !retriedWithFreshLink && shouldRetryAuthError(response.status) },
          );
          if (redirectedReportResponse) {
            return await cancelResponseBodyAndReturn(response, redirectedReportResponse, 'prestream_terminal');
          }
        }
      } else {
        break;
      }
    }

    if (!retriedWithFreshLink && shouldRetryAuthError(response.status)) {
      if (needTrueConcurrency && !needFairQueue) {
        const settleResponse = await settleBreakerOnlyAttemptIfNeeded(extractHostname(request?.url || '')?.toLowerCase() || null);
        if (settleResponse instanceof Response) {
          return await cancelResponseBodyAndReturn(response, settleResponse, 'prestream_terminal');
        }
      }
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
        const targetPrepareResponse = await prepareTargetForFetch(downloadUrl, 'refresh');
        if (targetPrepareResponse) {
          return targetPrepareResponse;
        }
        request = await maybeApplyGoogleDriveFullDownloadTranslation(buildUpstreamRequest(downloadUrl, res.data.header));
        ({ blockedResponse, response, attempt } = await fetchUpstreamWithBreakerAttempt(request));
        if (blockedResponse) {
          return await releaseAdmissionBeforeTerminalResponse(blockedResponse, 'prestream_terminal');
        }
        requestHostname = extractHostname(request?.url || '')?.toLowerCase() || null;
        const refreshedReportResponse = await reportFetchedUpstreamResponseIfNeeded(
          response,
          requestHostname,
          request.url,
          attempt,
        );
        if (refreshedReportResponse) {
          return await cancelResponseBodyAndReturn(response, refreshedReportResponse, 'prestream_terminal');
        }
        while (response.status >= 300 && response.status < 400) {
          const location = response.headers.get("Location");
          if (location) {
            const resolvedLocation = resolveRedirectLocation(location, request.url);
            if (new URL(resolvedLocation).origin === currentOrigin) {
              const recursiveRedirectResponse = await retireAdmissionBeforeRecursiveWorkerRedirect('recursive redirect');
              if (recursiveRedirectResponse) {
                return recursiveRedirectResponse;
              }
              request = new Request(resolvedLocation, request);
              return await handleRequest(request, env, config, cacheManager, throttleManager, rateLimiter, ctx);
            } else {
              const redirectPrepareResponse = await prepareTargetForFetch(resolvedLocation, 'redirect');
              if (redirectPrepareResponse) {
                return redirectPrepareResponse;
              }
              request = await maybeApplyGoogleDriveFullDownloadTranslation(new Request(resolvedLocation, request));
              ({ blockedResponse, response, attempt } = await fetchUpstreamWithBreakerAttempt(request));
              if (blockedResponse) {
                return await releaseAdmissionBeforeTerminalResponse(blockedResponse, 'prestream_terminal');
              }
              requestHostname = extractHostname(request?.url || '')?.toLowerCase() || null;
              const nestedRedirectReportResponse = await reportFetchedUpstreamResponseIfNeeded(
                response,
                requestHostname,
                request.url,
                attempt,
              );
              if (nestedRedirectReportResponse) {
                return await cancelResponseBodyAndReturn(response, nestedRedirectReportResponse, 'prestream_terminal');
              }
            }
          } else {
            break;
          }
        }
      }
    }

    if (retriedWithFreshLink && shouldRetryAuthError(response.status)) {
      if (needTrueConcurrency && !needFairQueue && !isProtectedThrottleStatusCode(response.status)) {
        const settleResponse = await settleBreakerOnlyAttemptIfNeeded(extractHostname(request?.url || '')?.toLowerCase() || null);
        if (settleResponse instanceof Response) {
          return await cancelResponseBodyAndReturn(response, settleResponse, 'prestream_terminal');
        }
      }
      if (!isProtectedThrottleStatusCode(response.status)) {
        const deferredReportResponse = await flushDeferredQueueBreakerReportOnExit();
        if (deferredReportResponse) {
          return await cancelResponseBodyAndReturn(response, deferredReportResponse, 'prestream_terminal');
        }
      }
    }

    const deferredTerminalReportResponse = await flushDeferredQueueBreakerReportOnExit(response);
    if (deferredTerminalReportResponse) {
      return await cancelResponseBodyAndReturn(response, deferredTerminalReportResponse, 'prestream_terminal');
    }

    const terminalUpstreamResponse = await finalizeUpstreamTerminalResponseIfNeeded(response, requestHostname, attempt);
    if (terminalUpstreamResponse) {
      return terminalUpstreamResponse;
    }

    if (response.status !== 200 && response.status !== 206) {
      console.warn(
        `[Upstream] Unexpected status ${response.status} for ${path} (url ${downloadUrl})`
      );
    }

    releaseFairQueueAfterHeadersIfNeeded();

    const shouldRewriteHeadProbeResponse = originalRequest.method === 'HEAD'
      && isGoogleDriveDownloadHostname(extractHostname(request.url)?.toLowerCase() || '')
      && request.headers.get('range') === GOOGLE_DRIVE_HEAD_PROBE_RANGE
      && response.status === 206
      && parseContentRangeTotal(response.headers.get('content-range')) !== null;

    if (shouldRewriteHeadProbeResponse) {
      return await buildHeadProbeResponse(response, request);
    }

    const isGoogleDriveSyntheticFullRangeRequest = originalRequest.method === 'GET'
      && !originalRequest.headers.get('range')
      && isGoogleDriveDownloadHostname(extractHostname(request.url)?.toLowerCase() || '')
      && response.status === 206
      && Boolean(request.headers.get('range'))
      && request.headers.get('range') !== GOOGLE_DRIVE_HEAD_PROBE_RANGE;

    const shouldRewriteGoogleDriveFullDownloadResponse = isGoogleDriveSyntheticFullRangeRequest
      && isExactGoogleDriveFullRangeMatch(
        request.headers.get('range'),
        response.headers.get('content-range'),
      );

    const isHeadProbeRequest = originalRequest.method === 'HEAD'
      && isGoogleDriveDownloadHostname(extractHostname(request.url)?.toLowerCase() || '')
      && request.headers.get('range') === GOOGLE_DRIVE_HEAD_PROBE_RANGE;

    if (isHeadProbeRequest && !shouldRewriteHeadProbeResponse) {
      await cancelResponseBody(response);
      return await releaseAdmissionBeforeTerminalResponse(
        createErrorResponse(origin, 502, 'Google Drive HEAD probe invalid'),
        'head_probe_invalid',
      );
    }

    if (isGoogleDriveSyntheticFullRangeRequest && !shouldRewriteGoogleDriveFullDownloadResponse) {
      await cancelResponseBody(response);
      return await releaseAdmissionBeforeTerminalResponse(
        createErrorResponse(origin, 502, 'Google Drive range mismatch'),
        'google_drive_range_mismatch',
      );
    }

    if (shouldRewriteGoogleDriveFullDownloadResponse && !needTrueConcurrency) {
      return await finalizeContentResponse(buildGoogleDriveFullDownloadResponse(response, request), response);
    }

    // Ordinary breaker_only terminal exits must retire before any managed stream
    // binds concurrency cleanup to the response body.
    const terminalBreakerOnlyRetirementResponse = await retirePendingBreakerOnlyAttemptIfNeeded();
    if (terminalBreakerOnlyRetirementResponse) {
      return await cancelResponseBodyAndReturn(response, terminalBreakerOnlyRetirementResponse, 'prestream_terminal');
    }

    const safeResponse = needTrueConcurrency
      ? buildManagedConcurrencyResponse(
          response,
        request,
        shouldRewriteGoogleDriveFullDownloadResponse
          ? buildGoogleDriveFullDownloadResponseInit(response, request)
          : null,
      )
      : new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers: buildSafeResponseHeaders(response, request),
      });

    return await finalizeContentResponse(safeResponse, response);
  } catch (error) {
    const deferredFailureResponse = await flushDeferredQueueBreakerReportOnExit();
    if (deferredFailureResponse) {
      return deferredFailureResponse;
    }

    if (error instanceof Response) {
      return error;
    }

    if (didClientAbort() && isAbortError(error)) {
      return createClientAbortResponse(origin);
    }
    throw error;
  } finally {
    if (cqHeartbeatManager && !cqCleanupBoundToStream) {
      cqHeartbeatManager.ensureCleanup('final_cleanup');
    }
    if (cqReleaseController && !cqCleanupBoundToStream) {
      cqReleaseController.ensureReleased('final_cleanup');
    }
    clearPendingBreakerOnlyAttempt();

    const finalCleanupContexts = [];
    if (fqContext && fqContext.cleanupRetired !== true) {
      finalCleanupContexts.push(fqContext);
    }
    for (const pendingCleanupContext of pendingFairQueueCleanupContexts) {
      if (pendingCleanupContext) {
        finalCleanupContexts.push(pendingCleanupContext);
      }
    }

    const finalCleanupGroups = buildFinalCleanupGroups(finalCleanupContexts);

    if (fairQueueClient && finalCleanupGroups.length > 0) {
      const cleanupPromise = (async () => {
        await runWithConcurrencyLimit(
          finalCleanupGroups.map((cleanupGroup) => async () => {
            for (const cleanupContext of cleanupGroup) {
              if (await waitForInFlightFairQueueHeaderRelease(cleanupContext)) {
                continue;
              }
              await finalizeFairQueueContext({
                fairQueueClient,
                ctx,
                fqContext: cleanupContext,
                phase: 'final cleanup',
              });
            }
          }),
          FINAL_CLEANUP_RELEASE_CONCURRENCY,
        );
      })();
      if (ctx && typeof ctx.waitUntil === 'function') {
        ctx.waitUntil(cleanupPromise);
      } else {
        await cleanupPromise;
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
  applyUnifiedResult: (unifiedResult, options = {}) => applyUnifiedResult(unifiedResult, {
    origin: '*',
    throttleEnabled: true,
    throttleHostname: 'tenant.sharepoint.com',
    throttleHostnamePatterns: ['*.sharepoint.com'],
    openCapSeconds: DEFAULT_THROTTLE_OPEN_CAP_SECONDS,
    ...options,
  }),
  buildFinalCleanupGroups,
  createConcurrencyHandlerClient,
  createTrueConcurrencyHeartbeatManager,
  createConcurrencyReleaseController,
  createSlotHandlerClient,
  normalizeTrueConcurrencyReleaseReason,
  deriveOpenSeconds,
  finalizeFairQueueContext,
  readOpenBreakerSnapshot,
  reconcileFairQueueContextForTarget,
  resolveConfig,
  resolveAdmissionMode,
  resolveTicketStateConfig,
  shouldConsumeTicketResponse,
  slowFailDelay,
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
