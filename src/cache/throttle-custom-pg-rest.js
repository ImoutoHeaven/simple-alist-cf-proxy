import { sha256Hash, applyVerifyHeaders, hasVerifyCredentials } from '../utils.js';
import { logEvent } from '../logging.js';
const BREAKER_TABLE = 'THROTTLE_PROTECTION';
const DEFAULT_CLOSE_THRESHOLD_PERCENT = 15;
const DEFAULT_HALF_OPEN_SUCCESS_THRESHOLD = 2;
const DEFAULT_HALF_OPEN_CLOSE_MODE = 'and';
const DEFAULT_HALF_OPEN_MAX_PROBE_COUNT = 4;
const DEFAULT_HALF_OPEN_MAX_SECONDS = 15;
const DEFAULT_HALF_OPEN_TIMEOUT_MODE = 'partial-close';
const VALID_BREAKER_STATES = new Set(['closed', 'open', 'half_open']);
const VALID_HALF_OPEN_CLOSE_MODES = new Set(['and', 'or']);
const VALID_HALF_OPEN_TIMEOUT_MODES = new Set(['open', 'close', 'partial-close']);

const sanitizeThresholds = (config = {}) => {
  const toInt = (value, fallback) => {
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) ? parsed : fallback;
  };

  return {
    openCapSeconds: Math.max(1, toInt(config.openCapSeconds, 60)),
    openThresholdPercent: Math.max(0, toInt(config.openThresholdPercent, 30)),
    closeThresholdPercent: Math.max(0, toInt(config.closeThresholdPercent, DEFAULT_CLOSE_THRESHOLD_PERCENT)),
    ewmaSpan: Math.max(1, toInt(config.ewmaSpan, 8)),
    consecutiveThreshold: Math.max(1, toInt(config.consecutiveThreshold, 4)),
    minSamplesBeforeEwmaOpen: Math.max(1, toInt(config.minSamplesBeforeEwmaOpen, 8)),
    idleResetSeconds: Math.max(0, toInt(config.idleResetSeconds, 900)),
    halfOpenSuccessThreshold: Math.max(1, toInt(config.halfOpenSuccessThreshold, DEFAULT_HALF_OPEN_SUCCESS_THRESHOLD)),
    halfOpenMaxProbeCount: Math.max(1, toInt(config.halfOpenMaxProbeCount, DEFAULT_HALF_OPEN_MAX_PROBE_COUNT)),
    halfOpenMaxSeconds: Math.max(1, toInt(config.halfOpenMaxSeconds, DEFAULT_HALF_OPEN_MAX_SECONDS)),
  };
};

const normalizeEnum = (value, validValues, fallback) => {
  if (typeof value !== 'string') {
    return fallback;
  }
  const normalized = value.trim().toLowerCase();
  return validValues.has(normalized) ? normalized : fallback;
};

const readBreakerField = (row, upperKey, lowerKey = upperKey.toLowerCase()) => {
  if (!row || typeof row !== 'object') {
    return undefined;
  }
  if (Object.prototype.hasOwnProperty.call(row, upperKey)) {
    return row[upperKey];
  }
  return row[lowerKey];
};

const parseNullableInt = (value) => {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isNaN(parsed) ? null : parsed;
};

const normalizeBreakerState = (value) => {
  if (typeof value !== 'string') {
    return null;
  }
  const normalized = value.trim().toLowerCase();
  return VALID_BREAKER_STATES.has(normalized) ? normalized : null;
};

const emptyBreakerSnapshot = () => ({
  recordExists: false,
  state: null,
  openUntil: null,
  reason: null,
  version: null,
  lastErrorCode: null,
});

const normalizeBreakerReason = (value) => {
  if (typeof value !== 'string') {
    return null;
  }
  const trimmed = value.trim();
  return trimmed === '' ? null : trimmed;
};

const parseBoolean = (value) => {
  if (value === true || value === 1 || value === '1') {
    return true;
  }
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    return normalized === 'true' || normalized === 't';
  }
  return false;
};

const readBreakerSnapshot = (row, options = {}) => {
  if (!row || typeof row !== 'object') {
    return emptyBreakerSnapshot();
  }

  const snapshot = {
    recordExists: true,
    state: normalizeBreakerState(readBreakerField(row, 'STATE')),
    openUntil: parseNullableInt(readBreakerField(row, 'OPEN_UNTIL')),
    reason: normalizeBreakerReason(readBreakerField(row, 'OPEN_REASON')),
    version: parseNullableInt(readBreakerField(row, 'VERSION')),
    lastErrorCode: parseNullableInt(readBreakerField(row, 'LAST_ERROR_CODE')),
  };

  if (options.includeHalfOpenDeadline) {
    snapshot.halfOpenDeadline = parseNullableInt(readBreakerField(row, 'HALF_OPEN_DEADLINE'));
  }

  if (options.includeAttemptAuthorization) {
    snapshot.attemptGranted = parseBoolean(readBreakerField(row, 'ATTEMPT_GRANTED'));
    snapshot.attemptTicket = parseNullableInt(readBreakerField(row, 'ATTEMPT_TICKET'));
  }

  return snapshot;
};

/**
 * Execute query via PostgREST API
 * @param {string} postgrestUrl - PostgREST API base URL
 * @param {string|string[]} verifyHeader - Authentication header name(s)
 * @param {string|string[]} verifySecret - Authentication header value(s)
 * @param {string} tableName - Table name
 * @param {string} method - HTTP method (GET, POST, PATCH, DELETE)
 * @param {string} filters - URL query filters (for GET/PATCH/DELETE)
 * @param {Object} body - Request body (for POST/PATCH)
 * @param {Object} extraHeaders - Additional headers
 * @returns {Promise<Object>} - Query result
 */
const executeQuery = async (postgrestUrl, verifyHeader, verifySecret, tableName, method, filters = '', body = null, extraHeaders = {}) => {
  const url = `${postgrestUrl}/${tableName}${filters ? `?${filters}` : ''}`;

  const headers = {
    'Content-Type': 'application/json',
    ...extraHeaders,
  };
  applyVerifyHeaders(headers, verifyHeader, verifySecret);

  const options = {
    method,
    headers,
  };

  if (body) {
    options.body = JSON.stringify(body);
  }

  const response = await fetch(url, options);

  if (!response.ok) {
    const errorText = await response.text();

    // Check if table doesn't exist (PGRST205 error)
    if (response.status === 404 && errorText.includes('PGRST205')) {
      throw new Error(
        `PostgREST table not found: "${tableName}". ` +
        `Please create the table manually using init.sql. ` +
        `CREATE TABLE ${tableName} (...) (see init.sql for full schema)`
      );
    }

    throw new Error(`PostgREST API error (${response.status}): ${errorText}`);
  }

  // For POST/PATCH/DELETE, PostgREST returns the affected rows or empty
  // For GET, it returns an array of rows
  let result;
  const contentType = response.headers.get('content-type');
  if (contentType && contentType.includes('application/json')) {
    result = await response.json();
  } else {
    result = [];
  }

  // Get Content-Range header to determine affected rows count
  const contentRange = response.headers.get('content-range');
  let affectedRows = 0;
  if (contentRange) {
    // Content-Range format: "0-4/*" or "*/0" (no matches)
    const match = contentRange.match(/(\d+)-(\d+)|\*\/(\d+)/);
    if (match) {
      if (match[1] !== undefined && match[2] !== undefined) {
        affectedRows = parseInt(match[2], 10) - parseInt(match[1], 10) + 1;
      } else if (match[3] !== undefined) {
        affectedRows = parseInt(match[3], 10);
      }
    }
  } else if (method === 'POST' && response.status === 201) {
    // POST successful, assume 1 row inserted
    affectedRows = 1;
  } else if (method === 'PATCH' || method === 'DELETE') {
    // For PATCH/DELETE without Prefer: return=representation
    // We need to use Prefer: return=minimal and check if response is empty
    affectedRows = Array.isArray(result) ? result.length : 0;
  }

  return {
    data: Array.isArray(result) ? result : [],
    affectedRows,
  };
};

const executeBreakerRpc = async (postgrestUrl, verifyHeader, verifySecret, rpcName, body) => {
  const rpcUrl = `${postgrestUrl}/rpc/${rpcName}`;
  const rpcHeaders = { 'Content-Type': 'application/json' };
  applyVerifyHeaders(rpcHeaders, verifyHeader, verifySecret);

  const rpcResponse = await fetch(rpcUrl, {
    method: 'POST',
    headers: rpcHeaders,
    body: JSON.stringify(body),
  });

  if (!rpcResponse.ok) {
    const errorText = await rpcResponse.text();
    throw new Error(`PostgREST RPC ${rpcName} failed (${rpcResponse.status}): ${errorText}`);
  }

  const rpcResult = await rpcResponse.json();
  if (!Array.isArray(rpcResult) || rpcResult.length === 0) {
    throw new Error(`PostgREST RPC ${rpcName} returned no rows`);
  }

  return rpcResult[0];
};

/**
 * Read the raw breaker snapshot for a hostname
 * @param {string} hostname - Hostname to check
 * @param {Object} config - Throttle configuration
 * @param {string} config.postgrestUrl - PostgREST API endpoint
 * @param {string|string[]} config.verifyHeader - Authentication header name(s)
 * @param {string|string[]} config.verifySecret - Authentication header value(s)
 * @returns {Promise<{recordExists: boolean, state: string|null, openUntil: number|null, reason: string|null, version: number|null, lastErrorCode: number|null} | null>}
 */
export const getBreakerState = async (hostname, config) => {
  if (!config.postgrestUrl || !hasVerifyCredentials(config.verifyHeader, config.verifySecret)) {
    return null;
  }

  if (!hostname || typeof hostname !== 'string') {
    return null;
  }

  const { postgrestUrl, verifyHeader, verifySecret } = config;

  // Calculate hostname hash
  const hostnameHash = await sha256Hash(hostname);
  if (!hostnameHash) {
    throw new Error('Failed to calculate hostname hash');
  }

  const filters = `HOSTNAME_HASH=eq.${hostnameHash}`;
  const queryResult = await executeQuery(
    postgrestUrl,
    verifyHeader,
    verifySecret,
    BREAKER_TABLE,
    'GET',
    filters
  );

  const records = queryResult.data || [];

  if (!records || records.length === 0) {
    return emptyBreakerSnapshot();
  }

  const result = records[0];

  return readBreakerSnapshot(result);
};

/**
 * Authorize a half-open breaker attempt for a hostname
 * @param {string} hostname - Hostname
 * @param {Object} config - Throttle configuration
 * @returns {Promise<{recordExists: boolean, state: string|null, openUntil: number|null, reason: string|null, version: number|null, lastErrorCode: number|null, halfOpenDeadline?: number|null, attemptGranted?: boolean, attemptTicket?: number|null} | null>}
 */
export const authorizeBreakerAttempt = async (hostname, config) => {
  if (!config.postgrestUrl || !hasVerifyCredentials(config.verifyHeader, config.verifySecret)) {
    return null;
  }

  if (!hostname || typeof hostname !== 'string') {
    return null;
  }

  const { postgrestUrl, verifyHeader, verifySecret } = config;
  const thresholds = sanitizeThresholds(config);
  const hostnameHash = await sha256Hash(hostname);
  if (!hostnameHash) {
    throw new Error('Failed to calculate hostname hash');
  }

  const now = Math.floor(Date.now() / 1000);
  const halfOpenTimeoutMode = normalizeEnum(
    config?.halfOpenTimeoutMode,
    VALID_HALF_OPEN_TIMEOUT_MODES,
    DEFAULT_HALF_OPEN_TIMEOUT_MODE,
  );

  const row = await executeBreakerRpc(
    postgrestUrl,
    verifyHeader,
    verifySecret,
    'download_authorize_breaker_attempt',
    {
      p_hostname_hash: hostnameHash,
      p_hostname: hostname,
      p_now: now,
      p_open_cap_seconds: thresholds.openCapSeconds,
      p_close_threshold_percent: thresholds.closeThresholdPercent,
      p_half_open_success_threshold: thresholds.halfOpenSuccessThreshold,
      p_half_open_close_mode: normalizeEnum(
        config?.halfOpenCloseMode,
        VALID_HALF_OPEN_CLOSE_MODES,
        DEFAULT_HALF_OPEN_CLOSE_MODE,
      ),
      p_half_open_max_probe_count: thresholds.halfOpenMaxProbeCount,
      p_half_open_max_seconds: thresholds.halfOpenMaxSeconds,
      p_half_open_timeout_mode: halfOpenTimeoutMode,
    },
  );

  return readBreakerSnapshot(row, {
    includeHalfOpenDeadline: true,
    includeAttemptAuthorization: true,
  });
};

export const settleBreakerAttempt = async (hostname, updateData, config) => {
  if (!config.postgrestUrl || !hasVerifyCredentials(config.verifyHeader, config.verifySecret)) {
    return null;
  }

  if (!hostname || typeof hostname !== 'string') {
    return null;
  }

  const attemptVersionRaw = Number.isFinite(updateData?.attemptVersion)
    ? Number(updateData.attemptVersion)
    : Number.parseInt(updateData?.attemptVersion, 10);
  const attemptTicketRaw = Number.isFinite(updateData?.attemptTicket)
    ? Number(updateData.attemptTicket)
    : Number.parseInt(updateData?.attemptTicket, 10);
  const attemptVersion = Number.isFinite(attemptVersionRaw) ? Math.trunc(attemptVersionRaw) : null;
  const attemptTicket = Number.isFinite(attemptTicketRaw) ? Math.trunc(attemptTicketRaw) : null;

  if (!Number.isFinite(attemptVersion) || !Number.isFinite(attemptTicket)) {
    logEvent('warn', 'Throttle', 'invalid_attempt_identity', {
      attemptVersion: updateData?.attemptVersion,
      attemptTicket: updateData?.attemptTicket,
    });
    return null;
  }

  const { postgrestUrl, verifyHeader, verifySecret } = config;
  const hostnameHash = await sha256Hash(hostname);
  if (!hostnameHash) {
    throw new Error('Failed to calculate hostname hash');
  }

  const row = await executeBreakerRpc(
    postgrestUrl,
    verifyHeader,
    verifySecret,
    'download_settle_breaker_attempt',
    {
      p_hostname_hash: hostnameHash,
      p_hostname: hostname,
      p_attempt_version: attemptVersion,
      p_attempt_ticket: attemptTicket,
      p_now: Math.floor(Date.now() / 1000),
    },
  );

  return readBreakerSnapshot(row, { includeHalfOpenDeadline: true });
};

/**
 * Report a breaker sample for a hostname using the breaker report RPC
 * @param {string} hostname - Hostname
 * @param {Object} updateData - Update data
 * @param {number} updateData.sample - 1 for protectable error, 0 for success
 * @param {number} updateData.statusCode - HTTP status code for this event
 * @param {number|null} updateData.retryAfterSeconds - Parsed numeric Retry-After reopen duration
 * @param {Object} config - Throttle configuration
 * @returns {Promise<{recordExists: boolean, state: string|null, openUntil: number|null, reason: string|null, version: number|null, lastErrorCode: number|null} | null>}
 */
export const reportBreakerSample = async (hostname, updateData, config) => {
  if (!config.postgrestUrl || !hasVerifyCredentials(config.verifyHeader, config.verifySecret)) {
    return null;
  }

  if (!hostname || typeof hostname !== 'string') {
    return null;
  }

  const sampleValue = Number(updateData?.sample);
  const sample = sampleValue >= 1 ? 1 : 0;
  if (!Number.isFinite(sampleValue)) {
    logEvent('warn', 'Throttle', 'invalid_sample', { sample: updateData?.sample });
    return null;
  }

  const statusCode = Number.isFinite(updateData?.statusCode)
    ? Number(updateData.statusCode)
    : Number.parseInt(updateData?.statusCode, 10);
  const retryAfterSecondsRaw = Number.isFinite(updateData?.retryAfterSeconds)
    ? Number(updateData.retryAfterSeconds)
    : Number.parseInt(updateData?.retryAfterSeconds, 10);
  const attemptVersionRaw = Number.isFinite(updateData?.attemptVersion)
    ? Number(updateData.attemptVersion)
    : Number.parseInt(updateData?.attemptVersion, 10);
  const attemptTicketRaw = Number.isFinite(updateData?.attemptTicket)
    ? Number(updateData.attemptTicket)
    : Number.parseInt(updateData?.attemptTicket, 10);
  const retryAfterSeconds = Number.isFinite(retryAfterSecondsRaw) && retryAfterSecondsRaw > 0
    ? Math.max(1, Math.ceil(retryAfterSecondsRaw))
    : null;
  const attemptVersion = Number.isFinite(attemptVersionRaw) ? Math.trunc(attemptVersionRaw) : null;
  const attemptTicket = Number.isFinite(attemptTicketRaw) ? Math.trunc(attemptTicketRaw) : null;

  if (!Number.isFinite(statusCode)) {
    logEvent('warn', 'Throttle', 'invalid_status_code', { statusCode: updateData?.statusCode });
    return null;
  }

  const { postgrestUrl, verifyHeader, verifySecret } = config;
  const thresholds = sanitizeThresholds(config);
  const now = Math.floor(Date.now() / 1000);
  const halfOpenCloseMode = normalizeEnum(
    config?.halfOpenCloseMode,
    VALID_HALF_OPEN_CLOSE_MODES,
    DEFAULT_HALF_OPEN_CLOSE_MODE,
  );
  const halfOpenTimeoutMode = normalizeEnum(
    config?.halfOpenTimeoutMode,
    VALID_HALF_OPEN_TIMEOUT_MODES,
    DEFAULT_HALF_OPEN_TIMEOUT_MODE,
  );

  // Calculate hostname hash
  const hostnameHash = await sha256Hash(hostname);
  if (!hostnameHash) {
    throw new Error('Failed to calculate hostname hash');
  }

  const row = await executeBreakerRpc(
    postgrestUrl,
    verifyHeader,
    verifySecret,
    'download_report_breaker_sample',
    {
      p_hostname_hash: hostnameHash,
      p_hostname: hostname,
      p_now: now,
      p_sample: sample,
      p_status_code: statusCode,
      p_open_cap_seconds: thresholds.openCapSeconds,
      p_open_threshold_percent: thresholds.openThresholdPercent,
      p_close_threshold_percent: thresholds.closeThresholdPercent,
      p_ewma_span: thresholds.ewmaSpan,
      p_consecutive_threshold: thresholds.consecutiveThreshold,
      p_min_samples_before_ewma_open: thresholds.minSamplesBeforeEwmaOpen,
      p_idle_reset_seconds: thresholds.idleResetSeconds,
      p_half_open_success_threshold: thresholds.halfOpenSuccessThreshold,
      p_half_open_close_mode: halfOpenCloseMode,
      p_half_open_max_seconds: thresholds.halfOpenMaxSeconds,
      p_half_open_timeout_mode: halfOpenTimeoutMode,
      p_attempt_version: attemptVersion,
      p_attempt_ticket: attemptTicket,
      p_retry_after_seconds: retryAfterSeconds,
    },
  );

  logEvent('info', 'Throttle', 'breaker_updated', {
    hostname,
    state: readBreakerField(row, 'STATE'),
    openUntil: readBreakerField(row, 'OPEN_UNTIL'),
    reason: readBreakerField(row, 'OPEN_REASON'),
    version: readBreakerField(row, 'VERSION'),
    code: readBreakerField(row, 'LAST_ERROR_CODE'),
  });

  return readBreakerSnapshot(row);
};
