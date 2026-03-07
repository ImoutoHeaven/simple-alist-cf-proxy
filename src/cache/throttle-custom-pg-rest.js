import { sha256Hash, applyVerifyHeaders, hasVerifyCredentials } from '../utils.js';
const BREAKER_TABLE = 'THROTTLE_PROTECTION';
const DEFAULT_PROBE_LEASE_SECONDS = 15;
const VALID_BREAKER_STATES = new Set(['closed', 'open', 'half_open']);

const sanitizeThresholds = (config) => {
  const toInt = (value, fallback) => {
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) ? parsed : fallback;
  };

  return {
    openCapSeconds: Math.max(1, toInt(config.openCapSeconds, 60)),
    openThresholdPercent: Math.max(0, toInt(config.openThresholdPercent, 20)),
    ewmaSpan: Math.max(1, toInt(config.ewmaSpan, 8)),
    consecutiveThreshold: Math.max(1, toInt(config.consecutiveThreshold, 4)),
  };
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
  probeLeaseUntil: null,
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
    probeLeaseUntil: options.includeProbeLeaseUntil
      ? parseNullableInt(readBreakerField(row, 'PROBE_LEASE_UNTIL'))
      : undefined,
    lastErrorCode: parseNullableInt(readBreakerField(row, 'LAST_ERROR_CODE')),
  };

  if (!options.includeProbeLeaseUntil) {
    delete snapshot.probeLeaseUntil;
  }

  if (options.includeProbeGranted) {
    snapshot.probeGranted = parseBoolean(readBreakerField(row, 'PROBE_GRANTED'));
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
 * @returns {Promise<{recordExists: boolean, state: string|null, openUntil: number|null, reason: string|null, version: number|null, probeLeaseUntil: number|null, lastErrorCode: number|null} | null>}
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

  return readBreakerSnapshot(result, { includeProbeLeaseUntil: true });
};

/**
 * Claim the single half-open breaker probe lease for a hostname
 * @param {string} hostname - Hostname
 * @param {Object} config - Throttle configuration
 * @returns {Promise<{recordExists: boolean, state: string|null, openUntil: number|null, reason: string|null, version: number|null, lastErrorCode: number|null, probeLeaseUntil?: number|null, probeGranted?: boolean} | null>}
 */
export const claimBreakerProbe = async (hostname, config) => {
  if (!config.postgrestUrl || !hasVerifyCredentials(config.verifyHeader, config.verifySecret)) {
    return null;
  }

  if (!hostname || typeof hostname !== 'string') {
    return null;
  }

  const { postgrestUrl, verifyHeader, verifySecret } = config;
  const hostnameHash = await sha256Hash(hostname);
  if (!hostnameHash) {
    throw new Error('Failed to calculate hostname hash');
  }

  const now = Math.floor(Date.now() / 1000);
  const probeLeaseSeconds = Math.max(
    1,
    Number.parseInt(config?.probeLeaseSeconds, 10) || DEFAULT_PROBE_LEASE_SECONDS,
  );

  const row = await executeBreakerRpc(
    postgrestUrl,
    verifyHeader,
    verifySecret,
    'download_claim_breaker_probe',
    {
      p_hostname_hash: hostnameHash,
      p_hostname: hostname,
      p_now: now,
      p_probe_lease_seconds: probeLeaseSeconds,
    },
  );

  return readBreakerSnapshot(row, {
    includeProbeLeaseUntil: true,
    includeProbeGranted: true,
  });
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
    console.warn('[Throttle] Skip reportBreakerSample: invalid sample:', updateData?.sample);
    return null;
  }

  const statusCode = Number.isFinite(updateData?.statusCode)
    ? Number(updateData.statusCode)
    : Number.parseInt(updateData?.statusCode, 10);
  const retryAfterSecondsRaw = Number.isFinite(updateData?.retryAfterSeconds)
    ? Number(updateData.retryAfterSeconds)
    : Number.parseInt(updateData?.retryAfterSeconds, 10);
  const probeVersionRaw = Number.isFinite(updateData?.probeVersion)
    ? Number(updateData.probeVersion)
    : Number.parseInt(updateData?.probeVersion, 10);
  const retryAfterSeconds = Number.isFinite(retryAfterSecondsRaw) && retryAfterSecondsRaw > 0
    ? Math.max(1, Math.ceil(retryAfterSecondsRaw))
    : null;
  const probeVersion = Number.isFinite(probeVersionRaw) ? Math.trunc(probeVersionRaw) : null;

  if (!Number.isFinite(statusCode)) {
    console.warn('[Throttle] Skip reportBreakerSample: invalid statusCode:', updateData?.statusCode);
    return null;
  }

  const { postgrestUrl, verifyHeader, verifySecret } = config;
  const thresholds = sanitizeThresholds(config);
  const now = Math.floor(Date.now() / 1000);

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
      p_ewma_span: thresholds.ewmaSpan,
      p_consecutive_threshold: thresholds.consecutiveThreshold,
      p_probe_version: probeVersion,
      p_retry_after_seconds: retryAfterSeconds,
    },
  );

  console.log(
    `[Throttle] Updated breaker for ${hostname}: state=${readBreakerField(row, 'STATE')}, openUntil=${readBreakerField(row, 'OPEN_UNTIL')}, reason=${readBreakerField(row, 'OPEN_REASON')}, version=${readBreakerField(row, 'VERSION')}, code=${readBreakerField(row, 'LAST_ERROR_CODE')}`
  );

  return readBreakerSnapshot(row);
};
