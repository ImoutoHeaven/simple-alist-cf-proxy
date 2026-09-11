const POSTGREST_ORIGIN = 'https://postgrest.example.test';
const CACHE_STATE_URL = `${POSTGREST_ORIGIN}/rpc/download_get_cache_state`;
const CACHE_ACQUIRE_URL = `${POSTGREST_ORIGIN}/rpc/download_acquire_cache_refresh`;
const CACHE_FINISH_URL = `${POSTGREST_ORIGIN}/rpc/download_finish_cache_refresh`;
const CACHE_CLEANUP_URL = `${POSTGREST_ORIGIN}/rpc/download_cleanup_expired_cache`;
const TICKET_STATE_READ_URL = `${POSTGREST_ORIGIN}/rpc/download_get_ticket_state`;
const TICKET_STATE_MARK_URL = `${POSTGREST_ORIGIN}/rpc/download_mark_ticket_used`;

const jsonResponse = (payload, status = 200) => new Response(JSON.stringify(payload), {
  status,
  headers: { 'content-type': 'application/json' },
});

const now = () => Math.floor(Date.now() / 1000);
const timestamp = (seconds) => seconds ? new Date(seconds * 1000).toISOString() : null;

export const createCacheRpcFixture = ({
  includeTicketState = false,
  databaseNowSeconds = Math.floor(Date.now() / 1000),
  ticketIdleTimeoutSeconds = 300,
  acquireLeaseDurationSeconds = 180,
} = {}) => {
  const now = () => databaseNowSeconds;
  const leaseDurationSeconds = Number.isFinite(Number(acquireLeaseDurationSeconds))
    && Number(acquireLeaseDurationSeconds) > 0
    ? Number(acquireLeaseDurationSeconds)
    : 180;
  const calls = [];
  const entries = new Map();
  let leaseSequence = 0;
  let finishHandler = null;

  const reset = () => {
    calls.length = 0;
    entries.clear();
    leaseSequence = 0;
    finishHandler = null;
  };

  const setFinishHandler = (handler) => {
    finishHandler = typeof handler === 'function' ? handler : null;
  };

  const setLeaseDuration = (pathHash, seconds) => {
    const entry = getEntry(pathHash);
    entry.leaseUntilSeconds = databaseNowSeconds + Math.max(0, Number(seconds) || 0);
  };

  const seedLease = (pathHash, leaseId, seconds = 180) => {
    const entry = getEntry(pathHash);
    entry.leaseId = leaseId;
    entry.leaseUntilSeconds = databaseNowSeconds + Math.max(0, Number(seconds) || 0);
  };

  const getEntry = (pathHash) => {
    let entry = entries.get(pathHash);
    if (!entry) {
      entry = {
        cached: null,
        version: null,
        leaseId: null,
        leaseUntilSeconds: null,
        retryAfterSeconds: null,
        invalidVersion: null,
      };
      entries.set(pathHash, entry);
    }
    return entry;
  };

  const newLeaseId = () => {
    leaseSequence += 1;
    return `11111111-1111-4111-8111-${String(leaseSequence).padStart(12, '0')}`;
  };

  const isFresh = (entry) => {
    const expiresAt = entry.cached?.linkData?.download?.expires_at;
    return Boolean(
      entry.cached
      && entry.cached.timestamp + 1800 >= now()
      && Number.isSafeInteger(expiresAt)
      && expiresAt > now()
      && entry.invalidVersion !== entry.version,
    );
  };

  const stateRow = (entry, result) => ({
    result,
    path_hash: null,
    path: entry.cached?.path || null,
    link_data: result === 'ready' ? JSON.stringify(entry.cached.linkData) : null,
    cache_timestamp: result === 'ready' ? entry.cached.timestamp : null,
    hostname_hash: result === 'ready' ? entry.cached.hostnameHash : null,
    version: entry.version,
    lease_id: entry.leaseId,
    lease_until: timestamp(entry.leaseUntilSeconds),
    invalid_version: entry.invalidVersion,
    retry_after: timestamp(entry.retryAfterSeconds),
    last_error_code: null,
    updated_at: timestamp(databaseNowSeconds),
    observed_at: timestamp(databaseNowSeconds),
  });

  const handle = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input?.url;
    if (includeTicketState && url === TICKET_STATE_READ_URL) {
      const body = init.body ? JSON.parse(init.body) : {};
      const nowSeconds = now();
      return jsonResponse([{
        found: true,
        ticket_hash: body.p_ticket_hash,
        issued_at: nowSeconds,
        first_used_at: null,
        hard_expire_at: nowSeconds + 600,
        idle_timeout_seconds: ticketIdleTimeoutSeconds,
        idle_policy: 'first_use',
        idle_lease_expires_at: nowSeconds + 300,
      }]);
    }
    if (includeTicketState && url === TICKET_STATE_MARK_URL) {
      return jsonResponse({ result: 'transitioned', first_used_at: now() });
    }
    if (![CACHE_STATE_URL, CACHE_ACQUIRE_URL, CACHE_FINISH_URL, CACHE_CLEANUP_URL].includes(url)) {
      return null;
    }
    const body = init.body ? JSON.parse(init.body) : {};
    calls.push({ url, body });
    if (url === CACHE_CLEANUP_URL) {
      return jsonResponse(0);
    }

    const entry = getEntry(body.p_path_hash);
    const nowSeconds = now();
    if (url === CACHE_STATE_URL) {
      if (isFresh(entry)) {
        return jsonResponse([stateRow(entry, 'ready')]);
      }
      if (entry.leaseId && entry.leaseUntilSeconds > nowSeconds) {
        return jsonResponse([stateRow(entry, 'wait')]);
      }
      if (entry.retryAfterSeconds && entry.retryAfterSeconds > nowSeconds) {
        return jsonResponse([stateRow(entry, 'backoff')]);
      }
      return jsonResponse([stateRow(entry, 'missing')]);
    }

    if (url === CACHE_ACQUIRE_URL) {
      if (isFresh(entry) && body.p_observed_version !== entry.version) {
        return jsonResponse([stateRow(entry, 'ready')]);
      }
      if (entry.leaseId && entry.leaseUntilSeconds > nowSeconds) {
        return jsonResponse([stateRow(entry, 'wait')]);
      }
      if (entry.retryAfterSeconds && entry.retryAfterSeconds > nowSeconds) {
        return jsonResponse([stateRow(entry, 'backoff')]);
      }
      if (body.p_observed_version && body.p_observed_version === entry.version) {
        entry.invalidVersion = body.p_observed_version;
      }
      entry.leaseId = newLeaseId();
      entry.leaseUntilSeconds = nowSeconds + leaseDurationSeconds;
      entry.retryAfterSeconds = null;
      return jsonResponse([stateRow(entry, 'acquired')]);
    }

    let finishOverride;
    if (finishHandler) {
      finishOverride = await finishHandler({ body, entry, init });
    }
    if (finishOverride instanceof Response) {
      return finishOverride;
    }
    if (finishOverride && typeof finishOverride === 'object') {
      return jsonResponse(Array.isArray(finishOverride) ? finishOverride : [finishOverride]);
    }

    if (body.p_link_data) {
      entry.cached = {
        path: body.p_path,
        linkData: JSON.parse(body.p_link_data),
        timestamp: nowSeconds,
        hostnameHash: body.p_hostname_hash || null,
      };
      entry.version = body.p_lease_id;
      entry.leaseId = body.p_lease_id;
      entry.leaseUntilSeconds = null;
      entry.retryAfterSeconds = null;
      entry.invalidVersion = null;
      return jsonResponse([{
        result: 'committed',
        version: entry.version,
        invalid_version: null,
        retry_after: null,
        last_error_code: null,
        observed_at: timestamp(databaseNowSeconds),
      }]);
    }

    entry.leaseId = body.p_lease_id;
    entry.leaseUntilSeconds = null;
    entry.retryAfterSeconds = nowSeconds + 30;
    entry.invalidVersion = body.p_failed_version || entry.version;
    return jsonResponse([{
      result: 'committed',
      version: entry.version,
      invalid_version: entry.invalidVersion,
      retry_after: timestamp(entry.retryAfterSeconds),
      last_error_code: body.p_error_code ?? null,
      observed_at: timestamp(databaseNowSeconds),
    }]);
  };

  return { handle, reset, setFinishHandler, setLeaseDuration, seedLease, calls };
};

export const addDownloadEnvelope = async (response) => {
  if (!(response instanceof Response)) {
    return response;
  }
  const contentType = response.headers.get('content-type') || '';
  if (!contentType.toLowerCase().includes('json')) {
    return response;
  }
  let payload;
  try {
    payload = await response.clone().json();
  } catch {
    return response;
  }
  if (
    payload?.code !== 200
    || !payload.data
    || typeof payload.data !== 'object'
    || Array.isArray(payload.data)
    || typeof payload.data.url !== 'string'
    || payload.data.download
  ) {
    return response;
  }
  payload.data.download = {
    provider: 'test',
    ticket: 'test-ticket-abcdefghijklmnopqrstuvwxyz',
    expires_at: now() + 300,
    report_success: false,
  };
  return jsonResponse(payload, response.status);
};

export { CACHE_STATE_URL, CACHE_ACQUIRE_URL, CACHE_FINISH_URL, CACHE_CLEANUP_URL, TICKET_STATE_READ_URL, TICKET_STATE_MARK_URL };
