import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { authorizeBreakerAttempt, getBreakerState, reportBreakerSample, settleBreakerAttempt } from '../src/cache/throttle-custom-pg-rest.js';
import { scheduleAllCleanups } from '../src/cleanup-scheduler.js';
import { encryptBindingPayload } from '../src/origin-binding.js';
import worker from '../src/worker.js';
import { __fairQueueTestHooks } from '../src/worker.js';

const {
  resolveConfig,
  readOpenBreakerSnapshot,
  applyUnifiedResult,
  deriveOpenSeconds,
  createSlotHandlerClient,
} = __fairQueueTestHooks;

const createJsonResponse = (payload) => new Response(JSON.stringify(payload), {
  status: 200,
  headers: { 'content-type': 'application/json' },
});

const ACK_HANDOFF_URL = 'https://cq.example.test/api/v1/concurrency/ack_handoff';
const HEARTBEAT_URL = 'https://cq.example.test/api/v1/concurrency/heartbeat';
const DEFAULT_TRUE_CONCURRENCY_HEARTBEAT = {
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
};

const createFakeHeartbeatSocket = ({
  helloAck = {
    type: 'hello_ack',
    generation: 7,
    deadlineMs: Date.now() + 15_000,
    ackTimeoutMs: 2000,
    heartbeatIntervalMs: 5000,
    heartbeatTimeoutMs: 15000,
    reconnectGraceMs: 12000,
    startTimeoutMs: 7000,
    hardExpireAtMs: Date.now() + 60_000,
  },
  heartbeatAck = {
    type: 'heartbeat_ack',
    generation: 7,
    deadlineMs: Date.now() + 15_000,
    hardExpireAtMs: Date.now() + 60_000,
  },
} = {}) => {
  const listeners = new Map();

  const emit = (type, event = {}) => {
    const handlers = listeners.get(type);
    if (!handlers) {
      return;
    }
    for (const handler of [...handlers]) {
      handler(event);
    }
  };

  return {
    sent: [],
    addEventListener(type, handler) {
      const handlers = listeners.get(type) || new Set();
      handlers.add(handler);
      listeners.set(type, handlers);
    },
    removeEventListener(type, handler) {
      listeners.get(type)?.delete(handler);
    },
    accept() {},
    send(data) {
      this.sent.push(data);
      const payload = JSON.parse(data);
      if (payload.type === 'hello' && helloAck) {
        queueMicrotask(() => emit('message', { data: JSON.stringify(helloAck) }));
        return;
      }
      if (payload.type === 'heartbeat' && heartbeatAck) {
        queueMicrotask(() => emit('message', { data: JSON.stringify(heartbeatAck) }));
      }
    },
    close(code = 1000, reason = '') {
      queueMicrotask(() => emit('close', { code, reason }));
    },
  };
};

const createClaimGrantResponse = ({
  leaseId,
  leaseToken,
  expiresAtMs = Date.now() + 1_000,
  handoffToken = `handoff-${leaseId}`,
  handoffDeadlineMs = expiresAtMs - 1,
}) => createJsonResponse({
  result: 'granted',
  leaseId,
  leaseToken,
  expiresAtMs,
  handoffToken,
  handoffDeadlineMs,
});

const decodeHostnameHash = async (hostname) => {
  const data = new TextEncoder().encode(hostname);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  return Array.from(new Uint8Array(hashBuffer)).map((b) => b.toString(16).padStart(2, '0')).join('');
};

const encodeBase64Url = (input) => Buffer.from(input)
  .toString('base64')
  .replace(/\+/g, '-')
  .replace(/\//g, '_')
  .replace(/=+$/g, '');

const encodeSignatureBase64Url = (input) => Buffer.from(input)
  .toString('base64')
  .replace(/\+/g, '-')
  .replace(/\//g, '_');

const DEFAULT_IDLE_TIMEOUT_SECONDS = 300;
const TICKET_STATE_READ_URL = 'https://postgrest.example.test/rpc/download_get_ticket_state';
const TICKET_STATE_MARK_URL = 'https://postgrest.example.test/rpc/download_mark_ticket_used';
const TICKET_STATE_CLEANUP_URL = 'https://postgrest.example.test/rpc/download_cleanup_expired_tickets';

const decodeSignedRequestPayload = (request) => {
  const payload = new URL(request.url).searchParams.get('payload');
  assert.ok(payload, 'signed request is missing payload');
  return JSON.parse(Buffer.from(payload, 'base64url').toString('utf8'));
};

const assertSignedRequestPayloadContract = (request) => {
  const payloadJson = decodeSignedRequestPayload(request);
  assert.equal(typeof payloadJson.ticketNonce, 'string');
  assert.equal(Number.isInteger(payloadJson.idle_timeout), true);
  assert.match(payloadJson.ticketNonce, /^[A-Za-z0-9_-]{22,}$/);
};

const createTicketNonce = () => encodeBase64Url(crypto.getRandomValues(new Uint8Array(16)));

const ticketStateRpcState = {
  cleanupBodies: [],
  cleanupHandler: null,
  markBodies: [],
  markHandler: null,
  readBodies: [],
  readHandler: null,
};

const resetTicketStateRpcState = () => {
  ticketStateRpcState.cleanupBodies = [];
  ticketStateRpcState.cleanupHandler = null;
  ticketStateRpcState.markBodies = [];
  ticketStateRpcState.markHandler = null;
  ticketStateRpcState.readBodies = [];
  ticketStateRpcState.readHandler = null;
};

const createDefaultTicketStateRow = (body, overrides = {}) => {
  const nowSeconds = Math.floor(Date.now() / 1000);
  return {
    found: true,
    ticket_hash: body?.p_ticket_hash ?? null,
    issued_at: nowSeconds,
    first_used_at: null,
    hard_expire_at: nowSeconds + 600,
    ip_hash: null,
    path_hash: null,
    ...overrides,
  };
};

const handleTicketStateRpc = async (url, init = {}) => {
  if (url === TICKET_STATE_READ_URL) {
    const body = JSON.parse(init.body);
    ticketStateRpcState.readBodies.push(body);
    if (typeof ticketStateRpcState.readHandler === 'function') {
      const response = await ticketStateRpcState.readHandler(body, init);
      if (response instanceof Response) {
        return response;
      }
      return createJsonResponse(Array.isArray(response) ? response : [response]);
    }
    return createJsonResponse([createDefaultTicketStateRow(body)]);
  }

  if (url === TICKET_STATE_MARK_URL) {
    const body = JSON.parse(init.body);
    ticketStateRpcState.markBodies.push(body);
    if (typeof ticketStateRpcState.markHandler === 'function') {
      const response = await ticketStateRpcState.markHandler(body, init);
      if (response instanceof Response) {
        return response;
      }
      return createJsonResponse(response);
    }
    return createJsonResponse({
      result: 'transitioned',
      first_used_at: body?.p_now ?? Math.floor(Date.now() / 1000),
    });
  }

  if (url === TICKET_STATE_CLEANUP_URL) {
    const body = JSON.parse(init.body);
    ticketStateRpcState.cleanupBodies.push(body);
    if (typeof ticketStateRpcState.cleanupHandler === 'function') {
      const response = await ticketStateRpcState.cleanupHandler(body, init);
      if (response instanceof Response) {
        return response;
      }
      return createJsonResponse(response);
    }
    return createJsonResponse({ deleted: 0 });
  }

  return null;
};

const signPayload = async (payload, expire, token) => {
  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(token),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  );
  const signature = await crypto.subtle.sign(
    'HMAC',
    key,
    new TextEncoder().encode(`${payload}:${expire}`),
  );
  return `${encodeSignatureBase64Url(Buffer.from(signature))}:${expire}`;
};

const buildRuntimeBootstrap = (options = {}) => ({
  configVersion: 'task5-runtime',
  ttlSeconds: 300,
  global: {
    defaultProfileId: 'default',
  },
  pathProfiles: [{
    id: 'default',
    dynamic: false,
    actions: {
      checkOriginMode: '',
    },
  }],
  common: {
    tokenHmacKey: 'bootstrap-token',
    workerAddresses: ['https://worker.example.com'],
    landingWorkerAddresses: ['https://landing.example.com'],
    binding: {
      defaultModes: '',
      version: 1,
    },
  },
  download: {
    address: 'https://alist.example.com',
    db: {
      mode: 'custom-pg-rest',
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
      cacheEnabled: options.cacheEnabled === true,
      ...(options.rateLimit ? { rateLimit: options.rateLimit } : {}),
    },
    throttleProfiles: {
      default: {
        hostPatterns: options.hostPatterns || ['*.sharepoint.com'],
        openCapSeconds: 60,
        openThresholdPercent: 30,
        closeThresholdPercent: 15,
        ewmaSpan: 8,
        consecutiveThreshold: 4,
        minSamplesBeforeEwmaOpen: 8,
        idleResetSeconds: 900,
        halfOpenSuccessThreshold: 2,
        halfOpenCloseMode: 'and',
        halfOpenMaxProbeCount: 4,
        halfOpenMaxSeconds: 15,
        halfOpenTimeoutMode: 'partial-close',
        protectHttpCodes: [429, 499, 500, 502, 503, 504],
      },
    },
    ...(options.fairQueueHostPatterns ? {
      fairQueue: {
        enabled: true,
        hostPatterns: options.fairQueueHostPatterns,
        ...(options.fairQueueSiteBucket ? { siteBucket: options.fairQueueSiteBucket } : {}),
        slotHandlerUrl: 'https://slot-handler.example.test',
        slotHandlerAuthKey: 'slot-secret',
        slotHandlerAuthHeader: 'X-FQ-Auth',
      },
    } : {}),
    ...(options.trueConcurrencyHostPatterns ? {
      trueConcurrency: {
        enabled: true,
        hostPatterns: options.trueConcurrencyHostPatterns,
        handlerUrl: 'https://cq.example.test',
        handlerAuthKey: 'cq-secret',
        heartbeat: {
          ...DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
          ...(options.trueConcurrencyHeartbeatOverrides || {}),
        },
      },
    } : {}),
  },
});

const buildSignedWorkerRequest = async (pathname = '/downloads/test.bin', options = {}) => {
  const {
    idleTimeoutSeconds = DEFAULT_IDLE_TIMEOUT_SECONDS,
    omitTicketNonce = false,
    payloadFileSize = undefined,
    payloadMutator = null,
    ticketNonce = createTicketNonce(),
  } = options;
  const token = 'bootstrap-token';
  const expire = Math.floor(Date.now() / 1000) + 300;
  const encryptedBinding = await encryptBindingPayload({
    v: 2,
    issuer: 'https://landing.example.com',
    workerAddress: 'https://worker.example.com',
  }, token);
  const payloadObject = {
    v: 1,
    expireTime: expire,
    idle_timeout: idleTimeoutSeconds,
    ...(omitTicketNonce ? {} : { ticketNonce }),
    encrypt: encryptedBinding,
    ...(payloadFileSize !== undefined ? { filesize: payloadFileSize } : {}),
  };
  const finalPayloadObject = typeof payloadMutator === 'function'
    ? payloadMutator({ ...payloadObject })
    : payloadObject;
  const payload = encodeBase64Url(JSON.stringify(finalPayloadObject));
  const payloadSign = await signPayload(payload, expire, token);
  const url = new URL(pathname, 'https://worker.example.com');
  url.searchParams.set('payload', payload);
  url.searchParams.set('payloadSign', payloadSign);
  return new Request(url, {
    headers: {
      origin: 'https://landing.example.com',
      'CF-Connecting-IP': '192.0.2.10',
    },
  });
};

const createDeferred = () => {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const waitForCondition = async (
  predicate,
  {
    timeoutMs = 250,
    intervalMs = 5,
    message = 'timed out waiting for condition',
  } = {},
) => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return;
    }
    await sleep(intervalMs);
  }
  if (predicate()) {
    return;
  }
  throw new Error(message);
};

const buildBootstrap = () => ({
  common: {
    tokenHmacKey: 'bootstrap-token',
    workerAddresses: ['https://worker.example.com'],
    landingWorkerAddresses: ['https://landing.example.com'],
  },
  download: {
    address: 'https://alist.example.com',
    db: {
      mode: 'custom-pg-rest',
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
      cacheEnabled: false,
    },
    throttleProfiles: {
      default: {
        hostPatterns: ['*.sharepoint.com'],
        openCapSeconds: 60,
        openThresholdPercent: 30,
        closeThresholdPercent: 15,
        ewmaSpan: 8,
        consecutiveThreshold: 4,
        minSamplesBeforeEwmaOpen: 8,
        idleResetSeconds: 900,
        halfOpenSuccessThreshold: 2,
        halfOpenCloseMode: 'and',
        halfOpenMaxProbeCount: 4,
        halfOpenMaxSeconds: 15,
        halfOpenTimeoutMode: 'partial-close',
        protectHttpCodes: [429, 499, 500, 502, 503, 504],
      },
    },
  },
});

const wrappedFetch = globalThis.fetch;
const wrappedFetchBound = typeof wrappedFetch === 'function' ? wrappedFetch.bind(globalThis) : wrappedFetch;
let delegatedFetch = wrappedFetchBound;

const fetchWithDefaultTicketStateRpc = async (input, init = {}) => {
  const url = typeof input === 'string' ? input : input.url;
  const ticketStateResponse = await handleTicketStateRpc(url, init);
  if (ticketStateResponse) {
    return ticketStateResponse;
  }
  if (url === HEARTBEAT_URL) {
    try {
      return await delegatedFetch(input, init);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (!message.includes(`Unexpected fetch URL in test: ${HEARTBEAT_URL}`)) {
        throw error;
      }
    }
    return {
      status: 101,
      webSocket: createFakeHeartbeatSocket(),
    };
  }
  if (typeof delegatedFetch !== 'function') {
    throw new Error('global fetch handler not configured');
  }
  return delegatedFetch(input, init);
};

Object.defineProperty(globalThis, 'fetch', {
  configurable: true,
  enumerable: true,
  get() {
    return fetchWithDefaultTicketStateRpc;
  },
  set(value) {
    resetTicketStateRpcState();
    if (value === fetchWithDefaultTicketStateRpc) {
      delegatedFetch = wrappedFetchBound;
      return;
    }
    delegatedFetch = value;
  },
});

test('signed worker request fixtures include ticketNonce and idle_timeout', async () => {
  assertSignedRequestPayloadContract(await buildSignedWorkerRequest());
});

test('resolveConfig returns the canonical breaker runtime config', () => {
  const config = resolveConfig({}, buildBootstrap(), { download: {} });

  assert.equal(config.throttleEnabled, true);
  assert.equal(config.ticketStateTableName, 'DOWNLOAD_TICKET_STATE_TABLE');
  assert.equal(config.cacheConfig.ticketStateTableName, 'DOWNLOAD_TICKET_STATE_TABLE');
  assert.deepEqual(config.throttleHostnamePatterns, ['*.sharepoint.com']);
  assert.deepEqual(config.throttleConfig, {
    postgrestUrl: 'https://postgrest.example.test',
    verifyHeader: ['X-Verify'],
    verifySecret: ['secret'],
    openCapSeconds: 60,
    openThresholdPercent: 30,
    closeThresholdPercent: 15,
    ewmaSpan: 8,
    consecutiveThreshold: 4,
    minSamplesBeforeEwmaOpen: 8,
    idleResetSeconds: 900,
    halfOpenSuccessThreshold: 2,
    halfOpenCloseMode: 'and',
    halfOpenMaxProbeCount: 4,
    halfOpenMaxSeconds: 15,
    halfOpenTimeoutMode: 'partial-close',
    protectHttpCodes: [429, 499, 500, 502, 503, 504],
  });
  assert.equal('probeLeaseSeconds' in config.throttleConfig, false);
});

test('worker fails closed when breaker snapshot lookup fails', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  let upstreamFetches = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/file',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      throw new Error('snapshot unavailable');
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
        ATTEMPT_GRANTED: false,
        ATTEMPT_TICKET: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 2,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      upstreamFetches += 1;
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/snapshot-failure.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.equal(upstreamFetches, 0);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('worker authorizes the actual fetch attempt after a closed pre-check snapshot', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  let upstreamFetches = 0;
  let authorizeCalls = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/file',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeCalls += 1;
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 2,
        LAST_ERROR_CODE: null,
        ATTEMPT_GRANTED: false,
        ATTEMPT_TICKET: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 2,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      upstreamFetches += 1;
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/claim-failure.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.equal(upstreamFetches, 1);
    assert.equal(authorizeCalls, 1);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('worker fails closed when breaker sample reporting fails after half_open authority snapshot', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  let upstreamFetches = 0;
  let reportCalls = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/file',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 1,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 1,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 2,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportCalls += 1;
      throw new Error('report unavailable');
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      upstreamFetches += 1;
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/report-failure.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.equal(upstreamFetches, 1);
    assert.equal(reportCalls, 1);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('slot-handler acquire payload omits breaker admission fields in queue_only mode', async () => {
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.test',
      totalMaxWaitMs: 20000,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 1,
    },
  });

  const fqContext = {
    hostname: 'tenant.sharepoint.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  const originalFetch = globalThis.fetch;
  let seenPayload = null;

  globalThis.fetch = async (_url, init) => {
    seenPayload = JSON.parse(init.body);
    return new Response(JSON.stringify({
      result: 'granted',
      queryToken: 'query-queue-only-grant',
      invocationEpoch: 1,
      slotToken: 'slot-1',
    }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.equal(result.kind, 'granted');
    assert.equal(typeof seenPayload.now, 'number');
    assert.deepEqual({ ...seenPayload, now: 0 }, {
      hostname: 'tenant.sharepoint.com',
      hostnameHash: 'host-hash',
      ipBucket: 'ip-bucket',
      siteBucket: 'site-bucket',
      now: 0,
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler acquire payload carries breaker admission fields for queue_breaker mode', async () => {
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.test',
      totalMaxWaitMs: 20000,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 1,
    },
  });

  const fqContext = {
    hostname: 'tenant.sharepoint.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    breakerEnabled: true,
    openCapSeconds: 75,
    closeThresholdPercent: 19,
    halfOpenSuccessThreshold: 3,
    halfOpenCloseMode: 'or',
    halfOpenMaxProbeCount: 4,
    halfOpenMaxSeconds: 15,
    halfOpenTimeoutMode: 'partial-close',
  };

  const originalFetch = globalThis.fetch;
  let seenPayload = null;

  globalThis.fetch = async (_url, init) => {
    seenPayload = JSON.parse(init.body);
    return new Response(JSON.stringify({
      result: 'granted',
      queryToken: 'query-queue-breaker-grant',
      invocationEpoch: 1,
      slotToken: 'slot-1',
    }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.equal(result.kind, 'granted');
    assert.equal(seenPayload.breakerEnabled, true);
    assert.equal(seenPayload.openCapSeconds, 75);
    assert.equal(seenPayload.closeThresholdPercent, 19);
    assert.equal(seenPayload.halfOpenSuccessThreshold, 3);
    assert.equal(seenPayload.halfOpenCloseMode, 'or');
    assert.equal(seenPayload.halfOpenMaxProbeCount, 4);
    assert.equal(seenPayload.halfOpenMaxSeconds, 15);
    assert.equal(seenPayload.halfOpenTimeoutMode, 'partial-close');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('queue_breaker transport preserves closeThresholdPercent=0 from the resolved throttle config', async () => {
  const runtimeBootstrap = buildRuntimeBootstrap({
    hostPatterns: ['*.sharepoint.com'],
    fairQueueHostPatterns: ['*.sharepoint.com'],
  });
  runtimeBootstrap.download.throttleProfiles.default.closeThresholdPercent = 0;

  const { throttleConfig } = resolveConfig({}, runtimeBootstrap, { download: {} });
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.test',
      totalMaxWaitMs: 20000,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 1,
    },
  });
  const fqContext = {
    hostname: 'tenant.sharepoint.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    breakerEnabled: true,
    openCapSeconds: throttleConfig.openCapSeconds,
    closeThresholdPercent: throttleConfig.closeThresholdPercent,
    halfOpenSuccessThreshold: throttleConfig.halfOpenSuccessThreshold,
    halfOpenCloseMode: throttleConfig.halfOpenCloseMode,
    halfOpenMaxProbeCount: throttleConfig.halfOpenMaxProbeCount,
    halfOpenMaxSeconds: throttleConfig.halfOpenMaxSeconds,
    halfOpenTimeoutMode: throttleConfig.halfOpenTimeoutMode,
  };

  const originalFetch = globalThis.fetch;
  let seenPayload = null;

  globalThis.fetch = async (_url, init) => {
    seenPayload = JSON.parse(init.body);
    return new Response(JSON.stringify({
      result: 'granted',
      queryToken: 'query-queue-breaker-grant-zero-threshold',
      invocationEpoch: 1,
      slotToken: 'slot-1',
    }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.equal(result.kind, 'granted');
    assert.equal(throttleConfig.closeThresholdPercent, 0);
    assert.equal(seenPayload.closeThresholdPercent, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler throttled responses preserve raw breaker snapshot metadata', async () => {
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.test',
      totalMaxWaitMs: 20000,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 1,
    },
    throttleConfig: {
      openCapSeconds: 60,
    },
  });

  const fqContext = {
    hostname: 'tenant.sharepoint.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };
  const nowSeconds = Math.floor(Date.now() / 1000);
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async () => new Response(JSON.stringify({
    result: 'throttled',
    queryToken: 'query-throttled-snapshot',
    invocationEpoch: 1,
    throttleCode: 429,
    breakerOpenUntil: nowSeconds + 22,
    breakerReason: 'http_429',
    breakerVersion: 12,
  }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.equal(result.kind, 'throttled');
    assert.deepEqual(result.breakerSnapshot, {
      state: 'open',
      openUntil: nowSeconds + 22,
      reason: 'http_429',
      version: 12,
      lastErrorCode: 429,
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('queue_breaker timeout absorption matches breaker_only authoritative post-timeout state', async () => {
  const hostname = 'tenant.sharepoint.com';
  const hostnameHash = await decodeHostnameHash(hostname);
  const runtimeBootstrap = buildRuntimeBootstrap({
    hostPatterns: ['*.sharepoint.com'],
    fairQueueHostPatterns: ['*.sharepoint.com'],
  });
  Object.assign(runtimeBootstrap.download.throttleProfiles.default, {
    openCapSeconds: 75,
    closeThresholdPercent: 19,
    halfOpenSuccessThreshold: 3,
    halfOpenCloseMode: 'or',
    halfOpenMaxProbeCount: 4,
    halfOpenMaxSeconds: 15,
    halfOpenTimeoutMode: 'open',
  });

  const { throttleConfig } = resolveConfig({}, runtimeBootstrap, { download: {} });
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.test',
      totalMaxWaitMs: 20000,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 1,
    },
  });
  const fqContext = {
    hostname,
    hostnameHash,
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    breakerEnabled: true,
    openCapSeconds: throttleConfig.openCapSeconds,
    closeThresholdPercent: throttleConfig.closeThresholdPercent,
    halfOpenSuccessThreshold: throttleConfig.halfOpenSuccessThreshold,
    halfOpenCloseMode: throttleConfig.halfOpenCloseMode,
    halfOpenMaxProbeCount: throttleConfig.halfOpenMaxProbeCount,
    halfOpenMaxSeconds: throttleConfig.halfOpenMaxSeconds,
    halfOpenTimeoutMode: throttleConfig.halfOpenTimeoutMode,
  };

  const authoritativeOpenUntil = Math.floor(Date.now() / 1000) + 22;
  const authoritativeSnapshot = {
    state: 'open',
    openUntil: authoritativeOpenUntil,
    reason: 'http_429',
    version: 45,
    lastErrorCode: 429,
  };
  const originalFetch = globalThis.fetch;
  const acquireBodies = [];
  const authorizeBodies = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 44,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'throttled',
        queryToken: 'query-timeout-absorbed',
        invocationEpoch: 1,
        throttleCode: authoritativeSnapshot.lastErrorCode,
        breakerOpenUntil: authoritativeSnapshot.openUntil,
        breakerReason: authoritativeSnapshot.reason,
        breakerVersion: authoritativeSnapshot.version,
      });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: authoritativeSnapshot.state,
        OPEN_UNTIL: authoritativeSnapshot.openUntil,
        OPEN_REASON: authoritativeSnapshot.reason,
        VERSION: authoritativeSnapshot.version,
        LAST_ERROR_CODE: authoritativeSnapshot.lastErrorCode,
        HALF_OPEN_DEADLINE: null,
        ATTEMPT_GRANTED: false,
        ATTEMPT_TICKET: null,
      }]);
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const snapshotBeforeAuthorize = await getBreakerState(hostname, throttleConfig);
    const fqResult = await client.waitForSlot({}, fqContext);
    const breakerOnlyResult = await authorizeBreakerAttempt(hostname, throttleConfig);

    assert.equal(snapshotBeforeAuthorize.state, 'half_open');
    assert.equal(fqResult.kind, 'throttled');
    assert.ok(Number.isFinite(fqResult.retryAfter) && fqResult.retryAfter > 0);
    assert.deepEqual(fqResult.breakerSnapshot, authoritativeSnapshot);
    assert.deepEqual(
      {
        state: breakerOnlyResult.state,
        openUntil: breakerOnlyResult.openUntil,
        reason: breakerOnlyResult.reason,
        version: breakerOnlyResult.version,
        lastErrorCode: breakerOnlyResult.lastErrorCode,
      },
      authoritativeSnapshot,
    );
    assert.equal(breakerOnlyResult.halfOpenDeadline, null);
    assert.equal(breakerOnlyResult.attemptGranted, false);
    assert.equal(breakerOnlyResult.attemptTicket, null);

    assert.equal(acquireBodies.length, 1);
    assert.equal(authorizeBodies.length, 1);
    assert.equal(acquireBodies[0].hostname, hostname);
    assert.equal(acquireBodies[0].hostnameHash, hostnameHash);
    assert.equal(authorizeBodies[0].p_hostname, hostname);
    assert.equal(authorizeBodies[0].p_hostname_hash, hostnameHash);
    assert.equal(acquireBodies[0].openCapSeconds, authorizeBodies[0].p_open_cap_seconds);
    assert.equal(acquireBodies[0].closeThresholdPercent, authorizeBodies[0].p_close_threshold_percent);
    assert.equal(acquireBodies[0].halfOpenSuccessThreshold, authorizeBodies[0].p_half_open_success_threshold);
    assert.equal(acquireBodies[0].halfOpenCloseMode, authorizeBodies[0].p_half_open_close_mode);
    assert.equal(acquireBodies[0].halfOpenMaxProbeCount, authorizeBodies[0].p_half_open_max_probe_count);
    assert.equal(acquireBodies[0].halfOpenMaxSeconds, authorizeBodies[0].p_half_open_max_seconds);
    assert.equal(acquireBodies[0].halfOpenTimeoutMode, authorizeBodies[0].p_half_open_timeout_mode);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler client rechecks shared breaker state on repeated throttled acquires', async () => {
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.test',
      totalMaxWaitMs: 20000,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 1,
    },
    throttleConfig: {
      openCapSeconds: 60,
    },
  });

  const host = `tenant-${Date.now()}-${Math.random().toString(16).slice(2)}.sharepoint.com`;
  const fqContext = {
    hostname: host,
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };
  const nowSeconds = Math.floor(Date.now() / 1000);
  const originalFetch = globalThis.fetch;
  let fetchCalls = 0;

  globalThis.fetch = async () => {
    fetchCalls += 1;
      return new Response(JSON.stringify({
        result: 'throttled',
        queryToken: `query-throttled-${fetchCalls}`,
        invocationEpoch: fetchCalls,
        throttleCode: 429,
        breakerOpenUntil: nowSeconds + 30,
        breakerReason: 'http_429',
      breakerVersion: fetchCalls,
    }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const first = await client.waitForSlot({}, { ...fqContext });
    const second = await client.waitForSlot({}, { ...fqContext });

    assert.equal(first.kind, 'throttled');
    assert.equal(second.kind, 'throttled');
    assert.equal(fetchCalls, 2);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('worker does not fail fast on half_open breaker snapshots', async () => {
  const unifiedResult = {
    throttle: {
      state: 'half_open',
      openUntil: Math.floor(Date.now() / 1000) + 30,
      reason: 'http_429',
      version: 9,
      lastErrorCode: 429,
    },
  };

  const response = await applyUnifiedResult(unifiedResult);
  assert.equal(response, null);
});

test('worker does not retain or expose local breaker mirror state', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  delete globalThis.bootstrapCache;
  __fairQueueTestHooks.clearOverloadedByHost?.();

  globalThis.fetch = async (input) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/file',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      return createJsonResponse([{
        STATE: 'open',
        OPEN_UNTIL: Math.floor(Date.now() / 1000) + 30,
        OPEN_REASON: 'http_429',
        VERSION: 1,
        LAST_ERROR_CODE: 429,
      }]);
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/mirror-authority.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 429);
    assert.equal(__fairQueueTestHooks.getBreakerMirrorSize?.() ?? 0, 0);
    assert.equal('mirrorBreakerSnapshot' in __fairQueueTestHooks, false);
    assert.equal('pruneBreakerMirrorCache' in __fairQueueTestHooks, false);
    assert.equal('getBreakerMirrorSize' in __fairQueueTestHooks, false);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
    __fairQueueTestHooks.clearOverloadedByHost?.();
  }
});


test('worker blocks immediately when unified breaker snapshot is open', async () => {
  const unifiedResult = {
    throttle: {
      state: 'open',
      openUntil: Math.floor(Date.now() / 1000) + 30,
      reason: 'http_429',
      version: 9,
      lastErrorCode: 429,
    },
  };

  const response = await applyUnifiedResult(unifiedResult);
  assert.equal(response.status, 429);
});

test('deriveOpenSeconds caps numeric Retry-After and adds one second', () => {
  assert.equal(deriveOpenSeconds('999', 60), 60);
  assert.equal(deriveOpenSeconds('7', 60), 8);
  assert.equal(deriveOpenSeconds('Wed, 21 Oct 2030 07:28:00 GMT', 60), null);
});

test('worker interprets open breaker snapshots from raw fields', () => {
  const nowSeconds = 1_000;
  assert.deepEqual(
    readOpenBreakerSnapshot({
      recordExists: true,
      state: 'open',
      openUntil: nowSeconds + 30,
      reason: 'http_429',
      version: 9,
      lastErrorCode: 429,
    }, 60, nowSeconds),
    {
      state: 'open',
      openUntil: nowSeconds + 30,
      reason: 'http_429',
      version: 9,
      errorCode: 429,
      retryAfter: 30,
    }
  );
});

test('getBreakerState returns raw breaker snapshot fields', async () => {
  const originalFetch = globalThis.fetch;
  const nowSeconds = Math.floor(Date.now() / 1000);

  globalThis.fetch = async () => createJsonResponse([{
    STATE: 'open',
    OPEN_UNTIL: nowSeconds + 22,
    OPEN_REASON: 'http_429',
    VERSION: 7,
    LAST_ERROR_CODE: 429,
  }]);

  try {
    const result = await getBreakerState('tenant.sharepoint.com', {
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
    });

    assert.deepEqual(result, {
      recordExists: true,
      state: 'open',
      openUntil: nowSeconds + 22,
      reason: 'http_429',
      version: 7,
      lastErrorCode: 429,
    });
    assert.equal(Object.hasOwn(result, 'status'), false);
    assert.equal(Object.hasOwn(result, 'retryAfter'), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('getBreakerState preserves the half_open breaker state', async () => {
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async () => createJsonResponse([{
    STATE: 'half_open',
    OPEN_UNTIL: null,
    OPEN_REASON: 'http_429',
    VERSION: 12,
    LAST_ERROR_CODE: 429,
  }]);

  try {
    const result = await getBreakerState('tenant.sharepoint.com', {
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
    });

    assert.deepEqual(result, {
      recordExists: true,
      state: 'half_open',
      openUntil: null,
      reason: 'http_429',
      version: 12,
      lastErrorCode: 429,
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('getBreakerState does not expose legacy probe lease timing fields', async () => {
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async () => createJsonResponse([{
    STATE: 'half_open',
    OPEN_UNTIL: null,
    OPEN_REASON: 'http_429',
    VERSION: 12,
    LAST_ERROR_CODE: 429,
    HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
  }]);

  try {
    const result = await getBreakerState('tenant.sharepoint.com', {
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
    });

    assert.equal('probeLeaseUntil' in result, false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('getBreakerState preserves the closed breaker state', async () => {
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async () => createJsonResponse([{
    STATE: 'closed',
    OPEN_UNTIL: null,
    OPEN_REASON: null,
    VERSION: 11,
    LAST_ERROR_CODE: null,
  }]);

  try {
    const result = await getBreakerState('tenant.sharepoint.com', {
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
    });

    assert.deepEqual(result, {
      recordExists: true,
      state: 'closed',
      openUntil: null,
      reason: null,
      version: 11,
      lastErrorCode: null,
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('getBreakerState ignores breaker states outside closed open and half_open', async () => {
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async () => createJsonResponse([{
    STATE: 'unexpected_state',
    OPEN_UNTIL: 188,
    OPEN_REASON: 'http_429',
    VERSION: 12,
    LAST_ERROR_CODE: 429,
  }]);

  try {
    const result = await getBreakerState('tenant.sharepoint.com', {
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
    });

    assert.equal(result.state, null);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('reportBreakerSample sends the canonical breaker report payload', async () => {
  const originalFetch = globalThis.fetch;
  let rpcUrl = null;
  let rpcBody = null;

  globalThis.fetch = async (url, init) => {
    rpcUrl = url;
    rpcBody = JSON.parse(init.body);
    return createJsonResponse([{
      STATE: 'open',
      OPEN_UNTIL: 123,
      OPEN_REASON: 'http_503',
      VERSION: 5,
      LAST_ERROR_CODE: 503,
      CONSECUTIVE_ERROR_COUNT: 0,
    }]);
  };

  try {
    const result = await reportBreakerSample('tenant.sharepoint.com', {
      sample: 1,
      statusCode: 503,
      attemptVersion: 11,
      attemptTicket: 2,
      retryAfterSeconds: 9,
    }, {
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
      openCapSeconds: 75,
      openThresholdPercent: 35,
      closeThresholdPercent: 15,
      ewmaSpan: 11,
      consecutiveThreshold: 6,
      minSamplesBeforeEwmaOpen: 8,
      idleResetSeconds: 900,
      halfOpenSuccessThreshold: 2,
      halfOpenCloseMode: 'and',
      halfOpenMaxProbeCount: 4,
      halfOpenMaxSeconds: 15,
      halfOpenTimeoutMode: 'partial-close',
      protectHttpCodes: [429, 503],
    });

    assert.equal(rpcUrl, 'https://postgrest.example.test/rpc/download_report_breaker_sample');
    assert.equal(rpcBody.p_sample, 1);
    assert.equal(rpcBody.p_status_code, 503);
    assert.equal(rpcBody.p_open_cap_seconds, 75);
    assert.equal(rpcBody.p_open_threshold_percent, 35);
    assert.equal(rpcBody.p_close_threshold_percent, 15);
    assert.equal(rpcBody.p_ewma_span, 11);
    assert.equal(rpcBody.p_consecutive_threshold, 6);
    assert.equal(rpcBody.p_min_samples_before_ewma_open, 8);
    assert.equal(rpcBody.p_idle_reset_seconds, 900);
    assert.equal(rpcBody.p_half_open_success_threshold, 2);
    assert.equal(rpcBody.p_half_open_close_mode, 'and');
    assert.equal(rpcBody.p_half_open_max_seconds, 15);
    assert.equal(rpcBody.p_half_open_timeout_mode, 'partial-close');
    assert.equal(rpcBody.p_attempt_version, 11);
    assert.equal(rpcBody.p_attempt_ticket, 2);
    assert.equal(rpcBody.p_retry_after_seconds, 9);
    assert.deepEqual(Object.keys(rpcBody).sort(), [
      'p_attempt_ticket',
      'p_attempt_version',
      'p_close_threshold_percent',
      'p_consecutive_threshold',
      'p_ewma_span',
      'p_half_open_close_mode',
      'p_half_open_max_seconds',
      'p_half_open_success_threshold',
      'p_half_open_timeout_mode',
      'p_hostname',
      'p_hostname_hash',
      'p_idle_reset_seconds',
      'p_min_samples_before_ewma_open',
      'p_now',
      'p_open_cap_seconds',
      'p_open_threshold_percent',
      'p_retry_after_seconds',
      'p_sample',
      'p_status_code',
    ]);
    assert.deepEqual(result, {
      recordExists: true,
      state: 'open',
      openUntil: 123,
      reason: 'http_503',
      version: 5,
      lastErrorCode: 503,
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('settleBreakerAttempt sends the canonical breaker settlement payload', async () => {
  const originalFetch = globalThis.fetch;
  let rpcUrl = null;
  let rpcBody = null;

  globalThis.fetch = async (url, init) => {
    rpcUrl = url;
    rpcBody = JSON.parse(init.body);
    return createJsonResponse([{
      STATE: 'half_open',
      OPEN_UNTIL: null,
      OPEN_REASON: 'http_429',
      VERSION: 18,
      LAST_ERROR_CODE: 429,
      HALF_OPEN_DEADLINE: 999,
    }]);
  };

  try {
    const result = await settleBreakerAttempt('tenant.sharepoint.com', {
      attemptVersion: 18,
      attemptTicket: 5,
    }, {
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
    });

    assert.equal(rpcUrl, 'https://postgrest.example.test/rpc/download_settle_breaker_attempt');
    assert.equal(rpcBody.p_attempt_version, 18);
    assert.equal(rpcBody.p_attempt_ticket, 5);
    assert.equal(rpcBody.p_hostname, 'tenant.sharepoint.com');
    assert.equal(typeof rpcBody.p_now, 'number');
    assert.equal(result.state, 'half_open');
    assert.equal(result.version, 18);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('worker authorizes and reports success samples for each managed redirect hop when authority rows are absent', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const snapshotBodies = [];
  const authorizeBodies = [];
  const reportBodies = [];
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/start',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      snapshotBodies.push(url);
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
        ATTEMPT_GRANTED: false,
        ATTEMPT_TICKET: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://tenant.sharepoint.com/final' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/final') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest(), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.equal(snapshotBodies.length, 1);
    assert.equal(authorizeBodies.length, 2);
    assert.deepEqual(
      authorizeBodies.map((body) => body.p_hostname),
      ['tenant.sharepoint.com', 'tenant.sharepoint.com'],
    );
    assert.deepEqual(
      reportBodies.map((body) => ({ attemptVersion: body.p_attempt_version, attemptTicket: body.p_attempt_ticket })),
      [
        { attemptVersion: null, attemptTicket: null },
        { attemptVersion: null, attemptTicket: null },
      ],
    );
    assert.deepEqual(
      reportBodies.map((body) => ({ sample: body.p_sample, statusCode: body.p_status_code })),
      [
        { sample: 0, statusCode: 302 },
        { sample: 0, statusCode: 200 },
      ],
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('worker forwards a granted attempt version and ticket into reportBreakerSample on matching hop', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const reportBodies = [];
  let snapshotReads = 0;
  let authorizeCalls = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/probe-target',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      snapshotReads += 1;
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 12,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeCalls += 1;
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 13,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 2,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 14,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/probe-target') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/probe-version.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.equal(snapshotReads, 1);
    assert.equal(authorizeCalls, 1);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].p_hostname, 'tenant.sharepoint.com');
    assert.equal(reportBodies[0].p_attempt_version, 13);
    assert.equal(reportBodies[0].p_attempt_ticket, 2);
    assert.equal(reportBodies[0].p_sample, 0);
    assert.equal(reportBodies[0].p_status_code, 200);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('worker authorizes each managed redirect host without reviving worker-side probe state', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const tenantHash = await decodeHostnameHash('tenant.sharepoint.com');
  const snapshotHashes = [];
  const authorizeBodies = [];
  const reportBodies = [];
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: ['*.sharepoint.com', '*.office.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/start',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      const match = url.match(/HOSTNAME_HASH=eq\.([0-9a-f]+)/i);
      const requestedHash = match ? match[1] : '';
      snapshotHashes.push(requestedHash);
      if (requestedHash === tenantHash) {
        return createJsonResponse([{
          STATE: 'closed',
          OPEN_UNTIL: null,
          OPEN_REASON: null,
          VERSION: 1,
          LAST_ERROR_CODE: null,
        }]);
      }
      throw new Error(`Unexpected hostname hash in test: ${requestedHash}`);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 41,
        LAST_ERROR_CODE: null,
        ATTEMPT_GRANTED: false,
        ATTEMPT_TICKET: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 42,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://files.office.com/final' },
      });
    }

    if (url === 'https://files.office.com/final') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/cross-host-redirect.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.deepEqual(snapshotHashes, [tenantHash]);
    assert.equal(authorizeBodies.length, 2);
    assert.equal(reportBodies.length, 2);
    assert.deepEqual(
      authorizeBodies.map((body) => body.p_hostname),
      [
        'tenant.sharepoint.com',
        'files.office.com',
      ],
    );
    assert.deepEqual(
      reportBodies.map((body) => ({ hostname: body.p_hostname, attemptVersion: body.p_attempt_version, attemptTicket: body.p_attempt_ticket })),
      [
        { hostname: 'tenant.sharepoint.com', attemptVersion: null, attemptTicket: null },
        { hostname: 'files.office.com', attemptVersion: null, attemptTicket: null },
      ],
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('worker reauthorizes same-host refresh attempts when authority rows are absent', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const snapshotBodies = [];
  const authorizeBodies = [];
  const reportBodies = [];
  let linkFetchCount = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      linkFetchCount += 1;
      return createJsonResponse({
        code: 200,
        data: {
          url: linkFetchCount === 1
            ? 'https://tenant.sharepoint.com/start'
            : 'https://tenant.sharepoint.com/fresh',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      snapshotBodies.push(url);
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
        ATTEMPT_GRANTED: false,
        ATTEMPT_TICKET: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/start') {
      return new Response('expired', {
        status: 401,
        headers: { 'content-type': 'text/plain' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/fresh') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest(), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.equal(snapshotBodies.length, 1);
    assert.equal(authorizeBodies.length, 2);
    assert.deepEqual(
      authorizeBodies.map((body) => body.p_hostname),
      ['tenant.sharepoint.com', 'tenant.sharepoint.com'],
    );
    assert.deepEqual(
      reportBodies.map((body) => ({ attemptVersion: body.p_attempt_version, attemptTicket: body.p_attempt_ticket })),
      [{ attemptVersion: null, attemptTicket: null }],
    );
    assert.deepEqual(
      reportBodies.map((body) => ({ sample: body.p_sample, statusCode: body.p_status_code })),
      [
        { sample: 0, statusCode: 200 },
      ],
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('worker authorizes and reports refreshed external redirects with attempt tickets', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const snapshotBodies = [];
  const authorizeBodies = [];
  const reportBodies = [];
  let linkFetchCount = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: ['*.sharepoint.com', '*.office.com'],
      }));
    }

    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      linkFetchCount += 1;
      return createJsonResponse({
        code: 200,
        data: {
          url: linkFetchCount === 1
            ? 'https://tenant.sharepoint.com/stale'
            : 'https://tenant.sharepoint.com/refresh-start',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      snapshotBodies.push(url);
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_claim_breaker_probe') {
      throw new Error('legacy claim path called');
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeBodies.push(JSON.parse(init.body));
      if (authorizeBodies.length === 1) {
        return createJsonResponse([{
          STATE: 'closed',
          OPEN_UNTIL: null,
          OPEN_REASON: null,
          VERSION: 1,
          LAST_ERROR_CODE: null,
          HALF_OPEN_DEADLINE: null,
          ATTEMPT_GRANTED: false,
          ATTEMPT_TICKET: null,
        }]);
      }
      if (authorizeBodies.length === 2) {
        return createJsonResponse([{
          STATE: 'half_open',
          OPEN_UNTIL: null,
          OPEN_REASON: 'http_429',
          VERSION: 21,
          LAST_ERROR_CODE: 429,
          HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
          ATTEMPT_GRANTED: true,
          ATTEMPT_TICKET: 1,
        }]);
      }
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 31,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 2,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/stale') {
      return new Response('expired', {
        status: 401,
        headers: { 'content-type': 'text/plain' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/refresh-start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://files.office.com/final' },
      });
    }

    if (url === 'https://files.office.com/final') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/refreshed-external-redirect.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.equal(linkFetchCount, 2);
    assert.equal(snapshotBodies.length, 1);
    assert.deepEqual(
      authorizeBodies.map((body) => body.p_hostname),
      ['tenant.sharepoint.com', 'tenant.sharepoint.com', 'files.office.com'],
    );
    assert.deepEqual(
      reportBodies.map((body) => ({
        hostname: body.p_hostname,
        statusCode: body.p_status_code,
        attemptVersion: body.p_attempt_version,
        attemptTicket: body.p_attempt_ticket,
      })),
      [
        {
          hostname: 'tenant.sharepoint.com',
          statusCode: 302,
          attemptVersion: 21,
          attemptTicket: 1,
        },
        {
          hostname: 'files.office.com',
          statusCode: 200,
          attemptVersion: 31,
          attemptTicket: 2,
        },
      ],
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker settles old attempt before CQ wait and reauthorizes after CQ grant', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const calls = [];
  const settleBodies = [];
  const authorizeBodies = [];
  const fairQueueReleaseBodies = [];
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse({
        ...buildRuntimeBootstrap({ fairQueueHostPatterns: ['*.sharepoint.com'] }),
        download: {
          ...buildRuntimeBootstrap({ fairQueueHostPatterns: ['*.sharepoint.com'] }).download,
          trueConcurrency: {
            enabled: true,
            hostPatterns: ['*.sharepoint.com'],
            handlerUrl: 'https://cq.example.test',
            handlerAuthKey: 'cq-secret',
            heartbeat: DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
          },
        },
      });
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/file',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-qb-wait',
        invocationEpoch: 1,
        slotToken: 'slot-qb-wait',
        meta: {
          attemptVersion: 44,
          attemptTicket: 6,
        },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      calls.push(body.waitToken ? 'concurrency-acquire-continue' : 'concurrency-acquire-fast');
      if (!body.waitToken) {
        return createJsonResponse({
          result: 'wait',
          waitToken: 'wait-qb-1',
          scope: 'host',
          retryAfter: 1,
        });
      }
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-qb-1',
        leaseToken: 'token-qb-1',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-qb-1',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return createClaimGrantResponse({
        leaseId: 'lease-qb-1',
        leaseToken: 'token-qb-1',
      });
    }

    if (url === ACK_HANDOFF_URL) {
      calls.push('concurrency-ack-handoff');
      return createJsonResponse({ result: 'acknowledged' });
    }

    if (url === HEARTBEAT_URL) {
      calls.push('heartbeat-upgrade');
      return {
        status: 101,
        webSocket: createFakeHeartbeatSocket(),
      };
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      calls.push('breaker-settle');
      settleBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 44,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      fairQueueReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.push('breaker-authorize');
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 45,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 8,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('qb-wait-ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      calls.push('breaker-report');
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 46,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/qb-cq-wait.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'qb-wait-ok');
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(calls, [
      'fairqueue-acquire',
      'concurrency-acquire-fast',
      'breaker-settle',
      'fairqueue-release',
      'concurrency-acquire-continue',
      'concurrency-claim',
      'concurrency-ack-handoff',
      'breaker-authorize',
      'heartbeat-upgrade',
      'origin-fetch',
      'breaker-report',
      'concurrency-release',
    ]);
    assert.equal(settleBodies.length, 1);
    assert.equal(settleBodies[0].p_attempt_version, 44);
    assert.equal(settleBodies[0].p_attempt_ticket, 6);
    assert.equal(authorizeBodies.length, 1);
    assert.equal(fairQueueReleaseBodies[0].hitUpstreamAtMs, 0);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker cancels CQ wait when old attempt settlement fails before continue-wait', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const calls = [];
  const cancelBodies = [];
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse({
        ...buildRuntimeBootstrap({ fairQueueHostPatterns: ['*.sharepoint.com'] }),
        download: {
          ...buildRuntimeBootstrap({ fairQueueHostPatterns: ['*.sharepoint.com'] }).download,
          trueConcurrency: {
            enabled: true,
            hostPatterns: ['*.sharepoint.com'],
            handlerUrl: 'https://cq.example.test',
            handlerAuthKey: 'cq-secret',
            heartbeat: DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
          },
        },
      });
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/file',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-qb-settle-fail',
        invocationEpoch: 1,
        slotToken: 'slot-qb-settle-fail',
        meta: {
          attemptVersion: 64,
          attemptTicket: 11,
        },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      calls.push(body.waitToken ? 'concurrency-acquire-continue' : 'concurrency-acquire-fast');
      if (!body.waitToken) {
        return createJsonResponse({
          result: 'wait',
          waitToken: 'wait-qb-settle-fail-1',
          scope: 'host',
          retryAfter: 1,
        });
      }
      throw new Error('worker must not continue CQ wait after settle failure');
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      calls.push('breaker-settle');
      throw new Error('settle unavailable');
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/cancel') {
      calls.push('concurrency-cancel');
      cancelBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'cancelled' });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      throw new Error('origin fetch should not run after breaker settle failure');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/qb-cq-settle-fail.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    const body = await response.json();
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.match(body.message, /attempt settlement/i);
    assert.deepEqual(calls, [
      'fairqueue-acquire',
      'concurrency-acquire-fast',
      'breaker-settle',
      'fairqueue-release',
      'concurrency-cancel',
    ]);
    assert.equal(cancelBodies.length, 1);
    assert.equal(cancelBodies[0].reason, 'worker_aborted');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker releases CQ lease and returns breaker terminal response when fresh authorize denies after wait grant', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const calls = [];
  const fairQueueReleaseBodies = [];
  const concurrencyReleaseBodies = [];
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse({
        ...buildRuntimeBootstrap({ fairQueueHostPatterns: ['*.sharepoint.com'] }),
        download: {
          ...buildRuntimeBootstrap({ fairQueueHostPatterns: ['*.sharepoint.com'] }).download,
          trueConcurrency: {
            enabled: true,
            hostPatterns: ['*.sharepoint.com'],
            handlerUrl: 'https://cq.example.test',
            handlerAuthKey: 'cq-secret',
            heartbeat: DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
          },
        },
      });
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/file',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-qb-deny-after-wait',
        invocationEpoch: 1,
        slotToken: 'slot-qb-deny-after-wait',
        meta: {
          attemptVersion: 54,
          attemptTicket: 9,
        },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      calls.push(body.waitToken ? 'concurrency-acquire-continue' : 'concurrency-acquire-fast');
      if (!body.waitToken) {
        return createJsonResponse({
          result: 'wait',
          waitToken: 'wait-qb-deny-1',
          scope: 'host',
          retryAfter: 1,
        });
      }
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-qb-deny-1',
        leaseToken: 'token-qb-deny-1',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-qb-deny-1',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return createClaimGrantResponse({
        leaseId: 'lease-qb-deny-1',
        leaseToken: 'token-qb-deny-1',
      });
    }

    if (url === ACK_HANDOFF_URL) {
      calls.push('concurrency-ack-handoff');
      return createJsonResponse({ result: 'acknowledged' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      calls.push('breaker-settle');
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 54,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      fairQueueReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.push('breaker-authorize');
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 55,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: false,
        ATTEMPT_TICKET: null,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      concurrencyReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      throw new Error('origin fetch should not run after fresh breaker deny');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/qb-cq-deny-after-wait.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 429);
    assert.deepEqual(calls, [
      'fairqueue-acquire',
      'concurrency-acquire-fast',
      'breaker-settle',
      'fairqueue-release',
      'concurrency-acquire-continue',
      'concurrency-claim',
      'concurrency-ack-handoff',
      'breaker-authorize',
      'concurrency-release',
    ]);
    assert.equal(fairQueueReleaseBodies[0].hitUpstreamAtMs, 0);
    assert.equal(concurrencyReleaseBodies.length, 1);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('worker blocks same-host refresh when a new authorize call returns reopened state', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const reportBodies = [];
  const settleBodies = [];
  let linkFetchCount = 0;
  let authorizeCalls = 0;
  let initialUpstreamFetches = 0;
  let refreshedUpstreamFetches = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      linkFetchCount += 1;
      return createJsonResponse({
        code: 200,
        data: {
          url: linkFetchCount === 1
            ? 'https://tenant.sharepoint.com/start'
            : 'https://tenant.sharepoint.com/fresh',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 10,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeCalls += 1;
      if (authorizeCalls === 1) {
        return createJsonResponse([{
          STATE: 'half_open',
          OPEN_UNTIL: null,
          OPEN_REASON: 'http_429',
          VERSION: 10,
          LAST_ERROR_CODE: 429,
          HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
          ATTEMPT_GRANTED: true,
          ATTEMPT_TICKET: 1,
        }]);
      }
      return createJsonResponse([{
        STATE: 'open',
        OPEN_UNTIL: Math.floor(Date.now() / 1000) + 30,
        OPEN_REASON: 'http_429',
        VERSION: 11,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: null,
        ATTEMPT_GRANTED: false,
        ATTEMPT_TICKET: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 12,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      settleBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 10,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/start') {
      initialUpstreamFetches += 1;
      return new Response('expired', {
        status: 401,
        headers: { 'content-type': 'text/plain' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/fresh') {
      refreshedUpstreamFetches += 1;
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/same-host-refresh-open.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 429);
    assert.equal(linkFetchCount, 2);
    assert.equal(authorizeCalls, 2);
    assert.equal(initialUpstreamFetches, 1);
    assert.equal(refreshedUpstreamFetches, 0);
    assert.deepEqual(reportBodies, []);
    assert.equal(settleBodies.length, 1);
    assert.equal(settleBodies[0].p_attempt_version, 10);
    assert.equal(settleBodies[0].p_attempt_ticket, 1);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('worker blocks same-host refresh when a new authorize call finds a full half_open epoch', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const reportBodies = [];
  const settleBodies = [];
  let linkFetchCount = 0;
  let authorizeCalls = 0;
  let initialUpstreamFetches = 0;
  let refreshedUpstreamFetches = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      linkFetchCount += 1;
      return createJsonResponse({
        code: 200,
        data: {
          url: linkFetchCount === 1
            ? 'https://tenant.sharepoint.com/start'
            : 'https://tenant.sharepoint.com/fresh',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 10,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeCalls += 1;
      if (authorizeCalls === 1) {
        return createJsonResponse([{
          STATE: 'half_open',
          OPEN_UNTIL: null,
          OPEN_REASON: 'http_429',
          VERSION: 10,
          LAST_ERROR_CODE: 429,
          HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
          ATTEMPT_GRANTED: true,
          ATTEMPT_TICKET: 1,
        }]);
      }
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 10,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: false,
        ATTEMPT_TICKET: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 12,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      settleBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 10,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/start') {
      initialUpstreamFetches += 1;
      return new Response('expired', {
        status: 401,
        headers: { 'content-type': 'text/plain' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/fresh') {
      refreshedUpstreamFetches += 1;
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/same-host-refresh-full-epoch.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 429);
    assert.equal(linkFetchCount, 2);
    assert.equal(authorizeCalls, 2);
    assert.equal(initialUpstreamFetches, 1);
    assert.equal(refreshedUpstreamFetches, 0);
    assert.deepEqual(reportBodies, []);
    assert.equal(settleBodies.length, 1);
    assert.equal(settleBodies[0].p_attempt_version, 10);
    assert.equal(settleBodies[0].p_attempt_ticket, 1);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('worker settles live breaker_only attempt before refresh enters queue_breaker when fair queue prep fails', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const calls = [];
  const settleBodies = [];
  let linkFetchCount = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: ['*.office.com', '*.sharepoint.com'],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      linkFetchCount += 1;
      calls.push(`link-${linkFetchCount}`);
      return createJsonResponse({
        code: 200,
        data: {
          url: linkFetchCount === 1
            ? 'https://files.office.com/stale'
            : 'https://tenant.sharepoint.com/fresh',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      calls.push('breaker-snapshot');
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 70,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.push('breaker-authorize');
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 70,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 5,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      calls.push('breaker-settle');
      settleBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 70,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      throw new Error('fair queue acquire should not run after fair queue prep failure');
    }

    if (url === 'https://files.office.com/stale') {
      calls.push('origin-fetch-401');
      return new Response('expired', {
        status: 401,
        headers: { 'content-type': 'text/plain' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/fresh') {
      throw new Error('refreshed queue_breaker target should not fetch after fair queue prep failure');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const baseRequest = await buildSignedWorkerRequest('/downloads/refresh-to-queue-breaker-prep-failure.bin');
    const requestHeaders = new Headers(baseRequest.headers);
    requestHeaders.delete('CF-Connecting-IP');
    const request = new Request(baseRequest, { headers: requestHeaders });

    const response = await worker.fetch(request, {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    const body = await response.json();
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.equal(body.message, 'Fair queue unavailable');
    assert.equal(linkFetchCount, 2);
    assert.deepEqual(calls, [
      'link-1',
      'breaker-snapshot',
      'breaker-authorize',
      'origin-fetch-401',
      'link-2',
      'breaker-settle',
    ]);
    assert.equal(settleBodies.length, 1);
    assert.equal(settleBodies[0].p_attempt_version, 70);
    assert.equal(settleBodies[0].p_attempt_ticket, 5);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('worker settles live breaker_only attempt before refresh enters true concurrency when CQ acquire fails', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const calls = [];
  const settleBodies = [];
  const cancelBodies = [];
  let linkFetchCount = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse({
        ...buildRuntimeBootstrap({
          hostPatterns: ['*.office.com', '*.sharepoint.com'],
        }),
        download: {
          ...buildRuntimeBootstrap({
            hostPatterns: ['*.office.com', '*.sharepoint.com'],
          }).download,
          trueConcurrency: {
            enabled: true,
            hostPatterns: ['*.sharepoint.com'],
            handlerUrl: 'https://cq.example.test',
            handlerAuthKey: 'cq-secret',
            heartbeat: DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
          },
        },
      });
    }

    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      linkFetchCount += 1;
      calls.push(`link-${linkFetchCount}`);
      return createJsonResponse({
        code: 200,
        data: {
          url: linkFetchCount === 1
            ? 'https://files.office.com/stale'
            : 'https://tenant.sharepoint.com/fresh',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      calls.push('breaker-snapshot');
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 90,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.push('breaker-authorize');
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 91,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 6,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      calls.push('breaker-settle');
      settleBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 91,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('cq-acquire');
      return new Response(JSON.stringify({
        code: 503,
        message: 'cq unavailable',
      }), {
        status: 503,
        headers: { 'content-type': 'application/json' },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/cancel') {
      calls.push('cq-cancel');
      cancelBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'cancelled' });
    }

    if (url === 'https://files.office.com/stale') {
      calls.push('origin-fetch-401');
      return new Response('expired', {
        status: 401,
        headers: { 'content-type': 'text/plain' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/fresh') {
      throw new Error('refreshed breaker_only true-concurrency target should not fetch after CQ acquire failure');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/refresh-to-cq-acquire-failure.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    const body = await response.json();
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.equal(body.message, 'True concurrency unavailable');
    assert.equal(linkFetchCount, 2);
    assert.deepEqual(calls, [
      'link-1',
      'breaker-snapshot',
      'breaker-authorize',
      'origin-fetch-401',
      'link-2',
      'breaker-settle',
      'cq-acquire',
      'cq-cancel',
    ]);
    assert.equal(cancelBodies.length, 1);
    assert.equal(settleBodies.length, 1);
    assert.equal(settleBodies[0].p_attempt_version, 91);
    assert.equal(settleBodies[0].p_attempt_ticket, 6);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('worker ignores unified-check open breaker rows for unmanaged hosts', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  let upstreamFetches = 0;
  let authorizeCalls = 0;
  let reportCalls = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        rateLimit: {
          enabled: true,
          windowSeconds: 60,
          limit: 10,
        },
      }));
    }

    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://unmanaged.example.com/file',
          header: {},
        },
      });
    }

    if (url === 'https://postgrest.example.test/rpc/download_unified_check') {
      return createJsonResponse([{
        cache_link_data: null,
        cache_timestamp: null,
        cache_hostname_hash: null,
        rate_access_count: 0,
        rate_last_window_time: Math.floor(Date.now() / 1000),
        rate_block_until: null,
        throttle_record_exists: true,
        throttle_state: 'open',
        throttle_open_until: Math.floor(Date.now() / 1000) + 30,
        throttle_reason: 'http_429',
        throttle_version: 9,
        throttle_last_error_code: 429,
        active_last_access_time: null,
        active_total_access_count: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeCalls += 1;
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportCalls += 1;
      return createJsonResponse([]);
    }

    if (url === 'https://unmanaged.example.com/file') {
      upstreamFetches += 1;
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/unmanaged.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.equal(upstreamFetches, 1);
    assert.equal(authorizeCalls, 0);
    assert.equal(reportCalls, 0);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('unified breaker lookup uses actual Google-family host authority', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const googleAuthorityHash = await decodeHostnameHash('google');
  const driveHostHash = await decodeHostnameHash('drive.google.com');
  const googleApiHostHash = await decodeHostnameHash('www.googleapis.com');
  const unifiedBodies = [];
  const authorizeBodies = [];
  const reportBodies = [];
  const breakerStateByHash = new Map();
  let originFetches = 0;
  let linkCallCount = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: [
          'drive.google.com',
          '*.googleapis.com',
          '*.googleusercontent.com',
        ],
        rateLimit: {
          enabled: true,
          windowSeconds: 60,
          limit: 10,
        },
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      linkCallCount += 1;
      return createJsonResponse({
        code: 200,
        data: {
          url: linkCallCount === 1
            ? 'https://drive.google.com/uc?id=unified-test&export=download'
            : 'https://www.googleapis.com/drive/v3/files/unified-test?alt=media',
          header: {},
        },
      });
    }

    if (url === 'https://postgrest.example.test/rpc/download_unified_check') {
      const body = JSON.parse(init.body);
      unifiedBodies.push(body);
      const snapshot = breakerStateByHash.get(body.p_throttle_hostname_hash);
      return createJsonResponse([{
        cache_link_data: null,
        cache_timestamp: null,
        cache_hostname_hash: null,
        rate_access_count: 0,
        rate_last_window_time: Math.floor(Date.now() / 1000),
        rate_block_until: null,
        throttle_record_exists: Boolean(snapshot),
        throttle_state: snapshot?.STATE || null,
        throttle_open_until: snapshot?.OPEN_UNTIL || null,
        throttle_reason: snapshot?.OPEN_REASON || null,
        throttle_version: snapshot?.VERSION || null,
        throttle_last_error_code: snapshot?.LAST_ERROR_CODE || null,
        active_last_access_time: null,
        active_total_access_count: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      const body = JSON.parse(init.body);
      reportBodies.push(body);
      breakerStateByHash.set(body.p_hostname_hash, {
        STATE: 'open',
        OPEN_UNTIL: Math.floor(Date.now() / 1000) + 30,
        OPEN_REASON: `http_${body.p_status_code}`,
        VERSION: 5,
        LAST_ERROR_CODE: body.p_status_code,
      });
      return createJsonResponse([{
        STATE: 'open',
        OPEN_UNTIL: Math.floor(Date.now() / 1000) + 30,
        OPEN_REASON: `http_${body.p_status_code}`,
        VERSION: 5,
        LAST_ERROR_CODE: body.p_status_code,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      const body = JSON.parse(init.body);
      authorizeBodies.push(body);
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 5,
        LAST_ERROR_CODE: null,
        ATTEMPT_GRANTED: false,
        ATTEMPT_TICKET: null,
      }]);
    }

    if (url === 'https://drive.google.com/uc?id=unified-test&export=download') {
      originFetches += 1;
      return new Response('protected', {
        status: 429,
        headers: {
          'content-type': 'text/plain',
          'Retry-After': '8',
        },
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/unified-test?alt=media') {
      originFetches += 1;
      return new Response('unexpected', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const firstResponse = await worker.fetch(await buildSignedWorkerRequest('/downloads/unified-google-drive.bin', {
      payloadFileSize: 13,
    }), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });
    assert.equal(firstResponse.status, 429);

    const secondResponse = await worker.fetch(await buildSignedWorkerRequest('/downloads/unified-google-api.bin', {
      payloadFileSize: 13,
    }), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });
    assert.equal(secondResponse.status, 200);

    await Promise.allSettled(waitUntilPromises);

    assert.deepEqual(
      unifiedBodies.map((body) => body.p_throttle_hostname_hash),
      [driveHostHash, googleApiHostHash],
    );
    assert.notDeepEqual(
      unifiedBodies.map((body) => body.p_throttle_hostname_hash),
      [googleAuthorityHash, googleAuthorityHash],
    );
    assert.deepEqual(
      authorizeBodies.map((body) => body.p_hostname),
      ['drive.google.com', 'www.googleapis.com'],
    );
    assert.deepEqual(
      reportBodies.map((body) => ({ hostname: body.p_hostname, hostnameHash: body.p_hostname_hash })),
      [
        { hostname: 'drive.google.com', hostnameHash: driveHostHash },
        { hostname: 'www.googleapis.com', hostnameHash: googleApiHostHash },
      ],
    );
    assert.equal(reportBodies[0].p_hostname_hash, await decodeHostnameHash('drive.google.com'));
    assert.notEqual(reportBodies[0].p_hostname_hash, googleAuthorityHash);
    assert.notEqual(reportBodies[1].p_hostname_hash, googleAuthorityHash);
    assert.equal(originFetches, 2);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('cache-hit unified breaker lookup uses cached actual Google API host hash', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const unifiedBodies = [];
  const googleAuthorityHash = await decodeHostnameHash('google');
  const googleApiHostHash = await decodeHostnameHash('www.googleapis.com');
  let originFetches = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        cacheEnabled: true,
        hostPatterns: [
          'drive.google.com',
          '*.googleapis.com',
          '*.googleusercontent.com',
        ],
        rateLimit: {
          enabled: true,
          windowSeconds: 60,
          limit: 10,
        },
      }));
    }

    if (url === 'https://postgrest.example.test/rpc/download_unified_check') {
      const body = JSON.parse(init.body);
      unifiedBodies.push(body);
      assert.equal(body.p_throttle_hostname_hash, null);
      return createJsonResponse([{
        cache_link_data: JSON.stringify({
          url: 'https://www.googleapis.com/drive/v3/files/cache-hit?alt=media',
          header: {},
        }),
        cache_timestamp: Math.floor(Date.now() / 1000),
        cache_hostname_hash: googleApiHostHash,
        rate_access_count: 0,
        rate_last_window_time: Math.floor(Date.now() / 1000),
        rate_block_until: null,
        throttle_record_exists: true,
        throttle_state: 'open',
        throttle_open_until: Math.floor(Date.now() / 1000) + 30,
        throttle_reason: 'http_429',
        throttle_version: 9,
        throttle_last_error_code: 429,
        active_last_access_time: null,
        active_total_access_count: null,
      }]);
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/cache-hit?alt=media') {
      originFetches += 1;
      return new Response('unexpected', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/unified-cache-hit-google-api.bin', {
      payloadFileSize: 13,
    }), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 429);
    assert.equal(unifiedBodies.length, 1);
    assert.equal(googleApiHostHash, await decodeHostnameHash('www.googleapis.com'));
    assert.notEqual(googleApiHostHash, googleAuthorityHash);
    assert.equal(originFetches, 0);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker ignores unified-check breaker rows and still reaches atomic slot admission', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const unifiedBodies = [];
  const reportBodies = [];
  let alistLinkCalls = 0;
  let acquireCalls = 0;
  let authorizeCalls = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        rateLimit: {
          enabled: true,
          windowSeconds: 60,
          limit: 10,
        },
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      alistLinkCalls += 1;
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/file',
          header: {},
        },
      });
    }

    if (url === 'https://postgrest.example.test/rpc/download_unified_check') {
      unifiedBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        cache_link_data: null,
        cache_timestamp: null,
        cache_hostname_hash: null,
        rate_access_count: 0,
        rate_last_window_time: Math.floor(Date.now() / 1000),
        rate_block_until: null,
        throttle_record_exists: true,
        throttle_state: 'open',
        throttle_open_until: Math.floor(Date.now() / 1000) + 30,
        throttle_reason: 'http_429',
        throttle_version: 9,
        throttle_last_error_code: 429,
        active_last_access_time: null,
        active_total_access_count: null,
      }]);
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireCalls += 1;
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-unified-queue-breaker',
        invocationEpoch: 1,
        slotToken: 'slot-1',
        meta: {
          attemptVersion: 7,
          attemptTicket: 2,
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeCalls += 1;
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 10,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/unified-queue-breaker.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.equal(alistLinkCalls, 1);
    assert.equal(unifiedBodies.length, 1);
    assert.equal(unifiedBodies[0].p_throttle_hostname_hash, null);
    assert.equal(acquireCalls, 1);
    assert.equal(authorizeCalls, 0);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].p_attempt_version, 7);
    assert.equal(reportBodies[0].p_attempt_ticket, 2);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker cache-hit unified flow ignores breaker rows and still reaches slot admission', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const unifiedBodies = [];
  const reportBodies = [];
  let alistLinkCalls = 0;
  let acquireCalls = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        cacheEnabled: true,
        rateLimit: {
          enabled: true,
          windowSeconds: 60,
          limit: 10,
        },
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://postgrest.example.test/rpc/download_unified_check') {
      unifiedBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        cache_link_data: JSON.stringify({
          url: 'https://tenant.sharepoint.com/file',
          header: {},
        }),
        cache_timestamp: Math.floor(Date.now() / 1000),
        cache_hostname_hash: 'cached-host-hash',
        rate_access_count: 0,
        rate_last_window_time: Math.floor(Date.now() / 1000),
        rate_block_until: null,
        throttle_record_exists: true,
        throttle_state: 'open',
        throttle_open_until: Math.floor(Date.now() / 1000) + 30,
        throttle_reason: 'http_429',
        throttle_version: 9,
        throttle_last_error_code: 429,
        active_last_access_time: null,
        active_total_access_count: null,
      }]);
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      alistLinkCalls += 1;
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/file',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireCalls += 1;
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-unified-cache-hit-queue-breaker',
        invocationEpoch: 1,
        slotToken: 'slot-1',
        meta: {
          attemptVersion: 7,
          attemptTicket: 2,
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 10,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/unified-cache-hit-queue-breaker.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.equal(unifiedBodies.length, 1);
    assert.equal(unifiedBodies[0].p_throttle_hostname_hash, null);
    assert.equal(alistLinkCalls, 0);
    assert.equal(acquireCalls, 1);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].p_attempt_version, 7);
    assert.equal(reportBodies[0].p_attempt_ticket, 2);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker HALF_OPEN_FULL returns throttle-protected response and never releases a slot', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  let authorizeCalls = 0;
  let reportCalls = 0;
  let releaseCalls = 0;
  let upstreamFetches = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/file',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 12,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      return createJsonResponse({
        result: 'throttled',
        queryToken: 'query-half-open-full',
        invocationEpoch: 1,
        reason: 'try_acquire_half_open_full',
        throttleCode: 503,
        retryAfter: 9,
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseCalls += 1;
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeCalls += 1;
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportCalls += 1;
      return createJsonResponse([]);
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      upstreamFetches += 1;
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/half-open-full.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.equal(response.headers.get('Retry-After'), '9');
    assert.equal(response.headers.get('X-Throttle-Protected'), 'true');
    assert.equal(authorizeCalls, 0);
    assert.equal(reportCalls, 0);
    assert.equal(releaseCalls, 0);
    assert.equal(upstreamFetches, 0);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker redirects reacquire atomic attempts on managed host changes', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const authorizeBodies = [];
  const reportBodies = [];
  const acquireBodies = [];
  const releaseBodies = [];
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: ['*.sharepoint.com'],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://a.sharepoint.com/start',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: `slot-${acquireBodies.length}`,
        meta: {
          attemptVersion: 100 + acquireBodies.length,
          attemptTicket: acquireBodies.length,
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 22,
        LAST_ERROR_CODE: null,
        ATTEMPT_GRANTED: false,
        ATTEMPT_TICKET: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
        }]);
    }

    if (url === 'https://a.sharepoint.com/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://b.sharepoint.com/final' },
      });
    }

    if (url === 'https://b.sharepoint.com/final') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-breaker-redirect.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.equal(acquireBodies.length, 2);
    assert.deepEqual(acquireBodies.map((body) => body.hostname), [
      'a.sharepoint.com',
      'b.sharepoint.com',
    ]);
    assert.deepEqual(authorizeBodies, []);
    assert.deepEqual(releaseBodies.map((body) => body.hostname), [
      'a.sharepoint.com',
      'b.sharepoint.com',
    ]);
    assert.deepEqual(
      reportBodies.map((body) => ({ hostname: body.p_hostname, attemptVersion: body.p_attempt_version, attemptTicket: body.p_attempt_ticket })),
      [
        { hostname: 'a.sharepoint.com', attemptVersion: 101, attemptTicket: 1 },
        { hostname: 'b.sharepoint.com', attemptVersion: 102, attemptTicket: 2 },
      ],
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker reacquires and reports actual Google-family host attempts across cross-host redirects', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const googleAuthorityHash = await decodeHostnameHash('google');
  const driveHostHash = await decodeHostnameHash('drive.google.com');
  const googleApiHostHash = await decodeHostnameHash('www.googleapis.com');
  const acquireBodies = [];
  const releaseBodies = [];
  const reportBodies = [];
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: [
          'drive.google.com',
          '*.googleapis.com',
          '*.googleusercontent.com',
        ],
        fairQueueHostPatterns: [
          'drive.google.com',
          '*.googleapis.com',
          '*.googleusercontent.com',
        ],
        fairQueueSiteBucket: {
          modes: ['googledrive'],
        },
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://drive.google.com/uc?id=grouped-redirect&export=download',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      const body = JSON.parse(init.body);
      acquireBodies.push(body);
      return createJsonResponse({
        result: 'granted',
        queryToken: `query-google-${acquireBodies.length}`,
        invocationEpoch: acquireBodies.length,
        slotToken: `slot-google-${acquireBodies.length}`,
        ...(body.breakerEnabled === true ? {
          meta: {
            attemptVersion: 800 + acquireBodies.length,
            attemptTicket: acquireBodies.length,
          },
        } : {}),
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      throw new Error('queue_breaker redirect lifecycle should not call direct breaker authorize');
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://drive.google.com/uc?id=grouped-redirect&export=download') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://www.googleapis.com/drive/v3/files/grouped-redirect?alt=media' },
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/grouped-redirect?alt=media') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-breaker-google-grouped-redirect.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.equal(acquireBodies.length, 2);
    assert.equal(acquireBodies[0].hostname, 'drive.google.com');
    assert.equal(acquireBodies[1].hostname, 'www.googleapis.com');
    assert.equal(acquireBodies[0].breakerEnabled, true);
    assert.equal(acquireBodies[1].breakerEnabled, true);
    assert.equal(acquireBodies[0].openCapSeconds, 60);
    assert.equal(acquireBodies[0].closeThresholdPercent, 15);
    assert.equal(acquireBodies[0].halfOpenSuccessThreshold, 2);
    assert.equal(acquireBodies[0].halfOpenCloseMode, 'and');
    assert.equal(acquireBodies[1].openCapSeconds, 60);
    assert.equal(acquireBodies[1].closeThresholdPercent, 15);
    assert.equal(acquireBodies[1].halfOpenSuccessThreshold, 2);
    assert.equal(acquireBodies[1].halfOpenCloseMode, 'and');
    assert.equal(acquireBodies[1].halfOpenMaxProbeCount, 4);
    assert.equal(acquireBodies[1].halfOpenMaxSeconds, 15);
    assert.equal(acquireBodies[1].halfOpenTimeoutMode, 'partial-close');
    assert.equal(acquireBodies[0].siteBucket, acquireBodies[1].siteBucket);
    assert.deepEqual(
      releaseBodies.map((body) => body.hostname),
      ['drive.google.com', 'www.googleapis.com'],
    );
    assert.deepEqual(
      reportBodies.map((body) => ({
        hostname: body.p_hostname,
        hostnameHash: body.p_hostname_hash,
        statusCode: body.p_status_code,
        attemptVersion: body.p_attempt_version,
        attemptTicket: body.p_attempt_ticket,
      })),
      [
        {
          hostname: 'drive.google.com',
          hostnameHash: driveHostHash,
          statusCode: 302,
          attemptVersion: 801,
          attemptTicket: 1,
        },
        {
          hostname: 'www.googleapis.com',
          hostnameHash: googleApiHostHash,
          statusCode: 200,
          attemptVersion: 802,
          attemptTicket: 2,
        },
      ],
    );
    assert.notEqual(reportBodies[0].p_hostname_hash, googleAuthorityHash);
    assert.notEqual(reportBodies[1].p_hostname_hash, googleAuthorityHash);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker reports actual Google redirect before redirected reacquire throttles', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const googleAuthorityHash = await decodeHostnameHash('google');
  const driveHostHash = await decodeHostnameHash('drive.google.com');
  const acquireBodies = [];
  const releaseBodies = [];
  const reportBodies = [];
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: [
          'drive.google.com',
          '*.googleapis.com',
          '*.googleusercontent.com',
        ],
        fairQueueHostPatterns: [
          'drive.google.com',
          '*.googleapis.com',
          '*.googleusercontent.com',
        ],
        fairQueueSiteBucket: {
          modes: ['googledrive'],
        },
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://drive.google.com/uc?id=grouped-redirect-throttled&export=download',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      const body = JSON.parse(init.body);
      acquireBodies.push(body);
      if (acquireBodies.length === 1) {
        return createJsonResponse({
          result: 'granted',
          queryToken: 'query-google-throttled-1',
          invocationEpoch: 1,
          slotToken: 'slot-google-throttled-1',
          meta: {
            attemptVersion: 801,
            attemptTicket: 1,
          },
        });
      }

      return createJsonResponse({
        result: 'throttled',
        queryToken: 'query-google-throttled-2',
        invocationEpoch: 2,
        throttleCode: 429,
        breakerOpenUntil: Math.floor(Date.now() / 1000) + 30,
        breakerReason: 'http_429',
        breakerVersion: 17,
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      throw new Error('queue_breaker redirect lifecycle should not call direct breaker authorize');
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://drive.google.com/uc?id=grouped-redirect-throttled&export=download') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://www.googleapis.com/drive/v3/files/grouped-redirect-throttled?alt=media' },
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/grouped-redirect-throttled?alt=media') {
      throw new Error('redirected Google target should not be fetched after reacquire throttle');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-breaker-google-grouped-redirect-throttled.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 429);
    assert.equal(acquireBodies.length, 2);
    assert.equal(acquireBodies[0].breakerEnabled, true);
    assert.equal(acquireBodies[1].breakerEnabled, true);
    assert.equal(acquireBodies[1].halfOpenMaxProbeCount, 4);
    assert.deepEqual(
      releaseBodies.map((body) => body.hostname),
      ['drive.google.com'],
    );
    assert.deepEqual(
      reportBodies.map((body) => ({
        hostname: body.p_hostname,
        hostnameHash: body.p_hostname_hash,
        statusCode: body.p_status_code,
        attemptVersion: body.p_attempt_version,
        attemptTicket: body.p_attempt_ticket,
      })),
      [{
        hostname: 'drive.google.com',
        hostnameHash: driveHostHash,
        statusCode: 302,
        attemptVersion: 801,
        attemptTicket: 1,
      }],
    );
    assert.notEqual(reportBodies[0].p_hostname_hash, googleAuthorityHash);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker dual mode reports actual Google redirect before fairqueue reacquire throttles', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const googleAuthorityHash = await decodeHostnameHash('google');
  const driveHostHash = await decodeHostnameHash('drive.google.com');
  const fairQueueAcquireBodies = [];
  const fairQueueReleaseBodies = [];
  const concurrencyAcquireBodies = [];
  const concurrencyReleaseBodies = [];
  const reportBodies = [];
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse({
        ...buildRuntimeBootstrap({
          hostPatterns: [
            'drive.google.com',
            '*.googleapis.com',
            '*.googleusercontent.com',
          ],
          fairQueueHostPatterns: [
            'drive.google.com',
            '*.googleapis.com',
            '*.googleusercontent.com',
          ],
          fairQueueSiteBucket: {
            modes: ['googledrive'],
          },
        }),
        download: {
          ...buildRuntimeBootstrap({
            hostPatterns: [
              'drive.google.com',
              '*.googleapis.com',
              '*.googleusercontent.com',
            ],
            fairQueueHostPatterns: [
              'drive.google.com',
              '*.googleapis.com',
              '*.googleusercontent.com',
            ],
            fairQueueSiteBucket: {
              modes: ['googledrive'],
            },
          }).download,
          trueConcurrency: {
            enabled: true,
            hostPatterns: [
              'drive.google.com',
              '*.googleapis.com',
              '*.googleusercontent.com',
            ],
            siteBucket: {
              modes: ['googledrive'],
            },
            handlerUrl: 'https://cq.example.test',
            handlerAuthKey: 'cq-secret',
            heartbeat: DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
          },
        },
      });
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://drive.google.com/uc?id=grouped-dual-redirect-throttled&export=download',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      const body = JSON.parse(init.body);
      fairQueueAcquireBodies.push(body);
      if (fairQueueAcquireBodies.length === 1) {
        return createJsonResponse({
          result: 'granted',
          queryToken: 'query-google-dual-throttled-1',
          invocationEpoch: 1,
          slotToken: 'slot-google-dual-throttled-1',
          meta: {
            attemptVersion: 801,
            attemptTicket: 1,
          },
        });
      }

      return createJsonResponse({
        result: 'throttled',
        queryToken: 'query-google-dual-throttled-2',
        invocationEpoch: 2,
        throttleCode: 429,
        breakerOpenUntil: Math.floor(Date.now() / 1000) + 30,
        breakerReason: 'http_429',
        breakerVersion: 17,
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      fairQueueReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      concurrencyAcquireBodies.push(body);
      if (concurrencyAcquireBodies.length > 1) {
        throw new Error('redirected Google target should not continue into CQ acquire after fairqueue throttle');
      }
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-google-dual-throttled-1',
        leaseToken: 'token-google-dual-throttled-1',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-google-dual-throttled-1',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponse({
        leaseId: 'lease-google-dual-throttled-1',
        leaseToken: 'token-google-dual-throttled-1',
      });
    }

    if (url === ACK_HANDOFF_URL) {
      return createJsonResponse({ result: 'acknowledged' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      concurrencyReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      throw new Error('queue_breaker redirect lifecycle should not call direct breaker authorize');
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://drive.google.com/uc?id=grouped-dual-redirect-throttled&export=download') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://www.googleapis.com/drive/v3/files/grouped-dual-redirect-throttled?alt=media' },
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/grouped-dual-redirect-throttled?alt=media') {
      throw new Error('redirected Google target should not be fetched after fairqueue throttle');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-breaker-google-grouped-dual-redirect-throttled.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 429);
    assert.equal(fairQueueAcquireBodies.length, 2);
    assert.equal(concurrencyAcquireBodies.length, 1);
    assert.equal(fairQueueAcquireBodies[0].breakerEnabled, true);
    assert.equal(fairQueueAcquireBodies[1].breakerEnabled, true);
    assert.deepEqual(
      fairQueueReleaseBodies.map((body) => body.hostname),
      ['drive.google.com'],
    );
    assert.equal(concurrencyReleaseBodies.length, 1);
    assert.deepEqual(
      reportBodies.map((body) => ({
        hostname: body.p_hostname,
        hostnameHash: body.p_hostname_hash,
        statusCode: body.p_status_code,
        attemptVersion: body.p_attempt_version,
        attemptTicket: body.p_attempt_ticket,
      })),
      [{
        hostname: 'drive.google.com',
        hostnameHash: driveHostHash,
        statusCode: 302,
        attemptVersion: 801,
        attemptTicket: 1,
      }],
    );
    assert.notEqual(reportBodies[0].p_hostname_hash, googleAuthorityHash);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker dual mode reports actual Google redirect when CQ expires before second-hop fetch', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const googleAuthorityHash = await decodeHostnameHash('google');
  const driveHostHash = await decodeHostnameHash('drive.google.com');
  const fairQueueAcquireBodies = [];
  const fairQueueReleaseBodies = [];
  const concurrencyAcquireBodies = [];
  const concurrencyReleaseBodies = [];
  const reportBodies = [];
  const settleBodies = [];
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse({
        ...buildRuntimeBootstrap({
          hostPatterns: [
            'drive.google.com',
            '*.googleapis.com',
            '*.googleusercontent.com',
          ],
          fairQueueHostPatterns: [
            'drive.google.com',
            '*.googleapis.com',
            '*.googleusercontent.com',
          ],
          fairQueueSiteBucket: {
            modes: ['googledrive'],
          },
        }),
        download: {
          ...buildRuntimeBootstrap({
            hostPatterns: [
              'drive.google.com',
              '*.googleapis.com',
              '*.googleusercontent.com',
            ],
            fairQueueHostPatterns: [
              'drive.google.com',
              '*.googleapis.com',
              '*.googleusercontent.com',
            ],
            fairQueueSiteBucket: {
              modes: ['googledrive'],
            },
          }).download,
          trueConcurrency: {
            enabled: true,
            hostPatterns: [
              'drive.google.com',
              '*.googleapis.com',
              '*.googleusercontent.com',
            ],
            siteBucket: {
              modes: ['googledrive'],
            },
            handlerUrl: 'https://cq.example.test',
            handlerAuthKey: 'cq-secret',
            heartbeat: DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
          },
        },
      });
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://drive.google.com/uc?id=grouped-dual-cq-expired&export=download',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      const body = JSON.parse(init.body);
      fairQueueAcquireBodies.push(body);
      return createJsonResponse({
        result: 'granted',
        queryToken: `query-google-dual-cq-expired-${fairQueueAcquireBodies.length}`,
        invocationEpoch: fairQueueAcquireBodies.length,
        slotToken: `slot-google-dual-cq-expired-${fairQueueAcquireBodies.length}`,
        ...(fairQueueAcquireBodies.length === 1 ? {
          meta: {
            attemptVersion: 801,
            attemptTicket: 1,
          },
        } : {}),
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      fairQueueReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      concurrencyAcquireBodies.push(body);
      if (concurrencyAcquireBodies.length === 1) {
        return createJsonResponse({
          result: 'granted',
          leaseId: 'lease-google-dual-cq-expired-1',
          leaseToken: 'token-google-dual-cq-expired-1',
          expiresAtMs: body.hardExpireAtMs,
          claimToken: 'claim-token-google-dual-cq-expired-1',
        });
      }

      return createJsonResponse({
        result: 'expired',
        reason: 'hard_expired',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      concurrencyReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponse({
        leaseId: 'lease-google-dual-cq-expired-1',
        leaseToken: 'token-google-dual-cq-expired-1',
      });
    }

    if (url === ACK_HANDOFF_URL) {
      return createJsonResponse({ result: 'acknowledged' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      throw new Error('queue_breaker redirect lifecycle should not call direct breaker authorize');
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      settleBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://drive.google.com/uc?id=grouped-dual-cq-expired&export=download') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://www.googleapis.com/drive/v3/files/grouped-dual-cq-expired?alt=media' },
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/grouped-dual-cq-expired?alt=media') {
      throw new Error('redirected Google target should not be fetched after CQ terminal response');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-breaker-google-grouped-dual-cq-expired.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 401);
    assert.equal(fairQueueAcquireBodies.length, 2);
    assert.equal(concurrencyAcquireBodies.length, 2);
    assert.equal(fairQueueAcquireBodies[0].breakerEnabled, true);
    assert.equal(fairQueueAcquireBodies[1].breakerEnabled, true);
    assert.deepEqual(
      fairQueueReleaseBodies.map((body) => body.hostname),
      ['drive.google.com', 'www.googleapis.com'],
    );
    assert.equal(concurrencyReleaseBodies.length, 1);
    assert.deepEqual(
      settleBodies,
      [],
    );
    assert.deepEqual(
      reportBodies.map((body) => ({
        hostname: body.p_hostname,
        hostnameHash: body.p_hostname_hash,
        statusCode: body.p_status_code,
        attemptVersion: body.p_attempt_version,
        attemptTicket: body.p_attempt_ticket,
      })),
      [{
        hostname: 'drive.google.com',
        hostnameHash: driveHostHash,
        statusCode: 302,
        attemptVersion: 801,
        attemptTicket: 1,
      }],
    );
    assert.notEqual(reportBodies[0].p_hostname_hash, googleAuthorityHash);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker dual mode reports actual Google redirect when CQ wait later expires before second-hop fetch', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const googleAuthorityHash = await decodeHostnameHash('google');
  const driveHostHash = await decodeHostnameHash('drive.google.com');
  const fairQueueAcquireBodies = [];
  const fairQueueReleaseBodies = [];
  const concurrencyAcquireBodies = [];
  const concurrencyReleaseBodies = [];
  const concurrencyCancelBodies = [];
  const reportBodies = [];
  const settleBodies = [];
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse({
        ...buildRuntimeBootstrap({
          hostPatterns: [
            'drive.google.com',
            '*.googleapis.com',
            '*.googleusercontent.com',
          ],
          fairQueueHostPatterns: [
            'drive.google.com',
            '*.googleapis.com',
            '*.googleusercontent.com',
          ],
          fairQueueSiteBucket: {
            modes: ['googledrive'],
          },
        }),
        download: {
          ...buildRuntimeBootstrap({
            hostPatterns: [
              'drive.google.com',
              '*.googleapis.com',
              '*.googleusercontent.com',
            ],
            fairQueueHostPatterns: [
              'drive.google.com',
              '*.googleapis.com',
              '*.googleusercontent.com',
            ],
            fairQueueSiteBucket: {
              modes: ['googledrive'],
            },
          }).download,
          trueConcurrency: {
            enabled: true,
            hostPatterns: [
              'drive.google.com',
              '*.googleapis.com',
              '*.googleusercontent.com',
            ],
            siteBucket: {
              modes: ['googledrive'],
            },
            handlerUrl: 'https://cq.example.test',
            handlerAuthKey: 'cq-secret',
            heartbeat: DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
          },
        },
      });
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://drive.google.com/uc?id=grouped-dual-cq-wait-expired&export=download',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      const body = JSON.parse(init.body);
      fairQueueAcquireBodies.push(body);
      return createJsonResponse({
        result: 'granted',
        queryToken: `query-google-dual-cq-wait-expired-${fairQueueAcquireBodies.length}`,
        invocationEpoch: fairQueueAcquireBodies.length,
        slotToken: `slot-google-dual-cq-wait-expired-${fairQueueAcquireBodies.length}`,
        ...(fairQueueAcquireBodies.length === 1 ? {
          meta: {
            attemptVersion: 801,
            attemptTicket: 1,
          },
        } : {}),
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      fairQueueReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      concurrencyAcquireBodies.push(body);
      if (concurrencyAcquireBodies.length === 1) {
        return createJsonResponse({
          result: 'granted',
          leaseId: 'lease-google-dual-cq-wait-expired-1',
          leaseToken: 'token-google-dual-cq-wait-expired-1',
          expiresAtMs: body.hardExpireAtMs,
          claimToken: 'claim-token-google-dual-cq-wait-expired-1',
        });
      }

      if (concurrencyAcquireBodies.length === 2) {
        return createJsonResponse({
          result: 'wait',
          waitToken: 'wait-google-dual-cq-wait-expired-1',
          scope: 'host',
          retryAfter: 1,
        });
      }

      return createJsonResponse({
        result: 'expired',
        reason: 'hard_expired',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      concurrencyReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponse({
        leaseId: 'lease-google-dual-cq-wait-expired-1',
        leaseToken: 'token-google-dual-cq-wait-expired-1',
      });
    }

    if (url === ACK_HANDOFF_URL) {
      return createJsonResponse({ result: 'acknowledged' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/cancel') {
      concurrencyCancelBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'cancelled' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      throw new Error('queue_breaker redirect lifecycle should not call direct breaker authorize');
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      settleBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://drive.google.com/uc?id=grouped-dual-cq-wait-expired&export=download') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://www.googleapis.com/drive/v3/files/grouped-dual-cq-wait-expired?alt=media' },
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/grouped-dual-cq-wait-expired?alt=media') {
      throw new Error('redirected Google target should not be fetched after CQ wait terminal response');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-breaker-google-grouped-dual-cq-wait-expired.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 401);
    assert.equal(fairQueueAcquireBodies.length, 2);
    assert.equal(concurrencyAcquireBodies.length, 3);
    assert.equal(fairQueueAcquireBodies[0].breakerEnabled, true);
    assert.equal(fairQueueAcquireBodies[1].breakerEnabled, true);
    assert.deepEqual(
      fairQueueReleaseBodies.map((body) => body.hostname),
      ['drive.google.com', 'www.googleapis.com'],
    );
    assert.equal(concurrencyReleaseBodies.length, 1);
    assert.equal(concurrencyCancelBodies.length, 1);
    assert.deepEqual(
      settleBodies,
      [],
    );
    assert.deepEqual(
      reportBodies.map((body) => ({
        hostname: body.p_hostname,
        hostnameHash: body.p_hostname_hash,
        statusCode: body.p_status_code,
        attemptVersion: body.p_attempt_version,
        attemptTicket: body.p_attempt_ticket,
      })),
      [{
        hostname: 'drive.google.com',
        hostnameHash: driveHostHash,
        statusCode: 302,
        attemptVersion: 801,
        attemptTicket: 1,
      }],
    );
    assert.notEqual(reportBodies[0].p_hostname_hash, googleAuthorityHash);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

for (const terminalCase of [
  {
    name: 'timeout',
    buildAcquireResponse: () => createJsonResponse({
      result: 'timeout',
    }),
  },
  {
    name: 'conflict',
    buildAcquireResponse: () => new Response(JSON.stringify({
      result: 'conflict',
    }), {
      status: 409,
      headers: { 'content-type': 'application/json' },
    }),
  },
]) {
  test(`queue_breaker reports actual Google redirect when redirected reacquire ${terminalCase.name}s before fetch`, async () => {
    const originalFetch = globalThis.fetch;
    const waitUntilPromises = [];
    const googleAuthorityHash = await decodeHostnameHash('google');
    const driveHostHash = await decodeHostnameHash('drive.google.com');
    const acquireBodies = [];
    const releaseBodies = [];
    const reportBodies = [];
    delete globalThis.bootstrapCache;

    globalThis.fetch = async (input, init = {}) => {
      const url = typeof input === 'string' ? input : input.url;

      if (url === 'https://controller.example.test/api/v0/bootstrap') {
        return createJsonResponse(buildRuntimeBootstrap({
          hostPatterns: [
            'drive.google.com',
            '*.googleapis.com',
            '*.googleusercontent.com',
          ],
          fairQueueHostPatterns: [
            'drive.google.com',
            '*.googleapis.com',
            '*.googleusercontent.com',
          ],
          fairQueueSiteBucket: {
            modes: ['googledrive'],
          },
        }));
      }

      if (url === 'https://alist.example.com/api/fs/link') {
        return createJsonResponse({
          code: 200,
          data: {
            url: `https://drive.google.com/uc?id=grouped-redirect-${terminalCase.name}&export=download`,
            header: {},
          },
        });
      }

      if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
        const body = JSON.parse(init.body);
        acquireBodies.push(body);
        if (acquireBodies.length === 1) {
          return createJsonResponse({
            result: 'granted',
            queryToken: `query-google-${terminalCase.name}-1`,
            invocationEpoch: 1,
            slotToken: `slot-google-${terminalCase.name}-1`,
            meta: {
              attemptVersion: 801,
              attemptTicket: 1,
            },
          });
        }

        return terminalCase.buildAcquireResponse();
      }

      if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
        releaseBodies.push(JSON.parse(init.body));
        return createJsonResponse({ result: 'ok' });
      }

      if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
        throw new Error('queue_breaker redirect lifecycle should not call direct breaker authorize');
      }

      if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
        reportBodies.push(JSON.parse(init.body));
        return createJsonResponse([{
          STATE: 'closed',
          OPEN_UNTIL: null,
          OPEN_REASON: null,
          VERSION: reportBodies.length,
          LAST_ERROR_CODE: null,
        }]);
      }

      if (url === `https://drive.google.com/uc?id=grouped-redirect-${terminalCase.name}&export=download`) {
        return new Response(null, {
          status: 302,
          headers: { Location: `https://www.googleapis.com/drive/v3/files/grouped-redirect-${terminalCase.name}?alt=media` },
        });
      }

      if (url === `https://www.googleapis.com/drive/v3/files/grouped-redirect-${terminalCase.name}?alt=media`) {
        throw new Error('redirected Google target should not be fetched after reacquire terminal response');
      }

      throw new Error(`Unexpected fetch URL in test: ${url}`);
    };

    try {
      const response = await worker.fetch(await buildSignedWorkerRequest(`/downloads/queue-breaker-google-grouped-redirect-${terminalCase.name}.bin`), {
        CONTROLLER_URL: 'https://controller.example.test',
        CONTROLLER_API_TOKEN: 'controller-token',
        ENV: 'test',
        ROLE: 'download',
        INSTANCE_ID: 'worker-1',
        BOOTSTRAP_CACHE_MODE: 'direct',
      }, {
        waitUntil(promise) {
          waitUntilPromises.push(promise);
        },
      });

      await Promise.all(waitUntilPromises);

      assert.equal(response.status, 503);
      assert.equal(acquireBodies.length, 2);
      assert.equal(acquireBodies[0].breakerEnabled, true);
      assert.equal(acquireBodies[1].breakerEnabled, true);
      assert.deepEqual(
        releaseBodies.map((body) => body.hostname),
        ['drive.google.com'],
      );
      assert.deepEqual(
        reportBodies.map((body) => ({
          hostname: body.p_hostname,
          hostnameHash: body.p_hostname_hash,
          statusCode: body.p_status_code,
          attemptVersion: body.p_attempt_version,
          attemptTicket: body.p_attempt_ticket,
        })),
        [{
          hostname: 'drive.google.com',
          hostnameHash: driveHostHash,
          statusCode: 302,
          attemptVersion: 801,
          attemptTicket: 1,
        }],
      );
      assert.notEqual(reportBodies[0].p_hostname_hash, googleAuthorityHash);
    } finally {
      globalThis.fetch = originalFetch;
      delete globalThis.bootstrapCache;
    }
  });
}

test('queue_breaker redirects reacquire when site buckets change on same host', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const authorizeBodies = [];
  const reportBodies = [];
  const acquireBodies = [];
  const releaseBodies = [];
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: ['*.sharepoint.com'],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/alpha/start',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: `slot-${acquireBodies.length}`,
        meta: {
          attemptVersion: 200 + acquireBodies.length,
          attemptTicket: acquireBodies.length,
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://tenant.sharepoint.com/sites/beta/final' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/beta/final') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-breaker-site-bucket-redirect.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.equal(acquireBodies.length, 2);
    assert.equal(acquireBodies[0].hostname, 'tenant.sharepoint.com');
    assert.equal(acquireBodies[1].hostname, 'tenant.sharepoint.com');
    assert.notEqual(acquireBodies[0].siteBucket, acquireBodies[1].siteBucket);
    assert.deepEqual(authorizeBodies, []);
    assert.equal(releaseBodies.length, 2);
    assert.equal(releaseBodies[0].hostname, 'tenant.sharepoint.com');
    assert.equal(releaseBodies[1].hostname, 'tenant.sharepoint.com');
    assert.notEqual(releaseBodies[0].siteBucket, releaseBodies[1].siteBucket);
    assert.deepEqual(
      reportBodies.map((body) => ({ hostname: body.p_hostname, attemptVersion: body.p_attempt_version, attemptTicket: body.p_attempt_ticket })),
      [
        { hostname: 'tenant.sharepoint.com', attemptVersion: 201, attemptTicket: 1 },
        { hostname: 'tenant.sharepoint.com', attemptVersion: 202, attemptTicket: 2 },
      ],
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker defers same-host same-site redirect reporting until the terminal hop without attempt metadata', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const authorizeBodies = [];
  const reportBodies = [];
  const acquireBodies = [];
  const releaseBodies = [];
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: ['*.sharepoint.com'],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/alpha/start',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: 'slot-1',
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'open',
        OPEN_UNTIL: Math.floor(Date.now() / 1000) + 60,
        OPEN_REASON: 'http_500',
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: 500,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://tenant.sharepoint.com/sites/alpha/final' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/final') {
      assert.equal(reportBodies.length, 0);
      return new Response('boom', {
        status: 500,
        headers: { 'content-type': 'text/plain' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-breaker-same-site-redirect.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 500);
    assert.equal(acquireBodies.length, 1);
    assert.equal(releaseBodies.length, 1);
    assert.deepEqual(authorizeBodies, []);
    assert.deepEqual(
      reportBodies.map((body) => ({
        hostname: body.p_hostname,
        statusCode: body.p_status_code,
        attemptVersion: body.p_attempt_version,
        attemptTicket: body.p_attempt_ticket,
      })),
      [
        {
          hostname: 'tenant.sharepoint.com',
          statusCode: 500,
          attemptVersion: null,
          attemptTicket: null,
        },
      ],
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker terminal report failure disarms deferred same-site redirect attempt', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const authorizeBodies = [];
  const reportBodies = [];
  const acquireBodies = [];
  const releaseBodies = [];
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: ['*.sharepoint.com'],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/alpha/start',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: 'slot-1',
        meta: {
          attemptVersion: 911,
          attemptTicket: 6,
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      throw new Error('report unavailable');
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://tenant.sharepoint.com/sites/alpha/final' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/final') {
      assert.equal(reportBodies.length, 0);
      return new Response('boom', {
        status: 500,
        headers: { 'content-type': 'text/plain' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-breaker-terminal-report-failure-disarms-deferred.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.equal(acquireBodies.length, 1);
    assert.equal(releaseBodies.length, 1);
    assert.deepEqual(authorizeBodies, []);
    assert.deepEqual(
      reportBodies.map((body) => ({
        hostname: body.p_hostname,
        statusCode: body.p_status_code,
        attemptVersion: body.p_attempt_version,
        attemptTicket: body.p_attempt_ticket,
      })),
      [{
        hostname: 'tenant.sharepoint.com',
        statusCode: 500,
        attemptVersion: 911,
        attemptTicket: 6,
      }],
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker flushes a deferred same-site redirect as 302 when the terminal response is 404 and slot admission returned attempt metadata', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const authorizeBodies = [];
  const reportBodies = [];
  const acquireBodies = [];
  const releaseBodies = [];
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: ['*.sharepoint.com'],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/alpha/start',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: 'slot-1',
        meta: {
          attemptVersion: 901,
          attemptTicket: 4,
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://tenant.sharepoint.com/sites/alpha/missing' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/missing') {
      assert.equal(reportBodies.length, 0);
      return new Response('missing', {
        status: 404,
        headers: { 'content-type': 'text/plain' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-breaker-same-site-redirect-404.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 404);
    assert.equal(acquireBodies.length, 1);
    assert.equal(releaseBodies.length, 1);
    assert.deepEqual(authorizeBodies, []);
    assert.deepEqual(
      reportBodies.map((body) => ({
        hostname: body.p_hostname,
        statusCode: body.p_status_code,
        attemptVersion: body.p_attempt_version,
        attemptTicket: body.p_attempt_ticket,
      })),
      [{
        hostname: 'tenant.sharepoint.com',
        statusCode: 302,
        attemptVersion: 901,
        attemptTicket: 4,
      }],
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker flushes deferred same-site redirect before propagating thrown fetch error', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const authorizeBodies = [];
  const reportBodies = [];
  const acquireBodies = [];
  const releaseBodies = [];
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: ['*.sharepoint.com'],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/alpha/start',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: 'slot-1',
        meta: {
          attemptVersion: 701,
          attemptTicket: 8,
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://tenant.sharepoint.com/sites/alpha/final' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/final') {
      assert.equal(reportBodies.length, 0);
      throw new Error('final hop fetch crashed');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-breaker-deferred-throw.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 500);
    assert.equal(acquireBodies.length, 1);
    assert.equal(releaseBodies.length, 1);
    assert.deepEqual(authorizeBodies, []);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].p_status_code, 302);
    assert.equal(reportBodies[0].p_attempt_version, 701);
    assert.equal(reportBodies[0].p_attempt_ticket, 8);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker flushes deferred same-site redirect before propagating refresh hop exception', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const authorizeBodies = [];
  const reportBodies = [];
  const acquireBodies = [];
  const releaseBodies = [];
  let linkFetchCount = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: ['*.sharepoint.com'],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      linkFetchCount += 1;
      if (linkFetchCount === 2) {
        assert.equal(reportBodies.length, 0);
      }
      return createJsonResponse({
        code: 200,
        data: {
          url: linkFetchCount === 1
            ? 'https://tenant.sharepoint.com/sites/alpha/start'
            : 'https://tenant.sharepoint.com/sites/alpha/final',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: 'slot-1',
        meta: {
          attemptVersion: 702,
          attemptTicket: 9,
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://tenant.sharepoint.com/sites/alpha/stale' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/stale') {
      return new Response('expired', {
        status: 401,
        headers: { 'content-type': 'text/plain' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/final') {
      assert.equal(reportBodies.length, 0);
      throw new Error('refreshed hop fetch crashed');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-breaker-deferred-refresh-throw.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 500);
    assert.equal(linkFetchCount, 2);
    assert.equal(acquireBodies.length, 1);
    assert.equal(releaseBodies.length, 1);
    assert.deepEqual(authorizeBodies, []);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].p_status_code, 302);
    assert.equal(reportBodies[0].p_attempt_version, 702);
    assert.equal(reportBodies[0].p_attempt_ticket, 9);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker returns authority unavailable when deferred flush fails but still releases slot', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const authorizeBodies = [];
  const reportBodies = [];
  const acquireBodies = [];
  const releaseBodies = [];
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: ['*.sharepoint.com'],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/alpha/start',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: 'slot-1',
        meta: {
          attemptVersion: 703,
          attemptTicket: 10,
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      throw new Error('report unavailable');
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://tenant.sharepoint.com/sites/alpha/final' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/final') {
      assert.equal(reportBodies.length, 0);
      throw new Error('terminal hop fetch crashed');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-breaker-deferred-flush-failure.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.equal(acquireBodies.length, 1);
    assert.equal(releaseBodies.length, 1);
    assert.deepEqual(authorizeBodies, []);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].p_status_code, 302);
    assert.equal(reportBodies[0].p_attempt_version, 703);
    assert.equal(reportBodies[0].p_attempt_ticket, 10);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker preserves a deferred same-site no-meta admission across auth refresh when the refreshed target keeps the same fair-queue context', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const authorizeBodies = [];
  const reportBodies = [];
  const acquireBodies = [];
  const releaseBodies = [];
  let linkFetchCount = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: ['*.sharepoint.com'],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      linkFetchCount += 1;
      if (linkFetchCount === 2) {
        assert.equal(reportBodies.length, 0);
      }
      return createJsonResponse({
        code: 200,
        data: {
          url: linkFetchCount === 1
            ? 'https://tenant.sharepoint.com/sites/alpha/start'
            : 'https://tenant.sharepoint.com/sites/alpha/final',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: 'slot-1',
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://tenant.sharepoint.com/sites/alpha/stale' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/stale') {
      return new Response('expired', {
        status: 401,
        headers: { 'content-type': 'text/plain' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/final') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-breaker-refresh-same-site.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.equal(linkFetchCount, 2);
    assert.equal(acquireBodies.length, 1);
    assert.equal(releaseBodies.length, 1);
    assert.deepEqual(authorizeBodies, []);
    assert.deepEqual(
      reportBodies.map((body) => ({
        hostname: body.p_hostname,
        statusCode: body.p_status_code,
        attemptVersion: body.p_attempt_version,
        attemptTicket: body.p_attempt_ticket,
      })),
      [
        {
          hostname: 'tenant.sharepoint.com',
          statusCode: 200,
          attemptVersion: null,
          attemptTicket: null,
        },
      ],
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker flushes a deferred same-site redirect as 302 after refresh when the refreshed terminal response is 403 and slot admission returned attempt metadata', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const authorizeBodies = [];
  const reportBodies = [];
  const acquireBodies = [];
  const releaseBodies = [];
  let linkFetchCount = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: ['*.sharepoint.com'],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      linkFetchCount += 1;
      if (linkFetchCount === 2) {
        assert.equal(reportBodies.length, 0);
      }
      return createJsonResponse({
        code: 200,
        data: {
          url: linkFetchCount === 1
            ? 'https://tenant.sharepoint.com/sites/alpha/start'
            : 'https://tenant.sharepoint.com/sites/alpha/forbidden',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: 'slot-1',
        meta: {
          attemptVersion: 902,
          attemptTicket: 5,
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://tenant.sharepoint.com/sites/alpha/stale' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/stale') {
      return new Response('expired', {
        status: 401,
        headers: { 'content-type': 'text/plain' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/forbidden') {
      assert.equal(reportBodies.length, 0);
      return new Response('forbidden', {
        status: 403,
        headers: { 'content-type': 'text/plain' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-breaker-refresh-same-site-403.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 403);
    assert.equal(linkFetchCount, 2);
    assert.equal(acquireBodies.length, 1);
    assert.equal(releaseBodies.length, 1);
    assert.deepEqual(authorizeBodies, []);
    assert.deepEqual(
      reportBodies.map((body) => ({
        hostname: body.p_hostname,
        statusCode: body.p_status_code,
        attemptVersion: body.p_attempt_version,
        attemptTicket: body.p_attempt_ticket,
      })),
      [{
        hostname: 'tenant.sharepoint.com',
        statusCode: 302,
        attemptVersion: 902,
        attemptTicket: 5,
      }],
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker flushes a deferred same-site attempt only when refresh failure leaves the original auth error', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const authorizeBodies = [];
  const reportBodies = [];
  const acquireBodies = [];
  const releaseBodies = [];
  let linkFetchCount = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: ['*.sharepoint.com'],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      linkFetchCount += 1;
      if (linkFetchCount === 2) {
        assert.equal(reportBodies.length, 0);
        return createJsonResponse({ code: 500, message: 'refresh failed' });
      }
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/alpha/start',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: 'slot-1',
        meta: {
          attemptVersion: 801,
          attemptTicket: 6,
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://tenant.sharepoint.com/sites/alpha/stale' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/stale') {
      return new Response('expired', {
        status: 401,
        headers: { 'content-type': 'text/plain' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-breaker-refresh-failure.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 401);
    assert.equal(linkFetchCount, 2);
    assert.equal(acquireBodies.length, 1);
    assert.equal(releaseBodies.length, 1);
    assert.deepEqual(authorizeBodies, []);
    assert.deepEqual(
      reportBodies.map((body) => ({
        hostname: body.p_hostname,
        statusCode: body.p_status_code,
        attemptVersion: body.p_attempt_version,
        attemptTicket: body.p_attempt_ticket,
      })),
      [
        {
          hostname: 'tenant.sharepoint.com',
          statusCode: 302,
          attemptVersion: 801,
          attemptTicket: 6,
        },
      ],
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker flushes a deferred same-site redirect attempt before refreshing to a new breaker_only target', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const authorizeBodies = [];
  const reportBodies = [];
  const acquireBodies = [];
  const releaseBodies = [];
  let linkFetchCount = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: ['*.sharepoint.com', '*.office.com'],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      linkFetchCount += 1;
      return createJsonResponse({
        code: 200,
        data: {
          url: linkFetchCount === 1
            ? 'https://tenant.sharepoint.com/sites/alpha/start'
            : 'https://files.office.com/final',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: 'slot-1',
        meta: {
          attemptVersion: 601,
          attemptTicket: 9,
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 22,
        LAST_ERROR_CODE: null,
        HALF_OPEN_DEADLINE: null,
        ATTEMPT_GRANTED: false,
        ATTEMPT_TICKET: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://tenant.sharepoint.com/sites/alpha/stale' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/alpha/stale') {
      return new Response('expired', {
        status: 401,
        headers: { 'content-type': 'text/plain' },
      });
    }

    if (url === 'https://files.office.com/final') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-breaker-deferred-refresh.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.equal(linkFetchCount, 2);
    assert.equal(acquireBodies.length, 1);
    assert.equal(releaseBodies.length, 1);
    assert.deepEqual(authorizeBodies.map((body) => body.p_hostname), ['files.office.com']);
    assert.deepEqual(
      reportBodies.map((body) => ({
        hostname: body.p_hostname,
        statusCode: body.p_status_code,
        attemptVersion: body.p_attempt_version,
        attemptTicket: body.p_attempt_ticket,
      })),
      [
        {
          hostname: 'tenant.sharepoint.com',
          statusCode: 302,
          attemptVersion: 601,
          attemptTicket: 9,
        },
        {
          hostname: 'files.office.com',
          statusCode: 200,
          attemptVersion: null,
          attemptTicket: null,
        },
      ],
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_only redirects reacquire fair-queue slots across managed hosts', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const acquireBodies = [];
  const releaseBodies = [];
  let authorizeCalls = 0;
  let reportCalls = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: [],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://a.sharepoint.com/start',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: `slot-${acquireBodies.length}`,
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeCalls += 1;
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportCalls += 1;
      return createJsonResponse([]);
    }

    if (url === 'https://a.sharepoint.com/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://b.sharepoint.com/final' },
      });
    }

    if (url === 'https://b.sharepoint.com/final') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-only-redirect.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.equal(authorizeCalls, 0);
    assert.equal(reportCalls, 0);
    assert.equal(acquireBodies.length, 2);
    assert.deepEqual(acquireBodies.map((body) => body.hostname), [
      'a.sharepoint.com',
      'b.sharepoint.com',
    ]);
    assert.equal(releaseBodies.length, 2);
    assert.deepEqual(releaseBodies.map((body) => body.hostname), [
      'a.sharepoint.com',
      'b.sharepoint.com',
    ]);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_only retries release of the old managed context in finally after redirect release failure', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const acquireBodies = [];
  const releaseBodies = [];
  const releaseAttemptsByHost = new Map();
  let authorizeCalls = 0;
  let reportCalls = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: [],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://a.sharepoint.com/start',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: `slot-${acquireBodies.length}`,
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      const body = JSON.parse(init.body);
      releaseBodies.push(body);
      const attempts = (releaseAttemptsByHost.get(body.hostname) ?? 0) + 1;
      releaseAttemptsByHost.set(body.hostname, attempts);

      if (body.hostname === 'a.sharepoint.com' && attempts <= 3) {
        return new Response(JSON.stringify({ error: 'retry later' }), {
          status: 503,
          headers: { 'content-type': 'application/json' },
        });
      }

      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeCalls += 1;
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportCalls += 1;
      return createJsonResponse([]);
    }

    if (url === 'https://a.sharepoint.com/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://b.sharepoint.com/final' },
      });
    }

    if (url === 'https://b.sharepoint.com/final') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-only-release-compensation.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.equal(authorizeCalls, 0);
    assert.equal(reportCalls, 0);
    assert.equal(acquireBodies.length, 2);
    assert.deepEqual(acquireBodies.map((body) => body.hostname), [
      'a.sharepoint.com',
      'b.sharepoint.com',
    ]);
    assert.equal(releaseAttemptsByHost.get('a.sharepoint.com'), 4);
    assert.equal(releaseAttemptsByHost.get('b.sharepoint.com'), 1);
    assert.deepEqual(releaseBodies.slice(0, 3).map((body) => body.hostname), [
      'a.sharepoint.com',
      'a.sharepoint.com',
      'a.sharepoint.com',
    ]);
    assert.deepEqual(releaseBodies.slice(3).map((body) => body.hostname), [
      'b.sharepoint.com',
      'a.sharepoint.com',
    ]);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_only retries release of the dropped managed context in finally after redirecting into an unmanaged target', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const acquireBodies = [];
  const releaseBodies = [];
  const releaseAttemptsByHost = new Map();
  let authorizeCalls = 0;
  let reportCalls = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: [],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://a.sharepoint.com/start',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: 'slot-1',
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      const body = JSON.parse(init.body);
      releaseBodies.push(body);
      const attempts = (releaseAttemptsByHost.get(body.hostname) ?? 0) + 1;
      releaseAttemptsByHost.set(body.hostname, attempts);

      if (body.hostname === 'a.sharepoint.com' && attempts <= 3) {
        return new Response(JSON.stringify({ error: 'retry later' }), {
          status: 503,
          headers: { 'content-type': 'application/json' },
        });
      }

      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeCalls += 1;
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportCalls += 1;
      return createJsonResponse([]);
    }

    if (url === 'https://a.sharepoint.com/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://unmanaged.example.com/final' },
      });
    }

    if (url === 'https://unmanaged.example.com/final') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-only-to-unmanaged-release-compensation.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.equal(authorizeCalls, 0);
    assert.equal(reportCalls, 0);
    assert.equal(acquireBodies.length, 1);
    assert.deepEqual(acquireBodies.map((body) => body.hostname), [
      'a.sharepoint.com',
    ]);
    assert.equal(releaseAttemptsByHost.get('a.sharepoint.com'), 4);
    assert.deepEqual(releaseBodies.map((body) => body.hostname), [
      'a.sharepoint.com',
      'a.sharepoint.com',
      'a.sharepoint.com',
      'a.sharepoint.com',
    ]);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_only final cleanup deduplicates duplicate slot tokens and keeps the first collected context', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const acquireBodies = [];
  const releaseBodies = [];
  const releaseAttemptsByToken = new Map();
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: [],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://a.sharepoint.com/start',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: 'slot-dup',
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      const body = JSON.parse(init.body);
      releaseBodies.push(body);
      const attempts = (releaseAttemptsByToken.get(body.slotToken) ?? 0) + 1;
      releaseAttemptsByToken.set(body.slotToken, attempts);

      if (attempts <= 3) {
        return new Response(JSON.stringify({ error: 'retry later' }), {
          status: 503,
          headers: { 'content-type': 'application/json' },
        });
      }

      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://a.sharepoint.com/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://b.sharepoint.com/final' },
      });
    }

    if (url === 'https://b.sharepoint.com/final') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-only-final-cleanup-dedupe.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.deepEqual(acquireBodies.map((body) => body.hostname), [
      'a.sharepoint.com',
      'b.sharepoint.com',
    ]);
    assert.equal(releaseAttemptsByToken.get('slot-dup'), 4);
    assert.deepEqual(releaseBodies.map((body) => body.hostname), [
      'a.sharepoint.com',
      'a.sharepoint.com',
      'a.sharepoint.com',
      'b.sharepoint.com',
    ]);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_only final cleanup keeps same-host releases serial', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const finalReleaseDeferreds = [];
  const finalReleaseStartTokens = [];
  const siteBucketAlpha = await decodeHostnameHash('sites:a');
  const siteBucketBeta = await decodeHostnameHash('sites:b');
  const siteBucketUnknown = await decodeHostnameHash('unknown');
  const inlineFailureTokens = new Set(['slot-alpha', 'slot-beta']);
  const releaseAttemptsByToken = new Map();
  let currentHostInFlight = 0;
  let maxHostInFlight = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: [],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/a/start',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      const body = JSON.parse(init.body);
      const tokenBySiteBucket = {
        [siteBucketAlpha]: 'slot-alpha',
        [siteBucketBeta]: 'slot-beta',
        [siteBucketUnknown]: 'slot-final',
      };
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: tokenBySiteBucket[body.siteBucket],
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      const body = JSON.parse(init.body);
      const attempts = (releaseAttemptsByToken.get(body.slotToken) ?? 0) + 1;
      releaseAttemptsByToken.set(body.slotToken, attempts);

      if (inlineFailureTokens.has(body.slotToken) && attempts <= 3) {
        return new Response(JSON.stringify({ error: 'retry later' }), {
          status: 503,
          headers: { 'content-type': 'application/json' },
        });
      }

      currentHostInFlight += 1;
      maxHostInFlight = Math.max(maxHostInFlight, currentHostInFlight);
      finalReleaseStartTokens.push(body.slotToken);

      const deferred = createDeferred();
      finalReleaseDeferreds.push(() => {
        currentHostInFlight -= 1;
        deferred.resolve(createJsonResponse({ result: 'ok' }));
      });
      return deferred.promise;
    }

    if (url === 'https://tenant.sharepoint.com/sites/a/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://tenant.sharepoint.com/sites/b/step' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/b/step') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://tenant.sharepoint.com/final' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/final') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-only-final-cleanup-same-host-serial.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    assert.equal(response.status, 200);

    await waitForCondition(() => finalReleaseStartTokens.length === 1, {
      timeoutMs: 300,
      message: 'expected first same-host final cleanup release to start',
    });
    await sleep(30);
    assert.deepEqual(finalReleaseStartTokens, ['slot-final']);
    assert.equal(maxHostInFlight, 1);

    finalReleaseDeferreds.shift()?.();
    await waitForCondition(() => finalReleaseStartTokens.length === 2, {
      timeoutMs: 300,
      message: 'expected second same-host final cleanup release to start after the first completed',
    });
    await sleep(30);
    assert.deepEqual(finalReleaseStartTokens, ['slot-final', 'slot-alpha']);
    assert.equal(maxHostInFlight, 1);

    finalReleaseDeferreds.shift()?.();
    await waitForCondition(() => finalReleaseStartTokens.length === 3, {
      timeoutMs: 300,
      message: 'expected third same-host final cleanup release to stay queued behind the second',
    });
    assert.deepEqual(finalReleaseStartTokens, ['slot-final', 'slot-alpha', 'slot-beta']);
    assert.equal(maxHostInFlight, 1);

    finalReleaseDeferreds.shift()?.();
    await Promise.allSettled(waitUntilPromises);
  } finally {
    while (finalReleaseDeferreds.length > 0) {
      finalReleaseDeferreds.shift()?.();
    }
    await Promise.allSettled(waitUntilPromises);
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_only final cleanup overlaps different hosts but caps global concurrency at 2', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const inlineFailureTokens = new Set(['slot-a', 'slot-b', 'slot-c']);
  const releaseAttemptsByToken = new Map();
  const finalReleaseStartHosts = [];
  const finalReleaseDeferreds = [];
  let currentGlobalInFlight = 0;
  let maxGlobalInFlight = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: [],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://a.sharepoint.com/start',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      const body = JSON.parse(init.body);
      const tokenByHost = {
        'a.sharepoint.com': 'slot-a',
        'b.sharepoint.com': 'slot-b',
        'c.sharepoint.com': 'slot-c',
        'd.sharepoint.com': 'slot-d',
      };
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: tokenByHost[body.hostname],
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      const body = JSON.parse(init.body);
      const attempts = (releaseAttemptsByToken.get(body.slotToken) ?? 0) + 1;
      releaseAttemptsByToken.set(body.slotToken, attempts);

      if (inlineFailureTokens.has(body.slotToken) && attempts <= 3) {
        return new Response(JSON.stringify({ error: 'retry later' }), {
          status: 503,
          headers: { 'content-type': 'application/json' },
        });
      }

      currentGlobalInFlight += 1;
      maxGlobalInFlight = Math.max(maxGlobalInFlight, currentGlobalInFlight);
      finalReleaseStartHosts.push(body.hostname);

      const deferred = createDeferred();
      finalReleaseDeferreds.push(() => {
        currentGlobalInFlight -= 1;
        deferred.resolve(createJsonResponse({ result: 'ok' }));
      });
      return deferred.promise;
    }

    if (url === 'https://a.sharepoint.com/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://b.sharepoint.com/step' },
      });
    }

    if (url === 'https://b.sharepoint.com/step') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://c.sharepoint.com/more' },
      });
    }

    if (url === 'https://c.sharepoint.com/more') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://d.sharepoint.com/final' },
      });
    }

    if (url === 'https://d.sharepoint.com/final') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/queue-only-final-cleanup-concurrency.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    assert.equal(response.status, 200);

    await waitForCondition(() => finalReleaseStartHosts.length === 2, {
      timeoutMs: 300,
      message: 'expected two different host final cleanup releases to overlap',
    });
    assert.deepEqual(finalReleaseStartHosts, [
      'd.sharepoint.com',
      'a.sharepoint.com',
    ]);
    assert.equal(maxGlobalInFlight, 2);

    await sleep(30);
    assert.equal(finalReleaseStartHosts.length, 2);

    finalReleaseDeferreds.shift()?.();
    await waitForCondition(() => finalReleaseStartHosts.length === 3, {
      timeoutMs: 300,
      message: 'expected queued final cleanup work to start after one host completed',
    });
    assert.deepEqual(finalReleaseStartHosts, [
      'd.sharepoint.com',
      'a.sharepoint.com',
      'b.sharepoint.com',
    ]);
    assert.equal(maxGlobalInFlight, 2);

    finalReleaseDeferreds.shift()?.();
    await waitForCondition(() => finalReleaseStartHosts.length === 4, {
      timeoutMs: 300,
      message: 'expected the fourth host cleanup to remain queued behind the global cap',
    });
    assert.deepEqual(finalReleaseStartHosts, [
      'd.sharepoint.com',
      'a.sharepoint.com',
      'b.sharepoint.com',
      'c.sharepoint.com',
    ]);
    assert.equal(maxGlobalInFlight, 2);

    while (finalReleaseDeferreds.length > 0) {
      finalReleaseDeferreds.shift()?.();
    }
    await Promise.allSettled(waitUntilPromises);
  } finally {
    while (finalReleaseDeferreds.length > 0) {
      finalReleaseDeferreds.shift()?.();
    }
    await Promise.allSettled(waitUntilPromises);
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('unmanaged redirects into queue_only and acquires a slot for the managed target', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const acquireBodies = [];
  const releaseBodies = [];
  let authorizeCalls = 0;
  let reportCalls = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: [],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://unmanaged.example.com/start',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: 'slot-1',
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeCalls += 1;
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportCalls += 1;
      return createJsonResponse([]);
    }

    if (url === 'https://unmanaged.example.com/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://tenant.sharepoint.com/final' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/final') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/unmanaged-to-queue-only-redirect.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.equal(authorizeCalls, 0);
    assert.equal(reportCalls, 0);
    assert.equal(acquireBodies.length, 1);
    assert.equal(acquireBodies[0].hostname, 'tenant.sharepoint.com');
    assert.equal(releaseBodies.length, 1);
    assert.equal(releaseBodies[0].hostname, 'tenant.sharepoint.com');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('breaker_only redirects into queue_breaker and the managed target gets atomic admission', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const authorizeBodies = [];
  const reportBodies = [];
  const acquireBodies = [];
  const releaseBodies = [];
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: ['*.office.com', '*.sharepoint.com'],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://files.office.com/start',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: 'slot-1',
        meta: {
          attemptVersion: 41,
          attemptTicket: 3,
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 22,
        LAST_ERROR_CODE: null,
        ATTEMPT_GRANTED: false,
        ATTEMPT_TICKET: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://files.office.com/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://tenant.sharepoint.com/final' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/final') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/breaker-only-to-queue-breaker-redirect.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.deepEqual(authorizeBodies.map((body) => body.p_hostname), ['files.office.com']);
    assert.equal(acquireBodies.length, 1);
    assert.equal(acquireBodies[0].hostname, 'tenant.sharepoint.com');
    assert.equal(releaseBodies.length, 1);
    assert.equal(releaseBodies[0].hostname, 'tenant.sharepoint.com');
    assert.deepEqual(
      reportBodies.map((body) => ({
        hostname: body.p_hostname,
        statusCode: body.p_status_code,
        attemptVersion: body.p_attempt_version,
        attemptTicket: body.p_attempt_ticket,
      })),
      [
        {
          hostname: 'files.office.com',
          statusCode: 302,
          attemptVersion: null,
          attemptTicket: null,
        },
        {
          hostname: 'tenant.sharepoint.com',
          statusCode: 200,
          attemptVersion: 41,
          attemptTicket: 3,
        },
      ],
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('unmanaged refresh into queue_breaker bootstraps atomic admission for the refreshed host', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const authorizeBodies = [];
  const reportBodies = [];
  const acquireBodies = [];
  const releaseBodies = [];
  let linkFetchCount = 0;
  delete globalThis.bootstrapCache;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: ['*.sharepoint.com'],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      linkFetchCount += 1;
      return createJsonResponse({
        code: 200,
        data: {
          url: linkFetchCount === 1
            ? 'https://unmanaged.example.com/stale'
            : 'https://tenant.sharepoint.com/fresh',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-granted',
        invocationEpoch: 1,
        slotToken: 'slot-1',
        meta: {
          attemptVersion: 51,
          attemptTicket: 4,
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://unmanaged.example.com/stale') {
      return new Response('expired', {
        status: 401,
        headers: { 'content-type': 'text/plain' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/fresh') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/unmanaged-to-queue-breaker-refresh.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.all(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.equal(linkFetchCount, 2);
    assert.deepEqual(authorizeBodies, []);
    assert.equal(acquireBodies.length, 1);
    assert.equal(acquireBodies[0].hostname, 'tenant.sharepoint.com');
    assert.equal(releaseBodies.length, 1);
    assert.equal(releaseBodies[0].hostname, 'tenant.sharepoint.com');
    assert.deepEqual(
      reportBodies.map((body) => ({
        hostname: body.p_hostname,
        statusCode: body.p_status_code,
        attemptVersion: body.p_attempt_version,
        attemptTicket: body.p_attempt_ticket,
      })),
      [
        {
          hostname: 'tenant.sharepoint.com',
          statusCode: 200,
          attemptVersion: 51,
          attemptTicket: 4,
        },
      ],
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('client abort during unmanaged redirect fair-queue bootstrap returns client abort response', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const acquireBodies = [];
  delete globalThis.bootstrapCache;

  const controller = new AbortController();

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        hostPatterns: [],
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://unmanaged.example.com/start',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      setTimeout(() => controller.abort(), 0);
      return await new Promise((_, reject) => {
        const abortError = new Error('Aborted');
        abortError.name = 'AbortError';
        init.signal.addEventListener('abort', () => reject(abortError), { once: true });
      });
    }

    if (url === 'https://unmanaged.example.com/start') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://tenant.sharepoint.com/final' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/final') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const baseRequest = await buildSignedWorkerRequest('/downloads/unmanaged-redirect-abort.bin');
    const request = new Request(baseRequest, { signal: controller.signal });
    const response = await worker.fetch(request, {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.allSettled(waitUntilPromises);

    assert.equal(acquireBodies.length, 1);
    assert.equal(acquireBodies[0].hostname, 'tenant.sharepoint.com');
    assert.equal(response.status, 499);
    assert.deepEqual(await response.json(), {
      code: 499,
      message: 'client aborted request',
    });
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('scheduleAllCleanups routes hard-expiry cleanup through download_cleanup_expired_tickets for custom-pg-rest config', async () => {
  const originalFetch = globalThis.fetch;
  let unexpectedFetchCalls = 0;
  const beforeCleanupNow = Math.floor(Date.now() / 1000);

  globalThis.fetch = async (input) => {
    unexpectedFetchCalls += 1;
    const url = typeof input === 'string' ? input : input.url;
    throw new Error(`Unexpected non-ticket-state cleanup fetch: ${url}`);
  };
  ticketStateRpcState.cleanupHandler = async () => ({ deleted: 3 });

  try {
    await scheduleAllCleanups({
      dbMode: 'custom-pg-rest',
      cacheConfig: {
        postgrestUrl: 'https://postgrest.example.test/',
        verifyHeader: ['X-Verify'],
        verifySecret: ['secret'],
        ticketStateTableName: 'DOWNLOAD_TICKET_STATE_TABLE',
        cleanupProbability: 1,
      },
    }, {}, null);

    assert.equal(unexpectedFetchCalls, 0);
    assert.equal(ticketStateRpcState.cleanupBodies.length, 1);
    assert.deepEqual(Object.keys(ticketStateRpcState.cleanupBodies[0]).sort(), ['p_now', 'p_table_name']);
    assert.equal(ticketStateRpcState.cleanupBodies[0].p_table_name, 'DOWNLOAD_TICKET_STATE_TABLE');
    assert.equal(Number.isInteger(ticketStateRpcState.cleanupBodies[0].p_now), true);
    assert.equal('p_last_active_table_name' in ticketStateRpcState.cleanupBodies[0], false);
    assert.ok(ticketStateRpcState.cleanupBodies[0].p_now >= beforeCleanupNow);
    assert.ok(ticketStateRpcState.cleanupBodies[0].p_now <= Math.floor(Date.now() / 1000));
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('scheduleAllCleanups skips ticket-state cleanup when dbMode is empty', async () => {
  const originalFetch = globalThis.fetch;
  let unexpectedFetchCalls = 0;
  const bootstrap = buildBootstrap();
  bootstrap.download.db = {
    mode: '',
    postgrestUrl: 'https://postgrest.example.test',
    verifyHeader: ['X-Verify'],
    verifySecret: ['secret'],
    ticketStateTable: 'DOWNLOAD_TICKET_STATE_TABLE',
  };

  globalThis.fetch = async (input) => {
    unexpectedFetchCalls += 1;
    const url = typeof input === 'string' ? input : input.url;
    throw new Error(`Unexpected non-ticket-state cleanup fetch: ${url}`);
  };
  ticketStateRpcState.cleanupHandler = async () => ({ deleted: 4 });

  try {
    const config = resolveConfig({}, bootstrap, { download: {} });

    await scheduleAllCleanups({
      ...config,
      cleanupPercentage: 100,
    }, {}, null);

    assert.equal(config.dbMode, '');
    assert.equal(unexpectedFetchCalls, 0);
    assert.equal(ticketStateRpcState.cleanupBodies.length, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('download_cleanup_expired_tickets hard-expiry contract deletes by HARD_EXPIRE_AT', async () => {
  const initSql = await readFile(new URL('../init.sql', import.meta.url), 'utf8');

  assert.match(initSql, /CREATE OR REPLACE FUNCTION download_cleanup_expired_tickets\(/i);
  assert.match(initSql, /DELETE FROM %1\$I WHERE "HARD_EXPIRE_AT" < \$1/i);
});
