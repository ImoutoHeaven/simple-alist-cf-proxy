import { test } from 'node:test';
import assert from 'node:assert/strict';
import worker, { __fairQueueTestHooks } from '../src/worker.js';
import { encryptBindingPayload } from '../src/origin-binding.js';
import { sha256Hash } from '../src/utils.js';

const {
  buildFinalCleanupGroups,
  clearOverloadedByHost,
  createConcurrencyReleaseController,
  createSlotHandlerClient,
  createTrueConcurrencyHeartbeatManager,
  normalizeTrueConcurrencyReleaseReason,
} = __fairQueueTestHooks;

const createJsonResponse = (payload, init = {}) => new Response(JSON.stringify(payload), {
  status: init.status ?? 200,
  headers: {
    'content-type': 'application/json',
    ...(init.headers || {}),
  },
});

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

const buildHeartbeatHelloAck = (overrides = {}) => ({
  type: 'hello_ack',
  generation: 7,
  deadlineMs: Date.now() + 15_000,
  ackTimeoutMs: 2000,
  heartbeatIntervalMs: 5000,
  heartbeatTimeoutMs: 15000,
  reconnectGraceMs: 12000,
  startTimeoutMs: 7000,
  hardExpireAtMs: Date.now() + 60_000,
  ...overrides,
});

const buildHeartbeatAck = (generation = 7, overrides = {}) => ({
  type: 'heartbeat_ack',
  generation,
  deadlineMs: Date.now() + 15_000,
  hardExpireAtMs: Date.now() + 60_000,
  ...overrides,
});

const createFakeHeartbeatSocket = ({
  helloAck = buildHeartbeatHelloAck(),
  heartbeatAck = buildHeartbeatAck(),
  onSend = null,
} = {}) => {
  const listeners = new Map();
  let closeArgs = null;

  const emit = (type, event = {}) => {
    const handlers = listeners.get(type);
    if (!handlers) {
      return;
    }
    for (const handler of [...handlers]) {
      handler(event);
    }
  };

  const emitJsonMessage = (payload) => {
    queueMicrotask(() => emit('message', { data: JSON.stringify(payload) }));
  };

  const socket = {
    accepted: false,
    sent: [],
    addEventListener(type, handler) {
      const handlers = listeners.get(type) || new Set();
      handlers.add(handler);
      listeners.set(type, handlers);
    },
    removeEventListener(type, handler) {
      listeners.get(type)?.delete(handler);
    },
    accept() {
      this.accepted = true;
    },
    send(data) {
      this.sent.push(data);
      const payload = JSON.parse(data);
      onSend?.(payload, { emit, emitJsonMessage, socket: this });
      if (payload.type === 'hello') {
        if (helloAck) {
          emitJsonMessage(typeof helloAck === 'function' ? helloAck(payload) : helloAck);
        }
        return;
      }
      if (payload.type === 'heartbeat') {
        if (heartbeatAck) {
          emitJsonMessage(typeof heartbeatAck === 'function' ? heartbeatAck(payload) : heartbeatAck);
        }
        return;
      }
      throw new Error(`unexpected socket payload type: ${payload.type}`);
    },
    close(code = 1000, reason = '') {
      closeArgs = { code, reason };
      queueMicrotask(() => emit('close', { code, reason }));
    },
    emitClose(code = 1000, reason = '') {
      closeArgs = { code, reason };
      queueMicrotask(() => emit('close', { code, reason }));
    },
    emitError(error = new Error('heartbeat socket error')) {
      queueMicrotask(() => emit('error', { error }));
    },
    emitJsonMessage,
    getCloseArgs() {
      return closeArgs;
    },
  };

  return socket;
};

const createHeartbeatManagerSession = ({
  socket = createFakeHeartbeatSocket(),
  generation = 1,
  heartbeatIntervalMs = 1000,
  reconnectGraceMs = 12000,
  hardExpireAtMs = Date.now() + 60_000,
} = {}) => {
  let closeReason = null;
  return {
    socket,
    session: {
      ws: socket,
      generation,
      deadlineMs: Date.now() + 15_000,
      ackTimeoutMs: 2000,
      heartbeatIntervalMs,
      heartbeatTimeoutMs: 15_000,
      reconnectGraceMs,
      startTimeoutMs: 7000,
      hardExpireAtMs,
      close(reason = '') {
        closeReason = reason;
      },
      async sendHeartbeat() {
        return buildHeartbeatAck(generation, {
          deadlineMs: Date.now() + 15_000,
          hardExpireAtMs,
        });
      },
    },
    getCloseReason() {
      return closeReason;
    },
  };
};

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
    idle_timeout_seconds: 300,
    idle_policy: 'first_use',
    idle_lease_expires_at: nowSeconds + 300,
    idle_renew_owner_lease_id: null,
    idle_renew_owner_last_heartbeat_at: null,
    ip_hash: null,
    path_hash: null,
    ...overrides,
  };
};

const respondWithJson = (payload, init = {}) => createJsonResponse(payload, init);

const handleTicketStateRpc = async (url, init = {}) => {
  if (url === TICKET_STATE_READ_URL) {
    const body = JSON.parse(init.body);
    ticketStateRpcState.readBodies.push(body);
    if (typeof ticketStateRpcState.readHandler === 'function') {
      const response = await ticketStateRpcState.readHandler(body, init);
      if (response instanceof Response) {
        return response;
      }
      return respondWithJson(Array.isArray(response) ? response : [response]);
    }
    return respondWithJson([createDefaultTicketStateRow(body)]);
  }

  if (url === TICKET_STATE_MARK_URL) {
    const body = JSON.parse(init.body);
    ticketStateRpcState.markBodies.push(body);
    if (typeof ticketStateRpcState.markHandler === 'function') {
      const response = await ticketStateRpcState.markHandler(body, init);
      if (response instanceof Response) {
        return response;
      }
      return respondWithJson(response);
    }
    return respondWithJson({
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
      return respondWithJson(response);
    }
    return respondWithJson({ deleted: 0 });
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

const buildRuntimeBootstrap = ({
  dbMode = 'custom-pg-rest',
  includeTicketStateWiring = dbMode === 'custom-pg-rest',
  fairQueueHostPatterns = [],
  fairQueueSiteBucket = undefined,
  trueConcurrencyHostPatterns = [],
  trueConcurrencyHeartbeatOverrides = null,
  trueConcurrencySiteBucket = undefined,
  throttleHostPatterns = [],
} = {}) => ({
  configVersion: 'task-group-4-runtime',
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
      mode: dbMode,
      ...(includeTicketStateWiring ? {
        postgrestUrl: 'https://postgrest.example.test',
        verifyHeader: ['X-Verify'],
        verifySecret: ['secret'],
        cacheEnabled: false,
      } : {}),
      cleanupPercentage: 0,
    },
    throttleProfiles: {
      default: {
        hostPatterns: throttleHostPatterns,
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
    ...(fairQueueHostPatterns.length > 0 ? {
      fairQueue: {
        enabled: true,
        hostPatterns: fairQueueHostPatterns,
        slotHandlerUrl: 'https://slot-handler.example.test',
        slotHandlerAuthKey: 'slot-secret',
        slotHandlerAuthHeader: 'X-FQ-Auth',
        ...(fairQueueSiteBucket !== undefined ? { siteBucket: fairQueueSiteBucket } : {}),
      },
    } : {}),
    ...(trueConcurrencyHostPatterns.length > 0 ? {
      trueConcurrency: {
        enabled: true,
        hostPatterns: trueConcurrencyHostPatterns,
        handlerUrl: 'https://cq.example.test',
        handlerAuthKey: 'cq-secret',
        heartbeat: {
          ...DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
          ...(trueConcurrencyHeartbeatOverrides || {}),
        },
        ...(trueConcurrencySiteBucket !== undefined ? { siteBucket: trueConcurrencySiteBucket } : {}),
      },
    } : {}),
  },
});

const buildWorkerEnv = () => ({
  CONTROLLER_URL: 'https://controller.example.test',
  CONTROLLER_API_TOKEN: 'controller-token',
  ENV: 'test',
  ROLE: 'download',
  INSTANCE_ID: 'worker-1',
  BOOTSTRAP_CACHE_MODE: 'direct',
});

const buildSignedWorkerRequest = async ({
  pathname = '/downloads/task-group-4.bin',
  expireOffsetSeconds = 300,
  idleTimeoutSeconds = DEFAULT_IDLE_TIMEOUT_SECONDS,
  omitIdleTimeout = false,
  omitTicketNonce = false,
  payloadExpireTime = null,
  payloadSignExpire = null,
  payloadFileSize = undefined,
  payloadMutator = null,
  signal = undefined,
  ticketNonce = createTicketNonce(),
} = {}) => {
  const token = 'bootstrap-token';
  const expire = payloadSignExpire ?? (Math.floor(Date.now() / 1000) + expireOffsetSeconds);
  const encryptedBinding = await encryptBindingPayload({
    v: 2,
    issuer: 'https://landing.example.com',
    workerAddress: 'https://worker.example.com',
  }, token);
  const payloadObject = {
    v: 1,
    expireTime: payloadExpireTime ?? expire,
    ...(omitIdleTimeout ? {} : { idle_timeout: idleTimeoutSeconds }),
    ...(omitTicketNonce ? {} : { ticketNonce }),
    encrypt: encryptedBinding,
    ...(payloadFileSize !== undefined ? { filesize: payloadFileSize } : {}),
  };
  const finalPayloadObject = typeof payloadMutator === 'function'
    ? payloadMutator(structuredClone(payloadObject))
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
    signal,
  });
};

const readJson = async (response) => JSON.parse(await response.text());

test('worker normalizes CQ release reasons to the canonical contract', () => {
  const scenarios = new Map([
    ['stream_complete', 'stream_complete'],
    ['client_disconnect', 'client_disconnect'],
    ['hard_expiry', 'hard_expiry'],
    ['upstream_failure', 'upstream_failure'],
    ['origin_fetch_failure', 'origin_fetch_failure'],
    ['heartbeat_connect_failed', 'heartbeat_connect_failed'],
    ['heartbeat_lost', 'heartbeat_lost'],
    ['final_cleanup', 'final_cleanup'],
    ['target_change', 'final_cleanup'],
    ['grant_delivery_failed', 'final_cleanup'],
    ['acquire_delivery_failed', 'final_cleanup'],
    ['head_probe_complete', 'stream_complete'],
    ['head_probe_invalid', 'origin_fetch_failure'],
    ['prestream_terminal', 'origin_fetch_failure'],
    ['google_drive_range_mismatch', 'origin_fetch_failure'],
  ]);

  for (const [input, expected] of scenarios) {
    assert.equal(normalizeTrueConcurrencyReleaseReason(input), expected, input);
  }
});

test('signed worker request fixtures include ticketNonce and idle_timeout', async () => {
  assertSignedRequestPayloadContract(await buildSignedWorkerRequest());
});

const createClaimGrantResponseFromRequest = (init) => {
  const body = JSON.parse(init.body);
  const suffix = body.claimToken.replace(/^claim-token-?/, '') || '1';
  const expiresAtMs = Date.now() + 60_000;
  return createJsonResponse({
    result: 'granted',
    leaseId: `lease-${suffix}`,
    leaseToken: `token-${suffix}`,
    expiresAtMs,
    handoffToken: `handoff-${suffix}`,
    handoffDeadlineMs: expiresAtMs - 1_000,
  });
};

const createAckHandoffResponse = (payload) => {
  const status = payload?.result === 'acknowledged'
    ? 200
    : payload?.result === 'conflict'
      ? 409
      : 410;
  return createJsonResponse(payload, { status });
};

const ACK_HANDOFF_URL = 'https://cq.example.test/api/v1/concurrency/ack_handoff';
const wrappedFetch = globalThis.fetch;
const wrappedFetchBound = typeof wrappedFetch === 'function' ? wrappedFetch.bind(globalThis) : wrappedFetch;
let delegatedFetch = wrappedFetchBound;
let ackHandoffMock = null;
let heartbeatMock = null;

const setAckHandoffMock = (handler = null) => {
  ackHandoffMock = typeof handler === 'function' ? handler : null;
};

const setHeartbeatMock = (handler = null) => {
  heartbeatMock = typeof handler === 'function' ? handler : null;
};

const fetchWithDefaultAckHandoff = async (input, init = {}) => {
  const url = typeof input === 'string' ? input : input.url;
  const ticketStateResponse = await handleTicketStateRpc(url, init);
  if (ticketStateResponse) {
    return ticketStateResponse;
  }
  if (url === ACK_HANDOFF_URL) {
    if (ackHandoffMock) {
      return ackHandoffMock(input, init);
    }
    if (typeof delegatedFetch === 'function') {
      try {
        return await delegatedFetch(input, init);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        if (!message.includes(`Unexpected fetch URL in test: ${ACK_HANDOFF_URL}`)) {
          throw error;
        }
      }
    }
    return createAckHandoffResponse({ result: 'acknowledged' });
  }
  if (url === HEARTBEAT_URL) {
    if (heartbeatMock) {
      return heartbeatMock(input, init);
    }
    if (typeof delegatedFetch === 'function') {
      try {
        return await delegatedFetch(input, init);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        if (!message.includes(`Unexpected fetch URL in test: ${HEARTBEAT_URL}`)) {
          throw error;
        }
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
    return fetchWithDefaultAckHandoff;
  },
  set(value) {
    ackHandoffMock = null;
    heartbeatMock = null;
    resetTicketStateRpcState();
    if (value === fetchWithDefaultAckHandoff) {
      delegatedFetch = wrappedFetchBound;
      return;
    }
    delegatedFetch = value;
  },
});

const createTestContext = () => {
  const waitUntilPromises = [];
  return {
    ctx: {
      waitUntil(promise) {
        waitUntilPromises.push(Promise.resolve(promise));
      },
    },
    waitUntilPromises,
  };
};

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
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  if (predicate()) {
    return;
  }
  throw new Error(message);
};

const createTrackedTextBody = (text) => {
  const encoded = new TextEncoder().encode(text);
  let pulled = false;
  let cancelled = false;

  return {
    stream: new ReadableStream({
      pull(controller) {
        if (!pulled) {
          controller.enqueue(encoded);
          pulled = true;
        }
        controller.close();
      },
      cancel() {
        cancelled = true;
      },
    }),
    get cancelled() {
      return cancelled;
    },
  };
};

const GOOGLE_DRIVE_HOST_PATTERNS = [
  'drive.google.com',
  '*.googleapis.com',
  '*.googleusercontent.com',
  'drive.usercontent.google.com',
];

const hashSiteKey = async (siteKey) => sha256Hash(siteKey);

const captureAdmissionPayloads = async ({
  targetUrl,
  fairQueueHostPatterns = [],
  fairQueueSiteBucket = undefined,
  trueConcurrencyHostPatterns = [],
  trueConcurrencySiteBucket = undefined,
}) => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  let fairQueueAcquireBody = null;
  let fairQueueReleaseBody = null;
  let concurrencyAcquireBody = null;
  let concurrencyReleaseBody = null;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns,
        fairQueueSiteBucket,
        trueConcurrencyHostPatterns,
        trueConcurrencySiteBucket,
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: targetUrl,
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      fairQueueAcquireBody = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-sitebucket',
        invocationEpoch: 1,
        slotToken: 'slot-sitebucket',
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      fairQueueReleaseBody = JSON.parse(init.body);
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      concurrencyAcquireBody = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-sitebucket',
        leaseToken: 'token-sitebucket',
        expiresAtMs: concurrencyAcquireBody.hardExpireAtMs,
        claimToken: 'claim-token-sitebucket',
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      concurrencyReleaseBody = JSON.parse(init.body);
      return createJsonResponse({ result: 'released' });
    }

    if (url === targetUrl) {
      calls.push('origin-fetch');
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const bodyText = await response.text();
    await Promise.allSettled(waitUntilPromises);
    return {
      bodyText,
      calls,
      concurrencyAcquireBody,
      concurrencyReleaseBody,
      fairQueueAcquireBody,
      fairQueueReleaseBody,
      status: response.status,
    };
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
};

test('dual mode performs fairqueue acquire before concurrency acquire and origin fetch', async () => {
  const originalFetch = globalThis.fetch;
  const originalRandomUUID = crypto.randomUUID;
  const calls = [];

  crypto.randomUUID = () => 'req-dual-1';
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-1',
        invocationEpoch: 1,
        slotToken: 'slot-1',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      assert.equal(body.requestId, 'req-dual-1');
      assert.equal(body.hostname, 'tenant.sharepoint.com');
      assert.equal(typeof body.hardExpireAtMs, 'number');
      assert.equal(typeof body.hostnameHash, 'string');
      assert.equal(typeof body.siteBucket, 'string');
      assert.equal(typeof body.ipBucket, 'string');
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-1',
        leaseToken: 'token-1',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-1',
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('ok', {
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
        VERSION: 1,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      return createJsonResponse({ result: 'ok' });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);

    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'ok');
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(calls.slice(0, 4), [
      'fairqueue-acquire',
      'concurrency-acquire',
      'concurrency-claim',
      'origin-fetch',
    ]);
  } finally {
    globalThis.fetch = originalFetch;
    crypto.randomUUID = originalRandomUUID;
    delete globalThis.bootstrapCache;
  }
});

test('dual mode fast terminal CQ hard expiry releases fairqueue and returns link expired', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-fast-terminal',
        invocationEpoch: 1,
        slotToken: 'slot-fast-terminal',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      return new Response(JSON.stringify({ result: 'expired', reason: 'hard_expired' }), {
        status: 410,
        headers: { 'content-type': 'application/json' },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      return createJsonResponse({ result: 'ok' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), createTestContext().ctx);
    const body = await readJson(response);
    assert.equal(response.status, 401);
    assert.equal(body.message, 'link expired');
    assert.deepEqual(calls, ['fairqueue-acquire', 'concurrency-acquire', 'fairqueue-release']);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('dual mode fast expired CQ result returns link expired after releasing fairqueue', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-expired-terminal',
        invocationEpoch: 1,
        slotToken: 'slot-expired-terminal',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      return new Response(JSON.stringify({ result: 'expired', reason: 'hard_expired' }), {
        status: 410,
        headers: { 'content-type': 'application/json' },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      return createJsonResponse({ result: 'ok' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);

    assert.equal(response.status, 401);
    const body = await readJson(response);
    assert.equal(body.message, 'link expired');
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(calls, [
      'fairqueue-acquire',
      'concurrency-acquire',
      'fairqueue-release',
    ]);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('dual mode proceeds without precheck and still completes fairqueue then concurrency admission', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-malformed-precheck',
        invocationEpoch: 1,
        slotToken: 'slot-malformed-precheck',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-malformed-precheck',
        leaseToken: 'token-malformed-precheck',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-malformed-precheck',
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('ok', {
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
        VERSION: 1,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      return createJsonResponse({ result: 'ok' });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'ok');
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(calls.slice(0, 4), [
      'fairqueue-acquire',
      'concurrency-acquire',
      'concurrency-claim',
      'origin-fetch',
    ]);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('dual mode fast terminal CQ hard expiry returns link expired after waiting for cleanup tasks', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-1',
        invocationEpoch: 1,
        slotToken: 'slot-1',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      return new Response(JSON.stringify({ result: 'expired', reason: 'hard_expired' }), {
        status: 410,
        headers: { 'content-type': 'application/json' },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      return createJsonResponse({ result: 'ok' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    await Promise.allSettled(waitUntilPromises);
    const body = await readJson(response);
    assert.equal(response.status, 401);
    assert.equal(body.message, 'link expired');
    assert.deepEqual(calls, ['fairqueue-acquire', 'concurrency-acquire', 'fairqueue-release']);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker dual mode settles breaker attempt before returning link expired on CQ hard expiry', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const reportBodies = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-qb-cq-deny',
        invocationEpoch: 1,
        slotToken: 'slot-qb-cq-deny',
        meta: {
          attemptVersion: 17,
          attemptTicket: 3,
        },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      return new Response(JSON.stringify({ result: 'expired', reason: 'hard_expired' }), {
        status: 410,
        headers: { 'content-type': 'application/json' },
      });
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      calls.push('breaker-settle');
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 17,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      return createJsonResponse({ result: 'ok' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(response.status, 401);
    assert.equal(body.message, 'link expired');
    assert.deepEqual(calls, [
      'fairqueue-acquire',
      'concurrency-acquire',
      'breaker-settle',
      'fairqueue-release',
    ]);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].p_attempt_version, 17);
    assert.equal(reportBodies[0].p_attempt_ticket, 3);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('breaker_only with true concurrency authorizes breaker only after CQ claim and ack_handoff', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  let ackBody = null;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
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
      calls.push('breaker-snapshot');
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 10,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.push('breaker-authorize');
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 10,
        LAST_ERROR_CODE: null,
        ATTEMPT_GRANTED: false,
        ATTEMPT_TICKET: null,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-breaker-only',
        leaseToken: 'token-breaker-only',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-breaker-only',
      });
    }

    if (url === ACK_HANDOFF_URL) {
      calls.push('concurrency-ack-handoff');
      ackBody = JSON.parse(init.body);
      return createAckHandoffResponse({ result: 'acknowledged' });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('ok', {
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
        VERSION: 11,
        LAST_ERROR_CODE: null,
      }]);
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'ok');
    await Promise.allSettled(waitUntilPromises);
    const acquireIndex = calls.indexOf('concurrency-acquire');
    const claimIndex = calls.indexOf('concurrency-claim');
    const ackIndex = calls.indexOf('concurrency-ack-handoff');
    const authorizeIndex = calls.indexOf('breaker-authorize');
    const originIndex = calls.indexOf('origin-fetch');
    assert.ok(acquireIndex >= 0);
    assert.ok(claimIndex > acquireIndex);
    assert.ok(ackIndex > claimIndex);
    assert.ok(authorizeIndex > ackIndex);
    assert.ok(originIndex > authorizeIndex);
    assert.equal(ackBody?.handoffToken, 'handoff-breaker-only');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('breaker_only with true concurrency does not authorize or settle before CQ deny', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const settleBodies = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
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
      calls.push('breaker-snapshot');
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 21,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.push('breaker-authorize');
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 22,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 4,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      return createJsonResponse({ result: 'deny', scope: 'host', reason: 'full', retryAfter: 4 });
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      calls.push('breaker-settle');
      settleBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 22,
        LAST_ERROR_CODE: 429,
      }]);
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), createTestContext().ctx);
    assert.equal(response.status, 503);
    assert.deepEqual(calls, ['breaker-snapshot', 'concurrency-acquire']);
    assert.equal(settleBodies.length, 0);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('breaker_only with true concurrency authorizes after ack_handoff and settles granted attempts across auth refresh retries', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const settleBodies = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      calls.push('link-api');
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/file',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      calls.push('breaker-snapshot');
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 31,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.push('breaker-authorize');
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 32,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 7,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-breaker-refresh',
        leaseToken: 'token-breaker-refresh',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-breaker-refresh',
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch-401');
      return new Response('expired', {
        status: 401,
        headers: { 'content-type': 'text/plain' },
      });
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      calls.push('breaker-settle');
      settleBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 32,
        LAST_ERROR_CODE: 429,
      }]);
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === ACK_HANDOFF_URL) {
      calls.push('concurrency-ack-handoff');
      return createAckHandoffResponse({ result: 'acknowledged' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    assert.equal(response.status, 401);
    assert.equal(body.code, 401);
    assert.equal(typeof body.message, 'string');
    assert.notEqual(body.message, 'expired');
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(calls, [
      'link-api',
      'breaker-snapshot',
      'concurrency-acquire',
      'concurrency-claim',
      'concurrency-ack-handoff',
      'breaker-authorize',
      'origin-fetch-401',
      'breaker-settle',
      'link-api',
      'breaker-authorize',
      'origin-fetch-401',
      'breaker-settle',
      'concurrency-release',
    ]);
    assert.equal(settleBodies.length, 2);
    assert.equal(settleBodies[0].p_attempt_version, 32);
    assert.equal(settleBodies[0].p_attempt_ticket, 7);
    assert.equal(settleBodies[1].p_attempt_version, 32);
    assert.equal(settleBodies[1].p_attempt_ticket, 7);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('breaker_only with true concurrency reports protected auth refresh failures once without settling the refreshed attempt', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const settleBodies = [];
  const reportBodies = [];
  let linkFetchCount = 0;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      const bootstrap = buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
      });
      bootstrap.download.throttleProfiles.default.protectHttpCodes = [401, 410, 429, 499, 500, 502, 503, 504];
      return createJsonResponse(bootstrap);
    }

    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      linkFetchCount += 1;
      calls.push('link-api');
      return createJsonResponse({
        code: 200,
        data: {
          url: linkFetchCount === 1
            ? 'https://tenant.sharepoint.com/protected-refresh-start'
            : 'https://tenant.sharepoint.com/protected-refresh-final',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      calls.push('breaker-snapshot');
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 51,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.push('breaker-authorize');
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 52,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 8,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-breaker-protected-refresh',
        leaseToken: 'token-breaker-protected-refresh',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-breaker-protected-refresh',
      });
    }

    if (url === 'https://tenant.sharepoint.com/protected-refresh-start') {
      calls.push('origin-fetch-401-start');
      return new Response('expired', {
        status: 401,
        headers: { 'content-type': 'text/plain' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/protected-refresh-final') {
      calls.push('origin-fetch-401-final');
      return new Response('expired-final', {
        status: 401,
        headers: { 'content-type': 'text/plain' },
      });
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      calls.push('breaker-settle');
      settleBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 52,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      calls.push('breaker-report');
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: reportBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === ACK_HANDOFF_URL) {
      calls.push('concurrency-ack-handoff');
      return createAckHandoffResponse({ result: 'acknowledged' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 401);
    assert.equal(body.code, 401);
    assert.equal(typeof body.message, 'string');
    assert.notEqual(body.message, 'expired-final');
    assert.equal(linkFetchCount, 2);
    assert.ok(calls.indexOf('origin-fetch-401-start') >= 0);
    assert.ok(calls.indexOf('origin-fetch-401-final') > calls.indexOf('origin-fetch-401-start'));
    assert.ok(calls.indexOf('breaker-report') > calls.indexOf('origin-fetch-401-final'));
    assert.ok(calls.includes('concurrency-release'));
    assert.equal(settleBodies.length, 1);
    assert.equal(settleBodies[0].p_attempt_version, 52);
    assert.equal(settleBodies[0].p_attempt_ticket, 8);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].p_sample, 1);
    assert.equal(reportBodies[0].p_status_code, 401);
    assert.equal(reportBodies[0].p_attempt_version, 52);
    assert.equal(reportBodies[0].p_attempt_ticket, 8);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('breaker_only with true concurrency returns authority unavailable when no-sample settle fails during refresh retirement', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const originBody = createTrackedTextBody('expired');

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      calls.push('link-api');
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/file',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      calls.push('breaker-snapshot');
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 41,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.push('breaker-authorize');
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 42,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 8,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-breaker-refresh-settle-fail',
        leaseToken: 'token-breaker-refresh-settle-fail',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-breaker-refresh-settle-fail',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === ACK_HANDOFF_URL) {
      calls.push('concurrency-ack-handoff');
      return createAckHandoffResponse({ result: 'acknowledged' });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch-401');
      return new Response(originBody.stream, {
        status: 401,
        headers: { 'content-type': 'text/plain' },
      });
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      calls.push('breaker-settle');
      throw new Error('settle unavailable');
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(response.status, 503);
    assert.match(body.message, /attempt settlement/i);
    assert.deepEqual(calls, [
      'link-api',
      'breaker-snapshot',
      'concurrency-acquire',
      'concurrency-claim',
      'concurrency-ack-handoff',
      'breaker-authorize',
      'origin-fetch-401',
      'breaker-settle',
      'concurrency-release',
    ]);
    assert.equal(originBody.cancelled, true);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('breaker_only with true concurrency returns generated JSON for protected terminal upstream responses after sample reporting', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const reportBodies = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      calls.push('link-api');
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/file',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      calls.push('breaker-snapshot');
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 111,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.push('breaker-authorize');
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 112,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 15,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-protected-terminal',
        leaseToken: 'token-protected-terminal',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-protected-terminal',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === ACK_HANDOFF_URL) {
      calls.push('concurrency-ack-handoff');
      return createAckHandoffResponse({ result: 'acknowledged' });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch-500');
      return new Response('boom', {
        status: 500,
        headers: { 'content-type': 'text/plain' },
      });
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      calls.push('breaker-report');
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'open',
        OPEN_UNTIL: Math.floor(Date.now() / 1000) + 30,
        OPEN_REASON: 'http_500',
        VERSION: 113,
        LAST_ERROR_CODE: 500,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      throw new Error('protected terminal CQ path should not settle breaker attempt debt');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 500);
    assert.equal(body.code, 500);
    assert.equal(typeof body.message, 'string');
    assert.notEqual(body.message, 'boom');
    assert.deepEqual(calls, [
      'link-api',
      'breaker-snapshot',
      'concurrency-acquire',
      'concurrency-claim',
      'concurrency-ack-handoff',
      'breaker-authorize',
      'origin-fetch-500',
      'breaker-report',
      'concurrency-release',
    ]);
    assert.deepEqual(
      reportBodies.map((reportBody) => ({
        sample: reportBody.p_sample,
        statusCode: reportBody.p_status_code,
        attemptVersion: reportBody.p_attempt_version,
        attemptTicket: reportBody.p_attempt_ticket,
      })),
      [{ sample: 1, statusCode: 500, attemptVersion: 112, attemptTicket: 15 }],
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('breaker_only with true concurrency returns generated JSON for non-protected terminal upstream responses after settling breaker attempt debt', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const settleBodies = [];
  const originBody = createTrackedTextBody('forbidden');

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      calls.push('link-api');
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/file',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      calls.push('breaker-snapshot');
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 131,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.push('breaker-authorize');
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 132,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 17,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-non-protected-terminal',
        leaseToken: 'token-non-protected-terminal',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-non-protected-terminal',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === ACK_HANDOFF_URL) {
      calls.push('concurrency-ack-handoff');
      return createAckHandoffResponse({ result: 'acknowledged' });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch-403');
      return new Response(originBody.stream, {
        status: 403,
        headers: { 'content-type': 'text/plain' },
      });
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      calls.push('breaker-settle');
      settleBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 132,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      throw new Error('non-protected terminal CQ path should not report breaker samples');
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 403);
    assert.equal(body.code, 403);
    assert.equal(typeof body.message, 'string');
    assert.notEqual(body.message, 'forbidden');
    assert.deepEqual(calls, [
      'link-api',
      'breaker-snapshot',
      'concurrency-acquire',
      'concurrency-claim',
      'concurrency-ack-handoff',
      'breaker-authorize',
      'origin-fetch-403',
      'breaker-settle',
      'concurrency-release',
    ]);
    assert.equal(settleBodies.length, 1);
    assert.equal(settleBodies[0].p_attempt_version, 132);
    assert.equal(settleBodies[0].p_attempt_ticket, 17);
    assert.equal(originBody.cancelled, true);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('breaker_only with true concurrency fails closed and cancels the upstream body when protected sample reporting fails', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const reportBodies = [];
  const originBody = createTrackedTextBody('boom');

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      calls.push('link-api');
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/file',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      calls.push('breaker-snapshot');
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 121,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.push('breaker-authorize');
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 122,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 16,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-protected-terminal-report-fail',
        leaseToken: 'token-protected-terminal-report-fail',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-protected-terminal-report-fail',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === ACK_HANDOFF_URL) {
      calls.push('concurrency-ack-handoff');
      return createAckHandoffResponse({ result: 'acknowledged' });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch-500');
      return new Response(originBody.stream, {
        status: 500,
        headers: { 'content-type': 'text/plain' },
      });
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      calls.push('breaker-report');
      reportBodies.push(JSON.parse(init.body));
      throw new Error('report unavailable');
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      throw new Error('protected terminal CQ report failure path should not settle breaker attempt debt');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.match(body.message, /sample report/i);
    assert.deepEqual(calls, [
      'link-api',
      'breaker-snapshot',
      'concurrency-acquire',
      'concurrency-claim',
      'concurrency-ack-handoff',
      'breaker-authorize',
      'origin-fetch-500',
      'breaker-report',
      'concurrency-release',
    ]);
    assert.deepEqual(
      reportBodies.map((reportBody) => ({
        sample: reportBody.p_sample,
        statusCode: reportBody.p_status_code,
        attemptVersion: reportBody.p_attempt_version,
        attemptTicket: reportBody.p_attempt_ticket,
      })),
      [{ sample: 1, statusCode: 500, attemptVersion: 122, attemptTicket: 16 }],
    );
    assert.equal(originBody.cancelled, true);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('breaker_only with true concurrency checks handler readiness before authorizing breaker attempts', async () => {
  const originalFetch = globalThis.fetch;
  const originalDateNow = Date.now;
  const calls = [];
  let advancedClock = false;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      advancedClock = true;
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/file',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      calls.push('breaker-snapshot');
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 41,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.push('breaker-authorize');
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 42,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 8,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      throw new Error('concurrency acquire should not be reached after hard expiry');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const baseNowMs = Date.now();
    Date.now = () => (advancedClock ? (baseNowMs + 5000) : baseNowMs);

    const expireSeconds = Math.floor((baseNowMs + 2500) / 1000);
    const request = await buildSignedWorkerRequest({
      payloadExpireTime: expireSeconds,
      payloadSignExpire: expireSeconds,
    });
    const response = await worker.fetch(request, buildWorkerEnv(), createTestContext().ctx);
    assert.equal(response.status, 401);
    assert.deepEqual(calls, ['breaker-snapshot']);
  } finally {
    Date.now = originalDateNow;
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('breaker_only with true concurrency does not authorize or settle on client-aborted CQ acquire', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const settleBodies = [];
  const abortController = new AbortController();

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
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
      calls.push('breaker-snapshot');
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 51,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.push('breaker-authorize');
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 52,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 9,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      abortController.abort();
      throw new DOMException('The operation was aborted.', 'AbortError');
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      calls.push('breaker-settle');
      settleBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 52,
        LAST_ERROR_CODE: 429,
      }]);
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const request = await buildSignedWorkerRequest({ signal: abortController.signal });
    const response = await worker.fetch(request, buildWorkerEnv(), createTestContext().ctx);
    assert.equal(response.status, 499);
    assert.deepEqual(calls, ['breaker-snapshot', 'concurrency-acquire']);
    assert.equal(settleBodies.length, 0);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('breaker_only with true concurrency waits and only authorizes after CQ claim and ack_handoff', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const settleBodies = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
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
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 71,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.push('authorize-breaker-attempt');
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 72,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 11,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      calls.push(body.waitToken ? 'concurrency-acquire-continue' : 'concurrency-acquire-fast');
      if (!body.waitToken) {
        return createJsonResponse({
          result: 'wait',
          waitToken: 'breaker-only-wait-token',
          scope: 'host',
          retryAfter: 1,
        });
      }
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-breaker-only-wait',
        leaseToken: 'token-breaker-only-wait',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-breaker-only-wait',
      });
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      calls.push('settle-breaker-attempt');
      settleBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 72,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('wait-ok', {
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
        VERSION: 73,
        LAST_ERROR_CODE: null,
      }]);
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === ACK_HANDOFF_URL) {
      calls.push('concurrency-ack-handoff');
      return createAckHandoffResponse({ result: 'acknowledged' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    assert.equal(await response.text(), 'wait-ok');
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(calls.slice(0, 6), [
      'concurrency-acquire-fast',
      'concurrency-acquire-continue',
      'concurrency-claim',
      'concurrency-ack-handoff',
      'authorize-breaker-attempt',
      'origin-fetch',
    ]);
    assert.equal(settleBodies.length, 0);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('breaker_only with true concurrency does not authorize or settle before terminal CQ response', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const settleBodies = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
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
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 81,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.push('authorize-breaker-attempt');
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 82,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 12,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire-fast');
      return createJsonResponse({ result: 'expired', reason: 'hard_expired' }, { status: 410 });
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      calls.push('settle-breaker-attempt');
      settleBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 82,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      throw new Error('origin fetch should not run after terminal CQ response');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), createTestContext().ctx);
    const body = await readJson(response);
    assert.equal(response.status, 401);
    assert.equal(body.message, 'link expired');
    assert.deepEqual(calls, ['concurrency-acquire-fast']);
    assert.equal(settleBodies.length, 0);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('breaker_only with true concurrency does not authorize or settle before acquire failure cleanup', async () => {
  const originalFetch = globalThis.fetch;
  const originalRandomUUID = crypto.randomUUID;
  const calls = [];
  const settleBodies = [];
  let acquireBody = null;
  let cancelBody = null;

  crypto.randomUUID = () => 'req-breaker-only-acquire-failure';
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
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
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 91,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.push('authorize-breaker-attempt');
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 92,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 13,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire-fast');
      acquireBody = JSON.parse(init.body);
      throw new TypeError('fetch failed');
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      calls.push('settle-breaker-attempt');
      settleBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 92,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/cancel') {
      calls.push('concurrency-cancel');
      cancelBody = JSON.parse(init.body);
      return createJsonResponse({ result: 'cancelled' });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      throw new Error('origin fetch should not run after acquire failure');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(response.status, 503);
    assert.match(body.message, /true concurrency unavailable/i);
    assert.deepEqual(calls, [
      'concurrency-acquire-fast',
      'concurrency-cancel',
    ]);
    assert.equal(settleBodies.length, 0);
    assert.equal(cancelBody?.requestId, acquireBody?.requestId);
    assert.equal(cancelBody?.hostname, acquireBody?.hostname);
  } finally {
    globalThis.fetch = originalFetch;
    crypto.randomUUID = originalRandomUUID;
    delete globalThis.bootstrapCache;
  }
});

test('breaker_only with true concurrency does not authorize or settle after claim transport failure before returning', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const settleBodies = [];
  let releaseBody = null;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
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
      calls.push('breaker-snapshot');
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 101,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.push('breaker-authorize');
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 102,
        LAST_ERROR_CODE: 429,
        HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 14,
      }]);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire-fast');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-breaker-only-claim-fail',
        leaseToken: 'token-breaker-only-claim-fail',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-breaker-only-claim-fail',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      throw new Error('claim transport failed');
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      releaseBody = JSON.parse(init.body);
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      calls.push('breaker-settle');
      settleBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 102,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('should-not-fetch', { status: 200 });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(response.status, 503);
    assert.deepEqual(calls, [
      'breaker-snapshot',
      'concurrency-acquire-fast',
      'concurrency-claim',
      'concurrency-release',
    ]);
    assert.equal(releaseBody.leaseToken, 'token-breaker-only-claim-fail');
    assert.equal(releaseBody.reason, 'final_cleanup');
    assert.equal(settleBodies.length, 0);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_breaker dual mode settles breaker attempt when target expires before CQ acquire', async () => {
  const originalFetch = globalThis.fetch;
  const originalDateNow = Date.now;
  const calls = [];
  const fairQueueReleaseBodies = [];
  const settleBodies = [];
  let advancedClock = false;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      advancedClock = true;
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-expire-before-cq',
        invocationEpoch: 1,
        slotToken: 'slot-expire-before-cq',
        meta: {
          attemptVersion: 61,
          attemptTicket: 4,
        },
      });
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      calls.push('breaker-settle');
      settleBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 61,
        LAST_ERROR_CODE: 429,
      }]);
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      fairQueueReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      throw new Error('concurrency acquire should not be reached after expiry gate');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const baseNowMs = Date.now();
    Date.now = () => (advancedClock ? (baseNowMs + 5000) : baseNowMs);

    const expireSeconds = Math.floor((baseNowMs + 2500) / 1000);
    const request = await buildSignedWorkerRequest({
      payloadExpireTime: expireSeconds,
      payloadSignExpire: expireSeconds,
    });
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(request, buildWorkerEnv(), ctx);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(response.status, 401);
    assert.deepEqual(calls, [
      'fairqueue-acquire',
      'breaker-settle',
      'fairqueue-release',
    ]);
    assert.equal(fairQueueReleaseBodies.length, 1);
    assert.equal(fairQueueReleaseBodies[0].hitUpstreamAtMs, 0);
    assert.equal(settleBodies.length, 1);
    assert.equal(settleBodies[0].p_attempt_version, 61);
    assert.equal(settleBodies[0].p_attempt_ticket, 4);
  } finally {
    Date.now = originalDateNow;
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('dual mode malformed concurrency acquire result fails closed after fairqueue release and best-effort cancel cleanup', async () => {
  const originalFetch = globalThis.fetch;
  const originalRandomUUID = crypto.randomUUID;
  const calls = [];
  const fairQueueReleaseBodies = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-malformed-acquire',
        invocationEpoch: 1,
        slotToken: 'slot-malformed-acquire',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      return createJsonResponse({ result: 'allow' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/cancel') {
      calls.push('concurrency-cancel');
      const body = JSON.parse(init.body);
      assert.equal(body.requestId, 'req-malformed-acquire-recovery');
      return createJsonResponse({ result: 'cancelled' });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      fairQueueReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    crypto.randomUUID = () => 'req-malformed-acquire-recovery';
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(response.status, 503);
    assert.match(body.message, /true concurrency unavailable/i);
    assert.deepEqual(calls, ['fairqueue-acquire', 'concurrency-acquire', 'fairqueue-release', 'concurrency-cancel']);
    assert.equal(fairQueueReleaseBodies.length, 1);
    assert.equal(fairQueueReleaseBodies[0].hitUpstreamAtMs, 0);
  } finally {
    globalThis.fetch = originalFetch;
    crypto.randomUUID = originalRandomUUID;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency malformed acquire response still triggers best-effort cancel cleanup', async () => {
  const originalFetch = globalThis.fetch;
  const originalRandomUUID = crypto.randomUUID;
  const calls = [];
  let acquireBody = null;
  let cancelBody = null;

  crypto.randomUUID = () => 'req-ambiguous-acquire-1';
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      acquireBody = JSON.parse(init.body);
      return new Response('{"result":"granted"', {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/cancel') {
      calls.push('concurrency-cancel');
      cancelBody = JSON.parse(init.body);
      return createJsonResponse({ result: 'cancelled' });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      throw new Error('origin fetch should not run after ambiguous acquire failure');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.match(body.message, /true concurrency unavailable/i);
    assert.deepEqual(calls, ['concurrency-acquire', 'concurrency-cancel']);
    assert.equal(cancelBody?.requestId, acquireBody?.requestId);
    assert.equal(cancelBody?.hostname, acquireBody?.hostname);
    assert.equal(cancelBody?.hostnameHash, acquireBody?.hostnameHash);
    assert.equal(cancelBody?.siteBucket, acquireBody?.siteBucket);
    assert.equal(cancelBody?.ipBucket, acquireBody?.ipBucket);
    assert.equal(cancelBody?.hardExpireAtMs, acquireBody?.hardExpireAtMs);
  } finally {
    globalThis.fetch = originalFetch;
    crypto.randomUUID = originalRandomUUID;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency acquire fetch rejection after dispatch still triggers best-effort cancel cleanup', async () => {
  const originalFetch = globalThis.fetch;
  const originalRandomUUID = crypto.randomUUID;
  const calls = [];
  let acquireBody = null;
  let cancelBody = null;

  crypto.randomUUID = () => 'req-ambiguous-reject-1';
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      acquireBody = JSON.parse(init.body);
      throw new TypeError('fetch failed');
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/cancel') {
      calls.push('concurrency-cancel');
      cancelBody = JSON.parse(init.body);
      return createJsonResponse({ result: 'cancelled' });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      throw new Error('origin fetch should not run after acquire fetch rejection');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.match(body.message, /true concurrency unavailable/i);
    assert.deepEqual(calls, ['concurrency-acquire', 'concurrency-cancel']);
    assert.equal(cancelBody?.requestId, acquireBody?.requestId);
    assert.equal(cancelBody?.hostname, acquireBody?.hostname);
    assert.equal(cancelBody?.hostnameHash, acquireBody?.hostnameHash);
    assert.equal(cancelBody?.siteBucket, acquireBody?.siteBucket);
    assert.equal(cancelBody?.ipBucket, acquireBody?.ipBucket);
    assert.equal(cancelBody?.hardExpireAtMs, acquireBody?.hardExpireAtMs);
  } finally {
    globalThis.fetch = originalFetch;
    crypto.randomUUID = originalRandomUUID;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency non-200 acquire response after dispatch still triggers best-effort cancel cleanup', async () => {
  const originalFetch = globalThis.fetch;
  const originalRandomUUID = crypto.randomUUID;
  const calls = [];
  let acquireBody = null;
  let cancelBody = null;

  crypto.randomUUID = () => 'req-ambiguous-status-1';
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      acquireBody = JSON.parse(init.body);
      return new Response('service unavailable', { status: 503 });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/cancel') {
      calls.push('concurrency-cancel');
      cancelBody = JSON.parse(init.body);
      return createJsonResponse({ result: 'cancelled' });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      throw new Error('origin fetch should not run after acquire non-200 failure');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.match(body.message, /true concurrency unavailable/i);
    assert.deepEqual(calls, ['concurrency-acquire', 'concurrency-cancel']);
    assert.equal(cancelBody?.requestId, acquireBody?.requestId);
    assert.equal(cancelBody?.hostname, acquireBody?.hostname);
    assert.equal(cancelBody?.hostnameHash, acquireBody?.hostnameHash);
    assert.equal(cancelBody?.siteBucket, acquireBody?.siteBucket);
    assert.equal(cancelBody?.ipBucket, acquireBody?.ipBucket);
    assert.equal(cancelBody?.hardExpireAtMs, acquireBody?.hardExpireAtMs);
  } finally {
    globalThis.fetch = originalFetch;
    crypto.randomUUID = originalRandomUUID;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency only skips precheck and fairqueue and acks handoff before origin fetch', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  let ackBody = null;
  let heartbeatHeaders = null;
  const heartbeatSocket = createFakeHeartbeatSocket();

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://cq.example.test/api/v1/concurrency/precheck') {
      calls.push('precheck');
      return createJsonResponse({ result: 'allow' });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({ result: 'granted', queryToken: 'q', invocationEpoch: 1, slotToken: 's' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-1',
        leaseToken: 'token-1',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-1',
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/ack_handoff') {
      calls.push('concurrency-ack-handoff');
      ackBody = JSON.parse(init.body);
      return createAckHandoffResponse({ result: 'acknowledged' });
    }

    if (url === HEARTBEAT_URL) {
      calls.push('heartbeat-upgrade');
      heartbeatHeaders = init.headers;
      return {
        status: 101,
        webSocket: heartbeatSocket,
      };
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'ok');
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(calls.slice(0, 5), ['concurrency-acquire', 'concurrency-claim', 'concurrency-ack-handoff', 'heartbeat-upgrade', 'origin-fetch']);
    assert.equal(typeof ackBody?.requestId, 'string');
    assert.equal(ackBody?.handoffToken, 'handoff-1');
    assert.equal(heartbeatHeaders?.Upgrade, 'websocket');
    assert.equal(heartbeatHeaders?.['X-CQ-Auth'], 'cq-secret');
    const helloPayload = JSON.parse(heartbeatSocket.sent[0]);
    assert.equal(helloPayload.type, 'hello');
    assert.equal(helloPayload.requestId, ackBody.requestId);
    assert.equal(helloPayload.leaseId, 'lease-1');
    assert.equal(helloPayload.leaseToken, 'token-1');
    assert.equal(Object.hasOwn(helloPayload, 'downloadedBytes'), false);
    assert.equal(calls.includes('precheck'), false);
    assert.equal(calls.includes('fairqueue-acquire'), false);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency heartbeat hello timeout retries initial connect before origin fetch and releases heartbeat_connect_failed', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const heartbeatSockets = [];
  let releaseBody = null;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHeartbeatOverrides: {
          helloTimeoutMs: 5,
          startTimeoutMs: 50,
          initialConnectMaxAttempts: 3,
          initialConnectMaxElapsedMs: 20,
          reconnectSafetyMarginMs: 1,
        },
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-heartbeat-timeout',
        leaseToken: 'token-heartbeat-timeout',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-heartbeat-timeout',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === ACK_HANDOFF_URL) {
      calls.push('concurrency-ack-handoff');
      return createAckHandoffResponse({ result: 'acknowledged' });
    }

    if (url === HEARTBEAT_URL) {
      calls.push('heartbeat-upgrade');
      const socket = createFakeHeartbeatSocket({ helloAck: null, heartbeatAck: null });
      heartbeatSockets.push(socket);
      return {
        status: 101,
        webSocket: socket,
      };
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      releaseBody = JSON.parse(init.body);
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      throw new Error('origin fetch should not run after heartbeat hello timeout');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.match(body.message, /true concurrency unavailable/i);
    assert.equal(calls.includes('origin-fetch'), false);
    assert.equal(calls.filter((call) => call === 'heartbeat-upgrade').length, 3);
    assert.equal(releaseBody?.reason, 'heartbeat_connect_failed');
    assert.deepEqual(
      heartbeatSockets.map((socket) => JSON.parse(socket.sent[0]).attempt),
      [1, 2, 3],
    );
    for (const socket of heartbeatSockets) {
      const helloPayload = JSON.parse(socket.sent[0]);
      assert.equal(helloPayload.type, 'hello');
      assert.equal(Object.hasOwn(helloPayload, 'downloadedBytes'), false);
    }
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency heartbeat_connect_failed schedules release retry cleanup through waitUntil when immediate release fails', async () => {
  const originalFetch = globalThis.fetch;
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  const releaseBodies = [];
  const retryDelays = [];
  let releaseAttempts = 0;

  globalThis.setTimeout = (callback, delay = 0, ...args) => {
    retryDelays.push(delay);
    Promise.resolve().then(() => callback(...args));
    return { cleared: false };
  };
  globalThis.clearTimeout = (handle) => {
    if (handle && typeof handle === 'object') {
      handle.cleared = true;
    }
  };

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHeartbeatOverrides: {
          initialConnectMaxAttempts: 1,
          initialConnectMaxElapsedMs: 5,
          helloTimeoutMs: 5,
          startTimeoutMs: 40,
          reconnectSafetyMarginMs: 1,
        },
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-heartbeat-release-retry',
        leaseToken: 'token-heartbeat-release-retry',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-heartbeat-release-retry',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === ACK_HANDOFF_URL) {
      return createAckHandoffResponse({ result: 'acknowledged' });
    }

    if (url === HEARTBEAT_URL) {
      return { status: 503 };
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      releaseAttempts += 1;
      releaseBodies.push(JSON.parse(init.body));
      if (releaseAttempts === 1) {
        throw new Error('release transport failed');
      }
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      throw new Error('origin fetch should not run after immediate heartbeat upgrade failure');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.match(body.message, /true concurrency unavailable/i);
    assert.equal(waitUntilPromises.length > 0, true);
    assert.equal(releaseBodies.length >= 2, true);
    assert.deepEqual(releaseBodies.map((entry) => entry.reason), ['heartbeat_connect_failed', 'heartbeat_connect_failed']);
    assert.equal(retryDelays.includes(2000), true);
  } finally {
    globalThis.fetch = originalFetch;
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency initial heartbeat terminal mapping suppresses duplicate release or falls back to heartbeat_lost before origin fetch', async () => {
  const originalFetch = globalThis.fetch;
  const scenarios = [
    { reason: 'heartbeat_start_timeout', terminalResult: 'released', expectedReleaseReason: null },
    { reason: 'hard_expired', terminalResult: 'expired', expectedReleaseReason: null },
    { reason: 'already_released', terminalResult: 'terminal', expectedReleaseReason: null },
    { reason: 'request_cancelled', terminalResult: 'terminal', expectedReleaseReason: null },
    { reason: 'token_mismatch', terminalResult: 'terminal', expectedReleaseReason: 'heartbeat_lost' },
    { reason: 'protocol_error', terminalResult: 'terminal', expectedReleaseReason: 'heartbeat_lost' },
  ];

  try {
    for (const scenario of scenarios) {
      let originFetchCalled = false;
      let releaseBody = null;

      globalThis.fetch = async (input, init = {}) => {
        const url = typeof input === 'string' ? input : input.url;

        if (url === 'https://controller.example.test/api/v0/bootstrap') {
          return createJsonResponse(buildRuntimeBootstrap({
            trueConcurrencyHostPatterns: ['*.sharepoint.com'],
            trueConcurrencyHeartbeatOverrides: {
              helloTimeoutMs: 5,
              startTimeoutMs: 50,
              initialConnectMaxAttempts: 1,
              initialConnectMaxElapsedMs: 20,
              reconnectSafetyMarginMs: 1,
            },
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

        if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
          const body = JSON.parse(init.body);
          return createJsonResponse({
            result: 'granted',
            leaseId: `lease-initial-terminal-${scenario.reason}`,
            leaseToken: `token-initial-terminal-${scenario.reason}`,
            expiresAtMs: body.hardExpireAtMs,
            claimToken: `claim-token-initial-terminal-${scenario.reason}`,
          });
        }

        if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
          return createClaimGrantResponseFromRequest(init);
        }

        if (url === ACK_HANDOFF_URL) {
          return createAckHandoffResponse({ result: 'acknowledged' });
        }

        if (url === HEARTBEAT_URL) {
          return {
            status: 101,
            webSocket: createFakeHeartbeatSocket({
              helloAck: {
                type: 'terminal',
                result: scenario.terminalResult,
                reason: scenario.reason,
              },
              heartbeatAck: null,
            }),
          };
        }

        if (url === 'https://cq.example.test/api/v1/concurrency/release') {
          releaseBody = JSON.parse(init.body);
          return createJsonResponse({ result: 'released' });
        }

        if (url === 'https://tenant.sharepoint.com/file') {
          originFetchCalled = true;
          return new Response('should-not-fetch', { status: 200 });
        }

        throw new Error(`Unexpected fetch URL in test: ${url}`);
      };

      delete globalThis.bootstrapCache;
      const { ctx, waitUntilPromises } = createTestContext();
      const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
      const body = await readJson(response);
      await Promise.allSettled(waitUntilPromises);

      assert.equal(response.status, 503, scenario.reason);
      assert.match(body.message, /true concurrency unavailable/i, scenario.reason);
      assert.equal(originFetchCalled, false, scenario.reason);
      assert.equal(releaseBody?.reason ?? null, scenario.expectedReleaseReason, scenario.reason);
    }
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency claim success must ack_handoff before queue_breaker authorize and origin fetch', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const settleBodies = [];
  const authorizeBodies = [];
  let ackBody = null;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        throttleHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/demo/file',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-handoff-queue-breaker',
        invocationEpoch: 1,
        slotToken: 'slot-handoff-queue-breaker',
        meta: {
          attemptVersion: 91,
          attemptTicket: 12,
        },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      calls.push(body.waitToken ? 'concurrency-acquire-continue' : 'concurrency-acquire-fast');
      if (!body.waitToken) {
        return createJsonResponse({
          result: 'wait',
          waitToken: 'wait-handoff-queue-breaker',
          scope: 'host',
          retryAfter: 1,
        });
      }
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-handoff-queue-breaker',
        leaseToken: 'token-handoff-queue-breaker',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-handoff-queue-breaker',
      });
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

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/ack_handoff') {
      calls.push('concurrency-ack-handoff');
      ackBody = JSON.parse(init.body);
      return createAckHandoffResponse({ result: 'acknowledged' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.push('breaker-authorize');
      authorizeBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 92,
        LAST_ERROR_CODE: null,
        ATTEMPT_GRANTED: false,
        ATTEMPT_TICKET: null,
      }]);
    }

    if (url === 'https://tenant.sharepoint.com/sites/demo/file') {
      calls.push('origin-fetch');
      return new Response('handoff-ok', {
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
        VERSION: 93,
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
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'handoff-ok');
    await Promise.allSettled(waitUntilPromises);

    const claimIndex = calls.indexOf('concurrency-claim');
    const ackIndex = calls.indexOf('concurrency-ack-handoff');
    const authorizeIndex = calls.indexOf('breaker-authorize');
    const originIndex = calls.indexOf('origin-fetch');
    assert.ok(claimIndex >= 0);
    assert.ok(ackIndex > claimIndex);
    assert.ok(authorizeIndex > ackIndex);
    assert.ok(originIndex > authorizeIndex);
    assert.equal(ackBody?.handoffToken, 'handoff-handoff-queue-breaker');
    assert.equal(settleBodies.length, 1);
    assert.equal(settleBodies[0].p_attempt_version, 91);
    assert.equal(settleBodies[0].p_attempt_ticket, 12);
    assert.equal(authorizeBodies.length, 1);
    assert.equal(authorizeBodies[0].p_hostname, 'tenant.sharepoint.com');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency ack_handoff transport or availability or malformed-success failure releases lease and does not fetch origin', async () => {
  const originalFetch = globalThis.fetch;
  const scenarios = [
    {
      name: 'transport failure',
      respondToAckHandoff() {
        throw new Error('ack handoff transport failed');
      },
    },
    {
      name: 'availability failure',
      respondToAckHandoff() {
        return new Response(JSON.stringify({ code: 503, message: 'ack handoff unavailable' }), {
          status: 503,
          headers: { 'content-type': 'application/json' },
        });
      },
    },
    {
      name: 'malformed-success normalization failure',
      respondToAckHandoff() {
        return createAckHandoffResponse({ result: 'acknowledged', reason: 'unexpected_reason' });
      },
    },
  ];

  try {
    for (const scenario of scenarios) {
      const calls = [];
      let releaseBody = null;

      globalThis.fetch = async (input, init = {}) => {
        const url = typeof input === 'string' ? input : input.url;

        if (url === 'https://controller.example.test/api/v0/bootstrap') {
          return createJsonResponse(buildRuntimeBootstrap({ trueConcurrencyHostPatterns: ['*.sharepoint.com'] }));
        }
        if (url === 'https://alist.example.com/api/fs/link') {
          return createJsonResponse({ code: 200, data: { url: 'https://tenant.sharepoint.com/file', header: {} } });
        }
        if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
          calls.push('concurrency-acquire-fast');
          const body = JSON.parse(init.body);
          return createJsonResponse({
            result: 'granted',
            leaseId: 'lease-ack-fail',
            leaseToken: 'token-ack-fail',
            expiresAtMs: body.hardExpireAtMs,
            claimToken: 'claim-token-ack-fail',
          });
        }
        if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
          calls.push('concurrency-claim');
          return createClaimGrantResponseFromRequest(init);
        }
        if (url === 'https://cq.example.test/api/v1/concurrency/ack_handoff') {
          calls.push('concurrency-ack-handoff');
          return scenario.respondToAckHandoff(init);
        }
        if (url === 'https://cq.example.test/api/v1/concurrency/release') {
          calls.push('concurrency-release');
          releaseBody = JSON.parse(init.body);
          return createJsonResponse({ result: 'released' });
        }
        if (url === 'https://tenant.sharepoint.com/file') {
          calls.push('origin-fetch');
          return new Response('should-not-fetch', { status: 200 });
        }
        throw new Error(`Unexpected fetch URL in test: ${url}`);
      };

      setAckHandoffMock((input, init = {}) => {
        calls.push('concurrency-ack-handoff');
        return scenario.respondToAckHandoff(input, init);
      });

      try {
        const { ctx, waitUntilPromises } = createTestContext();
        const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
        const body = await readJson(response);
        await Promise.allSettled(waitUntilPromises);
        assert.equal(response.status, 503, scenario.name);
        assert.equal(body.message, 'True concurrency unavailable', scenario.name);
        assert.deepEqual(calls, ['concurrency-acquire-fast', 'concurrency-claim', 'concurrency-ack-handoff', 'concurrency-release'], scenario.name);
        assert.equal(releaseBody.leaseId, 'lease-ack-fail', scenario.name);
        assert.equal(releaseBody.leaseToken, 'token-ack-fail', scenario.name);
        assert.equal(releaseBody.reason, 'final_cleanup', scenario.name);
      } finally {
        delete globalThis.bootstrapCache;
      }
    }
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency explicit ack_handoff terminal response does not release again and does not fetch origin', async () => {
  const originalFetch = globalThis.fetch;
  const scenarios = [
    {
      name: 'released',
      ackPayload: { result: 'released', reason: 'claim_handoff_timeout' },
      expectedStatus: 503,
      expectedMessage: 'True concurrency released (claim_handoff_timeout)',
    },
    {
      name: 'cancelled',
      ackPayload: { result: 'cancelled', reason: 'request_cancelled' },
      expectedStatus: 503,
      expectedMessage: 'True concurrency cancelled (request_cancelled)',
    },
    {
      name: 'expired',
      ackPayload: { result: 'expired', reason: 'hard_expired' },
      expectedStatus: 401,
      expectedMessage: 'link expired',
    },
  ];

  try {
    for (const scenario of scenarios) {
      const calls = [];

      globalThis.fetch = async (input, init = {}) => {
        const url = typeof input === 'string' ? input : input.url;

        if (url === 'https://controller.example.test/api/v0/bootstrap') {
          return createJsonResponse(buildRuntimeBootstrap({ trueConcurrencyHostPatterns: ['*.sharepoint.com'] }));
        }
        if (url === 'https://alist.example.com/api/fs/link') {
          return createJsonResponse({ code: 200, data: { url: 'https://tenant.sharepoint.com/file', header: {} } });
        }
        if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
          calls.push('concurrency-acquire-fast');
          const body = JSON.parse(init.body);
          return createJsonResponse({
            result: 'granted',
            leaseId: 'lease-ack-terminal',
            leaseToken: 'token-ack-terminal',
            expiresAtMs: body.hardExpireAtMs,
            claimToken: 'claim-token-ack-terminal',
          });
        }
        if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
          calls.push('concurrency-claim');
          return createClaimGrantResponseFromRequest(init);
        }
        if (url === 'https://cq.example.test/api/v1/concurrency/ack_handoff') {
          calls.push('concurrency-ack-handoff');
          return createAckHandoffResponse(scenario.ackPayload);
        }
        if (url === 'https://cq.example.test/api/v1/concurrency/release') {
          calls.push('concurrency-release');
          throw new Error('release should not run after explicit ack_handoff terminal response');
        }
        if (url === 'https://tenant.sharepoint.com/file') {
          calls.push('origin-fetch');
          return new Response('should-not-fetch', { status: 200 });
        }
        throw new Error(`Unexpected fetch URL in test: ${url}`);
      };

      setAckHandoffMock(() => {
        calls.push('concurrency-ack-handoff');
        return createAckHandoffResponse(scenario.ackPayload);
      });

      try {
        const { ctx, waitUntilPromises } = createTestContext();
        const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
        const bodyText = await response.text();
        await Promise.allSettled(waitUntilPromises);
        assert.equal(response.status, scenario.expectedStatus, scenario.name);
        assert.match(bodyText, new RegExp(scenario.expectedMessage.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')), scenario.name);
        assert.deepEqual(calls, ['concurrency-acquire-fast', 'concurrency-claim', 'concurrency-ack-handoff'], scenario.name);
      } finally {
        delete globalThis.bootstrapCache;
      }
    }
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency explicit ack_handoff conflict fails closed and waits for timeout compensation instead of releasing', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({ trueConcurrencyHostPatterns: ['*.sharepoint.com'] }));
    }
    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({ code: 200, data: { url: 'https://tenant.sharepoint.com/file', header: {} } });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire-fast');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-ack-conflict',
        leaseToken: 'token-ack-conflict',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-ack-conflict',
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return createClaimGrantResponseFromRequest(init);
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/ack_handoff') {
      calls.push('concurrency-ack-handoff');
      return createAckHandoffResponse({ result: 'conflict', reason: 'handoff_token_mismatch' });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      throw new Error('release should not run after explicit ack_handoff conflict');
    }
    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('should-not-fetch', { status: 200 });
    }
    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  setAckHandoffMock(() => {
    calls.push('concurrency-ack-handoff');
    return createAckHandoffResponse({ result: 'conflict', reason: 'handoff_token_mismatch' });
  });

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(response.status, 503);
    assert.equal(body.message, 'True concurrency conflict (handoff_token_mismatch)');
    assert.deepEqual(calls, ['concurrency-acquire-fast', 'concurrency-claim', 'concurrency-ack-handoff']);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency claim terminal replay with claim_handoff_timeout fails closed and does not fetch origin', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({ trueConcurrencyHostPatterns: ['*.sharepoint.com'] }));
    }
    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({ code: 200, data: { url: 'https://tenant.sharepoint.com/file', header: {} } });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire-fast');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-claim-timeout',
        leaseToken: 'token-claim-timeout',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-claim-timeout',
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return createJsonResponse({ result: 'released', reason: 'claim_handoff_timeout' }, { status: 410 });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      throw new Error('release should not run for claim_handoff_timeout terminal replay');
    }
    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('should-not-fetch', { status: 200 });
    }
    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(response.status, 503);
    assert.equal(body.message, 'True concurrency released (claim_handoff_timeout)');
    assert.deepEqual(calls, ['concurrency-acquire-fast', 'concurrency-claim']);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency acquire terminal replay with claim_handoff_timeout fails closed and does not fetch origin', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({ trueConcurrencyHostPatterns: ['*.sharepoint.com'] }));
    }
    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({ code: 200, data: { url: 'https://tenant.sharepoint.com/file', header: {} } });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire-fast');
      return createJsonResponse({ result: 'released', reason: 'claim_handoff_timeout' }, { status: 410 });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      throw new Error('claim should not run for claim_handoff_timeout acquire replay');
    }
    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('should-not-fetch', { status: 200 });
    }
    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(response.status, 503);
    assert.equal(body.message, 'True concurrency released (claim_handoff_timeout)');
    assert.deepEqual(calls, ['concurrency-acquire-fast']);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency acquire terminal replay with heartbeat_timeout fails closed and does not fetch origin', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({ trueConcurrencyHostPatterns: ['*.sharepoint.com'] }));
    }
    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({ code: 200, data: { url: 'https://tenant.sharepoint.com/file', header: {} } });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire-fast');
      return createJsonResponse({ result: 'released', reason: 'heartbeat_timeout' }, { status: 410 });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      throw new Error('claim should not run for heartbeat_timeout acquire replay');
    }
    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('should-not-fetch', { status: 200 });
    }
    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(response.status, 503);
    assert.equal(body.message, 'True concurrency released (heartbeat_timeout)');
    assert.deepEqual(calls, ['concurrency-acquire-fast']);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency claim terminal replay with heartbeat_timeout fails closed and does not fetch origin', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({ trueConcurrencyHostPatterns: ['*.sharepoint.com'] }));
    }
    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({ code: 200, data: { url: 'https://tenant.sharepoint.com/file', header: {} } });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire-fast');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-claim-heartbeat-timeout',
        leaseToken: 'token-claim-heartbeat-timeout',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-claim-heartbeat-timeout',
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return createJsonResponse({ result: 'released', reason: 'heartbeat_timeout' }, { status: 410 });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      throw new Error('release should not run for heartbeat_timeout terminal replay');
    }
    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('should-not-fetch', { status: 200 });
    }
    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(response.status, 503);
    assert.equal(body.message, 'True concurrency released (heartbeat_timeout)');
    assert.deepEqual(calls, ['concurrency-acquire-fast', 'concurrency-claim']);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency claim failure releases acquired lease before origin fetch', async () => {
  const originalFetch = globalThis.fetch;
  const scenarios = [
    {
      name: 'transport failure',
      respondToClaim() {
        throw new Error('claim transport failed');
      },
    },
    {
      name: 'availability failure',
      respondToClaim() {
        return new Response(JSON.stringify({ code: 503, message: 'claim unavailable' }), {
          status: 503,
          headers: { 'content-type': 'application/json' },
        });
      },
    },
    {
      name: 'malformed-success normalization failure',
      respondToClaim() {
        return createJsonResponse({
          result: 'granted',
          leaseId: 'lease-claim-fail',
          expiresAtMs: Date.now() + 1_000,
        });
      },
    },
  ];

  try {
    for (const scenario of scenarios) {
      const calls = [];
      let releaseBody = null;

      globalThis.fetch = async (input, init = {}) => {
        const url = typeof input === 'string' ? input : input.url;

        if (url === 'https://controller.example.test/api/v0/bootstrap') {
          return createJsonResponse(buildRuntimeBootstrap({ trueConcurrencyHostPatterns: ['*.sharepoint.com'] }));
        }
        if (url === 'https://alist.example.com/api/fs/link') {
          return createJsonResponse({ code: 200, data: { url: 'https://tenant.sharepoint.com/file', header: {} } });
        }
        if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
          calls.push('concurrency-acquire-fast');
          const body = JSON.parse(init.body);
          return createJsonResponse({ result: 'granted', leaseId: 'lease-claim-fail', leaseToken: 'token-claim-fail', expiresAtMs: body.hardExpireAtMs, claimToken: 'claim-token-fail' });
        }
        if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
          calls.push('concurrency-claim');
          return scenario.respondToClaim(init);
        }
        if (url === 'https://cq.example.test/api/v1/concurrency/release') {
          calls.push('concurrency-release');
          releaseBody = JSON.parse(init.body);
          return createJsonResponse({ result: 'released' });
        }
        if (url === 'https://tenant.sharepoint.com/file') {
          calls.push('origin-fetch');
          return new Response('should-not-fetch', { status: 200 });
        }
        throw new Error(`Unexpected fetch URL in test: ${url}`);
      };

      try {
        const { ctx, waitUntilPromises } = createTestContext();
        const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
        const body = await readJson(response);
        await Promise.allSettled(waitUntilPromises);
        assert.equal(response.status, 503, scenario.name);
        assert.equal(body.message, 'True concurrency unavailable', scenario.name);
        assert.deepEqual(calls, ['concurrency-acquire-fast', 'concurrency-claim', 'concurrency-release'], scenario.name);
        assert.equal(releaseBody.leaseId, 'lease-claim-fail', scenario.name);
        assert.equal(releaseBody.leaseToken, 'token-claim-fail', scenario.name);
        assert.equal(releaseBody.reason, 'final_cleanup', scenario.name);
      } finally {
        delete globalThis.bootstrapCache;
      }
    }
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency claim terminal response does not fetch origin', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({ trueConcurrencyHostPatterns: ['*.sharepoint.com'] }));
    }
    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({ code: 200, data: { url: 'https://tenant.sharepoint.com/file', header: {} } });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire-fast');
      const body = JSON.parse(init.body);
      return createJsonResponse({ result: 'granted', leaseId: 'lease-claim-terminal', leaseToken: 'token-claim-terminal', expiresAtMs: body.hardExpireAtMs, claimToken: 'claim-token-terminal' });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return new Response(JSON.stringify({ result: 'conflict', reason: 'grant_already_claimed' }), { status: 409, headers: { 'content-type': 'application/json' } });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }
    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('should-not-fetch', { status: 200 });
    }
    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(response.status, 503);
    assert.deepEqual(calls, ['concurrency-acquire-fast', 'concurrency-claim']);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency claim conflict grant_unclaimed releases acquired lease before origin fetch', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  let releaseBody = null;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({ trueConcurrencyHostPatterns: ['*.sharepoint.com'] }));
    }
    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({ code: 200, data: { url: 'https://tenant.sharepoint.com/file', header: {} } });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire-fast');
      const body = JSON.parse(init.body);
      return createJsonResponse({ result: 'granted', leaseId: 'lease-claim-conflict', leaseToken: 'token-claim-conflict', expiresAtMs: body.hardExpireAtMs, claimToken: 'claim-token-conflict' });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return new Response(JSON.stringify({ result: 'conflict', reason: 'grant_unclaimed' }), { status: 409, headers: { 'content-type': 'application/json' } });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      releaseBody = JSON.parse(init.body);
      return createJsonResponse({ result: 'released' });
    }
    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('should-not-fetch', { status: 200 });
    }
    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(response.status, 503);
    assert.equal(body.message, 'True concurrency conflict (grant_unclaimed)');
    assert.deepEqual(calls, ['concurrency-acquire-fast', 'concurrency-claim', 'concurrency-release']);
    assert.equal(releaseBody.leaseId, 'lease-claim-conflict');
    assert.equal(releaseBody.leaseToken, 'token-claim-conflict');
    assert.equal(releaseBody.reason, 'final_cleanup');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency only fast hard expiry returns link expired without fairqueue cleanup', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      return new Response(JSON.stringify({ result: 'expired', reason: 'hard_expired' }), {
        status: 410,
        headers: { 'content-type': 'application/json' },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/cancel') {
      calls.push('concurrency-cancel');
      throw new Error('cancel should not run for direct expired terminal response');
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      throw new Error('release should not run for direct expired terminal response');
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire' || url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      throw new Error('fairqueue should not be involved for true-concurrency-only expiry');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 401);
    assert.equal(body.message, 'link expired');
    assert.deepEqual(calls, ['concurrency-acquire']);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('expired true concurrency link rejects before handler calls', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    calls.push(url);

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const request = await buildSignedWorkerRequest({
      expireOffsetSeconds: 300,
      payloadExpireTime: Math.floor(Date.now() / 1000) - 5,
    });
    const response = await worker.fetch(request, buildWorkerEnv(), createTestContext().ctx);
    assert.equal(response.status, 401);
    assert.equal(calls.includes('https://cq.example.test/api/v1/concurrency/acquire'), false);
    assert.equal(calls.includes('https://cq.example.test/api/v1/concurrency/precheck'), false);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('dual mode treats CQ failure after fairqueue admission as unavailable and releases fairqueue', async () => {
  const originalFetch = globalThis.fetch;
  const originalDateNow = Date.now;
  const calls = [];
  const fairQueueReleaseBodies = [];
  const baseNowMs = 1766755200000;
  let nowMs = baseNowMs;

  Date.now = () => nowMs;
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      nowMs += 900;
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-expire-1',
        invocationEpoch: 1,
        slotToken: 'slot-expire-1',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      return createJsonResponse({ result: 'deny', scope: 'host', reason: 'full', retryAfter: 1 });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/cancel') {
      calls.push('concurrency-cancel');
      return createJsonResponse({ result: 'cancelled' });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      fairQueueReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      throw new Error('origin fetch should not run after expiry');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const expireAtSeconds = Math.floor(baseNowMs / 1000) + 1;
    const response = await worker.fetch(await buildSignedWorkerRequest({
      expireOffsetSeconds: 1,
      payloadExpireTime: expireAtSeconds,
    }), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(response.status, 503);
    assert.match(body.message, /true concurrency unavailable/i);
    assert.equal(calls.includes('concurrency-acquire'), true);
    assert.equal(calls.includes('origin-fetch'), false);
    assert.deepEqual(calls, ['fairqueue-acquire', 'concurrency-acquire', 'fairqueue-release', 'concurrency-cancel']);
    assert.equal(fairQueueReleaseBodies.length, 1);
    assert.equal(fairQueueReleaseBodies[0].hitUpstreamAtMs, 0);
  } finally {
    globalThis.fetch = originalFetch;
    Date.now = originalDateNow;
    delete globalThis.bootstrapCache;
  }
});

test('dual mode client-aborted CQ acquire releases the fairqueue grant as unused before cancel cleanup', async () => {
  const originalFetch = globalThis.fetch;
  const abortController = new AbortController();
  const calls = [];
  const fairQueueReleaseBodies = [];
  const cancelBodies = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/demo/file',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-abort-1',
        invocationEpoch: 1,
        slotToken: 'slot-abort-1',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      abortController.abort();
      throw new DOMException('The operation was aborted.', 'AbortError');
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      fairQueueReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/cancel') {
      calls.push('concurrency-cancel');
      cancelBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'cancelled' });
    }

    if (url === 'https://tenant.sharepoint.com/sites/demo/file') {
      throw new Error('origin fetch should not run after acquire-stage client abort');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const request = await buildSignedWorkerRequest({ signal: abortController.signal });
    const response = await worker.fetch(request, buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 499);
    assert.equal(body.message, 'client aborted request');
    assert.deepEqual(calls, ['fairqueue-acquire', 'concurrency-acquire', 'fairqueue-release', 'concurrency-cancel']);
    assert.equal(fairQueueReleaseBodies.length, 1);
    assert.equal(fairQueueReleaseBodies[0].hitUpstreamAtMs, 0);
    assert.equal(cancelBodies.length, 1);
    assert.equal(cancelBodies[0].reason, 'worker_aborted');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('non-positive payloadSign expiry is rejected before admission handlers run', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    calls.push(url);

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      throw new Error('concurrency acquire should not run for invalid payloadSign expiry');
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      throw new Error('origin fetch should not run for invalid payloadSign expiry');
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      throw new Error('concurrency release should not run for invalid payloadSign expiry');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest({
      payloadSignExpire: 0,
    }), buildWorkerEnv(), createTestContext().ctx);
    const body = await readJson(response);
    assert.equal(response.status, 401);
    assert.match(body.message, /payloadsign expire invalid/i);
    assert.equal(calls.includes('https://cq.example.test/api/v1/concurrency/precheck'), false);
    assert.equal(calls.includes('https://cq.example.test/api/v1/concurrency/acquire'), false);
    assert.equal(calls.includes('https://slot-handler.example.test/api/v1/fairqueue/acquire'), false);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

for (const idlePolicy of ['first_use', 'renewable']) {
  test(`unused ${idlePolicy} ticket is denied exactly at issued_at plus idle_timeout`, async () => {
    const originalFetch = globalThis.fetch;
    const calls = [];

    globalThis.fetch = async (input, init = {}) => {
      const url = typeof input === 'string' ? input : input.url;
      calls.push(url);

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

      if (url === 'https://tenant.sharepoint.com/file') {
        throw new Error('origin fetch should not run at the idle cutoff boundary');
      }

      throw new Error(`Unexpected fetch URL in test: ${url}`);
    };
    ticketStateRpcState.readHandler = (body) => createDefaultTicketStateRow(body, {
      issued_at: Math.floor(Date.now() / 1000) - DEFAULT_IDLE_TIMEOUT_SECONDS,
      first_used_at: null,
      idle_timeout_seconds: DEFAULT_IDLE_TIMEOUT_SECONDS,
      idle_policy: idlePolicy,
    });

    try {
      const response = await worker.fetch(
        await buildSignedWorkerRequest({ idleTimeoutSeconds: DEFAULT_IDLE_TIMEOUT_SECONDS }),
        buildWorkerEnv(),
        createTestContext().ctx,
      );
      const body = await readJson(response);

      assert.equal(response.status, 410);
      assert.equal(body.message, 'Link expired due to inactivity');
      assert.equal(ticketStateRpcState.readBodies.length, 1);
      assert.equal(ticketStateRpcState.markBodies.length, 0);
      assert.equal(calls.includes('https://tenant.sharepoint.com/file'), false);
    } finally {
      globalThis.fetch = originalFetch;
      delete globalThis.bootstrapCache;
    }
  });
}

test('unused first_use ticket with row idle_timeout_seconds 0 is denied immediately', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    calls.push(url);

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

    if (url === 'https://tenant.sharepoint.com/file') {
      throw new Error('origin fetch should not run for zero-timeout first-use ticket');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };
  ticketStateRpcState.readHandler = (body) => createDefaultTicketStateRow(body, {
    issued_at: Math.floor(Date.now() / 1000),
    first_used_at: null,
    idle_timeout_seconds: 0,
    idle_policy: 'first_use',
    idle_lease_expires_at: Math.floor(Date.now() / 1000),
  });

  try {
    const response = await worker.fetch(
      await buildSignedWorkerRequest({ idleTimeoutSeconds: 0 }),
      buildWorkerEnv(),
      createTestContext().ctx,
    );
    const body = await readJson(response);

    assert.equal(response.status, 410);
    assert.equal(body.message, 'Link expired due to inactivity');
    assert.equal(ticketStateRpcState.readBodies.length, 1);
    assert.equal(ticketStateRpcState.markBodies.length, 0);
    assert.equal(calls.includes('https://tenant.sharepoint.com/file'), false);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('renewable ticket rejects after first use when idle_lease_expires_at is in the past', async () => {
  const originalFetch = globalThis.fetch;
  const nowSeconds = Math.floor(Date.now() / 1000);

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://origin.example.test/file',
          header: {},
        },
      });
    }

    if (url === 'https://origin.example.test/file') {
      throw new Error('origin fetch should not run after renewable lease expiry');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };
  ticketStateRpcState.readHandler = (body) => createDefaultTicketStateRow(body, {
    issued_at: nowSeconds - 20,
    first_used_at: nowSeconds - 10,
    hard_expire_at: nowSeconds + 300,
    idle_timeout_seconds: 60,
    idle_policy: 'renewable',
    idle_lease_expires_at: nowSeconds - 1,
    idle_renew_owner_lease_id: '11111111-1111-1111-1111-111111111111',
    idle_renew_owner_last_heartbeat_at: nowSeconds - 2,
  });

  try {
    const response = await worker.fetch(
      await buildSignedWorkerRequest(),
      buildWorkerEnv(),
      createTestContext().ctx,
    );
    const body = await readJson(response);

    assert.equal(response.status, 401);
    assert.equal(body.message, 'link expired');
    assert.equal(ticketStateRpcState.markBodies.length, 0);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('renewable ticket rejects after first use when idle_lease_expires_at is absent', async () => {
  const originalFetch = globalThis.fetch;
  const nowSeconds = Math.floor(Date.now() / 1000);

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://origin.example.test/file',
          header: {},
        },
      });
    }

    if (url === 'https://origin.example.test/file') {
      throw new Error('origin fetch should not run when renewable lease is absent');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };
  ticketStateRpcState.readHandler = (body) => createDefaultTicketStateRow(body, {
    issued_at: nowSeconds - 20,
    first_used_at: nowSeconds - 10,
    hard_expire_at: nowSeconds + 300,
    idle_timeout_seconds: 60,
    idle_policy: 'renewable',
    idle_lease_expires_at: null,
    idle_renew_owner_lease_id: '11111111-1111-1111-1111-111111111111',
    idle_renew_owner_last_heartbeat_at: nowSeconds - 2,
  });

  try {
    const response = await worker.fetch(
      await buildSignedWorkerRequest(),
      buildWorkerEnv(),
      createTestContext().ctx,
    );
    const body = await readJson(response);

    assert.equal(response.status, 401);
    assert.equal(body.message, 'link expired');
    assert.equal(ticketStateRpcState.markBodies.length, 0);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('first_use ticket ignores idle_lease_expires_at after first use and still runs until hard_expire_at', async () => {
  const originalFetch = globalThis.fetch;
  const nowSeconds = Math.floor(Date.now() / 1000);

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://origin.example.test/file',
          header: {},
        },
      });
    }

    if (url === 'https://origin.example.test/file') {
      return new Response('ok', { status: 200 });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };
  ticketStateRpcState.readHandler = (body) => createDefaultTicketStateRow(body, {
    issued_at: nowSeconds - 20,
    first_used_at: nowSeconds - 10,
    hard_expire_at: nowSeconds + 300,
    idle_timeout_seconds: 60,
    idle_policy: 'first_use',
    idle_lease_expires_at: nowSeconds - 1,
  });

  try {
    const response = await worker.fetch(
      await buildSignedWorkerRequest(),
      buildWorkerEnv(),
      createTestContext().ctx,
    );

    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'ok');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('heartbeat hello forwards ticketHash alongside request and lease identity', async () => {
  const originalFetch = globalThis.fetch;
  const sent = [];
  const signedRequest = await buildSignedWorkerRequest();
  const signedUrl = new URL(signedRequest.url);
  const expectedTicketHash = await sha256Hash(
    `${signedUrl.searchParams.get('payload')}:${signedUrl.searchParams.get('payloadSign')}`,
  );

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({ trueConcurrencyHostPatterns: ['*.sharepoint.com'] }));
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-1',
        leaseToken: 'token-1',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-1',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === ACK_HANDOFF_URL) {
      return createAckHandoffResponse({ result: 'acknowledged' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      return new Response('hello-ok', { status: 200 });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };
  setHeartbeatMock(() => ({
    status: 101,
    webSocket: createFakeHeartbeatSocket({
      onSend: (payload) => sent.push(payload),
    }),
  }));

  try {
    const response = await worker.fetch(new Request(signedRequest), buildWorkerEnv(), createTestContext().ctx);

    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'hello-ok');
    assert.equal(sent[0]?.type, 'hello');
    assert.equal(sent[0]?.ticketHash, expectedTicketHash);
    assert.equal(typeof sent[0]?.requestId, 'string');
    assert.equal(typeof sent[0]?.leaseId, 'string');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('heartbeat refresh forwards ticketHash alongside generation and lease identity', async () => {
  const originalFetch = globalThis.fetch;
  const sent = [];
  const { ctx, waitUntilPromises } = createTestContext();
  const signedRequest = await buildSignedWorkerRequest();
  const signedUrl = new URL(signedRequest.url);
  const expectedTicketHash = await sha256Hash(
    `${signedUrl.searchParams.get('payload')}:${signedUrl.searchParams.get('payloadSign')}`,
  );

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHeartbeatOverrides: { intervalMs: 1, ackTimeoutMs: 1000 },
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-1',
        leaseToken: 'token-1',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-1',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === ACK_HANDOFF_URL) {
      return createAckHandoffResponse({ result: 'acknowledged' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      return new Response('refresh-ok', { status: 200 });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };
  setHeartbeatMock(() => ({
    status: 101,
    webSocket: createFakeHeartbeatSocket({
      helloAck: buildHeartbeatHelloAck({ heartbeatIntervalMs: 1 }),
      onSend: (payload) => sent.push(payload),
    }),
  }));

  try {
    const response = await worker.fetch(new Request(signedRequest), buildWorkerEnv(), ctx);
    await waitForCondition(() => sent.some((payload) => payload.type === 'heartbeat'));
    assert.equal(await response.text(), 'refresh-ok');
    await Promise.allSettled(waitUntilPromises);

    const refreshPayload = sent.find((payload) => payload.type === 'heartbeat');
    assert.equal(response.status, 200);
    assert.equal(refreshPayload?.ticketHash, expectedTicketHash);
    assert.equal(typeof refreshPayload?.generation, 'number');
    assert.equal(typeof refreshPayload?.leaseId, 'string');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('disabled mode accepts signed payloads without ticketNonce and idle_timeout', async () => {
  const originalFetch = globalThis.fetch;
  const originCalls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        dbMode: '',
        includeTicketStateWiring: false,
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

    if (url === 'https://tenant.sharepoint.com/file') {
      originCalls.push(url);
      return new Response('disabled-mode-ok', {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '16',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    resetTicketStateRpcState();

    const response = await worker.fetch(
      await buildSignedWorkerRequest({
        omitTicketNonce: true,
        omitIdleTimeout: true,
      }),
      buildWorkerEnv(),
      createTestContext().ctx,
    );

    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'disabled-mode-ok');
    assert.deepEqual(originCalls, ['https://tenant.sharepoint.com/file']);
    assert.equal(ticketStateRpcState.readBodies.length, 0);
    assert.equal(ticketStateRpcState.markBodies.length, 0);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('disabled mode never resolves ticket state during admission', async () => {
  const originalFetch = globalThis.fetch;
  const originCalls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        dbMode: '',
        includeTicketStateWiring: true,
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

    if (url === 'https://tenant.sharepoint.com/file') {
      originCalls.push(url);
      return new Response('disabled-admission-ok', {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '21',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    resetTicketStateRpcState();

    const response = await worker.fetch(
      await buildSignedWorkerRequest({
        omitTicketNonce: true,
        omitIdleTimeout: true,
      }),
      buildWorkerEnv(),
      createTestContext().ctx,
    );

    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'disabled-admission-ok');
    assert.deepEqual(originCalls, ['https://tenant.sharepoint.com/file']);
    assert.equal(ticketStateRpcState.readBodies.length, 0);
    assert.equal(ticketStateRpcState.markBodies.length, 0);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('used ticket skips idle denial and relies on hard expiry only', async () => {
  const originalFetch = globalThis.fetch;
  const originCalls = [];
  const firstUsedAt = Math.floor(Date.now() / 1000) - 1800;

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

    if (url === 'https://tenant.sharepoint.com/file') {
      originCalls.push(url);
      return new Response('used-ticket-ok', {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '14',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };
  ticketStateRpcState.readHandler = (body) => createDefaultTicketStateRow(body, {
    issued_at: Math.floor(Date.now() / 1000) - 3600,
    first_used_at: firstUsedAt,
    idle_timeout_seconds: 0,
  });
  ticketStateRpcState.markHandler = () => ({
    result: 'already_used',
    first_used_at: firstUsedAt,
  });

  try {
    const response = await worker.fetch(
      await buildSignedWorkerRequest({ idleTimeoutSeconds: 0 }),
      buildWorkerEnv(),
      createTestContext().ctx,
    );

    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'used-ticket-ok');
    assert.deepEqual(originCalls, ['https://tenant.sharepoint.com/file']);
    assert.equal(ticketStateRpcState.readBodies.length, 1);
    assert.equal(ticketStateRpcState.markBodies.length, 1);
    assert.equal(
      ticketStateRpcState.markBodies[0].p_ticket_hash,
      ticketStateRpcState.readBodies[0].p_ticket_hash,
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('enabled mode accepts signed idle_timeout: 0 as present ticket-state data', async () => {
  const originalFetch = globalThis.fetch;
  const originCalls = [];

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

    if (url === 'https://tenant.sharepoint.com/file') {
      originCalls.push(url);
      return new Response('idle-timeout-zero-ok', {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '20',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };
  try {
    resetTicketStateRpcState();
    ticketStateRpcState.readHandler = (body) => createDefaultTicketStateRow(body, {
      issued_at: Math.floor(Date.now() / 1000),
      first_used_at: null,
    });

    const response = await worker.fetch(
      await buildSignedWorkerRequest({ idleTimeoutSeconds: 0 }),
      buildWorkerEnv(),
      createTestContext().ctx,
    );

    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'idle-timeout-zero-ok');
    assert.deepEqual(originCalls, ['https://tenant.sharepoint.com/file']);
    assert.equal(ticketStateRpcState.readBodies.length, 1);
    assert.equal(ticketStateRpcState.markBodies.length, 1);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('streamed 200 content without content-length still marks the ticket used before returning content', async () => {
  const originalFetch = globalThis.fetch;
  const originCalls = [];

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

    if (url === 'https://tenant.sharepoint.com/file') {
      originCalls.push(url);
      return new Response('streamed-content', {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(
      await buildSignedWorkerRequest(),
      buildWorkerEnv(),
      createTestContext().ctx,
    );

    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'streamed-content');
    assert.deepEqual(originCalls, ['https://tenant.sharepoint.com/file']);
    assert.equal(ticketStateRpcState.readBodies.length, 1);
    assert.equal(ticketStateRpcState.markBodies.length, 1);
    assert.equal(
      ticketStateRpcState.markBodies[0].p_ticket_hash,
      ticketStateRpcState.readBodies[0].p_ticket_hash,
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

for (const interstitialCase of [
  {
    label: 'text/html',
    body: '<html><body>warning</body></html>',
    contentType: 'text/html',
  },
  {
    label: 'text/plain',
    body: 'provider warning page',
    contentType: 'text/plain',
  },
]) {
  test(`GET 200 ${interstitialCase.label} interstitials still mark the ticket used before returning the response`, async () => {
    const originalFetch = globalThis.fetch;
    const originCalls = [];

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

      if (url === 'https://tenant.sharepoint.com/file') {
        originCalls.push(url);
        return new Response(interstitialCase.body, {
          status: 200,
          headers: {
            'content-type': `${interstitialCase.contentType};charset=UTF-8`,
            'content-length': String(Buffer.byteLength(interstitialCase.body)),
          },
        });
      }

      throw new Error(`Unexpected fetch URL in test: ${url}`);
    };

    try {
      const response = await worker.fetch(
        await buildSignedWorkerRequest(),
        buildWorkerEnv(),
        createTestContext().ctx,
      );

      assert.equal(response.status, 200);
      assert.equal(await response.text(), interstitialCase.body);
      assert.deepEqual(originCalls, ['https://tenant.sharepoint.com/file']);
      assert.equal(ticketStateRpcState.readBodies.length, 1);
      assert.equal(ticketStateRpcState.markBodies.length, 1);
      assert.equal(
        ticketStateRpcState.markBodies[0].p_ticket_hash,
        ticketStateRpcState.readBodies[0].p_ticket_hash,
      );
    } finally {
      globalThis.fetch = originalFetch;
      delete globalThis.bootstrapCache;
    }
  });
}

for (const syntheticTextAttachmentCase of [
  {
    label: 'text/html',
    body: '<html><body>warning</body></html>',
    contentType: 'text/html',
  },
  {
    label: 'text/plain',
    body: 'provider warning page',
    contentType: 'text/plain',
  },
]) {
  test(`crypted GET 200 ${syntheticTextAttachmentCase.label} wrappers with positive content-length still mark the ticket used`, async () => {
    const originalFetch = globalThis.fetch;
    const originCalls = [];

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

      if (url === 'https://tenant.sharepoint.com/file') {
        originCalls.push(url);
        return new Response(syntheticTextAttachmentCase.body, {
          status: 200,
          headers: {
            'content-type': `${syntheticTextAttachmentCase.contentType};charset=UTF-8`,
            'content-length': String(Buffer.byteLength(syntheticTextAttachmentCase.body)),
          },
        });
      }

      throw new Error(`Unexpected fetch URL in test: ${url}`);
    };

    try {
      resetTicketStateRpcState();

      const response = await worker.fetch(
        await buildSignedWorkerRequest({
          pathname: '/downloads/opaque',
          payloadMutator: (payloadObject) => ({
            ...payloadObject,
            isCrypted: true,
          }),
        }),
        buildWorkerEnv(),
        createTestContext().ctx,
      );

      assert.equal(response.status, 200);
      assert.equal(await response.text(), syntheticTextAttachmentCase.body);
      assert.deepEqual(originCalls, ['https://tenant.sharepoint.com/file']);
      assert.match(response.headers.get('content-disposition') || '', /^attachment;/i);
      assert.equal(response.headers.get('content-length'), String(Buffer.byteLength(syntheticTextAttachmentCase.body)));
      assert.equal(ticketStateRpcState.readBodies.length, 1);
      assert.equal(ticketStateRpcState.markBodies.length, 1);
      assert.equal(
        ticketStateRpcState.markBodies[0].p_ticket_hash,
        ticketStateRpcState.readBodies[0].p_ticket_hash,
      );
    } finally {
      globalThis.fetch = originalFetch;
      delete globalThis.bootstrapCache;
    }
  });
}

for (const textualPathCase of [
  {
    label: 'text/plain warning body on .txt request path',
    body: 'provider warning page',
    contentType: 'text/plain',
    pathname: '/downloads/readme.txt',
  },
  {
    label: 'text/html warning body on .html request path',
    body: '<html><body>warning</body></html>',
    contentType: 'text/html',
    pathname: '/downloads/index.html',
  },
]) {
  test(`GET 200 streamed ${textualPathCase.label} still marks the ticket used`, async () => {
    const originalFetch = globalThis.fetch;
    const originCalls = [];

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

      if (url === 'https://tenant.sharepoint.com/file') {
        originCalls.push(url);
        return new Response(textualPathCase.body, {
          status: 200,
          headers: {
            'content-type': `${textualPathCase.contentType};charset=UTF-8`,
          },
        });
      }

      throw new Error(`Unexpected fetch URL in test: ${url}`);
    };

    try {
      resetTicketStateRpcState();

      const response = await worker.fetch(
        await buildSignedWorkerRequest({ pathname: textualPathCase.pathname }),
        buildWorkerEnv(),
        createTestContext().ctx,
      );

      assert.equal(response.status, 200);
      assert.equal(await response.text(), textualPathCase.body);
      assert.deepEqual(originCalls, ['https://tenant.sharepoint.com/file']);
      assert.equal(ticketStateRpcState.readBodies.length, 1);
      assert.equal(ticketStateRpcState.markBodies.length, 1);
      assert.equal(
        ticketStateRpcState.markBodies[0].p_ticket_hash,
        ticketStateRpcState.readBodies[0].p_ticket_hash,
      );
    } finally {
      globalThis.fetch = originalFetch;
      delete globalThis.bootstrapCache;
    }
  });
}

for (const cryptedTextAttachmentCase of [
  {
    label: 'text/plain attachment-signaled csv',
    body: 'timestamp,value\n2026-05-01,42\n',
    contentType: 'text/plain',
  },
  {
    label: 'text/html attachment-signaled csv',
    body: '<html><body>csv export body</body></html>',
    contentType: 'text/html',
  },
]) {
  test(`crypted GET 200 ${cryptedTextAttachmentCase.label} responses mark the ticket used for opaque paths when upstream attachment proves file content`, async () => {
    const originalFetch = globalThis.fetch;
    const originCalls = [];

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

      if (url === 'https://tenant.sharepoint.com/file') {
        originCalls.push(url);
        return new Response(cryptedTextAttachmentCase.body, {
          status: 200,
          headers: {
            'content-type': `${cryptedTextAttachmentCase.contentType};charset=UTF-8`,
            'content-disposition': 'attachment; filename="report.csv"',
          },
        });
      }

      throw new Error(`Unexpected fetch URL in test: ${url}`);
    };

    try {
      resetTicketStateRpcState();

      const response = await worker.fetch(
        await buildSignedWorkerRequest({
          pathname: '/downloads/opaque',
          payloadMutator: (payloadObject) => ({
            ...payloadObject,
            isCrypted: true,
          }),
        }),
        buildWorkerEnv(),
        createTestContext().ctx,
      );

      assert.equal(response.status, 200);
      assert.equal(await response.text(), cryptedTextAttachmentCase.body);
      assert.deepEqual(originCalls, ['https://tenant.sharepoint.com/file']);
      assert.match(response.headers.get('content-disposition') || '', /^attachment;/i);
      assert.equal(ticketStateRpcState.readBodies.length, 1);
      assert.equal(ticketStateRpcState.markBodies.length, 1);
      assert.equal(
        ticketStateRpcState.markBodies[0].p_ticket_hash,
        ticketStateRpcState.readBodies[0].p_ticket_hash,
      );
    } finally {
      globalThis.fetch = originalFetch;
      delete globalThis.bootstrapCache;
    }
  });
}

for (const attachmentTextCase of [
  {
    label: 'text/plain attachment-signaled csv',
    body: 'timestamp,value\n2026-05-01,42\n',
    contentType: 'text/plain',
    contentDisposition: 'attachment; filename="report.csv"',
  },
  {
    label: 'text/html attachment-signaled csv',
    body: '<html><body>csv export body</body></html>',
    contentType: 'text/html',
    contentDisposition: 'attachment; filename="report.csv"',
  },
]) {
  test(`GET 200 chunked ${attachmentTextCase.label} responses mark the ticket used for opaque paths`, async () => {
    const originalFetch = globalThis.fetch;
    const originCalls = [];

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

      if (url === 'https://tenant.sharepoint.com/file') {
        originCalls.push(url);
        return new Response(attachmentTextCase.body, {
          status: 200,
          headers: {
            'content-type': `${attachmentTextCase.contentType};charset=UTF-8`,
            'content-disposition': attachmentTextCase.contentDisposition,
          },
        });
      }

      throw new Error(`Unexpected fetch URL in test: ${url}`);
    };

    try {
      resetTicketStateRpcState();

      const response = await worker.fetch(
        await buildSignedWorkerRequest({ pathname: '/downloads/opaque' }),
        buildWorkerEnv(),
        createTestContext().ctx,
      );

      assert.equal(response.status, 200);
      assert.equal(await response.text(), attachmentTextCase.body);
      assert.deepEqual(originCalls, ['https://tenant.sharepoint.com/file']);
      assert.equal(ticketStateRpcState.readBodies.length, 1);
      assert.equal(ticketStateRpcState.markBodies.length, 1);
      assert.equal(
        ticketStateRpcState.markBodies[0].p_ticket_hash,
        ticketStateRpcState.readBodies[0].p_ticket_hash,
      );
    } finally {
      globalThis.fetch = originalFetch;
      delete globalThis.bootstrapCache;
    }
  });
}

test('numeric-ish signed payload expireTime and idle_timeout fields are rejected before ticket-state read', async () => {
  const cases = [
    {
      label: 'expireTime string',
      expectedMessage: 'payload expire invalid',
      buildRequest: () => buildSignedWorkerRequest({
        payloadMutator: (payloadObject) => ({
          ...payloadObject,
          expireTime: String(payloadObject.expireTime),
        }),
      }),
    },
    {
      label: 'expireTime fractional number',
      expectedMessage: 'payload expire invalid',
      buildRequest: () => buildSignedWorkerRequest({
        payloadMutator: (payloadObject) => ({
          ...payloadObject,
          expireTime: payloadObject.expireTime + 0.5,
        }),
      }),
    },
    {
      label: 'idle_timeout string',
      expectedMessage: 'payload idle_timeout invalid',
      buildRequest: () => buildSignedWorkerRequest({
        payloadMutator: (payloadObject) => ({
          ...payloadObject,
          idle_timeout: String(payloadObject.idle_timeout),
        }),
      }),
    },
    {
      label: 'idle_timeout boolean',
      expectedMessage: 'payload idle_timeout invalid',
      buildRequest: () => buildSignedWorkerRequest({
        payloadMutator: (payloadObject) => ({
          ...payloadObject,
          idle_timeout: true,
        }),
      }),
    },
  ];

  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    calls.push(url);

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

    if (url === 'https://tenant.sharepoint.com/file') {
      throw new Error('origin fetch should not run for malformed signed numeric fields');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    for (const testCase of cases) {
      resetTicketStateRpcState();
      calls.length = 0;
      delete globalThis.bootstrapCache;

      const response = await worker.fetch(
        await testCase.buildRequest(),
        buildWorkerEnv(),
        createTestContext().ctx,
      );
      const body = await readJson(response);

      assert.equal(response.status, 401, testCase.label);
      assert.equal(body.message, testCase.expectedMessage, testCase.label);
      assert.equal(ticketStateRpcState.readBodies.length, 0, testCase.label);
      assert.equal(ticketStateRpcState.markBodies.length, 0, testCase.label);
      assert.equal(calls.includes('https://tenant.sharepoint.com/file'), false, testCase.label);
    }
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('missing ticket-state row is rejected as unauthorized protocol mismatch', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    calls.push(url);

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

    if (url === 'https://tenant.sharepoint.com/file') {
      throw new Error('origin fetch should not run when ticket state is missing');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };
  ticketStateRpcState.readHandler = () => ({
    found: false,
    ticket_hash: null,
    issued_at: null,
    first_used_at: null,
    hard_expire_at: null,
    ip_hash: null,
    path_hash: null,
  });

  try {
    const response = await worker.fetch(
      await buildSignedWorkerRequest(),
      buildWorkerEnv(),
      createTestContext().ctx,
    );
    const body = await readJson(response);

    assert.equal(response.status, 401);
    assert.equal(body.message, 'ticket state missing');
    assert.equal(ticketStateRpcState.readBodies.length, 1);
    assert.equal(ticketStateRpcState.markBodies.length, 0);
    assert.equal(calls.includes('https://tenant.sharepoint.com/file'), false);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('missing or malformed ticketNonce is rejected before ticket-state read', async () => {
  const cases = [
    {
      label: 'missing',
      buildRequest: () => buildSignedWorkerRequest({ omitTicketNonce: true }),
    },
    {
      label: 'malformed',
      buildRequest: () => buildSignedWorkerRequest({
        payloadMutator: (payloadObject) => ({
          ...payloadObject,
          ticketNonce: 'not-valid-$$',
        }),
      }),
    },
  ];

  for (const testCase of cases) {
    const originalFetch = globalThis.fetch;
    const calls = [];

    globalThis.fetch = async (input, init = {}) => {
      const url = typeof input === 'string' ? input : input.url;
      calls.push(url);

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

      if (url === 'https://tenant.sharepoint.com/file') {
        throw new Error(`origin fetch should not run for ${testCase.label} ticketNonce`);
      }

      throw new Error(`Unexpected fetch URL in test: ${url}`);
    };

    try {
      const response = await worker.fetch(
        await testCase.buildRequest(),
        buildWorkerEnv(),
        createTestContext().ctx,
      );
      const body = await readJson(response);

      assert.equal(response.status, 401);
      assert.equal(body.message, 'payload ticketNonce invalid');
      assert.equal(ticketStateRpcState.readBodies.length, 0);
      assert.equal(ticketStateRpcState.markBodies.length, 0);
      assert.equal(calls.includes('https://tenant.sharepoint.com/file'), false);
    } finally {
      globalThis.fetch = originalFetch;
      delete globalThis.bootstrapCache;
    }
  }
});

test('concurrent content responses treat already_used as success', async () => {
  const originalFetch = globalThis.fetch;
  let markResultCount = 0;

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

    if (url === 'https://tenant.sharepoint.com/file') {
      return new Response('concurrent-ok', {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '13',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };
  ticketStateRpcState.readHandler = (body) => createDefaultTicketStateRow(body, {
    issued_at: Math.floor(Date.now() / 1000),
    first_used_at: null,
  });
  ticketStateRpcState.markHandler = () => {
    markResultCount += 1;
    return {
      result: markResultCount === 1 ? 'transitioned' : 'already_used',
      first_used_at: Math.floor(Date.now() / 1000),
    };
  };

  try {
    const baseRequest = await buildSignedWorkerRequest();
    const { ctx: ctx1 } = createTestContext();
    const { ctx: ctx2 } = createTestContext();
    const [responseA, responseB] = await Promise.all([
      worker.fetch(new Request(baseRequest), buildWorkerEnv(), ctx1),
      worker.fetch(new Request(baseRequest), buildWorkerEnv(), ctx2),
    ]);

    assert.equal(responseA.status, 200);
    assert.equal(responseB.status, 200);
    assert.equal(await responseA.text(), 'concurrent-ok');
    assert.equal(await responseB.text(), 'concurrent-ok');
    assert.equal(ticketStateRpcState.readBodies.length, 2);
    assert.equal(ticketStateRpcState.markBodies.length, 2);
    assert.equal(
      ticketStateRpcState.markBodies[0].p_ticket_hash,
      ticketStateRpcState.markBodies[1].p_ticket_hash,
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('content response fails when mark ticket used returns storage_error', async () => {
  const originalFetch = globalThis.fetch;
  const originBody = createTrackedTextBody('should-not-be-released');

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

    if (url === 'https://tenant.sharepoint.com/file') {
      return new Response(originBody.stream, {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '19',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };
  ticketStateRpcState.markHandler = () => ({ result: 'storage_error' });

  try {
    const response = await worker.fetch(
      await buildSignedWorkerRequest(),
      buildWorkerEnv(),
      createTestContext().ctx,
    );
    const body = await readJson(response);

    assert.equal(response.status, 502);
    assert.equal(body.message, 'ticket state update failed');
    assert.equal(ticketStateRpcState.markBodies.length, 1);
    assert.equal(originBody.cancelled, true);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('fairqueue-only same-target redirect retires old state and reacquires a new slot', async () => {
  const originalFetch = globalThis.fetch;
  const fairQueueAcquireBodies = [];
  const fairQueueReleaseBodies = [];
  const originFetches = [];

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
          url: 'https://tenant.sharepoint.com/sites/demo/start-fq.bin',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      fairQueueAcquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: `query-fq-${fairQueueAcquireBodies.length}`,
        invocationEpoch: fairQueueAcquireBodies.length,
        slotToken: `slot-fq-${fairQueueAcquireBodies.length}`,
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      fairQueueReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://tenant.sharepoint.com/sites/demo/start-fq.bin') {
      originFetches.push(url);
      return new Response(null, {
        status: 302,
        headers: {
          Location: 'https://tenant.sharepoint.com/sites/demo/final-fq.bin?rotated=1',
        },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/demo/final-fq.bin?rotated=1') {
      originFetches.push(url);
      return new Response('fq-redirect-ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    assert.equal(await response.text(), 'fq-redirect-ok');
    await Promise.allSettled(waitUntilPromises);

    assert.equal(fairQueueAcquireBodies.length, 2);
    assert.deepEqual(originFetches, [
      'https://tenant.sharepoint.com/sites/demo/start-fq.bin',
      'https://tenant.sharepoint.com/sites/demo/final-fq.bin?rotated=1',
    ]);
    assert.deepEqual(
      fairQueueReleaseBodies.map((body) => body.slotToken),
      ['slot-fq-1', 'slot-fq-2'],
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('fairqueue-only refresh target rotation retires old state and reacquires a new slot', async () => {
  const originalFetch = globalThis.fetch;
  const fairQueueAcquireBodies = [];
  const fairQueueReleaseBodies = [];
  const originFetches = [];
  let linkRequestCount = 0;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const requestUrl = new URL(url);
      const isRefresh = requestUrl.searchParams.get('refresh') === 'true';
      linkRequestCount += 1;
      return createJsonResponse({
        code: 200,
        data: {
          url: isRefresh
            ? 'https://tenant.sharepoint.com/sites/demo/final-refresh-fq.bin?refreshed=1'
            : 'https://tenant.sharepoint.com/sites/demo/start-refresh-fq.bin',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      fairQueueAcquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: `query-refresh-fq-${fairQueueAcquireBodies.length}`,
        invocationEpoch: fairQueueAcquireBodies.length,
        slotToken: `slot-refresh-fq-${fairQueueAcquireBodies.length}`,
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      fairQueueReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://tenant.sharepoint.com/sites/demo/start-refresh-fq.bin') {
      originFetches.push(url);
      return new Response('auth expired', {
        status: 401,
        headers: { 'content-type': 'text/plain' },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/demo/final-refresh-fq.bin?refreshed=1') {
      originFetches.push(url);
      return new Response('fq-refresh-ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    assert.equal(await response.text(), 'fq-refresh-ok');
    await Promise.allSettled(waitUntilPromises);

    assert.equal(linkRequestCount, 2);
    assert.equal(fairQueueAcquireBodies.length, 2);
    assert.deepEqual(originFetches, [
      'https://tenant.sharepoint.com/sites/demo/start-refresh-fq.bin',
      'https://tenant.sharepoint.com/sites/demo/final-refresh-fq.bin?refreshed=1',
    ]);
    assert.deepEqual(
      fairQueueReleaseBodies.map((body) => body.slotToken),
      ['slot-refresh-fq-1', 'slot-refresh-fq-2'],
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency acquire success followed by origin fetch failure releases true concurrency first and fairqueue second', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://cq.example.test/api/v1/concurrency/precheck') {
      calls.push('precheck');
      return createJsonResponse({ result: 'allow' });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-1',
        invocationEpoch: 1,
        slotToken: 'slot-1',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-1',
        leaseToken: 'token-1',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-1',
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      throw new Error('origin fetch failed before response delivery');
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      return createJsonResponse({ result: 'ok' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    await Promise.allSettled(waitUntilPromises);
    const body = await readJson(response);
    assert.equal(response.status, 500);
    assert.match(body.message, /origin fetch failed/);
    assert.deepEqual(calls.slice(-2), ['concurrency-release', 'fairqueue-release']);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('dual mode attempts early fairqueue release after origin headers and falls back on final cleanup when it fails', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  let releaseAttempts = 0;
  let resolveEarlyRelease = () => {};
  const earlyReleaseGate = new Promise((resolve) => {
    resolveEarlyRelease = resolve;
  });

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://cq.example.test/api/v1/concurrency/precheck') {
      calls.push('precheck');
      return createJsonResponse({ result: 'allow' });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-1',
        invocationEpoch: 1,
        slotToken: 'slot-1',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-1',
        leaseToken: 'token-1',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-1',
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('streamed-body', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseAttempts += 1;
      calls.push(`fairqueue-release-${releaseAttempts}`);
      if (releaseAttempts === 1) {
        await earlyReleaseGate;
        return new Response('fail once', { status: 500 });
      }
      return createJsonResponse({ result: 'ok' });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const responsePromise = worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const settledBeforeEarlyReleaseCompletes = await Promise.race([
      responsePromise.then(() => 'resolved'),
      new Promise((resolve) => setTimeout(() => resolve('waiting'), 50)),
    ]);
    assert.equal(settledBeforeEarlyReleaseCompletes, 'resolved');
    resolveEarlyRelease();
    const response = await responsePromise;
    assert.equal(await response.text(), 'streamed-body');
    assert.equal(response.status, 200);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(calls.includes('fairqueue-release-1'), true);
    assert.equal(calls.includes('fairqueue-release-2'), true);
  } finally {
    resolveEarlyRelease();
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency managed streaming releases after body completion and does not return the upstream body directly', async () => {
  const originalFetch = globalThis.fetch;
  let upstreamBody = null;
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-1',
        leaseToken: 'token-1',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-1',
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      upstreamBody = new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode('managed-stream-body'));
          controller.close();
        },
      });
      return new Response(upstreamBody, {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    assert.notEqual(response.body, upstreamBody);
    assert.equal(await response.text(), 'managed-stream-body');
    await new Promise((resolve) => setTimeout(resolve, 0));
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(calls, ['concurrency-acquire', 'concurrency-release']);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency managed streaming reconnects after heartbeat_ack timeout using hello_ack timing and keeps the stream active', async () => {
  const originalFetch = globalThis.fetch;
  const heartbeatSockets = [];
  let heartbeatUpgradeCount = 0;
  let releaseBody = null;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHeartbeatOverrides: {
          intervalMs: 1000,
          ackTimeoutMs: 1000,
          helloTimeoutMs: 5,
          startTimeoutMs: 50,
          initialConnectMaxElapsedMs: 5,
          reconnectSafetyMarginMs: 1,
          reconnectMaxAttempts: 2,
          reconnectMaxElapsedMs: 50,
          reconnectBaseDelayMs: 1,
          reconnectMaxDelayMs: 1,
        },
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-heartbeat-reconnect',
        leaseToken: 'token-heartbeat-reconnect',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-heartbeat-reconnect',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === ACK_HANDOFF_URL) {
      return createAckHandoffResponse({ result: 'acknowledged' });
    }

    if (url === HEARTBEAT_URL) {
      heartbeatUpgradeCount += 1;
      const generation = heartbeatUpgradeCount;
      const socket = createFakeHeartbeatSocket({
        helloAck: buildHeartbeatHelloAck({
          generation,
          heartbeatIntervalMs: 1,
          ackTimeoutMs: 5,
          reconnectGraceMs: 40,
          deadlineMs: Date.now() + 1000,
          hardExpireAtMs: Date.now() + 1000,
        }),
        heartbeatAck: generation === 1 ? null : buildHeartbeatAck(generation, {
          deadlineMs: Date.now() + 1000,
          hardExpireAtMs: Date.now() + 1000,
        }),
      });
      heartbeatSockets.push(socket);
      return {
        status: 101,
        webSocket: socket,
      };
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      return new Response(new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode('reconnect-ok'));
          setTimeout(() => controller.close(), 25);
        },
      }), {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      releaseBody = JSON.parse(init.body);
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    await waitForCondition(() => heartbeatUpgradeCount === 2, {
      message: 'expected reconnect after missing heartbeat_ack',
    });
    assert.equal(await response.text(), 'reconnect-ok');
    await Promise.allSettled(waitUntilPromises);

    assert.equal(releaseBody?.reason, 'stream_complete');
    assert.equal(heartbeatUpgradeCount, 2);
    const firstHeartbeat = JSON.parse(heartbeatSockets[0].sent[1]);
    const secondHeartbeat = JSON.parse(heartbeatSockets[1].sent[1]);
    assert.equal(firstHeartbeat.type, 'heartbeat');
    assert.equal(secondHeartbeat.type, 'heartbeat');
    assert.equal(Object.hasOwn(firstHeartbeat, 'downloadedBytes'), false);
    assert.equal(Object.hasOwn(secondHeartbeat, 'downloadedBytes'), false);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency managed streaming aborts after websocket close exhausts reconnect budget and releases heartbeat_lost', async () => {
  const originalFetch = globalThis.fetch;
  const heartbeatSockets = [];
  let heartbeatUpgradeCount = 0;
  let releaseBody = null;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHeartbeatOverrides: {
          helloTimeoutMs: 5,
          startTimeoutMs: 50,
          initialConnectMaxElapsedMs: 5,
          reconnectSafetyMarginMs: 1,
          reconnectMaxAttempts: 2,
          reconnectMaxElapsedMs: 20,
          reconnectBaseDelayMs: 1,
          reconnectMaxDelayMs: 1,
        },
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-heartbeat-close',
        leaseToken: 'token-heartbeat-close',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-heartbeat-close',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === ACK_HANDOFF_URL) {
      return createAckHandoffResponse({ result: 'acknowledged' });
    }

    if (url === HEARTBEAT_URL) {
      heartbeatUpgradeCount += 1;
      if (heartbeatUpgradeCount === 1) {
        const socket = createFakeHeartbeatSocket({
          helloAck: buildHeartbeatHelloAck({
            heartbeatIntervalMs: 10,
            ackTimeoutMs: 10,
            reconnectGraceMs: 30,
            deadlineMs: Date.now() + 1000,
            hardExpireAtMs: Date.now() + 1000,
          }),
        });
        heartbeatSockets.push(socket);
        return {
          status: 101,
          webSocket: socket,
        };
      }
      return { status: 503 };
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      return new Response(new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode('close-budget'));
        },
      }), {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      releaseBody = JSON.parse(init.body);
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const reader = response.body.getReader();
    const firstChunk = await reader.read();
    assert.equal(new TextDecoder().decode(firstChunk.value), 'close-budget');
    heartbeatSockets[0].emitClose(1006, 'network_lost');
    await waitForCondition(() => releaseBody !== null, {
      timeoutMs: 150,
      message: 'expected heartbeat_lost release after reconnect exhaustion',
    });
    await assert.rejects(() => reader.read(), /abort/i);
    await Promise.allSettled(waitUntilPromises);

    assert.equal(releaseBody?.reason, 'heartbeat_lost');
    assert.equal(heartbeatUpgradeCount, 3);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency heartbeat terminal reasons map duplicate-release suppression and heartbeat_lost fallback', async () => {
  const originalFetch = globalThis.fetch;
  const scenarios = [
    { reason: 'heartbeat_timeout', expectRelease: false },
    { reason: 'heartbeat_start_timeout', expectRelease: false },
    { reason: 'hard_expired', expectRelease: false },
    { reason: 'already_released', expectRelease: false },
    { reason: 'request_cancelled', expectRelease: false },
    { reason: 'token_mismatch', expectRelease: true },
    { reason: 'protocol_error', expectRelease: true },
  ];

  try {
    for (const scenario of scenarios) {
      let releaseBody = null;
      const heartbeatSocket = createFakeHeartbeatSocket({
        helloAck: buildHeartbeatHelloAck({
          heartbeatIntervalMs: 1,
          ackTimeoutMs: 5,
          reconnectGraceMs: 40,
          deadlineMs: Date.now() + 1000,
          hardExpireAtMs: Date.now() + 1000,
        }),
        heartbeatAck: null,
        onSend(payload, { emitJsonMessage }) {
          if (payload.type === 'heartbeat') {
            emitJsonMessage({
              type: 'terminal',
              result: scenario.expectRelease ? 'conflict' : 'released',
              reason: scenario.reason,
            });
          }
        },
      });

      globalThis.fetch = async (input, init = {}) => {
        const url = typeof input === 'string' ? input : input.url;

        if (url === 'https://controller.example.test/api/v0/bootstrap') {
          return createJsonResponse(buildRuntimeBootstrap({
            trueConcurrencyHostPatterns: ['*.sharepoint.com'],
            trueConcurrencyHeartbeatOverrides: {
              intervalMs: 1000,
              ackTimeoutMs: 1000,
              helloTimeoutMs: 5,
              startTimeoutMs: 50,
              initialConnectMaxElapsedMs: 5,
              reconnectSafetyMarginMs: 1,
            },
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

        if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
          const body = JSON.parse(init.body);
          return createJsonResponse({
            result: 'granted',
            leaseId: `lease-terminal-${scenario.reason}`,
            leaseToken: `token-terminal-${scenario.reason}`,
            expiresAtMs: body.hardExpireAtMs,
            claimToken: `claim-token-terminal-${scenario.reason}`,
          });
        }

        if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
          return createClaimGrantResponseFromRequest(init);
        }

        if (url === ACK_HANDOFF_URL) {
          return createAckHandoffResponse({ result: 'acknowledged' });
        }

        if (url === HEARTBEAT_URL) {
          return {
            status: 101,
            webSocket: heartbeatSocket,
          };
        }

        if (url === 'https://tenant.sharepoint.com/file') {
          return new Response(new ReadableStream({
            start(controller) {
              controller.enqueue(new TextEncoder().encode(`terminal-${scenario.reason}`));
            },
          }), {
            status: 200,
            headers: { 'content-type': 'application/octet-stream' },
          });
        }

        if (url === 'https://cq.example.test/api/v1/concurrency/release') {
          releaseBody = JSON.parse(init.body);
          return createJsonResponse({ result: 'released' });
        }

        throw new Error(`Unexpected fetch URL in test: ${url}`);
      };

      delete globalThis.bootstrapCache;
      const { ctx, waitUntilPromises } = createTestContext();
      const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
      const reader = response.body.getReader();
      const firstChunk = await reader.read();
      assert.equal(new TextDecoder().decode(firstChunk.value), `terminal-${scenario.reason}`);
      const terminalResult = await Promise.race([
        reader.read().then(
          () => 'resolved',
          (error) => error,
        ),
        new Promise((resolve) => setTimeout(() => resolve('timeout'), 150)),
      ]);
      await Promise.allSettled(waitUntilPromises);

      assert.notEqual(terminalResult, 'timeout', scenario.reason);
      assert.match(String(terminalResult?.message ?? terminalResult), /abort/i, scenario.reason);
      if (scenario.expectRelease) {
        assert.equal(releaseBody?.reason, 'heartbeat_lost', scenario.reason);
      } else {
        assert.equal(releaseBody, null, scenario.reason);
      }
    }
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency managed streaming releases upstream_failure after a mid-stream body error and closes heartbeat cleanup', async () => {
  const originalFetch = globalThis.fetch;
  let releaseBody = null;
  const heartbeatSocket = createFakeHeartbeatSocket();

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-upstream-failure',
        leaseToken: 'token-upstream-failure',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-upstream-failure',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === ACK_HANDOFF_URL) {
      return createAckHandoffResponse({ result: 'acknowledged' });
    }

    if (url === HEARTBEAT_URL) {
      return {
        status: 101,
        webSocket: heartbeatSocket,
      };
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      let sentChunk = false;
      return new Response(new ReadableStream({
        pull(controller) {
          if (!sentChunk) {
            sentChunk = true;
            controller.enqueue(new TextEncoder().encode('upstream-first-chunk'));
            return;
          }
          controller.error(new Error('origin stream exploded'));
        },
      }), {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      releaseBody = JSON.parse(init.body);
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const reader = response.body.getReader();
    const firstChunk = await reader.read();
    assert.equal(new TextDecoder().decode(firstChunk.value), 'upstream-first-chunk');
    await assert.rejects(() => reader.read(), /origin stream exploded/);
    await Promise.allSettled(waitUntilPromises);

    assert.equal(releaseBody?.reason, 'upstream_failure');
    assert.equal(heartbeatSocket.getCloseArgs()?.reason, 'upstream_failure');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency managed streaming prefers IdentityTransformStream fallback and releases on completion', async () => {
  const originalFetch = globalThis.fetch;
  const originalIdentityTransformStream = globalThis.IdentityTransformStream;
  const identityCalls = [];
  const calls = [];

  globalThis.IdentityTransformStream = class FakeIdentityTransformStream {
    constructor() {
      identityCalls.push('identity-transform');
      const inner = new TransformStream();
      this.writable = inner.writable;
      this.readable = inner.readable;
    }
  };

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-identity-1',
        leaseToken: 'token-identity-1',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-identity-1',
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('identity-stream-body', {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': 'not-a-number',
        },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    assert.equal(response.headers.get('content-length'), null);
    assert.equal(await response.text(), 'identity-stream-body');
    await new Promise((resolve) => setTimeout(resolve, 0));
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(identityCalls, ['identity-transform']);
    assert.deepEqual(calls, ['concurrency-acquire', 'origin-fetch', 'concurrency-release']);
  } finally {
    globalThis.fetch = originalFetch;
    globalThis.IdentityTransformStream = originalIdentityTransformStream;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency managed streaming binds CQ cleanup to waitUntil for post-return client cancellation', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const heartbeatSocket = createFakeHeartbeatSocket();

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-waituntil-1',
        leaseToken: 'token-waituntil-1',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-waituntil-1',
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      return new Response(new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode('managed-stream-chunk'));
        },
      }), {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === HEARTBEAT_URL) {
      return {
        status: 101,
        webSocket: heartbeatSocket,
      };
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    assert.equal(waitUntilPromises.length, 1);

    const reader = response.body.getReader();
    const firstChunk = await reader.read();
    assert.equal(new TextDecoder().decode(firstChunk.value), 'managed-stream-chunk');
    await reader.cancel('client closed download');

    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(calls, ['concurrency-acquire', 'concurrency-release']);
    assert.equal(heartbeatSocket.getCloseArgs()?.reason, 'client_disconnect');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency header-only response releases immediately', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-1',
        leaseToken: 'token-1',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-1',
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      return new Response(null, {
        status: 204,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    assert.equal(response.status, 204);
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(calls, ['concurrency-acquire', 'concurrency-release']);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('concurrency release controller retries immediate then 2s 4s 8s', async () => {
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  const delays = [];
  const reasons = [];
  let timeoutId = 0;

  globalThis.setTimeout = (callback, delay = 0, ...args) => {
    delays.push(delay);
    Promise.resolve().then(() => callback(...args));
    timeoutId += 1;
    return timeoutId;
  };
  globalThis.clearTimeout = () => {};

  try {
    const controller = createConcurrencyReleaseController({
      client: {
        async release(_ctx, _lease, reason) {
          reasons.push(reason);
          throw new Error('release failed');
        },
      },
      ctx: {
        waitUntil() {},
      },
      lease: {
        leaseId: 'lease-1',
        leaseToken: 'token-1',
      },
      label: 'test-lease',
    });

    await controller.ensureReleased('stream_complete');
    assert.deepEqual(reasons, ['stream_complete', 'stream_complete', 'stream_complete', 'stream_complete']);
    assert.deepEqual(delays, [2000, 4000, 8000]);
  } finally {
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
  }
});

test('concurrency release controller is single-flight across repeated triggers', async () => {
  const reasons = [];
  const controller = createConcurrencyReleaseController({
    client: {
      async release(_ctx, _lease, reason) {
        reasons.push(reason);
        return { result: 'released' };
      },
    },
    ctx: { waitUntil() {} },
    lease: {
      leaseId: 'lease-1',
      leaseToken: 'token-1',
    },
    label: 'single-flight-test',
  });

  await Promise.all([
    controller.ensureReleased('stream_complete'),
    controller.releaseImmediately('client_disconnect'),
    controller.ensureReleased('hard_expiry'),
  ]);

  assert.deepEqual(reasons, ['stream_complete']);
});

test('concurrency release controller treats direct expired release as settled', async () => {
  const reasons = [];
  const controller = createConcurrencyReleaseController({
    client: {
      async release(_ctx, _lease, reason) {
        reasons.push(reason);
        return { result: 'expired', reason: 'hard_expired' };
      },
    },
    ctx: { waitUntil() {} },
    lease: {
      leaseId: 'lease-1',
      leaseToken: 'token-1',
    },
    label: 'expired-terminal-test',
  });

  const released = await controller.releaseImmediately('stream_complete');

  assert.equal(released, true);
  assert.equal(controller.isSettled(), true);
  assert.deepEqual(reasons, ['stream_complete']);
});

test('heartbeat manager fails closed when initial connect returns after the elapsed budget is already exhausted', async () => {
  const originalDateNow = Date.now;
  let nowMs = 10_000;
  const hardExpireAtMs = nowMs + 60_000;
  const { session, getCloseReason } = createHeartbeatManagerSession({ hardExpireAtMs });
  let manager = null;

  Date.now = () => nowMs;

  try {
    manager = createTrueConcurrencyHeartbeatManager({
      client: {
        async connectHeartbeat() {
          nowMs += 25;
          return session;
        },
      },
      ctx: createTestContext().ctx,
      plan: {
        requestId: 'request-late-heartbeat',
        hardExpireAtMs,
        clientInstanceId: 'worker-test',
      },
      lease: {
        leaseId: 'lease-late-heartbeat',
        leaseToken: 'token-late-heartbeat',
      },
      heartbeatConfig: {
        ...DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
        initialConnectMaxAttempts: 1,
        initialConnectMaxElapsedMs: 20,
        reconnectSafetyMarginMs: 1,
      },
      clientSignal: new AbortController().signal,
      abortStream() {},
    });

    await assert.rejects(() => manager.startBeforeOriginFetch());
    assert.equal(getCloseReason(), 'heartbeat_connect_failed');
  } finally {
    Date.now = originalDateNow;
    await manager?.ensureCleanup('test_cleanup');
  }
});

test('heartbeat manager fails closed when pending initial connect exhausts the elapsed budget', async () => {
  const originalDateNow = Date.now;
  let nowMs = 11_000;
  const hardExpireAtMs = nowMs + 60_000;
  let manager = null;
  let startPromise = null;
  let connectSignals = [];

  Date.now = () => nowMs;

  try {
    manager = createTrueConcurrencyHeartbeatManager({
      client: {
        connectHeartbeat(_ctx, _identity, signal) {
          connectSignals.push(signal);
          return new Promise((resolve, reject) => {
            signal?.addEventListener?.('abort', () => reject(new Error('connect aborted')), { once: true });
          });
        },
      },
      ctx: createTestContext().ctx,
      plan: {
        requestId: 'request-pending-initial-heartbeat',
        hardExpireAtMs,
        clientInstanceId: 'worker-test',
      },
      lease: {
        leaseId: 'lease-pending-initial-heartbeat',
        leaseToken: 'token-pending-initial-heartbeat',
      },
      heartbeatConfig: {
        ...DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
        initialConnectMaxAttempts: 1,
        initialConnectMaxElapsedMs: 5,
        reconnectSafetyMarginMs: 1,
      },
      clientSignal: new AbortController().signal,
      abortStream() {},
    });

    startPromise = manager.startBeforeOriginFetch();
    startPromise.catch(() => {});
    const outcome = await Promise.race([
      startPromise.then(
        () => ({ status: 'resolved' }),
        (error) => ({ status: 'rejected', error }),
      ),
      new Promise((resolve) => setTimeout(() => resolve({ status: 'pending' }), 30)),
    ]);

    assert.equal(outcome.status, 'rejected');
    assert.match(outcome.error?.message || '', /heartbeat initial connect/i);
    assert.equal(connectSignals.length, 1);
    assert.equal(connectSignals[0]?.aborted, true);
  } finally {
    Date.now = originalDateNow;
    await manager?.ensureCleanup('test_cleanup');
    await startPromise?.catch(() => {});
  }
});

test('heartbeat manager clamps reconnect waits to the remaining reconnect budget before sleeping', async () => {
  const originalDateNow = Date.now;
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  let nowMs = 20_000;
  const hardExpireAtMs = nowMs + 60_000;
  const reconnectDelays = [];
  const abortReasons = [];
  let connectCalls = 0;
  let manager = null;
  const { session, socket } = createHeartbeatManagerSession({
    heartbeatIntervalMs: 1000,
    reconnectGraceMs: 2000,
    hardExpireAtMs,
  });

  Date.now = () => nowMs;
  globalThis.setTimeout = (callback, delay = 0, ...args) => {
    const handle = { cleared: false };
    if (delay < 1000) {
      reconnectDelays.push(delay);
      Promise.resolve().then(() => {
        if (handle.cleared) {
          return;
        }
        nowMs += delay;
        callback(...args);
      });
    }
    return handle;
  };
  globalThis.clearTimeout = (handle) => {
    if (handle && typeof handle === 'object') {
      handle.cleared = true;
    }
  };

  try {
    manager = createTrueConcurrencyHeartbeatManager({
      client: {
        async connectHeartbeat() {
          connectCalls += 1;
          if (connectCalls === 1) {
            return session;
          }
          throw new Error('reconnect failed');
        },
      },
      ctx: createTestContext().ctx,
      plan: {
        requestId: 'request-reconnect-clamp',
        hardExpireAtMs,
        clientInstanceId: 'worker-test',
      },
      lease: {
        leaseId: 'lease-reconnect-clamp',
        leaseToken: 'token-reconnect-clamp',
      },
      heartbeatConfig: {
        ...DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
        reconnectGraceMs: 1000,
        reconnectMaxAttempts: 2,
        reconnectMaxElapsedMs: 10,
        reconnectBaseDelayMs: 50,
        reconnectMaxDelayMs: 50,
        reconnectSafetyMarginMs: 1,
      },
      clientSignal: new AbortController().signal,
      abortStream(reason) {
        abortReasons.push(reason);
      },
    });

    await manager.startBeforeOriginFetch();
    socket.emitClose(1000, 'simulated_disconnect');
    await Promise.resolve();
    await new Promise((resolve) => originalSetTimeout(resolve, 0));

    assert.deepEqual(reconnectDelays, [10]);
    assert.deepEqual(abortReasons, ['heartbeat_lost']);
  } finally {
    Date.now = originalDateNow;
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
    await manager?.ensureCleanup('test_cleanup');
  }
});

test('heartbeat manager applies jittered exponential backoff between reconnect attempts', async () => {
  const originalDateNow = Date.now;
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  const originalMathRandom = Math.random;
  let nowMs = 30_000;
  const hardExpireAtMs = nowMs + 60_000;
  const reconnectDelays = [];
  const abortReasons = [];
  let connectCalls = 0;
  let manager = null;
  const { session, socket } = createHeartbeatManagerSession({
    heartbeatIntervalMs: 1000,
    reconnectGraceMs: 1000,
    hardExpireAtMs,
  });

  Date.now = () => nowMs;
  Math.random = () => 0.5;
  globalThis.setTimeout = (callback, delay = 0, ...args) => {
    const handle = { cleared: false };
    if (delay < 1000) {
      reconnectDelays.push(delay);
      Promise.resolve().then(() => {
        if (handle.cleared) {
          return;
        }
        nowMs += delay;
        callback(...args);
      });
    }
    return handle;
  };
  globalThis.clearTimeout = (handle) => {
    if (handle && typeof handle === 'object') {
      handle.cleared = true;
    }
  };

  try {
    manager = createTrueConcurrencyHeartbeatManager({
      client: {
        async connectHeartbeat() {
          connectCalls += 1;
          if (connectCalls === 1) {
            return session;
          }
          throw new Error('reconnect failed');
        },
      },
      ctx: createTestContext().ctx,
      plan: {
        requestId: 'request-reconnect-jitter',
        hardExpireAtMs,
        clientInstanceId: 'worker-test',
      },
      lease: {
        leaseId: 'lease-reconnect-jitter',
        leaseToken: 'token-reconnect-jitter',
      },
      heartbeatConfig: {
        ...DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
        reconnectGraceMs: 2000,
        reconnectMaxAttempts: 3,
        reconnectMaxElapsedMs: 900,
        reconnectBaseDelayMs: 10,
        reconnectMaxDelayMs: 100,
        reconnectSafetyMarginMs: 1,
      },
      clientSignal: new AbortController().signal,
      abortStream(reason) {
        abortReasons.push(reason);
      },
    });

    await manager.startBeforeOriginFetch();
    socket.emitClose(1000, 'simulated_disconnect');
    await Promise.resolve();
    await new Promise((resolve) => originalSetTimeout(resolve, 0));

    assert.deepEqual(reconnectDelays, [15, 30]);
    assert.deepEqual(abortReasons, ['heartbeat_lost']);
  } finally {
    Date.now = originalDateNow;
    Math.random = originalMathRandom;
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
    await manager?.ensureCleanup('test_cleanup');
  }
});

test('heartbeat manager fails closed when reconnect returns after the reconnect budget is already exhausted', async () => {
  const originalDateNow = Date.now;
  let nowMs = 40_000;
  const hardExpireAtMs = nowMs + 60_000;
  const abortReasons = [];
  let connectCalls = 0;
  let manager = null;
  const initial = createHeartbeatManagerSession({
    heartbeatIntervalMs: 1000,
    reconnectGraceMs: 1000,
    hardExpireAtMs,
  });
  const lateReconnect = createHeartbeatManagerSession({
    generation: 2,
    heartbeatIntervalMs: 1000,
    reconnectGraceMs: 1000,
    hardExpireAtMs,
  });

  Date.now = () => nowMs;

  try {
    manager = createTrueConcurrencyHeartbeatManager({
      client: {
        async connectHeartbeat() {
          connectCalls += 1;
          if (connectCalls === 1) {
            return initial.session;
          }
          nowMs += 25;
          return lateReconnect.session;
        },
      },
      ctx: createTestContext().ctx,
      plan: {
        requestId: 'request-reconnect-late-success',
        hardExpireAtMs,
        clientInstanceId: 'worker-test',
      },
      lease: {
        leaseId: 'lease-reconnect-late-success',
        leaseToken: 'token-reconnect-late-success',
      },
      heartbeatConfig: {
        ...DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
        reconnectGraceMs: 1000,
        reconnectMaxAttempts: 2,
        reconnectMaxElapsedMs: 20,
        reconnectBaseDelayMs: 1,
        reconnectMaxDelayMs: 1,
        reconnectSafetyMarginMs: 1,
      },
      clientSignal: new AbortController().signal,
      abortStream(reason) {
        abortReasons.push(reason);
      },
    });

    await manager.startBeforeOriginFetch();
    initial.socket.emitClose(1000, 'simulated_disconnect');
    await Promise.resolve();
    await new Promise((resolve) => setTimeout(resolve, 0));

    assert.equal(lateReconnect.getCloseReason(), 'heartbeat_lost');
    assert.deepEqual(abortReasons, ['heartbeat_lost']);
  } finally {
    Date.now = originalDateNow;
    await manager?.ensureCleanup('test_cleanup');
  }
});

test('heartbeat manager aborts when pending reconnect exhausts the reconnect budget', async () => {
  const originalDateNow = Date.now;
  let nowMs = 45_000;
  const hardExpireAtMs = nowMs + 60_000;
  const abortReasons = [];
  let connectCalls = 0;
  let reconnectSignal = null;
  let manager = null;
  const initial = createHeartbeatManagerSession({
    heartbeatIntervalMs: 1000,
    reconnectGraceMs: 1000,
    hardExpireAtMs,
  });

  Date.now = () => nowMs;

  try {
    manager = createTrueConcurrencyHeartbeatManager({
      client: {
        connectHeartbeat(_ctx, _identity, signal) {
          connectCalls += 1;
          if (connectCalls === 1) {
            return initial.session;
          }
          reconnectSignal = signal;
          return new Promise((resolve, reject) => {
            signal?.addEventListener?.('abort', () => reject(new Error('reconnect aborted')), { once: true });
          });
        },
      },
      ctx: createTestContext().ctx,
      plan: {
        requestId: 'request-pending-reconnect-heartbeat',
        hardExpireAtMs,
        clientInstanceId: 'worker-test',
      },
      lease: {
        leaseId: 'lease-pending-reconnect-heartbeat',
        leaseToken: 'token-pending-reconnect-heartbeat',
      },
      heartbeatConfig: {
        ...DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
        reconnectGraceMs: 1000,
        reconnectMaxAttempts: 2,
        reconnectMaxElapsedMs: 5,
        reconnectBaseDelayMs: 1,
        reconnectMaxDelayMs: 1,
        reconnectSafetyMarginMs: 1,
      },
      clientSignal: new AbortController().signal,
      abortStream(reason) {
        abortReasons.push(reason);
      },
    });

    await manager.startBeforeOriginFetch();
    initial.socket.emitClose(1000, 'simulated_disconnect');
    await new Promise((resolve) => setTimeout(resolve, 30));

    assert.equal(connectCalls, 2);
    assert.equal(reconnectSignal?.aborted, true);
    assert.deepEqual(abortReasons, ['heartbeat_lost']);
  } finally {
    Date.now = originalDateNow;
    await manager?.ensureCleanup('test_cleanup');
  }
});

test('true concurrency managed streaming releases on client abort', async () => {
  const originalFetch = globalThis.fetch;
  const abortController = new AbortController();
  const calls = [];
  const heartbeatSocket = createFakeHeartbeatSocket();

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-1',
        leaseToken: 'token-1',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-1',
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      return new Response(new ReadableStream({
        start() {},
      }), {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === HEARTBEAT_URL) {
      return {
        status: 101,
        webSocket: heartbeatSocket,
      };
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const request = await buildSignedWorkerRequest({ signal: abortController.signal });
    const response = await worker.fetch(request, buildWorkerEnv(), createTestContext().ctx);
    const reader = response.body.getReader();
    abortController.abort();
    await assert.rejects(() => reader.read(), /aborted/i);
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.deepEqual(calls, ['concurrency-acquire', 'concurrency-release']);
    assert.equal(heartbeatSocket.getCloseArgs()?.reason, 'client_disconnect');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency acquire abort returns 499 client abort response', async () => {
  const originalFetch = globalThis.fetch;
  const abortController = new AbortController();
  const calls = [];
  let resolveAcquireStarted;
  const acquireStarted = new Promise((resolve) => {
    resolveAcquireStarted = resolve;
  });

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      resolveAcquireStarted();
      return new Promise((_resolve, reject) => {
        const abortError = Object.assign(new Error('This operation was aborted'), { name: 'AbortError' });
        if (init.signal?.aborted) {
          reject(abortError);
          return;
        }
        init.signal?.addEventListener('abort', () => reject(abortError), { once: true });
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      throw new Error('origin fetch should not run after acquire-stage client abort');
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const workerPromise = worker.fetch(
      await buildSignedWorkerRequest({ signal: abortController.signal }),
      buildWorkerEnv(),
      createTestContext().ctx,
    );

    await acquireStarted;
    abortController.abort();

    const response = await workerPromise;
    const body = await readJson(response);

    assert.equal(response.status, 499);
    assert.equal(body.message, 'client aborted request');
    assert.deepEqual(calls, ['concurrency-acquire']);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency managed streaming releases when client is already aborted at bind time', async () => {
  const originalFetch = globalThis.fetch;
  const abortController = new AbortController();
  const calls = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-1',
        leaseToken: 'token-1',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-1',
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      const response = new Response(new ReadableStream({
        start() {},
      }), {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
      abortController.abort();
      return response;
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const request = await buildSignedWorkerRequest({ signal: abortController.signal });
    const response = await worker.fetch(request, buildWorkerEnv(), createTestContext().ctx);
    const reader = response.body.getReader();
    const readResult = await Promise.race([
      reader.read().then(
        () => 'resolved',
        (error) => error,
      ),
      new Promise((resolve) => setTimeout(() => resolve('timeout'), 50)),
    ]);

    assert.notEqual(readResult, 'timeout');
    assert.match(String(readResult?.message ?? readResult), /abort/i);
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.deepEqual(calls, ['concurrency-acquire', 'concurrency-release']);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency managed streaming releases on hard-expiry cutoff', async () => {
  const originalFetch = globalThis.fetch;
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  const heartbeatSocket = createFakeHeartbeatSocket();
  let acquireCalled = false;
  let releaseCalled = false;
  let releaseBody = null;
  let hardExpireAtMs = 0;
  const timeoutCallbacks = [];
  const { ctx, waitUntilPromises } = createTestContext();
  let pulledResolve;
  const pulledPromise = new Promise((resolve) => {
    pulledResolve = resolve;
  });

  globalThis.setTimeout = (callback, delay, ...args) => {
    if (delay <= 2_000) {
      const handle = { cleared: false };
      timeoutCallbacks.push(() => {
        if (!handle.cleared) callback(...args);
      });
      return handle;
    }
    return originalSetTimeout(callback, delay, ...args);
  };
  globalThis.clearTimeout = (handle) => {
    if (handle && typeof handle === 'object' && 'cleared' in handle) {
      handle.cleared = true;
      return;
    }
    originalClearTimeout(handle);
  };

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      acquireCalled = true;
      const body = JSON.parse(init.body);
      hardExpireAtMs = body.hardExpireAtMs;
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-1',
        leaseToken: 'token-1',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-1',
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      let sentChunk = false;
      return new Response(new ReadableStream({
        pull(controller) {
          if (!sentChunk) {
            sentChunk = true;
            controller.enqueue(new TextEncoder().encode('slow-body'));
            pulledResolve();
          }
        },
      }), {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-1',
        leaseToken: 'token-1',
        expiresAtMs: hardExpireAtMs,
        handoffToken: 'handoff-1',
        handoffDeadlineMs: hardExpireAtMs - 1,
      });
    }

    if (url === HEARTBEAT_URL) {
      return {
        status: 101,
        webSocket: heartbeatSocket,
      };
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      releaseCalled = true;
      releaseBody = JSON.parse(init.body);
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const request = await buildSignedWorkerRequest({
      expireOffsetSeconds: 1,
      payloadExpireTime: Math.floor(Date.now() / 1000) + 1,
    });
    const response = await worker.fetch(request, buildWorkerEnv(), ctx);
    const reader = response.body.getReader();
    const firstRead = reader.read();
    await pulledPromise;
    await firstRead;
    timeoutCallbacks.splice(0).forEach((callback) => callback());
    await assert.rejects(() => reader.read(), /aborted/i);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(releaseBody.reason, 'hard_expiry');
    assert.equal(acquireCalled, true);
    assert.equal(releaseCalled, true);
    assert.equal(heartbeatSocket.getCloseArgs()?.reason, 'hard_expiry');
  } finally {
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('redirect to new target releases old lease and reruns target admission with a new request id', async () => {
  const originalFetch = globalThis.fetch;
  const originalRandomUUID = crypto.randomUUID;
  const requestIds = [];
  const calls = [];
  const concurrencyReleaseBodies = [];

  crypto.randomUUID = () => `req-${requestIds.length + 1}`;
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant-a.sharepoint.com/file',
          header: {},
        },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/precheck') {
      calls.push(`precheck:${JSON.parse(init.body).hostname}`);
      return createJsonResponse({ result: 'allow' });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push(`fairqueue-acquire:${JSON.parse(init.body).hostname}`);
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-1',
        invocationEpoch: 1,
        slotToken: 'slot-1',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      requestIds.push(body.requestId);
      calls.push(`concurrency-acquire:${body.hostname}`);
      return createJsonResponse({
        result: 'granted',
        leaseId: `lease-${requestIds.length}`,
        leaseToken: `token-${requestIds.length}`,
        expiresAtMs: body.hardExpireAtMs,
        claimToken: `claim-token-${requestIds.length}`,
      });
    }

    if (url === 'https://tenant-a.sharepoint.com/file') {
      calls.push('origin-fetch:a');
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://tenant-b.sharepoint.com/file' },
      });
    }

    if (url === 'https://tenant-b.sharepoint.com/file') {
      calls.push('origin-fetch:b');
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      concurrencyReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      return createJsonResponse({ result: 'ok' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    assert.equal(await response.text(), 'ok');
    await Promise.allSettled(waitUntilPromises);
    assert.equal(requestIds.length, 2);
    assert.notEqual(requestIds[0], requestIds[1]);
    assert.equal(calls.includes('concurrency-release'), true);
    assert.equal(concurrencyReleaseBodies[0]?.reason, 'final_cleanup');
    assert.equal(calls.includes('origin-fetch:b'), true);
  } finally {
    globalThis.fetch = originalFetch;
    crypto.randomUUID = originalRandomUUID;
    delete globalThis.bootstrapCache;
  }
});

test('same-host redirect still rotates admission state and reacquires with a new request id', async () => {
  const originalFetch = globalThis.fetch;
  const originalRandomUUID = crypto.randomUUID;
  let requestCounter = 0;
  const concurrencyRequestIds = [];
  const fairQueueAcquireBodies = [];
  const fairQueueReleaseBodies = [];
  const concurrencyReleaseBodies = [];
  const originFetches = [];

  crypto.randomUUID = () => {
    requestCounter += 1;
    return `req-same-host-${requestCounter}`;
  };

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/demo/start.bin',
          header: {},
        },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/precheck') {
      return createJsonResponse({ result: 'allow' });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      fairQueueAcquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        queryToken: `query-${fairQueueAcquireBodies.length}`,
        invocationEpoch: fairQueueAcquireBodies.length,
        slotToken: `slot-${fairQueueAcquireBodies.length}`,
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      concurrencyRequestIds.push(body.requestId);
      return createJsonResponse({
        result: 'granted',
        leaseId: `lease-${concurrencyRequestIds.length}`,
        leaseToken: `token-${concurrencyRequestIds.length}`,
        expiresAtMs: body.hardExpireAtMs,
        claimToken: `claim-token-${concurrencyRequestIds.length}`,
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      fairQueueReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      concurrencyReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://tenant.sharepoint.com/sites/demo/start.bin') {
      originFetches.push(url);
      return new Response(null, {
        status: 302,
        headers: {
          Location: 'https://tenant.sharepoint.com/sites/demo/final.bin?rotated=1',
        },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/demo/final.bin?rotated=1') {
      originFetches.push(url);
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    assert.equal(await response.text(), 'ok');
    await Promise.allSettled(waitUntilPromises);

    assert.equal(concurrencyRequestIds.length, 2);
    assert.notEqual(concurrencyRequestIds[0], concurrencyRequestIds[1]);
    assert.equal(fairQueueAcquireBodies.length, 2);
    assert.equal(fairQueueReleaseBodies.length >= 2, true);
    assert.equal(concurrencyReleaseBodies.length >= 1, true);
    assert.equal(concurrencyReleaseBodies[0]?.reason, 'final_cleanup');
    assert.deepEqual(originFetches, [
      'https://tenant.sharepoint.com/sites/demo/start.bin',
      'https://tenant.sharepoint.com/sites/demo/final.bin?rotated=1',
    ]);
  } finally {
    globalThis.fetch = originalFetch;
    crypto.randomUUID = originalRandomUUID;
    delete globalThis.bootstrapCache;
  }
});

test('relative redirect resolves to an absolute upstream target before target preparation', async () => {
  const originalFetch = globalThis.fetch;
  const originalRandomUUID = crypto.randomUUID;
  let requestCounter = 0;
  const concurrencyRequestIds = [];
  const fairQueueReleaseBodies = [];
  const concurrencyReleaseBodies = [];
  const originFetches = [];

  crypto.randomUUID = () => {
    requestCounter += 1;
    return `req-relative-${requestCounter}`;
  };

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/demo/start-relative.bin',
          header: {},
        },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/precheck') {
      return createJsonResponse({ result: 'allow' });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      return createJsonResponse({
        result: 'granted',
        queryToken: `query-relative-${requestCounter}`,
        invocationEpoch: requestCounter,
        slotToken: `slot-relative-${requestCounter}`,
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      concurrencyRequestIds.push(body.requestId);
      return createJsonResponse({
        result: 'granted',
        leaseId: `lease-relative-${concurrencyRequestIds.length}`,
        leaseToken: `token-relative-${concurrencyRequestIds.length}`,
        expiresAtMs: body.hardExpireAtMs,
        claimToken: `claim-token-relative-${concurrencyRequestIds.length}`,
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      fairQueueReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      concurrencyReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://tenant.sharepoint.com/sites/demo/start-relative.bin') {
      originFetches.push(url);
      return new Response(null, {
        status: 302,
        headers: {
          Location: '/sites/demo/final-relative.bin?rotated=1',
        },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/demo/final-relative.bin?rotated=1') {
      originFetches.push(url);
      return new Response('relative-ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    assert.equal(await response.text(), 'relative-ok');
    await Promise.allSettled(waitUntilPromises);

    assert.equal(concurrencyRequestIds.length, 2);
    assert.notEqual(concurrencyRequestIds[0], concurrencyRequestIds[1]);
    assert.equal(fairQueueReleaseBodies.length >= 2, true);
    assert.equal(concurrencyReleaseBodies.length >= 1, true);
    assert.deepEqual(originFetches, [
      'https://tenant.sharepoint.com/sites/demo/start-relative.bin',
      'https://tenant.sharepoint.com/sites/demo/final-relative.bin?rotated=1',
    ]);
  } finally {
    globalThis.fetch = originalFetch;
    crypto.randomUUID = originalRandomUUID;
    delete globalThis.bootstrapCache;
  }
});

test('same-origin worker redirect releases old admission state before recursion', async () => {
  const originalFetch = globalThis.fetch;
  const originalRandomUUID = crypto.randomUUID;
  let requestCounter = 0;
  let linkRequestCount = 0;
  let fairQueueAcquireCount = 0;
  let concurrencyAcquireCount = 0;
  const calls = [];
  const recursiveRequest = await buildSignedWorkerRequest({ pathname: '/downloads/recursive.bin' });

  crypto.randomUUID = () => {
    requestCounter += 1;
    return `req-recursive-${requestCounter}`;
  };

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      linkRequestCount += 1;
      calls.push(`api-link:${linkRequestCount}`);
      return createJsonResponse({
        code: 200,
        data: {
          url: linkRequestCount === 1
            ? 'https://tenant-a.sharepoint.com/file'
            : 'https://tenant-b.sharepoint.com/recursive-file',
          header: {},
        },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/precheck') {
      return createJsonResponse({ result: 'allow' });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      fairQueueAcquireCount += 1;
      calls.push(`fairqueue-acquire:${fairQueueAcquireCount}`);
      return createJsonResponse({
        result: 'granted',
        queryToken: `query-recursive-${fairQueueAcquireCount}`,
        invocationEpoch: fairQueueAcquireCount,
        slotToken: `slot-recursive-${fairQueueAcquireCount}`,
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      concurrencyAcquireCount += 1;
      calls.push(`concurrency-acquire:${concurrencyAcquireCount}`);
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: `lease-recursive-${concurrencyAcquireCount}`,
        leaseToken: `token-recursive-${concurrencyAcquireCount}`,
        expiresAtMs: body.hardExpireAtMs,
        claimToken: `claim-token-recursive-${concurrencyAcquireCount}`,
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      const body = JSON.parse(init.body);
      calls.push(`fairqueue-release:${body.slotToken}`);
      return createJsonResponse({ result: 'ok' });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      const body = JSON.parse(init.body);
      calls.push(`concurrency-release:${body.leaseId}`);
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://tenant-a.sharepoint.com/file') {
      calls.push('origin-fetch:outer');
      return new Response(null, {
        status: 302,
        headers: {
          Location: recursiveRequest.url,
        },
      });
    }

    if (url === 'https://tenant-b.sharepoint.com/recursive-file') {
      calls.push('origin-fetch:inner');
      return new Response('recursive-ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    assert.equal(await response.text(), 'recursive-ok');
    await Promise.allSettled(waitUntilPromises);

    const fairQueueReleaseIndex = calls.indexOf('fairqueue-release:slot-recursive-1');
    const concurrencyReleaseIndex = calls.indexOf('concurrency-release:lease-recursive-1');
    const recursiveLinkIndex = calls.indexOf('api-link:2');
    assert.notEqual(fairQueueReleaseIndex, -1);
    assert.notEqual(concurrencyReleaseIndex, -1);
    assert.notEqual(recursiveLinkIndex, -1);
    assert.ok(fairQueueReleaseIndex < recursiveLinkIndex);
    assert.ok(concurrencyReleaseIndex < recursiveLinkIndex);
  } finally {
    globalThis.fetch = originalFetch;
    crypto.randomUUID = originalRandomUUID;
    delete globalThis.bootstrapCache;
  }
});

test('queue_only fast wait releases unused fairqueue grant before continue-wait and then releases CQ after origin fetch', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const fairQueueReleaseBodies = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/demo/file',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-wait-1',
        invocationEpoch: 1,
        slotToken: 'slot-wait-1',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      calls.push(body.waitToken ? 'concurrency-acquire-continue' : 'concurrency-acquire-fast');
      if (!body.waitToken) {
        return createJsonResponse({
          result: 'wait',
          waitToken: 'wait-token-1',
          scope: 'host',
          retryAfter: 1,
        });
      }
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-wait-1',
        leaseToken: 'token-wait-1',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-wait-1',
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      fairQueueReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://tenant.sharepoint.com/sites/demo/file') {
      calls.push('origin-fetch');
      return new Response('wait-ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/cancel') {
      calls.push('concurrency-cancel');
      throw new Error('cancel should not run after wait grant');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    assert.equal(await response.text(), 'wait-ok');
    await Promise.allSettled(waitUntilPromises);

    assert.deepEqual(calls, [
      'fairqueue-acquire',
      'concurrency-acquire-fast',
      'fairqueue-release',
      'concurrency-acquire-continue',
      'origin-fetch',
      'concurrency-release',
    ]);
    assert.equal(fairQueueReleaseBodies.length, 1);
    assert.equal(fairQueueReleaseBodies[0].hitUpstreamAtMs, 0);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_only replays acquire after continue-wait timeout abort and recovers the active lease', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const continueBodies = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/demo/file',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-replay-1',
        invocationEpoch: 1,
        slotToken: 'slot-replay-1',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      calls.push(body.waitToken ? 'concurrency-acquire-continue' : 'concurrency-acquire-fast');
      if (!body.waitToken) {
        return createJsonResponse({
          result: 'wait',
          waitToken: 'wait-replay-1',
          scope: 'host',
          retryAfter: 1,
        });
      }
      continueBodies.push(body);
      if (continueBodies.length === 1) {
        const error = new Error('The operation was aborted.');
        error.name = 'AbortError';
        throw error;
      }
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-replay-1',
        leaseToken: 'token-replay-1',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-replay-1',
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://tenant.sharepoint.com/sites/demo/file') {
      calls.push('origin-fetch');
      return new Response('replay-ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/cancel') {
      calls.push('concurrency-cancel');
      throw new Error('cancel should not run after timeout abort replay recovery');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    assert.equal(await response.text(), 'replay-ok');
    await Promise.allSettled(waitUntilPromises);

    assert.deepEqual(calls, [
      'fairqueue-acquire',
      'concurrency-acquire-fast',
      'fairqueue-release',
      'concurrency-acquire-continue',
      'concurrency-acquire-continue',
      'origin-fetch',
      'concurrency-release',
    ]);
    assert.equal(continueBodies.length, 2);
    assert.equal(continueBodies[0].waitToken, 'wait-replay-1');
    assert.equal(continueBodies[1].waitToken, 'wait-replay-1');
    assert.equal(continueBodies[0].requestId, continueBodies[1].requestId);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('CQ wait attempt budget counts timeout-replayed continue-wait acquires against the cap', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const cancelBodies = [];
  const continueBodies = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      const bootstrap = buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
      });
      bootstrap.download.trueConcurrency.waitTotalMaxMs = 20000;
      bootstrap.download.trueConcurrency.waitMaxAttemptsCap = 3;
      return createJsonResponse(bootstrap);
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/demo/file',
          header: {},
        },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      calls.push(body.waitToken ? 'concurrency-acquire-continue' : 'concurrency-acquire-fast');
      if (!body.waitToken) {
        return createJsonResponse({
          result: 'wait',
          waitToken: 'wait-replay-budget-1',
          scope: 'host',
          retryAfter: 1,
        });
      }

      continueBodies.push(body);
      if (continueBodies.length === 1) {
        const abortError = new Error('The operation was aborted.');
        abortError.name = 'AbortError';
        throw abortError;
      }

      if (continueBodies.length === 2) {
        return createJsonResponse({
          result: 'wait',
          waitToken: 'wait-replay-budget-1',
          scope: 'host',
          retryAfter: 1,
        });
      }

      throw new Error('worker must not issue a fourth CQ acquire attempt after timeout replay reaches the cap');
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/cancel') {
      calls.push('concurrency-cancel');
      cancelBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'cancelled' });
    }

    if (url === 'https://tenant.sharepoint.com/sites/demo/file') {
      throw new Error('origin fetch should not run after replayed wait attempt cap exhaustion');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.match(body.message, /wait budget exhausted/i);
    assert.deepEqual(calls, [
      'concurrency-acquire-fast',
      'concurrency-acquire-continue',
      'concurrency-acquire-continue',
      'concurrency-cancel',
    ]);
    assert.equal(continueBodies.length, 2);
    assert.equal(cancelBodies.length, 1);
    assert.equal(cancelBodies[0].reason, 'worker_aborted');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('CQ wait elapsed budget exhaustion cancels CQ request and returns the existing unavailable surface', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const cancelBodies = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      const bootstrap = buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
      });
      bootstrap.download.trueConcurrency.waitTotalMaxMs = 25;
      bootstrap.download.trueConcurrency.waitMaxAttemptsCap = 35;
      return createJsonResponse(bootstrap);
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/demo/file',
          header: {},
        },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      calls.push(body.waitToken ? 'concurrency-acquire-continue' : 'concurrency-acquire-fast');
      if (!body.waitToken) {
        return createJsonResponse({
          result: 'wait',
          waitToken: 'wait-budget-elapsed-1',
          scope: 'host',
          retryAfter: 1,
        });
      }
      await new Promise((resolve, reject) => {
        const abortError = new Error('The operation was aborted.');
        abortError.name = 'AbortError';
        init.signal?.addEventListener('abort', () => reject(abortError), { once: true });
        setTimeout(() => resolve(), 1000);
      });
      throw new Error('continue-wait acquire should abort before resolving');
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/cancel') {
      calls.push('concurrency-cancel');
      cancelBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'cancelled' });
    }

    if (url === 'https://tenant.sharepoint.com/sites/demo/file') {
      throw new Error('origin fetch should not run after wait budget exhaustion');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.match(body.message, /wait budget exhausted/i);
    assert.deepEqual(calls, [
      'concurrency-acquire-fast',
      'concurrency-acquire-continue',
      'concurrency-cancel',
    ]);
    assert.equal(cancelBodies.length, 1);
    assert.equal(cancelBodies[0].reason, 'worker_aborted');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('CQ wait attempt budget exhaustion hard-fails before a third acquire call', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const cancelBodies = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      const bootstrap = buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
      });
      bootstrap.download.trueConcurrency.waitTotalMaxMs = 20000;
      bootstrap.download.trueConcurrency.waitMaxAttemptsCap = 2;
      return createJsonResponse(bootstrap);
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/demo/file',
          header: {},
        },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      calls.push(body.waitToken ? 'concurrency-acquire-continue' : 'concurrency-acquire-fast');
      return createJsonResponse({
        result: 'wait',
        waitToken: 'wait-budget-attempt-1',
        scope: 'host',
        retryAfter: 1,
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/cancel') {
      calls.push('concurrency-cancel');
      cancelBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'cancelled' });
    }

    if (url === 'https://tenant.sharepoint.com/sites/demo/file') {
      throw new Error('origin fetch should not run after wait attempt budget exhaustion');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.equal(calls.filter((call) => call === 'concurrency-acquire-continue').length, 1);
    assert.equal(calls.includes('concurrency-cancel'), true);
    assert.match(body.message, /wait budget exhausted/i);
    assert.equal(cancelBodies.length, 1);
    assert.equal(cancelBodies[0].reason, 'worker_aborted');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_only aborts CQ wait and cancels request when unused fairqueue release cannot be confirmed', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const cancelBodies = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/demo/file',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-release-fail-1',
        invocationEpoch: 1,
        slotToken: 'slot-release-fail-1',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      calls.push(body.waitToken ? 'concurrency-acquire-continue' : 'concurrency-acquire-fast');
      if (!body.waitToken) {
        return createJsonResponse({
          result: 'wait',
          waitToken: 'wait-release-fail-1',
          scope: 'host',
          retryAfter: 1,
        });
      }
      throw new Error('worker must not continue waiting after failed fairqueue release');
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      return new Response('release unavailable', { status: 503 });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/cancel') {
      calls.push('concurrency-cancel');
      cancelBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'cancelled' });
    }

    if (url === 'https://tenant.sharepoint.com/sites/demo/file') {
      throw new Error('origin fetch should not run after failed fairqueue release');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.match(body.message, /fair queue/i);
    assert.deepEqual(calls.slice(0, 3), [
      'fairqueue-acquire',
      'concurrency-acquire-fast',
      'fairqueue-release',
    ]);
    assert.equal(calls.includes('concurrency-acquire-continue'), false);
    assert.equal(calls.includes('origin-fetch'), false);
    assert.equal(calls.filter((call) => call === 'concurrency-cancel').length, 1);
    assert.equal(cancelBodies.length, 1);
    assert.equal(cancelBodies[0].reason, 'worker_aborted');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('queue_only wait hard expiry cancels CQ request after unused fairqueue release and returns link expired', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const fairQueueReleaseBodies = [];
  const cancelBodies = [];

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: ['*.sharepoint.com'],
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/demo/file',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-terminal-1',
        invocationEpoch: 1,
        slotToken: 'slot-terminal-1',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      calls.push(body.waitToken ? 'concurrency-acquire-continue' : 'concurrency-acquire-fast');
      if (!body.waitToken) {
        return createJsonResponse({
          result: 'wait',
          waitToken: 'wait-token-terminal',
          scope: 'host',
          retryAfter: 1,
        });
      }
      return createJsonResponse({
        result: 'expired',
        reason: 'hard_expired',
      }, { status: 410 });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      fairQueueReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/cancel') {
      calls.push('concurrency-cancel');
      cancelBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'cancelled' });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      throw new Error('release should not run for waiting-only terminal cleanup');
    }

    if (url === 'https://tenant.sharepoint.com/sites/demo/file') {
      throw new Error('origin fetch should not run after wait terminal');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    const body = await readJson(response);
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 401);
    assert.equal(body.message, 'link expired');
    assert.deepEqual(calls, [
      'fairqueue-acquire',
      'concurrency-acquire-fast',
      'fairqueue-release',
      'concurrency-acquire-continue',
      'concurrency-cancel',
    ]);
    assert.equal(fairQueueReleaseBodies[0].hitUpstreamAtMs, 0);
    assert.equal(cancelBodies.length, 1);
    assert.equal(cancelBodies[0].reason, 'worker_aborted');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('Google Drive HEAD requests use GET range probe, expose resumable headers, and do not mark the ticket used', async () => {
  const originalFetch = globalThis.fetch;
  const originCalls = [];
  const originUrls = new Set([
    'https://drive.google.com/uc?id=test-file&export=download',
    'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
  ]);

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url, method, headers } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: originCalls.length === 0
            ? 'https://drive.google.com/uc?id=test-file&export=download'
            : 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
        },
      });
    }

    if (originUrls.has(url)) {
      originCalls.push({ method, range: headers.get('range') });
      return new Response('x', {
        status: 206,
        headers: {
          'content-type': 'application/octet-stream',
          'content-disposition': 'attachment; filename="probe.bin"',
          'content-range': 'bytes 0-0/100',
          'content-length': '1',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    for (let i = 0; i < 2; i += 1) {
      const baseRequest = await buildSignedWorkerRequest();
      const request = new Request(baseRequest.url, {
        method: 'HEAD',
        headers: baseRequest.headers,
      });

      const response = await worker.fetch(request, buildWorkerEnv(), createTestContext().ctx);

      assert.equal(response.status, 200);
      assert.equal(response.headers.get('accept-ranges'), 'bytes');
      assert.equal(response.headers.get('content-length'), '100');
      assert.equal(response.headers.get('content-range'), null);
      assert.equal(response.headers.get('content-type'), 'application/octet-stream');
      assert.equal(await response.text(), '');
    }

    assert.deepEqual(originCalls, [
      { method: 'GET', range: 'bytes=0-0' },
      { method: 'GET', range: 'bytes=0-0' },
    ]);
    assert.equal(ticketStateRpcState.markBodies.length, 0);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency Google Drive HEAD probe releases stream_complete after a successful probe response', async () => {
  const originalFetch = globalThis.fetch;
  let releaseBody = null;

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url, method, headers } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: GOOGLE_DRIVE_HOST_PATTERNS,
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
        },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-head-success',
        leaseToken: 'token-head-success',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-head-success',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      releaseBody = JSON.parse(init.body);
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/test-file?alt=media') {
      assert.equal(method, 'GET');
      assert.equal(headers.get('range'), 'bytes=0-0');
      return new Response('x', {
        status: 206,
        headers: {
          'content-type': 'application/octet-stream',
          'content-disposition': 'attachment; filename="probe.bin"',
          'content-range': 'bytes 0-0/100',
          'content-length': '1',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const baseRequest = await buildSignedWorkerRequest();
    const request = new Request(baseRequest.url, {
      method: 'HEAD',
      headers: baseRequest.headers,
    });
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(request, buildWorkerEnv(), ctx);
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('accept-ranges'), 'bytes');
    assert.equal(response.headers.get('content-length'), '100');
    assert.equal(response.headers.get('content-range'), null);
    assert.equal(releaseBody?.reason, 'stream_complete');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('Google Drive HEAD range probes fail safely and cancel body when upstream returns 200', async () => {
  const originalFetch = globalThis.fetch;
  const originBody = createTrackedTextBody('unsafe-head-body');

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url, method, headers } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
        },
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/test-file?alt=media') {
      assert.equal(method, 'GET');
      assert.equal(headers.get('range'), 'bytes=0-0');
      return new Response(originBody.stream, {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '16',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const baseRequest = await buildSignedWorkerRequest();
    const request = new Request(baseRequest.url, {
      method: 'HEAD',
      headers: baseRequest.headers,
    });

    const response = await worker.fetch(request, buildWorkerEnv(), createTestContext().ctx);
    assert.notEqual(response.status, 200);
    assert.equal(await response.text(), JSON.stringify({
      code: 502,
      message: 'Google Drive HEAD probe invalid',
    }));
    assert.equal(originBody.cancelled, true);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('Google Drive HEAD range probes fail safely and cancel body when Content-Range is missing', async () => {
  const originalFetch = globalThis.fetch;
  const originBody = createTrackedTextBody('unsafe-head-body');

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
        },
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/test-file?alt=media') {
      return new Response(originBody.stream, {
        status: 206,
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '1',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const baseRequest = await buildSignedWorkerRequest();
    const request = new Request(baseRequest.url, {
      method: 'HEAD',
      headers: baseRequest.headers,
    });

    const response = await worker.fetch(request, buildWorkerEnv(), createTestContext().ctx);
    assert.notEqual(response.status, 200);
    assert.equal(await response.text(), JSON.stringify({
      code: 502,
      message: 'Google Drive HEAD probe invalid',
    }));
    assert.equal(originBody.cancelled, true);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('Google Drive HEAD range probes fail safely and cancel body when Content-Range total is invalid', async () => {
  const originalFetch = globalThis.fetch;
  const originBody = createTrackedTextBody('unsafe-head-body');

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
        },
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/test-file?alt=media') {
      return new Response(originBody.stream, {
        status: 206,
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '1',
          'content-range': 'bytes 0-0/*',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const baseRequest = await buildSignedWorkerRequest();
    const request = new Request(baseRequest.url, {
      method: 'HEAD',
      headers: baseRequest.headers,
    });

    const response = await worker.fetch(request, buildWorkerEnv(), createTestContext().ctx);
    assert.notEqual(response.status, 200);
    assert.equal(await response.text(), JSON.stringify({
      code: 502,
      message: 'Google Drive HEAD probe invalid',
    }));
    assert.equal(originBody.cancelled, true);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('Google Drive HEAD probe failure waits for in-flight fairqueue header release before terminal cleanup', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const releaseBodies = [];
  let concurrencyReleaseBody = null;
  let releaseStarted = null;
  let finishHeaderRelease = null;

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url, method, headers } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: GOOGLE_DRIVE_HOST_PATTERNS,
        trueConcurrencyHostPatterns: GOOGLE_DRIVE_HOST_PATTERNS,
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-invalid-head',
        invocationEpoch: 1,
        slotToken: 'slot-invalid-head',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-invalid-head',
        leaseToken: 'token-invalid-head',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-invalid-head',
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/test-file?alt=media') {
      calls.push('origin-fetch');
      assert.equal(method, 'GET');
      assert.equal(headers.get('range'), 'bytes=0-0');
      return new Response('unsafe-head-body', {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '16',
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      releaseBodies.push(JSON.parse(init.body));
      releaseStarted?.();
      await new Promise((resolve) => {
        finishHeaderRelease = resolve;
      });
      return createJsonResponse({ result: 'ok' });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      concurrencyReleaseBody = JSON.parse(init.body);
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const baseRequest = await buildSignedWorkerRequest();
    const request = new Request(baseRequest.url, {
      method: 'HEAD',
      headers: baseRequest.headers,
    });
    const { ctx, waitUntilPromises } = createTestContext();
    const responsePromise = worker.fetch(request, buildWorkerEnv(), ctx);
    await new Promise((resolve) => {
      releaseStarted = resolve;
    });

    assert.equal(calls.filter((call) => call === 'fairqueue-release').length, 1);
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(calls.filter((call) => call === 'fairqueue-release').length, 1);

    finishHeaderRelease();
    const response = await responsePromise;
    await Promise.allSettled(waitUntilPromises);
    assert.equal(response.status, 502);
    assert.equal(calls.filter((call) => call === 'fairqueue-release').length, 1);
    assert.equal(releaseBodies.length, 1);
    assert.equal(releaseBodies[0].slotToken, 'slot-invalid-head');
    assert.equal(concurrencyReleaseBody?.reason, 'origin_fetch_failure');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('Non-Google-Drive HEAD requests are forwarded without range probe rewrite', async () => {
  const originalFetch = globalThis.fetch;
  const originCalls = [];

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url, method, headers } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/demo/file',
          header: {},
        },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/demo/file') {
      originCalls.push({ method, range: headers.get('range') });
      return new Response(null, {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '123',
          'accept-ranges': 'bytes',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const baseRequest = await buildSignedWorkerRequest();
    const request = new Request(baseRequest.url, {
      method: 'HEAD',
      headers: baseRequest.headers,
    });

    const response = await worker.fetch(request, buildWorkerEnv(), createTestContext().ctx);

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('accept-ranges'), 'bytes');
    assert.equal(response.headers.get('content-length'), '123');
    assert.deepEqual(originCalls, [{ method: 'HEAD', range: null }]);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('generic Worker-created responses strip invalid content-length and transfer-encoding while preserving the body', async () => {
  const originalFetch = globalThis.fetch;
  const originCalls = [];

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url, method, headers } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/sites/demo/file',
          header: {},
        },
      });
    }

    if (url === 'https://tenant.sharepoint.com/sites/demo/file') {
      originCalls.push({ method, range: headers.get('range') });
      return new Response('download-body', {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '13junk',
          'transfer-encoding': 'chunked',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(
      await buildSignedWorkerRequest(),
      buildWorkerEnv(),
      createTestContext().ctx,
    );

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('content-length'), null);
    assert.equal(response.headers.get('transfer-encoding'), null);
    assert.equal(await response.text(), 'download-body');
    assert.deepEqual(originCalls, [{ method: 'GET', range: null }]);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('Google Drive GET downloads prefer payload filesize for full-range translation, expose accept-ranges, and mark the ticket used before returning content', async () => {
  const originalFetch = globalThis.fetch;
  const originCalls = [];

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url, method, headers } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
        },
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/test-file?alt=media') {
      originCalls.push({ method, range: headers.get('range') });
      return new Response('download-body', {
        status: 206,
        headers: {
          'content-type': 'application/octet-stream',
          'content-disposition': 'attachment; filename="download.bin"',
          'content-length': '13',
          'content-range': 'bytes 0-12/13',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(
      await buildSignedWorkerRequest({ payloadFileSize: 13 }),
      buildWorkerEnv(),
      createTestContext().ctx,
    );

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('accept-ranges'), 'bytes');
    assert.equal(response.headers.get('content-length'), '13');
    assert.equal(response.headers.get('content-range'), null);
    assert.equal(response.headers.get('content-type'), 'application/octet-stream');
    assert.equal(await response.text(), 'download-body');
    assert.deepEqual(originCalls, [{ method: 'GET', range: 'bytes=0-12' }]);
    assert.equal(ticketStateRpcState.readBodies.length, 1);
    assert.equal(ticketStateRpcState.markBodies.length, 1);
    assert.equal(
      ticketStateRpcState.markBodies[0].p_ticket_hash,
      ticketStateRpcState.readBodies[0].p_ticket_hash,
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('Google Drive synthetic full-download responses strip transfer-encoding and preserve content-length', async () => {
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url, headers } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
          filesize: 13,
        },
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/test-file?alt=media') {
      assert.equal(headers.get('range'), 'bytes=0-12');
      return new Response('download-body', {
        status: 206,
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '13',
          'content-range': 'bytes 0-12/13',
          'transfer-encoding': 'chunked',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(
      await buildSignedWorkerRequest(),
      buildWorkerEnv(),
      createTestContext().ctx,
    );

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('transfer-encoding'), null);
    assert.equal(response.headers.get('content-length'), '13');
    assert.equal(await response.text(), 'download-body');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('Google Drive GET downloads without client range rewrite full-file 206 responses into fixed-length 200 responses when payload filesize is known', async () => {
  const originalFetch = globalThis.fetch;
  const originCalls = [];

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url, method, headers } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
          filesize: 13,
        },
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/test-file?alt=media') {
      originCalls.push({ method, range: headers.get('range') });
      return new Response('download-body', {
        status: 206,
        headers: {
          'content-type': 'application/octet-stream',
          'content-disposition': 'attachment; filename="download.bin"',
          'content-length': '13',
          'content-range': 'bytes 0-12/13',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(
      await buildSignedWorkerRequest(),
      buildWorkerEnv(),
      createTestContext().ctx,
    );

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('content-length'), '13');
    assert.equal(response.headers.get('content-range'), null);
    assert.equal(response.headers.get('accept-ranges'), 'bytes');
    assert.equal(await response.text(), 'download-body');
    assert.deepEqual(originCalls, [{ method: 'GET', range: 'bytes=0-12' }]);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('Google Drive GET downloads without payload filesize probe HEAD first and rewrite full-file 206 responses into fixed-length 200 responses', async () => {
  const originalFetch = globalThis.fetch;
  const originCalls = [];

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url, method, headers } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
        },
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/test-file?alt=media') {
      originCalls.push({ method, range: headers.get('range') });
      if (method === 'HEAD') {
        return new Response(null, {
          status: 200,
          headers: {
            'content-type': 'application/octet-stream',
            'content-length': '13',
          },
        });
      }
      return new Response('download-body', {
        status: 206,
        headers: {
          'content-type': 'application/octet-stream',
          'content-disposition': 'attachment; filename="download.bin"',
          'content-length': '13',
          'content-range': 'bytes 0-12/13',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(
      await buildSignedWorkerRequest(),
      buildWorkerEnv(),
      createTestContext().ctx,
    );

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('content-length'), '13');
    assert.equal(response.headers.get('content-range'), null);
    assert.equal(response.headers.get('accept-ranges'), 'bytes');
    assert.equal(await response.text(), 'download-body');
    assert.deepEqual(originCalls, [
      { method: 'HEAD', range: null },
      { method: 'GET', range: 'bytes=0-12' },
    ]);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('Google Drive GET downloads without payload filesize fall back to a 0-0 range probe when HEAD does not expose content-length', async () => {
  const originalFetch = globalThis.fetch;
  const originCalls = [];
  const headProbeBody = createTrackedTextBody('head-probe-body');
  const rangeProbeBody = createTrackedTextBody('range-probe-body');

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url, method, headers } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
        },
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/test-file?alt=media') {
      originCalls.push({ method, range: headers.get('range') });
      if (method === 'HEAD') {
        return new Response(headProbeBody.stream, {
          status: 200,
          headers: {
            'content-type': 'application/octet-stream',
          },
        });
      }
      if (headers.get('range') === 'bytes=0-0') {
        return new Response(rangeProbeBody.stream, {
          status: 206,
          headers: {
            'content-type': 'application/octet-stream',
            'content-length': '1',
            'content-range': 'bytes 0-0/13',
          },
        });
      }
      return new Response('download-body', {
        status: 206,
        headers: {
          'content-type': 'application/octet-stream',
          'content-disposition': 'attachment; filename="download.bin"',
          'content-length': '13',
          'content-range': 'bytes 0-12/13',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(
      await buildSignedWorkerRequest(),
      buildWorkerEnv(),
      createTestContext().ctx,
    );

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('content-length'), '13');
    assert.equal(response.headers.get('content-range'), null);
    assert.equal(response.headers.get('accept-ranges'), 'bytes');
    assert.equal(await response.text(), 'download-body');
    assert.deepEqual(originCalls, [
      { method: 'HEAD', range: null },
      { method: 'GET', range: 'bytes=0-0' },
      { method: 'GET', range: 'bytes=0-12' },
    ]);
    assert.equal(headProbeBody.cancelled, true);
    assert.equal(rangeProbeBody.cancelled, true);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('Google Drive GET downloads do not rewrite partial 206 responses that do not match the requested full-file range', async () => {
  const originalFetch = globalThis.fetch;
  const originCalls = [];

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url, method, headers } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
          filesize: 13,
        },
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/test-file?alt=media') {
      originCalls.push({ method, range: headers.get('range') });
      return new Response('partial-body', {
        status: 206,
        headers: {
          'content-type': 'application/octet-stream',
          'content-disposition': 'attachment; filename="download.bin"',
          'content-length': '12',
          'content-range': 'bytes 1-12/13',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(
      await buildSignedWorkerRequest({ payloadFileSize: 13 }),
      buildWorkerEnv(),
      createTestContext().ctx,
    );

    assert.notEqual(response.status, 206);
    assert.notEqual(await response.text(), 'partial-body');
    assert.deepEqual(originCalls, [{ method: 'GET', range: 'bytes=0-12' }]);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('Google Drive GET downloads reject synthetic full-range 206 responses with mismatched totals', async () => {
  const originalFetch = globalThis.fetch;
  const originCalls = [];
  const originBody = createTrackedTextBody('partial-body');

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url, method, headers } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
          filesize: 13,
        },
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/test-file?alt=media') {
      originCalls.push({ method, range: headers.get('range') });
      return new Response(originBody.stream, {
        status: 206,
        headers: {
          'content-type': 'application/octet-stream',
          'content-disposition': 'attachment; filename="download.bin"',
          'content-length': '13',
          'content-range': 'bytes 0-12/20',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(
      await buildSignedWorkerRequest({ payloadFileSize: 13 }),
      buildWorkerEnv(),
      createTestContext().ctx,
    );

    assert.notEqual(response.status, 206);
    assert.notEqual(await response.text(), 'partial-body');
    assert.deepEqual(originCalls, [{ method: 'GET', range: 'bytes=0-12' }]);
    assert.equal(originBody.cancelled, true);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('Google Drive full-range mismatch waits for in-flight fairqueue header release before terminal cleanup', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const releaseBodies = [];
  let concurrencyReleaseBody = null;
  let releaseStarted = null;
  let finishHeaderRelease = null;

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url, method, headers } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        fairQueueHostPatterns: GOOGLE_DRIVE_HOST_PATTERNS,
        trueConcurrencyHostPatterns: GOOGLE_DRIVE_HOST_PATTERNS,
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
          filesize: 13,
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.push('fairqueue-acquire');
      return createJsonResponse({
        result: 'granted',
        queryToken: 'query-range-mismatch',
        invocationEpoch: 1,
        slotToken: 'slot-range-mismatch',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-range-mismatch',
        leaseToken: 'token-range-mismatch',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-range-mismatch',
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/test-file?alt=media') {
      calls.push('origin-fetch');
      assert.equal(method, 'GET');
      assert.equal(headers.get('range'), 'bytes=0-12');
      return new Response('partial-body', {
        status: 206,
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '13',
          'content-range': 'bytes 0-12/20',
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      releaseBodies.push(JSON.parse(init.body));
      releaseStarted?.();
      await new Promise((resolve) => {
        finishHeaderRelease = resolve;
      });
      return createJsonResponse({ result: 'ok' });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      concurrencyReleaseBody = JSON.parse(init.body);
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const responsePromise = worker.fetch(
      await buildSignedWorkerRequest({ payloadFileSize: 13 }),
      buildWorkerEnv(),
      ctx,
    );
    await new Promise((resolve) => {
      releaseStarted = resolve;
    });

    assert.equal(calls.filter((call) => call === 'fairqueue-release').length, 1);
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(calls.filter((call) => call === 'fairqueue-release').length, 1);

    finishHeaderRelease();
    const response = await responsePromise;
    await Promise.allSettled(waitUntilPromises);
    assert.equal(response.status, 502);
    assert.equal(calls.filter((call) => call === 'fairqueue-release').length, 1);
    assert.equal(releaseBodies.length, 1);
    assert.equal(releaseBodies[0].slotToken, 'slot-range-mismatch');
    assert.equal(concurrencyReleaseBody?.reason, 'origin_fetch_failure');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('Google Drive GET downloads fail open to the original GET path when HEAD and 0-0 probes both fail', async () => {
  const originalFetch = globalThis.fetch;
  const originCalls = [];

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url, method, headers } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
        },
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/test-file?alt=media') {
      originCalls.push({ method, range: headers.get('range') });
      if (method === 'HEAD') {
        throw new Error('HEAD probe failed');
      }
      if (headers.get('range') === 'bytes=0-0') {
        throw new Error('range probe failed');
      }
      return new Response('download-body', {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
          'content-disposition': 'attachment; filename="download.bin"',
          'content-length': '13',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(
      await buildSignedWorkerRequest(),
      buildWorkerEnv(),
      createTestContext().ctx,
    );

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('content-length'), '13');
    assert.equal(response.headers.get('accept-ranges'), 'bytes');
    assert.equal(await response.text(), 'download-body');
    assert.deepEqual(originCalls, [
      { method: 'HEAD', range: null },
      { method: 'GET', range: 'bytes=0-0' },
      { method: 'GET', range: null },
    ]);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency known-length downloads use FixedLengthStream to preserve content-length', async () => {
  const originalFetch = globalThis.fetch;
  const originalFixedLengthStream = globalThis.FixedLengthStream;
  const calls = [];
  const fixedLengthCalls = [];

  globalThis.FixedLengthStream = class FakeFixedLengthStream {
    constructor(length) {
      fixedLengthCalls.push(length);
      const inner = new TransformStream();
      this.writable = inner.writable;
      this.readable = inner.readable;
    }
  };

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: GOOGLE_DRIVE_HOST_PATTERNS,
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
        },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-fixed-length',
        leaseToken: 'token-fixed-length',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-fixed-length',
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/test-file?alt=media') {
      calls.push('origin-fetch');
      return new Response('download-body', {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
          'content-disposition': 'attachment; filename="download.bin"',
          'content-length': '13',
        },
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(
      await buildSignedWorkerRequest({ payloadFileSize: 13 }),
      buildWorkerEnv(),
      ctx,
    );

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('content-length'), '13');
    assert.equal(await response.text(), 'download-body');
    await new Promise((resolve) => setTimeout(resolve, 0));
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(calls, ['concurrency-acquire', 'origin-fetch', 'concurrency-release']);
    assert.deepEqual(fixedLengthCalls, [13]);
  } finally {
    globalThis.fetch = originalFetch;
    globalThis.FixedLengthStream = originalFixedLengthStream;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency managed streaming strips transfer-encoding and preserves content-length', async () => {
  const originalFetch = globalThis.fetch;
  const originalFixedLengthStream = globalThis.FixedLengthStream;
  const calls = [];

  globalThis.FixedLengthStream = class FakeFixedLengthStream {
    constructor() {
      const inner = new TransformStream();
      this.writable = inner.writable;
      this.readable = inner.readable;
    }
  };

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: ['*.sharepoint.com'],
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-hop-by-hop',
        leaseToken: 'token-hop-by-hop',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-hop-by-hop',
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('managed-stream', {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '14',
          'transfer-encoding': 'chunked',
        },
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(
      await buildSignedWorkerRequest(),
      buildWorkerEnv(),
      ctx,
    );

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('transfer-encoding'), null);
    assert.equal(response.headers.get('content-length'), '14');
    assert.equal(await response.text(), 'managed-stream');
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(calls, ['concurrency-acquire', 'origin-fetch', 'concurrency-release']);
  } finally {
    globalThis.fetch = originalFetch;
    globalThis.FixedLengthStream = originalFixedLengthStream;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency malformed content-length falls back to generic managed streaming', async () => {
  const originalFetch = globalThis.fetch;
  const originalFixedLengthStream = globalThis.FixedLengthStream;
  const calls = [];
  const fixedLengthCalls = [];

  globalThis.FixedLengthStream = class FakeFixedLengthStream {
    constructor(length) {
      fixedLengthCalls.push(length);
      const inner = new TransformStream();
      this.writable = inner.writable;
      this.readable = inner.readable;
    }
  };

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: GOOGLE_DRIVE_HOST_PATTERNS,
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
        },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-invalid-length',
        leaseToken: 'token-invalid-length',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-invalid-length',
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/test-file?alt=media') {
      calls.push('origin-fetch');
      return new Response('download-body', {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
          'content-disposition': 'attachment; filename="download.bin"',
          'content-length': '13junk',
        },
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(
      await buildSignedWorkerRequest({ payloadFileSize: 13 }),
      buildWorkerEnv(),
      ctx,
    );

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('content-length'), null);
    assert.equal(await response.text(), 'download-body');
    await new Promise((resolve) => setTimeout(resolve, 0));
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(calls, ['concurrency-acquire', 'origin-fetch', 'concurrency-release']);
    assert.deepEqual(fixedLengthCalls, []);
  } finally {
    globalThis.fetch = originalFetch;
    globalThis.FixedLengthStream = originalFixedLengthStream;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency rewrites exact Google full-file 206 responses into fixed-length 200 responses', async () => {
  const originalFetch = globalThis.fetch;
  const originalFixedLengthStream = globalThis.FixedLengthStream;
  const calls = [];
  const originCalls = [];
  const fixedLengthCalls = [];

  globalThis.FixedLengthStream = class FakeFixedLengthStream {
    constructor(length) {
      fixedLengthCalls.push(length);
      const inner = new TransformStream();
      this.writable = inner.writable;
      this.readable = inner.readable;
    }
  };

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url, method, headers } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: GOOGLE_DRIVE_HOST_PATTERNS,
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
          filesize: 13,
        },
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      const body = JSON.parse(init.body);
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-rewrite-length',
        leaseToken: 'token-rewrite-length',
        expiresAtMs: body.hardExpireAtMs,
        claimToken: 'claim-token-rewrite-length',
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/test-file?alt=media') {
      originCalls.push({ method, range: headers.get('range') });
      calls.push('origin-fetch');
      return new Response('download-body', {
        status: 206,
        headers: {
          'content-type': 'application/octet-stream',
          'content-disposition': 'attachment; filename="download.bin"',
          'content-length': '13',
          'content-range': 'bytes 0-12/13',
        },
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(
      await buildSignedWorkerRequest({ payloadFileSize: 13 }),
      buildWorkerEnv(),
      ctx,
    );

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('content-length'), '13');
    assert.equal(response.headers.get('content-range'), null);
    assert.equal(await response.text(), 'download-body');
    await new Promise((resolve) => setTimeout(resolve, 0));
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(originCalls, [{ method: 'GET', range: 'bytes=0-12' }]);
    assert.deepEqual(calls, ['concurrency-acquire', 'origin-fetch', 'concurrency-release']);
    assert.deepEqual(fixedLengthCalls, [13]);
  } finally {
    globalThis.fetch = originalFetch;
    globalThis.FixedLengthStream = originalFixedLengthStream;
    delete globalThis.bootstrapCache;
  }
});

test('Google Drive ranged GET downloads synthesize accept-ranges from 206 content-range responses', async () => {
  const originalFetch = globalThis.fetch;
  const originCalls = [];

  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url, method, headers } = request;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap());
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      return createJsonResponse({
        code: 200,
        data: {
          url: 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
        },
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/test-file?alt=media') {
      originCalls.push({ method, range: headers.get('range') });
      return new Response('x', {
        status: 206,
        headers: {
          'content-type': 'application/octet-stream',
          'content-disposition': 'attachment; filename="download.bin"',
          'content-length': '1',
          'content-range': 'bytes 0-0/13',
        },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const baseRequest = await buildSignedWorkerRequest();
    const rangedRequest = new Request(baseRequest.url, {
      method: 'GET',
      headers: new Headers(baseRequest.headers),
    });
    rangedRequest.headers.set('range', 'bytes=0-0');

    const response = await worker.fetch(
      rangedRequest,
      buildWorkerEnv(),
      createTestContext().ctx,
    );

    assert.equal(response.status, 206);
    assert.equal(response.headers.get('accept-ranges'), 'bytes');
    assert.equal(response.headers.get('content-range'), 'bytes 0-0/13');
    assert.equal(response.headers.get('content-length'), '1');
    assert.equal(await response.text(), 'x');
    assert.deepEqual(originCalls, [{ method: 'GET', range: 'bytes=0-0' }]);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('mode-only siteBucket payload keeps SharePoint site buckets aligned across fairqueue and true concurrency', async () => {
  const expectedSiteBucket = await hashSiteKey('sites:demo');

  const result = await captureAdmissionPayloads({
    targetUrl: 'https://tenant.sharepoint.com/sites/demo/file',
    fairQueueHostPatterns: ['*.sharepoint.com'],
    fairQueueSiteBucket: { mode: 'sharepoint' },
    trueConcurrencyHostPatterns: ['*.sharepoint.com'],
    trueConcurrencySiteBucket: { mode: 'sharepoint' },
  });

  assert.equal(result.status, 200);
  assert.equal(result.bodyText, 'ok');
  assert.equal(result.fairQueueAcquireBody?.siteBucket, expectedSiteBucket);
  assert.equal(result.concurrencyAcquireBody?.siteBucket, expectedSiteBucket);
  assert.deepEqual(result.calls.slice(0, 3), ['fairqueue-acquire', 'concurrency-acquire', 'origin-fetch']);
});

test('modes-only siteBucket payload derives one stable Google Drive site bucket across Google host families', async () => {
  const expectedSiteBucket = await hashSiteKey('googledrive:unspecified');
  const googleDriveUrls = [
    'https://drive.google.com/uc?id=test-file&export=download',
    'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
    'https://lh3.googleusercontent.com/test-file',
    'https://drive.usercontent.google.com/download?id=test-file',
  ];

  for (const targetUrl of googleDriveUrls) {
    const result = await captureAdmissionPayloads({
      targetUrl,
      fairQueueHostPatterns: GOOGLE_DRIVE_HOST_PATTERNS,
      fairQueueSiteBucket: { modes: ['sharepoint', 'googledrive'] },
      trueConcurrencyHostPatterns: GOOGLE_DRIVE_HOST_PATTERNS,
      trueConcurrencySiteBucket: { modes: ['sharepoint', 'googledrive'] },
    });

    assert.equal(result.status, 200);
    assert.equal(result.bodyText, 'ok');
    assert.equal(result.fairQueueAcquireBody?.siteBucket, expectedSiteBucket);
    assert.equal(result.concurrencyAcquireBody?.siteBucket, expectedSiteBucket);
  }
});

test('combined siteBucket payload prefers modes over mode for Google Drive requests', async () => {
  const expectedSiteBucket = await hashSiteKey('googledrive:unspecified');

  const result = await captureAdmissionPayloads({
    targetUrl: 'https://drive.google.com/uc?id=test-file&export=download',
    fairQueueHostPatterns: GOOGLE_DRIVE_HOST_PATTERNS,
    fairQueueSiteBucket: {
      mode: 'sharepoint',
      modes: ['googledrive'],
    },
    trueConcurrencyHostPatterns: GOOGLE_DRIVE_HOST_PATTERNS,
    trueConcurrencySiteBucket: {
      mode: 'sharepoint',
      modes: ['googledrive'],
    },
  });

  assert.equal(result.status, 200);
  assert.equal(result.fairQueueAcquireBody?.siteBucket, expectedSiteBucket);
  assert.equal(result.concurrencyAcquireBody?.siteBucket, expectedSiteBucket);
});

test('SharePoint site bucket derivation keeps /personal, /sites, and /teams identities unchanged', async () => {
  const personalResult = await captureAdmissionPayloads({
    targetUrl: 'https://tenant.sharepoint.com/personal/demo/file',
    trueConcurrencyHostPatterns: ['*.sharepoint.com'],
    trueConcurrencySiteBucket: { mode: 'sharepoint' },
  });
  const sitesResult = await captureAdmissionPayloads({
    targetUrl: 'https://tenant.sharepoint.com/sites/demo/file',
    trueConcurrencyHostPatterns: ['*.sharepoint.com'],
    trueConcurrencySiteBucket: { mode: 'sharepoint' },
  });
  const teamsResult = await captureAdmissionPayloads({
    targetUrl: 'https://tenant.sharepoint.com/teams/demo/file',
    trueConcurrencyHostPatterns: ['*.sharepoint.com'],
    trueConcurrencySiteBucket: { mode: 'sharepoint' },
  });

  assert.equal(personalResult.status, 200);
  assert.equal(sitesResult.status, 200);
  assert.equal(teamsResult.status, 200);
  assert.equal(personalResult.concurrencyAcquireBody?.siteBucket, await hashSiteKey('personal:demo'));
  assert.equal(sitesResult.concurrencyAcquireBody?.siteBucket, await hashSiteKey('sites:demo'));
  assert.equal(teamsResult.concurrencyAcquireBody?.siteBucket, await hashSiteKey('teams:demo'));
  assert.notEqual(personalResult.concurrencyAcquireBody?.siteBucket, sitesResult.concurrencyAcquireBody?.siteBucket);
  assert.notEqual(personalResult.concurrencyAcquireBody?.siteBucket, teamsResult.concurrencyAcquireBody?.siteBucket);
  assert.notEqual(sitesResult.concurrencyAcquireBody?.siteBucket, teamsResult.concurrencyAcquireBody?.siteBucket);
});

test('disabled googledrive mode falls back to host-derived site bucket without throwing', async () => {
  const expectedSiteBucket = await hashSiteKey('unknown');

  const result = await captureAdmissionPayloads({
    targetUrl: 'https://drive.google.com/uc?id=test-file&export=download',
    fairQueueHostPatterns: GOOGLE_DRIVE_HOST_PATTERNS,
    fairQueueSiteBucket: { mode: 'sharepoint' },
    trueConcurrencyHostPatterns: GOOGLE_DRIVE_HOST_PATTERNS,
    trueConcurrencySiteBucket: { mode: 'sharepoint' },
  });

  assert.equal(result.status, 200);
  assert.equal(result.bodyText, 'ok');
  assert.equal(result.fairQueueAcquireBody?.siteBucket, expectedSiteBucket);
  assert.equal(result.concurrencyAcquireBody?.siteBucket, expectedSiteBucket);
});

test('host siteBucket fallback derives plain host bucket when provider modes do not match', async () => {
  const result = await captureAdmissionPayloads({
    targetUrl: 'https://files.example.com/download.bin',
    fairQueueHostPatterns: ['*.example.com'],
    fairQueueSiteBucket: { modes: ['host', 'sharepoint'] },
    trueConcurrencyHostPatterns: ['*.example.com'],
    trueConcurrencySiteBucket: { modes: ['host', 'sharepoint'] },
  });

  const expectedSiteBucket = await hashSiteKey('host:files.example.com');
  assert.equal(result.status, 200);
  assert.equal(result.fairQueueAcquireBody?.siteBucket, expectedSiteBucket);
  assert.equal(result.concurrencyAcquireBody?.siteBucket, expectedSiteBucket);
});

test('actual host admission payloads are preserved across Google host families', async () => {
  const googleFamilyHosts = [
    'www.googleapis.com',
    'drive.google.com',
    'content.googleapis.com',
    'lh3.googleusercontent.com',
    'drive.usercontent.google.com',
  ];

  for (const host of googleFamilyHosts) {
    const result = await captureAdmissionPayloads({
      targetUrl: `https://${host}/download.bin`,
      fairQueueHostPatterns: GOOGLE_DRIVE_HOST_PATTERNS,
      fairQueueSiteBucket: { modes: ['googledrive'] },
      trueConcurrencyHostPatterns: GOOGLE_DRIVE_HOST_PATTERNS,
      trueConcurrencySiteBucket: { modes: ['googledrive'] },
    });

    assert.equal(result.status, 200);
    assert.equal(result.fairQueueAcquireBody?.hostname, host);
    assert.equal(result.fairQueueAcquireBody?.hostnameHash, await sha256Hash(host));
    assert.equal(result.concurrencyAcquireBody?.hostname, host);
    assert.equal(result.concurrencyAcquireBody?.hostnameHash, await sha256Hash(host));
    assert.notEqual(result.fairQueueAcquireBody?.hostnameHash, await sha256Hash('google'));
    assert.notEqual(result.concurrencyAcquireBody?.hostnameHash, await sha256Hash('google'));
  }
});

test('breaker authority uses actual Google Drive hostnames', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const googleAuthorityHash = await sha256Hash('google');
  const driveHostHash = await sha256Hash('drive.google.com');
  const googleApiHostHash = await sha256Hash('www.googleapis.com');
  const snapshotHashes = [];
  const authorizeBodies = [];
  const reportBodies = [];
  const concurrencyBodies = [];
  const originHosts = [];
  const breakerStateByHash = new Map();
  let linkCallCount = 0;

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({
        trueConcurrencyHostPatterns: GOOGLE_DRIVE_HOST_PATTERNS,
        throttleHostPatterns: GOOGLE_DRIVE_HOST_PATTERNS,
      }));
    }

    if (url === 'https://alist.example.com/api/fs/link') {
      linkCallCount += 1;
      return createJsonResponse({
        code: 200,
        data: {
          url: linkCallCount === 1
            ? 'https://drive.google.com/uc?id=test-file&export=download'
            : 'https://www.googleapis.com/drive/v3/files/test-file?alt=media',
          header: {},
        },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      const match = url.match(/HOSTNAME_HASH=eq\.([0-9a-f]+)/i);
      const requestedHash = match ? match[1] : '';
      snapshotHashes.push(requestedHash);
      const snapshot = breakerStateByHash.get(requestedHash);
      if (!snapshot) {
        return createJsonResponse([]);
      }
      return createJsonResponse([snapshot]);
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

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      const body = JSON.parse(init.body);
      concurrencyBodies.push(body);
      return createJsonResponse({
        result: 'granted',
        leaseId: `lease-google-${concurrencyBodies.length}`,
        leaseToken: `token-google-${concurrencyBodies.length}`,
        expiresAtMs: body.hardExpireAtMs,
        claimToken: `claim-token-google-${concurrencyBodies.length}`,
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      return createClaimGrantResponseFromRequest(init);
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      const body = JSON.parse(init.body);
      reportBodies.push(body);
      breakerStateByHash.set(body.p_hostname_hash, {
        STATE: 'open',
        OPEN_UNTIL: Math.floor(Date.now() / 1000) + 30,
        OPEN_REASON: `http_${body.p_status_code}`,
        VERSION: 7,
        LAST_ERROR_CODE: body.p_status_code,
      });
      return createJsonResponse([{
        STATE: 'open',
        OPEN_UNTIL: Math.floor(Date.now() / 1000) + 30,
        OPEN_REASON: `http_${body.p_status_code}`,
        VERSION: 7,
        LAST_ERROR_CODE: body.p_status_code,
      }]);
    }

    if (url === 'https://drive.google.com/uc?id=test-file&export=download') {
      originHosts.push('drive.google.com');
      return new Response('protected', {
        status: 429,
        headers: {
          'content-type': 'text/plain',
          'Retry-After': '8',
        },
      });
    }

    if (url === 'https://www.googleapis.com/drive/v3/files/test-file?alt=media') {
      originHosts.push('www.googleapis.com');
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const firstCtx = createTestContext();
    const firstResponse = await worker.fetch(
      await buildSignedWorkerRequest({ pathname: '/downloads/google-breaker-drive.bin', payloadFileSize: 13 }),
      buildWorkerEnv(),
      firstCtx.ctx,
    );
    waitUntilPromises.push(...firstCtx.waitUntilPromises);
    assert.equal(firstResponse.status, 429);
    await firstResponse.text();

    const secondCtx = createTestContext();
    const secondResponse = await worker.fetch(
      await buildSignedWorkerRequest({ pathname: '/downloads/google-breaker-api.bin', payloadFileSize: 13 }),
      buildWorkerEnv(),
      secondCtx.ctx,
    );
    waitUntilPromises.push(...secondCtx.waitUntilPromises);
    assert.equal(secondResponse.status, 200);
    await secondResponse.text();

    await Promise.allSettled(waitUntilPromises);

    assert.deepEqual(snapshotHashes, [driveHostHash, googleApiHostHash]);
    assert.notDeepEqual(snapshotHashes, [googleAuthorityHash, googleAuthorityHash]);
    assert.deepEqual(authorizeBodies.map((body) => body.p_hostname), ['drive.google.com', 'www.googleapis.com']);
    assert.deepEqual(
      reportBodies.map((body) => ({
        hostname: body.p_hostname,
        hostnameHash: body.p_hostname_hash,
        statusCode: body.p_status_code,
      })),
      [
        {
          hostname: 'drive.google.com',
          hostnameHash: driveHostHash,
          statusCode: 429,
        },
        {
          hostname: 'www.googleapis.com',
          hostnameHash: googleApiHostHash,
          statusCode: 200,
        },
      ],
    );
    assert.notEqual(reportBodies[0].p_hostname_hash, googleAuthorityHash);
    assert.notEqual(reportBodies[1].p_hostname_hash, googleAuthorityHash);
    assert.equal(concurrencyBodies.length, 2);
    assert.deepEqual(originHosts, ['drive.google.com', 'www.googleapis.com']);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('recognized Google host overload stays scoped to actual host authority', async () => {
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.test',
      totalMaxWaitMs: 300,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
      authKey: '',
    },
  });

  const googleSiteBucket = await hashSiteKey('googledrive:unspecified');
  const driveContext = {
    hostname: 'drive.google.com',
    hostnameHash: 'drive-host-hash',
    ipBucket: 'google-ip-bucket',
    siteBucket: googleSiteBucket,
  };
  const googleApisContext = {
    hostname: 'www.googleapis.com',
    hostnameHash: 'googleapis-host-hash',
    ipBucket: 'google-ip-bucket',
    siteBucket: googleSiteBucket,
  };

  let fetchCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    if (fetchCalls === 1) {
      return createJsonResponse({
        result: 'overloaded',
        reason: 'overload_host',
      });
    }
    return createJsonResponse({
      result: 'granted',
      queryToken: 'google-overload-grant',
      invocationEpoch: 1,
      slotToken: 'slot-google-overload',
    });
  };

  try {
    const first = await client.waitForSlot({}, driveContext);
    assert.equal(first.kind, 'timeout');

    const second = await client.waitForSlot({}, googleApisContext);
    assert.equal(second.kind, 'granted');
    assert.equal(fetchCalls, 2);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('final cleanup groups recognized Google Drive hosts by actual host authority', () => {
  const cleanupGroups = buildFinalCleanupGroups([
    {
      hostname: 'drive.google.com',
      hostnameHash: 'drive-host-hash',
      queryToken: 'google-drive-query',
      invocationEpoch: 1,
    },
    {
      hostname: 'www.googleapis.com',
      hostnameHash: 'googleapis-host-hash',
      queryToken: 'google-api-query',
      invocationEpoch: 2,
    },
    {
      hostname: 'lh3.googleusercontent.com',
      hostnameHash: 'googleusercontent-host-hash',
      queryToken: 'googleusercontent-query',
      invocationEpoch: 3,
    },
    {
      hostname: 'files.example.com',
      hostnameHash: 'unknown-host-hash',
      queryToken: 'unknown-query',
      invocationEpoch: 4,
    },
  ]);

  assert.equal(cleanupGroups.length, 4);
  assert.deepEqual(
    cleanupGroups.map((group) => group.map((context) => context.queryToken)),
    [
      ['google-drive-query'],
      ['google-api-query'],
      ['googleusercontent-query'],
      ['unknown-query'],
    ],
  );
});

test('final cleanup keeps SharePoint host grouping unchanged', () => {
  const cleanupGroups = buildFinalCleanupGroups([
    {
      hostname: 'tenant-a.sharepoint.com',
      hostnameHash: 'sharepoint-host-a',
      queryToken: 'sharepoint-query-a',
      invocationEpoch: 1,
    },
    {
      hostname: 'tenant-b.sharepoint.com',
      hostnameHash: 'sharepoint-host-b',
      queryToken: 'sharepoint-query-b',
      invocationEpoch: 2,
    },
  ]);

  assert.equal(cleanupGroups.length, 2);
  assert.deepEqual(
    cleanupGroups.map((group) => group.map((context) => context.queryToken)),
    [['sharepoint-query-a'], ['sharepoint-query-b']],
  );
});
