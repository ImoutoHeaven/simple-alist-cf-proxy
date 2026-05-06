import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { encryptBindingPayload } from '../src/origin-binding.js';
import worker, { __fairQueueTestHooks } from '../src/worker.js';

const { resolveConfig, resolveAdmissionMode } = __fairQueueTestHooks;

const createJsonResponse = (payload) => new Response(JSON.stringify(payload), {
  status: 200,
  headers: { 'content-type': 'application/json' },
});

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

const buildSignedWorkerRequest = async (pathname = '/downloads/test.bin', options = {}) => {
  const {
    idleTimeoutSeconds = DEFAULT_IDLE_TIMEOUT_SECONDS,
    omitTicketNonce = false,
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

const buildRuntimeBootstrap = ({ fairQueueHostPatterns = [], throttleHostPatterns = [], trueConcurrencyHostPatterns = [] } = {}) => ({
  configVersion: 'task1-admission-mode',
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
      cacheEnabled: false,
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
    fairQueue: {
      enabled: fairQueueHostPatterns.length > 0,
      hostPatterns: fairQueueHostPatterns,
      slotHandlerUrl: 'https://slot-handler.example.test',
      slotHandlerAuthKey: 'slot-secret',
      slotHandlerAuthHeader: 'X-FQ-Auth',
    },
    ...(trueConcurrencyHostPatterns.length > 0 ? {
      trueConcurrency: {
        enabled: true,
        hostPatterns: trueConcurrencyHostPatterns,
        handlerUrl: 'https://cq.example.test',
        handlerAuthKey: 'cq-secret',
        heartbeat: DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
      },
    } : {}),
  },
});

const buildEnv = () => ({
  CONTROLLER_URL: 'https://controller.example.test',
  CONTROLLER_API_TOKEN: 'controller-token',
  ENV: 'test',
  ROLE: 'download',
  INSTANCE_ID: 'worker-1',
  BOOTSTRAP_CACHE_MODE: 'direct',
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

const createModeHarness = ({ fairQueueHostPatterns = [], throttleHostPatterns = [], trueConcurrencyHostPatterns = [] } = {}) => {
  const bootstrap = buildRuntimeBootstrap({ fairQueueHostPatterns, throttleHostPatterns, trueConcurrencyHostPatterns });
  const config = resolveConfig({}, bootstrap, { download: {} });
  return {
    bootstrap,
    config,
  };
};

test('signed worker request fixtures include ticketNonce and idle_timeout', async () => {
  assertSignedRequestPayloadContract(await buildSignedWorkerRequest());
});

const runModeScenario = async ({
  fairQueueHostPatterns = [],
  throttleHostPatterns = [],
  trueConcurrencyHostPatterns = [],
  authorizeAttemptSnapshot = {
    STATE: 'closed',
    OPEN_UNTIL: null,
    OPEN_REASON: null,
    VERSION: 2,
    LAST_ERROR_CODE: null,
    ATTEMPT_GRANTED: false,
    ATTEMPT_TICKET: null,
  },
  settleAttemptSnapshot = {
    STATE: 'closed',
    OPEN_UNTIL: null,
    OPEN_REASON: null,
    VERSION: 3,
    LAST_ERROR_CODE: null,
  },
  settleError = null,
  upstreamResponse = null,
  slotHandlerResponse = {
    result: 'granted',
    queryToken: 'query-mode-default',
    invocationEpoch: 1,
    slotToken: 'slot-1',
  },
} = {}) => {
  const { bootstrap } = createModeHarness({ fairQueueHostPatterns, throttleHostPatterns, trueConcurrencyHostPatterns });
  const calls = {
    acquire: 0,
    release: 0,
    snapshot: 0,
    authorize: 0,
    report: 0,
    settle: 0,
    concurrencyAcquire: 0,
    concurrencyClaim: 0,
    concurrencyRelease: 0,
    concurrencyCancel: 0,
  };
  const acquireBodies = [];
  const authorizeBodies = [];
  const reportBodies = [];
  const settleBodies = [];
  const releaseBodies = [];
  const concurrencyAcquireBodies = [];
  const concurrencyReleaseBodies = [];
  const concurrencyCancelBodies = [];
  const waitUntilPromises = [];
  const originalFetch = globalThis.fetch;

  delete globalThis.bootstrapCache;
  __fairQueueTestHooks.clearOverloadedByHost?.();

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(bootstrap);
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
      calls.snapshot += 1;
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.authorize += 1;
      authorizeBodies.push(JSON.parse(init.body));
      const snapshot = typeof authorizeAttemptSnapshot === 'function'
        ? await authorizeAttemptSnapshot({ calls, authorizeBodies, reportBodies, settleBodies })
        : authorizeAttemptSnapshot;
      return createJsonResponse([snapshot]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      calls.report += 1;
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 3,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      calls.settle += 1;
      settleBodies.push(JSON.parse(init.body));
      if (settleError) {
        throw settleError;
      }
      const snapshot = typeof settleAttemptSnapshot === 'function'
        ? await settleAttemptSnapshot({ calls, authorizeBodies, reportBodies, settleBodies })
        : settleAttemptSnapshot;
      return createJsonResponse([snapshot]);
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.acquire += 1;
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse(
        typeof slotHandlerResponse === 'function'
          ? await slotHandlerResponse({ calls, acquireBodies, authorizeBodies, reportBodies, releaseBodies })
          : slotHandlerResponse
      );
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.release += 1;
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.concurrencyAcquire += 1;
      concurrencyAcquireBodies.push(JSON.parse(init.body));
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-mode-1',
        leaseToken: 'token-mode-1',
        expiresAtMs: Date.now() + 1000,
        claimToken: 'claim-token-mode-1',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.concurrencyClaim += 1;
      return createClaimGrantResponse({
        leaseId: 'lease-mode-1',
        leaseToken: 'token-mode-1',
      });
    }

    if (url === ACK_HANDOFF_URL) {
      return createJsonResponse({ result: 'acknowledged' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.concurrencyRelease += 1;
      concurrencyReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/cancel') {
      calls.concurrencyCancel += 1;
      concurrencyCancelBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'cancelled' });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      if (upstreamResponse instanceof Response) {
        return upstreamResponse;
      }
      if (typeof upstreamResponse === 'function') {
        return upstreamResponse({ calls, authorizeBodies, reportBodies, settleBodies });
      }
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildEnv(), {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });
    const responseBodyText = await response.clone().text();
    await Promise.allSettled(waitUntilPromises);
    return {
      response,
      responseBodyText,
      calls,
      acquireBodies,
      authorizeBodies,
      reportBodies,
      settleBodies,
      releaseBodies,
      concurrencyAcquireBodies,
      concurrencyReleaseBodies,
      concurrencyCancelBodies,
    };
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
    __fairQueueTestHooks.clearOverloadedByHost?.();
  }
};

test('none mode does not call slot-handler or breaker RPCs', async () => {
  const { config } = createModeHarness();

  assert.equal(typeof resolveAdmissionMode, 'function');
  assert.equal(resolveAdmissionMode(config, 'tenant.sharepoint.com'), 'none');

  const { response, calls } = await runModeScenario();
  assert.equal(response.status, 200);
  assert.equal(calls.acquire, 0);
  assert.equal(calls.authorize, 0);
  assert.equal(calls.report, 0);
});

test('breaker_only calls authorize/report but never slot-handler', async () => {
  const { config } = createModeHarness({
    throttleHostPatterns: ['*.sharepoint.com'],
  });

  assert.equal(typeof resolveAdmissionMode, 'function');
  assert.equal(resolveAdmissionMode(config, 'tenant.sharepoint.com'), 'breaker_only');

  const { response, calls } = await runModeScenario({
    throttleHostPatterns: ['*.sharepoint.com'],
  });
  assert.equal(response.status, 200);
  assert.equal(calls.acquire, 0);
  assert.equal(calls.authorize, 1);
  assert.equal(calls.report, 1);
  assert.equal(calls.settle, 0);
});

test('breaker_only explicitly settles authorized no-sample terminal responses before returning upstream status', async () => {
  const { response, responseBodyText, calls, reportBodies, settleBodies } = await runModeScenario({
    throttleHostPatterns: ['*.sharepoint.com'],
    authorizeAttemptSnapshot: {
      STATE: 'half_open',
      OPEN_UNTIL: null,
      OPEN_REASON: 'http_429',
      VERSION: 7,
      LAST_ERROR_CODE: 429,
      HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
      ATTEMPT_GRANTED: true,
      ATTEMPT_TICKET: 2,
    },
    upstreamResponse: new Response('missing', {
      status: 404,
      headers: { 'content-type': 'text/plain' },
    }),
  });

  assert.equal(response.status, 404);
  const body = JSON.parse(responseBodyText);
  assert.equal(body.code, 404);
  assert.equal(typeof body.message, 'string');
  assert.notEqual(body.message, 'missing');
  assert.equal(calls.acquire, 0);
  assert.equal(calls.authorize, 1);
  assert.deepEqual(reportBodies, []);
  assert.equal(calls.settle, 1);
  assert.equal(settleBodies[0].p_attempt_version, 7);
  assert.equal(settleBodies[0].p_attempt_ticket, 2);
});

test('breaker_only fails closed when settlement fails on authorized no-sample terminal response', async () => {
  const originBody = createTrackedTextBody('missing');
  const { response, responseBodyText, calls, reportBodies, settleBodies } = await runModeScenario({
    throttleHostPatterns: ['*.sharepoint.com'],
    authorizeAttemptSnapshot: {
      STATE: 'half_open',
      OPEN_UNTIL: null,
      OPEN_REASON: 'http_429',
      VERSION: 9,
      LAST_ERROR_CODE: 429,
      HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
      ATTEMPT_GRANTED: true,
      ATTEMPT_TICKET: 4,
    },
    settleError: new Error('settle unavailable'),
    upstreamResponse: new Response(originBody.stream, {
      status: 404,
      headers: { 'content-type': 'text/plain' },
    }),
  });

  assert.equal(response.status, 503);
  assert.match(JSON.parse(responseBodyText).message, /attempt settlement/i);
  assert.equal(calls.acquire, 0);
  assert.equal(calls.authorize, 1);
  assert.deepEqual(reportBodies, []);
  assert.equal(calls.settle, 1);
  assert.equal(settleBodies[0].p_attempt_version, 9);
  assert.equal(settleBodies[0].p_attempt_ticket, 4);
  assert.equal(originBody.cancelled, true);
});

test('breaker_only settles authorized origin fetch throws before returning the worker error contract', async () => {
  const { response, responseBodyText, calls, reportBodies, settleBodies } = await runModeScenario({
    throttleHostPatterns: ['*.sharepoint.com'],
    authorizeAttemptSnapshot: {
      STATE: 'half_open',
      OPEN_UNTIL: null,
      OPEN_REASON: 'http_429',
      VERSION: 11,
      LAST_ERROR_CODE: 429,
      HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
      ATTEMPT_GRANTED: true,
      ATTEMPT_TICKET: 6,
    },
    upstreamResponse: () => {
      throw new Error('origin exploded');
    },
  });

  assert.equal(response.status, 500);
  assert.equal(JSON.parse(responseBodyText).message, 'origin exploded');
  assert.equal(calls.acquire, 0);
  assert.equal(calls.authorize, 1);
  assert.deepEqual(reportBodies, []);
  assert.equal(calls.settle, 1);
  assert.equal(settleBodies[0].p_attempt_version, 11);
  assert.equal(settleBodies[0].p_attempt_ticket, 6);
});

test('breaker_only fails closed when settlement fails after origin fetch throws before sampling', async () => {
  const { response, responseBodyText, calls, reportBodies, settleBodies } = await runModeScenario({
    throttleHostPatterns: ['*.sharepoint.com'],
    authorizeAttemptSnapshot: {
      STATE: 'half_open',
      OPEN_UNTIL: null,
      OPEN_REASON: 'http_429',
      VERSION: 13,
      LAST_ERROR_CODE: 429,
      HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
      ATTEMPT_GRANTED: true,
      ATTEMPT_TICKET: 8,
    },
    settleError: new Error('settle unavailable'),
    upstreamResponse: () => {
      throw new Error('origin exploded');
    },
  });

  assert.equal(response.status, 503);
  assert.match(JSON.parse(responseBodyText).message, /attempt settlement/i);
  assert.equal(calls.acquire, 0);
  assert.equal(calls.authorize, 1);
  assert.deepEqual(reportBodies, []);
  assert.equal(calls.settle, 1);
  assert.equal(settleBodies[0].p_attempt_version, 13);
  assert.equal(settleBodies[0].p_attempt_ticket, 8);
});

test('queue_breaker settles slot-carried breaker attempt when direct origin fetch throws before any upstream response', async () => {
  const { response, responseBodyText, calls, reportBodies, settleBodies } = await runModeScenario({
    fairQueueHostPatterns: ['*.sharepoint.com'],
    throttleHostPatterns: ['*.sharepoint.com'],
    slotHandlerResponse: {
      result: 'granted',
      queryToken: 'query-mode-queue-breaker-throw',
      invocationEpoch: 1,
      slotToken: 'slot-queue-breaker-throw',
      meta: {
        attemptVersion: 17,
        attemptTicket: 12,
      },
    },
    upstreamResponse: () => {
      throw new Error('origin exploded');
    },
  });

  assert.equal(response.status, 500);
  assert.equal(JSON.parse(responseBodyText).message, 'origin exploded');
  assert.equal(calls.acquire, 1);
  assert.equal(calls.release, 1);
  assert.equal(calls.authorize, 0);
  assert.deepEqual(reportBodies, []);
  assert.equal(calls.settle, 1);
  assert.equal(settleBodies[0].p_attempt_version, 17);
  assert.equal(settleBodies[0].p_attempt_ticket, 12);
});

test('queue_only calls slot-handler but never breaker RPCs', async () => {
  const { config } = createModeHarness({
    fairQueueHostPatterns: ['*.sharepoint.com'],
    trueConcurrencyHostPatterns: ['*.sharepoint.com'],
  });

  assert.equal(typeof resolveAdmissionMode, 'function');
  assert.equal(resolveAdmissionMode(config, 'tenant.sharepoint.com'), 'queue_only');

  const { response, calls } = await runModeScenario({
    fairQueueHostPatterns: ['*.sharepoint.com'],
    trueConcurrencyHostPatterns: ['*.sharepoint.com'],
  });
  assert.equal(response.status, 200);
  assert.equal(calls.acquire, 1);
  assert.equal(calls.authorize, 0);
  assert.equal(calls.report, 0);
  assert.equal(calls.concurrencyAcquire, 1);
});

test('queue_breaker uses slot-handler READY attempt tokens and skips authorize RPC', async () => {
  const { config } = createModeHarness({
    fairQueueHostPatterns: ['*.sharepoint.com'],
    throttleHostPatterns: ['*.sharepoint.com'],
  });

  assert.equal(typeof resolveAdmissionMode, 'function');
  assert.equal(resolveAdmissionMode(config, 'tenant.sharepoint.com'), 'queue_breaker');

  const { response, calls, reportBodies } = await runModeScenario({
    fairQueueHostPatterns: ['*.sharepoint.com'],
    throttleHostPatterns: ['*.sharepoint.com'],
    slotHandlerResponse: {
      result: 'granted',
      queryToken: 'query-mode-queue-breaker',
      invocationEpoch: 1,
      slotToken: 'slot-1',
      meta: {
        attemptVersion: 7,
        attemptTicket: 2,
      },
    },
  });

  assert.equal(response.status, 200);
  assert.equal(calls.acquire, 1);
  assert.equal(calls.snapshot, 0);
  assert.equal(calls.authorize, 0);
  assert.equal(calls.report, 1);
  assert.equal(reportBodies[0].p_attempt_version, 7);
  assert.equal(reportBodies[0].p_attempt_ticket, 2);
});

test('mode routing depends on hostname pattern matches, not global subsystem disable', () => {
  const { config } = createModeHarness({
    fairQueueHostPatterns: ['*.sharepoint.com'],
    throttleHostPatterns: ['*.office.com'],
  });

  assert.equal(config.fairQueueEnabled, true);
  assert.equal(config.throttleEnabled, true);
  assert.equal(resolveAdmissionMode(config, 'tenant.sharepoint.com'), 'queue_only');
  assert.equal(resolveAdmissionMode(config, 'files.office.com'), 'breaker_only');
  assert.equal(resolveAdmissionMode(config, 'example.net'), 'none');
});

test('queue_breaker has no post-slot authorize fallback branch', async () => {
  const source = await readFile(new URL('../src/worker.js', import.meta.url), 'utf8');

  assert.equal(
    /requestAdmissionMode === 'queue_breaker'[\s\S]*authorizeBreakerAttemptIfNeeded\(requestHostname\)/.test(source),
    false,
  );
});

test('queue_only with true concurrency wait path never calls breaker RPCs', async () => {
  const { config } = createModeHarness({
    fairQueueHostPatterns: ['*.sharepoint.com'],
    trueConcurrencyHostPatterns: ['*.sharepoint.com'],
  });

  assert.equal(resolveAdmissionMode(config, 'tenant.sharepoint.com'), 'queue_only');

  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const calls = [];
  let acquireCallCount = 0;

  delete globalThis.bootstrapCache;
  __fairQueueTestHooks.clearOverloadedByHost?.();

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
        queryToken: 'query-mode-wait',
        invocationEpoch: 1,
        slotToken: 'slot-mode-wait',
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      acquireCallCount += 1;
      calls.push(`concurrency-acquire-${acquireCallCount}`);
      if (acquireCallCount === 1) {
        return createJsonResponse({
          result: 'wait',
          waitToken: 'wait-mode-1',
          scope: 'host',
          retryAfter: 1,
        });
      }
      return createJsonResponse({
        result: 'granted',
        leaseId: 'lease-mode-wait',
        leaseToken: 'token-mode-wait',
        expiresAtMs: Date.now() + 1000,
        claimToken: 'claim-token-mode-wait',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('concurrency-claim');
      return createClaimGrantResponse({
        leaseId: 'lease-mode-wait',
        leaseToken: 'token-mode-wait',
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

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('mode-wait-ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    if (/^https:\/\/postgrest\.example\.test\//.test(url)) {
      throw new Error(`breaker RPC should not run in queue_only wait path: ${url}`);
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildEnv(), {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });
    assert.equal(await response.text(), 'mode-wait-ok');
    await Promise.allSettled(waitUntilPromises);
    assert.equal(response.status, 200);
    assert.deepEqual(calls, [
      'fairqueue-acquire',
      'concurrency-acquire-1',
      'fairqueue-release',
      'concurrency-acquire-2',
      'concurrency-claim',
      'concurrency-ack-handoff',
      'heartbeat-upgrade',
      'origin-fetch',
      'concurrency-release',
    ]);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
    __fairQueueTestHooks.clearOverloadedByHost?.();
  }
});
