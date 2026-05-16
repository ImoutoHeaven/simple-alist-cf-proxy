import { test } from 'node:test';
import assert from 'node:assert/strict';
import { nextOverloadDelayMs } from '../src/fairqueue-overload.js';
import worker, { __fairQueueTestHooks } from '../src/worker.js';
import { encryptBindingPayload } from '../src/origin-binding.js';

const FAIL_FAST_WAIT_MAX_MS = 250;

const createJsonResponse = (payload) => new Response(JSON.stringify(payload), {
  status: 200,
  headers: { 'content-type': 'application/json' },
});

const createStreamingSseResponse = (frames, init = {}) => {
  const encoder = new TextEncoder();
  let index = 0;

  return new Response(new ReadableStream({
    pull(controller) {
      if (index >= frames.length) {
        controller.close();
        return;
      }

      controller.enqueue(encoder.encode(frames[index]));
      index += 1;
      if (index >= frames.length) {
        controller.close();
      }
    },
  }), {
    status: init.status ?? 200,
    headers: {
      'content-type': 'text/event-stream',
      ...(init.headers || {}),
    },
  });
};

const createFairQueueWaitSseResponse = (finalPayload, options = {}) => {
  const acceptedPayload = {
    queryToken: options.queryToken ?? finalPayload.queryToken ?? 'fq-q-default',
    invocationEpoch: options.invocationEpoch ?? finalPayload.invocationEpoch ?? 1,
    deadlineMs: options.acceptedDeadlineMs ?? finalPayload.deadlineMs ?? (Date.now() + 1_000),
  };

  return createStreamingSseResponse([
    'event: accepted\n',
    `data: ${JSON.stringify(acceptedPayload)}\n\n`,
    ': keepalive 1710000000001\n\n',
    'event: result\n',
    `data: ${JSON.stringify({
      ...finalPayload,
      queryToken: finalPayload.queryToken ?? acceptedPayload.queryToken,
      invocationEpoch: finalPayload.invocationEpoch ?? acceptedPayload.invocationEpoch,
    })}\n\n`,
  ], options);
};

const createFairQueueAcceptedOnlyResponse = ({
  queryToken = 'fq-q-default',
  invocationEpoch = 1,
  deadlineMs = Date.now() + 1_000,
  onAccepted = null,
  onCancel = null,
} = {}) => {
  const encoder = new TextEncoder();
  const acceptedFrame = encoder.encode(
    `event: accepted\ndata: ${JSON.stringify({ queryToken, invocationEpoch, deadlineMs })}\n\n`,
  );

  return new Response(new ReadableStream({
    start(controller) {
      controller.enqueue(acceptedFrame);
      onAccepted?.();
    },
    pull() {
      return new Promise(() => {});
    },
    cancel() {
      onCancel?.();
    },
  }), {
    status: 200,
    headers: { 'content-type': 'text/event-stream' },
  });
};

const createFairQueueWaitResponseFromInit = (init, finalPayload, options = {}) => {
  const requestBody = JSON.parse(init.body);
  return createFairQueueWaitSseResponse(
    finalPayload.result === 'granted' && finalPayload.releaseOwnerRequired === undefined
      ? { ...finalPayload, releaseOwnerRequired: true }
      : finalPayload,
    {
      ...options,
      acceptedDeadlineMs: requestBody.deadlineMs,
    },
  );
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

const buildRuntimeBootstrap = ({ fairQueueHostPatterns = [] } = {}) => ({
  configVersion: 'chunk1-overload-runtime',
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
      cleanupPercentage: 0,
    },
    throttleProfiles: {
      default: {
        hostPatterns: [],
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
      },
    } : {}),
  },
});

const buildSignedWorkerRequest = async (pathname = '/downloads/chunk1-overload.bin', options = {}) => {
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

const buildWorkerEnv = () => ({
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

const waitForNextTurn = () => new Promise((resolve) => setTimeout(resolve, 0));

const createDeferred = () => {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
};

test('signed worker request fixtures include ticketNonce and idle_timeout', async () => {
  assertSignedRequestPayloadContract(await buildSignedWorkerRequest());
});

test('overload backoff increases by 500ms up to 2s', () => {
  assert.equal(nextOverloadDelayMs(0), 500);
  assert.equal(nextOverloadDelayMs(1), 1000);
  assert.equal(nextOverloadDelayMs(2), 1500);
  assert.equal(nextOverloadDelayMs(6), 2000);
});

test('overload backoff uses 500ms staircase capped at 2s', () => {
  const OVERLOAD_STAIRCASE = [
    { streak: 0, delayMs: 500 },
    { streak: 1, delayMs: 1000 },
    { streak: 2, delayMs: 1500 },
    { streak: 3, delayMs: 2000 },
    { streak: 4, delayMs: 2000 },
  ];

  for (const item of OVERLOAD_STAIRCASE) {
    assert.equal(
      nextOverloadDelayMs(item.streak),
      item.delayMs,
      `expected overload delay for streak ${item.streak}`
    );
  }
});

test('overload jitter never exceeds 2s cap', () => {
  const delay = nextOverloadDelayMs(10, {
    jitter: true,
    random: () => 0.999999,
  });
  assert.equal(delay, 2000);
});

test('overload first-step jitter stays near 500ms and never near 2s', () => {
  const delay = nextOverloadDelayMs(0, {
    jitter: true,
    random: () => 0.999999,
  });
  assert.ok(delay >= 500, `expected jittered delay >= 500ms, got ${delay}`);
  assert.ok(delay <= 650, `expected jittered delay to stay close to 500ms, got ${delay}`);
});

test('overload jitter is bounded by jitterCap when random is 1', () => {
  const delay = nextOverloadDelayMs(0, {
    jitter: true,
    jitterMaxMs: 100,
    random: () => 1,
  });
  assert.equal(delay, 600);
});

test('slot-handler client defaults auth header name to X-FQ-Auth', async () => {
  const { createSlotHandlerClient } = __fairQueueTestHooks;
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: 'secret',
      authHeader: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (_url, init) => {
    const headers = new Headers(init?.headers);
    assert.equal(headers.get('X-FQ-Auth'), 'secret');
    return createFairQueueWaitSseResponse({
      result: 'granted',
      queryToken: 'query-auth-default',
      invocationEpoch: 1,
      slotToken: 'slot-1',
      releaseOwnerRequired: true,
    });
  };

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.equal(result.kind, 'granted');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler client uses configured auth header name', async () => {
  const { createSlotHandlerClient } = __fairQueueTestHooks;
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: 'secret',
      authHeader: 'X-Custom-FQ-Auth',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (_url, init) => {
    const headers = new Headers(init?.headers);
    assert.equal(headers.get('X-Custom-FQ-Auth'), 'secret');
    assert.equal(headers.get('X-FQ-Auth'), null);
    return createFairQueueWaitSseResponse({
      result: 'granted',
      queryToken: 'query-auth-custom',
      invocationEpoch: 1,
      slotToken: 'slot-1',
      releaseOwnerRequired: true,
    });
  };

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.equal(result.kind, 'granted');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler client rejects non-overloaded JSON terminal setup results for wait', async () => {
  const { createSlotHandlerClient } = __fairQueueTestHooks;
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({
    result: 'granted',
    queryToken: 'query-json-granted',
    invocationEpoch: 1,
    slotToken: 'slot-json-granted',
    releaseOwnerRequired: true,
  }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.deepEqual(result, { kind: 'timeout', reason: 'slot-handler-invalid-response' });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler client stores queryToken and invocationEpoch after accepted SSE before abort', async () => {
  const { createSlotHandlerClient } = __fairQueueTestHooks;
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };
  const controller = new AbortController();

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    setTimeout(() => controller.abort(), 0);
    return createFairQueueAcceptedOnlyResponse({
      queryToken: 'query-pending',
      invocationEpoch: 4,
    });
  };

  try {
    await assert.rejects(
      client.waitForSlot({}, fqContext, controller.signal),
      (error) => error && error.name === 'AbortError',
    );
    assert.equal(fqContext.queryToken, 'query-pending');
    assert.equal(fqContext.invocationEpoch, 4);
    assert.equal(fqContext.slotToken, undefined);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler client treats accepted SSE missing ownership as timeout', async () => {
  const { createSlotHandlerClient } = __fairQueueTestHooks;
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 50,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => createStreamingSseResponse([
    'event: accepted\n',
    `data: ${JSON.stringify({ queryToken: 'query-partial', deadlineMs: Date.now() + 1_000 })}\n\n`,
  ]);

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.deepEqual(result, { kind: 'timeout', reason: 'slot-handler-unreachable' });
    assert.equal(fqContext.queryToken, undefined);
    assert.equal(fqContext.invocationEpoch, undefined);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler client surfaces atomic attempt tokens from granted responses', async () => {
  const { createSlotHandlerClient } = __fairQueueTestHooks;
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => createFairQueueWaitSseResponse({
    result: 'granted',
    queryToken: 'query-granted',
    invocationEpoch: 6,
    slotToken: 'slot-1',
    releaseOwnerRequired: true,
    meta: {
      attemptVersion: 7,
      attemptTicket: 2,
    },
  });

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.equal(result.kind, 'granted');
    assert.equal(result.attemptVersion, 7);
    assert.equal(result.attemptTicket, 2);
    assert.equal(fqContext.queryToken, 'query-granted');
    assert.equal(fqContext.invocationEpoch, 6);
    assert.equal(fqContext.slotToken, 'slot-1');
    assert.equal(fqContext.releaseOwnerRequired, true);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler client marks owner-routed release only when granted response requires it', async () => {
  const { createSlotHandlerClient } = __fairQueueTestHooks;
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => createFairQueueWaitSseResponse({
    result: 'granted',
    queryToken: 'query-granted-owner',
    invocationEpoch: 9,
    slotToken: 'slot-owner',
    releaseOwnerRequired: true,
  });

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.equal(result.kind, 'granted');
    assert.equal(fqContext.queryToken, 'query-granted-owner');
    assert.equal(fqContext.invocationEpoch, 9);
    assert.equal(fqContext.slotToken, 'slot-owner');
    assert.equal(fqContext.releaseOwnerRequired, true);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler client rejects granted SSE final results missing repeated ownership', async () => {
  const { createSlotHandlerClient } = __fairQueueTestHooks;
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => createStreamingSseResponse([
    'event: accepted\n',
    `data: ${JSON.stringify({ queryToken: 'query-owned', invocationEpoch: 3, deadlineMs: Date.now() + 1_000 })}\n\n`,
    'event: result\n',
    `data: ${JSON.stringify({ result: 'granted', slotToken: 'slot-1' })}\n\n`,
  ]);

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.deepEqual(result, { kind: 'timeout', reason: 'slot-handler-invalid-response' });
    assert.equal(fqContext.queryToken, 'query-owned');
    assert.equal(fqContext.invocationEpoch, 3);
    assert.equal(fqContext.slotToken, undefined);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler client preserves retryAfter for HALF_OPEN_FULL responses', async () => {
  const { createSlotHandlerClient } = __fairQueueTestHooks;
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => createFairQueueWaitSseResponse({
    result: 'throttled',
    queryToken: 'query-throttled',
    invocationEpoch: 5,
    reason: 'try_acquire_half_open_full',
    throttleCode: 503,
    retryAfter: 9,
  });

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.equal(result.kind, 'throttled');
    assert.equal(result.throttleCode, 503);
    assert.equal(result.retryAfter, 9);
    assert.equal(fqContext.queryToken, null);
    assert.equal(fqContext.invocationEpoch, null);
    assert.equal(fqContext.slotToken, null);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler client rejects throttled SSE final results missing repeated ownership', async () => {
  const { createSlotHandlerClient } = __fairQueueTestHooks;
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => createStreamingSseResponse([
    'event: accepted\n',
    `data: ${JSON.stringify({ queryToken: 'query-owned', invocationEpoch: 3, deadlineMs: Date.now() + 1_000 })}\n\n`,
    'event: result\n',
    `data: ${JSON.stringify({ result: 'throttled', throttleCode: 503 })}\n\n`,
  ]);

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.deepEqual(result, { kind: 'timeout', reason: 'slot-handler-invalid-response' });
    assert.equal(fqContext.queryToken, 'query-owned');
    assert.equal(fqContext.invocationEpoch, 3);
    assert.equal(fqContext.slotToken, undefined);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler client clears full ownership tuple on timeout responses', async () => {
  const { createSlotHandlerClient } = __fairQueueTestHooks;
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => createFairQueueWaitSseResponse({
    result: 'timeout',
    queryToken: 'query-timeout',
    invocationEpoch: 7,
  });

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.deepEqual(result, { kind: 'timeout', reason: 'slot-handler-timeout' });
    assert.equal(fqContext.queryToken, null);
    assert.equal(fqContext.invocationEpoch, null);
    assert.equal(fqContext.slotToken, null);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler client clears accepted tuple on overloaded SSE results', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => createFairQueueWaitSseResponse({
    result: 'overloaded',
    queryToken: 'query-owned',
    invocationEpoch: 3,
    reason: 'overload_global',
    retryAfter: 2,
  });

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.deepEqual(result, {
      kind: 'overloaded',
      scope: 'global',
      reason: 'overload_global',
      retryAfter: 2,
    });
    assert.equal(fqContext.queryToken, null);
    assert.equal(fqContext.invocationEpoch, null);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('slot-handler client clears accepted tuple on conflict responses', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 50,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => createFairQueueWaitSseResponse({
    result: 'conflict',
    queryToken: 'query-conflict',
    invocationEpoch: 8,
  });

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.deepEqual(result, { kind: 'conflict', reason: null });
    assert.equal(fqContext.queryToken, null);
    assert.equal(fqContext.invocationEpoch, null);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('conflict final result does not emit abandon after accepted tuple', async () => {
  const { clearOverloadedByHost } = __fairQueueTestHooks;
  const waitUntilPromises = [];
  const abandonBodies = [];
  const releaseBodies = [];
  let abandonCalls = 0;
  const originalFetch = globalThis.fetch;
  delete globalThis.bootstrapCache;
  clearOverloadedByHost();

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({ fairQueueHostPatterns: ['*.sharepoint.com'] }));
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

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/wait') {
      return createFairQueueWaitSseResponse({
        result: 'conflict',
        queryToken: 'query-conflict-owned',
        invocationEpoch: 21,
      }, {
        acceptedDeadlineMs: JSON.parse(init.body).deadlineMs,
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/abandon') {
      abandonCalls += 1;
      abandonBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/fairqueue-conflict-single-owner.bin'), buildWorkerEnv(), {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    assert.equal(response.status, 503);
    await waitForNextTurn();
    await Promise.allSettled(waitUntilPromises);
    await waitForNextTurn();

    assert.equal(abandonCalls, 0);
    assert.deepEqual(abandonBodies, []);
    assert.deepEqual(releaseBodies, []);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
    delete globalThis.bootstrapCache;
  }
});

test('global overload final result does not emit abandon after accepted tuple', async () => {
  const { clearOverloadedByHost } = __fairQueueTestHooks;
  const waitUntilPromises = [];
  const abandonBodies = [];
  const releaseBodies = [];
  let abandonCalls = 0;
  const originalFetch = globalThis.fetch;
  delete globalThis.bootstrapCache;
  clearOverloadedByHost();

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(buildRuntimeBootstrap({ fairQueueHostPatterns: ['*.sharepoint.com'] }));
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

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/wait') {
      return createFairQueueWaitSseResponse({
        result: 'overloaded',
        queryToken: 'query-global-owned',
        invocationEpoch: 31,
        reason: 'overload_global',
        retryAfter: 2,
      }, {
        acceptedDeadlineMs: JSON.parse(init.body).deadlineMs,
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/abandon') {
      abandonCalls += 1;
      abandonBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/fairqueue-global-overload-single-owner.bin'), buildWorkerEnv(), {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    assert.equal(response.status, 503);
    await waitForNextTurn();
    await Promise.allSettled(waitUntilPromises);
    await waitForNextTurn();

    assert.equal(abandonCalls, 0);
    assert.deepEqual(abandonBodies, []);
    assert.deepEqual(releaseBodies, []);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
    delete globalThis.bootstrapCache;
  }
});

test('resolveConfig wires controller slotHandlerAuthHeader into slot-handler requests', async () => {
  const { resolveConfig, createSlotHandlerClient } = __fairQueueTestHooks;
  const config = resolveConfig(
    {},
    {
      common: {
        tokenHmacKey: 'bootstrap-token',
        workerAddresses: ['https://worker.example.com'],
        landingWorkerAddresses: ['https://landing.example.com'],
      },
      download: {
        address: 'https://alist.example.com',
        throttleProfiles: {
          default: {
            hostPatterns: [],
            openCapSeconds: 60,
            openThresholdPercent: 20,
            ewmaSpan: 8,
            consecutiveThreshold: 4,
            protectHttpCodes: [429, 499, 500, 502, 503, 504],
          },
        },
        fairQueue: {
          enabled: true,
          hostPatterns: ['example.com'],
          slotHandlerUrl: 'https://slot-handler.example.com',
          slotHandlerAuthKey: 'secret',
          slotHandlerAuthHeader: 'X-Bootstrap-FQ-Auth',
        },
      },
    },
    { download: {} },
  );

  assert.equal(config.slotHandlerConfig.authHeader, 'X-Bootstrap-FQ-Auth');

  const client = createSlotHandlerClient(config);
  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (_url, init) => {
    const headers = new Headers(init?.headers);
    assert.equal(headers.get('X-Bootstrap-FQ-Auth'), 'secret');
    assert.equal(headers.get('X-FQ-Auth'), null);
    return createFairQueueWaitSseResponse({
      result: 'granted',
      queryToken: 'query-bootstrap-auth',
      invocationEpoch: 1,
      slotToken: 'slot-1',
      releaseOwnerRequired: true,
    });
  };

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.equal(result.kind, 'granted');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('global overload should fail fast with Retry-After', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  let fetchCalls = 0;
  let seenUrl = null;
  let capturedResponse = null;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url) => {
    fetchCalls += 1;
    seenUrl = String(url);
    capturedResponse = new Response(JSON.stringify({
      result: 'overloaded',
      reason: 'overload_global',
      retryAfter: 3,
    }), {
      status: 503,
      headers: { 'content-type': 'application/json' },
    });
    return capturedResponse.clone();
  };

  try {
    const startedAt = Date.now();
    const result = await client.waitForSlot({}, fqContext);
    const elapsedMs = Date.now() - startedAt;
    const body = await capturedResponse.json();

    assert.deepEqual(result, {
      kind: 'overloaded',
      scope: 'global',
      reason: 'overload_global',
      retryAfter: 3,
    });
    assert.equal(seenUrl, 'https://slot-handler.example.com/api/v1/fairqueue/wait');
    assert.equal(capturedResponse.status, 503);
    assert.equal(body.result, 'overloaded');
    assert.equal(body.reason.startsWith('overload_'), true);
    assert.equal(fetchCalls, 1);
    assert.ok(elapsedMs < 200, `expected fail-fast global overload, got ${elapsedMs}ms`);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('global overload cooldown should suppress repeated wait calls', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  let fetchCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    if (fetchCalls === 1) {
      return new Response(JSON.stringify({
        result: 'overloaded',
        reason: 'overload_global',
        retryAfter: 2,
      }), {
        status: 503,
        headers: { 'content-type': 'application/json' },
      });
    }
    return new Response(JSON.stringify({
      result: 'granted',
      queryToken: 'query-global-cooldown-grant',
      invocationEpoch: 1,
      slotToken: 'slot-1',
    }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const first = await client.waitForSlot({}, fqContext);
    assert.deepEqual(first, {
      kind: 'overloaded',
      scope: 'global',
      reason: 'overload_global',
      retryAfter: 2,
    });
    assert.equal(fetchCalls, 1);

    const startedAt = Date.now();
    const second = await client.waitForSlot({}, fqContext);
    const elapsedMs = Date.now() - startedAt;
    assert.equal(second.kind, 'overloaded');
    assert.equal(second.scope, 'global');
    assert.ok(second.retryAfter >= 1);
    assert.equal(fetchCalls, 1);
    assert.ok(elapsedMs < 200, `expected cached global overload short-circuit, got ${elapsedMs}ms`);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('host-scoped overload setup failure should fail fast without retry loop', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  let fetchCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    return new Response(JSON.stringify({
      result: 'overloaded',
      reason: 'overload_host',
      retryAfter: 2,
    }), {
      status: 503,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const startedAt = Date.now();
    const result = await client.waitForSlot({}, fqContext);
    const elapsedMs = Date.now() - startedAt;

    assert.deepEqual(result, {
      kind: 'overloaded',
      scope: 'host',
      reason: 'overload_host',
      retryAfter: 2,
    });
    assert.equal(fetchCalls, 1);
    assert.ok(elapsedMs < FAIL_FAST_WAIT_MAX_MS, `expected fail-fast host overload, got ${elapsedMs}ms`);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('site-scoped overload setup failure should fail fast without retry loop', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  let fetchCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    return new Response(JSON.stringify({
      result: 'overloaded',
      reason: 'overload_site',
      retryAfter: 2,
    }), {
      status: 503,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const startedAt = Date.now();
    const result = await client.waitForSlot({}, fqContext);
    const elapsedMs = Date.now() - startedAt;

    assert.deepEqual(result, {
      kind: 'overloaded',
      scope: 'site',
      reason: 'overload_site',
      retryAfter: 2,
    });
    assert.equal(fetchCalls, 1);
    assert.ok(elapsedMs < FAIL_FAST_WAIT_MAX_MS, `expected fail-fast site overload, got ${elapsedMs}ms`);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('ip-scoped overload setup failure should fail fast without retry loop', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  let fetchCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    return new Response(JSON.stringify({
      result: 'overloaded',
      reason: 'overload_ip',
      retryAfter: 2,
    }), {
      status: 503,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const startedAt = Date.now();
    const result = await client.waitForSlot({}, fqContext);
    const elapsedMs = Date.now() - startedAt;

    assert.deepEqual(result, {
      kind: 'overloaded',
      scope: 'ip',
      reason: 'overload_ip',
      retryAfter: 2,
    });
    assert.equal(fetchCalls, 1);
    assert.ok(elapsedMs < FAIL_FAST_WAIT_MAX_MS, `expected fail-fast ip overload, got ${elapsedMs}ms`);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('site-scoped overload does not suppress other sites under same host', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 300,
      authKey: '',
    },
  });

  const siteAContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-a',
    siteBucket: 'site-a',
  };

  const siteBContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-a',
    siteBucket: 'site-b',
  };

  let fetchCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    if (fetchCalls === 1) {
      return new Response(JSON.stringify({
        result: 'overloaded',
        reason: 'overload_site',
        retryAfter: 2,
      }), {
        status: 503,
        headers: { 'content-type': 'application/json' },
      });
    }
    return createFairQueueWaitSseResponse({
      result: 'granted',
      queryToken: 'query-site-b-grant',
      invocationEpoch: 1,
      slotToken: 'slot-site-b-1',
      releaseOwnerRequired: true,
    });
  };

  try {
    const first = await client.waitForSlot({}, siteAContext);
    assert.deepEqual(first, {
      kind: 'overloaded',
      scope: 'site',
      reason: 'overload_site',
      retryAfter: 2,
    });

    const startedAt = Date.now();
    const second = await client.waitForSlot({}, siteBContext);
    const elapsedMs = Date.now() - startedAt;

    assert.equal(second.kind, 'granted');
    assert.equal(fetchCalls, 2);
    assert.ok(elapsedMs < 250, `expected no host-wide suppression for other site, got ${elapsedMs}ms`);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('ip-scoped overload does not suppress other ip buckets under same host and site', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 300,
      authKey: '',
    },
  });

  const ipAContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-a',
    siteBucket: 'site-a',
  };

  const ipBContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-b',
    siteBucket: 'site-a',
  };

  let fetchCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    if (fetchCalls === 1) {
      return new Response(JSON.stringify({
        result: 'overloaded',
        reason: 'overload_ip',
        retryAfter: 2,
      }), {
        status: 503,
        headers: { 'content-type': 'application/json' },
      });
    }
    return createFairQueueWaitSseResponse({
      result: 'granted',
      queryToken: 'query-ip-b-grant',
      invocationEpoch: 1,
      slotToken: 'slot-ip-b-1',
      releaseOwnerRequired: true,
    });
  };

  try {
    const first = await client.waitForSlot({}, ipAContext);
    assert.deepEqual(first, {
      kind: 'overloaded',
      scope: 'ip',
      reason: 'overload_ip',
      retryAfter: 2,
    });

    const startedAt = Date.now();
    const second = await client.waitForSlot({}, ipBContext);
    const elapsedMs = Date.now() - startedAt;

    assert.equal(second.kind, 'granted');
    assert.equal(fetchCalls, 2);
    assert.ok(elapsedMs < 250, `expected no host-wide suppression for other ip bucket, got ${elapsedMs}ms`);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('unknown scoped overload reason does not create scoped cooldown state', async () => {
  const {
    createSlotHandlerClient,
    clearOverloadedByHost,
    getHostOverloadedRemainingMs,
    getSiteOverloadedRemainingMs,
    getIpOverloadedRemainingMs,
  } = __fairQueueTestHooks;
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 300,
      authKey: '',
    },
  });

  const siteAContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-a',
    siteBucket: 'site-a',
  };

  const siteBContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-a',
    siteBucket: 'site-b',
  };

  let fetchCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    if (fetchCalls === 1) {
      return new Response(JSON.stringify({
        result: 'overloaded',
        reason: 'unexpected_overload_scope',
      }), {
        status: 503,
        headers: { 'content-type': 'application/json' },
      });
    }
    return createFairQueueWaitSseResponse({
      result: 'granted',
      queryToken: 'query-unknown-scope-grant',
      invocationEpoch: 1,
      slotToken: 'slot-2',
      releaseOwnerRequired: true,
    });
  };

  try {
    const first = await client.waitForSlot({}, siteAContext);
    assert.deepEqual(first, {
      kind: 'overloaded',
      scope: 'unexpected_overload_scope',
      reason: 'unexpected_overload_scope',
      retryAfter: 60,
    });
    assert.equal(getHostOverloadedRemainingMs(siteAContext.hostname), 0);
    assert.equal(getSiteOverloadedRemainingMs(siteAContext.hostname, siteAContext.siteBucket), 0);
    assert.equal(getIpOverloadedRemainingMs(siteAContext.hostname, siteAContext.siteBucket, siteAContext.ipBucket), 0);

    const startedAt = Date.now();
    const second = await client.waitForSlot({}, siteBContext);
    const elapsedMs = Date.now() - startedAt;

    assert.equal(second.kind, 'granted');
    assert.equal(fetchCalls, 2);
    assert.ok(elapsedMs < 250, `expected unknown scoped overload not to suppress later requests, got ${elapsedMs}ms`);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('ip-scoped keys with embedded delimiters do not collide', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 300,
      authKey: '',
    },
  });

  const contextA = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    siteBucket: 'site\x00part',
    ipBucket: 'ip',
  };

  const contextB = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    siteBucket: 'site',
    ipBucket: 'part\x00ip',
  };

  let fetchCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    if (fetchCalls === 1) {
      return new Response(JSON.stringify({
        result: 'overloaded',
        reason: 'overload_ip',
        retryAfter: 2,
      }), {
        status: 503,
        headers: { 'content-type': 'application/json' },
      });
    }
    return createFairQueueWaitSseResponse({
      result: 'granted',
      queryToken: 'query-safe-key-grant',
      invocationEpoch: 1,
      slotToken: 'slot-safe-key',
      releaseOwnerRequired: true,
    });
  };

  try {
    const first = await client.waitForSlot({}, contextA);
    assert.deepEqual(first, {
      kind: 'overloaded',
      scope: 'ip',
      reason: 'overload_ip',
      retryAfter: 2,
    });

    const second = await client.waitForSlot({}, contextB);
    assert.equal(second.kind, 'granted');
    assert.equal(fetchCalls, 2);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('scoped overload maps opportunistically clean expired entries', async () => {
  const {
    clearOverloadedByHost,
    markSiteOverloaded,
    markIpOverloaded,
    getSiteOverloadedRemainingMs,
    getIpOverloadedRemainingMs,
    getOverloadedMapSizes,
  } = __fairQueueTestHooks;

  clearOverloadedByHost();

  try {
    for (let i = 0; i < 24; i += 1) {
      markSiteOverloaded('cleanup.example', `site-${i}`, 1);
      markIpOverloaded('cleanup.example', `site-${i}`, `ip-${i}`, 1);
    }

    await new Promise((resolve) => setTimeout(resolve, 10));

    const before = getOverloadedMapSizes();
    getSiteOverloadedRemainingMs('cleanup.example', 'fresh-site');
    getIpOverloadedRemainingMs('cleanup.example', 'fresh-site', 'fresh-ip');
    const after = getOverloadedMapSizes();

    assert.ok(before.site > 0 && before.ip > 0);
    assert.ok(after.site < before.site, `expected site map cleanup: ${before.site} -> ${after.site}`);
    assert.ok(after.ip < before.ip, `expected ip map cleanup: ${before.ip} -> ${after.ip}`);
  } finally {
    clearOverloadedByHost();
  }
});

test('scoped overload setup failure leaves ownership metadata empty', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 45000,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    return new Response(JSON.stringify({
      result: 'overloaded',
      reason: 'overload_host',
      retryAfter: 4,
    }), {
      status: 503,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.deepEqual(result, {
      kind: 'overloaded',
      scope: 'host',
      reason: 'overload_host',
      retryAfter: 4,
    });
    assert.equal(fqContext.queryToken, undefined);
    assert.equal(fqContext.invocationEpoch, undefined);
    assert.equal(fqContext.slotToken, undefined);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('releaseSlot retries timed out releases with dedicated 1500ms timeout', async () => {
  const { createSlotHandlerClient } = __fairQueueTestHooks;
  const originalWarn = console.warn;
  console.warn = () => {};
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: '',
    },
  });
  console.warn = originalWarn;

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    slotToken: 'slot-timeout',
    queryToken: 'query-release-timeout',
    invocationEpoch: 1,
    releaseOwnerRequired: false,
    nowMs: Date.now(),
  };

  let calls = 0;
  let firstAttemptSawSignal = false;
  let firstAbortElapsedMs = null;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (_url, init) => {
    calls += 1;
    if (calls === 1) {
      const signal = init?.signal;
      firstAttemptSawSignal = Boolean(signal);
      const startedAt = Date.now();
      return new Promise((_resolve, reject) => {
        if (!signal) {
          return;
        }
        const rejectAborted = () => {
          firstAbortElapsedMs = Date.now() - startedAt;
          const error = new Error('Aborted');
          error.name = 'AbortError';
          reject(error);
        };
        if (signal.aborted) {
          rejectAborted();
          return;
        }
        signal.addEventListener('abort', rejectAborted, { once: true });
      });
    }

    return Promise.resolve(new Response(JSON.stringify({ result: 'ok' }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    }));
  };

  const timedOut = Symbol('test-timeout');

  try {
    const result = await Promise.race([
      client.releaseSlot({}, fqContext),
      new Promise((resolve) => setTimeout(() => resolve(timedOut), 3200)),
    ]);

    assert.equal(result, true);
    assert.equal(calls, 2);
    assert.equal(firstAttemptSawSignal, true);
    assert.ok(firstAbortElapsedMs !== null, 'expected first release attempt to abort');
    assert.ok(firstAbortElapsedMs >= 1300, `expected dedicated release timeout near 1500ms, got ${firstAbortElapsedMs}ms`);
    assert.ok(
      firstAbortElapsedMs < 2400,
      `expected dedicated 1500ms release timeout instead of any legacy long-poll clamp, got ${firstAbortElapsedMs}ms`
    );
  } finally {
    globalThis.fetch = originalFetch;
    console.warn = originalWarn;
  }
});

test('releaseSlot retries on retryable status and network errors', async () => {
  const { createSlotHandlerClient } = __fairQueueTestHooks;
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    slotToken: 'slot-1',
    queryToken: 'query-release-retryable',
    invocationEpoch: 1,
    releaseOwnerRequired: false,
    nowMs: Date.now(),
  };

  let calls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    calls += 1;
    if (calls === 1) {
      return new Response('rate limited', { status: 429 });
    }
    if (calls === 2) {
      throw new Error('network down');
    }
    return new Response(JSON.stringify({ result: 'ok' }), { status: 200 });
  };

  try {
    await client.releaseSlot({}, fqContext);
    assert.equal(calls, 3);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('releaseSlot does not retry on non-retryable 4xx', async () => {
  const { createSlotHandlerClient } = __fairQueueTestHooks;
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    slotToken: 'slot-1',
    queryToken: 'query-release-4xx',
    invocationEpoch: 1,
    releaseOwnerRequired: false,
    nowMs: Date.now(),
  };

  let calls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    calls += 1;
    return new Response('bad request', { status: 400 });
  };

  try {
    await client.releaseSlot({}, fqContext);
    assert.equal(calls, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
