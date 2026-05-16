import { test } from 'node:test';
import assert from 'node:assert/strict';
import worker, { __fairQueueTestHooks } from '../src/worker.js';
import { encryptBindingPayload } from '../src/origin-binding.js';

const { createSlotHandlerClient } = __fairQueueTestHooks;

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

const createAcceptedOnlyFairQueueWaitResponse = ({
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
  configVersion: 'task3-runtime',
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

const buildSignedWorkerRequest = async (pathname = '/downloads/task3.bin', options = {}) => {
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

test('slowFailDelay is bypassed under node test runner', async () => {
  const startedAt = Date.now();
  await __fairQueueTestHooks.slowFailDelay();
  const elapsedMs = Date.now() - startedAt;

  assert.ok(elapsedMs < 100, `expected node test slowFailDelay bypass, got ${elapsedMs}ms`);
});

test('slot-handler client sends abandon with queryToken invocationEpoch and auth header', async () => {
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      authKey: 'secret',
      authHeader: 'X-FQ-Auth',
    },
  });
  const fqContext = { queryToken: 'query-1', invocationEpoch: 7 };
  let seenUrl = null;
  let seenBody = null;
  let seenAuthHeader = null;

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init) => {
    seenUrl = String(url);
    seenBody = JSON.parse(init.body);
    seenAuthHeader = new Headers(init.headers).get('X-FQ-Auth');
    return new Response(null, { status: 204 });
  };

  try {
    const ok = await client.abandonWait({}, fqContext);
    assert.equal(ok, true);
    assert.match(seenUrl, /\/api\/v1\/fairqueue\/abandon$/);
    assert.equal(seenAuthHeader, 'secret');
    assert.deepEqual(seenBody, { queryToken: 'query-1', invocationEpoch: 7 });
    assert.equal(Object.hasOwn(seenBody, 'cleanupRetired'), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler client abandon noops once slotToken exists', async () => {
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      authKey: 'secret',
      authHeader: 'X-FQ-Auth',
    },
  });

  let fetchCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    return new Response(null, { status: 204 });
  };

  try {
    const ok = await client.abandonWait({}, {
      queryToken: 'query-1',
      slotToken: 'slot-1',
    });
    assert.equal(ok, true);
    assert.equal(fetchCalls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler client sends claimed release with full release identity and owner routing headers', async () => {
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      authKey: 'secret',
      authHeader: 'X-FQ-Auth',
    },
  });
  const fqContext = {
    hostname: 'tenant.sharepoint.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    slotToken: 'slot-1',
    queryToken: 'query-1',
    invocationEpoch: 7,
    releaseOwnerRequired: true,
    hitUpstreamAtMs: 123,
  };
  let seenUrl = null;
  let seenBody = null;
  let seenAuthHeader = null;
  let seenOwnerTokenHeader = null;
  let seenOwnerEpochHeader = null;

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init) => {
    seenUrl = String(url);
    seenBody = JSON.parse(init.body);
    const headers = new Headers(init.headers);
    seenAuthHeader = headers.get('X-FQ-Auth');
    seenOwnerTokenHeader = headers.get('X-FQ-Owner-Token');
    seenOwnerEpochHeader = headers.get('X-FQ-Owner-Epoch');
    return new Response(null, { status: 204 });
  };

  try {
    const ok = await client.releaseSlot({}, fqContext);
    assert.equal(ok, true);
    assert.match(seenUrl, /\/api\/v1\/fairqueue\/release$/);
    assert.equal(seenAuthHeader, 'secret');
    assert.equal(seenOwnerTokenHeader, 'query-1');
    assert.equal(seenOwnerEpochHeader, '7');
    assert.equal(seenBody.hostname, 'tenant.sharepoint.com');
    assert.equal(seenBody.hostnameHash, 'host-hash');
    assert.equal(seenBody.ipBucket, 'ip-bucket');
    assert.equal(seenBody.siteBucket, 'site-bucket');
    assert.equal(seenBody.slotToken, 'slot-1');
    assert.equal(seenBody.queryToken, 'query-1');
    assert.equal(seenBody.invocationEpoch, 7);
    assert.equal(seenBody.releaseOwnerRequired, true);
    assert.equal(seenBody.hitUpstreamAtMs, 123);
    assert.equal(typeof seenBody.now, 'number');
    assert.equal(Object.hasOwn(seenBody, 'cleanupRetired'), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler client sends direct release with full release identity but no owner routing headers', async () => {
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      authKey: 'secret',
      authHeader: 'X-FQ-Auth',
    },
  });
  const fqContext = {
    hostname: 'tenant.sharepoint.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    slotToken: 'slot-plain-release',
    queryToken: 'query-plain-release',
    invocationEpoch: 12,
    releaseOwnerRequired: false,
    hitUpstreamAtMs: 456,
  };
  let seenBody = null;
  let seenOwnerTokenHeader = null;
  let seenOwnerEpochHeader = null;

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (_url, init) => {
    seenBody = JSON.parse(init.body);
    const headers = new Headers(init.headers);
    seenOwnerTokenHeader = headers.get('X-FQ-Owner-Token');
    seenOwnerEpochHeader = headers.get('X-FQ-Owner-Epoch');
    return new Response(null, { status: 204 });
  };

  try {
    const ok = await client.releaseSlot({}, fqContext);
    assert.equal(ok, true);
    assert.equal(seenBody.hostname, 'tenant.sharepoint.com');
    assert.equal(seenBody.hostnameHash, 'host-hash');
    assert.equal(seenBody.ipBucket, 'ip-bucket');
    assert.equal(seenBody.siteBucket, 'site-bucket');
    assert.equal(seenBody.slotToken, 'slot-plain-release');
    assert.equal(seenBody.queryToken, 'query-plain-release');
    assert.equal(seenBody.invocationEpoch, 12);
    assert.equal(seenBody.releaseOwnerRequired, false);
    assert.equal(seenOwnerTokenHeader, null);
    assert.equal(seenOwnerEpochHeader, null);
    assert.equal(Object.hasOwn(seenBody, 'cleanupRetired'), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler client abandon skips without valid invocationEpoch', async () => {
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      authKey: 'secret',
      authHeader: 'X-FQ-Auth',
    },
  });

  let fetchCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    return new Response(null, { status: 204 });
  };

  try {
    const missingEpoch = await client.abandonWait({}, { queryToken: 'query-1' });
    const zeroEpoch = await client.abandonWait({}, { queryToken: 'query-2', invocationEpoch: 0 });
    assert.equal(missingEpoch, true);
    assert.equal(zeroEpoch, true);
    assert.equal(fetchCalls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('terminal timeout result does not emit abandon after accepted tuple', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const abandonBodies = [];
  const releaseBodies = [];
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
          url: 'https://tenant.sharepoint.com/start',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/wait') {
      const payload = JSON.parse(init.body);
      return createFairQueueWaitSseResponse({
        result: 'timeout',
        queryToken: 'query-timeout',
        invocationEpoch: 4,
      }, {
        acceptedDeadlineMs: payload.deadlineMs,
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/abandon') {
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
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/fairqueue-timeout-cleanup.bin'), buildWorkerEnv(), {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.deepEqual(abandonBodies, []);
    assert.deepEqual(releaseBodies, []);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('timeout early-return does not emit abandon after terminal SSE result', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const abandonBodies = [];
  const releaseBodies = [];
  let abandonCalls = 0;
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
          url: 'https://tenant.sharepoint.com/start',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/wait') {
      const payload = JSON.parse(init.body);
      return createFairQueueWaitSseResponse({
        result: 'timeout',
        queryToken: 'query-timeout-single-owner',
        invocationEpoch: 14,
      }, {
        acceptedDeadlineMs: payload.deadlineMs,
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
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/fairqueue-timeout-single-owner.bin'), buildWorkerEnv(), {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    assert.equal(response.status, 503);
    await waitForNextTurn();
    await Promise.allSettled(waitUntilPromises);

    assert.equal(abandonCalls, 0);
    assert.deepEqual(abandonBodies, []);
    assert.deepEqual(releaseBodies, []);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('timeout early-return does not schedule retry cleanup after terminal SSE result', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const abandonBodies = [];
  const releaseBodies = [];
  let abandonCalls = 0;
  let activeAbandonCalls = 0;
  let maxActiveAbandonCalls = 0;
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
          url: 'https://tenant.sharepoint.com/start',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/wait') {
      const payload = JSON.parse(init.body);
      return createFairQueueWaitSseResponse({
        result: 'timeout',
        queryToken: 'query-timeout-retry',
        invocationEpoch: 18,
      }, {
        acceptedDeadlineMs: payload.deadlineMs,
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/abandon') {
      abandonCalls += 1;
      activeAbandonCalls += 1;
      maxActiveAbandonCalls = Math.max(maxActiveAbandonCalls, activeAbandonCalls);
      abandonBodies.push(JSON.parse(init.body));
      try {
        return createJsonResponse({ result: 'ok' });
      } finally {
        activeAbandonCalls -= 1;
      }
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/fairqueue-timeout-retry.bin'), buildWorkerEnv(), {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    assert.equal(response.status, 503);
    await waitForNextTurn();
    await Promise.allSettled(waitUntilPromises);

    assert.equal(abandonCalls, 0);
    assert.equal(maxActiveAbandonCalls, 0);
    assert.deepEqual(abandonBodies, []);
    assert.deepEqual(releaseBodies, []);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('client abort before grant uses abandon cleanup', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const waitBodies = [];
  const abandonBodies = [];
  const releaseBodies = [];
  delete globalThis.bootstrapCache;

  const controller = new AbortController();

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
          url: 'https://tenant.sharepoint.com/start',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/wait') {
      waitBodies.push(JSON.parse(init.body));
      setTimeout(() => controller.abort(), 0);
      return createAcceptedOnlyFairQueueWaitResponse({
        queryToken: 'fq-q1',
        invocationEpoch: 1,
        deadlineMs: Date.now() + 1_000,
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/abandon') {
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
    const baseRequest = await buildSignedWorkerRequest('/downloads/fairqueue-abort-cleanup.bin');
    const request = new Request(baseRequest, { signal: controller.signal });
    const response = await worker.fetch(request, buildWorkerEnv(), {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.allSettled(waitUntilPromises);

    assert.equal(waitBodies.length, 1);
    assert.equal(response.status, 499);
    assert.deepEqual(abandonBodies, [{ queryToken: 'fq-q1', invocationEpoch: 1 }]);
    assert.deepEqual(releaseBodies, []);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('throttled terminal handling clears ownership tuple without issuing abandon', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const abandonBodies = [];
  const releaseBodies = [];
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
          url: 'https://tenant.sharepoint.com/start',
          header: {},
        },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/wait') {
      const payload = JSON.parse(init.body);
      return createFairQueueWaitSseResponse({
        result: 'throttled',
        queryToken: 'query-throttled',
        invocationEpoch: 6,
        throttleCode: 503,
        retryAfter: 4,
      }, {
        acceptedDeadlineMs: payload.deadlineMs,
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/abandon') {
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
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/fairqueue-throttled-cleanup.bin'), buildWorkerEnv(), {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });

    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.deepEqual(abandonBodies, []);
    assert.deepEqual(releaseBodies, []);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('reconcile retires pre-grant context through abandon', async () => {
  assert.equal(
    typeof __fairQueueTestHooks.reconcileFairQueueContextForTarget,
    'function',
    'reconcile cleanup helper missing: pre-grant abandon routing is not implemented yet',
  );

  const cleanupCalls = [];
  await __fairQueueTestHooks.reconcileFairQueueContextForTarget({
    fairQueueClient: {
      async releaseSlot(_ctx, contextToRelease) {
        cleanupCalls.push({ kind: 'release', token: contextToRelease.slotToken });
        return true;
      },
      async abandonWait(_ctx, contextToAbandon) {
        cleanupCalls.push({
          kind: 'abandon',
          token: contextToAbandon.queryToken,
          invocationEpoch: contextToAbandon.invocationEpoch,
        });
        return true;
      },
    },
    fqContext: {
      hostname: 'a.sharepoint.com',
      hostnameHash: 'hash-a',
      ipBucket: 'ip-bucket',
      siteBucket: 'site-a',
      queryToken: 'query-pre-grant',
      invocationEpoch: 3,
    },
    targetUrl: 'https://b.sharepoint.com/final',
    phase: 'redirect',
  });

  assert.deepEqual(cleanupCalls, [{ kind: 'abandon', token: 'query-pre-grant', invocationEpoch: 3 }]);
});

test('finalizer prefers release when slotToken exists', async () => {
  assert.equal(
    typeof __fairQueueTestHooks.finalizeFairQueueContext,
    'function',
    'unified finalizer helper missing: release-preference cleanup is not implemented yet',
  );

  const cleanupCalls = [];
  const finalized = await __fairQueueTestHooks.finalizeFairQueueContext({
    fairQueueClient: {
      async releaseSlot(_ctx, contextToRelease) {
        cleanupCalls.push({ kind: 'release', token: contextToRelease.slotToken });
        return true;
      },
      async abandonWait(_ctx, contextToAbandon) {
        cleanupCalls.push({ kind: 'abandon', token: contextToAbandon.queryToken });
        return true;
      },
    },
    fqContext: {
      hostname: 'tenant.sharepoint.com',
      hostnameHash: 'host-hash',
      ipBucket: 'ip-bucket',
      siteBucket: 'site-bucket',
      slotToken: 'slot-1',
      queryToken: 'query-1',
    },
    phase: 'final cleanup',
  });

  assert.equal(finalized, true);
  assert.deepEqual(cleanupCalls, [{ kind: 'release', token: 'slot-1' }]);
});

test('grant promotion suppresses abandon during abort/final cleanup race', async () => {
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      authKey: '',
    },
    testHooks: {
      onGrantPromotion(promotedContext) {
        promotedSnapshots.push({
          hostname: promotedContext.hostname,
          hostnameHash: promotedContext.hostnameHash,
          ipBucket: promotedContext.ipBucket,
          siteBucket: promotedContext.siteBucket,
          queryToken: promotedContext.queryToken,
          invocationEpoch: promotedContext.invocationEpoch,
          slotToken: promotedContext.slotToken,
          grantPromoted: promotedContext.grantPromoted,
          slotAcquiredAt: promotedContext.slotAcquiredAt,
          attemptVersion: promotedContext.attemptVersion,
          attemptTicket: promotedContext.attemptTicket,
        });
      },
    },
  });

  const fqContext = {
    hostname: 'tenant.sharepoint.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };
  const promotedSnapshots = [];
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async () => createFairQueueWaitSseResponse({
    result: 'granted',
    queryToken: 'query-granted',
    invocationEpoch: 5,
    slotToken: 'slot-granted',
    releaseOwnerRequired: true,
    meta: {
      attemptVersion: 7,
      attemptTicket: 11,
    },
  });

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.equal(result.kind, 'granted');
  } finally {
    globalThis.fetch = originalFetch;
  }

  assert.equal(promotedSnapshots.length, 1);
  assert.deepEqual(promotedSnapshots[0], {
    hostname: 'tenant.sharepoint.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    queryToken: 'query-granted',
    invocationEpoch: 5,
    slotToken: 'slot-granted',
    grantPromoted: true,
    slotAcquiredAt: fqContext.slotAcquiredAt,
    attemptVersion: 7,
    attemptTicket: 11,
  });

  const cleanupCalls = [];
  const promotedContext = {
    ...promotedSnapshots[0],
  };
  const promotedLeakContext = {
    ...promotedSnapshots[0],
    slotToken: null,
  };

  const finalized = await __fairQueueTestHooks.finalizeFairQueueContext({
    fairQueueClient: {
      async releaseSlot(_ctx, contextToRelease) {
        cleanupCalls.push({
          kind: 'release',
          slotToken: contextToRelease.slotToken,
          queryToken: contextToRelease.queryToken,
          grantPromoted: contextToRelease.grantPromoted,
        });
        return true;
      },
      async abandonWait(_ctx, contextToAbandon) {
        cleanupCalls.push({
          kind: 'abandon',
          queryToken: contextToAbandon.queryToken,
          grantPromoted: contextToAbandon.grantPromoted,
        });
        return true;
      },
    },
    fqContext: promotedContext,
    phase: 'final cleanup',
  });

  assert.equal(finalized, true);
  const finalizedLeak = await __fairQueueTestHooks.finalizeFairQueueContext({
    fairQueueClient: {
      async releaseSlot(_ctx, contextToRelease) {
        cleanupCalls.push({
          kind: 'release',
          slotToken: contextToRelease.slotToken,
          queryToken: contextToRelease.queryToken,
          grantPromoted: contextToRelease.grantPromoted,
        });
        return true;
      },
      async abandonWait(_ctx, contextToAbandon) {
        cleanupCalls.push({
          kind: 'abandon',
          queryToken: contextToAbandon.queryToken,
          grantPromoted: contextToAbandon.grantPromoted,
        });
        return true;
      },
    },
    fqContext: promotedLeakContext,
    phase: 'abort cleanup race fallback',
  });

  assert.equal(finalizedLeak, true);
  assert.equal(promotedLeakContext.slotToken, null);
  assert.equal(promotedLeakContext.queryToken, null);
  assert.equal(promotedLeakContext.invocationEpoch, null);
  assert.deepEqual(cleanupCalls, [{
    kind: 'release',
    slotToken: 'slot-granted',
    queryToken: 'query-granted',
    grantPromoted: true,
  }]);
});

test('successful release clears slotToken queryToken and attempt metadata', async () => {
  const fqContext = {
    hostname: 'tenant.sharepoint.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    slotToken: 'slot-clear',
    queryToken: 'query-clear',
    invocationEpoch: 8,
    attemptVersion: 3,
    attemptTicket: 9,
    slotAcquiredAt: 987654,
  };

  const finalized = await __fairQueueTestHooks.finalizeFairQueueContext({
    fairQueueClient: {
      async releaseSlot() {
        return true;
      },
      async abandonWait() {
        throw new Error('release cleanup should not abandon');
      },
    },
    fqContext,
    phase: 'release cleanup',
  });

  assert.equal(finalized, true);
  assert.equal(fqContext.slotToken, null);
  assert.equal(fqContext.queryToken, null);
  assert.equal(fqContext.invocationEpoch, null);
  assert.equal(fqContext.attemptVersion, null);
  assert.equal(fqContext.attemptTicket, null);
  assert.equal(fqContext.slotAcquiredAt, null);
});

test('successful abandon clears queryToken and pre-grant queue metadata', async () => {
  const fqContext = {
    hostname: 'tenant.sharepoint.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    queryToken: 'query-abandon-clear',
    invocationEpoch: 11,
    attemptVersion: 5,
    attemptTicket: 13,
    deferredReportArmed: true,
    deferredReportStatusCode: 503,
  };

  const finalized = await __fairQueueTestHooks.finalizeFairQueueContext({
    fairQueueClient: {
      async releaseSlot() {
        throw new Error('pre-grant cleanup should not release');
      },
      async abandonWait() {
        return true;
      },
    },
    fqContext,
    phase: 'abandon cleanup',
  });

  assert.equal(finalized, true);
  assert.equal(fqContext.queryToken, null);
  assert.equal(fqContext.invocationEpoch, null);
  assert.equal(fqContext.slotToken, null);
  assert.equal(fqContext.attemptVersion, null);
  assert.equal(fqContext.attemptTicket, null);
  assert.equal(fqContext.deferredReportArmed, false);
  assert.equal(fqContext.deferredReportStatusCode, null);
});

test('late cleanup keeps stale and newer abandon identities distinct by invocationEpoch', () => {
  const cleanupGroups = __fairQueueTestHooks.buildFinalCleanupGroups([
    {
      hostname: 'tenant-a.sharepoint.com',
      hostnameHash: 'host-a',
      queryToken: 'query-shared',
      invocationEpoch: 1,
    },
    {
      hostname: 'tenant-a.sharepoint.com',
      hostnameHash: 'host-a',
      queryToken: 'query-shared',
      invocationEpoch: 2,
    },
  ]);

  assert.deepEqual(
    cleanupGroups.flat().map((context) => `${context.queryToken}:${context.invocationEpoch}`),
    ['query-shared:1', 'query-shared:2'],
  );
});

test('final cleanup dedupes release and abandon identities independently', () => {
  assert.equal(
    typeof __fairQueueTestHooks.buildFinalCleanupGroups,
    'function',
    'mixed cleanup dedupe helper missing: release/abandon grouping is not implemented yet',
  );

  const cleanupGroups = __fairQueueTestHooks.buildFinalCleanupGroups([
    {
      hostname: 'tenant-a.sharepoint.com',
      hostnameHash: 'host-a',
      slotToken: 'slot-dup',
      queryToken: 'query-shadowed',
    },
    {
      hostname: 'tenant-a.sharepoint.com',
      hostnameHash: 'host-a',
      slotToken: 'slot-dup',
      queryToken: 'query-shadowed-2',
    },
    {
      hostname: 'tenant-a.sharepoint.com',
      hostnameHash: 'host-a',
      queryToken: 'query-dup',
    },
    {
      hostname: 'tenant-a.sharepoint.com',
      hostnameHash: 'host-a',
      queryToken: 'query-dup',
    },
    {
      hostname: 'tenant-b.sharepoint.com',
      hostnameHash: 'host-b',
      slotToken: 'slot-b',
      queryToken: 'query-b-shadowed',
    },
    {
      hostname: 'tenant-b.sharepoint.com',
      hostnameHash: 'host-b',
      queryToken: 'query-b',
    },
  ]);

  assert.deepEqual(
    cleanupGroups.flat().map((context) => context.slotToken || context.queryToken),
    ['slot-dup', 'query-dup', 'slot-b', 'query-b'],
  );
});
