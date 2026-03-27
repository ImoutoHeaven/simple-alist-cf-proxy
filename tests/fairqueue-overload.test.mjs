import { test } from 'node:test';
import assert from 'node:assert/strict';
import { nextOverloadDelayMs } from '../src/fairqueue-overload.js';
import worker, { __fairQueueTestHooks } from '../src/worker.js';
import { encryptBindingPayload } from '../src/origin-binding.js';

const SCOPED_OVERLOAD_WAIT_MIN_MS = 350;
const SCOPED_OVERLOAD_WAIT_MAX_MS = 3000;

const createJsonResponse = (payload) => new Response(JSON.stringify(payload), {
  status: 200,
  headers: { 'content-type': 'application/json' },
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

const buildSignedWorkerRequest = async (pathname = '/downloads/chunk1-overload.bin') => {
  const token = 'bootstrap-token';
  const expire = Math.floor(Date.now() / 1000) + 300;
  const encryptedBinding = await encryptBindingPayload({
    v: 2,
    issuer: 'https://landing.example.com',
    workerAddress: 'https://worker.example.com',
  }, token);
  const payload = encodeBase64Url(JSON.stringify({
    v: 1,
    expireTime: expire,
    encrypt: encryptedBinding,
  }));
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
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
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
    return new Response(JSON.stringify({
      result: 'granted',
      queryToken: 'query-auth-default',
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
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
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
    return new Response(JSON.stringify({
      result: 'granted',
      queryToken: 'query-auth-custom',
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
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler client stores queryToken and invocationEpoch on pending responses', async () => {
  const { createSlotHandlerClient } = __fairQueueTestHooks;
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
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
    return new Response(JSON.stringify({
      result: 'pending',
      queryToken: 'query-pending',
      invocationEpoch: 4,
    }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
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

test('slot-handler client rejects pending responses missing accepted ownership', async () => {
  const { createSlotHandlerClient } = __fairQueueTestHooks;
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 50,
      perRequestTimeoutMs: 50,
      maxAttemptsCap: 1,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    queryToken: 'query-owned',
    invocationEpoch: 3,
  };
  const originalFetch = globalThis.fetch;
  const originalMathRandom = Math.random;
  globalThis.fetch = async () => new Response(JSON.stringify({
    result: 'pending',
    queryToken: 'query-partial',
  }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });
  Math.random = () => 0;

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.deepEqual(result, { kind: 'timeout', reason: 'slot-handler-invalid-response' });
    assert.equal(fqContext.queryToken, 'query-owned');
    assert.equal(fqContext.invocationEpoch, 3);
  } finally {
    globalThis.fetch = originalFetch;
    Math.random = originalMathRandom;
  }
});

test('slot-handler client surfaces atomic attempt tokens from granted responses', async () => {
  const { createSlotHandlerClient } = __fairQueueTestHooks;
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
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
    queryToken: 'query-granted',
    invocationEpoch: 6,
    slotToken: 'slot-1',
    meta: {
      attemptVersion: 7,
      attemptTicket: 2,
    },
  }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.equal(result.kind, 'granted');
    assert.equal(result.attemptVersion, 7);
    assert.equal(result.attemptTicket, 2);
    assert.equal(fqContext.queryToken, 'query-granted');
    assert.equal(fqContext.invocationEpoch, 6);
    assert.equal(fqContext.slotToken, 'slot-1');
    assert.equal(fqContext.releaseOwnerRequired, undefined);
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
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
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
    queryToken: 'query-granted-owner',
    invocationEpoch: 9,
    slotToken: 'slot-owner',
    releaseOwnerRequired: true,
  }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
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

test('slot-handler client rejects granted responses missing accepted ownership', async () => {
  const { createSlotHandlerClient } = __fairQueueTestHooks;
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    queryToken: 'query-owned',
    invocationEpoch: 3,
  };

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({
    result: 'granted',
    slotToken: 'slot-1',
    queryToken: 'query-partial',
  }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });

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
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    queryToken: 'query-stale',
    invocationEpoch: 2,
    slotToken: 'slot-stale',
  };

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({
    result: 'throttled',
    queryToken: 'query-throttled',
    invocationEpoch: 5,
    reason: 'try_acquire_half_open_full',
    throttleCode: 503,
    retryAfter: 9,
  }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
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

test('slot-handler client rejects throttled responses missing accepted ownership', async () => {
  const { createSlotHandlerClient } = __fairQueueTestHooks;
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    queryToken: 'query-owned',
    invocationEpoch: 3,
    slotToken: 'slot-owned',
  };

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({
    result: 'throttled',
    throttleCode: 503,
  }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.deepEqual(result, { kind: 'timeout', reason: 'slot-handler-invalid-response' });
    assert.equal(fqContext.queryToken, 'query-owned');
    assert.equal(fqContext.invocationEpoch, 3);
    assert.equal(fqContext.slotToken, 'slot-owned');
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
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    queryToken: 'query-timeout',
    invocationEpoch: 7,
    slotToken: 'slot-timeout',
  };

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({
    result: 'timeout',
  }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
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

test('slot-handler client preserves last owned tuple on overloaded responses', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    queryToken: 'query-owned',
    invocationEpoch: 3,
  };

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({
    result: 'overloaded',
    reason: 'overload_global',
    retryAfter: 2,
  }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.deepEqual(result, {
      kind: 'overloaded',
      scope: 'global',
      retryAfter: 2,
    });
    assert.equal(fqContext.queryToken, 'query-owned');
    assert.equal(fqContext.invocationEpoch, 3);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('slot-handler client preserves last owned tuple on conflict responses', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 50,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    queryToken: 'query-conflict',
    invocationEpoch: 8,
  };
  const originalMathRandom = Math.random;

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({
    result: 'conflict',
  }), {
    status: 409,
    headers: { 'content-type': 'application/json' },
  });

  Math.random = () => 0;

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.deepEqual(result, { kind: 'conflict' });
    assert.equal(fqContext.queryToken, 'query-conflict');
    assert.equal(fqContext.invocationEpoch, 8);
  } finally {
    globalThis.fetch = originalFetch;
    Math.random = originalMathRandom;
    clearOverloadedByHost();
  }
});

test('conflict early-return emits exactly one abandon for one accepted tuple', async () => {
  const { clearOverloadedByHost } = __fairQueueTestHooks;
  const waitUntilPromises = [];
  const abandonBodies = [];
  const releaseBodies = [];
  const firstAbandon = createDeferred();
  let abandonCalls = 0;
  let acquireCalls = 0;
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

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireCalls += 1;
      if (acquireCalls === 1) {
        return createJsonResponse({
          result: 'pending',
          queryToken: 'query-conflict-owned',
          invocationEpoch: 21,
        });
      }
      return new Response(JSON.stringify({
        result: 'conflict',
      }), {
        status: 409,
        headers: { 'content-type': 'application/json' },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/abandon') {
      abandonCalls += 1;
      abandonBodies.push(JSON.parse(init.body));
      if (abandonCalls === 1) {
        return await firstAbandon.promise;
      }
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
    assert.equal(abandonCalls, 1);

    firstAbandon.resolve(createJsonResponse({ result: 'ok' }));
    await Promise.allSettled(waitUntilPromises);
    await waitForNextTurn();

    assert.deepEqual(abandonBodies, [{ queryToken: 'query-conflict-owned', invocationEpoch: 21 }]);
    assert.deepEqual(releaseBodies, []);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
    delete globalThis.bootstrapCache;
  }
});

test('global overload early-return emits exactly one abandon for one accepted tuple', async () => {
  const { clearOverloadedByHost } = __fairQueueTestHooks;
  const waitUntilPromises = [];
  const abandonBodies = [];
  const releaseBodies = [];
  const firstAbandon = createDeferred();
  let abandonCalls = 0;
  let acquireCalls = 0;
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

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireCalls += 1;
      if (acquireCalls === 1) {
        return createJsonResponse({
          result: 'pending',
          queryToken: 'query-global-owned',
          invocationEpoch: 31,
        });
      }
      return createJsonResponse({
        result: 'overloaded',
        reason: 'overload_global',
        retryAfter: 2,
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/abandon') {
      abandonCalls += 1;
      abandonBodies.push(JSON.parse(init.body));
      if (abandonCalls === 1) {
        return await firstAbandon.promise;
      }
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
    assert.equal(abandonCalls, 1);

    firstAbandon.resolve(createJsonResponse({ result: 'ok' }));
    await Promise.allSettled(waitUntilPromises);
    await waitForNextTurn();

    assert.deepEqual(abandonBodies, [{ queryToken: 'query-global-owned', invocationEpoch: 31 }]);
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
    return new Response(JSON.stringify({
      result: 'granted',
      queryToken: 'query-bootstrap-auth',
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
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
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
      reason: 'overload_global',
      retryAfter: 3,
    }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const startedAt = Date.now();
    const result = await client.waitForSlot({}, fqContext);
    const elapsedMs = Date.now() - startedAt;

    assert.deepEqual(result, {
      kind: 'overloaded',
      scope: 'global',
      retryAfter: 3,
    });
    assert.equal(fetchCalls, 1);
    assert.ok(elapsedMs < 200, `expected fail-fast global overload, got ${elapsedMs}ms`);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('global overload cooldown should suppress repeated acquire calls', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
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
        status: 200,
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

test('scoped overload should keep bounded wait loop and then grant', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
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
        reason: 'overload_host',
      }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    return new Response(JSON.stringify({
      result: 'granted',
      queryToken: 'query-host-overload-grant',
      invocationEpoch: 1,
      slotToken: 'slot-1',
    }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const startedAt = Date.now();
    const result = await client.waitForSlot({}, fqContext);
    const elapsedMs = Date.now() - startedAt;

    assert.equal(result.kind, 'granted');
    assert.equal(fetchCalls, 2);
    assert.ok(
      elapsedMs >= SCOPED_OVERLOAD_WAIT_MIN_MS,
      `expected bounded wait loop (>=${SCOPED_OVERLOAD_WAIT_MIN_MS}ms), got ${elapsedMs}ms`
    );
    assert.ok(
      elapsedMs <= SCOPED_OVERLOAD_WAIT_MAX_MS,
      `expected bounded wait (<=${SCOPED_OVERLOAD_WAIT_MAX_MS}ms), got ${elapsedMs}ms`
    );
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('site-scoped overload should keep bounded wait loop and then grant', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
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
        reason: 'overload_site',
      }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    return new Response(JSON.stringify({
      result: 'granted',
      queryToken: 'query-site-overload-grant',
      invocationEpoch: 1,
      slotToken: 'slot-site-1',
    }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const startedAt = Date.now();
    const result = await client.waitForSlot({}, fqContext);
    const elapsedMs = Date.now() - startedAt;

    assert.equal(result.kind, 'granted');
    assert.equal(fetchCalls, 2);
    assert.ok(
      elapsedMs >= SCOPED_OVERLOAD_WAIT_MIN_MS,
      `expected bounded wait loop (>=${SCOPED_OVERLOAD_WAIT_MIN_MS}ms), got ${elapsedMs}ms`
    );
    assert.ok(
      elapsedMs <= SCOPED_OVERLOAD_WAIT_MAX_MS,
      `expected bounded wait (<=${SCOPED_OVERLOAD_WAIT_MAX_MS}ms), got ${elapsedMs}ms`
    );
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('ip-scoped overload should keep bounded wait loop and then grant', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
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
        reason: 'overload_ip',
      }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    return new Response(JSON.stringify({
      result: 'granted',
      queryToken: 'query-ip-overload-grant',
      invocationEpoch: 1,
      slotToken: 'slot-ip-1',
    }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const startedAt = Date.now();
    const result = await client.waitForSlot({}, fqContext);
    const elapsedMs = Date.now() - startedAt;

    assert.equal(result.kind, 'granted');
    assert.equal(fetchCalls, 2);
    assert.ok(
      elapsedMs >= SCOPED_OVERLOAD_WAIT_MIN_MS,
      `expected bounded wait loop (>=${SCOPED_OVERLOAD_WAIT_MIN_MS}ms), got ${elapsedMs}ms`
    );
    assert.ok(
      elapsedMs <= SCOPED_OVERLOAD_WAIT_MAX_MS,
      `expected bounded wait (<=${SCOPED_OVERLOAD_WAIT_MAX_MS}ms), got ${elapsedMs}ms`
    );
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('site-scoped overload cooldown should not suppress other sites under same host', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 300,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
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
      }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    return new Response(JSON.stringify({
      result: 'granted',
      queryToken: 'query-site-b-grant',
      invocationEpoch: 1,
      slotToken: 'slot-site-b-1',
    }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const first = await client.waitForSlot({}, siteAContext);
    assert.equal(first.kind, 'timeout');

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

test('ip-scoped overload cooldown should not suppress other ip buckets under same host/site', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 300,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
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
      }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    return new Response(JSON.stringify({
      result: 'granted',
      queryToken: 'query-ip-b-grant',
      invocationEpoch: 1,
      slotToken: 'slot-ip-b-1',
    }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const first = await client.waitForSlot({}, ipAContext);
    assert.equal(first.kind, 'timeout');

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
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
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
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    return new Response(JSON.stringify({
      result: 'granted',
      queryToken: 'query-unknown-scope-grant',
      invocationEpoch: 1,
      slotToken: 'slot-2',
    }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const first = await client.waitForSlot({}, siteAContext);
    assert.equal(first.kind, 'timeout');
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
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
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
      }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    return new Response(JSON.stringify({
      result: 'granted',
      queryToken: 'query-safe-key-grant',
      invocationEpoch: 1,
      slotToken: 'slot-safe-key',
    }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const first = await client.waitForSlot({}, contextA);
    assert.equal(first.kind, 'timeout');

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

test('scoped overload wait uses strict 500ms staircase contract', async () => {
  const { createSlotHandlerClient, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 45000,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
      authKey: '',
    },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
  };

  const fetchCallTimes = [];
  let fetchCalls = 0;
  const originalFetch = globalThis.fetch;
  const originalMathRandom = Math.random;

  globalThis.fetch = async () => {
    fetchCalls += 1;
    fetchCallTimes.push(Date.now());
    if (fetchCalls <= 4) {
      return new Response(JSON.stringify({
        result: 'overloaded',
        reason: 'overload_host',
      }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }

    return new Response(JSON.stringify({
      result: 'granted',
      queryToken: 'query-staircase-grant',
      invocationEpoch: 1,
      slotToken: 'slot-host-1',
    }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  Math.random = () => 1;

  try {
    const result = await client.waitForSlot({}, fqContext);
    assert.equal(result.kind, 'granted');

    const expectedDelays = [500, 1000, 1500, 2000];
    const delayToleranceMs = 90;
    const observedDelays = [];
    for (let i = 1; i < fetchCallTimes.length; i += 1) {
      observedDelays.push(fetchCallTimes[i] - fetchCallTimes[i - 1]);
    }

    assert.equal(observedDelays.length, expectedDelays.length);
    for (let i = 0; i < expectedDelays.length; i += 1) {
      const expected = expectedDelays[i];
      const observed = observedDelays[i];
      assert.ok(
        observed >= expected,
        `expected delay step ${i + 1} >= ${expected}ms, got ${observed}ms`
      );
      assert.ok(
        observed <= expected + delayToleranceMs,
        `expected delay step ${i + 1} <= ${expected + delayToleranceMs}ms, got ${observed}ms`
      );
    }
  } finally {
    globalThis.fetch = originalFetch;
    Math.random = originalMathRandom;
    clearOverloadedByHost();
  }
});

test('near-expiry host-overload cooldown should not be inflated to extra long sleep', async () => {
  const { createSlotHandlerClient, markHostOverloaded, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
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
      result: 'granted',
      queryToken: 'query-near-expiry-grant',
      invocationEpoch: 1,
      slotToken: 'slot-1',
    }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    markHostOverloaded(fqContext.hostname, 40);
    const startedAt = Date.now();
    const result = await client.waitForSlot({}, fqContext);
    const elapsedMs = Date.now() - startedAt;

    assert.equal(result.kind, 'granted');
    assert.equal(fetchCalls, 1);
    assert.ok(elapsedMs < 700, `expected near-expiry cooldown to stay sub-second, got ${elapsedMs}ms`);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('abort during host-overload cooldown should stop immediately without acquire call', async () => {
  const { createSlotHandlerClient, markHostOverloaded, clearOverloadedByHost } = __fairQueueTestHooks;
  clearOverloadedByHost();

  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      totalMaxWaitMs: 20000,
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
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
      result: 'granted',
      queryToken: 'query-release-retry-grant',
      invocationEpoch: 1,
      slotToken: 'slot-1',
    }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const controller = new AbortController();
    markHostOverloaded(fqContext.hostname, 1000);
    const pending = client.waitForSlot({}, fqContext, controller.signal);
    setTimeout(() => controller.abort(), 30);

    await assert.rejects(pending, (error) => error && error.name === 'AbortError');
    assert.equal(fetchCalls, 0);
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
      perRequestTimeoutMs: 100,
      maxAttemptsCap: 8,
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
      `expected dedicated 1500ms release timeout instead of acquire perRequestTimeoutMs or long-poll clamping, got ${firstAbortElapsedMs}ms`
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
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
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
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
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
