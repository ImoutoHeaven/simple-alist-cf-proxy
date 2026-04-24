import { test } from 'node:test';
import assert from 'node:assert/strict';
import worker, { __fairQueueTestHooks } from '../src/worker.js';
import { encryptBindingPayload } from '../src/origin-binding.js';

const { createConcurrencyReleaseController } = __fairQueueTestHooks;

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

const buildRuntimeBootstrap = ({ fairQueueHostPatterns = [], trueConcurrencyHostPatterns = [] } = {}) => ({
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
    ...(trueConcurrencyHostPatterns.length > 0 ? {
      trueConcurrency: {
        enabled: true,
        hostPatterns: trueConcurrencyHostPatterns,
        handlerUrl: 'https://cq.example.test',
        handlerAuthKey: 'cq-secret',
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
  payloadExpireTime = null,
  payloadSignExpire = null,
  signal = undefined,
} = {}) => {
  const token = 'bootstrap-token';
  const expire = payloadSignExpire ?? (Math.floor(Date.now() / 1000) + expireOffsetSeconds);
  const encryptedBinding = await encryptBindingPayload({
    v: 2,
    issuer: 'https://landing.example.com',
    workerAddress: 'https://worker.example.com',
  }, token);
  const payload = encodeBase64Url(JSON.stringify({
    v: 1,
    expireTime: payloadExpireTime ?? expire,
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
    signal,
  });
};

const readJson = async (response) => JSON.parse(await response.text());

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

test('dual mode performs precheck before fairqueue and acquire before origin fetch', async () => {
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
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      return createJsonResponse({ result: 'ok' });
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
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(calls.slice(0, 4), [
      'precheck',
      'fairqueue-acquire',
      'concurrency-acquire',
      'origin-fetch',
    ]);
  } finally {
    globalThis.fetch = originalFetch;
    crypto.randomUUID = originalRandomUUID;
    delete globalThis.bootstrapCache;
  }
});

test('dual mode precheck deny returns 503 before fairqueue', async () => {
  const originalFetch = globalThis.fetch;
  let fairqueueCalls = 0;

  globalThis.fetch = async (input) => {
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
      return createJsonResponse({ result: 'deny', scope: 'host', reason: 'full', retryAfter: 7 });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      fairqueueCalls += 1;
      throw new Error('fairqueue should not be called after precheck deny');
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), createTestContext().ctx);
    const body = await readJson(response);
    assert.equal(response.status, 503);
    assert.equal(fairqueueCalls, 0);
    assert.match(body.message, /concurrency/i);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('dual mode malformed precheck deny fails open to fairqueue admission', async () => {
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
      return createJsonResponse({ result: 'deny', scope: 'host', reason: 'full', retryAfter: 0 });
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
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
      return createJsonResponse({ result: 'ok' });
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
      'precheck',
      'fairqueue-acquire',
      'concurrency-acquire',
      'origin-fetch',
    ]);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('dual mode concurrency acquire deny releases fairqueue and returns 503', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input) => {
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
      return createJsonResponse({ result: 'deny', scope: 'site', reason: 'full', retryAfter: 3 });
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
    assert.equal(response.status, 503);
    assert.deepEqual(calls, ['precheck', 'fairqueue-acquire', 'concurrency-acquire', 'fairqueue-release']);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('dual mode malformed concurrency acquire deny fails closed as handler failure', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input) => {
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
        queryToken: 'query-malformed-acquire',
        invocationEpoch: 1,
        slotToken: 'slot-malformed-acquire',
      });
    }

    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      calls.push('concurrency-acquire');
      return createJsonResponse({ result: 'deny', scope: 'site', reason: 'full', retryAfter: 0 });
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
    assert.equal(response.status, 503);
    assert.match(body.message, /true concurrency unavailable/i);
    assert.deepEqual(calls, ['precheck', 'fairqueue-acquire', 'concurrency-acquire', 'fairqueue-release']);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('true concurrency only skips precheck and fairqueue', async () => {
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
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
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
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(calls.slice(0, 2), ['concurrency-acquire', 'origin-fetch']);
    assert.equal(calls.includes('precheck'), false);
    assert.equal(calls.includes('fairqueue-acquire'), false);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('expired true concurrency link rejects before handler calls', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

  globalThis.fetch = async (input) => {
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

test('dual mode rejects link that expires during fairqueue admission before concurrency acquire', async () => {
  const originalFetch = globalThis.fetch;
  const originalDateNow = Date.now;
  const calls = [];
  const baseNowMs = 1766755200000;
  let nowMs = baseNowMs;

  Date.now = () => nowMs;
  globalThis.fetch = async (input) => {
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
      nowMs += 400;
      return createJsonResponse({ result: 'allow' });
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

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push('fairqueue-release');
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
    assert.equal(response.status, 401);
    assert.match(body.message, /link expired/i);
    assert.equal(calls.includes('concurrency-acquire'), false);
    assert.equal(calls.includes('origin-fetch'), false);
    assert.deepEqual(calls, ['precheck', 'fairqueue-acquire', 'fairqueue-release']);
  } finally {
    globalThis.fetch = originalFetch;
    Date.now = originalDateNow;
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
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      calls.push('origin-fetch');
      throw new Error('origin fetch failed before response delivery');
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
      });
    }

    if (url === 'https://tenant.sharepoint.com/file') {
      return new Response(null, {
        status: 204,
        headers: { 'content-type': 'application/octet-stream' },
      });
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

test('true concurrency managed streaming releases on client abort', async () => {
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

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const request = await buildSignedWorkerRequest({
      expireOffsetSeconds: 1,
      payloadExpireTime: Math.floor(Date.now() / 1000) + 1,
    });
    const response = await worker.fetch(request, buildWorkerEnv(), createTestContext().ctx);
    const reader = response.body.getReader();
    await new Promise((resolve) => setTimeout(resolve, 1300));
    await assert.rejects(() => reader.read(), /aborted/i);
    assert.deepEqual(calls, ['concurrency-acquire', 'concurrency-release']);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('redirect to new target releases old lease and reruns target admission with a new request id', async () => {
  const originalFetch = globalThis.fetch;
  const originalRandomUUID = crypto.randomUUID;
  const requestIds = [];
  const calls = [];

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
    assert.equal(await response.text(), 'ok');
    await Promise.allSettled(waitUntilPromises);
    assert.equal(requestIds.length, 2);
    assert.notEqual(requestIds[0], requestIds[1]);
    assert.equal(calls.includes('concurrency-release'), true);
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
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      fairQueueReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
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
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      fairQueueReleaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
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
      });
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      const body = JSON.parse(init.body);
      calls.push(`fairqueue-release:${body.slotToken}`);
      return createJsonResponse({ result: 'ok' });
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
