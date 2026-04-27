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
  fairQueueHostPatterns = [],
  fairQueueSiteBucket = undefined,
  trueConcurrencyHostPatterns = [],
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
      mode: 'custom-pg-rest',
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
      cacheEnabled: false,
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

const GOOGLE_DRIVE_HOST_PATTERNS = [
  'drive.google.com',
  '*.googleapis.com',
  '*.googleusercontent.com',
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
      });
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
      'origin-fetch',
      'breaker-report',
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
      'origin-fetch',
      'breaker-report',
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

test('breaker_only with true concurrency authorizes breaker before CQ acquire', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];

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
        VERSION: 11,
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
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(calls.slice(0, 4), [
      'breaker-snapshot',
      'breaker-authorize',
      'concurrency-acquire',
      'origin-fetch',
    ]);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('breaker_only with true concurrency settles breaker attempt when CQ deny happens after authorize', async () => {
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
    assert.deepEqual(calls, [
      'breaker-snapshot',
      'breaker-authorize',
      'concurrency-acquire',
      'breaker-settle',
    ]);
    assert.equal(settleBodies.length, 1);
    assert.equal(settleBodies[0].p_attempt_version, 22);
    assert.equal(settleBodies[0].p_attempt_ticket, 4);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('breaker_only with true concurrency settles granted attempt before auth refresh retries and releases CQ at terminal', async () => {
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

    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('concurrency-release');
      return createJsonResponse({ result: 'released' });
    }

    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createTestContext();
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildWorkerEnv(), ctx);
    assert.equal(response.status, 401);
    assert.equal(await response.text(), 'expired');
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(calls, [
      'link-api',
      'breaker-snapshot',
      'breaker-authorize',
      'concurrency-acquire',
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

test('breaker_only with true concurrency settles granted attempt on client-aborted CQ acquire', async () => {
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
    assert.deepEqual(calls, [
      'breaker-snapshot',
      'breaker-authorize',
      'concurrency-acquire',
      'breaker-settle',
    ]);
    assert.equal(settleBodies.length, 1);
    assert.equal(settleBodies[0].p_attempt_version, 52);
    assert.equal(settleBodies[0].p_attempt_ticket, 9);
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

test('Google Drive HEAD requests use GET range probe and expose resumable headers', async () => {
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
  const expectedSiteBucket = await hashSiteKey('host:drive.google.com');

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

test('breaker authority collapses recognized Google Drive hosts into the logical google bucket', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const googleAuthorityHash = await sha256Hash('google');
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
      });
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
      await buildSignedWorkerRequest({ pathname: '/downloads/google-breaker-drive.bin' }),
      buildWorkerEnv(),
      firstCtx.ctx,
    );
    waitUntilPromises.push(...firstCtx.waitUntilPromises);
    assert.equal(firstResponse.status, 429);

    const secondCtx = createTestContext();
    const secondResponse = await worker.fetch(
      await buildSignedWorkerRequest({ pathname: '/downloads/google-breaker-api.bin' }),
      buildWorkerEnv(),
      secondCtx.ctx,
    );
    waitUntilPromises.push(...secondCtx.waitUntilPromises);
    assert.equal(secondResponse.status, 429);

    await Promise.allSettled(waitUntilPromises);

    assert.deepEqual(snapshotHashes, [googleAuthorityHash, googleAuthorityHash]);
    assert.deepEqual(authorizeBodies.map((body) => body.p_hostname), ['google']);
    assert.deepEqual(
      reportBodies.map((body) => ({
        hostname: body.p_hostname,
        hostnameHash: body.p_hostname_hash,
        statusCode: body.p_status_code,
      })),
      [{
        hostname: 'google',
        hostnameHash: googleAuthorityHash,
        statusCode: 429,
      }],
    );
    assert.equal(concurrencyBodies.length, 1);
    assert.deepEqual(originHosts, ['drive.google.com']);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('recognized Google host overload suppresses other Google Drive host-family acquires', async () => {
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
    assert.equal(second.kind, 'timeout');
    assert.equal(fetchCalls, 1);
  } finally {
    globalThis.fetch = originalFetch;
    clearOverloadedByHost();
  }
});

test('final cleanup groups recognized Google Drive hosts into one logical Google bucket instead of unknown', () => {
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

  assert.equal(cleanupGroups.length, 2);
  assert.deepEqual(
    cleanupGroups.map((group) => group.map((context) => context.queryToken)),
    [
      ['google-drive-query', 'google-api-query', 'googleusercontent-query'],
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
