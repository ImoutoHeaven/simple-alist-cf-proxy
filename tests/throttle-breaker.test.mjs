import { test } from 'node:test';
import assert from 'node:assert/strict';
import { getBreakerState, reportBreakerSample } from '../src/cache/throttle-custom-pg-rest.js';
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
  },
});

const buildSignedWorkerRequest = async (pathname = '/downloads/test.bin') => {
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

test('resolveConfig returns the canonical breaker runtime config', () => {
  const config = resolveConfig({}, buildBootstrap(), { download: {} });

  assert.equal(config.throttleEnabled, true);
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
    assert.equal(seenPayload.halfOpenMaxProbeCount, 4);
    assert.equal(seenPayload.halfOpenMaxSeconds, 15);
    assert.equal(seenPayload.halfOpenTimeoutMode, 'partial-close');
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

test('worker blocks same-host refresh when a new authorize call returns reopened state', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const reportBodies = [];
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
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('worker blocks same-host refresh when a new authorize call finds a full half_open epoch', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const reportBodies = [];
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

test('scheduleAllCleanups leaves breaker state untouched', async () => {
  const originalFetch = globalThis.fetch;
  let fetchCalls = 0;

  globalThis.fetch = async () => {
    fetchCalls += 1;
    return createJsonResponse(0);
  };

  try {
    await scheduleAllCleanups({
      dbMode: 'custom-pg-rest',
      throttleEnabled: true,
      throttleConfig: {
        openCapSeconds: 60,
      },
    }, {}, null);

    assert.equal(fetchCalls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
