import { test } from 'node:test';
import assert from 'node:assert/strict';
import { getBreakerState, claimBreakerProbe, reportBreakerSample } from '../src/cache/throttle-custom-pg-rest.js';
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
      cacheEnabled: false,
      ...(options.rateLimit ? { rateLimit: options.rateLimit } : {}),
    },
    throttleProfiles: {
      default: {
        hostPatterns: options.hostPatterns || ['*.sharepoint.com'],
        openCapSeconds: 60,
        openThresholdPercent: 20,
        ewmaSpan: 8,
        consecutiveThreshold: 4,
        protectHttpCodes: [429, 499, 500, 502, 503, 504],
      },
    },
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
        openThresholdPercent: 20,
        ewmaSpan: 8,
        consecutiveThreshold: 4,
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
    openThresholdPercent: 20,
    ewmaSpan: 8,
    consecutiveThreshold: 4,
    protectHttpCodes: [429, 499, 500, 502, 503, 504],
  });
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

    if (url === 'https://postgrest.example.test/rpc/download_claim_breaker_probe') {
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
        PROBE_GRANTED: false,
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

test('worker skips breaker probe claim when authority snapshot is closed', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  let upstreamFetches = 0;
  let claimCalls = 0;
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

    if (url === 'https://postgrest.example.test/rpc/download_claim_breaker_probe') {
      claimCalls += 1;
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 2,
        LAST_ERROR_CODE: null,
        PROBE_GRANTED: false,
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
    assert.equal(claimCalls, 0);
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

    if (url === 'https://postgrest.example.test/rpc/download_claim_breaker_probe') {
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 1,
        LAST_ERROR_CODE: 429,
        PROBE_GRANTED: true,
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

test('slot-handler acquire payload carries only fair-queue context fields', async () => {
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
    return new Response(JSON.stringify({ result: 'granted', slotToken: 'slot-1' }), {
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
      probeLeaseUntil: null,
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
      probeLeaseUntil: null,
      lastErrorCode: 429,
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('getBreakerState exposes probe lease timing from raw breaker snapshots', async () => {
  const originalFetch = globalThis.fetch;
  const nowSeconds = Math.floor(Date.now() / 1000);

  globalThis.fetch = async () => createJsonResponse([{
    STATE: 'half_open',
    OPEN_UNTIL: null,
    OPEN_REASON: 'http_429',
    VERSION: 12,
    LAST_ERROR_CODE: 429,
    PROBE_LEASE_UNTIL: nowSeconds + 15,
  }]);

  try {
    const result = await getBreakerState('tenant.sharepoint.com', {
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
    });

    assert.equal(result.probeLeaseUntil, nowSeconds + 15);
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
      probeLeaseUntil: null,
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

test('claimBreakerProbe claims a breaker probe lease via RPC', async () => {
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
      VERSION: 10,
      LAST_ERROR_CODE: 429,
      PROBE_GRANTED: true,
    }]);
  };

  try {
    const result = await claimBreakerProbe('tenant.sharepoint.com', {
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
    });

    assert.equal(rpcUrl, 'https://postgrest.example.test/rpc/download_claim_breaker_probe');
    assert.equal(rpcBody.p_probe_lease_seconds, 15);
    assert.deepEqual(result, {
      recordExists: true,
      state: 'half_open',
      openUntil: null,
      reason: 'http_429',
      version: 10,
      lastErrorCode: 429,
      probeLeaseUntil: null,
      probeGranted: true,
    });
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
      retryAfterSeconds: 9,
    }, {
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
      openCapSeconds: 75,
      openThresholdPercent: 35,
      ewmaSpan: 11,
      consecutiveThreshold: 6,
      protectHttpCodes: [429, 503],
    });

    assert.equal(rpcUrl, 'https://postgrest.example.test/rpc/download_report_breaker_sample');
    assert.equal(rpcBody.p_sample, 1);
    assert.equal(rpcBody.p_status_code, 503);
    assert.equal(rpcBody.p_open_cap_seconds, 75);
    assert.equal(rpcBody.p_open_threshold_percent, 35);
    assert.equal(rpcBody.p_ewma_span, 11);
    assert.equal(rpcBody.p_consecutive_threshold, 6);
    assert.equal(rpcBody.p_probe_version, null);
    assert.equal(rpcBody.p_retry_after_seconds, 9);
    assert.deepEqual(Object.keys(rpcBody).sort(), [
      'p_consecutive_threshold',
      'p_ewma_span',
      'p_hostname',
      'p_hostname_hash',
      'p_now',
      'p_open_cap_seconds',
      'p_open_threshold_percent',
      'p_probe_version',
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

test('worker reports success samples for each managed redirect hop without claim when authority rows are absent', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const snapshotBodies = [];
  const claimBodies = [];
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

    if (url === 'https://postgrest.example.test/rpc/download_claim_breaker_probe') {
      claimBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
        PROBE_GRANTED: false,
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
    assert.equal(snapshotBodies.length, 3);
    assert.equal(claimBodies.length, 0);
    assert.deepEqual(
      reportBodies.map((body) => body.p_probe_version),
      [null, null],
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

test('worker forwards granted probe version into reportBreakerSample on matching hop', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const reportBodies = [];
  let snapshotReads = 0;
  let claimCalls = 0;
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

    if (url === 'https://postgrest.example.test/rpc/download_claim_breaker_probe') {
      claimCalls += 1;
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 13,
        LAST_ERROR_CODE: 429,
        PROBE_GRANTED: true,
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
    assert.equal(snapshotReads, 2);
    assert.equal(claimCalls, 1);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].p_hostname, 'tenant.sharepoint.com');
    assert.equal(reportBodies[0].p_probe_version, 13);
    assert.equal(reportBodies[0].p_sample, 0);
    assert.equal(reportBodies[0].p_status_code, 200);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('worker re-reads authority snapshot for each managed redirect host before claim gating', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const tenantHash = await decodeHostnameHash('tenant.sharepoint.com');
  const filesHash = await decodeHostnameHash('files.office.com');
  const snapshotHashes = [];
  const claimBodies = [];
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
      if (requestedHash === filesHash) {
        return createJsonResponse([]);
      }
      throw new Error(`Unexpected hostname hash in test: ${requestedHash}`);
    }

    if (url === 'https://postgrest.example.test/rpc/download_claim_breaker_probe') {
      claimBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 41,
        LAST_ERROR_CODE: null,
        PROBE_GRANTED: false,
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
    assert.deepEqual(snapshotHashes, [tenantHash, tenantHash, filesHash]);
    assert.equal(claimBodies.length, 0);
    assert.equal(reportBodies.length, 2);
    assert.deepEqual(
      reportBodies.map((body) => ({ hostname: body.p_hostname, probeVersion: body.p_probe_version })),
      [
        { hostname: 'tenant.sharepoint.com', probeVersion: null },
        { hostname: 'files.office.com', probeVersion: null },
      ],
    );
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('worker reports success after same-host 401 refresh without claim when authority rows are absent', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const snapshotBodies = [];
  const claimBodies = [];
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

    if (url === 'https://postgrest.example.test/rpc/download_claim_breaker_probe') {
      claimBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
        PROBE_GRANTED: false,
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
    assert.equal(snapshotBodies.length, 3);
    assert.equal(claimBodies.length, 0);
    assert.deepEqual(
      reportBodies.map((body) => body.p_probe_version),
      [null],
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

test('worker blocks same-host refresh when authority reopens after a claimed probe', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const snapshotStates = [];
  const reportBodies = [];
  let linkFetchCount = 0;
  let claimCalls = 0;
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
      const state = snapshotStates.length < 2
        ? {
            STATE: 'half_open',
            OPEN_UNTIL: null,
            OPEN_REASON: 'http_429',
            VERSION: 10,
            LAST_ERROR_CODE: 429,
          }
        : {
            STATE: 'open',
            OPEN_UNTIL: Math.floor(Date.now() / 1000) + 30,
            OPEN_REASON: 'http_429',
            VERSION: 11,
            LAST_ERROR_CODE: 429,
          };
      snapshotStates.push(state);
      return createJsonResponse([state]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_claim_breaker_probe') {
      claimCalls += 1;
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 10,
        LAST_ERROR_CODE: 429,
        PROBE_GRANTED: true,
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
    assert.equal(claimCalls, 1);
    assert.equal(initialUpstreamFetches, 1);
    assert.equal(refreshedUpstreamFetches, 0);
    assert.deepEqual(snapshotStates.map((state) => state.STATE), ['half_open', 'half_open', 'open']);
    assert.deepEqual(reportBodies, []);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('worker reclaims or blocks same-host refresh when authority moved to a newer half_open version', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const snapshotStates = [];
  const reportBodies = [];
  let linkFetchCount = 0;
  let claimCalls = 0;
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
      const state = snapshotStates.length < 2
        ? {
            STATE: 'half_open',
            OPEN_UNTIL: null,
            OPEN_REASON: 'http_429',
            VERSION: 10,
            LAST_ERROR_CODE: 429,
          }
        : {
            STATE: 'half_open',
            OPEN_UNTIL: null,
            OPEN_REASON: 'http_429',
            VERSION: 21,
            LAST_ERROR_CODE: 429,
          };
      snapshotStates.push(state);
      return createJsonResponse([state]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_claim_breaker_probe') {
      claimCalls += 1;
      if (claimCalls === 1) {
        return createJsonResponse([{
          STATE: 'half_open',
          OPEN_UNTIL: null,
          OPEN_REASON: 'http_429',
          VERSION: 10,
          LAST_ERROR_CODE: 429,
          PROBE_GRANTED: true,
        }]);
      }
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 21,
        LAST_ERROR_CODE: 429,
        PROBE_GRANTED: false,
        PROBE_LEASE_UNTIL: Math.floor(Date.now() / 1000) + 15,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 22,
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
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/same-host-refresh-newer-half-open.bin'), {
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
    assert.equal(claimCalls, 2);
    assert.equal(initialUpstreamFetches, 1);
    assert.equal(refreshedUpstreamFetches, 0);
    assert.deepEqual(snapshotStates.map((state) => state.VERSION), [10, 10, 21]);
    assert.deepEqual(reportBodies, []);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('worker reclaims same-host refresh when the matching half_open lease expired', async () => {
  const originalFetch = globalThis.fetch;
  const originalDateNow = Date.now;
  const waitUntilPromises = [];
  const snapshotStates = [];
  let linkFetchCount = 0;
  let claimCalls = 0;
  let initialUpstreamFetches = 0;
  let refreshedUpstreamFetches = 0;
  const nowSequence = [1_700_000_000_000, 1_700_000_000_000, 1_700_000_020_000, 1_700_000_020_000];
  delete globalThis.bootstrapCache;

  Date.now = () => nowSequence.length > 1 ? nowSequence.shift() : nowSequence[0];

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
      const state = snapshotStates.length < 2
        ? {
            STATE: 'half_open',
            OPEN_UNTIL: null,
            OPEN_REASON: 'http_429',
            VERSION: 10,
            LAST_ERROR_CODE: 429,
            PROBE_LEASE_UNTIL: 1_700_000_015,
          }
        : {
            STATE: 'half_open',
            OPEN_UNTIL: null,
            OPEN_REASON: 'http_429',
            VERSION: 10,
            LAST_ERROR_CODE: 429,
            PROBE_LEASE_UNTIL: 1_700_000_010,
          };
      snapshotStates.push(state);
      return createJsonResponse([state]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_claim_breaker_probe') {
      claimCalls += 1;
      if (claimCalls === 1) {
        return createJsonResponse([{
          STATE: 'half_open',
          OPEN_UNTIL: null,
          OPEN_REASON: 'http_429',
          VERSION: 10,
          LAST_ERROR_CODE: 429,
          PROBE_GRANTED: true,
          PROBE_LEASE_UNTIL: 1_700_000_015,
        }]);
      }
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 11,
        LAST_ERROR_CODE: 429,
        PROBE_GRANTED: false,
        PROBE_LEASE_UNTIL: 1_700_000_035,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
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
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/same-host-refresh-expired-lease.bin'), {
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
    assert.equal(claimCalls, 2);
    assert.equal(initialUpstreamFetches, 1);
    assert.equal(refreshedUpstreamFetches, 0);
    assert.deepEqual(snapshotStates.map((state) => state.PROBE_LEASE_UNTIL), [1_700_000_015, 1_700_000_015, 1_700_000_010]);
  } finally {
    globalThis.fetch = originalFetch;
    Date.now = originalDateNow;
    delete globalThis.bootstrapCache;
  }
});

test('worker re-reads same-host authority at claim time after redirect delay', async () => {
  const originalFetch = globalThis.fetch;
  const originalDateNow = Date.now;
  const waitUntilPromises = [];
  const nowSequence = [1_700_000_000_000, 1_700_000_000_000, 1_700_000_005_000, 1_700_000_005_000];
  const snapshotStates = [];
  const claimBodies = [];
  delete globalThis.bootstrapCache;

  Date.now = () => nowSequence.length > 1 ? nowSequence.shift() : nowSequence[0];

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
      const state = snapshotStates.length === 0
        ? {
            STATE: 'open',
            OPEN_UNTIL: 1_700_000_001,
            OPEN_REASON: 'http_429',
            VERSION: 7,
            LAST_ERROR_CODE: 429,
          }
        : {
            STATE: 'open',
            OPEN_UNTIL: 1_700_000_000,
            OPEN_REASON: 'http_429',
            VERSION: 8,
            LAST_ERROR_CODE: 429,
          };
      snapshotStates.push(state);
      return createJsonResponse([state]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_claim_breaker_probe') {
      claimBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'half_open',
        OPEN_UNTIL: null,
        OPEN_REASON: 'http_429',
        VERSION: 9,
        LAST_ERROR_CODE: 429,
        PROBE_GRANTED: true,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 10,
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
    const response = await worker.fetch(await buildSignedWorkerRequest('/downloads/same-host-delay.bin'), {
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
    assert.equal(snapshotStates.length, 3);
    assert.equal(claimBodies.length, 2);
  } finally {
    globalThis.fetch = originalFetch;
    Date.now = originalDateNow;
    delete globalThis.bootstrapCache;
  }
});

test('worker ignores unified-check open breaker rows for unmanaged hosts', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  let upstreamFetches = 0;
  let claimCalls = 0;
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

    if (url === 'https://postgrest.example.test/rpc/download_claim_breaker_probe') {
      claimCalls += 1;
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
    assert.equal(claimCalls, 0);
    assert.equal(reportCalls, 0);
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
