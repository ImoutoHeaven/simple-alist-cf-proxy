import { test } from 'node:test';
import assert from 'node:assert/strict';
import worker, { __fairQueueTestHooks } from '../src/worker.js';
import { encryptBindingPayload } from '../src/origin-binding.js';

const { createSlotHandlerClient } = __fairQueueTestHooks;

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

const buildSignedWorkerRequest = async (pathname = '/downloads/task3.bin') => {
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

test('slot-handler client sends abandon with queryToken and auth header', async () => {
  const client = createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.com',
      authKey: 'secret',
      authHeader: 'X-FQ-Auth',
    },
  });
  const fqContext = { queryToken: 'query-1' };
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
    assert.deepEqual(seenBody, { queryToken: 'query-1' });
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

test('finalizer abandons when queryToken exists without slotToken', async () => {
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

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      return createJsonResponse({
        result: 'timeout',
        queryToken: 'query-timeout',
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
    assert.deepEqual(abandonBodies, [{ queryToken: 'query-timeout' }]);
    assert.deepEqual(releaseBodies, []);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('client abort before grant uses abandon cleanup', async () => {
  const originalFetch = globalThis.fetch;
  const waitUntilPromises = [];
  const acquireBodies = [];
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

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      acquireBodies.push(JSON.parse(init.body));
      setTimeout(() => controller.abort(), 0);
      return createJsonResponse({
        result: 'pending',
        queryToken: 'query-abort',
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

    assert.equal(acquireBodies.length, 1);
    assert.equal(response.status, 499);
    assert.deepEqual(abandonBodies, [{ queryToken: 'query-abort' }]);
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
        cleanupCalls.push({ kind: 'abandon', token: contextToAbandon.queryToken });
        return true;
      },
    },
    fqContext: {
      hostname: 'a.sharepoint.com',
      hostnameHash: 'hash-a',
      ipBucket: 'ip-bucket',
      siteBucket: 'site-a',
      queryToken: 'query-pre-grant',
    },
    targetUrl: 'https://b.sharepoint.com/final',
    phase: 'redirect',
  });

  assert.deepEqual(cleanupCalls, [{ kind: 'abandon', token: 'query-pre-grant' }]);
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
      perRequestTimeoutMs: 8000,
      maxAttemptsCap: 8,
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

  globalThis.fetch = async () => new Response(JSON.stringify({
    result: 'granted',
    queryToken: 'query-granted',
    slotToken: 'slot-granted',
    meta: {
      attemptVersion: 7,
      attemptTicket: 11,
    },
  }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
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
  assert.equal(fqContext.attemptVersion, null);
  assert.equal(fqContext.attemptTicket, null);
  assert.equal(fqContext.deferredReportArmed, false);
  assert.equal(fqContext.deferredReportStatusCode, null);
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
