import { test } from 'node:test';
import assert from 'node:assert/strict';
import { __fairQueueTestHooks } from '../src/worker.js';

const { resolveConfig, createConcurrencyHandlerClient } = __fairQueueTestHooks;

const buildBootstrap = (trueConcurrency = undefined) => ({
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
    ...(trueConcurrency === undefined ? {} : { trueConcurrency }),
  },
});

test('resolveConfig exposes true concurrency config with deterministic defaults', () => {
  const config = resolveConfig({}, buildBootstrap({
    enabled: true,
    hostPatterns: ['*.sharepoint.com'],
    handlerUrl: 'https://cq.example.test/',
    handlerAuthKey: 'cq-secret',
  }), { download: {} });

  assert.equal(config.trueConcurrencyEnabled, true);
  assert.deepEqual(config.trueConcurrencyHostnamePatterns, ['*.sharepoint.com']);
  assert.deepEqual(config.trueConcurrencySiteBucket, { mode: 'sharepoint' });
  assert.deepEqual(config.concurrencyHandlerConfig, {
    url: 'https://cq.example.test/',
    authKey: 'cq-secret',
    authHeader: 'X-CQ-Auth',
    precheckTimeoutMs: 1200,
    acquireTimeoutMs: 2000,
    releaseTimeoutMs: 1500,
  });
});

test('resolveConfig requires true concurrency hostPatterns handlerUrl and handlerAuthKey when enabled', () => {
  assert.throws(
    () => resolveConfig({}, buildBootstrap({
      enabled: true,
      handlerUrl: 'https://cq.example.test',
      handlerAuthKey: 'cq-secret',
    }), { download: {} }),
    /hostPatterns/
  );

  assert.throws(
    () => resolveConfig({}, buildBootstrap({
      enabled: true,
      hostPatterns: ['*.sharepoint.com'],
      handlerAuthKey: 'cq-secret',
    }), { download: {} }),
    /handlerUrl/
  );

  assert.throws(
    () => resolveConfig({}, buildBootstrap({
      enabled: true,
      hostPatterns: ['*.sharepoint.com'],
      handlerUrl: 'https://cq.example.test',
    }), { download: {} }),
    /handlerAuthKey/
  );
});

test('resolveConfig rejects unsupported true concurrency site bucket modes', () => {
  assert.throws(
    () => resolveConfig({}, buildBootstrap({
      enabled: true,
      hostPatterns: ['*.sharepoint.com'],
      handlerUrl: 'https://cq.example.test',
      handlerAuthKey: 'cq-secret',
      siteBucket: { mode: 'host' },
    }), { download: {} }),
    /sharepoint/
  );
});

test('concurrency client sends precheck to the normalized endpoint with auth header and timeout signal', async () => {
  const calls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    calls.push({
      url,
      method: init.method,
      headers: init.headers,
      body: JSON.parse(init.body),
      hasSignal: init.signal instanceof AbortSignal,
    });
    return new Response(JSON.stringify({ result: 'allow' }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const client = createConcurrencyHandlerClient({
      concurrencyHandlerConfig: {
        url: 'https://cq.example.test/',
        authKey: 'cq-secret',
        authHeader: 'X-CQ-Auth',
        precheckTimeoutMs: 1200,
      },
    });

    const result = await client.precheck(null, {
      hostname: 'tenant.sharepoint.com',
      hostnameHash: 'host-hash',
      siteBucket: 'site-hash',
      ipBucket: 'ip-hash',
      nowMs: 101,
    });

    assert.deepEqual(result, { result: 'allow' });
    assert.equal(calls.length, 1);
    assert.equal(calls[0].url, 'https://cq.example.test/api/v1/concurrency/precheck');
    assert.equal(calls[0].method, 'POST');
    assert.equal(calls[0].headers['X-CQ-Auth'], 'cq-secret');
    assert.equal(calls[0].hasSignal, true);
    assert.deepEqual(calls[0].body, {
      hostname: 'tenant.sharepoint.com',
      hostnameHash: 'host-hash',
      siteBucket: 'site-hash',
      ipBucket: 'ip-hash',
      nowMs: 101,
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('concurrency client normalizes granted acquire responses', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    assert.equal(url, 'https://cq.example.test/api/v1/concurrency/acquire');
    assert.equal(init.headers['X-CQ-Auth'], 'cq-secret');
    assert.deepEqual(JSON.parse(init.body), {
      hostname: 'tenant.sharepoint.com',
      hostnameHash: 'host-hash',
      siteBucket: 'site-hash',
      ipBucket: 'ip-hash',
      requestId: 'req-1',
      hardExpireAtMs: 5000,
      nowMs: 111,
    });
    return new Response(JSON.stringify({
      result: 'granted',
      leaseId: 'lease-1',
      leaseToken: 'token-1',
      expiresAtMs: 4999,
    }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const client = createConcurrencyHandlerClient({
      concurrencyHandlerConfig: {
        url: 'https://cq.example.test',
        authKey: 'cq-secret',
        acquireTimeoutMs: 2000,
      },
    });

    const result = await client.acquire(null, {
      hostname: 'tenant.sharepoint.com',
      hostnameHash: 'host-hash',
      siteBucket: 'site-hash',
      ipBucket: 'ip-hash',
      requestId: 'req-1',
      hardExpireAtMs: 5000,
      nowMs: 111,
    });

    assert.deepEqual(result, {
      result: 'granted',
      leaseId: 'lease-1',
      leaseToken: 'token-1',
      expiresAtMs: 4999,
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('concurrency client sends required release payload and parses noop results', async () => {
  const originalFetch = globalThis.fetch;
  const originalNow = Date.now;
  Date.now = () => 999;
  globalThis.fetch = async (url, init = {}) => {
    assert.equal(url, 'https://cq.example.test/api/v1/concurrency/release');
    assert.equal(init.headers['X-CQ-Auth'], 'cq-secret');
    assert.deepEqual(JSON.parse(init.body), {
      leaseId: 'lease-1',
      leaseToken: 'token-1',
      reason: 'client_disconnect',
      nowMs: 999,
    });
    return new Response(JSON.stringify({ result: 'noop', reason: 'expired' }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  try {
    const client = createConcurrencyHandlerClient({
      concurrencyHandlerConfig: {
        url: 'https://cq.example.test/',
        authKey: 'cq-secret',
        releaseTimeoutMs: 1500,
      },
    });

    const result = await client.release(null, {
      leaseId: 'lease-1',
      leaseToken: 'token-1',
    }, 'client_disconnect');

    assert.deepEqual(result, { result: 'noop', reason: 'expired' });
  } finally {
    globalThis.fetch = originalFetch;
    Date.now = originalNow;
  }
});

test('concurrency client throws on malformed success payloads', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({ result: 'granted' }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });

  try {
    const client = createConcurrencyHandlerClient({
      concurrencyHandlerConfig: {
        url: 'https://cq.example.test',
        authKey: 'cq-secret',
        acquireTimeoutMs: 2000,
      },
    });

    await assert.rejects(
      () => client.acquire(null, {
        hostname: 'tenant.sharepoint.com',
        hostnameHash: 'host-hash',
        siteBucket: 'site-hash',
        ipBucket: 'ip-hash',
        requestId: 'req-1',
        hardExpireAtMs: 5000,
        nowMs: 111,
      }),
      /leaseId/
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('concurrency client rejects precheck success payloads with endpoint-invalid results', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({
    result: 'granted',
    leaseId: 'lease-1',
    leaseToken: 'token-1',
    expiresAtMs: 5000,
  }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });

  try {
    const client = createConcurrencyHandlerClient({
      concurrencyHandlerConfig: {
        url: 'https://cq.example.test',
        authKey: 'cq-secret',
        precheckTimeoutMs: 1200,
      },
    });

    await assert.rejects(
      () => client.precheck(null, {
        hostname: 'tenant.sharepoint.com',
        hostnameHash: 'host-hash',
        siteBucket: 'site-hash',
        ipBucket: 'ip-hash',
        nowMs: 101,
      }),
      /precheck/
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('concurrency client rejects acquire success payloads with endpoint-invalid results', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({ result: 'allow' }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });

  try {
    const client = createConcurrencyHandlerClient({
      concurrencyHandlerConfig: {
        url: 'https://cq.example.test',
        authKey: 'cq-secret',
        acquireTimeoutMs: 2000,
      },
    });

    await assert.rejects(
      () => client.acquire(null, {
        hostname: 'tenant.sharepoint.com',
        hostnameHash: 'host-hash',
        siteBucket: 'site-hash',
        ipBucket: 'ip-hash',
        requestId: 'req-1',
        hardExpireAtMs: 5000,
        nowMs: 111,
      }),
      /acquire/
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('concurrency client rejects acquire granted payloads beyond hard expiry', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({
    result: 'granted',
    leaseId: 'lease-1',
    leaseToken: 'token-1',
    expiresAtMs: 5001,
  }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });

  try {
    const client = createConcurrencyHandlerClient({
      concurrencyHandlerConfig: {
        url: 'https://cq.example.test',
        authKey: 'cq-secret',
        acquireTimeoutMs: 2000,
      },
    });

    await assert.rejects(
      () => client.acquire(null, {
        hostname: 'tenant.sharepoint.com',
        hostnameHash: 'host-hash',
        siteBucket: 'site-hash',
        ipBucket: 'ip-hash',
        requestId: 'req-1',
        hardExpireAtMs: 5000,
        nowMs: 111,
      }),
      /hardExpireAtMs/
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('concurrency client rejects release success payloads with endpoint-invalid results', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({
    result: 'deny',
    scope: 'host',
    reason: 'full',
    retryAfter: 3,
  }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });

  try {
    const client = createConcurrencyHandlerClient({
      concurrencyHandlerConfig: {
        url: 'https://cq.example.test',
        authKey: 'cq-secret',
        releaseTimeoutMs: 1500,
      },
    });

    await assert.rejects(
      () => client.release(null, {
        leaseId: 'lease-1',
        leaseToken: 'token-1',
      }, 'client_disconnect'),
      /release/
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('concurrency client rejects release noop payloads with unsupported reasons', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({ result: 'noop', reason: 'other' }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });

  try {
    const client = createConcurrencyHandlerClient({
      concurrencyHandlerConfig: {
        url: 'https://cq.example.test',
        authKey: 'cq-secret',
        releaseTimeoutMs: 1500,
      },
    });

    await assert.rejects(
      () => client.release(null, {
        leaseId: 'lease-1',
        leaseToken: 'token-1',
      }, 'client_disconnect'),
      /noop/
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('concurrency client rejects deny payloads with unsupported scope', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({
    result: 'deny',
    scope: 'global',
    reason: 'full',
    retryAfter: 3,
  }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });

  try {
    const client = createConcurrencyHandlerClient({
      concurrencyHandlerConfig: {
        url: 'https://cq.example.test',
        authKey: 'cq-secret',
        precheckTimeoutMs: 1200,
      },
    });

    await assert.rejects(
      () => client.precheck(null, {
        hostname: 'tenant.sharepoint.com',
        hostnameHash: 'host-hash',
        siteBucket: 'site-hash',
        ipBucket: 'ip-hash',
        nowMs: 101,
      }),
      /scope/
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('concurrency client rejects deny payloads with unsupported reason', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({
    result: 'deny',
    scope: 'host',
    reason: 'busy',
    retryAfter: 3,
  }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });

  try {
    const client = createConcurrencyHandlerClient({
      concurrencyHandlerConfig: {
        url: 'https://cq.example.test',
        authKey: 'cq-secret',
        acquireTimeoutMs: 2000,
      },
    });

    await assert.rejects(
      () => client.acquire(null, {
        hostname: 'tenant.sharepoint.com',
        hostnameHash: 'host-hash',
        siteBucket: 'site-hash',
        ipBucket: 'ip-hash',
        requestId: 'req-1',
        hardExpireAtMs: 5000,
        nowMs: 111,
      }),
      /reason/
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});
