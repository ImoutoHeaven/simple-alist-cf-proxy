import { test } from 'node:test';
import assert from 'node:assert/strict';
import worker from '../src/worker.js';
import { encryptBindingPayload } from '../src/origin-binding.js';
import { sha256Hash } from '../src/utils.js';
import { createCacheRpcFixture } from './cache-rpc-fixture.mjs';

const encodeBase64Url = (value) => Buffer.from(value)
  .toString('base64')
  .replace(/\+/g, '-')
  .replace(/\//g, '_')
  .replace(/=+$/g, '');

const signPayload = async (payload, expire, token) => {
  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(token),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  );
  const signature = await crypto.subtle.sign(
    { name: 'HMAC', hash: 'SHA-256' },
    key,
    new TextEncoder().encode(`${payload}:${expire}`),
  );
  return `${btoa(String.fromCharCode(...new Uint8Array(signature))).replace(/\+/g, '-').replace(/\//g, '_')}:${expire}`;
};

const buildBootstrap = () => ({
  configVersion: 'cache-coordination-worker',
  global: { defaultProfileId: 'default' },
  pathProfiles: [{ id: 'default', dynamic: false, actions: { checkOriginMode: '' } }],
  common: {
    tokenHmacKey: 'bootstrap-token',
    workerAddresses: ['https://worker.example.com'],
    landingWorkerAddresses: ['https://landing.example.com'],
    binding: { defaultModes: '', version: 1 },
  },
  download: {
    address: 'https://alist.example.com',
    db: {
      mode: 'custom-pg-rest',
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
      cacheEnabled: true,
      cleanupPercentage: 0,
    },
    throttleProfiles: { default: { hostPatterns: [] } },
  },
});

const buildRequest = async (pathname, {
  method = 'GET',
  filesize = undefined,
  expireOffsetSeconds = 300,
  idleTimeoutSeconds = 300,
} = {}) => {
  const expire = Math.floor(Date.now() / 1000) + expireOffsetSeconds;
  const encrypted = await encryptBindingPayload({
    v: 2,
    issuer: 'https://landing.example.com',
    workerAddress: 'https://worker.example.com',
  }, 'bootstrap-token');
  const payloadData = {
    v: 1,
    expireTime: expire,
    idle_timeout: idleTimeoutSeconds,
    ticketNonce: 'abcdefghijklmnopqrstuvwxyz012345',
    encrypt: encrypted,
  };
  if (filesize !== undefined) {
    payloadData.filesize = filesize;
  }
  const payload = encodeBase64Url(JSON.stringify(payloadData));
  const payloadSign = await signPayload(payload, expire, 'bootstrap-token');
  const url = new URL(pathname, 'https://worker.example.com');
  url.searchParams.set('payload', payload);
  url.searchParams.set('payloadSign', payloadSign);
  return new Request(url, {
    method,
    headers: {
      origin: 'https://landing.example.com',
      'CF-Connecting-IP': '192.0.2.10',
    },
  });
};

const createContext = () => ({ waitUntil() {} });

const createStalledRpcResponse = (signal) => {
  let rejectBody;
  const bodyPromise = new Promise((resolve, reject) => {
    rejectBody = reject;
  });
  const abort = () => rejectBody(signal?.reason || new DOMException('The operation was aborted', 'AbortError'));
  if (signal && typeof signal.addEventListener === 'function') {
    if (signal.aborted) {
      abort();
    } else {
      signal.addEventListener('abort', abort, { once: true });
    }
  }
  return {
    ok: true,
    status: 200,
    json: () => bodyPromise,
  };
};

test('worker coordinates a cold link and reuses the acknowledged cache on the next request', async () => {
  const cache = createCacheRpcFixture();
  const bootstrap = buildBootstrap();
  const upstreamUrl = 'https://download.example.test/file.bin';
  let alistCalls = 0;
  let upstreamCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return new Response(JSON.stringify(bootstrap), { headers: { 'content-type': 'application/json' } });
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url === 'https://postgrest.example.test/rpc/download_get_ticket_state') {
      const body = JSON.parse(init.body);
      const now = Math.floor(Date.now() / 1000);
      return new Response(JSON.stringify([{
        found: true,
        ticket_hash: body.p_ticket_hash,
        issued_at: now,
        first_used_at: null,
        hard_expire_at: now + 300,
        idle_timeout_seconds: 300,
        idle_policy: 'first_use',
        idle_lease_expires_at: now + 300,
      }]), { headers: { 'content-type': 'application/json' } });
    }
    if (url === 'https://postgrest.example.test/rpc/download_mark_ticket_used') {
      return new Response(JSON.stringify({ result: 'transitioned' }), { headers: { 'content-type': 'application/json' } });
    }
    if (url === 'https://alist.example.com/api/fs/link') {
      assert.equal(JSON.parse(init.body).action, 'acquire');
      alistCalls += 1;
      return new Response(JSON.stringify({
        code: 200,
        data: {
          url: upstreamUrl,
          header: {},
          size: 5,
          download: {
            provider: 'generic',
            ticket: 'test-ticket-abcdefghijklmnopqrstuvwxyz',
            expires_at: Math.floor(Date.now() / 1000) + 300,
            report_success: false,
          },
        },
      }), { headers: { 'content-type': 'application/json' } });
    }
    if (url === upstreamUrl) {
      upstreamCalls += 1;
      return new Response('hello', { status: 200, headers: { 'content-length': '5' } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const first = await worker.fetch(await buildRequest('/downloads/cache-worker.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, createContext());
    assert.equal(first.status, 200);
    assert.equal(await first.text(), 'hello');

    const second = await worker.fetch(await buildRequest('/downloads/cache-worker.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-1',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, createContext());
    assert.equal(second.status, 200);
    assert.equal(await second.text(), 'hello');
    assert.equal(alistCalls, 1);
    assert.equal(upstreamCalls, 2);
    assert.equal(cache.calls.filter(({ url }) => url.endsWith('download_acquire_cache_refresh')).length, 1);
    assert.equal(cache.calls.filter(({ url }) => url.endsWith('download_finish_cache_refresh')).length, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('direct ready cache survives a Worker clock ahead of the database', { concurrency: false }, async () => {
  const databaseNowSeconds = Math.floor(Date.now() / 1000);
  const cache = createCacheRpcFixture({
    includeTicketState: true,
    databaseNowSeconds,
    ticketIdleTimeoutSeconds: 3600,
  });
  const bootstrap = buildBootstrap();
  const linkData = {
    url: 'https://download.example.test/clock-ahead.bin',
    header: {},
    download: {
      provider: 'generic',
      ticket: 'test-ticket-clock-ahead-abcdefghijklmnopqrstuvwxyz',
      expires_at: databaseNowSeconds + 120,
      report_success: false,
    },
  };
  let alistCalls = 0;
  const originalFetch = globalThis.fetch;
  const originalNow = Date.now;
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return new Response(JSON.stringify(bootstrap), { headers: { 'content-type': 'application/json' } });
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url === 'https://alist.example.com/api/fs/link') {
      alistCalls += 1;
      return new Response(JSON.stringify({ code: 200, data: linkData }), {
        headers: { 'content-type': 'application/json' },
      });
    }
    if (url === linkData.url) {
      return new Response('clock-ahead-content', { status: 200, headers: { 'content-length': '19' } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  const env = {
    CONTROLLER_URL: 'https://controller.example.test',
    CONTROLLER_API_TOKEN: 'controller-token',
    ENV: 'test',
    ROLE: 'download',
    INSTANCE_ID: 'worker-1',
    BOOTSTRAP_CACHE_MODE: 'direct',
  };
  try {
    const firstRequest = await buildRequest('/downloads/clock-ahead.bin', { expireOffsetSeconds: 3600, idleTimeoutSeconds: 3600 });
    const secondRequest = await buildRequest('/downloads/clock-ahead.bin', { expireOffsetSeconds: 3600, idleTimeoutSeconds: 3600 });
    const first = await worker.fetch(firstRequest, env, createContext());
    assert.equal(first.status, 200);
    assert.equal(await first.text(), 'clock-ahead-content');

    Date.now = () => originalNow() + 360_000;
    const second = await worker.fetch(secondRequest, env, createContext());
    assert.equal(second.status, 200, await second.clone().text());
    assert.equal(await second.text(), 'clock-ahead-content');
    assert.equal(alistCalls, 1);
    assert.equal(cache.calls.filter(({ url }) => url.endsWith('download_acquire_cache_refresh')).length, 1);
    assert.equal(cache.calls.filter(({ url }) => url.endsWith('download_finish_cache_refresh')).length, 1);
  } finally {
    Date.now = originalNow;
    globalThis.fetch = originalFetch;
  }
});

test('cache refresh owner accepts a database-valid envelope when the Worker clock is ahead', { concurrency: false }, async () => {
  delete globalThis.bootstrapCache;
  const databaseNowSeconds = Math.floor(Date.now() / 1000);
  const cache = createCacheRpcFixture({ includeTicketState: true, databaseNowSeconds, ticketIdleTimeoutSeconds: 3600 });
  const bootstrap = buildBootstrap();
  const linkData = {
    url: 'https://download.example.test/owner-clock-ahead.bin',
    header: {},
    download: {
      provider: 'generic',
      ticket: 'test-ticket-owner-clock-ahead-abcdefghijklmnopqrstuvwxyz',
      expires_at: databaseNowSeconds + 120,
      report_success: false,
    },
  };
  let alistCalls = 0;
  const originalFetch = globalThis.fetch;
  const originalNow = Date.now;
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return new Response(JSON.stringify(bootstrap), { headers: { 'content-type': 'application/json' } });
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url === 'https://alist.example.com/api/fs/link') {
      alistCalls += 1;
      return new Response(JSON.stringify({ code: 200, data: linkData }), {
        headers: { 'content-type': 'application/json' },
      });
    }
    if (url === linkData.url) {
      return new Response('owner-clock-ahead-content', { status: 200, headers: { 'content-length': '25' } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  const env = {
    CONTROLLER_URL: 'https://controller.example.test',
    CONTROLLER_API_TOKEN: 'controller-token',
    ENV: 'test',
    ROLE: 'download',
    INSTANCE_ID: 'worker-1',
    BOOTSTRAP_CACHE_MODE: 'direct',
  };
  try {
    const request = await buildRequest('/downloads/owner-clock-ahead.bin', {
      expireOffsetSeconds: 3600,
      idleTimeoutSeconds: 3600,
    });
    Date.now = () => originalNow() + 360_000;
    const response = await worker.fetch(request, env, createContext());
    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'owner-clock-ahead-content');
    assert.equal(alistCalls, 1);
    const finishCalls = cache.calls.filter(({ url }) => url.endsWith('download_finish_cache_refresh'));
    assert.equal(finishCalls.length, 1);
    assert.ok(finishCalls[0].body.p_link_data, 'owner should publish the database-valid envelope');
  } finally {
    Date.now = originalNow;
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('unified ready cache survives a Worker clock behind the database', { concurrency: false }, async () => {
  delete globalThis.bootstrapCache;
  const databaseNowSeconds = Math.floor(Date.now() / 1000);
  const cache = createCacheRpcFixture({ includeTicketState: true, databaseNowSeconds });
  const bootstrap = buildBootstrap();
  bootstrap.download.db.rateLimit = {
    enabled: true,
    windowSeconds: 60,
    limit: 100,
    cleanupPercentage: 0,
  };
  const linkData = {
    url: 'https://download.example.test/unified-clock-behind.bin',
    header: {},
    download: {
      provider: 'generic',
      ticket: 'test-ticket-unified-clock-behind-abcdefghijklmnopqrstuvwxyz',
      expires_at: databaseNowSeconds + 120,
      report_success: false,
    },
  };
  const version = '11111111-1111-4111-8111-000000000001';
  let unifiedCalls = 0;
  let alistCalls = 0;
  const originalFetch = globalThis.fetch;
  const originalNow = Date.now;
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return new Response(JSON.stringify(bootstrap), { headers: { 'content-type': 'application/json' } });
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url === 'https://postgrest.example.test/rpc/download_unified_check') {
      unifiedCalls += 1;
      const ready = unifiedCalls > 1;
      return new Response(JSON.stringify([{
        cache_link_data: ready ? JSON.stringify(linkData) : null,
        cache_timestamp: ready ? databaseNowSeconds : null,
        cache_hostname_hash: null,
        cache_version: ready ? version : null,
        cache_observed_at: new Date(databaseNowSeconds * 1000).toISOString(),
        rate_access_count: 0,
        rate_last_window_time: databaseNowSeconds,
        rate_block_until: null,
        throttle_record_exists: false,
        throttle_state: null,
        throttle_open_until: null,
        throttle_reason: null,
        throttle_version: null,
        throttle_last_error_code: null,
      }]), { headers: { 'content-type': 'application/json' } });
    }
    if (url === 'https://alist.example.com/api/fs/link') {
      alistCalls += 1;
      return new Response(JSON.stringify({ code: 200, data: linkData }), {
        headers: { 'content-type': 'application/json' },
      });
    }
    if (url === linkData.url) {
      return new Response('unified-clock-behind-content', { status: 200, headers: { 'content-length': '28' } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  const env = {
    CONTROLLER_URL: 'https://controller.example.test',
    CONTROLLER_API_TOKEN: 'controller-token',
    ENV: 'test',
    ROLE: 'download',
    INSTANCE_ID: 'worker-1',
    BOOTSTRAP_CACHE_MODE: 'direct',
  };
  try {
    const firstRequest = await buildRequest('/downloads/unified-clock-behind.bin', { expireOffsetSeconds: 3600 });
    const secondRequest = await buildRequest('/downloads/unified-clock-behind.bin', { expireOffsetSeconds: 3600 });
    const first = await worker.fetch(firstRequest, env, createContext());
    assert.equal(first.status, 200);
    assert.equal(await first.text(), 'unified-clock-behind-content');

    Date.now = () => originalNow() - 360_000;
    const second = await worker.fetch(secondRequest, env, createContext());
    assert.equal(second.status, 200);
    assert.equal(await second.text(), 'unified-clock-behind-content');
    assert.equal(unifiedCalls, 2);
    assert.equal(alistCalls, 1);
    assert.equal(cache.calls.filter(({ url }) => url.endsWith('download_get_cache_state')).length, 0);
    assert.equal(cache.calls.filter(({ url }) => url.endsWith('download_acquire_cache_refresh')).length, 1);
    assert.equal(cache.calls.filter(({ url }) => url.endsWith('download_finish_cache_refresh')).length, 1);
  } finally {
    Date.now = originalNow;
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('direct and unified ready cache paths reject malformed state consistently', { concurrency: false }, async () => {
  const baseLink = {
    url: 'https://download.example.test/malformed-ready.bin',
    header: null,
    download: {
      provider: 'generic',
      ticket: 'test-ticket-malformed-ready-abcdefghijklmnopqrstuvwxyz',
      expires_at: Math.floor(Date.now() / 1000) + 600,
      report_success: false,
    },
  };
  const cases = [
    { label: 'missing-url', mutate: (link) => { delete link.url; } },
    { label: 'empty-url', mutate: (link) => { link.url = ''; } },
    { label: 'missing-provider', mutate: (link) => { delete link.download.provider; } },
    { label: 'missing-ticket', mutate: (link) => { delete link.download.ticket; } },
    { label: 'missing-report-flag', mutate: (link) => { delete link.download.report_success; } },
    { label: 'non-http-url', mutate: (link) => { link.url = 'ftp://download.example.test/file'; } },
    { label: 'missing-version', omitVersion: true },
    { label: 'missing-observation', omitObservedAt: true },
    { label: 'malformed-serialized-link', serialized: '{malformed-link-json' },
  ];

  for (const mode of ['direct', 'unified']) {
    for (const testCase of cases) {
      delete globalThis.bootstrapCache;
      const databaseNowSeconds = Math.floor(Date.now() / 1000);
      const cache = createCacheRpcFixture({ includeTicketState: true, databaseNowSeconds });
      const bootstrap = buildBootstrap();
      if (mode === 'unified') {
        bootstrap.download.db.rateLimit = {
          enabled: true,
          windowSeconds: 60,
          limit: 100,
          cleanupPercentage: 0,
        };
      }
      const path = `/downloads/malformed-${mode}-${testCase.label}.bin`;
      const pathHash = await sha256Hash(path);
      const malformedLink = JSON.parse(JSON.stringify(baseLink));
      testCase.mutate?.(malformedLink);
      const serializedLink = testCase.serialized || JSON.stringify(malformedLink);
      const originalFetch = globalThis.fetch;
      let alistCalls = 0;
      globalThis.fetch = async (input, init = {}) => {
        const url = typeof input === 'string' ? input : input.url;
        if (url === 'https://controller.example.test/api/v0/bootstrap') {
          return new Response(JSON.stringify(bootstrap), { headers: { 'content-type': 'application/json' } });
        }
        if (mode === 'direct' && url.endsWith('download_get_cache_state')) {
          return new Response(JSON.stringify([{
            result: 'ready',
            path_hash: pathHash,
            path,
            link_data: serializedLink,
            cache_timestamp: databaseNowSeconds,
            hostname_hash: null,
            version: testCase.omitVersion ? null : '11111111-1111-4111-8111-000000000020',
            lease_id: null,
            lease_until: null,
            invalid_version: null,
            retry_after: null,
            last_error_code: null,
            observed_at: testCase.omitObservedAt ? null : new Date(databaseNowSeconds * 1000).toISOString(),
            updated_at: new Date(databaseNowSeconds * 1000).toISOString(),
          }]), { headers: { 'content-type': 'application/json' } });
        }
        if (mode === 'unified' && url.endsWith('download_unified_check')) {
          return new Response(JSON.stringify([{
            cache_link_data: serializedLink,
            cache_timestamp: databaseNowSeconds,
            cache_hostname_hash: null,
            cache_version: testCase.omitVersion ? null : '11111111-1111-4111-8111-000000000020',
            cache_observed_at: testCase.omitObservedAt ? null : new Date(databaseNowSeconds * 1000).toISOString(),
            rate_access_count: 0,
            rate_last_window_time: databaseNowSeconds,
            rate_block_until: null,
            throttle_record_exists: false,
            throttle_state: null,
            throttle_open_until: null,
            throttle_reason: null,
            throttle_version: null,
            throttle_last_error_code: null,
          }]), { headers: { 'content-type': 'application/json' } });
        }
        const cacheResponse = await cache.handle(input, init);
        if (cacheResponse) {
          return cacheResponse;
        }
        if (url === 'https://alist.example.com/api/fs/link') {
          alistCalls += 1;
          throw new Error(`unexpected OpenList call for ${mode}/${testCase.label}`);
        }
        throw new Error(`unexpected fetch URL for ${mode}/${testCase.label}: ${url}`);
      };

      const env = {
        CONTROLLER_URL: 'https://controller.example.test',
        CONTROLLER_API_TOKEN: 'controller-token',
        ENV: 'test',
        ROLE: 'download',
        INSTANCE_ID: 'worker-1',
        BOOTSTRAP_CACHE_MODE: 'direct',
      };
      try {
        const response = await worker.fetch(await buildRequest(path, { expireOffsetSeconds: 3600, idleTimeoutSeconds: 3600 }), env, createContext());
        assert.equal(response.status, 503, `${mode}/${testCase.label}`);
        assert.equal((await response.json()).reason, 'cache_coordinator_unavailable', `${mode}/${testCase.label}`);
        assert.equal(alistCalls, 0, `${mode}/${testCase.label} must stop before OpenList`);
      } finally {
        globalThis.fetch = originalFetch;
        delete globalThis.bootstrapCache;
      }
    }
  }
});

test('unified malformed ready state stays coordinator unavailable with fail-open rate handling', { concurrency: false }, async () => {
  delete globalThis.bootstrapCache;
  const databaseNowSeconds = Math.floor(Date.now() / 1000);
  const cache = createCacheRpcFixture({ includeTicketState: true, databaseNowSeconds });
  const bootstrap = buildBootstrap();
  bootstrap.download.db.rateLimit = {
    enabled: true,
    windowSeconds: 60,
    limit: 100,
    cleanupPercentage: 0,
    pgErrorHandle: 'fail-open',
  };
  const path = '/downloads/malformed-unified-fail-open.bin';
  const originalFetch = globalThis.fetch;
  let alistCalls = 0;
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return new Response(JSON.stringify(bootstrap), { headers: { 'content-type': 'application/json' } });
    }
    if (url.endsWith('download_unified_check')) {
      return new Response(JSON.stringify([{
        cache_link_data: JSON.stringify({
          url: 'https://download.example.test/malformed-fail-open.bin',
          header: null,
          download: {
            provider: 'generic',
            expires_at: databaseNowSeconds + 600,
            report_success: false,
          },
        }),
        cache_timestamp: databaseNowSeconds,
        cache_hostname_hash: null,
        cache_version: '11111111-1111-4111-8111-000000000021',
        cache_observed_at: new Date(databaseNowSeconds * 1000).toISOString(),
        rate_access_count: 0,
        rate_last_window_time: databaseNowSeconds,
        rate_block_until: null,
        throttle_record_exists: false,
        throttle_state: null,
        throttle_open_until: null,
        throttle_reason: null,
        throttle_version: null,
        throttle_last_error_code: null,
      }]), { headers: { 'content-type': 'application/json' } });
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url === 'https://alist.example.com/api/fs/link') {
      alistCalls += 1;
    }
    throw new Error(`unexpected fetch URL in fail-open parity test: ${url}`);
  };

  const env = {
    CONTROLLER_URL: 'https://controller.example.test',
    CONTROLLER_API_TOKEN: 'controller-token',
    ENV: 'test',
    ROLE: 'download',
    INSTANCE_ID: 'worker-1',
    BOOTSTRAP_CACHE_MODE: 'direct',
  };
  try {
    const response = await worker.fetch(await buildRequest(path, { expireOffsetSeconds: 3600, idleTimeoutSeconds: 3600 }), env, createContext());
    assert.equal(response.status, 503);
    assert.equal((await response.json()).reason, 'cache_coordinator_unavailable');
    assert.equal(alistCalls, 0);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('database backoff duration stays stable when the Worker clock is ahead', { concurrency: false }, async () => {
  const databaseNowSeconds = Math.floor(Date.now() / 1000);
  const cache = createCacheRpcFixture({
    includeTicketState: true,
    databaseNowSeconds,
    ticketIdleTimeoutSeconds: 3600,
  });
  const bootstrap = buildBootstrap();
  const linkData = {
    url: 'https://download.example.test/backoff-clock.bin',
    header: {},
    download: {
      provider: 'generic',
      ticket: 'test-ticket-backoff-clock-abcdefghijklmnopqrstuvwxyz',
      expires_at: databaseNowSeconds + 600,
      report_success: false,
    },
  };
  let alistCalls = 0;
  const originalFetch = globalThis.fetch;
  const originalNow = Date.now;
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return new Response(JSON.stringify(bootstrap), { headers: { 'content-type': 'application/json' } });
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url === 'https://alist.example.com/api/fs/link') {
      alistCalls += 1;
      return new Response(JSON.stringify({ code: 200, data: linkData }), {
        headers: { 'content-type': 'application/json' },
      });
    }
    if (url === linkData.url) {
      return new Response('upstream-failure', { status: 500 });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  const env = {
    CONTROLLER_URL: 'https://controller.example.test',
    CONTROLLER_API_TOKEN: 'controller-token',
    ENV: 'test',
    ROLE: 'download',
    INSTANCE_ID: 'worker-1',
    BOOTSTRAP_CACHE_MODE: 'direct',
  };
  try {
    const firstRequest = await buildRequest('/downloads/backoff-clock.bin', { expireOffsetSeconds: 3600, idleTimeoutSeconds: 3600 });
    const secondRequest = await buildRequest('/downloads/backoff-clock.bin', { expireOffsetSeconds: 3600, idleTimeoutSeconds: 3600 });
    const first = await worker.fetch(firstRequest, env, createContext());
    assert.equal(first.status, 503);

    Date.now = () => originalNow() + 360_000;
    const second = await worker.fetch(secondRequest, env, createContext());
    assert.equal(second.status, 503);
    assert.equal((await second.json()).reason, 'cache_backoff');
    assert.equal(second.headers.get('Retry-After'), '30');
    assert.equal(alistCalls, 2);
    assert.equal(cache.calls.filter(({ url }) => url.endsWith('download_acquire_cache_refresh')).length, 1);
  } finally {
    Date.now = originalNow;
    globalThis.fetch = originalFetch;
  }
});

test('a follower polling deadline returns coordinator 503 without acquiring a second link', { concurrency: false }, async () => {
  delete globalThis.bootstrapCache;
  const databaseNowSeconds = Math.floor(Date.now() / 1000);
  const cache = createCacheRpcFixture({ includeTicketState: true, databaseNowSeconds });
  const bootstrap = buildBootstrap();
  const path = '/downloads/follower-deadline.bin';
  const pathHash = await sha256Hash(path);
  cache.seedLease(pathHash, '11111111-1111-4111-8111-000000000009', 1);
  const originalFetch = globalThis.fetch;
  const originalNow = Date.now;
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return new Response(JSON.stringify(bootstrap), { headers: { 'content-type': 'application/json' } });
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    throw new Error(`OpenList or upstream must not be called: ${url}`);
  };

  const env = {
    CONTROLLER_URL: 'https://controller.example.test',
    CONTROLLER_API_TOKEN: 'controller-token',
    ENV: 'test',
    ROLE: 'download',
    INSTANCE_ID: 'worker-1',
    BOOTSTRAP_CACHE_MODE: 'direct',
  };
  try {
    const request = await buildRequest(path, { expireOffsetSeconds: 3600, idleTimeoutSeconds: 3600 });
    Date.now = () => originalNow() - 360_000;
    const follower = await worker.fetch(request, env, createContext());
    assert.equal(follower.status, 503);
    assert.equal((await follower.json()).reason, 'cache_refresh_expired');
    assert.equal(cache.calls.filter(({ url }) => url.endsWith('download_get_cache_state')).length > 0, true);
    assert.equal(cache.calls.filter(({ url }) => url.endsWith('download_acquire_cache_refresh')).length, 0);
  } finally {
    Date.now = originalNow;
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('a stalled follower poll body returns coordinator 503 at the lease deadline', { concurrency: false }, async () => {
  delete globalThis.bootstrapCache;
  const databaseNowSeconds = Math.floor(Date.now() / 1000);
  const cache = createCacheRpcFixture({ includeTicketState: true, databaseNowSeconds });
  const bootstrap = buildBootstrap();
  const path = '/downloads/stalled-follower-poll.bin';
  const pathHash = await sha256Hash(path);
  cache.seedLease(pathHash, '11111111-1111-4111-8111-000000000010', 1);
  let stateCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return new Response(JSON.stringify(bootstrap), { headers: { 'content-type': 'application/json' } });
    }
    if (url.endsWith('download_get_cache_state')) {
      stateCalls += 1;
      if (stateCalls > 1) {
        return createStalledRpcResponse(init.signal);
      }
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    throw new Error(`OpenList or upstream must not be called: ${url}`);
  };

  const env = {
    CONTROLLER_URL: 'https://controller.example.test',
    CONTROLLER_API_TOKEN: 'controller-token',
    ENV: 'test',
    ROLE: 'download',
    INSTANCE_ID: 'worker-1',
    BOOTSTRAP_CACHE_MODE: 'direct',
  };
  try {
    const response = await worker.fetch(await buildRequest(path), env, createContext());
    assert.equal(response.status, 503);
    assert.equal((await response.json()).reason, 'cache_refresh_expired');
    assert.equal(stateCalls, 2);
    assert.equal(cache.calls.filter(({ url }) => url.endsWith('download_acquire_cache_refresh')).length, 0);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('client cancellation while reading the initial cache state returns 499', { concurrency: false }, async () => {
  delete globalThis.bootstrapCache;
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const bootstrap = buildBootstrap();
  const controller = new AbortController();
  let stateStartedResolve;
  const stateStarted = new Promise((resolve) => { stateStartedResolve = resolve; });
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return new Response(JSON.stringify(bootstrap), { headers: { 'content-type': 'application/json' } });
    }
    if (url.endsWith('download_get_cache_state')) {
      stateStartedResolve();
      return createStalledRpcResponse(init.signal);
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    throw new Error(`OpenList or upstream must not be called: ${url}`);
  };

  const env = {
    CONTROLLER_URL: 'https://controller.example.test',
    CONTROLLER_API_TOKEN: 'controller-token',
    ENV: 'test',
    ROLE: 'download',
    INSTANCE_ID: 'worker-1',
    BOOTSTRAP_CACHE_MODE: 'direct',
  };
  try {
    const request = new Request(await buildRequest('/downloads/client-abort-state.bin'), { signal: controller.signal });
    const responsePromise = worker.fetch(request, env, createContext());
    await stateStarted;
    controller.abort(new DOMException('client cancelled', 'AbortError'));
    const response = await responsePromise;
    assert.equal(response.status, 499);
    assert.equal((await response.json()).reason, 'client_aborted');
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('client cancellation while reading a follower poll body returns 499', { concurrency: false }, async () => {
  delete globalThis.bootstrapCache;
  const databaseNowSeconds = Math.floor(Date.now() / 1000);
  const cache = createCacheRpcFixture({ includeTicketState: true, databaseNowSeconds });
  const bootstrap = buildBootstrap();
  const path = '/downloads/client-abort-follower-poll.bin';
  const pathHash = await sha256Hash(path);
  cache.seedLease(pathHash, '11111111-1111-4111-8111-000000000011', 30);
  let stateCalls = 0;
  let pollStartedResolve;
  const pollStarted = new Promise((resolve) => { pollStartedResolve = resolve; });
  const controller = new AbortController();
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return new Response(JSON.stringify(bootstrap), { headers: { 'content-type': 'application/json' } });
    }
    if (url.endsWith('download_get_cache_state')) {
      stateCalls += 1;
      if (stateCalls > 1) {
        pollStartedResolve();
        return createStalledRpcResponse(init.signal);
      }
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    throw new Error(`OpenList or upstream must not be called: ${url}`);
  };

  const env = {
    CONTROLLER_URL: 'https://controller.example.test',
    CONTROLLER_API_TOKEN: 'controller-token',
    ENV: 'test',
    ROLE: 'download',
    INSTANCE_ID: 'worker-1',
    BOOTSTRAP_CACHE_MODE: 'direct',
  };
  try {
    const request = new Request(await buildRequest(path), { signal: controller.signal });
    const responsePromise = worker.fetch(request, env, createContext());
    await pollStarted;
    controller.abort(new DOMException('client cancelled', 'AbortError'));
    const response = await responsePromise;
    assert.equal(response.status, 499);
    assert.equal((await response.json()).reason, 'client_aborted');
    assert.equal(cache.calls.filter(({ url }) => url.endsWith('download_acquire_cache_refresh')).length, 0);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('concurrent cold requests share one cache refresh owner', async () => {
  delete globalThis.bootstrapCache;
  const cache = createCacheRpcFixture();
  const bootstrap = buildBootstrap();
  const upstreamUrl = 'https://download.example.test/concurrent.bin';
  let alistCalls = 0;
  let upstreamCalls = 0;
  let alistStartedResolve;
  const alistStarted = new Promise((resolve) => { alistStartedResolve = resolve; });
  let allowAListResolve;
  const allowAList = new Promise((resolve) => { allowAListResolve = resolve; });
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return new Response(JSON.stringify(bootstrap), { headers: { 'content-type': 'application/json' } });
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url === 'https://postgrest.example.test/rpc/download_get_ticket_state') {
      const body = JSON.parse(init.body);
      const nowSeconds = Math.floor(Date.now() / 1000);
      return new Response(JSON.stringify([{
        found: true,
        ticket_hash: body.p_ticket_hash,
        issued_at: nowSeconds,
        first_used_at: null,
        hard_expire_at: nowSeconds + 300,
        idle_timeout_seconds: 300,
        idle_policy: 'first_use',
        idle_lease_expires_at: nowSeconds + 300,
      }]), { headers: { 'content-type': 'application/json' } });
    }
    if (url === 'https://postgrest.example.test/rpc/download_mark_ticket_used') {
      return new Response(JSON.stringify({ result: 'transitioned' }), { headers: { 'content-type': 'application/json' } });
    }
    if (url === 'https://alist.example.com/api/fs/link') {
      assert.equal(JSON.parse(init.body).action, 'acquire');
      alistCalls += 1;
      alistStartedResolve();
      await allowAList;
      return new Response(JSON.stringify({
        code: 200,
        data: {
          url: upstreamUrl,
          header: {},
          size: 5,
          download: {
            provider: 'generic',
            ticket: 'test-ticket-abcdefghijklmnopqrstuvwxyz',
            expires_at: Math.floor(Date.now() / 1000) + 300,
            report_success: false,
          },
        },
      }), { headers: { 'content-type': 'application/json' } });
    }
    if (url === upstreamUrl) {
      upstreamCalls += 1;
      return new Response('hello', { status: 200, headers: { 'content-length': '5' } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  const env = {
    CONTROLLER_URL: 'https://controller.example.test',
    CONTROLLER_API_TOKEN: 'controller-token',
    ENV: 'test',
    ROLE: 'download',
    INSTANCE_ID: 'worker-1',
    BOOTSTRAP_CACHE_MODE: 'direct',
  };
  try {
    const firstPromise = worker.fetch(await buildRequest('/downloads/concurrent-cache-worker.bin'), env, createContext());
    await alistStarted;
    const secondPromise = worker.fetch(await buildRequest('/downloads/concurrent-cache-worker.bin'), env, createContext());
    const stateDeadline = Date.now() + 1000;
    while (Date.now() < stateDeadline && cache.calls.filter(({ url }) => url.endsWith('download_get_cache_state')).length < 2) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    assert.equal(cache.calls.filter(({ url }) => url.endsWith('download_get_cache_state')).length >= 2, true);
    assert.equal(cache.calls.filter(({ url }) => url.endsWith('download_acquire_cache_refresh')).length, 1);
    allowAListResolve();
    const [first, second] = await Promise.all([firstPromise, secondPromise]);
    assert.equal(first.status, 200);
    assert.equal(second.status, 200);
    assert.equal(await first.text(), 'hello');
    assert.equal(await second.text(), 'hello');
    assert.equal(alistCalls, 1);
    assert.equal(upstreamCalls, 2);
    assert.equal(cache.calls.filter(({ url }) => url.endsWith('download_finish_cache_refresh')).length, 1);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('a valid Google HEAD probe publishes its lease and the next GET reuses it', async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const bootstrap = buildBootstrap();
  const upstreamUrl = 'https://drive.usercontent.google.com/download?id=head-then-get';
  let alistCalls = 0;
  const upstreamRequests = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return new Response(JSON.stringify(bootstrap), { headers: { 'content-type': 'application/json' } });
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url === 'https://alist.example.com/api/fs/link') {
      assert.equal(JSON.parse(init.body).action, 'acquire');
      alistCalls += 1;
      return new Response(JSON.stringify({
        code: 200,
        data: {
          url: upstreamUrl,
          header: {},
          size: 5,
          download: {
            provider: 'generic',
            ticket: 'test-ticket-head-then-get-abcdefghijklmnopqrstuvwxyz',
            expires_at: Math.floor(Date.now() / 1000) + 300,
            report_success: false,
          },
        },
      }), { headers: { 'content-type': 'application/json' } });
    }
    if (url === upstreamUrl) {
      const request = typeof input === 'string' ? null : input;
      upstreamRequests.push({
        method: request?.method || init.method || 'GET',
        range: request?.headers.get('range') || init.headers?.range || null,
      });
      if (request?.headers.get('range') === 'bytes=0-0') {
        return new Response('x', {
          status: 206,
          headers: { 'content-range': 'bytes 0-0/5', 'content-length': '1' },
        });
      }
      return new Response('hello', {
        status: 206,
        headers: { 'content-range': 'bytes 0-4/5', 'content-length': '5' },
      });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  const env = {
    CONTROLLER_URL: 'https://controller.example.test',
    CONTROLLER_API_TOKEN: 'controller-token',
    ENV: 'test',
    ROLE: 'download',
    INSTANCE_ID: 'worker-1',
    BOOTSTRAP_CACHE_MODE: 'direct',
  };
  try {
    const headResponse = await worker.fetch(
      await buildRequest('/downloads/head-then-get.bin', { method: 'HEAD', filesize: 5 }),
      env,
      createContext(),
    );
    assert.equal(headResponse.status, 200);
    assert.equal(await headResponse.text(), '');

    const finishCalls = cache.calls.filter(({ url }) => url.endsWith('download_finish_cache_refresh'));
    assert.equal(finishCalls.length, 1);
    assert.ok(finishCalls[0].body.p_link_data, 'valid HEAD must publish link data');
    assert.equal(finishCalls[0].body.p_error_code, null);

    const getResponse = await worker.fetch(
      await buildRequest('/downloads/head-then-get.bin', { filesize: 5 }),
      env,
      createContext(),
    );
    assert.equal(getResponse.status, 200);
    assert.equal(await getResponse.text(), 'hello');
    assert.equal(alistCalls, 1, 'GET should reuse the successful HEAD publication');
    assert.deepEqual(upstreamRequests.map(({ range }) => range), ['bytes=0-0', 'bytes=0-4']);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('an invalid Google full-range response is rejected before cache publication', async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const bootstrap = buildBootstrap();
  const upstreamUrl = 'https://drive.usercontent.google.com/download?id=invalid-range';
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return new Response(JSON.stringify(bootstrap), { headers: { 'content-type': 'application/json' } });
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url === 'https://alist.example.com/api/fs/link') {
      return new Response(JSON.stringify({
        code: 200,
        data: {
          url: upstreamUrl,
          header: {},
          size: 5,
          download: {
            provider: 'generic',
            ticket: 'test-ticket-invalid-range-abcdefghijklmnopqrstuvwxyz',
            expires_at: Math.floor(Date.now() / 1000) + 300,
            report_success: false,
          },
        },
      }), { headers: { 'content-type': 'application/json' } });
    }
    if (url === upstreamUrl) {
      return new Response('bad-range', {
        status: 206,
        headers: { 'content-range': 'bytes 0-3/5', 'content-length': '4' },
      });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const response = await worker.fetch(
      await buildRequest('/downloads/invalid-range.bin', { filesize: 5 }),
      {
        CONTROLLER_URL: 'https://controller.example.test',
        CONTROLLER_API_TOKEN: 'controller-token',
        ENV: 'test',
        ROLE: 'download',
        INSTANCE_ID: 'worker-1',
        BOOTSTRAP_CACHE_MODE: 'direct',
      },
      createContext(),
    );
    assert.equal(response.status, 502);
    assert.equal((await response.json()).reason, 'google_drive_range_mismatch');

    const finishCalls = cache.calls.filter(({ url }) => url.endsWith('download_finish_cache_refresh'));
    assert.equal(finishCalls.length, 1);
    assert.equal(finishCalls[0].body.p_link_data, null, 'invalid range must not publish link data');
  } finally {
    globalThis.fetch = originalFetch;
  }
});
