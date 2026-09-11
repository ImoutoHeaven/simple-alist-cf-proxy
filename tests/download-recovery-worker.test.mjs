import { test } from 'node:test';
import assert from 'node:assert/strict';
import worker, { __fairQueueTestHooks } from '../src/worker.js';
import { encryptBindingPayload } from '../src/origin-binding.js';
import { addDownloadEnvelope, createCacheRpcFixture } from './cache-rpc-fixture.mjs';

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

const buildBootstrap = ({ fairQueue = false, throttle = false, trueConcurrency = false } = {}) => ({
  configVersion: 'download-recovery-worker',
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
    throttleProfiles: {
      default: {
        hostPatterns: throttle ? ['*.googleapis.com'] : [],
        protectHttpCodes: [401, 403, 410, 429, 500, 502, 503, 504],
      },
    },
    ...(fairQueue ? {
      fairQueue: {
        enabled: true,
        hostPatterns: ['*.googleapis.com'],
        slotHandlerUrl: 'https://slot-handler.example.test',
        slotHandlerAuthKey: 'slot-secret',
        slotHandlerAuthHeader: 'X-FQ-Auth',
      },
    } : {}),
    ...(trueConcurrency ? {
      trueConcurrency: {
        enabled: true,
        hostPatterns: ['*.googleapis.com'],
        handlerUrl: 'https://cq.example.test',
        handlerAuthKey: 'cq-secret',
        heartbeat: {},
      },
    } : {}),
  },
});

const buildRequest = async (pathname, { filesize = 5, method = 'GET', headers: extraHeaders = {} } = {}) => {
  const expire = Math.floor(Date.now() / 1000) + 300;
  const encrypted = await encryptBindingPayload({
    v: 2,
    issuer: 'https://landing.example.com',
    workerAddress: 'https://worker.example.com',
  }, 'bootstrap-token');
  const payload = encodeBase64Url(JSON.stringify({
    v: 1,
    expireTime: expire,
    idle_timeout: 300,
    ticketNonce: 'abcdefghijklmnopqrstuvwxyz012345',
    ...(filesize === undefined ? {} : { filesize }),
    encrypt: encrypted,
  }));
  const payloadSign = await signPayload(payload, expire, 'bootstrap-token');
  const url = new URL(pathname, 'https://worker.example.com');
  url.searchParams.set('payload', payload);
  url.searchParams.set('payloadSign', payloadSign);
  return new Request(url, {
    method,
    headers: {
      origin: 'https://landing.example.com',
      'CF-Connecting-IP': '192.0.2.10',
      ...extraHeaders,
    },
  });
};

const createContext = () => {
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

const jsonResponse = (payload, init = {}) => new Response(JSON.stringify(payload), {
  status: init.status ?? 200,
  headers: { 'content-type': 'application/json', ...(init.headers || {}) },
});

const linkFor = (index) => ({
  url: `https://www.googleapis.com/drive/v3/files/recovery-${index}?alt=media`,
  header: {},
  size: 5,
  download: {
    provider: 'GoogleDrive',
    ticket: `google-ticket-${index}-abcdefghijklmnopqrstuvwxyz`,
    expires_at: Math.floor(Date.now() / 1000) + 300,
    report_success: true,
  },
});

const fairQueueGrantResponse = (index) => {
  const frames = [
    'event: accepted\n',
    `data: ${JSON.stringify({ queryToken: `query-${index}`, invocationEpoch: index, deadlineMs: Date.now() + 10_000 })}\n\n`,
    'event: result\n',
    `data: ${JSON.stringify({
      result: 'granted',
      queryToken: `query-${index}`,
      invocationEpoch: index,
      slotToken: `slot-${index}`,
      releaseOwnerRequired: true,
      meta: { attemptVersion: index, attemptTicket: index },
    })}\n\n`,
  ];
  let frameIndex = 0;
  const encoder = new TextEncoder();
  return new Response(new ReadableStream({
    pull(controller) {
      if (frameIndex >= frames.length) {
        controller.close();
        return;
      }
      controller.enqueue(encoder.encode(frames[frameIndex]));
      frameIndex += 1;
    },
  }), { status: 200, headers: { 'content-type': 'text/event-stream' } });
};

const createHeartbeatSocket = () => {
  const listeners = new Map();
  const emit = (type, event) => {
    for (const listener of listeners.get(type) || []) {
      listener(event);
    }
  };
  return {
    addEventListener(type, listener) {
      const set = listeners.get(type) || new Set();
      set.add(listener);
      listeners.set(type, set);
    },
    removeEventListener(type, listener) {
      listeners.get(type)?.delete(listener);
    },
    accept() {},
    send(data) {
      const payload = JSON.parse(data);
      if (payload.type === 'hello') {
        queueMicrotask(() => emit('message', { data: JSON.stringify({
          type: 'hello_ack',
          generation: 1,
          deadlineMs: Date.now() + 60_000,
          ackTimeoutMs: 2_000,
          heartbeatIntervalMs: 60_000,
          heartbeatTimeoutMs: 2_000,
          reconnectGraceMs: 2_000,
          startTimeoutMs: 2_000,
          hardExpireAtMs: Date.now() + 300_000,
        }) }));
      } else if (payload.type === 'heartbeat') {
        queueMicrotask(() => emit('message', { data: JSON.stringify({
          type: 'heartbeat_ack',
          generation: 1,
          deadlineMs: Date.now() + 60_000,
          hardExpireAtMs: Date.now() + 300_000,
        }) }));
      }
    },
    close(code = 1000, reason = '') {
      queueMicrotask(() => emit('close', { code, reason }));
    },
  };
};

test('Google quota recovery is owner-only, bounded to four tickets, and reports the final failure', async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const links = [linkFor(0), linkFor(1), linkFor(2), linkFor(3)];
  const acquireBodies = [];
  const reportBodies = [];
  const upstreamBodies = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      acquireBodies.push(body);
      return jsonResponse({ code: 200, data: links[acquireBodies.length - 1] });
    }
    if (url.startsWith('https://www.googleapis.com/drive/v3/files/recovery-')) {
      upstreamBodies.push(url);
      return new Response(JSON.stringify({
        error: {
          code: 403,
          errors: [{ reason: 'downloadQuotaExceeded' }],
        },
      }), {
        status: 403,
        headers: { 'content-type': 'application/json' },
      });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createContext();
    const response = await worker.fetch(await buildRequest('/downloads/quota-recovery.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    const body = await response.json();
    await Promise.allSettled(waitUntilPromises);

    assert.equal(response.status, 503);
    assert.equal(body.reason, 'upstream_auth_retry_exhausted');
    assert.equal(response.headers.get('Retry-After'), '30');
    assert.equal(acquireBodies.length, 4);
    assert.equal(upstreamBodies.length, 4);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].feedback.outcome, 'failure');
    assert.equal(reportBodies[0].feedback.status_code, 403);
    assert.equal(reportBodies[0].feedback.reason, 'downloadQuota');
    assert.deepEqual(acquireBodies.slice(1).map((bodyValue) => bodyValue.feedback.ticket), [
      links[0].download.ticket,
      links[1].download.ticket,
      links[2].download.ticket,
    ]);
    assert.deepEqual(acquireBodies.slice(1).map((bodyValue) => bodyValue.exclude.length), [1, 2, 3]);
    assert.equal(cache.calls.filter(({ url }) => url.endsWith('download_finish_cache_refresh')).length, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('OpenList HTTP errors preserve Retry-After and aborted JSON reads keep abort identity', async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  let abortController;
  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      return new Response(JSON.stringify({ code: 503, message: 'pool unavailable' }), {
        status: 503,
        headers: { 'content-type': 'application/json', 'Retry-After': '17' },
      });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createContext();
    const response = await worker.fetch(await buildRequest('/downloads/alist-error.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    assert.equal(response.status, 503);
    assert.equal(response.headers.get('Retry-After'), '30');
    await Promise.allSettled(waitUntilPromises);

    abortController = new AbortController();
    abortController.abort();
    const abortedRequest = new Request((await buildRequest('/downloads/aborted-api.bin')).url, {
      headers: (await buildRequest('/downloads/aborted-api.bin')).headers,
      signal: abortController.signal,
    });
    const abortedResponse = await worker.fetch(abortedRequest, {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, createContext().ctx);
    assert.equal(abortedResponse.status, 499);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('bounded Google error classification cancels an oversized body without exposing it', async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  let cancelled = false;
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      return jsonResponse({ code: 200, data: linkFor(0) });
    }
    if (url.startsWith('https://www.googleapis.com/drive/v3/files/recovery-0')) {
      const prefix = new TextEncoder().encode(JSON.stringify({ error: { errors: [{ reason: 'other' }] } }));
      return new Response(new ReadableStream({
        start(controller) {
          controller.enqueue(prefix);
          controller.enqueue(new Uint8Array(5000));
        },
        cancel() {
          cancelled = true;
        },
      }), { status: 403, headers: { 'content-type': 'application/json' } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createContext();
    const response = await worker.fetch(await buildRequest('/downloads/oversized-error.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(response.status, 403);
    assert.equal(cancelled, true);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('concurrent cached Google failures share one refresh owner and adopt its replacement', async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  const warmLink = { ...linkFor(0), download: { ...linkFor(0).download, report_success: false } };
  const replacementLink = { ...linkFor(1), size: 8 };
  let phase = 'warm';
  let failedFetches = 0;
  let releaseFailures;
  const bothFailures = new Promise((resolve) => { releaseFailures = resolve; });
  const acquireBodies = [];
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      acquireBodies.push(body);
      return jsonResponse({ code: 200, data: phase === 'warm' ? warmLink : replacementLink });
    }
    if (url.endsWith('/recovery-0?alt=media')) {
      if (phase === 'recovery') {
        failedFetches += 1;
        if (failedFetches === 2) {
          releaseFailures();
        }
        await bothFailures;
        return jsonResponse({ error: { errors: [{ reason: 'downloadQuotaExceeded' }] } }, { status: 403 });
      }
      return new Response('warm', { status: 200, headers: { 'content-length': '5' } });
    }
    if (url.endsWith('/recovery-1?alt=media')) {
      return new Response('recovered', { status: 200, headers: { 'content-length': '8' } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  const env = {
    CONTROLLER_URL: 'https://controller.example.test',
    CONTROLLER_API_TOKEN: 'controller-token',
    ENV: 'test',
    ROLE: 'download',
    INSTANCE_ID: 'worker-recovery',
    BOOTSTRAP_CACHE_MODE: 'direct',
  };
  try {
    const warmContext = createContext();
    const warmResponse = await worker.fetch(await buildRequest('/downloads/shared-recovery.bin'), env, warmContext.ctx);
    assert.equal(warmResponse.status, 200);
    assert.equal(await warmResponse.text(), 'warm');
    await Promise.allSettled(warmContext.waitUntilPromises);

    phase = 'recovery';
    const firstContext = createContext();
    const secondContext = createContext();
    const [first, second] = await Promise.all([
      worker.fetch(await buildRequest('/downloads/shared-recovery.bin'), env, firstContext.ctx),
      worker.fetch(await buildRequest('/downloads/shared-recovery.bin'), env, secondContext.ctx),
    ]);
    assert.equal(first.status, 200);
    assert.equal(second.status, 200);
    assert.deepEqual(await Promise.all([first.text(), second.text()]), ['recovered', 'recovered']);
    await Promise.allSettled([...firstContext.waitUntilPromises, ...secondContext.waitUntilPromises]);
    assert.equal(failedFetches, 2);
    assert.equal(acquireBodies.length, 2, 'one cached link and one owner replacement');
    assert.equal(cache.calls.filter(({ url }) => url.endsWith('download_acquire_cache_refresh')).length >= 2, true);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('terminal reports retry with one event id after transport and pool failures', async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  const reportBodies = [];
  let reportAttempts = 0;
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportAttempts += 1;
        reportBodies.push(body);
        if (reportAttempts === 1) {
          throw new Error('report transport failed');
        }
        if (reportAttempts === 2) {
          return new Response('{}', { status: 503, headers: { 'Retry-After': '1' } });
        }
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: { ...linkFor(0), download: { ...linkFor(0).download, provider: 'generic' } } });
    }
    if (url.includes('/recovery-0?alt=media')) {
      return new Response('gone', { status: 404, headers: { 'content-type': 'text/plain' } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createContext();
    const response = await worker.fetch(await buildRequest('/downloads/report-retry.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    assert.equal(response.status, 404);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(reportAttempts, 3);
    assert.equal(reportBodies[0].feedback.event_id, reportBodies[1].feedback.event_id);
    assert.equal(reportBodies[1].feedback.event_id, reportBodies[2].feedback.event_id);
    assert.equal(reportBodies[0].feedback.outcome, 'failure');
    assert.equal(reportBodies[0].feedback.status_code, 404);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('client cancellation before handoff schedules an independent abandoned report', async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  const abortController = new AbortController();
  const reportBodies = [];
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        assert.notEqual(init.signal, abortController.signal);
        assert.equal(init.signal?.aborted, false);
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: { ...linkFor(0), download: { ...linkFor(0).download, provider: 'generic' } } });
    }
    if (url.includes('/recovery-0?alt=media')) {
      abortController.abort();
      return new Response('aborted', { status: 200, headers: { 'content-length': '7' } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const baseRequest = await buildRequest('/downloads/abandoned-report.bin');
    const request = new Request(baseRequest.url, { headers: baseRequest.headers, signal: abortController.signal });
    const { ctx, waitUntilPromises } = createContext();
    const response = await worker.fetch(request, {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(response.status, 499);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].feedback.outcome, 'abandoned');
    assert.equal(reportBodies[0].feedback.status_code, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('canonical Google quota recovery settles queue breaker debt without a failure sample and rotates admission identity', async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  const calls = [];
  const acquireBodies = [];
  const settleBodies = [];
  const sampleBodies = [];
  let linkCalls = 0;
  let queueGrants = 0;
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url, headers } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap({ fairQueue: true, throttle: true }));
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      linkCalls += 1;
      acquireBodies.push(body);
      return jsonResponse({ code: 200, data: linkFor(linkCalls - 1) });
    }
    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/wait') {
      queueGrants += 1;
      calls.push(`queue-${queueGrants}`);
      return fairQueueGrantResponse(queueGrants);
    }
    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.push(`release-${JSON.parse(init.body).slotToken}`);
      return jsonResponse({ result: 'ok' });
    }
    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      settleBodies.push(JSON.parse(init.body));
      return jsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: settleBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }
    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      sampleBodies.push(JSON.parse(init.body));
      return jsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: sampleBodies.length,
        LAST_ERROR_CODE: null,
      }]);
    }
    if (url.includes('/recovery-0?alt=media')) {
      return new Response(JSON.stringify({ error: { errors: [{ reason: 'downloadQuotaExceeded' }] } }), {
        status: 403,
        headers: { 'content-type': 'application/json' },
      });
    }
    if (url.includes('/recovery-1?alt=media')) {
      calls.push('content-success');
      assert.equal(headers.get('range'), 'bytes=0-4');
      return new Response('fresh', { status: 200, headers: { 'content-length': '5' } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    delete globalThis.bootstrapCache;
    const resolved = __fairQueueTestHooks.resolveConfig({ ENABLE_CF_RATELIMITER: 'false' }, buildBootstrap({ fairQueue: true, throttle: true }), { download: {} });
    assert.equal(resolved.fairQueueEnabled, true);
    assert.equal(__fairQueueTestHooks.resolveAdmissionMode(resolved, 'www.googleapis.com'), 'queue_breaker');
    const { ctx, waitUntilPromises } = createContext();
    const response = await worker.fetch(await buildRequest('/downloads/queue-breaker-quota.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'fresh');
    await Promise.allSettled(waitUntilPromises);
    assert.equal(linkCalls, 2);
    assert.equal(queueGrants, 2);
    assert.equal(settleBodies.length, 1);
    assert.equal(sampleBodies.length, 1);
    assert.equal(sampleBodies[0].p_sample, 0, 'quota must never become a host failure sample');
    assert.equal(settleBodies[0].p_attempt_version, 1);
    assert.equal(settleBodies[0].p_attempt_ticket, 1);
    assert.deepEqual(acquireBodies[1].exclude, [linkFor(0).download.ticket]);
    assert.notEqual(calls.indexOf('queue-1'), calls.indexOf('queue-2'));
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('missing Google size fails closed before any content request or obsolete probe', async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  const firstLink = { ...linkFor(0) };
  delete firstLink.size;
  const calls = [];
  let linkCalls = 0;
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      linkCalls += 1;
      return jsonResponse({ code: 200, data: linkCalls === 1 ? firstLink : linkFor(1) });
    }
    if (url.includes('/recovery-0?alt=media') || url.includes('/recovery-1?alt=media')) {
      calls.push(url);
      throw new Error('content request should not be reached when Google size is missing');
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createContext();
    const response = await worker.fetch(await buildRequest('/downloads/size-probe-recovery.bin', { filesize: null }), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    assert.equal(response.status, 503);
    assert.equal(calls.length, 0);
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(calls, []);
    assert.equal(linkCalls, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Google suffix and EOF-clipped ranges pass exact content validation', async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  const upstreamRanges = [];
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      return jsonResponse({ code: 200, data: linkFor(0) });
    }
    if (url.includes('/recovery-0?alt=media')) {
      const range = request.headers.get('range');
      upstreamRanges.push(range);
      if (range === 'bytes=-2') {
        return new Response('de', {
          status: 206,
          headers: { 'content-range': 'bytes 3-4/5', 'content-length': '2' },
        });
      }
      if (range === 'bytes=0-99') {
        return new Response('abcde', {
          status: 206,
          headers: { 'content-range': 'bytes 0-4/5', 'content-length': '5' },
        });
      }
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const suffixContext = createContext();
    const suffixResponse = await worker.fetch(
      await buildRequest('/downloads/range-suffix.bin', { headers: { range: 'bytes=-2' } }),
      {
        CONTROLLER_URL: 'https://controller.example.test',
        CONTROLLER_API_TOKEN: 'controller-token',
        ENV: 'test',
        ROLE: 'download',
        INSTANCE_ID: 'worker-recovery',
        BOOTSTRAP_CACHE_MODE: 'direct',
      },
      suffixContext.ctx,
    );
    assert.equal(suffixResponse.status, 206);
    assert.equal(await suffixResponse.text(), 'de');
    await Promise.allSettled(suffixContext.waitUntilPromises);

    const clippedContext = createContext();
    const clippedResponse = await worker.fetch(
      await buildRequest('/downloads/range-clipped.bin', { headers: { range: 'bytes=0-99' } }),
      {
        CONTROLLER_URL: 'https://controller.example.test',
        CONTROLLER_API_TOKEN: 'controller-token',
        ENV: 'test',
        ROLE: 'download',
        INSTANCE_ID: 'worker-recovery',
        BOOTSTRAP_CACHE_MODE: 'direct',
      },
      clippedContext.ctx,
    );
    assert.equal(clippedResponse.status, 206);
    assert.equal(await clippedResponse.text(), 'abcde');
    await Promise.allSettled(clippedContext.waitUntilPromises);
    assert.deepEqual(upstreamRanges, ['bytes=-2', 'bytes=0-99']);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Google content qualification rejects wrong-size and metadata responses while allowing valid text', async () => {
  const scenarios = [
    {
      name: 'wrong-size',
      response: () => new Response('bad!', { status: 200, headers: { 'content-length': '4' } }),
      status: 502,
      reason: 'google_drive_size_mismatch',
    },
    {
      name: 'metadata-204',
      response: () => new Response(null, { status: 204 }),
      status: 502,
      reason: 'google_drive_content_invalid',
    },
    {
      name: 'range-length-mismatch',
      response: () => new Response('hello', {
        status: 206,
        headers: { 'content-range': 'bytes 0-4/5', 'content-length': '9' },
      }),
      status: 502,
      reason: 'google_drive_range_mismatch',
    },
    {
      name: 'valid-text',
      response: () => new Response('hello', { status: 200, headers: { 'content-type': 'text/plain', 'content-length': '5' } }),
      status: 200,
      body: 'hello',
    },
  ];

  for (const scenario of scenarios) {
    const cache = createCacheRpcFixture({ includeTicketState: true });
    const originalFetch = globalThis.fetch;
    const contentRequests = [];
    globalThis.fetch = async (input, init = {}) => {
      const request = input instanceof Request ? input : new Request(input, init);
      const { url } = request;
      if (url === 'https://controller.example.test/api/v0/bootstrap') {
        return jsonResponse(buildBootstrap());
      }
      const cacheResponse = await cache.handle(input, init);
      if (cacheResponse) {
        return cacheResponse;
      }
      if (url.startsWith('https://alist.example.com/api/fs/link')) {
        const body = JSON.parse(init.body);
        if (body.action === 'report') {
          return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
        }
        return jsonResponse({ code: 200, data: linkFor(0) });
      }
      if (url.includes('/recovery-0?alt=media')) {
        contentRequests.push(request);
        return scenario.response();
      }
      throw new Error(`unexpected fetch URL: ${url}`);
    };

    try {
      const { ctx, waitUntilPromises } = createContext();
      const response = await worker.fetch(await buildRequest(`/downloads/${scenario.name}.bin`), {
        CONTROLLER_URL: 'https://controller.example.test',
        CONTROLLER_API_TOKEN: 'controller-token',
        ENV: 'test',
        ROLE: 'download',
        INSTANCE_ID: 'worker-recovery',
        BOOTSTRAP_CACHE_MODE: 'direct',
      }, ctx);
      assert.equal(response.status, scenario.status);
      if (scenario.body) {
        assert.equal(await response.text(), scenario.body);
      } else {
        const body = await response.json();
        assert.equal(body.reason, scenario.reason);
        assert.equal(cache.calls.filter(({ url }) => url.endsWith('download_finish_cache_refresh')).length, 1);
      }
      await Promise.allSettled(waitUntilPromises);
      assert.equal(contentRequests.length, 1);
    } finally {
      globalThis.fetch = originalFetch;
    }
  }
});

test('Google provider identity preserves HEAD and full-range handling across CDN redirects', async () => {
  const scenarios = [
    {
      name: 'cdn-head',
      method: 'HEAD',
      link: { ...linkFor(0), url: 'https://cdn.example.test/media/head' },
      fetchResponse(request) {
        assert.equal(request.url, 'https://cdn.example.test/media/head');
        assert.equal(request.method, 'GET');
        assert.equal(request.headers.get('range'), 'bytes=0-0');
        return new Response('x', { status: 206, headers: { 'content-range': 'bytes 0-0/5', 'content-length': '1' } });
      },
      status: 200,
      body: '',
    },
    {
      name: 'google-cdn-full',
      link: linkFor(0),
      fetchResponse(request) {
        if (request.url.startsWith('https://www.googleapis.com/drive/v3/files/recovery-0')) {
          assert.equal(request.headers.get('range'), 'bytes=0-4');
          return new Response(null, { status: 302, headers: { location: 'https://cdn.example.test/media/full' } });
        }
        assert.equal(request.url, 'https://cdn.example.test/media/full');
        assert.equal(request.headers.get('range'), 'bytes=0-4');
        return new Response('hello', { status: 206, headers: { 'content-range': 'bytes 0-4/5', 'content-length': '5' } });
      },
      status: 200,
      body: 'hello',
    },
    {
      name: 'cdn-head-mismatch',
      method: 'HEAD',
      link: { ...linkFor(0), url: 'https://cdn.example.test/media/mismatch' },
      fetchResponse(request) {
        return new Response('xx', { status: 206, headers: { 'content-range': 'bytes 0-1/5', 'content-length': '2' } });
      },
      status: 502,
      reason: 'google_drive_probe_invalid',
    },
  ];

  for (const scenario of scenarios) {
    const cache = createCacheRpcFixture({ includeTicketState: true });
    const originalFetch = globalThis.fetch;
    const requests = [];
    globalThis.fetch = async (input, init = {}) => {
      const request = input instanceof Request ? input : new Request(input, init);
      const { url } = request;
      if (url === 'https://controller.example.test/api/v0/bootstrap') {
        return jsonResponse(buildBootstrap());
      }
      const cacheResponse = await cache.handle(input, init);
      if (cacheResponse) {
        return cacheResponse;
      }
      if (url.startsWith('https://alist.example.com/api/fs/link')) {
        const body = JSON.parse(init.body);
        if (body.action === 'report') {
          return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
        }
        return jsonResponse({ code: 200, data: scenario.link });
      }
      requests.push(request);
      return scenario.fetchResponse(request);
    };

    try {
      const { ctx, waitUntilPromises } = createContext();
      const response = await worker.fetch(await buildRequest(`/downloads/${scenario.name}.bin`, { method: scenario.method || 'GET' }), {
        CONTROLLER_URL: 'https://controller.example.test',
        CONTROLLER_API_TOKEN: 'controller-token',
        ENV: 'test',
        ROLE: 'download',
        INSTANCE_ID: 'worker-recovery',
        BOOTSTRAP_CACHE_MODE: 'direct',
      }, ctx);
      assert.equal(response.status, scenario.status);
      if (scenario.body !== undefined && scenario.status === 200) {
        assert.equal(await response.text(), scenario.body);
      } else if (scenario.status !== 200) {
        assert.equal((await response.json()).reason, scenario.reason);
      }
      await Promise.allSettled(waitUntilPromises);
      assert.equal(requests.length, scenario.name === 'google-cdn-full' ? 2 : 1);
    } finally {
      globalThis.fetch = originalFetch;
    }
  }
});

test('protected nonquota Google failures are sampled before replacement in breaker modes', async () => {
  for (const fairQueue of [false, true]) {
    delete globalThis.bootstrapCache;
    const resolvedAdmission = __fairQueueTestHooks.resolveConfig(
      { ENABLE_CF_RATELIMITER: 'false' },
      buildBootstrap({ fairQueue, throttle: true }),
      { download: {} },
    );
    assert.equal(
      __fairQueueTestHooks.resolveAdmissionMode(resolvedAdmission, 'www.googleapis.com'),
      fairQueue ? 'queue_breaker' : 'breaker_only',
    );
    const cache = createCacheRpcFixture({ includeTicketState: true });
    const originalFetch = globalThis.fetch;
    const samples = [];
    let linkCalls = 0;
    let queueCalls = 0;
    globalThis.fetch = async (input, init = {}) => {
      const request = input instanceof Request ? input : new Request(input, init);
      const { url } = request;
      if (url === 'https://controller.example.test/api/v0/bootstrap') {
        return jsonResponse(buildBootstrap({ fairQueue, throttle: true }));
      }
      const cacheResponse = await cache.handle(input, init);
      if (cacheResponse) {
        return cacheResponse;
      }
      if (url.startsWith('https://alist.example.com/api/fs/link')) {
        const body = JSON.parse(init.body);
        if (body.action === 'report') {
          return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
        }
        linkCalls += 1;
        return jsonResponse({ code: 200, data: linkFor(linkCalls - 1) });
      }
      if (url === 'https://slot-handler.example.test/api/v1/fairqueue/wait') {
        queueCalls += 1;
        return fairQueueGrantResponse(queueCalls);
      }
      if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
        return jsonResponse({ result: 'ok' });
      }
      if (url.startsWith('https://postgrest.example.test/THROTTLE_PROTECTION')) {
        return jsonResponse([{
          STATE: 'closed',
          OPEN_UNTIL: null,
          OPEN_REASON: null,
          VERSION: 1,
          LAST_ERROR_CODE: null,
        }]);
      }
      if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
        return jsonResponse([{
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
        const body = JSON.parse(init.body);
        samples.push(body);
        return jsonResponse([{
          STATE: 'closed',
          OPEN_UNTIL: null,
          OPEN_REASON: null,
          VERSION: samples.length,
          LAST_ERROR_CODE: body.p_status_code,
        }]);
      }
      if (url === 'https://www.googleapis.com/drive/v3/files/recovery-0?alt=media') {
        return new Response('server-failure', { status: 500, headers: { 'content-length': '14' } });
      }
      if (url === 'https://www.googleapis.com/drive/v3/files/recovery-1?alt=media') {
        return new Response('hello', { status: 200, headers: { 'content-length': '5' } });
      }
      throw new Error(`unexpected fetch URL: ${url}`);
    };

    try {
      const { ctx, waitUntilPromises } = createContext();
      const response = await worker.fetch(await buildRequest(`/downloads/nonquota-${fairQueue ? 'queue' : 'breaker'}.bin`), {
        CONTROLLER_URL: 'https://controller.example.test',
        CONTROLLER_API_TOKEN: 'controller-token',
        ENV: 'test',
        ROLE: 'download',
        INSTANCE_ID: 'worker-recovery',
        BOOTSTRAP_CACHE_MODE: 'direct',
      }, ctx);
      assert.equal(response.status, 200);
      assert.equal(await response.text(), 'hello');
      await Promise.allSettled(waitUntilPromises);
      assert.equal(linkCalls, 2);
      assert.equal(samples[0].p_sample, 1, fairQueue ? 'queue_breaker must sample nonquota failure' : 'breaker_only must sample nonquota failure');
      assert.equal(samples.some((body) => body.p_status_code === 500), true);
    } finally {
      globalThis.fetch = originalFetch;
      delete globalThis.bootstrapCache;
    }
  }
});

test('healthy cache recursive redirects preserve the global attempt cap', async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  const outerRequest = await buildRequest('/downloads/recursive-cache-outer.bin');
  const innerRequest = await buildRequest('/downloads/recursive-cache-inner.bin');
  let phase = 'warm';
  let linkCalls = 0;
  const attemptedUrls = [];
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      linkCalls += 1;
      return jsonResponse({ code: 200, data: linkFor(linkCalls - 1) });
    }
    if (url.endsWith('/recovery-0?alt=media')) {
      attemptedUrls.push(url);
      if (phase === 'warm') {
        return new Response('outer', { status: 200, headers: { 'content-length': '5' } });
      }
      return new Response(null, { status: 302, headers: { location: innerRequest.url } });
    }
    if (url.endsWith('/recovery-1?alt=media')) {
      attemptedUrls.push(url);
      if (phase === 'warm') {
        return new Response('inner', { status: 200, headers: { 'content-length': '5' } });
      }
      return jsonResponse({ error: { errors: [{ reason: 'downloadQuotaExceeded' }] } }, { status: 403 });
    }
    if (/\/recovery-[234]\?alt=media$/.test(url)) {
      attemptedUrls.push(url);
      return jsonResponse({ error: { errors: [{ reason: 'downloadQuotaExceeded' }] } }, { status: 403 });
    }
    if (url.endsWith('/recovery-4?alt=media')) {
      throw new Error('fifth recursive authorization must not be opened');
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  const env = {
    CONTROLLER_URL: 'https://controller.example.test',
    CONTROLLER_API_TOKEN: 'controller-token',
    ENV: 'test',
    ROLE: 'download',
    INSTANCE_ID: 'worker-recovery',
    BOOTSTRAP_CACHE_MODE: 'direct',
  };
  try {
    const outerWarmContext = createContext();
    const outerWarm = await worker.fetch(outerRequest, env, outerWarmContext.ctx);
    assert.equal(outerWarm.status, 200);
    await outerWarm.text();
    await Promise.allSettled(outerWarmContext.waitUntilPromises);
    const innerWarmContext = createContext();
    const innerWarm = await worker.fetch(innerRequest, env, innerWarmContext.ctx);
    assert.equal(innerWarm.status, 200);
    await innerWarm.text();
    await Promise.allSettled(innerWarmContext.waitUntilPromises);

    phase = 'recover';
    const recoveryContext = createContext();
    const response = await worker.fetch(outerRequest, env, recoveryContext.ctx);
    assert.equal(response.status, 503);
    assert.equal((await response.json()).reason, 'upstream_auth_retry_exhausted');
    await Promise.allSettled(recoveryContext.waitUntilPromises);
    assert.equal(linkCalls, 4);
    assert.equal(attemptedUrls.some((url) => url.endsWith('/recovery-4?alt=media')), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('validated headers survive a cache finish that crosses the opening deadline', async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  const performanceDescriptor = Object.getOwnPropertyDescriptor(globalThis, 'performance');
  let monotonicNow = 0;
  let finishStarted;
  const finishStartedPromise = new Promise((resolve) => { finishStarted = resolve; });
  let releaseFinish;
  const finishReleasePromise = new Promise((resolve) => { releaseFinish = resolve; });
  cache.setFinishHandler(async ({ body }) => {
    if (body.p_link_data) {
      finishStarted();
      await finishReleasePromise;
      throw new Error('simulated cache finish failure after headers');
    }
  });
  Object.defineProperty(globalThis, 'performance', {
    configurable: true,
    value: { now: () => monotonicNow },
  });
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: linkFor(0) });
    }
    if (url.includes('/recovery-0?alt=media')) {
      return new Response('hello', { status: 200, headers: { 'content-length': '5' } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const context = createContext();
    const requestPromise = worker.fetch(await buildRequest('/downloads/finish-crosses-deadline.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, context.ctx);
    await finishStartedPromise;
    monotonicNow = 200_000;
    releaseFinish();
    const response = await requestPromise;
    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'hello');
    await Promise.allSettled(context.waitUntilPromises);
  } finally {
    globalThis.fetch = originalFetch;
    if (performanceDescriptor) {
      Object.defineProperty(globalThis, 'performance', performanceDescriptor);
    }
  }
});

test('validated headers survive a stale cache finish result held beyond the opening deadline', async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  const performanceDescriptor = Object.getOwnPropertyDescriptor(globalThis, 'performance');
  let monotonicNow = 0;
  let finishStarted;
  const finishStartedPromise = new Promise((resolve) => { finishStarted = resolve; });
  let releaseFinish;
  const finishReleasePromise = new Promise((resolve) => { releaseFinish = resolve; });
  let finishCalled = false;
  cache.setFinishHandler(async ({ body }) => {
    if (!body.p_link_data) {
      return undefined;
    }
    finishCalled = true;
    finishStarted();
    await finishReleasePromise;
    return {
      result: 'stale',
      version: null,
      invalid_version: body.p_failed_version,
      retry_after: null,
      last_error_code: null,
      observed_at: new Date().toISOString(),
    };
  });
  Object.defineProperty(globalThis, 'performance', {
    configurable: true,
    value: { now: () => monotonicNow },
  });
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: linkFor(0) });
    }
    if (url.includes('/recovery-0?alt=media')) {
      return new Response('hello', { status: 200, headers: { 'content-length': '5' } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const context = createContext();
    const responsePromise = worker.fetch(await buildRequest('/downloads/stale-finish.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, context.ctx);
    await finishStartedPromise;
    assert.equal(finishCalled, true);
    monotonicNow = 200_000;
    releaseFinish();
    const response = await responsePromise;
    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'hello');
    await Promise.allSettled(context.waitUntilPromises);
    assert.equal(finishCalled, true);
  } finally {
    globalThis.fetch = originalFetch;
    if (performanceDescriptor) {
      Object.defineProperty(globalThis, 'performance', performanceDescriptor);
    }
  }
});

test('CQ healthy streams retain the signed lease hard expiry beyond the opening budget', async () => {
  const cache = createCacheRpcFixture({
    includeTicketState: true,
    acquireLeaseDurationSeconds: 30.15,
  });
  const originalFetch = globalThis.fetch;
  delete globalThis.bootstrapCache;
  const calls = [];
  let acquireBody = null;
  let laterChunkReady;
  const laterChunkPromise = new Promise((resolve) => { laterChunkReady = resolve; });
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap({ trueConcurrency: true }));
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: linkFor(0) });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      acquireBody = JSON.parse(init.body);
      calls.push('acquire');
      return jsonResponse({
        result: 'granted',
        leaseId: 'lease-long-stream',
        leaseToken: 'token-long-stream',
        expiresAtMs: acquireBody.hardExpireAtMs - 1_000,
        claimToken: 'claim-long-stream',
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/claim') {
      calls.push('claim');
      return jsonResponse({
        result: 'granted',
        leaseId: 'lease-long-stream',
        leaseToken: 'token-long-stream',
        expiresAtMs: acquireBody.hardExpireAtMs - 1_000,
        handoffToken: 'handoff-long-stream',
        handoffDeadlineMs: Date.now() + 10_000,
      });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/ack_handoff') {
      calls.push('ack');
      return jsonResponse({ result: 'acknowledged' });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/heartbeat') {
      calls.push('heartbeat');
      return { status: 101, webSocket: createHeartbeatSocket() };
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/release') {
      calls.push('release');
      return jsonResponse({ result: 'released' });
    }
    if (url.includes('/recovery-0?alt=media')) {
      calls.push('origin');
      return new Response(new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode('he'));
          setTimeout(() => {
            try {
              controller.enqueue(new TextEncoder().encode('llo'));
              controller.close();
            } catch (_error) {
              // The consumer may have cancelled after a premature stream abort.
            } finally {
              laterChunkReady();
            }
          }, 250);
        },
      }), {
        status: 206,
        headers: { 'content-range': 'bytes 0-4/5', 'content-length': '5' },
      });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createContext();
    const response = await worker.fetch(await buildRequest('/downloads/cq-long-stream.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    assert.equal(response.status, 200);
    assert.ok(acquireBody);
    assert.ok(acquireBody.hardExpireAtMs - Date.now() > 150_000);
    const reader = response.body.getReader();
    const firstChunk = await reader.read();
    assert.equal(new TextDecoder().decode(firstChunk.value), 'he');
    await laterChunkPromise;
    const laterChunk = await reader.read();
    assert.equal(new TextDecoder().decode(laterChunk.value), 'llo');
    await reader.cancel();
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(calls.slice(0, 5), ['acquire', 'claim', 'ack', 'heartbeat', 'origin']);
    assert.equal(calls.includes('release'), true);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('recursive Worker redirects carry the recovery attempt budget into the new path', async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  const innerRequest = await buildRequest('/downloads/recursive-budget-inner.bin');
  const linkCalls = [];
  let innerFailures = 0;
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      linkCalls.push(body.path);
      return jsonResponse({ code: 200, data: linkFor(linkCalls.length - 1) });
    }
    if (url.endsWith('/recovery-0?alt=media')) {
      return new Response(null, { status: 302, headers: { Location: innerRequest.url } });
    }
    if (url.endsWith('/recovery-1?alt=media') || url.endsWith('/recovery-2?alt=media') || url.endsWith('/recovery-3?alt=media')) {
      innerFailures += 1;
      return jsonResponse({ error: { errors: [{ reason: 'downloadQuotaExceeded' }] } }, { status: 403 });
    }
    if (url.endsWith('/recovery-4?alt=media')) {
      throw new Error('fifth cold-chain authorization must not be opened');
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    delete globalThis.bootstrapCache;
    const { ctx, waitUntilPromises } = createContext();
    const response = await worker.fetch(await buildRequest('/downloads/recursive-budget-outer.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    assert.equal(response.status, 503);
    assert.equal((await response.json()).reason, 'upstream_auth_retry_exhausted');
    await Promise.allSettled(waitUntilPromises);
    assert.equal(linkCalls.length, 4);
    assert.equal(innerFailures, 3);
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
});

test('final report aborts a stalled JSON body at the ownership deadline', async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  const expiringLink = linkFor(0);
  expiringLink.download.expires_at = Math.floor(Date.now() / 1000) + 1;
  let reportAttempts = 0;
  let reportBodyCancelled = false;
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportAttempts += 1;
        const bodyStream = new ReadableStream({
          cancel() {
            reportBodyCancelled = true;
          },
        });
        return new Response(bodyStream, { status: 200, headers: { 'content-type': 'application/json' } });
      }
      return jsonResponse({ code: 200, data: { ...expiringLink, download: { ...expiringLink.download, provider: 'generic' } } });
    }
    if (url.includes('/recovery-0?alt=media')) {
      return new Response('gone', { status: 404, headers: { 'content-type': 'text/plain' } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createContext();
    const response = await worker.fetch(await buildRequest('/downloads/stalled-report.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    assert.equal(response.status, 404);
    await Promise.allSettled(waitUntilPromises);
    assert.equal(reportAttempts, 1);
    assert.equal(reportBodyCancelled, true);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('owner deadline callback aborts even when the timer fires fractionally early', async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  const performanceDescriptor = Object.getOwnPropertyDescriptor(globalThis, 'performance');
  let monotonicNow = 0;
  let ownerTimerCallback = null;
  let ownerTimerHandle = null;
  let ownerTimerDelay = null;
  let upstreamStarted;
  const upstreamStartedPromise = new Promise((resolve) => { upstreamStarted = resolve; });
  const reportBodies = [];
  Object.defineProperty(globalThis, 'performance', {
    configurable: true,
    value: { now: () => monotonicNow },
  });
  globalThis.setTimeout = (callback, delay, ...args) => {
    if (!ownerTimerCallback && Number(delay) >= 100_000 && Number(delay) <= 160_000) {
      ownerTimerDelay = Number(delay);
      ownerTimerHandle = { ownerTimer: true, cleared: false };
      ownerTimerCallback = () => {
        if (!ownerTimerHandle.cleared) {
          callback(...args);
        }
      };
      return ownerTimerHandle;
    }
    return originalSetTimeout(callback, delay, ...args);
  };
  globalThis.clearTimeout = (handle) => {
    if (handle?.ownerTimer) {
      handle.cleared = true;
      return;
    }
    return originalClearTimeout(handle);
  };
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: linkFor(0) });
    }
    if (url.includes('/recovery-0?alt=media')) {
      upstreamStarted(init.signal);
      return await new Promise((resolve, reject) => {
        init.signal?.addEventListener?.('abort', () => reject(init.signal.reason || new DOMException('aborted', 'AbortError')), { once: true });
      });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const context = createContext();
    const requestPromise = worker.fetch(await buildRequest('/downloads/fractional-owner-deadline.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, context.ctx);
    await upstreamStartedPromise;
    assert.equal(typeof ownerTimerCallback, 'function');
    assert.equal(Number.isFinite(ownerTimerDelay), true);
    monotonicNow = ownerTimerDelay - 0.1;
    ownerTimerCallback();
    const response = await Promise.race([
      requestPromise,
      new Promise((_, reject) => originalSetTimeout(() => reject(new Error('owner deadline did not abort')), 500)),
    ]);
    assert.equal(response.status, 503);
    assert.equal((await response.json()).reason, 'recovery_deadline_exhausted');
    await Promise.allSettled(context.waitUntilPromises);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].feedback.ticket, linkFor(0).download.ticket);
    assert.equal(reportBodies[0].feedback.outcome, 'failure');
    assert.equal(reportBodies[0].feedback.status_code, 0);
    assert.equal(reportBodies[0].feedback.reason, 'recovery_deadline_exhausted');
  } finally {
    globalThis.fetch = originalFetch;
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
    if (performanceDescriptor) {
      Object.defineProperty(globalThis, 'performance', performanceDescriptor);
    }
  }
});

test('owned transport failures return shared backoff and real fetch aborts report abandonment', async () => {
  for (const kind of ['transport', 'abort']) {
    const cache = createCacheRpcFixture({ includeTicketState: true });
    const originalFetch = globalThis.fetch;
    const abortController = new AbortController();
    const reports = [];
    let finishBodies = [];
    globalThis.fetch = async (input, init = {}) => {
      const request = input instanceof Request ? input : new Request(input, init);
      const { url } = request;
      if (url === 'https://controller.example.test/api/v0/bootstrap') {
        return jsonResponse(buildBootstrap());
      }
      const cacheResponse = await cache.handle(input, init);
      if (cacheResponse) {
        return cacheResponse;
      }
      if (url.startsWith('https://alist.example.com/api/fs/link')) {
        const body = JSON.parse(init.body);
        if (body.action === 'report') {
          assert.notEqual(init.signal, abortController.signal);
          assert.equal(init.signal?.aborted, false);
          reports.push(body);
          return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
        }
        return jsonResponse({ code: 200, data: linkFor(0) });
      }
      if (url.includes('/recovery-0?alt=media')) {
        if (kind === 'abort') {
          abortController.abort();
          throw abortController.signal.reason || new DOMException('aborted', 'AbortError');
        }
        throw new TypeError('network down');
      }
      throw new Error(`unexpected fetch URL: ${url}`);
    };

    try {
      const baseRequest = await buildRequest(`/downloads/owned-${kind}.bin`);
      const request = kind === 'abort'
        ? new Request(baseRequest.url, { headers: baseRequest.headers, signal: abortController.signal })
        : baseRequest;
      const { ctx, waitUntilPromises } = createContext();
      const response = await worker.fetch(request, {
        CONTROLLER_URL: 'https://controller.example.test',
        CONTROLLER_API_TOKEN: 'controller-token',
        ENV: 'test',
        ROLE: 'download',
        INSTANCE_ID: 'worker-recovery',
        BOOTSTRAP_CACHE_MODE: 'direct',
      }, ctx);
      assert.equal(response.status, kind === 'abort' ? 499 : 503);
      await Promise.allSettled(waitUntilPromises);
      assert.equal(reports.length, 1);
      assert.equal(reports[0].feedback.outcome, kind === 'abort' ? 'abandoned' : 'failure');
      assert.equal(reports[0].feedback.status_code, 0);
      finishBodies = cache.calls
        .filter(({ url }) => url.endsWith('download_finish_cache_refresh'))
        .map(({ body }) => body);
      assert.equal(finishBodies.length, 1);
      assert.equal(finishBodies[0].p_link_data, null);
    } finally {
      globalThis.fetch = originalFetch;
    }
  }
});

test('Google synthetic full-range 200 requires Content-Length matching Link.size', { concurrency: false }, async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  delete globalThis.bootstrapCache;
  const reportBodies = [];
  const markCalls = [];
  const originRanges = [];
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url.endsWith('/rpc/download_mark_ticket_used')) {
      markCalls.push(JSON.parse(init.body));
    }
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: linkFor(0) });
    }
    if (url === 'https://www.googleapis.com/drive/v3/files/recovery-0?alt=media') {
      originRanges.push(request.headers.get('range'));
      return new Response('hello', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createContext();
    const response = await worker.fetch(await buildRequest('/downloads/google-no-content-length.bin', { filesize: 99 }), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    assert.equal(response.status, 502);
    assert.equal((await response.json()).reason, 'google_drive_content_invalid');
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(originRanges, ['bytes=0-4']);
    assert.deepEqual(markCalls, []);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].feedback.outcome, 'failure');
    assert.equal(reportBodies[0].feedback.status_code, 200);
    assert.equal(reportBodies[0].feedback.reason, 'google_drive_content_invalid');
    const failedFinish = cache.calls.find(({ url, body }) => (
      url.endsWith('download_finish_cache_refresh') && body.p_link_data == null
    ));
    assert.equal(failedFinish?.body.p_error_code, 502);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('invalid Google 2xx content never samples success in breaker admission modes', { concurrency: false }, async () => {
  const invalidContentCases = [
    {
      name: 'synthetic-200-without-content-length',
      response: () => new Response('hello', { status: 200, headers: { 'content-type': 'application/octet-stream' } }),
      reason: 'google_drive_content_invalid',
      status: 200,
    },
    {
      name: 'wrong-size',
      response: () => new Response('bad!', { status: 200, headers: { 'content-length': '4' } }),
      reason: 'google_drive_size_mismatch',
      status: 200,
    },
    {
      name: 'metadata-204',
      response: () => new Response(null, { status: 204 }),
      reason: 'google_drive_content_invalid',
      status: 204,
    },
    {
      name: 'malformed-range',
      response: () => new Response('hello', {
        status: 206,
        headers: { 'content-range': 'bytes 0-4/5', 'content-length': '9' },
      }),
      reason: 'google_drive_range_mismatch',
      status: 206,
    },
  ];
  const modes = [
    { name: 'breaker_only', fairQueue: false },
    { name: 'queue_breaker', fairQueue: true },
  ];

  for (const mode of modes) {
    for (const invalidCase of invalidContentCases) {
      const cache = createCacheRpcFixture({ includeTicketState: true });
      const originalFetch = globalThis.fetch;
      delete globalThis.bootstrapCache;
      const sampleBodies = [];
      const settleBodies = [];
      const reportBodies = [];
      const markCalls = [];
      const releaseBodies = [];
      let upstreamCalls = 0;
      globalThis.fetch = async (input, init = {}) => {
        const request = input instanceof Request ? input : new Request(input, init);
        const { url } = request;
        if (url.endsWith('/rpc/download_mark_ticket_used')) {
          markCalls.push(JSON.parse(init.body));
        }
        if (url === 'https://controller.example.test/api/v0/bootstrap') {
          return jsonResponse(buildBootstrap({ fairQueue: mode.fairQueue, throttle: true }));
        }
        const cacheResponse = await cache.handle(input, init);
        if (cacheResponse) {
          return cacheResponse;
        }
        if (url.startsWith('https://alist.example.com/api/fs/link')) {
          const body = JSON.parse(init.body);
          if (body.action === 'report') {
            reportBodies.push(body);
            return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
          }
          return jsonResponse({ code: 200, data: linkFor(0) });
        }
        if (mode.fairQueue && url === 'https://slot-handler.example.test/api/v1/fairqueue/wait') {
          return fairQueueGrantResponse(1);
        }
        if (mode.fairQueue && url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
          releaseBodies.push(JSON.parse(init.body));
          return jsonResponse({ result: 'ok' });
        }
        if (!mode.fairQueue && url.startsWith('https://postgrest.example.test/THROTTLE_PROTECTION')) {
          return jsonResponse([{
            STATE: 'closed',
            OPEN_UNTIL: null,
            OPEN_REASON: null,
            VERSION: 1,
            LAST_ERROR_CODE: null,
          }]);
        }
        if (!mode.fairQueue && url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
          return jsonResponse([{
            STATE: 'half_open',
            OPEN_UNTIL: null,
            OPEN_REASON: 'http_429',
            VERSION: 2,
            LAST_ERROR_CODE: 429,
            HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
            ATTEMPT_GRANTED: true,
            ATTEMPT_TICKET: 7,
          }]);
        }
        if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
          sampleBodies.push(JSON.parse(init.body));
          return jsonResponse([{
            STATE: 'closed',
            OPEN_UNTIL: null,
            OPEN_REASON: null,
            VERSION: 3,
            LAST_ERROR_CODE: null,
          }]);
        }
        if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
          settleBodies.push(JSON.parse(init.body));
          return jsonResponse([{
            STATE: 'closed',
            OPEN_UNTIL: null,
            OPEN_REASON: null,
            VERSION: 4,
            LAST_ERROR_CODE: null,
          }]);
        }
        if (url === 'https://www.googleapis.com/drive/v3/files/recovery-0?alt=media') {
          upstreamCalls += 1;
          return invalidCase.response();
        }
        throw new Error(`unexpected fetch URL: ${url}`);
      };

      try {
        const { ctx, waitUntilPromises } = createContext();
        const response = await worker.fetch(await buildRequest(`/downloads/${mode.name}-${invalidCase.name}.bin`), {
          CONTROLLER_URL: 'https://controller.example.test',
          CONTROLLER_API_TOKEN: 'controller-token',
          ENV: 'test',
          ROLE: 'download',
          INSTANCE_ID: 'worker-recovery',
          BOOTSTRAP_CACHE_MODE: 'direct',
        }, ctx);
        assert.equal(response.status, 502, `${mode.name}/${invalidCase.name}`);
        assert.equal((await response.json()).reason, invalidCase.reason, `${mode.name}/${invalidCase.name}`);
        await Promise.allSettled(waitUntilPromises);
        assert.equal(upstreamCalls, 1, `${mode.name}/${invalidCase.name}`);
        assert.equal(sampleBodies.some((body) => body.p_sample === 0), false, `${mode.name}/${invalidCase.name}`);
        assert.equal(sampleBodies.some((body) => body.p_sample === 1), false, `${mode.name}/${invalidCase.name}`);
        assert.equal(settleBodies.length, 1, `${mode.name}/${invalidCase.name}`);
        assert.equal(settleBodies[0].p_attempt_version, mode.fairQueue ? 1 : 2, `${mode.name}/${invalidCase.name}`);
        assert.equal(settleBodies[0].p_attempt_ticket, mode.fairQueue ? 1 : 7, `${mode.name}/${invalidCase.name}`);
        assert.equal(reportBodies.some((body) => body.feedback.outcome === 'success'), false, `${mode.name}/${invalidCase.name}`);
        assert.equal(reportBodies.some((body) => body.feedback.outcome === 'failure' && body.feedback.status_code === invalidCase.status), true, `${mode.name}/${invalidCase.name}`);
        assert.deepEqual(markCalls, [], `${mode.name}/${invalidCase.name}`);
        const finishBody = cache.calls.find(({ url, body }) => (
          url.endsWith('download_finish_cache_refresh') && body.p_link_data == null
        ))?.body;
        assert.equal(finishBody?.p_error_code, 502, `${mode.name}/${invalidCase.name}`);
        if (mode.fairQueue) {
          assert.equal(releaseBodies.length, 1, `${mode.name}/${invalidCase.name}`);
        }
      } finally {
        globalThis.fetch = originalFetch;
      }
    }
  }
});

test('Google If-Range fallback preserves a validated full-file 200 response', { concurrency: false }, async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  delete globalThis.bootstrapCache;
  const originRequests = [];
  const reportBodies = [];
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: linkFor(0) });
    }
    if (url === 'https://www.googleapis.com/drive/v3/files/recovery-0?alt=media') {
      originRequests.push({
        range: request.headers.get('range'),
        ifRange: request.headers.get('if-range'),
      });
      return new Response('hello', {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '5',
        },
      });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createContext();
    const response = await worker.fetch(await buildRequest('/downloads/google-if-range.bin', {
      headers: {
        range: 'bytes=0-1',
        'if-range': '"etag-google-1"',
      },
    }), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'hello');
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(originRequests, [{ range: 'bytes=0-1', ifRange: '"etag-google-1"' }]);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].feedback.outcome, 'success');
    assert.equal(reportBodies[0].feedback.status_code, 200);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('upstream request headers keep backend credentials same-origin and strip browser or internal secrets across redirects', { concurrency: false }, async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  delete globalThis.bootstrapCache;
  const originRequests = [];
  let redirectBodyCancelled = false;
  let sameOriginRedirectBodyCancelled = false;
  const link = {
    ...linkFor(0),
    url: 'https://source.example.test/start',
    header: {
      Authorization: 'Bearer backend-secret',
      Cookie: 'backend-cookie',
      Referer: 'https://source.example.test/backend-referrer',
      Origin: 'https://source.example.test/backend-origin',
      'X-Inner-Auth': 'backend-inner-secret',
      'X-OpenList-Transport': 'transport-secret',
    },
    download: { ...linkFor(0).download, provider: 'generic', report_success: false },
  };
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      return jsonResponse({ code: 200, data: link });
    }
    if (url === link.url) {
      originRequests.push({
        url,
        authorization: request.headers.get('authorization'),
        cookie: request.headers.get('cookie'),
        referer: request.headers.get('referer'),
        origin: request.headers.get('origin'),
        innerAuth: request.headers.get('x-inner-auth'),
        transport: request.headers.get('x-openlist-transport'),
        range: request.headers.get('range'),
        ifRange: request.headers.get('if-range'),
        ifNoneMatch: request.headers.get('if-none-match'),
      });
      return new Response(new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode('redirect-body'));
        },
        cancel() {
          redirectBodyCancelled = true;
        },
      }), {
        status: 302,
        headers: { location: 'https://source.example.test/same-origin-hop' },
      });
    }
    if (url === 'https://source.example.test/same-origin-hop') {
      originRequests.push({
        url,
        authorization: request.headers.get('authorization'),
        cookie: request.headers.get('cookie'),
        referer: request.headers.get('referer'),
        origin: request.headers.get('origin'),
        innerAuth: request.headers.get('x-inner-auth'),
        transport: request.headers.get('x-openlist-transport'),
        range: request.headers.get('range'),
        ifRange: request.headers.get('if-range'),
        ifNoneMatch: request.headers.get('if-none-match'),
      });
      return new Response(new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode('same-origin-redirect-body'));
        },
        cancel() {
          sameOriginRedirectBodyCancelled = true;
        },
      }), {
        status: 302,
        headers: { location: 'https://cdn.example.test/final' },
      });
    }
    if (url === 'https://cdn.example.test/final') {
      originRequests.push({
        url,
        authorization: request.headers.get('authorization'),
        cookie: request.headers.get('cookie'),
        referer: request.headers.get('referer'),
        origin: request.headers.get('origin'),
        innerAuth: request.headers.get('x-inner-auth'),
        transport: request.headers.get('x-openlist-transport'),
        range: request.headers.get('range'),
        ifRange: request.headers.get('if-range'),
        ifNoneMatch: request.headers.get('if-none-match'),
      });
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream', 'content-length': '2' },
      });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const baseRequest = await buildRequest('/downloads/header-boundaries.bin', {
      headers: {
        authorization: 'Bearer browser-secret',
        cookie: 'browser-cookie',
        'X-Inner-Auth': 'browser-inner-secret',
        range: 'bytes=0-1',
        'if-range': '"client-etag"',
        'if-none-match': '"client-none"',
      },
    });
    const { ctx, waitUntilPromises } = createContext();
    const response = await worker.fetch(baseRequest, {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'ok');
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(originRequests, [
      {
        url: 'https://source.example.test/start',
        authorization: 'Bearer backend-secret',
        cookie: 'backend-cookie',
        referer: 'https://source.example.test/backend-referrer',
        origin: 'https://source.example.test/backend-origin',
        innerAuth: null,
        transport: 'transport-secret',
        range: 'bytes=0-1',
        ifRange: '"client-etag"',
        ifNoneMatch: '"client-none"',
      },
      {
        url: 'https://source.example.test/same-origin-hop',
        authorization: 'Bearer backend-secret',
        cookie: 'backend-cookie',
        referer: 'https://source.example.test/backend-referrer',
        origin: 'https://source.example.test/backend-origin',
        innerAuth: null,
        transport: 'transport-secret',
        range: 'bytes=0-1',
        ifRange: '"client-etag"',
        ifNoneMatch: '"client-none"',
      },
      {
        url: 'https://cdn.example.test/final',
        authorization: null,
        cookie: null,
        referer: null,
        origin: null,
        innerAuth: null,
        transport: null,
        range: 'bytes=0-1',
        ifRange: '"client-etag"',
        ifNoneMatch: '"client-none"',
      },
    ]);
    assert.equal(redirectBodyCancelled, true);
    assert.equal(sameOriginRedirectBodyCancelled, true);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('HTTPS downgrade redirects remove origin-bound backend credentials', { concurrency: false }, async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  delete globalThis.bootstrapCache;
  const originRequests = [];
  const link = {
    ...linkFor(0),
    url: 'https://source.example.test/downgrade',
    header: { Authorization: 'Bearer downgrade-secret' },
    download: { ...linkFor(0).download, provider: 'generic', report_success: false },
  };
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      return jsonResponse({ code: 200, data: link });
    }
    if (url === link.url) {
      originRequests.push({ url, authorization: request.headers.get('authorization') });
      return new Response(null, { status: 302, headers: { location: 'http://source.example.test/downgrade-final' } });
    }
    if (url === 'http://source.example.test/downgrade-final') {
      originRequests.push({ url, authorization: request.headers.get('authorization') });
      return new Response('ok', { status: 200, headers: { 'content-length': '2' } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createContext();
    const response = await worker.fetch(await buildRequest('/downloads/https-downgrade.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'ok');
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(originRequests, [
      { url: 'https://source.example.test/downgrade', authorization: 'Bearer downgrade-secret' },
      { url: 'http://source.example.test/downgrade-final', authorization: null },
    ]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('conditional 304 passes through validators without ticket consumption or success reporting', { concurrency: false }, async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  delete globalThis.bootstrapCache;
  let conditionalBodyCancelled = false;
  const reportBodies = [];
  const markCalls = [];
  const link = {
    ...linkFor(0),
    url: 'https://files.example.test/conditional.bin',
    download: { ...linkFor(0).download, provider: 'generic', report_success: true },
  };
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url.endsWith('/rpc/download_mark_ticket_used')) {
      markCalls.push(JSON.parse(init.body));
    }
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: link });
    }
    if (url === link.url) {
      assert.equal(request.headers.get('if-none-match'), '"etag-conditional"');
      return {
        status: 304,
        statusText: 'Not Modified',
        headers: new Headers({ etag: '"etag-conditional"', 'last-modified': 'Wed, 21 Oct 2015 07:28:00 GMT' }),
        body: {
          async cancel() {
            conditionalBodyCancelled = true;
          },
        },
      };
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createContext();
    const response = await worker.fetch(await buildRequest('/downloads/conditional.bin', {
      headers: { 'if-none-match': '"etag-conditional"' },
    }), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    assert.equal(response.status, 304);
    assert.equal(await response.text(), '');
    assert.equal(response.headers.get('etag'), '"etag-conditional"');
    assert.equal(response.headers.get('last-modified'), 'Wed, 21 Oct 2015 07:28:00 GMT');
    await Promise.allSettled(waitUntilPromises);
    assert.equal(conditionalBodyCancelled, true);
    assert.deepEqual(markCalls, []);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].feedback.outcome, 'abandoned');
    assert.equal(reportBodies[0].feedback.status_code, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('conditional 304 settles breaker trials without a sample and reports neutral authorization feedback', { concurrency: false }, async () => {
  for (const mode of [
    { name: 'breaker_only', fairQueue: false, attemptVersion: 2, attemptTicket: 7 },
    { name: 'queue_breaker', fairQueue: true, attemptVersion: 1, attemptTicket: 1 },
  ]) {
    const cache = createCacheRpcFixture({ includeTicketState: true });
    const originalFetch = globalThis.fetch;
    delete globalThis.bootstrapCache;
    const settleBodies = [];
    const reportBodies = [];
    const markCalls = [];
    const releaseBodies = [];
    let conditionalBodyCancelled = false;
    const link = {
      ...linkFor(0),
      download: { ...linkFor(0).download, report_success: true },
    };
    globalThis.fetch = async (input, init = {}) => {
      const request = input instanceof Request ? input : new Request(input, init);
      const { url } = request;
      if (url.endsWith('/rpc/download_mark_ticket_used')) {
        markCalls.push(JSON.parse(init.body));
      }
      if (url === 'https://controller.example.test/api/v0/bootstrap') {
        return jsonResponse(buildBootstrap({ fairQueue: mode.fairQueue, throttle: true }));
      }
      const cacheResponse = await cache.handle(input, init);
      if (cacheResponse) {
        return cacheResponse;
      }
      if (url.startsWith('https://alist.example.com/api/fs/link')) {
        const body = JSON.parse(init.body);
        if (body.action === 'report') {
          reportBodies.push(body);
          return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
        }
        return jsonResponse({ code: 200, data: link });
      }
      if (mode.fairQueue && url === 'https://slot-handler.example.test/api/v1/fairqueue/wait') {
        return fairQueueGrantResponse(1);
      }
      if (mode.fairQueue && url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
        releaseBodies.push(JSON.parse(init.body));
        return jsonResponse({ result: 'ok' });
      }
      if (url.startsWith('https://postgrest.example.test/THROTTLE_PROTECTION')) {
        return jsonResponse([{
          STATE: 'closed',
          OPEN_UNTIL: null,
          OPEN_REASON: null,
          VERSION: 1,
          LAST_ERROR_CODE: null,
        }]);
      }
      if (!mode.fairQueue && url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
        return jsonResponse([{
          STATE: 'half_open',
          OPEN_UNTIL: null,
          OPEN_REASON: 'http_429',
          VERSION: mode.attemptVersion,
          LAST_ERROR_CODE: 429,
          HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
          ATTEMPT_GRANTED: true,
          ATTEMPT_TICKET: mode.attemptTicket,
        }]);
      }
      if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
        settleBodies.push(JSON.parse(init.body));
        return jsonResponse([{
          STATE: 'closed',
          OPEN_UNTIL: null,
          OPEN_REASON: null,
          VERSION: 3,
          LAST_ERROR_CODE: null,
        }]);
      }
      if (url === link.url) {
        assert.equal(request.headers.get('if-none-match'), '"etag-conditional"');
        return {
          status: 304,
          statusText: 'Not Modified',
          headers: new Headers({ etag: '"etag-conditional"' }),
          body: {
            async cancel() {
              conditionalBodyCancelled = true;
            },
          },
        };
      }
      throw new Error(`unexpected fetch URL: ${url}`);
    };

    try {
      const { ctx, waitUntilPromises } = createContext();
      const response = await worker.fetch(await buildRequest(`/downloads/${mode.name}-conditional.bin`, {
        headers: { 'if-none-match': '"etag-conditional"' },
      }), {
        CONTROLLER_URL: 'https://controller.example.test',
        CONTROLLER_API_TOKEN: 'controller-token',
        ENV: 'test',
        ROLE: 'download',
        INSTANCE_ID: 'worker-recovery',
        BOOTSTRAP_CACHE_MODE: 'direct',
      }, ctx);
      assert.equal(response.status, 304, mode.name);
      assert.equal(await response.text(), '', mode.name);
      await Promise.allSettled(waitUntilPromises);
      assert.equal(conditionalBodyCancelled, true, mode.name);
      assert.deepEqual(markCalls, [], mode.name);
      assert.equal(reportBodies.length, 1, mode.name);
      assert.equal(reportBodies[0].feedback.outcome, 'abandoned', mode.name);
      assert.equal(reportBodies[0].feedback.status_code, 0, mode.name);
      assert.equal(settleBodies.length, 1, mode.name);
      assert.equal(settleBodies[0].p_attempt_version, mode.attemptVersion, mode.name);
      assert.equal(settleBodies[0].p_attempt_ticket, mode.attemptTicket, mode.name);
      if (mode.fairQueue) {
        assert.equal(releaseBodies.length, 1, mode.name);
      }
    } finally {
      globalThis.fetch = originalFetch;
    }
  }
});

test('multi-range multipart responses pass through while cache and success bookkeeping stay conditional', { concurrency: false }, async () => {
  const multipartBody = [
    '--ranges',
    'Content-Range: bytes 0-0/5',
    '',
    'h',
    '--ranges',
    'Content-Range: bytes 2-2/5',
    '',
    'l',
    '--ranges--',
    '',
  ].join('\r\n');
  const scenarios = [
    { name: 'owner', warm: false },
    { name: 'owner-held-finish', warm: false, holdFinish: true },
    { name: 'cached', warm: true },
    { name: 'cached-google', warm: true, google: true },
    { name: 'trial-breaker', warm: false, trial: true, throttle: true },
    { name: 'trial-queue-breaker', warm: false, trial: true, throttle: true, fairQueue: true },
  ];

  for (const scenario of scenarios) {
    const cache = createCacheRpcFixture({ includeTicketState: true });
    const originalFetch = globalThis.fetch;
    const performanceDescriptor = Object.getOwnPropertyDescriptor(globalThis, 'performance');
    const originalSetTimeout = globalThis.setTimeout;
    const originalClearTimeout = globalThis.clearTimeout;
    delete globalThis.bootstrapCache;
    const reportBodies = [];
    const markCalls = [];
    const settleBodies = [];
    const originCalls = [];
    let acquireCalls = 0;
    let monotonicNow = 0;
    let finishStarted;
    const finishStartedPromise = new Promise((resolve) => { finishStarted = resolve; });
    let releaseFinish;
    const finishReleasePromise = new Promise((resolve) => { releaseFinish = resolve; });
    let ownerTimerHandle = null;
    let ownerTimerCallback = null;
    if (scenario.holdFinish) {
      Object.defineProperty(globalThis, 'performance', {
        configurable: true,
        value: { now: () => monotonicNow },
      });
      globalThis.setTimeout = (callback, delay, ...args) => {
        if (!ownerTimerHandle && Number(delay) >= 149_000 && Number(delay) <= 151_000) {
          ownerTimerHandle = { ownerTimer: true, cleared: false };
          ownerTimerCallback = () => {
            if (!ownerTimerHandle.cleared) {
              callback(...args);
            }
          };
          return ownerTimerHandle;
        }
        return originalSetTimeout(callback, delay, ...args);
      };
      globalThis.clearTimeout = (handle) => {
        if (handle?.ownerTimer) {
          handle.cleared = true;
          return;
        }
        return originalClearTimeout(handle);
      };
      cache.setFinishHandler(async ({ body }) => {
        if (body.p_link_data === null && body.p_error_code === 206) {
          finishStarted();
          await finishReleasePromise;
        }
        return undefined;
      });
    }
    const link = scenario.google || scenario.trial
      ? {
        ...linkFor(0),
        url: `https://www.googleapis.com/drive/v3/files/multipart-${scenario.name}.bin?alt=media`,
        download: { ...linkFor(0).download, report_success: scenario.trial === true },
      }
      : {
        ...linkFor(0),
        url: `https://files.example.test/multipart-${scenario.name}.bin`,
        download: { ...linkFor(0).download, provider: 'generic', report_success: true },
      };
    let phase = scenario.warm ? 'warm' : 'multipart';
    let originSignal = null;
    globalThis.fetch = async (input, init = {}) => {
      const request = input instanceof Request ? input : new Request(input, init);
      const { url } = request;
      if (url.endsWith('/rpc/download_mark_ticket_used')) {
        markCalls.push(JSON.parse(init.body));
      }
      if (url === 'https://controller.example.test/api/v0/bootstrap') {
        return jsonResponse(buildBootstrap({ fairQueue: scenario.fairQueue, throttle: scenario.throttle }));
      }
      const cacheResponse = await cache.handle(input, init);
      if (cacheResponse) {
        return cacheResponse;
      }
      if (url.startsWith('https://alist.example.com/api/fs/link')) {
        const body = JSON.parse(init.body);
        if (body.action === 'report') {
          reportBodies.push(body);
          return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
        }
        acquireCalls += 1;
        return jsonResponse({ code: 200, data: link });
      }
      if (scenario.trial && scenario.fairQueue && url === 'https://slot-handler.example.test/api/v1/fairqueue/wait') {
        return fairQueueGrantResponse(1);
      }
      if (scenario.trial && scenario.fairQueue && url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
        return jsonResponse({ result: 'ok' });
      }
      if (scenario.trial && scenario.throttle && !scenario.fairQueue && url.startsWith('https://postgrest.example.test/THROTTLE_PROTECTION')) {
        return jsonResponse([{
          STATE: 'closed',
          OPEN_UNTIL: null,
          OPEN_REASON: null,
          VERSION: 1,
          LAST_ERROR_CODE: null,
        }]);
      }
      if (scenario.trial && !scenario.fairQueue && url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
        return jsonResponse([{
          STATE: 'half_open',
          OPEN_UNTIL: null,
          OPEN_REASON: 'http_429',
          VERSION: 2,
          LAST_ERROR_CODE: 429,
          HALF_OPEN_DEADLINE: Math.floor(Date.now() / 1000) + 15,
          ATTEMPT_GRANTED: true,
          ATTEMPT_TICKET: 7,
        }]);
      }
      if (scenario.trial && url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
        settleBodies.push(JSON.parse(init.body));
        return jsonResponse([{
          STATE: 'closed',
          OPEN_UNTIL: null,
          OPEN_REASON: null,
          VERSION: 3,
          LAST_ERROR_CODE: null,
        }]);
      }
      if (scenario.trial && url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
        throw new Error('multipart trial must settle without a breaker sample');
      }
      if (url === link.url) {
        originSignal = init.signal;
        originCalls.push({ phase, range: request.headers.get('range') });
        if (phase === 'warm') {
          return new Response('warm!', { status: 200, headers: { 'content-length': '5' } });
        }
        return new Response(multipartBody, {
          status: 206,
          headers: {
            'content-type': 'multipart/byteranges; boundary=ranges',
            'content-length': String(new TextEncoder().encode(multipartBody).byteLength),
          },
        });
      }
      throw new Error(`unexpected fetch URL: ${url}`);
    };

    try {
      const env = {
        CONTROLLER_URL: 'https://controller.example.test',
        CONTROLLER_API_TOKEN: 'controller-token',
        ENV: 'test',
        ROLE: 'download',
        INSTANCE_ID: 'worker-recovery',
        BOOTSTRAP_CACHE_MODE: 'direct',
      };
      if (scenario.warm) {
        const warmContext = createContext();
        const warmResponse = await worker.fetch(await buildRequest(`/downloads/multipart-${scenario.name}.bin`), env, warmContext.ctx);
        assert.equal(warmResponse.status, 200);
        assert.equal(await warmResponse.text(), 'warm!');
        await Promise.allSettled(warmContext.waitUntilPromises);
        reportBodies.length = 0;
        markCalls.length = 0;
        phase = 'multipart';
      }

      const multipartContext = createContext();
      const multipartRequestPromise = worker.fetch(await buildRequest(`/downloads/multipart-${scenario.name}.bin`, {
        headers: { range: 'bytes=0-0,2-2' },
      }), env, multipartContext.ctx);
      let multipartResponse;
      if (scenario.holdFinish) {
        await finishStartedPromise;
        assert.equal(typeof ownerTimerCallback, 'function');
        assert.equal(ownerTimerHandle?.cleared, true);
        ownerTimerCallback();
        assert.equal(originSignal?.aborted, false);
        monotonicNow = 160_000;
        releaseFinish();
      }
      multipartResponse = await multipartRequestPromise;
      assert.equal(multipartResponse.status, 206);
      assert.equal(multipartResponse.headers.get('content-type'), 'multipart/byteranges; boundary=ranges');
      assert.equal(await multipartResponse.text(), multipartBody);
      await Promise.allSettled(multipartContext.waitUntilPromises);
      if (scenario.google) {
        assert.deepEqual(reportBodies, []);
      } else {
        assert.equal(reportBodies.length, 1);
        assert.equal(reportBodies[0].feedback.outcome, 'abandoned');
        assert.equal(reportBodies[0].feedback.status_code, 0);
      }
      assert.equal(markCalls.length, 1);
      assert.equal(typeof markCalls[0].p_ticket_hash, 'string');
      assert.equal(originCalls.at(-1)?.range, 'bytes=0-0,2-2');
      if (scenario.trial) {
        assert.equal(settleBodies.length, 1);
        assert.equal(settleBodies[0].p_attempt_version, scenario.fairQueue ? 1 : 2);
        assert.equal(settleBodies[0].p_attempt_ticket, scenario.fairQueue ? 1 : 7);
      }
      if (scenario.warm) {
        assert.equal(acquireCalls, 1);
        assert.equal(cache.calls.filter(({ url }) => url.endsWith('download_finish_cache_refresh')).length, 1);
        phase = 'warm';
        const healthyContext = createContext();
        const healthyResponse = await worker.fetch(await buildRequest(`/downloads/multipart-${scenario.name}.bin`), env, healthyContext.ctx);
        assert.equal(healthyResponse.status, 200);
        assert.equal(await healthyResponse.text(), 'warm!');
        await Promise.allSettled(healthyContext.waitUntilPromises);
        assert.equal(acquireCalls, 1);
      } else {
        const failedFinish = cache.calls.find(({ url, body }) => (
          url.endsWith('download_finish_cache_refresh') && body.p_link_data == null
        ));
        assert.equal(failedFinish?.body.p_error_code, 206);
      }
    } finally {
      globalThis.fetch = originalFetch;
      globalThis.setTimeout = originalSetTimeout;
      globalThis.clearTimeout = originalClearTimeout;
      if (performanceDescriptor) {
        Object.defineProperty(globalThis, 'performance', performanceDescriptor);
      }
    }
  }
});

test('client abort during a stalled breaker snapshot keeps terminal cleanup independent', { concurrency: false }, async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  delete globalThis.bootstrapCache;
  const abortController = new AbortController();
  const reportBodies = [];
  let snapshotStarted;
  const snapshotStartedPromise = new Promise((resolve) => { snapshotStarted = resolve; });
  let snapshotSignal = null;
  let snapshotBodyCancelled = false;
  let finishSignal = null;
  let reportSignal = null;
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap({ throttle: true }));
    }
    if (url.endsWith('download_finish_cache_refresh')) {
      finishSignal = init.signal;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportSignal = init.signal;
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      return jsonResponse({ code: 200, data: linkFor(0) });
    }
    if (url.startsWith('https://postgrest.example.test/THROTTLE_PROTECTION')) {
      snapshotSignal = init.signal;
      snapshotStarted();
      return new Response(new ReadableStream({
        start() {},
        cancel() {
          snapshotBodyCancelled = true;
        },
      }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const baseRequest = await buildRequest('/downloads/stalled-breaker-snapshot.bin');
    const request = new Request(baseRequest.url, {
      headers: baseRequest.headers,
      signal: abortController.signal,
    });
    const ctx = {};
    const workerPromise = worker.fetch(request, {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    await snapshotStartedPromise;
    assert.notEqual(snapshotSignal, abortController.signal);
    abortController.abort(new DOMException('client canceled snapshot', 'AbortError'));
    const response = await workerPromise;
    assert.equal(response.status, 499);
    assert.equal((await response.json()).reason, 'client_aborted');
    assert.equal(snapshotBodyCancelled, true);
    assert.notEqual(finishSignal, abortController.signal);
    assert.equal(finishSignal?.aborted, false);
    assert.notEqual(reportSignal, abortController.signal);
    assert.equal(reportSignal?.aborted, false);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].feedback.outcome, 'abandoned');
    assert.equal(reportBodies[0].feedback.status_code, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('client abort during breaker settlement keeps a bounded live cleanup signal', { concurrency: false }, async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  delete globalThis.bootstrapCache;
  const abortController = new AbortController();
  const reportBodies = [];
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  let cleanupTimerCallback = null;
  globalThis.setTimeout = (callback, delay, ...args) => {
    if (!cleanupTimerCallback && Number(delay) >= 1_900 && Number(delay) <= 2_100) {
      const handle = { fakeCleanupTimer: true, cleared: false };
      cleanupTimerCallback = () => {
        if (!handle.cleared) {
          callback(...args);
        }
      };
      return handle;
    }
    return originalSetTimeout(callback, delay, ...args);
  };
  globalThis.clearTimeout = (handle) => {
    if (handle?.fakeCleanupTimer) {
      handle.cleared = true;
      return;
    }
    return originalClearTimeout(handle);
  };
  let settleStarted;
  const settleStartedPromise = new Promise((resolve) => { settleStarted = resolve; });
  let settleSignal = null;
  let settleBodyCancelled = false;
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      const bootstrap = buildBootstrap({ throttle: true });
      bootstrap.download.throttleProfiles.default.protectHttpCodes = [403, 410, 429, 500, 502, 503, 504];
      return jsonResponse(bootstrap);
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: linkFor(0) });
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://postgrest.example.test/THROTTLE_PROTECTION')) {
      return jsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
      }]);
    }
    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      return jsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 2,
        LAST_ERROR_CODE: null,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 7,
      }]);
    }
    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      return jsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 3,
        LAST_ERROR_CODE: 401,
      }]);
    }
    if (url === 'https://www.googleapis.com/drive/v3/files/recovery-0?alt=media') {
      return new Response(JSON.stringify({ error: { errors: [{ reason: 'authError' }] } }), {
        status: 401,
        headers: { 'content-type': 'application/json' },
      });
    }
    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      settleSignal = init.signal;
      settleStarted();
      return new Response(new ReadableStream({
        start() {},
        cancel() {
          settleBodyCancelled = true;
        },
      }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const baseRequest = await buildRequest('/downloads/stalled-breaker-settlement.bin');
    const request = new Request(baseRequest.url, {
      headers: baseRequest.headers,
      signal: abortController.signal,
    });
    const ctx = {};
    const workerPromise = worker.fetch(request, {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    await settleStartedPromise;
    assert.equal(settleSignal?.aborted, false);
    assert.notEqual(settleSignal, abortController.signal);
    abortController.abort(new DOMException('client canceled settlement', 'AbortError'));
    assert.equal(typeof cleanupTimerCallback, 'function');
    cleanupTimerCallback();
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
    const response = await workerPromise;
    assert.equal(response.status, 503);
    assert.equal(settleSignal?.aborted, true);
    assert.equal(settleBodyCancelled, true);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].feedback.outcome, 'abandoned');
    assert.equal(reportBodies[0].feedback.status_code, 0);
  } finally {
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
    globalThis.fetch = originalFetch;
  }
});

test('generic HEAD metadata with report_success does not emit a content-success report', { concurrency: false }, async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  delete globalThis.bootstrapCache;
  const reportBodies = [];
  const markCalls = [];
  const genericLink = {
    ...linkFor(0),
    url: 'https://files.example.test/reportable-head.bin',
    download: { ...linkFor(0).download, provider: 'generic', report_success: true },
  };
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url.endsWith('/rpc/download_mark_ticket_used')) {
      markCalls.push(JSON.parse(init.body));
    }
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: genericLink });
    }
    if (url === genericLink.url) {
      assert.equal(request.method, 'HEAD');
      return new Response(null, {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '123',
          'accept-ranges': 'bytes',
        },
      });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createContext();
    const response = await worker.fetch(await buildRequest('/downloads/generic-head-report.bin', { method: 'HEAD' }), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    assert.equal(response.status, 200);
    assert.equal(response.headers.get('content-length'), '123');
    await Promise.allSettled(waitUntilPromises);
    assert.deepEqual(reportBodies, []);
    assert.deepEqual(markCalls, []);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('cached generic non-owner transport has no report while cached Google terminal reports actual status', { concurrency: false }, async () => {
  const scenarios = [
    {
      name: 'generic',
      link: {
        ...linkFor(0),
        url: 'https://files.example.test/cached-generic.bin',
        download: { ...linkFor(0).download, provider: 'generic', report_success: false },
      },
      terminal: () => new TypeError('cached generic transport failure'),
      status: 503,
      reason: 'upstream_transport_error',
      expectReport: false,
    },
    {
      name: 'google',
      link: { ...linkFor(0), download: { ...linkFor(0).download, report_success: false } },
      terminal: () => new Response('gone', { status: 404, headers: { 'content-type': 'text/plain' } }),
      status: 404,
      reason: 'upstream_rejected',
      expectReport: true,
    },
  ];

  for (const scenario of scenarios) {
    const cache = createCacheRpcFixture({ includeTicketState: true });
    const originalFetch = globalThis.fetch;
    delete globalThis.bootstrapCache;
    const reportBodies = [];
    let phase = 'warm';
    globalThis.fetch = async (input, init = {}) => {
      const request = input instanceof Request ? input : new Request(input, init);
      const { url } = request;
      if (url === 'https://controller.example.test/api/v0/bootstrap') {
        return jsonResponse(buildBootstrap());
      }
      const cacheResponse = await cache.handle(input, init);
      if (cacheResponse) {
        return cacheResponse;
      }
      if (url.startsWith('https://alist.example.com/api/fs/link')) {
        const body = JSON.parse(init.body);
        if (body.action === 'report') {
          reportBodies.push(body);
          return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
        }
        return jsonResponse({ code: 200, data: scenario.link });
      }
      if (url === scenario.link.url) {
        if (phase === 'warm') {
          return new Response('warm!', { status: 200, headers: { 'content-length': '5' } });
        }
        return scenario.terminal();
      }
      throw new Error(`unexpected fetch URL: ${url}`);
    };

    try {
      const env = {
        CONTROLLER_URL: 'https://controller.example.test',
        CONTROLLER_API_TOKEN: 'controller-token',
        ENV: 'test',
        ROLE: 'download',
        INSTANCE_ID: 'worker-recovery',
        BOOTSTRAP_CACHE_MODE: 'direct',
      };
      const warmContext = createContext();
      const warmResponse = await worker.fetch(await buildRequest(`/downloads/cached-${scenario.name}.bin`), env, warmContext.ctx);
      assert.equal(warmResponse.status, 200);
      await warmResponse.text();
      await Promise.allSettled(warmContext.waitUntilPromises);
      phase = 'terminal';
      const terminalContext = createContext();
      const response = await worker.fetch(await buildRequest(`/downloads/cached-${scenario.name}.bin`), env, terminalContext.ctx);
      assert.equal(response.status, scenario.status);
      assert.equal((await response.json()).reason, scenario.reason);
      await Promise.allSettled(terminalContext.waitUntilPromises);
      if (scenario.expectReport) {
        assert.equal(reportBodies.length, 1);
        assert.equal(reportBodies[0].feedback.outcome, 'failure');
        assert.equal(reportBodies[0].feedback.status_code, 404);
        assert.equal(reportBodies[0].feedback.reason, 'upstream_http_404');
      } else {
        assert.deepEqual(reportBodies, []);
      }
    } finally {
      globalThis.fetch = originalFetch;
    }
  }
});

test('fourth authorization may redirect internally without opening a fifth ticket', { concurrency: false }, async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  delete globalThis.bootstrapCache;
  const innerRequest = await buildRequest('/downloads/fourth-internal-redirect-inner.bin');
  const acquireBodies = [];
  const originUrls = [];
  const reportBodies = [];
  let linkCalls = 0;
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      acquireBodies.push(body);
      linkCalls += 1;
      return jsonResponse({ code: 200, data: linkFor(linkCalls - 1) });
    }
    if (/recovery-[012]\?alt=media$/.test(url)) {
      originUrls.push(url);
      return jsonResponse({ error: { errors: [{ reason: 'downloadQuotaExceeded' }] } }, { status: 403 });
    }
    if (url.endsWith('/recovery-3?alt=media')) {
      originUrls.push(url);
      return new Response(null, { status: 302, headers: { Location: innerRequest.url } });
    }
    if (url.endsWith('/recovery-4?alt=media')) {
      throw new Error('fifth internal-redirect ticket must not be fetched');
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createContext();
    const response = await worker.fetch(await buildRequest('/downloads/fourth-internal-redirect.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    assert.equal(response.status, 503);
    assert.equal((await response.json()).reason, 'upstream_auth_retry_exhausted');
    await Promise.allSettled(waitUntilPromises);
    assert.equal(acquireBodies.length, 4);
    assert.equal(originUrls.length, 4);
    assert.equal(originUrls.some((url) => url.endsWith('/recovery-4?alt=media')), false);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].feedback.ticket, linkFor(3).download.ticket);
    assert.equal(reportBodies[0].feedback.outcome, 'abandoned');
    assert.equal(reportBodies[0].feedback.status_code, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('recursive cached child uses the inherited deadline for a stalled origin and reports failure status0', { concurrency: false }, async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true, acquireLeaseDurationSeconds: 31.5 });
  const originalFetch = globalThis.fetch;
  delete globalThis.bootstrapCache;
  const childRequest = await buildRequest('/downloads/inherited-cached-child.bin');
  const parentLink = {
    ...linkFor(0),
    url: 'https://files.example.test/inherited-parent.bin',
    download: { ...linkFor(0).download, provider: 'generic', report_success: false },
  };
  const childLink = {
    ...linkFor(1),
    url: 'https://files.example.test/inherited-cached-child.bin',
    download: { ...linkFor(1).download, provider: 'generic', report_success: true },
  };
  let phase = 'warm-child';
  const reportBodies = [];
  let childOriginSignal = null;
  let childOriginStarted;
  const childOriginStartedPromise = new Promise((resolve) => { childOriginStarted = resolve; });
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: body.path.includes('inherited-cached-child') ? childLink : parentLink });
    }
    if (url === parentLink.url) {
      return new Response(null, { status: 302, headers: { location: childRequest.url } });
    }
    if (url === childLink.url) {
      if (phase === 'warm-child') {
        return new Response('warm!', { status: 200, headers: { 'content-length': '5' } });
      }
      childOriginSignal = init.signal;
      childOriginStarted();
      return await new Promise((_, reject) => {
        init.signal?.addEventListener?.('abort', () => reject(init.signal.reason || new DOMException('child deadline', 'AbortError')), { once: true });
      });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const env = {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    };
    const warmContext = createContext();
    const warmResponse = await worker.fetch(childRequest, env, warmContext.ctx);
    assert.equal(warmResponse.status, 200);
    assert.equal(await warmResponse.text(), 'warm!');
    await Promise.allSettled(warmContext.waitUntilPromises);
    reportBodies.length = 0;
    phase = 'parent-redirect';
    const parentContext = createContext();
    const parentResponsePromise = worker.fetch(await buildRequest('/downloads/inherited-parent.bin'), env, parentContext.ctx);
    await childOriginStartedPromise;
    assert.equal(childOriginSignal?.aborted, false);
    await new Promise((resolve) => setTimeout(resolve, 1_700));
    const response = await parentResponsePromise;
    assert.equal(response.status, 503);
    assert.equal((await response.json()).reason, 'recovery_deadline_exhausted');
    await Promise.allSettled(parentContext.waitUntilPromises);
    assert.equal(childOriginSignal?.aborted, true);
    assert.equal(reportBodies.length, 2);
    assert.equal(reportBodies[0].feedback.ticket, parentLink.download.ticket);
    assert.equal(reportBodies[0].feedback.outcome, 'abandoned');
    assert.equal(reportBodies[0].feedback.status_code, 0);
    assert.equal(reportBodies[1].feedback.ticket, childLink.download.ticket);
    assert.equal(reportBodies[1].feedback.outcome, 'failure');
    assert.equal(reportBodies[1].feedback.status_code, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('recursive child aborts a stalled initial cache RPC at the inherited deadline before link acquisition', { concurrency: false }, async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true, acquireLeaseDurationSeconds: 30.15 });
  const originalFetch = globalThis.fetch;
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  const performanceDescriptor = Object.getOwnPropertyDescriptor(globalThis, 'performance');
  let monotonicNow = 0;
  let inheritedDeadlineHandle = null;
  let inheritedDeadlineCallback = null;
  const deadlineHandles = [];
  Object.defineProperty(globalThis, 'performance', {
    configurable: true,
    value: { now: () => monotonicNow },
  });
  globalThis.setTimeout = (callback, delay, ...args) => {
    if (Number(delay) >= 100 && Number(delay) <= 200) {
      const callbackSource = String(callback);
      const kind = callbackSource.includes('cacheOwnerAbortController') ? 'owner' : 'inherited';
      const handle = { kind, cleared: false };
      handle.callback = () => {
        if (!handle.cleared) {
          callback(...args);
        }
      };
      deadlineHandles.push(handle);
      if (kind === 'inherited' && !inheritedDeadlineHandle) {
        inheritedDeadlineHandle = handle;
        inheritedDeadlineCallback = handle.callback;
      }
      return handle;
    }
    return originalSetTimeout(callback, delay, ...args);
  };
  globalThis.clearTimeout = (handle) => {
    if (deadlineHandles.includes(handle)) {
      handle.cleared = true;
      return;
    }
    return originalClearTimeout(handle);
  };
  delete globalThis.bootstrapCache;
  const childRequest = await buildRequest('/downloads/inherited-cache-rpc-child.bin');
  const parentLink = {
    ...linkFor(0),
    url: 'https://files.example.test/inherited-cache-rpc-parent.bin',
    download: { ...linkFor(0).download, provider: 'generic', report_success: true },
  };
  const reportBodies = [];
  let cacheStateCalls = 0;
  let cacheStateSignal = null;
  let cacheStateStarted;
  const cacheStateStartedPromise = new Promise((resolve) => { cacheStateStarted = resolve; });
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    if (url.endsWith('/rpc/download_get_cache_state')) {
      cacheStateCalls += 1;
      if (cacheStateCalls === 2) {
        cacheStateSignal = init.signal;
        cacheStateStarted();
        return new Response(new ReadableStream({
          start() {},
          cancel() {},
        }), { status: 200, headers: { 'content-type': 'application/json' } });
      }
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: parentLink });
    }
    if (url === parentLink.url) {
      return new Response(null, { status: 302, headers: { location: childRequest.url } });
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const env = {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    };
    const context = createContext();
    const responsePromise = worker.fetch(await buildRequest('/downloads/inherited-cache-rpc-parent.bin'), env, context.ctx);
    await cacheStateStartedPromise;
    assert.equal(cacheStateSignal?.aborted, false);
    assert.equal(typeof inheritedDeadlineCallback, 'function');
    monotonicNow = 149.9;
    inheritedDeadlineCallback();
    const response = await responsePromise;
    assert.equal(response.status, 503);
    assert.equal((await response.json()).reason, 'recovery_deadline_exhausted');
    await Promise.allSettled(context.waitUntilPromises);
    assert.equal(cacheStateCalls, 2);
    assert.equal(cacheStateSignal?.aborted, true);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].feedback.ticket, parentLink.download.ticket);
    assert.equal(reportBodies[0].feedback.outcome, 'abandoned');
    assert.equal(reportBodies[0].feedback.status_code, 0);
  } finally {
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
    if (performanceDescriptor) {
      Object.defineProperty(globalThis, 'performance', performanceDescriptor);
    }
    globalThis.fetch = originalFetch;
  }
});

test('rate-enabled recursive cached child carries the inherited deadline through unified cache lookup and origin', { concurrency: false }, async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true, acquireLeaseDurationSeconds: 30.15 });
  const originalFetch = globalThis.fetch;
  delete globalThis.bootstrapCache;
  const childRequest = await buildRequest('/downloads/inherited-unified-child.bin');
  const parentLink = {
    ...linkFor(0),
    url: 'https://files.example.test/inherited-unified-parent.bin',
    download: { ...linkFor(0).download, provider: 'generic', report_success: true },
  };
  const childLink = {
    ...linkFor(1),
    url: 'https://files.example.test/inherited-unified-child.bin',
    download: { ...linkFor(1).download, provider: 'generic', report_success: true },
  };
  const reportBodies = [];
  let unifiedCalls = 0;
  let unifiedSignal = null;
  let childOriginSignal = null;
  let childOriginStarted;
  const childOriginStartedPromise = new Promise((resolve) => { childOriginStarted = resolve; });
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      const bootstrap = buildBootstrap();
      bootstrap.download.db.rateLimit = { enabled: true, windowSeconds: 60, limit: 10 };
      return jsonResponse(bootstrap);
    }
    if (url === 'https://postgrest.example.test/rpc/download_unified_check') {
      unifiedCalls += 1;
      unifiedSignal = init.signal;
      const row = {
        cache_link_data: unifiedCalls === 2 ? JSON.stringify(childLink) : null,
        cache_timestamp: unifiedCalls === 2 ? Math.floor(Date.now() / 1000) : null,
        cache_hostname_hash: unifiedCalls === 2 ? 'cached-unified-host' : null,
        cache_version: unifiedCalls === 2 ? '11111111-1111-4111-8111-000000000099' : null,
        cache_observed_at: new Date().toISOString(),
        rate_access_count: 0,
        rate_last_window_time: Math.floor(Date.now() / 1000),
        rate_block_until: null,
        throttle_record_exists: false,
        throttle_state: null,
        throttle_open_until: null,
        throttle_reason: null,
        throttle_version: null,
        throttle_last_error_code: null,
      };
      return jsonResponse([row]);
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: parentLink });
    }
    if (url === parentLink.url) {
      return new Response(null, { status: 302, headers: { location: childRequest.url } });
    }
    if (url === childLink.url) {
      childOriginSignal = init.signal;
      childOriginStarted();
      return await new Promise((_, reject) => {
        init.signal?.addEventListener?.('abort', () => reject(init.signal.reason || new DOMException('unified child deadline', 'AbortError')), { once: true });
      });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const env = {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    };
    const context = createContext();
    const responsePromise = worker.fetch(await buildRequest('/downloads/inherited-unified-parent.bin'), env, context.ctx);
    await childOriginStartedPromise;
    assert.equal(unifiedCalls, 2);
    assert.equal(unifiedSignal?.aborted, false);
    assert.equal(childOriginSignal, unifiedSignal);
    await new Promise((resolve) => setTimeout(resolve, 400));
    const response = await responsePromise;
    assert.equal(response.status, 503);
    assert.equal((await response.json()).reason, 'recovery_deadline_exhausted');
    await Promise.allSettled(context.waitUntilPromises);
    assert.equal(unifiedSignal?.aborted, true);
    assert.equal(reportBodies.length, 2);
    assert.equal(reportBodies[0].feedback.ticket, parentLink.download.ticket);
    assert.equal(reportBodies[0].feedback.outcome, 'abandoned');
    assert.equal(reportBodies[0].feedback.status_code, 0);
    assert.equal(reportBodies[1].feedback.ticket, childLink.download.ticket);
    assert.equal(reportBodies[1].feedback.outcome, 'failure');
    assert.equal(reportBodies[1].feedback.status_code, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('recursive child clears its opening deadline before a valid delayed stream body', { concurrency: false }, async () => {
  const cache = createCacheRpcFixture({ includeTicketState: true, acquireLeaseDurationSeconds: 30.15 });
  const originalFetch = globalThis.fetch;
  delete globalThis.bootstrapCache;
  const childRequest = await buildRequest('/downloads/inherited-stream-child.bin');
  const parentLink = {
    ...linkFor(0),
    url: 'https://files.example.test/inherited-stream-parent.bin',
    download: { ...linkFor(0).download, provider: 'generic', report_success: true },
  };
  const childLink = {
    ...linkFor(1),
    url: 'https://files.example.test/inherited-stream-child.bin',
    download: { ...linkFor(1).download, provider: 'generic', report_success: true },
  };
  const reportBodies = [];
  let childOriginSignal = null;
  let childLaterChunk;
  const childLaterChunkPromise = new Promise((resolve) => { childLaterChunk = resolve; });
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: body.path.includes('inherited-stream-child') ? childLink : parentLink });
    }
    if (url === parentLink.url) {
      return new Response(null, { status: 302, headers: { location: childRequest.url } });
    }
    if (url === childLink.url) {
      childOriginSignal = init.signal;
      return new Response(new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode('he'));
          setTimeout(() => {
            try {
              controller.enqueue(new TextEncoder().encode('llo'));
              controller.close();
            } finally {
              childLaterChunk();
            }
          }, 250);
        },
      }), {
        status: 200,
        headers: { 'content-type': 'application/octet-stream', 'content-length': '5' },
      });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const env = {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    };
    const context = createContext();
    const responsePromise = worker.fetch(await buildRequest('/downloads/inherited-stream-parent.bin'), env, context.ctx);
    const response = await responsePromise;
    assert.equal(response.status, 200);
    const reader = response.body.getReader();
    assert.equal(new TextDecoder().decode((await reader.read()).value), 'he');
    await childLaterChunkPromise;
    assert.equal(childOriginSignal?.aborted, false);
    assert.equal(new TextDecoder().decode((await reader.read()).value), 'llo');
    await reader.cancel();
    await Promise.allSettled(context.waitUntilPromises);
    assert.equal(reportBodies.length, 2);
    assert.equal(reportBodies[0].feedback.ticket, parentLink.download.ticket);
    assert.equal(reportBodies[0].feedback.outcome, 'abandoned');
    assert.equal(reportBodies[0].feedback.status_code, 0);
    assert.equal(reportBodies[1].feedback.ticket, childLink.download.ticket);
    assert.equal(reportBodies[1].feedback.outcome, 'success');
    assert.equal(reportBodies[1].feedback.status_code, 200);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('unified cached breaker decisions wait for issued state and preserve eligibility on cancellation', { concurrency: false }, async () => {
  delete globalThis.bootstrapCache;
  for (const scenario of [
    {
      name: 'google-reportable',
      link: { ...linkFor(0), download: { ...linkFor(0).download, report_success: true } },
      expectReport: true,
      abortDuringDelay: false,
    },
    {
      name: 'generic-cached',
      link: {
        ...linkFor(0),
        url: 'https://files.example.test/unified-generic-cached.bin',
        download: { ...linkFor(0).download, provider: 'generic', report_success: false },
      },
      expectReport: false,
      abortDuringDelay: false,
    },
    {
      name: 'google-cancelled',
      link: { ...linkFor(0), download: { ...linkFor(0).download, report_success: true } },
      expectReport: true,
      abortDuringDelay: true,
    },
  ]) {
    const cache = createCacheRpcFixture({ includeTicketState: true });
    const originalFetch = globalThis.fetch;
    const originalSetTimeout = globalThis.setTimeout;
    const originalClearTimeout = globalThis.clearTimeout;
    const originalNodeTestContext = process.env.NODE_TEST_CONTEXT;
    const clientController = new AbortController();
    const reportBodies = [];
    let delayStarted;
    const delayStartedPromise = new Promise((resolve) => { delayStarted = resolve; });
    let delayCallback = null;
    let delayHandle = null;
    globalThis.setTimeout = (callback, delay, ...args) => {
      if (!delayHandle && Number(delay) === 5_000) {
        delayHandle = { cleared: false };
        delayCallback = () => {
          if (!delayHandle.cleared) {
            callback(...args);
          }
        };
        delayStarted();
        return delayHandle;
      }
      return originalSetTimeout(callback, delay, ...args);
    };
    globalThis.clearTimeout = (handle) => {
      if (handle === delayHandle) {
        handle.cleared = true;
        return;
      }
      return originalClearTimeout(handle);
    };
    delete process.env.NODE_TEST_CONTEXT;
    globalThis.fetch = async (input, init = {}) => {
      const request = input instanceof Request ? input : new Request(input, init);
      const { url } = request;
      if (url === 'https://controller.example.test/api/v0/bootstrap') {
        const bootstrap = buildBootstrap({ throttle: true });
        bootstrap.download.throttleProfiles.default.hostPatterns = ['*.googleapis.com', 'files.example.test'];
        bootstrap.download.db.rateLimit = { enabled: true, windowSeconds: 60, limit: 10 };
        return jsonResponse(bootstrap);
      }
      if (url === 'https://postgrest.example.test/rpc/download_unified_check') {
        return jsonResponse([{
          cache_link_data: JSON.stringify(scenario.link),
          cache_timestamp: Math.floor(Date.now() / 1000),
          cache_hostname_hash: 'unified-cached-host',
          cache_version: '11111111-1111-4111-8111-000000000099',
          cache_observed_at: new Date().toISOString(),
          rate_access_count: 0,
          rate_last_window_time: Math.floor(Date.now() / 1000),
          rate_block_until: null,
          throttle_record_exists: true,
          throttle_state: 'open',
          throttle_open_until: Math.floor(Date.now() / 1000) + 30,
          throttle_reason: 'http_429',
          throttle_version: 9,
          throttle_last_error_code: 429,
        }]);
      }
      const cacheResponse = await cache.handle(input, init);
      if (cacheResponse) {
        return cacheResponse;
      }
      if (url.startsWith('https://alist.example.com/api/fs/link')) {
        const body = JSON.parse(init.body);
        if (body.action === 'report') {
          reportBodies.push(body);
          return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
        }
        throw new Error('unified cache adoption must not acquire a second link');
      }
      throw new Error(`unexpected fetch URL: ${url}`);
    };

    try {
      const { ctx, waitUntilPromises } = createContext();
      const baseRequest = await buildRequest(`/downloads/${scenario.name}.bin`);
      const request = scenario.abortDuringDelay
        ? new Request(baseRequest.url, { headers: baseRequest.headers, signal: clientController.signal })
        : baseRequest;
      const workerPromise = worker.fetch(request, {
        CONTROLLER_URL: 'https://controller.example.test',
        CONTROLLER_API_TOKEN: 'controller-token',
        ENV: 'test',
        ROLE: 'download',
        INSTANCE_ID: 'worker-recovery',
        BOOTSTRAP_CACHE_MODE: 'direct',
      }, ctx);
      await delayStartedPromise;
      if (scenario.abortDuringDelay) {
        clientController.abort(new DOMException('cancel unified breaker delay', 'AbortError'));
      } else {
        delayCallback();
      }
      const response = await workerPromise;
      await Promise.allSettled(waitUntilPromises);
      if (scenario.abortDuringDelay) {
        assert.equal(response.status, 499, scenario.name);
        assert.equal((await response.json()).reason, 'client_aborted', scenario.name);
      } else {
        assert.equal(response.status, 503, scenario.name);
        assert.equal((await response.json()).reason, 'breaker_open', scenario.name);
      }
      if (scenario.expectReport) {
        assert.equal(reportBodies.length, 1, scenario.name);
        assert.equal(reportBodies[0].feedback.ticket, scenario.link.download.ticket, scenario.name);
        assert.equal(reportBodies[0].feedback.outcome, 'abandoned', scenario.name);
        assert.equal(reportBodies[0].feedback.status_code, 0, scenario.name);
      } else {
        assert.deepEqual(reportBodies, [], scenario.name);
      }
    } finally {
      globalThis.fetch = originalFetch;
      globalThis.setTimeout = originalSetTimeout;
      globalThis.clearTimeout = originalClearTimeout;
      if (originalNodeTestContext === undefined) {
        delete process.env.NODE_TEST_CONTEXT;
      } else {
        process.env.NODE_TEST_CONTEXT = originalNodeTestContext;
      }
    }
  }
});

test('terminal breaker cleanup keeps the independent OpenList report alive past two seconds', { concurrency: false }, async () => {
  delete globalThis.bootstrapCache;
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  const clientController = new AbortController();
  let cleanupTimerCallback = null;
  globalThis.setTimeout = (callback, delay, ...args) => {
    if (!cleanupTimerCallback && Number(delay) >= 1_990 && Number(delay) <= 2_010) {
      const handle = { terminalCleanupTimer: true, cleared: false };
      cleanupTimerCallback = () => {
        if (!handle.cleared) {
          callback(...args);
        }
      };
      return handle;
    }
    return originalSetTimeout(callback, delay, ...args);
  };
  globalThis.clearTimeout = (handle) => {
    if (handle?.terminalCleanupTimer) {
      handle.cleared = true;
      return;
    }
    return originalClearTimeout(handle);
  };
  const link = {
    ...linkFor(0),
    url: 'https://www.googleapis.com/drive/v3/files/terminal-cleanup.bin?alt=media',
    download: { ...linkFor(0).download, report_success: true },
  };
  const reportBodies = [];
  const reportSignals = [];
  let reportStarted;
  const reportStartedPromise = new Promise((resolve) => { reportStarted = resolve; });
  let settleStarted;
  const settleStartedPromise = new Promise((resolve) => { settleStarted = resolve; });
  let settleSignal = null;
  let settleBodyCancelled = false;
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap({ throttle: true }));
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        reportSignals.push(init.signal);
        reportStarted();
        if (reportBodies.length === 1) {
          return new Response('{}', { status: 503, headers: { 'Retry-After': '1' } });
        }
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: link });
    }
    if (url.startsWith('https://postgrest.example.test/THROTTLE_PROTECTION')) {
      return jsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
      }]);
    }
    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      return jsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 2,
        LAST_ERROR_CODE: null,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 7,
      }]);
    }
    if (url === link.url) {
      return new Response('gone', { status: 404, headers: { 'content-type': 'text/plain' } });
    }
    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      settleSignal = init.signal;
      settleStarted();
      return new Response(new ReadableStream({
        start() {},
        cancel() {
          settleBodyCancelled = true;
        },
      }), { status: 200, headers: { 'content-type': 'application/json' } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const baseRequest = await buildRequest('/downloads/terminal-cleanup.bin');
    const request = new Request(baseRequest.url, {
      headers: baseRequest.headers,
      signal: clientController.signal,
    });
    const context = createContext();
    const workerPromise = worker.fetch(request, {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, context.ctx);
    await settleStartedPromise;
    assert.notEqual(settleSignal, clientController.signal);
    assert.equal(settleSignal?.aborted, false);
    assert.equal(typeof cleanupTimerCallback, 'function');
    cleanupTimerCallback();
    const response = await workerPromise;
    assert.equal(response.status, 503);
    assert.equal((await response.json()).reason, 'breaker_settle_failed');
    assert.equal(settleSignal?.aborted, true);
    assert.equal(settleBodyCancelled, true);
    await Promise.race([
      reportStartedPromise,
      new Promise((resolve) => originalSetTimeout(resolve, 250)),
    ]);
    assert.equal(reportBodies.length >= 1, true);
    assert.notEqual(reportSignals[0], clientController.signal);
    assert.equal(reportSignals[0]?.aborted, false);
    clientController.abort(new DOMException('client canceled after breaker cleanup', 'AbortError'));
    assert.equal(reportSignals[0]?.aborted, false);
    await Promise.allSettled(context.waitUntilPromises);
    assert.equal(reportBodies.length, 2);
    assert.equal(reportBodies[0].feedback.event_id, reportBodies[1].feedback.event_id);
    assert.equal(reportBodies[0].feedback.outcome, 'failure');
    assert.equal(reportBodies[0].feedback.status_code, 404);
  } finally {
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
    globalThis.fetch = originalFetch;
  }
});

test('invalid Google content settles breaker debt in terminal cleanup and preserves status0 on authority failure', { concurrency: false }, async () => {
  delete globalThis.bootstrapCache;
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  const clientController = new AbortController();
  let cleanupTimerCallback = null;
  globalThis.setTimeout = (callback, delay, ...args) => {
    if (!cleanupTimerCallback && Number(delay) >= 1_990 && Number(delay) <= 2_010) {
      const handle = { terminalCleanupTimer: true, cleared: false };
      cleanupTimerCallback = () => {
        if (!handle.cleared) {
          callback(...args);
        }
      };
      return handle;
    }
    return originalSetTimeout(callback, delay, ...args);
  };
  globalThis.clearTimeout = (handle) => {
    if (handle?.terminalCleanupTimer) {
      handle.cleared = true;
      return;
    }
    return originalClearTimeout(handle);
  };
  const link = {
    ...linkFor(0),
    download: { ...linkFor(0).download, report_success: true },
  };
  const reportBodies = [];
  let settleStarted;
  const settleStartedPromise = new Promise((resolve) => { settleStarted = resolve; });
  let settleSignal = null;
  let settleBodyCancelled = false;
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap({ throttle: true }));
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: link });
    }
    if (url.startsWith('https://postgrest.example.test/THROTTLE_PROTECTION')) {
      return jsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
      }]);
    }
    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      return jsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 2,
        LAST_ERROR_CODE: null,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 7,
      }]);
    }
    if (url === 'https://www.googleapis.com/drive/v3/files/recovery-0?alt=media') {
      return new Response('hello', { status: 200, headers: { 'content-type': 'application/octet-stream' } });
    }
    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      settleSignal = init.signal;
      settleStarted();
      return new Response(new ReadableStream({
        start() {},
        cancel() {
          settleBodyCancelled = true;
        },
      }), { status: 200, headers: { 'content-type': 'application/json' } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const baseRequest = await buildRequest('/downloads/invalid-content-cleanup.bin');
    const request = new Request(baseRequest.url, {
      headers: baseRequest.headers,
      signal: clientController.signal,
    });
    const context = createContext();
    const workerPromise = worker.fetch(request, {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, context.ctx);
    await settleStartedPromise;
    assert.notEqual(settleSignal, clientController.signal);
    assert.equal(settleSignal?.aborted, false);
    clientController.abort(new DOMException('client canceled invalid content', 'AbortError'));
    assert.equal(settleSignal?.aborted, false);
    assert.equal(typeof cleanupTimerCallback, 'function');
    cleanupTimerCallback();
    const response = await workerPromise;
    assert.equal(response.status, 503);
    assert.equal((await response.json()).reason, 'breaker_settle_failed');
    assert.equal(settleSignal?.aborted, true);
    assert.equal(settleBodyCancelled, true);
    await Promise.allSettled(context.waitUntilPromises);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].feedback.outcome, 'failure');
    assert.equal(reportBodies[0].feedback.status_code, 0);
  } finally {
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
    globalThis.fetch = originalFetch;
  }
});

test('invalid Google content preserves execution-expiry feedback when terminal settlement expires', { concurrency: false }, async () => {
  delete globalThis.bootstrapCache;
  const cache = createCacheRpcFixture({ includeTicketState: true, acquireLeaseDurationSeconds: 30.15 });
  const originalFetch = globalThis.fetch;
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  const performanceDescriptor = Object.getOwnPropertyDescriptor(globalThis, 'performance');
  let monotonicNow = 0;
  let ownerTimerHandle = null;
  let ownerTimerCallback = null;
  let cleanupTimerHandle = null;
  let cleanupTimerCallback = null;
  let settleStarted;
  const settleStartedPromise = new Promise((resolve) => { settleStarted = resolve; });
  let settleSignal = null;
  let settleBodyCancelled = false;
  const reportBodies = [];
  const link = {
    ...linkFor(0),
    download: { ...linkFor(0).download, report_success: true },
  };
  Object.defineProperty(globalThis, 'performance', {
    configurable: true,
    value: { now: () => monotonicNow },
  });
  globalThis.setTimeout = (callback, delay, ...args) => {
    const numericDelay = Number(delay);
    if (!ownerTimerHandle && numericDelay >= 100 && numericDelay <= 200) {
      ownerTimerHandle = { ownerTimer: true, cleared: false };
      ownerTimerCallback = () => {
        if (!ownerTimerHandle.cleared) {
          callback(...args);
        }
      };
      return ownerTimerHandle;
    }
    if (!cleanupTimerHandle && numericDelay >= 1_990 && numericDelay <= 2_010) {
      cleanupTimerHandle = { cleanupTimer: true, cleared: false };
      cleanupTimerCallback = () => {
        if (!cleanupTimerHandle.cleared) {
          callback(...args);
        }
      };
      return cleanupTimerHandle;
    }
    return originalSetTimeout(callback, delay, ...args);
  };
  globalThis.clearTimeout = (handle) => {
    if (handle?.ownerTimer || handle?.cleanupTimer) {
      handle.cleared = true;
      return;
    }
    return originalClearTimeout(handle);
  };
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap({ throttle: true }));
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: link });
    }
    if (url.startsWith('https://postgrest.example.test/THROTTLE_PROTECTION')) {
      return jsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
      }]);
    }
    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      return jsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 2,
        LAST_ERROR_CODE: null,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 7,
      }]);
    }
    if (url === 'https://www.googleapis.com/drive/v3/files/recovery-0?alt=media') {
      return new Response('hello', { status: 200, headers: { 'content-type': 'application/octet-stream' } });
    }
    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      settleSignal = init.signal;
      settleStarted();
      return new Response(new ReadableStream({
        start(controller) {
          const abort = () => {
            settleBodyCancelled = true;
            controller.error(init.signal?.reason || new DOMException('settlement expired', 'AbortError'));
          };
          init.signal?.addEventListener?.('abort', abort, { once: true });
          if (init.signal?.aborted) {
            abort();
          }
        },
        cancel() {
          settleBodyCancelled = true;
        },
      }), { status: 200, headers: { 'content-type': 'application/json' } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const context = createContext();
    const responsePromise = worker.fetch(await buildRequest('/downloads/invalid-content-deadline-cleanup.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, context.ctx);
    await settleStartedPromise;
    assert.equal(settleSignal?.aborted, false);
    assert.equal(typeof ownerTimerCallback, 'function');
    assert.equal(typeof cleanupTimerCallback, 'function');
    monotonicNow = 200;
    ownerTimerCallback();
    assert.equal(settleSignal?.aborted, false);
    cleanupTimerCallback();
    const response = await responsePromise;
    await Promise.allSettled(context.waitUntilPromises);
    assert.equal(response.status, 503);
    assert.equal((await response.json()).reason, 'breaker_settle_failed');
    assert.equal(settleSignal?.aborted, true);
    assert.equal(settleBodyCancelled, true);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].feedback.outcome, 'failure');
    assert.equal(reportBodies[0].feedback.status_code, 0);
    assert.equal(reportBodies[0].feedback.reason, 'breaker_settle_failed');
  } finally {
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
    if (performanceDescriptor) {
      Object.defineProperty(globalThis, 'performance', performanceDescriptor);
    }
    globalThis.fetch = originalFetch;
  }
});

test('client cancellation during origin cleanup preserves abandoned feedback when settlement fails', { concurrency: false }, async () => {
  delete globalThis.bootstrapCache;
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  const clientController = new AbortController();
  const link = {
    ...linkFor(0),
    download: { ...linkFor(0).download, report_success: true },
  };
  const reportBodies = [];
  const settleBodies = [];
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap({ throttle: true }));
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: link });
    }
    if (url.startsWith('https://postgrest.example.test/THROTTLE_PROTECTION')) {
      return jsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
      }]);
    }
    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      return jsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 2,
        LAST_ERROR_CODE: null,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 7,
      }]);
    }
    if (url === link.url) {
      clientController.abort(new DOMException('client canceled origin', 'AbortError'));
      throw clientController.signal.reason;
    }
    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      settleBodies.push(JSON.parse(init.body));
      throw new Error('breaker authority unavailable during cleanup');
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createContext();
    const baseRequest = await buildRequest('/downloads/client-origin-cause.bin');
    const request = new Request(baseRequest.url, {
      headers: baseRequest.headers,
      signal: clientController.signal,
    });
    const response = await worker.fetch(request, {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    assert.equal(response.status, 503);
    assert.equal((await response.json()).reason, 'breaker_settle_failed');
    await Promise.allSettled(waitUntilPromises);
    assert.equal(settleBodies.length, 1);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].feedback.outcome, 'abandoned');
    assert.equal(reportBodies[0].feedback.status_code, 0);
    assert.equal(reportBodies[0].feedback.reason, 'download_abandoned');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('execution expiry during origin cleanup preserves deadline feedback when settlement fails', { concurrency: false }, async () => {
  delete globalThis.bootstrapCache;
  const cache = createCacheRpcFixture({ includeTicketState: true, acquireLeaseDurationSeconds: 30.15 });
  const originalFetch = globalThis.fetch;
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  const performanceDescriptor = Object.getOwnPropertyDescriptor(globalThis, 'performance');
  let monotonicNow = 0;
  let ownerTimerCallback = null;
  let ownerTimerHandle = null;
  Object.defineProperty(globalThis, 'performance', { configurable: true, value: { now: () => monotonicNow } });
  globalThis.setTimeout = (callback, delay, ...args) => {
    if (!ownerTimerHandle && Number(delay) >= 100 && Number(delay) <= 200) {
      ownerTimerHandle = { ownerTimer: true, cleared: false };
      ownerTimerCallback = () => {
        if (!ownerTimerHandle.cleared) {
          callback(...args);
        }
      };
      return ownerTimerHandle;
    }
    return originalSetTimeout(callback, delay, ...args);
  };
  globalThis.clearTimeout = (handle) => {
    if (handle?.ownerTimer) {
      handle.cleared = true;
      return;
    }
    return originalClearTimeout(handle);
  };
  const link = {
    ...linkFor(0),
    download: { ...linkFor(0).download, report_success: true },
  };
  const reportBodies = [];
  const settleBodies = [];
  let originStarted;
  const originStartedPromise = new Promise((resolve) => { originStarted = resolve; });
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap({ throttle: true }));
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: link });
    }
    if (url.startsWith('https://postgrest.example.test/THROTTLE_PROTECTION')) {
      return jsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
      }]);
    }
    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      return jsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 2,
        LAST_ERROR_CODE: null,
        ATTEMPT_GRANTED: true,
        ATTEMPT_TICKET: 7,
      }]);
    }
    if (url === link.url) {
      originStarted();
      return await new Promise((_, reject) => {
        init.signal?.addEventListener?.('abort', () => reject(init.signal.reason || new DOMException('origin deadline', 'AbortError')), { once: true });
      });
    }
    if (url === 'https://postgrest.example.test/rpc/download_settle_breaker_attempt') {
      settleBodies.push(JSON.parse(init.body));
      throw new Error('breaker authority unavailable during deadline cleanup');
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const context = createContext();
    const responsePromise = worker.fetch(await buildRequest('/downloads/deadline-origin-cause.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, context.ctx);
    await originStartedPromise;
    assert.equal(typeof ownerTimerCallback, 'function');
    monotonicNow = 200;
    ownerTimerCallback();
    const response = await responsePromise;
    assert.equal(response.status, 503);
    assert.equal((await response.json()).reason, 'breaker_settle_failed');
    await Promise.allSettled(context.waitUntilPromises);
    assert.equal(settleBodies.length, 1);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].feedback.outcome, 'failure');
    assert.equal(reportBodies[0].feedback.status_code, 0);
    assert.equal(reportBodies[0].feedback.reason, 'recovery_deadline_exhausted');
  } finally {
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
    if (performanceDescriptor) {
      Object.defineProperty(globalThis, 'performance', performanceDescriptor);
    }
    globalThis.fetch = originalFetch;
  }
});

test('CQ authority transport failure maps returned terminal cause to failure status0', { concurrency: false }, async () => {
  delete globalThis.bootstrapCache;
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  const reportBodies = [];
  const link = {
    ...linkFor(0),
    download: { ...linkFor(0).download, report_success: true },
  };
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap({ trueConcurrency: true }));
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: link });
    }
    if (url === 'https://cq.example.test/api/v1/concurrency/acquire') {
      throw new TypeError('CQ authority unavailable');
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createContext();
    const response = await worker.fetch(await buildRequest('/downloads/cq-authority-failure.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    assert.equal(response.status, 503);
    assert.equal((await response.json()).reason, 'cq_acquire_failed');
    await Promise.allSettled(waitUntilPromises);
    assert.equal(reportBodies.length, 1);
    assert.equal(reportBodies[0].feedback.outcome, 'failure');
    assert.equal(reportBodies[0].feedback.status_code, 0);
    assert.equal(reportBodies[0].feedback.reason, 'cq_acquire_failed');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('recursive child mark cancellation uses the client lifetime and reports abandoned status0', { concurrency: false }, async () => {
  delete globalThis.bootstrapCache;
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  const clientController = new AbortController();
  const childRequest = await buildRequest('/downloads/mark-cancel-child.bin');
  const parentLink = {
    ...linkFor(0),
    url: 'https://files.example.test/mark-cancel-parent.bin',
    download: { ...linkFor(0).download, provider: 'generic', report_success: true },
  };
  const childLink = {
    ...linkFor(1),
    url: 'https://files.example.test/mark-cancel-child.bin',
    download: { ...linkFor(1).download, provider: 'generic', report_success: true },
  };
  let phase = 'warm-child';
  const reportBodies = [];
  let markStarted;
  const markStartedPromise = new Promise((resolve) => { markStarted = resolve; });
  let markSignal = null;
  let markBodyCancelled = false;
  let releaseMarkBody = null;
  let childBodyCancelled = false;
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    if (url.endsWith('/rpc/download_mark_ticket_used') && phase === 'stall-mark') {
      markSignal = init.signal;
      markStarted();
      return new Response(new ReadableStream({
        start(controller) {
          releaseMarkBody = () => {
            try {
              controller.close();
            } catch (_error) {
              // The signal may have cancelled the body first.
            }
          };
        },
        cancel() {
          markBodyCancelled = true;
        },
      }), { status: 200, headers: { 'content-type': 'application/json' } });
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: body.path.includes('mark-cancel-child') ? childLink : parentLink });
    }
    if (url === parentLink.url) {
      return new Response(null, { status: 302, headers: { location: childRequest.url } });
    }
    if (url === childLink.url) {
      if (phase === 'warm-child') {
        return new Response('warm!', { status: 200, headers: { 'content-length': '5' } });
      }
      return new Response(new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode('child-content'));
        },
        cancel() {
          childBodyCancelled = true;
        },
      }), { status: 200, headers: { 'content-type': 'application/octet-stream', 'content-length': '13' } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const env = {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    };
    const warmContext = createContext();
    const warmResponse = await worker.fetch(childRequest, env, warmContext.ctx);
    assert.equal(warmResponse.status, 200);
    assert.equal(await warmResponse.text(), 'warm!');
    await Promise.allSettled(warmContext.waitUntilPromises);
    reportBodies.length = 0;
    phase = 'stall-mark';
    const baseRequest = await buildRequest('/downloads/mark-cancel-parent.bin');
    const request = new Request(baseRequest.url, {
      headers: baseRequest.headers,
      signal: clientController.signal,
    });
    const context = createContext();
    const responsePromise = worker.fetch(request, env, context.ctx);
    await markStartedPromise;
    assert.notEqual(markSignal, clientController.signal);
    assert.equal(markSignal?.aborted, false);
    clientController.abort(new DOMException('client canceled child mark', 'AbortError'));
    if (!markSignal?.aborted) {
      releaseMarkBody?.();
    }
    let responseTimeout;
    const response = await Promise.race([
      responsePromise,
      new Promise((_, reject) => {
        responseTimeout = originalSetTimeout(() => reject(new Error('child mark did not settle')), 1_000);
      }),
    ]);
    originalClearTimeout(responseTimeout);
    assert.equal(response.status, 499);
    assert.equal((await response.json()).reason, 'client_aborted');
    await Promise.allSettled(context.waitUntilPromises);
    assert.equal(markSignal?.aborted, true);
    assert.equal(markBodyCancelled, true);
    assert.equal(childBodyCancelled, true);
    assert.equal(reportBodies.some((body) => (
      body.feedback.ticket === childLink.download.ticket
      && body.feedback.outcome === 'abandoned'
      && body.feedback.status_code === 0
    )), true);
  } finally {
    releaseMarkBody?.();
    globalThis.fetch = originalFetch;
  }
});

test('recursive child early returns retire the inherited execution timer', { concurrency: false }, async () => {
  delete globalThis.bootstrapCache;
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  const timerHandles = [];
  globalThis.setTimeout = (callback, delay, ...args) => {
    if (Number(delay) >= 100_000) {
      const handle = { inheritedDeadlineTimer: true, cleared: false };
      timerHandles.push(handle);
      return handle;
    }
    return originalSetTimeout(callback, delay, ...args);
  };
  globalThis.clearTimeout = (handle) => {
    if (handle?.inheritedDeadlineTimer) {
      handle.cleared = true;
      return;
    }
    return originalClearTimeout(handle);
  };
  const childRequest = await buildRequest('/downloads/%E0%A4%A');
  const parentLink = {
    ...linkFor(0),
    url: 'https://files.example.test/inherited-early-parent.bin',
    download: { ...linkFor(0).download, provider: 'generic', report_success: true },
  };
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: parentLink });
    }
    if (url === parentLink.url) {
      return new Response(null, { status: 302, headers: { location: childRequest.url } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const { ctx, waitUntilPromises } = createContext();
    const response = await worker.fetch(await buildRequest('/downloads/inherited-early-parent.bin'), {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    }, ctx);
    assert.equal(response.status, 400);
    assert.equal((await response.json()).reason, 'invalid_path_encoding');
    await Promise.allSettled(waitUntilPromises);
    assert.equal(timerHandles.length >= 2, true);
    assert.equal(timerHandles.every((handle) => handle.cleared), true);
  } finally {
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
    globalThis.fetch = originalFetch;
  }
});

test('recursive cached generic-false handoff remains report-free', { concurrency: false }, async () => {
  delete globalThis.bootstrapCache;
  const cache = createCacheRpcFixture({ includeTicketState: true });
  const originalFetch = globalThis.fetch;
  const childRequest = await buildRequest('/downloads/generic-handoff-child.bin');
  const parentLink = {
    ...linkFor(0),
    url: 'https://files.example.test/generic-handoff-parent.bin',
    download: { ...linkFor(0).download, provider: 'generic', report_success: false },
  };
  const childLink = {
    ...linkFor(1),
    url: 'https://files.example.test/generic-handoff-child.bin',
    download: { ...linkFor(1).download, provider: 'generic', report_success: false },
  };
  let phase = 'warm-parent';
  const reportBodies = [];
  globalThis.fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const { url } = request;
    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return jsonResponse(buildBootstrap());
    }
    const cacheResponse = await cache.handle(input, init);
    if (cacheResponse) {
      return cacheResponse;
    }
    if (url.startsWith('https://alist.example.com/api/fs/link')) {
      const body = JSON.parse(init.body);
      if (body.action === 'report') {
        reportBodies.push(body);
        return jsonResponse({ code: 200, data: { applied: true, duplicate: false, stale: false } });
      }
      return jsonResponse({ code: 200, data: body.path.includes('generic-handoff-child') ? childLink : parentLink });
    }
    if (url === parentLink.url) {
      if (phase === 'warm-parent') {
        return new Response('warm!', { status: 200, headers: { 'content-length': '5' } });
      }
      return new Response(null, { status: 302, headers: { location: childRequest.url } });
    }
    if (url === childLink.url) {
      return new Response('child', { status: 200, headers: { 'content-length': '5' } });
    }
    throw new Error(`unexpected fetch URL: ${url}`);
  };

  try {
    const env = {
      CONTROLLER_URL: 'https://controller.example.test',
      CONTROLLER_API_TOKEN: 'controller-token',
      ENV: 'test',
      ROLE: 'download',
      INSTANCE_ID: 'worker-recovery',
      BOOTSTRAP_CACHE_MODE: 'direct',
    };
    const warmContext = createContext();
    const warmResponse = await worker.fetch(await buildRequest('/downloads/generic-handoff-parent.bin'), env, warmContext.ctx);
    assert.equal(warmResponse.status, 200);
    assert.equal(await warmResponse.text(), 'warm!');
    await Promise.allSettled(warmContext.waitUntilPromises);
    phase = 'redirect';
    const context = createContext();
    const response = await worker.fetch(await buildRequest('/downloads/generic-handoff-parent.bin'), env, context.ctx);
    assert.equal(response.status, 200);
    assert.equal(await response.text(), 'child');
    await Promise.allSettled(context.waitUntilPromises);
    assert.deepEqual(reportBodies, []);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
