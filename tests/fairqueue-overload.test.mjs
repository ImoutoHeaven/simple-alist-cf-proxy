import { test } from 'node:test';
import assert from 'node:assert/strict';
import { nextOverloadDelayMs } from '../src/fairqueue-overload.js';
import { __fairQueueTestHooks } from '../src/worker.js';

const SCOPED_OVERLOAD_WAIT_MIN_MS = 350;
const SCOPED_OVERLOAD_WAIT_MAX_MS = 3000;

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
    throttleConfig: { throttleTimeWindow: 60 },
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
    throttleConfig: { throttleTimeWindow: 60 },
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
    return new Response(JSON.stringify({ result: 'granted', slotToken: 'slot-1' }), {
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
    throttleConfig: { throttleTimeWindow: 60 },
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
    return new Response(JSON.stringify({ result: 'granted', slotToken: 'slot-1' }), {
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
    throttleConfig: { throttleTimeWindow: 60 },
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
    return new Response(JSON.stringify({ result: 'granted', slotToken: 'slot-site-1' }), {
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
    throttleConfig: { throttleTimeWindow: 60 },
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
    return new Response(JSON.stringify({ result: 'granted', slotToken: 'slot-ip-1' }), {
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
    throttleConfig: { throttleTimeWindow: 60 },
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
    return new Response(JSON.stringify({ result: 'granted', slotToken: 'slot-site-b-1' }), {
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
    throttleConfig: { throttleTimeWindow: 60 },
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
    return new Response(JSON.stringify({ result: 'granted', slotToken: 'slot-ip-b-1' }), {
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
    throttleConfig: { throttleTimeWindow: 60 },
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

    return new Response(JSON.stringify({ result: 'granted', slotToken: 'slot-host-1' }), {
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
    throttleConfig: { throttleTimeWindow: 60 },
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
    return new Response(JSON.stringify({ result: 'granted', slotToken: 'slot-1' }), {
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
    throttleConfig: { throttleTimeWindow: 60 },
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
    return new Response(JSON.stringify({ result: 'granted', slotToken: 'slot-1' }), {
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
    throttleConfig: { throttleTimeWindow: 60 },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    slotToken: 'slot-1',
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
    throttleConfig: { throttleTimeWindow: 60 },
  });

  const fqContext = {
    hostname: 'example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    slotToken: 'slot-1',
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
