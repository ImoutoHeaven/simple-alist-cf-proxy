import { test } from 'node:test';
import assert from 'node:assert/strict';
import { nextOverloadDelayMs } from '../src/fairqueue-overload.js';
import { __fairQueueTestHooks } from '../src/worker.js';

test('overload backoff increases by 500ms up to 4s', () => {
  assert.equal(nextOverloadDelayMs(0), 1000);
  assert.equal(nextOverloadDelayMs(1), 1500);
  assert.equal(nextOverloadDelayMs(2), 2000);
  assert.equal(nextOverloadDelayMs(6), 4000);
});

test('host-level overloaded cooldown avoids tight acquire loops', () => {
  const HOST_OVERLOAD_STAIRCASE = [
    { streak: 0, delayMs: 1000 },
    { streak: 1, delayMs: 2000 },
    { streak: 2, delayMs: 3000 },
    { streak: 3, delayMs: 4000 },
    { streak: 4, delayMs: 4000 },
  ];

  for (const item of HOST_OVERLOAD_STAIRCASE) {
    assert.equal(
      nextOverloadDelayMs(item.streak, { hostOverload: true }),
      item.delayMs,
      `expected host-overload delay for streak ${item.streak}`
    );
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
