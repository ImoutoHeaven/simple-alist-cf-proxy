import { test } from 'node:test';
import assert from 'node:assert/strict';
import { __fairQueueTestHooks } from '../src/worker.js';

const { resolveConfig, createConcurrencyHandlerClient } = __fairQueueTestHooks;

const DEFAULT_TRUE_CONCURRENCY_HEARTBEAT = {
  enabled: true,
  required: true,
  path: '/api/v1/concurrency/heartbeat',
  intervalMs: 5000,
  timeoutMs: 15000,
  reconnectGraceMs: 12000,
  helloTimeoutMs: 2000,
  startTimeoutMs: 7000,
  ackTimeoutMs: 2000,
  initialConnectMaxAttempts: 3,
  initialConnectMaxElapsedMs: 3000,
  reconnectMaxAttempts: 3,
  reconnectMaxElapsedMs: 10000,
  reconnectBaseDelayMs: 250,
  reconnectMaxDelayMs: 2000,
  reconnectSafetyMarginMs: 1000,
};

const buildTrueConcurrency = (overrides = {}) => {
  const config = {
    enabled: true,
    hostPatterns: ['*.sharepoint.com'],
    handlerUrl: 'https://cq.example.test/',
    handlerAuthKey: 'cq-secret',
    heartbeat: {
      enabled: true,
      required: true,
    },
    ...overrides,
  };

  if (overrides.heartbeat && typeof overrides.heartbeat === 'object') {
    config.heartbeat = {
      enabled: true,
      required: true,
      ...overrides.heartbeat,
    };
  }

  return config;
};

const createFakeHeartbeatSocket = ({
  helloAck = {
    type: 'hello_ack',
    generation: 7,
    deadlineMs: 12000,
    ackTimeoutMs: 2300,
    heartbeatIntervalMs: 5100,
    heartbeatTimeoutMs: 15100,
    reconnectGraceMs: 11900,
    startTimeoutMs: 6900,
    hardExpireAtMs: 20000,
  },
  heartbeatAck = {
    type: 'heartbeat_ack',
    generation: 7,
    deadlineMs: 15000,
    hardExpireAtMs: 20000,
  },
} = {}) => {
  const listeners = new Map();
  let closeArgs = null;

  const emit = (type, event = {}) => {
    const handlers = listeners.get(type);
    if (!handlers) {
      return;
    }
    for (const handler of [...handlers]) {
      handler(event);
    }
  };

  return {
    accepted: false,
    sent: [],
    addEventListener(type, handler) {
      const handlers = listeners.get(type) || new Set();
      handlers.add(handler);
      listeners.set(type, handlers);
    },
    removeEventListener(type, handler) {
      listeners.get(type)?.delete(handler);
    },
    accept() {
      this.accepted = true;
    },
    send(data) {
      this.sent.push(data);
      const payload = JSON.parse(data);
      if (payload.type === 'hello') {
        queueMicrotask(() => emit('message', { data: JSON.stringify(helloAck) }));
        return;
      }
      if (payload.type === 'heartbeat') {
        queueMicrotask(() => emit('message', { data: JSON.stringify(heartbeatAck) }));
        return;
      }
      throw new Error(`unexpected socket payload type: ${payload.type}`);
    },
    close(code = 1000, reason = '') {
      closeArgs = { code, reason };
      queueMicrotask(() => emit('close', { code, reason }));
    },
    getCloseArgs() {
      return closeArgs;
    },
  };
};

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
  const config = resolveConfig({}, buildBootstrap(buildTrueConcurrency()), { download: {} });

  assert.equal(config.trueConcurrencyEnabled, true);
  assert.deepEqual(config.trueConcurrencyHostnamePatterns, ['*.sharepoint.com']);
  assert.deepEqual(config.trueConcurrencySiteBucket, {
    mode: 'sharepoint',
    modes: ['sharepoint'],
  });
  assert.deepEqual(config.concurrencyHandlerConfig, {
    url: 'https://cq.example.test/',
    authKey: 'cq-secret',
    authHeader: 'X-CQ-Auth',
    acquireTimeoutMs: 11500,
    releaseTimeoutMs: 1500,
    waitTotalMaxMs: 20000,
    waitMaxAttemptsCap: 35,
    heartbeat: DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
  });
});

test('resolveConfig rejects malformed CQ wait budget bootstrap values', () => {
  assert.throws(
    () => resolveConfig({}, buildBootstrap(buildTrueConcurrency({ waitTotalMaxMs: 12.5 })), { download: {} }),
    /waitTotalMaxMs/
  );

  assert.throws(
    () => resolveConfig({}, buildBootstrap(buildTrueConcurrency({ waitMaxAttemptsCap: 0 })), { download: {} }),
    /waitMaxAttemptsCap/
  );
});

test('resolveConfig default acquire timeout keeps explicit slack above the default CQ wait poll window', () => {
  const config = resolveConfig({}, buildBootstrap(buildTrueConcurrency()), { download: {} });

  assert.equal(config.concurrencyHandlerConfig.acquireTimeoutMs, 11500);
  assert.ok(config.concurrencyHandlerConfig.acquireTimeoutMs > 10000);
});

test('resolveConfig requires true concurrency hostPatterns handlerUrl and handlerAuthKey when enabled', () => {
  assert.throws(
    () => resolveConfig({}, buildBootstrap(buildTrueConcurrency({
      handlerUrl: 'https://cq.example.test',
      handlerAuthKey: 'cq-secret',
      hostPatterns: undefined,
    })), { download: {} }),
    /hostPatterns/
  );

  assert.throws(
    () => resolveConfig({}, buildBootstrap(buildTrueConcurrency({
      handlerAuthKey: 'cq-secret',
      handlerUrl: undefined,
    })), { download: {} }),
    /handlerUrl/
  );

  assert.throws(
    () => resolveConfig({}, buildBootstrap(buildTrueConcurrency({
      handlerUrl: 'https://cq.example.test',
      handlerAuthKey: undefined,
    })), { download: {} }),
    /handlerAuthKey/
  );
});

test('resolveConfig requires heartbeat config and validates heartbeat timing when true concurrency is enabled', () => {
  assert.throws(
    () => resolveConfig({}, buildBootstrap({
      enabled: true,
      hostPatterns: ['*.sharepoint.com'],
      handlerUrl: 'https://cq.example.test',
      handlerAuthKey: 'cq-secret',
    }), { download: {} }),
    /heartbeat/
  );

  assert.throws(
    () => resolveConfig({}, buildBootstrap(buildTrueConcurrency({
      heartbeat: { enabled: false },
    })), { download: {} }),
    /heartbeat\.enabled/
  );

  assert.throws(
    () => resolveConfig({}, buildBootstrap(buildTrueConcurrency({
      heartbeat: { required: false },
    })), { download: {} }),
    /heartbeat\.required/
  );

  assert.throws(
    () => resolveConfig({}, buildBootstrap(buildTrueConcurrency({
      heartbeat: { intervalMs: 5000, timeoutMs: 5000 },
    })), { download: {} }),
    /timeoutMs/
  );

  assert.throws(
    () => resolveConfig({}, buildBootstrap(buildTrueConcurrency({
      heartbeat: { timeoutMs: 15000, reconnectGraceMs: 15001 },
    })), { download: {} }),
    /reconnectGraceMs/
  );

  assert.throws(
    () => resolveConfig({}, buildBootstrap(buildTrueConcurrency({
      heartbeat: { reconnectGraceMs: 12000, reconnectSafetyMarginMs: 12000 },
    })), { download: {} }),
    /reconnectSafetyMarginMs/
  );

  assert.throws(
    () => resolveConfig({}, buildBootstrap(buildTrueConcurrency({
      heartbeat: { reconnectGraceMs: 12000, reconnectSafetyMarginMs: 1000, reconnectMaxElapsedMs: 11001 },
    })), { download: {} }),
    /reconnect/
  );

  assert.throws(
    () => resolveConfig({}, buildBootstrap(buildTrueConcurrency({
      heartbeat: {
        helloTimeoutMs: 2000,
        startTimeoutMs: 7000,
        reconnectSafetyMarginMs: 1000,
        initialConnectMaxElapsedMs: 4001,
      },
    })), { download: {} }),
    /startTimeoutMs/
  );
});

test('resolveConfig accepts explicit disabled heartbeat config when true concurrency is disabled', () => {
  const config = resolveConfig({}, buildBootstrap(buildTrueConcurrency({
    enabled: false,
    heartbeat: {
      enabled: false,
      required: false,
    },
  })), { download: {} });

  assert.equal(config.trueConcurrencyEnabled, false);
  assert.deepEqual(config.concurrencyHandlerConfig.heartbeat, {
    ...DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
    enabled: false,
    required: false,
  });
});

test('resolveConfig accepts host site bucket mode', () => {
  const config = resolveConfig({}, buildBootstrap(buildTrueConcurrency({
    hostPatterns: ['*.example.com'],
    handlerUrl: 'https://cq.example.test',
    siteBucket: { mode: 'host' },
  })), { download: {} });

  assert.deepEqual(config.trueConcurrencySiteBucket, {
    mode: 'host',
    modes: ['host'],
  });
});

test('resolveConfig normalizes true concurrency site bucket modes with precedence and fallback', () => {
  const modesWin = resolveConfig({}, buildBootstrap(buildTrueConcurrency({
    hostPatterns: ['*.example.com'],
    handlerUrl: 'https://cq.example.test',
    siteBucket: { mode: 'googledrive', modes: ['', 'host', 'host', 'sharepoint'] },
  })), { download: {} });

  assert.deepEqual(modesWin.trueConcurrencySiteBucket, {
    mode: 'host',
    modes: ['host', 'sharepoint'],
  });

  const emptyModesFallback = resolveConfig({}, buildBootstrap(buildTrueConcurrency({
    hostPatterns: ['*.example.com'],
    handlerUrl: 'https://cq.example.test',
    siteBucket: { mode: 'googledrive', modes: ['', '   '] },
  })), { download: {} });

  assert.deepEqual(emptyModesFallback.trueConcurrencySiteBucket, {
    mode: 'googledrive',
    modes: ['googledrive'],
  });

  const defaultModes = resolveConfig({}, buildBootstrap(buildTrueConcurrency({
    hostPatterns: ['*.example.com'],
    handlerUrl: 'https://cq.example.test',
    siteBucket: {},
  })), { download: {} });

  assert.deepEqual(defaultModes.trueConcurrencySiteBucket, {
    mode: 'sharepoint',
    modes: ['sharepoint'],
  });
});

test('resolveConfig rejects unsupported true concurrency site bucket modes', () => {
  assert.throws(
    () => resolveConfig({}, buildBootstrap(buildTrueConcurrency({
      siteBucket: { modes: ['sharepoint', 'other'] },
    })), { download: {} }),
    /unsupported siteBucket mode other/
  );
});

test('concurrency client sends wait acquire to the normalized endpoint with auth header and timeout signal', async () => {
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
    return new Response(JSON.stringify({ result: 'wait', waitToken: 'wait-1', scope: 'host', retryAfter: 2 }), {
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
      nowMs: 101,
      waitToken: 'wait-1',
    });

    assert.deepEqual(result, { result: 'wait', waitToken: 'wait-1', scope: 'host', retryAfter: 2 });
    assert.equal(calls.length, 1);
    assert.equal(calls[0].url, 'https://cq.example.test/api/v1/concurrency/acquire');
    assert.equal(calls[0].method, 'POST');
    assert.equal(calls[0].headers['X-CQ-Auth'], 'cq-secret');
    assert.equal(calls[0].hasSignal, true);
    assert.deepEqual(calls[0].body, {
      hostname: 'tenant.sharepoint.com',
      hostnameHash: 'host-hash',
      siteBucket: 'site-hash',
      ipBucket: 'ip-hash',
      requestId: 'req-1',
      hardExpireAtMs: 5000,
      nowMs: 101,
      waitToken: 'wait-1',
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('concurrency client honors per-call acquire timeout override', async () => {
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async (_url, init = {}) => new Promise((_resolve, reject) => {
    const abortError = new Error('The operation was aborted.');
    abortError.name = 'AbortError';

    if (init.signal?.aborted) {
      reject(abortError);
      return;
    }

    init.signal?.addEventListener('abort', () => reject(abortError), { once: true });
  });

  try {
    const client = createConcurrencyHandlerClient({
      concurrencyHandlerConfig: {
        url: 'https://cq.example.test/',
        authKey: 'cq-secret',
        acquireTimeoutMs: 2000,
      },
    });

    const startedAt = Date.now();
    await assert.rejects(
      () => client.acquire(null, {
        hostname: 'tenant.sharepoint.com',
        hostnameHash: 'host-hash',
        siteBucket: 'site-hash',
        ipBucket: 'ip-hash',
        requestId: 'req-1',
        hardExpireAtMs: 5000,
        nowMs: 101,
        waitToken: 'wait-1',
      }, undefined, 25),
      (error) => {
        assert.equal(error?.name, 'AbortError');
        return true;
      }
    );
    const elapsedMs = Date.now() - startedAt;
    assert.ok(elapsedMs < 250, `expected shorter acquire timeout override, saw ${elapsedMs}ms`);
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
      claimToken: 'claim-token-1',
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
      claimToken: 'claim-token-1',
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('concurrency client exposes connectHeartbeat and enforces heartbeat wire contract', async () => {
  const calls = [];
  const originalFetch = globalThis.fetch;
  const originalNow = Date.now;
  const ws = createFakeHeartbeatSocket();

  Date.now = () => 123456;
  globalThis.fetch = async (url, init = {}) => {
    calls.push({
      url,
      method: init.method,
      headers: init.headers,
      hasSignal: init.signal instanceof AbortSignal,
    });
    return {
      status: 101,
      webSocket: ws,
    };
  };

  try {
    const client = createConcurrencyHandlerClient({
      concurrencyHandlerConfig: {
        url: 'https://cq.example.test/',
        authKey: 'cq-secret',
        authHeader: 'X-CQ-Auth',
        heartbeat: DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
      },
    });

    assert.equal(typeof client.connectHeartbeat, 'function');

    const heartbeat = await client.connectHeartbeat(null, {
      requestId: 'req-1',
      leaseId: 'lease-1',
      leaseToken: 'token-1',
      ticketHash: 'ticket-hash-1',
      hardExpireAtMs: 20000,
      clientInstanceId: 'client-1',
      attempt: 2,
    });

    assert.equal(calls.length, 1);
    assert.equal(calls[0].url, 'https://cq.example.test/api/v1/concurrency/heartbeat');
    assert.equal(calls[0].method, undefined);
    assert.equal(calls[0].headers.Upgrade, 'websocket');
    assert.equal(calls[0].headers['X-CQ-Auth'], 'cq-secret');
    assert.equal(calls[0].hasSignal, true);
    assert.equal(ws.accepted, true);

    const hello = JSON.parse(ws.sent[0]);
    assert.equal(hello.type, 'hello');
    assert.equal(hello.requestId, 'req-1');
    assert.equal(hello.leaseId, 'lease-1');
    assert.equal(hello.leaseToken, 'token-1');
    assert.equal(hello.ticketHash, 'ticket-hash-1');
    assert.equal(hello.hardExpireAtMs, 20000);
    assert.equal(hello.clientInstanceId, 'client-1');
    assert.equal(hello.attempt, 2);
    assert.equal(typeof hello.nowMs, 'number');
    assert.equal(Object.hasOwn(hello, 'downloadedBytes'), false);

    assert.equal(heartbeat.ws, ws);
    assert.equal(heartbeat.generation, 7);
    assert.equal(heartbeat.deadlineMs, 12000);
    assert.equal(heartbeat.ackTimeoutMs, 2300);
    assert.equal(heartbeat.heartbeatIntervalMs, 5100);
    assert.equal(heartbeat.heartbeatTimeoutMs, 15100);
    assert.equal(heartbeat.reconnectGraceMs, 11900);
    assert.equal(heartbeat.startTimeoutMs, 6900);
    assert.equal(heartbeat.hardExpireAtMs, 20000);
    assert.equal(typeof heartbeat.close, 'function');
    assert.equal(typeof heartbeat.sendHeartbeat, 'function');

    const ack = await heartbeat.sendHeartbeat();
    const heartbeatMessage = JSON.parse(ws.sent[1]);
    assert.equal(heartbeatMessage.type, 'heartbeat');
    assert.equal(heartbeatMessage.requestId, 'req-1');
    assert.equal(heartbeatMessage.leaseId, 'lease-1');
    assert.equal(heartbeatMessage.leaseToken, 'token-1');
    assert.equal(heartbeatMessage.ticketHash, 'ticket-hash-1');
    assert.equal(heartbeatMessage.generation, 7);
    assert.equal(typeof heartbeatMessage.nowMs, 'number');
    assert.equal(Object.hasOwn(heartbeatMessage, 'downloadedBytes'), false);

    assert.deepEqual(ack, {
      type: 'heartbeat_ack',
      generation: 7,
      deadlineMs: 15000,
      hardExpireAtMs: 20000,
    });

    heartbeat.close('done');
    assert.deepEqual(ws.getCloseArgs(), { code: 1000, reason: 'done' });
  } finally {
    globalThis.fetch = originalFetch;
    Date.now = originalNow;
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

test('concurrency client parses direct expired release results', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({ result: 'expired', reason: 'hard_expired' }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });

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

    assert.deepEqual(result, { result: 'expired', reason: 'hard_expired' });
  } finally {
    globalThis.fetch = originalFetch;
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

test('concurrency client rejects acquire success payloads with endpoint-invalid results', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({
    result: 'allow',
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
        nowMs: 101,
      }),
      /acquire/
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
    claimToken: 'claim-token-1',
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

test('concurrency client rejects wait payloads with unsupported scope', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({
    result: 'wait',
    scope: 'global',
    waitToken: 'wait-1',
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
        nowMs: 101,
      }),
      /scope/
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('concurrency client rejects conflict payloads with unsupported reason', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({
    result: 'conflict',
    reason: 'busy',
  }), {
    status: 409,
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

test('resolveConfig omits legacy precheck timeout from true concurrency config', () => {
  const config = resolveConfig({}, buildBootstrap(buildTrueConcurrency({
    precheckTimeoutMs: 9999,
  })), { download: {} });

  assert.equal(Object.hasOwn(config.concurrencyHandlerConfig, 'precheckTimeoutMs'), false);
  assert.deepEqual(config.concurrencyHandlerConfig, {
    url: 'https://cq.example.test/',
    authKey: 'cq-secret',
    authHeader: 'X-CQ-Auth',
    acquireTimeoutMs: 11500,
    releaseTimeoutMs: 1500,
    waitTotalMaxMs: 20000,
    waitMaxAttemptsCap: 35,
    heartbeat: DEFAULT_TRUE_CONCURRENCY_HEARTBEAT,
  });
});

test('concurrency client normalizes wait responses and forwards waitToken on continue-wait acquire', async () => {
  const originalFetch = globalThis.fetch;
  const seenBodies = [];

  globalThis.fetch = async (url, init = {}) => {
    assert.equal(url, 'https://cq.example.test/api/v1/concurrency/acquire');
    assert.equal(init.headers['X-CQ-Auth'], 'cq-secret');
    const body = JSON.parse(init.body);
    seenBodies.push(body);
    if (seenBodies.length === 1) {
      return new Response(JSON.stringify({
        result: 'wait',
        waitToken: 'wait-1',
        scope: 'host',
        retryAfter: 2,
      }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }

    return new Response(JSON.stringify({
      result: 'granted',
      leaseId: 'lease-1',
      leaseToken: 'token-1',
      expiresAtMs: 5000,
      claimToken: 'claim-token-1',
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

    const first = await client.acquire(null, {
      hostname: 'tenant.sharepoint.com',
      hostnameHash: 'host-hash',
      siteBucket: 'site-hash',
      ipBucket: 'ip-hash',
      requestId: 'req-1',
      hardExpireAtMs: 5000,
      nowMs: 111,
    });
    assert.deepEqual(first, {
      result: 'wait',
      waitToken: 'wait-1',
      scope: 'host',
      retryAfter: 2,
    });

    const second = await client.acquire(null, {
      hostname: 'tenant.sharepoint.com',
      hostnameHash: 'host-hash',
      siteBucket: 'site-hash',
      ipBucket: 'ip-hash',
      requestId: 'req-1',
      hardExpireAtMs: 5000,
      nowMs: 222,
      waitToken: 'wait-1',
    });
    assert.deepEqual(second, {
      result: 'granted',
      leaseId: 'lease-1',
      leaseToken: 'token-1',
      expiresAtMs: 5000,
      claimToken: 'claim-token-1',
    });

    assert.equal(Object.hasOwn(seenBodies[0], 'waitToken'), false);
    assert.equal(seenBodies[1].waitToken, 'wait-1');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('concurrency client sends cancel payload and parses cancelled results', async () => {
  const originalFetch = globalThis.fetch;
  const originalNow = Date.now;
  Date.now = () => 999;

  globalThis.fetch = async (url, init = {}) => {
    assert.equal(url, 'https://cq.example.test/api/v1/concurrency/cancel');
    assert.equal(init.headers['X-CQ-Auth'], 'cq-secret');
    assert.deepEqual(JSON.parse(init.body), {
      requestId: 'req-1',
      hostname: 'tenant.sharepoint.com',
      hostnameHash: 'host-hash',
      siteBucket: 'site-hash',
      ipBucket: 'ip-hash',
      hardExpireAtMs: 5000,
      reason: 'worker_aborted',
      nowMs: 999,
    });
    return new Response(JSON.stringify({ result: 'cancelled' }), {
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

    const result = await client.cancel(null, {
      requestId: 'req-1',
      hostname: 'tenant.sharepoint.com',
      hostnameHash: 'host-hash',
      siteBucket: 'site-hash',
      ipBucket: 'ip-hash',
      hardExpireAtMs: 5000,
    }, 'worker_aborted');

    assert.deepEqual(result, { result: 'cancelled' });
  } finally {
    globalThis.fetch = originalFetch;
    Date.now = originalNow;
  }
});
