import assert from 'node:assert/strict';
import test from 'node:test';

import { encryptBindingPayload } from '../src/origin-binding.js';
import worker, { __fairQueueTestHooks } from '../src/worker.js';

const CONTROLLER_URL = 'https://controller.example.test';
const CONTROLLER_BOOTSTRAP_URL = `${CONTROLLER_URL}/api/v0/bootstrap`;

async function captureConsole(callback) {
  const originalLog = console.log;
  const originalWarn = console.warn;
  const originalError = console.error;
  const entries = [];

  console.log = (...args) => {
    entries.push({ level: 'info', text: args.map(String).join(' ') });
  };
  console.warn = (...args) => {
    entries.push({ level: 'warn', text: args.map(String).join(' ') });
  };
  console.error = (...args) => {
    entries.push({ level: 'error', text: args.map(String).join(' ') });
  };

  try {
    await callback();
  } finally {
    console.log = originalLog;
    console.warn = originalWarn;
    console.error = originalError;
  }

  return entries;
}

function buildBootstrap(overrides = {}) {
  return {
    configVersion: 'worker-observability-terminal-test',
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
      ...(overrides.common || {}),
    },
    download: {
      address: 'https://alist.example.com',
      auth: {
        ipv4Only: true,
      },
      db: {
        mode: '',
      },
      ...(overrides.download || {}),
    },
    ...Object.fromEntries(
      Object.entries(overrides).filter(([key]) => key !== 'common' && key !== 'download'),
    ),
  };
}

function buildControllerEnv(extra = {}) {
  return {
    CONTROLLER_URL,
    CONTROLLER_API_TOKEN: 'controller-token',
    ENV: 'test',
    ROLE: 'download',
    INSTANCE_ID: 'worker-1',
    BOOTSTRAP_CACHE_MODE: 'direct',
    ...extra,
  };
}

async function withFetchStub(fetchStub, callback) {
  const originalFetch = globalThis.fetch;
  delete globalThis.bootstrapCache;
  globalThis.fetch = fetchStub;
  try {
    return await callback();
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
  }
}

function buildBootstrapFetch(bootstrap) {
  return async (input) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url === CONTROLLER_BOOTSTRAP_URL) {
      return new Response(JSON.stringify(bootstrap), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };
}

function terminalEntries(entries, reason) {
  return entries.filter((entry) => entry.text.includes('[Terminal] response')
    && (!reason || entry.text.includes(`reason=${reason}`)));
}

function createStreamingSseResponse(frames, init = {}) {
  const encoder = new TextEncoder();
  let index = 0;

  return new Response(new ReadableStream({
    pull(controller) {
      if (index >= frames.length) {
        controller.close();
        return;
      }

      controller.enqueue(encoder.encode(frames[index]));
      index += 1;
      if (index >= frames.length) {
        controller.close();
      }
    },
  }), {
    status: init.status ?? 200,
    headers: {
      'content-type': 'text/event-stream',
      ...(init.headers || {}),
    },
  });
}

function createFairQueueWaitSseResponse(finalPayload, options = {}) {
  const acceptedPayload = {
    queryToken: options.queryToken ?? finalPayload.queryToken ?? 'fq-query-observability',
    invocationEpoch: options.invocationEpoch ?? finalPayload.invocationEpoch ?? 1,
    deadlineMs: options.acceptedDeadlineMs ?? finalPayload.deadlineMs ?? (Date.now() + 1_000),
  };

  return createStreamingSseResponse([
    'event: accepted\n',
    `data: ${JSON.stringify(acceptedPayload)}\n\n`,
    ': keepalive 1710000000001\n\n',
    'event: result\n',
    `data: ${JSON.stringify({
      ...finalPayload,
      queryToken: finalPayload.queryToken ?? acceptedPayload.queryToken,
      invocationEpoch: finalPayload.invocationEpoch ?? acceptedPayload.invocationEpoch,
    })}\n\n`,
  ], options);
}

function encodeBase64Url(input) {
  return Buffer.from(input)
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
}

function encodeSignatureBase64Url(input) {
  return Buffer.from(input)
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');
}

async function signPayload(payload, expire, token) {
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
}

async function buildSignedWorkerRequest(pathname = '/download/file.bin') {
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
    idle_timeout: 300,
    ticketNonce: 'abcdefghijklmnopqrstuvwxyz',
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
}

function createBreakerRow(overrides = {}) {
  return {
    STATE: 'closed',
    OPEN_UNTIL: null,
    OPEN_REASON: null,
    VERSION: 3,
    LAST_ERROR_CODE: null,
    ATTEMPT_GRANTED: true,
    ATTEMPT_TICKET: 11,
    ...overrides,
  };
}

test('shared logging helpers remain exposed through worker test hooks', async () => {
  assert.equal(typeof __fairQueueTestHooks.logEvent, 'function');
  assert.equal(typeof __fairQueueTestHooks.bindWaitUntil, 'function');
});

test('logTerminalResponse returns original response and logs non-200/206 statuses', async () => {
  const response = new Response('Forbidden', { status: 403 });
  let returned;

  const entries = await captureConsole(() => {
    returned = __fairQueueTestHooks.logTerminalResponse(response, 'inner_auth_rejected', {
      phase: 'fetch',
      host: 'worker.example.com',
    });
  });

  assert.equal(returned, response);
  assert.equal(entries.length, 1);
  const [entry] = entries;
  assert.ok(entry.text.includes('[Terminal] response'));
  assert.match(entry.text, /^\[Terminal\] response /);
  assert.match(entry.text, /status=403/);
  assert.match(entry.text, /reason=inner_auth_rejected/);
  assert.match(entry.text, /phase=fetch/);
  assert.match(entry.text, /host=worker\.example\.com/);
});

test('logTerminalResponse suppresses successful content responses only', async () => {
  const entries = await captureConsole(() => {
    __fairQueueTestHooks.logTerminalResponse(new Response('ok', { status: 200 }), 'ok');
    __fairQueueTestHooks.logTerminalResponse(new Response('partial', { status: 206 }), 'partial');
    __fairQueueTestHooks.logTerminalResponse(new Response(null, { status: 204 }), 'empty_success');
    __fairQueueTestHooks.logTerminalResponse(Response.redirect('https://example.com/next', 302), 'redirect_returned');
  });

  const entriesFor200 = entries.filter((entry) => /status=200|status=206/.test(entry.text));
  assert.equal(entriesFor200.length, 0);
  assert.equal(entries.length, 2);
  assert.match(entries[0].text, /status=204/);
  assert.match(entries[0].text, /reason=empty_success/);
  assert.match(entries[1].text, /status=302/);
  assert.match(entries[1].text, /reason=redirect_returned/);
});

test('bindWaitUntil remains callable through worker test hooks', async () => {
  const waitUntilPromises = [];
  const ctx = {
    waitUntil(promise) {
      waitUntilPromises.push(promise);
    },
  };

  let result;
  await captureConsole(async () => {
    const boundPromise = __fairQueueTestHooks.bindWaitUntil(
      ctx,
      Promise.resolve('complete'),
      'CQ',
      'release_cleanup',
      { requestId: 'req-wait-1' },
    );

    assert.equal(waitUntilPromises.length, 1);
    assert.equal(waitUntilPromises[0], boundPromise);
    result = await boundPromise;
  });

  assert.equal(result, 'complete');
});

test('concurrency handler client logs acquire lifecycle', async () => {
  const client = __fairQueueTestHooks.createConcurrencyHandlerClient({
    concurrencyHandlerConfig: {
      url: 'https://cq.example.test',
      authKey: '',
    },
  });
  const plan = {
    hostname: 'files.example.com',
    hostnameHash: 'host-hash',
    siteBucket: 'site-bucket',
    ipBucket: 'ip-bucket',
    requestId: 'cq-lifecycle-1',
    hardExpireAtMs: Date.now() + 60_000,
    nowMs: Date.now(),
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({
    result: 'conflict',
    reason: 'request_id_tuple_mismatch',
  }), {
    status: 409,
    headers: { 'content-type': 'application/json' },
  });

  try {
    const entries = await captureConsole(async () => {
      const result = await client.acquire({}, plan);
      assert.equal(result.result, 'conflict');
    });

    assert.ok(entries.some((entry) => /\[CQ\] acquire_start/.test(entry.text)
      && /requestId=cq-lifecycle-1/.test(entry.text)
      && /host=files\.example\.com/.test(entry.text)));
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('slot-handler client logs terminal fair queue lifecycle result', async () => {
  const client = __fairQueueTestHooks.createSlotHandlerClient({
    slotHandlerConfig: {
      url: 'https://slot-handler.example.test',
      totalMaxWaitMs: 20_000,
      authKey: '',
    },
  });
  const fqContext = {
    hostname: 'queue.example.com',
    hostnameHash: 'host-hash',
    ipBucket: 'ip-bucket',
    siteBucket: 'site-bucket',
    requestId: 'fq-lifecycle-1',
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => createFairQueueWaitSseResponse({
    result: 'timeout',
    reason: 'slot-handler-timeout',
  });

  try {
    const entries = await captureConsole(async () => {
      const result = await client.waitForSlot({}, fqContext);
      assert.equal(result.kind, 'timeout');
    });

    assert.ok(entries.some((entry) => /\[FQ\] terminal_result/.test(entry.text)
      && /requestId=fq-lifecycle-1/.test(entry.text)
      && /host=queue\.example\.com/.test(entry.text)
      && /result=timeout/.test(entry.text)));
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('worker breaker path logs sample report lifecycle events', async () => {
  const bootstrap = buildBootstrap({
    download: {
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
  let reportCalls = 0;
  const fetchStub = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url === CONTROLLER_BOOTSTRAP_URL) {
      return new Response(JSON.stringify(bootstrap), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    if (url === 'https://postgrest.example.test/rpc/download_get_ticket_state') {
      const body = JSON.parse(init.body);
      const nowSeconds = Math.floor(Date.now() / 1000);
      return new Response(JSON.stringify([{
        found: true,
        ticket_hash: body.p_ticket_hash,
        issued_at: nowSeconds,
        first_used_at: null,
        hard_expire_at: nowSeconds + 600,
        idle_timeout_seconds: 300,
        idle_policy: 'first_use',
        idle_lease_expires_at: nowSeconds + 300,
        idle_renew_owner_lease_id: null,
        idle_renew_owner_last_heartbeat_at: null,
      }]), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    if (/^https:\/\/postgrest\.example\.test\/THROTTLE_PROTECTION\?HOSTNAME_HASH=eq\./.test(url)) {
      return new Response(JSON.stringify([]), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      return new Response(JSON.stringify([createBreakerRow()]), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    if (url === 'https://postgrest.example.test/rpc/download_report_breaker_sample') {
      reportCalls += 1;
      return new Response(JSON.stringify([createBreakerRow({
        STATE: 'open',
        OPEN_UNTIL: Math.floor(Date.now() / 1000) + 60,
        OPEN_REASON: 'http_503',
        VERSION: 4,
        LAST_ERROR_CODE: 503,
      })]), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    if (url === 'https://alist.example.com/api/fs/link') {
      return new Response(JSON.stringify({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/file',
          header: {},
        },
      }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    if (url === 'https://tenant.sharepoint.com/file') {
      return new Response('upstream unavailable', {
        status: 503,
        headers: { 'retry-after': '9' },
      });
    }
    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  let response;
  const entries = await withFetchStub(fetchStub, () => captureConsole(async () => {
    response = await worker.fetch(await buildSignedWorkerRequest('/downloads/breaker-sample.bin'), buildControllerEnv(), {});
  }));

  assert.equal(response.status, 503);
  assert.equal(reportCalls, 1);
  assert.ok(entries.some((entry) => /\[Breaker\] sample_report_start/.test(entry.text)
    && /host=tenant\.sharepoint\.com/.test(entry.text)
    && /status=503/.test(entry.text)));
  assert.ok(entries.some((entry) => /\[Breaker\] sample_report_done/.test(entry.text)
    && /host=tenant\.sharepoint\.com/.test(entry.text)
    && /status=503/.test(entry.text)));
  assert.ok(terminalEntries(entries, 'upstream_generated_5xx').some((entry) => /status=503/.test(entry.text)));
});

test('worker.fetch does not log fq/cq unavailable terminal reasons on a successful managed download', async () => {
  const bootstrapFq = buildBootstrap({
    download: {
      fairQueue: {
        enabled: true,
        hostPatterns: ['*.sharepoint.com'],
        slotHandlerUrl: 'https://slot-handler.example.test',
        slotHandlerAuthKey: 'slot-secret',
        slotHandlerAuthHeader: 'X-FQ-Auth',
      },
    },
  });
  const fetchStub = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url === CONTROLLER_BOOTSTRAP_URL) {
      return new Response(JSON.stringify(bootstrapFq), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    if (new URL(url).origin + new URL(url).pathname === 'https://alist.example.com/api/fs/link') {
      return new Response(JSON.stringify({
        code: 200,
        data: {
          url: 'https://tenant.sharepoint.com/file',
          header: {},
        },
      }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/wait') {
      const payload = JSON.parse(init.body);
      return createFairQueueWaitSseResponse({
        result: 'granted',
        queryToken: payload.requestId,
        invocationEpoch: 1,
        slotToken: 'slot-1',
        releaseOwnerRequired: true,
      }, { acceptedDeadlineMs: payload.deadlineMs });
    }
    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      return new Response(JSON.stringify({ result: 'ok' }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    if (url === 'https://tenant.sharepoint.com/file') {
      return new Response('ok', {
        status: 200,
        headers: { 'content-type': 'text/plain' },
      });
    }
    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  let response;
  const entries = await withFetchStub(fetchStub, () => captureConsole(async () => {
    response = await worker.fetch(await buildSignedWorkerRequest('/downloads/managed-success.bin'), buildControllerEnv(), {});
  }));

  assert.equal(response.status, 200, await response.text());
  assert.equal(terminalEntries(entries, 'fq_unavailable').length, 0);
  assert.equal(terminalEntries(entries, 'cq_unavailable').length, 0);
  const releaseStart = entries.find((entry) => entry.text.includes('[FQ] release_start'));
  const releaseResult = entries.find((entry) => entry.text.includes('[FQ] release_result'));
  assert.match(releaseStart?.text || '', /releaseKind=after_use/);
  assert.match(releaseResult?.text || '', /releaseKind=after_use/);
});

test('worker.fetch does not log alist_api_error when auth refresh ignores the error response', async () => {
  const bootstrap = buildBootstrap();
  let alistCalls = 0;
  const fetchStub = async (input) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url === CONTROLLER_BOOTSTRAP_URL) {
      return new Response(JSON.stringify(bootstrap), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    if (new URL(url).origin + new URL(url).pathname === 'https://alist.example.com/api/fs/link') {
      alistCalls += 1;
      if (alistCalls === 1) {
        return new Response(JSON.stringify({
          code: 200,
          data: {
            url: 'https://tenant.sharepoint.com/file',
            header: {},
          },
        }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      }
      return new Response('temporary upstream error', {
        status: 500,
        headers: { 'content-type': 'text/plain' },
      });
    }
    if (url === 'https://tenant.sharepoint.com/file') {
      return new Response('unauthorized', {
        status: 401,
        headers: { 'content-type': 'text/plain' },
      });
    }
    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  let response;
  const entries = await withFetchStub(fetchStub, () => captureConsole(async () => {
    response = await worker.fetch(await buildSignedWorkerRequest('/downloads/auth-refresh.bin'), buildControllerEnv(), {});
  }));

  assert.equal(response.status, 401, await response.text());
  assert.equal(terminalEntries(entries, 'alist_api_error').length, 0);
});

test('worker.fetch logs distinct terminal reason when refreshed upstream auth retry is exhausted', async () => {
  const bootstrap = buildBootstrap();
  let alistCalls = 0;
  const fetchStub = async (input) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url === CONTROLLER_BOOTSTRAP_URL) {
      return new Response(JSON.stringify(bootstrap), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    if (new URL(url).origin + new URL(url).pathname === 'https://alist.example.com/api/fs/link') {
      alistCalls += 1;
      return new Response(JSON.stringify({
        code: 200,
        data: {
          url: `https://tenant.sharepoint.com/file-${alistCalls}`,
          header: {},
        },
      }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    if (url === 'https://tenant.sharepoint.com/file-1' || url === 'https://tenant.sharepoint.com/file-2') {
      return new Response('unauthorized', {
        status: 401,
        headers: { 'content-type': 'text/plain' },
      });
    }
    throw new Error(`Unexpected fetch URL in test: ${url}`);
  };

  let response;
  const entries = await withFetchStub(fetchStub, () => captureConsole(async () => {
    response = await worker.fetch(await buildSignedWorkerRequest('/downloads/auth-retry-exhausted.bin'), buildControllerEnv(), {});
  }));

  assert.equal(response.status, 401, await response.text());
  assert.equal(alistCalls, 2);
  assert.ok(terminalEntries(entries, 'upstream_auth_retry_exhausted').some((entry) => /status=401/.test(entry.text)));
  assert.equal(terminalEntries(entries, 'upstream_generated_4xx').filter((entry) => /status=401/.test(entry.text)).length, 0);
});

test('worker.fetch logs terminal response for missing download payload', async () => {
  const bootstrap = buildBootstrap();
  const request = new Request('https://worker.example.com/download/file.bin', {
    headers: {
      origin: 'https://landing.example.com',
      'CF-Connecting-IP': '192.0.2.10',
    },
  });

  let response;
  const entries = await withFetchStub(buildBootstrapFetch(bootstrap), () => captureConsole(async () => {
    response = await worker.fetch(request, buildControllerEnv(), {});
  }));

  assert.equal(response.status, 401);
  assert.ok(terminalEntries(entries, 'payload_missing').some((entry) => /status=401/.test(entry.text)));
});

test('worker.fetch logs terminal response for internal API non-content response', async () => {
  const request = new Request('https://worker.example.com/api/v0/missing', {
    headers: { authorization: 'Bearer internal-token' },
  });

  let response;
  const entries = await captureConsole(async () => {
    response = await worker.fetch(request, { INTERNAL_API_TOKEN: 'internal-token' }, {});
  });

  assert.equal(response.status, 404);
  assert.ok(terminalEntries(entries, 'internal_api_response').some((entry) => /status=404/.test(entry.text)));
});

test('worker.fetch logs terminal response for inner auth rejection', async () => {
  const request = new Request('https://worker.example.com/download/file.bin');

  let response;
  const entries = await captureConsole(async () => {
    response = await worker.fetch(request, buildControllerEnv({ INNER_AUTH_SECRET: 'inner-secret' }), {});
  });

  assert.equal(response.status, 403);
  assert.equal(await response.text(), 'Forbidden');
  assert.ok(terminalEntries(entries, 'inner_auth_rejected').some((entry) => /status=403/.test(entry.text)));
});

test('worker.fetch logs terminal response for unavailable controller state', async () => {
  const request = new Request('https://worker.example.com/download/file.bin');

  let response;
  const entries = await captureConsole(async () => {
    response = await worker.fetch(request, {}, {});
  });

  assert.equal(response.status, 503);
  assert.ok(terminalEntries(entries, 'controller_state_unavailable').some((entry) => /status=503/.test(entry.text)));
});

test('worker.fetch logs terminal response for prohibited source', async () => {
  const bootstrap = buildBootstrap({
    common: {
      workerAddresses: ['https://other-worker.example.com'],
    },
  });
  const request = new Request('https://worker.example.com/download/file.bin');

  let response;
  const entries = await withFetchStub(buildBootstrapFetch(bootstrap), () => captureConsole(async () => {
    response = await worker.fetch(request, buildControllerEnv(), {});
  }));

  assert.equal(response.status, 403);
  assert.ok(terminalEntries(entries, 'prohibited_source').some((entry) => /status=403/.test(entry.text)));
});

test('worker.fetch logs terminal response for IPv6 block', async () => {
  const bootstrap = buildBootstrap();
  const request = new Request('https://worker.example.com/download/file.bin', {
    headers: {
      'CF-Connecting-IP': '2001:db8::1',
      origin: 'https://landing.example.com',
    },
  });

  let response;
  const entries = await withFetchStub(buildBootstrapFetch(bootstrap), () => captureConsole(async () => {
    response = await worker.fetch(request, buildControllerEnv(), {});
  }));

  assert.equal(response.status, 403);
  assert.deepEqual(await response.json(), {
    code: 403,
    message: 'ipv6 access is prohibited',
  });
  assert.ok(terminalEntries(entries, 'ipv6_blocked').some((entry) => /status=403/.test(entry.text)));
});

test('worker.fetch logs terminal response for OPTIONS preflight', async () => {
  const bootstrap = buildBootstrap();
  const request = new Request('https://worker.example.com/download/file.bin', {
    method: 'OPTIONS',
    headers: {
      origin: 'https://landing.example.com',
    },
  });

  let response;
  const entries = await withFetchStub(buildBootstrapFetch(bootstrap), () => captureConsole(async () => {
    response = await worker.fetch(request, buildControllerEnv(), {});
  }));

  assert.equal(response.status, 204);
  assert.equal(await response.text(), '');
  assert.equal(response.headers.get('Access-Control-Allow-Methods'), 'GET, HEAD, OPTIONS');
  assert.ok(terminalEntries(entries, 'options_preflight').some((entry) => /status=204/.test(entry.text)));
});

test('worker.fetch logs terminal response for top-level catch-to-500', async () => {
  const bootstrap = buildBootstrap({
    common: {
      tokenHmacKey: '',
    },
  });
  const request = new Request('https://worker.example.com/download/file.bin');

  let response;
  const entries = await withFetchStub(buildBootstrapFetch(bootstrap), () => captureConsole(async () => {
    response = await worker.fetch(request, buildControllerEnv(), {});
  }));

  assert.equal(response.status, 500);
  assert.ok(terminalEntries(entries, 'top_level_exception').some((entry) => /status=500/.test(entry.text)));
});
