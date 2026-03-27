import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { encryptBindingPayload } from '../src/origin-binding.js';
import worker, { __fairQueueTestHooks } from '../src/worker.js';

const { resolveConfig, resolveAdmissionMode } = __fairQueueTestHooks;

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

const buildRuntimeBootstrap = ({ fairQueueHostPatterns = [], throttleHostPatterns = [] } = {}) => ({
  configVersion: 'task1-admission-mode',
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
        hostPatterns: throttleHostPatterns,
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
    fairQueue: {
      enabled: fairQueueHostPatterns.length > 0,
      hostPatterns: fairQueueHostPatterns,
      slotHandlerUrl: 'https://slot-handler.example.test',
      slotHandlerAuthKey: 'slot-secret',
      slotHandlerAuthHeader: 'X-FQ-Auth',
    },
  },
});

const buildEnv = () => ({
  CONTROLLER_URL: 'https://controller.example.test',
  CONTROLLER_API_TOKEN: 'controller-token',
  ENV: 'test',
  ROLE: 'download',
  INSTANCE_ID: 'worker-1',
  BOOTSTRAP_CACHE_MODE: 'direct',
});

const createModeHarness = ({ fairQueueHostPatterns = [], throttleHostPatterns = [] } = {}) => {
  const bootstrap = buildRuntimeBootstrap({ fairQueueHostPatterns, throttleHostPatterns });
  const config = resolveConfig({}, bootstrap, { download: {} });
  return {
    bootstrap,
    config,
  };
};

const runModeScenario = async ({
  fairQueueHostPatterns = [],
  throttleHostPatterns = [],
  slotHandlerResponse = {
    result: 'granted',
    queryToken: 'query-mode-default',
    invocationEpoch: 1,
    slotToken: 'slot-1',
  },
} = {}) => {
  const { bootstrap } = createModeHarness({ fairQueueHostPatterns, throttleHostPatterns });
  const calls = {
    acquire: 0,
    release: 0,
    snapshot: 0,
    authorize: 0,
    report: 0,
  };
  const acquireBodies = [];
  const authorizeBodies = [];
  const reportBodies = [];
  const releaseBodies = [];
  const waitUntilPromises = [];
  const originalFetch = globalThis.fetch;

  delete globalThis.bootstrapCache;
  __fairQueueTestHooks.clearOverloadedByHost?.();

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url === 'https://controller.example.test/api/v0/bootstrap') {
      return createJsonResponse(bootstrap);
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
      calls.snapshot += 1;
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 1,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt') {
      calls.authorize += 1;
      authorizeBodies.push(JSON.parse(init.body));
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
      calls.report += 1;
      reportBodies.push(JSON.parse(init.body));
      return createJsonResponse([{
        STATE: 'closed',
        OPEN_UNTIL: null,
        OPEN_REASON: null,
        VERSION: 3,
        LAST_ERROR_CODE: null,
      }]);
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/acquire') {
      calls.acquire += 1;
      acquireBodies.push(JSON.parse(init.body));
      return createJsonResponse(
        typeof slotHandlerResponse === 'function'
          ? await slotHandlerResponse({ calls, acquireBodies, authorizeBodies, reportBodies, releaseBodies })
          : slotHandlerResponse
      );
    }

    if (url === 'https://slot-handler.example.test/api/v1/fairqueue/release') {
      calls.release += 1;
      releaseBodies.push(JSON.parse(init.body));
      return createJsonResponse({ result: 'ok' });
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
    const response = await worker.fetch(await buildSignedWorkerRequest(), buildEnv(), {
      waitUntil(promise) {
        waitUntilPromises.push(promise);
      },
    });
    await Promise.allSettled(waitUntilPromises);
    return {
      response,
      calls,
      acquireBodies,
      authorizeBodies,
      reportBodies,
      releaseBodies,
    };
  } finally {
    globalThis.fetch = originalFetch;
    delete globalThis.bootstrapCache;
    __fairQueueTestHooks.clearOverloadedByHost?.();
  }
};

test('none mode does not call slot-handler or breaker RPCs', async () => {
  const { config } = createModeHarness();

  assert.equal(typeof resolveAdmissionMode, 'function');
  assert.equal(resolveAdmissionMode(config, 'tenant.sharepoint.com'), 'none');

  const { response, calls } = await runModeScenario();
  assert.equal(response.status, 200);
  assert.equal(calls.acquire, 0);
  assert.equal(calls.authorize, 0);
  assert.equal(calls.report, 0);
});

test('breaker_only calls authorize/report but never slot-handler', async () => {
  const { config } = createModeHarness({
    throttleHostPatterns: ['*.sharepoint.com'],
  });

  assert.equal(typeof resolveAdmissionMode, 'function');
  assert.equal(resolveAdmissionMode(config, 'tenant.sharepoint.com'), 'breaker_only');

  const { response, calls } = await runModeScenario({
    throttleHostPatterns: ['*.sharepoint.com'],
  });
  assert.equal(response.status, 200);
  assert.equal(calls.acquire, 0);
  assert.equal(calls.authorize, 1);
  assert.equal(calls.report, 1);
});

test('queue_only calls slot-handler but never breaker RPCs', async () => {
  const { config } = createModeHarness({
    fairQueueHostPatterns: ['*.sharepoint.com'],
  });

  assert.equal(typeof resolveAdmissionMode, 'function');
  assert.equal(resolveAdmissionMode(config, 'tenant.sharepoint.com'), 'queue_only');

  const { response, calls } = await runModeScenario({
    fairQueueHostPatterns: ['*.sharepoint.com'],
  });
  assert.equal(response.status, 200);
  assert.equal(calls.acquire, 1);
  assert.equal(calls.authorize, 0);
  assert.equal(calls.report, 0);
});

test('queue_breaker uses slot-handler READY attempt tokens and skips authorize RPC', async () => {
  const { config } = createModeHarness({
    fairQueueHostPatterns: ['*.sharepoint.com'],
    throttleHostPatterns: ['*.sharepoint.com'],
  });

  assert.equal(typeof resolveAdmissionMode, 'function');
  assert.equal(resolveAdmissionMode(config, 'tenant.sharepoint.com'), 'queue_breaker');

  const { response, calls, reportBodies } = await runModeScenario({
    fairQueueHostPatterns: ['*.sharepoint.com'],
    throttleHostPatterns: ['*.sharepoint.com'],
    slotHandlerResponse: {
      result: 'granted',
      queryToken: 'query-mode-queue-breaker',
      invocationEpoch: 1,
      slotToken: 'slot-1',
      meta: {
        attemptVersion: 7,
        attemptTicket: 2,
      },
    },
  });

  assert.equal(response.status, 200);
  assert.equal(calls.acquire, 1);
  assert.equal(calls.snapshot, 0);
  assert.equal(calls.authorize, 0);
  assert.equal(calls.report, 1);
  assert.equal(reportBodies[0].p_attempt_version, 7);
  assert.equal(reportBodies[0].p_attempt_ticket, 2);
});

test('mode routing depends on hostname pattern matches, not global subsystem disable', () => {
  const { config } = createModeHarness({
    fairQueueHostPatterns: ['*.sharepoint.com'],
    throttleHostPatterns: ['*.office.com'],
  });

  assert.equal(config.fairQueueEnabled, true);
  assert.equal(config.throttleEnabled, true);
  assert.equal(resolveAdmissionMode(config, 'tenant.sharepoint.com'), 'queue_only');
  assert.equal(resolveAdmissionMode(config, 'files.office.com'), 'breaker_only');
  assert.equal(resolveAdmissionMode(config, 'example.net'), 'none');
});

test('queue_breaker has no post-slot authorize fallback branch', async () => {
  const source = await readFile(new URL('../src/worker.js', import.meta.url), 'utf8');

  assert.equal(
    /requestAdmissionMode === 'queue_breaker'[\s\S]*authorizeBreakerAttemptIfNeeded\(requestHostname\)/.test(source),
    false,
  );
});
