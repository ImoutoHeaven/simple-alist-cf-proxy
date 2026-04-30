import { test } from 'node:test';
import assert from 'node:assert/strict';
import { authorizeBreakerAttempt } from '../src/cache/throttle-custom-pg-rest.js';

const createJsonResponse = (payload) => new Response(JSON.stringify(payload), {
  status: 200,
  headers: { 'content-type': 'application/json' },
});

test('authorizeBreakerAttempt sends the full canonical half-open authorize tuple to SQL without adding ATTEMPT_VERSION', async () => {
  const originalFetch = globalThis.fetch;
  let rpcUrl = null;
  let rpcBody = null;
  const deadline = Math.floor(Date.now() / 1000) + 15;

  globalThis.fetch = async (url, init) => {
    rpcUrl = url;
    rpcBody = JSON.parse(init.body);
    return createJsonResponse([{
      STATE: 'half_open',
      OPEN_UNTIL: null,
      OPEN_REASON: 'http_429',
      VERSION: 10,
      LAST_ERROR_CODE: 429,
      HALF_OPEN_DEADLINE: deadline,
      ATTEMPT_GRANTED: true,
      ATTEMPT_TICKET: 2,
    }]);
  };

  try {
    const result = await authorizeBreakerAttempt('tenant.sharepoint.com', {
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
      openCapSeconds: 75,
      closeThresholdPercent: 19,
      halfOpenSuccessThreshold: 3,
      halfOpenCloseMode: 'or',
      halfOpenMaxProbeCount: 4,
      halfOpenMaxSeconds: 15,
      halfOpenTimeoutMode: 'partial-close',
    });

    assert.equal(rpcUrl, 'https://postgrest.example.test/rpc/download_authorize_breaker_attempt');
    assert.equal(rpcBody.p_half_open_max_probe_count, 4);
    assert.equal(rpcBody.p_half_open_max_seconds, 15);
    assert.equal(rpcBody.p_half_open_timeout_mode, 'partial-close');
    assert.equal(rpcBody.p_open_cap_seconds, 75);
    assert.equal(rpcBody.p_close_threshold_percent, 19);
    assert.equal(rpcBody.p_half_open_success_threshold, 3);
    assert.equal(rpcBody.p_half_open_close_mode, 'or');
    assert.ok(!Object.prototype.hasOwnProperty.call(rpcBody, 'p_attempt_version'));
    assert.deepEqual(Object.keys(rpcBody).sort(), [
      'p_close_threshold_percent',
      'p_half_open_close_mode',
      'p_half_open_max_probe_count',
      'p_half_open_max_seconds',
      'p_half_open_success_threshold',
      'p_half_open_timeout_mode',
      'p_hostname',
      'p_hostname_hash',
      'p_now',
      'p_open_cap_seconds',
    ]);
    assert.deepEqual(result, {
      recordExists: true,
      state: 'half_open',
      openUntil: null,
      reason: 'http_429',
      version: 10,
      lastErrorCode: 429,
      halfOpenDeadline: deadline,
      attemptGranted: true,
      attemptTicket: 2,
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('authorizeBreakerAttempt defaults the full canonical half-open authorize tuple', async () => {
  const originalFetch = globalThis.fetch;
  let rpcBody = null;

  globalThis.fetch = async (_url, init) => {
    rpcBody = JSON.parse(init.body);
    return createJsonResponse([{
      STATE: 'closed',
      OPEN_UNTIL: null,
      OPEN_REASON: null,
      VERSION: 1,
      LAST_ERROR_CODE: null,
      HALF_OPEN_DEADLINE: null,
      ATTEMPT_GRANTED: false,
      ATTEMPT_TICKET: null,
    }]);
  };

  try {
    await authorizeBreakerAttempt('tenant.sharepoint.com', {
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
    });

    assert.equal(rpcBody.p_half_open_max_probe_count, 4);
    assert.equal(rpcBody.p_half_open_max_seconds, 15);
    assert.equal(rpcBody.p_half_open_timeout_mode, 'partial-close');
    assert.equal(rpcBody.p_open_cap_seconds, 60);
    assert.equal(rpcBody.p_close_threshold_percent, 15);
    assert.equal(rpcBody.p_half_open_success_threshold, 2);
    assert.equal(rpcBody.p_half_open_close_mode, 'and');
    assert.ok(!Object.prototype.hasOwnProperty.call(rpcBody, 'p_attempt_version'));
  } finally {
    globalThis.fetch = originalFetch;
  }
});
