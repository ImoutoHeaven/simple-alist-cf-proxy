import { test } from 'node:test';
import assert from 'node:assert/strict';
import { unifiedCheck } from '../src/unified-check.js';

const buildUnifiedCheckRow = () => ({
  cache_link_data: null,
  cache_timestamp: null,
  cache_hostname_hash: null,
  rate_access_count: 0,
  rate_last_window_time: Math.floor(Date.now() / 1000),
  rate_block_until: null,
  throttle_record_exists: false,
  throttle_state: null,
  throttle_open_until: null,
  throttle_reason: null,
  throttle_version: null,
  throttle_last_error_code: null,
  active_last_access_time: null,
  active_total_access_count: null,
});

test('unifiedCheck sends cacheEnabled and throttleHostnameHash', async () => {
  const originalFetch = globalThis.fetch;
  let capturedBody = null;

  globalThis.fetch = async (_url, options) => {
    capturedBody = JSON.parse(options.body);
    return {
      ok: true,
      json: async () => [buildUnifiedCheckRow()],
    };
  };

  try {
    await unifiedCheck('/test/path', '192.0.2.1', {
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
      cacheEnabled: false,
      throttleHostnameHash: 'throttle-hash',
    });

    assert.equal(capturedBody.p_cache_enabled, false);
    assert.equal(capturedBody.p_throttle_hostname_hash, 'throttle-hash');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('unifiedCheck sends the canonical breaker lookup payload', async () => {
  const originalFetch = globalThis.fetch;
  let capturedBody = null;

  globalThis.fetch = async (_url, options) => {
    capturedBody = JSON.parse(options.body);
    return {
      ok: true,
      json: async () => [buildUnifiedCheckRow()],
    };
  };

  try {
    await unifiedCheck('/test/path', '192.0.2.1', {
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
      cacheEnabled: false,
    });

    assert.equal(capturedBody.p_throttle_hostname_hash, null);
    assert.deepEqual(Object.keys(capturedBody).sort(), [
      'p_block_seconds',
      'p_cache_enabled',
      'p_cache_table_name',
      'p_cache_ttl',
      'p_idle_timeout',
      'p_ip_hash',
      'p_ip_range',
      'p_last_active_table_name',
      'p_limit',
      'p_now',
      'p_path_hash',
      'p_ratelimit_table_name',
      'p_throttle_hostname_hash',
      'p_window_seconds',
    ]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('unifiedCheck returns open breaker snapshot fields', async () => {
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async () => ({
    ok: true,
    json: async () => [{
      ...buildUnifiedCheckRow(),
      throttle_record_exists: true,
      throttle_state: 'open',
      throttle_open_until: 173,
      throttle_reason: 'http_429',
      throttle_version: 9,
      throttle_last_error_code: 429,
    }],
  });

  try {
    const result = await unifiedCheck('/test/path', '192.0.2.1', {
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
      cacheEnabled: false,
    });

    assert.deepEqual(result.throttle, {
      recordExists: true,
      state: 'open',
      openUntil: 173,
      reason: 'http_429',
      version: 9,
      lastErrorCode: 429,
    });
    assert.equal(Object.hasOwn(result.throttle, 'status'), false);
    assert.equal(Object.hasOwn(result.throttle, 'retryAfter'), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('unifiedCheck preserves the half_open breaker state', async () => {
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async () => ({
    ok: true,
    json: async () => [{
      ...buildUnifiedCheckRow(),
      throttle_record_exists: true,
      throttle_state: 'half_open',
      throttle_open_until: 188,
      throttle_reason: 'http_429',
      throttle_version: 12,
      throttle_last_error_code: 429,
    }],
  });

  try {
    const result = await unifiedCheck('/test/path', '192.0.2.1', {
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
      cacheEnabled: false,
    });

    assert.deepEqual(result.throttle, {
      recordExists: true,
      state: 'half_open',
      openUntil: 188,
      reason: 'http_429',
      version: 12,
      lastErrorCode: 429,
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('unifiedCheck preserves the closed breaker state', async () => {
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async () => ({
    ok: true,
    json: async () => [{
      ...buildUnifiedCheckRow(),
      throttle_record_exists: true,
      throttle_state: 'closed',
      throttle_open_until: null,
      throttle_reason: null,
      throttle_version: 11,
      throttle_last_error_code: null,
    }],
  });

  try {
    const result = await unifiedCheck('/test/path', '192.0.2.1', {
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
      cacheEnabled: false,
    });

    assert.deepEqual(result.throttle, {
      recordExists: true,
      state: 'closed',
      openUntil: null,
      reason: null,
      version: 11,
      lastErrorCode: null,
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('unifiedCheck ignores breaker states outside closed open and half_open', async () => {
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async () => ({
    ok: true,
    json: async () => [{
      ...buildUnifiedCheckRow(),
      throttle_record_exists: true,
      throttle_state: 'unexpected_state',
      throttle_open_until: 188,
      throttle_reason: 'http_429',
      throttle_version: 12,
      throttle_last_error_code: 429,
    }],
  });

  try {
    const result = await unifiedCheck('/test/path', '192.0.2.1', {
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
      cacheEnabled: false,
    });

    assert.equal(result.throttle.state, null);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
