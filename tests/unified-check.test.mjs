import { test } from 'node:test';
import assert from 'node:assert/strict';
import { unifiedCheck } from '../src/unified-check.js';

test('unifiedCheck sends cacheEnabled and throttleHostnameHash', async () => {
  const originalFetch = globalThis.fetch;
  let capturedBody = null;

  globalThis.fetch = async (_url, options) => {
    capturedBody = JSON.parse(options.body);
    return {
      ok: true,
      json: async () => [
        {
          cache_link_data: null,
          cache_timestamp: null,
          cache_hostname_hash: null,
          rate_access_count: 0,
          rate_last_window_time: Math.floor(Date.now() / 1000),
          rate_block_until: null,
          throttle_record_exists: false,
          throttle_is_protected: null,
          throttle_error_timestamp: null,
          throttle_error_code: null,
          active_last_access_time: null,
          active_total_access_count: null,
        },
      ],
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
