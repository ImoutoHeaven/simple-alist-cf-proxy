import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  acquireCacheRefresh,
  cleanupExpiredCache,
  finishCacheRefresh,
  getCacheState,
} from '../src/cache/custom-pg-rest.js';
import { sha256Hash } from '../src/utils.js';

const POSTGREST_URL = 'https://postgrest.example.test';
const cacheConfig = {
  postgrestUrl: POSTGREST_URL,
  verifyHeader: ['X-Verify'],
  verifySecret: ['secret'],
  linkTTL: 300,
  tableName: 'DOWNLOAD_CACHE_TABLE',
};

const jsonResponse = (payload, status = 200) => new Response(JSON.stringify(payload), {
  status,
  headers: { 'content-type': 'application/json' },
});

const withFetchStub = async (stub, callback) => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = stub;
  try {
    return await callback();
  } finally {
    globalThis.fetch = originalFetch;
  }
};

const readyLink = {
  url: 'https://signed.example.test/download',
  header: { 'X-Upstream': 'value' },
  download: {
    provider: 'test',
    ticket: 'ticket-abcdefghijklmnopqrstuvwxyz',
    expires_at: Math.floor(Date.now() / 1000) + 300,
    report_success: false,
  },
};

test('cache adapter sends the read RPC and exposes a normalized ready state', async () => {
  let seenUrl;
  let seenInit;
  const state = await withFetchStub(async (url, init) => {
    seenUrl = url;
    seenInit = init;
    return jsonResponse([{
      result: 'ready',
      path_hash: await sha256Hash('/adapter/ready'),
      path: '/adapter/ready',
      link_data: JSON.stringify(readyLink),
      cache_timestamp: Math.floor(Date.now() / 1000),
      hostname_hash: 'host-hash',
      version: '11111111-1111-4111-8111-000000000001',
      lease_id: null,
      lease_until: null,
      invalid_version: null,
      retry_after: null,
      last_error_code: null,
      observed_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    }]);
  }, () => getCacheState('/adapter/ready', cacheConfig));

  assert.equal(seenUrl, `${POSTGREST_URL}/rpc/download_get_cache_state`);
  assert.equal(seenInit.method, 'POST');
  assert.equal(seenInit.headers['X-Verify'], 'secret');
  assert.deepEqual(JSON.parse(seenInit.body), {
    p_path_hash: await sha256Hash('/adapter/ready'),
    p_cache_ttl: 300,
    p_cache_table_name: 'DOWNLOAD_CACHE_TABLE',
  });
  assert.equal(state.result, 'ready');
  assert.deepEqual(state.linkData, readyLink);
  assert.equal(state.version, '11111111-1111-4111-8111-000000000001');
  assert.equal(state.hostnameHash, 'host-hash');
  assert.equal(state.leaseUntilMs, null);
  assert.equal(state.retryAfterSeconds, null);
});

test('cache adapter sends observed-version acquisition and conditional finish payloads', async () => {
  const observedVersion = '11111111-1111-4111-8111-000000000002';
  const leaseId = '11111111-1111-4111-8111-000000000003';
  const seen = [];
  const state = await withFetchStub(async (url, init) => {
    seen.push({ url, body: JSON.parse(init.body) });
    if (url.endsWith('/download_acquire_cache_refresh')) {
      return jsonResponse([{
        result: 'acquired',
        path_hash: 'path-hash',
        path: '/adapter/acquire',
        link_data: null,
        cache_timestamp: null,
        hostname_hash: null,
        version: observedVersion,
        lease_id: leaseId,
        lease_until: new Date(Date.now() + 180_000).toISOString(),
        invalid_version: observedVersion,
        retry_after: null,
        last_error_code: null,
        observed_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }]);
    }
    return jsonResponse([{
      result: 'committed',
      version: leaseId,
      invalid_version: null,
      retry_after: null,
      last_error_code: null,
      observed_at: new Date().toISOString(),
    }]);
  }, async () => {
    const acquired = await acquireCacheRefresh('/adapter/acquire', observedVersion, cacheConfig);
    assert.equal(acquired.result, 'acquired');
    assert.equal(acquired.leaseId, leaseId);
    assert.equal(acquired.invalidVersion, observedVersion);
    assert.ok(acquired.leaseUntilMs > Date.now());

    const finished = await finishCacheRefresh('/adapter/acquire', leaseId, {
      linkData: readyLink,
      path: '/adapter/acquire',
      hostnameHash: 'host-hash',
      failedVersion: observedVersion,
      errorCode: null,
    }, cacheConfig);
    assert.equal(finished.result, 'committed');
    assert.equal(finished.version, leaseId);
  });

  assert.equal(seen.length, 2);
  assert.deepEqual(seen[0].body, {
    p_path_hash: await sha256Hash('/adapter/acquire'),
    p_observed_version: observedVersion,
    p_cache_ttl: 300,
    p_cache_table_name: 'DOWNLOAD_CACHE_TABLE',
  });
  assert.deepEqual(seen[1].body, {
    p_path_hash: await sha256Hash('/adapter/acquire'),
    p_lease_id: leaseId,
    p_link_data: JSON.stringify(readyLink),
    p_path: '/adapter/acquire',
    p_hostname_hash: 'host-hash',
    p_failed_version: observedVersion,
    p_error_code: null,
    p_cache_table_name: 'DOWNLOAD_CACHE_TABLE',
  });
});

test('cache adapter shares ready-link validation with readers before finish publication', async () => {
  let calls = 0;
  await withFetchStub(async () => {
    calls += 1;
    return jsonResponse([{
      result: 'committed',
      version: '11111111-1111-4111-8111-000000000006',
      invalid_version: null,
      retry_after: null,
      last_error_code: null,
      observed_at: new Date().toISOString(),
    }]);
  }, async () => {
    await assert.rejects(
      finishCacheRefresh('/adapter/invalid-finish', '11111111-1111-4111-8111-000000000007', {
        linkData: {
          url: 'https://signed.example.test/download',
          header: null,
          download: {
            provider: 'test',
            expires_at: Math.floor(Date.now() / 1000) + 300,
            report_success: false,
          },
        },
      }, cacheConfig),
      (error) => error?.logFields?.operation === 'finish' && /incomplete ready link/i.test(error.message),
    );
    assert.equal(calls, 0);

    await assert.rejects(
      finishCacheRefresh('/adapter/invalid-finish-url', '11111111-1111-4111-8111-000000000008', {
        linkData: {
          ...readyLink,
          url: 'ftp://signed.example.test/download',
        },
      }, cacheConfig),
      (error) => error?.logFields?.operation === 'finish' && /incomplete ready link/i.test(error.message),
    );
    assert.equal(calls, 0);
  });

  const nullableHeaderLink = { ...readyLink, header: null };
  const finished = await withFetchStub(async () => jsonResponse([{
    result: 'committed',
    version: '11111111-1111-4111-8111-000000000007',
    invalid_version: null,
    retry_after: null,
    last_error_code: null,
    observed_at: new Date().toISOString(),
  }]), () => finishCacheRefresh('/adapter/nullable-header', '11111111-1111-4111-8111-000000000007', {
    linkData: nullableHeaderLink,
  }, cacheConfig));
  assert.equal(finished.result, 'committed');
});

test('cache adapter normalizes wait and backoff deadlines and keeps the public ready contract', async () => {
  let stateResult = 'wait';
  await withFetchStub(async (url) => {
    if (url.endsWith('/download_get_cache_state')) {
      return jsonResponse([{
        result: stateResult,
        path_hash: 'path-hash',
        path: '/adapter/wait',
        link_data: stateResult === 'ready' ? JSON.stringify(readyLink) : null,
        cache_timestamp: stateResult === 'ready' ? Math.floor(Date.now() / 1000) : null,
        hostname_hash: null,
        version: stateResult === 'ready' ? '11111111-1111-4111-8111-000000000004' : null,
        lease_id: stateResult === 'wait' ? '11111111-1111-4111-8111-000000000005' : null,
        lease_until: stateResult === 'wait' ? new Date(Date.now() + 5_000).toISOString() : null,
        invalid_version: null,
        retry_after: stateResult === 'backoff' ? new Date(Date.now() + 7_000).toISOString() : null,
        last_error_code: stateResult === 'backoff' ? 503 : null,
        observed_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }]);
    }
    throw new Error(`unexpected adapter URL: ${url}`);
  }, async () => {
    const waiting = await getCacheState('/adapter/wait', cacheConfig);
    assert.equal(waiting.result, 'wait');
    assert.ok(waiting.leaseUntilMs > Date.now());
    assert.equal(waiting.retryAfterSeconds, null);

    stateResult = 'backoff';
    const backoff = await getCacheState('/adapter/wait', cacheConfig);
    assert.equal(backoff.result, 'backoff');
    assert.ok(backoff.retryAfterSeconds >= 1);
    assert.equal(backoff.lastErrorCode, 503);

    stateResult = 'ready';
    const checked = await getCacheState('/adapter/wait', cacheConfig);
    assert.equal(checked.result, 'ready');
    assert.deepEqual(checked.linkData, readyLink);
  });

});

test('cache adapter rejects malformed state responses without echoing response content', async () => {
  await withFetchStub(async () => new Response(
    'MALFORMED_ADAPTER_RESPONSE_SECRET',
    { status: 200, headers: { 'content-type': 'application/json' } },
  ), async () => {
    await assert.rejects(
      getCacheState('/adapter/malformed', cacheConfig),
      (error) => {
        assert.match(error.message, /invalid JSON/i);
        assert.doesNotMatch(error.message, /MALFORMED_ADAPTER_RESPONSE_SECRET/);
        return true;
      },
    );
  });

  await withFetchStub(async () => jsonResponse([{ result: 'secret-state-value' }]), async () => {
    await assert.rejects(
      getCacheState('/adapter/unknown-state', cacheConfig),
      (error) => {
        assert.match(error.message, /unknown state/i);
        assert.doesNotMatch(error.message, /secret-state-value/);
        return true;
      },
    );
  });
});

test('cache adapter preserves response-body abort identity for success and error RPCs', async () => {
  const abortError = () => Object.assign(new Error('response body aborted'), { name: 'AbortError' });

  await withFetchStub(async () => ({
    ok: true,
    json: async () => {
      throw abortError();
    },
  }), async () => {
    await assert.rejects(
      getCacheState('/adapter/body-abort-success', cacheConfig),
      (error) => error.name === 'AbortError',
    );
  });

  await withFetchStub(async () => ({
    ok: false,
    status: 503,
    text: async () => {
      throw abortError();
    },
  }), async () => {
    await assert.rejects(
      acquireCacheRefresh('/adapter/body-abort-error', null, cacheConfig),
      (error) => error.name === 'AbortError',
    );
  });

  const controller = new AbortController();
  controller.abort();
  await withFetchStub(async () => ({
    ok: true,
    json: async () => {
      throw new SyntaxError('secret parser source');
    },
  }), async () => {
    await assert.rejects(
      getCacheState('/adapter/signal-abort', { ...cacheConfig, signal: controller.signal }),
      (error) => error.name === 'AbortError' && !/secret parser source/.test(error.message),
    );
  });
});

test('cache adapter accepts scalar cleanup results and treats cleanup failures as zero', async () => {
  let cleanupPayload = 7;
  const result = await withFetchStub(async (url) => {
    assert.equal(url, `${POSTGREST_URL}/rpc/download_cleanup_expired_cache`);
    return jsonResponse(cleanupPayload);
  }, () => cleanupExpiredCache(cacheConfig));
  assert.equal(result, 7);

  cleanupPayload = { deleted: 3 };
  const objectResult = await withFetchStub(async () => jsonResponse(cleanupPayload), () => cleanupExpiredCache(cacheConfig));
  assert.equal(objectResult, 3);

  const failedResult = await withFetchStub(async () => jsonResponse({ invalid: true }), () => cleanupExpiredCache(cacheConfig));
  assert.equal(failedResult, 0);
});
