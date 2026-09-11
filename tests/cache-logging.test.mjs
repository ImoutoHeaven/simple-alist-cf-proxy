import { test } from 'node:test';
import assert from 'node:assert/strict';
import { finishCacheRefresh, getCacheState } from '../src/cache/custom-pg-rest.js';

const captureConsole = async (callback) => {
  const originalLog = console.log;
  const originalWarn = console.warn;
  const originalError = console.error;
  const entries = [];

  console.log = (...args) => {
    entries.push(args.map(String).join(' '));
  };
  console.warn = (...args) => {
    entries.push(args.map(String).join(' '));
  };
  console.error = (...args) => {
    entries.push(args.map(String).join(' '));
  };

  try {
    await callback();
    return entries.join('\n');
  } finally {
    console.log = originalLog;
    console.warn = originalWarn;
    console.error = originalError;
  }
};

const assertPersistenceLogsAreSanitized = (allLogs) => {
  assert.doesNotMatch(allLogs, /Authorization:\s*Bearer|Bearer\s+leaked/i);
  assert.doesNotMatch(allLogs, /203\.0\.113\.9(?:\/32)?/);
  assert.doesNotMatch(allLogs, /https:\/\/signed\.example\.test\/download\?[^\s]+/);
  assert.doesNotMatch(allLogs, /payload=secret|token=secret|signature=secret/i);
  assert.doesNotMatch(allLogs, /RAW_POSTGREST_BODY_MARKER/);
  assert.doesNotMatch(allLogs, /MALFORMED_JSON_BODY_MARKER/);
  assert.ok(allLogs.includes('[Cache]') || allLogs.includes('[RateLimit]') || allLogs.includes('[CleanupScheduler]') || allLogs.includes('[Throttle]'));
};

const cacheConfig = {
  postgrestUrl: 'https://postgrest.example.test',
  verifyHeader: ['X-Verify'],
  verifySecret: ['secret'],
  linkTTL: 300,
  cleanupProbability: 1,
  tableName: 'DOWNLOAD_CACHE_TABLE',
};

test('cache RPC logs do not expose sensitive values', async () => {
  const originalFetch = globalThis.fetch;
  let fetchCalls = 0;

  globalThis.fetch = async (url, init = {}) => {
    fetchCalls += 1;

    if (url === 'https://postgrest.example.test/rpc/download_get_cache_state') {
      return new Response(
        'MALFORMED_JSON_BODY_MARKER Authorization: Bearer leaked',
        { status: 200, headers: { 'content-type': 'application/json' } },
      );
    }
    if (url === 'https://postgrest.example.test/rpc/download_finish_cache_refresh') {
      return new Response(
        'RAW_POSTGREST_BODY_MARKER Authorization: Bearer leaked for 203.0.113.9/32 at https://signed.example.test/download?payload=secret&token=secret&signature=secret',
        { status: 500 },
      );
    }

    throw new Error(`Unexpected fetch URL in test: ${url} call=${fetchCalls} method=${init.method}`);
  };

  try {
    const allLogs = await captureConsole(async () => {
      await assert.rejects(getCacheState('/private/file.bin', cacheConfig));
      await assert.rejects(
        finishCacheRefresh('/private/file.bin', '11111111-1111-4111-8111-000000000001', {
          linkData: {
            url: 'https://signed.example.test/download?payload=secret&token=secret&signature=secret',
            authorization: 'Bearer leaked',
          },
        }, cacheConfig),
      );
    });

    assertPersistenceLogsAreSanitized(allLogs);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
