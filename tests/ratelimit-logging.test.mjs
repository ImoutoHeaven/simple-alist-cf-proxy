import { test } from 'node:test';
import assert from 'node:assert/strict';
import { reportBreakerSample } from '../src/cache/throttle-custom-pg-rest.js';
import { scheduleAllCleanups } from '../src/cleanup-scheduler.js';
import { checkRateLimit } from '../src/ratelimit/custom-pg-rest.js';

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
  assert.ok(allLogs.includes('[Cache]') || allLogs.includes('[RateLimit]') || allLogs.includes('[CleanupScheduler]') || allLogs.includes('[Throttle]'));
};

const createJsonResponse = (payload, init = {}) => new Response(JSON.stringify(payload), {
  status: init.status ?? 200,
  headers: {
    'content-type': 'application/json',
    ...(init.headers || {}),
  },
});

test('rate limit fail-open and cleanup scheduler logs do not expose sensitive values', async () => {
  const originalFetch = globalThis.fetch;
  const originalRandom = Math.random;
  const waitUntilPromises = [];

  Math.random = () => 0;
  globalThis.fetch = async (url) => {
    if (url === 'https://postgrest.example.test/rpc/download_upsert_rate_limit') {
      return new Response(
        'RAW_POSTGREST_BODY_MARKER Authorization: Bearer leaked for 203.0.113.9/32 at https://signed.example.test/download?payload=secret&token=secret&signature=secret',
        { status: 500 },
      );
    }

    if (String(url).startsWith('https://postgrest.example.test/DOWNLOAD_CACHE_TABLE?TIMESTAMP=lt.')) {
      return new Response(
        'RAW_POSTGREST_BODY_MARKER Authorization: Bearer leaked for 203.0.113.9/32 at https://signed.example.test/download?payload=secret&token=secret&signature=secret',
        { status: 500 },
      );
    }

    return createJsonResponse([], { headers: { 'content-range': '*/0' } });
  };

  try {
    const allLogs = await captureConsole(async () => {
      await checkRateLimit('203.0.113.9', {
        postgrestUrl: 'https://postgrest.example.test',
        verifyHeader: ['X-Verify'],
        verifySecret: ['secret'],
        windowTimeSeconds: 60,
        limit: 1,
        pgErrorHandle: 'fail-open',
      });

      await scheduleAllCleanups({
        dbMode: 'custom-pg-rest',
        cleanupPercentage: 100,
        cacheEnabled: true,
        cacheConfig: {
          postgrestUrl: 'https://postgrest.example.test',
          verifyHeader: ['X-Verify'],
          verifySecret: ['secret'],
          linkTTL: 300,
          tableName: 'DOWNLOAD_CACHE_TABLE',
        },
      }, {}, {
        waitUntil(promise) {
          waitUntilPromises.push(promise);
        },
      });
      await Promise.allSettled(waitUntilPromises);
    });

    assertPersistenceLogsAreSanitized(allLogs);
  } finally {
    globalThis.fetch = originalFetch;
    Math.random = originalRandom;
  }
});

test('breaker sample report logs do not expose sensitive values', async () => {
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async () => createJsonResponse([{
    STATE: 'open',
    OPEN_UNTIL: 123,
    OPEN_REASON: 'Authorization: Bearer leaked for 203.0.113.9/32 at https://signed.example.test/download?payload=secret&token=secret&signature=secret',
    VERSION: 5,
    LAST_ERROR_CODE: 503,
  }]);

  try {
    const allLogs = await captureConsole(async () => {
      await reportBreakerSample('tenant.sharepoint.com', {
        sample: 1,
        statusCode: 503,
      }, {
        postgrestUrl: 'https://postgrest.example.test',
        verifyHeader: ['X-Verify'],
        verifySecret: ['secret'],
      });
    });

    assertPersistenceLogsAreSanitized(allLogs);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
