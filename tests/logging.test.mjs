import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import { fetchControllerState } from '../src/controller-adapter.js';
import { handleInternalApiIfAny } from '../src/internal-api.js';
import {
  bindWaitUntil,
  logEvent,
  sanitizeLogStructuredValue,
  sanitizeLogValue,
} from '../src/logging.js';

const supportRuntimeFiles = [
  'src/origin-binding.js',
  'src/internal-api.js',
  'src/controller-adapter.js',
];

function captureConsole(callback) {
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

  return Promise.resolve()
    .then(callback)
    .then(
      () => entries,
      (error) => {
        error.entries = entries;
        throw error;
      },
    )
    .finally(() => {
      console.log = originalLog;
      console.warn = originalWarn;
      console.error = originalError;
    });
}

test('support runtime modules route logs through shared logging helper', () => {
  const runtimeConsoleMatchesOutsideLogging = supportRuntimeFiles.flatMap((filePath) => {
    const source = readFileSync(new URL(`../${filePath}`, import.meta.url), 'utf8');
    const matches = source.match(/console\.(?:log|warn|error)/g) || [];
    return matches.map((match) => `${filePath}:${match}`);
  });

  assert.equal(runtimeConsoleMatchesOutsideLogging.length, 0);
});

test('internal API D1 clear failure logs a sanitized error message', async () => {
  const waits = [];
  const entries = await captureConsole(async () => {
    const response = await handleInternalApiIfAny(
      new Request('https://worker.example.test/api/v0/refresh', {
        method: 'POST',
        headers: { authorization: 'Bearer control-token' },
        body: JSON.stringify({ targets: ['bootstrap'] }),
      }),
      {
        INTERNAL_API_TOKEN: 'control-token',
        ENV: 'prod',
        ROLE: 'edge',
        CACHE_D1: {
          prepare() {
            return {
              bind() {
                return {};
              },
            };
          },
          batch() {
            throw new Error('D1 batch failed Authorization: Bearer leaked token=secret');
          },
        },
      },
      {
        waitUntil(promise) {
          waits.push(promise);
        },
      },
    );

    assert.equal(response.status, 204);
    await Promise.all(waits);
  });
  const logText = entries.map((entry) => entry.text).join('\n');

  assert.match(logText, /\[InternalApi\] d1-cache-clear-failed/);
  assert.match(logText, /D1 batch failed/);
  assert.doesNotMatch(logText, /Authorization:\s*Bearer/i);
  assert.doesNotMatch(logText, /Bearer\s+leaked/i);
  assert.doesNotMatch(logText, /token=secret/i);
});

test('controller fetch failure logs a sanitized error message and preserves null return', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({
    configVersion: 'v1',
    global: { defaultProfileId: 'Bearer leaked token=secret' },
    pathProfiles: [],
  }), { status: 200, headers: { 'content-type': 'application/json' } });

  try {
    const entries = await captureConsole(async () => {
      const state = await fetchControllerState(
        new Request('https://worker.example.test/download/file.txt'),
        {
          CONTROLLER_URL: 'https://controller.example.test',
          CONTROLLER_API_TOKEN: 'controller-token',
          BOOTSTRAP_CACHE_MODE: 'direct',
          ENV: 'prod',
          ROLE: 'edge',
          INSTANCE_ID: 'instance-1',
        },
      );

      assert.equal(state, null);
    });
    const logText = entries.map((entry) => entry.text).join('\n');

    assert.match(logText, /\[Controller\] fetch-failed/);
    assert.match(logText, /Unknown path profile from controller bootstrap/);
    assert.doesNotMatch(logText, /Bearer\s+leaked/i);
    assert.doesNotMatch(logText, /token=secret/i);
  } finally {
    globalThis.fetch = originalFetch;
    globalThis.bootstrapCache = null;
  }
});

test('sanitizeLogValue removes sensitive strings from arbitrary log text', () => {
  const logText = sanitizeLogValue(`
    Authorization: Bearer leaked
    Bearer leaked
    client=203.0.113.9/32
    payload=secret token=secret signature=secret verifySecret=secret
    download https://signed.example.test/download?token=secret&signature=secret
  `);

  assert.doesNotMatch(logText, /Bearer\s+leaked/i);
  assert.doesNotMatch(logText, /Authorization:\s*Bearer/i);
  assert.doesNotMatch(logText, /203\.0\.113\.9(?:\/32)?/);
  assert.doesNotMatch(logText, /payload=secret|token=secret|signature=secret|verifySecret=secret/i);
  assert.doesNotMatch(logText, /\?[^\s]*/);
  assert.match(logText, /\[redacted\]/);
});

test('sanitizeLogValue removes IPv6 and IPv6 CIDR strings from arbitrary log text', () => {
  const logText = sanitizeLogValue(`
    client=2001:db8::1/60
    peer=[2001:db8::2]:443
    range=[2001:db8::3]/64
    remote=2001:db8:85a3::8a2e:370:7334
    message=completed:ok
  `);

  assert.doesNotMatch(logText, /2001:db8::1\/60/i);
  assert.doesNotMatch(logText, /\[2001:db8::2\]/i);
  assert.doesNotMatch(logText, /\[2001:db8::3\]\/64/i);
  assert.doesNotMatch(logText, /\[redacted\]\/64/i);
  assert.doesNotMatch(logText, /2001:db8:85a3::8a2e:370:7334/i);
  assert.match(logText, /message=completed:ok/);
  assert.match(logText, /\[redacted\]/);
});

test('sanitizeLogValue strips query strings from bracketed IPv6 URLs', () => {
  const logText = sanitizeLogValue('url=http://[2001:db8::4]:8080/path?token=secret&signature=secret message=completed:ok');

  assert.doesNotMatch(logText, /2001:db8::4/i);
  assert.doesNotMatch(logText, /\?/);
  assert.doesNotMatch(logText, /token|signature/i);
  assert.match(logText, /\/path/);
  assert.match(logText, /message=completed:ok/);
  assert.match(logText, /\[redacted\]/);
});

test('sanitizeLogStructuredValue redacts sensitive fields and handles circular objects', () => {
  const value = {
    ok: true,
    cache_link_data: {
      url: 'https://signed.example.test/download?token=secret&signature=secret',
      authorization: 'Bearer leaked',
    },
    token: 'secret',
    ip: '203.0.113.9',
  };
  value.self = value;

  assert.doesNotThrow(() => sanitizeLogStructuredValue(value));
  const logText = sanitizeLogValue(value);

  assert.doesNotMatch(logText, /Bearer\s+leaked/i);
  assert.doesNotMatch(logText, /Authorization:\s*Bearer/i);
  assert.doesNotMatch(logText, /203\.0\.113\.9(?:\/32)?/);
  assert.doesNotMatch(logText, /payload=secret|token=secret|signature=secret|verifySecret=secret/i);
  assert.doesNotMatch(logText, /cache_link_data.*https:\/\/signed\.example/i);
  assert.doesNotMatch(logText, /\?[^\s]*/);
  assert.match(logText, /\[redacted\]/);
});

test('logEvent emits sanitized concise fields', async () => {
  const entries = await captureConsole(() => {
    logEvent('warn', 'LoggingTest', 'leak-check', {
      authorization: 'Bearer leaked',
      message: 'payload=secret token=secret signature=secret verifySecret=secret 203.0.113.9/32',
      cache_link_data: {
        url: 'https://signed.example.test/download?token=secret',
      },
    });
  });
  const logText = entries.map((entry) => entry.text).join('\n');

  assert.match(logText, /\[LoggingTest\] leak-check/);
  assert.doesNotMatch(logText, /Bearer\s+leaked/i);
  assert.doesNotMatch(logText, /Authorization:\s*Bearer/i);
  assert.doesNotMatch(logText, /203\.0\.113\.9(?:\/32)?/);
  assert.doesNotMatch(logText, /payload=secret|token=secret|signature=secret|verifySecret=secret/i);
  assert.doesNotMatch(logText, /cache_link_data.*https:\/\/signed\.example/i);
  assert.doesNotMatch(logText, /\?[^\s]*/);
  assert.match(logText, /\[redacted\]/);
});

test('sanitizeLogValue redacts sensitive JSON text', () => {
  const logText = sanitizeLogValue(JSON.stringify({
    cache_link_data: {
      url: 'https://signed.example.test/download?token=secret&signature=secret',
    },
    authorization: 'Bearer leaked',
  }));

  assert.doesNotMatch(logText, /Bearer\s+leaked/i);
  assert.doesNotMatch(logText, /Authorization:\s*Bearer/i);
  assert.doesNotMatch(logText, /cache_link_data.*https:\/\/signed\.example/i);
  assert.doesNotMatch(logText, /\?[^\s]*/);
  assert.match(logText, /\[redacted\]/);
});

test('bindWaitUntil logs bound, done, failed, and inline states while preserving rejection', async () => {
  const waits = [];
  const ctx = {
    waitUntil(promise) {
      waits.push(promise);
    },
  };

  const entries = await captureConsole(async () => {
    bindWaitUntil(ctx, Promise.resolve('ok'), 'WaitTest', 'background', { token: 'secret' });
    await waits.at(-1);

    const rejection = new Error('Authorization: Bearer leaked');
    const wrapped = bindWaitUntil(ctx, Promise.reject(rejection), 'WaitTest', 'background', {
      ip: '203.0.113.9/32',
    });
    await assert.rejects(waits.at(-1), rejection);
    await assert.rejects(wrapped, rejection);

    const inline = bindWaitUntil(null, Promise.resolve('inline-ok'), 'WaitTest', 'inline', {
      url: 'https://signed.example.test/download?token=secret',
    });
    assert.equal(await inline, 'inline-ok');
  });

  const logText = entries.map((entry) => entry.text).join('\n');
  assert.match(logText, /state=bound/);
  assert.match(logText, /state=done/);
  assert.match(logText, /state=failed/);
  assert.match(logText, /state=inline/);
  assert.doesNotMatch(logText, /Bearer\s+leaked/i);
  assert.doesNotMatch(logText, /Authorization:\s*Bearer/i);
  assert.doesNotMatch(logText, /203\.0\.113\.9(?:\/32)?/);
  assert.doesNotMatch(logText, /\?[^\s]*/);
});

test('bindWaitUntil contains malformed fields and preserves original promise settlement', async () => {
  const waits = [];
  const ctx = {
    waitUntil(promise) {
      waits.push(promise);
    },
  };
  const hostileFields = new Proxy({}, {
    ownKeys() {
      throw new Error('field enumeration leaked');
    },
  });
  const originalRejection = new Error('original failure');

  await captureConsole(async () => {
    assert.doesNotThrow(() => {
      bindWaitUntil(ctx, Promise.resolve('ok'), 'WaitTest', 'hostile', hostileFields);
    });
    assert.equal(await waits.at(-1), 'ok');

    const wrapped = bindWaitUntil(ctx, Promise.reject(originalRejection), 'WaitTest', 'hostile', hostileFields);
    await assert.rejects(waits.at(-1), originalRejection);
    await assert.rejects(wrapped, originalRejection);

    const inline = bindWaitUntil(null, Promise.resolve('inline-ok'), 'WaitTest', 'hostile-inline', hostileFields);
    assert.equal(await inline, 'inline-ok');
  });
});

test('bindWaitUntil propagates waitUntil exceptions after safe logging', async () => {
  const waitUntilFailure = new Error('waitUntil failed');
  const hostileFields = new Proxy({}, {
    ownKeys() {
      throw new Error('field enumeration leaked');
    },
  });
  const ctx = {
    waitUntil() {
      throw waitUntilFailure;
    },
  };

  await assert.rejects(
    captureConsole(() => bindWaitUntil(ctx, Promise.resolve('ok'), 'WaitTest', 'wait-until-throw', hostileFields)),
    waitUntilFailure,
  );
});
