import { test } from 'node:test';
import assert from 'node:assert/strict';
import { getBootstrapConfig } from '../src/controller-client.js';
import { reportBreakerSample } from '../src/cache/throttle-custom-pg-rest.js';
import { fetchControllerState } from '../src/controller-adapter.js';
import { __fairQueueTestHooks } from '../src/worker.js';

const { resolveConfig } = __fairQueueTestHooks;
const { resolveTicketStateConfig, shouldConsumeTicketResponse } = __fairQueueTestHooks;

const CURRENT_SCHEMA_EPOCH = 4;

const buildBootstrap = () => ({
  common: {
    tokenHmacKey: 'bootstrap-token',
    workerAddresses: ['https://worker.example.com'],
    landingWorkerAddresses: ['https://landing.example.com'],
  },
  download: {
    address: 'https://alist.example.com',
    throttleProfiles: {
      default: {
        hostPatterns: ['*.default.example'],
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
      sharepoint: {
        hostPatterns: ['*.sharepoint.com'],
        openCapSeconds: 75,
        openThresholdPercent: 35,
        closeThresholdPercent: 15,
        ewmaSpan: 11,
        consecutiveThreshold: 6,
        minSamplesBeforeEwmaOpen: 8,
        idleResetSeconds: 900,
        halfOpenSuccessThreshold: 2,
        halfOpenCloseMode: 'and',
        halfOpenMaxProbeCount: 4,
        halfOpenMaxSeconds: 15,
        halfOpenTimeoutMode: 'partial-close',
        protectHttpCodes: [429, 503],
      },
    },
  },
});

const buildControllerEnv = (overrides = {}) => ({
  CONTROLLER_URL: 'https://controller.example.test',
  CONTROLLER_API_TOKEN: 'controller-token',
  ENV: 'staging',
  ROLE: 'download',
  INSTANCE_ID: 'worker-1',
  ...overrides,
});

const buildDynamicBootstrap = () => ({
  ...buildBootstrap(),
  global: { defaultProfileId: 'download-default' },
  pathRules: [
    { profileId: 'download-default', pattern: '/**' },
  ],
  pathProfiles: [
    {
      id: 'download-default',
      dynamic: true,
      actions: {
        pathAction: ['asis'],
        throttleProfile: 'default',
      },
    },
  ],
});

const buildStaticBootstrap = (throttleProfile) => ({
  ...buildBootstrap(),
  global: { defaultProfileId: 'download-static' },
  pathRules: [
    { profileId: 'download-static', pattern: '/**' },
  ],
  pathProfiles: [
    {
      id: 'download-static',
      dynamic: false,
      actions: {
        pathAction: ['asis'],
        throttleProfile,
      },
    },
  ],
});

const buildStaticBootstrapWithoutThrottleProfile = () => ({
  ...buildBootstrap(),
  global: { defaultProfileId: 'download-static-implicit-default' },
  pathRules: [
    { profileId: 'download-static-implicit-default', pattern: '/**' },
  ],
  pathProfiles: [
    {
      id: 'download-static-implicit-default',
      dynamic: false,
      actions: {
        pathAction: ['asis'],
      },
    },
  ],
});

const createJsonResponse = (payload) => new Response(JSON.stringify(payload), {
  status: 200,
  headers: { 'content-type': 'application/json' },
});

const resetBootstrapClientState = () => {
  delete globalThis.bootstrapCache;
  delete globalThis.cacheD1TablesInitialized;
};

const createCacheD1Mock = (row) => {
  const state = { writes: [] };
  return {
    state,
    async batch() {},
    prepare(sql) {
      return {
        bind(...args) {
          if (sql.includes('SELECT payload_json')) {
            return {
              first: async () => row,
            };
          }
          if (sql.includes('INSERT OR REPLACE INTO bootstrap_cache')) {
            return {
              run: async () => {
                state.writes.push({ sql, args });
                return {};
              },
            };
          }
          return {
            run: async () => ({}),
          };
        },
      };
    },
  };
};

test('resolveConfig rejects unknown throttleProfile instead of falling back to default', () => {
  assert.throws(
    () => resolveConfig({}, buildBootstrap(), { download: { throttleProfile: 'missing' } }),
    /Unknown throttleProfile/
  );
});

test('resolveConfig skips the implicit default throttleProfile when breaker storage is disabled', () => {
  const bootstrap = buildBootstrap();
  delete bootstrap.download.throttleProfiles.default;

  const config = resolveConfig({}, bootstrap, { download: {} });

  assert.equal(config.dbMode, '');
  assert.equal(config.throttleEnabled, false);
  assert.deepEqual(config.throttleHostnamePatterns, []);
});

test('resolveConfig still requires the canonical default throttleProfile when custom-pg-rest is enabled', () => {
  const bootstrap = buildBootstrap();
  delete bootstrap.download.throttleProfiles.default;
  bootstrap.download.db = {
    mode: 'custom-pg-rest',
    postgrestUrl: 'https://postgrest.example.test',
    verifyHeader: ['X-Verify'],
    verifySecret: ['secret'],
    cacheEnabled: false,
  };

  assert.throws(
    () => resolveConfig({}, bootstrap, { download: {} }),
    /Unknown throttleProfile/
  );
});

test('resolveConfig reads download.db.ticketStateTable and exposes ticketStateTableName', () => {
  const bootstrap = buildBootstrap();
  bootstrap.download.db = {
    mode: 'custom-pg-rest',
    postgrestUrl: 'https://postgrest.example.test',
    verifyHeader: ['X-Verify'],
    verifySecret: ['secret'],
    cacheEnabled: false,
    ticketStateTable: 'DOWNLOAD_TICKET_STATE_TABLE',
  };

  const config = resolveConfig({}, bootstrap, { download: {} });

  assert.equal(config.cacheConfig.ticketStateTableName, 'DOWNLOAD_TICKET_STATE_TABLE');
  assert.equal(config.ticketStateTableName, 'DOWNLOAD_TICKET_STATE_TABLE');
  assert.equal(config.lastActiveTableName, undefined);
  assert.equal(Object.hasOwn(config.cacheConfig, 'lastActiveTableName'), false);
});

test('ticket-state runtime config remains mandatory even when dbMode is not custom-pg-rest', () => {
  const ticketStateConfig = {
    postgrestUrl: 'https://postgrest.example.test',
    verifyHeader: ['X-Verify'],
    verifySecret: ['secret'],
    ticketStateTableName: 'DOWNLOAD_TICKET_STATE_TABLE',
  };

  assert.deepEqual(
    resolveTicketStateConfig({
      dbMode: '',
      cacheConfig: ticketStateConfig,
    }),
    ticketStateConfig,
  );
});

test('resolveTicketStateConfig accepts real bootstrap output when db mode is empty', () => {
  const bootstrap = buildBootstrap();
  bootstrap.download.db = {
    mode: '',
    postgrestUrl: 'https://postgrest.example.test',
    verifyHeader: ['X-Verify'],
    verifySecret: ['secret'],
    ticketStateTable: 'DOWNLOAD_TICKET_STATE_TABLE',
  };

  const config = resolveConfig({}, bootstrap, { download: {} });

  assert.deepEqual(resolveTicketStateConfig(config), {
    postgrestUrl: 'https://postgrest.example.test',
    verifyHeader: ['X-Verify'],
    verifySecret: ['secret'],
    ticketStateTableName: 'DOWNLOAD_TICKET_STATE_TABLE',
  });
});

test('ticket-state runtime config fails closed when PostgREST wiring is missing', () => {
  assert.throws(
    () => resolveTicketStateConfig({
      dbMode: '',
      cacheConfig: {
        ticketStateTableName: 'DOWNLOAD_TICKET_STATE_TABLE',
      },
    }),
    /ticket-state db configuration missing/i,
  );
});

test('mark-used predicate ignores metadata-only JSON 200 responses', () => {
  const response = new Response(JSON.stringify({ ok: true }), {
    status: 200,
    headers: {
      'content-type': 'application/json;charset=UTF-8',
      'content-length': '12',
    },
  });

  assert.equal(shouldConsumeTicketResponse({
    requestMethod: 'GET',
    response,
    knownFileSize: 4096,
  }), false);
});

test('mark-used predicate ignores metadata-only JSON 200 responses with synthetic attachment headers', () => {
  const response = new Response(JSON.stringify({ ok: true }), {
    status: 200,
    headers: {
      'content-type': 'application/json;charset=UTF-8',
      'content-length': '12',
      'content-disposition': 'attachment; filename="metadata.json"',
    },
  });

  assert.equal(shouldConsumeTicketResponse({
    requestMethod: 'GET',
    response,
    syntheticContentDisposition: true,
  }), false);
});

test('mark-used predicate accepts JSON file-content responses with upstream attachment headers without known file size', () => {
  const response = new Response('{"records":[1,2,3]}', {
    status: 200,
    headers: {
      'content-type': 'application/json;charset=UTF-8',
      'content-length': '19',
      'content-disposition': 'attachment; filename="download.json"',
    },
  });

  assert.equal(shouldConsumeTicketResponse({
    requestMethod: 'GET',
    response,
    syntheticContentDisposition: false,
  }), true);
});

test('mark-used predicate consumes ambiguous text/html 200 responses even when they only expose body text', () => {
  const response = new Response('<html><body>warning</body></html>', {
    status: 200,
    headers: {
      'content-type': 'text/html;charset=UTF-8',
      'content-length': '33',
    },
  });

  assert.equal(shouldConsumeTicketResponse({
    requestMethod: 'GET',
    response,
  }), true);
});

test('mark-used predicate consumes ambiguous text/plain 200 responses even when they only expose body text', () => {
  const response = new Response('provider warning page', {
    status: 200,
    headers: {
      'content-type': 'text/plain;charset=UTF-8',
      'content-length': '21',
    },
  });

  assert.equal(shouldConsumeTicketResponse({
    requestMethod: 'GET',
    response,
  }), true);
});

test('mark-used predicate consumes synthetic text/plain attachment headers when the worker releases a user-facing textual download response', () => {
  const response = new Response('provider warning page', {
    status: 200,
    headers: {
      'content-type': 'text/plain;charset=UTF-8',
      'content-disposition': 'attachment; filename="opaque.bin"',
    },
  });

  assert.equal(shouldConsumeTicketResponse({
    requestMethod: 'GET',
    response,
    requestPath: '/downloads/opaque',
    syntheticContentDisposition: true,
  }), true);
});

for (const syntheticTextAttachmentCase of [
  {
    label: 'text/plain',
    body: 'provider warning page',
    contentType: 'text/plain',
  },
  {
    label: 'text/html',
    body: '<html><body>warning</body></html>',
    contentType: 'text/html',
  },
]) {
  test(`mark-used predicate consumes synthetic ${syntheticTextAttachmentCase.label} attachment wrappers even with positive content-length`, () => {
    const response = new Response(syntheticTextAttachmentCase.body, {
      status: 200,
      headers: {
        'content-type': `${syntheticTextAttachmentCase.contentType};charset=UTF-8`,
        'content-length': String(Buffer.byteLength(syntheticTextAttachmentCase.body)),
        'content-disposition': 'attachment; filename="opaque.bin"',
      },
    });

    assert.equal(shouldConsumeTicketResponse({
      requestMethod: 'GET',
      response,
      requestPath: '/downloads/opaque',
      syntheticContentDisposition: true,
    }), true);
  });
}

for (const textualPathCase of [
  {
    label: 'text/plain warning bodies on .txt request paths',
    body: 'provider warning page',
    contentType: 'text/plain',
    requestPath: '/downloads/readme.txt',
  },
  {
    label: 'text/html warning bodies on .html request paths',
    body: '<html><body>warning</body></html>',
    contentType: 'text/html',
    requestPath: '/downloads/index.html',
  },
]) {
  test(`mark-used predicate consumes ${textualPathCase.label} without upstream file signals`, () => {
    const response = new Response(textualPathCase.body, {
      status: 200,
      headers: {
        'content-type': `${textualPathCase.contentType};charset=UTF-8`,
      },
    });

    assert.equal(shouldConsumeTicketResponse({
      requestMethod: 'GET',
      response,
      requestPath: textualPathCase.requestPath,
    }), true);
  });
}

for (const cryptedTextAttachmentCase of [
  {
    label: 'text/plain',
    body: 'timestamp,value\n2026-05-01,42\n',
    contentType: 'text/plain',
  },
  {
    label: 'text/html',
    body: '<html><body>csv export body</body></html>',
    contentType: 'text/html',
  },
]) {
  test(`mark-used predicate accepts crypted opaque ${cryptedTextAttachmentCase.label} attachments when the upstream response proves file content`, () => {
    const upstreamResponse = new Response(cryptedTextAttachmentCase.body, {
      status: 200,
      headers: {
        'content-type': `${cryptedTextAttachmentCase.contentType};charset=UTF-8`,
        'content-disposition': 'attachment; filename="report.csv"',
      },
    });
    const response = new Response(cryptedTextAttachmentCase.body, {
      status: 200,
      headers: {
        'content-type': `${cryptedTextAttachmentCase.contentType};charset=UTF-8`,
        'content-disposition': 'attachment; filename="report.csv.enc"',
      },
    });

    assert.equal(shouldConsumeTicketResponse({
      requestMethod: 'GET',
      response,
      upstreamResponse,
      requestPath: '/downloads/opaque',
      syntheticContentDisposition: true,
    }), true);
  });
}

test('mark-used predicate accepts chunked text/plain attachment responses with opaque request paths', () => {
  const response = new Response('timestamp,value\n2026-05-01,42\n', {
    status: 200,
    headers: {
      'content-type': 'text/plain;charset=UTF-8',
      'content-disposition': 'attachment; filename="report.csv"',
    },
  });

  assert.equal(shouldConsumeTicketResponse({
    requestMethod: 'GET',
    response,
    requestPath: '/downloads/opaque',
    syntheticContentDisposition: false,
  }), true);
});

test('mark-used predicate accepts chunked text/html attachment responses with opaque request paths', () => {
  const response = new Response('<html><body>csv export body</body></html>', {
    status: 200,
    headers: {
      'content-type': 'text/html;charset=UTF-8',
      'content-disposition': 'attachment; filename="report.csv"',
    },
  });

  assert.equal(shouldConsumeTicketResponse({
    requestMethod: 'GET',
    response,
    requestPath: '/downloads/opaque',
    syntheticContentDisposition: false,
  }), true);
});

test('mark-used predicate accepts actual file-content responses', () => {
  const response = new Response('abc', {
    status: 206,
    headers: {
      'content-range': 'bytes 0-2/3',
      'content-length': '3',
      'accept-ranges': 'bytes',
    },
  });

  assert.equal(shouldConsumeTicketResponse({
    requestMethod: 'GET',
    response,
    knownFileSize: 3,
  }), true);
});

test('mark-used predicate accepts ordinary binary 200 file responses', () => {
  const response = new Response('file-bytes', {
    status: 200,
    headers: {
      'content-type': 'application/octet-stream',
      'content-length': '10',
    },
  });

  assert.equal(shouldConsumeTicketResponse({
    requestMethod: 'GET',
    response,
  }), true);
});

test('mark-used predicate accepts streamed binary 200 responses without content-length hints', () => {
  const response = new Response('stream-body', {
    status: 200,
    headers: {
      'content-type': 'application/octet-stream',
    },
  });

  assert.equal(shouldConsumeTicketResponse({
    requestMethod: 'GET',
    response,
  }), true);
});

test('resolveConfig skips implicit default throttleProfile validation when decision omits the selector', () => {
  const config = resolveConfig({}, buildBootstrap(), { download: {} });

  assert.equal(config.throttleEnabled, false);
  assert.deepEqual(config.throttleHostnamePatterns, []);
  assert.deepEqual(config.throttleConfig.protectHttpCodes, [429, 499, 500, 502, 503, 504]);
});

test('resolveConfig ignores invalid implicit default protectHttpCodes when breaker storage is disabled', () => {
  const bootstrap = buildBootstrap();
  bootstrap.download.throttleProfiles.default.protectHttpCodes = [429, 700];

  const config = resolveConfig({}, bootstrap, { download: {} });

  assert.equal(config.throttleEnabled, false);
  assert.deepEqual(config.throttleHostnamePatterns, []);
  assert.deepEqual(config.throttleConfig.protectHttpCodes, [429, 499, 500, 502, 503, 504]);
});

test('resolveConfig rejects invalid protectHttpCodes from an explicit throttleProfile', () => {
  const bootstrap = buildBootstrap();
  bootstrap.download.throttleProfiles.default.protectHttpCodes = [429, 700];

  assert.throws(
    () => resolveConfig({}, bootstrap, { download: { throttleProfile: 'default' } }),
    /protectHttpCodes/
  );
});

test('resolveConfig rejects invalid halfOpenCloseMode from an explicit throttleProfile', () => {
  const bootstrap = buildBootstrap();
  bootstrap.download.throttleProfiles.default.halfOpenCloseMode = 'xor';

  assert.throws(
    () => resolveConfig({}, bootstrap, { download: { throttleProfile: 'default' } }),
    /halfOpenCloseMode/
  );
});

test('resolveConfig rejects invalid halfOpenTimeoutMode from an explicit throttleProfile', () => {
  const bootstrap = buildBootstrap();
  bootstrap.download.throttleProfiles.default.halfOpenTimeoutMode = 'linger';

  assert.throws(
    () => resolveConfig({}, bootstrap, { download: { throttleProfile: 'default' } }),
    /halfOpenTimeoutMode/
  );
});

test('resolveConfig rejects blank throttleProfile when the field is present', () => {
  assert.throws(
    () => resolveConfig({}, buildBootstrap(), { download: { throttleProfile: '' } }),
    /Invalid throttleProfile/
  );
});

test('resolveConfig rejects non-string throttleProfile when the field is present', () => {
  assert.throws(
    () => resolveConfig({}, buildBootstrap(), { download: { throttleProfile: 123 } }),
    /Invalid throttleProfile/
  );
});

test('fetchControllerState preserves blank dynamic throttleProfile for strict validation', async () => {
  resetBootstrapClientState();
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url) => {
    if (String(url).endsWith('/bootstrap')) {
      return createJsonResponse(buildDynamicBootstrap());
    }
    if (String(url).endsWith('/decision')) {
      return createJsonResponse({ download: { throttleProfile: '' } });
    }
    throw new Error(`unexpected controller URL: ${url}`);
  };

  try {
    const state = await fetchControllerState(
      new Request('https://worker.example.com/downloads/file.bin'),
      buildControllerEnv({ BOOTSTRAP_CACHE_MODE: 'direct' }),
    );

    assert.equal(state.decision.download.throttleProfile, '');
    assert.throws(
      () => resolveConfig({}, state.bootstrap, state.decision),
      /Invalid throttleProfile/
    );
  } finally {
    globalThis.fetch = originalFetch;
    resetBootstrapClientState();
  }
});

test('fetchControllerState preserves blank static throttleProfile for strict validation', async () => {
  resetBootstrapClientState();
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url) => {
    if (String(url).endsWith('/bootstrap')) {
      return createJsonResponse(buildStaticBootstrap(''));
    }
    throw new Error(`unexpected controller URL: ${url}`);
  };

  try {
    const state = await fetchControllerState(
      new Request('https://worker.example.com/downloads/file.bin'),
      buildControllerEnv({ BOOTSTRAP_CACHE_MODE: 'direct' }),
    );

    assert.equal(state.decision.download.throttleProfile, '');
    assert.throws(
      () => resolveConfig({}, state.bootstrap, state.decision),
      /Invalid throttleProfile/
    );
  } finally {
    globalThis.fetch = originalFetch;
    resetBootstrapClientState();
  }
});

test('fetchControllerState preserves non-string static throttleProfile for strict validation', async () => {
  resetBootstrapClientState();
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url) => {
    if (String(url).endsWith('/bootstrap')) {
      return createJsonResponse(buildStaticBootstrap(123));
    }
    throw new Error(`unexpected controller URL: ${url}`);
  };

  try {
    const state = await fetchControllerState(
      new Request('https://worker.example.com/downloads/file.bin'),
      buildControllerEnv({ BOOTSTRAP_CACHE_MODE: 'direct' }),
    );

    assert.equal(state.decision.download.throttleProfile, 123);
    assert.throws(
      () => resolveConfig({}, state.bootstrap, state.decision),
      /Invalid throttleProfile/
    );
  } finally {
    globalThis.fetch = originalFetch;
    resetBootstrapClientState();
  }
});

test('fetchControllerState preserves omitted static throttleProfile without forcing implicit default validation', async () => {
  resetBootstrapClientState();
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url) => {
    if (String(url).endsWith('/bootstrap')) {
      return createJsonResponse(buildStaticBootstrapWithoutThrottleProfile());
    }
    throw new Error(`unexpected controller URL: ${url}`);
  };

  try {
    const state = await fetchControllerState(
      new Request('https://worker.example.com/downloads/file.bin'),
      buildControllerEnv({ BOOTSTRAP_CACHE_MODE: 'direct' }),
    );

    assert.equal(Object.prototype.hasOwnProperty.call(state.decision.download, 'throttleProfile'), false);

    const config = resolveConfig({}, state.bootstrap, state.decision);
    assert.equal(config.throttleEnabled, false);
    assert.deepEqual(config.throttleHostnamePatterns, []);
  } finally {
    globalThis.fetch = originalFetch;
    resetBootstrapClientState();
  }
});

test('fetchControllerState matches path rules by pattern only', async () => {
  resetBootstrapClientState();
  const originalFetch = globalThis.fetch;
  const bootstrap = {
    ...buildBootstrap(),
    global: { defaultProfileId: 'download-fallback' },
    pathRules: [
      {
        profileId: 'download-specific',
        pattern: '/downloads/**',
        priority: 300,
        prefix: ['/legacy-never-match'],
        pathIncludes: ['legacy-never-match'],
      },
      {
        profileId: 'download-fallback',
        pattern: '/**',
        priority: 100,
      },
    ],
    pathProfiles: [
      {
        id: 'download-specific',
        dynamic: false,
        actions: {
          pathAction: ['block'],
          throttleProfile: 'default',
        },
      },
      {
        id: 'download-fallback',
        dynamic: false,
        actions: {
          pathAction: ['asis'],
          throttleProfile: 'default',
        },
      },
    ],
  };

  globalThis.fetch = async (url) => {
    if (String(url).endsWith('/bootstrap')) {
      return createJsonResponse(bootstrap);
    }
    throw new Error(`unexpected controller URL: ${url}`);
  };

  try {
    const state = await fetchControllerState(
      new Request('https://worker.example.com/downloads/file.bin'),
      buildControllerEnv({ BOOTSTRAP_CACHE_MODE: 'direct' }),
    );

    assert.equal(state.profileId, 'download-specific');
    assert.deepEqual(state.decision.download.pathAction, ['block']);
  } finally {
    globalThis.fetch = originalFetch;
    resetBootstrapClientState();
  }
});

test('fetchControllerState fails closed when defaultProfileId points to a missing profile', async () => {
  resetBootstrapClientState();
  const originalFetch = globalThis.fetch;
  const bootstrap = {
    ...buildBootstrap(),
    global: { defaultProfileId: 'missing-profile' },
    pathRules: [],
    pathProfiles: [
      {
        id: 'download-fallback',
        dynamic: false,
        actions: {
          pathAction: ['asis'],
          throttleProfile: 'default',
        },
      },
    ],
  };

  globalThis.fetch = async (url) => {
    if (String(url).endsWith('/bootstrap')) {
      return createJsonResponse(bootstrap);
    }
    throw new Error(`unexpected controller URL: ${url}`);
  };

  try {
    const state = await fetchControllerState(
      new Request('https://worker.example.com/downloads/file.bin'),
      buildControllerEnv({ BOOTSTRAP_CACHE_MODE: 'direct' }),
    );

    assert.equal(state, null);
  } finally {
    globalThis.fetch = originalFetch;
    resetBootstrapClientState();
  }
});

test('fetchControllerState fails closed when bootstrap omits defaultProfileId and no rule matches', async () => {
  resetBootstrapClientState();
  const originalFetch = globalThis.fetch;
  const bootstrap = {
    ...buildBootstrap(),
    global: {},
    pathRules: [],
    pathProfiles: [
      {
        id: 'download-fallback',
        dynamic: false,
        actions: {
          pathAction: ['asis'],
          throttleProfile: 'default',
        },
      },
    ],
  };

  globalThis.fetch = async (url) => {
    if (String(url).endsWith('/bootstrap')) {
      return createJsonResponse(bootstrap);
    }
    throw new Error(`unexpected controller URL: ${url}`);
  };

  try {
    const state = await fetchControllerState(
      new Request('https://worker.example.com/downloads/file.bin'),
      buildControllerEnv({ BOOTSTRAP_CACHE_MODE: 'direct' }),
    );

    assert.equal(state, null);
  } finally {
    globalThis.fetch = originalFetch;
    resetBootstrapClientState();
  }
});

test('fetchControllerState fails closed when matched profileId points to a missing profile', async () => {
  resetBootstrapClientState();
  const originalFetch = globalThis.fetch;
  const bootstrap = {
    ...buildBootstrap(),
    global: { defaultProfileId: 'download-default' },
    pathRules: [
      { profileId: 'missing-profile', pattern: '/downloads/**', priority: 300 },
      { profileId: 'download-default', pattern: '/**', priority: 100 },
    ],
    pathProfiles: [
      {
        id: 'download-default',
        dynamic: false,
        actions: {
          pathAction: ['asis'],
          throttleProfile: 'default',
        },
      },
      {
        id: 'other-profile',
        dynamic: false,
        actions: {
          pathAction: ['block'],
          throttleProfile: 'default',
        },
      },
    ],
  };

  globalThis.fetch = async (url) => {
    if (String(url).endsWith('/bootstrap')) {
      return createJsonResponse(bootstrap);
    }
    throw new Error(`unexpected controller URL: ${url}`);
  };

  try {
    const state = await fetchControllerState(
      new Request('https://worker.example.com/downloads/file.bin'),
      buildControllerEnv({ BOOTSTRAP_CACHE_MODE: 'direct' }),
    );

    assert.equal(state, null);
  } finally {
    globalThis.fetch = originalFetch;
    resetBootstrapClientState();
  }
});

test('resolveConfig maps halfOpenMaxProbeCount and drops probeLeaseSeconds', () => {
  const config = resolveConfig({}, buildBootstrap(), { download: { throttleProfile: 'sharepoint' } });

  assert.deepEqual(config.throttleHostnamePatterns, ['*.sharepoint.com']);
  assert.deepEqual(config.throttleConfig, {
    postgrestUrl: '',
    verifyHeader: [],
    verifySecret: [],
    openCapSeconds: 75,
    openThresholdPercent: 35,
    closeThresholdPercent: 15,
    ewmaSpan: 11,
    consecutiveThreshold: 6,
    minSamplesBeforeEwmaOpen: 8,
    idleResetSeconds: 900,
    halfOpenSuccessThreshold: 2,
    halfOpenCloseMode: 'and',
    halfOpenMaxProbeCount: 4,
    halfOpenMaxSeconds: 15,
    halfOpenTimeoutMode: 'partial-close',
    protectHttpCodes: [429, 503],
  });
  assert.equal('probeLeaseSeconds' in config.throttleConfig, false);
});

test('resolveConfig preserves an explicit closeThresholdPercent of 0', () => {
  const bootstrap = buildBootstrap();
  bootstrap.download.throttleProfiles.default.closeThresholdPercent = 0;

  const config = resolveConfig({}, bootstrap, { download: { throttleProfile: 'default' } });

  assert.equal(config.throttleConfig.closeThresholdPercent, 0);
});

test('resolveConfig rejects controller throttle profiles with impossible half-open budgets', () => {
  const bootstrap = buildBootstrap();
  bootstrap.download.throttleProfiles.default.halfOpenSuccessThreshold = 5;

  assert.throws(
    () => resolveConfig({}, bootstrap, { download: { throttleProfile: 'default' } }),
    /halfOpenSuccessThreshold must be <= halfOpenMaxProbeCount/
  );
});


test('resolveConfig rejects controller throttle profiles that exceed the SQL half-open bitmap ceiling', () => {
  const bootstrap = buildBootstrap();
  bootstrap.download.throttleProfiles.default.halfOpenMaxProbeCount = 64;

  assert.throws(
    () => resolveConfig({}, bootstrap, { download: { throttleProfile: 'default' } }),
    /halfOpenMaxProbeCount must be <= 63/
  );
});

test('getBootstrapConfig ignores stale in-memory payloads from the prior schema epoch', async () => {
  resetBootstrapClientState();
  globalThis.bootstrapCache = {
    expAt: Date.now() + 60_000,
    data: {
      schemaEpoch: CURRENT_SCHEMA_EPOCH - 1,
      data: { configVersion: 'stale-memory' },
    },
  };

  const freshPayload = { configVersion: 'fresh-memory', ttlSeconds: 120 };
  let fetchCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    return createJsonResponse(freshPayload);
  };

  try {
    const data = await getBootstrapConfig(buildControllerEnv({ BOOTSTRAP_CACHE_MODE: 'direct' }));
    assert.equal(data.configVersion, 'fresh-memory');
    assert.equal(fetchCalls, 1);
    assert.equal(globalThis.bootstrapCache.data.schemaEpoch, CURRENT_SCHEMA_EPOCH);
    assert.deepEqual(globalThis.bootstrapCache.data.data, freshPayload);
  } finally {
    globalThis.fetch = originalFetch;
    resetBootstrapClientState();
  }
});

test('reportBreakerSample sends the canonical breaker RPC payload', async () => {
  let rpcBody = null;
  let rpcUrl = null;
  const originalFetch = globalThis.fetch;
  const originalRandom = Math.random;
  Math.random = () => 1;
  globalThis.fetch = async (url, init) => {
    rpcUrl = url;
    rpcBody = JSON.parse(init.body);
    return createJsonResponse([{
      STATE: 'closed',
      OPEN_UNTIL: null,
      OPEN_REASON: null,
      VERSION: 3,
      LAST_ERROR_CODE: 503,
      CONSECUTIVE_ERROR_COUNT: 0,
    }]);
  };

  try {
    await reportBreakerSample('tenant.sharepoint.com', {
      sample: 1,
      statusCode: 503,
    }, {
      postgrestUrl: 'https://postgrest.example.test',
      verifyHeader: ['X-Verify'],
      verifySecret: ['secret'],
      openCapSeconds: 75,
      openThresholdPercent: 35,
      closeThresholdPercent: 15,
      ewmaSpan: 11,
      consecutiveThreshold: 6,
      minSamplesBeforeEwmaOpen: 8,
      idleResetSeconds: 900,
      halfOpenSuccessThreshold: 2,
      halfOpenCloseMode: 'and',
      halfOpenMaxProbeCount: 4,
      halfOpenMaxSeconds: 15,
      halfOpenTimeoutMode: 'partial-close',
    });

    assert.equal(rpcUrl, 'https://postgrest.example.test/rpc/download_report_breaker_sample');
    assert.equal(rpcBody.p_hostname, 'tenant.sharepoint.com');
    assert.equal(typeof rpcBody.p_hostname_hash, 'string');
    assert.equal(rpcBody.p_sample, 1);
    assert.equal(rpcBody.p_status_code, 503);
    assert.equal(rpcBody.p_open_cap_seconds, 75);
    assert.equal(rpcBody.p_open_threshold_percent, 35);
    assert.equal(rpcBody.p_close_threshold_percent, 15);
    assert.equal(rpcBody.p_ewma_span, 11);
    assert.equal(rpcBody.p_consecutive_threshold, 6);
    assert.equal(rpcBody.p_min_samples_before_ewma_open, 8);
    assert.equal(rpcBody.p_idle_reset_seconds, 900);
    assert.equal(rpcBody.p_half_open_success_threshold, 2);
    assert.equal(rpcBody.p_half_open_close_mode, 'and');
    assert.equal(rpcBody.p_half_open_max_seconds, 15);
    assert.equal(rpcBody.p_half_open_timeout_mode, 'partial-close');
    assert.equal(rpcBody.p_attempt_version, null);
    assert.equal(rpcBody.p_attempt_ticket, null);
    assert.equal(rpcBody.p_retry_after_seconds, null);
    assert.deepEqual(Object.keys(rpcBody).sort(), [
      'p_attempt_ticket',
      'p_attempt_version',
      'p_close_threshold_percent',
      'p_consecutive_threshold',
      'p_ewma_span',
      'p_half_open_close_mode',
      'p_half_open_max_seconds',
      'p_half_open_success_threshold',
      'p_half_open_timeout_mode',
      'p_hostname',
      'p_hostname_hash',
      'p_idle_reset_seconds',
      'p_min_samples_before_ewma_open',
      'p_now',
      'p_open_cap_seconds',
      'p_open_threshold_percent',
      'p_retry_after_seconds',
      'p_sample',
      'p_status_code',
    ]);
  } finally {
    globalThis.fetch = originalFetch;
    Math.random = originalRandom;
  }
});

test('getBootstrapConfig ignores stale D1 payloads from the prior schema epoch and rewrites cache', async () => {
  resetBootstrapClientState();
  const cacheDb = createCacheD1Mock({
    payload_json: JSON.stringify({
      schemaEpoch: CURRENT_SCHEMA_EPOCH - 1,
      data: { configVersion: 'stale-d1' },
    }),
    expires_at: Date.now() + 60_000,
  });

  const freshPayload = { configVersion: 'fresh-d1', ttlSeconds: 90 };
  let fetchCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    return createJsonResponse(freshPayload);
  };

  try {
    const data = await getBootstrapConfig(buildControllerEnv({
      BOOTSTRAP_CACHE_MODE: 'd1',
      CACHE_D1: cacheDb,
      INIT_TABLES: 'true',
    }));

    assert.equal(data.configVersion, 'fresh-d1');
    assert.equal(fetchCalls, 1);
    assert.equal(cacheDb.state.writes.length, 1);

    const writtenPayload = JSON.parse(cacheDb.state.writes[0].args[5]);
    assert.equal(writtenPayload.schemaEpoch, CURRENT_SCHEMA_EPOCH);
    assert.deepEqual(writtenPayload.data, freshPayload);
  } finally {
    globalThis.fetch = originalFetch;
    resetBootstrapClientState();
  }
});
