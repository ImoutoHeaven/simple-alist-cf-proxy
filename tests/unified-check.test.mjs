import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as unifiedCheckModule from '../src/unified-check.js';

const { unifiedCheck, readTicketState, markTicketUsed } = unifiedCheckModule;

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
});

const buildTicketStateConfig = () => ({
  postgrestUrl: 'https://postgrest.example.test',
  verifyHeader: ['X-Verify'],
  verifySecret: ['secret'],
  ticketStateTableName: 'DOWNLOAD_TICKET_STATE_TABLE',
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
      'p_ip_hash',
      'p_ip_range',
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

test('readTicketState sends ticket hash and no legacy idle fields', async () => {
  const originalFetch = globalThis.fetch;
  let capturedUrl = null;
  let capturedBody = null;

  globalThis.fetch = async (url, options) => {
    capturedUrl = url;
    capturedBody = JSON.parse(options.body);
    return {
      ok: true,
      json: async () => [{
        found: true,
        ticket_hash: 'ticket-hash',
        issued_at: 1710000000,
        first_used_at: null,
        hard_expire_at: 1710003600,
        ip_hash: 'ip-hash',
        path_hash: 'path-hash',
      }],
    };
  };

  try {
    const result = await readTicketState('ticket-hash', buildTicketStateConfig());

    assert.equal(capturedUrl, 'https://postgrest.example.test/rpc/download_get_ticket_state');
    assert.deepEqual(capturedBody, {
      p_ticket_hash: 'ticket-hash',
      p_table_name: 'DOWNLOAD_TICKET_STATE_TABLE',
    });
    assert.equal(Object.hasOwn(capturedBody, 'p_idle_timeout'), false);
    assert.equal(Object.hasOwn(capturedBody, 'p_last_active_table_name'), false);
    assert.deepEqual(result, {
      found: true,
      ticketHash: 'ticket-hash',
      issuedAt: 1710000000,
      firstUsedAt: null,
      hardExpireAt: 1710003600,
      ipHash: 'ip-hash',
      pathHash: 'path-hash',
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('readTicketState surfaces missing ticket rows as a not-found result', async () => {
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async () => ({
    ok: true,
    json: async () => [{
      found: false,
      ticket_hash: null,
      issued_at: null,
      first_used_at: null,
      hard_expire_at: null,
      ip_hash: null,
      path_hash: null,
    }],
  });

  try {
    const result = await readTicketState('missing-ticket', buildTicketStateConfig());

    assert.deepEqual(result, {
      found: false,
      ticketHash: null,
      issuedAt: null,
      firstUsedAt: null,
      hardExpireAt: null,
      ipHash: null,
      pathHash: null,
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('markTicketUsed returns transitioned and already_used as success outcomes', async () => {
  const originalFetch = globalThis.fetch;
  const capturedBodies = [];
  const results = [
    { result: 'transitioned', first_used_at: 1710000001 },
    { result: 'already_used', first_used_at: 1710000001 },
  ];

  globalThis.fetch = async (_url, options) => {
    capturedBodies.push(JSON.parse(options.body));
    return {
      ok: true,
      json: async () => results.shift(),
    };
  };

  try {
    const transitioned = await markTicketUsed('ticket-hash', buildTicketStateConfig(), 1710000001);
    const alreadyUsed = await markTicketUsed('ticket-hash', buildTicketStateConfig(), 1710000002);

    assert.deepEqual(capturedBodies, [
      {
        p_ticket_hash: 'ticket-hash',
        p_now: 1710000001,
        p_table_name: 'DOWNLOAD_TICKET_STATE_TABLE',
      },
      {
        p_ticket_hash: 'ticket-hash',
        p_now: 1710000002,
        p_table_name: 'DOWNLOAD_TICKET_STATE_TABLE',
      },
    ]);
    assert.deepEqual(transitioned, {
      result: 'transitioned',
      firstUsedAt: 1710000001,
    });
    assert.deepEqual(alreadyUsed, {
      result: 'already_used',
      firstUsedAt: 1710000001,
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('ticket-state helpers never send legacy idle arguments', async () => {
  const originalFetch = globalThis.fetch;
  const capturedBodies = [];

  globalThis.fetch = async (_url, options) => {
    capturedBodies.push(JSON.parse(options.body));
    return {
      ok: true,
      json: async () => (capturedBodies.length === 1
        ? [{
            found: true,
            ticket_hash: 'ticket-hash',
            issued_at: 1710000000,
            first_used_at: null,
            hard_expire_at: 1710003600,
            ip_hash: null,
            path_hash: null,
          }]
        : { result: 'transitioned', first_used_at: 1710000001 }),
    };
  };

  try {
    await readTicketState('ticket-hash', buildTicketStateConfig());
    await markTicketUsed('ticket-hash', buildTicketStateConfig(), 1710000001);

    for (const body of capturedBodies) {
      assert.equal(Object.hasOwn(body, 'p_idle_timeout'), false);
      assert.equal(Object.hasOwn(body, 'p_last_active_table_name'), false);
    }
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
