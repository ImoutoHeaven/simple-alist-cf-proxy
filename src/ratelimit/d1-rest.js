import { calculateIPSubnet, sha256Hash } from '../utils.js';

const DEFAULT_TABLE = 'DOWNLOAD_IP_RATELIMIT_TABLE';

const executeQuery = async (accountId, databaseId, apiToken, sql, params = []) => {
  const endpoint = `https://api.cloudflare.com/client/v4/accounts/${accountId}/d1/database/${databaseId}/query`;

  const body = { sql };
  if (params && params.length > 0) {
    body.params = params;
  }

  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${apiToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`D1 REST API error (${response.status}): ${errorText}`);
  }

  const result = await response.json();

  if (!result.success) {
    throw new Error(`D1 REST API query failed: ${JSON.stringify(result.errors || 'Unknown error')}`);
  }

  return result.result?.[0] || { results: [], success: true };
};

const ensureTable = async (accountId, databaseId, apiToken, tableName) => {
  const sql = `
    CREATE TABLE IF NOT EXISTS ${tableName} (
      IP_HASH TEXT PRIMARY KEY,
      IP_RANGE TEXT NOT NULL,
      ACCESS_COUNT INTEGER NOT NULL,
      LAST_WINDOW_TIME INTEGER NOT NULL,
      BLOCK_UNTIL INTEGER
    )
  `;
  await executeQuery(accountId, databaseId, apiToken, sql);

  const indexWindow = `CREATE INDEX IF NOT EXISTS idx_rate_limit_window ON ${tableName}(LAST_WINDOW_TIME)`;
  await executeQuery(accountId, databaseId, apiToken, indexWindow);
  const indexBlock = `CREATE INDEX IF NOT EXISTS idx_rate_limit_block ON ${tableName}(BLOCK_UNTIL) WHERE BLOCK_UNTIL IS NOT NULL`;
  await executeQuery(accountId, databaseId, apiToken, indexBlock);
};

export const checkRateLimit = async (ip, config) => {
  if (!config.accountId || !config.databaseId || !config.apiToken || !config.windowTimeSeconds || !config.limit) {
    return { allowed: true };
  }

  if (!ip || typeof ip !== 'string') {
    return { allowed: true };
  }

  try {
    const { accountId, databaseId, apiToken } = config;
    const tableName = config.tableName || DEFAULT_TABLE;

    await ensureTable(accountId, databaseId, apiToken, tableName);

    const ipSubnet = calculateIPSubnet(ip, config.ipv4Suffix, config.ipv6Suffix);
    if (!ipSubnet) {
      return { allowed: true };
    }

    const ipHash = await sha256Hash(ipSubnet);
    if (!ipHash) {
      return { allowed: true };
    }

    const now = Math.floor(Date.now() / 1000);

    const triggerCleanup = () => {
      const probability = config.cleanupProbability || 0.01;
      if (Math.random() < probability) {
        console.log(`[Rate Limit Cleanup] Triggered cleanup (probability: ${probability * 100}%)`);

        const cleanupPromise = cleanupExpiredRecords(accountId, databaseId, apiToken, tableName, config.windowTimeSeconds)
          .then((deletedCount) => {
            console.log(`[Rate Limit Cleanup] Background cleanup finished: ${deletedCount} records deleted`);
            return deletedCount;
          })
          .catch((error) => {
            console.error('[Rate Limit Cleanup] Background cleanup failed:', error instanceof Error ? error.message : String(error));
          });

        if (config.ctx && config.ctx.waitUntil) {
          config.ctx.waitUntil(cleanupPromise);
          console.log('[Rate Limit Cleanup] Cleanup scheduled in background (using ctx.waitUntil)');
        } else {
          console.warn('[Rate Limit Cleanup] No ctx.waitUntil available, cleanup may be interrupted');
        }
      }
    };

    const blockTimeSeconds = config.blockTimeSeconds || 0;
    const upsertSql = `
      INSERT INTO ${tableName} (IP_HASH, IP_RANGE, ACCESS_COUNT, LAST_WINDOW_TIME, BLOCK_UNTIL)
      VALUES (?, ?, 1, ?, NULL)
      ON CONFLICT (IP_HASH) DO UPDATE SET
        ACCESS_COUNT = CASE
          WHEN ? - ${tableName}.LAST_WINDOW_TIME >= ? THEN 1
          WHEN ${tableName}.BLOCK_UNTIL IS NOT NULL AND ${tableName}.BLOCK_UNTIL <= ? THEN 1
          WHEN ${tableName}.ACCESS_COUNT >= ? THEN ${tableName}.ACCESS_COUNT
          ELSE ${tableName}.ACCESS_COUNT + 1
        END,
        LAST_WINDOW_TIME = CASE
          WHEN ? - ${tableName}.LAST_WINDOW_TIME >= ? THEN ?
          WHEN ${tableName}.BLOCK_UNTIL IS NOT NULL AND ${tableName}.BLOCK_UNTIL <= ? THEN ?
          ELSE ${tableName}.LAST_WINDOW_TIME
        END,
        BLOCK_UNTIL = CASE
          WHEN ? - ${tableName}.LAST_WINDOW_TIME >= ? THEN NULL
          WHEN ${tableName}.BLOCK_UNTIL IS NOT NULL AND ${tableName}.BLOCK_UNTIL <= ? THEN NULL
          WHEN ${tableName}.ACCESS_COUNT >= ? AND ? > 0 THEN ? + ?
          ELSE ${tableName}.BLOCK_UNTIL
        END
      RETURNING ACCESS_COUNT, LAST_WINDOW_TIME, BLOCK_UNTIL
    `;

    const upsertParams = [
      ipHash, ipSubnet, now,
      now, config.windowTimeSeconds, now, config.limit,
      now, config.windowTimeSeconds, now, now, now,
      now, config.windowTimeSeconds, now, config.limit, blockTimeSeconds, now, blockTimeSeconds,
    ];

    const queryResult = await executeQuery(accountId, databaseId, apiToken, upsertSql, upsertParams);
    const records = queryResult.results || [];

    if (!records || records.length === 0) {
      throw new Error('D1 REST UPSERT returned no rows');
    }

    const row = records[0];
    const accessCount = Number.parseInt(row.ACCESS_COUNT, 10);
    const lastWindowTime = Number.parseInt(row.LAST_WINDOW_TIME, 10);
    const blockUntil = row.BLOCK_UNTIL ? Number.parseInt(row.BLOCK_UNTIL, 10) : null;

    triggerCleanup();

    if (blockUntil && blockUntil > now) {
      const retryAfter = blockUntil - now;
      return {
        allowed: false,
        ipSubnet,
        retryAfter: Math.max(1, retryAfter),
      };
    }

    if (accessCount >= config.limit) {
      const diff = now - lastWindowTime;
      const retryAfter = config.windowTimeSeconds - diff;
      return {
        allowed: false,
        ipSubnet,
        retryAfter: Math.max(1, retryAfter),
      };
    }

    return { allowed: true };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);

    if (config.pgErrorHandle === 'fail-open') {
      console.error('Rate limit check failed (fail-open):', errorMessage);
      return { allowed: true };
    }

    return {
      allowed: false,
      error: `Rate limit check failed: ${errorMessage}`,
    };
  }
};

const cleanupExpiredRecords = async (accountId, databaseId, apiToken, tableName, windowTimeSeconds) => {
  const now = Math.floor(Date.now() / 1000);
  const cutoffTime = now - (windowTimeSeconds * 2);

  try {
    console.log(`[Rate Limit Cleanup] Executing DELETE query (cutoff: ${cutoffTime}, windowTime: ${windowTimeSeconds}s)`);

    const deleteSql = `
      DELETE FROM ${tableName}
      WHERE LAST_WINDOW_TIME < ?
        AND (BLOCK_UNTIL IS NULL OR BLOCK_UNTIL < ?)
    `;

    const result = await executeQuery(accountId, databaseId, apiToken, deleteSql, [cutoffTime, now]);
    const deletedCount = result.meta?.changes || 0;

    console.log(`[Rate Limit Cleanup] DELETE completed: ${deletedCount} expired records deleted`);
    return deletedCount;
  } catch (error) {
    console.error('[Rate Limit Cleanup] DELETE failed:', error instanceof Error ? error.message : String(error));
    return 0;
  }
};

export const formatWindowTime = (seconds) => {
  if (seconds % 3600 === 0) {
    return `${seconds / 3600}h`;
  }
  if (seconds % 60 === 0) {
    return `${seconds / 60}m`;
  }
  return `${seconds}s`;
};
