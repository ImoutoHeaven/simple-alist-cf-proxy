import { sha256Hash } from '../utils.js';

const DEFAULT_IPRANGE_TABLE = 'IPRANGE_BANDWIDTH_QUOTA_TABLE';
const DEFAULT_FILEPATH_TABLE = 'IPRANGE_FILEPATH_BANDWIDTH_QUOTA_TABLE';

const executeQuery = async (accountId, databaseId, apiToken, sql, params = []) => {
  const endpoint = `https://api.cloudflare.com/client/v4/accounts/${accountId}/d1/database/${databaseId}/query`;
  const body = { sql };
  if (params.length > 0) {
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
    const text = await response.text().catch(() => '');
    throw new Error(`D1 REST API error (${response.status}): ${text}`);
  }

  const result = await response.json();
  if (!result.success) {
    throw new Error(`D1 REST API query failed: ${JSON.stringify(result.errors || 'Unknown error')}`);
  }

  return result.result || [];
};

const ensureIprangeTable = async (accountId, databaseId, apiToken, tableName) => {
  await executeQuery(accountId, databaseId, apiToken, `
    CREATE TABLE IF NOT EXISTS ${tableName} (
      IP_HASH TEXT PRIMARY KEY,
      IP_RANGE TEXT NOT NULL,
      BYTES_USED INTEGER NOT NULL DEFAULT 0,
      WINDOW_START INTEGER NOT NULL,
      BLOCK_UNTIL INTEGER
    )
  `);
  await executeQuery(accountId, databaseId, apiToken, `CREATE INDEX IF NOT EXISTS idx_${tableName.toLowerCase()}_window ON ${tableName}(WINDOW_START)`);
  await executeQuery(accountId, databaseId, apiToken, `CREATE INDEX IF NOT EXISTS idx_${tableName.toLowerCase()}_block ON ${tableName}(BLOCK_UNTIL) WHERE BLOCK_UNTIL IS NOT NULL`);
};

const ensureFilepathTable = async (accountId, databaseId, apiToken, tableName) => {
  await executeQuery(accountId, databaseId, apiToken, `
    CREATE TABLE IF NOT EXISTS ${tableName} (
      COMPOSITE_HASH TEXT PRIMARY KEY,
      IP_RANGE TEXT NOT NULL,
      FILEPATH TEXT NOT NULL,
      BYTES_USED INTEGER NOT NULL DEFAULT 0,
      WINDOW_START INTEGER NOT NULL,
      BLOCK_UNTIL INTEGER
    )
  `);
  await executeQuery(accountId, databaseId, apiToken, `CREATE INDEX IF NOT EXISTS idx_${tableName.toLowerCase()}_window ON ${tableName}(WINDOW_START)`);
  await executeQuery(accountId, databaseId, apiToken, `CREATE INDEX IF NOT EXISTS idx_${tableName.toLowerCase()}_block ON ${tableName}(BLOCK_UNTIL) WHERE BLOCK_UNTIL IS NOT NULL`);
};

const calculateFilepathQuota = (config, filesize) => {
  if (!config || config.value <= 0) {
    return 0;
  }

  if (config.type === 'dynamic') {
    if (!filesize || filesize <= 0) {
      console.warn('[Bandwidth Quota D1 REST] Dynamic quota requires filesize, skipping filepath quota update');
      return 0;
    }
    const quota = Math.floor(filesize * config.value);
    console.log(`[Bandwidth Quota D1 REST] Dynamic filepath quota: ${filesize} bytes * ${config.value} = ${quota} bytes`);
    return quota;
  }

  console.log(`[Bandwidth Quota D1 REST] Static filepath quota: ${config.value} bytes`);
  return config.value;
};

const parseInteger = (value, fallback = 0) => {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const logQuotaUpdate = ({
  scope,
  key,
  bytesAdded,
  limit,
  windowSeconds,
  row,
  now,
}) => {
  if (!row) {
    console.log(`[Bandwidth Quota D1 REST] ${scope} quota write: key=${key} bytesAdded=${bytesAdded} (no row returned)`);
    return;
  }

  const bytesUsed = parseInteger(row.BYTES_USED ?? row.bytes_used ?? row.bytesUsed, 0);
  const windowStart = parseInteger(row.WINDOW_START ?? row.window_start ?? row.windowStart, 0);
  const blockUntilRaw = row.BLOCK_UNTIL ?? row.block_until ?? row.blockUntil;
  const blockUntil = Number.isFinite(blockUntilRaw) ? blockUntilRaw : parseInteger(blockUntilRaw, null);
  const blockActive = Number.isFinite(blockUntil) && blockUntil > now;
  const shouldBlock = limit > 0 && windowSeconds > 0 && bytesUsed >= limit;
  const segments = [
    `[Bandwidth Quota D1 REST] ${scope} quota write`,
    `key=${key}`,
    `bytesAdded=${bytesAdded}`,
    `windowUsed=${limit > 0 ? `${bytesUsed}/${limit}` : `${bytesUsed}`}`,
    `windowStart=${windowStart}`,
    `blockActive=${blockActive}`,
    `shouldBlock=${shouldBlock}`,
  ];

  if (Number.isFinite(blockUntil)) {
    segments.push(`blockUntil=${blockUntil}`);
  }

  console.log(segments.join(' | '));
};

export const upsertBandwidthQuota = async (config, ipRange, filepath, bytes, filesize = 0) => {
  if (!config || !config.accountId || !config.databaseId || !config.apiToken) {
    console.warn('[Bandwidth Quota D1 REST] Missing configuration, skipping quota update');
    return { success: false };
  }

  const normalizedBytes = Number.isFinite(bytes) ? Math.trunc(bytes) : bytes;
  console.log(
    '[Bandwidth Quota D1 REST] Upsert request:',
    `ipRange=${ipRange || 'N/A'}`,
    `filepath=${filepath || 'N/A'}`,
    `bytes=${normalizedBytes}`
  );

  if (!ipRange || !Number.isFinite(bytes) || bytes <= 0) {
    console.log('[Bandwidth Quota D1 REST] Upsert skipped: invalid ipRange or bytes');
    return { success: true };
  }

  const now = Math.floor(Date.now() / 1000);
  const byteCount = Math.max(0, Math.trunc(bytes));
  const blockTimeSeconds = config.blockTimeSeconds || 0;

  try {
    const { accountId, databaseId, apiToken } = config;
    const updates = [];
    const totalQuotaActive = Boolean(config.totalEnabled) && config.iprangeLimit > 0 && config.windowTimeTotalSeconds > 0;
    const filepathQuotaLimit = config.filepathEnabled
      ? calculateFilepathQuota(config.filepathLimitConfig, filesize)
      : 0;
    const filepathQuotaActive =
      Boolean(config.filepathEnabled) &&
      filepath &&
      config.windowTimeFilepathSeconds > 0 &&
      filepathQuotaLimit > 0;

    if (totalQuotaActive) {
      console.log('[Bandwidth Quota D1 REST] Total quota update active');
      const tableName = config.iprangeTableName || DEFAULT_IPRANGE_TABLE;
      await ensureIprangeTable(accountId, databaseId, apiToken, tableName);

      const ipHash = await sha256Hash(ipRange);
      if (!ipHash) {
        console.error('[Bandwidth Quota D1 REST] Failed to compute IP hash');
      } else {
        const sql = `
          INSERT INTO ${tableName} (IP_HASH, IP_RANGE, BYTES_USED, WINDOW_START, BLOCK_UNTIL)
          VALUES (?, ?, ?, ?, NULL)
          ON CONFLICT (IP_HASH) DO UPDATE SET
            BYTES_USED = CASE
              WHEN ? - ${tableName}.WINDOW_START >= ? THEN ?
              WHEN ${tableName}.BLOCK_UNTIL IS NOT NULL AND ${tableName}.BLOCK_UNTIL <= ? THEN ?
              WHEN ${tableName}.BYTES_USED >= ? THEN ${tableName}.BYTES_USED
              ELSE ${tableName}.BYTES_USED + ?
            END,
            WINDOW_START = CASE
              WHEN ? - ${tableName}.WINDOW_START >= ? THEN ?
              WHEN ${tableName}.BLOCK_UNTIL IS NOT NULL AND ${tableName}.BLOCK_UNTIL <= ? THEN ?
              ELSE ${tableName}.WINDOW_START
            END,
            BLOCK_UNTIL = CASE
              WHEN ? - ${tableName}.WINDOW_START >= ? THEN NULL
              WHEN ${tableName}.BLOCK_UNTIL IS NOT NULL AND ${tableName}.BLOCK_UNTIL <= ? THEN NULL
              WHEN ${tableName}.BYTES_USED + ? > ? AND ? > 0 THEN ? + ?
              ELSE ${tableName}.BLOCK_UNTIL
            END
          RETURNING BYTES_USED, WINDOW_START, BLOCK_UNTIL
        `;

        const params = [
          ipHash,
          ipRange,
          byteCount,
          now,
          now,
          config.windowTimeTotalSeconds,
          byteCount,
          now,
          byteCount,
          config.iprangeLimit,
          byteCount,
          now,
          config.windowTimeTotalSeconds,
          now,
          now,
          now,
          config.windowTimeTotalSeconds,
          now,
          byteCount,
          config.iprangeLimit,
          blockTimeSeconds,
          now,
          blockTimeSeconds,
        ];

        updates.push(
          executeQuery(accountId, databaseId, apiToken, sql, params).then((rows) => {
            const row = Array.isArray(rows) ? rows[0] : null;
            logQuotaUpdate({
              scope: 'iprange',
              key: ipRange,
              bytesAdded: byteCount,
              limit: config.iprangeLimit,
              windowSeconds: config.windowTimeTotalSeconds,
              row,
              now,
            });
          })
        );
      }
    }

    if (filepathQuotaActive) {
      console.log('[Bandwidth Quota D1 REST] Filepath quota update active');
      const tableName = config.filepathTableName || DEFAULT_FILEPATH_TABLE;
      await ensureFilepathTable(accountId, databaseId, apiToken, tableName);

      const compositeHash = await sha256Hash(`${ipRange}${filepath}`);
      if (!compositeHash) {
        console.error('[Bandwidth Quota D1 REST] Failed to compute composite hash');
      } else {
        const sql = `
          INSERT INTO ${tableName} (COMPOSITE_HASH, IP_RANGE, FILEPATH, BYTES_USED, WINDOW_START, BLOCK_UNTIL)
          VALUES (?, ?, ?, ?, ?, NULL)
          ON CONFLICT (COMPOSITE_HASH) DO UPDATE SET
            BYTES_USED = CASE
              WHEN ? - ${tableName}.WINDOW_START >= ? THEN ?
              WHEN ${tableName}.BLOCK_UNTIL IS NOT NULL AND ${tableName}.BLOCK_UNTIL <= ? THEN ?
              WHEN ${tableName}.BYTES_USED >= ? THEN ${tableName}.BYTES_USED
              ELSE ${tableName}.BYTES_USED + ?
            END,
            WINDOW_START = CASE
              WHEN ? - ${tableName}.WINDOW_START >= ? THEN ?
              WHEN ${tableName}.BLOCK_UNTIL IS NOT NULL AND ${tableName}.BLOCK_UNTIL <= ? THEN ?
              ELSE ${tableName}.WINDOW_START
            END,
            BLOCK_UNTIL = CASE
              WHEN ? - ${tableName}.WINDOW_START >= ? THEN NULL
              WHEN ${tableName}.BLOCK_UNTIL IS NOT NULL AND ${tableName}.BLOCK_UNTIL <= ? THEN NULL
              WHEN ${tableName}.BYTES_USED + ? > ? AND ? > 0 THEN ? + ?
              ELSE ${tableName}.BLOCK_UNTIL
            END
          RETURNING BYTES_USED, WINDOW_START, BLOCK_UNTIL
        `;

        const params = [
          compositeHash,
          ipRange,
          filepath,
          byteCount,
          now,
          now,
          config.windowTimeFilepathSeconds,
          byteCount,
          now,
          byteCount,
          filepathQuotaLimit,
          byteCount,
          now,
          config.windowTimeFilepathSeconds,
          now,
          now,
          now,
          config.windowTimeFilepathSeconds,
          now,
          byteCount,
          filepathQuotaLimit,
          blockTimeSeconds,
          now,
          blockTimeSeconds,
        ];

        updates.push(
          executeQuery(accountId, databaseId, apiToken, sql, params).then((rows) => {
            const row = Array.isArray(rows) ? rows[0] : null;
            logQuotaUpdate({
              scope: 'filepath',
              key: filepath,
              bytesAdded: byteCount,
              limit: filepathQuotaLimit,
              windowSeconds: config.windowTimeFilepathSeconds,
              row,
              now,
            });
          })
        );
      }
    }

    if (updates.length > 0) {
      await Promise.all(updates);
    } else {
      console.log('[Bandwidth Quota D1 REST] Upsert skipped: no active quota scopes');
    }

    return { success: true };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error('[Bandwidth Quota D1 REST] Upsert failed:', message);
    return { success: false, error: message };
  }
};
