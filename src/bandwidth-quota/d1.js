import { sha256Hash } from '../utils.js';

const DEFAULT_IPRANGE_TABLE = 'IPRANGE_BANDWIDTH_QUOTA_TABLE';
const DEFAULT_FILEPATH_TABLE = 'IPRANGE_FILEPATH_BANDWIDTH_QUOTA_TABLE';

const ensureIprangeTable = async (db, tableName) => {
  await db.prepare(`
    CREATE TABLE IF NOT EXISTS ${tableName} (
      IP_HASH TEXT PRIMARY KEY,
      IP_RANGE TEXT NOT NULL,
      BYTES_USED INTEGER NOT NULL DEFAULT 0,
      WINDOW_START INTEGER NOT NULL,
      BLOCK_UNTIL INTEGER
    )
  `).run();

  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_${tableName.toLowerCase()}_window ON ${tableName}(WINDOW_START)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_${tableName.toLowerCase()}_block ON ${tableName}(BLOCK_UNTIL) WHERE BLOCK_UNTIL IS NOT NULL`).run();
};

const ensureFilepathTable = async (db, tableName) => {
  await db.prepare(`
    CREATE TABLE IF NOT EXISTS ${tableName} (
      COMPOSITE_HASH TEXT PRIMARY KEY,
      IP_RANGE TEXT NOT NULL,
      FILEPATH TEXT NOT NULL,
      BYTES_USED INTEGER NOT NULL DEFAULT 0,
      WINDOW_START INTEGER NOT NULL,
      BLOCK_UNTIL INTEGER
    )
  `).run();

  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_${tableName.toLowerCase()}_window ON ${tableName}(WINDOW_START)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_${tableName.toLowerCase()}_block ON ${tableName}(BLOCK_UNTIL) WHERE BLOCK_UNTIL IS NOT NULL`).run();
};

const calculateFilepathQuota = (config, filesize) => {
  if (!config || config.value <= 0) {
    return 0;
  }

  if (config.type === 'dynamic') {
    if (!filesize || filesize <= 0) {
      console.warn('[Bandwidth Quota D1] Dynamic quota requires filesize, skipping filepath quota update');
      return 0;
    }
    const quota = Math.floor(filesize * config.value);
    console.log(`[Bandwidth Quota D1] Dynamic filepath quota: ${filesize} bytes * ${config.value} = ${quota} bytes`);
    return quota;
  }

  console.log(`[Bandwidth Quota D1] Static filepath quota: ${config.value} bytes`);
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
    console.log(`[Bandwidth Quota D1] ${scope} quota write: key=${key} bytesAdded=${bytesAdded} (no row returned)`);
    return;
  }

  const bytesUsed = parseInteger(row.BYTES_USED ?? row.bytes_used ?? row.bytesUsed, 0);
  const windowStart = parseInteger(row.WINDOW_START ?? row.window_start ?? row.windowStart, 0);
  const blockUntilRaw = row.BLOCK_UNTIL ?? row.block_until ?? row.blockUntil;
  const blockUntil = Number.isFinite(blockUntilRaw) ? blockUntilRaw : parseInteger(blockUntilRaw, null);
  const blockActive = Number.isFinite(blockUntil) && blockUntil > now;
  const shouldBlock = limit > 0 && windowSeconds > 0 && bytesUsed >= limit;
  const segments = [
    `[Bandwidth Quota D1] ${scope} quota write`,
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
  if (!config || !config.env || !config.databaseBinding) {
    console.warn('[Bandwidth Quota D1] Missing configuration, skipping quota update');
    return { success: false };
  }

  const normalizedBytes = Number.isFinite(bytes) ? Math.trunc(bytes) : bytes;
  console.log(
    '[Bandwidth Quota D1] Upsert request:',
    `ipRange=${ipRange || 'N/A'}`,
    `filepath=${filepath || 'N/A'}`,
    `bytes=${normalizedBytes}`
  );

  if (!ipRange || !Number.isFinite(bytes) || bytes <= 0) {
    console.log('[Bandwidth Quota D1] Upsert skipped: invalid ipRange or bytes');
    return { success: true };
  }

  try {
    const db = config.env[config.databaseBinding];
    if (!db) {
      throw new Error(`D1 database binding "${config.databaseBinding}" not found`);
    }

    const now = Math.floor(Date.now() / 1000);
    const byteCount = Math.max(0, Math.trunc(bytes));
    const totalQuotaActive = Boolean(config.totalEnabled) && config.iprangeLimit > 0 && config.windowTimeTotalSeconds > 0;
    const filepathQuotaLimit = config.filepathEnabled
      ? calculateFilepathQuota(config.filepathLimitConfig, filesize)
      : 0;
    const filepathQuotaActive =
      Boolean(config.filepathEnabled) &&
      filepath &&
      config.windowTimeFilepathSeconds > 0 &&
      filepathQuotaLimit > 0;

    if (!totalQuotaActive && !filepathQuotaActive) {
      console.log('[Bandwidth Quota D1] Upsert skipped: no active quota scopes');
      return { success: true };
    }

    if (totalQuotaActive) {
      await ensureIprangeTable(db, config.iprangeTableName || DEFAULT_IPRANGE_TABLE);

      const ipHash = await sha256Hash(ipRange);
      if (!ipHash) {
        console.error('[Bandwidth Quota D1] Failed to compute IP hash');
      } else {
        const tableName = config.iprangeTableName || DEFAULT_IPRANGE_TABLE;
        const blockTimeSeconds = config.blockTimeSeconds || 0;
        const stmt = db.prepare(`
          INSERT INTO ${tableName} (IP_HASH, IP_RANGE, BYTES_USED, WINDOW_START, BLOCK_UNTIL)
          VALUES (?1, ?2, ?3, ?4, NULL)
          ON CONFLICT(IP_HASH) DO UPDATE SET
            BYTES_USED = CASE
              WHEN ?4 - ${tableName}.WINDOW_START >= ?5 THEN ?3
              WHEN ${tableName}.BLOCK_UNTIL IS NOT NULL AND ${tableName}.BLOCK_UNTIL <= ?4 THEN ?3
              WHEN ${tableName}.BYTES_USED >= ?6 THEN ${tableName}.BYTES_USED
              ELSE ${tableName}.BYTES_USED + ?3
            END,
            WINDOW_START = CASE
              WHEN ?4 - ${tableName}.WINDOW_START >= ?5 THEN ?4
              WHEN ${tableName}.BLOCK_UNTIL IS NOT NULL AND ${tableName}.BLOCK_UNTIL <= ?4 THEN ?4
              ELSE ${tableName}.WINDOW_START
            END,
            BLOCK_UNTIL = CASE
              WHEN ?4 - ${tableName}.WINDOW_START >= ?5 THEN NULL
              WHEN ${tableName}.BLOCK_UNTIL IS NOT NULL AND ${tableName}.BLOCK_UNTIL <= ?4 THEN NULL
              WHEN ${tableName}.BYTES_USED + ?3 > ?6 AND ?7 > 0 THEN ?4 + ?7
              ELSE ${tableName}.BLOCK_UNTIL
            END
          RETURNING BYTES_USED, WINDOW_START, BLOCK_UNTIL
        `);

        const result = await stmt
          .bind(
            ipHash,
            ipRange,
            byteCount,
            now,
            config.windowTimeTotalSeconds,
            config.iprangeLimit,
            blockTimeSeconds
          )
          .all();

        const row = result?.results?.[0] || null;
        logQuotaUpdate({
          scope: 'iprange',
          key: ipRange,
          bytesAdded: byteCount,
          limit: config.iprangeLimit,
          windowSeconds: config.windowTimeTotalSeconds,
          row,
          now,
        });
      }
    }

    if (filepathQuotaActive) {
      await ensureFilepathTable(db, config.filepathTableName || DEFAULT_FILEPATH_TABLE);

      const compositeHash = await sha256Hash(`${ipRange}${filepath}`);
      if (!compositeHash) {
        console.error('[Bandwidth Quota D1] Failed to compute composite hash');
      } else {
        const tableName = config.filepathTableName || DEFAULT_FILEPATH_TABLE;
        const blockTimeSeconds = config.blockTimeSeconds || 0;
        const stmt = db.prepare(`
          INSERT INTO ${tableName} (COMPOSITE_HASH, IP_RANGE, FILEPATH, BYTES_USED, WINDOW_START, BLOCK_UNTIL)
          VALUES (?1, ?2, ?3, ?4, ?5, NULL)
          ON CONFLICT(COMPOSITE_HASH) DO UPDATE SET
            BYTES_USED = CASE
              WHEN ?5 - ${tableName}.WINDOW_START >= ?6 THEN ?4
              WHEN ${tableName}.BLOCK_UNTIL IS NOT NULL AND ${tableName}.BLOCK_UNTIL <= ?5 THEN ?4
              WHEN ${tableName}.BYTES_USED >= ?7 THEN ${tableName}.BYTES_USED
              ELSE ${tableName}.BYTES_USED + ?4
            END,
            WINDOW_START = CASE
              WHEN ?5 - ${tableName}.WINDOW_START >= ?6 THEN ?5
              WHEN ${tableName}.BLOCK_UNTIL IS NOT NULL AND ${tableName}.BLOCK_UNTIL <= ?5 THEN ?5
              ELSE ${tableName}.WINDOW_START
            END,
            BLOCK_UNTIL = CASE
              WHEN ?5 - ${tableName}.WINDOW_START >= ?6 THEN NULL
              WHEN ${tableName}.BLOCK_UNTIL IS NOT NULL AND ${tableName}.BLOCK_UNTIL <= ?5 THEN NULL
              WHEN ${tableName}.BYTES_USED + ?4 > ?7 AND ?8 > 0 THEN ?5 + ?8
              ELSE ${tableName}.BLOCK_UNTIL
            END
          RETURNING BYTES_USED, WINDOW_START, BLOCK_UNTIL
        `);

        const result = await stmt
          .bind(
            compositeHash,
            ipRange,
            filepath,
            byteCount,
            now,
            config.windowTimeFilepathSeconds,
            filepathQuotaLimit,
            blockTimeSeconds
          )
          .all();

        const row = result?.results?.[0] || null;
        logQuotaUpdate({
          scope: 'filepath',
          key: filepath,
          bytesAdded: byteCount,
          limit: filepathQuotaLimit,
          windowSeconds: config.windowTimeFilepathSeconds,
          row,
          now,
        });
      }
    }

    return { success: true };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error('[Bandwidth Quota D1] Upsert failed:', message);
    return { success: false, error: message };
  }
};
