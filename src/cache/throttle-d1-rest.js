import { sha256Hash } from '../utils.js';

const sanitizeThresholds = (config) => {
  const toInt = (value, fallback) => {
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) ? parsed : fallback;
  };

  return {
    throttleTimeWindow: Math.max(1, toInt(config.throttleTimeWindow, 60)),
    observeWindowSeconds: Math.max(1, toInt(config.observeWindowSeconds, 60)),
    errorRatioPercent: Math.max(0, toInt(config.errorRatioPercent, 20)),
    consecutiveThreshold: Math.max(1, toInt(config.consecutiveThreshold, 4)),
    minSampleCount: Math.max(1, toInt(config.minSampleCount, 8)),
  };
};

const computeNextState = (existing, event, thresholds) => {
  const state = {
    hostname: existing.hostname,
    errorTimestamp: existing.errorTimestamp ?? null,
    isProtected: existing.isProtected ?? 0,
    lastErrorCode: existing.lastErrorCode ?? null,
    obsWindowStart: existing.obsWindowStart ?? event.now,
    obsErrorCount: existing.obsErrorCount ?? 0,
    obsSuccessCount: existing.obsSuccessCount ?? 0,
    consecutiveErrorCount: existing.consecutiveErrorCount ?? 0,
  };

  if (!existing.exists) {
    state.isProtected = 0;
    state.errorTimestamp = null;
    state.lastErrorCode = null;
    state.obsWindowStart = event.now;
    state.obsErrorCount = 0;
    state.obsSuccessCount = 0;
    state.consecutiveErrorCount = 0;
  }

  if (!state.obsWindowStart || (event.now - state.obsWindowStart) >= thresholds.observeWindowSeconds) {
    state.obsWindowStart = event.now;
    state.obsErrorCount = 0;
    state.obsSuccessCount = 0;
    // 连续错误计数只在成功时清零
  }

  if (event.isError) {
    state.obsErrorCount += 1;
    state.consecutiveErrorCount += 1;
  } else {
    state.obsSuccessCount += 1;
    state.consecutiveErrorCount = 0;
  }

  let ratioTrigger = false;
  let consecutiveTrigger = false;
  let shouldProtect = false;

  if (event.isError) {
    const total = state.obsErrorCount + state.obsSuccessCount;
    if (total >= thresholds.minSampleCount) {
      ratioTrigger = (state.obsErrorCount * 100) >= (thresholds.errorRatioPercent * total);
    }
    consecutiveTrigger = state.consecutiveErrorCount >= thresholds.consecutiveThreshold;
    shouldProtect = ratioTrigger || consecutiveTrigger;
  }

  if (shouldProtect) {
    state.isProtected = 1;
    state.errorTimestamp = event.now;
    state.lastErrorCode = event.statusCode ?? state.lastErrorCode;
  } else if (state.isProtected === 1 && state.errorTimestamp !== null) {
    if ((event.now - state.errorTimestamp) >= thresholds.throttleTimeWindow) {
      state.isProtected = 0;
      state.errorTimestamp = null;
      state.lastErrorCode = null;
      state.obsWindowStart = event.now;
      state.obsErrorCount = 0;
      state.obsSuccessCount = 0;
    } else {
      state.isProtected = 1;
    }
  } else {
    state.isProtected = 0;
    if (event.isError) {
      state.lastErrorCode = event.statusCode ?? state.lastErrorCode;
    }
  }

  return state;
};

/**
 * Execute SQL query via D1 REST API
 * @param {string} accountId - Cloudflare account ID
 * @param {string} databaseId - D1 database ID
 * @param {string} apiToken - Cloudflare API token
 * @param {string} sql - SQL query
 * @param {Array} params - Query parameters
 * @returns {Promise<Object>} - Query result
 */
const executeQuery = async (accountId, databaseId, apiToken, sql, params = []) => {
  const url = `https://api.cloudflare.com/client/v4/accounts/${accountId}/d1/database/${databaseId}/query`;

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${apiToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      sql,
      params,
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    console.error(`[Throttle] D1 REST API error (${response.status}): ${errorText}`);
    return { results: [], meta: {} };
  }

  const result = await response.json();

  if (!result.success) {
    const errors = result.errors || [];
    const errorMessage = errors.map(e => e.message).join(', ');
    console.error(`[Throttle] D1 REST API query failed: ${errorMessage}`);
    return { results: [], meta: {} };
  }

  return result.result?.[0] || { results: [], meta: {} };
};

const addColumnIfMissing = async (accountId, databaseId, apiToken, tableName, columnDefinition) => {
  try {
    await executeQuery(accountId, databaseId, apiToken, `ALTER TABLE ${tableName} ADD COLUMN ${columnDefinition}`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (!/duplicate column|already exists/i.test(message)) {
      console.warn('[Throttle] Failed to add column via D1 REST:', columnDefinition, message);
    }
  }
};

/**
 * Ensure the THROTTLE_PROTECTION table exists in the database
 * @param {string} accountId
 * @param {string} databaseId
 * @param {string} apiToken
 * @param {string} tableName
 * @returns {Promise<void>}
 */
const ensureTable = async (accountId, databaseId, apiToken, tableName) => {
  const createTableSql = `
    CREATE TABLE IF NOT EXISTS ${tableName} (
      HOSTNAME_HASH TEXT PRIMARY KEY,
      HOSTNAME TEXT NOT NULL,
      ERROR_TIMESTAMP INTEGER,
      IS_PROTECTED INTEGER,
      LAST_ERROR_CODE INTEGER,
      OBS_WINDOW_START INTEGER,
      OBS_ERROR_COUNT INTEGER NOT NULL DEFAULT 0,
      OBS_SUCCESS_COUNT INTEGER NOT NULL DEFAULT 0,
      CONSECUTIVE_ERROR_COUNT INTEGER NOT NULL DEFAULT 0
    )
  `;
  await executeQuery(accountId, databaseId, apiToken, createTableSql);

  const indexSql = `CREATE INDEX IF NOT EXISTS idx_throttle_timestamp ON ${tableName}(ERROR_TIMESTAMP)`;
  await executeQuery(accountId, databaseId, apiToken, indexSql);

  await addColumnIfMissing(accountId, databaseId, apiToken, tableName, 'OBS_WINDOW_START INTEGER');
  await addColumnIfMissing(accountId, databaseId, apiToken, tableName, 'OBS_ERROR_COUNT INTEGER NOT NULL DEFAULT 0');
  await addColumnIfMissing(accountId, databaseId, apiToken, tableName, 'OBS_SUCCESS_COUNT INTEGER NOT NULL DEFAULT 0');
  await addColumnIfMissing(accountId, databaseId, apiToken, tableName, 'CONSECUTIVE_ERROR_COUNT INTEGER NOT NULL DEFAULT 0');
};

/**
 * Check throttle protection status for a hostname
 * @param {string} hostname - Hostname to check
 * @param {Object} config - Throttle configuration
 * @param {string} config.accountId - Cloudflare account ID
 * @param {string} config.databaseId - D1 database ID
 * @param {string} config.apiToken - Cloudflare API token
 * @param {string} config.tableName - Table name (defaults to 'THROTTLE_PROTECTION')
 * @param {number} config.throttleTimeWindow - Time window in seconds
 * @returns {Promise<{status: 'normal_operation'|'resume_operation'|'protected', recordExists: boolean, errorCode?: number, retryAfter?: number} | null>}
 */
export const checkThrottle = async (hostname, config) => {
  if (!config.accountId || !config.databaseId || !config.apiToken || !config.throttleTimeWindow) {
    return null;
  }

  if (!hostname || typeof hostname !== 'string') {
    return null;
  }

  try {
    const { accountId, databaseId, apiToken } = config;
    const tableName = config.tableName || 'THROTTLE_PROTECTION';

    await ensureTable(accountId, databaseId, apiToken, tableName);

    // Calculate hostname hash
    const hostnameHash = await sha256Hash(hostname);
    if (!hostnameHash) {
      console.error('[Throttle] Failed to calculate hostname hash');
      return null;
    }

    // Query throttle protection status
    const selectSql = `SELECT HOSTNAME_HASH, HOSTNAME, ERROR_TIMESTAMP, IS_PROTECTED, LAST_ERROR_CODE FROM ${tableName} WHERE HOSTNAME_HASH = ?`;
    const queryResult = await executeQuery(accountId, databaseId, apiToken, selectSql, [hostnameHash]);

    const results = queryResult.results || [];

    if (!results || results.length === 0) {
      // No record found - normal operation (first time)
      return { status: 'normal_operation', recordExists: false };
    }

    const result = results[0];

    // BREAKING CHANGE: IS_PROTECTED semantics
    //   1 = protected (error detected)
    //   0 = normal operation (initialized or recovered)
    //   NULL = invalid state (should not exist in valid records)
    // Check protection status
    if (result.IS_PROTECTED === 0) {
      // Normal operation (record exists with IS_PROTECTED = 0)
      return { status: 'normal_operation', recordExists: true };
    } else if (result.IS_PROTECTED !== 1) {
      // IS_PROTECTED is NULL or other invalid value - treat as normal but log warning
      console.warn('[Throttle] Invalid IS_PROTECTED value:', result.IS_PROTECTED, 'for hostname:', hostname);
      return { status: 'normal_operation', recordExists: true };
    }

    // Protected - check time window
    const now = Math.floor(Date.now() / 1000);
    const errorTimestamp = Number.parseInt(result.ERROR_TIMESTAMP, 10);
    const timeSinceError = now - errorTimestamp;

    if (timeSinceError >= config.throttleTimeWindow) {
      // Time window expired - resume operation
      return { status: 'resume_operation', recordExists: true };
    } else {
      // Still within time window - protected
      const retryAfter = config.throttleTimeWindow - timeSinceError;
      return {
        status: 'protected',
        recordExists: true,
        errorCode: result.LAST_ERROR_CODE || 503,
        retryAfter,
      };
    }
  } catch (error) {
    console.error('[Throttle] Check failed:', error.message);
    return null;
  }
};

/**
 * Update throttle protection status for a hostname
 * @param {string} hostname - Hostname
 * @param {Object} updateData - Update data
 * @param {'error'|'success'} updateData.eventType - Event type
 * @param {number} updateData.statusCode - HTTP status code (or null)
 * @param {Object} config - Throttle configuration
 * @returns {Promise<void>}
 */
export const updateThrottle = async (hostname, updateData, config) => {
  if (!config.accountId || !config.databaseId || !config.apiToken) {
    return;
  }

  if (!hostname || typeof hostname !== 'string') {
    return;
  }

  const eventType = updateData?.eventType;
  const isError = eventType === 'error';
  const isSuccess = eventType === 'success';

  if (!isError && !isSuccess) {
    console.warn('[Throttle] Skipping updateThrottle due to invalid eventType:', eventType);
    return;
  }

  const statusCode = Number.isFinite(updateData?.statusCode)
    ? Number(updateData.statusCode)
    : Number.parseInt(updateData?.statusCode, 10);

  if (!Number.isFinite(statusCode)) {
    console.warn('[Throttle] Skip updateThrottle: invalid statusCode:', updateData?.statusCode);
    return;
  }

  try {
    const { accountId, databaseId, apiToken } = config;
    const tableName = config.tableName || 'THROTTLE_PROTECTION';
    const thresholds = sanitizeThresholds(config);
    const now = Math.floor(Date.now() / 1000);

    await ensureTable(accountId, databaseId, apiToken, tableName);

    // Calculate hostname hash
    const hostnameHash = await sha256Hash(hostname);
    if (!hostnameHash) {
      console.error('[Throttle] Failed to calculate hostname hash');
      return;
    }

    // Probabilistic cleanup helper
    const triggerCleanup = () => {
      const probability = config.cleanupProbability || 0.01;
      if (Math.random() < probability) {
        console.log(`[Throttle Cleanup] Triggered cleanup (probability: ${probability * 100}%)`);

        const cleanupPromise = cleanupExpiredThrottle(accountId, databaseId, apiToken, tableName, thresholds.throttleTimeWindow)
          .then((deletedCount) => {
            console.log(`[Throttle Cleanup] Background cleanup finished: ${deletedCount} records deleted`);
            return deletedCount;
          })
          .catch((error) => {
            console.error('[Throttle Cleanup] Background cleanup failed:', error instanceof Error ? error.message : String(error));
          });

        if (config.ctx && config.ctx.waitUntil) {
          config.ctx.waitUntil(cleanupPromise);
          console.log(`[Throttle Cleanup] Cleanup scheduled in background (using ctx.waitUntil)`);
        } else {
          console.warn(`[Throttle Cleanup] No ctx.waitUntil available, cleanup may be interrupted`);
        }
      }
    };

    const selectSql = `SELECT HOSTNAME, ERROR_TIMESTAMP, IS_PROTECTED, LAST_ERROR_CODE, OBS_WINDOW_START, OBS_ERROR_COUNT, OBS_SUCCESS_COUNT, CONSECUTIVE_ERROR_COUNT FROM ${tableName} WHERE HOSTNAME_HASH = ? LIMIT 1`;
    const existingResult = await executeQuery(accountId, databaseId, apiToken, selectSql, [hostnameHash]);
    const existingRow = (existingResult.results || [])[0];

    const nextState = computeNextState(
      {
        hostname,
        errorTimestamp: existingRow?.ERROR_TIMESTAMP,
        isProtected: existingRow?.IS_PROTECTED,
        lastErrorCode: existingRow?.LAST_ERROR_CODE,
        obsWindowStart: existingRow?.OBS_WINDOW_START,
        obsErrorCount: existingRow?.OBS_ERROR_COUNT,
        obsSuccessCount: existingRow?.OBS_SUCCESS_COUNT,
        consecutiveErrorCount: existingRow?.CONSECUTIVE_ERROR_COUNT,
        exists: Boolean(existingRow),
      },
      {
        isError,
        statusCode: statusCode ?? null,
        now,
      },
      thresholds
    );

    // Atomic UPSERT - D1 REST API doesn't support RETURNING in the same way,
    // but the operation is still atomic
    const upsertSql = `
      INSERT INTO ${tableName} (
        HOSTNAME_HASH,
        HOSTNAME,
        ERROR_TIMESTAMP,
        IS_PROTECTED,
        LAST_ERROR_CODE,
        OBS_WINDOW_START,
        OBS_ERROR_COUNT,
        OBS_SUCCESS_COUNT,
        CONSECUTIVE_ERROR_COUNT
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT (HOSTNAME_HASH) DO UPDATE SET
        HOSTNAME = excluded.HOSTNAME,
        ERROR_TIMESTAMP = excluded.ERROR_TIMESTAMP,
        IS_PROTECTED = excluded.IS_PROTECTED,
        LAST_ERROR_CODE = excluded.LAST_ERROR_CODE,
        OBS_WINDOW_START = excluded.OBS_WINDOW_START,
        OBS_ERROR_COUNT = excluded.OBS_ERROR_COUNT,
        OBS_SUCCESS_COUNT = excluded.OBS_SUCCESS_COUNT,
        CONSECUTIVE_ERROR_COUNT = excluded.CONSECUTIVE_ERROR_COUNT
    `;

    const upsertResult = await executeQuery(
      accountId,
      databaseId,
      apiToken,
      upsertSql,
      [
        hostnameHash,
        nextState.hostname,
        nextState.errorTimestamp,
        nextState.isProtected,
        nextState.lastErrorCode,
        nextState.obsWindowStart,
        nextState.obsErrorCount,
        nextState.obsSuccessCount,
        nextState.consecutiveErrorCount,
      ]
    );

    const changed = upsertResult.meta?.changes;
    console.log(
      `[Throttle] Updated protection for ${hostname}: isProtected=${nextState.isProtected}, errorCode=${nextState.lastErrorCode}, metaChanges=${changed}`
    );

    // Trigger cleanup probabilistically
    triggerCleanup();
  } catch (error) {
    console.error('[Throttle] Update failed:', error.message);
    // Don't propagate error - throttle failure should not block downloads
  }
};

/**
 * Clean up expired records from the database
 * BREAKING CHANGE: Removes records where IS_PROTECTED = 0 and ERROR_TIMESTAMP is older than throttleTimeWindow * 2
 * @param {string} accountId - Cloudflare account ID
 * @param {string} databaseId - D1 database ID
 * @param {string} apiToken - Cloudflare API token
 * @param {string} tableName - Table name
 * @param {number} throttleTimeWindow - Time window in seconds
 * @returns {Promise<number>} - Number of deleted records
 */
const cleanupExpiredThrottle = async (accountId, databaseId, apiToken, tableName, throttleTimeWindow) => {
  const now = Math.floor(Date.now() / 1000);
  const cutoffTime = now - (throttleTimeWindow * 2);

  try {
    console.log(`[Throttle Cleanup] Executing DELETE query (cutoff: ${cutoffTime}, timeWindow: ${throttleTimeWindow}s)`);

    // Delete records where IS_PROTECTED = 0 and ERROR_TIMESTAMP is old or NULL
    const deleteSql = `
      DELETE FROM ${tableName}
      WHERE IS_PROTECTED = 0
        AND (ERROR_TIMESTAMP IS NULL OR ERROR_TIMESTAMP < ?)
    `;

    const result = await executeQuery(accountId, databaseId, apiToken, deleteSql, [cutoffTime]);

    const deletedCount = result.meta?.changes || 0;
    console.log(`[Throttle Cleanup] DELETE completed: ${deletedCount} expired records deleted (older than ${throttleTimeWindow * 2}s)`);

    return deletedCount;
  } catch (error) {
    // Log error but don't propagate (cleanup failure shouldn't block requests)
    console.error('[Throttle Cleanup] DELETE failed:', error instanceof Error ? error.message : String(error));
    return 0;
  }
};
