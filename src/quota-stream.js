const DEFAULT_UPDATE_INTERVAL_BASE = 30000;
const DEFAULT_UPDATE_INTERVAL_RANDOM = 5000;
const LOG_PREFIX = '[Quota Stream]';
const textEncoder = new TextEncoder();

/**
 * Safely determine the byte length of a stream chunk.
 * @param {any} chunk
 * @returns {number}
 */
const getChunkByteLength = (chunk) => {
  if (chunk === null || chunk === undefined) {
    return 0;
  }

  if (typeof chunk === 'string') {
    return textEncoder.encode(chunk).length;
  }

  if (typeof chunk.byteLength === 'number') {
    return chunk.byteLength;
  }

  if (ArrayBuffer.isView(chunk) && typeof chunk.buffer?.byteLength === 'number') {
    return chunk.byteLength;
  }

  if (chunk instanceof ArrayBuffer) {
    return chunk.byteLength;
  }

  return 0;
};

/**
 * Format cancellation reason for logging.
 * @param {any} reason
 * @returns {string}
 */
const formatReason = (reason) => {
  if (reason === undefined || reason === null) {
    return 'unknown';
  }

  if (typeof reason === 'string') {
    return reason;
  }

  if (typeof reason.message === 'string') {
    return reason.message;
  }

  try {
    return JSON.stringify(reason);
  } catch (_err) {
    return String(reason);
  }
};

/**
 * Validate a numeric value is finite and >= 0.
 * @param {any} value
 * @returns {boolean}
 */
const isNonNegativeNumber = (value) => Number.isFinite(value) && value >= 0;

/**
 * Validate a numeric value is finite and > 0.
 * @param {any} value
 * @returns {boolean}
 */
const isPositiveNumber = (value) => Number.isFinite(value) && value > 0;

/**
 * Safely schedule background work with optional waitUntil support.
 * @param {Function} promiseFactory
 * @param {string} errorLabel
 * @param {ExecutionContext|undefined} ctx
 * @param {Function|undefined} onMissingWaitUntil
 */
const scheduleBackgroundTask = (promiseFactory, errorLabel, ctx, onMissingWaitUntil) => {
  if (typeof promiseFactory !== 'function') {
    return;
  }

  let promise;
  try {
    promise = promiseFactory();
  } catch (error) {
    console.error(`${LOG_PREFIX} ${errorLabel}: ${error?.message || error}`);
    return;
  }

  if (!promise || typeof promise.then !== 'function') {
    return;
  }

  const monitoredPromise = promise.catch((error) => {
    console.error(`${LOG_PREFIX} ${errorLabel}: ${error?.message || error}`);
  });

  if (ctx && typeof ctx.waitUntil === 'function') {
    ctx.waitUntil(monitoredPromise);
  } else {
    if (typeof onMissingWaitUntil === 'function') {
      onMissingWaitUntil();
    } else {
      console.warn(`${LOG_PREFIX} ctx.waitUntil unavailable; background task runs synchronously`);
    }
  }
};

/**
 * Wraps a ReadableStream to monitor quota consumption
 * @param {ReadableStream} sourceStream - Original response body
 * @param {Object} options - Configuration
 * @param {string} options.ipRange - IP subnet (e.g., "192.168.1.0/24")
 * @param {string} options.ipRangeHash - SHA256 of IP subnet
 * @param {string} options.filepath - File path
 * @param {string} options.filepathHash - SHA256 of filepath
 * @param {number} options.expectedBytes - Pre-deducted bytes (full file or Range)
 * @param {Object} options.config - quotaConfig from worker
 * @param {Object} options.dbConfig - Database configuration
 * @param {Object} options.ctx - Cloudflare Workers context (for waitUntil)
 * @returns {ReadableStream} Wrapped stream
 */
export function wrapStreamWithQuotaMonitoring(sourceStream, options = {}) {
  if (!sourceStream || typeof sourceStream.pipeThrough !== 'function') {
    return sourceStream;
  }

  const {
    ipRange,
    ipRangeHash,
    filepath,
    filepathHash,
    expectedBytes = 0,
    config = {},
    dbConfig = {},
    ctx,
  } = options;

  if (!ipRangeHash || !filepathHash) {
    console.warn(`${LOG_PREFIX} Missing hash identifiers for quota tracking (ipRange: ${ipRange || 'unknown'}, filepath: ${filepath || 'unknown'})`);
    return sourceStream;
  }

  const updateIntervalBase = isPositiveNumber(config.updateIntervalBase) ? config.updateIntervalBase : DEFAULT_UPDATE_INTERVAL_BASE;
  const updateIntervalRandom = isNonNegativeNumber(config.updateIntervalRandom) ? config.updateIntervalRandom : DEFAULT_UPDATE_INTERVAL_RANDOM;
  const normalizedExpectedBytes = Number.isFinite(expectedBytes) ? Math.max(0, Math.floor(expectedBytes)) : 0;
  const expectedBytesDisplay = Number.isFinite(expectedBytes) ? normalizedExpectedBytes : 'unknown';

  const getNextUpdateDelay = () => updateIntervalBase + Math.floor(Math.random() * updateIntervalRandom);

  let actualBytes = 0;
  let lastReportedBytes = 0;
  let lastUpdateTime = Date.now();
  let nextUpdateDelay = getNextUpdateDelay();
  let waitUntilWarned = false;

  const warnMissingWaitUntil = () => {
    if (!waitUntilWarned) {
      console.warn(`${LOG_PREFIX} ctx.waitUntil unavailable; background updates may not complete if execution ends early`);
      waitUntilWarned = true;
    }
  };

  const performProgressUpdate = (label = 'Progress update failed') => {
    if (actualBytes === lastReportedBytes) {
      return;
    }

    if (!dbConfig?.dbMode) {
      return;
    }
    scheduleBackgroundTask(
      () => updateQuotaProgress(ipRangeHash, filepathHash, actualBytes, dbConfig),
      label,
      ctx,
      warnMissingWaitUntil
    );
    lastReportedBytes = actualBytes;
    lastUpdateTime = Date.now();
    nextUpdateDelay = getNextUpdateDelay();
    console.log(`${LOG_PREFIX} Progress updated: ${actualBytes}/${expectedBytesDisplay} bytes`);
  };

  const performRefund = (contextLabel) => {
    if (!dbConfig?.dbMode) {
      return;
    }

    const refundAmount = Math.max(0, normalizedExpectedBytes - Math.floor(actualBytes));
    if (refundAmount <= 0) {
      return;
    }

    console.log(`${LOG_PREFIX} Refunding ${refundAmount} bytes (${contextLabel})`);
    scheduleBackgroundTask(
      () => refundQuota(ipRangeHash, filepathHash, refundAmount, dbConfig),
      'Refund failed',
      ctx,
      warnMissingWaitUntil
    );
  };

  return sourceStream.pipeThrough(new TransformStream({
    transform(chunk, controller) {
      const chunkSize = getChunkByteLength(chunk);
      actualBytes += chunkSize;

      const now = Date.now();
      const elapsed = now - lastUpdateTime;

      if (elapsed >= nextUpdateDelay) {
        performProgressUpdate();
      }

      controller.enqueue(chunk);
    },

    flush() {
      console.log(`${LOG_PREFIX} Transfer complete: expected=${expectedBytesDisplay}, actual=${actualBytes}`);

      if (dbConfig?.dbMode) {
        performProgressUpdate('Final progress update failed');
      }

      if (actualBytes < normalizedExpectedBytes) {
        performRefund('complete');
      }
    },

    cancel(reason) {
      console.log(`${LOG_PREFIX} Transfer cancelled: expected=${expectedBytesDisplay}, actual=${actualBytes}, reason=${formatReason(reason)}`);

      if (dbConfig?.dbMode) {
        performProgressUpdate('Final progress update failed');
      }

      if (actualBytes < normalizedExpectedBytes) {
        performRefund('cancelled');
      }
    }
  }));
}

/**
 * Updates actual bytes transferred in database
 * @param {string} ipRangeHash
 * @param {string} filepathHash
 * @param {number} actualBytes - Current actual bytes transferred
 * @param {Object} dbConfig
 * @returns {Promise<void>}
 */
async function updateQuotaProgress(ipRangeHash, filepathHash, actualBytes, dbConfig) {
  const safeBytes = Math.max(0, Math.floor(actualBytes || 0));
  const now = Math.floor(Date.now() / 1000);

  if (!ipRangeHash || !filepathHash) {
    console.warn(`${LOG_PREFIX} Skipping quota progress update due to missing identifiers`);
    return;
  }

  if (!dbConfig?.dbMode) {
    console.warn(`${LOG_PREFIX} Skipping quota progress update due to missing dbMode`);
    return;
  }

  if (dbConfig.dbMode === 'custom-pg-rest') {
    if (!dbConfig.postgrestUrl) {
      console.warn(`${LOG_PREFIX} PostgREST URL missing, cannot update quota progress`);
      return;
    }

    const response = await fetch(`${dbConfig.postgrestUrl}/rpc/download_update_quota_progress`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Prefer': 'return=representation'
      },
      body: JSON.stringify({
        p_ip_range_hash: ipRangeHash,
        p_filepath_hash: filepathHash,
        p_update_bytes: safeBytes,
        p_now: now,
        p_file_table_name: 'file_ip_download_quota',
        p_global_table_name: 'global_ip_download_quota'
      })
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`PostgREST error: ${response.status} ${errorText}`);
    }

    return;
  }

  if (dbConfig.dbMode === 'd1') {
    const bindingName = dbConfig.databaseBinding;
    const db = bindingName ? dbConfig.env?.[bindingName] : undefined;

    if (!db) {
      throw new Error('D1 binding not found for quota progress update');
    }

    await db.prepare(`
      UPDATE file_ip_download_quota
      SET bytes_downloaded = ?
      WHERE ip_range_hash = ? AND filepath_hash = ?
    `).bind(safeBytes, ipRangeHash, filepathHash).run();

    await db.prepare(`
      UPDATE global_ip_download_quota
      SET total_bytes_downloaded = total_bytes_downloaded + ?
      WHERE ip_range_hash = ?
    `).bind(safeBytes, ipRangeHash).run();

    return;
  }

  if (dbConfig.dbMode === 'd1-rest') {
    await executeD1RestStatement(dbConfig, `
      UPDATE file_ip_download_quota
      SET bytes_downloaded = ?
      WHERE ip_range_hash = ? AND filepath_hash = ?
    `, [safeBytes, ipRangeHash, filepathHash]);

    await executeD1RestStatement(dbConfig, `
      UPDATE global_ip_download_quota
      SET total_bytes_downloaded = total_bytes_downloaded + ?
      WHERE ip_range_hash = ?
    `, [safeBytes, ipRangeHash]);

    return;
  }

  console.warn(`${LOG_PREFIX} Unsupported dbMode (${dbConfig.dbMode}) for quota progress update`);
}

/**
 * Refunds unused quota (expected - actual)
 * @param {string} ipRangeHash
 * @param {string} filepathHash
 * @param {number} refundAmount - Bytes to refund
 * @param {Object} dbConfig
 * @returns {Promise<void>}
 */
async function refundQuota(ipRangeHash, filepathHash, refundAmount, dbConfig) {
  const safeRefund = Math.max(0, Math.floor(refundAmount || 0));

  if (safeRefund <= 0) {
    return;
  }

  if (!ipRangeHash || !filepathHash) {
    console.warn(`${LOG_PREFIX} Skipping quota refund due to missing identifiers`);
    return;
  }

  if (!dbConfig?.dbMode) {
    console.warn(`${LOG_PREFIX} Skipping quota refund due to missing dbMode`);
    return;
  }

  if (dbConfig.dbMode === 'custom-pg-rest') {
    if (!dbConfig.postgrestUrl) {
      console.warn(`${LOG_PREFIX} PostgREST URL missing, cannot refund quota`);
      return;
    }

    const response = await fetch(`${dbConfig.postgrestUrl}/rpc/download_refund_quota`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Prefer': 'return=representation'
      },
      body: JSON.stringify({
        p_ip_range_hash: ipRangeHash,
        p_filepath_hash: filepathHash,
        p_refund_bytes: safeRefund,
        p_file_table_name: 'file_ip_download_quota',
        p_global_table_name: 'global_ip_download_quota'
      })
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`PostgREST error: ${response.status} ${errorText}`);
    }

    return;
  }

  if (dbConfig.dbMode === 'd1') {
    const bindingName = dbConfig.databaseBinding;
    const db = bindingName ? dbConfig.env?.[bindingName] : undefined;

    if (!db) {
      throw new Error('D1 binding not found for quota refund');
    }

    await db.prepare(`
      UPDATE file_ip_download_quota
      SET bytes_downloaded = MAX(0, bytes_downloaded - ?)
      WHERE ip_range_hash = ? AND filepath_hash = ?
    `).bind(safeRefund, ipRangeHash, filepathHash).run();

    await db.prepare(`
      UPDATE global_ip_download_quota
      SET total_bytes_downloaded = MAX(0, total_bytes_downloaded - ?)
      WHERE ip_range_hash = ?
    `).bind(safeRefund, ipRangeHash).run();

    return;
  }

  if (dbConfig.dbMode === 'd1-rest') {
    await executeD1RestStatement(dbConfig, `
      UPDATE file_ip_download_quota
      SET bytes_downloaded = MAX(0, bytes_downloaded - ?)
      WHERE ip_range_hash = ? AND filepath_hash = ?
    `, [safeRefund, ipRangeHash, filepathHash]);

    await executeD1RestStatement(dbConfig, `
      UPDATE global_ip_download_quota
      SET total_bytes_downloaded = MAX(0, total_bytes_downloaded - ?)
      WHERE ip_range_hash = ?
    `, [safeRefund, ipRangeHash]);

    return;
  }

  console.warn(`${LOG_PREFIX} Unsupported dbMode (${dbConfig.dbMode}) for quota refund`);
}

/**
 * Execute a single SQL statement via D1 REST API.
 * @param {Object} dbConfig
 * @param {string} sql
 * @param {Array} params
 * @returns {Promise<void>}
 */
async function executeD1RestStatement(dbConfig, sql, params = []) {
  const { accountId, databaseId, apiToken } = dbConfig || {};

  if (!accountId || !databaseId || !apiToken) {
    throw new Error('D1 REST configuration incomplete');
  }

  const endpoint = `https://api.cloudflare.com/client/v4/accounts/${accountId}/d1/database/${databaseId}/query`;

  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${apiToken}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      sql,
      params
    })
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`D1 REST error: ${response.status} ${errorText}`);
  }

  const result = await response.json();

  if (!result.success) {
    const errors = Array.isArray(result.errors) ? result.errors : [];
    const message = errors.map(err => err.message).join(', ') || 'Unknown error';
    throw new Error(`D1 REST query failed: ${message}`);
  }
}
