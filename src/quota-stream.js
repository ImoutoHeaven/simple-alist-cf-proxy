import { checkQuotaBalance, deleteQuotaBlock, insertQuotaSettlement } from './quota-service.js';

const LOG_PREFIX = '[Quota Stream]';
const textEncoder = new TextEncoder();

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
  } catch (_error) {
    return String(reason);
  }
};

const toSafeInteger = (value) => {
  if (!Number.isFinite(value)) {
    return 0;
  }
  if (value <= 0) {
    return 0;
  }
  return Math.floor(value);
};

const isWindowActive = (windowSeconds, windowStartTime) => {
  if (!Number.isFinite(windowSeconds) || windowSeconds <= 0) {
    return false;
  }
  if (!Number.isFinite(windowStartTime) || windowStartTime <= 0) {
    return false;
  }
  const elapsed = Date.now() - windowStartTime;
  return elapsed <= (windowSeconds * 1000);
};

const buildWindowList = (windowSecondsList, balanceParams = {}) => {
  if (Array.isArray(windowSecondsList) && windowSecondsList.length > 0) {
    return windowSecondsList;
  }

  const windows = [];
  if (Number.isFinite(balanceParams.fileWindowSeconds)) {
    windows.push(balanceParams.fileWindowSeconds);
  }
  if (Number.isFinite(balanceParams.globalWindowSeconds)) {
    windows.push(balanceParams.globalWindowSeconds);
  }
  return windows;
};

export function wrapStreamWithQuotaMonitoring(sourceStream, options = {}) {
  if (!sourceStream || typeof sourceStream.pipeThrough !== 'function') {
    return sourceStream;
  }

  const {
    requestId,
    ipRangeHash,
    filepathHash,
    expectedBytes = 0,
    windowStartTime = Date.now(),
    windowSecondsList = [],
    balanceParams = {},
    dbConfig = {},
    quotaConfig = {},
    ctx,
  } = options;

  if (!requestId || !ipRangeHash || !filepathHash) {
    console.warn(`${LOG_PREFIX} Missing request identifiers, skipping quota settlement`);
    return sourceStream;
  }

  let actualBytes = 0;
  let finalized = false;

  const windowsToCheck = buildWindowList(windowSecondsList, balanceParams);

  const finalizeQuota = async (contextLabel) => {
    if (finalized) {
      return;
    }
    finalized = true;

    const normalizedExpected = toSafeInteger(expectedBytes);
    const normalizedActual = toSafeInteger(actualBytes);
    const refundAmount = Math.max(0, normalizedExpected - normalizedActual);

    if (refundAmount <= 0) {
      return;
    }

    const withinWindow = windowsToCheck.some((seconds) => isWindowActive(seconds, windowStartTime));
    if (!withinWindow) {
      console.log(`${LOG_PREFIX} Window expired, skip refund for ${contextLabel}`);
      return;
    }

    try {
      const settlement = await insertQuotaSettlement(dbConfig, {
        requestId,
        ipRangeHash,
        filepathHash,
        actualBytes: normalizedActual,
        refundAmount,
        windowStartTime,
      });

      if (!settlement?.success) {
        console.warn(`${LOG_PREFIX} Settlement insert failed (${contextLabel})`);
        return;
      }

      console.log(`${LOG_PREFIX} Refunded ${refundAmount} bytes (${contextLabel})`);

      if (!ctx || typeof ctx.waitUntil !== 'function') {
        console.warn(`${LOG_PREFIX} ctx.waitUntil unavailable; skipping async balance reconciliation`);
        return;
      }

      const nowSeconds = Math.floor(Date.now() / 1000);
      ctx.waitUntil((async () => {
        try {
          const balance = await checkQuotaBalance(dbConfig, {
            ipRangeHash,
            filepathHash,
            deductBytes: 0,
            nowSeconds,
            fileWindowSeconds: balanceParams.fileWindowSeconds,
            fileMaxBytes: balanceParams.fileMaxBytes,
            fileBlockSeconds: balanceParams.fileBlockSeconds,
            globalWindowSeconds: balanceParams.globalWindowSeconds,
            globalMaxBytes: balanceParams.globalMaxBytes,
            globalBlockSeconds: balanceParams.globalBlockSeconds,
          });

          if (!balance.insufficient) {
            if (quotaConfig.fileQuota?.enabled) {
              await deleteQuotaBlock(dbConfig, {
                ipRangeHash,
                filepathHash,
                blockType: 'file_quota',
              });
            }
            if (quotaConfig.globalQuota?.enabled) {
              await deleteQuotaBlock(dbConfig, {
                ipRangeHash,
                filepathHash: null,
                blockType: 'global_quota',
              });
            }
          }
        } catch (error) {
          console.error(`${LOG_PREFIX} Async balance check failed: ${error instanceof Error ? error.message : error}`);
        }
      })());
    } catch (error) {
      console.error(`${LOG_PREFIX} Settlement failed (${contextLabel}): ${error instanceof Error ? error.message : error}`);
    }
  };

  const wrappedStream = new TransformStream({
    transform(chunk, controller) {
      actualBytes += getChunkByteLength(chunk);
      controller.enqueue(chunk);
    },
    async flush() {
      await finalizeQuota('complete');
    },
    async cancel(reason) {
      console.log(`${LOG_PREFIX} Transfer cancelled, reason=${formatReason(reason)}`);
      await finalizeQuota('cancelled');
    },
  });

  return sourceStream.pipeThrough(wrappedStream);
}
