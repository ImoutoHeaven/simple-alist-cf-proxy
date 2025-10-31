import { sha256Hash, applyVerifyHeaders, hasVerifyCredentials } from '../utils.js';

const DEFAULT_IPRANGE_TABLE = 'IPRANGE_BANDWIDTH_QUOTA_TABLE';
const DEFAULT_FILEPATH_TABLE = 'IPRANGE_FILEPATH_BANDWIDTH_QUOTA_TABLE';

const callRpc = async (url, headers, body) => {
  const response = await fetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const errorText = await response.text().catch(() => '');
    throw new Error(`Bandwidth quota RPC failed (${response.status}): ${errorText}`);
  }

  return response;
};

const calculateFilepathQuota = (config, filesize) => {
  if (!config || config.value <= 0) {
    return 0;
  }

  if (config.type === 'dynamic') {
    if (!filesize || filesize <= 0) {
      console.warn('[Bandwidth Quota] Dynamic quota requires filesize, skipping filepath quota update');
      return 0;
    }
    const quota = Math.floor(filesize * config.value);
    console.log(`[Bandwidth Quota] Dynamic filepath quota: ${filesize} bytes * ${config.value} = ${quota} bytes`);
    return quota;
  }

  console.log(`[Bandwidth Quota] Static filepath quota: ${config.value} bytes`);
  return config.value;
};

export const upsertBandwidthQuota = async (config, ipRange, filepath, bytes, filesize = 0) => {
  if (!config || !config.postgrestUrl || !hasVerifyCredentials(config.verifyHeader, config.verifySecret)) {
    console.warn('[Bandwidth Quota] Missing PostgREST configuration, skipping quota update');
    return { success: false };
  }

  if (!ipRange || !Number.isFinite(bytes) || bytes <= 0) {
    return { success: true };
  }

  const now = Math.floor(Date.now() / 1000);
  const blockTimeSeconds = config.blockTimeSeconds || 0;
  const totalQuotaActive = Boolean(config.totalEnabled) && config.iprangeLimit > 0 && config.windowTimeTotalSeconds > 0;
  const filepathQuotaLimit = config.filepathEnabled
    ? calculateFilepathQuota(config.filepathLimitConfig, filesize)
    : 0;
  const filepathQuotaActive =
    Boolean(config.filepathEnabled) &&
    filepath &&
    config.windowTimeFilepathSeconds > 0 &&
    filepathQuotaLimit > 0;

  try {
    const headers = { 'Content-Type': 'application/json' };
    applyVerifyHeaders(headers, config.verifyHeader, config.verifySecret);

    const requests = [];

    if (totalQuotaActive) {
      const ipHash = await sha256Hash(ipRange);
      if (!ipHash) {
        console.error('[Bandwidth Quota] Failed to compute IP hash');
      } else {
        const rpcUrl = `${config.postgrestUrl}/rpc/bandwidth_upsert_iprange_quota`;
        const rpcBody = {
          p_ip_hash: ipHash,
          p_ip_range: ipRange,
          p_bytes_to_add: Math.max(0, Math.trunc(bytes)),
          p_now: now,
          p_window_seconds: config.windowTimeTotalSeconds,
          p_quota_bytes: config.iprangeLimit,
          p_block_seconds: blockTimeSeconds,
          p_table_name: config.iprangeTableName || DEFAULT_IPRANGE_TABLE,
        };

        requests.push(callRpc(rpcUrl, headers, rpcBody));
      }
    }

    if (filepathQuotaActive) {
      const compositeHash = await sha256Hash(`${ipRange}${filepath}`);
      if (!compositeHash) {
        console.error('[Bandwidth Quota] Failed to compute composite hash');
      } else {
        const rpcUrl = `${config.postgrestUrl}/rpc/bandwidth_upsert_filepath_quota`;
        const rpcBody = {
          p_composite_hash: compositeHash,
          p_ip_range: ipRange,
          p_filepath: filepath,
          p_bytes_to_add: Math.max(0, Math.trunc(bytes)),
          p_now: now,
          p_window_seconds: config.windowTimeFilepathSeconds,
          p_quota_bytes: filepathQuotaLimit,
          p_block_seconds: blockTimeSeconds,
          p_table_name: config.filepathTableName || DEFAULT_FILEPATH_TABLE,
        };

        requests.push(callRpc(rpcUrl, headers, rpcBody));
      }
    }

    if (requests.length === 0) {
      return { success: true };
    }

    await Promise.all(requests);
    return { success: true };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error('[Bandwidth Quota] Upsert failed:', message);
    return { success: false, error: message };
  }
};
