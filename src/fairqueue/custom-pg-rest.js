const buildHeaders = (verifyHeader, verifySecret) => {
  const headers = { 'Content-Type': 'application/json' };
  if (Array.isArray(verifyHeader) && Array.isArray(verifySecret)) {
    for (let i = 0; i < Math.min(verifyHeader.length, verifySecret.length); i++) {
      headers[verifyHeader[i]] = verifySecret[i];
    }
  }
  return headers;
};

const parseSlotId = (payload) => {
  if (payload === null || payload === undefined) {
    return NaN;
  }

  if (typeof payload === 'number') {
    return Number.isFinite(payload) ? payload : NaN;
  }

  if (Array.isArray(payload)) {
    for (const item of payload) {
      const parsed = parseSlotId(item);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
    return NaN;
  }

  if (payload && typeof payload === 'object') {
    for (const value of Object.values(payload)) {
      const parsed = parseSlotId(value);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
    return NaN;
  }

  if (typeof payload === 'string') {
    const trimmed = payload.trim();
    if (trimmed === '') {
      return NaN;
    }
    const coerced = Number(trimmed);
    return Number.isFinite(coerced) ? coerced : NaN;
  }

  const coerced = Number(payload);
  return Number.isFinite(coerced) ? coerced : NaN;
};

const tryAcquireFairSlot = async (hostname, ipHash, config) => {
  if (!config?.postgrestUrl) {
    throw new Error('[Fair Queue PG] Missing PostgREST URL');
  }

  const headers = buildHeaders(config.verifyHeader, config.verifySecret);
  const body = {
    p_hostname_pattern: hostname,
    p_ip_hash: ipHash,
    p_global_limit: config.globalLimit,
    p_per_ip_limit: config.perIpLimit,
    p_zombie_timeout_seconds: config.zombieTimeoutSeconds,
  };

  const response = await fetch(`${config.postgrestUrl}/rpc/func_try_acquire_slot`, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    throw new Error(`PostgREST RPC failed: ${response.status}`);
  }

  const payload = await response.json();
  const slotId = parseSlotId(payload);
  if (!Number.isFinite(slotId)) {
    throw new Error('[Fair Queue PG] Invalid slot id returned from PostgREST');
  }

  return slotId;
};

const releaseFairSlot = async (slotId, config) => {
  if (!config?.postgrestUrl) {
    throw new Error('[Fair Queue PG] Missing PostgREST URL');
  }

  const headers = buildHeaders(config.verifyHeader, config.verifySecret);

  const response = await fetch(`${config.postgrestUrl}/rpc/func_release_fair_slot`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ p_slot_id: slotId }),
  });

  if (!response.ok) {
    throw new Error(`PostgREST release RPC failed: ${response.status}`);
  }

  console.log(`[Fair Queue PG] Released slot ${slotId}`);
};

const cleanupZombieSlots = async (config) => {
  if (!config?.postgrestUrl) {
    throw new Error('[Fair Queue PG] Missing PostgREST URL');
  }

  const headers = buildHeaders(config.verifyHeader, config.verifySecret);

  const response = await fetch(`${config.postgrestUrl}/rpc/func_cleanup_zombie_slots`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ p_zombie_timeout_seconds: config.zombieTimeoutSeconds }),
  });

  if (!response.ok) {
    throw new Error(`PostgREST cleanup RPC failed: ${response.status}`);
  }

  const deleted = await response.json();
  if (deleted > 0) {
    console.log(`[Fair Queue PG] Cleaned up ${deleted} zombie slots`);
  }
  return deleted;
};

export { tryAcquireFairSlot, releaseFairSlot, cleanupZombieSlots };
