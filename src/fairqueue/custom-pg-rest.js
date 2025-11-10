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

const parseBooleanPayload = (payload) => {
  if (typeof payload === 'boolean') {
    return payload;
  }
  if (payload === null || payload === undefined) {
    return null;
  }
  if (Array.isArray(payload)) {
    for (const item of payload) {
      const parsed = parseBooleanPayload(item);
      if (parsed !== null) {
        return parsed;
      }
    }
    return null;
  }
  if (typeof payload === 'string') {
    const normalized = payload.trim().toLowerCase();
    if (normalized === 'true' || normalized === 't' || normalized === '1') {
      return true;
    }
    if (normalized === 'false' || normalized === 'f' || normalized === '0') {
      return false;
    }
    return null;
  }
  if (typeof payload === 'number') {
    if (payload === 1) {
      return true;
    }
    if (payload === 0) {
      return false;
    }
    return null;
  }
  if (payload && typeof payload === 'object') {
    for (const value of Object.values(payload)) {
      const parsed = parseBooleanPayload(value);
      if (parsed !== null) {
        return parsed;
      }
    }
  }
  return null;
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
    p_cooldown_seconds: config.ipCooldownEnabled ? config.ipCooldownSeconds : 0,
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
    body: JSON.stringify({
      p_slot_id: slotId,
      p_enable_cooldown: Boolean(config.ipCooldownEnabled),
    }),
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


const cleanupIpCooldown = async (config) => {
  if (!config?.postgrestUrl) {
    throw new Error('[Fair Queue PG] Missing PostgREST URL');
  }

  if (!config?.ipCooldownEnabled) {
    return 0;
  }

  const ttlSeconds = Number.isFinite(config.ipCooldownCleanupTtlSeconds)
    ? config.ipCooldownCleanupTtlSeconds
    : Math.max((config.ipCooldownSeconds || 0) * 10, 60);

  if (ttlSeconds <= 0) {
    return 0;
  }

  const headers = buildHeaders(config.verifyHeader, config.verifySecret);

  const response = await fetch(`${config.postgrestUrl}/rpc/func_cleanup_ip_cooldown`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ p_ttl_seconds: ttlSeconds }),
  });

  if (!response.ok) {
    throw new Error(`PostgREST cooldown cleanup RPC failed: ${response.status}`);
  }

  const payload = await response.json();
  const deleted = typeof payload === 'number' ? payload : Number(payload);
  if (Number.isFinite(deleted) && deleted > 0) {
    console.log(`[Fair Queue PG] Cleaned up ${deleted} cooldown records`);
    return deleted;
  }

  return 0;
};

const shouldEnforceQueueDepth = (config) =>
  Number.isFinite(config?.maxWaitersPerIp) && config.maxWaitersPerIp > 0;

const tryRegisterQueueWaiter = async (hostname, ipHash, config) => {
  if (!config?.postgrestUrl) {
    throw new Error('[Fair Queue PG] Missing PostgREST URL');
  }

  if (!shouldEnforceQueueDepth(config) || !ipHash) {
    return true;
  }

  const headers = buildHeaders(config.verifyHeader, config.verifySecret);
  const response = await fetch(`${config.postgrestUrl}/rpc/func_try_register_queue_waiter`, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      p_hostname_pattern: hostname,
      p_ip_hash: ipHash,
      p_max_waiters: config.maxWaitersPerIp,
    }),
  });

  if (!response.ok) {
    throw new Error(`PostgREST queue register RPC failed: ${response.status}`);
  }

  const payload = await response.json();
  const allowed = parseBooleanPayload(payload);
  if (allowed === null) {
    throw new Error('[Fair Queue PG] Invalid queue registration response');
  }
  return allowed;
};

const releaseQueueWaiter = async (hostname, ipHash, config) => {
  if (!config?.postgrestUrl) {
    throw new Error('[Fair Queue PG] Missing PostgREST URL');
  }

  if (!ipHash) {
    return;
  }

  const headers = buildHeaders(config.verifyHeader, config.verifySecret);
  const response = await fetch(`${config.postgrestUrl}/rpc/func_release_queue_waiter`, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      p_hostname_pattern: hostname,
      p_ip_hash: ipHash,
    }),
  });

  if (!response.ok) {
    throw new Error(`PostgREST queue release RPC failed: ${response.status}`);
  }
};

const cleanupQueueDepth = async (config) => {
  if (!config?.postgrestUrl) {
    throw new Error('[Fair Queue PG] Missing PostgREST URL');
  }

  const ttlSeconds = Number.isFinite(config?.queueDepthCleanupTtlSeconds)
    ? config.queueDepthCleanupTtlSeconds
    : Math.max((config?.ipCooldownSeconds || 0) * 10, 60);

  if (ttlSeconds <= 0) {
    return 0;
  }

  const headers = buildHeaders(config.verifyHeader, config.verifySecret);

  const response = await fetch(`${config.postgrestUrl}/rpc/func_cleanup_queue_depth`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ p_ttl_seconds: ttlSeconds }),
  });

  if (!response.ok) {
    throw new Error(`PostgREST queue depth cleanup RPC failed: ${response.status}`);
  }

  const payload = await response.json();
  const deleted = typeof payload === 'number' ? payload : Number(payload);
  if (Number.isFinite(deleted) && deleted > 0) {
    console.log(`[Fair Queue PG] Cleaned up ${deleted} queue depth records`);
    return deleted;
  }

  return 0;
};

export {
  tryAcquireFairSlot,
  releaseFairSlot,
  cleanupZombieSlots,
  cleanupIpCooldown,
  tryRegisterQueueWaiter,
  releaseQueueWaiter,
  cleanupQueueDepth,
};
