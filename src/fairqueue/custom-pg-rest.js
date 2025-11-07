const buildHeaders = (verifyHeader, verifySecret) => {
  const headers = { 'Content-Type': 'application/json' };
  if (Array.isArray(verifyHeader) && Array.isArray(verifySecret)) {
    for (let i = 0; i < Math.min(verifyHeader.length, verifySecret.length); i++) {
      headers[verifyHeader[i]] = verifySecret[i];
    }
  }
  return headers;
};

const acquireFairSlot = async (hostname, ipHash, config) => {
  if (!config?.postgrestUrl) {
    throw new Error('[Fair Queue PG] Missing PostgREST URL');
  }

  const headers = buildHeaders(config.verifyHeader, config.verifySecret);
  const body = {
    p_hostname_pattern: hostname,
    p_ip_hash: ipHash,
    p_global_limit: config.globalLimit,
    p_per_ip_limit: config.perIpLimit,
    p_queue_wait_timeout_ms: config.queueWaitTimeoutMs,
    p_zombie_timeout_seconds: config.zombieTimeoutSeconds,
    p_poll_interval_ms: config.pollIntervalMs,
  };

  const response = await fetch(`${config.postgrestUrl}/rpc/func_acquire_fair_slot`, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    throw new Error(`PostgREST RPC failed: ${response.status}`);
  }

  const slotId = await response.json();

  if (slotId === 0) {
    console.warn(`[Fair Queue PG] Per-IP limit reached: ${ipHash}`);
    const error = new Error(`Per-IP limit ${config.perIpLimit} reached`);
    error.name = 'PerIpLimitError';
    throw error;
  }

  if (slotId < 0) {
    console.warn(`[Fair Queue PG] Queue timeout for ${hostname}`);
    const error = new Error(`Queue timeout after ${config.queueWaitTimeoutMs}ms`);
    error.name = 'QueueTimeoutError';
    throw error;
  }

  console.log(`[Fair Queue PG] Acquired slot ${slotId} for ${hostname}`);
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

export { acquireFairSlot, releaseFairSlot, cleanupZombieSlots };
