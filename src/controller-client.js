const DEFAULT_BOOTSTRAP_CACHE_MODE = 'do+kv';
const CONTROL_PREFIX_DEFAULT = '/api/v0';
const BOOTSTRAP_TTL_FALLBACK = 300;

const getApiBase = (env) => {
  if (!env?.CONTROLLER_URL) {
    throw new Error('CONTROLLER_URL is required for controller client');
  }
  const prefix = env.CONTROLLER_API_PREFIX || CONTROL_PREFIX_DEFAULT;
  return `${env.CONTROLLER_URL.replace(/\/+$/, '')}${prefix}`;
};

const postJson = async (url, token, body) => {
  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify(body),
  });

  if (!resp.ok) {
    throw new Error(`${url} failed: ${resp.status}`);
  }
  return resp;
};

export async function fetchBootstrapFromController(env, envName, role, instanceId) {
  const apiBase = getApiBase(env);
  const resp = await postJson(`${apiBase}/bootstrap`, env.CONTROLLER_API_TOKEN, {
    role,
    env: envName,
    instance_id: instanceId,
  });
  return resp.json();
}

export async function fetchDecisionFromController(env, body) {
  const apiBase = getApiBase(env);
  const resp = await postJson(`${apiBase}/decision`, env.CONTROLLER_API_TOKEN, body);
  return resp.json();
}

export async function postMetricsToController(env, batch) {
  const apiBase = getApiBase(env);
  await postJson(`${apiBase}/metrics`, env.CONTROLLER_API_TOKEN, batch);
}

const toBool = (value) => {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'string') return value.toLowerCase() === 'true';
  return Boolean(value);
};

const parseNumber = (value) => {
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
};

const safeJsonParse = (value) => {
  if (typeof value !== 'string') {
    return null;
  }
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
};

const getCacheDb = (env) => env?.CACHE_D1;

const ensureD1Tables = async (env) => {
  if (!toBool(env?.INIT_TABLES)) {
    return;
  }
  if (globalThis.cacheD1TablesInitialized) {
    return;
  }
  const db = getCacheDb(env);
  if (!db) {
    throw new Error('CACHE_D1 binding is required for D1 cache mode');
  }
  await db.batch([
    db.prepare(`CREATE TABLE IF NOT EXISTS bootstrap_cache (
      env TEXT NOT NULL,
      role TEXT NOT NULL,
      config_version TEXT NOT NULL,
      ttl_seconds INTEGER NOT NULL,
      expires_at INTEGER NOT NULL,
      payload_json TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      PRIMARY KEY (env, role)
    );`),
  ]);
  globalThis.cacheD1TablesInitialized = true;
};

const rememberBootstrapInMemory = (data, expAtOverride) => {
  const ttlMs = (Number.isFinite(data?.ttlSeconds) ? data.ttlSeconds : BOOTSTRAP_TTL_FALLBACK) * 1000;
  const expAt = Number.isFinite(expAtOverride) ? expAtOverride : Date.now() + ttlMs;
  globalThis.bootstrapCache = {
    expAt,
    data,
  };
};

const getBootstrapCache = () => {
  if (!globalThis.bootstrapCache) {
    return null;
  }
  if (globalThis.bootstrapCache.expAt > Date.now()) {
    return globalThis.bootstrapCache.data;
  }
  return null;
};

const readBootstrapFromD1 = async (env, envName, role) => {
  const db = getCacheDb(env);
  if (!db) {
    throw new Error('CACHE_D1 binding is required for BOOTSTRAP_CACHE_MODE=d1');
  }
  await ensureD1Tables(env);

  const row = await db
    .prepare(
      `SELECT payload_json, expires_at FROM bootstrap_cache WHERE env = ? AND role = ? LIMIT 1;`
    )
    .bind(envName, role)
    .first();

  if (!row) {
    return null;
  }

  const expAt = parseNumber(row.expires_at);
  if (!expAt || expAt <= Date.now()) {
    return null;
  }

  const payload = safeJsonParse(row.payload_json) || row.payload_json;
  if (!payload) {
    return null;
  }

  rememberBootstrapInMemory(payload, expAt);
  return payload;
};

const writeBootstrapToD1 = async (env, envName, role, data, expAtOverride) => {
  const db = getCacheDb(env);
  if (!db) {
    throw new Error('CACHE_D1 binding is required for BOOTSTRAP_CACHE_MODE=d1');
  }
  await ensureD1Tables(env);

  const ttlSeconds = Number.isFinite(data?.ttlSeconds) ? data.ttlSeconds : BOOTSTRAP_TTL_FALLBACK;
  const expAt = Number.isFinite(expAtOverride)
    ? expAtOverride
    : Date.now() + Math.max(60, ttlSeconds) * 1000;

  await db
    .prepare(
      `INSERT OR REPLACE INTO bootstrap_cache (
        env, role, config_version, ttl_seconds, expires_at, payload_json, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?);`
    )
    .bind(envName, role, data?.configVersion || '', ttlSeconds, expAt, JSON.stringify(data), Date.now())
    .run();

  rememberBootstrapInMemory(data, expAt);
};

export async function getBootstrapConfig(env) {
  const cached = getBootstrapCache();
  if (cached) {
    return cached;
  }

  const mode = env.BOOTSTRAP_CACHE_MODE || DEFAULT_BOOTSTRAP_CACHE_MODE;
  const envName = env.ENV;
  const role = env.ROLE;
  const instanceId = env.INSTANCE_ID;

  if (mode === 'direct') {
    const data = await fetchBootstrapFromController(env, envName, role, instanceId);
    rememberBootstrapInMemory(data);
    return data;
  }

  if (mode === 'd1') {
    const cachedD1 = await readBootstrapFromD1(env, envName, role);
    if (cachedD1) {
      return cachedD1;
    }
    const data = await fetchBootstrapFromController(env, envName, role, instanceId);
    await writeBootstrapToD1(env, envName, role, data);
    return data;
  }

  if (!env.BOOTSTRAP_DO) {
    throw new Error('BOOTSTRAP_DO binding is required for DO bootstrap mode');
  }

  const stub = env.BOOTSTRAP_DO.get(env.BOOTSTRAP_DO.idFromName('global'));
  const resp = await stub.fetch('https://do.internal/bootstrap', {
    method: 'POST',
    body: JSON.stringify({ env: envName, role, instance_id: instanceId }),
  });
  if (!resp.ok) {
    throw new Error(`BootstrapDO failed: ${resp.status}`);
  }
  const data = await resp.json();
  rememberBootstrapInMemory(data);
  return data;
}

export async function getDecisionForRequest(env, payload) {
  return fetchDecisionFromController(env, payload);
}
