const DEFAULT_BOOTSTRAP_CACHE_MODE = 'do+kv';
const DEFAULT_DECISION_CACHE_MODE = 'do';
const CONTROL_PREFIX_DEFAULT = '/api/v0';

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

/**
 * 直连 controller 获取 bootstrap。
 * @param {any} env
 * @param {string} envName
 * @param {string} role
 * @param {string} instanceId
 */
export async function fetchBootstrapFromController(env, envName, role, instanceId) {
  const apiBase = getApiBase(env);
  const resp = await postJson(`${apiBase}/bootstrap`, env.CONTROLLER_API_TOKEN, {
    role,
    env: envName,
    instance_id: instanceId,
  });
  return resp.json();
}

/**
 * 直连 controller 获取 decision。
 * @param {any} env
 * @param {any} body
 */
export async function fetchDecisionFromController(env, body) {
  const apiBase = getApiBase(env);
  const resp = await postJson(`${apiBase}/decision`, env.CONTROLLER_API_TOKEN, body);
  return resp.json();
}

/**
 * 直连 controller 上报 metrics。
 * @param {any} env
 * @param {any} batch
 */
export async function postMetricsToController(env, batch) {
  const apiBase = getApiBase(env);
  await postJson(`${apiBase}/metrics`, env.CONTROLLER_API_TOKEN, batch);
}

const rememberBootstrapInMemory = (data) => {
  const ttlMs = (Number.isFinite(data?.ttlSeconds) ? data.ttlSeconds : 300) * 1000;
  globalThis.bootstrapCache = {
    expAt: Date.now() + ttlMs,
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

/**
 * 获取 bootstrap，支持 direct / DO / DO+KV。
 * @param {any} env
 */
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

const buildDecisionCacheKey = (env, ctx) => {
  const host = ctx?.host || '';
  const path = ctx?.path || '';
  const method = ctx?.method || '';
  return `${env.ENV || ''}|${env.ROLE || ''}|${host}|${path}|${method}|${ctx?.bootstrapVersion || ''}`;
};

const rememberDecisionInMemory = (key, data) => {
  const ttlMs = (Number.isFinite(data?.ttlSeconds) ? data.ttlSeconds : 60) * 1000;
  if (!globalThis.decisionCache) {
    globalThis.decisionCache = {};
  }
  globalThis.decisionCache[key] = {
    expAt: Date.now() + ttlMs,
    data,
  };
};

const getDecisionFromMemory = (key) => {
  const cache = globalThis.decisionCache;
  if (!cache || !cache[key]) {
    return null;
  }
  if (cache[key].expAt > Date.now()) {
    return cache[key].data;
  }
  return null;
};

/**
 * 获取 decision，支持 direct / DO。
 * @param {any} env
 * @param {any} bootstrap
 * @param {any} ctx
 */
export async function getDecisionForRequest(env, bootstrap, ctx) {
  const mode = env.DECISION_CACHE_MODE || DEFAULT_DECISION_CACHE_MODE;
  const cacheKey = buildDecisionCacheKey(env, { ...ctx, bootstrapVersion: bootstrap?.configVersion });

  if (mode === 'direct') {
    const direct = await fetchDecisionFromController(env, {
      role: env.ROLE,
      env: env.ENV,
      instance_id: env.INSTANCE_ID,
      request: ctx,
      bootstrapVersion: bootstrap?.configVersion,
    });
    rememberDecisionInMemory(cacheKey, direct);
    return direct;
  }

  const cached = getDecisionFromMemory(cacheKey);
  if (cached) {
    return cached;
  }

  if (!env.DECISION_DO) {
    throw new Error('DECISION_DO binding is required for DO decision mode');
  }

  const stub = env.DECISION_DO.get(env.DECISION_DO.idFromName('global'));
  const resp = await stub.fetch('https://do.internal/decision', {
    method: 'POST',
    body: JSON.stringify({
      env: env.ENV,
      role: env.ROLE,
      instance_id: env.INSTANCE_ID,
      request: ctx,
      bootstrapVersion: bootstrap?.configVersion,
    }),
  });

  if (!resp.ok) {
    throw new Error(`DecisionDO failed: ${resp.status}`);
  }

  const data = await resp.json();
  rememberDecisionInMemory(cacheKey, data);
  return data;
}
