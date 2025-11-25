import { fetchBootstrapFromController } from '../controller-client.js';

const KV_PREFIX = 'bootstrap:';

const jsonResponse = (data) =>
  new Response(JSON.stringify(data), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });

const getTtlMs = (data) => {
  const ttlSeconds = Number.isFinite(data?.ttlSeconds) ? data.ttlSeconds : 300;
  return Math.max(60, ttlSeconds) * 1000;
};

export class BootstrapDO {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.cache = new Map();
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === '/bootstrap') {
      return this.handleBootstrap(request);
    }
    if (url.pathname === '/bootstrap/refresh') {
      return this.handleRefresh(request);
    }
    return new Response('not found', { status: 404 });
  }

  buildCacheKey(envName, role) {
    return `${envName || ''}:${role || ''}`;
  }

  buildKvKey(cacheKey) {
    return `${KV_PREFIX}${cacheKey}`;
  }

  async handleBootstrap(request) {
    const body = await request.json().catch(() => null);
    const envName = body?.env;
    const role = body?.role;
    const instanceId = body?.instance_id || body?.instanceId;

    if (!envName || !role) {
      return new Response('bad request', { status: 400 });
    }

    const now = Date.now();
    const cacheKey = this.buildCacheKey(envName, role);

    const cached = this.cache.get(cacheKey);
    if (cached && cached.expAt > now) {
      return jsonResponse(cached.data);
    }

    const kvCached = await this.readFromKv(cacheKey, now);
    if (kvCached) {
      this.cache.set(cacheKey, kvCached);
      return jsonResponse(kvCached.data);
    }

    const data = await fetchBootstrapFromController(this.env, envName, role, instanceId);
    const ttlMs = getTtlMs(data);
    const record = { data, expAt: Date.now() + ttlMs };
    this.cache.set(cacheKey, record);
    await this.writeToKv(cacheKey, record, ttlMs);
    return jsonResponse(data);
  }

  async handleRefresh(request) {
    const body = await request.json().catch(() => ({}));
    const targets = Array.isArray(body?.targets) && body.targets.length > 0 ? body.targets : ['all'];
    const shouldClear = targets.includes('all') || targets.includes('bootstrap');

    if (shouldClear) {
      this.cache.clear();
      await this.clearKv();
    }

    return new Response(null, { status: 204 });
  }

  async readFromKv(cacheKey, now) {
    if (!this.env.BOOTSTRAP_KV) {
      return null;
    }
    const raw = await this.env.BOOTSTRAP_KV.get(this.buildKvKey(cacheKey));
    if (!raw) {
      return null;
    }
    try {
      const parsed = JSON.parse(raw);
      if (parsed?.expAt && parsed.expAt > now && parsed.data) {
        return parsed;
      }
    } catch {
      return null;
    }
    return null;
  }

  async writeToKv(cacheKey, record, ttlMs) {
    if (!this.env.BOOTSTRAP_KV) {
      return;
    }
    const ttlSeconds = Math.max(60, Math.floor(ttlMs / 1000));
    await this.env.BOOTSTRAP_KV.put(this.buildKvKey(cacheKey), JSON.stringify(record), {
      expirationTtl: ttlSeconds,
    });
  }

  async clearKv() {
    if (!this.env.BOOTSTRAP_KV || typeof this.env.BOOTSTRAP_KV.list !== 'function') {
      return;
    }
    const list = await this.env.BOOTSTRAP_KV.list({ prefix: KV_PREFIX });
    if (!list?.keys?.length) {
      return;
    }
    await Promise.all(
      list.keys.map((k) => this.env.BOOTSTRAP_KV.delete(k.name).catch(() => null))
    );
  }
}
