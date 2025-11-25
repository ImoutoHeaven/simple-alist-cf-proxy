import { fetchDecisionFromController } from '../controller-client.js';

const jsonResponse = (data) =>
  new Response(JSON.stringify(data), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });

const buildCacheKey = (body) => {
  const req = body?.request || {};
  return [
    body?.env || '',
    body?.role || '',
    req.host || '',
    req.path || '',
    req.method || '',
    body?.bootstrapVersion || '',
  ].join('|');
};

const getTtlMs = (data) => {
  const ttlSeconds = Number.isFinite(data?.ttlSeconds) ? data.ttlSeconds : 60;
  return Math.max(1, ttlSeconds) * 1000;
};

export class DecisionDO {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.cache = new Map();
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === '/decision') {
      return this.handleDecision(request);
    }
    if (url.pathname === '/decision/refresh') {
      return this.handleRefresh(request);
    }
    return new Response('not found', { status: 404 });
  }

  async handleDecision(request) {
    const body = await request.json().catch(() => null);
    if (!body?.env || !body?.role || !body?.request) {
      return new Response('bad request', { status: 400 });
    }

    const cacheKey = buildCacheKey(body);
    const now = Date.now();
    const cached = this.cache.get(cacheKey);
    if (cached && cached.expAt > now) {
      return jsonResponse(cached.data);
    }

    const data = await fetchDecisionFromController(this.env, body);
    const ttlMs = getTtlMs(data);
    this.cache.set(cacheKey, { data, expAt: Date.now() + ttlMs });
    return jsonResponse(data);
  }

  async handleRefresh(request) {
    const body = await request.json().catch(() => ({}));
    const targets = Array.isArray(body?.targets) && body.targets.length > 0 ? body.targets : ['all'];
    if (targets.includes('all') || targets.includes('decision')) {
      this.cache.clear();
    }

    return new Response(null, { status: 204 });
  }
}
