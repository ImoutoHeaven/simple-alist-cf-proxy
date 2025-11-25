import { getBootstrapConfig, getDecisionForRequest } from './controller-client.js';
import { getClientIp } from './origin-binding.js';

const hasControllerBase = (env) =>
  !!(env?.CONTROLLER_URL && env?.CONTROLLER_API_TOKEN && env?.ENV && env?.ROLE && env?.INSTANCE_ID);

const canUseBootstrap = (env) => {
  const mode = env?.BOOTSTRAP_CACHE_MODE || 'do+kv';
  return mode === 'direct' || !!env?.BOOTSTRAP_DO;
};

const canUseDecision = (env) => {
  const mode = env?.DECISION_CACHE_MODE || 'do';
  return mode === 'direct' || !!env?.DECISION_DO;
};

const buildDecisionContext = (request) => {
  const url = new URL(request.url);
  const cf = request.cf || {};

  const headers = {};
  request.headers.forEach((value, key) => {
    headers[key] = value;
  });

  return {
    ip: getClientIp(request) || '',
    asn: Number.parseInt(cf.asn, 10) || 0,
    country: cf.country || '',
    continent: cf.continent || '',
    userAgent: request.headers.get('user-agent') || '',
    method: request.method || 'GET',
    host: url.host || '',
    path: url.pathname || '/',
    query: url.search ? url.search.slice(1) : '',
    referer: request.headers.get('referer'),
    headers,
  };
};

export async function fetchControllerState(request, env) {
  if (!hasControllerBase(env) || !canUseBootstrap(env) || !canUseDecision(env)) {
    return null;
  }

  try {
    const bootstrap = await getBootstrapConfig(env);
    const ctx = buildDecisionContext(request);
    const decision = await getDecisionForRequest(env, bootstrap, ctx);
    return { bootstrap, decision, ctx };
  } catch (error) {
    console.error('[controller] fetch failed:', error instanceof Error ? error.message : String(error));
    return null;
  }
}
