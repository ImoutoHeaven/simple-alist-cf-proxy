const CONTROL_PREFIX = '/api/v0/';

const extractBearer = (authorization) => {
  if (typeof authorization !== 'string') {
    return null;
  }
  const trimmed = authorization.trim();
  if (!trimmed.toLowerCase().startsWith('bearer ')) {
    return null;
  }
  return trimmed.slice(7);
};

const matchTarget = (pathname, target) => pathname === `${CONTROL_PREFIX}${target}`;

const safeWaitUntil = (ctx, promise) => {
  if (ctx && typeof ctx.waitUntil === 'function' && promise) {
    try {
      ctx.waitUntil(promise);
    } catch {
      // Keep silent to avoid exposing control surface issues.
    }
  }
};

/**
 * 控制面内部 API 入口，token 不匹配时静默回退。
 * @param {Request} request
 * @param {Record<string, any>} env
 * @param {ExecutionContext} ctx
 * @returns {Promise<Response|null>}
 */
export async function handleInternalApiIfAny(request, env, ctx) {
  let url;
  try {
    url = new URL(request.url);
  } catch {
    return null;
  }

  if (!url.pathname.startsWith(CONTROL_PREFIX)) {
    return null;
  }

  const expectedToken = env?.INTERNAL_API_TOKEN;
  const token = extractBearer(request.headers.get('authorization'));
  if (!expectedToken || !token || token !== expectedToken) {
    return null;
  }

  if (matchTarget(url.pathname, 'health') && request.method === 'GET') {
    return handleHealth(env);
  }

  if (matchTarget(url.pathname, 'refresh') && request.method === 'POST') {
    return handleRefresh(request, env, ctx);
  }

  if (matchTarget(url.pathname, 'flush') && request.method === 'POST') {
    return handleFlush(request, env, ctx);
  }

  return new Response(null, { status: 404 });
}

function handleHealth(env) {
  const headers = new Headers();
  if (env?.APP_NAME) headers.set('X-App-Name', String(env.APP_NAME));
  if (env?.APP_VERSION) headers.set('X-App-Version', String(env.APP_VERSION));
  if (env?.ENV) headers.set('X-Env', String(env.ENV));
  if (env?.ROLE) headers.set('X-Role', String(env.ROLE));
  if (env?.INSTANCE_ID) headers.set('X-Instance-Id', String(env.INSTANCE_ID));

  return new Response(null, { status: 204, headers });
}

async function handleRefresh(request, env, ctx) {
  const body = await request.json().catch(() => ({}));
  const targets = Array.isArray(body?.targets) && body.targets.length > 0 ? body.targets : ['all'];

  if (targets.includes('all') || targets.includes('bootstrap')) {
    globalThis.bootstrapCache = null;
  }
  if (targets.includes('all') || targets.includes('decision')) {
    globalThis.decisionCache = {};
  }

  const promises = [];

  if (targets.includes('all') || targets.includes('bootstrap')) {
    const promise = notifyDo(env, 'BOOTSTRAP_DO', '/bootstrap/refresh', { mode: body?.mode || 'lazy', targets });
    if (promise) promises.push(promise);
  }
  if (targets.includes('all') || targets.includes('decision')) {
    const promise = notifyDo(env, 'DECISION_DO', '/decision/refresh', { mode: body?.mode || 'lazy', targets });
    if (promise) promises.push(promise);
  }
  if (targets.includes('metrics')) {
    const promise = notifyDo(env, 'METRICS_DO', '/metrics/flush', { reason: body?.reason || 'refresh' });
    if (promise) promises.push(promise);
  }

  for (const p of promises) {
    safeWaitUntil(ctx, p);
  }

  return new Response(null, { status: 204 });
}

async function handleFlush(request, env, ctx) {
  const body = await request.json().catch(() => ({}));
  const targets = Array.isArray(body?.targets) && body.targets.length > 0 ? body.targets : ['metrics'];

  const promises = [];

  if (targets.includes('metrics')) {
    const promise = notifyDo(env, 'METRICS_DO', '/metrics/flush', { reason: body?.reason || 'flush' });
    if (promise) promises.push(promise);
  }

  for (const p of promises) {
    safeWaitUntil(ctx, p);
  }

  return new Response(null, { status: 204 });
}

function notifyDo(env, bindingName, path, body) {
  if (!env || !env[bindingName]) {
    return null;
  }
  const namespace = env[bindingName];
  const stub = namespace.get(namespace.idFromName('global'));
  return stub.fetch(`https://do.internal${path}`, {
    method: 'POST',
    body: JSON.stringify(body || {}),
  });
}
