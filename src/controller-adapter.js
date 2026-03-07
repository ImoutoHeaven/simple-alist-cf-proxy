import { getBootstrapConfig, getDecisionForRequest } from './controller-client.js';
import { getClientIp } from './origin-binding.js';

const hasControllerBase = (env) =>
  !!(env?.CONTROLLER_URL && env?.CONTROLLER_API_TOKEN && env?.ENV && env?.ROLE && env?.INSTANCE_ID);

const canUseBootstrap = (env) => {
  const mode = env?.BOOTSTRAP_CACHE_MODE || 'd1';
  if (mode === 'direct') return true;
  if (mode === 'd1') return !!env?.CACHE_D1;
  return false;
};

const normalizePath = (pathname) => {
  if (typeof pathname !== 'string') {
    return '/';
  }
  try {
    const decoded = decodeURIComponent(pathname);
    if (!decoded) {
      return '/';
    }
    return decoded.startsWith('/') ? decoded : `/${decoded}`;
  } catch {
    return pathname.startsWith('/') ? pathname : `/${pathname}`;
  }
};

const globToRegex = (pattern) => {
  const escaped = pattern
    .replace(/[-/\\^$+?.()|[\]{}]/g, '\\$&')
    .replace(/\*\*/g, '::GLOBSTAR::')
    .replace(/\*/g, '[^/]*')
    .replace(/::GLOBSTAR::/g, '.*');
  return new RegExp(`^${escaped}$`);
};

const matchPattern = (pattern, filepath) => {
  if (!pattern || typeof pattern !== 'string') return false;
  const regex = globToRegex(pattern);
  return regex.test(filepath);
};

const ruleMatches = (rule, filepath) => {
  if (!rule) return false;

  if (typeof rule.pattern === 'string' && rule.pattern.length > 0) {
    return matchPattern(rule.pattern, filepath);
  }

  return false;
};

const matchPathRule = (pathRules, filepath) => {
  if (!Array.isArray(pathRules) || pathRules.length === 0) {
    return null;
  }
  let best = null;
  for (const rule of pathRules) {
    if (!rule) {
      continue;
    }
    if (!ruleMatches(rule, filepath)) {
      continue;
    }
    if (!best) {
      best = rule;
      continue;
    }
    const currentPriority = Number.isFinite(rule.priority) ? rule.priority : 0;
    const bestPriority = Number.isFinite(best.priority) ? best.priority : 0;
    if (currentPriority > bestPriority) {
      best = rule;
    }
  }
  return best;
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

const findProfileById = (profiles, profileId) => {
  if (!Array.isArray(profiles) || profiles.length === 0) {
    return null;
  }
  const target = typeof profileId === 'string' ? profileId.trim() : '';
  if (!target) {
    return null;
  }
  for (const profile of profiles) {
    if (profile && typeof profile.id === 'string' && profile.id === target) {
      return profile;
    }
  }
  return null;
};

const normalizeStringArray = (value) => {
  if (!value) return [];
  if (Array.isArray(value)) {
    return value
      .map((entry) => (typeof entry === 'string' ? entry.trim() : ''))
      .filter((entry) => entry.length > 0);
  }
  return [];
};

const pickString = (value, fallback = '') => {
  if (typeof value === 'string' && value.trim()) {
    return value.trim();
  }
  return fallback;
};

const mergeDownloadDecision = (base, dynamic) => {
  if (!dynamic || typeof dynamic !== 'object') {
    return base;
  }
  const merged = { ...base };
  if (Array.isArray(dynamic.pathAction) && dynamic.pathAction.length > 0) {
    merged.pathAction = dynamic.pathAction;
  }
  if (dynamic.checkOriginMode) {
    merged.checkOriginMode = pickString(dynamic.checkOriginMode, merged.checkOriginMode);
  }
  if (Object.prototype.hasOwnProperty.call(dynamic, 'throttleProfile')) {
    merged.throttleProfile = dynamic.throttleProfile;
  }
  if (dynamic.blockReason) {
    merged.blockReason = dynamic.blockReason;
  }
  return merged;
};

const buildStaticDownloadDecision = (profile, bootstrap) => {
  const actions = profile?.actions || {};
  const downloadBootstrap = bootstrap?.download || {};
  const pathAction = normalizeStringArray(actions.pathAction);
  const checkOriginMode = pickString(actions.checkOriginMode, downloadBootstrap.originBindingDefault || '');
  const blockReason = pickString(actions.blockReason, '');

  const decision = {
    pathAction,
    checkOriginMode,
    blockReason: blockReason || undefined,
  };

  if (Object.prototype.hasOwnProperty.call(actions, 'throttleProfile')) {
    decision.throttleProfile = actions.throttleProfile;
  }

  return decision;
};

export async function fetchControllerState(request, env) {
  if (!hasControllerBase(env) || !canUseBootstrap(env)) {
    return null;
  }

  try {
    const bootstrap = await getBootstrapConfig(env);
    const ctx = buildDecisionContext(request);
    const filepath = normalizePath(ctx.path || '/');
    const rule = matchPathRule(bootstrap?.pathRules || [], filepath);
    const matchedProfileId = pickString(rule?.profileId, '');
    if (rule && !matchedProfileId) {
      throw new Error('controller pathRules.profileId is required');
    }
    const defaultProfileId = pickString(bootstrap?.global?.defaultProfileId, '');
    const profileId = matchedProfileId || defaultProfileId;
    if (!profileId) {
      throw new Error('controller global.defaultProfileId is required');
    }
    const profile = findProfileById(bootstrap?.pathProfiles || [], profileId);
    if (!profile) {
      throw new Error(`Unknown path profile from controller bootstrap: ${profileId}`);
    }

    let decisionPayload = null;
    if (profile.dynamic) {
      decisionPayload = await getDecisionForRequest(env, {
        role: env.ROLE,
        env: env.ENV,
        instance_id: env.INSTANCE_ID,
        profileId: profile.id,
        filepath,
        request: ctx,
        bootstrapVersion: bootstrap?.configVersion,
      });
    }

    const staticDecision = buildStaticDownloadDecision(profile, bootstrap);
    const effectiveDecision = mergeDownloadDecision(staticDecision, decisionPayload?.download);

    return {
      bootstrap,
      decision: { download: effectiveDecision },
      ctx,
      profileId: profile.id,
      pathRule: rule,
    };
  } catch (error) {
    console.error('[controller] fetch failed:', error instanceof Error ? error.message : String(error));
    return null;
  }
}
