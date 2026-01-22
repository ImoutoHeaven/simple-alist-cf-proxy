// Cloudflare Snippet: pre-auth + cache lookup for download
// Set HMAC_SECRET in CONFIG to common.tokenHmacKey.
// Leave HMAC_SECRET empty to skip payloadSign verification.

const DEFAULTS = {
  payloadSignCheck: true,
  payloadExpireTimeCheck: true,
};

const CONFIG = [
  // Example:
  // { pattern: "alist-download-*.example.com/*", config: { HMAC_SECRET: "replace-with-common-tokenHmacKey", payloadSignCheck: true, payloadExpireTimeCheck: true } },
  // { pattern: "alist-download-*.example.com/**", config: { HMAC_SECRET: "replace-with-common-tokenHmacKey", payloadSignCheck: true, payloadExpireTimeCheck: true } },
  // { pattern: "alist-download-*.example.com", config: { HMAC_SECRET: "replace-with-common-tokenHmacKey", payloadSignCheck: true, payloadExpireTimeCheck: true } },
];

const splitPattern = (pattern) => {
  if (typeof pattern !== "string") return null;
  const trimmed = pattern.trim();
  if (!trimmed) return null;
  const slashIndex = trimmed.indexOf("/");
  if (slashIndex === -1) return { host: trimmed, path: null };
  const host = trimmed.slice(0, slashIndex);
  if (!host) return null;
  return { host, path: trimmed.slice(slashIndex) };
};

const escapeRegex = (value) => value.replace(/[.+?^${}()|[\]\\]/g, "\\$&");

const compileHostPattern = (pattern) => {
  if (typeof pattern !== "string") return null;
  const host = pattern.trim().toLowerCase();
  if (!host) return null;
  const escaped = escapeRegex(host).replace(/\*/g, "[^.]*");
  try {
    return new RegExp(`^${escaped}$`);
  } catch {
    return null;
  }
};

const compilePathPattern = (pattern) => {
  if (typeof pattern !== "string") return null;
  const path = pattern.trim();
  if (!path.startsWith("/")) return null;
  let out = "";
  for (let i = 0; i < path.length; i++) {
    const ch = path[i];
    if (ch === "*") {
      if (path[i + 1] === "*") {
        const isLast = i + 2 >= path.length;
        const prevIsSlash = i > 0 && path[i - 1] === "/";
        if (isLast && prevIsSlash && out.endsWith("/") && out.length > 1) {
          out = `${out.slice(0, -1)}(?:/.*)?`;
        } else {
          out += ".*";
        }
        i++;
      } else {
        out += "[^/]*";
      }
      continue;
    }
    out += /[.+?^${}()|[\]\\]/.test(ch) ? `\\${ch}` : ch;
  }
  try {
    return new RegExp(`^${out}$`);
  } catch {
    return null;
  }
};

const compileConfigEntry = (entry) => {
  const pattern = entry && entry.pattern;
  const parts = splitPattern(pattern);
  if (!parts) {
    return { pattern, hostRegex: null, pathRegex: null, config: (entry && entry.config) || {} };
  }
  const hostRegex = compileHostPattern(parts.host);
  if (!hostRegex) {
    return { pattern, hostRegex: null, pathRegex: null, config: (entry && entry.config) || {} };
  }
  const pathRegex = parts.path ? compilePathPattern(parts.path) : null;
  if (parts.path && !pathRegex) {
    return { pattern, hostRegex: null, pathRegex: null, config: (entry && entry.config) || {} };
  }
  return { pattern, hostRegex, pathRegex, config: (entry && entry.config) || {} };
};

const COMPILED_CONFIG = CONFIG.map(compileConfigEntry);

const pickConfig = (hostname, path) => {
  const host = typeof hostname === "string" ? hostname.toLowerCase() : "";
  const requestPath = typeof path === "string" ? path : "";
  if (!host) return null;
  for (const rule of COMPILED_CONFIG) {
    if (!rule || !rule.hostRegex) continue;
    if (!rule.hostRegex.test(host)) continue;
    if (rule.pathRegex && !rule.pathRegex.test(requestPath)) continue;
    return rule.config || null;
  }
  return null;
};

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const hmacKeyCache = new Map();

const getHmacKey = (secret) => {
  const key = typeof secret === "string" ? secret : "";
  if (!key) {
    return Promise.reject(new Error("HMAC secret missing"));
  }
  if (!hmacKeyCache.has(key)) {
    hmacKeyCache.set(
      key,
      crypto.subtle.importKey(
        "raw",
        encoder.encode(key),
        { name: "HMAC", hash: "SHA-256" },
        false,
        ["sign"]
      )
    );
  }
  return hmacKeyCache.get(key);
};

const base64UrlEncode = (bytes) =>
  btoa(String.fromCharCode(...bytes)).replace(/\+/g, "-").replace(/\//g, "_");

const base64UrlDecodeToString = (value) => {
  if (typeof value !== "string" || !value) return null;
  let normalized = value.replace(/-/g, "+").replace(/_/g, "/");
  const mod = normalized.length % 4;
  if (mod === 1) return null;
  if (mod > 0) normalized = normalized.padEnd(normalized.length + (4 - mod), "=");
  try {
    const binary = atob(normalized);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    return decoder.decode(bytes);
  } catch {
    return null;
  }
};

const normalizePath = (pathname) => {
  if (typeof pathname !== "string") return null;
  let decoded;
  try {
    decoded = decodeURIComponent(pathname);
  } catch {
    return null;
  }
  if (decoded.length === 0) return "/";
  return decoded.startsWith("/") ? decoded : `/${decoded}`;
};

const parseSignature = (sig) => {
  if (!sig || typeof sig !== "string") return null;
  const idx = sig.lastIndexOf(":");
  if (idx <= 0 || idx === sig.length - 1) return null;
  const expire = Number.parseInt(sig.slice(idx + 1), 10);
  if (Number.isNaN(expire)) return null;
  return { expire };
};

const isExpired = (expire, nowSeconds) => expire > 0 && expire < nowSeconds;

const hmacSha256Sign = async (secret, data, expire) => {
  const key = await getHmacKey(secret);
  const payload = `${data}:${expire}`;
  const buf = await crypto.subtle.sign("HMAC", key, encoder.encode(payload));
  return `${base64UrlEncode(new Uint8Array(buf))}:${expire}`;
};

const readPayloadExpireTime = (payload) => {
  if (!payload || typeof payload !== "object") return null;
  const raw = payload.expireTime;
  if (typeof raw === "number" && Number.isFinite(raw)) return Math.trunc(raw);
  if (typeof raw === "string") {
    const parsed = Number.parseInt(raw, 10);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
};

const deny = (msg) =>
  new Response(msg, { status: 403, headers: { "Cache-Control": "no-store" } });

const unauthorized = (msg) =>
  new Response(msg, { status: 401, headers: { "Cache-Control": "no-store" } });

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const hostname = url.hostname;

    const path = normalizePath(url.pathname);
    if (!path) return new Response("invalid path", { status: 400 });

    const selected = pickConfig(hostname, path);
    const config = selected && typeof selected === "object" ? { ...DEFAULTS, ...selected } : { ...DEFAULTS };
    const secret = typeof config.HMAC_SECRET === "string" ? config.HMAC_SECRET : "";
    const hasSecret = secret.length > 0;
    const payloadSignCheck = hasSecret && config.payloadSignCheck !== false;
    const payloadExpireTimeCheck = hasSecret && config.payloadExpireTimeCheck !== false;

    const nowSeconds = Math.floor(Date.now() / 1000);

    const payload = url.searchParams.get("payload") || "";
    const payloadSign = url.searchParams.get("payloadSign") || "";
    let payloadMeta = null;

    if (payloadSignCheck) {
      if (!payload) return unauthorized("payload missing");
      if (!payloadSign) return unauthorized("payloadSign missing");
      payloadMeta = parseSignature(payloadSign);
      if (!payloadMeta) return unauthorized("payloadSign invalid");
      if (isExpired(payloadMeta.expire, nowSeconds)) return unauthorized("payloadSign expired");
    }

    const tasks = [];
    if (payloadSignCheck && payloadMeta) {
      tasks.push(
        hmacSha256Sign(secret, payload, payloadMeta.expire).then((expected) => ({ label: "payloadSign", expected }))
      );
    }

    const results = await Promise.all(tasks);
    for (const item of results) {
      if (item.label === "payloadSign" && item.expected !== payloadSign) {
        return unauthorized("payloadSign mismatch");
      }
    }

    if (payloadExpireTimeCheck) {
      if (!payload) return unauthorized("payload missing");
      const decodedPayload = base64UrlDecodeToString(payload);
      if (!decodedPayload) return new Response("payload decode failed", { status: 400 });
      let payloadData;
      try {
        payloadData = JSON.parse(decodedPayload);
      } catch {
        return new Response("payload invalid", { status: 400 });
      }
      const expireTimestamp = readPayloadExpireTime(payloadData);
      if (!Number.isFinite(expireTimestamp) || expireTimestamp <= 0) {
        return new Response("payload expire invalid", { status: 400 });
      }
      if (nowSeconds > expireTimestamp) return unauthorized("link expired");
    }

    const isGet = request.method === "GET";
    const hasRange = request.headers.has("range");
    if (!isGet || hasRange) {
      return fetch(request);
    }

    const cache = caches.default;
    const cacheUrl = new URL(request.url);
    cacheUrl.search = "";
    cacheUrl.hash = "";

    const cacheKey = new Request(cacheUrl.toString(), request);
    const cached = await cache.match(cacheKey);
    if (cached) {
      const headers = new Headers(cached.headers);
      headers.set("X-Snippet-Cache", "HIT");
      return new Response(cached.body, {
        status: cached.status,
        statusText: cached.statusText,
        headers,
      });
    }

    return fetch(request);
  },
};
