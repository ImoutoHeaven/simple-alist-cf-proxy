// Cloudflare Snippet: pre-auth + cache lookup for download
// Set HMAC_SECRET in CONFIG to common.tokenHmacKey (and keep common.signSecret aligned).
// Leave HMAC_SECRET empty to skip all signature checks.

const DEFAULTS = {
  additionalInfoCheck: true,
  additionExpireTimeCheck: true,
  hashCheck: true,
  signCheck: true,
  workerCheck: true,
};

const CONFIG = [
  // Example:
  // { pattern: "alist-download-*.example.com/*", config: { HMAC_SECRET: "replace-with-common-tokenHmacKey", additionalInfoCheck: true, additionExpireTimeCheck: true, hashCheck: true, signCheck: true, workerCheck: true } },
  // { pattern: "alist-download-*.example.com/**", config: { HMAC_SECRET: "replace-with-common-tokenHmacKey", additionalInfoCheck: true, additionExpireTimeCheck: true, hashCheck: true, signCheck: true, workerCheck: true } },
  // { pattern: "alist-download-*.example.com", config: { HMAC_SECRET: "replace-with-common-tokenHmacKey", additionalInfoCheck: true, additionExpireTimeCheck: true, hashCheck: true, signCheck: true, workerCheck: true } },
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

const base64EncodeUtf8 = (text) => {
  const bytes = encoder.encode(text);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
};

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

const readAdditionalExpireTime = (payload) => {
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
    const additionalInfoCheck = hasSecret && config.additionalInfoCheck !== false;
    const additionExpireTimeCheck = hasSecret && config.additionExpireTimeCheck !== false;
    const hashCheck = hasSecret && config.hashCheck !== false;
    const signCheck = hasSecret && config.signCheck !== false;
    const workerCheck = hasSecret && config.workerCheck !== false;

    const nowSeconds = Math.floor(Date.now() / 1000);

    let sign = "";
    let signMeta = null;
    let hashSign = "";
    let workerSign = "";
    let additionalInfo = "";
    let additionalInfoSign = "";

    let hashMeta = null;
    let workerMeta = null;
    let additionalMeta = null;

    if (signCheck) {
      sign = url.searchParams.get("sign") || "";
      signMeta = parseSignature(sign);
      if (!signMeta) return unauthorized("sign invalid");
      if (isExpired(signMeta.expire, nowSeconds)) return unauthorized("sign expired");
    }

    if (hashCheck) {
      hashSign = url.searchParams.get("hashSign") || "";
      hashMeta = parseSignature(hashSign);
      if (!hashMeta) return unauthorized("hashSign invalid");
      if (isExpired(hashMeta.expire, nowSeconds)) return unauthorized("hashSign expired");
    }

    if (workerCheck) {
      workerSign = url.searchParams.get("workerSign") || "";
      workerMeta = parseSignature(workerSign);
      if (!workerMeta) return unauthorized("workerSign invalid");
      if (isExpired(workerMeta.expire, nowSeconds)) return unauthorized("workerSign expired");
    }

    if (additionalInfoCheck) {
      additionalInfo = url.searchParams.get("additionalInfo") || "";
      additionalInfoSign = url.searchParams.get("additionalInfoSign") || "";
      if (!additionalInfo) return unauthorized("additionalInfo missing");
      if (!additionalInfoSign) return unauthorized("additionalInfoSign missing");
      additionalMeta = parseSignature(additionalInfoSign);
      if (!additionalMeta) return unauthorized("additionalInfoSign invalid");
      if (isExpired(additionalMeta.expire, nowSeconds)) return unauthorized("additionalInfoSign expired");
    }

    const tasks = [];
    if (signCheck && signMeta) {
      tasks.push(
        hmacSha256Sign(secret, path, signMeta.expire).then((expected) => ({ label: "sign", expected }))
      );
    }
    if (hashCheck && hashMeta) {
      const base64Path = base64EncodeUtf8(path);
      tasks.push(
        hmacSha256Sign(secret, base64Path, hashMeta.expire).then((expected) => ({ label: "hashSign", expected }))
      );
    }
    if (workerCheck && workerMeta) {
      const workerAddr = url.origin;
      const workerVerifyData = JSON.stringify({ path, worker_addr: workerAddr });
      tasks.push(
        hmacSha256Sign(secret, workerVerifyData, workerMeta.expire).then((expected) => ({ label: "workerSign", expected }))
      );
    }
    if (additionalInfoCheck && additionalMeta) {
      tasks.push(
        hmacSha256Sign(secret, additionalInfo, additionalMeta.expire).then((expected) => ({
          label: "additionalInfoSign",
          expected,
        }))
      );
    }

    const results = await Promise.all(tasks);
    for (const item of results) {
      if (item.label === "sign" && item.expected !== sign) return unauthorized("sign mismatch");
      if (item.label === "hashSign" && item.expected !== hashSign) return unauthorized("hashSign mismatch");
      if (item.label === "workerSign" && item.expected !== workerSign) return unauthorized("workerSign mismatch");
      if (item.label === "additionalInfoSign" && item.expected !== additionalInfoSign) {
        return unauthorized("additionalInfoSign mismatch");
      }
    }

    if (additionalInfoCheck && additionExpireTimeCheck && additionalMeta) {
      const decodedAdditional = base64UrlDecodeToString(additionalInfo);
      if (!decodedAdditional) return new Response("additionalInfo decode failed", { status: 400 });
      let additionalPayload;
      try {
        additionalPayload = JSON.parse(decodedAdditional);
      } catch {
        return new Response("additionalInfo invalid", { status: 400 });
      }
      const expireTimestamp = readAdditionalExpireTime(additionalPayload);
      if (!Number.isFinite(expireTimestamp) || expireTimestamp <= 0) {
        return new Response("additionalInfo expire invalid", { status: 400 });
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
