// Cloudflare Snippet: pre-auth + cache for download
// Set HMAC_SECRET to common.tokenHmacKey (and keep common.signSecret aligned).
const HMAC_SECRET = "replace-with-common-tokenHmacKey";
const CACHE_TTL = 7 * 24 * 60 * 60;
const MAX_CACHE_SIZE = 512 * 1024 * 1024;
const CACHE_HOST = "cache.local";

// Download should verify all three signatures.
const REQUIRE_HASH_SIGN = true;
const REQUIRE_WORKER_SIGN = true;

// Optional: restrict checks to a hostname set (empty = all).
const DOWNLOAD_HOSTS = new Set([
  // "dl.example.com",
]);

const encoder = new TextEncoder();
let hmacKeyPromise = null;

const getHmacKey = () => {
  if (!hmacKeyPromise) {
    hmacKeyPromise = crypto.subtle.importKey(
      "raw",
      encoder.encode(HMAC_SECRET),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign"]
    );
  }
  return hmacKeyPromise;
};

const base64UrlEncode = (bytes) =>
  btoa(String.fromCharCode(...bytes)).replace(/\+/g, "-").replace(/\//g, "_");

const base64EncodeUtf8 = (text) => {
  const bytes = encoder.encode(text);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
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

const hmacSha256Sign = async (data, expire) => {
  const key = await getHmacKey();
  const payload = `${data}:${expire}`;
  const buf = await crypto.subtle.sign("HMAC", key, encoder.encode(payload));
  return `${base64UrlEncode(new Uint8Array(buf))}:${expire}`;
};

const deny = (msg) =>
  new Response(msg, { status: 403, headers: { "Cache-Control": "no-store" } });

export default {
  async fetch(request, env, ctx) {
    if (!HMAC_SECRET) return new Response("misconfigured", { status: 500 });

    const url = new URL(request.url);
    const nowSeconds = Math.floor(Date.now() / 1000);

    const path = normalizePath(url.pathname);
    if (!path) return new Response("invalid path", { status: 400 });

    const sign = url.searchParams.get("sign") || "";
    const signMeta = parseSignature(sign);
    if (!signMeta) return deny("sign invalid");
    if (isExpired(signMeta.expire, nowSeconds)) return deny("sign expired");

    const isDownloadHost =
      DOWNLOAD_HOSTS.size === 0 || DOWNLOAD_HOSTS.has(url.hostname);
    const requireHashSign = isDownloadHost && REQUIRE_HASH_SIGN;
    const requireWorkerSign = isDownloadHost && REQUIRE_WORKER_SIGN;

    const hashSign = url.searchParams.get("hashSign") || "";
    const workerSign = url.searchParams.get("workerSign") || "";

    let hashMeta = null;
    let workerMeta = null;

    if (requireHashSign) {
      hashMeta = parseSignature(hashSign);
      if (!hashMeta) return deny("hashSign invalid");
      if (isExpired(hashMeta.expire, nowSeconds)) return deny("hashSign expired");
    }

    if (requireWorkerSign) {
      workerMeta = parseSignature(workerSign);
      if (!workerMeta) return deny("workerSign invalid");
      if (isExpired(workerMeta.expire, nowSeconds)) return deny("workerSign expired");
    }

    const workerAddr = new URL(request.url).origin;
    const base64Path = base64EncodeUtf8(path);
    const workerVerifyData = JSON.stringify({ path, worker_addr: workerAddr });

    const tasks = [
      hmacSha256Sign(path, signMeta.expire).then((expected) => ({ label: "sign", expected })),
    ];
    if (requireHashSign) {
      tasks.push(
        hmacSha256Sign(base64Path, hashMeta.expire).then((expected) => ({ label: "hashSign", expected }))
      );
    }
    if (requireWorkerSign) {
      tasks.push(
        hmacSha256Sign(workerVerifyData, workerMeta.expire).then((expected) => ({ label: "workerSign", expected }))
      );
    }

    const results = await Promise.all(tasks);
    for (const item of results) {
      if (item.label === "sign" && item.expected !== sign) return deny("sign mismatch");
      if (item.label === "hashSign" && item.expected !== hashSign) return deny("hashSign mismatch");
      if (item.label === "workerSign" && item.expected !== workerSign) return deny("workerSign mismatch");
    }

    const isGet = request.method === "GET";
    const hasRange = request.headers.has("range");
    if (!isGet || hasRange) {
      return fetch(request);
    }

    const cache = caches.default;
    const cacheUrl = new URL(request.url);
    cacheUrl.protocol = "https:";
    cacheUrl.username = "";
    cacheUrl.password = "";
    cacheUrl.hostname = CACHE_HOST;
    cacheUrl.port = "";
    cacheUrl.search = "";
    cacheUrl.hash = "";

    const cacheKey = new Request(cacheUrl.toString(), { method: "GET" });
    const cached = await cache.match(cacheKey);
    if (cached) return cached;

    const origin = await fetch(request);
    if (origin.status === 200) {
      const contentLength = origin.headers.get("Content-Length");
      const transferEncoding = origin.headers.get("Transfer-Encoding");
      const size = contentLength ? Number.parseInt(contentLength, 10) : NaN;
      const isChunked =
        !contentLength &&
        typeof transferEncoding === "string" &&
        transferEncoding.toLowerCase().includes("chunked");

      if (!Number.isFinite(size) || size <= MAX_CACHE_SIZE) {
        if (!isChunked) {
          const toCache = origin.clone();
          toCache.headers.set(
            "Cache-Control",
            `public, max-age=${CACHE_TTL}, s-maxage=${CACHE_TTL}`
          );
          ctx.waitUntil(cache.put(cacheKey, toCache).catch(() => {}));
        }
      }
    }

    return origin;
  },
};
