// Cloudflare Snippet: pre-auth + cache lookup for download
// Set HMAC_SECRET to common.tokenHmacKey (and keep common.signSecret aligned).
const HMAC_SECRET = "replace-with-common-tokenHmacKey";
const WINDOW_TIME = 10;
const WINDOW_QUOTA = 52;
const IPV4_PREFIX = 32;
const IPV6_PREFIX = 60;
const RATE_CACHE_HOSTNAME = "cachethrottle.local";
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

const maskIPv4 = (ip, prefix) => {
  if (prefix <= 0) return "0.0.0.0/0";
  if (prefix >= 32) return `${ip}/32`;
  const parts = ip.split(".").map(Number);
  if (parts.length !== 4 || parts.some(Number.isNaN)) return ip;
  const ipInt = ((parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]) >>> 0;
  const mask = (0xffffffff << (32 - prefix)) >>> 0;
  const maskedInt = (ipInt & mask) >>> 0;
  const maskedIp = [
    (maskedInt >>> 24) & 0xff,
    (maskedInt >>> 16) & 0xff,
    (maskedInt >>> 8) & 0xff,
    maskedInt & 0xff,
  ].join(".");
  return `${maskedIp}/${prefix}`;
};

const parseIPv6 = (ip) => {
  if (typeof ip !== "string" || !ip.includes(":") || ip.includes(".")) return null;
  const parts = ip.split("::");
  if (parts.length > 2) return null;
  const left = parts[0] ? parts[0].split(":").filter(Boolean) : [];
  const right = parts.length === 2 && parts[1] ? parts[1].split(":").filter(Boolean) : [];
  if (left.length + right.length > 8) return null;
  const full = [
    ...left,
    ...Array(8 - (left.length + right.length)).fill("0"),
    ...right,
  ];
  return Uint16Array.from(full.map((h) => Number.parseInt(h, 16) || 0));
};

const maskIPv6 = (hextets, prefix) => {
  const out = new Uint16Array(hextets);
  const full = Math.floor(prefix / 16);
  const rem = prefix % 16;
  for (let i = 0; i < 8; i += 1) {
    if (i < full) continue;
    if (i === full && rem > 0) {
      out[i] &= (0xffff << (16 - rem));
    } else {
      out[i] = 0;
    }
  }
  return out;
};

const fnv1a32hex = (str) => {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i += 1) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h.toString(16).padStart(8, "0");
};

const getSubnetKey = (ip) => {
  if (!ip) return "unknown";
  let processingIp = ip.toLowerCase();
  let subnet = processingIp;
  if (processingIp.startsWith("::ffff:") && processingIp.includes(".")) {
    processingIp = processingIp.substring(7);
  }
  const isV6 = processingIp.includes(":") && !processingIp.includes(".");
  if (isV6) {
    const parsed = parseIPv6(processingIp);
    if (parsed) {
      const masked = maskIPv6(parsed, IPV6_PREFIX);
      subnet = Array.from(masked).map((n) => n.toString(16)).join(":") + `/${IPV6_PREFIX}`;
    } else {
      subnet = processingIp;
    }
  } else if (processingIp.includes(".")) {
    subnet = (IPV4_PREFIX < 32) ? maskIPv4(processingIp, IPV4_PREFIX) : `${processingIp}/32`;
  } else {
    subnet = "unknown";
  }
  return fnv1a32hex(subnet);
};

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

const safeEqual = (a, b) => {
  if (typeof a !== "string" || typeof b !== "string") return false;
  if (a.length !== b.length) return false;
  const aB = encoder.encode(a);
  const bB = encoder.encode(b);
  let diff = 0;
  for (let i = 0; i < aB.length; i += 1) {
    diff |= aB[i] ^ bB[i];
  }
  return diff === 0;
};

const checkRateLimit = async (ip) => {
  if (!ip) return { ok: false, status: 403, msg: "No IP" };
  const key = getSubnetKey(ip);
  const now = Math.floor(Date.now() / 1000);
  const curKeyId = Math.floor(now / WINDOW_TIME);
  const prevKeyId = curKeyId - 1;
  const overlap = WINDOW_TIME - (now % WINDOW_TIME);
  const prevWeight = overlap / WINDOW_TIME;
  const cache = caches.default;
  const curUrl = `https://${RATE_CACHE_HOSTNAME}/${key}/${curKeyId}`;
  const prevUrl = `https://${RATE_CACHE_HOSTNAME}/${key}/${prevKeyId}`;
  const [curRes, prevRes] = await Promise.all([
    cache.match(curUrl),
    cache.match(prevUrl),
  ]);
  let curCount = 0;
  let prevCount = 0;
  if (curRes) curCount = Number.parseInt(await curRes.text(), 10) || 0;
  if (prevRes) prevCount = Number.parseInt(await prevRes.text(), 10) || 0;
  const estimate = curCount + (prevCount * prevWeight);
  if (estimate >= WINDOW_QUOTA) {
    return { ok: false, status: 429, msg: "Rate limit exceeded" };
  }
  const newCount = curCount + 1;
  const putRes = new Response(String(newCount), {
    headers: {
      "Content-Type": "text/plain",
      "Cache-Control": `public, max-age=${WINDOW_TIME * 2}`,
    },
  });
  await cache.put(curUrl, putRes);
  return { ok: true };
};

const deny = (msg) =>
  new Response(msg, { status: 403, headers: { "Cache-Control": "no-store" } });

export default {
  async fetch(request, env, ctx) {
    if (!HMAC_SECRET) return new Response("misconfigured", { status: 500 });

    try {
      const clientIP = request.headers.get("cf-connecting-ip");
      const decision = await checkRateLimit(clientIP);
      if (!decision.ok) {
        return new Response(decision.msg, {
          status: decision.status,
          headers: { "Retry-After": String(WINDOW_TIME) },
        });
      }
    } catch (_error) {
      // fail-open
    }

    const url = new URL(request.url);
    const nowSeconds = Math.floor(Date.now() / 1000);

    const path = normalizePath(url.pathname);
    if (!path) return new Response("invalid path", { status: 400 });

    const sign = url.searchParams.get("sign") || "";
    const signMeta = parseSignature(sign);
    if (!signMeta) return deny("sign invalid");
    if (isExpired(signMeta.expire, nowSeconds)) return deny("sign expired");

    const hashSign = url.searchParams.get("hashSign") || "";
    const workerSign = url.searchParams.get("workerSign") || "";
    const additionalInfo = url.searchParams.get("additionalInfo") || "";
    const additionalInfoSign = url.searchParams.get("additionalInfoSign") || "";

    let hashMeta = null;
    let workerMeta = null;
    let additionalMeta = null;

    hashMeta = parseSignature(hashSign);
    if (!hashMeta) return deny("hashSign invalid");
    if (isExpired(hashMeta.expire, nowSeconds)) return deny("hashSign expired");

    workerMeta = parseSignature(workerSign);
    if (!workerMeta) return deny("workerSign invalid");
    if (isExpired(workerMeta.expire, nowSeconds)) return deny("workerSign expired");

    if (additionalInfo) {
      if (!additionalInfoSign) return deny("additionalInfoSign missing");
      additionalMeta = parseSignature(additionalInfoSign);
      if (!additionalMeta) return deny("additionalInfoSign invalid");
      if (isExpired(additionalMeta.expire, nowSeconds)) return deny("additionalInfoSign expired");
    }

    const workerAddr = new URL(request.url).origin;
    const base64Path = base64EncodeUtf8(path);
    const workerVerifyData = JSON.stringify({ path, worker_addr: workerAddr });

    const tasks = [
      hmacSha256Sign(path, signMeta.expire).then((expected) => ({ label: "sign", expected })),
      hmacSha256Sign(base64Path, hashMeta.expire).then((expected) => ({ label: "hashSign", expected })),
      hmacSha256Sign(workerVerifyData, workerMeta.expire).then((expected) => ({ label: "workerSign", expected })),
    ];
    if (additionalMeta) {
      tasks.push(
        hmacSha256Sign(additionalInfo, additionalMeta.expire).then((expected) => ({ label: "additionalInfoSign", expected }))
      );
    }

    const results = await Promise.all(tasks);
    for (const item of results) {
      if (item.label === "sign" && !safeEqual(item.expected, sign)) return deny("sign mismatch");
      if (item.label === "hashSign" && !safeEqual(item.expected, hashSign)) return deny("hashSign mismatch");
      if (item.label === "workerSign" && !safeEqual(item.expected, workerSign)) return deny("workerSign mismatch");
      if (item.label === "additionalInfoSign" && !safeEqual(item.expected, additionalInfoSign)) {
        return deny("additionalInfoSign mismatch");
      }
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

    const cacheKey = new Request(cacheUrl.toString(), { method: "GET" });
    const cached = await cache.match(cacheKey);
    if (cached) return cached;

    return fetch(request);
  },
};
