import { logEvent } from './logging.js';
import { calculateIPSubnet, sha256Hash } from './utils.js';

const BASE64_CHARS = { '+': '-', '/': '_', '=': '' };
const VALID_ORIGIN_MODES = new Set([
  'ip',
  'iprange',
  'continent',
  'country',
  'region',
  'city',
  'asn',
  'tls',
  'path',
]);

const base64UrlEncode = (bytes) => {
  if (!(bytes instanceof Uint8Array)) {
    return '';
  }
  let binary = '';
  for (let i = 0; i < bytes.length; i += 1) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary).replace(/[+/=]/g, (c) => BASE64_CHARS[c]);
};

const base64UrlDecode = (value) => {
  if (typeof value !== 'string' || value.length === 0) {
    return null;
  }
  let normalized = value.replace(/-/g, '+').replace(/_/g, '/');
  const mod = normalized.length % 4;
  if (mod === 1) {
    return null;
  }
  if (mod > 0) {
    normalized = normalized.padEnd(normalized.length + (4 - mod), '=');
  }
  try {
    const binary = atob(normalized);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i += 1) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
  } catch (_error) {
    return null;
  }
};

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
const hmacKeyCache = new Map();

const normalizeHeaderValue = (headers, name) => {
  if (!headers) return null;
  if (typeof headers.get === 'function') {
    const value = headers.get(name);
    return value && value.trim() ? value.trim() : null;
  }
  if (typeof headers === 'object' && headers !== null) {
    const lower = name.toLowerCase();
    for (const key of Object.keys(headers)) {
      if (key.toLowerCase() === lower) {
        const value = headers[key];
        if (typeof value === 'string' && value.trim()) {
          return value.trim();
        }
      }
    }
  }
  return null;
};

export const getClientIp = (request) => {
  if (!request || typeof request !== 'object') {
    return null;
  }
  const ip = normalizeHeaderValue(request.headers, 'CF-Connecting-IP');
  return ip || null;
};

export const normalizePath = (pathname) => {
  if (typeof pathname !== 'string') {
    return null;
  }
  let decoded;
  try {
    decoded = decodeURIComponent(pathname);
  } catch {
    return null;
  }
  if (decoded.length === 0) {
    return '/';
  }
  return decoded.startsWith('/') ? decoded : `/${decoded}`;
};

const normalizeRegionValue = (mode, value) => {
  if (typeof value !== 'string' && typeof value !== 'number') {
    return null;
  }
  const str = String(value).trim();
  if (!str) {
    return null;
  }
  if (mode === 'country' || mode === 'continent') {
    return str.toUpperCase();
  }
  return str.toLowerCase();
};

const normalizeAsnValue = (value) => {
  if (value === undefined || value === null) {
    return null;
  }
  const str = String(value).trim();
  return str ? str : null;
};

const normalizeBindingConfig = (bindingConfig) => {
  const cfg = bindingConfig && typeof bindingConfig === 'object' ? bindingConfig : {};
  const version = Number.isFinite(cfg.version) && cfg.version > 0 ? Math.trunc(cfg.version) : 1;
  const ipv4Suffix = typeof cfg.ipv4Suffix === 'string' && cfg.ipv4Suffix.trim()
    ? cfg.ipv4Suffix.trim()
    : '/32';
  const ipv6Suffix = typeof cfg.ipv6Suffix === 'string' && cfg.ipv6Suffix.trim()
    ? cfg.ipv6Suffix.trim()
    : '/60';
  const bindTls = cfg.bindTls !== false;
  return {
    version,
    ipv4Suffix,
    ipv6Suffix,
    bindTls,
  };
};

const getHmacKey = (secret) => {
  const key = typeof secret === 'string' ? secret : '';
  if (!key) {
    return Promise.reject(new Error('HMAC secret missing'));
  }
  if (!hmacKeyCache.has(key)) {
    hmacKeyCache.set(
      key,
      crypto.subtle.importKey(
        'raw',
        textEncoder.encode(key),
        { name: 'HMAC', hash: 'SHA-256' },
        false,
        ['sign']
      )
    );
  }
  return hmacKeyCache.get(key);
};

const hmacSha256 = async (secret, data) => {
  const key = await getHmacKey(secret);
  const buf = await crypto.subtle.sign('HMAC', key, textEncoder.encode(data));
  return new Uint8Array(buf);
};

export const parseCheckOriginEnv = (rawValue) => {
  if (typeof rawValue !== 'string') {
    return [];
  }
  const trimmed = rawValue.trim();
  if (!trimmed) {
    return [];
  }
  const modes = [];
  trimmed.split(',').forEach((part) => {
    const normalized = part.trim().toLowerCase();
    if (!normalized) {
      return;
    }
    if (VALID_ORIGIN_MODES.has(normalized)) {
      modes.push(normalized);
    } else {
      logEvent('warn', 'OriginBinding', 'unknown-check-origin-field-ignored', { field: part });
    }
  });
  return modes;
};

export const buildBindingStr = async ({
  modes,
  path,
  cf,
  clientIP,
  bindingConfig,
  token,
}) => {
  const modeList = Array.isArray(modes) ? modes : [];
  const modeSet = new Set(modeList);
  const cfg = normalizeBindingConfig(bindingConfig);
  const fail = (reason) => ({ ok: false, reason });

  if (typeof token !== 'string' || !token) {
    return fail('binding token missing');
  }

  let pathHash = 'any';
  if (modeSet.has('path')) {
    const normalizedPath = normalizePath(path);
    if (!normalizedPath) {
      return fail('binding path missing');
    }
    pathHash = await sha256Hash(normalizedPath);
    if (!pathHash) {
      return fail('binding path hash missing');
    }
  }

  const ipValue = typeof clientIP === 'string' ? clientIP.trim() : '';
  let ipScope = 'any';
  if (modeSet.has('ip')) {
    if (!ipValue) {
      return fail('binding ip missing');
    }
    ipScope = ipValue;
  } else if (modeSet.has('iprange')) {
    if (!ipValue) {
      return fail('binding ip missing');
    }
    const subnet = calculateIPSubnet(ipValue, cfg.ipv4Suffix, cfg.ipv6Suffix);
    if (!subnet) {
      return fail('binding iprange missing');
    }
    ipScope = subnet;
  }

  const safeCf = cf && typeof cf === 'object' ? cf : {};

  let country = 'any';
  if (modeSet.has('country')) {
    country = normalizeRegionValue('country', safeCf.country);
    if (!country) {
      return fail('binding country missing');
    }
  }

  let continent = 'any';
  if (modeSet.has('continent')) {
    continent = normalizeRegionValue('continent', safeCf.continent);
    if (!continent) {
      return fail('binding continent missing');
    }
  }

  let region = 'any';
  if (modeSet.has('region')) {
    region = normalizeRegionValue('region', safeCf.region);
    if (!region) {
      return fail('binding region missing');
    }
  }

  let city = 'any';
  if (modeSet.has('city')) {
    city = normalizeRegionValue('city', safeCf.city);
    if (!city) {
      return fail('binding city missing');
    }
  }

  let asn = 'any';
  if (modeSet.has('asn')) {
    asn = normalizeAsnValue(safeCf.asn);
    if (!asn) {
      return fail('binding asn missing');
    }
  }

  let tlsHash = 'any';
  const wantsTls = modeSet.has('tls') && cfg.bindTls;
  if (wantsTls) {
    const tlsExtensions = typeof safeCf.tlsClientExtensionsSha1 === 'string'
      ? safeCf.tlsClientExtensionsSha1.trim()
      : '';
    const tlsCiphers = typeof safeCf.tlsClientCiphersSha1 === 'string'
      ? safeCf.tlsClientCiphersSha1.trim()
      : '';
    if (!tlsExtensions || !tlsCiphers) {
      return fail('binding tls missing');
    }
    tlsHash = await sha256Hash(`${tlsExtensions}|${tlsCiphers}`);
    if (!tlsHash) {
      return fail('binding tls hash missing');
    }
  }

  const canonical = [
    `v${cfg.version}`,
    pathHash || 'any',
    ipScope || 'any',
    country || 'any',
    continent || 'any',
    region || 'any',
    city || 'any',
    asn || 'any',
    tlsHash || 'any',
  ].join('|');

  try {
    const macBytes = await hmacSha256(token, canonical);
    const bindingStr = base64UrlEncode(macBytes).replace(/=+$/u, '');
    return { ok: true, bindingStr };
  } catch (error) {
    return fail(error instanceof Error ? error.message : 'binding hmac failed');
  }
};

export const deriveAesKeyFromToken = async (token) => {
  if (typeof token !== 'string' || !token) {
    throw new Error('token secret is required for origin encryption');
  }
  const material = textEncoder.encode(`aes:${token}`);
  const hash = await crypto.subtle.digest('SHA-256', material);
  return crypto.subtle.importKey(
    'raw',
    hash,
    { name: 'AES-GCM' },
    false,
    ['encrypt', 'decrypt']
  );
};

const encryptPayload = async (payload, token) => {
  if (!payload || typeof payload !== 'object') {
    throw new Error('payload is required');
  }
  const aesKey = await deriveAesKeyFromToken(token);
  const iv = new Uint8Array(12);
  crypto.getRandomValues(iv);
  const plaintext = textEncoder.encode(JSON.stringify(payload));
  const cipherBuffer = await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, aesKey, plaintext);
  const cipherBytes = new Uint8Array(cipherBuffer);
  const envelope = {
    v: 2,
    iv: base64UrlEncode(iv),
    ct: base64UrlEncode(cipherBytes),
  };
  return base64UrlEncode(textEncoder.encode(JSON.stringify(envelope)));
};

const decryptPayload = async (encryptValue, token) => {
  if (typeof encryptValue !== 'string' || encryptValue.length === 0) {
    return null;
  }
  const payloadBytes = base64UrlDecode(encryptValue);
  if (!payloadBytes) {
    return null;
  }
  let envelope;
  try {
    envelope = JSON.parse(textDecoder.decode(payloadBytes));
  } catch (_error) {
    return null;
  }
  if (!envelope || (envelope.v !== 1 && envelope.v !== 2)) {
    return null;
  }
  if (typeof envelope.iv !== 'string' || typeof envelope.ct !== 'string') {
    return null;
  }
  const ivBytes = base64UrlDecode(envelope.iv);
  const cipherBytes = base64UrlDecode(envelope.ct);
  if (!ivBytes || ivBytes.length !== 12 || !cipherBytes || cipherBytes.length === 0) {
    return null;
  }
  try {
    const aesKey = await deriveAesKeyFromToken(token);
    const plaintext = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: ivBytes }, aesKey, cipherBytes);
    const decoded = textDecoder.decode(new Uint8Array(plaintext));
    const payload = JSON.parse(decoded);
    if (!payload || typeof payload !== 'object') {
      return null;
    }
    return payload;
  } catch (_error) {
    return null;
  }
};

export const encryptBindingPayload = async (payload, token) => encryptPayload(payload, token);

export const decryptBindingPayload = async (encryptValue, token) => {
  const payload = await decryptPayload(encryptValue, token);
  if (!payload || payload.v !== 2) {
    return null;
  }
  return payload;
};
