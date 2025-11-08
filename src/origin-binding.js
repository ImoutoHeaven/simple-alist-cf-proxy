import { calculateIPSubnet } from './utils.js';

const BASE64_CHARS = { '+': '-', '/': '_', '=': '' };
const VALID_ORIGIN_MODES = new Set(['ip', 'iprange', 'continent', 'country', 'region', 'city', 'asn']);

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

export const buildOriginSnapshot = (cf, ip) => {
  if (!ip || typeof ip !== 'string' || !ip.trim()) {
    return null;
  }
  const safeCf = cf && typeof cf === 'object' ? cf : {};
  const snapshot = {
    ver: 1,
    ip_addr: ip.trim(),
  };
  if (typeof safeCf.country === 'string' && safeCf.country) {
    snapshot.country = safeCf.country;
  }
  if (typeof safeCf.continent === 'string' && safeCf.continent) {
    snapshot.continent = safeCf.continent;
  }
  if (typeof safeCf.region === 'string' && safeCf.region) {
    snapshot.region = safeCf.region;
  }
  if (typeof safeCf.city === 'string' && safeCf.city) {
    snapshot.city = safeCf.city;
  }
  if (typeof safeCf.asn !== 'undefined' && safeCf.asn !== null) {
    snapshot.asn = String(safeCf.asn);
  }
  return snapshot;
};

export const deriveAesKeyFromToken = async (token) => {
  if (typeof token !== 'string' || !token) {
    throw new Error('TOKEN is required for origin encryption');
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

export const encryptOriginSnapshot = async (snapshot, token) => {
  if (!snapshot || typeof snapshot !== 'object') {
    throw new Error('snapshot is required');
  }
  const aesKey = await deriveAesKeyFromToken(token);
  const iv = new Uint8Array(12);
  crypto.getRandomValues(iv);
  const plaintext = textEncoder.encode(JSON.stringify(snapshot));
  const cipherBuffer = await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, aesKey, plaintext);
  const cipherBytes = new Uint8Array(cipherBuffer);
  const payload = {
    v: 1,
    iv: base64UrlEncode(iv),
    ct: base64UrlEncode(cipherBytes),
  };
  return base64UrlEncode(textEncoder.encode(JSON.stringify(payload)));
};

export const decryptOriginSnapshot = async (encryptValue, token) => {
  if (typeof encryptValue !== 'string' || encryptValue.length === 0) {
    return null;
  }
  const payloadBytes = base64UrlDecode(encryptValue);
  if (!payloadBytes) {
    return null;
  }
  let payload;
  try {
    payload = JSON.parse(textDecoder.decode(payloadBytes));
  } catch (_error) {
    return null;
  }
  if (!payload || payload.v !== 1 || typeof payload.iv !== 'string' || typeof payload.ct !== 'string') {
    return null;
  }
  const ivBytes = base64UrlDecode(payload.iv);
  const cipherBytes = base64UrlDecode(payload.ct);
  if (!ivBytes || ivBytes.length !== 12 || !cipherBytes || cipherBytes.length === 0) {
    return null;
  }
  try {
    const aesKey = await deriveAesKeyFromToken(token);
    const plaintext = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: ivBytes }, aesKey, cipherBytes);
    const decoded = textDecoder.decode(new Uint8Array(plaintext));
    const snapshot = JSON.parse(decoded);
    if (!snapshot || snapshot.ver !== 1 || typeof snapshot.ip_addr !== 'string') {
      return null;
    }
    return snapshot;
  } catch (_error) {
    return null;
  }
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
      console.warn(`[origin-binding] Unknown CHECK_ORIGIN field "${part}" ignored`);
    }
  });
  return modes;
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

export const checkOriginMatch = (snapshot, current, modes, options = {}) => {
  if (!Array.isArray(modes) || modes.length === 0) {
    return { ok: true, failedFields: [] };
  }
  const failedFields = [];
  const ipv4Suffix = typeof options.ipv4Suffix === 'string' ? options.ipv4Suffix : '/32';
  const ipv6Suffix = typeof options.ipv6Suffix === 'string' ? options.ipv6Suffix : '/60';
  const currentIp = current?.ip_addr || null;
  const snapshotIp = snapshot?.ip_addr || null;

  for (const mode of modes) {
    switch (mode) {
      case 'ip': {
        if (!snapshotIp || !currentIp || snapshotIp !== currentIp) {
          failedFields.push('ip');
        }
        break;
      }
      case 'iprange': {
        if (!snapshotIp || !currentIp) {
          failedFields.push('iprange');
          break;
        }
        const sRange = calculateIPSubnet(snapshotIp, ipv4Suffix, ipv6Suffix);
        const cRange = calculateIPSubnet(currentIp, ipv4Suffix, ipv6Suffix);
        if (!sRange || !cRange || sRange !== cRange) {
          failedFields.push('iprange');
        }
        break;
      }
      case 'continent':
      case 'country':
      case 'region':
      case 'city':
      case 'asn': {
        const left = normalizeRegionValue(mode, snapshot?.[mode]);
        const right = normalizeRegionValue(mode, current?.[mode]);
        if (!left || !right || left !== right) {
          failedFields.push(mode);
        }
        break;
      }
      default:
        // Unknown modes are ignored (already warned during parsing)
        break;
    }
  }

  return {
    ok: failedFields.length === 0,
    failedFields,
  };
};

