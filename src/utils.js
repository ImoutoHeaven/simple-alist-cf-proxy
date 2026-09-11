/**
 * Parse boolean value from environment variable
 * @param {*} value - Value to parse
 * @param {boolean} defaultValue - Default value if parsing fails
 * @returns {boolean}
 */
export const parseBoolean = (value, defaultValue = false) => {
  if (value === undefined || value === null || value === '') return defaultValue;
  const lowered = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(lowered)) return true;
  if (['0', 'false', 'no', 'off'].includes(lowered)) return false;
  return defaultValue;
};

/**
 * Parse integer value from environment variable
 * @param {*} value - Value to parse
 * @param {number} defaultValue - Default value if parsing fails
 * @returns {number}
 */
export const parseInteger = (value, defaultValue) => {
  if (value === undefined || value === null || value === '') return defaultValue;
  const parsed = Number.parseInt(value, 10);
  if (Number.isNaN(parsed)) return defaultValue;
  return parsed;
};

/**
 * Parse float number from environment variable
 * @param {*} value - Value to parse
 * @param {number} defaultValue - Default value if parsing fails
 * @returns {number}
 */
export const parseNumber = (value, defaultValue) => {
  if (value === undefined || value === null || value === '') return defaultValue;
  const parsed = Number.parseFloat(value);
  if (Number.isNaN(parsed)) return defaultValue;
  return parsed;
};

/**
 * Parse time window string (e.g., "24h", "4h", "30m", "10s") to seconds
 * @param {string} value - Time window string
 * @returns {number} - Time in seconds, or 0 if invalid
 */
export const parseWindowTime = (value) => {
  if (!value || typeof value !== 'string') return 0;
  const trimmed = value.trim();
  const match = trimmed.match(/^(\d+)(d|h|m|s)$/);
  if (!match) return 0;
  const num = Number.parseInt(match[1], 10);
  if (Number.isNaN(num) || num <= 0) return 0;
  const unit = match[2];
  if (unit === 'd') return num * 86400;
  if (unit === 'h') return num * 3600;
  if (unit === 'm') return num * 60;
  if (unit === 's') return num;
  return 0;
};

/**
 * Calculate subnet string for IPv4/IPv6 address
 * @param {string} ip - IP address
 * @param {string} ipv4Suffix - IPv4 subnet suffix (e.g., "/32")
 * @param {string} ipv6Suffix - IPv6 subnet suffix (e.g., "/60")
 * @returns {string}
 */
export const calculateIPSubnet = (ip, ipv4Suffix, ipv6Suffix) => {
  if (!ip || typeof ip !== 'string') return '';
  const trimmedIP = ip.trim();
  if (!trimmedIP) return '';

  let processingIP = trimmedIP;
  const lowerIP = trimmedIP.toLowerCase();
  if (lowerIP.startsWith('::ffff:') && lowerIP.includes('.')) {
    processingIP = trimmedIP.substring(7);
  }

  if (processingIP.includes(':') && !processingIP.includes('.')) {
    const suffix = ipv6Suffix || '/60';
    const prefixLength = Number.parseInt(suffix.replace('/', ''), 10);
    if (Number.isNaN(prefixLength) || prefixLength < 0 || prefixLength > 128) {
      return `${processingIP}${suffix}`;
    }

    try {
      const parts = processingIP.split('::');
      if (parts.length > 2) {
        return `${processingIP}${suffix}`;
      }
      const left = parts[0] ? parts[0].split(':').filter(Boolean) : [];
      const right = parts.length === 2 && parts[1] ? parts[1].split(':').filter(Boolean) : [];
      if (left.length + right.length > 8) {
        return `${processingIP}${suffix}`;
      }
      const full = [
        ...left,
        ...Array(8 - (left.length + right.length)).fill('0'),
        ...right,
      ];
      const expanded = full.map((h) => Number.parseInt(h, 16) || 0);

      const bitsPerGroup = 16;
      const fullGroups = Math.floor(prefixLength / bitsPerGroup);
      const remainingBits = prefixLength % bitsPerGroup;

      for (let i = fullGroups; i < 8; i += 1) {
        if (i === fullGroups && remainingBits > 0) {
          const mask = (0xFFFF << (bitsPerGroup - remainingBits)) & 0xFFFF;
          expanded[i] = (expanded[i] || 0) & mask;
        } else {
          expanded[i] = 0;
        }
      }

      const hex = expanded.map((n) => (n || 0).toString(16));
      return `${hex.join(':')}${suffix}`;
    } catch (error) {
      return `${processingIP}${suffix}`;
    }
  } else {
    const suffix = ipv4Suffix || '/32';
    const prefixLength = Number.parseInt(suffix.replace('/', ''), 10);
    if (Number.isNaN(prefixLength) || prefixLength < 0 || prefixLength > 32) {
      return `${processingIP}${suffix}`;
    }

    try {
      const octets = processingIP.split('.').map((o) => Number.parseInt(o, 10));
      if (octets.length !== 4 || octets.some((o) => Number.isNaN(o) || o < 0 || o > 255)) {
        return `${processingIP}${suffix}`;
      }

      let ipInt = (octets[0] << 24) | (octets[1] << 16) | (octets[2] << 8) | octets[3];
      const mask = prefixLength === 0 ? 0 : (0xFFFFFFFF << (32 - prefixLength)) >>> 0;
      ipInt = (ipInt & mask) >>> 0;

      const subnetOctets = [
        (ipInt >>> 24) & 0xFF,
        (ipInt >>> 16) & 0xFF,
        (ipInt >>> 8) & 0xFF,
        ipInt & 0xFF,
      ];

      return `${subnetOctets.join('.')}${suffix}`;
    } catch (error) {
      return `${processingIP}${suffix}`;
    }
  }
};

/**
 * Calculate SHA256 hash of a string
 * @param {string} text - Text to hash
 * @returns {Promise<string>} - Hex string of hash
 */
export const sha256Hash = async (text) => {
  if (!text || typeof text !== 'string') return '';
  const encoder = new TextEncoder();
  const data = encoder.encode(text);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
  return hashHex;
};

/**
 * Extract hostname from a URL
 * @param {string} url - Full URL string
 * @returns {string} - Hostname or empty string if invalid
 */
export const extractHostname = (url) => {
  if (!url || typeof url !== 'string') return '';
  try {
    const parsedUrl = new URL(url);
    return parsedUrl.hostname.toLowerCase();
  } catch (error) {
    return '';
  }
};

/**
 * Apply verify header/secret pairs to a headers object.
 * Supports both legacy string values and new array format.
 * @param {Object} targetHeaders - Headers object to mutate
 * @param {string|string[]} verifyHeader - Header name(s)
 * @param {string|string[]} verifySecret - Header value(s)
 */
export const applyVerifyHeaders = (targetHeaders, verifyHeader, verifySecret) => {
  if (!targetHeaders || typeof targetHeaders !== 'object') {
    return;
  }

  if (Array.isArray(verifyHeader) && Array.isArray(verifySecret)) {
    verifyHeader.forEach((headerName, index) => {
      if (!headerName || typeof headerName !== 'string') {
        return;
      }
      const secretValue = verifySecret[index];
      if (secretValue === undefined || secretValue === null) {
        return;
      }
      targetHeaders[headerName] = secretValue;
    });
    return;
  }

  if (typeof verifyHeader === 'string' && typeof verifySecret === 'string' && verifyHeader && verifySecret) {
    targetHeaders[verifyHeader] = verifySecret;
  }
};

/**
 * Determine if verify header/secret values are present.
 * Supports both legacy string format and new array format.
 * @param {string|string[]} verifyHeader
 * @param {string|string[]} verifySecret
 * @returns {boolean}
 */
export const hasVerifyCredentials = (verifyHeader, verifySecret) => {
  if (Array.isArray(verifyHeader) && Array.isArray(verifySecret)) {
    return verifyHeader.length > 0 && verifySecret.length > 0;
  }

  return Boolean(verifyHeader) && Boolean(verifySecret);
};

export const readResponseTextWithSignal = async (response, signal = null) => {
  if (!response?.body || typeof response.body.getReader !== 'function') {
    if (typeof response?.text === 'function') {
      return response.text();
    }
    if (typeof response?.json === 'function') {
      return JSON.stringify(await response.json());
    }
    return '';
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let text = '';
  let aborted = false;
  let abortReason = null;
  let rejectAbort;
  const abortPromise = new Promise((_, reject) => {
    rejectAbort = reject;
  });
  const onAbort = () => {
    aborted = true;
    abortReason = signal?.reason || new DOMException('The operation was aborted', 'AbortError');
    rejectAbort(abortReason);
    reader.cancel()?.catch?.(() => {});
  };

  if (signal?.aborted) {
    onAbort();
  } else {
    signal?.addEventListener?.('abort', onAbort, { once: true });
  }

  try {
    while (true) {
      const readResult = await Promise.race([reader.read(), abortPromise]);
      if (aborted || signal?.aborted) {
        throw abortReason || signal?.reason || new DOMException('The operation was aborted', 'AbortError');
      }
      if (readResult.done) {
        text += decoder.decode();
        return text;
      }
      text += decoder.decode(readResult.value, { stream: true });
    }
  } finally {
    signal?.removeEventListener?.('abort', onAbort);
    try {
      reader.releaseLock();
    } catch (_error) {
      // Best-effort reader cleanup.
    }
  }
};

export const isUsableReadyLink = (linkData) => {
  const expiresAt = linkData?.download?.expires_at;
  if (!(
    linkData
    && typeof linkData === 'object'
    && !Array.isArray(linkData)
    && typeof linkData.url === 'string'
    && linkData.url.trim()
  )) {
    return false;
  }

  let parsedUrl;
  try {
    parsedUrl = new URL(linkData.url);
  } catch {
    return false;
  }
  if (parsedUrl.protocol !== 'http:' && parsedUrl.protocol !== 'https:') {
    return false;
  }

  return Boolean(
    (linkData.header === undefined || linkData.header === null || (
      typeof linkData.header === 'object' && !Array.isArray(linkData.header)
    ))
    && linkData.download
    && typeof linkData.download.provider === 'string'
    && linkData.download.provider.trim()
    && typeof linkData.download.ticket === 'string'
    && linkData.download.ticket.trim()
    && Number.isSafeInteger(expiresAt)
    && expiresAt > 0
    && typeof linkData.download.report_success === 'boolean'
  );
};

/**
 * Match hostname against a pattern (supports wildcard)
 * Pattern examples:
 *   "*.sharepoint.com" matches "contoso-my.sharepoint.com" and "sharepoint.com"
 *   "example.com" matches only "example.com"
 *
 * @param {string} hostname - Hostname to check (e.g., "contoso-my.sharepoint.com")
 * @param {string} pattern - Pattern to match (e.g., "*.sharepoint.com")
 * @returns {boolean} - True if hostname matches pattern
 */
export const matchHostnamePattern = (hostname, pattern) => {
  if (!hostname || !pattern || typeof hostname !== 'string' || typeof pattern !== 'string') {
    return false;
  }

  const normalizedHostname = hostname.toLowerCase();
  const normalizedPattern = pattern.toLowerCase();

  // Exact match
  if (normalizedHostname === normalizedPattern) {
    return true;
  }

  // Wildcard match: *.example.com
  if (normalizedPattern.startsWith('*.')) {
    const suffix = normalizedPattern.substring(1); // Remove '*' to get '.example.com'
    const rootDomain = suffix.substring(1); // Remove '.' to get 'example.com'

    // Match both "xxx.example.com" and "example.com" itself
    return normalizedHostname.endsWith(suffix) || normalizedHostname === rootDomain;
  }

  return false;
};
