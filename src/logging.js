const REDACTED = '[redacted]';
const OMIT = Symbol('omit-log-field');
const MAX_STRING_LENGTH = 1_000;
const MAX_DEPTH = 6;
const SENSITIVE_KEY_FRAGMENTS = [
  'token',
  'secret',
  'auth',
  'authorization',
  'payload',
  'signature',
  'cookie',
  'linkdata',
  'cachelinkdata',
  'slottoken',
  'leasetoken',
  'claimtoken',
  'handofftoken',
  'waittoken',
];

function normalizeKey(key) {
  return String(key ?? '').replace(/[^a-z0-9]/gi, '').toLowerCase();
}

function isSensitiveKey(key) {
  const normalized = normalizeKey(key);
  return SENSITIVE_KEY_FRAGMENTS.some((fragment) => normalized.includes(fragment));
}

function isIpKey(key) {
  const normalized = normalizeKey(key);
  return normalized === 'ip'
    || /(?:^|client|remote|source|origin|forwarded|real)ip$/.test(normalized)
    || normalized.includes('iprange')
    || normalized.includes('cidr')
    || normalized.includes('ipaddress');
}

function normalizeWhitespace(value) {
  return String(value).replace(/\s+/g, ' ').trim();
}

function truncate(value) {
  return value.length > MAX_STRING_LENGTH ? `${value.slice(0, MAX_STRING_LENGTH)}...` : value;
}

function isLikelyIpv6Address(value) {
  const withoutCidr = String(value).replace(/\/\d{1,3}$/, '');
  const address = withoutCidr.startsWith('[') && withoutCidr.endsWith(']')
    ? withoutCidr.slice(1, -1)
    : withoutCidr;

  if (!address.includes(':') || /[^a-f0-9:]/i.test(address) || (address.match(/::/g) || []).length > 1) {
    return false;
  }

  const groups = address.split(':');
  if (address.includes('::')) {
    const explicitGroups = groups.filter(Boolean);
    return explicitGroups.length > 0
      && explicitGroups.length < 8
      && explicitGroups.every((group) => /^[a-f0-9]{1,4}$/i.test(group));
  }

  return groups.length === 8 && groups.every((group) => /^[a-f0-9]{1,4}$/i.test(group));
}

function scrubIpv6Strings(value) {
  let text = value.replace(/\[[a-f0-9:]+\](?:\/\d{1,3}|:\d{1,5})?/gi, (candidate) => {
    const address = candidate.replace(/(?:\/\d{1,3}|:\d{1,5})$/, '');
    return isLikelyIpv6Address(address) ? REDACTED : candidate;
  });

  text = text.replace(/(^|[^a-z0-9:])((?:[a-f0-9]{0,4}:){2,}[a-f0-9]{0,4}(?:\/\d{1,3})?)(?=$|[^a-z0-9:])/gi, (match, prefix, candidate) => (
    isLikelyIpv6Address(candidate) ? `${prefix}${REDACTED}` : match
  ));

  return text;
}

function scrubString(value) {
  let text = normalizeWhitespace(value);
  if (!text) {
    return text;
  }

  text = text.replace(/Authorization\s*:\s*Bearer\s+[^\s,;)}\]]+/gi, `Authorization: ${REDACTED}`);
  text = text.replace(/Bearer\s+[^\s,;)}\]]+/gi, `Bearer ${REDACTED}`);
  text = text.replace(/https?:\/\/\[[a-f0-9:]+\](?::\d{1,5})?[^\s,;)}\]]*/gi, (url) => {
    const queryIndex = url.indexOf('?');
    return queryIndex === -1 ? url : url.slice(0, queryIndex);
  });
  text = text.replace(/https?:\/\/[^\s,;)}\]]+/gi, (url) => {
    const queryIndex = url.indexOf('?');
    return queryIndex === -1 ? url : url.slice(0, queryIndex);
  });
  text = text.replace(/\b(verifySecret|secret|token|payload|signature)\s*[:=]\s*([^\s,;&)}\]]+)/gi, '$1=[redacted]');
  text = text.replace(/\b(?:\d{1,3}\.){3}\d{1,3}(?:\/\d{1,2})?\b/g, REDACTED);
  text = scrubIpv6Strings(text);

  return truncate(text);
}

function hasSensitiveJsonText(text) {
  const trimmed = String(text).trim();
  return /^[{[]/.test(trimmed)
    && (/"?(cache[_-]?link[_-]?data|authorization|auth|token|secret|payload|signature|cookie)"?\s*:/i.test(text)
      || /https?:\/\/[^\s"']+\?[^\s"']+/i.test(text));
}

function sanitizeAny(value, key, seen, depth) {
  try {
    if (isSensitiveKey(key)) {
      return REDACTED;
    }
    if (isIpKey(key)) {
      return OMIT;
    }
    if (value === null || value === undefined) {
      return value;
    }
    if (typeof value === 'string') {
      if (hasSensitiveJsonText(value)) {
        return REDACTED;
      }
      const scrubbed = scrubString(value);
      return scrubbed;
    }
    if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') {
      return value;
    }
    if (typeof value === 'symbol' || typeof value === 'function') {
      return scrubString(String(value));
    }
    if (seen.has(value)) {
      return '[circular]';
    }
    if (depth >= MAX_DEPTH) {
      return '[truncated]';
    }

    seen.add(value);
    if (Array.isArray(value)) {
      return value
        .map((item) => sanitizeAny(item, '', seen, depth + 1))
        .filter((item) => item !== OMIT);
    }

    const output = {};
    for (const [entryKey, entryValue] of Object.entries(value)) {
      const sanitized = sanitizeAny(entryValue, entryKey, seen, depth + 1);
      if (sanitized !== OMIT) {
        output[scrubString(entryKey)] = sanitized;
      }
    }
    return output;
  } catch {
    return REDACTED;
  }
}

function stringifySanitized(value) {
  try {
    if (typeof value === 'string') {
      return scrubString(value);
    }
    if (value === null || value === undefined) {
      return String(value);
    }
    if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') {
      return String(value);
    }
    return scrubString(JSON.stringify(value));
  } catch {
    return REDACTED;
  }
}

export function sanitizeLogStructuredValue(value) {
  return sanitizeAny(value, '', new WeakSet(), 0);
}

export function sanitizeLogValue(value) {
  return stringifySanitized(sanitizeLogStructuredValue(value));
}

export function logEvent(level, scope, event, fields = {}) {
  try {
    const safeScope = sanitizeLogValue(scope) || 'Log';
    const safeEvent = sanitizeLogValue(event) || 'event';
    const safeFields = sanitizeLogStructuredValue(fields);
    const parts = [`[${safeScope}]`, safeEvent];

    if (safeFields && typeof safeFields === 'object' && !Array.isArray(safeFields)) {
      for (const [key, value] of Object.entries(safeFields)) {
        parts.push(`${sanitizeLogValue(key)}=${sanitizeLogValue(value)}`);
      }
    }

    const line = parts.join(' ');
    if (level === 'warn') {
      console.warn(line);
    } else if (level === 'error') {
      console.error(line);
    } else {
      console.log(line);
    }
  } catch {
    // Logging must never affect application flow.
  }
}

export function bindWaitUntil(ctx, promise, scope, event, fields = {}) {
  const logState = (level, state, extraFields) => {
    logEvent(level, scope, event, { fields, state, ...extraFields });
  };

  const tracked = Promise.resolve(promise).then(
    (value) => {
      logState('info', 'done');
      return value;
    },
    (error) => {
      logState('error', 'failed', { error });
      throw error;
    },
  );

  if (ctx && typeof ctx.waitUntil === 'function') {
    logState('info', 'bound');
    try {
      ctx.waitUntil(tracked);
    } catch (error) {
      logState('error', 'bind_failed', { error });
      throw error;
    }
  } else {
    logState('info', 'inline');
  }

  return tracked;
}
