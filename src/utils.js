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
  const match = trimmed.match(/^(\d+)(h|m|s)$/);
  if (!match) return 0;
  const num = Number.parseInt(match[1], 10);
  if (Number.isNaN(num) || num <= 0) return 0;
  const unit = match[2];
  if (unit === 'h') return num * 3600;
  if (unit === 'm') return num * 60;
  if (unit === 's') return num;
  return 0;
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
