/**
 * Rate Limiter Configuration
 * @typedef {Object} RateLimitConfig
 * @property {number} windowTimeSeconds - Time window in seconds.
 * @property {number} limit - Maximum requests allowed per window.
 * @property {string} ipv4Suffix - IPv4 subnet suffix (e.g., "/32").
 * @property {string} ipv6Suffix - IPv6 subnet suffix (e.g., "/60").
 * @property {string} pgErrorHandle - Error handling strategy: "fail-open" or "fail-closed".
 * @property {number} cleanupProbability - Cleanup probability (0.0 - 1.0).
 * @property {number} blockTimeSeconds - Additional block duration in seconds when the limit is exceeded.
 */

/**
 * Rate Limit Result
 * @typedef {Object} RateLimitResult
 * @property {boolean} allowed - Whether the request is allowed.
 * @property {string} [ipSubnet] - IP subnet that triggered the rate limit (when blocked).
 * @property {number} [retryAfter] - Seconds to wait before retrying (when blocked).
 * @property {string} [error] - Error message when database access fails and pgErrorHandle is "fail-closed".
 */

/**
 * Rate limiter implementations expose a `checkRateLimit(ip, config)` method.
 * This file serves as documentation only.
 */
