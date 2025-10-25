import * as d1RateLimiter from './d1.js';
import * as d1RestRateLimiter from './d1-rest.js';
import * as customPgRestRateLimiter from './custom-pg-rest.js';

/**
 * Create a rate limiter instance based on the database mode.
 * @param {string|null} dbMode - Database mode: "d1", "d1-rest", "custom-pg-rest", or falsy to disable.
 * @returns {Object|null} - Rate limiter implementation, or null when disabled.
 */
export const createRateLimiter = (dbMode) => {
  if (!dbMode) {
    return null;
  }

  const normalizedDbMode = String(dbMode).trim().toLowerCase();

  switch (normalizedDbMode) {
    case 'd1':
      return d1RateLimiter;
    case 'd1-rest':
      return d1RestRateLimiter;
    case 'custom-pg-rest':
      return customPgRestRateLimiter;
    default:
      throw new Error(
        `Invalid DB_MODE for rate limiter: "${dbMode}". Valid options are: "d1", "d1-rest", "custom-pg-rest", or leave empty to disable rate limiting.`
      );
  }
};
