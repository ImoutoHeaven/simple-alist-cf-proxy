import * as customPgRestRateLimiter from './custom-pg-rest.js';

/**
 * Create a rate limiter instance based on the database mode.
 * @param {string|null} dbMode - Database mode: "custom-pg-rest", or falsy to disable.
 * @returns {Object|null} - Rate limiter implementation, or null when disabled.
 */
export const createRateLimiter = (dbMode) => {
  if (!dbMode) {
    return null;
  }

  const normalizedDbMode = String(dbMode || '').trim().toLowerCase();

  if (normalizedDbMode === 'custom-pg-rest') {
    return customPgRestRateLimiter;
  }

  throw new Error(
    `Invalid DB_MODE for rate limiter: "${dbMode}". Valid option is: "custom-pg-rest", or leave empty to disable rate limiting.`
  );
};
