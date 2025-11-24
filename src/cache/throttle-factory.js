import * as throttleCustomPgRest from './throttle-custom-pg-rest.js';

/**
 * Create throttle manager instance based on DB mode
 * @param {string} dbMode - Database mode ('custom-pg-rest')
 * @returns {Object} - Throttle manager with checkThrottle and updateThrottle methods
 * @throws {Error} - If dbMode is invalid
 */
export const createThrottleManager = (dbMode) => {
  const normalizedMode = String(dbMode || '').toLowerCase().trim();

  if (normalizedMode === 'custom-pg-rest') {
    return throttleCustomPgRest;
  }

  throw new Error(
    `Invalid DB_MODE: "${dbMode}". Valid option is: "custom-pg-rest", or leave empty to disable throttle protection.`
  );
};
