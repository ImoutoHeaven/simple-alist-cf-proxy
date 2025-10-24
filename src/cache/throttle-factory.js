import * as throttleD1 from './throttle-d1.js';
import * as throttleD1Rest from './throttle-d1-rest.js';
import * as throttleCustomPgRest from './throttle-custom-pg-rest.js';

/**
 * Create throttle manager instance based on DB mode
 * @param {string} dbMode - Database mode ('d1', 'd1-rest', 'custom-pg-rest')
 * @returns {Object} - Throttle manager with checkThrottle and updateThrottle methods
 * @throws {Error} - If dbMode is invalid
 */
export const createThrottleManager = (dbMode) => {
  const normalizedMode = String(dbMode).toLowerCase().trim();

  switch (normalizedMode) {
    case 'd1':
      return throttleD1;
    case 'd1-rest':
      return throttleD1Rest;
    case 'custom-pg-rest':
      return throttleCustomPgRest;
    default:
      throw new Error(
        `Invalid DB_MODE: "${dbMode}". Valid options are: "d1", "d1-rest", "custom-pg-rest", or leave empty to disable throttle protection.`
      );
  }
};
