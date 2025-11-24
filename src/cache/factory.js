import * as customPgRestCacheManager from './custom-pg-rest.js';

/**
 * Create cache manager instance based on DB mode
 * @param {string} dbMode - Database mode ('custom-pg-rest')
 * @returns {Object} - Cache manager with checkCache and saveCache methods
 * @throws {Error} - If dbMode is invalid
 */
export const createCacheManager = (dbMode) => {
  const normalizedMode = String(dbMode || '').toLowerCase().trim();

  if (normalizedMode === 'custom-pg-rest') {
    return customPgRestCacheManager;
  }

  throw new Error(
    `Invalid DB_MODE: "${dbMode}". Valid option is: "custom-pg-rest", or leave empty to disable caching.`
  );
};
