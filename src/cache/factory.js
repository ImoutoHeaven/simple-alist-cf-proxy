import * as d1CacheManager from './d1.js';
import * as d1RestCacheManager from './d1-rest.js';
import * as customPgRestCacheManager from './custom-pg-rest.js';

/**
 * Create cache manager instance based on DB mode
 * @param {string} dbMode - Database mode ('d1', 'd1-rest', 'custom-pg-rest')
 * @returns {Object} - Cache manager with checkCache and saveCache methods
 * @throws {Error} - If dbMode is invalid
 */
export const createCacheManager = (dbMode) => {
  const normalizedMode = String(dbMode).toLowerCase().trim();

  switch (normalizedMode) {
    case 'd1':
      return d1CacheManager;
    case 'd1-rest':
      return d1RestCacheManager;
    case 'custom-pg-rest':
      return customPgRestCacheManager;
    default:
      throw new Error(
        `Invalid DB_MODE: "${dbMode}". Valid options are: "d1", "d1-rest", "custom-pg-rest", or leave empty to disable caching.`
      );
  }
};
