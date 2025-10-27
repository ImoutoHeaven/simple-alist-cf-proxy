/**
 * Download link cache interface
 *
 * This file defines the interface that all cache implementations must follow.
 * Actual implementations are in d1.js, d1-rest.js, and custom-pg-rest.js
 *
 * All cache implementations support:
 * - TTL-based expiration
 * - Atomic UPSERT operations
 * - Background cleanup with configurable probability
 * - ctx.waitUntil for non-blocking cleanup
 */

/**
 * Check if a cached download link exists and is still valid
 *
 * @param {string} path - File path to check
 * @param {Object} config - Cache configuration
 * @param {Object} config.env - Cloudflare Workers env object (for D1 binding)
 * @param {string} config.databaseBinding - D1 database binding name (for D1)
 * @param {string} config.accountId - Cloudflare account ID (for D1-REST)
 * @param {string} config.databaseId - D1 database ID (for D1-REST)
 * @param {string} config.apiToken - Cloudflare API token (for D1-REST)
 * @param {string} config.postgrestUrl - PostgREST API endpoint (for custom-pg-rest)
 * @param {string|string[]} config.verifyHeader - Authentication header name(s) (for custom-pg-rest)
 * @param {string|string[]} config.verifySecret - Authentication header value(s) (for custom-pg-rest)
 * @param {string} config.tableName - Table name (defaults to 'DOWNLOAD_CACHE_TABLE')
 * @param {number} config.linkTTL - Link TTL in seconds
 * @param {Object} config.ctx - ExecutionContext for waitUntil (optional)
 *
 * @returns {Promise<{linkData?: Object} | null>}
 *   Returns { linkData: Object } if cache hit and not expired
 *   Returns null if cache miss or expired
 *   linkData format: { url: string, header: Object|null, Expiration: any, concurrency: number, part_size: number }
 */
export const checkCache = async (path, config) => {
  throw new Error('checkCache must be implemented by subclass');
};

/**
 * Save download link data to cache with atomic UPSERT
 *
 * @param {string} path - File path
 * @param {Object} linkData - Download link data from AList API response.data
 * @param {Object} config - Cache configuration (same as checkCache)
 *
 * @returns {Promise<void>}
 *   Saves the link data atomically using UPSERT
 *   May trigger probabilistic cleanup in background via ctx.waitUntil
 */
export const saveCache = async (path, linkData, config) => {
  throw new Error('saveCache must be implemented by subclass');
};
