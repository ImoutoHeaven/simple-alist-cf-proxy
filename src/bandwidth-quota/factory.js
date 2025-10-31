import * as customPgRestBandwidth from './custom-pg-rest.js';
import * as d1Bandwidth from './d1.js';
import * as d1RestBandwidth from './d1-rest.js';

/**
 * Return the bandwidth quota manager implementation for the specified database mode.
 * @param {string|null} dbMode
 * @returns {{upsertBandwidthQuota: Function}|null}
 */
export const createBandwidthQuotaManager = (dbMode) => {
  if (!dbMode) {
    return null;
  }

  const normalized = String(dbMode).trim().toLowerCase();

  switch (normalized) {
    case 'custom-pg-rest':
      return customPgRestBandwidth;
    case 'd1':
      return d1Bandwidth;
    case 'd1-rest':
      return d1RestBandwidth;
    default:
      throw new Error(
        `Invalid DB_MODE for bandwidth quota: "${dbMode}". Valid options: "custom-pg-rest", "d1", "d1-rest", or leave empty to disable quotas.`
      );
  }
};
