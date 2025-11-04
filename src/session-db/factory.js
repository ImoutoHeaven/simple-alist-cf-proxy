import { SessionDBManagerD1 } from './d1.js';
import { SessionDBManagerD1Rest } from './d1-rest.js';
import { SessionDBManagerPostgREST } from './custom-pg-rest.js';

/**
 * Create session database manager instance based on configuration.
 *
 * @param {object} config
 * @returns {object|null}
 */
export const createSessionDBManager = (config) => {
  if (!config?.sessionEnabled) {
    return null;
  }

  const rawMode = (config.sessionDbMode || config.dbMode || '').trim().toLowerCase();
  if (!rawMode) {
    throw new Error('SESSION_ENABLED is true but no SESSION_DB_MODE or DB_MODE specified');
  }

  const sessionConfig = config.sessionDbConfig || {};

  switch (rawMode) {
    case 'd1':
      return new SessionDBManagerD1(sessionConfig);
    case 'd1-rest':
      return new SessionDBManagerD1Rest(sessionConfig);
    case 'custom-pg-rest':
      return new SessionDBManagerPostgREST(sessionConfig);
    default:
      throw new Error(
        `Invalid SESSION_DB_MODE: "${config.sessionDbMode || rawMode}". Valid options are: "d1", "d1-rest", "custom-pg-rest".`
      );
  }
};
