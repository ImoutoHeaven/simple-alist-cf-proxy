const DEFAULT_TABLE_NAME = 'SESSION_MAPPING_TABLE';

export class SessionDBManagerD1 {
  #ensureTablePromise = null;

  constructor(options = {}) {
    this.env = options.env || null;
    this.databaseBinding = options.databaseBinding || 'SESSIONDB';
    this.tableName = options.tableName || DEFAULT_TABLE_NAME;
  }

  #getDatabase() {
    if (!this.env) {
      throw new Error('[SessionDB][D1] env binding container is not available');
    }
    const db = this.env[this.databaseBinding];
    if (!db || typeof db.prepare !== 'function') {
      throw new Error(`[SessionDB][D1] binding "${this.databaseBinding}" is not available or invalid`);
    }
    return db;
  }

  async #ensureTable(db) {
    if (!this.#ensureTablePromise) {
      this.#ensureTablePromise = (async () => {
        try {
          await db.batch([
            db.prepare(`
              CREATE TABLE IF NOT EXISTS ${this.tableName} (
                SESSION_TICKET TEXT PRIMARY KEY,
                FILE_PATH TEXT NOT NULL,
                IP_SUBNET TEXT NOT NULL,
                WORKER_ADDRESS TEXT NOT NULL,
                EXPIRE_AT INTEGER NOT NULL,
                CREATED_AT INTEGER NOT NULL
              )
            `),
            db.prepare(`CREATE INDEX IF NOT EXISTS idx_session_expire ON ${this.tableName}(EXPIRE_AT)`),
          ]);
        } catch (error) {
          console.error(
            '[SessionDB][D1] Failed to ensure session table:',
            error instanceof Error ? error.message : String(error)
          );
          this.#ensureTablePromise = null;
          throw error;
        }
      })();
    }

    return this.#ensureTablePromise;
  }

  async get(sessionTicket) {
    const db = this.#getDatabase();
    await this.#ensureTable(db);
    const sql = `SELECT SESSION_TICKET, FILE_PATH, IP_SUBNET, WORKER_ADDRESS, EXPIRE_AT FROM ${this.tableName} WHERE SESSION_TICKET = ? LIMIT 1`;
    const row = await db.prepare(sql).bind(sessionTicket).first();
    if (!row) {
      return { found: false };
    }

    const expireRaw = row.EXPIRE_AT ?? row.expire_at ?? row.expireAt;
    const expireAt = Number.isFinite(Number(expireRaw)) ? Number(expireRaw) : 0;

    return {
      found: true,
      file_path: row.FILE_PATH ?? row.file_path ?? '',
      ip_subnet: row.IP_SUBNET ?? row.ip_subnet ?? '',
      worker_address: row.WORKER_ADDRESS ?? row.worker_address ?? '',
      expire_at: expireAt,
    };
  }
}
