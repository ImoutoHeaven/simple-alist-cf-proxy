const DEFAULT_TABLE_NAME = 'SESSION_MAPPING_TABLE';

export class SessionDBManagerD1Rest {
  constructor(options = {}) {
    this.accountId = options.accountId || '';
    this.databaseId = options.databaseId || '';
    this.apiToken = options.apiToken || '';
    this.tableName = options.tableName || DEFAULT_TABLE_NAME;

    if (!this.accountId || !this.databaseId || !this.apiToken) {
      throw new Error('[SessionDB][D1-REST] accountId, databaseId, and apiToken are required');
    }
  }

  async #execute(sql, params = []) {
    const endpoint = `https://api.cloudflare.com/client/v4/accounts/${this.accountId}/d1/database/${this.databaseId}/query`;
    const body = params.length > 0 ? { sql, params } : { sql };

    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${this.apiToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const errorText = await response.text().catch(() => '');
      throw new Error(`[SessionDB][D1-REST] query failed (${response.status}): ${errorText}`);
    }

    const payload = await response.json().catch(() => ({}));
    if (payload && payload.success === false) {
      throw new Error(
        `[SessionDB][D1-REST] query unsuccessful: ${JSON.stringify(payload.errors || [])}`
      );
    }

    return payload?.result?.[0] || {};
  }

  async get(sessionTicket) {
    const sql = `SELECT SESSION_TICKET, FILE_PATH, IP_SUBNET, WORKER_ADDRESS, EXPIRE_AT FROM ${this.tableName} WHERE SESSION_TICKET = ? LIMIT 1`;
    const result = await this.#execute(sql, [sessionTicket]);
    const rows = Array.isArray(result?.results) ? result.results : [];

    if (!rows || rows.length === 0) {
      return { found: false };
    }

    const row = rows[0] || {};
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
