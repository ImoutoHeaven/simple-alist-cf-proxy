import { applyVerifyHeaders } from '../utils.js';

const DEFAULT_TABLE_NAME = 'SESSION_MAPPING_TABLE';

export class SessionDBManagerPostgREST {
  constructor(options = {}) {
    this.postgrestUrl = options.postgrestUrl || '';
    this.tableName = options.tableName || DEFAULT_TABLE_NAME;
    this.verifyHeader = options.verifyHeader;
    this.verifySecret = options.verifySecret;

    if (!this.postgrestUrl) {
      throw new Error('[SessionDB][PostgREST] postgrestUrl is required');
    }
  }

  async #callRpc(rpcName, payload = {}) {
    const url = `${this.postgrestUrl}/rpc/${rpcName}`;
    const headers = { 'Content-Type': 'application/json' };
    applyVerifyHeaders(headers, this.verifyHeader, this.verifySecret);

    const response = await fetch(url, {
      method: 'POST',
      headers,
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const errorText = await response.text().catch(() => '');
      throw new Error(`[SessionDB][PostgREST] RPC ${rpcName} failed (${response.status}): ${errorText}`);
    }

    const contentType = response.headers.get('content-type') || '';
    if (contentType.includes('application/json')) {
      return response.json().catch(() => null);
    }
    return null;
  }

  #normalizeResult(raw) {
    if (!raw) {
      return null;
    }

    if (Array.isArray(raw)) {
      if (raw.length === 0) {
        return null;
      }
      const first = raw[0];
      if (first && typeof first === 'object') {
        if (first.session_get && typeof first.session_get === 'object') {
          return first.session_get;
        }
        return first;
      }
      return null;
    }

    if (typeof raw === 'object') {
      if (raw.session_get && typeof raw.session_get === 'object') {
        return raw.session_get;
      }
      return raw;
    }

    return null;
  }

  async get(sessionTicket) {
    const payload = { p_session_ticket: sessionTicket };
    const raw = await this.#callRpc('session_get', payload);
    const result = this.#normalizeResult(raw);

    if (!result || result.found !== true) {
      return { found: false };
    }

    const expireRaw = result.expire_at ?? result.EXPIRE_AT;
    const expireAt = Number.isFinite(Number(expireRaw)) ? Number(expireRaw) : 0;

    return {
      found: true,
      file_path: result.file_path ?? result.FILE_PATH ?? '',
      ip_subnet: result.ip_subnet ?? result.IP_SUBNET ?? '',
      worker_address: result.worker_address ?? result.WORKER_ADDRESS ?? '',
      expire_at: expireAt,
    };
  }
}
