import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const initSql = readFileSync(join(__dirname, 'init.sql'), 'utf8');

const readFunctionBody = (functionName) => {
  const pattern = new RegExp(
    `CREATE OR REPLACE FUNCTION ${functionName}[\\s\\S]*?\\$\\$ LANGUAGE plpgsql;`,
    'i',
  );
  const match = initSql.match(pattern);
  expect(match, `expected to find function ${functionName} in init.sql`).toBeTruthy();
  return match[0];
};

const expectPatternIndex = (text, pattern, message) => {
  const index = text.search(pattern);
  expect(index, message).toBeGreaterThan(-1);
  return index;
};

describe('init.sql breaker RPC definitions', () => {
  it('defines breaker warmup runtime columns', () => {
    expect(initSql).toMatch(/"SAMPLES_SINCE_RESET"\s+INTEGER\s+NOT NULL DEFAULT 0/i);
    expect(initSql).toMatch(/"LAST_SAMPLE_AT"\s+INTEGER/i);
  });

  it('defines canonical resolved/success half-open batch columns and removes replaced bookkeeping columns', () => {
    expect(initSql).toMatch(/"HALF_OPEN_BUDGET"\s+INTEGER\s+NOT NULL DEFAULT 0/i);
    expect(initSql).toMatch(/"HALF_OPEN_ISSUED"\s+INTEGER\s+NOT NULL DEFAULT 0/i);
    expect(initSql).toMatch(/"HALF_OPEN_RESOLVED_MASK"\s+BIGINT\s+NOT NULL DEFAULT 0/i);
    expect(initSql).toMatch(/"HALF_OPEN_SUCCESS_MASK"\s+BIGINT\s+NOT NULL DEFAULT 0/i);
    expect(initSql).toMatch(/"HALF_OPEN_DEADLINE"\s+INTEGER/i);
    expect(initSql).not.toMatch(/"HALF_OPEN_REPORTED_MASK"\s+BIGINT\s+NOT NULL DEFAULT 0/i);
    expect(initSql).not.toMatch(/"HALF_OPEN_SUCCESS_COUNT"\s+INTEGER\s+NOT NULL DEFAULT 0/i);
    expect(initSql).not.toMatch(/"PROBE_LEASE_UNTIL"/i);
  });

  it('defines download_authorize_breaker_attempt and removes download_claim_breaker_probe', () => {
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION download_authorize_breaker_attempt\(/i);
    expect(initSql).not.toMatch(/CREATE OR REPLACE FUNCTION download_claim_breaker_probe\(/i);
  });

  it('defines download_settle_breaker_attempt for non-probe terminal settlement', () => {
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION download_settle_breaker_attempt\(/i);
  });

  it('marks only the resolved mask in download_settle_breaker_attempt without modifying success bookkeeping or sample counters', () => {
    const functionBody = readFunctionBody('download_settle_breaker_attempt');

    expect(functionBody).toMatch(/v_half_open_resolved_mask := COALESCE\(v_half_open_resolved_mask, 0\) \| v_ticket_mask/i);
    expect(functionBody).toMatch(/"HALF_OPEN_RESOLVED_MASK" = v_half_open_resolved_mask/i);
    expect(functionBody).not.toMatch(/v_total_samples :=/i);
    expect(functionBody).not.toMatch(/v_half_open_success_mask := v_half_open_success_mask \|/i);
  });

  it('uses an explicit primary-key conflict target when seeding func_authorize_breaker_attempt', () => {
    const functionBody = readFunctionBody('func_authorize_breaker_attempt');

    expect(functionBody).toContain('ON CONFLICT ON CONSTRAINT "THROTTLE_PROTECTION_pkey" DO NOTHING');
    expect(functionBody).not.toContain('ON CONFLICT ("HOSTNAME_HASH") DO NOTHING');
  });

  it('uses an explicit primary-key conflict target when seeding download_report_breaker_sample', () => {
    const functionBody = readFunctionBody('download_report_breaker_sample');

    expect(functionBody).toContain('ON CONFLICT ON CONSTRAINT "THROTTLE_PROTECTION_pkey" DO NOTHING');
    expect(functionBody).not.toContain('ON CONFLICT ("HOSTNAME_HASH") DO NOTHING');
  });

  it('accepts warmup, half-open close, and timeout parameters in download_report_breaker_sample', () => {
    const functionBody = readFunctionBody('download_report_breaker_sample');

    expect(functionBody).toMatch(/p_min_samples_before_ewma_open\s+INTEGER/i);
    expect(functionBody).toMatch(/p_idle_reset_seconds\s+INTEGER/i);
    expect(functionBody).toMatch(/p_close_threshold_percent\s+INTEGER/i);
    expect(functionBody).toMatch(/p_half_open_success_threshold\s+INTEGER/i);
    expect(functionBody).toMatch(/p_half_open_close_mode\s+TEXT/i);
    expect(functionBody).toMatch(/p_half_open_max_seconds\s+INTEGER/i);
    expect(functionBody).toMatch(/p_half_open_timeout_mode\s+TEXT/i);
  });

  it('requires half-open attempt version and ticket in download_report_breaker_sample', () => {
    const functionBody = readFunctionBody('download_report_breaker_sample');

    expect(functionBody).toMatch(/p_attempt_version\s+BIGINT DEFAULT NULL/i);
    expect(functionBody).toMatch(/p_attempt_ticket\s+INTEGER DEFAULT NULL/i);
    expect(functionBody).not.toMatch(/p_probe_version\s+BIGINT DEFAULT NULL/i);
  });

  it('tracks half-open reports by resolved and success masks instead of legacy reported-count bookkeeping', () => {
    const functionBody = readFunctionBody('download_report_breaker_sample');

    expect(functionBody).toMatch(/v_half_open_ticket_mask_limit\s+CONSTANT\s+INTEGER\s*:=\s*63/i);
    expect(functionBody).toMatch(/IF v_has_attempt_version <> v_has_attempt_ticket THEN/i);
    expect(functionBody).toMatch(/IF v_state = 'half_open' THEN/i);
    expect(functionBody).toMatch(/IF p_attempt_version <> v_version THEN/i);
    expect(functionBody).toMatch(/IF p_attempt_ticket < 1 OR p_attempt_ticket > v_half_open_issued OR p_attempt_ticket > v_half_open_ticket_mask_limit THEN/i);
    expect(functionBody).toMatch(/v_ticket_mask := \(1::BIGINT << \(p_attempt_ticket - 1\)\)/i);
    expect(functionBody).toMatch(/IF \(v_half_open_resolved_mask & v_ticket_mask\) <> 0 THEN/i);
    expect(functionBody).toMatch(/v_half_open_resolved_mask := v_half_open_resolved_mask \| v_ticket_mask/i);
    expect(functionBody).toMatch(/v_half_open_success_mask := v_half_open_success_mask \| v_ticket_mask/i);
    expect(functionBody).not.toMatch(/v_probe_version_matches/i);
  });

  it('reopens exhausted half-open batches using resolved-mask pending debt and success-mask close evidence', () => {
    const functionBody = readFunctionBody('download_report_breaker_sample');

    expect(functionBody).toMatch(/v_issued_mask\s*:=\s*CASE\s+WHEN v_half_open_issued >= v_half_open_ticket_mask_limit THEN 9223372036854775807::BIGINT/i);
    expect(functionBody).toMatch(/v_pending_mask\s*:=\s*v_issued_mask\s*&\s*~v_half_open_resolved_mask/i);
    expect(functionBody).toMatch(/v_success_count\s*:=\s*bit_count\(v_half_open_success_mask::bit\(63\)\)::INTEGER/i);
    expect(functionBody).toMatch(/ELSIF v_half_open_issued = v_half_open_budget\s+AND v_half_open_issued > 0\s+AND v_pending_mask = 0 THEN[\s\S]*?v_timeout_open_seconds := CASE[\s\S]*?v_state := 'open';\s+v_open_until := v_now \+ v_timeout_open_seconds;\s+v_success_streak := 0;\s+v_last_open_seconds := v_timeout_open_seconds;/i);
    expect(functionBody).not.toMatch(/v_half_open_success_count/i);
    expect(functionBody).not.toMatch(/v_required_report_mask/i);
  });

  it('rejects oversized half-open budgets before authorize stores a ticket mask that cannot fit in BIGINT', () => {
    const functionBody = readFunctionBody('func_authorize_breaker_attempt');

    expect(functionBody).toMatch(/v_half_open_ticket_mask_limit\s+CONSTANT\s+INTEGER\s*:=\s*63/i);
    expect(functionBody).toMatch(/IF p_half_open_max_probe_count > v_half_open_ticket_mask_limit THEN\s+RAISE EXCEPTION 'func_authorize_breaker_attempt half-open max probe count exceeds BIGINT mask capacity: %', p_half_open_max_probe_count;/i);
  });

  it('accepts the full canonical authorize helper configuration in download_authorize_breaker_attempt without ATTEMPT_VERSION', () => {
    const functionBody = readFunctionBody('download_authorize_breaker_attempt');

    expect(functionBody).toMatch(/p_open_cap_seconds\s+INTEGER/i);
    expect(functionBody).toMatch(/p_close_threshold_percent\s+INTEGER/i);
    expect(functionBody).toMatch(/p_half_open_success_threshold\s+INTEGER/i);
    expect(functionBody).toMatch(/p_half_open_close_mode\s+TEXT/i);
    expect(functionBody).toMatch(/p_half_open_max_probe_count\s+INTEGER/i);
    expect(functionBody).toMatch(/p_half_open_max_seconds\s+INTEGER/i);
    expect(functionBody).toMatch(/p_half_open_timeout_mode\s+TEXT/i);
    expect(functionBody).toMatch(/"HALF_OPEN_DEADLINE"\s+INTEGER/i);
    expect(functionBody).toMatch(/"ATTEMPT_GRANTED"\s+BOOLEAN/i);
    expect(functionBody).toMatch(/"ATTEMPT_TICKET"\s+INTEGER/i);
    expect(functionBody).not.toMatch(/ATTEMPT_VERSION/i);
  });

  it('only transitions open to half_open when open_until is explicitly expired', () => {
    const functionBody = readFunctionBody('func_authorize_breaker_attempt');

    expect(functionBody).toMatch(/IF\s+v_state\s*=\s*'open'\s+AND\s+v_open_until\s+IS\s+NULL\s+THEN\s+RAISE EXCEPTION/i);
    expect(functionBody).toMatch(/IF\s+v_state\s*=\s*'open'\s+AND\s+v_open_until\s*<=\s*v_now\s+THEN/i);
  });

  it('applies half-open timeout mode only when half_open_deadline is explicitly reached', () => {
    const functionBody = readFunctionBody('func_authorize_breaker_attempt');

    expect(functionBody).toMatch(/IF\s+v_half_open_deadline\s*<=\s*v_now\s+THEN/i);
  });

  it('requires explicit breaker thresholds instead of SQL fallbacks', () => {
    const functionBody = readFunctionBody('download_report_breaker_sample');

    for (const requiredParam of [
      /p_open_cap_seconds\s+is\s+null/i,
      /p_open_threshold_percent\s+is\s+null/i,
      /p_ewma_span\s+is\s+null/i,
      /p_consecutive_threshold\s+is\s+null/i,
      /p_min_samples_before_ewma_open\s+is\s+null/i,
      /p_idle_reset_seconds\s+is\s+null/i,
      /p_close_threshold_percent\s+is\s+null/i,
      /p_half_open_success_threshold\s+is\s+null/i,
      /p_half_open_close_mode\s+is\s+null/i,
      /p_half_open_max_seconds\s+is\s+null/i,
      /p_half_open_timeout_mode\s+is\s+null/i,
    ]) {
      expect(functionBody).toMatch(requiredParam);
    }

    for (const legacyFallback of [
      /coalesce\(p_open_cap_seconds,\s*60\)/i,
      /coalesce\(p_open_threshold_percent,\s*20\)/i,
      /coalesce\(p_ewma_span,\s*8\)/i,
      /coalesce\(p_consecutive_threshold,\s*4\)/i,
      /coalesce\(p_min_samples_before_ewma_open,\s*1\)/i,
      /coalesce\(p_idle_reset_seconds,\s*0\)/i,
      /coalesce\(p_close_threshold_percent,\s*10\)/i,
      /coalesce\(p_half_open_success_threshold,\s*2\)/i,
      /coalesce\(p_half_open_close_mode,\s*'and'\)/i,
      /coalesce\(p_half_open_max_seconds,\s*0\)/i,
      /coalesce\(p_half_open_timeout_mode,\s*'partial-close'\)/i,
    ]) {
      expect(functionBody).not.toMatch(legacyFallback);
    }
  });

  it('requires explicit full canonical authorize inputs instead of SQL fallbacks', () => {
    const functionBody = readFunctionBody('func_authorize_breaker_attempt');

    for (const requiredParam of [
      /p_open_cap_seconds\s+is\s+null/i,
      /p_close_threshold_percent\s+is\s+null/i,
      /p_half_open_success_threshold\s+is\s+null/i,
      /p_half_open_close_mode\s+is\s+null/i,
      /p_half_open_max_probe_count\s+is\s+null/i,
      /p_half_open_max_seconds\s+is\s+null/i,
      /p_half_open_timeout_mode\s+is\s+null/i,
    ]) {
      expect(functionBody).toMatch(requiredParam);
    }

    for (const legacyFallback of [
      /coalesce\(p_open_cap_seconds,\s*60\)/i,
      /coalesce\(p_close_threshold_percent,\s*10\)/i,
      /coalesce\(p_half_open_success_threshold,\s*2\)/i,
      /coalesce\(p_half_open_close_mode,\s*'and'\)/i,
      /coalesce\(p_half_open_max_probe_count,\s*4\)/i,
      /coalesce\(p_half_open_max_seconds,\s*0\)/i,
      /coalesce\(p_half_open_timeout_mode,\s*'partial-close'\)/i,
    ]) {
      expect(functionBody).not.toMatch(legacyFallback);
    }

    expect(functionBody).not.toMatch(/p_probe_lease_seconds/i);
  });

  it('defines one canonical authorize helper shared by download_authorize_breaker_attempt and fq_admit_batch', () => {
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION func_authorize_breaker_attempt\(/i);
    const authorizeBody = readFunctionBody('download_authorize_breaker_attempt');
    const admitBody = readFunctionBody('fq_admit_batch');

    expect(authorizeBody).toMatch(/FROM\s+func_authorize_breaker_attempt\s*\(/i);
    expect(admitBody).toMatch(/FROM\s+func_authorize_breaker_attempt\s*\(/i);
    expect(admitBody).toMatch(/status\s*:=\s*'HALF_OPEN_FULL'/i);
  });

  it('uses the canonical authorize helper before any THROTTLED return in fq_admit_batch', () => {
    const functionBody = readFunctionBody('fq_admit_batch');
    const helperIndex = expectPatternIndex(
      functionBody,
      /FROM\s+func_authorize_breaker_attempt\s*\(/i,
      'expected fq_admit_batch to call func_authorize_breaker_attempt',
    );
    const throttledIndex = expectPatternIndex(
      functionBody,
      /status\s*:=\s*'THROTTLED'/i,
      'expected fq_admit_batch to expose THROTTLED responses',
    );

    expect(
      throttledIndex,
      'expected fq_admit_batch to reach the canonical helper before assigning THROTTLED',
    ).toBeGreaterThan(helperIndex);
    expect(functionBody).not.toMatch(
      /SELECT\s+"STATE",\s+"OPEN_UNTIL",\s+"OPEN_REASON",\s+"VERSION",\s+"LAST_ERROR_CODE"[\s\S]*?FROM\s+"THROTTLE_PROTECTION"[\s\S]*?v_throttled\s*:=/i,
    );
    expect(functionBody).not.toMatch(/IF\s+v_throttled\s+THEN[\s\S]*?status\s*:=\s*'THROTTLED'/i);
  });

  it('replaces fq_try_acquire_batch with fq_admit_batch', () => {
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION fq_admit_batch\(/i);
    expect(initSql).not.toMatch(/CREATE OR REPLACE FUNCTION fq_try_acquire_batch\(/i);
  });

  it('defines fq_admit_batch with the full canonical breaker gate inputs', () => {
    const functionBody = readFunctionBody('fq_admit_batch');

    expect(functionBody).toMatch(/p_breaker_enabled\s+BOOLEAN/i);
    expect(functionBody).toMatch(/p_open_cap_seconds\s+INT/i);
    expect(functionBody).toMatch(/p_close_threshold_percent\s+INT/i);
    expect(functionBody).toMatch(/p_half_open_success_threshold\s+INT/i);
    expect(functionBody).toMatch(/p_half_open_close_mode\s+TEXT/i);
    expect(functionBody).toMatch(/p_half_open_max_probe_count\s+INT/i);
    expect(functionBody).toMatch(/p_half_open_max_seconds\s+INT/i);
    expect(functionBody).toMatch(/p_half_open_timeout_mode\s+TEXT/i);
  });

  it('returns retry_after and attempt fields from fq_admit_batch', () => {
    const functionBody = readFunctionBody('fq_admit_batch');

    expect(functionBody).toMatch(/retry_after\s+INT/i);
    expect(functionBody).toMatch(/attempt_version\s+BIGINT/i);
    expect(functionBody).toMatch(/attempt_ticket\s+INT/i);
    expect(functionBody.match(/\bslot_token\s+TEXT\b/gi) ?? []).toHaveLength(1);
  });

  it('returns explicit IP_TOO_MANY with null non-applicable fields when host slot acquisition hits per-IP structure limits', () => {
    const functionBody = readFunctionBody('fq_admit_batch');

    expect(functionBody).toMatch(/if\s+v_host_slot_id\s*=\s*0\s+then[\s\S]*?status\s*:=\s*'IP_TOO_MANY'[\s\S]*?slot_token\s*:=\s*NULL[\s\S]*?retry_after\s*:=\s*NULL[\s\S]*?attempt_version\s*:=\s*NULL[\s\S]*?attempt_ticket\s*:=\s*NULL[\s\S]*?return next;[\s\S]*?continue;[\s\S]*?elsif\s+v_host_slot_id\s*<\s*0\s+then/i);
  });

  it('releases the host slot before returning site-side IP_TOO_MANY with null non-applicable fields', () => {
    const functionBody = readFunctionBody('fq_admit_batch');

    expect(functionBody).toMatch(/if\s+v_site_slot_id\s*=\s*0\s+then[\s\S]*?PERFORM\s+func_release_host_slot\(v_host_slot_id,\s*FALSE\);[\s\S]*?status\s*:=\s*'IP_TOO_MANY'[\s\S]*?slot_token\s*:=\s*NULL[\s\S]*?retry_after\s*:=\s*NULL[\s\S]*?attempt_version\s*:=\s*NULL[\s\S]*?attempt_ticket\s*:=\s*NULL[\s\S]*?return next;[\s\S]*?continue;[\s\S]*?elsif\s+v_site_slot_id\s*<\s*0\s+then/i);
  });

  it('defines true-concurrency request-ledger tables and rpc entrypoints', () => {
    expect(initSql).toMatch(/CREATE TABLE IF NOT EXISTS\s+concurrency_leases/i);
    expect(initSql).toMatch(/CREATE TABLE IF NOT EXISTS\s+concurrency_requests/i);
    expect(initSql).toMatch(/CREATE TABLE IF NOT EXISTS\s+concurrency_wait_tokens/i);
    expect(initSql).toMatch(/waiter_lease_until_ms\s+bigint\s+NOT NULL/i);
    expect(initSql).toMatch(/CREATE TABLE IF NOT EXISTS\s+concurrency_host_counters/i);
    expect(initSql).toMatch(/CREATE TABLE IF NOT EXISTS\s+concurrency_site_counters/i);
    expect(initSql).toMatch(/CREATE TABLE IF NOT EXISTS\s+concurrency_site_ip_counters/i);
    expect(initSql).toMatch(/CREATE UNIQUE INDEX IF NOT EXISTS\s+concurrency_leases_request_id_idx\s+ON\s+concurrency_leases\s*\(request_id\)/i);
    expect(initSql).toMatch(/CREATE INDEX IF NOT EXISTS\s+concurrency_requests_waiting_host_idx/i);
    expect(initSql).not.toMatch(/CREATE OR REPLACE FUNCTION\s+cq_precheck\(/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_expire_scope\(/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_release\(/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_claim_grant\(/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_ack_handoff\(/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_terminalize_waiting\(/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_promote_waiting_request\(/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_acquire\(/i);
    expect(initSql).not.toMatch(/CREATE OR REPLACE FUNCTION\s+cq_release_by_request\(/i);
  });

  it('defines request-ledger handoff metadata for the active handoff sub-phase', () => {
    expect(initSql).toMatch(/handoff_state\s+text\s+CHECK\s*\(handoff_state\s+IN\s*\('none',\s*'pending',\s*'acknowledged',\s*'compensated'\)\)/i);
    expect(initSql).toMatch(/handoff_token\s+text/i);
    expect(initSql).toMatch(/handoff_deadline_ms\s+bigint/i);
    expect(initSql).toMatch(/handoff_acked_at_ms\s+bigint/i);
  });

  it('keeps handoff schema setup fresh-only without compatibility alter/backfill DDL', () => {
    expect(initSql).not.toMatch(/ALTER TABLE\s+concurrency_requests\s+ADD COLUMN IF NOT EXISTS\s+handoff_state/i);
    expect(initSql).not.toMatch(/ALTER TABLE\s+concurrency_requests\s+ADD COLUMN IF NOT EXISTS\s+handoff_token/i);
    expect(initSql).not.toMatch(/ALTER TABLE\s+concurrency_requests\s+ADD COLUMN IF NOT EXISTS\s+handoff_deadline_ms/i);
    expect(initSql).not.toMatch(/ALTER TABLE\s+concurrency_requests\s+ADD COLUMN IF NOT EXISTS\s+handoff_acked_at_ms/i);
    expect(initSql).not.toMatch(/UPDATE\s+concurrency_requests\s+SET\s+handoff_state\s*=\s*'none'\s+WHERE\s+handoff_state\s+IS\s+NULL/i);
    expect(initSql).not.toMatch(/ALTER TABLE\s+concurrency_requests\s+ALTER COLUMN\s+handoff_state\s+SET\s+DEFAULT\s+'none'/i);
    expect(initSql).not.toMatch(/ALTER TABLE\s+concurrency_requests\s+ALTER COLUMN\s+handoff_state\s+SET\s+NOT\s+NULL/i);
  });

  it('encodes provisional fast-only waiting acquisition in cq_acquire', () => {
    const acquireBody = readFunctionBody('cq_acquire');

    expect(acquireBody).toMatch(/v_now_ms\s+bigint := COALESCE\(p_now_ms, \(EXTRACT\(EPOCH FROM clock_timestamp\(\)\) \* 1000\)::bigint\)/i);
    expect(acquireBody).not.toMatch(/WHERE concurrency_requests\.wait_token = v_wait_token_input/i);
    expect(acquireBody).not.toMatch(/RAISE EXCEPTION 'cq_acquire stale wait token'/i);
    expect(acquireBody).toMatch(/INSERT INTO concurrency_wait_tokens[\s\S]*?wait_token,[\s\S]*?request_id,[\s\S]*?scope,[\s\S]*?retry_after/i);
    expect(acquireBody).not.toMatch(/INSERT INTO concurrency_requests[\s\S]*?state,[\s\S]*?'waiting'[\s\S]*?wait_token,[\s\S]*?waiter_lease_until_ms/i);
  });

  it('removes legacy wait-state SQL surface from the CQ acquire path', () => {
    expect(initSql).not.toMatch(/CREATE OR REPLACE FUNCTION\s+cq_continue_wait_probe\(/i);
    expect(initSql).not.toMatch(/p_wait_poll_window_ms/i);
    expect(initSql).not.toMatch(/p_wait_reconnect_grace_ms/i);
    expect(initSql).not.toMatch(/cq_acquire\s*\([^)]*p_wait_token/s);
  });

  it('defines deadline-driven cq_wait_state_probe for SSE wait streams', () => {
    const probeBody = readFunctionBody('cq_wait_state_probe');

    expect(probeBody).toMatch(/p_deadline_ms\s+bigint/i);
    expect(probeBody).toMatch(/v_effective_deadline_ms\s+bigint/i);
    expect(probeBody).toMatch(/waiter_lease_until_ms\s*=\s*v_effective_deadline_ms/i);
    expect(probeBody).toMatch(/terminal_reason\s*=\s*'wait_stream_timeout'/i);
    expect(probeBody).toMatch(/reason\s*:=\s*'wait_stream_timeout'/i);
  });

  it('documents request-ledger replay, waiting terminalization, and release idempotency', () => {
    const acquireBody = readFunctionBody('cq_acquire');
    const terminalizeBody = readFunctionBody('cq_terminalize_waiting');
    const releaseBody = readFunctionBody('cq_release');
    const expireBody = readFunctionBody('cq_expire_scope');
    const expireActiveBody = readFunctionBody('cq_expire_active_request_if_due');
    const claimBody = readFunctionBody('cq_claim_grant');
    const ackBody = readFunctionBody('cq_ack_handoff');

    expect(acquireBody).toMatch(/request_id/i);
    expect(acquireBody).toMatch(/wait_token/i);
    expect(acquireBody).toMatch(/waiter_lease_until_ms/i);
    expect(acquireBody).toMatch(/FOR UPDATE/i);
    expect(acquireBody).toMatch(/tuple/i);
    expect(acquireBody).toMatch(/RAISE EXCEPTION 'cq_acquire request_id tuple mismatch'/i);
    expect(acquireBody).toMatch(/WHEN 'waiting' THEN[\s\S]*?terminal_reason = 'wait_stream_timeout'[\s\S]*?result := 'wait'/i);
    expect(acquireBody).toMatch(/WHEN 'active' THEN[\s\S]*?result := 'expired'[\s\S]*?result := 'conflict'/i);
    expect(acquireBody).toMatch(/WHEN 'released' THEN[\s\S]*?result := 'released'/i);
    expect(acquireBody).toMatch(/WHEN 'cancelled' THEN[\s\S]*?result := 'cancelled'/i);
    expect(acquireBody).toMatch(/WHEN 'expired' THEN[\s\S]*?result := 'expired'/i);
    expect(acquireBody).toMatch(/IF p_hard_expire_at_ms IS NULL OR p_hard_expire_at_ms <= v_now_ms THEN[\s\S]*?result := 'expired'/i);

    expect(claimBody).toMatch(/WHEN 'active' THEN[\s\S]*?result := 'expired'[\s\S]*?v_request\.claim_token IS DISTINCT FROM v_claim_token[\s\S]*?result := 'conflict'[\s\S]*?result := 'granted'/i);
    expect(claimBody).toMatch(/claim_state = 'claimed'/i);
    expect(claimBody).toMatch(/handoff_state = 'pending'/i);
    expect(claimBody).toMatch(/handoff_token/i);
    expect(claimBody).toMatch(/handoff_deadline_ms/i);

    expect(terminalizeBody).toMatch(/p_hostname\s+text/i);
    expect(terminalizeBody).toMatch(/p_hostname_hash\s+text/i);
    expect(terminalizeBody).toMatch(/p_site_bucket\s+text/i);
    expect(terminalizeBody).toMatch(/p_ip_bucket\s+text/i);
    expect(terminalizeBody).toMatch(/v_hostname\s+text := BTRIM\(COALESCE\(p_hostname, ''\)\)/i);
    expect(terminalizeBody).toMatch(/v_site_bucket\s+text := COALESCE\(NULLIF\(BTRIM\(COALESCE\(p_site_bucket, ''\)\), ''\), 'unknown'\)/i);
    expect(terminalizeBody).toMatch(/v_request\.hostname IS DISTINCT FROM v_hostname/i);
    expect(terminalizeBody).toMatch(/v_request\.hard_expire_at_ms IS DISTINCT FROM p_hard_expire_at_ms/i);
    expect(terminalizeBody).toMatch(/RAISE EXCEPTION 'cq_terminalize_waiting request_id tuple mismatch'/i);
    expect(terminalizeBody).toMatch(/RAISE EXCEPTION 'cq_terminalize_waiting must release active lease'/i);
    expect(terminalizeBody).toMatch(/IF v_request\.state = 'waiting' THEN[\s\S]*?state = 'released'[\s\S]*?terminal_reason = v_terminal_reason[\s\S]*?result := 'released'/i);
    expect(terminalizeBody).toMatch(/result := 'noop';[\s\S]*?reason := 'already_terminal'/i);

    expect(releaseBody).toMatch(/lease_token/i);
    expect(releaseBody).toMatch(/state\s*=\s*'released'/i);
    expect(releaseBody).toMatch(/FOR UPDATE/i);
    expect(releaseBody).toMatch(/result\s*:=\s*'released'/i);
    expect(releaseBody).toMatch(/pg_advisory_xact_lock\(3, hashtext\(v_request_id\)\)/i);
    expect(releaseBody).toMatch(/cq_apply_request_terminal_transition\(/i);

    expect(expireBody).toMatch(/pg_try_advisory_xact_lock\(3, hashtext\(v_row\.request_id\)\)/i);
    expect(expireBody).toMatch(/cq_expire_active_request_if_due\(v_row\.request_id, v_now_ms\)/i);
    expect(expireActiveBody).toMatch(/FROM concurrency_requests[\s\S]*?WHERE request_id = p_request_id[\s\S]*?FOR UPDATE/i);
    expect(expireActiveBody).toMatch(/FROM concurrency_leases[\s\S]*?WHERE request_id = p_request_id[\s\S]*?AND state = 'active'[\s\S]*?FOR UPDATE/i);
    expect(expireActiveBody).toMatch(/cq_apply_request_terminal_transition\(p_request_id, 'expired', 'hard_expired', p_now_ms\)/i);

    expect(ackBody).toMatch(/handoff_token_mismatch/i);
    expect(ackBody).toMatch(/handoff_state\s*=\s*'acknowledged'/i);
    expect(ackBody).toMatch(/v_commit_now_ms\s+bigint\s*:=\s*\(EXTRACT\(EPOCH FROM clock_timestamp\(\)\) \* 1000\)::bigint/i);
    expect(ackBody).toMatch(/handoff_acked_at_ms\s*=\s*v_commit_now_ms/i);
  });

  it('represents handoff-timeout compensation in claim replay, ack_handoff, and recovery SQL paths', () => {
    const claimBody = readFunctionBody('cq_claim_grant');
    const ackBody = readFunctionBody('cq_ack_handoff');
    const expireBody = readFunctionBody('cq_expire_scope');
    const expireActiveBody = readFunctionBody('cq_expire_active_request_if_due');

    expect(initSql).toMatch(/claim_handoff_timeout/i);
    expect(claimBody).toMatch(/handoff_state\s*=\s*'pending'[\s\S]*?handoff_deadline_ms\s*<=\s*v_now_ms[\s\S]*?claim_handoff_timeout/i);
    expect(ackBody).toMatch(/handoff_deadline_ms\s*<=\s*v_now_ms[\s\S]*?claim_handoff_timeout/i);
    expect(expireBody).toMatch(/handoff_deadline_ms/i);
    expect(expireActiveBody).toMatch(/handoff_state\s*=\s*'pending'[\s\S]*?handoff_deadline_ms\s*<=\s*p_now_ms[\s\S]*?claim_handoff_timeout/i);
  });

  it('uses executable plpgsql lock statements for true-concurrency counters', () => {
    const acquireBody = readFunctionBody('cq_acquire');

    expect(acquireBody).toMatch(/PERFORM\s+1\s+FROM\s+concurrency_host_counters\s+WHERE hostname_hash = v_hostname_hash\s+FOR UPDATE;/i);
    expect(acquireBody).toMatch(/PERFORM\s+1\s+FROM\s+concurrency_site_counters\s+WHERE hostname_hash = v_hostname_hash AND site_bucket = v_site_bucket\s+FOR UPDATE;/i);
    expect(acquireBody).toMatch(/PERFORM\s+1\s+FROM\s+concurrency_site_ip_counters\s+WHERE hostname_hash = v_hostname_hash AND site_bucket = v_site_bucket AND ip_bucket = v_ip_bucket\s+FOR UPDATE;/i);
    expect(acquireBody).not.toMatch(/SELECT\s+1\s+FROM\s+concurrency_host_counters\s+WHERE hostname_hash = v_hostname_hash\s+FOR UPDATE;/i);
    expect(acquireBody).not.toMatch(/SELECT\s+1\s+FROM\s+concurrency_site_counters\s+WHERE hostname_hash = v_hostname_hash AND site_bucket = v_site_bucket\s+FOR UPDATE;/i);
    expect(acquireBody).not.toMatch(/SELECT\s+1\s+FROM\s+concurrency_site_ip_counters\s+WHERE hostname_hash = v_hostname_hash AND site_bucket = v_site_bucket AND ip_bucket = v_ip_bucket\s+FOR UPDATE;/i);
  });

  it('serializes request_id before request-ledger lookup and tuple-scoped counter locks', () => {
    const acquireBody = readFunctionBody('cq_acquire');
    const requestLockIndex = acquireBody.search(/pg_advisory_xact_lock\(\s*3\s*,\s*hashtext\(v_request_id\)\s*\)/i);
    const hostLockIndex = acquireBody.search(/PERFORM\s+1\s+FROM\s+concurrency_host_counters\s+WHERE hostname_hash = v_hostname_hash\s+FOR UPDATE;/i);
    const requestRowIndex = acquireBody.search(/FROM\s+concurrency_requests\s+WHERE request_id = v_request_id\s+FOR UPDATE/i);

    expect(requestLockIndex).toBeGreaterThan(-1);
    expect(hostLockIndex).toBeGreaterThan(requestLockIndex);
    expect(requestRowIndex).toBeGreaterThan(requestLockIndex);
  });

  it('serializes request-scoped terminal lifecycle mutations through request_id helper locking', () => {
    const helperBody = readFunctionBody('cq_apply_request_terminal_transition');
    const releaseBody = readFunctionBody('cq_release');
    const claimBody = readFunctionBody('cq_claim_grant');
    const promoteBody = readFunctionBody('cq_promote_waiting_request');
    const probeBody = readFunctionBody('cq_wait_state_probe');

    expect(helperBody).toMatch(/FROM concurrency_requests[\s\S]*?WHERE request_id = p_request_id[\s\S]*?FOR UPDATE/i);
    expect(helperBody).toMatch(/FROM concurrency_leases[\s\S]*?WHERE request_id = p_request_id[\s\S]*?AND state = 'active'[\s\S]*?FOR UPDATE/i);
    expect(helperBody).toMatch(/UPDATE concurrency_requests[\s\S]*?state = v_terminal_state/i);
    expect(helperBody).toMatch(/UPDATE concurrency_leases[\s\S]*?state = v_terminal_state/i);
    expect(helperBody).toMatch(/active_count\s*=\s*GREATEST\(active_count\s*-\s*1,\s*0\)/i);

    expect(releaseBody).toMatch(/SELECT\s+concurrency_leases\.request_id[\s\S]*?INTO\s+v_request_id[\s\S]*?FROM\s+concurrency_leases[\s\S]*?WHERE lease_id = p_lease_id;/i);
    expect(releaseBody).toMatch(/pg_advisory_xact_lock\(3, hashtext\(v_request_id\)\)/i);
    expect(releaseBody).not.toMatch(/PERFORM\s+cq_expire_scope\(\s*'host'/i);

    expect(claimBody).toMatch(/cq_apply_request_terminal_transition\(v_request_id, 'expired', 'hard_expired', v_now_ms\)/i);
    expect(promoteBody).toMatch(/cq_apply_request_terminal_transition\(v_request_id, 'expired', 'hard_expired', v_now_ms\)/i);
    expect(probeBody).toMatch(/cq_apply_request_terminal_transition\(v_request_id, 'expired', 'hard_expired', v_now_ms\)/i);
  });

  it('gives cq_release expiry precedence before active token mismatch handling', () => {
    const releaseBody = readFunctionBody('cq_release');
    const expiryIndex = expectPatternIndex(
      releaseBody,
      /IF v_request\.hard_expire_at_ms <= v_now_ms[\s\S]*?result := 'expired'/i,
      'expected cq_release to evaluate authoritative expiry before conflict handling',
    );
    const tokenMismatchIndex = expectPatternIndex(
      releaseBody,
      /IF v_locked_lease\.lease_token IS DISTINCT FROM p_lease_token THEN[\s\S]*?reason := 'token_mismatch'/i,
      'expected cq_release to retain wrong-token conflict handling for still-active leases',
    );

    expect(tokenMismatchIndex).toBeGreaterThan(expiryIndex);
  });

  it('routes cq_expire_scope through request-centric advisory locking without lease-first or counter-first blocking', () => {
    const expireBody = readFunctionBody('cq_expire_scope');
    const expiredRowsIndex = expectPatternIndex(
      expireBody,
      /WITH\s+expired_rows\s+AS\s*\(/i,
      'expected cq_expire_scope to enumerate candidate expired requests',
    );
    const requestTryLockIndex = expectPatternIndex(
      expireBody,
      /pg_try_advisory_xact_lock\(3, hashtext\(v_row\.request_id\)\)/i,
      'expected cq_expire_scope to enter the request advisory domain before expiring a request',
    );

    expect(requestTryLockIndex).toBeGreaterThan(expiredRowsIndex);
    expect(expireBody).not.toMatch(/FROM\s+concurrency_leases[\s\S]*?FOR UPDATE SKIP LOCKED/i);
    expect(expireBody).not.toMatch(/FROM\s+concurrency_host_counters[\s\S]*?FOR UPDATE/i);
    expect(expireBody).not.toMatch(/FROM\s+concurrency_site_counters[\s\S]*?FOR UPDATE/i);
    expect(expireBody).not.toMatch(/FROM\s+concurrency_site_ip_counters[\s\S]*?FOR UPDATE/i);
  });

  it('locks cq_release through request scope before lease mutation', () => {
    const releaseBody = readFunctionBody('cq_release');
    const releaseScopeLookupIndex = expectPatternIndex(
      releaseBody,
      /SELECT\s+concurrency_leases\.request_id\s+INTO\s+v_request_id\s+FROM\s+concurrency_leases\s+WHERE lease_id = p_lease_id;/i,
      'expected cq_release to resolve authoritative request id before lifecycle locking',
    );
    const requestLockIndex = expectPatternIndex(
      releaseBody,
      /pg_advisory_xact_lock\(3, hashtext\(v_request_id\)\)/i,
      'expected cq_release to enter request advisory lock domain',
    );
    const requestRowIndex = expectPatternIndex(
      releaseBody,
      /FROM\s+concurrency_requests\s+WHERE\s+concurrency_requests\.request_id = v_request_id\s+FOR UPDATE/i,
      'expected cq_release to lock authoritative request row after request advisory lock',
    );
    const leaseLockIndex = expectPatternIndex(
      releaseBody,
      /SELECT\s+\*\s+INTO\s+v_locked_lease\s+FROM\s+concurrency_leases\s+WHERE lease_id = p_lease_id\s+FOR UPDATE;/i,
      'expected cq_release to lock the addressed lease after request-scoped lifecycle locking begins',
    );

    expect(requestLockIndex).toBeGreaterThan(releaseScopeLookupIndex);
    expect(requestRowIndex).toBeGreaterThan(requestLockIndex);
    expect(leaseLockIndex).toBeGreaterThan(requestLockIndex);
  });

  it('resolves authoritative wait-token request ids before entering request advisory locking in wait-state probe paths', () => {
    const probeBody = readFunctionBody('cq_wait_state_probe');

    const probeResolveIndex = expectPatternIndex(
      probeBody,
      /SELECT\s+concurrency_requests\.request_id\s+INTO\s+v_authoritative_request_id\s+FROM\s+concurrency_requests\s+WHERE concurrency_requests\.wait_token = v_wait_token_input/i,
      'expected cq_wait_state_probe to resolve the authoritative request id from wait_token before advisory locking',
    );
    const probeLockIndex = expectPatternIndex(
      probeBody,
      /pg_advisory_xact_lock\(3, hashtext\(v_authoritative_request_id\)\)/i,
      'expected cq_wait_state_probe to enter request advisory locking with the authoritative request id',
    );
    const probeRequestRowIndex = expectPatternIndex(
      probeBody,
      /FROM\s+concurrency_requests\s+WHERE request_id = v_authoritative_request_id\s+FOR UPDATE/i,
      'expected cq_wait_state_probe to lock the authoritative request row after entering advisory locking',
    );

    expect(probeLockIndex).toBeGreaterThan(probeResolveIndex);
    expect(probeRequestRowIndex).toBeGreaterThan(probeLockIndex);
    expect(probeBody).not.toMatch(/pg_advisory_xact_lock\(3, hashtext\(v_request_id\)\)/i);
  });
});

describe('init.sql ticket-state contract definitions', () => {
  it('defines the dual-policy ticket-state table with renewal ownership fields and hard-expiry index', () => {
    expect(initSql).toMatch(/CREATE TABLE IF NOT EXISTS "DOWNLOAD_TICKET_STATE_TABLE"/i);
    expect(initSql).toMatch(/"TICKET_HASH"\s+TEXT\s+NOT NULL/i);
    expect(initSql).toMatch(/"ISSUED_AT"\s+BIGINT\s+NOT NULL/i);
    expect(initSql).toMatch(/"HARD_EXPIRE_AT"\s+BIGINT\s+NOT NULL/i);
    expect(initSql).toMatch(/"IDLE_TIMEOUT_SECONDS"\s+INTEGER\s+NOT NULL/i);
    expect(initSql).toMatch(/"FIRST_USED_AT"\s+BIGINT\s+NULL/i);
    expect(initSql).toMatch(/"IDLE_POLICY"\s+TEXT\s+NOT NULL\s+CHECK\s*\("IDLE_POLICY"\s+IN\s*\('first_use',\s*'renewable'\)\)/i);
    expect(initSql).toMatch(/"IDLE_LEASE_EXPIRES_AT"\s+BIGINT\s+NOT NULL/i);
    expect(initSql).toMatch(/"IDLE_RENEW_OWNER_LEASE_ID"\s+UUID\s+NULL/i);
    expect(initSql).toMatch(/"IDLE_RENEW_OWNER_LAST_HEARTBEAT_AT"\s+BIGINT\s+NULL/i);
    expect(initSql).toMatch(/"IP_HASH"\s+TEXT\s+NULL/i);
    expect(initSql).toMatch(/"PATH_HASH"\s+TEXT\s+NULL/i);
    expect(initSql).toMatch(/PRIMARY KEY \("TICKET_HASH"\)/i);
    expect(initSql).toMatch(/CREATE INDEX IF NOT EXISTS idx_download_ticket_state_hard_expire\s+ON "DOWNLOAD_TICKET_STATE_TABLE"\("HARD_EXPIRE_AT"\)/i);
  });

  it('removes the legacy last-active table and rpc surface', () => {
    expect(initSql).not.toMatch(/CREATE TABLE IF NOT EXISTS "DOWNLOAD_LAST_ACTIVE_TABLE"/i);
    expect(initSql).not.toMatch(/CREATE OR REPLACE FUNCTION download_update_last_active\(/i);
  });

  it('defines an insert-only seed rpc that seeds first_use policy and renewal defaults', () => {
    const functionBody = readFunctionBody('download_seed_ticket');

    expect(functionBody).toMatch(/p_ticket_hash\s+TEXT/i);
    expect(functionBody).toMatch(/p_issued_at\s+BIGINT/i);
    expect(functionBody).toMatch(/p_hard_expire_at\s+BIGINT/i);
    expect(functionBody).toMatch(/p_idle_timeout_seconds\s+INTEGER/i);
    expect(functionBody).toMatch(/p_ip_hash\s+TEXT DEFAULT NULL/i);
    expect(functionBody).toMatch(/p_path_hash\s+TEXT DEFAULT NULL/i);
    expect(functionBody).toMatch(/p_table_name\s+TEXT DEFAULT 'DOWNLOAD_TICKET_STATE_TABLE'/i);
    expect(functionBody).toMatch(/INSERT INTO %1\$I \("TICKET_HASH", "ISSUED_AT", "FIRST_USED_AT", "HARD_EXPIRE_AT", "IDLE_TIMEOUT_SECONDS", "IDLE_POLICY", "IDLE_LEASE_EXPIRES_AT", "IDLE_RENEW_OWNER_LEASE_ID", "IDLE_RENEW_OWNER_LAST_HEARTBEAT_AT", "IP_HASH", "PATH_HASH"\)/i);
    expect(functionBody).toMatch(/VALUES \(\$1, \$2, NULL, \$3, \$4, ''first_use'', \$2 \+ \$4, NULL, NULL, \$5, \$6\)/i);
    expect(functionBody).toMatch(/RETURN json_build_object\('result', 'seeded'\)/i);
    expect(functionBody).toMatch(/WHEN unique_violation THEN\s+RETURN json_build_object\('result', 'collision'\)/i);
    expect(functionBody).toMatch(/WHEN others THEN\s+RETURN json_build_object\('result', 'storage_error', 'error', SQLERRM\)/i);
    expect(functionBody).not.toMatch(/ON CONFLICT/i);
  });

  it('defines a ticket-state read rpc with dual-policy lifecycle and renewal ownership fields', () => {
    const functionBody = readFunctionBody('download_get_ticket_state');

    expect(functionBody).toMatch(/p_ticket_hash\s+TEXT/i);
    expect(functionBody).toMatch(/p_table_name\s+TEXT DEFAULT 'DOWNLOAD_TICKET_STATE_TABLE'/i);
    expect(functionBody).toMatch(/RETURNS TABLE\s*\(\s*found BOOLEAN,\s*ticket_hash TEXT,\s*issued_at BIGINT,\s*first_used_at BIGINT,\s*hard_expire_at BIGINT,\s*idle_timeout_seconds INTEGER,\s*idle_policy TEXT,\s*idle_lease_expires_at BIGINT,\s*idle_renew_owner_lease_id UUID,\s*idle_renew_owner_last_heartbeat_at BIGINT,\s*ip_hash TEXT,\s*path_hash TEXT\s*\)/i);
  });

  it('defines a mark-used rpc that only updates first-use state', () => {
    const functionBody = readFunctionBody('download_mark_ticket_used');

    expect(functionBody).toMatch(/p_ticket_hash\s+TEXT/i);
    expect(functionBody).toMatch(/p_now\s+BIGINT/i);
    expect(functionBody).toMatch(/p_table_name\s+TEXT DEFAULT 'DOWNLOAD_TICKET_STATE_TABLE'/i);
    expect(functionBody).toMatch(/FOR UPDATE/i);
    expect(functionBody).toMatch(/COALESCE\("FIRST_USED_AT", \$2\)/i);
    expect(functionBody).toMatch(/RETURN json_build_object\('result', 'transitioned', 'first_used_at', v_effective_first_used_at\)/i);
    expect(functionBody).toMatch(/RETURN json_build_object\('result', 'already_used', 'first_used_at', v_effective_first_used_at\)/i);
    expect(functionBody).toMatch(/RETURN json_build_object\('result', 'storage_error'\)/i);
    expect(functionBody).not.toMatch(/SET[\s\S]*?"IDLE_POLICY"/i);
    expect(functionBody).not.toMatch(/SET[\s\S]*?"IDLE_LEASE_EXPIRES_AT"/i);
    expect(functionBody).not.toMatch(/SET[\s\S]*?"IDLE_RENEW_OWNER_LEASE_ID"/i);
    expect(functionBody).not.toMatch(/SET[\s\S]*?"IDLE_RENEW_OWNER_LAST_HEARTBEAT_AT"/i);
  });

  it('defines ticket cleanup by hard expiry and removes legacy last-active outputs from download_unified_check', () => {
    const cleanupBody = readFunctionBody('download_cleanup_expired_tickets');
    const unifiedCheckBody = readFunctionBody('download_unified_check');

    expect(cleanupBody).toMatch(/DELETE FROM %1\$I WHERE "HARD_EXPIRE_AT" < \$1/i);
    expect(cleanupBody).toMatch(/RETURN json_build_object\('deleted', v_deleted_count\)/i);

    expect(unifiedCheckBody).not.toMatch(/p_idle_timeout\s+INTEGER/i);
    expect(unifiedCheckBody).not.toMatch(/p_last_active_table_name\s+TEXT/i);
    expect(unifiedCheckBody).not.toMatch(/active_last_access_time\s+INTEGER/i);
    expect(unifiedCheckBody).not.toMatch(/active_total_access_count\s+INTEGER/i);
    expect(unifiedCheckBody).not.toMatch(/LAST_ACCESS_TIME/i);
    expect(unifiedCheckBody).not.toMatch(/TOTAL_ACCESS_COUNT/i);
  });
});

describe('init.sql heartbeat contract definitions', () => {
  it('defines request heartbeat columns, state default, and deadline index', () => {
    expect(initSql).toMatch(/heartbeat_state\s+text\s+NOT NULL\s+DEFAULT 'none'/i);
    expect(initSql).toMatch(/heartbeat_generation\s+bigint\s+NOT NULL\s+DEFAULT 0/i);
    expect(initSql).toMatch(/heartbeat_last_at_ms\s+bigint/i);
    expect(initSql).toMatch(/heartbeat_deadline_ms\s+bigint/i);
    expect(initSql).toMatch(/heartbeat_grace_until_ms\s+bigint/i);
    expect(initSql).toMatch(/heartbeat_connected_at_ms\s+bigint/i);
    expect(initSql).toMatch(/heartbeat_disconnected_at_ms\s+bigint/i);
    expect(initSql).toMatch(/heartbeat_terminal_reason\s+text/i);
    expect(initSql).toMatch(/CREATE INDEX IF NOT EXISTS\s+concurrency_requests_heartbeat_deadline_idx/i);
  });

  it('keeps the heartbeat cleanup trigger as a current-state declaration without migrate-only replacement DDL', () => {
    expect(initSql).toMatch(/CREATE TRIGGER\s+cq_concurrency_requests_heartbeat_cleanup/i);
    expect(initSql).toMatch(/EXECUTE FUNCTION\s+cq_apply_heartbeat_terminal_cleanup_trigger\(\)/i);
    expect(initSql).not.toMatch(/DROP\s+TRIGGER\s+IF\s+EXISTS\s+cq_concurrency_requests_heartbeat_cleanup\s+ON\s+concurrency_requests/i);
  });

  it('defines heartbeat rpc entrypoints and ack_handoff start timeout contract', () => {
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_heartbeat_open\(/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_heartbeat_refresh\(/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_heartbeat_disconnect\(/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_expire_heartbeat_if_due\(/i);

    const openBody = readFunctionBody('cq_heartbeat_open');
    const refreshBody = readFunctionBody('cq_heartbeat_refresh');

    expect(openBody).toMatch(/p_ticket_hash\s+text/i);
    expect(refreshBody).toMatch(/p_ticket_hash\s+text/i);
    expect(openBody).toMatch(/p_ticket_table_name\s+text\s+DEFAULT\s+'DOWNLOAD_TICKET_STATE_TABLE'/i);
    expect(refreshBody).toMatch(/p_ticket_table_name\s+text\s+DEFAULT\s+'DOWNLOAD_TICKET_STATE_TABLE'/i);
    expect(openBody).toMatch(/cq_heartbeat_touch_ticket_renewal\(v_ticket_hash,\s*p_lease_id,\s*v_now_ms,\s*v_ticket_table_name\)/i);
    expect(refreshBody).toMatch(/cq_heartbeat_touch_ticket_renewal\(v_ticket_hash,\s*p_lease_id,\s*v_now_ms,\s*v_ticket_table_name\)/i);

    const ackBody = readFunctionBody('cq_ack_handoff');
    expect(ackBody).toMatch(/p_start_timeout_ms\s+bigint/i);
    expect(ackBody).toMatch(/RETURNS TABLE\s*\(\s*result\s+text,\s*reason\s+text,\s*heartbeat_deadline_ms\s+bigint\s*\)/i);
    expect(ackBody).toMatch(/heartbeat_state\s*=\s*'none'/i);
    expect(ackBody).toMatch(/heartbeat_deadline_ms\s*=\s*LEAST\(v_commit_now_ms \+ p_start_timeout_ms, v_request\.hard_expire_at_ms\)/i);
  });

  it('locks ticket renewal ownership before authoritative request rows to avoid fan-out deadlocks', () => {
    const openBody = readFunctionBody('cq_heartbeat_open');
    const refreshBody = readFunctionBody('cq_heartbeat_refresh');

    const openTicketLockIndex = expectPatternIndex(
      openBody,
      /pg_advisory_xact_lock\(4,\s*hashtext\(v_ticket_hash\)\)/i,
      'expected cq_heartbeat_open to take a ticketHash advisory lock before request-row locking',
    );
    const openRequestRowLockIndex = expectPatternIndex(
      openBody,
      /FROM concurrency_requests\s+WHERE request_id = v_request_id\s+FOR UPDATE/i,
      'expected cq_heartbeat_open to lock the authoritative request row',
    );
    expect(openTicketLockIndex).toBeLessThan(openRequestRowLockIndex);

    const refreshTicketLockIndex = expectPatternIndex(
      refreshBody,
      /pg_advisory_xact_lock\(4,\s*hashtext\(v_ticket_hash\)\)/i,
      'expected cq_heartbeat_refresh to take a ticketHash advisory lock before request-row locking',
    );
    const refreshRequestRowLockIndex = expectPatternIndex(
      refreshBody,
      /FROM concurrency_requests\s+WHERE request_id = v_request_id\s+FOR UPDATE/i,
      'expected cq_heartbeat_refresh to lock the authoritative request row',
    );
    expect(refreshTicketLockIndex).toBeLessThan(refreshRequestRowLockIndex);
  });

  it('applies deterministic heartbeat cleanup on terminal request transitions', () => {
    const helperBody = readFunctionBody('cq_apply_request_terminal_transition');

    expect(helperBody).toMatch(/heartbeat_state\s*=\s*'none'/i);
    expect(helperBody).toMatch(/heartbeat_deadline_ms\s*=\s*NULL/i);
    expect(helperBody).toMatch(/heartbeat_grace_until_ms\s*=\s*NULL/i);
    expect(helperBody).toMatch(/heartbeat_terminal_reason\s*=\s*p_terminal_reason/i);
    expect(helperBody).not.toMatch(/heartbeat_generation\s*=\s*0/i);
    expect(helperBody).not.toMatch(/heartbeat_last_at_ms\s*=\s*NULL/i);
  });

  it('requires active live heartbeat state for ticket renewal ownership while allowing custom ticket tables', () => {
    const helperBody = readFunctionBody('cq_heartbeat_touch_ticket_renewal');

    expect(helperBody).toMatch(/p_ticket_table_name\s+text\s+DEFAULT\s+'DOWNLOAD_TICKET_STATE_TABLE'/i);
    expect(helperBody).toMatch(/v_ticket_table_name\s+text\s*:=\s*COALESCE\(NULLIF\(BTRIM\(COALESCE\(p_ticket_table_name,\s*''\)\),\s*''\),\s*'DOWNLOAD_TICKET_STATE_TABLE'\)/i);
    expect(helperBody).toMatch(/SELECT \* FROM %1\$I WHERE "TICKET_HASH" = \$1 FOR UPDATE/i);
    expect(helperBody).toMatch(/WHERE lease_id = v_ticket\."IDLE_RENEW_OWNER_LEASE_ID"/i);
    expect(helperBody).toMatch(/v_owner_request\.state\s*=\s*'active'/i);
    expect(helperBody).toMatch(/COALESCE\(v_owner_request\.heartbeat_state,\s*'none'\)\s+IN\s*\('connected',\s*'grace'\)/i);
    expect(helperBody).toMatch(/COALESCE\(v_owner_request\.heartbeat_deadline_ms,\s*0\)\s*>\s*v_now_ms/i);
    expect(helperBody).toMatch(/UPDATE %1\$I/i);
  });

  it('fails closed when the seeded renewal row is missing instead of accepting a compatibility noop path', () => {
    const helperBody = readFunctionBody('cq_heartbeat_touch_ticket_renewal');

    expect(helperBody).toMatch(/IF v_ticket_row_count = 0 THEN\s+RETURN 'ticket_not_found';\s+END IF;/i);
    expect(helperBody).not.toMatch(/IF v_ticket_row_count = 0 THEN\s+RETURN 'noop';\s+END IF;/i);
  });

  it('gives hard expiry precedence before heartbeat start timeout and heartbeat timeout', () => {
    const expireActiveBody = readFunctionBody('cq_expire_active_request_if_due');

    const hardExpiredIndex = expectPatternIndex(
      expireActiveBody,
      /hard_expired/i,
      'expected hard expiry terminal branch in cq_expire_active_request_if_due',
    );
    const heartbeatStartTimeoutIndex = expectPatternIndex(
      expireActiveBody,
      /heartbeat_start_timeout/i,
      'expected heartbeat_start_timeout branch in cq_expire_active_request_if_due',
    );
    const heartbeatTimeoutIndex = expectPatternIndex(
      expireActiveBody,
      /heartbeat_timeout/i,
      'expected heartbeat_timeout branch in cq_expire_active_request_if_due',
    );

    expect(heartbeatStartTimeoutIndex).toBeGreaterThan(hardExpiredIndex);
    expect(heartbeatTimeoutIndex).toBeGreaterThan(hardExpiredIndex);
  });
});
