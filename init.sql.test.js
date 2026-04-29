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

  it('defines half-open batch columns and removes single-probe lease columns', () => {
    expect(initSql).toMatch(/"HALF_OPEN_BUDGET"\s+INTEGER\s+NOT NULL DEFAULT 0/i);
    expect(initSql).toMatch(/"HALF_OPEN_ISSUED"\s+INTEGER\s+NOT NULL DEFAULT 0/i);
    expect(initSql).toMatch(/"HALF_OPEN_REPORTED_MASK"\s+BIGINT\s+NOT NULL DEFAULT 0/i);
    expect(initSql).toMatch(/"HALF_OPEN_SUCCESS_COUNT"\s+INTEGER\s+NOT NULL DEFAULT 0/i);
    expect(initSql).toMatch(/"HALF_OPEN_DEADLINE"\s+INTEGER/i);
    expect(initSql).not.toMatch(/"PROBE_LEASE_UNTIL"/i);
  });

  it('defines download_authorize_breaker_attempt and removes download_claim_breaker_probe', () => {
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION download_authorize_breaker_attempt\(/i);
    expect(initSql).not.toMatch(/CREATE OR REPLACE FUNCTION download_claim_breaker_probe\(/i);
  });

  it('defines download_settle_breaker_attempt for non-probe terminal settlement', () => {
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION download_settle_breaker_attempt\(/i);
  });

  it('marks only the reported mask in download_settle_breaker_attempt without modifying sample counters', () => {
    const functionBody = readFunctionBody('download_settle_breaker_attempt');

    expect(functionBody).toMatch(/v_half_open_reported_mask := COALESCE\(v_half_open_reported_mask, 0\) \| v_ticket_mask/i);
    expect(functionBody).toMatch(/"HALF_OPEN_REPORTED_MASK" = v_half_open_reported_mask/i);
    expect(functionBody).not.toMatch(/v_total_samples :=/i);
    expect(functionBody).not.toMatch(/v_half_open_success_count := v_half_open_success_count \+ 1/i);
  });

  it('uses an explicit primary-key conflict target when seeding download_authorize_breaker_attempt', () => {
    const functionBody = readFunctionBody('download_authorize_breaker_attempt');

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

  it('tracks half-open reports by epoch ticket instead of single-probe version matching', () => {
    const functionBody = readFunctionBody('download_report_breaker_sample');

    expect(functionBody).toMatch(/v_half_open_ticket_mask_limit\s+CONSTANT\s+INTEGER\s*:=\s*63/i);
    expect(functionBody).toMatch(/IF p_attempt_version\s+IS NOT NULL THEN/i);
    expect(functionBody).toMatch(/IF v_state <> 'half_open' OR p_attempt_version <> v_version THEN/i);
    expect(functionBody).toMatch(/IF p_attempt_ticket IS NULL OR p_attempt_ticket < 1 OR p_attempt_ticket > v_half_open_issued OR p_attempt_ticket > v_half_open_ticket_mask_limit THEN/i);
    expect(functionBody).toMatch(/v_ticket_mask := \(1::BIGINT << \(p_attempt_ticket - 1\)\)/i);
    expect(functionBody).toMatch(/IF \(v_half_open_reported_mask & v_ticket_mask\) <> 0 THEN/i);
    expect(functionBody).toMatch(/v_half_open_reported_mask := v_half_open_reported_mask \| v_ticket_mask/i);
    expect(functionBody).not.toMatch(/v_probe_version_matches/i);
  });

  it('reopens exhausted half-open batches after all issued tickets report without enough proof using a safe full-mask comparison', () => {
    const functionBody = readFunctionBody('download_report_breaker_sample');

    expect(functionBody).toMatch(/v_half_open_success_count := v_half_open_success_count \+ 1/i);
    expect(functionBody).toMatch(/v_required_report_mask\s*:=\s*CASE\s+WHEN v_half_open_issued = v_half_open_ticket_mask_limit THEN 9223372036854775807::BIGINT\s+ELSE \(\(1::BIGINT << \(v_half_open_issued - 1\)\) - 1\) \| \(1::BIGINT << \(v_half_open_issued - 1\)\)\s+END/i);
    expect(functionBody).toMatch(/ELSIF v_half_open_issued = v_half_open_budget\s+AND v_half_open_issued > 0\s+AND \(v_half_open_reported_mask & v_required_report_mask\) = v_required_report_mask THEN\s+v_timeout_open_seconds := CASE[\s\S]*?v_state := 'open';\s+v_open_until := v_now \+ v_timeout_open_seconds;\s+v_last_open_seconds := v_timeout_open_seconds;/i);
    expect(functionBody).not.toMatch(/\(1::BIGINT << v_half_open_issued\) - 1/i);
  });

  it('rejects oversized half-open budgets before authorize stores a ticket mask that cannot fit in BIGINT', () => {
    const functionBody = readFunctionBody('download_authorize_breaker_attempt');

    expect(functionBody).toMatch(/v_half_open_ticket_mask_limit\s+CONSTANT\s+INTEGER\s*:=\s*63/i);
    expect(functionBody).toMatch(/IF p_half_open_max_probe_count > v_half_open_ticket_mask_limit THEN\s+RAISE EXCEPTION 'download_authorize_breaker_attempt half-open max probe count exceeds BIGINT mask capacity: %', p_half_open_max_probe_count;/i);
  });

  it('accepts explicit half-open batch authority parameters in download_authorize_breaker_attempt', () => {
    const functionBody = readFunctionBody('download_authorize_breaker_attempt');

    expect(functionBody).toMatch(/p_half_open_max_probe_count\s+INTEGER/i);
    expect(functionBody).toMatch(/p_half_open_max_seconds\s+INTEGER/i);
    expect(functionBody).toMatch(/p_half_open_timeout_mode\s+TEXT/i);
    expect(functionBody).toMatch(/"HALF_OPEN_DEADLINE"\s+INTEGER/i);
    expect(functionBody).toMatch(/"ATTEMPT_GRANTED"\s+BOOLEAN/i);
    expect(functionBody).toMatch(/"ATTEMPT_TICKET"\s+INTEGER/i);
  });

  it('only transitions open to half_open when open_until is explicitly expired', () => {
    const functionBody = readFunctionBody('download_authorize_breaker_attempt');

    expect(functionBody).toMatch(/IF\s+v_state\s*=\s*'open'\s+AND\s+v_open_until\s+IS\s+NULL\s+THEN\s+RAISE EXCEPTION/i);
    expect(functionBody).toMatch(/ELSIF\s+v_state\s*=\s*'open'\s+AND\s+v_open_until\s*<=\s*v_now\s+THEN/i);
    expect(functionBody).not.toMatch(/ELSIF\s+v_state\s*=\s*'open'\s+THEN/i);
  });

  it('applies half-open timeout mode only when half_open_deadline is explicitly reached', () => {
    const functionBody = readFunctionBody('download_authorize_breaker_attempt');

    expect(functionBody).toMatch(/ELSIF\s+v_state\s*=\s*'half_open'\s+AND\s+v_half_open_deadline\s*<=\s*v_now\s+THEN/i);
    expect(functionBody).not.toMatch(/v_half_open_deadline\s+IS\s+NULL\s+OR\s+v_half_open_deadline\s*<=\s*v_now/i);
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

  it('requires explicit authorize inputs instead of SQL fallbacks', () => {
    const functionBody = readFunctionBody('download_authorize_breaker_attempt');

    for (const requiredParam of [
      /p_half_open_max_probe_count\s+is\s+null/i,
      /p_half_open_max_seconds\s+is\s+null/i,
      /p_half_open_timeout_mode\s+is\s+null/i,
    ]) {
      expect(functionBody).toMatch(requiredParam);
    }

    for (const legacyFallback of [
      /coalesce\(p_half_open_max_probe_count,\s*4\)/i,
      /coalesce\(p_half_open_max_seconds,\s*0\)/i,
      /coalesce\(p_half_open_timeout_mode,\s*'partial-close'\)/i,
    ]) {
      expect(functionBody).not.toMatch(legacyFallback);
    }

    expect(functionBody).not.toMatch(/p_probe_lease_seconds/i);
  });

  it('replaces fq_try_acquire_batch with fq_admit_batch', () => {
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION fq_admit_batch\(/i);
    expect(initSql).not.toMatch(/CREATE OR REPLACE FUNCTION fq_try_acquire_batch\(/i);
  });

  it('defines fq_admit_batch with explicit breaker gate inputs', () => {
    const functionBody = readFunctionBody('fq_admit_batch');

    expect(functionBody).toMatch(/p_breaker_enabled\s+BOOLEAN/i);
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
    expect(initSql).toMatch(/CREATE TABLE IF NOT EXISTS\s+concurrency_host_counters/i);
    expect(initSql).toMatch(/CREATE TABLE IF NOT EXISTS\s+concurrency_site_counters/i);
    expect(initSql).toMatch(/CREATE TABLE IF NOT EXISTS\s+concurrency_site_ip_counters/i);
    expect(initSql).toMatch(/CREATE UNIQUE INDEX IF NOT EXISTS\s+concurrency_leases_request_id_idx\s+ON\s+concurrency_leases\s*\(request_id\)/i);
    expect(initSql).toMatch(/CREATE INDEX IF NOT EXISTS\s+concurrency_requests_waiting_host_idx/i);
    expect(initSql).not.toMatch(/CREATE OR REPLACE FUNCTION\s+cq_precheck\(/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_expire_scope\(/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_release\(/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_claim_grant\(/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_cancel\(/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_promote_waiting_request\(/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_acquire\(/i);
    expect(initSql).not.toMatch(/CREATE OR REPLACE FUNCTION\s+cq_release_by_request\(/i);
  });

  it('encodes waiting-model acquire timing and wait-token replay in cq_acquire', () => {
    const acquireBody = readFunctionBody('cq_acquire');

    expect(acquireBody).toMatch(/p_wait_token\s+text DEFAULT NULL/i);
    expect(acquireBody).toMatch(/p_wait_poll_window_ms\s+integer DEFAULT 0/i);
    expect(acquireBody).toMatch(/p_wait_reconnect_grace_ms\s+integer DEFAULT 0/i);
    expect(acquireBody).toMatch(/v_now_ms\s+bigint := COALESCE\(p_now_ms, \(EXTRACT\(EPOCH FROM clock_timestamp\(\)\) \* 1000\)::bigint\)/i);
    expect(acquireBody).toMatch(/v_waiter_lease_until_ms := v_now_ms[\s\S]*?p_wait_poll_window_ms[\s\S]*?p_wait_reconnect_grace_ms/i);
    expect(acquireBody).toMatch(/WHERE concurrency_requests\.wait_token = v_wait_token_input/i);
    expect(acquireBody).toMatch(/RAISE EXCEPTION 'cq_acquire stale wait token'/i);
    expect(acquireBody).toMatch(/IF v_wait_token_input IS NOT NULL THEN[\s\S]*?UPDATE concurrency_requests[\s\S]*?SET waiter_lease_until_ms = v_waiter_lease_until_ms/i);
    expect(acquireBody).toMatch(/INSERT INTO concurrency_requests[\s\S]*?state,[\s\S]*?'waiting'[\s\S]*?wait_token,[\s\S]*?waiter_lease_until_ms/i);
  });

  it('documents request-ledger replay, cancel tombstones, and release idempotency', () => {
    const acquireBody = readFunctionBody('cq_acquire');
    const cancelBody = readFunctionBody('cq_cancel');
    const releaseBody = readFunctionBody('cq_release');
    const expireBody = readFunctionBody('cq_expire_scope');
    const expireActiveBody = readFunctionBody('cq_expire_active_request_if_due');
    const claimBody = readFunctionBody('cq_claim_grant');

    expect(acquireBody).toMatch(/request_id/i);
    expect(acquireBody).toMatch(/wait_token/i);
    expect(acquireBody).toMatch(/waiter_lease_until_ms/i);
    expect(acquireBody).toMatch(/FOR UPDATE/i);
    expect(acquireBody).toMatch(/tuple/i);
    expect(acquireBody).toMatch(/RAISE EXCEPTION 'cq_acquire request_id tuple mismatch'/i);
    expect(acquireBody).toMatch(/RAISE EXCEPTION 'cq_acquire stale wait token'/i);
    expect(acquireBody).toMatch(/WHEN 'active' THEN[\s\S]*?result := 'expired'[\s\S]*?result := 'conflict'/i);
    expect(acquireBody).toMatch(/WHEN 'released' THEN[\s\S]*?result := 'released'/i);
    expect(acquireBody).toMatch(/WHEN 'cancelled' THEN[\s\S]*?result := 'cancelled'/i);
    expect(acquireBody).toMatch(/WHEN 'expired' THEN[\s\S]*?result := 'expired'/i);
    expect(acquireBody).toMatch(/IF p_hard_expire_at_ms IS NULL OR p_hard_expire_at_ms <= v_now_ms THEN[\s\S]*?result := 'expired'/i);

    expect(claimBody).toMatch(/WHEN 'active' THEN[\s\S]*?result := 'expired'[\s\S]*?v_request\.claim_token IS DISTINCT FROM v_claim_token[\s\S]*?result := 'conflict'[\s\S]*?result := 'granted'/i);
    expect(claimBody).toMatch(/claim_state = 'claimed'/i);

    expect(cancelBody).toMatch(/INSERT INTO concurrency_requests/i);
    expect(cancelBody).toMatch(/p_hostname\s+text/i);
    expect(cancelBody).toMatch(/VALUES \([\s\S]*?'cancelled'[\s\S]*?'request_cancelled'/i);
    expect(cancelBody).toMatch(/v_hostname\s+text := BTRIM\(COALESCE\(p_hostname, ''\)\)/i);
    expect(cancelBody).toMatch(/v_request\.hostname IS DISTINCT FROM v_hostname/i);
    expect(cancelBody).toMatch(/RAISE EXCEPTION 'cq_cancel request_id tuple mismatch'/i);
    expect(cancelBody).toMatch(/RAISE EXCEPTION 'cq_cancel must release active lease'/i);
    expect(cancelBody).not.toMatch(/v_hostname_hash,\s*\n\s*v_hostname_hash/i);
    expect(cancelBody).toMatch(/result := 'noop';[\s\S]*?reason := 'already_terminal'/i);

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
    const requestLockIndex = acquireBody.search(/pg_advisory_xact_lock\(\s*3\s*,\s*hashtext\(v_authoritative_request_id\)\s*\)/i);
    const hostLockIndex = acquireBody.search(/PERFORM\s+1\s+FROM\s+concurrency_host_counters\s+WHERE hostname_hash = v_hostname_hash\s+FOR UPDATE;/i);
    const requestRowIndex = acquireBody.search(/FROM\s+concurrency_requests\s+WHERE request_id = v_authoritative_request_id\s+FOR UPDATE/i);

    expect(requestLockIndex).toBeGreaterThan(-1);
    expect(hostLockIndex).toBeGreaterThan(requestLockIndex);
    expect(requestRowIndex).toBeGreaterThan(requestLockIndex);
  });

  it('serializes request-scoped terminal lifecycle mutations through request_id helper locking', () => {
    const helperBody = readFunctionBody('cq_apply_request_terminal_transition');
    const releaseBody = readFunctionBody('cq_release');
    const claimBody = readFunctionBody('cq_claim_grant');
    const promoteBody = readFunctionBody('cq_promote_waiting_request');
    const probeBody = readFunctionBody('cq_continue_wait_probe');

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

  it('resolves authoritative wait-token request ids before entering request advisory locking in reconnect paths', () => {
    const acquireBody = readFunctionBody('cq_acquire');
    const probeBody = readFunctionBody('cq_continue_wait_probe');

    const acquireResolveIndex = expectPatternIndex(
      acquireBody,
      /SELECT\s+concurrency_requests\.request_id\s+INTO\s+v_authoritative_request_id\s+FROM\s+concurrency_requests\s+WHERE concurrency_requests\.wait_token = v_wait_token_input/i,
      'expected cq_acquire to resolve the authoritative request id from wait_token before advisory locking',
    );
    const acquireLockIndex = expectPatternIndex(
      acquireBody,
      /pg_advisory_xact_lock\(3, hashtext\(v_authoritative_request_id\)\)/i,
      'expected cq_acquire to enter request advisory locking with the authoritative request id',
    );
    const acquireRequestRowIndex = expectPatternIndex(
      acquireBody,
      /FROM\s+concurrency_requests\s+WHERE request_id = v_authoritative_request_id\s+FOR UPDATE/i,
      'expected cq_acquire to lock the authoritative request row after entering advisory locking',
    );

    expect(acquireLockIndex).toBeGreaterThan(acquireResolveIndex);
    expect(acquireRequestRowIndex).toBeGreaterThan(acquireLockIndex);
    expect(acquireBody).not.toMatch(/pg_advisory_xact_lock\(3, hashtext\(v_request_id\)\)/i);

    const probeResolveIndex = expectPatternIndex(
      probeBody,
      /SELECT\s+concurrency_requests\.request_id\s+INTO\s+v_authoritative_request_id\s+FROM\s+concurrency_requests\s+WHERE concurrency_requests\.wait_token = v_wait_token_input/i,
      'expected cq_continue_wait_probe to resolve the authoritative request id from wait_token before advisory locking',
    );
    const probeLockIndex = expectPatternIndex(
      probeBody,
      /pg_advisory_xact_lock\(3, hashtext\(v_authoritative_request_id\)\)/i,
      'expected cq_continue_wait_probe to enter request advisory locking with the authoritative request id',
    );
    const probeRequestRowIndex = expectPatternIndex(
      probeBody,
      /FROM\s+concurrency_requests\s+WHERE request_id = v_authoritative_request_id\s+FOR UPDATE/i,
      'expected cq_continue_wait_probe to lock the authoritative request row after entering advisory locking',
    );

    expect(probeLockIndex).toBeGreaterThan(probeResolveIndex);
    expect(probeRequestRowIndex).toBeGreaterThan(probeLockIndex);
    expect(probeBody).not.toMatch(/pg_advisory_xact_lock\(3, hashtext\(v_request_id\)\)/i);
  });
});
