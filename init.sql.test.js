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

  it('defines true-concurrency lease tables and rpc entrypoints', () => {
    expect(initSql).toMatch(/CREATE TABLE IF NOT EXISTS\s+concurrency_leases/i);
    expect(initSql).toMatch(/CREATE TABLE IF NOT EXISTS\s+concurrency_host_counters/i);
    expect(initSql).toMatch(/CREATE TABLE IF NOT EXISTS\s+concurrency_site_counters/i);
    expect(initSql).toMatch(/CREATE TABLE IF NOT EXISTS\s+concurrency_site_ip_counters/i);
    expect(initSql).toMatch(/CREATE UNIQUE INDEX IF NOT EXISTS\s+concurrency_leases_request_id_idx\s+ON\s+concurrency_leases\s*\(request_id\)/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_precheck\(/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_expire_scope\(/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_acquire\(/i);
    expect(initSql).toMatch(/CREATE OR REPLACE FUNCTION\s+cq_release\(/i);
  });

  it('uses database-authoritative time for cq_precheck lease visibility and retry_after', () => {
    const precheckBody = readFunctionBody('cq_precheck');

    expect(precheckBody).toMatch(/clock_timestamp\(\)/i);
    expect(precheckBody).not.toMatch(/p_now_ms/i);
    expect(precheckBody).toMatch(/expires_at_ms\s*>\s*v_now_ms/i);
    expect(precheckBody).toMatch(/retry_after\s*:=\s*CASE/i);
  });

  it('documents acquire replay rejection and release idempotency', () => {
    const acquireBody = readFunctionBody('cq_acquire');
    const releaseBody = readFunctionBody('cq_release');
    const expireBody = readFunctionBody('cq_expire_scope');

    expect(acquireBody).toMatch(/request_id/i);
    expect(acquireBody).toMatch(/hard_expire_at_ms/i);
    expect(acquireBody).toMatch(/expires_at_ms/i);
    expect(acquireBody).toMatch(/FOR UPDATE/i);
    expect(acquireBody).toMatch(/state\s*=\s*'active'/i);
    expect(acquireBody).toMatch(/tuple/i);
    expect(acquireBody).toMatch(/RAISE EXCEPTION 'cq_acquire request_id tuple mismatch'/i);
    expect(acquireBody).toMatch(/RAISE EXCEPTION 'cq_acquire request_id replay is no longer active'/i);
    expect(acquireBody).toMatch(/RAISE EXCEPTION 'cq_acquire hard_expire_at_ms is already in the past'/i);
    expect(acquireBody).toMatch(/FROM\s+concurrency_host_counters[\s\S]*?FOR UPDATE/i);
    expect(acquireBody).toMatch(/FROM\s+concurrency_site_counters[\s\S]*?FOR UPDATE/i);
    expect(acquireBody).toMatch(/FROM\s+concurrency_site_ip_counters[\s\S]*?FOR UPDATE/i);
    expect(acquireBody.indexOf('FROM concurrency_host_counters')).toBeLessThan(acquireBody.indexOf('FROM concurrency_site_counters'));
    expect(acquireBody.indexOf('FROM concurrency_site_counters')).toBeLessThan(acquireBody.indexOf('FROM concurrency_site_ip_counters'));
    expect(acquireBody).toMatch(/v_expires_at_ms\s*:=\s*p_hard_expire_at_ms/i);

    expect(releaseBody).toMatch(/lease_token/i);
    expect(releaseBody).toMatch(/state\s*=\s*'released'/i);
    expect(releaseBody).toMatch(/released_at/i);
    expect(releaseBody).toMatch(/FOR UPDATE/i);
    expect(releaseBody).toMatch(/active_count\s*=\s*GREATEST\(active_count\s*-\s*1,\s*0\)/i);
    expect(releaseBody).toMatch(/result\s*:=\s*'released'/i);

    expect(expireBody).toMatch(/state\s*=\s*'expired'/i);
    expect(expireBody).toMatch(/FOR UPDATE/i);
    expect(expireBody).toMatch(/concurrency_host_counters/i);
    expect(expireBody).toMatch(/concurrency_site_counters/i);
    expect(expireBody).toMatch(/concurrency_site_ip_counters/i);
    expect(expireBody).toMatch(/active_count\s*=\s*GREATEST\(active_count\s*-\s*v_expired_count,\s*0\)/i);
  });

  it('uses executable plpgsql lock statements for true-concurrency counters', () => {
    const acquireBody = readFunctionBody('cq_acquire');
    const expireBody = readFunctionBody('cq_expire_scope');

    expect(acquireBody).toMatch(/PERFORM\s+1\s+FROM\s+concurrency_host_counters\s+WHERE hostname_hash = v_hostname_hash\s+FOR UPDATE;/i);
    expect(acquireBody).toMatch(/PERFORM\s+1\s+FROM\s+concurrency_site_counters\s+WHERE hostname_hash = v_hostname_hash AND site_bucket = v_site_bucket\s+FOR UPDATE;/i);
    expect(acquireBody).toMatch(/PERFORM\s+1\s+FROM\s+concurrency_site_ip_counters\s+WHERE hostname_hash = v_hostname_hash AND site_bucket = v_site_bucket AND ip_bucket = v_ip_bucket\s+FOR UPDATE;/i);
    expect(acquireBody).not.toMatch(/SELECT\s+1\s+FROM\s+concurrency_host_counters\s+WHERE hostname_hash = v_hostname_hash\s+FOR UPDATE;/i);
    expect(acquireBody).not.toMatch(/SELECT\s+1\s+FROM\s+concurrency_site_counters\s+WHERE hostname_hash = v_hostname_hash AND site_bucket = v_site_bucket\s+FOR UPDATE;/i);
    expect(acquireBody).not.toMatch(/SELECT\s+1\s+FROM\s+concurrency_site_ip_counters\s+WHERE hostname_hash = v_hostname_hash AND site_bucket = v_site_bucket AND ip_bucket = v_ip_bucket\s+FOR UPDATE;/i);

    expect(expireBody).toMatch(/PERFORM\s+1\s+FROM\s+concurrency_host_counters[\s\S]*?FOR UPDATE;/i);
    expect(expireBody).not.toMatch(/SELECT\s+1\s+FROM\s+concurrency_host_counters[\s\S]*?FOR UPDATE;/i);
  });

  it('serializes request_id before tuple-scoped counter locks', () => {
    const acquireBody = readFunctionBody('cq_acquire');
    const requestLockIndex = acquireBody.search(/pg_advisory_xact_lock\(\s*3\s*,\s*hashtext\(v_request_id\)\s*\)/i);
    const hostLockIndex = acquireBody.search(/PERFORM\s+1\s+FROM\s+concurrency_host_counters\s+WHERE hostname_hash = v_hostname_hash\s+FOR UPDATE;/i);
    const existingLeaseIndex = acquireBody.search(/FROM\s+concurrency_leases\s+WHERE request_id = v_request_id\s+FOR UPDATE/i);

    expect(requestLockIndex).toBeGreaterThan(-1);
    expect(hostLockIndex).toBeGreaterThan(requestLockIndex);
    expect(existingLeaseIndex).toBeGreaterThan(requestLockIndex);
  });

  it('locks host-scope and site-scope expiry counters before lease rows', () => {
    const expireBody = readFunctionBody('cq_expire_scope');
    const hostBranch = expireBody.match(/IF v_scope = 'host' THEN([\s\S]*?)ELSIF v_scope = 'site' THEN/i)?.[1] ?? '';
    const siteBranch = expireBody.match(/ELSIF v_scope = 'site' THEN([\s\S]*?)ELSE/i)?.[1] ?? '';

    expect(hostBranch).toMatch(/PERFORM\s+1\s+FROM\s+concurrency_host_counters\s+WHERE hostname_hash = p_hostname_hash\s+FOR UPDATE;/i);
    expect(hostBranch).toMatch(/SELECT DISTINCT\s+site_bucket\s+FROM\s*\(\s*SELECT\s+site_bucket,\s+ip_bucket\s+FROM\s+concurrency_leases/i);
    expect(hostBranch).toMatch(/PERFORM\s+1\s+FROM\s+concurrency_site_counters\s+WHERE hostname_hash = p_hostname_hash\s+AND site_bucket = v_site_lock\.site_bucket\s+FOR UPDATE;/i);
    expect(hostBranch).toMatch(/SELECT DISTINCT\s+site_bucket,\s+ip_bucket\s+FROM\s*\(\s*SELECT\s+site_bucket,\s+ip_bucket\s+FROM\s+concurrency_leases/i);
    expect(hostBranch).toMatch(/PERFORM\s+1\s+FROM\s+concurrency_site_ip_counters\s+WHERE hostname_hash = p_hostname_hash\s+AND site_bucket = v_site_ip_lock\.site_bucket\s+AND ip_bucket = v_site_ip_lock\.ip_bucket\s+FOR UPDATE;/i);

    expect(siteBranch).toMatch(/PERFORM\s+1\s+FROM\s+concurrency_host_counters\s+WHERE hostname_hash = p_hostname_hash\s+FOR UPDATE;/i);
    expect(siteBranch).toMatch(/PERFORM\s+1\s+FROM\s+concurrency_site_counters\s+WHERE hostname_hash = p_hostname_hash\s+AND site_bucket = p_site_bucket\s+FOR UPDATE;/i);
    expect(siteBranch).toMatch(/SELECT DISTINCT\s+ip_bucket\s+FROM\s*\(\s*SELECT\s+ip_bucket\s+FROM\s+concurrency_leases/i);
    expect(siteBranch).toMatch(/PERFORM\s+1\s+FROM\s+concurrency_site_ip_counters\s+WHERE hostname_hash = p_hostname_hash\s+AND site_bucket = p_site_bucket\s+AND ip_bucket = v_site_ip_lock\.ip_bucket\s+FOR UPDATE;/i);
  });

  it('locks cq_release shared counters before the lease row', () => {
    const releaseBody = readFunctionBody('cq_release');
    const hostLockIndex = releaseBody.search(/FROM concurrency_host_counters/i);
    const siteLockIndex = releaseBody.search(/FROM concurrency_site_counters/i);
    const siteIpLockIndex = releaseBody.search(/FROM concurrency_site_ip_counters/i);
    const leaseLockIndex = releaseBody.search(/FROM concurrency_leases\s+WHERE lease_id = p_lease_id\s+FOR UPDATE/i);

    expect(releaseBody).toMatch(/SELECT\s+lease_id,\s*hostname_hash,\s*site_bucket,\s*ip_bucket\s+INTO\s+v_release_scope\s+FROM concurrency_leases\s+WHERE lease_id = p_lease_id;/i);
    expect(releaseBody).toMatch(/PERFORM\s+1\s+FROM\s+concurrency_host_counters\s+WHERE hostname_hash = v_release_scope\.hostname_hash\s+FOR UPDATE;/i);
    expect(releaseBody).toMatch(/PERFORM\s+1\s+FROM\s+concurrency_site_counters\s+WHERE hostname_hash = v_release_scope\.hostname_hash\s+AND site_bucket = v_release_scope\.site_bucket\s+FOR UPDATE;/i);
    expect(releaseBody).toMatch(/PERFORM\s+1\s+FROM\s+concurrency_site_ip_counters\s+WHERE hostname_hash = v_release_scope\.hostname_hash\s+AND site_bucket = v_release_scope\.site_bucket\s+AND ip_bucket = v_release_scope\.ip_bucket\s+FOR UPDATE;/i);
    expect(releaseBody).toMatch(/SELECT\s+\*\s+INTO\s+v_locked_lease\s+FROM concurrency_leases\s+WHERE lease_id = p_lease_id\s+FOR UPDATE;/i);

    expect(hostLockIndex).toBeGreaterThan(-1);
    expect(siteLockIndex).toBeGreaterThan(hostLockIndex);
    expect(siteIpLockIndex).toBeGreaterThan(siteLockIndex);
    expect(leaseLockIndex).toBeGreaterThan(siteIpLockIndex);
  });

  it('runs host-scope targeted expiry cleanup on the release hot path before locking the addressed lease', () => {
    const releaseBody = readFunctionBody('cq_release');
    const releaseScopeLookupIndex = releaseBody.search(/SELECT\s+lease_id,\s*hostname_hash,\s*site_bucket,\s*ip_bucket\s+INTO\s+v_release_scope/i);
    const expireIndex = releaseBody.search(/PERFORM\s+cq_expire_scope\(\s*'host'\s*,\s*v_release_scope\.hostname_hash\s*,\s*NULL\s*,\s*NULL\s*,\s*v_now_ms\s*,\s*500\s*\);/i);
    const leaseLockIndex = releaseBody.search(/FROM concurrency_leases\s+WHERE lease_id = p_lease_id\s+FOR UPDATE/i);

    expect(expireIndex).toBeGreaterThan(releaseScopeLookupIndex);
    expect(leaseLockIndex).toBeGreaterThan(expireIndex);
  });
});
