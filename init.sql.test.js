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
});
