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

  it('uses an explicit primary-key conflict target when seeding download_claim_breaker_probe', () => {
    const functionBody = readFunctionBody('download_claim_breaker_probe');

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

  it('accepts explicit half-open timeout parameters in download_claim_breaker_probe', () => {
    const functionBody = readFunctionBody('download_claim_breaker_probe');

    expect(functionBody).toMatch(/p_probe_lease_seconds\s+INTEGER/i);
    expect(functionBody).toMatch(/p_half_open_max_seconds\s+INTEGER/i);
    expect(functionBody).toMatch(/p_half_open_timeout_mode\s+TEXT/i);
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

  it('requires explicit claim timeout inputs instead of SQL fallbacks', () => {
    const functionBody = readFunctionBody('download_claim_breaker_probe');

    for (const requiredParam of [
      /p_probe_lease_seconds\s+is\s+null/i,
      /p_half_open_max_seconds\s+is\s+null/i,
      /p_half_open_timeout_mode\s+is\s+null/i,
    ]) {
      expect(functionBody).toMatch(requiredParam);
    }

    for (const legacyFallback of [
      /coalesce\(p_probe_lease_seconds,\s*15\)/i,
      /coalesce\(p_half_open_max_seconds,\s*0\)/i,
      /coalesce\(p_half_open_timeout_mode,\s*'partial-close'\)/i,
    ]) {
      expect(functionBody).not.toMatch(legacyFallback);
    }
  });
});
