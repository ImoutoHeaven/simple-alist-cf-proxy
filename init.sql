-- ========================================
-- Infrastructure Tables For simple-alist-cf-proxy
-- ========================================
-- Default table names align with environment variable defaults:
--   DOWNLOAD_CACHE_TABLE           (env: DOWNLOAD_CACHE_TABLE)
--   THROTTLE_PROTECTION            (canonical breaker runtime table)
--   DOWNLOAD_IP_RATELIMIT_TABLE    (env: DOWNLOAD_IP_RATELIMIT_TABLE)
--
-- If you override the configurable environment variables, adjust the CREATE TABLE
-- statements accordingly before applying this script.


-- ========================================
-- Download Cache Table Schema
-- ========================================
-- Purpose: Cache AList /api/fs/link responses to reduce API calls
-- Compatible with: SQLite (D1), PostgreSQL

CREATE TABLE IF NOT EXISTS "DOWNLOAD_CACHE_TABLE" (
  "PATH_HASH" TEXT PRIMARY KEY,
  "PATH" TEXT NOT NULL,
  "LINK_DATA" TEXT NOT NULL,
  "TIMESTAMP" INTEGER NOT NULL,
  "HOSTNAME_HASH" TEXT
);

CREATE INDEX IF NOT EXISTS idx_download_cache_timestamp
  ON "DOWNLOAD_CACHE_TABLE"("TIMESTAMP");
CREATE INDEX IF NOT EXISTS idx_download_cache_hostname
  ON "DOWNLOAD_CACHE_TABLE"("HOSTNAME_HASH");


-- ========================================
-- Download Last Active Table Schema
-- ========================================
-- Purpose: Track last access time and usage count per IP/path pair
-- Compatible with: PostgreSQL

CREATE TABLE IF NOT EXISTS "DOWNLOAD_LAST_ACTIVE_TABLE" (
  "IP_HASH" TEXT NOT NULL,
  "PATH_HASH" TEXT NOT NULL,
  "LAST_ACCESS_TIME" BIGINT NOT NULL,
  "TOTAL_ACCESS_COUNT" INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY ("IP_HASH", "PATH_HASH")
);

CREATE INDEX IF NOT EXISTS idx_download_last_active_time
  ON "DOWNLOAD_LAST_ACTIVE_TABLE"("LAST_ACCESS_TIME");


-- ========================================
-- Stored Procedure: Upsert Download Last Active
-- ========================================
CREATE OR REPLACE FUNCTION download_update_last_active(
  p_ip_hash TEXT,
  p_path_hash TEXT,
  p_last_access_time BIGINT,
  p_table_name TEXT DEFAULT 'DOWNLOAD_LAST_ACTIVE_TABLE'
)
RETURNS JSON AS $$
DECLARE
  sql TEXT;
BEGIN
  sql := format(
    'INSERT INTO %1$I ("IP_HASH", "PATH_HASH", "LAST_ACCESS_TIME", "TOTAL_ACCESS_COUNT")
     VALUES ($1, $2, $3, 1)
     ON CONFLICT ("IP_HASH", "PATH_HASH") DO UPDATE SET
       "LAST_ACCESS_TIME" = EXCLUDED."LAST_ACCESS_TIME",
       "TOTAL_ACCESS_COUNT" = %1$I."TOTAL_ACCESS_COUNT" + 1',
    p_table_name
  );

  EXECUTE sql USING p_ip_hash, p_path_hash, p_last_access_time;
  RETURN json_build_object('success', true);
EXCEPTION
  WHEN others THEN
    RETURN json_build_object('success', false, 'error', SQLERRM);
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- PostgreSQL Stored Procedure: Atomic UPSERT (Download Cache)
-- ========================================
CREATE OR REPLACE FUNCTION download_upsert_download_cache(
  p_path_hash TEXT,
  p_path TEXT,
  p_link_data TEXT,
  p_timestamp INTEGER,
  p_hostname_hash TEXT DEFAULT NULL,
  p_table_name TEXT DEFAULT 'DOWNLOAD_CACHE_TABLE'
)
RETURNS TABLE(
  "PATH_HASH" TEXT,
  "PATH" TEXT,
  "LINK_DATA" TEXT,
  "TIMESTAMP" INTEGER,
  "HOSTNAME_HASH" TEXT
) AS $$
DECLARE
  sql TEXT;
BEGIN
  sql := format(
    'INSERT INTO %1$I ("PATH_HASH", "PATH", "LINK_DATA", "TIMESTAMP", "HOSTNAME_HASH")
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT ("PATH_HASH") DO UPDATE SET
       "LINK_DATA" = EXCLUDED."LINK_DATA",
       "TIMESTAMP" = EXCLUDED."TIMESTAMP",
       "PATH" = EXCLUDED."PATH",
       "HOSTNAME_HASH" = EXCLUDED."HOSTNAME_HASH"
     RETURNING "PATH_HASH", "PATH", "LINK_DATA", "TIMESTAMP", "HOSTNAME_HASH"',
    p_table_name
  );

  RETURN QUERY EXECUTE sql USING p_path_hash, p_path, p_link_data, p_timestamp, p_hostname_hash;
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- Optional: Cleanup Function (PostgreSQL)
-- ========================================
CREATE OR REPLACE FUNCTION download_cleanup_expired_cache(
  p_ttl_seconds INTEGER,
  p_table_name TEXT DEFAULT 'DOWNLOAD_CACHE_TABLE'
)
RETURNS INTEGER AS $$
DECLARE
  deleted_count INTEGER;
  sql TEXT;
BEGIN
  sql := format(
    'DELETE FROM %1$I
     WHERE EXTRACT(EPOCH FROM NOW())::INTEGER - "TIMESTAMP" > $1',
    p_table_name
  );

  EXECUTE sql USING p_ttl_seconds;
  GET DIAGNOSTICS deleted_count = ROW_COUNT;

  RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- Throttle Protection Table Schema
-- ========================================
CREATE TABLE IF NOT EXISTS "THROTTLE_PROTECTION" (
  "HOSTNAME_HASH" TEXT PRIMARY KEY,
  "HOSTNAME" TEXT NOT NULL,
  "STATE" TEXT NOT NULL CHECK ("STATE" IN ('closed', 'open', 'half_open')),
  "OPEN_UNTIL" INTEGER,
  "EWMA_SCORE" NUMERIC NOT NULL DEFAULT 0,
  "TOTAL_SAMPLES" INTEGER NOT NULL DEFAULT 0,
  "SAMPLES_SINCE_RESET" INTEGER NOT NULL DEFAULT 0,
  "LAST_SAMPLE_AT" INTEGER,
  "CONSECUTIVE_ERROR_COUNT" INTEGER NOT NULL DEFAULT 0,
  "SUCCESS_STREAK" INTEGER NOT NULL DEFAULT 0,
  "HALF_OPEN_SINCE" INTEGER,
  "HALF_OPEN_BUDGET" INTEGER NOT NULL DEFAULT 0,
  "HALF_OPEN_ISSUED" INTEGER NOT NULL DEFAULT 0,
  "HALF_OPEN_REPORTED_MASK" BIGINT NOT NULL DEFAULT 0,
  "HALF_OPEN_SUCCESS_COUNT" INTEGER NOT NULL DEFAULT 0,
  "HALF_OPEN_DEADLINE" INTEGER,
  "LAST_ERROR_CODE" INTEGER,
  "OPEN_REASON" TEXT,
  "LAST_OPEN_SECONDS" INTEGER NOT NULL DEFAULT 0,
  "VERSION" BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_throttle_open_until
  ON "THROTTLE_PROTECTION"("OPEN_UNTIL");

CREATE INDEX IF NOT EXISTS idx_throttle_state
  ON "THROTTLE_PROTECTION"("STATE");


-- ========================================
-- PostgreSQL Stored Procedure: Authorize Breaker Attempt
-- ========================================
CREATE OR REPLACE FUNCTION download_authorize_breaker_attempt(
  p_hostname_hash TEXT,
  p_hostname TEXT,
  p_now INTEGER,
  p_half_open_max_probe_count INTEGER,
  p_half_open_max_seconds INTEGER,
  p_half_open_timeout_mode TEXT
)
RETURNS TABLE(
  "HOSTNAME_HASH" TEXT,
  "HOSTNAME" TEXT,
  "STATE" TEXT,
  "OPEN_UNTIL" INTEGER,
  "OPEN_REASON" TEXT,
  "LAST_ERROR_CODE" INTEGER,
  "VERSION" BIGINT,
  "HALF_OPEN_DEADLINE" INTEGER,
  "ATTEMPT_GRANTED" BOOLEAN,
  "ATTEMPT_TICKET" INTEGER
) AS $$
DECLARE
  v_now INTEGER := COALESCE(p_now, EXTRACT(EPOCH FROM NOW())::INTEGER);
  v_half_open_ticket_mask_limit CONSTANT INTEGER := 63;
  v_half_open_max_probe_count INTEGER;
  v_half_open_max_seconds INTEGER;
  v_half_open_timeout_mode TEXT;
  v_timeout_open_seconds INTEGER := 0;

  v_hostname TEXT := p_hostname;
  v_state TEXT := 'closed';
  v_initial_state TEXT := 'closed';
  v_open_until INTEGER := NULL;
  v_ewma_score NUMERIC := 0;
  v_total_samples INTEGER := 0;
  v_samples_since_reset INTEGER := 0;
  v_consecutive_error_count INTEGER := 0;
  v_success_streak INTEGER := 0;
  v_half_open_since INTEGER := NULL;
  v_half_open_budget INTEGER := 0;
  v_half_open_issued INTEGER := 0;
  v_half_open_reported_mask BIGINT := 0;
  v_half_open_success_count INTEGER := 0;
  v_half_open_deadline INTEGER := NULL;
  v_last_error_code INTEGER := NULL;
  v_open_reason TEXT := NULL;
  v_last_open_seconds INTEGER := 0;
  v_version BIGINT := 0;
  v_attempt_granted BOOLEAN := FALSE;
  v_attempt_ticket INTEGER := NULL;
  v_locked BOOLEAN := FALSE;
  v_locked_row_count INTEGER := 0;
BEGIN
  IF p_hostname_hash IS NULL OR p_hostname_hash = '' THEN
    RETURN;
  END IF;

  IF p_half_open_max_probe_count IS NULL
    OR p_half_open_max_seconds IS NULL
    OR p_half_open_timeout_mode IS NULL
    OR BTRIM(p_half_open_timeout_mode) = '' THEN
    RAISE EXCEPTION 'download_authorize_breaker_attempt requires non-null half-open settings';
  END IF;

  IF p_half_open_max_probe_count > v_half_open_ticket_mask_limit THEN
    RAISE EXCEPTION 'download_authorize_breaker_attempt half-open max probe count exceeds BIGINT mask capacity: %', p_half_open_max_probe_count;
  END IF;

  v_half_open_max_probe_count := GREATEST(1, p_half_open_max_probe_count);
  v_half_open_max_seconds := GREATEST(1, p_half_open_max_seconds);
  v_half_open_timeout_mode := LOWER(BTRIM(p_half_open_timeout_mode));
  IF v_half_open_timeout_mode NOT IN ('open', 'close', 'partial-close') THEN
    RAISE EXCEPTION 'download_authorize_breaker_attempt invalid p_half_open_timeout_mode: %', p_half_open_timeout_mode;
  END IF;

  WHILE NOT v_locked LOOP
    SELECT
      tp."HOSTNAME",
      tp."STATE",
      tp."OPEN_UNTIL",
      tp."EWMA_SCORE",
      tp."TOTAL_SAMPLES",
      tp."SAMPLES_SINCE_RESET",
      tp."CONSECUTIVE_ERROR_COUNT",
      tp."SUCCESS_STREAK",
      tp."HALF_OPEN_SINCE",
      tp."HALF_OPEN_BUDGET",
      tp."HALF_OPEN_ISSUED",
      tp."HALF_OPEN_REPORTED_MASK",
      tp."HALF_OPEN_SUCCESS_COUNT",
      tp."HALF_OPEN_DEADLINE",
      tp."LAST_ERROR_CODE",
      tp."OPEN_REASON",
      tp."LAST_OPEN_SECONDS",
      tp."VERSION"
    INTO
      v_hostname,
      v_state,
      v_open_until,
      v_ewma_score,
      v_total_samples,
      v_samples_since_reset,
      v_consecutive_error_count,
      v_success_streak,
      v_half_open_since,
      v_half_open_budget,
      v_half_open_issued,
      v_half_open_reported_mask,
      v_half_open_success_count,
      v_half_open_deadline,
      v_last_error_code,
      v_open_reason,
      v_last_open_seconds,
      v_version
    FROM "THROTTLE_PROTECTION" AS tp
    WHERE tp."HOSTNAME_HASH" = p_hostname_hash
    FOR UPDATE;

    GET DIAGNOSTICS v_locked_row_count = ROW_COUNT;
    v_locked := v_locked_row_count > 0;

    IF NOT v_locked THEN
      INSERT INTO "THROTTLE_PROTECTION" ("HOSTNAME_HASH", "HOSTNAME", "STATE")
      VALUES (p_hostname_hash, COALESCE(NULLIF(p_hostname, ''), p_hostname_hash), 'closed')
      ON CONFLICT ON CONSTRAINT "THROTTLE_PROTECTION_pkey" DO NOTHING;
    END IF;
  END LOOP;

  v_hostname := COALESCE(NULLIF(p_hostname, ''), v_hostname, p_hostname_hash);
  v_state := COALESCE(NULLIF(v_state, ''), 'closed');
  v_ewma_score := COALESCE(v_ewma_score, 0);
  v_total_samples := COALESCE(v_total_samples, 0);
  v_samples_since_reset := COALESCE(v_samples_since_reset, 0);
  v_consecutive_error_count := COALESCE(v_consecutive_error_count, 0);
  v_success_streak := COALESCE(v_success_streak, 0);
  v_half_open_budget := COALESCE(v_half_open_budget, 0);
  v_half_open_issued := COALESCE(v_half_open_issued, 0);
  v_half_open_reported_mask := COALESCE(v_half_open_reported_mask, 0);
  v_half_open_success_count := COALESCE(v_half_open_success_count, 0);
  v_last_open_seconds := COALESCE(v_last_open_seconds, 0);
  v_version := COALESCE(v_version, 0);
  v_initial_state := v_state;

  IF v_half_open_budget > v_half_open_ticket_mask_limit
    OR v_half_open_issued > v_half_open_ticket_mask_limit
    OR v_half_open_reported_mask < 0 THEN
    RAISE EXCEPTION 'download_authorize_breaker_attempt half-open ticket state exceeds BIGINT mask capacity';
  END IF;

  IF v_state = 'open' AND v_open_until IS NULL THEN
    RAISE EXCEPTION 'download_authorize_breaker_attempt invalid open row without OPEN_UNTIL';
  ELSIF v_state = 'open' AND v_open_until > v_now THEN
    NULL;
  ELSIF v_state = 'open' AND v_open_until <= v_now THEN
    v_state := 'half_open';
    v_open_until := NULL;
    v_success_streak := 0;
    v_half_open_since := v_now;
    v_half_open_budget := v_half_open_max_probe_count;
    v_half_open_issued := 1;
    v_half_open_reported_mask := 0;
    v_half_open_success_count := 0;
    v_half_open_deadline := v_now + v_half_open_max_seconds;
    v_attempt_granted := TRUE;
    v_attempt_ticket := 1;
  ELSIF v_state = 'half_open' AND v_half_open_deadline <= v_now THEN
    v_timeout_open_seconds := CASE
      WHEN v_last_open_seconds > 0 THEN v_last_open_seconds
      ELSE 1
    END;

    IF v_half_open_timeout_mode = 'open' THEN
      v_state := 'open';
      v_open_until := v_now + v_timeout_open_seconds;
      v_last_open_seconds := v_timeout_open_seconds;
    ELSIF v_half_open_timeout_mode = 'close' THEN
      v_state := 'closed';
      v_open_until := NULL;
      v_ewma_score := 0;
      v_consecutive_error_count := 0;
      v_success_streak := 0;
      v_samples_since_reset := 0;
      v_last_error_code := NULL;
      v_open_reason := NULL;
      v_last_open_seconds := 0;
    ELSE
      IF v_half_open_success_count > 0 THEN
        v_state := 'closed';
        v_open_until := NULL;
        v_ewma_score := 0;
        v_consecutive_error_count := 0;
        v_success_streak := 0;
        v_samples_since_reset := 0;
        v_last_error_code := NULL;
        v_open_reason := NULL;
        v_last_open_seconds := 0;
      ELSE
        v_state := 'open';
        v_open_until := v_now + v_timeout_open_seconds;
        v_last_open_seconds := v_timeout_open_seconds;
      END IF;
    END IF;

    v_half_open_since := NULL;
    v_half_open_budget := 0;
    v_half_open_issued := 0;
    v_half_open_reported_mask := 0;
    v_half_open_success_count := 0;
    v_half_open_deadline := NULL;
  ELSIF v_state = 'half_open'
    AND v_half_open_deadline > v_now
    AND v_half_open_issued < v_half_open_budget THEN
    v_half_open_issued := v_half_open_issued + 1;
    v_attempt_granted := TRUE;
    v_attempt_ticket := v_half_open_issued;
  ELSIF v_state = 'closed' THEN
    v_open_until := NULL;
    v_success_streak := 0;
    v_half_open_since := NULL;
    v_half_open_budget := 0;
    v_half_open_issued := 0;
    v_half_open_reported_mask := 0;
    v_half_open_success_count := 0;
    v_half_open_deadline := NULL;
  END IF;

  IF v_state = 'open' OR v_state = 'closed' THEN
    v_half_open_since := NULL;
    v_half_open_budget := 0;
    v_half_open_issued := 0;
    v_half_open_reported_mask := 0;
    v_half_open_success_count := 0;
    v_half_open_deadline := NULL;
  END IF;

  IF v_state IS DISTINCT FROM v_initial_state THEN
    v_version := v_version + 1;
  END IF;

  RETURN QUERY
  UPDATE "THROTTLE_PROTECTION" AS tp SET
    "HOSTNAME" = v_hostname,
    "STATE" = v_state,
    "OPEN_UNTIL" = v_open_until,
    "EWMA_SCORE" = v_ewma_score,
    "TOTAL_SAMPLES" = v_total_samples,
    "SAMPLES_SINCE_RESET" = v_samples_since_reset,
    "CONSECUTIVE_ERROR_COUNT" = v_consecutive_error_count,
    "SUCCESS_STREAK" = v_success_streak,
    "HALF_OPEN_SINCE" = v_half_open_since,
    "HALF_OPEN_BUDGET" = v_half_open_budget,
    "HALF_OPEN_ISSUED" = v_half_open_issued,
    "HALF_OPEN_REPORTED_MASK" = v_half_open_reported_mask,
    "HALF_OPEN_SUCCESS_COUNT" = v_half_open_success_count,
    "HALF_OPEN_DEADLINE" = v_half_open_deadline,
    "LAST_ERROR_CODE" = v_last_error_code,
    "OPEN_REASON" = v_open_reason,
    "LAST_OPEN_SECONDS" = v_last_open_seconds,
    "VERSION" = v_version
  WHERE tp."HOSTNAME_HASH" = p_hostname_hash
  RETURNING
    p_hostname_hash,
    tp."HOSTNAME",
    tp."STATE",
    tp."OPEN_UNTIL",
    tp."OPEN_REASON",
    tp."LAST_ERROR_CODE",
    tp."VERSION",
    tp."HALF_OPEN_DEADLINE",
    v_attempt_granted,
    v_attempt_ticket;
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- PostgreSQL Stored Procedure: Report Breaker Sample
-- ========================================
CREATE OR REPLACE FUNCTION download_report_breaker_sample(
  p_hostname_hash TEXT,
  p_hostname TEXT,
  p_now INTEGER,
  p_sample NUMERIC,
  p_status_code INTEGER,
  p_open_cap_seconds INTEGER,
  p_open_threshold_percent INTEGER,
  p_close_threshold_percent INTEGER,
  p_ewma_span INTEGER,
  p_consecutive_threshold INTEGER,
  p_min_samples_before_ewma_open INTEGER,
  p_idle_reset_seconds INTEGER,
  p_half_open_success_threshold INTEGER,
  p_half_open_close_mode TEXT,
  p_half_open_max_seconds INTEGER,
  p_half_open_timeout_mode TEXT,
  p_retry_after_seconds INTEGER DEFAULT NULL,
  p_attempt_version BIGINT DEFAULT NULL,
  p_attempt_ticket INTEGER DEFAULT NULL
)
RETURNS TABLE(
  "HOSTNAME_HASH" TEXT,
  "HOSTNAME" TEXT,
  "STATE" TEXT,
  "OPEN_UNTIL" INTEGER,
  "EWMA_SCORE" NUMERIC,
  "TOTAL_SAMPLES" INTEGER,
  "SAMPLES_SINCE_RESET" INTEGER,
  "CONSECUTIVE_ERROR_COUNT" INTEGER,
  "SUCCESS_STREAK" INTEGER,
  "HALF_OPEN_DEADLINE" INTEGER,
  "LAST_SAMPLE_AT" INTEGER,
  "LAST_ERROR_CODE" INTEGER,
  "OPEN_REASON" TEXT,
  "LAST_OPEN_SECONDS" INTEGER,
  "VERSION" BIGINT
) AS $$
DECLARE
  v_now INTEGER := COALESCE(p_now, EXTRACT(EPOCH FROM NOW())::INTEGER);
  v_half_open_ticket_mask_limit CONSTANT INTEGER := 63;
  v_sample NUMERIC := CASE WHEN COALESCE(p_sample, 0) >= 1 THEN 1 ELSE 0 END;
  v_open_cap_seconds INTEGER;
  v_open_threshold NUMERIC;
  v_close_threshold NUMERIC;
  v_ewma_span INTEGER;
  v_alpha NUMERIC;
  v_consecutive_threshold INTEGER;
  v_min_samples_before_ewma_open INTEGER;
  v_idle_reset_seconds INTEGER;
  v_half_open_success_threshold INTEGER;
  v_half_open_close_mode TEXT;
  v_half_open_timeout_mode TEXT;

  v_hostname TEXT := p_hostname;
  v_state TEXT := 'closed';
  v_open_until INTEGER := NULL;
  v_ewma_score NUMERIC := 0;
  v_total_samples INTEGER := 0;
  v_samples_since_reset INTEGER := 0;
  v_consecutive_error_count INTEGER := 0;
  v_success_streak INTEGER := 0;
  v_half_open_budget INTEGER := 0;
  v_half_open_issued INTEGER := 0;
  v_half_open_reported_mask BIGINT := 0;
  v_half_open_success_count INTEGER := 0;
  v_half_open_deadline INTEGER := NULL;
  v_half_open_since INTEGER := NULL;
  v_last_sample_at INTEGER := NULL;
  v_last_error_code INTEGER := NULL;
  v_open_reason TEXT := NULL;
  v_last_open_seconds INTEGER := 0;
  v_version BIGINT := 0;
  v_initial_state TEXT := 'closed';

  v_locked BOOLEAN := FALSE;
  v_locked_row_count INTEGER := 0;
  v_should_open BOOLEAN := FALSE;
  v_should_close BOOLEAN := FALSE;
  v_half_open_timed_out BOOLEAN := FALSE;
  v_open_seconds INTEGER := 0;
  v_timeout_open_seconds INTEGER := 0;
  v_ticket_mask BIGINT := 0;
  v_required_report_mask BIGINT := 0;
BEGIN
  IF p_hostname_hash IS NULL OR p_hostname_hash = '' THEN
    RETURN;
  END IF;

  IF p_open_cap_seconds IS NULL
    OR p_open_threshold_percent IS NULL
    OR p_close_threshold_percent IS NULL
    OR p_ewma_span IS NULL
    OR p_consecutive_threshold IS NULL
    OR p_min_samples_before_ewma_open IS NULL
    OR p_idle_reset_seconds IS NULL
    OR p_half_open_success_threshold IS NULL
    OR p_half_open_close_mode IS NULL
    OR BTRIM(p_half_open_close_mode) = ''
    OR p_half_open_max_seconds IS NULL
    OR p_half_open_timeout_mode IS NULL
    OR BTRIM(p_half_open_timeout_mode) = '' THEN
    RAISE EXCEPTION 'download_report_breaker_sample requires non-null breaker thresholds';
  END IF;

  v_open_cap_seconds := GREATEST(1, p_open_cap_seconds);
  v_open_threshold := GREATEST(0, p_open_threshold_percent) / 100.0;
  v_close_threshold := GREATEST(0, p_close_threshold_percent) / 100.0;
  v_ewma_span := GREATEST(1, p_ewma_span);
  v_alpha := 2.0 / (v_ewma_span + 1.0);
  v_consecutive_threshold := GREATEST(1, p_consecutive_threshold);
  v_min_samples_before_ewma_open := GREATEST(1, p_min_samples_before_ewma_open);
  v_idle_reset_seconds := GREATEST(0, p_idle_reset_seconds);
  v_half_open_success_threshold := GREATEST(1, p_half_open_success_threshold);
  v_half_open_close_mode := LOWER(BTRIM(p_half_open_close_mode));
  v_half_open_timeout_mode := LOWER(BTRIM(p_half_open_timeout_mode));
  IF v_half_open_close_mode NOT IN ('and', 'or') THEN
    RAISE EXCEPTION 'download_report_breaker_sample invalid p_half_open_close_mode: %', p_half_open_close_mode;
  END IF;
  IF v_half_open_timeout_mode NOT IN ('open', 'close', 'partial-close') THEN
    RAISE EXCEPTION 'download_report_breaker_sample invalid p_half_open_timeout_mode: %', p_half_open_timeout_mode;
  END IF;

  WHILE NOT v_locked LOOP
    SELECT
      tp."HOSTNAME",
      tp."STATE",
      tp."OPEN_UNTIL",
      tp."EWMA_SCORE",
      tp."TOTAL_SAMPLES",
      tp."SAMPLES_SINCE_RESET",
      tp."CONSECUTIVE_ERROR_COUNT",
      tp."SUCCESS_STREAK",
      tp."HALF_OPEN_BUDGET",
      tp."HALF_OPEN_ISSUED",
      tp."HALF_OPEN_REPORTED_MASK",
      tp."HALF_OPEN_SUCCESS_COUNT",
      tp."HALF_OPEN_DEADLINE",
      tp."HALF_OPEN_SINCE",
      tp."LAST_SAMPLE_AT",
      tp."LAST_ERROR_CODE",
      tp."OPEN_REASON",
      tp."LAST_OPEN_SECONDS",
      tp."VERSION"
    INTO
      v_hostname,
      v_state,
      v_open_until,
      v_ewma_score,
      v_total_samples,
      v_samples_since_reset,
      v_consecutive_error_count,
      v_success_streak,
      v_half_open_budget,
      v_half_open_issued,
      v_half_open_reported_mask,
      v_half_open_success_count,
      v_half_open_deadline,
      v_half_open_since,
      v_last_sample_at,
      v_last_error_code,
      v_open_reason,
      v_last_open_seconds,
      v_version
    FROM "THROTTLE_PROTECTION" AS tp
    WHERE tp."HOSTNAME_HASH" = p_hostname_hash
    FOR UPDATE;

    GET DIAGNOSTICS v_locked_row_count = ROW_COUNT;
    v_locked := v_locked_row_count > 0;

    IF NOT v_locked THEN
      INSERT INTO "THROTTLE_PROTECTION" ("HOSTNAME_HASH", "HOSTNAME", "STATE")
      VALUES (p_hostname_hash, COALESCE(NULLIF(p_hostname, ''), p_hostname_hash), 'closed')
      ON CONFLICT ON CONSTRAINT "THROTTLE_PROTECTION_pkey" DO NOTHING;
    END IF;
  END LOOP;

  v_hostname := COALESCE(NULLIF(p_hostname, ''), v_hostname, p_hostname_hash);
  v_state := COALESCE(NULLIF(v_state, ''), 'closed');
  v_ewma_score := COALESCE(v_ewma_score, 0);
  v_total_samples := COALESCE(v_total_samples, 0);
  v_samples_since_reset := COALESCE(v_samples_since_reset, 0);
  v_consecutive_error_count := COALESCE(v_consecutive_error_count, 0);
  v_success_streak := COALESCE(v_success_streak, 0);
  v_half_open_budget := COALESCE(v_half_open_budget, 0);
  v_half_open_issued := COALESCE(v_half_open_issued, 0);
  v_half_open_reported_mask := COALESCE(v_half_open_reported_mask, 0);
  v_half_open_success_count := COALESCE(v_half_open_success_count, 0);
  v_last_open_seconds := COALESCE(v_last_open_seconds, 0);
  v_version := COALESCE(v_version, 0);
  v_initial_state := v_state;

  IF v_half_open_budget > v_half_open_ticket_mask_limit
    OR v_half_open_issued > v_half_open_ticket_mask_limit
    OR v_half_open_reported_mask < 0 THEN
    RAISE EXCEPTION 'download_report_breaker_sample half-open ticket state exceeds BIGINT mask capacity';
  END IF;

  IF v_half_open_issued > 0 THEN
    v_required_report_mask := CASE
      WHEN v_half_open_issued = v_half_open_ticket_mask_limit THEN 9223372036854775807::BIGINT
      ELSE ((1::BIGINT << (v_half_open_issued - 1)) - 1) | (1::BIGINT << (v_half_open_issued - 1))
    END;
  END IF;

  IF p_attempt_version IS NOT NULL THEN
    IF v_state <> 'half_open' OR p_attempt_version <> v_version THEN
      RETURN QUERY SELECT
        p_hostname_hash,
        v_hostname,
        v_state,
        v_open_until,
        v_ewma_score,
        v_total_samples,
        v_samples_since_reset,
        v_consecutive_error_count,
        v_success_streak,
        v_half_open_deadline,
        v_last_sample_at,
        v_last_error_code,
        v_open_reason,
        v_last_open_seconds,
        v_version;
      RETURN;
    END IF;

    IF p_attempt_ticket IS NULL OR p_attempt_ticket < 1 OR p_attempt_ticket > v_half_open_issued OR p_attempt_ticket > v_half_open_ticket_mask_limit THEN
      RETURN QUERY SELECT
        p_hostname_hash,
        v_hostname,
        v_state,
        v_open_until,
        v_ewma_score,
        v_total_samples,
        v_samples_since_reset,
        v_consecutive_error_count,
        v_success_streak,
        v_half_open_deadline,
        v_last_sample_at,
        v_last_error_code,
        v_open_reason,
        v_last_open_seconds,
        v_version;
      RETURN;
    END IF;

    v_ticket_mask := (1::BIGINT << (p_attempt_ticket - 1));
    IF (v_half_open_reported_mask & v_ticket_mask) <> 0 THEN
      RETURN QUERY SELECT
        p_hostname_hash,
        v_hostname,
        v_state,
        v_open_until,
        v_ewma_score,
        v_total_samples,
        v_samples_since_reset,
        v_consecutive_error_count,
        v_success_streak,
        v_half_open_deadline,
        v_last_sample_at,
        v_last_error_code,
        v_open_reason,
        v_last_open_seconds,
        v_version;
      RETURN;
    END IF;

    v_half_open_reported_mask := v_half_open_reported_mask | v_ticket_mask;
  ELSIF v_state = 'half_open' THEN
    RETURN QUERY SELECT
      p_hostname_hash,
      v_hostname,
      v_state,
      v_open_until,
      v_ewma_score,
      v_total_samples,
      v_samples_since_reset,
      v_consecutive_error_count,
      v_success_streak,
      v_half_open_deadline,
      v_last_sample_at,
      v_last_error_code,
      v_open_reason,
      v_last_open_seconds,
      v_version;
    RETURN;
  END IF;

  IF v_state = 'closed' AND v_idle_reset_seconds > 0 AND v_last_sample_at IS NOT NULL AND (v_now - v_last_sample_at) >= v_idle_reset_seconds THEN
    v_ewma_score := 0;
    v_consecutive_error_count := 0;
    v_success_streak := 0;
    v_samples_since_reset := 0;
    v_last_error_code := NULL;
    v_open_reason := NULL;
    v_last_open_seconds := 0;
  END IF;

  IF v_state = 'closed' THEN
    v_open_until := NULL;
    v_success_streak := 0;
    v_half_open_since := NULL;
    v_half_open_budget := 0;
    v_half_open_issued := 0;
    v_half_open_reported_mask := 0;
    v_half_open_success_count := 0;
    v_half_open_deadline := NULL;
  ELSIF v_state = 'open' AND v_open_until IS NOT NULL AND v_open_until <= v_now THEN
    NULL;
  END IF;

  v_half_open_timed_out := v_state = 'half_open'
    AND v_half_open_deadline IS NOT NULL
    AND v_half_open_deadline <= v_now;

  IF v_state = 'half_open' AND v_half_open_timed_out THEN
    v_timeout_open_seconds := CASE
      WHEN v_last_open_seconds > 0 THEN v_last_open_seconds
      ELSE 1
    END;

    IF v_half_open_timeout_mode = 'open' THEN
      v_state := 'open';
      v_open_until := v_now + v_timeout_open_seconds;
      v_last_open_seconds := v_timeout_open_seconds;
    ELSIF v_half_open_timeout_mode = 'close' THEN
      v_state := 'closed';
      v_open_until := NULL;
      v_ewma_score := 0;
      v_consecutive_error_count := 0;
      v_success_streak := 0;
      v_samples_since_reset := 0;
      v_last_error_code := NULL;
      v_open_reason := NULL;
      v_last_open_seconds := 0;
    ELSE
      IF v_half_open_success_count > 0 THEN
        v_state := 'closed';
        v_open_until := NULL;
        v_ewma_score := 0;
        v_consecutive_error_count := 0;
        v_success_streak := 0;
        v_samples_since_reset := 0;
        v_last_error_code := NULL;
        v_open_reason := NULL;
        v_last_open_seconds := 0;
      ELSE
        v_state := 'open';
        v_open_until := v_now + v_timeout_open_seconds;
        v_last_open_seconds := v_timeout_open_seconds;
      END IF;
    END IF;

    v_half_open_since := NULL;
    v_half_open_budget := 0;
    v_half_open_issued := 0;
    v_half_open_reported_mask := 0;
    v_half_open_success_count := 0;
    v_half_open_deadline := NULL;
  ELSE
    v_ewma_score := (v_alpha * v_sample) + ((1 - v_alpha) * v_ewma_score);
    v_total_samples := v_total_samples + 1;
    v_samples_since_reset := v_samples_since_reset + 1;
    v_last_sample_at := v_now;

    IF v_sample = 1 THEN
      v_consecutive_error_count := v_consecutive_error_count + 1;
      v_success_streak := 0;
      v_last_error_code := p_status_code;
      v_should_open := v_state = 'half_open'
        OR v_state = 'open'
        OR v_consecutive_error_count >= v_consecutive_threshold
        OR (
          v_samples_since_reset >= v_min_samples_before_ewma_open
          AND v_ewma_score >= v_open_threshold
        );

      IF v_should_open THEN
        IF p_retry_after_seconds IS NOT NULL AND p_retry_after_seconds > 0 THEN
          v_open_seconds := LEAST(v_open_cap_seconds, GREATEST(1, p_retry_after_seconds));
        ELSIF v_last_open_seconds > 0 THEN
          v_open_seconds := LEAST(v_open_cap_seconds, v_last_open_seconds * 2);
        ELSE
          v_open_seconds := 1;
        END IF;

        v_state := 'open';
        v_open_until := v_now + v_open_seconds;
        v_half_open_since := NULL;
        v_half_open_budget := 0;
        v_half_open_issued := 0;
        v_half_open_reported_mask := 0;
        v_half_open_success_count := 0;
        v_half_open_deadline := NULL;
        v_open_reason := CASE
          WHEN p_status_code IS NOT NULL THEN 'http_' || p_status_code::TEXT
          ELSE 'error_sample'
        END;
        v_last_open_seconds := v_open_seconds;
      ELSIF v_state = 'closed' THEN
        v_open_until := NULL;
        v_open_reason := NULL;
      END IF;
    ELSE
      v_consecutive_error_count := 0;

      IF v_state = 'half_open' THEN
        v_success_streak := v_success_streak + 1;
        v_half_open_success_count := v_half_open_success_count + 1;
        v_should_close := CASE
          WHEN v_half_open_close_mode = 'or' THEN
            v_half_open_success_count >= v_half_open_success_threshold
            OR v_ewma_score <= v_close_threshold
          ELSE
            v_half_open_success_count >= v_half_open_success_threshold
            AND v_ewma_score <= v_close_threshold
        END;

        IF v_should_close THEN
          v_state := 'closed';
          v_open_until := NULL;
          v_ewma_score := 0;
          v_consecutive_error_count := 0;
          v_success_streak := 0;
          v_samples_since_reset := 0;
          v_last_error_code := NULL;
          v_open_reason := NULL;
          v_last_open_seconds := 0;
          v_half_open_since := NULL;
          v_half_open_budget := 0;
          v_half_open_issued := 0;
          v_half_open_reported_mask := 0;
          v_half_open_success_count := 0;
          v_half_open_deadline := NULL;
        ELSIF v_half_open_issued = v_half_open_budget
          AND v_half_open_issued > 0
          AND (v_half_open_reported_mask & v_required_report_mask) = v_required_report_mask THEN
          v_timeout_open_seconds := CASE
            WHEN v_last_open_seconds > 0 THEN v_last_open_seconds
            ELSE 1
          END;
          v_state := 'open';
          v_open_until := v_now + v_timeout_open_seconds;
          v_last_open_seconds := v_timeout_open_seconds;
        END IF;
      ELSIF v_state = 'closed' THEN
        v_open_until := NULL;
        v_success_streak := 0;
        IF v_ewma_score <= v_close_threshold THEN
          v_open_reason := NULL;
        END IF;
        IF v_ewma_score = 0 THEN
          v_last_error_code := NULL;
        END IF;
      END IF;
    END IF;
  END IF;

  IF v_state = 'open' OR v_state = 'closed' THEN
    v_half_open_since := NULL;
    v_half_open_budget := 0;
    v_half_open_issued := 0;
    v_half_open_reported_mask := 0;
    v_half_open_success_count := 0;
    v_half_open_deadline := NULL;
  END IF;

  IF v_state IS DISTINCT FROM v_initial_state THEN
    v_version := v_version + 1;
  END IF;

  RETURN QUERY
  UPDATE "THROTTLE_PROTECTION" AS tp SET
    "HOSTNAME" = v_hostname,
    "STATE" = v_state,
    "OPEN_UNTIL" = v_open_until,
    "EWMA_SCORE" = v_ewma_score,
    "TOTAL_SAMPLES" = v_total_samples,
    "SAMPLES_SINCE_RESET" = v_samples_since_reset,
    "CONSECUTIVE_ERROR_COUNT" = v_consecutive_error_count,
    "SUCCESS_STREAK" = v_success_streak,
    "HALF_OPEN_SINCE" = v_half_open_since,
    "HALF_OPEN_BUDGET" = v_half_open_budget,
    "HALF_OPEN_ISSUED" = v_half_open_issued,
    "HALF_OPEN_REPORTED_MASK" = v_half_open_reported_mask,
    "HALF_OPEN_SUCCESS_COUNT" = v_half_open_success_count,
    "HALF_OPEN_DEADLINE" = v_half_open_deadline,
    "LAST_SAMPLE_AT" = v_last_sample_at,
    "LAST_ERROR_CODE" = v_last_error_code,
    "OPEN_REASON" = v_open_reason,
    "LAST_OPEN_SECONDS" = v_last_open_seconds,
    "VERSION" = v_version
  WHERE tp."HOSTNAME_HASH" = p_hostname_hash
  RETURNING
    p_hostname_hash,
    tp."HOSTNAME",
    tp."STATE",
    tp."OPEN_UNTIL",
    tp."EWMA_SCORE",
    tp."TOTAL_SAMPLES",
    tp."SAMPLES_SINCE_RESET",
    tp."CONSECUTIVE_ERROR_COUNT",
    tp."SUCCESS_STREAK",
    tp."HALF_OPEN_DEADLINE",
    tp."LAST_SAMPLE_AT",
    tp."LAST_ERROR_CODE",
    tp."OPEN_REASON",
    tp."LAST_OPEN_SECONDS",
    tp."VERSION";
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION download_settle_breaker_attempt(
  p_hostname_hash TEXT,
  p_hostname TEXT,
  p_attempt_version BIGINT,
  p_attempt_ticket INTEGER,
  p_now INTEGER DEFAULT NULL
)
RETURNS TABLE(
  "HOSTNAME_HASH" TEXT,
  "HOSTNAME" TEXT,
  "STATE" TEXT,
  "OPEN_UNTIL" INTEGER,
  "EWMA_SCORE" NUMERIC,
  "TOTAL_SAMPLES" INTEGER,
  "SAMPLES_SINCE_RESET" INTEGER,
  "CONSECUTIVE_ERROR_COUNT" INTEGER,
  "SUCCESS_STREAK" INTEGER,
  "HALF_OPEN_DEADLINE" INTEGER,
  "LAST_SAMPLE_AT" INTEGER,
  "LAST_ERROR_CODE" INTEGER,
  "OPEN_REASON" TEXT,
  "LAST_OPEN_SECONDS" INTEGER,
  "VERSION" BIGINT
) AS $$
DECLARE
  v_now INTEGER := COALESCE(p_now, EXTRACT(EPOCH FROM NOW())::INTEGER);
  v_half_open_ticket_mask_limit CONSTANT INTEGER := 63;
  v_hostname TEXT := p_hostname;
  v_state TEXT := 'closed';
  v_open_until INTEGER := NULL;
  v_ewma_score NUMERIC := 0;
  v_total_samples INTEGER := 0;
  v_samples_since_reset INTEGER := 0;
  v_consecutive_error_count INTEGER := 0;
  v_success_streak INTEGER := 0;
  v_half_open_budget INTEGER := 0;
  v_half_open_issued INTEGER := 0;
  v_half_open_reported_mask BIGINT := 0;
  v_half_open_success_count INTEGER := 0;
  v_half_open_deadline INTEGER := NULL;
  v_last_sample_at INTEGER := NULL;
  v_last_error_code INTEGER := NULL;
  v_open_reason TEXT := NULL;
  v_last_open_seconds INTEGER := 0;
  v_version BIGINT := 0;
  v_ticket_mask BIGINT := 0;
  v_locked BOOLEAN := FALSE;
  v_locked_row_count INTEGER := 0;
BEGIN
  IF p_hostname_hash IS NULL OR p_hostname_hash = '' OR p_attempt_version IS NULL THEN
    RETURN;
  END IF;

  IF p_attempt_ticket IS NULL OR p_attempt_ticket < 1 OR p_attempt_ticket > v_half_open_ticket_mask_limit THEN
    RETURN QUERY
    SELECT
      p_hostname_hash,
      COALESCE(NULLIF(p_hostname, ''), p_hostname_hash),
      'closed'::TEXT,
      NULL::INTEGER,
      0::NUMERIC,
      0::INTEGER,
      0::INTEGER,
      0::INTEGER,
      0::INTEGER,
      NULL::INTEGER,
      NULL::INTEGER,
      NULL::INTEGER,
      NULL::TEXT,
      0::INTEGER,
      0::BIGINT;
    RETURN;
  END IF;

  WHILE NOT v_locked LOOP
    SELECT
      tp."HOSTNAME",
      tp."STATE",
      tp."OPEN_UNTIL",
      tp."EWMA_SCORE",
      tp."TOTAL_SAMPLES",
      tp."SAMPLES_SINCE_RESET",
      tp."CONSECUTIVE_ERROR_COUNT",
      tp."SUCCESS_STREAK",
      tp."HALF_OPEN_BUDGET",
      tp."HALF_OPEN_ISSUED",
      tp."HALF_OPEN_REPORTED_MASK",
      tp."HALF_OPEN_SUCCESS_COUNT",
      tp."HALF_OPEN_DEADLINE",
      tp."LAST_SAMPLE_AT",
      tp."LAST_ERROR_CODE",
      tp."OPEN_REASON",
      tp."LAST_OPEN_SECONDS",
      tp."VERSION"
    INTO
      v_hostname,
      v_state,
      v_open_until,
      v_ewma_score,
      v_total_samples,
      v_samples_since_reset,
      v_consecutive_error_count,
      v_success_streak,
      v_half_open_budget,
      v_half_open_issued,
      v_half_open_reported_mask,
      v_half_open_success_count,
      v_half_open_deadline,
      v_last_sample_at,
      v_last_error_code,
      v_open_reason,
      v_last_open_seconds,
      v_version
    FROM "THROTTLE_PROTECTION" AS tp
    WHERE tp."HOSTNAME_HASH" = p_hostname_hash
    FOR UPDATE;

    GET DIAGNOSTICS v_locked_row_count = ROW_COUNT;
    v_locked := v_locked_row_count > 0;

    IF NOT v_locked THEN
      INSERT INTO "THROTTLE_PROTECTION" ("HOSTNAME_HASH", "HOSTNAME", "STATE")
      VALUES (p_hostname_hash, COALESCE(NULLIF(p_hostname, ''), p_hostname_hash), 'closed')
      ON CONFLICT ON CONSTRAINT "THROTTLE_PROTECTION_pkey" DO NOTHING;
    END IF;
  END LOOP;

  IF v_state = 'half_open' AND p_attempt_version = v_version AND p_attempt_ticket <= v_half_open_issued THEN
    v_ticket_mask := (1::BIGINT << (p_attempt_ticket - 1));
    v_half_open_reported_mask := COALESCE(v_half_open_reported_mask, 0) | v_ticket_mask;

    UPDATE "THROTTLE_PROTECTION" AS tp SET
      "HALF_OPEN_REPORTED_MASK" = v_half_open_reported_mask
    WHERE tp."HOSTNAME_HASH" = p_hostname_hash;
  END IF;

  RETURN QUERY
  SELECT
    p_hostname_hash,
    COALESCE(v_hostname, COALESCE(NULLIF(p_hostname, ''), p_hostname_hash)),
    v_state,
    v_open_until,
    v_ewma_score,
    v_total_samples,
    v_samples_since_reset,
    v_consecutive_error_count,
    v_success_streak,
    v_half_open_deadline,
    v_last_sample_at,
    v_last_error_code,
    v_open_reason,
    v_last_open_seconds,
    v_version;
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- Download IP Rate Limit Table Schema
-- ========================================
CREATE TABLE IF NOT EXISTS "DOWNLOAD_IP_RATELIMIT_TABLE" (
  "IP_HASH" TEXT PRIMARY KEY,
  "IP_RANGE" TEXT NOT NULL,
  "ACCESS_COUNT" INTEGER NOT NULL,
  "LAST_WINDOW_TIME" INTEGER NOT NULL,
  "BLOCK_UNTIL" INTEGER
);

CREATE INDEX IF NOT EXISTS idx_download_rate_limit_window
  ON "DOWNLOAD_IP_RATELIMIT_TABLE"("LAST_WINDOW_TIME");
CREATE INDEX IF NOT EXISTS idx_download_rate_limit_block
  ON "DOWNLOAD_IP_RATELIMIT_TABLE"("BLOCK_UNTIL")
  WHERE "BLOCK_UNTIL" IS NOT NULL;


-- ========================================
-- PostgreSQL Stored Procedure: Atomic UPSERT (Rate Limit)
-- ========================================
CREATE OR REPLACE FUNCTION download_upsert_rate_limit(
  p_ip_hash TEXT,
  p_ip_range TEXT,
  p_now INTEGER,
  p_window_seconds INTEGER,
  p_limit INTEGER,
  p_block_seconds INTEGER,
  p_table_name TEXT DEFAULT 'DOWNLOAD_IP_RATELIMIT_TABLE'
)
RETURNS TABLE(
  "ACCESS_COUNT" INTEGER,
  "LAST_WINDOW_TIME" INTEGER,
  "BLOCK_UNTIL" INTEGER
) AS $$
DECLARE
  sql TEXT;
BEGIN
  sql := format(
    'INSERT INTO %1$I ("IP_HASH", "IP_RANGE", "ACCESS_COUNT", "LAST_WINDOW_TIME", "BLOCK_UNTIL")
     VALUES ($1, $2, 1, $3, NULL)
     ON CONFLICT ("IP_HASH") DO UPDATE SET
       "ACCESS_COUNT" = CASE
         WHEN %1$I."BLOCK_UNTIL" IS NOT NULL AND %1$I."BLOCK_UNTIL" > $3 THEN %1$I."ACCESS_COUNT"
         WHEN $3 - %1$I."LAST_WINDOW_TIME" >= $4 THEN 1
         WHEN %1$I."BLOCK_UNTIL" IS NOT NULL AND %1$I."BLOCK_UNTIL" <= $3 THEN 1
         WHEN %1$I."ACCESS_COUNT" >= $5 THEN %1$I."ACCESS_COUNT"
         ELSE %1$I."ACCESS_COUNT" + 1
       END,
       "LAST_WINDOW_TIME" = CASE
         WHEN %1$I."BLOCK_UNTIL" IS NOT NULL AND %1$I."BLOCK_UNTIL" > $3 THEN %1$I."LAST_WINDOW_TIME"
         WHEN $3 - %1$I."LAST_WINDOW_TIME" >= $4 THEN $3
         WHEN %1$I."BLOCK_UNTIL" IS NOT NULL AND %1$I."BLOCK_UNTIL" <= $3 THEN $3
         ELSE %1$I."LAST_WINDOW_TIME"
       END,
       "BLOCK_UNTIL" = CASE
         WHEN %1$I."BLOCK_UNTIL" IS NOT NULL AND %1$I."BLOCK_UNTIL" > $3 THEN %1$I."BLOCK_UNTIL"
         WHEN %1$I."BLOCK_UNTIL" IS NOT NULL AND %1$I."BLOCK_UNTIL" <= $3 THEN NULL
         WHEN (%1$I."BLOCK_UNTIL" IS NULL OR %1$I."BLOCK_UNTIL" <= $3)
              AND (
                CASE
                  WHEN $3 - %1$I."LAST_WINDOW_TIME" >= $4 THEN 1
                  WHEN %1$I."BLOCK_UNTIL" IS NOT NULL AND %1$I."BLOCK_UNTIL" <= $3 THEN 1
                  WHEN %1$I."ACCESS_COUNT" >= $5 THEN %1$I."ACCESS_COUNT"
                  ELSE %1$I."ACCESS_COUNT" + 1
                END
              ) >= $5
              AND $6 > 0 THEN $3 + $6
         ELSE %1$I."BLOCK_UNTIL"
       END
     RETURNING "ACCESS_COUNT", "LAST_WINDOW_TIME", "BLOCK_UNTIL"',
    p_table_name
  );

  RETURN QUERY EXECUTE sql USING p_ip_hash, p_ip_range, p_now, p_window_seconds, p_limit, p_block_seconds;
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- Optional: Rate Limit Cleanup Function (PostgreSQL)
-- ========================================
CREATE OR REPLACE FUNCTION download_cleanup_ip_ratelimit(
  p_window_seconds INTEGER,
  p_table_name TEXT DEFAULT 'DOWNLOAD_IP_RATELIMIT_TABLE'
)
RETURNS INTEGER AS $$
DECLARE
  deleted_count INTEGER;
  sql TEXT;
  cutoff INTEGER;
  now_ts INTEGER;
BEGIN
  now_ts := EXTRACT(EPOCH FROM NOW())::INTEGER;
  cutoff := now_ts - (p_window_seconds * 2);

  sql := format(
    'DELETE FROM %1$I
     WHERE "LAST_WINDOW_TIME" < $1
       AND ("BLOCK_UNTIL" IS NULL OR "BLOCK_UNTIL" < $2)',
    p_table_name
  );

  EXECUTE sql USING cutoff, now_ts;
  GET DIAGNOSTICS deleted_count = ROW_COUNT;

  RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- Unified Check Function (Rate Limit + Cache + Throttle)
-- ========================================
-- Purpose: Combines Rate Limit + Cache + Throttle checks in a single database round-trip

CREATE OR REPLACE FUNCTION download_unified_check(
  -- Cache parameters
  p_path_hash TEXT,
  p_cache_ttl INTEGER,
  p_cache_enabled BOOLEAN,
  p_cache_table_name TEXT,

  -- Rate limit parameters
  p_ip_hash TEXT,
  p_ip_range TEXT,
  p_window_seconds INTEGER,
  p_limit INTEGER,
  p_block_seconds INTEGER,
  p_ratelimit_table_name TEXT,

  -- Throttle parameters
  p_throttle_hostname_hash TEXT,

  -- General parameters
  p_now BIGINT DEFAULT NULL,

  -- Last active parameters
  p_idle_timeout INTEGER DEFAULT 0,
  p_last_active_table_name TEXT DEFAULT 'DOWNLOAD_LAST_ACTIVE_TABLE'
)
RETURNS TABLE(
  -- Cache result
  cache_link_data TEXT,
  cache_timestamp INTEGER,
  cache_hostname_hash TEXT,

  -- Rate limit result
  rate_access_count INTEGER,
  rate_last_window_time INTEGER,
  rate_block_until INTEGER,

  -- Throttle result
  throttle_record_exists BOOLEAN,
  throttle_state TEXT,
  throttle_open_until INTEGER,
  throttle_reason TEXT,
  throttle_version BIGINT,
  throttle_last_error_code INTEGER,

  -- Last active result
  active_last_access_time INTEGER,
  active_total_access_count INTEGER
) AS $$
DECLARE
  v_now BIGINT;
  v_cache_record RECORD;
  v_rate_record RECORD;
  v_throttle_record RECORD;
  v_cache_hostname_hash TEXT;
  v_throttle_hostname_hash TEXT;
  v_active_record RECORD;

  v_cache_link_data TEXT := NULL;
  v_cache_timestamp INTEGER := NULL;

  v_rate_access_count INTEGER := NULL;
  v_rate_last_window_time INTEGER := NULL;
  v_rate_block_until INTEGER := NULL;

  v_throttle_record_exists BOOLEAN := FALSE;
  v_throttle_state TEXT := NULL;
  v_throttle_open_until INTEGER := NULL;
  v_throttle_reason TEXT := NULL;
  v_throttle_version BIGINT := NULL;
  v_throttle_last_error_code INTEGER := NULL;
  v_throttle_row_count INTEGER := 0;

  v_active_last_access_time INTEGER := NULL;
  v_active_total_access_count INTEGER := NULL;
  v_actual_path_hash TEXT := NULL;
BEGIN
  v_now := COALESCE(p_now, EXTRACT(EPOCH FROM NOW())::BIGINT);

  -- Step 1: Cache lookup
  v_actual_path_hash := p_path_hash;

  IF p_cache_enabled THEN
    EXECUTE format('SELECT "LINK_DATA", "TIMESTAMP", "HOSTNAME_HASH" FROM %1$I WHERE "PATH_HASH" = $1', p_cache_table_name)
      INTO v_cache_record
      USING v_actual_path_hash;

    IF v_cache_record."TIMESTAMP" IS NOT NULL AND (v_now - v_cache_record."TIMESTAMP") <= p_cache_ttl THEN
      v_cache_link_data := v_cache_record."LINK_DATA";
      v_cache_timestamp := v_cache_record."TIMESTAMP";
      v_cache_hostname_hash := v_cache_record."HOSTNAME_HASH";
    ELSE
      v_cache_link_data := NULL;
      v_cache_timestamp := NULL;
      v_cache_hostname_hash := NULL;
    END IF;
  END IF;

  -- Step 2: Rate limit upsert
  SELECT *
    INTO v_rate_record
  FROM download_upsert_rate_limit(
    p_ip_hash,
    p_ip_range,
    v_now::INTEGER,
    p_window_seconds,
    p_limit,
    p_block_seconds,
    p_ratelimit_table_name
  );

  v_rate_access_count := v_rate_record."ACCESS_COUNT";
  v_rate_last_window_time := v_rate_record."LAST_WINDOW_TIME";
  v_rate_block_until := v_rate_record."BLOCK_UNTIL";

  -- Step 3: Throttle lookup (provided hostname hash or cache hostname)
  v_throttle_hostname_hash := COALESCE(p_throttle_hostname_hash, v_cache_hostname_hash);
  IF v_throttle_hostname_hash IS NOT NULL THEN
    SELECT "STATE", "OPEN_UNTIL", "OPEN_REASON", "VERSION", "LAST_ERROR_CODE"
      INTO v_throttle_record
    FROM "THROTTLE_PROTECTION"
    WHERE "HOSTNAME_HASH" = v_throttle_hostname_hash;

    GET DIAGNOSTICS v_throttle_row_count = ROW_COUNT;
    IF v_throttle_row_count > 0 THEN
      v_throttle_record_exists := TRUE;
      v_throttle_state := v_throttle_record."STATE";
      v_throttle_open_until := v_throttle_record."OPEN_UNTIL";
      v_throttle_reason := v_throttle_record."OPEN_REASON";
      v_throttle_version := v_throttle_record."VERSION";
      v_throttle_last_error_code := v_throttle_record."LAST_ERROR_CODE";
    ELSE
      v_throttle_record_exists := FALSE;
      v_throttle_state := NULL;
      v_throttle_open_until := NULL;
      v_throttle_reason := NULL;
      v_throttle_version := NULL;
      v_throttle_last_error_code := NULL;
    END IF;
  ELSE
    v_throttle_record_exists := FALSE;
    v_throttle_state := NULL;
    v_throttle_open_until := NULL;
    v_throttle_reason := NULL;
    v_throttle_version := NULL;
    v_throttle_last_error_code := NULL;
  END IF;

  -- Step 4: Last active lookup
  EXECUTE format('SELECT "LAST_ACCESS_TIME", "TOTAL_ACCESS_COUNT" FROM %1$I WHERE "IP_HASH" = $1 AND "PATH_HASH" = $2 LIMIT 1', p_last_active_table_name)
    INTO v_active_record
    USING p_ip_hash, v_actual_path_hash;

  IF v_active_record."LAST_ACCESS_TIME" IS NOT NULL THEN
    v_active_last_access_time := v_active_record."LAST_ACCESS_TIME";
    v_active_total_access_count := v_active_record."TOTAL_ACCESS_COUNT";
  END IF;

  RETURN QUERY SELECT
    v_cache_link_data,
    v_cache_timestamp,
    v_cache_hostname_hash,
    v_rate_access_count,
    v_rate_last_window_time,
    v_rate_block_until,
    v_throttle_record_exists,
    v_throttle_state,
    v_throttle_open_until,
    v_throttle_reason,
    v_throttle_version,
    v_throttle_last_error_code,
    v_active_last_access_time,
    v_active_total_access_count;
END;
$$ LANGUAGE plpgsql;
-- ========================================
-- Fair Queue Table Schema (PostgreSQL)
-- ========================================
CREATE TABLE IF NOT EXISTS "fq_host_slot_pool" (
  "id" SERIAL PRIMARY KEY,
  "hostname_pattern" TEXT NOT NULL,
  "slot_index" INTEGER NOT NULL,
  "status" TEXT NOT NULL DEFAULT 'available',
  "ip_hash" TEXT,
  "locked_at" TIMESTAMP WITH TIME ZONE,
  "created_at" TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fq_host_slot_pool_unique
  ON "fq_host_slot_pool" ("hostname_pattern", "slot_index");

CREATE INDEX IF NOT EXISTS idx_fq_host_slot_pool_host_status
  ON "fq_host_slot_pool" ("hostname_pattern", "status");

CREATE TABLE IF NOT EXISTS "fq_site_slot_pool" (
  "id" SERIAL PRIMARY KEY,
  "hostname_pattern" TEXT NOT NULL,
  "site_bucket" TEXT NOT NULL,
  "slot_index" INTEGER NOT NULL,
  "status" TEXT NOT NULL DEFAULT 'available',
  "ip_hash" TEXT,
  "locked_at" TIMESTAMP WITH TIME ZONE,
  "created_at" TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fq_site_slot_pool_unique
  ON "fq_site_slot_pool" ("hostname_pattern", "site_bucket", "slot_index");

CREATE INDEX IF NOT EXISTS idx_fq_site_slot_pool_host_status
  ON "fq_site_slot_pool" ("hostname_pattern", "site_bucket", "status");

CREATE TABLE IF NOT EXISTS "fq_host_ip_cooldown" (
  "hostname_pattern" TEXT NOT NULL,
  "ip_hash" TEXT NOT NULL,
  "last_release_at" TIMESTAMP WITH TIME ZONE NOT NULL,
  "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  PRIMARY KEY ("hostname_pattern", "ip_hash")
);

CREATE INDEX IF NOT EXISTS idx_fq_host_ip_cooldown_ts
  ON "fq_host_ip_cooldown" ("last_release_at");

CREATE TABLE IF NOT EXISTS "fq_site_ip_cooldown" (
  "hostname_pattern" TEXT NOT NULL,
  "site_bucket" TEXT NOT NULL,
  "ip_hash" TEXT NOT NULL,
  "last_release_at" TIMESTAMP WITH TIME ZONE NOT NULL,
  "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  PRIMARY KEY ("hostname_pattern", "site_bucket", "ip_hash")
);

CREATE INDEX IF NOT EXISTS idx_fq_site_ip_cooldown_ts
  ON "fq_site_ip_cooldown" ("last_release_at");

-- ========================================
-- Fair Queue Slot RPCs
-- ========================================
CREATE OR REPLACE FUNCTION func_try_acquire_host_slot(
  p_hostname_pattern           TEXT,
  p_ip_hash                    TEXT,
  p_global_limit               INT,
  p_per_ip_limit               INT,
  p_zombie_timeout_seconds     INT,
  p_cooldown_seconds           INT
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
  v_now               TIMESTAMP WITH TIME ZONE := clock_timestamp();
  v_zombie_timeout    INTERVAL := (GREATEST(COALESCE(p_zombie_timeout_seconds, 0), 0)::TEXT || ' seconds')::INTERVAL;
  v_cooldown_interval INTERVAL := CASE
    WHEN p_cooldown_seconds > 0 THEN (p_cooldown_seconds::TEXT || ' seconds')::INTERVAL
    ELSE NULL
  END;
  v_slot_id           INT;
  v_current_ip_slots  INT := 0;
  v_last_release_at   TIMESTAMP WITH TIME ZONE;
  v_new_slot_index    INT;
BEGIN
  IF p_hostname_pattern IS NULL OR p_hostname_pattern = '' THEN
    RETURN NULL;
  END IF;

  PERFORM pg_advisory_xact_lock(1, hashtext(p_hostname_pattern));

  IF p_global_limit IS NOT NULL AND p_global_limit > 0 THEN
    INSERT INTO "fq_host_slot_pool" ("hostname_pattern", "slot_index", "status")
    SELECT p_hostname_pattern, gs.slot_index, 'available'
    FROM generate_series(1, p_global_limit) AS gs(slot_index)
    ON CONFLICT ("hostname_pattern", "slot_index") DO NOTHING;
  END IF;

  SELECT COUNT(*) INTO v_current_ip_slots
  FROM "fq_host_slot_pool"
  WHERE "hostname_pattern" = p_hostname_pattern
    AND "status" = 'locked'
    AND "ip_hash" = p_ip_hash
    AND "locked_at" IS NOT NULL
    AND "locked_at" >= (v_now - v_zombie_timeout);

  IF p_per_ip_limit > 0 THEN
    IF v_current_ip_slots >= p_per_ip_limit THEN
      RETURN 0;
    END IF;
  END IF;

  IF v_cooldown_interval IS NOT NULL
     AND p_ip_hash IS NOT NULL
     AND p_ip_hash <> ''
     AND p_per_ip_limit > 0
     AND v_current_ip_slots < p_per_ip_limit THEN
    SELECT "last_release_at"
      INTO v_last_release_at
    FROM "fq_host_ip_cooldown"
    WHERE "hostname_pattern" = p_hostname_pattern
      AND "ip_hash" = p_ip_hash;

    IF v_last_release_at IS NOT NULL
       AND v_last_release_at > (v_now - v_cooldown_interval) THEN
      RETURN 0;
    END IF;
  END IF;

  IF p_global_limit IS NOT NULL AND p_global_limit > 0 THEN
    SELECT "id" INTO v_slot_id
    FROM "fq_host_slot_pool"
    WHERE "hostname_pattern" = p_hostname_pattern
      AND "slot_index" <= p_global_limit
      AND (
        "status" = 'available'
        OR (
          "status" = 'locked'
          AND "locked_at" IS NOT NULL
          AND "locked_at" < (v_now - v_zombie_timeout)
        )
      )
    ORDER BY "slot_index"
    FOR UPDATE SKIP LOCKED
    LIMIT 1;
  ELSE
    SELECT "id" INTO v_slot_id
    FROM "fq_host_slot_pool"
    WHERE "hostname_pattern" = p_hostname_pattern
      AND (
        "status" = 'available'
        OR (
          "status" = 'locked'
          AND "locked_at" IS NOT NULL
          AND "locked_at" < (v_now - v_zombie_timeout)
        )
      )
    ORDER BY "slot_index"
    FOR UPDATE SKIP LOCKED
    LIMIT 1;
  END IF;

  IF v_slot_id IS NOT NULL THEN
    UPDATE "fq_host_slot_pool"
    SET "status" = 'locked',
        "ip_hash" = p_ip_hash,
        "locked_at" = v_now
    WHERE "id" = v_slot_id;

    RETURN v_slot_id;
  END IF;

  IF p_global_limit IS NOT NULL AND p_global_limit > 0 THEN
    RETURN -1;
  END IF;

  SELECT COALESCE(MAX("slot_index"), 0) + 1
    INTO v_new_slot_index
  FROM "fq_host_slot_pool"
  WHERE "hostname_pattern" = p_hostname_pattern;

  INSERT INTO "fq_host_slot_pool" ("hostname_pattern", "slot_index", "status", "ip_hash", "locked_at")
  VALUES (p_hostname_pattern, v_new_slot_index, 'locked', p_ip_hash, v_now)
  RETURNING "id" INTO v_slot_id;

  RETURN v_slot_id;
END;
$$;


-- ========================================
-- True Concurrency Lease And Counter RPCs
-- ========================================
CREATE TABLE IF NOT EXISTS concurrency_leases (
  lease_id uuid PRIMARY KEY,
  lease_token text NOT NULL,
  request_id text NOT NULL,
  hostname_hash text NOT NULL,
  hostname text NOT NULL,
  site_bucket text NOT NULL,
  ip_bucket text NOT NULL,
  hard_expire_at_ms bigint NOT NULL,
  expires_at_ms bigint NOT NULL,
  expires_at timestamptz NOT NULL,
  state text NOT NULL CHECK (state IN ('active', 'released', 'expired')),
  released_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS concurrency_leases_request_id_idx
  ON concurrency_leases (request_id);

CREATE INDEX IF NOT EXISTS concurrency_leases_scope_state_idx
  ON concurrency_leases (hostname_hash, site_bucket, ip_bucket, state, expires_at_ms);

CREATE TABLE IF NOT EXISTS concurrency_host_counters (
  hostname_hash text PRIMARY KEY,
  hostname text NOT NULL,
  active_count integer NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS concurrency_site_counters (
  hostname_hash text NOT NULL,
  site_bucket text NOT NULL,
  active_count integer NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (hostname_hash, site_bucket)
);

CREATE TABLE IF NOT EXISTS concurrency_site_ip_counters (
  hostname_hash text NOT NULL,
  site_bucket text NOT NULL,
  ip_bucket text NOT NULL,
  active_count integer NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (hostname_hash, site_bucket, ip_bucket)
);

CREATE OR REPLACE FUNCTION cq_make_uuid(p_seed text)
RETURNS uuid
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT (
    substr(md5(p_seed), 1, 8) || '-' ||
    substr(md5(p_seed), 9, 4) || '-' ||
    substr(md5(p_seed), 13, 4) || '-' ||
    substr(md5(p_seed), 17, 4) || '-' ||
    substr(md5(p_seed), 21, 12)
  )::uuid;
$$;

CREATE OR REPLACE FUNCTION cq_precheck(
  p_hostname_hash text,
  p_site_bucket text,
  p_ip_bucket text,
  p_host_max_in_flight integer DEFAULT 0,
  p_site_max_in_flight integer DEFAULT 0,
  p_site_ip_max_in_flight integer DEFAULT 0
)
RETURNS TABLE(result text, scope text, reason text, retry_after integer) AS $$
DECLARE
  v_now_ms bigint := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint;
  v_hostname_hash text := BTRIM(COALESCE(p_hostname_hash, ''));
  v_site_bucket text := COALESCE(NULLIF(BTRIM(COALESCE(p_site_bucket, '')), ''), 'unknown');
  v_ip_bucket text := COALESCE(NULLIF(BTRIM(COALESCE(p_ip_bucket, '')), ''), 'unknown');
  v_host_count integer := 0;
  v_site_count integer := 0;
  v_site_ip_count integer := 0;
  v_min_expires_at_ms bigint := NULL;
BEGIN
  IF v_hostname_hash = '' THEN
    RAISE EXCEPTION 'cq_precheck hostname_hash is required';
  END IF;

  IF COALESCE(p_host_max_in_flight, 0) > 0 THEN
    SELECT COUNT(*), MIN(l.expires_at_ms)
      INTO v_host_count, v_min_expires_at_ms
    FROM concurrency_leases AS l
    WHERE l.hostname_hash = v_hostname_hash
      AND l.state = 'active'
      AND l.expires_at_ms > v_now_ms;

    IF v_host_count >= p_host_max_in_flight THEN
      result := 'deny';
      scope := 'host';
      reason := 'full';
      retry_after := CASE
        WHEN v_min_expires_at_ms IS NOT NULL THEN GREATEST(1, CEIL((v_min_expires_at_ms - v_now_ms) / 1000.0)::integer)
        ELSE 1
      END;
      RETURN NEXT;
      RETURN;
    END IF;
  END IF;

  IF COALESCE(p_site_max_in_flight, 0) > 0 THEN
    SELECT COUNT(*), MIN(l.expires_at_ms)
      INTO v_site_count, v_min_expires_at_ms
    FROM concurrency_leases AS l
    WHERE l.hostname_hash = v_hostname_hash
      AND l.site_bucket = v_site_bucket
      AND l.state = 'active'
      AND l.expires_at_ms > v_now_ms;

    IF v_site_count >= p_site_max_in_flight THEN
      result := 'deny';
      scope := 'site';
      reason := 'full';
      retry_after := CASE
        WHEN v_min_expires_at_ms IS NOT NULL THEN GREATEST(1, CEIL((v_min_expires_at_ms - v_now_ms) / 1000.0)::integer)
        ELSE 1
      END;
      RETURN NEXT;
      RETURN;
    END IF;
  END IF;

  IF COALESCE(p_site_ip_max_in_flight, 0) > 0 THEN
    SELECT COUNT(*), MIN(l.expires_at_ms)
      INTO v_site_ip_count, v_min_expires_at_ms
    FROM concurrency_leases AS l
    WHERE l.hostname_hash = v_hostname_hash
      AND l.site_bucket = v_site_bucket
      AND l.ip_bucket = v_ip_bucket
      AND l.state = 'active'
      AND l.expires_at_ms > v_now_ms;

    IF v_site_ip_count >= p_site_ip_max_in_flight THEN
      result := 'deny';
      scope := 'site_ip';
      reason := 'full';
      retry_after := CASE
        WHEN v_min_expires_at_ms IS NOT NULL THEN GREATEST(1, CEIL((v_min_expires_at_ms - v_now_ms) / 1000.0)::integer)
        ELSE 1
      END;
      RETURN NEXT;
      RETURN;
    END IF;
  END IF;

  result := 'allow';
  scope := NULL;
  reason := NULL;
  retry_after := NULL;
  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cq_expire_scope(
  p_scope text,
  p_hostname_hash text,
  p_site_bucket text DEFAULT NULL,
  p_ip_bucket text DEFAULT NULL,
  p_now_ms bigint DEFAULT NULL,
  p_limit integer DEFAULT 500
)
RETURNS integer AS $$
DECLARE
  v_scope text := LOWER(BTRIM(COALESCE(p_scope, '')));
  v_now_ms bigint := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint;
  v_limit integer := GREATEST(COALESCE(p_limit, 500), 0);
  v_expired_count integer := 0;
  v_row record;
  v_site_lock record;
  v_site_ip_lock record;
BEGIN
  IF p_hostname_hash IS NULL OR BTRIM(p_hostname_hash) = '' OR v_limit = 0 THEN
    RETURN 0;
  END IF;

  IF v_scope NOT IN ('host', 'site', 'site_ip') THEN
    RAISE EXCEPTION 'cq_expire_scope invalid scope: %', p_scope;
  END IF;

  IF v_scope IN ('site', 'site_ip') AND (p_site_bucket IS NULL OR BTRIM(p_site_bucket) = '') THEN
    RETURN 0;
  END IF;

  IF v_scope = 'site_ip' AND (p_ip_bucket IS NULL OR BTRIM(p_ip_bucket) = '') THEN
    RETURN 0;
  END IF;

  IF v_scope = 'host' THEN
    PERFORM 1
      FROM concurrency_host_counters
      WHERE hostname_hash = p_hostname_hash
      FOR UPDATE;

    FOR v_site_lock IN
      SELECT DISTINCT site_bucket
      FROM (
        SELECT site_bucket, ip_bucket
        FROM concurrency_leases AS l
        WHERE l.state = 'active'
          AND l.hostname_hash = p_hostname_hash
          AND l.expires_at_ms <= v_now_ms
        ORDER BY l.expires_at_ms, l.lease_id
        LIMIT v_limit
      ) AS host_site_rows
      ORDER BY site_bucket
    LOOP
      PERFORM 1
        FROM concurrency_site_counters
        WHERE hostname_hash = p_hostname_hash
          AND site_bucket = v_site_lock.site_bucket
        FOR UPDATE;
    END LOOP;

    FOR v_site_ip_lock IN
      SELECT DISTINCT site_bucket, ip_bucket
      FROM (
        SELECT site_bucket, ip_bucket
        FROM concurrency_leases AS l
        WHERE l.state = 'active'
          AND l.hostname_hash = p_hostname_hash
          AND l.expires_at_ms <= v_now_ms
        ORDER BY l.expires_at_ms, l.lease_id
        LIMIT v_limit
      ) AS host_site_ip_rows
      ORDER BY site_bucket, ip_bucket
    LOOP
      PERFORM 1
        FROM concurrency_site_ip_counters
        WHERE hostname_hash = p_hostname_hash
          AND site_bucket = v_site_ip_lock.site_bucket
          AND ip_bucket = v_site_ip_lock.ip_bucket
        FOR UPDATE;
    END LOOP;
  ELSIF v_scope = 'site' THEN
    PERFORM 1
      FROM concurrency_host_counters
      WHERE hostname_hash = p_hostname_hash
      FOR UPDATE;
    PERFORM 1
      FROM concurrency_site_counters
      WHERE hostname_hash = p_hostname_hash
        AND site_bucket = p_site_bucket
      FOR UPDATE;

    FOR v_site_ip_lock IN
      SELECT DISTINCT ip_bucket
      FROM (
        SELECT ip_bucket
        FROM concurrency_leases AS l
        WHERE l.state = 'active'
          AND l.hostname_hash = p_hostname_hash
          AND l.site_bucket = p_site_bucket
          AND l.expires_at_ms <= v_now_ms
        ORDER BY l.expires_at_ms, l.lease_id
        LIMIT v_limit
      ) AS site_ip_rows
      ORDER BY ip_bucket
    LOOP
      PERFORM 1
        FROM concurrency_site_ip_counters
        WHERE hostname_hash = p_hostname_hash
          AND site_bucket = p_site_bucket
          AND ip_bucket = v_site_ip_lock.ip_bucket
        FOR UPDATE;
    END LOOP;
  ELSE
    PERFORM 1
      FROM concurrency_host_counters
      WHERE hostname_hash = p_hostname_hash
      FOR UPDATE;
    PERFORM 1
      FROM concurrency_site_counters
      WHERE hostname_hash = p_hostname_hash
        AND site_bucket = p_site_bucket
      FOR UPDATE;
    PERFORM 1
      FROM concurrency_site_ip_counters
      WHERE hostname_hash = p_hostname_hash
        AND site_bucket = p_site_bucket
        AND ip_bucket = p_ip_bucket
      FOR UPDATE;
  END IF;

  FOR v_row IN
    WITH expired_rows AS (
      SELECT l.lease_id, l.site_bucket, l.ip_bucket
      FROM concurrency_leases AS l
      WHERE l.state = 'active'
        AND l.hostname_hash = p_hostname_hash
        AND l.expires_at_ms <= v_now_ms
        AND (
          v_scope = 'host'
          OR (v_scope = 'site' AND l.site_bucket = p_site_bucket)
          OR (v_scope = 'site_ip' AND l.site_bucket = p_site_bucket AND l.ip_bucket = p_ip_bucket)
        )
      ORDER BY l.expires_at_ms, l.lease_id
      LIMIT v_limit
      FOR UPDATE SKIP LOCKED
    )
    UPDATE concurrency_leases AS l
    SET state = 'expired',
        released_at = COALESCE(released_at, to_timestamp(v_now_ms / 1000.0)),
        updated_at = now()
    FROM expired_rows
    WHERE l.lease_id = expired_rows.lease_id
    RETURNING expired_rows.site_bucket, expired_rows.ip_bucket
  LOOP
    v_expired_count := v_expired_count + 1;

    UPDATE concurrency_site_counters
    SET active_count = GREATEST(active_count - 1, 0),
        updated_at = now()
    WHERE hostname_hash = p_hostname_hash
      AND site_bucket = v_row.site_bucket;

    UPDATE concurrency_site_ip_counters
    SET active_count = GREATEST(active_count - 1, 0),
        updated_at = now()
    WHERE hostname_hash = p_hostname_hash
      AND site_bucket = v_row.site_bucket
      AND ip_bucket = v_row.ip_bucket;
  END LOOP;

  IF v_expired_count > 0 THEN
    UPDATE concurrency_host_counters
    SET active_count = GREATEST(active_count - v_expired_count, 0),
        updated_at = now()
    WHERE hostname_hash = p_hostname_hash;
  END IF;

  RETURN v_expired_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cq_release(
  p_lease_id uuid,
  p_lease_token text,
  p_reason text,
  p_now_ms bigint DEFAULT NULL
)
RETURNS TABLE(result text, reason text) AS $$
DECLARE
  v_now_ms bigint := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint;
  v_release_scope record;
  v_locked_lease record;
  v_release_scope_row_count bigint := 0;
  v_locked_lease_row_count bigint := 0;
BEGIN
  IF p_lease_id IS NULL THEN
    result := 'noop';
    reason := 'not_found';
    RETURN NEXT;
    RETURN;
  END IF;

  SELECT lease_id, hostname_hash, site_bucket, ip_bucket
    INTO v_release_scope
  FROM concurrency_leases
  WHERE lease_id = p_lease_id;

  GET DIAGNOSTICS v_release_scope_row_count = ROW_COUNT;

  IF v_release_scope_row_count = 0 THEN
    result := 'noop';
    reason := 'not_found';
    RETURN NEXT;
    RETURN;
  END IF;

  PERFORM cq_expire_scope('host', v_release_scope.hostname_hash, NULL, NULL, v_now_ms, 500);

  PERFORM 1
  FROM concurrency_host_counters
  WHERE hostname_hash = v_release_scope.hostname_hash
  FOR UPDATE;

  PERFORM 1
  FROM concurrency_site_counters
  WHERE hostname_hash = v_release_scope.hostname_hash
    AND site_bucket = v_release_scope.site_bucket
  FOR UPDATE;

  PERFORM 1
  FROM concurrency_site_ip_counters
  WHERE hostname_hash = v_release_scope.hostname_hash
    AND site_bucket = v_release_scope.site_bucket
    AND ip_bucket = v_release_scope.ip_bucket
  FOR UPDATE;

  SELECT *
    INTO v_locked_lease
  FROM concurrency_leases
  WHERE lease_id = p_lease_id
  FOR UPDATE;

  GET DIAGNOSTICS v_locked_lease_row_count = ROW_COUNT;

  IF v_locked_lease_row_count = 0 THEN
    result := 'noop';
    reason := 'not_found';
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_locked_lease.lease_token IS DISTINCT FROM p_lease_token THEN
    result := 'noop';
    reason := 'token_mismatch';
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_locked_lease.state = 'released' THEN
    result := 'noop';
    reason := 'already_released';
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_locked_lease.state = 'expired' THEN
    result := 'noop';
    reason := 'expired';
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_locked_lease.state <> 'active' THEN
    result := 'noop';
    reason := 'not_found';
    RETURN NEXT;
    RETURN;
  END IF;

  UPDATE concurrency_leases
  SET state = 'released',
      released_at = COALESCE(released_at, to_timestamp(v_now_ms / 1000.0)),
      updated_at = now()
  WHERE lease_id = p_lease_id;

  UPDATE concurrency_host_counters
  SET active_count = GREATEST(active_count - 1, 0),
      updated_at = now()
  WHERE hostname_hash = v_locked_lease.hostname_hash;

  UPDATE concurrency_site_counters
  SET active_count = GREATEST(active_count - 1, 0),
      updated_at = now()
  WHERE hostname_hash = v_locked_lease.hostname_hash
    AND site_bucket = v_locked_lease.site_bucket;

  UPDATE concurrency_site_ip_counters
  SET active_count = GREATEST(active_count - 1, 0),
      updated_at = now()
  WHERE hostname_hash = v_locked_lease.hostname_hash
    AND site_bucket = v_locked_lease.site_bucket
    AND ip_bucket = v_locked_lease.ip_bucket;

  result := 'released';
  reason := NULL;
  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cq_release_by_request(
  p_request_id text,
  p_hostname_hash text,
  p_site_bucket text,
  p_ip_bucket text,
  p_hard_expire_at_ms bigint,
  p_reason text,
  p_now_ms bigint DEFAULT NULL
)
RETURNS TABLE(result text, reason text) AS $$
DECLARE
  v_request_id text := BTRIM(COALESCE(p_request_id, ''));
  v_hostname_hash text := BTRIM(COALESCE(p_hostname_hash, ''));
  v_site_bucket text := COALESCE(NULLIF(BTRIM(COALESCE(p_site_bucket, '')), ''), 'unknown');
  v_ip_bucket text := COALESCE(NULLIF(BTRIM(COALESCE(p_ip_bucket, '')), ''), 'unknown');
  v_locked_lease record;
  v_locked_lease_row_count bigint := 0;
BEGIN
  IF v_request_id = ''
    OR v_hostname_hash = ''
    OR p_hard_expire_at_ms IS NULL
    OR p_hard_expire_at_ms <= 0 THEN
    result := 'noop';
    reason := 'not_found';
    RETURN NEXT;
    RETURN;
  END IF;

  PERFORM pg_advisory_xact_lock(3, hashtext(v_request_id));

  SELECT *
    INTO v_locked_lease
  FROM concurrency_leases
  WHERE request_id = v_request_id
  FOR UPDATE;

  GET DIAGNOSTICS v_locked_lease_row_count = ROW_COUNT;

  IF v_locked_lease_row_count = 0 THEN
    result := 'noop';
    reason := 'not_found';
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_locked_lease.hostname_hash IS DISTINCT FROM v_hostname_hash
    OR v_locked_lease.site_bucket IS DISTINCT FROM v_site_bucket
    OR v_locked_lease.ip_bucket IS DISTINCT FROM v_ip_bucket
    OR v_locked_lease.hard_expire_at_ms IS DISTINCT FROM p_hard_expire_at_ms THEN
    result := 'noop';
    reason := 'not_found';
    RETURN NEXT;
    RETURN;
  END IF;

  RETURN QUERY
  SELECT released.result, released.reason
  FROM cq_release(v_locked_lease.lease_id, v_locked_lease.lease_token, p_reason, p_now_ms) AS released;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cq_acquire(
  p_hostname_hash text,
  p_hostname text,
  p_site_bucket text,
  p_ip_bucket text,
  p_request_id text,
  p_hard_expire_at_ms bigint,
  p_now_ms bigint,
  p_host_max_in_flight integer DEFAULT 0,
  p_site_max_in_flight integer DEFAULT 0,
  p_site_ip_max_in_flight integer DEFAULT 0,
  p_cleanup_limit integer DEFAULT 500
)
RETURNS TABLE(result text, lease_id uuid, lease_token text, expires_at_ms bigint, scope text, reason text, retry_after integer) AS $$
DECLARE
  v_now_ms bigint := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint;
  v_site_bucket text := COALESCE(NULLIF(BTRIM(COALESCE(p_site_bucket, '')), ''), 'unknown');
  v_ip_bucket text := COALESCE(NULLIF(BTRIM(COALESCE(p_ip_bucket, '')), ''), 'unknown');
  v_hostname_hash text := BTRIM(COALESCE(p_hostname_hash, ''));
  v_hostname text := COALESCE(NULLIF(BTRIM(COALESCE(p_hostname, '')), ''), v_hostname_hash);
  v_request_id text := BTRIM(COALESCE(p_request_id, ''));
  v_existing record;
  v_lease_seed text;
  v_lease_token text;
  v_lease_id uuid;
  v_retry_after integer := 1;
  v_expires_at_ms bigint;
  v_host_count integer := 0;
  v_site_count integer := 0;
  v_site_ip_count integer := 0;
  v_min_expires_at_ms bigint := NULL;
  v_existing_row_count bigint := 0;
BEGIN
  IF v_request_id = '' THEN
    RAISE EXCEPTION 'cq_acquire request_id is required';
  END IF;

  IF v_hostname_hash = '' THEN
    RAISE EXCEPTION 'cq_acquire hostname_hash is required';
  END IF;

  IF p_hard_expire_at_ms IS NULL OR p_hard_expire_at_ms <= v_now_ms THEN
    RAISE EXCEPTION 'cq_acquire hard_expire_at_ms is already in the past';
  END IF;

  v_expires_at_ms := p_hard_expire_at_ms;

  PERFORM pg_advisory_xact_lock(3, hashtext(v_request_id));

  PERFORM cq_expire_scope('host', v_hostname_hash, NULL, NULL, v_now_ms, GREATEST(COALESCE(p_cleanup_limit, 500), 1));

  INSERT INTO concurrency_host_counters (hostname_hash, hostname, active_count)
  VALUES (v_hostname_hash, v_hostname, 0)
  ON CONFLICT (hostname_hash) DO UPDATE SET hostname = EXCLUDED.hostname;

  INSERT INTO concurrency_site_counters (hostname_hash, site_bucket, active_count)
  VALUES (v_hostname_hash, v_site_bucket, 0)
  ON CONFLICT (hostname_hash, site_bucket) DO NOTHING;

  INSERT INTO concurrency_site_ip_counters (hostname_hash, site_bucket, ip_bucket, active_count)
  VALUES (v_hostname_hash, v_site_bucket, v_ip_bucket, 0)
  ON CONFLICT (hostname_hash, site_bucket, ip_bucket) DO NOTHING;

  PERFORM 1 FROM concurrency_host_counters WHERE hostname_hash = v_hostname_hash FOR UPDATE;
  PERFORM 1 FROM concurrency_site_counters WHERE hostname_hash = v_hostname_hash AND site_bucket = v_site_bucket FOR UPDATE;
  PERFORM 1 FROM concurrency_site_ip_counters WHERE hostname_hash = v_hostname_hash AND site_bucket = v_site_bucket AND ip_bucket = v_ip_bucket FOR UPDATE;

  SELECT *
    INTO v_existing
  FROM concurrency_leases
  WHERE request_id = v_request_id
  FOR UPDATE;

  GET DIAGNOSTICS v_existing_row_count = ROW_COUNT;

  IF v_existing_row_count > 0 THEN
    IF v_existing.hostname_hash IS DISTINCT FROM v_hostname_hash
      OR v_existing.site_bucket IS DISTINCT FROM v_site_bucket
      OR v_existing.ip_bucket IS DISTINCT FROM v_ip_bucket
      OR v_existing.hard_expire_at_ms IS DISTINCT FROM p_hard_expire_at_ms THEN
      RAISE EXCEPTION 'cq_acquire request_id tuple mismatch';
    END IF;

    IF v_existing.state = 'active' THEN
      result := 'granted';
      lease_id := v_existing.lease_id;
      lease_token := v_existing.lease_token;
      expires_at_ms := v_existing.expires_at_ms;
      scope := NULL;
      reason := NULL;
      retry_after := NULL;
      RETURN NEXT;
      RETURN;
    END IF;

    RAISE EXCEPTION 'cq_acquire request_id replay is no longer active';
  END IF;

  SELECT active_count
    INTO v_host_count
  FROM concurrency_host_counters
  WHERE hostname_hash = v_hostname_hash;

  IF COALESCE(p_host_max_in_flight, 0) > 0 AND v_host_count >= p_host_max_in_flight THEN
    SELECT MIN(l.expires_at_ms)
      INTO v_min_expires_at_ms
    FROM concurrency_leases AS l
    WHERE l.hostname_hash = v_hostname_hash
      AND l.state = 'active';

    result := 'deny';
    lease_id := NULL;
    lease_token := NULL;
    expires_at_ms := NULL;
    scope := 'host';
    reason := 'full';
    retry_after := CASE
      WHEN v_min_expires_at_ms IS NOT NULL AND v_min_expires_at_ms > v_now_ms THEN GREATEST(1, CEIL((v_min_expires_at_ms - v_now_ms) / 1000.0)::integer)
      ELSE v_retry_after
    END;
    RETURN NEXT;
    RETURN;
  END IF;

  SELECT active_count
    INTO v_site_count
  FROM concurrency_site_counters
  WHERE hostname_hash = v_hostname_hash
    AND site_bucket = v_site_bucket;

  IF COALESCE(p_site_max_in_flight, 0) > 0 AND v_site_count >= p_site_max_in_flight THEN
    SELECT MIN(l.expires_at_ms)
      INTO v_min_expires_at_ms
    FROM concurrency_leases AS l
    WHERE l.hostname_hash = v_hostname_hash
      AND l.site_bucket = v_site_bucket
      AND l.state = 'active';

    result := 'deny';
    lease_id := NULL;
    lease_token := NULL;
    expires_at_ms := NULL;
    scope := 'site';
    reason := 'full';
    retry_after := CASE
      WHEN v_min_expires_at_ms IS NOT NULL AND v_min_expires_at_ms > v_now_ms THEN GREATEST(1, CEIL((v_min_expires_at_ms - v_now_ms) / 1000.0)::integer)
      ELSE v_retry_after
    END;
    RETURN NEXT;
    RETURN;
  END IF;

  SELECT active_count
    INTO v_site_ip_count
  FROM concurrency_site_ip_counters
  WHERE hostname_hash = v_hostname_hash
    AND site_bucket = v_site_bucket
    AND ip_bucket = v_ip_bucket;

  IF COALESCE(p_site_ip_max_in_flight, 0) > 0 AND v_site_ip_count >= p_site_ip_max_in_flight THEN
    SELECT MIN(l.expires_at_ms)
      INTO v_min_expires_at_ms
    FROM concurrency_leases AS l
    WHERE l.hostname_hash = v_hostname_hash
      AND l.site_bucket = v_site_bucket
      AND l.ip_bucket = v_ip_bucket
      AND l.state = 'active';

    result := 'deny';
    lease_id := NULL;
    lease_token := NULL;
    expires_at_ms := NULL;
    scope := 'site_ip';
    reason := 'full';
    retry_after := CASE
      WHEN v_min_expires_at_ms IS NOT NULL AND v_min_expires_at_ms > v_now_ms THEN GREATEST(1, CEIL((v_min_expires_at_ms - v_now_ms) / 1000.0)::integer)
      ELSE v_retry_after
    END;
    RETURN NEXT;
    RETURN;
  END IF;

  v_lease_seed := v_request_id || '|' || v_hostname_hash || '|' || v_site_bucket || '|' || v_ip_bucket || '|' || p_hard_expire_at_ms::text;
  v_lease_token := md5(v_lease_seed || '|token');
  v_lease_id := cq_make_uuid(v_lease_seed || '|lease_id');

  INSERT INTO concurrency_leases (
    lease_id,
    lease_token,
    request_id,
    hostname_hash,
    hostname,
    site_bucket,
    ip_bucket,
    hard_expire_at_ms,
    expires_at_ms,
    expires_at,
    state,
    released_at,
    created_at,
    updated_at
  ) VALUES (
    v_lease_id,
    v_lease_token,
    v_request_id,
    v_hostname_hash,
    v_hostname,
    v_site_bucket,
    v_ip_bucket,
    p_hard_expire_at_ms,
    v_expires_at_ms,
    to_timestamp(v_expires_at_ms / 1000.0),
    'active',
    NULL,
    now(),
    now()
  );

  UPDATE concurrency_host_counters
  SET active_count = active_count + 1,
      updated_at = now()
  WHERE hostname_hash = v_hostname_hash;

  UPDATE concurrency_site_counters
  SET active_count = active_count + 1,
      updated_at = now()
  WHERE hostname_hash = v_hostname_hash
    AND site_bucket = v_site_bucket;

  UPDATE concurrency_site_ip_counters
  SET active_count = active_count + 1,
      updated_at = now()
  WHERE hostname_hash = v_hostname_hash
    AND site_bucket = v_site_bucket
    AND ip_bucket = v_ip_bucket;

  result := 'granted';
  lease_id := v_lease_id;
  lease_token := v_lease_token;
  expires_at_ms := v_expires_at_ms;
  scope := NULL;
  reason := NULL;
  retry_after := NULL;
  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION func_try_acquire_site_slot(
  p_hostname_pattern           TEXT,
  p_site_bucket                TEXT,
  p_ip_hash                    TEXT,
  p_global_limit               INT,
  p_per_ip_limit               INT,
  p_zombie_timeout_seconds     INT,
  p_cooldown_seconds           INT
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
  v_now               TIMESTAMP WITH TIME ZONE := clock_timestamp();
  v_zombie_timeout    INTERVAL := (GREATEST(COALESCE(p_zombie_timeout_seconds, 0), 0)::TEXT || ' seconds')::INTERVAL;
  v_cooldown_interval INTERVAL := CASE
    WHEN p_cooldown_seconds > 0 THEN (p_cooldown_seconds::TEXT || ' seconds')::INTERVAL
    ELSE NULL
  END;
  v_slot_id           INT;
  v_current_ip_slots  INT := 0;
  v_last_release_at   TIMESTAMP WITH TIME ZONE;
  v_new_slot_index    INT;
  v_site_bucket       TEXT := COALESCE(NULLIF(p_site_bucket, ''), 'unknown');
BEGIN
  IF p_hostname_pattern IS NULL OR p_hostname_pattern = '' THEN
    RETURN NULL;
  END IF;

  PERFORM pg_advisory_xact_lock(2, hashtext(p_hostname_pattern || ':' || v_site_bucket));

  IF p_global_limit IS NOT NULL AND p_global_limit > 0 THEN
    INSERT INTO "fq_site_slot_pool" ("hostname_pattern", "site_bucket", "slot_index", "status")
    SELECT p_hostname_pattern, v_site_bucket, gs.slot_index, 'available'
    FROM generate_series(1, p_global_limit) AS gs(slot_index)
    ON CONFLICT ("hostname_pattern", "site_bucket", "slot_index") DO NOTHING;
  END IF;

  SELECT COUNT(*) INTO v_current_ip_slots
  FROM "fq_site_slot_pool"
  WHERE "hostname_pattern" = p_hostname_pattern
    AND "site_bucket" = v_site_bucket
    AND "status" = 'locked'
    AND "ip_hash" = p_ip_hash
    AND "locked_at" IS NOT NULL
    AND "locked_at" >= (v_now - v_zombie_timeout);

  IF p_per_ip_limit > 0 THEN
    IF v_current_ip_slots >= p_per_ip_limit THEN
      RETURN 0;
    END IF;
  END IF;

  IF v_cooldown_interval IS NOT NULL
     AND p_ip_hash IS NOT NULL
     AND p_ip_hash <> ''
     AND p_per_ip_limit > 0
     AND v_current_ip_slots < p_per_ip_limit THEN
    SELECT "last_release_at"
      INTO v_last_release_at
    FROM "fq_site_ip_cooldown"
    WHERE "hostname_pattern" = p_hostname_pattern
      AND "site_bucket" = v_site_bucket
      AND "ip_hash" = p_ip_hash;

    IF v_last_release_at IS NOT NULL
       AND v_last_release_at > (v_now - v_cooldown_interval) THEN
      RETURN 0;
    END IF;
  END IF;

  IF p_global_limit IS NOT NULL AND p_global_limit > 0 THEN
    SELECT "id" INTO v_slot_id
    FROM "fq_site_slot_pool"
    WHERE "hostname_pattern" = p_hostname_pattern
      AND "site_bucket" = v_site_bucket
      AND "slot_index" <= p_global_limit
      AND (
        "status" = 'available'
        OR (
          "status" = 'locked'
          AND "locked_at" IS NOT NULL
          AND "locked_at" < (v_now - v_zombie_timeout)
        )
      )
    ORDER BY "slot_index"
    FOR UPDATE SKIP LOCKED
    LIMIT 1;
  ELSE
    SELECT "id" INTO v_slot_id
    FROM "fq_site_slot_pool"
    WHERE "hostname_pattern" = p_hostname_pattern
      AND "site_bucket" = v_site_bucket
      AND (
        "status" = 'available'
        OR (
          "status" = 'locked'
          AND "locked_at" IS NOT NULL
          AND "locked_at" < (v_now - v_zombie_timeout)
        )
      )
    ORDER BY "slot_index"
    FOR UPDATE SKIP LOCKED
    LIMIT 1;
  END IF;

  IF v_slot_id IS NOT NULL THEN
    UPDATE "fq_site_slot_pool"
    SET "status" = 'locked',
        "ip_hash" = p_ip_hash,
        "locked_at" = v_now
    WHERE "id" = v_slot_id;

    RETURN v_slot_id;
  END IF;

  IF p_global_limit IS NOT NULL AND p_global_limit > 0 THEN
    RETURN -1;
  END IF;

  SELECT COALESCE(MAX("slot_index"), 0) + 1
    INTO v_new_slot_index
  FROM "fq_site_slot_pool"
  WHERE "hostname_pattern" = p_hostname_pattern
    AND "site_bucket" = v_site_bucket;

  INSERT INTO "fq_site_slot_pool" ("hostname_pattern", "site_bucket", "slot_index", "status", "ip_hash", "locked_at")
  VALUES (p_hostname_pattern, v_site_bucket, v_new_slot_index, 'locked', p_ip_hash, v_now)
  RETURNING "id" INTO v_slot_id;

  RETURN v_slot_id;
END;
$$;

CREATE OR REPLACE FUNCTION func_release_host_slot(
  p_slot_id INT,
  p_enable_cooldown BOOLEAN DEFAULT TRUE
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  v_hostname_pattern TEXT;
  v_ip_hash          TEXT;
  v_row_count        INTEGER := 0;
BEGIN
  SELECT "hostname_pattern", "ip_hash"
    INTO v_hostname_pattern, v_ip_hash
  FROM "fq_host_slot_pool"
  WHERE "id" = p_slot_id
  FOR UPDATE;

  GET DIAGNOSTICS v_row_count = ROW_COUNT;

  IF v_row_count = 0 THEN
    RETURN;
  END IF;

  IF p_enable_cooldown
     AND v_ip_hash IS NOT NULL
     AND v_ip_hash <> '' THEN
    INSERT INTO "fq_host_ip_cooldown" ("hostname_pattern", "ip_hash", "last_release_at")
    VALUES (v_hostname_pattern, v_ip_hash, clock_timestamp())
    ON CONFLICT ("hostname_pattern", "ip_hash")
    DO UPDATE SET
      "last_release_at" = EXCLUDED."last_release_at";
  END IF;

  UPDATE "fq_host_slot_pool"
  SET "status" = 'available',
      "ip_hash" = NULL,
      "locked_at" = NULL
  WHERE "id" = p_slot_id;
END;
$$;

CREATE OR REPLACE FUNCTION func_release_site_slot(
  p_slot_id INT,
  p_enable_cooldown BOOLEAN DEFAULT TRUE
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  v_hostname_pattern TEXT;
  v_site_bucket      TEXT;
  v_ip_hash          TEXT;
  v_row_count        INTEGER := 0;
BEGIN
  SELECT "hostname_pattern", "site_bucket", "ip_hash"
    INTO v_hostname_pattern, v_site_bucket, v_ip_hash
  FROM "fq_site_slot_pool"
  WHERE "id" = p_slot_id
  FOR UPDATE;

  GET DIAGNOSTICS v_row_count = ROW_COUNT;

  IF v_row_count = 0 THEN
    RETURN;
  END IF;

  IF p_enable_cooldown
     AND v_ip_hash IS NOT NULL
     AND v_ip_hash <> '' THEN
    INSERT INTO "fq_site_ip_cooldown" ("hostname_pattern", "site_bucket", "ip_hash", "last_release_at")
    VALUES (v_hostname_pattern, v_site_bucket, v_ip_hash, clock_timestamp())
    ON CONFLICT ("hostname_pattern", "site_bucket", "ip_hash")
    DO UPDATE SET
      "last_release_at" = EXCLUDED."last_release_at";
  END IF;

  UPDATE "fq_site_slot_pool"
  SET "status" = 'available',
      "ip_hash" = NULL,
      "locked_at" = NULL
  WHERE "id" = p_slot_id;
END;
$$;

-- ========================================
-- Fair Queue Cleanup RPCs
-- ========================================
CREATE OR REPLACE FUNCTION func_cleanup_host_zombie_slots(
  p_zombie_timeout_seconds INT
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  recovered_count INTEGER;
  v_zombie_timeout INTERVAL := (GREATEST(COALESCE(p_zombie_timeout_seconds, 0), 0)::TEXT || ' seconds')::INTERVAL;
BEGIN
  UPDATE "fq_host_slot_pool"
  SET "status" = 'available',
      "ip_hash" = NULL,
      "locked_at" = NULL
  WHERE "status" = 'locked'
    AND "locked_at" IS NOT NULL
    AND "locked_at" < (clock_timestamp() - v_zombie_timeout);

  GET DIAGNOSTICS recovered_count = ROW_COUNT;
  RETURN recovered_count;
END;
$$;

CREATE OR REPLACE FUNCTION func_cleanup_site_zombie_slots(
  p_zombie_timeout_seconds INT
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  recovered_count INTEGER;
  v_zombie_timeout INTERVAL := (GREATEST(COALESCE(p_zombie_timeout_seconds, 0), 0)::TEXT || ' seconds')::INTERVAL;
BEGIN
  UPDATE "fq_site_slot_pool"
  SET "status" = 'available',
      "ip_hash" = NULL,
      "locked_at" = NULL
  WHERE "status" = 'locked'
    AND "locked_at" IS NOT NULL
    AND "locked_at" < (clock_timestamp() - v_zombie_timeout);

  GET DIAGNOSTICS recovered_count = ROW_COUNT;
  RETURN recovered_count;
END;
$$;

CREATE OR REPLACE FUNCTION func_cleanup_host_ip_cooldown(
  p_ttl_seconds INT
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_cutoff  TIMESTAMP WITH TIME ZONE;
  v_deleted INTEGER;
BEGIN
  IF p_ttl_seconds <= 0 THEN
    RETURN 0;
  END IF;

  v_cutoff := clock_timestamp() - (p_ttl_seconds::TEXT || ' seconds')::INTERVAL;

  DELETE FROM "fq_host_ip_cooldown"
  WHERE "last_release_at" < v_cutoff;

  GET DIAGNOSTICS v_deleted = ROW_COUNT;
  RETURN v_deleted;
END;
$$;

CREATE OR REPLACE FUNCTION func_cleanup_site_ip_cooldown(
  p_ttl_seconds INT
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_cutoff  TIMESTAMP WITH TIME ZONE;
  v_deleted INTEGER;
BEGIN
  IF p_ttl_seconds <= 0 THEN
    RETURN 0;
  END IF;

  v_cutoff := clock_timestamp() - (p_ttl_seconds::TEXT || ' seconds')::INTERVAL;

  DELETE FROM "fq_site_ip_cooldown"
  WHERE "last_release_at" < v_cutoff;

  GET DIAGNOSTICS v_deleted = ROW_COUNT;
  RETURN v_deleted;
END;
$$;

-- ========================================
-- Slot-Handler Friendly Fair Queue RPCs
-- ========================================
CREATE OR REPLACE FUNCTION fq_admit_batch(
  p_hostname_hash TEXT,
  p_hostname TEXT,
  p_site_buckets TEXT[],
  p_ip_buckets TEXT[],
  p_now_ms BIGINT,
  p_host_max_slot_per_host INT,
  p_host_max_slot_per_ip INT,
  p_site_max_slot_per_site INT,
  p_site_max_slot_per_ip INT,
  p_zombie_timeout INT,
  p_cooldown_seconds INT,
  p_breaker_enabled BOOLEAN,
  p_half_open_max_probe_count INT,
  p_half_open_max_seconds INT,
  p_half_open_timeout_mode TEXT
)
RETURNS TABLE(
  status TEXT,
  slot_token TEXT,
  throttle_code INT,
  breaker_open_until INT,
  breaker_reason TEXT,
  breaker_version BIGINT,
  retry_after INT,
  attempt_version BIGINT,
  attempt_ticket INT
) AS $$
DECLARE
  v_hostname TEXT;
  v_site_bucket TEXT;
  v_ip_bucket TEXT;
  v_now INTEGER := COALESCE((p_now_ms / 1000)::INTEGER, EXTRACT(EPOCH FROM NOW())::INTEGER);
  v_breaker_state TEXT := NULL;
  v_breaker_open_until INTEGER := NULL;
  v_breaker_reason TEXT := NULL;
  v_breaker_version BIGINT := NULL;
  v_throttled BOOLEAN := FALSE;
  v_throttle_code INTEGER := NULL;
  v_breaker_row_count INTEGER := 0;
  v_authorize_half_open_deadline INTEGER := NULL;
  v_authorize_attempt_granted BOOLEAN := FALSE;
  v_authorize_attempt_ticket INTEGER := NULL;
  v_host_slot_id INT;
  v_site_slot_id INT;
  v_site_len INT;
  v_ip_len INT;
  v_idx INT;
BEGIN
  v_hostname := COALESCE(NULLIF(p_hostname, ''), NULLIF(p_hostname_hash, ''));
  v_site_len := COALESCE(array_length(p_site_buckets, 1), 0);
  v_ip_len := COALESCE(array_length(p_ip_buckets, 1), 0);
  IF v_site_len = 0 OR v_ip_len = 0 OR v_site_len <> v_ip_len THEN
    RETURN;
  END IF;

  IF COALESCE(p_breaker_enabled, FALSE)
    AND p_hostname_hash IS NOT NULL
    AND p_hostname_hash <> '' THEN
    SELECT "STATE", "OPEN_UNTIL", "OPEN_REASON", "VERSION", "LAST_ERROR_CODE"
    INTO v_breaker_state, v_breaker_open_until, v_breaker_reason, v_breaker_version, v_throttle_code
    FROM "THROTTLE_PROTECTION"
    WHERE "HOSTNAME_HASH" = p_hostname_hash;

    GET DIAGNOSTICS v_breaker_row_count = ROW_COUNT;
    v_throttled := v_breaker_row_count > 0
      AND v_breaker_state = 'open'
      AND v_breaker_open_until IS NOT NULL
      AND v_breaker_open_until > v_now;
  END IF;

  IF v_hostname IS NULL THEN
    FOR v_idx IN 1..v_site_len LOOP
      status := 'WAIT';
      slot_token := NULL::TEXT;
      throttle_code := NULL::INTEGER;
      breaker_open_until := NULL::INTEGER;
      breaker_reason := NULL::TEXT;
      breaker_version := NULL::BIGINT;
      retry_after := NULL::INTEGER;
      attempt_version := NULL::BIGINT;
      attempt_ticket := NULL::INTEGER;
      RETURN NEXT;
    END LOOP;
    RETURN;
  END IF;

  IF v_throttled THEN
    FOR v_idx IN 1..v_site_len LOOP
      status := 'THROTTLED';
      slot_token := NULL::TEXT;
      throttle_code := v_throttle_code;
      breaker_open_until := v_breaker_open_until;
      breaker_reason := v_breaker_reason;
      breaker_version := v_breaker_version;
      retry_after := NULL::INTEGER;
      attempt_version := NULL::BIGINT;
      attempt_ticket := NULL::INTEGER;
      RETURN NEXT;
    END LOOP;
    RETURN;
  END IF;

  FOR v_idx IN 1..v_site_len LOOP
    IF COALESCE(p_breaker_enabled, FALSE)
      AND v_breaker_state = 'open'
      AND v_breaker_open_until IS NOT NULL
      AND v_breaker_open_until > v_now THEN
      status := 'THROTTLED';
      slot_token := NULL::TEXT;
      throttle_code := v_throttle_code;
      breaker_open_until := v_breaker_open_until;
      breaker_reason := v_breaker_reason;
      breaker_version := v_breaker_version;
      retry_after := NULL::INTEGER;
      attempt_version := NULL::BIGINT;
      attempt_ticket := NULL::INTEGER;
      RETURN NEXT;
      CONTINUE;
    END IF;

    v_site_bucket := COALESCE(NULLIF(p_site_buckets[v_idx], ''), 'unknown');
    v_ip_bucket := p_ip_buckets[v_idx];

    BEGIN
      v_host_slot_id := func_try_acquire_host_slot(
        v_hostname,
        v_ip_bucket,
        COALESCE(p_host_max_slot_per_host, 0),
        COALESCE(p_host_max_slot_per_ip, 0),
        COALESCE(p_zombie_timeout, 0),
        COALESCE(p_cooldown_seconds, 0)
      );
    EXCEPTION
      WHEN others THEN
        v_host_slot_id := NULL;
    END;

    IF v_host_slot_id IS NULL THEN
      status := 'WAIT';
      slot_token := NULL::TEXT;
      throttle_code := v_throttle_code;
      breaker_open_until := v_breaker_open_until;
      breaker_reason := v_breaker_reason;
      breaker_version := v_breaker_version;
      retry_after := NULL::INTEGER;
      attempt_version := NULL::BIGINT;
      attempt_ticket := NULL::INTEGER;
      RETURN NEXT;
      CONTINUE;
    END IF;

    IF v_host_slot_id = 0 THEN
      status := 'IP_TOO_MANY';
      slot_token := NULL::TEXT;
      throttle_code := v_throttle_code;
      breaker_open_until := v_breaker_open_until;
      breaker_reason := v_breaker_reason;
      breaker_version := v_breaker_version;
      retry_after := NULL::INTEGER;
      attempt_version := NULL::BIGINT;
      attempt_ticket := NULL::INTEGER;
      RETURN NEXT;
      CONTINUE;
    ELSIF v_host_slot_id < 0 THEN
      status := 'WAIT';
      slot_token := NULL::TEXT;
      throttle_code := v_throttle_code;
      breaker_open_until := v_breaker_open_until;
      breaker_reason := v_breaker_reason;
      breaker_version := v_breaker_version;
      retry_after := NULL::INTEGER;
      attempt_version := NULL::BIGINT;
      attempt_ticket := NULL::INTEGER;
      RETURN NEXT;
      CONTINUE;
    END IF;

    BEGIN
      v_site_slot_id := func_try_acquire_site_slot(
        v_hostname,
        v_site_bucket,
        v_ip_bucket,
        COALESCE(p_site_max_slot_per_site, 0),
        COALESCE(p_site_max_slot_per_ip, 0),
        COALESCE(p_zombie_timeout, 0),
        COALESCE(p_cooldown_seconds, 0)
      );
    EXCEPTION
      WHEN others THEN
        v_site_slot_id := NULL;
    END;

    IF v_site_slot_id IS NULL THEN
      PERFORM func_release_host_slot(v_host_slot_id, FALSE);
      status := 'WAIT';
      slot_token := NULL::TEXT;
      throttle_code := v_throttle_code;
      breaker_open_until := v_breaker_open_until;
      breaker_reason := v_breaker_reason;
      breaker_version := v_breaker_version;
      retry_after := NULL::INTEGER;
      attempt_version := NULL::BIGINT;
      attempt_ticket := NULL::INTEGER;
      RETURN NEXT;
      CONTINUE;
    END IF;

    IF v_site_slot_id = 0 THEN
      PERFORM func_release_host_slot(v_host_slot_id, FALSE);
      status := 'IP_TOO_MANY';
      slot_token := NULL::TEXT;
      throttle_code := v_throttle_code;
      breaker_open_until := v_breaker_open_until;
      breaker_reason := v_breaker_reason;
      breaker_version := v_breaker_version;
      retry_after := NULL::INTEGER;
      attempt_version := NULL::BIGINT;
      attempt_ticket := NULL::INTEGER;
      RETURN NEXT;
      CONTINUE;
    ELSIF v_site_slot_id < 0 THEN
      PERFORM func_release_host_slot(v_host_slot_id, FALSE);
      status := 'WAIT';
      slot_token := NULL::TEXT;
      throttle_code := v_throttle_code;
      breaker_open_until := v_breaker_open_until;
      breaker_reason := v_breaker_reason;
      breaker_version := v_breaker_version;
      retry_after := NULL::INTEGER;
      attempt_version := NULL::BIGINT;
      attempt_ticket := NULL::INTEGER;
      RETURN NEXT;
      CONTINUE;
    END IF;

    IF COALESCE(p_breaker_enabled, FALSE)
      AND p_hostname_hash IS NOT NULL
      AND p_hostname_hash <> '' THEN
      SELECT
        "STATE",
        "OPEN_UNTIL",
        "OPEN_REASON",
        "VERSION",
        "HALF_OPEN_DEADLINE",
        "ATTEMPT_GRANTED",
        "ATTEMPT_TICKET",
        "LAST_ERROR_CODE"
      INTO
        v_breaker_state,
        v_breaker_open_until,
        v_breaker_reason,
        v_breaker_version,
        v_authorize_half_open_deadline,
        v_authorize_attempt_granted,
        v_authorize_attempt_ticket,
        v_throttle_code
      FROM download_authorize_breaker_attempt(
        p_hostname_hash,
        p_hostname,
        v_now,
        p_half_open_max_probe_count,
        p_half_open_max_seconds,
        p_half_open_timeout_mode
      );

      IF v_breaker_state = 'open'
        AND v_breaker_open_until IS NOT NULL
        AND v_breaker_open_until > v_now THEN
        PERFORM func_release_site_slot(v_site_slot_id, FALSE);
        PERFORM func_release_host_slot(v_host_slot_id, FALSE);
        status := 'THROTTLED';
        slot_token := NULL::TEXT;
        throttle_code := v_throttle_code;
        breaker_open_until := v_breaker_open_until;
        breaker_reason := v_breaker_reason;
        breaker_version := v_breaker_version;
        retry_after := NULL::INTEGER;
        attempt_version := NULL::BIGINT;
        attempt_ticket := NULL::INTEGER;
        RETURN NEXT;
        CONTINUE;
      END IF;

      IF v_breaker_state = 'half_open' AND NOT COALESCE(v_authorize_attempt_granted, FALSE) THEN
        PERFORM func_release_site_slot(v_site_slot_id, FALSE);
        PERFORM func_release_host_slot(v_host_slot_id, FALSE);
        status := 'HALF_OPEN_FULL';
        slot_token := NULL::TEXT;
        throttle_code := v_throttle_code;
        breaker_open_until := v_breaker_open_until;
        breaker_reason := v_breaker_reason;
        breaker_version := v_breaker_version;
        retry_after := CASE
          WHEN v_authorize_half_open_deadline IS NOT NULL AND v_authorize_half_open_deadline > v_now THEN v_authorize_half_open_deadline - v_now
          ELSE 1
        END;
        attempt_version := NULL::BIGINT;
        attempt_ticket := NULL::INTEGER;
        RETURN NEXT;
        CONTINUE;
      END IF;
    END IF;

    status := 'READY';
    slot_token := encode(convert_to(jsonb_build_object('host', v_host_slot_id, 'site', v_site_slot_id)::text, 'UTF8'), 'base64');
    throttle_code := v_throttle_code;
    breaker_open_until := v_breaker_open_until;
    breaker_reason := v_breaker_reason;
    breaker_version := v_breaker_version;
    retry_after := NULL::INTEGER;
    attempt_version := CASE
      WHEN COALESCE(v_authorize_attempt_granted, FALSE) THEN v_breaker_version
      ELSE NULL::BIGINT
    END;
    attempt_ticket := CASE
      WHEN COALESCE(v_authorize_attempt_granted, FALSE) THEN v_authorize_attempt_ticket
      ELSE NULL::INTEGER
    END;
    RETURN NEXT;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fq_release_dual(
  p_hostname_hash TEXT,
  p_site_bucket TEXT,
  p_ip_bucket TEXT,
  p_slot_token TEXT,
  p_now_ms BIGINT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  v_payload JSONB;
  v_host_slot_id INT;
  v_site_slot_id INT;
BEGIN
  IF p_slot_token IS NULL OR p_slot_token = '' THEN
    RETURN;
  END IF;

  BEGIN
    v_payload := convert_from(decode(p_slot_token, 'base64'), 'UTF8')::jsonb;
    v_host_slot_id := (v_payload->>'host')::INT;
    v_site_slot_id := (v_payload->>'site')::INT;
  EXCEPTION
    WHEN others THEN
      RETURN;
  END;

  IF v_host_slot_id IS NOT NULL AND v_host_slot_id > 0 THEN
    PERFORM func_release_host_slot(v_host_slot_id, TRUE);
  END IF;

  IF v_site_slot_id IS NOT NULL AND v_site_slot_id > 0 THEN
    PERFORM func_release_site_slot(v_site_slot_id, TRUE);
  END IF;
END;
$$;
