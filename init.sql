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
  "PROBE_LEASE_UNTIL" INTEGER,
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
-- PostgreSQL Stored Procedure: Claim Breaker Probe
-- ========================================
CREATE OR REPLACE FUNCTION download_claim_breaker_probe(
  p_hostname_hash TEXT,
  p_hostname TEXT,
  p_now INTEGER,
  p_probe_lease_seconds INTEGER,
  p_half_open_max_seconds INTEGER,
  p_half_open_timeout_mode TEXT
)
RETURNS TABLE(
  "HOSTNAME_HASH" TEXT,
  "HOSTNAME" TEXT,
  "STATE" TEXT,
  "OPEN_UNTIL" INTEGER,
  "EWMA_SCORE" NUMERIC,
  "TOTAL_SAMPLES" INTEGER,
  "CONSECUTIVE_ERROR_COUNT" INTEGER,
  "SUCCESS_STREAK" INTEGER,
  "PROBE_LEASE_UNTIL" INTEGER,
  "LAST_ERROR_CODE" INTEGER,
  "OPEN_REASON" TEXT,
  "LAST_OPEN_SECONDS" INTEGER,
  "VERSION" BIGINT,
  "PROBE_GRANTED" BOOLEAN
) AS $$
DECLARE
  v_now INTEGER := COALESCE(p_now, EXTRACT(EPOCH FROM NOW())::INTEGER);
  v_probe_lease_seconds INTEGER;
  v_half_open_max_seconds INTEGER;
  v_half_open_timeout_mode TEXT;
  v_half_open_timed_out BOOLEAN := FALSE;
  v_timeout_open_seconds INTEGER := 0;

  v_hostname TEXT := p_hostname;
  v_state TEXT := 'closed';
  v_open_until INTEGER := NULL;
  v_ewma_score NUMERIC := 0;
  v_total_samples INTEGER := 0;
  v_samples_since_reset INTEGER := 0;
  v_consecutive_error_count INTEGER := 0;
  v_success_streak INTEGER := 0;
  v_half_open_since INTEGER := NULL;
  v_probe_lease_until INTEGER := NULL;
  v_last_error_code INTEGER := NULL;
  v_open_reason TEXT := NULL;
  v_last_open_seconds INTEGER := 0;
  v_version BIGINT := 0;
  v_probe_granted BOOLEAN := FALSE;
  v_locked BOOLEAN := FALSE;
  v_locked_row_count INTEGER := 0;
BEGIN
  IF p_hostname_hash IS NULL OR p_hostname_hash = '' THEN
    RETURN;
  END IF;

  IF p_probe_lease_seconds IS NULL
    OR p_half_open_max_seconds IS NULL
    OR p_half_open_timeout_mode IS NULL
    OR BTRIM(p_half_open_timeout_mode) = '' THEN
    RAISE EXCEPTION 'download_claim_breaker_probe requires non-null probe settings';
  END IF;

  v_probe_lease_seconds := GREATEST(1, p_probe_lease_seconds);
  v_half_open_max_seconds := GREATEST(0, p_half_open_max_seconds);
  v_half_open_timeout_mode := LOWER(BTRIM(p_half_open_timeout_mode));
  IF v_half_open_timeout_mode NOT IN ('open', 'close', 'partial-close') THEN
    RAISE EXCEPTION 'download_claim_breaker_probe invalid p_half_open_timeout_mode: %', p_half_open_timeout_mode;
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
      tp."PROBE_LEASE_UNTIL",
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
      v_probe_lease_until,
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
  v_last_open_seconds := COALESCE(v_last_open_seconds, 0);
  v_version := COALESCE(v_version, 0);

  IF v_state = 'open' AND (v_open_until IS NULL OR v_open_until <= v_now) THEN
    v_state := 'half_open';
    v_open_until := NULL;
    v_success_streak := 0;
    v_half_open_since := COALESCE(v_half_open_since, v_now);
    IF v_probe_lease_until IS NULL OR v_probe_lease_until <= v_now THEN
      v_probe_lease_until := v_now + v_probe_lease_seconds;
      v_probe_granted := TRUE;
    END IF;
    v_version := v_version + 1;
  END IF;

  v_half_open_timed_out := v_half_open_max_seconds > 0
    AND v_half_open_since IS NOT NULL
    AND (v_now - v_half_open_since) >= v_half_open_max_seconds;

  IF v_state = 'half_open' AND v_half_open_timed_out THEN
    v_timeout_open_seconds := CASE
      WHEN v_last_open_seconds > 0 THEN v_last_open_seconds
      ELSE 1
    END;

    IF v_half_open_timeout_mode = 'open' THEN
      v_state := 'open';
      v_open_until := v_now + v_timeout_open_seconds;
      v_probe_lease_until := NULL;
      v_half_open_since := NULL;
      v_last_open_seconds := v_timeout_open_seconds;
    ELSIF v_half_open_timeout_mode = 'close' THEN
      v_state := 'closed';
      v_open_until := NULL;
      v_probe_lease_until := NULL;
      v_ewma_score := 0;
      v_consecutive_error_count := 0;
      v_success_streak := 0;
      v_samples_since_reset := 0;
      v_last_error_code := NULL;
      v_open_reason := NULL;
      v_last_open_seconds := 0;
      v_half_open_since := NULL;
    ELSE
      IF v_success_streak > 0 THEN
        v_state := 'closed';
        v_open_until := NULL;
        v_probe_lease_until := NULL;
        v_ewma_score := 0;
        v_consecutive_error_count := 0;
        v_success_streak := 0;
        v_samples_since_reset := 0;
        v_last_error_code := NULL;
        v_open_reason := NULL;
        v_last_open_seconds := 0;
        v_half_open_since := NULL;
      ELSE
        v_state := 'open';
        v_open_until := v_now + v_timeout_open_seconds;
        v_probe_lease_until := NULL;
        v_half_open_since := NULL;
        v_last_open_seconds := v_timeout_open_seconds;
      END IF;
    END IF;
    v_probe_granted := FALSE;
    v_version := v_version + 1;
  ELSIF v_state = 'half_open' AND (v_probe_lease_until IS NULL OR v_probe_lease_until <= v_now) THEN
    v_probe_lease_until := v_now + v_probe_lease_seconds;
    v_probe_granted := TRUE;
    v_version := v_version + 1;
  ELSIF v_state = 'closed' THEN
    v_open_until := NULL;
    v_probe_lease_until := NULL;
    v_success_streak := 0;
    v_half_open_since := NULL;
  END IF;

  IF v_state = 'open' OR v_state = 'closed' THEN
    v_half_open_since := NULL;
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
    "PROBE_LEASE_UNTIL" = v_probe_lease_until,
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
    tp."CONSECUTIVE_ERROR_COUNT",
    tp."SUCCESS_STREAK",
    tp."PROBE_LEASE_UNTIL",
    tp."LAST_ERROR_CODE",
    tp."OPEN_REASON",
    tp."LAST_OPEN_SECONDS",
    tp."VERSION",
    v_probe_granted;
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
  p_probe_version BIGINT DEFAULT NULL
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
  "PROBE_LEASE_UNTIL" INTEGER,
  "LAST_SAMPLE_AT" INTEGER,
  "LAST_ERROR_CODE" INTEGER,
  "OPEN_REASON" TEXT,
  "LAST_OPEN_SECONDS" INTEGER,
  "VERSION" BIGINT
) AS $$
DECLARE
  v_now INTEGER := COALESCE(p_now, EXTRACT(EPOCH FROM NOW())::INTEGER);
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
  v_half_open_max_seconds INTEGER;
  v_half_open_timeout_mode TEXT;

  v_hostname TEXT := p_hostname;
  v_state TEXT := 'closed';
  v_open_until INTEGER := NULL;
  v_ewma_score NUMERIC := 0;
  v_total_samples INTEGER := 0;
  v_samples_since_reset INTEGER := 0;
  v_consecutive_error_count INTEGER := 0;
  v_success_streak INTEGER := 0;
  v_probe_lease_until INTEGER := NULL;
  v_half_open_since INTEGER := NULL;
  v_last_sample_at INTEGER := NULL;
  v_last_error_code INTEGER := NULL;
  v_open_reason TEXT := NULL;
  v_last_open_seconds INTEGER := 0;
  v_version BIGINT := 0;

  v_locked BOOLEAN := FALSE;
  v_locked_row_count INTEGER := 0;
  v_should_open BOOLEAN := FALSE;
  v_should_close BOOLEAN := FALSE;
  v_half_open_timed_out BOOLEAN := FALSE;
  v_open_seconds INTEGER := 0;
  v_timeout_open_seconds INTEGER := 0;
  v_probe_version_matches BOOLEAN := FALSE;
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
  v_half_open_max_seconds := GREATEST(0, p_half_open_max_seconds);
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
      tp."PROBE_LEASE_UNTIL",
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
      v_probe_lease_until,
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
  v_last_open_seconds := COALESCE(v_last_open_seconds, 0);
  v_version := COALESCE(v_version, 0);
  v_probe_version_matches := p_probe_version IS NOT NULL AND p_probe_version = v_version;

  IF p_probe_version IS NOT NULL THEN
    IF v_state <> 'half_open' OR NOT v_probe_version_matches THEN
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
        v_probe_lease_until,
        v_last_sample_at,
        v_last_error_code,
        v_open_reason,
        v_last_open_seconds,
        v_version;
      RETURN;
    END IF;

    v_version := v_version + 1;
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
      v_probe_lease_until,
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
    v_probe_lease_until := NULL;
    v_success_streak := 0;
    v_half_open_since := NULL;
  ELSIF v_state = 'open' AND v_open_until IS NOT NULL AND v_open_until <= v_now THEN
    NULL;
  END IF;

  v_half_open_timed_out := v_half_open_max_seconds > 0
    AND v_half_open_since IS NOT NULL
    AND (v_now - v_half_open_since) >= v_half_open_max_seconds;

  IF v_state = 'half_open' AND v_half_open_timed_out THEN
    v_timeout_open_seconds := CASE
      WHEN v_last_open_seconds > 0 THEN v_last_open_seconds
      ELSE 1
    END;

    IF v_half_open_timeout_mode = 'open' THEN
      v_state := 'open';
      v_open_until := v_now + v_timeout_open_seconds;
      v_probe_lease_until := NULL;
      v_half_open_since := NULL;
      v_last_open_seconds := v_timeout_open_seconds;
    ELSIF v_half_open_timeout_mode = 'close' THEN
      v_state := 'closed';
      v_open_until := NULL;
      v_probe_lease_until := NULL;
      v_ewma_score := 0;
      v_consecutive_error_count := 0;
      v_success_streak := 0;
      v_samples_since_reset := 0;
      v_last_error_code := NULL;
      v_open_reason := NULL;
      v_last_open_seconds := 0;
      v_half_open_since := NULL;
    ELSE
      IF v_success_streak > 0 THEN
        v_state := 'closed';
        v_open_until := NULL;
        v_probe_lease_until := NULL;
        v_ewma_score := 0;
        v_consecutive_error_count := 0;
        v_success_streak := 0;
        v_samples_since_reset := 0;
        v_last_error_code := NULL;
        v_open_reason := NULL;
        v_last_open_seconds := 0;
        v_half_open_since := NULL;
      ELSE
        v_state := 'open';
        v_open_until := v_now + v_timeout_open_seconds;
        v_probe_lease_until := NULL;
        v_half_open_since := NULL;
        v_last_open_seconds := v_timeout_open_seconds;
      END IF;
    END IF;
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
        v_probe_lease_until := NULL;
        v_open_reason := CASE
          WHEN p_status_code IS NOT NULL THEN 'http_' || p_status_code::TEXT
          ELSE 'error_sample'
        END;
        v_last_open_seconds := v_open_seconds;
      ELSIF v_state = 'closed' THEN
        v_open_until := NULL;
        v_probe_lease_until := NULL;
        v_open_reason := NULL;
      END IF;
    ELSE
      v_consecutive_error_count := 0;

      IF v_state = 'half_open' THEN
        v_success_streak := v_success_streak + 1;
        v_probe_lease_until := NULL;
        v_should_close := CASE
          WHEN v_half_open_close_mode = 'or' THEN
            v_success_streak >= v_half_open_success_threshold
            OR v_ewma_score <= v_close_threshold
          ELSE
            v_success_streak >= v_half_open_success_threshold
            AND v_ewma_score <= v_close_threshold
        END;

        IF v_should_close THEN
          v_state := 'closed';
          v_open_until := NULL;
          v_probe_lease_until := NULL;
          v_ewma_score := 0;
          v_consecutive_error_count := 0;
          v_success_streak := 0;
          v_samples_since_reset := 0;
          v_last_error_code := NULL;
          v_open_reason := NULL;
          v_last_open_seconds := 0;
          v_half_open_since := NULL;
        END IF;
      ELSIF v_state = 'closed' THEN
        v_open_until := NULL;
        v_probe_lease_until := NULL;
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
    "PROBE_LEASE_UNTIL" = v_probe_lease_until,
    "HALF_OPEN_SINCE" = v_half_open_since,
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
    tp."PROBE_LEASE_UNTIL",
    tp."LAST_SAMPLE_AT",
    tp."LAST_ERROR_CODE",
    tp."OPEN_REASON",
    tp."LAST_OPEN_SECONDS",
    tp."VERSION";
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
CREATE OR REPLACE FUNCTION fq_try_acquire_batch(
  p_hostname_hash TEXT,
  p_hostname TEXT,
  p_site_buckets TEXT[],
  p_ip_buckets TEXT[],
  p_now_ms BIGINT,
  p_host_max_slot_per_host INT,
  p_host_max_slot_per_ip INT,
  p_site_max_slot_per_site INT,
  p_site_max_slot_per_ip INT,
  p_zombie_timeout INT DEFAULT 30,
  p_cooldown_seconds INT DEFAULT 0
)
RETURNS TABLE(
  status TEXT,
  slot_token TEXT,
  throttle_code INT,
  breaker_open_until INT,
  breaker_reason TEXT,
  breaker_version BIGINT
) AS $$
DECLARE
  v_hostname TEXT;
  v_site_bucket TEXT;
  v_ip_bucket TEXT;
  v_now INTEGER := COALESCE((p_now_ms / 1000)::INTEGER, EXTRACT(EPOCH FROM NOW())::INTEGER);
  v_throttled BOOLEAN := FALSE;
  v_throttle_code INTEGER := NULL;
  v_breaker_state TEXT := NULL;
  v_breaker_open_until INTEGER := NULL;
  v_breaker_reason TEXT := NULL;
  v_breaker_version BIGINT := NULL;
  v_breaker_row_count INTEGER := 0;
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

  IF p_hostname_hash IS NOT NULL AND p_hostname_hash <> '' THEN
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
      RETURN NEXT;
    END LOOP;
    RETURN;
  END IF;

  FOR v_idx IN 1..v_site_len LOOP
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
      RETURN NEXT;
      CONTINUE;
    ELSIF v_host_slot_id < 0 THEN
      status := 'QUEUE_FULL';
      slot_token := NULL::TEXT;
      throttle_code := v_throttle_code;
      breaker_open_until := v_breaker_open_until;
      breaker_reason := v_breaker_reason;
      breaker_version := v_breaker_version;
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
      RETURN NEXT;
      CONTINUE;
    ELSIF v_site_slot_id < 0 THEN
      PERFORM func_release_host_slot(v_host_slot_id, FALSE);
      status := 'QUEUE_FULL';
      slot_token := NULL::TEXT;
      throttle_code := v_throttle_code;
      breaker_open_until := v_breaker_open_until;
      breaker_reason := v_breaker_reason;
      breaker_version := v_breaker_version;
      RETURN NEXT;
      CONTINUE;
    END IF;

    status := 'ACQUIRED';
    slot_token := encode(convert_to(jsonb_build_object('host', v_host_slot_id, 'site', v_site_slot_id)::text, 'UTF8'), 'base64');
    throttle_code := v_throttle_code;
    breaker_open_until := v_breaker_open_until;
    breaker_reason := v_breaker_reason;
    breaker_version := v_breaker_version;
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
