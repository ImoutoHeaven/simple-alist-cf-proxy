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
-- Download Ticket State Table Schema
-- ========================================
-- Purpose: Track issuance, first use, and hard expiry per signed ticket
-- Compatible with: PostgreSQL

CREATE TABLE IF NOT EXISTS "DOWNLOAD_TICKET_STATE_TABLE" (
  "TICKET_HASH" TEXT NOT NULL,
  "ISSUED_AT" BIGINT NOT NULL,
  "FIRST_USED_AT" BIGINT NULL,
  "HARD_EXPIRE_AT" BIGINT NOT NULL,
  "IP_HASH" TEXT NULL,
  "PATH_HASH" TEXT NULL,
  PRIMARY KEY ("TICKET_HASH")
);

CREATE INDEX IF NOT EXISTS idx_download_ticket_state_hard_expire
  ON "DOWNLOAD_TICKET_STATE_TABLE"("HARD_EXPIRE_AT");


-- ========================================
-- Stored Procedure: Seed Download Ticket State
-- ========================================
CREATE OR REPLACE FUNCTION download_seed_ticket(
  p_ticket_hash TEXT,
  p_issued_at BIGINT,
  p_hard_expire_at BIGINT,
  p_ip_hash TEXT DEFAULT NULL,
  p_path_hash TEXT DEFAULT NULL,
  p_table_name TEXT DEFAULT 'DOWNLOAD_TICKET_STATE_TABLE'
)
RETURNS JSON AS $$
DECLARE
  sql TEXT;
BEGIN
  sql := format(
    'INSERT INTO %1$I ("TICKET_HASH", "ISSUED_AT", "FIRST_USED_AT", "HARD_EXPIRE_AT", "IP_HASH", "PATH_HASH")
     VALUES ($1, $2, NULL, $3, $4, $5)',
    p_table_name
  );

  EXECUTE sql USING p_ticket_hash, p_issued_at, p_hard_expire_at, p_ip_hash, p_path_hash;
  RETURN json_build_object('result', 'seeded');
EXCEPTION
  WHEN unique_violation THEN
    RETURN json_build_object('result', 'collision');
  WHEN others THEN
    RETURN json_build_object('result', 'storage_error', 'error', SQLERRM);
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- Stored Procedure: Read Download Ticket State
-- ========================================
CREATE OR REPLACE FUNCTION download_get_ticket_state(
  p_ticket_hash TEXT,
  p_table_name TEXT DEFAULT 'DOWNLOAD_TICKET_STATE_TABLE'
)
RETURNS TABLE(
  found BOOLEAN,
  ticket_hash TEXT,
  issued_at BIGINT,
  first_used_at BIGINT,
  hard_expire_at BIGINT,
  ip_hash TEXT,
  path_hash TEXT
) AS $$
DECLARE
  sql TEXT;
  v_row_count INTEGER := 0;
BEGIN
  sql := format(
    'SELECT TRUE::BOOLEAN AS found,
            "TICKET_HASH",
            "ISSUED_AT",
            "FIRST_USED_AT",
            "HARD_EXPIRE_AT",
            "IP_HASH",
            "PATH_HASH"
       FROM %1$I
      WHERE "TICKET_HASH" = $1
      LIMIT 1',
    p_table_name
  );

  RETURN QUERY EXECUTE sql USING p_ticket_hash;
  GET DIAGNOSTICS v_row_count = ROW_COUNT;

  IF v_row_count = 0 THEN
    RETURN QUERY SELECT FALSE, NULL::TEXT, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, NULL::TEXT, NULL::TEXT;
  END IF;
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- Stored Procedure: Mark Download Ticket Used
-- ========================================
CREATE OR REPLACE FUNCTION download_mark_ticket_used(
  p_ticket_hash TEXT,
  p_now BIGINT,
  p_table_name TEXT DEFAULT 'DOWNLOAD_TICKET_STATE_TABLE'
)
RETURNS JSON AS $$
DECLARE
  v_locked_first_used_at BIGINT;
  v_effective_first_used_at BIGINT;
  v_row_count INTEGER := 0;
  sql_select TEXT;
  sql_update TEXT;
BEGIN
  sql_select := format(
    'SELECT "FIRST_USED_AT" FROM %1$I WHERE "TICKET_HASH" = $1 FOR UPDATE',
    p_table_name
  );
  EXECUTE sql_select INTO v_locked_first_used_at USING p_ticket_hash;
  GET DIAGNOSTICS v_row_count = ROW_COUNT;

  IF v_row_count = 0 THEN
    RETURN json_build_object('result', 'storage_error');
  END IF;

  sql_update := format(
    'UPDATE %1$I
        SET "FIRST_USED_AT" = COALESCE("FIRST_USED_AT", $2)
      WHERE "TICKET_HASH" = $1
      RETURNING "FIRST_USED_AT"',
    p_table_name
  );
  EXECUTE sql_update INTO v_effective_first_used_at USING p_ticket_hash, p_now;
  GET DIAGNOSTICS v_row_count = ROW_COUNT;

  IF v_row_count = 0 OR v_effective_first_used_at IS NULL THEN
    RETURN json_build_object('result', 'storage_error');
  ELSIF v_locked_first_used_at IS NULL THEN
    RETURN json_build_object('result', 'transitioned', 'first_used_at', v_effective_first_used_at);
  ELSE
    RETURN json_build_object('result', 'already_used', 'first_used_at', v_effective_first_used_at);
  END IF;
EXCEPTION
  WHEN others THEN
    RETURN json_build_object('result', 'storage_error');
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- Stored Procedure: Cleanup Expired Tickets
-- ========================================
CREATE OR REPLACE FUNCTION download_cleanup_expired_tickets(
  p_now BIGINT,
  p_table_name TEXT DEFAULT 'DOWNLOAD_TICKET_STATE_TABLE'
)
RETURNS JSON AS $$
DECLARE
  v_deleted_count INTEGER;
BEGIN
  EXECUTE format('DELETE FROM %1$I WHERE "HARD_EXPIRE_AT" < $1', p_table_name)
    USING p_now;
  GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
  RETURN json_build_object('deleted', v_deleted_count);
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
  "HALF_OPEN_RESOLVED_MASK" BIGINT NOT NULL DEFAULT 0,
  "HALF_OPEN_SUCCESS_MASK" BIGINT NOT NULL DEFAULT 0,
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
CREATE OR REPLACE FUNCTION func_authorize_breaker_attempt(
  p_hostname_hash TEXT,
  p_hostname TEXT,
  p_now INTEGER,
  p_open_cap_seconds INTEGER,
  p_close_threshold_percent INTEGER,
  p_half_open_success_threshold INTEGER,
  p_half_open_close_mode TEXT,
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
  v_open_cap_seconds INTEGER;
  v_close_threshold NUMERIC;
  v_half_open_success_threshold INTEGER;
  v_half_open_close_mode TEXT;
  v_half_open_max_probe_count INTEGER;
  v_half_open_max_seconds INTEGER;
  v_half_open_timeout_mode TEXT;
  v_timeout_open_seconds INTEGER := 0;
  v_issued_mask BIGINT := 0;
  v_pending_mask BIGINT := 0;
  v_success_count INTEGER := 0;
  v_should_close BOOLEAN := FALSE;

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
  v_half_open_resolved_mask BIGINT := 0;
  v_half_open_success_mask BIGINT := 0;
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

  IF p_open_cap_seconds IS NULL
    OR p_close_threshold_percent IS NULL
    OR p_half_open_success_threshold IS NULL
    OR p_half_open_close_mode IS NULL
    OR BTRIM(p_half_open_close_mode) = ''
    OR p_half_open_max_probe_count IS NULL
    OR p_half_open_max_seconds IS NULL
    OR p_half_open_timeout_mode IS NULL
    OR BTRIM(p_half_open_timeout_mode) = '' THEN
    RAISE EXCEPTION 'func_authorize_breaker_attempt requires non-null authorize settings';
  END IF;

  IF p_half_open_max_probe_count > v_half_open_ticket_mask_limit THEN
    RAISE EXCEPTION 'func_authorize_breaker_attempt half-open max probe count exceeds BIGINT mask capacity: %', p_half_open_max_probe_count;
  END IF;

  v_open_cap_seconds := GREATEST(1, p_open_cap_seconds);
  v_close_threshold := GREATEST(0, p_close_threshold_percent) / 100.0;
  v_half_open_success_threshold := GREATEST(1, p_half_open_success_threshold);
  v_half_open_close_mode := LOWER(BTRIM(p_half_open_close_mode));
  v_half_open_max_probe_count := GREATEST(1, p_half_open_max_probe_count);
  v_half_open_max_seconds := GREATEST(1, p_half_open_max_seconds);
  v_half_open_timeout_mode := LOWER(BTRIM(p_half_open_timeout_mode));
  IF v_half_open_close_mode NOT IN ('and', 'or') THEN
    RAISE EXCEPTION 'func_authorize_breaker_attempt invalid p_half_open_close_mode: %', p_half_open_close_mode;
  END IF;
  IF v_half_open_timeout_mode NOT IN ('open', 'close', 'partial-close') THEN
    RAISE EXCEPTION 'func_authorize_breaker_attempt invalid p_half_open_timeout_mode: %', p_half_open_timeout_mode;
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
      tp."HALF_OPEN_RESOLVED_MASK",
      tp."HALF_OPEN_SUCCESS_MASK",
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
      v_half_open_resolved_mask,
      v_half_open_success_mask,
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
  v_half_open_resolved_mask := COALESCE(v_half_open_resolved_mask, 0);
  v_half_open_success_mask := COALESCE(v_half_open_success_mask, 0);
  v_last_open_seconds := COALESCE(v_last_open_seconds, 0);
  v_version := COALESCE(v_version, 0);
  v_initial_state := v_state;

  IF v_half_open_budget > v_half_open_ticket_mask_limit
    OR v_half_open_issued > v_half_open_ticket_mask_limit
    OR v_half_open_resolved_mask < 0
    OR v_half_open_success_mask < 0
    OR (v_half_open_success_mask & ~v_half_open_resolved_mask) <> 0 THEN
    RAISE EXCEPTION 'func_authorize_breaker_attempt half-open ticket state exceeds BIGINT mask capacity';
  END IF;

  IF v_state = 'open' AND v_open_until IS NULL THEN
    RAISE EXCEPTION 'func_authorize_breaker_attempt invalid open row without OPEN_UNTIL';
  END IF;

  IF v_state = 'half_open' AND v_half_open_deadline IS NULL THEN
    RAISE EXCEPTION 'func_authorize_breaker_attempt invalid half_open row without HALF_OPEN_DEADLINE';
  END IF;

  IF v_state = 'open' AND v_open_until <= v_now THEN
    v_state := 'half_open';
    v_open_until := NULL;
    v_success_streak := 0;
    v_half_open_since := v_now;
    v_half_open_budget := v_half_open_max_probe_count;
    v_half_open_issued := 0;
    v_half_open_resolved_mask := 0;
    v_half_open_success_mask := 0;
    v_half_open_deadline := v_now + v_half_open_max_seconds;
  ELSIF v_state = 'half_open' THEN
    v_issued_mask := CASE
      WHEN v_half_open_issued >= v_half_open_ticket_mask_limit THEN 9223372036854775807::BIGINT
      WHEN v_half_open_issued > 0 THEN (1::BIGINT << v_half_open_issued) - 1
      ELSE 0
    END;
    v_pending_mask := v_issued_mask & ~v_half_open_resolved_mask;
    -- v_success_count := bit_count(v_half_open_success_mask)
    v_success_count := bit_count(v_half_open_success_mask::bit(63))::INTEGER;

    IF v_half_open_deadline <= v_now THEN
      IF v_half_open_timeout_mode = 'open' THEN
        v_timeout_open_seconds := CASE
          WHEN v_last_open_seconds > 0 THEN LEAST(v_open_cap_seconds, v_last_open_seconds * 2)
          ELSE 1
        END;
        v_state := 'open';
        v_open_until := v_now + v_timeout_open_seconds;
        v_success_streak := 0;
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
        IF v_half_open_success_mask <> 0 THEN
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
          v_timeout_open_seconds := CASE
            WHEN v_last_open_seconds > 0 THEN LEAST(v_open_cap_seconds, v_last_open_seconds * 2)
            ELSE 1
          END;
          v_state := 'open';
          v_open_until := v_now + v_timeout_open_seconds;
          v_success_streak := 0;
          v_last_open_seconds := v_timeout_open_seconds;
        END IF;
      END IF;

      v_half_open_since := NULL;
      v_half_open_budget := 0;
      v_half_open_issued := 0;
      v_half_open_resolved_mask := 0;
      v_half_open_success_mask := 0;
      v_half_open_deadline := NULL;
    ELSIF v_half_open_issued = v_half_open_budget
      AND v_half_open_issued > 0
      AND v_pending_mask = 0 THEN
      v_should_close := CASE
        WHEN v_half_open_close_mode = 'or' THEN
          v_success_count >= v_half_open_success_threshold
          OR v_ewma_score <= v_close_threshold
        ELSE
          v_success_count >= v_half_open_success_threshold
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
      ELSE
        v_timeout_open_seconds := CASE
          WHEN v_last_open_seconds > 0 THEN LEAST(v_open_cap_seconds, v_last_open_seconds * 2)
          ELSE 1
        END;
        v_state := 'open';
        v_open_until := v_now + v_timeout_open_seconds;
        v_success_streak := 0;
        v_last_open_seconds := v_timeout_open_seconds;
      END IF;

      v_half_open_since := NULL;
      v_half_open_budget := 0;
      v_half_open_issued := 0;
      v_half_open_resolved_mask := 0;
      v_half_open_success_mask := 0;
      v_half_open_deadline := NULL;
    END IF;
  ELSIF v_state = 'closed' THEN
    v_open_until := NULL;
    v_success_streak := 0;
    v_half_open_since := NULL;
    v_half_open_budget := 0;
    v_half_open_issued := 0;
    v_half_open_resolved_mask := 0;
    v_half_open_success_mask := 0;
    v_half_open_deadline := NULL;
  END IF;

  IF v_initial_state <> 'half_open' AND v_state = 'half_open' THEN
    v_version := v_version + 1;
  ELSIF v_initial_state = 'half_open' AND v_state <> 'half_open' THEN
    v_version := v_version + 1;
  END IF;

  IF v_state = 'half_open'
    AND v_half_open_deadline > v_now
    AND v_half_open_issued < v_half_open_budget THEN
    v_half_open_issued := v_half_open_issued + 1;
    v_attempt_granted := TRUE;
    v_attempt_ticket := v_half_open_issued;
  END IF;

  IF v_state = 'open' OR v_state = 'closed' THEN
    v_half_open_since := NULL;
    v_half_open_budget := 0;
    v_half_open_issued := 0;
    v_half_open_resolved_mask := 0;
    v_half_open_success_mask := 0;
    v_half_open_deadline := NULL;
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
    "HALF_OPEN_RESOLVED_MASK" = v_half_open_resolved_mask,
    "HALF_OPEN_SUCCESS_MASK" = v_half_open_success_mask,
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

CREATE OR REPLACE FUNCTION download_authorize_breaker_attempt(
  p_hostname_hash TEXT,
  p_hostname TEXT,
  p_now INTEGER,
  p_open_cap_seconds INTEGER,
  p_close_threshold_percent INTEGER,
  p_half_open_success_threshold INTEGER,
  p_half_open_close_mode TEXT,
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
BEGIN
  RETURN QUERY
  SELECT *
  FROM func_authorize_breaker_attempt(
    p_hostname_hash,
    p_hostname,
    p_now,
    p_open_cap_seconds,
    p_close_threshold_percent,
    p_half_open_success_threshold,
    p_half_open_close_mode,
    p_half_open_max_probe_count,
    p_half_open_max_seconds,
    p_half_open_timeout_mode
  );
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
  v_half_open_resolved_mask BIGINT := 0;
  v_half_open_success_mask BIGINT := 0;
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
  v_open_seconds INTEGER := 0;
  v_timeout_open_seconds INTEGER := 0;
  v_has_attempt_version BOOLEAN := p_attempt_version IS NOT NULL;
  v_has_attempt_ticket BOOLEAN := p_attempt_ticket IS NOT NULL;
  v_ticket_mask BIGINT := 0;
  v_issued_mask BIGINT := 0;
  v_pending_mask BIGINT := 0;
  v_success_count INTEGER := 0;
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
      tp."HALF_OPEN_RESOLVED_MASK",
      tp."HALF_OPEN_SUCCESS_MASK",
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
      v_half_open_resolved_mask,
      v_half_open_success_mask,
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
      IF v_has_attempt_version OR v_has_attempt_ticket THEN
        RETURN QUERY
        SELECT
          p_hostname_hash,
          COALESCE(NULLIF(p_hostname, ''), p_hostname_hash),
          COALESCE(NULLIF(v_state, ''), 'closed'),
          v_open_until,
          COALESCE(v_ewma_score, 0),
          COALESCE(v_total_samples, 0),
          COALESCE(v_samples_since_reset, 0),
          COALESCE(v_consecutive_error_count, 0),
          COALESCE(v_success_streak, 0),
          v_half_open_deadline,
          v_last_sample_at,
          v_last_error_code,
          v_open_reason,
          COALESCE(v_last_open_seconds, 0),
          COALESCE(v_version, 0);
        RETURN;
      END IF;

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
  v_half_open_resolved_mask := COALESCE(v_half_open_resolved_mask, 0);
  v_half_open_success_mask := COALESCE(v_half_open_success_mask, 0);
  v_last_open_seconds := COALESCE(v_last_open_seconds, 0);
  v_version := COALESCE(v_version, 0);
  v_initial_state := v_state;

  IF v_half_open_budget > v_half_open_ticket_mask_limit
    OR v_half_open_issued > v_half_open_ticket_mask_limit
    OR v_half_open_resolved_mask < 0
    OR v_half_open_success_mask < 0
    OR (v_half_open_success_mask & ~v_half_open_resolved_mask) <> 0 THEN
    RAISE EXCEPTION 'download_report_breaker_sample half-open ticket state exceeds BIGINT mask capacity';
  END IF;

  IF v_has_attempt_version <> v_has_attempt_ticket THEN
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

  IF v_state = 'half_open' THEN
    IF v_half_open_deadline IS NULL OR v_half_open_deadline <= v_now THEN
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

    IF NOT (v_has_attempt_version AND v_has_attempt_ticket) THEN
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

    IF p_attempt_version <> v_version THEN
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

    IF p_attempt_ticket < 1 OR p_attempt_ticket > v_half_open_issued OR p_attempt_ticket > v_half_open_ticket_mask_limit THEN
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
    IF (v_half_open_resolved_mask & v_ticket_mask) <> 0 THEN
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

    v_half_open_resolved_mask := v_half_open_resolved_mask | v_ticket_mask;
    IF v_sample = 0 THEN
      v_half_open_success_mask := v_half_open_success_mask | v_ticket_mask;
    END IF;
  ELSIF v_has_attempt_version AND v_has_attempt_ticket THEN
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
    v_half_open_resolved_mask := 0;
    v_half_open_success_mask := 0;
    v_half_open_deadline := NULL;
  ELSIF v_state = 'open' AND v_open_until IS NOT NULL AND v_open_until <= v_now THEN
    NULL;
  END IF;

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
      v_half_open_resolved_mask := 0;
      v_half_open_success_mask := 0;
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
      v_issued_mask := CASE
        WHEN v_half_open_issued >= v_half_open_ticket_mask_limit THEN 9223372036854775807::BIGINT
        WHEN v_half_open_issued > 0 THEN (1::BIGINT << v_half_open_issued) - 1
        ELSE 0
      END;
      v_pending_mask := v_issued_mask & ~v_half_open_resolved_mask;
      -- v_success_count := bit_count(v_half_open_success_mask)
      v_success_count := bit_count(v_half_open_success_mask::bit(63))::INTEGER;
      v_should_close := CASE
        WHEN v_half_open_close_mode = 'or' THEN
          v_success_count >= v_half_open_success_threshold
          OR v_ewma_score <= v_close_threshold
        ELSE
          v_success_count >= v_half_open_success_threshold
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
        v_half_open_resolved_mask := 0;
        v_half_open_success_mask := 0;
        v_half_open_deadline := NULL;
      ELSIF v_half_open_issued = v_half_open_budget
        AND v_half_open_issued > 0
        AND v_pending_mask = 0 THEN
        v_timeout_open_seconds := CASE
          WHEN v_last_open_seconds > 0 THEN LEAST(v_open_cap_seconds, v_last_open_seconds * 2)
          ELSE 1
        END;
        v_state := 'open';
        v_open_until := v_now + v_timeout_open_seconds;
        v_success_streak := 0;
        v_last_open_seconds := v_timeout_open_seconds;
        v_half_open_since := NULL;
        v_half_open_budget := 0;
        v_half_open_issued := 0;
        v_half_open_resolved_mask := 0;
        v_half_open_success_mask := 0;
        v_half_open_deadline := NULL;
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

  IF v_state = 'open' OR v_state = 'closed' THEN
    v_half_open_since := NULL;
    v_half_open_budget := 0;
    v_half_open_issued := 0;
    v_half_open_resolved_mask := 0;
    v_half_open_success_mask := 0;
    v_half_open_deadline := NULL;
  END IF;

  IF v_initial_state = 'half_open' AND v_state <> 'half_open' THEN
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
    "HALF_OPEN_RESOLVED_MASK" = v_half_open_resolved_mask,
    "HALF_OPEN_SUCCESS_MASK" = v_half_open_success_mask,
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
  v_half_open_resolved_mask BIGINT := 0;
  v_half_open_success_mask BIGINT := 0;
  v_half_open_deadline INTEGER := NULL;
  v_last_sample_at INTEGER := NULL;
  v_last_error_code INTEGER := NULL;
  v_open_reason TEXT := NULL;
  v_last_open_seconds INTEGER := 0;
  v_version BIGINT := 0;
  v_ticket_mask BIGINT := 0;
  v_has_attempt_version BOOLEAN := p_attempt_version IS NOT NULL;
  v_has_attempt_ticket BOOLEAN := p_attempt_ticket IS NOT NULL;
  v_locked BOOLEAN := FALSE;
  v_locked_row_count INTEGER := 0;
BEGIN
  IF p_hostname_hash IS NULL OR p_hostname_hash = '' THEN
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
      tp."HALF_OPEN_RESOLVED_MASK",
      tp."HALF_OPEN_SUCCESS_MASK",
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
      v_half_open_resolved_mask,
      v_half_open_success_mask,
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
      RETURN QUERY
      SELECT
        p_hostname_hash,
        COALESCE(NULLIF(p_hostname, ''), p_hostname_hash),
        COALESCE(NULLIF(v_state, ''), 'closed'),
        v_open_until,
        COALESCE(v_ewma_score, 0),
        COALESCE(v_total_samples, 0),
        COALESCE(v_samples_since_reset, 0),
        COALESCE(v_consecutive_error_count, 0),
        COALESCE(v_success_streak, 0),
        v_half_open_deadline,
        v_last_sample_at,
        v_last_error_code,
        v_open_reason,
        COALESCE(v_last_open_seconds, 0),
        COALESCE(v_version, 0);
      RETURN;
    END IF;
  END LOOP;

  v_hostname := COALESCE(NULLIF(p_hostname, ''), v_hostname, p_hostname_hash);
  v_half_open_resolved_mask := COALESCE(v_half_open_resolved_mask, 0);
  v_half_open_success_mask := COALESCE(v_half_open_success_mask, 0);

  IF v_has_attempt_version = v_has_attempt_ticket
    AND v_has_attempt_version
    AND v_state = 'half_open'
    AND v_half_open_deadline IS NOT NULL
    AND v_half_open_deadline > v_now
    AND p_attempt_version = v_version
    AND p_attempt_ticket >= 1
    AND p_attempt_ticket <= v_half_open_issued
    AND p_attempt_ticket <= v_half_open_ticket_mask_limit THEN
    v_ticket_mask := (1::BIGINT << (p_attempt_ticket - 1));
    IF (v_half_open_resolved_mask & v_ticket_mask) = 0 THEN
      v_half_open_resolved_mask := COALESCE(v_half_open_resolved_mask, 0) | v_ticket_mask;

      UPDATE "THROTTLE_PROTECTION" AS tp SET
        "HALF_OPEN_RESOLVED_MASK" = v_half_open_resolved_mask
      WHERE tp."HOSTNAME_HASH" = p_hostname_hash;
    END IF;
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
  p_now BIGINT DEFAULT NULL
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
  throttle_last_error_code INTEGER
) AS $$
DECLARE
  v_now BIGINT;
  v_cache_record RECORD;
  v_rate_record RECORD;
  v_throttle_record RECORD;
  v_cache_hostname_hash TEXT;
  v_throttle_hostname_hash TEXT;

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
    v_throttle_last_error_code;
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

CREATE TABLE IF NOT EXISTS concurrency_requests (
  request_id text PRIMARY KEY,
  hostname_hash text NOT NULL,
  hostname text NOT NULL,
  site_bucket text NOT NULL,
  ip_bucket text NOT NULL,
  hard_expire_at_ms bigint NOT NULL,
  state text NOT NULL CHECK (state IN ('waiting', 'active', 'released', 'cancelled', 'expired')),
  wait_token text UNIQUE,
  first_wait_at_ms bigint,
  waiter_lease_until_ms bigint,
  lease_id uuid,
  lease_token text,
  lease_expires_at_ms bigint,
  claim_token text,
  claim_state text CHECK (claim_state IN ('unclaimed', 'claimed', 'compensated')),
  claim_claimed_at_ms bigint,
  handoff_state text CHECK (handoff_state IN ('none', 'pending', 'acknowledged', 'compensated')) NOT NULL DEFAULT 'none',
  handoff_token text,
  handoff_deadline_ms bigint,
  handoff_acked_at_ms bigint,
  heartbeat_state text NOT NULL DEFAULT 'none' CHECK (heartbeat_state IN ('none', 'connected', 'grace')),
  heartbeat_generation bigint NOT NULL DEFAULT 0,
  heartbeat_last_at_ms bigint,
  heartbeat_deadline_ms bigint,
  heartbeat_grace_until_ms bigint,
  heartbeat_connected_at_ms bigint,
  heartbeat_disconnected_at_ms bigint,
  heartbeat_terminal_reason text,
  terminal_reason text,
  created_at_ms bigint NOT NULL,
  updated_at_ms bigint NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS concurrency_requests_claim_token_idx
  ON concurrency_requests (claim_token)
  WHERE claim_token IS NOT NULL;

CREATE INDEX IF NOT EXISTS concurrency_requests_waiting_host_idx
  ON concurrency_requests (hostname_hash, site_bucket, ip_bucket, first_wait_at_ms, request_id)
  WHERE state = 'waiting';

CREATE INDEX IF NOT EXISTS concurrency_requests_heartbeat_deadline_idx
  ON concurrency_requests (heartbeat_deadline_ms, request_id)
  WHERE state = 'active' AND heartbeat_deadline_ms IS NOT NULL;

CREATE OR REPLACE FUNCTION cq_apply_heartbeat_terminal_cleanup_trigger()
RETURNS trigger AS $$
BEGIN
  IF NEW.state IN ('released', 'expired', 'cancelled') THEN
    NEW.heartbeat_state := 'none';
    NEW.heartbeat_deadline_ms := NULL;
    NEW.heartbeat_grace_until_ms := NULL;
    NEW.heartbeat_terminal_reason := NEW.terminal_reason;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS cq_concurrency_requests_heartbeat_cleanup ON concurrency_requests;

CREATE TRIGGER cq_concurrency_requests_heartbeat_cleanup
BEFORE INSERT OR UPDATE ON concurrency_requests
FOR EACH ROW
EXECUTE FUNCTION cq_apply_heartbeat_terminal_cleanup_trigger();

CREATE OR REPLACE FUNCTION cq_heartbeat_released_replay_reason(
  p_terminal_reason text,
  p_fallback_reason text DEFAULT 'already_released'
)
RETURNS text AS $$
DECLARE
  v_reason text := COALESCE(NULLIF(BTRIM(COALESCE(p_terminal_reason, '')), ''), NULLIF(BTRIM(COALESCE(p_fallback_reason, '')), ''), 'already_released');
BEGIN
  IF v_reason IN ('heartbeat_start_timeout', 'heartbeat_timeout') THEN
    RETURN v_reason;
  END IF;

  RETURN 'already_released';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cq_apply_request_terminal_transition(
  p_request_id text,
  p_terminal_state text,
  p_terminal_reason text,
  p_now_ms bigint,
  p_claim_state text DEFAULT NULL,
  p_handoff_state text DEFAULT NULL
)
RETURNS boolean AS $$
DECLARE
  v_request concurrency_requests%ROWTYPE;
  v_active_lease concurrency_leases%ROWTYPE;
  v_request_row_count bigint := 0;
  v_active_lease_row_count bigint := 0;
  v_terminal_state text := LOWER(BTRIM(COALESCE(p_terminal_state, '')));
BEGIN
  IF BTRIM(COALESCE(p_request_id, '')) = '' THEN
    RETURN FALSE;
  END IF;

  IF v_terminal_state NOT IN ('released', 'expired') THEN
    RAISE EXCEPTION 'cq_apply_request_terminal_transition invalid terminal state: %', p_terminal_state;
  END IF;

  SELECT *
    INTO v_request
  FROM concurrency_requests
  WHERE request_id = p_request_id
  FOR UPDATE;

  GET DIAGNOSTICS v_request_row_count = ROW_COUNT;

  IF v_request_row_count = 0 OR v_request.state <> 'active' THEN
    RETURN FALSE;
  END IF;

  SELECT *
    INTO v_active_lease
  FROM concurrency_leases
  WHERE request_id = p_request_id
    AND state = 'active'
  FOR UPDATE;

  GET DIAGNOSTICS v_active_lease_row_count = ROW_COUNT;

  UPDATE concurrency_requests
  SET state = v_terminal_state,
      terminal_reason = p_terminal_reason,
      claim_state = CASE WHEN p_claim_state IS NOT NULL THEN p_claim_state ELSE claim_state END,
      handoff_state = CASE WHEN p_handoff_state IS NOT NULL THEN p_handoff_state ELSE handoff_state END,
      heartbeat_state = 'none',
      heartbeat_deadline_ms = NULL,
      heartbeat_grace_until_ms = NULL,
      heartbeat_terminal_reason = p_terminal_reason,
      updated_at_ms = p_now_ms
  WHERE request_id = p_request_id
    AND state = 'active';

  GET DIAGNOSTICS v_request_row_count = ROW_COUNT;

  IF v_request_row_count = 0 THEN
    RETURN FALSE;
  END IF;

  IF v_active_lease_row_count > 0 THEN
    UPDATE concurrency_leases
    SET state = v_terminal_state,
        released_at = COALESCE(released_at, to_timestamp(p_now_ms / 1000.0)),
        updated_at = to_timestamp(p_now_ms / 1000.0)
    WHERE lease_id = v_active_lease.lease_id;
  END IF;

  UPDATE concurrency_host_counters
  SET active_count = GREATEST(active_count - 1, 0),
      updated_at = to_timestamp(p_now_ms / 1000.0)
  WHERE hostname_hash = v_request.hostname_hash;

  UPDATE concurrency_site_counters
  SET active_count = GREATEST(active_count - 1, 0),
      updated_at = to_timestamp(p_now_ms / 1000.0)
  WHERE hostname_hash = v_request.hostname_hash
    AND site_bucket = v_request.site_bucket;

  UPDATE concurrency_site_ip_counters
  SET active_count = GREATEST(active_count - 1, 0),
      updated_at = to_timestamp(p_now_ms / 1000.0)
  WHERE hostname_hash = v_request.hostname_hash
    AND site_bucket = v_request.site_bucket
    AND ip_bucket = v_request.ip_bucket;

  RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cq_expire_active_request_if_due(
  p_request_id text,
  p_now_ms bigint
)
RETURNS boolean AS $$
DECLARE
  v_request concurrency_requests%ROWTYPE;
  v_active_lease concurrency_leases%ROWTYPE;
  v_request_row_count bigint := 0;
  v_active_lease_row_count bigint := 0;
BEGIN
  IF BTRIM(COALESCE(p_request_id, '')) = '' THEN
    RETURN FALSE;
  END IF;

  SELECT *
    INTO v_request
  FROM concurrency_requests
  WHERE request_id = p_request_id
  FOR UPDATE;

  GET DIAGNOSTICS v_request_row_count = ROW_COUNT;

  IF v_request_row_count = 0 OR v_request.state <> 'active' THEN
    RETURN FALSE;
  END IF;

  SELECT *
    INTO v_active_lease
  FROM concurrency_leases
  WHERE request_id = p_request_id
    AND state = 'active'
  FOR UPDATE;

  GET DIAGNOSTICS v_active_lease_row_count = ROW_COUNT;

  IF v_request.hard_expire_at_ms <= p_now_ms
    OR COALESCE(v_request.lease_expires_at_ms, 0) <= p_now_ms
    OR (v_active_lease_row_count > 0 AND (v_active_lease.hard_expire_at_ms <= p_now_ms OR v_active_lease.expires_at_ms <= p_now_ms)) THEN
    RETURN cq_apply_request_terminal_transition(p_request_id, 'expired', 'hard_expired', p_now_ms);
  END IF;

  IF v_request.handoff_state = 'acknowledged'
    AND COALESCE(v_request.heartbeat_state, 'none') = 'none'
    AND v_request.heartbeat_deadline_ms IS NOT NULL
    AND v_request.heartbeat_deadline_ms <= p_now_ms THEN
    RETURN cq_apply_request_terminal_transition(p_request_id, 'released', 'heartbeat_start_timeout', p_now_ms);
  END IF;

  IF COALESCE(v_request.heartbeat_state, 'none') IN ('connected', 'grace')
    AND v_request.heartbeat_deadline_ms IS NOT NULL
    AND v_request.heartbeat_deadline_ms <= p_now_ms THEN
    RETURN cq_apply_request_terminal_transition(p_request_id, 'released', 'heartbeat_timeout', p_now_ms);
  END IF;

  IF v_request.handoff_state = 'pending'
    AND v_request.handoff_deadline_ms IS NOT NULL
    AND v_request.handoff_deadline_ms <= p_now_ms THEN
    RETURN cq_apply_request_terminal_transition(p_request_id, 'released', 'claim_handoff_timeout', p_now_ms, 'compensated', 'compensated');
  END IF;

  RETURN FALSE;
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
RETURNS TABLE(request_id text) AS $$
DECLARE
  v_scope text := LOWER(BTRIM(COALESCE(p_scope, '')));
  v_now_ms bigint := COALESCE(p_now_ms, (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint);
  v_limit integer := GREATEST(COALESCE(p_limit, 500), 0);
  v_row record;
  v_expired boolean := FALSE;
BEGIN
  IF p_hostname_hash IS NULL OR BTRIM(p_hostname_hash) = '' OR v_limit = 0 THEN
    RETURN;
  END IF;

  IF v_scope NOT IN ('host', 'site', 'site_ip') THEN
    RAISE EXCEPTION 'cq_expire_scope invalid scope: %', p_scope;
  END IF;

  IF v_scope IN ('site', 'site_ip') AND (p_site_bucket IS NULL OR BTRIM(p_site_bucket) = '') THEN
    RETURN;
  END IF;

  IF v_scope = 'site_ip' AND (p_ip_bucket IS NULL OR BTRIM(p_ip_bucket) = '') THEN
    RETURN;
  END IF;

  FOR v_row IN
    WITH expired_rows AS (
      SELECT r.request_id,
             CASE
                WHEN r.handoff_state = 'pending' AND r.handoff_deadline_ms IS NOT NULL AND r.handoff_deadline_ms <= v_now_ms THEN r.handoff_deadline_ms
                WHEN r.heartbeat_deadline_ms IS NOT NULL AND r.heartbeat_deadline_ms <= v_now_ms THEN r.heartbeat_deadline_ms
                ELSE l.expires_at_ms
              END AS due_at_ms
      FROM concurrency_requests AS r
      LEFT JOIN concurrency_leases AS l
        ON l.request_id = r.request_id
       AND l.state = 'active'
      WHERE r.state = 'active'
        AND r.hostname_hash = p_hostname_hash
        AND (
          v_scope = 'host'
          OR (v_scope = 'site' AND r.site_bucket = p_site_bucket)
          OR (v_scope = 'site_ip' AND r.site_bucket = p_site_bucket AND r.ip_bucket = p_ip_bucket)
        )
        AND (
          (l.request_id IS NOT NULL AND l.expires_at_ms <= v_now_ms)
          OR (r.heartbeat_deadline_ms IS NOT NULL AND r.heartbeat_deadline_ms <= v_now_ms)
          OR (r.handoff_state = 'pending' AND r.handoff_deadline_ms IS NOT NULL AND r.handoff_deadline_ms <= v_now_ms)
        )
      ORDER BY due_at_ms, r.request_id
      LIMIT v_limit
    )
    SELECT expired_rows.request_id
    FROM expired_rows
  LOOP
    IF NOT pg_try_advisory_xact_lock(3, hashtext(v_row.request_id)) THEN
      CONTINUE;
    END IF;

    v_expired := cq_expire_active_request_if_due(v_row.request_id, v_now_ms);
    IF v_expired THEN
      cq_expire_scope.request_id := v_row.request_id;
      RETURN NEXT;
    END IF;
  END LOOP;

  RETURN;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cq_release(
  p_lease_id uuid,
  p_lease_token text,
  p_reason text,
  p_now_ms bigint DEFAULT NULL
)
RETURNS TABLE(result text, reason text, request_id text) AS $$
DECLARE
  v_now_ms bigint := COALESCE(p_now_ms, (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint);
  v_request concurrency_requests%ROWTYPE;
  v_locked_lease record;
  v_request_id text;
  v_input_terminal_reason text;
  v_terminal_reason text;
  v_locked_lease_row_count bigint := 0;
  v_transitioned boolean := FALSE;
BEGIN
  IF p_lease_id IS NULL THEN
    result := 'noop';
    reason := 'not_found';
    request_id := NULL;
    RETURN NEXT;
    RETURN;
  END IF;

  SELECT concurrency_leases.request_id
    INTO v_request_id
  FROM concurrency_leases
  WHERE lease_id = p_lease_id;

  IF v_request_id IS NULL THEN
    result := 'noop';
    reason := 'not_found';
    request_id := NULL;
    RETURN NEXT;
    RETURN;
  END IF;

  PERFORM pg_advisory_xact_lock(3, hashtext(v_request_id));

  SELECT *
    INTO v_request
  FROM concurrency_requests
  WHERE concurrency_requests.request_id = v_request_id
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
    request_id := NULL;
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.state = 'released' THEN
    result := 'noop';
    reason := 'already_released';
    request_id := v_request_id;
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.state = 'expired' THEN
    result := 'noop';
    reason := 'expired';
    request_id := v_request_id;
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.hard_expire_at_ms <= v_now_ms
    OR COALESCE(v_request.lease_expires_at_ms, 0) <= v_now_ms
    OR v_locked_lease.hard_expire_at_ms <= v_now_ms
    OR v_locked_lease.expires_at_ms <= v_now_ms THEN
    v_transitioned := cq_apply_request_terminal_transition(v_request_id, 'expired', 'hard_expired', v_now_ms);
    IF v_transitioned THEN
      result := 'expired';
      reason := 'hard_expired';
      request_id := v_request_id;
      RETURN NEXT;
      RETURN;
    END IF;
  END IF;

  IF v_locked_lease.lease_token IS DISTINCT FROM p_lease_token THEN
    result := 'noop';
    reason := 'token_mismatch';
    request_id := NULL;
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_locked_lease.state <> 'active' THEN
    result := 'noop';
    reason := 'not_found';
    request_id := NULL;
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.state <> 'active' THEN
    result := 'noop';
    reason := 'not_found';
    request_id := NULL;
    RETURN NEXT;
    RETURN;
  END IF;

  v_input_terminal_reason := COALESCE(NULLIF(BTRIM(COALESCE(p_reason, '')), ''), 'already_released');
  v_terminal_reason := CASE
    WHEN v_input_terminal_reason IN ('grant_delivery_failed', 'acquire_delivery_failed') THEN 'final_cleanup'
    ELSE v_input_terminal_reason
  END;

  v_transitioned := cq_apply_request_terminal_transition(
    v_request_id,
    'released',
    v_terminal_reason,
    v_now_ms,
    CASE WHEN v_input_terminal_reason IN ('grant_delivery_failed', 'acquire_delivery_failed') THEN 'compensated' ELSE NULL END
  );

  IF NOT v_transitioned THEN
    SELECT state, COALESCE(terminal_reason, '')
      INTO v_request.state, v_request.terminal_reason
    FROM concurrency_requests
    WHERE concurrency_requests.request_id = v_request_id;

    result := 'noop';
    reason := CASE
      WHEN v_request.state = 'expired' THEN 'expired'
      WHEN v_request.state = 'released' THEN 'already_released'
      ELSE 'not_found'
    END;
    request_id := CASE WHEN reason IN ('expired', 'already_released') THEN v_request_id ELSE NULL END;
    RETURN NEXT;
    RETURN;
  END IF;

  result := 'released';
  reason := NULL;
  request_id := v_request_id;
  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cq_cancel(
  p_request_id text,
  p_hostname text,
  p_hostname_hash text,
  p_site_bucket text,
  p_ip_bucket text,
  p_hard_expire_at_ms bigint,
  p_reason text,
  p_now_ms bigint DEFAULT NULL
)
RETURNS TABLE(result text, reason text) AS $$
DECLARE
  v_now_ms bigint := COALESCE(p_now_ms, (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint);
  v_request_id text := BTRIM(COALESCE(p_request_id, ''));
  v_hostname text := BTRIM(COALESCE(p_hostname, ''));
  v_hostname_hash text := BTRIM(COALESCE(p_hostname_hash, ''));
  v_site_bucket text := COALESCE(NULLIF(BTRIM(COALESCE(p_site_bucket, '')), ''), 'unknown');
  v_ip_bucket text := COALESCE(NULLIF(BTRIM(COALESCE(p_ip_bucket, '')), ''), 'unknown');
  v_request record;
  v_request_row_count bigint := 0;
BEGIN
  IF v_request_id = '' OR v_hostname = '' OR v_hostname_hash = '' OR p_hard_expire_at_ms IS NULL OR p_hard_expire_at_ms <= 0 THEN
    result := 'noop';
    reason := 'already_terminal';
    RETURN NEXT;
    RETURN;
  END IF;

  PERFORM pg_advisory_xact_lock(3, hashtext(v_request_id));

  SELECT *
    INTO v_request
  FROM concurrency_requests
  WHERE request_id = v_request_id
  FOR UPDATE;

  GET DIAGNOSTICS v_request_row_count = ROW_COUNT;

  IF v_request_row_count = 0 THEN
    INSERT INTO concurrency_requests (
      request_id,
      hostname_hash,
      hostname,
      site_bucket,
      ip_bucket,
      hard_expire_at_ms,
      state,
      terminal_reason,
      created_at_ms,
      updated_at_ms
    ) VALUES (
      v_request_id,
      v_hostname_hash,
      v_hostname,
      v_site_bucket,
      v_ip_bucket,
      p_hard_expire_at_ms,
      'cancelled',
      'request_cancelled',
      v_now_ms,
      v_now_ms
    );
    result := 'cancelled';
    reason := NULL;
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.hostname_hash IS DISTINCT FROM v_hostname_hash
    OR v_request.hostname IS DISTINCT FROM v_hostname
    OR v_request.site_bucket IS DISTINCT FROM v_site_bucket
    OR v_request.ip_bucket IS DISTINCT FROM v_ip_bucket
    OR v_request.hard_expire_at_ms IS DISTINCT FROM p_hard_expire_at_ms THEN
    RAISE EXCEPTION 'cq_cancel request_id tuple mismatch';
  END IF;

  IF v_request.state = 'active' THEN
    RAISE EXCEPTION 'cq_cancel must release active lease';
  END IF;

  IF v_request.state = 'waiting' THEN
    UPDATE concurrency_requests
    SET state = 'cancelled',
        terminal_reason = 'request_cancelled',
        updated_at_ms = v_now_ms
    WHERE request_id = v_request_id;
    result := 'cancelled';
    reason := NULL;
    RETURN NEXT;
    RETURN;
  END IF;

  result := 'noop';
  reason := 'already_terminal';
  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cq_promote_waiting_request(
  p_request_id text,
  p_hostname_hash text,
  p_site_bucket text,
  p_ip_bucket text,
  p_hard_expire_at_ms bigint,
  p_now_ms bigint,
  p_host_max_in_flight integer DEFAULT 0,
  p_site_max_in_flight integer DEFAULT 0,
  p_site_ip_max_in_flight integer DEFAULT 0
)
RETURNS TABLE(result text, lease_id uuid, lease_token text, expires_at_ms bigint, wait_token text, scope text, reason text, retry_after integer, claim_token text) AS $$
DECLARE
  v_now_ms bigint := COALESCE(p_now_ms, (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint);
  v_request_id text := BTRIM(COALESCE(p_request_id, ''));
  v_hostname_hash text := BTRIM(COALESCE(p_hostname_hash, ''));
  v_site_bucket text := COALESCE(NULLIF(BTRIM(COALESCE(p_site_bucket, '')), ''), 'unknown');
  v_ip_bucket text := COALESCE(NULLIF(BTRIM(COALESCE(p_ip_bucket, '')), ''), 'unknown');
  v_request record;
  v_request_row_count bigint := 0;
  v_host_count integer := 0;
  v_site_count integer := 0;
  v_site_ip_count integer := 0;
  v_min_expires_at_ms bigint := NULL;
  v_wait_scope text := NULL;
  v_retry_after integer := 1;
  v_lease_seed text;
  v_new_lease_token text;
  v_new_lease_id uuid;
  v_new_claim_token text;
  v_active_lease concurrency_leases%ROWTYPE;
  v_active_lease_row_count bigint := 0;
  v_transitioned boolean := FALSE;
BEGIN
  IF v_request_id = '' THEN
    RAISE EXCEPTION 'cq_promote_waiting_request request_id is required';
  END IF;

  IF v_hostname_hash = '' THEN
    RAISE EXCEPTION 'cq_promote_waiting_request hostname_hash is required';
  END IF;

  PERFORM pg_advisory_xact_lock(3, hashtext(v_request_id));

  SELECT *
    INTO v_request
  FROM concurrency_requests
  WHERE request_id = v_request_id
  FOR UPDATE;

  GET DIAGNOSTICS v_request_row_count = ROW_COUNT;

  IF v_request_row_count = 0 THEN
    RAISE EXCEPTION 'cq_promote_waiting_request request not found';
  END IF;

  IF v_request.hostname_hash IS DISTINCT FROM v_hostname_hash
    OR v_request.site_bucket IS DISTINCT FROM v_site_bucket
    OR v_request.ip_bucket IS DISTINCT FROM v_ip_bucket
    OR v_request.hard_expire_at_ms IS DISTINCT FROM p_hard_expire_at_ms THEN
    RAISE EXCEPTION 'cq_acquire request_id tuple mismatch';
  END IF;

  CASE v_request.state
    WHEN 'active' THEN
      SELECT *
        INTO v_active_lease
      FROM concurrency_leases
      WHERE request_id = v_request_id
        AND state = 'active'
      FOR UPDATE;

      GET DIAGNOSTICS v_active_lease_row_count = ROW_COUNT;

      IF v_request.hard_expire_at_ms <= v_now_ms
        OR COALESCE(v_request.lease_expires_at_ms, 0) <= v_now_ms
        OR (v_active_lease_row_count > 0 AND (v_active_lease.hard_expire_at_ms <= v_now_ms OR v_active_lease.expires_at_ms <= v_now_ms)) THEN
        v_transitioned := cq_apply_request_terminal_transition(v_request_id, 'expired', 'hard_expired', v_now_ms);

        result := 'expired';
        lease_id := NULL;
        lease_token := NULL;
        expires_at_ms := NULL;
        wait_token := NULL;
        scope := NULL;
        reason := CASE
          WHEN v_transitioned THEN 'hard_expired'
          ELSE COALESCE(v_request.terminal_reason, 'hard_expired')
        END;
        retry_after := NULL;
        claim_token := NULL;
        RETURN NEXT;
        RETURN;
      END IF;

      result := 'conflict';
      lease_id := NULL;
      lease_token := NULL;
      expires_at_ms := NULL;
      wait_token := NULL;
      scope := NULL;
      reason := CASE WHEN v_request.claim_state = 'claimed' THEN 'grant_already_claimed' ELSE 'grant_unclaimed' END;
      retry_after := NULL;
      claim_token := NULL;
      RETURN NEXT;
      RETURN;
    WHEN 'released' THEN
      result := 'released';
      lease_id := NULL;
      lease_token := NULL;
      expires_at_ms := NULL;
      wait_token := NULL;
      scope := NULL;
      reason := COALESCE(v_request.terminal_reason, 'already_released');
      retry_after := NULL;
      claim_token := NULL;
      RETURN NEXT;
      RETURN;
    WHEN 'cancelled' THEN
      result := 'cancelled';
      lease_id := NULL;
      lease_token := NULL;
      expires_at_ms := NULL;
      wait_token := NULL;
      scope := NULL;
      reason := 'request_cancelled';
      retry_after := NULL;
      claim_token := NULL;
      RETURN NEXT;
      RETURN;
    WHEN 'expired' THEN
      result := 'expired';
      lease_id := NULL;
      lease_token := NULL;
      expires_at_ms := NULL;
      wait_token := NULL;
      scope := NULL;
      reason := COALESCE(v_request.terminal_reason, 'hard_expired');
      retry_after := NULL;
      claim_token := NULL;
      RETURN NEXT;
      RETURN;
    WHEN 'waiting' THEN
      NULL;
    ELSE
      RAISE EXCEPTION 'cq_promote_waiting_request invalid request state: %', v_request.state;
  END CASE;

  IF v_request.hard_expire_at_ms <= v_now_ms THEN
    UPDATE concurrency_requests
    SET state = 'expired',
        terminal_reason = 'hard_expired',
        updated_at_ms = v_now_ms
    WHERE request_id = v_request_id;

    result := 'expired';
    lease_id := NULL;
    lease_token := NULL;
    expires_at_ms := NULL;
    wait_token := NULL;
    scope := NULL;
    reason := 'hard_expired';
    retry_after := NULL;
    claim_token := NULL;
    RETURN NEXT;
    RETURN;
  END IF;

  IF COALESCE(v_request.waiter_lease_until_ms, 0) <= v_now_ms THEN
    UPDATE concurrency_requests
    SET state = 'expired',
        terminal_reason = 'waiter_detached_timeout',
        updated_at_ms = v_now_ms
    WHERE request_id = v_request_id;

    result := 'expired';
    lease_id := NULL;
    lease_token := NULL;
    expires_at_ms := NULL;
    wait_token := NULL;
    scope := NULL;
    reason := 'waiter_detached_timeout';
    retry_after := NULL;
    claim_token := NULL;
    RETURN NEXT;
    RETURN;
  END IF;

  PERFORM cq_expire_scope('host', v_hostname_hash, NULL, NULL, v_now_ms, 500);

  INSERT INTO concurrency_host_counters (hostname_hash, hostname, active_count)
  VALUES (v_hostname_hash, v_request.hostname, 0)
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

    v_wait_scope := 'host';
    v_retry_after := CASE
      WHEN v_min_expires_at_ms IS NOT NULL AND v_min_expires_at_ms > v_now_ms THEN GREATEST(1, CEIL((v_min_expires_at_ms - v_now_ms) / 1000.0)::integer)
      ELSE 1
    END;
  ELSE
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

      v_wait_scope := 'site';
      v_retry_after := CASE
        WHEN v_min_expires_at_ms IS NOT NULL AND v_min_expires_at_ms > v_now_ms THEN GREATEST(1, CEIL((v_min_expires_at_ms - v_now_ms) / 1000.0)::integer)
        ELSE 1
      END;
    ELSE
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

        v_wait_scope := 'site_ip';
        v_retry_after := CASE
          WHEN v_min_expires_at_ms IS NOT NULL AND v_min_expires_at_ms > v_now_ms THEN GREATEST(1, CEIL((v_min_expires_at_ms - v_now_ms) / 1000.0)::integer)
          ELSE 1
        END;
      END IF;
    END IF;
  END IF;

  IF v_wait_scope IS NOT NULL THEN
    result := 'wait';
    lease_id := NULL;
    lease_token := NULL;
    expires_at_ms := NULL;
    wait_token := v_request.wait_token;
    scope := v_wait_scope;
    reason := NULL;
    retry_after := v_retry_after;
    claim_token := NULL;
    RETURN NEXT;
    RETURN;
  END IF;

  v_lease_seed := v_request_id || '|' || v_hostname_hash || '|' || v_site_bucket || '|' || v_ip_bucket || '|' || p_hard_expire_at_ms::text;
  v_new_lease_token := md5(v_lease_seed || '|token');
  v_new_lease_id := cq_make_uuid(v_lease_seed || '|lease_id');
  v_new_claim_token := md5(v_lease_seed || '|claim|' || v_now_ms::text);

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
    v_new_lease_id,
    v_new_lease_token,
    v_request_id,
    v_hostname_hash,
    v_request.hostname,
    v_site_bucket,
    v_ip_bucket,
    p_hard_expire_at_ms,
    p_hard_expire_at_ms,
    to_timestamp(p_hard_expire_at_ms / 1000.0),
    'active',
    NULL,
    now(),
    now()
  );

  UPDATE concurrency_requests
  SET state = 'active',
      lease_id = v_new_lease_id,
      lease_token = v_new_lease_token,
      lease_expires_at_ms = p_hard_expire_at_ms,
      claim_token = v_new_claim_token,
      claim_state = 'unclaimed',
      claim_claimed_at_ms = NULL,
      handoff_state = 'none',
      handoff_token = NULL,
      handoff_deadline_ms = NULL,
      handoff_acked_at_ms = NULL,
      terminal_reason = NULL,
      updated_at_ms = v_now_ms
  WHERE request_id = v_request_id;

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
  lease_id := v_new_lease_id;
  lease_token := v_new_lease_token;
  expires_at_ms := p_hard_expire_at_ms;
  wait_token := NULL;
  scope := NULL;
  reason := NULL;
  retry_after := NULL;
  claim_token := v_new_claim_token;
  RETURN NEXT;
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
  p_wait_token text DEFAULT NULL,
  p_wait_poll_window_ms integer DEFAULT 0,
  p_wait_reconnect_grace_ms integer DEFAULT 0,
  p_host_max_in_flight integer DEFAULT 0,
  p_site_max_in_flight integer DEFAULT 0,
  p_site_ip_max_in_flight integer DEFAULT 0,
  p_cleanup_limit integer DEFAULT 500
)
RETURNS TABLE(result text, lease_id uuid, lease_token text, expires_at_ms bigint, wait_token text, scope text, reason text, retry_after integer, claim_token text) AS $$
DECLARE
  v_now_ms bigint := COALESCE(p_now_ms, (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint);
  v_site_bucket text := COALESCE(NULLIF(BTRIM(COALESCE(p_site_bucket, '')), ''), 'unknown');
  v_ip_bucket text := COALESCE(NULLIF(BTRIM(COALESCE(p_ip_bucket, '')), ''), 'unknown');
  v_hostname_hash text := BTRIM(COALESCE(p_hostname_hash, ''));
  v_hostname text := COALESCE(NULLIF(BTRIM(COALESCE(p_hostname, '')), ''), v_hostname_hash);
  v_request_id text := BTRIM(COALESCE(p_request_id, ''));
  v_authoritative_request_id text := v_request_id;
  v_wait_token_input text := NULLIF(BTRIM(COALESCE(p_wait_token, '')), '');
  v_request record;
  v_request_row_count bigint := 0;
  v_lease_seed text;
  v_new_wait_token text;
  v_new_lease_token text;
  v_new_lease_id uuid;
  v_new_claim_token text;
  v_retry_after integer := 1;
  v_waiter_lease_until_ms bigint := 0;
  v_host_count integer := 0;
  v_site_count integer := 0;
  v_site_ip_count integer := 0;
  v_min_expires_at_ms bigint := NULL;
  v_wait_scope text := 'host';
  v_active_lease concurrency_leases%ROWTYPE;
  v_active_lease_row_count bigint := 0;
  v_transitioned boolean := FALSE;
BEGIN
  IF v_request_id = '' THEN
    RAISE EXCEPTION 'cq_acquire request_id is required';
  END IF;

  IF v_hostname_hash = '' THEN
    RAISE EXCEPTION 'cq_acquire hostname_hash is required';
  END IF;

  v_waiter_lease_until_ms := v_now_ms
    + GREATEST(COALESCE(p_wait_poll_window_ms, 0), 1)
    + GREATEST(COALESCE(p_wait_reconnect_grace_ms, 0), 1);

  IF v_wait_token_input IS NOT NULL THEN
    SELECT concurrency_requests.request_id
      INTO v_authoritative_request_id
    FROM concurrency_requests
    WHERE concurrency_requests.wait_token = v_wait_token_input;

    GET DIAGNOSTICS v_request_row_count = ROW_COUNT;

    IF v_request_row_count = 0 THEN
      RAISE EXCEPTION 'cq_acquire stale wait token';
    END IF;
  END IF;

  PERFORM pg_advisory_xact_lock(3, hashtext(v_authoritative_request_id));

  IF v_wait_token_input IS NOT NULL THEN
    SELECT *
      INTO v_request
    FROM concurrency_requests
    WHERE request_id = v_authoritative_request_id
    FOR UPDATE;

    GET DIAGNOSTICS v_request_row_count = ROW_COUNT;

    IF v_request_row_count = 0 OR v_request.wait_token IS DISTINCT FROM v_wait_token_input THEN
      RAISE EXCEPTION 'cq_acquire stale wait token';
    END IF;
  ELSE
    SELECT *
      INTO v_request
    FROM concurrency_requests
    WHERE request_id = v_request_id
    FOR UPDATE;

    GET DIAGNOSTICS v_request_row_count = ROW_COUNT;
  END IF;

  IF v_request_row_count > 0 THEN
    IF v_request.request_id IS DISTINCT FROM v_request_id
      OR v_request.hostname_hash IS DISTINCT FROM v_hostname_hash
      OR v_request.site_bucket IS DISTINCT FROM v_site_bucket
      OR v_request.ip_bucket IS DISTINCT FROM v_ip_bucket
      OR v_request.hard_expire_at_ms IS DISTINCT FROM p_hard_expire_at_ms THEN
      RAISE EXCEPTION 'cq_acquire request_id tuple mismatch';
    END IF;

    CASE v_request.state
      WHEN 'active' THEN
        SELECT *
          INTO v_active_lease
        FROM concurrency_leases
        WHERE request_id = v_request_id
          AND state = 'active'
        FOR UPDATE;

        GET DIAGNOSTICS v_active_lease_row_count = ROW_COUNT;

        IF v_request.hard_expire_at_ms <= v_now_ms
          OR COALESCE(v_request.lease_expires_at_ms, 0) <= v_now_ms
          OR (v_active_lease_row_count > 0 AND (v_active_lease.hard_expire_at_ms <= v_now_ms OR v_active_lease.expires_at_ms <= v_now_ms)) THEN
          v_transitioned := cq_apply_request_terminal_transition(v_request_id, 'expired', 'hard_expired', v_now_ms);

          result := 'expired';
          lease_id := NULL;
          lease_token := NULL;
          expires_at_ms := NULL;
          wait_token := NULL;
          scope := NULL;
          reason := CASE
            WHEN v_transitioned THEN 'hard_expired'
            ELSE COALESCE(v_request.terminal_reason, 'hard_expired')
          END;
          retry_after := NULL;
          claim_token := NULL;
          RETURN NEXT;
          RETURN;
        END IF;

        result := 'conflict';
        lease_id := NULL;
        lease_token := NULL;
        expires_at_ms := NULL;
        wait_token := NULL;
        scope := NULL;
        reason := CASE WHEN v_request.claim_state = 'claimed' THEN 'grant_already_claimed' ELSE 'grant_unclaimed' END;
        retry_after := NULL;
        claim_token := NULL;
        RETURN NEXT;
        RETURN;
      WHEN 'released' THEN
        result := 'released';
        lease_id := NULL;
        lease_token := NULL;
        expires_at_ms := NULL;
        wait_token := NULL;
        scope := NULL;
        reason := COALESCE(v_request.terminal_reason, 'already_released');
        retry_after := NULL;
        claim_token := NULL;
        RETURN NEXT;
        RETURN;
      WHEN 'cancelled' THEN
        result := 'cancelled';
        lease_id := NULL;
        lease_token := NULL;
        expires_at_ms := NULL;
        wait_token := NULL;
        scope := NULL;
        reason := 'request_cancelled';
        retry_after := NULL;
        claim_token := NULL;
        RETURN NEXT;
        RETURN;
      WHEN 'expired' THEN
        result := 'expired';
        lease_id := NULL;
        lease_token := NULL;
        expires_at_ms := NULL;
        wait_token := NULL;
        scope := NULL;
        reason := COALESCE(v_request.terminal_reason, 'hard_expired');
        retry_after := NULL;
        claim_token := NULL;
        RETURN NEXT;
        RETURN;
      WHEN 'waiting' THEN
        IF v_request.hard_expire_at_ms <= v_now_ms THEN
          UPDATE concurrency_requests
          SET state = 'expired',
              terminal_reason = 'hard_expired',
              updated_at_ms = v_now_ms
          WHERE request_id = v_request_id;

          result := 'expired';
          lease_id := NULL;
          lease_token := NULL;
          expires_at_ms := NULL;
          wait_token := NULL;
          scope := NULL;
          reason := 'hard_expired';
          retry_after := NULL;
          claim_token := NULL;
          RETURN NEXT;
          RETURN;
        END IF;

        IF COALESCE(v_request.waiter_lease_until_ms, 0) <= v_now_ms THEN
          UPDATE concurrency_requests
          SET state = 'expired',
              terminal_reason = 'waiter_detached_timeout',
              updated_at_ms = v_now_ms
          WHERE request_id = v_request_id;

          result := 'expired';
          lease_id := NULL;
          lease_token := NULL;
          expires_at_ms := NULL;
          wait_token := NULL;
          scope := NULL;
          reason := 'waiter_detached_timeout';
          retry_after := NULL;
          claim_token := NULL;
          RETURN NEXT;
          RETURN;
        END IF;

        IF v_wait_token_input IS NOT NULL THEN
          UPDATE concurrency_requests
          SET waiter_lease_until_ms = v_waiter_lease_until_ms,
              updated_at_ms = v_now_ms
          WHERE request_id = v_request_id;
        END IF;

        result := 'wait';
        lease_id := NULL;
        lease_token := NULL;
        expires_at_ms := NULL;
        wait_token := v_request.wait_token;
        scope := 'host';
        reason := NULL;
        retry_after := 1;
        claim_token := NULL;
        RETURN NEXT;
        RETURN;
    END CASE;
  END IF;

  IF p_hard_expire_at_ms IS NULL OR p_hard_expire_at_ms <= v_now_ms THEN
    result := 'expired';
    lease_id := NULL;
    lease_token := NULL;
    expires_at_ms := NULL;
    wait_token := NULL;
    scope := NULL;
    reason := 'hard_expired';
    retry_after := NULL;
    claim_token := NULL;
    RETURN NEXT;
    RETURN;
  END IF;

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

    v_wait_scope := 'host';
    v_retry_after := CASE
      WHEN v_min_expires_at_ms IS NOT NULL AND v_min_expires_at_ms > v_now_ms THEN GREATEST(1, CEIL((v_min_expires_at_ms - v_now_ms) / 1000.0)::integer)
      ELSE 1
    END;
  ELSE
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

      v_wait_scope := 'site';
      v_retry_after := CASE
        WHEN v_min_expires_at_ms IS NOT NULL AND v_min_expires_at_ms > v_now_ms THEN GREATEST(1, CEIL((v_min_expires_at_ms - v_now_ms) / 1000.0)::integer)
        ELSE 1
      END;
    ELSE
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

        v_wait_scope := 'site_ip';
        v_retry_after := CASE
          WHEN v_min_expires_at_ms IS NOT NULL AND v_min_expires_at_ms > v_now_ms THEN GREATEST(1, CEIL((v_min_expires_at_ms - v_now_ms) / 1000.0)::integer)
          ELSE 1
        END;
      ELSE
        v_wait_scope := NULL;
      END IF;
    END IF;
  END IF;

  IF v_wait_scope IS NOT NULL THEN
    v_lease_seed := v_request_id || '|' || v_hostname_hash || '|' || v_site_bucket || '|' || v_ip_bucket || '|' || p_hard_expire_at_ms::text;
    v_new_wait_token := md5(v_lease_seed || '|wait_token');

    INSERT INTO concurrency_requests (
      request_id,
      hostname_hash,
      hostname,
      site_bucket,
      ip_bucket,
      hard_expire_at_ms,
      state,
      wait_token,
      first_wait_at_ms,
      waiter_lease_until_ms,
      created_at_ms,
      updated_at_ms
    ) VALUES (
      v_request_id,
      v_hostname_hash,
      v_hostname,
      v_site_bucket,
      v_ip_bucket,
      p_hard_expire_at_ms,
      'waiting',
      v_new_wait_token,
      v_now_ms,
      v_waiter_lease_until_ms,
      v_now_ms,
      v_now_ms
    );

    result := 'wait';
    lease_id := NULL;
    lease_token := NULL;
    expires_at_ms := NULL;
    wait_token := v_new_wait_token;
    scope := v_wait_scope;
    reason := NULL;
    retry_after := v_retry_after;
    claim_token := NULL;
    RETURN NEXT;
    RETURN;
  END IF;

  v_lease_seed := v_request_id || '|' || v_hostname_hash || '|' || v_site_bucket || '|' || v_ip_bucket || '|' || p_hard_expire_at_ms::text;
  v_new_lease_token := md5(v_lease_seed || '|token');
  v_new_lease_id := cq_make_uuid(v_lease_seed || '|lease_id');
  v_new_claim_token := md5(v_lease_seed || '|claim|' || v_now_ms::text);

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
    v_new_lease_id,
    v_new_lease_token,
    v_request_id,
    v_hostname_hash,
    v_hostname,
    v_site_bucket,
    v_ip_bucket,
    p_hard_expire_at_ms,
    p_hard_expire_at_ms,
    to_timestamp(p_hard_expire_at_ms / 1000.0),
    'active',
    NULL,
    now(),
    now()
  );

  INSERT INTO concurrency_requests (
    request_id,
    hostname_hash,
    hostname,
    site_bucket,
    ip_bucket,
    hard_expire_at_ms,
    state,
    lease_id,
    lease_token,
    lease_expires_at_ms,
    claim_token,
    claim_state,
    claim_claimed_at_ms,
    handoff_state,
    handoff_token,
    handoff_deadline_ms,
    handoff_acked_at_ms,
    created_at_ms,
    updated_at_ms
  ) VALUES (
    v_request_id,
    v_hostname_hash,
    v_hostname,
    v_site_bucket,
    v_ip_bucket,
    p_hard_expire_at_ms,
    'active',
    v_new_lease_id,
    v_new_lease_token,
    p_hard_expire_at_ms,
    v_new_claim_token,
    'unclaimed',
    NULL,
    'none',
    NULL,
    NULL,
    NULL,
    v_now_ms,
    v_now_ms
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
  lease_id := v_new_lease_id;
  lease_token := v_new_lease_token;
  expires_at_ms := p_hard_expire_at_ms;
  wait_token := NULL;
  scope := NULL;
  reason := NULL;
  retry_after := NULL;
  claim_token := v_new_claim_token;
  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cq_claim_grant(
  p_request_id text,
  p_claim_token text,
  p_now_ms bigint DEFAULT NULL
)
RETURNS TABLE(result text, lease_id uuid, lease_token text, expires_at_ms bigint, handoff_token text, handoff_deadline_ms bigint, reason text) AS $$
DECLARE
  v_now_ms bigint := COALESCE(p_now_ms, (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint);
  v_request_id text := BTRIM(COALESCE(p_request_id, ''));
  v_claim_token text := BTRIM(COALESCE(p_claim_token, ''));
  v_request record;
  v_active_lease concurrency_leases%ROWTYPE;
  v_request_row_count bigint := 0;
  v_active_lease_row_count bigint := 0;
  v_transitioned boolean := FALSE;
  v_granted_lease_id uuid := NULL;
  v_granted_lease_token text := NULL;
  v_granted_expires_at_ms bigint := NULL;
  v_new_handoff_token text := NULL;
  v_new_handoff_deadline_ms bigint := NULL;
BEGIN
  IF v_request_id = '' OR v_claim_token = '' THEN
    result := 'conflict';
    lease_id := NULL;
    lease_token := NULL;
    expires_at_ms := NULL;
    handoff_token := NULL;
    handoff_deadline_ms := NULL;
    reason := 'grant_unclaimed';
    RETURN NEXT;
    RETURN;
  END IF;

  PERFORM pg_advisory_xact_lock(3, hashtext(v_request_id));

  SELECT *
    INTO v_request
  FROM concurrency_requests
  WHERE request_id = v_request_id
  FOR UPDATE;

  GET DIAGNOSTICS v_request_row_count = ROW_COUNT;

  IF v_request_row_count = 0 THEN
    result := 'conflict';
    lease_id := NULL;
    lease_token := NULL;
    expires_at_ms := NULL;
    handoff_token := NULL;
    handoff_deadline_ms := NULL;
    reason := 'grant_unclaimed';
    RETURN NEXT;
    RETURN;
  END IF;

  CASE v_request.state
    WHEN 'active' THEN
      SELECT *
        INTO v_active_lease
      FROM concurrency_leases
      WHERE request_id = v_request_id
        AND state = 'active'
      FOR UPDATE;

      GET DIAGNOSTICS v_active_lease_row_count = ROW_COUNT;

      IF v_request.hard_expire_at_ms <= v_now_ms
        OR COALESCE(v_request.lease_expires_at_ms, 0) <= v_now_ms
        OR (v_active_lease_row_count > 0 AND (v_active_lease.hard_expire_at_ms <= v_now_ms OR v_active_lease.expires_at_ms <= v_now_ms)) THEN
        v_transitioned := cq_apply_request_terminal_transition(v_request_id, 'expired', 'hard_expired', v_now_ms);

        result := 'expired';
        lease_id := NULL;
        lease_token := NULL;
        expires_at_ms := NULL;
        handoff_token := NULL;
        handoff_deadline_ms := NULL;
        reason := CASE
          WHEN v_transitioned THEN 'hard_expired'
          ELSE COALESCE(v_request.terminal_reason, 'hard_expired')
        END;
        RETURN NEXT;
        RETURN;
      END IF;

      IF v_request.handoff_state = 'pending'
        AND v_request.handoff_deadline_ms IS NOT NULL
        AND v_request.handoff_deadline_ms <= v_now_ms THEN
        v_transitioned := cq_apply_request_terminal_transition(
          v_request_id,
          'released',
          'claim_handoff_timeout',
          v_now_ms,
          'compensated',
          'compensated'
        );

        result := 'released';
        lease_id := NULL;
        lease_token := NULL;
        expires_at_ms := NULL;
        handoff_token := NULL;
        handoff_deadline_ms := NULL;
        reason := CASE
          WHEN v_transitioned THEN 'claim_handoff_timeout'
          ELSE COALESCE(v_request.terminal_reason, 'claim_handoff_timeout')
        END;
        RETURN NEXT;
        RETURN;
      END IF;

      IF v_request.claim_token IS DISTINCT FROM v_claim_token THEN
        result := 'conflict';
        lease_id := NULL;
        lease_token := NULL;
        expires_at_ms := NULL;
        handoff_token := NULL;
        handoff_deadline_ms := NULL;
        reason := CASE WHEN v_request.claim_state = 'claimed' THEN 'grant_already_claimed' ELSE 'grant_unclaimed' END;
        RETURN NEXT;
        RETURN;
      END IF;

      v_granted_lease_id := CASE WHEN v_active_lease_row_count > 0 THEN v_active_lease.lease_id ELSE v_request.lease_id END;
      v_granted_lease_token := CASE WHEN v_active_lease_row_count > 0 THEN v_active_lease.lease_token ELSE v_request.lease_token END;
      v_granted_expires_at_ms := CASE WHEN v_active_lease_row_count > 0 THEN v_active_lease.expires_at_ms ELSE v_request.lease_expires_at_ms END;

      IF v_request.claim_state = 'unclaimed' THEN
        v_new_handoff_token := md5(v_request_id || '|' || v_claim_token || '|handoff|' || v_now_ms::text);
        v_new_handoff_deadline_ms := LEAST(v_granted_expires_at_ms - 1, v_request.hard_expire_at_ms - 1, v_now_ms + 5000);

        UPDATE concurrency_requests
        SET claim_state = 'claimed',
            claim_claimed_at_ms = v_now_ms,
            handoff_state = 'pending',
            handoff_token = v_new_handoff_token,
            handoff_deadline_ms = v_new_handoff_deadline_ms,
            handoff_acked_at_ms = NULL,
            updated_at_ms = v_now_ms
        WHERE request_id = v_request_id;

        handoff_token := v_new_handoff_token;
        handoff_deadline_ms := v_new_handoff_deadline_ms;
      ELSIF v_request.claim_state IS DISTINCT FROM 'claimed' THEN
        result := 'conflict';
        lease_id := NULL;
        lease_token := NULL;
        expires_at_ms := NULL;
        handoff_token := NULL;
        handoff_deadline_ms := NULL;
        reason := 'grant_already_claimed';
        RETURN NEXT;
        RETURN;
      ELSE
        handoff_token := v_request.handoff_token;
        handoff_deadline_ms := v_request.handoff_deadline_ms;
      END IF;

      result := 'granted';
      lease_id := v_granted_lease_id;
      lease_token := v_granted_lease_token;
      expires_at_ms := v_granted_expires_at_ms;
      reason := NULL;
      RETURN NEXT;
      RETURN;
    WHEN 'released' THEN
      result := 'released';
      lease_id := NULL;
      lease_token := NULL;
      expires_at_ms := NULL;
      handoff_token := NULL;
      handoff_deadline_ms := NULL;
      reason := COALESCE(v_request.terminal_reason, 'already_released');
      RETURN NEXT;
      RETURN;
    WHEN 'cancelled' THEN
      result := 'cancelled';
      lease_id := NULL;
      lease_token := NULL;
      expires_at_ms := NULL;
      handoff_token := NULL;
      handoff_deadline_ms := NULL;
      reason := 'request_cancelled';
      RETURN NEXT;
      RETURN;
    WHEN 'expired' THEN
      result := 'expired';
      lease_id := NULL;
      lease_token := NULL;
      expires_at_ms := NULL;
      handoff_token := NULL;
      handoff_deadline_ms := NULL;
      reason := COALESCE(v_request.terminal_reason, 'hard_expired');
      RETURN NEXT;
      RETURN;
    ELSE
      result := 'conflict';
      lease_id := NULL;
      lease_token := NULL;
      expires_at_ms := NULL;
      handoff_token := NULL;
      handoff_deadline_ms := NULL;
      reason := 'grant_unclaimed';
      RETURN NEXT;
      RETURN;
  END CASE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cq_ack_handoff(
  p_request_id text,
  p_handoff_token text,
  p_now_ms bigint DEFAULT NULL,
  p_start_timeout_ms bigint DEFAULT NULL
)
RETURNS TABLE(result text, reason text, heartbeat_deadline_ms bigint) AS $$
DECLARE
  v_now_ms bigint := COALESCE(p_now_ms, (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint);
  v_commit_now_ms bigint := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint;
  v_request_id text := BTRIM(COALESCE(p_request_id, ''));
  v_handoff_token text := BTRIM(COALESCE(p_handoff_token, ''));
  v_request record;
  v_active_lease concurrency_leases%ROWTYPE;
  v_request_row_count bigint := 0;
  v_active_lease_row_count bigint := 0;
  v_transitioned boolean := FALSE;
  v_heartbeat_deadline_ms bigint := NULL;
BEGIN
  IF v_request_id = '' OR v_handoff_token = '' OR COALESCE(p_start_timeout_ms, 0) <= 0 THEN
    result := 'conflict';
    reason := 'handoff_token_mismatch';
    heartbeat_deadline_ms := NULL;
    RETURN NEXT;
    RETURN;
  END IF;

  PERFORM pg_advisory_xact_lock(3, hashtext(v_request_id));

  SELECT *
    INTO v_request
  FROM concurrency_requests
  WHERE request_id = v_request_id
  FOR UPDATE;

  GET DIAGNOSTICS v_request_row_count = ROW_COUNT;

  IF v_request_row_count = 0 THEN
    result := 'conflict';
    reason := 'handoff_token_mismatch';
    heartbeat_deadline_ms := NULL;
    RETURN NEXT;
    RETURN;
  END IF;

  CASE v_request.state
    WHEN 'released' THEN
      result := 'released';
      reason := COALESCE(v_request.terminal_reason, 'already_released');
      heartbeat_deadline_ms := NULL;
      RETURN NEXT;
      RETURN;
    WHEN 'cancelled' THEN
      result := 'cancelled';
      reason := 'request_cancelled';
      heartbeat_deadline_ms := NULL;
      RETURN NEXT;
      RETURN;
    WHEN 'expired' THEN
      result := 'expired';
      reason := COALESCE(v_request.terminal_reason, 'hard_expired');
      heartbeat_deadline_ms := NULL;
      RETURN NEXT;
      RETURN;
    WHEN 'active' THEN
      NULL;
    ELSE
      result := 'conflict';
      reason := 'handoff_token_mismatch';
      heartbeat_deadline_ms := NULL;
      RETURN NEXT;
      RETURN;
  END CASE;

  SELECT *
    INTO v_active_lease
  FROM concurrency_leases
  WHERE request_id = v_request_id
    AND state = 'active'
  FOR UPDATE;

  GET DIAGNOSTICS v_active_lease_row_count = ROW_COUNT;

  IF v_request.hard_expire_at_ms <= v_now_ms
    OR COALESCE(v_request.lease_expires_at_ms, 0) <= v_now_ms
    OR (v_active_lease_row_count > 0 AND (v_active_lease.hard_expire_at_ms <= v_now_ms OR v_active_lease.expires_at_ms <= v_now_ms)) THEN
    v_transitioned := cq_apply_request_terminal_transition(v_request_id, 'expired', 'hard_expired', v_now_ms);

    result := 'expired';
    reason := CASE
      WHEN v_transitioned THEN 'hard_expired'
      ELSE COALESCE(v_request.terminal_reason, 'hard_expired')
    END;
    heartbeat_deadline_ms := NULL;
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.handoff_state = 'pending'
    AND v_request.handoff_deadline_ms IS NOT NULL
    AND v_request.handoff_deadline_ms <= v_now_ms THEN
    v_transitioned := cq_apply_request_terminal_transition(
      v_request_id,
      'released',
      'claim_handoff_timeout',
      v_now_ms,
      'compensated',
      'compensated'
    );

    result := 'released';
    reason := CASE
      WHEN v_transitioned THEN 'claim_handoff_timeout'
      ELSE COALESCE(v_request.terminal_reason, 'claim_handoff_timeout')
    END;
    heartbeat_deadline_ms := NULL;
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.handoff_token IS NOT DISTINCT FROM v_handoff_token
    AND v_request.handoff_state = 'pending' THEN
    IF v_request.hard_expire_at_ms <= v_commit_now_ms
      OR COALESCE(v_request.lease_expires_at_ms, 0) <= v_commit_now_ms
      OR (v_active_lease_row_count > 0 AND (v_active_lease.hard_expire_at_ms <= v_commit_now_ms OR v_active_lease.expires_at_ms <= v_commit_now_ms)) THEN
      v_transitioned := cq_apply_request_terminal_transition(v_request_id, 'expired', 'hard_expired', v_commit_now_ms);

      result := 'expired';
      reason := CASE
        WHEN v_transitioned THEN 'hard_expired'
        ELSE COALESCE(v_request.terminal_reason, 'hard_expired')
      END;
      heartbeat_deadline_ms := NULL;
      RETURN NEXT;
      RETURN;
    END IF;

    v_heartbeat_deadline_ms := LEAST(v_commit_now_ms + p_start_timeout_ms, v_request.hard_expire_at_ms);

    UPDATE concurrency_requests
    SET handoff_state = 'acknowledged',
        handoff_acked_at_ms = v_commit_now_ms,
        heartbeat_state = 'none',
        heartbeat_deadline_ms = LEAST(v_commit_now_ms + p_start_timeout_ms, v_request.hard_expire_at_ms),
        heartbeat_grace_until_ms = NULL,
        heartbeat_terminal_reason = NULL,
        updated_at_ms = v_commit_now_ms
    WHERE request_id = v_request_id;

    result := 'acknowledged';
    reason := NULL;
    heartbeat_deadline_ms := v_heartbeat_deadline_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.handoff_token IS NOT DISTINCT FROM v_handoff_token
    AND v_request.handoff_state = 'acknowledged' THEN
    result := 'acknowledged';
    reason := NULL;
    heartbeat_deadline_ms := COALESCE(v_request.heartbeat_deadline_ms, LEAST(v_commit_now_ms + p_start_timeout_ms, v_request.hard_expire_at_ms));
    RETURN NEXT;
    RETURN;
  END IF;

  result := 'conflict';
  reason := 'handoff_token_mismatch';
  heartbeat_deadline_ms := NULL;
  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cq_heartbeat_open(
  p_request_id text,
  p_lease_id uuid,
  p_lease_token text,
  p_hard_expire_at_ms bigint,
  p_now_ms bigint,
  p_heartbeat_timeout_ms bigint,
  p_ack_timeout_ms bigint,
  p_heartbeat_interval_ms bigint,
  p_reconnect_grace_ms bigint,
  p_start_timeout_ms bigint
)
RETURNS TABLE(result text, reason text, generation bigint, deadline_ms bigint, ack_timeout_ms bigint, heartbeat_interval_ms bigint, heartbeat_timeout_ms bigint, reconnect_grace_ms bigint, start_timeout_ms bigint, hard_expire_at_ms bigint) AS $$
DECLARE
  v_now_ms bigint := COALESCE(p_now_ms, (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint);
  v_request_id text := BTRIM(COALESCE(p_request_id, ''));
  v_lease_token text := BTRIM(COALESCE(p_lease_token, ''));
  v_request concurrency_requests%ROWTYPE;
  v_active_lease concurrency_leases%ROWTYPE;
  v_request_row_count bigint := 0;
  v_active_lease_row_count bigint := 0;
  v_transitioned boolean := FALSE;
  v_generation bigint := 0;
  v_deadline_ms bigint := NULL;
BEGIN
  IF v_request_id = ''
    OR p_lease_id IS NULL
    OR v_lease_token = ''
    OR COALESCE(p_hard_expire_at_ms, 0) <= 0
    OR COALESCE(p_heartbeat_timeout_ms, 0) <= 0
    OR COALESCE(p_ack_timeout_ms, 0) <= 0
    OR COALESCE(p_heartbeat_interval_ms, 0) <= 0
    OR COALESCE(p_reconnect_grace_ms, 0) <= 0
    OR COALESCE(p_start_timeout_ms, 0) <= 0 THEN
    result := 'conflict';
    reason := 'invalid_request';
    RETURN NEXT;
    RETURN;
  END IF;

  PERFORM pg_advisory_xact_lock(3, hashtext(v_request_id));

  SELECT *
    INTO v_request
  FROM concurrency_requests
  WHERE request_id = v_request_id
  FOR UPDATE;

  GET DIAGNOSTICS v_request_row_count = ROW_COUNT;

  IF v_request_row_count = 0 THEN
    result := 'conflict';
    reason := 'request_not_found';
    RETURN NEXT;
    RETURN;
  END IF;

  CASE v_request.state
    WHEN 'released' THEN
      result := 'terminal';
      reason := cq_heartbeat_released_replay_reason(v_request.terminal_reason);
      RETURN NEXT;
      RETURN;
    WHEN 'cancelled' THEN
      result := 'terminal';
      reason := 'request_cancelled';
      RETURN NEXT;
      RETURN;
    WHEN 'expired' THEN
      result := 'terminal';
      reason := COALESCE(v_request.terminal_reason, 'hard_expired');
      RETURN NEXT;
      RETURN;
    WHEN 'active' THEN
      NULL;
    ELSE
      result := 'conflict';
      reason := 'invalid_request_state';
      RETURN NEXT;
      RETURN;
  END CASE;

  SELECT *
    INTO v_active_lease
  FROM concurrency_leases
  WHERE request_id = v_request_id
    AND state = 'active'
  FOR UPDATE;

  GET DIAGNOSTICS v_active_lease_row_count = ROW_COUNT;

  IF v_active_lease_row_count = 0
    OR v_request.lease_id IS DISTINCT FROM p_lease_id
    OR v_active_lease.lease_id IS DISTINCT FROM p_lease_id THEN
    result := 'conflict';
    reason := 'lease_mismatch';
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.lease_token IS DISTINCT FROM v_lease_token
    OR v_active_lease.lease_token IS DISTINCT FROM v_lease_token THEN
    result := 'conflict';
    reason := 'lease_token_mismatch';
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.hard_expire_at_ms IS DISTINCT FROM p_hard_expire_at_ms
    OR v_active_lease.hard_expire_at_ms IS DISTINCT FROM p_hard_expire_at_ms THEN
    result := 'conflict';
    reason := 'hard_expire_at_mismatch';
    hard_expire_at_ms := v_request.hard_expire_at_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.hard_expire_at_ms <= v_now_ms
    OR COALESCE(v_request.lease_expires_at_ms, 0) <= v_now_ms
    OR v_active_lease.hard_expire_at_ms <= v_now_ms
    OR v_active_lease.expires_at_ms <= v_now_ms THEN
    v_transitioned := cq_apply_request_terminal_transition(v_request_id, 'expired', 'hard_expired', v_now_ms);
    result := CASE WHEN v_transitioned THEN 'expired' ELSE 'terminal' END;
    reason := CASE
      WHEN v_transitioned THEN 'hard_expired'
      ELSE COALESCE(v_request.terminal_reason, 'hard_expired')
    END;
    hard_expire_at_ms := v_request.hard_expire_at_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.handoff_state IS DISTINCT FROM 'acknowledged' THEN
    result := 'conflict';
    reason := 'handoff_not_acknowledged';
    hard_expire_at_ms := v_request.hard_expire_at_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  IF COALESCE(v_request.heartbeat_state, 'none') = 'none'
    AND v_request.heartbeat_deadline_ms IS NOT NULL
    AND v_request.heartbeat_deadline_ms <= v_now_ms THEN
    v_transitioned := cq_apply_request_terminal_transition(v_request_id, 'released', 'heartbeat_start_timeout', v_now_ms);
    result := CASE WHEN v_transitioned THEN 'released' ELSE 'terminal' END;
    reason := CASE
      WHEN v_transitioned THEN 'heartbeat_start_timeout'
      ELSE cq_heartbeat_released_replay_reason(v_request.terminal_reason, 'heartbeat_start_timeout')
    END;
    hard_expire_at_ms := v_request.hard_expire_at_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  IF COALESCE(v_request.heartbeat_state, 'none') IN ('connected', 'grace')
    AND v_request.heartbeat_deadline_ms IS NOT NULL
    AND v_request.heartbeat_deadline_ms <= v_now_ms THEN
    v_transitioned := cq_apply_request_terminal_transition(v_request_id, 'released', 'heartbeat_timeout', v_now_ms);
    result := CASE WHEN v_transitioned THEN 'released' ELSE 'terminal' END;
    reason := CASE
      WHEN v_transitioned THEN 'heartbeat_timeout'
      ELSE cq_heartbeat_released_replay_reason(v_request.terminal_reason, 'heartbeat_timeout')
    END;
    hard_expire_at_ms := v_request.hard_expire_at_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  CASE COALESCE(v_request.heartbeat_state, 'none')
    WHEN 'none' THEN
      NULL;
    WHEN 'connected' THEN
      NULL;
    WHEN 'grace' THEN
      IF v_request.heartbeat_grace_until_ms IS NULL OR v_request.heartbeat_grace_until_ms < v_now_ms THEN
        result := 'conflict';
        reason := 'grace_expired';
        hard_expire_at_ms := v_request.hard_expire_at_ms;
        RETURN NEXT;
        RETURN;
      END IF;
    ELSE
      result := 'conflict';
      reason := 'invalid_heartbeat_state';
      hard_expire_at_ms := v_request.hard_expire_at_ms;
      RETURN NEXT;
      RETURN;
  END CASE;

  v_generation := COALESCE(v_request.heartbeat_generation, 0) + 1;
  v_deadline_ms := LEAST(v_now_ms + p_heartbeat_timeout_ms, v_request.hard_expire_at_ms);

  UPDATE concurrency_requests
  SET heartbeat_state = 'connected',
      heartbeat_generation = v_generation,
      heartbeat_connected_at_ms = v_now_ms,
      heartbeat_last_at_ms = v_now_ms,
      heartbeat_deadline_ms = v_deadline_ms,
      heartbeat_grace_until_ms = NULL,
      heartbeat_disconnected_at_ms = NULL,
      heartbeat_terminal_reason = NULL,
      updated_at_ms = v_now_ms
  WHERE request_id = v_request_id;

  result := 'accepted';
  reason := NULL;
  generation := v_generation;
  deadline_ms := v_deadline_ms;
  ack_timeout_ms := p_ack_timeout_ms;
  heartbeat_interval_ms := p_heartbeat_interval_ms;
  heartbeat_timeout_ms := p_heartbeat_timeout_ms;
  reconnect_grace_ms := p_reconnect_grace_ms;
  start_timeout_ms := p_start_timeout_ms;
  hard_expire_at_ms := v_request.hard_expire_at_ms;
  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cq_heartbeat_refresh(
  p_request_id text,
  p_lease_id uuid,
  p_lease_token text,
  p_generation bigint,
  p_now_ms bigint,
  p_heartbeat_timeout_ms bigint
)
RETURNS TABLE(result text, reason text, generation bigint, deadline_ms bigint, ack_timeout_ms bigint, heartbeat_interval_ms bigint, heartbeat_timeout_ms bigint, reconnect_grace_ms bigint, start_timeout_ms bigint, hard_expire_at_ms bigint) AS $$
DECLARE
  v_now_ms bigint := COALESCE(p_now_ms, (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint);
  v_request_id text := BTRIM(COALESCE(p_request_id, ''));
  v_lease_token text := BTRIM(COALESCE(p_lease_token, ''));
  v_request concurrency_requests%ROWTYPE;
  v_active_lease concurrency_leases%ROWTYPE;
  v_request_row_count bigint := 0;
  v_active_lease_row_count bigint := 0;
  v_transitioned boolean := FALSE;
  v_deadline_ms bigint := NULL;
BEGIN
  IF v_request_id = ''
    OR p_lease_id IS NULL
    OR v_lease_token = ''
    OR COALESCE(p_generation, 0) <= 0
    OR COALESCE(p_heartbeat_timeout_ms, 0) <= 0 THEN
    result := 'conflict';
    reason := 'invalid_request';
    RETURN NEXT;
    RETURN;
  END IF;

  PERFORM pg_advisory_xact_lock(3, hashtext(v_request_id));

  SELECT *
    INTO v_request
  FROM concurrency_requests
  WHERE request_id = v_request_id
  FOR UPDATE;

  GET DIAGNOSTICS v_request_row_count = ROW_COUNT;

  IF v_request_row_count = 0 THEN
    result := 'conflict';
    reason := 'request_not_found';
    RETURN NEXT;
    RETURN;
  END IF;

  CASE v_request.state
    WHEN 'released' THEN
      result := 'terminal';
      reason := cq_heartbeat_released_replay_reason(v_request.terminal_reason);
      RETURN NEXT;
      RETURN;
    WHEN 'cancelled' THEN
      result := 'terminal';
      reason := 'request_cancelled';
      RETURN NEXT;
      RETURN;
    WHEN 'expired' THEN
      result := 'terminal';
      reason := COALESCE(v_request.terminal_reason, 'hard_expired');
      RETURN NEXT;
      RETURN;
    WHEN 'active' THEN
      NULL;
    ELSE
      result := 'conflict';
      reason := 'invalid_request_state';
      RETURN NEXT;
      RETURN;
  END CASE;

  SELECT *
    INTO v_active_lease
  FROM concurrency_leases
  WHERE request_id = v_request_id
    AND state = 'active'
  FOR UPDATE;

  GET DIAGNOSTICS v_active_lease_row_count = ROW_COUNT;

  IF v_active_lease_row_count = 0
    OR v_request.lease_id IS DISTINCT FROM p_lease_id
    OR v_active_lease.lease_id IS DISTINCT FROM p_lease_id THEN
    result := 'conflict';
    reason := 'lease_mismatch';
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.lease_token IS DISTINCT FROM v_lease_token
    OR v_active_lease.lease_token IS DISTINCT FROM v_lease_token THEN
    result := 'conflict';
    reason := 'lease_token_mismatch';
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.hard_expire_at_ms <= v_now_ms
    OR COALESCE(v_request.lease_expires_at_ms, 0) <= v_now_ms
    OR v_active_lease.hard_expire_at_ms <= v_now_ms
    OR v_active_lease.expires_at_ms <= v_now_ms THEN
    v_transitioned := cq_apply_request_terminal_transition(v_request_id, 'expired', 'hard_expired', v_now_ms);
    result := CASE WHEN v_transitioned THEN 'expired' ELSE 'terminal' END;
    reason := CASE
      WHEN v_transitioned THEN 'hard_expired'
      ELSE COALESCE(v_request.terminal_reason, 'hard_expired')
    END;
    hard_expire_at_ms := v_request.hard_expire_at_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.handoff_state IS DISTINCT FROM 'acknowledged' THEN
    result := 'conflict';
    reason := 'handoff_not_acknowledged';
    hard_expire_at_ms := v_request.hard_expire_at_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  IF COALESCE(v_request.heartbeat_state, 'none') IN ('connected', 'grace')
    AND v_request.heartbeat_deadline_ms IS NOT NULL
    AND v_request.heartbeat_deadline_ms <= v_now_ms THEN
    v_transitioned := cq_apply_request_terminal_transition(v_request_id, 'released', 'heartbeat_timeout', v_now_ms);
    result := CASE WHEN v_transitioned THEN 'released' ELSE 'terminal' END;
    reason := CASE
      WHEN v_transitioned THEN 'heartbeat_timeout'
      ELSE cq_heartbeat_released_replay_reason(v_request.terminal_reason, 'heartbeat_timeout')
    END;
    hard_expire_at_ms := v_request.hard_expire_at_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  IF COALESCE(v_request.heartbeat_state, 'none') IS DISTINCT FROM 'connected' THEN
    result := 'conflict';
    reason := 'invalid_heartbeat_state';
    hard_expire_at_ms := v_request.hard_expire_at_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.heartbeat_generation IS DISTINCT FROM p_generation THEN
    result := 'noop';
    reason := 'stale_generation';
    generation := v_request.heartbeat_generation;
    deadline_ms := v_request.heartbeat_deadline_ms;
    hard_expire_at_ms := v_request.hard_expire_at_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  v_deadline_ms := LEAST(v_now_ms + p_heartbeat_timeout_ms, v_request.hard_expire_at_ms);

  UPDATE concurrency_requests
  SET heartbeat_state = 'connected',
      heartbeat_last_at_ms = v_now_ms,
      heartbeat_deadline_ms = v_deadline_ms,
      heartbeat_grace_until_ms = NULL,
      heartbeat_disconnected_at_ms = NULL,
      heartbeat_terminal_reason = NULL,
      updated_at_ms = v_now_ms
  WHERE request_id = v_request_id;

  result := 'accepted';
  reason := NULL;
  generation := v_request.heartbeat_generation;
  deadline_ms := v_deadline_ms;
  heartbeat_timeout_ms := p_heartbeat_timeout_ms;
  hard_expire_at_ms := v_request.hard_expire_at_ms;
  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cq_heartbeat_disconnect(
  p_request_id text,
  p_lease_id uuid,
  p_lease_token text,
  p_generation bigint,
  p_now_ms bigint,
  p_reconnect_grace_ms bigint
)
RETURNS TABLE(result text, reason text, generation bigint, deadline_ms bigint, ack_timeout_ms bigint, heartbeat_interval_ms bigint, heartbeat_timeout_ms bigint, reconnect_grace_ms bigint, start_timeout_ms bigint, hard_expire_at_ms bigint) AS $$
DECLARE
  v_now_ms bigint := COALESCE(p_now_ms, (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint);
  v_request_id text := BTRIM(COALESCE(p_request_id, ''));
  v_lease_token text := BTRIM(COALESCE(p_lease_token, ''));
  v_request concurrency_requests%ROWTYPE;
  v_active_lease concurrency_leases%ROWTYPE;
  v_request_row_count bigint := 0;
  v_active_lease_row_count bigint := 0;
  v_transitioned boolean := FALSE;
  v_grace_until_ms bigint := NULL;
  v_deadline_ms bigint := NULL;
BEGIN
  IF v_request_id = ''
    OR p_lease_id IS NULL
    OR v_lease_token = ''
    OR COALESCE(p_generation, 0) <= 0
    OR COALESCE(p_reconnect_grace_ms, 0) <= 0 THEN
    result := 'conflict';
    reason := 'invalid_request';
    RETURN NEXT;
    RETURN;
  END IF;

  PERFORM pg_advisory_xact_lock(3, hashtext(v_request_id));

  SELECT *
    INTO v_request
  FROM concurrency_requests
  WHERE request_id = v_request_id
  FOR UPDATE;

  GET DIAGNOSTICS v_request_row_count = ROW_COUNT;

  IF v_request_row_count = 0 THEN
    result := 'conflict';
    reason := 'request_not_found';
    RETURN NEXT;
    RETURN;
  END IF;

  CASE v_request.state
    WHEN 'released' THEN
      result := 'terminal';
      reason := cq_heartbeat_released_replay_reason(v_request.terminal_reason);
      RETURN NEXT;
      RETURN;
    WHEN 'cancelled' THEN
      result := 'terminal';
      reason := 'request_cancelled';
      RETURN NEXT;
      RETURN;
    WHEN 'expired' THEN
      result := 'terminal';
      reason := COALESCE(v_request.terminal_reason, 'hard_expired');
      RETURN NEXT;
      RETURN;
    WHEN 'active' THEN
      NULL;
    ELSE
      result := 'conflict';
      reason := 'invalid_request_state';
      RETURN NEXT;
      RETURN;
  END CASE;

  SELECT *
    INTO v_active_lease
  FROM concurrency_leases
  WHERE request_id = v_request_id
    AND state = 'active'
  FOR UPDATE;

  GET DIAGNOSTICS v_active_lease_row_count = ROW_COUNT;

  IF v_active_lease_row_count = 0
    OR v_request.lease_id IS DISTINCT FROM p_lease_id
    OR v_active_lease.lease_id IS DISTINCT FROM p_lease_id THEN
    result := 'conflict';
    reason := 'lease_mismatch';
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.lease_token IS DISTINCT FROM v_lease_token
    OR v_active_lease.lease_token IS DISTINCT FROM v_lease_token THEN
    result := 'conflict';
    reason := 'lease_token_mismatch';
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.hard_expire_at_ms <= v_now_ms
    OR COALESCE(v_request.lease_expires_at_ms, 0) <= v_now_ms
    OR v_active_lease.hard_expire_at_ms <= v_now_ms
    OR v_active_lease.expires_at_ms <= v_now_ms THEN
    v_transitioned := cq_apply_request_terminal_transition(v_request_id, 'expired', 'hard_expired', v_now_ms);
    result := CASE WHEN v_transitioned THEN 'expired' ELSE 'terminal' END;
    reason := CASE
      WHEN v_transitioned THEN 'hard_expired'
      ELSE COALESCE(v_request.terminal_reason, 'hard_expired')
    END;
    hard_expire_at_ms := v_request.hard_expire_at_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.handoff_state IS DISTINCT FROM 'acknowledged' THEN
    result := 'conflict';
    reason := 'handoff_not_acknowledged';
    hard_expire_at_ms := v_request.hard_expire_at_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  IF COALESCE(v_request.heartbeat_state, 'none') IN ('connected', 'grace')
    AND v_request.heartbeat_deadline_ms IS NOT NULL
    AND v_request.heartbeat_deadline_ms <= v_now_ms THEN
    v_transitioned := cq_apply_request_terminal_transition(v_request_id, 'released', 'heartbeat_timeout', v_now_ms);
    result := CASE WHEN v_transitioned THEN 'released' ELSE 'terminal' END;
    reason := CASE
      WHEN v_transitioned THEN 'heartbeat_timeout'
      ELSE cq_heartbeat_released_replay_reason(v_request.terminal_reason, 'heartbeat_timeout')
    END;
    hard_expire_at_ms := v_request.hard_expire_at_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  IF COALESCE(v_request.heartbeat_state, 'none') IS DISTINCT FROM 'connected' THEN
    result := 'conflict';
    reason := 'invalid_heartbeat_state';
    hard_expire_at_ms := v_request.hard_expire_at_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.heartbeat_generation IS DISTINCT FROM p_generation THEN
    result := 'noop';
    reason := 'stale_generation';
    generation := v_request.heartbeat_generation;
    deadline_ms := v_request.heartbeat_deadline_ms;
    hard_expire_at_ms := v_request.hard_expire_at_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  v_grace_until_ms := LEAST(v_now_ms + p_reconnect_grace_ms, v_request.hard_expire_at_ms);
  v_deadline_ms := LEAST(COALESCE(v_request.heartbeat_deadline_ms, v_grace_until_ms), v_grace_until_ms, v_request.hard_expire_at_ms);

  UPDATE concurrency_requests
  SET heartbeat_state = 'grace',
      heartbeat_deadline_ms = v_deadline_ms,
      heartbeat_grace_until_ms = v_grace_until_ms,
      heartbeat_disconnected_at_ms = v_now_ms,
      heartbeat_terminal_reason = NULL,
      updated_at_ms = v_now_ms
  WHERE request_id = v_request_id;

  result := 'accepted';
  reason := NULL;
  generation := v_request.heartbeat_generation;
  deadline_ms := v_deadline_ms;
  reconnect_grace_ms := p_reconnect_grace_ms;
  hard_expire_at_ms := v_request.hard_expire_at_ms;
  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cq_expire_heartbeat_if_due(
  p_request_id text,
  p_now_ms bigint
)
RETURNS TABLE(result text, reason text, generation bigint, deadline_ms bigint, ack_timeout_ms bigint, heartbeat_interval_ms bigint, heartbeat_timeout_ms bigint, reconnect_grace_ms bigint, start_timeout_ms bigint, hard_expire_at_ms bigint) AS $$
DECLARE
  v_now_ms bigint := COALESCE(p_now_ms, (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint);
  v_request concurrency_requests%ROWTYPE;
  v_active_lease concurrency_leases%ROWTYPE;
  v_request_row_count bigint := 0;
  v_active_lease_row_count bigint := 0;
  v_transitioned boolean := FALSE;
BEGIN
  PERFORM pg_advisory_xact_lock(3, hashtext(BTRIM(COALESCE(p_request_id, ''))));

  SELECT *
    INTO v_request
  FROM concurrency_requests
  WHERE request_id = p_request_id
  FOR UPDATE;

  GET DIAGNOSTICS v_request_row_count = ROW_COUNT;

  IF v_request_row_count = 0 THEN
    result := 'conflict';
    reason := 'request_not_found';
    RETURN NEXT;
    RETURN;
  END IF;

  CASE v_request.state
    WHEN 'released' THEN
      result := 'terminal';
      reason := cq_heartbeat_released_replay_reason(v_request.terminal_reason);
      RETURN NEXT;
      RETURN;
    WHEN 'cancelled' THEN
      result := 'terminal';
      reason := 'request_cancelled';
      RETURN NEXT;
      RETURN;
    WHEN 'expired' THEN
      result := 'terminal';
      reason := COALESCE(v_request.terminal_reason, 'hard_expired');
      RETURN NEXT;
      RETURN;
    WHEN 'active' THEN
      NULL;
    ELSE
      result := 'conflict';
      reason := 'invalid_request_state';
      RETURN NEXT;
      RETURN;
  END CASE;

  SELECT *
    INTO v_active_lease
  FROM concurrency_leases
  WHERE request_id = p_request_id
    AND state = 'active'
  FOR UPDATE;

  GET DIAGNOSTICS v_active_lease_row_count = ROW_COUNT;

  IF v_request.hard_expire_at_ms <= v_now_ms
    OR COALESCE(v_request.lease_expires_at_ms, 0) <= v_now_ms
    OR (v_active_lease_row_count > 0 AND (v_active_lease.hard_expire_at_ms <= v_now_ms OR v_active_lease.expires_at_ms <= v_now_ms)) THEN
    v_transitioned := cq_apply_request_terminal_transition(p_request_id, 'expired', 'hard_expired', v_now_ms);
    result := CASE WHEN v_transitioned THEN 'expired' ELSE 'terminal' END;
    reason := CASE
      WHEN v_transitioned THEN 'hard_expired'
      ELSE COALESCE(v_request.terminal_reason, 'hard_expired')
    END;
    generation := v_request.heartbeat_generation;
    hard_expire_at_ms := v_request.hard_expire_at_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.heartbeat_deadline_ms IS NULL OR v_request.heartbeat_deadline_ms > v_now_ms THEN
    result := 'noop';
    reason := 'not_due';
    generation := v_request.heartbeat_generation;
    deadline_ms := v_request.heartbeat_deadline_ms;
    hard_expire_at_ms := v_request.hard_expire_at_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_request.handoff_state = 'acknowledged' AND COALESCE(v_request.heartbeat_state, 'none') = 'none' THEN
    v_transitioned := cq_apply_request_terminal_transition(p_request_id, 'released', 'heartbeat_start_timeout', v_now_ms);
    result := CASE WHEN v_transitioned THEN 'released' ELSE 'terminal' END;
    reason := CASE
      WHEN v_transitioned THEN 'heartbeat_start_timeout'
      ELSE cq_heartbeat_released_replay_reason(v_request.terminal_reason, 'heartbeat_start_timeout')
    END;
    generation := v_request.heartbeat_generation;
    hard_expire_at_ms := v_request.hard_expire_at_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  IF COALESCE(v_request.heartbeat_state, 'none') IN ('connected', 'grace') THEN
    v_transitioned := cq_apply_request_terminal_transition(p_request_id, 'released', 'heartbeat_timeout', v_now_ms);
    result := CASE WHEN v_transitioned THEN 'released' ELSE 'terminal' END;
    reason := CASE
      WHEN v_transitioned THEN 'heartbeat_timeout'
      ELSE cq_heartbeat_released_replay_reason(v_request.terminal_reason, 'heartbeat_timeout')
    END;
    generation := v_request.heartbeat_generation;
    hard_expire_at_ms := v_request.hard_expire_at_ms;
    RETURN NEXT;
    RETURN;
  END IF;

  result := 'conflict';
  reason := 'invalid_heartbeat_state';
  generation := v_request.heartbeat_generation;
  deadline_ms := v_request.heartbeat_deadline_ms;
  hard_expire_at_ms := v_request.hard_expire_at_ms;
  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cq_continue_wait_probe(
  p_hostname_hash text,
  p_hostname text,
  p_site_bucket text,
  p_ip_bucket text,
  p_request_id text,
  p_hard_expire_at_ms bigint,
  p_now_ms bigint,
  p_wait_token text
)
RETURNS TABLE(result text, lease_id uuid, lease_token text, expires_at_ms bigint, wait_token text, scope text, reason text, retry_after integer, claim_token text) AS $$
DECLARE
  v_now_ms bigint := COALESCE(p_now_ms, (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint);
  v_site_bucket text := COALESCE(NULLIF(BTRIM(COALESCE(p_site_bucket, '')), ''), 'unknown');
  v_ip_bucket text := COALESCE(NULLIF(BTRIM(COALESCE(p_ip_bucket, '')), ''), 'unknown');
  v_hostname_hash text := BTRIM(COALESCE(p_hostname_hash, ''));
  v_request_id text := BTRIM(COALESCE(p_request_id, ''));
  v_authoritative_request_id text := NULL;
  v_wait_token_input text := NULLIF(BTRIM(COALESCE(p_wait_token, '')), '');
  v_request record;
  v_request_row_count bigint := 0;
  v_active_lease concurrency_leases%ROWTYPE;
  v_active_lease_row_count bigint := 0;
  v_transitioned boolean := FALSE;
BEGIN
  IF v_wait_token_input IS NULL THEN
    RAISE EXCEPTION 'cq_acquire stale wait token';
  END IF;

  SELECT concurrency_requests.request_id
    INTO v_authoritative_request_id
  FROM concurrency_requests
  WHERE concurrency_requests.wait_token = v_wait_token_input;

  GET DIAGNOSTICS v_request_row_count = ROW_COUNT;

  IF v_request_row_count = 0 THEN
    RAISE EXCEPTION 'cq_acquire stale wait token';
  END IF;

  PERFORM pg_advisory_xact_lock(3, hashtext(v_authoritative_request_id));

  SELECT *
    INTO v_request
  FROM concurrency_requests
  WHERE request_id = v_authoritative_request_id
  FOR UPDATE;

  GET DIAGNOSTICS v_request_row_count = ROW_COUNT;

  IF v_request_row_count = 0 OR v_request.wait_token IS DISTINCT FROM v_wait_token_input THEN
    RAISE EXCEPTION 'cq_acquire stale wait token';
  END IF;

  IF v_request.request_id IS DISTINCT FROM v_request_id
    OR v_request.hostname_hash IS DISTINCT FROM v_hostname_hash
    OR v_request.site_bucket IS DISTINCT FROM v_site_bucket
    OR v_request.ip_bucket IS DISTINCT FROM v_ip_bucket
    OR v_request.hard_expire_at_ms IS DISTINCT FROM p_hard_expire_at_ms THEN
    RAISE EXCEPTION 'cq_acquire request_id tuple mismatch';
  END IF;

  CASE v_request.state
    WHEN 'active' THEN
      SELECT *
        INTO v_active_lease
      FROM concurrency_leases
      WHERE request_id = v_request_id
        AND state = 'active'
      FOR UPDATE;

      GET DIAGNOSTICS v_active_lease_row_count = ROW_COUNT;

      IF v_request.hard_expire_at_ms <= v_now_ms
        OR COALESCE(v_request.lease_expires_at_ms, 0) <= v_now_ms
        OR (v_active_lease_row_count > 0 AND (v_active_lease.hard_expire_at_ms <= v_now_ms OR v_active_lease.expires_at_ms <= v_now_ms)) THEN
        v_transitioned := cq_apply_request_terminal_transition(v_request_id, 'expired', 'hard_expired', v_now_ms);

        result := 'expired';
        lease_id := NULL;
        lease_token := NULL;
        expires_at_ms := NULL;
        wait_token := NULL;
        scope := NULL;
        reason := CASE
          WHEN v_transitioned THEN 'hard_expired'
          ELSE COALESCE(v_request.terminal_reason, 'hard_expired')
        END;
        retry_after := NULL;
        claim_token := NULL;
        RETURN NEXT;
        RETURN;
      END IF;

      result := 'conflict';
      lease_id := NULL;
      lease_token := NULL;
      expires_at_ms := NULL;
      wait_token := NULL;
      scope := NULL;
      reason := CASE WHEN v_request.claim_state = 'claimed' THEN 'grant_already_claimed' ELSE 'grant_unclaimed' END;
      retry_after := NULL;
      claim_token := NULL;
      RETURN NEXT;
      RETURN;
    WHEN 'released' THEN
      result := 'released';
      lease_id := NULL;
      lease_token := NULL;
      expires_at_ms := NULL;
      wait_token := NULL;
      scope := NULL;
      reason := COALESCE(v_request.terminal_reason, 'already_released');
      retry_after := NULL;
      claim_token := NULL;
      RETURN NEXT;
      RETURN;
    WHEN 'cancelled' THEN
      result := 'cancelled';
      lease_id := NULL;
      lease_token := NULL;
      expires_at_ms := NULL;
      wait_token := NULL;
      scope := NULL;
      reason := 'request_cancelled';
      retry_after := NULL;
      claim_token := NULL;
      RETURN NEXT;
      RETURN;
    WHEN 'expired' THEN
      result := 'expired';
      lease_id := NULL;
      lease_token := NULL;
      expires_at_ms := NULL;
      wait_token := NULL;
      scope := NULL;
      reason := COALESCE(v_request.terminal_reason, 'hard_expired');
      retry_after := NULL;
      claim_token := NULL;
      RETURN NEXT;
      RETURN;
    WHEN 'waiting' THEN
      IF v_request.hard_expire_at_ms <= v_now_ms THEN
        UPDATE concurrency_requests
        SET state = 'expired',
            terminal_reason = 'hard_expired',
            updated_at_ms = v_now_ms
        WHERE request_id = v_request_id;

        result := 'expired';
        lease_id := NULL;
        lease_token := NULL;
        expires_at_ms := NULL;
        wait_token := NULL;
        scope := NULL;
        reason := 'hard_expired';
        retry_after := NULL;
        claim_token := NULL;
        RETURN NEXT;
        RETURN;
      END IF;

      IF COALESCE(v_request.waiter_lease_until_ms, 0) <= v_now_ms THEN
        UPDATE concurrency_requests
        SET state = 'expired',
            terminal_reason = 'waiter_detached_timeout',
            updated_at_ms = v_now_ms
        WHERE request_id = v_request_id;

        result := 'expired';
        lease_id := NULL;
        lease_token := NULL;
        expires_at_ms := NULL;
        wait_token := NULL;
        scope := NULL;
        reason := 'waiter_detached_timeout';
        retry_after := NULL;
        claim_token := NULL;
        RETURN NEXT;
        RETURN;
      END IF;

      result := 'wait';
      lease_id := NULL;
      lease_token := NULL;
      expires_at_ms := NULL;
      wait_token := v_request.wait_token;
      scope := 'host';
      reason := NULL;
      retry_after := 1;
      claim_token := NULL;
      RETURN NEXT;
      RETURN;
  END CASE;
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
  p_open_cap_seconds INT,
  p_close_threshold_percent INT,
  p_half_open_success_threshold INT,
  p_half_open_close_mode TEXT,
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
  v_throttle_code INTEGER := NULL;
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
      FROM func_authorize_breaker_attempt(
        p_hostname_hash,
        p_hostname,
        v_now,
        p_open_cap_seconds,
        p_close_threshold_percent,
        p_half_open_success_threshold,
        p_half_open_close_mode,
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
