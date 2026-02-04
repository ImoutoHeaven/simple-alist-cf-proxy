-- ========================================
-- Infrastructure Tables For simple-alist-cf-proxy
-- ========================================
-- Default table names align with environment variable defaults:
--   DOWNLOAD_CACHE_TABLE           (env: DOWNLOAD_CACHE_TABLE)
--   THROTTLE_PROTECTION            (env: THROTTLE_PROTECTION_TABLE)
--   DOWNLOAD_IP_RATELIMIT_TABLE    (env: DOWNLOAD_IP_RATELIMIT_TABLE)
--
-- If you override the environment variables, adjust the CREATE TABLE
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
  "ERROR_TIMESTAMP" INTEGER,
  "IS_PROTECTED" INTEGER,
  "LAST_ERROR_CODE" INTEGER,
  "OBS_WINDOW_START" INTEGER,
  "OBS_ERROR_COUNT" INTEGER NOT NULL DEFAULT 0,
  "OBS_SUCCESS_COUNT" INTEGER NOT NULL DEFAULT 0,
  "CONSECUTIVE_ERROR_COUNT" INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_throttle_timestamp
  ON "THROTTLE_PROTECTION"("ERROR_TIMESTAMP");


-- ========================================
-- PostgreSQL Stored Procedure: Atomic UPSERT (Throttle)
-- ========================================
CREATE OR REPLACE FUNCTION download_upsert_throttle_protection(
  p_hostname_hash TEXT,
  p_hostname TEXT,
  p_now INTEGER,
  p_is_error BOOLEAN,
  p_status_code INTEGER,
  p_throttle_time_window INTEGER,
  p_observe_window_seconds INTEGER,
  p_error_ratio_percent INTEGER,
  p_consecutive_threshold INTEGER,
  p_min_sample_count INTEGER,
  p_fast_error_ratio_percent INTEGER DEFAULT NULL,
  p_fast_min_sample_count INTEGER DEFAULT NULL,
  p_table_name TEXT DEFAULT 'THROTTLE_PROTECTION'
)
RETURNS TABLE(
  "HOSTNAME_HASH" TEXT,
  "HOSTNAME" TEXT,
  "ERROR_TIMESTAMP" INTEGER,
  "IS_PROTECTED" INTEGER,
  "LAST_ERROR_CODE" INTEGER,
  "OBS_WINDOW_START" INTEGER,
  "OBS_ERROR_COUNT" INTEGER,
  "OBS_SUCCESS_COUNT" INTEGER,
  "CONSECUTIVE_ERROR_COUNT" INTEGER
) AS $$
DECLARE
  sql TEXT;
  v_now INTEGER := COALESCE(p_now, EXTRACT(EPOCH FROM NOW())::INTEGER);
  v_table_name TEXT := COALESCE(NULLIF(p_table_name, ''), 'THROTTLE_PROTECTION');
  v_observe_window_seconds INTEGER := GREATEST(1, COALESCE(p_observe_window_seconds, 60));
  v_error_ratio_percent INTEGER := GREATEST(0, COALESCE(p_error_ratio_percent, 0));
  v_consecutive_threshold INTEGER := GREATEST(1, COALESCE(p_consecutive_threshold, 1));
  v_min_sample_count INTEGER := GREATEST(1, COALESCE(p_min_sample_count, 1));
  v_throttle_time_window INTEGER := GREATEST(1, COALESCE(p_throttle_time_window, 1));
  v_fast_error_ratio_percent INTEGER := NULL;
  v_fast_min_sample_count INTEGER := NULL;

  v_hostname TEXT := p_hostname;
  v_error_timestamp INTEGER := NULL;
  v_is_protected INTEGER := 0;
  v_last_error_code INTEGER := NULL;
  v_obs_window_start INTEGER := NULL;
  v_obs_error_count INTEGER := 0;
  v_obs_success_count INTEGER := 0;
  v_consecutive_error_count INTEGER := 0;
  v_ratio_trigger BOOLEAN := FALSE;
  v_fast_ratio_trigger BOOLEAN := FALSE;
  v_consecutive_trigger BOOLEAN := FALSE;
  v_should_protect BOOLEAN := FALSE;
  v_locked BOOLEAN := FALSE;
  v_locked_row_count INTEGER := 0;
  v_total INTEGER := 0;
BEGIN
  v_fast_error_ratio_percent := GREATEST(
    v_error_ratio_percent,
    COALESCE(p_fast_error_ratio_percent, v_error_ratio_percent)
  );
  v_fast_min_sample_count := COALESCE(p_fast_min_sample_count, 4);
  IF v_fast_min_sample_count <= 0 THEN
    v_fast_min_sample_count := 0;
  ELSE
    v_fast_min_sample_count := LEAST(
      v_min_sample_count,
      GREATEST(1, v_fast_min_sample_count)
    );
  END IF;

  -- Lock row to avoid lost updates under concurrency
  WHILE NOT v_locked LOOP
    v_hostname := NULL;
    v_error_timestamp := NULL;
    v_is_protected := NULL;
    v_last_error_code := NULL;
    v_obs_window_start := NULL;
    v_obs_error_count := 0;
    v_obs_success_count := 0;
    v_consecutive_error_count := 0;

    EXECUTE format(
      'SELECT "HOSTNAME", "ERROR_TIMESTAMP", "IS_PROTECTED", "LAST_ERROR_CODE", "OBS_WINDOW_START", "OBS_ERROR_COUNT", "OBS_SUCCESS_COUNT", "CONSECUTIVE_ERROR_COUNT"
         FROM %1$I WHERE "HOSTNAME_HASH" = $1
         FOR UPDATE',
      v_table_name
    )
    INTO v_hostname, v_error_timestamp, v_is_protected, v_last_error_code, v_obs_window_start, v_obs_error_count, v_obs_success_count, v_consecutive_error_count
    USING p_hostname_hash;

    GET DIAGNOSTICS v_locked_row_count = ROW_COUNT;
    v_locked := v_locked_row_count > 0;

    IF NOT v_locked THEN
      EXECUTE format(
        'INSERT INTO %1$I ("HOSTNAME_HASH", "HOSTNAME", "ERROR_TIMESTAMP", "IS_PROTECTED", "LAST_ERROR_CODE", "OBS_WINDOW_START", "OBS_ERROR_COUNT", "OBS_SUCCESS_COUNT", "CONSECUTIVE_ERROR_COUNT")
         VALUES ($1, $2, NULL, 0, NULL, $3, 0, 0, 0)
         ON CONFLICT ("HOSTNAME_HASH") DO NOTHING',
        v_table_name
      ) USING p_hostname_hash, COALESCE(p_hostname, p_hostname_hash), v_now;
    END IF;
  END LOOP;

  v_hostname := COALESCE(p_hostname, v_hostname);
  v_is_protected := COALESCE(v_is_protected, 0);
  v_obs_error_count := COALESCE(v_obs_error_count, 0);
  v_obs_success_count := COALESCE(v_obs_success_count, 0);
  v_consecutive_error_count := COALESCE(v_consecutive_error_count, 0);

  IF v_obs_window_start IS NULL OR (v_now - v_obs_window_start) >= v_observe_window_seconds THEN
    v_obs_window_start := v_now;
    v_obs_error_count := 0;
    v_obs_success_count := 0;
    -- 连续错误计数在观察窗口重置时不会被重置，只有成功事件清零
  END IF;

  IF COALESCE(p_is_error, FALSE) THEN
    v_obs_error_count := v_obs_error_count + 1;
    v_consecutive_error_count := v_consecutive_error_count + 1;
  ELSE
    v_obs_success_count := v_obs_success_count + 1;
    v_consecutive_error_count := 0;
  END IF;

  IF COALESCE(p_is_error, FALSE) THEN
    v_total := v_obs_error_count + v_obs_success_count;

    IF v_total >= v_min_sample_count THEN
      v_ratio_trigger := (v_obs_error_count * 100) >= (v_error_ratio_percent * v_total);
    END IF;

    IF v_fast_min_sample_count > 0 AND v_total >= v_fast_min_sample_count THEN
      v_fast_ratio_trigger := (v_obs_error_count * 100) >= (v_fast_error_ratio_percent * v_total);
    END IF;

    v_consecutive_trigger := v_consecutive_error_count >= v_consecutive_threshold;
    v_should_protect := v_ratio_trigger OR v_fast_ratio_trigger OR v_consecutive_trigger;
  END IF;

  IF v_should_protect THEN
    v_is_protected := 1;
    v_error_timestamp := v_now;
    v_last_error_code := p_status_code;
  ELSE
    IF v_is_protected = 1 AND v_error_timestamp IS NOT NULL THEN
      IF (v_now - v_error_timestamp) >= v_throttle_time_window THEN
        v_is_protected := 0;
        v_error_timestamp := NULL;
        v_last_error_code := NULL;
        v_obs_window_start := v_now;
        v_obs_error_count := 0;
        v_obs_success_count := 0;
      ELSE
        v_is_protected := 1;
      END IF;
    ELSE
      v_is_protected := 0;
      IF COALESCE(p_is_error, FALSE) THEN
        v_last_error_code := p_status_code;
      END IF;
    END IF;
  END IF;

  sql := format(
    'UPDATE %1$I SET
       "HOSTNAME" = $2,
       "ERROR_TIMESTAMP" = $3,
       "IS_PROTECTED" = $4,
       "LAST_ERROR_CODE" = $5,
       "OBS_WINDOW_START" = $6,
       "OBS_ERROR_COUNT" = $7,
       "OBS_SUCCESS_COUNT" = $8,
       "CONSECUTIVE_ERROR_COUNT" = $9
     WHERE "HOSTNAME_HASH" = $1
     RETURNING "HOSTNAME_HASH", "HOSTNAME", "ERROR_TIMESTAMP", "IS_PROTECTED", "LAST_ERROR_CODE", "OBS_WINDOW_START", "OBS_ERROR_COUNT", "OBS_SUCCESS_COUNT", "CONSECUTIVE_ERROR_COUNT"',
    v_table_name
  );

  RETURN QUERY EXECUTE sql USING
    p_hostname_hash,
    v_hostname,
    v_error_timestamp,
    v_is_protected,
    v_last_error_code,
    v_obs_window_start,
    v_obs_error_count,
    v_obs_success_count,
    v_consecutive_error_count;
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- Optional: Throttle Cleanup Function (PostgreSQL)
-- ========================================
-- BREAKING CHANGE: IS_PROTECTED semantics changed
--   1 = protected (error detected)
--   0 = normal operation (initialized or recovered)
--   NULL = record does not exist (query result only)
-- Cleanup: Delete records with IS_PROTECTED = 0 and expired ERROR_TIMESTAMP
CREATE OR REPLACE FUNCTION download_cleanup_throttle_protection(
  p_ttl_seconds INTEGER,
  p_table_name TEXT DEFAULT 'THROTTLE_PROTECTION'
)
RETURNS INTEGER AS $$
DECLARE
  deleted_count INTEGER;
  sql TEXT;
BEGIN
  sql := format(
    'DELETE FROM %1$I
     WHERE "IS_PROTECTED" = 0
       AND ("ERROR_TIMESTAMP" IS NULL OR EXTRACT(EPOCH FROM NOW())::INTEGER - "ERROR_TIMESTAMP" > $1)',
    p_table_name
  );

  EXECUTE sql USING p_ttl_seconds;
  GET DIAGNOSTICS deleted_count = ROW_COUNT;

  RETURN deleted_count;
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
  p_throttle_time_window INTEGER,
  p_throttle_table_name TEXT,
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
  throttle_is_protected INTEGER,
  throttle_error_timestamp INTEGER,
  throttle_error_code INTEGER,

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
  v_throttle_is_protected INTEGER := NULL;
  v_throttle_error_timestamp INTEGER := NULL;
  v_throttle_error_code INTEGER := NULL;

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
    EXECUTE format('SELECT "IS_PROTECTED", "ERROR_TIMESTAMP", "LAST_ERROR_CODE" FROM %1$I WHERE "HOSTNAME_HASH" = $1', p_throttle_table_name)
      INTO v_throttle_record
      USING v_throttle_hostname_hash;

    IF v_throttle_record."IS_PROTECTED" IS NOT NULL THEN
      v_throttle_record_exists := TRUE;
      v_throttle_is_protected := v_throttle_record."IS_PROTECTED";
      v_throttle_error_timestamp := v_throttle_record."ERROR_TIMESTAMP";
      v_throttle_error_code := v_throttle_record."LAST_ERROR_CODE";
    ELSE
      v_throttle_record_exists := FALSE;
      v_throttle_is_protected := NULL;
      v_throttle_error_timestamp := NULL;
      v_throttle_error_code := NULL;
    END IF;
  ELSE
    v_throttle_record_exists := FALSE;
    v_throttle_is_protected := NULL;
    v_throttle_error_timestamp := NULL;
    v_throttle_error_code := NULL;
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
    v_throttle_is_protected,
    v_throttle_error_timestamp,
    v_throttle_error_code,
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
     AND v_current_ip_slots > 0
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
     AND v_current_ip_slots > 0
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
CREATE OR REPLACE FUNCTION fq_check_throttle(
  p_hostname_hash TEXT,
  p_hostname TEXT,
  p_throttle_time_window INTEGER DEFAULT 60
)
RETURNS TABLE(
  is_protected BOOLEAN,
  error_code INTEGER,
  retry_after INTEGER
) AS $$
DECLARE
  v_window INTEGER := GREATEST(1, COALESCE(p_throttle_time_window, 60));
  v_now INTEGER := EXTRACT(EPOCH FROM NOW())::INTEGER;
  v_error_timestamp INTEGER;
  v_error_code INTEGER;
  v_is_protected INTEGER;
  v_retry_after INTEGER := NULL;
  v_rows INTEGER := 0;
BEGIN
  IF p_hostname_hash IS NULL OR p_hostname_hash = '' THEN
    RETURN QUERY SELECT FALSE, NULL::INTEGER, NULL::INTEGER;
    RETURN;
  END IF;

  IF p_hostname IS NULL OR p_hostname = '' THEN
    p_hostname := p_hostname_hash;
  END IF;

  BEGIN
    INSERT INTO "THROTTLE_PROTECTION" ("HOSTNAME_HASH", "HOSTNAME", "IS_PROTECTED", "LAST_ERROR_CODE")
    VALUES (p_hostname_hash, p_hostname, 0, NULL)
    ON CONFLICT ("HOSTNAME_HASH") DO NOTHING;
  EXCEPTION
    WHEN others THEN
      NULL;
  END;

  SELECT "IS_PROTECTED", "LAST_ERROR_CODE", "ERROR_TIMESTAMP"
  INTO v_is_protected, v_error_code, v_error_timestamp
  FROM "THROTTLE_PROTECTION"
  WHERE "HOSTNAME_HASH" = p_hostname_hash;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  IF v_rows = 0 THEN
    RETURN QUERY SELECT FALSE, NULL::INTEGER, NULL::INTEGER;
    RETURN;
  END IF;

  IF COALESCE(v_is_protected, 0) = 1 AND v_error_timestamp IS NOT NULL THEN
    IF (v_now - v_error_timestamp) < v_window THEN
      v_retry_after := GREATEST(v_window - (v_now - v_error_timestamp), 0);
      RETURN QUERY SELECT TRUE, v_error_code, v_retry_after;
      RETURN;
    END IF;
  END IF;

  RETURN QUERY SELECT FALSE, NULL::INTEGER, NULL::INTEGER;
END;
$$ LANGUAGE plpgsql;

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
  p_cooldown_seconds INT DEFAULT 0,
  p_throttle_time_window INT DEFAULT 60
)
RETURNS TABLE(
  status TEXT,
  slot_token TEXT,
  throttle_code INT,
  throttle_retry_after INT
) AS $$
DECLARE
  v_hostname TEXT;
  v_site_bucket TEXT;
  v_ip_bucket TEXT;
  v_throttled BOOLEAN := FALSE;
  v_throttle_code INTEGER := NULL;
  v_throttle_retry_after INTEGER := NULL;
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
    SELECT t.is_protected, t.error_code, t.retry_after
    INTO v_throttled, v_throttle_code, v_throttle_retry_after
    FROM fq_check_throttle(p_hostname_hash, v_hostname, p_throttle_time_window) AS t;
  END IF;

  IF v_hostname IS NULL THEN
    FOR v_idx IN 1..v_site_len LOOP
      status := 'WAIT';
      slot_token := NULL::TEXT;
      throttle_code := NULL::INTEGER;
      throttle_retry_after := NULL::INTEGER;
      RETURN NEXT;
    END LOOP;
    RETURN;
  END IF;

  IF v_throttled THEN
    FOR v_idx IN 1..v_site_len LOOP
      status := 'THROTTLED';
      slot_token := NULL::TEXT;
      throttle_code := v_throttle_code;
      throttle_retry_after := v_throttle_retry_after;
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
      throttle_retry_after := v_throttle_retry_after;
      RETURN NEXT;
      CONTINUE;
    END IF;

    IF v_host_slot_id = 0 THEN
      status := 'IP_TOO_MANY';
      slot_token := NULL::TEXT;
      throttle_code := v_throttle_code;
      throttle_retry_after := v_throttle_retry_after;
      RETURN NEXT;
      CONTINUE;
    ELSIF v_host_slot_id < 0 THEN
      status := 'QUEUE_FULL';
      slot_token := NULL::TEXT;
      throttle_code := v_throttle_code;
      throttle_retry_after := v_throttle_retry_after;
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
      throttle_retry_after := v_throttle_retry_after;
      RETURN NEXT;
      CONTINUE;
    END IF;

    IF v_site_slot_id = 0 THEN
      PERFORM func_release_host_slot(v_host_slot_id, FALSE);
      status := 'IP_TOO_MANY';
      slot_token := NULL::TEXT;
      throttle_code := v_throttle_code;
      throttle_retry_after := v_throttle_retry_after;
      RETURN NEXT;
      CONTINUE;
    ELSIF v_site_slot_id < 0 THEN
      PERFORM func_release_host_slot(v_host_slot_id, FALSE);
      status := 'QUEUE_FULL';
      slot_token := NULL::TEXT;
      throttle_code := v_throttle_code;
      throttle_retry_after := v_throttle_retry_after;
      RETURN NEXT;
      CONTINUE;
    END IF;

    status := 'ACQUIRED';
    slot_token := encode(convert_to(jsonb_build_object('host', v_host_slot_id, 'site', v_site_slot_id)::text, 'UTF8'), 'base64');
    throttle_code := v_throttle_code;
    throttle_retry_after := v_throttle_retry_after;
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
