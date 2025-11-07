-- ========================================
-- Infrastructure Tables For simple-alist-cf-proxy
-- ========================================
-- Default table names align with environment variable defaults:
--   DOWNLOAD_CACHE_TABLE           (env: DOWNLOAD_CACHE_TABLE)
--   THROTTLE_PROTECTION            (env: THROTTLE_PROTECTION_TABLE)
--   DOWNLOAD_IP_RATELIMIT_TABLE    (env: DOWNLOAD_IP_RATELIMIT_TABLE)
--   SESSION_MAPPING_TABLE          (env: SESSION_TABLE_NAME)
--
-- If you override the environment variables, adjust the CREATE TABLE
-- statements accordingly before applying this script.


-- ========================================
-- Session Mapping Table Schema
-- ========================================
-- Purpose: Stores landing-worker issued session tickets
-- Compatible with: PostgreSQL

CREATE TABLE IF NOT EXISTS "SESSION_MAPPING_TABLE" (
  "SESSION_TICKET" TEXT PRIMARY KEY,
  "FILE_PATH" TEXT NOT NULL,
  "FILE_PATH_HASH" TEXT NOT NULL,
  "IP_SUBNET" TEXT NOT NULL,
  "WORKER_ADDRESS" TEXT NOT NULL,
  "EXPIRE_AT" BIGINT NOT NULL,
  "CREATED_AT" BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_session_expire
  ON "SESSION_MAPPING_TABLE"("EXPIRE_AT");
CREATE INDEX IF NOT EXISTS idx_session_file_path_hash
  ON "SESSION_MAPPING_TABLE"("FILE_PATH_HASH");


-- ========================================
-- Stored Procedure: Insert Session Mapping
-- ========================================
CREATE OR REPLACE FUNCTION session_insert(
  p_session_ticket TEXT,
  p_file_path TEXT,
  p_file_path_hash TEXT,
  p_ip_subnet TEXT,
  p_worker_address TEXT,
  p_expire_at BIGINT,
  p_created_at BIGINT,
  p_table_name TEXT DEFAULT 'SESSION_MAPPING_TABLE'
)
RETURNS JSON AS $$
DECLARE
  v_sql TEXT;
BEGIN
  v_sql := format(
    'INSERT INTO %1$I ("SESSION_TICKET", "FILE_PATH", "FILE_PATH_HASH", "IP_SUBNET", "WORKER_ADDRESS", "EXPIRE_AT", "CREATED_AT")
     VALUES ($1, $2, $3, $4, $5, $6, $7)',
    p_table_name
  );

  EXECUTE v_sql USING p_session_ticket, p_file_path, p_file_path_hash, p_ip_subnet, p_worker_address, p_expire_at, p_created_at;

  RETURN json_build_object('success', true);
EXCEPTION
  WHEN others THEN
    RETURN json_build_object('success', false, 'error', SQLERRM);
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- Stored Procedure: Retrieve Session Mapping (Compatibility)
-- ========================================
CREATE OR REPLACE FUNCTION session_get(
  p_session_ticket TEXT,
  p_table_name TEXT DEFAULT 'SESSION_MAPPING_TABLE'
)
RETURNS JSON AS $$
DECLARE
  v_sql TEXT;
  v_record RECORD;
BEGIN
  v_sql := format(
    'SELECT "SESSION_TICKET", "FILE_PATH", "FILE_PATH_HASH", "IP_SUBNET", "WORKER_ADDRESS", "EXPIRE_AT", "CREATED_AT"
     FROM %1$I WHERE "SESSION_TICKET" = $1 LIMIT 1',
    p_table_name
  );

  EXECUTE v_sql INTO v_record USING p_session_ticket;

  IF v_record IS NULL OR v_record."SESSION_TICKET" IS NULL THEN
    RETURN json_build_object('found', false);
  END IF;

  RETURN json_build_object(
    'found', true,
    'file_path', v_record."FILE_PATH",
    'file_path_hash', v_record."FILE_PATH_HASH",
    'ip_subnet', v_record."IP_SUBNET",
    'worker_address', v_record."WORKER_ADDRESS",
    'expire_at', v_record."EXPIRE_AT"
  );
EXCEPTION
  WHEN others THEN
    RETURN json_build_object('found', false, 'error', SQLERRM);
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- Stored Procedure: Cleanup Expired Sessions
-- ========================================
CREATE OR REPLACE FUNCTION session_cleanup_expired(
  p_table_name TEXT DEFAULT 'SESSION_MAPPING_TABLE',
  p_now BIGINT DEFAULT NULL
)
RETURNS JSON AS $$
DECLARE
  v_now BIGINT;
  v_sql TEXT;
  v_deleted BIGINT := 0;
BEGIN
  v_now := COALESCE(p_now, EXTRACT(EPOCH FROM NOW())::BIGINT);

  v_sql := format('DELETE FROM %1$I WHERE "EXPIRE_AT" < $1', p_table_name);
  EXECUTE v_sql USING v_now;
  GET DIAGNOSTICS v_deleted = ROW_COUNT;

  RETURN json_build_object('deleted', v_deleted);
EXCEPTION
  WHEN others THEN
    RETURN json_build_object('deleted', 0, 'error', SQLERRM);
END;
$$ LANGUAGE plpgsql;


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
  "LAST_ERROR_CODE" INTEGER
);

CREATE INDEX IF NOT EXISTS idx_throttle_timestamp
  ON "THROTTLE_PROTECTION"("ERROR_TIMESTAMP");


-- ========================================
-- PostgreSQL Stored Procedure: Atomic UPSERT (Throttle)
-- ========================================
CREATE OR REPLACE FUNCTION download_upsert_throttle_protection(
  p_hostname_hash TEXT,
  p_hostname TEXT,
  p_error_timestamp INTEGER,
  p_is_protected INTEGER,
  p_last_error_code INTEGER,
  p_table_name TEXT DEFAULT 'THROTTLE_PROTECTION'
)
RETURNS TABLE(
  "HOSTNAME_HASH" TEXT,
  "HOSTNAME" TEXT,
  "ERROR_TIMESTAMP" INTEGER,
  "IS_PROTECTED" INTEGER,
  "LAST_ERROR_CODE" INTEGER
) AS $$
DECLARE
  sql TEXT;
BEGIN
  sql := format(
    'INSERT INTO %1$I ("HOSTNAME_HASH", "HOSTNAME", "ERROR_TIMESTAMP", "IS_PROTECTED", "LAST_ERROR_CODE")
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT ("HOSTNAME_HASH") DO UPDATE SET
       "HOSTNAME" = EXCLUDED."HOSTNAME",
       "ERROR_TIMESTAMP" = EXCLUDED."ERROR_TIMESTAMP",
       "IS_PROTECTED" = EXCLUDED."IS_PROTECTED",
       "LAST_ERROR_CODE" = EXCLUDED."LAST_ERROR_CODE"
     RETURNING "HOSTNAME_HASH", "HOSTNAME", "ERROR_TIMESTAMP", "IS_PROTECTED", "LAST_ERROR_CODE"',
    p_table_name
  );

  RETURN QUERY EXECUTE sql USING p_hostname_hash, p_hostname, p_error_timestamp, p_is_protected, p_last_error_code;
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
         WHEN $3 - %1$I."LAST_WINDOW_TIME" >= $4 THEN 1
         WHEN %1$I."BLOCK_UNTIL" IS NOT NULL AND %1$I."BLOCK_UNTIL" <= $3 THEN 1
         WHEN %1$I."ACCESS_COUNT" >= $5 THEN %1$I."ACCESS_COUNT"
         ELSE %1$I."ACCESS_COUNT" + 1
       END,
       "LAST_WINDOW_TIME" = CASE
         WHEN $3 - %1$I."LAST_WINDOW_TIME" >= $4 THEN $3
         WHEN %1$I."BLOCK_UNTIL" IS NOT NULL AND %1$I."BLOCK_UNTIL" <= $3 THEN $3
         ELSE %1$I."LAST_WINDOW_TIME"
       END,
       "BLOCK_UNTIL" = CASE
         WHEN $3 - %1$I."LAST_WINDOW_TIME" >= $4 THEN NULL
         WHEN %1$I."BLOCK_UNTIL" IS NOT NULL AND %1$I."BLOCK_UNTIL" <= $3 THEN NULL
         WHEN %1$I."BLOCK_UNTIL" IS NOT NULL AND %1$I."BLOCK_UNTIL" > $3 THEN %1$I."BLOCK_UNTIL"
         WHEN (%1$I."BLOCK_UNTIL" IS NULL OR %1$I."BLOCK_UNTIL" <= $3) AND %1$I."ACCESS_COUNT" >= $5 AND $6 > 0 THEN $3 + $6
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
-- Unified Check Function (Session + Rate Limit + Cache + Throttle)
-- ========================================
-- Purpose: Combines Session validation + Rate Limit + Cache + Throttle checks in a single database round-trip

CREATE OR REPLACE FUNCTION download_unified_check(
  -- Cache parameters
  p_path_hash TEXT,
  p_cache_ttl INTEGER,
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

  -- Session parameters
  p_session_ticket TEXT DEFAULT NULL,
  p_session_table_name TEXT DEFAULT 'SESSION_MAPPING_TABLE',
  p_now BIGINT DEFAULT NULL,

  -- Last active parameters
  p_idle_timeout INTEGER DEFAULT 0,
  p_last_active_table_name TEXT DEFAULT 'DOWNLOAD_LAST_ACTIVE_TABLE'
)
RETURNS TABLE(
  -- Session result
  session_found BOOLEAN,
  session_file_path TEXT,
  session_ip_subnet TEXT,
  session_worker_address TEXT,
  session_expire_at BIGINT,
  session_error TEXT,

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
  v_session_record RECORD;
  v_cache_record RECORD;
  v_rate_record RECORD;
  v_throttle_record RECORD;
  v_cache_hostname_hash TEXT;
  v_active_record RECORD;

  v_session_found BOOLEAN := NULL;
  v_session_file_path TEXT := NULL;
  v_session_file_path_hash TEXT := NULL;
  v_session_ip_subnet TEXT := NULL;
  v_session_worker_address TEXT := NULL;
  v_session_expire_at BIGINT := NULL;
  v_session_error TEXT := NULL;

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

  -- Step 1: Session validation (if ticket provided)
  IF p_session_ticket IS NOT NULL AND btrim(p_session_ticket) <> '' THEN
    EXECUTE format('SELECT * FROM %1$I WHERE "SESSION_TICKET" = $1 LIMIT 1', p_session_table_name)
      INTO v_session_record
      USING p_session_ticket;

    IF v_session_record IS NULL OR v_session_record."SESSION_TICKET" IS NULL THEN
      RETURN QUERY SELECT
        FALSE,
        NULL::TEXT,
        NULL::TEXT,
        NULL::TEXT,
        NULL::BIGINT,
        'session not found'::TEXT,
        NULL::TEXT,
        NULL::INTEGER,
        NULL::TEXT,
        NULL::INTEGER,
        NULL::INTEGER,
        NULL::INTEGER,
        NULL::BOOLEAN,
        NULL::INTEGER,
        NULL::INTEGER,
        NULL::INTEGER,
        NULL::INTEGER,
        NULL::INTEGER;
      RETURN;
    END IF;

    IF v_session_record."EXPIRE_AT" IS NOT NULL AND v_session_record."EXPIRE_AT" < v_now THEN
      RETURN QUERY SELECT
        FALSE,
        NULL::TEXT,
        NULL::TEXT,
        NULL::TEXT,
        v_session_record."EXPIRE_AT",
        'session expired'::TEXT,
        NULL::TEXT,
        NULL::INTEGER,
        NULL::TEXT,
        NULL::INTEGER,
        NULL::INTEGER,
        NULL::INTEGER,
        NULL::BOOLEAN,
        NULL::INTEGER,
        NULL::INTEGER,
        NULL::INTEGER,
        NULL::INTEGER,
        NULL::INTEGER;
      RETURN;
    END IF;

    v_session_found := TRUE;
    v_session_file_path := v_session_record."FILE_PATH";
    v_session_file_path_hash := v_session_record."FILE_PATH_HASH";
    v_session_ip_subnet := v_session_record."IP_SUBNET";
    v_session_worker_address := v_session_record."WORKER_ADDRESS";
    v_session_expire_at := v_session_record."EXPIRE_AT";
  END IF;

  -- Step 2: Cache lookup
  -- IMPORTANT: If session mode, use pre-stored FILE_PATH_HASH (not the /session/* URL path)
  IF v_session_found IS TRUE AND v_session_file_path_hash IS NOT NULL THEN
    -- Session mode: use FILE_PATH_HASH from session table
    v_actual_path_hash := v_session_file_path_hash;
  ELSE
    -- Non-session mode: use provided path_hash
    v_actual_path_hash := p_path_hash;
  END IF;

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

  -- Step 3: Rate limit upsert
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

  -- Step 4: Throttle lookup (only when cache provided hostname)
  IF v_cache_hostname_hash IS NOT NULL THEN
    EXECUTE format('SELECT "IS_PROTECTED", "ERROR_TIMESTAMP", "LAST_ERROR_CODE" FROM %1$I WHERE "HOSTNAME_HASH" = $1', p_throttle_table_name)
      INTO v_throttle_record
      USING v_cache_hostname_hash;

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

  -- Step 5: Last active lookup
  -- IMPORTANT: Use v_actual_path_hash (respects session mode path hash)
  EXECUTE format('SELECT "LAST_ACCESS_TIME", "TOTAL_ACCESS_COUNT" FROM %1$I WHERE "IP_HASH" = $1 AND "PATH_HASH" = $2 LIMIT 1', p_last_active_table_name)
    INTO v_active_record
    USING p_ip_hash, v_actual_path_hash;

  IF v_active_record."LAST_ACCESS_TIME" IS NOT NULL THEN
    v_active_last_access_time := v_active_record."LAST_ACCESS_TIME";
    v_active_total_access_count := v_active_record."TOTAL_ACCESS_COUNT";
  END IF;

  RETURN QUERY SELECT
    v_session_found,
    v_session_file_path,
    v_session_ip_subnet,
    v_session_worker_address,
    v_session_expire_at,
    v_session_error,
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
