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
-- Bandwidth Quota Tables Schema
-- ========================================
-- Purpose: Track byte consumption per IP range and per IP range + filepath
-- Compatible with: SQLite (D1), PostgreSQL

CREATE TABLE IF NOT EXISTS "IPRANGE_BANDWIDTH_QUOTA_TABLE" (
  "IP_HASH" TEXT PRIMARY KEY,
  "IP_RANGE" TEXT NOT NULL,
  "BYTES_USED" INTEGER NOT NULL DEFAULT 0,
  "WINDOW_START" INTEGER NOT NULL,
  "BLOCK_UNTIL" INTEGER
);

CREATE INDEX IF NOT EXISTS idx_iprange_bandwidth_window
  ON "IPRANGE_BANDWIDTH_QUOTA_TABLE"("WINDOW_START");
CREATE INDEX IF NOT EXISTS idx_iprange_bandwidth_block
  ON "IPRANGE_BANDWIDTH_QUOTA_TABLE"("BLOCK_UNTIL")
  WHERE "BLOCK_UNTIL" IS NOT NULL;

CREATE TABLE IF NOT EXISTS "IPRANGE_FILEPATH_BANDWIDTH_QUOTA_TABLE" (
  "COMPOSITE_HASH" TEXT PRIMARY KEY,
  "IP_RANGE" TEXT NOT NULL,
  "FILEPATH" TEXT NOT NULL,
  "BYTES_USED" INTEGER NOT NULL DEFAULT 0,
  "WINDOW_START" INTEGER NOT NULL,
  "BLOCK_UNTIL" INTEGER
);

CREATE INDEX IF NOT EXISTS idx_filepath_bandwidth_window
  ON "IPRANGE_FILEPATH_BANDWIDTH_QUOTA_TABLE"("WINDOW_START");
CREATE INDEX IF NOT EXISTS idx_filepath_bandwidth_block
  ON "IPRANGE_FILEPATH_BANDWIDTH_QUOTA_TABLE"("BLOCK_UNTIL")
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
         WHEN %1$I."ACCESS_COUNT" >= $5 AND $6 > 0 THEN $3 + $6
         ELSE %1$I."BLOCK_UNTIL"
       END
     RETURNING "ACCESS_COUNT", "LAST_WINDOW_TIME", "BLOCK_UNTIL"',
    p_table_name
  );

  RETURN QUERY EXECUTE sql USING p_ip_hash, p_ip_range, p_now, p_window_seconds, p_limit, p_block_seconds;
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- PostgreSQL Stored Procedure: Atomic UPSERT (Bandwidth Quota - IP Range)
-- ========================================
CREATE OR REPLACE FUNCTION bandwidth_upsert_iprange_quota(
  p_ip_hash TEXT,
  p_ip_range TEXT,
  p_bytes_to_add INTEGER,
  p_now INTEGER,
  p_window_seconds INTEGER,
  p_quota_bytes INTEGER,
  p_block_seconds INTEGER,
  p_table_name TEXT DEFAULT 'IPRANGE_BANDWIDTH_QUOTA_TABLE'
)
RETURNS TABLE(
  "BYTES_USED" INTEGER,
  "WINDOW_START" INTEGER,
  "BLOCK_UNTIL" INTEGER
) AS $$
DECLARE
  sql TEXT;
BEGIN
  sql := format(
    'INSERT INTO %1$I ("IP_HASH", "IP_RANGE", "BYTES_USED", "WINDOW_START", "BLOCK_UNTIL")
     VALUES ($1, $2, $3, $4, NULL)
     ON CONFLICT ("IP_HASH") DO UPDATE SET
       "BYTES_USED" = CASE
         WHEN $4 - %1$I."WINDOW_START" >= $5 THEN $3
         WHEN %1$I."BLOCK_UNTIL" IS NOT NULL AND %1$I."BLOCK_UNTIL" <= $4 THEN $3
         WHEN %1$I."BYTES_USED" >= $6 THEN %1$I."BYTES_USED"
         ELSE %1$I."BYTES_USED" + $3
       END,
       "WINDOW_START" = CASE
         WHEN $4 - %1$I."WINDOW_START" >= $5 THEN $4
         WHEN %1$I."BLOCK_UNTIL" IS NOT NULL AND %1$I."BLOCK_UNTIL" <= $4 THEN $4
         ELSE %1$I."WINDOW_START"
       END,
       "BLOCK_UNTIL" = CASE
         WHEN $4 - %1$I."WINDOW_START" >= $5 THEN NULL
         WHEN %1$I."BLOCK_UNTIL" IS NOT NULL AND %1$I."BLOCK_UNTIL" <= $4 THEN NULL
         WHEN %1$I."BYTES_USED" + $3 > $6 AND $7 > 0 THEN $4 + $7
         ELSE %1$I."BLOCK_UNTIL"
       END
     RETURNING "BYTES_USED", "WINDOW_START", "BLOCK_UNTIL"',
    p_table_name
  );

  RETURN QUERY EXECUTE sql USING p_ip_hash, p_ip_range, p_bytes_to_add, p_now, p_window_seconds, p_quota_bytes, p_block_seconds;
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- PostgreSQL Stored Procedure: Atomic UPSERT (Bandwidth Quota - IP Range + Filepath)
-- ========================================
CREATE OR REPLACE FUNCTION bandwidth_upsert_filepath_quota(
  p_composite_hash TEXT,
  p_ip_range TEXT,
  p_filepath TEXT,
  p_bytes_to_add INTEGER,
  p_now INTEGER,
  p_window_seconds INTEGER,
  p_quota_bytes INTEGER,
  p_block_seconds INTEGER,
  p_table_name TEXT DEFAULT 'IPRANGE_FILEPATH_BANDWIDTH_QUOTA_TABLE'
)
RETURNS TABLE(
  "BYTES_USED" INTEGER,
  "WINDOW_START" INTEGER,
  "BLOCK_UNTIL" INTEGER
) AS $$
DECLARE
  sql TEXT;
BEGIN
  sql := format(
    'INSERT INTO %1$I ("COMPOSITE_HASH", "IP_RANGE", "FILEPATH", "BYTES_USED", "WINDOW_START", "BLOCK_UNTIL")
     VALUES ($1, $2, $3, $4, $5, NULL)
     ON CONFLICT ("COMPOSITE_HASH") DO UPDATE SET
       "BYTES_USED" = CASE
         WHEN $5 - %1$I."WINDOW_START" >= $6 THEN $4
         WHEN %1$I."BLOCK_UNTIL" IS NOT NULL AND %1$I."BLOCK_UNTIL" <= $5 THEN $4
         WHEN %1$I."BYTES_USED" >= $7 THEN %1$I."BYTES_USED"
         ELSE %1$I."BYTES_USED" + $4
       END,
       "WINDOW_START" = CASE
         WHEN $5 - %1$I."WINDOW_START" >= $6 THEN $5
         WHEN %1$I."BLOCK_UNTIL" IS NOT NULL AND %1$I."BLOCK_UNTIL" <= $5 THEN $5
         ELSE %1$I."WINDOW_START"
       END,
       "BLOCK_UNTIL" = CASE
         WHEN $5 - %1$I."WINDOW_START" >= $6 THEN NULL
         WHEN %1$I."BLOCK_UNTIL" IS NOT NULL AND %1$I."BLOCK_UNTIL" <= $5 THEN NULL
         WHEN %1$I."BYTES_USED" + $4 > $7 AND $8 > 0 THEN $5 + $8
         ELSE %1$I."BLOCK_UNTIL"
       END
     RETURNING "BYTES_USED", "WINDOW_START", "BLOCK_UNTIL"',
    p_table_name
  );

  RETURN QUERY EXECUTE sql USING p_composite_hash, p_ip_range, p_filepath, p_bytes_to_add, p_now, p_window_seconds, p_quota_bytes, p_block_seconds;
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- Optional: Bandwidth Quota Cleanup Function (PostgreSQL)
-- ========================================
CREATE OR REPLACE FUNCTION bandwidth_cleanup_quota(
  p_window_seconds INTEGER,
  p_table_name TEXT DEFAULT 'IPRANGE_BANDWIDTH_QUOTA_TABLE'
)
RETURNS INTEGER AS $$
DECLARE
  deleted_count INTEGER;
  sql TEXT;
  cutoff INTEGER;
  now_ts INTEGER;
BEGIN
  IF p_window_seconds IS NULL OR p_window_seconds <= 0 THEN
    RETURN 0;
  END IF;

  now_ts := EXTRACT(EPOCH FROM NOW())::INTEGER;
  cutoff := now_ts - (p_window_seconds * 2);

  sql := format(
    'DELETE FROM %1$I
     WHERE "WINDOW_START" < $1
       AND ("BLOCK_UNTIL" IS NULL OR "BLOCK_UNTIL" < $2)',
    p_table_name
  );

  EXECUTE sql USING cutoff, now_ts;
  GET DIAGNOSTICS deleted_count = ROW_COUNT;

  RETURN deleted_count;
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
-- Unified Check Function (RTT Optimization: 3→1)
-- ========================================
-- Purpose: Combines Rate Limit + Cache + Throttle checks in a single database round-trip
-- Used by: Worker unified check (src/unified-check.js)
--
-- Performance: Reduces 3 separate queries to 1 RTT
-- - Cache hit scenario: 3 RTT → 1 RTT (66% reduction)
-- - Cache miss scenario: 3 RTT → 2 RTT (33% reduction)

CREATE OR REPLACE FUNCTION download_unified_check(
  -- Cache params
  p_path_hash TEXT,
  p_cache_ttl INTEGER,
  p_cache_table_name TEXT,

  -- Rate limit params
  p_ip_hash TEXT,
  p_ip_range TEXT,
  p_now INTEGER,
  p_window_seconds INTEGER,
  p_limit INTEGER,
  p_block_seconds INTEGER,
  p_ratelimit_table_name TEXT,

  -- Throttle params
  p_throttle_time_window INTEGER,
  p_throttle_table_name TEXT,

  -- Bandwidth quota params
  p_bandwidth_iprange_quota INTEGER DEFAULT 0,
  p_bandwidth_filepath_quota INTEGER DEFAULT 0,
  p_bandwidth_window_total_seconds INTEGER DEFAULT 0,
  p_bandwidth_window_filepath_seconds INTEGER DEFAULT 0,
  p_bandwidth_block_seconds INTEGER DEFAULT 0,
  p_bandwidth_iprange_table TEXT DEFAULT 'IPRANGE_BANDWIDTH_QUOTA_TABLE',
  p_bandwidth_filepath_table TEXT DEFAULT 'IPRANGE_FILEPATH_BANDWIDTH_QUOTA_TABLE',
  p_bandwidth_composite_hash TEXT DEFAULT NULL
)
RETURNS TABLE(
  -- Cache result (nullable if not found or expired)
  cache_link_data TEXT,
  cache_timestamp INTEGER,
  cache_hostname_hash TEXT,

  -- Rate limit result (always returns)
  rate_access_count INTEGER,
  rate_last_window_time INTEGER,
  rate_block_until INTEGER,

  -- Throttle result (nullable if not found)
  throttle_record_exists BOOLEAN,
  throttle_is_protected INTEGER,
  throttle_error_timestamp INTEGER,
  throttle_error_code INTEGER,

  -- Bandwidth quota result
  bandwidth_iprange_allowed BOOLEAN,
  bandwidth_iprange_bytes_used INTEGER,
  bandwidth_iprange_block_until INTEGER,
  bandwidth_filepath_allowed BOOLEAN,
  bandwidth_filepath_bytes_used INTEGER,
  bandwidth_filepath_block_until INTEGER
) AS $$
DECLARE
  cache_sql TEXT;
  rate_result RECORD;
  throttle_sql TEXT;
  cache_rec RECORD;
  throttle_rec RECORD;
  cache_hostname_hash_var TEXT;
  iprange_quota_sql TEXT;
  filepath_quota_sql TEXT;
  iprange_quota_rec RECORD;
  filepath_quota_rec RECORD;
  bandwidth_window_total_seconds INTEGER;
  bandwidth_window_filepath_seconds INTEGER;
  composite_hash TEXT;
BEGIN
  -- Step 1: Check cache
  cache_sql := format(
    'SELECT "LINK_DATA", "TIMESTAMP", "HOSTNAME_HASH" FROM %1$I WHERE "PATH_HASH" = $1',
    p_cache_table_name
  );
  EXECUTE cache_sql INTO cache_rec USING p_path_hash;

  -- Check if cache exists and is not expired
  -- Note: EXECUTE INTO RECORD does not set FOUND correctly, check field instead
  IF cache_rec."TIMESTAMP" IS NOT NULL AND (p_now - cache_rec."TIMESTAMP") <= p_cache_ttl THEN
    cache_link_data := cache_rec."LINK_DATA";
    cache_timestamp := cache_rec."TIMESTAMP";
    cache_hostname_hash_var := cache_rec."HOSTNAME_HASH";
  ELSE
    cache_link_data := NULL;
    cache_timestamp := NULL;
    cache_hostname_hash_var := NULL;
  END IF;

  cache_hostname_hash := cache_hostname_hash_var;

  -- Step 2: Upsert rate limit (reuse existing function)
  SELECT * INTO rate_result FROM download_upsert_rate_limit(
    p_ip_hash,
    p_ip_range,
    p_now,
    p_window_seconds,
    p_limit,
    p_block_seconds,
    p_ratelimit_table_name
  );

  rate_access_count := rate_result."ACCESS_COUNT";
  rate_last_window_time := rate_result."LAST_WINDOW_TIME";
  rate_block_until := rate_result."BLOCK_UNTIL";

  -- Step 3: Check throttle (only if we have hostname_hash from cache)
  IF cache_hostname_hash_var IS NOT NULL THEN
    throttle_sql := format(
      'SELECT "IS_PROTECTED", "ERROR_TIMESTAMP", "LAST_ERROR_CODE"
       FROM %1$I WHERE "HOSTNAME_HASH" = $1',
      p_throttle_table_name
    );
    EXECUTE throttle_sql INTO throttle_rec USING cache_hostname_hash_var;

    -- Note: EXECUTE INTO RECORD does not set FOUND correctly, check field instead
    IF throttle_rec."IS_PROTECTED" IS NOT NULL THEN
      -- Record exists in database
      throttle_record_exists := TRUE;
      throttle_is_protected := throttle_rec."IS_PROTECTED";
      throttle_error_timestamp := throttle_rec."ERROR_TIMESTAMP";
      throttle_error_code := throttle_rec."LAST_ERROR_CODE";
    ELSE
      -- Record does not exist in database
      throttle_record_exists := FALSE;
      throttle_is_protected := NULL;
      throttle_error_timestamp := NULL;
      throttle_error_code := NULL;
    END IF;
  ELSE
    -- No cache or no hostname_hash - skip throttle check
    throttle_record_exists := FALSE;
    throttle_is_protected := NULL;
    throttle_error_timestamp := NULL;
    throttle_error_code := NULL;
  END IF;

  bandwidth_iprange_allowed := TRUE;
  bandwidth_iprange_bytes_used := 0;
  bandwidth_iprange_block_until := NULL;
  bandwidth_filepath_allowed := TRUE;
  bandwidth_filepath_bytes_used := 0;
  bandwidth_filepath_block_until := NULL;

  bandwidth_window_total_seconds := COALESCE(p_bandwidth_window_total_seconds, 0);
  bandwidth_window_filepath_seconds := COALESCE(p_bandwidth_window_filepath_seconds, 0);
  composite_hash := p_bandwidth_composite_hash;

  IF p_bandwidth_iprange_quota > 0 AND bandwidth_window_total_seconds > 0 THEN
    iprange_quota_sql := format(
      'SELECT "BYTES_USED", "WINDOW_START", "BLOCK_UNTIL"
       FROM %1$I WHERE "IP_HASH" = $1',
      p_bandwidth_iprange_table
    );
    EXECUTE iprange_quota_sql INTO iprange_quota_rec USING p_ip_hash;

    IF iprange_quota_rec."BYTES_USED" IS NOT NULL THEN
      bandwidth_iprange_bytes_used := iprange_quota_rec."BYTES_USED";

      IF iprange_quota_rec."BLOCK_UNTIL" IS NOT NULL AND iprange_quota_rec."BLOCK_UNTIL" > p_now THEN
        bandwidth_iprange_allowed := FALSE;
        bandwidth_iprange_block_until := iprange_quota_rec."BLOCK_UNTIL";
      ELSIF iprange_quota_rec."WINDOW_START" IS NOT NULL
        AND (p_now - iprange_quota_rec."WINDOW_START") < bandwidth_window_total_seconds
        AND iprange_quota_rec."BYTES_USED" >= p_bandwidth_iprange_quota THEN
        bandwidth_iprange_allowed := FALSE;
        bandwidth_iprange_block_until := iprange_quota_rec."WINDOW_START" + bandwidth_window_total_seconds;
      ELSE
        bandwidth_iprange_allowed := TRUE;
        bandwidth_iprange_block_until := NULL;
      END IF;
    ELSE
      bandwidth_iprange_allowed := TRUE;
      bandwidth_iprange_bytes_used := 0;
      bandwidth_iprange_block_until := NULL;
    END IF;
  ELSE
    bandwidth_iprange_allowed := TRUE;
    bandwidth_iprange_bytes_used := 0;
    bandwidth_iprange_block_until := NULL;
  END IF;

  IF p_bandwidth_filepath_quota > 0 AND bandwidth_window_filepath_seconds > 0 AND composite_hash IS NOT NULL THEN
    filepath_quota_sql := format(
      'SELECT "BYTES_USED", "WINDOW_START", "BLOCK_UNTIL"
       FROM %1$I WHERE "COMPOSITE_HASH" = $1',
      p_bandwidth_filepath_table
    );
    EXECUTE filepath_quota_sql INTO filepath_quota_rec USING composite_hash;

    IF filepath_quota_rec."BYTES_USED" IS NOT NULL THEN
      bandwidth_filepath_bytes_used := filepath_quota_rec."BYTES_USED";

      IF filepath_quota_rec."BLOCK_UNTIL" IS NOT NULL AND filepath_quota_rec."BLOCK_UNTIL" > p_now THEN
        bandwidth_filepath_allowed := FALSE;
        bandwidth_filepath_block_until := filepath_quota_rec."BLOCK_UNTIL";
      ELSIF filepath_quota_rec."WINDOW_START" IS NOT NULL
        AND (p_now - filepath_quota_rec."WINDOW_START") < bandwidth_window_filepath_seconds
        AND filepath_quota_rec."BYTES_USED" >= p_bandwidth_filepath_quota THEN
        bandwidth_filepath_allowed := FALSE;
        bandwidth_filepath_block_until := filepath_quota_rec."WINDOW_START" + bandwidth_window_filepath_seconds;
      ELSE
        bandwidth_filepath_allowed := TRUE;
        bandwidth_filepath_block_until := NULL;
      END IF;
    ELSE
      bandwidth_filepath_allowed := TRUE;
      bandwidth_filepath_bytes_used := 0;
      bandwidth_filepath_block_until := NULL;
    END IF;
  ELSE
    bandwidth_filepath_allowed := TRUE;
    bandwidth_filepath_bytes_used := 0;
    bandwidth_filepath_block_until := NULL;
  END IF;

  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;
