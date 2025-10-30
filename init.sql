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
-- File Download Quota Table Schema
-- ========================================
-- Purpose: Track per-file download quotas per IP range
-- Compatible with: SQLite (D1), PostgreSQL
CREATE TABLE IF NOT EXISTS file_ip_download_quota (
  ip_range_hash TEXT,
  filepath_hash TEXT,
  bytes_downloaded BIGINT DEFAULT 0,
  last_window_time INTEGER NOT NULL,
  blocked_until INTEGER,
  PRIMARY KEY (ip_range_hash, filepath_hash)
);


-- ========================================
-- Global Download Quota Table Schema
-- ========================================
-- Purpose: Track global download quotas per IP range
-- Compatible with: SQLite (D1), PostgreSQL
CREATE TABLE IF NOT EXISTS global_ip_download_quota (
  ip_range_hash TEXT PRIMARY KEY,
  total_bytes_downloaded BIGINT DEFAULT 0,
  last_window_time INTEGER NOT NULL,
  blocked_until INTEGER
);


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
-- PostgreSQL Stored Procedure: File Download Quota Check
-- ========================================
CREATE OR REPLACE FUNCTION download_check_file_quota(
  p_ip_range_hash TEXT,
  p_filepath_hash TEXT,
  p_deduct_bytes BIGINT,
  p_now INTEGER,
  p_window_seconds INTEGER,
  p_max_quota BIGINT,
  p_block_seconds INTEGER,
  p_file_table_name TEXT DEFAULT 'file_ip_download_quota'
)
RETURNS TABLE(
  bytes_downloaded BIGINT,
  last_window_time INTEGER,
  blocked_until INTEGER,
  quota_exceeded BOOLEAN
) AS $$
DECLARE
  sql TEXT;
BEGIN
  sql := format(
    'WITH upsert AS (
       INSERT INTO %1$I ("ip_range_hash", "filepath_hash", "bytes_downloaded", "last_window_time", "blocked_until")
       VALUES (
         $1,
         $2,
         CASE WHEN GREATEST(COALESCE($3, 0), 0) > $6 THEN 0 ELSE GREATEST(COALESCE($3, 0), 0) END,
         $4,
         CASE
           WHEN GREATEST(COALESCE($3, 0), 0) > $6 THEN $4 + CASE WHEN COALESCE($7, 0) > 0 THEN COALESCE($7, 0) ELSE 1 END
           ELSE NULL
         END
       )
       ON CONFLICT ("ip_range_hash", "filepath_hash") DO UPDATE SET
         "bytes_downloaded" = CASE
           WHEN %1$I."blocked_until" IS NOT NULL AND %1$I."blocked_until" > $4 THEN %1$I."bytes_downloaded"
           WHEN $4 - %1$I."last_window_time" >= $5 THEN CASE WHEN GREATEST(COALESCE($3, 0), 0) > $6 THEN 0 ELSE GREATEST(COALESCE($3, 0), 0) END
           WHEN %1$I."bytes_downloaded" + GREATEST(COALESCE($3, 0), 0) > $6 THEN %1$I."bytes_downloaded"
           ELSE %1$I."bytes_downloaded" + GREATEST(COALESCE($3, 0), 0)
         END,
         "last_window_time" = CASE
           WHEN %1$I."blocked_until" IS NOT NULL AND %1$I."blocked_until" > $4 THEN %1$I."last_window_time"
           WHEN $4 - %1$I."last_window_time" >= $5 THEN $4
           ELSE %1$I."last_window_time"
         END,
         "blocked_until" = CASE
           WHEN %1$I."blocked_until" IS NOT NULL AND %1$I."blocked_until" > $4 THEN %1$I."blocked_until"
           WHEN $4 - %1$I."last_window_time" >= $5 THEN CASE
             WHEN GREATEST(COALESCE($3, 0), 0) > $6 THEN $4 + CASE WHEN COALESCE($7, 0) > 0 THEN COALESCE($7, 0) ELSE 1 END
             ELSE NULL
           END
           WHEN %1$I."bytes_downloaded" + GREATEST(COALESCE($3, 0), 0) > $6 THEN $4 + CASE WHEN COALESCE($7, 0) > 0 THEN COALESCE($7, 0) ELSE 1 END
           ELSE NULL
         END
       RETURNING
         "bytes_downloaded",
         "last_window_time",
         "blocked_until"
     )
     SELECT
       "bytes_downloaded",
       "last_window_time",
       "blocked_until",
       ("blocked_until" IS NOT NULL AND "blocked_until" > $4) AS quota_exceeded
     FROM upsert',
    p_file_table_name
  );

  RETURN QUERY EXECUTE sql USING
    p_ip_range_hash,
    p_filepath_hash,
    p_deduct_bytes,
    p_now,
    p_window_seconds,
    p_max_quota,
    p_block_seconds;
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- PostgreSQL Stored Procedure: Global Download Quota Check
-- ========================================
CREATE OR REPLACE FUNCTION download_check_global_quota(
  p_ip_range_hash TEXT,
  p_deduct_bytes BIGINT,
  p_now INTEGER,
  p_window_seconds INTEGER,
  p_max_quota BIGINT,
  p_block_seconds INTEGER,
  p_global_table_name TEXT DEFAULT 'global_ip_download_quota'
)
RETURNS TABLE(
  total_bytes_downloaded BIGINT,
  last_window_time INTEGER,
  blocked_until INTEGER,
  quota_exceeded BOOLEAN
) AS $$
DECLARE
  sql TEXT;
BEGIN
  sql := format(
    'WITH upsert AS (
       INSERT INTO %1$I ("ip_range_hash", "total_bytes_downloaded", "last_window_time", "blocked_until")
       VALUES (
         $1,
         CASE WHEN GREATEST(COALESCE($2, 0), 0) > $5 THEN 0 ELSE GREATEST(COALESCE($2, 0), 0) END,
         $3,
         CASE
           WHEN GREATEST(COALESCE($2, 0), 0) > $5 THEN $3 + CASE WHEN COALESCE($6, 0) > 0 THEN COALESCE($6, 0) ELSE 1 END
           ELSE NULL
         END
       )
       ON CONFLICT ("ip_range_hash") DO UPDATE SET
         "total_bytes_downloaded" = CASE
           WHEN %1$I."blocked_until" IS NOT NULL AND %1$I."blocked_until" > $3 THEN %1$I."total_bytes_downloaded"
           WHEN $3 - %1$I."last_window_time" >= $4 THEN CASE WHEN GREATEST(COALESCE($2, 0), 0) > $5 THEN 0 ELSE GREATEST(COALESCE($2, 0), 0) END
           WHEN %1$I."total_bytes_downloaded" + GREATEST(COALESCE($2, 0), 0) > $5 THEN %1$I."total_bytes_downloaded"
           ELSE %1$I."total_bytes_downloaded" + GREATEST(COALESCE($2, 0), 0)
         END,
         "last_window_time" = CASE
           WHEN %1$I."blocked_until" IS NOT NULL AND %1$I."blocked_until" > $3 THEN %1$I."last_window_time"
           WHEN $3 - %1$I."last_window_time" >= $4 THEN $3
           ELSE %1$I."last_window_time"
         END,
         "blocked_until" = CASE
           WHEN %1$I."blocked_until" IS NOT NULL AND %1$I."blocked_until" > $3 THEN %1$I."blocked_until"
           WHEN $3 - %1$I."last_window_time" >= $4 THEN CASE
             WHEN GREATEST(COALESCE($2, 0), 0) > $5 THEN $3 + CASE WHEN COALESCE($6, 0) > 0 THEN COALESCE($6, 0) ELSE 1 END
             ELSE NULL
           END
           WHEN %1$I."total_bytes_downloaded" + GREATEST(COALESCE($2, 0), 0) > $5 THEN $3 + CASE WHEN COALESCE($6, 0) > 0 THEN COALESCE($6, 0) ELSE 1 END
           ELSE NULL
         END
       RETURNING
         "total_bytes_downloaded",
         "last_window_time",
         "blocked_until"
     )
     SELECT
       "total_bytes_downloaded",
       "last_window_time",
       "blocked_until",
       ("blocked_until" IS NOT NULL AND "blocked_until" > $3) AS quota_exceeded
     FROM upsert',
    p_global_table_name
  );

  RETURN QUERY EXECUTE sql USING
    p_ip_range_hash,
    p_deduct_bytes,
    p_now,
    p_window_seconds,
    p_max_quota,
    p_block_seconds;
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- PostgreSQL Stored Procedure: Update Quota Progress
-- ========================================
CREATE OR REPLACE FUNCTION download_update_quota_progress(
  p_ip_range_hash TEXT,
  p_filepath_hash TEXT,
  p_update_bytes BIGINT,
  p_now INTEGER,
  p_file_table_name TEXT DEFAULT 'file_ip_download_quota',
  p_global_table_name TEXT DEFAULT 'global_ip_download_quota'
)
RETURNS TABLE(
  success BOOLEAN
) AS $$
DECLARE
  file_select_sql TEXT;
  file_insert_sql TEXT;
  file_update_sql TEXT;
  global_select_sql TEXT;
  global_insert_sql TEXT;
  global_update_sql TEXT;
  file_record RECORD;
  global_record RECORD;
  current_file_bytes BIGINT;
  new_file_bytes BIGINT;
  delta BIGINT;
  current_global_bytes BIGINT;
  new_global_bytes BIGINT;
  safe_update_bytes BIGINT;
BEGIN
  safe_update_bytes := GREATEST(COALESCE(p_update_bytes, 0), 0);

  file_select_sql := format(
    'SELECT "ip_range_hash", "filepath_hash", "bytes_downloaded"
       FROM %1$I
       WHERE "ip_range_hash" = $1 AND "filepath_hash" = $2
       FOR UPDATE',
    p_file_table_name
  );

  EXECUTE file_select_sql INTO file_record USING p_ip_range_hash, p_filepath_hash;

  IF file_record."ip_range_hash" IS NULL THEN
    file_insert_sql := format(
      'INSERT INTO %1$I ("ip_range_hash", "filepath_hash", "bytes_downloaded", "last_window_time", "blocked_until")
         VALUES ($1, $2, $3, $4, NULL)
         ON CONFLICT ("ip_range_hash", "filepath_hash") DO NOTHING',
      p_file_table_name
    );

    EXECUTE file_insert_sql USING p_ip_range_hash, p_filepath_hash, safe_update_bytes, p_now;

    EXECUTE file_select_sql INTO file_record USING p_ip_range_hash, p_filepath_hash;
  END IF;

  current_file_bytes := COALESCE(file_record."bytes_downloaded", 0);
  new_file_bytes := safe_update_bytes;

  file_update_sql := format(
    'UPDATE %1$I
       SET "bytes_downloaded" = $3
       WHERE "ip_range_hash" = $1 AND "filepath_hash" = $2',
    p_file_table_name
  );
  EXECUTE file_update_sql USING p_ip_range_hash, p_filepath_hash, new_file_bytes;

  delta := new_file_bytes - current_file_bytes;

  global_select_sql := format(
    'SELECT "ip_range_hash", "total_bytes_downloaded"
       FROM %1$I
       WHERE "ip_range_hash" = $1
       FOR UPDATE',
    p_global_table_name
  );
  EXECUTE global_select_sql INTO global_record USING p_ip_range_hash;

  IF global_record."ip_range_hash" IS NULL THEN
    global_insert_sql := format(
      'INSERT INTO %1$I ("ip_range_hash", "total_bytes_downloaded", "last_window_time", "blocked_until")
         VALUES ($1, $2, $3, NULL)
         ON CONFLICT ("ip_range_hash") DO NOTHING',
      p_global_table_name
    );
    EXECUTE global_insert_sql USING p_ip_range_hash, GREATEST(delta, 0), p_now;

    EXECUTE global_select_sql INTO global_record USING p_ip_range_hash;
  END IF;

  current_global_bytes := COALESCE(global_record."total_bytes_downloaded", 0);
  new_global_bytes := current_global_bytes + delta;

  IF new_global_bytes < 0 THEN
    new_global_bytes := 0;
  END IF;

  global_update_sql := format(
    'UPDATE %1$I
       SET "total_bytes_downloaded" = $2
       WHERE "ip_range_hash" = $1',
    p_global_table_name
  );
  EXECUTE global_update_sql USING p_ip_range_hash, new_global_bytes;

  RETURN QUERY SELECT TRUE;
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- PostgreSQL Stored Procedure: Refund Quota Progress
-- ========================================
CREATE OR REPLACE FUNCTION download_refund_quota(
  p_ip_range_hash TEXT,
  p_filepath_hash TEXT,
  p_refund_bytes BIGINT,
  p_file_table_name TEXT DEFAULT 'file_ip_download_quota',
  p_global_table_name TEXT DEFAULT 'global_ip_download_quota'
)
RETURNS TABLE(
  success BOOLEAN
) AS $$
DECLARE
  file_select_sql TEXT;
  file_update_sql TEXT;
  global_select_sql TEXT;
  global_update_sql TEXT;
  file_record RECORD;
  global_record RECORD;
  refund_amount BIGINT;
  current_file_bytes BIGINT;
  new_file_bytes BIGINT;
  current_global_bytes BIGINT;
  new_global_bytes BIGINT;
  adjusted_delta BIGINT;
BEGIN
  refund_amount := GREATEST(COALESCE(p_refund_bytes, 0), 0);

  file_select_sql := format(
    'SELECT "ip_range_hash", "filepath_hash", "bytes_downloaded"
       FROM %1$I
       WHERE "ip_range_hash" = $1 AND "filepath_hash" = $2
       FOR UPDATE',
    p_file_table_name
  );

  EXECUTE file_select_sql INTO file_record USING p_ip_range_hash, p_filepath_hash;

  IF file_record."ip_range_hash" IS NOT NULL THEN
    current_file_bytes := COALESCE(file_record."bytes_downloaded", 0);
    new_file_bytes := current_file_bytes - refund_amount;
    IF new_file_bytes < 0 THEN
      new_file_bytes := 0;
    END IF;

    file_update_sql := format(
      'UPDATE %1$I
         SET "bytes_downloaded" = $3
         WHERE "ip_range_hash" = $1 AND "filepath_hash" = $2',
      p_file_table_name
    );
    EXECUTE file_update_sql USING p_ip_range_hash, p_filepath_hash, new_file_bytes;

    adjusted_delta := current_file_bytes - new_file_bytes;

    global_select_sql := format(
      'SELECT "ip_range_hash", "total_bytes_downloaded"
         FROM %1$I
         WHERE "ip_range_hash" = $1
         FOR UPDATE',
      p_global_table_name
    );
    EXECUTE global_select_sql INTO global_record USING p_ip_range_hash;

    IF global_record."ip_range_hash" IS NOT NULL THEN
      current_global_bytes := COALESCE(global_record."total_bytes_downloaded", 0);
      new_global_bytes := current_global_bytes - adjusted_delta;
      IF new_global_bytes < 0 THEN
        new_global_bytes := 0;
      END IF;

      global_update_sql := format(
        'UPDATE %1$I
           SET "total_bytes_downloaded" = $2
           WHERE "ip_range_hash" = $1',
        p_global_table_name
      );
      EXECUTE global_update_sql USING p_ip_range_hash, new_global_bytes;
    END IF;
  END IF;

  RETURN QUERY SELECT TRUE;
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

  -- Quota params
  p_filepath_hash TEXT DEFAULT NULL,
  p_deduct_bytes BIGINT DEFAULT 0,
  p_file_quota_window_seconds INTEGER DEFAULT NULL,
  p_file_quota_max_bytes BIGINT DEFAULT NULL,
  p_file_quota_block_seconds INTEGER DEFAULT NULL,
  p_global_quota_window_seconds INTEGER DEFAULT NULL,
  p_global_quota_max_bytes BIGINT DEFAULT NULL,
  p_global_quota_block_seconds INTEGER DEFAULT NULL,
  p_file_quota_table_name TEXT DEFAULT 'file_ip_download_quota',
  p_global_quota_table_name TEXT DEFAULT 'global_ip_download_quota'
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

  -- File quota result (nullable if not enabled)
  file_quota_bytes_downloaded BIGINT,
  file_quota_blocked_until INTEGER,
  file_quota_exceeded BOOLEAN,

  -- Global quota result (nullable if not enabled)
  global_quota_bytes_downloaded BIGINT,
  global_quota_blocked_until INTEGER,
  global_quota_exceeded BOOLEAN
) AS $$
DECLARE
  cache_sql TEXT;
  rate_result RECORD;
  throttle_sql TEXT;
  cache_rec RECORD;
  throttle_rec RECORD;
  cache_hostname_hash_var TEXT;
  file_quota_result RECORD;
  global_quota_result RECORD;
  deduct_bytes_value BIGINT;
BEGIN
  deduct_bytes_value := GREATEST(COALESCE(p_deduct_bytes, 0), 0);

  file_quota_bytes_downloaded := NULL;
  file_quota_blocked_until := NULL;
  file_quota_exceeded := FALSE;
  global_quota_bytes_downloaded := NULL;
  global_quota_blocked_until := NULL;
  global_quota_exceeded := FALSE;

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

  -- Step 4: Check per-file quota when parameters are provided
  IF p_filepath_hash IS NOT NULL
     AND p_file_quota_window_seconds IS NOT NULL
     AND p_file_quota_max_bytes IS NOT NULL
     AND p_file_quota_block_seconds IS NOT NULL
     AND p_file_quota_table_name IS NOT NULL THEN
    SELECT * INTO file_quota_result FROM download_check_file_quota(
      p_ip_hash,
      p_filepath_hash,
      deduct_bytes_value,
      p_now,
      p_file_quota_window_seconds,
      p_file_quota_max_bytes,
      p_file_quota_block_seconds,
      p_file_quota_table_name
    );

    IF file_quota_result.bytes_downloaded IS NOT NULL THEN
      file_quota_bytes_downloaded := file_quota_result.bytes_downloaded;
      file_quota_blocked_until := file_quota_result.blocked_until;
      file_quota_exceeded := file_quota_result.quota_exceeded;
    END IF;
  END IF;

  -- Step 5: Check global quota when parameters are provided
  IF p_global_quota_window_seconds IS NOT NULL
     AND p_global_quota_max_bytes IS NOT NULL
     AND p_global_quota_block_seconds IS NOT NULL
     AND p_global_quota_table_name IS NOT NULL THEN
    SELECT * INTO global_quota_result FROM download_check_global_quota(
      p_ip_hash,
      deduct_bytes_value,
      p_now,
      p_global_quota_window_seconds,
      p_global_quota_max_bytes,
      p_global_quota_block_seconds,
      p_global_quota_table_name
    );

    IF global_quota_result.total_bytes_downloaded IS NOT NULL THEN
      global_quota_bytes_downloaded := global_quota_result.total_bytes_downloaded;
      global_quota_blocked_until := global_quota_result.blocked_until;
      global_quota_exceeded := global_quota_result.quota_exceeded;
    END IF;
  END IF;

  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;
