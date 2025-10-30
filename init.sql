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
-- Compatible with: PostgreSQL

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
-- Quota Transaction Log Schema
-- ========================================
CREATE TABLE IF NOT EXISTS quota_transactions (
  id BIGSERIAL PRIMARY KEY,
  request_id TEXT NOT NULL,
  ip_range_hash TEXT NOT NULL,
  filepath_hash TEXT NOT NULL,
  transaction_type TEXT NOT NULL CHECK (transaction_type IN ('debit', 'settlement')),
  estimated_bytes BIGINT,
  actual_bytes BIGINT,
  amount BIGINT NOT NULL,
  window_start_time BIGINT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  CONSTRAINT quota_transactions_amount_check
    CHECK ((transaction_type = 'debit' AND amount >= 0) OR (transaction_type = 'settlement' AND amount <= 0)),
  CONSTRAINT quota_transactions_estimated_check
    CHECK (estimated_bytes IS NULL OR estimated_bytes >= 0),
  CONSTRAINT quota_transactions_actual_check
    CHECK (actual_bytes IS NULL OR actual_bytes >= 0),
  CONSTRAINT quota_transactions_window_check
    CHECK (window_start_time >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_quota_transactions_request
  ON quota_transactions (request_id);
CREATE INDEX IF NOT EXISTS idx_quota_transactions_ip_window
  ON quota_transactions (ip_range_hash, window_start_time);
CREATE INDEX IF NOT EXISTS idx_quota_transactions_file_ip
  ON quota_transactions (filepath_hash, ip_range_hash);
CREATE INDEX IF NOT EXISTS idx_quota_transactions_created_at
  ON quota_transactions (created_at);

-- ========================================
-- Quota Block Table Schema
-- ========================================
CREATE TABLE IF NOT EXISTS quota_blocks (
  ip_range_hash TEXT NOT NULL,
  filepath_hash TEXT,
  block_type TEXT NOT NULL CHECK (block_type IN ('file_quota', 'global_quota', 'rate_limit')),
  blocked_until BIGINT NOT NULL,
  reason TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  filepath_hash_key TEXT GENERATED ALWAYS AS (COALESCE(filepath_hash, '')) STORED,
  PRIMARY KEY (ip_range_hash, block_type, filepath_hash_key),
  CONSTRAINT quota_blocks_blocked_until_check CHECK (blocked_until >= 0)
);

CREATE INDEX IF NOT EXISTS idx_quota_blocks_blocked_until
  ON quota_blocks (blocked_until);
CREATE INDEX IF NOT EXISTS idx_quota_blocks_created_at
  ON quota_blocks (created_at);

-- ========================================
-- PostgreSQL Stored Procedure: Check Quota Blocks
-- ========================================
CREATE OR REPLACE FUNCTION check_quota_block(
  p_ip_range_hash TEXT,
  p_filepath_hash TEXT,
  p_now BIGINT,
  p_block_types TEXT[] DEFAULT ARRAY['file_quota', 'global_quota']::TEXT[]
)
RETURNS TABLE(
  block_type TEXT,
  blocked_until BIGINT,
  reason TEXT
) AS $$
BEGIN
  RETURN QUERY
  SELECT b.block_type, b.blocked_until, b.reason
    FROM quota_blocks b
   WHERE b.ip_range_hash = p_ip_range_hash
     AND b.block_type = ANY(p_block_types)
     AND b.blocked_until > p_now
     AND (
       (b.filepath_hash IS NULL)
       OR (p_filepath_hash IS NOT NULL AND b.filepath_hash = p_filepath_hash)
     );
END;
$$ LANGUAGE plpgsql;

-- ========================================
-- PostgreSQL Stored Procedure: Check Quota Balance
-- ========================================
CREATE OR REPLACE FUNCTION check_quota_balance(
  p_ip_range_hash TEXT,
  p_filepath_hash TEXT,
  p_deduct_bytes BIGINT,
  p_now BIGINT,
  p_file_window_seconds INTEGER DEFAULT NULL,
  p_file_max_bytes BIGINT DEFAULT NULL,
  p_file_block_seconds INTEGER DEFAULT NULL,
  p_global_window_seconds INTEGER DEFAULT NULL,
  p_global_max_bytes BIGINT DEFAULT NULL,
  p_global_block_seconds INTEGER DEFAULT NULL
)
RETURNS TABLE(
  file_usage BIGINT,
  file_projected BIGINT,
  file_limit BIGINT,
  file_remaining BIGINT,
  file_insufficient BOOLEAN,
  file_blocked_until BIGINT,
  global_usage BIGINT,
  global_projected BIGINT,
  global_limit BIGINT,
  global_remaining BIGINT,
  global_insufficient BOOLEAN,
  global_blocked_until BIGINT
) AS $$
DECLARE
  sanitized_deduct BIGINT := GREATEST(COALESCE(p_deduct_bytes, 0), 0);
  cutoff_file_ms BIGINT;
  cutoff_global_ms BIGINT;
  block_until_value BIGINT;
BEGIN
  file_usage := NULL;
  file_projected := NULL;
  file_limit := NULL;
  file_remaining := NULL;
  file_insufficient := NULL;
  file_blocked_until := NULL;
  global_usage := NULL;
  global_projected := NULL;
  global_limit := NULL;
  global_remaining := NULL;
  global_insufficient := NULL;
  global_blocked_until := NULL;

  IF p_file_window_seconds IS NOT NULL AND p_file_window_seconds > 0
     AND p_file_max_bytes IS NOT NULL AND p_file_max_bytes > 0 THEN
    cutoff_file_ms := (p_now - p_file_window_seconds) * 1000;
    IF cutoff_file_ms < 0 THEN
      cutoff_file_ms := 0;
    END IF;

    SELECT COALESCE(SUM(amount), 0)
      INTO file_usage
      FROM quota_transactions
     WHERE ip_range_hash = p_ip_range_hash
       AND filepath_hash = p_filepath_hash
       AND window_start_time >= cutoff_file_ms;

    file_projected := file_usage + sanitized_deduct;
    file_limit := p_file_max_bytes;
    file_remaining := GREATEST(p_file_max_bytes - file_usage, 0);
    file_insufficient := file_projected > p_file_max_bytes;

    IF file_insufficient THEN
      IF p_file_block_seconds IS NOT NULL AND p_file_block_seconds > 0 THEN
        block_until_value := p_now + p_file_block_seconds;

        INSERT INTO quota_blocks (ip_range_hash, filepath_hash, block_type, blocked_until, reason)
        VALUES (
          p_ip_range_hash,
          p_filepath_hash,
          'file_quota',
          block_until_value,
          format('Quota exceeded: %s > %s bytes within %s seconds', file_projected, p_file_max_bytes, p_file_window_seconds)
        )
        ON CONFLICT (ip_range_hash, block_type, filepath_hash_key) DO UPDATE
          SET blocked_until = GREATEST(quota_blocks.blocked_until, EXCLUDED.blocked_until),
              reason = EXCLUDED.reason;

        SELECT blocked_until
          INTO file_blocked_until
          FROM quota_blocks
         WHERE ip_range_hash = p_ip_range_hash
           AND block_type = 'file_quota'
           AND (
             (p_filepath_hash IS NULL AND filepath_hash IS NULL)
             OR (p_filepath_hash IS NOT NULL AND filepath_hash = p_filepath_hash)
           );
      ELSE
        file_blocked_until := NULL;
      END IF;
    ELSE
      file_blocked_until := NULL;
    END IF;
  ELSE
    file_usage := 0;
    file_projected := sanitized_deduct;
    file_limit := NULL;
    file_remaining := NULL;
    file_insufficient := FALSE;
    file_blocked_until := NULL;
  END IF;

  IF p_global_window_seconds IS NOT NULL AND p_global_window_seconds > 0
     AND p_global_max_bytes IS NOT NULL AND p_global_max_bytes > 0 THEN
    cutoff_global_ms := (p_now - p_global_window_seconds) * 1000;
    IF cutoff_global_ms < 0 THEN
      cutoff_global_ms := 0;
    END IF;

    SELECT COALESCE(SUM(amount), 0)
      INTO global_usage
      FROM quota_transactions
     WHERE ip_range_hash = p_ip_range_hash
       AND window_start_time >= cutoff_global_ms;

    global_projected := global_usage + sanitized_deduct;
    global_limit := p_global_max_bytes;
    global_remaining := GREATEST(p_global_max_bytes - global_usage, 0);
    global_insufficient := global_projected > p_global_max_bytes;

    IF global_insufficient THEN
      IF p_global_block_seconds IS NOT NULL AND p_global_block_seconds > 0 THEN
        block_until_value := p_now + p_global_block_seconds;

        INSERT INTO quota_blocks (ip_range_hash, filepath_hash, block_type, blocked_until, reason)
        VALUES (
          p_ip_range_hash,
          NULL,
          'global_quota',
          block_until_value,
          format('Global quota exceeded: %s > %s bytes within %s seconds', global_projected, p_global_max_bytes, p_global_window_seconds)
        )
        ON CONFLICT (ip_range_hash, block_type, filepath_hash_key) DO UPDATE
          SET blocked_until = GREATEST(quota_blocks.blocked_until, EXCLUDED.blocked_until),
              reason = EXCLUDED.reason;

        SELECT blocked_until
          INTO global_blocked_until
          FROM quota_blocks
         WHERE ip_range_hash = p_ip_range_hash
           AND block_type = 'global_quota'
           AND filepath_hash IS NULL;
      ELSE
        global_blocked_until := NULL;
      END IF;
    ELSE
      global_blocked_until := NULL;
    END IF;
  ELSE
    global_usage := 0;
    global_projected := sanitized_deduct;
    global_limit := NULL;
    global_remaining := NULL;
    global_insufficient := FALSE;
    global_blocked_until := NULL;
  END IF;

  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- ========================================
-- PostgreSQL Stored Procedure: Insert Quota Debit
-- ========================================
CREATE OR REPLACE FUNCTION insert_quota_debit(
  p_request_id TEXT,
  p_ip_range_hash TEXT,
  p_filepath_hash TEXT,
  p_estimated_bytes BIGINT,
  p_amount BIGINT,
  p_window_start_time BIGINT
)
RETURNS TABLE(success BOOLEAN) AS $$
DECLARE
  sanitized_amount BIGINT := GREATEST(COALESCE(p_amount, 0), 0);
  sanitized_estimated BIGINT := GREATEST(COALESCE(p_estimated_bytes, 0), 0);
  sanitized_window BIGINT := GREATEST(COALESCE(p_window_start_time, 0), 0);
  full_request_id TEXT := p_request_id || '::debit';
  inserted_id BIGINT;
BEGIN
  INSERT INTO quota_transactions (
    request_id,
    ip_range_hash,
    filepath_hash,
    transaction_type,
    estimated_bytes,
    actual_bytes,
    amount,
    window_start_time
  )
  VALUES (
    full_request_id,
    p_ip_range_hash,
    p_filepath_hash,
    'debit',
    sanitized_estimated,
    NULL,
    sanitized_amount,
    sanitized_window
  )
  ON CONFLICT (request_id) DO NOTHING
  RETURNING id INTO inserted_id;

  success := inserted_id IS NOT NULL;
  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- ========================================
-- PostgreSQL Stored Procedure: Insert Quota Settlement
-- ========================================
CREATE OR REPLACE FUNCTION insert_quota_settlement(
  p_request_id TEXT,
  p_ip_range_hash TEXT,
  p_filepath_hash TEXT,
  p_actual_bytes BIGINT,
  p_refund_amount BIGINT,
  p_window_start_time BIGINT
)
RETURNS TABLE(success BOOLEAN) AS $$
DECLARE
  sanitized_actual BIGINT := GREATEST(COALESCE(p_actual_bytes, 0), 0);
  sanitized_window BIGINT := GREATEST(COALESCE(p_window_start_time, 0), 0);
  normalized_refund BIGINT := COALESCE(p_refund_amount, 0);
  full_request_id TEXT := p_request_id || '::settlement';
  inserted_id BIGINT;
BEGIN
  IF normalized_refund > 0 THEN
    normalized_refund := -normalized_refund;
  END IF;

  INSERT INTO quota_transactions (
    request_id,
    ip_range_hash,
    filepath_hash,
    transaction_type,
    estimated_bytes,
    actual_bytes,
    amount,
    window_start_time
  )
  VALUES (
    full_request_id,
    p_ip_range_hash,
    p_filepath_hash,
    'settlement',
    NULL,
    sanitized_actual,
    normalized_refund,
    sanitized_window
  )
  ON CONFLICT (request_id) DO NOTHING
  RETURNING id INTO inserted_id;

  success := inserted_id IS NOT NULL;
  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- ========================================
-- PostgreSQL Stored Procedure: Delete Quota Block
-- ========================================
CREATE OR REPLACE FUNCTION delete_quota_block(
  p_ip_range_hash TEXT,
  p_filepath_hash TEXT,
  p_block_type TEXT
)
RETURNS INTEGER AS $$
DECLARE
  deleted_count INTEGER;
BEGIN
  DELETE FROM quota_blocks
   WHERE ip_range_hash = p_ip_range_hash
     AND block_type = p_block_type
     AND (
       (p_filepath_hash IS NULL AND filepath_hash IS NULL)
       OR (p_filepath_hash IS NOT NULL AND filepath_hash = p_filepath_hash)
     );

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
  p_throttle_table_name TEXT
)
RETURNS TABLE(
  cache_link_data TEXT,
  cache_timestamp INTEGER,
  cache_hostname_hash TEXT,
  rate_access_count INTEGER,
  rate_last_window_time INTEGER,
  rate_block_until INTEGER,
  throttle_record_exists BOOLEAN,
  throttle_is_protected INTEGER,
  throttle_error_timestamp INTEGER,
  throttle_error_code INTEGER
) AS $$
DECLARE
  cache_sql TEXT;
  cache_rec RECORD;
  rate_result RECORD;
  throttle_sql TEXT;
  throttle_rec RECORD;
  cache_hostname_hash_var TEXT;
BEGIN
  -- Step 1: Cache lookup
  cache_sql := format(
    'SELECT "LINK_DATA", "TIMESTAMP", "HOSTNAME_HASH" FROM %1$I WHERE "PATH_HASH" = $1',
    p_cache_table_name
  );
  EXECUTE cache_sql INTO cache_rec USING p_path_hash;

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

  -- Step 2: Rate limit upsert
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

  -- Step 3: Throttle lookup (conditional)
  IF cache_hostname_hash_var IS NOT NULL THEN
    throttle_sql := format(
      'SELECT "IS_PROTECTED", "ERROR_TIMESTAMP", "LAST_ERROR_CODE"
         FROM %1$I WHERE "HOSTNAME_HASH" = $1',
      p_throttle_table_name
    );
    EXECUTE throttle_sql INTO throttle_rec USING cache_hostname_hash_var;

    IF throttle_rec."IS_PROTECTED" IS NOT NULL THEN
      throttle_record_exists := TRUE;
      throttle_is_protected := throttle_rec."IS_PROTECTED";
      throttle_error_timestamp := throttle_rec."ERROR_TIMESTAMP";
      throttle_error_code := throttle_rec."LAST_ERROR_CODE";
    ELSE
      throttle_record_exists := FALSE;
      throttle_is_protected := NULL;
      throttle_error_timestamp := NULL;
      throttle_error_code := NULL;
    END IF;
  ELSE
    throttle_record_exists := FALSE;
    throttle_is_protected := NULL;
    throttle_error_timestamp := NULL;
    throttle_error_code := NULL;
  END IF;

  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;
