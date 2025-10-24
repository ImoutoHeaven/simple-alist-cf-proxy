-- ========================================
-- Download Cache Table Schema
-- ========================================
-- Purpose: Cache AList /api/fs/link responses to reduce API calls
-- Compatible with: SQLite (D1), PostgreSQL

CREATE TABLE IF NOT EXISTS "DOWNLOAD_CACHE_TABLE" (
  "PATH_HASH" TEXT PRIMARY KEY,      -- SHA256 hash of file path
  "PATH" TEXT NOT NULL,              -- Original file path
  "LINK_DATA" TEXT NOT NULL,         -- JSON string of response.data field
  "TIMESTAMP" INTEGER NOT NULL       -- Unix timestamp (seconds) when link was fetched
);

-- Create index to optimize cleanup queries
CREATE INDEX IF NOT EXISTS idx_timestamp ON "DOWNLOAD_CACHE_TABLE"("TIMESTAMP");


-- ========================================
-- PostgreSQL Stored Procedure: Atomic UPSERT
-- ========================================
-- Only used for custom-pg-rest mode
--
-- Function: Atomically insert or update download cache record
-- Returns: Updated PATH_HASH, LINK_DATA, TIMESTAMP for verification
--
-- Usage example (PostgREST):
--   POST /rpc/upsert_download_cache
--   Body: {"p_path_hash": "abc123...", "p_path": "/file.rar", "p_link_data": "{...}", "p_timestamp": 1700000000}

CREATE OR REPLACE FUNCTION upsert_download_cache(
  p_path_hash TEXT,
  p_path TEXT,
  p_link_data TEXT,
  p_timestamp INTEGER
)
RETURNS TABLE(
  "PATH_HASH" TEXT,
  "PATH" TEXT,
  "LINK_DATA" TEXT,
  "TIMESTAMP" INTEGER
) AS $$
BEGIN
  -- Atomic UPSERT: Insert new record or update existing one
  RETURN QUERY
  INSERT INTO "DOWNLOAD_CACHE_TABLE" ("PATH_HASH", "PATH", "LINK_DATA", "TIMESTAMP")
  VALUES (p_path_hash, p_path, p_link_data, p_timestamp)
  ON CONFLICT ON CONSTRAINT "DOWNLOAD_CACHE_TABLE_pkey" DO UPDATE SET
    "LINK_DATA" = EXCLUDED."LINK_DATA",
    "TIMESTAMP" = EXCLUDED."TIMESTAMP",
    "PATH" = EXCLUDED."PATH"  -- Update path in case of encoding changes

  -- Return updated record for application verification
  RETURNING "DOWNLOAD_CACHE_TABLE"."PATH_HASH",
            "DOWNLOAD_CACHE_TABLE"."PATH",
            "DOWNLOAD_CACHE_TABLE"."LINK_DATA",
            "DOWNLOAD_CACHE_TABLE"."TIMESTAMP";
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- Optional: Cleanup Function (PostgreSQL)
-- ========================================
-- Function: Delete expired cache records older than specified TTL
-- Usage: Can be called manually or scheduled via pg_cron
--
-- Example (delete records older than 2 hours):
--   SELECT cleanup_expired_cache(7200);

CREATE OR REPLACE FUNCTION cleanup_expired_cache(
  p_ttl_seconds INTEGER
)
RETURNS INTEGER AS $$
DECLARE
  deleted_count INTEGER;
BEGIN
  WITH deleted AS (
    DELETE FROM "DOWNLOAD_CACHE_TABLE"
    WHERE EXTRACT(EPOCH FROM NOW())::INTEGER - "TIMESTAMP" > p_ttl_seconds
    RETURNING 1
  )
  SELECT COUNT(*) INTO deleted_count FROM deleted;

  RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- Optional: pg_cron Scheduled Cleanup
-- ========================================
-- Automatically clean up expired cache every hour
-- Requires pg_cron extension: CREATE EXTENSION IF NOT EXISTS pg_cron;
--
-- Uncomment to enable (adjust TTL as needed):
--
-- SELECT cron.schedule(
--   'cleanup-download-cache',           -- Job name
--   '0 * * * *',                        -- Run every hour at minute 0
--   $$SELECT cleanup_expired_cache(7200)$$  -- Delete records older than 2 hours (adjust as needed)
-- );


-- ========================================
-- Migration Notes
-- ========================================
-- This is a new table, no migration needed from previous versions.
-- If you want to clear all cached data, run:
--
-- TRUNCATE TABLE "DOWNLOAD_CACHE_TABLE";
-- (or DELETE FROM "DOWNLOAD_CACHE_TABLE"; for SQLite)


-- ========================================
-- Throttle Protection Table Schema
-- ========================================
-- Purpose: Protect against repeated requests to failing hostnames
-- Compatible with: SQLite (D1), PostgreSQL

CREATE TABLE IF NOT EXISTS "THROTTLE_PROTECTION" (
  "HOSTNAME_HASH" TEXT PRIMARY KEY,      -- SHA256 hash of hostname
  "HOSTNAME" TEXT NOT NULL,              -- Plain hostname (e.g., "contoso-my.sharepoint.com")
  "ERROR_TIMESTAMP" INTEGER,             -- Unix timestamp (seconds) when last 4xx/5xx error occurred (NULL = no error)
  "IS_PROTECTED" INTEGER,                -- 1 = protected, NULL = not protected
  "LAST_ERROR_CODE" INTEGER              -- HTTP error code (e.g., 429, 503) or NULL
);

-- Create index to optimize cleanup queries
CREATE INDEX IF NOT EXISTS idx_throttle_timestamp ON "THROTTLE_PROTECTION"("ERROR_TIMESTAMP");


-- ========================================
-- PostgreSQL Stored Procedure: Atomic UPSERT for Throttle Protection
-- ========================================
-- Only used for custom-pg-rest mode
--
-- Function: Atomically insert or update throttle protection record
-- Returns: Updated HOSTNAME_HASH, HOSTNAME, ERROR_TIMESTAMP, IS_PROTECTED, LAST_ERROR_CODE for verification
--
-- Usage example (PostgREST):
--   POST /rpc/upsert_throttle_protection
--   Body: {"p_hostname_hash": "abc123...", "p_hostname": "contoso-my.sharepoint.com", "p_error_timestamp": 1700000000, "p_is_protected": 1, "p_last_error_code": 429}

CREATE OR REPLACE FUNCTION upsert_throttle_protection(
  p_hostname_hash TEXT,
  p_hostname TEXT,
  p_error_timestamp INTEGER,
  p_is_protected INTEGER,
  p_last_error_code INTEGER
)
RETURNS TABLE(
  "HOSTNAME_HASH" TEXT,
  "HOSTNAME" TEXT,
  "ERROR_TIMESTAMP" INTEGER,
  "IS_PROTECTED" INTEGER,
  "LAST_ERROR_CODE" INTEGER
) AS $$
BEGIN
  -- Atomic UPSERT: Insert new record or update existing one
  RETURN QUERY
  INSERT INTO "THROTTLE_PROTECTION" ("HOSTNAME_HASH", "HOSTNAME", "ERROR_TIMESTAMP", "IS_PROTECTED", "LAST_ERROR_CODE")
  VALUES (p_hostname_hash, p_hostname, p_error_timestamp, p_is_protected, p_last_error_code)
  ON CONFLICT ON CONSTRAINT "THROTTLE_PROTECTION_pkey" DO UPDATE SET
    "HOSTNAME" = EXCLUDED."HOSTNAME",
    "ERROR_TIMESTAMP" = EXCLUDED."ERROR_TIMESTAMP",
    "IS_PROTECTED" = EXCLUDED."IS_PROTECTED",
    "LAST_ERROR_CODE" = EXCLUDED."LAST_ERROR_CODE"

  -- Return updated record for application verification
  RETURNING "THROTTLE_PROTECTION"."HOSTNAME_HASH",
            "THROTTLE_PROTECTION"."HOSTNAME",
            "THROTTLE_PROTECTION"."ERROR_TIMESTAMP",
            "THROTTLE_PROTECTION"."IS_PROTECTED",
            "THROTTLE_PROTECTION"."LAST_ERROR_CODE";
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- Optional: Throttle Cleanup Function (PostgreSQL)
-- ========================================
-- Function: Delete throttle protection records that are no longer needed
-- Removes records where IS_PROTECTED IS NULL and ERROR_TIMESTAMP is older than specified TTL
-- Usage: Can be called manually or scheduled via pg_cron
--
-- Example (delete records older than 2 minutes):
--   SELECT cleanup_throttle_protection(120);

CREATE OR REPLACE FUNCTION cleanup_throttle_protection(
  p_ttl_seconds INTEGER
)
RETURNS INTEGER AS $$
DECLARE
  deleted_count INTEGER;
BEGIN
  WITH deleted AS (
    DELETE FROM "THROTTLE_PROTECTION"
    WHERE "IS_PROTECTED" IS NULL
      AND ("ERROR_TIMESTAMP" IS NULL OR EXTRACT(EPOCH FROM NOW())::INTEGER - "ERROR_TIMESTAMP" > p_ttl_seconds)
    RETURNING 1
  )
  SELECT COUNT(*) INTO deleted_count FROM deleted;

  RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;


-- ========================================
-- Optional: pg_cron Scheduled Cleanup for Throttle Protection
-- ========================================
-- Automatically clean up throttle protection records every hour
-- Requires pg_cron extension: CREATE EXTENSION IF NOT EXISTS pg_cron;
--
-- Uncomment to enable (adjust TTL as needed):
--
-- SELECT cron.schedule(
--   'cleanup-throttle-protection',       -- Job name
--   '0 * * * *',                          -- Run every hour at minute 0
--   $$SELECT cleanup_throttle_protection(120)$$  -- Delete records older than 2 minutes (adjust as needed)
-- );
