-- ========================================
-- Session Mapping Schema and Helpers
-- ========================================

CREATE TABLE IF NOT EXISTS "SESSION_MAPPING_TABLE" (
  "SESSION_TICKET" TEXT PRIMARY KEY,
  "FILE_PATH" TEXT NOT NULL,
  "IP_SUBNET" TEXT NOT NULL,
  "WORKER_ADDRESS" TEXT NOT NULL,
  "EXPIRE_AT" BIGINT NOT NULL,
  "CREATED_AT" BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_session_expire
  ON "SESSION_MAPPING_TABLE" ("EXPIRE_AT");

-- ========================================
-- Stored Procedure: Insert Session Mapping
-- ========================================

CREATE OR REPLACE FUNCTION session_insert(
  p_session_ticket TEXT,
  p_file_path TEXT,
  p_ip_subnet TEXT,
  p_worker_address TEXT,
  p_expire_at BIGINT,
  p_created_at BIGINT
)
RETURNS JSON AS $$
BEGIN
  INSERT INTO "SESSION_MAPPING_TABLE" (
    "SESSION_TICKET",
    "FILE_PATH",
    "IP_SUBNET",
    "WORKER_ADDRESS",
    "EXPIRE_AT",
    "CREATED_AT"
  )
  VALUES (
    p_session_ticket,
    p_file_path,
    p_ip_subnet,
    p_worker_address,
    p_expire_at,
    p_created_at
  );

  RETURN json_build_object('success', true);
EXCEPTION
  WHEN others THEN
    RETURN json_build_object('success', false, 'error', SQLERRM);
END;
$$ LANGUAGE plpgsql;

-- ========================================
-- Stored Procedure: Retrieve Session Mapping
-- ========================================

CREATE OR REPLACE FUNCTION session_get(
  p_session_ticket TEXT
)
RETURNS JSON AS $$
DECLARE
  record_row "SESSION_MAPPING_TABLE"%ROWTYPE;
BEGIN
  SELECT *
    INTO record_row
  FROM "SESSION_MAPPING_TABLE"
  WHERE "SESSION_TICKET" = p_session_ticket;

  IF record_row."FILE_PATH" IS NULL THEN
    RETURN json_build_object('found', false);
  END IF;

  RETURN json_build_object(
    'found', true,
    'file_path', record_row."FILE_PATH",
    'ip_subnet', record_row."IP_SUBNET",
    'worker_address', record_row."WORKER_ADDRESS",
    'expire_at', record_row."EXPIRE_AT"
  );
EXCEPTION
  WHEN others THEN
    RETURN json_build_object('found', false, 'error', SQLERRM);
END;
$$ LANGUAGE plpgsql;

-- ========================================
-- Stored Procedure: Cleanup Expired Sessions
-- ========================================

CREATE OR REPLACE FUNCTION session_cleanup_expired()
RETURNS JSON AS $$
DECLARE
  deleted_count BIGINT := 0;
  now_seconds BIGINT;
BEGIN
  now_seconds := EXTRACT(EPOCH FROM NOW())::BIGINT;

  DELETE FROM "SESSION_MAPPING_TABLE"
  WHERE "EXPIRE_AT" < now_seconds;

  GET DIAGNOSTICS deleted_count = ROW_COUNT;

  RETURN json_build_object('deleted', deleted_count);
EXCEPTION
  WHEN others THEN
    RETURN json_build_object('deleted', 0, 'error', SQLERRM);
END;
$$ LANGUAGE plpgsql;
