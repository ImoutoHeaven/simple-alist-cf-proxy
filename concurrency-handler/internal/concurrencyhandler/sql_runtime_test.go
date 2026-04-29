package concurrencyhandler

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const runtimePostgresImage = "postgres:16-alpine"

func concurrencyModuleRootDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime caller unavailable")
	}
	return filepath.Join(filepath.Dir(file), "..", "..")
}

func normalizeRuntimeContainerID(raw string) string {
	lines := strings.Split(strings.TrimSpace(raw), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line != "" {
			return line
		}
	}
	return ""
}

func normalizeRuntimeDockerAddr(raw string) string {
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "127.0.0.1:"):
			return line
		case strings.HasPrefix(line, "0.0.0.0:"):
			return "127.0.0.1:" + strings.TrimPrefix(line, "0.0.0.0:")
		case strings.HasPrefix(line, "[::]:"):
			return "127.0.0.1:" + strings.TrimPrefix(line, "[::]:")
		}
	}
	return ""
}

func requireRuntimeConcurrencyDB(t *testing.T) *sql.DB {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}
	if out, err := exec.Command("docker", "version", "--format", "{{.Server.Version}}").CombinedOutput(); err != nil {
		t.Skipf("docker daemon unavailable: %v (%s)", err, strings.TrimSpace(string(out)))
	}

	dbName := fmt.Sprintf("concurrency_sql_%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	runCmd := exec.CommandContext(
		ctx,
		"docker",
		"run",
		"--rm",
		"-d",
		"-e", "POSTGRES_PASSWORD=postgres",
		"-e", "POSTGRES_DB="+dbName,
		"-p", "127.0.0.1::5432",
		runtimePostgresImage,
	)
	runOut, err := runCmd.CombinedOutput()
	if err != nil {
		t.Skipf("start postgres container: %v (%s)", err, strings.TrimSpace(string(runOut)))
	}
	containerID := normalizeRuntimeContainerID(string(runOut))
	if containerID == "" {
		t.Fatalf("docker run returned no container id: %s", strings.TrimSpace(string(runOut)))
	}
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", containerID).Run()
	})

	addr := waitForRuntimeConcurrencyPostgresPort(t, containerID)
	dsn := fmt.Sprintf("postgres://postgres:postgres@%s/%s?sslmode=disable", addr, dbName)
	db := waitForRuntimeConcurrencyPostgresReady(t, containerID, dsn)
	t.Cleanup(func() {
		_ = db.Close()
	})

	applyRuntimeConcurrencyInitSQL(t, containerID, dbName)
	return db
}

func waitForRuntimeConcurrencyPostgresPort(t *testing.T, containerID string) string {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		out, err := exec.CommandContext(ctx, "docker", "port", containerID, "5432/tcp").CombinedOutput()
		cancel()
		if err == nil {
			if addr := normalizeRuntimeDockerAddr(string(out)); addr != "" {
				return addr
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("docker port mapping for %s never became available", containerID)
	return ""
}

func waitForRuntimeConcurrencyPostgresReady(t *testing.T, containerID, dsn string) *sql.DB {
	t.Helper()

	deadline := time.Now().Add(45 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			lastErr = err
			time.Sleep(250 * time.Millisecond)
			continue
		}

		pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = db.PingContext(pingCtx)
		cancel()
		if err == nil {
			return db
		}

		lastErr = err
		_ = db.Close()
		time.Sleep(250 * time.Millisecond)
	}

	logs, _ := exec.Command("docker", "logs", containerID).CombinedOutput()
	t.Fatalf("postgres container not ready: %v\nlogs:\n%s", lastErr, strings.TrimSpace(string(logs)))
	return nil
}

func applyRuntimeConcurrencyInitSQL(t *testing.T, containerID, dbName string) {
	t.Helper()

	initSQLPath := filepath.Join(concurrencyModuleRootDir(t), "..", "init.sql")
	initSQL, err := os.ReadFile(initSQLPath)
	if err != nil {
		t.Fatalf("read init.sql: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	cmd := exec.CommandContext(
		ctx,
		"docker",
		"exec",
		"-i",
		containerID,
		"psql",
		"-v", "ON_ERROR_STOP=1",
		"-U", "postgres",
		"-d", dbName,
	)
	cmd.Stdin = bytes.NewReader(initSQL)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("apply init.sql: %v\n%s", err, strings.TrimSpace(string(out)))
	}
}

func seedConcurrencyCounterRows(t *testing.T, db *sql.DB, hostnameHash, hostname, siteBucket, ipBucket string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO concurrency_host_counters (hostname_hash, hostname, active_count)
		VALUES ($1, $2, 0)
		ON CONFLICT (hostname_hash) DO NOTHING
	`, hostnameHash, hostname); err != nil {
		t.Fatalf("seed host counter: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO concurrency_site_counters (hostname_hash, site_bucket, active_count)
		VALUES ($1, $2, 0)
		ON CONFLICT (hostname_hash, site_bucket) DO NOTHING
	`, hostnameHash, siteBucket); err != nil {
		t.Fatalf("seed site counter: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO concurrency_site_ip_counters (hostname_hash, site_bucket, ip_bucket, active_count)
		VALUES ($1, $2, $3, 0)
		ON CONFLICT (hostname_hash, site_bucket, ip_bucket) DO NOTHING
	`, hostnameHash, siteBucket, ipBucket); err != nil {
		t.Fatalf("seed site_ip counter: %v", err)
	}
}

func lockCounterTuple(t *testing.T, db *sql.DB, hostnameHash, siteBucket, ipBucket string) *sql.Tx {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin lock tx: %v", err)
	}
	t.Cleanup(func() {
		_ = tx.Rollback()
	})
	for _, stmt := range []struct {
		query string
		args  []any
	}{
		{query: `SELECT 1 FROM concurrency_host_counters WHERE hostname_hash = $1 FOR UPDATE`, args: []any{hostnameHash}},
		{query: `SELECT 1 FROM concurrency_site_counters WHERE hostname_hash = $1 AND site_bucket = $2 FOR UPDATE`, args: []any{hostnameHash, siteBucket}},
		{query: `SELECT 1 FROM concurrency_site_ip_counters WHERE hostname_hash = $1 AND site_bucket = $2 AND ip_bucket = $3 FOR UPDATE`, args: []any{hostnameHash, siteBucket, ipBucket}},
	} {
		if _, err := tx.Exec(stmt.query, stmt.args...); err != nil {
			t.Fatalf("lock tuple counters: %v", err)
		}
	}
	return tx
}

func waitForRequestIDAdvisoryLock(t *testing.T, db *sql.DB, requestID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		var locked bool
		err := db.QueryRowContext(ctx, `
			SELECT EXISTS(
				SELECT 1
				FROM pg_locks
				WHERE locktype = 'advisory'
				  AND mode = 'ExclusiveLock'
				  AND granted
				  AND classid = 3
				  AND objid = hashtext($1)
			)
		`, requestID).Scan(&locked)
		if err == nil && locked {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("request_id advisory lock not observed for %q", requestID)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func holdRequestIDAdvisoryLock(t *testing.T, db *sql.DB, requestID string) (*sql.Conn, *sql.Tx) {
	t.Helper()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open advisory lock connection: %v", err)
	}

	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		_ = conn.Close()
		t.Fatalf("begin advisory lock transaction: %v", err)
	}

	if _, err := tx.ExecContext(context.Background(), `
		SELECT pg_advisory_xact_lock(3, hashtext($1))
	`, requestID); err != nil {
		_ = tx.Rollback()
		_ = conn.Close()
		t.Fatalf("hold advisory lock for request_id %q: %v", requestID, err)
	}

	waitForRequestIDAdvisoryLock(t, db, requestID)
	return conn, tx
}

type runtimeAcquireCall struct {
	HostnameHash      string
	Hostname          string
	SiteBucket        string
	IPBucket          string
	RequestID         string
	HardExpireMs      int64
	NowMs             int64
	WaitToken         string
	WaitPollWindowMs  int
	WaitReconnectMs   int
	HostMaxInFlight   int
	SiteMaxInFlight   int
	SiteIPMaxInFlight int
	CleanupLimit      int
}

type runtimeAcquireResult struct {
	Result      string
	LeaseID     string
	LeaseToken  string
	ExpiresAtMs int64
	WaitToken   sql.NullString
	Scope       sql.NullString
	Reason      sql.NullString
	RetryAfter  sql.NullInt64
	ClaimToken  sql.NullString
}

func seedRuntimeActiveLeases(t *testing.T, db *sql.DB, base runtimeAcquireCall, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		req := base
		req.RequestID = fmt.Sprintf("%s-%d", base.RequestID, i+1)
		req.NowMs = base.NowMs + int64(i)
		result, err := execRuntimeAcquire(context.Background(), db, req)
		if err != nil {
			t.Fatalf("seed active lease %d: %v", i+1, err)
		}
		if result.Result != "granted" {
			t.Fatalf("expected seed active lease %d granted, got %+v", i+1, result)
		}
	}
}

func execRuntimeAcquire(ctx context.Context, db *sql.DB, req runtimeAcquireCall) (*runtimeAcquireResult, error) {
	waitPollWindowMs := req.WaitPollWindowMs
	if waitPollWindowMs <= 0 {
		waitPollWindowMs = 10000
	}
	waitReconnectMs := req.WaitReconnectMs
	if waitReconnectMs <= 0 {
		waitReconnectMs = 1500
	}
	cleanupLimit := req.CleanupLimit
	if cleanupLimit <= 0 {
		cleanupLimit = 500
	}

	result := &runtimeAcquireResult{}
	err := db.QueryRowContext(ctx, `
		SELECT result,
		       COALESCE(lease_id::text, ''),
		       COALESCE(lease_token, ''),
		       COALESCE(expires_at_ms, 0),
		       wait_token,
		       scope,
		       reason,
		       retry_after,
		       claim_token
		FROM cq_acquire($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
	`,
		req.HostnameHash,
		req.Hostname,
		req.SiteBucket,
		req.IPBucket,
		req.RequestID,
		req.HardExpireMs,
		req.NowMs,
		req.WaitToken,
		waitPollWindowMs,
		waitReconnectMs,
		req.HostMaxInFlight,
		req.SiteMaxInFlight,
		req.SiteIPMaxInFlight,
		cleanupLimit,
	).Scan(
		&result.Result,
		&result.LeaseID,
		&result.LeaseToken,
		&result.ExpiresAtMs,
		&result.WaitToken,
		&result.Scope,
		&result.Reason,
		&result.RetryAfter,
		&result.ClaimToken,
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func execRuntimePromoteWaiting(ctx context.Context, db *sql.DB, req PromoteWaitingRequest, cfg Config) (*runtimeAcquireResult, error) {
	result := &runtimeAcquireResult{}
	err := db.QueryRowContext(ctx, `
		SELECT result,
		       COALESCE(lease_id::text, ''),
		       COALESCE(lease_token, ''),
		       COALESCE(expires_at_ms, 0),
		       wait_token,
		       scope,
		       reason,
		       retry_after,
		       claim_token
		FROM cq_promote_waiting_request($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`,
		req.RequestID,
		req.HostnameHash,
		req.SiteBucket,
		req.IPBucket,
		req.HardExpireAtMs,
		req.NowMs,
		cfg.Concurrency.Caps.HostMaxInFlight,
		cfg.Concurrency.Caps.SiteMaxInFlight,
		cfg.Concurrency.Caps.SiteIPMaxInFlight,
	).Scan(
		&result.Result,
		&result.LeaseID,
		&result.LeaseToken,
		&result.ExpiresAtMs,
		&result.WaitToken,
		&result.Scope,
		&result.Reason,
		&result.RetryAfter,
		&result.ClaimToken,
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func execRuntimeContinueWaitProbe(ctx context.Context, db *sql.DB, req AcquireRequest) (*runtimeAcquireResult, error) {
	result := &runtimeAcquireResult{}
	err := db.QueryRowContext(ctx, `
		SELECT result,
		       COALESCE(lease_id::text, ''),
		       COALESCE(lease_token, ''),
		       COALESCE(expires_at_ms, 0),
		       wait_token,
		       scope,
		       reason,
		       retry_after,
		       claim_token
		FROM cq_continue_wait_probe($1, $2, $3, $4, $5, $6, $7, $8)
	`,
		req.HostnameHash,
		req.Hostname,
		canonicalBucket(req.SiteBucket),
		canonicalBucket(req.IPBucket),
		req.RequestID,
		req.HardExpireAtMs,
		req.NowMs,
		strings.TrimSpace(req.WaitToken),
	).Scan(
		&result.Result,
		&result.LeaseID,
		&result.LeaseToken,
		&result.ExpiresAtMs,
		&result.WaitToken,
		&result.Scope,
		&result.Reason,
		&result.RetryAfter,
		&result.ClaimToken,
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func TestRuntimeAcquireReturnsExpiredWhenNowPastHardExpiry(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	result, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash: "expired-host",
		Hostname:     "expired.example.com",
		SiteBucket:   "site-a",
		IPBucket:     "ip-a",
		RequestID:    "past-hard-expiry",
		HardExpireMs: nowMs - 1,
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("expired acquire: %v", err)
	}
	if result.Result != "expired" || result.Reason.String != "hard_expired" {
		t.Fatalf("expected hard expired acquire result, got %+v", result)
	}

	var count int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM concurrency_requests
		WHERE request_id = $1
	`, "past-hard-expiry").Scan(&count); err != nil {
		t.Fatalf("count expired request rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected no request row for already expired acquire, got %d", count)
	}
}

func TestRuntimeAcquireCreatesWaitingRequestWithStableWaitToken(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	seeded, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "waiting-host",
		Hostname:          "waiting.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "waiting-seed",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	if seeded.Result != "granted" {
		t.Fatalf("expected seed acquire granted, got %+v", seeded)
	}

	result, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "waiting-host",
		Hostname:          "waiting.example.com",
		SiteBucket:        "site-b",
		IPBucket:          "ip-b",
		RequestID:         "waiting-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("waiting acquire: %v", err)
	}
	if result.Result != "wait" {
		t.Fatalf("expected wait result, got %+v", result)
	}
	if !result.WaitToken.Valid || strings.TrimSpace(result.WaitToken.String) == "" {
		t.Fatalf("expected stable wait token, got %+v", result)
	}
	if result.RetryAfter.Int64 <= 0 {
		t.Fatalf("expected retry_after > 0, got %+v", result)
	}

	var state, waitToken string
	var waiterLeaseUntilMs int64
	if err := db.QueryRowContext(context.Background(), `
		SELECT state, wait_token, waiter_lease_until_ms
		FROM concurrency_requests
		WHERE request_id = $1
	`, "waiting-request").Scan(&state, &waitToken, &waiterLeaseUntilMs); err != nil {
		t.Fatalf("read waiting request: %v", err)
	}
	if state != "waiting" {
		t.Fatalf("expected waiting request state, got %q", state)
	}
	if waitToken != result.WaitToken.String {
		t.Fatalf("expected stored wait token %q, got %q", result.WaitToken.String, waitToken)
	}
	if waiterLeaseUntilMs <= nowMs {
		t.Fatalf("expected waiter_lease_until_ms > now, got %d now=%d", waiterLeaseUntilMs, nowMs)
	}
}

func TestRuntimeAcquireGrantMustBeClaimedOnce(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "claim-host",
		Hostname:          "claim.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "claim-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	if grant.Result != "granted" || strings.TrimSpace(grant.LeaseToken) == "" || !grant.ClaimToken.Valid || strings.TrimSpace(grant.ClaimToken.String) == "" {
		t.Fatalf("expected granted acquire with lease and claim token, got %+v", grant)
	}

	replay, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "claim-host",
		Hostname:          "claim.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "claim-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs + 1,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("active replay before claim: %v", err)
	}
	if replay.Result != "conflict" || replay.Reason.String != "grant_unclaimed" || strings.TrimSpace(replay.LeaseToken) != "" {
		t.Fatalf("expected unclaimed conflict without lease token, got %+v", replay)
	}

	var claimResult string
	var claimLeaseID, claimLeaseToken, claimReason sql.NullString
	var claimExpires sql.NullInt64
	if err := db.QueryRowContext(context.Background(), `
		SELECT result, lease_id::text, lease_token, expires_at_ms, reason
		FROM cq_claim_grant($1, $2, $3)
	`, "claim-request", grant.ClaimToken.String, nowMs+2).Scan(&claimResult, &claimLeaseID, &claimLeaseToken, &claimExpires, &claimReason); err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	if claimResult != "granted" || claimLeaseID.String != grant.LeaseID || claimLeaseToken.String != grant.LeaseToken || claimExpires.Int64 != grant.ExpiresAtMs {
		t.Fatalf("expected one claim to return original lease identity, got result=%q lease=%q token=%q expires=%d", claimResult, claimLeaseID.String, claimLeaseToken.String, claimExpires.Int64)
	}

	var duplicateResult string
	var duplicateLeaseID, duplicateLeaseToken, duplicateReason sql.NullString
	var duplicateExpires sql.NullInt64
	if err := db.QueryRowContext(context.Background(), `
		SELECT result, lease_id::text, lease_token, expires_at_ms, reason
		FROM cq_claim_grant($1, $2, $3)
	`, "claim-request", grant.ClaimToken.String, nowMs+3).Scan(&duplicateResult, &duplicateLeaseID, &duplicateLeaseToken, &duplicateExpires, &duplicateReason); err != nil {
		t.Fatalf("duplicate claim grant: %v", err)
	}
	if duplicateResult != "granted" || duplicateLeaseID.String != grant.LeaseID || duplicateLeaseToken.String != grant.LeaseToken || duplicateExpires.Int64 != grant.ExpiresAtMs || duplicateReason.Valid {
		t.Fatalf("expected duplicate claim replay to return original lease identity, got result=%q lease=%q token=%q expires=%d reason=%q", duplicateResult, duplicateLeaseID.String, duplicateLeaseToken.String, duplicateExpires.Int64, duplicateReason.String)
	}

	replayAfterClaim, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash: "claim-host",
		Hostname:     "claim.example.com",
		SiteBucket:   "site-a",
		IPBucket:     "ip-a",
		RequestID:    "claim-request",
		HardExpireMs: nowMs + 120_000,
		NowMs:        nowMs + 4,
	})
	if err != nil {
		t.Fatalf("active replay after claim: %v", err)
	}
	if replayAfterClaim.Result != "conflict" || replayAfterClaim.Reason.String != "grant_already_claimed" || strings.TrimSpace(replayAfterClaim.LeaseToken) != "" {
		t.Fatalf("expected mismatched acquire replay to remain conflict after claim, got %+v", replayAfterClaim)
	}
}

func TestRuntimeAcquireReturnsExpiredBeforeReplayConflictForExpiredActiveRequest(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "active-expired-acquire-host",
		Hostname:          "active-expired-acquire.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "active-expired-acquire-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed active acquire: %v", err)
	}
	if grant.Result != "granted" {
		t.Fatalf("expected granted seed acquire, got %+v", grant)
	}

	pastMs := nowMs - 1_000
	if _, err := db.ExecContext(context.Background(), `
		UPDATE concurrency_leases
		SET hard_expire_at_ms = $2::bigint,
		    expires_at_ms = $2::bigint,
		    expires_at = to_timestamp(($2::bigint) / 1000.0)
		WHERE lease_id = $1::uuid
	`, grant.LeaseID, pastMs); err != nil {
		t.Fatalf("age active lease into expired state: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		UPDATE concurrency_requests
		SET lease_expires_at_ms = $2::bigint,
		    updated_at_ms = $2::bigint
		WHERE request_id = $1
	`, "active-expired-acquire-request", pastMs); err != nil {
		t.Fatalf("age active request into expired state: %v", err)
	}

	replay, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "active-expired-acquire-host",
		Hostname:          "active-expired-acquire.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "active-expired-acquire-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs + 10,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("replay expired active acquire: %v", err)
	}
	if replay.Result != "expired" || replay.Reason.String != "hard_expired" {
		t.Fatalf("expected expired replay before conflict, got %+v", replay)
	}
}

func TestRuntimeContinueWaitProbeReturnsExpiredBeforeReplayConflictForExpiredActiveRequest(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	busy, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "continue-wait-expired-host",
		Hostname:          "continue-wait-expired.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "continue-wait-expired-busy",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed busy lease: %v", err)
	}
	if busy.Result != "granted" {
		t.Fatalf("expected granted busy seed, got %+v", busy)
	}

	waiting, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "continue-wait-expired-host",
		Hostname:          "continue-wait-expired.example.com",
		SiteBucket:        "site-b",
		IPBucket:          "ip-b",
		RequestID:         "continue-wait-expired-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs + 1,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed waiting request: %v", err)
	}
	if waiting.Result != "wait" || !waiting.WaitToken.Valid {
		t.Fatalf("expected waiting acquire with token, got %+v", waiting)
	}

	var releaseResult string
	var releaseReason, releaseRequestID sql.NullString
	if err := db.QueryRowContext(context.Background(), `
		SELECT result, reason, request_id
		FROM cq_release($1::uuid, $2, $3, $4)
	`, busy.LeaseID, busy.LeaseToken, "stream_complete", nowMs+2).Scan(&releaseResult, &releaseReason, &releaseRequestID); err != nil {
		t.Fatalf("release busy lease: %v", err)
	}
	if releaseResult != "released" {
		t.Fatalf("expected busy lease release before promotion, got result=%q reason=%q", releaseResult, releaseReason.String)
	}

	promoted, err := execRuntimePromoteWaiting(context.Background(), db, PromoteWaitingRequest{
		RequestID:      "continue-wait-expired-request",
		HostnameHash:   "continue-wait-expired-host",
		SiteBucket:     "site-b",
		IPBucket:       "ip-b",
		HardExpireAtMs: nowMs + 120_000,
		NowMs:          nowMs + 3,
	}, validTestConfig())
	if err != nil {
		t.Fatalf("promote waiting request: %v", err)
	}
	if promoted.Result != "granted" {
		t.Fatalf("expected promoted request granted, got %+v", promoted)
	}

	pastMs := nowMs - 1_000
	if _, err := db.ExecContext(context.Background(), `
		UPDATE concurrency_leases
		SET hard_expire_at_ms = $2::bigint,
		    expires_at_ms = $2::bigint,
		    expires_at = to_timestamp(($2::bigint) / 1000.0)
		WHERE lease_id = $1::uuid
	`, promoted.LeaseID, pastMs); err != nil {
		t.Fatalf("age promoted lease into expired state: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		UPDATE concurrency_requests
		SET lease_expires_at_ms = $2::bigint,
		    updated_at_ms = $3::bigint
		WHERE request_id = $1
	`, "continue-wait-expired-request", pastMs, nowMs-500); err != nil {
		t.Fatalf("age promoted request into expired active state: %v", err)
	}

	probe, err := execRuntimeContinueWaitProbe(context.Background(), db, AcquireRequest{
		HostnameHash:   "continue-wait-expired-host",
		Hostname:       "continue-wait-expired.example.com",
		SiteBucket:     "site-b",
		IPBucket:       "ip-b",
		RequestID:      "continue-wait-expired-request",
		HardExpireAtMs: nowMs + 120_000,
		NowMs:          nowMs + 10,
		WaitToken:      waiting.WaitToken.String,
	})
	if err != nil {
		t.Fatalf("continue wait probe on expired active request: %v", err)
	}
	if probe.Result != "expired" || probe.Reason.String != "hard_expired" {
		t.Fatalf("expected continue wait probe expired before conflict, got %+v", probe)
	}
}

func TestRuntimePromoteWaitingRequestReturnsExpiredBeforeReplayConflictForExpiredActiveRequest(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	busy, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "promote-expired-host",
		Hostname:          "promote-expired.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "promote-expired-busy",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed busy lease: %v", err)
	}
	if busy.Result != "granted" {
		t.Fatalf("expected granted busy seed, got %+v", busy)
	}

	waiting, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "promote-expired-host",
		Hostname:          "promote-expired.example.com",
		SiteBucket:        "site-b",
		IPBucket:          "ip-b",
		RequestID:         "promote-expired-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs + 1,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed waiting request: %v", err)
	}
	if waiting.Result != "wait" {
		t.Fatalf("expected waiting seed request, got %+v", waiting)
	}

	var releaseResult string
	var releaseReason, releaseRequestID sql.NullString
	if err := db.QueryRowContext(context.Background(), `
		SELECT result, reason, request_id
		FROM cq_release($1::uuid, $2, $3, $4)
	`, busy.LeaseID, busy.LeaseToken, "stream_complete", nowMs+2).Scan(&releaseResult, &releaseReason, &releaseRequestID); err != nil {
		t.Fatalf("release busy lease: %v", err)
	}
	if releaseResult != "released" {
		t.Fatalf("expected release before first promotion, got result=%q reason=%q", releaseResult, releaseReason.String)
	}

	promoted, err := execRuntimePromoteWaiting(context.Background(), db, PromoteWaitingRequest{
		RequestID:      "promote-expired-request",
		HostnameHash:   "promote-expired-host",
		SiteBucket:     "site-b",
		IPBucket:       "ip-b",
		HardExpireAtMs: nowMs + 120_000,
		NowMs:          nowMs + 3,
	}, validTestConfig())
	if err != nil {
		t.Fatalf("first promote waiting request: %v", err)
	}
	if promoted.Result != "granted" {
		t.Fatalf("expected first promotion granted, got %+v", promoted)
	}

	pastMs := nowMs - 1_000
	if _, err := db.ExecContext(context.Background(), `
		UPDATE concurrency_leases
		SET hard_expire_at_ms = $2::bigint,
		    expires_at_ms = $2::bigint,
		    expires_at = to_timestamp(($2::bigint) / 1000.0)
		WHERE lease_id = $1::uuid
	`, promoted.LeaseID, pastMs); err != nil {
		t.Fatalf("age promoted lease into expired state: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		UPDATE concurrency_requests
		SET state = 'active',
		    lease_expires_at_ms = $2::bigint,
		    claim_state = 'claimed',
		    claim_claimed_at_ms = $3::bigint,
		    updated_at_ms = $3::bigint
		WHERE request_id = $1
	`, "promote-expired-request", pastMs, nowMs-500); err != nil {
		t.Fatalf("age promoted request into expired active state: %v", err)
	}

	result, err := execRuntimePromoteWaiting(context.Background(), db, PromoteWaitingRequest{
		RequestID:      "promote-expired-request",
		HostnameHash:   "promote-expired-host",
		SiteBucket:     "site-b",
		IPBucket:       "ip-b",
		HardExpireAtMs: nowMs + 120_000,
		NowMs:          nowMs + 10,
	}, validTestConfig())
	if err != nil {
		t.Fatalf("promote waiting request against expired active row: %v", err)
	}
	if result.Result != "expired" || result.Reason.String != "hard_expired" {
		t.Fatalf("expected promote expired before conflict, got %+v", result)
	}
}

func TestRuntimeDuplicateCleanupDoesNotDoubleDecrementCounters(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "duplicate-cleanup-host",
		Hostname:          "duplicate-cleanup.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "duplicate-cleanup-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed active lease: %v", err)
	}
	if grant.Result != "granted" {
		t.Fatalf("expected granted seed lease, got %+v", grant)
	}

	readCounters := func() (int, int, int) {
		t.Helper()
		var hostCount, siteCount, siteIPCount int
		if err := db.QueryRowContext(context.Background(), `
			SELECT active_count
			FROM concurrency_host_counters
			WHERE hostname_hash = $1
		`, "duplicate-cleanup-host").Scan(&hostCount); err != nil {
			t.Fatalf("read host counter: %v", err)
		}
		if err := db.QueryRowContext(context.Background(), `
			SELECT active_count
			FROM concurrency_site_counters
			WHERE hostname_hash = $1 AND site_bucket = $2
		`, "duplicate-cleanup-host", "site-a").Scan(&siteCount); err != nil {
			t.Fatalf("read site counter: %v", err)
		}
		if err := db.QueryRowContext(context.Background(), `
			SELECT active_count
			FROM concurrency_site_ip_counters
			WHERE hostname_hash = $1 AND site_bucket = $2 AND ip_bucket = $3
		`, "duplicate-cleanup-host", "site-a", "ip-a").Scan(&siteIPCount); err != nil {
			t.Fatalf("read site_ip counter: %v", err)
		}
		return hostCount, siteCount, siteIPCount
	}

	hostCount, siteCount, siteIPCount := readCounters()
	if hostCount != 1 || siteCount != 1 || siteIPCount != 1 {
		t.Fatalf("expected one active lease before cleanup, got host=%d site=%d site_ip=%d", hostCount, siteCount, siteIPCount)
	}

	var firstResult string
	var firstReason, firstRequestID sql.NullString
	if err := db.QueryRowContext(context.Background(), `
		SELECT result, reason, request_id
		FROM cq_release($1::uuid, $2, $3, $4)
	`, grant.LeaseID, grant.LeaseToken, "stream_complete", nowMs+1).Scan(&firstResult, &firstReason, &firstRequestID); err != nil {
		t.Fatalf("first release: %v", err)
	}
	if firstResult != "released" || !firstRequestID.Valid || firstRequestID.String != "duplicate-cleanup-request" {
		t.Fatalf("expected first release to release authoritative request, got result=%q reason=%q request=%q", firstResult, firstReason.String, firstRequestID.String)
	}

	hostCount, siteCount, siteIPCount = readCounters()
	if hostCount != 0 || siteCount != 0 || siteIPCount != 0 {
		t.Fatalf("expected first cleanup to decrement counters once, got host=%d site=%d site_ip=%d", hostCount, siteCount, siteIPCount)
	}

	var secondResult string
	var secondReason, secondRequestID sql.NullString
	if err := db.QueryRowContext(context.Background(), `
		SELECT result, reason, request_id
		FROM cq_release($1::uuid, $2, $3, $4)
	`, grant.LeaseID, grant.LeaseToken, "stream_complete", nowMs+2).Scan(&secondResult, &secondReason, &secondRequestID); err != nil {
		t.Fatalf("second release: %v", err)
	}
	if secondResult != "noop" || secondReason.String == "" {
		t.Fatalf("expected repeated cleanup to become idempotent noop, got result=%q reason=%q request=%q", secondResult, secondReason.String, secondRequestID.String)
	}

	hostCount, siteCount, siteIPCount = readCounters()
	if hostCount != 0 || siteCount != 0 || siteIPCount != 0 {
		t.Fatalf("expected duplicate cleanup to avoid double decrement, got host=%d site=%d site_ip=%d", hostCount, siteCount, siteIPCount)
	}
}

func TestRuntimeClaimGrantExpiresHardExpiredActiveLease(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "claim-expired-host",
		Hostname:          "claim-expired.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "claim-expired-request",
		HardExpireMs:      nowMs + 5,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	if grant.Result != "granted" || !grant.ClaimToken.Valid || strings.TrimSpace(grant.ClaimToken.String) == "" {
		t.Fatalf("expected granted acquire with claim token, got %+v", grant)
	}

	var claimResult string
	var claimLeaseID, claimLeaseToken, claimReason sql.NullString
	var claimExpires sql.NullInt64
	if err := db.QueryRowContext(context.Background(), `
		SELECT result, lease_id::text, lease_token, expires_at_ms, reason
		FROM cq_claim_grant($1, $2, $3)
	`, "claim-expired-request", grant.ClaimToken.String, nowMs+10).Scan(&claimResult, &claimLeaseID, &claimLeaseToken, &claimExpires, &claimReason); err != nil {
		t.Fatalf("claim expired grant: %v", err)
	}
	if claimResult != "expired" || claimReason.String != "hard_expired" || claimLeaseToken.Valid || claimLeaseID.Valid || claimExpires.Valid {
		t.Fatalf("expected expired claim without lease identity, got result=%q reason=%q leaseID=%q leaseToken=%q expires=%d", claimResult, claimReason.String, claimLeaseID.String, claimLeaseToken.String, claimExpires.Int64)
	}

	var requestState, terminalReason string
	if err := db.QueryRowContext(context.Background(), `
		SELECT state, COALESCE(terminal_reason, '')
		FROM concurrency_requests
		WHERE request_id = $1
	`, "claim-expired-request").Scan(&requestState, &terminalReason); err != nil {
		t.Fatalf("read expired claim request: %v", err)
	}
	if requestState != "expired" || terminalReason != "hard_expired" {
		t.Fatalf("expected claim to make request expired, got state=%q reason=%q", requestState, terminalReason)
	}

	var hostActiveCount int
	if err := db.QueryRowContext(context.Background(), `SELECT active_count FROM concurrency_host_counters WHERE hostname_hash = $1`, "claim-expired-host").Scan(&hostActiveCount); err != nil {
		t.Fatalf("read host counter: %v", err)
	}
	if hostActiveCount != 0 {
		t.Fatalf("expected expired claim to decrement active count, got %d", hostActiveCount)
	}
}

func TestRuntimeCancelAbsentRowCreatesCancelledTombstone(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	var cancelResult string
	var cancelReason sql.NullString
	if err := db.QueryRowContext(context.Background(), `
		SELECT result, reason
		FROM cq_cancel($1, $2, $3, $4, $5, $6, $7, $8)
	`, "cancelled-request", "cancel.example.com", "cancel-host", "site-a", "ip-a", nowMs+60_000, "worker_aborted", nowMs).Scan(&cancelResult, &cancelReason); err != nil {
		t.Fatalf("cancel absent row: %v", err)
	}
	if cancelResult != "cancelled" {
		t.Fatalf("expected cancelled result, got result=%q reason=%q", cancelResult, cancelReason.String)
	}

	var state, terminalReason, hostname string
	if err := db.QueryRowContext(context.Background(), `
		SELECT state, terminal_reason, hostname
		FROM concurrency_requests
		WHERE request_id = $1
	`, "cancelled-request").Scan(&state, &terminalReason, &hostname); err != nil {
		t.Fatalf("read cancelled tombstone: %v", err)
	}
	if state != "cancelled" || terminalReason != "request_cancelled" {
		t.Fatalf("expected cancelled tombstone, got state=%q terminal_reason=%q", state, terminalReason)
	}
	if hostname != "cancel.example.com" {
		t.Fatalf("expected cancelled tombstone hostname to persist actual host, got %q", hostname)
	}

	replay, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash: "cancel-host",
		Hostname:     "cancel.example.com",
		SiteBucket:   "site-a",
		IPBucket:     "ip-a",
		RequestID:    "cancelled-request",
		HardExpireMs: nowMs + 60_000,
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("replay acquire after cancel tombstone: %v", err)
	}
	if replay.Result != "cancelled" || replay.Reason.String != "request_cancelled" {
		t.Fatalf("expected cancelled replay, got %+v", replay)
	}
}

func TestAcquireFastExpiresStaleWaitingRowBeforeReplay(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO concurrency_requests (
			request_id, hostname_hash, hostname, site_bucket, ip_bucket, hard_expire_at_ms,
			state, wait_token, first_wait_at_ms, waiter_lease_until_ms, created_at_ms, updated_at_ms
		) VALUES ($1, $2, $3, $4, $5, $6, 'waiting', $7, $8, $9, $8, $8)
	`, "stale-waiting-request", "stale-host", "stale.example.com", "site-a", "ip-a", nowMs+60_000, "wait-stale", nowMs-10_000, nowMs-1_000); err != nil {
		t.Fatalf("seed stale waiting row: %v", err)
	}

	result, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash: "stale-host",
		Hostname:     "stale.example.com",
		SiteBucket:   "site-a",
		IPBucket:     "ip-a",
		RequestID:    "stale-waiting-request",
		HardExpireMs: nowMs + 60_000,
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("replay stale waiting row: %v", err)
	}
	if result.Result != "expired" || result.Reason.String != "waiter_detached_timeout" {
		t.Fatalf("expected expired waiter_detached_timeout replay, got %+v", result)
	}

	var state, terminalReason string
	if err := db.QueryRowContext(context.Background(), `
		SELECT state, terminal_reason
		FROM concurrency_requests
		WHERE request_id = $1
	`, "stale-waiting-request").Scan(&state, &terminalReason); err != nil {
		t.Fatalf("read expired waiting row: %v", err)
	}
	if state != "expired" || terminalReason != "waiter_detached_timeout" {
		t.Fatalf("expected expired waiting tombstone, got state=%q terminal_reason=%q", state, terminalReason)
	}
}

func TestExpireActiveLeaseMarksRequestExpiredAndDecrementsCounters(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	lease, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "startup-expire-host",
		Hostname:          "startup-expire.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "active-request",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed active lease: %v", err)
	}
	if lease.Result != "granted" {
		t.Fatalf("expected active lease grant, got %+v", lease)
	}

	expiredAtMs := nowMs - 1_000
	if _, err := db.ExecContext(context.Background(), `
		UPDATE concurrency_leases
		SET hard_expire_at_ms = $2::bigint,
		    expires_at_ms = $2::bigint,
		    expires_at = to_timestamp(($2::bigint) / 1000.0)
		WHERE lease_id = $1::uuid
	`, lease.LeaseID, expiredAtMs); err != nil {
		t.Fatalf("age active lease into expired state: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		UPDATE concurrency_requests
		SET hard_expire_at_ms = $2::bigint,
		    lease_expires_at_ms = $2::bigint,
		    updated_at_ms = $2::bigint
		WHERE request_id = $1
	`, "active-request", expiredAtMs); err != nil {
		t.Fatalf("age active request into expired state: %v", err)
	}

	var expiredRequestID string
	if err := db.QueryRowContext(context.Background(), `
		SELECT request_id
		FROM cq_expire_scope($1, $2, $3, $4, $5, $6)
	`, "host", "startup-expire-host", nil, nil, nowMs, 500).Scan(&expiredRequestID); err != nil {
		t.Fatalf("expire active lease scope: %v", err)
	}
	if expiredRequestID != "active-request" {
		t.Fatalf("expected expired active request id, got %q", expiredRequestID)
	}

	var leaseState string
	if err := db.QueryRowContext(context.Background(), `
		SELECT state
		FROM concurrency_leases
		WHERE lease_id = $1::uuid
	`, lease.LeaseID).Scan(&leaseState); err != nil {
		t.Fatalf("read expired lease state: %v", err)
	}
	if leaseState != "expired" {
		t.Fatalf("expected expired lease state, got %q", leaseState)
	}

	var requestState, terminalReason string
	if err := db.QueryRowContext(context.Background(), `
		SELECT state, terminal_reason
		FROM concurrency_requests
		WHERE request_id = $1
	`, "active-request").Scan(&requestState, &terminalReason); err != nil {
		t.Fatalf("read expired request state: %v", err)
	}
	if requestState != "expired" || terminalReason != "hard_expired" {
		t.Fatalf("expected expired request tombstone, got state=%q terminal_reason=%q", requestState, terminalReason)
	}

	var hostCount, siteCount, siteIPCount int
	if err := db.QueryRowContext(context.Background(), `
		SELECT active_count
		FROM concurrency_host_counters
		WHERE hostname_hash = $1
	`, "startup-expire-host").Scan(&hostCount); err != nil {
		t.Fatalf("read host counter: %v", err)
	}
	if err := db.QueryRowContext(context.Background(), `
		SELECT active_count
		FROM concurrency_site_counters
		WHERE hostname_hash = $1 AND site_bucket = $2
	`, "startup-expire-host", "site-a").Scan(&siteCount); err != nil {
		t.Fatalf("read site counter: %v", err)
	}
	if err := db.QueryRowContext(context.Background(), `
		SELECT active_count
		FROM concurrency_site_ip_counters
		WHERE hostname_hash = $1 AND site_bucket = $2 AND ip_bucket = $3
	`, "startup-expire-host", "site-a", "ip-a").Scan(&siteIPCount); err != nil {
		t.Fatalf("read site_ip counter: %v", err)
	}
	if hostCount != 0 || siteCount != 0 || siteIPCount != 0 {
		t.Fatalf("expected counters decremented after active expiry, got host=%d site=%d site_ip=%d", hostCount, siteCount, siteIPCount)
	}
}

func TestRuntimeExpireScopeSkipsBusyRequestScopedExpiryUntilRequestLockReleases(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	requestID := "expire-scope-busy-request"
	hostnameHash := "expire-scope-busy-host"

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      hostnameHash,
		Hostname:          "expire-scope-busy.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         requestID,
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed active lease: %v", err)
	}
	if grant.Result != "granted" {
		t.Fatalf("expected granted seed lease, got %+v", grant)
	}

	pastMs := nowMs - 1_000
	if _, err := db.ExecContext(context.Background(), `
		UPDATE concurrency_leases
		SET hard_expire_at_ms = $2::bigint,
		    expires_at_ms = $2::bigint,
		    expires_at = to_timestamp(($2::bigint) / 1000.0)
		WHERE lease_id = $1::uuid
	`, grant.LeaseID, pastMs); err != nil {
		t.Fatalf("age addressed lease into expired state: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		UPDATE concurrency_requests
		SET hard_expire_at_ms = $2::bigint,
		    lease_expires_at_ms = $2::bigint,
		    updated_at_ms = $2::bigint
		WHERE request_id = $1
	`, requestID, pastMs); err != nil {
		t.Fatalf("age authoritative request into expired state: %v", err)
	}

	requestConn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open request lock connection: %v", err)
	}
	defer requestConn.Close()

	requestTx, err := requestConn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin request lock transaction: %v", err)
	}
	defer requestTx.Rollback()

	if _, err := requestTx.ExecContext(context.Background(), `
		SELECT pg_advisory_xact_lock(3, hashtext($1))
	`, requestID); err != nil {
		t.Fatalf("hold authoritative request advisory lock: %v", err)
	}

	expireConn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open expire scope connection: %v", err)
	}
	defer expireConn.Close()

	expireTx, err := expireConn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin expire scope transaction: %v", err)
	}
	defer expireTx.Rollback()

	if _, err := expireTx.ExecContext(context.Background(), `SET LOCAL statement_timeout = '250ms'`); err != nil {
		t.Fatalf("set statement timeout: %v", err)
	}

	rows, err := expireTx.QueryContext(context.Background(), `
		SELECT request_id
		FROM cq_expire_scope($1, $2, $3, $4, $5, $6)
	`, "host", hostnameHash, nil, nil, nowMs+10, 500)
	if err != nil {
		t.Fatalf("expire scope while authoritative request lock busy: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var expiredRequestID string
		if err := rows.Scan(&expiredRequestID); err != nil {
			t.Fatalf("scan skipped expire scope row: %v", err)
		}
		t.Fatalf("expected busy request-scoped expiry to skip locked request, got %q", expiredRequestID)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate expire scope rows while lock busy: %v", err)
	}
	if err := expireTx.Commit(); err != nil {
		t.Fatalf("commit skipped expire scope transaction: %v", err)
	}

	var lockedState string
	if err := db.QueryRowContext(context.Background(), `
		SELECT state
		FROM concurrency_requests
		WHERE request_id = $1
	`, requestID).Scan(&lockedState); err != nil {
		t.Fatalf("read request state while advisory lock held: %v", err)
	}
	if lockedState != "active" {
		t.Fatalf("expected busy request to remain active until advisory lock releases, got %q", lockedState)
	}

	if err := requestTx.Rollback(); err != nil {
		t.Fatalf("release authoritative request advisory lock: %v", err)
	}

	var expiredRequestID string
	if err := db.QueryRowContext(context.Background(), `
		SELECT request_id
		FROM cq_expire_scope($1, $2, $3, $4, $5, $6)
	`, "host", hostnameHash, nil, nil, nowMs+11, 500).Scan(&expiredRequestID); err != nil {
		t.Fatalf("expire scope after advisory lock release: %v", err)
	}
	if expiredRequestID != requestID {
		t.Fatalf("expected expire scope to process request after advisory lock release, got %q", expiredRequestID)
	}
}

func TestRuntimePromoteWaitingRequestGrantsWhenCapacityFrees(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	busy, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "promote-host",
		Hostname:          "promote.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "busy-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed busy lease: %v", err)
	}
	if busy.Result != "granted" {
		t.Fatalf("expected busy lease granted, got %+v", busy)
	}

	waiting, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "promote-host",
		Hostname:          "promote.example.com",
		SiteBucket:        "site-b",
		IPBucket:          "ip-b",
		RequestID:         "waiting-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed waiting request: %v", err)
	}
	if waiting.Result != "wait" {
		t.Fatalf("expected waiting request, got %+v", waiting)
	}

	var releaseResult string
	var releaseReason sql.NullString
	var releaseRequestID sql.NullString
	if err := db.QueryRowContext(context.Background(), `
		SELECT result, reason, request_id
		FROM cq_release($1::uuid, $2, $3, $4)
	`, busy.LeaseID, busy.LeaseToken, "stream_complete", nowMs+1).Scan(&releaseResult, &releaseReason, &releaseRequestID); err != nil {
		t.Fatalf("release busy lease: %v", err)
	}
	if releaseResult != "released" {
		t.Fatalf("expected release result released, got result=%q reason=%q", releaseResult, releaseReason.String)
	}

	result, err := execRuntimePromoteWaiting(context.Background(), db, PromoteWaitingRequest{
		RequestID:      "waiting-request",
		HostnameHash:   "promote-host",
		SiteBucket:     "site-b",
		IPBucket:       "ip-b",
		HardExpireAtMs: nowMs + 120_000,
		NowMs:          nowMs + 2,
	}, validTestConfig())
	if err != nil {
		t.Fatalf("promote waiting request: %v", err)
	}
	if result.Result != "granted" || strings.TrimSpace(result.LeaseID) == "" || strings.TrimSpace(result.LeaseToken) == "" {
		t.Fatalf("expected granted promote result, got %+v", result)
	}

	var requestState string
	var leaseID string
	if err := db.QueryRowContext(context.Background(), `
		SELECT state, COALESCE(lease_id::text, '')
		FROM concurrency_requests
		WHERE request_id = $1
	`, "waiting-request").Scan(&requestState, &leaseID); err != nil {
		t.Fatalf("read promoted request: %v", err)
	}
	if requestState != "active" || strings.TrimSpace(leaseID) == "" {
		t.Fatalf("expected waiting request promoted to active with lease, got state=%q leaseID=%q", requestState, leaseID)
	}
}

func TestPromoteWaitingZeroCapsUseFirstDenyingEnabledLayer(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)

	for _, tc := range []struct {
		name          string
		waitingSite   string
		waitingIP     string
		hostCap       int
		siteCap       int
		siteIPCap     int
		wantWaitScope string
	}{
		{
			name:          "host cap disabled promotes site denial",
			waitingSite:   "site-a",
			waitingIP:     "ip-b",
			hostCap:       0,
			siteCap:       1,
			siteIPCap:     2,
			wantWaitScope: "site",
		},
		{
			name:          "site cap disabled keeps host denial first",
			waitingSite:   "site-b",
			waitingIP:     "ip-b",
			hostCap:       1,
			siteCap:       0,
			siteIPCap:     2,
			wantWaitScope: "host",
		},
		{
			name:          "host cap disabled promotes site ip denial",
			waitingSite:   "site-a",
			waitingIP:     "ip-a",
			hostCap:       0,
			siteCap:       2,
			siteIPCap:     1,
			wantWaitScope: "site_ip",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nowMs := time.Now().UnixMilli()
			nameSlug := strings.ReplaceAll(tc.name, " ", "-")
			hostnameHash := "promote-zero-caps-" + nameSlug
			hostname := hostnameHash + ".example.com"

			busy, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
				HostnameHash:      hostnameHash,
				Hostname:          hostname,
				SiteBucket:        "site-a",
				IPBucket:          "ip-a",
				RequestID:         hostnameHash + "-busy",
				HardExpireMs:      nowMs + 120_000,
				NowMs:             nowMs,
				HostMaxInFlight:   64,
				SiteMaxInFlight:   32,
				SiteIPMaxInFlight: 4,
			})
			if err != nil {
				t.Fatalf("seed busy lease: %v", err)
			}
			if busy.Result != "granted" {
				t.Fatalf("expected busy lease granted, got %+v", busy)
			}

			waitingRequestID := hostnameHash + "-waiting"
			waiting, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
				HostnameHash:      hostnameHash,
				Hostname:          hostname,
				SiteBucket:        tc.waitingSite,
				IPBucket:          tc.waitingIP,
				RequestID:         waitingRequestID,
				HardExpireMs:      nowMs + 120_000,
				NowMs:             nowMs + 1,
				HostMaxInFlight:   1,
				SiteMaxInFlight:   1,
				SiteIPMaxInFlight: 1,
			})
			if err != nil {
				t.Fatalf("seed waiting request: %v", err)
			}
			if waiting.Result != "wait" {
				t.Fatalf("expected waiting request, got %+v", waiting)
			}

			cfg := validTestConfig()
			cfg.Concurrency.Caps.HostMaxInFlight = tc.hostCap
			cfg.Concurrency.Caps.SiteMaxInFlight = tc.siteCap
			cfg.Concurrency.Caps.SiteIPMaxInFlight = tc.siteIPCap

			result, err := execRuntimePromoteWaiting(context.Background(), db, PromoteWaitingRequest{
				RequestID:      waitingRequestID,
				HostnameHash:   hostnameHash,
				SiteBucket:     tc.waitingSite,
				IPBucket:       tc.waitingIP,
				HardExpireAtMs: nowMs + 120_000,
				NowMs:          nowMs + 2,
			}, cfg)
			if err != nil {
				t.Fatalf("promote waiting request: %v", err)
			}
			if result.Result != "wait" {
				t.Fatalf("expected wait, got %+v", result)
			}
			if !result.Scope.Valid || result.Scope.String != tc.wantWaitScope {
				t.Fatalf("expected scope=%s, got %+v", tc.wantWaitScope, result)
			}
		})
	}
}

func TestRuntimeAcquireZeroCapsReachSQLUnchanged(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	hostnameHash := "runtime-zero-caps"
	hostname := hostnameHash + ".example.com"

	seedRuntimeActiveLeases(t, db, runtimeAcquireCall{
		HostnameHash:      hostnameHash,
		Hostname:          hostname,
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         hostnameHash + "-busy",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   64,
		SiteMaxInFlight:   32,
		SiteIPMaxInFlight: 4,
	}, 4)

	result, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      hostnameHash,
		Hostname:          hostname,
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         hostnameHash + "-next",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs + 10,
		HostMaxInFlight:   0,
		SiteMaxInFlight:   0,
		SiteIPMaxInFlight: 0,
	})
	if err != nil {
		t.Fatalf("runtime acquire with explicit zero caps: %v", err)
	}
	if result.Result == "wait" {
		t.Fatalf("expected explicit zero caps to reach SQL unchanged, got %+v", result)
	}
}

func TestRuntimeAcquireCleansExpiredCapacityUsingRequestClock(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	seeded, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash: "cleanup-host",
		Hostname:     "cleanup.example.com",
		SiteBucket:   "site-a",
		IPBucket:     "ip-a",
		RequestID:    "stale-capacity-seed",
		HardExpireMs: nowMs + 60_000,
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	if seeded.Result != "granted" {
		t.Fatalf("expected seed acquire granted, got %+v", seeded)
	}

	pastMs := nowMs - 1_000
	if _, err := db.ExecContext(context.Background(), `
		UPDATE concurrency_leases
		SET hard_expire_at_ms = $2::bigint,
		    expires_at_ms = $2::bigint,
		    expires_at = to_timestamp(($2::bigint) / 1000.0)
		WHERE lease_id = $1::uuid
	`, seeded.LeaseID, pastMs); err != nil {
		t.Fatalf("age seeded lease into expired state: %v", err)
	}

	result, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "cleanup-host",
		Hostname:          "cleanup.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "stale-capacity-next",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("expected cleanup acquire granted, got err=%v", err)
	}
	if result.Result != "granted" {
		t.Fatalf("expected cleanup acquire granted, got %+v", result)
	}

	var state string
	if err := db.QueryRowContext(context.Background(), `
		SELECT state
		FROM concurrency_leases
		WHERE request_id = $1
	`, "stale-capacity-seed").Scan(&state); err != nil {
		t.Fatalf("lookup seeded lease state: %v", err)
	}
	if state != "expired" {
		t.Fatalf("expected stale lease to expire during acquire cleanup, got %q", state)
	}

	var hostCount, siteCount, siteIPCount int
	if err := db.QueryRowContext(context.Background(), `
		SELECT active_count
		FROM concurrency_host_counters
		WHERE hostname_hash = $1
	`, "cleanup-host").Scan(&hostCount); err != nil {
		t.Fatalf("read host counter: %v", err)
	}
	if err := db.QueryRowContext(context.Background(), `
		SELECT active_count
		FROM concurrency_site_counters
		WHERE hostname_hash = $1 AND site_bucket = $2
	`, "cleanup-host", "site-a").Scan(&siteCount); err != nil {
		t.Fatalf("read site counter: %v", err)
	}
	if err := db.QueryRowContext(context.Background(), `
		SELECT active_count
		FROM concurrency_site_ip_counters
		WHERE hostname_hash = $1 AND site_bucket = $2 AND ip_bucket = $3
	`, "cleanup-host", "site-a", "ip-a").Scan(&siteIPCount); err != nil {
		t.Fatalf("read site_ip counter: %v", err)
	}
	if hostCount != 1 || siteCount != 1 || siteIPCount != 1 {
		t.Fatalf("expected one active lease after cleanup acquire, got host=%d site=%d site_ip=%d", hostCount, siteCount, siteIPCount)
	}
}

func TestRuntimeReleaseOnlyMutatesAuthoritativeRequestLifecycle(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	staleLease, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash: "release-host",
		Hostname:     "release.example.com",
		SiteBucket:   "site-a",
		IPBucket:     "ip-a",
		RequestID:    "release-stale-lease",
		HardExpireMs: nowMs + 60_000,
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("seed stale lease: %v", err)
	}
	releaseLease, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash: "release-host",
		Hostname:     "release.example.com",
		SiteBucket:   "site-b",
		IPBucket:     "ip-b",
		RequestID:    "release-live-lease",
		HardExpireMs: nowMs + 120_000,
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("seed release lease: %v", err)
	}

	pastMs := nowMs - 1_000
	if _, err := db.ExecContext(context.Background(), `
		UPDATE concurrency_leases
		SET hard_expire_at_ms = $2::bigint,
		    expires_at_ms = $2::bigint,
		    expires_at = to_timestamp(($2::bigint) / 1000.0)
		WHERE lease_id = $1::uuid
	`, staleLease.LeaseID, pastMs); err != nil {
		t.Fatalf("age stale lease: %v", err)
	}

	var releaseResult string
	var releaseReason sql.NullString
	var releaseRequestID sql.NullString
	if err := db.QueryRowContext(context.Background(), `
		SELECT result, reason, request_id
		FROM cq_release($1::uuid, $2, $3, $4)
	`, releaseLease.LeaseID, releaseLease.LeaseToken, "stream_complete", nowMs).Scan(&releaseResult, &releaseReason, &releaseRequestID); err != nil {
		t.Fatalf("release active lease: %v", err)
	}
	if releaseResult != "released" {
		t.Fatalf("expected release result released, got result=%q reason=%q", releaseResult, releaseReason.String)
	}

	var staleState, releasedState string
	if err := db.QueryRowContext(context.Background(), `SELECT state FROM concurrency_leases WHERE lease_id = $1::uuid`, staleLease.LeaseID).Scan(&staleState); err != nil {
		t.Fatalf("read stale lease state: %v", err)
	}
	if err := db.QueryRowContext(context.Background(), `SELECT state FROM concurrency_leases WHERE lease_id = $1::uuid`, releaseLease.LeaseID).Scan(&releasedState); err != nil {
		t.Fatalf("read released lease state: %v", err)
	}
	if staleState != "active" {
		t.Fatalf("expected unrelated stale lease to remain outside release authority, got %q", staleState)
	}
	if releasedState != "released" {
		t.Fatalf("expected addressed lease released, got %q", releasedState)
	}

	var hostCount int
	if err := db.QueryRowContext(context.Background(), `
		SELECT active_count
		FROM concurrency_host_counters
		WHERE hostname_hash = $1
	`, "release-host").Scan(&hostCount); err != nil {
		t.Fatalf("read host counter: %v", err)
	}
	if hostCount != 1 {
		t.Fatalf("expected host counter to reflect one remaining active request, got %d", hostCount)
	}
}

func TestRuntimeReleaseMarksRequestReleasedWithoutCreatingNewLease(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	lease, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash: "release-request-host",
		Hostname:     "release-request.example.com",
		SiteBucket:   "site-a",
		IPBucket:     "ip-a",
		RequestID:    "release-request",
		HardExpireMs: nowMs + 60_000,
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	if lease.Result != "granted" {
		t.Fatalf("expected granted seed lease, got %+v", lease)
	}

	var releaseResult string
	var releaseReason sql.NullString
	var releaseRequestID sql.NullString
	if err := db.QueryRowContext(context.Background(), `
		SELECT result, reason, request_id
		FROM cq_release($1::uuid, $2, $3, $4)
	`, lease.LeaseID, lease.LeaseToken, "stream_complete", int64(1)).Scan(&releaseResult, &releaseReason, &releaseRequestID); err != nil {
		t.Fatalf("release active lease: %v", err)
	}
	if releaseResult != "released" {
		t.Fatalf("expected release result released, got result=%q reason=%q", releaseResult, releaseReason.String)
	}

	var leaseCount int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM concurrency_leases WHERE request_id = $1`, "release-request").Scan(&leaseCount); err != nil {
		t.Fatalf("count request leases: %v", err)
	}
	if leaseCount != 1 {
		t.Fatalf("expected release to avoid creating new leases, got %d rows", leaseCount)
	}

	var leaseState string
	if err := db.QueryRowContext(context.Background(), `SELECT state FROM concurrency_leases WHERE lease_id = $1::uuid`, lease.LeaseID).Scan(&leaseState); err != nil {
		t.Fatalf("read released lease state: %v", err)
	}
	if leaseState != "released" {
		t.Fatalf("expected released lease state, got %q", leaseState)
	}

	var requestState, terminalReason string
	if err := db.QueryRowContext(context.Background(), `
		SELECT state, terminal_reason
		FROM concurrency_requests
		WHERE request_id = $1
	`, "release-request").Scan(&requestState, &terminalReason); err != nil {
		t.Fatalf("read released request state: %v", err)
	}
	if requestState != "released" || terminalReason != "already_released" {
		t.Fatalf("expected released request tombstone, got state=%q terminal_reason=%q", requestState, terminalReason)
	}
}

func TestRuntimeReleaseReturnsExpiredWhenAddressedActiveLeaseIsAlreadyExpired(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	lease, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash: "release-expired-host",
		Hostname:     "release-expired.example.com",
		SiteBucket:   "site-a",
		IPBucket:     "ip-a",
		RequestID:    "release-expired-request",
		HardExpireMs: nowMs + 60_000,
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	if lease.Result != "granted" {
		t.Fatalf("expected granted seed lease, got %+v", lease)
	}

	pastMs := nowMs - 1_000
	if _, err := db.ExecContext(context.Background(), `
		UPDATE concurrency_leases
		SET hard_expire_at_ms = $2::bigint,
		    expires_at_ms = $2::bigint,
		    expires_at = to_timestamp(($2::bigint) / 1000.0)
		WHERE lease_id = $1::uuid
	`, lease.LeaseID, pastMs); err != nil {
		t.Fatalf("age addressed lease into expired state: %v", err)
	}

	var releaseResult string
	var releaseReason sql.NullString
	var releaseRequestID sql.NullString
	if err := db.QueryRowContext(context.Background(), `
		SELECT result, reason, request_id
		FROM cq_release($1::uuid, $2, $3, $4)
	`, lease.LeaseID, lease.LeaseToken, "stream_complete", nowMs+1).Scan(&releaseResult, &releaseReason, &releaseRequestID); err != nil {
		t.Fatalf("release expired addressed lease: %v", err)
	}
	if releaseResult != "expired" || releaseReason.String != "hard_expired" || releaseRequestID.String != "release-expired-request" {
		t.Fatalf("expected release to return expired authoritative outcome, got result=%q reason=%q request=%q", releaseResult, releaseReason.String, releaseRequestID.String)
	}

	var requestState, terminalReason string
	if err := db.QueryRowContext(context.Background(), `
		SELECT state, terminal_reason
		FROM concurrency_requests
		WHERE request_id = $1
	`, "release-expired-request").Scan(&requestState, &terminalReason); err != nil {
		t.Fatalf("read expired release request state: %v", err)
	}
	if requestState != "expired" || terminalReason != "hard_expired" {
		t.Fatalf("expected release to terminalize request as expired, got state=%q terminal_reason=%q", requestState, terminalReason)
	}
}

func TestRuntimeReleaseReturnsExpiredBeforeWrongTokenConflictForExpiredActiveLease(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	lease, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash: "release-wrong-token-expired-host",
		Hostname:     "release-wrong-token-expired.example.com",
		SiteBucket:   "site-a",
		IPBucket:     "ip-a",
		RequestID:    "release-wrong-token-expired-request",
		HardExpireMs: nowMs + 60_000,
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	if lease.Result != "granted" {
		t.Fatalf("expected granted seed lease, got %+v", lease)
	}

	pastMs := nowMs - 1_000
	if _, err := db.ExecContext(context.Background(), `
		UPDATE concurrency_leases
		SET hard_expire_at_ms = $2::bigint,
		    expires_at_ms = $2::bigint,
		    expires_at = to_timestamp(($2::bigint) / 1000.0)
		WHERE lease_id = $1::uuid
	`, lease.LeaseID, pastMs); err != nil {
		t.Fatalf("age addressed lease into expired state: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		UPDATE concurrency_requests
		SET hard_expire_at_ms = $2::bigint,
		    lease_expires_at_ms = $2::bigint,
		    updated_at_ms = $2::bigint
		WHERE request_id = $1
	`, "release-wrong-token-expired-request", pastMs); err != nil {
		t.Fatalf("age authoritative request into expired state: %v", err)
	}

	var releaseResult string
	var releaseReason sql.NullString
	var releaseRequestID sql.NullString
	if err := db.QueryRowContext(context.Background(), `
		SELECT result, reason, request_id
		FROM cq_release($1::uuid, $2, $3, $4)
	`, lease.LeaseID, "wrong-lease-token", "stream_complete", nowMs+1).Scan(&releaseResult, &releaseReason, &releaseRequestID); err != nil {
		t.Fatalf("release expired addressed lease with wrong token: %v", err)
	}
	if releaseResult != "expired" || releaseReason.String != "hard_expired" || releaseRequestID.String != "release-wrong-token-expired-request" {
		t.Fatalf("expected release expiry precedence before wrong-token conflict, got result=%q reason=%q request=%q", releaseResult, releaseReason.String, releaseRequestID.String)
	}
}

func TestRuntimeCompensationReleaseReasonsPersist(t *testing.T) {
	for _, reason := range []string{releaseReasonGrantDeliveryFailed, releaseReasonAcquireDeliveryFailed} {
		t.Run(reason, func(t *testing.T) {
			db := requireRuntimeConcurrencyDB(t)
			nowMs := time.Now().UnixMilli()
			lease, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
				HostnameHash:      "compensate-host-" + reason,
				Hostname:          "compensate.example.com",
				SiteBucket:        "site-a",
				IPBucket:          "ip-a",
				RequestID:         "compensate-request-" + reason,
				HardExpireMs:      nowMs + 60_000,
				NowMs:             nowMs,
				HostMaxInFlight:   1,
				SiteMaxInFlight:   1,
				SiteIPMaxInFlight: 1,
			})
			if err != nil {
				t.Fatalf("seed acquire: %v", err)
			}
			if lease.Result != "granted" {
				t.Fatalf("expected granted seed lease, got %+v", lease)
			}
			var releaseResult string
			var releaseReason, releaseRequestID sql.NullString
			if err := db.QueryRowContext(context.Background(), `
				SELECT result, reason, request_id
				FROM cq_release($1::uuid, $2, $3, $4)
			`, lease.LeaseID, lease.LeaseToken, reason, nowMs+1).Scan(&releaseResult, &releaseReason, &releaseRequestID); err != nil {
				t.Fatalf("release compensation: %v", err)
			}
			if releaseResult != "released" {
				t.Fatalf("expected released compensation, got result=%q reason=%q", releaseResult, releaseReason.String)
			}
			var requestState, terminalReason string
			if err := db.QueryRowContext(context.Background(), `
				SELECT state, terminal_reason
				FROM concurrency_requests
				WHERE request_id = $1
			`, "compensate-request-"+reason).Scan(&requestState, &terminalReason); err != nil {
				t.Fatalf("read compensated request: %v", err)
			}
			if requestState != "released" || terminalReason != reason {
				t.Fatalf("expected compensation reason persisted, got state=%q reason=%q", requestState, terminalReason)
			}
			if terminalReason == "stream_complete" || terminalReason == "already_released" {
				t.Fatalf("compensation reason collapsed to %q", terminalReason)
			}
		})
	}
}

func TestRuntimeAcquireSerializesRequestIDAcrossTuplesAndRejectsConflictingReuse(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	seedConcurrencyCounterRows(t, db, "host-a", "a.example.com", "site-a", "ip-a")
	seedConcurrencyCounterRows(t, db, "host-b", "b.example.com", "site-b", "ip-b")

	lockTx := lockCounterTuple(t, db, "host-a", "site-a", "ip-a")
	defer func() {
		_ = lockTx.Rollback()
	}()

	requestID := "shared-request-id"
	nowMs := time.Now().UnixMilli()
	hardExpireMs := nowMs + 60_000

	type acquireOutcome struct {
		result *runtimeAcquireResult
		err    error
	}

	var wg sync.WaitGroup
	firstCh := make(chan acquireOutcome, 1)
	secondCh := make(chan acquireOutcome, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		result, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
			HostnameHash: "host-a",
			Hostname:     "a.example.com",
			SiteBucket:   "site-a",
			IPBucket:     "ip-a",
			RequestID:    requestID,
			HardExpireMs: hardExpireMs,
			NowMs:        nowMs,
		})
		firstCh <- acquireOutcome{result: result, err: err}
	}()

	waitForRequestIDAdvisoryLock(t, db, requestID)

	wg.Add(1)
	go func() {
		defer wg.Done()
		result, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
			HostnameHash: "host-b",
			Hostname:     "b.example.com",
			SiteBucket:   "site-b",
			IPBucket:     "ip-b",
			RequestID:    requestID,
			HardExpireMs: hardExpireMs,
			NowMs:        nowMs,
		})
		secondCh <- acquireOutcome{result: result, err: err}
	}()

	select {
	case outcome := <-secondCh:
		t.Fatalf("expected second acquire to wait on request_id serialization, got early result=%+v err=%v", outcome.result, outcome.err)
	case <-time.After(200 * time.Millisecond):
	}

	if err := lockTx.Commit(); err != nil {
		t.Fatalf("commit lock tx: %v", err)
	}

	first := <-firstCh
	if first.err != nil {
		t.Fatalf("expected first acquire granted, got err=%v", first.err)
	}
	if first.result == nil || first.result.Result != "granted" {
		t.Fatalf("expected first acquire granted, got %+v", first.result)
	}

	second := <-secondCh
	if second.err == nil {
		t.Fatalf("expected second acquire tuple mismatch error, got result=%+v", second.result)
	}
	if !strings.Contains(second.err.Error(), "cq_acquire request_id tuple mismatch") {
		t.Fatalf("expected deterministic tuple mismatch, got %v", second.err)
	}

	wg.Wait()
	var leaseCount int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM concurrency_leases WHERE request_id = $1`, requestID).Scan(&leaseCount); err != nil {
		t.Fatalf("count request_id leases: %v", err)
	}
	if leaseCount != 1 {
		t.Fatalf("expected one lease row for request_id, got %d", leaseCount)
	}
}

func TestRuntimeAcquireWaitTokenReconnectLocksAuthoritativeRequestID(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	hardExpireMs := nowMs + 120_000
	authoritativeRequestID := "acquire-wait-lock-authoritative"
	fakeRequestID := "acquire-wait-lock-fake"

	busy, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "acquire-wait-lock-host",
		Hostname:          "acquire-wait-lock.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "acquire-wait-lock-busy",
		HardExpireMs:      hardExpireMs,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed busy lease: %v", err)
	}
	if busy.Result != "granted" {
		t.Fatalf("expected granted busy seed, got %+v", busy)
	}

	waiting, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "acquire-wait-lock-host",
		Hostname:          "acquire-wait-lock.example.com",
		SiteBucket:        "site-b",
		IPBucket:          "ip-b",
		RequestID:         authoritativeRequestID,
		HardExpireMs:      hardExpireMs,
		NowMs:             nowMs + 1,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed waiting request: %v", err)
	}
	if waiting.Result != "wait" || !waiting.WaitToken.Valid {
		t.Fatalf("expected waiting request with token, got %+v", waiting)
	}

	type acquireOutcome struct {
		result *runtimeAcquireResult
		err    error
	}

	reconnect := func(requestID string) <-chan acquireOutcome {
		ch := make(chan acquireOutcome, 1)
		go func() {
			result, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
				HostnameHash:      "acquire-wait-lock-host",
				Hostname:          "acquire-wait-lock.example.com",
				SiteBucket:        "site-b",
				IPBucket:          "ip-b",
				RequestID:         requestID,
				HardExpireMs:      hardExpireMs,
				NowMs:             nowMs + 2,
				WaitToken:         waiting.WaitToken.String,
				HostMaxInFlight:   1,
				SiteMaxInFlight:   1,
				SiteIPMaxInFlight: 1,
			})
			ch <- acquireOutcome{result: result, err: err}
		}()
		return ch
	}

	fakeConn, fakeTx := holdRequestIDAdvisoryLock(t, db, fakeRequestID)
	fakeOutcomeCh := reconnect(fakeRequestID)
	select {
	case outcome := <-fakeOutcomeCh:
		if outcome.err == nil || !strings.Contains(outcome.err.Error(), "cq_acquire request_id tuple mismatch") {
			_ = fakeTx.Rollback()
			_ = fakeConn.Close()
			t.Fatalf("expected reconnect tuple mismatch after ignoring caller lock, got result=%+v err=%v", outcome.result, outcome.err)
		}
	case <-time.After(200 * time.Millisecond):
		if err := fakeTx.Rollback(); err != nil {
			t.Fatalf("release fake advisory lock: %v", err)
		}
		if err := fakeConn.Close(); err != nil {
			t.Fatalf("close fake advisory lock connection: %v", err)
		}
		outcome := <-fakeOutcomeCh
		t.Fatalf("expected reconnect to ignore caller-supplied advisory lock, but it blocked until fake lock released; result=%+v err=%v", outcome.result, outcome.err)
	}
	if err := fakeTx.Rollback(); err != nil {
		t.Fatalf("release fake advisory lock after early tuple mismatch: %v", err)
	}
	if err := fakeConn.Close(); err != nil {
		t.Fatalf("close fake advisory lock connection after early tuple mismatch: %v", err)
	}

	authConn, authTx := holdRequestIDAdvisoryLock(t, db, authoritativeRequestID)
	authOutcomeCh := reconnect(fakeRequestID)
	select {
	case outcome := <-authOutcomeCh:
		_ = authTx.Rollback()
		_ = authConn.Close()
		t.Fatalf("expected reconnect to block on the authoritative request_id lock, got early result=%+v err=%v", outcome.result, outcome.err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := authTx.Rollback(); err != nil {
		t.Fatalf("release authoritative advisory lock: %v", err)
	}
	if err := authConn.Close(); err != nil {
		t.Fatalf("close authoritative advisory lock connection: %v", err)
	}
	authOutcome := <-authOutcomeCh
	if authOutcome.err == nil || !strings.Contains(authOutcome.err.Error(), "cq_acquire request_id tuple mismatch") {
		t.Fatalf("expected authoritative-lock reconnect to finish with tuple mismatch after unlock, got result=%+v err=%v", authOutcome.result, authOutcome.err)
	}
}

func TestRuntimeContinueWaitProbeLocksAuthoritativeRequestID(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	hardExpireMs := nowMs + 120_000
	authoritativeRequestID := "continue-wait-lock-authoritative"
	fakeRequestID := "continue-wait-lock-fake"

	busy, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "continue-wait-lock-host",
		Hostname:          "continue-wait-lock.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "continue-wait-lock-busy",
		HardExpireMs:      hardExpireMs,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed busy lease: %v", err)
	}
	if busy.Result != "granted" {
		t.Fatalf("expected granted busy seed, got %+v", busy)
	}

	waiting, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "continue-wait-lock-host",
		Hostname:          "continue-wait-lock.example.com",
		SiteBucket:        "site-b",
		IPBucket:          "ip-b",
		RequestID:         authoritativeRequestID,
		HardExpireMs:      hardExpireMs,
		NowMs:             nowMs + 1,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("seed waiting request: %v", err)
	}
	if waiting.Result != "wait" || !waiting.WaitToken.Valid {
		t.Fatalf("expected waiting request with token, got %+v", waiting)
	}

	type probeOutcome struct {
		result *runtimeAcquireResult
		err    error
	}

	probe := func(requestID string) <-chan probeOutcome {
		ch := make(chan probeOutcome, 1)
		go func() {
			result, err := execRuntimeContinueWaitProbe(context.Background(), db, AcquireRequest{
				HostnameHash:   "continue-wait-lock-host",
				Hostname:       "continue-wait-lock.example.com",
				SiteBucket:     "site-b",
				IPBucket:       "ip-b",
				RequestID:      requestID,
				HardExpireAtMs: hardExpireMs,
				NowMs:          nowMs + 2,
				WaitToken:      waiting.WaitToken.String,
			})
			ch <- probeOutcome{result: result, err: err}
		}()
		return ch
	}

	fakeConn, fakeTx := holdRequestIDAdvisoryLock(t, db, fakeRequestID)
	fakeOutcomeCh := probe(fakeRequestID)
	select {
	case outcome := <-fakeOutcomeCh:
		if outcome.err == nil || !strings.Contains(outcome.err.Error(), "cq_acquire request_id tuple mismatch") {
			_ = fakeTx.Rollback()
			_ = fakeConn.Close()
			t.Fatalf("expected continue wait probe tuple mismatch after ignoring caller lock, got result=%+v err=%v", outcome.result, outcome.err)
		}
	case <-time.After(200 * time.Millisecond):
		if err := fakeTx.Rollback(); err != nil {
			t.Fatalf("release fake advisory lock: %v", err)
		}
		if err := fakeConn.Close(); err != nil {
			t.Fatalf("close fake advisory lock connection: %v", err)
		}
		outcome := <-fakeOutcomeCh
		t.Fatalf("expected continue wait probe to ignore caller-supplied advisory lock, but it blocked until fake lock released; result=%+v err=%v", outcome.result, outcome.err)
	}
	if err := fakeTx.Rollback(); err != nil {
		t.Fatalf("release fake advisory lock after early tuple mismatch: %v", err)
	}
	if err := fakeConn.Close(); err != nil {
		t.Fatalf("close fake advisory lock connection after early tuple mismatch: %v", err)
	}

	authConn, authTx := holdRequestIDAdvisoryLock(t, db, authoritativeRequestID)
	authOutcomeCh := probe(fakeRequestID)
	select {
	case outcome := <-authOutcomeCh:
		_ = authTx.Rollback()
		_ = authConn.Close()
		t.Fatalf("expected continue wait probe to block on the authoritative request_id lock, got early result=%+v err=%v", outcome.result, outcome.err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := authTx.Rollback(); err != nil {
		t.Fatalf("release authoritative advisory lock: %v", err)
	}
	if err := authConn.Close(); err != nil {
		t.Fatalf("close authoritative advisory lock connection: %v", err)
	}
	authOutcome := <-authOutcomeCh
	if authOutcome.err == nil || !strings.Contains(authOutcome.err.Error(), "cq_acquire request_id tuple mismatch") {
		t.Fatalf("expected authoritative-lock continue wait probe to finish with tuple mismatch after unlock, got result=%+v err=%v", authOutcome.result, authOutcome.err)
	}
}
