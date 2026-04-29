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
	if duplicateResult != "conflict" || duplicateReason.String != "grant_already_claimed" || duplicateLeaseToken.Valid {
		t.Fatalf("expected duplicate claim conflict without lease identity, got result=%q reason=%q leaseToken=%q", duplicateResult, duplicateReason.String, duplicateLeaseToken.String)
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
		t.Fatalf("expected already-claimed replay conflict without lease token, got %+v", replayAfterClaim)
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

func TestRuntimeReleaseExpiresStaleHostScopeUsingRequestClock(t *testing.T) {
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
	if staleState != "expired" {
		t.Fatalf("expected stale lease expired during release cleanup, got %q", staleState)
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
	if hostCount != 0 {
		t.Fatalf("expected host counter drained after release cleanup, got %d", hostCount)
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
