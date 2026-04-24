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
	Scope       sql.NullString
	Reason      sql.NullString
	RetryAfter  sql.NullInt64
}

func execRuntimeAcquire(ctx context.Context, db *sql.DB, req runtimeAcquireCall) (*runtimeAcquireResult, error) {
	hostMaxInFlight := req.HostMaxInFlight
	if hostMaxInFlight <= 0 {
		hostMaxInFlight = 64
	}
	siteMaxInFlight := req.SiteMaxInFlight
	if siteMaxInFlight <= 0 {
		siteMaxInFlight = 32
	}
	siteIPMaxInFlight := req.SiteIPMaxInFlight
	if siteIPMaxInFlight <= 0 {
		siteIPMaxInFlight = 4
	}
	cleanupLimit := req.CleanupLimit
	if cleanupLimit <= 0 {
		cleanupLimit = 500
	}

	result := &runtimeAcquireResult{}
	err := db.QueryRowContext(ctx, `
		SELECT result, lease_id::text, lease_token, expires_at_ms, scope, reason, retry_after
		FROM cq_acquire($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`,
		req.HostnameHash,
		req.Hostname,
		req.SiteBucket,
		req.IPBucket,
		req.RequestID,
		req.HardExpireMs,
		req.NowMs,
		hostMaxInFlight,
		siteMaxInFlight,
		siteIPMaxInFlight,
		cleanupLimit,
	).Scan(
		&result.Result,
		&result.LeaseID,
		&result.LeaseToken,
		&result.ExpiresAtMs,
		&result.Scope,
		&result.Reason,
		&result.RetryAfter,
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func TestRuntimeAcquireRejectsPastHardExpiryUsingDatabaseClock(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)

	_, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash: "expired-host",
		Hostname:     "expired.example.com",
		SiteBucket:   "site-a",
		IPBucket:     "ip-a",
		RequestID:    "past-hard-expiry",
		HardExpireMs: 1000,
		NowMs:        1,
	})
	if err == nil {
		t.Fatal("expected past hard expiry acquire to fail")
	}
	if !strings.Contains(err.Error(), "cq_acquire hard_expire_at_ms is already in the past") {
		t.Fatalf("expected past hard expiry error, got %v", err)
	}
}

func TestRuntimePostgresPrecheckAllowsOnceDatabaseClockSeesLeaseExpired(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	seeded, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "precheck-host",
		Hostname:          "precheck.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "precheck-stale-seed",
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

	cfg := validTestConfig()
	cfg.Concurrency.Caps.HostMaxInFlight = 1
	cfg.Concurrency.Caps.SiteMaxInFlight = 1
	cfg.Concurrency.Caps.SiteIPMaxInFlight = 1
	backend := &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}}

	result, err := backend.Precheck(context.Background(), PrecheckRequest{
		Hostname:     "precheck.example.com",
		HostnameHash: "precheck-host",
		SiteBucket:   "site-a",
		IPBucket:     "ip-a",
		NowMs:        1,
	})
	if err != nil {
		t.Fatalf("precheck error: %v", err)
	}
	if result.Result != "allow" {
		t.Fatalf("expected allow once database clock sees lease expired, got %+v", result)
	}
}

func TestRuntimeAcquireCleansExpiredCapacityUsingDatabaseClock(t *testing.T) {
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
		NowMs:             1,
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

func TestRuntimeReleaseExpiresStaleHostScopeUsingDatabaseClock(t *testing.T) {
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
	if err := db.QueryRowContext(context.Background(), `
		SELECT result, reason
		FROM cq_release($1::uuid, $2, $3, $4)
	`, releaseLease.LeaseID, releaseLease.LeaseToken, "stream_complete", int64(1)).Scan(&releaseResult, &releaseReason); err != nil {
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
