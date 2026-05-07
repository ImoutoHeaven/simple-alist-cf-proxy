package concurrencyhandler

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
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

var (
	runtimePostgresMu          sync.Mutex
	runtimePostgresContainerID string
	runtimePostgresAddr        string
	runtimePostgresTemplateDB  string
)

func TestMain(m *testing.M) {
	code := m.Run()
	cleanupSharedRuntimeConcurrencyPostgres()
	os.Exit(code)
}

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

	containerID, addr, templateDB := requireSharedRuntimeConcurrencyPostgres(t)
	dbName := fmt.Sprintf("concurrency_sql_%d", time.Now().UnixNano())
	cloneRuntimeConcurrencyTemplateDB(t, containerID, addr, templateDB, dbName)

	dsn := fmt.Sprintf("postgres://postgres:postgres@%s/%s?sslmode=disable", addr, dbName)
	db := waitForRuntimeConcurrencyPostgresReady(t, containerID, dsn)
	t.Cleanup(func() {
		_ = dropRuntimeConcurrencyDB(addr, dbName)
	})
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func requireSharedRuntimeConcurrencyPostgres(t *testing.T) (containerID, addr, templateDB string) {
	t.Helper()

	runtimePostgresMu.Lock()
	defer runtimePostgresMu.Unlock()

	if runtimePostgresContainerID != "" && runtimePostgresAddr != "" && runtimePostgresTemplateDB != "" {
		return runtimePostgresContainerID, runtimePostgresAddr, runtimePostgresTemplateDB
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	runCmd := exec.CommandContext(
		ctx,
		"docker",
		"run",
		"--rm",
		"-d",
		"-e", "POSTGRES_PASSWORD=postgres",
		"-p", "127.0.0.1::5432",
		runtimePostgresImage,
	)
	runOut, err := runCmd.CombinedOutput()
	if err != nil {
		t.Skipf("start postgres container: %v (%s)", err, strings.TrimSpace(string(runOut)))
	}
	containerID = normalizeRuntimeContainerID(string(runOut))
	if containerID == "" {
		t.Fatalf("docker run returned no container id: %s", strings.TrimSpace(string(runOut)))
	}

	addr = waitForRuntimeConcurrencyPostgresPort(t, containerID)
	templateDB = fmt.Sprintf("concurrency_template_%d", time.Now().UnixNano())
	createRuntimeConcurrencyDB(t, containerID, addr, templateDB, "")
	applyRuntimeConcurrencyInitSQL(t, containerID, templateDB)

	runtimePostgresContainerID = containerID
	runtimePostgresAddr = addr
	runtimePostgresTemplateDB = templateDB
	return runtimePostgresContainerID, runtimePostgresAddr, runtimePostgresTemplateDB
}

func createRuntimeConcurrencyDB(t *testing.T, containerID, addr, dbName, templateDB string) {
	t.Helper()

	adminDSN := fmt.Sprintf("postgres://postgres:postgres@%s/postgres?sslmode=disable", addr)
	adminDB := waitForRuntimeConcurrencyPostgresReady(t, containerID, adminDSN)
	defer func() {
		_ = adminDB.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stmt := "CREATE DATABASE " + dbName
	if templateDB != "" {
		stmt += " TEMPLATE " + templateDB
	}
	if _, err := adminDB.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("create runtime database %s: %v", dbName, err)
	}
}

func cloneRuntimeConcurrencyTemplateDB(t *testing.T, containerID, addr, templateDB, dbName string) {
	t.Helper()
	createRuntimeConcurrencyDB(t, containerID, addr, dbName, templateDB)
}

func dropRuntimeConcurrencyDB(addr, dbName string) error {
	if strings.TrimSpace(addr) == "" || strings.TrimSpace(dbName) == "" {
		return nil
	}
	adminDB, err := sql.Open("pgx", fmt.Sprintf("postgres://postgres:postgres@%s/postgres?sslmode=disable", addr))
	if err != nil {
		return err
	}
	defer func() {
		_ = adminDB.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = adminDB.ExecContext(ctx, "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)")
	return err
}

func cleanupSharedRuntimeConcurrencyPostgres() {
	runtimePostgresMu.Lock()
	containerID := runtimePostgresContainerID
	runtimePostgresContainerID = ""
	runtimePostgresAddr = ""
	runtimePostgresTemplateDB = ""
	runtimePostgresMu.Unlock()

	if containerID == "" {
		return
	}
	_ = exec.Command("docker", "rm", "-f", containerID).Run()
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

type runtimeClaimGrantResult struct {
	Result            string
	LeaseID           string
	LeaseToken        string
	ExpiresAtMs       int64
	HandoffToken      sql.NullString
	HandoffDeadlineMs sql.NullInt64
	Reason            sql.NullString
}

type runtimeAckHandoffResult struct {
	Result              string
	Reason              sql.NullString
	HeartbeatDeadlineMs sql.NullInt64
}

type runtimeHeartbeatResult struct {
	Result              string
	Reason              sql.NullString
	Generation          sql.NullInt64
	DeadlineMs          sql.NullInt64
	AckTimeoutMs        sql.NullInt64
	HeartbeatIntervalMs sql.NullInt64
	HeartbeatTimeoutMs  sql.NullInt64
	ReconnectGraceMs    sql.NullInt64
	StartTimeoutMs      sql.NullInt64
	HardExpireAtMs      sql.NullInt64
}

type runtimeRequestState struct {
	State                     string
	TerminalReason            sql.NullString
	ClaimState                sql.NullString
	ClaimClaimedAtMs          sql.NullInt64
	HandoffState              sql.NullString
	HandoffToken              sql.NullString
	HandoffDeadlineMs         sql.NullInt64
	HandoffAckedAtMs          sql.NullInt64
	HeartbeatState            sql.NullString
	HeartbeatGeneration       sql.NullInt64
	HeartbeatLastAtMs         sql.NullInt64
	HeartbeatDeadlineMs       sql.NullInt64
	HeartbeatGraceUntilMs     sql.NullInt64
	HeartbeatConnectedAtMs    sql.NullInt64
	HeartbeatDisconnectedAtMs sql.NullInt64
	HeartbeatTerminalReason   sql.NullString
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

func execRuntimeClaimGrant(ctx context.Context, db *sql.DB, requestID, claimToken string, nowMs int64) (*runtimeClaimGrantResult, error) {
	result := &runtimeClaimGrantResult{}
	err := db.QueryRowContext(ctx, `
		SELECT result,
		       COALESCE(lease_id::text, ''),
		       COALESCE(lease_token, ''),
		       COALESCE(expires_at_ms, 0),
		       handoff_token,
		       handoff_deadline_ms,
		       reason
		FROM cq_claim_grant($1, $2, $3)
	`, requestID, claimToken, nowMs).Scan(
		&result.Result,
		&result.LeaseID,
		&result.LeaseToken,
		&result.ExpiresAtMs,
		&result.HandoffToken,
		&result.HandoffDeadlineMs,
		&result.Reason,
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func execRuntimeAckHandoff(ctx context.Context, db *sql.DB, requestID, handoffToken string, nowMs, startTimeoutMs int64) (*runtimeAckHandoffResult, error) {
	result := &runtimeAckHandoffResult{}
	err := db.QueryRowContext(ctx, `
		SELECT result, reason, heartbeat_deadline_ms
		FROM cq_ack_handoff($1, $2, $3, $4)
	`, requestID, handoffToken, nowMs, startTimeoutMs).Scan(&result.Result, &result.Reason, &result.HeartbeatDeadlineMs)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func execRuntimeHeartbeatOpen(ctx context.Context, db *sql.DB, req HeartbeatOpenRequest) (*runtimeHeartbeatResult, error) {
	result := &runtimeHeartbeatResult{}
	ticketHash := lookupTestStructStringField(req, "TicketHash")
	if ticketHash == "" {
		ticketHash = runtimeTicketHashForRequest(req.RequestID)
	}
	if err := ensureRuntimeHeartbeatTicketRow(ctx, db, ticketHash, req.NowMs, req.HardExpireAtMs, 300); err != nil {
		return nil, err
	}
	err := db.QueryRowContext(ctx, `
		SELECT result, reason, generation, deadline_ms, ack_timeout_ms, heartbeat_interval_ms, heartbeat_timeout_ms, reconnect_grace_ms, start_timeout_ms, hard_expire_at_ms
		FROM cq_heartbeat_open($1, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`, req.RequestID, req.LeaseID, req.LeaseToken, ticketHash, req.HardExpireAtMs, req.NowMs, req.HeartbeatTimeoutMs, req.AckTimeoutMs, req.HeartbeatIntervalMs, req.ReconnectGraceMs, req.StartTimeoutMs).Scan(
		&result.Result,
		&result.Reason,
		&result.Generation,
		&result.DeadlineMs,
		&result.AckTimeoutMs,
		&result.HeartbeatIntervalMs,
		&result.HeartbeatTimeoutMs,
		&result.ReconnectGraceMs,
		&result.StartTimeoutMs,
		&result.HardExpireAtMs,
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func execRuntimeHeartbeatRefresh(ctx context.Context, db *sql.DB, req HeartbeatRefreshRequest) (*runtimeHeartbeatResult, error) {
	result := &runtimeHeartbeatResult{}
	ticketHash := lookupTestStructStringField(req, "TicketHash")
	if ticketHash == "" {
		ticketHash = runtimeTicketHashForRequest(req.RequestID)
	}
	err := db.QueryRowContext(ctx, `
		SELECT result, reason, generation, deadline_ms, ack_timeout_ms, heartbeat_interval_ms, heartbeat_timeout_ms, reconnect_grace_ms, start_timeout_ms, hard_expire_at_ms
		FROM cq_heartbeat_refresh($1, $2::uuid, $3, $4, $5, $6, $7)
	`, req.RequestID, req.LeaseID, req.LeaseToken, ticketHash, req.Generation, req.NowMs, req.HeartbeatTimeoutMs).Scan(
		&result.Result,
		&result.Reason,
		&result.Generation,
		&result.DeadlineMs,
		&result.AckTimeoutMs,
		&result.HeartbeatIntervalMs,
		&result.HeartbeatTimeoutMs,
		&result.ReconnectGraceMs,
		&result.StartTimeoutMs,
		&result.HardExpireAtMs,
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func execRuntimeHeartbeatOpenForTicketTable(ctx context.Context, db *sql.DB, req HeartbeatOpenRequest, ticketTableName string) (*runtimeHeartbeatResult, error) {
	result := &runtimeHeartbeatResult{}
	ticketHash := lookupTestStructStringField(req, "TicketHash")
	if ticketHash == "" {
		ticketHash = runtimeTicketHashForRequest(req.RequestID)
	}
	err := db.QueryRowContext(ctx, `
		SELECT result, reason, generation, deadline_ms, ack_timeout_ms, heartbeat_interval_ms, heartbeat_timeout_ms, reconnect_grace_ms, start_timeout_ms, hard_expire_at_ms
		FROM cq_heartbeat_open($1, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`, req.RequestID, req.LeaseID, req.LeaseToken, ticketHash, req.HardExpireAtMs, req.NowMs, req.HeartbeatTimeoutMs, req.AckTimeoutMs, req.HeartbeatIntervalMs, req.ReconnectGraceMs, req.StartTimeoutMs, ticketTableName).Scan(
		&result.Result,
		&result.Reason,
		&result.Generation,
		&result.DeadlineMs,
		&result.AckTimeoutMs,
		&result.HeartbeatIntervalMs,
		&result.HeartbeatTimeoutMs,
		&result.ReconnectGraceMs,
		&result.StartTimeoutMs,
		&result.HardExpireAtMs,
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func execRuntimeHeartbeatDisconnect(ctx context.Context, db *sql.DB, req HeartbeatDisconnectRequest) (*runtimeHeartbeatResult, error) {
	result := &runtimeHeartbeatResult{}
	err := db.QueryRowContext(ctx, `
		SELECT result, reason, generation, deadline_ms, ack_timeout_ms, heartbeat_interval_ms, heartbeat_timeout_ms, reconnect_grace_ms, start_timeout_ms, hard_expire_at_ms
		FROM cq_heartbeat_disconnect($1, $2::uuid, $3, $4, $5, $6)
	`, req.RequestID, req.LeaseID, req.LeaseToken, req.Generation, req.NowMs, req.ReconnectGraceMs).Scan(
		&result.Result,
		&result.Reason,
		&result.Generation,
		&result.DeadlineMs,
		&result.AckTimeoutMs,
		&result.HeartbeatIntervalMs,
		&result.HeartbeatTimeoutMs,
		&result.ReconnectGraceMs,
		&result.StartTimeoutMs,
		&result.HardExpireAtMs,
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func execRuntimeExpireHeartbeatIfDue(ctx context.Context, db *sql.DB, requestID string, nowMs int64) (*runtimeHeartbeatResult, error) {
	result := &runtimeHeartbeatResult{}
	err := db.QueryRowContext(ctx, `
		SELECT result, reason, generation, deadline_ms, ack_timeout_ms, heartbeat_interval_ms, heartbeat_timeout_ms, reconnect_grace_ms, start_timeout_ms, hard_expire_at_ms
		FROM cq_expire_heartbeat_if_due($1, $2)
	`, requestID, nowMs).Scan(
		&result.Result,
		&result.Reason,
		&result.Generation,
		&result.DeadlineMs,
		&result.AckTimeoutMs,
		&result.HeartbeatIntervalMs,
		&result.HeartbeatTimeoutMs,
		&result.ReconnectGraceMs,
		&result.StartTimeoutMs,
		&result.HardExpireAtMs,
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func runtimeTicketHashForRequest(requestID string) string {
	return requestID + "-ticket-hash"
}

func seedRuntimeFirstUseTicketRow(t *testing.T, db *sql.DB, ticketHash string, issuedAtSeconds, hardExpireAtSeconds int64, idleTimeoutSeconds int) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO "DOWNLOAD_TICKET_STATE_TABLE" (
			"TICKET_HASH", "ISSUED_AT", "HARD_EXPIRE_AT", "IDLE_TIMEOUT_SECONDS", "FIRST_USED_AT",
			"IDLE_POLICY", "IDLE_LEASE_EXPIRES_AT", "IDLE_RENEW_OWNER_LEASE_ID", "IDLE_RENEW_OWNER_LAST_HEARTBEAT_AT",
			"IP_HASH", "PATH_HASH"
		) VALUES ($1, $2::bigint, $3::bigint, $4::integer, NULL, 'first_use', $2::bigint + $4::bigint, NULL, NULL, NULL, NULL)
	`, ticketHash, issuedAtSeconds, hardExpireAtSeconds, idleTimeoutSeconds); err != nil {
		t.Fatalf("seed first_use ticket row for %q: %v", ticketHash, err)
	}
}

func ensureRuntimeHeartbeatTicketRow(ctx context.Context, db *sql.DB, ticketHash string, issuedAtMs, hardExpireAtMs int64, idleTimeoutSeconds int) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO "DOWNLOAD_TICKET_STATE_TABLE" (
			"TICKET_HASH", "ISSUED_AT", "HARD_EXPIRE_AT", "IDLE_TIMEOUT_SECONDS", "FIRST_USED_AT",
			"IDLE_POLICY", "IDLE_LEASE_EXPIRES_AT", "IDLE_RENEW_OWNER_LEASE_ID", "IDLE_RENEW_OWNER_LAST_HEARTBEAT_AT",
			"IP_HASH", "PATH_HASH"
		) VALUES ($1, $2::bigint, $3::bigint, $4::integer, NULL, 'first_use', $2::bigint + $4::bigint, NULL, NULL, NULL, NULL)
		ON CONFLICT ("TICKET_HASH") DO NOTHING
	`, ticketHash, issuedAtMs/1000, hardExpireAtMs/1000, idleTimeoutSeconds)
	return err
}

func seedRuntimeDefaultHeartbeatTicketForRequest(t *testing.T, db *sql.DB, requestID string, issuedAtMs, hardExpireAtMs int64) string {
	t.Helper()
	ticketHash := runtimeTicketHashForRequest(requestID)
	seedRuntimeFirstUseTicketRow(t, db, ticketHash, issuedAtMs/1000, hardExpireAtMs/1000, 300)
	return ticketHash
}

type runtimeTicketState struct {
	TicketHash                    string
	IssuedAt                      int64
	HardExpireAt                  int64
	IdleTimeoutSeconds            int64
	FirstUsedAt                   sql.NullInt64
	IdlePolicy                    string
	IdleLeaseExpiresAt            int64
	IdleRenewOwnerLeaseID         sql.NullString
	IdleRenewOwnerLastHeartbeatAt sql.NullInt64
}

func readRuntimeTicketState(t *testing.T, db *sql.DB, ticketHash string) runtimeTicketState {
	t.Helper()
	var state runtimeTicketState
	if err := db.QueryRowContext(context.Background(), `
		SELECT "TICKET_HASH",
		       "ISSUED_AT",
		       "HARD_EXPIRE_AT",
		       "IDLE_TIMEOUT_SECONDS",
		       "FIRST_USED_AT",
		       "IDLE_POLICY",
		       "IDLE_LEASE_EXPIRES_AT",
		       "IDLE_RENEW_OWNER_LEASE_ID"::text,
		       "IDLE_RENEW_OWNER_LAST_HEARTBEAT_AT"
		FROM "DOWNLOAD_TICKET_STATE_TABLE"
		WHERE "TICKET_HASH" = $1
	`, ticketHash).Scan(
		&state.TicketHash,
		&state.IssuedAt,
		&state.HardExpireAt,
		&state.IdleTimeoutSeconds,
		&state.FirstUsedAt,
		&state.IdlePolicy,
		&state.IdleLeaseExpiresAt,
		&state.IdleRenewOwnerLeaseID,
		&state.IdleRenewOwnerLastHeartbeatAt,
	); err != nil {
		t.Fatalf("read ticket row for %q: %v", ticketHash, err)
	}
	return state
}

func readRuntimeTicketStateFromTable(t *testing.T, db *sql.DB, ticketHash, tableName string) runtimeTicketState {
	t.Helper()
	query := fmt.Sprintf(`
		SELECT "TICKET_HASH",
		       "ISSUED_AT",
		       "HARD_EXPIRE_AT",
		       "IDLE_TIMEOUT_SECONDS",
		       "FIRST_USED_AT",
		       "IDLE_POLICY",
		       "IDLE_LEASE_EXPIRES_AT",
		       "IDLE_RENEW_OWNER_LEASE_ID"::text,
		       "IDLE_RENEW_OWNER_LAST_HEARTBEAT_AT"
		FROM %q
		WHERE "TICKET_HASH" = $1
	`, tableName)
	var state runtimeTicketState
	if err := db.QueryRowContext(context.Background(), query, ticketHash).Scan(
		&state.TicketHash,
		&state.IssuedAt,
		&state.HardExpireAt,
		&state.IdleTimeoutSeconds,
		&state.FirstUsedAt,
		&state.IdlePolicy,
		&state.IdleLeaseExpiresAt,
		&state.IdleRenewOwnerLeaseID,
		&state.IdleRenewOwnerLastHeartbeatAt,
	); err != nil {
		t.Fatalf("read ticket row for %q from %s: %v", ticketHash, tableName, err)
	}
	return state
}

func createRuntimeTicketStateTableClone(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()
	query := fmt.Sprintf(`CREATE TABLE %q AS SELECT * FROM "DOWNLOAD_TICKET_STATE_TABLE" WITH NO DATA`, tableName)
	if _, err := db.ExecContext(context.Background(), query); err != nil {
		t.Fatalf("create runtime ticket table clone %s: %v", tableName, err)
	}
}

func seedRuntimeFirstUseTicketRowInTable(t *testing.T, db *sql.DB, tableName, ticketHash string, issuedAtSeconds, hardExpireAtSeconds int64, idleTimeoutSeconds int) {
	t.Helper()
	query := fmt.Sprintf(`
		INSERT INTO %q (
			"TICKET_HASH", "ISSUED_AT", "HARD_EXPIRE_AT", "IDLE_TIMEOUT_SECONDS", "FIRST_USED_AT",
			"IDLE_POLICY", "IDLE_LEASE_EXPIRES_AT", "IDLE_RENEW_OWNER_LEASE_ID", "IDLE_RENEW_OWNER_LAST_HEARTBEAT_AT",
			"IP_HASH", "PATH_HASH"
		) VALUES ($1, $2::bigint, $3::bigint, $4::integer, NULL, 'first_use', $2::bigint + $4::bigint, NULL, NULL, NULL, NULL)
	`, tableName)
	if _, err := db.ExecContext(context.Background(), query, ticketHash, issuedAtSeconds, hardExpireAtSeconds, idleTimeoutSeconds); err != nil {
		t.Fatalf("seed first_use ticket row for %q in %s: %v", ticketHash, tableName, err)
	}
}

func seedRuntimeConnectedHeartbeat(t *testing.T, db *sql.DB, requestID, hostnameHash string, nowMs, heartbeatTimeoutMs int64) (*runtimeAcquireResult, *runtimeHeartbeatResult) {
	t.Helper()

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      hostnameHash,
		Hostname:          hostnameHash + ".example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         requestID,
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, requestID, grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, requestID, claim.HandoffToken.String, nowMs+2, 7_000); err != nil {
		t.Fatalf("ack handoff: %v", err)
	}
	ticketHash := seedRuntimeDefaultHeartbeatTicketForRequest(t, db, requestID, nowMs, grant.ExpiresAtMs)
	openReq := HeartbeatOpenRequest{
		RequestID:           requestID,
		LeaseID:             grant.LeaseID,
		LeaseToken:          grant.LeaseToken,
		HardExpireAtMs:      grant.ExpiresAtMs,
		NowMs:               nowMs + 3,
		HeartbeatTimeoutMs:  heartbeatTimeoutMs,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	}
	setTestStructStringField(&openReq, "TicketHash", ticketHash)
	open, err := execRuntimeHeartbeatOpen(context.Background(), db, openReq)
	if err != nil {
		t.Fatalf("heartbeat open: %v", err)
	}
	if open.Result != "accepted" || !open.Generation.Valid {
		t.Fatalf("expected accepted heartbeat open, got %+v", open)
	}
	return grant, open
}

func assertRuntimeHeartbeatReleasedReplayReason(t *testing.T, db *sql.DB, requestID string, grant *runtimeAcquireResult, generation, nowMs int64, expectedReason string) {
	t.Helper()

	open, err := execRuntimeHeartbeatOpen(context.Background(), db, HeartbeatOpenRequest{
		RequestID:           requestID,
		LeaseID:             grant.LeaseID,
		LeaseToken:          grant.LeaseToken,
		HardExpireAtMs:      grant.ExpiresAtMs,
		NowMs:               nowMs,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	})
	if err != nil {
		t.Fatalf("heartbeat open replay: %v", err)
	}
	if open.Result != "terminal" || open.Reason.String != expectedReason {
		t.Fatalf("expected heartbeat open replay terminal %q, got %+v", expectedReason, open)
	}

	refresh, err := execRuntimeHeartbeatRefresh(context.Background(), db, HeartbeatRefreshRequest{
		RequestID:          requestID,
		LeaseID:            grant.LeaseID,
		LeaseToken:         grant.LeaseToken,
		Generation:         generation,
		NowMs:              nowMs + 1,
		HeartbeatTimeoutMs: 15_000,
	})
	if err != nil {
		t.Fatalf("heartbeat refresh replay: %v", err)
	}
	if refresh.Result != "terminal" || refresh.Reason.String != expectedReason {
		t.Fatalf("expected heartbeat refresh replay terminal %q, got %+v", expectedReason, refresh)
	}

	disconnect, err := execRuntimeHeartbeatDisconnect(context.Background(), db, HeartbeatDisconnectRequest{
		RequestID:        requestID,
		LeaseID:          grant.LeaseID,
		LeaseToken:       grant.LeaseToken,
		Generation:       generation,
		NowMs:            nowMs + 2,
		ReconnectGraceMs: 12_000,
	})
	if err != nil {
		t.Fatalf("heartbeat disconnect replay: %v", err)
	}
	if disconnect.Result != "terminal" || disconnect.Reason.String != expectedReason {
		t.Fatalf("expected heartbeat disconnect replay terminal %q, got %+v", expectedReason, disconnect)
	}

	expire, err := execRuntimeExpireHeartbeatIfDue(context.Background(), db, requestID, nowMs+3)
	if err != nil {
		t.Fatalf("heartbeat expiry replay: %v", err)
	}
	if expire.Result != "terminal" || expire.Reason.String != expectedReason {
		t.Fatalf("expected heartbeat expiry replay terminal %q, got %+v", expectedReason, expire)
	}
}

func readRuntimeRequestState(t *testing.T, db *sql.DB, requestID string) runtimeRequestState {
	t.Helper()

	var state runtimeRequestState
	if err := db.QueryRowContext(context.Background(), `
		SELECT state,
		       terminal_reason,
		       claim_state,
		       claim_claimed_at_ms,
		       handoff_state,
		       handoff_token,
		       handoff_deadline_ms,
		       handoff_acked_at_ms,
		       heartbeat_state,
		       heartbeat_generation,
		       heartbeat_last_at_ms,
		       heartbeat_deadline_ms,
		       heartbeat_grace_until_ms,
		       heartbeat_connected_at_ms,
		       heartbeat_disconnected_at_ms,
		       heartbeat_terminal_reason
		FROM concurrency_requests
		WHERE request_id = $1
	`, requestID).Scan(
		&state.State,
		&state.TerminalReason,
		&state.ClaimState,
		&state.ClaimClaimedAtMs,
		&state.HandoffState,
		&state.HandoffToken,
		&state.HandoffDeadlineMs,
		&state.HandoffAckedAtMs,
		&state.HeartbeatState,
		&state.HeartbeatGeneration,
		&state.HeartbeatLastAtMs,
		&state.HeartbeatDeadlineMs,
		&state.HeartbeatGraceUntilMs,
		&state.HeartbeatConnectedAtMs,
		&state.HeartbeatDisconnectedAtMs,
		&state.HeartbeatTerminalReason,
	); err != nil {
		t.Fatalf("read request state for %q: %v", requestID, err)
	}

	return state
}

func readRuntimeActiveCounters(t *testing.T, db *sql.DB, hostnameHash, siteBucket, ipBucket string) (int, int, int) {
	t.Helper()

	var hostCount, siteCount, siteIPCount int
	if err := db.QueryRowContext(context.Background(), `
		SELECT active_count
		FROM concurrency_host_counters
		WHERE hostname_hash = $1
	`, hostnameHash).Scan(&hostCount); err != nil {
		t.Fatalf("read host counter for %q: %v", hostnameHash, err)
	}
	if err := db.QueryRowContext(context.Background(), `
		SELECT active_count
		FROM concurrency_site_counters
		WHERE hostname_hash = $1 AND site_bucket = $2
	`, hostnameHash, siteBucket).Scan(&siteCount); err != nil {
		t.Fatalf("read site counter for %q/%q: %v", hostnameHash, siteBucket, err)
	}
	if err := db.QueryRowContext(context.Background(), `
		SELECT active_count
		FROM concurrency_site_ip_counters
		WHERE hostname_hash = $1 AND site_bucket = $2 AND ip_bucket = $3
	`, hostnameHash, siteBucket, ipBucket).Scan(&siteIPCount); err != nil {
		t.Fatalf("read site_ip counter for %q/%q/%q: %v", hostnameHash, siteBucket, ipBucket, err)
	}

	return hostCount, siteCount, siteIPCount
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

func TestRuntimeTicketMarkTicketUsedPreservesRenewalFields(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowSeconds := time.Now().Unix()
	ticketHash := fmt.Sprintf("runtime-ticket-renewable-%d", time.Now().UnixNano())
	expectedLeaseExpiresAt := nowSeconds + 60
	expectedOwnerLeaseID := "11111111-1111-1111-1111-111111111111"
	expectedOwnerHeartbeatAt := nowSeconds - 5

	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO "DOWNLOAD_TICKET_STATE_TABLE" (
			"TICKET_HASH", "ISSUED_AT", "HARD_EXPIRE_AT", "IDLE_TIMEOUT_SECONDS", "FIRST_USED_AT",
			"IDLE_POLICY", "IDLE_LEASE_EXPIRES_AT", "IDLE_RENEW_OWNER_LEASE_ID", "IDLE_RENEW_OWNER_LAST_HEARTBEAT_AT",
			"IP_HASH", "PATH_HASH"
		) VALUES ($1, $2, $3, $4, NULL, 'renewable', $5, $6::uuid, $7, $8, $9)
	`, ticketHash, nowSeconds-10, nowSeconds+300, 60, expectedLeaseExpiresAt, expectedOwnerLeaseID, expectedOwnerHeartbeatAt, "ip-hash", "path-hash"); err != nil {
		t.Fatalf("seed renewable ticket row: %v", err)
	}

	var rawResponse string
	if err := db.QueryRowContext(context.Background(), `
		SELECT download_mark_ticket_used($1, $2, $3)
	`, ticketHash, nowSeconds, "DOWNLOAD_TICKET_STATE_TABLE").Scan(&rawResponse); err != nil {
		t.Fatalf("mark ticket used: %v", err)
	}

	var response struct {
		Result      string `json:"result"`
		FirstUsedAt int64  `json:"first_used_at"`
	}
	if err := json.Unmarshal([]byte(rawResponse), &response); err != nil {
		t.Fatalf("decode mark ticket used response: %v", err)
	}
	if response.Result != "transitioned" {
		t.Fatalf("expected transitioned result, got %q", response.Result)
	}
	if response.FirstUsedAt != nowSeconds {
		t.Fatalf("expected first_used_at %d, got %d", nowSeconds, response.FirstUsedAt)
	}

	var persistedFirstUsedAt int64
	var idlePolicy string
	var idleLeaseExpiresAt int64
	var ownerLeaseID string
	var ownerHeartbeatAt int64
	if err := db.QueryRowContext(context.Background(), `
		SELECT "FIRST_USED_AT",
		       "IDLE_POLICY",
		       "IDLE_LEASE_EXPIRES_AT",
		       COALESCE("IDLE_RENEW_OWNER_LEASE_ID"::text, ''),
		       "IDLE_RENEW_OWNER_LAST_HEARTBEAT_AT"
		FROM "DOWNLOAD_TICKET_STATE_TABLE"
		WHERE "TICKET_HASH" = $1
	`, ticketHash).Scan(&persistedFirstUsedAt, &idlePolicy, &idleLeaseExpiresAt, &ownerLeaseID, &ownerHeartbeatAt); err != nil {
		t.Fatalf("read ticket row after mark used: %v", err)
	}

	if persistedFirstUsedAt != nowSeconds {
		t.Fatalf("expected persisted first_used_at %d, got %d", nowSeconds, persistedFirstUsedAt)
	}
	if idlePolicy != "renewable" {
		t.Fatalf("expected idle policy to remain renewable, got %q", idlePolicy)
	}
	if idleLeaseExpiresAt != expectedLeaseExpiresAt {
		t.Fatalf("expected idle lease expiry %d, got %d", expectedLeaseExpiresAt, idleLeaseExpiresAt)
	}
	if ownerLeaseID != expectedOwnerLeaseID {
		t.Fatalf("expected owner lease id %q, got %q", expectedOwnerLeaseID, ownerLeaseID)
	}
	if ownerHeartbeatAt != expectedOwnerHeartbeatAt {
		t.Fatalf("expected owner heartbeat at %d, got %d", expectedOwnerHeartbeatAt, ownerHeartbeatAt)
	}
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

func TestRuntimeClaimGrantTransitionsToHandoffPending(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "handoff-pending-host",
		Hostname:          "handoff-pending.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "handoff-pending-request",
		HardExpireMs:      nowMs + 120_000,
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

	claim, err := execRuntimeClaimGrant(context.Background(), db, "handoff-pending-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	if claim.Result != "granted" {
		t.Fatalf("expected granted claim result, got %+v", claim)
	}
	if claim.LeaseID != grant.LeaseID || claim.LeaseToken != grant.LeaseToken || claim.ExpiresAtMs != grant.ExpiresAtMs {
		t.Fatalf("expected claim grant to preserve lease identity, got %+v grant=%+v", claim, grant)
	}
	if !claim.HandoffToken.Valid || strings.TrimSpace(claim.HandoffToken.String) == "" {
		t.Fatalf("expected claim grant to return handoff token, got %+v", claim)
	}
	if !claim.HandoffDeadlineMs.Valid || claim.HandoffDeadlineMs.Int64 <= nowMs+1 || claim.HandoffDeadlineMs.Int64 >= grant.ExpiresAtMs {
		t.Fatalf("expected handoff deadline between claim time and lease expiry, got %+v", claim)
	}

	request := readRuntimeRequestState(t, db, "handoff-pending-request")
	if request.State != "active" {
		t.Fatalf("expected request to remain active after claim grant, got %+v", request)
	}
	if request.ClaimState.String != "claimed" || !request.ClaimClaimedAtMs.Valid || request.ClaimClaimedAtMs.Int64 != nowMs+1 {
		t.Fatalf("expected claim grant to persist claimed state and timestamp, got %+v", request)
	}
	if request.HandoffState.String != "pending" || request.HandoffToken.String != claim.HandoffToken.String || request.HandoffDeadlineMs.Int64 != claim.HandoffDeadlineMs.Int64 {
		t.Fatalf("expected claim grant to persist handoff pending metadata, got %+v claim=%+v", request, claim)
	}
	if request.HandoffAckedAtMs.Valid {
		t.Fatalf("expected handoff_acked_at_ms to remain null before ack, got %+v", request)
	}
}

func TestRuntimeDuplicateClaimReplaysSameHandoffPayload(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "handoff-replay-host",
		Hostname:          "handoff-replay.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "handoff-replay-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}

	firstClaim, err := execRuntimeClaimGrant(context.Background(), db, "handoff-replay-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("first claim grant: %v", err)
	}
	duplicateClaim, err := execRuntimeClaimGrant(context.Background(), db, "handoff-replay-request", grant.ClaimToken.String, nowMs+2)
	if err != nil {
		t.Fatalf("duplicate claim grant: %v", err)
	}

	if duplicateClaim.Result != "granted" {
		t.Fatalf("expected duplicate claim replay to stay granted, got %+v", duplicateClaim)
	}
	if duplicateClaim.LeaseID != firstClaim.LeaseID || duplicateClaim.LeaseToken != firstClaim.LeaseToken || duplicateClaim.ExpiresAtMs != firstClaim.ExpiresAtMs {
		t.Fatalf("expected duplicate claim replay to preserve lease identity, first=%+v duplicate=%+v", firstClaim, duplicateClaim)
	}
	if duplicateClaim.HandoffToken.String != firstClaim.HandoffToken.String || duplicateClaim.HandoffDeadlineMs.Int64 != firstClaim.HandoffDeadlineMs.Int64 {
		t.Fatalf("expected duplicate claim replay to preserve handoff payload, first=%+v duplicate=%+v", firstClaim, duplicateClaim)
	}
	if duplicateClaim.Reason.Valid {
		t.Fatalf("expected duplicate claim replay without terminal reason, got %+v", duplicateClaim)
	}
}

func TestRuntimeAckHandoffAcknowledgesPendingRequest(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "handoff-ack-host",
		Hostname:          "handoff-ack.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "handoff-ack-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "handoff-ack-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}

	workerNowMs := nowMs - 30_000
	beforeAck := time.Now().UnixMilli()
	ack, err := execRuntimeAckHandoff(context.Background(), db, "handoff-ack-request", claim.HandoffToken.String, workerNowMs, 7000)
	afterAck := time.Now().UnixMilli()
	if err != nil {
		t.Fatalf("ack handoff: %v", err)
	}
	if ack.Result != "acknowledged" || ack.Reason.Valid {
		t.Fatalf("expected acknowledged ack_handoff result, got %+v", ack)
	}

	request := readRuntimeRequestState(t, db, "handoff-ack-request")
	if request.HandoffState.String != "acknowledged" || !request.HandoffAckedAtMs.Valid {
		t.Fatalf("expected ack_handoff to persist acknowledged handoff state, got %+v", request)
	}
	if request.HandoffAckedAtMs.Int64 < beforeAck || request.HandoffAckedAtMs.Int64 > afterAck || request.HandoffAckedAtMs.Int64 == workerNowMs {
		t.Fatalf("expected handoff ack timestamp from handler-side commit time in [%d,%d] instead of worker time %d, got %+v", beforeAck, afterAck, workerNowMs, request)
	}
}

func TestRuntimeDuplicateAckHandoffPreservesOriginalAckTimestamp(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "handoff-duplicate-ack-host",
		Hostname:          "handoff-duplicate-ack.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "handoff-duplicate-ack-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "handoff-duplicate-ack-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "handoff-duplicate-ack-request", claim.HandoffToken.String, nowMs+2, 7000); err != nil {
		t.Fatalf("first ack handoff: %v", err)
	}

	firstAck := readRuntimeRequestState(t, db, "handoff-duplicate-ack-request")
	if !firstAck.HandoffAckedAtMs.Valid {
		t.Fatalf("expected first ack to persist timestamp, got %+v", firstAck)
	}

	duplicateAck, err := execRuntimeAckHandoff(context.Background(), db, "handoff-duplicate-ack-request", claim.HandoffToken.String, nowMs+3, 7000)
	if err != nil {
		t.Fatalf("duplicate ack handoff: %v", err)
	}
	if duplicateAck.Result != "acknowledged" || duplicateAck.Reason.Valid {
		t.Fatalf("expected duplicate ack replay to stay acknowledged, got %+v", duplicateAck)
	}

	secondAck := readRuntimeRequestState(t, db, "handoff-duplicate-ack-request")
	if !secondAck.HandoffAckedAtMs.Valid || secondAck.HandoffAckedAtMs.Int64 != firstAck.HandoffAckedAtMs.Int64 {
		t.Fatalf("expected duplicate ack to preserve original timestamp, first=%+v second=%+v", firstAck, secondAck)
	}
}

func TestRuntimeOverdueHandoffPendingCompensatesBeforeReplay(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "handoff-timeout-host",
		Hostname:          "handoff-timeout.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "handoff-timeout-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "handoff-timeout-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	if !claim.HandoffDeadlineMs.Valid {
		t.Fatalf("expected handoff deadline for timeout test, got %+v", claim)
	}

	overdueNow := claim.HandoffDeadlineMs.Int64
	duplicateClaim, err := execRuntimeClaimGrant(context.Background(), db, "handoff-timeout-request", grant.ClaimToken.String, overdueNow)
	if err != nil {
		t.Fatalf("duplicate claim after overdue handoff: %v", err)
	}
	if duplicateClaim.Result != "released" || duplicateClaim.Reason.String != "claim_handoff_timeout" {
		t.Fatalf("expected overdue duplicate claim to compensate before replay, got %+v", duplicateClaim)
	}

	request := readRuntimeRequestState(t, db, "handoff-timeout-request")
	if request.State != "released" || request.TerminalReason.String != "claim_handoff_timeout" {
		t.Fatalf("expected overdue handoff to transition request to released/claim_handoff_timeout, got %+v", request)
	}
	if request.ClaimState.String != "compensated" || request.HandoffState.String != "compensated" {
		t.Fatalf("expected overdue handoff to mark compensated sub-states, got %+v", request)
	}
	if request.HandoffAckedAtMs.Valid {
		t.Fatalf("expected timed out handoff to remain unacked, got %+v", request)
	}

	hostCount, siteCount, siteIPCount := readRuntimeActiveCounters(t, db, "handoff-timeout-host", "site-a", "ip-a")
	if hostCount != 0 || siteCount != 0 || siteIPCount != 0 {
		t.Fatalf("expected handoff-timeout compensation to decrement counters once, got host=%d site=%d site_ip=%d", hostCount, siteCount, siteIPCount)
	}
}

func TestRuntimeLateAckAfterHandoffTimeoutReturnsTerminalWithoutResurrection(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "handoff-late-ack-host",
		Hostname:          "handoff-late-ack.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "handoff-late-ack-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "handoff-late-ack-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}

	overdueNow := claim.HandoffDeadlineMs.Int64
	if _, err := execRuntimeClaimGrant(context.Background(), db, "handoff-late-ack-request", grant.ClaimToken.String, overdueNow); err != nil {
		t.Fatalf("compensating duplicate claim: %v", err)
	}

	ack, err := execRuntimeAckHandoff(context.Background(), db, "handoff-late-ack-request", claim.HandoffToken.String, overdueNow+1, 7000)
	if err != nil {
		t.Fatalf("late ack after timeout compensation: %v", err)
	}
	if ack.Result != "released" || ack.Reason.String != "claim_handoff_timeout" {
		t.Fatalf("expected late ack to replay terminal handoff-timeout result, got %+v", ack)
	}

	request := readRuntimeRequestState(t, db, "handoff-late-ack-request")
	if request.State != "released" || request.TerminalReason.String != "claim_handoff_timeout" {
		t.Fatalf("expected timed out request to stay terminal after late ack, got %+v", request)
	}
	if request.HandoffState.String == "acknowledged" || request.HandoffAckedAtMs.Valid {
		t.Fatalf("expected late ack not to resurrect acknowledged handoff state, got %+v", request)
	}
}

func TestRuntimeAckHandoffSeedsHeartbeatStartDeadline(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	startTimeoutMs := int64(7000)

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-start-host",
		Hostname:          "heartbeat-start.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-start-request",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-start-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}

	workerNowMs := nowMs - 45_000
	beforeAck := time.Now().UnixMilli()
	ack, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-start-request", claim.HandoffToken.String, workerNowMs, startTimeoutMs)
	afterAck := time.Now().UnixMilli()
	if err != nil {
		t.Fatalf("ack handoff: %v", err)
	}
	minDeadline := beforeAck + startTimeoutMs
	maxDeadline := afterAck + startTimeoutMs
	if !ack.HeartbeatDeadlineMs.Valid || ack.HeartbeatDeadlineMs.Int64 < minDeadline || ack.HeartbeatDeadlineMs.Int64 > maxDeadline || ack.HeartbeatDeadlineMs.Int64 == workerNowMs+startTimeoutMs {
		t.Fatalf("expected heartbeat start deadline from handler-side commit time in [%d,%d] instead of worker-derived %d, got %+v", minDeadline, maxDeadline, workerNowMs+startTimeoutMs, ack)
	}

	request := readRuntimeRequestState(t, db, "heartbeat-start-request")
	if request.HandoffState.String != "acknowledged" {
		t.Fatalf("expected acknowledged handoff state, got %+v", request)
	}
	if request.HeartbeatState.String != "none" || !request.HeartbeatDeadlineMs.Valid || request.HeartbeatDeadlineMs.Int64 < minDeadline || request.HeartbeatDeadlineMs.Int64 > maxDeadline {
		t.Fatalf("expected heartbeat start state none with deadline in [%d,%d], got %+v", minDeadline, maxDeadline, request)
	}
	if request.HeartbeatGraceUntilMs.Valid || request.HeartbeatTerminalReason.Valid {
		t.Fatalf("expected fresh heartbeat start state without grace or terminal reason, got %+v", request)
	}
}

func TestRuntimeExpireActiveRequestIfDueReleasesHeartbeatStartTimeout(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-timeout-host",
		Hostname:          "heartbeat-timeout.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-timeout-request",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-timeout-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	ack, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-timeout-request", claim.HandoffToken.String, nowMs+2, 25)
	if err != nil {
		t.Fatalf("ack handoff: %v", err)
	}

	var expired bool
	if err := db.QueryRowContext(context.Background(), `
		SELECT cq_expire_active_request_if_due($1, $2)
	`, "heartbeat-timeout-request", ack.HeartbeatDeadlineMs.Int64).Scan(&expired); err != nil {
		t.Fatalf("expire active request if due: %v", err)
	}
	if !expired {
		t.Fatal("expected heartbeat start timeout expiry to transition the request")
	}

	request := readRuntimeRequestState(t, db, "heartbeat-timeout-request")
	if request.State != "released" || request.TerminalReason.String != "heartbeat_start_timeout" {
		t.Fatalf("expected heartbeat start timeout terminal release, got %+v", request)
	}

	hostCount, siteCount, siteIPCount := readRuntimeActiveCounters(t, db, "heartbeat-timeout-host", "site-a", "ip-a")
	if hostCount != 0 || siteCount != 0 || siteIPCount != 0 {
		t.Fatalf("expected heartbeat start timeout to decrement counters, got host=%d site=%d site_ip=%d", hostCount, siteCount, siteIPCount)
	}
}

func TestRuntimeHeartbeatStartDeadlinePreservesHardExpiryPrecedence(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	hardExpireAtMs := nowMs + 40

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-hard-expiry-host",
		Hostname:          "heartbeat-hard-expiry.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-hard-expiry-request",
		HardExpireMs:      hardExpireAtMs,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-hard-expiry-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	ack, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-hard-expiry-request", claim.HandoffToken.String, nowMs+2, 7_000)
	if err != nil {
		t.Fatalf("ack handoff: %v", err)
	}
	if !ack.HeartbeatDeadlineMs.Valid || ack.HeartbeatDeadlineMs.Int64 != hardExpireAtMs {
		t.Fatalf("expected heartbeat deadline capped by hard expiry %d, got %+v", hardExpireAtMs, ack)
	}

	var expired bool
	if err := db.QueryRowContext(context.Background(), `
		SELECT cq_expire_active_request_if_due($1, $2)
	`, "heartbeat-hard-expiry-request", ack.HeartbeatDeadlineMs.Int64).Scan(&expired); err != nil {
		t.Fatalf("expire active request if due: %v", err)
	}
	if !expired {
		t.Fatal("expected hard expiry to terminalize the request")
	}

	request := readRuntimeRequestState(t, db, "heartbeat-hard-expiry-request")
	if request.State != "expired" || request.TerminalReason.String != "hard_expired" {
		t.Fatalf("expected hard_expired precedence over heartbeat start timeout, got %+v", request)
	}
}

func TestRuntimeHeartbeatOpenAcceptsAcknowledgedLease(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-open-host",
		Hostname:          "heartbeat-open.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-open-request",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-open-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-open-request", claim.HandoffToken.String, nowMs+2, 7000); err != nil {
		t.Fatalf("ack handoff: %v", err)
	}

	open, err := execRuntimeHeartbeatOpen(context.Background(), db, HeartbeatOpenRequest{
		RequestID:           "heartbeat-open-request",
		LeaseID:             grant.LeaseID,
		LeaseToken:          grant.LeaseToken,
		HardExpireAtMs:      grant.ExpiresAtMs,
		NowMs:               nowMs + 3,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	})
	if err != nil {
		t.Fatalf("heartbeat open: %v", err)
	}
	if open.Result != "accepted" || !open.Generation.Valid || open.Generation.Int64 != 1 {
		t.Fatalf("expected accepted heartbeat open result, got %+v", open)
	}
	if !open.DeadlineMs.Valid || open.DeadlineMs.Int64 != nowMs+3+15_000 {
		t.Fatalf("expected heartbeat deadline %d, got %+v", nowMs+3+15_000, open)
	}

	request := readRuntimeRequestState(t, db, "heartbeat-open-request")
	if request.HeartbeatState.String != "connected" || request.HeartbeatGeneration.Int64 != 1 {
		t.Fatalf("expected connected heartbeat state, got %+v", request)
	}
	if request.HeartbeatConnectedAtMs.Int64 != nowMs+3 || request.HeartbeatLastAtMs.Int64 != nowMs+3 {
		t.Fatalf("expected connected and last heartbeat timestamps at %d, got %+v", nowMs+3, request)
	}
}

func TestRuntimeHeartbeatOpenPromotesFirstUseTicketToRenewableOwner(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	ticketHash := "heartbeat-renewable-owner-ticket"

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-renewable-owner-host",
		Hostname:          "heartbeat-renewable-owner.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-renewable-owner-request",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-renewable-owner-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-renewable-owner-request", claim.HandoffToken.String, nowMs+2, 7_000); err != nil {
		t.Fatalf("ack handoff: %v", err)
	}
	seedRuntimeFirstUseTicketRow(t, db, ticketHash, nowMs/1000, grant.ExpiresAtMs/1000, 60)

	openReq := HeartbeatOpenRequest{
		RequestID:           "heartbeat-renewable-owner-request",
		LeaseID:             grant.LeaseID,
		LeaseToken:          grant.LeaseToken,
		HardExpireAtMs:      grant.ExpiresAtMs,
		NowMs:               nowMs + 3,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	}
	setTestStructStringField(&openReq, "TicketHash", ticketHash)
	open, err := execRuntimeHeartbeatOpen(context.Background(), db, openReq)
	if err != nil {
		t.Fatalf("heartbeat open: %v", err)
	}
	if open.Result != "accepted" || !open.Generation.Valid {
		t.Fatalf("expected accepted heartbeat open result, got %+v", open)
	}

	ticket := readRuntimeTicketState(t, db, ticketHash)
	if ticket.IdlePolicy != "renewable" {
		t.Fatalf("expected renewable idle policy after heartbeat open, got %+v", ticket)
	}
	if !ticket.IdleRenewOwnerLeaseID.Valid || ticket.IdleRenewOwnerLeaseID.String != grant.LeaseID {
		t.Fatalf("expected heartbeat open to record renewal owner lease id %q, got %+v", grant.LeaseID, ticket)
	}
	if !ticket.IdleRenewOwnerLastHeartbeatAt.Valid || ticket.IdleRenewOwnerLastHeartbeatAt.Int64 <= 0 {
		t.Fatalf("expected heartbeat open to record renewal owner heartbeat, got %+v", ticket)
	}
	if ticket.IdleLeaseExpiresAt <= ticket.IssuedAt || ticket.IdleLeaseExpiresAt > ticket.HardExpireAt {
		t.Fatalf("expected heartbeat open to advance idle lease within hard expiry, got %+v", ticket)
	}
}

func TestRuntimeHeartbeatOpenWithoutTicketRowFailsClosed(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	ticketHash := "heartbeat-disabled-ticket-state-ticket"

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-disabled-ticket-state-host",
		Hostname:          "heartbeat-disabled-ticket-state.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-disabled-ticket-state-request",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-disabled-ticket-state-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-disabled-ticket-state-request", claim.HandoffToken.String, nowMs+2, 7_000); err != nil {
		t.Fatalf("ack handoff: %v", err)
	}

	openReq := HeartbeatOpenRequest{
		RequestID:           "heartbeat-disabled-ticket-state-request",
		LeaseID:             grant.LeaseID,
		LeaseToken:          grant.LeaseToken,
		HardExpireAtMs:      grant.ExpiresAtMs,
		NowMs:               nowMs + 3,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	}
	open := &runtimeHeartbeatResult{}
	err = db.QueryRowContext(context.Background(), `
		SELECT result, reason, generation, deadline_ms, ack_timeout_ms, heartbeat_interval_ms, heartbeat_timeout_ms, reconnect_grace_ms, start_timeout_ms, hard_expire_at_ms
		FROM cq_heartbeat_open($1, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`, openReq.RequestID, openReq.LeaseID, openReq.LeaseToken, ticketHash, openReq.HardExpireAtMs, openReq.NowMs, openReq.HeartbeatTimeoutMs, openReq.AckTimeoutMs, openReq.HeartbeatIntervalMs, openReq.ReconnectGraceMs, openReq.StartTimeoutMs).Scan(
		&open.Result,
		&open.Reason,
		&open.Generation,
		&open.DeadlineMs,
		&open.AckTimeoutMs,
		&open.HeartbeatIntervalMs,
		&open.HeartbeatTimeoutMs,
		&open.ReconnectGraceMs,
		&open.StartTimeoutMs,
		&open.HardExpireAtMs,
	)
	if err != nil {
		t.Fatalf("heartbeat open without seeded ticket row: %v", err)
	}
	if open.Result != "conflict" || open.Reason.String != "ticket_not_found" {
		t.Fatalf("expected missing-row heartbeat open to fail closed with ticket_not_found, got %+v", open)
	}
	if open.Generation.Valid || open.DeadlineMs.Valid {
		t.Fatalf("expected missing-row heartbeat open to avoid connected heartbeat outputs, got %+v", open)
	}

	request := readRuntimeRequestState(t, db, "heartbeat-disabled-ticket-state-request")
	if !request.HeartbeatState.Valid || request.HeartbeatState.String != "none" {
		t.Fatalf("expected missing-row heartbeat open to leave request in non-connected heartbeat state, got %+v", request)
	}
	if !request.HeartbeatGeneration.Valid || request.HeartbeatGeneration.Int64 != 0 {
		t.Fatalf("expected missing-row heartbeat open to preserve pre-connect generation 0, got %+v", request)
	}
	if request.HeartbeatConnectedAtMs.Valid || request.HeartbeatLastAtMs.Valid {
		t.Fatalf("expected missing-row heartbeat open to avoid connected heartbeat timestamps, got %+v", request)
	}

	var ticketRowCount int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		  FROM "DOWNLOAD_TICKET_STATE_TABLE"
		 WHERE "TICKET_HASH" = $1
	`, ticketHash).Scan(&ticketRowCount); err != nil {
		t.Fatalf("count ticket rows for missing-row heartbeat path: %v", err)
	}
	if ticketRowCount != 0 {
		t.Fatalf("expected missing-row heartbeat path to avoid renewal row writes, found %d rows", ticketRowCount)
	}
}

func TestRuntimeHeartbeatRefreshWithoutTicketRowFailsClosed(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	ticketHash := "heartbeat-refresh-missing-ticket-row"

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-refresh-missing-ticket-row-host",
		Hostname:          "heartbeat-refresh-missing-ticket-row.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-refresh-missing-ticket-row-request",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-refresh-missing-ticket-row-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-refresh-missing-ticket-row-request", claim.HandoffToken.String, nowMs+2, 7_000); err != nil {
		t.Fatalf("ack handoff: %v", err)
	}
	seedRuntimeFirstUseTicketRow(t, db, ticketHash, nowMs/1000, grant.ExpiresAtMs/1000, 60)

	openReq := HeartbeatOpenRequest{
		RequestID:           "heartbeat-refresh-missing-ticket-row-request",
		LeaseID:             grant.LeaseID,
		LeaseToken:          grant.LeaseToken,
		HardExpireAtMs:      grant.ExpiresAtMs,
		NowMs:               nowMs + 3,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	}
	setTestStructStringField(&openReq, "TicketHash", ticketHash)
	open, err := execRuntimeHeartbeatOpen(context.Background(), db, openReq)
	if err != nil {
		t.Fatalf("heartbeat open with seeded ticket row: %v", err)
	}
	if open.Result != "accepted" || !open.Generation.Valid {
		t.Fatalf("expected accepted heartbeat open before deleting ticket row, got %+v", open)
	}

	if _, err := db.ExecContext(context.Background(), `
		DELETE FROM "DOWNLOAD_TICKET_STATE_TABLE"
		 WHERE "TICKET_HASH" = $1
	`, ticketHash); err != nil {
		t.Fatalf("delete seeded ticket row before refresh: %v", err)
	}

	refreshReq := HeartbeatRefreshRequest{
		RequestID:          "heartbeat-refresh-missing-ticket-row-request",
		LeaseID:            grant.LeaseID,
		LeaseToken:         grant.LeaseToken,
		Generation:         open.Generation.Int64,
		NowMs:              nowMs + 4,
		HeartbeatTimeoutMs: 15_000,
	}
	setTestStructStringField(&refreshReq, "TicketHash", ticketHash)
	refresh, err := execRuntimeHeartbeatRefresh(context.Background(), db, refreshReq)
	if err != nil {
		t.Fatalf("heartbeat refresh after deleting ticket row: %v", err)
	}
	if refresh.Result != "conflict" || refresh.Reason.String != "ticket_not_found" {
		t.Fatalf("expected missing-row heartbeat refresh to fail closed with ticket_not_found, got %+v", refresh)
	}
	if refresh.Generation.Valid || refresh.DeadlineMs.Valid {
		t.Fatalf("expected missing-row heartbeat refresh to avoid success payload fields, got %+v", refresh)
	}

	var ticketRowCount int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		  FROM "DOWNLOAD_TICKET_STATE_TABLE"
		 WHERE "TICKET_HASH" = $1
	`, ticketHash).Scan(&ticketRowCount); err != nil {
		t.Fatalf("count ticket rows after missing-row refresh path: %v", err)
	}
	if ticketRowCount != 0 {
		t.Fatalf("expected missing-row heartbeat refresh path to avoid renewal row writes, found %d rows", ticketRowCount)
	}
}

func TestRuntimeHeartbeatRefreshByNonOwnerLeavesRenewalRowUntouched(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	ticketHash := "heartbeat-renewable-non-owner-ticket"

	grantA, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-renewable-non-owner-a",
		Hostname:          "heartbeat-renewable-non-owner-a.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-renewable-non-owner-request-a",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire a: %v", err)
	}
	claimA, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-renewable-non-owner-request-a", grantA.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant a: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-renewable-non-owner-request-a", claimA.HandoffToken.String, nowMs+2, 7_000); err != nil {
		t.Fatalf("ack handoff a: %v", err)
	}
	seedRuntimeFirstUseTicketRow(t, db, ticketHash, nowMs/1000, (nowMs+60_000)/1000, 60)

	openReqA := HeartbeatOpenRequest{
		RequestID:           "heartbeat-renewable-non-owner-request-a",
		LeaseID:             grantA.LeaseID,
		LeaseToken:          grantA.LeaseToken,
		HardExpireAtMs:      grantA.ExpiresAtMs,
		NowMs:               nowMs + 3,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	}
	setTestStructStringField(&openReqA, "TicketHash", ticketHash)
	_, err = execRuntimeHeartbeatOpen(context.Background(), db, openReqA)
	if err != nil {
		t.Fatalf("heartbeat open a: %v", err)
	}

	grantB, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-renewable-non-owner-b",
		Hostname:          "heartbeat-renewable-non-owner-b.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-renewable-non-owner-request-b",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs + 5,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire b: %v", err)
	}
	claimB, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-renewable-non-owner-request-b", grantB.ClaimToken.String, nowMs+6)
	if err != nil {
		t.Fatalf("claim grant b: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-renewable-non-owner-request-b", claimB.HandoffToken.String, nowMs+7, 7_000); err != nil {
		t.Fatalf("ack handoff b: %v", err)
	}

	openReqB := HeartbeatOpenRequest{
		RequestID:           "heartbeat-renewable-non-owner-request-b",
		LeaseID:             grantB.LeaseID,
		LeaseToken:          grantB.LeaseToken,
		HardExpireAtMs:      grantB.ExpiresAtMs,
		NowMs:               nowMs + 8,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	}
	setTestStructStringField(&openReqB, "TicketHash", ticketHash)
	openB, err := execRuntimeHeartbeatOpen(context.Background(), db, openReqB)
	if err != nil {
		t.Fatalf("heartbeat open b: %v", err)
	}
	if openB.Result != "accepted" || !openB.Generation.Valid {
		t.Fatalf("expected accepted non-owner heartbeat open, got %+v", openB)
	}
	beforeRefresh := readRuntimeTicketState(t, db, ticketHash)

	refreshReqB := HeartbeatRefreshRequest{
		RequestID:          "heartbeat-renewable-non-owner-request-b",
		LeaseID:            grantB.LeaseID,
		LeaseToken:         grantB.LeaseToken,
		Generation:         openB.Generation.Int64,
		NowMs:              nowMs + 9,
		HeartbeatTimeoutMs: 15_000,
	}
	setTestStructStringField(&refreshReqB, "TicketHash", ticketHash)
	refreshB, err := execRuntimeHeartbeatRefresh(context.Background(), db, refreshReqB)
	if err != nil {
		t.Fatalf("heartbeat refresh b: %v", err)
	}
	if refreshB.Result != "accepted" || refreshB.Generation.Int64 != openB.Generation.Int64 {
		t.Fatalf("expected accepted non-owner heartbeat refresh, got %+v", refreshB)
	}
	afterRefresh := readRuntimeTicketState(t, db, ticketHash)
	if beforeRefresh != afterRefresh {
		t.Fatalf("expected non-owner heartbeat refresh to leave ticket row untouched, before=%+v after=%+v", beforeRefresh, afterRefresh)
	}
	if beforeRefresh.IdleRenewOwnerLeaseID.String != grantA.LeaseID {
		t.Fatalf("expected owner lease to remain request A before takeover, got %+v", beforeRefresh)
	}
}

func TestRuntimeHeartbeatOpenTransfersRenewalOwnershipOnlyAfterPreviousOwnerTurnsStale(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	ticketHash := "heartbeat-renewable-stale-owner-ticket"

	grantA, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-renewable-stale-owner-a",
		Hostname:          "heartbeat-renewable-stale-owner-a.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-renewable-stale-owner-request-a",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire a: %v", err)
	}
	claimA, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-renewable-stale-owner-request-a", grantA.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant a: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-renewable-stale-owner-request-a", claimA.HandoffToken.String, nowMs+2, 7_000); err != nil {
		t.Fatalf("ack handoff a: %v", err)
	}
	seedRuntimeFirstUseTicketRow(t, db, ticketHash, nowMs/1000, (nowMs+60_000)/1000, 60)

	openReqA := HeartbeatOpenRequest{
		RequestID:           "heartbeat-renewable-stale-owner-request-a",
		LeaseID:             grantA.LeaseID,
		LeaseToken:          grantA.LeaseToken,
		HardExpireAtMs:      grantA.ExpiresAtMs,
		NowMs:               nowMs + 3,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	}
	setTestStructStringField(&openReqA, "TicketHash", ticketHash)
	openA, err := execRuntimeHeartbeatOpen(context.Background(), db, openReqA)
	if err != nil {
		t.Fatalf("heartbeat open a: %v", err)
	}
	disconnectA, err := execRuntimeHeartbeatDisconnect(context.Background(), db, HeartbeatDisconnectRequest{
		RequestID:        "heartbeat-renewable-stale-owner-request-a",
		LeaseID:          grantA.LeaseID,
		LeaseToken:       grantA.LeaseToken,
		Generation:       openA.Generation.Int64,
		NowMs:            nowMs + 20,
		ReconnectGraceMs: 12_000,
	})
	if err != nil {
		t.Fatalf("heartbeat disconnect a: %v", err)
	}
	beforeStale := readRuntimeTicketState(t, db, ticketHash)

	grantB, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-renewable-stale-owner-b",
		Hostname:          "heartbeat-renewable-stale-owner-b.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-renewable-stale-owner-request-b",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs + 25,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire b: %v", err)
	}
	claimB, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-renewable-stale-owner-request-b", grantB.ClaimToken.String, nowMs+26)
	if err != nil {
		t.Fatalf("claim grant b: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-renewable-stale-owner-request-b", claimB.HandoffToken.String, nowMs+27, 7_000); err != nil {
		t.Fatalf("ack handoff b: %v", err)
	}

	openReqB := HeartbeatOpenRequest{
		RequestID:           "heartbeat-renewable-stale-owner-request-b",
		LeaseID:             grantB.LeaseID,
		LeaseToken:          grantB.LeaseToken,
		HardExpireAtMs:      grantB.ExpiresAtMs,
		NowMs:               nowMs + 30,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	}
	setTestStructStringField(&openReqB, "TicketHash", ticketHash)
	beforeStaleOpen, err := execRuntimeHeartbeatOpen(context.Background(), db, openReqB)
	if err != nil {
		t.Fatalf("heartbeat open b before stale cutoff: %v", err)
	}
	if beforeStaleOpen.Result != "accepted" {
		t.Fatalf("expected accepted heartbeat open for non-owner before stale cutoff, got %+v", beforeStaleOpen)
	}
	if got := readRuntimeTicketState(t, db, ticketHash); got != beforeStale {
		t.Fatalf("expected non-stale non-owner open to leave ticket row untouched, before=%+v after=%+v", beforeStale, got)
	}

	refreshReqB := HeartbeatRefreshRequest{
		RequestID:          "heartbeat-renewable-stale-owner-request-b",
		LeaseID:            grantB.LeaseID,
		LeaseToken:         grantB.LeaseToken,
		Generation:         beforeStaleOpen.Generation.Int64,
		NowMs:              disconnectA.DeadlineMs.Int64 + 1,
		HeartbeatTimeoutMs: 15_000,
	}
	setTestStructStringField(&refreshReqB, "TicketHash", ticketHash)
	afterStaleOpen, err := execRuntimeHeartbeatRefresh(context.Background(), db, refreshReqB)
	if err != nil {
		t.Fatalf("heartbeat refresh b after stale cutoff: %v", err)
	}
	if afterStaleOpen.Result != "accepted" {
		t.Fatalf("expected accepted heartbeat refresh after stale cutoff, got %+v", afterStaleOpen)
	}
	updated := readRuntimeTicketState(t, db, ticketHash)
	if !updated.IdleRenewOwnerLeaseID.Valid || updated.IdleRenewOwnerLeaseID.String != grantB.LeaseID {
		t.Fatalf("expected stale takeover to replace renewal owner with lease B, got %+v", updated)
	}
	if updated.IdleLeaseExpiresAt < beforeStale.IdleLeaseExpiresAt {
		t.Fatalf("expected stale takeover to not shorten idle lease expiry, before=%+v after=%+v", beforeStale, updated)
	}
}

func TestRuntimeHeartbeatOpenBlocksTerminalOwnerTakeoverUntilPreservedHeartbeatDeadlineExpires(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	ticketHash := "heartbeat-renewable-terminal-owner-ticket"

	grantA, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-renewable-terminal-owner-a",
		Hostname:          "heartbeat-renewable-terminal-owner-a.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-renewable-terminal-owner-request-a",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire a: %v", err)
	}
	claimA, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-renewable-terminal-owner-request-a", grantA.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant a: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-renewable-terminal-owner-request-a", claimA.HandoffToken.String, nowMs+2, 7_000); err != nil {
		t.Fatalf("ack handoff a: %v", err)
	}
	seedRuntimeFirstUseTicketRow(t, db, ticketHash, nowMs/1000, grantA.ExpiresAtMs/1000, 60)

	openReqA := HeartbeatOpenRequest{
		RequestID:           "heartbeat-renewable-terminal-owner-request-a",
		LeaseID:             grantA.LeaseID,
		LeaseToken:          grantA.LeaseToken,
		HardExpireAtMs:      grantA.ExpiresAtMs,
		NowMs:               nowMs + 3,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	}
	setTestStructStringField(&openReqA, "TicketHash", ticketHash)
	openA, err := execRuntimeHeartbeatOpen(context.Background(), db, openReqA)
	if err != nil {
		t.Fatalf("heartbeat open a: %v", err)
	}
	var releaseResult string
	var releaseReason, releaseRequestID sql.NullString
	if err := db.QueryRowContext(context.Background(), `
		SELECT result, reason, request_id
		FROM cq_release($1::uuid, $2, $3, $4)
	`, grantA.LeaseID, grantA.LeaseToken, "stream_complete", nowMs+10).Scan(&releaseResult, &releaseReason, &releaseRequestID); err != nil {
		t.Fatalf("release terminal renewal owner: %v", err)
	}
	if releaseResult != "released" || releaseRequestID.String != "heartbeat-renewable-terminal-owner-request-a" {
		t.Fatalf("expected release of owner request A, got result=%q reason=%q request=%q", releaseResult, releaseReason.String, releaseRequestID.String)
	}
	ownerAfterRelease := readRuntimeRequestState(t, db, "heartbeat-renewable-terminal-owner-request-a")
	if ownerAfterRelease.State != "released" {
		t.Fatalf("expected request A to be terminal before takeover, got %+v", ownerAfterRelease)
	}
	if !ownerAfterRelease.HeartbeatDeadlineMs.Valid || ownerAfterRelease.HeartbeatDeadlineMs.Int64 != openA.DeadlineMs.Int64 {
		t.Fatalf("expected terminal owner to preserve heartbeat deadline %d, got %+v", openA.DeadlineMs.Int64, ownerAfterRelease)
	}

	grantB, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-renewable-terminal-owner-b",
		Hostname:          "heartbeat-renewable-terminal-owner-b.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-renewable-terminal-owner-request-b",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs + 20,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire b: %v", err)
	}
	claimB, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-renewable-terminal-owner-request-b", grantB.ClaimToken.String, nowMs+21)
	if err != nil {
		t.Fatalf("claim grant b: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-renewable-terminal-owner-request-b", claimB.HandoffToken.String, nowMs+22, 7_000); err != nil {
		t.Fatalf("ack handoff b: %v", err)
	}

	openReqB := HeartbeatOpenRequest{
		RequestID:           "heartbeat-renewable-terminal-owner-request-b",
		LeaseID:             grantB.LeaseID,
		LeaseToken:          grantB.LeaseToken,
		HardExpireAtMs:      grantB.ExpiresAtMs,
		NowMs:               nowMs + 23,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	}
	setTestStructStringField(&openReqB, "TicketHash", ticketHash)
	openB, err := execRuntimeHeartbeatOpen(context.Background(), db, openReqB)
	if err != nil {
		t.Fatalf("heartbeat open b after terminal owner release: %v", err)
	}
	if openB.Result != "accepted" {
		t.Fatalf("expected accepted heartbeat open after terminal owner release, got %+v", openB)
	}

	beforeStale := readRuntimeTicketState(t, db, ticketHash)
	if !beforeStale.IdleRenewOwnerLeaseID.Valid || beforeStale.IdleRenewOwnerLeaseID.String != grantA.LeaseID {
		t.Fatalf("expected released owner lease A to keep renewal ownership until stale cutoff, got %+v", beforeStale)
	}

	refreshReqB := HeartbeatRefreshRequest{
		RequestID:          "heartbeat-renewable-terminal-owner-request-b",
		LeaseID:            grantB.LeaseID,
		LeaseToken:         grantB.LeaseToken,
		Generation:         openB.Generation.Int64,
		NowMs:              openA.DeadlineMs.Int64 + 1,
		HeartbeatTimeoutMs: 15_000,
	}
	setTestStructStringField(&refreshReqB, "TicketHash", ticketHash)
	refreshB, err := execRuntimeHeartbeatRefresh(context.Background(), db, refreshReqB)
	if err != nil {
		t.Fatalf("heartbeat refresh b after terminal owner stale cutoff: %v", err)
	}
	if refreshB.Result != "accepted" {
		t.Fatalf("expected accepted heartbeat refresh after terminal owner stale cutoff, got %+v", refreshB)
	}

	afterStale := readRuntimeTicketState(t, db, ticketHash)
	if !afterStale.IdleRenewOwnerLeaseID.Valid || afterStale.IdleRenewOwnerLeaseID.String != grantB.LeaseID {
		t.Fatalf("expected takeover after preserved heartbeat deadline expiry to move renewal owner to lease B, got %+v", afterStale)
	}
	if !afterStale.IdleRenewOwnerLastHeartbeatAt.Valid || afterStale.IdleRenewOwnerLastHeartbeatAt.Int64 < refreshReqB.NowMs/1000 {
		t.Fatalf("expected takeover after preserved heartbeat deadline expiry to refresh last heartbeat timestamp, got %+v", afterStale)
	}
	if afterStale.IdleLeaseExpiresAt < beforeStale.IdleLeaseExpiresAt {
		t.Fatalf("expected takeover after preserved heartbeat deadline expiry to avoid shortening lease expiry, before=%+v after=%+v", beforeStale, afterStale)
	}
}

func TestRuntimeHeartbeatOpenUsesCustomTicketStateTableForRenewal(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	ticketTableName := "CUSTOM_DOWNLOAD_TICKET_STATE_TABLE"
	ticketHash := "heartbeat-custom-ticket-table-ticket"

	createRuntimeTicketStateTableClone(t, db, ticketTableName)

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-custom-ticket-table-host",
		Hostname:          "heartbeat-custom-ticket-table.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-custom-ticket-table-request",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-custom-ticket-table-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-custom-ticket-table-request", claim.HandoffToken.String, nowMs+2, 7_000); err != nil {
		t.Fatalf("ack handoff: %v", err)
	}
	seedRuntimeFirstUseTicketRowInTable(t, db, ticketTableName, ticketHash, nowMs/1000, grant.ExpiresAtMs/1000, 60)

	openReq := HeartbeatOpenRequest{
		RequestID:           "heartbeat-custom-ticket-table-request",
		LeaseID:             grant.LeaseID,
		LeaseToken:          grant.LeaseToken,
		HardExpireAtMs:      grant.ExpiresAtMs,
		NowMs:               nowMs + 3,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	}
	setTestStructStringField(&openReq, "TicketHash", ticketHash)
	open, err := execRuntimeHeartbeatOpenForTicketTable(context.Background(), db, openReq, ticketTableName)
	if err != nil {
		t.Fatalf("heartbeat open against custom ticket table: %v", err)
	}
	if open.Result != "accepted" {
		t.Fatalf("expected accepted heartbeat open against custom ticket table, got %+v", open)
	}

	ticket := readRuntimeTicketStateFromTable(t, db, ticketHash, ticketTableName)
	if ticket.IdlePolicy != "renewable" {
		t.Fatalf("expected custom ticket table row promoted to renewable, got %+v", ticket)
	}
	if !ticket.IdleRenewOwnerLeaseID.Valid || ticket.IdleRenewOwnerLeaseID.String != grant.LeaseID {
		t.Fatalf("expected custom ticket table row to record renewal owner lease id %q, got %+v", grant.LeaseID, ticket)
	}

	var defaultTableCount int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		  FROM "DOWNLOAD_TICKET_STATE_TABLE"
		 WHERE "TICKET_HASH" = $1
	`, ticketHash).Scan(&defaultTableCount); err != nil {
		t.Fatalf("count default ticket rows: %v", err)
	}
	if defaultTableCount != 0 {
		t.Fatalf("expected renewal to avoid default ticket table writes for custom table flow, found %d rows", defaultTableCount)
	}
}

func TestRuntimeHeartbeatOpenWithZeroIdleTimeoutStaysFirstUse(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()
	ticketHash := "heartbeat-zero-timeout-ticket"

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-zero-timeout-host",
		Hostname:          "heartbeat-zero-timeout.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-zero-timeout-request",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-zero-timeout-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-zero-timeout-request", claim.HandoffToken.String, nowMs+2, 7_000); err != nil {
		t.Fatalf("ack handoff: %v", err)
	}
	seedRuntimeFirstUseTicketRow(t, db, ticketHash, nowMs/1000, grant.ExpiresAtMs/1000, 0)

	openReq := HeartbeatOpenRequest{
		RequestID:           "heartbeat-zero-timeout-request",
		LeaseID:             grant.LeaseID,
		LeaseToken:          grant.LeaseToken,
		HardExpireAtMs:      grant.ExpiresAtMs,
		NowMs:               nowMs + 3,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	}
	setTestStructStringField(&openReq, "TicketHash", ticketHash)
	open, err := execRuntimeHeartbeatOpen(context.Background(), db, openReq)
	if err != nil {
		t.Fatalf("heartbeat open: %v", err)
	}
	if open.Result != "accepted" || !open.Generation.Valid {
		t.Fatalf("expected accepted heartbeat open result, got %+v", open)
	}
	ticket := readRuntimeTicketState(t, db, ticketHash)
	if ticket.IdlePolicy != "first_use" {
		t.Fatalf("expected zero-timeout heartbeat to remain first_use, got %+v", ticket)
	}
	if ticket.IdleRenewOwnerLeaseID.Valid || ticket.IdleRenewOwnerLastHeartbeatAt.Valid {
		t.Fatalf("expected zero-timeout heartbeat to skip owner-renew metadata, got %+v", ticket)
	}
}

func TestRuntimeHeartbeatReleasedReplayUsesAlreadyReleasedForWorkerReleaseReasons(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	workerReleaseReasons := []string{"stream_complete", "client_disconnect", "heartbeat_connect_failed", "heartbeat_lost"}
	for index, releaseReason := range workerReleaseReasons {
		t.Run(releaseReason, func(t *testing.T) {
			caseNowMs := nowMs + int64(index*1_000)
			requestID := "heartbeat-replay-" + strings.ReplaceAll(releaseReason, "_", "-")
			grant, open := seedRuntimeConnectedHeartbeat(t, db, requestID, "heartbeat-replay-"+strings.ReplaceAll(releaseReason, "_", "-"), caseNowMs, 15_000)

			var releaseResult string
			var releaseReplayReason, releaseRequestID sql.NullString
			if err := db.QueryRowContext(context.Background(), `
				SELECT result, reason, request_id
				FROM cq_release($1::uuid, $2, $3, $4)
			`, grant.LeaseID, grant.LeaseToken, releaseReason, caseNowMs+4).Scan(&releaseResult, &releaseReplayReason, &releaseRequestID); err != nil {
				t.Fatalf("release active heartbeat lease: %v", err)
			}
			if releaseResult != "released" || releaseRequestID.String != requestID {
				t.Fatalf("expected release to persist %q for %q, got result=%q reason=%q request=%q", releaseReason, requestID, releaseResult, releaseReplayReason.String, releaseRequestID.String)
			}

			request := readRuntimeRequestState(t, db, requestID)
			if request.State != "released" || request.TerminalReason.String != releaseReason || request.HeartbeatTerminalReason.String != releaseReason {
				t.Fatalf("expected HTTP release to keep persisted worker reason %q, got %+v", releaseReason, request)
			}

			assertRuntimeHeartbeatReleasedReplayReason(t, db, requestID, grant, open.Generation.Int64, caseNowMs+5, "already_released")
		})
	}
}

func TestRuntimeHeartbeatReleasedReplayPreservesHeartbeatNativeReasons(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	startGrant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-replay-preserve-start-host",
		Hostname:          "heartbeat-replay-preserve-start.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-replay-preserve-start",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire for start timeout replay: %v", err)
	}
	startClaim, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-replay-preserve-start", startGrant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant for start timeout replay: %v", err)
	}
	startAck, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-replay-preserve-start", startClaim.HandoffToken.String, nowMs+2, 25)
	if err != nil {
		t.Fatalf("ack handoff for start timeout replay: %v", err)
	}
	var startExpired bool
	if err := db.QueryRowContext(context.Background(), `
		SELECT cq_expire_active_request_if_due($1, $2)
	`, "heartbeat-replay-preserve-start", startAck.HeartbeatDeadlineMs.Int64).Scan(&startExpired); err != nil {
		t.Fatalf("expire heartbeat start timeout: %v", err)
	}
	if !startExpired {
		t.Fatal("expected heartbeat start timeout to transition request")
	}
	assertRuntimeHeartbeatReleasedReplayReason(t, db, "heartbeat-replay-preserve-start", startGrant, 1, nowMs+5, "heartbeat_start_timeout")

	timeoutGrant, timeoutOpen := seedRuntimeConnectedHeartbeat(t, db, "heartbeat-replay-preserve-timeout", "heartbeat-replay-preserve-timeout-host", nowMs+1_000, 25)
	expired, err := execRuntimeExpireHeartbeatIfDue(context.Background(), db, "heartbeat-replay-preserve-timeout", timeoutOpen.DeadlineMs.Int64)
	if err != nil {
		t.Fatalf("expire heartbeat timeout: %v", err)
	}
	if expired.Result != "released" || expired.Reason.String != "heartbeat_timeout" {
		t.Fatalf("expected heartbeat_timeout release, got %+v", expired)
	}
	assertRuntimeHeartbeatReleasedReplayReason(t, db, "heartbeat-replay-preserve-timeout", timeoutGrant, timeoutOpen.Generation.Int64, timeoutOpen.DeadlineMs.Int64+1, "heartbeat_timeout")
}

func TestRuntimeHeartbeatRefreshUpdatesCurrentGenerationDeadline(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-refresh-host",
		Hostname:          "heartbeat-refresh.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-refresh-request",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-refresh-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-refresh-request", claim.HandoffToken.String, nowMs+2, 7000); err != nil {
		t.Fatalf("ack handoff: %v", err)
	}
	open, err := execRuntimeHeartbeatOpen(context.Background(), db, HeartbeatOpenRequest{
		RequestID:           "heartbeat-refresh-request",
		LeaseID:             grant.LeaseID,
		LeaseToken:          grant.LeaseToken,
		HardExpireAtMs:      grant.ExpiresAtMs,
		NowMs:               nowMs + 3,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	})
	if err != nil {
		t.Fatalf("heartbeat open: %v", err)
	}

	refresh, err := execRuntimeHeartbeatRefresh(context.Background(), db, HeartbeatRefreshRequest{
		RequestID:          "heartbeat-refresh-request",
		LeaseID:            grant.LeaseID,
		LeaseToken:         grant.LeaseToken,
		Generation:         open.Generation.Int64,
		NowMs:              nowMs + 20,
		HeartbeatTimeoutMs: 15_000,
	})
	if err != nil {
		t.Fatalf("heartbeat refresh: %v", err)
	}
	if refresh.Result != "accepted" || refresh.Generation.Int64 != open.Generation.Int64 {
		t.Fatalf("expected accepted heartbeat refresh result, got %+v", refresh)
	}
	if refresh.DeadlineMs.Int64 != nowMs+20+15_000 {
		t.Fatalf("expected refreshed heartbeat deadline %d, got %+v", nowMs+20+15_000, refresh)
	}

	request := readRuntimeRequestState(t, db, "heartbeat-refresh-request")
	if request.HeartbeatState.String != "connected" || request.HeartbeatLastAtMs.Int64 != nowMs+20 || request.HeartbeatDeadlineMs.Int64 != nowMs+20+15_000 {
		t.Fatalf("expected refreshed connected heartbeat state, got %+v", request)
	}
}

func TestRuntimeHeartbeatDisconnectMovesCurrentGenerationToGrace(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-disconnect-host",
		Hostname:          "heartbeat-disconnect.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-disconnect-request",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-disconnect-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-disconnect-request", claim.HandoffToken.String, nowMs+2, 7000); err != nil {
		t.Fatalf("ack handoff: %v", err)
	}
	open, err := execRuntimeHeartbeatOpen(context.Background(), db, HeartbeatOpenRequest{
		RequestID:           "heartbeat-disconnect-request",
		LeaseID:             grant.LeaseID,
		LeaseToken:          grant.LeaseToken,
		HardExpireAtMs:      grant.ExpiresAtMs,
		NowMs:               nowMs + 3,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	})
	if err != nil {
		t.Fatalf("heartbeat open: %v", err)
	}

	disconnectNow := nowMs + 50
	disconnect, err := execRuntimeHeartbeatDisconnect(context.Background(), db, HeartbeatDisconnectRequest{
		RequestID:        "heartbeat-disconnect-request",
		LeaseID:          grant.LeaseID,
		LeaseToken:       grant.LeaseToken,
		Generation:       open.Generation.Int64,
		NowMs:            disconnectNow,
		ReconnectGraceMs: 12_000,
	})
	if err != nil {
		t.Fatalf("heartbeat disconnect: %v", err)
	}
	if disconnect.Result != "accepted" || disconnect.Generation.Int64 != open.Generation.Int64 {
		t.Fatalf("expected accepted heartbeat disconnect result, got %+v", disconnect)
	}

	request := readRuntimeRequestState(t, db, "heartbeat-disconnect-request")
	expectedGraceUntil := disconnectNow + 12_000
	if request.HeartbeatState.String != "grace" || request.HeartbeatDisconnectedAtMs.Int64 != disconnectNow || request.HeartbeatGraceUntilMs.Int64 != expectedGraceUntil {
		t.Fatalf("expected grace heartbeat state after disconnect, got %+v", request)
	}
	if request.HeartbeatDeadlineMs.Int64 != expectedGraceUntil {
		t.Fatalf("expected grace heartbeat deadline %d, got %+v", expectedGraceUntil, request)
	}
}

func TestRuntimeExpireHeartbeatIfDueReleasesConnectedHeartbeatTimeout(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-expire-host",
		Hostname:          "heartbeat-expire.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-expire-request",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-expire-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-expire-request", claim.HandoffToken.String, nowMs+2, 7000); err != nil {
		t.Fatalf("ack handoff: %v", err)
	}
	open, err := execRuntimeHeartbeatOpen(context.Background(), db, HeartbeatOpenRequest{
		RequestID:           "heartbeat-expire-request",
		LeaseID:             grant.LeaseID,
		LeaseToken:          grant.LeaseToken,
		HardExpireAtMs:      grant.ExpiresAtMs,
		NowMs:               nowMs + 3,
		HeartbeatTimeoutMs:  25,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	})
	if err != nil {
		t.Fatalf("heartbeat open: %v", err)
	}

	expired, err := execRuntimeExpireHeartbeatIfDue(context.Background(), db, "heartbeat-expire-request", open.DeadlineMs.Int64)
	if err != nil {
		t.Fatalf("expire heartbeat if due: %v", err)
	}
	if expired.Result != "released" || expired.Reason.String != "heartbeat_timeout" {
		t.Fatalf("expected heartbeat timeout terminal release, got %+v", expired)
	}

	request := readRuntimeRequestState(t, db, "heartbeat-expire-request")
	if request.State != "released" || request.TerminalReason.String != "heartbeat_timeout" {
		t.Fatalf("expected heartbeat timeout persisted terminal state, got %+v", request)
	}
}

func TestRuntimeHeartbeatReplacementMakesPreviousGenerationStale(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-replacement-host",
		Hostname:          "heartbeat-replacement.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-replacement-request",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-replacement-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-replacement-request", claim.HandoffToken.String, nowMs+2, 7000); err != nil {
		t.Fatalf("ack handoff: %v", err)
	}
	firstOpen, err := execRuntimeHeartbeatOpen(context.Background(), db, HeartbeatOpenRequest{
		RequestID:           "heartbeat-replacement-request",
		LeaseID:             grant.LeaseID,
		LeaseToken:          grant.LeaseToken,
		HardExpireAtMs:      grant.ExpiresAtMs,
		NowMs:               nowMs + 3,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	})
	if err != nil {
		t.Fatalf("first heartbeat open: %v", err)
	}
	secondOpen, err := execRuntimeHeartbeatOpen(context.Background(), db, HeartbeatOpenRequest{
		RequestID:           "heartbeat-replacement-request",
		LeaseID:             grant.LeaseID,
		LeaseToken:          grant.LeaseToken,
		HardExpireAtMs:      grant.ExpiresAtMs,
		NowMs:               nowMs + 4,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	})
	if err != nil {
		t.Fatalf("second heartbeat open: %v", err)
	}
	if firstOpen.Generation.Int64 != 1 || secondOpen.Generation.Int64 != 2 {
		t.Fatalf("expected replacement generations 1 then 2, got first=%+v second=%+v", firstOpen, secondOpen)
	}

	refresh, err := execRuntimeHeartbeatRefresh(context.Background(), db, HeartbeatRefreshRequest{
		RequestID:          "heartbeat-replacement-request",
		LeaseID:            grant.LeaseID,
		LeaseToken:         grant.LeaseToken,
		Generation:         firstOpen.Generation.Int64,
		NowMs:              nowMs + 5,
		HeartbeatTimeoutMs: 15_000,
	})
	if err != nil {
		t.Fatalf("stale heartbeat refresh: %v", err)
	}
	if refresh.Result != "noop" || refresh.Reason.String != "stale_generation" {
		t.Fatalf("expected stale heartbeat refresh noop, got %+v", refresh)
	}

	disconnect, err := execRuntimeHeartbeatDisconnect(context.Background(), db, HeartbeatDisconnectRequest{
		RequestID:        "heartbeat-replacement-request",
		LeaseID:          grant.LeaseID,
		LeaseToken:       grant.LeaseToken,
		Generation:       firstOpen.Generation.Int64,
		NowMs:            nowMs + 6,
		ReconnectGraceMs: 12_000,
	})
	if err != nil {
		t.Fatalf("stale heartbeat disconnect: %v", err)
	}
	if disconnect.Result != "noop" || disconnect.Reason.String != "stale_generation" {
		t.Fatalf("expected stale heartbeat disconnect noop, got %+v", disconnect)
	}

	request := readRuntimeRequestState(t, db, "heartbeat-replacement-request")
	if request.HeartbeatState.String != "connected" || request.HeartbeatGeneration.Int64 != secondOpen.Generation.Int64 || request.HeartbeatDeadlineMs.Int64 != secondOpen.DeadlineMs.Int64 {
		t.Fatalf("expected replacement to keep current generation connected state, got %+v", request)
	}
	if request.HeartbeatDisconnectedAtMs.Valid {
		t.Fatalf("expected stale generation close not to persist disconnect timestamp, got %+v", request)
	}
}

func TestRuntimeHeartbeatReconnectDuringGraceIncrementsGeneration(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-grace-reconnect-host",
		Hostname:          "heartbeat-grace-reconnect.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-grace-reconnect-request",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-grace-reconnect-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-grace-reconnect-request", claim.HandoffToken.String, nowMs+2, 7000); err != nil {
		t.Fatalf("ack handoff: %v", err)
	}
	open, err := execRuntimeHeartbeatOpen(context.Background(), db, HeartbeatOpenRequest{
		RequestID:           "heartbeat-grace-reconnect-request",
		LeaseID:             grant.LeaseID,
		LeaseToken:          grant.LeaseToken,
		HardExpireAtMs:      grant.ExpiresAtMs,
		NowMs:               nowMs + 3,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	})
	if err != nil {
		t.Fatalf("heartbeat open: %v", err)
	}
	disconnect, err := execRuntimeHeartbeatDisconnect(context.Background(), db, HeartbeatDisconnectRequest{
		RequestID:        "heartbeat-grace-reconnect-request",
		LeaseID:          grant.LeaseID,
		LeaseToken:       grant.LeaseToken,
		Generation:       open.Generation.Int64,
		NowMs:            nowMs + 25,
		ReconnectGraceMs: 12_000,
	})
	if err != nil {
		t.Fatalf("heartbeat disconnect: %v", err)
	}
	reconnect, err := execRuntimeHeartbeatOpen(context.Background(), db, HeartbeatOpenRequest{
		RequestID:           "heartbeat-grace-reconnect-request",
		LeaseID:             grant.LeaseID,
		LeaseToken:          grant.LeaseToken,
		HardExpireAtMs:      grant.ExpiresAtMs,
		NowMs:               disconnect.DeadlineMs.Int64 - 1,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	})
	if err != nil {
		t.Fatalf("grace reconnect heartbeat open: %v", err)
	}
	if reconnect.Result != "accepted" || reconnect.Generation.Int64 != 2 {
		t.Fatalf("expected grace reconnect generation 2, got %+v", reconnect)
	}
	request := readRuntimeRequestState(t, db, "heartbeat-grace-reconnect-request")
	if request.HeartbeatState.String != "connected" || request.HeartbeatGeneration.Int64 != 2 {
		t.Fatalf("expected grace reconnect to restore connected generation 2, got %+v", request)
	}
}

func TestRuntimeHeartbeatReconnectAfterGraceDeadlineIsRefused(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-late-reconnect-host",
		Hostname:          "heartbeat-late-reconnect.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-late-reconnect-request",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-late-reconnect-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-late-reconnect-request", claim.HandoffToken.String, nowMs+2, 7000); err != nil {
		t.Fatalf("ack handoff: %v", err)
	}
	open, err := execRuntimeHeartbeatOpen(context.Background(), db, HeartbeatOpenRequest{
		RequestID:           "heartbeat-late-reconnect-request",
		LeaseID:             grant.LeaseID,
		LeaseToken:          grant.LeaseToken,
		HardExpireAtMs:      grant.ExpiresAtMs,
		NowMs:               nowMs + 3,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    25,
		StartTimeoutMs:      7_000,
	})
	if err != nil {
		t.Fatalf("heartbeat open: %v", err)
	}
	disconnect, err := execRuntimeHeartbeatDisconnect(context.Background(), db, HeartbeatDisconnectRequest{
		RequestID:        "heartbeat-late-reconnect-request",
		LeaseID:          grant.LeaseID,
		LeaseToken:       grant.LeaseToken,
		Generation:       open.Generation.Int64,
		NowMs:            nowMs + 10,
		ReconnectGraceMs: 25,
	})
	if err != nil {
		t.Fatalf("heartbeat disconnect: %v", err)
	}
	reconnect, err := execRuntimeHeartbeatOpen(context.Background(), db, HeartbeatOpenRequest{
		RequestID:           "heartbeat-late-reconnect-request",
		LeaseID:             grant.LeaseID,
		LeaseToken:          grant.LeaseToken,
		HardExpireAtMs:      grant.ExpiresAtMs,
		NowMs:               disconnect.DeadlineMs.Int64 + 1,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    25,
		StartTimeoutMs:      7_000,
	})
	if err != nil {
		t.Fatalf("late reconnect heartbeat open: %v", err)
	}
	if reconnect.Result != "released" || reconnect.Reason.String != "heartbeat_timeout" {
		t.Fatalf("expected late reconnect refusal heartbeat_timeout, got %+v", reconnect)
	}
	request := readRuntimeRequestState(t, db, "heartbeat-late-reconnect-request")
	if request.State != "released" || request.TerminalReason.String != "heartbeat_timeout" {
		t.Fatalf("expected late reconnect not to revive released request, got %+v", request)
	}
}

func TestRuntimeReleaseDuringConnectedHeartbeatClearsHeartbeatState(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-release-connected-host",
		Hostname:          "heartbeat-release-connected.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-release-connected-request",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-release-connected-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-release-connected-request", claim.HandoffToken.String, nowMs+2, 7000); err != nil {
		t.Fatalf("ack handoff: %v", err)
	}
	open, err := execRuntimeHeartbeatOpen(context.Background(), db, HeartbeatOpenRequest{
		RequestID:           "heartbeat-release-connected-request",
		LeaseID:             grant.LeaseID,
		LeaseToken:          grant.LeaseToken,
		HardExpireAtMs:      grant.ExpiresAtMs,
		NowMs:               nowMs + 3,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	})
	if err != nil {
		t.Fatalf("heartbeat open: %v", err)
	}
	var releaseResult string
	var releaseReason, releaseRequestID sql.NullString
	if err := db.QueryRowContext(context.Background(), `
		SELECT result, reason, request_id
		FROM cq_release($1::uuid, $2, $3, $4)
	`, grant.LeaseID, grant.LeaseToken, "stream_complete", nowMs+4).Scan(&releaseResult, &releaseReason, &releaseRequestID); err != nil {
		t.Fatalf("release connected heartbeat lease: %v", err)
	}
	if releaseResult != "released" || releaseRequestID.String != "heartbeat-release-connected-request" {
		t.Fatalf("expected connected heartbeat release, got result=%q reason=%q request=%q", releaseResult, releaseReason.String, releaseRequestID.String)
	}
	request := readRuntimeRequestState(t, db, "heartbeat-release-connected-request")
	if request.State != "released" || request.HeartbeatState.String != "none" {
		t.Fatalf("expected connected heartbeat release cleanup, got %+v", request)
	}
	if !request.HeartbeatDeadlineMs.Valid || request.HeartbeatDeadlineMs.Int64 != open.DeadlineMs.Int64 {
		t.Fatalf("expected connected heartbeat release to preserve deadline %d, got %+v", open.DeadlineMs.Int64, request)
	}
	if request.HeartbeatGraceUntilMs.Valid {
		t.Fatalf("expected connected heartbeat release to keep grace empty, got %+v", request)
	}
	if !request.HeartbeatTerminalReason.Valid || request.HeartbeatTerminalReason.String != request.TerminalReason.String {
		t.Fatalf("expected connected heartbeat release to mirror terminal reason into heartbeat cleanup, got %+v", request)
	}
	if request.HeartbeatGeneration.Int64 != open.Generation.Int64 || request.HeartbeatConnectedAtMs.Int64 != nowMs+3 || request.HeartbeatLastAtMs.Int64 != nowMs+3 {
		t.Fatalf("expected connected heartbeat release to preserve generation and audit timestamps, got %+v", request)
	}
}

func TestRuntimeReleaseDuringGraceHeartbeatClearsHeartbeatState(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "heartbeat-release-grace-host",
		Hostname:          "heartbeat-release-grace.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "heartbeat-release-grace-request",
		HardExpireMs:      nowMs + 60_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "heartbeat-release-grace-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}
	if _, err := execRuntimeAckHandoff(context.Background(), db, "heartbeat-release-grace-request", claim.HandoffToken.String, nowMs+2, 7000); err != nil {
		t.Fatalf("ack handoff: %v", err)
	}
	open, err := execRuntimeHeartbeatOpen(context.Background(), db, HeartbeatOpenRequest{
		RequestID:           "heartbeat-release-grace-request",
		LeaseID:             grant.LeaseID,
		LeaseToken:          grant.LeaseToken,
		HardExpireAtMs:      grant.ExpiresAtMs,
		NowMs:               nowMs + 3,
		HeartbeatTimeoutMs:  15_000,
		AckTimeoutMs:        2_000,
		HeartbeatIntervalMs: 5_000,
		ReconnectGraceMs:    12_000,
		StartTimeoutMs:      7_000,
	})
	if err != nil {
		t.Fatalf("heartbeat open: %v", err)
	}
	disconnect, err := execRuntimeHeartbeatDisconnect(context.Background(), db, HeartbeatDisconnectRequest{
		RequestID:        "heartbeat-release-grace-request",
		LeaseID:          grant.LeaseID,
		LeaseToken:       grant.LeaseToken,
		Generation:       open.Generation.Int64,
		NowMs:            nowMs + 20,
		ReconnectGraceMs: 12_000,
	})
	if err != nil {
		t.Fatalf("heartbeat disconnect: %v", err)
	}
	var releaseResult string
	var releaseReason, releaseRequestID sql.NullString
	if err := db.QueryRowContext(context.Background(), `
		SELECT result, reason, request_id
		FROM cq_release($1::uuid, $2, $3, $4)
	`, grant.LeaseID, grant.LeaseToken, "stream_complete", nowMs+21).Scan(&releaseResult, &releaseReason, &releaseRequestID); err != nil {
		t.Fatalf("release grace heartbeat lease: %v", err)
	}
	if releaseResult != "released" || releaseRequestID.String != "heartbeat-release-grace-request" {
		t.Fatalf("expected grace heartbeat release, got result=%q reason=%q request=%q", releaseResult, releaseReason.String, releaseRequestID.String)
	}
	request := readRuntimeRequestState(t, db, "heartbeat-release-grace-request")
	if request.State != "released" || request.HeartbeatState.String != "none" {
		t.Fatalf("expected grace heartbeat release cleanup, got %+v", request)
	}
	if !request.HeartbeatDeadlineMs.Valid || request.HeartbeatDeadlineMs.Int64 != disconnect.DeadlineMs.Int64 {
		t.Fatalf("expected grace heartbeat release to preserve deadline %d, got %+v", disconnect.DeadlineMs.Int64, request)
	}
	if !request.HeartbeatGraceUntilMs.Valid || request.HeartbeatGraceUntilMs.Int64 != disconnect.DeadlineMs.Int64 {
		t.Fatalf("expected grace heartbeat release to preserve grace deadline %d, got %+v", disconnect.DeadlineMs.Int64, request)
	}
	if !request.HeartbeatTerminalReason.Valid || request.HeartbeatTerminalReason.String != request.TerminalReason.String {
		t.Fatalf("expected grace heartbeat release to mirror terminal reason into heartbeat cleanup, got %+v", request)
	}
	if request.HeartbeatGeneration.Int64 != open.Generation.Int64 {
		t.Fatalf("expected grace heartbeat release to preserve generation, got %+v", request)
	}
}

func TestRuntimeLateReleaseAfterHandoffTimeoutDoesNotDoubleDecrementCounters(t *testing.T) {
	db := requireRuntimeConcurrencyDB(t)
	nowMs := time.Now().UnixMilli()

	grant, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
		HostnameHash:      "handoff-late-release-host",
		Hostname:          "handoff-late-release.example.com",
		SiteBucket:        "site-a",
		IPBucket:          "ip-a",
		RequestID:         "handoff-late-release-request",
		HardExpireMs:      nowMs + 120_000,
		NowMs:             nowMs,
		HostMaxInFlight:   1,
		SiteMaxInFlight:   1,
		SiteIPMaxInFlight: 1,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}
	claim, err := execRuntimeClaimGrant(context.Background(), db, "handoff-late-release-request", grant.ClaimToken.String, nowMs+1)
	if err != nil {
		t.Fatalf("claim grant: %v", err)
	}

	overdueNow := claim.HandoffDeadlineMs.Int64
	if _, err := execRuntimeClaimGrant(context.Background(), db, "handoff-late-release-request", grant.ClaimToken.String, overdueNow); err != nil {
		t.Fatalf("compensating duplicate claim: %v", err)
	}

	hostCount, siteCount, siteIPCount := readRuntimeActiveCounters(t, db, "handoff-late-release-host", "site-a", "ip-a")
	if hostCount != 0 || siteCount != 0 || siteIPCount != 0 {
		t.Fatalf("expected compensation to decrement counters before late release, got host=%d site=%d site_ip=%d", hostCount, siteCount, siteIPCount)
	}

	var releaseResult string
	var releaseReason, releaseRequestID sql.NullString
	if err := db.QueryRowContext(context.Background(), `
		SELECT result, reason, request_id
		FROM cq_release($1::uuid, $2, $3, $4)
	`, grant.LeaseID, grant.LeaseToken, "stream_complete", overdueNow+1).Scan(&releaseResult, &releaseReason, &releaseRequestID); err != nil {
		t.Fatalf("late release after handoff timeout: %v", err)
	}
	if releaseResult != "noop" || releaseReason.String != "already_released" || releaseRequestID.String != "handoff-late-release-request" {
		t.Fatalf("expected late release to stay idempotent after compensation, got result=%q reason=%q request=%q", releaseResult, releaseReason.String, releaseRequestID.String)
	}

	hostCount, siteCount, siteIPCount = readRuntimeActiveCounters(t, db, "handoff-late-release-host", "site-a", "ip-a")
	if hostCount != 0 || siteCount != 0 || siteIPCount != 0 {
		t.Fatalf("expected late release not to double decrement counters, got host=%d site=%d site_ip=%d", hostCount, siteCount, siteIPCount)
	}

	request := readRuntimeRequestState(t, db, "handoff-late-release-request")
	if request.TerminalReason.String != "claim_handoff_timeout" || request.ClaimState.String != "compensated" || request.HandoffState.String != "compensated" {
		t.Fatalf("expected late release not to erase timeout-compensation state, got %+v", request)
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

	var requestState, terminalReason, heartbeatTerminalReason string
	if err := db.QueryRowContext(context.Background(), `
		SELECT state, terminal_reason, heartbeat_terminal_reason
		FROM concurrency_requests
		WHERE request_id = $1
	`, "release-request").Scan(&requestState, &terminalReason, &heartbeatTerminalReason); err != nil {
		t.Fatalf("read released request state: %v", err)
	}
	if requestState != "released" || terminalReason != "stream_complete" || heartbeatTerminalReason != "stream_complete" {
		t.Fatalf("expected canonical release persistence, got state=%q terminal_reason=%q heartbeat_terminal_reason=%q", requestState, terminalReason, heartbeatTerminalReason)
	}
}

func TestRuntimeReleasePersistsCanonicalWorkerReasons(t *testing.T) {
	canonicalReasons := []string{
		"stream_complete",
		"client_disconnect",
		"hard_expiry",
		"upstream_failure",
		"origin_fetch_failure",
		"heartbeat_connect_failed",
		"heartbeat_lost",
		"final_cleanup",
	}

	for _, reason := range canonicalReasons {
		t.Run(reason, func(t *testing.T) {
			db := requireRuntimeConcurrencyDB(t)
			nowMs := time.Now().UnixMilli()
			requestID := "canonical-release-" + strings.ReplaceAll(reason, "_", "-")
			lease, err := execRuntimeAcquire(context.Background(), db, runtimeAcquireCall{
				HostnameHash: "canonical-release-host-" + reason,
				Hostname:     "canonical-release.example.com",
				SiteBucket:   "site-a",
				IPBucket:     "ip-a",
				RequestID:    requestID,
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
			`, lease.LeaseID, lease.LeaseToken, reason, nowMs+1).Scan(&releaseResult, &releaseReason, &releaseRequestID); err != nil {
				t.Fatalf("release active lease: %v", err)
			}
			if releaseResult != "released" {
				t.Fatalf("expected released result, got result=%q reason=%q", releaseResult, releaseReason.String)
			}

			var requestState, terminalReason, heartbeatTerminalReason string
			if err := db.QueryRowContext(context.Background(), `
				SELECT state, terminal_reason, heartbeat_terminal_reason
				FROM concurrency_requests
				WHERE request_id = $1
			`, requestID).Scan(&requestState, &terminalReason, &heartbeatTerminalReason); err != nil {
				t.Fatalf("read released request state: %v", err)
			}
			if requestState != "released" || terminalReason != reason || heartbeatTerminalReason != reason {
				t.Fatalf("expected canonical reason %q persisted, got state=%q terminal_reason=%q heartbeat_terminal_reason=%q", reason, requestState, terminalReason, heartbeatTerminalReason)
			}
		})
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

func TestRuntimeCompensationReleaseReasonsCanonicalizeToFinalCleanup(t *testing.T) {
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
			if requestState != "released" || terminalReason != "final_cleanup" {
				t.Fatalf("expected compensation release to canonicalize to final_cleanup, got state=%q reason=%q", requestState, terminalReason)
			}
			if terminalReason == reason {
				t.Fatalf("expected compensation reason %q not to leak into terminal state", terminalReason)
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
