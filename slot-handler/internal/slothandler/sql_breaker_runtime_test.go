package slothandler

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const runtimePostgresImage = "postgres:16-alpine"

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

func requireRuntimeBreakerDB(t *testing.T) *sql.DB {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}
	if out, err := exec.Command("docker", "version", "--format", "{{.Server.Version}}").CombinedOutput(); err != nil {
		t.Skipf("docker daemon unavailable: %v (%s)", err, strings.TrimSpace(string(out)))
	}

	dbName := fmt.Sprintf("breaker_sql_%d", time.Now().UnixNano())
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

	addr := waitForRuntimePostgresPort(t, containerID)
	dsn := fmt.Sprintf("postgres://postgres:postgres@%s/%s?sslmode=disable", addr, dbName)
	db := waitForRuntimePostgresReady(t, containerID, dsn)
	t.Cleanup(func() {
		_ = db.Close()
	})

	applyRuntimeInitSQL(t, containerID, dbName)
	return db
}

func waitForRuntimePostgresPort(t *testing.T, containerID string) string {
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

func waitForRuntimePostgresReady(t *testing.T, containerID, dsn string) *sql.DB {
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

func applyRuntimeInitSQL(t *testing.T, containerID, dbName string) {
	t.Helper()

	initSQLPath := filepath.Join(moduleRootDir(t), "..", "init.sql")
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

func TestInitSQLRuntimeClaimAndReportRequireMatchingProbeVersion(t *testing.T) {
	db := requireRuntimeBreakerDB(t)
	ctx := context.Background()
	const (
		hostnameHash = "runtime-host-hash"
		hostname     = "tenant.sharepoint.com"
		now          = 1_700_000_000
	)

	_, err := db.ExecContext(ctx, `
		INSERT INTO "THROTTLE_PROTECTION" (
			"HOSTNAME_HASH", "HOSTNAME", "STATE", "OPEN_UNTIL", "LAST_ERROR_CODE", "OPEN_REASON", "VERSION"
		) VALUES ($1, $2, 'open', $3, 429, 'http_429', 7)
	`, hostnameHash, hostname, now-1)
	if err != nil {
		t.Fatalf("seed breaker row: %v", err)
	}

	var (
		claimState      string
		claimOpenUntil  sql.NullInt64
		claimLeaseUntil sql.NullInt64
		claimVersion    int64
		probeGranted    bool
	)
	err = db.QueryRowContext(ctx, `
		SELECT "STATE", "OPEN_UNTIL", "PROBE_LEASE_UNTIL", "VERSION", "PROBE_GRANTED"
		FROM download_claim_breaker_probe($1, $2, $3, $4, $5, $6)
	`, hostnameHash, hostname, now, 15, 0, "partial-close").Scan(
		&claimState,
		&claimOpenUntil,
		&claimLeaseUntil,
		&claimVersion,
		&probeGranted,
	)
	if err != nil {
		t.Fatalf("claim breaker probe: %v", err)
	}
	if claimState != "half_open" {
		t.Fatalf("expected claim state half_open, got %q", claimState)
	}
	if claimOpenUntil.Valid {
		t.Fatalf("expected claim to clear open_until, got %v", claimOpenUntil.Int64)
	}
	if !claimLeaseUntil.Valid || claimLeaseUntil.Int64 <= now {
		t.Fatalf("expected claim to issue probe lease, got %v", claimLeaseUntil)
	}
	if claimVersion != 8 {
		t.Fatalf("expected claim version 8, got %d", claimVersion)
	}
	if !probeGranted {
		t.Fatalf("expected claim to grant probe")
	}

	var persistedHalfOpenSince sql.NullInt64
	err = db.QueryRowContext(ctx, `
		SELECT "HALF_OPEN_SINCE"
		FROM "THROTTLE_PROTECTION"
		WHERE "HOSTNAME_HASH" = $1
	`, hostnameHash).Scan(&persistedHalfOpenSince)
	if err != nil {
		t.Fatalf("query claimed half_open row: %v", err)
	}
	if !persistedHalfOpenSince.Valid || persistedHalfOpenSince.Int64 != now {
		t.Fatalf("expected open->half_open claim to persist HALF_OPEN_SINCE %d, got %v", now, persistedHalfOpenSince)
	}

	var (
		staleState        string
		staleTotalSamples int
		staleSuccesses    int
		staleVersion      int64
	)
	err = db.QueryRowContext(ctx, `
		SELECT "STATE", "TOTAL_SAMPLES", "SUCCESS_STREAK", "VERSION"
		FROM download_report_breaker_sample($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
	`, hostnameHash, hostname, now, 0, 200, 60, 20, 15, 8, 4, 8, 900, 2, "and", 0, "partial-close", nil, 7).Scan(
		&staleState,
		&staleTotalSamples,
		&staleSuccesses,
		&staleVersion,
	)
	if err != nil {
		t.Fatalf("report stale probe version: %v", err)
	}
	if staleState != "half_open" || staleTotalSamples != 0 || staleSuccesses != 0 || staleVersion != 8 {
		t.Fatalf("stale probe version should be ignored, got state=%q total=%d success=%d version=%d", staleState, staleTotalSamples, staleSuccesses, staleVersion)
	}

	var (
		acceptedState        string
		acceptedTotalSamples int
		acceptedSuccesses    int
		acceptedVersion      int64
	)
	err = db.QueryRowContext(ctx, `
		SELECT "STATE", "TOTAL_SAMPLES", "SUCCESS_STREAK", "VERSION"
		FROM download_report_breaker_sample($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
	`, hostnameHash, hostname, now, 0, 200, 60, 20, 15, 8, 4, 8, 900, 2, "and", 0, "partial-close", nil, 8).Scan(
		&acceptedState,
		&acceptedTotalSamples,
		&acceptedSuccesses,
		&acceptedVersion,
	)
	if err != nil {
		t.Fatalf("report accepted probe version: %v", err)
	}
	if acceptedState != "half_open" || acceptedTotalSamples != 1 || acceptedSuccesses != 1 || acceptedVersion != 9 {
		t.Fatalf("accepted probe version should advance half_open recovery, got state=%q total=%d success=%d version=%d", acceptedState, acceptedTotalSamples, acceptedSuccesses, acceptedVersion)
	}
}

func TestInitSQLRuntimeReportDoesNotPromoteExpiredOpenRow(t *testing.T) {
	db := requireRuntimeBreakerDB(t)
	ctx := context.Background()
	const (
		hostnameHash = "runtime-open-host"
		hostname     = "files.office.com"
		now          = 1_700_000_100
	)

	_, err := db.ExecContext(ctx, `
		INSERT INTO "THROTTLE_PROTECTION" (
			"HOSTNAME_HASH", "HOSTNAME", "STATE", "OPEN_UNTIL", "LAST_ERROR_CODE", "OPEN_REASON", "VERSION"
		) VALUES ($1, $2, 'open', $3, 429, 'http_429', 3)
	`, hostnameHash, hostname, now-1)
	if err != nil {
		t.Fatalf("seed expired open row: %v", err)
	}

	var (
		reportState        string
		reportOpenUntil    sql.NullInt64
		reportTotalSamples int
		reportVersion      int64
	)
	err = db.QueryRowContext(ctx, `
		SELECT "STATE", "OPEN_UNTIL", "TOTAL_SAMPLES", "VERSION"
		FROM download_report_breaker_sample($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
	`, hostnameHash, hostname, now, 0, 200, 60, 20, 15, 8, 4, 8, 900, 2, "and", 0, "partial-close", nil, nil).Scan(
		&reportState,
		&reportOpenUntil,
		&reportTotalSamples,
		&reportVersion,
	)
	if err != nil {
		t.Fatalf("report expired open row: %v", err)
	}
	if reportState != "open" {
		t.Fatalf("expected report to leave expired row open, got %q", reportState)
	}
	if !reportOpenUntil.Valid || reportOpenUntil.Int64 != now-1 {
		t.Fatalf("expected report to keep original open_until, got %v", reportOpenUntil)
	}
	if reportTotalSamples != 1 || reportVersion != 3 {
		t.Fatalf("expected report to avoid promoting expired open row, got total=%d version=%d", reportTotalSamples, reportVersion)
	}

	var (
		claimState     string
		claimOpenUntil sql.NullInt64
		claimVersion   int64
		probeGranted   bool
	)
	err = db.QueryRowContext(ctx, `
		SELECT "STATE", "OPEN_UNTIL", "VERSION", "PROBE_GRANTED"
		FROM download_claim_breaker_probe($1, $2, $3, $4, $5, $6)
	`, hostnameHash, hostname, now, 15, 0, "partial-close").Scan(
		&claimState,
		&claimOpenUntil,
		&claimVersion,
		&probeGranted,
	)
	if err != nil {
		t.Fatalf("claim after expired open report: %v", err)
	}
	if claimState != "half_open" {
		t.Fatalf("expected claim to own open->half_open, got %q", claimState)
	}
	if claimOpenUntil.Valid {
		t.Fatalf("expected claim to clear open_until, got %v", claimOpenUntil.Int64)
	}
	if claimVersion != 4 {
		t.Fatalf("expected claim version 4, got %d", claimVersion)
	}
	if !probeGranted {
		t.Fatalf("expected claim to grant probe after expired open row")
	}
}

func TestInitSQLRuntimeClaimRemintsExpiredHalfOpenLease(t *testing.T) {
	db := requireRuntimeBreakerDB(t)
	ctx := context.Background()
	const (
		hostnameHash  = "runtime-half-open-host"
		hostname      = "tenant.sharepoint.com"
		now           = 1_700_000_200
		halfOpenSince = now - 20
	)

	_, err := db.ExecContext(ctx, `
		INSERT INTO "THROTTLE_PROTECTION" (
			"HOSTNAME_HASH", "HOSTNAME", "STATE", "OPEN_UNTIL", "PROBE_LEASE_UNTIL", "HALF_OPEN_SINCE", "LAST_ERROR_CODE", "OPEN_REASON", "VERSION"
		) VALUES ($1, $2, 'half_open', NULL, $3, $4, 429, 'http_429', 10)
	`, hostnameHash, hostname, now-1, halfOpenSince)
	if err != nil {
		t.Fatalf("seed expired half-open row: %v", err)
	}

	var (
		claimState      string
		claimOpenUntil  sql.NullInt64
		claimLeaseUntil sql.NullInt64
		claimVersion    int64
		probeGranted    bool
	)
	err = db.QueryRowContext(ctx, `
		SELECT "STATE", "OPEN_UNTIL", "PROBE_LEASE_UNTIL", "VERSION", "PROBE_GRANTED"
		FROM download_claim_breaker_probe($1, $2, $3, $4, $5, $6)
	`, hostnameHash, hostname, now, 15, 0, "partial-close").Scan(
		&claimState,
		&claimOpenUntil,
		&claimLeaseUntil,
		&claimVersion,
		&probeGranted,
	)
	if err != nil {
		t.Fatalf("claim expired half-open lease: %v", err)
	}
	if claimState != "half_open" {
		t.Fatalf("expected state half_open after lease remint, got %q", claimState)
	}
	if claimOpenUntil.Valid {
		t.Fatalf("expected reminted half_open lease to keep open_until null, got %v", claimOpenUntil.Int64)
	}
	if !claimLeaseUntil.Valid || claimLeaseUntil.Int64 <= now {
		t.Fatalf("expected reminted lease to move into the future, got %v", claimLeaseUntil)
	}
	if claimVersion != 11 {
		t.Fatalf("expected expired half_open lease to bump version to 11, got %d", claimVersion)
	}
	if !probeGranted {
		t.Fatalf("expected expired half_open lease remint to grant probe")
	}

	var persistedHalfOpenSince sql.NullInt64
	err = db.QueryRowContext(ctx, `
		SELECT "HALF_OPEN_SINCE"
		FROM "THROTTLE_PROTECTION"
		WHERE "HOSTNAME_HASH" = $1
	`, hostnameHash).Scan(&persistedHalfOpenSince)
	if err != nil {
		t.Fatalf("query reminted half_open row: %v", err)
	}
	if !persistedHalfOpenSince.Valid || persistedHalfOpenSince.Int64 != halfOpenSince {
		t.Fatalf("expected reminted half_open lease to preserve HALF_OPEN_SINCE %d, got %v", halfOpenSince, persistedHalfOpenSince)
	}
}

func TestInitSQLRuntimeReportRejectsMissingRequiredThresholds(t *testing.T) {
	db := requireRuntimeBreakerDB(t)
	ctx := context.Background()
	const (
		hostnameHash = "runtime-missing-thresholds-host"
		hostname     = "missing.sharepoint.com"
		now          = 1_700_000_250
	)

	var state string
	err := db.QueryRowContext(ctx, `
		SELECT "STATE"
		FROM download_report_breaker_sample($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
	`, hostnameHash, hostname, now, 1, 429, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil).Scan(&state)
	if err == nil {
		t.Fatalf("expected missing required thresholds to fail, got state %q", state)
	}
	if !strings.Contains(err.Error(), "requires non-null breaker thresholds") {
		t.Fatalf("expected missing-threshold error, got %v", err)
	}
}

func TestInitSQLRuntimeRejectsUnknownHalfOpenModes(t *testing.T) {
	db := requireRuntimeBreakerDB(t)
	ctx := context.Background()

	t.Run("claim rejects unknown timeout mode", func(t *testing.T) {
		const (
			hostnameHash = "runtime-invalid-timeout-mode-host"
			hostname     = "invalid-timeout.sharepoint.com"
			now          = 1_700_000_275
		)

		var state string
		err := db.QueryRowContext(ctx, `
			SELECT "STATE"
			FROM download_claim_breaker_probe($1, $2, $3, $4, $5, $6)
		`, hostnameHash, hostname, now, 15, 0, "linger").Scan(&state)
		if err == nil {
			t.Fatalf("expected unknown p_half_open_timeout_mode to fail, got state %q", state)
		}
		if !strings.Contains(err.Error(), "p_half_open_timeout_mode") {
			t.Fatalf("expected unknown-timeout-mode error, got %v", err)
		}
	})

	t.Run("report rejects unknown close mode", func(t *testing.T) {
		const (
			hostnameHash = "runtime-invalid-close-mode-host"
			hostname     = "invalid-close.sharepoint.com"
			now          = 1_700_000_276
		)

		var state string
		err := db.QueryRowContext(ctx, `
			SELECT "STATE"
			FROM download_report_breaker_sample($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
		`, hostnameHash, hostname, now, 0, 200, 60, 20, 15, 8, 4, 8, 900, 2, "xor", 0, "partial-close", nil, nil).Scan(&state)
		if err == nil {
			t.Fatalf("expected unknown p_half_open_close_mode to fail, got state %q", state)
		}
		if !strings.Contains(err.Error(), "p_half_open_close_mode") {
			t.Fatalf("expected unknown-close-mode error, got %v", err)
		}
	})
}

func TestInitSQLRuntimeWarmupUsesSamplesSinceResetInsteadOfLifetimeTotals(t *testing.T) {
	db := requireRuntimeBreakerDB(t)
	ctx := context.Background()
	const (
		hostnameHash = "runtime-warmup-host"
		hostname     = "warmup.sharepoint.com"
		now          = 1_700_000_300
	)

	_, err := db.ExecContext(ctx, `
		INSERT INTO "THROTTLE_PROTECTION" (
			"HOSTNAME_HASH", "HOSTNAME", "STATE", "EWMA_SCORE", "TOTAL_SAMPLES", "SAMPLES_SINCE_RESET",
			"CONSECUTIVE_ERROR_COUNT", "LAST_SAMPLE_AT", "VERSION"
		) VALUES ($1, $2, 'closed', $3, 50, 6, 0, $4, 0)
	`, hostnameHash, hostname, 0.2, now-30)
	if err != nil {
		t.Fatalf("seed warmup row: %v", err)
	}

	var (
		state                 string
		ewma                  float64
		totalSamples          int
		samplesSinceReset     int
		consecutiveErrorCount int
		lastSampleAt          sql.NullInt64
	)
	err = db.QueryRowContext(ctx, `
		SELECT "STATE", "EWMA_SCORE"::double precision, "TOTAL_SAMPLES", "SAMPLES_SINCE_RESET", "CONSECUTIVE_ERROR_COUNT", "LAST_SAMPLE_AT"
		FROM download_report_breaker_sample($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
	`, hostnameHash, hostname, now, 1, 429, 60, 30, 15, 8, 4, 8, 900, 2, "and", 0, "partial-close", nil, nil).Scan(
		&state,
		&ewma,
		&totalSamples,
		&samplesSinceReset,
		&consecutiveErrorCount,
		&lastSampleAt,
	)
	if err != nil {
		t.Fatalf("report warmup-gated sample: %v", err)
	}
	if state != "closed" {
		t.Fatalf("expected warmup gate to keep state closed, got %q", state)
	}
	if math.Abs(ewma-0.3777777778) > 0.000001 {
		t.Fatalf("expected ewma 0.377778 after sample, got %.6f", ewma)
	}
	if totalSamples != 51 {
		t.Fatalf("expected total_samples to remain lifetime count 51, got %d", totalSamples)
	}
	if samplesSinceReset != 7 {
		t.Fatalf("expected samples_since_reset to advance to 7, got %d", samplesSinceReset)
	}
	if consecutiveErrorCount != 1 {
		t.Fatalf("expected consecutive_error_count 1, got %d", consecutiveErrorCount)
	}
	if !lastSampleAt.Valid || lastSampleAt.Int64 != now {
		t.Fatalf("expected last_sample_at %d, got %v", now, lastSampleAt)
	}
}

func TestInitSQLRuntimeWarmupTrendOpensAtExactBoundary(t *testing.T) {
	db := requireRuntimeBreakerDB(t)
	ctx := context.Background()
	const (
		hostnameHash = "runtime-warmup-boundary-host"
		hostname     = "boundary.sharepoint.com"
		now          = 1_700_000_350
	)

	_, err := db.ExecContext(ctx, `
		INSERT INTO "THROTTLE_PROTECTION" (
			"HOSTNAME_HASH", "HOSTNAME", "STATE", "EWMA_SCORE", "TOTAL_SAMPLES", "SAMPLES_SINCE_RESET",
			"CONSECUTIVE_ERROR_COUNT", "LAST_SAMPLE_AT", "VERSION"
		) VALUES ($1, $2, 'closed', $3, 51, 7, 0, $4, 0)
	`, hostnameHash, hostname, 0.2, now-30)
	if err != nil {
		t.Fatalf("seed warmup-boundary row: %v", err)
	}

	var (
		state                 string
		openUntil             sql.NullInt64
		ewma                  float64
		totalSamples          int
		samplesSinceReset     int
		consecutiveErrorCount int
		lastSampleAt          sql.NullInt64
		lastErrorCode         sql.NullInt64
		openReason            sql.NullString
		lastOpenSeconds       int
	)
	err = db.QueryRowContext(ctx, `
		SELECT "STATE", "OPEN_UNTIL", "EWMA_SCORE"::double precision, "TOTAL_SAMPLES", "SAMPLES_SINCE_RESET", "CONSECUTIVE_ERROR_COUNT", "LAST_SAMPLE_AT", "LAST_ERROR_CODE", "OPEN_REASON", "LAST_OPEN_SECONDS"
		FROM download_report_breaker_sample($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
	`, hostnameHash, hostname, now, 1, 429, 60, 30, 15, 8, 4, 8, 900, 2, "and", 0, "partial-close", nil, nil).Scan(
		&state,
		&openUntil,
		&ewma,
		&totalSamples,
		&samplesSinceReset,
		&consecutiveErrorCount,
		&lastSampleAt,
		&lastErrorCode,
		&openReason,
		&lastOpenSeconds,
	)
	if err != nil {
		t.Fatalf("report warmup-boundary sample: %v", err)
	}
	if state != "open" {
		t.Fatalf("expected warmup boundary to open breaker, got %q", state)
	}
	if !openUntil.Valid || openUntil.Int64 != now+1 {
		t.Fatalf("expected open_until %d at warmup boundary, got %v", now+1, openUntil)
	}
	if math.Abs(ewma-0.3777777778) > 0.000001 {
		t.Fatalf("expected ewma 0.377778 at warmup boundary, got %.6f", ewma)
	}
	if totalSamples != 52 {
		t.Fatalf("expected total_samples lifetime count 52, got %d", totalSamples)
	}
	if samplesSinceReset != 8 {
		t.Fatalf("expected samples_since_reset to reach boundary 8, got %d", samplesSinceReset)
	}
	if consecutiveErrorCount != 1 {
		t.Fatalf("expected consecutive_error_count 1 at warmup boundary, got %d", consecutiveErrorCount)
	}
	if !lastSampleAt.Valid || lastSampleAt.Int64 != now {
		t.Fatalf("expected last_sample_at %d, got %v", now, lastSampleAt)
	}
	if !lastErrorCode.Valid || lastErrorCode.Int64 != 429 {
		t.Fatalf("expected last_error_code 429, got %v", lastErrorCode)
	}
	if !openReason.Valid || openReason.String != "http_429" {
		t.Fatalf("expected open_reason http_429 at warmup boundary, got %v", openReason)
	}
	if lastOpenSeconds != 1 {
		t.Fatalf("expected last_open_seconds 1 at warmup boundary, got %d", lastOpenSeconds)
	}
}

func TestInitSQLRuntimeClosedIdleGapSoftResetsBreakerMemory(t *testing.T) {
	db := requireRuntimeBreakerDB(t)
	ctx := context.Background()
	const (
		hostnameHash = "runtime-idle-reset-host"
		hostname     = "idle.sharepoint.com"
		now          = 1_700_000_400
	)

	_, err := db.ExecContext(ctx, `
		INSERT INTO "THROTTLE_PROTECTION" (
			"HOSTNAME_HASH", "HOSTNAME", "STATE", "EWMA_SCORE", "TOTAL_SAMPLES", "SAMPLES_SINCE_RESET",
			"CONSECUTIVE_ERROR_COUNT", "SUCCESS_STREAK", "LAST_SAMPLE_AT", "LAST_ERROR_CODE", "OPEN_REASON", "LAST_OPEN_SECONDS", "VERSION"
		) VALUES ($1, $2, 'closed', $3, 42, 7, 3, 2, $4, 503, 'http_503', 16, 0)
	`, hostnameHash, hostname, 0.95, now-901)
	if err != nil {
		t.Fatalf("seed stale closed row: %v", err)
	}

	var (
		state                 string
		ewma                  float64
		totalSamples          int
		samplesSinceReset     int
		consecutiveErrorCount int
		successStreak         int
		lastSampleAt          sql.NullInt64
		lastErrorCode         sql.NullInt64
		openReason            sql.NullString
		lastOpenSeconds       int
	)
	err = db.QueryRowContext(ctx, `
		SELECT "STATE", "EWMA_SCORE"::double precision, "TOTAL_SAMPLES", "SAMPLES_SINCE_RESET", "CONSECUTIVE_ERROR_COUNT", "SUCCESS_STREAK", "LAST_SAMPLE_AT", "LAST_ERROR_CODE", "OPEN_REASON", "LAST_OPEN_SECONDS"
		FROM download_report_breaker_sample($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
	`, hostnameHash, hostname, now, 1, 429, 60, 30, 15, 8, 4, 8, 900, 2, "and", 0, "partial-close", nil, nil).Scan(
		&state,
		&ewma,
		&totalSamples,
		&samplesSinceReset,
		&consecutiveErrorCount,
		&successStreak,
		&lastSampleAt,
		&lastErrorCode,
		&openReason,
		&lastOpenSeconds,
	)
	if err != nil {
		t.Fatalf("report sample after stale idle gap: %v", err)
	}
	if state != "closed" {
		t.Fatalf("expected stale closed row to soft-reset and stay closed, got %q", state)
	}
	if math.Abs(ewma-0.2222222222) > 0.000001 {
		t.Fatalf("expected ewma 0.222222 after idle reset, got %.6f", ewma)
	}
	if totalSamples != 43 {
		t.Fatalf("expected total_samples lifetime count 43, got %d", totalSamples)
	}
	if samplesSinceReset != 1 {
		t.Fatalf("expected samples_since_reset to restart at 1, got %d", samplesSinceReset)
	}
	if consecutiveErrorCount != 1 {
		t.Fatalf("expected consecutive_error_count 1 after reset, got %d", consecutiveErrorCount)
	}
	if successStreak != 0 {
		t.Fatalf("expected success_streak reset to 0, got %d", successStreak)
	}
	if !lastSampleAt.Valid || lastSampleAt.Int64 != now {
		t.Fatalf("expected last_sample_at %d, got %v", now, lastSampleAt)
	}
	if !lastErrorCode.Valid || lastErrorCode.Int64 != 429 {
		t.Fatalf("expected last_error_code 429, got %v", lastErrorCode)
	}
	if openReason.Valid {
		t.Fatalf("expected idle-reset closed sample to keep open_reason null, got %q", openReason.String)
	}
	if lastOpenSeconds != 0 {
		t.Fatalf("expected idle-reset closed sample to clear last_open_seconds, got %d", lastOpenSeconds)
	}
}

func TestInitSQLRuntimeHalfOpenProtectedSampleReopensImmediately(t *testing.T) {
	db := requireRuntimeBreakerDB(t)
	ctx := context.Background()
	const (
		hostnameHash = "runtime-half-open-reopen-host"
		hostname     = "probe.sharepoint.com"
		now          = 1_700_000_500
	)

	_, err := db.ExecContext(ctx, `
		INSERT INTO "THROTTLE_PROTECTION" (
			"HOSTNAME_HASH", "HOSTNAME", "STATE", "EWMA_SCORE", "TOTAL_SAMPLES", "SAMPLES_SINCE_RESET",
			"CONSECUTIVE_ERROR_COUNT", "SUCCESS_STREAK", "PROBE_LEASE_UNTIL", "HALF_OPEN_SINCE", "LAST_SAMPLE_AT", "VERSION"
		) VALUES ($1, $2, 'half_open', 0, 12, 0, 0, 0, $3, $4, $5, 11)
	`, hostnameHash, hostname, now+15, now-2, now-30)
	if err != nil {
		t.Fatalf("seed half_open row: %v", err)
	}

	var (
		state             string
		openUntil         sql.NullInt64
		samplesSinceReset int
		version           int64
		lastErrorCode     sql.NullInt64
		openReason        sql.NullString
	)
	err = db.QueryRowContext(ctx, `
		SELECT "STATE", "OPEN_UNTIL", "SAMPLES_SINCE_RESET", "VERSION", "LAST_ERROR_CODE", "OPEN_REASON"
		FROM download_report_breaker_sample($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
	`, hostnameHash, hostname, now, 1, 429, 60, 30, 15, 8, 4, 8, 900, 2, "and", 0, "partial-close", nil, 11).Scan(
		&state,
		&openUntil,
		&samplesSinceReset,
		&version,
		&lastErrorCode,
		&openReason,
	)
	if err != nil {
		t.Fatalf("report protected half_open sample: %v", err)
	}
	if state != "open" {
		t.Fatalf("expected protected half_open sample to reopen breaker, got %q", state)
	}
	if !openUntil.Valid || openUntil.Int64 != now+1 {
		t.Fatalf("expected reopened breaker open_until %d, got %v", now+1, openUntil)
	}
	if samplesSinceReset != 1 {
		t.Fatalf("expected samples_since_reset to advance to 1, got %d", samplesSinceReset)
	}
	if version != 12 {
		t.Fatalf("expected accepted probe version to advance to 12, got %d", version)
	}
	if !lastErrorCode.Valid || lastErrorCode.Int64 != 429 {
		t.Fatalf("expected last_error_code 429, got %v", lastErrorCode)
	}
	if !openReason.Valid || openReason.String != "http_429" {
		t.Fatalf("expected open_reason http_429, got %v", openReason)
	}
}

func TestInitSQLRuntimeHalfOpenCloseResetsBreakerBaseline(t *testing.T) {
	db := requireRuntimeBreakerDB(t)
	ctx := context.Background()
	const (
		hostnameHash = "runtime-half-open-close-host"
		hostname     = "recovery.sharepoint.com"
		now          = 1_700_000_600
	)

	_, err := db.ExecContext(ctx, `
		INSERT INTO "THROTTLE_PROTECTION" (
			"HOSTNAME_HASH", "HOSTNAME", "STATE", "EWMA_SCORE", "TOTAL_SAMPLES", "SAMPLES_SINCE_RESET",
			"CONSECUTIVE_ERROR_COUNT", "SUCCESS_STREAK", "PROBE_LEASE_UNTIL", "LAST_SAMPLE_AT", "LAST_ERROR_CODE", "OPEN_REASON", "LAST_OPEN_SECONDS", "VERSION"
		) VALUES ($1, $2, 'half_open', $3, 25, 5, 2, 1, $4, $5, 429, 'http_429', 8, 20)
	`, hostnameHash, hostname, 0.01, now+15, now-30)
	if err != nil {
		t.Fatalf("seed closing half_open row: %v", err)
	}

	var (
		state                 string
		openUntil             sql.NullInt64
		ewma                  float64
		totalSamples          int
		samplesSinceReset     int
		consecutiveErrorCount int
		successStreak         int
		lastSampleAt          sql.NullInt64
		lastErrorCode         sql.NullInt64
		openReason            sql.NullString
		lastOpenSeconds       int
		version               int64
	)
	err = db.QueryRowContext(ctx, `
		SELECT "STATE", "OPEN_UNTIL", "EWMA_SCORE"::double precision, "TOTAL_SAMPLES", "SAMPLES_SINCE_RESET", "CONSECUTIVE_ERROR_COUNT", "SUCCESS_STREAK", "LAST_SAMPLE_AT", "LAST_ERROR_CODE", "OPEN_REASON", "LAST_OPEN_SECONDS", "VERSION"
		FROM download_report_breaker_sample($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
	`, hostnameHash, hostname, now, 0, 200, 60, 30, 15, 8, 4, 8, 900, 2, "and", 0, "partial-close", nil, 20).Scan(
		&state,
		&openUntil,
		&ewma,
		&totalSamples,
		&samplesSinceReset,
		&consecutiveErrorCount,
		&successStreak,
		&lastSampleAt,
		&lastErrorCode,
		&openReason,
		&lastOpenSeconds,
		&version,
	)
	if err != nil {
		t.Fatalf("report successful half_open sample: %v", err)
	}
	if state != "closed" {
		t.Fatalf("expected half_open recovery to close breaker, got %q", state)
	}
	if openUntil.Valid {
		t.Fatalf("expected recovered breaker to clear open_until, got %v", openUntil.Int64)
	}
	if ewma != 0 {
		t.Fatalf("expected recovered breaker ewma baseline 0, got %.6f", ewma)
	}
	if totalSamples != 26 {
		t.Fatalf("expected total_samples lifetime count 26, got %d", totalSamples)
	}
	if samplesSinceReset != 0 {
		t.Fatalf("expected recovered breaker samples_since_reset 0, got %d", samplesSinceReset)
	}
	if consecutiveErrorCount != 0 {
		t.Fatalf("expected recovered breaker consecutive_error_count 0, got %d", consecutiveErrorCount)
	}
	if successStreak != 0 {
		t.Fatalf("expected recovered breaker success_streak 0, got %d", successStreak)
	}
	if !lastSampleAt.Valid || lastSampleAt.Int64 != now {
		t.Fatalf("expected last_sample_at %d, got %v", now, lastSampleAt)
	}
	if lastErrorCode.Valid {
		t.Fatalf("expected recovered breaker to clear last_error_code, got %v", lastErrorCode)
	}
	if openReason.Valid {
		t.Fatalf("expected recovered breaker to clear open_reason, got %q", openReason.String)
	}
	if lastOpenSeconds != 0 {
		t.Fatalf("expected recovered breaker last_open_seconds 0, got %d", lastOpenSeconds)
	}
	if version != 21 {
		t.Fatalf("expected accepted probe version to advance to 21, got %d", version)
	}
}

func TestInitSQLRuntimeHalfOpenCloseModeOrClosesOnFirstLowEWMASuccess(t *testing.T) {
	db := requireRuntimeBreakerDB(t)
	ctx := context.Background()
	const (
		hostnameHash  = "runtime-half-open-close-or-host"
		hostname      = "or-close.sharepoint.com"
		now           = 1_700_000_700
		halfOpenSince = now - 1
	)

	_, err := db.ExecContext(ctx, `
		INSERT INTO "THROTTLE_PROTECTION" (
			"HOSTNAME_HASH", "HOSTNAME", "STATE", "EWMA_SCORE", "TOTAL_SAMPLES", "SAMPLES_SINCE_RESET",
			"CONSECUTIVE_ERROR_COUNT", "SUCCESS_STREAK", "PROBE_LEASE_UNTIL", "HALF_OPEN_SINCE", "LAST_SAMPLE_AT", "LAST_ERROR_CODE", "OPEN_REASON", "LAST_OPEN_SECONDS", "VERSION"
		) VALUES ($1, $2, 'half_open', 0.05, 10, 3, 0, 0, $3, $4, $5, 429, 'http_429', 8, 30)
	`, hostnameHash, hostname, now+15, halfOpenSince, now-30)
	if err != nil {
		t.Fatalf("seed half_open close-mode or row: %v", err)
	}

	var (
		state                  string
		openUntil              sql.NullInt64
		successStreak          int
		version                int64
		persistedHalfOpenSince sql.NullInt64
	)
	err = db.QueryRowContext(ctx, `
		SELECT "STATE", "OPEN_UNTIL", "SUCCESS_STREAK", "VERSION"
		FROM download_report_breaker_sample($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
	`, hostnameHash, hostname, now, 0, 200, 60, 30, 15, 8, 4, 8, 900, 2, "or", 0, "partial-close", nil, 30).Scan(
		&state,
		&openUntil,
		&successStreak,
		&version,
	)
	if err != nil {
		t.Fatalf("report half_open success with close-mode or: %v", err)
	}
	if state != "closed" {
		t.Fatalf("expected close-mode or to close on first low-ewma success, got %q", state)
	}
	if openUntil.Valid {
		t.Fatalf("expected close-mode or to clear open_until, got %v", openUntil.Int64)
	}
	if successStreak != 0 {
		t.Fatalf("expected close-mode or to reset success_streak, got %d", successStreak)
	}
	if version != 31 {
		t.Fatalf("expected accepted probe version to advance to 31, got %d", version)
	}

	err = db.QueryRowContext(ctx, `
		SELECT "HALF_OPEN_SINCE"
		FROM "THROTTLE_PROTECTION"
		WHERE "HOSTNAME_HASH" = $1
	`, hostnameHash).Scan(&persistedHalfOpenSince)
	if err != nil {
		t.Fatalf("query close-mode or row: %v", err)
	}
	if persistedHalfOpenSince.Valid {
		t.Fatalf("expected close-mode or to clear HALF_OPEN_SINCE, got %v", persistedHalfOpenSince.Int64)
	}
}

func TestInitSQLRuntimeHalfOpenCloseModeAndWaitsForSuccessThreshold(t *testing.T) {
	db := requireRuntimeBreakerDB(t)
	ctx := context.Background()
	const (
		hostnameHash  = "runtime-half-open-close-and-host"
		hostname      = "and-close.sharepoint.com"
		now           = 1_700_000_750
		halfOpenSince = now - 2
	)

	_, err := db.ExecContext(ctx, `
		INSERT INTO "THROTTLE_PROTECTION" (
			"HOSTNAME_HASH", "HOSTNAME", "STATE", "EWMA_SCORE", "TOTAL_SAMPLES", "SAMPLES_SINCE_RESET",
			"CONSECUTIVE_ERROR_COUNT", "SUCCESS_STREAK", "PROBE_LEASE_UNTIL", "HALF_OPEN_SINCE", "LAST_SAMPLE_AT", "LAST_ERROR_CODE", "OPEN_REASON", "LAST_OPEN_SECONDS", "VERSION"
		) VALUES ($1, $2, 'half_open', 0.05, 10, 3, 0, 0, $3, $4, $5, 429, 'http_429', 8, 40)
	`, hostnameHash, hostname, now+15, halfOpenSince, now-30)
	if err != nil {
		t.Fatalf("seed half_open close-mode and row: %v", err)
	}

	var (
		state                  string
		openUntil              sql.NullInt64
		probeLeaseUntil        sql.NullInt64
		successStreak          int
		version                int64
		persistedHalfOpenSince sql.NullInt64
	)
	err = db.QueryRowContext(ctx, `
		SELECT "STATE", "OPEN_UNTIL", "PROBE_LEASE_UNTIL", "SUCCESS_STREAK", "VERSION"
		FROM download_report_breaker_sample($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
	`, hostnameHash, hostname, now, 0, 200, 60, 30, 15, 8, 4, 8, 900, 2, "and", 0, "partial-close", nil, 40).Scan(
		&state,
		&openUntil,
		&probeLeaseUntil,
		&successStreak,
		&version,
	)
	if err != nil {
		t.Fatalf("report half_open success with close-mode and: %v", err)
	}
	if state != "half_open" {
		t.Fatalf("expected close-mode and to keep half_open until both close conditions are met, got %q", state)
	}
	if openUntil.Valid {
		t.Fatalf("expected close-mode and to keep open_until null, got %v", openUntil.Int64)
	}
	if probeLeaseUntil.Valid {
		t.Fatalf("expected close-mode and success to consume the probe lease, got %v", probeLeaseUntil.Int64)
	}
	if successStreak != 1 {
		t.Fatalf("expected close-mode and to keep first success on the row, got %d", successStreak)
	}
	if version != 41 {
		t.Fatalf("expected accepted probe version to advance to 41, got %d", version)
	}

	err = db.QueryRowContext(ctx, `
		SELECT "HALF_OPEN_SINCE"
		FROM "THROTTLE_PROTECTION"
		WHERE "HOSTNAME_HASH" = $1
	`, hostnameHash).Scan(&persistedHalfOpenSince)
	if err != nil {
		t.Fatalf("query close-mode and row: %v", err)
	}
	if !persistedHalfOpenSince.Valid || persistedHalfOpenSince.Int64 != halfOpenSince {
		t.Fatalf("expected close-mode and to preserve HALF_OPEN_SINCE %d, got %v", halfOpenSince, persistedHalfOpenSince)
	}
}

func TestInitSQLRuntimeClaimTimeoutModeOpenReopensTimedOutHalfOpen(t *testing.T) {
	db := requireRuntimeBreakerDB(t)
	ctx := context.Background()
	const (
		hostnameHash  = "runtime-half-open-timeout-open-host"
		hostname      = "timeout-open.sharepoint.com"
		now           = 1_700_000_800
		halfOpenSince = now - 5
	)

	_, err := db.ExecContext(ctx, `
		INSERT INTO "THROTTLE_PROTECTION" (
			"HOSTNAME_HASH", "HOSTNAME", "STATE", "OPEN_UNTIL", "EWMA_SCORE", "TOTAL_SAMPLES", "SAMPLES_SINCE_RESET",
			"SUCCESS_STREAK", "PROBE_LEASE_UNTIL", "HALF_OPEN_SINCE", "LAST_SAMPLE_AT", "LAST_ERROR_CODE", "OPEN_REASON", "LAST_OPEN_SECONDS", "VERSION"
		) VALUES ($1, $2, 'half_open', NULL, 0.6, 10, 4, 0, $3, $4, $5, 429, 'http_429', 7, 50)
	`, hostnameHash, hostname, now-1, halfOpenSince, now-30)
	if err != nil {
		t.Fatalf("seed timed-out half_open row for open mode: %v", err)
	}

	var (
		state                  string
		openUntil              sql.NullInt64
		probeLeaseUntil        sql.NullInt64
		version                int64
		probeGranted           bool
		persistedHalfOpenSince sql.NullInt64
	)
	err = db.QueryRowContext(ctx, `
		SELECT "STATE", "OPEN_UNTIL", "PROBE_LEASE_UNTIL", "VERSION", "PROBE_GRANTED"
		FROM download_claim_breaker_probe($1, $2, $3, $4, $5, $6)
	`, hostnameHash, hostname, now, 15, 5, "open").Scan(
		&state,
		&openUntil,
		&probeLeaseUntil,
		&version,
		&probeGranted,
	)
	if err != nil {
		t.Fatalf("claim timed-out half_open row with open mode: %v", err)
	}
	if state != "open" {
		t.Fatalf("expected timeout mode open to reopen half_open row, got %q", state)
	}
	if !openUntil.Valid || openUntil.Int64 != now+7 {
		t.Fatalf("expected timeout mode open to reuse last_open_seconds, got %v", openUntil)
	}
	if probeLeaseUntil.Valid {
		t.Fatalf("expected timeout mode open to leave probe lease empty, got %v", probeLeaseUntil.Int64)
	}
	if version != 51 {
		t.Fatalf("expected timeout mode open to advance version to 51, got %d", version)
	}
	if probeGranted {
		t.Fatalf("expected timeout mode open to resolve without granting a probe")
	}

	err = db.QueryRowContext(ctx, `
		SELECT "HALF_OPEN_SINCE"
		FROM "THROTTLE_PROTECTION"
		WHERE "HOSTNAME_HASH" = $1
	`, hostnameHash).Scan(&persistedHalfOpenSince)
	if err != nil {
		t.Fatalf("query timeout-open row: %v", err)
	}
	if persistedHalfOpenSince.Valid {
		t.Fatalf("expected timeout mode open to clear HALF_OPEN_SINCE, got %v", persistedHalfOpenSince.Int64)
	}
}

func TestInitSQLRuntimeClaimTimeoutModeCloseForceClosesTimedOutHalfOpen(t *testing.T) {
	db := requireRuntimeBreakerDB(t)
	ctx := context.Background()
	const (
		hostnameHash  = "runtime-half-open-timeout-close-host"
		hostname      = "timeout-close.sharepoint.com"
		now           = 1_700_000_850
		halfOpenSince = now - 6
	)

	_, err := db.ExecContext(ctx, `
		INSERT INTO "THROTTLE_PROTECTION" (
			"HOSTNAME_HASH", "HOSTNAME", "STATE", "OPEN_UNTIL", "EWMA_SCORE", "TOTAL_SAMPLES", "SAMPLES_SINCE_RESET",
			"CONSECUTIVE_ERROR_COUNT", "SUCCESS_STREAK", "PROBE_LEASE_UNTIL", "HALF_OPEN_SINCE", "LAST_SAMPLE_AT", "LAST_ERROR_CODE", "OPEN_REASON", "LAST_OPEN_SECONDS", "VERSION"
		) VALUES ($1, $2, 'half_open', NULL, 0.42, 15, 6, 1, 0, $3, $4, $5, 429, 'http_429', 7, 60)
	`, hostnameHash, hostname, now-1, halfOpenSince, now-40)
	if err != nil {
		t.Fatalf("seed timed-out half_open row for close mode: %v", err)
	}

	var (
		state                  string
		openUntil              sql.NullInt64
		version                int64
		probeGranted           bool
		ewma                   float64
		samplesSinceReset      int
		successStreak          int
		lastErrorCode          sql.NullInt64
		openReason             sql.NullString
		lastOpenSeconds        int
		persistedHalfOpenSince sql.NullInt64
	)
	err = db.QueryRowContext(ctx, `
		SELECT "STATE", "OPEN_UNTIL", "VERSION", "PROBE_GRANTED"
		FROM download_claim_breaker_probe($1, $2, $3, $4, $5, $6)
	`, hostnameHash, hostname, now, 15, 5, "close").Scan(
		&state,
		&openUntil,
		&version,
		&probeGranted,
	)
	if err != nil {
		t.Fatalf("claim timed-out half_open row with close mode: %v", err)
	}
	if state != "closed" {
		t.Fatalf("expected timeout mode close to force-close half_open row, got %q", state)
	}
	if openUntil.Valid {
		t.Fatalf("expected timeout mode close to clear open_until, got %v", openUntil.Int64)
	}
	if version != 61 {
		t.Fatalf("expected timeout mode close to advance version to 61, got %d", version)
	}
	if probeGranted {
		t.Fatalf("expected timeout mode close to resolve without granting a probe")
	}

	err = db.QueryRowContext(ctx, `
		SELECT "EWMA_SCORE"::double precision, "SAMPLES_SINCE_RESET", "SUCCESS_STREAK", "LAST_ERROR_CODE", "OPEN_REASON", "LAST_OPEN_SECONDS", "HALF_OPEN_SINCE"
		FROM "THROTTLE_PROTECTION"
		WHERE "HOSTNAME_HASH" = $1
	`, hostnameHash).Scan(
		&ewma,
		&samplesSinceReset,
		&successStreak,
		&lastErrorCode,
		&openReason,
		&lastOpenSeconds,
		&persistedHalfOpenSince,
	)
	if err != nil {
		t.Fatalf("query timeout-close row: %v", err)
	}
	if ewma != 0 {
		t.Fatalf("expected timeout mode close to clear EWMA baseline, got %.6f", ewma)
	}
	if samplesSinceReset != 0 {
		t.Fatalf("expected timeout mode close to reset samples_since_reset, got %d", samplesSinceReset)
	}
	if successStreak != 0 {
		t.Fatalf("expected timeout mode close to reset success_streak, got %d", successStreak)
	}
	if lastErrorCode.Valid {
		t.Fatalf("expected timeout mode close to clear last_error_code, got %v", lastErrorCode)
	}
	if openReason.Valid {
		t.Fatalf("expected timeout mode close to clear open_reason, got %q", openReason.String)
	}
	if lastOpenSeconds != 0 {
		t.Fatalf("expected timeout mode close to clear last_open_seconds, got %d", lastOpenSeconds)
	}
	if persistedHalfOpenSince.Valid {
		t.Fatalf("expected timeout mode close to clear HALF_OPEN_SINCE, got %v", persistedHalfOpenSince.Int64)
	}
}

func TestInitSQLRuntimeClaimTimeoutModePartialCloseUsesSuccessfulEvidence(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		hostnameHash         string
		hostname             string
		now                  int
		successStreak        int
		wantState            string
		wantOpenUntil        sql.NullInt64
		wantLastOpenSeconds  int
		wantHalfOpenCleared  bool
		wantLastErrorCleared bool
	}{
		{
			name:                 "closes when success evidence exists",
			hostnameHash:         "runtime-half-open-timeout-partial-close-host",
			hostname:             "partial-close.sharepoint.com",
			now:                  1_700_000_900,
			successStreak:        1,
			wantState:            "closed",
			wantOpenUntil:        sql.NullInt64{},
			wantLastOpenSeconds:  0,
			wantHalfOpenCleared:  true,
			wantLastErrorCleared: true,
		},
		{
			name:                 "reopens when no success evidence exists",
			hostnameHash:         "runtime-half-open-timeout-partial-open-host",
			hostname:             "partial-open.sharepoint.com",
			now:                  1_700_000_950,
			successStreak:        0,
			wantState:            "open",
			wantOpenUntil:        sql.NullInt64{Int64: 1_700_000_954, Valid: true},
			wantLastOpenSeconds:  4,
			wantHalfOpenCleared:  true,
			wantLastErrorCleared: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := requireRuntimeBreakerDB(t)
			ctx := context.Background()
			halfOpenSince := tc.now - 6

			_, err := db.ExecContext(ctx, `
				INSERT INTO "THROTTLE_PROTECTION" (
					"HOSTNAME_HASH", "HOSTNAME", "STATE", "OPEN_UNTIL", "EWMA_SCORE", "TOTAL_SAMPLES", "SAMPLES_SINCE_RESET",
					"SUCCESS_STREAK", "PROBE_LEASE_UNTIL", "HALF_OPEN_SINCE", "LAST_SAMPLE_AT", "LAST_ERROR_CODE", "OPEN_REASON", "LAST_OPEN_SECONDS", "VERSION"
				) VALUES ($1, $2, 'half_open', NULL, 0.2, 9, 3, $3, $4, $5, $6, 429, 'http_429', 4, 70)
			`, tc.hostnameHash, tc.hostname, tc.successStreak, tc.now-1, halfOpenSince, tc.now-30)
			if err != nil {
				t.Fatalf("seed timed-out half_open row for partial-close mode: %v", err)
			}

			var (
				state                  string
				openUntil              sql.NullInt64
				version                int64
				probeGranted           bool
				lastOpenSeconds        int
				lastErrorCode          sql.NullInt64
				persistedHalfOpenSince sql.NullInt64
			)
			err = db.QueryRowContext(ctx, `
				SELECT "STATE", "OPEN_UNTIL", "VERSION", "PROBE_GRANTED"
				FROM download_claim_breaker_probe($1, $2, $3, $4, $5, $6)
			`, tc.hostnameHash, tc.hostname, tc.now, 15, 5, "partial-close").Scan(
				&state,
				&openUntil,
				&version,
				&probeGranted,
			)
			if err != nil {
				t.Fatalf("claim timed-out half_open row with partial-close mode: %v", err)
			}
			if state != tc.wantState {
				t.Fatalf("expected partial-close mode to end in %q, got %q", tc.wantState, state)
			}
			if openUntil != tc.wantOpenUntil {
				t.Fatalf("expected partial-close mode open_until %v, got %v", tc.wantOpenUntil, openUntil)
			}
			if version != 71 {
				t.Fatalf("expected partial-close mode to advance version to 71, got %d", version)
			}
			if probeGranted {
				t.Fatalf("expected partial-close mode to resolve without granting a probe")
			}

			err = db.QueryRowContext(ctx, `
				SELECT "LAST_OPEN_SECONDS", "LAST_ERROR_CODE", "HALF_OPEN_SINCE"
				FROM "THROTTLE_PROTECTION"
				WHERE "HOSTNAME_HASH" = $1
			`, tc.hostnameHash).Scan(
				&lastOpenSeconds,
				&lastErrorCode,
				&persistedHalfOpenSince,
			)
			if err != nil {
				t.Fatalf("query partial-close row: %v", err)
			}
			if lastOpenSeconds != tc.wantLastOpenSeconds {
				t.Fatalf("expected partial-close mode last_open_seconds %d, got %d", tc.wantLastOpenSeconds, lastOpenSeconds)
			}
			if tc.wantLastErrorCleared && lastErrorCode.Valid {
				t.Fatalf("expected partial-close close branch to clear last_error_code, got %v", lastErrorCode)
			}
			if !tc.wantLastErrorCleared && (!lastErrorCode.Valid || lastErrorCode.Int64 != 429) {
				t.Fatalf("expected partial-close reopen branch to preserve last_error_code 429, got %v", lastErrorCode)
			}
			if tc.wantHalfOpenCleared && persistedHalfOpenSince.Valid {
				t.Fatalf("expected partial-close mode to clear HALF_OPEN_SINCE, got %v", persistedHalfOpenSince.Int64)
			}
		})
	}
}
