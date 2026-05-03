package concurrencyhandler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib"
	"regexp"
	"strings"
	"time"
)

type pgRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

type pgQueryer interface {
	Query(ctx context.Context, query string, args ...any) (pgRows, error)
	Close() error
}

type sqlDBClient struct {
	db *sql.DB
}

func (s *sqlDBClient) Query(ctx context.Context, query string, args ...any) (pgRows, error) {
	return s.db.QueryContext(ctx, query, args...)
}

func (s *sqlDBClient) Close() error {
	return s.db.Close()
}

type postgresBackend struct {
	cfg Config
	db  pgQueryer
}

func newPostgresBackend(cfg Config) (*postgresBackend, error) {
	if strings.TrimSpace(cfg.Backend.Postgres.DSN) == "" {
		return nil, errors.New("postgres dsn is empty")
	}
	db, err := sql.Open("pgx", cfg.Backend.Postgres.DSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return &postgresBackend{cfg: cfg, db: &sqlDBClient{db: db}}, nil
}

func (p *postgresBackend) Close() error {
	if p == nil || p.db == nil {
		return nil
	}
	return p.db.Close()
}

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validatedIdentifier(name string) (string, error) {
	trimmed := strings.TrimSpace(name)
	if !identifierPattern.MatchString(trimmed) {
		return "", fmt.Errorf("invalid identifier %q", name)
	}
	return trimmed, nil
}

func rpcSelectAll(name string, argCount int) (string, error) {
	validated, err := validatedIdentifier(name)
	if err != nil {
		return "", err
	}
	placeholders := make([]string, 0, argCount)
	for i := 1; i <= argCount; i++ {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
	}
	return fmt.Sprintf("SELECT * FROM %s(%s)", validated, strings.Join(placeholders, ",")), nil
}

func rpcSelectColumn(name string, argCount int, column string) (string, error) {
	validated, err := validatedIdentifier(name)
	if err != nil {
		return "", err
	}
	validatedColumn, err := validatedIdentifier(column)
	if err != nil {
		return "", err
	}
	placeholders := make([]string, 0, argCount)
	for i := 1; i <= argCount; i++ {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
	}
	return fmt.Sprintf("SELECT %s FROM %s(%s)", validatedColumn, validated, strings.Join(placeholders, ",")), nil
}

func rpcSelectRowJSON(name string, argCount int) (string, error) {
	validated, err := validatedIdentifier(name)
	if err != nil {
		return "", err
	}
	placeholders := make([]string, 0, argCount)
	for i := 1; i <= argCount; i++ {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
	}
	return fmt.Sprintf("SELECT row_to_json(result_row) FROM %s(%s) AS result_row", validated, strings.Join(placeholders, ",")), nil
}

func (p *postgresBackend) Acquire(ctx context.Context, req AcquireRequest) (*AcquireResult, error) {
	query, err := rpcSelectRowJSON(p.cfg.Concurrency.RPC.AcquireFunc, 14)
	if err != nil {
		return nil, err
	}
	rows, err := p.db.Query(ctx, query,
		req.HostnameHash,
		req.Hostname,
		canonicalBucket(req.SiteBucket),
		canonicalBucket(req.IPBucket),
		req.RequestID,
		req.HardExpireAtMs,
		req.NowMs,
		strings.TrimSpace(req.WaitToken),
		p.cfg.Concurrency.Wait.WaitPollWindowMs,
		p.cfg.Concurrency.Wait.WaitReconnectGraceMs,
		p.cfg.Concurrency.Caps.HostMaxInFlight,
		p.cfg.Concurrency.Caps.SiteMaxInFlight,
		p.cfg.Concurrency.Caps.SiteIPMaxInFlight,
		boundedExpireLimit(p.cfg.Concurrency.Sweep.BatchSize, 500),
	)
	if err != nil {
		return nil, classifyAcquireConflict(err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, errors.New("empty acquire result")
	}
	var raw []byte
	if err := rows.Scan(&raw); err != nil {
		return nil, err
	}
	result, err := decodeAcquireJSONResult(raw)
	if err != nil {
		return nil, err
	}
	if err := validateAcquireResult(req, result); err != nil {
		return nil, err
	}
	return result, rows.Err()
}

func (p *postgresBackend) ProbeContinueWait(ctx context.Context, req AcquireRequest) (*AcquireResult, error) {
	query, err := rpcSelectRowJSON(fixedContinueWaitProbeFunc, 8)
	if err != nil {
		return nil, err
	}
	rows, err := p.db.Query(ctx, query,
		req.HostnameHash,
		req.Hostname,
		canonicalBucket(req.SiteBucket),
		canonicalBucket(req.IPBucket),
		req.RequestID,
		req.HardExpireAtMs,
		req.NowMs,
		strings.TrimSpace(req.WaitToken),
	)
	if err != nil {
		return nil, classifyAcquireConflict(err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, errors.New("empty continue-wait probe result")
	}
	var raw []byte
	if err := rows.Scan(&raw); err != nil {
		return nil, err
	}
	result, err := decodeAcquireJSONResult(raw)
	if err != nil {
		return nil, err
	}
	if err := validateAcquireResult(req, result); err != nil {
		return nil, err
	}
	return result, rows.Err()
}

func (p *postgresBackend) ClaimGrant(ctx context.Context, req ClaimGrantRequest) (*ClaimGrantResult, error) {
	query, err := rpcSelectAll(fixedClaimGrantFunc, 3)
	if err != nil {
		return nil, err
	}
	rows, err := p.db.Query(ctx, query, req.RequestID, req.ClaimToken, req.NowMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, errors.New("empty claim grant result")
	}
	result := &ClaimGrantResult{}
	var leaseID sql.NullString
	var leaseToken sql.NullString
	var expiresAtMs sql.NullInt64
	var handoffToken sql.NullString
	var handoffDeadlineMs sql.NullInt64
	var reason sql.NullString
	if err := rows.Scan(&result.Result, &leaseID, &leaseToken, &expiresAtMs, &handoffToken, &handoffDeadlineMs, &reason); err != nil {
		return nil, err
	}
	result.LeaseID = leaseID.String
	result.LeaseToken = leaseToken.String
	result.ExpiresAtMs = expiresAtMs.Int64
	result.HandoffToken = handoffToken.String
	result.HandoffDeadlineMs = handoffDeadlineMs.Int64
	result.Reason = reason.String
	if err := validateClaimGrantResult(result); err != nil {
		return nil, err
	}
	return result, rows.Err()
}

func (p *postgresBackend) AckHandoff(ctx context.Context, req AckHandoffBackendRequest) (*AckHandoffResult, error) {
	query, err := rpcSelectAll(fixedAckHandoffFunc, 4)
	if err != nil {
		return nil, err
	}
	rows, err := p.db.Query(ctx, query, req.RequestID, req.HandoffToken, req.NowMs, req.StartTimeoutMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, errors.New("empty ack handoff result")
	}
	result := &AckHandoffResult{}
	var reason sql.NullString
	var heartbeatDeadlineMs sql.NullInt64
	if err := rows.Scan(&result.Result, &reason, &heartbeatDeadlineMs); err != nil {
		return nil, err
	}
	result.Reason = reason.String
	result.HeartbeatDeadlineMs = heartbeatDeadlineMs.Int64
	if err := validateAckHandoffResult(result); err != nil {
		return nil, err
	}
	return result, rows.Err()
}

func (p *postgresBackend) HeartbeatOpen(ctx context.Context, req HeartbeatOpenRequest) (*HeartbeatResult, error) {
	query, err := rpcSelectAll("cq_heartbeat_open", 10)
	if err != nil {
		return nil, err
	}
	rows, err := p.db.Query(ctx, query, req.RequestID, req.LeaseID, req.LeaseToken, req.HardExpireAtMs, req.NowMs, req.HeartbeatTimeoutMs, req.AckTimeoutMs, req.HeartbeatIntervalMs, req.ReconnectGraceMs, req.StartTimeoutMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanHeartbeatResult(rows, "empty heartbeat open result")
}

func (p *postgresBackend) HeartbeatRefresh(ctx context.Context, req HeartbeatRefreshRequest) (*HeartbeatResult, error) {
	query, err := rpcSelectAll("cq_heartbeat_refresh", 6)
	if err != nil {
		return nil, err
	}
	rows, err := p.db.Query(ctx, query, req.RequestID, req.LeaseID, req.LeaseToken, req.Generation, req.NowMs, req.HeartbeatTimeoutMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanHeartbeatResult(rows, "empty heartbeat refresh result")
}

func (p *postgresBackend) HeartbeatDisconnect(ctx context.Context, req HeartbeatDisconnectRequest) (*HeartbeatResult, error) {
	query, err := rpcSelectAll("cq_heartbeat_disconnect", 6)
	if err != nil {
		return nil, err
	}
	rows, err := p.db.Query(ctx, query, req.RequestID, req.LeaseID, req.LeaseToken, req.Generation, req.NowMs, req.ReconnectGraceMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanHeartbeatResult(rows, "empty heartbeat disconnect result")
}

func (p *postgresBackend) ExpireHeartbeatIfDue(ctx context.Context, req ExpireHeartbeatRequest) (*HeartbeatResult, error) {
	query, err := rpcSelectAll("cq_expire_heartbeat_if_due", 2)
	if err != nil {
		return nil, err
	}
	rows, err := p.db.Query(ctx, query, req.RequestID, req.NowMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanHeartbeatResult(rows, "empty expire heartbeat result")
}

func (p *postgresBackend) LoadActiveHeartbeatDeadlines(ctx context.Context, nowMs int64, limit int) ([]HeartbeatDeadlineSnapshot, error) {
	query := `
		SELECT request_id, heartbeat_deadline_ms
		FROM concurrency_requests
		WHERE state = 'active'
		  AND heartbeat_deadline_ms IS NOT NULL
		  AND hard_expire_at_ms > $1
		ORDER BY heartbeat_deadline_ms, request_id`
	args := []any{nowMs}
	if limit > 0 {
		query += `
		LIMIT $2`
		args = append(args, limit)
	}
	rows, err := p.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var snapshots []HeartbeatDeadlineSnapshot
	for rows.Next() {
		var snapshot HeartbeatDeadlineSnapshot
		if err := rows.Scan(&snapshot.RequestID, &snapshot.DeadlineMs); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return snapshots, nil
}

func scanHeartbeatResult(rows pgRows, emptyMessage string) (*HeartbeatResult, error) {
	if !rows.Next() {
		return nil, errors.New(emptyMessage)
	}
	result := &HeartbeatResult{}
	var reason sql.NullString
	var generation, deadlineMs, ackTimeoutMs, heartbeatIntervalMs, heartbeatTimeoutMs, reconnectGraceMs, startTimeoutMs, hardExpireAtMs sql.NullInt64
	if err := rows.Scan(&result.Result, &reason, &generation, &deadlineMs, &ackTimeoutMs, &heartbeatIntervalMs, &heartbeatTimeoutMs, &reconnectGraceMs, &startTimeoutMs, &hardExpireAtMs); err != nil {
		return nil, err
	}
	result.Reason = reason.String
	result.Generation = generation.Int64
	result.DeadlineMs = deadlineMs.Int64
	result.AckTimeoutMs = ackTimeoutMs.Int64
	result.HeartbeatIntervalMs = heartbeatIntervalMs.Int64
	result.HeartbeatTimeoutMs = heartbeatTimeoutMs.Int64
	result.ReconnectGraceMs = reconnectGraceMs.Int64
	result.StartTimeoutMs = startTimeoutMs.Int64
	result.HardExpireAtMs = hardExpireAtMs.Int64
	if err := validateHeartbeatResult(result); err != nil {
		return nil, err
	}
	return result, rows.Err()
}

func (p *postgresBackend) Release(ctx context.Context, req ReleaseRequest) (*ReleaseResult, error) {
	query, err := rpcSelectAll(p.cfg.Concurrency.RPC.ReleaseFunc, 4)
	if err != nil {
		return nil, err
	}
	rows, err := p.db.Query(ctx, query, req.LeaseID, req.LeaseToken, req.Reason, req.NowMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, errors.New("empty release result")
	}
	result := &ReleaseResult{}
	var reason sql.NullString
	var requestID sql.NullString
	if err := rows.Scan(&result.Result, &reason, &requestID); err != nil {
		return nil, err
	}
	result.Reason = reason.String
	result.RequestID = requestID.String
	if err := validateReleaseResult(result); err != nil {
		return nil, err
	}
	return result, rows.Err()
}

func (p *postgresBackend) PromoteWaiting(ctx context.Context, req PromoteWaitingRequest) (*AcquireResult, error) {
	query, err := rpcSelectRowJSON(fixedPromoteWaitingFunc, 9)
	if err != nil {
		return nil, err
	}
	rows, err := p.db.Query(
		ctx,
		query,
		req.RequestID,
		req.HostnameHash,
		canonicalBucket(req.SiteBucket),
		canonicalBucket(req.IPBucket),
		req.HardExpireAtMs,
		req.NowMs,
		p.cfg.Concurrency.Caps.HostMaxInFlight,
		p.cfg.Concurrency.Caps.SiteMaxInFlight,
		p.cfg.Concurrency.Caps.SiteIPMaxInFlight,
	)
	if err != nil {
		return nil, classifyAcquireConflict(err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, errors.New("empty promote waiting result")
	}
	var raw []byte
	if err := rows.Scan(&raw); err != nil {
		return nil, err
	}
	result, err := decodeAcquireJSONResult(raw)
	if err != nil {
		return nil, err
	}
	if err := validateAcquireResult(AcquireRequest{HardExpireAtMs: req.HardExpireAtMs}, result); err != nil {
		return nil, err
	}
	return result, rows.Err()
}

func (p *postgresBackend) Cancel(ctx context.Context, req CancelRequest) (*CancelResult, error) {
	query, err := rpcSelectAll(fixedCancelFunc, 8)
	if err != nil {
		return nil, err
	}
	rows, err := p.db.Query(
		ctx,
		query,
		req.RequestID,
		req.Hostname,
		req.HostnameHash,
		canonicalBucket(req.SiteBucket),
		canonicalBucket(req.IPBucket),
		req.HardExpireAtMs,
		req.Reason,
		req.NowMs,
	)
	if err != nil {
		return nil, classifyCancelConflict(err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, errors.New("empty cancel result")
	}
	result := &CancelResult{}
	var reason sql.NullString
	if err := rows.Scan(&result.Result, &reason); err != nil {
		return nil, err
	}
	result.Reason = reason.String
	if err := validateCancelResult(result); err != nil {
		return nil, err
	}
	return result, rows.Err()
}

func (p *postgresBackend) ExpireScope(ctx context.Context, req ExpireScopeRequest) (*ExpireScopeResult, error) {
	query, err := rpcSelectColumn(p.cfg.Concurrency.RPC.ExpireFunc, 6, "request_id")
	if err != nil {
		return nil, err
	}
	rows, err := p.db.Query(ctx, query,
		strings.TrimSpace(req.Scope),
		req.HostnameHash,
		canonicalBucket(req.SiteBucket),
		canonicalBucket(req.IPBucket),
		req.NowMs,
		boundedExpireLimit(req.Limit, p.cfg.Concurrency.Sweep.BatchSize),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := &ExpireScopeResult{}
	for rows.Next() {
		var requestID string
		if err := rows.Scan(&requestID); err != nil {
			return nil, err
		}
		result.ExpiredRequestIDs = append(result.ExpiredRequestIDs, requestID)
	}
	result.ExpiredCount = len(result.ExpiredRequestIDs)
	if err := validateExpireScopeResult(result); err != nil {
		return nil, err
	}
	return result, rows.Err()
}

func (p *postgresBackend) LoadWaitingRequests(ctx context.Context) ([]requestSnapshot, error) {
	rows, err := p.db.Query(ctx, `
		SELECT request_id,
		       hostname,
		       hostname_hash,
		       site_bucket,
		       ip_bucket,
		       wait_token,
		       first_wait_at_ms,
		       waiter_lease_until_ms,
		       hard_expire_at_ms
		FROM concurrency_requests
		WHERE state = 'waiting'
		ORDER BY hostname_hash, first_wait_at_ms, request_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var snapshots []requestSnapshot
	for rows.Next() {
		var snap requestSnapshot
		var waitToken sql.NullString
		var firstWaitAtMs sql.NullInt64
		var waiterLeaseUntilMs sql.NullInt64
		if err := rows.Scan(
			&snap.RequestID,
			&snap.Hostname,
			&snap.HostnameHash,
			&snap.SiteBucket,
			&snap.IPBucket,
			&waitToken,
			&firstWaitAtMs,
			&waiterLeaseUntilMs,
			&snap.HardExpireAtMs,
		); err != nil {
			return nil, err
		}
		snap.State = "waiting"
		snap.WaitToken = waitToken.String
		snap.FirstWaitAtMs = firstWaitAtMs.Int64
		snap.WaiterLeaseUntilMs = waiterLeaseUntilMs.Int64
		snap.TupleKey = makeTupleKey(snap.HostnameHash, snap.SiteBucket, snap.IPBucket)
		snapshots = append(snapshots, snap)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return snapshots, nil
}

func (p *postgresBackend) LoadActiveRequestIDs(ctx context.Context) ([]string, error) {
	rows, err := p.db.Query(ctx, `
		SELECT request_id
		FROM concurrency_requests
		WHERE state = 'active'
		ORDER BY request_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var requestIDs []string
	for rows.Next() {
		var requestID string
		if err := rows.Scan(&requestID); err != nil {
			return nil, err
		}
		requestIDs = append(requestIDs, requestID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return requestIDs, nil
}

func (p *postgresBackend) LoadOverdueHandoffPendingRequestIDs(ctx context.Context, nowMs int64, limit int) ([]string, error) {
	rows, err := p.db.Query(ctx, `
		SELECT request_id
		FROM concurrency_requests
		WHERE state = 'active'
		  AND handoff_state = 'pending'
		  AND handoff_deadline_ms IS NOT NULL
		  AND handoff_deadline_ms <= $1
		  AND hard_expire_at_ms > $1
		  AND lease_expires_at_ms > $1
		ORDER BY handoff_deadline_ms, request_id
		LIMIT $2`, nowMs, boundedExpireLimit(limit, p.cfg.Concurrency.Sweep.BatchSize))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var requestIDs []string
	for rows.Next() {
		var requestID string
		if err := rows.Scan(&requestID); err != nil {
			return nil, err
		}
		requestIDs = append(requestIDs, requestID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return requestIDs, nil
}

func (p *postgresBackend) ExpireActiveRequestIfDue(ctx context.Context, requestID string, nowMs int64) (bool, error) {
	rows, err := p.db.Query(ctx, "SELECT cq_expire_active_request_if_due($1, $2)", requestID, nowMs)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return false, errors.New("empty expire-active-request result")
	}
	var expired bool
	if err := rows.Scan(&expired); err != nil {
		return false, err
	}
	return expired, rows.Err()
}
