package concurrencyhandler

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

type heartbeatScheduleEntry struct {
	deadlineMs int64
	timer      *time.Timer
}

type heartbeatSession struct {
	requestID  string
	leaseID    string
	leaseToken string
	generation int64
	accepted   bool
	skipClose  bool
}

type heartbeatEnvelope struct {
	Type string `json:"type"`
}

type heartbeatHelloMessage struct {
	Type             string `json:"type"`
	RequestID        string `json:"requestId"`
	LeaseID          string `json:"leaseId"`
	LeaseToken       string `json:"leaseToken"`
	HardExpireAtMs   int64  `json:"hardExpireAtMs"`
	ClientInstanceID string `json:"clientInstanceId"`
	Attempt          int64  `json:"attempt"`
	NowMs            int64  `json:"nowMs"`
}

type heartbeatHelloAckMessage struct {
	Type                string `json:"type"`
	Generation          int64  `json:"generation"`
	DeadlineMs          int64  `json:"deadlineMs"`
	AckTimeoutMs        int64  `json:"ackTimeoutMs"`
	HeartbeatIntervalMs int64  `json:"heartbeatIntervalMs"`
	HeartbeatTimeoutMs  int64  `json:"heartbeatTimeoutMs"`
	ReconnectGraceMs    int64  `json:"reconnectGraceMs"`
	StartTimeoutMs      int64  `json:"startTimeoutMs"`
	HardExpireAtMs      int64  `json:"hardExpireAtMs"`
}

type heartbeatRefreshMessage struct {
	Type       string `json:"type"`
	RequestID  string `json:"requestId"`
	LeaseID    string `json:"leaseId"`
	LeaseToken string `json:"leaseToken"`
	Generation int64  `json:"generation"`
	NowMs      int64  `json:"nowMs"`
}

type heartbeatAckMessage struct {
	Type           string `json:"type"`
	Generation     int64  `json:"generation"`
	DeadlineMs     int64  `json:"deadlineMs"`
	HardExpireAtMs int64  `json:"hardExpireAtMs"`
}

type heartbeatTerminalMessage struct {
	Type   string `json:"type"`
	Result string `json:"result"`
	Reason string `json:"reason"`
}

type heartbeatRuntime struct {
	cfg     ConcurrencyHeartbeatConfig
	backend heartbeatBackend

	mu                 sync.Mutex
	deadlines          map[string]*heartbeatScheduleEntry
	currentGenerations map[string]int64
	onTerminal         func(requestID string, nowMs int64)
	stopped            bool
}

func newHeartbeatRuntime(cfg ConcurrencyHeartbeatConfig, backend heartbeatBackend) *heartbeatRuntime {
	return &heartbeatRuntime{
		cfg:                cfg,
		backend:            backend,
		deadlines:          make(map[string]*heartbeatScheduleEntry),
		currentGenerations: make(map[string]int64),
	}
}

func (h *heartbeatRuntime) setTerminalHook(fn func(requestID string, nowMs int64)) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onTerminal = fn
}

func (h *heartbeatRuntime) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.backend == nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	var (
		session heartbeatSession
		closed  bool
	)
	closeConn := func(code websocket.StatusCode) {
		if closed {
			return
		}
		closed = true
		_ = conn.Close(code, "")
	}
	defer func() {
		if !closed {
			closeConn(websocket.StatusNormalClosure)
		}
		if session.accepted && !session.skipClose {
			h.handleDisconnect(session)
		}
	}()

	hello, ok := h.readHello(conn, r.Context())
	if !ok {
		closeConn(websocket.StatusPolicyViolation)
		return
	}

	handlerNowMs := time.Now().UnixMilli()
	result, err := h.backend.HeartbeatOpen(r.Context(), HeartbeatOpenRequest{
		RequestID:           hello.RequestID,
		LeaseID:             hello.LeaseID,
		LeaseToken:          hello.LeaseToken,
		HardExpireAtMs:      hello.HardExpireAtMs,
		NowMs:               handlerNowMs,
		HeartbeatTimeoutMs:  int64(h.cfg.TimeoutMs),
		AckTimeoutMs:        int64(h.cfg.AckTimeoutMs),
		HeartbeatIntervalMs: int64(h.cfg.IntervalMs),
		ReconnectGraceMs:    int64(h.cfg.ReconnectGraceMs),
		StartTimeoutMs:      int64(h.cfg.StartTimeoutMs),
	})
	if err != nil || result == nil {
		closeConn(websocket.StatusInternalError)
		return
	}

	switch result.Result {
	case "accepted":
		session = heartbeatSession{
			requestID:  hello.RequestID,
			leaseID:    hello.LeaseID,
			leaseToken: hello.LeaseToken,
			generation: result.Generation,
			accepted:   true,
		}
		h.setCurrentGeneration(session.requestID, session.generation)
		h.schedule(session.requestID, result.DeadlineMs)
		if !h.writeJSON(conn, heartbeatHelloAckMessage{
			Type:                "hello_ack",
			Generation:          result.Generation,
			DeadlineMs:          result.DeadlineMs,
			AckTimeoutMs:        result.AckTimeoutMs,
			HeartbeatIntervalMs: result.HeartbeatIntervalMs,
			HeartbeatTimeoutMs:  result.HeartbeatTimeoutMs,
			ReconnectGraceMs:    result.ReconnectGraceMs,
			StartTimeoutMs:      result.StartTimeoutMs,
			HardExpireAtMs:      result.HardExpireAtMs,
		}) {
			closeConn(websocket.StatusInternalError)
			return
		}
	case "released", "expired", "terminal":
		h.cancel(hello.RequestID)
		h.invokeTerminal(hello.RequestID, time.Now().UnixMilli())
		h.writeTerminal(conn, result.Result, result.Reason)
		session.skipClose = true
		closeConn(websocket.StatusNormalClosure)
		return
	case "conflict":
		if reason, ok := mapConflictReasonToTerminal(result.Reason); ok {
			h.writeTerminal(conn, "terminal", reason)
		}
		closeConn(websocket.StatusPolicyViolation)
		return
	default:
		closeConn(websocket.StatusPolicyViolation)
		return
	}

	for {
		messageType, payload, err := conn.Read(r.Context())
		if err != nil {
			if websocket.CloseStatus(err) != -1 {
				closed = true
			}
			return
		}
		if messageType != websocket.MessageText {
			h.writeTerminal(conn, "terminal", "protocol_error")
			closeConn(websocket.StatusPolicyViolation)
			return
		}
		if h.handleHeartbeatMessage(conn, r.Context(), session, payload) {
			continue
		}
		closeConn(websocket.StatusPolicyViolation)
		return
	}
}

func (h *heartbeatRuntime) handleHeartbeatMessage(conn *websocket.Conn, ctx context.Context, session heartbeatSession, payload []byte) bool {
	envelope := heartbeatEnvelope{}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		h.writeTerminal(conn, "terminal", "protocol_error")
		return false
	}
	if envelope.Type != "heartbeat" {
		h.writeTerminal(conn, "terminal", "protocol_error")
		return false
	}

	message := heartbeatRefreshMessage{}
	if err := json.Unmarshal(payload, &message); err != nil || !validHeartbeatRefreshMessage(message) {
		h.writeTerminal(conn, "terminal", "protocol_error")
		return false
	}
	if message.RequestID != session.requestID || message.LeaseID != session.leaseID || message.LeaseToken != session.leaseToken {
		h.writeTerminal(conn, "terminal", "token_mismatch")
		return false
	}
	if message.Generation != session.generation || !h.isCurrentGeneration(session.requestID, session.generation) {
		return true
	}

	result, err := h.backend.HeartbeatRefresh(ctx, HeartbeatRefreshRequest{
		RequestID:          session.requestID,
		LeaseID:            session.leaseID,
		LeaseToken:         session.leaseToken,
		Generation:         message.Generation,
		NowMs:              time.Now().UnixMilli(),
		HeartbeatTimeoutMs: int64(h.cfg.TimeoutMs),
	})
	if err != nil || result == nil {
		return false
	}

	switch result.Result {
	case "accepted":
		h.schedule(session.requestID, result.DeadlineMs)
		return h.writeJSON(conn, heartbeatAckMessage{
			Type:           "heartbeat_ack",
			Generation:     result.Generation,
			DeadlineMs:     result.DeadlineMs,
			HardExpireAtMs: result.HardExpireAtMs,
		})
	case "released", "expired", "terminal":
		h.cancel(session.requestID)
		h.invokeTerminal(session.requestID, time.Now().UnixMilli())
		h.writeTerminal(conn, result.Result, result.Reason)
		return false
	case "conflict":
		if reason, ok := mapConflictReasonToTerminal(result.Reason); ok {
			h.writeTerminal(conn, "terminal", reason)
		}
		return false
	case "noop":
		return true
	default:
		return false
	}
}

func (h *heartbeatRuntime) readHello(conn *websocket.Conn, requestCtx context.Context) (heartbeatHelloMessage, bool) {
	ctx, cancel := context.WithTimeout(requestCtx, time.Duration(h.cfg.HelloTimeoutMs)*time.Millisecond)
	defer cancel()
	messageType, payload, err := conn.Read(ctx)
	if err != nil || messageType != websocket.MessageText {
		return heartbeatHelloMessage{}, false
	}
	envelope := heartbeatEnvelope{}
	if err := json.Unmarshal(payload, &envelope); err != nil || envelope.Type != "hello" {
		return heartbeatHelloMessage{}, false
	}
	hello := heartbeatHelloMessage{}
	if err := json.Unmarshal(payload, &hello); err != nil || !validHeartbeatHelloMessage(hello) {
		return heartbeatHelloMessage{}, false
	}
	return hello, true
}

func (h *heartbeatRuntime) handleDisconnect(session heartbeatSession) {
	if h == nil || h.backend == nil || session.requestID == "" || !h.isCurrentGeneration(session.requestID, session.generation) {
		return
	}
	result, err := h.backend.HeartbeatDisconnect(context.Background(), HeartbeatDisconnectRequest{
		RequestID:        session.requestID,
		LeaseID:          session.leaseID,
		LeaseToken:       session.leaseToken,
		Generation:       session.generation,
		NowMs:            time.Now().UnixMilli(),
		ReconnectGraceMs: int64(h.cfg.ReconnectGraceMs),
	})
	if err != nil || result == nil {
		return
	}
	switch result.Result {
	case "accepted":
		h.schedule(session.requestID, result.DeadlineMs)
	case "released", "expired", "terminal":
		h.cancel(session.requestID)
		h.invokeTerminal(session.requestID, time.Now().UnixMilli())
	}
}

func (h *heartbeatRuntime) schedule(requestID string, deadlineMs int64) {
	if h == nil {
		return
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" || deadlineMs <= 0 {
		return
	}
	entry := &heartbeatScheduleEntry{deadlineMs: deadlineMs}
	delay := time.Until(time.UnixMilli(deadlineMs))
	if delay < 0 {
		delay = 0
	}

	h.mu.Lock()
	if h.stopped {
		h.mu.Unlock()
		return
	}
	if existing := h.deadlines[requestID]; existing != nil && existing.timer != nil {
		existing.timer.Stop()
	}
	h.deadlines[requestID] = entry
	h.mu.Unlock()

	timer := time.AfterFunc(delay, func() {
		h.expireScheduledRequest(requestID, deadlineMs)
	})

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped {
		timer.Stop()
		delete(h.deadlines, requestID)
		return
	}
	if h.deadlines[requestID] != entry {
		timer.Stop()
		return
	}
	entry.timer = timer
}

func (h *heartbeatRuntime) expireScheduledRequest(requestID string, scheduledDeadline int64) {
	if h == nil || h.backend == nil || !h.isScheduledDeadline(requestID, scheduledDeadline) {
		return
	}
	nowMs := time.Now().UnixMilli()
	result, err := h.backend.ExpireHeartbeatIfDue(context.Background(), ExpireHeartbeatRequest{RequestID: requestID, NowMs: nowMs})
	if err != nil || result == nil {
		if h.isScheduledDeadline(requestID, scheduledDeadline) {
			h.schedule(requestID, nowMs+200)
		}
		return
	}
	switch result.Result {
	case "released", "expired", "terminal":
		h.cancel(requestID)
		h.invokeTerminal(requestID, nowMs)
	case "noop":
		if result.Reason == "not_due" && result.DeadlineMs > nowMs {
			h.schedule(requestID, result.DeadlineMs)
		}
	case "conflict":
		if result.Reason == "request_not_found" || result.Reason == "invalid_request_state" {
			h.cancel(requestID)
		}
	}
}

func (h *heartbeatRuntime) cancel(requestID string) {
	if h == nil {
		return
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if entry := h.deadlines[requestID]; entry != nil && entry.timer != nil {
		entry.timer.Stop()
	}
	delete(h.deadlines, requestID)
	delete(h.currentGenerations, requestID)
}

func (h *heartbeatRuntime) recoverActiveDeadlines(ctx context.Context, nowMs int64) error {
	if h == nil || h.backend == nil {
		return nil
	}
	rows, err := h.backend.LoadActiveHeartbeatDeadlines(ctx, nowMs, 0)
	if err != nil {
		return err
	}
	for _, row := range rows {
		h.schedule(row.RequestID, row.DeadlineMs)
	}
	return nil
}

func (h *heartbeatRuntime) close() {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stopped = true
	for requestID, entry := range h.deadlines {
		if entry != nil && entry.timer != nil {
			entry.timer.Stop()
		}
		delete(h.deadlines, requestID)
	}
	h.currentGenerations = make(map[string]int64)
}

func (h *heartbeatRuntime) scheduledDeadline(requestID string) int64 {
	if h == nil {
		return 0
	}
	requestID = strings.TrimSpace(requestID)
	h.mu.Lock()
	defer h.mu.Unlock()
	if entry := h.deadlines[requestID]; entry != nil {
		return entry.deadlineMs
	}
	return 0
}

func (h *heartbeatRuntime) currentGeneration(requestID string) int64 {
	if h == nil {
		return 0
	}
	requestID = strings.TrimSpace(requestID)
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.currentGenerations[requestID]
}

func (h *heartbeatRuntime) setCurrentGeneration(requestID string, generation int64) {
	if h == nil {
		return
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" || generation <= 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.currentGenerations[requestID] = generation
}

func (h *heartbeatRuntime) isCurrentGeneration(requestID string, generation int64) bool {
	if h == nil {
		return false
	}
	requestID = strings.TrimSpace(requestID)
	h.mu.Lock()
	defer h.mu.Unlock()
	return generation > 0 && h.currentGenerations[requestID] == generation
}

func (h *heartbeatRuntime) isScheduledDeadline(requestID string, deadlineMs int64) bool {
	if h == nil {
		return false
	}
	requestID = strings.TrimSpace(requestID)
	h.mu.Lock()
	defer h.mu.Unlock()
	entry := h.deadlines[requestID]
	return entry != nil && entry.deadlineMs == deadlineMs && !h.stopped
}

func (h *heartbeatRuntime) invokeTerminal(requestID string, nowMs int64) {
	if h == nil {
		return
	}
	h.mu.Lock()
	fn := h.onTerminal
	h.mu.Unlock()
	if fn != nil {
		fn(strings.TrimSpace(requestID), nowMs)
	}
}

func (h *heartbeatRuntime) writeJSON(conn *websocket.Conn, payload any) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return wsjson.Write(ctx, conn, payload) == nil
}

func (h *heartbeatRuntime) writeTerminal(conn *websocket.Conn, result, reason string) {
	_ = h.writeJSON(conn, heartbeatTerminalMessage{Type: "terminal", Result: result, Reason: reason})
}

func mapConflictReasonToTerminal(reason string) (string, bool) {
	switch strings.TrimSpace(reason) {
	case "lease_mismatch", "lease_token_mismatch":
		return "token_mismatch", true
	default:
		return "", false
	}
}

func validHeartbeatHelloMessage(message heartbeatHelloMessage) bool {
	return message.Type == "hello" && strings.TrimSpace(message.RequestID) != "" && strings.TrimSpace(message.LeaseID) != "" && strings.TrimSpace(message.LeaseToken) != "" && message.HardExpireAtMs > 0 && strings.TrimSpace(message.ClientInstanceID) != "" && message.Attempt > 0 && message.NowMs > 0
}

func validHeartbeatRefreshMessage(message heartbeatRefreshMessage) bool {
	return message.Type == "heartbeat" && strings.TrimSpace(message.RequestID) != "" && strings.TrimSpace(message.LeaseID) != "" && strings.TrimSpace(message.LeaseToken) != "" && message.Generation > 0 && message.NowMs > 0
}
