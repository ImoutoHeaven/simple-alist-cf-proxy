package slothandler

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readTextFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}

func TestSlotHandlerReadmeDocumentsRootRunCommands(t *testing.T) {
	path := filepath.Join(moduleRootDir(t), "README.md")
	text := readTextFile(t, path)

	required := []string{
		"以下 Docker 与 Compose 命令均在仓库根目录执行。",
		"docker build -t slot-handler:local -f slot-handler/Dockerfile slot-handler",
		"docker compose -f slot-handler/docker-compose.yml up --build",
		"容器通过只读 bind mount 读取 `slot-handler/config.json`，容器内路径固定为 `/app/config.json`。",
		"CI 会执行：",
		"slot-handler/**",
		".github/workflows/slot-handler-ci.yml",
		"workflow_dispatch",
		"在 `slot-handler/` 工作目录内执行 `go test ./...`",
		"在 `slot-handler/` 工作目录内执行 `go build ./...`",
		"docker build -t slot-handler:ci -f slot-handler/Dockerfile slot-handler",
		"docker compose -f slot-handler/docker-compose.yml build",
		"基于 `slot-handler/config.json` 的容器启动与 `/api/v0/health` 鉴权烟测",
		"curl -i -H \"Authorization: Bearer <internalApiToken>\" http://127.0.0.1:8080/api/v0/health",
		"根目录 `go.work` 已移除。",
		"go -C slot-handler test ./...",
		"go -C slot-handler build ./...",
	}
	for _, token := range required {
		if !strings.Contains(text, token) {
			t.Fatalf("expected README.md to contain %q", token)
		}
	}
}

func TestRepoDocsDescribeSSEWaitProtocol(t *testing.T) {
	repoRoot := filepath.Join(moduleRootDir(t), "..")
	docs := []struct {
		name string
		path string
	}{
		{name: "repo README", path: filepath.Join(repoRoot, "README.md")},
		{name: "download worker architecture", path: filepath.Join(repoRoot, "download-worker-architecture.md")},
		{name: "concurrency-handler README", path: filepath.Join(repoRoot, "concurrency-handler", "README.md")},
		{name: "slot-handler README", path: filepath.Join(repoRoot, "slot-handler", "README.md")},
	}

	required := []string{
		"/api/v1/concurrency/wait",
		"/api/v1/fairqueue/wait",
		"text/event-stream",
		"CQ: acquire fast HTTP -> wait SSE -> claim HTTP -> ack_handoff HTTP -> heartbeat WebSocket -> origin fetch -> release HTTP",
		"a promoted grant releases as `unused_grant` before origin dispatch",
		"as `after_use` after origin dispatch",
		"active waiter",
		"final SSE result events repeat the accepted ownership tuple",
	}
	legacyContinueWaitLongPolling := "continue" + "-wait long " + "poll" + "ing"
	legacyReconnectWithQueryToken := "reconnect with query" + "Token"
	legacyPendingPoll := "pending " + "poll"
	legacyFairQueueAcquire := "/api/v1/fairqueue/" + "acquire"
	legacyWaitPollWindowKey := "waitPoll" + "WindowMs"
	legacyWaitReconnectGraceKey := "waitReconnect" + "GraceMs"
	legacyPerRequestTimeoutKey := "perRequest" + "TimeoutMs"
	legacyMaxAttemptsCapKey := "maxAttempts" + "Cap"
	banned := []string{
		legacyContinueWaitLongPolling,
		legacyReconnectWithQueryToken,
		legacyPendingPoll,
		legacyFairQueueAcquire,
		legacyWaitPollWindowKey,
		legacyWaitReconnectGraceKey,
		legacyPerRequestTimeoutKey,
		legacyMaxAttemptsCapKey,
		"detachGraceMs",
		"waitToken 续连",
		"acquire 轮询",
		"长轮询",
		"No compatibility mode, long-poll fallback, or automatic SSE reconnect exists.",
		"/api/v1/fairqueue/abandon",
		"pre-grant `/abandon`",
	}

	for _, doc := range docs {
		text := readTextFile(t, doc.path)
		for _, token := range required {
			if !strings.Contains(text, token) {
				t.Fatalf("expected %s to contain %q", doc.name, token)
			}
		}
		for _, token := range banned {
			if strings.Contains(text, token) {
				t.Fatalf("expected %s to omit legacy wait polling phrase %q", doc.name, token)
			}
		}
	}
}

func TestRepoDocsDescribeCQWaitFairQueueReleaseBoundary(t *testing.T) {
	repoRoot := filepath.Join(moduleRootDir(t), "..")
	docs := []string{
		filepath.Join(repoRoot, "README.md"),
		filepath.Join(repoRoot, "download-worker-architecture.md"),
		filepath.Join(repoRoot, "concurrency-handler", "README.md"),
	}
	required := []string{
		"before opening CQ SSE",
		"no live FQ fingerprint",
		"no `after_use` transition",
		"no second FQ release",
	}

	for _, path := range docs {
		text := readTextFile(t, path)
		for _, token := range required {
			if !strings.Contains(text, token) {
				t.Fatalf("expected %s to contain %q", path, token)
			}
		}
	}
}

func TestRepoDocsDescribeCleanBreakReleaseDeployment(t *testing.T) {
	repoRoot := filepath.Join(moduleRootDir(t), "..")
	docs := []string{
		filepath.Join(repoRoot, "README.md"),
		filepath.Join(repoRoot, "download-worker-architecture.md"),
		filepath.Join(repoRoot, "concurrency-handler", "README.md"),
		filepath.Join(repoRoot, "slot-handler", "README.md"),
	}

	for _, path := range docs {
		text := readTextFile(t, path)
		for _, token := range []string{"clean-break release", "mixed versions are unsupported"} {
			if !strings.Contains(text, token) {
				t.Fatalf("expected %s to contain %q", path, token)
			}
		}
	}
}

func TestSlotHandlerReadmeDocumentsZeroSmoothReleaseInterval(t *testing.T) {
	path := filepath.Join(moduleRootDir(t), "README.md")
	text := readTextFile(t, path)
	if !strings.Contains(text, "`smoothReleaseIntervalMs=0` disables public release spacing") {
		t.Fatalf("expected README.md to document zero smooth release interval")
	}
}

func TestFairQueueAbandonRouteIsRemoved(t *testing.T) {
	path := filepath.Join(moduleRootDir(t), "internal", "slothandler", "server.go")
	text := readTextFile(t, path)
	for _, token := range []string{"/api/v1/fairqueue/abandon", "/api/v1/fairqueue/acquire", "handleAbandon("} {
		if strings.Contains(text, token) {
			t.Fatalf("expected server.go to omit removed fairqueue abandon route token %q", token)
		}
	}
}

func TestSlotHandlerReadmeOmitsAbandonContract(t *testing.T) {
	path := filepath.Join(moduleRootDir(t), "README.md")
	text := readTextFile(t, path)
	for _, token := range []string{"/api/v1/fairqueue/abandon", "/api/v1/fairqueue/acquire", "release or abandon HTTP cleanup", "pre-grant `/abandon`"} {
		if strings.Contains(text, token) {
			t.Fatalf("expected slot-handler README to omit removed fairqueue abandon contract token %q", token)
		}
	}
}

func TestSlotHandlerMetricsSnapshotOmitsReadyLatchMetrics(t *testing.T) {
	s := newTestServer()
	s.updateRuntime(&Config{FairQueue: FairQueueConfig{}}, &stubBackend{}, "test", true)

	snap := s.collectMetricsSnapshot()
	for _, name := range []string{"ready_latch_expire_count", "ready_latched_count", "ready_latch_age_ms"} {
		if _, ok := snap.Counts[name]; ok {
			t.Fatalf("expected counts to omit ready-latch metric %q, got %+v", name, snap.Counts)
		}
		if _, ok := snap.Metrics[name]; ok {
			t.Fatalf("expected metrics to omit ready-latch metric %q, got %+v", name, snap.Metrics)
		}
	}
}
