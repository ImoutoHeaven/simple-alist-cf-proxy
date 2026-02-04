package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestInitSQLHasNoQueueDepthOrWaiterDeadCode(t *testing.T) {
	path := filepath.Join("..", "init.sql")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read init.sql: %v", err)
	}
	text := strings.ToLower(string(raw))
	if found, token := containsBannedToken(text); found {
		t.Fatalf("init.sql contains banned token: %s", token)
	}
}

func TestBannedTokenScanIsCaseInsensitive(t *testing.T) {
	buildToken := func(parts ...string) string {
		return strings.Join(parts, "")
	}
	expectedToken := buildToken("fq", "_", "register", "_", "waiter")
	text := "prefix " + strings.ToUpper(expectedToken) + " suffix"
	if found, token := containsBannedToken(strings.ToLower(text)); !found || token != expectedToken {
		t.Fatalf("expected banned token to be detected case-insensitively: %s", expectedToken)
	}
}

func containsBannedToken(text string) (bool, string) {
	buildToken := func(parts ...string) string {
		return strings.Join(parts, "")
	}
	banned := []string{
		buildToken("fq", "_", "register", "_", "waiter"),
		buildToken("fq", "_", "release", "_", "waiter"),
		buildToken("fq", "_", "host", "_", "waiter", "_", "depth"),
		buildToken("fq", "_", "site", "_", "waiter", "_", "depth"),
		buildToken("func", "_", "cleanup", "_", "host", "_", "queue", "_", "depth"),
		buildToken("func", "_", "cleanup", "_", "site", "_", "queue", "_", "depth"),
		buildToken("queue", "_", "depth"),
		buildToken("ip", "_", "queue", "_", "depth"),
		buildToken("waiter", "_", "depth"),
	}
	for _, token := range banned {
		if strings.Contains(text, token) {
			return true, token
		}
	}
	return false, ""
}

func TestInitSQLHasNoFoundUsage(t *testing.T) {
	path := filepath.Join("..", "init.sql")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read init.sql: %v", err)
	}
	if regexp.MustCompile(`(?i)\bfound\b`).Match(raw) {
		t.Fatalf("init.sql must not use plpgsql FOUND")
	}
}

func TestInitSQLHasBatchTryAcquireFunction(t *testing.T) {
	path := filepath.Join("..", "init.sql")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read init.sql: %v", err)
	}
	text := strings.ToLower(string(raw))
	if !strings.Contains(text, "fq_try_acquire_batch") {
		t.Fatalf("init.sql missing fq_try_acquire_batch")
	}
	if strings.Contains(text, "fq_try_acquire_dual") {
		t.Fatalf("init.sql should not include fq_try_acquire_dual")
	}
}
