package slothandler

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func readInitSQLNormalized(t *testing.T) string {
	t.Helper()

	path := filepath.Join(moduleRootDir(t), "..", "init.sql")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read init.sql: %v", err)
	}
	return strings.ToLower(string(raw))
}

func batchAcquireFunctionBody(t *testing.T, text string) string {
	t.Helper()

	pattern := `(?s)create\s+or\s+replace\s+function\s+fq_try_acquire_batch\s*\(.*?\)\s*returns\s+table\s*\(.*?\)\s*as\s*\$\$(.*?)\$\$\s*language\s+plpgsql\s*;`
	m := regexp.MustCompile(pattern).FindStringSubmatch(text)
	if len(m) != 2 {
		t.Fatalf("unable to locate fq_try_acquire_batch function body in init.sql")
	}
	return m[1]
}

func mustFindIndex(t *testing.T, text string, pattern string) []int {
	t.Helper()
	idx := regexp.MustCompile(pattern).FindStringIndex(text)
	if idx == nil {
		t.Fatalf("pattern not found: %s", pattern)
	}
	return idx
}

func requireReleaseInBranch(t *testing.T, body string, start []int, end int, name string) {
	t.Helper()
	if end <= start[0] || end > len(body) {
		t.Fatalf("invalid branch range for %s", name)
	}
	branch := body[start[0]:end]
	if !regexp.MustCompile(`func_release_host_slot\(\s*v_host_slot_id\s*,\s*false\s*\)`).MatchString(branch) {
		t.Fatalf("init.sql missing host release in %s branch", name)
	}
}

func TestInitSQLBatchAcquireReleasesHostOnSiteFailure(t *testing.T) {
	body := batchAcquireFunctionBody(t, readInitSQLNormalized(t))
	nullBranch := mustFindIndex(t, body, `if\s+v_site_slot_id\s+is\s+null\s+then`)
	ipTooManyBranch := mustFindIndex(t, body, `if\s+v_site_slot_id\s*=\s*0\s+then`)

	requireReleaseInBranch(t, body, nullBranch, ipTooManyBranch[0], "site-slot NULL failure")
}

func TestInitSQLBatchAcquireReleasesHostOnSiteQueueSignals(t *testing.T) {
	body := batchAcquireFunctionBody(t, readInitSQLNormalized(t))
	ipTooManyBranch := mustFindIndex(t, body, `if\s+v_site_slot_id\s*=\s*0\s+then`)
	queueFullBranch := mustFindIndex(t, body, `elsif\s+v_site_slot_id\s*<\s*0\s+then`)
	acquiredBranch := mustFindIndex(t, body, `status\s*:=\s*'acquired'`)

	requireReleaseInBranch(t, body, ipTooManyBranch, queueFullBranch[0], "site-slot IP_TOO_MANY")
	requireReleaseInBranch(t, body, queueFullBranch, acquiredBranch[0], "site-slot QUEUE_FULL")
}
