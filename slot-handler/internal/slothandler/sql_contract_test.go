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

	pattern := `(?s)create\s+or\s+replace\s+function\s+fq_admit_batch\s*\(.*?\)\s*returns\s+table\s*\(.*?\)\s*as\s*\$\$(.*?)\$\$\s*language\s+plpgsql\s*;`
	m := regexp.MustCompile(pattern).FindStringSubmatch(text)
	if len(m) != 2 {
		t.Fatalf("unable to locate fq_admit_batch function body in init.sql")
	}
	return m[1]
}

func intFunctionBody(t *testing.T, text string, fnName string) string {
	t.Helper()

	pattern := `(?s)create\s+or\s+replace\s+function\s+` + regexp.QuoteMeta(fnName) + `\s*\(.*?\)\s*returns\s+int\s+language\s+plpgsql\s+as\s*\$\$(.*?)\$\$\s*;`
	m := regexp.MustCompile(pattern).FindStringSubmatch(text)
	if len(m) != 2 {
		t.Fatalf("unable to locate %s function body in init.sql", fnName)
	}
	return m[1]
}

func tableFunctionBody(t *testing.T, text string, fnName string) string {
	t.Helper()

	pattern := `(?s)create\s+or\s+replace\s+function\s+` + regexp.QuoteMeta(fnName) + `\s*\(.*?\)\s*returns\s+table\s*\(.*?\)\s*as\s*\$\$(.*?)\$\$\s*language\s+plpgsql\s*;`
	m := regexp.MustCompile(pattern).FindStringSubmatch(text)
	if len(m) != 2 {
		t.Fatalf("unable to locate %s function body in init.sql", fnName)
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
	branch := branchTextInRange(t, body, start, end, name)
	if !regexp.MustCompile(`func_release_host_slot\(\s*v_host_slot_id\s*,\s*false\s*\)`).MatchString(branch) {
		t.Fatalf("init.sql missing host release in %s branch", name)
	}
}

func requireReleaseCallsInBranch(t *testing.T, body string, start []int, end int, name string, patterns ...string) {
	t.Helper()
	branch := branchTextInRange(t, body, start, end, name)
	for _, pattern := range patterns {
		if !regexp.MustCompile(pattern).MatchString(branch) {
			t.Fatalf("init.sql missing %s in %s branch", pattern, name)
		}
	}
}

func requireNullAssignmentsInStatusBranches(t *testing.T, body string, status string, fields ...string) {
	t.Helper()

	branchPattern := regexp.MustCompile(`status\s*:=\s*'` + regexp.QuoteMeta(status) + `'`)
	branches := branchPattern.FindAllStringIndex(body, -1)
	if len(branches) == 0 {
		t.Fatalf("missing %s branch in fq_admit_batch", status)
	}

	nullPattern := regexp.MustCompile(`^null(?:::\w+)?$`)
	returnNextPattern := regexp.MustCompile(`return\s+next\s*;`)

	for _, branch := range branches {
		segment := body[branch[0]:]
		if end := returnNextPattern.FindStringIndex(segment); end != nil {
			segment = segment[:end[1]]
		}

		for _, field := range fields {
			assignmentPattern := regexp.MustCompile(regexp.QuoteMeta(field) + `\s*:=\s*([^;]+);`)
			assignments := assignmentPattern.FindAllStringSubmatch(segment, -1)
			if len(assignments) == 0 {
				t.Fatalf("status %s must assign %s", status, field)
			}
			for _, assignment := range assignments {
				value := strings.TrimSpace(assignment[1])
				if !nullPattern.MatchString(value) {
					t.Fatalf("status %s must assign %s null, got %s", status, field, value)
				}
			}
		}
	}
}

func branchTextInRange(t *testing.T, body string, start []int, end int, name string) string {
	t.Helper()
	if end <= start[0] || end > len(body) {
		t.Fatalf("invalid branch range for %s", name)
	}
	return body[start[0]:end]
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
	readyBranch := mustFindIndex(t, body, `status\s*:=\s*'ready'`)

	requireReleaseInBranch(t, body, ipTooManyBranch, queueFullBranch[0], "site-slot IP_TOO_MANY")
	requireReleaseInBranch(t, body, queueFullBranch, readyBranch[0], "site-slot QUEUE_FULL")
}

func TestInitSQLBatchAcquireHostSlotZeroReturnsIPTooManyWithNullFields(t *testing.T) {
	body := batchAcquireFunctionBody(t, readInitSQLNormalized(t))
	ipTooManyBranch := mustFindIndex(t, body, `if\s+v_host_slot_id\s*=\s*0\s+then`)
	queueFullBranch := mustFindIndex(t, body, `elsif\s+v_host_slot_id\s*<\s*0\s+then`)

	requireReleaseCallsInBranch(
		t,
		body,
		ipTooManyBranch,
		queueFullBranch[0],
		"host-slot IP_TOO_MANY",
		`status\s*:=\s*'ip_too_many'`,
		`slot_token\s*:=\s*null(?:::\w+)?\s*;`,
		`retry_after\s*:=\s*null(?:::\w+)?\s*;`,
		`attempt_version\s*:=\s*null(?:::\w+)?\s*;`,
		`attempt_ticket\s*:=\s*null(?:::\w+)?\s*;`,
	)
}

func TestInitSQLBatchAcquireSiteSlotZeroReturnsIPTooManyAfterReleasingHost(t *testing.T) {
	body := batchAcquireFunctionBody(t, readInitSQLNormalized(t))
	ipTooManyBranch := mustFindIndex(t, body, `if\s+v_site_slot_id\s*=\s*0\s+then`)
	queueFullBranch := mustFindIndex(t, body, `elsif\s+v_site_slot_id\s*<\s*0\s+then`)

	requireReleaseCallsInBranch(
		t,
		body,
		ipTooManyBranch,
		queueFullBranch[0],
		"site-slot IP_TOO_MANY",
		`func_release_host_slot\(\s*v_host_slot_id\s*,\s*false\s*\)`,
		`status\s*:=\s*'ip_too_many'`,
		`slot_token\s*:=\s*null(?:::\w+)?\s*;`,
		`retry_after\s*:=\s*null(?:::\w+)?\s*;`,
		`attempt_version\s*:=\s*null(?:::\w+)?\s*;`,
		`attempt_ticket\s*:=\s*null(?:::\w+)?\s*;`,
	)
}

func TestInitSQLCooldownConditionIncludesZeroActiveSlots(t *testing.T) {
	text := readInitSQLNormalized(t)

	hostBody := intFunctionBody(t, text, "func_try_acquire_host_slot")
	if regexp.MustCompile(`and\s+v_current_ip_slots\s*>\s*0`).MatchString(hostBody) {
		t.Fatalf("host cooldown predicate must not require v_current_ip_slots > 0")
	}
	if !regexp.MustCompile(`and\s+v_current_ip_slots\s*<\s*p_per_ip_limit`).MatchString(hostBody) {
		t.Fatalf("host cooldown predicate must keep under-limit guard")
	}

	siteBody := intFunctionBody(t, text, "func_try_acquire_site_slot")
	if regexp.MustCompile(`and\s+v_current_ip_slots\s*>\s*0`).MatchString(siteBody) {
		t.Fatalf("site cooldown predicate must not require v_current_ip_slots > 0")
	}
	if !regexp.MustCompile(`and\s+v_current_ip_slots\s*<\s*p_per_ip_limit`).MatchString(siteBody) {
		t.Fatalf("site cooldown predicate must keep under-limit guard")
	}
}
