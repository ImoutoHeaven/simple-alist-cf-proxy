package slothandler

import (
	"regexp"
	"strings"
	"testing"
)

var (
	sqlSingleQuotedLiteralPattern   = regexp.MustCompile(`'(?:''|[^'])*'`)
	sqlLineCommentPattern           = regexp.MustCompile(`--[^\n]*`)
	sqlBlockCommentPattern          = regexp.MustCompile(`(?s)/\*.*?\*/`)
	plpgsqlFoundControlFlowPattern = regexp.MustCompile(`\b(?:if|elsif|elseif)\b[^;]*\bfound\b[^;]*\bthen\b|\bwhile\b[^;]*\bfound\b[^;]*\bloop\b|\b(?:exit|continue)(?:\s+[a-z_][a-z0-9_$]*)?\s+when\b[^;]*\bfound\b[^;]*;|\bcase\s+when\b[^;]*\bfound\b[^;]*\bthen\b|\bcase\b[^;]*\bfound\b[^;]*\bwhen\b[^;]*\bthen\b`)
)

func TestInitSQLHasNoQueueDepthOrWaiterDeadCode(t *testing.T) {
	text := readInitSQLNormalized(t)
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
	if containsPlpgsqlFoundUsage(readInitSQLNormalized(t)) {
		t.Fatalf("init.sql must not use plpgsql FOUND control flow")
	}
}

func TestFoundUsageScannerIgnoresQuotedMessageText(t *testing.T) {
	text := "raise exception 'cq_promote_waiting_request request not found';"
	if containsPlpgsqlFoundUsage(text) {
		t.Fatalf("quoted message text must not be treated as plpgsql FOUND usage")
	}
}

func TestFoundUsageScannerIgnoresQuotedCommentMarkersBeforeControlFlow(t *testing.T) {
	for _, text := range []string{
		"raise notice '--'; if found then",
		"raise notice '/*'; if found then",
	} {
		if !containsPlpgsqlFoundUsage(text) {
			t.Fatalf("expected to detect plpgsql FOUND control flow after quoted comment marker in %q", text)
		}
	}
}

func TestFoundUsageScannerDetectsControlFlow(t *testing.T) {
	for _, text := range []string{
		"if found then",
		"elsif not found then",
		"while found loop",
		"exit when found;",
		"continue when not found;",
	} {
		if !containsPlpgsqlFoundUsage(text) {
			t.Fatalf("expected to detect plpgsql FOUND control flow in %q", text)
		}
	}
}

func TestFoundUsageScannerDetectsParenthesizedAndCaseForms(t *testing.T) {
	for _, text := range []string{
		"if (found) then",
		"if not (found) then",
		"elsif (found) then",
		"while not (found) loop",
		"continue when (found);",
		"continue when not (found);",
		"case when found then perform 1;",
		"case when (found) then perform 1;",
	} {
		if !containsPlpgsqlFoundUsage(text) {
			t.Fatalf("expected to detect parenthesized or case FOUND control flow in %q", text)
		}
	}
}

func TestFoundUsageScannerDetectsElseIfLabeledAndCaseExpressionForms(t *testing.T) {
	for _, text := range []string{
		"elseif found then",
		"exit myloop when found;",
		"continue myloop when found;",
		"exit myloop when not (found);",
		"continue myloop when not (found);",
		"case found when true then perform 1; end case;",
		"case (found) when true then perform 1; end case;",
	} {
		if !containsPlpgsqlFoundUsage(text) {
			t.Fatalf("expected to detect elseif, labeled, or case-expression FOUND control flow in %q", text)
		}
	}
}

func containsPlpgsqlFoundUsage(text string) bool {
	stripped := strings.ToLower(text)
	stripped = sqlSingleQuotedLiteralPattern.ReplaceAllString(stripped, " ")
	stripped = sqlBlockCommentPattern.ReplaceAllString(stripped, " ")
	stripped = sqlLineCommentPattern.ReplaceAllString(stripped, " ")
	return plpgsqlFoundControlFlowPattern.MatchString(stripped)
}

func TestInitSQLHasBatchAdmissionFunction(t *testing.T) {
	text := readInitSQLNormalized(t)
	if !strings.Contains(text, "fq_admit_batch") {
		t.Fatalf("init.sql missing fq_admit_batch")
	}
	if strings.Contains(text, "fq_try_acquire_batch") {
		t.Fatalf("init.sql should not include fq_try_acquire_batch")
	}
	if strings.Contains(text, "fq_try_acquire_dual") {
		t.Fatalf("init.sql should not include fq_try_acquire_dual")
	}
}
