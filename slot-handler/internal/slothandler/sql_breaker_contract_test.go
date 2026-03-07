package slothandler

import (
	"regexp"
	"testing"
)

func TestInitSQLThrottleProtectionUsesBreakerStateColumns(t *testing.T) {
	text := readInitSQLNormalized(t)
	for _, want := range []string{
		`"state"\s+text`,
		`"open_until"\s+integer`,
		`"ewma_score"\s+numeric`,
		`"success_streak"\s+integer`,
		`"probe_lease_until"\s+integer`,
		`"version"\s+bigint`,
	} {
		if !regexp.MustCompile(want).MatchString(text) {
			t.Fatalf("missing breaker column pattern %s", want)
		}
	}

	for _, legacy := range []string{`"is_protected"`, `"error_timestamp"`, `"obs_window_start"`} {
		if regexp.MustCompile(legacy).MatchString(text) {
			t.Fatalf("legacy throttle column still present: %s", legacy)
		}
	}
}

func TestInitSQLDefinesClaimAndReportBreakerFunctions(t *testing.T) {
	text := readInitSQLNormalized(t)
	for _, fn := range []string{
		`create\s+or\s+replace\s+function\s+download_claim_breaker_probe`,
		`create\s+or\s+replace\s+function\s+download_report_breaker_sample`,
	} {
		if !regexp.MustCompile(fn).MatchString(text) {
			t.Fatalf("missing function: %s", fn)
		}
	}
}

func TestInitSQLReportBreakerSampleAcceptsProbeVersion(t *testing.T) {
	text := readInitSQLNormalized(t)
	if !regexp.MustCompile(`download_report_breaker_sample\s*\([^)]*p_probe_version\s+bigint`).MatchString(text) {
		t.Fatalf("download_report_breaker_sample must accept p_probe_version")
	}
}

func TestInitSQLBreakerFunctionsDoNotUseLegacyMigrationDDL(t *testing.T) {
	text := readInitSQLNormalized(t)
	if regexp.MustCompile(`drop\s+function\s+if\s+exists\s+download_report_breaker_sample`).MatchString(text) {
		t.Fatalf("init.sql must not include legacy breaker function migration DDL")
	}
	if regexp.MustCompile(`alter\s+table`).MatchString(text) {
		t.Fatalf("init.sql must stay schema-clean and avoid ALTER TABLE migration DDL")
	}
}

func TestInitSQLReportBreakerSampleDoesNotPromoteOpenToHalfOpen(t *testing.T) {
	body := tableFunctionBody(t, readInitSQLNormalized(t), "download_report_breaker_sample")
	if regexp.MustCompile(`if\s+v_state\s*=\s*'open'\s+and(?s:.*?)v_state\s*:=\s*'half_open'`).MatchString(body) {
		t.Fatalf("download_report_breaker_sample must not advance open to half_open")
	}
}

func TestInitSQLClaimBreakerProbeOwnsOpenToHalfOpenTransition(t *testing.T) {
	body := tableFunctionBody(t, readInitSQLNormalized(t), "download_claim_breaker_probe")
	if !regexp.MustCompile(`if\s+v_state\s*=\s*'open'\s+and(?s:.*?)v_state\s*:=\s*'half_open'`).MatchString(body) {
		t.Fatalf("download_claim_breaker_probe must own open to half_open")
	}
}

func TestInitSQLReportBreakerSampleConsumesAcceptedProbeVersion(t *testing.T) {
	body := tableFunctionBody(t, readInitSQLNormalized(t), "download_report_breaker_sample")
	if !regexp.MustCompile(`if\s+p_probe_version\s+is\s+not\s+null\s+then(?s:.*?)if\s+v_state\s*<>\s*'half_open'\s+or\s+not\s+v_probe_version_matches\s+then(?s:.*?)return\s+query`).MatchString(body) {
		t.Fatalf("download_report_breaker_sample must no-op stale or duplicate probe-version reports")
	}
	if !regexp.MustCompile(`if\s+p_probe_version\s+is\s+not\s+null\s+then(?s:.*?)v_version\s*:=\s*v_version\s*\+\s*1`).MatchString(body) {
		t.Fatalf("download_report_breaker_sample must retire an accepted probe version after one use")
	}
}

func TestInitSQLBatchAcquireKeepsRawBreakerMetadataOutsideOpenState(t *testing.T) {
	body := batchAcquireFunctionBody(t, readInitSQLNormalized(t))
	if regexp.MustCompile(`if\s+not\s+v_throttled\s+then(?s:.*?)v_(throttle_code|breaker_open_until|breaker_reason|breaker_version)\s*:=\s*null`).MatchString(body) {
		t.Fatalf("fq_try_acquire_batch must keep raw breaker metadata even when not actively open")
	}
}

func TestInitSQLOmitsLegacyThrottleProtectionEnvOverrideComment(t *testing.T) {
	text := readInitSQLNormalized(t)
	if regexp.MustCompile(`throttle_protection_table`).MatchString(text) {
		t.Fatalf("init.sql should not document legacy THROTTLE_PROTECTION table override")
	}
}
