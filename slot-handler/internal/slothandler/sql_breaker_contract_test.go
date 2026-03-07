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
