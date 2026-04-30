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
		`"samples_since_reset"\s+integer`,
		`"last_sample_at"\s+integer`,
		`"success_streak"\s+integer`,
		`"half_open_since"\s+integer`,
		`"half_open_budget"\s+integer`,
		`"half_open_issued"\s+integer`,
		`"half_open_resolved_mask"\s+bigint`,
		`"half_open_success_mask"\s+bigint`,
		`"half_open_deadline"\s+integer`,
		`"version"\s+bigint`,
	} {
		if !regexp.MustCompile(want).MatchString(text) {
			t.Fatalf("missing breaker column pattern %s", want)
		}
	}
	if regexp.MustCompile(`"probe_lease_until"`).MatchString(text) {
		t.Fatalf("legacy single-probe lease column still present")
	}

	for _, legacy := range []string{`"is_protected"`, `"error_timestamp"`, `"obs_window_start"`} {
		if regexp.MustCompile(legacy).MatchString(text) {
			t.Fatalf("legacy throttle column still present: %s", legacy)
		}
	}
}

func TestInitSQLDefinesAuthorizeAndReportBreakerFunctions(t *testing.T) {
	text := readInitSQLNormalized(t)
	for _, fn := range []string{
		`create\s+or\s+replace\s+function\s+download_authorize_breaker_attempt`,
		`create\s+or\s+replace\s+function\s+download_report_breaker_sample`,
	} {
		if !regexp.MustCompile(fn).MatchString(text) {
			t.Fatalf("missing function: %s", fn)
		}
	}
	if regexp.MustCompile(`create\s+or\s+replace\s+function\s+download_claim_breaker_probe`).MatchString(text) {
		t.Fatalf("legacy single-probe claim function still present")
	}
}

func TestInitSQLReportBreakerSampleAcceptsWarmupAndAttemptParameters(t *testing.T) {
	text := readInitSQLNormalized(t)
	pattern := `download_report_breaker_sample\s*\([^)]*p_min_samples_before_ewma_open\s+integer[^)]*p_idle_reset_seconds\s+integer[^)]*p_attempt_version\s+bigint\s+default\s+null[^)]*p_attempt_ticket\s+integer\s+default\s+null`
	if !regexp.MustCompile(pattern).MatchString(text) {
		t.Fatalf("download_report_breaker_sample must accept warmup, idle reset, and attempt version/ticket parameters")
	}
	if regexp.MustCompile(`p_probe_version\s+bigint`).MatchString(text) {
		t.Fatalf("legacy single-probe report parameter still present")
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

func TestInitSQLAuthorizeBreakerAttemptOwnsOpenToHalfOpenTransition(t *testing.T) {
	body := tableFunctionBody(t, readInitSQLNormalized(t), "func_authorize_breaker_attempt")
	pattern := `if\s+v_state\s*=\s*'open'\s+and\s+v_open_until\s*<=\s*v_now\s+then(?s:.*?)v_state\s*:=\s*'half_open'(?s:.*?)v_half_open_budget\s*:=\s*v_half_open_max_probe_count(?s:.*?)v_half_open_issued\s*:=\s*0(?s:.*?)v_half_open_resolved_mask\s*:=\s*0(?s:.*?)v_half_open_success_mask\s*:=\s*0(?s:.*?)v_half_open_deadline\s*:=\s*v_now\s*\+\s*v_half_open_max_seconds`
	if !regexp.MustCompile(pattern).MatchString(body) {
		t.Fatalf("func_authorize_breaker_attempt must own open->half_open batch normalization")
	}
}

func TestInitSQLReportBreakerSampleTracksAcceptedAttemptTickets(t *testing.T) {
	body := tableFunctionBody(t, readInitSQLNormalized(t), "download_report_breaker_sample")
	if !regexp.MustCompile(`if\s+v_state\s*=\s*'half_open'\s+then(?s:.*?)if\s+p_attempt_version\s*<>\s*v_version\s+then(?s:.*?)return\s+query`).MatchString(body) {
		t.Fatalf("download_report_breaker_sample must no-op stale half_open epoch reports")
	}
	if !regexp.MustCompile(`if\s+p_attempt_ticket\s*<\s*1\s+or\s+p_attempt_ticket\s*>\s*v_half_open_issued\s+or\s+p_attempt_ticket\s*>\s*v_half_open_ticket_mask_limit\s+then(?s:.*?)return\s+query`).MatchString(body) {
		t.Fatalf("download_report_breaker_sample must reject invalid half_open attempt tickets")
	}
	if !regexp.MustCompile(`v_ticket_mask\s*:=\s*\(1::bigint\s*<<\s*\(p_attempt_ticket\s*-\s*1\)\)`).MatchString(body) {
		t.Fatalf("download_report_breaker_sample must derive a per-ticket mask")
	}
	if !regexp.MustCompile(`if\s+\(v_half_open_resolved_mask\s*&\s*v_ticket_mask\)\s*<>\s*0\s+then(?s:.*?)return\s+query`).MatchString(body) {
		t.Fatalf("download_report_breaker_sample must ignore duplicate half_open ticket reports")
	}
	if !regexp.MustCompile(`v_half_open_resolved_mask\s*:=\s*v_half_open_resolved_mask\s*\|\s*v_ticket_mask`).MatchString(body) {
		t.Fatalf("download_report_breaker_sample must record accepted half_open ticket reports in the resolved mask")
	}
	if regexp.MustCompile(`v_probe_version_matches`).MatchString(body) {
		t.Fatalf("legacy single-probe version matching still present")
	}
}

func TestInitSQLReportBreakerSampleUsesSamplesSinceResetForTrendGate(t *testing.T) {
	body := tableFunctionBody(t, readInitSQLNormalized(t), "download_report_breaker_sample")
	if !regexp.MustCompile(`v_samples_since_reset\s*>=\s*v_min_samples_before_ewma_open`).MatchString(body) {
		t.Fatalf("download_report_breaker_sample must gate ewma opens with samples_since_reset")
	}
	if regexp.MustCompile(`v_total_samples\s*>=\s*v_min_samples_before_ewma_open`).MatchString(body) {
		t.Fatalf("download_report_breaker_sample must not use total_samples as the ewma warmup gate")
	}
}

func TestInitSQLReportBreakerSampleKeepsHalfOpenProtectedSamplesAsImmediateReopens(t *testing.T) {
	body := tableFunctionBody(t, readInitSQLNormalized(t), "download_report_breaker_sample")
	if !regexp.MustCompile(`v_should_open\s*:=\s*v_state\s*=\s*'half_open'\s+or`).MatchString(body) {
		t.Fatalf("download_report_breaker_sample must reopen protected half_open samples immediately")
	}
}

func TestInitSQLReportBreakerSampleSoftResetsClosedIdleRows(t *testing.T) {
	body := tableFunctionBody(t, readInitSQLNormalized(t), "download_report_breaker_sample")
	pattern := `if\s+v_state\s*=\s*'closed'\s+and\s+v_idle_reset_seconds\s*>\s*0\s+and\s+v_last_sample_at\s+is\s+not\s+null\s+and\s+\(v_now\s*-\s*v_last_sample_at\)\s*>=\s*v_idle_reset_seconds\s+then(?s:.*?)v_ewma_score\s*:=\s*0(?s:.*?)v_consecutive_error_count\s*:=\s*0(?s:.*?)v_success_streak\s*:=\s*0(?s:.*?)v_samples_since_reset\s*:=\s*0(?s:.*?)v_last_error_code\s*:=\s*null(?s:.*?)v_open_reason\s*:=\s*null(?s:.*?)v_last_open_seconds\s*:=\s*0`
	if !regexp.MustCompile(pattern).MatchString(body) {
		t.Fatalf("download_report_breaker_sample must soft-reset stale closed rows before evaluating new samples")
	}
}

func TestInitSQLReportBreakerSampleResetsBaselineAfterHalfOpenCloses(t *testing.T) {
	body := tableFunctionBody(t, readInitSQLNormalized(t), "download_report_breaker_sample")
	pattern := `if\s+v_should_close\s+then(?s:.*?)v_state\s*:=\s*'closed'(?s:.*?)v_ewma_score\s*:=\s*0(?s:.*?)v_consecutive_error_count\s*:=\s*0(?s:.*?)v_success_streak\s*:=\s*0(?s:.*?)v_samples_since_reset\s*:=\s*0(?s:.*?)v_half_open_since\s*:=\s*null`
	if !regexp.MustCompile(pattern).MatchString(body) {
		t.Fatalf("download_report_breaker_sample must reset breaker baseline after half_open closes")
	}
}

func TestInitSQLReportBreakerSampleParameterizesHalfOpenCloseRule(t *testing.T) {
	body := tableFunctionBody(t, readInitSQLNormalized(t), "download_report_breaker_sample")
	if regexp.MustCompile(`v_success_streak\s*>=\s*2\s+and\s+v_ewma_score\s*<=\s*v_close_threshold`).MatchString(body) {
		t.Fatalf("download_report_breaker_sample close rule must be parameterized")
	}
	if !regexp.MustCompile(`v_should_close\s*:=\s*case\s+when\s+v_half_open_close_mode\s*=\s*'or'\s+then`).MatchString(body) {
		t.Fatalf("download_report_breaker_sample must branch close behavior on half_open_close_mode")
	}
	if !regexp.MustCompile(`v_success_count\s*>=\s*v_half_open_success_threshold`).MatchString(body) {
		t.Fatalf("download_report_breaker_sample must use half_open_success_threshold")
	}
}

func TestInitSQLDefinesAtomicAdmissionFunctionContract(t *testing.T) {
	text := readInitSQLNormalized(t)
	pattern := `create\s+or\s+replace\s+function\s+fq_admit_batch\s*\([^)]*p_breaker_enabled\s+boolean[^)]*p_open_cap_seconds\s+int[^)]*p_close_threshold_percent\s+int[^)]*p_half_open_success_threshold\s+int[^)]*p_half_open_close_mode\s+text[^)]*p_half_open_max_probe_count\s+int[^)]*p_half_open_max_seconds\s+int[^)]*p_half_open_timeout_mode\s+text[^)]*\)\s*returns\s+table\s*\([^)]*status\s+text[^)]*slot_token\s+text[^)]*throttle_code\s+int[^)]*breaker_open_until\s+int[^)]*breaker_reason\s+text[^)]*breaker_version\s+bigint[^)]*retry_after\s+int[^)]*attempt_version\s+bigint[^)]*attempt_ticket\s+int`
	if !regexp.MustCompile(pattern).MatchString(text) {
		t.Fatalf("fq_admit_batch must expose the atomic admission breaker gate contract")
	}
	if regexp.MustCompile(`create\s+or\s+replace\s+function\s+fq_try_acquire_batch`).MatchString(text) {
		t.Fatalf("legacy fq_try_acquire_batch function still present")
	}
}

func TestInitSQLDefinesCanonicalAuthorizeHelperContract(t *testing.T) {
	text := readInitSQLNormalized(t)
	if !regexp.MustCompile(`create\s+or\s+replace\s+function\s+func_authorize_breaker_attempt\s*\(`).MatchString(text) {
		t.Fatalf("init.sql must define one canonical authorize helper")
	}
	if !regexp.MustCompile(`create\s+or\s+replace\s+function\s+download_authorize_breaker_attempt(?s:.*?)from\s+func_authorize_breaker_attempt\s*\(`).MatchString(text) {
		t.Fatalf("download_authorize_breaker_attempt must call the canonical authorize helper")
	}
	if !regexp.MustCompile(`create\s+or\s+replace\s+function\s+fq_admit_batch(?s:.*?)from\s+func_authorize_breaker_attempt\s*\(`).MatchString(text) {
		t.Fatalf("fq_admit_batch must call the canonical authorize helper")
	}
}

func TestInitSQLAdmitBatchOnlyAssignsNullReadyOnlyFieldsOutsideReady(t *testing.T) {
	body := batchAcquireFunctionBody(t, readInitSQLNormalized(t))
	for _, status := range []string{"wait", "throttled", "half_open_full"} {
		requireNullAssignmentsInStatusBranches(t, body, status, "slot_token", "attempt_version", "attempt_ticket")
	}
}

func TestInitSQLAdmitBatchReleasesBothSlotsWhenBreakerOpenDeniesAfterAcquire(t *testing.T) {
	body := batchAcquireFunctionBody(t, readInitSQLNormalized(t))
	throttledBranch := mustFindIndex(t, body, `if\s+v_breaker_state\s*=\s*'open'`)
	halfOpenFullBranch := mustFindIndex(t, body, `if\s+v_breaker_state\s*=\s*'half_open'\s+and\s+not\s+coalesce\(v_authorize_attempt_granted,\s*false\)\s+then`)

	requireReleaseCallsInBranch(t, body, throttledBranch, halfOpenFullBranch[0], "post-acquire THROTTLED", `func_release_site_slot\(\s*v_site_slot_id\s*,\s*false\s*\)`, `func_release_host_slot\(\s*v_host_slot_id\s*,\s*false\s*\)`)
}

func TestInitSQLAdmitBatchHalfOpenFullReleasesBothSlotsAndRequiresPositiveRetryAfter(t *testing.T) {
	body := batchAcquireFunctionBody(t, readInitSQLNormalized(t))
	halfOpenFullBranch := mustFindIndex(t, body, `if\s+v_breaker_state\s*=\s*'half_open'\s+and\s+not\s+coalesce\(v_authorize_attempt_granted,\s*false\)\s+then`)
	readyBranch := mustFindIndex(t, body, `status\s*:=\s*'ready'`)
	branch := body[halfOpenFullBranch[0]:readyBranch[0]]

	requireReleaseCallsInBranch(t, body, halfOpenFullBranch, readyBranch[0], "HALF_OPEN_FULL", `func_release_site_slot\(\s*v_site_slot_id\s*,\s*false\s*\)`, `func_release_host_slot\(\s*v_host_slot_id\s*,\s*false\s*\)`)
	if !regexp.MustCompile(`retry_after\s*:=\s*case\s+when\s+v_authorize_half_open_deadline\s+is\s+not\s+null\s+and\s+v_authorize_half_open_deadline\s*>\s*v_now\s+then\s+v_authorize_half_open_deadline\s*-\s*v_now\s+else\s+1\s+end`).MatchString(branch) {
		t.Fatalf("HALF_OPEN_FULL must assign a positive non-null retry_after")
	}
}

func TestInitSQLAdmitBatchGuardsBreakerReadAndAuthorizeWhenDisabled(t *testing.T) {
	body := batchAcquireFunctionBody(t, readInitSQLNormalized(t))
	readPattern := `if\s+coalesce\(p_breaker_enabled,\s*false\)\s+and\s+p_hostname_hash\s+is\s+not\s+null\s+and\s+p_hostname_hash\s*<>\s*''\s+then(?s:.*?)from\s+"throttle_protection"`
	authorizePattern := `if\s+coalesce\(p_breaker_enabled,\s*false\)\s+and\s+p_hostname_hash\s+is\s+not\s+null\s+and\s+p_hostname_hash\s*<>\s*''\s+then(?s:.*?)from\s+func_authorize_breaker_attempt\s*\(`
	if !regexp.MustCompile(readPattern).MatchString(body) {
		t.Fatalf("fq_admit_batch must guard the THROTTLE_PROTECTION read with p_breaker_enabled")
	}
	if !regexp.MustCompile(authorizePattern).MatchString(body) {
		t.Fatalf("fq_admit_batch must guard func_authorize_breaker_attempt with p_breaker_enabled")
	}
}

func TestInitSQLBatchAcquireKeepsRawBreakerMetadataOutsideOpenState(t *testing.T) {
	body := batchAcquireFunctionBody(t, readInitSQLNormalized(t))
	if regexp.MustCompile(`if\s+not\s+v_throttled\s+then(?s:.*?)v_(throttle_code|breaker_open_until|breaker_reason|breaker_version)\s*:=\s*null`).MatchString(body) {
		t.Fatalf("fq_admit_batch must keep raw breaker metadata even when not actively open")
	}
}

func TestInitSQLOmitsLegacyThrottleProtectionEnvOverrideComment(t *testing.T) {
	text := readInitSQLNormalized(t)
	if regexp.MustCompile(`throttle_protection_table`).MatchString(text) {
		t.Fatalf("init.sql should not document legacy THROTTLE_PROTECTION table override")
	}
}
