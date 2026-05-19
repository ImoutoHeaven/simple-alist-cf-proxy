package concurrencyhandler

import "testing"

func TestReleasedReplayValidatorsAllowCanonicalReleasedReasons(t *testing.T) {
	reasons := []string{"stream_complete", "heartbeat_timeout", "claim_handoff_timeout", "final_cleanup"}

	for _, reason := range reasons {
		t.Run(reason, func(t *testing.T) {
			if err := validateAcquireResult(validAcquireRequest(), &AcquireResult{Result: "released", Reason: reason}); err != nil {
				t.Fatalf("validateAcquireResult rejected %q: %v", reason, err)
			}
			if err := validateClaimGrantResult(&ClaimGrantResult{Result: "released", Reason: reason}); err != nil {
				t.Fatalf("validateClaimGrantResult rejected %q: %v", reason, err)
			}
			if err := validateAckHandoffResult(&AckHandoffResult{Result: "released", Reason: reason}); err != nil {
				t.Fatalf("validateAckHandoffResult rejected %q: %v", reason, err)
			}
		})
	}
}

func TestReleasedReplayValidatorsRejectCompensationReasons(t *testing.T) {
	reasons := []string{releaseReasonGrantDeliveryFailed, releaseReasonAcquireDeliveryFailed}

	for _, reason := range reasons {
		t.Run(reason, func(t *testing.T) {
			if err := validateAcquireResult(validAcquireRequest(), &AcquireResult{Result: "released", Reason: reason}); err == nil {
				t.Fatalf("validateAcquireResult unexpectedly accepted %q", reason)
			}
			if err := validateClaimGrantResult(&ClaimGrantResult{Result: "released", Reason: reason}); err == nil {
				t.Fatalf("validateClaimGrantResult unexpectedly accepted %q", reason)
			}
			if err := validateAckHandoffResult(&AckHandoffResult{Result: "released", Reason: reason}); err == nil {
				t.Fatalf("validateAckHandoffResult unexpectedly accepted %q", reason)
			}
		})
	}
}

func TestClaimGrantExpiredReasonValidation(t *testing.T) {
	tests := []struct {
		name    string
		reason  string
		wantErr bool
	}{
		{name: "hard_expired", reason: "hard_expired"},
		{name: "waiter_detached_timeout", reason: "waiter_detached_timeout"},
		{name: "wait_stream_timeout", reason: "wait_stream_timeout"},
		{name: "invalid_terminal_reason", reason: "not_a_terminal_reason", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateClaimGrantResult(&ClaimGrantResult{Result: "expired", Reason: tt.reason})
			if tt.wantErr {
				if err == nil {
					t.Fatalf("validateClaimGrantResult unexpectedly accepted %q", tt.reason)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateClaimGrantResult rejected %q: %v", tt.reason, err)
			}
		})
	}
}
