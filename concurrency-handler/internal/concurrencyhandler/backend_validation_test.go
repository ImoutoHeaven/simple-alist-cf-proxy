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
