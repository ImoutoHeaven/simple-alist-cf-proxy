package slothandler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleAcquireRequiresHostnameOrHash(t *testing.T) {
	s := newTestServer()
	s.cfg = &Config{Auth: AuthConfig{Enabled: false}}

	body := strings.NewReader(`{"ipBucket":"ip1","siteBucket":"s1","now":123}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/fairqueue/acquire", body)
	rec := httptest.NewRecorder()

	s.handleAcquire(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "hostname or hostnameHash is required") {
		t.Fatalf("expected missing hostname error, got %q", rec.Body.String())
	}
}
