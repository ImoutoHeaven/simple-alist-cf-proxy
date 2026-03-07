package slothandler

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestAcquireRequestOmitsLegacyThrottleWindowField(t *testing.T) {
	if _, ok := reflect.TypeOf(AcquireRequest{}).FieldByName("ThrottleTimeWindow"); ok {
		t.Fatalf("AcquireRequest must not expose legacy ThrottleTimeWindow field")
	}
	if _, ok := reflect.TypeOf(AcquirePayload{}).FieldByName("ThrottleTimeWindow"); ok {
		t.Fatalf("AcquirePayload must not expose legacy ThrottleTimeWindow field")
	}
}

func TestServerGoOmitsLegacyThrottleWindowRequestPlumbing(t *testing.T) {
	path := filepath.Join(moduleRootDir(t), "internal", "slothandler", "server.go")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read server.go: %v", err)
	}
	text := strings.ToLower(string(raw))
	for _, banned := range []string{"throttletimewindowseconds", "sanitizethrottlewindowseconds"} {
		if strings.Contains(text, banned) {
			t.Fatalf("server.go still contains legacy throttle window request plumbing: %s", banned)
		}
	}
}

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
