package slothandler

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func requireJSONStringField(t *testing.T, obj map[string]json.RawMessage, key, want string) {
	t.Helper()

	raw, ok := obj[key]
	if !ok {
		t.Fatalf("expected %q key to be present", key)
	}

	var got string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %s: %v", key, err)
	}
	if got != want {
		t.Fatalf("expected %s to be %q, got %q", key, want, got)
	}
}

func TestConfigUsesDeterministicLocalContainerDefaults(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(moduleRootDir(t), "config.json"))
	if err != nil {
		t.Fatalf("read config.json: %v", err)
	}

	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatalf("decode config.json: %v", err)
	}

	controllerRaw, ok := top["controller"]
	if !ok {
		t.Fatalf("expected controller key to be present")
	}

	var controller map[string]json.RawMessage
	if err := json.Unmarshal(controllerRaw, &controller); err != nil {
		t.Fatalf("decode controller: %v", err)
	}

	requireJSONStringField(t, controller, "url", "")
	requireJSONStringField(t, controller, "apiToken", "")
	requireJSONStringField(t, controller, "env", "")
	requireJSONStringField(t, top, "internalApiToken", "local-dev-internal-token")
	requireJSONStringField(t, top, "listen", ":8080")
}

func TestRootGoWorkIsRemoved(t *testing.T) {
	path := filepath.Join(moduleRootDir(t), "..", "go.work")
	if _, err := os.Stat(path); err == nil {
		t.Fatalf("expected %s to be removed", path)
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat %s: %v", path, err)
	}
}
