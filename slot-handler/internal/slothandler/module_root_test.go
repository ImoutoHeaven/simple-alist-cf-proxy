package slothandler

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func moduleRootDir(t *testing.T) string {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed")
	}
	// Walk upward until we find the module's go.mod.
	dir := filepath.Dir(thisFile)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("unable to locate go.mod from %s", thisFile)
		}
		dir = parent
	}
}
