package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSlotHandlerHasNoQueueDepthCleanup(t *testing.T) {
	banned := []string{
		"queuedepthcleanupttl",
		"queue_depth_cleanup_ttl",
		"queuedepthzombie",
		"queue_depth_zombie",
		"func_cleanup_host_queue_depth",
		"func_cleanup_site_queue_depth",
	}

	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			name := entry.Name()
			if name == "vendor" || name == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		base := filepath.Base(path)
		if base == "slot_handler_sanity_test.go" || base == "sql_schema_test.go" {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := strings.ToLower(string(raw))
		for _, token := range banned {
			if strings.Contains(text, token) {
				return fmt.Errorf("slot-handler contains banned token: %s (file=%s)", token, path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan slot-handler: %v", err)
	}
}
