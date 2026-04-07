package slothandler

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func mustReadModuleText(t *testing.T, rel string) string {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(moduleRootDir(t), rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}

	return string(raw)
}

func yamlSectionKeys(t *testing.T, text, section string) []string {
	t.Helper()

	lines := strings.Split(text, "\n")
	inSection := false
	keys := []string{}
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		indent := len(line) - len(strings.TrimLeft(line, " "))

		if !inSection {
			if indent == 0 && trimmed == section+":" {
				inSection = true
			}
			continue
		}

		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if indent == 0 {
			break
		}
		if indent == 2 && strings.HasSuffix(trimmed, ":") {
			keys = append(keys, strings.TrimSuffix(trimmed, ":"))
		}
	}

	if !inSection {
		t.Fatalf("expected %s section to exist", section)
	}

	return keys
}

func TestSlotHandlerContainerAssetPlacement(t *testing.T) {
	moduleRoot := moduleRootDir(t)
	repoRoot := filepath.Clean(filepath.Join(moduleRoot, ".."))

	mustExist := []string{
		filepath.Join(moduleRoot, "Dockerfile"),
		filepath.Join(moduleRoot, "docker-compose.yml"),
	}
	for _, path := range mustExist {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected %s to exist: %v", path, err)
		}
	}

	mustNotExist := []string{
		filepath.Join(moduleRoot, "docker-compose.local.yml"),
		filepath.Join(moduleRoot, "env.example"),
		filepath.Join(repoRoot, "Dockerfile"),
		filepath.Join(repoRoot, "docker-compose.yml"),
		filepath.Join(repoRoot, "docker-compose.local.yml"),
		filepath.Join(repoRoot, "env.example"),
	}
	for _, path := range mustNotExist {
		if _, err := os.Stat(path); err == nil {
			t.Fatalf("expected %s to be absent", path)
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat %s: %v", path, err)
		}
	}
}

func TestSlotHandlerDockerfileContract(t *testing.T) {
	text := mustReadModuleText(t, "Dockerfile")
	if strings.Count(text, "FROM ") != 2 {
		t.Fatalf("expected Dockerfile to contain exactly two FROM stages")
	}

	required := []string{
		"FROM golang:1.24.4",
		"CGO_ENABLED=0",
		"FROM debian:bookworm-slim",
		"COPY --from=build /out/slot-handler /app/slot-handler",
		"USER slot-handler",
		"EXPOSE 8080",
		"ENTRYPOINT [\"/app/slot-handler\"]",
		"CMD [\"-c\", \"/app/config.json\"]",
	}
	for _, token := range required {
		if !strings.Contains(text, token) {
			t.Fatalf("expected Dockerfile to contain %q", token)
		}
	}
	if strings.Contains(text, "GOARCH=amd64") {
		t.Fatalf("expected Dockerfile to avoid hard-coding GOARCH=amd64")
	}
	if strings.Contains(strings.ToUpper(text), "HEALTHCHECK") {
		t.Fatalf("expected Dockerfile to omit HEALTHCHECK")
	}
}

func TestSlotHandlerComposeFileContract(t *testing.T) {
	text := mustReadModuleText(t, "docker-compose.yml")
	serviceKeys := yamlSectionKeys(t, text, "services")
	if len(serviceKeys) != 1 || serviceKeys[0] != "slot-handler" {
		t.Fatalf("expected services to define exactly [slot-handler], got %v", serviceKeys)
	}

	required := []string{
		"services:",
		"slot-handler:",
		"image: slot-handler:ci",
		"context: .",
		"dockerfile: Dockerfile",
		"- \"8080:8080\"",
		"./config.json:/app/config.json:ro",
		"restart: unless-stopped",
	}
	for _, token := range required {
		if !strings.Contains(text, token) {
			t.Fatalf("expected docker-compose.yml to contain %q", token)
		}
	}

	forbidden := []string{
		"environment:",
		"env_file:",
		"command:",
		"entrypoint:",
		"user:",
		"healthcheck:",
	}
	for _, token := range forbidden {
		if strings.Contains(text, token) {
			t.Fatalf("expected docker-compose.yml to omit %q", token)
		}
	}
}
