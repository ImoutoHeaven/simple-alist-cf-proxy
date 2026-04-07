package slothandler

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestNormalizedExecutableRunLines(t *testing.T) {
	t.Run("drops comments and heredoc payloads", func(t *testing.T) {
		raw := strings.Join([]string{
			"",
			"  set -euo pipefail",
			"  # this comment should not count as an executable line",
			"  grep -n '^FROM debian:bookworm-slim' slot-handler/Dockerfile",
			"  cat <<'EOF'",
			"  grep -n '^FROM debian:bookworm-slim' slot-handler/Dockerfile",
			"EOF",
			"  docker compose -f slot-handler/docker-compose.yml build",
			"",
		}, "\n")

		got := normalizedExecutableRunLines(raw)
		requireStringSetEqual(t, "normalized executable lines", got,
			"set -euo pipefail",
			"grep -n '^FROM debian:bookworm-slim' slot-handler/Dockerfile",
			"cat <<'EOF'",
			"docker compose -f slot-handler/docker-compose.yml build",
		)
	})

	t.Run("rejects inline comment carriers", func(t *testing.T) {
		lines := normalizedExecutableRunLines("true # docker compose -f slot-handler/docker-compose.yml build")
		if matchesRequiredExecutableLine(lines, "docker compose -f slot-handler/docker-compose.yml build") {
			t.Fatalf("expected inline-comment carrier not to match executable docker compose command")
		}
	})

	t.Run("rejects printf carriers", func(t *testing.T) {
		lines := normalizedExecutableRunLines("printf '%s\\n' \"docker image inspect slot-handler:ci\"")
		if matchesRequiredExecutableLine(lines, "docker image inspect slot-handler:ci") {
			t.Fatalf("expected printf carrier not to match executable docker image inspect command")
		}
	})

	t.Run("rejects quoted string echo carriers", func(t *testing.T) {
		lines := normalizedExecutableRunLines("echo \"grep -n '^FROM debian:bookworm-slim' slot-handler/Dockerfile\"")
		if matchesRequiredExecutableLine(lines, "grep -n '^FROM debian:bookworm-slim' slot-handler/Dockerfile") {
			t.Fatalf("expected echo carrier not to match executable grep command")
		}
	})

	t.Run("rejects variable assignment carriers", func(t *testing.T) {
		lines := normalizedExecutableRunLines("payload=\"docker image inspect slot-handler:ci\"")
		if matchesRequiredExecutableLine(lines, "docker image inspect slot-handler:ci") {
			t.Fatalf("expected variable assignment carrier not to match executable docker image inspect command")
		}
	})

	t.Run("accepts real executable lines", func(t *testing.T) {
		lines := normalizedExecutableRunLines("docker image inspect slot-handler:ci > /tmp/slot-handler-image.json")
		if !matchesRequiredExecutableLine(lines, "docker image inspect slot-handler:ci") {
			t.Fatalf("expected real docker image inspect command to match")
		}
	})
}

func mustParseWorkflowDocument(t *testing.T, raw []byte) *yaml.Node {
	t.Helper()

	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode workflow yaml: %v", err)
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) != 1 {
		t.Fatalf("expected workflow yaml document with a single root node")
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		t.Fatalf("expected workflow root to be a mapping node, got kind %d", root.Kind)
	}

	return root
}

func lookupMappingValue(node *yaml.Node, key string) (*yaml.Node, bool) {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil, false
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1], true
		}
	}

	return nil, false
}

func requireMappingValue(t *testing.T, node *yaml.Node, key, context string) *yaml.Node {
	t.Helper()

	value, ok := lookupMappingValue(node, key)
	if !ok {
		t.Fatalf("expected %s to contain key %q", context, key)
	}

	return value
}

func requireScalarValue(t *testing.T, node *yaml.Node, context, want string) {
	t.Helper()

	if node.Kind != yaml.ScalarNode {
		t.Fatalf("expected %s to be a scalar node, got kind %d", context, node.Kind)
	}
	if node.Value != want {
		t.Fatalf("expected %s to be %q, got %q", context, want, node.Value)
	}
}

func requireEmptyNode(t *testing.T, node *yaml.Node, context string) {
	t.Helper()

	switch node.Kind {
	case yaml.ScalarNode:
		if node.Tag != "!!null" && node.Value != "" {
			t.Fatalf("expected %s to be empty, got scalar value %q with tag %q", context, node.Value, node.Tag)
		}
	case yaml.MappingNode:
		if len(node.Content) != 0 {
			keys := make([]string, 0, len(node.Content)/2)
			for i := 0; i+1 < len(node.Content); i += 2 {
				keys = append(keys, node.Content[i].Value)
			}
			t.Fatalf("expected %s to be empty, got keys %v", context, keys)
		}
	default:
		t.Fatalf("expected %s to be empty, got node kind %d", context, node.Kind)
	}
}

func requireStringSetEqual(t *testing.T, context string, got []string, want ...string) {
	t.Helper()

	counts := map[string]int{}
	for _, item := range got {
		counts[item]++
	}
	for _, item := range want {
		counts[item]--
	}
	if len(got) != len(want) {
		t.Fatalf("expected %s to contain %v, got %v", context, want, got)
	}
	for item, delta := range counts {
		if delta != 0 {
			t.Fatalf("expected %s to contain %v, got %v (mismatch on %q)", context, want, got, item)
		}
	}
}

func requireMappingKeys(t *testing.T, node *yaml.Node, context string, want ...string) {
	t.Helper()

	if node.Kind != yaml.MappingNode {
		t.Fatalf("expected %s to be a mapping node, got kind %d", context, node.Kind)
	}
	keys := make([]string, 0, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		keys = append(keys, node.Content[i].Value)
	}
	if len(keys) == 0 {
		t.Fatalf("expected %s to contain keys %v, got empty mapping", context, want)
	}
	if len(want) == 0 {
		return
	}
	requireStringSetEqual(t, context+" keys", keys, want...)
}

func requireSequenceScalars(t *testing.T, node *yaml.Node, context string, want ...string) {
	t.Helper()

	if node.Kind != yaml.SequenceNode {
		t.Fatalf("expected %s to be a sequence node, got kind %d", context, node.Kind)
	}
	values := make([]string, 0, len(node.Content))
	for _, item := range node.Content {
		if item.Kind != yaml.ScalarNode {
			t.Fatalf("expected %s entries to be scalar nodes, got kind %d", context, item.Kind)
		}
		values = append(values, item.Value)
	}
	requireStringSetEqual(t, context, values, want...)
}

func requireStepByMappingValue(t *testing.T, steps *yaml.Node, key, want string) *yaml.Node {
	t.Helper()

	if steps.Kind != yaml.SequenceNode {
		t.Fatalf("expected workflow steps to be a sequence node, got kind %d", steps.Kind)
	}
	for _, step := range steps.Content {
		if step.Kind != yaml.MappingNode {
			continue
		}
		value, ok := lookupMappingValue(step, key)
		if !ok || value.Kind != yaml.ScalarNode {
			continue
		}
		if value.Value == want {
			return step
		}
	}

	t.Fatalf("expected workflow steps to contain %s=%q", key, want)
	return nil
}

func requireStringContains(t *testing.T, context, got, want string) {
	t.Helper()

	if !strings.Contains(got, want) {
		t.Fatalf("expected %s to contain %q", context, want)
	}
}

func requireStringContainsAll(t *testing.T, context, got string, want ...string) {
	t.Helper()

	for _, token := range want {
		requireStringContains(t, context, got, token)
	}
}

func requireStepRunText(t *testing.T, step *yaml.Node, context string) string {
	t.Helper()

	run := requireMappingValue(t, step, "run", context)
	if run.Kind != yaml.ScalarNode {
		t.Fatalf("expected %s run to be a scalar node, got kind %d", context, run.Kind)
	}

	return run.Value
}

func requireNamedStepRunExact(t *testing.T, steps *yaml.Node, name, want string) {
	t.Helper()

	step := requireStepByMappingValue(t, steps, "name", name)
	got := requireStepRunText(t, step, name+" step")
	if got != want {
		t.Fatalf("expected %s step run to be %q, got %q", name, want, got)
	}
}

func lineExecutesRequiredCommand(line, required string) bool {
	if !strings.HasPrefix(line, required) {
		return false
	}
	if len(line) == len(required) {
		return true
	}
	next := line[len(required)]
	switch next {
	case ' ', '\t', ';', '|', '&', '>', '<', ')':
		return true
	default:
		return false
	}
}

func matchesRequiredExecutableLine(lines []string, required string) bool {
	for _, line := range lines {
		if lineExecutesRequiredCommand(line, required) {
			return true
		}
	}

	return false
}

func normalizedExecutableRunLines(raw string) []string {
	lines := strings.Split(raw, "\n")
	normalized := make([]string, 0, len(lines))
	inHereDoc := false
	hereDocTerminator := ""

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if inHereDoc {
			if trimmed == hereDocTerminator {
				inHereDoc = false
				hereDocTerminator = ""
			}
			continue
		}
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		normalized = append(normalized, trimmed)
		if marker, ok := parseHereDocStart(trimmed); ok {
			inHereDoc = true
			hereDocTerminator = marker
		}
	}

	return normalized
}

func parseHereDocStart(line string) (string, bool) {
	idx := strings.Index(line, "<<")
	if idx == -1 {
		return "", false
	}
	marker := strings.TrimSpace(line[idx+2:])
	if marker == "" {
		return "", false
	}
	if strings.HasPrefix(marker, "-") {
		marker = strings.TrimSpace(marker[1:])
	}
	marker = strings.Trim(marker, "'\"")
	if marker == "" {
		return "", false
	}

	return marker, true
}

func requireNamedStepRunContainsAll(t *testing.T, steps *yaml.Node, name string, want ...string) {
	t.Helper()

	step := requireStepByMappingValue(t, steps, "name", name)
	run := requireStepRunText(t, step, name+" step")
	lines := normalizedExecutableRunLines(run)
	for _, required := range want {
		if !matchesRequiredExecutableLine(lines, required) {
			t.Fatalf("expected %s to execute %q; executable lines: %v", name+" step", required, lines)
		}
	}
}

func TestSlotHandlerWorkflowContract(t *testing.T) {
	path := filepath.Join(moduleRootDir(t), "..", ".github", "workflows", "slot-handler-ci.yml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read workflow: %v", err)
	}
	text := string(raw)
	root := mustParseWorkflowDocument(t, raw)

	requireScalarValue(t, requireMappingValue(t, root, "name", "workflow root"), "workflow name", "slot-handler-ci")

	onNode := requireMappingValue(t, root, "on", "workflow root")
	requireMappingKeys(t, onNode, "workflow triggers", "push", "pull_request", "workflow_dispatch")
	for _, trigger := range []string{"push", "pull_request"} {
		triggerNode := requireMappingValue(t, onNode, trigger, fmt.Sprintf("workflow trigger %s", trigger))
		requireMappingKeys(t, triggerNode, fmt.Sprintf("workflow trigger %s", trigger), "paths")
		pathsNode := requireMappingValue(t, triggerNode, "paths", fmt.Sprintf("workflow trigger %s", trigger))
		requireSequenceScalars(t, pathsNode, fmt.Sprintf("%s paths", trigger), "slot-handler/**", ".github/workflows/slot-handler-ci.yml")
	}
	requireEmptyNode(t, requireMappingValue(t, onNode, "workflow_dispatch", "workflow triggers"), "workflow_dispatch trigger")

	jobsNode := requireMappingValue(t, root, "jobs", "workflow root")
	requireMappingKeys(t, jobsNode, "workflow jobs", "go-verify", "docker-verify")

	goVerifyJob := requireMappingValue(t, jobsNode, "go-verify", "workflow jobs")
	requireScalarValue(t, requireMappingValue(t, goVerifyJob, "runs-on", "go-verify job"), "go-verify runs-on", "ubuntu-latest")
	goVerifyDefaults := requireMappingValue(t, goVerifyJob, "defaults", "go-verify job")
	goVerifyRunDefaults := requireMappingValue(t, goVerifyDefaults, "run", "go-verify defaults")
	requireScalarValue(t, requireMappingValue(t, goVerifyRunDefaults, "working-directory", "go-verify run defaults"), "go-verify working-directory", "slot-handler")
	goVerifySteps := requireMappingValue(t, goVerifyJob, "steps", "go-verify job")
	requireStepByMappingValue(t, goVerifySteps, "uses", "actions/checkout@v4")
	setupGoStep := requireStepByMappingValue(t, goVerifySteps, "uses", "actions/setup-go@v5")
	setupGoWith := requireMappingValue(t, setupGoStep, "with", "setup-go step")
	requireScalarValue(t, requireMappingValue(t, setupGoWith, "go-version-file", "setup-go with"), "setup-go go-version-file", "slot-handler/go.mod")
	requireNamedStepRunExact(t, goVerifySteps, "Run Go tests", "go test ./...")
	requireNamedStepRunExact(t, goVerifySteps, "Run Go build", "go build ./...")

	dockerVerifyJob := requireMappingValue(t, jobsNode, "docker-verify", "workflow jobs")
	requireScalarValue(t, requireMappingValue(t, dockerVerifyJob, "runs-on", "docker-verify job"), "docker-verify runs-on", "ubuntu-latest")
	requireScalarValue(t, requireMappingValue(t, dockerVerifyJob, "needs", "docker-verify job"), "docker-verify needs", "go-verify")
	dockerVerifySteps := requireMappingValue(t, dockerVerifyJob, "steps", "docker-verify job")
	requireStepByMappingValue(t, dockerVerifySteps, "uses", "actions/checkout@v4")
	requireNamedStepRunExact(t, dockerVerifySteps, "Assert workflow uses go-version-file", "grep -n 'go-version-file: slot-handler/go.mod' .github/workflows/slot-handler-ci.yml")
	requireNamedStepRunContainsAll(t, dockerVerifySteps, "Assert Dockerfile contract from source",
		"test \"$(grep -c '^FROM ' slot-handler/Dockerfile)\" -eq 2",
		"grep -n '^FROM golang:1.24.4' slot-handler/Dockerfile",
		"grep -n '^FROM debian:bookworm-slim' slot-handler/Dockerfile",
		"grep -n 'CGO_ENABLED=0' slot-handler/Dockerfile",
		"test \"$(awk '/^go / {print $2}' slot-handler/go.mod)\" = \"$(grep '^FROM golang:' slot-handler/Dockerfile | head -n1 | sed -E 's/^FROM golang:([^ ]+).*$/\\1/')\"",
	)
	requireNamedStepRunExact(t, dockerVerifySteps, "Build direct CI image", "docker build -t slot-handler:ci -f slot-handler/Dockerfile slot-handler")
	requireNamedStepRunExact(t, dockerVerifySteps, "Render Compose config", "docker compose -f slot-handler/docker-compose.yml config --format json > /tmp/slot-handler-compose.json")
	requireNamedStepRunContainsAll(t, dockerVerifySteps, "Assert rendered Compose contract",
		"jq -e '.services | keys == [\"slot-handler\"]' /tmp/slot-handler-compose.json > /dev/null",
		"jq -e '.services[\"slot-handler\"].image == \"slot-handler:ci\"' /tmp/slot-handler-compose.json > /dev/null",
		"jq -e '.services[\"slot-handler\"].restart == \"unless-stopped\"' /tmp/slot-handler-compose.json > /dev/null",
		"jq -e '.services[\"slot-handler\"].command == null' /tmp/slot-handler-compose.json > /dev/null",
		"jq -e '.services[\"slot-handler\"].entrypoint == null' /tmp/slot-handler-compose.json > /dev/null",
		"jq -e '.services[\"slot-handler\"].user == null' /tmp/slot-handler-compose.json > /dev/null",
		"jq -e '.services[\"slot-handler\"].environment == null' /tmp/slot-handler-compose.json > /dev/null",
		"jq -e '.services[\"slot-handler\"].env_file == null' /tmp/slot-handler-compose.json > /dev/null",
		"jq -e '.services[\"slot-handler\"].healthcheck == null' /tmp/slot-handler-compose.json > /dev/null",
		"jq -e '.services[\"slot-handler\"].ports | length == 1' /tmp/slot-handler-compose.json > /dev/null",
		"jq -e '.services[\"slot-handler\"].ports[0].target == 8080' /tmp/slot-handler-compose.json > /dev/null",
		"jq -e '(.services[\"slot-handler\"].ports[0].published | tostring) == \"8080\"' /tmp/slot-handler-compose.json > /dev/null",
		"jq -e '.services[\"slot-handler\"].ports[0].protocol == \"tcp\"' /tmp/slot-handler-compose.json > /dev/null",
		"jq -e '.services[\"slot-handler\"].volumes | length == 1' /tmp/slot-handler-compose.json > /dev/null",
		"jq -e '.services[\"slot-handler\"].volumes[0].target == \"/app/config.json\"' /tmp/slot-handler-compose.json > /dev/null",
		"jq -e '.services[\"slot-handler\"].volumes[0].type == \"bind\"' /tmp/slot-handler-compose.json > /dev/null",
		"jq -e '.services[\"slot-handler\"].volumes[0].read_only == true' /tmp/slot-handler-compose.json > /dev/null",
		"context=\"$(jq -r '.services[\"slot-handler\"].build.context' /tmp/slot-handler-compose.json)\"",
		"case \"$context\" in",
		"slot-handler|./slot-handler|*/slot-handler)",
		"*) echo \"unexpected build context: $context\"; exit 1 ;;",
		"dockerfile=\"$(jq -r '.services[\"slot-handler\"].build.dockerfile' /tmp/slot-handler-compose.json)\"",
		"case \"$dockerfile\" in",
		"Dockerfile|*/Dockerfile)",
		"*) echo \"unexpected dockerfile: $dockerfile\"; exit 1 ;;",
		"source_path=\"$(jq -r '.services[\"slot-handler\"].volumes[0].source' /tmp/slot-handler-compose.json)\"",
		"case \"$source_path\" in",
		"slot-handler/config.json|./slot-handler/config.json|*/slot-handler/config.json)",
		"*) echo \"unexpected config source: $source_path\"; exit 1 ;;",
	)
	requireNamedStepRunExact(t, dockerVerifySteps, "Build Compose image", "docker compose -f slot-handler/docker-compose.yml build")
	requireNamedStepRunContainsAll(t, dockerVerifySteps, "Assert image metadata",
		"docker image inspect slot-handler:ci > /tmp/slot-handler-image.json",
		"jq -e '.[0].Config.Healthcheck == null' /tmp/slot-handler-image.json > /dev/null",
		"jq -e '.[0].Config.ExposedPorts[\"8080/tcp\"] != null' /tmp/slot-handler-image.json > /dev/null",
		"jq -e '.[0].Config.User != \"\" and .[0].Config.User != \"0\" and .[0].Config.User != \"root\"' /tmp/slot-handler-image.json > /dev/null",
		"jq -e '.[0].Config.Entrypoint == [\"/app/slot-handler\"]' /tmp/slot-handler-image.json > /dev/null",
		"jq -e '.[0].Config.Cmd == [\"-c\", \"/app/config.json\"]' /tmp/slot-handler-image.json > /dev/null",
	)
	requireNamedStepRunContainsAll(t, dockerVerifySteps, "Assert image filesystem payload",
		"docker run --rm --entrypoint sh slot-handler:ci -c 'set -eu; test \"$(find /app -mindepth 1 -maxdepth 1 | wc -l)\" -eq 1; test -f /app/slot-handler'",
	)
	requireNamedStepRunExact(t, dockerVerifySteps, "Start Compose service", "docker compose -f slot-handler/docker-compose.yml up -d")
	requireNamedStepRunContainsAll(t, dockerVerifySteps, "Wait for authenticated health check",
		"token=\"$(jq -r '.internalApiToken' slot-handler/config.json)\"",
		"test \"$token\" = \"local-dev-internal-token\"",
		"timeout 30s",
		"status=\"$(curl --max-time 1 -sS -o /dev/null -w \"%{http_code}\" -H \"Authorization: Bearer ${token}\" http://127.0.0.1:8080/api/v0/health || true)\"",
		"if [ \"$status\" = \"204\" ]; then",
		"sleep 1",
		"docker compose -f slot-handler/docker-compose.yml ps",
		"docker compose -f slot-handler/docker-compose.yml logs --no-color",
	)
	requireNamedStepRunExact(t, dockerVerifySteps, "Assert CA bundle inside running container", "docker compose -f slot-handler/docker-compose.yml exec -T slot-handler test -f /etc/ssl/certs/ca-certificates.crt")
	stopComposeStep := requireStepByMappingValue(t, dockerVerifySteps, "name", "Stop Compose service")
	requireScalarValue(t, requireMappingValue(t, stopComposeStep, "if", "Stop Compose service step"), "Stop Compose service if", "always()")
	requireScalarValue(t, requireMappingValue(t, stopComposeStep, "run", "Stop Compose service step"), "Stop Compose service run", "docker compose -f slot-handler/docker-compose.yml down -v")

	required := []string{
		"FROM debian:bookworm-slim",
		"if: always()",
	}
	for _, token := range required {
		requireStringContains(t, "workflow text", text, token)
	}

	forbidden := []string{
		"docker push",
		"--push",
		"docker buildx",
		"gh release",
		"kubectl ",
		"helm ",
		"wrangler deploy",
		"scp ",
		"rsync ",
		"ssh ",
		"golangci-lint",
		"actionlint",
		"npm run lint",
		"make lint",
		"go test -bench",
		"benchmem",
	}
	for _, token := range forbidden {
		if strings.Contains(text, token) {
			t.Fatalf("expected workflow to omit %q", token)
		}
	}
	for _, token := range []string{"schedule:", "workflow_call:", "pull_request_target:", "release:", "lint:", "bench:", "benchmark:", "benchmarks:"} {
		if strings.Contains(text, token) {
			t.Fatalf("expected workflow to omit extra trigger or job token %q", token)
		}
	}
}
