package slothandler

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSlotHandlerReadmeDocumentsRootRunCommands(t *testing.T) {
	path := filepath.Join(moduleRootDir(t), "README.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	text := string(raw)

	required := []string{
		"以下 Docker 与 Compose 命令均在仓库根目录执行。",
		"docker build -t slot-handler:local -f slot-handler/Dockerfile slot-handler",
		"docker compose -f slot-handler/docker-compose.yml up --build",
		"容器通过只读 bind mount 读取 `slot-handler/config.json`，容器内路径固定为 `/app/config.json`。",
		"CI 会执行：",
		"slot-handler/**",
		".github/workflows/slot-handler-ci.yml",
		"workflow_dispatch",
		"在 `slot-handler/` 工作目录内执行 `go test ./...`",
		"在 `slot-handler/` 工作目录内执行 `go build ./...`",
		"docker build -t slot-handler:ci -f slot-handler/Dockerfile slot-handler",
		"docker compose -f slot-handler/docker-compose.yml build",
		"基于 `slot-handler/config.json` 的容器启动与 `/api/v0/health` 鉴权烟测",
		"curl -i -H \"Authorization: Bearer <internalApiToken>\" http://127.0.0.1:8080/api/v0/health",
		"根目录 `go.work` 已移除。",
		"go -C slot-handler test ./...",
		"go -C slot-handler build ./...",
	}
	for _, token := range required {
		if !strings.Contains(text, token) {
			t.Fatalf("expected README.md to contain %q", token)
		}
	}
}
