# Slot-Handler（公平队列服务）

独立的 Go 服务，用于托管下载端的公平队列（排队 + sleep 重试 + 抢 slot + 释放）。Worker 只需调用 HTTP 接口即可获得 slot，不再自己维护复杂的状态机。

## 快速开始

1. 复制 `config.json`，按需要修改监听地址、鉴权、数据库连接和公平队列参数。
2. 安装依赖并格式化：
   ```bash
   cd simple-alist-cf-proxy/slot-handler
   go mod tidy
   gofmt -w .
   ```
3. 构建与运行（示例）：
   ```bash
   go build -o slot-handler .
   ./slot-handler -c ./config.json
   ```

## 配置说明（config.json）

- `listen`：HTTP 监听地址，默认 `:8080`。
- `logLevel`：`debug` / `info` / `warn` / `error`。
- `auth`：轻量级共享密钥校验。
  - `enabled`：是否启用鉴权。
  - `header`：携带密钥的请求头名称（默认 `X-FQ-Auth`）。
  - `token`：共享密钥。
- `backend.mode`：`postgrest`（默认，通过 PostgREST 调 RPC）或 `postgres`（直连数据库）。
  - `backend.postgrest.baseUrl`：PostgREST 基础地址，例如 `https://db.example.com`。
  - `backend.postgrest.authHeader`：可选，传入 PostgREST 的 `Authorization` 值。
  - `backend.postgres.dsn`：PostgreSQL DSN，当 `mode=postgres` 时使用。
- `fairQueue`：公平队列参数。
  - `maxWaitMs` / `pollIntervalMs` / `pollWindowMs` / `minSlotHoldMs`：总等待时长、内部轮询间隔、本次轮询窗口预算、最小持锁时间。
  - `sessionIdleSeconds`：会话多久不轮询就判定过期（默认 90 秒）。
  - `maxSlotPerHost` / `maxSlotPerIp`：单 hostname/单 IP 的并发 slot 上限。
  - `maxWaitersPerIp`：每个 IP 允许的排队人数（>0 时才会注册/释放 waiter）。
  - `maxWaitersPerHost`：单 hostname 的全局排队上限（默认 50，<=0 关闭 host 级上限，不影响 Worker 返回的枚举，仅在内部 sleep 重试；当 `maxWaitersPerHost>0` 且 `maxWaitersPerIp<=0` 时仅启用 host 级排队）。
  - `zombieTimeoutSeconds` / `ipCooldownSeconds`：僵尸锁超时 & 冷却时间。
  - `cleanup`：后台清理配置（`enabled`、`intervalSeconds` 默认 1800 秒、`queueDepthZombieTtlSeconds` 未指定时默认 20 秒）。
  - `defaultGrantedCleanupDelay`：GRANTED 会话在释放 waiter 后延迟删除的秒数，默认 5 秒，用于给 client-cancel 遗言留短窗口。
- `rpc`：数据库 RPC 函数名（默认与 `download-init.sql` 中保持一致）。

## HTTP 接口

- `POST /api/v0/fairqueue/acquire`
  - 首次请求（不带 `queryToken`）：
    ```json
    {
      "hostname": "example.com",
      "hostnameHash": "sha256-of-hostname",
      "ipBucket": "203.0.113.4/32",
      "now": 1732170000000
    }
    ```
    返回 `{"result":"pending","queryToken":"..."}` 或 `{"result":"throttled",...}`。
  - 轮询请求（带 `queryToken`）：
    ```json
    {
      "queryToken": "uuid-from-first-call",
      "now": 1732170008000
    }
    ```
    返回 `pending | granted | throttled | timeout`，`granted` 时附带 `slotToken`。

- `POST /api/v0/fairqueue/release`
  - 负责按 `hitUpstreamAtMs + minSlotHoldMs` 计算最小持锁时间并调用数据库释放 slot。
  - 无论内部释放是否有小错误，始终返回 `{"result":"ok"}`。

## 运行时行为

- 短轮询：每次 HTTP 请求只使用 `pollWindowMs` 预算注册/抢锁，Worker 端默认 8 秒超时，多次轮询在 `maxWaitMs` + `SLOT_HANDLER_MAX_ATTEMPTS_CAP` 限制内完成。
- 终态只有三种：`granted`（拿到 slot）、`timeout`（等待超时/会话过期）、`throttled`（熔断，立即退出）；终态会立刻删除会话。
- 会话清理：`sessionIdleSeconds` 内无轮询或累计等待超过 `maxWaitMs` → 直接返回 `timeout` 并清理。
- 暂时错误/队列已满/IP 过多一律保持 `pending`，内部按 `pollIntervalMs` 自行 sleep 重试，避免客户端重试风暴。
- release 阶段即使数据库报错也只记录日志，不向调用方新增错误分支。
- Cleanup：`cleanup.enabled=true` 时，每隔 `intervalSeconds` 触发一次后台任务，依次调用 `func_cleanup_zombie_slots` / `func_cleanup_ip_cooldown` / `func_cleanup_queue_depth`。
