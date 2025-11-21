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
  - `maxWaitMs` / `pollIntervalMs` / `minSlotHoldMs`：等待时长、轮询间隔、最小持锁时间。
  - `maxSlotPerHost` / `maxSlotPerIp`：单 hostname/单 IP 的并发 slot 上限。
  - `maxWaitersPerIp`：每个 IP 允许的排队人数（>0 时才会注册/释放 waiter）。
  - `zombieTimeoutSeconds` / `ipCooldownSeconds`：僵尸锁超时 & 冷却时间。
  - `rpc`：数据库 RPC 函数名（默认与 `download-init.sql` 中保持一致）。

## HTTP 接口

- `POST /api/v0/fairqueue/acquire`
  - 请求体：
    ```json
    {
      "hostname": "example.com",
      "hostnameHash": "sha256-of-hostname",
      "ipBucket": "203.0.113.4/32",
      "now": 1732170000000,
      "maxWaitMs": 20000,
      "minSlotHoldMs": 800
    }
    ```
  - 响应结果只会有三种：
    - `{"result":"granted","slotToken":"...","holdMs":800}`
    - `{"result":"throttled","throttleCode":429}`
    - `{"result":"timeout"}`

- `POST /api/v0/fairqueue/release`
  - 负责按 `hitUpstreamAtMs + minSlotHoldMs` 计算最小持锁时间并调用数据库释放 slot。
  - 无论内部释放是否有小错误，始终返回 `{"result":"ok"}`。

## 运行时行为

- 只有三种终态：`granted`（拿到 slot）、`timeout`（等待超时）、`throttled`（熔断，立即退出）。
- 所有其他错误/队列状态一律按 `pollIntervalMs` 睡眠后重试，避免客户端重试风暴。
- release 阶段即使数据库报错也只记录日志，不向调用方新增错误分支。
