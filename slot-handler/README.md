# slot-handler（公平队列服务）

slot-handler 是独立的 Go HTTP 服务，为 download worker 提供公平排队与 slot 管理。Worker 通过 HTTP 接口获取/释放 slot，避免在 Worker 内直接维护复杂状态机或 SQL。

## 快速开始

```bash
cd simple-alist-cf-proxy/slot-handler
go mod tidy
go build -o slot-handler .
./slot-handler -c ./config.json
```

## 配置文件结构（config.json）

配置文件包含两部分：

1. **控制面元信息**（仅用于启动与内部 API）
   - `controller`：用于从 controller 拉取 slot-handler 配置与上报指标
     - `url` / `apiPrefix` / `apiToken` / `env` / `role` / `instanceId`
     - `appName` / `appVersion`（会写入 health 与 metrics）
   - `internalApiToken`：保护内部控制 API 的 Bearer token

2. **运行配置**（slot-handler 业务配置）
   - `listen`：监听地址
   - `logLevel`：`debug` / `info` / `warn` / `error`
   - `auth`：对外 API 鉴权
     - `enabled` / `header` / `token`（默认 header 为 `X-FQ-Auth`）
   - `backend`：数据库后端
     - `mode`：`postgrest` 或 `postgres`
     - `postgrest.baseUrl` / `postgrest.authHeader`
     - `postgres.dsn`
   - `fairQueue`：公平队列参数
     - `maxWaitMs` / `pollIntervalMs` / `pollWindowMs` / `minSlotHoldMs`
     - `smoothReleaseIntervalMs`：平滑释放同一 host 的 slot（为空或 <=0 表示不启用）
     - `globalMaxWaiters`：本机允许的最大排队会话数，超过后直接返回 `overloaded`
     - `sessionIdleSeconds`：会话长时间不轮询即超时
     - `maxSlotPerHost` / `maxSlotPerIp`：并发 slot 上限
    - `maxWaitersPerIp` / `maxWaitersPerHost`：等待队列上限（显式设置为 0 可关闭）
    - `zombieTimeoutSeconds` / `ipCooldownSeconds`
    - `defaultGrantedCleanupDelay`：GRANTED 会话延迟清理秒数（默认 5 秒）
    - `weightedScheduler`：热点 host 的加权调度开关与参数
      - `weightedScheduler.enabled`：是否启用加权调度
      - `weightedScheduler.hotPendingFactor` / `weightedScheduler.hotPendingMin`：热点判定阈值
      - `weightedScheduler.coldAvgWaitMs` / `weightedScheduler.hotAvgWaitMs`：冷/热点平均等待时间阈值
      - `weightedScheduler.maxProbesPerCycle`：每轮允许的 TryAcquire 上限
      - `weightedScheduler.baseWeight` / `weightedScheduler.weightPerWait`：权重基值与等待次数权重
    - `cleanup`：后台清理配置
   - `fairQueue.rpc`：RPC 函数名（需与 `init.sql` 对齐）

### controller 模式

当 `controller.url/apiToken/env` 填写完整时，slot-handler 启动会向 controller `/api/v0/bootstrap` 拉取配置；文件中的运行配置仅作为 fallback 与刷新时的基础元信息。

## HTTP API

### 对外公平队列 API（需 `auth`）

请求头：`X-FQ-Auth: <token>`（或自定义 header）

- `POST /api/v0/fairqueue/acquire`
  - 入参：`hostname` / `hostnameHash` / `ipBucket` / `now` / `throttleTimeWindowSeconds` / `queryToken`
  - 返回：
    - `result=granted`：包含 `slotToken`
    - `result=pending`：继续轮询（携带 `queryToken`）
    - `result=throttled`：包含 `throttleCode` / `throttleRetryAfter`
    - `result=overloaded`：slot-handler 过载，建议退避
    - `result=timeout`
- `POST /api/v0/fairqueue/release`
  - 入参：`slotToken` / `hostnameHash` / `ipBucket` / `hitUpstreamAtMs` / `now`
  - 返回：`{"result":"ok"}`
- `POST /api/v0/fairqueue/cancel`
  - 入参：`queryToken`
  - 返回：`{"result":"ok"}`

### 内部控制 API（需 Bearer token）

请求头：`Authorization: Bearer <internalApiToken>`，否则返回 404。

- `GET /api/v0/health`：健康检查（204 + X-* 头）
- `POST /api/v0/refresh`：重新加载配置（目标含 `all/config/bootstrap/fairqueue` 时生效）
- `POST /api/v0/flush`：触发会话 GC、数据库清理与 metrics flush

## 指标上报

当 controller 配置可用时，slot-handler 会按固定周期（默认 60s）向 `/api/v0/metrics` 上报 `slot_handler.snapshot`，包含：会话数量、关键计数、平滑释放 host 数量等信息。`/api/v0/flush` 会触发一次手动上报。

## 与 download worker 的关系

download worker 仅负责调用 slot-handler 的 `acquire/release/cancel`，排队与 slot 判定全部在 slot-handler 内部完成。对应的数据库函数与表结构请参考仓库根目录 `init.sql`。
