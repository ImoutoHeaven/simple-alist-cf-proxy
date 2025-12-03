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
  - `weightedScheduler`：热点 host 上的加权调度开关与调参（默认关闭，开后才会按等待次数/等待时长分配 TryAcquire 名额）：
    - `enabled`  
      - 是否启用加权调度。  
      - `false` 时：逻辑退回「每个 session 在 pollInterval 到来时，都自行 TryAcquire」，只受 `pollIntervalMs/maxWaitMs` 控制。  
      - `true` 时：只有被判定为热点的 host，会在每轮 TryAcquire 前经过调度器筛选，按权重挑出有限几个 session 去打 PG。
    - `hotPendingFactor` / `hotPendingMin`  
      - 用来判定「一个 host 的 pending 数量是否够多，值得启用热点调度」。  
      - 内部会算一个阈值：  
        - `hotPendingThreshold = max(hotPendingFactor * maxSlotPerHost, hotPendingMin)`。  
      - 当 `TotalPending < hotPendingThreshold` 时，即使平均等待时间稍微升高，也不会立刻视为“热点 host”，避免少量请求的抖动触发重度调度。  
      - 默认值：`hotPendingFactor=4, hotPendingMin=16`，配合典型 `maxSlotPerHost=4` 时，意味着大约有 16+ 个排队连接才会进入热点判定。
    - `coldAvgWaitMs` / `hotAvgWaitMs`  
      - 基于 host 级平均等待时间（AvgWaitMs）判定冷/热点的时间阈值。  
      - slot-handler 会在 session 结束时（GRANTED/TIMEOUT 等）更新每个 host 的 `AvgWaitMs`。  
      - 判定规则大致如下：  
        - `AvgWaitMs <= coldAvgWaitMs`：认为这是冷 host，排队仅相当于一两轮轮询，无需复杂调度；  
        - `AvgWaitMs >= hotAvgWaitMs` 且 pending 数也超过阈值：认为 host 真正“挤爆”，才开启 Weighted 调度。  
      - 默认值：  
        - `coldAvgWaitMs` 默认为 `pollIntervalMs`；  
        - `hotAvgWaitMs` 默认为 `3*pollIntervalMs + minSlotHoldMs`。  
      - 直观理解：当平均等待时间接近一次轮询，就算轻微排队；当平均等待时间已经是「多轮轮询 + 至少一次完整持锁时间」时，才视为热点。
    - `maxProbesPerCycle`  
      - 每个 poll 周期内，允许一个 host 对 PG 发起的 TryAcquire 次数上限。  
      - 用于限制某个热点 host 对 PG 的 QPS，避免所有 pending session 在每个 pollInterval 一起打 PG。  
      - 默认值：  
        - 若未配置，则为 `maxSlotPerHost`（若 `maxSlotPerHost<=0` 则退回到内置的默认 slot 上限）。  
      - 推荐策略：  
        - 第一版可以先保持默认值，让单个 host 的 TryAcquire 速率略高于理论 slot 释放速率；  
        - 若压测发现 PG 仍然很闲且队列较长，可适当增大；若 PG 压力较高，则可适当减小。
    - `baseWeight` / `weightPerWait`  
      - 控制「等待次数」对调度优先级的影响。  
      - 内部的权重公式为：  
        - `weight = baseWeight + weightPerWait * WaitCount`；  
        - 每个 `(hostnameHash, ipBucket)` bucket 都有一个 `VirtualTime`，每次被选中 TryAcquire 时 `VirtualTime += 1/weight`；  
        - 每轮总是选择 `VirtualTime` 最小的 bucket 进行 TryAcquire，因此 **weight 越大，VirtualTime 增长越慢，越容易被反复选中**。  
      - 默认：`baseWeight=1.0, weightPerWait=1.0`，即：  
        - 刚进入队列、一次 TryAcquire 都没失败时，`weight=1`；  
        - 失败 3 次的 bucket，`weight=4`；失败 10 次时，`weight=11`。  
      - 含义：  
        - 等得越久（WaitCount 越大）的 IP 子网会在多轮调度中获得更高被选中的频次；  
        - 新进来的 session 仍有机会尝试，只是当 host 真正拥挤时，整体倾向先照顾已经等很久的老 session。
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

## 控制面接入

- 默认优先从 controller `/api/v0/bootstrap`（`role=slot-handler`）拉取配置，controller 接入信息（URL/token/env/role/instanceId/appName/appVersion）与 `internalApiToken` 统一写在 `config.json` 顶层，不再从环境变量读取；如未填 controller 则直接使用 `config.json` 的本地参数。
- 暴露内部控制 API：`GET /api/v0/health`、`POST /api/v0/refresh`（重拉 controller 配置，如未配置 controller 则重载本地 config 文件并重置会话/平滑释放缓存）与 `POST /api/v0/flush`（触发一次 session GC + 清理任务 + 向 controller `/api/v0/metrics` 推送快照），调用需携带 `Authorization: Bearer $INTERNAL_API_TOKEN`，未配置 token 时默认 404 静默。
- 当配置了 controller 接入且有 `CONTROLLER_API_TOKEN` 时，会每 60 秒自动向 controller `/api/v0/metrics` 上报一次快照，内容包含 configVersion、会话状态计数与近期事件计数（throttled/granted/timeout/released 等）。
