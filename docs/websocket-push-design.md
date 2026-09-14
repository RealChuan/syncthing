# Syncthing 事件主动推送设计：WebSocket 协议

|          |                                                                                                                                                          |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 状态     | 设计稿（待评审）                                                                                                                                         |
| 日期     | 2026-09-09                                                                                                                                               |
| 关联文档 | [rest-events-api.md](rest-events-api.md)、[events-system-internals.md](events-system-internals.md)、[event-types-reference.md](event-types-reference.md) |

---

## 1. 背景与目标

### 1.1 现状

Syncthing 当前的事件消费方式是 REST 长轮询（`GET /rest/events?since=N&timeout=60`）：

- 每次轮询都是完整的 HTTP 请求/响应往返（连接复用靠 keep-alive）；
- 高频事件场景（`DownloadProgress`、`ItemFinished`）下轮询间隔就是事件延迟；
- 客户端需自行维护轮询循环、错误重试、游标管理（参见 [eventService.js](../gui/default/syncthing/core/eventService.js)）。

### 1.2 目标

新增基于 **WebSocket** 的服务端主动推送端点：

1. **低延迟**：事件产生后立即送达，无轮询间隔；
2. **单连接双向**：客户端可在线调整订阅（订阅掩码），无需重连；
3. **与 REST 互操作**：事件信封、`id` 游标语义与 `/rest/events` 完全一致，客户端可混用两种通道；
4. **可扩展的协议**：为未来的过滤、确认、多订阅等能力预留演进空间；
5. **面向所有客户端类型**：不仅浏览器，还包括外部程序、脚本、移动端等（见协议选型）。

### 1.3 非目标（v1 明确不做）

- 浏览器 GUI 迁移（原生 WebSocket API 无法携带 `X-API-Key` 请求头，v1 仅服务可设置请求头的非浏览器客户端）；
- 事件送达保证（保持现有尽力而为语义）；
- 慢客户端背压处理（见第 6.5 节决策）。

## 2. 协议选型

**结论：WebSocket。** "客户端不仅是 web"是选型的关键约束，逐一对比后 WebSocket 是唯一同时满足全部约束的选项。

先澄清一个常见误解：WebSocket 的 "Web" 只是历史命名。它是 RFC 6455 标准化的独立 TCP 帧协议，仅借用 HTTP 做一次 Upgrade 握手，非浏览器生态非常成熟：

| 客户端环境 | 可用库                                                 |
| ---------- | ------------------------------------------------------ |
| Go         | `gorilla/websocket`、`coder/websocket`（与服务端同库） |
| Python     | `websockets`、`websocket-client`                       |
| C#/.NET    | `System.Net.WebSockets`（标准库内置）                  |
| Java       | Jakarta WebSocket、OkHttp                              |
| Rust       | `tokio-tungstenite`                                    |
| Node.js    | `ws`                                                   |
| CLI 调试   | `websocat`                                             |

### 2.1 候选协议对比

约束条件：①双向控制（已定）②复用现有 HTTP 认证中间件 ③事件信封 JSON 与 REST 一致 ④项目依赖极简 ⑤浏览器与非浏览器客户端平等支持。

| 协议               | 双向    | 非浏览器生态                  | 复用现有 HTTP 栈                              | 结论                                                                                                                                                     |
| ------------------ | ------- | ----------------------------- | --------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **WebSocket**      | ✅      | ✅ 通用                       | ✅ Upgrade 是普通 HTTP 请求，穿过现有中间件链 | **选定**                                                                                                                                                 |
| SSE                | ❌ 单向 | ⚠️ 工具链偏浏览器 EventSource | ✅ 纯 HTTP 流                                 | 次优备选；需 SSE 推送 + REST 控制双通道，与双向单连接模型冲突                                                                                            |
| gRPC streaming     | ✅      | ✅ 强（代码生成）             | ❌ 浏览器需 grpc-web 代理；HTTP/2             | 33 种事件 `data` 多为弱类型 `map[string]interface{}`（见 event-types-reference.md），全量 proto 化改造巨大且破坏与 REST 的信封一致性；引入整套 gRPC 基建 |
| MQTT               | ✅      | ✅                            | ❌ 需内嵌或外置 broker                        | 为大规模生产/消费解耦设计；syncthing 是 P2P 桌面软件，事件生产者在进程内，broker 纯属多余一跳                                                            |
| WebTransport/HTTP3 | ✅      | ⚠️ 客户端库生态尚不成熟       | ❌                                            | 过新，覆盖不足；项目虽有 quic-go 但那是同步协议层，与 REST/GUI 层无关                                                                                    |
| 裸 TCP 自定义协议  | ✅      | ✅ 需自写                     | ❌ 无法复用认证/中间件/端口                   | 自造轮子，无收益                                                                                                                                         |

### 2.2 Go 依赖选择

推荐 `github.com/gorilla/websocket`：

- 事实标准，示例与生态最全，贡献者/用户熟悉度最高；
- 项目 2023 年恢复积极维护；
- 备选 `coder/websocket`（context 原生、更精简），接口差异不影响本设计的协议层。

## 3. 集成架构

### 3.1 方案对比

| 方案                              | 描述                                                                                          | 评价                                                                                          |
| --------------------------------- | --------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| A. 每连接原生订阅                 | 每条 WS 连接独立 `evLogger.Subscribe(mask)`，直读 channel                                     | `id` 从 1 重新计数，跨连接无法用 `since` 恢复，与 REST ID 空间不互通 ✗                        |
| **B. 复用 REST 共享缓冲（选定）** | WS 连接绑定 `getEventSub(mask)` 的长生命周期 `BufferedSubscription`，内部 `Since()` 循环写 WS | `id` 序列与 REST 完全同一空间；断线重连带 `since` 可从环形缓冲（1000 条）重放；零新增分发逻辑 |
| C. 独立 dispatcher 分发层         | 订阅 AllEvents，自维护客户端注册表                                                            | 重复实现 logger 扇出，多一跳延迟，违反 YAGNI ✗                                                |

### 3.2 架构图

```
┌──────────────────────────────────────────────────────────────┐
│                    事件生产者（lib/model 等）                   │
│                  evLogger.Log(type, data)                    │
└───────────────────────┬──────────────────────────────────────┘
                        ▼
┌──────────────────────────────────────────────────────────────┐
│            logger.Serve()（事件总线，lib/events）              │
│         按订阅掩码扇出 → 各 subscription channel               │
└───────┬──────────────────────────────┬───────────────────────┘
        │ mask=DefaultEventMask        │ mask=X（getEventSub 缓存）
        ▼                              ▼
┌──────────────────┐         ┌──────────────────────────┐
│ BufferedSub 1000 │   ...   │ BufferedSub 1000         │
│ pollingLoop      │         │ pollingLoop              │
└───────┬──────────┘         └───────────┬──────────────┘
        │                                │
        ▼                                ▼
  GET /rest/events                GET /rest/events/ws ←—— 新增
  （长轮询）                       ┌───────────────────────┐
                                  │ wsConn（每连接）        │
                                  │  reader goroutine     │
                                  │   ← 控制消息           │
                                  │  writer goroutine     │
                                  │   Since() 循环 → WS 帧 │
                                  └───────────────────────┘
```

**推送机制的实质**：`BufferedSubscription.pollingLoop` 每收到一条事件就 `cond.Broadcast()`，`Since()` 立即被唤醒返回——writer 循环拿到事件即刻写 WS 帧。事件延迟与直读 channel 相同（微秒级），无轮询间隔。

### 3.3 方案 B 的关键性质

- **ID 空间共享**：WS 与使用相同掩码的 REST 客户端读同一 `BufferedSubscription`，`id` 序列一致；
- **慢连接隔离**：`pollingLoop` 持续排空共享订阅 channel，单个慢 WS 客户端不会拖慢共享订阅（不会触发 logger 的 15ms 丢弃），只影响自己的重放窗口（环形缓冲覆盖）；
- **断线重连**：客户端持最后收到的 `id` 重连，从环形缓冲重放（最多 1000 条，与 REST 补拉能力一致）。

## 4. 详细设计

### 4.1 端点

```
GET /rest/events/ws?mask=DeviceConnected,FolderSummary&since=42
```

- 路由注册：`lib/api/api.go` 的 `Serve()` 中，与其他事件端点并列：
  ```go
  restMux.HandlerFunc(http.MethodGet, "/rest/events/ws", s.getWsEvents) // [mask] [since]
  ```
  （三个静态路径 `/rest/events`、`/rest/events/disk`、`/rest/events/ws` 无 httprouter 路由冲突。）
- `mask`（可选）：逗号分隔事件类型名，格式与 REST `events` 参数相同；默认 `DefaultEventMask`（即全部事件减去 `LocalChangeDetected`/`RemoteChangeDetected`，需监听磁盘变更须显式指定，与 REST 语义一致）；
- `since`（可选）：重放起点（订阅内 `id`）；缺省为该订阅缓冲区最新 `id`（只推未来事件）。

### 4.2 认证与安全

**仅 API Key**（设计决策）。握手请求必须携带以下任一：

- `X-API-Key: <key>` 请求头
- `Authorization: Bearer <key>` 请求头

认证路径完全复用现有中间件链：

1. `csrfManager`（[api_csrf.go:48-55](../lib/api/api_csrf.go)）对携带有效 API Key 的请求直接放行并附加 CORS 头——已验证；
2. GUI 启用身份验证时，`basicAuthAndSessionMiddleware` 对 API Key 的放行行为与其他 `/rest` 端点一致（外部工具现状即如此工作）；
3. 各中间件均不包装 `ResponseWriter`（已逐一核对：csrfManager、noCache、withDetails、redirectToHTTPS 均直接透传），`http.Hijacker` 断言成立，WebSocket Upgrade 可行。

认证失败 → HTTP 403（不升级，客户端收到普通 HTTP 错误响应）。无效掩码中的未知事件类型名 → HTTP 400（与 REST `getEventMask` 行为对齐：`UnmarshalEventType` 返回 0，掩码为 0 表示无事件，此处选择在握手期显式拒绝）。

浏览器原生 WebSocket API 无法设置自定义请求头，因此 v1 明确不支持浏览器连接（GUI 继续使用 REST 长轮询，两通道长期并存）。

### 4.3 协议定义

所有消息均为 JSON，一条 WS 文本帧一条消息。统一信封字段 `type` 区分消息种类。

#### 服务端 → 客户端

| type         | 格式                                                                        | 说明                                                                                                                                                         |
| ------------ | --------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `hello`      | `{"type":"hello","serverVersion":"v1.29.2","lastID":156}`                   | Upgrade 成功后第一帧。`lastID` = 该订阅环形缓冲中最新事件的 `id`（空缓冲为 0）；客户端用它检测缺口（自己的 `since` 落后 `lastID` 越多，缺口风险越大）        |
| `event`      | `{"type":"event","event":{...}}`                                            | `event` 内部结构与 REST `/rest/events` 返回数组元素**完全一致**（`id`/`globalID`/`time`/`type`/`data`，见 [rest-events-api.md](rest-events-api.md) 第 5 节） |
| `subscribed` | `{"type":"subscribed","mask":"DeviceConnected,FolderSummary","lastID":156}` | `subscribe`/`unsubscribe` 的确认；`lastID` 为新订阅缓冲的最新 `id`                                                                                           |
| `pong`       | `{"type":"pong","data":"..."}`                                              | 回显客户端 `ping` 的 `data`                                                                                                                                  |
| `error`      | `{"type":"error","code":"unknown_type","error":"unknown message type"}`     | 协议错误通知；**连接不关闭**（前向兼容：未知 `type` 报错但保持连接）                                                                                         |

`code` 取值：`invalid_json`（帧不是合法 JSON）、`unknown_type`（未识别的 `type`）、`invalid_mask`（subscribe 的 mask 无效）、`invalid_message`（已知 type 但字段缺失/类型错误）。

#### 客户端 → 服务端

| type          | 格式                                                                     | 说明                                                                                                                                                              |
| ------------- | ------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `subscribe`   | `{"type":"subscribe","mask":"DeviceConnected,FolderSummary","since":42}` | **替换**当前订阅掩码；`since` 可选，缺省为新订阅缓冲的 `lastID`（只推未来事件）。切换掩码即切换到另一 `BufferedSubscription`（`id` 空间随之切换，游标语义见 4.6） |
| `unsubscribe` | `{"type":"unsubscribe"}`                                                 | 等价于 `subscribe` 空掩码，停止推送；服务端回 `subscribed` 且 `mask` 为空串                                                                                       |
| `ping`        | `{"type":"ping","data":"arbitrary"}`                                     | 应用层心跳；`data` 可选，原样回显                                                                                                                                 |

#### 前向兼容规则

- 客户端**必须忽略**未知 `type` 的服务端消息与消息中的未知字段；
- 服务端对未知 `type` 回 `error`（`code=unknown_type`）但保持连接；
- 服务端对已知 `type` 的未知字段直接忽略。

#### 帧与连接限制

| 参数              | 值                                                   | 说明                                      |
| ----------------- | ---------------------------------------------------- | ----------------------------------------- |
| 读上限            | 4096 字节                                            | 控制消息极小，超限关闭（close code 1009） |
| 写超时            | 30 秒/帧                                             | 防死连接泄漏 goroutine（见 6.5 背压决策） |
| 服务端协议级 ping | 每 60 秒                                             | WS 控制帧；保持 NAT/代理连通、探测死对端  |
| 读空闲超时        | 120 秒                                               | 期间无任何帧（含 pong）则关闭             |
| 服务端关闭码      | 1001（服务停止）/ 1011（内部错误）/ 1009（消息过大） | 正常客户端断开使用 1000                   |

### 4.4 连接生命周期与数据流

每条连接两个 goroutine（handler 完成 Upgrade 并写出 `hello` 后启动）：

```
HTTP handler (getWsEvents)
  ├─ 中间件链完成 API Key 认证（复用，无新逻辑）
  ├─ websocket.Upgrade(...)，失败 → HTTP 错误
  ├─ 解析 mask/since → sub := s.getEventSub(mask)   ← 与 REST 同一缓存
  ├─ 写出 hello（含 sub.LastID()）
  └─ go reader(conn, state); go writer(conn, state)
```

**reader goroutine**（控制消息）：

```go
for {
    msg, err := conn.Read()          // 受读上限与读空闲超时约束
    if err != nil { break }          // 连接关闭/协议错误 → 退出
    switch msg.Type {
    case "subscribe":
        newSub := s.getEventSub(parseMask(msg.Mask))   // 复用 REST 缓存
        state.update(newSub, msg.Since, newSub.LastID()) // 互斥锁内替换
        outbox <- {"type":"subscribed", ...}
    case "unsubscribe":
        state.update(nil, 0, 0)
        outbox <- {"type":"subscribed","mask":"","lastID":0}
    case "ping":
        outbox <- {"type":"pong","data":msg.Data}
    default:
        outbox <- {"type":"error","code":"unknown_type",...}
    }
}
// 退出 → 触发连接关闭
```

**writer goroutine**（事件推送 + 出站消息）：

```go
for {
    // 1. 非阻塞清空控制消息队列（pong/subscribed/error 优先写出）
    for {
        select {
        case msg := <-outbox:
            if !writeJSON(msg) { return }   // 写失败/超时 → 关闭退出
        default:
            goto events
        }
    }
events:
    // 2. 取当前订阅状态，Since 最多阻塞 1 秒
    //    （有新事件时 cond.Broadcast 立即唤醒 —— 推送的实质）
    sub, since := state.current()
    if sub == nil { sleep(1s); continue }   // 已 unsubscribe
    evs := sub.Since(since, nil, 1*time.Second)
    for ev := range evs {
        if !writeJSON({"type":"event","event":ev}) { return }
    }
    state.setSince(lastID(evs))             // 记录推进位置
}
```

设计要点：

- **`Since` 超时 1 秒**：新事件通过 `cond.Broadcast()` 即时唤醒，超时仅作为关闭检测/订阅切换的轮询周期——控制消息（如 pong）最大延迟 1 秒，对心跳场景可忽略（客户端 ping 间隔通常 ≥30s）。此值为内部常量，可调；
- **单写者**：所有服务端帧（hello 之后）都经 writer goroutine 串行写出，天然满足 WS 并发写约束；reader 通过 outbox channel（带缓冲）投递控制响应；
- **订阅切换**：reader 在锁内替换 `state` 中的 (sub, since)，writer 下一轮循环（≤1 秒）生效；
- **写失败处理**：任一帧写错误或 30 秒超时 → 发送 close 帧 → 退出两个 goroutine → 连接资源释放。客户端凭游标重连补拉。

### 4.5 背压策略（设计决策：只管推送）

按决策**不做**慢客户端背压处理：

- 无事件丢弃逻辑、无慢速断连策略；
- 慢客户端的直接影响被环形缓冲自然吸收：`pollingLoop` 持续排空共享订阅 channel（不拖累其他订阅者），慢客户端只是自己的 `since` 落后，落后超过 1000 条时旧事件被缓冲覆盖——与 REST 客户端停止轮询的后果完全一致；
- 写超时（30 秒/帧）唯一作用是防止死连接泄漏 goroutine；
- 语义清晰：服务端永不主动因"慢"断开，数据缺口对客户端可见（`id` 跳变），由客户端决定是否 REST 补拉。

### 4.6 与 REST 的关系（长期并存）

| 维度      | REST `/rest/events`                      | WS `/rest/events/ws`                      |
| --------- | ---------------------------------------- | ----------------------------------------- |
| 事件信封  | `{id, globalID, time, type, data}`       | 完全相同（嵌在 `event` 字段内）           |
| `id` 空间 | 每掩码一套（`getEventSub` 缓存）         | **同一套**（同一 `BufferedSubscription`） |
| 掩码变更  | 新查询参数即新订阅                       | `subscribe` 控制消息                      |
| 交互      | 请求/响应                                | 双向消息流                                |
| 认证      | API Key / CSRF / Basic                   | 仅 API Key                                |
| 存废      | **保留不动**（GUI 与全部现有客户端依赖） | 新增                                      |

客户端可在两种通道间自由切换（如 WS 断开期间用 REST 补拉，恢复后继续 WS），只要掩码相同、游标连续。

### 4.7 可观测性（可选）

最小集，与现有 `lib/events/metrics.go` 风格一致：

```
syncthing_ws_events_connections        (gauge)   当前活跃 WS 连接数
syncthing_ws_events_auth_failures_total (counter) 握手认证失败次数
```

## 5. 扩展性设计

1. **统一信封**：所有消息 `{"type":...}`，新增消息种类零破坏；
2. **版本协商**：`hello` 携带 `serverVersion`，客户端可按版本启用特性；
3. **预定的演进路径**（均通过新增字段/消息类型实现，不破坏 v1 客户端）：
   - 按文件夹过滤：`subscribe` 增加可选 `folders` 字段，writer 侧过滤；
   - 多路订阅：`subscribe` 增加可选 `id` 字段，一条连接多个订阅并行，`event` 消息回带订阅 `id`；
   - 送达确认：新增 `ack` 客户端消息，服务端用于精确的客户端进度指标；
   - 浏览器支持：新增 REST 端点签发一次性短时 token，WS 握手以查询参数携带（届时 GUI eventService 可迁移）；
   - 二进制编码：`hello` 增加 `encoding` 字段协商（WS 原生支持二进制帧）。

## 6. 代码变更清单

| 文件                                            | 变更                                                                               | 规模        |
| ----------------------------------------------- | ---------------------------------------------------------------------------------- | ----------- |
| `go.mod` / `go.sum`                             | 新增 `github.com/gorilla/websocket`                                                | 1 依赖      |
| [lib/events/events.go](../lib/events/events.go) | `BufferedSubscription` 接口与实现增加 `LastID() int`（`s.mut` 保护下返回 `s.cur`） | ~6 行       |
| `lib/api/events_ws.go`（新建）                  | `getWsEvents` handler、`wsConn` 结构、reader/writer、常量（超时/上限）             | ~250-300 行 |
| [lib/api/api.go](../lib/api/api.go)             | 路由注册 1 行（`:266` 附近，与其他事件端点并列）                                   | 1 行        |
| `lib/api/events_ws_test.go`（新建）             | 测试（见第 7 节）                                                                  | ~200 行     |

不改动：`lib/events` 核心分发逻辑、现有 REST handler、GUI 前端、认证中间件。

## 7. 测试策略

沿用 `lib/api/api_test.go` 模式（`httptest.Server` + 真实中间件链）：

1. **认证**：无 Key → 403 不升级；错误 Key → 403；正确 Key（两种头形式）→ 升级成功收到 `hello`；
2. **协议**：`hello` 字段与首帧顺序；`ping`→`pong` 回显；未知 `type` → `error` 且连接保持；畸形 JSON → `error(invalid_json)`；超限帧 → 关闭 1009；
3. **订阅**：握手掩码生效（注入事件验证只收订阅类型）；`subscribe` 切换掩码 ≤1 秒生效并收到 `subscribed`；`unsubscribe` 后无事件；无效掩码 → 400 或 `error(invalid_mask)`；
4. **重放与游标**：注入 N 条事件后带 `since` 连接 → 按序重放；`id` 与 REST `Since()` 输出一致（同一缓冲读两次比对）；环形缓冲覆盖后重放返回的 `id` 有跳变（缺口可见）；
5. **生命周期**：客户端断开 → 服务端两 goroutine 退出（goroutine 泄漏检测）；服务端 ctx 取消 → 收到 close 1001；写超时路径（模拟慢读客户端）→ 连接关闭；
6. **并发**：多连接同掩码共享订阅互不干扰；多连接不同掩码独立游标。

## 8. 实施步骤

1. `lib/events`：`LastID()` 接口扩展 + 单测（独立可合入）；
2. 引入 `gorilla/websocket` 依赖；
3. `events_ws.go` 骨架：Upgrade、认证路径验证、`hello`；
4. writer 事件推送循环（握手掩码即完整可用）；
5. 控制协议：`subscribe`/`unsubscribe`/`ping`/`error`；
6. 生命周期硬化：读上限、读空闲超时、服务端 ping、close 码、优雅关闭；
7. 全量测试 + 在 `rest-events-api.md` 增补 WS 端点指引。

每步可独立验证；1-4 完成即构成最小可用版本（MVP：固定掩码推送）。

## 9. 风险与已知取舍

| 风险/取舍                    | 说明                                                                                                      | 缓解                                                                           |
| ---------------------------- | --------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| 掩码缓存无上限               | 每个 DISTINCT mask 在 `getEventSub` 缓存中创建永久订阅（WS 动态 subscribe 放大了这一既有行为，REST 亦然） | 现实中掩码组合有限；文档记录；如需治理可为 WS 连接内做掩码规范化（如排序去重） |
| 控制消息最大 1 秒延迟        | `Since` 超时轮询周期的副作用                                                                              | 心跳场景可忽略；常量可调；如需即时可演进为三 goroutine（pump/writer 分离）     |
| 代理/防火墙干扰 WS           | 企业环境可能断开长连接                                                                                    | 客户端重连 + `since` 重放即恢复；服务端 60s ping 维持 NAT                      |
| `gorilla/websocket` 维护状态 | 曾于 2022 年归档                                                                                          | 2023 年起恢复积极维护；`coder/websocket` 为随时可切换的备选                    |
| v1 不支持浏览器              | API Key 请求头限制                                                                                        | 明确的非目标；REST 长轮询继续服务 GUI；浏览器支持列入演进路径（一次性 token）  |
| 事件尽力而为语义不变         | 环形缓冲覆盖、无送达保证                                                                                  | 与 REST 一致；`id` 跳变使缺口对客户端可见；需要完整流时用 audit 日志           |

## 10. 附录：示例会话

```text
C: GET /rest/events/ws?mask=DeviceConnected,StateChanged&since=42
   （携带 X-API-Key；中间件链完成认证）
S: 101 Switching Protocols

S: {"type":"hello","serverVersion":"v1.29.2","lastID":57}

S: {"type":"event","event":{"id":58,"globalID":1042,"time":"2026-09-09T14:03:22.123456789+08:00","type":"DeviceConnected","data":{"id":"P56IOI7-...","deviceName":"laptop","clientName":"syncthing","clientVersion":"v1.29.0","type":"tcp-client","addr":"192.168.1.20:22000"}}}

C: {"type":"ping","data":"keepalive"}
S: {"type":"pong","data":"keepalive"}

C: {"type":"subscribe","mask":"FolderSummary","since":0}
S: {"type":"subscribed","mask":"FolderSummary","lastID":210}
S: {"type":"event","event":{"id":211,"globalID":1043,...,"type":"FolderSummary","data":{...}}}

C: {"type":"bogus"}
S: {"type":"error","code":"unknown_type","error":"unknown message type"}
   （连接保持）

C: （连接断开）
C: GET /rest/events/ws?mask=FolderSummary&since=211    ← 重连补拉
```
