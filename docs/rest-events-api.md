# `/rest/events` 接口分析

本文档分析 Syncthing REST API 中事件相关接口的代码位置、请求参数、认证方式、长轮询机制与响应结构。

- 接口代码：`lib/api/api.go`
- 事件系统：`lib/events/events.go`
- 认证/CSRF：`lib/api/api_csrf.go`、`lib/api/api_auth.go`

---

## 1. 接口定义

路由注册位于 `lib/api/api.go` 的 `service.Serve()` 中（`restMux` 为 `httprouter` 实例）：

```go
// lib/api/api.go:266-267
restMux.HandlerFunc(http.MethodGet, "/rest/events", s.getIndexEvents)      // [since] [limit] [timeout] [events]
restMux.HandlerFunc(http.MethodGet, "/rest/events/disk", s.getDiskEvents)  // [since] [limit] [timeout]
```

| 接口                | 方法 | 处理函数         | 事件掩码                                        |
| ------------------- | ---- | ---------------- | ----------------------------------------------- |
| `/rest/events`      | GET  | `getIndexEvents` | 默认 `DefaultEventMask`，可用 `events` 参数覆盖 |
| `/rest/events/disk` | GET  | `getDiskEvents`  | 固定 `DiskEventMask`                            |

两个掩码常量定义（`lib/api/api.go:63-71`）：

```go
// Default mask excludes these very noisy event types to avoid filling the pipe.
DefaultEventMask    = events.AllEvents &^ events.LocalChangeDetected &^ events.RemoteChangeDetected
DiskEventMask       = events.LocalChangeDetected | events.RemoteChangeDetected
EventSubBufferSize  = 1000
defaultEventTimeout = time.Minute
```

设计意图：`LocalChangeDetected` / `RemoteChangeDetected` 事件量极大（每个变更文件一条），默认从主事件流中排除，需要监听磁盘变更的客户端应使用 `/rest/events/disk`。

## 2. 处理函数

```go
// lib/api/api.go:1337-1375
func (s *service) getIndexEvents(w http.ResponseWriter, r *http.Request) {
	mask := s.getEventMask(r.URL.Query().Get("events"))
	sub := s.getEventSub(mask)
	s.getEvents(w, r, sub)
}

func (s *service) getDiskEvents(w http.ResponseWriter, r *http.Request) {
	sub := s.getEventSub(DiskEventMask)
	s.getEvents(w, r, sub)
}

func (*service) getEvents(w http.ResponseWriter, r *http.Request, eventSub events.BufferedSubscription) {
	qs := r.URL.Query()
	sinceStr := qs.Get("since")
	limitStr := qs.Get("limit")
	timeoutStr := qs.Get("timeout")
	since, _ := strconv.Atoi(sinceStr)
	limit, _ := strconv.Atoi(limitStr)

	timeout := defaultEventTimeout
	if timeoutSec, timeoutErr := strconv.Atoi(timeoutStr); timeoutErr == nil && timeoutSec >= 0 {
		timeout = time.Duration(timeoutSec) * time.Second
	}

	// Flush before blocking, to indicate that we've received the request and
	// that it should not be retried. Must set Content-Type header before flushing.
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	f := w.(http.Flusher)
	f.Flush()

	evs := eventSub.Since(since, []events.Event{}, timeout)
	if 0 < limit && limit < len(evs) {
		evs = evs[len(evs)-limit:]
	}

	sendJSON(w, evs)
}
```

### 请求参数

| 参数      | 类型      | 默认值             | 说明                                                                                            |
| --------- | --------- | ------------------ | ----------------------------------------------------------------------------------------------- |
| `since`   | int       | 0                  | 游标。返回 `id` **大于** 该值的事件。客户端应把上次收到的最后一条事件的 `id` 作为下次的 `since` |
| `limit`   | int       | 无限制             | 最多返回的事件数。超出时保留**最新**的 `limit` 条（`evs[len(evs)-limit:]`）                     |
| `timeout` | int（秒） | 60                 | 长轮询等待时长。`0` 是合法值（立即返回）                                                        |
| `events`  | string    | `DefaultEventMask` | 仅 `/rest/events` 支持。逗号分隔的事件类型名，如 `FolderSummary,StateChanged`                   |

### 关键行为

1. **先 Flush 再等待**：进入长轮询阻塞前就发送响应头（`Content-Type: application/json; charset=utf-8`），让客户端/代理知道请求已被接受，避免被重试。
2. **超时返回空数组**：`Since()` 超时后返回空 slice，序列化为 `[]`（而非 `null`）。
3. **`limit` 截取尾部**：只保留最新的事件。

## 3. 订阅缓存：按掩码复用

`getEventSub` 按 `mask` 缓存 `BufferedSubscription`（`lib/api/api.go:1389-1400`）：

```go
func (s *service) getEventSub(mask events.EventType) events.BufferedSubscription {
	s.eventSubsMut.Lock()
	bufsub, ok := s.eventSubs[mask]
	if !ok {
		evsub := s.evLogger.Subscribe(mask)
		bufsub = events.NewBufferedSubscription(evsub, EventSubBufferSize)
		s.eventSubs[mask] = bufsub
	}
	s.eventSubsMut.Unlock()
	return bufsub
}
```

两个常用订阅在**进程启动时**就已创建（见 `lib/syncthing/syncthing.go:139-143`），并传入 `api.New()`：

```go
// Event subscription for the API; must start early to catch the early
// events. The LocalChangeDetected event might overwhelm the event
// receiver in some situations so we will not subscribe to it here.
defaultSub := events.NewBufferedSubscription(a.evLogger.Subscribe(api.DefaultEventMask), api.EventSubBufferSize)
diskSub := events.NewBufferedSubscription(a.evLogger.Subscribe(api.DiskEventMask), api.EventSubBufferSize)
```

**重要后果**：`since` 游标（即事件 JSON 中的 `id`）是**每个订阅独立递增**的（见 `lib/events/events.go:337-368` 的 `sendEvent`）。如果客户端更换 `events` 过滤参数，会命中不同的缓存订阅，得到**另一套完全不同的 `id` 序列**，之前的 `since` 值不可复用。

## 4. 认证与安全

`/rest/` 路由的中间件包装顺序（`lib/api/api.go:343-384`）：

```
http.Server
 └─ redirectToHTTPS（若启用 TLS）
    └─ basicAuthAndSessionMiddleware（若 GUI 配置了用户名/密码或 LDAP）
       └─ withDetailsMiddleware（附加版本/设备 ID 响应头）
          └─ csrfManager（保护 "/rest" 前缀）
             └─ noCacheMiddleware
                └─ restMux（httprouter → getEvents）
```

`/rest/events` 属于受 CSRF 保护的前缀（`lib/api/api_csrf.go`），请求必须携带以下任一凭据：

| 凭据                                                                                 | 适用场景                   |
| ------------------------------------------------------------------------------------ | -------------------------- |
| `X-API-Key: <API key>` 请求头                                                        | 外部程序/脚本              |
| `Authorization: Bearer <API key>`                                                    | 外部程序/脚本              |
| `X-CSRF-Token-<shortID>: <token>` 请求头（token 来自 `CSRF-Token-<shortID>` Cookie） | 浏览器/GUI                 |
| Basic Auth / 会话 Cookie                                                             | 配置了身份验证时的叠加要求 |

同时响应带 `Access-Control-Allow-Origin: *` 与 `Access-Control-Allow-Headers: Content-Type, X-API-Key`，允许浏览器跨域携带 API Key 访问。

另见：登录尝试本身会产生 `LoginAttempt` 事件（`lib/api/api_auth.go:33-53`）。

## 5. 响应格式

返回 JSON 数组，每个元素是一个事件信封（`lib/events/events.go:252-260`）：

```go
type Event struct {
	// Per-subscription sequential event ID. Named "id" for backwards compatibility with the REST API
	SubscriptionID int `json:"id"`
	// Global ID of the event across all subscriptions
	GlobalID int         `json:"globalID"`
	Time     time.Time   `json:"time"`
	Type     EventType   `json:"type"`
	Data     interface{} `json:"data"`
}
```

### 字段说明

| 字段       | 类型   | 说明                                                                                 |
| ---------- | ------ | ------------------------------------------------------------------------------------ |
| `id`       | int    | **订阅内**顺序递增 ID。REST 客户端将其作为 `since` 游标。同一订阅内从 1 开始单调递增 |
| `globalID` | int    | **全局**事件 ID（跨所有订阅相同），`logger.nextGlobalID` 自增                        |
| `time`     | string | 事件产生时间，RFC3339Nano 高精度时间戳（`time.Now()` 在 `Log()` 调用时记录）         |
| `type`     | string | 事件类型名（`EventType.String()`），如 `"DeviceConnected"`                           |
| `data`     | object | 事件类型相关的负载数据，结构见《事件类型参考》文档                                   |

### 响应示例

```json
[
  {
    "id": 1,
    "globalID": 42,
    "time": "2026-09-09T10:00:00.123456789+08:00",
    "type": "Starting",
    "data": {
      "home": "/home/user/.local/state/syncthing",
      "myID": "P56IOI7-MZJNUZYT5RDGOJJ7VQZRD5Z..."
    }
  },
  {
    "id": 2,
    "globalID": 43,
    "time": "2026-09-09T10:00:01.034928+08:00",
    "type": "StartupComplete",
    "data": {
      "home": "/home/user/.local/state/syncthing",
      "myID": "P56IOI7-MZJNUZYT5RDGOJJ7VQZRD5Z..."
    }
  }
]
```

超时无新事件时返回 `[]`。

## 6. 典型客户端消费模式（长轮询循环）

官方开发工具 `cmd/dev/stevents/main.go` 演示了标准用法：

```go
since := 0
for {
	// GET /rest/events?since=<since>&timeout=60[&events=...]
	res := doRequest(fmt.Sprintf("/rest/events?since=%d%s", since, eventsArg))
	var events []event
	json.NewDecoder(res.Body).Decode(&events)
	for _, event := range events {
		print(event)
		since = event.ID // 推进游标
	}
}
```

要点：

- 循环发起请求，每次用上一批最后一条事件的 `id` 作为 `since`；
- 请求会在服务端挂起至多有新事件或 `timeout`（默认 60s）；
- Syncthing **不会**向 REST 客户端主动建立连接推送（没有 WebSocket/SSE）；所谓"实时性"由长轮询实现——事件产生后，正在阻塞的 `Since()` 会立即被唤醒并返回。

## 7. 事件丢失与可靠性

REST 事件流是**尽力而为**的，不保证投递：

1. `logger.sendEvent` 向订阅 channel（容量 64）投递时，若 15ms 内无法送入（订阅消费太慢），该事件**对该订阅被丢弃**（`lib/events/events.go:348-358`）。
2. `BufferedSubscription` 的环形缓冲（REST 订阅为 1000 条）被写满后会**覆盖最旧的事件**；客户端若长期不来拉取，旧事件即丢失。
3. 事件被丢弃/覆盖不会向客户端报错，只能通过 Prometheus 指标观测：

```
syncthing_events_total{event="DeviceConnected",state="created"}    # logger 收到的事件数
syncthing_events_total{event="DeviceConnected",state="delivered"}  # 成功送入订阅 channel 的事件数
syncthing_events_total{event="DeviceConnected",state="dropped"}    # 因订阅阻塞被丢弃的事件数
```

因此 `/rest/events` 适合 UI 刷新、监控告警等场景；需要完整可靠事件流的场景应使用审计日志（audit log，订阅 `AllEvents` 并逐行写文件，见 `lib/syncthing/auditservice.go`）。

## 8. 相关代码索引

| 内容                                             | 位置                                 |
| ------------------------------------------------ | ------------------------------------ |
| 路由注册                                         | `lib/api/api.go:266-267`             |
| 掩码/缓冲/超时常量                               | `lib/api/api.go:63-71`               |
| `getIndexEvents` / `getDiskEvents` / `getEvents` | `lib/api/api.go:1337-1375`           |
| `getEventMask`（解析 `events` 参数）             | `lib/api/api.go:1377-1387`           |
| `getEventSub`（按掩码缓存订阅）                  | `lib/api/api.go:1389-1400`           |
| 启动时创建默认/磁盘订阅                          | `lib/syncthing/syncthing.go:139-143` |
| `BufferedSubscription.Since`（长轮询核心）       | `lib/events/events.go:510-539`       |
| `Event` 结构体（JSON 信封）                      | `lib/events/events.go:252-260`       |
| CSRF / API Key 校验                              | `lib/api/api_csrf.go`                |
| 客户端示例（dev 工具）                           | `cmd/dev/stevents/main.go`           |

进一步阅读：

- 《事件系统内部机制》（`docs/events-system-internals.md`）：事件如何产生、分发、缓冲的完整函数调用流程
- 《事件类型参考》（`docs/event-types-reference.md`）：全部 33 种事件的触发时机与 `data` 结构
