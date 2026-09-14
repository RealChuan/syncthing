# Syncthing 事件系统内部机制与函数调用流程

本文档分析 `lib/events` 包的核心实现：事件如何产生、如何分发到订阅者、REST 长轮询如何工作，以及完整的函数调用流程。

核心代码：`lib/events/events.go`（约 580 行，无外部事件框架依赖，基于 channel + suture 服务模型）。

---

## 1. 总体架构

Syncthing 的事件系统是一个**进程内的发布/订阅（pub/sub）事件总线**，单个 `logger` 实例贯穿整个进程生命周期：

```
┌────────────────────────────────────────────────────────────────────┐
│                          事件生产者                                  │
│  model / config / discover / connections / scanner / api_auth ...  │
│         （各子系统在相应时机调用 evLogger.Log(type, data)）           │
└──────────────┬─────────────────────────────────────────────────────┘
               │ Log() → events channel (容量 64)
               ▼
┌────────────────────────────────────────────────────────────────────┐
│                     logger.Serve()（事件总线主循环）                  │
│  · 分配全局 ID（nextGlobalID++）                                     │
│  · 为每个匹配掩码的订阅分配订阅内 ID                                   │
│  · 逐个投递到订阅 channel（15ms 超时则丢弃该订阅的这条事件）            │
└──────┬─────────────────────┬──────────────────────┬────────────────┘
       │                     │                      │
       ▼                     ▼                      ▼
┌──────────────┐  ┌──────────────────┐  ┌────────────────────────┐
│ subscription │  │  subscription    │  │     subscription       │
│ mask=Default │  │  mask=Disk       │  │  mask=AllEvents        │
│              │  │                  │  │  （audit service）      │
└──────┬───────┘  └────────┬─────────┘  └───────────┬────────────┘
       │                   │                        │
       ▼                   ▼                        │ (直接读 channel)
┌──────────────────┐ ┌──────────────────┐           ▼
│ BufferedSub      │ │ BufferedSub      │   ┌───────────────┐
│ 环形缓冲 1000     │ │ 环形缓冲 1000     │   │ audit 日志文件  │
│ pollingLoop 搬运 │ │ pollingLoop 搬运  │   └───────────────┘
└──────┬───────────┘ └────────┬─────────┘
       │                    │
       ▼                    ▼
  GET /rest/events    GET /rest/events/disk
  （Since() 长轮询阻塞等待新事件）
```

三个关键角色：

| 组件                                             | 职责                                                                                                   |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------ |
| `Logger`（`logger`）                             | 事件总线。接收 `Log()` 调用，按订阅掩码分发；本身是一个 suture 服务（goroutine）                       |
| `Subscription`（`subscription`）                 | 一次 `Subscribe(mask)` 的结果，持有独立的事件 channel 和订阅内 ID 序列                                 |
| `BufferedSubscription`（`bufferedSubscription`） | 订阅的缓冲包装：后台 goroutine 持续把 channel 里的数据搬进环形缓冲，供 REST `Since()` 随机读取与长轮询 |

## 2. 核心类型与常量

```go
// lib/events/events.go

const BufferSize = 64                 // logger 事件入口 channel 与订阅 channel 的容量
const eventLogTimeout = 15 * time.Millisecond  // 向订阅 channel 投递的超时

type EventType int64                  // 位掩码：每种事件类型占一个 bit

type Event struct {
	SubscriptionID int         `json:"id"`       // 订阅内顺序 ID（REST 游标）
	GlobalID       int         `json:"globalID"` // 全局 ID
	Time           time.Time   `json:"time"`
	Type           EventType   `json:"type"`
	Data           interface{} `json:"data"`
}

type Logger interface {
	suture.Service                     // Serve(ctx) 由 supervisor 驱动
	Log(t EventType, data interface{})  // 生产事件
	Subscribe(mask EventType) Subscription
}

type subscription struct {
	mask          EventType
	events        chan Event           // 容量 64
	toUnsubscribe chan *subscription
	timeout       *time.Timer
	ctx           context.Context
}
```

## 3. logger：事件总线主循环

### 3.1 事件入口 `Log()`

```go
// lib/events/events.go:328-335
func (l *logger) Log(t EventType, data interface{}) {
	l.events <- Event{
		Time: time.Now(), // intentionally high precision
		Type: t,
		Data: data,
		// SubscriptionID and GlobalID are set in sendEvent
	}
}
```

`Log()` 只是把事件塞进 `l.events`（容量 64 的 channel），随即返回，**不阻塞业务逻辑**（除非 channel 满——即事件总线本身处理不过来）。

### 3.2 服务循环 `Serve()`

`logger` 由 suture supervisor 驱动（`cmd/syncthing/main.go:462-463` 中加入 `earlyService`，**早于配置加载和其他所有服务**启动，保证 `Starting` 等早期事件不丢失）：

```go
// lib/events/events.go:297-326
func (l *logger) Serve(ctx context.Context) error {
loop:
	for {
		select {
		case e := <-l.events:      // 新事件 → 分发
			l.sendEvent(e)
			metricEvents.WithLabelValues(e.Type.String(), metricEventStateCreated).Inc()
		case fn := <-l.funcs:      // Subscribe() 提交的闭包 → 在循环内执行
			fn(ctx)
		case s := <-l.toUnsubscribe:
			l.unsubscribe(s)
		case <-ctx.Done():
			break loop
		}
	}
	// 关闭所有订阅的 channel，让消费者的 range/Poll 退出
	for _, s := range l.subs {
		close(s.events)
	}
	return nil
}
```

单一 goroutine 串行处理所有事件和订阅管理，**免锁**地维护 `l.subs` 切片与 `l.nextSubscriptionIDs`。

### 3.3 分发 `sendEvent()`

```go
// lib/events/events.go:337-368（节选）
func (l *logger) sendEvent(e Event) {
	l.nextGlobalID++
	e.GlobalID = l.nextGlobalID

	for i, s := range l.subs {
		if s.mask&e.Type != 0 {                    // 订阅掩码命中该事件类型
			e.SubscriptionID = l.nextSubscriptionIDs[i]
			l.nextSubscriptionIDs[i]++

			l.timeout.Reset(eventLogTimeout)       // 15ms
			timedOut := false
			select {
			case s.events <- e:                    // 正常投递
				metricEvents.WithLabelValues(..., "delivered").Inc()
			case <-l.timeout.C:                    // 订阅 channel 满 → 丢弃
				timedOut = true
				metricEvents.WithLabelValues(..., "dropped").Inc()
			}
			// ... timer 清理
		}
	}
}
```

要点：

- `GlobalID` 全局递增，所有订阅看到的同一事件 `globalID` 相同；
- `SubscriptionID` 按订阅独立递增——这就是 REST 事件 JSON 中 `id` 的来源，因此 **`id` 只在单个订阅内有意义**；
- 15ms 投递超时：慢订阅不会拖垮事件总线，代价是该订阅丢失事件（`dropped` 指标）。

## 4. subscription：订阅

### 4.1 订阅 `Subscribe(mask)`

```go
// lib/events/events.go:370-402（节选）
func (l *logger) Subscribe(mask EventType) Subscription {
	res := make(chan Subscription)
	l.funcs <- func(ctx context.Context) {
		s := &subscription{
			mask:          mask,
			events:        make(chan Event, BufferSize),  // 容量 64
			toUnsubscribe: l.toUnsubscribe,
			timeout:       time.NewTimer(0),
			ctx:           ctx,
		}
		l.subs = append(l.subs, s)
		l.nextSubscriptionIDs = append(l.nextSubscriptionIDs, 1)  // 订阅内 ID 从 1 开始
		res <- s
	}
	return <-res
}
```

订阅操作通过 `l.funcs` channel 交给 `Serve()` 循环执行，保证与事件分发在同一 goroutine 内串行化。

### 4.2 消费方式

`subscription` 提供两种消费接口（`lib/events/events.go:431-466`）：

| 方式           | 接口                           | 使用者                                              |
| -------------- | ------------------------------ | --------------------------------------------------- |
| 阻塞读 channel | `C() <-chan Event`             | 内部消费者（audit service、autoUpgrade）——实时接收  |
| 带超时轮询     | `Poll(timeout) (Event, error)` | 超时返回 `ErrTimeout`，channel 关闭返回 `ErrClosed` |

## 5. BufferedSubscription：环形缓冲与长轮询

REST API 不直接消费 `subscription`，而是通过 `BufferedSubscription`（`lib/events/events.go:475-543`）。

### 5.1 创建与搬运循环

```go
// lib/events/events.go:489-508
func NewBufferedSubscription(s Subscription, size int) BufferedSubscription {
	bs := &bufferedSubscription{
		sub: s,
		buf: make([]Event, size),      // REST 订阅：size = 1000
	}
	bs.cond = syncutil.NewTimeoutCond(&bs.mut)
	go bs.pollingLoop()
	return bs
}

func (s *bufferedSubscription) pollingLoop() {
	for ev := range s.sub.C() {       // 持续排空订阅 channel
		s.mut.Lock()
		s.buf[s.next] = ev            // 写入环形缓冲
		s.next = (s.next + 1) % len(s.buf)
		s.cur = ev.SubscriptionID     // 记录最新订阅内 ID
		s.cond.Broadcast()            // 唤醒所有 Since() 等待者
		s.mut.Unlock()
	}
}
```

`pollingLoop` 是订阅 channel 与环形缓冲之间的搬运工：

- 保证 `subscription.events`（容量 64）被及时排空，避免触发 15ms 丢弃；
- 每写入一条事件 `Broadcast()` 一次，唤醒阻塞在 `Since()` 中的 HTTP 请求；
- 缓冲写满后**静默覆盖**最旧事件（`s.next` 循环前进）。

### 5.2 读取 `Since()`（长轮询核心）

```go
// lib/events/events.go:510-539
func (s *bufferedSubscription) Since(id int, into []Event, timeout time.Duration) []Event {
	s.mut.Lock()
	defer s.mut.Unlock()

	if id >= s.cur {                       // 没有比 id 更新的事件
		waiter := s.cond.SetupWait(timeout)
		defer waiter.Stop()

		for id >= s.cur {                  // 长轮询：等待新事件
			if eventsAvailable := waiter.Wait(); !eventsAvailable {
				return into                // 超时 → 返回已有内容（REST 层序列化为 []）
			}
		}
	}

	// 从环形缓冲中收集所有 SubscriptionID > id 的事件
	for i := s.next; i < len(s.buf); i++ {
		if s.buf[i].SubscriptionID > id {
			into = append(into, s.buf[i])
		}
	}
	for i := range s.next {
		if s.buf[i].SubscriptionID > id {
			into = append(into, s.buf[i])
		}
	}
	return into
}
```

逻辑解读：

1. `s.cur` 是缓冲中最新事件的订阅内 ID。若 `since >= s.cur`，说明没有新事件，则在 `TimeoutCond` 上等待（**长轮询阻塞点**）；
2. `pollingLoop` 每写入事件都会 `Broadcast()`，`waiter.Wait()` 被唤醒后重新检查条件；
3. 返回时从环形缓冲的两段（`s.next` 到末尾、开头到 `s.next`）收集所有 `SubscriptionID > id` 的事件，天然按时间有序；
4. 若 `since` 落后太多（超过 1000 条），被覆盖的事件直接跳过——ID 不连续是客户端检测丢事件的唯一线索。

## 6. 启动装配流程

```
cmd/syncthing/main.go
 └─ main()
     ├─ evLogger := events.NewLogger()          // main.go:462
     ├─ earlyService.Add(evLogger)              // main.go:463 —— 事件总线最先启动
     │    （earlyService: 在配置加载之前运行的 supervisor）
     │
     ├─ syncthing.LoadConfigAtStartup(..., evLogger, ...)   // 配置服务
     │
     └─ app, err := syncthing.New(cfgWrapper, sdb, evLogger, cert, appOpts)  // main.go:571
         └─ App.startup()                       // lib/syncthing/syncthing.go:129
             ├─ defaultSub := events.NewBufferedSubscription(
             │      a.evLogger.Subscribe(api.DefaultEventMask), api.EventSubBufferSize)  // :142
             ├─ diskSub := events.NewBufferedSubscription(
             │      a.evLogger.Subscribe(api.DiskEventMask), api.EventSubBufferSize)     // :143
             ├─ a.evLogger.Log(events.Starting, {...})       // :156 —— 首个事件
             │
             ├─ （各业务服务陆续加入 mainService supervisor 并启动，
             │     过程中产生的事件经 evLogger 分发到上述订阅）
             │
             ├─ api.New(..., defaultSub, diskSub, evLogger, ...)  // 订阅注入 API 服务
             └─ a.evLogger.Log(events.StartupComplete, {...})    // :319 —— 启动完成事件
```

其他固定订阅者（在 `App.startup()` 中装配）：

| 订阅者                                            | 掩码              | 用途                                   | 代码                                  |
| ------------------------------------------------- | ----------------- | -------------------------------------- | ------------------------------------- |
| audit service（若开启 `STAUDITFILE`/auditwriter） | `AllEvents`       | 每个事件序列化为一行 JSON 写入审计文件 | `lib/syncthing/auditservice.go:33-51` |
| autoUpgrade（candidate 构建）                     | `DeviceConnected` | 连接到更高版本的设备时触发升级检查     | `cmd/syncthing/main.go:701-703`       |
| ur failure handler                                | `Failure`         | 失败事件处理                           | `lib/syncthing/syncthing.go:130`      |

## 7. 完整函数调用流程

### 7.1 事件生产 → REST 响应（端到端）

以"设备建立连接"为例（`DeviceConnected`）：

```
[协议层] 新连接就绪
  │
  ├─ model.AddConnection()                         lib/model/model.go:2286
  │    构造 event := map[string]string{id, deviceName, clientName, clientVersion, type, addr}
  │
  ├─ m.evLogger.Log(events.DeviceConnected, event)  lib/model/model.go:2324
  │    │
  │    └─ l.events <- Event{Time, Type, Data}      lib/events/events.go:329（异步入口）
  │
  ▼ （logger.Serve goroutine）
logger.Serve(ctx)                                   lib/events/events.go:297
  └─ sendEvent(e)                                   lib/events/events.go:337
       ├─ nextGlobalID++ → e.GlobalID
       ├─ 遍历 l.subs，对 mask 命中的每个订阅：
       │    ├─ e.SubscriptionID = nextSubscriptionIDs[i]++
       │    └─ s.events <- e（15ms 超时 → 丢弃并计数 dropped）
       │
       ▼ （bufferedSubscription.pollingLoop goroutine，每个 REST 订阅一个）
  for ev := range s.sub.C()                         lib/events/events.go:500
       ├─ s.buf[s.next] = ev（环形缓冲写入）
       ├─ s.cur = ev.SubscriptionID
       └─ s.cond.Broadcast()（唤醒等待中的 HTTP 请求）
            │
            ▼ （HTTP handler goroutine）
[REST 客户端] GET /rest/events?since=42&timeout=60
  ├─ csrfManager 校验（X-API-Key / CSRF token）      lib/api/api_csrf.go:89-95
  ├─ getIndexEvents(w, r)                           lib/api/api.go:1337
  │    ├─ getEventMask("DeviceConnected")           lib/api/api.go:1377
  │    └─ getEventSub(mask)                          lib/api/api.go:1389（按掩码缓存）
  ├─ getEvents(w, r, sub)                           lib/api/api.go:1349
  │    ├─ 解析 since/limit/timeout
  │    ├─ w.Flush()（先发响应头）
  │    └─ eventSub.Since(since, [], timeout)         lib/events/events.go:510
  │         ├─ id < s.cur → 直接收集 SubscriptionID > id 的事件
  │         └─ id >= s.cur → cond.SetupWait(timeout).Wait()（阻塞至 Broadcast 或超时）
  └─ sendJSON(w, evs) → 200 OK, [{"id":43,"globalID":...,"type":"DeviceConnected",...}]
```

### 7.2 内部消费者路径（无 REST 参与）

以 audit service 为例：

```
evLogger.Log(type, data)
  → logger.Serve → sendEvent → s.events <- e
  → auditService.Serve(ctx):                        lib/syncthing/auditservice.go:33
       sub := s.evLogger.Subscribe(events.AllEvents)
       for { select { case ev, ok := <-sub.C(): enc.Encode(ev) } }
  → 每行一个事件 JSON 写入审计文件
```

内部消费者直接读 channel（`sub.C()`），没有缓冲层——所以 audit 日志记录的是事件总线投递成功的全量事件（15ms 丢弃仍可能丢）。

## 8. 并发与可靠性小结

| 机制                     | 参数                                  | 行为                                   |
| ------------------------ | ------------------------------------- | -------------------------------------- |
| `logger.events` 入口缓冲 | 64                                    | `Log()` 在总线过载时阻塞生产者（背压） |
| 订阅 channel 投递超时    | 15ms                                  | 慢订阅丢事件，总线不阻塞               |
| REST 环形缓冲            | 1000/订阅                             | 客户端长期不拉取则旧事件被覆盖         |
| 长轮询默认超时           | 60s                                   | 超时返回 `[]`，客户端立即重发          |
| Prometheus 指标          | `syncthing_events_total{event,state}` | created / delivered / dropped 三态计数 |

**设计取向**：事件系统服务于 UI 与监控，明确不保证可靠传输（代码注释即说明 LocalChangeDetected 类高频事件可能 "overwhelm the event receiver"）。需要完整事件流时应使用 audit 日志。

## 9. 相关代码索引

| 内容                                                | 位置                                        |
| --------------------------------------------------- | ------------------------------------------- |
| 事件类型定义与位掩码                                | `lib/events/events.go:25-63`                |
| `Event` 结构体                                      | `lib/events/events.go:252-260`              |
| `NewLogger` / `Serve` / `Log` / `sendEvent`         | `lib/events/events.go:282-368`              |
| `Subscribe` / `unsubscribe`                         | `lib/events/events.go:370-422`              |
| `subscription.Poll` / `C()`                         | `lib/events/events.go:431-466`              |
| `NewBufferedSubscription` / `pollingLoop` / `Since` | `lib/events/events.go:489-539`              |
| `NoopLogger`（测试/占位用）                         | `lib/events/events.go:556-582`              |
| Prometheus 指标                                     | `lib/events/metrics.go`                     |
| 事件总线启动                                        | `cmd/syncthing/main.go:462-463`             |
| 默认/磁盘订阅创建                                   | `lib/syncthing/syncthing.go:139-143`        |
| TimeoutCond 实现                                    | `lib/syncutil`（`syncutil.NewTimeoutCond`） |
