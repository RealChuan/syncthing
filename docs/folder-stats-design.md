# Syncthing 单文件夹传输统计接口设计

|          |                                                                                           |
| -------- | ----------------------------------------------------------------------------------------- |
| 状态     | 设计稿（待评审）                                                                          |
| 日期     | 2026-09-11（评审修订）                                                                    |
| 关联文档 | [websocket-push-design.md](websocket-push-design.md)（WS 推送通道，本设计复用其基础设施） |

---

## 1. 背景与目标

### 1.1 现状与问题

现有 REST 接口可以拼出单文件夹的上传/下载信息，但存在实时性与拼装成本问题：

1. **`/rest/db/completion`（本机设备）不是实时的**。`folderCompletion()`（[model.go:914-948](../lib/model/model.go)）中，本机设备的 `needBytes` 完全来自数据库 `CountNeed`，而数据库只在**整个文件拉取完成提交时**更新。场景推演：文件夹中只有一个 100GB 超大文件，传输到 99% 时 completion 仍然纹丝不动，直到文件完成才跳变。
2. **`/rest/db/completion`（远端设备）是半实时的**。远端通过 BEP `DownloadProgress` 每 `ProgressUpdateIntervalS`（默认 5s）自报已收块数并扣减；远端把该值设为 0 时降级为按文件粒度。
3. **上传/下载速率无任何 per-folder 数据**。连接级统计（`/rest/system/connections`）是设备粒度、含索引流量，无法拆分到文件夹。
4. 外部消费方需要轮询多个端点 + 订阅事件流再自行拼装，语义与时效性难以保证。

### 1.2 目标

在 syncthing 内部新增一个**单文件夹传输统计聚合层**，对外提供：

1. **全量快照查询**：新增 REST 端点，一次请求返回单文件夹的所有传输相关数据——各状态文件个数、聚合进度与总进度（overall）、实时速率、正在上传/下载明细、在途请求数；
2. **变化推送**：数据变化时通过事件系统推送（REST 长轮询与既有 WS 通道均可消费）；
3. **真实实时**：进度与速率在数据块粒度实时更新，特别是**单超大文件场景**下逐块可见变化；
4. **最小侵入**：核心传输逻辑零修改，仅在两个数据漏斗各加数行记录调用（纯增量，可随时移除）。

### 1.3 非目标（v1 明确不做）

- 修改 `/rest/db/status`、`/rest/db/completion` 等现有端点的行为（保持完全向后兼容）；
- 跨文件夹的全局汇总接口（后续可加 `folder` 为空时的聚合，本期 YAGNI）；
- 统计数据持久化（会话级内存数据，重启清零；持久统计由现有 `lib/stats` 负责）；
- 改动 BEP 协议或远端上报机制。

## 2. 现有数据源实时性审计

设计前的代码级审计结论（单超大文件场景推演基于"100GB 单文件、块大小 2MB、远端 ProgressUpdateIntervalS=5"）：

| 数据源                                                                     | 字段                               | 更新时机                                       | 单超大文件场景表现                 |
| -------------------------------------------------------------------------- | ---------------------------------- | ---------------------------------------------- | ---------------------------------- |
| `/rest/db/status`（[Summary()](../lib/model/folder_summary.go#L121-L203)） | `needBytes`/`inSyncBytes`          | 每块到达即扣减（活体 puller 状态，请求时现算） | **实时**，逐块递减                 |
| `/rest/db/completion`（本机）                                              | `needBytes`/`completion`           | 仅文件完成提交 DB 时                           | **卡死**，传输中不动               |
| `/rest/db/completion`（远端）                                              | `needBytes`/`completion`           | 远端每 5s 自报扣减 + 文件完成时索引更新        | 约 5s 粒度；远端关闭进度上报则卡死 |
| `DownloadProgress` 事件（本地 puller）                                     | `bytesDone`/`bytesTotal`（按文件） | 事件每 5s 聚合发送，但数据本体是活体           | 数据实时，推送 5s 粒度             |
| `RemoteDownloadProgress` 事件                                              | 按文件已收块数                     | 远端自报（5s）                                 | 5s 粒度，协议固有                  |
| `/rest/system/connections`                                                 | `in/outBytesTotal`                 | 连接级累计                                     | 设备粒度，**无文件夹维度**         |

**根因结论**：数据库（CountNeed/索引）的粒度是"文件"，远端自报的粒度是"5 秒"；两者都不提供"每块"粒度。要做到块级实时，必须在**本机数据路径**上记录——这正是本设计的埋点位置。两点补充：`db/status` 的 `global*` 为集群元数据、`local*`/`need*` 为本机计数（need = 本机相对 global 的缺口，不含远端视角）；其 `needBytes` = DB 按文件的基准 − 内存中活体 puller 的逐块累计（[sharedPullerState](../lib/model/sharedpullerstate.go#L305-L314)）——实时性来自内存扣减，DB 基准在单文件传输期间静止。

**语义区分**（本设计严格区分两种"字节"）：

- **进度字节**（progress bytes）：文件完成度语义，包含本地复制/复用块（`copiedFromOrigin`/`reused`）——回答"还差多少完成"；
- **网络字节**（transfer bytes）：真实收发的数据块字节——回答"带宽用了多少、速度多快"。重传块会被重复计入，这是吞吐量的真实语义。

现有数据源把两者混在 `needBytes` 扣减里；新接口将它们拆开成独立字段。

## 3. 方案选型

| 方案                                     | 描述                                                                 | 评价                                                                                                                                                 |
| ---------------------------------------- | -------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| A. 纯进度差分（零侵入）                  | 轮询现有进度数据做差分算速率                                         | 5s 粒度起、字节语义不纯（含本地复制）、上传侧受远端上报配置影响——与用户诉求直接冲突 ✗                                                                |
| B. 协议层消息拦截                        | 在 BEP 连接层按消息归属归账                                          | `Request`/`Response` 消息虽含 folder 字段，但字节计数器（`cr/cw`）是连接级且含压缩加密后的帧字节，按消息归账需要侵入 protocol 核心，侵入性反而更高 ✗ |
| **C. 数据漏斗埋点 + 观察者服务（选定）** | 在两个唯一数据漏斗各加数行记录调用，聚合/速率/推送逻辑全部收在新文件 | 每块到达即计数（真实时）、纯网络字节、核心逻辑零修改、埋点行可整体移除 ✓                                                                             |

方案 C 的两个埋点都是**唯一漏斗**：

- 上传：`model.Request()`（[model.go:1964](../lib/model/model.go#L1964)）——远端从本机拉块的唯一入口；
- 下载：`model.RequestGlobal()`（[model.go:2454-2462](../lib/model/model.go#L2454-L2462)）——本机向远端请求块的唯一出口（`pullBlock` 经此调用）。

## 4. 架构设计

```
┌─────────────────────────────────────────────────────────────────┐
│                        数据路径（既有代码）                         │
│                                                                   │
│  m.Request()          ←远端拉块（上传侧漏斗）                      │
│      │ 成功返回                                                     │
│      │ recordUpload(folder, device, n)          ← 新增数行         │
│  m.RequestGlobal()    →向远端要块（下载侧漏斗）                     │
│      │ 成功返回                                                     │
│      │ recordDownload(folder, device, n)        ← 新增数行         │
└──────┬──────────────────────────────────────────────────────────┘  │
       ▼                                                             │
┌─────────────────────────────────────────────────────────────────┐
│        folderTransferStats 统计服务（新文件，纯内存观察者）          │
│  · per (folder, device, 方向) 计数器与会话累计                      │
│  · 1s 粒度滑动窗口（默认 10 桶）→ 实时速率                          │
│  · 在途请求计数（请求进入/退出）                                     │
│  · 脏标记 + 合并窗口：有变化才推送                                   │
└──────┬───────────────────────────────┬───────────────────────────┘
       │                               │
       ▼                               ▼
┌──────────────────────┐   ┌─────────────────────────────────────┐
│ REST 快照端点（新增）  │   │ 事件系统（既有 evLogger）             │
│ GET /rest/db/        │   │ 新事件类型 FolderTransferStats        │
│ folderstats?folder=X │   │  → REST 长轮询 /rest/events           │
│ 组装：counts + 进度 +  │   │  → WS 通道 /rest/events/ws           │
│ 速率 + 在途 + 明细     │   │   （复用 websocket-push-design.md）   │
└──────────────────────┘   └─────────────────────────────────────┘
```

快照端点的数据来源（均在请求时现算；时效性承诺见 §5.5 矩阵）：

| 数据块                     | 来源                                                         |
| -------------------------- | ------------------------------------------------------------ |
| 各状态文件个数、文件夹状态 | 复用 `folderSummaryService.Summary()`                        |
| 下载进度                   | Summary 的 `needBytes`/`inSyncBytes`（活体 puller 扣减链路） |
| 正在下载明细               | `progressEmitter` 活体注册表（`registry[folder]`）           |
| 上传进度（按设备）         | `folderCompletion(device, folder)`                           |
| 正在上传明细               | `deviceDownloads[device]`（远端自报）                        |
| 速率 / 会话累计字节 / 在途 | **新统计服务**                                               |

## 5. 详细设计

### 5.1 埋点位置

**上传侧**——`model.Request()` 已有命名返回值 `(out protocol.RequestResponse, err error)`，函数入口（参数校验之后）加一个 defer：

```go
// lib/model/model.go — m.Request()，在 L1999 附近的调试日志前
defer func() {
    if err == nil && out != nil {
        m.transferStats.RecordUpload(req.Folder, deviceID, int64(len(out.Data())))
    }
}()
```

仅成功路径计数（哈希校验失败、文件不存在等错误返回不计数）；`out.Data()` 若无导出访问器，用 `req.Size`（两者在成功路径等值，末块以实际读取长度为准时取更小值）。

**下载侧**——`RequestGlobal()` 的返回处改为：

```go
// lib/model/model.go — m.RequestGlobal()，L2460-2462
buf, err := conn.Request(ctx, &protocol.Request{...})
if err == nil {
    m.transferStats.RecordDownload(folder, deviceID, int64(len(buf)))
}
return buf, err
```

**在途计数**（可选增强，同一对埋点顺带完成）：`RecordUploadStart/End`、`RecordDownloadStart/End` 在请求进入/退出时调用，统计服务内部维护在途计数；上传侧与既有 `connRequestLimiters` 的信号量并行不悖（不替换、只观察）。

两处埋点均为**纯增量调用**：不改变任何既有控制流、错误路径、锁顺序；删除这两处调用即完全还原。

### 5.2 统计服务

新建 `lib/model/folder_transfer_stats.go`，模式对齐 `folderSummaryService`（`svcutil.AsService` 生命周期）：

```go
type folderTransferStats struct {
    mut     sync.Mutex
    folders map[string]*folderStats
    evLogger events.Logger
    interval time.Duration   // 推送合并窗口，默认 1s（常量，可调）
}

type folderStats struct {
    up       directionStats                     // 文件夹级汇总（所有设备合计）
    down     directionStats
    byDevice map[protocol.DeviceID]*deviceStats // per 远端设备
}

type deviceStats struct {
    up   directionStats
    down directionStats
}

type directionStats struct {
    sessionBytes int64        // 会话累计（重启清零）
    inflight     int64        // 在途请求数
    buckets      [N]int64     // 滑动窗口：N=10 个 1s 桶（环形）
    bucketStart  time.Time    // 当前桶起始时刻
    dirty        bool         // 自上次推送以来是否有变化
}
```

**滑动窗口速率算法**：`Record*` 时按当前时刻定位桶（过期桶清零滚动）；查询 `Rate()` 时 = `sum(buckets) / 窗口有效秒数`（当前未满的桶按经过时间折算）。空闲超过窗口长度速率自然归零，无需显式清理。锁内操作均为 O(1)（一次 map 查找 + 数组写），相对每块至少 128KiB 的磁盘/网络 IO 开销可忽略。

**推送合并窗口**：`Record*` 置脏；服务 goroutine 每 `interval`（默认 1s）醒来，遍历脏文件夹，对每个脏文件夹发一次 `FolderTransferStats` 事件并清脏。"数据变化时推送"的工程化解读：变化必然触发推送，但同一窗口内的多个块合并为一次（逐块推送会淹没事件总线）。

**生命周期**：`NewModel` 中构造并启动，`Stop` 时随服务退出；文件夹移除时随 ClusterConfig 清理对应条目（监听既有配置提交钩子，与 `folderSummaryService` 相同模式）。

### 5.3 REST 端点与响应结构

```
GET /rest/db/folderstats?folder=<folderID>
```

- `folder` 必填；未知/未运行文件夹 → 404（与 `getDBStatus` 行为一致）；
- 认证复用既有 `/rest` 中间件链（API Key / CSRF），无新增逻辑；
- handler 为薄封装，组装逻辑在 model 的新方法 `FolderStats(folder string) (*FolderStatsSnapshot, error)` 中（需要访问 `deviceDownloads`、`progressEmitter` 等私有成员；同步在 `Model` 接口新增该方法并重新生成 mock）。

响应结构（字段名沿用 `FolderSummary` 的 JSON 命名以保持生态一致性）。示例场景：文件夹与唯一远端设备 `P56IOI7` 双向传输——本机正在从其下载 `huge.bin`（80GB），同时远端正在从本机拉取 `medium.zip`（9GB，已传 4500 块）：

```json
{
  "folder": "abcd-1234",
  "generatedAt": "2026-09-10T12:00:00+08:00",

  "state": {
    "state": "syncing",
    "stateChanged": "2026-09-10T11:58:30+08:00",
    "error": null,
    "watchError": null,
    "pullErrors": 2
  },

  "counts": {
    "global": {
      "files": 100,
      "directories": 10,
      "symlinks": 0,
      "deleted": 5,
      "bytes": 100000000000,
      "totalItems": 115
    },
    "local": {
      "files": 97,
      "directories": 10,
      "symlinks": 0,
      "deleted": 5,
      "bytes": 20000000000,
      "totalItems": 112
    },
    "need": {
      "files": 3,
      "directories": 0,
      "symlinks": 0,
      "deleted": 0,
      "bytes": 80000000000,
      "totalItems": 3
    },
    "inSync": { "files": 97, "bytes": 20000000000 },
    "receiveOnlyChanged": { "files": 0, "bytes": 0 }
  },

  "progress": {
    "overall": {
      "completion": 0.56,
      "bytesDone": 112000000000,
      "bytesTotal": 200000000000,
      "participants": 2,
      "source": "live+remoteReported"
    },
    "download": {
      "completion": 0.2,
      "bytesDone": 20000000000,
      "bytesTotal": 100000000000,
      "source": "live"
    },
    "upload": {
      "completion": 0.92,
      "bytesDone": 92000000000,
      "bytesTotal": 100000000000,
      "source": "remoteReported",
      "granularity": "5s",
      "byDevice": {
        "P56IOI7-MOCKDEVICE": {
          "completion": 0.92,
          "needBytes": 8000000000,
          "globalBytes": 100000000000,
          "remoteState": "syncing"
        }
      }
    }
  },

  "rate": {
    "download": {
      "bytesPerSec": 12582912,
      "windowSeconds": 10,
      "sessionBytes": 21000000000
    },
    "upload": {
      "bytesPerSec": 10485760,
      "windowSeconds": 10,
      "sessionBytes": 9000000000
    },
    "byDevice": {
      "P56IOI7-MOCKDEVICE": {
        "upload": { "bytesPerSec": 10485760, "sessionBytes": 9000000000 },
        "download": { "bytesPerSec": 12582912, "sessionBytes": 21000000000 }
      }
    }
  },

  "inflight": { "downloadRequests": 4, "uploadRequests": 6 },

  "active": {
    "downloading": [
      {
        "file": "huge.bin",
        "bytesDone": 19000000000,
        "bytesTotal": 80000000000,
        "blocksTotal": 40000,
        "blocksPulled": 9500,
        "blocksPulling": 4,
        "reused": 0,
        "copiedFromOrigin": 0,
        "copiedFromElsewhere": 0,
        "updated": "2026-09-10T12:00:00.123+08:00"
      }
    ],
    "uploading": {
      "P56IOI7-MOCKDEVICE": [{ "file": "medium.zip", "blocksDownloaded": 4500 }]
    }
  }
}
```

字段语义要点：

- `progress.overall`：**总进度**。`completion = Σ(globalBytes − needBytes_p) / (globalBytes × participants)`；participants 为**待收敛参与方**：本机（folder type ≠ send-only）+ 远端中 ClusterConfig 广播 folder type ≠ send-only 的设备（远端类型由 [BEP Folder.Type](../lib/protocol/bep_clusterconfig.go#L70-L76) 携带，model 需新增存储，见 §6）。**send-only 是"源"，其差异是故意状态而非待完成工作，不参与计算**（单向场景因此可达 100%）。本机份额逐块实时，远端份额 ≤5s；场景行为与边界见 §5.6。
- `progress.download.bytesDone` = `inSyncBytes`（活体扣减链路，**含本地复制块**，完成度语义）；
- **进度设备拆分的不对称是语义性的**：上传进度的主体是 N 个远端设备（各有 need → `byDevice`）；下载进度的主体只有本机（need = 本机对 global 的缺口，无"对某设备的下载进度"可言）——设备维度在下载侧体现为流量来源（`rate.byDevice.*.download`）；
- `rate.*.sessionBytes` / `bytesPerSec` = **纯网络字节**（埋点计数，不含本地复制、含重传）；
- `progress.upload` 文件夹级聚合 = 所有**待收敛远端设备** completion 的字节加权平均（`Σ(globalBytes − needBytes_p) / (globalBytes × n)`，与 overall 的远端部分同公式；无待收敛远端时 `completion = 1.0`）；`byDevice` 为按设备明细，5s 粒度权威值由 `source`/`granularity` 标注，消费方可用 `rate.byDevice.*.upload.sessionBytes` 做实时插值；
- `rate.byDevice`：**按设备双向归属**——多设备并发同步时逐设备统计。下载侧同一文件的不同块可来自不同设备（puller 按 `blockAvailability` → `leastBusy` 逐块选源，候选含两类：已完整拥有该文件的设备、以及自身仍在下载但其 temp 文件已含该块的设备（`FromTemporary`，[代码](../lib/model/model.go#L2861-L2905)）），每块计入真实源设备；`active.downloading[].blocksPulled` 为该文件跨设备合计；
- 其余字段的时效性见 §5.5 矩阵；两侧进度基准的一致性分析见 §5.6。

### 5.4 事件推送

新增事件类型 `FolderTransferStats`（[events.go](../lib/events/events.go) 三处小改：常量、`String()`、`UnmarshalEventType()`，与既有 33 种事件同模式）。

事件载荷（精简版快照，只含统计服务自有数据，避免对 model 的反向依赖）：

```json
{
  "type": "FolderTransferStats",
  "data": {
    "folder": "abcd-1234",
    "download": { "bytesPerSec": 12582912, "sessionBytes": 21000000000 },
    "upload": { "bytesPerSec": 10485760, "sessionBytes": 9000000000 },
    "inflight": { "downloadRequests": 4, "uploadRequests": 6 },
    "byDevice": {
      "P56IOI7-MOCKDEVICE": {
        "upload": { "bytesPerSec": 10485760, "sessionBytes": 9000000000 },
        "download": { "bytesPerSec": 12582912, "sessionBytes": 21000000000 }
      }
    }
  }
}
```

消费路径（零额外开发）：

- **REST 长轮询**：`GET /rest/events?events=FolderTransferStats`；
- **WS 推送**：`GET /rest/events/ws?mask=FolderTransferStats`——完全复用 [websocket-push-design.md](websocket-push-design.md) 的通道与游标语义。

全量数据（文件个数、进度、明细）不进事件，继续由快照端点按需查询；事件只承载高频变化的速率/计数，控制事件流量。

### 5.5 时效性矩阵（最终承诺）

| 字段                                         | 单超大文件场景时效性                  | 依赖                                            |
| -------------------------------------------- | ------------------------------------- | ----------------------------------------------- |
| `progress.overall`                           | 本机份额逐块实时 + 远端份额 ≤5s       | 聚合（Summary + Completion + 远端 folder type） |
| `progress.download.*`                        | 逐块实时                              | 活体 puller 状态（既有链路）                    |
| `active.downloading[]`                       | 逐块实时                              | 活体 puller 状态（既有链路）                    |
| `rate.download/upload.*`                     | 逐块实时                              | 新埋点                                          |
| `inflight.*`                                 | 逐块实时                              | 新埋点                                          |
| `rate.byDevice.*`                            | 逐块实时                              | 新埋点                                          |
| `progress.upload.*`（文件夹聚合与 byDevice） | ≤5s（远端自报）                       | 既有远端上报，协议固有                          |
| `active.uploading[][]`                       | ≤5s（远端自报）                       | 同上                                            |
| `counts.*`（文件个数）                       | 按需现算，底层按文件粒度（DB 索引）   | 既有 DB 查询                                    |
| `counts.need.bytes`                          | 逐块实时（DB 基准 + 内存扣减，见 §2） | 既有 DB + 内存链路                              |
| 事件推送延迟                                 | ≤合并窗口（默认 1s）                  | 新服务                                          |

### 5.6 场景与数据语义

**准确性基准（与 completion 对标）**：进度公式两侧同构——`global − (DB need − 在途扣减)`：已提交部分与 `db/completion` 同源同查询（分子 = `global − CountNeed`，分母 = `CountGlobal`），零偏差；差异仅在在途项——本机为内存活体（精确，含本地复制块），远端为 BEP 自报（≤5s，偏差上界 = 5s × 速率；远端自报同样含其本地复制块，语义对称）。设计的全部增量价值即在在途项。由此导出不变量：**空闲时（无在途）`progress.download` ≡ `db/completion`（本机）、`progress.upload.byDevice` ≡ `db/completion`（对应远端）**（见 §7）。

#### 单向传输与文件夹类型

结论：**单向传输是本设计的自然退化情形，无需任何条件分支或结构修改**。埋点对称地放在上传/下载两个漏斗上，聚合层不区分方向——某方向无流量时，该方向计数器自然为 0、进行中明细自然为空、对端 completion 自然为 100%，所有字段退化为诚实的单边数据。

依据：请求服务路径 [model.Request()](../lib/model/model.go#L1964-L2088) 不检查 folder type（folder type 只控制本机 puller 行为，不控制数据服务）——send-only 文件夹照常被远端拉块，单向上传场景的上传侧数据完整有效。

| 场景（本机 folder type） | 下载侧字段                                                                                        | 上传侧字段                                                     | overall                                                                                |
| ------------------------ | ------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `sendrecv`（双向，默认） | 全部正常                                                                                          | 全部正常                                                       | 正常                                                                                   |
| `receiveonly`（单向收）  | 正常：拉取链路逐块实时（[L1340](../lib/model/folder_sendrecv.go#L1340) 只排除 receive-encrypted） | `rate.upload` 自然为 0                                         | **= 本机完成度**（send-only 源被排除，无稀释）；要纯本机进度也可看 `progress.download` |
| `sendonly`（单向发）     | 无拉取活动；`progress.download` 显示本机与 global 的**静态差异**（故意不同步的量，不实时变化）    | 全部正常：远端照常拉取，completion / 远端自报 / 速率埋点均有效 | **= 远端（收敛方）完成度**，远端全到货即 100%（本机 send-only 份额被排除）             |
| `receiveencrypted`       | `active.downloading` 为空（§9 已列）；速率埋点正常                                                | 无                                                             | completion 可用                                                                        |

**残余边界**：源为 `sendrecv` 且恰好 100% 完成时，其份额仍计入 overall（它是合法收敛方，未来漂移会重新产生工作）——receive-only 本机 + 双向源的进度显示因此偏高，纯本机进度看 `progress.download`；远端 ClusterConfig 未收到（类型未知）保守计入；全员 send-only（participants = 0）时 `completion = 1.0`。

#### 字段生命周期语义

重启（"同步一半 → 关闭 → 续传"）是暴露字段语义差异最直观的探针，但它与忽略规则/增删文件这类**输入扰动**（见审计表）不同：后者改变 DB 基准，数据真的变了；前者**不改变任何进度数据**——重启改变的只是哪些字段需要内存重建。字段按生命周期分三类：

| 字段类别                              | 语义                             | 重启后行为                                                                                                         |
| ------------------------------------- | -------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `progress.*` / `counts.*` / `overall` | 绝对值（DB 持久基准 + 活体扣减） | **立即正确反映历史已完成部分**（大值）——DB 持久，`inSyncBytes` 等不受会话边界影响                                  |
| `rate.*.sessionBytes` / `bytesPerSec` | 会话累计（埋点计数）             | 归零重计。**sessionBytes 从不参与进度计算**，重启不污染进度                                                        |
| `active.*` / `inflight.*`             | 瞬时状态                         | puller 重新注册后重建；恢复处理前 active 为空、无活体扣减（进度短暂只反映已提交文件，与现有 `db/status` 行为一致） |

**续传边界**：① 字节语义分工——temp 已有内容作为 reused 块，计入 `bytesDone`（完成度不重复累计）但不计入 `sessionBytes`（网络字节只记本次真实传输）；② 在途进度不可预加载——temp 按块偏移稀疏写入（磁盘大小 ≠ 有效字节）、DB 不持久化块状态，准确值由 puller 重处理该文件时逐块重哈希得出（[reuseBlocks](../lib/model/folder_sendrecv.go#L1181-L1184)），上表"恢复处理前短暂低估"由此而来。

#### 场景审计表

| 场景                    | 准确性影响                                                                                                                                                                                     | 结论/处理                                                                                                                        |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| 忽略规则/unsupported 项 | DB 计数层已统一过滤：CountGlobal/CountLocal/need 查询均排除 `LocalInvalidFlags`（= Unsupported\|Ignored\|MustRescan\|ReceiveOnly\|RemoteInvalid，[定义](../lib/protocol/bep_fileinfo.go#L36)） | **设计零改动**；生效依赖下次扫描（[SetIgnored](../lib/model/folder.go#L808-L831)），偏差窗口 = 扫描延迟，与 GUI/`db/status` 一致 |
| 拉取错误/非法文件名     | 文件留在 need（无法同步），completion 与本设计同步停在真实值                                                                                                                                   | 非计算误差（确实未同步）；`state.pullErrors` 字段可见                                                                            |
| 新文件落盘未扫描        | 未进 DB，global/need 均不含                                                                                                                                                                    | 进度暂不可见；窗口 = 扫描间隔，与所有现有接口一致                                                                                |
| 索引传播延迟            | 本机 DB 的 global 滞后真实集群                                                                                                                                                                 | **快照内部自洽**：所有参与方 need 从同一 DB 视角计算，无跨设备错位                                                               |
| 远端在途自报延迟        | ≤5s（`ProgressUpdateIntervalS`）                                                                                                                                                               | 偏差上界 = 5s × 速率，`granularity` 字段标注                                                                                     |
| 参与方增删              | participants 变化 → overall 跳变                                                                                                                                                               | 诚实行为：新设备全额 need 进入分母（集群多了待收敛方）                                                                           |
| 重启/断点续传/会话边界  | 见上文（字段生命周期语义）                                                                                                                                                                     | 已声明                                                                                                                           |
| 中途增删文件            | 所有公式快照时从同一 DB 视角现算，无累积漂移；新增文件使 completion 与 overall 可能回退                                                                                                        | 诚实行为（工作量真实回归）；同步中的文件被删 → puller 取消并 Deregister，下次快照自动反映                                        |

## 6. 代码变更清单

| 文件                                                    | 变更                                                                                              | 规模       |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------------- | ---------- |
| `lib/model/folder_transfer_stats.go`（新建）            | 统计服务：计数器、滑动窗口、在途、脏推送循环、`FolderStatsSnapshot` 组装（含 overall 收敛方聚合） | ~300 行    |
| [model.go](../lib/model/model.go#L1964)                 | `Request()` 埋点（defer 记录）                                                                    | 3-5 行     |
| [model.go](../lib/model/model.go#L2454-L2462)           | `RequestGlobal()` 埋点（返回前记录）                                                              | 3-4 行     |
| [model.go](../lib/model/model.go) Model 接口            | 新增 `FolderStats(folder)` 方法声明；`NewModel` 构造注入                                          | ~6 行      |
| [model.go](../lib/model/model.go#L1392) ccHandleFolders | 新增 `remoteFolderTypes` map 存储远端 folder type（overall 收敛方判定用，纯增量）                 | ~4 行      |
| lib/model/mocks/model.go                                | 重新生成 mock（mockery）                                                                          | 自动生成   |
| [events.go](../lib/events/events.go)                    | 新增 `FolderTransferStats` 事件类型（三处）                                                       | 3 行       |
| [api.go](../lib/api/api.go)                             | 路由注册 1 行 + `getDBFolderStats` handler（解析参数、调用、`sendJSON`）                          | 1 + ~40 行 |
| `lib/model/folder_transfer_stats_test.go`（新建）       | 单测                                                                                              | ~200 行    |
| `lib/api/api_test.go`                                   | 端点测试追加                                                                                      | ~60 行     |

不改动：`folder_summary.go`、`progressemitter.go`、`devicedownloadstate.go`、BEP 协议、现有端点、GUI。

## 7. 测试策略

1. **统计服务单测**（`folder_transfer_stats_test.go`）：
   - 计数正确性：N 次记录后 `sessionBytes` 求和一致；错误路径不计数；
   - 滑动窗口：注入时间后验证速率计算（满桶、半桶、跨桶滚动、空闲归零）；
   - 并发：多 goroutine 并发 Record + Snapshot（`-race`）；
   - 推送节流：脏标记合并窗口内多次记录只发一次事件；无变化不发。
2. **埋点单测**（model_test 模式）：
   - `RequestGlobal` 成功记录字节数 = 返回 buffer 长度；失败不记录；
   - `Request` 成功路径（含 temp file 命中与常规读取两条成功返回）记录；校验失败路径不记录。
3. **端点测试**（`api_test.go`，`httptest` + 中间件链）：
   - 未知 folder → 404；正常 folder → JSON 结构完整（golden 断言关键字段）；
   - 认证行为与其他 `/rest/db/*` 端点一致。
4. **验收场景（来自设计评审）**：单超大文件传输中轮询快照端点——`progress.download.bytesDone`、`rate.*`、`active.downloading[0].bytesDone` 在相邻两次轮询间单调变化；对照 `/rest/db/completion`（本机）同期纹丝不动，验证实时性差异符合第 5.5 节矩阵。
5. **事件集成**：`mask=FolderTransferStats` 经 `/rest/events` 与 WS 通道均可收到（复用 WS 设计的测试模式）。
6. **completion 对标不变量**：空闲状态（无在途传输）下断言 `progress.download.completion` ≡ `/rest/db/completion?device=<本机>` 的 `completion`，`progress.upload.byDevice[D]` ≡ `/rest/db/completion?device=D`——已提交部分与 completion 同源零偏差的机器验证（见 §5.6）。

## 8. 实施步骤

1. 统计服务核心（计数器/窗口/速率）+ 单测——独立可合入；
2. 两处埋点 + 单测——行为不变，仅记录；
3. 事件类型注册 + 推送循环 + 单测；
4. `FolderStatsSnapshot` 组装方法 + Model 接口 + mock 重生成；
5. REST 端点 + handler 测试；
6. 单超大文件场景验收（第 7.4 节）。

步骤 1-2 完成即构成最小可用版本（内部已有实时数据）；3-5 逐层暴露。

## 9. 风险与已知取舍

| 风险/取舍                        | 说明                                                                                                         | 缓解                                                                          |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------- |
| 埋点引入性能开销                 | 每块一次 O(1) 加锁操作（128KiB+ 的 IO 对比下可忽略）                                                         | 锁内无 IO、无分配热路径；基准测试验证                                         |
| `out.Data()` 访问器可能不存在    | `requestResponse.data` 为私有字段                                                                            | 优先用 `req.Size`（成功路径两者等值）；或补一个只读访问器（纯增量）           |
| 上传进度 5s 粒度                 | 远端自报机制协议固有，无法本地解决                                                                           | `source`/`granularity` 显式标注；`sessionBytes` 实时可供插值；文档声明        |
| 会话累计重启清零                 | syncthing 无跨会话每文件夹传输字节的持久统计（lib/stats 仅 lastFile/lastScan 等）                            | 字段命名 `sessionBytes` 明确语义；跨会话累计需扩展 lib/stats 持久化（演进项） |
| 重传重复计数                     | 网络吞吐的真实语义（与连接级计数一致）                                                                       | 文档明确语义；进度字段不受影响                                                |
| 多连接设备归账                   | 同设备多连接时按 deviceID 归账（非 per-connection）                                                          | 与 completion/deviceDownloads 粒度一致，语义自洽                              |
| Model 接口变更                   | 新增方法需要全量 mock 重生成                                                                                 | additive 变更，编译器保证完整性                                               |
| 事件量                           | 高频传输时每文件夹每秒 1 条事件                                                                              | 合并窗口可调；事件载荷精简；`mask` 可选订阅                                   |
| receive-encrypted 无 active 明细 | [folder_sendrecv.go:1340](../lib/model/folder_sendrecv.go#L1340) 跳过 puller 注册，`active.downloading` 为空 | 与现有 `db/status` 扣减行为一致；速率（埋点）不受影响，照常统计               |
| 离线设备冻结 overall 份额        | overall 含全部收敛方，离线设备的 completion 停在最后已知值                                                   | 设备回归即恢复；消费方可看 byDevice 明细                                      |
| 删除不占 overall 字节            | 仅剩删除/元数据操作时 `overall.completion` = 1.0 但 `needDeletes > 0`                                        | 结合 `counts.need` 判断；文档声明                                             |
| overall 依赖远端 folder type     | 收敛方判定需要远端类型（BEP ClusterConfig 已携带；model 现不存储，需新增 map）                               | 未收到 ClusterConfig 的远端保守计入；存储为本设计变更（§6）                   |

## 10. 附录：与既有接口的关系

| 维度       | `/rest/db/status`                            | `/rest/db/completion`           | `/rest/db/folderstats`（本设计）         |
| ---------- | -------------------------------------------- | ------------------------------- | ---------------------------------------- |
| 范围       | 单文件夹：全局+本地计数，need/进度为本机视角 | 单文件夹 × 单设备（本机或远端） | 单文件夹，双向全量                       |
| 总进度     | 无                                           | 无（需自行平均多次调用）        | 有（overall：收敛方聚合，单向可达 100%） |
| 速率       | 无                                           | 无                              | 有（双向、按设备、实时）                 |
| 进行中明细 | 无                                           | 无                              | 有（下载实时/上传 5s）                   |
| 在途请求数 | 无                                           | 无                              | 有                                       |
| 上传侧数据 | 无                                           | completion + 5s 自报扣减        | completion（标注粒度）+ 实时网络字节     |
| 存废       | 保留（GUI 依赖）                             | 保留（GUI 依赖）                | 新增，不影响以上两者                     |
