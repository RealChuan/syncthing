# Syncthing 事件类型参考：触发时机与 JSON 结构

本文档整理 `lib/events` 中定义的全部事件类型：每种事件**在什么时机由哪段代码产生**（即 Syncthing "主动发出事件"的场景），以及事件 `data` 字段的基本 JSON 结构。

事件类型定义见 `lib/events/events.go:25-63`（`EventType` 为 int64 位掩码，每种类型占一个 bit）。

## 0. 关于"主动推送"

Syncthing **从不主动向 REST 客户端发起网络推送**（无 WebSocket/SSE/Webhook）。其工作方式是：

1. 内部子系统在**特定时机**调用 `evLogger.Log(type, data)` 主动产生事件（本文档第 2 节）；
2. REST 客户端通过 `GET /rest/events?since=N&timeout=60` **长轮询**拉取——正在阻塞的请求会在新事件产生后立即返回。

因此"何时主动推送事件"等价于"何时调用 `Log()`"，即下表所列的全部触发点。

## 1. 事件类型总表

按 `lib/events/events.go` 中的定义顺序（共 33 种）：

| 事件类型                   | 触发时机                                        | 生产代码                               |
| -------------------------- | ----------------------------------------------- | -------------------------------------- |
| `Starting`                 | 进程启动、算出本机设备 ID 后                    | `lib/syncthing/syncthing.go:156`       |
| `StartupComplete`          | 所有服务启动完毕                                | `lib/syncthing/syncthing.go:319`       |
| `DeviceDiscovered`         | 本地发现（beacon 广播）出现新设备               | `lib/discover/local.go:297`            |
| `DeviceConnected`          | 与设备建立（首条）连接                          | `lib/model/model.go:2324`              |
| `DeviceDisconnected`       | 与设备的最后一条连接关闭                        | `lib/model/model.go:1925`              |
| `DeviceRejected`（已废弃） | 未知设备尝试连接                                | `lib/model/model.go:2277`              |
| `PendingDevicesChanged`    | 待定设备列表增删                                | `lib/model/model.go:2269, 3221, 3267`  |
| `DevicePaused`             | 配置提交后设备被暂停                            | `lib/model/model.go:3054`              |
| `DeviceResumed`            | 配置提交后设备被恢复                            | `lib/model/model.go:3062`              |
| `ClusterConfigReceived`    | 收到远端 ClusterConfig 协议消息                 | `lib/model/model.go:1301`              |
| `LocalChangeDetected`      | 扫描发现本地变更并写入索引库                    | `lib/model/folder.go:1307`             |
| `RemoteChangeDetected`     | 同步拉取的远端变更写入本地                      | `lib/model/folder.go:1315`             |
| `LocalIndexUpdated`        | 本地索引（数据库）更新                          | `lib/model/folder.go:1341`             |
| `RemoteIndexUpdated`       | 收到远端索引（Index/Request 消息处理完）        | `lib/model/indexhandler.go:456`        |
| `ItemStarted`              | 同步引擎开始处理一个文件/目录/链接              | `lib/model/folder_sendrecv.go`（多处） |
| `ItemFinished`             | 同步引擎处理完一个文件/目录/链接                | `lib/model/folder_sendrecv.go`（多处） |
| `StateChanged`             | 文件夹状态机迁移（idle→scanning→syncing…）      | `lib/model/folderstate.go:137, 186`    |
| `FolderRejected`（已废弃） | 未知文件夹被分享过来                            | `lib/model/model.go:1434`              |
| `PendingFoldersChanged`    | 待定文件夹列表增删                              | `lib/model/model.go:1519, 3186, 3301`  |
| `ConfigSaved`              | 配置写入磁盘                                    | `lib/config/wrapper.go:525`            |
| `DownloadProgress`         | 本机下载进度变化（节流发送）                    | `lib/model/progressemitter.go:128`     |
| `RemoteDownloadProgress`   | 远端设备报告的下载进度（DownloadProgress 消息） | `lib/model/model.go:2429`              |
| `FolderSummary`            | 文件夹统计摘要更新（节流）                      | `lib/model/folder_summary.go:367`      |
| `FolderCompletion`         | 某设备对某文件夹的完成度变化                    | `lib/model/folder_summary.go:412`      |
| `FolderErrors`             | 文件夹出现错误条目                              | `lib/model/folder_sendrecv.go:232`     |
| `FolderScanProgress`       | 扫描大文件夹期间的进度                          | `lib/scanner/walk.go:184`              |
| `FolderPaused`             | 文件夹被暂停                                    | `lib/model/model.go:3030`              |
| `FolderResumed`            | 文件夹被恢复                                    | `lib/model/model.go:3030`              |
| `FolderWatchStateChanged`  | 文件系统监听（inotify 等）状态变化              | `lib/model/folder.go:1176`             |
| `ListenAddressesChanged`   | 监听地址（LAN/WAN）发生变化                     | `lib/connections/service.go:820`       |
| `LoginAttempt`             | REST/GUI 登录认证尝试（成功或失败）             | `lib/api/api_auth.go:43`               |
| `Failure`                  | 各类内部失败（扫描失败、时序异常等）            | 多处                                   |
| `UpgradeRestartScheduled`  | 自动升级完成、计划重启                          | `cmd/syncthing/main.go:752`            |

订阅掩码：

- `DefaultEventMask`（`/rest/events` 默认）= 全部事件 **减去** `LocalChangeDetected`、`RemoteChangeDetected`（高频事件单独走 `/rest/events/disk`）；
- `DiskEventMask`（`/rest/events/disk`）= `LocalChangeDetected | RemoteChangeDetected`。

## 2. 各事件 `data` 结构与触发说明

以下每节给出触发条件和 `data` 的基本 JSON 结构（`data` 之外的事件信封字段 `id`/`globalID`/`time`/`type` 略去，完整信封见 `docs/rest-events-api.md`）。

### 2.1 生命周期

#### Starting

进程启动早期（设备 ID 计算完成后）发出，总是进程的第一个事件。

```go
// lib/syncthing/syncthing.go:156
a.evLogger.Log(events.Starting, map[string]string{
	"home": locations.GetBaseDir(locations.ConfigBaseDir),
	"myID": a.myID.String(),
})
```

```json
{ "home": "/home/user/.local/state/syncthing", "myID": "P56IOI7-..." }
```

#### StartupComplete

所有子服务启动完成后发出（`lib/syncthing/syncthing.go:319`），结构与 `Starting` 相同（`home`、`myID`）。

### 2.2 设备与连接

#### DeviceConnected

`model.AddConnection()` 中，设备连接加入模型时发出（`lib/model/model.go:2311-2324`）：

```json
{
  "id": "P56IOI7-...",
  "deviceName": "laptop",
  "clientName": "syncthing",
  "clientVersion": "v1.27.8",
  "type": "tcp-client",
  "addr": "192.168.1.20:22000"
}
```

#### DeviceDisconnected

设备的**最后一条**连接关闭时发出（`lib/model/model.go:1923-1929`）：

```json
{ "id": "P56IOI7-...", "error": "connection closed" }
```

#### DeviceDiscovered

本地发现（mDNS/beacon）收到新设备通告时发出（`lib/discover/local.go:296-301`）：

```json
{
  "device": "P56IOI7-...",
  "addrs": ["192.168.1.20:22000", "[fe80::...]:22000"]
}
```

#### DevicePaused / DeviceResumed

配置提交（`CommitConfiguration`）导致设备暂停/恢复时发出（`lib/model/model.go:3054, 3062`）：

```json
{ "device": "P56IOI7-..." }
```

#### DeviceRejected（已废弃，由 PendingDevicesChanged 取代）

未知设备尝试连接时发出（`lib/model/model.go:2277`）：

```json
{ "device": "P56IOI7-...", "address": "192.168.1.20:22000" }
```

#### PendingDevicesChanged

待定设备（尚未接受/忽略的新设备）列表变化时发出。触发点：

- 收到未知设备的 ClusterConfig（`model.go:3221`）
- 待定设备过期清理（`model.go:2269`）
- 用户通过 API 忽略/忽略撤销（`DismissPendingDevice`，`model.go:3267`）

```json
{
  "added": [{ "deviceID": "...", "address": "...", "name": "..." }],
  "removed": [{ "deviceID": "..." }]
}
```

（`added`/`removed` 只出现非空的一个。）

#### ClusterConfigReceived

收到并处理完远端的 ClusterConfig 协议消息（`lib/model/model.go:1300-1302`）：

```json
{ "device": "P56IOI7-..." }
```

（`ClusterConfigReceivedEventData` 仅含 `device` 字段。）

### 2.3 磁盘变更（DiskEventMask，走 /rest/events/disk）

`LocalChangeDetected` 与 `RemoteChangeDetected` 由同一函数产生（`lib/model/folder.go:1351-1380` 的 `emitDiskChangeEvents`），区别在来源：

- `LocalChangeDetected`：**扫描**后发现本地变更写库时（`updateLocalsFromScanning`，`folder.go:1303-1309`）；
- `RemoteChangeDetected`：**同步拉取**（puller 应用远端变更）写库时（`updateLocalsFromPulling`，`folder.go:1311-1317`）。

对每个变更文件（跳过无效项）发一条事件：

```json
{
  "folder": "documents",
  "path": "report.docx",
  "action": "modified",
  "type": "file"
}
```

- `action`：`modified` / `deleted`（由 `file.IsDeleted()` 决定）
- `type`：`file` / `dir` / `symlink`

### 2.4 索引

#### LocalIndexUpdated

本地索引数据库更新后发出（`lib/model/folder.go:1341-1347`），一次批量更新一条：

```json
{
  "folder": "documents",
  "items": 3,
  "filenames": ["a.txt", "b.txt", "c.txt"],
  "sequence": 128,
  "version": 128
}
```

（`version` 为 `sequence` 的旧名兼容。）

#### RemoteIndexUpdated

收到远端索引并处理完毕（`lib/model/indexhandler.go:456-462`）：

```json
{
  "device": "P56IOI7-...",
  "folder": "documents",
  "items": 3,
  "sequence": 256,
  "version": 256
}
```

### 2.5 同步过程

#### ItemStarted / ItemFinished

同步引擎（send/receive 文件夹）处理每个 item（文件/目录/符号链接）的开始与结束，成对出现（`lib/model/folder_sendrecv.go` 多处，如 `:557-573`）：

```json
// ItemStarted
{
  "folder": "documents",
  "item": "report.docx",
  "type": "file",
  "action": "update"
}
```

```json
// ItemFinished
{
  "folder": "documents",
  "item": "report.docx",
  "type": "file",
  "action": "update",
  "error": null
}
```

`error` 为 `null`（成功）或错误字符串（`events.Error()`，`lib/events/events.go:548-554`）；`type` 为 `file`/`dir`/`symlink`，`action` 为 `update`/`delete` 等。

#### StateChanged

文件夹状态机迁移时发出。两个入口（`lib/model/folderstate.go`）：

- `setState()`（`:137`）：正常状态迁移（idle/scanning/syncing/…）
- `setError()`（`:186`）：进入或退出错误状态

```json
{
  "folder": "documents",
  "from": "idle",
  "to": "scanning",
  "duration": 12.3
}
```

`duration`（前一状态持续秒数）与 `error`（进入 `FolderError` 状态时）为可选字段。状态取值：`idle`、`scanning`、`syncing`、`sync-preparing`、`error`、`scan-waiting`、`sync-waiting` 等。

#### DownloadProgress

本机作为拉取方的下载进度，由 `ProgressEmitter` 周期性/节流发出（`lib/model/progressemitter.go:117-129`）：

```json
{
  "documents": {
    "report.docx": { "bytesTotal": 1048576, "bytesDone": 524288, ... }
  }
}
```

外层 key 为 folder ID，内层 key 为文件路径，值为 `PullerProgress` 结构（块级进度）。仅在有活动下载时发出。

#### RemoteDownloadProgress

远端设备从本机下载数据时，收到其 DownloadProgress 协议消息后发出（`lib/model/model.go:2429-2433`）：

```json
{
  "device": "P56IOI7-...",
  "folder": "documents",
  "state": { "report.docx": 42, "photo.jpg": 7 }
}
```

`state` 为 `文件路径 → 正在下载的块计数` 映射。

#### FolderErrors

文件夹出现错误条目（如扫描失败、权限问题）时发出（`lib/model/folder_sendrecv.go:231-234`）：

```json
{
  "folder": "documents",
  "errors": [{ "error": "...", "path": "..." }]
}
```

### 2.6 统计与摘要

#### FolderSummary

文件夹统计摘要（本地/全局/待同步的文件数与字节数等），按事件触发并节流合并（`lib/model/folder_summary.go:354-370`）：

```json
{
  "folder": "documents",
  "summary": {
    "globalFiles": 1234,
    "globalDirectories": 56,
    "globalSymlinks": 0,
    "globalDeleted": 12,
    "globalBytes": 1073741824,
    "globalTotalItems": 1302,
    "localFiles": 1200,
    "localDirectories": 56,
    "localSymlinks": 0,
    "localDeleted": 12,
    "localBytes": 1072693248,
    "localTotalItems": 1268,
    "needFiles": 34,
    "needDirectories": 0,
    "needSymlinks": 0,
    "needDeletes": 0,
    "needBytes": 1048576,
    "needTotalItems": 34,
    "receiveOnlyChangedFiles": 0,
    "...": 0,
    "errors": 0,
    "pullErrors": 0,
    "invalid": "",
    "state": "idle",
    "stateChanged": "2026-09-09T10:00:00Z",
    "version": 128,
    "sequence": 128,
    "ignorePatterns": false,
    "watchError": ""
  }
}
```

`FolderSummary` 结构体定义见 `lib/model/folder_summary.go:72` 起。

#### FolderCompletion

某远端设备对某文件夹的同步完成度（随 FolderSummary 一并发给共享设备，`lib/model/folder_summary.go:390-412`）：

```json
{
  "completion": 96.5,
  "globalBytes": 1073741824,
  "needBytes": 37748736,
  "globalItems": 1302,
  "needItems": 34,
  "folder": "documents",
  "device": "P56IOI7-..."
}
```

#### FolderScanProgress

扫描大文件夹期间定期发出（`lib/scanner/walk.go:182-186`，仅在 `STPROGRESS` 或扫描耗时较长时）：

```json
{ "folder": "documents", "current": 4521, "total": 10000, "rate": 1234.5 }
```

（`current`/`total` 为已扫描/总字节数，`rate` 为字节/秒。）

### 2.7 文件夹管理

#### FolderPaused / FolderResumed

配置提交导致文件夹暂停/恢复时发出（`lib/model/model.go:3024-3031`）：

```json
{ "id": "documents", "label": "我的文档" }
```

#### FolderRejected（已废弃，由 PendingFoldersChanged 取代）

未知文件夹被分享到本机时发出（`lib/model/model.go:1434`）：

```json
{ "device": "...", "folder": "...", "deviceName": "...", "folderLabel": "..." }
```

#### PendingFoldersChanged

待定文件夹（远端分享但本机未配置）列表变化时发出（`model.go:1519, 3186, 3301`）：

```json
{
  "added": [
    {
      "folderID": "...",
      "folderLabel": "...",
      "deviceID": "...",
      "deviceName": "..."
    }
  ],
  "removed": [{ "folderID": "..." }]
}
```

#### FolderWatchStateChanged

文件系统监听（inotify/kqueue/FSEvents）错误状态变化时发出（`lib/model/folder.go:1161-1177`）：

```json
{ "folder": "documents", "from": "watcher error: ...", "to": "" }
```

`from`/`to` 只在非空时出现；都为空表示回到正常。

### 2.8 网络监听

#### ListenAddressesChanged

连接服务成功监听（或监听失败）时发出（`lib/connections/service.go:818-824`）：

```json
{
  "address": "tcp://0.0.0.0:22000",
  "lan": ["tcp://192.168.1.10:22000"],
  "wan": ["tcp://203.0.113.10:22000"]
}
```

### 2.9 API 与认证

#### LoginAttempt

每次 REST/GUI 登录认证尝试后发出（无论成败，`lib/api/api_auth.go:33-53`）：

```json
{
  "success": false,
  "username": "admin",
  "remoteAddress": "192.168.1.20:54321",
  "proxy": ""
}
```

`proxy` 仅当请求经可信代理转发时出现。

### 2.10 失败与升级

#### Failure

通用失败事件，`data` 不定型：

- 字符串形式：`m.evLogger.Log(events.Failure, "watching for changes encountered an event outside of the filesystem root")`（`lib/model/folder.go:1138`）
- 结构化形式：`contract.FailureData{Description, Extra}`（`lib/model/indexhandler.go:473`），如索引时序异常

```json
{ "description": "...", "extra": { "seenSeq": "10", "returnedSeq": "5" } }
```

`ur.NewFailureHandler`（`lib/syncthing/syncthing.go:130`）订阅该事件做失败统计。

#### UpgradeRestartScheduled

自动升级二进制成功后、计划延迟重启时发出（`cmd/syncthing/main.go:752-755`）：

```json
{ "delayS": 60, "newVersion": "v1.28.0" }
```

## 3. 事件产生（Log 调用）的分布统计

`evLogger.Log(events.XXX, ...)` / `EventLogger.Log(...)` 的调用点分布（生产代码，不含测试）：

| 包                   | 调用点数量 | 主要事件                                                                                                                                 |
| -------------------- | ---------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `lib/model`          | 40+        | ItemStarted/Finished、StateChanged、PendingDevices/Folders、DeviceConnected/Disconnected、FolderSummary/Completion、LocalIndexUpdated 等 |
| `lib/syncthing`      | 2          | Starting、StartupComplete                                                                                                                |
| `lib/config`         | 1          | ConfigSaved                                                                                                                              |
| `lib/scanner`        | 3          | FolderScanProgress、Failure                                                                                                              |
| `lib/api`（含 auth） | 1          | LoginAttempt                                                                                                                             |
| `lib/discover`       | 1          | DeviceDiscovered                                                                                                                         |
| `lib/connections`    | 1          | ListenAddressesChanged                                                                                                                   |
| `cmd/syncthing`      | 1          | UpgradeRestartScheduled                                                                                                                  |

可以看出事件的生产主要集中在 `lib/model`（同步引擎与连接管理），这也是 GUI 实时状态的主要数据来源。

## 4. 消费建议

- **UI/监控**：订阅 `/rest/events`（默认掩码），用 `id` 作游标长轮询；高频变更需求再叠加 `/rest/events/disk`。
- **审计/合规**：使用 `--auditfile`（audit service 订阅 `AllEvents`），逐行 JSON 落盘，不依赖客户端拉取节奏。
- **注意 `id` 语义**：更换 `events` 过滤参数会切换到不同的缓存订阅，`id` 序列完全不同，游标不可跨订阅复用。
