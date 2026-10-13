# Dir-Space 快照设计

## 1. 背景

Curvine 当前的 `curvine fs du` 与 `curvine fs count` 在客户端递归调用
`get_status`/`list_status`。目录包含大量文件时，这会产生与目录项数量近似相同的
RPC、路径解析和元数据遍历，既慢，也会给 active master 的元数据请求路径施加压力。

历史提交 `b83fbd61`（原 GitHub #285，T224-2）留下了可追溯的纯内存聚合原型：

- `DirAgg { bytes, files, dirs }`
- `SubtreeAggregator`
- `DirSpaceSnapshot` 与 `PointSnapshot` 的初始类型
- 对空目录、深目录、孤儿边、环和顺序无关性的单元测试

该提交不在当前 `main-next`，原设计稿与 GitHub #224 的任务上下文已经不可用。本设计以
当前 `main-next`、`b83fbd61` 和现有 `du`/`count` 行为为事实来源重新定义功能。

## 2. 目标

1. 在 master 中异步构建目录子树的逻辑空间快照。
2. 以 O(1) 快照查找替代 `du`/`count` 的客户端递归。
3. 返回逻辑字节数、文件数、目录数以及快照新鲜度和完整性信息。
4. 扫描不持有全局 `fs_dir` 读锁，不修改元数据写路径、inode 序列化格式，也不改变
   journal/checkpoint 的格式和生成内容。
5. 支持低频全局快照和高频 tracked-point 快照；tracked point 来源包括 mount
   `cv_path` 和显式配置路径。
6. 扫描失败、超时、角色切换或触及资源上限时继续提供上一代完整快照。
7. 提供可观测指标，能够判断快照是否可用、陈旧、失败或被资源限制中止。

## 3. 非目标

- 不实现目录配额强制或把聚合结果接入 quota eviction。
- 不在 create/delete/rename/truncate/link 写路径上同步维护计数。
- 不持久化派生快照，不改变 journal/checkpoint 的格式和生成内容；允许修改 restore 的
  并发协调，以保证 DB handle 安全替换。
- 不统计物理副本占用；`bytes` 始终是 inode 的逻辑长度。
- 不实现 WebUI。
- 不保证查询结果线性一致；结果是带生成时间的最终一致快照。
- 第一阶段不支持通过 `cv admin config` 动态调整扫描器。配置从 master 配置文件读取，
  重启后生效。

## 4. 方案比较与结论

### 4.1 周期性旁路快照（采用）

后台从 RocksDB 的同一只读 snapshot 扫描 `inodes` 与 `edges`，在独立内存结构中聚合，
完成后一次性替换当前 generation。

优点：

- 元数据写路径零侵入。
- 扫描器失败不影响文件系统请求。
- 能复用 RocksDB 顺序扫描与原型聚合算法。
- 快照不可变，查询路径简单且不会读到半成品。

代价：

- 结果存在可配置的陈旧窗口。
- 全局扫描开销为 O(inodes + edges)，需要严格的内存、超时和单轮互斥限制。

### 4.2 写路径增量计数（不采用）

实时性更好，但 create/delete/rename/truncate/hard-link、journal replay、主备切换和修复
都必须原子维护祖先链计数。失败恢复与兼容风险远高于本需求收益。

### 4.3 查询时递归（仅保留兼容回退）

现状实现简单且实时，但大目录产生 O(N) RPC 和元数据访问。仅由 CLI 的显式 `--live`
模式使用，不作为默认路径。

## 5. 用户语义

### 5.1 聚合字段

`ContentSummary` 的字段定义如下：

| 字段 | 定义 |
| --- | --- |
| `logical_bytes` | 子树中每个非目录目录项所指 inode 的 `max(len, 0)` 之和 |
| `file_count` | 子树中非目录目录项数量；硬链接按目录项计数 |
| `directory_count` | 子树中目录数量，包含被查询目录自身 |

说明：

- 空目录返回 `bytes=0, files=0, dirs=1`。
- 文件路径不依赖快照，master 直接返回该 inode 的当前长度、`files=1, dirs=0`。
- symlink 和特殊文件与现有 CLI 一致，作为非目录项计数，其逻辑长度取 inode `len`。
- 硬链接按每条 namespace edge 计数和计费，与现有递归 `list_status` 语义一致。
- 物理占用仍由 CLI 根据 replication 展示，不写入快照。

### 5.2 快照状态

RPC 响应包含：

- `generation`：master 进程内单调递增的已发布代次；重启后从 1 重新开始。
- `started_ms` / `finished_ms`：扫描开始和完成时间。
- `source`：`GLOBAL`、`TRACKED_POINT`、`LIVE_FILE` 或 CLI 本地构造的
  `LIVE_RECURSIVE`。
- `degraded`：本轮跳过了损坏/无法解析的记录，但仍发布了可达子树。
- `anomaly_count`：本轮跳过的异常记录数量。

不存在已完成快照、当前目录是快照生成后新建/移入因而发生 snapshot miss 时，目录查询
返回新增的 `SnapshotNotReady` 错误（ErrorKind 37）。功能关闭或对端不支持该能力时返回
既有 `Unsupported`。若最近一次扫描失败但已有且命中旧快照，则继续返回旧快照；调用方
通过 `finished_ms` 判断新鲜度。

### 5.3 CLI 行为

- `curvine fs count <path>` 维持现有 Curvine-only 语义，默认调用服务端
  `GetContentSummary`。
- `curvine fs du <path>` 在非 mount 和 `fs_mode` mount 上默认调用服务端快照；在
  `cache_mode` mount 上继续递归 UFS，避免把“完整 UFS 内容”静默变成“已缓存的 Curvine
  元数据”。用户指定全局 `--cache-only` 后，cache-mode 路径允许查询 Curvine 快照。
- 增加 `--live`，强制使用当前客户端递归实现，用于排障和精确即时查询。
- 服务端不支持该 RPC（滚动升级期间）或返回 `SnapshotNotReady` 时，CLI 打印一次
  stderr 提示并回退到 live 递归，保持现有可用性。权限、网络、非法路径等其他错误不
  回退，避免掩盖真实故障。
- `du --verbose` 增加 `SOURCE`、`GENERATION` 和 `SNAPSHOT_TIME`；普通输出保持兼容。
- `count` 普通列保持兼容；仅在 stderr 输出回退提示。

## 6. 架构

```text
RocksDB snapshot
  ├─ inodes CF ──> compact inode facts (id, kind, len)
  └─ edges CF  ──> (parent_id, child_id)
                         │
                         v
                 SubtreeAggregator
                         │
                         v
                immutable generation
                         │
             DirSpaceSnapshotStore
                ├─ global snapshot
                └─ point snapshots
                         │
       path -> inode id -> O(1) lookup -> RPC -> CLI
```

模块职责：

| 文件 | 职责 |
| --- | --- |
| `space/types.rs` | 聚合值、generation、查询结果和扫描统计类型 |
| `space/agg.rs` | 与存储无关的子树聚合算法 |
| `space/scan_lease.rs` | 与 Raft restore 协调的可取消 metadata scan lease |
| `space/scanner.rs` | RocksDB snapshot、全局扫描、point BFS 扫描、资源边界 |
| `space/store.rs` | 完整 generation 发布、global/point 选择、旧快照保留 |
| `space/manager.rs` | 扫描编排、单轮互斥、leader 检查、路径查询 |
| `space/scheduler.rs` | 单一 coordinator `LoopTask` 与 `ScheduledExecutor` 生命周期 |

`DirSpaceManager` 在 `Master::new` 中构建，由 `MasterService`、`MasterHandler` 和
`MasterActor` 共享同一个 `Arc<DirSpaceManager>`。

## 7. 数据模型

规范类型：

```rust
pub struct DirAgg {
    pub logical_bytes: u64,
    pub file_count: u64,
    pub directory_count: u64,
}

pub enum SnapshotSource {
    Global,
    TrackedPoint,
    LiveFile,
    LiveRecursive,
}

pub struct SnapshotMeta {
    pub generation: u64,
    pub started_ms: i64,
    pub finished_ms: i64,
    pub inode_count: u64,
    pub edge_count: u64,
    pub anomaly_count: u64,
    pub degraded: bool,
}

pub struct DirSpaceSnapshot {
    pub meta: SnapshotMeta,
    pub root_id: i64,
    pub entries: HashMap<i64, DirAgg>,
}

pub struct ContentSummaryResult {
    pub aggregate: DirAgg,
    pub source: SnapshotSource,
    pub snapshot: Option<SnapshotMeta>,
}
```

快照存储采用 `parking_lot::RwLock<Arc<DirSpaceSnapshot>>`。发布时只在写锁内替换一个
`Arc`；查询只在读锁内 clone `Arc`，随后无锁读取不可变内容。无需引入 `arc-swap`
依赖。

point 快照按规范化根路径存入
`RwLock<HashMap<String, Arc<DirSpaceSnapshot>>>`，每个值同时保存当前根 inode id。
查询先按当前 path prefix 选择最长匹配 point，再验证当前解析出的根 id；这样移动出
tracked root 的子目录不会误命中旧 point。

`LIVE_FILE` 的 `snapshot=None`；GLOBAL/TRACKED_POINT 必须携带 snapshot meta。generation
由 store 在成功 publish 的临界区内分配，scanner 产物在发布前不带 generation。

## 8. 扫描算法

### 8.1 一致读取

`DBEngine` 增加基于 `rocksdb::SnapshotWithThreadMode<DB>` 的 bulk iterator 辅助方法。
全局扫描的 `inodes` 和 `edges` iterator 必须共享同一个 RocksDB snapshot。

### 8.1.1 与 Raft restore 的 scan lease

当前 `InodeStore::restore` 要求 `Arc<RocksInodeStore>` 强引用数为 1；scanner 若在释放
`fs_dir` 锁后直接 clone store，会使 restore 失败。为此引入 `MetadataScanControl`：

- `InodeStore::begin_snapshot_scan()` 在短时 `fs_dir.read()` 内申请
  `MetadataScanLease`，lease 持有 store Arc、取消 epoch 和 RAII reader guard。
- scanner 释放 `fs_dir` 锁后，仅通过 lease 访问 DB。
- `FsDir::restore()` 在清空 `root_dir` 之前调用
  `InodeStore::begin_restore()`：control 标记 restoring、递增取消 epoch，阻止新
  lease，并通过 Condvar 等待 active lease 归零。成功返回的 `MetadataRestoreGuard`
  持续覆盖后续 root/DB 恢复流程。
- scanner 每 4096 条记录检查 `lease.is_cancelled()`；被 restore 取消后立即退出并 drop
  RocksDB snapshot/iterator/store Arc，唤醒 restore。
- restore 最多等待 `dir_space_restore_wait_timeout`（默认 `"30s"`）；超时发生在
  `root_dir` 清空之前，因此原内存树和 DB 均保持可查询。无 active lease 后仍保留原有
  strong-count 防御检查；`restore_with_guard()` 只接受有效 restore guard。
- restore 成功或失败都通过 RAII guard 清除 restoring 状态并唤醒等待者。

`MetadataScanControl` 在 `InodeStore::new` 时从 `MasterConf` 注入 restore wait timeout。
scanner/manager 不得永久保存 `Arc<RocksInodeStore>`。只有单轮 scan lease 可以临时
增加 strong count。必须有“scan 与 restore 并发、restore 取消 scan 后成功”和
“restore timeout 后 DB 与内存树均不变”的回归测试。

### 8.1.2 全局读取流程

1. 创建 snapshot。
2. 顺序扫描 `inodes`，反序列化为紧凑 `InodeFact`。
3. 顺序扫描 `edges`，解析 `(parent_id, child_id)`。
4. 通过 `child_id` 查 `InodeFact`：
   - 目录：记录 `child_id -> parent_id`。
   - 非目录：向 `parent_id` 加入长度和文件计数。
5. 从 `ROOT_INODE_ID` BFS 标记可达目录。
6. 逆 BFS 顺序将子目录聚合到父目录。
7. 构建完成后发布新 generation 并释放 RocksDB snapshot。

不使用 `fs_dir` 全局锁进行扫描。跨 CF 的同一 RocksDB snapshot 保证本轮读取视图一致；
孤儿、环和缺失 inode 仅可能来自历史损坏或不完整旧数据。

### 8.2 异常规则

| 异常 | 处理 | anomaly 计数单位 |
| --- | --- | --- |
| root inode 缺失或不是目录 | 整轮失败，不发布 | 不适用 |
| inode key / edge key 无法解析 | 整轮失败，不发布 | 不适用 |
| inode 反序列化失败 | 整轮失败，不发布 | 不适用 |
| 单个 inode record 超过限制 | 整轮因 limit 失败，不发布 | 不适用 |
| edge 指向不存在 inode | 忽略该 edge，发布 degraded | 每条 edge |
| 同一目录出现多个 parent edge | 选择按 edge key 排序后的第一条，发布 degraded | 每个目录 |
| 完全重复 edge | 只计算一次，发布 degraded | 每条额外 edge |
| self-parent edge | 忽略，发布 degraded | 每条 edge |
| 目录环或不从 root 可达 | 剔除该连通分量 | 每个被剔除目录 |
| 负 inode id（root 常量除外） | 忽略对应 inode/edge，发布 degraded | 每条记录 |
| 负文件长度 | 按 0 计，发布 degraded | 每个 inode |
| bytes/files/dirs `checked_add` 溢出 | 整轮失败，不发布 | 不适用 |

`anomaly_count` 是上表规定单位的总和，同一条记录只按最先命中的规则计一次。

### 8.3 资源限制

deadline、active-master 状态和 scan lease 取消状态每处理 4096 条记录检查一次。
单 record bytes、edge/inode/dir 数和估算内存预算必须在每次反序列化、插入或容器扩容
之前检查，避免在批量检查前超额分配：

- 超过 `max_inodes`、`max_edges`、`max_dirs`、`max_depth`、估算内存预算或对应 scope
  的 timeout
- master 不再是 active
- 另一轮仍在运行

以上情况均中止本轮并保留上一代快照，绝不发布截断结果。deadline 是协作式的：单次
RocksDB iterator `next()` 无法被抢占，最坏超时延迟为一次存储读取加 4095 条记录处理
时间。单个
`AtomicBool::compare_exchange` 保证同一 master 同时最多一轮扫描。

### 8.4 tracked-point

tracked point 包括：

1. `auto_track_mounts=true` 时 mount table 中每个 `fs_mode` mount 的 `cv_path`。
   cache-mode UFS namespace 不完整，不创建 point。
2. `tracked_paths` 中显式配置的绝对路径。

每轮 point 扫描开始前，用短时 `fs_dir` 读锁把路径解析为目录 inode id，立即释放锁。
随后在同一个 RocksDB snapshot 上按目录 edge 做 BFS，并按批次读取 child inode。
point 扫描使用独立的 `track_max_inodes` 与 `track_max_depth`。

查询某个目录时，使用 `Path::has_prefix`（组件级，不是字符串 starts_with）对当前
规范化 path 做最长 prefix 匹配。在同一个短时 `fs_dir` 读锁中解析 query inode 与候选
point root 当前 inode id；验证 root id 且 point entries 包含 query inode。满足时若其
全局单调 `generation` 比 global 更新则优先返回 point，否则返回 global。时间戳只用于
展示年龄，不参与新旧排序。两者都 miss 时返回 `SnapshotNotReady`，不能返回全零或
FileNotFound。point 扫描失败仅保留该 point 的上一代，不影响 global。

显式配置 point 优先保留。动态 mount 按规范化 path 排序后占用剩余容量；超出
`max_tracked_points` 的 mount 跳过并增加聚合指标，不使 master 失败。mount 删除或变为
cache mode 时移除对应 point；显式配置数量本身超过上限则配置校验失败。

## 9. 调度与主备生命周期

- 一个 `dir-space-coordinator` scheduler 每 1 秒运行一次轻量 tick，根据 next-due
  时间决定触发 global 或 point；实际扫描仍在该专用线程串行执行。
- global 使用 `global_scan_interval` 和 `global_scan_timeout`。
- point 使用 `tracked_scan_interval`；一轮串行刷新所有 point，并受
  `tracked_scan_timeout` 总 deadline 约束。未轮到的 point 保留旧 generation，下一
  point 轮从上次未完成位置继续 round-robin。
- 任务只在 `MasterMonitor::is_active()` 为 true 时扫描。
- standby 不构建、不服务本地旧 generation；RPC 本身应由 active master 处理。
- `Master::start` 先完成 `mount_manager.restore_best_effort()`，再启动 Dir-Space
  coordinator，避免首轮遗漏恢复的 mount。
- coordinator 记录前一 tick 的 active 状态；发现 standby→active 后把 global/point
  next-due 设为当前时间，因此不等待完整周期。
- leader 丢失时，扫描器通过周期 deadline 检查尽快中止，不发布。
- master 关闭时由现有 `ScheduledExecutor` 生命周期停止任务。
- `LoopTask::terminate()` 返回 `MasterMonitor::is_stop()`。
- 第一次扫描失败不影响 master 启动，仅记录 error 与指标。

## 10. 配置

在 `MasterConf` 增加：

| 配置 | 默认值 | 说明 |
| --- | ---: | --- |
| `dir_space_enabled` | `false` | 总开关；默认关闭避免升级后突然产生扫描负载 |
| `dir_space_global_scan_interval` | `"10m"` | 全局扫描周期 |
| `dir_space_global_scan_timeout` | `"5m"` | global 单轮 cooperative deadline |
| `dir_space_tracked_scan_interval` | `"2m"` | tracked-point 调度周期 |
| `dir_space_tracked_scan_timeout` | `"90s"` | 一轮 point 刷新的总 deadline |
| `dir_space_max_inodes` | `5_000_000` | 全局单轮 inode 上限 |
| `dir_space_max_edges` | `10_000_000` | 全局单轮 dentry/edge 上限 |
| `dir_space_max_dirs` | `1_000_000` | 全局发布目录项上限 |
| `dir_space_memory_budget` | `"768MB"` | 基于容器容量的保守分配预算 |
| `dir_space_max_depth` | `1024` | 全局目录深度上限 |
| `dir_space_track_max_inodes` | `1_000_000` | 单 point inode 上限 |
| `dir_space_track_max_edges` | `2_000_000` | 单 point dentry/edge 上限 |
| `dir_space_track_memory_budget` | `"256MB"` | 单 point 保守分配预算 |
| `dir_space_track_max_depth` | `1024` | 单 point 深度上限 |
| `dir_space_max_tracked_points` | `128` | mount 与显式路径去重后的 point 总上限 |
| `dir_space_max_inode_record_bytes` | `"16MB"` | 单个序列化 inode 上限 |
| `dir_space_auto_track_mounts` | `true` | 自动跟踪 mount `cv_path` |
| `dir_space_tracked_paths` | `[]` | 额外跟踪的绝对目录路径 |
| `dir_space_restore_wait_timeout` | `"30s"` | restore 等待 scan lease 释放的上限 |

校验规则：

- 两个 interval 和两个 timeout 必须大于 0。
- global timeout 不得大于 global interval；tracked timeout 不得大于 tracked interval。
- 数量、字节预算和深度上限必须大于 0。
- 显式 point 数量超过 `max_tracked_points` 时拒绝启动；动态 mount 超量按上文策略跳过。
- tracked path 必须是规范化绝对路径，不能包含 glob。
- 重复路径去重，根路径 `/` 不创建 point（由 global 覆盖）。

首版配置仅在启动时读取。后续若统一 `ConfigHandle` 支持安全 scheduler 重建，可另行加入
runtime admin override；本设计不复用旧的 flat registry 临时模型。

## 11. RPC 与客户端

分配下一个空闲 master RPC code `68`：

```proto
message GetContentSummaryRequest {
    optional string path = 1;
    optional ClientContextProto client_context = 100;
}

message ContentSummaryProto {
    optional uint64 logical_bytes = 1;
    optional uint64 file_count = 2;
    optional uint64 directory_count = 3;
    optional uint64 generation = 4;
    optional int64 started_ms = 5;
    optional int64 finished_ms = 6;
    optional string source = 7;
    optional bool degraded = 8;
    optional uint64 anomaly_count = 9;
}

message GetContentSummaryResponse {
    optional ContentSummaryProto summary = 1;
}
```

全部字段使用 proto2 `optional`，保持向后兼容。文件结果不设置 generation/timestamp
字段，对应 model 的 `snapshot=None`。新增能力字符串
`get-content-summary-snapshot-v1`，滚动升级客户端可先通过能力判断；旧 master 上 CLI
直接走 live 回退。

handler 流程：

1. 校验并规范化绝对路径。
2. 在短时 `fs_dir` 读锁内解析 inode。
3. 文件直接构造 `LIVE_FILE` 结果。
4. 目录按 point → global 优先级查询。
5. 无完整 generation 或 snapshot miss 时返回 `SnapshotNotReady`；路径不存在沿用
   `FileNotFound`；功能关闭返回 `Unsupported`。

客户端在 `FsClient`、`CurvineFilesystem` 暴露 `get_content_summary(path)`；
`UnifiedFilesystem::try_get_content_summary_snapshot(path)` 返回
`FsResult<Option<ContentSummary>>`：`Some` 表示应使用 Curvine snapshot，cache-mode
完整 UFS 查询返回 `None`，由 CLI 调用现有 live helper。模型放在 `curvine-model`，
CLI 不再私有定义重复结构；live helper 构造 `source=LiveRecursive`、
`snapshot=None`。
`curvine-error` 新增 `ErrorKind::SnapshotNotReady = 37` 和对应 `FsError`，并覆盖
wire encode/decode；CLI 仅对 `Unsupported` 和 `SnapshotNotReady` 回退。

## 12. 可观测性

新增 master metrics：

| 指标 | 类型 | 标签/含义 |
| --- | --- | --- |
| `dir_space_global_ready` | Gauge | 有完整 global 快照为 1 |
| `dir_space_global_generation` | Gauge | 当前 global generation |
| `dir_space_global_age_seconds` | Gauge | 每次 coordinator tick 更新的快照年龄 |
| `dir_space_global_entries` | Gauge | global 目录聚合项数量 |
| `dir_space_points_configured` | Gauge | 当前有效 point 数 |
| `dir_space_points_ready` | Gauge | 已有完整 generation 的 point 数 |
| `dir_space_points_oldest_age_seconds` | Gauge | point 中最老快照年龄 |
| `dir_space_points_entries` | Gauge | 所有 point entries 总数 |
| `dir_space_scan_duration_ms` | HistogramVec | `scope`、`result` |
| `dir_space_scan_total` | CounterVec | `scope`、`result=success|failed|timeout|limit|role_lost|busy` |
| `dir_space_scan_inodes` | GaugeVec | 最近成功轮处理 inode 数 |
| `dir_space_scan_edges` | GaugeVec | 最近成功轮处理 edge 数 |
| `dir_space_scan_anomalies` | GaugeVec | 最近成功轮异常数 |

日志每轮只记录一条 summary；失败包含 scope、root/path、耗时、已处理数量与明确原因，
不得逐 inode 打日志。

## 13. 容量与性能

- 时间复杂度：全局 O(I + E + D)，I=inode、E=edge、D=可达目录。
- 内存复杂度：O(I + D + E_dir)。实现使用紧凑 fact，禁止保留完整 `InodeView`。
- `fill_cache(false)` 和 64 MiB readahead 避免污染 RocksDB block cache。
- RocksDB snapshot 会固定旧 SST，必须受对应 scope timeout 限制并在所有错误路径立即释放。
- 无 point 候选时查询进行一次路径解析；有 point 候选时在同一读锁内额外解析一次
  point root。随后只需一次 `Arc` clone 和 HashMap lookup。
- point 扫描按批次读取 inode，避免每个目录项一次独立 RPC；默认每批 1024 个 child id。
- 不允许同时执行 global 与 point 扫描，以限制磁盘和内存峰值。

上线前至少验证：

- 100 万 inode 合成数据的峰值 RSS、扫描时长与 RocksDB compaction 压力。
- 扫描期间 create/delete/rename/truncate 持续运行，metadata RPC P99 相对关闭功能的
  基线增幅不超过 10%。
- leader 切换期间无旧 leader 发布、无 master 启动失败。

## 14. 错误处理与降级

| 场景 | 行为 |
| --- | --- |
| 首轮尚未完成 | `SnapshotNotReady`；CLI 提示并 live 回退 |
| 扫描 I/O/反序列化失败 | 不发布，保留上一代，计数 `failed` |
| 超时/资源上限 | 不发布，保留上一代，计数对应原因 |
| 角色丢失 | 中止且不发布 |
| point 路径不存在/变为文件 | 保留旧 point 但不再用于查询，记录 warning |
| global 可用、point 不可用 | 返回 global |
| 当前目录不在旧 generation | `SnapshotNotReady`；CLI live 回退 |
| 快照含可忽略异常 | 发布 `degraded=true`，响应暴露 anomaly count |
| 功能关闭 | RPC 返回 `Unsupported`；CLI live 回退 |
| restore 请求到达 | 取消 scan lease；等待释放后 restore，超时则保持原 DB |

## 15. 测试策略

### 单元测试

- 聚合：空树、宽树、深树、硬链接、孤儿、环、self edge、溢出、顺序无关。
- store：完整 generation 原子替换、失败保留旧值、point/global 优先级。
- scan lease：restore 取消扫描、拒绝新 lease、超时不替换 DB、无强引用泄漏。
- 配置：默认值、非法 duration、零上限、相对 tracked path、重复路径。
- proto/model：所有字段转换和未知 source 的兼容处理。

### 存储集成测试

- 在临时 RocksDB 构造 inode/edge，验证全局扫描结果。
- 同一个 RocksDB snapshot 下并发 rename/truncate，结果必须属于单一读取视图。
- 注入缺失 edge/inode、负长度、损坏 inode。
- deadline、inode/dir/depth 上限和角色丢失均不发布半成品。

### Master/客户端测试

- scheduler 仅 active 扫描，切 standby 后中止。
- RPC 文件路径、目录路径、SnapshotNotReady、global 与 point source。
- CLI 默认 snapshot、旧 master/SnapshotNotReady 回退、`--live` 强制递归。
- cache-mode `du` 保持 UFS live；fs-mode 和 `count` 使用 Curvine snapshot。
- 现有 `du`/`count` 输出列兼容。

## 16. 发布顺序

1. 先合入默认关闭的 core、scanner 与指标。
2. 合入 RPC/client/CLI，旧 master 自动 live 回退。
3. 测试环境开启 global，观察扫描时间、RSS、SST pin 与 metadata RPC P99。
4. 开启 tracked points，验证 mount 和显式路径。
5. 根据规模测试调整上限后再在生产灰度开启。

任一阶段都可通过 `dir_space_enabled=false` 回退，不需要迁移或清理持久数据。

## 17. 可追溯性

- 基线：当前 `main-next`。
- 可复用原型：`b83fbd61 feat(master): add dir space aggregation core (#285)`。
- 原型归属：历史父任务 #224 / T224-2，仅用于追溯；后续应绑定新的 CoMind Issue ID。
- 原型代码不得直接整提交 cherry-pick；应按本设计重新引入并适配当前 RocksDB bulk scan、
  active-master 生命周期、proto 兼容规则和现有 CLI 回退。
