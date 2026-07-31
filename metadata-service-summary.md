# Curvine 元数据服务(MDS)总结文档

> 状态:评审中(Review) · 版本:v0.1 · 最后更新:2026-07-31
>
> 本文自顶向下组织:先厘清 **使用场景与客户端**(第 1 章),再定义 **CacheMode / FsMode 的产品能力**(第 2 章),然后基于这两者反推 **新的元数据服务、存储与 Schema 设计**(第 3 章),最后 **逐操作定义元数据交互**(第 4 章)。
>
> 第 1、2 章是对现状的事实性描述(附代码位置),用于统一认知;第 3、4 章是元数据服务的 **设计与演进方向**,供评审与迭代。

---

## 0. 概览

### 0.1 核心主线(Thesis)

本文的核心主线是:**元数据的"权威性 / 持久性等级"应当由挂载的 `WriteType` 决定**。

- **CacheMode**:UFS(S3/OSS/HDFS)是数据与元数据的 **权威**;Curvine 侧的元数据本质上是 UFS 元数据的 **可重建缓存(soft-state)**。丢失后可从 UFS 重新拉取,因此对 **强一致** 的要求较低,可以走"惰性填充 + 按 TTL / 内存压力淘汰单条"的路径。但"可重建"仅指 **正确性可回源**,后端 meta 存储 **仍需大容量 + 高可用**——一旦大面积丢失,请求会集中 fallback 至 UFS(S3),造成缓存击穿。
- **FsMode**:Curvine 本身是 **权威**,UFS 只是异步刷写的 **持久副本**。此时元数据是 **硬状态(hard-state)**,必须强一致、强持久(如 Raft journal 等强一致持久化),不可丢。

当前实现对两类子树 **一视同仁**:全部塞进同一棵内存树、同一把全局锁、同一份 Raft journal、同一个 RocksDB。这既让 cache-mode 路径背上了不必要的持久化 / 一致性开销,也让 fs-mode 路径与海量、易变的 cache 元数据争抢同一把锁。**按 WriteType 对元数据做分层治理**,是本设计的主线。

### 0.2 名词表

| 名词 | 含义 |
|---|---|
| **MDS / Master** | 元数据服务。当前是 `curvine-server` crate 内的 `master` 模块 |
| **Worker** | 数据节点,负责多级缓存(mem/SSD/HDD)块的读写 |
| **UFS** | Under File System,底层持久存储(S3 / OSS / GCS / HDFS) |
| **Mount** | 将一个 Curvine 路径 `cv_path` 绑定到一个 UFS 路径 `ufs_path` 的映射 |
| **WriteType** | 挂载的写入 / 权威模型,取值 `CacheMode` 或 `FsMode` |
| **AccessMode** | 挂载的读写权限,取值 `ReadOnly` / `ReadWrite`(与 WriteType 正交) |
| **inode** | 文件 / 目录的元数据节点 |
| **dentry / edge** | 目录项,`(parent_id, name) -> child_id` 的命名空间边 |
| **block** | 文件被切分成的定长数据块,由 Worker 缓存 |
| **journal** | 编辑日志,当前经 Raft 复制 |

### 0.3 代码位置索引(现状)

| 组件 | 路径 |
|---|---|
| Master 模块根 | `curvine-server/src/master/` |
| 内存树 + 变更逻辑 | `curvine-server/src/master/meta/fs_dir.rs` |
| inode 类型 | `curvine-server/src/master/meta/inode/` |
| RocksDB 持久化 | `curvine-server/src/master/meta/store/rocks_inode_store.rs` |
| Raft journal | `curvine-server/src/master/journal/` |
| 命名空间门面 | `curvine-server/src/master/fs/master_filesystem.rs` |
| RPC 分发 | `curvine-server/src/master/master_handler.rs` |
| WriteType / MountInfo | `crates/common/curvine-model/src/mount.rs` |
| 元数据 proto | `curvine-common/proto/master.proto` |
| RPC code 枚举 | `crates/common/curvine-fs-api/src/rpc_code.rs` |
| 统一 FS(UFS fallback) | `curvine-unified-fs/src/unified/unified_filesystem.rs` |

---

## 1. 使用场景与客户端

### 1.1 Curvine 定位

Curvine 是一个 **AI-Native / Cloud-Native 的文件系统**:在云对象存储之上叠加一层具备完整 POSIX 语义的分布式缓存,向上暴露文件语义,向下以对象存储作为持久层。

> "A high-performance POSIX file semantic layer built on top of cloud object storage, with an integrated multi-tier distributed cache, designed from the ground up for large-scale AI workloads and AI Agent platforms." — `README.md`

典型使用场景(`README.md` / `README_zh.md`):

| 场景 | 说明 | 对元数据的压力特征 |
|---|---|---|
| **AI Agent 平台存储** | K8s 上数万个有状态 Agent Pod,每个通过 CSI 获得隔离的 POSIX 工作区;开卷即 `mkdir`,毫秒级供给,无云 API 调用 | 海量小目录 / 小文件、极高并发的 `mkdir`/`create`/`stat`,租户隔离 |
| **LLM 训练加速** | 将训练集、checkpoint 缓存到 GPU 节点近端 | 大文件顺序读、周期性 checkpoint 写,`list`/`open` 密集 |
| **模型分发加速** | 多区域快速分发模型产物 | 读多写少,热点文件 `open`/`getBlockLocations` |
| **多模态数据湖访问加速** | 免拷贝地以 POSIX 访问对象存储数据湖 | 大量 `list`/`stat` 穿透 UFS,cache-mode 为主 |
| **OLAP 查询加速** | 为存算分离引擎提供热数据缓存 | 大文件读、分区目录 `list` |
| **大数据 Shuffle 加速** | Spark/Flink Shuffle 中间数据 | 大量临时文件 `create`/`delete`,fs-mode 为主 |
| **多云数据缓存** | 跨多云对象存储的统一缓存层 | 混合读写 |

**关键结论**:AI Agent 平台(数万 Pod / 数万 PVC)把元数据服务推向 **高并发小操作 + 海量 inode** 的工作负载;这正是当前"单一全局锁 + 全内存树"最吃紧的地方。

### 1.2 系统拓扑

```
                 ┌──────────────── Clients ─────────────────┐
   FUSE mount   Rust SDK   Java/Python SDK   CLI(cv)   CSI 驱动   LanceDB
      │            │            │              │          │         │
      │      (原生客户端均内嵌 curvine-client-core / curvine-unified-fs)  │
      ▼            ▼            ▼              ▼          ▼         ▼
   ── 元数据 RPC(RpcCode 2–34)──►   MASTER(MDS)  ── Raft 复制的元数据
                                        │  inode / block / mount 表
                                        │  返回 block 位置 / worker 地址
   ── 数据 RPC(RpcCode 80–83)──►   WORKER(s)  ── 多级缓存(mem→SSD→HDD)
                                        │
                                        ▼
                                   UFS(S3 / OSS / GCS / HDFS) ← 持久后端
```

- **Master**:元数据、Worker 协调、mount 表、Raft 共识。RPC 分发见 `master_handler.rs`。
- **Worker**:块缓存读写。数据面分发见 `worker/handler/block_handler.rs`。
- **UFS 绑定**:通过 `mount` 把 `cv_path` 绑定到 UFS URI(`mount.proto` 的 `MountInfoProto`)。
- **RPC 框架**:自研 `orpc`,按数字 `RpcCode` 分发(**非 gRPC**)。

### 1.3 客户端矩阵

所有原生客户端都建立在两层共享库之上:

- `curvine-client-core`:底层客户端。`FsClient`(`file/fs_client.rs`)是元数据 RPC 的权威封装;`CurvineFileSystem`(`file/curvine_filesystem.rs`)是高层易用 API;块 I/O 在 `block/`。
- `curvine-unified-fs`:在 `CurvineFileSystem` 之上增加 **透明 UFS fallback**(`unified/unified_filesystem.rs`、`fallback_fs_reader.rs`)。

| 客户端 | 载体 / 目录 | 与 MDS 的通信方式 | 说明 |
|---|---|---|---|
| **FUSE**(POSIX) | `curvine-fuse/` | 内嵌 `curvine-client-core` | 完整 POSIX,`git`/`inotify` 等工具可无改动运行 |
| **Rust SDK** | `curvine-client-core` + `curvine-client` + `curvine-unified-fs` | 直接 RPC | 全量 `FsClient` 能力面 |
| **Java SDK**(Hadoop FS) | `curvine-libsdk/java/`(JNI → `crates/sdk/curvine-libsdk-java/`) | JNI → Rust core → RPC | `cv://` scheme,实现 Hadoop `FileSystem`,即 HDFS 兼容接口,供 Spark/Flink/Hive 使用 |
| **Python SDK** | `crates/sdk/curvine-libsdk-python/`(PyO3) | PyO3 → Rust core → RPC | fsspec 兼容(`curvinefs`) |
| **CLI(`cv`)** | `curvine-cli/` | `curvine-unified-fs` → RPC | `cv fs` 是 HDFS 风格 shell;`cv mount`/`load`/`transfer` 管理挂载与数据搬运 |
| **CSI 驱动**(K8s) | `curvine-csi/`(Go) | **不直接 RPC**,而是拉起 `curvine-fuse` 子进程挂载 | 供给 PVC = 在 Curvine 上 `mkdir`;支持 embedded / standalone(MountPod)两种模式 |
| **S3aProxy** | `curvine-libsdk/java/.../S3aProxyFileSystem.java` | 读时查 mount 表,命中则走 Curvine 缓存 | Hadoop `S3AFileSystem` 子类,S3 读加速 shim(非 S3 服务端) |
| **LanceDB** | `curvine-lancedb/` | `curvine://` URI → object store | AI / 向量数据,免 FUSE |

**两点需要在设计中明确的边界**:

1. **不存在 S3 服务端网关**:`[s3_gateway]` / `enable_s3_gateway` 仅出现在某个 SPDK 部署 toml 中,仓库内 **无 Rust 实现**。当前 Curvine 的 S3 能力是:(a) S3 作为 UFS 后端(OpenDAL);(b) 客户端侧 `S3aProxyFileSystem` 读穿透加速。设计文档不应把"入站 S3 API"当作已交付特性。
2. **"HDFS 接口" = Hadoop `FileSystem` 客户端兼容**(`cv://` scheme),不是 NameNode 协议服务端;HDFS 也可作为 UFS 后端。

### 1.4 各客户端的元数据操作面

MDS 需要服务的元数据操作(master RPC)按客户端拆分如下,反映 MDS 的接口 surface area:

| 客户端 | mkdir | create | open | list | delete | rename | setattr | mount | 备注 |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|---|
| FUSE | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅(chmod/chown/utimes) | 经 cv | 全 POSIX |
| Rust SDK | ✅ | ✅ | ✅ | ✅(+stream/options) | ✅(+free) | ✅ | ✅(+symlink/link/resize/locks) | ✅ | 全量 |
| Java SDK | ✅ mkdirs | ✅ +append | ✅ | ✅ listStatus | ✅ | ✅ | ✅ setAttr/Owner/Perm/Times | getMountInfo | HDFS 兼容 |
| Python SDK | ✅ | ✅ +append | ✅ | ✅ | ✅ | ✅ | (via getstatus) | — | fsspec |
| CLI `cv fs` | ✅ | ✅ put/touch | ✅ cat/get | ✅ ls/du/count | ✅ rm/free | ✅ mv | ✅ chmod/chown | mount/umount | 管理 + shell |
| CSI | ✅(供给=mkdir) | — | — | — | ✅(删 PVC 目录) | — | — | — | 委托给 FUSE |
| S3aProxy | — | — | ✅(读穿透) | — | — | — | — | 读 mount 表 | S3 读加速 |
| LanceDB | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | — | `curvine://` |

完整请求 / 响应 schema 见 `curvine-common/proto/master.proto`。

RPC code 分区(`crates/common/curvine-fs-api/src/rpc_code.rs`):

- **元数据(2–34)**:`Mkdir, Delete, CreateFile, OpenFile, AppendFile, FileStatus, ListStatus, Exists, Rename, AddBlock, CompleteFile, GetBlockLocations, GetMasterInfo, SetAttr, Symlink, Link, ResizeFile, AssignWorker, GetLock/SetLock/ListLock, CreateFilesBatch, AddBlocksBatch, CompleteFilesBatch, Free, ListOptions, GetCvMetadataSnapshotPage, GetCvMetadataDeltaPage, Mount, UnMount, UpdateMount, GetMountTable, GetMountInfo`
- **Job / transfer(35–54)**:`SubmitJob, GetJobStatus, CancelJob, SubmitTransfer …`(数据预取 / 迁移)
- **数据面(80–83,Worker)**:`WriteBlock, ReadBlock, WriteBlocksBatch, WriteCommitsBatch`

### 1.5 场景 → 元数据负载特征小结

| 维度 | AI Agent(CSI/FUSE) | 训练 / 推理(SDK/FUSE) | 数据湖 / OLAP(SDK) | Shuffle(SDK) |
|---|---|---|---|---|
| 主导 WriteType | FsMode(隔离工作区) | 混合 | CacheMode(读穿透) | FsMode(临时文件) |
| inode 规模 | 极大(数万 Pod × 目录) | 中 | 大(镜像 UFS 命名空间) | 中,生命周期短 |
| 热点操作 | mkdir/create/stat/unlink | open/list/getBlockLocations | list/stat/open | create/delete |
| 一致性要求 | 强(POSIX) | 强 | 弱(UFS 权威,可容忍缓存滞后) | 强 |
| 元数据可重建性 | 否 | 否 | **是(可从 UFS 重建)** | 否 |

> 这张表直接支撑第 3 章的分层设计:**CacheMode / 数据湖类负载的元数据可重建、可淘汰**,不必与 fs-mode 硬状态共用同一套持久化与锁。

---

## 2. CacheMode 与 FsMode 产品能力

### 2.1 WriteType 定义

`CacheMode` 与 `FsMode` 是同一个枚举 `WriteType` 的两个取值(**不是两个独立类型**):

```rust
// crates/common/curvine-model/src/mount.rs:481
#[repr(i32)]
pub enum WriteType {
    #[default]
    CacheMode = 0,   // "cache_mode"
    FsMode   = 1,    // "fs_mode"
}
```

proto 镜像:`common.proto` 的 `WriteTypeProto { CACHE_MODE=0, FS_MODE=1 }`,作为 `MountInfoProto.write_type` 字段。

**每个 mount 恰好是其中之一**,它决定了"谁是权威、谁是副本",并反转读 / 写 / fallback / TTL 的全部逻辑。权威语义(以 `fallback_fs_reader.rs:29-55` 的模块文档为准):

- **FsMode** — Curvine 是权威,UFS 是刷写副本。
- **CacheMode** — UFS/S3 是权威,Curvine 是读穿透缓存。

### 2.2 CacheMode 语义(UFS 权威)

UFS(S3/OSS/HDFS)是数据与元数据的权威来源,Curvine 是其上的读穿透 / 缓存层。

> **核心原则 —— CacheMode 下 Curvine 能力 ⊆ UFS 能力。**
>
> 在 CacheMode 下,Curvine 只是 UFS 的 **从属缓存层**。无论是 **数据** 还是 **元数据**,Curvine 对外暴露的功能、语义与一致性保证都 **不得超出 UFS 本身所能提供的上界**。凡 UFS 不具备的语义,Curvine 也不得凭空提供;凡 UFS 提供的语义,Curvine 至多是"更快地"复现它,而不改变其含义。
>
> 这是下表所有"拒绝 / no-op / 透传"行为的 **根本原因,而非实现遗漏**:
> - 对象存储(S3/OSS)没有 `symlink`/`hardlink`/`resize`/POSIX 文件锁/`mknod` 等语义 → CacheMode 一律返回 unsupported;
> - 对象存储没有可变属性(`chmod`/`chown`/`utimes`)与真正的目录 inode → CacheMode 的 `set_attr` 只能 no-op,`mkdir` 仅透传 UFS;
> - 对象存储没有原子 `rename`、没有强一致的目录列举 → Curvine 不得对外承诺比 UFS 更强的原子性 / 一致性(如 `list_status` 直接回源 UFS,不用可能更"新"的本地缓存冒充权威)。
>
> **推论(对第 3 章设计的约束)**:CacheMode 子树的元数据 Schema 只需覆盖"UFS 能表达的那部分"(对象 key/大小/mtime/etag 的投影),无需为其保留 FsMode 才需要的富 POSIX 字段(nlink、符号链接目标、锁表、精确 ctime 等);对这部分元数据也不应提供强于 UFS 的一致性 / 持久性承诺。

| 维度 | 行为 | 代码位置 |
|---|---|---|
| **读(正常路径)** | 先校验缓存新鲜度(`check_cache_validity`);缓存失效则读 UFS。`len()` / `status()` 取自 **Curvine 缓存的元数据**(与 FsMode 相同) | `unified_filesystem.rs:422-438`、`fallback_fs_reader.rs:122-137` |
| **读(Worker 故障降级)** | Worker 故障后 fallback 直接读 S3、**不做一致性校验**;此时(且仅 CacheMode)`len()` 切换为报告 **S3 实时长度**,以便 `read_as_string` 等按 len 读的调用者看到当前对象大小而非过期缓存长度 | `fallback_fs_reader.rs:130-139,194-209` |
| **元数据读(get_status)** | 缓存有效时返回 Curvine(叠加 UFS 字段);缓存失效 / 出错时回源 UFS | `unified_filesystem.rs:1046-1073` |
| **readdir(list_status / list_options / list_stream)** | **无条件** 直接委托 UFS——始终返回 UFS 的实时目录视图,**不查 Curvine 缓存、不合并本地缓存项、不以可能更新的本地视图冒充权威**;各变体(含 `*_bytes` / `stream`)行为一致。契合 `⊆ UFS` 原则:目录结构以 UFS 为准 | `unified_filesystem.rs:1075-1125` |
| **写(open)** | 删除 Curvine 缓存项后直接写 UFS(`create` / `append`) | `unified_filesystem.rs:670-684` |
| **mkdir** | 仅在 UFS 上 `mkdir`,**不触碰 Curvine 缓存**;目录已存在报 `file_exists` | `unified_filesystem.rs:706-713` |
| **delete** | 先删 UFS 再删 Curvine 缓存;**拒绝删除挂载点根** | `unified_filesystem.rs:1017-1044` |
| **rename** | 先 `rename` UFS 再删源缓存;**不支持 rename flags** | `unified_filesystem.rs:757-787` |
| **TTL 默认** | `TtlAction::Delete`(淘汰即丢弃 Curvine 副本,UFS 不动) | `mount.rs:330-333` |
| **free** | 丢弃 Curvine 的元数据 / 块(`cv.delete`),UFS 完好;根路径 recursive 时逐个删子项而不删根 | `unified_filesystem.rs:271-303` |
| **set_attr(chmod/chown/utimes)** | cache-mode 下 **静默 no-op**,返回 `None`——不报错也不落 UFS | `unified_filesystem.rs:719-735` |
| **不支持的 POSIX 操作** | `symlink` / `link` / `resize` / `get_lock` / `set_lock` / `mknod` 在 cache-mode 上直接返回 **unsupported 错误**(与 set_attr 的 no-op 不同) | `unified_filesystem.rs:305-368,737-755` |

**元数据视角(小结,展开见 2.7)**:由于 `readdir` / `rename` / `hardlink` / `symlink` / `resize` / 锁 等操作要么不支持、要么直接 fallback 至 UFS(见上表),它们 **根本不经过 CacheMode 的元数据服务**。因此 CacheMode 侧需要维护的仅是 **目录 / 文件的属性元数据**(UFS 属性的投影,如 size/mtime/mode/owner),而非完整目录树、父子边、锁表等重型结构——这部分元数据是 **可从 UFS 重建的软状态(soft-state)**。

### 2.3 FsMode 语义(Curvine 权威)

Curvine 是真正的分布式文件系统权威,UFS 是异步刷写的持久副本。

> **核心定位 —— FsMode 就是一个"完整、持久、全 POSIX 语义"的分布式文件系统。**
>
> 与 CacheMode 的"从属缓存层"截然不同,FsMode 下 Curvine **不再受 UFS 能力上界约束**,而是自己 **定义并保证** 完整的文件系统语义:
>
> - **全 POSIX 语义**:支持目录树、`symlink` / `hardlink`(含 nlink 引用计数)、原子 `rename`(含跨目录)、`resize` / truncate、POSIX 文件锁、完整可变属性(`chmod` / `chown` / `utimes`,精确 atime/mtime/ctime)等——这些 **不依赖** 底层 UFS 是否具备,由 Curvine 元数据服务自身实现。
> - **元数据完整**:维护完整的命名空间(inode + 父子边 dentry)、块布局(block → worker 位置)、xattr、ACL、锁表等,是自洽的权威元数据,而非某个外部系统的投影。
> - **强持久**:元数据是 **不可丢失的硬状态**,必须经过 journal / 复制持久化;UFS 只是数据面的异步导出副本,用于容灾与冷读兜底,**元数据的权威始终在 Curvine 侧**。
>
> **推论(对第 3 章设计的约束)**:FsMode 子树的元数据 Schema 必须是 **完整的 POSIX 元数据模型**,并要求 **强一致 + 强持久 + 跨目录事务**;这决定了它需要一个权威、可信、支持事务的元数据存储,不能像 CacheMode 那样简化或弃置。

| 维度 | 行为 | 代码位置 |
|---|---|---|
| **读** | 读 Curvine 块;仅当 UFS 副本通过快照一致性校验(`ufs_mtime`/`len` 匹配)时才 fallback 到 UFS | `unified_filesystem.rs:408-421`、`fallback_fs_reader.rs:80-92` |
| **写** | 写 Curvine;若文件无缓存数据且非覆盖写,先 UFS→CV 拷贝(`copy_ufs_file`)再写 Curvine | `unified_filesystem.rs:649-668,586-620` |
| **元数据读** | 始终由 Curvine 提供 | `unified_filesystem.rs:1051` |
| **TTL 默认** | `TtlAction::Free`(淘汰块但保留元数据,数据仍可从 UFS 恢复) | `mount.rs` |
| **异步导出 UFS** | journal 回放时把 Curvine→UFS 导出 | `master/journal/ufs_loader.rs:56-90` |
| **resync** | 仅 fs-mode 允许,UFS→Curvine 元数据对账 | `curvine-cli/src/cmds/mount.rs:359-371` |

**元数据视角(小结,展开见 2.7)**:fs-mode 路径下的 inode / dentry / block 是 **权威硬状态(hard-state)**,是自洽的完整 POSIX 元数据模型,必须强一致、强持久,丢失即数据丢失。

### 2.4 正交维度

除 `WriteType` 外,`MountInfo`(`mount.rs:103-119`)还有几个 **正交** 的能力开关:

| 维度 | 取值 | 语义 | 生效范围 |
|---|---|---|---|
| **AccessMode** | `ReadOnly`(默认) / `ReadWrite` | 只读挂载:`is_read_only_cache_mode()` 且是写 RPC 时拒绝 | 仅对 cache-mode 强制(`unified_filesystem.rs:162-190`) |
| **auto_cache** | bool | 缺失 / 失效时是否自动提交缓存 job(需 `ttl_ms>0`) | `mount.rs:184-186` |
| **read_verify_ufs** | bool | 读时按 UFS mtime/len 校验缓存 | `mount.rs:111` |
| **ttl_ms / ttl_action** | i64 / `Delete`\|`Free`\|`None` | 淘汰时机与动作 | 见 2.2/2.3 默认值 |
| **storage_type / replicas / block_size** | 可选 | 块的存储介质、副本数、块大小 | 影响块放置 |
| **provider** | `Auto`/`OssHdfs`/`Opendal` | UFS 后端类型 | `mount.rs:40-60` |

> **产品能力组合示例**:`CacheMode + ReadOnly` = 只读的 S3/数据湖加速视图;`CacheMode + ReadWrite` = 写透传的 S3 加速;`FsMode` = 以 Curvine 为主、UFS 兜底持久的 POSIX 文件系统(Agent 工作区、Shuffle)。

### 2.5 Curvine-native(无 mount)—— FsMode 的子集

若某路径 **不属于任何 mount**(`get_mount` 返回 `None`),请求直达 `self.cv`,完全不涉及 UFS——即 **纯 Curvine 文件系统**,无外部存储。

**Curvine-native 不是独立的第三类,而是 FsMode 的一个退化子集**:FsMode 的完整定位是"Curvine 权威 + UFS 异步副本",若把其中的"UFS 异步副本"关闭(不 mount、不导出),剩下的就是 Curvine-native。

从 **元数据服务视角**,Curvine-native 与 FsMode **完全一致**:都是以 Curvine 为唯一权威、完整 POSIX 语义、强一致 + 强持久的 **硬状态**,元数据模型、Schema、一致性 / 持久性要求没有任何差别。二者唯一的区别在 **数据面**:是否存在一份异步导出到 UFS 的持久副本(用于容灾与冷读兜底)。

> **对第 3 章设计的意义**:元数据服务只需区分 **软状态(CacheMode)** 与 **硬状态(FsMode)** 两类即可;Curvine-native 归入 FsMode 的元数据路径,无需为其单独设计。是否导出 UFS 是一个 **数据面开关**,不改变元数据的组织方式。

### 2.6 产品能力矩阵(汇总)

> 下表把 Curvine-native 单列一列,仅为对比"有无 UFS 副本";在元数据设计上它与 FsMode 同属一类(见 2.5)。

| 能力 | CacheMode(RO) | CacheMode(RW) | FsMode | FsMode·无 UFS(native) |
|---|:-:|:-:|:-:|:-:|
| 权威方 | UFS | UFS | Curvine | Curvine |
| 元数据可从 UFS 重建 | ✅ | ✅ | 部分(resync) | ❌ |
| 需要强持久元数据(Raft) | 弱 | 弱 | ✅ | ✅ |
| 写透传 UFS | — | ✅ | ❌(异步导出) | ❌ |
| POSIX 全语义(symlink/lock/resize) | ❌ | ❌ | ✅ | ✅ |
| 淘汰默认动作 | Delete | Delete | Free | Free/None |
| 典型场景 | AI 训练 / 数据湖 / 模型分发(只读) | S3 写加速 | Agent 工作区 / Shuffle | 临时命名空间 |

### 2.7 软状态 vs 硬状态:元数据分层的本质依据

综合 2.2–2.6,两类挂载在 **元数据侧** 呈现出本质不同的两种状态形态。这是第 3 章分层设计的直接依据:

| 维度 | 软状态(CacheMode) | 硬状态(FsMode / native) |
|---|---|---|
| 权威方 | UFS | Curvine |
| 元数据内容 | 目录 / 文件 **属性** 的 UFS 投影 | 完整 POSIX 模型(命名空间树 + block 布局 + xattr / ACL / 锁) |
| 经过 MDS 的操作 | 仅属性读缓存;`readdir`/`rename`/`link`/`lock` 等绕过(fallback UFS 或不支持) | 全部元数据操作 |
| 可重建性 | 可从 UFS **完全重建** | 不可(UFS 无完整元数据,仅数据面可恢复) |
| 一致性要求 | 弱(不承诺强于 UFS,可容忍缓存滞后) | 强一致 + 跨目录事务 |
| 持久性 / 可用性要求 | 正确性可回源(可重建),但后端仍需 **大容量 + 高可用**(否则缓存击穿 UFS) | 强持久(不可丢),高可用 |
| 后端存储 | 大容量、高可用的 **属性缓存 KV**;写不经过共识、无跨目录 / 多键事务,但单条目需 **并发安全的原子点写**(多 MDS 并行) | 权威、可信、支持事务(Raft 或强一致 KV) |
| 计算层 | 可无状态 | 可无状态(状态外置),但存储层须强一致 |
| 弹性扩缩容 | 天然,任意路由、无迁移成本 | 受权威存储的一致性约束 |
| 存储 / 性能开销 | 低(无目录树 / 边索引 / 全局锁 / 事务) | 高(需命名空间树、事务、锁) |

由此得到三条关键设计判断:

1. **CacheMode 元数据可极致简化**:它绕过 MDS 的重型语义(目录树、跨目录事务、锁),只存属性投影,因此在 **存储空间**(无需常驻目录树 / 边索引)与 **性能**(无需全局命名空间锁、无需目录事务)上都能显著更高效。

2. **无状态与弹性扩缩容并非某一方专属**:CacheMode 因状态可从 UFS 重建,天然可做成无状态、任意路由、即时伸缩;FsMode 的计算层同样可通过把权威状态外置到强一致 KV 而无状态化。**二者真正的区别不在"计算层是否有状态",而在权威状态本身的一致性要求**——CacheMode 的后端 KV 写不经过共识、无需跨节点强一致与跨目录 / 多键分布式事务(可容忍滞后),但因 **多个 MDS 并行操作同一 KV**,单条目仍需 **并发安全的原子点写**;它也 **并非可随意丢弃的软缓存**:仍需大容量 + 高可用来避免缓存击穿 UFS;FsMode 的权威状态则必须强一致强持久,共识 / 事务的开销只是从计算层 **转移到存储层**,并未消失。

3. **两类元数据应在架构上物理分离**:用轻量、易弹性伸缩(但仍需大容量 + 高可用)的 **属性缓存 KV** 承载 CacheMode;用强一致 / 强持久、支持跨目录事务的 **权威元数据存储** 承载 FsMode / native。避免当前实现那样让二者共用同一棵内存树、同一把全局锁、同一份 journal。

### 2.8 从产品能力反推:对元数据服务的要求

| 产品能力 | 对 MDS 的要求 |
|---|---|
| CacheMode 元数据可重建 | 本质是一个 **属性缓存 KV**:只需按 key 的点操作 `Get` / `Put` / `Delete`,无目录树、无 `list`、无跨目录 / 多键事务、无写路径共识;条目可懒填充、按 TTL / 内存压力单条淘汰。两点例外:(1) 多 MDS 并行写同一 KV,单条目仍需 **原子点写**(CAS / 版本号);(2) 后端 KV 仍需 **大容量 + 高可用**——"可重建"只保证正确性可回源,大面积失效会缓存击穿 UFS |
| FsMode / native 强一致强持久 | MDS 是权威,需维护 **完整命名空间(目录树 + 块布局)**,提供全套 POSIX 元数据操作(`create` / `mkdir` / `list` / `rename` / `link` / `lock` …);元数据不可丢、跨目录改动要么全成要么全不成 |
| AI Agent 数万 PVC 高并发 | 打破全局单锁,支持 **按子树 / 分片 的并发**;`mkdir` 供给必须是毫秒级、无跨节点争用 |
| 海量 inode | inode 总量可能远超内存容量,不能要求 **全部常驻内存**;需分层——热数据在内存、其余落后端存储,按需换入。冷热怎么划分可按 WriteType 区别对待 |
| 只读挂载 / 多租户隔离 | MDS 需在挂载 / 子树粒度上承载 AccessMode、配额、租户策略 |
| S3aProxy / list 穿透 | `list_status` / `get_status` 需能高效回源 UFS 并缓存结果 |
| 大命名空间快速 failover | 主节点宕机、备节点接管后,必须先把元数据装载好才能对外服务。若命名空间有上亿 inode,而接管方要 **把整棵树从头重建一遍** 才能开工,这段时间整个文件系统不可用,可能长达数分钟。因此恢复不应依赖"全量重建",而要能 **增量 / 按需加载**,让服务尽快恢复 |

> 第 3 章据此提出"**按 WriteType 分层的元数据服务**"设计。

## 3. 新元数据服务设计

第 1、2 章确立了主线:元数据按 WriteType 分为 **软状态(CacheMode)** 与 **硬状态(FsMode / native)** 两类,二者对一致性、持久性、并发的要求截然不同。本章据此提出新的元数据服务设计,分四块展开:

1. **无状态 MDS + 弹性扩缩容**(3.2)——计算与存储分离,MDS 自身不持有权威状态。
2. **可插拔的 KV 存储后端**(3.3)——抽象存储接口,不绑死 RocksDB。
3. **按 WriteType 拆分存储**(3.4)——软 / 硬状态落到不同 KV DB 或同一 KV 的不同 Namespace。
4. **分 WriteType 的元数据 Schema 与能力**(3.5)——两类各自定义最小够用的 Schema 与操作面。

### 3.0 现状与要解决的问题

当前实现(代码位置见 0.3)把两类元数据 **揉在一起**:

| 现状 | 位置 | 问题 |
|---|---|---|
| 单棵全内存目录树 + 一把全局 `RwLock` | `fs_dir.rs` | 所有请求(含海量 cache 元数据)争抢同一把锁;树必须全量常驻内存 |
| 单个 RocksDB(WAL 关闭,靠 Raft journal 持久化) | `rocks_inode_store.rs` | 软 / 硬状态共用一份存储、一份 journal;`InodeStore` 注释明言"仅支持 RocksDB" |
| `WriteBatch` 非事务(单 DB 内的批量原子) | `rocks_inode_store.rs:266` | 跨目录事务能力弱;且与存储引擎强绑定 |
| 恢复需 `create_tree` 全量重建内存树 | `inode_store.rs:513` | 大命名空间 failover 慢(见 2.8) |

新设计要达成的目标:**软状态走轻量、可弹性伸缩的属性缓存;硬状态走强一致、可事务的权威存储;两者物理隔离、各用其所,且存储后端可插拔。**

### 3.1 总体架构:计算与存储分离

```
        Clients (FUSE / SDK / CLI / CSI …)
                     │  元数据 RPC(RpcCode 2–34)
                     ▼
   ┌──────────────────────────────────────────────┐
   │   MDS 集群(无状态,可水平扩缩容)              │
   │   ┌────────────┐  ┌────────────┐  ┌─────────┐ │
   │   │  MDS-1     │  │  MDS-2     │  │  MDS-n  │ │  ← 任意实例可服务任意请求
   │   └────────────┘  └────────────┘  └─────────┘ │
   │        按 mount 的 WriteType 路由到不同后端    │
   └───────────┬───────────────────────┬──────────┘
               │                       │
   软状态(CacheMode)            硬状态(FsMode / native)
               ▼                       ▼
   ┌────────────────────┐   ┌──────────────────────────┐
   │  属性缓存 KV        │   │  权威元数据 KV(强一致)   │
   │  (大容量 + 高可用)  │   │  (事务 + 持久 + 快照)     │
   └────────────────────┘   └──────────────────────────┘
        (可插拔后端:RocksDB / 分布式 KV / …)
```

核心变化:**MDS 从"内嵌全内存树 + 本地 RocksDB 的有状态单点"转为"无状态计算层 + 外置 KV 存储层"**。权威状态下沉到存储层,MDS 实例本身可增删、可路由、可失败重启而不丢数据。

### 3.2 无状态 MDS 与弹性扩缩容

**"无状态"的准确含义**:MDS 实例内存中不保存 **唯一、不可重建** 的权威状态;它对外的每个操作,其正确性都由外置 KV 存储保证,而非由某个实例的内存决定。

- **权威状态外置**:目录树、inode、边、块布局全部以 KV 为准。MDS 内存里至多是 **可重建的缓存**(热 inode、路径解析缓存),丢失只影响性能不影响正确性。
- **任意路由**:任一 MDS 实例都能服务任一请求;客户端可经 LB / 一致性哈希路由,不需要"这个子树只能由某台服务"。
- **弹性扩缩容**:加减实例不涉及数据搬迁——扩容即加计算、缩容即减计算,权威数据始终在 KV 层。
- **快速 failover**:实例宕机后,新实例 **无需全量重建内存树**(对比现状 `create_tree`),按需从 KV 拉取路径上的 inode 即可开始服务(呼应 2.8 的 failover 要求)。

> **两类都能无状态,但代价不同(呼应 2.7 判断 #2)**:CacheMode 天然无状态(状态可从 UFS 重建);FsMode 通过把权威状态外置到 **强一致 KV** 同样能无状态化——共识 / 事务的开销从计算层转移到了存储层,并未消失。因此 3.2 的无状态模型对两类都适用,区别只在 3.4 选择的后端。
>
> **并发正确性由存储层承担**:多个 MDS 并行改同一份状态时——CacheMode 靠单条目原子点写(CAS / 版本号),FsMode 靠存储层的事务与冲突检测。MDS 计算层不再需要那把全局锁。

### 3.3 可插拔的 KV 存储后端

现状 `InodeStore` 直接依赖 `RocksInodeStore`,并注释"仅支持 RocksDB"。新设计把存储抽象成 **一组接口(trait)**,RocksDB 只是其中一个实现:

**软状态后端接口(属性缓存 KV)** —— 只需点操作:

| 能力 | 说明 |
|---|---|
| `get(key)` / `put(key, val)` / `delete(key)` | 按 key 点读写。key 用 **`hash(parent_dir, name)`**(对路径身份取哈希),把条目 **均匀打散** 到整个 keyspace,**消除热点**——热目录、连续命名、公共路径前缀都不会把负载压到同一分片 / range。之所以能放心全量哈希,是因为 CacheMode 不做 `list` / readdir(无范围扫描),不需要保留路径前缀顺序 |
| 原子点写(CAS / 版本号) | 多 MDS 并行写同一条目时的并发安全 |
| TTL / 淘汰 | 按 TTL 或容量压力单条淘汰 |
| 高可用 + 大容量 | 避免大面积失效击穿 UFS |

**硬状态后端接口(权威元数据 KV)** —— 需要范围扫描与事务:

| 能力 | 说明 |
|---|---|
| 点读写 + 前缀 / 范围扫描 | 解析路径、`list` 目录(扫 edges 前缀) |
| **多键事务** | `rename` / `mkdir -p` / `unlink` 等跨键改动要原子 |
| 强一致 + 强持久 | 元数据不可丢 |
| 快照 / 增量恢复 | 支撑快速 failover |

**候选实现**:
- **RocksDB(嵌入式)**:保留现状单机 / 小规模路径;硬状态可配合 Raft journal(如今)。
- **分布式事务 KV(TiKV / FoundationDB 等)**:天然多键事务 + 强一致 + 水平扩展,最契合"无状态 MDS + 外置权威状态"。
- **软状态专用**:可用高可用 KV / 内存网格(Redis Cluster 等)承载属性缓存,与硬状态后端解耦选型。

> 抽象的价值:软 / 硬状态可 **各选最合适的后端**,而不是像现在被 RocksDB 一种实现绑死;也让 3.4 的"拆分存储"成为可能。

### 3.4 按 WriteType 拆分存储

同一个 Curvine 命名空间里,**CacheMode 与 FsMode 的挂载是并存的**(甚至嵌套):根 / native 子树是 FsMode 硬状态,其下可同时挂载若干 CacheMode(S3 加速)子树与 FsMode 子树。单个 mount 的 `write_type` 虽只取二者之一(`mount.rs:481`),但整个系统 **同时承载两类**,一次路径解析甚至可能跨越 cache-mode 与 fs-mode 子树。

因此 MDS 在解析路径、命中所属 mount 后,按该 mount 的 `write_type` 把元数据操作 **路由到对应后端**:

- **CacheMode → 软状态后端**(属性缓存 KV)
- **FsMode / native → 硬状态后端**(权威元数据 KV)

物理隔离有两种粒度,可按部署规模选择:

| 方案 | 做法 | 适用 |
|---|---|---|
| **不同 KV DB 实例** | 软 / 硬状态各接一套独立的 KV 集群 | 大规模;两类负载与容量需独立伸缩、独立选型 |
| **同一 KV 的不同 Namespace / keyspace** | 一套 KV,用前缀 / CF / namespace 隔离两类 | 中小规模;运维简单,仍逻辑隔离 |

无论哪种粒度,目标一致:**让海量、易变、可重建的 cache 元数据不再与权威硬状态争抢同一把锁、同一份 journal、同一份存储**(直接消除 3.0 的核心问题)。

> 命名空间隔离的额外收益:天然承载 **多租户 / 只读挂载 / 配额**——不同 mount(乃至不同租户)可落到不同 namespace,AccessMode、quota、TTL 策略按 namespace 施加(呼应 2.8)。

### 3.5 分 WriteType 的元数据 Schema 与能力

两类后端的 Schema 与操作面 **各自最小够用**,不再共用一套重型模型。

#### 3.5.1 软状态 Schema(CacheMode:属性缓存)

- **Key**:**`hash(parent_dir, name)`**——对路径身份取哈希,把条目均匀打散、**消除热点**(热目录 / 连续命名不再集中到同一分片)。可行前提:CacheMode 无 `list` / readdir,无需保留前缀顺序。
- **Value**:UFS 属性的投影——`size / mtime / mode / owner / etag` 等;**不含** 父子边、nlink、符号链接目标、锁表(2.2 已论证这些操作在 CacheMode 下不经过 MDS)。
- **操作面**:仅 `Get` / `Put` / `Delete` 点操作 + 原子点写;**无** `list`(readdir 回源 UFS)、**无** 目录树、**无** 跨键事务。
- **生命周期**:懒填充、按 TTL / 内存压力单条淘汰。

#### 3.5.2 硬状态 Schema(FsMode / native:完整 POSIX)

以现状 CF 布局为起点(`rocks_inode_store.rs`),它本就是 FsMode 需要的模型,继续沿用并要求事务化:

| 数据 | Key → Value | 用途 |
|---|---|---|
| **inode** | `inode_id` → inode 属性(mode/owner/times/nlink/size…) | 文件 / 目录节点 |
| **edge / dentry** | `(parent_id, name)` → `child_id` | 命名空间边,支撑路径解析与 `list`(前缀扫描) |
| **block** | `(block_id, worker_id)` → `BlockLocation` | 块布局 |
| **location** | `(worker_id, block_id)` → `block_id` | 按 worker 反查块(worker 下线清理) |
| **xattr / ACL / lock** | `inode_id` → … | 扩展属性、锁表等 POSIX 富语义 |

- **操作面**:全套 POSIX——`create` / `mkdir` / `list` / `rename` / `link` / `symlink` / `resize` / `lock` 等。
- **一致性**:跨键 / 跨目录改动(`rename`、`mkdir -p`、`unlink` 递归)必须在 **一个事务** 内原子完成——这正是现状 `WriteBatch` 非事务的短板所在(3.0)。
- **持久 / 恢复**:强持久,支持快照 + 增量恢复以快速 failover。

> **协议含义:必须改为 inode 寻址,不能再拿 full path 请求。** 若客户端每次都用完整路径 `/a/b/c/d` 请求,MDS 就得逐层解析——`root→a→b→c→d`,每层一次 `(parent_id, name)→child_id` 边查。在 **外置 KV** 下这是 O(depth) 次网络往返,直接把点查退化成遍历,前面"无状态 + 外置 KV"的收益被路径解析吃掉。
>
> 因此 fs-mode 元数据协议应 **以 inode 句柄寻址**(类似 FUSE lowlevel / NFS filehandle):
> - `lookup(parent_id, name)`:只带 **父 inode_id + name**,MDS 单次边点查即返回子 `inode_id`;
> - `stat` / `setattr` / `open` / `read`:基于已持有的 `inode_id` 直接点查 inode;
> - `create` / `mkdir` / `unlink`:带 **父 inode_id + name**,不带完整路径。
>
> 客户端(FUSE 本就按 `(parent_ino, name)` 工作)缓存历次 `lookup` 得到的 `inode_id` 并复用。这样路径解析的 O(depth) 成本从"每次请求都付"降为"仅首次逐层 `lookup`、之后靠句柄缓存复用",每个操作退化为 **1 次边或 inode 点查**,才真正发挥外置 KV 的点查能力。

#### 3.5.3 能力对照

| 维度 | 软状态(CacheMode) | 硬状态(FsMode / native) |
|---|---|---|
| Key 空间 | `hash(parent_dir, name)`(打散消热点) | inode_id + (parent_id, name) 边 |
| 存的内容 | UFS 属性投影 | 完整 POSIX inode + 边 + 块 + xattr/锁 |
| 操作面 | `Get`/`Put`/`Delete` 点操作 | 全套 POSIX 元数据操作 |
| `list` | 回源 UFS,不经 MDS | 扫 edges 前缀 |
| 事务 | 无(仅单条目原子) | 多键 / 跨目录事务 |
| 一致性 / 持久 | 弱一致、可重建;后端需高可用 + 大容量 | 强一致 + 强持久 |
| 后端选型 | 高可用属性缓存 KV | 强一致事务 KV |

---

## 4. 元数据操作与交互:具体方案设计

> 本章逐操作定义客户端与 MDS 的元数据交互(请求 / 响应、寻址方式、后端点查 / 事务、并发与一致性),分 CacheMode(软状态)与 FsMode / native(硬状态)两条路径展开。
>
> 除逐操作交互外,本章还包括:
> - **全局 Inode 分配**:在无状态、多 MDS 并行的前提下,如何全局唯一地分配 `inode_id`(避免单点 / 热点,兼顾外置 KV)。
> - **跨分片 rename**:当源 / 目标 inode 落在不同 KV 分片(或不同后端 / namespace)时,如何保证 `rename` 的原子性与一致性(跨分片事务 / 两阶段方案)。

_(待写)_
