# Etcd 服务发现管理方案

## 背景

Curvine 后续会引入更多无状态、可动态扩缩容的内部服务组件。不同组件可能由不同进程承载，并由 client、worker、master 或其他服务模块按需访问。

为支持这些组件动态 scale up/down，需要引入统一的服务发现机制：

- 服务实例启动后自动注册自身地址和服务元信息。
- 服务实例异常退出、Pod 被驱逐或节点故障后，其服务记录自动过期。
- 调用方能够在启动和运行期间发现可用服务实例。
- 服务发现能力可复用，MDS 只是第一类接入该能力的组件。

本方案选择基于 etcd 实现服务发现，并在 Curvine 项目中新增公共组件封装 `etcd-client`。

## 目标

- 新增公共服务发现组件，提供服务注册、发现、watch 和 lease keepalive 能力。
- 支持不同服务类型的实例以 lease 方式注册到 etcd。
- 支持调用方从 etcd 发现指定服务类型的实例；服务发现组件维护 endpoint cache，并向调用方暴露 snapshot 和 watch event。
- 保留现有 master、worker、client 静态配置和现有 master heartbeat 机制。
- 避免业务模块直接依赖 `etcd-client`，隔离第三方依赖和 etcd 细节。
- 在 etcd 不可用、watch 断开、服务实例上下线等场景下定义清晰的容错语义。

## 非目标

- 不使用 etcd 替代现有 master/journal 的 Raft 成员管理。
- 不修改现有 worker 向 master heartbeat 注册的语义。
- 不在第一阶段实现任何具体业务组件的数据分片、一致性协议或主从选举。
- 不要求所有 Curvine RPC 都迁移到服务发现路径；第一阶段只提供服务发现底座。
- 不把 `etcd-client` 直接暴露给 client、worker、master 或具体业务模块。

## 现状分析

当前 Curvine 的 master 地址主要来自静态配置：

- `ClusterConf::resolve_master_addrs()` 会从 `[client].master_addrs` 或 `[journal].journal_addrs` 推导 master RPC 地址。
- `ClusterConf::master_nodes()` 将 master 地址转换成 `NodeAddr`。
- `FsContext` 创建 `ClusterConnector` 时把 `conf.master_nodes()` 添加为可连接节点。
- Worker 仍通过 `MasterClient` 向 master 发送 heartbeat，完成 worker 注册、心跳和 block report。

因此，服务发现应作为独立路径引入，不替代现有 master 发现与 worker 注册：

```text
Existing path:
client / worker -> static master_addrs -> master

New path:
service instance -> register to etcd with lease
consumer -> discover service endpoints from etcd -> service RPC
```

MDS 在本方案中只是第一个预计接入的业务服务类型：它是新的无状态、可动态扩缩容服务，不是现有 master/Raft 成员管理的替代，也不复用 worker 向 master heartbeat 的注册路径。MDS 的 `ServiceKind` 常量、RPC client pool、一致性 hash key 选择和失败 blacklist 都应放在 MDS 相关业务模块内；通用 discovery crate 只认识字符串形式的 `ServiceKind`。如果未来 MDS 演进为有主从、分片 ownership 或与 master/Raft 强绑定的组件，需要重新评估当前“任意 serving endpoint 可被一致性 hash 选中”的前提。

## 总体架构

```text
                         ┌─────────────────────┐
                         │        etcd         │
                         │ service registry    │
                         └──────────▲──────────┘
                                    │ lease keepalive / watch
                    ┌───────────────┼────────────────┐
                    │               │                │
          ┌─────────┴──────┐ ┌──────┴─────────┐ ┌────┴──────────┐
          │ service inst 1 │ │ service inst 2 │ │ service inst N │
          └────────────────┘ └────────────────┘ └───────────────┘
                    ▲               ▲                ▲
                    │               │                │
                    └───────────────┼────────────────┘
                                    │ endpoint cache
                         ┌──────────┴──────────┐
                         │ service consumer    │
                         │ ServiceResolver     │
                         └─────────────────────┘
```

服务实例启动后向 etcd 注册服务记录，并绑定 lease。调用方通过 `ServiceResolverHandle` 获取指定服务类型的 endpoint snapshot 和 watch event；endpoint cache 由服务发现组件维护。

## 模块设计

### 公共服务发现 crate

新增 crate：

```text
crates/adapters/curvine-service-discovery
```

该 crate 放在 `crates/adapters/`，因为第一阶段实现会封装 `etcd-client` 以及其 `tonic` / `prost` 运行时依赖；`crates/common/` 继续保持纯共享类型、配置、proto 和宏等轻量模块。

该 crate 可以加入根 workspace `members`，但必须保持 `default = []`，默认只编译 trait、公共类型、key/value 编解码和测试 fake；`etcd-client` 只能通过 `etcd` feature 拉入。任何业务二进制如果要启用真实 etcd provider，必须显式透传自己的 feature，例如 `service-discovery-etcd = ["curvine-service-discovery/etcd"]`，不能让当前默认构建链路隐式开启 etcd 依赖。如果未来某个默认构建的业务二进制必须默认启用 etcd provider，需要先评估是否将该二进制从根 `default-members` 中移出，避免 `cargo build` 默认拉入 tonic/prost/codegen 链路。

职责：

- 定义服务发现通用 trait 和数据结构。
- 封装 etcd key/value 编码规则。
- 在 `etcd` feature 下实现 etcd provider。
- 向业务模块暴露 Curvine 自己的抽象，而非 `etcd-client` 类型。

feature / 构建策略：

| 构建场景 | `curvine-service-discovery` | 消费方 feature | 是否拉入 `etcd-client` | 说明 |
| --- | --- | --- | --- | --- |
| 默认 workspace build | `default = []` | 未开启 | 否 | 只编译抽象和 JSON/key 逻辑 |
| discovery 单元测试 | `default = []` | 未开启 | 否 | 使用测试模块内 `FakeResolver` / `FakeRegistry` |
| etcd provider 集成测试 | `--features etcd` | 未开启 | 是 | 需要本地或 CI sidecar etcd |
| 业务二进制接入 etcd | `etcd` | 显式开启业务 feature | 是 | 由业务 crate 控制 feature 传播 |

运行时约束：

- `EtcdServiceRegistry` 和 `EtcdServiceResolver` 构造时必须接收 Curvine 注入的 `Arc<curvine_runtime::runtime::Runtime>`，后台 keepalive、watch、重连任务都通过该 runtime spawn。
- `RegistrationGuard` / `ServiceResolverHandle` 必须持有必要的 runtime 生命周期 owner，避免 registry/resolver 被 drop 后后台 keepalive/watch 任务静默停止。
- discovery crate 内禁止新建独立 Tokio runtime，避免与 Curvine 统一 runtime、shutdown 和测试模型冲突。
- trait 可以保持 async API；provider 内部持有 runtime 仅用于管理后台任务生命周期。

当前实现模块结构：

```text
crates/adapters/curvine-service-discovery/src/
├── lib.rs
├── error.rs
├── endpoint.rs
├── fake.rs        # cfg(test) only
├── key.rs
├── registry.rs
├── resolver.rs
└── etcd_provider.rs
```

第一阶段不实现 `static_provider`。本地测试和业务单测如果需要固定 endpoint，直接在测试模块中提供 `FakeResolver` / `FakeRegistry`，避免把测试辅助实现暴露为正式 provider。

核心类型：

```rust
use curvine_proto::ComponentInfoProto;

pub type DiscoveryResult<T> = Result<T, DiscoveryError>;

#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("invalid service kind: {0}")]
    InvalidServiceKind(String),
    #[error("invalid service id: {0}")]
    InvalidServiceId(String),
    #[error("invalid endpoint value: {0}")]
    InvalidEndpointValue(String),
    #[error("endpoint key/value mismatch: key={key}, value_kind={value_kind}, value_id={value_id}")]
    KeyValueMismatch { key: String, value_kind: String, value_id: String },
    #[error("invalid registration options: {0}")]
    InvalidRegistrationOptions(String),
    #[error("etcd unavailable: {0}")]
    EtcdUnavailable(String),
    #[error("watch revision has been compacted: {revision}")]
    WatchCompacted { revision: i64 },
    #[error("resolver cache is stale")]
    StaleCache,
    #[error("service registration lost: {0}")]
    RegistrationLost(String),
}

pub struct ServiceKind(String);

impl ServiceKind {
    pub fn try_new(kind: impl Into<String>) -> DiscoveryResult<Self> {
        let kind = kind.into();
        // Validate non-empty lowercase [a-z0-9_-], then wrap it.
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub struct ServiceSnapshot {
    pub kind: ServiceKind,
    pub revision: i64,
    pub stale: bool,
    pub last_update_ms: u64,
    pub endpoints: Vec<ServiceEndpoint>,
}

pub struct ServiceEndpoint {
    pub kind: ServiceKind,
    pub id: String,
    pub host: String,
    pub rpc_port: u16,
    // Optional in JSON. Omitted when the service does not expose web/metrics/debug port.
    pub web_port: Option<u16>,
    // Structured component/version/capability metadata shared with existing compatibility framework.
    pub component_info: ComponentInfoProto,
    pub start_time_ms: u64,
    // Optional in JSON. Defaults to Serving when omitted. Discovery only passes it through.
    pub status: ServiceStatus,
    // Optional in JSON. Omitted when deployment metadata or caller-specific hints are not needed.
    pub metadata: Option<std::collections::BTreeMap<String, String>>,
}

pub enum ServiceStatus {
    Starting,
    Serving,
    Draining,
}
```

`ServiceKind` 是 key path 的类型边界，不能只依赖调用方手动调用 `try_new()`：如果为它实现 serde JSON/TOML 反序列化，必须在 `Deserialize` 中复用 `try_new()` 校验，或通过 `TryFrom<String>` 收口，避免 endpoint value 反序列化时绕过 `[a-z0-9_-]` 规则。

`ServiceStatus` 只作为 endpoint value 的状态字段透传。服务发现组件不解释 `Draining` 的截止时间、不等待连接排空，也不主动从 cache 删除 `Draining` endpoint；调用方可以按自身策略跳过或继续使用该 endpoint。

字段必填性：

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `kind` | 是 | 无 | 服务类型，例如 `mds`；具体业务 kind 常量由消费侧定义，不在通用 discovery crate 中硬编码 |
| `id` | 是 | 无 | 服务实例唯一 ID，同时作为 etcd key 末尾的 `service_id` |
| `host` | 是 | 无 | 调用方访问服务 RPC 的可连接地址，可为 DNS name、IPv4、IPv6 或 `localhost` |
| `rpc_port` | 是 | 无 | 服务 RPC 端口 |
| `web_port` | 否 | `null` / omit | 服务 Web、metrics 或 debug 端口；没有暴露时可省略 |
| `component_info` | 是 | 无 | 结构化组件版本信息，复用 `ComponentInfoProto` / `ComponentVersion`，包含 release、git、protocol 和 `capabilities[]` |
| `start_time_ms` | 是 | 无 | 服务实例启动时间，Unix epoch 毫秒 |
| `status` | 否 | `serving` | 服务状态；缺省按 `serving` 兼容旧实例。discovery 只透传，不处理 drain 语义 |
| `metadata` | 否 | `null` / omit | 运维辅助元信息或调用方自定义 hints，例如 pod、node、zone、weight |

`component_info` 的字段沿用 `ComponentInfoProto` 现有定义：`component`、`release_version`、`git_commit`、`git_tag`、`git_branch`、`protocol_version`、`min_protocol_version` 在 proto 层是 optional/default 字段，`capabilities` 是可为空数组。对服务发现 value 来说，外层 `component_info` 必填；生产服务应至少填充 `component`、`release_version`、`protocol_version`、`min_protocol_version` 和 `capabilities`，便于调用方复用兼容性判断和能力过滤。

注意 proto2 default 与 JSON/serde 语义不同：JSON 反序列化缺失 `protocol_version` 时会得到 `None`，不会自动变成 proto 默认值 `1`；缺失 `capabilities` 这类 `Vec` 字段还可能直接反序列化失败。因此调用方做协议或能力预过滤时，必须复用既有 `CompatibilityPolicy` / accessor 语义，例如用 `unwrap_or(1)` 处理 proto2 默认值，不能直接把裸 `None` 当成不兼容或不支持。实现阶段优先在 `curvine-proto` 的 prost serde 派生上统一增加 `#[serde(default)]`，让缺字段 JSON 更接近 protobuf 的宽松演进语义；如果该改动影响面过大，则 discovery value schema 必须明确规定生产者整体序列化真实 `ComponentInfoProto`，并且未来新增字段优先使用 optional 标量，新增 repeated 或非 optional 字段前需要额外兼容设计。

注册接口：

```rust
#[async_trait::async_trait]
pub trait ServiceRegistry: Send + Sync {
    async fn register(
        &self,
        endpoint: ServiceEndpoint,
        options: RegistrationOptions,
    ) -> DiscoveryResult<RegistrationGuard>;
}

pub struct RegistrationOptions {
    pub lease_ttl_secs: u64,
    pub keep_alive_interval_secs: u64,
    pub register_timeout_ms: u64,
}

impl Default for RegistrationOptions {
    fn default() -> Self {
        Self {
            lease_ttl_secs: 10,
            keep_alive_interval_secs: 3,
            register_timeout_ms: 5000,
        }
    }
}

impl RegistrationOptions {
    pub fn validate(&self) -> DiscoveryResult<()> {
        // lease_ttl_secs >= keep_alive_interval_secs * 3 > 0,
        // lease_ttl_secs must fit etcd API i64,
        // register_timeout_ms > 0.
        Ok(())
    }
}
```

注销通过 `RegistrationGuard` 完成，不在 registry 上暴露裸 `unregister(service_id)`。这样可以避免调用方只传 `service_id` 时缺失 `cluster_id` / `service_kind` 导致删错 key。

注册同一个 `{kind, service_id}` 必须是原子 create-if-absent 语义。已有 live registration 时，新的 `register()` 返回 `RegistrationAlreadyExists`，不能覆盖旧 key 的 lease；旧 guard 也不能在新实例注册后继续用旧 lease 覆盖新值。

`RegistrationOptions::validate()` 规则：

- `lease_ttl_secs > 0`。
- `keep_alive_interval_secs > 0`。
- `lease_ttl_secs >= keep_alive_interval_secs * 3`，确保 keepalive 至少有两次重试窗口；调用 etcd grant lease 时再安全转换为 etcd API 需要的 `i64`。
- `register_timeout_ms > 0`。

发现接口：

```rust
#[async_trait::async_trait]
pub trait ServiceResolver: Send + Sync {
    async fn list(&self, kind: ServiceKind) -> DiscoveryResult<ServiceSnapshot>;
    async fn watch(&self, kind: ServiceKind) -> DiscoveryResult<ServiceResolverHandle>;
}

pub struct ServiceResolverHandle {
    kind: ServiceKind,
    reader: SnapshotReader,
    events: ServiceWatch,
}

#[derive(Clone)]
pub struct SnapshotReader {
    cache: std::sync::Arc<tokio::sync::RwLock<ServiceSnapshot>>,
    allow_stale_cache: bool,
}

impl ServiceResolverHandle {
    pub fn reader(&self) -> SnapshotReader {
        self.reader.clone()
    }

    pub fn into_parts(self) -> (SnapshotReader, ServiceWatch) {
        (self.reader, self.events)
    }

    pub async fn next_event(&mut self) -> Option<DiscoveryResult<ServiceWatchEvent>> {
        self.events.recv().await
    }
}

impl SnapshotReader {
    pub async fn snapshot(&self) -> DiscoveryResult<ServiceSnapshot> {
        // Return a cloned immutable snapshot and never expose internal locks.
        // If stale && !allow_stale_cache, return DiscoveryError::StaleCache.
        // Otherwise return Ok(snapshot).
    }
}

pub type ServiceWatch = tokio::sync::mpsc::Receiver<DiscoveryResult<ServiceWatchEvent>>;

pub enum ServiceWatchEvent {
    Added(ServiceEndpoint),
    Updated(ServiceEndpoint),
    Removed { kind: ServiceKind, id: String },
    Reset(ServiceSnapshot),
}
```

`list(kind)` 是一次性查询；`watch(kind)` 是完整持续订阅，调用方不需要在调用 `watch()` 前额外调用 `list()`。

`watch(kind)` 必须在内部先执行一次 `list(kind)`，然后从 snapshot revision + 1 开始 watch，并把初始列表作为第一条 `Reset(ServiceSnapshot)` 事件发给调用方。当前实现中，`watch()` 在初始 list 成功、内部 cache 初始化且初始 `Reset` 已入队后返回 `ServiceResolverHandle`；真实 etcd watch stream 由后台任务从 `revision + 1` 建立，因此即使 watch stream 创建稍晚，也能通过 etcd revision 语义避免 list/watch 间隙丢事件。`Reset` 用于初始快照和 compact revision 后的全量刷新；普通 transient 断线恢复时优先从最后成功处理的 revision + 1 续接，不强制发送 `Reset`。服务发现组件负责维护 handle 内部 cache，调用方可以通过 `snapshot()` 获取当前 endpoint 集合。

`ServiceWatch` 第一阶段使用 `mpsc::Receiver`，语义是单消费者事件流。`ServiceResolverHandle::reader()` 可返回 clone 的 `SnapshotReader`，用于多个任务并发读取 snapshot；事件流仍只有一个消费者。多个模块需要各自消费事件时，应各自创建独立的 `ServiceResolverHandle`；这意味着第一阶段不做进程内 watch 复用，每个 handle 可以对应独立的 etcd watch。后续如果需要进程内 fan-out，再引入 broadcast/watch channel 封装。

`SnapshotReader::snapshot()` 返回 clone 后的不可变快照，不能向调用方暴露内部锁或可变引用。由于内部 cache 使用 `tokio::sync::RwLock`，`snapshot()` 是 async 方法，并且在 `allow_stale_cache = false` 且 cache stale 时返回 `DiscoveryError::StaleCache`。`ServiceSnapshot::stale` 表示当前 cache 是否来自断开的 watch 或无法确认最新 revision 的状态；`last_update_ms` 表示最近一次成功 list/watch 更新本地 cache 状态的时间。provider 内部可使用 `cached_snapshot()` 读取缓存而不受 `allow_stale_cache` 门控影响，用于断线重连时保留旧 endpoint。

错误模型约定：

- 参数校验失败返回 `InvalidServiceKind`、`InvalidServiceId`、`InvalidRegistrationOptions` 或 `InvalidDiscoveryConfig`。
- `DiscoveryError` 使用 `thiserror::Error` 派生；`etcd_client::Error` 通过 `From` 转换为 `EtcdUnavailable` 或更具体的 provider 错误，避免业务侧直接依赖 etcd error 类型。
- 对外接入已有模块时，在模块边界再把 `DiscoveryError` 映射为 `curvine_core_error::CommonError` 或 `curvine_error::FsError`，不要在 discovery crate 内混入业务 RPC 错误语义。
- endpoint JSON 解码失败返回或记录 `InvalidEndpointValue`；list/watch 路径必须忽略 malformed key/value 并继续处理后续 endpoint，避免单条脏数据毒化整个服务发现。
- key/value 的 kind 或 id 不一致时使用 `KeyValueMismatch`，resolver 丢弃该 endpoint。
- etcd 连接、请求或 watch stream 创建失败返回 `EtcdUnavailable`。
- watch revision 被 compact 时使用 `WatchCompacted`，provider 随后重新 list 并发送 `Reset(ServiceSnapshot)`。
- `allow_stale_cache = false` 且 cache stale 时，`snapshot()` 返回 `StaleCache`。
- lease keepalive 丢失后使用 `RegistrationLost` 或 `RegistrationStatus::KeepAliveLost { message }` 通知注册方。

### 配置模块

在 `curvine-config` 中仅新增 discovery 配置。具体业务服务的监听端口、实例名、启动参数等配置不放入服务发现组件范围，由对应业务组件自行定义。

```text
crates/common/curvine-config/src/discovery_conf.rs
```

示例配置：

```toml
[discovery]
enabled = true
provider = "etcd"
endpoints = [
    "http://etcd-0:2379",
    "http://etcd-1:2379",
    "http://etcd-2:2379"
]
prefix = "/curvine"
connect_timeout_ms = 3000
request_timeout_ms = 3000
watch_reconnect_min_ms = 1000
watch_reconnect_max_ms = 30000
watch_reconnect_jitter_ratio = 0.2
allow_stale_cache = true
```

默认行为：

- `discovery.enabled = false`。
- `provider = "etcd"` 是第一阶段唯一支持的真实 provider；配置结构按 string 保留扩展性，解析时使用 `serde(default)`。
- `cluster_id` 来自已有 `ClusterConf`，不在 discovery 配置中重复定义；用于 key path 前必须校验为非空且只包含 `[a-z0-9_-]`。
- 未开启 discovery 时，不加载 `etcd-client` provider，不影响现有部署。
- 如果 `enabled = true` 但 provider 未知或为空，应在启动时返回清晰配置错误；如果 `enabled = false`，未知 provider 不应影响进程启动。
- auth/TLS 字段第一阶段不加入配置；后续如果需要启用认证或加密，可通过 `serde(default)` 新增字段，属于向后兼容的配置扩展。

### 服务注册

服务进程启动流程：

1. 加载 `ClusterConf`。
2. 启动业务 RPC server，并获取实际 bind address。
3. 构造 `ServiceEndpoint { kind, ... }`。
4. 通过 `ServiceRegistry::register()` 写入 etcd。
5. 后台 keepalive lease。
6. 收到 shutdown signal 时设置 `Draining` 或主动 unregister。
7. 退出进程。

服务发现组件不会自动把状态从 `Starting` 切到 `Serving`。如果服务需要表达 ready 状态，应在业务 RPC server ready 后显式调用 `update_status(ServiceStatus::Serving)`。

注册方负责构造 `component_info`。建议业务服务使用 `curvine_sys::version::component_version("<component>")` 生成组件版本信息，再通过 `curvine_model::ProtoUtils::component_version_to_pb` 转成 `ComponentInfoProto`；通用 discovery crate 本身只依赖 `curvine-proto`，不依赖 `curvine-sys` 或 `curvine-model` 来生成业务组件信息。

注册应返回一个 guard：

```rust
pub struct RegistrationGuard {
    kind: ServiceKind,
    service_id: String,
    lease_id: i64,
    status_rx: tokio::sync::watch::Receiver<RegistrationStatus>,
    control: std::sync::Arc<dyn RegistrationControl>,
}

pub enum RegistrationStatus {
    Registered,
    KeepAliveLost { message: String },
    Revoking,
    Revoked,
}

impl RegistrationGuard {
    pub fn subscribe_status(&self) -> tokio::sync::watch::Receiver<RegistrationStatus> {
        self.status_rx.clone()
    }

    pub async fn update_endpoint(&self, endpoint: ServiceEndpoint) -> DiscoveryResult<()> {
        // Update mutable fields on the existing key with the same lease.
        // The endpoint kind and id must match this guard.
    }

    pub async fn update_status(&self, status: ServiceStatus) -> DiscoveryResult<()> {
        // Update only the endpoint status on the existing key with the same lease.
    }

    pub async fn shutdown(self) -> DiscoveryResult<()> {
        // Stop keepalive and revoke lease.
    }
}
```

guard 负责：

- 持有 lease 信息。
- 运行 keepalive 后台任务。
- 显式 `shutdown().await` 时尽力 revoke lease / delete key。
- `Drop` 中只能触发后台停止信号，不能依赖异步 revoke 一定完成；异常退出依赖 lease TTL 清理 key。
- keepalive 失败时更新 `RegistrationStatus::KeepAliveLost` 并记录错误。
- 服务进程可通过 `status_rx` 感知 lease 丢失，并按自身策略停止 serving、重试注册或主动退出。
- 服务进程需要切换 `Starting` / `Serving` / `Draining` 或更新 metadata 时，通过 `update_status()` / `update_endpoint()` 更新同一个 etcd key，并继续复用原 lease。
- `update_endpoint()` 禁止修改 `kind` 和 `id`；如果传入 endpoint 的 `kind` / `id` 与 guard 不一致，必须返回错误，避免更新到错误的 key。
- `update_endpoint()` 还必须校验 `endpoint.component_info.component` 存在且与 `endpoint.kind` / guard kind 一致；不一致或缺失时拒绝更新，避免注册方写入后被 resolver 按兼容规则丢弃。
- 当 guard 状态已经进入 `KeepAliveLost` 后，`update_status()` 和 `update_endpoint()` 必须返回错误；服务进程需要重新 `register()` 获取新的 lease 和 key 状态。
- `update_status()` 和 `update_endpoint()` 都通过 PUT 更新同一个 etcd key 的 value，并且必须继续绑定原 lease，例如使用 `PutOptions::with_lease(lease_id)`；如果 etcd-client 后续提供更合适的保留 lease 选项，也必须确保更新后 key 不会变成永久 key。
- 调用方会通过 watch 收到 `Updated(ServiceEndpoint)` 事件。

`RegistrationStatus::KeepAliveLost { message }` 和 `DiscoveryError::RegistrationLost(message)` 的分工如下：前者通过 `status_rx` 异步通知注册方后台 keepalive 状态变化；后者用于 `update_status()`、`update_endpoint()` 等同步 API 在 lease 已丢失后返回明确错误。

## Etcd key/value 规范

### Key 格式

```text
{prefix}/{cluster_id}/services/{service_kind}/{service_id}
```

以 MDS 为例，key 可以是：

```text
/curvine/prod-cluster/services/mds/mds-curvine-mds-0-9100-8c5d2a1f
```

字段说明：

| 字段 | 说明 |
| --- | --- |
| `prefix` | 全局命名空间，默认 `/curvine` |
| `cluster_id` | Curvine 集群 ID |
| `service_kind` | 服务类型，例如 MDS 使用 `mds` |
| `service_id` | 服务实例唯一 ID，建议包含 host、port 和启动 UUID |

`service_kind` 必须是非空 lowercase 标识，只允许 `[a-z0-9_-]`，避免 `MDS` 和 `mds` 被注册成两个不同服务类型。`service_id` 必须只包含适合出现在 etcd key 中的安全字符，建议使用 `[a-zA-Z0-9_-]`。`ServiceKind::try_new()` 应在构造时校验该规则并返回错误，避免业务模块写入不可预测的 key path。具体业务模块如果需要常量，例如 MDS 的 `mds`，应在业务模块内定义。

### Service ID 构造

`service_id` 用于标识一次服务进程生命周期内的服务实例。它需要具备唯一性、可读性，并避免进程重启后复用旧 etcd key。

推荐格式：

```text
{kind}-{instance_name}-{rpc_port}-{short_uuid}
```

字段说明：

| 字段 | 说明 |
| --- | --- |
| `kind` | 服务类型，例如 `mds` |
| `instance_name` | 实例可读名称；Kubernetes 场景优先使用 Pod name，非 Kubernetes 场景使用 sanitized host |
| `rpc_port` | 服务 RPC 端口，用于区分同一实例名下的多个进程 |
| `short_uuid` | 进程启动时生成的 8 到 12 位 UUID 片段，用于避免重启或快速拉起时复用旧 key |

示例：

```text
mds-curvine-mds-0-9100-8c5d2a1f
```

裸机或非 Kubernetes 场景可以使用 sanitized host：

```text
mds-10-0-0-12-9100-8c5d2a1f
```

构造规则：

- `service_id` 必须只包含适合出现在 etcd key 中的安全字符，建议使用 `[a-zA-Z0-9_-]`。
- `instance_name` 中的 `.`, `:`, `/`, 空格等字符应替换为 `-`。
- 不建议只使用 `host:port`，因为进程重启后可能复用同一地址，容易覆盖旧实例记录。
- `short_uuid` 只用于唯一性，不作为安全凭证。
- 因为 `service_id` 包含 UUID，服务快速重启时可能短时间出现旧 key 和新 key 同时存在；该窗口依赖旧 lease TTL 自动收敛，调用方 RPC 层需要能处理旧 endpoint 短暂不可达。
- 优雅重启时，旧实例应先调用 `RegistrationGuard::shutdown().await` 主动 revoke lease，再启动替换实例，从而缩短旧 key 与新 key 同时存在的窗口。

### Value 格式

value 使用 JSON，便于调试和兼容升级。下面以 MDS 为例，展示所有核心字段和可选字段：

```json
{
  "kind": "mds",
  "id": "mds-curvine-mds-0-9100-8c5d2a1f",
  "host": "curvine-mds-0.curvine-mds-headless.default.svc.cluster.local",
  "rpc_port": 9100,
  "web_port": 9101,
  "component_info": {
    "component": "mds",
    "release_version": "0.2.0",
    "git_commit": "8c5d2a1f...",
    "git_tag": "",
    "git_branch": "main",
    "protocol_version": 1,
    "min_protocol_version": 1,
    "capabilities": ["metadata-read", "metadata-write"]
  },
  "start_time_ms": 1780000000000,
  "status": "serving",
  "metadata": {
    "pod": "curvine-mds-0",
    "node": "worker-node-1",
    "zone": "az-a",
    "weight": "100"
  }
}
```

兼容规则：

- 新增字段必须使用默认值兼容旧 client。
- 删除字段前必须至少经历一个 minor 版本兼容期。
- 调用方遇到未知字段应忽略。
- discovery 负责发布 `ComponentInfoProto`，但不重复实现协议兼容判断；调用方 RPC/handshake 层继续复用现有 `CompatibilityPolicy` 或等价兼容框架判断是否可访问该 endpoint。
- `ServiceKind` 应与 `component_info.component` 保持一致；resolver 需要校验 `component_info.component` 存在且二者一致，不一致或缺失时丢弃 endpoint 并记录 warn。MDS 的组件名 `mds` 由 MDS 业务模块定义，必要时后续再补充到公共 proto 注释或组件枚举约定中。
- 调用方在一致性 hash 前可以基于 `component_info.capabilities` 做能力预过滤，例如只选择支持目标接口能力的 endpoint；具体能力名和兼容策略不放入通用 discovery crate。
- resolver 解析 endpoint 时必须校验 key 中的 `service_kind` / `service_id` 与 value 中的 `kind` / `id` 一致；不一致时丢弃该 endpoint 并记录 warn。
- discovery 组件不解析 `metadata` 的业务含义，所有 value 都按 string 透传。
- `metadata` 中 `curvine.` 前缀保留给框架使用，业务模块自定义 key 应避免使用该前缀。
- 第一阶段 `metadata` 仅支持 string map；如果后续需要复杂结构，应新增明确字段或版本化 value schema，而不是在 discovery 组件内解析业务 JSON。

## Lease 和心跳策略

建议参数如下。这些参数描述服务发现运行时策略，不新增具体业务服务配置文件。注册侧参数以 `RegistrationOptions::default()` 为权威默认值；discovery 配置只承载 etcd 连接、watch 和 cache 策略，避免同一默认值在多个配置结构中重复定义。

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `lease_ttl_secs` | `10` | etcd lease TTL，由注册方传入或使用默认值 |
| `keep_alive_interval_secs` | `3` | lease keepalive 周期，由注册方传入或使用默认值 |
| `register_timeout_ms` | `5000` | 服务注册超时，由注册方传入或使用默认值 |
| `watch_reconnect_min_ms` | `1000` | watch 断开后指数退避的初始间隔，来自 discovery 配置 |
| `watch_reconnect_max_ms` | `30000` | watch 断开后指数退避的最大间隔，来自 discovery 配置 |
| `watch_reconnect_jitter_ratio` | `0.2` | watch 重连 jitter 比例，避免多进程同频重连 |

策略：

- 服务注册必须绑定 lease。
- 服务正常退出时主动 revoke lease。
- 服务异常退出时依赖 TTL 自动清理 key。
- keepalive 连续失败超过阈值时，服务应进入不可服务状态或主动退出，由上层编排系统重启。
- 调用方不依赖服务自身 heartbeat，只信任 etcd lease 和 RPC 健康结果。
- watch 重连使用指数退避并附加 jitter，不使用固定 1 秒间隔，避免同进程多 handle 或多进程在 etcd 短暂故障后形成重连风暴。

## 调用方发现流程

### 启动流程

```text
service consumer start
  ├─ load ClusterConf
  ├─ if discovery disabled:
  │    └─ skip service discovery
  └─ if discovery enabled:
       ├─ connect etcd
       ├─ call watch(service_kind)
       ├─ resolver internally lists /services/{service_kind} prefix
       ├─ resolver validates endpoints and initializes endpoint cache
       └─ consumer receives initial Reset event and later watch events
```

启动语义：

- 调用方如果必须访问某个服务且发现列表为空，应快速返回明确错误。
- 调用方也可以由接入层决定是否等待目标服务出现；该策略不放入服务发现配置。
- 如果 etcd 暂时不可用，启动策略由配置控制：快速失败或等待重试。

### 运行时 watch

watch 事件处理：

| Etcd 事件 | 本地处理 |
| --- | --- |
| `PUT` | decode value，校验基础字段，upsert endpoint |
| `DELETE` | 从本地 cache 删除 endpoint |
| watch cancel | 保留旧 cache，后台重连 |
| compact revision | 重新 list 全量 endpoint，再恢复 watch |
| decode 失败 | 忽略该 endpoint 并记录 warn |

watch 启动必须遵循 revision 规则，避免 list 和 watch 之间漏事件：

1. 对 `{prefix}/{cluster_id}/services/{service_kind}/` 做一次 prefix list。
2. 记录 list response header revision，生成初始 endpoint snapshot。
3. 从 `revision + 1` 开始 watch 同一 prefix。
4. 如果 watch 返回 compacted revision，重新执行 prefix list，并向调用方发送 `Reset(ServiceSnapshot)`。
5. 如果 watch 连接断开但 revision 仍可继续，优先从最后处理成功的 revision + 1 恢复 watch。

第一阶段可以先不实现 prefix list 分页，但接口设计不能假设服务实例数量永远很小。落地时如果 etcd-client 支持 limit/range 分页，应把“完整 list 直到无更多 key”作为 provider 内部实现细节，并保证最终 snapshot 使用同一个 etcd header revision 语义；如果当前 etcd API/客户端无法安全分页，则需要在文档和配置中明确单 kind endpoint 数量上限。

watch 断开期间，`next_event()` 不持续向调用方刷错误；provider 记录日志和指标，并按 `watch_reconnect_min_ms` 到 `watch_reconnect_max_ms` 的指数退避加 per-watch jitter 在后台重连。初始化阶段如果初始 list 失败，`watch()` 直接返回 `DiscoveryError::EtcdUnavailable` 或对应错误；进入后台 watch loop 后，transient watch 错误和 compact 都由 provider 内部恢复，不持续向调用方发送错误事件。

本地 cache 要求：

- 使用并发安全结构维护 endpoint 列表。
- 读路径不能阻塞 watch 更新路径。
- watch 断开时，如果 `allow_stale_cache = true`，`ServiceResolverHandle::snapshot()` 可继续返回旧 endpoint，但必须通过指标和日志暴露 cache stale 状态。
- watch 断开时，如果 `allow_stale_cache = false`，`ServiceResolverHandle::snapshot()` 必须返回 `DiscoveryError::StaleCache`，不能返回空集合，避免调用方误判为当前没有服务实例。
- watch 恢复或收到 progress/create 确认时必须将 `stale` 置回 `false`，并在成功确认 cache 对应 revision 后更新 `last_update_ms`；仅标记 stale 时不能覆盖 `last_update_ms`。如果是 compact 或本地状态不可信导致的恢复，必须先完成全量 refresh，并向 `ServiceResolverHandle` 发送 `Reset(ServiceSnapshot)`；如果是普通 transient 断线且能从最后处理 revision + 1 续接，可不发送 `Reset`。
- stale 状态第一阶段通过 `ServiceSnapshot::stale`、指标和日志暴露，不新增专门的 watch event。

## Endpoint 使用边界

服务发现组件只负责提供 endpoint 集合，不负责业务 RPC 的 endpoint 选择、负载均衡、重试或失败摘除。原因是这些策略通常依赖具体业务语义，例如请求幂等性、连接池类型、协议兼容性、分片规则和调用方超时策略。

服务发现组件职责：

- 维护指定 `ServiceKind` 的 endpoint cache。
- 处理 etcd list/watch、watch 重连和 compact revision 后的全量刷新。
- 根据 etcd `PUT` / `DELETE` 更新本地 endpoint 集合。
- 过滤无法解码或明显非法的 endpoint value。
- 暴露 endpoint snapshot 或 watch stream 给调用方。

调用方职责：

- 使用一致性 hash 在 endpoint 集合中选择目标服务实例。
- 维护 RPC 失败后的短期 blacklist、重试和熔断策略。
- 管理具体服务的 client pool、连接复用和请求超时。

首个接入服务在自己的 RPC client 层使用一致性 hash 策略：

- 对业务请求的稳定 key 计算 hash，例如 path、inode、volume、namespace 或调用方自定义 key。
- 在当前可用 endpoint 集合上做一致性 hash 映射。
- 跳过 `status != Serving` 的 endpoint。
- RPC 失败后将 endpoint 放入短期 blacklist。
- blacklist 到期后允许重新尝试。
- 不在 etcd 中维护 shard ownership、load 信息或 draining deadline。

## 故障语义

### Etcd 不可用

| 场景 | 行为 |
| --- | --- |
| 服务启动时 etcd 不可用 | 注册失败，服务不进入 serving 状态 |
| 服务运行中 keepalive 失败 | 标记 unhealthy，重试；超过阈值后退出或停止服务 |
| 调用方启动时 etcd 不可用 | 根据配置快速失败或等待重试 |
| 调用方 watch 断开 | 保留旧 cache，后台重连 |

### 服务实例故障

| 场景 | 行为 |
| --- | --- |
| 服务正常下线 | 主动 unregister，调用方收到 DELETE |
| 服务进程崩溃 | lease TTL 到期后 etcd 删除 key |
| 服务 RPC 不可达但 lease 未过期 | 服务发现组件不主动删除 endpoint；调用方在自己的 RPC 层做重试、熔断或 blacklist |
| 服务升级中 | 可先注册 `Starting`，ready 后更新为 `Serving`，退出前更新为 `Draining`；discovery 只透传状态，不保证下线窗口，也不参与流量排空 |

## 集群内访问约束

etcd 仅在 Curvine 集群内部使用，第一阶段不引入 username、password、TLS 证书等认证和加密配置。部署侧应通过集群网络边界保证 etcd 只暴露给可信组件：

- etcd endpoint 使用集群内地址，不对公网暴露。
- 服务提供方和调用方通过同一 Curvine 集群的内网访问 etcd。
- 不同 Curvine 集群使用不同 `cluster_id` 或 prefix 隔离 key 空间。
- 运维侧通过 Kubernetes Service、NetworkPolicy、安全组或等价机制限制 etcd 访问范围。
- 方案中的 service discovery crate 不处理账号、密码、证书或 token。

安全边界假设：etcd 是服务发现数据的信任根。任何能写入该 key prefix 的进程都可以注册伪造 endpoint 或污染服务列表，因此生产部署必须在网络、RBAC、sidecar 或等价基础设施层限制 etcd 写权限；本组件只校验 value schema 和 key/value 一致性，不能替代部署层访问控制。

## 部署和配置说明

- 示例配置文件 `etc/curvine-cluster.toml`、`curvine-docker/deploy/example/conf/curvine-cluster.toml` 和 `curvine-docker/deploy/spdk/curvine-cluster-spdk.toml` 已包含默认关闭的 `[discovery]` 配置块。
- 启用时需要将 `enabled` 改为 `true`，并把 `endpoints` 配置为集群内部 etcd 地址列表，例如 `http://etcd-0:2379`。
- `prefix` 与已有 `cluster.cluster_id` 共同组成 key 空间；同一个 etcd 集群承载多个 Curvine 集群时，应使用不同 `cluster_id` 或不同 `prefix`。
- `watch_reconnect_min_ms`、`watch_reconnect_max_ms` 和 `watch_reconnect_jitter_ratio` 控制调用方 watch 断线后的后台重连退避；注册侧 lease TTL 不放入 `[discovery]`，由注册调用方通过 `RegistrationOptions` 指定或使用默认值。
- 第一阶段未提供 username、password、TLS 相关配置；如果部署环境需要这些能力，应先扩展 `DiscoveryConf` 和 `EtcdDiscoveryConfig`，并重新评估 `etcd-client` feature 组合。
- 真实 etcd 集成测试通过环境变量 `CURVINE_ETCD_ADDR` 控制；未设置时测试会主动 skip，设置后运行 `cargo test -p curvine-service-discovery --features etcd`。

## 指标和日志

建议新增指标：

| 指标 | 类型 | 说明 |
| --- | --- | --- |
| `curvine_discovery_etcd_connected` | gauge | etcd provider 是否连接正常 |
| `curvine_discovery_registered_services` | gauge | 当前进程注册的服务数量 |
| `curvine_discovery_resolved_endpoints` | gauge | 本地 cache 中 endpoint 数量 |
| `curvine_discovery_watch_restarts_total` | counter | watch 重启次数 |
| `curvine_discovery_decode_errors_total` | counter | endpoint value 解码失败次数 |
| `curvine_discovery_cache_stale` | gauge | resolver cache 是否处于 stale 状态 |
| `curvine_discovery_keepalive_lost_total` | counter | lease keepalive 丢失次数 |

指标实现复用项目已有 `curvine-metrics` 封装，按 provider/role/kind 维度打标签，避免每个业务模块重复注册同名指标：

- `provider`: 第一阶段固定为 `etcd`。
- `role`: `registry` 或 `resolver`。
- `kind`: 服务类型，例如 `mds`。

指标注册必须是进程级幂等的；如果构建未启用 `etcd` feature，etcd provider 相关指标不应引入额外依赖或启动后台采集。

日志要求：

- 服务注册成功、续租失败、注销失败需要记录结构化日志。
- 调用方 watch 断开和全量 refresh 需要记录日志。
- 不输出 etcd endpoint 以外的部署敏感信息。

## 依赖管理

在根 `Cargo.toml` 增加 workspace 依赖：

```toml
etcd-client = { version = "0.19", default-features = false }
```

`0.19` 是当前方案建议的验证版本，最终落地时以实际 `cargo check`、`cargo tree`、CI 和依赖兼容性验证结果为准；如果需要 pin 到更低版本，必须在 PR 中说明原因。

在 `curvine-service-discovery` 中通过 feature 控制：

```toml
[features]
default = []
etcd = ["dep:etcd-client", "dep:curvine-config", "dep:curvine-runtime", "dep:log"]

[dependencies]
curvine-config = { workspace = true, optional = true }
curvine-runtime = { workspace = true, optional = true }
etcd-client = { workspace = true, optional = true }
log = { workspace = true, optional = true }
```

注意事项：

- `etcd-client` 基于 `tonic` 和较新的 `prost`，可能与项目现有 `prost` 版本并存。
- `etcd-client` 可能引入 tonic/prost codegen 相关构建要求，落地前需要在 CI 和 Docker 构建环境中验证是否需要额外安装 `protoc`。
- 第一阶段使用 `default-features = false`，不启用 `etcd-client` 的 TLS/auth 相关能力，etcd 只作为集群内部服务发现组件使用；落地时需要用 `cargo tree -e features` 验证 TLS 相关 feature 没有被间接打开。
- 第一阶段不要全 workspace 默认启用 `etcd` feature，避免影响不使用服务发现的构建；根 workspace 可包含 `curvine-service-discovery`，但默认构建不能包含开启 etcd feature 的消费方链路。
- 如后续构建体积或依赖冲突明显，再评估统一升级 `prost` / `tonic` 依赖。

## 测试方案

### 单元测试

- key 生成和解析。
- endpoint JSON 编解码。
- `component_info` JSON 编解码，并校验 `kind == component_info.component`。
- `component_info` 缺失 optional 标量字段时，兼容性预过滤必须按既有默认值语义处理；缺失 `capabilities` 时的行为按最终 serde/default 策略覆盖测试。
- 测试模块内提供 `FakeResolver` / `FakeRegistry`，用于验证注册和发现接口。
- 非法 endpoint value 过滤。
- key 中 `service_kind` / `service_id` 与 value 中 `kind` / `id` 不一致时丢弃 endpoint。
- `ServiceKind::try_new()` 拒绝空字符串、大写字符和非法 key 字符；`ServiceKind` 反序列化不能绕过该校验。
- `RegistrationOptions::default()` 使用文档默认值。
- `RegistrationOptions::validate()` 拒绝 0 值以及 `lease_ttl_secs < keep_alive_interval_secs * 3`。
- `ServiceResolverHandle::snapshot()` 在 stale cache 且 `allow_stale_cache = false` 时返回 `DiscoveryError::StaleCache`。
- `RegistrationGuard::update_endpoint()` 拒绝修改 `kind` / `id`，并拒绝 `component_info.component` 与 kind 不一致的 endpoint。
- 重复注册相同 `{kind, service_id}` 必须失败，并且失败路径要回收新建 lease。
- `KeepAliveLost` 后 `update_status()` / `update_endpoint()` 返回错误。
- `update_status()` / `update_endpoint()` 必须保留原 lease；更新后 key 的 lease id 不变且 TTL 仍会自动过期。
- `update_status()` / `update_endpoint()` 成功后 watch 侧收到 `Updated(ServiceEndpoint)`。
- watch 事件到 cache 的 upsert/delete 逻辑。
- `ServiceSnapshot::stale` 和 `last_update_ms` 状态更新逻辑。
- malformed etcd key/value 只影响对应事件或记录，不能导致 list/watch 整体不可用。
- `ServiceStatus::Draining` 只透传给调用方，discovery 不主动删除 endpoint。

### 集成测试

真实 etcd 集成测试默认不进入普通 `cargo test` 路径：测试应标记 `#[ignore]`，或在未设置 `CURVINE_ETCD_ADDR` 时主动 skip。CI 可新增可选 job，通过 etcd sidecar 启用 `--features etcd` 并运行这些测试。

- 启动本地 etcd，注册一个测试服务实例，调用方能 list 到 endpoint。
- 重复注册相同 `{kind, service_id}` 返回错误，并且不会覆盖已有 registration 的 lease/value。
- malformed key/value 不会导致 list/watch 整体失败，后续合法 endpoint 仍能被处理。
- 服务实例 revoke lease 后，调用方 watch 收到 DELETE。
- 服务进程模拟崩溃后，lease TTL 到期，endpoint 自动消失。
- list 完成后、watch 建立前插入或删除 endpoint，不丢事件。
- watch 断开后重新 list 并恢复 watch。
- `allow_stale_cache = true` 时 watch 断开后 snapshot 返回旧 endpoint 且 `stale = true`。
- `allow_stale_cache = false` 时 watch 断开后 snapshot 返回 `DiscoveryError::StaleCache`。
- 多服务实例动态扩容，调用方 cache 更新为新列表。
- lease keepalive 丢失后，`RegistrationStatus::KeepAliveLost` 能被服务进程观察到。

### 故障注入测试

- etcd 临时不可达。
- etcd compact revision 导致 watch 失效。
- endpoint value 非法 JSON。

业务 RPC 失败但 etcd lease 尚未过期的场景属于调用方 RPC 层测试，不纳入服务发现组件测试范围。

## 落地任务拆分

| 任务 | 说明 | 主要文件 | 验证 |
| --- | --- | --- | --- |
| T1 | 新增 discovery 配置 | `crates/common/curvine-config` | `cargo test -p curvine-config` |
| T2 | 新增 `curvine-service-discovery` crate 和公共类型 | `crates/adapters/curvine-service-discovery` | `cargo test -p curvine-service-discovery` |
| T3 | 实现 key/value schema 和测试 fake resolver | `key.rs`, `endpoint.rs`, test modules | 单元测试 |
| T4 | 实现 etcd list/watch provider | `etcd_provider.rs` | revision/watch/compact 集成测试 |
| T5 | 实现 registration lease guard | `registry.rs`, `etcd_provider.rs` | lease/revoke/keepalive lost 测试 |
| T6 | 完善故障重连和 stale cache | `resolver.rs`, `etcd_provider.rs` | watch reconnect/stale cache 测试 |
| T7 | 更新设计、部署和示例配置文档 | design doc, example config | `git diff --check` |

建议第一阶段只交付 T1 到 T7，形成可复用发现底座。首个业务服务的 RPC client pool、endpoint picker、RPC 失败 blacklist 和业务路由策略应在对应业务模块中单独设计。

## 当前实现状态

- T1 已新增 `[discovery]` 配置并接入 `ClusterConf` 初始化；示例配置默认 `enabled = false`，不会影响现有部署。
- T2/T3 已新增 `crates/adapters/curvine-service-discovery`，包含公共 trait、key/value schema、`ServiceEndpoint`、`RegistrationGuard`、`SnapshotReader` 和测试模块内 fake resolver/registry。
- T4 已在 `etcd` feature 下实现 `EtcdServiceResolver` 的 `list` / `watch`，默认 feature 不拉入 `etcd-client` / `tonic`。
- T5 已实现 `ServiceRegistry` 的 etcd lease 注册、keepalive、状态更新和 `shutdown()` revoke；`update_status()` / `update_endpoint()` 均使用 `PutOptions::with_lease(lease_id)` 保留原 lease。
- T6 已实现 watch 断线后的 stale 标记、指数退避 + per-watch jitter 重连、compact 后全量 list + `Reset`、普通 transient 重连后的 stale 清除。
- 当前真实 etcd 集成测试由 `CURVINE_ETCD_ADDR` 控制；未设置时跳过，设置后可通过 `cargo test -p curvine-service-discovery --features etcd` 执行。

## 兼容性和迁移

- 默认关闭 discovery，不影响现有 Curvine 集群启动。
- 现有 master、worker、client 配置继续可用。
- 具体业务服务上线后，调用方可通过配置逐步开启对应服务发现。
- 如果需要灰度，可先部署服务实例并注册到 etcd，但不让调用方使用。
- 回滚时关闭 `[discovery].enabled` 即可绕过服务发现路径。

## 已确认决策

- 服务发现组件不负责业务分片，首个接入组件也不需要额外分片设计。
- 调用方访问目标服务时使用一致性 hash 选择 endpoint。
- 服务不需要独立健康检查 RPC，仅依赖 etcd lease 和业务 RPC 失败反馈。
- discovery 第一阶段不裁决服务升级期间的协议兼容性；endpoint value 发布 `ComponentInfoProto`，兼容性由既有 `CompatibilityPolicy` 在调用方 RPC/handshake 层处理。
- 不在 etcd 中维护 shard ownership、load 信息或 draining deadline。
