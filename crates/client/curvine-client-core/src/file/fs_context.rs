// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::block::{BlockClient, BlockClientPool};
use crate::file::{FsClient, MasterHandshake, WorkerPrecheck};
use crate::ClientMetrics;
use curvine_config::ClusterConf;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::RpcCode;
use curvine_io::CacheManager;
use curvine_io::IOResult;
use curvine_model::ProtoUtils;
use curvine_model::{ClientAddress, WorkerAddress, WorkerInfo};
use curvine_net::net::NetUtils;
use curvine_proto::{ClientAddressProto, ClientHeartbeatRequest, ClientHeartbeatResponse};
use curvine_rpc::client::{ClientConf, ClusterConnector};
use curvine_runtime::common::{TimeSpent, Utils};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_runtime::sync::FastRwLock;
use fxhash::FxHasher;
use log::{debug, info, warn};
use moka::policy::EvictionPolicy;
use moka::sync::{Cache, CacheBuilder};
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::future::Future;
use std::hash::BuildHasherDefault;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

static CLIENT_METRICS: OnceCell<ClientMetrics> = OnceCell::new();

// The core feature of the file system is thread-safe, which can be shared between multiple threads through Arc.
// 1. The cluster configuration file is saved.
// 2. Create client.
// 3. Perceive master switching.
pub struct FsContext {
    pub(crate) conf: ClusterConf,
    pub(crate) connector: Arc<ClusterConnector>,
    pub(crate) client_addr: ClientAddress,
    pub(crate) os_cache: CacheManager,
    pub(crate) failed_workers: Cache<u32, WorkerAddress, BuildHasherDefault<FxHasher>>,
    pub(crate) block_pool: Arc<BlockClientPool>,
    // Client-master handshake result: the master's advertised version /
    // protocol / capabilities (or a legacy marker when the master does not
    // advertise a compatibility contract). Populated by the first
    // GetFilesystemInfo call and shared by every FsClient clone. Stored
    // inline: FsContext itself is only ever shared behind an `Arc`, so the
    // interior-mutable FastRwLock needs no extra heap allocation.
    master_handshake: FastRwLock<MasterHandshake>,
    // Last-known live workers reported by the master on GetFilesystemInfo,
    // keyed by worker_id. Used by the T10 client-side worker pre-check to
    // evaluate a worker's structured version before opening a data connection.
    live_workers: FastRwLock<HashMap<u32, WorkerInfo>>,
    // Client-side worker pre-check (diagnose-only): evaluates a worker's
    // version/protocol/capabilities against the client's own supported range
    // plus the master-advertised bounds, and warns (never rejects) when a
    // worker looks incompatible.
    worker_precheck: FastRwLock<WorkerPrecheck>,
    // Whether this session has already reported its component_info to the
    // master. Handshake metadata is sent once per mount/session; the flag
    // keeps GetFilesystemInfo (which backs FUSE statfs and may be called
    // frequently) from carrying the payload on every request.
    handshake_reported: AtomicBool,
    // Guards the one-time lazy handshake: the first ordinary master RPC of a
    // session runs the client-master handshake first (best-effort), so every
    // client path (CLI, SDK, data-transfer, FUSE, direct
    // CurvineFileSystem/UnifiedFileSystem users) reports component_info and
    // caches the master's compatibility contract — not only FUSE mount.
    handshake_lock: tokio::sync::Mutex<()>,
    handshake_started: AtomicBool,
    // Whether the master acknowledged the most recent `ClientHeartbeat`. Starts
    // false and is cleared on any failure, so a lock is only stamped
    // session-owned while a lease is actually confirmed (#1878).
    session_active: AtomicBool,
}

impl FsContext {
    pub fn new(conf: impl Into<ClusterConf>) -> FsResult<Self> {
        let conf = conf.into();
        let rt = Arc::new(conf.client_rpc_conf().create_runtime());
        Self::with_rt(conf, rt)
    }

    pub fn with_rt(conf: impl Into<ClusterConf>, rt: Arc<Runtime>) -> FsResult<Self> {
        let conf = conf.into();
        let hostname = conf.client.hostname.to_owned();
        let ip = NetUtils::local_ip(&hostname);
        let client_addr = ClientAddress {
            client_name: Utils::uuid(),
            hostname,
            ip_addr: ip,
            port: 0,
        };

        CLIENT_METRICS
            .get_or_init(|| ClientMetrics::new(&conf.client.metadata_operation_buckets).unwrap());

        let connector = ClusterConnector::with_rt(conf.client_rpc_conf(), rt.clone());
        for node in conf.master_nodes() {
            connector.add_node(node)?;
        }

        let os_cache = CacheManager::new(
            conf.client.enable_read_ahead,
            conf.client.read_ahead_len,
            conf.client.drop_cache_len,
            conf.client.read_chunk_size as i64,
        );

        let exclude_workers = CacheBuilder::default()
            .time_to_live(Duration::from_millis(conf.client.failed_worker_ttl_ms))
            .eviction_policy(EvictionPolicy::lru())
            .build_with_hasher(BuildHasherDefault::<FxHasher>::default());

        let block_pool = Arc::new(BlockClientPool::new(
            conf.client.enable_block_conn_pool,
            conf.client.block_conn_idle_size,
            conf.client.block_conn_idle_time_ms,
        ));

        let context = Self {
            conf,
            connector: Arc::new(connector),
            client_addr,
            os_cache,
            failed_workers: exclude_workers,
            block_pool,
            master_handshake: FastRwLock::new(MasterHandshake::default()),
            live_workers: FastRwLock::new(HashMap::new()),
            worker_precheck: FastRwLock::new(WorkerPrecheck::default()),
            handshake_reported: AtomicBool::new(false),
            handshake_lock: tokio::sync::Mutex::new(()),
            handshake_started: AtomicBool::new(false),
            session_active: AtomicBool::new(false),
        };
        Ok(context)
    }

    pub fn clone_client_name(&self) -> String {
        self.client_addr.client_name.clone()
    }

    /// Cache the master's advertised version / protocol / capabilities from
    /// the client-master handshake. A master without a compatibility contract
    /// is recorded as legacy and never rejected. The worker pre-check bounds
    /// are refreshed from the same contract.
    pub fn set_master_handshake(&self, handshake: MasterHandshake) {
        self.worker_precheck.write().refresh(&handshake);
        *self.master_handshake.write() = handshake;
    }

    /// Cached master handshake. Defaults to a legacy peer before the first
    /// successful handshake so no component is rejected by default.
    pub fn master_handshake(&self) -> MasterHandshake {
        self.master_handshake.read().clone()
    }

    /// Atomically claim the one-time right to attach this client's
    /// `component_info` to a `GetFilesystemInfo` request. Returns `true` only
    /// for the first caller per session, so handshake metadata is reported
    /// once and frequent statfs queries stay lean. On a failed RPC the caller
    /// should call [`Self::reset_handshake_report`] so a later retry can
    /// still report.
    pub(crate) fn claim_handshake_report(&self) -> bool {
        !self.handshake_reported.swap(true, Ordering::Relaxed)
    }

    /// Allow a later `GetFilesystemInfo` call to report the handshake again
    /// (used when the first reporting request failed before reaching the
    /// master).
    pub(crate) fn reset_handshake_report(&self) {
        self.handshake_reported.store(false, Ordering::Relaxed);
    }

    /// Whether a `GetFilesystemInfo` request has already gone out this session
    /// (component_info reported once). Lets the lazy handshake skip re-running
    /// when the typed/bytes GetFilesystemInfo path already populated the
    /// cache.
    pub(crate) fn handshake_reported(&self) -> bool {
        self.handshake_reported.load(Ordering::Relaxed)
    }

    /// Lock guarding the one-time lazy handshake execution.
    pub(crate) fn handshake_lock(&self) -> &tokio::sync::Mutex<()> {
        &self.handshake_lock
    }

    /// Whether the one-time lazy handshake has already been attempted.
    pub(crate) fn handshake_started(&self) -> bool {
        self.handshake_started.load(Ordering::Relaxed)
    }

    /// Mark the one-time lazy handshake as attempted (used by the explicit
    /// `handshake()` so the first ordinary RPC does not re-run it).
    pub(crate) fn mark_handshake_started(&self) {
        self.handshake_started.store(true, Ordering::Relaxed);
    }

    pub fn conf(&self) -> &ClusterConf {
        &self.conf
    }

    pub fn clone_runtime(&self) -> Arc<Runtime> {
        self.connector.clone_runtime()
    }

    pub fn rt(&self) -> &Runtime {
        self.connector.rt()
    }

    pub fn is_local_worker(&self, addr: &WorkerAddress) -> bool {
        addr.is_local(&self.client_addr.hostname)
    }

    /// Replace the cached live-worker view reported by the master. Called on
    /// every successful GetFilesystemInfo so the T10 worker pre-check has the
    /// freshest worker versions before opening data connections.
    pub fn set_live_workers(&self, workers: &[WorkerInfo]) {
        let mut map = self.live_workers.write();
        map.clear();
        for worker in workers {
            map.insert(worker.worker_id(), worker.clone());
        }
    }

    /// Run the diagnose-only worker pre-check for a worker the client is about
    /// to open a data connection to. Uses the last-known version the master
    /// reported; workers the master has not reported (or legacy workers
    /// without `component_info`) are allowed silently and enforcement is left
    /// to the worker side. Never rejects — T10 acceptance: default is log /
    /// pre-check only. The worker is evaluated under the read guard so no
    /// `WorkerInfo` clone is needed on the connection hot path.
    fn precheck_worker(&self, addr: &WorkerAddress) {
        let workers = self.live_workers.read();
        let Some(worker) = workers.get(&addr.worker_id) else {
            return;
        };
        self.worker_precheck.read().warn_if_incompatible(worker);
    }

    pub async fn block_client(&self, addr: &WorkerAddress) -> IOResult<BlockClient> {
        // T10: pre-check the worker's version/protocol before opening a data
        // connection (diagnose-only; logs mismatches, never refuses).
        self.precheck_worker(addr);
        let client = self
            .connector
            .create_client(&addr.inet_addr(), false)
            .await?;
        Ok(BlockClient::new(client, addr.clone(), self))
    }

    pub async fn acquire_write(&self, addr: &WorkerAddress) -> IOResult<BlockClient> {
        self.block_pool.acquire_write(self, addr).await
    }

    pub async fn acquire_read(&self, addr: &WorkerAddress) -> IOResult<BlockClient> {
        self.block_pool.acquire_read(self, addr).await
    }

    pub fn read_chunk_size(&self) -> usize {
        self.conf.client.read_chunk_size
    }

    pub fn read_chunk_num(&self) -> usize {
        self.conf.client.read_chunk_num
    }

    pub fn read_parallel(&self) -> i64 {
        self.conf.client.read_parallel
    }

    pub fn read_since_size(&self) -> i64 {
        self.conf.client.read_slice_size
    }

    pub fn write_chunk_size(&self) -> usize {
        self.conf.client.write_chunk_size
    }

    pub fn write_chunk_num(&self) -> usize {
        self.conf.client.write_chunk_num
    }

    pub fn block_size(&self) -> i64 {
        self.conf.client.block_size
    }

    pub fn cluster_conf(&self) -> ClusterConf {
        self.conf.clone()
    }

    pub fn rpc_conf(&self) -> &ClientConf {
        self.connector.factory().conf()
    }

    pub fn clone_os_cache(&self) -> CacheManager {
        self.os_cache.clone()
    }

    pub fn get_metrics<'a>() -> &'a ClientMetrics {
        CLIENT_METRICS.get().expect("client get metrics error!")
    }

    pub async fn metrics_track<F, T>(operation: &'static str, future: F) -> FsResult<T>
    where
        F: Future<Output = FsResult<T>>,
    {
        let spent = TimeSpent::new();
        let result = future.await;
        Self::get_metrics()
            .metadata_operation_duration
            .with_label_values(&[operation])
            .observe(spent.used_us() as f64);
        result
    }

    // Exclude a worker
    pub fn add_failed_worker(&self, addr: &WorkerAddress) {
        self.failed_workers.insert(addr.worker_id, addr.clone())
    }

    pub fn is_failed_worker(&self, addr: &WorkerAddress) -> bool {
        self.failed_workers.contains_key(&addr.worker_id)
    }

    pub fn get_failed_workers(&self) -> Vec<u32> {
        let mut res = vec![];
        for item in self.failed_workers.iter() {
            res.push(item.1.worker_id);
        }

        res
    }

    pub fn client_addr_pb(&self) -> ClientAddressProto {
        ProtoUtils::client_address_to_pb(self.client_addr.clone())
    }

    pub fn exclude_workers(&self) -> Vec<u32> {
        self.failed_workers.iter().map(|x| x.1.worker_id).collect()
    }

    /// Heartbeat cadence this client agreed to, in milliseconds. `0` disables the
    /// client session lease entirely.
    pub fn heartbeat_interval_ms(&self) -> u64 {
        self.conf.client.heartbeat_interval_ms
    }

    /// Whether this client is configured to keep a master session alive.
    ///
    /// This is a config predicate only; use [`Self::session_active`] to learn
    /// whether a lease is actually confirmed.
    pub fn heartbeat_enabled(&self) -> bool {
        self.heartbeat_interval_ms() > 0
    }

    /// Whether the master acknowledged this client's most recent heartbeat.
    ///
    /// False before the first heartbeat completes, after any failed renewal, and
    /// against a master that does not support `ClientHeartbeat`.
    pub fn session_active(&self) -> bool {
        self.heartbeat_enabled() && self.session_active.load(Ordering::Acquire)
    }

    pub(crate) fn set_session_active(&self, active: bool) {
        self.session_active.store(active, Ordering::Release);
    }

    /// Whether a lock owned by `lock_client_id` can be reclaimed by session expiry.
    ///
    /// Only locks owned by this context's registered client id qualify, and only
    /// while the lease is confirmed. Every other lock (a synthetic owner such as
    /// `curvine-lancedb:<pid>`, or one taken before registration or after a
    /// failed renewal) keeps the legacy `lock_expire_time` path.
    pub fn is_session_owner(&self, lock_client_id: &str) -> bool {
        self.session_active() && lock_client_id == self.client_addr.client_name
    }

    /// Start the client session heartbeat task (T2 of #1867).
    ///
    /// The task holds only a `Weak` context and returns as soon as the last strong
    /// `Arc<FsContext>` is dropped, so a client that goes away stops renewing its
    /// lease and the master reclaims what it owned after
    /// `master.client_session_timeout`.
    ///
    /// Every client type inherits this by constructing `FsContext` through
    /// `CurvineFileSystem`, so FUSE, SDK, CLI and libsdk all report liveness
    /// without per-caller work. `client.heartbeat_interval_ms = 0` opts out.
    pub fn start_heartbeat_task(context: &Arc<FsContext>) {
        let interval_ms = context.heartbeat_interval_ms();
        if interval_ms == 0 {
            info!("client session heartbeat disabled (client.heartbeat_interval_ms = 0)");
            return;
        }

        let context = Arc::downgrade(context);
        let interval = Duration::from_millis(interval_ms);
        if let Some(strong_context) = context.upgrade() {
            strong_context.clone_runtime().spawn(async move {
                Self::run_heartbeat_task(context, interval).await;
            });
        }
    }

    async fn run_heartbeat_task(context: Weak<FsContext>, interval: Duration) {
        // The first tick fires immediately, but nothing orders it before the
        // caller's first lock: until the master acknowledges a heartbeat,
        // `session_active` stays false and locks keep the legacy TTL path.
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;

            let Some(context) = context.upgrade() else {
                return;
            };
            let result = Self::client_heartbeat(&context).await;
            context.set_session_active(result.is_ok());
            if let Err(e) = result {
                // A demoted master answers NotLeader while the client is already
                // discovering the new leader: expected during failover, so it must
                // not look like an error in the log.
                if matches!(e, FsError::NotLeaderMaster(_)) {
                    debug!("client session heartbeat: master is not active yet: {}", e);
                } else {
                    warn!("client session heartbeat: {}", e);
                }
            }
        }
    }

    async fn client_heartbeat(context: &Arc<FsContext>) -> FsResult<()> {
        let req = ClientHeartbeatRequest {
            client: context.client_addr_pb(),
            heartbeat_interval_ms: context.heartbeat_interval_ms(),
            pid: std::process::id().into(),
            mount_point: None,
            client_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        };
        let rep: ClientHeartbeatResponse = FsClient::new(context.clone())
            .rpc(RpcCode::ClientHeartbeat, req)
            .await?;

        if rep.new_session {
            info!(
                "registered client session {} with the master (lease {}ms, heartbeat {}ms)",
                context.client_addr.client_name,
                rep.session_timeout_ms,
                context.heartbeat_interval_ms()
            );
        }
        Ok(())
    }

    pub fn start_clean_task(context: &Arc<FsContext>, pool: Arc<BlockClientPool>) {
        let metric_report_enable = context.conf.client.metric_report_enable;
        let interval = Duration::from_millis(context.conf.client.clean_task_interval_ms);
        let context = Arc::downgrade(context);

        if let Some(strong_context) = context.upgrade() {
            strong_context.clone_runtime().spawn(async move {
                Self::run_clean_task(context, pool, metric_report_enable, interval).await;
            });
        }
    }

    async fn run_clean_task(
        context: Weak<FsContext>,
        pool: Arc<BlockClientPool>,
        metric_report_enable: bool,
        interval: std::time::Duration,
    ) {
        let mut interval = tokio::time::interval(interval);
        loop {
            interval.tick().await;

            pool.clear_idle_conn();

            if metric_report_enable {
                let Some(context) = context.upgrade() else {
                    return;
                };
                if let Err(e) = Self::metrics_report(&context).await {
                    warn!("metrics report: {}", e);
                }
            } else if context.strong_count() == 0 {
                return;
            }
        }
    }

    async fn metrics_report(context: &Arc<FsContext>) -> FsResult<()> {
        let metrics = ClientMetrics::encode()?;
        FsClient::new(context.clone()).metrics_report(metrics).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(heartbeat_interval_ms: u64) -> FsContext {
        let mut conf = ClusterConf::default();
        conf.client.heartbeat_interval_ms = heartbeat_interval_ms;
        FsContext::new(conf).unwrap()
    }

    #[test]
    fn lock_taken_before_the_first_heartbeat_is_not_session_owned() {
        // No polling for registration: right after construction no lease is
        // confirmed, so even this context's own locks stay on the legacy path.
        let ctx = context(12_000);
        let own = ctx.clone_client_name();
        assert!(!ctx.session_active());
        assert!(!ctx.is_session_owner(&own));
    }

    #[test]
    fn confirmed_lease_stamps_only_the_registered_client_id() {
        let ctx = context(12_000);
        let own = ctx.clone_client_name();
        ctx.set_session_active(true);

        assert!(ctx.is_session_owner(&own));
        // A synthetic owner (e.g. `curvine-lancedb:<pid>`) never registered a
        // session, so it must keep `lock_expire_time` reclamation.
        assert!(!ctx.is_session_owner("curvine-lancedb:42"));
    }

    #[test]
    fn failed_or_unsupported_renewal_reverts_to_the_legacy_path() {
        // The heartbeat task clears the flag on any error, including an old
        // master that does not know `ClientHeartbeat`.
        let ctx = context(12_000);
        let own = ctx.clone_client_name();
        ctx.set_session_active(true);
        ctx.set_session_active(false);
        assert!(!ctx.is_session_owner(&own));
    }

    #[test]
    fn disabled_heartbeat_never_claims_session_ownership() {
        let ctx = context(0);
        let own = ctx.clone_client_name();
        ctx.set_session_active(true);
        assert!(!ctx.is_session_owner(&own));
    }
}
