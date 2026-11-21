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

//! In-process transfer service.
//!
//! The embedded service runs the same store/planner/scheduler/RPC handler
//! components as the standalone `curvine-transfer` process, but without its own
//! RPC or web listener: the master routes transfer RPC codes to
//! [`TransferHandler`]. Only the in-memory store is supported, and the scheduler
//! is gated on the host process serving writes (leader *and* caught up with its
//! term) so that a standby, or a freshly elected leader that is still replaying
//! the inherited journal tail, never plans against a partial namespace.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use curvine_client_core::file::CurvineFileSystem;
use curvine_config::{ClusterConf, LoadTransferDefaults};
use curvine_core_error::CommonResult;
use curvine_error::FsError;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use log::{info, warn};
use parking_lot::Mutex;

use crate::common::UfsFactory;
use crate::transfer::scheduler::SchedulerRun;
use crate::transfer::{
    ClusterMetadataCache, MemoryTransferStore, TransferHandler, TransferMetrics, TransferPlanner,
    TransferScheduler, TransferService, TransferStoreBackend,
};

const MIN_SNAPSHOT_REFRESH: Duration = Duration::from_secs(1);
const MAX_SNAPSHOT_REFRESH: Duration = Duration::from_secs(10);
/// Lower bound on the refresh loop period. Without it a tiny
/// `cluster_snapshot_max_staleness` (half of it becomes the period) would turn
/// the loop into a sub-millisecond `GetMountTable` hammer against the master.
const MIN_SNAPSHOT_POLL: Duration = Duration::from_millis(100);
const LEADERSHIP_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// Upper bound on how long a demoted scheduler generation may take to finish
/// its in-flight tick before the supervisor gives up on waiting for it.
const SCHEDULER_STOP_TIMEOUT: Duration = Duration::from_secs(5);

fn snapshot_refresh_interval(mount_ttl: Duration, max_staleness: Duration) -> Duration {
    mount_ttl
        .clamp(MIN_SNAPSHOT_REFRESH, MAX_SNAPSHOT_REFRESH)
        .min(max_staleness / 2)
        .max(MIN_SNAPSHOT_POLL)
}

/// A scheduler generation as seen by the leadership supervisor.
///
/// Only the three things the supervisor needs are exposed, so the
/// promote/demote/re-promote state machine can be driven by a fake generation in
/// tests without a store, a planner or a real runtime.
trait SchedulerGeneration: Send + Sync + 'static {
    fn request_stop(&self);
    /// Whether [`Self::request_stop`] already happened on this generation.
    fn is_stopping(&self) -> bool;
    /// Waits for the worker loops to exit, giving up after `timeout`.
    fn wait_stopped(&self, timeout: Duration) -> impl Future<Output = bool> + Send;
}

impl SchedulerGeneration for SchedulerRun {
    fn request_stop(&self) {
        SchedulerRun::request_stop(self);
    }

    fn is_stopping(&self) -> bool {
        SchedulerRun::is_stopping(self)
    }

    fn wait_stopped(&self, timeout: Duration) -> impl Future<Output = bool> + Send {
        SchedulerRun::wait_stopped(self, timeout)
    }
}

/// Outcome of one supervisor poll over the scheduler slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotPoll {
    /// No generation, and the write gate is closed.
    Idle,
    /// A generation is running and the write gate is still open.
    Running,
    /// A new generation was started on this poll.
    Started,
    /// A draining generation exited and its slot was released.
    Drained,
    /// A draining generation outlived `stop_timeout`; its slot stays occupied
    /// and the next poll retries the wait.
    StillDraining,
}

/// One poll of the leadership supervisor's state machine.
///
/// The rules, in order:
///
/// 1. An empty slot with an open write gate starts a new generation.
/// 2. A generation that is draining (already asked to stop, or a closed gate)
///    is reaped *regardless* of the current gate: if the node starts serving
///    again before a demoted generation finished its last tick, forgetting about
///    it would either leave a serving master with a stopped scheduler or clear
///    the slot and let a second generation overlap the first.
/// 3. A generation that runs while the gate is open is left alone.
///
/// Only when the slot is empty *and* the gate is open can a replacement start,
/// so two generations never overlap and the slot is never cleared early.
async fn poll_scheduler_slot<G, F>(
    slot: &Mutex<Option<Arc<G>>>,
    serving: bool,
    stop_timeout: Duration,
    drain_warned: &AtomicBool,
    start: F,
) -> SlotPoll
where
    G: SchedulerGeneration,
    F: FnOnce() -> Arc<G>,
{
    let Some(run) = slot.lock().clone() else {
        if !serving {
            return SlotPoll::Idle;
        }
        let run = start();
        drain_warned.store(false, Ordering::Relaxed);
        *slot.lock() = Some(run);
        return SlotPoll::Started;
    };

    if serving && !run.is_stopping() {
        return SlotPoll::Running;
    }

    // Either the gate just closed under a running generation, or a previous
    // poll already asked this generation to stop and it is still draining.
    run.request_stop();
    if run.wait_stopped(stop_timeout).await {
        let mut slot = slot.lock();
        if slot
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &run))
        {
            *slot = None;
        }
        drop(slot);
        drain_warned.store(false, Ordering::Relaxed);
        return SlotPoll::Drained;
    }

    // Clearing the slot here would let the next promotion start a second
    // generation while this one still runs. Keep it occupied and retry.
    if !drain_warned.swap(true, Ordering::Relaxed) {
        warn!(
            "embedded transfer scheduler did not exit within {:?} after being asked to stop; keeping its slot and retrying on the next poll",
            stop_timeout
        );
    }
    SlotPoll::StillDraining
}

/// The embedded transfer service: everything the standalone server builds
/// except the RPC and web listeners, which belong to the host process.
#[derive(Clone)]
pub struct EmbeddedTransfer {
    service: TransferService<TransferStoreBackend>,
    cache: ClusterMetadataCache,
    scheduler: TransferScheduler<TransferStoreBackend>,
    rt: Arc<Runtime>,
    store_backend: &'static str,
    snapshot_refresh: Duration,
    is_serving: Arc<dyn Fn() -> bool + Send + Sync>,
    stop: Arc<AtomicBool>,
    started: Arc<AtomicBool>,
    /// The scheduler generation currently allowed to run, if this node serves
    /// writes. A generation carries both its stop flag and its completion
    /// signal: on demotion the supervisor sets the flag and waits for the
    /// workers to exit before handing the slot to a new generation, so two
    /// generations never run at the same time.
    scheduler_run: Arc<Mutex<Option<Arc<SchedulerRun>>>>,
    /// Set once a demoted generation exceeded [`SCHEDULER_STOP_TIMEOUT`] and the
    /// supervisor kept its slot occupied. The supervisor polls again, so it must
    /// not repeat the same warning on every poll; the flag is reset on each new
    /// generation (and whenever a draining generation finally exits).
    drain_warned: Arc<AtomicBool>,
}

impl EmbeddedTransfer {
    /// Builds the embedded transfer components. No background work starts until
    /// [`Self::start`] is called, so the host can finish binding its RPC server
    /// first.
    ///
    /// `is_serving` is polled on a dedicated runtime; the scheduler only runs
    /// while it returns true. Hosts pass the metadata write gate
    /// (`MasterMonitor::is_serving`), not raw leadership: a leader that has not
    /// yet applied an entry of its own term still has a partially replayed
    /// namespace.
    pub fn with_conf(
        conf: &ClusterConf,
        is_serving: impl Fn() -> bool + Send + Sync + 'static,
    ) -> CommonResult<Self> {
        if !conf.transfer.is_embedded() {
            return Err(FsError::common(
                "embedded transfer requires transfer.mode=embedded (no transfer.endpoints)",
            )
            .into());
        }

        // Dedicated runtime: the scheduler and metadata refresh loops must never
        // compete with the master's request runtime.
        let rt = Arc::new(conf.transfer_server_conf().create_runtime());
        let _ = TransferMetrics::get()?;

        let store = Arc::new(TransferStoreBackend::Memory(MemoryTransferStore::new()));
        let report_endpoints: Vec<String> = conf
            .master_nodes()
            .into_iter()
            .map(|node| node.to_string())
            .collect();
        info!(
            "configuring embedded curvine-transfer store_backend={} report_endpoints={:?} max_running={} max_tasks={} task_stale_timeout_ms={}",
            store.backend_label(),
            report_endpoints,
            conf.transfer.max_running_transfers,
            conf.transfer.max_tasks_per_transfer,
            conf.transfer.task_stale_timeout.as_millis(),
        );

        let fs = CurvineFileSystem::with_rt(conf.clone(), rt.clone())?;
        let factory = Arc::new(UfsFactory::with_rt(&conf.client, rt.clone()));
        let cache = ClusterMetadataCache::with_snapshot_policy(
            fs.clone(),
            conf.transfer.cluster_snapshot_max_staleness,
            conf.transfer.allow_submit_with_stale_snapshot,
        );
        let owner = if conf.transfer.instance_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            conf.transfer.instance_id.clone()
        };
        let store_backend = store.backend_label();
        let snapshot_refresh = snapshot_refresh_interval(
            Duration::from_millis(conf.client.mount_update_ttl_ms),
            conf.transfer.cluster_snapshot_max_staleness,
        );
        let planner = TransferPlanner::new(
            fs,
            factory.clone(),
            cache.clone(),
            conf.client.clone(),
            conf.transfer.max_tasks_per_transfer,
            conf.transfer.ufs_max_concurrency_per_endpoint,
            LoadTransferDefaults::from_job(&conf.job),
        );
        let scheduler = TransferScheduler::new(
            store.clone(),
            planner,
            cache.clone(),
            factory,
            owner,
            report_endpoints,
            conf.transfer.clone(),
        );
        let service = TransferService::with_cache_and_report_queue(
            store,
            cache.clone(),
            conf.transfer.task_stale_timeout,
            conf.transfer.task_report_queue_size(),
            conf.transfer.worker_threads,
        )?;

        Ok(Self {
            service,
            cache,
            scheduler,
            rt,
            store_backend,
            snapshot_refresh,
            is_serving: Arc::new(is_serving),
            stop: Arc::new(AtomicBool::new(false)),
            started: Arc::new(AtomicBool::new(false)),
            scheduler_run: Arc::new(Mutex::new(None)),
            drain_warned: Arc::new(AtomicBool::new(false)),
        })
    }

    /// RPC handler used by the host server to route transfer RPC codes.
    pub fn handler(&self) -> TransferHandler<TransferStoreBackend> {
        TransferHandler::new(self.service.clone())
    }

    /// Runtime owned by this embedded service (scheduler, metadata refresh,
    /// transfer client).
    ///
    /// The host dispatches transfer RPCs on this runtime's blocking pool: a
    /// submit may issue loopback metadata RPCs (`GetMountTable`,
    /// `GetFilesystemInfo`, `GetFileStatus`) back to the very master that serves
    /// it, so it must not occupy a master blocking thread while it waits for
    /// one.
    pub fn runtime(&self) -> &Runtime {
        &self.rt
    }

    pub fn store_backend_label(&self) -> &'static str {
        self.store_backend
    }

    /// Live worker count in this instance's own cluster snapshot, or `None`
    /// before the first successful refresh.
    ///
    /// `transfer_cluster_snapshot_live_workers` cannot answer this: the metric
    /// registry is process-global, so every embedded transfer in the same
    /// process writes the same gauge. Callers that must know what *this*
    /// scheduler can dispatch to (the transfer worker-removal barrier in tests,
    /// for instance) use this accessor instead.
    pub fn snapshot_live_workers(&self) -> Option<usize> {
        self.cache.snapshot_live_workers()
    }

    /// Starts the cluster snapshot refresh loop and the write-gate supervisor.
    /// Idempotent.
    pub fn start(&self) {
        if self.started.swap(true, Ordering::SeqCst) {
            return;
        }
        self.cache
            .clone()
            .start_refresh_loop(self.snapshot_refresh, self.stop.clone());

        let this = self.clone();
        self.rt.spawn(async move {
            this.supervise_leadership().await;
        });
        info!("embedded curvine-transfer started");
    }

    /// Stops the scheduler and background loops. Used by hosts that shut down
    /// without dropping the process. Idempotent; the scheduler generation is
    /// asked to stop but not awaited, so a shutdown hook never blocks.
    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(run) = self.scheduler_run.lock().as_ref() {
            run.request_stop();
        }
    }

    async fn supervise_leadership(&self) {
        loop {
            if self.stop.load(Ordering::Relaxed) {
                break;
            }
            let serving = (self.is_serving)();
            let scheduler = self.scheduler.clone();
            let rt = self.rt.clone();
            let outcome = poll_scheduler_slot(
                &self.scheduler_run,
                serving,
                SCHEDULER_STOP_TIMEOUT,
                &self.drain_warned,
                || Arc::new(scheduler.start(rt, Arc::new(AtomicBool::new(false)))),
            )
            .await;
            match outcome {
                SlotPoll::Started => {
                    if self.stop.load(Ordering::Relaxed) {
                        // shutdown() raced with this promotion; the exit path
                        // below stops whatever generation is still in the slot.
                        if let Some(run) = self.scheduler_run.lock().as_ref() {
                            run.request_stop();
                        }
                    } else {
                        info!(
                            "embedded transfer scheduler started: local master is serving writes"
                        );
                    }
                }
                SlotPoll::Drained => {
                    info!(
                        "embedded transfer scheduler stopped: the previous generation finished draining"
                    );
                    if serving {
                        // The gate is open again, so do not leave the serving
                        // master without a scheduler for a whole poll period.
                        continue;
                    }
                }
                SlotPoll::Idle | SlotPoll::Running | SlotPoll::StillDraining => {}
            }
            tokio::time::sleep(LEADERSHIP_POLL_INTERVAL).await;
        }
        if let Some(run) = self.scheduler_run.lock().take() {
            run.request_stop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        poll_scheduler_slot, snapshot_refresh_interval, SchedulerGeneration, SlotPoll,
        MAX_SNAPSHOT_REFRESH, MIN_SNAPSHOT_POLL, MIN_SNAPSHOT_REFRESH,
    };
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn snapshot_refresh_clamps_the_mount_ttl() {
        assert_eq!(
            snapshot_refresh_interval(Duration::from_secs(600), Duration::from_secs(600)),
            MAX_SNAPSHOT_REFRESH
        );
        assert_eq!(
            snapshot_refresh_interval(Duration::from_millis(1), Duration::from_secs(600)),
            MIN_SNAPSHOT_REFRESH
        );
    }

    /// A tiny staleness bound halves into the period; the floor keeps the
    /// refresh loop from hammering `GetMountTable`.
    #[test]
    fn snapshot_refresh_has_a_floor() {
        assert_eq!(
            snapshot_refresh_interval(Duration::from_millis(1_000), Duration::from_millis(1)),
            MIN_SNAPSHOT_POLL
        );
    }

    /// Stand-in for [`super::SchedulerRun`]: records whether it was asked to
    /// stop and only reports completion once the test releases it. Until then
    /// `wait_stopped` burns the whole timeout and reports failure, exactly like a
    /// generation stuck in a long tick.
    struct FakeGeneration {
        id: usize,
        stopping: Arc<AtomicBool>,
        finished: Arc<AtomicBool>,
    }

    impl FakeGeneration {
        fn new(id: usize) -> Arc<Self> {
            Arc::new(Self {
                id,
                stopping: Arc::new(AtomicBool::new(false)),
                finished: Arc::new(AtomicBool::new(false)),
            })
        }
    }

    impl SchedulerGeneration for FakeGeneration {
        fn request_stop(&self) {
            self.stopping.store(true, Ordering::SeqCst);
        }

        fn is_stopping(&self) -> bool {
            self.stopping.load(Ordering::SeqCst)
        }

        fn wait_stopped(
            &self,
            timeout: Duration,
        ) -> impl std::future::Future<Output = bool> + Send {
            let finished = self.finished.clone();
            async move {
                if finished.load(Ordering::SeqCst) {
                    return true;
                }
                tokio::time::sleep(timeout).await;
                finished.load(Ordering::SeqCst)
            }
        }
    }

    /// promote → demote (drain outlives the stop timeout) → re-promote.
    ///
    /// The critical case: the gate reopens while the demoted generation is still
    /// finishing its last tick. The old generation must still be reaped, no
    /// replacement may start while it drains, and once it is gone a serving
    /// master must get a new generation instead of staying scheduler-less.
    #[tokio::test]
    async fn draining_generation_is_reaped_after_repromotion() {
        const STOP_TIMEOUT: Duration = Duration::from_millis(20);

        let slot: Mutex<Option<Arc<FakeGeneration>>> = Mutex::new(None);
        let drain_warned = AtomicBool::new(false);
        let started: Mutex<Vec<Arc<FakeGeneration>>> = Mutex::new(Vec::new());
        let start = |id: usize| {
            let run = FakeGeneration::new(id);
            started.lock().push(run.clone());
            run
        };
        let must_not_start = || -> Arc<FakeGeneration> {
            panic!("a replacement started while the previous generation was still draining")
        };

        // 1. Promote: the write gate opens.
        let outcome =
            poll_scheduler_slot(&slot, true, STOP_TIMEOUT, &drain_warned, || start(0)).await;
        assert_eq!(outcome, SlotPoll::Started);
        let gen0 = slot
            .lock()
            .clone()
            .expect("a promoted node has a generation");
        assert_eq!(gen0.id, 0);
        assert!(!gen0.is_stopping());

        // 2. Demote with a generation that is slow to drain.
        let outcome =
            poll_scheduler_slot(&slot, false, STOP_TIMEOUT, &drain_warned, must_not_start).await;
        assert_eq!(outcome, SlotPoll::StillDraining);
        assert!(gen0.is_stopping());
        assert!(
            slot.lock()
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &gen0)),
            "the draining generation must keep its slot"
        );
        assert!(
            drain_warned.load(Ordering::SeqCst),
            "the timeout must be reported once"
        );

        // 3. Re-promote while the old generation still drains: the drain is
        //    retried even though the gate is open again, and nothing new starts.
        let outcome =
            poll_scheduler_slot(&slot, true, STOP_TIMEOUT, &drain_warned, must_not_start).await;
        assert_eq!(outcome, SlotPoll::StillDraining);
        assert!(
            slot.lock()
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &gen0)),
            "the re-promotion must not clear the draining generation's slot"
        );
        assert!(
            drain_warned.load(Ordering::SeqCst),
            "the warning is not repeated on every poll"
        );
        assert_eq!(
            started.lock().len(),
            1,
            "no second generation while the first still runs"
        );

        // 4. The old generation finally exits: its slot is released.
        gen0.finished.store(true, Ordering::SeqCst);
        let outcome =
            poll_scheduler_slot(&slot, true, STOP_TIMEOUT, &drain_warned, must_not_start).await;
        assert_eq!(outcome, SlotPoll::Drained);
        assert!(slot.lock().is_none(), "the drained slot is released");
        assert!(!drain_warned.load(Ordering::SeqCst));

        // 5. Serving with an empty slot: a replacement starts, and it is a new
        //    generation rather than the drained one.
        let outcome =
            poll_scheduler_slot(&slot, true, STOP_TIMEOUT, &drain_warned, || start(1)).await;
        assert_eq!(outcome, SlotPoll::Started);
        let gen1 = slot
            .lock()
            .clone()
            .expect("the serving node runs a scheduler");
        assert_eq!(gen1.id, 1);
        assert!(!Arc::ptr_eq(&gen0, &gen1));
        assert!(!gen1.is_stopping());
        assert_eq!(started.lock().len(), 2);

        // 6. A generation that drains before the timeout is released at once and
        //    the gate staying closed keeps the slot empty.
        gen1.finished.store(true, Ordering::SeqCst);
        let outcome =
            poll_scheduler_slot(&slot, false, STOP_TIMEOUT, &drain_warned, must_not_start).await;
        assert_eq!(outcome, SlotPoll::Drained);
        assert!(slot.lock().is_none());
        assert_eq!(
            poll_scheduler_slot(&slot, false, STOP_TIMEOUT, &drain_warned, must_not_start).await,
            SlotPoll::Idle
        );
    }
}
