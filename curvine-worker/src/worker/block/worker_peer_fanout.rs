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

use crate::worker::block::BlockMeta;
use curvine_core_error::{err_box, CommonResult};
use curvine_error::{ErrorKind, FsError, FsResult};
use curvine_model::HeartbeatStatus;
use curvine_net::net::NodeAddr;
use curvine_proto::{
    BlockReportInfoProto, BlockReportListRequest, BlockReportStatusProto, WorkerHeartbeatRequest,
};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use log::warn;
use prost::Message;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::{watch, Notify, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, timeout_at, Instant};

/// A worker RPC that is mirrored to one non-leader master peer.
#[derive(Debug, Clone)]
pub(crate) enum PeerMirrorRequest {
    Heartbeat(Box<WorkerHeartbeatRequest>),
    BlockReport(BlockReportListRequest),
}

impl PeerMirrorRequest {
    fn encoded_len(&self) -> usize {
        match self {
            Self::Heartbeat(request) => request.encoded_len(),
            Self::BlockReport(request) => request.encoded_len(),
        }
    }

    fn heartbeat_status(&self) -> Option<HeartbeatStatus> {
        match self {
            Self::Heartbeat(request) => Some(HeartbeatStatus::from(request.status)),
            Self::BlockReport(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FullReportSnapshot {
    cluster_id: String,
    worker_id: u32,
    worker_session_id: String,
    report_id: u64,
    blocks: Arc<Vec<BlockMeta>>,
    batch_size: usize,
    _inventory_permit: Option<Arc<BytePermit>>,
}

impl FullReportSnapshot {
    pub(crate) fn new(
        cluster_id: String,
        worker_id: u32,
        worker_session_id: String,
        report_id: u64,
        blocks: Vec<BlockMeta>,
        batch_size: usize,
    ) -> Self {
        Self {
            cluster_id,
            worker_id,
            worker_session_id,
            report_id,
            blocks: Arc::new(blocks),
            batch_size: batch_size.max(1),
            _inventory_permit: None,
        }
    }

    pub(crate) fn minimum_inventory_bytes(block_count: usize) -> usize {
        block_count
            .saturating_mul(std::mem::size_of::<BlockMeta>())
            .saturating_add(std::mem::size_of::<Vec<BlockMeta>>())
            .saturating_add(2 * std::mem::size_of::<usize>())
    }

    fn retained_inventory_bytes(&self) -> usize {
        self.blocks
            .capacity()
            .saturating_mul(std::mem::size_of::<BlockMeta>())
            .saturating_add(std::mem::size_of::<Vec<BlockMeta>>())
            .saturating_add(2 * std::mem::size_of::<usize>())
    }

    fn with_inventory_permit(mut self, permit: BytePermit) -> Self {
        self._inventory_permit = Some(Arc::new(permit));
        self
    }

    fn descriptor_bytes(cluster_id_len: usize, worker_session_id_len: usize) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(cluster_id_len)
            .saturating_add(worker_session_id_len)
    }

    fn batch_count(&self) -> usize {
        self.blocks.len().max(1).div_ceil(self.batch_size)
    }

    fn batch_request(&self, index: usize) -> BlockReportListRequest {
        let start = index.saturating_mul(self.batch_size);
        let end = (start + self.batch_size).min(self.blocks.len());
        let batch = &self.blocks[start..end];
        let mut request = BlockReportListRequest {
            cluster_id: self.cluster_id.clone(),
            worker_id: self.worker_id,
            full_report: true,
            total_len: self.blocks.len() as u64,
            report_id: Some(self.report_id),
            worker_session_id: Some(self.worker_session_id.clone()),
            blocks: Vec::with_capacity(batch.len()),
        };
        request.blocks.extend(batch.iter().map(|block| {
            let status = match block.state {
                crate::worker::block::BlockState::Finalized => BlockReportStatusProto::Finalized,
                crate::worker::block::BlockState::Writing
                | crate::worker::block::BlockState::Recovering
                | crate::worker::block::BlockState::Allocating
                | crate::worker::block::BlockState::Finalizing => BlockReportStatusProto::Writing,
            };
            BlockReportInfoProto {
                id: block.id,
                status: status.into(),
                block_size: block.len,
                storage_type: block.storage_type.into(),
            }
        }));
        request
    }
}

/// Transport seam for the worker's per-peer sender. The production
/// implementation delegates to FsClient's direct-peer API; tests can block or
/// fail one peer without blocking other peers or the primary leader request.
#[async_trait::async_trait]
pub(crate) trait PeerMirrorTransport: Send + Sync {
    fn master_peer_snapshot(&self) -> Vec<NodeAddr>;
    fn master_peer_leader_id(&self) -> Option<u64>;
    async fn send_to_peer(&self, peer_id: u64, request: PeerMirrorRequest) -> FsResult<()>;
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct WorkerPeerFanoutConfig {
    pub queue_max_ops: usize,
    pub queued_bytes_max: usize,
    pub max_peer_queues: usize,
    pub max_concurrency: usize,
    pub retry_timeout_ms: u64,
    pub retry_min_ms: u64,
    pub retry_max_ms: u64,
}

impl From<&curvine_config::WorkerConf> for WorkerPeerFanoutConfig {
    fn from(conf: &curvine_config::WorkerConf) -> Self {
        Self {
            queue_max_ops: conf.follower_report_queue_max_ops,
            queued_bytes_max: conf.follower_report_queue_max_bytes,
            max_peer_queues: conf.follower_report_max_peer_queues,
            max_concurrency: conf.follower_report_max_concurrency,
            retry_timeout_ms: conf.follower_report_retry_timeout_ms,
            retry_min_ms: conf.follower_report_retry_min_ms,
            retry_max_ms: conf.follower_report_retry_max_ms,
        }
    }
}

impl WorkerPeerFanoutConfig {
    fn validate(self) -> FsResult<()> {
        if self.queue_max_ops == 0
            || self.queue_max_ops > 4096
            || self.queued_bytes_max == 0
            || self.queued_bytes_max > 1024 * 1024 * 1024
            || self.max_peer_queues == 0
            || self.max_peer_queues > 32
            || self.max_concurrency == 0
            || self.max_concurrency > 32
            || self.retry_timeout_ms == 0
            || self.retry_timeout_ms > 300_000
            || self.retry_min_ms == 0
            || self.retry_min_ms > self.retry_timeout_ms
            || self.retry_max_ms < self.retry_min_ms
            || self.retry_max_ms > 60_000
        {
            return err_box!("invalid follower worker report fanout limits");
        }
        Ok(())
    }

    fn retry_timeout(self) -> Duration {
        Duration::from_millis(self.retry_timeout_ms)
    }

    fn retry_min(self) -> Duration {
        Duration::from_millis(self.retry_min_ms)
    }

    fn retry_max(self) -> Duration {
        Duration::from_millis(self.retry_max_ms)
    }
}

/// Owns bounded independent queues for each follower peer. Each task sends one
/// peer's requests in order, while the worker's leader RPC path only performs
/// a nonblocking enqueue after it has received the authoritative response.
pub(crate) struct WorkerPeerFanout {
    runtime: Arc<Runtime>,
    transport: Arc<dyn PeerMirrorTransport>,
    config: WorkerPeerFanoutConfig,
    peers: Mutex<HashMap<u64, PeerTask>>,
    latest_start: Mutex<Option<(WorkerHeartbeatRequest, BytePermit)>>,
    has_running: AtomicBool,
    global_bytes: Arc<GlobalByteBudget>,
    rpc_semaphore: Arc<Semaphore>,
    omitted_peers: Mutex<HashSet<u64>>,
    publication_gate: Mutex<()>,
    resync_retry_after: Mutex<Option<Instant>>,
    initial_full_report_active: AtomicBool,
    shutdown: AtomicBool,
}

struct PeerTask {
    addr: NodeAddr,
    mailbox: Arc<PeerMailbox>,
    stop_tx: watch::Sender<bool>,
    join: Option<JoinHandle<()>>,
}

struct PeerMailbox {
    state: Mutex<PeerMailboxState>,
    notify: Notify,
    max_ops: usize,
    global_bytes: Arc<GlobalByteBudget>,
}

struct PeerMailboxState {
    queue: VecDeque<QueuedRequest>,
    queued_ops: usize,
    next_sequence: u64,
    latest_running: Option<(u64, QueuedRequest)>,
    start: Option<QueuedRequest>,
    end: Option<QueuedRequest>,
    stopped: bool,
    needs_resync: bool,
    resync_in_progress: bool,
    resync_generation: u64,
}

struct QueuedRequest {
    sequence: u64,
    payload: QueuedPayload,
    _byte_permit: BytePermit,
    resync_generation: Option<u64>,
}

#[derive(Debug, Clone)]
enum QueuedPayload {
    Rpc(PeerMirrorRequest),
    FullReport(FullReportSnapshot),
}

impl QueuedPayload {
    fn encoded_len(&self) -> usize {
        match self {
            Self::Rpc(request) => request.encoded_len(),
            // The shared inventory is charged once through its byte permit;
            // each peer queue charges this descriptor, not every encoded batch.
            Self::FullReport(snapshot) => FullReportSnapshot::descriptor_bytes(
                snapshot.cluster_id.len(),
                snapshot.worker_session_id.len(),
            ),
        }
    }

    fn heartbeat_status(&self) -> Option<HeartbeatStatus> {
        match self {
            Self::Rpc(request) => request.heartbeat_status(),
            Self::FullReport(_) => None,
        }
    }

    fn is_end(&self) -> bool {
        self.heartbeat_status() == Some(HeartbeatStatus::End)
    }
}

#[derive(Debug)]
struct GlobalByteBudget {
    used: AtomicUsize,
    limit: usize,
}

#[derive(Debug)]
struct BytePermit {
    budget: Arc<GlobalByteBudget>,
    bytes: usize,
}

impl Drop for BytePermit {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

impl GlobalByteBudget {
    fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicUsize::new(0),
            limit,
        })
    }

    fn try_acquire(self: &Arc<Self>, bytes: usize) -> Option<BytePermit> {
        if bytes > self.limit {
            return None;
        }
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let next = used.checked_add(bytes)?;
            if next > self.limit {
                return None;
            }
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return Some(BytePermit {
                        budget: self.clone(),
                        bytes,
                    });
                }
                Err(current) => used = current,
            }
        }
    }

    fn remaining(&self) -> usize {
        self.limit.saturating_sub(self.used.load(Ordering::Acquire))
    }
}

impl WorkerPeerFanout {
    pub(crate) fn new(
        runtime: Arc<Runtime>,
        transport: Arc<dyn PeerMirrorTransport>,
        config: WorkerPeerFanoutConfig,
    ) -> FsResult<Arc<Self>> {
        config.validate()?;
        Ok(Arc::new(Self {
            runtime,
            transport,
            config,
            peers: Mutex::new(HashMap::new()),
            latest_start: Mutex::new(None),
            has_running: AtomicBool::new(false),
            global_bytes: GlobalByteBudget::new(config.queued_bytes_max),
            rpc_semaphore: Arc::new(Semaphore::new(config.max_concurrency)),
            omitted_peers: Mutex::new(HashSet::new()),
            publication_gate: Mutex::new(()),
            resync_retry_after: Mutex::new(None),
            initial_full_report_active: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
        }))
    }

    /// Publish a request only after the primary leader call succeeded. A
    /// missing leader hint skips this cycle to avoid sending the same
    /// authoritative request to every master.
    pub(crate) fn publish_heartbeat(&self, request: WorkerHeartbeatRequest) {
        if self.shutdown.load(Ordering::Acquire) {
            return;
        }
        let _publication = (HeartbeatStatus::from(request.status) == HeartbeatStatus::Start)
            .then(|| lock(&self.publication_gate));
        match HeartbeatStatus::from(request.status) {
            HeartbeatStatus::Start => self.remember_latest_start(request.clone()),
            HeartbeatStatus::Running => self.has_running.store(true, Ordering::Release),
            HeartbeatStatus::End => {}
        }

        let Some(leader_id) = self.transport.master_peer_leader_id() else {
            return;
        };
        let selected = self.reconcile_peers(leader_id);
        let is_end = HeartbeatStatus::from(request.status) == HeartbeatStatus::End;
        for (peer, mailbox, created) in selected {
            if created && !is_end {
                match self.latest_start_request() {
                    Some(start) if HeartbeatStatus::from(start.status) != HeartbeatStatus::End => {
                        mailbox.enqueue_control_start(start);
                    }
                    Some(_) => {}
                    None if HeartbeatStatus::from(request.status) == HeartbeatStatus::Running => {
                        let mut start = request.clone();
                        start.status = HeartbeatStatus::Start.into();
                        mailbox.enqueue_control_start(start);
                    }
                    None if HeartbeatStatus::from(request.status) == HeartbeatStatus::Start => {
                        mailbox.enqueue_control_start(request.clone());
                    }
                    None => {}
                }
            }

            if is_end {
                mailbox.enqueue_end(request.clone());
            } else if !(created && HeartbeatStatus::from(request.status) == HeartbeatStatus::Start)
            {
                mailbox.enqueue(PeerMirrorRequest::Heartbeat(Box::new(request.clone())));
            }
            let _ = peer;
        }
    }

    /// Queue a block report only after the authoritative leader accepted it.
    /// Reports stay FIFO with worker Start and lifecycle heartbeats per peer.
    pub(crate) fn publish_block_report(&self, request: BlockReportListRequest) {
        if self.shutdown.load(Ordering::Acquire) {
            return;
        }
        let _publication = lock(&self.publication_gate);
        let Some(leader_id) = self.transport.master_peer_leader_id() else {
            return;
        };
        for (_, mailbox, created) in self.reconcile_peers(leader_id) {
            if created {
                if let Some(start) = self.latest_start_request() {
                    mailbox.enqueue_control_start(start);
                }
            }
            mailbox.enqueue(PeerMirrorRequest::BlockReport(request.clone()));
        }
    }

    pub(crate) fn needs_full_resync(&self) -> bool {
        lock(&self.peers)
            .values()
            .any(|peer| peer.mailbox.needs_resync())
    }

    #[cfg(test)]
    fn has_stale_peer_state(&self) -> bool {
        lock(&self.peers)
            .values()
            .any(|peer| peer.mailbox.is_stale())
    }

    pub(crate) fn begin_initial_full_report(&self) {
        self.initial_full_report_active
            .store(true, Ordering::Release);
    }

    pub(crate) fn end_initial_full_report(&self) {
        self.initial_full_report_active
            .store(false, Ordering::Release);
    }

    /// Rebuild only peers whose bounded queues lost an update. The snapshot is
    /// the same local block metadata used for the worker's normal full report,
    /// streamed in bounded batches. Its shared inventory metadata is charged
    /// once to the global byte budget and released after all peer queues drop it.
    /// Oversized inventories leave peers stale; follower reads then fall back
    /// to the leader. Capacity failures are retried at the configured maximum
    /// retry interval to avoid repeatedly rebuilding the inventory.
    pub(crate) fn resync_with_snapshot<F>(&self, build_reports: F) -> CommonResult<()>
    where
        F: FnOnce(usize) -> CommonResult<Option<FullReportSnapshot>>,
    {
        if self.shutdown.load(Ordering::Acquire) {
            return Ok(());
        }
        let _publication = lock(&self.publication_gate);
        if !self.needs_full_resync() {
            return Ok(());
        }
        let Some(leader_id) = self.transport.master_peer_leader_id() else {
            return Ok(());
        };
        let Some(mut start) = self.latest_start_request() else {
            warn!("cannot resync follower worker state before a successful Start heartbeat");
            return Ok(());
        };
        if lock(&self.resync_retry_after).is_some_and(|deadline| Instant::now() < deadline) {
            return Ok(());
        }
        start.status = HeartbeatStatus::Start.into();
        let stale_peers: Vec<_> = self
            .reconcile_peers(leader_id)
            .into_iter()
            .filter(|(_, mailbox, _)| mailbox.needs_resync())
            .collect();
        if stale_peers.is_empty() {
            return Ok(());
        }

        // The inventory is shared by every stale peer, so budget its metadata
        // once. Reserve enough additional space for one Start and one snapshot
        // descriptor per selected queue before asking the store to build it.
        let start_bytes = PeerMirrorRequest::Heartbeat(Box::new(start.clone())).encoded_len();
        let session_id_len = start.worker_session_id.as_ref().map_or(0, String::len);
        let per_peer_bytes = start_bytes.saturating_add(FullReportSnapshot::descriptor_bytes(
            start.cluster_id.len(),
            session_id_len,
        ));
        let queue_overhead = per_peer_bytes.saturating_mul(stale_peers.len());
        let Some(max_inventory_bytes) = self.global_bytes.remaining().checked_sub(queue_overhead)
        else {
            warn!("follower full resync cannot fit its per-peer queue descriptors; follower state remains stale and reads use the leader");
            self.defer_resync();
            return Ok(());
        };

        let snapshot = match build_reports(max_inventory_bytes) {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                warn!("follower full resync inventory exceeds the remaining worker byte budget; follower state remains stale and reads use the leader");
                self.defer_resync();
                return Ok(());
            }
            Err(error) => {
                self.defer_resync();
                return Err(error);
            }
        };
        let inventory_bytes = snapshot.retained_inventory_bytes();
        if inventory_bytes > max_inventory_bytes {
            warn!(
                "follower full resync inventory requires {} bytes, budget is {}; follower state remains stale and reads use the leader",
                inventory_bytes, max_inventory_bytes
            );
            self.defer_resync();
            return Ok(());
        }
        let Some(inventory_permit) = self.global_bytes.try_acquire(inventory_bytes) else {
            warn!("follower full resync inventory no longer fits the worker byte budget; follower state remains stale and reads use the leader");
            self.defer_resync();
            return Ok(());
        };
        let snapshot = snapshot.with_inventory_permit(inventory_permit);

        if self.transport.master_peer_leader_id() != Some(leader_id) {
            self.defer_resync();
            return Ok(());
        }
        for (_, mailbox, _) in stale_peers {
            mailbox.enqueue_resync(start.clone(), snapshot.clone());
        }
        *lock(&self.resync_retry_after) = None;
        Ok(())
    }

    fn defer_resync(&self) {
        *lock(&self.resync_retry_after) = Some(Instant::now() + self.config.retry_max());
    }

    fn reconcile_peers(&self, leader_id: u64) -> Vec<(u64, Arc<PeerMailbox>, bool)> {
        let mut peers = self.transport.master_peer_snapshot();
        peers.sort_by_key(|peer| peer.id);
        peers.retain(|peer| peer.id != leader_id);

        let omitted: HashSet<u64> = peers
            .iter()
            .skip(self.config.max_peer_queues)
            .map(|peer| peer.id)
            .collect();
        {
            let mut previous = lock(&self.omitted_peers);
            if *previous != omitted {
                if !omitted.is_empty() {
                    warn!(
                        "worker follower-report queue cap {} reached; omitting master peers {:?}; omitted peers will not receive worker state and must refuse follower reads",
                        self.config.max_peer_queues,
                        omitted
                    );
                }
                *previous = omitted;
            }
        }
        peers.truncate(self.config.max_peer_queues);

        let new_peers_need_snapshot = self.initial_full_report_active.load(Ordering::Acquire)
            || self.has_running.load(Ordering::Acquire);

        let desired: HashMap<u64, NodeAddr> =
            peers.into_iter().map(|peer| (peer.id, peer)).collect();

        let mut removed = Vec::new();
        let mut selected = Vec::with_capacity(desired.len());
        {
            let mut current = lock(&self.peers);
            if self.shutdown.load(Ordering::Acquire) {
                return Vec::new();
            }
            let obsolete: Vec<u64> = current
                .iter()
                .filter_map(|(id, task)| {
                    desired
                        .get(id)
                        .filter(|peer| **peer == task.addr)
                        .is_none()
                        .then_some(*id)
                })
                .collect();
            for id in obsolete {
                if let Some(mut task) = current.remove(&id) {
                    let _ = task.stop_tx.send(true);
                    removed.push(task.join.take());
                }
            }

            for (id, addr) in desired {
                if let Some(task) = current.get(&id) {
                    selected.push((id, task.mailbox.clone(), false));
                } else {
                    let (stop_tx, stop_rx) = watch::channel(false);
                    let mailbox = Arc::new(PeerMailbox::new(
                        self.config.queue_max_ops,
                        self.global_bytes.clone(),
                    ));
                    if new_peers_need_snapshot {
                        let mut state = lock(&mailbox.state);
                        mailbox.mark_resync_locked(&mut state);
                    }
                    let join = self.runtime.spawn(run_peer_queue(
                        id,
                        mailbox.clone(),
                        self.transport.clone(),
                        stop_rx,
                        self.rpc_semaphore.clone(),
                        self.config,
                    ));
                    current.insert(
                        id,
                        PeerTask {
                            addr,
                            mailbox: mailbox.clone(),
                            stop_tx,
                            join: Some(join),
                        },
                    );
                    selected.push((id, mailbox, true));
                }
            }
        }
        // The stop signal is sent before removal. Join handles are deliberately
        // detached here: reconciliation runs on the worker's heartbeat caller
        // and must never block it on an unreachable peer.
        drop(removed);
        selected
    }

    fn remember_latest_start(&self, request: WorkerHeartbeatRequest) {
        let mut latest = lock(&self.latest_start);
        // The cached request is retained for peers discovered later, so include
        // it in the same global byte budget as queued and in-flight requests.
        *latest = None;
        let bytes = request.encoded_len();
        if let Some(permit) = self.global_bytes.try_acquire(bytes) {
            *latest = Some((request, permit));
        } else {
            warn!(
                "follower Start heartbeat exceeds the configured global queued-byte limit; new follower queues will remain stale"
            );
        }
    }

    fn latest_start_request(&self) -> Option<WorkerHeartbeatRequest> {
        lock(&self.latest_start)
            .as_ref()
            .map(|(request, _)| request.clone())
    }

    /// Send graceful End to current peers and await their queues. This method
    /// runs from the worker's external shutdown hook, after its primary End
    /// request has been sent to the leader.
    pub(crate) async fn shutdown(&self) {
        if self.shutdown.swap(true, Ordering::AcqRel) {
            return;
        }
        let tasks = {
            let mut peers = lock(&self.peers);
            peers
                .drain()
                .map(|(_, mut task)| {
                    let state = lock(&task.mailbox.state);
                    let has_real_end = state.end.is_some() || state.stopped;
                    drop(state);
                    (task.stop_tx, task.mailbox, task.join.take(), has_real_end)
                })
                .collect::<Vec<_>>()
        };
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut joins = Vec::new();
        let mut stops = Vec::new();
        for (stop_tx, mailbox, join, has_real_end) in tasks {
            if !has_real_end {
                let _ = stop_tx.send(true);
            } else {
                mailbox.notify.notify_one();
            }
            stops.push(stop_tx);
            if let Some(join) = join {
                joins.push(join);
            }
        }
        for join in &mut joins {
            if timeout_at(deadline, join).await.is_err() {
                warn!("timed out waiting for follower worker End during shutdown");
                for stop_tx in &stops {
                    let _ = stop_tx.send(true);
                }
                break;
            }
        }
    }
}

impl PeerMailbox {
    fn new(max_ops: usize, global_bytes: Arc<GlobalByteBudget>) -> Self {
        Self {
            state: Mutex::new(PeerMailboxState {
                queue: VecDeque::new(),
                queued_ops: 0,
                next_sequence: 0,
                latest_running: None,
                start: None,
                end: None,
                stopped: false,
                needs_resync: false,
                resync_in_progress: false,
                resync_generation: 0,
            }),
            notify: Notify::new(),
            max_ops,
            global_bytes,
        }
    }

    fn enqueue_control_start(&self, request: WorkerHeartbeatRequest) {
        let mut state = lock(&self.state);
        // Replace an unconsumed Start before reserving bytes so repeated
        // registration heartbeats cannot temporarily exceed the global cap.
        state.start = None;
        let payload = QueuedPayload::Rpc(PeerMirrorRequest::Heartbeat(Box::new(request)));
        let Some(permit) = self.global_bytes.try_acquire(payload.encoded_len()) else {
            self.mark_resync_locked(&mut state);
            drop(state);
            warn!("follower Start heartbeat does not fit the configured global queued-byte limit");
            self.notify.notify_one();
            return;
        };
        state.next_sequence = state.next_sequence.saturating_add(1);
        let sequence = state.next_sequence;
        state.start = Some(QueuedRequest {
            sequence,
            payload,
            _byte_permit: permit,
            resync_generation: None,
        });
        drop(state);
        self.notify.notify_one();
    }

    fn enqueue_end(&self, request: WorkerHeartbeatRequest) {
        let mut state = lock(&self.state);
        state.queue.clear();
        state.queued_ops = 0;
        state.latest_running = None;
        state.start = None;
        state.end = None;
        state.needs_resync = false;
        state.resync_in_progress = false;
        let payload = QueuedPayload::Rpc(PeerMirrorRequest::Heartbeat(Box::new(request)));
        let Some(permit) = self.global_bytes.try_acquire(payload.encoded_len()) else {
            state.stopped = true;
            drop(state);
            warn!("follower End heartbeat does not fit the configured global queued-byte limit");
            self.notify.notify_one();
            return;
        };
        state.next_sequence = state.next_sequence.saturating_add(1);
        let sequence = state.next_sequence;
        state.end = Some(QueuedRequest {
            sequence,
            payload,
            _byte_permit: permit,
            resync_generation: None,
        });
        drop(state);
        self.notify.notify_one();
    }

    fn enqueue(&self, request: PeerMirrorRequest) {
        if let Some(status) = request.heartbeat_status() {
            if status == HeartbeatStatus::Start {
                if let PeerMirrorRequest::Heartbeat(request) = request {
                    self.enqueue_control_start(*request);
                }
                return;
            }
            if status == HeartbeatStatus::End {
                if let PeerMirrorRequest::Heartbeat(request) = request {
                    self.enqueue_end(*request);
                }
                return;
            }
        }

        let is_running = request.heartbeat_status() == Some(HeartbeatStatus::Running);
        let bytes = request.encoded_len();
        let mut state = lock(&self.state);
        if state.needs_resync && is_running {
            return;
        }
        if state.needs_resync && !state.resync_in_progress {
            return;
        }
        state.next_sequence = state.next_sequence.saturating_add(1);
        let sequence = state.next_sequence;
        if is_running {
            // Running heartbeats coalesce to one slot. Release an older value
            // before reserving its replacement so a full budget cannot keep
            // a stale heartbeat indefinitely.
            state.latest_running = None;
            let permit = self.global_bytes.try_acquire(bytes);
            if let Some(permit) = permit {
                state.latest_running = Some((
                    sequence,
                    QueuedRequest {
                        sequence,
                        payload: QueuedPayload::Rpc(request),
                        _byte_permit: permit,
                        resync_generation: None,
                    },
                ));
                drop(state);
                self.notify.notify_one();
            } else {
                self.mark_resync_locked(&mut state);
                drop(state);
                self.notify.notify_one();
            }
            return;
        }

        let permit = self.global_bytes.try_acquire(bytes);
        if state.queued_ops >= self.max_ops || permit.is_none() {
            self.mark_resync_locked(&mut state);
            drop(state);
            self.notify.notify_one();
            return;
        }

        state.queued_ops += 1;
        state.queue.push_back(QueuedRequest {
            sequence,
            payload: QueuedPayload::Rpc(request),
            _byte_permit: permit.unwrap(),
            resync_generation: None,
        });
        drop(state);
        self.notify.notify_one();
    }

    fn enqueue_resync(&self, start: WorkerHeartbeatRequest, snapshot: FullReportSnapshot) {
        let mut state = lock(&self.state);
        if !state.needs_resync || state.resync_in_progress {
            return;
        }
        self.mark_resync_locked(&mut state);
        let generation = state.resync_generation;
        state.resync_in_progress = true;
        let start_payload = QueuedPayload::Rpc(PeerMirrorRequest::Heartbeat(Box::new(start)));
        let Some(start_permit) = self.global_bytes.try_acquire(start_payload.encoded_len()) else {
            self.mark_resync_locked(&mut state);
            drop(state);
            warn!(
                "follower full resync Start does not fit the configured global queued-byte limit"
            );
            self.notify.notify_one();
            return;
        };
        state.next_sequence = state.next_sequence.saturating_add(1);
        let start_sequence = state.next_sequence;
        state.start = Some(QueuedRequest {
            sequence: start_sequence,
            payload: start_payload,
            _byte_permit: start_permit,
            resync_generation: None,
        });
        let payload = QueuedPayload::FullReport(snapshot);
        let Some(permit) = self.global_bytes.try_acquire(payload.encoded_len()) else {
            self.mark_resync_locked(&mut state);
            drop(state);
            warn!("follower full resync snapshot does not fit the configured global queued-byte limit");
            self.notify.notify_one();
            return;
        };
        if state.queued_ops >= self.max_ops {
            drop(permit);
            self.mark_resync_locked(&mut state);
            drop(state);
            warn!("follower full resync exceeds the configured per-peer queue operation limit");
            self.notify.notify_one();
            return;
        }
        state.next_sequence = state.next_sequence.saturating_add(1);
        let sequence = state.next_sequence;
        state.queued_ops += 1;
        state.queue.push_back(QueuedRequest {
            sequence,
            payload,
            _byte_permit: permit,
            resync_generation: Some(generation),
        });
        drop(state);
        self.notify.notify_one();
    }

    fn mark_resync_locked(&self, state: &mut PeerMailboxState) {
        state.resync_generation = state.resync_generation.wrapping_add(1);
        state.needs_resync = true;
        state.resync_in_progress = false;
        state.queue.clear();
        state.queued_ops = 0;
        state.latest_running = None;
        state.start = None;
    }

    fn needs_resync(&self) -> bool {
        let state = lock(&self.state);
        state.needs_resync && !state.resync_in_progress
    }

    #[cfg(test)]
    fn is_stale(&self) -> bool {
        lock(&self.state).needs_resync
    }

    fn mark_send_failed(&self) {
        let mut state = lock(&self.state);
        self.mark_resync_locked(&mut state);
    }

    async fn recv(&self, stop_rx: &mut watch::Receiver<bool>) -> Option<QueuedRequest> {
        loop {
            if *stop_rx.borrow() {
                return None;
            }
            let notified = self.notify.notified();
            if let Some(request) = {
                let mut state = lock(&self.state);
                if let Some(end) = state.end.take() {
                    state.stopped = true;
                    Some(end)
                } else if let Some(start) = state.start.take() {
                    Some(start)
                } else {
                    let queued_sequence = state.queue.front().map(|entry| entry.sequence);
                    let running_sequence =
                        state.latest_running.as_ref().map(|(sequence, _)| *sequence);
                    match (queued_sequence, running_sequence) {
                        (Some(queued), Some(running)) if running < queued => {
                            let (_, request) = state.latest_running.take().unwrap();
                            Some(request)
                        }
                        (Some(_), _) => {
                            let entry = state.queue.pop_front().unwrap();
                            state.queued_ops = state.queued_ops.saturating_sub(1);
                            Some(entry)
                        }
                        (None, Some(_)) => {
                            let (_, request) = state.latest_running.take().unwrap();
                            Some(request)
                        }
                        (None, None) if state.stopped => return None,
                        (None, None) => None,
                    }
                }
            } {
                return Some(request);
            }

            tokio::select! {
                _ = notified => {},
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        return None;
                    }
                }
            }
        }
    }
}

async fn run_peer_queue(
    peer_id: u64,
    mailbox: Arc<PeerMailbox>,
    transport: Arc<dyn PeerMirrorTransport>,
    mut stop_rx: watch::Receiver<bool>,
    rpc_semaphore: Arc<Semaphore>,
    config: WorkerPeerFanoutConfig,
) {
    loop {
        let Some(request) = mailbox.recv(&mut stop_rx).await else {
            return;
        };
        let is_end = request.payload.is_end();
        let result = send_payload(
            transport.as_ref(),
            peer_id,
            request.payload.clone(),
            config,
            &rpc_semaphore,
            &mut stop_rx,
        )
        .await;
        if let Err(error) = result {
            warn!(
                "follower worker heartbeat to master peer {} failed: {}",
                peer_id, error
            );
            mailbox.mark_send_failed();
        } else if let Some(generation) = request.resync_generation {
            let mut state = lock(&mailbox.state);
            if state.resync_generation == generation {
                state.needs_resync = false;
                state.resync_in_progress = false;
            }
        }
        if is_end {
            return;
        }
    }
}

async fn send_payload(
    transport: &dyn PeerMirrorTransport,
    peer_id: u64,
    payload: QueuedPayload,
    config: WorkerPeerFanoutConfig,
    rpc_semaphore: &Arc<Semaphore>,
    stop_rx: &mut watch::Receiver<bool>,
) -> FsResult<()> {
    match payload {
        QueuedPayload::Rpc(request) => {
            send_one(transport, peer_id, request, config, rpc_semaphore, stop_rx).await
        }
        QueuedPayload::FullReport(snapshot) => {
            for index in 0..snapshot.batch_count() {
                let request = PeerMirrorRequest::BlockReport(snapshot.batch_request(index));
                send_one(transport, peer_id, request, config, rpc_semaphore, stop_rx).await?;
            }
            Ok(())
        }
    }
}

async fn send_one(
    transport: &dyn PeerMirrorTransport,
    peer_id: u64,
    request: PeerMirrorRequest,
    config: WorkerPeerFanoutConfig,
    rpc_semaphore: &Arc<Semaphore>,
    stop_rx: &mut watch::Receiver<bool>,
) -> FsResult<()> {
    if *stop_rx.borrow() {
        return Ok(());
    }
    let permit = tokio::select! {
        permit = rpc_semaphore.clone().acquire_owned() => match permit {
            Ok(permit) => permit,
            Err(_) => return err_box!("follower mirror sender stopped"),
        },
        changed = stop_rx.changed() => {
            if changed.is_err() || *stop_rx.borrow() {
                return Ok(());
            }
            return Ok(());
        }
    };
    let result = send_with_retry(transport, peer_id, request, config, stop_rx).await;
    drop(permit);
    result
}

async fn send_with_retry(
    transport: &dyn PeerMirrorTransport,
    peer_id: u64,
    request: PeerMirrorRequest,
    config: WorkerPeerFanoutConfig,
    stop_rx: &mut watch::Receiver<bool>,
) -> FsResult<()> {
    let deadline = Instant::now()
        .checked_add(config.retry_timeout())
        .unwrap_or_else(Instant::now);
    let mut delay = config.retry_min();
    loop {
        if *stop_rx.borrow() {
            return Ok(());
        }
        let send = transport.send_to_peer(peer_id, request.clone());
        let result = tokio::select! {
            result = timeout(deadline.saturating_duration_since(Instant::now()), send) => {
                match result {
                    Ok(result) => result,
                    Err(_) => Err(FsError::from(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "follower mirror RPC timed out",
                    ))),
                }
            },
            changed = stop_rx.changed() => {
                if changed.is_err() || *stop_rx.borrow() {
                    return Ok(());
                }
                continue;
            }
        };
        match result {
            Ok(()) => return Ok(()),
            Err(error) if retryable(&error) && Instant::now() < deadline => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                tokio::select! {
                    _ = sleep(delay.min(remaining)) => {},
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            return Ok(());
                        }
                    }
                }
                delay = (delay * 2).min(config.retry_max());
            }
            Err(error) => return Err(error),
        }
    }
}

fn retryable(error: &FsError) -> bool {
    // A follower's ResyncRequired response is terminal for this queued update:
    // fail the peer immediately so the heartbeat task can build a bounded full
    // snapshot instead of retrying the stale report until retry_timeout_ms.
    let message = error.to_string();
    if message.starts_with("ResyncRequired:") {
        return false;
    }

    matches!(error.kind(), ErrorKind::IO | ErrorKind::Timeout)
        || (matches!(error.kind(), ErrorKind::InProgress) && message.starts_with("NotRegistered:"))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::Notify;
    use tokio::time::timeout;

    #[derive(Default)]
    struct FakePeerTransport {
        peers: Mutex<Vec<NodeAddr>>,
        leader: AtomicU64,
        calls: Mutex<Vec<(u64, PeerCall)>>,
        slow_peer: AtomicU64,
        slow_blocked: AtomicBool,
        slow_entered: Notify,
        slow_release: Notify,
        resync_required_on_running: AtomicBool,
    }

    #[derive(Debug, Clone, Copy, PartialEq)]
    enum PeerCall {
        Heartbeat(HeartbeatStatus),
        BlockReport { full_report: bool },
    }

    #[async_trait::async_trait]
    impl PeerMirrorTransport for FakePeerTransport {
        fn master_peer_snapshot(&self) -> Vec<NodeAddr> {
            lock(&self.peers).clone()
        }

        fn master_peer_leader_id(&self) -> Option<u64> {
            let leader = self.leader.load(Ordering::Acquire);
            (leader != 0).then_some(leader)
        }

        async fn send_to_peer(&self, peer_id: u64, request: PeerMirrorRequest) -> FsResult<()> {
            let call = match request {
                PeerMirrorRequest::Heartbeat(request) => {
                    PeerCall::Heartbeat(HeartbeatStatus::from(request.status))
                }
                PeerMirrorRequest::BlockReport(request) => PeerCall::BlockReport {
                    full_report: request.full_report,
                },
            };
            lock(&self.calls).push((peer_id, call));
            if call == PeerCall::Heartbeat(HeartbeatStatus::Running)
                && self.resync_required_on_running.load(Ordering::Acquire)
            {
                return Err(FsError::in_progress_msg(
                    "ResyncRequired: worker must send a full report",
                ));
            }
            if self.slow_peer.load(Ordering::Acquire) == peer_id
                && !self.slow_blocked.swap(true, Ordering::AcqRel)
            {
                self.slow_entered.notify_one();
                self.slow_release.notified().await;
            }
            Ok(())
        }
    }

    fn heartbeat(status: HeartbeatStatus) -> WorkerHeartbeatRequest {
        WorkerHeartbeatRequest {
            status: status.into(),
            worker_id: 7,
            worker_session_id: Some("worker-session-7".to_string()),
            ..Default::default()
        }
    }

    fn config() -> WorkerPeerFanoutConfig {
        WorkerPeerFanoutConfig {
            queue_max_ops: 8,
            queued_bytes_max: 1024 * 1024,
            max_peer_queues: 8,
            max_concurrency: 2,
            retry_timeout_ms: 100,
            retry_min_ms: 1,
            retry_max_ms: 5,
        }
    }

    fn transport() -> Arc<FakePeerTransport> {
        Arc::new(FakePeerTransport {
            peers: Mutex::new(vec![
                NodeAddr::new(1, "master-1", 8995),
                NodeAddr::new(2, "master-2", 8995),
            ]),
            leader: AtomicU64::new(1),
            ..Default::default()
        })
    }

    #[test]
    fn slow_peer_does_not_block_primary_enqueue_or_other_peers() {
        let runtime = Arc::new(Runtime::new("peer-fanout-slow", 2, 2));
        runtime.block_on(async {
            let transport = transport();
            transport.slow_peer.store(2, Ordering::Release);
            *lock(&transport.peers) = vec![
                NodeAddr::new(1, "master-1", 8995),
                NodeAddr::new(2, "master-2", 8995),
                NodeAddr::new(3, "master-3", 8995),
            ];
            let fanout =
                WorkerPeerFanout::new(runtime.clone(), transport.clone(), config()).unwrap();

            let started = Instant::now();
            fanout.publish_heartbeat(heartbeat(HeartbeatStatus::Start));
            assert!(started.elapsed() < Duration::from_millis(100));
            timeout(Duration::from_secs(1), transport.slow_entered.notified())
                .await
                .unwrap();
            timeout(Duration::from_secs(1), async {
                loop {
                    if lock(&transport.calls).iter().any(|(peer_id, call)| {
                        *peer_id == 3 && *call == PeerCall::Heartbeat(HeartbeatStatus::Start)
                    }) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();

            transport.slow_release.notify_one();
            fanout.shutdown().await;
        });
    }

    #[test]
    fn start_heartbeats_are_charged_to_the_global_byte_budget() {
        let runtime = Arc::new(Runtime::new("peer-fanout-start-budget", 2, 2));
        runtime.block_on(async {
            let transport = transport();
            transport.slow_peer.store(2, Ordering::Release);
            let request = heartbeat(HeartbeatStatus::Start);
            let request_bytes =
                PeerMirrorRequest::Heartbeat(Box::new(request.clone())).encoded_len();
            let mut limits = config();
            // One copy is retained as the latest Start; the other is in flight
            // for peer 2.
            limits.queued_bytes_max = request_bytes * 2;
            let fanout = WorkerPeerFanout::new(runtime.clone(), transport.clone(), limits).unwrap();

            fanout.publish_heartbeat(request);
            timeout(Duration::from_secs(1), transport.slow_entered.notified())
                .await
                .unwrap();

            assert_eq!(
                fanout.global_bytes.used.load(Ordering::Acquire),
                limits.queued_bytes_max
            );

            transport.slow_release.notify_one();
            timeout(Duration::from_secs(1), async {
                while fanout.global_bytes.used.load(Ordering::Acquire) != request_bytes {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            fanout.shutdown().await;
        });
    }

    #[test]
    fn resync_required_signal_marks_peer_stale_without_retrying() {
        let runtime = Arc::new(Runtime::new("peer-fanout-resync-signal", 2, 2));
        runtime.block_on(async {
            let transport = transport();
            let fanout =
                WorkerPeerFanout::new(runtime.clone(), transport.clone(), config()).unwrap();

            fanout.publish_heartbeat(heartbeat(HeartbeatStatus::Start));
            timeout(Duration::from_secs(1), async {
                loop {
                    if lock(&transport.calls)
                        .iter()
                        .any(|(_, call)| *call == PeerCall::Heartbeat(HeartbeatStatus::Start))
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();

            transport
                .resync_required_on_running
                .store(true, Ordering::Release);
            fanout.publish_heartbeat(heartbeat(HeartbeatStatus::Running));
            timeout(Duration::from_secs(1), async {
                while !fanout.needs_full_resync() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();

            let running_calls = lock(&transport.calls)
                .iter()
                .filter(|(_, call)| *call == PeerCall::Heartbeat(HeartbeatStatus::Running))
                .count();
            assert_eq!(running_calls, 1, "resync signal must not use retry timeout");
            fanout.shutdown().await;
        });
    }

    #[test]
    fn dropped_running_heartbeat_marks_peer_stale_for_resync() {
        let runtime = Arc::new(Runtime::new("peer-fanout-running-overflow", 2, 2));
        runtime.block_on(async {
            let transport = transport();
            let start = heartbeat(HeartbeatStatus::Start);
            let start_bytes = PeerMirrorRequest::Heartbeat(Box::new(start.clone())).encoded_len();
            let mut report = block_report(1);
            report
                .blocks
                .extend((2..=32).map(|id| curvine_proto::BlockReportInfoProto {
                    id,
                    ..Default::default()
                }));
            let report_bytes = PeerMirrorRequest::BlockReport(report.clone()).encoded_len();
            let mut limits = config();
            assert!(report_bytes >= start_bytes);
            limits.queued_bytes_max = start_bytes + report_bytes;
            let fanout = WorkerPeerFanout::new(runtime.clone(), transport.clone(), limits).unwrap();

            fanout.publish_heartbeat(start);
            wait_for_calls(&transport, 1).await;

            transport.slow_peer.store(2, Ordering::Release);
            transport.slow_blocked.store(false, Ordering::Release);
            fanout.publish_block_report(report);
            timeout(Duration::from_secs(1), transport.slow_entered.notified())
                .await
                .unwrap();

            fanout.publish_heartbeat(heartbeat(HeartbeatStatus::Running));
            assert!(fanout.needs_full_resync());

            transport.slow_release.notify_one();
            timeout(Duration::from_secs(1), async {
                while fanout.global_bytes.used.load(Ordering::Acquire) != start_bytes {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            fanout.shutdown().await;
        });
    }

    #[test]
    fn newly_added_peer_receives_start_and_full_snapshot_before_running() {
        let runtime = Arc::new(Runtime::new("peer-fanout-new", 2, 2));
        runtime.block_on(async {
            let transport = transport();
            let fanout =
                WorkerPeerFanout::new(runtime.clone(), transport.clone(), config()).unwrap();
            fanout.publish_heartbeat(heartbeat(HeartbeatStatus::Start));
            wait_for_calls(&transport, 1).await;

            *lock(&transport.peers) = vec![
                NodeAddr::new(1, "master-1", 8995),
                NodeAddr::new(2, "master-2", 8995),
                NodeAddr::new(3, "master-3", 8995),
            ];
            fanout.publish_heartbeat(heartbeat(HeartbeatStatus::Running));
            assert!(fanout.needs_full_resync());
            fanout
                .resync_with_snapshot(|_| Ok(Some(snapshot())))
                .unwrap();
            timeout(Duration::from_secs(1), async {
                loop {
                    if !fanout.has_stale_peer_state() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            fanout.publish_heartbeat(heartbeat(HeartbeatStatus::Running));
            timeout(Duration::from_secs(1), async {
                loop {
                    let ready = {
                        let calls = lock(&transport.calls);
                        let new_peer: Vec<_> = calls
                            .iter()
                            .filter(|(peer, _)| *peer == 3)
                            .map(|(_, status)| *status)
                            .collect();
                        if let Some(full_report_index) = new_peer
                            .iter()
                            .position(|call| *call == PeerCall::BlockReport { full_report: true })
                        {
                            assert!(full_report_index > 0);
                            assert!(new_peer[..full_report_index]
                                .iter()
                                .all(|call| *call == PeerCall::Heartbeat(HeartbeatStatus::Start)));
                            if new_peer.len() > full_report_index + 1 {
                                assert_eq!(
                                    new_peer[full_report_index + 1],
                                    PeerCall::Heartbeat(HeartbeatStatus::Running)
                                );
                                true
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    };
                    if ready {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            fanout.shutdown().await;
        });
    }

    #[test]
    fn cached_leader_is_never_mirrored_and_changed_leader_is_removed() {
        let runtime = Arc::new(Runtime::new("peer-fanout-leader", 2, 2));
        runtime.block_on(async {
            let transport = transport();
            let fanout =
                WorkerPeerFanout::new(runtime.clone(), transport.clone(), config()).unwrap();
            fanout.publish_heartbeat(heartbeat(HeartbeatStatus::Start));
            wait_for_calls(&transport, 1).await;

            transport.leader.store(2, Ordering::Release);
            fanout.publish_heartbeat(heartbeat(HeartbeatStatus::Running));
            wait_for_calls(&transport, 2).await;
            {
                let calls = lock(&transport.calls);
                assert_eq!(calls.iter().filter(|(peer, _)| *peer == 2).count(), 1);
                assert!(calls.iter().any(|(peer, call)| *peer == 1
                    && *call == PeerCall::Heartbeat(HeartbeatStatus::Start)));
            }
            fanout.shutdown().await;
        });
    }

    #[test]
    fn graceful_shutdown_sends_only_the_real_end_heartbeat() {
        let runtime = Arc::new(Runtime::new("peer-fanout-end", 2, 2));
        runtime.block_on(async {
            let transport = transport();
            let fanout =
                WorkerPeerFanout::new(runtime.clone(), transport.clone(), config()).unwrap();
            fanout.publish_heartbeat(heartbeat(HeartbeatStatus::Start));
            wait_for_calls(&transport, 1).await;
            fanout.publish_heartbeat(heartbeat(HeartbeatStatus::End));
            fanout.shutdown().await;

            let calls = lock(&transport.calls);
            assert!(calls.iter().any(
                |(peer, call)| *peer == 2 && *call == PeerCall::Heartbeat(HeartbeatStatus::End)
            ));
            assert_eq!(
                calls
                    .iter()
                    .filter(|(_, call)| *call == PeerCall::Heartbeat(HeartbeatStatus::End))
                    .count(),
                1
            );
        });
    }

    #[test]
    fn peer_becoming_leader_during_inflight_mirror_is_not_retried_as_follower() {
        let runtime = Arc::new(Runtime::new("peer-fanout-role-change", 2, 2));
        runtime.block_on(async {
            let transport = transport();
            transport.slow_peer.store(2, Ordering::Release);
            let fanout =
                WorkerPeerFanout::new(runtime.clone(), transport.clone(), config()).unwrap();
            fanout.publish_heartbeat(heartbeat(HeartbeatStatus::Start));
            timeout(Duration::from_secs(1), transport.slow_entered.notified())
                .await
                .unwrap();

            transport.leader.store(2, Ordering::Release);
            fanout.publish_heartbeat(heartbeat(HeartbeatStatus::Running));
            timeout(Duration::from_secs(1), async {
                loop {
                    if lock(&transport.calls).iter().any(|(peer, call)| {
                        *peer == 1 && *call == PeerCall::Heartbeat(HeartbeatStatus::Start)
                    }) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();

            assert_eq!(
                lock(&transport.calls)
                    .iter()
                    .filter(|(peer, _)| *peer == 2)
                    .count(),
                1,
                "the old follower request may finish, but no further RPC targets the new leader"
            );
            fanout.shutdown().await;
        });
    }

    #[test]
    fn overflow_marks_peer_stale_and_full_resync_restores_it() {
        let runtime = Arc::new(Runtime::new("peer-fanout-overflow", 2, 2));
        runtime.block_on(async {
            let transport = transport();
            transport.slow_peer.store(2, Ordering::Release);
            let mut limits = config();
            limits.queue_max_ops = 1;
            let fanout = WorkerPeerFanout::new(runtime.clone(), transport.clone(), limits).unwrap();
            fanout.publish_heartbeat(heartbeat(HeartbeatStatus::Start));
            wait_for_calls(&transport, 1).await;
            transport.slow_entered.notified().await;

            fanout.publish_block_report(block_report(1));
            fanout.publish_block_report(block_report(2));
            fanout.publish_block_report(block_report(3));
            assert!(fanout.needs_full_resync());
            fanout.publish_heartbeat(heartbeat(HeartbeatStatus::Running));

            fanout
                .resync_with_snapshot(|_| Ok(Some(snapshot())))
                .unwrap();
            transport.slow_release.notify_one();
            timeout(Duration::from_secs(1), async {
                loop {
                    if !fanout.has_stale_peer_state() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            fanout.publish_heartbeat(heartbeat(HeartbeatStatus::Running));
            timeout(Duration::from_secs(1), async {
                loop {
                    if lock(&transport.calls).iter().any(|(peer, call)| {
                        *peer == 2 && *call == PeerCall::Heartbeat(HeartbeatStatus::Running)
                    }) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            fanout.shutdown().await;
        });
    }

    #[test]
    fn oversized_resync_inventory_is_retried_at_a_bounded_rate() {
        let runtime = Arc::new(Runtime::new("peer-fanout-resync-budget", 2, 2));
        runtime.block_on(async {
            let transport = transport();
            let mut limits = config();
            limits.retry_timeout_ms = 1_000;
            limits.retry_max_ms = 1_000;
            let fanout = WorkerPeerFanout::new(runtime.clone(), transport.clone(), limits).unwrap();
            fanout.publish_heartbeat(heartbeat(HeartbeatStatus::Start));
            wait_for_calls(&transport, 1).await;
            lock(&fanout.peers)
                .get(&2)
                .unwrap()
                .mailbox
                .mark_send_failed();

            let builds = Arc::new(AtomicUsize::new(0));
            let build_count = builds.clone();
            fanout
                .resync_with_snapshot(move |_| {
                    build_count.fetch_add(1, Ordering::SeqCst);
                    Ok(None)
                })
                .unwrap();
            fanout
                .resync_with_snapshot(|_| panic!("resync capacity failure must be rate limited"))
                .unwrap();

            assert_eq!(builds.load(Ordering::SeqCst), 1);
            assert!(fanout.has_stale_peer_state());
            fanout.shutdown().await;
        });
    }

    async fn wait_for_calls(transport: &FakePeerTransport, count: usize) {
        timeout(Duration::from_secs(1), async {
            loop {
                if lock(&transport.calls).len() >= count {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    fn block_report(id: i64) -> BlockReportListRequest {
        BlockReportListRequest {
            worker_id: 7,
            worker_session_id: Some("worker-session-7".to_string()),
            blocks: vec![curvine_proto::BlockReportInfoProto {
                id,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn full_resync_batches_keep_the_worker_registration_session() {
        assert_eq!(
            snapshot().batch_request(0).worker_session_id.as_deref(),
            Some("worker-session-7")
        );
    }

    fn snapshot() -> FullReportSnapshot {
        FullReportSnapshot::new(
            "test-cluster".into(),
            7,
            "worker-session-7".into(),
            10,
            vec![],
            16,
        )
    }
}
