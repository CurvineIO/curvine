// Copyright 2025 OPPO.
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::master::journal::JournalSystem;
use curvine_model::{ClientAddress, CommitBlock, CreateFileOptsBuilder, OpenFlags, WorkerInfo};

fn probe() -> (MasterFilesystem, Arc<MasterReplicationManager>, BlockId) {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.change_test_meta_dir(format!("commit-repair-{}", Utils::rand_id()));
    conf.master.block_replication_enabled = true;
    // Exercise the real constructor/registration, but prevent worker RPCs.
    conf.master.block_replication_concurrency_limit = 0;
    conf.master.block_replication_retry_interval = "1h".into();
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.master_monitor
        .set_role_for_test(curvine_raft::raft::RoleState::Leader);
    for id in [100, 200, 300] {
        fs.add_test_worker(WorkerInfo {
            address: WorkerAddress {
                worker_id: id,
                ..Default::default()
            },
            worker_session_id: format!("worker-{id}"),
            ..Default::default()
        });
    }
    let rt = Arc::new(AsyncRuntime::new("commit-repair-probe", 1, 1));
    let manager = MasterReplicationManager::new(&fs, &conf, &rt, &fs.worker_manager).unwrap();
    fs.create_with_opts(
        "/repair",
        CreateFileOptsBuilder::new()
            .replicas(2)
            .block_size(1_048_576)
            .build(),
        OpenFlags::new_create(),
    )
    .unwrap();
    let id = fs
        .add_block(
            "/repair",
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .unwrap()
        .block
        .id;
    (fs, manager, id)
}

fn commit(id: BlockId, len: i64, workers: &[u32]) -> CommitBlock {
    CommitBlock {
        block_id: id,
        block_len: len,
        locations: workers
            .iter()
            .map(|id| BlockLocation::with_id(*id))
            .collect(),
    }
}

#[test]
fn post_commit_probe_complete_queues_degraded_block() {
    let (fs, manager, id) = probe();
    fs.complete_file(
        "/repair",
        None,
        8,
        vec![commit(id, 8, &[100])],
        "",
        false,
        None,
    )
    .unwrap();
    assert!(
        manager.has_pending_repair(id),
        "successful degraded CompleteFile must retain repair demand"
    );
}

#[test]
fn post_commit_probe_add_block_queues_only_committed_block() {
    let (fs, manager, id) = probe();
    let next = fs
        .add_block(
            "/repair",
            None,
            ClientAddress::default(),
            vec![commit(id, 1_048_576, &[100])],
            vec![],
            1_048_576,
            Some(curvine_model::ExtendedBlock::new(
                id,
                1_048_576,
                Default::default(),
                Default::default(),
            )),
        )
        .unwrap();
    assert!(
        manager.has_pending_repair(id),
        "successful AddBlock must repair its degraded committed predecessor"
    );
    assert!(
        !manager.has_pending_repair(next.block.id),
        "fresh allocation is not a committed degraded replica"
    );
}

#[test]
fn post_commit_probe_empty_close_rediscovers_prior_deficit() {
    let (fs, manager, id) = probe();
    // A prior commit can predate manager notification (e.g. restored metadata).
    let mut dir = fs.fs_dir.write();
    let inp = crate::master::meta::inode::InodePath::resolve(dir.root_ptr(), "/repair", &dir.store)
        .unwrap();
    let mut inode = inp.get_last_inode().unwrap();
    dir.complete_file(
        "/repair",
        &mut inode,
        8,
        vec![commit(id, 8, &[100])],
        "",
        true,
        None,
        None,
    )
    .unwrap();
    drop(dir);
    assert!(!manager.has_pending_repair(id));
    fs.complete_file("/repair", None, 8, vec![], "", false, None)
        .unwrap();
    assert!(
        manager.has_pending_repair(id),
        "close without fresh commits must inspect existing file deficits"
    );
}

/// Real namespace/store and scheduling state, without detached consumer/reaper tasks.
/// Keep the receiver to make backpressure and retry order deterministic.
struct Fixture {
    fs: MasterFilesystem,
    manager: Arc<MasterReplicationManager>,
    receiver: Receiver<BlockId>,
    id: BlockId,
}

impl Fixture {
    fn new(replicas: u8, capacity: usize, enabled: bool) -> Self {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.change_test_meta_dir(format!("commit-repair-controlled-{}", Utils::rand_id()));
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);
        for id in [100, 200, 300] {
            fs.add_test_worker(WorkerInfo {
                address: WorkerAddress {
                    worker_id: id,
                    rpc_port: id,
                    ..Default::default()
                },
                worker_session_id: format!("worker-{id}"),
                ..Default::default()
            });
        }
        let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
        let manager = Arc::new(MasterReplicationManager {
            worker_manager: fs.worker_manager.clone(),
            fs: fs.clone(),
            replication_semaphore: Arc::new(Semaphore::new(1)),
            staging_queue_sender: Arc::new(sender),
            tracker: Arc::new(ReplicationTracker::new(1000)),
            worker_client_factory: Arc::new(Default::default()),
            replication_enabled: enabled,
            submit_timeout: Duration::from_secs(2),
            job_timeout_ms: 1000,
            reaper_interval: Duration::from_millis(100),
            clock_start: Instant::now(),
            metrics: Master::get_metrics().unwrap(),
            runtime: Arc::new(AsyncRuntime::new("commit-repair-controlled", 1, 2)),
            recovery_semaphore: Arc::new(Semaphore::new(0)),
            repairs: Arc::new(FastMutex::new(RepairQueue::default())),
        });
        fs.bind_replication_manager(&manager);
        fs.create_with_opts(
            "/repair",
            CreateFileOptsBuilder::new()
                .replicas(replicas.into())
                .block_size(1_048_576)
                .build(),
            OpenFlags::new_create(),
        )
        .unwrap();
        let id = fs
            .add_block(
                "/repair",
                None,
                ClientAddress::default(),
                vec![],
                vec![],
                0,
                None,
            )
            .unwrap()
            .block
            .id;
        Self {
            fs,
            manager,
            receiver,
            id,
        }
    }

    fn flush(&self, len: i64, workers: &[u32]) {
        self.fs
            .flush_file(
                "/repair",
                None,
                len,
                vec![commit(self.id, len, workers)],
                "",
            )
            .unwrap();
    }

    fn locations(&self) -> Vec<u32> {
        let mut ids: Vec<_> = self
            .fs
            .fs_dir
            .read()
            .get_block_locations(self.id)
            .unwrap()
            .into_iter()
            .map(|loc| loc.worker_id)
            .collect();
        ids.sort_unstable();
        ids
    }

    fn revision(&self) -> u64 {
        self.fs
            .fs_dir
            .read()
            .replication_block_state(self.id)
            .unwrap()
            .unwrap()
            .commit_revision
    }

    fn second_block(&self) -> BlockId {
        self.fs
            .add_block(
                "/repair",
                None,
                ClientAddress::default(),
                vec![commit(self.id, 1_048_576, &[100])],
                vec![],
                1_048_576,
                Some(curvine_model::ExtendedBlock::new(
                    self.id,
                    1_048_576,
                    Default::default(),
                    Default::default(),
                )),
            )
            .unwrap()
            .block
            .id
    }

    fn assert_idle(&mut self) {
        assert!(!self.manager.has_pending_repair(self.id));
        assert!(self.receiver.try_recv().is_err());
    }
}

#[test]
fn post_commit_flush_without_snapshot_records_demand() {
    let mut f = Fixture::new(2, 1, true);
    f.flush(8, &[100]);
    assert!(f.manager.has_pending_repair(f.id));
    assert_eq!(f.receiver.try_recv().unwrap(), f.id);
    assert_eq!(f.locations(), vec![100]);
    assert!(f.revision() > 0);
}

#[test]
fn post_commit_complete_flush_with_snapshot_records_demand() {
    let mut f = Fixture::new(2, 1, true);
    let blocks =
        f.fs.complete_file(
            "/repair",
            None,
            8,
            vec![commit(f.id, 8, &[100])],
            "",
            true,
            None,
        )
        .unwrap()
        .unwrap();
    assert_eq!(blocks.block_locs[0].locs.len(), 1);
    assert_eq!(f.receiver.try_recv().unwrap(), f.id);
    assert!(f.manager.has_pending_repair(f.id));
}

#[test]
fn post_commit_healthy_and_last_wins_do_not_queue() {
    let mut f = Fixture::new(2, 1, true);
    f.fs.flush_file(
        "/repair",
        None,
        8,
        vec![commit(f.id, 8, &[100]), commit(f.id, 8, &[100, 200])],
        "",
    )
    .unwrap();
    assert_eq!(f.locations(), vec![100, 200]);
    f.assert_idle();
}

#[test]
fn post_commit_duplicate_last_wins_degraded_queues_once() {
    let mut f = Fixture::new(2, 1, true);
    f.fs.flush_file(
        "/repair",
        None,
        8,
        vec![commit(f.id, 8, &[100, 200]), commit(f.id, 8, &[100])],
        "",
    )
    .unwrap();
    assert_eq!(f.locations(), vec![100]);
    assert_eq!(f.receiver.try_recv().unwrap(), f.id);
    assert!(f.receiver.try_recv().is_err());
    assert_eq!(f.manager.repairs.lock().due.len(), 1);
}

#[test]
fn post_commit_failed_complete_does_not_notify() {
    let mut f = Fixture::new(2, 1, true);
    assert!(f
        .fs
        .complete_file(
            "/missing",
            None,
            8,
            vec![commit(f.id, 8, &[100])],
            "",
            false,
            None
        )
        .is_err());
    assert_eq!(f.revision(), 0);
    f.assert_idle();
}

#[test]
fn post_commit_failed_add_block_does_not_notify() {
    let mut f = Fixture::new(2, 1, true);
    assert!(f
        .fs
        .add_block(
            "/missing",
            None,
            ClientAddress::default(),
            vec![commit(f.id, 1_048_576, &[100])],
            vec![],
            1_048_576,
            None
        )
        .is_err());
    assert_eq!(f.revision(), 0);
    f.assert_idle();
}

#[test]
fn post_commit_reuse_next_block_does_not_commit_or_notify() {
    let mut f = Fixture::new(2, 1, true);
    let reused =
        f.fs.add_block(
            "/repair",
            None,
            ClientAddress::default(),
            vec![commit(f.id, 8, &[100])],
            vec![],
            8,
            None,
        )
        .unwrap();
    assert_eq!(reused.block.id, f.id);
    assert_eq!(f.revision(), 0);
    f.assert_idle();
}

#[test]
fn post_commit_disabled_manager_does_not_affect_write() {
    let mut f = Fixture::new(2, 1, false);
    f.flush(8, &[100]);
    assert_eq!(f.locations(), vec![100]);
    f.assert_idle();
}

#[test]
fn post_commit_standby_notification_does_not_queue() {
    let mut f = Fixture::new(2, 1, true);
    f.flush(8, &[100]);
    assert_eq!(f.receiver.try_recv().unwrap(), f.id);
    assert!(f.manager.tracker.cancel_queued(f.id));
    *f.manager.repairs.lock() = RepairQueue::default();
    f.fs.master_monitor
        .set_role_for_test(curvine_raft::raft::RoleState::Follower);
    f.manager.notify_committed_blocks(vec![f.id]);
    f.assert_idle();
}

#[test]
fn post_commit_expired_weak_registration_does_not_affect_write() {
    let f = Fixture::new(2, 1, true);
    let weak = Arc::downgrade(&f.manager);
    let Fixture {
        fs, manager, id, ..
    } = f;
    drop(manager);
    assert!(
        weak.upgrade().is_none(),
        "filesystem binding must not form an Arc cycle"
    );
    fs.flush_file("/repair", None, 8, vec![commit(id, 8, &[100])], "")
        .unwrap();
}

#[test]
fn post_commit_cloned_filesystem_shares_registration() {
    let mut f = Fixture::new(2, 1, true);
    f.fs.clone()
        .flush_file("/repair", None, 8, vec![commit(f.id, 8, &[100])], "")
        .unwrap();
    assert_eq!(f.receiver.try_recv().unwrap(), f.id);
}

#[test]
fn post_commit_notifications_and_heartbeats_deduplicate() {
    let mut f = Fixture::new(2, 512, true);
    f.flush(8, &[100]);
    for _ in 0..100 {
        f.manager.notify_committed_blocks(vec![f.id, f.id]);
        f.manager
            .report_under_replicated_blocks(100, vec![f.id, f.id])
            .unwrap();
    }
    assert_eq!(f.receiver.try_recv().unwrap(), f.id);
    assert!(f.receiver.try_recv().is_err());
    assert_eq!(f.manager.repairs.lock().blocks.len(), 1);
    assert_eq!(f.manager.repairs.lock().due.len(), 1);
    // Draining staging does not remove the queued tracker marker.
    f.manager.retry_repairs(10_000);
    assert!(f.receiver.try_recv().is_err());
    assert_eq!(f.manager.repairs.lock().due.len(), 1);
}

#[test]
fn post_commit_full_staging_retains_demand_and_retries() {
    let mut f = Fixture::new(2, 1, true);
    let next = f.second_block();
    f.fs.flush_file(
        "/repair",
        None,
        1_048_584,
        vec![commit(next, 8, &[100])],
        "",
    )
    .unwrap();
    assert_eq!(f.receiver.len(), 1);
    assert!(
        f.manager.has_pending_repair(next),
        "full staging must retain committed demand"
    );
    assert_eq!(f.receiver.try_recv().unwrap(), f.id);
    f.manager.retry_repairs(10_000);
    assert_eq!(
        f.receiver.try_recv().unwrap(),
        next,
        "retry must not require another heartbeat"
    );
    assert!(f.receiver.try_recv().is_err());
}

#[test]
fn post_commit_closed_staging_does_not_fail_commit() {
    let mut f = Fixture::new(2, 1, true);
    f.receiver.close();
    f.flush(8, &[100]);
    assert_eq!(f.locations(), vec![100]);
    assert!(f.manager.has_pending_repair(f.id));
    assert!(
        f.manager.tracker.queue(f.id, 0),
        "closed staging must remove queued marker"
    );
    assert!(f.manager.tracker.cancel_queued(f.id));
    f.manager.retry_repairs(10_000);
    assert!(f.manager.has_pending_repair(f.id));
    assert!(f.manager.tracker.queue(f.id, 0));
}

#[test]
fn post_commit_zero_length_and_uncommitted_blocks_do_not_repair() {
    let mut f = Fixture::new(2, 1, true);
    f.manager.notify_committed_blocks(vec![f.id]);
    f.assert_idle();
    f.flush(0, &[100]);
    f.assert_idle();
    // A stale heartbeat demand for an empty block is retired on the next scan.
    f.manager
        .report_under_replicated_blocks(100, vec![f.id])
        .unwrap();
    f.manager.retry_repairs(10_000);
    assert!(!f.manager.has_pending_repair(f.id));
}

#[test]
fn post_commit_delayed_notification_observes_new_healthy_commit() {
    let mut f = Fixture::new(2, 1, true);
    f.flush(8, &[100, 200]);
    f.manager.notify_committed_blocks(vec![f.id]);
    f.assert_idle();
}

#[test]
fn post_commit_deleted_or_overwritten_block_demand_is_retired() {
    for overwrite in [false, true] {
        let mut f = Fixture::new(2, 1, true);
        f.flush(8, &[100]);
        if overwrite {
            f.fs.create_with_opts(
                "/repair",
                CreateFileOptsBuilder::new().build(),
                OpenFlags::new_create().set_overwrite(true),
            )
            .unwrap();
        } else {
            f.fs.delete("/repair", false).unwrap();
        }
        f.manager.notify_committed_blocks(vec![f.id]);
        f.manager.retry_repairs(10_000);
        assert!(!f.manager.has_pending_repair(f.id));
        assert_eq!(f.receiver.try_recv().unwrap(), f.id);
        let rt = f.manager.runtime.clone();
        rt.block_on(
            f.manager.replicate_block(
                f.id,
                f.manager
                    .replication_semaphore
                    .clone()
                    .try_acquire_owned()
                    .unwrap(),
            ),
        )
        .unwrap();
        assert!(f.manager.tracker.queue(f.id, 0));
    }
}

#[test]
fn post_commit_demotion_preserves_demand_until_promotion() {
    let mut f = Fixture::new(2, 1, true);
    f.flush(8, &[100]);
    assert_eq!(f.receiver.try_recv().unwrap(), f.id);
    f.fs.master_monitor
        .set_role_for_test(curvine_raft::raft::RoleState::Follower);
    let rt = f.manager.runtime.clone();
    rt.block_on(
        f.manager.replicate_block(
            f.id,
            f.manager
                .replication_semaphore
                .clone()
                .try_acquire_owned()
                .unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(f.manager.replication_semaphore.available_permits(), 1);
    f.manager.retry_repairs(10_000);
    assert!(f.receiver.try_recv().is_err());
    assert!(f.manager.has_pending_repair(f.id));
    f.fs.master_monitor
        .set_role_for_test(curvine_raft::raft::RoleState::Leader);
    f.manager.retry_repairs(10_000);
    assert_eq!(f.receiver.try_recv().unwrap(), f.id);
}

// Scripted workers run the real framed RPC protocol, but never copy block data.
// Abort and join listeners at test teardown; no detached server thread per test.
#[derive(Clone)]
struct Peer {
    submitted: Arc<FastMutex<Vec<SubmitBlockReplicationRequest>>>,
    prepared: Arc<FastMutex<Vec<PrepareReplicationRequest>>>,
    reject_submit: bool,
}

impl Default for Peer {
    fn default() -> Self {
        Self {
            submitted: Arc::new(FastMutex::new(Vec::new())),
            prepared: Arc::new(FastMutex::new(Vec::new())),
            reject_submit: false,
        }
    }
}

impl curvine_rpc::handler::HandlerService for Peer {
    type Item = Self;
    fn get_message_handler(&self, _: Option<curvine_net::net::ConnState>) -> Self::Item {
        self.clone()
    }
}

impl curvine_rpc::handler::MessageHandler for Peer {
    type Error = curvine_core_error::CommonErrorExt;
    fn handle(
        &self,
        msg: &curvine_rpc::message::Message,
    ) -> Result<curvine_rpc::message::Message, Self::Error> {
        match RpcCode::from(msg.code()) {
            RpcCode::PrepareReplication => {
                let req: PrepareReplicationRequest = msg.parse_header()?;
                let token = format!("token-{}", req.attempt_id);
                self.prepared.lock().push(req);
                Ok(Builder::success_with_header(msg, PrepareReplicationResponse { token }).build())
            }
            RpcCode::SubmitFencedReplication => {
                let req: SubmitBlockReplicationRequest = msg.parse_header()?;
                let response = SubmitBlockReplicationResponse {
                    success: !self.reject_submit,
                    message: None,
                    attempt_id: req.attempt_id.clone(),
                };
                self.submitted.lock().push(req);
                Ok(Builder::success_with_header(msg, response).build())
            }
            code => panic!("unexpected scripted-worker RPC {code:?}"),
        }
    }
}

struct PeerServer {
    runtime: Arc<AsyncRuntime>,
    task: curvine_runtime::runtime::JoinHandle<CommonResult<()>>,
}

impl Drop for PeerServer {
    fn drop(&mut self) {
        self.task.abort();
        let _ = self.runtime.block_on(&mut self.task);
    }
}

fn peers(f: &Fixture, reject_submit: bool) -> (Peer, Vec<PeerServer>) {
    let peer = Peer {
        reject_submit,
        ..Default::default()
    };
    let mut servers = Vec::new();
    for id in [100, 200, 300] {
        let port = curvine_net::net::NetUtils::hold_available_port();
        let mut conf = curvine_rpc::server::ServerConf::with_hostname("127.0.0.1", port);
        conf.io_threads = 1;
        conf.worker_threads = 2;
        let rt = f.manager.runtime.clone();
        let server = curvine_rpc::server::RpcServer::with_rt(rt.clone(), conf, peer.clone());
        let mut listener = server.new_state_listener();
        let task = rt.spawn(async move { server.run().await });
        rt.block_on(async { timeout(Duration::from_secs(2), listener.wait_startup()).await })
            .unwrap()
            .unwrap();
        f.fs.add_test_worker(WorkerInfo {
            address: WorkerAddress {
                worker_id: id,
                hostname: "127.0.0.1".into(),
                ip_addr: "127.0.0.1".into(),
                rpc_port: port as u32,
                web_port: 1,
            },
            worker_session_id: format!("worker-{id}"),
            ..Default::default()
        });
        servers.push(PeerServer { runtime: rt, task });
    }
    (peer, servers)
}

fn dispatch(f: &mut Fixture) {
    let id = f.receiver.try_recv().unwrap();
    let permit = f
        .manager
        .replication_semaphore
        .clone()
        .try_acquire_owned()
        .unwrap();
    f.manager
        .runtime
        .block_on(f.manager.replicate_block(id, permit))
        .unwrap();
}

fn success(f: &Fixture, request: &SubmitBlockReplicationRequest) {
    f.manager
        .finish_replicated_block(ReportBlockReplicationRequest {
            block_id: request.block_id,
            storage_type: curvine_model::StorageType::Disk.into(),
            success: true,
            message: None,
            attempt_id: request.attempt_id.clone(),
        })
        .unwrap();
}

#[test]
fn post_commit_real_processor_repairs_to_target_without_heartbeat() {
    let mut f = Fixture::new(3, 1, true);
    let (peer, _servers) = peers(&f, false);
    f.flush(8, &[100]);
    dispatch(&mut f);
    assert_eq!(f.manager.replication_semaphore.available_permits(), 0);
    let first = peer.submitted.lock()[0].clone();
    assert_eq!(
        first.target_token.as_ref().unwrap(),
        &format!("token-{}", first.attempt_id.as_ref().unwrap())
    );
    assert_ne!(first.target_worker_info.worker_id, 100);
    success(&f, &first);
    assert_eq!(f.locations().len(), 2);
    assert!(f.manager.has_pending_repair(f.id));
    assert_eq!(f.manager.replication_semaphore.available_permits(), 1);
    f.manager.retry_repairs(10_000);
    dispatch(&mut f);
    let second = peer.submitted.lock()[1].clone();
    assert_ne!(
        second.target_worker_info.worker_id,
        first.target_worker_info.worker_id
    );
    assert_ne!(first.attempt_id, second.attempt_id);
    success(&f, &second);
    assert_eq!(f.locations(), vec![100, 200, 300]);
    f.manager.retry_repairs(20_000);
    assert!(!f.manager.has_pending_repair(f.id));
    assert_eq!(peer.prepared.lock().len(), 2);
    assert!(f.receiver.try_recv().is_err());
}

#[test]
fn post_commit_real_processor_captures_current_revision_rejects_old_receipt() {
    let mut f = Fixture::new(2, 1, true);
    let (peer, _servers) = peers(&f, false);
    f.flush(8, &[100]);
    let queued_revision = f.revision();
    // The queued notification is only demand, not a captured version/locations.
    f.flush(9, &[100]);
    let dispatch_revision = f.revision();
    assert!(dispatch_revision > queued_revision);
    dispatch(&mut f);
    let first = peer.submitted.lock()[0].clone();
    success(&f, &first);
    assert_eq!(
        f.locations().len(),
        2,
        "dispatch must capture the current, not queued, revision"
    );
    f.flush(10, &[100]);
    f.manager.retry_repairs(10_000);
    dispatch(&mut f);
    let old = peer.submitted.lock()[1].clone();
    f.flush(11, &[100]);
    success(&f, &old);
    assert_eq!(
        f.locations(),
        vec![100],
        "old attempt must not authorize its target after a newer commit"
    );
    assert!(f.manager.has_pending_repair(f.id));
    f.manager.retry_repairs(20_000);
    dispatch(&mut f);
    let current = peer.submitted.lock()[2].clone();
    success(&f, &current);
    assert_eq!(f.locations().len(), 2);
    f.manager.retry_repairs(30_000);
    assert!(!f.manager.has_pending_repair(f.id));
}

#[test]
fn post_commit_real_processor_holds_limit_until_completion() {
    let mut f = Fixture::new(2, 2, true);
    let (peer, _servers) = peers(&f, false);
    let next = f.second_block();
    f.fs.flush_file(
        "/repair",
        None,
        1_048_584,
        vec![commit(next, 8, &[100])],
        "",
    )
    .unwrap();
    dispatch(&mut f);
    assert!(f
        .manager
        .replication_semaphore
        .clone()
        .try_acquire_owned()
        .is_err());
    assert_eq!(peer.submitted.lock().len(), 1);
    let first = peer.submitted.lock()[0].clone();
    f.manager.notify_committed_blocks(vec![f.id, f.id]);
    assert_eq!(
        f.receiver.len(),
        1,
        "duplicate inflight demand must not consume staging"
    );
    success(&f, &first);
    dispatch(&mut f);
    assert_eq!(peer.submitted.lock().len(), 2);
    assert_eq!(peer.submitted.lock()[1].block_id, next);
}

#[test]
fn post_commit_real_processor_submit_rejection_releases_limit_retains_demand() {
    let mut f = Fixture::new(2, 1, true);
    let (peer, _servers) = peers(&f, true);
    f.flush(8, &[100]);
    let id = f.receiver.try_recv().unwrap();
    let permit = f
        .manager
        .replication_semaphore
        .clone()
        .try_acquire_owned()
        .unwrap();
    assert!(f
        .manager
        .runtime
        .block_on(f.manager.replicate_block(id, permit))
        .is_err());
    let req = peer.submitted.lock()[0].clone();
    assert_eq!(f.manager.replication_semaphore.available_permits(), 1);
    assert!(f.manager.has_pending_repair(f.id));
    assert!(f
        .manager
        .tracker
        .uncertain_attempt(f.id, req.attempt_id.as_ref().unwrap())
        .is_some());
    f.manager.retry_repairs(10_000);
    assert!(
        f.receiver.try_recv().is_err(),
        "unreconciled target must not admit another attempt"
    );
    assert_eq!(f.locations(), vec![100]);
}

#[test]
fn post_commit_real_processor_result_timeout_releases_limit_retains_demand() {
    let mut f = Fixture::new(2, 1, true);
    let (peer, _servers) = peers(&f, false);
    f.flush(8, &[100]);
    dispatch(&mut f);
    let req = peer.submitted.lock()[0].clone();
    f.manager.reap_inflight_jobs(10_000);
    assert_eq!(f.manager.replication_semaphore.available_permits(), 1);
    assert!(f.manager.has_pending_repair(f.id));
    assert!(f
        .manager
        .tracker
        .uncertain_attempt(f.id, req.attempt_id.as_ref().unwrap())
        .is_some());
    // A valid same-version late result can finish the retained uncertain attempt.
    success(&f, &req);
    assert_eq!(f.locations().len(), 2);
    f.manager.retry_repairs(20_000);
    assert!(!f.manager.has_pending_repair(f.id));
}

#[test]
fn post_commit_atomic_publication_failure_never_records_demand() {
    for add_block in [false, true] {
        let mut f = Fixture::new(2, 1, true);
        {
            let dir = f.fs.fs_dir.read();
            let mut batch = dir.store.new_batch();
            batch
                .set_replica_commit_fence(
                    f.id,
                    &crate::master::meta::store::ReplicaCommitFence {
                        revision: u64::MAX,
                        workers: vec![],
                    },
                )
                .unwrap();
            batch.commit().unwrap();
        }
        if add_block {
            assert!(f
                .fs
                .add_block(
                    "/repair",
                    None,
                    ClientAddress::default(),
                    vec![commit(f.id, 1_048_576, &[100])],
                    vec![],
                    1_048_576,
                    Some(curvine_model::ExtendedBlock::new(
                        f.id,
                        1_048_576,
                        Default::default(),
                        Default::default()
                    ))
                )
                .is_err());
        } else {
            assert!(f
                .fs
                .complete_file(
                    "/repair",
                    None,
                    8,
                    vec![commit(f.id, 8, &[100])],
                    "",
                    false,
                    None
                )
                .is_err());
        }
        assert_eq!(f.revision(), u64::MAX);
        assert!(f.locations().is_empty());
        assert_eq!(
            f.fs.fs_dir
                .read()
                .replication_block_state(f.id)
                .unwrap()
                .unwrap()
                .block_len,
            0
        );
        f.assert_idle();
    }
}

#[test]
fn post_commit_healthy_add_block_does_not_queue_predecessor_or_fresh_block() {
    let mut f = Fixture::new(2, 1, true);
    let next =
        f.fs.add_block(
            "/repair",
            None,
            ClientAddress::default(),
            vec![commit(f.id, 1_048_576, &[100, 200])],
            vec![],
            1_048_576,
            Some(curvine_model::ExtendedBlock::new(
                f.id,
                1_048_576,
                Default::default(),
                Default::default(),
            )),
        )
        .unwrap();
    assert_eq!(f.locations(), vec![100, 200]);
    assert!(!f.manager.has_pending_repair(next.block.id));
    f.assert_idle();
}

#[test]
fn post_commit_batched_notification_uses_current_membership_across_files() {
    let mut f = Fixture::new(2, 4, true);
    let next = f.second_block();
    f.fs.flush_file(
        "/repair",
        None,
        1_048_584,
        vec![commit(next, 8, &[100, 200])],
        "",
    )
    .unwrap();
    f.fs.create_with_opts(
        "/other",
        CreateFileOptsBuilder::new().replicas(2).build(),
        OpenFlags::new_create(),
    )
    .unwrap();
    let other =
        f.fs.add_block(
            "/other",
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .unwrap()
        .block
        .id;
    f.fs.flush_file("/other", None, 8, vec![commit(other, 8, &[100])], "")
        .unwrap();
    let ids = HashSet::from([f.id, next, other, i64::MAX]);
    let mut deficits =
        f.fs.fs_dir
            .read()
            .committed_replication_deficits(&ids)
            .unwrap();
    deficits.sort_unstable();
    let mut expected = vec![f.id, other];
    expected.sort_unstable();
    assert_eq!(deficits, expected);
    f.manager
        .notify_committed_blocks(vec![f.id, next, other, i64::MAX, other]);
    assert_eq!(f.receiver.len(), 2);
    assert!(!f.manager.has_pending_repair(next));
    assert!(!f.manager.has_pending_repair(i64::MAX));
    // Make both queued demands healthy; retry retires them without a heartbeat.
    f.fs.flush_file(
        "/repair",
        None,
        1_048_584,
        vec![commit(f.id, 1_048_576, &[100, 200])],
        "",
    )
    .unwrap();
    f.fs.flush_file("/other", None, 8, vec![commit(other, 8, &[100, 200])], "")
        .unwrap();
    f.manager.retry_repairs(10_000);
    assert_eq!(f.manager.repairs.lock().blocks.len(), 0);
    assert_eq!(f.manager.repairs.lock().due.len(), 0);
    // Consumers will skip the already-healthy staged identities.
    while let Ok(id) = f.receiver.try_recv() {
        f.manager
            .runtime
            .block_on(
                f.manager.replicate_block(
                    id,
                    f.manager
                        .replication_semaphore
                        .clone()
                        .try_acquire_owned()
                        .unwrap(),
                ),
            )
            .unwrap();
    }
}

#[test]
fn post_commit_with_js_handles_share_late_manager_registration() {
    let f = Fixture::new(2, 1, true);
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.change_test_meta_dir(format!("commit-repair-with-js-{}", Utils::rand_id()));
    let js = JournalSystem::from_conf(&conf).unwrap();
    let original = js.fs();
    original
        .master_monitor
        .set_role_for_test(curvine_raft::raft::RoleState::Leader);
    let other = MasterFilesystem::with_js(&conf, &js);
    for id in [100, 200] {
        original.add_test_worker(WorkerInfo {
            address: WorkerAddress {
                worker_id: id,
                ..Default::default()
            },
            worker_session_id: format!("worker-{id}"),
            ..Default::default()
        });
    }
    let manager = Arc::new(MasterReplicationManager {
        fs: original.clone(),
        worker_manager: original.worker_manager.clone(),
        ..(*f.manager).clone()
    });
    // The secondary handle predates registration on the original.
    original.bind_replication_manager(&manager);
    original
        .create_with_opts(
            "/repair",
            CreateFileOptsBuilder::new().replicas(2).build(),
            OpenFlags::new_create(),
        )
        .unwrap();
    let id = original
        .add_block(
            "/repair",
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .unwrap()
        .block
        .id;
    other
        .flush_file("/repair", None, 8, vec![commit(id, 8, &[100])], "")
        .unwrap();
    assert!(manager.has_pending_repair(id));
    drop(manager);
    drop(original);
    drop(other);
    js.shutdown();
}
