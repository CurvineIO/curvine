// Copyright 2026 OPPO.
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

use super::*;
use crate::raft::storage::{ApplyMsg, HashAppStorage, MemLogStorage, RocksLogStorage};
use curvine_config::RaftPeer;
use curvine_net::net::NetUtils;
use curvine_runtime::sync::StateCtl;
use raft::eraftpb::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeV2, HardState, Message, Snapshot,
};
use raft::{StateRole, Storage};
use std::time::Instant;

fn test_node(rt: Arc<Runtime>, id: u64) -> RaftNode<MemLogStorage, HashAppStorage<String, String>> {
    test_node_with_voters(rt, id, vec![1, 2, 3])
}

fn test_node_with_voters(
    rt: Arc<Runtime>,
    id: u64,
    peer_ids: Vec<u64>,
) -> RaftNode<MemLogStorage, HashAppStorage<String, String>> {
    test_node_with_app(rt, id, peer_ids, HashAppStorage::new(), RoleMonitor::new())
}

fn test_node_with_app<B: AppStorage>(
    rt: Arc<Runtime>,
    id: u64,
    mut peer_ids: Vec<u64>,
    app_store: B,
    role_monitor: RoleMonitor,
) -> RaftNode<MemLogStorage, B> {
    let mut conf = JournalConf::with_test();
    conf.rpc_retry_max_duration_ms = 0;
    conf.conn_retry_max_duration_ms = 0;
    if !peer_ids.contains(&id) {
        peer_ids.push(id);
    }
    peer_ids.sort_unstable();
    conf.journal_addrs = peer_ids
        .into_iter()
        .map(|peer_id| {
            let port = if peer_id == id {
                conf.rpc_port
            } else {
                NetUtils::get_available_port()
            };
            RaftPeer::new(peer_id, &conf.hostname, port)
        })
        .collect();
    let group = RaftGroup::from_conf(&conf);
    let node_id = group.get_node_id(&conf.local_addr()).unwrap();
    assert_eq!(node_id, id);
    let client = RaftClient::new(rt.clone(), &group, conf.new_client_conf());
    let log_store = MemLogStorage::new();
    log_store
        .set_conf_state(&raft::eraftpb::ConfState {
            voters: group.voters(),
            ..Default::default()
        })
        .unwrap();
    let storage = PeerStorage::new(
        rt.clone(),
        node_id,
        log_store,
        app_store,
        client.clone(),
        &conf,
    );
    let raw = RawNode::new(
        &conf.new_raft_conf(node_id, 0),
        storage.clone(),
        &slog::Logger::root(slog::Discard, slog::o!()),
    )
    .unwrap();
    let (sender, receiver) = mpsc::channel(conf.message_size);
    let (session_sender, session_receiver) = mpsc::unbounded_channel();

    RaftNode {
        rt,
        raw,
        client,
        session: 100 * node_id + 1,
        peer_sessions: PeerSessions::default(),
        heartbeat_sequence: 0,
        session_sender,
        session_receiver,
        recovery: Recovery::load(&conf, node_id).unwrap(),
        pending_ready: None,
        snapshot_done: None,
        storage,
        receiver,
        sender,
        group,
        role_monitor,
        tick_interval: Duration::from_millis(conf.raft_tick_interval_ms),
        max_batch_size: conf.raft_batch_size.max(1),
        snapshot_interval_ms: 0,
        snapshot_min_interval_ms: 0,
        snapshot_entries: conf.snapshot_entries,
        last_snapshot_ms: 0,
        last_snapshot_op_id: 0,
    }
}

#[derive(Clone)]
struct RoleRecordingAppStorage {
    inner: HashAppStorage<String, String>,
    ctl: StateCtl,
    replayed_as_leader: Arc<std::sync::Mutex<Vec<bool>>>,
}

impl RoleRecordingAppStorage {
    fn new(inner: HashAppStorage<String, String>, ctl: StateCtl) -> Self {
        Self {
            inner,
            ctl,
            replayed_as_leader: Arc::new(std::sync::Mutex::new(vec![])),
        }
    }

    fn replayed_as_leader(&self) -> Vec<bool> {
        self.replayed_as_leader.lock().unwrap().clone()
    }
}

impl AppStorage for RoleRecordingAppStorage {
    async fn apply(&self, wait: bool, msg: ApplyMsg) -> RaftResult<()> {
        if matches!(msg, ApplyMsg::Scan(_)) {
            let leader = self.ctl.state::<RoleState>() == RoleState::Leader;
            self.replayed_as_leader.lock().unwrap().push(leader);
        }
        self.inner.apply(wait, msg).await
    }

    fn get_fsm_state(&self) -> FsmState {
        self.inner.get_fsm_state()
    }

    async fn role_change(&self, role: StateRole) -> RaftResult<()> {
        self.inner.role_change(role).await
    }

    async fn create_snapshot(&self) -> RaftResult<SnapshotData> {
        self.inner.create_snapshot().await
    }

    async fn apply_snapshot(&self, snapshot: SnapshotData) -> RaftResult<()> {
        self.inner.apply_snapshot(snapshot).await
    }

    fn snapshot_dir(&self, snapshot_id: u64) -> RaftResult<String> {
        self.inner.snapshot_dir(snapshot_id)
    }
}

fn test_rocks_node(
    rt: Arc<Runtime>,
    id: u64,
) -> (
    RaftNode<RocksLogStorage, HashAppStorage<String, String>>,
    JournalConf,
) {
    let mut conf = JournalConf::with_test();
    let node = test_rocks_node_with_conf(rt, id, &mut conf);
    (node, conf)
}

fn test_rocks_node_with_conf(
    rt: Arc<Runtime>,
    id: u64,
    conf: &mut JournalConf,
) -> RaftNode<RocksLogStorage, HashAppStorage<String, String>> {
    test_rocks_node_with_app(rt, id, conf, HashAppStorage::new(), RoleMonitor::new())
}

fn test_rocks_node_with_app<B: AppStorage>(
    rt: Arc<Runtime>,
    id: u64,
    conf: &mut JournalConf,
    app_store: B,
    role_monitor: RoleMonitor,
) -> RaftNode<RocksLogStorage, B> {
    conf.rpc_retry_max_duration_ms = 0;
    conf.conn_retry_max_duration_ms = 0;
    let root = std::env::temp_dir().join(format!(
        "curvine-rocks-node-{}-{}-{}",
        id,
        curvine_runtime::common::Utils::rand_id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    conf.journal_dir = root.to_str().unwrap().into();
    let mut peer_ids = vec![1, 2, 3];
    if !peer_ids.contains(&id) {
        peer_ids.push(id);
    }
    peer_ids.sort_unstable();
    conf.journal_addrs = peer_ids
        .into_iter()
        .map(|peer_id| {
            let port = if peer_id == id {
                conf.rpc_port
            } else {
                NetUtils::get_available_port()
            };
            RaftPeer::new(peer_id, &conf.hostname, port)
        })
        .collect();
    let group = RaftGroup::from_conf(conf);
    let node_id = group.get_node_id(&conf.local_addr()).unwrap();
    assert_eq!(node_id, id);
    let client = RaftClient::new(rt.clone(), &group, conf.new_client_conf());
    let log_store = RocksLogStorage::from_conf(conf, true);
    log_store
        .set_conf_state(&raft::eraftpb::ConfState {
            voters: group.voters(),
            ..Default::default()
        })
        .unwrap();
    let storage = PeerStorage::new(
        rt.clone(),
        node_id,
        log_store,
        app_store,
        client.clone(),
        conf,
    );
    let raw = RawNode::new(
        &conf.new_raft_conf(node_id, 0),
        storage.clone(),
        &slog::Logger::root(slog::Discard, slog::o!()),
    )
    .unwrap();
    let (sender, receiver) = mpsc::channel(conf.message_size);
    let (session_sender, session_receiver) = mpsc::unbounded_channel();

    RaftNode {
        rt,
        raw,
        client,
        session: 100 * node_id + 1,
        peer_sessions: PeerSessions::default(),
        heartbeat_sequence: 0,
        session_sender,
        session_receiver,
        recovery: Recovery::load(conf, node_id).unwrap(),
        pending_ready: None,
        snapshot_done: None,
        storage,
        receiver,
        sender,
        group,
        role_monitor,
        tick_interval: Duration::from_millis(conf.raft_tick_interval_ms),
        max_batch_size: conf.raft_batch_size.max(1),
        snapshot_interval_ms: 0,
        snapshot_min_interval_ms: 0,
        snapshot_entries: conf.snapshot_entries,
        last_snapshot_ms: 0,
        last_snapshot_op_id: 0,
    }
}

fn become_leader_and_fence<A: LogStorage, B: AppStorage>(node: &mut RaftNode<A, B>) {
    node.raw.raft.become_candidate();
    node.raw.raft.become_leader();
    let term = node.raw.raft.term;
    assert!(node.peer_sessions.begin(3, term, 100));
    node.peer_sessions.observe(
        &mut node.raw,
        SessionEvent {
            peer: 3,
            term,
            heartbeat: 100,
            response: Some(RaftResponse {
                session: Some(301),
                term: Some(term),
                recovering: Some(false),
            }),
        },
    );
    assert_eq!(node.peer_sessions.get(3), Some(301));
}

/// Freeze the app store at `index` so the compaction ceiling derived from the
/// fsm state is predictable.
fn freeze_applied_index<A: LogStorage, B: AppStorage>(node: &mut RaftNode<A, B>, index: u64) {
    let applied = AppliedIndex {
        term: 1,
        index,
        op_id: 0,
        rpc_id: 0,
    };
    node.rt
        .block_on(node.storage.app_store.apply_snapshot(SnapshotData {
            snapshot_id: index,
            node_id: 1,
            create_time: LocalTime::mills(),
            bytes_data: Some(SerdeUtils::serialize(&HashMap::<String, String>::new()).unwrap()),
            files_data: None,
            fsm_state: FsmState {
                applied: applied.clone(),
                ufs_applied: applied,
            },
        }))
        .unwrap();
}

fn remove_entry(id: u64) -> Entry {
    let change = ConfChange {
        change_type: ConfChangeType::RemoveNode.into(),
        node_id: id,
        ..Default::default()
    };
    Entry {
        data: change.encode_to_vec(),
        ..Default::default()
    }
}

#[test]
fn successful_remove_node_clears_the_production_session_state() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt.clone(), 1);
    become_leader_and_fence(&mut node);

    node.apply_config_change(&remove_entry(3)).unwrap();

    assert_eq!(node.peer_sessions.get(3), None);
    assert!(node.raw.raft.prs().get(3).is_none());
}

#[test]
fn rejected_remove_node_keeps_the_existing_session_fence() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt.clone(), 1);
    become_leader_and_fence(&mut node);

    let mut joint = ConfChangeV2::default();
    joint.set_transition(ConfChangeTransition::Explicit);
    joint.set_changes(vec![ConfChangeSingle {
        change_type: ConfChangeType::RemoveNode.into(),
        node_id: 2,
    }]);
    node.raw.apply_conf_change(&joint).unwrap();

    assert!(node.apply_config_change(&remove_entry(3)).is_err());
    assert_eq!(node.peer_sessions.get(3), Some(301));
    assert!(node.raw.raft.prs().get(3).is_some());
}

fn hold_current_ready<A: LogStorage, B: AppStorage>(node: &mut RaftNode<A, B>) {
    if !node.raw.has_ready() {
        node.raw.raft.become_candidate();
    }
    assert!(node.raw.has_ready());
    let ready = node.raw.ready();
    node.pending_ready = Some(PendingReady {
        ready,
        soft_state: None,
        demoted: false,
    });
}

#[test]
fn pending_ready_keeps_ping_available_and_rejects_raft_mutation() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt.clone(), 1);
    hold_current_ready(&mut node);
    let term = node.raw.raft.term;
    let last_index = node.raw.raft.raft_log.last_index();
    let mut promise = HashMap::new();

    let (ping_tx, ping_rx) = tokio::sync::oneshot::channel();
    let ping = Builder::new_rpc(RaftCode::Ping).build();
    node.handle_available(Envelope::new(ping, ping_tx), &mut promise)
        .unwrap();
    let ping_response = rt.block_on(ping_rx).unwrap().unwrap();
    assert_eq!(ping_response.response_status(), ResponseStatus::Success);

    let mut message = raft::eraftpb::Message {
        from: 2,
        to: 1,
        term: term + 1,
        ..Default::default()
    };
    message.set_msg_type(MessageType::MsgHeartbeat);
    let request = RaftRequest {
        message,
        sender_session: Some(201),
        receiver_session: None,
        leader_commit: Some(0),
    };
    let (raft_tx, raft_rx) = tokio::sync::oneshot::channel();
    let raft_message = Builder::new_rpc(RaftCode::Raft)
        .proto_header(request)
        .build();
    node.handle_available(Envelope::new(raft_message, raft_tx), &mut promise)
        .unwrap();
    let raft_response = rt.block_on(raft_rx).unwrap().unwrap();
    assert_eq!(raft_response.response_status(), ResponseStatus::Error);
    assert_eq!(node.raw.raft.term, term);
    assert_eq!(node.raw.raft.raft_log.last_index(), last_index);
    assert!(node.pending_ready.is_some());
}

#[test]
fn snapshot_completion_advances_the_held_ready_exactly_once() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt.clone(), 1);
    seed_applied_state(&node, 100);
    hold_current_ready(&mut node);
    node.last_snapshot_op_id = 0;
    node.last_snapshot_ms = 0;
    // Isolate the re-anchor: the create-snapshot branch must not fire, so only
    // `anchor_snapshot_markers()` can move the counters.
    node.snapshot_interval_ms = u64::MAX;
    node.snapshot_entries = u64::MAX;
    let mut promise = HashMap::new();

    rt.block_on(node.complete_pending_snapshot(Ok(()), &mut promise))
        .unwrap();
    assert!(
        node.storage.can_generate_snapshot(),
        "completing a snapshot must not schedule a create job"
    );
    assert_eq!(
        node.last_snapshot_op_id, 100,
        "completing a snapshot must re-anchor the entry counter at the applied state"
    );
    assert!(node.last_snapshot_ms > 0);
    assert!(node.pending_ready.is_none());
    let error = rt
        .block_on(node.complete_pending_snapshot(Ok(()), &mut promise))
        .expect_err("one Ready must not be completed twice");
    assert!(error.to_string().contains("without a pending Ready"));
}

#[test]
fn snapshot_failure_keeps_the_ready_unadvanced_for_node_shutdown() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt.clone(), 1);
    hold_current_ready(&mut node);
    let mut promise = HashMap::new();

    let error = rt
        .block_on(node.complete_pending_snapshot(
            Err(RaftError::other("injected snapshot failure".into())),
            &mut promise,
        ))
        .expect_err("snapshot failure must stop Ready completion");
    assert!(error.to_string().contains("injected snapshot failure"));
    assert!(node.pending_ready.is_some());
    assert!(node.raw.has_ready());
}

fn wait_for_snapshot_job(node: &RaftNode<MemLogStorage, HashAppStorage<String, String>>) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !node.storage.can_generate_snapshot() {
        assert!(
            Instant::now() < deadline,
            "the snapshot job did not finish in time"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_first_index<A: LogStorage, B: AppStorage>(node: &RaftNode<A, B>, expected: u64) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let first_index = node.storage.log_store.first_index().unwrap();
        if first_index == expected {
            return first_index;
        }
        assert!(
            Instant::now() < deadline,
            "the journal first index settled at {} instead of {}",
            first_index,
            expected
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn reported_snapshot(node_id: u64, index: u64) -> SnapshotData {
    let applied = AppliedIndex {
        term: 1,
        index,
        op_id: index,
        rpc_id: 0,
    };
    SnapshotData {
        snapshot_id: 1,
        node_id,
        create_time: LocalTime::mills(),
        bytes_data: Some(SerdeUtils::serialize(&HashMap::<String, String>::new()).unwrap()),
        files_data: None,
        fsm_state: FsmState {
            applied: applied.clone(),
            ufs_applied: applied,
        },
    }
}

fn applied_state(node_id: u64, op_id: u64) -> SnapshotData {
    SnapshotData {
        snapshot_id: 1,
        node_id,
        create_time: LocalTime::mills(),
        bytes_data: Some(SerdeUtils::serialize(&HashMap::<String, String>::new()).unwrap()),
        files_data: None,
        fsm_state: FsmState {
            applied: AppliedIndex {
                term: 1,
                index: 1,
                op_id,
                rpc_id: 0,
            },
            ..Default::default()
        },
    }
}

fn seed_applied_state<A: LogStorage, B: AppStorage>(node: &RaftNode<A, B>, op_id: u64) {
    node.rt
        .block_on(
            node.storage
                .app_store
                .apply_snapshot(applied_state(node.id(), op_id)),
        )
        .unwrap();
}

#[test]
fn restarted_node_does_not_create_a_snapshot_from_restored_state() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt.clone(), 1);

    // A restarted or newly joined node restores op_id 100 from its snapshot or
    // from journal replay, but this process has never created a snapshot.
    seed_applied_state(&node, 100);
    node.storage
        .append(&[Entry {
            index: 1,
            term: 1,
            ..Default::default()
        }])
        .unwrap();
    node.storage
        .set_hard_state(&HardState {
            term: 1,
            commit: 1,
            ..Default::default()
        })
        .unwrap();

    node.snapshot_interval_ms = 6 * 60 * 60 * 1000;
    node.snapshot_min_interval_ms = 0;
    node.snapshot_entries = 10;
    node.anchor_snapshot_markers();
    let anchored_ms = node.last_snapshot_ms;

    // Counting from op_id 0 would make the restored state look like a full
    // batch of unsnapshotted entries and create a snapshot on every start.
    node.apply_create_snapshot().unwrap();
    assert_eq!(node.last_snapshot_op_id, 100);
    assert_eq!(node.last_snapshot_ms, anchored_ms);
    assert!(
        node.storage.can_generate_snapshot(),
        "startup must not schedule a snapshot job"
    );

    // Entries applied after the anchor still trigger a snapshot.
    seed_applied_state(&node, 120);
    node.apply_create_snapshot().unwrap();
    assert_eq!(node.last_snapshot_op_id, 120);
    wait_for_snapshot_job(&node);
}

#[test]
fn zero_snapshot_entries_disables_follower_snapshots() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt.clone(), 1);

    seed_applied_state(&node, 100);
    node.storage
        .append(&[Entry {
            index: 1,
            term: 1,
            ..Default::default()
        }])
        .unwrap();
    node.storage
        .set_hard_state(&HardState {
            term: 1,
            commit: 1,
            ..Default::default()
        })
        .unwrap();

    node.snapshot_interval_ms = 0;
    node.snapshot_min_interval_ms = 0;
    node.snapshot_entries = 0;
    node.anchor_snapshot_markers();
    let anchored_op_id = node.last_snapshot_op_id;

    seed_applied_state(&node, 200);
    node.apply_create_snapshot().unwrap();

    assert_eq!(
        node.last_snapshot_op_id, anchored_op_id,
        "snapshot_entries = 0 must not create a snapshot for every entry"
    );
    assert!(
        node.storage.can_generate_snapshot(),
        "no snapshot job may be scheduled when snapshot creation is disabled"
    );
}

// Drive the real startup path: the counter must come from the restored state
// through `new_candidate`, not from test scaffolding.
#[test]
fn a_restarting_candidate_anchors_the_snapshot_marker_at_the_restored_state() {
    let mut conf = JournalConf::with_test();
    conf.rpc_retry_max_duration_ms = 0;
    conf.conn_retry_max_duration_ms = 0;
    conf.journal_dir = std::env::temp_dir()
        .join(format!("curvine-raft-anchor-{}", Utils::rand_id()))
        .to_str()
        .unwrap()
        .to_string();
    conf.journal_addrs = vec![1, 2, 3]
        .into_iter()
        .map(|peer_id| {
            let port = if peer_id == 1 {
                conf.rpc_port
            } else {
                NetUtils::get_available_port()
            };
            RaftPeer::new(peer_id, &conf.hostname, port)
        })
        .collect();
    let group = RaftGroup::from_conf(&conf);
    let node_id = group.get_node_id(&conf.local_addr()).unwrap();
    assert_eq!(node_id, 1);
    let journal_dir = conf.journal_dir.clone();

    // A restarted node restores op_id 100 from its journal before it applies
    // anything new, and no raft snapshot exists yet.
    let log_store = RocksLogStorage::from_conf(&conf, true);
    log_store
        .set_conf_state(&raft::eraftpb::ConfState {
            voters: group.voters(),
            ..Default::default()
        })
        .unwrap();
    log_store
        .append(&[Entry {
            index: 1,
            term: 1,
            ..Default::default()
        }])
        .unwrap();
    log_store
        .set_hard_state(&HardState {
            term: 1,
            commit: 1,
            ..Default::default()
        })
        .unwrap();

    let app_store: HashAppStorage<String, String> = HashAppStorage::new();
    let rt = Arc::new(Runtime::new("test-snapshot-anchor", 2, 2));
    rt.block_on(app_store.apply_snapshot(applied_state(node_id, 100)))
        .unwrap();

    let (sender, receiver) = mpsc::channel(conf.message_size);
    let mut node = rt
        .block_on(RaftNode::new_candidate(
            rt.clone(),
            &conf,
            log_store,
            app_store,
            RoleMonitor::new(),
            receiver,
            sender,
            &slog::Logger::root(slog::Discard, slog::o!()),
        ))
        .unwrap();

    assert_eq!(node.storage.get_fsm_state().op_id(), 100);
    assert_eq!(
        node.last_snapshot_op_id, 100,
        "the constructor must count the restored entries as already snapshotted"
    );

    // The restored state is the anchor, not a batch of unsnapshotted entries.
    // `finish_ready` calls this for a Ready without a role change, so it covers
    // the first Ready after startup.
    node.snapshot_interval_ms = 0;
    node.snapshot_min_interval_ms = 0;
    node.snapshot_entries = 10;
    let anchored_ms = node.last_snapshot_ms;
    node.apply_create_snapshot().unwrap();
    assert_eq!(node.last_snapshot_op_id, 100);
    assert_eq!(node.last_snapshot_ms, anchored_ms);
    assert!(
        node.storage.can_generate_snapshot(),
        "startup must not schedule a snapshot job"
    );

    drop(node);
    let _ = std::fs::remove_dir_all(&journal_dir);
}

// Stepping down reinstalls the local snapshot; the counters must move with it
// instead of counting the reinstalled state as new work.
#[test]
fn leader_step_down_reinstalls_the_local_snapshot_and_reanchors_the_markers() {
    let rt = Arc::new(Runtime::new("test-step-down-anchor", 2, 2));
    let (mut node, conf) = test_rocks_node(rt.clone(), 1);
    let journal_dir = conf.journal_dir.clone();

    seed_applied_state(&node, 100);
    node.last_snapshot_op_id = 0;
    node.last_snapshot_ms = 0;
    hold_current_ready(&mut node);
    let pending = node.pending_ready.take().unwrap();

    // The role monitor still reports leader while raft has stepped down, which
    // is the transition that reinstalls the local snapshot.
    node.role_monitor.advance_role(&SoftState {
        leader_id: 1,
        raft_state: raft::StateRole::Leader,
    });
    assert!(node.role_monitor.is_leader());
    assert!(!node.is_leader());

    let mut promise = HashMap::new();
    rt.block_on(node.finish_ready(
        pending.ready,
        Some(SoftState {
            leader_id: 2,
            raft_state: raft::StateRole::Follower,
        }),
        &mut promise,
        false,
    ))
    .unwrap();

    assert!(
        !node.role_monitor.is_leader(),
        "the step-down must be published before the local snapshot is reinstalled"
    );

    assert_eq!(
        node.last_snapshot_op_id, 100,
        "the local snapshot reinstall must re-anchor the entry counter"
    );
    assert!(node.last_snapshot_ms > 0);

    drop(node);
    let _ = std::fs::remove_dir_all(&journal_dir);
}

// The reinstall is the only follower-mode replay of the committed tail, so the
// step-down has to be visible from inside it. A post-completion role check
// would also pass with the old ordering.
#[test]
fn finish_ready_publishes_the_step_down_before_the_snapshot_reinstall() {
    let rt = Arc::new(Runtime::new("test-step-down-role", 2, 2));
    let monitor = RoleMonitor::new();
    let app = RoleRecordingAppStorage::new(HashAppStorage::new(), monitor.read_ctl());
    let mut conf = JournalConf::with_test();
    let mut node = test_rocks_node_with_app(rt.clone(), 1, &mut conf, app.clone(), monitor);
    let journal_dir = conf.journal_dir.clone();

    seed_applied_state(&node, 100);
    node.last_snapshot_op_id = 0;
    node.last_snapshot_ms = 0;
    hold_current_ready(&mut node);
    let pending = node.pending_ready.take().unwrap();

    node.role_monitor.advance_role(&SoftState {
        leader_id: 1,
        raft_state: StateRole::Leader,
    });
    assert!(node.role_monitor.is_leader());
    assert!(!node.is_leader());

    let mut promise = HashMap::new();
    rt.block_on(node.finish_ready(
        pending.ready,
        Some(SoftState {
            leader_id: 2,
            raft_state: StateRole::Follower,
        }),
        &mut promise,
        false,
    ))
    .unwrap();

    let replayed = app.replayed_as_leader();
    assert!(!replayed.is_empty(), "the reinstall must replay the tail");
    assert!(
        !replayed.iter().any(|leader| *leader),
        "the follower-mode replay must observe the published Follower role"
    );

    drop(node);
    let _ = std::fs::remove_dir_all(&journal_dir);
}

// `on_ready` parks a snapshot-bearing Ready; the business role must already be
// follower while the snapshot is still pending.
#[test]
fn pending_snapshot_publishes_the_step_down_before_parking_the_ready() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt.clone(), 1);
    node.recovery.active = false;
    node.role_monitor.advance_role(&SoftState {
        leader_id: 1,
        raft_state: StateRole::Leader,
    });
    assert!(node.role_monitor.is_leader());

    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 5;
    snapshot.mut_metadata().term = 3;
    snapshot.mut_metadata().mut_conf_state().voters = vec![1, 2, 3];

    let mut message = Message::default();
    message.set_msg_type(MessageType::MsgSnapshot);
    message.from = 2;
    message.to = 1;
    message.term = 3;
    message.set_snapshot(snapshot);

    node.raw.step(message).unwrap();
    assert!(node.raw.has_ready());

    let mut promise = HashMap::new();
    rt.block_on(node.on_ready(&mut promise)).unwrap();

    assert!(
        node.pending_ready.is_some(),
        "the snapshot Ready must be parked"
    );
    assert!(
        !node.role_monitor.is_leader(),
        "on_ready must publish the step-down while the snapshot is pending"
    );
}

// `on_ready` already published the step-down before parking the Ready, so when
// the snapshot completes the monitor no longer reports leader and only the
// recorded transition keeps the follower-mode reinstall alive.
#[test]
fn pending_snapshot_completion_keeps_the_follower_mode_reinstall() {
    let rt = Arc::new(Runtime::new("test-pending-step-down-role", 2, 2));
    let monitor = RoleMonitor::new();
    let app = RoleRecordingAppStorage::new(HashAppStorage::new(), monitor.read_ctl());
    let mut conf = JournalConf::with_test();
    let mut node = test_rocks_node_with_app(rt.clone(), 1, &mut conf, app.clone(), monitor);
    let journal_dir = conf.journal_dir.clone();

    seed_applied_state(&node, 100);
    hold_current_ready(&mut node);
    let mut pending = node.pending_ready.take().unwrap();
    pending.soft_state = Some(SoftState {
        leader_id: 2,
        raft_state: StateRole::Follower,
    });
    pending.demoted = true;

    node.role_monitor.advance_role(&SoftState {
        leader_id: 1,
        raft_state: StateRole::Leader,
    });
    assert!(node.role_monitor.is_leader());
    node.role_monitor.step_down();
    assert!(!node.role_monitor.is_leader());
    node.pending_ready = Some(pending);

    let mut promise = HashMap::new();
    rt.block_on(node.complete_pending_snapshot(Ok(()), &mut promise))
        .unwrap();

    let replayed = app.replayed_as_leader();
    assert!(
        !replayed.is_empty(),
        "resuming a parked snapshot must still reinstall it and replay the tail"
    );
    assert!(
        !replayed.iter().any(|leader| *leader),
        "the reinstall must replay as a follower"
    );

    drop(node);
    let _ = std::fs::remove_dir_all(&journal_dir);
}

#[test]
fn follower_snapshot_is_throttled_by_min_interval() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt.clone(), 1);

    // Freeze a follower state whose applied op_id (2) exceeds the
    // snapshot_entries trigger (1), so only the min interval holds snapshots
    // back.
    let fsm_state = FsmState {
        applied: AppliedIndex {
            term: 1,
            index: 1,
            op_id: 2,
            rpc_id: 0,
        },
        ..Default::default()
    };
    rt.block_on(node.storage.app_store.apply_snapshot(SnapshotData {
        snapshot_id: 1,
        node_id: 1,
        create_time: LocalTime::mills(),
        bytes_data: Some(SerdeUtils::serialize(&HashMap::<String, String>::new()).unwrap()),
        files_data: None,
        fsm_state,
    }))
    .unwrap();
    // The snapshot job builds the snapshot metadata from the committed log
    // entry (index 1, term 1), so seed a matching log entry and hard state.
    node.storage
        .append(&[Entry {
            index: 1,
            term: 1,
            ..Default::default()
        }])
        .unwrap();
    node.storage
        .set_hard_state(&HardState {
            term: 1,
            commit: 1,
            ..Default::default()
        })
        .unwrap();

    node.snapshot_interval_ms = 0;
    node.snapshot_min_interval_ms = 10 * 60 * 1000;
    node.snapshot_entries = 1;
    // Freeze the markers as if no snapshot had been recorded for the restored
    // state: the restored op_id then counts as pending work and only the min
    // interval can hold the next snapshot back.
    node.last_snapshot_ms = LocalTime::mills();
    node.last_snapshot_op_id = 0;

    // First check: the interval is open, so a create job is generated and
    // both markers advance.
    node.apply_create_snapshot().unwrap();
    let first_ms = node.last_snapshot_ms;
    let first_op_id = node.last_snapshot_op_id;
    assert!(first_ms > 0, "the first check must generate a snapshot job");
    assert_eq!(
        first_op_id, 2,
        "the first check must generate a snapshot job"
    );
    wait_for_snapshot_job(&node);

    // Push the applied op_id ahead by 2 so the entry count alone already
    // crosses the trigger (diff 2 > snapshot_entries 1): only the min
    // interval guard can hold the next snapshot back.
    rt.block_on(node.storage.app_store.apply_snapshot(SnapshotData {
        snapshot_id: 2,
        node_id: 1,
        create_time: LocalTime::mills(),
        bytes_data: Some(SerdeUtils::serialize(&HashMap::<String, String>::new()).unwrap()),
        files_data: None,
        fsm_state: FsmState {
            applied: AppliedIndex {
                term: 1,
                index: 1,
                op_id: 4,
                rpc_id: 0,
            },
            ..Default::default()
        },
    }))
    .unwrap();

    // Second check inside the interval: it is skipped, so no new create job
    // is generated and the markers stay unchanged.
    node.apply_create_snapshot().unwrap();
    assert_eq!(
        node.last_snapshot_ms, first_ms,
        "a second snapshot inside the interval must be skipped"
    );
    assert_eq!(
        node.last_snapshot_op_id, first_op_id,
        "a second snapshot inside the interval must be skipped"
    );

    // Once the interval expires, the next check may create a new snapshot
    // without resetting the op_id marker.
    let expired_anchor = LocalTime::mills().saturating_sub(node.snapshot_min_interval_ms + 1);
    node.last_snapshot_ms = expired_anchor;
    node.apply_create_snapshot().unwrap();
    assert!(
        node.last_snapshot_ms > expired_anchor,
        "the interval must allow a new snapshot afterwards"
    );
    assert_eq!(node.last_snapshot_op_id, 4);
    wait_for_snapshot_job(&node);
}

#[test]
fn min_commit_is_unbounded_on_a_follower() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt, 1);
    freeze_applied_index(&mut node, 100);

    // A follower trims only its own log, so no peer bounds it and the applied
    // index caps it where the snapshot itself is trimmed.
    assert_eq!(node.min_commit(), None);
}

#[test]
fn min_commit_on_a_leader_follows_the_slowest_live_voter() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt, 1);
    become_leader_and_fence(&mut node);
    freeze_applied_index(&mut node, 100);

    for (voter, committed) in [(2, 80), (3, 50)] {
        let pr = node.raw.raft.mut_prs().get_mut(voter).unwrap();
        pr.committed_index = committed;
        pr.recent_active = true;
    }

    assert_eq!(node.min_commit(), Some(50));
}

#[test]
fn min_commit_drops_a_voter_that_stopped_responding() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt, 1);
    become_leader_and_fence(&mut node);
    freeze_applied_index(&mut node, 100);

    for (voter, committed) in [(2, 80), (3, 50)] {
        let pr = node.raw.raft.mut_prs().get_mut(voter).unwrap();
        pr.committed_index = committed;
    }
    node.raw.raft.mut_prs().get_mut(2).unwrap().recent_active = true;
    // Voter 3 has been silent for longer than an election timeout, so its last
    // reported commit must not hold the ceiling back.

    assert_eq!(node.min_commit(), Some(80));
}

#[test]
fn min_commit_holds_at_a_voter_installing_a_snapshot() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt, 1);
    become_leader_and_fence(&mut node);
    freeze_applied_index(&mut node, 100);

    for (voter, committed) in [(2, 80), (3, 50)] {
        let pr = node.raw.raft.mut_prs().get_mut(voter).unwrap();
        pr.committed_index = committed;
    }
    node.raw.raft.mut_prs().get_mut(2).unwrap().recent_active = true;
    // Voter 3 is installing a snapshot at 60 and still reports 50. It cannot
    // answer heartbeats while the snapshot is applied, so the quorum check has
    // cleared its liveness flag; the ceiling must still stay at 50 instead of
    // jumping to the healthy voter or to the local applied index.
    let pr = node.raw.raft.mut_prs().get_mut(3).unwrap();
    pr.become_snapshot(60);
    assert!(!pr.recent_active);

    assert_eq!(node.min_commit(), Some(50));
}

#[test]
fn min_commit_without_a_live_voter_uses_the_last_known_commit() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt, 1);
    become_leader_and_fence(&mut node);
    freeze_applied_index(&mut node, 100);

    for (voter, committed) in [(2, 80), (3, 50)] {
        let pr = node.raw.raft.mut_prs().get_mut(voter).unwrap();
        pr.committed_index = committed;
    }
    // The periodic quorum check resets liveness, so no voter is marked active
    // right now; the ceiling must not jump to the local applied index.

    assert_eq!(node.min_commit(), Some(50));
}

#[test]
fn min_commit_is_unbounded_on_a_single_voter_leader() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node_with_voters(rt, 1, vec![1]);
    node.raw.raft.become_candidate();
    node.raw.raft.become_leader();
    freeze_applied_index(&mut node, 100);

    // A single-voter group has nobody to hold the trim back, so the local
    // applied index is the only limit.
    assert_eq!(node.min_commit(), None);
}

#[test]
fn min_commit_with_unreported_voters_trims_nothing() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt, 1);
    become_leader_and_fence(&mut node);
    freeze_applied_index(&mut node, 100);

    // Voters that never reported keep `Progress::new()`'s zero commit, so the
    // ceiling stays at 0 and the trim is a no-op instead of falling back to the
    // local applied index.
    for voter in [2, 3] {
        node.raw
            .raft
            .mut_prs()
            .get_mut(voter)
            .unwrap()
            .recent_active = true;
    }

    assert_eq!(node.min_commit(), Some(0));
}

#[test]
fn reported_snapshot_trims_the_journal_up_to_the_peer_watermark() {
    let rt = JournalConf::with_test().create_runtime();
    let mut conf = JournalConf::with_test();
    // An empty retention window keeps the trim right at the bound.
    conf.snapshot_entries = 0;
    let mut node = test_rocks_node_with_conf(rt, 1, &mut conf);
    become_leader_and_fence(&mut node);
    freeze_applied_index(&mut node, 100);

    let entries: Vec<Entry> = (1..=100)
        .map(|index| Entry {
            index,
            term: 1,
            ..Default::default()
        })
        .collect();
    node.storage.append(&entries).unwrap();
    node.storage.set_hard_state_commit(100).unwrap();

    for (voter, committed) in [(2, 80), (3, 50)] {
        let pr = node.raw.raft.mut_prs().get_mut(voter).unwrap();
        pr.committed_index = committed;
        pr.recent_active = true;
    }

    // The reporter has applied up to 90, but voter 3 still needs 51 onwards.
    node.storage
        .register_snapshot(reported_snapshot(2, 90), node.min_commit())
        .unwrap();

    assert_eq!(wait_for_first_index(&node, 50), 50);
}

#[test]
fn reported_snapshot_with_unreported_voters_trims_nothing() {
    let rt = JournalConf::with_test().create_runtime();
    let mut conf = JournalConf::with_test();
    conf.snapshot_entries = 0;
    let mut node = test_rocks_node_with_conf(rt, 1, &mut conf);
    become_leader_and_fence(&mut node);
    freeze_applied_index(&mut node, 100);

    let entries: Vec<Entry> = (1..=100)
        .map(|index| Entry {
            index,
            term: 1,
            ..Default::default()
        })
        .collect();
    node.storage.append(&entries).unwrap();
    node.storage.set_hard_state_commit(100).unwrap();

    // Voters that never reported keep `Progress::new()`'s zero commit, so the
    // trim must be a no-op instead of falling back to the local applied index.
    for voter in [2, 3] {
        node.raw
            .raft
            .mut_prs()
            .get_mut(voter)
            .unwrap()
            .recent_active = true;
    }

    node.storage
        .register_snapshot(reported_snapshot(2, 90), node.min_commit())
        .unwrap();

    assert_eq!(wait_for_first_index(&node, 1), 1);
}

#[test]
fn wiped_follower_early_rejoin_handshake_clamps_obsolete_commit_without_panic() {
    let rt = JournalConf::with_test().create_runtime();
    let follower_id = 3;
    let leader_id = 2;
    let mut node = test_node(rt.clone(), follower_id);
    assert_eq!(node.id(), follower_id);
    assert_eq!(node.raw.raft.raft_log.last_index(), 0);
    assert_eq!(node.raw.raft.raft_log.committed, 0);
    assert!(!node.recovery.active);
    assert_eq!(node.session, 301);

    let leader_term = 5;
    let high_commit = 105311900;
    let old_receiver_session = 99;

    let mut message = raft::eraftpb::Message {
        from: leader_id,
        to: node.id(),
        term: leader_term,
        commit: high_commit,
        ..Default::default()
    };
    message.set_msg_type(MessageType::MsgHeartbeat);
    let request = RaftRequest {
        message,
        sender_session: Some(201),
        receiver_session: Some(old_receiver_session),
        leader_commit: Some(high_commit),
    };

    let mut promise = HashMap::new();
    let (raft_tx, raft_rx) = tokio::sync::oneshot::channel();
    let raft_message = Builder::new_rpc(RaftCode::Raft)
        .proto_header(request)
        .build();

    node.handle_available(Envelope::new(raft_message, raft_tx), &mut promise)
        .unwrap();
    let response = rt.block_on(raft_rx).unwrap().unwrap();

    // The early handshake must not fail or panic.
    assert_eq!(response.response_status(), ResponseStatus::Success);

    let raft_response: RaftResponse = response.parse_header().unwrap();
    assert_eq!(raft_response.session, Some(node.session));
    assert_eq!(raft_response.term, Some(leader_term));
    assert_eq!(raft_response.recovering, Some(true));

    // Follower must not commit beyond its available local log.
    assert_eq!(node.raw.raft.raft_log.committed, 0);
    assert_eq!(node.raw.raft.raft_log.last_index(), 0);
    assert_eq!(node.raw.raft.leader_id, leader_id);

    // Follower must enter recovery quarantine, record abstention, and persist the marker.
    assert!(node.recovery.active);
    assert_eq!(node.raw.raft.vote, node.id());
    assert!(node.storage.conf.recovery_marker().exists());
    assert_eq!(
        std::fs::read_to_string(node.storage.conf.recovery_marker()).unwrap(),
        format!("{}\n", node.id())
    );

    // Before Ready is advanced, the storage has not yet persisted the new hard state.
    assert_eq!(
        node.storage.initial_state().unwrap().hard_state.vote,
        0,
        "storage has not yet persisted the self-vote before on_ready"
    );

    // Advance Ready to trigger hard-state persistence.
    assert!(node.raw.has_ready());
    rt.block_on(node.on_ready(&mut promise)).unwrap();

    // Now the hard state in durable storage MUST have recorded the self-vote.
    assert_eq!(
        node.storage.initial_state().unwrap().hard_state.vote,
        node.id(),
        "storage must persist self-vote via finish_ready before outbound replication can occur"
    );
    assert_eq!(
        node.storage.initial_state().unwrap().hard_state.term,
        leader_term
    );

    // While quarantined in recovery mode, voting is rejected and abstention is preserved.
    let (vote_tx, vote_rx) = tokio::sync::oneshot::channel();
    let mut vote_message = raft::eraftpb::Message {
        from: leader_id,
        to: node.id(),
        term: leader_term + 1,
        ..Default::default()
    };
    vote_message.set_msg_type(MessageType::MsgRequestVote);
    let vote_request = RaftRequest {
        message: vote_message,
        sender_session: Some(201),
        receiver_session: Some(node.session),
        leader_commit: Some(0),
    };
    let vote_rpc = Builder::new_rpc(RaftCode::Raft)
        .proto_header(vote_request)
        .build();
    node.handle_available(Envelope::new(vote_rpc, vote_tx), &mut promise)
        .unwrap();
    let vote_response = rt.block_on(vote_rx).unwrap().unwrap();
    assert_eq!(vote_response.response_status(), ResponseStatus::Error);
    assert_ne!(node.raw.raft.vote, leader_id);
    assert_eq!(node.raw.raft.vote, node.id());

    // Local election timeouts and campaigning are rejected during recovery quarantine.
    let (camp_tx, camp_rx) = tokio::sync::oneshot::channel();
    let mut camp_message = raft::eraftpb::Message {
        from: node.id(),
        to: node.id(),
        term: leader_term + 1,
        ..Default::default()
    };
    camp_message.set_msg_type(MessageType::MsgHup);
    let camp_request = RaftRequest {
        message: camp_message,
        sender_session: Some(node.session),
        receiver_session: Some(node.session),
        leader_commit: Some(0),
    };
    let camp_rpc = Builder::new_rpc(RaftCode::Raft)
        .proto_header(camp_request)
        .build();
    node.handle_available(Envelope::new(camp_rpc, camp_tx), &mut promise)
        .unwrap();
    let camp_response = rt.block_on(camp_rx).unwrap().unwrap();
    assert_eq!(camp_response.response_status(), ResponseStatus::Error);
    assert_eq!(node.raw.raft.state, raft::StateRole::Follower);

    // Recovery mode and durable marker survive restart even without explicit config.
    let reloaded = Recovery::load(&node.storage.conf, node.id()).unwrap();
    assert!(reloaded.active);

    // When the leader observes this response, verify that:
    // 1. Leader learns the new session.
    // 2. Leader treats the node as a replacement and resets replication progress.
    // 3. Leader does not trust stale progress and initiates probe/snapshot catch-up.
    let mut leader = test_node(rt.clone(), leader_id);
    assert_ne!(leader.id(), node.id());
    assert_eq!(leader.id(), leader_id);
    assert_eq!(leader.session, 201);
    leader.raw.raft.become_candidate();
    leader.raw.raft.become_leader();
    leader.raw.raft.term = leader_term;
    assert!(leader.peer_sessions.begin(node.id(), leader_term, 1));
    leader.peer_sessions.observe(
        &mut leader.raw,
        SessionEvent {
            peer: node.id(),
            term: leader_term,
            heartbeat: 1,
            response: Some(RaftResponse {
                session: Some(old_receiver_session),
                term: Some(leader_term),
                recovering: Some(false),
            }),
        },
    );
    assert_eq!(
        leader.peer_sessions.get(node.id()),
        Some(old_receiver_session)
    );
    {
        let pr = leader.raw.raft.mut_prs().get_mut(node.id()).unwrap();
        pr.matched = high_commit;
        pr.committed_index = high_commit;
        pr.recent_active = true;
    }

    assert!(leader.peer_sessions.begin(node.id(), leader_term, 2));
    leader.peer_sessions.observe(
        &mut leader.raw,
        SessionEvent {
            peer: node.id(),
            term: leader_term,
            heartbeat: 2,
            response: Some(raft_response),
        },
    );
    assert_eq!(leader.peer_sessions.get(node.id()), Some(node.session));
    let pr = leader.raw.raft.prs().get(node.id()).unwrap();
    assert_eq!(pr.matched, 0);
    assert_eq!(pr.committed_index, 0);
    assert!(!pr.recent_active);
    assert!(leader.raw.raft.msgs.iter().any(|m| m.to == node.id()));
    let catch_up_msg = leader
        .raw
        .raft
        .msgs
        .iter()
        .find(|m| m.to == node.id())
        .expect("leader should queue replication/catch-up message to the recovering follower");
    assert_eq!(catch_up_msg.from, leader.id());
    assert_eq!(catch_up_msg.to, node.id());
    assert_ne!(catch_up_msg.from, catch_up_msg.to);

    let _ = std::fs::remove_dir_all(&node.storage.conf.journal_dir);
    let _ = std::fs::remove_dir_all(&leader.storage.conf.journal_dir);
}

#[test]
fn wiped_follower_early_rejoin_with_zero_commit_heartbeat_activates_quarantine() {
    let rt = JournalConf::with_test().create_runtime();
    let follower_id = 3;
    let leader_id = 2;
    let mut node = test_node(rt.clone(), follower_id);
    assert_eq!(node.id(), follower_id);
    assert_eq!(node.raw.raft.raft_log.last_index(), 0);
    assert_eq!(node.raw.raft.raft_log.committed, 0);
    assert!(!node.recovery.active);
    assert_eq!(node.session, 301);

    let leader_term = 5;
    let leader_commit_val = 105311900;
    let old_receiver_session = 99;

    // Heartbeat has commit = 0, but leader_commit > 0, and receiver session is obsolete.
    let mut message = raft::eraftpb::Message {
        from: leader_id,
        to: node.id(),
        term: leader_term,
        commit: 0,
        ..Default::default()
    };
    message.set_msg_type(MessageType::MsgHeartbeat);
    let request = RaftRequest {
        message,
        sender_session: Some(201),
        receiver_session: Some(old_receiver_session),
        leader_commit: Some(leader_commit_val),
    };

    let mut promise = HashMap::new();
    let (raft_tx, raft_rx) = tokio::sync::oneshot::channel();
    let raft_message = Builder::new_rpc(RaftCode::Raft)
        .proto_header(request)
        .build();

    node.handle_available(Envelope::new(raft_message, raft_tx), &mut promise)
        .unwrap();
    let response = rt.block_on(raft_rx).unwrap().unwrap();
    assert_eq!(response.response_status(), ResponseStatus::Success);

    let raft_response: RaftResponse = response.parse_header().unwrap();
    assert_eq!(raft_response.session, Some(node.session));
    assert_eq!(raft_response.term, Some(leader_term));
    assert_eq!(raft_response.recovering, Some(true));

    // Follower must not commit beyond its available local log.
    assert_eq!(node.raw.raft.raft_log.committed, 0);
    assert_eq!(node.raw.raft.raft_log.last_index(), 0);
    assert_eq!(node.raw.raft.leader_id, leader_id);

    // Follower must enter recovery quarantine, record abstention, and persist the marker.
    assert!(node.recovery.active);
    assert_eq!(node.raw.raft.vote, node.id());
    assert!(node.storage.conf.recovery_marker().exists());
    assert_eq!(
        std::fs::read_to_string(node.storage.conf.recovery_marker()).unwrap(),
        format!("{}\n", node.id())
    );

    // While quarantined in recovery mode, voting is rejected and abstention is preserved.
    let (vote_tx, vote_rx) = tokio::sync::oneshot::channel();
    let mut vote_message = raft::eraftpb::Message {
        from: leader_id,
        to: node.id(),
        term: leader_term + 1,
        ..Default::default()
    };
    vote_message.set_msg_type(MessageType::MsgRequestVote);
    let vote_request = RaftRequest {
        message: vote_message,
        sender_session: Some(201),
        receiver_session: Some(node.session),
        leader_commit: Some(0),
    };
    let vote_rpc = Builder::new_rpc(RaftCode::Raft)
        .proto_header(vote_request)
        .build();
    node.handle_available(Envelope::new(vote_rpc, vote_tx), &mut promise)
        .unwrap();
    let vote_response = rt.block_on(vote_rx).unwrap().unwrap();
    assert_eq!(vote_response.response_status(), ResponseStatus::Error);
    assert_ne!(node.raw.raft.vote, leader_id);
    assert_eq!(node.raw.raft.vote, node.id());

    // Local election timeouts and campaigning are rejected during recovery quarantine.
    let (camp_tx, camp_rx) = tokio::sync::oneshot::channel();
    let mut camp_message = raft::eraftpb::Message {
        from: node.id(),
        to: node.id(),
        term: leader_term + 1,
        ..Default::default()
    };
    camp_message.set_msg_type(MessageType::MsgHup);
    let camp_request = RaftRequest {
        message: camp_message,
        sender_session: Some(node.session),
        receiver_session: Some(node.session),
        leader_commit: Some(0),
    };
    let camp_rpc = Builder::new_rpc(RaftCode::Raft)
        .proto_header(camp_request)
        .build();
    node.handle_available(Envelope::new(camp_rpc, camp_tx), &mut promise)
        .unwrap();
    let camp_response = rt.block_on(camp_rx).unwrap().unwrap();
    assert_eq!(camp_response.response_status(), ResponseStatus::Error);
    assert_eq!(node.raw.raft.state, raft::StateRole::Follower);

    let _ = std::fs::remove_dir_all(&node.storage.conf.journal_dir);
}

#[test]
fn wiped_follower_persists_self_vote_to_rocksdb_before_outbound_replication() {
    let rt = JournalConf::with_test().create_runtime();
    let follower_id = 3;
    let leader_id = 2;
    let (mut node, conf) = test_rocks_node(rt.clone(), follower_id);
    assert_eq!(node.id(), follower_id);
    assert_eq!(node.raw.raft.raft_log.last_index(), 0);
    assert_eq!(node.raw.raft.raft_log.committed, 0);
    assert!(!node.recovery.active);

    // Initially, RocksDB storage has default hard state (term 0, vote 0).
    assert_eq!(node.storage.initial_state().unwrap().hard_state.vote, 0);
    assert_eq!(node.storage.initial_state().unwrap().hard_state.term, 0);

    let leader_term = 7;
    let high_commit = 50000;
    let old_receiver_session = 99;

    let mut message = raft::eraftpb::Message {
        from: leader_id,
        to: node.id(),
        term: leader_term,
        commit: high_commit,
        ..Default::default()
    };
    message.set_msg_type(MessageType::MsgHeartbeat);
    let request = RaftRequest {
        message,
        sender_session: Some(201),
        receiver_session: Some(old_receiver_session),
        leader_commit: Some(high_commit),
    };

    let mut promise = HashMap::new();
    let (raft_tx, raft_rx) = tokio::sync::oneshot::channel();
    let raft_message = Builder::new_rpc(RaftCode::Raft)
        .proto_header(request)
        .build();

    // Heartbeat handling triggers automatic recovery quarantine and records self-vote in-memory.
    node.handle_available(Envelope::new(raft_message, raft_tx), &mut promise)
        .unwrap();
    let response = rt.block_on(raft_rx).unwrap().unwrap();
    assert_eq!(response.response_status(), ResponseStatus::Success);

    let raft_response: RaftResponse = response.parse_header().unwrap();
    assert_eq!(raft_response.recovering, Some(true));
    assert!(node.recovery.active);
    assert_eq!(node.raw.raft.vote, follower_id);
    assert!(conf.recovery_marker().exists());

    // CRITICAL: Before Ready is advanced, the durable RocksDB store has NOT yet persisted the self-vote.
    // The transport handshake reply carried no Raft replication message, so the leader cannot yet trust the peer.
    assert_eq!(
        node.storage.log_store.read().hard_state().vote,
        0,
        "RocksDB must not have recorded the vote before Ready persistence"
    );

    // Advance Ready: on_ready executes finish_ready, which persists HardState to disk before
    // sending outbound Raft messages. This test proves durable persistence after on_ready, while
    // production finish_ready() source ordering (store.set_hard_state(hs) at line 669 strictly
    // precedes self.send_messages() at line 674) guarantees persistence before outbound transmission.
    assert!(node.raw.has_ready());
    rt.block_on(node.on_ready(&mut promise)).unwrap();

    // Now RocksDB has durably persisted the HardState.
    assert_eq!(
        node.storage.log_store.read().hard_state().vote,
        follower_id,
        "RocksDB must record self-vote via set_hard_state before outbound replication messages"
    );
    assert_eq!(node.storage.log_store.read().hard_state().term, leader_term);

    // Simulate node crash and restart from disk:
    // Drop the node to close and release the RocksDB database lock.
    let journal_dir = conf.journal_dir.clone();
    drop(node);

    // Verify durable state directly by reopening RocksDB from disk.
    let reopened_log = RocksLogStorage::from_conf(&conf, false);
    let recovered_state = reopened_log.initial_state().unwrap();
    assert_eq!(
        recovered_state.hard_state.vote, follower_id,
        "reopened RocksDB on disk must retain the persisted self-vote"
    );
    assert_eq!(recovered_state.hard_state.term, leader_term);

    // Verify recovery marker survives restart and activates quarantine.
    let mut reloaded_recovery = Recovery::load(&conf, follower_id).unwrap();
    assert!(
        reloaded_recovery.active,
        "recovery marker on disk must restore active recovery quarantine across crash"
    );

    // Verify that after restart, attempting to grant a vote to another peer in the same term is rejected:
    let group = RaftGroup::from_conf(&conf);
    let client = RaftClient::new(rt.clone(), &group, conf.new_client_conf());
    let peer_storage = PeerStorage::new(
        rt.clone(),
        follower_id,
        reopened_log.clone(),
        HashAppStorage::<String, String>::new(),
        client,
        &conf,
    );
    let mut restarted_raw = RawNode::new(
        &conf.new_raft_conf(follower_id, 0),
        peer_storage,
        &slog::Logger::root(slog::Discard, slog::o!()),
    )
    .unwrap();
    assert_eq!(restarted_raw.raft.vote, follower_id);
    assert_eq!(restarted_raw.raft.term, leader_term);

    // 1. Recovery quarantine rejects the vote request:
    let same_term_vote_req = RaftRequest {
        message: raft::eraftpb::Message {
            from: leader_id,
            to: follower_id,
            term: leader_term,
            ..Default::default()
        },
        sender_session: Some(201),
        receiver_session: Some(301),
        leader_commit: Some(0),
    };
    let mut same_term_msg = same_term_vote_req.clone();
    same_term_msg
        .message
        .set_msg_type(MessageType::MsgRequestVote);
    assert!(reloaded_recovery
        .step(&mut restarted_raw, same_term_msg.clone(), 301)
        .is_err());

    // 2. Underlying Raft state rejects because self-vote abstention was durably restored from RocksDB:
    restarted_raw.step(same_term_msg.message).unwrap();
    assert_eq!(restarted_raw.raft.vote, follower_id);
    assert!(restarted_raw
        .raft
        .msgs
        .iter()
        .any(|m| m.get_msg_type() == MessageType::MsgRequestVoteResponse && m.reject));

    drop(reopened_log);
    let _ = std::fs::remove_dir_all(&journal_dir);
}

#[test]
fn quarantined_node_with_missing_marker_stays_alive_in_quarantine_when_finish_fails() {
    let rt = JournalConf::with_test().create_runtime();
    let follower_id = 3;
    let leader_id = 2;
    let (mut node, conf) = test_rocks_node(rt.clone(), follower_id);
    let leader_term = 7;
    let target_commit = 5;
    let old_receiver_session = 99;

    let mut message = raft::eraftpb::Message {
        from: leader_id,
        to: node.id(),
        term: leader_term,
        commit: target_commit,
        ..Default::default()
    };
    message.set_msg_type(MessageType::MsgHeartbeat);
    let request = RaftRequest {
        message,
        sender_session: Some(201),
        receiver_session: Some(old_receiver_session),
        leader_commit: Some(target_commit),
    };

    let mut promise = HashMap::new();
    let (raft_tx, raft_rx) = tokio::sync::oneshot::channel();
    let raft_message = Builder::new_rpc(RaftCode::Raft)
        .proto_header(request)
        .build();

    // Trigger automatic quarantine
    node.handle_available(Envelope::new(raft_message, raft_tx), &mut promise)
        .unwrap();
    let response = rt.block_on(raft_rx).unwrap().unwrap();
    assert_eq!(response.response_status(), ResponseStatus::Success);
    assert!(node.recovery.active);

    // Persist abstention self-vote to disk via Ready
    assert!(node.raw.has_ready());
    rt.block_on(node.on_ready(&mut promise)).unwrap();

    let marker = conf.recovery_marker();
    assert!(marker.exists());

    // Fenced leader heartbeat with current session to satisfy fencing
    let mut fenced_message = raft::eraftpb::Message {
        from: leader_id,
        to: node.id(),
        term: leader_term,
        commit: target_commit,
        ..Default::default()
    };
    fenced_message.set_msg_type(MessageType::MsgHeartbeat);
    let fenced_request = RaftRequest {
        message: fenced_message,
        sender_session: Some(201),
        receiver_session: Some(node.session),
        leader_commit: Some(target_commit),
    };
    let (fenced_tx, fenced_rx) = tokio::sync::oneshot::channel();
    let fenced_rpc = Builder::new_rpc(RaftCode::Raft)
        .proto_header(fenced_request)
        .build();
    node.handle_available(Envelope::new(fenced_rpc, fenced_tx), &mut promise)
        .unwrap();
    let _ = rt.block_on(fenced_rx).unwrap().unwrap();

    // Replicate log entries up to target_commit
    let entries: Vec<Entry> = (1..=target_commit)
        .map(|idx| Entry {
            index: idx,
            term: leader_term,
            ..Default::default()
        })
        .collect();
    node.storage.log_store.append(&entries).unwrap();
    node.raw.raft.raft_log.committed = target_commit;
    node.raw.raft.term = leader_term;
    node.raw.raft.leader_id = leader_id;
    node.raw.raft.state = raft::StateRole::Follower;
    node.storage
        .log_store
        .set_hard_state_commit(target_commit)
        .unwrap();
    rt.block_on(node.storage.app_store.apply(
        false,
        ApplyMsg::Entry(Entry {
            term: leader_term,
            index: target_commit,
            ..Default::default()
        }),
    ))
    .unwrap();

    // Now delete the marker on disk before finish check
    std::fs::remove_file(&marker).unwrap();
    assert!(!marker.exists());

    // Start the RaftNode event loop via start()!
    let sender = node.sender.clone();
    let storage = node.storage.clone();
    let session = node.session;
    let mut listener = node.start();

    // Policy A runtime behavior: run0() tick loop executes finish(), catches the missing-marker error,
    // logs a warning, keeps the node alive, and maintains active recovery quarantine.
    // The role monitor does NOT advance to Follower (times out).
    let timeout_res = rt.block_on(async {
        tokio::time::timeout(Duration::from_millis(150), listener.wait_follower()).await
    });
    assert!(
        timeout_res.is_err(),
        "role monitor must not advance while recovery marker is missing"
    );

    // Node remains alive and responsive in quarantine (ping succeeds)
    let (ping_tx, ping_rx) = tokio::sync::oneshot::channel();
    let ping = Builder::new_rpc(RaftCode::Ping).build();
    rt.block_on(sender.send(Envelope::new(ping, ping_tx)))
        .unwrap();
    let ping_response = rt.block_on(ping_rx).unwrap().unwrap();
    assert_eq!(ping_response.response_status(), ResponseStatus::Success);

    // Voting remains disabled during quarantine
    let (vote_tx, vote_rx) = tokio::sync::oneshot::channel();
    let mut vote_message = raft::eraftpb::Message {
        from: leader_id,
        to: follower_id,
        term: leader_term + 1,
        ..Default::default()
    };
    vote_message.set_msg_type(MessageType::MsgRequestVote);
    let vote_request = RaftRequest {
        message: vote_message,
        sender_session: Some(201),
        receiver_session: Some(session),
        leader_commit: Some(0),
    };
    let vote_rpc = Builder::new_rpc(RaftCode::Raft)
        .proto_header(vote_request)
        .build();
    rt.block_on(sender.send(Envelope::new(vote_rpc, vote_tx)))
        .unwrap();
    let vote_response = rt.block_on(vote_rx).unwrap().unwrap();
    assert_eq!(vote_response.response_status(), ResponseStatus::Error);

    // Operator restores the valid marker file
    std::fs::write(&marker, format!("{}\n", follower_id)).unwrap();
    assert!(marker.exists());

    // Subsequent tick in run0() now finishes recovery, unlinks marker, clears RocksDB indicator,
    // and advances role monitor to Follower!
    rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(3), listener.wait_follower()).await
    })
    .expect("node should complete recovery and advance to follower once marker is restored")
    .unwrap();

    assert!(!marker.exists(), "marker must be cleaned up on completion");

    // Verify storage recovery state is cleared in RocksDB
    assert_eq!(storage.recovery_state().unwrap(), None);

    let _ = std::fs::remove_dir_all(&conf.journal_dir);
}

#[test]
fn unfinished_recovery_with_deleted_marker_fails_closed_on_restart() {
    let rt = JournalConf::with_test().create_runtime();
    let follower_id = 3;
    let leader_id = 2;
    let (mut node, mut conf) = test_rocks_node(rt.clone(), follower_id);
    let leader_term = 7;
    let high_commit = 50000;
    let old_receiver_session = 99;

    let mut message = raft::eraftpb::Message {
        from: leader_id,
        to: node.id(),
        term: leader_term,
        commit: high_commit,
        ..Default::default()
    };
    message.set_msg_type(MessageType::MsgHeartbeat);
    let request = RaftRequest {
        message,
        sender_session: Some(201),
        receiver_session: Some(old_receiver_session),
        leader_commit: Some(high_commit),
    };

    let mut promise = HashMap::new();
    let (raft_tx, raft_rx) = tokio::sync::oneshot::channel();
    let raft_message = Builder::new_rpc(RaftCode::Raft)
        .proto_header(request)
        .build();

    // Trigger automatic quarantine
    node.handle_available(Envelope::new(raft_message, raft_tx), &mut promise)
        .unwrap();
    let response = rt.block_on(raft_rx).unwrap().unwrap();
    assert_eq!(response.response_status(), ResponseStatus::Success);
    assert!(node.recovery.active);

    // Advance Ready to persist HardState with abstention self-vote to disk
    rt.block_on(node.on_ready(&mut promise)).unwrap();

    let marker = conf.recovery_marker();
    assert!(marker.exists());

    let journal_dir = conf.journal_dir.clone();
    drop(node);

    // Delete the recovery marker prematurely while recovery was unfinished
    std::fs::remove_file(&marker).unwrap();
    assert!(!marker.exists());

    // Reopen log store from disk
    let reopened_log = RocksLogStorage::from_conf(&conf, false);
    let init_state = reopened_log.initial_state().unwrap();
    assert_eq!(init_state.hard_state.vote, follower_id);
    assert_eq!(init_state.hard_state.term, leader_term);
    assert_eq!(init_state.hard_state.commit, 0);

    // Without recover_from_peers, Recovery::load sees no marker
    let reloaded_recovery = Recovery::load(&conf, follower_id).unwrap();
    assert!(!reloaded_recovery.active);

    // Validating storage MUST fail closed: cannot resume as an ordinary voter
    let storage_recovery = reopened_log.recovery_state().unwrap();
    assert_eq!(storage_recovery, Some(follower_id));
    let err = reloaded_recovery
        .validate_storage(follower_id, storage_recovery)
        .expect_err(
            "startup validation must fail closed when unfinished recovery marker is missing",
        );
    assert!(err
        .to_string()
        .contains("unfinished member recovery detected in storage"));
    assert!(err.to_string().contains("recovery marker"));

    // Safe operator recovery procedure 1: explicitly configure recover_from_peers
    conf.recover_from_peers = Some(follower_id);
    let explicit_recovery = Recovery::load(&conf, follower_id).unwrap();
    assert!(
        explicit_recovery.active,
        "quarantine is restored by explicit configuration"
    );
    assert!(explicit_recovery
        .validate_storage(follower_id, storage_recovery)
        .is_ok());

    // Safe operator recovery procedure 2: restore the valid marker file
    // First verify that attempting to resume with an incorrect node ID or empty marker fails closed:
    conf.recover_from_peers = None;
    std::fs::write(&marker, "999\n").unwrap();
    let err_mismatch = Recovery::load(&conf, follower_id)
        .expect_err("marker with mismatched raft_id must be rejected on startup");
    assert!(err_mismatch.to_string().contains("bound to raft_id 999"));

    std::fs::write(&marker, "").unwrap();
    let err_empty = Recovery::load(&conf, follower_id)
        .expect_err("empty marker file must be rejected on startup");
    assert!(err_empty.to_string().contains("marker file is empty"));

    // Now write the correct marker file with the local follower_id:
    std::fs::write(&marker, format!("{}\n", follower_id)).unwrap();
    let restored_recovery = Recovery::load(&conf, follower_id).unwrap();
    assert!(restored_recovery.active, "quarantine is restored by marker");
    assert!(restored_recovery
        .validate_storage(follower_id, storage_recovery)
        .is_ok());

    drop(reopened_log);
    let _ = std::fs::remove_dir_all(&journal_dir);
}

#[test]
fn legitimate_candidate_self_vote_with_zero_commit_restarts_normally_without_recovery_marker() {
    let rt = Arc::new(Runtime::new("test-legit-candidate", 2, 2));
    let candidate_id = 1;
    let (node, conf) = test_rocks_node(rt.clone(), candidate_id);
    let journal_dir = conf.journal_dir.clone();

    // Candidate campaigns in term 1, votes for itself. Cluster is fresh, commit is 0.
    let mut hs = node.storage.log_store.initial_state().unwrap().hard_state;
    hs.term = 1;
    hs.vote = candidate_id;
    hs.commit = 0;
    node.storage.set_hard_state(&hs).unwrap();

    // Verify no recovery indicator was ever set
    assert_eq!(node.storage.recovery_state().unwrap(), None);
    assert!(!node.recovery.active);
    assert!(!conf.recovery_marker().exists());

    drop(node);

    // Reopen storage as if node crashed or restarted during election
    let reopened_log = RocksLogStorage::from_conf(&conf, false);
    let init_state = reopened_log.initial_state().unwrap();
    assert_eq!(init_state.hard_state.term, 1);
    assert_eq!(init_state.hard_state.vote, candidate_id);
    assert_eq!(init_state.hard_state.commit, 0);

    // Reopened storage must show no recovery indicator
    assert_eq!(reopened_log.recovery_state().unwrap(), None);

    // Recovery load sees no marker and no recover_from_peers
    let recovery = Recovery::load(&conf, candidate_id).unwrap();
    assert!(!recovery.active);

    // Legitimate candidate startup validation MUST SUCCEED (not falsely rejected by heuristic!)
    assert!(
        recovery
            .validate_storage(candidate_id, reopened_log.recovery_state().unwrap())
            .is_ok(),
        "legitimate candidate restart with zero commit must be accepted"
    );

    drop(reopened_log);
    let _ = std::fs::remove_dir_all(&journal_dir);
}

#[test]
fn crash_after_marker_unlink_before_storage_clear_fails_closed_on_restart() {
    let rt = Arc::new(Runtime::new("test-crash-boundary", 2, 2));
    let follower_id = 3;
    let (mut node, conf) = test_rocks_node(rt.clone(), follower_id);
    let journal_dir = conf.journal_dir.clone();
    let marker = conf.recovery_marker();

    // Activate recovery and ensure storage indicator is durable
    node.recovery.activate(follower_id).unwrap();
    node.storage.set_recovery_state(Some(follower_id)).unwrap();
    assert!(node.recovery.active);
    assert!(marker.exists());
    assert_eq!(node.storage.recovery_state().unwrap(), Some(follower_id));

    // Simulate crash after marker file unlinking (e.g. remove_file succeeds or parent dir sync happens,
    // but process crashes before clearing RocksDB storage indicator)
    std::fs::remove_file(&marker).unwrap();
    assert!(!marker.exists());

    drop(node);

    // Reopened process without recover_from_peers sees no marker file
    let reopened_log = RocksLogStorage::from_conf(&conf, false);
    assert_eq!(reopened_log.recovery_state().unwrap(), Some(follower_id));

    let reloaded_recovery = Recovery::load(&conf, follower_id).unwrap();
    assert!(!reloaded_recovery.active);

    // Must fail closed because storage indicator is still set while marker is missing
    let err = reloaded_recovery
        .validate_storage(follower_id, reopened_log.recovery_state().unwrap())
        .expect_err("crash boundary before storage clear must fail closed on restart");
    assert!(err
        .to_string()
        .contains("unfinished member recovery detected in storage"));

    drop(reopened_log);
    let _ = std::fs::remove_dir_all(&journal_dir);
}

#[test]
fn recovery_marker_present_without_storage_indicator_restores_indicator_and_quarantines() {
    let rt = Arc::new(Runtime::new("test-marker-no-indicator", 2, 2));
    let id = 1;
    let (node, conf) = test_rocks_node(rt.clone(), id);
    let journal_dir = conf.journal_dir.clone();
    let marker = conf.recovery_marker();

    // Ensure storage initially has no recovery state
    assert_eq!(node.storage.recovery_state().unwrap(), None);
    assert!(!node.recovery.active);
    assert!(!marker.exists());

    drop(node);

    // Operator places marker file on disk while RocksDB indicator is absent
    std::fs::write(&marker, format!("{}\n", id)).unwrap();
    assert!(marker.exists());

    // Reopen RocksDB log store
    let reopened_log = RocksLogStorage::from_conf(&conf, false);
    assert_eq!(reopened_log.recovery_state().unwrap(), None);

    // Recovery load reads marker and activates quarantine
    let recovery = Recovery::load(&conf, id).unwrap();
    assert!(recovery.active, "marker must activate recovery quarantine");

    // Startup validation succeeds because marker is present
    let storage_recovery = reopened_log.recovery_state().unwrap();
    assert!(recovery.validate_storage(id, storage_recovery).is_ok());

    // Startup path detects active recovery with missing storage indicator and syncs it
    if recovery.active && storage_recovery != Some(id) {
        reopened_log.set_recovery_state(Some(id)).unwrap();
    }
    assert_eq!(reopened_log.recovery_state().unwrap(), Some(id));

    drop(reopened_log);

    // Verify indicator is durably flushed to RocksDB across another restart
    let reopened_log2 = RocksLogStorage::from_conf(&conf, false);
    assert_eq!(reopened_log2.recovery_state().unwrap(), Some(id));

    // Recovery load remains active
    let recovery2 = Recovery::load(&conf, id).unwrap();
    assert!(recovery2.active);
    assert!(recovery2
        .validate_storage(id, reopened_log2.recovery_state().unwrap())
        .is_ok());

    drop(reopened_log2);
    let _ = std::fs::remove_dir_all(&journal_dir);
}

#[test]
fn ordinary_empty_node_starts_normally_without_recovery() {
    let rt = Arc::new(Runtime::new("test-empty-node-start", 2, 2));
    let id = 1;
    let (node, conf) = test_rocks_node(rt.clone(), id);
    let journal_dir = conf.journal_dir.clone();

    // Verify completely clean state
    assert_eq!(node.storage.recovery_state().unwrap(), None);
    assert!(!node.recovery.active);
    assert!(!conf.recovery_marker().exists());

    let hs = node.storage.initial_state().unwrap().hard_state;
    assert_eq!(hs.term, 0);
    assert_eq!(hs.vote, 0);
    assert_eq!(hs.commit, 0);

    // Validation must succeed without quarantine
    assert!(node.validate_recovery_storage().is_ok());

    // Storage recovery state remains None
    assert_eq!(node.storage.recovery_state().unwrap(), None);
    assert!(!node.recovery.active);

    drop(node);
    let _ = std::fs::remove_dir_all(&journal_dir);
}

#[test]
fn injected_storage_clear_failure_keeps_node_in_quarantine_and_retries_safely() {
    let rt = Arc::new(Runtime::new("test-storage-clear-failure", 2, 2));
    let follower_id = 3;
    let leader_id = 2;
    let (mut node, conf) = test_rocks_node(rt.clone(), follower_id);
    let journal_dir = conf.journal_dir.clone();
    let marker = conf.recovery_marker();

    node.recovery.activate(follower_id).unwrap();
    node.storage.set_recovery_state(Some(follower_id)).unwrap();
    assert!(node.recovery.active);
    assert!(marker.exists());
    assert_eq!(node.storage.recovery_state().unwrap(), Some(follower_id));

    // Simulate leader catching up follower to target_commit
    let target_commit = 5;
    let leader_term = 2;
    let mut request = RaftRequest {
        message: raft::eraftpb::Message {
            from: leader_id,
            to: follower_id,
            term: leader_term,
            commit: target_commit,
            ..Default::default()
        },
        sender_session: Some(201),
        receiver_session: Some(node.session),
        leader_commit: Some(target_commit),
    };
    request.message.set_msg_type(MessageType::MsgHeartbeat);
    node.recovery
        .step(&mut node.raw, request, node.session)
        .unwrap();

    let entries: Vec<Entry> = (1..=target_commit)
        .map(|idx| Entry {
            index: idx,
            term: leader_term,
            ..Default::default()
        })
        .collect();
    node.storage.log_store.append(&entries).unwrap();
    node.raw.raft.raft_log.committed = target_commit;
    node.raw.raft.term = leader_term;
    node.raw.raft.leader_id = leader_id;
    node.raw.raft.state = raft::StateRole::Follower;
    node.storage
        .log_store
        .set_hard_state_commit(target_commit)
        .unwrap();
    rt.block_on(node.storage.app_store.apply(
        false,
        ApplyMsg::Entry(Entry {
            term: leader_term,
            index: target_commit,
            ..Default::default()
        }),
    ))
    .unwrap();

    // Finish executes Phase 1: marker unlinked and parent dir synced
    let durable_commit = node
        .storage
        .log_store
        .initial_state()
        .unwrap()
        .hard_state
        .commit;
    let applied = node.storage.get_fsm_state().applied.index;
    let finish_res = node.recovery.finish(&node.raw, durable_commit, applied);
    assert!(finish_res.unwrap(), "finish must succeed Phase 1");
    assert!(!marker.exists(), "marker must be unlinked");
    assert!(node.recovery.marker_cleaned_up);

    // Simulate Phase 2 storage failure: storage.set_recovery_state(None) fails.
    // In run0(), on error, node.recovery.active is kept true:
    let simulated_storage_error = true;
    if simulated_storage_error {
        node.recovery.active = true;
    }
    assert!(
        node.recovery.active,
        "quarantine remains active on storage clear failure"
    );
    assert_eq!(node.storage.recovery_state().unwrap(), Some(follower_id));

    // Next iteration retry: finish() is called again.
    // Because marker_cleaned_up is true, it does not fail on missing marker!
    let finish_retry = node.recovery.finish(&node.raw, durable_commit, applied);
    assert!(finish_retry.unwrap(), "retry must succeed idempotently");
    assert!(!node.recovery.active);

    // Phase 2 retry succeeds
    node.storage.set_recovery_state(None).unwrap();
    node.recovery.marker_cleaned_up = false;
    assert_eq!(node.storage.recovery_state().unwrap(), None);

    drop(node);
    let _ = std::fs::remove_dir_all(&journal_dir);
}

// Send one message through the driver channel and let the real loop run one
// round. The ping makes the `select!` return at once instead of waiting for
// the ticker, and the timeout bounds a loop that never returns on its own.
fn drive_one_driver_round<A, B>(
    rt: &Runtime,
    node: &mut RaftNode<A, B>,
    promise: &mut PendingPromises,
) where
    A: crate::raft::storage::LogStorage,
    B: crate::raft::storage::AppStorage,
{
    let (tx, _rx) = tokio::sync::oneshot::channel();
    let ping = Builder::new_rpc(RaftCode::Ping).req_id(987_654).build();

    rt.block_on(async {
        node.sender.send(Envelope::new(ping, tx)).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), node.run0(promise)).await;
    });
}

// A step-down used to drop the callbacks of proposals still waiting for their
// entry. Dropping them is not a no-op: the server side is parked on
// `rx.await`, so the client saw neither a response nor an error until the
// connection broke. Every pending callback must be answered instead.
#[test]
fn pending_propose_callbacks_are_answered_when_the_leader_steps_down() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt.clone(), 1);
    let mut promise: PendingPromises = HashMap::new();

    // Lead term 1 so the proposal is accepted and parked by the driver.
    node.raw.raft.become_candidate();
    node.raw.raft.become_leader();
    let leader_term = node.raw.raft.term;
    node.role_monitor.advance_role(&SoftState {
        leader_id: node.id(),
        raft_state: raft::StateRole::Leader,
    });
    assert!(node.is_leader());

    let req_id = 4242;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let msg = Builder::new_rpc(RaftCode::Propose)
        .req_id(req_id)
        .proto_header(ProposeRequest {
            data: b"payload".to_vec(),
        })
        .build();
    node.handle_propose(Envelope::new(msg, tx), &mut promise)
        .unwrap();
    assert!(
        promise.contains_key(&req_id),
        "the proposal must be parked until its entry applies"
    );

    // A higher term demotes this node before the entry can be applied.
    node.raw.raft.become_follower(leader_term + 1, 2);
    assert!(!node.is_leader());

    drive_one_driver_round(&rt, &mut node, &mut promise);

    assert!(
        promise.is_empty(),
        "a demoted node must answer its callbacks, not keep them"
    );
    let response = rt
        .block_on(rx)
        .expect("the callback must be answered")
        .expect("the answer must be a response message");
    assert_eq!(
        response.response_status(),
        ResponseStatus::Error,
        "a caller whose entry will never apply must be told, not left waiting"
    );
    let error = response
        .check_error_ext::<RaftError>()
        .expect_err("the answer must carry the retryable reason");
    assert!(
        matches!(error, RaftError::LeaderNotReady(_)),
        "expected LeaderNotReady, got {:?}",
        error
    );
}

// The step-down drain only fires on a role change, and an idle follower
// produces no Ready at all. A callback left over from an older term must still
// be answered by the loop-level drain.
#[test]
fn idle_follower_answers_a_callback_left_over_from_an_older_term() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt.clone(), 1);
    let mut promise: PendingPromises = HashMap::new();

    // This node never led, so no Ready will ever carry a role change.
    assert!(!node.is_leader());
    let (tx, rx) = tokio::sync::oneshot::channel();
    promise.insert(777, tx);

    drive_one_driver_round(&rt, &mut node, &mut promise);

    assert!(
        promise.is_empty(),
        "the loop-level drain must answer callbacks no Ready will reach"
    );
    let response = rt
        .block_on(rx)
        .expect("the callback must be answered")
        .expect("the answer must be a response message");
    assert_eq!(response.response_status(), ResponseStatus::Error);
    let error = response
        .check_error_ext::<RaftError>()
        .expect_err("a non-leader must report a retryable error");
    assert!(
        matches!(error, RaftError::LeaderNotReady(_)),
        "expected LeaderNotReady, got {:?}",
        error
    );
}
