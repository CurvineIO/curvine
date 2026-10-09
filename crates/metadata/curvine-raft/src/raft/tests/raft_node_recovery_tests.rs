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
use crate::raft::storage::{HashAppStorage, MemLogStorage};
use curvine_config::RaftPeer;
use curvine_net::net::NetUtils;
use raft::eraftpb::{ConfChangeSingle, ConfChangeTransition, ConfChangeV2, HardState};
use std::time::Instant;

fn test_node(rt: Arc<Runtime>) -> RaftNode<MemLogStorage, HashAppStorage<String, String>> {
    let mut conf = JournalConf::with_test();
    let ports = [
        conf.rpc_port,
        NetUtils::get_available_port(),
        NetUtils::get_available_port(),
    ];
    conf.journal_addrs = ports
        .iter()
        .enumerate()
        .map(|(index, port)| RaftPeer::new((index + 1) as u64, &conf.hostname, *port))
        .collect();
    let group = RaftGroup::from_conf(&conf);
    let id = group.get_node_id(&conf.local_addr()).unwrap();
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
        log_store,
        HashAppStorage::new(),
        client.clone(),
        &conf,
    );
    let raw = RawNode::new(
        &conf.new_raft_conf(id, 0),
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
        session: 101,
        peer_sessions: PeerSessions::default(),
        heartbeat_sequence: 0,
        session_sender,
        session_receiver,
        recovery: Recovery::new(false),
        pending_ready: None,
        snapshot_done: None,
        storage,
        receiver,
        sender,
        group,
        role_monitor: RoleMonitor::new(),
        tick_interval: Duration::from_millis(conf.raft_tick_interval_ms),
        max_batch_size: conf.raft_batch_size.max(1),
        snapshot_interval_ms: 0,
        snapshot_min_interval_ms: 0,
        snapshot_entries: conf.snapshot_entries,
        last_snapshot_ms: 0,
        last_snapshot_op_id: 0,
    }
}

fn become_leader_and_fence(node: &mut RaftNode<MemLogStorage, HashAppStorage<String, String>>) {
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
    let mut node = test_node(rt.clone());
    become_leader_and_fence(&mut node);

    node.apply_config_change(&remove_entry(3)).unwrap();

    assert_eq!(node.peer_sessions.get(3), None);
    assert!(node.raw.raft.prs().get(3).is_none());
}

#[test]
fn rejected_remove_node_keeps_the_existing_session_fence() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt.clone());
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

fn hold_current_ready(node: &mut RaftNode<MemLogStorage, HashAppStorage<String, String>>) {
    if !node.raw.has_ready() {
        node.raw.raft.become_candidate();
    }
    assert!(node.raw.has_ready());
    let ready = node.raw.ready();
    node.pending_ready = Some(PendingReady {
        ready,
        soft_state: None,
    });
}

#[test]
fn pending_ready_keeps_ping_available_and_rejects_raft_mutation() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt.clone());
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
    let mut node = test_node(rt.clone());
    hold_current_ready(&mut node);
    let mut promise = HashMap::new();

    rt.block_on(node.complete_pending_snapshot(Ok(()), &mut promise))
        .unwrap();
    assert!(node.pending_ready.is_none());
    let error = rt
        .block_on(node.complete_pending_snapshot(Ok(()), &mut promise))
        .expect_err("one Ready must not be completed twice");
    assert!(error.to_string().contains("without a pending Ready"));
}

#[test]
fn snapshot_failure_keeps_the_ready_unadvanced_for_node_shutdown() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt.clone());
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

#[test]
fn follower_snapshot_is_throttled_by_min_interval() {
    let rt = JournalConf::with_test().create_runtime();
    let mut node = test_node(rt.clone());

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
    node.last_snapshot_ms = 0;
    node.last_snapshot_op_id = 0;

    // First check: the interval is open, so a create job is generated and
    // both markers advance.
    node.apply_create_snapshot().unwrap();
    let first_ms = node.last_snapshot_ms;
    let first_op_id = node.last_snapshot_op_id;
    assert!(first_ms > 0, "the first check must generate a snapshot job");
    assert_eq!(first_op_id, 2);
    wait_for_snapshot_job(&node);

    // Second check inside the interval: it is skipped, so no new create job
    // is generated and the markers stay unchanged.
    node.apply_create_snapshot().unwrap();
    assert_eq!(
        node.last_snapshot_ms, first_ms,
        "a second snapshot inside the interval must be skipped"
    );
    assert_eq!(node.last_snapshot_op_id, first_op_id);

    // Once the interval expires, the next check may create a new snapshot.
    let expired_anchor = LocalTime::mills().saturating_sub(node.snapshot_min_interval_ms + 1);
    node.last_snapshot_ms = expired_anchor;
    node.last_snapshot_op_id = 0;
    node.apply_create_snapshot().unwrap();
    assert!(
        node.last_snapshot_ms > expired_anchor,
        "the interval must allow a new snapshot afterwards"
    );
    assert_eq!(node.last_snapshot_op_id, 2);
    wait_for_snapshot_job(&node);
}
