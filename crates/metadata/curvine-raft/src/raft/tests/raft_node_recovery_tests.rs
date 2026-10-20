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
use crate::raft::storage::{HashAppStorage, MemLogStorage, RocksLogStorage};
use curvine_config::RaftPeer;
use curvine_net::net::NetUtils;
use raft::eraftpb::{ConfChangeSingle, ConfChangeTransition, ConfChangeV2, HardState};
use raft::Storage;
use std::time::Instant;

fn test_node(rt: Arc<Runtime>, id: u64) -> RaftNode<MemLogStorage, HashAppStorage<String, String>> {
    let mut conf = JournalConf::with_test();
    conf.rpc_retry_max_duration_ms = 0;
    conf.conn_retry_max_duration_ms = 0;
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
        HashAppStorage::new(),
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

fn test_rocks_node(
    rt: Arc<Runtime>,
    id: u64,
) -> (
    RaftNode<RocksLogStorage, HashAppStorage<String, String>>,
    JournalConf,
) {
    let mut conf = JournalConf::with_test();
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
    let group = RaftGroup::from_conf(&conf);
    let node_id = group.get_node_id(&conf.local_addr()).unwrap();
    assert_eq!(node_id, id);
    let client = RaftClient::new(rt.clone(), &group, conf.new_client_conf());
    let log_store = RocksLogStorage::from_conf(&conf, true);
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
        HashAppStorage::new(),
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

    let node = RaftNode {
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
        role_monitor: RoleMonitor::new(),
        tick_interval: Duration::from_millis(conf.raft_tick_interval_ms),
        max_batch_size: conf.raft_batch_size.max(1),
        snapshot_interval_ms: 0,
        snapshot_min_interval_ms: 0,
        snapshot_entries: conf.snapshot_entries,
        last_snapshot_ms: 0,
        last_snapshot_op_id: 0,
    };
    (node, conf)
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
    // Match the production constructors, which anchor `last_snapshot_ms` to
    // process start; the throttle must not treat that anchor as a snapshot.
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
    let reloaded_recovery = Recovery::load(&conf, follower_id).unwrap();
    assert!(
        reloaded_recovery.active,
        "recovery marker on disk must restore active recovery quarantine across crash"
    );

    drop(reopened_log);
    let _ = std::fs::remove_dir_all(&journal_dir);
}
