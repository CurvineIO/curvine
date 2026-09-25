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

//! Characterize the dependency behavior behind #1718 without RPC/runtime threads.
//! These tests intentionally exercise raft-rs directly, not Curvine's recovery adapter.
use raft::eraftpb::{Message, MessageType};
use raft::storage::MemStorage;
use raft::{Config, RawNode};

fn node(id: u64) -> RawNode<MemStorage> {
    let config = Config {
        id,
        election_tick: 10,
        heartbeat_tick: 1,
        ..Default::default()
    };
    let logger = slog::Logger::root(slog::Discard, slog::o!());
    RawNode::new(
        &config,
        MemStorage::new_with_conf_state((vec![1, 2, 3], vec![])),
        &logger,
    )
    .unwrap()
}

fn leader_with_stale_progress() -> RawNode<MemStorage> {
    let mut leader = node(1);
    leader.raft.become_candidate();
    leader.raft.become_leader();
    let last = leader.raft.raft_log.last_index();
    leader.raft.raft_log.commit_to(last);
    let peer = leader.raft.mut_prs().get_mut(3).unwrap();
    peer.matched = last;
    peer.become_replicate();
    leader.raft.msgs.clear();
    leader
}

#[test]
fn clamping_heartbeat_alone_does_not_restart_replication() {
    let mut leader = leader_with_stale_progress();
    let mut empty = node(3);
    let heartbeat = Message {
        msg_type: MessageType::MsgHeartbeat as i32,
        from: 1,
        to: 3,
        term: leader.raft.term,
        // Simulate the proposed clamp. The original commit was leader.last_index().
        commit: 0,
        ..Default::default()
    };
    empty.step(heartbeat).unwrap();
    for response in empty.raft.msgs.drain(..) {
        leader.step(response).unwrap();
    }
    assert_eq!(empty.raft.raft_log.last_index(), 0);
    assert_eq!(
        leader.raft.prs().get(3).unwrap().matched,
        leader.raft.raft_log.last_index()
    );
    assert!(
        leader.raft.msgs.is_empty(),
        "a normal heartbeat reply does not invalidate matched"
    );
}

#[test]
fn append_rejection_at_old_matched_is_ignored_in_replicate() {
    let mut leader = leader_with_stale_progress();
    let matched = leader.raft.prs().get(3).unwrap().matched;
    leader
        .step(Message {
            msg_type: MessageType::MsgAppendResponse as i32,
            from: 3,
            to: 1,
            term: leader.raft.term,
            index: matched,
            reject: true,
            reject_hint: 0,
            ..Default::default()
        })
        .unwrap();
    let peer = leader.raft.prs().get(3).unwrap();
    assert_eq!(peer.matched, matched);
    assert_eq!(peer.next_idx, matched + 1);
    assert!(leader.raft.msgs.is_empty());
}

#[test]
fn empty_follower_cannot_use_request_snapshot_after_learning_leader_term() {
    let mut empty = node(3);
    empty
        .step(Message {
            msg_type: MessageType::MsgHeartbeat as i32,
            from: 1,
            to: 3,
            term: 30,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        empty.request_snapshot(),
        Err(raft::Error::RequestSnapshotDropped)
    );
}
