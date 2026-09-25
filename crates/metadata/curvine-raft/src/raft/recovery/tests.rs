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
use raft::eraftpb::{Entry, Message, Snapshot};
use raft::storage::MemStorage;
use raft::Config;
use std::collections::VecDeque;

fn raw(id: u64, storage: MemStorage) -> RawNode<MemStorage> {
    RawNode::new(
        &Config {
            id,
            election_tick: 10,
            heartbeat_tick: 1,
            ..Default::default()
        },
        storage,
        &slog::Logger::root(slog::Discard, slog::o!()),
    )
    .unwrap()
}

struct Peer {
    raw: RawNode<MemStorage>,
    sessions: PeerSessions,
    recovery: Recovery,
    session: u64,
    sequence: u64,
    applied: u64,
}

impl Peer {
    fn new(id: u64, recovering: bool, session: u64) -> Self {
        Self {
            raw: raw(id, MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]))),
            sessions: PeerSessions::default(),
            recovery: Recovery::new(recovering),
            session,
            sequence: 0,
            applied: 0,
        }
    }

    fn ready(&mut self) -> Vec<(RaftRequest, u64)> {
        let mut messages = Vec::new();
        while self.raw.has_ready() {
            let mut ready = self.raw.ready();
            if *ready.snapshot() != Snapshot::default() {
                self.applied = ready.snapshot().get_metadata().index;
                self.raw
                    .mut_store()
                    .wl()
                    .apply_snapshot(ready.snapshot().clone())
                    .unwrap();
            }
            self.raw.mut_store().wl().append(ready.entries()).unwrap();
            if let Some(hs) = ready.hs() {
                self.raw.mut_store().wl().set_hardstate(hs.clone());
            }
            for entry in ready.take_committed_entries() {
                self.applied = entry.index;
            }
            messages.extend(ready.take_messages());
            messages.extend(ready.take_persisted_messages());
            let mut light = self.raw.advance(ready);
            if let Some(commit) = light.commit_index() {
                self.raw.mut_store().wl().mut_hard_state().commit = commit;
            }
            for entry in light.take_committed_entries() {
                self.applied = entry.index;
            }
            messages.extend(light.take_messages());
            self.raw.advance_apply();
        }
        self.recovery
            .finish(
                &self.raw,
                self.raw.store().rl().hard_state().commit,
                self.applied,
            )
            .unwrap();
        messages
            .into_iter()
            .map(|message| {
                self.sequence += 1;
                let receiver_session = self.sessions.get(message.to);
                (
                    RaftRequest {
                        message,
                        sender_session: Some(self.session),
                        receiver_session,
                        leader_commit: (self.raw.raft.state == StateRole::Leader)
                            .then_some(self.raw.raft.raft_log.committed),
                    },
                    self.sequence,
                )
            })
            .collect()
    }
}

struct Cluster {
    peers: Vec<Peer>,
    queue: VecDeque<(RaftRequest, u64)>,
    snapshots: usize,
}

impl Cluster {
    fn new() -> Self {
        let mut cluster = Self {
            peers: (1..=3).map(|id| Peer::new(id, false, id * 100)).collect(),
            queue: VecDeque::new(),
            snapshots: 0,
        };
        cluster.peers[0].raw.campaign().unwrap();
        cluster.drain();
        cluster.peers[0].raw.ping();
        cluster.drain();
        assert_eq!(cluster.peers[0].raw.raft.state, StateRole::Leader);
        cluster
    }

    fn drain(&mut self) {
        for peer in &mut self.peers {
            self.queue.extend(peer.ready());
        }
        let mut count = 0;
        while let Some((request, sequence)) = self.queue.pop_front() {
            count += 1;
            assert!(
                count < 1000,
                "replication must converge without a retry loop"
            );
            let message = &request.message;
            let from = message.from as usize - 1;
            let to = message.to as usize - 1;
            let term = message.term;
            let kind = message.get_msg_type();
            let peer = &mut self.peers[to];
            if peer.raw.raft.state == StateRole::Leader
                && matches!(
                    kind,
                    MessageType::MsgAppendResponse | MessageType::MsgHeartbeatResponse
                )
                && !peer.sessions.accepts(message.from, request.sender_session)
            {
                continue;
            }
            if kind == MessageType::MsgSnapshot {
                self.snapshots += 1;
            }
            if peer
                .recovery
                .step(&mut peer.raw, request, peer.session)
                .is_err()
            {
                continue;
            }
            let response = RaftResponse {
                session: Some(peer.session),
                term: Some(peer.raw.raft.term),
            };
            self.queue.extend(peer.ready());
            if kind == MessageType::MsgHeartbeat {
                let leader = &mut self.peers[from];
                assert!(leader.sessions.begin((to + 1) as u64, term, sequence));
                leader.sessions.observe(
                    &mut leader.raw,
                    SessionEvent {
                        peer: (to + 1) as u64,
                        term,
                        heartbeat: sequence,
                        response: Some(response),
                    },
                );
                self.queue.extend(leader.ready());
            }
        }
    }

    fn write(&mut self) {
        self.peers[0]
            .raw
            .propose(vec![], b"committed metadata".to_vec())
            .unwrap();
        self.drain();
    }
}

#[test]
fn empty_member_recovers_logs_or_snapshot_without_leader_change_or_new_write() {
    for compact in [false, true] {
        let mut cluster = Cluster::new();
        for _ in 0..5 {
            cluster.write();
        }
        let committed = cluster.peers[0].raw.raft.raft_log.committed;
        let term = cluster.peers[0].raw.raft.term;
        assert_eq!(
            cluster.peers[0].raw.raft.prs().get(3).unwrap().matched,
            committed
        );
        if compact {
            cluster.peers[0]
                .raw
                .mut_store()
                .wl()
                .compact(committed)
                .unwrap();
        }
        cluster.peers[2] = Peer::new(3, true, 301);
        cluster.peers[0].raw.ping();
        cluster.drain();
        assert_eq!(cluster.peers[0].raw.raft.term, term);
        assert_eq!(cluster.peers[0].raw.raft.state, StateRole::Leader);
        assert_eq!(cluster.peers[2].applied, committed);
        assert!(!cluster.peers[2].recovery.active);
        assert_eq!(cluster.snapshots > 0, compact);
        assert_eq!(cluster.peers[2].raw.store().rl().hard_state().vote, 3);
    }
}

#[test]
fn recovery_retries_a_temporarily_unavailable_snapshot() {
    let mut cluster = Cluster::new();
    cluster.write();
    let committed = cluster.peers[0].raw.raft.raft_log.committed;
    cluster.peers[0]
        .raw
        .mut_store()
        .wl()
        .compact(committed)
        .unwrap();
    cluster.peers[0]
        .raw
        .mut_store()
        .wl()
        .trigger_snap_unavailable();
    cluster.peers[2] = Peer::new(3, true, 301);
    cluster.peers[0].raw.ping();
    cluster.drain();
    assert!(cluster.peers[2].recovery.active);
    cluster.peers[0].raw.ping();
    cluster.drain();
    assert!(!cluster.peers[2].recovery.active);
    assert_eq!(cluster.peers[2].applied, committed);
}

#[test]
fn persisted_abstention_prevents_a_second_vote_after_normal_restart() {
    let mut cluster = Cluster::new();
    cluster.write();
    cluster.peers[2] = Peer::new(3, true, 301);
    cluster.peers[0].raw.ping();
    cluster.drain();
    let mut restarted = raw(3, cluster.peers[2].raw.store().clone());
    restarted
        .step(Message {
            msg_type: MessageType::MsgRequestVote as i32,
            from: 2,
            to: 3,
            term: restarted.raft.term,
            index: restarted.raft.raft_log.last_index(),
            log_term: restarted.raft.raft_log.last_term(),
            ..Default::default()
        })
        .unwrap();
    assert!(restarted
        .raft
        .msgs
        .iter()
        .any(|m| m.get_msg_type() == MessageType::MsgRequestVoteResponse && m.reject));
}

#[test]
fn stale_handshakes_and_dead_process_responses_cannot_restore_matched() {
    let mut cluster = Cluster::new();
    cluster.write();
    let leader = &mut cluster.peers[0];
    let term = leader.raw.raft.term;
    let event = |heartbeat, session, term| SessionEvent {
        peer: 3,
        term,
        heartbeat,
        response: Some(RaftResponse {
            session: Some(session),
            term: Some(term),
        }),
    };
    assert!(leader.sessions.begin(3, term, 100));
    assert!(!leader.sessions.begin(3, term, 101));
    leader
        .sessions
        .observe(&mut leader.raw, event(100, 301, term));
    assert_eq!(leader.raw.raft.prs().get(3).unwrap().matched, 0);
    assert!(!leader.raw.raft.prs().get(3).unwrap().recent_active);
    leader
        .sessions
        .observe(&mut leader.raw, event(99, 300, term));
    leader
        .sessions
        .observe(&mut leader.raw, event(101, 302, term + 1));
    assert_eq!(leader.sessions.get(3), Some(301));
    assert!(!leader.sessions.accepts(3, Some(300)));
    assert!(!leader.sessions.accepts(3, None));
    assert!(leader.sessions.accepts(3, Some(301)));
    leader.raw.raft.mut_prs().get_mut(3).unwrap().matched = 1;
    assert!(leader.sessions.begin(3, term, 102));
    leader
        .sessions
        .observe(&mut leader.raw, event(102, 301, term));
    assert_eq!(
        leader.raw.raft.prs().get(3).unwrap().matched,
        1,
        "duplicate session must not reset progress"
    );
}

fn heartbeat(commit: u64) -> RaftRequest {
    RaftRequest {
        message: Message {
            msg_type: MessageType::MsgHeartbeat as i32,
            from: 1,
            to: 3,
            term: 30,
            commit,
            ..Default::default()
        },
        sender_session: Some(100),
        receiver_session: None,
        leader_commit: Some(commit),
    }
}

#[test]
fn recovery_fails_closed_for_legacy_leader_and_does_not_vote_or_campaign() {
    let mut peer = Peer::new(3, true, 301);
    let mut request = heartbeat(10);
    request.leader_commit = None;
    assert!(peer.recovery.step(&mut peer.raw, request, 301).is_err());
    for kind in [
        MessageType::MsgRequestVote,
        MessageType::MsgRequestPreVote,
        MessageType::MsgTimeoutNow,
    ] {
        let mut request = heartbeat(10);
        request.message.set_msg_type(kind);
        assert!(peer.recovery.step(&mut peer.raw, request, 301).is_err());
    }
    assert!(peer.raw.raft.msgs.is_empty());
    assert_eq!(peer.raw.raft.term, 0);
}

#[test]
fn out_of_range_heartbeat_is_not_a_general_permission_to_clamp_commits() {
    let mut peer = Peer::new(3, false, 300);
    assert!(peer
        .recovery
        .step(&mut peer.raw, heartbeat(10), 300)
        .is_err());
    assert_eq!(peer.raw.raft.raft_log.committed, 0);
    peer.recovery.active = true;
    peer.recovery
        .step(&mut peer.raw, heartbeat(10), 300)
        .unwrap();
    assert_eq!(peer.raw.raft.term, 30);
    assert_eq!(peer.raw.raft.raft_log.committed, 0);
    assert!(peer.recovery.active);
    assert!(
        !peer.recovery.finish(&peer.raw, 10, 10).unwrap(),
        "unfenced/in-memory state is not recovery completion"
    );
    let mut stale = heartbeat(99);
    stale.message.term = 29;
    peer.recovery.step(&mut peer.raw, stale, 300).unwrap();
    assert_eq!(peer.raw.raft.raft_log.committed, 0);
}

#[test]
fn append_and_snapshot_for_a_dead_receiver_are_rejected() {
    let mut peer = Peer::new(3, true, 301);
    for kind in [MessageType::MsgAppend, MessageType::MsgSnapshot] {
        let mut request = heartbeat(10);
        request.message.set_msg_type(kind);
        request.receiver_session = Some(300);
        assert!(peer.recovery.step(&mut peer.raw, request, 301).is_err());
        assert_eq!(peer.raw.raft.raft_log.last_index(), 0);
    }
}

#[test]
fn recovery_marker_survives_restart_without_the_flag() {
    let root = std::env::temp_dir().join(format!(
        "curvine-recovery-marker-{}",
        curvine_runtime::common::Utils::rand_id()
    ));
    let mut conf = JournalConf {
        recover_from_peers: true,
        journal_dir: root.to_str().unwrap().into(),
        journal_addrs: (1..=3)
            .map(|id| crate::raft::RaftPeer::new(id, "localhost", 9000 + id as u16))
            .collect(),
        ..Default::default()
    };
    assert!(Recovery::load(&conf).unwrap().active);
    conf.recover_from_peers = false;
    let recovery = Recovery::load(&conf).unwrap();
    assert!(recovery.active);
    let mut cluster = Cluster::new();
    cluster.write();
    cluster.peers[2] = Peer::new(3, true, 301);
    cluster.peers[2].recovery = recovery;
    cluster.peers[0].raw.ping();
    cluster.drain();
    assert!(!cluster.peers[2].recovery.active);
    assert!(!Recovery::load(&conf).unwrap().active);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn an_unfinished_or_changed_leader_target_cannot_publish_readiness() {
    let mut peer = Peer::new(3, true, 301);
    let mut request = heartbeat(10);
    request.receiver_session = Some(301);
    peer.recovery
        .step(&mut peer.raw, request.clone(), 301)
        .unwrap();
    assert!(!peer.recovery.finish(&peer.raw, 0, 0).unwrap());
    request.message.from = 2;
    request.message.term = 31;
    request.leader_commit = Some(20);
    request.receiver_session = None;
    peer.recovery.step(&mut peer.raw, request, 301).unwrap();
    assert_eq!(peer.recovery.target, Some((2, 31, 20)));
    assert!(!peer.recovery.fenced);
    assert!(!peer.recovery.finish(&peer.raw, 20, 20).unwrap());
}

#[test]
fn fenced_peer_cannot_downgrade_to_a_legacy_session() {
    let mut cluster = Cluster::new();
    let leader = &mut cluster.peers[0];
    let term = leader.raw.raft.term;

    assert!(leader.sessions.begin(3, term, 100));
    leader.sessions.observe(
        &mut leader.raw,
        SessionEvent {
            peer: 3,
            term,
            heartbeat: 100,
            response: Some(RaftResponse {
                session: Some(301),
                term: Some(term),
            }),
        },
    );
    assert_eq!(leader.sessions.get(3), Some(301));

    assert!(leader.sessions.begin(3, term, 101));
    leader.sessions.observe(
        &mut leader.raw,
        SessionEvent {
            peer: 3,
            term,
            heartbeat: 101,
            response: Some(RaftResponse {
                session: None,
                term: Some(term),
            }),
        },
    );

    assert_eq!(leader.sessions.get(3), Some(301));
    assert!(!leader.sessions.accepts(3, None));
    assert!(leader.sessions.accepts(3, Some(301)));
}

#[test]
fn removing_a_peer_clears_its_session_and_handshake() {
    let mut sessions = PeerSessions::default();
    assert!(sessions.begin(3, 10, 100));
    sessions.peers.get_mut(&3).unwrap().session = Some(301);

    sessions.remove(3);

    assert_eq!(sessions.get(3), None);
    assert!(sessions.accepts(3, None));
    assert!(sessions.begin(3, 10, 101));
}

#[test]
fn failed_handshake_releases_slot_and_late_response_cannot_override_new_session() {
    let mut cluster = Cluster::new();
    let leader = &mut cluster.peers[0];
    let term = leader.raw.raft.term;
    assert!(leader.sessions.begin(3, term, 100));
    leader.sessions.observe(
        &mut leader.raw,
        SessionEvent {
            peer: 3,
            term,
            heartbeat: 100,
            response: None,
        },
    );
    assert!(leader.sessions.begin(3, term, 101));
    leader.sessions.observe(
        &mut leader.raw,
        SessionEvent {
            peer: 3,
            term,
            heartbeat: 101,
            response: Some(RaftResponse {
                session: Some(301),
                term: Some(term),
            }),
        },
    );
    // A response from the timed-out RPC, even when delivered much later.
    leader.sessions.observe(
        &mut leader.raw,
        SessionEvent {
            peer: 3,
            term,
            heartbeat: 100,
            response: Some(RaftResponse {
                session: Some(300),
                term: Some(term),
            }),
        },
    );
    assert_eq!(leader.sessions.get(3), Some(301));
}

#[test]
fn an_unfenced_heartbeat_cannot_commit_a_partially_recovered_suffix() {
    let mut peer = Peer::new(3, true, 301);
    peer.raw
        .mut_store()
        .wl()
        .append(&[
            Entry {
                index: 1,
                term: 29,
                ..Default::default()
            },
            Entry {
                index: 2,
                term: 29,
                ..Default::default()
            },
        ])
        .unwrap();
    assert_eq!(peer.raw.raft.raft_log.last_index(), 2);
    // This is within last_index, but the leader has not discovered session 301
    // and therefore must not use its old matched to certify this local suffix.
    peer.recovery
        .step(&mut peer.raw, heartbeat(2), 301)
        .unwrap();
    assert_eq!(peer.raw.raft.raft_log.committed, 0);
    peer.ready();
    assert_eq!(peer.applied, 0);
    assert!(!peer.recovery.fenced);
    assert!(peer.recovery.active);
}
