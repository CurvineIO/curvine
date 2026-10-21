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

impl Drop for Peer {
    fn drop(&mut self) {
        if let Some(marker) = &self.recovery.marker {
            if let Some(parent) = marker.parent() {
                let _ = std::fs::remove_dir_all(parent);
            }
        }
    }
}

impl Peer {
    fn new(id: u64, recovering: bool, session: u64) -> Self {
        let root = std::env::temp_dir().join(format!(
            "curvine-peer-marker-{}-{}-{}",
            id,
            session,
            curvine_runtime::common::Utils::rand_id()
        ));
        let marker = root.join("member-recovery-in-progress");
        let mut recovery = Recovery::new(recovering);
        recovery.marker = Some(marker.clone());
        if recovering {
            write_marker(&marker, id).unwrap();
        }
        Self {
            raw: raw(id, MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]))),
            sessions: PeerSessions::default(),
            recovery,
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
                recovering: Some(peer.recovery.active),
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
fn wiped_member_rejoins_without_active_recovery_recovering_snapshot_or_logs() {
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
        // Wiped peer rejoins with original Raft ID 3, new session 301, but recovery is NOT active.
        cluster.peers[2] = Peer::new(3, false, 301);
        cluster.peers[0].raw.ping();
        cluster.drain();
        assert_eq!(cluster.peers[0].raw.raft.term, term);
        assert_eq!(cluster.peers[0].raw.raft.state, StateRole::Leader);
        assert_eq!(cluster.peers[2].applied, committed);
        assert_eq!(cluster.peers[2].raw.raft.raft_log.committed, committed);
        assert_eq!(cluster.peers[2].raw.raft.raft_log.last_index(), committed);
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
fn unfenced_peer_accepts_sessions_until_an_exact_fence_is_learned() {
    let mut sessions = PeerSessions::default();
    assert!(sessions.accepts(3, None));
    assert!(sessions.accepts(3, Some(300)));
    sessions.peers.entry(3).or_default().incarnation = PeerIncarnation::Session(301);
    assert!(!sessions.accepts(3, None));
    assert!(!sessions.accepts(3, Some(300)));
    assert!(sessions.accepts(3, Some(301)));
}

#[test]
fn unfenced_leader_steps_sessioned_append_response_before_discovery() {
    let mut cluster = Cluster::new();
    cluster.peers[0].sessions.remove(3);

    cluster.write();

    let leader = &cluster.peers[0];
    let committed = leader.raw.raft.raft_log.committed;
    assert_eq!(leader.sessions.get(3), None);
    assert_eq!(leader.raw.raft.prs().get(3).unwrap().matched, committed);
}

#[test]
fn first_healthy_session_observation_preserves_progress_and_quorum_activity() {
    let mut cluster = Cluster::new();
    let leader = &mut cluster.peers[0];
    let term = leader.raw.raft.term;
    leader.sessions.peers.remove(&3);
    let progress = leader.raw.raft.mut_prs().get_mut(3).unwrap();
    progress.matched = 1;
    progress.committed_index = 1;
    progress.recent_active = true;

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
                recovering: Some(false),
            }),
        },
    );

    let progress = leader.raw.raft.prs().get(3).unwrap();
    assert_eq!(leader.sessions.get(3), Some(301));
    assert_eq!(progress.matched, 1);
    assert_eq!(progress.committed_index, 1);
    assert!(progress.recent_active);
}

#[test]
fn first_recovering_session_observation_resets_stale_progress() {
    let mut cluster = Cluster::new();
    let leader = &mut cluster.peers[0];
    let term = leader.raw.raft.term;
    leader.sessions.peers.remove(&3);
    let progress = leader.raw.raft.mut_prs().get_mut(3).unwrap();
    progress.matched = 1;
    progress.committed_index = 1;
    progress.recent_active = true;

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
                recovering: Some(true),
            }),
        },
    );

    let progress = leader.raw.raft.prs().get(3).unwrap();
    assert_eq!(leader.sessions.get(3), Some(301));
    assert_eq!(progress.matched, 0);
    assert_eq!(progress.committed_index, 0);
    assert!(!progress.recent_active);
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
            recovering: Some(false),
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
        recover_from_peers: Some(1),
        journal_dir: root.to_str().unwrap().into(),
        journal_addrs: (1..=3)
            .map(|id| crate::raft::RaftPeer::new(id, "localhost", 9000 + id as u16))
            .collect(),
        ..Default::default()
    };
    assert!(Recovery::load(&conf, 1).unwrap().active);
    conf.recover_from_peers = None;
    let recovery = Recovery::load(&conf, 1).unwrap();
    assert!(recovery.active);
    let mut cluster = Cluster::new();
    cluster.write();
    cluster.peers[2] = Peer::new(3, true, 301);
    cluster.peers[2].recovery = recovery;
    cluster.peers[0].raw.ping();
    cluster.drain();
    assert!(!cluster.peers[2].recovery.active);
    assert!(!Recovery::load(&conf, 1).unwrap().active);
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
fn fenced_peer_switches_to_a_correlated_legacy_replacement() {
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
                recovering: Some(false),
            }),
        },
    );
    assert_eq!(leader.sessions.get(3), Some(301));
    {
        let progress = leader.raw.raft.mut_prs().get_mut(3).unwrap();
        progress.matched = 8;
        progress.committed_index = 8;
        progress.recent_active = true;
    }

    assert!(leader.sessions.begin(3, term, 101));
    leader.sessions.observe(
        &mut leader.raw,
        SessionEvent {
            peer: 3,
            term,
            heartbeat: 101,
            response: Some(RaftResponse {
                session: None,
                term: None,
                recovering: None,
            }),
        },
    );

    assert_eq!(leader.sessions.get(3), None);
    assert!(leader.sessions.accepts(3, None));
    assert!(!leader.sessions.accepts(3, Some(301)));
    let progress = leader.raw.raft.prs().get(3).unwrap();
    assert_eq!(progress.matched, 0);
    assert_eq!(progress.committed_index, 0);
    assert!(!progress.recent_active);
}

#[test]
fn removing_a_peer_clears_its_session_and_handshake() {
    let mut sessions = PeerSessions::default();
    assert!(sessions.begin(3, 10, 100));
    sessions.peers.get_mut(&3).unwrap().incarnation = PeerIncarnation::Session(301);

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
                recovering: Some(false),
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
                recovering: Some(false),
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

#[test]
fn shared_recovery_target_activates_only_the_selected_member() {
    let root = std::env::temp_dir().join(format!(
        "curvine-targeted-recovery-{}",
        curvine_runtime::common::Utils::rand_id()
    ));
    let shared = JournalConf {
        recover_from_peers: Some(3),
        journal_addrs: (1..=3)
            .map(|id| crate::raft::RaftPeer::new(id, "localhost", 9100 + id as u16))
            .collect(),
        ..Default::default()
    };

    for local_id in 1..=3 {
        let mut conf = shared.clone();
        conf.journal_dir = root
            .join(format!("member-{local_id}"))
            .to_str()
            .unwrap()
            .into();
        let recovery = Recovery::load(&conf, local_id).unwrap();
        assert_eq!(recovery.active, local_id == 3);
        assert_eq!(conf.recovery_marker().exists(), local_id == 3);
    }

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn local_recovery_marker_rejects_a_conflicting_target_on_restart() {
    let root = std::env::temp_dir().join(format!(
        "curvine-conflicting-recovery-target-{}",
        curvine_runtime::common::Utils::rand_id()
    ));
    let mut conf = JournalConf {
        recover_from_peers: Some(3),
        journal_dir: root.to_str().unwrap().into(),
        journal_addrs: (1..=3)
            .map(|id| crate::raft::RaftPeer::new(id, "localhost", 9150 + id as u16))
            .collect(),
        ..Default::default()
    };

    assert!(Recovery::load(&conf, 3).unwrap().active);
    assert_eq!(
        std::fs::read_to_string(conf.recovery_marker()).unwrap(),
        "3\n"
    );

    conf.recover_from_peers = Some(2);
    let error = match Recovery::load(&conf, 3) {
        Ok(_) => panic!("an active marker must reject a conflicting recovery target"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("marker for raft_id 3"));
    assert!(error.to_string().contains("targets raft_id 2"));

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn unknown_recovery_target_is_rejected_before_any_marker_is_created() {
    let root = std::env::temp_dir().join(format!(
        "curvine-unknown-recovery-target-{}",
        curvine_runtime::common::Utils::rand_id()
    ));
    let conf = JournalConf {
        recover_from_peers: Some(4),
        journal_dir: root.to_str().unwrap().into(),
        journal_addrs: (1..=3)
            .map(|id| crate::raft::RaftPeer::new(id, "localhost", 9200 + id as u16))
            .collect(),
        ..Default::default()
    };

    let error = match Recovery::load(&conf, 1) {
        Ok(_) => panic!("unknown target must fail closed"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("unknown raft_id 4"));
    assert!(!conf.recovery_marker().exists());
}

#[test]
fn wrong_sequence_and_stale_term_cannot_change_a_session_fence() {
    let mut cluster = Cluster::new();
    let leader = &mut cluster.peers[0];
    let term = leader.raw.raft.term;

    assert!(leader.sessions.begin(3, term, 200));
    leader.sessions.observe(
        &mut leader.raw,
        SessionEvent {
            peer: 3,
            term,
            heartbeat: 201,
            response: Some(RaftResponse {
                session: None,
                term: None,
                recovering: None,
            }),
        },
    );
    assert_eq!(leader.sessions.get(3), Some(300));
    assert!(!leader.sessions.accepts(3, None));

    leader.sessions.observe(
        &mut leader.raw,
        SessionEvent {
            peer: 3,
            term,
            heartbeat: 200,
            response: Some(RaftResponse {
                session: None,
                term: None,
                recovering: None,
            }),
        },
    );
    assert_eq!(leader.sessions.get(3), None);
    assert!(leader.sessions.accepts(3, None));

    assert!(leader.sessions.begin(3, term - 1, 202));
    leader.sessions.observe(
        &mut leader.raw,
        SessionEvent {
            peer: 3,
            term: term - 1,
            heartbeat: 202,
            response: Some(RaftResponse {
                session: Some(302),
                term: Some(term - 1),
                recovering: Some(false),
            }),
        },
    );
    assert_eq!(leader.sessions.get(3), None);
    assert!(leader.sessions.accepts(3, None));
    assert!(!leader.sessions.accepts(3, Some(302)));
}

#[test]
fn legacy_to_session_replacement_resets_progress_and_rejects_legacy_acks() {
    let mut cluster = Cluster::new();
    let leader = &mut cluster.peers[0];
    let term = leader.raw.raft.term;

    assert!(leader.sessions.begin(3, term, 300));
    leader.sessions.observe(
        &mut leader.raw,
        SessionEvent {
            peer: 3,
            term,
            heartbeat: 300,
            response: Some(RaftResponse {
                session: None,
                term: None,
                recovering: None,
            }),
        },
    );
    {
        let progress = leader.raw.raft.mut_prs().get_mut(3).unwrap();
        progress.matched = 9;
        progress.committed_index = 9;
        progress.recent_active = true;
    }

    assert!(leader.sessions.begin(3, term, 301));
    leader.sessions.observe(
        &mut leader.raw,
        SessionEvent {
            peer: 3,
            term,
            heartbeat: 301,
            response: Some(RaftResponse {
                session: Some(303),
                term: Some(term),
                recovering: Some(false),
            }),
        },
    );

    assert_eq!(leader.sessions.get(3), Some(303));
    assert!(!leader.sessions.accepts(3, None));
    assert!(leader.sessions.accepts(3, Some(303)));
    let progress = leader.raw.raft.prs().get(3).unwrap();
    assert_eq!(progress.matched, 0);
    assert_eq!(progress.committed_index, 0);
    assert!(!progress.recent_active);
}

#[test]
fn detected_wiped_member_quarantine_rejects_voting_and_campaigning() {
    let mut peer = Peer::new(3, false, 301);
    let mut request = heartbeat(10);
    request.receiver_session = Some(300); // obsolete receiver session from prior dead incarnation
    assert!(!peer.recovery.active);
    assert_eq!(peer.raw.raft.vote, 0);

    // Leader heartbeat triggers automatic recovery quarantine.
    peer.recovery.step(&mut peer.raw, request, 301).unwrap();
    assert!(peer.recovery.active);
    assert_eq!(peer.raw.raft.vote, 3);

    // While recovery quarantine is active, voting and campaigning messages fail closed.
    for kind in [
        MessageType::MsgRequestVote,
        MessageType::MsgRequestPreVote,
        MessageType::MsgHup,
        MessageType::MsgTimeoutNow,
    ] {
        let mut msg = heartbeat(10);
        msg.message.set_msg_type(kind);
        msg.receiver_session = Some(301);
        let err = peer
            .recovery
            .step(&mut peer.raw, msg, 301)
            .expect_err("voting and campaigning must be rejected during recovery quarantine");
        assert!(err
            .to_string()
            .contains("voting and campaigning are disabled"));
    }
    // Abstention is preserved and the peer remains in Follower role.
    assert_eq!(peer.raw.raft.vote, 3);
    assert_eq!(peer.raw.raft.state, StateRole::Follower);
}

#[test]
fn detected_wiped_member_with_zero_commit_activates_quarantine_and_disables_elections_until_finish()
{
    let mut peer = Peer::new(3, false, 301);
    let leader_id = 1;
    let leader_term = 30;
    let target_commit = 10;

    // Triggering heartbeat has commit = 0, but leader_commit > 0, and receiver session is obsolete.
    let mut request = heartbeat(0);
    request.message.from = leader_id;
    request.message.term = leader_term;
    request.message.commit = 0;
    request.receiver_session = Some(300); // obsolete receiver session from prior dead incarnation
    request.leader_commit = Some(target_commit); // leader_commit > 0

    assert_eq!(peer.raw.raft.raft_log.last_index(), 0);
    assert_eq!(peer.raw.raft.raft_log.committed, 0);
    assert!(!peer.recovery.active);
    assert_eq!(peer.raw.raft.vote, 0);
    let marker = peer.recovery.marker.as_ref().unwrap().clone();
    assert!(!marker.exists());

    // 1. Leader heartbeat with commit = 0 must trigger automatic recovery quarantine for empty log.
    peer.recovery.step(&mut peer.raw, request, 301).unwrap();
    assert!(peer.recovery.active);
    assert_eq!(peer.raw.raft.vote, 3);
    assert!(marker.exists());
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "3\n");

    // 2. Voting and campaigning remain disabled during recovery quarantine.
    for kind in [
        MessageType::MsgRequestVote,
        MessageType::MsgRequestPreVote,
        MessageType::MsgHup,
        MessageType::MsgTimeoutNow,
    ] {
        let mut msg = heartbeat(target_commit);
        msg.message.set_msg_type(kind);
        msg.receiver_session = Some(301);
        let err = peer
            .recovery
            .step(&mut peer.raw, msg, 301)
            .expect_err("voting and campaigning must be rejected during recovery quarantine");
        assert!(err
            .to_string()
            .contains("voting and campaigning are disabled"));
    }
    assert_eq!(peer.raw.raft.vote, 3);
    assert_eq!(peer.raw.raft.state, StateRole::Follower);

    // 3. Recovery cannot finish prematurely before log/commit catch up and session is fenced.
    assert!(!peer.recovery.finish(&peer.raw, 0, 0).unwrap());
    assert!(peer.recovery.active);
    assert!(marker.exists());

    // 4. Follower catches up log and commit to target_commit, receives a fenced probe.
    let entries: Vec<Entry> = (1..=target_commit)
        .map(|idx| Entry {
            index: idx,
            term: leader_term,
            ..Default::default()
        })
        .collect();
    peer.raw.mut_store().wl().append(&entries).unwrap();
    peer.raw.raft.raft_log.committed = target_commit;
    peer.raw.raft.term = leader_term;
    peer.raw.raft.leader_id = leader_id;
    peer.raw.raft.state = StateRole::Follower;

    // Fenced leader heartbeat with current session.
    let mut fenced_request = heartbeat(target_commit);
    fenced_request.message.from = leader_id;
    fenced_request.message.term = leader_term;
    fenced_request.receiver_session = Some(301);
    fenced_request.leader_commit = Some(target_commit);
    peer.recovery
        .step(&mut peer.raw, fenced_request, 301)
        .unwrap();

    // 5. Calling finish() completes recovery: marker is removed and quarantine is deactivated.
    assert!(peer
        .recovery
        .finish(&peer.raw, target_commit, target_commit)
        .unwrap());
    assert!(!peer.recovery.active);
    assert!(!marker.exists());

    // 6. After recovery completes, voting and campaigning are no longer disabled.
    let mut vote_msg = heartbeat(target_commit);
    vote_msg.message.set_msg_type(MessageType::MsgRequestVote);
    vote_msg.receiver_session = Some(301);
    vote_msg.message.term = leader_term + 1;
    assert!(peer.recovery.step(&mut peer.raw, vote_msg, 301).is_ok());
}

#[test]
fn detected_wiped_member_creates_marker_atomically_and_survives_restart() {
    let root = std::env::temp_dir().join(format!(
        "curvine-detected-recovery-marker-{}",
        curvine_runtime::common::Utils::rand_id()
    ));
    let conf = JournalConf {
        recover_from_peers: None, // NO explicit recovery flag configured
        journal_dir: root.to_str().unwrap().into(),
        journal_addrs: (1..=3)
            .map(|id| crate::raft::RaftPeer::new(id, "localhost", 9300 + id as u16))
            .collect(),
        ..Default::default()
    };

    let recovery = Recovery::load(&conf, 3).unwrap();
    assert!(!recovery.active);
    assert!(!conf.recovery_marker().exists());

    let mut peer = Peer::new(3, false, 301);
    peer.recovery = recovery;

    let mut request = heartbeat(10);
    request.receiver_session = Some(300); // obsolete receiver session
    peer.recovery.step(&mut peer.raw, request, 301).unwrap();

    assert!(peer.recovery.active);
    assert!(conf.recovery_marker().exists());
    assert_eq!(
        std::fs::read_to_string(conf.recovery_marker()).unwrap(),
        "3\n"
    );

    // Restart without the recover_from_peers flag: marker forces recovery mode to remain active.
    let restarted = Recovery::load(&conf, 3).unwrap();
    assert!(restarted.active);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn healthy_follower_obsolete_session_heartbeat_does_not_activate_recovery() {
    let root = std::env::temp_dir().join(format!(
        "curvine-healthy-follower-{}",
        curvine_runtime::common::Utils::rand_id()
    ));
    let conf = JournalConf {
        recover_from_peers: None,
        journal_dir: root.to_str().unwrap().into(),
        journal_addrs: (1..=3)
            .map(|id| crate::raft::RaftPeer::new(id, "localhost", 9350 + id as u16))
            .collect(),
        ..Default::default()
    };

    let mut peer = Peer::new(3, false, 301);
    peer.recovery = Recovery::load(&conf, 3).unwrap();

    // Populate local log up to index 10 (simulating clean restart with intact state).
    let entries: Vec<Entry> = (1..=10)
        .map(|idx| Entry {
            index: idx,
            term: 30,
            ..Default::default()
        })
        .collect();
    peer.raw.mut_store().wl().append(&entries).unwrap();
    peer.raw.raft.raft_log.committed = 10;
    assert_eq!(peer.raw.raft.raft_log.last_index(), 10);

    // Leader sends heartbeat with obsolete receiver session 300 and commit 10 (not past tail).
    let mut request = heartbeat(10);
    request.receiver_session = Some(300);
    peer.recovery.step(&mut peer.raw, request, 301).unwrap();

    // Recovery must NOT be activated for a healthy follower.
    assert!(!peer.recovery.active);
    assert!(!conf.recovery_marker().exists());
    assert_eq!(peer.raw.raft.vote, 0);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn detected_wiped_member_fails_closed_in_two_node_cluster_or_unsupported_leader() {
    // 1. Two-node cluster cannot safely auto-recover a wiped voter
    let mut peer2 = Peer::new(2, false, 201);
    peer2.raw = raw(2, MemStorage::new_with_conf_state((vec![1, 2], vec![])));
    let mut request = heartbeat(10);
    request.receiver_session = Some(200);
    let err = peer2
        .recovery
        .step(&mut peer2.raw, request, 201)
        .expect_err("two-node cluster must fail closed on auto-recovery");
    assert!(err.to_string().contains("at least three voters"));
    assert!(!peer2.recovery.active);

    // 2. Missing recovery handshake fields from leader fails closed
    let mut peer3 = Peer::new(3, false, 301);
    let mut req_no_sender = heartbeat(10);
    req_no_sender.receiver_session = Some(300);
    req_no_sender.sender_session = None;
    let err = peer3
        .recovery
        .step(&mut peer3.raw, req_no_sender, 301)
        .expect_err("missing sender_session must fail closed");
    assert!(err
        .to_string()
        .contains("leader supporting the recovery handshake"));
    assert!(!peer3.recovery.active);

    let mut req_no_commit = heartbeat(10);
    req_no_commit.receiver_session = Some(300);
    req_no_commit.leader_commit = None;
    let err = peer3
        .recovery
        .step(&mut peer3.raw, req_no_commit, 301)
        .expect_err("missing leader_commit must fail closed");
    assert!(err
        .to_string()
        .contains("leader supporting the recovery handshake"));
    assert!(!peer3.recovery.active);
}

#[test]
fn recovery_activate_fails_when_no_marker_configured() {
    let mut recovery = Recovery::new(false);
    assert!(recovery.marker.is_none());
    assert!(!recovery.active);

    let err = recovery
        .activate(3)
        .expect_err("activation must fail when no durable marker is configured");
    assert!(err
        .to_string()
        .contains("no durable recovery marker configured"));
    assert!(
        !recovery.active,
        "active must not be set when activation fails"
    );
}

#[test]
fn recovery_activate_is_idempotent_when_already_active() {
    // When already active without a marker (e.g. loaded from peers or test setup),
    // calling activate() returns Ok(()) idempotently without requiring marker.
    let mut active_no_marker = Recovery::new(true);
    assert!(active_no_marker.marker.is_none());
    assert!(active_no_marker.active);
    assert!(active_no_marker.activate(3).is_ok());
    assert!(active_no_marker.active);

    // When already active with a configured marker, activate() is also idempotent.
    let root = std::env::temp_dir().join(format!(
        "curvine-recovery-idempotent-{}",
        curvine_runtime::common::Utils::rand_id()
    ));
    let marker = root.join("member-recovery-in-progress");
    std::fs::create_dir_all(&root).unwrap();
    write_marker(&marker, 3).unwrap();

    let mut active_with_marker = Recovery::new(true);
    active_with_marker.marker = Some(marker.clone());
    assert!(active_with_marker.activate(3).is_ok());
    assert!(active_with_marker.active);
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "3\n");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn recovery_activate_rejects_marker_belonging_to_different_id_without_overwriting() {
    let root = std::env::temp_dir().join(format!(
        "curvine-recovery-activate-diff-id-{}",
        curvine_runtime::common::Utils::rand_id()
    ));
    let marker = root.join("member-recovery-in-progress");
    std::fs::create_dir_all(&root).unwrap();

    // Pre-create a marker belonging to raft_id 2
    write_marker(&marker, 2).unwrap();
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "2\n");

    let mut recovery = Recovery::new(false);
    recovery.marker = Some(marker.clone());

    // Attempt to activate for raft_id 3
    let err = recovery
        .activate(3)
        .expect_err("activate must reject marker belonging to a different ID");
    assert!(err.to_string().contains("marker is bound to raft_id 2"));
    assert!(err.to_string().contains("local raft_id is 3"));
    assert!(!recovery.active, "active must remain false on failure");

    // The existing marker file must NOT be overwritten
    assert_eq!(
        std::fs::read_to_string(&marker).unwrap(),
        "2\n",
        "marker belonging to another raft_id must not be overwritten"
    );

    // Activating for matching ID 2 succeeds and preserves the marker
    recovery.activate(2).unwrap();
    assert!(recovery.active);
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "2\n");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn recovery_marker_competing_creation_prevents_replacement_races() {
    let root = std::env::temp_dir().join(format!(
        "curvine-recovery-competing-{}",
        curvine_runtime::common::Utils::rand_id()
    ));
    let marker = root.join("member-recovery-in-progress");
    std::fs::create_dir_all(&root).unwrap();

    // Multiple concurrent threads attempt to create or activate the marker for local_id = 3
    let mut handles = Vec::new();
    for _ in 0..10 {
        let m = marker.clone();
        handles.push(std::thread::spawn(move || {
            let mut r = Recovery::new(false);
            r.marker = Some(m);
            r.activate(3)
        }));
    }

    for h in handles {
        assert!(
            h.join().unwrap().is_ok(),
            "competing creation with matching ID must succeed"
        );
    }

    // Marker must contain exactly "3\n"
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "3\n");

    // A competing creation attempting a conflicting ID 2 must fail and cannot overwrite
    let mut r_conflicting = Recovery::new(false);
    r_conflicting.marker = Some(marker.clone());
    let err = r_conflicting
        .activate(2)
        .expect_err("competing creation with conflicting ID must be rejected");
    assert!(err.to_string().contains("marker is bound to raft_id 3"));
    assert_eq!(
        std::fs::read_to_string(&marker).unwrap(),
        "3\n",
        "existing marker must not be overwritten by competing creation"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn detected_wiped_member_fails_to_quarantine_if_marker_is_unconfigured() {
    let mut peer = Peer::new(3, false, 301);
    peer.recovery.marker = None; // simulate unconfigured marker path
    let mut request = heartbeat(10);
    request.receiver_session = Some(300);

    let err = peer
        .recovery
        .step(&mut peer.raw, request, 301)
        .expect_err("step must fail closed if marker cannot be written");
    assert!(err
        .to_string()
        .contains("no durable recovery marker configured"));
    assert!(
        !peer.recovery.active,
        "peer must not enter in-memory recovery quarantine"
    );
}

#[test]
fn recovery_activate_fails_for_empty_marker_without_overwriting() {
    let root = std::env::temp_dir().join(format!(
        "curvine-recovery-empty-marker-{}",
        curvine_runtime::common::Utils::rand_id()
    ));
    let marker = root.join("member-recovery-in-progress");
    std::fs::create_dir_all(&root).unwrap();

    // 1. Create an empty marker file
    std::fs::write(&marker, "").unwrap();
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "");

    let mut recovery = Recovery::new(false);
    recovery.marker = Some(marker.clone());

    // Calling activate() must fail closed
    let err = recovery
        .activate(3)
        .expect_err("activation must fail when marker is empty");
    assert!(
        err.to_string().contains("empty"),
        "error should describe empty marker: {err}"
    );
    assert!(
        !recovery.active,
        "recovery active must remain false when activation fails"
    );

    // Existing marker must not be overwritten
    assert_eq!(
        std::fs::read_to_string(&marker).unwrap(),
        "",
        "existing empty marker must not be overwritten"
    );

    // 2. Whitespace-only marker must also fail closed without overwriting
    std::fs::write(&marker, "   \n\t").unwrap();
    let err_ws = recovery
        .activate(3)
        .expect_err("activation must fail when marker is whitespace-only");
    assert!(
        err_ws.to_string().contains("empty"),
        "error should describe empty marker: {err_ws}"
    );
    assert!(
        !recovery.active,
        "recovery active must remain false when activation fails"
    );
    assert_eq!(
        std::fs::read_to_string(&marker).unwrap(),
        "   \n\t",
        "existing whitespace marker must not be overwritten"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn recovery_finish_fails_closed_when_configured_marker_is_missing() {
    let mut peer = Peer::new(3, true, 301);
    let leader_id = 1;
    let leader_term = 10;
    let target_commit = 5;

    // Follower receives heartbeat establishing target commit and session fencing.
    let mut request = heartbeat(target_commit);
    request.message.from = leader_id;
    request.message.term = leader_term;
    request.receiver_session = Some(301);
    request.leader_commit = Some(target_commit);
    peer.recovery.step(&mut peer.raw, request, 301).unwrap();

    // Satisfy all recovery completion conditions:
    // Append log entries up to target_commit, advance committed index, and set leader/term.
    let entries: Vec<Entry> = (1..=target_commit)
        .map(|idx| Entry {
            index: idx,
            term: leader_term,
            ..Default::default()
        })
        .collect();
    peer.raw.mut_store().wl().append(&entries).unwrap();
    peer.raw.raft.raft_log.committed = target_commit;
    peer.raw.raft.term = leader_term;
    peer.raw.raft.leader_id = leader_id;
    peer.raw.raft.state = StateRole::Follower;

    assert!(peer.recovery.active);
    let marker = peer.recovery.marker.as_ref().unwrap().clone();
    assert!(marker.exists());

    // Remove the configured recovery marker prior to finish()
    std::fs::remove_file(&marker).unwrap();
    assert!(!marker.exists());

    // Calling finish() must fail closed: return descriptive error and leave active == true
    let err = peer
        .recovery
        .finish(&peer.raw, target_commit, target_commit)
        .expect_err("finish must fail when configured marker is missing");
    assert!(
        err.to_string().contains("missing"),
        "error must describe missing marker: {err}"
    );
    assert!(
        peer.recovery.active,
        "recovery active must remain true when finish fails"
    );

    // Voting and campaigning remain disabled after failed finish
    for kind in [
        MessageType::MsgRequestVote,
        MessageType::MsgRequestPreVote,
        MessageType::MsgHup,
        MessageType::MsgTimeoutNow,
    ] {
        let mut msg = heartbeat(target_commit);
        msg.message.set_msg_type(kind);
        msg.receiver_session = Some(301);
        let err = peer
            .recovery
            .step(&mut peer.raw, msg, 301)
            .expect_err("voting and campaigning must remain disabled after failed finish");
        assert!(err
            .to_string()
            .contains("voting and campaigning are disabled"));
    }

    // If the marker is restored, finish() succeeds and sets active == false
    write_marker(&marker, 3).unwrap();
    assert!(peer
        .recovery
        .finish(&peer.raw, target_commit, target_commit)
        .unwrap());
    assert!(
        !peer.recovery.active,
        "recovery active must be false after successful finish"
    );
    assert!(
        !marker.exists(),
        "recovery marker must be removed after successful finish"
    );
}

#[test]
fn recovery_finish_fails_closed_when_marker_unlink_fails() {
    let mut peer = Peer::new(3, true, 301);
    let leader_id = 1;
    let leader_term = 10;
    let target_commit = 5;

    let mut request = heartbeat(target_commit);
    request.message.from = leader_id;
    request.message.term = leader_term;
    request.receiver_session = Some(301);
    request.leader_commit = Some(target_commit);
    peer.recovery.step(&mut peer.raw, request, 301).unwrap();

    let entries: Vec<Entry> = (1..=target_commit)
        .map(|idx| Entry {
            index: idx,
            term: leader_term,
            ..Default::default()
        })
        .collect();
    peer.raw.mut_store().wl().append(&entries).unwrap();
    peer.raw.raft.raft_log.committed = target_commit;
    peer.raw.raft.term = leader_term;
    peer.raw.raft.leader_id = leader_id;
    peer.raw.raft.state = StateRole::Follower;

    let marker = peer.recovery.marker.as_ref().unwrap().clone();
    assert!(marker.exists());

    // Inject marker deletion failure
    peer.recovery.unlink_failure_injected = true;
    let err = peer
        .recovery
        .finish(&peer.raw, target_commit, target_commit)
        .expect_err("finish must fail when marker unlink fails");
    assert!(err.to_string().contains("injected marker unlink failure"));
    assert!(peer.recovery.active, "quarantine must remain active");
    assert!(marker.exists(), "marker must still exist on disk");

    // Voting and campaigning remain disabled
    let mut vote_msg = heartbeat(target_commit);
    vote_msg.message.set_msg_type(MessageType::MsgRequestVote);
    vote_msg.receiver_session = Some(301);
    assert!(peer.recovery.step(&mut peer.raw, vote_msg, 301).is_err());

    // Unset injection: finish succeeds and cleans up marker
    peer.recovery.unlink_failure_injected = false;
    assert!(peer
        .recovery
        .finish(&peer.raw, target_commit, target_commit)
        .unwrap());
    assert!(!peer.recovery.active);
    assert!(!marker.exists());
}

#[test]
fn recovery_finish_fails_closed_when_parent_directory_sync_fails_and_retries_safely() {
    let mut peer = Peer::new(3, true, 301);
    let leader_id = 1;
    let leader_term = 10;
    let target_commit = 5;

    let mut request = heartbeat(target_commit);
    request.message.from = leader_id;
    request.message.term = leader_term;
    request.receiver_session = Some(301);
    request.leader_commit = Some(target_commit);
    peer.recovery.step(&mut peer.raw, request, 301).unwrap();

    let entries: Vec<Entry> = (1..=target_commit)
        .map(|idx| Entry {
            index: idx,
            term: leader_term,
            ..Default::default()
        })
        .collect();
    peer.raw.mut_store().wl().append(&entries).unwrap();
    peer.raw.raft.raft_log.committed = target_commit;
    peer.raw.raft.term = leader_term;
    peer.raw.raft.leader_id = leader_id;
    peer.raw.raft.state = StateRole::Follower;

    let marker = peer.recovery.marker.as_ref().unwrap().clone();
    assert!(marker.exists());

    // Inject parent directory synchronization failure
    peer.recovery.sync_failure_injected = true;
    let err = peer
        .recovery
        .finish(&peer.raw, target_commit, target_commit)
        .expect_err("finish must fail when parent directory sync fails");
    assert!(err
        .to_string()
        .contains("injected parent directory sync failure"));
    assert!(peer.recovery.active, "quarantine must remain active");
    assert!(
        peer.recovery.marker_unlinked,
        "marker must be marked as unlinked"
    );
    assert!(
        !marker.exists(),
        "marker was unlinked before sync failure was encountered"
    );

    // Voting and campaigning remain disabled
    let mut camp_msg = heartbeat(target_commit);
    camp_msg.message.set_msg_type(MessageType::MsgHup);
    camp_msg.receiver_session = Some(301);
    assert!(peer.recovery.step(&mut peer.raw, camp_msg, 301).is_err());

    // Retrying with sync_failure_injected = false must succeed without failing on missing marker
    peer.recovery.sync_failure_injected = false;
    assert!(peer
        .recovery
        .finish(&peer.raw, target_commit, target_commit)
        .unwrap());
    assert!(!peer.recovery.active, "recovery completed successfully");
    assert!(!peer.recovery.marker_unlinked);
}

#[test]
fn crash_between_durable_catchup_and_marker_cleanup_preserves_quarantine_on_restart() {
    let root = std::env::temp_dir().join(format!(
        "curvine-recovery-crash-catchup-{}",
        curvine_runtime::common::Utils::rand_id()
    ));
    let marker = root.join("member-recovery-in-progress");
    std::fs::create_dir_all(&root).unwrap();
    write_marker(&marker, 3).unwrap();

    let conf = JournalConf {
        recover_from_peers: None,
        journal_dir: root.to_str().unwrap().into(),
        journal_addrs: (1..=3)
            .map(|id| crate::raft::RaftPeer::new(id, "localhost", 9000 + id as u16))
            .collect(),
        ..Default::default()
    };

    // Even if catch-up and apply were durable, if crash occurred before marker cleanup,
    // marker on disk forces the restarted process into active recovery quarantine.
    let reloaded = Recovery::load(&conf, 3).unwrap();
    assert!(reloaded.active, "restarted node must preserve quarantine");
    assert_eq!(reloaded.marker_path(), Some(marker.as_path()));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn startup_validation_rejects_unfinished_recovery_when_marker_is_missing() {
    let mut recovery = Recovery::new(false);
    recovery.marker = Some(std::path::PathBuf::from("/tmp/test-marker"));

    // 1. Normal fresh startup (no storage indicator): Ok
    assert!(recovery.validate_storage(3, None).is_ok());

    // 2. Legitimate candidate with self-vote in uncommitted cluster:
    // No recovery was ever active, so storage indicator is None -> Ok
    assert!(recovery.validate_storage(3, None).is_ok());

    // 3. Normal voter restart: storage indicator is None -> Ok
    assert!(recovery.validate_storage(3, None).is_ok());

    // 4. When recovery is already active, validate_storage always passes -> Ok
    let active_recovery = Recovery::new(true);
    assert!(active_recovery.validate_storage(3, Some(3)).is_ok());

    // 5. Unfinished recovery with missing marker (active == false, storage == Some(3)) -> Err
    let err = recovery
        .validate_storage(3, Some(3))
        .expect_err("unfinished recovery with missing marker must fail closed");
    assert!(err
        .to_string()
        .contains("unfinished member recovery detected in storage"));
    assert!(err.to_string().contains("raft_id 3"));
    assert!(err.to_string().contains("/tmp/test-marker"));

    // 6. Conflicting storage recovery ID: storage indicator targets peer 2, local node is 3 -> Err
    let err_conflict = recovery
        .validate_storage(3, Some(2))
        .expect_err("conflicting storage recovery target must fail closed");
    assert!(err_conflict
        .to_string()
        .contains("storage recovery indicator targets raft_id 2, but local node is 3"));
}
