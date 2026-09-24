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

//! Lost-state recovery is an adapter protocol, not a relaxation of raft-rs's
//! committed-log invariants. A process incarnation is learned only from a
//! correlated heartbeat RPC, never from an unsolicited Raft response.
use crate::conf::JournalConf;
use crate::proto::raft::{RaftRequest, RaftResponse};
use crate::raft::{RaftError, RaftResult};
use raft::eraftpb::MessageType;
use raft::{RawNode, StateRole, Storage};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Default)]
struct PeerSession {
    in_flight: Option<(u64, u64)>, // term, heartbeat sequence
    session: Option<u64>,
}

#[derive(Default)]
pub(super) struct PeerSessions {
    peers: HashMap<u64, PeerSession>,
}

pub(super) struct SessionEvent {
    pub peer: u64,
    pub term: u64,
    pub heartbeat: u64,
    pub response: Option<RaftResponse>,
}

impl PeerSessions {
    pub fn get(&self, peer: u64) -> Option<u64> {
        self.peers.get(&peer).and_then(|p| p.session)
    }

    pub fn accepts(&self, peer: u64, session: Option<u64>) -> bool {
        self.get(peer) == session
    }

    pub fn remove(&mut self, peer: u64) {
        self.peers.remove(&peer);
    }

    /// Only one heartbeat per peer is used for session discovery at a time.
    /// Other heartbeats still flow normally. Sender sequence alone is unsafe:
    /// requests can reach the old/new processes in the opposite order.
    pub fn begin(&mut self, peer: u64, term: u64, heartbeat: u64) -> bool {
        let state = self.peers.entry(peer).or_default();
        if state
            .in_flight
            .is_some_and(|(pending_term, _)| pending_term == term)
        {
            return false;
        }
        state.in_flight = Some((term, heartbeat));
        true
    }

    pub fn observe<T: Storage>(&mut self, raw: &mut RawNode<T>, event: SessionEvent) {
        let Some(peer) = self.peers.get_mut(&event.peer) else {
            return;
        };
        if peer.in_flight != Some((event.term, event.heartbeat)) {
            return;
        }
        peer.in_flight = None;
        if raw.raft.state != StateRole::Leader || event.term != raw.raft.term {
            return;
        }
        let Some(response) = event.response else {
            return;
        };
        if response.term != Some(event.term) || raw.raft.prs().get(event.peer).is_none() {
            return;
        }
        let Some(session) = response.session else {
            return;
        };
        if peer.session == Some(session) {
            return;
        }
        peer.session = Some(session);
        let next = raw.raft.raft_log.last_index() + 1;
        let pr = raw.raft.mut_prs().get_mut(event.peer).unwrap();
        pr.matched = 0;
        pr.committed_index = 0;
        pr.pending_request_snapshot = 0;
        pr.pending_snapshot = 0;
        pr.become_probe();
        pr.next_idx = next;
        // The transport reply establishes an incarnation, not durable Raft
        // participation. Only its fenced Raft responses may renew quorum activity.
        pr.recent_active = false;
        // Do not retag messages queued for the dead process with its successor's
        // session. Already-dispatched RPCs carry the old receiver session.
        raw.raft.msgs.retain(|message| message.to != event.peer);
        raw.raft.send_append(event.peer);
    }
}

pub(super) struct Recovery {
    pub active: bool,
    target: Option<(u64, u64, u64)>, // leader, term, committed index
    fenced: bool,
    marker: Option<PathBuf>,
}

impl Recovery {
    pub fn new(active: bool) -> Self {
        Self {
            active,
            target: None,
            fenced: false,
            marker: None,
        }
    }

    /// A restart must not silently re-enable elections midway through recovery,
    /// even when the operator has already removed the configuration flag.
    pub fn load(conf: &JournalConf) -> RaftResult<Self> {
        let marker = conf.recovery_marker();
        let pending = marker.try_exists()?;
        if !conf.recover_from_peers && !pending {
            return Ok(Self::new(false));
        }
        if conf.journal_addrs.len() < 3 {
            return Err(RaftError::other("member recovery requires a healthy majority of an existing cluster with at least three voters".into()));
        }
        std::fs::create_dir_all(&conf.journal_dir)?;
        if !pending {
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&marker)?
                .sync_all()?;
            std::fs::File::open(&conf.journal_dir)?.sync_all()?;
        }
        let mut recovery = Self::new(true);
        recovery.marker = Some(marker);
        Ok(recovery)
    }

    pub fn step<T: Storage>(
        &mut self,
        raw: &mut RawNode<T>,
        mut request: RaftRequest,
        session: u64,
    ) -> RaftResult<()> {
        let kind = request.message.get_msg_type();
        let from = request.message.from;
        let term = request.message.term;
        let is_leader_message = matches!(
            kind,
            MessageType::MsgHeartbeat | MessageType::MsgAppend | MessageType::MsgSnapshot
        );
        // Heartbeats must be accepted across restarts so that the sender can
        // discover the new session. Log/snapshot payloads must not cross it.
        if matches!(
            kind,
            MessageType::MsgAppend | MessageType::MsgSnapshot | MessageType::MsgTimeoutNow
        ) && request
            .receiver_session
            .is_some_and(|expected| expected != session)
        {
            return Err(RaftError::other(
                "Raft receiver session changed; retry after heartbeat handshake".into(),
            ));
        }
        if self.active {
            if !is_leader_message {
                return Err(RaftError::other(
                    "member recovery in progress; voting and campaigning are disabled".into(),
                ));
            }
            if request.sender_session.is_none() || request.leader_commit.is_none() {
                return Err(RaftError::other(
                    "member recovery requires a leader supporting the recovery handshake".into(),
                ));
            }
            if kind != MessageType::MsgHeartbeat && request.receiver_session != Some(session) {
                return Err(RaftError::other(
                    "member recovery is waiting for a fenced leader probe".into(),
                ));
            }
        }
        // Do not reinterpret a stale heartbeat, nor globally weaken commit_to.
        if kind == MessageType::MsgHeartbeat && term >= raw.raft.term {
            let past_tail = request.message.commit > raw.raft.raft_log.last_index();
            if past_tail && !self.active {
                return Err(RaftError::other("leader heartbeat exceeds local log tail; possible lost persistent state; use journal.recover_from_peers for an isolated member rebuild".into()));
            }
            // Before the leader has learned this incarnation, even an in-range
            // commit can refer to its obsolete matched index. A partially
            // recovered suffix must be verified by a fenced probe first.
            if self.active && (past_tail || request.receiver_session != Some(session)) {
                request.message.commit = request.message.commit.min(raw.raft.raft_log.committed);
            }
        }
        raw.step(request.message)?;
        if self.active
            && raw.raft.state == StateRole::Follower
            && raw.raft.leader_id == from
            && raw.raft.term == term
        {
            // A lost journal also lost voted_for. Persist an abstention in this
            // term via Ready before sending Raft replication responses (the
            // transport handshake reply itself carries no replication ACK).
            // A self vote cannot grant another candidate a second vote,
            // including after restarting without recovery mode.
            // No vote response is emitted, and existing votes are never changed.
            if raw.raft.vote == 0 {
                raw.raft.vote = raw.raft.id;
            }
            let commit = request.leader_commit.unwrap();
            if self
                .target
                .is_none_or(|(leader, previous_term, _)| leader != from || previous_term != term)
            {
                self.target = Some((from, term, commit));
                self.fenced = false;
            }
            // A newly elected leader may initially advertise commit=0.
            if self.target.is_some_and(|(_, _, target)| target == 0) && commit > 0 {
                self.target = Some((from, term, commit));
            }
            self.fenced |= request.receiver_session == Some(session);
        }
        Ok(())
    }

    /// Must be called after Ready is persisted and asynchronous snapshot/apply
    /// work has completed. An in-memory restored index alone is insufficient.
    pub fn finish<T: Storage>(
        &mut self,
        raw: &RawNode<T>,
        durable_commit: u64,
        applied: u64,
    ) -> RaftResult<bool> {
        let Some((leader, term, target)) = self.target else {
            return Ok(false);
        };
        if !self.active
            || !self.fenced
            || target == 0
            || raw.raft.leader_id != leader
            || raw.raft.term != term
            || raw.raft.state != StateRole::Follower
            || raw.raft.raft_log.committed < target
            || raw.raft.raft_log.last_index() < target
            || durable_commit < target
            || applied < raw.raft.raft_log.committed
        {
            return Ok(false);
        }
        if let Some(marker) = &self.marker {
            std::fs::remove_file(marker)?;
            std::fs::File::open(marker.parent().unwrap())?.sync_all()?;
        }
        self.active = false;
        Ok(true)
    }
}

#[cfg(test)]
mod tests;
