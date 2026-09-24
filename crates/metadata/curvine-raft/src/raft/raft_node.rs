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

#![allow(clippy::too_many_arguments)]

use crate::conf::{JournalConf, JournalConfExt};
use crate::proto::raft::*;
use crate::raft::raft_error::RaftError;
use crate::raft::recovery::{PeerSessions, Recovery, SessionEvent};
use crate::raft::storage::{AppStorage, ApplyMsg, LogStorage, PeerStorage};
use crate::raft::*;
use crate::utils::SerdeUtils;
use curvine_core_error::ErrorExt;
use curvine_io::DataSlice;
use curvine_net::net::InetAddr;
use curvine_rpc::client::dispatch::{Callback, Envelope};
use curvine_rpc::message::{Builder, RefMessage, ResponseStatus};
use curvine_runtime::common::{DurationUnit, LocalTime, TimeSpent, Utils};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_runtime::sync::channel::{CallChannel, CallReceiver};
use log::{debug, error, info, warn};
use prost::Message as PMessage;
use raft::eraftpb::{ConfChange, Entry, EntryType, MessageType, Snapshot};
use raft::prelude::ConfChangeType;
use raft::{RawNode, SoftState};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, MissedTickBehavior};

struct ProposeApply {
    response: ProposeResponse,
    apply_done: Option<CallReceiver<RaftResult<()>>>,
}

pub struct RaftNode<A, B>
where
    A: LogStorage,
    B: AppStorage,
{
    rt: Arc<Runtime>,

    // raft-rs node
    raw: RawNode<PeerStorage<A, B>>,

    client: RaftClient,

    session: u64,
    peer_sessions: PeerSessions,
    heartbeat_sequence: u64,
    session_sender: mpsc::UnboundedSender<SessionEvent>,
    session_receiver: mpsc::UnboundedReceiver<SessionEvent>,
    recovery: Recovery,

    storage: PeerStorage<A, B>,

    receiver: mpsc::Receiver<Envelope>,

    #[allow(unused)]
    sender: mpsc::Sender<Envelope>,

    // raft node group
    group: RaftGroup,

    role_monitor: RoleMonitor,

    tick_interval: Duration,

    max_batch_size: usize,

    snapshot_interval_ms: u64,

    snapshot_entries: u64,

    last_snapshot_ms: u64,

    last_snapshot_op_id: u64,
}

impl<A, B> RaftNode<A, B>
where
    A: LogStorage,
    B: AppStorage,
{
    // Create a leader node.
    pub async fn new_candidate(
        rt: Arc<Runtime>,
        conf: &JournalConf,
        log_store: A,
        app_store: B,
        role_monitor: RoleMonitor,
        receiver: mpsc::Receiver<Envelope>,
        sender: mpsc::Sender<Envelope>,
        logger: &slog::Logger,
    ) -> RaftResult<Self> {
        let group = RaftGroup::from_conf(conf);
        let recovery = Recovery::load(conf)?;
        let id = group.get_node_id(&conf.local_addr())?;

        let client = RaftClient::new(rt.clone(), &group, conf.new_client_conf());
        let snapshot_interval_ms = DurationUnit::from_str(&conf.snapshot_interval)
            .unwrap()
            .as_millis();
        let snapshot_entries = conf.snapshot_entries;
        let tick_interval = Duration::from_millis(conf.raft_tick_interval_ms);
        let max_batch_size = conf.raft_batch_size.max(1);

        let last_applied = Self::install_snapshot(&log_store, &app_store, group.voters()).await?;
        let config = conf.new_raft_conf(id, last_applied);
        config.validate()?;

        let storage = PeerStorage::new(rt.clone(), log_store, app_store, client.clone(), conf);
        let raw = RawNode::new(&config, storage.clone(), logger)?;
        let (session_sender, session_receiver) = mpsc::unbounded_channel();
        // raw.raft.become_candidate();

        let node = Self {
            rt,
            raw,
            client,
            session: Utils::rand_id(),
            peer_sessions: PeerSessions::default(),
            heartbeat_sequence: 0,
            session_sender,
            session_receiver,
            recovery,
            storage,
            receiver,
            sender,
            group,
            role_monitor,
            tick_interval,
            max_batch_size,
            snapshot_interval_ms,
            snapshot_entries,
            last_snapshot_ms: LocalTime::mills(),
            last_snapshot_op_id: 0,
        };

        Ok(node)
    }

    // Create a follower node.
    pub async fn new_follower(
        rt: Arc<Runtime>,
        conf: &JournalConf,
        log_store: A,
        app_store: B,
        role_monitor: RoleMonitor,
        receiver: mpsc::Receiver<Envelope>,
        sender: mpsc::Sender<Envelope>,
        logger: &slog::Logger,
    ) -> RaftResult<Self> {
        let group = RaftGroup::from_conf(conf);
        let recovery = Recovery::load(conf)?;
        let id = group.get_node_id(&conf.local_addr())?;
        let client = RaftClient::new(rt.clone(), &group, conf.new_client_conf());
        let snapshot_interval_ms = DurationUnit::from_str(&conf.snapshot_interval)
            .unwrap()
            .as_millis();
        let snapshot_entries = conf.snapshot_entries;
        let tick_interval = Duration::from_millis(conf.raft_tick_interval_ms);
        let max_batch_size = conf.raft_batch_size.max(1);

        // raft basic configuration.
        let config = conf.new_raft_conf(id, 0);
        config.validate()?;

        client.join_cluster(id, &conf.local_addr()).await?;
        let storage = PeerStorage::new(rt.clone(), log_store, app_store, client.clone(), conf);
        let raw = RawNode::new(&config, storage.clone(), logger)?;
        let (session_sender, session_receiver) = mpsc::unbounded_channel();
        let node = Self {
            rt,
            raw,
            client,
            session: Utils::rand_id(),
            peer_sessions: PeerSessions::default(),
            heartbeat_sequence: 0,
            session_sender,
            session_receiver,
            recovery,
            storage,
            receiver,
            sender,
            group,
            role_monitor,
            tick_interval,
            max_batch_size,
            snapshot_interval_ms,
            snapshot_entries,
            last_snapshot_ms: LocalTime::mills(),
            last_snapshot_op_id: 0,
        };

        Ok(node)
    }

    // Check whether recovery from snapshot is required.
    pub async fn install_snapshot(
        log_store: &A,
        app_store: &B,
        voters: Vec<u64>,
    ) -> RaftResult<u64> {
        info!("init raft state: {:?}", log_store.initial_state()?);

        let spend = TimeSpent::new();

        match log_store.latest_snapshot()? {
            None => {
                let fsm_state = app_store.get_fsm_state();
                let mut snapshot = Snapshot::default();
                snapshot.mut_metadata().mut_conf_state().voters = voters;

                log_store.apply_snapshot(snapshot.clone())?;
                // A leader that steps down before the first raft snapshot is
                // persisted already has the correct local metadata. Installing
                // an empty application snapshot would wipe populated state and
                // is refused by journal_loader (#1207, #1268).
                if fsm_state.applied.index == 0 {
                    app_store.apply_snapshot(SnapshotData::default()).await?;
                } else {
                    info!(
                        "skip empty snapshot install; preserving local state at applied index {}",
                        fsm_state.applied.index
                    );
                }
            }

            Some(mut snapshot) => {
                snapshot.mut_metadata().mut_conf_state().voters = voters;
                // log store application snapshot.
                log_store.apply_snapshot(snapshot.clone())?;
                // app store app snapshot.
                let snapshot_data: SnapshotData = SnapshotData::decode(snapshot.get_data())?;

                info!(
                    "install snapshot start, dir: {}, fsm_state {:?}",
                    snapshot_data.data_dir(),
                    snapshot_data.fsm_state
                );

                app_store.apply_snapshot(snapshot_data).await?;

                info!("install snapshot end, cost {} ms", spend.used_ms());
            }
        };

        let fsm_state = app_store.get_fsm_state();
        app_store
            .apply(true, ApplyMsg::new_scan(fsm_state.applied))
            .await?;
        Ok(app_store.get_fsm_state().applied.index)
    }

    pub fn is_leader(&self) -> bool {
        let leader_id = self.raw.raft.leader_id;
        leader_id == self.raw.raft.id && leader_id != DEFAULT_LEADER_ID
    }

    pub fn id(&self) -> NodeId {
        self.raw.raft.id
    }

    pub fn leader(&self) -> NodeId {
        self.raw.raft.leader_id
    }

    pub fn start(self) -> RoleStateListener {
        let mut node = self;
        let rt = node.rt.clone();

        let listener = node.role_monitor.new_listener();
        rt.spawn(async move {
            node.run().await;
        });

        listener
    }

    pub async fn run(&mut self) {
        if let Err(e) = self.run0().await {
            error!("raft node stop: {}", e);
        }
        self.role_monitor.advance_exit();
    }

    async fn run0(&mut self) -> RaftResult<()> {
        let mut promise = HashMap::new();
        let mut ticker = interval(self.tick_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        while self.role_monitor.is_running() {
            tokio::select! {
                biased;

                _ = ticker.tick() => {
                    if !self.recovery.active {
                        self.raw.tick();
                    }
                }

                Some(event) = self.session_receiver.recv() => {
                    self.peer_sessions.observe(&mut self.raw, event);
                }

                result = self.receiver.recv() => {
                    let Some(env) = result else { break };
                    self.handle(env, &mut promise)?;

                    for _ in 1..self.max_batch_size {
                        let Ok(env) = self.receiver.try_recv() else { break };
                        self.handle(env, &mut promise)?;
                    }
                }
            }

            // The raft state processing failed and the node directly reported an error.
            self.on_ready(&mut promise).await?;
            if self.recovery.active
                && !self.storage.is_snapshot_applying()
                && self.recovery.finish(
                    &self.raw,
                    self.storage.log_store.initial_state()?.hard_state.commit,
                    self.storage.get_fsm_state().applied.index,
                )?
            {
                info!(
                    "member recovery completed, raft_id {}, term {}, applied {}",
                    self.id(),
                    self.raw.raft.term,
                    self.storage.get_fsm_state().applied.index
                );
                self.role_monitor.advance_role(&SoftState {
                    leader_id: self.leader(),
                    raft_state: self.raw.raft.state,
                });
            }
        }

        Ok(())
    }

    fn send_not_leader(leader_id: u64, env: Envelope, group: &RaftGroup) -> RaftResult<()> {
        let error = if leader_id == DEFAULT_LEADER_ID {
            RaftError::leader_not_ready()
        } else {
            RaftError::not_leader(leader_id, group)
        };
        let msg = Ok(env.msg.error_ext(&error));
        env.send_with_log(msg);
        Ok(())
    }

    fn send_request_error(env: Envelope, error: RaftError) -> RaftResult<()> {
        warn!(
            "raft request {:?} failed: {}",
            RaftCode::from(env.msg.code()),
            error
        );
        let msg = Ok(env.msg.error_ext(&error));
        env.send_with_log(msg);
        Ok(())
    }

    fn send_malformed_request_error(env: Envelope, error: RaftError) -> RaftResult<()> {
        debug!(
            "malformed raft request {:?}: {}",
            RaftCode::from(env.msg.code()),
            error
        );
        let msg = Ok(env.msg.error_ext(&error));
        env.send_with_log(msg);
        Ok(())
    }

    fn handle_conf_change(
        &mut self,
        env: Envelope,
        promise: &mut HashMap<i64, Callback>,
    ) -> RaftResult<()> {
        let header: ConfChangeRequest = match env.msg.parse_header() {
            Ok(header) => header,
            Err(e) => return Self::send_malformed_request_error(env, e.into()),
        };
        let mut change: ConfChange = header.change;

        if !self.is_leader() {
            Self::send_not_leader(self.leader(), env, &self.group)?;
        } else {
            if change.get_node_id() == 0 {
                change.set_node_id(self.id())
            }

            let context = SerdeUtils::serialize(&env.msg.req_id())?;
            if let Err(e) = self.raw.propose_conf_change(context, change) {
                return Self::send_request_error(env, e.into());
            }
            promise.insert(env.msg.req_id(), env.cb);
        }

        Ok(())
    }

    // Handle raft internal messages, such as elections, heartbeats, voting, etc.
    fn handle_raft(&mut self, env: Envelope) -> RaftResult<()> {
        let raft: RaftRequest = match env.msg.parse_header() {
            Ok(raft) => raft,
            Err(e) => return Self::send_malformed_request_error(env, e.into()),
        };

        let message = &raft.message;
        if message.to != self.id() || message.from == self.id() || message.from == DEFAULT_LEADER_ID
        {
            return Self::send_request_error(
                env,
                RaftError::other("invalid Raft peer identity".into()),
            );
        }
        if self.is_leader()
            && matches!(
                message.get_msg_type(),
                MessageType::MsgAppendResponse | MessageType::MsgHeartbeatResponse
            )
            && !self
                .peer_sessions
                .accepts(message.from, raft.sender_session)
        {
            return Self::send_request_error(
                env,
                RaftError::other(
                    "unverified Raft sender session; heartbeat handshake required".into(),
                ),
            );
        }
        if let Err(e) = self.recovery.step(&mut self.raw, raft, self.session) {
            return Self::send_request_error(env, e);
        }
        let rep_msg = Builder::success(&env.msg)
            .proto_header(RaftResponse {
                session: Some(self.session),
                term: Some(self.raw.raft.term),
            })
            .build();
        env.send_with_log(Ok(rep_msg));
        Ok(())
    }

    fn handle_propose(
        &mut self,
        env: Envelope,
        promise: &mut HashMap<i64, Callback>,
    ) -> RaftResult<()> {
        if !self.is_leader() {
            return Self::send_not_leader(self.leader(), env, &self.group);
        }

        let before_index = self.raw.raft.raft_log.last_index() + 1;
        let header: ProposeRequest = match env.msg.parse_header() {
            Ok(header) => header,
            Err(e) => return Self::send_malformed_request_error(env, e.into()),
        };
        let context = SerdeUtils::serialize(&env.msg.req_id())?;
        if let Err(e) = self.raw.propose(context, header.data) {
            return Self::send_request_error(env, e.into());
        }

        let after_index = self.raw.raft.raft_log.last_index() + 1;
        if before_index == after_index {
            let error = RaftError::other("propose execute fail".into());
            let rep_msg = env.msg.error_ext(&error);

            // Propose execution failed and notify the client service.
            env.send_with_log(Ok(rep_msg));
        } else {
            // Keep callbacks from the client service side.
            promise.insert(env.msg.req_id(), env.cb);
        }

        Ok(())
    }

    fn handle_ping(&self, env: Envelope) -> RaftResult<()> {
        let header = PingResponse {
            leader_id: self.leader(),
            group: self.group.to_proto(),
        };
        let msg = Builder::success(&env.msg).proto_header(header).build();

        env.send_with_log(Ok(msg));
        Ok(())
    }

    fn handle(&mut self, env: Envelope, promise: &mut HashMap<i64, Callback>) -> RaftResult<()> {
        let code = RaftCode::from(env.msg.code());
        //info!("receive: {:?} {:?}", code, env.msg);
        match code {
            RaftCode::Raft => self.handle_raft(env),

            RaftCode::ConfChange => self.handle_conf_change(env, promise),

            RaftCode::Propose => self.handle_propose(env, promise),

            RaftCode::Ping => self.handle_ping(env),

            _ => {
                let ext = env
                    .msg
                    .error_ext(&RaftError::other("Unsupported request type".into()));
                env.send_with_log(Ok(ext));
                Ok(())
            }
        }
    }

    async fn on_ready(&mut self, promise: &mut HashMap<i64, Callback>) -> RaftResult<()> {
        // Snapshot is being applied and messages are not processed.
        if self.storage.is_snapshot_applying() {
            return Ok(());
        }

        // Determine whether the raft module has completed processing of the message.
        if !self.raw.has_ready() {
            return Ok(());
        }

        // Get the ready structure.
        let mut ready = self.raw.ready();

        let soft_state = ready.ss().map(|ss| SoftState {
            leader_id: ss.leader_id,
            raft_state: ss.raft_state,
        });

        // Process snapshots.
        if *ready.snapshot() != Snapshot::default() {
            self.storage
                .gen_apply_snapshot_job(ready.snapshot().clone())?;
            // Snapshot, HardState and the application must be durable before
            // advancing Ready or acknowledging snapshot replication.
            self.storage.wait_snapshot_applied().await?;
        }

        // Persist entries before the HardState that may commit them. A crash may
        // leave extra uncommitted entries, but must never leave commit past tail.
        if !ready.entries().is_empty() {
            self.storage.append(&ready.entries()[..])?;
        }

        if let Some(hs) = ready.hs() {
            let store = self.raw.mut_store();
            store.set_hard_state(hs)?;
        }

        // Raft messages may be sent only after their Ready state is durable.
        if !ready.messages().is_empty() {
            self.send_messages(ready.take_messages()).await?;
        }

        // Get the message that the leader has fallen into the disk and send these messages to other nodes.
        if !ready.persisted_messages().is_empty() {
            // Send out the persisted messages come from the node.
            self.send_messages(ready.take_persisted_messages()).await?;
        }

        // Get the committed log entries, that is, the messages confirmed by most nodes.
        // Only the leader will run.
        self.apply_committed_entries(ready.take_committed_entries(), promise)
            .await?;

        // Execute advance to update the raft module status.
        let mut light_rd = self.raw.advance(ready);
        // advance returns a new commit index, which needs to be persisted.
        if let Some(commit) = light_rd.commit_index() {
            self.storage.set_hard_state_commit(commit)?;
        }

        // The advance interface will return a new raft msg.
        self.send_messages(light_rd.take_messages()).await?;

        // The advance interface will return a new committed entries.
        self.apply_committed_entries(light_rd.take_committed_entries(), promise)
            .await?;

        self.raw.advance_apply();

        if let Some(ss) = soft_state {
            info!(
                "raft state change, current leader address {}, node {}",
                self.group.get_addr_only_string(self.leader()),
                self.group.get_addr_string(self.id()),
            );

            self.storage.role_change(ss.raft_state).await?;
            let to_follower = self.role_monitor.is_leader() && !self.is_leader();
            if to_follower {
                Self::install_snapshot(
                    &self.storage.log_store,
                    &self.storage.app_store,
                    self.group.voters(),
                )
                .await?;
            }

            if !self.recovery.active {
                self.role_monitor.advance_role(&ss);
            }
        } else {
            // Determine whether a snapshot is needed.
            self.apply_create_snapshot()?;
        }
        Ok(())
    }

    async fn send_messages(&mut self, msgs: Vec<LibRaftMessage>) -> RaftResult<()> {
        for message in msgs {
            let to = message.get_to();
            let msg_type = message.get_msg_type();
            let index = message.index;
            let send_msg = Builder::new_rpc(RaftCode::Raft)
                .proto_header(RaftRequest {
                    message: message.clone(),
                    sender_session: Some(self.session),
                    receiver_session: self.peer_sessions.get(to),
                    leader_commit: self.is_leader().then_some(self.raw.raft.raft_log.committed),
                })
                .build();

            let client = self.client.clone();
            let session_sender = self.session_sender.clone();
            let term = message.term;
            self.heartbeat_sequence += 1;
            let heartbeat = self.heartbeat_sequence;
            let discover = msg_type == MessageType::MsgHeartbeat
                && self.is_leader()
                && self.peer_sessions.begin(to, term, heartbeat);
            self.rt.spawn(async move {
                // Heartbeat and voting messages do not need to be retryed.
                let res: RaftResult<RaftResponse> = if msg_type == MessageType::MsgHeartbeat
                    || msg_type == MessageType::MsgRequestPreVote
                    || msg_type == MessageType::MsgRequestVote
                {
                    client.timeout_rpc(to, send_msg).await.map_err(|x| x.1)
                } else {
                    client.retry_rpc(to, send_msg).await
                };
                if let Err(e) = &res {
                    warn!(
                        "send message error, to {}, index {}, msg_type {:?}: {}",
                        to, index, msg_type, e
                    );
                }
                if discover {
                    let _ = session_sender.send(SessionEvent {
                        peer: to,
                        term,
                        heartbeat,
                        response: res.ok(),
                    });
                }
            });
        }

        Ok(())
    }

    pub fn apply_config_change(&mut self, entry: &Entry) -> RaftResult<ConfChangeResponse> {
        let change: ConfChange = PMessage::decode(entry.get_data())?;
        let id = change.get_node_id();
        let add_addr = match change.get_change_type() {
            ConfChangeType::AddNode => {
                Some(SerdeUtils::deserialize::<InetAddr>(change.get_context())?)
            }
            ConfChangeType::RemoveNode => None,
            _ => unimplemented!(),
        };

        // Apply and persist Raft membership first. Transport and session state
        // must not claim a change succeeded when raft-rs rejected it.
        let cs = self.raw.apply_conf_change(&change)?;
        self.raw.mut_store().set_conf_state(&cs)?;

        match change.get_change_type() {
            ConfChangeType::AddNode => {
                let addr = add_addr.unwrap();
                info!(
                    "Raft adding node: {}({}), current leader: {}({:?})",
                    id,
                    addr,
                    self.leader(),
                    self.group.get_addr(&self.leader())
                );
                self.client.add_node(id, &addr)?;
                self.group.insert(id, &addr);
            }

            ConfChangeType::RemoveNode => {
                // Membership removal ends the incarnation-fencing lifetime for
                // this raft ID. A later AddNode with the same ID must negotiate
                // a fresh session instead of inheriting stale peer state.
                self.peer_sessions.remove(id);
                if id == self.id() {
                    self.role_monitor.advance_exit();
                } else {
                    self.group.remove(&id);
                }
            }

            _ => unreachable!(),
        }

        Ok(ConfChangeResponse::default())
    }

    async fn apply_propose(
        &mut self,
        entry: Entry,
        wait_for_response: bool,
    ) -> RaftResult<ProposeApply> {
        let applied_index = entry.index;
        let apply_done = if wait_for_response {
            let (tx, rx) = CallChannel::channel();
            self.storage
                .apply_propose(false, ApplyMsg::new_entry_with_ack(entry, tx))
                .await?;
            Some(rx)
        } else {
            self.storage
                .apply_propose(false, ApplyMsg::new_entry(entry))
                .await?;
            None
        };

        Ok(ProposeApply {
            response: ProposeResponse {
                applied_index: Some(applied_index),
            },
            apply_done,
        })
    }

    fn send_propose_response_after_apply(
        rt: &Arc<Runtime>,
        req_id: i64,
        sender: Callback,
        response: ProposeResponse,
        apply_done: CallReceiver<RaftResult<()>>,
    ) {
        let response_msg = Builder::new_rpc(RaftCode::Propose)
            .response(ResponseStatus::Success)
            .proto_header(response)
            .req_id(req_id)
            .build();

        rt.spawn(async move {
            let result = match apply_done.receive().await {
                Ok(Ok(())) => Ok(response_msg),
                Ok(Err(error)) => Ok(Builder::new_rpc(RaftCode::Propose)
                    .response(ResponseStatus::Error)
                    .req_id(req_id)
                    .data(DataSlice::Buffer(error.encode()))
                    .build()),
                Err(error) => {
                    let error = RaftError::from(error);
                    Ok(Builder::new_rpc(RaftCode::Propose)
                        .response(ResponseStatus::Error)
                        .req_id(req_id)
                        .data(DataSlice::Buffer(error.encode()))
                        .build())
                }
            };
            if sender.send(result).is_err() {
                warn!("The client connection has been closed, req {}", req_id)
            }
        });
    }

    // Whether you need to create a new snapshot.
    fn apply_create_snapshot(&mut self) -> RaftResult<()> {
        if self.is_leader() || !self.storage.can_generate_snapshot() {
            return Ok(());
        }

        let last_op_id = self.storage.get_fsm_state().op_id();
        let diff = last_op_id.saturating_sub(self.last_snapshot_op_id);
        if (LocalTime::mills() - self.last_snapshot_ms > self.snapshot_interval_ms && diff > 0)
            || diff > self.snapshot_entries
        {
            self.storage.gen_create_snapshot_job()?;

            self.last_snapshot_ms = LocalTime::mills();
            self.last_snapshot_op_id = last_op_id;
        }

        Ok(())
    }

    async fn apply_committed_entries(
        &mut self,
        entries: Vec<Entry>,
        client_send: &mut HashMap<i64, Callback>,
    ) -> RaftResult<()> {
        for entry in entries {
            let is_conf_change = matches!(entry.get_entry_type(), EntryType::EntryConfChange);
            let should_respond = self.is_leader() && !entry.get_data().is_empty();
            let (entry_index, entry_term, entry_context) = if should_respond {
                (entry.index, entry.term, Some(entry.get_context().to_vec()))
            } else {
                (0, 0, None)
            };
            let rep_msg = if is_conf_change {
                let rep = self.apply_config_change(&entry)?;
                self.storage
                    .apply_propose(false, ApplyMsg::new_entry(entry))
                    .await?;
                Builder::new_rpc(RaftCode::ConfChange)
                    .response(ResponseStatus::Success)
                    .proto_header(rep)
            } else {
                let apply = self.apply_propose(entry, should_respond).await?;
                if should_respond {
                    let Some(entry_context) = entry_context else {
                        warn!(
                            "leader committed entry has no response context, index {}, term {}",
                            entry_index, entry_term
                        );
                        continue;
                    };
                    let Some(apply_done) = apply.apply_done else {
                        warn!(
                            "leader committed entry has no apply acknowledgement, index {}, term {}",
                            entry_index, entry_term
                        );
                        continue;
                    };
                    let req_id: i64 = match SerdeUtils::deserialize(&entry_context) {
                        Ok(req_id) => req_id,
                        Err(e) => {
                            warn!(
                                "failed to decode raft entry context for response, index {}, term {}: {}",
                                entry_index, entry_term, e
                            );
                            continue;
                        }
                    };
                    match client_send.remove(&req_id) {
                        Some(sender) => Self::send_propose_response_after_apply(
                            &self.rt,
                            req_id,
                            sender,
                            apply.response,
                            apply_done,
                        ),

                        None => {
                            warn!("Not found client for request {}", req_id)
                        }
                    };
                }
                continue;
            };

            // Followers only need to replay the message and do not need to respond to the customer service.
            if should_respond {
                let Some(entry_context) = entry_context else {
                    continue;
                };
                let req_id: i64 = match SerdeUtils::deserialize(&entry_context) {
                    Ok(req_id) => req_id,
                    Err(e) => {
                        warn!(
                            "failed to decode raft entry context for response, index {}, term {}: {}",
                            entry_index, entry_term, e
                        );
                        continue;
                    }
                };
                let rep_msg = rep_msg.req_id(req_id).build();
                match client_send.remove(&req_id) {
                    Some(sender) => {
                        if sender.send(Ok(rep_msg)).is_err() {
                            warn!("The client connection has been closed, req {}", req_id)
                        }
                    }

                    None => {
                        warn!("Not found client for request {}", req_id)
                    }
                };
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::storage::{HashAppStorage, MemLogStorage};
    use curvine_config::RaftPeer;
    use curvine_net::net::NetUtils;
    use raft::eraftpb::{ConfChangeSingle, ConfChangeTransition, ConfChangeV2};

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
            storage,
            receiver,
            sender,
            group,
            role_monitor: RoleMonitor::new(),
            tick_interval: Duration::from_millis(conf.raft_tick_interval_ms),
            max_batch_size: conf.raft_batch_size.max(1),
            snapshot_interval_ms: 0,
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
}
