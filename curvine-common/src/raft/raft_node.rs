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

use crate::conf::JournalConf;
use crate::proto::raft::*;
use crate::raft::raft_error::RaftError;
use crate::raft::storage::{AppStorage, LogStorage, PeerStorage};
use crate::raft::*;
use crate::utils::SerdeUtils;
use log::{error, info, warn};
use orpc::client::dispatch::{Callback, Envelope};
use orpc::common::{DurationUnit, LocalTime, TimeSpent};
use orpc::io::net::InetAddr;
use orpc::message::{Builder, RefMessage, ResponseStatus};
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::try_err;
use prost::Message as PMessage;
use raft::eraftpb::{ConfChange, Entry, EntryType, MessageType, Snapshot};
use raft::prelude::ConfChangeType;
use raft::RawNode;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::timeout;

pub struct RaftNode<A, B>
where
    A: LogStorage,
    B: AppStorage,
{
    rt: Arc<Runtime>,

    // raft-rs node
    raw: RawNode<PeerStorage<A, B>>,

    client: RaftClient,

    storage: PeerStorage<A, B>,

    receiver: mpsc::Receiver<Envelope>,

    #[allow(unused)]
    sender: mpsc::Sender<Envelope>,

    // raft node group
    group: RaftGroup,

    role_monitor: RoleMonitor,

    tick_interval: Duration,

    poll_interval: Duration,

    snapshot_interval_ms: u64,

    snapshot_entries: u64,

    last_snapshot_ms: u64,

    last_snapshot_index: u64,

    // Index of each node commit
    commit_info: HashMap<NodeId, u64>,
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
        let id = group.get_node_id(&conf.local_addr())?;
        let voters = group.peers.iter().map(|x| *x.0).collect();

        let client = RaftClient::new(rt.clone(), &group, conf.new_client_conf());
        let snapshot_interval_ms = DurationUnit::from_str(&conf.snapshot_interval)
            .unwrap()
            .as_millis();
        let snapshot_entries = conf.snapshot_entries;
        let tick_interval = Duration::from_millis(conf.raft_tick_interval_ms);
        let poll_interval = Duration::from_millis(conf.raft_poll_interval_ms);

        let last_applied = Self::install_snapshot(&log_store, &app_store, voters).await?;
        let config = conf.new_raft_conf(id, last_applied);
        config.validate()?;

        let storage = PeerStorage::new(log_store, app_store, client.clone(), conf);
        let raw = RawNode::new(&config, storage.clone(), logger)?;
        // raw.raft.become_candidate();

        let node = Self {
            rt,
            raw,
            client,
            storage,
            receiver,
            sender,
            group,
            role_monitor,
            tick_interval,
            poll_interval,
            snapshot_interval_ms,
            snapshot_entries,
            last_snapshot_ms: LocalTime::mills(),
            last_snapshot_index: 0,
            commit_info: Default::default(),
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
        let id = group.get_node_id(&conf.local_addr())?;
        let client = RaftClient::new(rt.clone(), &group, conf.new_client_conf());
        let snapshot_interval_ms = DurationUnit::from_str(&conf.snapshot_interval)
            .unwrap()
            .as_millis();
        let snapshot_entries = conf.snapshot_entries;
        let tick_interval = Duration::from_millis(conf.raft_tick_interval_ms);
        let poll_interval = Duration::from_millis(conf.raft_poll_interval_ms);

        // raft basic configuration.
        let config = conf.new_raft_conf(id, 0);
        config.validate()?;

        client.join_cluster(id, &conf.local_addr()).await?;
        let storage = PeerStorage::new(log_store, app_store, client.clone(), conf);
        let raw = RawNode::new(&config, storage.clone(), logger)?;
        let node = Self {
            rt,
            raw,
            client,
            storage,
            receiver,
            sender,
            group,
            role_monitor,
            tick_interval,
            poll_interval,
            snapshot_interval_ms,
            snapshot_entries,
            last_snapshot_ms: LocalTime::mills(),
            last_snapshot_index: 0,
            commit_info: Default::default(),
        };

        Ok(node)
    }

    // Check whether recovery from snapshot is required.
    pub async fn install_snapshot(
        log_store: &A,
        app_store: &B,
        voters: Vec<u64>,
    ) -> RaftResult<u64> {
        let spend = TimeSpent::new();

        let snapshot = match log_store.latest_snapshot()? {
            None => {
                let mut snapshot = Snapshot::default();
                snapshot.mut_metadata().mut_conf_state().voters = voters;

                log_store.apply_snapshot(snapshot.clone())?;
                snapshot
            }

            Some(mut snapshot) => {
                snapshot.mut_metadata().mut_conf_state().voters = voters;
                // log store application snapshot.
                log_store.apply_snapshot(snapshot.clone())?;
                // app store app snapshot.
                let snapshot_data = SnapshotData::decode(snapshot.get_data())?;
                info!(
                    "install snapshot start, meta: {:?}, snapshot data {:?}",
                    snapshot.get_metadata(),
                    snapshot_data
                );
                app_store.apply_snapshot(&snapshot_data)?;

                snapshot
            }
        };

        // Replay the log after the snapshot.
        let mut last_applied = snapshot.metadata.as_ref().map(|x| x.index).unwrap_or(0);
        let mut reply_state = log_store.initial_state()?.hard_state;
        loop {
            let entries = log_store.scan_entries(last_applied + 1, last_applied + 1000)?;
            if entries.is_empty() {
                break;
            }
            info!(
                "install snapshot replay log, start_index: {}, entries: {}",
                last_applied + 1,
                entries.len()
            );

            for entry in &entries {
                app_store.apply(false, &entry.data).await?;
            }

            let last_entry = entries.last().unwrap();
            last_applied = last_entry.index;

            // After log replay, set index and term.
            reply_state.commit = last_entry.index;
            reply_state.term = last_entry.term;
        }

        // Modify hard_state after log replay
        log_store.set_hard_state(&reply_state)?;

        info!("install snapshot end, cost {} ms", spend.used_ms());
        Ok(last_applied)
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
        let mut now = Instant::now();
        let mut promise = HashMap::new();

        while self.role_monitor.is_running() {
            loop {
                match timeout(self.poll_interval, self.receiver.recv()).await {
                    Ok(Some(env)) => self.handle(env, &mut promise)?,
                    Ok(None) => unreachable!(),
                    Err(_) => break,
                };
            }

            if now.elapsed() >= self.tick_interval {
                now = Instant::now();
                self.raw.tick();
            }

            // The raft state processing failed and the node directly reported an error.
            self.on_ready(&mut promise).await?;
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

    fn handle_conf_change(
        &mut self,
        env: Envelope,
        promise: &mut HashMap<i64, Callback>,
    ) -> RaftResult<()> {
        let header: ConfChangeRequest = env.msg.parse_header()?;
        let mut change: ConfChange = header.change;

        if !self.is_leader() {
            Self::send_not_leader(self.leader(), env, &self.group)?;
        } else {
            if change.get_node_id() == 0 {
                change.set_node_id(self.id())
            }

            let context = SerdeUtils::serialize(&env.msg.req_id())?;
            self.raw.propose_conf_change(context, change)?;
            promise.insert(env.msg.req_id(), env.cb);
        }

        Ok(())
    }

    // Handle raft internal messages, such as elections, heartbeats, voting, etc.
    fn handle_raft(&mut self, env: Envelope) -> RaftResult<()> {
        let raft: RaftRequest = try_err!(env.msg.parse_header());
        self.commit_info
            .insert(raft.message.from, raft.message.commit);
        self.raw.step(raft.message)?;
        let rep_msg = Builder::success(&env.msg)
            .proto_header(RaftResponse::default())
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
        let header: ProposeRequest = env.msg.parse_header()?;
        let context = SerdeUtils::serialize(&env.msg.req_id())?;
        self.raw.propose(context, header.data)?;

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

        // Get the message that needs to be sent to other nodes.
        // Only the leader call returns true.
        if !ready.messages().is_empty() {
            self.send_messages(ready.take_messages()).await?;
        }

        // Process snapshots.
        if *ready.snapshot() != Snapshot::default() {
            self.storage
                .gen_apply_snapshot_job(ready.snapshot().clone())?;
        }

        // Get the committed log entries, that is, the messages confirmed by most nodes.
        // Only the leader will run.
        let mut apply_res = self
            .apply_committed_entries(ready.take_committed_entries(), promise)
            .await;

        // Get the normal log entries of the ready structure and save it.
        if !ready.entries().is_empty() {
            self.storage.append(&ready.entries()[..])?;
        }

        // If hard state changes, it needs to be saved.
        if let Some(hs) = ready.hs() {
            let store = self.raw.mut_store();
            store.set_hard_state(hs)?;
        }

        if let Some(ss) = ready.ss() {
            info!(
                "Raft soft state change, node id {}, current leader {}",
                self.id(),
                self.leader()
            );
            self.role_monitor.advance_role(ss);
        }

        // Get the message that the leader has fallen into the disk and send these messages to other nodes.
        if !ready.persisted_messages().is_empty() {
            // Send out the persisted messages come from the node.
            self.send_messages(ready.take_persisted_messages()).await?;
        }

        // Execute advance to update the raft module status.
        let mut light_rd = self.raw.advance(ready);
        // advance returns a new commit index, which needs to be persisted.
        if let Some(commit) = light_rd.commit_index() {
            self.storage.set_hard_state_commit(commit)?;
        }

        // The advance interface will return a new raft msg.
        self.send_messages(light_rd.take_messages()).await?;

        // The advance interface will return a new committed entries.
        if apply_res.is_ok() {
            apply_res = self
                .apply_committed_entries(light_rd.take_committed_entries(), promise)
                .await;
        }

        match apply_res {
            Err(e) => {
                if self.is_leader() {
                    // The leader replays the UFS logs. If it fails, it logs a warning and doesn't execute `advance_apply`
                    // in this run. The committed entries remain committed and will be re-applied on the next `on_ready`.
                    // `advance_apply` is only called after a successful apply, which ensures idempotency across retries.
                    warn!("apply_committed_entries err {}", e)
                } else {
                    return Err(e);
                }
            }

            Ok(()) => self.raw.advance_apply(),
        }

        // Determine whether a snapshot is needed.
        self.apply_create_snapshot()?;

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
                })
                .build();

            let client = self.client.clone();
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
                if let Err(e) = res {
                    warn!(
                        "send message error, to {}, index {}, msg_type {:?}: {}",
                        to, index, msg_type, e
                    );
                }
            });
        }

        Ok(())
    }

    pub fn apply_config_change(&mut self, entry: &Entry) -> RaftResult<ConfChangeResponse> {
        let change: ConfChange = PMessage::decode(entry.get_data())?;
        let id = change.get_node_id();

        match change.get_change_type() {
            ConfChangeType::AddNode => {
                let addr: InetAddr = SerdeUtils::deserialize(change.get_context())?;
                info!(
                    "Raft adding node: {}({}), current leader: {}({:?})",
                    id,
                    addr,
                    self.leader(),
                    self.group.get_addr(&self.leader())
                );
                self.group.insert(id, &addr);
                self.client.add_node(id, &addr)?;
            }

            ConfChangeType::RemoveNode => {
                if change.get_node_id() == self.id() {
                    self.role_monitor.advance_exit();
                } else {
                    self.group.remove(&id);
                }
            }

            _ => unimplemented!(),
        }

        // When a new node joins, create a snapshot.
        if let Ok(cs) = self.raw.apply_conf_change(&change) {
            let store = self.raw.mut_store();
            store.set_conf_state(&cs)?;
        }

        Ok(ConfChangeResponse::default())
    }

    async fn apply_propose(&mut self, entry: &Entry) -> RaftResult<ProposeResponse> {
        self.storage
            .apply_propose(self.is_leader(), &entry.data)
            .await?;
        Ok(ProposeResponse::default())
    }

    // Whether you need to create a new snapshot.
    fn apply_create_snapshot(&mut self) -> RaftResult<()> {
        if !self.storage.can_generate_snapshot() {
            return Ok(());
        }

        let last_applied = self.raw.raft.raft_log.applied;
        let diff = last_applied - self.last_snapshot_index;

        if (LocalTime::mills() - self.last_snapshot_ms > self.snapshot_interval_ms && diff > 0)
            || diff > self.snapshot_entries
        {
            if self.is_leader() {
                self.commit_info
                    .insert(self.id(), self.raw.raft.raft_log.committed);
            }

            let raw_compact = *(self.commit_info.values().min().unwrap_or(&0));
            let compact_id = raw_compact.min(last_applied);
            self.storage
                .gen_create_snapshot_job(self.id(), last_applied, compact_id)?;

            self.last_snapshot_ms = LocalTime::mills();
            self.last_snapshot_index = last_applied;
        }

        Ok(())
    }

    async fn apply_committed_entries(
        &mut self,
        entries: Vec<Entry>,
        client_send: &mut HashMap<i64, Callback>,
    ) -> RaftResult<()> {
        for entry in entries {
            if entry.get_data().is_empty() {
                continue;
            }
            let req_id: i64 = SerdeUtils::deserialize(entry.get_context())?;

            let rep_msg = if let EntryType::EntryConfChange = entry.get_entry_type() {
                let rep = self.apply_config_change(&entry)?;
                Builder::new_rpc(RaftCode::ConfChange)
                    .response(ResponseStatus::Success)
                    .req_id(req_id)
                    .proto_header(rep)
                    .build()
            } else {
                let rep = self.apply_propose(&entry).await?;
                Builder::new_rpc(RaftCode::Propose)
                    .response(ResponseStatus::Success)
                    .req_id(req_id)
                    .proto_header(rep)
                    .build()
            };

            // Followers only need to replay the message and do not need to respond to the customer service.
            if self.is_leader() {
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
