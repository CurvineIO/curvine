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

use crate::conf::JournalConf;
use crate::raft::storage::{AppStorage, LogStorage};
use crate::raft::{RaftNode, RaftResult, RaftServer, RoleMonitor, RoleStateListener};
use orpc::runtime::Runtime;
use orpc::sync::StateCtl;
use slog::{o, Drain};
use std::sync::Arc;

/// Raft log synchronization wrapper class.
/// Simplifies the creation of raft nodes.
pub struct RaftJournal<A, B> {
    rt: Arc<Runtime>,
    log_store: A,
    app_store: B,
    role_monitor: RoleMonitor,
    conf: JournalConf,
    logger: slog::Logger,
}

impl<A, B> RaftJournal<A, B>
where
    A: LogStorage,
    B: AppStorage,
{
    pub fn new(
        rt: Arc<Runtime>,
        log_store: A,
        app_store: B,
        conf: JournalConf,
        role_monitor: RoleMonitor,
    ) -> Self {
        // Create a raft client manager.
        let logger = Self::create_slog(&conf);

        Self {
            rt,
            log_store,
            app_store,
            role_monitor,
            conf,
            logger,
        }
    }

    // Create a raft-rs logger to record raft internal modification logs for easy problem detection.
    fn create_slog(_: &JournalConf) -> slog::Logger {
        let drain = slog_stdlog::StdLog.fuse();
        let drain = drain.filter_level(slog::Level::Info).fuse();
        slog::Logger::root(drain, o!())
    }

    pub async fn run(self) -> RaftResult<RoleStateListener> {
        self.run_candidate().await
    }

    // Start the current raft node as a leader candidate node.
    pub async fn run_candidate(self) -> RaftResult<RoleStateListener> {
        let mut server = RaftServer::with_rt(self.rt.clone(), &self.conf);
        let receiver = server.take_receiver()?;
        let sender = server.new_sender();
        let node = RaftNode::new_candidate(
            self.rt,
            &self.conf,
            self.log_store,
            self.app_store,
            self.role_monitor,
            receiver,
            sender,
            &self.logger,
        )
        .await
        .unwrap();

        Self::start(server, node).await
    }

    pub async fn run_follower(self) -> RaftResult<RoleStateListener> {
        let mut server = RaftServer::with_rt(self.rt.clone(), &self.conf);
        let receiver = server.take_receiver()?;
        let sender = server.new_sender();

        let node = RaftNode::new_follower(
            self.rt,
            &self.conf,
            self.log_store,
            self.app_store,
            self.role_monitor,
            receiver,
            sender,
            &self.logger,
        )
        .await?;

        Self::start(server, node).await
    }

    async fn start(server: RaftServer, node: RaftNode<A, B>) -> RaftResult<RoleStateListener> {
        // Start the tcp service and wait for it to be ready.
        let mut server_listener = server.start();
        server_listener.wait_running().await?;

        Ok(node.start())
    }

    pub fn new_state_listener(&self) -> RoleStateListener {
        self.role_monitor.new_listener()
    }

    pub fn new_state_ctl(&self) -> StateCtl {
        self.role_monitor.read_ctl()
    }

    pub fn new_rt(&self) -> Arc<Runtime> {
        self.rt.clone()
    }

    pub fn log_store(&self) -> &A {
        &self.log_store
    }

    pub fn app_store(&self) -> &B {
        &self.app_store
    }
}
