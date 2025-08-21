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

use crate::master::replication::block_replication_manager::BlockReplicationManager;
use curvine_common::error::FsError;
use orpc::handler::MessageHandler;
use orpc::message::Message;
use orpc::runtime::AsyncRuntime;
use orpc::CommonResult;
use std::sync::Arc;

pub struct BlockReplicationRpcHandler {
    manager: BlockReplicationManager,
    runtime: Arc<AsyncRuntime>,
}

impl BlockReplicationRpcHandler {
    pub fn new(manager: BlockReplicationManager, runtime: Arc<AsyncRuntime>) -> Self {
        todo!()
    }

    pub fn submit_replication_job() -> CommonResult<bool> {
        todo!()
    }

    pub fn accept_replication_result() -> CommonResult<()> {
        todo!()
    }
}

impl MessageHandler for BlockReplicationRpcHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> Result<Message, Self::Error> {
        todo!()
    }
}
