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

use crate::master::RpcContext;
use crate::worker::replication::worker_replication_manager::WorkerReplicationManager;
use curvine_common::error::FsError;
use curvine_common::fs::RpcCode;
use curvine_common::proto::SumbitBlockReplicationRequest;
use curvine_common::FsResult;
use log::warn;
use orpc::error::ErrorImpl;
use orpc::handler::MessageHandler;
use orpc::message::Message;

#[derive(Clone)]
pub struct WorkerReplicationHandler {
    manager: WorkerReplicationManager,
}

impl WorkerReplicationHandler {
    pub fn new(manager: &WorkerReplicationManager) -> Self {
        Self {
            manager: manager.clone(),
        }
    }

    pub fn accept_job(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: SumbitBlockReplicationRequest = ctx.parse_header()?;
        self.manager.accept_job(req.into())?;
        todo!()
    }
}

impl MessageHandler for WorkerReplicationHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let code = RpcCode::from(msg.code());

        let mut rpc_context = RpcContext::new(msg);
        let ctx = &mut rpc_context;

        let response = match code {
            RpcCode::SubmitBlockReplicationJob => self.accept_job(ctx),
            _ => Err(FsError::Common(ErrorImpl::with_source(
                format!("Unsupported operation: {:?}", code).into(),
            ))),
        };

        // Record the request processing status
        if let Err(ref e) = response {
            warn!("Request {:?} failed: {}", code, e);
        }

        response
    }
}
