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
use crate::master::RpcContext;
use curvine_common::error::FsError;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    ReportBlockReplicationRequest, ReportBlockReplicationResponse, SubmitBlockReplicationResponse,
    SumbitBlockReplicationRequest,
};
use curvine_common::FsResult;
use log::warn;
use orpc::error::ErrorImpl;
use orpc::handler::MessageHandler;
use orpc::message::Message;
use orpc::runtime::AsyncRuntime;
use orpc::CommonResult;
use std::sync::Arc;

#[derive(Clone)]
pub struct BlockReplicationRpcHandler {
    manager: BlockReplicationManager,
    runtime: Arc<AsyncRuntime>,
}

impl BlockReplicationRpcHandler {
    pub fn new(manager: BlockReplicationManager, runtime: Arc<AsyncRuntime>) -> Self {
        todo!()
    }

    pub fn report_replication_result(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: ReportBlockReplicationRequest = ctx.parse_header()?;
        // todo: error handling
        let _ = self.manager.report_replicated_block(req);
        let response = ReportBlockReplicationResponse {
            success: true,
            message: None,
        };
        ctx.response(response)
    }
}

impl MessageHandler for BlockReplicationRpcHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let code = RpcCode::from(msg.code());

        // Create RpcContext
        let mut rpc_context = RpcContext::new(msg);
        let ctx = &mut rpc_context;

        let response = match code {
            RpcCode::ReportBlockReplicationResult => self.report_replication_result(ctx),
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
