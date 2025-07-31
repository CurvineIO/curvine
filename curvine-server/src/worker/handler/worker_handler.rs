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
use crate::worker::block::BlockStore;
use crate::worker::handler::BlockHandler;
use crate::worker::load::FileLoadService;
use curvine_common::error::FsError;
use curvine_common::fs::RpcCode;
use curvine_common::proto::*;
use curvine_common::FsResult;
use orpc::err_box;
use orpc::handler::MessageHandler;
use orpc::message::Message;
use orpc::runtime::{RpcRuntime, Runtime};
use std::sync::Arc;

pub struct WorkerHandler {
    pub store: BlockStore,
    pub handler: Option<BlockHandler>,
    pub file_loader: Arc<FileLoadService>,
    pub rt: Arc<Runtime>,
}

impl MessageHandler for WorkerHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let code = RpcCode::from(msg.code());
        let mut rpc_context = RpcContext::new(msg);
        let ctx = &mut rpc_context;

        // Process load task requests
        if code == RpcCode::SubmitLoadTask {
            return self.handle_load_task(ctx);
        }

        // Handle load task requests
        if code == RpcCode::CancelLoadJob {
            return self.cancel_load_job(ctx);
        }

        // Leave other requests to BlockHandler for processing
        let h = self.get_handler(msg)?;
        h.handle(msg)
    }
}

impl WorkerHandler {
    fn get_handler(&mut self, msg: &Message) -> FsResult<&mut BlockHandler> {
        if self.handler.is_none() {
            let handler = BlockHandler::new(RpcCode::from(msg.code()), self.store.clone())?;

            let _ = self.handler.replace(handler);
        }

        match self.handler.as_mut() {
            None => err_box!("The request is not initialized"),
            Some(v) => Ok(v),
        }
    }

    /// Process load task requests (received directly from RPC)
    fn handle_load_task(&mut self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: LoadTaskRequest = ctx.parse_header()?;
        let response = self.rt.block_on(self.file_loader.handle_load_task(req))?;
        ctx.response(response)
    }

    fn cancel_load_job(&mut self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: CancelLoadRequest = ctx.parse_header()?;
        let response = self.rt.block_on(self.file_loader.cancel_load_task(req))?;
        ctx.response(response)
    }
}
