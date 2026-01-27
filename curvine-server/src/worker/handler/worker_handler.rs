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

use crate::worker::block::BlockStore;
use crate::worker::handler::BlockHandler;
use crate::worker::handler::WriteContext;
use crate::worker::replication::worker_replication_handler::WorkerReplicationHandler;
use crate::worker::task::TaskManager;
use curvine_client::file::FsContext;
use curvine_common::error::FsError;
use curvine_common::fs::RpcCode;
use curvine_common::proto::*;
use curvine_common::state::LoadTaskInfo;
use curvine_common::utils::SerdeUtils;
use curvine_common::FsResult;
use log::debug;
use orpc::handler::MessageHandler;
use orpc::message::{Builder, Message, RequestStatus};
use orpc::runtime::Runtime;
use std::sync::Arc;

pub struct WorkerHandler {
    pub store: BlockStore,
    pub handler: Option<BlockHandler>,
    pub task_manager: Arc<TaskManager>,
    pub rt: Arc<Runtime>,
    pub replication_handler: WorkerReplicationHandler,
    pub fs_context: Arc<FsContext>,
}

impl MessageHandler for WorkerHandler {
    type Error = FsError;

    fn is_sync(&self, msg: &Message) -> bool {
        self.is_sync_request(msg)
    }

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let code = RpcCode::from(msg.code());
        match code {
            RpcCode::SubmitTask => self.task_submit(msg),
            RpcCode::CancelJob => self.cancel_job(msg),
            RpcCode::SubmitBlockReplicationJob => self.replication_handler.handle(msg),
            _ => {
                let h = self.get_handler(msg)?;
                let res = h.handle(msg);
                if matches!(
                    msg.request_status(),
                    RequestStatus::Cancel | RequestStatus::Complete
                ) {
                    let _ = self.handler.take();
                }
                res
            }
        }
    }

    async fn async_handle(&mut self, msg: Message) -> FsResult<Message> {
        let code = RpcCode::from(msg.code());
        match code {
            RpcCode::SubmitTask => self.task_submit(&msg),
            RpcCode::CancelJob => self.cancel_job(&msg),
            RpcCode::SubmitBlockReplicationJob => self.replication_handler.handle(&msg),
            _ => {
                let request_status = msg.request_status();
                let h = self.get_handler(&msg)?;
                let res = h.async_handle(msg).await;
                if matches!(
                    request_status,
                    RequestStatus::Cancel | RequestStatus::Complete
                ) {
                    let _ = self.handler.take();
                }
                res
            }
        }
    }
}

impl WorkerHandler {
    fn is_sync_request(&self, msg: &Message) -> bool {
        let code = RpcCode::from(msg.code());
        match code {
            RpcCode::SubmitTask | RpcCode::CancelJob | RpcCode::SubmitBlockReplicationJob => true,
            RpcCode::WriteBlock => {
                if let Some(handler) = self
                    .handler
                    .as_ref()
                    .filter(|h| Self::handler_matches_code(h, code))
                {
                    handler.is_sync(msg)
                } else if msg.request_status() == RequestStatus::Open {
                    match WriteContext::from_req(msg) {
                        Ok(ctx) => {
                            let has_pipeline_stream = ctx.has_pipeline_stream();
                            let result = !has_pipeline_stream;
                            debug!(
                                "WorkerHandler::is_sync_request: WriteBlock Open, has_pipeline_stream={}, is_sync={}, block_id={}",
                                has_pipeline_stream, result, ctx.block.id
                            );
                            result
                        }
                        Err(e) => {
                            debug!(
                                "WorkerHandler::is_sync_request: failed to parse WriteContext: {}, defaulting to sync",
                                e
                            );
                            true
                        }
                    }
                } else {
                    false
                }
            }
            RpcCode::ReadBlock | RpcCode::WriteBlocksBatch => true,
            _ => self
                .handler
                .as_ref()
                .filter(|h| Self::handler_matches_code(h, code))
                .map(|h| h.is_sync(msg))
                .unwrap_or(true),
        }
    }

    fn handler_matches_code(handler: &BlockHandler, code: RpcCode) -> bool {
        matches!(
            (handler, code),
            (BlockHandler::Writer(_), RpcCode::WriteBlock)
                | (BlockHandler::Reader(_), RpcCode::ReadBlock)
                | (BlockHandler::BatchWriter(_), RpcCode::WriteBlocksBatch)
        )
    }

    fn get_handler(&mut self, msg: &Message) -> FsResult<&mut BlockHandler> {
        let code = RpcCode::from(msg.code());
        let need_new = self.handler.is_none()
            || matches!(msg.request_status(), RequestStatus::Open)
            || self
                .handler
                .as_ref()
                .map(|h| !Self::handler_matches_code(h, code))
                .unwrap_or(true);
        if need_new {
            self.handler = Some(BlockHandler::new(
                msg,
                self.store.clone(),
                self.fs_context.clone(),
            )?);
        }

        self.handler
            .as_mut()
            .ok_or_else(|| FsError::common("The request is not initialized"))
    }

    pub fn task_submit(&self, msg: &Message) -> FsResult<Message> {
        let req: SubmitTaskRequest = msg.parse_header()?;
        let task: LoadTaskInfo = SerdeUtils::deserialize(&req.task_command)?;
        let task_id = task.task_id.clone();

        self.task_manager.submit_task(task)?;
        let response = SubmitTaskResponse { task_id };

        Ok(Builder::success(msg).proto_header(response).build())
    }

    pub fn cancel_job(&self, msg: &Message) -> FsResult<Message> {
        let req: CancelJobRequest = msg.parse_header()?;
        self.task_manager.cancel_job(req.job_id)?;
        Ok(msg.success())
    }
}
