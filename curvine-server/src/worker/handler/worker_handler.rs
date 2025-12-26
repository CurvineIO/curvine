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
use crate::worker::replication::worker_replication_handler::WorkerReplicationHandler;
use crate::worker::task::TaskManager;
use curvine_common::error::FsError;
use curvine_common::fs::RpcCode;
use curvine_common::proto::*;
use curvine_common::state::LoadTaskInfo;
use curvine_common::utils::SerdeUtils;
use curvine_common::FsResult;
use orpc::err_box;
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
}

impl MessageHandler for WorkerHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let code = RpcCode::from(msg.code());
        match code {
            RpcCode::SubmitTask => self.task_submit(msg),

            RpcCode::CancelJob => self.cancel_job(msg),

            RpcCode::SubmitBlockReplicationJob => self.replication_handler.handle(msg),

            _ => {
                let h = self.get_handler(msg)?;
                h.handle(msg)
            }
        }
    }
}

impl WorkerHandler {
    fn get_handler(&mut self, msg: &Message) -> FsResult<&mut BlockHandler> {
        if self.handler.is_none() || !matches!(msg.request_status(), RequestStatus::Running) {
            let handler = BlockHandler::new(RpcCode::from(msg.code()), self.store.clone())?;

            let _ = self.handler.replace(handler);
        }

        match self.handler.as_mut() {
            None => err_box!("The request is not initialized"),
            Some(v) => Ok(v),
        }
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
