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
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::RpcCode;
use curvine_model::LoadTaskInfo;
use curvine_proto::*;
use curvine_rpc::handler::MessageHandler;
use curvine_rpc::message::{Builder, Message, RequestStatus, ResponseStatus};
use curvine_runtime::common::SerdeUtils;
use curvine_runtime::runtime::Runtime;
use curvine_runtime::sync::FastMutex;
use std::sync::Arc;

pub struct WorkerHandler {
    pub store: BlockStore,
    pub handler: FastMutex<Option<BlockHandler>>,
    pub task_manager: Arc<TaskManager>,
    pub rt: Arc<Runtime>,
    pub replication_handler: WorkerReplicationHandler,
}

impl MessageHandler for WorkerHandler {
    type Error = FsError;

    fn handle(&self, msg: &Message) -> FsResult<Message> {
        crate::fault_point! {
            sync,
            name: "worker.rpc.before_dispatch",
            description: "Before a Worker RPC is dispatched to its business handler",
            context: {
                "req_id" => msg.req_id(),
                "rpc_code" => msg.code() as i32,
                "request_status" => i8::from(msg.request_status()),
            },
            return_error: |fault| Err(FsError::common(fault.message)),
        }

        let code = RpcCode::from(msg.code());
        match code {
            RpcCode::SubmitTask => self.task_submit(msg),

            RpcCode::QueryTransferTask => self.query_transfer_task(msg),

            RpcCode::CancelJob => self.cancel_job(msg),

            RpcCode::SubmitBlockReplicationJob => self.replication_handler.handle(msg),

            _ => {
                let mut handler = self.handler.lock();
                let h = self.get_handler(msg, &mut handler)?;
                let res = h.handle(msg);

                if res
                    .as_ref()
                    .is_ok_and(|msg| msg.response_status() == ResponseStatus::Success)
                    && matches!(
                        msg.request_status(),
                        RequestStatus::Cancel | RequestStatus::Complete
                    )
                {
                    let _ = handler.take();
                };

                res
            }
        }
    }
}

impl WorkerHandler {
    fn get_handler<'a>(
        &self,
        msg: &Message,
        handler: &'a mut Option<BlockHandler>,
    ) -> FsResult<&'a mut BlockHandler> {
        let code = RpcCode::from(msg.code());

        let need_new_handler = match msg.request_status() {
            RequestStatus::Open => true,
            _ => handler.is_none() || !Self::handler_matches_code(handler, code),
        };

        if need_new_handler {
            let _ = handler.replace(BlockHandler::new(code, self.store.clone())?);
        }

        match handler.as_mut() {
            None => err_box!("The request is not initialized"),
            Some(v) => Ok(v),
        }
    }

    // Check if the current handler type matches the request code
    fn handler_matches_code(handler: &Option<BlockHandler>, code: RpcCode) -> bool {
        matches!(
            (handler, code),
            (Some(BlockHandler::Writer(_)), RpcCode::WriteBlock)
                | (Some(BlockHandler::Reader(_)), RpcCode::ReadBlock)
                | (
                    Some(BlockHandler::BatchWriter(_)),
                    RpcCode::WriteBlocksBatch
                )
                | (Some(BlockHandler::BatchWriter(_)), RpcCode::WriteBlock)
        )
    }

    pub fn task_submit(&self, msg: &Message) -> FsResult<Message> {
        let req: SubmitTaskRequest = msg.parse_header()?;
        let task: LoadTaskInfo = SerdeUtils::deserialize(&req.task_command)?;
        let task_id = task.task_id.clone();

        let submit = self.task_manager.submit_task(task)?;
        let response = SubmitTaskResponse {
            task_id,
            accepted: Some(submit.accepted),
            reject_reason: Some(submit.reject_reason),
        };

        Ok(Builder::success(msg).proto_header(response).build())
    }

    pub fn cancel_job(&self, msg: &Message) -> FsResult<Message> {
        let req: CancelJobRequest = msg.parse_header()?;
        self.task_manager.cancel_job(req.job_id)?;
        Ok(msg.success())
    }

    pub fn query_transfer_task(&self, msg: &Message) -> FsResult<Message> {
        let req: QueryTransferTaskRequest = msg.parse_header()?;
        let progress = self.task_manager.query_transfer_task(
            &req.job_id,
            &req.task_id,
            req.run_id,
            req.attempt_id,
            &req.worker_session_id,
        )?;

        let response = match progress {
            Some(progress) => QueryTransferTaskResponse {
                found: true,
                state: transfer_task_state_code(progress.state),
                progress: TransferProgressProto {
                    loaded_size: progress.loaded_size,
                    total_size: progress.total_size,
                    update_time: progress.update_time,
                    message: progress.message,
                },
            },
            None => QueryTransferTaskResponse {
                found: false,
                state: TransferTaskStateProto::TransferTaskFailed.into(),
                progress: TransferProgressProto {
                    loaded_size: 0,
                    total_size: 0,
                    update_time: 0,
                    message: String::new(),
                },
            },
        };
        Ok(Builder::success(msg).proto_header(response).build())
    }
}

fn transfer_task_state_code(state: curvine_model::JobTaskState) -> i32 {
    match state {
        curvine_model::JobTaskState::Pending | curvine_model::JobTaskState::UNKNOWN => {
            TransferTaskStateProto::TransferTaskPending.into()
        }
        curvine_model::JobTaskState::Loading => TransferTaskStateProto::TransferTaskRunning.into(),
        curvine_model::JobTaskState::Completed => {
            TransferTaskStateProto::TransferTaskCompleted.into()
        }
        curvine_model::JobTaskState::Failed => TransferTaskStateProto::TransferTaskFailed.into(),
        curvine_model::JobTaskState::Canceled => {
            TransferTaskStateProto::TransferTaskCanceled.into()
        }
    }
}
