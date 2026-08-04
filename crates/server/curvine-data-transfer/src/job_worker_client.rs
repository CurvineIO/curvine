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

use std::time::Duration;

use curvine_error::FsResult;
use curvine_fs_api::RpcCode;
use curvine_model::{JobTaskType, LoadTaskInfo};
use curvine_proto::*;
use curvine_rpc::client::RpcClient;
use curvine_rpc::RpcUtils;
use curvine_runtime::common::SerdeUtils;
use prost::Message as PMessage;

#[derive(Clone)]
pub struct JobWorkerClient {
    client: RpcClient,
    timeout: Duration,
}

impl JobWorkerClient {
    pub fn new(client: RpcClient, timeout: Duration) -> Self {
        Self { client, timeout }
    }

    pub async fn rpc<T, R>(&self, code: RpcCode, header: T) -> FsResult<R>
    where
        T: PMessage + Default,
        R: PMessage + Default,
    {
        RpcUtils::proto_rpc(&self.client, self.timeout, code, header).await
    }

    pub async fn submit_load_task_response(
        &self,
        task: LoadTaskInfo,
    ) -> FsResult<SubmitTaskResponse> {
        let request = SubmitTaskRequest {
            task_type: JobTaskType::Load.into(),
            task_command: SerdeUtils::serialize(&task)?,
        };

        self.rpc(RpcCode::SubmitTask, request).await
    }

    pub async fn submit_load_task(&self, task: LoadTaskInfo) -> FsResult<()> {
        let response = self.submit_load_task_response(task).await?;
        if !response.accepted.unwrap_or(true) {
            return Err(curvine_error::FsError::common(format!(
                "Worker rejected load task {}: {}",
                response.task_id,
                response
                    .reject_reason
                    .unwrap_or_else(|| "no reason provided".to_string())
            )));
        }
        Ok(())
    }

    pub async fn cancel_job(&self, job_id: impl AsRef<str>) -> FsResult<()> {
        let request = CancelJobRequest {
            job_id: job_id.as_ref().to_string(),
        };

        let _: CancelJobResponse = self.rpc(RpcCode::CancelJob, request).await?;
        Ok(())
    }

    pub async fn query_transfer_task(
        &self,
        job_id: impl AsRef<str>,
        run_id: u64,
        task_id: impl AsRef<str>,
        attempt_id: u64,
        worker_session_id: impl AsRef<str>,
    ) -> FsResult<QueryTransferTaskResponse> {
        let request = QueryTransferTaskRequest {
            job_id: job_id.as_ref().to_string(),
            run_id,
            task_id: task_id.as_ref().to_string(),
            attempt_id,
            worker_session_id: worker_session_id.as_ref().to_string(),
        };

        self.rpc(RpcCode::QueryTransferTask, request).await
    }
}
