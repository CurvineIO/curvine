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

use curvine_common::fs::RpcCode;
use curvine_common::proto::*;
use curvine_common::state::{JobTaskType, LoadTaskInfo};
use curvine_common::utils::{RpcUtils, SerdeUtils};
use curvine_common::FsResult;
use orpc::client::RpcClient;
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

    pub async fn submit_load_task(&self, task: LoadTaskInfo) -> FsResult<()> {
        let request = SubmitTaskRequest {
            task_type: JobTaskType::Load.into(),
            task_command: SerdeUtils::serialize(&task)?,
        };

        let _: SubmitTaskResponse = self.rpc(RpcCode::SubmitTask, request).await?;
        Ok(())
    }

    pub async fn cancel_job(&self, job_id: impl AsRef<str>) -> FsResult<()> {
        let request = CancelJobRequest {
            job_id: job_id.as_ref().to_string(),
        };

        let _: CancelJobResponse = self.rpc(RpcCode::CancelJob, request).await?;
        Ok(())
    }
}
