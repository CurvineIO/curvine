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

use std::sync::Arc;

use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    CancelJobRequest, CancelJobResponse, GetJobStatusRequest, GetJobStatusResponse,
    SubmitJobRequest, SubmitJobResponse, TaskReportRequest, TaskReportResponse,
};
use curvine_common::state::{
    JobStatus, LoadJobCommand, LoadJobResult, WorkProgress, WorkState, WorkType,
};
use curvine_common::utils::{ProtoUtils, SerdeUtils};
use curvine_common::FsResult;

use crate::file::FsClient;

/// Job master client
#[derive(Clone)]
pub struct JobMasterClient {
    client: Arc<FsClient>,
}

impl JobMasterClient {
    pub fn new(client: Arc<FsClient>) -> Self {
        Self { client }
    }

    pub async fn submit_load(&self, path: impl AsRef<str>) -> FsResult<LoadJobResult> {
        self.submit_load_job(LoadJobCommand::builder(path.as_ref()).build())
            .await
    }

    // Submit loading task
    pub async fn submit_load_job(&self, command: LoadJobCommand) -> FsResult<LoadJobResult> {
        let req = SubmitJobRequest {
            job_type: WorkType::Load.into(),
            job_command: SerdeUtils::serialize(&command)?,
        };

        let rep: SubmitJobResponse = self.client.rpc(RpcCode::SubmitJob, req).await?;
        Ok(LoadJobResult {
            job_id: rep.job_id,
            target_path: rep.target_path,
        })
    }

    /// Get loading task status according to the path
    pub async fn get_job_status(&self, job_id: impl AsRef<str>) -> FsResult<JobStatus> {
        let req = GetJobStatusRequest {
            job_id: job_id.as_ref().to_string(),
            verbose: false,
        };

        let status: GetJobStatusResponse = self.client.rpc(RpcCode::GetJobStatus, req).await?;

        Ok(JobStatus {
            job_id: status.job_id,
            state: WorkState::from(status.state as i8),
            source_path: status.source_path,
            target_path: status.target_path,
            progress: ProtoUtils::work_progress_from_pb(status.progress),
        })
    }

    /// Cancel the loading task
    pub async fn cancel_job(&self, job_id: impl AsRef<str>) -> FsResult<()> {
        let req = CancelJobRequest {
            job_id: job_id.as_ref().to_string(),
        };
        let _: CancelJobResponse = self.client.rpc(RpcCode::CancelJob, req).await?;
        Ok(())
    }

    pub async fn report_task(
        &self,
        job_id: impl AsRef<str>,
        task_id: impl AsRef<str>,
        report: WorkProgress,
    ) -> FsResult<()> {
        let req = TaskReportRequest {
            job_id: job_id.as_ref().to_string(),
            task_id: task_id.as_ref().to_string(),
            report: ProtoUtils::work_progress_to_pb(report),
        };
        let _: TaskReportResponse = self.client.rpc(RpcCode::ReportTask, req).await?;
        Ok(())
    }
}
