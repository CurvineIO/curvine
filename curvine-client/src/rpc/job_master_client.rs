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

use log::info;
use std::sync::Arc;
use tokio::time;

use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    CancelJobRequest, CancelJobResponse, GetJobStatusRequest, GetJobStatusResponse,
    SubmitJobRequest, SubmitJobResponse, TaskReportRequest, TaskReportResponse,
};
use curvine_common::state::{
    JobStatus, JobTaskProgress, JobTaskState, JobTaskType, LoadJobCommand, LoadJobResult,
};
use curvine_common::utils::{ProtoUtils, SerdeUtils};
use curvine_common::FsResult;
use orpc::common::TimeSpent;
use orpc::err_box;

use crate::file::{FsClient, FsContext};

/// Job master client
#[derive(Clone)]
pub struct JobMasterClient {
    client: Arc<FsClient>,
}

impl JobMasterClient {
    pub fn new(client: Arc<FsClient>) -> Self {
        Self { client }
    }

    pub fn with_context(context: &Arc<FsContext>) -> Self {
        let client = Arc::new(FsClient::new(context.clone()));
        Self::new(client)
    }

    pub async fn submit_load(&self, path: impl AsRef<str>) -> FsResult<LoadJobResult> {
        self.submit_publish(path).await
    }

    pub async fn submit_publish(&self, path: impl AsRef<str>) -> FsResult<LoadJobResult> {
        self.submit_load_job(LoadJobCommand::publish_builder(path.as_ref()).build())
            .await
    }

    pub async fn submit_hydrate(
        &self,
        path: impl AsRef<str>,
        expected_target_mtime: Option<i64>,
        expected_target_missing: bool,
    ) -> FsResult<LoadJobResult> {
        let mut builder = LoadJobCommand::hydrate_builder(path.as_ref());
        if let Some(mtime) = expected_target_mtime {
            builder = builder.expected_target_mtime(mtime);
        }
        if expected_target_missing {
            builder = builder.expected_target_missing(true);
        }
        self.submit_load_job(builder.build()).await
    }

    // Submit loading task
    pub async fn submit_load_job(&self, command: LoadJobCommand) -> FsResult<LoadJobResult> {
        let req = SubmitJobRequest {
            job_type: JobTaskType::Load.into(),
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
            state: JobTaskState::from(status.state as i8),
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
        report: JobTaskProgress,
    ) -> FsResult<()> {
        let req = TaskReportRequest {
            job_id: job_id.as_ref().to_string(),
            task_id: task_id.as_ref().to_string(),
            report: ProtoUtils::work_progress_to_pb(report),
        };
        let _: TaskReportResponse = self.client.rpc(RpcCode::ReportTask, req).await?;
        Ok(())
    }

    pub async fn wait_job_complete(&self, job_id: impl AsRef<str>, mark: &str) -> FsResult<()> {
        let time = self.client.conf().client.max_sync_wait_timeout;
        time::timeout(time, self.wait_job_complete0(job_id, mark)).await?
    }

    async fn wait_job_complete0(&self, job_id: impl AsRef<str>, mark: &str) -> FsResult<()> {
        let mut ticks = 0;
        let time = TimeSpent::new();
        let conf = &self.client.conf().client;
        let job_id = job_id.as_ref();

        loop {
            let status = self.get_job_status(job_id).await?;
            match status.state {
                JobTaskState::Completed => break,

                JobTaskState::Failed | JobTaskState::Canceled => {
                    return err_box!(
                        "job {} {:?}: {}",
                        status.job_id,
                        status.state,
                        status.progress.message
                    )
                }

                _ => {
                    ticks += 1;

                    let sleep_time = conf
                        .sync_check_interval_max
                        .min(conf.sync_check_interval_min * ticks);
                    time::sleep(sleep_time).await;

                    if ticks % conf.sync_check_log_tick == 0 {
                        info!(
                            "[{}] waiting for job {} to complete, elapsed: {} ms, progress: {}",
                            mark,
                            status.job_id,
                            time.used_ms(),
                            status.progress_string(false)
                        );
                    }
                }
            }
        }

        Ok(())
    }
}
