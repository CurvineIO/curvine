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

use crate::master::{JobManager, RpcContext};
use curvine_core_error::err_box;
use curvine_error::FsResult;
use curvine_model::{
    JobStatus, JobTaskState, JobTaskType, LoadJobCommand, LoadTaskStatus, ProtoUtils,
};
use curvine_proto::{
    CancelJobRequest, CancelJobResponse, GetJobStatusRequest, GetJobStatusResponse,
    ListJobStatusRequest, ListJobStatusResponse, LoadTaskStatusProto, SubmitJobRequest,
    SubmitJobResponse, TaskReportRequest, TaskReportResponse,
};
use curvine_rpc::message::Message;
use curvine_runtime::common::SerdeUtils;
use std::sync::Arc;

/// The master loads the task service
/// Handle load task related requests from clients and Worker
pub struct JobHandler {
    job_manager: Arc<JobManager>,
}

impl JobHandler {
    fn task_status_response(task: LoadTaskStatus) -> LoadTaskStatusProto {
        LoadTaskStatusProto {
            task_id: task.task_id,
            source_path: task.source_path,
            target_path: task.target_path,
            worker: task.worker,
            state: task.state as i32,
            progress: ProtoUtils::work_progress_to_pb(task.progress),
            create_time: task.create_time,
            update_time: task.update_time,
        }
    }

    fn job_status_response(status: JobStatus) -> GetJobStatusResponse {
        GetJobStatusResponse {
            job_id: status.job_id,
            state: status.state as i32,
            source_path: status.source_path,
            target_path: status.target_path,
            progress: ProtoUtils::work_progress_to_pb(status.progress),
            total_files: Some(status.total_files),
            completed_files: Some(status.completed_files),
            failed_files: Some(status.failed_files),
            running_files: Some(status.running_files),
            pending_files: Some(status.pending_files),
            loading_files: Some(status.loading_files),
            source_type: Some(i32::from(status.source_type)),
            tasks: status
                .tasks
                .into_iter()
                .map(Self::task_status_response)
                .collect(),
        }
    }

    /// Create a new Master Loading Task Service
    pub fn new(job_manager: Arc<JobManager>) -> Self {
        Self { job_manager }
    }

    /// Submit a load or export job.
    ///
    /// Handles the submission of a new job by parsing the request,
    /// validating parameters, and forwarding to the job manager.
    pub async fn submit_job(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: SubmitJobRequest = ctx.parse_header()?;
        let command: LoadJobCommand = SerdeUtils::deserialize(&req.job_command)?;
        ctx.set_audit(Some(command.source_path.clone()), None);

        if command.source_path.is_empty() {
            return err_box!("Path cannot be empty");
        }

        let res = match req.job_type {
            v if v == JobTaskType::Load as i32 => self.job_manager.submit_load_job(command).await?,
            v if v == JobTaskType::Export as i32 => {
                self.job_manager.submit_export_job(command).await?
            }
            v => return err_box!("Unsupported job type: {}", v),
        };
        let response = SubmitJobResponse {
            job_id: res.job_id,
            target_path: res.target_path,
            state: res.state as i32,
        };

        ctx.response(response)
    }

    /// Get the loading task status
    ///
    /// Retrieves the current status of a load job by its ID and constructs
    /// a response with detailed metrics.
    pub fn get_load_status(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: GetJobStatusRequest = ctx.parse_header()?;
        ctx.set_audit(Some(req.job_id.clone()), None);

        let status =
            self.job_manager
                .get_job_status_with_options(&req.job_id, req.verbose, false)?;
        let response = Self::job_status_response(status);

        ctx.response(response)
    }

    pub fn list_load_status(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: ListJobStatusRequest = ctx.parse_header()?;
        ctx.set_audit(req.path_prefix.clone(), None);
        let limit = req.limit.unwrap_or(100) as usize;
        let include_finished = req.include_finished.unwrap_or(true);
        let state_filter = req.state.map(|state| JobTaskState::from(state as i8));
        let include_tasks = req.include_tasks.unwrap_or(false);
        let failed_only = req.failed_only.unwrap_or(false);
        let offset = req.offset.unwrap_or(0) as usize;
        let statuses = self.job_manager.list_job_statuses_filtered(
            req.path_prefix.as_deref(),
            limit,
            include_finished,
            state_filter,
            req.mount_path.as_deref(),
            include_tasks,
            failed_only,
            offset,
        );
        let jobs = statuses
            .into_iter()
            .map(Self::job_status_response)
            .collect();
        ctx.response(ListJobStatusResponse { jobs })
    }

    /// Cancel the loading task
    ///
    /// Handles the cancellation of a load job by its ID and returns
    /// the result of the cancellation operation.
    pub async fn cancel_job(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: CancelJobRequest = ctx.parse_header()?;

        let job_id = req.job_id;
        ctx.set_audit(Some(job_id.clone()), None);

        self.job_manager.cancel_job(job_id.clone()).await?;

        ctx.response(CancelJobResponse {})
    }

    /// Handle the task status reported by Worker
    ///
    /// Processes status reports from worker nodes about load tasks,
    /// updating the job status in the load manager.
    pub fn task_report(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: TaskReportRequest = ctx.parse_header()?;
        let job_id = req.job_id.clone();
        ctx.set_audit(Some(job_id.clone()), None);

        // Process task reports - use block_on to call async method
        self.job_manager.update_progress(
            req.job_id,
            req.task_id,
            ProtoUtils::work_progress_from_pb(req.report),
        )?;

        ctx.response(TaskReportResponse {})
    }
}
