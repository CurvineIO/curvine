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
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    CancelJobRequest, CancelJobResponse, GetJobStatusRequest, GetJobStatusResponse,
    SubmitJobRequest, SubmitJobResponse, TaskReportRequest, TaskReportResponse,
};
use curvine_common::state::LoadJobCommand;
use curvine_common::utils::{ProtoUtils, SerdeUtils};
use curvine_common::FsResult;
use orpc::err_box;
use orpc::message::Message;
use std::sync::Arc;

/// The master loads the task service
/// Handle load task related requests from clients and Worker
pub struct JobHandler {
    job_manager: Arc<JobManager>,
}

impl JobHandler {
    /// Create a new Master Loading Task Service
    pub fn new(job_manager: Arc<JobManager>) -> Self {
        Self { job_manager }
    }

    /// Submit loading task
    ///
    /// Handles the submission of a new load job by parsing the request,
    /// validating parameters, and forwarding to the load manager.
    pub fn submit_load_job(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: SubmitJobRequest = ctx.parse_header()?;
        let command: LoadJobCommand = SerdeUtils::deserialize(&req.job_command)?;
        ctx.set_audit(Some(command.source_path.clone()), None);

        if command.source_path.is_empty() {
            return err_box!("Path cannot be empty");
        }

        let res = self.job_manager.submit_load_job(command)?;
        let response = SubmitJobResponse {
            job_id: res.job_id,
            target_path: res.target_path,
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

        let status = self.job_manager.get_job_status(&req.job_id)?;
        let response = GetJobStatusResponse {
            job_id: status.job_id,
            state: status.state as i32,
            source_path: status.source_path,
            target_path: status.target_path,
            progress: ProtoUtils::work_progress_to_pb(status.progress),
            expected_source_id: status.expected_source_id,
            expected_source_len: status.expected_source_len,
            expected_source_mtime: status.expected_source_mtime,
            source_generation: status.source_generation,
        };

        ctx.response(response)
    }

    /// Cancel the loading task
    ///
    /// Handles the cancellation of a load job by its ID and returns
    /// the result of the cancellation operation.
    pub fn cancel_job(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: CancelJobRequest = ctx.parse_header()?;

        let job_id = req.job_id;
        ctx.set_audit(Some(job_id.clone()), None);

        self.job_manager.cancel_job(job_id.clone())?;

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

    pub fn handle(&mut self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        match ctx.code {
            RpcCode::SubmitJob => self.submit_load_job(ctx),
            RpcCode::GetJobStatus => self.get_load_status(ctx),
            RpcCode::CancelJob => self.cancel_job(ctx),
            RpcCode::ReportTask => self.task_report(ctx),
            v => err_box!("Unsupported operation: {:?}", v),
        }
    }
}
