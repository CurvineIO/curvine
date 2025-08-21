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

use crate::master::{LoadManager, RpcContext};
use curvine_common::error::FsError;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    CancelLoadRequest, CancelLoadResponse, GetLoadStatusRequest, GetLoadStatusResponse,
    LoadJobRequest, LoadJobResponse, LoadMetrics, LoadState, LoadTaskReportRequest,
    LoadTaskReportResponse,
};
use curvine_common::FsResult;
use log::{debug, error, info, warn};
use orpc::err_box;
use orpc::error::ErrorImpl;
use orpc::handler::MessageHandler;
use orpc::message::Message;
use orpc::runtime::{RpcRuntime, Runtime};
use std::sync::Arc;

/// The master loads the task service
/// Handle load task related requests from clients and Worker
pub struct MasterLoadService {
    /// Loading Task Manager
    load_manager: Arc<LoadManager>,
    rt: Arc<Runtime>,
}

impl MasterLoadService {
    /// Create a new Master Loading Task Service
    pub fn new(load_manager: Arc<LoadManager>, runtime: Arc<Runtime>) -> Self {
        Self {
            load_manager,
            rt: runtime,
        }
    }

    /// Submit loading task
    ///
    /// Handles the submission of a new load job by parsing the request,
    /// validating parameters, and forwarding to the load manager.
    pub fn submit_load_job(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: LoadJobRequest = ctx.parse_header()?;
        // Check the request parameters
        if req.path.is_empty() {
            return err_box!("Path cannot be empty");
        }

        let ttl = req.ttl.as_deref();
        let recursive = req.recursive.unwrap_or(false);

        // Submit task - use block_on to call async method
        let (job_id, target_path) = self
            .rt
            .block_on(self.load_manager.submit_job(&req.path, ttl, recursive))?;

        // Construct the response
        let response = LoadJobResponse {
            job_id,
            target_path,
        };

        // Return response
        ctx.response(response)
    }

    /// Get the loading task status
    ///
    /// Retrieves the current status of a load job by its ID and constructs
    /// a response with detailed metrics.
    pub fn get_load_status(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: GetLoadStatusRequest = ctx.parse_header()?;

        // Get the task ID
        let job_id = req.job_id.clone();
        ctx.set_audit(Some(job_id.clone()), None);

        info!("Received get_load_status request for job: {}", job_id);

        // Get task status - use block_on to call async method
        let job = match self
            .rt
            .block_on(self.load_manager.get_load_job_status(job_id.clone()))?
        {
            Some(status) => status,
            None => {
                return Err(FsError::Common(ErrorImpl::with_source(
                    format!("Job with id {} not found", job_id).into(),
                )))
            }
        };

        //Accumulate loaded_size and total_size
        let mut total_loaded_size: u64 = 0;
        let mut total_total_size: u64 = 0;

        for task_detail in job.task_details.values() {
            if let Some(loaded_size) = task_detail.loaded_size {
                total_loaded_size += loaded_size;
            }
            if let Some(total_size) = task_detail.total_size {
                total_total_size += total_size;
            }
        }

        // Construct the main task indicator information
        let main_metrics = LoadMetrics {
            job_id: job_id.clone(),
            task_id: "main".to_string(),
            path: job.source_path.clone(),
            target_path: job.target_path.clone(),
            total_size: Some(total_total_size as i64),
            loaded_size: Some(total_loaded_size as i64),
            create_time: Some(job.create_time.timestamp()),
            update_time: Some(job.update_time.timestamp()),
            expire_time: Some(job.expire_time.unwrap().timestamp()),
        };

        // Construct state response
        let response = GetLoadStatusResponse {
            job_id: job.job_id.clone(),
            path: job.source_path.clone(),
            target_path: job.target_path.clone(),
            state: job.state as i32,
            message: job.message.clone(),
            // Return to main task indicator information
            metrics: Some(main_metrics),
        };

        // Return response
        ctx.response(response)
    }

    /// Cancel the loading task
    ///
    /// Handles the cancellation of a load job by its ID and returns
    /// the result of the cancellation operation.
    pub fn cancel_load_job(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: CancelLoadRequest = ctx.parse_header()?;

        // Get the task ID
        let job_id = req.job_id.clone();
        ctx.set_audit(Some(job_id.clone()), None);

        info!("Received cancel_load_job request for job: {}", job_id);

        // Cancel the task - use block_on to call async method
        let success = self
            .rt
            .block_on(self.load_manager.cancel_job(job_id.clone()))?;

        // Construct the response
        let response = CancelLoadResponse {
            success,
            message: Some(if success {
                format!("Job {} cancelled successfully", job_id)
            } else {
                format!("Failed to cancel job {}", job_id)
            }),
        };

        // Return response
        ctx.response(response)
    }

    /// Handle the task status reported by Worker
    ///
    /// Processes status reports from worker nodes about load tasks,
    /// updating the job status in the load manager.
    pub fn handle_task_report(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: LoadTaskReportRequest = ctx.parse_header()?;

        let job_id = req.job_id.clone();
        let worker_id = req.worker_id;
        let state = req.state;

        // Set up audit information
        ctx.set_audit(Some(job_id.clone()), None);

        // Logging, the detailed level is adjusted according to the report type
        if let Some(ref metrics) = req.metrics {
            let task_id = &metrics.task_id;
            let state_name = LoadState::from_i32(state).unwrap();

            debug!(
                "Received task report: job={}, task={}, worker={}, state={}, progress={}/{} bytes",
                job_id,
                task_id,
                worker_id,
                state_name.as_str_name(),
                metrics.loaded_size.unwrap_or(0),
                metrics.total_size.unwrap_or(0)
            );
        }

        // Process task reports - use block_on to call async method
        match self
            .rt
            .block_on(self.load_manager.handle_task_report(req.clone()))
        {
            Ok(_) => {
                debug!("Successfully processed task report for job: {}", job_id);
            }
            Err(e) => {
                error!("Failed to process task report for job {}: {}", job_id, e);
                return Err(FsError::Common(ErrorImpl::with_source(
                    format!("Failed to handle task report: {}", e).into(),
                )));
            }
        }

        // Construct the response
        let response = LoadTaskReportResponse {
            success: true,
            message: Some(format!(
                "Task report for job {} processed successfully",
                job_id
            )),
        };

        // Return response
        ctx.response(response)
    }
}

impl MessageHandler for MasterLoadService {
    type Error = FsError;

    /// Handle incoming RPC messages by dispatching to the appropriate handler method
    ///
    /// Routes messages based on their RPC code to the corresponding handler method
    /// and records any errors that occur during processing.
    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let code = RpcCode::from(msg.code());

        // Create RpcContext
        let mut rpc_context = RpcContext::new(msg);
        let ctx = &mut rpc_context;

        let response = match code {
            RpcCode::SubmitLoadJob => self.submit_load_job(ctx),
            RpcCode::GetLoadStatus => self.get_load_status(ctx),
            RpcCode::CancelLoadJob => self.cancel_load_job(ctx),
            RpcCode::ReportLoadTask => self.handle_task_report(ctx),
            _ => Err(FsError::Common(ErrorImpl::with_source(
                format!("Unsupported operation: {:?}", code).into(),
            ))),
        };

        // Record the request processing status
        if let Err(ref e) = response {
            warn!("Request {:?} failed: {}", code, e);
        }

        response
    }
}
