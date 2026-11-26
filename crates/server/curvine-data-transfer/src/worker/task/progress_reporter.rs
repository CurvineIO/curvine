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

use crate::worker::task::TaskContext;
use curvine_client_core::file::CurvineFileSystem;
use curvine_error::{FsError, FsResult};
use curvine_model::{TransferTaskResult, WorkerTaskProgress};
use curvine_transfer_client::TransferClient;
use std::sync::Arc;
use std::time::Duration;

const TRANSFER_REPORT_TIMEOUT_MS: u64 = 5_000;

/// Reports progress and an optional result for one task attempt.
///
/// The sink is built once, when the executor is created, so a task with group
/// report endpoints never falls back to the worker's default client.
#[derive(Clone)]
pub struct ProgressReporter {
    task: Arc<TaskContext>,
    client: Option<TransferClient>,
    inline_limit: usize,
}

impl ProgressReporter {
    pub(crate) fn new(
        task: Arc<TaskContext>,
        fs: &CurvineFileSystem,
        default_transfer_client: Option<&TransferClient>,
        inline_limit: usize,
    ) -> FsResult<Self> {
        let client = match task.info.transfer_report.as_ref() {
            Some(report) => {
                let endpoints = if !report.report_endpoints.is_empty() {
                    report.report_endpoints.clone()
                } else if !report.report_target.is_empty() {
                    vec![report.report_target.clone()]
                } else {
                    Vec::new()
                };
                if endpoints.is_empty() {
                    default_transfer_client.cloned()
                } else {
                    Some(
                        TransferClient::with_report_endpoints(&fs.fs_context(), endpoints)
                            .map_err(|_| {
                                FsError::invalid_argument("Invalid transfer report endpoints")
                            })?,
                    )
                }
            }
            // A task without a report target cannot be accounted for by anyone.
            None => None,
        };
        if task.info.transfer_report.is_some() && client.is_none() {
            return Err(FsError::invalid_argument(
                "No transfer report endpoint is configured",
            ));
        }
        Ok(Self {
            task,
            client,
            inline_limit,
        })
    }

    /// A result supplied with progress is retained on this worker attempt before
    /// sending, so a failed final report can be recovered by a worker query.
    /// Plugins whose final report fails must retain their context.
    pub async fn report(
        &self,
        progress: WorkerTaskProgress,
        result: Option<TransferTaskResult>,
    ) -> FsResult<()> {
        let task = &self.task.info;
        let report = task.transfer_report.as_ref().ok_or_else(|| {
            FsError::common(format!(
                "Transfer task {} has no transfer report target",
                task.task_id
            ))
        })?;
        if let Some(result) = &result {
            if matches!(result, TransferTaskResult::Reference { reference, .. } if reference.trim().is_empty())
            {
                return Err(FsError::invalid_argument(
                    "Transfer task result reference must not be blank",
                ));
            }
            if let TransferTaskResult::Inline(bytes) = result {
                if bytes.len() > self.inline_limit {
                    return Err(FsError::common(format!(
                        "inline task result is {} bytes, exceeding worker limit {}",
                        bytes.len(),
                        self.inline_limit,
                    )));
                }
            }
            self.task.publish_result(&progress, result.clone())?;
        }
        let client = self.client.as_ref().ok_or_else(|| {
            FsError::common(format!(
                "Transfer task {} has no transfer report client",
                task.task_id
            ))
        })?;
        let accepted = tokio::time::timeout(
            Duration::from_millis(TRANSFER_REPORT_TIMEOUT_MS),
            client.report_task_with_result(
                &task.job.job_id,
                &task.task_id,
                report,
                progress,
                result,
            ),
        )
        .await
        .map_err(|_| {
            FsError::common(format!(
                "Transfer task report timed out after {} ms: job={}, task={}, attempt={}",
                TRANSFER_REPORT_TIMEOUT_MS, task.job.job_id, task.task_id, report.attempt_id,
            ))
        })??;
        if !accepted {
            return Err(FsError::common(format!(
                "Transfer task report rejected: job={}, task={}, attempt={}",
                task.job.job_id, task.task_id, report.attempt_id,
            )));
        }
        Ok(())
    }
}
