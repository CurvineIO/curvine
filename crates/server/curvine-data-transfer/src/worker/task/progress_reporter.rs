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
use curvine_job_client::{JobMasterClient, TransferClient};
use curvine_model::{JobTaskProgress, TransferTaskResult};
use log::warn;
use std::sync::Arc;
use std::time::Duration;

const TRANSFER_REPORT_TIMEOUT_MS: u64 = 5_000;

#[derive(Clone)]
enum ReportSink {
    Transfer(Option<TransferClient>),
    Legacy(JobMasterClient),
}

/// Reports progress to the appropriate sink for this task attempt.
#[derive(Clone)]
pub struct ProgressReporter {
    task: Arc<TaskContext>,
    sink: ReportSink,
    inline_limit: usize,
}

impl ProgressReporter {
    pub(crate) fn new(
        task: Arc<TaskContext>,
        fs: &CurvineFileSystem,
        default_transfer_client: Option<&TransferClient>,
        inline_limit: usize,
    ) -> Self {
        let sink = match &task.info.transfer_report {
            Some(report) => {
                let endpoints = if !report.report_endpoints.is_empty() {
                    report.report_endpoints.clone()
                } else if !report.report_target.is_empty() {
                    vec![report.report_target.clone()]
                } else {
                    Vec::new()
                };
                let client = if endpoints.is_empty() {
                    default_transfer_client.cloned()
                } else {
                    match TransferClient::with_report_endpoints(&fs.fs_context(), endpoints) {
                        Ok(client) => Some(client),
                        Err(err) => {
                            warn!(
                                "transfer task has invalid report endpoints: job={} run={} task={} attempt={} err={}",
                                task.info.job.job_id, report.run_id, task.info.task_id, report.attempt_id, err,
                            );
                            None
                        }
                    }
                };
                ReportSink::Transfer(client)
            }
            None => ReportSink::Legacy(JobMasterClient::new(fs.fs_client())),
        };
        Self {
            task,
            sink,
            inline_limit,
        }
    }

    /// A result supplied with progress is retained on this worker attempt before
    /// sending, so a failed final report can be recovered by a worker query.
    /// Plugins whose final report fails must keep their context (`should_remove`).
    pub async fn report(
        &self,
        progress: JobTaskProgress,
        result: Option<TransferTaskResult>,
    ) -> FsResult<()> {
        let task = &self.task.info;
        match &self.sink {
            ReportSink::Transfer(client) => {
                let report = task
                    .transfer_report
                    .as_ref()
                    .expect("transfer report identity");
                if let Some(result) = &result {
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
                let client = client.as_ref().ok_or_else(|| {
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
                        TRANSFER_REPORT_TIMEOUT_MS,
                        task.job.job_id,
                        task.task_id,
                        report.attempt_id,
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
            ReportSink::Legacy(client) => {
                if result.is_some() {
                    return Err(FsError::common(
                        "legacy task reports do not support results",
                    ));
                }
                client
                    .report_task(&task.job.job_id, &task.task_id, progress)
                    .await
            }
        }
    }
}
