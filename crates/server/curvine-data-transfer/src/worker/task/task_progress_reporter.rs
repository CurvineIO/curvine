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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use curvine_error::{FsError, FsResult};
use curvine_job_client::{TransferClient, TransferTaskProgressReport};
use curvine_model::{JobTaskProgress, TransferTaskReportInfo};
use curvine_runtime::runtime::{JoinHandle, RpcRuntime, Runtime};
use log::debug;
use tokio::sync::mpsc;

const TRANSFER_REPORT_TIMEOUT_MS: u64 = 5_000;
const MIN_PROGRESS_REPORT_QUEUE_CAPACITY: usize = 1_024;
const MAX_PROGRESS_REPORT_QUEUE_CAPACITY: usize = 10_000;
const PROGRESS_REPORT_BATCH_SIZE: usize = 256;
const MAX_PROGRESS_REPORT_BATCH_DELAY: Duration = Duration::from_millis(25);

pub(super) struct ProgressReport {
    pub(super) report: TransferTaskProgressReport,
    pub(super) log_context: String,
}

#[derive(Hash, Eq, PartialEq)]
pub(super) struct ProgressReportKey {
    job_id: String,
    run_id: u64,
    task_id: String,
    attempt_id: u64,
    worker_id: u32,
    worker_session_id: String,
}

impl ProgressReportKey {
    fn new(report: &ProgressReport) -> Self {
        Self {
            job_id: report.report.job_id.clone(),
            run_id: report.report.info.run_id,
            task_id: report.report.task_id.clone(),
            attempt_id: report.report.info.attempt_id,
            worker_id: report.report.info.worker_id,
            worker_session_id: report.report.info.worker_session_id.clone(),
        }
    }
}

pub(super) fn insert_latest_progress(
    reports: &mut HashMap<ProgressReportKey, ProgressReport>,
    report: ProgressReport,
) {
    reports.insert(ProgressReportKey::new(&report), report);
}

pub(super) fn progress_report_batch_delay(report_interval: Duration) -> Duration {
    // Transfer derives its report interval below the stale timeout, so this
    // keeps batching from consuming an entire heartbeat safety margin.
    report_interval.min(MAX_PROGRESS_REPORT_BATCH_DELAY)
}

pub(super) fn progress_report_queue_capacity(worker_task_concurrency: usize) -> usize {
    worker_task_concurrency.clamp(
        MIN_PROGRESS_REPORT_QUEUE_CAPACITY,
        MAX_PROGRESS_REPORT_QUEUE_CAPACITY,
    )
}

/// Shared Worker-side progress batcher for one ordered Transfer endpoint set.
///
/// It only handles running snapshots. Terminal state changes keep their
/// synchronous single-report acknowledgement path.
pub(crate) struct TransferProgressBatcher {
    sender: mpsc::Sender<ProgressReport>,
    transfer_client: TransferClient,
    join_handle: JoinHandle<()>,
}

impl TransferProgressBatcher {
    pub(crate) fn new(
        rt: Arc<Runtime>,
        transfer_client: TransferClient,
        worker_task_concurrency: usize,
        progress_report_interval: Duration,
    ) -> Self {
        let queue_capacity = progress_report_queue_capacity(worker_task_concurrency);
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let join_handle = rt.spawn(Self::run(
            receiver,
            transfer_client.clone(),
            progress_report_batch_delay(progress_report_interval),
        ));
        Self {
            sender,
            transfer_client,
            join_handle,
        }
    }

    fn enqueue_progress(&self, report: ProgressReport) {
        if self.sender.try_send(report).is_err() {
            debug!("transfer progress batch queue full, dropping update");
        }
    }

    async fn report_terminal(
        &self,
        job_id: &str,
        task_id: &str,
        report_info: &TransferTaskReportInfo,
        progress: JobTaskProgress,
        log_context: &str,
    ) -> FsResult<()> {
        send_report(
            &self.transfer_client,
            job_id,
            task_id,
            report_info,
            progress,
            log_context,
        )
        .await
    }

    async fn run(
        mut receiver: mpsc::Receiver<ProgressReport>,
        transfer_client: TransferClient,
        batch_delay: Duration,
    ) {
        while let Some(first) = receiver.recv().await {
            let mut reports = HashMap::new();
            insert_latest_progress(&mut reports, first);
            let deadline = tokio::time::Instant::now() + batch_delay;

            for _ in 1..PROGRESS_REPORT_BATCH_SIZE {
                match tokio::time::timeout_at(deadline, receiver.recv()).await {
                    Ok(Some(report)) => insert_latest_progress(&mut reports, report),
                    Ok(None) | Err(_) => break,
                }
            }

            let reports = reports.into_values().collect::<Vec<_>>();
            let batch = reports.iter().map(|report| report.report.clone()).collect();
            let result = tokio::time::timeout(
                Duration::from_millis(TRANSFER_REPORT_TIMEOUT_MS),
                transfer_client.report_task_batch(batch),
            )
            .await;

            match result {
                Ok(Ok(accepted)) => {
                    for (report, accepted) in reports.into_iter().zip(accepted) {
                        if !accepted {
                            debug!(
                                "transfer task progress batch report rejected: {}",
                                report.log_context
                            );
                        }
                    }
                }
                Ok(Err(err)) => {
                    debug!(
                        "transfer task progress batch report failed: reports={} err={}",
                        reports.len(),
                        err
                    );
                }
                Err(_) => {
                    debug!(
                        "transfer task progress batch report timed out after {} ms: reports={}",
                        TRANSFER_REPORT_TIMEOUT_MS,
                        reports.len()
                    );
                }
            }
        }
    }
}

impl Drop for TransferProgressBatcher {
    fn drop(&mut self) {
        self.join_handle.abort();
    }
}

/// Per-task facade over a Worker-shared progress batcher.
pub struct TaskProgressReporter {
    batcher: Arc<TransferProgressBatcher>,
    job_id: String,
    task_id: String,
    report_info: TransferTaskReportInfo,
    log_context: String,
}

impl TaskProgressReporter {
    pub(crate) fn new(
        batcher: Arc<TransferProgressBatcher>,
        job_id: String,
        task_id: String,
        report_info: TransferTaskReportInfo,
        log_context: String,
    ) -> Self {
        Self {
            batcher,
            job_id,
            task_id,
            report_info,
            log_context,
        }
    }

    pub fn enqueue_progress(&self, progress: JobTaskProgress) {
        self.batcher.enqueue_progress(ProgressReport {
            report: TransferTaskProgressReport {
                job_id: self.job_id.clone(),
                task_id: self.task_id.clone(),
                info: self.report_info.clone(),
                progress,
            },
            log_context: self.log_context.clone(),
        });
    }

    pub async fn report_terminal(&self, progress: JobTaskProgress) -> FsResult<()> {
        self.batcher
            .report_terminal(
                &self.job_id,
                &self.task_id,
                &self.report_info,
                progress,
                &self.log_context,
            )
            .await
    }
}

async fn send_report(
    transfer_client: &TransferClient,
    job_id: &str,
    task_id: &str,
    report_info: &TransferTaskReportInfo,
    progress: JobTaskProgress,
    log_context: &str,
) -> FsResult<()> {
    let accepted = tokio::time::timeout(
        Duration::from_millis(TRANSFER_REPORT_TIMEOUT_MS),
        transfer_client.report_task(job_id, task_id, report_info, progress),
    )
    .await
    .map_err(|_| {
        FsError::common(format!(
            "Transfer task report timed out after {} ms: {}",
            TRANSFER_REPORT_TIMEOUT_MS, log_context
        ))
    })??;
    if !accepted {
        return Err(FsError::common(format!(
            "Transfer task report rejected: {}",
            log_context
        )));
    }
    Ok(())
}
