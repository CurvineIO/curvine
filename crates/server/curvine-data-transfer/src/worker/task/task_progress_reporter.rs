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

use curvine_error::{FsError, FsResult};
use curvine_job_client::TransferClient;
use curvine_model::{JobTaskProgress, TransferTaskReportInfo};
use curvine_runtime::runtime::{JoinHandle, RpcRuntime, Runtime};
use log::debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

const TRANSFER_REPORT_TIMEOUT_MS: u64 = 5_000;
const REPORT_QUEUE_CAPACITY: usize = 64;

struct ReportMessage {
    progress: JobTaskProgress,
    reply: Option<oneshot::Sender<FsResult<()>>>,
}

/// Background transfer progress reporter that keeps the copy hot path non-blocking.
///
/// Progress updates for the same task are coalesced so only the latest snapshot is sent.
pub struct TaskProgressReporter {
    sender: mpsc::Sender<ReportMessage>,
    join_handle: JoinHandle<()>,
}

impl TaskProgressReporter {
    pub fn new(
        rt: Arc<Runtime>,
        transfer_client: TransferClient,
        job_id: String,
        task_id: String,
        report_info: TransferTaskReportInfo,
        log_context: String,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(REPORT_QUEUE_CAPACITY);
        let join_handle = rt.spawn(Self::run(
            receiver,
            transfer_client,
            job_id,
            task_id,
            report_info,
            log_context,
        ));
        Self {
            sender,
            join_handle,
        }
    }

    pub fn enqueue_progress(&self, progress: JobTaskProgress) {
        let message = ReportMessage {
            progress,
            reply: None,
        };
        if self.sender.try_send(message).is_err() {
            debug!("transfer task progress report queue full, dropping update");
        }
    }

    pub async fn report_terminal(&self, progress: JobTaskProgress) -> FsResult<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let message = ReportMessage {
            progress,
            reply: Some(reply_tx),
        };
        self.sender
            .send(message)
            .await
            .map_err(|_| FsError::common("Transfer task progress reporter stopped"))?;
        reply_rx
            .await
            .map_err(|_| FsError::common("Transfer task progress reporter stopped"))?
    }

    async fn run(
        mut receiver: mpsc::Receiver<ReportMessage>,
        transfer_client: TransferClient,
        job_id: String,
        task_id: String,
        report_info: TransferTaskReportInfo,
        log_context: String,
    ) {
        while let Some(message) = receiver.recv().await {
            let mut latest = message.progress;
            let mut terminal_reply = message.reply;

            while let Ok(next) = receiver.try_recv() {
                if next.reply.is_some() {
                    latest = next.progress;
                    terminal_reply = next.reply;
                } else if terminal_reply.is_none() {
                    latest = next.progress;
                }
            }

            let result = send_report(
                &transfer_client,
                &job_id,
                &task_id,
                &report_info,
                latest,
                &log_context,
            )
            .await;

            if let Some(reply) = terminal_reply {
                let _ = reply.send(result);
            } else if let Err(err) = result {
                debug!(
                    "transfer task async progress report failed: {} err={}",
                    log_context, err
                );
            }
        }
    }
}

impl Drop for TaskProgressReporter {
    fn drop(&mut self) {
        self.join_handle.abort();
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
