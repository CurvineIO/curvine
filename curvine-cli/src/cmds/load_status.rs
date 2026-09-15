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

use clap::Parser;
use curvine_core_error::CommonResult;
use curvine_job_client::{JobListOptions, JobMasterClient, TransferClient};
use curvine_model::{JobStatus, JobTaskState, LoadTaskStatus, TransferState};

use crate::util::*;

#[derive(Parser, Debug)]
pub struct LoadStatusCommand {
    job_id: Option<String>,

    /// Query the newest job whose source or target path matches this prefix.
    #[arg(long)]
    path: Option<String>,

    #[arg(long, short = 'v')]
    verbose: bool,

    /// When verbose is enabled, only print failed task details.
    #[arg(long, alias = "failed_only")]
    failed_only: bool,

    #[arg(long, short = 'w', default_value = "5s")]
    watch: Option<String>,
}

impl LoadStatusCommand {
    pub fn new(job_id: String, verbose: bool, watch: String) -> Self {
        Self {
            job_id: Some(job_id),
            path: None,
            verbose,
            failed_only: false,
            watch: Some(watch),
        }
    }

    pub fn is_legacy_path_query(&self) -> bool {
        self.job_id.is_none() && self.path.is_some()
    }

    pub async fn execute(&self, client: JobMasterClient) -> CommonResult<()> {
        if self.job_id.is_none() && self.path.is_none() {
            eprintln!("Error: provide a job_id or --path <cv-or-ufs-path>");
            std::process::exit(1);
        }

        if let Some(watch_interval) = &self.watch {
            self.watch_status(client, watch_interval).await
        } else {
            let status = self.fetch_status(&client).await;
            print_job_status(&status, self.verbose, self.failed_only);
            Ok(())
        }
    }

    async fn fetch_status(&self, client: &JobMasterClient) -> JobStatus {
        if let Some(job_id) = &self.job_id {
            if self.verbose {
                handle_rpc_result(client.get_job_status_verbose(job_id)).await
            } else {
                handle_rpc_result(client.get_job_status(job_id)).await
            }
        } else {
            let path = self.path.as_deref().expect("path checked by execute");
            let mut jobs =
                handle_rpc_result(client.list_job_statuses_with_options(JobListOptions {
                    path_prefix: Some(path.to_string()),
                    limit: 1,
                    include_finished: true,
                    include_tasks: self.verbose,
                    failed_only: self.failed_only,
                    ..Default::default()
                }))
                .await;
            if jobs.is_empty() {
                eprintln!("Error: no load job found for path prefix {}", path);
                std::process::exit(1);
            }
            jobs.remove(0)
        }
    }

    /// keep watch job status
    ///
    /// # Arguments
    /// * `client` - LoadClient Instance
    /// * `interval_str` - Example: 5s, 1m
    async fn watch_status(&self, client: JobMasterClient, interval_str: &str) -> CommonResult<()> {
        // Resolution refresh interval
        let duration = parse_duration(interval_str).unwrap_or_else(|_| {
            eprintln!("Error: Invalid watch interval format: {}", interval_str);
            eprintln!("    Supported formats: <number>s (seconds), <number>m (minutes)");
            eprintln!("    Example: 5s, 1m");
            std::process::exit(1);
        });

        println!(
            "Watching job status (refresh every {}). Press Ctrl+C to stop.",
            format_duration(&duration)
        );

        loop {
            if cfg!(target_os = "windows") {
                let _ = std::process::Command::new("cmd")
                    .args(["/c", "cls"])
                    .status();
            } else {
                print!("\x1B[2J\x1B[1;1H");
            }

            let target = self
                .job_id
                .as_deref()
                .or(self.path.as_deref())
                .unwrap_or("<unknown>");
            println!(
                "\n Checking status for {} (refreshing every {})",
                target,
                format_duration(&duration)
            );
            println!("Press Ctrl+C to stop watching.");

            let status = self.fetch_status(&client).await;
            print_job_status(&status, self.verbose, self.failed_only);

            if status.state.is_finish() {
                break;
            }

            tokio::time::sleep(duration).await;
        }

        Ok(())
    }

    pub async fn execute_transfer_only(&self, transfer_client: TransferClient) -> CommonResult<()> {
        let Some(job_id) = self.job_id.as_deref() else {
            eprintln!("Error: transfer status requires a job_id");
            std::process::exit(1);
        };

        println!("\n Checking status for {}", job_id);

        if let Some(watch_interval) = &self.watch {
            self.watch_transfer_status(transfer_client, watch_interval, job_id)
                .await
        } else {
            print_transfer_status(&transfer_client, job_id).await?;
            Ok(())
        }
    }

    async fn watch_transfer_status(
        &self,
        transfer_client: TransferClient,
        interval_str: &str,
        job_id: &str,
    ) -> CommonResult<()> {
        let duration = parse_duration(interval_str).unwrap_or_else(|_| {
            eprintln!("Error: Invalid watch interval format: {}", interval_str);
            eprintln!("    Supported formats: <number>s (seconds), <number>m (minutes)");
            eprintln!("    Example: 5s, 1m");
            std::process::exit(1);
        });

        println!(
            "Watching transfer status (refresh every {}). Press Ctrl+C to stop.",
            format_duration(&duration)
        );

        loop {
            if cfg!(target_os = "windows") {
                let _ = std::process::Command::new("cmd")
                    .args(["/c", "cls"])
                    .status();
            } else {
                print!("\x1B[2J\x1B[1;1H");
            }

            println!(
                "\n Checking status for {} (refreshing every {})",
                job_id,
                format_duration(&duration)
            );
            println!("Press Ctrl+C to stop watching.");

            let state = print_transfer_status(&transfer_client, job_id).await?;
            if state.is_terminal() {
                break;
            }

            tokio::time::sleep(duration).await;
        }

        Ok(())
    }
}

pub(crate) fn parse_job_state(value: &str) -> Option<JobTaskState> {
    match value.to_ascii_lowercase().as_str() {
        "pending" => Some(JobTaskState::Pending),
        "loading" | "running" => Some(JobTaskState::Loading),
        "completed" | "complete" | "done" => Some(JobTaskState::Completed),
        "failed" | "fail" => Some(JobTaskState::Failed),
        "canceled" | "cancelled" | "cancel" => Some(JobTaskState::Canceled),
        "unknown" => Some(JobTaskState::UNKNOWN),
        _ => None,
    }
}

pub(crate) fn print_job_table(jobs: &[JobStatus]) {
    if jobs.is_empty() {
        println!("No load jobs found.");
        return;
    }
    println!(
        "{:<34} {:<12} {:<10} {:>6} {:>6} {:>6} {:>6}  {:<40} -> TARGET",
        "JOB_ID", "SOURCE_TYPE", "STATE", "TOTAL", "DONE", "FAIL", "RUN", "SOURCE"
    );
    for job in jobs {
        println!(
            "{:<34} {:<12} {:<10} {:>6} {:>6} {:>6} {:>6}  {:<40} -> {}",
            job.job_id,
            job.source_type.as_str(),
            format!("{:?}", job.state),
            job.total_files,
            job.completed_files,
            job.failed_files,
            job.running_files,
            truncate(&job.source_path, 40),
            job.target_path
        );
    }
}

pub(crate) fn print_job_status(status: &JobStatus, verbose: bool, failed_only: bool) {
    println!("{}", status);
    println!("Source: {}", status.source_type.as_str());
    println!(
        "Files: total={}, completed={}, failed={}, pending={}, loading={}, running={}",
        status.total_files,
        status.completed_files,
        status.failed_files,
        status.pending_files,
        status.loading_files,
        status.running_files
    );
    if verbose {
        print_task_table(&status.tasks, failed_only);
    }
}

pub(crate) fn print_task_table(tasks: &[LoadTaskStatus], failed_only: bool) {
    // When filtering to failures but no task actually failed (e.g. a job that
    // failed at dispatch with tasks left Pending), fall back to showing all
    // tasks so the failure context is not hidden behind "No task details found".
    let has_failed = tasks.iter().any(|task| task.state == JobTaskState::Failed);
    let rows = tasks
        .iter()
        .filter(|task| !failed_only || !has_failed || task.state == JobTaskState::Failed)
        .collect::<Vec<_>>();
    if rows.is_empty() {
        println!("No task details found.");
        return;
    }
    println!(
        "{:<34} {:<10} {:<24} {:<36} {:<36} MESSAGE",
        "TASK_ID", "STATE", "WORKER", "SOURCE", "TARGET"
    );
    for task in rows {
        println!(
            "{:<34} {:<10} {:<24} {:<36} {:<36} {}",
            task.task_id,
            format!("{:?}", task.state),
            truncate(&task.worker, 24),
            task.source_path,
            task.target_path,
            task.progress.message
        );
    }
}

async fn print_transfer_status(
    client: &TransferClient,
    job_id: &str,
) -> CommonResult<TransferState> {
    let status = handle_rpc_result(client.status(job_id)).await;
    let state = TransferState::from(status.state);
    println!("Job ID: {}", status.job_id);
    println!("Run ID: {}", status.run_id);
    println!("State: {:?}", state);
    println!(
        "Progress: {} / {}, message={}",
        bytes_to_string(status.progress.loaded_size.max(0)),
        bytes_to_string(status.progress.total_size.max(0)),
        status.progress.message
    );
    if let Some(summary) = status.task_summary {
        println!(
            "Files: {} completed, {} failed, {} running, {} pending; {} transferred",
            summary.completed,
            summary.failed,
            summary.running,
            summary.pending,
            bytes_to_string(summary.completed_size.max(0)),
        );
    }
    Ok(state)
}

fn truncate(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_string();
    }
    let keep = max.saturating_sub(1);
    format!("{}…", value.chars().take(keep).collect::<String>())
}
