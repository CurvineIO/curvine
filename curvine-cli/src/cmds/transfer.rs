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

use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};
use comfy_table::{presets::ASCII_MARKDOWN, Table};
use curvine_job_client::TransferClient;
use curvine_model::{TransferKind, TransferState};
use curvine_proto::{
    CancelTransferResponse, GetTransferStatusResponse, ListTransferTenantsResponse,
    ListTransfersResponse, SubmitTransferResponse, TransferJobStatusProto, TransferTaskStatusProto,
    TransferTaskSummaryProto, TransferTenantSummaryProto,
};
use curvine_core_error::{err_box, CommonResult};

use crate::util::{bytes_to_string, handle_rpc_result, parse_duration};

#[derive(Parser, Debug)]
pub struct TransferCommand {
    #[command(subcommand)]
    command: TransferSubCommand,
}

#[derive(Subcommand, Debug)]
enum TransferSubCommand {
    /// List transfer jobs
    List(TransferListCommand),

    /// Show one transfer job; add --verbose for task details
    Status(TransferStatusCommand),

    /// Show task page for one transfer job
    Tasks(TransferTasksCommand),

    /// Cancel one transfer job
    Cancel(TransferCancelCommand),

    /// Rerun a completed, failed, canceled, or partially successful transfer as a new job
    Retry(TransferRetryCommand),

    /// Summarize transfer jobs by tenant
    Tenants(TransferTenantsCommand),
}

#[derive(Parser, Debug)]
pub struct TransferListCommand {
    #[arg(long)]
    kind: Option<TransferKindArg>,

    #[arg(long)]
    state: Option<TransferStateArg>,

    #[arg(long)]
    submitter: Option<String>,

    #[arg(long)]
    tenant: Option<String>,

    #[arg(long, default_value_t = 20)]
    limit: u32,

    #[arg(long)]
    page_token: Option<String>,

    #[arg(long)]
    all: bool,

    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    format: OutputFormat,

    #[arg(long)]
    full_id: bool,

    #[arg(long, short = 'v')]
    verbose: bool,
}

#[derive(Parser, Debug)]
pub struct TransferStatusCommand {
    job_id: String,

    #[arg(long, short = 'v')]
    verbose: bool,

    #[arg(long, short = 'w')]
    watch: bool,

    #[arg(long, default_value = "1s")]
    interval: String,

    #[arg(long, default_value_t = 20)]
    limit: u32,

    #[arg(long)]
    page_token: Option<String>,

    #[arg(long)]
    all: bool,

    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    format: OutputFormat,

    #[arg(long)]
    full_id: bool,
}

#[derive(Parser, Debug)]
pub struct TransferTasksCommand {
    job_id: String,

    #[arg(long, short = 'v')]
    verbose: bool,

    #[arg(long, default_value_t = 50)]
    limit: u32,

    #[arg(long)]
    page_token: Option<String>,

    #[arg(long)]
    all: bool,

    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    format: OutputFormat,

    #[arg(long)]
    full_id: bool,
}

#[derive(Parser, Debug)]
pub struct TransferCancelCommand {
    job_id: String,

    #[arg(long)]
    run_id: Option<u64>,

    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    format: OutputFormat,
}

#[derive(Parser, Debug)]
pub struct TransferRetryCommand {
    job_id: String,

    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    format: OutputFormat,
}

#[derive(Parser, Debug)]
pub struct TransferTenantsCommand {
    #[arg(long, default_value_t = 20)]
    limit: u32,

    #[arg(long)]
    page_token: Option<String>,

    #[arg(long)]
    all: bool,

    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    format: OutputFormat,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum TransferKindArg {
    Load,
    Export,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum TransferStateArg {
    Pending,
    Planning,
    Dispatching,
    Running,
    Canceling,
    Completed,
    Failed,
    Canceled,
    PartialSuccess,
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
pub enum OutputFormat {
    Table,
    Json,
}

impl TransferCommand {
    pub async fn execute(&self, client: TransferClient) -> CommonResult<()> {
        match &self.command {
            TransferSubCommand::List(cmd) => cmd.execute(client).await,
            TransferSubCommand::Status(cmd) => cmd.execute(client).await,
            TransferSubCommand::Tasks(cmd) => cmd.execute(client).await,
            TransferSubCommand::Cancel(cmd) => cmd.execute(client).await,
            TransferSubCommand::Retry(cmd) => cmd.execute(client).await,
            TransferSubCommand::Tenants(cmd) => cmd.execute(client).await,
        }
    }
}

impl TransferListCommand {
    async fn execute(&self, client: TransferClient) -> CommonResult<()> {
        let response = list_transfer_pages(
            &client,
            self.kind.map(TransferKind::from),
            self.state.map(TransferState::from),
            self.submitter.clone(),
            self.tenant.clone(),
            self.limit,
            self.page_token.clone(),
            self.all,
        )
        .await?;
        if self.format == OutputFormat::Json {
            println!("{}", serde_json::to_string_pretty(&response)?);
            return Ok(());
        }
        print_jobs_table(&response.jobs, self.full_id, self.verbose);
        print_next_page(response.next_page_token.as_deref());
        Ok(())
    }
}

impl TransferStatusCommand {
    async fn execute(&self, client: TransferClient) -> CommonResult<()> {
        if self.watch && self.format == OutputFormat::Json {
            return err_box!("--watch only supports --format table");
        }
        let interval = self
            .watch
            .then(|| parse_watch_interval(&self.interval))
            .transpose()?;
        let include_tasks = self.verbose || self.format == OutputFormat::Json;
        let mut first = true;
        loop {
            let response = status_transfer_pages(
                &client,
                &self.job_id,
                if include_tasks { self.limit } else { 0 },
                include_tasks.then(|| self.page_token.clone()).flatten(),
                include_tasks && self.all,
            )
            .await?;
            if self.format == OutputFormat::Json {
                println!("{}", serde_json::to_string_pretty(&response)?);
                return Ok(());
            }
            if !first {
                println!();
            }
            first = false;
            let job = status_to_job(&response);
            print_status_summary(&job, response.task_summary.as_ref(), self.verbose);
            if self.verbose {
                print_tasks_table(&response.tasks, self.full_id, true);
                print_next_page(response.next_page_token.as_deref());
            }
            if !self.watch || transfer_is_terminal(response.state) {
                return Ok(());
            }
            if let Some(interval) = interval {
                tokio::time::sleep(interval).await;
            }
        }
    }
}

impl TransferTasksCommand {
    async fn execute(&self, client: TransferClient) -> CommonResult<()> {
        let response = status_transfer_pages(
            &client,
            &self.job_id,
            self.limit,
            self.page_token.clone(),
            self.all,
        )
        .await?;
        if self.format == OutputFormat::Json {
            println!("{}", serde_json::to_string_pretty(&response.tasks)?);
            return Ok(());
        }
        print_tasks_table(&response.tasks, self.full_id, self.verbose);
        print_next_page(response.next_page_token.as_deref());
        Ok(())
    }
}

impl TransferCancelCommand {
    async fn execute(&self, client: TransferClient) -> CommonResult<()> {
        let response = handle_rpc_result(client.cancel(&self.job_id, self.run_id)).await;
        println!("{}", render_cancel_response(&response, self.format)?);
        Ok(())
    }
}

impl TransferRetryCommand {
    async fn execute(&self, client: TransferClient) -> CommonResult<()> {
        let response = handle_rpc_result(client.retry(&self.job_id)).await;
        println!(
            "{}",
            render_retry_response(&self.job_id, &response, self.format)?
        );
        Ok(())
    }
}

impl TransferTenantsCommand {
    async fn execute(&self, client: TransferClient) -> CommonResult<()> {
        let response =
            list_tenant_pages(&client, self.limit, self.page_token.clone(), self.all).await?;
        if self.format == OutputFormat::Json {
            println!("{}", serde_json::to_string_pretty(&response)?);
            return Ok(());
        }
        print_tenants_table(&response.tenants);
        print_next_page(response.next_page_token.as_deref());
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn list_transfer_pages(
    client: &TransferClient,
    kind: Option<TransferKind>,
    state: Option<TransferState>,
    submitter: Option<String>,
    tenant: Option<String>,
    limit: u32,
    page_token: Option<String>,
    all: bool,
) -> CommonResult<ListTransfersResponse> {
    validate_all_page_limit(limit, all)?;
    let mut response = handle_rpc_result(client.list(
        kind,
        state,
        submitter.clone(),
        tenant.clone(),
        Some(limit),
        page_token.clone(),
    ))
    .await;
    if !all {
        return Ok(response);
    }

    let mut previous_token = page_token;
    while let Some(next_token) = response.next_page_token.clone() {
        ensure_page_advanced(previous_token.as_deref(), &next_token)?;
        let mut next = handle_rpc_result(client.list(
            kind,
            state,
            submitter.clone(),
            tenant.clone(),
            Some(limit),
            Some(next_token.clone()),
        ))
        .await;
        response.jobs.append(&mut next.jobs);
        previous_token = Some(next_token);
        response.next_page_token = next.next_page_token;
    }
    Ok(response)
}

async fn status_transfer_pages(
    client: &TransferClient,
    job_id: &str,
    limit: u32,
    page_token: Option<String>,
    all: bool,
) -> CommonResult<GetTransferStatusResponse> {
    validate_all_page_limit(limit, all)?;
    let mut response =
        handle_rpc_result(client.status_page(job_id, Some(limit), page_token.clone())).await;
    if !all {
        return Ok(response);
    }

    let mut previous_token = page_token;
    while let Some(next_token) = response.next_page_token.clone() {
        ensure_page_advanced(previous_token.as_deref(), &next_token)?;
        let mut next =
            handle_rpc_result(client.status_page(job_id, Some(limit), Some(next_token.clone())))
                .await;
        response.tasks.append(&mut next.tasks);
        previous_token = Some(next_token);
        response.next_page_token = next.next_page_token;
    }
    Ok(response)
}

async fn list_tenant_pages(
    client: &TransferClient,
    limit: u32,
    page_token: Option<String>,
    all: bool,
) -> CommonResult<ListTransferTenantsResponse> {
    validate_all_page_limit(limit, all)?;
    let mut response =
        handle_rpc_result(client.list_tenants(Some(limit), page_token.clone())).await;
    if !all {
        return Ok(response);
    }

    let mut previous_token = page_token;
    while let Some(next_token) = response.next_page_token.clone() {
        ensure_page_advanced(previous_token.as_deref(), &next_token)?;
        let mut next =
            handle_rpc_result(client.list_tenants(Some(limit), Some(next_token.clone()))).await;
        response.tenants.append(&mut next.tenants);
        previous_token = Some(next_token);
        response.next_page_token = next.next_page_token;
    }
    Ok(response)
}

fn validate_all_page_limit(limit: u32, all: bool) -> CommonResult<()> {
    if all && limit == 0 {
        return err_box!("--all requires --limit to be greater than 0");
    }
    Ok(())
}

fn ensure_page_advanced(previous: Option<&str>, next: &str) -> CommonResult<()> {
    if previous == Some(next) {
        return err_box!("Transfer service returned the same next_page_token: {next}");
    }
    Ok(())
}

fn parse_watch_interval(value: &str) -> CommonResult<Duration> {
    let interval = match parse_duration(value) {
        Ok(interval) => interval,
        Err(err) => return err_box!("Invalid --interval {}: {}", value, err),
    };
    if interval.is_zero() {
        return err_box!("--interval must be greater than zero");
    }
    Ok(interval)
}

fn status_to_job(response: &GetTransferStatusResponse) -> TransferJobStatusProto {
    TransferJobStatusProto {
        job_id: response.job_id.clone(),
        run_id: response.run_id,
        kind: response.kind.unwrap_or_default(),
        state: response.state,
        source_path: response.source_path.clone().unwrap_or_default(),
        target_path: response.target_path.clone().unwrap_or_default(),
        progress: response.progress.clone(),
        submitter: response.submitter.clone().unwrap_or_default(),
        tenant: response.tenant.clone().unwrap_or_default(),
        created_at: response.created_at.unwrap_or_default(),
        updated_at: response.updated_at.unwrap_or_default(),
        owner: response.owner.clone(),
        lease_epoch: response.lease_epoch,
        lease_expire_at: response.lease_expire_at,
        cv_metadata_epoch: response.cv_metadata_epoch,
    }
}

pub fn render_cancel_response(
    response: &CancelTransferResponse,
    format: OutputFormat,
) -> CommonResult<String> {
    if format == OutputFormat::Json {
        return Ok(serde_json::to_string_pretty(response)?);
    }
    let mut table = md_table();
    table.set_header(["JOB ID", "STATE"]);
    table.add_row([
        response.job_id.clone(),
        state_name(response.state).to_string(),
    ]);
    Ok(table.to_string())
}

pub fn render_retry_response(
    original_job_id: &str,
    response: &SubmitTransferResponse,
    format: OutputFormat,
) -> CommonResult<String> {
    if format == OutputFormat::Json {
        return Ok(serde_json::to_string_pretty(response)?);
    }
    let mut table = md_table();
    table.set_header(["RETRY OF", "NEW JOB ID", "STATE"]);
    table.add_row([
        short_id(original_job_id),
        response.job_id.clone(),
        state_name(response.state).to_string(),
    ]);
    Ok(table.to_string())
}

impl From<TransferKindArg> for TransferKind {
    fn from(value: TransferKindArg) -> Self {
        match value {
            TransferKindArg::Load => TransferKind::Load,
            TransferKindArg::Export => TransferKind::Export,
        }
    }
}

impl From<TransferStateArg> for TransferState {
    fn from(value: TransferStateArg) -> Self {
        match value {
            TransferStateArg::Pending => TransferState::Pending,
            TransferStateArg::Planning => TransferState::Planning,
            TransferStateArg::Dispatching => TransferState::Dispatching,
            TransferStateArg::Running => TransferState::Running,
            TransferStateArg::Canceling => TransferState::Canceling,
            TransferStateArg::Completed => TransferState::Completed,
            TransferStateArg::Failed => TransferState::Failed,
            TransferStateArg::Canceled => TransferState::Canceled,
            TransferStateArg::PartialSuccess => TransferState::PartialSuccess,
        }
    }
}

fn print_jobs_table(jobs: &[TransferJobStatusProto], full_id: bool, verbose: bool) {
    let mut table = md_table();
    if verbose {
        table.set_header([
            "JOB ID", "KIND", "STATE", "PROGRESS", "OWNER", "CV EPOCH", "SOURCE", "TARGET",
            "UPDATED", "MESSAGE",
        ]);
    } else {
        table.set_header([
            "JOB ID", "KIND", "STATE", "PROGRESS", "SOURCE", "TARGET", "UPDATED",
        ]);
    }
    for job in jobs {
        let mut row = vec![
            display_id(&job.job_id, full_id),
            kind_name(job.kind).to_string(),
            state_name(job.state).to_string(),
            progress_text(job.progress.loaded_size, job.progress.total_size),
        ];
        if verbose {
            row.push(owner_text(job.owner.as_deref()));
            row.push(
                job.cv_metadata_epoch
                    .map(|epoch| epoch.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            );
        }
        row.push(truncate_middle(
            &job.source_path,
            if verbose { 36 } else { 28 },
        ));
        row.push(truncate_middle(
            &job.target_path,
            if verbose { 36 } else { 28 },
        ));
        row.push(format_timestamp(job.updated_at));
        if verbose {
            row.push(truncate_middle(&job.progress.message, 32));
        }
        table.add_row(row);
    }
    println!("{table}");
}

fn print_status_summary(
    job: &TransferJobStatusProto,
    task_summary: Option<&TransferTaskSummaryProto>,
    verbose: bool,
) {
    let mut table = md_table();
    table.set_header(["FIELD", "VALUE"]);
    table.add_row(["Job ID".to_string(), job.job_id.clone()]);
    table.add_row(["Run".to_string(), job.run_id.to_string()]);
    if job.kind != 0 {
        table.add_row(["Kind".to_string(), kind_name(job.kind).to_string()]);
    }
    table.add_row(["State".to_string(), state_name(job.state).to_string()]);
    table.add_row([
        "Progress".to_string(),
        progress_text(job.progress.loaded_size, job.progress.total_size),
    ]);
    if let Some(summary) = task_summary {
        table.add_row(["Files".to_string(), task_summary_text(summary)]);
    }
    if !job.progress.message.is_empty() {
        table.add_row(["Message".to_string(), job.progress.message.clone()]);
    }
    if !job.source_path.is_empty() {
        table.add_row(["Source".to_string(), job.source_path.clone()]);
    }
    if !job.target_path.is_empty() {
        table.add_row(["Target".to_string(), job.target_path.clone()]);
    }
    table.add_row(["Updated".to_string(), format_timestamp(job.updated_at)]);
    if verbose {
        if !job.submitter.is_empty() {
            table.add_row(["Submitter".to_string(), job.submitter.clone()]);
        }
        if !job.tenant.is_empty() {
            table.add_row(["Tenant".to_string(), job.tenant.clone()]);
        }
        if let Some(owner) = &job.owner {
            if !owner.is_empty() {
                table.add_row(["Owner".to_string(), owner.clone()]);
            }
        }
        if let Some(epoch) = job.lease_epoch {
            table.add_row(["Lease".to_string(), epoch.to_string()]);
        }
        if let Some(expire_at) = job.lease_expire_at {
            table.add_row(["Lease Expires".to_string(), format_timestamp(expire_at)]);
        }
        if let Some(epoch) = job.cv_metadata_epoch {
            table.add_row(["CV Metadata Epoch".to_string(), epoch.to_string()]);
        }
        table.add_row(["Created".to_string(), format_timestamp(job.created_at)]);
    }
    println!("{table}");
}

fn print_tasks_table(tasks: &[TransferTaskStatusProto], full_id: bool, verbose: bool) {
    let mut table = md_table();
    if verbose {
        table.set_header([
            "TASK ID", "ATTEMPT", "STATE", "PROGRESS", "WORKER", "SESSION", "RETRY", "MESSAGE",
            "SOURCE", "TARGET",
        ]);
    } else {
        table.set_header([
            "TASK ID", "STATE", "PROGRESS", "WORKER", "RETRY", "SOURCE", "TARGET",
        ]);
    }
    for task in tasks {
        let mut row = vec![
            display_id(&task.task_id, full_id),
            task_state_name(task.state).to_string(),
            progress_text(task.progress.loaded_size, task.progress.total_size),
            task.worker_id.to_string(),
            task.retry_count.to_string(),
        ];
        if verbose {
            row.insert(1, task.attempt_id.to_string());
            row.insert(5, display_id(&task.worker_session_id, full_id));
            row.push(truncate_middle(&task.progress.message, 28));
        }
        row.push(truncate_middle(
            &task.source_path,
            if verbose { 36 } else { 28 },
        ));
        row.push(truncate_middle(
            &task.target_path,
            if verbose { 36 } else { 28 },
        ));
        table.add_row(row);
    }
    println!("{table}");
}

fn print_tenants_table(tenants: &[TransferTenantSummaryProto]) {
    let mut table = md_table();
    table.set_header([
        "TENANT",
        "PENDING",
        "EXECUTING",
        "COMPLETED",
        "FAILED",
        "CANCELED",
        "PARTIAL",
        "TOTAL",
    ]);
    for tenant in tenants {
        table.add_row([
            if tenant.tenant.is_empty() {
                "<default>".to_string()
            } else {
                tenant.tenant.clone()
            },
            tenant.pending.to_string(),
            tenant.executing.to_string(),
            tenant.completed.to_string(),
            tenant.failed.to_string(),
            tenant.canceled.to_string(),
            tenant.partial_success.to_string(),
            tenant.total.to_string(),
        ]);
    }
    println!("{table}");
}

fn task_summary_text(summary: &TransferTaskSummaryProto) -> String {
    let mut parts = vec![
        format!("{} completed", summary.completed),
        format!("{} failed", summary.failed),
        format!("{} running", summary.running),
    ];
    if summary.pending > 0 {
        parts.push(format!("{} pending", summary.pending));
    }
    if summary.canceled > 0 {
        parts.push(format!("{} canceled", summary.canceled));
    }
    if summary.stale > 0 {
        parts.push(format!("{} stale", summary.stale));
    }
    parts.push(format!(
        "{} transferred",
        bytes_to_string(summary.completed_size.max(0))
    ));
    parts.join(", ")
}

fn print_next_page(next_page_token: Option<&str>) {
    if let Some(token) = next_page_token {
        println!("More results: use --page-token {token}");
    }
}

fn md_table() -> Table {
    let mut table = Table::new();
    table.load_preset(ASCII_MARKDOWN);
    table
}

fn short_id(value: &str) -> String {
    value.chars().take(12).collect()
}

fn display_id(value: &str, full_id: bool) -> String {
    if full_id {
        value.to_string()
    } else {
        short_id(value)
    }
}

fn owner_text(owner: Option<&str>) -> String {
    match owner {
        Some(value) if !value.is_empty() => short_id(value),
        _ => String::new(),
    }
}

fn truncate_middle(value: &str, max_len: usize) -> String {
    let char_len = value.chars().count();
    if char_len <= max_len {
        return value.to_string();
    }
    if max_len <= 3 {
        return value.chars().take(max_len).collect();
    }
    let keep = (max_len - 3) / 2;
    let tail = max_len - 3 - keep;
    let head: String = value.chars().take(keep).collect();
    let mut tail_chars: Vec<char> = value.chars().rev().take(tail).collect();
    tail_chars.reverse();
    let tail_text: String = tail_chars.into_iter().collect();
    format!("{head}...{tail_text}")
}

fn progress_text(loaded: i64, total: i64) -> String {
    if total <= 0 {
        return bytes_to_string(loaded.max(0));
    }
    let loaded = loaded.max(0);
    let percentage = (loaded as f64 / total as f64 * 100.0).clamp(0.0, 100.0);
    format!(
        "{} / {} ({percentage:.0}%)",
        bytes_to_string(loaded),
        bytes_to_string(total)
    )
}

fn format_timestamp(timestamp_ms: i64) -> String {
    if timestamp_ms <= 0 {
        return "-".to_string();
    }
    let Some(timestamp) = chrono::DateTime::from_timestamp_millis(timestamp_ms) else {
        return "-".to_string();
    };
    timestamp
        .with_timezone(&chrono::Local)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

fn transfer_is_terminal(state: i32) -> bool {
    matches!(state, 6..=9)
}

fn kind_name(kind: i32) -> &'static str {
    match kind {
        1 => "Load",
        2 => "Export",
        _ => "Unknown",
    }
}

fn state_name(state: i32) -> &'static str {
    match state {
        1 => "Pending",
        2 => "Planning",
        3 => "Dispatching",
        4 => "Running",
        5 => "Canceling",
        6 => "Completed",
        7 => "Failed",
        8 => "Canceled",
        9 => "PartialSuccess",
        _ => "Unknown",
    }
}

fn task_state_name(state: i32) -> &'static str {
    match state {
        1 => "Pending",
        2 => "Running",
        3 => "Completed",
        4 => "Failed",
        5 => "Canceled",
        6 => "Stale",
        _ => "Unknown",
    }
}
