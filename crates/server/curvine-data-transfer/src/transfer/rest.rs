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

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use curvine_error::FsError;
use curvine_model::{
    TransferCommand, TransferJobRecord, TransferKind, TransferState, TransferTaskCounts,
};
use curvine_proto::{
    ListTransferTenantsRequest, ListTransfersRequest, SubmitTransferRequest,
    TransferJobStatusProto, TransferKindProto, TransferProgressProto, TransferStateProto,
    TransferTaskStateProto, TransferTaskStatusProto, TransferTaskSummaryProto,
    TransferTenantSummaryProto,
};
use serde::{Deserialize, Serialize};

use super::service::{job_to_proto, reject_relative_segments};
use super::{task_summary_to_proto, TransferService, TransferStore, TransferStoreBackend};

const DEFAULT_JOB_PAGE_SIZE: u32 = 20;
const DEFAULT_TASK_PAGE_SIZE: u32 = 50;
const MAX_TASK_PAGE_SIZE: u32 = 1000;

#[derive(Clone)]
struct RestState {
    service: TransferService<TransferStoreBackend>,
}

pub(crate) fn router(service: TransferService<TransferStoreBackend>) -> Router {
    Router::new()
        .route("/api/transfer/jobs", get(list_jobs).post(submit_job))
        .route("/api/transfer/jobs/:job_id", get(get_job))
        .route("/api/transfer/jobs/:job_id/tasks", get(list_tasks))
        .route("/api/transfer/jobs/:job_id/cancel", post(cancel_job))
        .route("/api/transfer/jobs/:job_id/retry", post(retry_job))
        .route("/api/transfer/tenants", get(list_tenants))
        .with_state(RestState { service })
}

#[derive(Debug, Default, Deserialize)]
struct ListJobsQuery {
    kind: Option<String>,
    state: Option<String>,
    submitter: Option<String>,
    tenant: Option<String>,
    path: Option<String>,
    limit: Option<u32>,
    page_token: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct PageQuery {
    limit: Option<u32>,
    page_token: Option<String>,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    message: String,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

type ApiResult<T> = Result<T, ApiError>;

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
        }
    }

    fn from_fs(err: FsError) -> Self {
        let status = match err {
            FsError::JobNotFound(_) => StatusCode::NOT_FOUND,
            FsError::TransferTargetConflict(_) | FsError::TransferAlreadyRunning(_) => {
                StatusCode::CONFLICT
            }
            FsError::InvalidArgument(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            status,
            message: err.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorBody {
                message: self.message,
            }),
        )
            .into_response()
    }
}

#[derive(Debug, Serialize)]
struct ProgressDto {
    loaded_size: i64,
    total_size: i64,
    update_time: i64,
    message: String,
    total_files: u64,
    loaded_files: u64,
    skipped_files: u64,
    skipped_size: i64,
}

#[derive(Debug, Serialize)]
struct JobDto {
    job_id: String,
    run_id: u64,
    kind: String,
    state: String,
    source_path: String,
    target_path: String,
    progress: ProgressDto,
    submitter: String,
    tenant: String,
    created_at: i64,
    updated_at: i64,
    owner: Option<String>,
    lease_epoch: Option<u64>,
    lease_expire_at: Option<i64>,
    cv_metadata_epoch: Option<u64>,
}

#[derive(Debug, Serialize)]
struct TaskDto {
    task_id: String,
    attempt_id: u64,
    source_path: String,
    target_path: String,
    worker_id: u32,
    worker_session_id: String,
    state: String,
    progress: ProgressDto,
    retry_count: u32,
    updated_at: i64,
}

#[derive(Debug, Serialize)]
struct TaskSummaryDto {
    pending: u64,
    running: u64,
    completed: u64,
    failed: u64,
    canceled: u64,
    stale: u64,
    completed_size: i64,
}

#[derive(Debug, Serialize)]
struct TenantDto {
    tenant: String,
    pending: u64,
    executing: u64,
    completed: u64,
    failed: u64,
    canceled: u64,
    partial_success: u64,
    total: u64,
}

#[derive(Debug, Serialize)]
struct JobsResponse {
    jobs: Vec<JobDto>,
    next_page_token: Option<String>,
}

#[derive(Debug, Serialize)]
struct JobDetailResponse {
    job: JobDto,
    task_summary: TaskSummaryDto,
    tasks: Vec<TaskDto>,
    next_page_token: Option<String>,
}

#[derive(Debug, Serialize)]
struct TasksResponse {
    tasks: Vec<TaskDto>,
    next_page_token: Option<String>,
}

#[derive(Debug, Serialize)]
struct TenantsResponse {
    tenants: Vec<TenantDto>,
    next_page_token: Option<String>,
}

#[derive(Debug, Serialize)]
struct JobStateResponse {
    job_id: String,
    run_id: u64,
    state: String,
}

#[derive(Debug, Default, Deserialize)]
struct SubmitJobBody {
    #[serde(default)]
    kind: Option<String>,
    source_path: String,
    #[serde(default)]
    files: Vec<String>,
    #[serde(default)]
    overwrite: Option<bool>,
    #[serde(default)]
    submitter: Option<String>,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    force: bool,
}

#[derive(Debug, Serialize)]
struct SubmitJobResponse {
    job_id: String,
    run_id: u64,
    state: String,
    files: usize,
    created: bool,
}

fn normalized_files(files: Vec<String>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    files
        .into_iter()
        .map(|file| file.trim().to_string())
        .filter(|file| !file.is_empty() && !file.starts_with('#'))
        .filter(|file| seen.insert(file.clone()))
        .collect()
}

async fn submit_job(
    State(state): State<RestState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let mut file_list_body = false;
    let body = if content_type.starts_with("application/json") {
        serde_json::from_slice::<SubmitJobBody>(&body)
            .map_err(|err| ApiError::bad_request(format!("invalid JSON body: {err}")))?
    } else {
        file_list_body = true;
        let source_path = params
            .get("source_path")
            .ok_or_else(|| ApiError::bad_request("source_path query parameter is required"))?
            .clone();
        let text = std::str::from_utf8(&body)
            .map_err(|_| ApiError::bad_request("file list must be UTF-8 text"))?;
        SubmitJobBody {
            kind: params.get("kind").cloned(),
            source_path,
            files: text.lines().map(str::to_string).collect(),
            overwrite: params.get("overwrite").map(|value| value != "false"),
            submitter: params.get("submitter").cloned(),
            tenant: params.get("tenant").cloned(),
            force: params.get("force").is_some_and(|value| value != "false"),
        }
    };

    let kind = match body
        .kind
        .as_deref()
        .unwrap_or("load")
        .to_ascii_lowercase()
        .as_str()
    {
        "load" => TransferKind::Load,
        "export" => TransferKind::Export,
        other => return Err(ApiError::bad_request(format!("invalid kind '{other}'"))),
    };
    if body.source_path.trim().is_empty() {
        return Err(ApiError::bad_request("source_path is required"));
    }
    let files = normalized_files(body.files);
    if file_list_body && files.is_empty() {
        return Err(ApiError::bad_request("file list is empty"));
    }
    if kind != TransferKind::Load && !files.is_empty() {
        return Err(ApiError::bad_request(
            "files is supported for load jobs only",
        ));
    }
    for file in &files {
        let path = curvine_fs_api::Path::from_str(file)
            .map_err(|_| ApiError::bad_request(format!("invalid file list path: {file}")))?;
        reject_relative_segments(&path).map_err(|err| ApiError::bad_request(err.to_string()))?;
    }
    let target_path = state
        .service
        .default_target_for_source(kind, body.source_path.trim())
        .map_err(ApiError::from_fs)?;
    let overwrite = body.overwrite.unwrap_or(true);
    let submitter = body
        .submitter
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "http-api".to_string());
    let tenant = body.tenant.unwrap_or_default();
    let mut command = TransferCommand {
        kind,
        source_path: body.source_path.trim().to_string(),
        target_path: target_path.trim().to_string(),
        client_request_id: TransferCommand::default_client_request_id_with_overwrite(
            kind,
            body.source_path.trim(),
            target_path.trim(),
            overwrite,
        ),
        submitter: submitter.clone(),
        tenant: tenant.clone(),
        options: std::collections::BTreeMap::new(),
        files: files.clone(),
    };
    if let Some(digest) = command.files_digest() {
        command.client_request_id = format!("{}_files_{digest}", command.client_request_id);
    }
    command.set_overwrite(overwrite);

    let existing = state
        .service
        .get_transfer_by_request(&submitter, &command.client_request_id)
        .map_err(ApiError::from_fs)?;
    let command_bytes = serde_json::to_vec(&command)
        .map_err(|err| ApiError::bad_request(format!("invalid command: {err}")))?;
    let req = SubmitTransferRequest {
        kind: kind as i32,
        source_path: command.source_path.clone(),
        target_path: command.target_path.clone(),
        client_request_id: command.client_request_id.clone(),
        submitter: command.submitter.clone(),
        tenant: command.tenant.clone(),
        command: command_bytes,
        // Version 1 servers drop the files field and would load the whole root.
        protocol_version: Some(if files.is_empty() { 1 } else { 2 }),
    };
    let mut job = state
        .service
        .submit_transfer(req)
        .map_err(ApiError::from_fs)?;
    let mut created = existing.is_none();
    if body.force && job.state.is_terminal() {
        job = state
            .service
            .retry_transfer(&job.job_id)
            .map_err(ApiError::from_fs)?;
        created = true;
    }
    let status = if created {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    Ok((
        status,
        Json(SubmitJobResponse {
            job_id: job.job_id,
            run_id: job.run_id,
            state: state_name(job.state as i32),
            files: files.len(),
            created,
        }),
    )
        .into_response())
}

async fn list_jobs(
    State(state): State<RestState>,
    Query(query): Query<ListJobsQuery>,
) -> ApiResult<Json<JobsResponse>> {
    collect_jobs(&state.service, &query).map(Json)
}

async fn get_job(
    State(state): State<RestState>,
    Path(job_id): Path<String>,
    Query(query): Query<PageQuery>,
) -> ApiResult<Json<JobDetailResponse>> {
    collect_job(&state.service, &job_id, &query).map(Json)
}

async fn list_tasks(
    State(state): State<RestState>,
    Path(job_id): Path<String>,
    Query(query): Query<PageQuery>,
) -> ApiResult<Json<TasksResponse>> {
    collect_tasks(&state.service, &job_id, &query).map(Json)
}

async fn list_tenants(
    State(state): State<RestState>,
    Query(query): Query<PageQuery>,
) -> ApiResult<Json<TenantsResponse>> {
    collect_tenants(&state.service, &query).map(Json)
}

async fn cancel_job(
    State(state): State<RestState>,
    Path(job_id): Path<String>,
) -> ApiResult<Json<JobStateResponse>> {
    collect_cancel(&state.service, &job_id).map(Json)
}

async fn retry_job(
    State(state): State<RestState>,
    Path(job_id): Path<String>,
) -> ApiResult<Json<JobDto>> {
    collect_retry(&state.service, &job_id).map(Json)
}

fn collect_jobs<S: TransferStore>(
    service: &TransferService<S>,
    query: &ListJobsQuery,
) -> ApiResult<JobsResponse> {
    let kind = query.kind.as_deref().map(parse_kind).transpose()?;
    let state = query.state.as_deref().map(parse_state).transpose()?;
    validate_page_token(query.page_token.as_deref())?;

    let resp = service
        .list_transfers(ListTransfersRequest {
            kind,
            state,
            page_size: Some(query.limit.unwrap_or(DEFAULT_JOB_PAGE_SIZE)),
            page_token: query.page_token.clone(),
            submitter: query.submitter.clone(),
            tenant: query.tenant.clone(),
            path: query.path.clone(),
        })
        .map_err(ApiError::from_fs)?;

    Ok(JobsResponse {
        jobs: resp.jobs.iter().map(job_dto).collect(),
        next_page_token: resp.next_page_token,
    })
}

fn collect_job<S: TransferStore>(
    service: &TransferService<S>,
    job_id: &str,
    query: &PageQuery,
) -> ApiResult<JobDetailResponse> {
    let (job, counts, tasks, next_page_token) = transfer_status(service, job_id, query)?;
    Ok(JobDetailResponse {
        job: job_dto(&job_to_proto(job)),
        task_summary: summary_dto(&task_summary_to_proto(counts)),
        tasks: tasks.iter().map(task_dto).collect(),
        next_page_token,
    })
}

fn collect_tasks<S: TransferStore>(
    service: &TransferService<S>,
    job_id: &str,
    query: &PageQuery,
) -> ApiResult<TasksResponse> {
    let (_, _, tasks, next_page_token) = transfer_status(service, job_id, query)?;
    Ok(TasksResponse {
        tasks: tasks.iter().map(task_dto).collect(),
        next_page_token,
    })
}

fn collect_tenants<S: TransferStore>(
    service: &TransferService<S>,
    query: &PageQuery,
) -> ApiResult<TenantsResponse> {
    validate_page_token(query.page_token.as_deref())?;

    let resp = service
        .list_tenant_summaries(ListTransferTenantsRequest {
            page_size: Some(query.limit.unwrap_or(DEFAULT_JOB_PAGE_SIZE)),
            page_token: query.page_token.clone(),
        })
        .map_err(ApiError::from_fs)?;

    Ok(TenantsResponse {
        tenants: resp.tenants.iter().map(tenant_dto).collect(),
        next_page_token: resp.next_page_token,
    })
}

fn collect_cancel<S: TransferStore>(
    service: &TransferService<S>,
    job_id: &str,
) -> ApiResult<JobStateResponse> {
    let job = service.get_transfer(job_id).map_err(ApiError::from_fs)?;
    let job_id = job.job_id.clone();
    let run_id = job.run_id;

    if job.state.is_terminal() || job.state == TransferState::Canceling {
        return Ok(JobStateResponse {
            job_id,
            run_id,
            state: state_name(job.state as i32),
        });
    }

    let state = service
        .request_cancel(job_id.as_str(), None)
        .map_err(|err| ApiError::conflict(err.to_string()))?;

    Ok(JobStateResponse {
        job_id,
        run_id,
        state: state_name(state as i32),
    })
}

fn collect_retry<S: TransferStore>(
    service: &TransferService<S>,
    job_id: &str,
) -> ApiResult<JobDto> {
    let job = service.get_transfer(job_id).map_err(ApiError::from_fs)?;
    if !job.state.is_terminal() {
        return Err(ApiError::conflict(format!(
            "only terminal transfers can be retried, job {} is {}",
            job.job_id,
            state_name(job.state as i32)
        )));
    }

    let retried = service.retry_transfer(job_id).map_err(ApiError::from_fs)?;
    Ok(job_dto(&job_to_proto(retried)))
}

fn transfer_status<S: TransferStore>(
    service: &TransferService<S>,
    job_id: &str,
    query: &PageQuery,
) -> ApiResult<(
    TransferJobRecord,
    TransferTaskCounts,
    Vec<TransferTaskStatusProto>,
    Option<String>,
)> {
    validate_page_token(query.page_token.as_deref())?;
    let page_size = query
        .limit
        .unwrap_or(DEFAULT_TASK_PAGE_SIZE)
        .clamp(1, MAX_TASK_PAGE_SIZE);
    service
        .get_transfer_status(job_id, Some(page_size), query.page_token.clone())
        .map_err(ApiError::from_fs)
}

fn parse_kind(value: &str) -> ApiResult<i32> {
    match value.trim().to_ascii_lowercase().as_str() {
        "load" => Ok(TransferKindProto::TransferLoad as i32),
        "export" => Ok(TransferKindProto::TransferExport as i32),
        other => Err(ApiError::bad_request(format!(
            "invalid kind '{other}': expected load or export"
        ))),
    }
}

fn parse_state(value: &str) -> ApiResult<i32> {
    let state = match value.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "pending" => TransferStateProto::TransferPending,
        "planning" => TransferStateProto::TransferPlanning,
        "dispatching" => TransferStateProto::TransferDispatching,
        "running" => TransferStateProto::TransferRunning,
        "canceling" => TransferStateProto::TransferCanceling,
        "completed" => TransferStateProto::TransferCompleted,
        "failed" => TransferStateProto::TransferFailed,
        "canceled" => TransferStateProto::TransferCanceled,
        "partial_success" => TransferStateProto::TransferPartialSuccess,
        other => {
            return Err(ApiError::bad_request(format!(
                "invalid state '{other}': expected one of pending, planning, dispatching, \
                 running, canceling, completed, failed, canceled, partial_success"
            )))
        }
    };
    Ok(state as i32)
}

fn validate_page_token(token: Option<&str>) -> ApiResult<()> {
    if let Some(token) = token {
        if token.trim().parse::<usize>().is_err() {
            return Err(ApiError::bad_request(format!(
                "invalid page_token '{token}': expected a non-negative integer offset"
            )));
        }
    }
    Ok(())
}

fn progress_dto(progress: &TransferProgressProto) -> ProgressDto {
    ProgressDto {
        loaded_size: progress.loaded_size,
        total_size: progress.total_size,
        update_time: progress.update_time,
        message: progress.message.clone(),
        total_files: progress.total_files.unwrap_or_default(),
        loaded_files: progress.loaded_files.unwrap_or_default(),
        skipped_files: progress.skipped_files.unwrap_or_default(),
        skipped_size: progress.skipped_size.unwrap_or_default(),
    }
}

fn job_dto(job: &TransferJobStatusProto) -> JobDto {
    JobDto {
        job_id: job.job_id.clone(),
        run_id: job.run_id,
        kind: kind_name(job.kind),
        state: state_name(job.state),
        source_path: job.source_path.clone(),
        target_path: job.target_path.clone(),
        progress: progress_dto(&job.progress),
        submitter: job.submitter.clone(),
        tenant: job.tenant.clone(),
        created_at: job.created_at,
        updated_at: job.updated_at,
        owner: job.owner.clone(),
        lease_epoch: job.lease_epoch,
        lease_expire_at: job.lease_expire_at,
        cv_metadata_epoch: job.cv_metadata_epoch,
    }
}

fn task_dto(task: &TransferTaskStatusProto) -> TaskDto {
    TaskDto {
        task_id: task.task_id.clone(),
        attempt_id: task.attempt_id,
        source_path: task.source_path.clone(),
        target_path: task.target_path.clone(),
        worker_id: task.worker_id,
        worker_session_id: task.worker_session_id.clone(),
        state: task_state_name(task.state),
        progress: progress_dto(&task.progress),
        retry_count: task.retry_count,
        updated_at: task.updated_at,
    }
}

fn summary_dto(summary: &TransferTaskSummaryProto) -> TaskSummaryDto {
    TaskSummaryDto {
        pending: summary.pending,
        running: summary.running,
        completed: summary.completed,
        failed: summary.failed,
        canceled: summary.canceled,
        stale: summary.stale,
        completed_size: summary.completed_size,
    }
}

fn tenant_dto(summary: &TransferTenantSummaryProto) -> TenantDto {
    TenantDto {
        tenant: summary.tenant.clone(),
        pending: summary.pending,
        executing: summary.executing,
        completed: summary.completed,
        failed: summary.failed,
        canceled: summary.canceled,
        partial_success: summary.partial_success,
        total: summary.total,
    }
}

fn kind_name(kind: i32) -> String {
    match TransferKindProto::from_i32(kind) {
        Some(TransferKindProto::TransferLoad) => "load",
        Some(TransferKindProto::TransferExport) => "export",
        None => "unknown",
    }
    .to_string()
}

fn state_name(state: i32) -> String {
    match TransferStateProto::from_i32(state) {
        Some(TransferStateProto::TransferPending) => "pending",
        Some(TransferStateProto::TransferPlanning) => "planning",
        Some(TransferStateProto::TransferDispatching) => "dispatching",
        Some(TransferStateProto::TransferRunning) => "running",
        Some(TransferStateProto::TransferCanceling) => "canceling",
        Some(TransferStateProto::TransferCompleted) => "completed",
        Some(TransferStateProto::TransferFailed) => "failed",
        Some(TransferStateProto::TransferCanceled) => "canceled",
        Some(TransferStateProto::TransferPartialSuccess) => "partial_success",
        None => "unknown",
    }
    .to_string()
}

fn task_state_name(state: i32) -> String {
    match TransferTaskStateProto::from_i32(state) {
        Some(TransferTaskStateProto::TransferTaskPending) => "pending",
        Some(TransferTaskStateProto::TransferTaskRunning) => "running",
        Some(TransferTaskStateProto::TransferTaskCompleted) => "completed",
        Some(TransferTaskStateProto::TransferTaskFailed) => "failed",
        Some(TransferTaskStateProto::TransferTaskCanceled) => "canceled",
        Some(TransferTaskStateProto::TransferTaskStale) => "stale",
        None => "unknown",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use curvine_model::{TransferKind, TransferState};
    use curvine_proto::SubmitTransferRequest;

    use super::*;
    use crate::transfer::MemoryTransferStore;

    fn test_service() -> (
        Arc<MemoryTransferStore>,
        TransferService<MemoryTransferStore>,
    ) {
        let store = Arc::new(MemoryTransferStore::new());
        let service = TransferService::new(store.clone());
        (store, service)
    }

    fn submit(
        service: &TransferService<MemoryTransferStore>,
        kind: TransferKind,
        submitter: &str,
        tenant: &str,
        request_id: &str,
    ) -> String {
        let (source, target) = match kind {
            TransferKind::Load => (
                format!("s3://bucket/source-{request_id}"),
                format!("/cv/target-{request_id}"),
            ),
            TransferKind::Export => (
                format!("/cv/source-{request_id}"),
                format!("s3://bucket/target-{request_id}"),
            ),
        };
        service
            .submit_transfer(SubmitTransferRequest {
                kind: kind as i32,
                source_path: source,
                target_path: target,
                client_request_id: request_id.to_string(),
                submitter: submitter.to_string(),
                tenant: tenant.to_string(),
                command: Vec::new(),
                protocol_version: Some(1),
            })
            .unwrap()
            .job_id
    }

    fn submit_load(service: &TransferService<MemoryTransferStore>, request_id: &str) -> String {
        submit(service, TransferKind::Load, "tester", "default", request_id)
    }

    #[test]
    fn normalized_file_list_dedupes_and_filters_comments() {
        assert_eq!(
            normalized_files(vec![
                " a.parquet ".into(),
                "# comment".into(),
                "a.parquet".into(),
                "b.parquet".into(),
                "".into(),
            ]),
            vec!["a.parquet".to_string(), "b.parquet".to_string()]
        );
    }

    #[test]
    fn list_jobs_returns_empty_page() {
        let (_store, service) = test_service();
        let resp = collect_jobs(&service, &ListJobsQuery::default()).unwrap();
        assert!(resp.jobs.is_empty());
        assert!(resp.next_page_token.is_none());
    }

    #[test]
    fn list_jobs_filters_by_kind_state_submitter_and_tenant() {
        let (store, service) = test_service();
        let load = submit(&service, TransferKind::Load, "alice", "team-a", "r1");
        let export = submit(&service, TransferKind::Export, "bob", "team-b", "r2");
        store.set_job_state(&load, TransferState::Running);
        store.set_job_state(&export, TransferState::Completed);

        let resp = collect_jobs(
            &service,
            &ListJobsQuery {
                kind: Some("LOAD".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(resp.jobs.len(), 1);
        assert_eq!(resp.jobs[0].job_id, load);
        assert_eq!(resp.jobs[0].kind, "load");
        assert_eq!(resp.jobs[0].state, "running");

        let resp = collect_jobs(
            &service,
            &ListJobsQuery {
                state: Some("completed".to_string()),
                submitter: Some("bob".to_string()),
                tenant: Some("team-b".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(resp.jobs.len(), 1);
        assert_eq!(resp.jobs[0].job_id, export);
        assert_eq!(resp.jobs[0].kind, "export");

        let resp = collect_jobs(
            &service,
            &ListJobsQuery {
                tenant: Some("nobody".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(resp.jobs.is_empty());
    }

    #[test]
    fn list_jobs_filters_by_path_substring() {
        let (_store, service) = test_service();
        let load = submit(&service, TransferKind::Load, "alice", "team-a", "r1");
        let export = submit(&service, TransferKind::Export, "bob", "team-b", "r2");

        // Case-insensitive substring match on the target path only.
        // "S3://BUCKET" matches the export target s3://bucket/target-r2,
        // but not the load target /cv/target-r1.
        let resp = collect_jobs(
            &service,
            &ListJobsQuery {
                path: Some("S3://BUCKET".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(resp.jobs.len(), 1);
        assert_eq!(resp.jobs[0].job_id, export);

        let resp = collect_jobs(
            &service,
            &ListJobsQuery {
                path: Some("target-r1".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(resp.jobs.len(), 1);
        assert_eq!(resp.jobs[0].job_id, load);

        let resp = collect_jobs(
            &service,
            &ListJobsQuery {
                path: Some("target-r2".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(resp.jobs.len(), 1);
        assert_eq!(resp.jobs[0].job_id, export);

        // Source paths are not searched.
        let resp = collect_jobs(
            &service,
            &ListJobsQuery {
                path: Some("source-r1".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(resp.jobs.is_empty());

        // LIKE wildcards in the input are matched literally.
        let resp = collect_jobs(
            &service,
            &ListJobsQuery {
                path: Some("%".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(resp.jobs.is_empty());
    }

    #[test]
    fn list_jobs_paginates_without_overlap() {
        let (_store, service) = test_service();
        for index in 0..3 {
            submit_load(&service, &format!("page-{index}"));
        }

        let first = collect_jobs(
            &service,
            &ListJobsQuery {
                limit: Some(2),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(first.jobs.len(), 2);
        let token = first.next_page_token.clone().expect("next page token");

        let second = collect_jobs(
            &service,
            &ListJobsQuery {
                limit: Some(2),
                page_token: Some(token),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(second.jobs.len(), 1);
        assert!(second.next_page_token.is_none());
        assert!(second
            .jobs
            .iter()
            .all(|job| first.jobs.iter().all(|seen| seen.job_id != job.job_id)));
    }

    #[test]
    fn job_detail_unknown_job_returns_not_found() {
        let (_store, service) = test_service();
        let err = collect_job(&service, "missing-job", &PageQuery::default()).unwrap_err();
        assert_eq!(err.status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn job_detail_returns_job_with_empty_task_page() {
        let (_store, service) = test_service();
        let job = submit_load(&service, "detail");

        let detail = collect_job(&service, &job, &PageQuery::default()).unwrap();
        assert_eq!(detail.job.job_id, job);
        assert_eq!(detail.job.state, "pending");
        assert!(detail.tasks.is_empty());
        assert!(detail.next_page_token.is_none());
        assert_eq!(detail.task_summary.pending, 0);

        let tasks = collect_tasks(&service, &job, &PageQuery::default()).unwrap();
        assert!(tasks.tasks.is_empty());
    }

    #[test]
    fn invalid_filters_and_page_tokens_are_bad_requests() {
        let (_store, service) = test_service();

        let err = collect_jobs(
            &service,
            &ListJobsQuery {
                kind: Some("bogus".to_string()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);

        let err = collect_jobs(
            &service,
            &ListJobsQuery {
                state: Some("done".to_string()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);

        let err = collect_jobs(
            &service,
            &ListJobsQuery {
                page_token: Some("abc".to_string()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn tenant_summary_lists_each_tenant() {
        let (_store, service) = test_service();
        submit(&service, TransferKind::Load, "alice", "team-a", "tenant-1");
        submit(&service, TransferKind::Load, "bob", "team-b", "tenant-2");

        let resp = collect_tenants(&service, &PageQuery::default()).unwrap();
        assert_eq!(resp.tenants.len(), 2);
        let team_a = resp
            .tenants
            .iter()
            .find(|item| item.tenant == "team-a")
            .expect("team-a summary");
        assert_eq!(team_a.total, 1);
        assert_eq!(team_a.pending, 1);
    }

    #[test]
    fn cancel_running_job_moves_to_canceling() {
        let (store, service) = test_service();
        let job = submit_load(&service, "cancel-running");
        store.set_job_state(&job, TransferState::Running);

        let resp = collect_cancel(&service, &job).unwrap();
        assert_eq!(resp.job_id, job);
        assert_eq!(resp.state, "canceling");
    }

    #[test]
    fn cancel_terminal_job_is_idempotent() {
        let (store, service) = test_service();
        let job = submit_load(&service, "cancel-completed");
        store.set_job_state(&job, TransferState::Completed);

        let resp = collect_cancel(&service, &job).unwrap();
        assert_eq!(resp.state, "completed");
    }

    #[test]
    fn cancel_or_retry_missing_job_returns_not_found() {
        let (_store, service) = test_service();

        let err = collect_cancel(&service, "missing-job").unwrap_err();
        assert_eq!(err.status, StatusCode::NOT_FOUND);

        let err = collect_retry(&service, "missing-job").unwrap_err();
        assert_eq!(err.status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn retry_active_job_returns_conflict() {
        let (store, service) = test_service();
        let job = submit_load(&service, "retry-running");
        store.set_job_state(&job, TransferState::Running);

        let err = collect_retry(&service, &job).unwrap_err();
        assert_eq!(err.status, StatusCode::CONFLICT);
        assert!(err.message.contains("running"));
    }

    #[tokio::test]
    async fn submit_job_parses_json_derives_target_and_is_idempotent() {
        use std::collections::HashMap as StdHashMap;
        use std::time::Duration;

        use axum::http::HeaderMap;
        use bytes::Bytes;
        use curvine_client_core::file::CurvineFileSystem;
        use curvine_config::ClusterConf;
        use curvine_model::MountInfo;
        use curvine_runtime::runtime::AsyncRuntime;

        use crate::transfer::cluster_cache::ClusterSnapshot;
        use crate::transfer::ClusterMetadataCache;

        let store = Arc::new(TransferStoreBackend::Memory(MemoryTransferStore::new()));
        let rt = Arc::new(AsyncRuntime::single());
        let fs = CurvineFileSystem::with_rt(ClusterConf::default(), rt).unwrap();
        let cache = ClusterMetadataCache::new(fs);
        cache.set_snapshot_for_test(ClusterSnapshot {
            version: 1,
            mounts: vec![MountInfo {
                ufs_path: "s3://bucket/dir".to_string(),
                cv_path: "/data/dir".to_string(),
                ..Default::default()
            }],
            workers: Vec::new(),
            updated_at: 1,
        });
        let service = TransferService::with_cache(store.clone(), cache, Duration::from_secs(60));
        let state = RestState { service };

        let make_request = || {
            let mut headers = HeaderMap::new();
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                "application/json".parse().unwrap(),
            );
            let body = Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "source_path": "s3://bucket/dir/a.parquet",
                    "submitter": "tester"
                }))
                .unwrap(),
            );
            (Query(StdHashMap::new()), headers, body)
        };

        let (query, headers, body) = make_request();
        let response = submit_job(State(state.clone()), query, headers, body)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let (query, headers, body) = make_request();
        let response = submit_job(State(state.clone()), query, headers, body)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let jobs = store.list_active_transfers().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].target_path, "/data/dir/a.parquet");

        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            "text/plain".parse().unwrap(),
        );
        let mut query = StdHashMap::new();
        query.insert("source_path".to_string(), "s3://bucket/dir".to_string());
        let err = submit_job(
            State(state),
            Query(query),
            headers,
            Bytes::from("# no files\n\n"),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("file list is empty"));
    }

    #[tokio::test]
    async fn submit_job_unmounted_source_returns_bad_request() {
        use std::collections::HashMap as StdHashMap;
        use std::time::Duration;

        use axum::http::HeaderMap;
        use bytes::Bytes;
        use curvine_client_core::file::CurvineFileSystem;
        use curvine_config::ClusterConf;
        use curvine_model::MountInfo;
        use curvine_runtime::runtime::AsyncRuntime;

        use crate::transfer::cluster_cache::ClusterSnapshot;
        use crate::transfer::ClusterMetadataCache;

        let store = Arc::new(TransferStoreBackend::Memory(MemoryTransferStore::new()));
        let rt = Arc::new(AsyncRuntime::single());
        let fs = CurvineFileSystem::with_rt(ClusterConf::default(), rt).unwrap();
        let cache = ClusterMetadataCache::new(fs);
        cache.set_snapshot_for_test(ClusterSnapshot {
            version: 1,
            mounts: vec![MountInfo {
                ufs_path: "s3://bucket/dir".to_string(),
                cv_path: "/data/dir".to_string(),
                ..Default::default()
            }],
            workers: Vec::new(),
            updated_at: 1,
        });
        let service = TransferService::with_cache(store, cache, Duration::from_secs(60));
        let state = RestState { service };

        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            "application/json".parse().unwrap(),
        );
        let body = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "source_path": "s3://other-bucket/a.parquet",
                "submitter": "tester"
            }))
            .unwrap(),
        );

        let err = submit_job(State(state), Query(StdHashMap::new()), headers, body)
            .await
            .unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(
            err.message.contains("No mount found"),
            "unexpected message: {}",
            err.message
        );
    }

    #[test]
    fn retry_terminal_job_creates_new_job() {
        let (store, service) = test_service();
        let job = submit_load(&service, "retry-completed");
        store.set_job_state(&job, TransferState::Completed);

        let retried = collect_retry(&service, &job).unwrap();
        assert_ne!(retried.job_id, job);
        assert_eq!(retried.kind, "load");
        assert_eq!(retried.state, "pending");
    }
}
