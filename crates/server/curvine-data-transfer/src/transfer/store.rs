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

use curvine_error::FsResult;
use curvine_model::{
    summarize_transfer_tasks, StaleTaskAttempt, TaskAttemptStart, TransferJobRecord, TransferLease,
    TransferListFilter, TransferProgress, TransferStateUpdate, TransferTaskCounts,
    TransferTaskRecord, TransferTaskReport, TransferTaskState, TransferTaskStatus,
    TransferTaskSummary, TransferTenantSummary, WorkerInflight,
};

pub trait TransferStore: Send + Sync + 'static {
    fn check_available(&self) -> FsResult<()>;

    /// Durably persist a request, preserving request-id idempotency. Target
    /// conflicts are checked by the scheduler after a request enters an
    /// executing state, so pending requests do not block submit.
    fn create_or_get_by_request_id(&self, job: TransferJobRecord) -> FsResult<TransferJobRecord> {
        self.create_or_get_by_request_id_checked(job)
    }

    /// Compatibility alias for callers that still use the former method name.
    fn create_or_get_by_request_id_checked(
        &self,
        job: TransferJobRecord,
    ) -> FsResult<TransferJobRecord>;

    fn get_transfer(&self, job_id: &str) -> FsResult<Option<TransferJobRecord>>;

    fn get_transfer_by_request(
        &self,
        submitter: &str,
        client_request_id: &str,
    ) -> FsResult<Option<TransferJobRecord>>;

    fn list_active_transfers(&self) -> FsResult<Vec<TransferJobRecord>>;

    fn find_conflicting_active_transfer(
        &self,
        target_path: &str,
        submitter: &str,
        client_request_id: &str,
    ) -> FsResult<Option<TransferJobRecord>>;

    fn count_active_transfers(&self) -> FsResult<u64>;

    fn count_executing_transfers(&self) -> FsResult<u64>;

    fn list_transfers(&self, filter: TransferListFilter) -> FsResult<Vec<TransferJobRecord>>;

    fn list_tenant_summaries(
        &self,
        limit: usize,
        offset: usize,
    ) -> FsResult<Vec<TransferTenantSummary>>;

    fn purge_terminal_transfers(&self, older_than_ms: i64, limit: usize) -> FsResult<usize>;

    fn list_transfer_tasks(&self, job_id: &str, run_id: u64) -> FsResult<Vec<TransferTaskRecord>>;

    /// Attempt start time for phase breakdown. Backends override with a
    /// narrow point query; default is unknown (phase metrics skip it).
    fn get_task_attempt_started_at(
        &self,
        _job_id: &str,
        _run_id: u64,
        _task_id: &str,
    ) -> FsResult<Option<i64>> {
        Ok(None)
    }

    /// Return progress/counts without requiring the caller to deserialize task definitions.
    fn get_transfer_task_summary(
        &self,
        job_id: &str,
        run_id: u64,
        now_ms: i64,
    ) -> FsResult<TransferTaskSummary> {
        let tasks = self.list_transfer_tasks(job_id, run_id)?;
        Ok(summarize_transfer_tasks(&tasks, now_ms))
    }

    /// Global transfer-task state counts across all jobs (observability only).
    /// Backends override with a narrow GROUP BY; the default is a full scan,
    /// so callers must keep this off the hot scheduling path (metrics poll).
    fn count_tasks_global(&self) -> FsResult<TransferTaskCounts> {
        Ok(TransferTaskCounts::default())
    }

    /// Return only the requested status page. Backends override this with a
    /// narrow, ordered query so source read plans never enter status polling.
    fn list_transfer_task_status_page(
        &self,
        job_id: &str,
        run_id: u64,
        offset: usize,
        limit: usize,
    ) -> FsResult<(Vec<TransferTaskStatus>, bool)> {
        let mut tasks = self.list_transfer_tasks(job_id, run_id)?;
        tasks.sort_by(|left, right| left.task_id.cmp(&right.task_id));
        let mut page = tasks
            .into_iter()
            .skip(offset)
            .take(limit.saturating_add(1))
            .collect::<Vec<_>>();
        let has_next = page.len() > limit;
        page.truncate(limit);
        Ok((page.into_iter().map(Into::into).collect(), has_next))
    }

    fn request_cancel(&self, job_id: &str, run_id: u64, now_ms: i64) -> FsResult<bool>;

    fn acquire_runnable_transfer(
        &self,
        owner: &str,
        lease_ms: i64,
        now_ms: i64,
        max_executing_transfers: u64,
        prefer_pending: bool,
    ) -> FsResult<Option<TransferLease>>;

    fn renew_lease(
        &self,
        job_id: &str,
        run_id: u64,
        owner: &str,
        lease_epoch: u64,
        lease_ms: i64,
        now_ms: i64,
    ) -> FsResult<bool>;

    fn update_transfer_state(&self, update: TransferStateUpdate) -> FsResult<bool>;

    fn requeue_transfer(&self, update: TransferRequeueUpdate) -> FsResult<bool>;

    fn set_transfer_cv_metadata_epoch(
        &self,
        job_id: &str,
        run_id: u64,
        owner: &str,
        lease_epoch: u64,
        cv_metadata_epoch: u64,
        now_ms: i64,
    ) -> FsResult<bool>;

    /// Persist the immutable mount snapshot after a lease is acquired. The
    /// lease guard makes the first writer authoritative across scheduler
    /// instances and keeps retries idempotent.
    fn set_transfer_mount_snapshot(&self, update: TransferMountSnapshotUpdate) -> FsResult<bool>;

    fn insert_tasks(&self, tasks: Vec<TransferTaskRecord>) -> FsResult<()>;

    fn persist_planned_tasks(&self, update: TransferPlannedTasks) -> FsResult<bool>;

    fn update_task_state(&self, update: TransferTaskStateUpdate) -> FsResult<bool>;

    fn claim_pending_tasks(
        &self,
        job_id: &str,
        run_id: u64,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>>;

    /// Per-worker aggregation of in-flight (Running) tasks, used by the
    /// scheduler's least-loaded worker choice. Must count only state = 2 rows.
    fn select_inflight_by_worker(&self) -> FsResult<Vec<WorkerInflight>> {
        Ok(Vec::new())
    }

    /// Mark only attempts that a preceding worker probe confirmed as missing.
    /// Backends must fence the update with the candidate's attempt identity and
    /// original stale deadline so a concurrent report wins.
    fn mark_stale_attempts(
        &self,
        job_id: &str,
        run_id: u64,
        owner: &str,
        lease_epoch: u64,
        now_ms: i64,
        candidates: Vec<TransferTaskRecord>,
    ) -> FsResult<Vec<StaleTaskAttempt>>;

    fn list_stale_running_tasks(
        &self,
        job_id: &str,
        run_id: u64,
        stale_before_ms: i64,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>>;

    /// List tasks in the given state for a job run, oldest-updated first, capped by `limit`.
    fn list_tasks_by_state(
        &self,
        job_id: &str,
        run_id: u64,
        state: TransferTaskState,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>>;

    fn has_failed_tasks(&self, job_id: &str, run_id: u64) -> FsResult<bool>;

    fn has_recoverable_tasks(&self, job_id: &str, run_id: u64) -> FsResult<bool>;

    fn start_task_attempt(&self, start: TaskAttemptStart) -> FsResult<bool>;

    /// Persist a task progress/terminal report.
    ///
    /// Updates only the task row (including `stale_deadline_at`). Job-level
    /// `summary` is refreshed separately by [`Self::flush_job_progress_summary`].
    fn update_task_report(&self, report: TransferTaskReport) -> FsResult<bool>;

    /// Persist a previously computed summary. The task aggregation and the job
    /// write are separate so job locks never cover task scans.
    fn update_job_progress_summary(
        &self,
        job_id: &str,
        run_id: u64,
        summary: TransferProgress,
        now_ms: i64,
    ) -> FsResult<()>;

    fn flush_job_progress_summary(&self, job_id: &str, run_id: u64) -> FsResult<()> {
        let now_ms = curvine_runtime::common::LocalTime::mills() as i64;
        let summary = self.get_transfer_task_summary(job_id, run_id, now_ms)?;
        self.update_job_progress_summary(job_id, run_id, summary.progress, now_ms)
    }
}

pub struct TransferRequeueUpdate {
    pub job_id: String,
    pub run_id: u64,
    pub owner: String,
    pub lease_epoch: u64,
    pub message: String,
    pub next_attempt_at_ms: i64,
    /// Replaces the persisted mount snapshot retry count when this requeue is
    /// caused by a retryable metadata refresh failure.
    pub mount_snapshot_attempts: Option<u32>,
    /// Replaces the persisted planning retry count when this requeue is
    /// caused by a retryable planning failure (e.g. UFS rate limiting).
    pub planning_attempts: Option<u32>,
    pub now_ms: i64,
}

pub struct TransferMountSnapshotUpdate {
    pub job_id: String,
    pub run_id: u64,
    pub owner: String,
    pub lease_epoch: u64,
    pub mount_snapshot_json: String,
    pub cluster_snapshot_version: u64,
    pub now_ms: i64,
}

pub struct TransferPlannedTasks {
    pub job_id: String,
    pub run_id: u64,
    pub owner: String,
    pub lease_epoch: u64,
    pub tasks: Vec<TransferTaskRecord>,
    pub message: String,
    pub now_ms: i64,
}

pub struct TransferTaskStateUpdate {
    pub job_id: String,
    pub run_id: u64,
    pub owner: String,
    pub lease_epoch: u64,
    pub task_id: String,
    pub from_states: Vec<TransferTaskState>,
    pub state: TransferTaskState,
    pub message: String,
    pub now_ms: i64,
    /// When true, the store bumps retry_count on this transition so the
    /// task_max_retries ceiling actually bounds repeated stale/reject retry
    /// storms (previously retry_count never incremented and stale tasks
    /// redelivered forever, starving newer Pending jobs).
    pub bump_retry: bool,
}

impl TransferTaskStateUpdate {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        job_id: String,
        run_id: u64,
        owner: String,
        lease_epoch: u64,
        task_id: String,
        state: TransferTaskState,
        message: String,
        now_ms: i64,
    ) -> Self {
        Self {
            job_id,
            run_id,
            owner,
            lease_epoch,
            task_id,
            from_states: Vec::new(),
            state,
            message,
            now_ms,
            bump_retry: false,
        }
    }
}
