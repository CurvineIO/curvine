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

#[cfg(feature = "transfer-store-postgres")]
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::{
    StaleTaskAttempt, TaskAttemptStart, TransferJobRecord, TransferLease, TransferListFilter,
    TransferStateUpdate, TransferTaskRecord, TransferTaskReport, TransferTaskState,
    TransferTenantSummary,
};

/// Selection policy only; all modes share the existing atomic claim and capacity checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulingPriority {
    /// Original owned -> expired -> Pending order, without a Running-check cooldown.
    Legacy,
    Admission,
    Maintenance,
}

// Until the JSON-backed stores are upgraded, older servers can drop fields when
// rewriting a record. Keep non-legacy records out of those stores in this stage.
#[cfg(feature = "transfer-store-postgres")]
pub(super) fn require_legacy_job(job: &TransferJobRecord) -> FsResult<()> {
    if job.spec_version != 0 || !job.spec_blob.is_empty() {
        return Err(FsError::common(
            "Transfer job specs are not enabled for this store",
        ));
    }
    Ok(())
}

#[cfg(feature = "transfer-store-postgres")]
pub(super) fn require_legacy_task(task: &TransferTaskRecord) -> FsResult<()> {
    if !task.spec_blob.is_empty() || !task.result_json.is_empty() {
        return Err(FsError::common(
            "Transfer task specs and results are not enabled for this store",
        ));
    }
    Ok(())
}

pub trait TransferStore: Send + Sync + 'static {
    fn check_available(&self) -> FsResult<()>;

    fn create_or_get_by_request_id(&self, job: TransferJobRecord) -> FsResult<TransferJobRecord>;

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

    fn has_active_transfer_by_key(&self, job_key: &str) -> FsResult<bool>;

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

    /// Task records for scheduler summaries, without requiring result payloads.
    fn list_task_progress(&self, job_id: &str, run_id: u64) -> FsResult<Vec<TransferTaskRecord>> {
        self.list_transfer_tasks(job_id, run_id)
    }

    /// Read only the requested task rows, ordered by task ID.
    fn list_task_page(
        &self,
        job_id: &str,
        run_id: u64,
        offset: usize,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>> {
        let mut tasks = self.list_transfer_tasks(job_id, run_id)?;
        tasks.sort_unstable_by(|a, b| a.task_id.cmp(&b.task_id));
        Ok(tasks.into_iter().skip(offset).take(limit).collect())
    }

    fn request_cancel(&self, job_id: &str, run_id: u64, now_ms: i64) -> FsResult<bool>;

    fn acquire_runnable_transfer(
        &self,
        owner: &str,
        lease_ms: i64,
        now_ms: i64,
        max_executing_transfers: u64,
    ) -> FsResult<Option<TransferLease>>;

    /// Select a policy without changing atomic claim or capacity checks.
    fn acquire_runnable_transfer_with_priority(
        &self,
        owner: &str,
        lease_ms: i64,
        now_ms: i64,
        max_executing_transfers: u64,
        priority: SchedulingPriority,
    ) -> FsResult<Option<TransferLease>> {
        let _ = priority;
        self.acquire_runnable_transfer(owner, lease_ms, now_ms, max_executing_transfers)
    }

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

    fn insert_tasks(&self, tasks: Vec<TransferTaskRecord>) -> FsResult<()>;

    fn persist_planned_tasks(&self, update: TransferPlannedTasks) -> FsResult<bool>;

    fn update_task_state(&self, update: TransferTaskStateUpdate) -> FsResult<bool>;

    fn claim_pending_tasks(
        &self,
        job_id: &str,
        run_id: u64,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>>;

    fn mark_stale_attempts(
        &self,
        job_id: &str,
        run_id: u64,
        owner: &str,
        lease_epoch: u64,
        now_ms: i64,
        limit: usize,
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

    /// Apply progress and any result atomically to the matching current attempt.
    /// A report without a result leaves that attempt's stored result unchanged.
    fn update_task_report(&self, report: TransferTaskReport) -> FsResult<bool>;
}

pub struct TransferRequeueUpdate {
    pub job_id: String,
    pub run_id: u64,
    pub owner: String,
    pub lease_epoch: u64,
    pub message: String,
    pub next_attempt_at_ms: i64,
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
}

/// Bound ordinary checks without tying them to progress reports (which refresh updated_at).
/// Short leases retain at least three quarters of their lifetime for renewal.
pub(crate) fn running_check_deadline(now_ms: i64, lease_ms: i64) -> i64 {
    let interval = (lease_ms / 4).clamp(1, 200);
    now_ms.saturating_add(lease_ms).saturating_sub(interval)
}

/// ACK a replay only when it is the same committed result from the same attempt.
/// In particular, a stale progress report cannot turn a terminal task back into Running.
pub(crate) fn is_duplicate_terminal_report(
    task: &TransferTaskRecord,
    report: &TransferTaskReport,
) -> bool {
    task.job_id == report.job_id
        && task.run_id == report.run_id
        && task.task_id == report.task_id
        && task.attempt_id == report.attempt_id
        && task.worker_id == report.worker_id
        && task.worker_session_id == report.worker_session_id
        && task.state.is_terminal()
        && task.state == report.state
        && task.progress == report.progress
}
