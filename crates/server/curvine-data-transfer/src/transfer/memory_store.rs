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

use super::SchedulingPriority;
use std::collections::{HashMap, HashSet};

use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::{
    StaleTaskAttempt, TaskAttemptStart, TransferJobRecord, TransferLease, TransferListFilter,
    TransferState, TransferStateUpdate, TransferTaskRecord, TransferTaskReport, TransferTaskState,
    TransferTenantSummary,
};
use parking_lot::Mutex;

use super::task_result::{task_report_write, TaskReportWrite};
use crate::transfer::{
    apply_task_report_progress, TransferPlannedTasks, TransferRequeueUpdate, TransferStore,
    TransferTaskStateUpdate,
};

#[derive(Default)]
pub struct MemoryTransferStore {
    inner: Mutex<MemoryTransferState>,
}

#[derive(Default)]
struct MemoryTransferState {
    jobs: HashMap<String, TransferJobRecord>,
    request_index: HashMap<(String, String), String>,
    tasks: HashMap<(String, u64, String), TransferTaskRecord>,
}

impl MemoryTransferStore {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub(crate) fn set_job_state(&self, job_id: &str, state: TransferState) {
        if let Some(job) = self.inner.lock().jobs.get_mut(job_id) {
            job.state = state;
        }
    }

    #[cfg(test)]
    pub(crate) fn job_count(&self) -> usize {
        self.inner.lock().jobs.len()
    }
}

impl TransferStore for MemoryTransferStore {
    fn check_available(&self) -> FsResult<()> {
        Ok(())
    }

    fn create_or_get_by_request_id(&self, job: TransferJobRecord) -> FsResult<TransferJobRecord> {
        self.create_or_get_by_request_id_checked(job)
    }

    fn create_or_get_by_request_id_checked(
        &self,
        job: TransferJobRecord,
    ) -> FsResult<TransferJobRecord> {
        let mut inner = self.inner.lock();
        let request_key = (job.submitter.clone(), job.client_request_id.clone());
        if let Some(job_id) = inner.request_index.get(&request_key) {
            let existing = inner
                .jobs
                .get(job_id)
                .cloned()
                .ok_or_else(|| FsError::job_not_found(job_id));
            let existing = existing?;
            if existing.command_json != job.command_json
                || existing.spec_version != job.spec_version
                || existing.spec_blob != job.spec_blob
            {
                return Err(FsError::common(format!(
                    "Transfer request ID {} submitted by {} is already bound to job {} with a different command",
                    job.client_request_id, job.submitter, existing.job_id
                )));
            }
            return Ok(existing);
        }

        if let Some(existing) = inner
            .jobs
            .values()
            .find(|existing| existing.job_key == job.job_key && !existing.state.is_terminal())
        {
            if existing.command_json == job.command_json
                && existing.spec_version == job.spec_version
                && existing.spec_blob == job.spec_blob
            {
                return Ok(existing.clone());
            }
            return Err(FsError::transfer_already_running(format!(
                "job_key {} has running job {} with different command",
                existing.job_key, existing.job_id
            )));
        }

        if let Some(existing) = inner.jobs.values().find(|existing| {
            !(existing.state.is_terminal()
                || (existing.submitter == job.submitter
                    && existing.client_request_id == job.client_request_id))
                && target_paths_conflict(&existing.target_path, &job.target_path)
        }) {
            return Err(FsError::transfer_target_conflict(format!(
                "target {} conflicts with active transfer {} target {}",
                job.target_path, existing.job_id, existing.target_path
            )));
        }

        inner.request_index.insert(request_key, job.job_id.clone());
        inner.jobs.insert(job.job_id.clone(), job.clone());
        Ok(job)
    }

    fn get_transfer(&self, job_id: &str) -> FsResult<Option<TransferJobRecord>> {
        Ok(self.inner.lock().jobs.get(job_id).cloned())
    }

    fn get_transfer_by_request(
        &self,
        submitter: &str,
        client_request_id: &str,
    ) -> FsResult<Option<TransferJobRecord>> {
        let inner = self.inner.lock();
        let Some(job_id) = inner
            .request_index
            .get(&(submitter.to_string(), client_request_id.to_string()))
        else {
            return Ok(None);
        };
        Ok(inner.jobs.get(job_id).cloned())
    }

    fn list_active_transfers(&self) -> FsResult<Vec<TransferJobRecord>> {
        Ok(self
            .inner
            .lock()
            .jobs
            .values()
            .filter(|job| !job.state.is_terminal())
            .cloned()
            .collect())
    }

    fn has_active_transfer_by_key(&self, job_key: &str) -> FsResult<bool> {
        Ok(self
            .inner
            .lock()
            .jobs
            .values()
            .any(|job| job.job_key == job_key && !job.state.is_terminal()))
    }

    fn find_conflicting_active_transfer(
        &self,
        target_path: &str,
        submitter: &str,
        client_request_id: &str,
    ) -> FsResult<Option<TransferJobRecord>> {
        Ok(self
            .inner
            .lock()
            .jobs
            .values()
            .find(|job| {
                !(job.state.is_terminal()
                    || (job.submitter == submitter && job.client_request_id == client_request_id))
                    && target_paths_conflict(target_path, &job.target_path)
            })
            .cloned())
    }

    fn count_active_transfers(&self) -> FsResult<u64> {
        Ok(self
            .inner
            .lock()
            .jobs
            .values()
            .filter(|job| !job.state.is_terminal())
            .count() as u64)
    }

    fn count_executing_transfers(&self) -> FsResult<u64> {
        Ok(self
            .inner
            .lock()
            .jobs
            .values()
            .filter(|job| job.state.is_executing())
            .count() as u64)
    }

    fn list_transfers(&self, filter: TransferListFilter) -> FsResult<Vec<TransferJobRecord>> {
        let mut jobs: Vec<_> = self
            .inner
            .lock()
            .jobs
            .values()
            .filter(|job| filter.kind.is_none_or(|kind| job.kind == kind))
            .filter(|job| filter.state.is_none_or(|state| job.state == state))
            .filter(|job| {
                filter
                    .submitter
                    .as_ref()
                    .is_none_or(|submitter| job.submitter == *submitter)
            })
            .filter(|job| {
                filter
                    .tenant
                    .as_ref()
                    .is_none_or(|tenant| job.tenant == *tenant)
            })
            .filter(|job| {
                filter.path.as_ref().is_none_or(|path| {
                    let path = path.to_lowercase();
                    job.target_path.to_lowercase().contains(&path)
                })
            })
            .cloned()
            .collect();
        jobs.sort_by(|left, right| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then_with(|| right.created_at.cmp(&left.created_at))
                .then_with(|| right.job_id.cmp(&left.job_id))
        });
        Ok(jobs
            .into_iter()
            .skip(filter.offset)
            .take(filter.limit)
            .collect())
    }

    fn list_tenant_summaries(
        &self,
        limit: usize,
        offset: usize,
    ) -> FsResult<Vec<TransferTenantSummary>> {
        let inner = self.inner.lock();
        let mut summaries = HashMap::<String, TransferTenantSummary>::new();
        for job in inner.jobs.values() {
            add_job_to_tenant_summary(&mut summaries, job);
        }
        Ok(sorted_tenant_summaries(summaries, limit, offset))
    }

    fn purge_terminal_transfers(&self, older_than_ms: i64, limit: usize) -> FsResult<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let mut inner = self.inner.lock();
        let job_ids = inner
            .jobs
            .values()
            .filter(|job| job.state.is_terminal() && job.updated_at < older_than_ms)
            .take(limit)
            .map(|job| job.job_id.clone())
            .collect::<Vec<_>>();
        for job_id in &job_ids {
            inner.jobs.remove(job_id);
            inner
                .request_index
                .retain(|_, indexed_job_id| indexed_job_id != job_id);
            inner
                .tasks
                .retain(|(task_job_id, _, _), _| task_job_id != job_id);
        }
        Ok(job_ids.len())
    }

    fn list_transfer_tasks(&self, job_id: &str, run_id: u64) -> FsResult<Vec<TransferTaskRecord>> {
        Ok(self
            .inner
            .lock()
            .tasks
            .values()
            .filter(|task| task.job_id == job_id && task.run_id == run_id)
            .cloned()
            .collect())
    }

    fn list_task_progress(&self, job_id: &str, run_id: u64) -> FsResult<Vec<TransferTaskRecord>> {
        let inner = self.inner.lock();
        Ok(inner
            .tasks
            .values()
            .filter(|t| t.job_id == job_id && t.run_id == run_id)
            .map(|t| TransferTaskRecord {
                job_id: t.job_id.clone(),
                run_id: t.run_id,
                task_id: t.task_id.clone(),
                attempt_id: t.attempt_id,
                source_path: t.source_path.clone(),
                target_path: t.target_path.clone(),
                worker_id: t.worker_id,
                worker_session_id: t.worker_session_id.clone(),
                source_read_plan_json: t.source_read_plan_json.clone(),
                report_target_json: t.report_target_json.clone(),
                state: t.state,
                progress: t.progress.clone(),
                retry_count: t.retry_count,
                attempt_started_at: t.attempt_started_at,
                last_report_at: t.last_report_at,
                stale_deadline_at: t.stale_deadline_at,
                updated_at: t.updated_at,
                spec_blob: t.spec_blob.clone(),
                result_json: String::new(),
            })
            .collect())
    }

    fn list_task_page(
        &self,
        job_id: &str,
        run_id: u64,
        offset: usize,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>> {
        let inner = self.inner.lock();
        let mut tasks: Vec<_> = inner
            .tasks
            .values()
            .filter(|t| t.job_id == job_id && t.run_id == run_id)
            .collect();
        tasks.sort_unstable_by(|a, b| a.task_id.cmp(&b.task_id));
        Ok(tasks
            .into_iter()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect())
    }

    fn request_cancel(&self, job_id: &str, run_id: u64, now_ms: i64) -> FsResult<bool> {
        let mut inner = self.inner.lock();
        let Some(job) = inner.jobs.get_mut(job_id) else {
            return Ok(false);
        };
        if job.run_id != run_id || job.state.is_terminal() {
            return Ok(false);
        }
        job.cancel_requested = true;
        job.state = TransferState::Canceling;
        job.summary.message = "cancel requested".to_string();
        job.summary.update_time = now_ms;
        job.updated_at = now_ms;
        Ok(true)
    }

    fn acquire_runnable_transfer(
        &self,
        owner: &str,
        lease_ms: i64,
        now_ms: i64,
        max_executing_transfers: u64,
    ) -> FsResult<Option<TransferLease>> {
        self.acquire_runnable_transfer_with_priority(
            owner,
            lease_ms,
            now_ms,
            max_executing_transfers,
            SchedulingPriority::Legacy,
        )
    }

    fn acquire_runnable_transfer_with_priority(
        &self,
        owner: &str,
        lease_ms: i64,
        now_ms: i64,
        max_executing_transfers: u64,
        priority: SchedulingPriority,
    ) -> FsResult<Option<TransferLease>> {
        let mut inner = self.inner.lock();
        // Collect the executing count and the set of tenants that already own
        // executing work in a single pass, so pending selection does not rescan
        // every job once per candidate.
        let mut executing = 0u64;
        let mut executing_tenants: HashSet<&str> = HashSet::new();
        for job in inner.jobs.values() {
            if job.state.is_executing() {
                executing = executing.saturating_add(1);
                executing_tenants.insert(job.tenant.as_str());
            }
        }
        let pending = || {
            (executing < max_executing_transfers)
                .then(|| {
                    inner
                        .jobs
                        .values()
                        .filter(|job| {
                            job.state == TransferState::Pending && job.lease_expire_at <= now_ms
                        })
                        .min_by_key(|job| {
                            (
                                u64::from(executing_tenants.contains(job.tenant.as_str())),
                                job.updated_at,
                                &job.job_id,
                            )
                        })
                        .map(|job| job.job_id.clone())
                })
                .flatten()
        };
        let owned = || {
            inner
                .jobs
                .values()
                .filter(|job| {
                    job.owner == owner
                        && matches!(job.state, TransferState::Running | TransferState::Canceling)
                        && job.lease_expire_at > now_ms
                        && job.updated_at < now_ms
                        && (priority == SchedulingPriority::Legacy
                            || job.state == TransferState::Canceling
                            || job.lease_expire_at
                                <= super::running_check_deadline(now_ms, lease_ms))
                })
                .min_by_key(|job| {
                    (
                        job.updated_at,
                        (priority != SchedulingPriority::Legacy).then_some(&job.job_id),
                    )
                })
                .map(|job| job.job_id.clone())
        };
        let expired = || {
            inner
                .jobs
                .values()
                .filter(|job| {
                    job.state.is_runnable()
                        && job.state != TransferState::Pending
                        && job.lease_expire_at <= now_ms
                })
                .min_by_key(|job| {
                    (
                        job.updated_at,
                        (priority != SchedulingPriority::Legacy).then_some(&job.job_id),
                    )
                })
                .map(|job| job.job_id.clone())
        };
        let candidate_id = match priority {
            SchedulingPriority::Legacy => owned().or_else(expired).or_else(pending),
            SchedulingPriority::Admission => pending().or_else(expired).or_else(owned),
            SchedulingPriority::Maintenance => expired().or_else(owned).or_else(pending),
        };
        let Some(candidate_id) = candidate_id else {
            return Ok(None);
        };
        let Some(job) = inner.jobs.get_mut(&candidate_id) else {
            return Ok(None);
        };

        if job.state == TransferState::Pending {
            job.state = TransferState::Planning;
            job.summary.message = "acquired for planning".to_string();
            job.summary.update_time = now_ms;
        }
        job.owner = owner.to_string();
        job.lease_epoch = job.lease_epoch.saturating_add(1);
        job.lease_expire_at = now_ms.saturating_add(lease_ms);
        job.updated_at = now_ms;
        Ok(Some(TransferLease {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            owner: job.owner.clone(),
            lease_epoch: job.lease_epoch,
        }))
    }

    fn renew_lease(
        &self,
        job_id: &str,
        run_id: u64,
        owner: &str,
        lease_epoch: u64,
        lease_ms: i64,
        now_ms: i64,
    ) -> FsResult<bool> {
        let mut inner = self.inner.lock();
        let Some(job) = inner.jobs.get_mut(job_id) else {
            return Ok(false);
        };
        if job.run_id != run_id
            || job.owner != owner
            || job.lease_epoch != lease_epoch
            || job.lease_expire_at <= now_ms
            || job.state.is_terminal()
        {
            return Ok(false);
        }

        job.lease_expire_at = now_ms.saturating_add(lease_ms);
        job.updated_at = now_ms;
        Ok(true)
    }

    fn update_transfer_state(&self, update: TransferStateUpdate) -> FsResult<bool> {
        let mut inner = self.inner.lock();
        let Some(job) = inner.jobs.get_mut(&update.job_id) else {
            return Ok(false);
        };
        if job.run_id != update.run_id
            || job.owner != update.owner
            || job.lease_epoch != update.lease_epoch
            || job.lease_expire_at <= update.now_ms
            || !update.from_states.contains(&job.state)
        {
            return Ok(false);
        }

        job.state = update.to_state;
        job.summary.message = update.message;
        job.updated_at = update.now_ms;
        Ok(true)
    }

    fn requeue_transfer(&self, update: TransferRequeueUpdate) -> FsResult<bool> {
        let mut inner = self.inner.lock();
        let Some(job) = inner.jobs.get_mut(&update.job_id) else {
            return Ok(false);
        };
        if job.run_id != update.run_id
            || job.owner != update.owner
            || job.lease_epoch != update.lease_epoch
            || job.lease_expire_at <= update.now_ms
            || job.state != TransferState::Planning
        {
            return Ok(false);
        }

        job.state = TransferState::Pending;
        job.owner.clear();
        job.lease_expire_at = update.next_attempt_at_ms;
        job.summary.message = update.message;
        job.summary.update_time = update.now_ms;
        job.updated_at = update.now_ms;
        Ok(true)
    }

    fn set_transfer_cv_metadata_epoch(
        &self,
        job_id: &str,
        run_id: u64,
        owner: &str,
        lease_epoch: u64,
        cv_metadata_epoch: u64,
        now_ms: i64,
    ) -> FsResult<bool> {
        let mut inner = self.inner.lock();
        let Some(job) = inner.jobs.get_mut(job_id) else {
            return Ok(false);
        };
        if job.run_id != run_id
            || job.owner != owner
            || job.lease_epoch != lease_epoch
            || job.lease_expire_at <= now_ms
            || job.state.is_terminal()
        {
            return Ok(false);
        }
        if job
            .cv_metadata_epoch
            .is_some_and(|existing| existing != cv_metadata_epoch)
        {
            return Ok(false);
        }

        job.cv_metadata_epoch = Some(cv_metadata_epoch);
        job.updated_at = now_ms;
        Ok(true)
    }

    fn insert_tasks(&self, tasks: Vec<TransferTaskRecord>) -> FsResult<()> {
        let mut inner = self.inner.lock();
        for task in tasks {
            let key = (task.job_id.clone(), task.run_id, task.task_id.clone());
            inner.tasks.entry(key).or_insert(task);
        }
        Ok(())
    }

    fn persist_planned_tasks(&self, update: TransferPlannedTasks) -> FsResult<bool> {
        let mut inner = self.inner.lock();
        let valid_owner = inner.jobs.get(&update.job_id).is_some_and(|job| {
            job.run_id == update.run_id
                && job.owner == update.owner
                && job.lease_epoch == update.lease_epoch
                && job.lease_expire_at > update.now_ms
                && job.state == TransferState::Planning
        });
        if !valid_owner {
            return Ok(false);
        }
        for task in update.tasks {
            let key = (task.job_id.clone(), task.run_id, task.task_id.clone());
            inner.tasks.entry(key).or_insert(task);
        }
        let job = inner
            .jobs
            .get_mut(&update.job_id)
            .ok_or_else(|| FsError::job_not_found(&update.job_id))?;
        job.state = TransferState::Dispatching;
        job.summary.total_files = update.total_files;
        job.summary.total_size = update.total_size;
        job.summary.loaded_files = 0;
        job.summary.skipped_files = update.skipped_files;
        job.summary.skipped_size = update.skipped_size;
        job.summary.message = update.message;
        job.summary.update_time = update.now_ms;
        job.updated_at = update.now_ms;
        Ok(true)
    }

    fn update_task_state(&self, update: TransferTaskStateUpdate) -> FsResult<bool> {
        let mut inner = self.inner.lock();
        let Some(job) = inner.jobs.get(&update.job_id) else {
            return Ok(false);
        };
        if job.run_id != update.run_id
            || job.owner != update.owner
            || job.lease_epoch != update.lease_epoch
            || job.lease_expire_at <= update.now_ms
            || job.state.is_terminal()
        {
            return Ok(false);
        }
        let key = (update.job_id, update.run_id, update.task_id);
        let Some(task) = inner.tasks.get_mut(&key) else {
            return Ok(false);
        };
        if !update.from_states.is_empty() && !update.from_states.contains(&task.state) {
            return Ok(false);
        }

        if update.state == TransferTaskState::Pending {
            task.result_json.clear();
        }
        task.state = update.state;
        task.progress.message = update.message;
        task.progress.update_time = update.now_ms;
        task.updated_at = update.now_ms;
        Ok(true)
    }

    fn claim_pending_tasks(
        &self,
        job_id: &str,
        run_id: u64,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>> {
        let inner = self.inner.lock();
        Ok(inner
            .tasks
            .values()
            .filter(|task| {
                task.job_id == job_id
                    && task.run_id == run_id
                    && task.state == TransferTaskState::Pending
            })
            .take(limit)
            .cloned()
            .collect())
    }

    fn mark_stale_attempts(
        &self,
        job_id: &str,
        run_id: u64,
        owner: &str,
        lease_epoch: u64,
        now_ms: i64,
        limit: usize,
    ) -> FsResult<Vec<StaleTaskAttempt>> {
        let mut inner = self.inner.lock();
        let Some(job) = inner.jobs.get(job_id) else {
            return Ok(Vec::new());
        };
        if job.run_id != run_id
            || job.owner != owner
            || job.lease_epoch != lease_epoch
            || job.lease_expire_at <= now_ms
        {
            return Ok(Vec::new());
        }

        let mut stale = Vec::new();
        for task in inner.tasks.values_mut().filter(|task| {
            task.job_id == job_id
                && task.run_id == run_id
                && task.state == TransferTaskState::Running
                && task.stale_deadline_at < now_ms
        }) {
            task.result_json.clear();
            task.state = TransferTaskState::Stale;
            task.retry_count = task.retry_count.saturating_add(1);
            task.progress.message = "task stale timeout".to_string();
            task.progress.update_time = now_ms;
            task.updated_at = now_ms;
            stale.push(StaleTaskAttempt { task: task.clone() });
            if stale.len() >= limit {
                break;
            }
        }
        Ok(stale)
    }

    fn list_stale_running_tasks(
        &self,
        job_id: &str,
        run_id: u64,
        stale_before_ms: i64,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>> {
        Ok(self
            .inner
            .lock()
            .tasks
            .values()
            .filter(|task| {
                task.job_id == job_id
                    && task.run_id == run_id
                    && task.state == TransferTaskState::Running
                    && task.stale_deadline_at < stale_before_ms
            })
            .take(limit)
            .cloned()
            .collect())
    }

    fn list_tasks_by_state(
        &self,
        job_id: &str,
        run_id: u64,
        state: TransferTaskState,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>> {
        let mut tasks: Vec<_> = self
            .inner
            .lock()
            .tasks
            .values()
            .filter(|task| task.job_id == job_id && task.run_id == run_id && task.state == state)
            .cloned()
            .collect();
        tasks.sort_by_key(|task| task.updated_at);
        tasks.truncate(limit);
        Ok(tasks)
    }

    fn has_failed_tasks(&self, job_id: &str, run_id: u64) -> FsResult<bool> {
        Ok(self.inner.lock().tasks.values().any(|task| {
            task.job_id == job_id
                && task.run_id == run_id
                && task.state == TransferTaskState::Failed
        }))
    }

    fn has_recoverable_tasks(&self, job_id: &str, run_id: u64) -> FsResult<bool> {
        Ok(self.inner.lock().tasks.values().any(|task| {
            task.job_id == job_id
                && task.run_id == run_id
                && matches!(
                    task.state,
                    TransferTaskState::Pending
                        | TransferTaskState::Running
                        | TransferTaskState::Stale
                )
        }))
    }

    fn start_task_attempt(&self, start: TaskAttemptStart) -> FsResult<bool> {
        let mut inner = self.inner.lock();
        let Some(job) = inner.jobs.get(&start.job_id) else {
            return Ok(false);
        };
        if job.run_id != start.run_id
            || job.owner != start.owner
            || job.lease_epoch != start.lease_epoch
            || job.lease_expire_at <= start.now_ms
            || job.cancel_requested
            || job.state.is_terminal()
        {
            return Ok(false);
        }
        let key = (start.job_id, start.run_id, start.task_id);
        let Some(task) = inner.tasks.get_mut(&key) else {
            return Ok(false);
        };
        if !matches!(
            task.state,
            TransferTaskState::Pending | TransferTaskState::Stale
        ) {
            return Ok(false);
        }

        task.result_json.clear();
        task.attempt_id = start.attempt_id;
        task.worker_id = start.worker_id;
        task.worker_session_id = start.worker_session_id;
        task.report_target_json = start.report_target_json;
        task.state = TransferTaskState::Running;
        task.attempt_started_at = start.now_ms;
        task.last_report_at = start.now_ms;
        task.stale_deadline_at = start.stale_deadline_at;
        task.updated_at = start.now_ms;
        Ok(true)
    }

    fn update_task_report(&self, report: TransferTaskReport) -> FsResult<bool> {
        let mut inner = self.inner.lock();
        if !inner
            .jobs
            .get(&report.job_id)
            .is_some_and(|job| job.run_id == report.run_id)
        {
            return Ok(false);
        }
        let key = (report.job_id.clone(), report.run_id, report.task_id.clone());
        let mut result_only = None;
        let (task_job_id, previous_progress) = {
            let Some(task) = inner.tasks.get_mut(&key) else {
                return Ok(false);
            };
            match task_report_write(task, &report)? {
                TaskReportWrite::Rejected => return Ok(false),
                TaskReportWrite::Duplicate => return Ok(true),
                TaskReportWrite::ResultOnly(result) => {
                    task.result_json = result;
                    result_only = Some(task.job_id.clone());
                }
                TaskReportWrite::Update(Some(result)) => task.result_json = result,
                TaskReportWrite::Update(None) => {}
            }

            let previous_progress = task.progress.clone();
            if result_only.is_none() {
                task.state = report.state;
                task.progress = report.progress.clone();
                task.last_report_at = report.now_ms;
                task.stale_deadline_at = report.stale_deadline_at;
                task.updated_at = report.now_ms;
            }
            (task.job_id.clone(), previous_progress)
        };

        if let Some(job_id) = result_only {
            let job = inner.jobs.get_mut(&job_id).expect("job checked above");
            job.updated_at = report.now_ms.max(job.updated_at.saturating_add(1));
            return Ok(true);
        }

        if let Some(job) = inner.jobs.get_mut(&task_job_id) {
            apply_task_report_progress(
                &mut job.summary,
                &previous_progress,
                &report.progress,
                report.now_ms,
            );
            if report.state == TransferTaskState::Completed {
                job.summary.loaded_files = job.summary.loaded_files.saturating_add(1);
            }
            job.updated_at = report.now_ms;
        }
        Ok(true)
    }
}

fn add_job_to_tenant_summary(
    summaries: &mut HashMap<String, TransferTenantSummary>,
    job: &TransferJobRecord,
) {
    let summary = summaries
        .entry(job.tenant.clone())
        .or_insert_with(|| TransferTenantSummary {
            tenant: job.tenant.clone(),
            ..Default::default()
        });
    summary.total = summary.total.saturating_add(1);
    match job.state {
        TransferState::Pending => summary.pending = summary.pending.saturating_add(1),
        TransferState::Planning
        | TransferState::Dispatching
        | TransferState::Running
        | TransferState::Canceling => summary.executing = summary.executing.saturating_add(1),
        TransferState::Completed => summary.completed = summary.completed.saturating_add(1),
        TransferState::Failed => summary.failed = summary.failed.saturating_add(1),
        TransferState::Canceled => summary.canceled = summary.canceled.saturating_add(1),
        TransferState::PartialSuccess => {
            summary.partial_success = summary.partial_success.saturating_add(1)
        }
    }
}

fn sorted_tenant_summaries(
    summaries: HashMap<String, TransferTenantSummary>,
    limit: usize,
    offset: usize,
) -> Vec<TransferTenantSummary> {
    let mut values = summaries.into_values().collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right
            .active()
            .cmp(&left.active())
            .then_with(|| right.total.cmp(&left.total))
            .then_with(|| left.tenant.cmp(&right.tenant))
    });
    values.into_iter().skip(offset).take(limit).collect()
}

fn target_paths_conflict(left: &str, right: &str) -> bool {
    let left = normalize_target_path(left);
    let right = normalize_target_path(right);
    if left == "/" || right == "/" {
        return true;
    }
    left == right
        || left
            .strip_prefix(&right)
            .is_some_and(|suffix| suffix.starts_with('/'))
        || right
            .strip_prefix(&left)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn normalize_target_path(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        trimmed.to_string()
    }
}
