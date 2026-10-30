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

use std::collections::{HashMap, HashSet};

use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::{
    summarize_transfer_tasks, StaleTaskAttempt, TaskAttemptStart, TransferCommand,
    TransferCountFilter, TransferJobRecord, TransferLease, TransferListFilter, TransferProgress,
    TransferState, TransferStateCounts, TransferStateUpdate, TransferTaskCounts,
    TransferTaskRecord, TransferTaskReport, TransferTaskState, TransferTaskStatus,
    TransferTenantSummary, WorkerInflight,
};
use parking_lot::Mutex;

use crate::transfer::{
    ensure_task_transaction_bound, TransferMountSnapshotUpdate, TransferPlannedTasks,
    TransferRequeueUpdate, TransferStore, TransferTaskStateUpdate, MAX_JOBS_PER_STORE_TRANSACTION,
    MAX_TASKS_PER_STORE_TRANSACTION,
};

fn running_job_needs_scheduler(
    state: &MemoryTransferState,
    job: &TransferJobRecord,
    now_ms: i64,
) -> bool {
    let mut has_running = false;
    for task in state
        .tasks
        .values()
        .filter(|task| task.job_id == job.job_id && task.run_id == job.run_id)
    {
        match task.state {
            TransferTaskState::Pending | TransferTaskState::Stale | TransferTaskState::Failed => {
                return true;
            }
            TransferTaskState::Running => {
                if task.stale_deadline_at < now_ms {
                    return true;
                }
                has_running = true;
            }
            TransferTaskState::Completed | TransferTaskState::Canceled => {}
        }
    }
    !has_running
}

#[derive(Default)]
pub struct MemoryTransferStore {
    inner: Mutex<MemoryTransferState>,
}

#[derive(Default)]
struct MemoryTransferState {
    jobs: HashMap<String, TransferJobRecord>,
    request_index: HashMap<(String, String), String>,
    tasks: HashMap<(String, u64, String), TransferTaskRecord>,
    cleanup_lease: Option<(String, i64)>,
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
            if existing.command_json != job.command_json {
                return Err(FsError::common(format!(
                    "Transfer request ID {} submitted by {} is already bound to job {} with a different command",
                    job.client_request_id, job.submitter, existing.job_id
                )));
            }
            return Ok(existing);
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
        // fs_mode jobs target one file per entry and order themselves by
        // generation checks; an overlapping target from someone else is not a
        // conflict for them.
        if TransferCommand::is_fs_mode_submitter(submitter) {
            return Ok(None);
        }
        Ok(self
            .inner
            .lock()
            .jobs
            .values()
            .find(|job| {
                job.state.is_executing()
                    && !TransferCommand::is_fs_mode_submitter(&job.submitter)
                    && !(job.submitter == submitter && job.client_request_id == client_request_id)
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
            .filter(|job| filter.matches_state(job.state))
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

    fn count_transfers_by_state(
        &self,
        filter: &TransferCountFilter,
    ) -> FsResult<TransferStateCounts> {
        let mut counts = TransferStateCounts::default();
        for job in self.inner.lock().jobs.values() {
            if filter.matches(job) {
                counts.add(job.state, 1);
            }
        }
        Ok(counts)
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
        let _ =
            self.purge_terminal_tasks(older_than_ms, limit.min(MAX_TASKS_PER_STORE_TRANSACTION))?;
        self.purge_terminal_jobs(older_than_ms, limit.min(MAX_JOBS_PER_STORE_TRANSACTION))
    }

    fn purge_terminal_tasks(&self, older_than_ms: i64, task_limit: usize) -> FsResult<usize> {
        if task_limit == 0 {
            return Ok(0);
        }
        let task_limit = task_limit.min(MAX_TASKS_PER_STORE_TRANSACTION);
        let mut inner = self.inner.lock();
        // Never delete task rows without a retained terminal snapshot (C1).
        // Jobs without a snapshot keep their rows until a snapshot lands, so
        // TTL can never render detail as 0/empty.
        let mut targets = inner
            .jobs
            .values()
            .filter(|job| {
                job.state.is_terminal()
                    && job.updated_at < older_than_ms
                    && job.terminal_summary.is_some()
                    && job.terminal_counts.is_some()
            })
            .map(|job| (job.updated_at, job.job_id.clone(), job.run_id))
            .collect::<Vec<_>>();
        targets.sort();
        let mut keys = Vec::new();
        for (_, job_id, run_id) in targets {
            let mut task_ids = inner
                .tasks
                .keys()
                .filter(|(task_job_id, task_run_id, _)| {
                    *task_job_id == job_id && *task_run_id == run_id
                })
                .cloned()
                .collect::<Vec<_>>();
            task_ids.sort();
            for key in task_ids {
                keys.push(key);
                if keys.len() >= task_limit {
                    break;
                }
            }
            if keys.len() >= task_limit {
                break;
            }
        }
        let deleted = keys.len();
        for key in keys {
            inner.tasks.remove(&key);
        }
        Ok(deleted)
    }

    fn purge_terminal_tasks_with_cleanup_lease(
        &self,
        older_than_ms: i64,
        task_limit: usize,
        owner: &str,
        now_ms: i64,
    ) -> FsResult<usize> {
        if task_limit == 0 {
            return Ok(0);
        }
        let task_limit = task_limit.min(MAX_TASKS_PER_STORE_TRANSACTION);
        let mut inner = self.inner.lock();
        let lease_owned = inner
            .cleanup_lease
            .as_ref()
            .is_some_and(|(lease_owner, expires_at)| lease_owner == owner && *expires_at > now_ms);
        if !lease_owned {
            return Ok(0);
        }
        let mut targets = inner
            .jobs
            .values()
            .filter(|job| {
                job.state.is_terminal()
                    && job.updated_at < older_than_ms
                    && job.terminal_summary.is_some()
                    && job.terminal_counts.is_some()
            })
            .map(|job| (job.updated_at, job.job_id.clone(), job.run_id))
            .collect::<Vec<_>>();
        targets.sort();
        let mut keys = Vec::with_capacity(task_limit);
        for (_, job_id, run_id) in targets {
            let mut task_ids = inner
                .tasks
                .keys()
                .filter(|(task_job_id, task_run_id, _)| {
                    *task_job_id == job_id && *task_run_id == run_id
                })
                .cloned()
                .collect::<Vec<_>>();
            task_ids.sort();
            for key in task_ids {
                keys.push(key);
                if keys.len() >= task_limit {
                    break;
                }
            }
            if keys.len() >= task_limit {
                break;
            }
        }
        let deleted = keys.len();
        for key in keys {
            inner.tasks.remove(&key);
        }
        Ok(deleted)
    }

    fn purge_terminal_jobs(&self, older_than_ms: i64, limit: usize) -> FsResult<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let limit = limit.min(MAX_JOBS_PER_STORE_TRANSACTION);
        let mut inner = self.inner.lock();
        let mut targets = inner
            .jobs
            .values()
            .filter(|job| job.state.is_terminal() && job.updated_at < older_than_ms)
            .filter(|job| {
                !inner.tasks.keys().any(|(task_job_id, task_run_id, _)| {
                    *task_job_id == job.job_id && *task_run_id == job.run_id
                })
            })
            .map(|job| (job.updated_at, job.job_id.clone()))
            .collect::<Vec<_>>();
        targets.sort();
        targets.truncate(limit);
        let deleted = targets.len();
        for (_, job_id) in targets {
            inner.jobs.remove(&job_id);
            inner
                .request_index
                .retain(|_, indexed_job_id| indexed_job_id != &job_id);
        }
        Ok(deleted)
    }

    fn purge_terminal_jobs_with_cleanup_lease(
        &self,
        older_than_ms: i64,
        limit: usize,
        owner: &str,
        now_ms: i64,
    ) -> FsResult<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let limit = limit.min(MAX_JOBS_PER_STORE_TRANSACTION);
        let mut inner = self.inner.lock();
        let lease_owned = inner
            .cleanup_lease
            .as_ref()
            .is_some_and(|(lease_owner, expires_at)| lease_owner == owner && *expires_at > now_ms);
        if !lease_owned {
            return Ok(0);
        }
        let mut targets = inner
            .jobs
            .values()
            .filter(|job| job.state.is_terminal() && job.updated_at < older_than_ms)
            .filter(|job| {
                !inner.tasks.keys().any(|(task_job_id, task_run_id, _)| {
                    *task_job_id == job.job_id && *task_run_id == job.run_id
                })
            })
            .map(|job| (job.updated_at, job.job_id.clone()))
            .collect::<Vec<_>>();
        targets.sort();
        targets.truncate(limit);
        let deleted = targets.len();
        for (_, job_id) in targets {
            inner.jobs.remove(&job_id);
            inner
                .request_index
                .retain(|_, indexed_job_id| indexed_job_id != &job_id);
        }
        Ok(deleted)
    }

    fn try_acquire_cleanup_lease(&self, owner: &str, now_ms: i64, lease_ms: i64) -> FsResult<bool> {
        let mut inner = self.inner.lock();
        let expires_at = now_ms.saturating_add(lease_ms.max(1));
        match &inner.cleanup_lease {
            Some((current_owner, expiry)) if current_owner != owner && *expiry > now_ms => {
                Ok(false)
            }
            _ => {
                inner.cleanup_lease = Some((owner.to_string(), expires_at));
                Ok(true)
            }
        }
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

    fn get_transfer_task_summary(
        &self,
        job_id: &str,
        run_id: u64,
        now_ms: i64,
    ) -> FsResult<curvine_model::TransferTaskSummary> {
        let inner = self.inner.lock();
        Ok(summarize_transfer_tasks(
            inner
                .tasks
                .values()
                .filter(|task| task.job_id == job_id && task.run_id == run_id),
            now_ms,
        ))
    }

    fn list_transfer_task_status_page(
        &self,
        job_id: &str,
        run_id: u64,
        offset: usize,
        limit: usize,
    ) -> FsResult<(Vec<TransferTaskStatus>, bool)> {
        let inner = self.inner.lock();
        let mut tasks = inner
            .tasks
            .values()
            .filter(|task| task.job_id == job_id && task.run_id == run_id)
            .collect::<Vec<_>>();
        tasks.sort_by(|left, right| left.task_id.cmp(&right.task_id));
        let mut page = tasks
            .into_iter()
            .skip(offset)
            .take(limit.saturating_add(1))
            .map(TransferTaskStatus::from)
            .collect::<Vec<_>>();
        let has_next = page.len() > limit;
        page.truncate(limit);
        Ok((page, has_next))
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
        prefer_pending: bool,
    ) -> FsResult<Option<TransferLease>> {
        let mut inner = self.inner.lock();
        let running_check_interval_ms =
            crate::transfer::running_transfer_check_interval_ms(lease_ms);
        let running_event_lease_before_ms =
            now_ms.saturating_add(lease_ms.saturating_sub(running_check_interval_ms));
        let running_renew_lease_before_ms = now_ms.saturating_add(lease_ms / 2);
        let candidate_id = if !prefer_pending {
            inner
                .jobs
                .values()
                .filter(|job| {
                    job.owner == owner
                        && job.lease_expire_at > now_ms
                        && ((job.state == TransferState::Canceling && job.updated_at < now_ms)
                            || (job.state == TransferState::Running
                                && (job.lease_expire_at <= running_renew_lease_before_ms
                                    || (job.lease_expire_at <= running_event_lease_before_ms
                                        && running_job_needs_scheduler(&inner, job, now_ms)))))
                })
                .min_by_key(|job| job.updated_at)
                .map(|job| job.job_id.clone())
        } else {
            None
        };
        let candidate_id =
            candidate_id
                .or_else(|| {
                    inner
                        .jobs
                        .values()
                        .filter(|job| {
                            job.state.is_runnable()
                                && job.state != TransferState::Pending
                                && job.lease_expire_at <= now_ms
                        })
                        .min_by_key(|job| job.updated_at)
                        .map(|job| job.job_id.clone())
                })
                .or_else(|| {
                    if !inner.jobs.values().any(|job| {
                        job.state == TransferState::Pending && job.lease_expire_at <= now_ms
                    }) {
                        return None;
                    }
                    let executing = inner
                        .jobs
                        .values()
                        .filter(|job| job.state.is_executing())
                        .collect::<Vec<_>>();
                    // Every executing job takes a slot; fs_mode jobs only skip
                    // the target-overlap check, like in the SQL stores.
                    if executing.len() as u64 >= max_executing_transfers {
                        return None;
                    }
                    let executing_targets = executing
                        .iter()
                        .filter(|job| !TransferCommand::is_fs_mode_submitter(&job.submitter))
                        .map(|job| job.target_path.as_str())
                        .collect::<Vec<_>>();
                    let executing_tenants = inner
                        .jobs
                        .values()
                        .filter(|job| job.state.is_executing())
                        .map(|job| job.tenant.as_str())
                        .collect::<HashSet<_>>();
                    inner
                        .jobs
                        .values()
                        .filter(|job| {
                            job.state == TransferState::Pending
                                && job.lease_expire_at <= now_ms
                                && (TransferCommand::is_fs_mode_submitter(&job.submitter)
                                    || !executing_targets.iter().any(|target_path| {
                                        target_paths_conflict(&job.target_path, target_path)
                                    }))
                        })
                        .min_by_key(|job| {
                            let tenant_pressure =
                                u64::from(executing_tenants.contains(job.tenant.as_str()));
                            (tenant_pressure, job.updated_at, job.job_id.clone())
                        })
                        .map(|job| job.job_id.clone())
                });
        let Some(candidate_id) = candidate_id else {
            return Ok(None);
        };

        let conflict = inner
            .jobs
            .get(&candidate_id)
            .filter(|candidate| candidate.state != TransferState::Pending)
            .and_then(|candidate| {
                if TransferCommand::is_fs_mode_submitter(&candidate.submitter) {
                    return None;
                }
                inner
                    .jobs
                    .values()
                    .find(|existing| {
                        existing.job_id != candidate.job_id
                            && existing.state.is_executing()
                            && !TransferCommand::is_fs_mode_submitter(&existing.submitter)
                            && target_paths_conflict(&candidate.target_path, &existing.target_path)
                    })
                    .cloned()
            });
        if let Some(existing) = conflict {
            let job = inner
                .jobs
                .get_mut(&candidate_id)
                .ok_or_else(|| FsError::job_not_found(&candidate_id))?;
            job.state = TransferState::Failed;
            job.summary.message = format!(
                "target {} conflicts with active transfer {} target {}",
                job.target_path, existing.job_id, existing.target_path
            );
            job.summary.update_time = now_ms;
            job.updated_at = now_ms;
            return Ok(None);
        }

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
        if let Some(mount_snapshot_attempts) = update.mount_snapshot_attempts {
            job.mount_snapshot_attempts = mount_snapshot_attempts;
        }
        if let Some(planning_attempts) = update.planning_attempts {
            job.planning_attempts = planning_attempts;
        }
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

    fn set_transfer_mount_snapshot(&self, update: TransferMountSnapshotUpdate) -> FsResult<bool> {
        let mut inner = self.inner.lock();
        let Some(job) = inner.jobs.get_mut(&update.job_id) else {
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
        if !job.mount_snapshot_json.trim().is_empty() && job.mount_snapshot_json != "{}" {
            return Ok(job.mount_snapshot_json == update.mount_snapshot_json);
        }
        job.mount_snapshot_json = update.mount_snapshot_json;
        job.cluster_snapshot_version = update.cluster_snapshot_version;
        job.updated_at = update.now_ms;
        Ok(true)
    }

    fn insert_tasks(&self, tasks: Vec<TransferTaskRecord>) -> FsResult<()> {
        ensure_task_transaction_bound(tasks.len())?;
        let mut inner = self.inner.lock();
        for task in tasks {
            let key = (task.job_id.clone(), task.run_id, task.task_id.clone());
            inner.tasks.entry(key).or_insert(task);
        }
        Ok(())
    }

    fn append_planned_tasks(&self, update: TransferPlannedTasks) -> FsResult<bool> {
        ensure_task_transaction_bound(update.tasks.len())?;
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
        Ok(true)
    }

    fn persist_planned_tasks(&self, update: TransferPlannedTasks) -> FsResult<bool> {
        ensure_task_transaction_bound(update.tasks.len())?;
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
        job.summary.message = update.message;
        job.summary.update_time = update.now_ms;
        job.updated_at = update.now_ms;
        Ok(true)
    }

    fn has_transfer_tasks(&self, job_id: &str, run_id: u64) -> FsResult<bool> {
        Ok(self
            .inner
            .lock()
            .tasks
            .keys()
            .any(|(task_job_id, task_run_id, _)| task_job_id == job_id && *task_run_id == run_id))
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

    fn select_inflight_by_worker(&self) -> FsResult<Vec<WorkerInflight>> {
        let inner = self.inner.lock();
        let mut by_worker: std::collections::BTreeMap<u32, WorkerInflight> = Default::default();
        for task in inner.tasks.values() {
            if task.state != TransferTaskState::Running {
                continue;
            }
            let entry = by_worker
                .entry(task.worker_id)
                .or_insert_with(|| WorkerInflight {
                    worker_id: task.worker_id,
                    task_count: 0,
                    estimated_bytes: 0,
                    unknown_bytes_count: 0,
                });
            entry.task_count += 1;
            let bytes = if task.estimated_size > 0 {
                task.estimated_size
            } else {
                task.progress.total_size
            };
            if bytes <= 0 {
                entry.unknown_bytes_count += 1;
            }
            entry.estimated_bytes += bytes;
        }
        Ok(by_worker.into_values().collect())
    }

    fn mark_stale_attempts(
        &self,
        job_id: &str,
        run_id: u64,
        owner: &str,
        lease_epoch: u64,
        now_ms: i64,
        candidates: Vec<TransferTaskRecord>,
    ) -> FsResult<Vec<StaleTaskAttempt>> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        ensure_task_transaction_bound(candidates.len())?;
        let mut inner = self.inner.lock();
        let owner_valid = inner.jobs.get(job_id).is_some_and(|job| {
            job.run_id == run_id
                && job.owner == owner
                && job.lease_epoch == lease_epoch
                && job.lease_expire_at > now_ms
        });
        if !owner_valid {
            return Ok(Vec::new());
        }

        let mut stale = Vec::new();
        for candidate in candidates {
            if candidate.job_id != job_id
                || candidate.run_id != run_id
                || candidate.state != TransferTaskState::Running
                || candidate.stale_deadline_at >= now_ms
            {
                continue;
            }
            let key = (job_id.to_string(), run_id, candidate.task_id.clone());
            let Some(task) = inner.tasks.get_mut(&key) else {
                continue;
            };
            if task.state != TransferTaskState::Running
                || task.attempt_id != candidate.attempt_id
                || task.worker_id != candidate.worker_id
                || task.worker_session_id != candidate.worker_session_id
                || task.stale_deadline_at != candidate.stale_deadline_at
            {
                continue;
            }
            task.state = TransferTaskState::Stale;
            task.retry_count = task.retry_count.saturating_add(1);
            task.progress.message = candidate.progress.message.clone();
            task.progress.update_time = now_ms;
            task.updated_at = now_ms;
            stale.push(StaleTaskAttempt { task: task.clone() });
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

    fn list_failed_tasks(&self, job_id: &str, run_id: u64) -> FsResult<Vec<TransferTaskRecord>> {
        let mut tasks: Vec<_> = self
            .inner
            .lock()
            .tasks
            .values()
            .filter(|task| {
                task.job_id == job_id
                    && task.run_id == run_id
                    && task.state == TransferTaskState::Failed
            })
            .cloned()
            .collect();
        tasks.sort_by(|left, right| left.task_id.cmp(&right.task_id));
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

        task.attempt_id = start.attempt_id;
        task.worker_id = start.worker_id;
        task.worker_session_id = start.worker_session_id;
        task.report_target_json = start.report_target_json;
        task.state = TransferTaskState::Running;
        task.attempt_started_at = start.now_ms;
        task.last_report_at = start.now_ms;
        task.stale_deadline_at = start.stale_deadline_at;
        task.updated_at = start.now_ms;
        task.progress.loaded_size = start
            .resume_loaded_size
            .clamp(0, task.progress.total_size.max(0));
        task.progress.update_time = start.now_ms;
        Ok(true)
    }

    fn update_task_report(&self, report: TransferTaskReport) -> FsResult<bool> {
        let mut inner = self.inner.lock();
        let key = (report.job_id, report.run_id, report.task_id);
        let Some(task) = inner.tasks.get_mut(&key) else {
            return Ok(false);
        };
        if task.attempt_id != report.attempt_id
            || task.worker_id != report.worker_id
            || task.worker_session_id != report.worker_session_id
            || !task.state.is_running()
        {
            return Ok(false);
        }

        task.state = report.state;
        let reported = report.progress.loaded_size.max(0);
        match report.state {
            TransferTaskState::Running => {
                task.progress.loaded_size = task.progress.loaded_size.max(0).max(reported);
                task.progress.total_size = report.progress.total_size;
                task.progress.update_time = report.progress.update_time;
                task.progress.message = report.progress.message.clone();
            }
            _ => {
                task.progress = report.progress.clone();
            }
        }
        task.last_report_at = report.now_ms;
        task.stale_deadline_at = report.stale_deadline_at;
        task.updated_at = report.now_ms;
        task.accepted_at_ms = report.accepted_at_ms;
        task.first_byte_at_ms = report.first_byte_at_ms;
        if report.state != TransferTaskState::Running {
            task.block_locations = report.block_locations.clone();
        }
        Ok(true)
    }

    fn get_task_attempt_started_at(
        &self,
        job_id: &str,
        run_id: u64,
        task_id: &str,
    ) -> FsResult<Option<i64>> {
        let inner = self.inner.lock();
        Ok(inner
            .tasks
            .get(&(job_id.to_string(), run_id, task_id.to_string()))
            .map(|task| task.attempt_started_at)
            .filter(|started| *started > 0))
    }

    fn update_job_progress_summary(
        &self,
        job_id: &str,
        run_id: u64,
        summary: TransferProgress,
        now_ms: i64,
    ) -> FsResult<()> {
        let mut inner = self.inner.lock();
        let Some(job) = inner.jobs.get_mut(job_id) else {
            return Ok(());
        };
        if job.run_id != run_id {
            return Ok(());
        }
        job.summary = summary;
        job.updated_at = now_ms;
        Ok(())
    }

    fn update_terminal_snapshot(
        &self,
        job_id: &str,
        run_id: u64,
        summary: TransferProgress,
        counts: TransferTaskCounts,
        now_ms: i64,
    ) -> FsResult<()> {
        let mut inner = self.inner.lock();
        let Some(job) = inner.jobs.get_mut(job_id) else {
            return Ok(());
        };
        if job.run_id != run_id {
            return Ok(());
        }
        if job.terminal_summary.is_none() {
            job.terminal_summary = Some(summary.clone());
            job.terminal_counts = Some(counts);
        }
        job.summary = summary;
        job.updated_at = now_ms;
        Ok(())
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
    let left = normalized_target_path(left);
    let right = normalized_target_path(right);
    if left == "/" || right == "/" {
        return true;
    }
    left == right
        || left
            .strip_prefix(right)
            .is_some_and(|suffix| suffix.starts_with('/'))
        || right
            .strip_prefix(left)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn normalized_target_path(path: &str) -> &str {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        "/"
    } else {
        trimmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transfer::{TransferPlannedTasks, TransferStore};
    use curvine_model::{
        TaskAttemptStart, TransferKind, TransferProgress, TransferState, TransferTaskReport,
        TransferTaskState,
    };

    fn sample_job(job_id: &str) -> TransferJobRecord {
        TransferJobRecord {
            job_key: format!("key-{job_id}"),
            job_id: job_id.to_string(),
            run_id: 1,
            kind: TransferKind::Load,
            source_path: "file:///src".to_string(),
            target_path: format!("/dst/{job_id}"),
            command_json: "{}".to_string(),
            mount_snapshot_json: "{}".to_string(),
            secret_ref_json: "{}".to_string(),
            cluster_snapshot_version: 1,
            mount_snapshot_attempts: 0,
            planning_attempts: 0,
            terminal_summary: None,
            terminal_counts: None,
            cv_metadata_epoch: None,
            state: TransferState::Pending,
            owner: String::new(),
            lease_epoch: 0,
            lease_expire_at: 0,
            cancel_requested: false,
            summary: TransferProgress::default(),
            client_request_id: format!("req-{job_id}"),
            submitter: "test".to_string(),
            tenant: "test".to_string(),
            created_at: 1,
            updated_at: 1,
        }
    }

    fn sample_task(job: &TransferJobRecord, task_id: &str) -> TransferTaskRecord {
        TransferTaskRecord {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            task_id: task_id.to_string(),
            attempt_id: 0,
            source_path: job.source_path.clone(),
            target_path: job.target_path.clone(),
            worker_id: 0,
            worker_session_id: String::new(),
            source_read_plan_json: "{}".to_string(),
            report_target_json: "{}".to_string(),
            state: TransferTaskState::Pending,
            progress: TransferProgress::default(),
            estimated_size: 0,
            retry_count: 0,
            attempt_started_at: 0,
            last_report_at: 0,
            stale_deadline_at: 0,
            updated_at: 1,
            accepted_at_ms: 0,
            first_byte_at_ms: 0,
            block_locations: Vec::new(),
            replace_target: false,
        }
    }

    fn running_store(task_state: TransferTaskState) -> (MemoryTransferStore, TransferJobRecord) {
        let store = MemoryTransferStore::new();
        let job = sample_job("running-check");
        store
            .create_or_get_by_request_id_checked(job.clone())
            .unwrap();
        let mut task = sample_task(&job, "t1");
        task.state = task_state;
        task.stale_deadline_at = 60_001;

        let mut inner = store.inner.lock();
        let record = inner.jobs.get_mut(&job.job_id).unwrap();
        record.state = TransferState::Running;
        record.owner = "owner".to_string();
        record.lease_epoch = 1;
        record.lease_expire_at = 60_000;
        inner
            .tasks
            .insert((job.job_id.clone(), job.run_id, task.task_id.clone()), task);
        drop(inner);
        (store, job)
    }

    #[test]
    fn healthy_running_task_is_reacquired_only_for_lease_renewal() {
        let (store, _) = running_store(TransferTaskState::Running);

        assert!(store
            .acquire_runnable_transfer("owner", 60_000, 29_999, 10, false)
            .unwrap()
            .is_none());
        let lease = store
            .acquire_runnable_transfer("owner", 60_000, 30_000, 10, false)
            .unwrap()
            .unwrap();
        assert_eq!(lease.lease_epoch, 2);
    }

    #[test]
    fn completed_running_job_is_reacquired_after_check_interval() {
        let (store, _) = running_store(TransferTaskState::Completed);

        assert!(store
            .acquire_runnable_transfer("owner", 60_000, 4_999, 10, false)
            .unwrap()
            .is_none());
        assert!(store
            .acquire_runnable_transfer("owner", 60_000, 5_000, 10, false)
            .unwrap()
            .is_some());
    }

    #[test]
    fn progress_report_updates_task_without_job_summary_until_flush() {
        let store = MemoryTransferStore::new();
        let job = sample_job("progress-decouple");
        store
            .create_or_get_by_request_id_checked(job.clone())
            .unwrap();
        let lease = store
            .acquire_runnable_transfer("owner", 60_000, 2, 10, false)
            .unwrap()
            .unwrap();
        assert!(store
            .persist_planned_tasks(TransferPlannedTasks {
                job_id: job.job_id.clone(),
                run_id: job.run_id,
                owner: lease.owner.clone(),
                lease_epoch: lease.lease_epoch,
                tasks: vec![sample_task(&job, "t1"), sample_task(&job, "t2")],
                message: "planned".to_string(),
                now_ms: 3,
            })
            .unwrap());
        for task_id in ["t1", "t2"] {
            assert!(store
                .start_task_attempt(TaskAttemptStart {
                    job_id: job.job_id.clone(),
                    run_id: job.run_id,
                    owner: lease.owner.clone(),
                    lease_epoch: lease.lease_epoch,
                    task_id: task_id.to_string(),
                    attempt_id: 1,
                    worker_id: 7,
                    worker_session_id: "session".to_string(),
                    report_target_json: "{}".to_string(),
                    now_ms: 4,
                    stale_deadline_at: 1_000,
                    resume_loaded_size: 0,
                })
                .unwrap());
        }

        assert!(store
            .update_task_report(TransferTaskReport {
                job_id: job.job_id.clone(),
                run_id: job.run_id,
                task_id: "t1".to_string(),
                attempt_id: 1,
                worker_id: 7,
                worker_session_id: "session".to_string(),
                state: TransferTaskState::Running,
                progress: TransferProgress {
                    loaded_size: 10,
                    total_size: 100,
                    update_time: 5,
                    message: "t1".to_string(),
                },
                now_ms: 5,
                stale_deadline_at: 2_000,
                accepted_at_ms: 0,
                first_byte_at_ms: 0,
                block_locations: Vec::new(),
            })
            .unwrap());
        assert!(store
            .update_task_report(TransferTaskReport {
                job_id: job.job_id.clone(),
                run_id: job.run_id,
                task_id: "t2".to_string(),
                attempt_id: 1,
                worker_id: 7,
                worker_session_id: "session".to_string(),
                state: TransferTaskState::Running,
                progress: TransferProgress {
                    loaded_size: 20,
                    total_size: 100,
                    update_time: 6,
                    message: "t2".to_string(),
                },
                now_ms: 6,
                stale_deadline_at: 3_000,
                accepted_at_ms: 0,
                first_byte_at_ms: 0,
                block_locations: Vec::new(),
            })
            .unwrap());

        let tasks = store.list_transfer_tasks(&job.job_id, job.run_id).unwrap();
        assert_eq!(
            tasks
                .iter()
                .find(|task| task.task_id == "t1")
                .unwrap()
                .stale_deadline_at,
            2_000
        );
        assert_eq!(
            tasks
                .iter()
                .find(|task| task.task_id == "t2")
                .unwrap()
                .progress
                .loaded_size,
            20
        );
        assert_eq!(
            store
                .get_transfer(&job.job_id)
                .unwrap()
                .unwrap()
                .summary
                .loaded_size,
            0
        );

        store
            .flush_job_progress_summary(&job.job_id, job.run_id)
            .unwrap();
        assert_eq!(
            store
                .get_transfer(&job.job_id)
                .unwrap()
                .unwrap()
                .summary
                .loaded_size,
            30
        );

        assert!(store
            .update_task_report(TransferTaskReport {
                job_id: job.job_id.clone(),
                run_id: job.run_id,
                task_id: "t1".to_string(),
                attempt_id: 1,
                worker_id: 7,
                worker_session_id: "session".to_string(),
                state: TransferTaskState::Completed,
                progress: TransferProgress {
                    loaded_size: 100,
                    total_size: 100,
                    update_time: 7,
                    message: "done".to_string(),
                },
                now_ms: 7,
                stale_deadline_at: 4_000,
                accepted_at_ms: 0,
                first_byte_at_ms: 0,
                block_locations: Vec::new(),
            })
            .unwrap());
        assert_eq!(
            store
                .get_transfer(&job.job_id)
                .unwrap()
                .unwrap()
                .summary
                .loaded_size,
            30,
            "terminal reports must not update job.summary until flush"
        );
        store
            .flush_job_progress_summary(&job.job_id, job.run_id)
            .unwrap();
        assert_eq!(
            store
                .get_transfer(&job.job_id)
                .unwrap()
                .unwrap()
                .summary
                .loaded_size,
            120
        );
    }
}
