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

use curvine_common::state::{
    StaleTaskAttempt, TaskAttemptStart, TransferJobRecord, TransferLease, TransferListFilter,
    TransferState, TransferStateUpdate, TransferTaskRecord, TransferTaskReport, TransferTaskState,
    TransferTenantSummary,
};
use curvine_common::FsResult;

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

    fn request_cancel(&self, job_id: &str, run_id: u64, now_ms: i64) -> FsResult<bool>;

    fn acquire_runnable_transfer(
        &self,
        owner: &str,
        lease_ms: i64,
        now_ms: i64,
        max_executing_transfers: u64,
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

    fn set_transfer_state(
        &self,
        job_id: &str,
        run_id: u64,
        state: TransferState,
        message: impl Into<String>,
        now_ms: i64,
    ) -> FsResult<bool>;

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

    fn update_task_state(
        &self,
        job_id: &str,
        run_id: u64,
        task_id: &str,
        state: TransferTaskState,
        message: impl Into<String>,
        now_ms: i64,
    ) -> FsResult<bool>;

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

    fn list_recoverable_tasks(
        &self,
        job_id: &str,
        run_id: u64,
    ) -> FsResult<Vec<TransferTaskRecord>>;

    fn start_task_attempt(&self, start: TaskAttemptStart) -> FsResult<bool>;

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
