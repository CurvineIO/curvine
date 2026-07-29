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

use curvine_common::error::FsError;
use curvine_common::state::{
    StaleTaskAttempt, TaskAttemptStart, TransferJobRecord, TransferLease, TransferListFilter,
    TransferState, TransferStateUpdate, TransferTaskRecord, TransferTaskReport, TransferTaskState,
    TransferTenantSummary,
};
use curvine_common::FsResult;
use std::time::Instant;

use crate::transfer::{
    MemoryTransferStore, MysqlTransferStore, SqliteTransferStore, TransferMetrics,
    TransferRequeueUpdate, TransferStore,
};

pub enum TransferStoreBackend {
    Memory(MemoryTransferStore),
    Sqlite(SqliteTransferStore),
    Mysql(MysqlTransferStore),
}

impl TransferStore for TransferStoreBackend {
    fn check_available(&self) -> FsResult<()> {
        self.record_store_operation("check_available", || match self {
            Self::Memory(store) => store.check_available(),
            Self::Sqlite(store) => store.check_available(),
            Self::Mysql(store) => store.check_available(),
        })
    }

    fn create_or_get_by_request_id(&self, job: TransferJobRecord) -> FsResult<TransferJobRecord> {
        self.record_store_operation("create_or_get_by_request_id", || match self {
            Self::Memory(store) => store.create_or_get_by_request_id(job),
            Self::Sqlite(store) => store.create_or_get_by_request_id(job),
            Self::Mysql(store) => store.create_or_get_by_request_id(job),
        })
    }

    fn create_or_get_by_request_id_checked(
        &self,
        job: TransferJobRecord,
    ) -> FsResult<TransferJobRecord> {
        self.record_store_operation("create_or_get_by_request_id_checked", || match self {
            Self::Memory(store) => store.create_or_get_by_request_id_checked(job),
            Self::Sqlite(store) => store.create_or_get_by_request_id_checked(job),
            Self::Mysql(store) => store.create_or_get_by_request_id_checked(job),
        })
    }

    fn get_transfer(&self, job_id: &str) -> FsResult<Option<TransferJobRecord>> {
        self.record_store_operation("get_transfer", || match self {
            Self::Memory(store) => store.get_transfer(job_id),
            Self::Sqlite(store) => store.get_transfer(job_id),
            Self::Mysql(store) => store.get_transfer(job_id),
        })
    }

    fn get_transfer_by_request(
        &self,
        submitter: &str,
        client_request_id: &str,
    ) -> FsResult<Option<TransferJobRecord>> {
        self.record_store_operation("get_transfer_by_request", || match self {
            Self::Memory(store) => store.get_transfer_by_request(submitter, client_request_id),
            Self::Sqlite(store) => store.get_transfer_by_request(submitter, client_request_id),
            Self::Mysql(store) => store.get_transfer_by_request(submitter, client_request_id),
        })
    }

    fn list_active_transfers(&self) -> FsResult<Vec<TransferJobRecord>> {
        self.record_store_operation("list_active_transfers", || match self {
            Self::Memory(store) => store.list_active_transfers(),
            Self::Sqlite(store) => store.list_active_transfers(),
            Self::Mysql(store) => store.list_active_transfers(),
        })
    }

    fn find_conflicting_active_transfer(
        &self,
        target_path: &str,
        submitter: &str,
        client_request_id: &str,
    ) -> FsResult<Option<TransferJobRecord>> {
        self.record_store_operation("find_conflicting_active_transfer", || match self {
            Self::Memory(store) => {
                store.find_conflicting_active_transfer(target_path, submitter, client_request_id)
            }
            Self::Sqlite(store) => {
                store.find_conflicting_active_transfer(target_path, submitter, client_request_id)
            }
            Self::Mysql(store) => {
                store.find_conflicting_active_transfer(target_path, submitter, client_request_id)
            }
        })
    }

    fn count_active_transfers(&self) -> FsResult<u64> {
        self.record_store_operation("count_active_transfers", || match self {
            Self::Memory(store) => store.count_active_transfers(),
            Self::Sqlite(store) => store.count_active_transfers(),
            Self::Mysql(store) => store.count_active_transfers(),
        })
    }

    fn count_executing_transfers(&self) -> FsResult<u64> {
        self.record_store_operation("count_executing_transfers", || match self {
            Self::Memory(store) => store.count_executing_transfers(),
            Self::Sqlite(store) => store.count_executing_transfers(),
            Self::Mysql(store) => store.count_executing_transfers(),
        })
    }

    fn list_transfers(&self, filter: TransferListFilter) -> FsResult<Vec<TransferJobRecord>> {
        self.record_store_operation("list_transfers", || match self {
            Self::Memory(store) => store.list_transfers(filter),
            Self::Sqlite(store) => store.list_transfers(filter),
            Self::Mysql(store) => store.list_transfers(filter),
        })
    }

    fn list_tenant_summaries(
        &self,
        limit: usize,
        offset: usize,
    ) -> FsResult<Vec<TransferTenantSummary>> {
        self.record_store_operation("list_tenant_summaries", || match self {
            Self::Memory(store) => store.list_tenant_summaries(limit, offset),
            Self::Sqlite(store) => store.list_tenant_summaries(limit, offset),
            Self::Mysql(store) => store.list_tenant_summaries(limit, offset),
        })
    }

    fn purge_terminal_transfers(&self, older_than_ms: i64, limit: usize) -> FsResult<usize> {
        self.record_store_operation("purge_terminal_transfers", || match self {
            Self::Memory(store) => store.purge_terminal_transfers(older_than_ms, limit),
            Self::Sqlite(store) => store.purge_terminal_transfers(older_than_ms, limit),
            Self::Mysql(store) => store.purge_terminal_transfers(older_than_ms, limit),
        })
    }

    fn list_transfer_tasks(&self, job_id: &str, run_id: u64) -> FsResult<Vec<TransferTaskRecord>> {
        self.record_store_operation("list_transfer_tasks", || match self {
            Self::Memory(store) => store.list_transfer_tasks(job_id, run_id),
            Self::Sqlite(store) => store.list_transfer_tasks(job_id, run_id),
            Self::Mysql(store) => store.list_transfer_tasks(job_id, run_id),
        })
    }

    fn request_cancel(&self, job_id: &str, run_id: u64, now_ms: i64) -> FsResult<bool> {
        self.record_store_operation("request_cancel", || match self {
            Self::Memory(store) => store.request_cancel(job_id, run_id, now_ms),
            Self::Sqlite(store) => store.request_cancel(job_id, run_id, now_ms),
            Self::Mysql(store) => store.request_cancel(job_id, run_id, now_ms),
        })
    }

    fn acquire_runnable_transfer(
        &self,
        owner: &str,
        lease_ms: i64,
        now_ms: i64,
        max_executing_transfers: u64,
    ) -> FsResult<Option<TransferLease>> {
        self.record_store_operation("acquire_runnable_transfer", || match self {
            Self::Memory(store) => {
                store.acquire_runnable_transfer(owner, lease_ms, now_ms, max_executing_transfers)
            }
            Self::Sqlite(store) => {
                store.acquire_runnable_transfer(owner, lease_ms, now_ms, max_executing_transfers)
            }
            Self::Mysql(store) => {
                store.acquire_runnable_transfer(owner, lease_ms, now_ms, max_executing_transfers)
            }
        })
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
        self.record_store_operation("renew_lease", || match self {
            Self::Memory(store) => {
                store.renew_lease(job_id, run_id, owner, lease_epoch, lease_ms, now_ms)
            }
            Self::Sqlite(store) => {
                store.renew_lease(job_id, run_id, owner, lease_epoch, lease_ms, now_ms)
            }
            Self::Mysql(store) => {
                store.renew_lease(job_id, run_id, owner, lease_epoch, lease_ms, now_ms)
            }
        })
    }

    fn update_transfer_state(&self, update: TransferStateUpdate) -> FsResult<bool> {
        self.record_store_operation("update_transfer_state", || match self {
            Self::Memory(store) => store.update_transfer_state(update),
            Self::Sqlite(store) => store.update_transfer_state(update),
            Self::Mysql(store) => store.update_transfer_state(update),
        })
    }

    fn set_transfer_state(
        &self,
        job_id: &str,
        run_id: u64,
        state: TransferState,
        message: impl Into<String>,
        now_ms: i64,
    ) -> FsResult<bool> {
        let message = message.into();
        self.record_store_operation("set_transfer_state", || match self {
            Self::Memory(store) => {
                store.set_transfer_state(job_id, run_id, state, message.clone(), now_ms)
            }
            Self::Sqlite(store) => {
                store.set_transfer_state(job_id, run_id, state, message.clone(), now_ms)
            }
            Self::Mysql(store) => store.set_transfer_state(job_id, run_id, state, message, now_ms),
        })
    }

    fn requeue_transfer(&self, update: TransferRequeueUpdate) -> FsResult<bool> {
        self.record_store_operation("requeue_transfer", || match self {
            Self::Memory(store) => store.requeue_transfer(update),
            Self::Sqlite(store) => store.requeue_transfer(update),
            Self::Mysql(store) => store.requeue_transfer(update),
        })
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
        self.record_store_operation("set_transfer_cv_metadata_epoch", || match self {
            Self::Memory(store) => store.set_transfer_cv_metadata_epoch(
                job_id,
                run_id,
                owner,
                lease_epoch,
                cv_metadata_epoch,
                now_ms,
            ),
            Self::Sqlite(store) => store.set_transfer_cv_metadata_epoch(
                job_id,
                run_id,
                owner,
                lease_epoch,
                cv_metadata_epoch,
                now_ms,
            ),
            Self::Mysql(store) => store.set_transfer_cv_metadata_epoch(
                job_id,
                run_id,
                owner,
                lease_epoch,
                cv_metadata_epoch,
                now_ms,
            ),
        })
    }

    fn insert_tasks(&self, tasks: Vec<TransferTaskRecord>) -> FsResult<()> {
        self.record_store_operation("insert_tasks", || match self {
            Self::Memory(store) => store.insert_tasks(tasks),
            Self::Sqlite(store) => store.insert_tasks(tasks),
            Self::Mysql(store) => store.insert_tasks(tasks),
        })
    }

    fn update_task_state(
        &self,
        job_id: &str,
        run_id: u64,
        task_id: &str,
        state: TransferTaskState,
        message: impl Into<String>,
        now_ms: i64,
    ) -> FsResult<bool> {
        self.record_store_operation("update_task_state", || match self {
            Self::Memory(store) => {
                store.update_task_state(job_id, run_id, task_id, state, message, now_ms)
            }
            Self::Sqlite(store) => {
                store.update_task_state(job_id, run_id, task_id, state, message, now_ms)
            }
            Self::Mysql(store) => {
                store.update_task_state(job_id, run_id, task_id, state, message, now_ms)
            }
        })
    }

    fn claim_pending_tasks(
        &self,
        job_id: &str,
        run_id: u64,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>> {
        self.record_store_operation("claim_pending_tasks", || match self {
            Self::Memory(store) => store.claim_pending_tasks(job_id, run_id, limit),
            Self::Sqlite(store) => store.claim_pending_tasks(job_id, run_id, limit),
            Self::Mysql(store) => store.claim_pending_tasks(job_id, run_id, limit),
        })
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
        self.record_store_operation("mark_stale_attempts", || match self {
            Self::Memory(store) => {
                store.mark_stale_attempts(job_id, run_id, owner, lease_epoch, now_ms, limit)
            }
            Self::Sqlite(store) => {
                store.mark_stale_attempts(job_id, run_id, owner, lease_epoch, now_ms, limit)
            }
            Self::Mysql(store) => {
                store.mark_stale_attempts(job_id, run_id, owner, lease_epoch, now_ms, limit)
            }
        })
    }

    fn list_recoverable_tasks(
        &self,
        job_id: &str,
        run_id: u64,
    ) -> FsResult<Vec<TransferTaskRecord>> {
        self.record_store_operation("list_recoverable_tasks", || match self {
            Self::Memory(store) => store.list_recoverable_tasks(job_id, run_id),
            Self::Sqlite(store) => store.list_recoverable_tasks(job_id, run_id),
            Self::Mysql(store) => store.list_recoverable_tasks(job_id, run_id),
        })
    }

    fn start_task_attempt(&self, start: TaskAttemptStart) -> FsResult<bool> {
        self.record_store_operation("start_task_attempt", || match self {
            Self::Memory(store) => store.start_task_attempt(start),
            Self::Sqlite(store) => store.start_task_attempt(start),
            Self::Mysql(store) => store.start_task_attempt(start),
        })
    }

    fn update_task_report(&self, report: TransferTaskReport) -> FsResult<bool> {
        self.record_store_operation("update_task_report", || match self {
            Self::Memory(store) => store.update_task_report(report),
            Self::Sqlite(store) => store.update_task_report(report),
            Self::Mysql(store) => store.update_task_report(report),
        })
    }
}

impl TransferStoreBackend {
    pub fn backend_label(&self) -> &'static str {
        match self {
            Self::Memory(_) => "memory",
            Self::Sqlite(_) => "sqlite",
            Self::Mysql(_) => "mysql",
        }
    }

    fn record_store_operation<T>(
        &self,
        operation: &'static str,
        f: impl FnOnce() -> FsResult<T>,
    ) -> FsResult<T> {
        let start = Instant::now();
        let result = f();
        if let Ok(metrics) = TransferMetrics::get() {
            let backend = self.backend_label();
            metrics.observe_store_operation(
                backend,
                operation,
                if result.is_ok() { "success" } else { "error" },
                start.elapsed().as_micros(),
            );
            if is_store_unavailable_result(&result) {
                metrics.record_store_unavailable(backend, operation);
            } else {
                metrics.record_store_available(backend);
            }
        }
        result
    }
}

fn is_store_unavailable_result<T>(result: &FsResult<T>) -> bool {
    let Err(err) = result else {
        return false;
    };
    is_store_unavailable_error(err)
}

pub(crate) fn is_store_unavailable_error(err: &FsError) -> bool {
    if matches!(
        err.kind(),
        curvine_common::error::ErrorKind::IO
            | curvine_common::error::ErrorKind::TransferStoreUnavailable
    ) {
        return true;
    }

    let message = err.to_string();
    message.contains("sqlite transfer store error:")
        || message.contains("mysql transfer store error:")
        || message.contains("Unknown database")
        || message.contains("No database selected")
        || message.contains("doesn't exist")
        || message.contains("Can't connect")
        || message.contains("Connection refused")
        || message.contains("Lost connection")
        || message.contains("server has gone away")
}
