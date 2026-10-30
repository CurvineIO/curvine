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

use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::{
    transfer_task_summary_from_aggregate, StaleTaskAttempt, TaskAttemptStart, TransferCommand,
    TransferCountFilter, TransferJobRecord, TransferKind, TransferLease, TransferListFilter,
    TransferProgress, TransferState, TransferStateCounts, TransferStateUpdate, TransferTaskCounts,
    TransferTaskRecord, TransferTaskReport, TransferTaskState, TransferTaskStatus,
    TransferTaskSummary, TransferTenantSummary, WorkerInflight,
};
use parking_lot::Mutex;
use rusqlite::{
    params, params_from_iter, types::Value as SqliteValue, Connection, OptionalExtension, Row,
    TransactionBehavior,
};
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use crate::transfer::{
    ensure_task_transaction_bound, store_error_detail, TransferMountSnapshotUpdate,
    TransferPlannedTasks, TransferRequeueUpdate, TransferStore, TransferTaskStateUpdate,
    MAX_JOBS_PER_STORE_TRANSACTION, MAX_TASKS_PER_STORE_TRANSACTION,
};

const TRANSFER_SCHEMA_VERSION: i64 = 5;

pub struct SqliteTransferStore {
    conn: Mutex<Connection>,
}

impl SqliteTransferStore {
    pub fn open(path: impl AsRef<Path>) -> FsResult<Self> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent).map_err(|err| {
                FsError::common(format!(
                    "Failed to create sqlite transfer store directory {}: {}",
                    parent.display(),
                    err
                ))
            })?;
        }
        let conn = Connection::open(path.as_ref()).map_err(sqlite_err)?;
        init_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

impl TransferStore for SqliteTransferStore {
    fn check_available(&self) -> FsResult<()> {
        self.conn
            .lock()
            .query_row("select 1", [], |_| Ok(()))
            .map_err(sqlite_err)
    }

    fn create_or_get_by_request_id(&self, job: TransferJobRecord) -> FsResult<TransferJobRecord> {
        self.create_or_get_by_request_id_checked(job)
    }

    fn create_or_get_by_request_id_checked(
        &self,
        job: TransferJobRecord,
    ) -> FsResult<TransferJobRecord> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        if let Some(existing) = select_job_by_request(&tx, &job.submitter, &job.client_request_id)?
        {
            if existing.command_json != job.command_json {
                return Err(FsError::common(format!(
                    "Transfer request ID {} submitted by {} is already bound to job {} with a different command",
                    job.client_request_id, job.submitter, existing.job_id
                )));
            }
            tx.commit().map_err(sqlite_err)?;
            return Ok(existing);
        }

        insert_job(&tx, &job)?;
        tx.commit().map_err(sqlite_err)?;
        Ok(job)
    }

    fn get_transfer(&self, job_id: &str) -> FsResult<Option<TransferJobRecord>> {
        select_job_by_id(&self.conn.lock(), job_id)
    }

    fn get_transfer_by_request(
        &self,
        submitter: &str,
        client_request_id: &str,
    ) -> FsResult<Option<TransferJobRecord>> {
        select_job_by_request(&self.conn.lock(), submitter, client_request_id)
    }

    fn list_active_transfers(&self) -> FsResult<Vec<TransferJobRecord>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(job_select_sql("where state not in (6, 7, 8, 9)").as_str())
            .map_err(sqlite_err)?;
        let rows = stmt.query_map([], sqlite_job_row).map_err(sqlite_err)?;
        collect_sqlite_rows(rows)
    }

    fn has_active_transfer_by_key(&self, job_key: &str) -> FsResult<bool> {
        Ok(select_non_terminal_job_by_key(&self.conn.lock(), job_key)?.is_some())
    }

    fn find_conflicting_active_transfer(
        &self,
        target_path: &str,
        submitter: &str,
        client_request_id: &str,
    ) -> FsResult<Option<TransferJobRecord>> {
        select_conflicting_active_transfer(
            &self.conn.lock(),
            target_path,
            submitter,
            client_request_id,
        )
    }

    fn count_active_transfers(&self) -> FsResult<u64> {
        self.conn
            .lock()
            .query_row(
                "select count(*) from transfer_jobs where state in (1, 2, 3, 4, 5)",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count as u64)
            .map_err(sqlite_err)
    }

    fn count_executing_transfers(&self) -> FsResult<u64> {
        self.conn
            .lock()
            .query_row(
                "select count(*) from transfer_jobs where state in (2, 3, 4, 5)",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count as u64)
            .map_err(sqlite_err)
    }

    fn list_transfers(&self, filter: TransferListFilter) -> FsResult<Vec<TransferJobRecord>> {
        let conn = self.conn.lock();
        let (where_sql, values) = list_filter_sqlite_params(&filter);
        let sql = job_select_sql(&format!(
            "{where_sql}
             order by updated_at desc, created_at desc, job_id desc
             limit ? offset ?"
        ));
        let mut values = values;
        values.push(SqliteValue::Integer(filter.limit as i64));
        values.push(SqliteValue::Integer(filter.offset as i64));
        let mut stmt = conn.prepare(sql.as_str()).map_err(sqlite_err)?;
        let rows = stmt
            .query_map(params_from_iter(values), sqlite_job_row)
            .map_err(sqlite_err)?;
        collect_sqlite_rows(rows)
    }

    fn count_transfers_by_state(
        &self,
        filter: &TransferCountFilter,
    ) -> FsResult<TransferStateCounts> {
        // Reuse the list filter's WHERE builder with the state predicate cleared.
        let predicate = TransferListFilter {
            kind: filter.kind,
            states: Vec::new(),
            submitter: filter.submitter.clone(),
            tenant: filter.tenant.clone(),
            limit: 0,
            offset: 0,
        };
        let (where_sql, values) = list_filter_sqlite_params(&predicate);
        let sql = format!("select state, count(*) from transfer_jobs {where_sql} group by state");
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(sql.as_str()).map_err(sqlite_err)?;
        let rows = stmt
            .query_map(params_from_iter(values), |row| {
                Ok((row.get::<_, i32>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(sqlite_err)?;
        let mut counts = TransferStateCounts::default();
        for row in rows {
            let (state, count) = row.map_err(sqlite_err)?;
            counts.add(TransferState::from(state), count.max(0) as u64);
        }
        Ok(counts)
    }

    fn list_tenant_summaries(
        &self,
        limit: usize,
        offset: usize,
    ) -> FsResult<Vec<TransferTenantSummary>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "select tenant,
                        sum(case when state = 1 then 1 else 0 end) as pending,
                        sum(case when state in (2, 3, 4, 5) then 1 else 0 end) as executing,
                        sum(case when state = 6 then 1 else 0 end) as completed,
                        sum(case when state = 7 then 1 else 0 end) as failed,
                        sum(case when state = 8 then 1 else 0 end) as canceled,
                        sum(case when state = 9 then 1 else 0 end) as partial_success,
                        count(*) as total
                 from transfer_jobs
                 group by tenant
                 order by (sum(case when state in (1, 2, 3, 4, 5) then 1 else 0 end)) desc,
                          count(*) desc,
                          tenant asc
                 limit ?1 offset ?2",
            )
            .map_err(sqlite_err)?;
        let rows = stmt
            .query_map(
                params![limit as i64, offset as i64],
                sqlite_tenant_summary_row,
            )
            .map_err(sqlite_err)?;
        collect_sqlite_rows(rows)
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
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        let keys = {
            let mut stmt = tx
                .prepare(
                    "select t.job_id, t.run_id, t.task_id from transfer_tasks t
                     join transfer_jobs j on t.job_id = j.job_id and t.run_id = j.run_id
                     where j.state in (6, 7, 8, 9) and j.updated_at < ?1
                       and j.terminal_summary_json is not null
                       and j.terminal_counts_json is not null
                     order by j.updated_at asc, t.task_id asc
                     limit ?2",
                )
                .map_err(sqlite_err)?;
            let rows = stmt
                .query_map(params![older_than_ms, task_limit as i64], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(sqlite_err)?;
            collect_sqlite_rows(rows)?
        };
        let deleted = keys.len();
        for (job_id, run_id, task_id) in &keys {
            tx.execute(
                "delete from transfer_tasks where job_id = ?1 and run_id = ?2 and task_id = ?3",
                params![job_id, run_id, task_id],
            )
            .map_err(sqlite_err)?;
        }
        tx.commit().map_err(sqlite_err)?;
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
        let mut conn = self.conn.lock();
        let lease_available: Option<i64> = conn
            .query_row(
                "select 1 from transfer_cleanup_lease
                 where id = 1 and owner = ?1 and lease_expire_at > ?2",
                params![owner, now_ms],
                |row| row.get(0),
            )
            .optional()
            .map_err(sqlite_err)?;
        if lease_available.is_none() {
            return Ok(0);
        }
        let keys = {
            let mut stmt = conn
                .prepare(
                    "select t.job_id, t.run_id, t.task_id from transfer_tasks t
                     join transfer_jobs j on t.job_id = j.job_id and t.run_id = j.run_id
                     where j.state in (6, 7, 8, 9) and j.updated_at < ?1
                       and j.terminal_summary_json is not null
                       and j.terminal_counts_json is not null
                     order by j.updated_at asc, t.task_id asc
                     limit ?2",
                )
                .map_err(sqlite_err)?;
            let rows = stmt
                .query_map(params![older_than_ms, task_limit as i64], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(sqlite_err)?;
            collect_sqlite_rows(rows)?
        };
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_err)?;
        let lease_owned: Option<i64> = tx
            .query_row(
                "select 1 from transfer_cleanup_lease
                 where id = 1 and owner = ?1 and lease_expire_at > ?2",
                params![owner, now_ms],
                |row| row.get(0),
            )
            .optional()
            .map_err(sqlite_err)?;
        if lease_owned.is_none() {
            tx.commit().map_err(sqlite_err)?;
            return Ok(0);
        }
        let deleted = keys.len();
        for (job_id, run_id, task_id) in &keys {
            tx.execute(
                "delete from transfer_tasks where job_id = ?1 and run_id = ?2 and task_id = ?3",
                params![job_id, run_id, task_id],
            )
            .map_err(sqlite_err)?;
        }
        tx.commit().map_err(sqlite_err)?;
        Ok(deleted)
    }

    fn purge_terminal_jobs(&self, older_than_ms: i64, limit: usize) -> FsResult<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let limit = limit.min(MAX_JOBS_PER_STORE_TRANSACTION);
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        let keys = {
            let mut stmt = tx
                .prepare(
                    "select j.job_id, j.run_id from transfer_jobs j
                     where j.state in (6, 7, 8, 9) and j.updated_at < ?1
                       and not exists (
                           select 1 from transfer_tasks t
                           where t.job_id = j.job_id and t.run_id = j.run_id
                       )
                     order by j.updated_at asc
                     limit ?2",
                )
                .map_err(sqlite_err)?;
            let rows = stmt
                .query_map(params![older_than_ms, limit as i64], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(sqlite_err)?;
            collect_sqlite_rows(rows)?
        };
        for (job_id, run_id) in &keys {
            tx.execute(
                "delete from transfer_jobs
                 where job_id = ?1 and run_id = ?2 and state in (6, 7, 8, 9)
                   and not exists (
                       select 1 from transfer_tasks t
                       where t.job_id = ?1 and t.run_id = ?2
                   )",
                params![job_id, run_id],
            )
            .map_err(sqlite_err)?;
        }
        tx.commit().map_err(sqlite_err)?;
        Ok(keys.len())
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
        let mut conn = self.conn.lock();
        let lease_available: Option<i64> = conn
            .query_row(
                "select 1 from transfer_cleanup_lease
                 where id = 1 and owner = ?1 and lease_expire_at > ?2",
                params![owner, now_ms],
                |row| row.get(0),
            )
            .optional()
            .map_err(sqlite_err)?;
        if lease_available.is_none() {
            return Ok(0);
        }
        let keys = {
            let mut stmt = conn
                .prepare(
                    "select j.job_id, j.run_id from transfer_jobs j
                     where j.state in (6, 7, 8, 9) and j.updated_at < ?1
                       and not exists (
                           select 1 from transfer_tasks t
                           where t.job_id = j.job_id and t.run_id = j.run_id
                       )
                     order by j.updated_at asc
                     limit ?2",
                )
                .map_err(sqlite_err)?;
            let rows = stmt
                .query_map(params![older_than_ms, limit as i64], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(sqlite_err)?;
            collect_sqlite_rows(rows)?
        };
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_err)?;
        let lease_owned: Option<i64> = tx
            .query_row(
                "select 1 from transfer_cleanup_lease
                 where id = 1 and owner = ?1 and lease_expire_at > ?2",
                params![owner, now_ms],
                |row| row.get(0),
            )
            .optional()
            .map_err(sqlite_err)?;
        if lease_owned.is_none() {
            tx.commit().map_err(sqlite_err)?;
            return Ok(0);
        }
        for (job_id, run_id) in &keys {
            tx.execute(
                "delete from transfer_jobs
                 where job_id = ?1 and run_id = ?2 and state in (6, 7, 8, 9)
                   and not exists (
                       select 1 from transfer_tasks t
                       where t.job_id = ?1 and t.run_id = ?2
                   )",
                params![job_id, run_id],
            )
            .map_err(sqlite_err)?;
        }
        tx.commit().map_err(sqlite_err)?;
        Ok(keys.len())
    }

    fn try_acquire_cleanup_lease(&self, owner: &str, now_ms: i64, lease_ms: i64) -> FsResult<bool> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        let changed = tx
            .execute(
                "insert into transfer_cleanup_lease(id, owner, lease_expire_at)
                 values (1, ?1, ?2)
                 on conflict(id) do update set owner=excluded.owner,
                     lease_expire_at=excluded.lease_expire_at
                 where transfer_cleanup_lease.owner=excluded.owner
                    or transfer_cleanup_lease.lease_expire_at <= ?3",
                params![owner, now_ms.saturating_add(lease_ms.max(1)), now_ms],
            )
            .map_err(sqlite_err)?;
        tx.commit().map_err(sqlite_err)?;
        Ok(changed == 1)
    }

    fn list_transfer_tasks(&self, job_id: &str, run_id: u64) -> FsResult<Vec<TransferTaskRecord>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "select job_id, run_id, task_id, attempt_id, source_path, target_path,
                        worker_id, worker_session_id, source_read_plan_json, report_target_json,
                        state, progress_json, retry_count, attempt_started_at, last_report_at,
                        stale_deadline_at, updated_at
                 from transfer_tasks where job_id = ?1 and run_id = ?2
                 order by task_id asc",
            )
            .map_err(sqlite_err)?;
        let rows = stmt
            .query_map(params![job_id, run_id as i64], sqlite_task_row)
            .map_err(sqlite_err)?;
        collect_sqlite_rows(rows)
    }

    fn count_tasks_global(&self) -> FsResult<TransferTaskCounts> {
        let conn = self.conn.lock();
        let mut counts = TransferTaskCounts::default();
        let mut stmt = conn
            .prepare("select state, count(*) from transfer_tasks group by state")
            .map_err(sqlite_err)?;
        let rows = stmt
            .query_map([], |row| Ok((row.get::<_, i32>(0)?, row.get::<_, i64>(1)?)))
            .map_err(sqlite_err)?;
        for row in rows {
            let (state, n) = row.map_err(sqlite_err)?;
            match state {
                1 => counts.pending += n as u64,
                2 => counts.running += n as u64,
                3 => counts.completed += n as u64,
                4 => counts.failed += n as u64,
                5 => counts.canceled += n as u64,
                6 => counts.stale += n as u64,
                _ => {}
            }
        }
        Ok(counts)
    }

    fn get_transfer_task_summary(
        &self,
        job_id: &str,
        run_id: u64,
        now_ms: i64,
    ) -> FsResult<TransferTaskSummary> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "select state, progress_json from transfer_tasks
                 where job_id = ?1 and run_id = ?2
                 order by updated_at asc, task_id asc",
            )
            .map_err(sqlite_err)?;
        let rows = stmt
            .query_map(params![job_id, run_id as i64], |row| {
                Ok((
                    TransferTaskState::from(row.get::<_, i32>(0)?),
                    serde_json::from_str::<TransferProgress>(&row.get::<_, String>(1)?)
                        .unwrap_or_default(),
                ))
            })
            .map_err(sqlite_err)?;
        summarize_task_progress(rows, now_ms)
    }

    fn list_transfer_task_status_page(
        &self,
        job_id: &str,
        run_id: u64,
        offset: usize,
        limit: usize,
    ) -> FsResult<(Vec<TransferTaskStatus>, bool)> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "select task_id, attempt_id, source_path, target_path, worker_id, worker_session_id,
                        state, progress_json, retry_count, updated_at
                 from transfer_tasks where job_id = ?1 and run_id = ?2
                 order by task_id asc limit ?3 offset ?4",
            )
            .map_err(sqlite_err)?;
        let mut page = collect_sqlite_rows(
            stmt.query_map(
                params![
                    job_id,
                    run_id as i64,
                    limit.saturating_add(1) as i64,
                    offset as i64
                ],
                sqlite_task_status_row,
            )
            .map_err(sqlite_err)?,
        )?;
        let has_next = page.len() > limit;
        page.truncate(limit);
        Ok((page, has_next))
    }

    fn request_cancel(&self, job_id: &str, run_id: u64, now_ms: i64) -> FsResult<bool> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        let Some(summary_json) = tx
            .query_row(
                "select summary_json from transfer_jobs
                 where job_id = ?1 and run_id = ?2 and state not in (6, 7, 8, 9)",
                params![job_id, run_id as i64],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sqlite_err)?
        else {
            tx.commit().map_err(sqlite_err)?;
            return Ok(false);
        };
        let mut summary: TransferProgress =
            serde_json::from_str(&summary_json).map_err(|err| FsError::common(err.to_string()))?;
        summary.message = "cancel requested".to_string();
        summary.update_time = now_ms;
        let summary_json = transfer_progress_json(&summary)?;
        let affected = tx
            .execute(
                "update transfer_jobs
                 set cancel_requested = 1, state = ?3, summary_json = ?4, updated_at = ?5
                 where job_id = ?1 and run_id = ?2 and state not in (6, 7, 8, 9)",
                params![
                    job_id,
                    run_id as i64,
                    TransferState::Canceling as i32,
                    summary_json,
                    now_ms
                ],
            )
            .map_err(sqlite_err)?;
        tx.commit().map_err(sqlite_err)?;
        Ok(affected > 0)
    }

    fn acquire_runnable_transfer(
        &self,
        owner: &str,
        lease_ms: i64,
        now_ms: i64,
        max_executing_transfers: u64,
        prefer_pending: bool,
    ) -> FsResult<Option<TransferLease>> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        let running_check_interval_ms =
            crate::transfer::running_transfer_check_interval_ms(lease_ms);
        let running_event_lease_before_ms =
            now_ms.saturating_add(lease_ms.saturating_sub(running_check_interval_ms));
        let running_renew_lease_before_ms = now_ms.saturating_add(lease_ms / 2);
        let mut candidate: Option<(String, i64, i64, i64)> = if !prefer_pending {
            tx.query_row(
                "select job_id, run_id, lease_epoch, state
                 from transfer_jobs as job
                 where job.owner = ?1 and job.lease_expire_at > ?2
                   and (
                        (job.state = 5 and job.updated_at < ?2)
                        or (
                            job.state = 4 and (
                                job.lease_expire_at <= ?4
                                or (
                                    job.lease_expire_at <= ?3 and (
                                        exists (
                                            select 1 from transfer_tasks as task
                                            where task.job_id = job.job_id and task.run_id = job.run_id
                                              and task.state in (1, 4, 6)
                                        )
                                        or exists (
                                            select 1 from transfer_tasks as task
                                            where task.job_id = job.job_id and task.run_id = job.run_id
                                              and task.state = 2 and task.stale_deadline_at < ?2
                                        )
                                        or not exists (
                                            select 1 from transfer_tasks as task
                                            where task.job_id = job.job_id and task.run_id = job.run_id
                                              and task.state in (1, 2, 6)
                                        )
                                    )
                                )
                            )
                        )
                   )
                 order by job.updated_at asc
                 limit 1",
                params![
                    owner,
                    now_ms,
                    running_event_lease_before_ms,
                    running_renew_lease_before_ms,
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(sqlite_err)?
        } else {
            None
        };
        if candidate.is_none() {
            candidate = tx
                .query_row(
                    "select job_id, run_id, lease_epoch, state
                 from transfer_jobs
                 where state in (2, 3, 4, 5) and lease_expire_at <= ?1
                 order by updated_at asc
                 limit 1",
                    params![now_ms],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()
                .map_err(sqlite_err)?;
        }
        if candidate.is_none() && has_pending_transfer(&tx, now_ms)? {
            let executing = tx
                .query_row(
                    "select count(*) from transfer_jobs where state in (2, 3, 4, 5)",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(sqlite_err)? as u64;
            if executing < max_executing_transfers {
                let active_targets = select_executing_targets(&tx)?;
                candidate = select_pending_transfer(&tx, now_ms, &active_targets)?;
            }
        }
        let Some((job_id, run_id, lease_epoch, state)) = candidate else {
            tx.commit().map_err(sqlite_err)?;
            return Ok(None);
        };

        let record = tx
            .query_row(
                job_select_sql("where job_id = ?1").as_str(),
                params![&job_id],
                sqlite_job_row,
            )
            .map_err(sqlite_err)?;
        let conflict = if state != TransferState::Pending as i64 {
            select_conflicting_active_transfer(
                &tx,
                &record.target_path,
                &record.submitter,
                &record.client_request_id,
            )?
        } else {
            None
        };
        if let Some(existing) = conflict {
            let mut summary = record.summary;
            summary.message = format!(
                "target {} conflicts with active transfer {} target {}",
                record.target_path, existing.job_id, existing.target_path
            );
            summary.update_time = now_ms;
            tx.execute(
                "update transfer_jobs
                 set state = ?1, owner = '', lease_expire_at = 0,
                     summary_json = ?2, updated_at = ?3
                 where job_id = ?4 and run_id = ?5 and lease_epoch = ?6",
                params![
                    TransferState::Failed as i32,
                    transfer_progress_json(&summary)?,
                    now_ms,
                    job_id,
                    run_id,
                    lease_epoch
                ],
            )
            .map_err(sqlite_err)?;
            tx.commit().map_err(sqlite_err)?;
            return Ok(None);
        }

        let new_epoch = lease_epoch.saturating_add(1);
        let new_state = if state == TransferState::Pending as i64 {
            TransferState::Planning as i32
        } else {
            state as i32
        };
        let summary_json = if state == TransferState::Pending as i64 {
            transfer_progress_json(&TransferProgress {
                update_time: now_ms,
                message: "acquired for planning".to_string(),
                ..Default::default()
            })?
        } else {
            tx.query_row(
                "select summary_json from transfer_jobs where job_id = ?1",
                params![job_id],
                |row| row.get::<_, String>(0),
            )
            .map_err(sqlite_err)?
        };
        let affected = tx
            .execute(
                "update transfer_jobs
                 set state = ?1, owner = ?2, lease_epoch = ?3, lease_expire_at = ?4,
                     summary_json = ?5, updated_at = ?6
                 where job_id = ?7 and run_id = ?8 and lease_epoch = ?9",
                params![
                    new_state,
                    owner,
                    new_epoch,
                    now_ms.saturating_add(lease_ms),
                    summary_json,
                    now_ms,
                    job_id,
                    run_id,
                    lease_epoch
                ],
            )
            .map_err(sqlite_err)?;
        tx.commit().map_err(sqlite_err)?;
        if affected == 0 {
            return Ok(None);
        }
        Ok(Some(TransferLease {
            job_id,
            run_id: run_id as u64,
            owner: owner.to_string(),
            lease_epoch: new_epoch as u64,
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
        let affected = self
            .conn
            .lock()
            .execute(
                "update transfer_jobs
                 set lease_expire_at = ?6, updated_at = ?7
                 where job_id = ?1 and run_id = ?2 and owner = ?3 and lease_epoch = ?4
                   and state not in (6, 7, 8, 9) and lease_expire_at > ?7",
                params![
                    job_id,
                    run_id as i64,
                    owner,
                    lease_epoch as i64,
                    lease_ms,
                    now_ms.saturating_add(lease_ms),
                    now_ms
                ],
            )
            .map_err(sqlite_err)?;
        Ok(affected > 0)
    }

    fn update_transfer_state(&self, update: TransferStateUpdate) -> FsResult<bool> {
        let states = state_list(&update.from_states);
        let sql = format!(
            "update transfer_jobs
             set state = ?1, summary_json = ?2, updated_at = ?3
             where job_id = ?4 and run_id = ?5 and owner = ?6 and lease_epoch = ?7
               and lease_expire_at > ?3 and state in ({states})"
        );
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        let Some(job) = tx
            .query_row(
                job_select_sql("where job_id = ?1").as_str(),
                params![&update.job_id],
                sqlite_job_row,
            )
            .optional()
            .map_err(sqlite_err)?
        else {
            tx.commit().map_err(sqlite_err)?;
            return Ok(false);
        };
        let mut summary = job.summary;
        summary.message = update.message;
        summary.update_time = update.now_ms;
        let affected = tx
            .execute(
                &sql,
                params![
                    update.to_state as i32,
                    transfer_progress_json(&summary)?,
                    update.now_ms,
                    update.job_id,
                    update.run_id as i64,
                    update.owner,
                    update.lease_epoch as i64
                ],
            )
            .map_err(sqlite_err)?;
        tx.commit().map_err(sqlite_err)?;
        Ok(affected > 0)
    }

    fn requeue_transfer(&self, update: TransferRequeueUpdate) -> FsResult<bool> {
        let summary = transfer_progress_json(&TransferProgress {
            message: update.message,
            update_time: update.now_ms,
            ..Default::default()
        })?;
        let affected = self
            .conn
            .lock()
            .execute(
                "update transfer_jobs
                 set state = ?1, owner = '', lease_expire_at = ?2, summary_json = ?3,
                     mount_snapshot_attempts = coalesce(?4, mount_snapshot_attempts),
                     planning_attempts = coalesce(?5, planning_attempts), updated_at = ?6
                 where job_id = ?7 and run_id = ?8 and owner = ?9 and lease_epoch = ?10
                   and lease_expire_at > ?6 and state = ?11",
                params![
                    TransferState::Pending as i32,
                    update.next_attempt_at_ms,
                    summary,
                    update.mount_snapshot_attempts.map(i64::from),
                    update.planning_attempts.map(i64::from),
                    update.now_ms,
                    update.job_id,
                    update.run_id as i64,
                    update.owner,
                    update.lease_epoch as i64,
                    TransferState::Planning as i32
                ],
            )
            .map_err(sqlite_err)?;
        Ok(affected > 0)
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
        let affected = self
            .conn
            .lock()
            .execute(
                "update transfer_jobs
                 set cv_metadata_epoch = ?1, updated_at = ?2
                 where job_id = ?3 and run_id = ?4 and owner = ?5 and lease_epoch = ?6
                   and lease_expire_at > ?2 and state not in (6, 7, 8, 9)
                   and (cv_metadata_epoch is null or cv_metadata_epoch = ?1)",
                params![
                    cv_metadata_epoch as i64,
                    now_ms,
                    job_id,
                    run_id as i64,
                    owner,
                    lease_epoch as i64
                ],
            )
            .map_err(sqlite_err)?;
        Ok(affected > 0)
    }

    fn set_transfer_mount_snapshot(&self, update: TransferMountSnapshotUpdate) -> FsResult<bool> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        let Some(mut job) = tx
            .query_row(
                job_select_sql("where job_id = ?1").as_str(),
                params![&update.job_id],
                sqlite_job_row,
            )
            .optional()
            .map_err(sqlite_err)?
        else {
            tx.commit().map_err(sqlite_err)?;
            return Ok(false);
        };
        if job.run_id != update.run_id
            || job.owner != update.owner
            || job.lease_epoch != update.lease_epoch
            || job.lease_expire_at <= update.now_ms
            || job.state.is_terminal()
        {
            tx.commit().map_err(sqlite_err)?;
            return Ok(false);
        }
        if !job.mount_snapshot_json.trim().is_empty() && job.mount_snapshot_json != "{}" {
            let matches = job.mount_snapshot_json == update.mount_snapshot_json;
            tx.commit().map_err(sqlite_err)?;
            return Ok(matches);
        }
        job.mount_snapshot_json = update.mount_snapshot_json;
        job.cluster_snapshot_version = update.cluster_snapshot_version;
        job.updated_at = update.now_ms;
        let affected = tx
            .execute(
                "update transfer_jobs
                 set mount_snapshot_json = ?1, cluster_snapshot_version = ?2,
                     updated_at = ?3
                 where job_id = ?4 and run_id = ?5 and owner = ?6 and lease_epoch = ?7
                   and lease_expire_at > ?3 and state not in (6, 7, 8, 9)",
                params![
                    job.mount_snapshot_json,
                    job.cluster_snapshot_version as i64,
                    job.updated_at,
                    job.job_id,
                    job.run_id as i64,
                    job.owner,
                    job.lease_epoch as i64
                ],
            )
            .map_err(sqlite_err)?;
        tx.commit().map_err(sqlite_err)?;
        Ok(affected > 0)
    }

    fn insert_tasks(&self, tasks: Vec<TransferTaskRecord>) -> FsResult<()> {
        ensure_task_transaction_bound(tasks.len())?;
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        for task in tasks {
            insert_task(&tx, &task)?;
        }
        tx.commit().map_err(sqlite_err)?;
        Ok(())
    }

    fn append_planned_tasks(&self, update: TransferPlannedTasks) -> FsResult<bool> {
        ensure_task_transaction_bound(update.tasks.len())?;
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        let valid_owner = tx
            .query_row(
                "select 1 from transfer_jobs
                 where job_id = ?1 and run_id = ?2 and owner = ?3 and lease_epoch = ?4
                   and lease_expire_at > ?5 and state = ?6",
                params![
                    &update.job_id,
                    update.run_id as i64,
                    &update.owner,
                    update.lease_epoch as i64,
                    update.now_ms,
                    TransferState::Planning as i32,
                ],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(sqlite_err)?
            .is_some();
        if !valid_owner {
            tx.commit().map_err(sqlite_err)?;
            return Ok(false);
        }
        for task in update.tasks {
            insert_task(&tx, &task)?;
        }
        tx.commit().map_err(sqlite_err)?;
        Ok(true)
    }

    fn persist_planned_tasks(&self, update: TransferPlannedTasks) -> FsResult<bool> {
        ensure_task_transaction_bound(update.tasks.len())?;
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        let summary = transfer_progress_json(&TransferProgress {
            message: update.message,
            update_time: update.now_ms,
            ..Default::default()
        })?;
        let affected = tx
            .execute(
                "update transfer_jobs
                 set state = ?1, summary_json = ?2, updated_at = ?3
                 where job_id = ?4 and run_id = ?5 and owner = ?6 and lease_epoch = ?7
                   and lease_expire_at > ?3 and state = ?8",
                params![
                    TransferState::Dispatching as i32,
                    summary,
                    update.now_ms,
                    update.job_id,
                    update.run_id as i64,
                    update.owner,
                    update.lease_epoch as i64,
                    TransferState::Planning as i32,
                ],
            )
            .map_err(sqlite_err)?;
        if affected == 0 {
            tx.commit().map_err(sqlite_err)?;
            return Ok(false);
        }
        for task in update.tasks {
            insert_task(&tx, &task)?;
        }
        tx.commit().map_err(sqlite_err)?;
        Ok(true)
    }

    fn has_transfer_tasks(&self, job_id: &str, run_id: u64) -> FsResult<bool> {
        let conn = self.conn.lock();
        conn.query_row(
            "select 1 from transfer_tasks where job_id = ?1 and run_id = ?2 limit 1",
            params![job_id, run_id as i64],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map(|row| row.is_some())
        .map_err(sqlite_err)
    }

    fn update_task_state(&self, update: TransferTaskStateUpdate) -> FsResult<bool> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        let Some(job) = tx
            .query_row(
                job_select_sql("where job_id = ?1").as_str(),
                params![&update.job_id],
                sqlite_job_row,
            )
            .optional()
            .map_err(sqlite_err)?
        else {
            tx.commit().map_err(sqlite_err)?;
            return Ok(false);
        };
        if job.run_id != update.run_id
            || job.owner != update.owner
            || job.lease_epoch != update.lease_epoch
            || job.lease_expire_at <= update.now_ms
            || job.state.is_terminal()
        {
            tx.commit().map_err(sqlite_err)?;
            return Ok(false);
        }
        let Some(mut task) = tx
            .query_row(
                "select job_id, run_id, task_id, attempt_id, source_path, target_path,
                        worker_id, worker_session_id, source_read_plan_json, report_target_json,
                        state, progress_json, retry_count, attempt_started_at, last_report_at,
                        stale_deadline_at, updated_at
                 from transfer_tasks
                 where job_id = ?1 and run_id = ?2 and task_id = ?3",
                params![&update.job_id, update.run_id as i64, &update.task_id],
                sqlite_task_row,
            )
            .optional()
            .map_err(sqlite_err)?
        else {
            tx.commit().map_err(sqlite_err)?;
            return Ok(false);
        };
        if !update.from_states.is_empty() && !update.from_states.contains(&task.state) {
            tx.commit().map_err(sqlite_err)?;
            return Ok(false);
        }

        task.state = update.state;
        if update.bump_retry {
            task.retry_count = task.retry_count.saturating_add(1);
        }
        task.progress.message = update.message;
        task.progress.update_time = update.now_ms;
        task.updated_at = update.now_ms;
        update_task_record(&tx, &task)?;
        tx.commit().map_err(sqlite_err)?;
        Ok(true)
    }

    fn claim_pending_tasks(
        &self,
        job_id: &str,
        run_id: u64,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "select job_id, run_id, task_id, attempt_id, source_path, target_path,
                        worker_id, worker_session_id, source_read_plan_json, report_target_json,
                        state, progress_json, retry_count, attempt_started_at, last_report_at,
                        stale_deadline_at, updated_at
                 from transfer_tasks
                 where job_id = ?1 and run_id = ?2 and state = 1
                 order by task_id asc
                 limit ?3",
            )
            .map_err(sqlite_err)?;
        let rows = stmt
            .query_map(
                params![job_id, run_id as i64, limit as i64],
                sqlite_task_row,
            )
            .map_err(sqlite_err)?;
        collect_sqlite_rows(rows)
    }

    fn select_inflight_by_worker(&self) -> FsResult<Vec<WorkerInflight>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "select worker_id, count(*), coalesce(sum(json_extract(progress_json, '$.total_size')), 0),
                        count(*) filter (where coalesce(json_extract(progress_json, '$.total_size'), 0) > 0)
                 from transfer_tasks where state = 2
                 group by worker_id",
            )
            .map_err(sqlite_err)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .map_err(sqlite_err)?;
        let mut out = Vec::new();
        for row in rows {
            let (worker_id, task_count, bytes, nonzero) = row.map_err(sqlite_err)?;
            out.push(WorkerInflight {
                worker_id: u32::try_from(worker_id).unwrap_or(0),
                task_count: u64::try_from(task_count).unwrap_or(u64::MAX),
                estimated_bytes: bytes,
                unknown_bytes_count: u64::try_from(task_count.saturating_sub(nonzero)).unwrap_or(0),
            });
        }
        Ok(out)
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
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        let valid_owner: Option<i64> = tx
            .query_row(
                "select 1 from transfer_jobs
                 where job_id = ?1 and run_id = ?2 and owner = ?3 and lease_epoch = ?4
                   and lease_expire_at > ?5",
                params![job_id, run_id as i64, owner, lease_epoch as i64, now_ms],
                |row| row.get(0),
            )
            .optional()
            .map_err(sqlite_err)?;
        if valid_owner.is_none() {
            tx.commit().map_err(sqlite_err)?;
            return Ok(Vec::new());
        }

        let mut stale = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            if candidate.job_id != job_id
                || candidate.run_id != run_id
                || candidate.state != TransferTaskState::Running
                || candidate.stale_deadline_at >= now_ms
            {
                continue;
            }
            let mut task = candidate.clone();
            task.state = TransferTaskState::Stale;
            task.retry_count = task.retry_count.saturating_add(1);
            task.progress.update_time = now_ms;
            task.updated_at = now_ms;
            let affected = tx
                .execute(
                    "update transfer_tasks
                     set state = ?1, progress_json = ?2, retry_count = ?3, updated_at = ?4
                     where job_id = ?5 and run_id = ?6 and task_id = ?7 and state = 2
                       and attempt_id = ?8 and worker_id = ?9 and worker_session_id = ?10
                       and stale_deadline_at = ?11 and stale_deadline_at < ?4
                       and exists (
                           select 1 from transfer_jobs
                           where job_id = ?5 and run_id = ?6 and owner = ?12
                             and lease_epoch = ?13 and lease_expire_at > ?4
                       )",
                    params![
                        task.state as i32,
                        transfer_progress_json(&task.progress)?,
                        task.retry_count as i64,
                        now_ms,
                        job_id,
                        run_id as i64,
                        &task.task_id,
                        candidate.attempt_id as i64,
                        candidate.worker_id as i64,
                        &candidate.worker_session_id,
                        candidate.stale_deadline_at,
                        owner,
                        lease_epoch as i64,
                    ],
                )
                .map_err(sqlite_err)?;
            if affected > 0 {
                stale.push(StaleTaskAttempt { task });
            }
        }
        tx.commit().map_err(sqlite_err)?;
        Ok(stale)
    }

    fn list_stale_running_tasks(
        &self,
        job_id: &str,
        run_id: u64,
        stale_before_ms: i64,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "select job_id, run_id, task_id, attempt_id, source_path, target_path,
                        worker_id, worker_session_id, source_read_plan_json, report_target_json,
                        state, progress_json, retry_count, attempt_started_at, last_report_at,
                        stale_deadline_at, updated_at
                 from transfer_tasks
                 where job_id = ?1 and run_id = ?2 and state = 2 and stale_deadline_at < ?3
                 order by stale_deadline_at asc
                 limit ?4",
            )
            .map_err(sqlite_err)?;
        let rows = stmt
            .query_map(
                params![job_id, run_id as i64, stale_before_ms, limit as i64],
                sqlite_task_row,
            )
            .map_err(sqlite_err)?;
        collect_sqlite_rows(rows)
    }

    fn list_tasks_by_state(
        &self,
        job_id: &str,
        run_id: u64,
        state: TransferTaskState,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "select job_id, run_id, task_id, attempt_id, source_path, target_path,
                        worker_id, worker_session_id, source_read_plan_json, report_target_json,
                        state, progress_json, retry_count, attempt_started_at, last_report_at,
                        stale_deadline_at, updated_at
                 from transfer_tasks
                 where job_id = ?1 and run_id = ?2 and state = ?3
                 order by task_id asc
                 limit ?4",
            )
            .map_err(sqlite_err)?;
        let rows = stmt
            .query_map(
                params![job_id, run_id as i64, state as i32, limit as i64],
                sqlite_task_row,
            )
            .map_err(sqlite_err)?;
        collect_sqlite_rows(rows)
    }

    fn list_failed_tasks(&self, job_id: &str, run_id: u64) -> FsResult<Vec<TransferTaskRecord>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "select job_id, run_id, task_id, attempt_id, source_path, target_path,
                        worker_id, worker_session_id, source_read_plan_json, report_target_json,
                        state, progress_json, retry_count, attempt_started_at, last_report_at,
                        stale_deadline_at, updated_at
                 from transfer_tasks
                 where job_id = ?1 and run_id = ?2 and state = ?3
                 order by task_id asc",
            )
            .map_err(sqlite_err)?;
        let rows = stmt
            .query_map(
                params![job_id, run_id as i64, TransferTaskState::Failed as i32],
                sqlite_task_row,
            )
            .map_err(sqlite_err)?;
        collect_sqlite_rows(rows)
    }

    fn has_failed_tasks(&self, job_id: &str, run_id: u64) -> FsResult<bool> {
        self.conn
            .lock()
            .query_row(
                "select exists(
                     select 1 from transfer_tasks
                      where job_id = ?1 and run_id = ?2 and state = ?3
                 )",
                params![job_id, run_id as i64, TransferTaskState::Failed as i32],
                |row| row.get(0),
            )
            .map_err(sqlite_err)
    }

    fn has_recoverable_tasks(&self, job_id: &str, run_id: u64) -> FsResult<bool> {
        self.conn
            .lock()
            .query_row(
                "select exists(
                     select 1 from transfer_tasks
                      where job_id = ?1 and run_id = ?2 and state in (1, 2, 6)
                 )",
                params![job_id, run_id as i64],
                |row| row.get(0),
            )
            .map_err(sqlite_err)
    }

    fn start_task_attempt(&self, start: TaskAttemptStart) -> FsResult<bool> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        let previous: Option<TransferTaskRecord> = tx
            .query_row(
                "select job_id, run_id, task_id, attempt_id, source_path, target_path,
                        worker_id, worker_session_id, source_read_plan_json, report_target_json,
                        state, progress_json, retry_count, attempt_started_at, last_report_at,
                        stale_deadline_at, updated_at
                 from transfer_tasks
                 where job_id = ?1 and run_id = ?2 and task_id = ?3",
                params![start.job_id, start.run_id as i64, start.task_id],
                sqlite_task_row,
            )
            .optional()
            .map_err(sqlite_err)?;
        let Some(previous) = previous else {
            tx.commit().map_err(sqlite_err)?;
            return Ok(false);
        };
        if !matches!(
            previous.state,
            TransferTaskState::Pending | TransferTaskState::Stale
        ) {
            tx.commit().map_err(sqlite_err)?;
            return Ok(false);
        }
        let mut progress = previous.progress.clone();
        progress.loaded_size = start
            .resume_loaded_size
            .clamp(0, progress.total_size.max(0));
        progress.update_time = start.now_ms;
        let affected = tx
            .execute(
                "update transfer_tasks
                 set attempt_id = ?1, worker_id = ?2, worker_session_id = ?3,
                     report_target_json = ?4, state = 2, attempt_started_at = ?5,
                     last_report_at = ?5, stale_deadline_at = ?6, updated_at = ?5,
                     progress_json = ?12
                 where job_id = ?7 and run_id = ?8 and task_id = ?9 and state in (1, 6)
                   and exists (
                       select 1 from transfer_jobs
                        where job_id = ?7 and run_id = ?8 and owner = ?10
                          and lease_epoch = ?11 and cancel_requested = 0
                          and lease_expire_at > ?5 and state not in (6, 7, 8, 9)
                   )",
                params![
                    start.attempt_id as i64,
                    start.worker_id as i64,
                    start.worker_session_id,
                    start.report_target_json,
                    start.now_ms,
                    start.stale_deadline_at,
                    start.job_id,
                    start.run_id as i64,
                    start.task_id,
                    start.owner,
                    start.lease_epoch as i64,
                    transfer_progress_json(&progress)?,
                ],
            )
            .map_err(sqlite_err)?;
        tx.commit().map_err(sqlite_err)?;
        Ok(affected > 0)
    }

    fn update_task_report(&self, report: TransferTaskReport) -> FsResult<bool> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        let previous = tx
            .query_row(
                "select job_id, run_id, task_id, attempt_id, source_path, target_path,
                        worker_id, worker_session_id, source_read_plan_json, report_target_json,
                        state, progress_json, retry_count, attempt_started_at, last_report_at,
                        stale_deadline_at, updated_at
                 from transfer_tasks
                 where job_id = ?1 and run_id = ?2 and task_id = ?3",
                params![report.job_id, report.run_id as i64, report.task_id],
                sqlite_task_row,
            )
            .optional()
            .map_err(sqlite_err)?;
        let Some(previous) = previous else {
            tx.commit().map_err(sqlite_err)?;
            return Ok(false);
        };
        if previous.attempt_id != report.attempt_id
            || previous.worker_id != report.worker_id
            || previous.worker_session_id != report.worker_session_id
            || !previous.state.is_running()
        {
            tx.commit().map_err(sqlite_err)?;
            return Ok(false);
        }
        let mut progress = report.progress.clone();
        if report.state == TransferTaskState::Running {
            progress.loaded_size = previous
                .progress
                .loaded_size
                .max(0)
                .max(report.progress.loaded_size.max(0));
        }
        let affected = tx
            .execute(
                "update transfer_tasks
                 set state = ?1, progress_json = ?2, last_report_at = ?3,
                     stale_deadline_at = ?4, updated_at = ?3
                 where job_id = ?5 and run_id = ?6 and task_id = ?7 and attempt_id = ?8
                   and worker_id = ?9 and worker_session_id = ?10 and state in (1, 2)",
                params![
                    report.state as i32,
                    transfer_progress_json(&progress)?,
                    report.now_ms,
                    report.stale_deadline_at,
                    report.job_id,
                    report.run_id as i64,
                    report.task_id,
                    report.attempt_id as i64,
                    report.worker_id as i64,
                    report.worker_session_id,
                ],
            )
            .map_err(sqlite_err)?;
        tx.commit().map_err(sqlite_err)?;
        if affected == 0 {
            return Ok(false);
        }
        Ok(true)
    }

    fn get_task_attempt_started_at(
        &self,
        job_id: &str,
        run_id: u64,
        task_id: &str,
    ) -> FsResult<Option<i64>> {
        let conn = self.conn.lock();
        let started: Option<i64> = conn
            .query_row(
                "select attempt_started_at from transfer_tasks
                 where job_id = ?1 and run_id = ?2 and task_id = ?3",
                params![job_id, run_id as i64, task_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sqlite_err)?;
        Ok(started.filter(|started| *started > 0))
    }

    fn update_job_progress_summary(
        &self,
        job_id: &str,
        run_id: u64,
        summary: TransferProgress,
        now_ms: i64,
    ) -> FsResult<()> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        tx.execute(
            "update transfer_jobs set summary_json = ?1, updated_at = ?2
             where job_id = ?3 and run_id = ?4",
            params![
                transfer_progress_json(&summary)?,
                now_ms,
                job_id,
                run_id as i64,
            ],
        )
        .map_err(sqlite_err)?;
        tx.commit().map_err(sqlite_err)?;
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
        let counts_json = serde_json::to_string(&counts)
            .map_err(|_| FsError::common("Unable to encode transfer counts"))?;
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        tx.execute(
            "update transfer_jobs
             set summary_json = ?1, terminal_summary_json = coalesce(terminal_summary_json, ?1),
                 terminal_counts_json = coalesce(terminal_counts_json, ?2), updated_at = ?3
             where job_id = ?4 and run_id = ?5",
            params![
                transfer_progress_json(&summary)?,
                counts_json,
                now_ms,
                job_id,
                run_id as i64,
            ],
        )
        .map_err(sqlite_err)?;
        tx.commit().map_err(sqlite_err)?;
        Ok(())
    }
}

fn update_task_record(conn: &Connection, task: &TransferTaskRecord) -> FsResult<bool> {
    let affected = conn
        .execute(
            "update transfer_tasks
             set attempt_id = ?1, worker_id = ?2, worker_session_id = ?3,
                 source_read_plan_json = ?4, report_target_json = ?5, state = ?6,
                 progress_json = ?7, retry_count = ?8, attempt_started_at = ?9,
                 last_report_at = ?10, stale_deadline_at = ?11, updated_at = ?12
             where job_id = ?13 and run_id = ?14 and task_id = ?15",
            params![
                task.attempt_id as i64,
                task.worker_id as i64,
                task.worker_session_id,
                task.source_read_plan_json,
                task.report_target_json,
                task.state as i32,
                transfer_progress_json(&task.progress)?,
                task.retry_count as i64,
                task.attempt_started_at,
                task.last_report_at,
                task.stale_deadline_at,
                task.updated_at,
                task.job_id,
                task.run_id as i64,
                task.task_id
            ],
        )
        .map_err(sqlite_err)?;
    Ok(affected > 0)
}

fn init_schema(conn: &Connection) -> FsResult<()> {
    conn.execute_batch(
        "
        create table if not exists transfer_schema_version (
            id integer primary key check(id = 1),
            version integer not null,
            updated_at integer not null
        );
        ",
    )
    .map_err(sqlite_err)?;
    conn.execute(
        "insert or ignore into transfer_schema_version(id, version, updated_at)
         values (1, ?1, strftime('%s', 'now') * 1000)",
        params![TRANSFER_SCHEMA_VERSION],
    )
    .map_err(sqlite_err)?;
    let version = conn
        .query_row(
            "select version from transfer_schema_version where id = 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(sqlite_err)?;
    migrate_schema(conn, version)?;
    create_base_tables(conn)?;
    create_indexes(conn)?;
    Ok(())
}

fn create_base_tables(conn: &Connection) -> FsResult<()> {
    conn.execute_batch(
        "
        create table if not exists transfer_jobs (
            job_id text primary key,
            job_key text not null,
            run_id integer not null,
            kind integer not null,
            source_path text not null,
            target_path text not null,
            command_json text not null,
            mount_snapshot_json text not null,
            secret_ref_json text not null,
            cluster_snapshot_version integer not null,
            mount_snapshot_attempts integer not null default 0,
            planning_attempts integer not null default 0,
            cv_metadata_epoch integer,
            state integer not null,
            owner text not null,
            lease_epoch integer not null,
            lease_expire_at integer not null,
            cancel_requested integer not null,
            summary_json text not null,
            terminal_summary_json text,
            terminal_counts_json text,
            client_request_id text not null,
            submitter text not null,
            tenant text not null,
            created_at integer not null,
            updated_at integer not null
        );
        create table if not exists transfer_cleanup_lease (
            id integer primary key check (id = 1),
            owner text not null,
            lease_expire_at integer not null
        );
        create table if not exists transfer_tasks (
            job_id text not null,
            run_id integer not null,
            task_id text not null,
            attempt_id integer not null,
            source_path text not null,
            target_path text not null,
            worker_id integer not null,
            worker_session_id text not null,
            source_read_plan_json text not null,
            report_target_json text not null,
            state integer not null,
            progress_json text not null,
            retry_count integer not null,
            attempt_started_at integer not null,
            last_report_at integer not null,
            stale_deadline_at integer not null,
            updated_at integer not null,
            primary key(job_id, run_id, task_id)
        );
        ",
    )
    .map_err(sqlite_err)?;
    Ok(())
}

fn create_indexes(conn: &Connection) -> FsResult<()> {
    conn.execute_batch(
        "
        create unique index if not exists transfer_jobs_request_idx
            on transfer_jobs(submitter, client_request_id);
        create index if not exists transfer_jobs_job_key_state_idx
            on transfer_jobs(job_key, state);
        create index if not exists transfer_jobs_state_lease_idx
            on transfer_jobs(state, lease_expire_at);
        create index if not exists transfer_jobs_state_updated_idx
            on transfer_jobs(state, updated_at);
        create index if not exists transfer_jobs_owner_state_updated_idx
            on transfer_jobs(owner, state, updated_at);
        create index if not exists transfer_jobs_target_state_idx
            on transfer_jobs(target_path, state);
        create index if not exists transfer_jobs_tenant_state_updated_idx
            on transfer_jobs(tenant, state, updated_at);
        create index if not exists transfer_jobs_submitter_state_updated_idx
            on transfer_jobs(submitter, state, updated_at);
        create index if not exists transfer_tasks_job_state_idx
            on transfer_tasks(job_id, run_id, state);
        create index if not exists transfer_tasks_worker_idx
            on transfer_tasks(worker_id, worker_session_id, state);
        create index if not exists transfer_tasks_stale_idx
            on transfer_tasks(state, stale_deadline_at);
        ",
    )
    .map_err(sqlite_err)?;
    Ok(())
}

fn migrate_schema(conn: &Connection, version: i64) -> FsResult<()> {
    match version {
        1 => migrate_sqlite_schema_v1_to_v2(conn)?,
        2..=5 => {}
        _ => {
            return Err(FsError::common(format!(
                "Unsupported sqlite transfer schema version {}, expected {}",
                version, TRANSFER_SCHEMA_VERSION
            )));
        }
    }
    if version < 3 {
        migrate_sqlite_schema_v2_to_v3(conn)?;
    }
    if version < 4 {
        migrate_sqlite_schema_v3_to_v4(conn)?;
    }
    if version < 5 {
        migrate_sqlite_schema_v4_to_v5(conn)?;
    }
    if version != TRANSFER_SCHEMA_VERSION {
        conn.execute(
            "update transfer_schema_version
             set version = ?1, updated_at = strftime('%s', 'now') * 1000
             where id = 1",
            params![TRANSFER_SCHEMA_VERSION],
        )
        .map_err(sqlite_err)?;
    }
    Ok(())
}

fn migrate_sqlite_schema_v2_to_v3(conn: &Connection) -> FsResult<()> {
    if sqlite_column_exists(conn, "transfer_jobs", "mount_snapshot_attempts")? {
        return Ok(());
    }
    conn.execute(
        "alter table transfer_jobs
         add column mount_snapshot_attempts integer not null default 0",
        [],
    )
    .map(|_| ())
    .map_err(sqlite_err)
}

fn migrate_sqlite_schema_v3_to_v4(conn: &Connection) -> FsResult<()> {
    if sqlite_column_exists(conn, "transfer_jobs", "planning_attempts")? {
        return Ok(());
    }
    conn.execute(
        "alter table transfer_jobs
         add column planning_attempts integer not null default 0",
        [],
    )
    .map(|_| ())
    .map_err(sqlite_err)
}

fn migrate_sqlite_schema_v4_to_v5(conn: &Connection) -> FsResult<()> {
    if !sqlite_column_exists(conn, "transfer_jobs", "terminal_summary_json")? {
        conn.execute(
            "alter table transfer_jobs add column terminal_summary_json text",
            [],
        )
        .map(|_| ())
        .map_err(sqlite_err)?;
    }
    if !sqlite_column_exists(conn, "transfer_jobs", "terminal_counts_json")? {
        conn.execute(
            "alter table transfer_jobs add column terminal_counts_json text",
            [],
        )
        .map(|_| ())
        .map_err(sqlite_err)?;
    }
    Ok(())
}

fn migrate_sqlite_schema_v1_to_v2(conn: &Connection) -> FsResult<()> {
    if !sqlite_column_exists(conn, "transfer_jobs", "target_path")? {
        conn.execute(
            "alter table transfer_jobs add column target_path text not null default ''",
            [],
        )
        .map_err(sqlite_err)?;
    }
    let mut stmt = conn
        .prepare("select job_id, command_json from transfer_jobs where target_path = ''")
        .map_err(sqlite_err)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(sqlite_err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_err)?;
    for (job_id, command_json) in rows {
        let command: TransferCommand = serde_json::from_str(&command_json).map_err(|_| {
            FsError::common("Stored transfer command is invalid and cannot be migrated")
        })?;
        conn.execute(
            "update transfer_jobs set target_path = ?1 where job_id = ?2",
            params![command.target_path, job_id],
        )
        .map_err(sqlite_err)?;
    }
    Ok(())
}

fn sqlite_column_exists(conn: &Connection, table: &str, column: &str) -> FsResult<bool> {
    let mut stmt = conn
        .prepare(format!("pragma table_info({table})").as_str())
        .map_err(sqlite_err)?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(sqlite_err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_err)?;
    Ok(columns.iter().any(|name| name == column))
}

fn insert_job(conn: &Connection, job: &TransferJobRecord) -> FsResult<()> {
    conn.execute(
        "insert into transfer_jobs (
            job_id, job_key, run_id, kind, source_path, target_path, command_json,
            mount_snapshot_json, secret_ref_json, cluster_snapshot_version, mount_snapshot_attempts,
            planning_attempts,
            cv_metadata_epoch, state, owner, lease_epoch, lease_expire_at, cancel_requested, summary_json,
            terminal_summary_json, terminal_counts_json,
            client_request_id, submitter, tenant, created_at, updated_at
        ) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17,
                  ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)",
        params![
            job.job_id,
            job.job_key,
            job.run_id as i64,
            job.kind as i32,
            job.source_path,
            job.target_path,
            job.command_json,
            job.mount_snapshot_json,
            job.secret_ref_json,
            job.cluster_snapshot_version as i64,
            job.mount_snapshot_attempts as i64,
            job.planning_attempts as i64,
            job.cv_metadata_epoch.map(|v| v as i64),
            job.state as i32,
            job.owner,
            job.lease_epoch as i64,
            job.lease_expire_at,
            if job.cancel_requested { 1_i64 } else { 0_i64 },
            transfer_progress_json(&job.summary)?,
            job.terminal_summary
                .as_ref()
                .map(transfer_progress_json)
                .transpose()?,
            job.terminal_counts
                .as_ref()
                .map(|counts| serde_json::to_string(counts)
                    .map_err(|_| FsError::common("Unable to encode transfer counts")))
                .transpose()?,
            job.client_request_id,
            job.submitter,
            job.tenant,
            job.created_at,
            job.updated_at
        ],
    )
    .map_err(sqlite_err)?;
    Ok(())
}

fn insert_task(conn: &Connection, task: &TransferTaskRecord) -> FsResult<()> {
    conn.execute(
        "insert or ignore into transfer_tasks (
            job_id, run_id, task_id, attempt_id, source_path, target_path, worker_id,
            worker_session_id, source_read_plan_json, report_target_json, state, progress_json,
            retry_count, attempt_started_at, last_report_at, stale_deadline_at, updated_at
        ) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
        params![
            task.job_id,
            task.run_id as i64,
            task.task_id,
            task.attempt_id as i64,
            task.source_path,
            task.target_path,
            task.worker_id as i64,
            task.worker_session_id,
            task.source_read_plan_json,
            task.report_target_json,
            task.state as i32,
            transfer_progress_json(&task.progress)?,
            task.retry_count as i64,
            task.attempt_started_at,
            task.last_report_at,
            task.stale_deadline_at,
            task.updated_at
        ],
    )
    .map_err(sqlite_err)?;
    Ok(())
}

fn select_job_by_id(conn: &Connection, job_id: &str) -> FsResult<Option<TransferJobRecord>> {
    conn.query_row(
        job_select_sql("where job_id = ?1").as_str(),
        params![job_id],
        sqlite_job_row,
    )
    .optional()
    .map_err(sqlite_err)
}

fn select_job_by_request(
    conn: &Connection,
    submitter: &str,
    client_request_id: &str,
) -> FsResult<Option<TransferJobRecord>> {
    conn.query_row(
        job_select_sql("where submitter = ?1 and client_request_id = ?2").as_str(),
        params![submitter, client_request_id],
        sqlite_job_row,
    )
    .optional()
    .map_err(sqlite_err)
}

fn select_non_terminal_job_by_key(
    conn: &Connection,
    job_key: &str,
) -> FsResult<Option<TransferJobRecord>> {
    conn.query_row(
        job_select_sql("where job_key = ?1 and state not in (6, 7, 8, 9) limit 1").as_str(),
        params![job_key],
        sqlite_job_row,
    )
    .optional()
    .map_err(sqlite_err)
}

fn select_conflicting_active_transfer(
    conn: &Connection,
    target_path: &str,
    submitter: &str,
    client_request_id: &str,
) -> FsResult<Option<TransferJobRecord>> {
    // fs_mode jobs neither conflict nor are conflicted with (see
    // `TransferCommand::is_fs_mode_submitter`).
    if TransferCommand::is_fs_mode_submitter(submitter) {
        return Ok(None);
    }
    let child_prefix = sqlite_like_prefix(target_path);
    let exact_or_child = conn
        .query_row(
            job_select_sql(
                "where state in (2, 3, 4, 5)
               and submitter not like 'fs-mode:%'
               and not (submitter = ?2 and client_request_id = ?3)
               and (
                   target_path = ?1
                   or target_path like ?4 escape '\\'
               )
             limit 1",
            )
            .as_str(),
            params![target_path, submitter, client_request_id, child_prefix],
            sqlite_job_row,
        )
        .optional()
        .map_err(sqlite_err)?;
    if exact_or_child.is_some() {
        return Ok(exact_or_child);
    }

    for ancestor in ancestor_paths(target_path) {
        let ancestor_conflict = conn
            .query_row(
                job_select_sql(
                    "where state in (2, 3, 4, 5)
                       and submitter not like 'fs-mode:%'
                       and not (submitter = ?2 and client_request_id = ?3)
                       and target_path = ?1
                     limit 1",
                )
                .as_str(),
                params![ancestor, submitter, client_request_id],
                sqlite_job_row,
            )
            .optional()
            .map_err(sqlite_err)?;
        if ancestor_conflict.is_some() {
            return Ok(ancestor_conflict);
        }
    }
    Ok(None)
}

fn select_executing_targets(conn: &Connection) -> FsResult<Vec<(String, String)>> {
    let mut stmt = conn
        .prepare(
            "select target_path, tenant from transfer_jobs
             where state in (2, 3, 4, 5) and submitter not like 'fs-mode:%'",
        )
        .map_err(sqlite_err)?;
    let rows = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(sqlite_err)?;
    collect_sqlite_rows(rows)
}

fn has_pending_transfer(conn: &Connection, now_ms: i64) -> FsResult<bool> {
    conn.query_row(
        "select exists(
             select 1 from transfer_jobs where state = 1 and lease_expire_at <= ?1
         )",
        params![now_ms],
        |row| row.get(0),
    )
    .map_err(sqlite_err)
}

fn select_pending_transfer(
    conn: &Connection,
    now_ms: i64,
    active_targets: &[(String, String)],
) -> FsResult<Option<(String, i64, i64, i64)>> {
    let mut values = vec![SqliteValue::Integer(now_ms)];
    let mut exclusions = Vec::with_capacity(active_targets.len());
    let mut active_tenants = BTreeSet::new();
    // An executing job on "/" overlaps every path. Only fs_mode jobs, which are
    // exempt from overlap, may still start.
    let mut root_active = false;

    for (target_path, tenant) in active_targets {
        let target_path = normalize_target_path(target_path);
        if target_path == "/" {
            root_active = true;
            continue;
        }
        active_tenants.insert(tenant.as_str());

        let (child_lower, child_upper) = child_path_bounds(&target_path);
        let mut conflicts = vec![
            "target_path = ?".to_string(),
            "(target_path >= ? and target_path < ?)".to_string(),
        ];
        values.push(SqliteValue::Text(target_path.clone()));
        values.push(SqliteValue::Text(child_lower));
        values.push(SqliteValue::Text(child_upper));

        let ancestors = ancestor_paths(&target_path);
        if !ancestors.is_empty() {
            conflicts.push(format!(
                "target_path in ({})",
                std::iter::repeat_n("?", ancestors.len())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            values.extend(ancestors.into_iter().map(SqliteValue::Text));
        }
        exclusions.push(format!("not ({})", conflicts.join(" or ")));
    }
    if root_active {
        // Exclusion parameters are unused when only fs_mode jobs may start.
        values.truncate(1);
    }

    let mut sql = "select job_id, run_id, lease_epoch, state
                   from transfer_jobs
                   where state = 1 and lease_expire_at <= ?"
        .to_string();
    if root_active {
        sql.push_str(" and submitter like 'fs-mode:%'");
    } else if !exclusions.is_empty() {
        sql.push_str(" and (submitter like 'fs-mode:%' or (");
        sql.push_str(&exclusions.join(" and "));
        sql.push_str("))");
    }
    if active_tenants.is_empty() {
        sql.push_str(" order by updated_at asc, job_id asc limit 1");
    } else {
        sql.push_str(" order by case when tenant in (");
        sql.push_str(
            &std::iter::repeat_n("?", active_tenants.len())
                .collect::<Vec<_>>()
                .join(", "),
        );
        sql.push_str(") then 1 else 0 end asc, updated_at asc, job_id asc limit 1");
        values.extend(
            active_tenants
                .into_iter()
                .map(|tenant| SqliteValue::Text(tenant.to_string())),
        );
    }

    conn.query_row(sql.as_str(), params_from_iter(values), |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
    })
    .optional()
    .map_err(sqlite_err)
}

fn sqlite_like_prefix(path: &str) -> String {
    let path = path
        .trim_end_matches('/')
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    format!("{path}/%")
}

fn job_select_sql(where_sql: &str) -> String {
    format!(
        "select job_id, job_key, run_id, kind, source_path, target_path, command_json,
                mount_snapshot_json, secret_ref_json, cluster_snapshot_version, mount_snapshot_attempts,
                planning_attempts,
                cv_metadata_epoch, state, owner, lease_epoch, lease_expire_at, cancel_requested,
                summary_json, client_request_id, submitter, tenant, created_at, updated_at,
                terminal_summary_json, terminal_counts_json
         from transfer_jobs {where_sql}"
    )
}

fn ancestor_paths(target_path: &str) -> Vec<String> {
    if target_path == "/" {
        return Vec::new();
    }
    let mut ancestors = vec!["/".to_string()];
    let trimmed = target_path.trim_matches('/');
    let mut current = String::new();
    let mut parts = trimmed.split('/').peekable();
    while let Some(part) = parts.next() {
        if parts.peek().is_none() {
            break;
        }
        current.push('/');
        current.push_str(part);
        ancestors.push(current.clone());
    }
    ancestors
}

fn normalize_target_path(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        trimmed.to_string()
    }
}

fn child_path_bounds(target_path: &str) -> (String, String) {
    let lower = format!("{}/", target_path.trim_end_matches('/'));
    let upper = lexicographic_successor(&lower).unwrap_or_else(|| "\u{10ffff}".to_string());
    (lower, upper)
}

fn lexicographic_successor(value: &str) -> Option<String> {
    let mut bytes = value.as_bytes().to_vec();
    for index in (0..bytes.len()).rev() {
        if bytes[index] < u8::MAX {
            bytes[index] += 1;
            bytes.truncate(index + 1);
            return String::from_utf8(bytes).ok();
        }
    }
    None
}

fn sqlite_job_row(row: &Row<'_>) -> rusqlite::Result<TransferJobRecord> {
    let summary_json: String = row.get(18)?;
    let terminal_summary_json: Option<String> = row.get(24)?;
    let terminal_counts_json: Option<String> = row.get(25)?;
    Ok(TransferJobRecord {
        job_id: row.get(0)?,
        job_key: row.get(1)?,
        run_id: row.get::<_, i64>(2)? as u64,
        kind: TransferKind::from(row.get::<_, i32>(3)?),
        source_path: row.get(4)?,
        target_path: row.get(5)?,
        command_json: row.get(6)?,
        mount_snapshot_json: row.get(7)?,
        secret_ref_json: row.get(8)?,
        cluster_snapshot_version: row.get::<_, i64>(9)? as u64,
        mount_snapshot_attempts: row.get::<_, i64>(10)? as u32,
        planning_attempts: row.get::<_, i64>(11)? as u32,
        cv_metadata_epoch: row.get::<_, Option<i64>>(12)?.map(|v| v as u64),
        state: TransferState::from(row.get::<_, i32>(13)?),
        owner: row.get(14)?,
        lease_epoch: row.get::<_, i64>(15)? as u64,
        lease_expire_at: row.get(16)?,
        cancel_requested: row.get::<_, i64>(17)? != 0,
        summary: serde_json::from_str(&summary_json).unwrap_or_default(),
        terminal_summary: terminal_summary_json
            .as_deref()
            .and_then(|text| serde_json::from_str(text).ok()),
        terminal_counts: terminal_counts_json
            .as_deref()
            .and_then(|text| serde_json::from_str(text).ok()),
        client_request_id: row.get(19)?,
        submitter: row.get(20)?,
        tenant: row.get(21)?,
        created_at: row.get(22)?,
        updated_at: row.get(23)?,
    })
}

fn list_filter_sqlite_params(filter: &TransferListFilter) -> (String, Vec<SqliteValue>) {
    let mut clauses: Vec<String> = Vec::new();
    let mut values = Vec::new();
    if let Some(kind) = filter.kind {
        clauses.push("kind = ?".to_string());
        values.push(SqliteValue::Integer(kind as i32 as i64));
    }
    if !filter.states.is_empty() {
        let placeholders = vec!["?"; filter.states.len()].join(", ");
        clauses.push(format!("state in ({placeholders})"));
        for state in &filter.states {
            values.push(SqliteValue::Integer(*state as i32 as i64));
        }
    }
    if let Some(submitter) = &filter.submitter {
        clauses.push("submitter = ?".to_string());
        values.push(SqliteValue::Text(submitter.clone()));
    }
    if let Some(tenant) = &filter.tenant {
        clauses.push("tenant = ?".to_string());
        values.push(SqliteValue::Text(tenant.clone()));
    }
    if clauses.is_empty() {
        (String::new(), values)
    } else {
        (format!("where {}", clauses.join(" and ")), values)
    }
}

fn sqlite_task_row(row: &Row<'_>) -> rusqlite::Result<TransferTaskRecord> {
    let progress_json: String = row.get(11)?;
    Ok(TransferTaskRecord {
        job_id: row.get(0)?,
        run_id: row.get::<_, i64>(1)? as u64,
        task_id: row.get(2)?,
        attempt_id: row.get::<_, i64>(3)? as u64,
        source_path: row.get(4)?,
        target_path: row.get(5)?,
        worker_id: row.get::<_, i64>(6)? as u32,
        worker_session_id: row.get(7)?,
        source_read_plan_json: row.get(8)?,
        report_target_json: row.get(9)?,
        state: TransferTaskState::from(row.get::<_, i32>(10)?),
        progress: serde_json::from_str(&progress_json).unwrap_or_default(),
        estimated_size: 0,
        retry_count: row.get::<_, i64>(12)? as u32,
        attempt_started_at: row.get(13)?,
        last_report_at: row.get(14)?,
        stale_deadline_at: row.get(15)?,
        updated_at: row.get(16)?,
        // Columnar tasks table carries no phase markers; observability skips them.
        accepted_at_ms: 0,
        first_byte_at_ms: 0,
        replace_target: false,
        // Columnar tasks table carries no block locations; segment finalize
        // falls back to the master layout query on this store.
        block_locations: Vec::new(),
    })
}

fn sqlite_task_status_row(row: &Row<'_>) -> rusqlite::Result<TransferTaskStatus> {
    let progress_json: String = row.get(7)?;
    Ok(TransferTaskStatus {
        task_id: row.get(0)?,
        attempt_id: row.get::<_, i64>(1)? as u64,
        source_path: row.get(2)?,
        target_path: row.get(3)?,
        worker_id: row.get::<_, i64>(4)? as u32,
        worker_session_id: row.get(5)?,
        state: TransferTaskState::from(row.get::<_, i32>(6)?),
        progress: serde_json::from_str(&progress_json).unwrap_or_default(),
        retry_count: row.get::<_, i64>(8)? as u32,
        updated_at: row.get(9)?,
    })
}

fn summarize_task_progress(
    rows: impl Iterator<Item = rusqlite::Result<(TransferTaskState, TransferProgress)>>,
    now_ms: i64,
) -> FsResult<TransferTaskSummary> {
    let mut counts = TransferTaskCounts::default();
    let mut loaded_size = 0;
    let mut total_size = 0;
    let mut first_error = None;
    let mut last_message = None;
    for row in rows {
        let (state, progress) = row.map_err(sqlite_err)?;
        loaded_size += progress.loaded_size;
        total_size += progress.total_size;
        match state {
            TransferTaskState::Pending => counts.pending += 1,
            TransferTaskState::Running => counts.running += 1,
            TransferTaskState::Completed => {
                counts.completed += 1;
                counts.completed_size += progress.loaded_size.max(0);
            }
            TransferTaskState::Failed => {
                counts.failed += 1;
                if first_error.is_none() && !progress.message.is_empty() {
                    first_error = Some(progress.message.clone());
                }
            }
            TransferTaskState::Canceled => counts.canceled += 1,
            TransferTaskState::Stale => counts.stale += 1,
        }
        if !progress.message.is_empty() {
            last_message = Some(progress.message);
        }
    }
    Ok(transfer_task_summary_from_aggregate(
        counts,
        loaded_size,
        total_size,
        first_error.as_deref(),
        last_message.as_deref(),
        now_ms,
    ))
}

fn sqlite_tenant_summary_row(row: &Row<'_>) -> rusqlite::Result<TransferTenantSummary> {
    Ok(TransferTenantSummary {
        tenant: row.get(0)?,
        pending: row.get::<_, i64>(1)? as u64,
        executing: row.get::<_, i64>(2)? as u64,
        completed: row.get::<_, i64>(3)? as u64,
        failed: row.get::<_, i64>(4)? as u64,
        canceled: row.get::<_, i64>(5)? as u64,
        partial_success: row.get::<_, i64>(6)? as u64,
        total: row.get::<_, i64>(7)? as u64,
    })
}

fn collect_sqlite_rows<T>(rows: impl Iterator<Item = rusqlite::Result<T>>) -> FsResult<Vec<T>> {
    let mut values = Vec::new();
    for row in rows {
        values.push(row.map_err(sqlite_err)?);
    }
    Ok(values)
}

fn transfer_progress_json(progress: &TransferProgress) -> FsResult<String> {
    serde_json::to_string(progress)
        .map_err(|_| FsError::common("Unable to encode transfer progress"))
}

fn state_list(states: &[TransferState]) -> String {
    states
        .iter()
        .map(|state| (*state as i32).to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn sqlite_err(err: rusqlite::Error) -> FsError {
    log::warn!("transfer SQLite store operation failed: {}", err);
    FsError::transfer_store_unavailable(format!(
        "Transfer metadata store is unavailable; verify transfer.store_url and local disk health: {}",
        store_error_detail(&err)
    ))
}
