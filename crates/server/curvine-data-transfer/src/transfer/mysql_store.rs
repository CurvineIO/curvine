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
    summarize_transfer_tasks, transfer_task_summary_from_aggregate, StaleTaskAttempt,
    TaskAttemptStart, TransferCommand, TransferCountFilter, TransferJobRecord, TransferKind,
    TransferLease, TransferListFilter, TransferProgress, TransferState, TransferStateCounts,
    TransferStateUpdate, TransferTaskCounts, TransferTaskRecord, TransferTaskReport,
    TransferTaskState, TransferTaskStatus, TransferTaskSummary, TransferTenantSummary,
    WorkerInflight,
};
use mysql::prelude::*;
use mysql::{params, IsolationLevel, Params, Pool, PooledConn, TxOpts};
use std::collections::{BTreeMap, BTreeSet};
use std::thread;
use std::time::Duration;

use crate::transfer::{
    ensure_task_transaction_bound, store_error_detail, TransferMountSnapshotUpdate,
    TransferPlannedTasks, TransferRequeueUpdate, TransferStore, TransferTaskStateUpdate,
    MAX_JOBS_PER_STORE_TRANSACTION, MAX_TASKS_PER_STORE_TRANSACTION,
};

const TRANSFER_SCHEMA_VERSION: u64 = 5;
const TRANSFER_SCHEMA_V3: u64 = 3;
const TRANSFER_SCHEMA_V4: u64 = 4;
const MYSQL_SCHEMA_LOCK_NAME: &str = "curvine_transfer_schema";
const MYSQL_SCHEMA_LOCK_TIMEOUT_SECS: u64 = 60;
const MYSQL_MIGRATION_DDL_LOCK_TIMEOUT_SECS: u64 = 60;
const MYSQL_MIGRATION_ROW_LOCK_TIMEOUT_SECS: u64 = 5;
const MYSQL_TASK_RUNTIME_COLUMNS: &[(&str, &str)] = &[
    ("source_path", "varchar(4096) not null default ''"),
    ("target_path", "varchar(4096) not null default ''"),
    ("report_target_json", "longtext null"),
    ("retry_count", "int unsigned not null default 0"),
    ("attempt_started_at", "bigint not null default 0"),
    ("last_report_at", "bigint not null default 0"),
    ("progress_loaded_size", "bigint not null default 0"),
    ("progress_total_size", "bigint not null default 0"),
    ("progress_updated_at", "bigint not null default 0"),
    ("progress_message", "text null"),
];

type TaskSummaryRow = (
    Option<u64>,
    Option<u64>,
    Option<u64>,
    Option<u64>,
    Option<u64>,
    Option<u64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
);

fn mysql_cleanup_lease_owned(
    conn: &mut impl Queryable,
    owner: &str,
    now_ms: i64,
) -> FsResult<bool> {
    conn.exec_first(
        "select 1 from transfer_cleanup_lease
         where id = 1 and owner = :owner and lease_expire_at > :now_ms for update",
        params! { "owner" => owner, "now_ms" => now_ms },
    )
    .map(|row: Option<u8>| row.is_some())
    .map_err(mysql_err)
}

fn mysql_purge_terminal_task_rows(
    conn: &mut impl Queryable,
    older_than_ms: i64,
    task_limit: usize,
) -> FsResult<usize> {
    let task_limit = task_limit.min(MAX_TASKS_PER_STORE_TRANSACTION);
    let keys: Vec<(String, u64, String)> = conn
        .exec(
            "select t.job_id, t.run_id, t.task_id from transfer_tasks t
             join transfer_jobs j on t.job_id = j.job_id and t.run_id = j.run_id
             where j.state in (6, 7, 8, 9) and j.updated_at < :older_than_ms
               and json_type(json_extract(j.record_json, '$.terminal_summary')) = 'OBJECT'
               and json_type(json_extract(j.record_json, '$.terminal_counts')) = 'OBJECT'
             order by j.updated_at asc, t.task_id asc
             limit :limit",
            params! {
                "older_than_ms" => older_than_ms,
                "limit" => task_limit as u64,
            },
        )
        .map_err(mysql_err)?;
    let deleted = keys.len();
    for (job_id, run_id, task_id) in &keys {
        conn.exec_drop(
            "delete from transfer_tasks
             where job_id = :job_id and run_id = :run_id and task_id = :task_id",
            params! {
                "job_id" => job_id,
                "run_id" => run_id,
                "task_id" => task_id,
            },
        )
        .map_err(mysql_err)?;
    }
    Ok(deleted)
}

fn mysql_purge_terminal_job_rows(
    conn: &mut impl Queryable,
    older_than_ms: i64,
    limit: usize,
) -> FsResult<usize> {
    let limit = limit.min(MAX_JOBS_PER_STORE_TRANSACTION);
    let keys: Vec<(String, u64)> = conn
        .exec(
            "select j.job_id, j.run_id from transfer_jobs j
             where j.state in (6, 7, 8, 9) and j.updated_at < :older_than_ms
               and not exists (
                   select 1 from transfer_tasks t
                   where t.job_id = j.job_id and t.run_id = j.run_id
               )
             order by j.updated_at asc
             limit :limit",
            params! {
                "older_than_ms" => older_than_ms,
                "limit" => limit as u64,
            },
        )
        .map_err(mysql_err)?;
    for (job_id, run_id) in &keys {
        conn.exec_drop(
            "delete from transfer_jobs
             where job_id = :job_id and run_id = :run_id and state in (6, 7, 8, 9)
               and not exists (
                   select 1 from transfer_tasks t
                   where t.job_id = :job_id and t.run_id = :run_id
               )",
            params! {
                "job_id" => job_id,
                "run_id" => run_id,
            },
        )
        .map_err(mysql_err)?;
    }
    Ok(keys.len())
}

pub struct MysqlTransferStore {
    pool: Pool,
}

impl MysqlTransferStore {
    pub fn open(url: &str) -> FsResult<Self> {
        let pool = Pool::new(url).map_err(mysql_err)?;
        let mut conn = pool.get_conn().map_err(mysql_err)?;
        if !mysql_schema_ready(&mut conn)? {
            configure_mysql_migration_connection(&mut conn)?;
            init_schema(&mut conn)?;
        }
        Ok(Self { pool })
    }

    fn conn(&self) -> FsResult<PooledConn> {
        self.pool.get_conn().map_err(mysql_err)
    }

    fn task_runtime_projection_ready(&self) -> bool {
        // `record_json` is authoritative. Runtime columns are write-through caches
        // for new writes only; historical rows are never rewritten to enable them.
        false
    }
}

impl TransferStore for MysqlTransferStore {
    fn check_available(&self) -> FsResult<()> {
        self.conn()?.query_drop("select 1").map_err(mysql_err)
    }

    fn create_or_get_by_request_id(&self, job: TransferJobRecord) -> FsResult<TransferJobRecord> {
        self.create_or_get_by_request_id_checked(job)
    }

    fn create_or_get_by_request_id_checked(
        &self,
        job: TransferJobRecord,
    ) -> FsResult<TransferJobRecord> {
        const MAX_PERSIST_RETRIES: usize = 3;

        for attempt in 0..MAX_PERSIST_RETRIES {
            let mut conn = self.conn()?;
            match insert_job_raw(&mut conn, job_params(&job)?) {
                Ok(()) => return Ok(job),
                Err(err) if mysql_error_code(&err) == Some(1062) => {
                    let existing =
                        select_job_by_request(&mut conn, &job.submitter, &job.client_request_id)?
                            .ok_or_else(|| {
                            FsError::transfer_store_unavailable(
                                "Transfer request duplicate was committed but could not be read",
                            )
                        })?;
                    if existing.command_json != job.command_json {
                        return Err(FsError::common(format!(
                            "Transfer request ID {} submitted by {} is already bound to job {} with a different command",
                            job.client_request_id, job.submitter, existing.job_id
                        )));
                    }
                    return Ok(existing);
                }
                Err(err)
                    if matches!(mysql_error_code(&err), Some(1205 | 1213))
                        && attempt + 1 < MAX_PERSIST_RETRIES =>
                {
                    log::warn!(
                        "retrying transfer submit persistence after MySQL transient error code={} attempt={}/{}",
                        mysql_error_code(&err).unwrap_or_default(),
                        attempt + 1,
                        MAX_PERSIST_RETRIES
                    );
                    thread::sleep(Duration::from_millis(2_u64.saturating_pow(attempt as u32)));
                }
                Err(err) => return Err(mysql_err(err)),
            }
        }

        unreachable!("transfer submit persistence retry loop must return")
    }

    fn get_transfer(&self, job_id: &str) -> FsResult<Option<TransferJobRecord>> {
        select_job_by_id(&mut self.conn()?, job_id)
    }

    fn get_transfer_by_request(
        &self,
        submitter: &str,
        client_request_id: &str,
    ) -> FsResult<Option<TransferJobRecord>> {
        select_job_by_request(&mut self.conn()?, submitter, client_request_id)
    }

    fn list_active_transfers(&self) -> FsResult<Vec<TransferJobRecord>> {
        select_jobs(
            &mut self.conn()?,
            "select record_json from transfer_jobs where state not in (6, 7, 8, 9)",
            Params::Empty,
        )
    }

    fn has_active_transfer_by_key(&self, job_key: &str) -> FsResult<bool> {
        Ok(select_non_terminal_job_by_key(&mut self.conn()?, job_key)?.is_some())
    }

    fn find_conflicting_active_transfer(
        &self,
        target_path: &str,
        submitter: &str,
        client_request_id: &str,
    ) -> FsResult<Option<TransferJobRecord>> {
        select_conflicting_active_transfer(
            &mut self.conn()?,
            target_path,
            submitter,
            client_request_id,
        )
    }

    fn count_active_transfers(&self) -> FsResult<u64> {
        self.conn()?
            .exec_first::<u64, _, _>(
                "select count(*) from transfer_jobs where state in (1, 2, 3, 4, 5)",
                Params::Empty,
            )
            .map_err(mysql_err)?
            .ok_or_else(|| FsError::common("Failed to count active transfer jobs"))
    }

    fn count_executing_transfers(&self) -> FsResult<u64> {
        self.conn()?
            .exec_first::<u64, _, _>(
                "select count(*) from transfer_jobs where state in (2, 3, 4, 5)",
                Params::Empty,
            )
            .map_err(mysql_err)?
            .ok_or_else(|| FsError::common("Failed to count executing transfer jobs"))
    }

    fn list_transfers(&self, filter: TransferListFilter) -> FsResult<Vec<TransferJobRecord>> {
        let (where_sql, mut values) = job_where_mysql(
            filter.kind,
            &filter.states,
            filter.submitter.as_deref(),
            filter.tenant.as_deref(),
        );
        let sql = format!(
            "select record_json from transfer_jobs {where_sql}
             order by updated_at desc, created_at desc, job_id desc
             limit ? offset ?"
        );
        values.push(mysql::Value::from(filter.limit as u64));
        values.push(mysql::Value::from(filter.offset as u64));
        select_jobs(&mut self.conn()?, sql.as_str(), Params::Positional(values))
    }

    fn count_transfers_by_state(
        &self,
        filter: &TransferCountFilter,
    ) -> FsResult<TransferStateCounts> {
        let (where_sql, values) = job_where_mysql(
            filter.kind,
            &[],
            filter.submitter.as_deref(),
            filter.tenant.as_deref(),
        );
        let sql = format!("select state, count(*) from transfer_jobs {where_sql} group by state");
        let params = if values.is_empty() {
            Params::Empty
        } else {
            Params::Positional(values)
        };
        let rows: Vec<(i32, u64)> = self.conn()?.exec(sql.as_str(), params).map_err(mysql_err)?;
        let mut counts = TransferStateCounts::default();
        for (state, count) in rows {
            counts.add(TransferState::from(state), count);
        }
        Ok(counts)
    }

    fn list_tenant_summaries(
        &self,
        limit: usize,
        offset: usize,
    ) -> FsResult<Vec<TransferTenantSummary>> {
        let mut summaries = BTreeMap::new();
        for job in select_jobs(
            &mut self.conn()?,
            "select record_json from transfer_jobs",
            Params::Empty,
        )? {
            let summary =
                summaries
                    .entry(job.tenant.clone())
                    .or_insert_with(|| TransferTenantSummary {
                        tenant: job.tenant.clone(),
                        ..Default::default()
                    });
            summary.total = summary.total.saturating_add(1);
            match job.state {
                TransferState::Pending => summary.pending = summary.pending.saturating_add(1),
                state if state.is_executing() => {
                    summary.executing = summary.executing.saturating_add(1)
                }
                TransferState::Completed => summary.completed = summary.completed.saturating_add(1),
                TransferState::Failed => summary.failed = summary.failed.saturating_add(1),
                TransferState::Canceled => summary.canceled = summary.canceled.saturating_add(1),
                TransferState::PartialSuccess => {
                    summary.partial_success = summary.partial_success.saturating_add(1)
                }
                _ => {}
            }
        }
        let mut summaries = summaries.into_values().collect::<Vec<_>>();
        summaries.sort_by(|left, right| {
            right
                .active()
                .cmp(&left.active())
                .then(right.total.cmp(&left.total))
                .then(left.tenant.cmp(&right.tenant))
        });
        Ok(summaries.into_iter().skip(offset).take(limit).collect())
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
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        let deleted = mysql_purge_terminal_task_rows(&mut tx, older_than_ms, task_limit)?;
        tx.commit().map_err(mysql_err)?;
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
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        if !mysql_cleanup_lease_owned(&mut tx, owner, now_ms)? {
            tx.commit().map_err(mysql_err)?;
            return Ok(0);
        }
        let deleted = mysql_purge_terminal_task_rows(&mut tx, older_than_ms, task_limit)?;
        tx.commit().map_err(mysql_err)?;
        Ok(deleted)
    }

    fn purge_terminal_jobs(&self, older_than_ms: i64, limit: usize) -> FsResult<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let limit = limit.min(MAX_JOBS_PER_STORE_TRANSACTION);
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        let deleted = mysql_purge_terminal_job_rows(&mut tx, older_than_ms, limit)?;
        tx.commit().map_err(mysql_err)?;
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
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        if !mysql_cleanup_lease_owned(&mut tx, owner, now_ms)? {
            tx.commit().map_err(mysql_err)?;
            return Ok(0);
        }
        let deleted = mysql_purge_terminal_job_rows(&mut tx, older_than_ms, limit)?;
        tx.commit().map_err(mysql_err)?;
        Ok(deleted)
    }

    fn try_acquire_cleanup_lease(&self, owner: &str, now_ms: i64, lease_ms: i64) -> FsResult<bool> {
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        // One conditional upsert makes first acquisition atomic too. A
        // select-then-insert lets two empty-table transactions both claim it.
        tx.exec_drop(
            "insert into transfer_cleanup_lease(id, owner, lease_expire_at)
             values (1, :owner, :lease_expire_at)
             on duplicate key update
                 owner=if(owner=values(owner) or lease_expire_at<=:now_ms, values(owner), owner),
                 lease_expire_at=if(owner=values(owner) or lease_expire_at<=:now_ms,
                                    values(lease_expire_at), lease_expire_at)",
            params! {
                "owner" => owner,
                "lease_expire_at" => now_ms.saturating_add(lease_ms.max(1)),
                "now_ms" => now_ms,
            },
        )
        .map_err(mysql_err)?;
        let current_owner: Option<String> = tx
            .query_first("select owner from transfer_cleanup_lease where id = 1")
            .map_err(mysql_err)?;
        tx.commit().map_err(mysql_err)?;
        Ok(current_owner.as_deref() == Some(owner))
    }

    fn list_transfer_tasks(&self, job_id: &str, run_id: u64) -> FsResult<Vec<TransferTaskRecord>> {
        select_tasks(
            &mut self.conn()?,
            job_id,
            run_id,
            None,
            None,
            self.task_runtime_projection_ready(),
        )
    }

    fn count_tasks_global(&self) -> FsResult<TransferTaskCounts> {
        let mut counts = TransferTaskCounts::default();
        for row in self
            .conn()?
            .exec::<(i32, u64), _, _>(
                "select state, count(*) from transfer_tasks group by state",
                Params::Empty,
            )
            .map_err(mysql_err)?
        {
            match row.0 {
                1 => counts.pending += row.1,
                2 => counts.running += row.1,
                3 => counts.completed += row.1,
                4 => counts.failed += row.1,
                5 => counts.canceled += row.1,
                6 => counts.stale += row.1,
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
        if !self.task_runtime_projection_ready() {
            return Ok(summarize_transfer_tasks(
                &self.list_transfer_tasks(job_id, run_id)?,
                now_ms,
            ));
        }
        select_task_summary(&mut self.conn()?, job_id, run_id, now_ms)
    }

    fn list_transfer_task_status_page(
        &self,
        job_id: &str,
        run_id: u64,
        offset: usize,
        limit: usize,
    ) -> FsResult<(Vec<TransferTaskStatus>, bool)> {
        if !self.task_runtime_projection_ready() {
            let mut tasks = self.list_transfer_tasks(job_id, run_id)?;
            tasks.sort_by(|left, right| left.task_id.cmp(&right.task_id));
            let mut page = tasks
                .into_iter()
                .skip(offset)
                .take(limit.saturating_add(1))
                .collect::<Vec<_>>();
            let has_next = page.len() > limit;
            page.truncate(limit);
            return Ok((page.into_iter().map(Into::into).collect(), has_next));
        }
        select_task_status_page(&mut self.conn()?, job_id, run_id, offset, limit)
    }

    fn request_cancel(&self, job_id: &str, run_id: u64, now_ms: i64) -> FsResult<bool> {
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        let mut job = match select_job_by_id_for_update(&mut tx, job_id)? {
            Some(job) if job.run_id == run_id && !job.state.is_terminal() => job,
            _ => {
                tx.commit().map_err(mysql_err)?;
                return Ok(false);
            }
        };
        job.cancel_requested = true;
        job.state = TransferState::Canceling;
        job.summary.message = "cancel requested".to_string();
        job.summary.update_time = now_ms;
        job.updated_at = now_ms;
        let updated = exec_update_job(&mut tx, &job)?;
        tx.commit().map_err(mysql_err)?;
        Ok(updated)
    }

    fn acquire_runnable_transfer(
        &self,
        owner: &str,
        lease_ms: i64,
        now_ms: i64,
        max_executing_transfers: u64,
        prefer_pending: bool,
    ) -> FsResult<Option<TransferLease>> {
        let mut conn = self.conn()?;
        let running_check_interval_ms =
            crate::transfer::running_transfer_check_interval_ms(lease_ms);
        let running_event_lease_before_ms =
            now_ms.saturating_add(lease_ms.saturating_sub(running_check_interval_ms));
        let running_renew_lease_before_ms = now_ms.saturating_add(lease_ms / 2);
        // Conflict reads below are non-locking, so they need fresh committed state.
        let mut tx = conn
            .start_transaction(
                TxOpts::default().set_isolation_level(Some(IsolationLevel::ReadCommitted)),
            )
            .map_err(mysql_err)?;

        tx.exec_first::<u64, _, _>(
            "select version from transfer_schema_version where id = 1 for update",
            Params::Empty,
        )
        .map_err(mysql_err)?
        .ok_or_else(|| FsError::common("Missing mysql transfer schema version"))?;
        let mut job: Option<(String, u64, u64, i32)> = if !prefer_pending {
            tx.exec_first(
                "select job_id, run_id, lease_epoch, state
                 from transfer_jobs as job
                 where job.owner = :owner and job.lease_expire_at > :now_ms
                   and (
                        (job.state = 5 and job.updated_at < :now_ms)
                        or (
                            job.state = 4 and (
                                job.lease_expire_at <= :running_renew_lease_before_ms
                                or (
                                    job.lease_expire_at <= :running_event_lease_before_ms and (
                                        exists (
                                            select 1 from transfer_tasks as task
                                            where task.job_id = job.job_id and task.run_id = job.run_id
                                              and task.state in (1, 4, 6)
                                        )
                                        or exists (
                                            select 1 from transfer_tasks as task
                                            where task.job_id = job.job_id and task.run_id = job.run_id
                                              and task.state = 2 and task.stale_deadline_at < :now_ms
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
                params! {
                    "owner" => owner,
                    "now_ms" => now_ms,
                    "running_event_lease_before_ms" => running_event_lease_before_ms,
                    "running_renew_lease_before_ms" => running_renew_lease_before_ms,
                },
            )
            .map_err(mysql_err)?
        } else {
            None
        };
        if job.is_none() {
            job = tx
                .exec_first(
                    "select job_id, run_id, lease_epoch, state
                 from transfer_jobs
                 where state in (2, 3, 4, 5) and lease_expire_at <= :now_ms
                 order by updated_at asc
                 limit 1",
                    params! { "now_ms" => now_ms },
                )
                .map_err(mysql_err)?;
        }
        if job.is_none() && has_pending_transfer(&mut tx, now_ms)? {
            let executing = tx
                .exec_first::<u64, _, _>(
                    "select count(*) from transfer_jobs where state in (2, 3, 4, 5)",
                    Params::Empty,
                )
                .map_err(mysql_err)?
                .ok_or_else(|| FsError::common("Failed to count executing transfer jobs"))?;
            if executing < max_executing_transfers {
                let active_targets = select_executing_targets(&mut tx)?;
                job = select_pending_transfer(&mut tx, now_ms, &active_targets)?;
            }
        }
        let Some((job_id, run_id, lease_epoch, state)) = job else {
            tx.commit().map_err(mysql_err)?;
            return Ok(None);
        };
        let mut record =
            select_job_by_id(&mut tx, &job_id)?.ok_or_else(|| FsError::job_not_found(&job_id))?;
        let conflict = if state != TransferState::Pending as i32 {
            select_conflicting_active_transfer(
                &mut tx,
                &record.target_path,
                &record.submitter,
                &record.client_request_id,
            )?
        } else {
            None
        };
        if let Some(existing) = conflict {
            record.state = TransferState::Failed;
            record.owner.clear();
            record.lease_expire_at = 0;
            record.summary.message = format!(
                "target {} conflicts with active transfer {} target {}",
                record.target_path, existing.job_id, existing.target_path
            );
            record.summary.update_time = now_ms;
            record.updated_at = now_ms;
            exec_update_job(&mut tx, &record)?;
            tx.commit().map_err(mysql_err)?;
            return Ok(None);
        }
        if state == TransferState::Pending as i32 {
            record.state = TransferState::Planning;
            record.summary.message = "acquired for planning".to_string();
            record.summary.update_time = now_ms;
        }
        record.owner = owner.to_string();
        record.lease_epoch = lease_epoch.saturating_add(1);
        record.lease_expire_at = now_ms.saturating_add(lease_ms);
        record.updated_at = now_ms;
        let affected = exec_affected(
            &mut tx,
            "update transfer_jobs
             set state = :state, owner = :owner, lease_epoch = :new_epoch, lease_expire_at = :lease_expire_at,
                 record_json = :record_json, updated_at = :updated_at
             where job_id = :job_id and run_id = :run_id and lease_epoch = :old_epoch",
            params! {
                "state" => record.state as i32,
                "owner" => owner,
                "new_epoch" => record.lease_epoch,
                "lease_expire_at" => record.lease_expire_at,
                "record_json" => json(&record)?,
                "updated_at" => now_ms,
                "job_id" => &job_id,
                "run_id" => run_id,
                "old_epoch" => lease_epoch,
            },
        )?;
        tx.commit().map_err(mysql_err)?;
        if affected == 0 {
            return Ok(None);
        }
        Ok(Some(TransferLease {
            job_id,
            run_id,
            owner: owner.to_string(),
            lease_epoch: record.lease_epoch,
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
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        let mut job = match select_job_by_id_for_update(&mut tx, job_id)? {
            Some(job)
                if job.run_id == run_id
                    && job.owner == owner
                    && job.lease_epoch == lease_epoch
                    && job.lease_expire_at > now_ms
                    && !job.state.is_terminal() =>
            {
                job
            }
            _ => {
                tx.commit().map_err(mysql_err)?;
                return Ok(false);
            }
        };
        job.lease_expire_at = now_ms.saturating_add(lease_ms);
        job.updated_at = now_ms;
        let updated = exec_update_job(&mut tx, &job)?;
        tx.commit().map_err(mysql_err)?;
        Ok(updated)
    }

    fn update_transfer_state(&self, update: TransferStateUpdate) -> FsResult<bool> {
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        let mut job = match select_job_by_id_for_update(&mut tx, &update.job_id)? {
            Some(job)
                if job.run_id == update.run_id
                    && job.owner == update.owner
                    && job.lease_epoch == update.lease_epoch
                    && job.lease_expire_at > update.now_ms
                    && update.from_states.contains(&job.state) =>
            {
                job
            }
            _ => {
                tx.commit().map_err(mysql_err)?;
                return Ok(false);
            }
        };
        job.state = update.to_state;
        job.summary.message = update.message;
        job.summary.update_time = update.now_ms;
        job.updated_at = update.now_ms;
        let updated = exec_update_job(&mut tx, &job)?;
        tx.commit().map_err(mysql_err)?;
        Ok(updated)
    }

    fn requeue_transfer(&self, update: TransferRequeueUpdate) -> FsResult<bool> {
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        let mut job = match select_job_by_id_for_update(&mut tx, &update.job_id)? {
            Some(job)
                if job.run_id == update.run_id
                    && job.owner == update.owner
                    && job.lease_epoch == update.lease_epoch
                    && job.lease_expire_at > update.now_ms
                    && job.state == TransferState::Planning =>
            {
                job
            }
            _ => {
                tx.commit().map_err(mysql_err)?;
                return Ok(false);
            }
        };
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
        let updated = exec_update_job(&mut tx, &job)?;
        tx.commit().map_err(mysql_err)?;
        Ok(updated)
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
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        let mut job = match select_job_by_id_for_update(&mut tx, job_id)? {
            Some(job)
                if job.run_id == run_id
                    && job.owner == owner
                    && job.lease_epoch == lease_epoch
                    && job.lease_expire_at > now_ms
                    && !job.state.is_terminal()
                    && job
                        .cv_metadata_epoch
                        .is_none_or(|existing| existing == cv_metadata_epoch) =>
            {
                job
            }
            _ => {
                tx.commit().map_err(mysql_err)?;
                return Ok(false);
            }
        };
        job.cv_metadata_epoch = Some(cv_metadata_epoch);
        job.updated_at = now_ms;
        let updated = exec_update_job(&mut tx, &job)?;
        tx.commit().map_err(mysql_err)?;
        Ok(updated)
    }

    fn set_transfer_mount_snapshot(&self, update: TransferMountSnapshotUpdate) -> FsResult<bool> {
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        let Some(mut job) = select_job_by_id_for_update(&mut tx, &update.job_id)? else {
            tx.commit().map_err(mysql_err)?;
            return Ok(false);
        };
        if job.run_id != update.run_id
            || job.owner != update.owner
            || job.lease_epoch != update.lease_epoch
            || job.lease_expire_at <= update.now_ms
            || job.state.is_terminal()
        {
            tx.commit().map_err(mysql_err)?;
            return Ok(false);
        }
        if !job.mount_snapshot_json.trim().is_empty() && job.mount_snapshot_json != "{}" {
            let matches = job.mount_snapshot_json == update.mount_snapshot_json;
            tx.commit().map_err(mysql_err)?;
            return Ok(matches);
        }
        job.mount_snapshot_json = update.mount_snapshot_json;
        job.cluster_snapshot_version = update.cluster_snapshot_version;
        job.updated_at = update.now_ms;
        let updated = exec_affected(
            &mut tx,
            "update transfer_jobs
             set record_json = :record_json, updated_at = :updated_at
             where job_id = :job_id and run_id = :run_id and owner = :owner
               and lease_epoch = :lease_epoch and lease_expire_at > :updated_at
               and state not in (6, 7, 8, 9)",
            params! {
                "record_json" => json(&job)?,
                "updated_at" => job.updated_at,
                "job_id" => &job.job_id,
                "run_id" => job.run_id,
                "owner" => &job.owner,
                "lease_epoch" => job.lease_epoch,
            },
        )? > 0;
        tx.commit().map_err(mysql_err)?;
        Ok(updated)
    }

    fn insert_tasks(&self, tasks: Vec<TransferTaskRecord>) -> FsResult<()> {
        ensure_task_transaction_bound(tasks.len())?;
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        for task in tasks {
            insert_task(&mut tx, &task)?;
        }
        tx.commit().map_err(mysql_err)?;
        Ok(())
    }

    fn append_planned_tasks(&self, update: TransferPlannedTasks) -> FsResult<bool> {
        ensure_task_transaction_bound(update.tasks.len())?;
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        match select_job_by_id_for_update(&mut tx, &update.job_id)? {
            Some(job)
                if job.run_id == update.run_id
                    && job.owner == update.owner
                    && job.lease_epoch == update.lease_epoch
                    && job.lease_expire_at > update.now_ms
                    && job.state == TransferState::Planning => {}
            _ => {
                tx.commit().map_err(mysql_err)?;
                return Ok(false);
            }
        }
        for task in update.tasks {
            insert_task(&mut tx, &task)?;
        }
        tx.commit().map_err(mysql_err)?;
        Ok(true)
    }

    fn persist_planned_tasks(&self, update: TransferPlannedTasks) -> FsResult<bool> {
        ensure_task_transaction_bound(update.tasks.len())?;
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        let mut job = match select_job_by_id_for_update(&mut tx, &update.job_id)? {
            Some(job)
                if job.run_id == update.run_id
                    && job.owner == update.owner
                    && job.lease_epoch == update.lease_epoch
                    && job.lease_expire_at > update.now_ms
                    && job.state == TransferState::Planning =>
            {
                job
            }
            _ => {
                tx.commit().map_err(mysql_err)?;
                return Ok(false);
            }
        };
        for task in update.tasks {
            insert_task(&mut tx, &task)?;
        }
        job.state = TransferState::Dispatching;
        job.summary.message = update.message;
        job.summary.update_time = update.now_ms;
        job.updated_at = update.now_ms;
        let updated = exec_update_job(&mut tx, &job)?;
        tx.commit().map_err(mysql_err)?;
        Ok(updated)
    }

    fn has_transfer_tasks(&self, job_id: &str, run_id: u64) -> FsResult<bool> {
        self.conn()?
            .exec_first(
                "select 1 from transfer_tasks where job_id = :job_id and run_id = :run_id limit 1",
                params! { "job_id" => job_id, "run_id" => run_id },
            )
            .map(|row: Option<u8>| row.is_some())
            .map_err(mysql_err)
    }

    fn update_task_state(&self, update: TransferTaskStateUpdate) -> FsResult<bool> {
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        match select_job_by_id_for_update(&mut tx, &update.job_id)? {
            Some(job)
                if job.run_id == update.run_id
                    && job.owner == update.owner
                    && job.lease_epoch == update.lease_epoch
                    && job.lease_expire_at > update.now_ms
                    && !job.state.is_terminal() =>
            {
                job
            }
            _ => {
                tx.commit().map_err(mysql_err)?;
                return Ok(false);
            }
        };
        let mut task = match select_task_for_update(
            &mut tx,
            &update.job_id,
            update.run_id,
            &update.task_id,
            self.task_runtime_projection_ready(),
        )? {
            Some(task) => task,
            None => {
                tx.commit().map_err(mysql_err)?;
                return Ok(false);
            }
        };
        if !update.from_states.is_empty() && !update.from_states.contains(&task.state) {
            tx.commit().map_err(mysql_err)?;
            return Ok(false);
        }
        task.state = update.state;
        if update.bump_retry {
            task.retry_count = task.retry_count.saturating_add(1);
        }
        task.progress.message = update.message;
        task.progress.update_time = update.now_ms;
        task.updated_at = update.now_ms;
        let updated = exec_update_task(&mut tx, &task)?;
        tx.commit().map_err(mysql_err)?;
        Ok(updated)
    }

    fn claim_pending_tasks(
        &self,
        job_id: &str,
        run_id: u64,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>> {
        select_tasks(
            &mut self.conn()?,
            job_id,
            run_id,
            Some(TransferTaskState::Pending),
            Some(limit),
            self.task_runtime_projection_ready(),
        )
    }

    fn select_inflight_by_worker(&self) -> FsResult<Vec<WorkerInflight>> {
        // 0-byte rows (legacy tasks whose size is unknown) are counted and the
        // scheduler substitutes the configured default estimate per row, so a
        // batch of unknown tasks cannot bypass the byte budget (design §3.1).
        let sql = if self.task_runtime_projection_ready() {
            "select worker_id, count(*), coalesce(sum(progress_total_size), 0), count(nullif(progress_total_size, 0))
             from transfer_tasks where state = 2
             group by worker_id"
        } else {
            "select worker_id, count(*), coalesce(sum(cast(json_unquote(json_extract(record_json, '$.progress.total_size')) as signed)), 0), count(nullif(cast(json_unquote(json_extract(record_json, '$.progress.total_size')) as signed), 0))
             from transfer_tasks where state = 2
             group by worker_id"
        };
        let rows: Vec<(u32, i64, Option<i64>, i64)> =
            self.conn()?.exec(sql, ()).map_err(mysql_err)?;
        Ok(rows
            .into_iter()
            .map(|(worker_id, task_count, bytes, nonzero)| WorkerInflight {
                worker_id,
                task_count: u64::try_from(task_count).unwrap_or(u64::MAX),
                estimated_bytes: bytes.unwrap_or(0),
                unknown_bytes_count: u64::try_from(task_count.saturating_sub(nonzero)).unwrap_or(0),
            })
            .collect())
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
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        let valid_owner: Option<u8> = tx
            .exec_first(
                "select 1 from transfer_jobs
                 where job_id = :job_id and run_id = :run_id and owner = :owner
                   and lease_epoch = :lease_epoch and lease_expire_at > :now_ms",
                params! {
                    "job_id" => job_id,
                    "run_id" => run_id,
                    "owner" => owner,
                    "lease_epoch" => lease_epoch,
                    "now_ms" => now_ms,
                },
            )
            .map_err(mysql_err)?;
        if valid_owner.is_none() {
            tx.commit().map_err(mysql_err)?;
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
            let updated = exec_affected(
                &mut tx,
                "update transfer_tasks
                 set state = :state, retry_count = :retry_count,
                     progress_updated_at = :progress_updated_at,
                     progress_message = :progress_message, record_json = :record_json,
                     updated_at = :updated_at
                 where job_id = :job_id and run_id = :run_id and task_id = :task_id
                   and state = 2 and attempt_id = :attempt_id and worker_id = :worker_id
                   and worker_session_id = :worker_session_id
                   and stale_deadline_at = :stale_deadline_at
                   and stale_deadline_at < :now_ms
                   and exists (
                       select 1 from transfer_jobs
                       where job_id = :job_id and run_id = :run_id and owner = :owner
                         and lease_epoch = :lease_epoch and lease_expire_at > :now_ms
                   )",
                params! {
                    "state" => task.state as i32,
                    "retry_count" => task.retry_count,
                    "progress_updated_at" => task.progress.update_time,
                    "progress_message" => &task.progress.message,
                    "record_json" => json(&task)?,
                    "updated_at" => task.updated_at,
                    "job_id" => job_id,
                    "run_id" => run_id,
                    "task_id" => &task.task_id,
                    "attempt_id" => candidate.attempt_id,
                    "worker_id" => candidate.worker_id,
                    "worker_session_id" => &candidate.worker_session_id,
                    "stale_deadline_at" => candidate.stale_deadline_at,
                    "now_ms" => now_ms,
                    "owner" => owner,
                    "lease_epoch" => lease_epoch,
                },
            )? > 0;
            if updated {
                stale.push(StaleTaskAttempt { task });
            }
        }
        tx.commit().map_err(mysql_err)?;
        Ok(stale)
    }

    fn list_stale_running_tasks(
        &self,
        job_id: &str,
        run_id: u64,
        stale_before_ms: i64,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>> {
        if !self.task_runtime_projection_ready() {
            let rows: Vec<String> = self
                .conn()?
                .exec(
                    "select record_json from transfer_tasks
                     where job_id = :job_id and run_id = :run_id and state = 2
                       and stale_deadline_at < :stale_before_ms
                     order by stale_deadline_at asc
                     limit :limit",
                    params! {
                        "job_id" => job_id,
                        "run_id" => run_id,
                        "stale_before_ms" => stale_before_ms,
                        "limit" => limit as u64,
                    },
                )
                .map_err(mysql_err)?;
            return rows
                .into_iter()
                .map(|row| serde_json::from_str(&row).map_err(json_err))
                .collect();
        }
        let rows: Vec<mysql::Row> = self
            .conn()?
            .exec(
                "select record_json, state, attempt_id, worker_id, worker_session_id,
                        stale_deadline_at, report_target_json, retry_count, attempt_started_at,
                        last_report_at, progress_loaded_size, progress_total_size,
                        progress_updated_at, progress_message, updated_at
                 from transfer_tasks
                 where job_id = :job_id and run_id = :run_id and state = 2
                   and stale_deadline_at < :stale_before_ms
                 order by stale_deadline_at asc
                 limit :limit",
                params! {
                    "job_id" => job_id,
                    "run_id" => run_id,
                    "stale_before_ms" => stale_before_ms,
                    "limit" => limit as u64,
                },
            )
            .map_err(mysql_err)?;
        rows.into_iter().map(|row| task_from_row(&row)).collect()
    }

    fn list_tasks_by_state(
        &self,
        job_id: &str,
        run_id: u64,
        state: TransferTaskState,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>> {
        select_tasks(
            &mut self.conn()?,
            job_id,
            run_id,
            Some(state),
            Some(limit),
            self.task_runtime_projection_ready(),
        )
    }

    fn list_failed_tasks(&self, job_id: &str, run_id: u64) -> FsResult<Vec<TransferTaskRecord>> {
        select_tasks(
            &mut self.conn()?,
            job_id,
            run_id,
            Some(TransferTaskState::Failed),
            None,
            self.task_runtime_projection_ready(),
        )
    }

    fn has_failed_tasks(&self, job_id: &str, run_id: u64) -> FsResult<bool> {
        let result: Option<u8> = self
            .conn()?
            .exec_first(
                "select 1 from transfer_tasks
                 where job_id = :job_id and run_id = :run_id and state = :state
                 limit 1",
                params! {
                    "job_id" => job_id,
                    "run_id" => run_id,
                    "state" => TransferTaskState::Failed as i32,
                },
            )
            .map_err(mysql_err)?;
        Ok(result.is_some())
    }

    fn has_recoverable_tasks(&self, job_id: &str, run_id: u64) -> FsResult<bool> {
        let result: Option<u8> = self
            .conn()?
            .exec_first(
                "select 1 from transfer_tasks
                 where job_id = :job_id and run_id = :run_id and state in (1, 2, 6)
                 limit 1",
                params! { "job_id" => job_id, "run_id" => run_id },
            )
            .map_err(mysql_err)?;
        Ok(result.is_some())
    }

    fn start_task_attempt(&self, start: TaskAttemptStart) -> FsResult<bool> {
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        let Some(job) = select_job_by_id_for_update(&mut tx, &start.job_id)? else {
            tx.commit().map_err(mysql_err)?;
            return Ok(false);
        };
        if job.run_id != start.run_id
            || job.owner != start.owner
            || job.lease_epoch != start.lease_epoch
            || job.lease_expire_at <= start.now_ms
            || job.cancel_requested
            || job.state.is_terminal()
        {
            tx.commit().map_err(mysql_err)?;
            return Ok(false);
        }
        let mut task = match select_task_for_update(
            &mut tx,
            &start.job_id,
            start.run_id,
            &start.task_id,
            self.task_runtime_projection_ready(),
        )? {
            Some(task)
                if matches!(
                    task.state,
                    TransferTaskState::Pending | TransferTaskState::Stale
                ) =>
            {
                task
            }
            _ => {
                tx.commit().map_err(mysql_err)?;
                return Ok(false);
            }
        };
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
        let updated = exec_update_task(&mut tx, &task)?;
        tx.commit().map_err(mysql_err)?;
        Ok(updated)
    }

    fn get_task_attempt_started_at(
        &self,
        job_id: &str,
        run_id: u64,
        task_id: &str,
    ) -> FsResult<Option<i64>> {
        self.conn()?
            .exec_first::<i64, _, _>(
                "select attempt_started_at from transfer_tasks where job_id = :job_id and run_id = :run_id and task_id = :task_id",
                params! {
                    "job_id" => job_id,
                    "run_id" => run_id,
                    "task_id" => task_id,
                },
            )
            .map_err(mysql_err)
    }

    fn update_task_report(&self, report: TransferTaskReport) -> FsResult<bool> {
        let mut conn = self.conn()?;
        update_task_report_in(&mut conn, &report)
    }

    /// One transaction for the whole drained report batch: production measures
    /// ~15 ms for a single-row report write against ~7 ms for a plain read, so
    /// the per-report commit — not the row update — dominates the cost.
    fn update_task_reports(&self, reports: Vec<TransferTaskReport>) -> FsResult<Vec<bool>> {
        if reports.len() <= 1 {
            return reports
                .into_iter()
                .map(|report| self.update_task_report(report))
                .collect();
        }
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        let mut accepted = Vec::with_capacity(reports.len());
        for report in &reports {
            accepted.push(update_task_report_in(&mut tx, report)?);
        }
        tx.commit().map_err(mysql_err)?;
        Ok(accepted)
    }

    fn update_job_progress_summary(
        &self,
        job_id: &str,
        run_id: u64,
        summary: TransferProgress,
        now_ms: i64,
    ) -> FsResult<()> {
        let summary_json = serde_json::to_string(&summary).map_err(json_err)?;
        self.conn()?
            .exec_drop(
                "update transfer_jobs
             set record_json = json_set(record_json, '$.summary', cast(:summary_json as json),
                                        '$.updated_at', :updated_at), updated_at = :updated_at
             where job_id = :job_id and run_id = :run_id",
                params! {
                    "summary_json" => summary_json,
                    "updated_at" => now_ms,
                    "job_id" => job_id,
                    "run_id" => run_id,
                },
            )
            .map_err(mysql_err)?;
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
        let summary_json = serde_json::to_string(&summary).map_err(json_err)?;
        let counts_json = serde_json::to_string(&counts).map_err(json_err)?;
        self.conn()?
            .exec_drop(
                "update transfer_jobs
             set record_json = json_set(record_json, '$.summary', cast(:summary_json as json),
                                        '$.terminal_summary', cast(:summary_json as json),
                                        '$.terminal_counts', cast(:counts_json as json),
                                        '$.updated_at', :updated_at), updated_at = :updated_at
             where job_id = :job_id and run_id = :run_id
               and coalesce(json_type(json_extract(record_json, '$.terminal_summary')), 'NULL') != 'OBJECT'",
                params! {
                    "summary_json" => summary_json,
                    "counts_json" => counts_json,
                    "updated_at" => now_ms,
                    "job_id" => job_id,
                    "run_id" => run_id,
                },
            )
            .map_err(mysql_err)?;
        Ok(())
    }
}

/// Row update shared by the single and batched report paths.
fn update_task_report_in(conn: &mut impl Queryable, report: &TransferTaskReport) -> FsResult<bool> {
    let (sql, loaded) = match report.state {
            TransferTaskState::Running => (
                "update transfer_tasks
             set state = :state, progress_loaded_size = greatest(progress_loaded_size, :progress_loaded_size),
                 progress_total_size = :progress_total_size,
                 progress_updated_at = :progress_updated_at,
                 progress_message = :progress_message, last_report_at = :last_report_at,
                 stale_deadline_at = :stale_deadline_at,
                 record_json = json_set(record_json,
                     '$.state', case :state
                         when 1 then 'Pending'
                         when 2 then 'Running'
                         when 3 then 'Completed'
                         when 4 then 'Failed'
                         when 5 then 'Canceled'
                         when 6 then 'Stale'
                     end,
                     '$.progress.loaded_size', greatest(coalesce(cast(json_unquote(json_extract(record_json, '$.progress.loaded_size')) as signed), 0), :progress_loaded_size),
                     '$.progress.total_size', :progress_total_size,
                     '$.progress.update_time', :progress_updated_at,
                     '$.progress.message', :progress_message,
                     '$.last_report_at', :last_report_at,
                     '$.stale_deadline_at', :stale_deadline_at,
                     '$.updated_at', :updated_at,
                     '$.accepted_at_ms', :accepted_at_ms,
                     '$.first_byte_at_ms', :first_byte_at_ms,
                     '$.block_locations', cast(:block_locations as json)),
                 updated_at = :updated_at
             where job_id = :job_id and run_id = :run_id and task_id = :task_id
               and attempt_id = :attempt_id and worker_id = :worker_id
               and worker_session_id = :worker_session_id and state in (1, 2)",
                report.progress.loaded_size.max(0),
            ),
            _ => (
                "update transfer_tasks
             set state = :state, progress_loaded_size = :progress_loaded_size,
                 progress_total_size = :progress_total_size,
                 progress_updated_at = :progress_updated_at,
                 progress_message = :progress_message, last_report_at = :last_report_at,
                 stale_deadline_at = :stale_deadline_at,
                 record_json = json_set(record_json,
                     '$.state', case :state
                         when 1 then 'Pending'
                         when 2 then 'Running'
                         when 3 then 'Completed'
                         when 4 then 'Failed'
                         when 5 then 'Canceled'
                         when 6 then 'Stale'
                     end,
                     '$.progress.loaded_size', :progress_loaded_size,
                     '$.progress.total_size', :progress_total_size,
                     '$.progress.update_time', :progress_updated_at,
                     '$.progress.message', :progress_message,
                     '$.last_report_at', :last_report_at,
                     '$.stale_deadline_at', :stale_deadline_at,
                     '$.updated_at', :updated_at,
                     '$.accepted_at_ms', :accepted_at_ms,
                     '$.first_byte_at_ms', :first_byte_at_ms,
                     '$.block_locations', cast(:block_locations as json)),
                 updated_at = :updated_at
             where job_id = :job_id and run_id = :run_id and task_id = :task_id
               and attempt_id = :attempt_id and worker_id = :worker_id
               and worker_session_id = :worker_session_id and state in (1, 2)",
                report.progress.loaded_size,
            ),
        };
    let updated = exec_affected(
        conn,
        sql,
        params! {
            "state" => report.state as i32,
            "progress_loaded_size" => loaded,
            "progress_total_size" => report.progress.total_size,
            "progress_updated_at" => report.progress.update_time,
            "progress_message" => &report.progress.message,
            "last_report_at" => report.now_ms,
            "stale_deadline_at" => report.stale_deadline_at,
            "updated_at" => report.now_ms,
            "accepted_at_ms" => report.accepted_at_ms,
            "first_byte_at_ms" => report.first_byte_at_ms,
            "block_locations" => json(&report.block_locations)?,
            "job_id" => &report.job_id,
            "run_id" => report.run_id,
            "task_id" => &report.task_id,
            "attempt_id" => report.attempt_id,
            "worker_id" => report.worker_id,
            "worker_session_id" => &report.worker_session_id,
        },
    )? > 0;
    Ok(updated)
}

fn init_schema(conn: &mut PooledConn) -> FsResult<()> {
    conn.query_drop(
        "create table if not exists transfer_schema_version (
            id tinyint unsigned primary key,
            version bigint unsigned not null,
            updated_at bigint not null
        )",
    )
    .map_err(mysql_err)?;
    let now_ms = curvine_runtime::common::LocalTime::mills();
    conn.exec_drop(
        "insert ignore into transfer_schema_version(id, version, updated_at)
         values (1, :version, :updated_at)",
        params! {
            "version" => TRANSFER_SCHEMA_VERSION,
            "updated_at" => now_ms,
        },
    )
    .map_err(mysql_err)?;
    acquire_mysql_schema_lock(conn)?;
    let result = init_schema_locked(conn, now_ms);
    let release_result = release_mysql_schema_lock(conn);
    match (result, release_result) {
        (Err(err), _) => Err(err),
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(err)) => Err(err),
    }
}

fn init_schema_locked(conn: &mut PooledConn, now_ms: u64) -> FsResult<()> {
    let version = conn
        .exec_first::<u64, _, _>(
            "select version from transfer_schema_version where id = 1",
            Params::Empty,
        )
        .map_err(mysql_err)?
        .ok_or_else(|| FsError::common("Missing mysql transfer schema version"))?;
    conn.query_drop(
        "create table if not exists transfer_jobs (
            job_id varchar(128) primary key,
            submitter varchar(255) not null,
            tenant varchar(255) not null,
            client_request_id varchar(255) not null,
            job_key varchar(1024) not null,
            target_path varchar(2048) not null,
            run_id bigint unsigned not null,
            kind int not null,
            state int not null,
            owner varchar(255) not null,
            lease_epoch bigint unsigned not null,
            lease_expire_at bigint not null,
            cancel_requested tinyint not null,
            record_json longtext not null,
            created_at bigint not null,
            updated_at bigint not null,
            unique key transfer_jobs_request_idx(submitter, client_request_id),
            key transfer_jobs_job_key_state_idx(job_key(255), state),
            key transfer_jobs_target_state_idx(target_path(255), state),
            key transfer_jobs_state_lease_idx(state, lease_expire_at),
            key transfer_jobs_state_updated_idx(state, updated_at),
            key transfer_jobs_owner_state_updated_idx(owner, state, updated_at),
            key transfer_jobs_tenant_state_updated_idx(tenant, state, updated_at),
            key transfer_jobs_submitter_state_updated_idx(submitter, state, updated_at)
        )",
    )
    .map_err(mysql_err)?;
    conn.query_drop(
        "create table if not exists transfer_cleanup_lease (
            id tinyint primary key,
            owner varchar(255) not null,
            lease_expire_at bigint not null
        )",
    )
    .map_err(mysql_err)?;
    conn.query_drop(
        "create table if not exists transfer_tasks (
            job_id varchar(128) not null,
            run_id bigint unsigned not null,
            task_id varchar(255) not null,
            state int not null,
            attempt_id bigint unsigned not null,
            worker_id bigint unsigned not null,
            worker_session_id varchar(255) not null,
            stale_deadline_at bigint not null,
            source_path varchar(4096) not null default '',
            target_path varchar(4096) not null default '',
            report_target_json longtext null,
            retry_count int unsigned not null default 0,
            attempt_started_at bigint not null default 0,
            last_report_at bigint not null default 0,
            progress_loaded_size bigint not null default 0,
            progress_total_size bigint not null default 0,
            progress_updated_at bigint not null default 0,
            progress_message text null,
            record_json longtext not null,
            updated_at bigint not null,
            primary key(job_id, run_id, task_id),
            key transfer_tasks_job_state_idx(job_id, run_id, state),
            key transfer_tasks_job_updated_idx(job_id, run_id, updated_at),
            key transfer_tasks_worker_idx(worker_id, worker_session_id, state),
            key transfer_tasks_stale_idx(state, stale_deadline_at)
        )",
    )
    .map_err(mysql_err)?;
    migrate_mysql_schema(conn, version, now_ms)
}

fn migrate_mysql_schema(conn: &mut PooledConn, mut version: u64, now_ms: u64) -> FsResult<()> {
    if version > TRANSFER_SCHEMA_VERSION {
        return Err(FsError::common(format!(
            "Unsupported mysql transfer schema version {}, expected {}",
            version, TRANSFER_SCHEMA_VERSION
        )));
    }
    if version == 2 {
        migrate_mysql_schema_v2_to_v3(conn)?;
        update_mysql_schema_version(conn, TRANSFER_SCHEMA_V3, now_ms)?;
        version = TRANSFER_SCHEMA_V3;
    }
    if version == TRANSFER_SCHEMA_V3 {
        migrate_mysql_schema_v3_to_v4(conn)?;
        update_mysql_schema_version(conn, TRANSFER_SCHEMA_V4, now_ms)?;
        version = TRANSFER_SCHEMA_V4;
    }
    if version == TRANSFER_SCHEMA_V4 {
        migrate_mysql_schema_v4_to_v5(conn)?;
        update_mysql_schema_version(conn, TRANSFER_SCHEMA_VERSION, now_ms)?;
        version = TRANSFER_SCHEMA_VERSION;
    }
    if version != TRANSFER_SCHEMA_VERSION {
        return Err(FsError::common(format!(
            "Unsupported mysql transfer schema version {}, expected {}",
            version, TRANSFER_SCHEMA_VERSION
        )));
    }
    migrate_mysql_task_runtime_columns(conn)?;
    ensure_mysql_ttl_purge_index(conn)
}

/// v5 adds the discovery tables (instance registry + topology version).
/// Table creation is idempotent, so the migration only fills the version gap.
fn migrate_mysql_schema_v4_to_v5(conn: &mut PooledConn) -> FsResult<()> {
    conn.query_drop(
        "create table if not exists transfer_instances (
            kind varchar(32) not null,
            instance_id varchar(128) not null,
            host varchar(255) not null,
            rpc_port int unsigned not null,
            web_port int unsigned null,
            start_epoch bigint not null,
            heartbeat_at bigint not null,
            lease_expire_at bigint not null,
            primary key (kind, instance_id)
        )",
    )
    .map_err(mysql_err)?;
    conn.query_drop(
        "create table if not exists transfer_topology (
            id tinyint primary key,
            topology_version bigint not null,
            updated_at bigint not null
        )",
    )
    .map_err(mysql_err)?;
    conn.exec_drop(
        "insert ignore into transfer_topology (id, topology_version, updated_at)
         values (1, 1, :now_ms)",
        params! { "now_ms" => curvine_runtime::common::LocalTime::mills() as i64 },
    )
    .map_err(mysql_err)?;
    Ok(())
}

fn migrate_mysql_schema_v2_to_v3(conn: &mut PooledConn) -> FsResult<()> {
    if !mysql_column_exists(conn, "transfer_jobs", "target_path")? {
        conn.query_drop(
            "alter table transfer_jobs
             add column target_path varchar(2048) not null default '' after job_key",
        )
        .map_err(mysql_err)?;
    }
    // Historical rows stay JSON-authoritative. Runtime columns are populated
    // only by future writes, so migration never rewrites existing jobs.
    Ok(())
}

fn migrate_mysql_schema_v3_to_v4(conn: &mut PooledConn) -> FsResult<()> {
    if !mysql_column_exists(conn, "transfer_jobs", "tenant")? {
        conn.query_drop(
            "alter table transfer_jobs
             add column tenant varchar(255) not null default '' after submitter",
        )
        .map_err(mysql_err)?;
    }
    // Historical rows stay JSON-authoritative. Runtime columns are populated
    // only by future writes, so migration never rewrites existing jobs.
    Ok(())
}

// Additive schema only: `record_json` remains authoritative; never rewrite history
// merely to populate runtime projection columns.
fn migrate_mysql_task_runtime_columns(conn: &mut PooledConn) -> FsResult<()> {
    for &(column, definition) in MYSQL_TASK_RUNTIME_COLUMNS {
        if !mysql_column_exists(conn, "transfer_tasks", column)? {
            conn.query_drop(format!(
                "alter table transfer_tasks add column {column} {definition}"
            ))
            .map_err(mysql_err)?;
        }
    }
    ensure_mysql_legacy_task_json_sync_trigger(conn)
}

fn ensure_mysql_legacy_task_json_sync_trigger(conn: &mut PooledConn) -> FsResult<()> {
    if !mysql_trigger_exists(conn, "transfer_tasks_record_json_insert_sync")? {
        conn.query_drop(
            "create trigger transfer_tasks_record_json_insert_sync
             before insert on transfer_tasks for each row
             begin
                 set new.state = coalesce(case json_unquote(json_extract(new.record_json, '$.state'))
                         when 'Pending' then 1 when '1' then 1
                         when 'Running' then 2 when '2' then 2
                         when 'Completed' then 3 when '3' then 3
                         when 'Failed' then 4 when '4' then 4
                         when 'Canceled' then 5 when '5' then 5
                         when 'Stale' then 6 when '6' then 6
                     end, new.state),
                     new.attempt_id = coalesce(cast(json_unquote(json_extract(new.record_json, '$.attempt_id')) as unsigned), new.attempt_id),
                     new.worker_id = coalesce(cast(json_unquote(json_extract(new.record_json, '$.worker_id')) as unsigned), new.worker_id),
                     new.worker_session_id = coalesce(json_unquote(json_extract(new.record_json, '$.worker_session_id')), new.worker_session_id),
                     new.stale_deadline_at = coalesce(cast(json_unquote(json_extract(new.record_json, '$.stale_deadline_at')) as signed), new.stale_deadline_at),
                     new.source_path = coalesce(json_unquote(json_extract(new.record_json, '$.source_path')), new.source_path),
                     new.target_path = coalesce(json_unquote(json_extract(new.record_json, '$.target_path')), new.target_path),
                     new.report_target_json = coalesce(json_unquote(json_extract(new.record_json, '$.report_target_json')), new.report_target_json),
                     new.retry_count = coalesce(cast(json_unquote(json_extract(new.record_json, '$.retry_count')) as unsigned), new.retry_count),
                     new.attempt_started_at = coalesce(cast(json_unquote(json_extract(new.record_json, '$.attempt_started_at')) as signed), new.attempt_started_at),
                     new.last_report_at = coalesce(cast(json_unquote(json_extract(new.record_json, '$.last_report_at')) as signed), new.last_report_at),
                     new.progress_loaded_size = coalesce(cast(json_unquote(json_extract(new.record_json, '$.progress.loaded_size')) as signed), new.progress_loaded_size),
                     new.progress_total_size = coalesce(cast(json_unquote(json_extract(new.record_json, '$.progress.total_size')) as signed), new.progress_total_size),
                     new.progress_updated_at = coalesce(cast(json_unquote(json_extract(new.record_json, '$.progress.update_time')) as signed), new.progress_updated_at),
                     new.progress_message = coalesce(json_unquote(json_extract(new.record_json, '$.progress.message')), new.progress_message),
                     new.updated_at = coalesce(cast(json_unquote(json_extract(new.record_json, '$.updated_at')) as signed), new.updated_at);
             end",
        )
        .map_err(mysql_err)?;
    }
    if mysql_trigger_exists(conn, "transfer_tasks_record_json_update_sync")? {
        return Ok(());
    }
    conn.query_drop(
        "create trigger transfer_tasks_record_json_update_sync
         before update on transfer_tasks for each row
         begin
             set new.state = coalesce(case json_unquote(json_extract(new.record_json, '$.state'))
                     when 'Pending' then 1 when '1' then 1
                     when 'Running' then 2 when '2' then 2
                     when 'Completed' then 3 when '3' then 3
                     when 'Failed' then 4 when '4' then 4
                     when 'Canceled' then 5 when '5' then 5
                     when 'Stale' then 6 when '6' then 6
                 end, new.state),
                 new.attempt_id = coalesce(cast(json_unquote(json_extract(new.record_json, '$.attempt_id')) as unsigned), new.attempt_id),
                 new.worker_id = coalesce(cast(json_unquote(json_extract(new.record_json, '$.worker_id')) as unsigned), new.worker_id),
                 new.worker_session_id = coalesce(json_unquote(json_extract(new.record_json, '$.worker_session_id')), new.worker_session_id),
                 new.stale_deadline_at = coalesce(cast(json_unquote(json_extract(new.record_json, '$.stale_deadline_at')) as signed), new.stale_deadline_at),
                 new.source_path = coalesce(json_unquote(json_extract(new.record_json, '$.source_path')), new.source_path),
                 new.target_path = coalesce(json_unquote(json_extract(new.record_json, '$.target_path')), new.target_path),
                 new.report_target_json = coalesce(json_unquote(json_extract(new.record_json, '$.report_target_json')), new.report_target_json),
                 new.retry_count = coalesce(cast(json_unquote(json_extract(new.record_json, '$.retry_count')) as unsigned), new.retry_count),
                 new.attempt_started_at = coalesce(cast(json_unquote(json_extract(new.record_json, '$.attempt_started_at')) as signed), new.attempt_started_at),
                 new.last_report_at = coalesce(cast(json_unquote(json_extract(new.record_json, '$.last_report_at')) as signed), new.last_report_at),
                 new.progress_loaded_size = coalesce(cast(json_unquote(json_extract(new.record_json, '$.progress.loaded_size')) as signed), new.progress_loaded_size),
                 new.progress_total_size = coalesce(cast(json_unquote(json_extract(new.record_json, '$.progress.total_size')) as signed), new.progress_total_size),
                 new.progress_updated_at = coalesce(cast(json_unquote(json_extract(new.record_json, '$.progress.update_time')) as signed), new.progress_updated_at),
                 new.progress_message = coalesce(json_unquote(json_extract(new.record_json, '$.progress.message')), new.progress_message),
                 new.updated_at = coalesce(cast(json_unquote(json_extract(new.record_json, '$.updated_at')) as signed), new.updated_at);
         end",
    )
    .map_err(mysql_err)
}

fn mysql_trigger_exists(conn: &mut PooledConn, name: &str) -> FsResult<bool> {
    conn.exec_first::<u8, _, _>(
        "select 1 from information_schema.triggers
         where trigger_schema = database() and trigger_name = :name",
        params! { "name" => name },
    )
    .map(|trigger| trigger.is_some())
    .map_err(mysql_err)
}

fn acquire_mysql_schema_lock(conn: &mut PooledConn) -> FsResult<()> {
    let acquired = conn
        .exec_first::<u8, _, _>(
            "select get_lock(:name, :timeout)",
            params! {
                "name" => MYSQL_SCHEMA_LOCK_NAME,
                "timeout" => MYSQL_SCHEMA_LOCK_TIMEOUT_SECS,
            },
        )
        .map_err(mysql_err)?;
    if acquired == Some(1) {
        Ok(())
    } else {
        Err(FsError::transfer_store_unavailable(
            "Timed out acquiring the MySQL transfer schema migration lock",
        ))
    }
}

fn configure_mysql_migration_connection(conn: &mut PooledConn) -> FsResult<()> {
    conn.query_drop(format!(
        "set session lock_wait_timeout = {MYSQL_MIGRATION_DDL_LOCK_TIMEOUT_SECS}"
    ))
    .map_err(mysql_err)?;
    conn.query_drop(format!(
        "set session innodb_lock_wait_timeout = {MYSQL_MIGRATION_ROW_LOCK_TIMEOUT_SECS}"
    ))
    .map_err(mysql_err)
}

fn release_mysql_schema_lock(conn: &mut PooledConn) -> FsResult<()> {
    conn.exec_drop(
        "select release_lock(:name)",
        params! { "name" => MYSQL_SCHEMA_LOCK_NAME },
    )
    .map_err(mysql_err)
}

fn update_mysql_schema_version(conn: &mut PooledConn, version: u64, now_ms: u64) -> FsResult<()> {
    conn.exec_drop(
        "update transfer_schema_version
         set version = :version, updated_at = :updated_at
         where id = 1",
        params! {
            "version" => version,
            "updated_at" => now_ms,
        },
    )
    .map_err(mysql_err)?;
    Ok(())
}

fn mysql_column_exists(conn: &mut PooledConn, table: &str, column: &str) -> FsResult<bool> {
    conn.exec_first::<String, _, _>(
        "select column_name from information_schema.columns
         where table_schema = database()
           and table_name = :table
           and column_name = :column",
        params! {
            "table" => table,
            "column" => column,
        },
    )
    .map(|value| value.is_some())
    .map_err(mysql_err)
}

fn mysql_index_exists(conn: &mut PooledConn, table: &str, index: &str) -> FsResult<bool> {
    conn.exec_first::<String, _, _>(
        "select index_name from information_schema.statistics
         where table_schema = database()
           and table_name = :table
           and index_name = :index",
        params! {
            "table" => table,
            "index" => index,
        },
    )
    .map(|value| value.is_some())
    .map_err(mysql_err)
}

fn ensure_mysql_ttl_purge_index(conn: &mut PooledConn) -> FsResult<()> {
    if !mysql_index_exists(conn, "transfer_jobs", "transfer_jobs_state_updated_idx")? {
        conn.query_drop(
            "create index transfer_jobs_state_updated_idx on transfer_jobs(state, updated_at)",
        )
        .map_err(mysql_err)?;
    }
    Ok(())
}

fn mysql_schema_ready(conn: &mut PooledConn) -> FsResult<bool> {
    let version = match conn.exec_first::<u64, _, _>(
        "select version from transfer_schema_version where id = 1",
        Params::Empty,
    ) {
        Ok(version) => version,
        Err(err) if mysql_error_code(&err) == Some(1146) => return Ok(false),
        Err(err) => return Err(mysql_err(err)),
    };
    if version != Some(TRANSFER_SCHEMA_VERSION)
        || !mysql_column_exists(conn, "transfer_jobs", "tenant")?
        || !mysql_column_exists(conn, "transfer_jobs", "target_path")?
    {
        return Ok(false);
    }
    for &(column, _) in MYSQL_TASK_RUNTIME_COLUMNS {
        if !mysql_column_exists(conn, "transfer_tasks", column)? {
            return Ok(false);
        }
    }
    // Projection triggers only maintain an optional cache. JSON-authoritative
    // readers must not start DDL merely because a legacy trigger is absent.
    Ok(true)
}

fn insert_job_raw(conn: &mut impl Queryable, params: mysql::Params) -> Result<(), mysql::Error> {
    conn.exec_drop(
        "insert into transfer_jobs (
            job_id, submitter, tenant, client_request_id, job_key, target_path, run_id, kind, state, owner, lease_epoch,
            lease_expire_at, cancel_requested, record_json, created_at, updated_at
        ) values (
            :job_id, :submitter, :tenant, :client_request_id, :job_key, :target_path, :run_id, :kind, :state, :owner,
            :lease_epoch, :lease_expire_at, :cancel_requested, :record_json, :created_at, :updated_at
        )",
        params,
    )
}

fn insert_task(conn: &mut impl Queryable, task: &TransferTaskRecord) -> FsResult<()> {
    let _ = exec_affected(
        conn,
        "insert ignore into transfer_tasks (
            job_id, run_id, task_id, state, attempt_id, worker_id, worker_session_id,
            stale_deadline_at, source_path, target_path, report_target_json, retry_count,
            attempt_started_at, last_report_at, progress_loaded_size, progress_total_size,
            progress_updated_at, progress_message, record_json, updated_at
        ) values (
            :job_id, :run_id, :task_id, :state, :attempt_id, :worker_id, :worker_session_id,
            :stale_deadline_at, :source_path, :target_path, :report_target_json, :retry_count,
            :attempt_started_at, :last_report_at, :progress_loaded_size, :progress_total_size,
            :progress_updated_at, :progress_message, :record_json, :updated_at
        )",
        task_params(task)?,
    )?;
    Ok(())
}

fn select_job_by_id(
    conn: &mut impl Queryable,
    job_id: &str,
) -> FsResult<Option<TransferJobRecord>> {
    select_job(
        conn,
        "select record_json from transfer_jobs where job_id = :job_id",
        params! { "job_id" => job_id },
    )
}

fn select_job_by_id_for_update(
    conn: &mut impl Queryable,
    job_id: &str,
) -> FsResult<Option<TransferJobRecord>> {
    select_job(
        conn,
        "select record_json from transfer_jobs where job_id = :job_id for update",
        params! { "job_id" => job_id },
    )
}

fn select_job_by_request(
    conn: &mut impl Queryable,
    submitter: &str,
    client_request_id: &str,
) -> FsResult<Option<TransferJobRecord>> {
    select_job(
        conn,
        "select record_json from transfer_jobs
         where submitter = :submitter and client_request_id = :client_request_id",
        params! { "submitter" => submitter, "client_request_id" => client_request_id },
    )
}

fn select_non_terminal_job_by_key(
    conn: &mut impl Queryable,
    job_key: &str,
) -> FsResult<Option<TransferJobRecord>> {
    select_job(
        conn,
        "select record_json from transfer_jobs
         where job_key = :job_key and state not in (6, 7, 8, 9)
         limit 1",
        params! { "job_key" => job_key },
    )
}

fn select_conflicting_active_transfer(
    conn: &mut impl Queryable,
    target_path: &str,
    submitter: &str,
    client_request_id: &str,
) -> FsResult<Option<TransferJobRecord>> {
    // fs_mode jobs neither conflict nor are conflicted with (see
    // `TransferCommand::is_fs_mode_submitter`).
    if TransferCommand::is_fs_mode_submitter(submitter) {
        return Ok(None);
    }
    // `record_json` remains the source of truth for historical jobs. Do not
    // backfill target_path merely to make this lookup indexable.
    let conflict = select_jobs(
        conn,
        "select record_json from transfer_jobs
         where state in (2, 3, 4, 5) and submitter not like 'fs-mode:%'",
        Params::Empty,
    )?
    .into_iter()
    .find(|job| {
        !(job.submitter == submitter && job.client_request_id == client_request_id)
            && target_paths_conflict(&job.target_path, target_path)
    });
    Ok(conflict)
}

fn select_executing_targets(conn: &mut impl Queryable) -> FsResult<Vec<(String, String)>> {
    Ok(select_jobs(
        conn,
        "select record_json from transfer_jobs
         where state in (2, 3, 4, 5) and submitter not like 'fs-mode:%'",
        Params::Empty,
    )?
    .into_iter()
    .map(|job| (job.target_path, job.tenant))
    .collect())
}

fn has_pending_transfer(conn: &mut impl Queryable, now_ms: i64) -> FsResult<bool> {
    conn.exec_first::<u8, _, _>(
        "select 1 from transfer_jobs where state = 1 and lease_expire_at <= :now_ms limit 1",
        params! { "now_ms" => now_ms },
    )
    .map(|value| value.is_some())
    .map_err(mysql_err)
}

fn select_pending_transfer(
    conn: &mut impl Queryable,
    now_ms: i64,
    active_targets: &[(String, String)],
) -> FsResult<Option<(String, u64, u64, i32)>> {
    let active_tenants = active_targets
        .iter()
        .map(|(_, tenant)| tenant.as_str())
        .collect::<BTreeSet<_>>();
    let mut preferred = None;
    let mut fallback = None;
    for job in select_jobs(
        conn,
        "select record_json from transfer_jobs
         where state = 1 and lease_expire_at <= :now_ms",
        params! { "now_ms" => now_ms },
    )? {
        if !TransferCommand::is_fs_mode_submitter(&job.submitter)
            && active_targets
                .iter()
                .any(|(target_path, _)| target_paths_conflict(&job.target_path, target_path))
        {
            continue;
        }
        let slot = if active_tenants.contains(job.tenant.as_str()) {
            &mut fallback
        } else {
            &mut preferred
        };
        if slot.as_ref().is_none_or(|current: &TransferJobRecord| {
            (job.updated_at, &job.job_id) < (current.updated_at, &current.job_id)
        }) {
            *slot = Some(job);
        }
    }
    Ok(preferred
        .or(fallback)
        .map(|job| (job.job_id, job.run_id, job.lease_epoch, job.state as i32)))
}

fn select_job(
    conn: &mut impl Queryable,
    sql: &str,
    params: mysql::Params,
) -> FsResult<Option<TransferJobRecord>> {
    let value: Option<String> = conn.exec_first(sql, params).map_err(mysql_err)?;
    value
        .map(|json| serde_json::from_str(&json).map_err(json_err))
        .transpose()
}

fn target_paths_conflict(left: &str, right: &str) -> bool {
    let left = left.trim_end_matches('/');
    let right = right.trim_end_matches('/');
    left.is_empty()
        || right.is_empty()
        || left == right
        || left
            .strip_prefix(right)
            .is_some_and(|suffix| suffix.starts_with('/'))
        || right
            .strip_prefix(left)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

/// Shared WHERE builder for the job list and the state-grouped count. Returns the
/// clause text and the positional values the caller must bind in order.
pub(super) fn job_where_mysql(
    kind: Option<TransferKind>,
    states: &[TransferState],
    submitter: Option<&str>,
    tenant: Option<&str>,
) -> (String, Vec<mysql::Value>) {
    let mut clauses = Vec::new();
    let mut values: Vec<mysql::Value> = Vec::new();
    if let Some(kind) = kind {
        clauses.push("kind = ?".to_string());
        values.push(mysql::Value::from(kind as i32));
    }
    if !states.is_empty() {
        let placeholders = vec!["?"; states.len()].join(", ");
        clauses.push(format!("state in ({placeholders})"));
        values.extend(states.iter().map(|state| mysql::Value::from(*state as i32)));
    }
    if let Some(submitter) = submitter {
        clauses.push("submitter = ?".to_string());
        values.push(mysql::Value::from(submitter.to_string()));
    }
    if let Some(tenant) = tenant {
        clauses.push("tenant = ?".to_string());
        values.push(mysql::Value::from(tenant.to_string()));
    }
    if clauses.is_empty() {
        (String::new(), values)
    } else {
        (format!("where {}", clauses.join(" and ")), values)
    }
}

fn select_jobs(
    conn: &mut impl Queryable,
    sql: &str,
    params: mysql::Params,
) -> FsResult<Vec<TransferJobRecord>> {
    let values: Vec<String> = conn.exec(sql, params).map_err(mysql_err)?;
    values
        .into_iter()
        .map(|json| serde_json::from_str(&json).map_err(json_err))
        .collect()
}

fn select_task_for_update(
    conn: &mut impl Queryable,
    job_id: &str,
    run_id: u64,
    task_id: &str,
    runtime_projection_ready: bool,
) -> FsResult<Option<TransferTaskRecord>> {
    if !runtime_projection_ready {
        return conn
            .exec_first(
                "select record_json from transfer_tasks
                 where job_id = :job_id and run_id = :run_id and task_id = :task_id
                 for update",
                params! { "job_id" => job_id, "run_id" => run_id, "task_id" => task_id },
            )
            .map_err(mysql_err)?
            .map(|record_json: String| serde_json::from_str(&record_json).map_err(json_err))
            .transpose();
    }
    let value: Option<mysql::Row> = conn
        .exec_first(
            "select record_json, state, attempt_id, worker_id, worker_session_id,
                    stale_deadline_at, report_target_json, retry_count, attempt_started_at,
                    last_report_at, progress_loaded_size, progress_total_size,
                    progress_updated_at, progress_message, updated_at
             from transfer_tasks
             where job_id = :job_id and run_id = :run_id and task_id = :task_id
             for update",
            params! { "job_id" => job_id, "run_id" => run_id, "task_id" => task_id },
        )
        .map_err(mysql_err)?;
    value.map(|row| task_from_row(&row)).transpose()
}

fn select_tasks(
    conn: &mut impl Queryable,
    job_id: &str,
    run_id: u64,
    state: Option<TransferTaskState>,
    limit: Option<usize>,
    runtime_projection_ready: bool,
) -> FsResult<Vec<TransferTaskRecord>> {
    if !runtime_projection_ready {
        return select_legacy_tasks(conn, job_id, run_id, state, limit);
    }
    let mut sql = "select record_json, state, attempt_id, worker_id, worker_session_id,
                stale_deadline_at, report_target_json, retry_count, attempt_started_at,
                last_report_at, progress_loaded_size, progress_total_size,
                progress_updated_at, progress_message, updated_at
         from transfer_tasks where job_id = :job_id and run_id = :run_id"
        .to_string();
    if state.is_some() {
        sql.push_str(" and state = :state");
    }
    // task_id order rides the PK / (job_id, run_id, state) index tail: zero filesort.
    sql.push_str(" order by task_id asc");
    if limit.is_some() {
        sql.push_str(" limit :limit");
    }
    let rows: Vec<mysql::Row> = conn
        .exec(
            sql,
            params! {
                "job_id" => job_id,
                "run_id" => run_id,
                "state" => state.map(|s| s as i32),
                "limit" => limit.map(|v| v as u64),
            },
        )
        .map_err(mysql_err)?;
    rows.into_iter().map(|row| task_from_row(&row)).collect()
}

fn select_legacy_tasks(
    conn: &mut impl Queryable,
    job_id: &str,
    run_id: u64,
    state: Option<TransferTaskState>,
    limit: Option<usize>,
) -> FsResult<Vec<TransferTaskRecord>> {
    let mut sql = "select record_json from transfer_tasks
                   where job_id = :job_id and run_id = :run_id"
        .to_string();
    if state.is_some() {
        sql.push_str(" and state = :state");
    }
    // task_id order rides the PK / (job_id, run_id, state) index tail: zero filesort.
    sql.push_str(" order by task_id asc");
    if limit.is_some() {
        sql.push_str(" limit :limit");
    }
    let rows: Vec<String> = conn
        .exec(
            sql,
            params! {
                "job_id" => job_id,
                "run_id" => run_id,
                "state" => state.map(|value| value as i32),
                "limit" => limit.map(|value| value as u64),
            },
        )
        .map_err(mysql_err)?;
    rows.into_iter()
        .map(|record_json| serde_json::from_str(&record_json).map_err(json_err))
        .collect()
}

fn task_from_row(row: &mysql::Row) -> FsResult<TransferTaskRecord> {
    let record_json: String = mysql_column(row, 0)?;
    let state: i32 = mysql_column(row, 1)?;
    let attempt_id: u64 = mysql_column(row, 2)?;
    let worker_id: u64 = mysql_column(row, 3)?;
    let worker_session_id: String = mysql_column(row, 4)?;
    let stale_deadline_at: i64 = mysql_column(row, 5)?;
    let report_target_json = mysql_column::<Option<String>>(row, 6)?.unwrap_or_default();
    let retry_count: u32 = mysql_column(row, 7)?;
    let attempt_started_at: i64 = mysql_column(row, 8)?;
    let last_report_at: i64 = mysql_column(row, 9)?;
    let progress_loaded_size: i64 = mysql_column(row, 10)?;
    let progress_total_size: i64 = mysql_column(row, 11)?;
    let progress_updated_at: i64 = mysql_column(row, 12)?;
    let progress_message = mysql_column::<Option<String>>(row, 13)?.unwrap_or_default();
    let updated_at: i64 = mysql_column(row, 14)?;
    let mut task: TransferTaskRecord = serde_json::from_str(&record_json).map_err(json_err)?;
    task.state = TransferTaskState::from(state);
    task.attempt_id = attempt_id;
    task.worker_id = u32::try_from(worker_id)
        .map_err(|_| FsError::common("Transfer task worker ID exceeds u32"))?;
    task.worker_session_id = worker_session_id;
    task.stale_deadline_at = stale_deadline_at;
    task.report_target_json = report_target_json;
    task.retry_count = retry_count;
    task.attempt_started_at = attempt_started_at;
    task.last_report_at = last_report_at;
    task.progress = TransferProgress {
        loaded_size: progress_loaded_size,
        total_size: progress_total_size,
        update_time: progress_updated_at,
        message: progress_message,
    };
    task.updated_at = updated_at;
    Ok(task)
}

fn select_task_status_page(
    conn: &mut impl Queryable,
    job_id: &str,
    run_id: u64,
    offset: usize,
    limit: usize,
) -> FsResult<(Vec<TransferTaskStatus>, bool)> {
    let rows: Vec<mysql::Row> = conn
        .exec(
            "select task_id, attempt_id, source_path, target_path, worker_id, worker_session_id,
                    state, progress_loaded_size, progress_total_size, progress_updated_at,
                    progress_message, retry_count, updated_at
             from transfer_tasks where job_id = :job_id and run_id = :run_id
             order by task_id asc limit :limit offset :offset",
            params! {
                "job_id" => job_id,
                "run_id" => run_id,
                "limit" => limit.saturating_add(1) as u64,
                "offset" => offset as u64,
            },
        )
        .map_err(mysql_err)?;
    let mut page = rows
        .into_iter()
        .map(|row| {
            let task_id: String = mysql_column(&row, 0)?;
            let attempt_id: u64 = mysql_column(&row, 1)?;
            let source_path: String = mysql_column(&row, 2)?;
            let target_path: String = mysql_column(&row, 3)?;
            let worker_id: u64 = mysql_column(&row, 4)?;
            let worker_session_id: String = mysql_column(&row, 5)?;
            let state: i32 = mysql_column(&row, 6)?;
            let loaded_size: i64 = mysql_column(&row, 7)?;
            let total_size: i64 = mysql_column(&row, 8)?;
            let update_time: i64 = mysql_column(&row, 9)?;
            let message = mysql_column::<Option<String>>(&row, 10)?.unwrap_or_default();
            let retry_count: u32 = mysql_column(&row, 11)?;
            let updated_at: i64 = mysql_column(&row, 12)?;
            Ok(TransferTaskStatus {
                task_id,
                attempt_id,
                source_path,
                target_path,
                worker_id: u32::try_from(worker_id)
                    .map_err(|_| FsError::common("Transfer task worker ID exceeds u32"))?,
                worker_session_id,
                state: TransferTaskState::from(state),
                progress: TransferProgress {
                    loaded_size,
                    total_size,
                    update_time,
                    message,
                },
                retry_count,
                updated_at,
            })
        })
        .collect::<FsResult<Vec<_>>>()?;
    let has_next = page.len() > limit;
    page.truncate(limit);
    Ok((page, has_next))
}

fn select_task_summary(
    conn: &mut impl Queryable,
    job_id: &str,
    run_id: u64,
    now_ms: i64,
) -> FsResult<TransferTaskSummary> {
    let row: TaskSummaryRow = conn
        .exec_first(
            "select
                sum(case when state = 1 then 1 else 0 end),
                sum(case when state = 2 then 1 else 0 end),
                sum(case when state = 3 then 1 else 0 end),
                sum(case when state = 4 then 1 else 0 end),
                sum(case when state = 5 then 1 else 0 end),
                sum(case when state = 6 then 1 else 0 end),
                sum(progress_loaded_size), sum(progress_total_size),
                sum(case when state = 3 and progress_loaded_size > 0 then progress_loaded_size else 0 end)
             from transfer_tasks where job_id = :job_id and run_id = :run_id",
            params! { "job_id" => job_id, "run_id" => run_id },
        )
        .map_err(mysql_err)?
        .unwrap_or_default();
    let first_error: Option<String> = conn
        .exec_first(
            "select progress_message from transfer_tasks
             where job_id = :job_id and run_id = :run_id and state = 4 and progress_message <> ''
             order by updated_at asc, task_id asc limit 1",
            params! { "job_id" => job_id, "run_id" => run_id },
        )
        .map_err(mysql_err)?;
    let last_message: Option<String> = conn
        .exec_first(
            "select progress_message from transfer_tasks
             where job_id = :job_id and run_id = :run_id and progress_message <> ''
             order by updated_at desc, task_id desc limit 1",
            params! { "job_id" => job_id, "run_id" => run_id },
        )
        .map_err(mysql_err)?;
    Ok(transfer_task_summary_from_aggregate(
        TransferTaskCounts {
            pending: row.0.unwrap_or_default(),
            running: row.1.unwrap_or_default(),
            completed: row.2.unwrap_or_default(),
            failed: row.3.unwrap_or_default(),
            canceled: row.4.unwrap_or_default(),
            stale: row.5.unwrap_or_default(),
            completed_size: row.8.unwrap_or_default(),
        },
        row.6.unwrap_or_default(),
        row.7.unwrap_or_default(),
        first_error.as_deref(),
        last_message.as_deref(),
        now_ms,
    ))
}

fn mysql_column<T: mysql::prelude::FromValue>(row: &mysql::Row, index: usize) -> FsResult<T> {
    row.get(index)
        .ok_or_else(|| FsError::common("Transfer metadata row contains an invalid task column"))
}

fn exec_update_job(conn: &mut impl Queryable, job: &TransferJobRecord) -> FsResult<bool> {
    let affected = exec_affected(
        conn,
        "update transfer_jobs
         set tenant = :tenant, target_path = :target_path, kind = :kind, state = :state, owner = :owner, lease_epoch = :lease_epoch,
             lease_expire_at = :lease_expire_at, cancel_requested = :cancel_requested,
             record_json = :record_json, created_at = :created_at, updated_at = :updated_at
         where job_id = :job_id and run_id = :run_id",
        job_params(job)?,
    )?;
    Ok(affected > 0)
}

fn exec_update_task(conn: &mut impl Queryable, task: &TransferTaskRecord) -> FsResult<bool> {
    let affected = exec_affected(
        conn,
        "update transfer_tasks
         set state = :state, attempt_id = :attempt_id, worker_id = :worker_id,
             worker_session_id = :worker_session_id, stale_deadline_at = :stale_deadline_at,
             report_target_json = :report_target_json, retry_count = :retry_count,
             attempt_started_at = :attempt_started_at, last_report_at = :last_report_at,
             progress_loaded_size = :progress_loaded_size,
             progress_total_size = :progress_total_size,
             progress_updated_at = :progress_updated_at,
             progress_message = :progress_message, record_json = :record_json,
             updated_at = :updated_at
         where job_id = :job_id and run_id = :run_id and task_id = :task_id",
        task_params(task)?,
    )?;
    Ok(affected > 0)
}

fn exec_affected(conn: &mut impl Queryable, sql: &str, params: mysql::Params) -> FsResult<u64> {
    let result = conn.exec_iter(sql, params).map_err(mysql_err)?;
    Ok(result.affected_rows())
}

fn job_params(job: &TransferJobRecord) -> FsResult<mysql::Params> {
    Ok(params! {
        "job_id" => &job.job_id,
        "submitter" => &job.submitter,
        "tenant" => &job.tenant,
        "client_request_id" => &job.client_request_id,
        "job_key" => &job.job_key,
        "target_path" => &job.target_path,
        "run_id" => job.run_id,
        "kind" => job.kind as i32,
        "state" => job.state as i32,
        "owner" => &job.owner,
        "lease_epoch" => job.lease_epoch,
        "lease_expire_at" => job.lease_expire_at,
        "cancel_requested" => job.cancel_requested,
        "record_json" => json(job)?,
        "created_at" => job.created_at,
        "updated_at" => job.updated_at,
    })
}

fn task_params(task: &TransferTaskRecord) -> FsResult<mysql::Params> {
    Ok(params! {
        "job_id" => &task.job_id,
        "run_id" => task.run_id,
        "task_id" => &task.task_id,
        "state" => task.state as i32,
        "attempt_id" => task.attempt_id,
        "worker_id" => task.worker_id,
        "worker_session_id" => &task.worker_session_id,
        "stale_deadline_at" => task.stale_deadline_at,
        "source_path" => &task.source_path,
        "target_path" => &task.target_path,
        "report_target_json" => &task.report_target_json,
        "retry_count" => task.retry_count,
        "attempt_started_at" => task.attempt_started_at,
        "last_report_at" => task.last_report_at,
        "progress_loaded_size" => task.progress.loaded_size,
        "progress_total_size" => task.progress.total_size,
        "progress_updated_at" => task.progress.update_time,
        "progress_message" => &task.progress.message,
        "record_json" => json(task)?,
        "updated_at" => task.updated_at,
    })
}

fn json<T: serde::Serialize>(value: &T) -> FsResult<String> {
    serde_json::to_string(value).map_err(json_err)
}

fn json_err(_: serde_json::Error) -> FsError {
    FsError::common("Transfer metadata store contains invalid data")
}

fn mysql_err(err: mysql::Error) -> FsError {
    log::warn!("transfer MySQL store operation failed: {}", err);
    FsError::transfer_store_unavailable(format!(
        "Transfer metadata store is unavailable; verify transfer.store_url and database connectivity: {}",
        store_error_detail(&err)
    ))
}

fn mysql_error_code(err: &mysql::Error) -> Option<u16> {
    match err {
        mysql::Error::MySqlError(err) => Some(err.code),
        _ => None,
    }
}
