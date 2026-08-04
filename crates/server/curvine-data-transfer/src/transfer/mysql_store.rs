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
    StaleTaskAttempt, TaskAttemptStart, TransferJobRecord, TransferLease, TransferListFilter,
    TransferState, TransferStateUpdate, TransferTaskRecord, TransferTaskReport, TransferTaskState,
    TransferTenantSummary,
};
use mysql::prelude::*;
use mysql::{params, Params, Pool, PooledConn, TxOpts, Value as MysqlValue};

use crate::transfer::{
    apply_task_report_progress, TransferPlannedTasks, TransferRequeueUpdate, TransferStore,
    TransferTaskStateUpdate,
};

const TRANSFER_SCHEMA_VERSION: u64 = 4;
const TRANSFER_SCHEMA_V3: u64 = 3;

type TenantSummaryRow = (
    String,
    Option<u64>,
    Option<u64>,
    Option<u64>,
    Option<u64>,
    Option<u64>,
    Option<u64>,
    u64,
);

pub struct MysqlTransferStore {
    pool: Pool,
}

impl MysqlTransferStore {
    pub fn open(url: &str) -> FsResult<Self> {
        let pool = Pool::new(url).map_err(mysql_err)?;
        let mut conn = pool.get_conn().map_err(mysql_err)?;
        init_schema(&mut conn)?;
        Ok(Self { pool })
    }

    fn conn(&self) -> FsResult<PooledConn> {
        self.pool.get_conn().map_err(mysql_err)
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
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        if let Some(existing) =
            select_job_by_request(&mut tx, &job.submitter, &job.client_request_id)?
        {
            if existing.command_json != job.command_json {
                return Err(FsError::common(format!(
                    "Transfer request ID {} submitted by {} is already bound to job {} with a different command",
                    job.client_request_id, job.submitter, existing.job_id
                )));
            }
            tx.commit().map_err(mysql_err)?;
            return Ok(existing);
        }
        if let Some(existing) = select_non_terminal_job_by_key(&mut tx, &job.job_key)? {
            if existing.command_json == job.command_json {
                tx.commit().map_err(mysql_err)?;
                return Ok(existing);
            }
            return Err(FsError::transfer_already_running(format!(
                "job_key {} has running job {} with different command",
                existing.job_key, existing.job_id
            )));
        }
        if let Some(existing) = select_conflicting_active_transfer(
            &mut tx,
            &job.target_path,
            &job.submitter,
            &job.client_request_id,
        )? {
            return Err(FsError::transfer_target_conflict(format!(
                "target {} conflicts with active transfer {} target {}",
                job.target_path, existing.job_id, existing.target_path
            )));
        }
        insert_job(&mut tx, &job)?;
        tx.commit().map_err(mysql_err)?;
        Ok(job)
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
                "select count(*) from transfer_jobs where state not in (6, 7, 8, 9)",
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
        let mut conn = self.conn()?;
        let (where_sql, mut values) = list_filter_mysql_params(&filter);
        let sql = format!(
            "select record_json from transfer_jobs
             {where_sql}
             order by updated_at desc, created_at desc, job_id desc
             limit ? offset ?"
        );
        values.push(MysqlValue::from(filter.limit as u64));
        values.push(MysqlValue::from(filter.offset as u64));
        select_jobs(&mut conn, &sql, Params::Positional(values))
    }

    fn list_tenant_summaries(
        &self,
        limit: usize,
        offset: usize,
    ) -> FsResult<Vec<TransferTenantSummary>> {
        self.conn()?
            .exec_map(
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
                 limit :limit offset :offset",
                params! {
                    "limit" => limit as u64,
                    "offset" => offset as u64,
                },
                |(
                    tenant,
                    pending,
                    executing,
                    completed,
                    failed,
                    canceled,
                    partial_success,
                    total,
                ): TenantSummaryRow| TransferTenantSummary {
                    tenant,
                    pending: pending.unwrap_or_default(),
                    executing: executing.unwrap_or_default(),
                    completed: completed.unwrap_or_default(),
                    failed: failed.unwrap_or_default(),
                    canceled: canceled.unwrap_or_default(),
                    partial_success: partial_success.unwrap_or_default(),
                    total,
                },
            )
            .map_err(mysql_err)
    }

    fn purge_terminal_transfers(&self, older_than_ms: i64, limit: usize) -> FsResult<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        let job_ids: Vec<String> = tx
            .exec(
                "select job_id from transfer_jobs
                 where state in (6, 7, 8, 9) and updated_at < :older_than_ms
                 order by updated_at asc
                 limit :limit",
                params! {
                    "older_than_ms" => older_than_ms,
                    "limit" => limit as u64,
                },
            )
            .map_err(mysql_err)?;
        for job_id in &job_ids {
            tx.exec_drop(
                "delete from transfer_tasks where job_id = :job_id",
                params! { "job_id" => job_id },
            )
            .map_err(mysql_err)?;
            tx.exec_drop(
                "delete from transfer_jobs where job_id = :job_id and state in (6, 7, 8, 9)",
                params! { "job_id" => job_id },
            )
            .map_err(mysql_err)?;
        }
        tx.commit().map_err(mysql_err)?;
        Ok(job_ids.len())
    }

    fn list_transfer_tasks(&self, job_id: &str, run_id: u64) -> FsResult<Vec<TransferTaskRecord>> {
        select_tasks(&mut self.conn()?, job_id, run_id, None, None)
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
    ) -> FsResult<Option<TransferLease>> {
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;

        tx.exec_first::<u64, _, _>(
            "select version from transfer_schema_version where id = 1 for update",
            Params::Empty,
        )
        .map_err(mysql_err)?
        .ok_or_else(|| FsError::common("Missing mysql transfer schema version"))?;
        let executing = tx
            .exec_first::<u64, _, _>(
                "select count(*) from transfer_jobs where state in (2, 3, 4, 5)",
                Params::Empty,
            )
            .map_err(mysql_err)?
            .ok_or_else(|| FsError::common("Failed to count executing transfer jobs"))?;
        let mut job: Option<(String, u64, u64, i32)> = tx
            .exec_first(
                "select job_id, run_id, lease_epoch, state
                 from transfer_jobs
                 where owner = :owner and state in (4, 5) and lease_expire_at > :now_ms
                   and updated_at < :now_ms
                 order by updated_at asc
                 limit 1",
                params! { "owner" => owner, "now_ms" => now_ms },
            )
            .map_err(mysql_err)?;
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
        if job.is_none() && executing < max_executing_transfers {
            job = tx
                .exec_first(
                    "select pending.job_id, pending.run_id, pending.lease_epoch, pending.state
                     from transfer_jobs pending
                     where pending.state = 1 and pending.lease_expire_at <= :now_ms
                     order by case when exists (
                         select 1 from transfer_jobs executing
                         where executing.tenant = pending.tenant and executing.state in (2, 3, 4, 5)
                         limit 1
                     ) then 1 else 0 end asc,
                     pending.updated_at asc, pending.job_id asc
                     limit 1",
                    params! { "now_ms" => now_ms },
                )
                .map_err(mysql_err)?;
        }
        let Some((job_id, run_id, lease_epoch, state)) = job else {
            tx.commit().map_err(mysql_err)?;
            return Ok(None);
        };
        let mut record =
            select_job_by_id(&mut tx, &job_id)?.ok_or_else(|| FsError::job_not_found(&job_id))?;
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

    fn insert_tasks(&self, tasks: Vec<TransferTaskRecord>) -> FsResult<()> {
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

    fn persist_planned_tasks(&self, update: TransferPlannedTasks) -> FsResult<bool> {
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
        )
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
        let rows: Vec<String> = tx
            .exec(
                "select record_json from transfer_tasks
                 where job_id = :job_id and run_id = :run_id and state = 2
                   and stale_deadline_at < :now_ms
                 order by stale_deadline_at asc
                 limit :limit",
                params! {
                    "job_id" => job_id,
                    "run_id" => run_id,
                    "now_ms" => now_ms,
                    "limit" => limit as u64,
                },
            )
            .map_err(mysql_err)?;
        let mut stale = Vec::with_capacity(rows.len());
        for row in rows {
            let mut task: TransferTaskRecord = serde_json::from_str(&row).map_err(json_err)?;
            task.state = TransferTaskState::Stale;
            task.retry_count = task.retry_count.saturating_add(1);
            task.progress.message = "task stale timeout".to_string();
            task.progress.update_time = now_ms;
            task.updated_at = now_ms;
            exec_update_task(&mut tx, &task)?;
            stale.push(StaleTaskAttempt { task });
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
        rows.into_iter()
            .map(|row| serde_json::from_str(&row).map_err(json_err))
            .collect()
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
        let mut task =
            match select_task_for_update(&mut tx, &start.job_id, start.run_id, &start.task_id)? {
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
        let updated = exec_update_task(&mut tx, &task)?;
        tx.commit().map_err(mysql_err)?;
        Ok(updated)
    }

    fn update_task_report(&self, report: TransferTaskReport) -> FsResult<bool> {
        let mut conn = self.conn()?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .map_err(mysql_err)?;
        let mut job = match select_job_by_id_for_update(&mut tx, &report.job_id)? {
            Some(job) if job.run_id == report.run_id => job,
            _ => {
                tx.commit().map_err(mysql_err)?;
                return Ok(false);
            }
        };
        let mut task = match select_task_for_update(
            &mut tx,
            &report.job_id,
            report.run_id,
            &report.task_id,
        )? {
            Some(task)
                if task.attempt_id == report.attempt_id
                    && task.worker_id == report.worker_id
                    && task.worker_session_id == report.worker_session_id
                    && task.state.is_running() =>
            {
                task
            }
            _ => {
                tx.commit().map_err(mysql_err)?;
                return Ok(false);
            }
        };
        let previous_progress = task.progress.clone();
        task.state = report.state;
        task.progress = report.progress.clone();
        task.last_report_at = report.now_ms;
        task.stale_deadline_at = report.stale_deadline_at;
        task.updated_at = report.now_ms;
        exec_update_task(&mut tx, &task)?;
        apply_task_report_progress(
            &mut job.summary,
            &previous_progress,
            &report.progress,
            report.now_ms,
        );
        job.updated_at = report.now_ms;
        exec_update_job(&mut tx, &job)?;
        tx.commit().map_err(mysql_err)?;
        Ok(true)
    }
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
    let now_ms = orpc::common::LocalTime::mills();
    conn.exec_drop(
        "insert ignore into transfer_schema_version(id, version, updated_at)
         values (1, :version, :updated_at)",
        params! {
            "version" => TRANSFER_SCHEMA_VERSION,
            "updated_at" => now_ms,
        },
    )
    .map_err(mysql_err)?;
    let version = conn
        .exec_first::<u64, _, _>(
            "select version from transfer_schema_version where id = 1",
            Params::Empty,
        )
        .map_err(mysql_err)?
        .ok_or_else(|| FsError::common("Missing mysql transfer schema version"))?;
    migrate_mysql_schema(conn, version, now_ms)?;

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
            key transfer_jobs_owner_state_updated_idx(owner, state, updated_at),
            key transfer_jobs_tenant_state_updated_idx(tenant, state, updated_at),
            key transfer_jobs_submitter_state_updated_idx(submitter, state, updated_at)
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
            record_json longtext not null,
            updated_at bigint not null,
            primary key(job_id, run_id, task_id),
            key transfer_tasks_job_state_idx(job_id, run_id, state),
            key transfer_tasks_worker_idx(worker_id, worker_session_id, state),
            key transfer_tasks_stale_idx(state, stale_deadline_at)
        )",
    )
    .map_err(mysql_err)?;
    Ok(())
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
        update_mysql_schema_version(conn, TRANSFER_SCHEMA_VERSION, now_ms)?;
    } else if version != TRANSFER_SCHEMA_VERSION {
        return Err(FsError::common(format!(
            "Unsupported mysql transfer schema version {}, expected {}",
            version, TRANSFER_SCHEMA_VERSION
        )));
    }
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
    conn.query_drop(
        "update transfer_jobs
         set target_path = json_unquote(json_extract(record_json, '$.target_path'))
         where target_path = ''",
    )
    .map_err(mysql_err)?;
    create_mysql_target_index(conn)?;
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
    conn.query_drop(
        "update transfer_jobs
         set tenant = coalesce(json_unquote(json_extract(record_json, '$.tenant')), '')
         where tenant = ''",
    )
    .map_err(mysql_err)?;
    create_mysql_list_indexes(conn)?;
    Ok(())
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

fn create_mysql_target_index(conn: &mut PooledConn) -> FsResult<()> {
    conn.query_drop(
        "create index transfer_jobs_target_state_idx
         on transfer_jobs(target_path(255), state)",
    )
    .or_else(|err| {
        if mysql_error_code(&err) == Some(1061) {
            Ok(())
        } else {
            Err(err)
        }
    })
    .map_err(mysql_err)?;
    Ok(())
}

fn create_mysql_list_indexes(conn: &mut PooledConn) -> FsResult<()> {
    conn.query_drop(
        "create index transfer_jobs_tenant_state_updated_idx
         on transfer_jobs(tenant, state, updated_at)",
    )
    .or_else(|err| {
        if mysql_error_code(&err) == Some(1061) {
            Ok(())
        } else {
            Err(err)
        }
    })
    .map_err(mysql_err)?;
    conn.query_drop(
        "create index transfer_jobs_submitter_state_updated_idx
         on transfer_jobs(submitter, state, updated_at)",
    )
    .or_else(|err| {
        if mysql_error_code(&err) == Some(1061) {
            Ok(())
        } else {
            Err(err)
        }
    })
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

fn insert_job(conn: &mut impl Queryable, job: &TransferJobRecord) -> FsResult<()> {
    let _ = exec_affected(
        conn,
        "insert into transfer_jobs (
            job_id, submitter, tenant, client_request_id, job_key, target_path, run_id, kind, state, owner, lease_epoch,
            lease_expire_at, cancel_requested, record_json, created_at, updated_at
        ) values (
            :job_id, :submitter, :tenant, :client_request_id, :job_key, :target_path, :run_id, :kind, :state, :owner,
            :lease_epoch, :lease_expire_at, :cancel_requested, :record_json, :created_at, :updated_at
        )",
        job_params(job)?,
    )?;
    Ok(())
}

fn insert_task(conn: &mut impl Queryable, task: &TransferTaskRecord) -> FsResult<()> {
    let _ = exec_affected(
        conn,
        "insert ignore into transfer_tasks (
            job_id, run_id, task_id, state, attempt_id, worker_id, worker_session_id,
            stale_deadline_at, record_json, updated_at
        ) values (
            :job_id, :run_id, :task_id, :state, :attempt_id, :worker_id, :worker_session_id,
            :stale_deadline_at, :record_json, :updated_at
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
    let (child_lower, child_upper) = child_path_bounds(target_path);
    let exact_or_child = select_job(
        conn,
        "select record_json from transfer_jobs
         where state not in (6, 7, 8, 9)
           and not (submitter = :submitter and client_request_id = :client_request_id)
           and (
               target_path = :target_path
               or (target_path >= :child_lower and target_path < :child_upper)
           )
         limit 1
         for update",
        params! {
            "target_path" => target_path,
            "submitter" => submitter,
            "client_request_id" => client_request_id,
            "child_lower" => child_lower,
            "child_upper" => child_upper,
        },
    )?;
    if exact_or_child.is_some() {
        return Ok(exact_or_child);
    }

    for ancestor in ancestor_paths(target_path) {
        let ancestor_conflict = select_job(
            conn,
            "select record_json from transfer_jobs
             where state not in (6, 7, 8, 9)
               and not (submitter = :submitter and client_request_id = :client_request_id)
               and target_path = :target_path
             limit 1
             for update",
            params! {
                "target_path" => ancestor,
                "submitter" => submitter,
                "client_request_id" => client_request_id,
            },
        )?;
        if ancestor_conflict.is_some() {
            return Ok(ancestor_conflict);
        }
    }
    Ok(None)
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

fn child_path_bounds(target_path: &str) -> (String, String) {
    let lower = if target_path == "/" {
        "/".to_string()
    } else {
        format!("{}/", target_path.trim_end_matches('/'))
    };
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

fn list_filter_mysql_params(filter: &TransferListFilter) -> (String, Vec<MysqlValue>) {
    let mut clauses = Vec::new();
    let mut values = Vec::new();
    if let Some(kind) = filter.kind {
        clauses.push("kind = ?");
        values.push(MysqlValue::from(kind as i32));
    }
    if let Some(state) = filter.state {
        clauses.push("state = ?");
        values.push(MysqlValue::from(state as i32));
    }
    if let Some(submitter) = &filter.submitter {
        clauses.push("submitter = ?");
        values.push(MysqlValue::from(submitter.as_str()));
    }
    if let Some(tenant) = &filter.tenant {
        clauses.push("tenant = ?");
        values.push(MysqlValue::from(tenant.as_str()));
    }
    if clauses.is_empty() {
        (String::new(), values)
    } else {
        (format!("where {}", clauses.join(" and ")), values)
    }
}

fn select_task_for_update(
    conn: &mut impl Queryable,
    job_id: &str,
    run_id: u64,
    task_id: &str,
) -> FsResult<Option<TransferTaskRecord>> {
    let value: Option<String> = conn
        .exec_first(
            "select record_json from transfer_tasks
             where job_id = :job_id and run_id = :run_id and task_id = :task_id
             for update",
            params! { "job_id" => job_id, "run_id" => run_id, "task_id" => task_id },
        )
        .map_err(mysql_err)?;
    value
        .map(|json| serde_json::from_str(&json).map_err(json_err))
        .transpose()
}

fn select_tasks(
    conn: &mut impl Queryable,
    job_id: &str,
    run_id: u64,
    state: Option<TransferTaskState>,
    limit: Option<usize>,
) -> FsResult<Vec<TransferTaskRecord>> {
    let mut sql =
        "select record_json from transfer_tasks where job_id = :job_id and run_id = :run_id"
            .to_string();
    if state.is_some() {
        sql.push_str(" and state = :state");
    }
    sql.push_str(" order by updated_at asc");
    if limit.is_some() {
        sql.push_str(" limit :limit");
    }
    let rows: Vec<String> = conn
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
    rows.into_iter()
        .map(|json| serde_json::from_str(&json).map_err(json_err))
        .collect()
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
             record_json = :record_json, updated_at = :updated_at
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
    FsError::transfer_store_unavailable(
        "Transfer metadata store is unavailable; verify transfer.store_url and database connectivity",
    )
}

fn mysql_error_code(err: &mysql::Error) -> Option<u16> {
    match err {
        mysql::Error::MySqlError(err) => Some(err.code),
        _ => None,
    }
}
