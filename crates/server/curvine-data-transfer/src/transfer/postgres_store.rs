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

use curvine_error::{FsError, FsResult};
use curvine_model::{
    StaleTaskAttempt, TaskAttemptStart, TransferJobRecord, TransferLease, TransferListFilter,
    TransferState, TransferStateUpdate, TransferTaskRecord, TransferTaskReport, TransferTaskState,
    TransferTenantSummary,
};
use native_tls::{Certificate, TlsConnector};
use percent_encoding::percent_decode_str;
use postgres::{error::SqlState, GenericClient, Row, Transaction};
use postgres_native_tls::MakeTlsConnector;
use r2d2::{Pool, PooledConnection};
use r2d2_postgres::PostgresConnectionManager;
use std::error::Error as _;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use url::Url;

use crate::transfer::{
    apply_task_report_progress, TransferPlannedTasks, TransferRequeueUpdate, TransferStore,
    TransferTaskStateUpdate,
};

const TRANSFER_SCHEMA_VERSION: i64 = 1;
const TRANSFER_SCHEMA_LOCK: i64 = 8_195_260_721_118_841;

type PgManager = PostgresConnectionManager<MakeTlsConnector>;
type PgConnection = PooledConnection<PgManager>;

pub struct PostgresTransferStore {
    pool: Pool<PgManager>,
}

impl PostgresTransferStore {
    pub fn open(url: &str) -> FsResult<Self> {
        let (config, root_cert_path) = postgres_config(url)?;
        let tls = postgres_tls_connector(root_cert_path)?;
        let manager = PostgresConnectionManager::new(config, MakeTlsConnector::new(tls));
        let pool = Pool::builder().build(manager).map_err(postgres_pool_err)?;
        let mut conn = pool.get().map_err(postgres_pool_err)?;
        init_schema(&mut conn)?;
        Ok(Self { pool })
    }

    fn conn(&self) -> FsResult<PgConnection> {
        self.pool.get().map_err(postgres_pool_err)
    }

    fn transaction<T>(&self, f: impl FnOnce(&mut Transaction<'_>) -> FsResult<T>) -> FsResult<T> {
        let mut conn = self.conn()?;
        let mut tx = conn.transaction().map_err(postgres_err)?;
        let result = f(&mut tx)?;
        tx.commit().map_err(postgres_err)?;
        Ok(result)
    }
}

fn postgres_config(url: &str) -> FsResult<(postgres::Config, Option<PathBuf>)> {
    let parsed_url =
        Url::parse(url).map_err(|_| FsError::common("Invalid PostgreSQL transfer store URL"))?;
    if parsed_url.fragment().is_some() {
        return Err(FsError::common(
            "PostgreSQL transfer store URL must not contain a fragment",
        ));
    }
    let Some(query) = parsed_url.query() else {
        let config = postgres::Config::from_str(url)
            .map_err(|_| FsError::common("Invalid PostgreSQL transfer store URL"))?;
        return Ok((config, None));
    };
    let mut root_cert_path = None;
    let mut query_pairs = Vec::new();
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode_str(key)
            .decode_utf8()
            .map_err(|_| FsError::common("Invalid PostgreSQL transfer store URL"))?;
        if key == "sslrootcert" {
            let value = percent_decode_str(value)
                .decode_utf8()
                .map_err(|_| FsError::common("Invalid PostgreSQL sslrootcert path"))?;
            if root_cert_path.replace(value.into_owned()).is_some() {
                return Err(FsError::common(
                    "PostgreSQL transfer store URL contains multiple sslrootcert values",
                ));
            }
        } else {
            query_pairs.push(pair);
        }
    }
    let Some(root_cert_path) = root_cert_path else {
        let config = postgres::Config::from_str(url)
            .map_err(|_| FsError::common("Invalid PostgreSQL transfer store URL"))?;
        return Ok((config, None));
    };
    // tokio-postgres percent-decodes query values but treats `+` literally.
    // Preserve unrelated parameters instead of form-encoding them through Url.
    let (base_url, _) = url
        .split_once('?')
        .expect("a parsed PostgreSQL URL with a query contains '?'");
    let store_url = if query_pairs.is_empty() {
        base_url.to_string()
    } else {
        format!("{}?{}", base_url, query_pairs.join("&"))
    };
    let config = postgres::Config::from_str(&store_url)
        .map_err(|_| FsError::common("Invalid PostgreSQL transfer store URL"))?;
    if config.get_ssl_mode() == postgres::config::SslMode::Disable {
        // No TLS handshake occurs, so a root CA is irrelevant on this path.
        return Ok((config, None));
    }
    if root_cert_path.is_empty() {
        return Err(FsError::common(
            "PostgreSQL sslrootcert must name a PEM certificate file",
        ));
    }
    Ok((config, Some(PathBuf::from(root_cert_path))))
}

fn postgres_tls_connector(root_cert_path: Option<PathBuf>) -> FsResult<TlsConnector> {
    let mut builder = TlsConnector::builder();
    if let Some(path) = root_cert_path {
        let pem = fs::read(&path).map_err(|_| {
            FsError::common(format!(
                "Unable to read PostgreSQL root certificate at '{}'",
                path.display()
            ))
        })?;
        let certificate = Certificate::from_pem(&pem).map_err(|_| {
            FsError::common(format!(
                "PostgreSQL root certificate at '{}' is not valid PEM",
                path.display()
            ))
        })?;
        builder.add_root_certificate(certificate);
    }
    builder
        .build()
        .map_err(|_| FsError::common("Unable to initialize PostgreSQL TLS support"))
}

fn init_schema(conn: &mut PgConnection) -> FsResult<()> {
    let mut tx = conn.transaction().map_err(postgres_err)?;
    tx.query_one("select pg_advisory_xact_lock($1)", &[&TRANSFER_SCHEMA_LOCK])
        .map_err(postgres_err)?;
    tx.batch_execute(
        "create table if not exists transfer_schema_version (
            id smallint primary key,
            version bigint not null,
            updated_at bigint not null
        );
        create table if not exists transfer_jobs (
            job_id text primary key,
            submitter text not null,
            tenant text not null,
            client_request_id text not null,
            job_key text not null,
            target_path text collate \"C\" not null,
            run_id bigint not null,
            kind integer not null,
            state integer not null,
            owner text not null,
            lease_epoch bigint not null,
            lease_expire_at bigint not null,
            cancel_requested boolean not null,
            record_json text not null,
            created_at bigint not null,
            updated_at bigint not null,
            unique(submitter, client_request_id)
        );
        create table if not exists transfer_tasks (
            job_id text not null,
            run_id bigint not null,
            task_id text not null,
            state integer not null,
            attempt_id bigint not null,
            worker_id bigint not null,
            worker_session_id text not null,
            stale_deadline_at bigint not null,
            record_json text not null,
            updated_at bigint not null,
            primary key(job_id, run_id, task_id)
        );
        create index if not exists transfer_jobs_job_key_state_idx
            on transfer_jobs(left(job_key, 255), state);
        create index if not exists transfer_jobs_target_state_idx
            on transfer_jobs(target_path, state);
        create index if not exists transfer_jobs_state_lease_idx
            on transfer_jobs(state, lease_expire_at);
        create index if not exists transfer_jobs_owner_state_updated_idx
            on transfer_jobs(owner, state, updated_at);
        create index if not exists transfer_jobs_tenant_state_updated_idx
            on transfer_jobs(tenant, state, updated_at);
        create index if not exists transfer_jobs_submitter_state_updated_idx
            on transfer_jobs(submitter, state, updated_at);
        create index if not exists transfer_tasks_job_state_idx
            on transfer_tasks(job_id, run_id, state);
        create index if not exists transfer_tasks_worker_idx
            on transfer_tasks(worker_id, worker_session_id, state);
        create index if not exists transfer_tasks_stale_idx
            on transfer_tasks(state, stale_deadline_at);",
    )
    .map_err(postgres_err)?;
    let now_ms = db_u64(curvine_runtime::common::LocalTime::mills())?;
    tx.execute(
        "insert into transfer_schema_version(id, version, updated_at)
         values (1, $1, $2)
         on conflict (id) do nothing",
        &[&TRANSFER_SCHEMA_VERSION, &now_ms],
    )
    .map_err(postgres_err)?;
    let version: i64 = tx
        .query_opt(
            "select version from transfer_schema_version where id = 1",
            &[],
        )
        .map_err(postgres_err)?
        .map(|row| row.get(0))
        .ok_or_else(|| FsError::common("Missing PostgreSQL transfer schema version"))?;
    if version != TRANSFER_SCHEMA_VERSION {
        return Err(FsError::common(format!(
            "Unsupported PostgreSQL transfer schema version {}, expected {}",
            version, TRANSFER_SCHEMA_VERSION
        )));
    }
    tx.commit().map_err(postgres_err)?;
    Ok(())
}

fn lock_submission_gate(conn: &mut impl GenericClient) -> FsResult<()> {
    conn.query_opt(
        "select version from transfer_schema_version where id = 1 for update",
        &[],
    )
    .map_err(postgres_err)?
    .ok_or_else(|| FsError::common("Missing PostgreSQL transfer schema version"))?;
    Ok(())
}

fn select_runnable_owned(
    conn: &mut impl GenericClient,
    owner: &str,
    now_ms: i64,
) -> FsResult<Option<(String, u64, u64, i32)>> {
    select_lease_candidate(
        conn,
        "select job_id, run_id, lease_epoch, state
         from transfer_jobs
         where owner = $1 and state in (4, 5) and lease_expire_at > $2
           and updated_at < $2
         order by updated_at asc
         limit 1
         for update",
        &[&owner, &now_ms],
    )
}

fn select_expired_runnable(
    conn: &mut impl GenericClient,
    now_ms: i64,
) -> FsResult<Option<(String, u64, u64, i32)>> {
    select_lease_candidate(
        conn,
        "select job_id, run_id, lease_epoch, state
         from transfer_jobs
         where state in (2, 3, 4, 5) and lease_expire_at <= $1
         order by updated_at asc
         limit 1
         for update",
        &[&now_ms],
    )
}

fn select_pending_runnable(
    conn: &mut impl GenericClient,
    now_ms: i64,
) -> FsResult<Option<(String, u64, u64, i32)>> {
    select_lease_candidate(
        conn,
        "select pending.job_id, pending.run_id, pending.lease_epoch, pending.state
         from transfer_jobs pending
         where pending.state = 1 and pending.lease_expire_at <= $1
         order by case when exists (
             select 1 from transfer_jobs executing
             where executing.tenant = pending.tenant and executing.state in (2, 3, 4, 5)
             limit 1
         ) then 1 else 0 end asc,
         pending.updated_at asc, pending.job_id asc
         limit 1
         for update",
        &[&now_ms],
    )
}

fn select_lease_candidate(
    conn: &mut impl GenericClient,
    sql: &str,
    params: &[&(dyn postgres::types::ToSql + Sync)],
) -> FsResult<Option<(String, u64, u64, i32)>> {
    conn.query_opt(sql, params)
        .map_err(postgres_err)?
        .map(|row| {
            Ok((
                row.get(0),
                read_u64(&row, 1)?,
                read_u64(&row, 2)?,
                row.get(3),
            ))
        })
        .transpose()
}

fn insert_job(conn: &mut impl GenericClient, job: &TransferJobRecord) -> FsResult<()> {
    let run_id = db_u64(job.run_id)?;
    let lease_epoch = db_u64(job.lease_epoch)?;
    let record_json = json(job)?;
    conn.execute(
        "insert into transfer_jobs (
            job_id, submitter, tenant, client_request_id, job_key, target_path, run_id, kind, state, owner,
            lease_epoch, lease_expire_at, cancel_requested, record_json, created_at, updated_at
        ) values (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
        )",
        &[
            &job.job_id,
            &job.submitter,
            &job.tenant,
            &job.client_request_id,
            &job.job_key,
            &job.target_path,
            &run_id,
            &(job.kind as i32),
            &(job.state as i32),
            &job.owner,
            &lease_epoch,
            &job.lease_expire_at,
            &job.cancel_requested,
            &record_json,
            &job.created_at,
            &job.updated_at,
        ],
    )
    .map_err(postgres_err)?;
    Ok(())
}

fn insert_task(conn: &mut impl GenericClient, task: &TransferTaskRecord) -> FsResult<()> {
    let run_id = db_u64(task.run_id)?;
    let attempt_id = db_u64(task.attempt_id)?;
    let worker_id = i64::from(task.worker_id);
    let record_json = json(task)?;
    conn.execute(
        "insert into transfer_tasks (
            job_id, run_id, task_id, state, attempt_id, worker_id, worker_session_id,
            stale_deadline_at, record_json, updated_at
        ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        on conflict (job_id, run_id, task_id) do nothing",
        &[
            &task.job_id,
            &run_id,
            &task.task_id,
            &(task.state as i32),
            &attempt_id,
            &worker_id,
            &task.worker_session_id,
            &task.stale_deadline_at,
            &record_json,
            &task.updated_at,
        ],
    )
    .map_err(postgres_err)?;
    Ok(())
}

fn update_job(conn: &mut impl GenericClient, job: &TransferJobRecord) -> FsResult<bool> {
    let run_id = db_u64(job.run_id)?;
    let lease_epoch = db_u64(job.lease_epoch)?;
    let record_json = json(job)?;
    conn.execute(
        "update transfer_jobs
         set tenant = $1, target_path = $2, kind = $3, state = $4, owner = $5, lease_epoch = $6,
             lease_expire_at = $7, cancel_requested = $8, record_json = $9,
             created_at = $10, updated_at = $11
         where job_id = $12 and run_id = $13",
        &[
            &job.tenant,
            &job.target_path,
            &(job.kind as i32),
            &(job.state as i32),
            &job.owner,
            &lease_epoch,
            &job.lease_expire_at,
            &job.cancel_requested,
            &record_json,
            &job.created_at,
            &job.updated_at,
            &job.job_id,
            &run_id,
        ],
    )
    .map(|affected| affected > 0)
    .map_err(postgres_err)
}

fn update_task(conn: &mut impl GenericClient, task: &TransferTaskRecord) -> FsResult<bool> {
    let run_id = db_u64(task.run_id)?;
    let attempt_id = db_u64(task.attempt_id)?;
    let worker_id = i64::from(task.worker_id);
    let record_json = json(task)?;
    conn.execute(
        "update transfer_tasks
         set state = $1, attempt_id = $2, worker_id = $3, worker_session_id = $4,
             stale_deadline_at = $5, record_json = $6, updated_at = $7
         where job_id = $8 and run_id = $9 and task_id = $10",
        &[
            &(task.state as i32),
            &attempt_id,
            &worker_id,
            &task.worker_session_id,
            &task.stale_deadline_at,
            &record_json,
            &task.updated_at,
            &task.job_id,
            &run_id,
            &task.task_id,
        ],
    )
    .map(|affected| affected > 0)
    .map_err(postgres_err)
}

fn select_job_by_id(
    conn: &mut impl GenericClient,
    job_id: &str,
) -> FsResult<Option<TransferJobRecord>> {
    select_job(
        conn,
        "select record_json from transfer_jobs where job_id = $1",
        &[&job_id],
    )
}

fn select_job_by_id_for_update(
    conn: &mut impl GenericClient,
    job_id: &str,
) -> FsResult<Option<TransferJobRecord>> {
    select_job(
        conn,
        "select record_json from transfer_jobs where job_id = $1 for update",
        &[&job_id],
    )
}

fn select_job_by_request(
    conn: &mut impl GenericClient,
    submitter: &str,
    client_request_id: &str,
) -> FsResult<Option<TransferJobRecord>> {
    select_job(
        conn,
        "select record_json from transfer_jobs
         where submitter = $1 and client_request_id = $2",
        &[&submitter, &client_request_id],
    )
}

fn select_non_terminal_job_by_key(
    conn: &mut impl GenericClient,
    job_key: &str,
) -> FsResult<Option<TransferJobRecord>> {
    select_job(
        conn,
        "select record_json from transfer_jobs
         where left(job_key, 255) = left($1, 255)
           and job_key = $1 and state not in (6, 7, 8, 9)
         limit 1",
        &[&job_key],
    )
}

fn select_conflicting_active_transfer(
    conn: &mut impl GenericClient,
    target_path: &str,
    submitter: &str,
    client_request_id: &str,
) -> FsResult<Option<TransferJobRecord>> {
    let (child_lower, child_upper) = child_path_bounds(target_path);
    let exact_or_child = select_job(
        conn,
        "select record_json from transfer_jobs
         where state not in (6, 7, 8, 9)
           and not (submitter = $1 and client_request_id = $2)
           and (target_path = $3 or (target_path >= $4 and target_path < $5))
         limit 1
         for update",
        &[
            &submitter,
            &client_request_id,
            &target_path,
            &child_lower,
            &child_upper,
        ],
    )?;
    if exact_or_child.is_some() {
        return Ok(exact_or_child);
    }

    for ancestor in ancestor_paths(target_path) {
        let conflict = select_job(
            conn,
            "select record_json from transfer_jobs
             where state not in (6, 7, 8, 9)
               and not (submitter = $1 and client_request_id = $2)
               and target_path = $3
             limit 1
             for update",
            &[&submitter, &client_request_id, &ancestor],
        )?;
        if conflict.is_some() {
            return Ok(conflict);
        }
    }
    Ok(None)
}

fn select_job(
    conn: &mut impl GenericClient,
    sql: &str,
    params: &[&(dyn postgres::types::ToSql + Sync)],
) -> FsResult<Option<TransferJobRecord>> {
    conn.query_opt(sql, params)
        .map_err(postgres_err)?
        .map(|row| job_from_row(&row))
        .transpose()
}

fn select_jobs(
    conn: &mut impl GenericClient,
    sql: &str,
    params: &[&(dyn postgres::types::ToSql + Sync)],
) -> FsResult<Vec<TransferJobRecord>> {
    conn.query(sql, params)
        .map_err(postgres_err)?
        .iter()
        .map(job_from_row)
        .collect()
}

fn select_task_for_update(
    conn: &mut impl GenericClient,
    job_id: &str,
    run_id: u64,
    task_id: &str,
) -> FsResult<Option<TransferTaskRecord>> {
    let run_id = db_u64(run_id)?;
    conn.query_opt(
        "select record_json from transfer_tasks
         where job_id = $1 and run_id = $2 and task_id = $3
         for update",
        &[&job_id, &run_id, &task_id],
    )
    .map_err(postgres_err)?
    .map(|row| task_from_row(&row))
    .transpose()
}

fn select_tasks(
    conn: &mut impl GenericClient,
    job_id: &str,
    run_id: u64,
    state: Option<TransferTaskState>,
    limit: Option<usize>,
) -> FsResult<Vec<TransferTaskRecord>> {
    let run_id = db_u64(run_id)?;
    let state = state.map(|value| value as i32);
    let limit = limit.map(db_limit).transpose()?;
    let rows = match (state.as_ref(), limit.as_ref()) {
        (Some(state), Some(limit)) => conn.query(
            "select record_json from transfer_tasks
             where job_id = $1 and run_id = $2 and state = $3
             order by updated_at asc
             limit $4",
            &[&job_id, &run_id, state, limit],
        ),
        (Some(state), None) => conn.query(
            "select record_json from transfer_tasks
             where job_id = $1 and run_id = $2 and state = $3
             order by updated_at asc",
            &[&job_id, &run_id, state],
        ),
        (None, Some(limit)) => conn.query(
            "select record_json from transfer_tasks
             where job_id = $1 and run_id = $2
             order by updated_at asc
             limit $3",
            &[&job_id, &run_id, limit],
        ),
        (None, None) => conn.query(
            "select record_json from transfer_tasks
             where job_id = $1 and run_id = $2
             order by updated_at asc",
            &[&job_id, &run_id],
        ),
    }
    .map_err(postgres_err)?;
    rows.iter().map(task_from_row).collect()
}

fn count_rows(
    conn: &mut impl GenericClient,
    sql: &str,
    params: &[&(dyn postgres::types::ToSql + Sync)],
) -> FsResult<u64> {
    let count: i64 = conn.query_one(sql, params).map_err(postgres_err)?.get(0);
    u64::try_from(count)
        .map_err(|_| FsError::common("Transfer store returned a negative row count"))
}

fn job_from_row(row: &Row) -> FsResult<TransferJobRecord> {
    let value: String = row.get(0);
    serde_json::from_str(&value).map_err(json_err)
}

fn task_from_row(row: &Row) -> FsResult<TransferTaskRecord> {
    let value: String = row.get(0);
    serde_json::from_str(&value).map_err(json_err)
}

fn tenant_summary_from_row(row: &Row) -> FsResult<TransferTenantSummary> {
    Ok(TransferTenantSummary {
        tenant: row.get(0),
        pending: read_u64(row, 1)?,
        executing: read_u64(row, 2)?,
        completed: read_u64(row, 3)?,
        failed: read_u64(row, 4)?,
        canceled: read_u64(row, 5)?,
        partial_success: read_u64(row, 6)?,
        total: read_u64(row, 7)?,
    })
}

fn read_u64(row: &Row, index: usize) -> FsResult<u64> {
    let value: i64 = row.get(index);
    u64::try_from(value).map_err(|_| {
        FsError::common("PostgreSQL transfer store contains an invalid unsigned value")
    })
}

fn db_u64(value: u64) -> FsResult<i64> {
    i64::try_from(value)
        .map_err(|_| FsError::common("Transfer metadata value exceeds PostgreSQL BIGINT range"))
}

fn db_limit(value: usize) -> FsResult<i64> {
    i64::try_from(value)
        .map_err(|_| FsError::common("Transfer query limit exceeds PostgreSQL BIGINT range"))
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

fn json<T: serde::Serialize>(value: &T) -> FsResult<String> {
    serde_json::to_string(value).map_err(json_err)
}

fn json_err(_: serde_json::Error) -> FsError {
    FsError::common("Transfer metadata store contains invalid data")
}

fn postgres_err(err: postgres::Error) -> FsError {
    log::warn!("transfer PostgreSQL store operation failed: {}", err);
    if let Some(tls_error) = err
        .source()
        .and_then(|source| source.downcast_ref::<native_tls::Error>())
    {
        return FsError::transfer_store_unavailable(format!(
            "PostgreSQL TLS handshake failed: {tls_error}; verify the server certificate, hostname, and transfer.store_url sslrootcert"
        ));
    }
    if postgres_error_is_unavailable(&err) {
        FsError::transfer_store_unavailable(
            "Transfer metadata store is unavailable; verify transfer.store_url and PostgreSQL connectivity",
        )
    } else {
        let message = err.as_db_error().map_or_else(
            || err.to_string(),
            |db_error| db_error.message().to_string(),
        );
        FsError::common(format!(
            "PostgreSQL transfer store operation failed: {message}"
        ))
    }
}

fn postgres_error_is_unavailable(err: &postgres::Error) -> bool {
    err.is_closed()
        || err.code().is_some_and(is_postgres_connectivity_error)
        || err
            .source()
            .is_some_and(|source| source.is::<std::io::Error>())
}

fn is_postgres_connectivity_error(code: &SqlState) -> bool {
    code == &SqlState::CONNECTION_EXCEPTION
        || code == &SqlState::CONNECTION_DOES_NOT_EXIST
        || code == &SqlState::CONNECTION_FAILURE
        || code == &SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION
        || code == &SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION
        || code == &SqlState::TOO_MANY_CONNECTIONS
        || code == &SqlState::ADMIN_SHUTDOWN
        || code == &SqlState::CRASH_SHUTDOWN
        || code == &SqlState::CANNOT_CONNECT_NOW
}

fn postgres_pool_err(err: r2d2::Error) -> FsError {
    log::warn!("transfer PostgreSQL store connection pool failed: {}", err);
    FsError::transfer_store_unavailable(
        "Transfer metadata store is unavailable; verify transfer.store_url and PostgreSQL connectivity",
    )
}

impl TransferStore for PostgresTransferStore {
    fn check_available(&self) -> FsResult<()> {
        self.conn()?
            .query_one("select 1", &[])
            .map(|_| ())
            .map_err(postgres_err)
    }

    fn create_or_get_by_request_id(&self, job: TransferJobRecord) -> FsResult<TransferJobRecord> {
        self.create_or_get_by_request_id_checked(job)
    }

    fn create_or_get_by_request_id_checked(
        &self,
        job: TransferJobRecord,
    ) -> FsResult<TransferJobRecord> {
        self.transaction(|tx| {
            // PostgreSQL does not take MySQL-style next-key locks for an absent path range.
            // This stable row serializes only submissions, so overlapping targets cannot pass
            // their conflict checks concurrently.
            lock_submission_gate(tx)?;
            if let Some(existing) =
                select_job_by_request(tx, &job.submitter, &job.client_request_id)?
            {
                if existing.command_json != job.command_json {
                    return Err(FsError::common(format!(
                        "Transfer request ID {} submitted by {} is already bound to job {} with a different command",
                        job.client_request_id, job.submitter, existing.job_id
                    )));
                }
                return Ok(existing);
            }
            if let Some(existing) = select_non_terminal_job_by_key(tx, &job.job_key)? {
                if existing.command_json == job.command_json {
                    return Ok(existing);
                }
                return Err(FsError::transfer_already_running(format!(
                    "job_key {} has running job {} with different command",
                    existing.job_key, existing.job_id
                )));
            }
            if let Some(existing) = select_conflicting_active_transfer(
                tx,
                &job.target_path,
                &job.submitter,
                &job.client_request_id,
            )? {
                return Err(FsError::transfer_target_conflict(format!(
                    "target {} conflicts with active transfer {} target {}",
                    job.target_path, existing.job_id, existing.target_path
                )));
            }
            insert_job(tx, &job)?;
            Ok(job)
        })
    }

    fn get_transfer(&self, job_id: &str) -> FsResult<Option<TransferJobRecord>> {
        let mut conn = self.conn()?;
        select_job_by_id(&mut *conn, job_id)
    }

    fn get_transfer_by_request(
        &self,
        submitter: &str,
        client_request_id: &str,
    ) -> FsResult<Option<TransferJobRecord>> {
        let mut conn = self.conn()?;
        select_job_by_request(&mut *conn, submitter, client_request_id)
    }

    fn list_active_transfers(&self) -> FsResult<Vec<TransferJobRecord>> {
        let mut conn = self.conn()?;
        select_jobs(
            &mut *conn,
            "select record_json from transfer_jobs where state not in (6, 7, 8, 9)",
            &[],
        )
    }

    fn find_conflicting_active_transfer(
        &self,
        target_path: &str,
        submitter: &str,
        client_request_id: &str,
    ) -> FsResult<Option<TransferJobRecord>> {
        self.transaction(|tx| {
            select_conflicting_active_transfer(tx, target_path, submitter, client_request_id)
        })
    }

    fn count_active_transfers(&self) -> FsResult<u64> {
        let mut conn = self.conn()?;
        count_rows(
            &mut *conn,
            "select count(*) from transfer_jobs where state not in (6, 7, 8, 9)",
            &[],
        )
    }

    fn count_executing_transfers(&self) -> FsResult<u64> {
        let mut conn = self.conn()?;
        count_rows(
            &mut *conn,
            "select count(*) from transfer_jobs where state in (2, 3, 4, 5)",
            &[],
        )
    }

    fn list_transfers(&self, filter: TransferListFilter) -> FsResult<Vec<TransferJobRecord>> {
        let kind = filter.kind.map(|value| value as i32);
        let state = filter.state.map(|value| value as i32);
        let has_submitter = filter.submitter.is_some();
        let submitter = filter.submitter.unwrap_or_default();
        let has_tenant = filter.tenant.is_some();
        let tenant = filter.tenant.unwrap_or_default();
        let limit = db_limit(filter.limit)?;
        let offset = db_limit(filter.offset)?;
        let mut clauses = Vec::new();
        let mut params: Vec<&(dyn postgres::types::ToSql + Sync)> = Vec::new();
        if let Some(value) = kind.as_ref() {
            clauses.push(format!("kind = ${}", params.len() + 1));
            params.push(value);
        }
        if let Some(value) = state.as_ref() {
            clauses.push(format!("state = ${}", params.len() + 1));
            params.push(value);
        }
        if has_submitter {
            clauses.push(format!("submitter = ${}", params.len() + 1));
            params.push(&submitter);
        }
        if has_tenant {
            clauses.push(format!("tenant = ${}", params.len() + 1));
            params.push(&tenant);
        }
        let where_sql = if clauses.is_empty() {
            String::new()
        } else {
            format!("where {}", clauses.join(" and "))
        };
        let sql = format!(
            "select record_json from transfer_jobs {where_sql}
             order by updated_at desc, created_at desc, job_id desc
             limit ${} offset ${}",
            params.len() + 1,
            params.len() + 2
        );
        params.push(&limit);
        params.push(&offset);
        let mut conn = self.conn()?;
        select_jobs(&mut *conn, &sql, &params)
    }

    fn list_tenant_summaries(
        &self,
        limit: usize,
        offset: usize,
    ) -> FsResult<Vec<TransferTenantSummary>> {
        let limit = db_limit(limit)?;
        let offset = db_limit(offset)?;
        let rows = self
            .conn()?
            .query(
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
                 limit $1 offset $2",
                &[&limit, &offset],
            )
            .map_err(postgres_err)?;
        rows.iter().map(tenant_summary_from_row).collect()
    }

    fn purge_terminal_transfers(&self, older_than_ms: i64, limit: usize) -> FsResult<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let limit = db_limit(limit)?;
        self.transaction(|tx| {
            let job_ids = tx
                .query(
                    "select job_id from transfer_jobs
                     where state in (6, 7, 8, 9) and updated_at < $1
                     order by updated_at asc
                     limit $2
                     for update",
                    &[&older_than_ms, &limit],
                )
                .map_err(postgres_err)?;
            for row in &job_ids {
                let job_id: String = row.get(0);
                tx.execute("delete from transfer_tasks where job_id = $1", &[&job_id])
                    .map_err(postgres_err)?;
                tx.execute(
                    "delete from transfer_jobs
                     where job_id = $1 and state in (6, 7, 8, 9)",
                    &[&job_id],
                )
                .map_err(postgres_err)?;
            }
            Ok(job_ids.len())
        })
    }

    fn list_transfer_tasks(&self, job_id: &str, run_id: u64) -> FsResult<Vec<TransferTaskRecord>> {
        let mut conn = self.conn()?;
        select_tasks(&mut *conn, job_id, run_id, None, None)
    }

    fn request_cancel(&self, job_id: &str, run_id: u64, now_ms: i64) -> FsResult<bool> {
        self.transaction(|tx| {
            let mut job = match select_job_by_id_for_update(tx, job_id)? {
                Some(job) if job.run_id == run_id && !job.state.is_terminal() => job,
                _ => return Ok(false),
            };
            job.cancel_requested = true;
            job.state = TransferState::Canceling;
            job.summary.message = "cancel requested".to_string();
            job.summary.update_time = now_ms;
            job.updated_at = now_ms;
            update_job(tx, &job)
        })
    }

    fn acquire_runnable_transfer(
        &self,
        owner: &str,
        lease_ms: i64,
        now_ms: i64,
        max_executing_transfers: u64,
    ) -> FsResult<Option<TransferLease>> {
        let max_executing_transfers = db_u64(max_executing_transfers)?;
        self.transaction(|tx| {
            lock_submission_gate(tx)?;
            let executing = count_rows(
                tx,
                "select count(*) from transfer_jobs where state in (2, 3, 4, 5)",
                &[],
            )?;
            let executing = db_u64(executing)?;
            let mut candidate = select_runnable_owned(tx, owner, now_ms)?;
            if candidate.is_none() {
                candidate = select_expired_runnable(tx, now_ms)?;
            }
            if candidate.is_none() && executing < max_executing_transfers {
                candidate = select_pending_runnable(tx, now_ms)?;
            }
            let Some((job_id, run_id, lease_epoch, state)) = candidate else {
                return Ok(None);
            };
            let mut record =
                select_job_by_id(tx, &job_id)?.ok_or_else(|| FsError::job_not_found(&job_id))?;
            if state == TransferState::Pending as i32 {
                record.state = TransferState::Planning;
                record.summary.message = "acquired for planning".to_string();
                record.summary.update_time = now_ms;
            }
            record.owner = owner.to_string();
            record.lease_epoch = lease_epoch.saturating_add(1);
            record.lease_expire_at = now_ms.saturating_add(lease_ms);
            record.updated_at = now_ms;
            let affected = tx
                .execute(
                    "update transfer_jobs
                     set state = $1, owner = $2, lease_epoch = $3, lease_expire_at = $4,
                         record_json = $5, updated_at = $6
                     where job_id = $7 and run_id = $8 and lease_epoch = $9",
                    &[
                        &(record.state as i32),
                        &owner,
                        &db_u64(record.lease_epoch)?,
                        &record.lease_expire_at,
                        &json(&record)?,
                        &now_ms,
                        &job_id,
                        &db_u64(run_id)?,
                        &db_u64(lease_epoch)?,
                    ],
                )
                .map_err(postgres_err)?;
            if affected == 0 {
                return Ok(None);
            }
            Ok(Some(TransferLease {
                job_id,
                run_id,
                owner: owner.to_string(),
                lease_epoch: record.lease_epoch,
            }))
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
        self.transaction(|tx| {
            let mut job = match select_job_by_id_for_update(tx, job_id)? {
                Some(job)
                    if job.run_id == run_id
                        && job.owner == owner
                        && job.lease_epoch == lease_epoch
                        && job.lease_expire_at > now_ms
                        && !job.state.is_terminal() =>
                {
                    job
                }
                _ => return Ok(false),
            };
            job.lease_expire_at = now_ms.saturating_add(lease_ms);
            job.updated_at = now_ms;
            update_job(tx, &job)
        })
    }

    fn update_transfer_state(&self, update: TransferStateUpdate) -> FsResult<bool> {
        self.transaction(|tx| {
            let mut job = match select_job_by_id_for_update(tx, &update.job_id)? {
                Some(job)
                    if job.run_id == update.run_id
                        && job.owner == update.owner
                        && job.lease_epoch == update.lease_epoch
                        && job.lease_expire_at > update.now_ms
                        && update.from_states.contains(&job.state) =>
                {
                    job
                }
                _ => return Ok(false),
            };
            job.state = update.to_state;
            job.summary.message = update.message;
            job.summary.update_time = update.now_ms;
            job.updated_at = update.now_ms;
            update_job(tx, &job)
        })
    }

    fn requeue_transfer(&self, update: TransferRequeueUpdate) -> FsResult<bool> {
        self.transaction(|tx| {
            let mut job = match select_job_by_id_for_update(tx, &update.job_id)? {
                Some(job)
                    if job.run_id == update.run_id
                        && job.owner == update.owner
                        && job.lease_epoch == update.lease_epoch
                        && job.lease_expire_at > update.now_ms
                        && job.state == TransferState::Planning =>
                {
                    job
                }
                _ => return Ok(false),
            };
            job.state = TransferState::Pending;
            job.owner.clear();
            job.lease_expire_at = update.next_attempt_at_ms;
            job.summary.message = update.message;
            job.summary.update_time = update.now_ms;
            job.updated_at = update.now_ms;
            update_job(tx, &job)
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
        self.transaction(|tx| {
            let mut job = match select_job_by_id_for_update(tx, job_id)? {
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
                _ => return Ok(false),
            };
            job.cv_metadata_epoch = Some(cv_metadata_epoch);
            job.updated_at = now_ms;
            update_job(tx, &job)
        })
    }

    fn insert_tasks(&self, tasks: Vec<TransferTaskRecord>) -> FsResult<()> {
        self.transaction(|tx| {
            for task in &tasks {
                insert_task(tx, task)?;
            }
            Ok(())
        })
    }

    fn persist_planned_tasks(&self, update: TransferPlannedTasks) -> FsResult<bool> {
        self.transaction(|tx| {
            let mut job = match select_job_by_id_for_update(tx, &update.job_id)? {
                Some(job)
                    if job.run_id == update.run_id
                        && job.owner == update.owner
                        && job.lease_epoch == update.lease_epoch
                        && job.lease_expire_at > update.now_ms
                        && job.state == TransferState::Planning =>
                {
                    job
                }
                _ => return Ok(false),
            };
            for task in &update.tasks {
                insert_task(tx, task)?;
            }
            job.state = TransferState::Dispatching;
            job.summary.message = update.message;
            job.summary.update_time = update.now_ms;
            job.updated_at = update.now_ms;
            update_job(tx, &job)
        })
    }

    fn update_task_state(&self, update: TransferTaskStateUpdate) -> FsResult<bool> {
        self.transaction(|tx| {
            match select_job_by_id_for_update(tx, &update.job_id)? {
                Some(job)
                    if job.run_id == update.run_id
                        && job.owner == update.owner
                        && job.lease_epoch == update.lease_epoch
                        && job.lease_expire_at > update.now_ms
                        && !job.state.is_terminal() => {}
                _ => return Ok(false),
            }
            let mut task =
                match select_task_for_update(tx, &update.job_id, update.run_id, &update.task_id)? {
                    Some(task) => task,
                    None => return Ok(false),
                };
            if !update.from_states.is_empty() && !update.from_states.contains(&task.state) {
                return Ok(false);
            }
            task.state = update.state;
            task.progress.message = update.message;
            task.progress.update_time = update.now_ms;
            task.updated_at = update.now_ms;
            update_task(tx, &task)
        })
    }

    fn claim_pending_tasks(
        &self,
        job_id: &str,
        run_id: u64,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>> {
        let mut conn = self.conn()?;
        select_tasks(
            &mut *conn,
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
        let run_id_value = db_u64(run_id)?;
        let lease_epoch_value = db_u64(lease_epoch)?;
        let limit = db_limit(limit)?;
        self.transaction(|tx| {
            let owner_valid = tx
                .query_opt(
                    "select 1 from transfer_jobs
                     where job_id = $1 and run_id = $2 and owner = $3
                       and lease_epoch = $4 and lease_expire_at > $5",
                    &[&job_id, &run_id_value, &owner, &lease_epoch_value, &now_ms],
                )
                .map_err(postgres_err)?
                .is_some();
            if !owner_valid {
                return Ok(Vec::new());
            }
            let rows = tx
                .query(
                    "select record_json from transfer_tasks
                     where job_id = $1 and run_id = $2 and state = 2
                       and stale_deadline_at < $3
                     order by stale_deadline_at asc
                     limit $4
                     for update",
                    &[&job_id, &run_id_value, &now_ms, &limit],
                )
                .map_err(postgres_err)?;
            let mut stale = Vec::with_capacity(rows.len());
            for row in rows {
                let mut task = task_from_row(&row)?;
                task.state = TransferTaskState::Stale;
                task.retry_count = task.retry_count.saturating_add(1);
                task.progress.message = "task stale timeout".to_string();
                task.progress.update_time = now_ms;
                task.updated_at = now_ms;
                update_task(tx, &task)?;
                stale.push(StaleTaskAttempt { task });
            }
            Ok(stale)
        })
    }

    fn list_stale_running_tasks(
        &self,
        job_id: &str,
        run_id: u64,
        stale_before_ms: i64,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>> {
        let run_id = db_u64(run_id)?;
        let limit = db_limit(limit)?;
        let rows = self
            .conn()?
            .query(
                "select record_json from transfer_tasks
                 where job_id = $1 and run_id = $2 and state = 2
                   and stale_deadline_at < $3
                 order by stale_deadline_at asc
                 limit $4",
                &[&job_id, &run_id, &stale_before_ms, &limit],
            )
            .map_err(postgres_err)?;
        rows.iter().map(task_from_row).collect()
    }

    fn list_tasks_by_state(
        &self,
        job_id: &str,
        run_id: u64,
        state: TransferTaskState,
        limit: usize,
    ) -> FsResult<Vec<TransferTaskRecord>> {
        let mut conn = self.conn()?;
        select_tasks(&mut *conn, job_id, run_id, Some(state), Some(limit))
    }

    fn has_failed_tasks(&self, job_id: &str, run_id: u64) -> FsResult<bool> {
        let run_id = db_u64(run_id)?;
        self.conn()?
            .query_opt(
                "select 1 from transfer_tasks
                 where job_id = $1 and run_id = $2 and state = $3
                 limit 1",
                &[&job_id, &run_id, &(TransferTaskState::Failed as i32)],
            )
            .map(|row| row.is_some())
            .map_err(postgres_err)
    }

    fn has_recoverable_tasks(&self, job_id: &str, run_id: u64) -> FsResult<bool> {
        let run_id = db_u64(run_id)?;
        self.conn()?
            .query_opt(
                "select 1 from transfer_tasks
                 where job_id = $1 and run_id = $2 and state in (1, 2, 6)
                 limit 1",
                &[&job_id, &run_id],
            )
            .map(|row| row.is_some())
            .map_err(postgres_err)
    }

    fn start_task_attempt(&self, start: TaskAttemptStart) -> FsResult<bool> {
        self.transaction(|tx| {
            let Some(job) = select_job_by_id_for_update(tx, &start.job_id)? else {
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
            let mut task =
                match select_task_for_update(tx, &start.job_id, start.run_id, &start.task_id)? {
                    Some(task)
                        if matches!(
                            task.state,
                            TransferTaskState::Pending | TransferTaskState::Stale
                        ) =>
                    {
                        task
                    }
                    _ => return Ok(false),
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
            update_task(tx, &task)
        })
    }

    fn update_task_report(&self, report: TransferTaskReport) -> FsResult<bool> {
        self.transaction(|tx| {
            let mut job = match select_job_by_id_for_update(tx, &report.job_id)? {
                Some(job) if job.run_id == report.run_id => job,
                _ => return Ok(false),
            };
            let mut task =
                match select_task_for_update(tx, &report.job_id, report.run_id, &report.task_id)? {
                    Some(task)
                        if task.attempt_id == report.attempt_id
                            && task.worker_id == report.worker_id
                            && task.worker_session_id == report.worker_session_id
                            && task.state.is_running() =>
                    {
                        task
                    }
                    _ => return Ok(false),
                };
            let previous_progress = task.progress.clone();
            task.state = report.state;
            task.progress = report.progress.clone();
            task.last_report_at = report.now_ms;
            task.stale_deadline_at = report.stale_deadline_at;
            task.updated_at = report.now_ms;
            update_task(tx, &task)?;
            apply_task_report_progress(
                &mut job.summary,
                &previous_progress,
                &report.progress,
                report.now_ms,
            );
            job.updated_at = report.now_ms;
            update_job(tx, &job)?;
            Ok(true)
        })
    }
}
