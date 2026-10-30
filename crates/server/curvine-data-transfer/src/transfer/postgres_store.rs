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
    summarize_transfer_tasks, StaleTaskAttempt, TaskAttemptStart, TransferCommand,
    TransferCountFilter, TransferJobRecord, TransferLease, TransferListFilter, TransferProgress,
    TransferState, TransferStateCounts, TransferStateUpdate, TransferTaskCounts,
    TransferTaskRecord, TransferTaskReport, TransferTaskState, TransferTaskStatus,
    TransferTaskSummary, TransferTenantSummary, WorkerInflight,
};
use percent_encoding::percent_decode_str;
use postgres::{error::SqlState, GenericClient, Row, Transaction};
use postgres_rustls::MakeTlsConnector;
use r2d2::{Pool, PooledConnection};
use r2d2_postgres::PostgresConnectionManager;
use rustls::pki_types::CertificateDer;
use rustls::RootCertStore;
use rustls_pemfile::certs;
use std::collections::BTreeSet;
use std::error::Error as _;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio_rustls::TlsConnector as TokioTlsConnector;
use url::Url;

use crate::transfer::{
    ensure_task_transaction_bound, store_error_detail, TransferMountSnapshotUpdate,
    TransferPlannedTasks, TransferRequeueUpdate, TransferStore, TransferTaskStateUpdate,
    MAX_JOBS_PER_STORE_TRANSACTION, MAX_TASKS_PER_STORE_TRANSACTION,
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
        let tls = postgres_rustls_connector(root_cert_path)?;
        let manager = PostgresConnectionManager::new(config, tls);
        let pool = Pool::builder().build(manager).map_err(postgres_pool_err)?;
        let mut conn = pool.get().map_err(postgres_pool_err)?;
        if !postgres_schema_ready(&mut conn)? {
            init_schema(&mut conn)?;
        }
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

/// Builds a PostgreSQL TLS connector backed by rustls (no system OpenSSL).
pub fn postgres_rustls_connector(root_cert_path: Option<PathBuf>) -> FsResult<MakeTlsConnector> {
    let mut root_store = RootCertStore::empty();
    let native_roots = rustls_native_certs::load_native_certs();
    for cert in native_roots.certs {
        if root_store.add(cert).is_err() {
            log::warn!("transfer PostgreSQL TLS: skipped an invalid system root certificate");
        }
    }
    if let Some(path) = root_cert_path {
        let pem = fs::read(&path).map_err(|_| {
            FsError::common(format!(
                "Unable to read PostgreSQL root certificate at '{}'",
                path.display()
            ))
        })?;
        let mut reader = pem.as_slice();
        let parsed: Vec<CertificateDer<'static>> = certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| {
                FsError::common(format!(
                    "PostgreSQL root certificate at '{}' is not valid PEM",
                    path.display()
                ))
            })?;
        if parsed.is_empty() {
            return Err(FsError::common(format!(
                "PostgreSQL root certificate at '{}' contains no certificates",
                path.display()
            )));
        }
        for certificate in parsed {
            root_store.add(certificate).map_err(|_| {
                FsError::common(format!(
                    "PostgreSQL root certificate at '{}' is not a valid trust anchor",
                    path.display()
                ))
            })?;
        }
    }
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let connector = TokioTlsConnector::from(Arc::new(config));
    Ok(MakeTlsConnector::new(connector))
}

fn postgres_schema_ready(conn: &mut PgConnection) -> FsResult<bool> {
    let version = match conn.query_opt(
        "select version from transfer_schema_version where id = 1",
        &[],
    ) {
        Ok(version) => version.map(|row| row.get::<_, i64>(0)),
        Err(err) if err.code() == Some(&SqlState::UNDEFINED_TABLE) => return Ok(false),
        Err(err) => return Err(postgres_err(err)),
    };
    if version != Some(TRANSFER_SCHEMA_VERSION) {
        return Ok(false);
    }
    let runtime_column_count: i64 = conn
        .query_one(
            "select count(*)::bigint from information_schema.columns
             where table_schema = current_schema() and table_name = 'transfer_tasks'
               and column_name in (
                   'source_path', 'target_path', 'report_target_json', 'retry_count',
                   'attempt_started_at', 'last_report_at', 'progress_loaded_size',
                   'progress_total_size', 'progress_updated_at', 'progress_message'
               )",
            &[],
        )
        .map_err(postgres_err)?
        .get(0);
    // Projection triggers only maintain an optional cache. JSON-authoritative
    // readers must not start DDL merely because a legacy trigger is absent.
    Ok(runtime_column_count == 10)
}

fn init_schema(conn: &mut PgConnection) -> FsResult<()> {
    let mut tx = conn.transaction().map_err(postgres_err)?;
    tx.batch_execute("set local lock_timeout = '60s'; set local statement_timeout = '90s';")
        .map_err(postgres_err)?;
    tx.query_one("select pg_advisory_xact_lock($1)", &[&TRANSFER_SCHEMA_LOCK])
        .map_err(postgres_err)?;
    // Index creation is a performance concern only. Building indexes on an
    // existing multi-million-row table would block serving writes, so they are
    // created only when this call bootstraps fresh tables; existing databases
    // get missing indexes through maintenance-plane `create index concurrently`.
    let fresh_tables: bool = tx
        .query_one("select to_regclass('transfer_tasks') is null", &[])
        .map_err(postgres_err)?
        .get(0);
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
        create table if not exists transfer_cleanup_lease (
            id smallint primary key check (id = 1),
            owner text not null,
            lease_expire_at bigint not null
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
            source_path text not null default '',
            target_path text not null default '',
            report_target_json text not null default '',
            retry_count integer not null default 0,
            attempt_started_at bigint not null default 0,
            last_report_at bigint not null default 0,
            progress_loaded_size bigint not null default 0,
            progress_total_size bigint not null default 0,
            progress_updated_at bigint not null default 0,
            progress_message text not null default '',
            record_json text not null,
            updated_at bigint not null,
            primary key(job_id, run_id, task_id)
        );",
    )
    .map_err(postgres_err)?;
    if fresh_tables {
        tx.batch_execute(
            "create index if not exists transfer_jobs_job_key_state_idx
            on transfer_jobs(left(job_key, 255), state);
        create index if not exists transfer_jobs_target_state_idx
            on transfer_jobs(target_path, state);
        create index if not exists transfer_jobs_state_lease_idx
            on transfer_jobs(state, lease_expire_at);
        create index if not exists transfer_jobs_state_updated_idx
            on transfer_jobs(state, updated_at);
        create index if not exists transfer_jobs_owner_state_updated_idx
            on transfer_jobs(owner, state, updated_at);
        create index if not exists transfer_jobs_tenant_state_updated_idx
            on transfer_jobs(tenant, state, updated_at);
        create index if not exists transfer_jobs_submitter_state_updated_idx
            on transfer_jobs(submitter, state, updated_at);
        create index if not exists transfer_tasks_job_state_idx
            on transfer_tasks(job_id, run_id, state);
        create index if not exists transfer_tasks_job_updated_idx
            on transfer_tasks(job_id, run_id, updated_at);
        create index if not exists transfer_tasks_worker_idx
            on transfer_tasks(worker_id, worker_session_id, state);
        create index if not exists transfer_tasks_stale_idx
            on transfer_tasks(state, stale_deadline_at);",
        )
        .map_err(postgres_err)?;
    }
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
    migrate_postgres_task_runtime_columns(&mut tx)?;
    ensure_postgres_legacy_task_json_sync_trigger(&mut tx)?;
    tx.commit().map_err(postgres_err)?;
    Ok(())
}

// Additive schema only: `record_json` remains authoritative; never rewrite history
// merely to populate runtime projection columns.
fn migrate_postgres_task_runtime_columns(conn: &mut impl GenericClient) -> FsResult<()> {
    conn.batch_execute(
        "alter table transfer_tasks
             add column if not exists source_path text not null default '',
             add column if not exists target_path text not null default '',
             add column if not exists report_target_json text not null default '',
             add column if not exists retry_count integer not null default 0,
             add column if not exists attempt_started_at bigint not null default 0,
             add column if not exists last_report_at bigint not null default 0,
             add column if not exists progress_loaded_size bigint not null default 0,
             add column if not exists progress_total_size bigint not null default 0,
             add column if not exists progress_updated_at bigint not null default 0,
             add column if not exists progress_message text not null default '';",
    )
    .map_err(postgres_err)
}

fn ensure_postgres_legacy_task_json_sync_trigger(conn: &mut impl GenericClient) -> FsResult<()> {
    conn.batch_execute(
        "create or replace function curvine_sync_transfer_task_record_json()
         returns trigger language plpgsql as $$
         begin
             new.state := coalesce(case new.record_json::jsonb ->> 'state'
                 when 'Pending' then 1 when '1' then 1
                 when 'Running' then 2 when '2' then 2
                 when 'Completed' then 3 when '3' then 3
                 when 'Failed' then 4 when '4' then 4
                 when 'Canceled' then 5 when '5' then 5
                 when 'Stale' then 6 when '6' then 6
             end, new.state);
             new.attempt_id := coalesce((new.record_json::jsonb ->> 'attempt_id')::bigint, new.attempt_id);
             new.worker_id := coalesce((new.record_json::jsonb ->> 'worker_id')::bigint, new.worker_id);
             new.worker_session_id := coalesce(new.record_json::jsonb ->> 'worker_session_id', new.worker_session_id);
             new.stale_deadline_at := coalesce((new.record_json::jsonb ->> 'stale_deadline_at')::bigint, new.stale_deadline_at);
             new.source_path := coalesce(new.record_json::jsonb ->> 'source_path', new.source_path);
             new.target_path := coalesce(new.record_json::jsonb ->> 'target_path', new.target_path);
             new.report_target_json := coalesce(new.record_json::jsonb ->> 'report_target_json', new.report_target_json);
             new.retry_count := coalesce((new.record_json::jsonb ->> 'retry_count')::integer, new.retry_count);
             new.attempt_started_at := coalesce((new.record_json::jsonb ->> 'attempt_started_at')::bigint, new.attempt_started_at);
             new.last_report_at := coalesce((new.record_json::jsonb ->> 'last_report_at')::bigint, new.last_report_at);
             new.progress_loaded_size := coalesce((new.record_json::jsonb #>> '{progress,loaded_size}')::bigint, new.progress_loaded_size);
             new.progress_total_size := coalesce((new.record_json::jsonb #>> '{progress,total_size}')::bigint, new.progress_total_size);
             new.progress_updated_at := coalesce((new.record_json::jsonb #>> '{progress,update_time}')::bigint, new.progress_updated_at);
             new.progress_message := coalesce(new.record_json::jsonb #>> '{progress,message}', new.progress_message);
             new.updated_at := coalesce((new.record_json::jsonb ->> 'updated_at')::bigint, new.updated_at);
             return new;
         end;
         $$;
         do $$
         begin
             if not exists (
                 select 1 from pg_trigger
                 where tgrelid = 'transfer_tasks'::regclass
                   and tgname = 'transfer_tasks_record_json_sync'
                   and not tgisinternal
             ) then
                 create trigger transfer_tasks_record_json_sync
                 before update on transfer_tasks
                 for each row execute function curvine_sync_transfer_task_record_json();
             end if;
             if not exists (
                 select 1 from pg_trigger
                 where tgrelid = 'transfer_tasks'::regclass
                   and tgname = 'transfer_tasks_record_json_insert_sync'
                   and not tgisinternal
             ) then
                 create trigger transfer_tasks_record_json_insert_sync
                 before insert on transfer_tasks
                 for each row execute function curvine_sync_transfer_task_record_json();
             end if;
         end;
         $$;",
    )
    .map_err(postgres_err)
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
    lease_ms: i64,
) -> FsResult<Option<(String, u64, u64, i32)>> {
    let running_check_interval_ms = crate::transfer::running_transfer_check_interval_ms(lease_ms);
    let running_event_lease_before_ms =
        now_ms.saturating_add(lease_ms.saturating_sub(running_check_interval_ms));
    let running_renew_lease_before_ms = now_ms.saturating_add(lease_ms / 2);
    select_lease_candidate(
        conn,
        "select job_id, run_id, lease_epoch, state
         from transfer_jobs as job
         where job.owner = $1 and job.lease_expire_at > $2
           and (
                (job.state = 5 and job.updated_at < $2)
                or (
                    job.state = 4 and (
                        job.lease_expire_at <= $4
                        or (
                            job.lease_expire_at <= $3 and (
                                exists (
                                    select 1 from transfer_tasks as task
                                    where task.job_id = job.job_id and task.run_id = job.run_id
                                      and task.state in (1, 4, 6)
                                )
                                or exists (
                                    select 1 from transfer_tasks as task
                                    where task.job_id = job.job_id and task.run_id = job.run_id
                                      and task.state = 2 and task.stale_deadline_at < $2
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
         limit 1
         for update",
        &[
            &owner,
            &now_ms,
            &running_event_lease_before_ms,
            &running_renew_lease_before_ms,
        ],
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

fn has_pending_runnable(conn: &mut impl GenericClient, now_ms: i64) -> FsResult<bool> {
    conn.query_opt(
        "select 1 from transfer_jobs where state = 1 and lease_expire_at <= $1 limit 1",
        &[&now_ms],
    )
    .map(|value| value.is_some())
    .map_err(postgres_err)
}

fn select_pending_runnable(
    conn: &mut impl GenericClient,
    now_ms: i64,
    active_targets: &[(String, String)],
) -> FsResult<Option<(String, u64, u64, i32)>> {
    let mut values: Vec<Box<dyn postgres::types::ToSql + Sync>> = vec![Box::new(now_ms)];
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
        active_tenants.insert(tenant.clone());

        let (child_lower, child_upper) = child_path_bounds(&target_path);
        let exact = values.len() + 1;
        values.push(Box::new(target_path.clone()));
        let child_lower_index = values.len() + 1;
        values.push(Box::new(child_lower));
        let child_upper_index = values.len() + 1;
        values.push(Box::new(child_upper));
        let mut conflicts = vec![
            format!("target_path = ${exact}::text"),
            format!(
                "(target_path >= ${child_lower_index}::text and target_path < ${child_upper_index}::text)"
            ),
        ];

        let ancestors = ancestor_paths(&target_path);
        if !ancestors.is_empty() {
            let mut indexes = Vec::with_capacity(ancestors.len());
            for ancestor in ancestors {
                indexes.push((values.len() + 1).to_string());
                values.push(Box::new(ancestor));
            }
            conflicts.push(format!(
                "target_path in ({})",
                indexes
                    .into_iter()
                    .map(|index| format!("${index}::text"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        exclusions.push(format!("not ({})", conflicts.join(" or ")));
    }
    if root_active {
        // Exclusion parameters are unused when only fs_mode jobs may start.
        values.truncate(1);
    }

    let mut sql = "select job_id, run_id, lease_epoch, state
                   from transfer_jobs
                   where state = 1 and lease_expire_at <= $1::bigint"
        .to_string();
    if root_active {
        sql.push_str(" and submitter like 'fs-mode:%'");
    } else if !exclusions.is_empty() {
        sql.push_str(" and (submitter like 'fs-mode:%' or (");
        sql.push_str(&exclusions.join(" and "));
        sql.push_str("))");
    }
    if active_tenants.is_empty() {
        sql.push_str(" order by updated_at asc, job_id asc limit 1 for update");
    } else {
        let mut indexes = Vec::with_capacity(active_tenants.len());
        for tenant in active_tenants {
            indexes.push((values.len() + 1).to_string());
            values.push(Box::new(tenant));
        }
        sql.push_str(&format!(
            " order by case when tenant in ({}) then 1 else 0 end asc, updated_at asc, job_id asc limit 1 for update",
            indexes
                .into_iter()
                .map(|index| format!("${index}::text"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let params = values
        .iter()
        .map(|value| value.as_ref() as &(dyn postgres::types::ToSql + Sync))
        .collect::<Vec<_>>();
    select_lease_candidate(conn, &sql, &params)
}

fn select_executing_targets(conn: &mut impl GenericClient) -> FsResult<Vec<(String, String)>> {
    conn.query(
        "select target_path, tenant from transfer_jobs
         where state in (2, 3, 4, 5) and submitter not like 'fs-mode:%'",
        &[],
    )
    .map_err(postgres_err)
    .map(|rows| {
        rows.into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect()
    })
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

fn insert_job(conn: &mut impl GenericClient, job: &TransferJobRecord) -> FsResult<bool> {
    let run_id = db_u64(job.run_id)?;
    let lease_epoch = db_u64(job.lease_epoch)?;
    let record_json = json(job)?;
    conn.execute(
        "insert into transfer_jobs (
            job_id, submitter, tenant, client_request_id, job_key, target_path, run_id, kind, state, owner,
            lease_epoch, lease_expire_at, cancel_requested, record_json, created_at, updated_at
        ) values (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
        ) on conflict (submitter, client_request_id) do nothing",
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
    .map(|affected| affected > 0)
    .map_err(postgres_err)
}

fn insert_task(conn: &mut impl GenericClient, task: &TransferTaskRecord) -> FsResult<()> {
    let run_id = db_u64(task.run_id)?;
    let attempt_id = db_u64(task.attempt_id)?;
    let worker_id = i64::from(task.worker_id);
    let record_json = json(task)?;
    conn.execute(
        "insert into transfer_tasks (
            job_id, run_id, task_id, state, attempt_id, worker_id, worker_session_id,
            stale_deadline_at, source_path, target_path, report_target_json, retry_count,
            attempt_started_at, last_report_at, progress_loaded_size, progress_total_size,
            progress_updated_at, progress_message, record_json, updated_at
        ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
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
            &task.source_path,
            &task.target_path,
            &task.report_target_json,
            &(task.retry_count as i32),
            &task.attempt_started_at,
            &task.last_report_at,
            &task.progress.loaded_size,
            &task.progress.total_size,
            &task.progress.update_time,
            &task.progress.message,
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
    let state = task.state as i32;
    let retry_count = task.retry_count as i32;
    let record_json = json(task)?;
    conn.execute(
        "update transfer_tasks
         set state = $1, attempt_id = $2, worker_id = $3, worker_session_id = $4,
             stale_deadline_at = $5, report_target_json = $6, retry_count = $7,
             attempt_started_at = $8, last_report_at = $9, progress_loaded_size = $10,
             progress_total_size = $11, progress_updated_at = $12, progress_message = $13,
             record_json = $14, updated_at = $15
         where job_id = $16 and run_id = $17 and task_id = $18",
        &[
            &state,
            &attempt_id,
            &worker_id,
            &task.worker_session_id,
            &task.stale_deadline_at,
            &task.report_target_json,
            &retry_count,
            &task.attempt_started_at,
            &task.last_report_at,
            &task.progress.loaded_size,
            &task.progress.total_size,
            &task.progress.update_time,
            &task.progress.message,
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
    // fs_mode jobs neither conflict nor are conflicted with (see
    // `TransferCommand::is_fs_mode_submitter`).
    if TransferCommand::is_fs_mode_submitter(submitter) {
        return Ok(None);
    }
    let (child_lower, child_upper) = child_path_bounds(target_path);
    let exact_or_child = select_job(
        conn,
        "select record_json from transfer_jobs
         where state not in (6, 7, 8, 9)
           and submitter not like 'fs-mode:%'
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
               and submitter not like 'fs-mode:%'
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
        "select record_json, state, attempt_id, worker_id, worker_session_id,
                stale_deadline_at, report_target_json, retry_count, attempt_started_at,
                last_report_at, progress_loaded_size, progress_total_size,
                progress_updated_at, progress_message, updated_at
         from transfer_tasks
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
            "select record_json, state, attempt_id, worker_id, worker_session_id,
                    stale_deadline_at, report_target_json, retry_count, attempt_started_at,
                    last_report_at, progress_loaded_size, progress_total_size,
                    progress_updated_at, progress_message, updated_at
             from transfer_tasks
             where job_id = $1 and run_id = $2 and state = $3
             order by task_id asc
             limit $4",
            &[&job_id, &run_id, state, limit],
        ),
        (Some(state), None) => conn.query(
            "select record_json, state, attempt_id, worker_id, worker_session_id,
                    stale_deadline_at, report_target_json, retry_count, attempt_started_at,
                    last_report_at, progress_loaded_size, progress_total_size,
                    progress_updated_at, progress_message, updated_at
             from transfer_tasks
             where job_id = $1 and run_id = $2 and state = $3
             order by task_id asc",
            &[&job_id, &run_id, state],
        ),
        (None, Some(limit)) => conn.query(
            "select record_json, state, attempt_id, worker_id, worker_session_id,
                    stale_deadline_at, report_target_json, retry_count, attempt_started_at,
                    last_report_at, progress_loaded_size, progress_total_size,
                    progress_updated_at, progress_message, updated_at
             from transfer_tasks
             where job_id = $1 and run_id = $2
             order by task_id asc
             limit $3",
            &[&job_id, &run_id, limit],
        ),
        (None, None) => conn.query(
            "select record_json, state, attempt_id, worker_id, worker_session_id,
                    stale_deadline_at, report_target_json, retry_count, attempt_started_at,
                    last_report_at, progress_loaded_size, progress_total_size,
                    progress_updated_at, progress_message, updated_at
             from transfer_tasks
             where job_id = $1 and run_id = $2
             order by task_id asc",
            &[&job_id, &run_id],
        ),
    }
    .map_err(postgres_err)?;
    rows.iter().map(task_from_row).collect()
}

fn select_task_status_page(
    conn: &mut impl GenericClient,
    job_id: &str,
    run_id: u64,
    offset: usize,
    limit: usize,
) -> FsResult<(Vec<TransferTaskStatus>, bool)> {
    let mut tasks = select_tasks(conn, job_id, run_id, None, None)?;
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

fn select_task_summary(
    conn: &mut impl GenericClient,
    job_id: &str,
    run_id: u64,
    now_ms: i64,
) -> FsResult<TransferTaskSummary> {
    Ok(summarize_transfer_tasks(
        &select_tasks(conn, job_id, run_id, None, None)?,
        now_ms,
    ))
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

/// Shared WHERE builder for the job list and the state-grouped count, so the two
/// cannot drift apart.
pub(super) fn transfer_filter_clause<'a>(
    kind: &'a Option<i32>,
    states: &'a [i32],
    submitter: Option<&'a String>,
    tenant: Option<&'a String>,
    clauses: &mut Vec<String>,
    params: &mut Vec<&'a (dyn postgres::types::ToSql + Sync)>,
) {
    if let Some(value) = kind.as_ref() {
        clauses.push(format!("kind = ${}", params.len() + 1));
        params.push(value);
    }
    if !states.is_empty() {
        let first = params.len() + 1;
        let placeholders = (first..first + states.len())
            .map(|index| format!("${index}"))
            .collect::<Vec<_>>()
            .join(", ");
        clauses.push(format!("state in ({placeholders})"));
        for state in states {
            params.push(state);
        }
    }
    if let Some(value) = submitter {
        clauses.push(format!("submitter = ${}", params.len() + 1));
        params.push(value);
    }
    if let Some(value) = tenant {
        clauses.push(format!("tenant = ${}", params.len() + 1));
        params.push(value);
    }
}

fn where_sql_of(clauses: &[String]) -> String {
    if clauses.is_empty() {
        String::new()
    } else {
        format!("where {}", clauses.join(" and "))
    }
}

/// Job counts grouped by state over the whole table, for the console's buckets.
fn count_transfers_by_state_sql(
    conn: &mut impl GenericClient,
    kind: &Option<i32>,
    submitter: Option<&String>,
    tenant: Option<&String>,
) -> FsResult<TransferStateCounts> {
    let mut clauses = Vec::new();
    let mut params: Vec<&(dyn postgres::types::ToSql + Sync)> = Vec::new();
    transfer_filter_clause(kind, &[], submitter, tenant, &mut clauses, &mut params);
    let sql = format!(
        "select state, count(*) from transfer_jobs {} group by state",
        where_sql_of(&clauses)
    );
    let mut counts = TransferStateCounts::default();
    for row in conn
        .query(sql.as_str(), &params)
        .map_err(postgres_err)?
        .iter()
    {
        let state: i32 = row.get(0);
        let count: i64 = row.get(1);
        counts.add(
            TransferState::from(state),
            u64::try_from(count).unwrap_or(0),
        );
    }
    Ok(counts)
}

fn job_from_row(row: &Row) -> FsResult<TransferJobRecord> {
    let value: String = row.get(0);
    serde_json::from_str(&value).map_err(json_err)
}

fn task_from_row(row: &Row) -> FsResult<TransferTaskRecord> {
    // Runtime columns are a cache. Historical rows remain JSON-authoritative.
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

fn normalize_target_path(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        trimmed.to_string()
    }
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

fn postgres_rustls_error(err: &postgres::Error) -> Option<String> {
    fn walk(source: &(dyn std::error::Error + 'static)) -> Option<String> {
        if let Some(rustls_error) = source.downcast_ref::<rustls::Error>() {
            return Some(rustls_error.to_string());
        }
        source.source().and_then(walk)
    }
    err.source().and_then(walk)
}

fn postgres_err(err: postgres::Error) -> FsError {
    log::warn!("transfer PostgreSQL store operation failed: {}", err);
    if let Some(tls_error) = postgres_rustls_error(&err) {
        return FsError::transfer_store_unavailable(format!(
            "PostgreSQL TLS handshake failed: {tls_error}; verify the server certificate, hostname, and transfer.store_url sslrootcert"
        ));
    }
    if postgres_error_is_unavailable(&err) {
        FsError::transfer_store_unavailable(format!(
            "Transfer metadata store is unavailable; verify transfer.store_url and PostgreSQL connectivity: {}",
            store_error_detail(&err)
        ))
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
    FsError::transfer_store_unavailable(format!(
        "Transfer metadata store is unavailable; verify transfer.store_url and PostgreSQL connectivity: {}",
        store_error_detail(&err)
    ))
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
            if insert_job(tx, &job)? {
                return Ok(job);
            }
            let existing = select_job_by_request(tx, &job.submitter, &job.client_request_id)?
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
            Ok(existing)
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

    fn has_active_transfer_by_key(&self, job_key: &str) -> FsResult<bool> {
        let mut conn = self.conn()?;
        Ok(select_non_terminal_job_by_key(&mut *conn, job_key)?.is_some())
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
            "select count(*) from transfer_jobs where state in (1, 2, 3, 4, 5)",
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
        let states: Vec<i32> = filter.states.iter().map(|state| *state as i32).collect();
        let submitter = filter.submitter;
        let tenant = filter.tenant;
        let limit = db_limit(filter.limit)?;
        let offset = db_limit(filter.offset)?;
        let mut clauses = Vec::new();
        let mut params: Vec<&(dyn postgres::types::ToSql + Sync)> = Vec::new();
        transfer_filter_clause(
            &kind,
            &states,
            submitter.as_ref(),
            tenant.as_ref(),
            &mut clauses,
            &mut params,
        );
        let where_sql = where_sql_of(&clauses);
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

    fn count_transfers_by_state(
        &self,
        filter: &TransferCountFilter,
    ) -> FsResult<TransferStateCounts> {
        let kind = filter.kind.map(|value| value as i32);
        let mut conn = self.conn()?;
        count_transfers_by_state_sql(
            &mut *conn,
            &kind,
            filter.submitter.as_ref(),
            filter.tenant.as_ref(),
        )
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
        let _ =
            self.purge_terminal_tasks(older_than_ms, limit.min(MAX_TASKS_PER_STORE_TRANSACTION))?;
        self.purge_terminal_jobs(older_than_ms, limit.min(MAX_JOBS_PER_STORE_TRANSACTION))
    }

    fn purge_terminal_tasks(&self, older_than_ms: i64, task_limit: usize) -> FsResult<usize> {
        if task_limit == 0 {
            return Ok(0);
        }
        let limit = db_limit(task_limit.min(MAX_TASKS_PER_STORE_TRANSACTION))?;
        self.transaction(|tx| {
            let affected = tx
                .execute(
                    "with candidates as (
                         select t.ctid
                         from transfer_tasks t
                         join transfer_jobs j on t.job_id = j.job_id and t.run_id = j.run_id
                         where j.state in (6, 7, 8, 9) and j.updated_at < $1
                           and (j.record_json::jsonb ->> 'terminal_summary') is not null
                           and (j.record_json::jsonb ->> 'terminal_counts') is not null
                         order by j.updated_at asc, t.task_id asc
                         limit $2
                         for update of t
                     )
                     delete from transfer_tasks t using candidates
                     where t.ctid = candidates.ctid",
                    &[&older_than_ms, &limit],
                )
                .map_err(postgres_err)?;
            Ok(affected as usize)
        })
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
        let limit = db_limit(task_limit.min(MAX_TASKS_PER_STORE_TRANSACTION))?;
        self.transaction(|tx| {
            let lease_owned = tx
                .query_opt(
                    "select 1 from transfer_cleanup_lease
                     where id = 1 and owner = $1 and lease_expire_at > $2 for update",
                    &[&owner, &now_ms],
                )
                .map_err(postgres_err)?
                .is_some();
            if !lease_owned {
                return Ok(0);
            }
            let affected = tx
                .execute(
                    "with candidates as (
                         select t.ctid
                         from transfer_tasks t
                         join transfer_jobs j on t.job_id = j.job_id and t.run_id = j.run_id
                         where j.state in (6, 7, 8, 9) and j.updated_at < $1
                           and (j.record_json::jsonb ->> 'terminal_summary') is not null
                           and (j.record_json::jsonb ->> 'terminal_counts') is not null
                         order by j.updated_at asc, t.task_id asc
                         limit $2
                         for update of t
                     )
                     delete from transfer_tasks t using candidates
                     where t.ctid = candidates.ctid",
                    &[&older_than_ms, &limit],
                )
                .map_err(postgres_err)?;
            Ok(affected as usize)
        })
    }

    fn purge_terminal_jobs(&self, older_than_ms: i64, limit: usize) -> FsResult<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let limit = db_limit(limit.min(MAX_JOBS_PER_STORE_TRANSACTION))?;
        self.transaction(|tx| {
            let rows = tx
                .query(
                    "select j.job_id, j.run_id from transfer_jobs j
                     where j.state in (6, 7, 8, 9) and j.updated_at < $1
                       and not exists (
                           select 1 from transfer_tasks t
                           where t.job_id = j.job_id and t.run_id = j.run_id
                       )
                     order by j.updated_at asc
                     limit $2
                     for update of j",
                    &[&older_than_ms, &limit],
                )
                .map_err(postgres_err)?;
            let keys = rows
                .iter()
                .map(|row| (row.get::<_, String>(0), row.get::<_, i64>(1)))
                .collect::<Vec<_>>();
            for (job_id, run_id) in &keys {
                tx.execute(
                    "delete from transfer_jobs
                     where job_id = $1 and run_id = $2 and state in (6, 7, 8, 9)
                       and not exists (
                           select 1 from transfer_tasks t
                           where t.job_id = $1 and t.run_id = $2
                       )",
                    &[job_id, run_id],
                )
                .map_err(postgres_err)?;
            }
            Ok(keys.len())
        })
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
        let limit = db_limit(limit.min(MAX_JOBS_PER_STORE_TRANSACTION))?;
        self.transaction(|tx| {
            let lease_owned = tx
                .query_opt(
                    "select 1 from transfer_cleanup_lease
                     where id = 1 and owner = $1 and lease_expire_at > $2 for update",
                    &[&owner, &now_ms],
                )
                .map_err(postgres_err)?
                .is_some();
            if !lease_owned {
                return Ok(0);
            }
            let affected = tx
                .execute(
                    "with candidates as (
                         select j.ctid
                         from transfer_jobs j
                         where j.state in (6, 7, 8, 9) and j.updated_at < $1
                           and not exists (
                               select 1 from transfer_tasks t
                               where t.job_id = j.job_id and t.run_id = j.run_id
                           )
                         order by j.updated_at asc
                         limit $2
                         for update of j
                     )
                     delete from transfer_jobs j using candidates
                     where j.ctid = candidates.ctid",
                    &[&older_than_ms, &limit],
                )
                .map_err(postgres_err)?;
            Ok(affected as usize)
        })
    }

    fn try_acquire_cleanup_lease(&self, owner: &str, now_ms: i64, lease_ms: i64) -> FsResult<bool> {
        self.transaction(|tx| {
            // Conditional upsert is required: SELECT ... FOR UPDATE does
            // not lock an absent row, so two first-time schedulers could
            // otherwise both decide the lease is available.
            let acquired = tx
                .query_opt(
                    "insert into transfer_cleanup_lease(id, owner, lease_expire_at)
                     values (1, $1, $2)
                     on conflict(id) do update set
                         owner=excluded.owner,
                         lease_expire_at=excluded.lease_expire_at
                     where transfer_cleanup_lease.owner=excluded.owner
                        or transfer_cleanup_lease.lease_expire_at <= $3
                     returning owner",
                    &[&owner, &now_ms.saturating_add(lease_ms.max(1)), &now_ms],
                )
                .map_err(postgres_err)?;
            Ok(acquired.is_some())
        })
    }

    fn list_transfer_tasks(&self, job_id: &str, run_id: u64) -> FsResult<Vec<TransferTaskRecord>> {
        let mut conn = self.conn()?;
        select_tasks(&mut *conn, job_id, run_id, None, None)
    }

    fn count_tasks_global(&self) -> FsResult<TransferTaskCounts> {
        let mut conn = self.conn()?;
        let mut counts = TransferTaskCounts::default();
        let rows = conn
            .query(
                "select state, count(*) from transfer_tasks group by state",
                &[],
            )
            .map_err(postgres_err)?;
        for row in rows {
            let state: i32 = row.get(0);
            let n: i64 = row.get(1);
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
        let mut conn = self.conn()?;
        select_task_summary(&mut *conn, job_id, run_id, now_ms)
    }

    fn list_transfer_task_status_page(
        &self,
        job_id: &str,
        run_id: u64,
        offset: usize,
        limit: usize,
    ) -> FsResult<(Vec<TransferTaskStatus>, bool)> {
        let mut conn = self.conn()?;
        select_task_status_page(&mut *conn, job_id, run_id, offset, limit)
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
        prefer_pending: bool,
    ) -> FsResult<Option<TransferLease>> {
        let max_executing_transfers = db_u64(max_executing_transfers)?;
        self.transaction(|tx| {
            lock_submission_gate(tx)?;
            let mut candidate = if !prefer_pending {
                select_runnable_owned(tx, owner, now_ms, lease_ms)?
            } else {
                None
            };
            if candidate.is_none() {
                candidate = select_expired_runnable(tx, now_ms)?;
            }
            if candidate.is_none() && has_pending_runnable(tx, now_ms)? {
                let executing = count_rows(
                    tx,
                    "select count(*) from transfer_jobs where state in (2, 3, 4, 5)",
                    &[],
                )?;
                if db_u64(executing)? < max_executing_transfers {
                    let active_targets = select_executing_targets(tx)?;
                    candidate = select_pending_runnable(tx, now_ms, &active_targets)?;
                }
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
            if let Some(mount_snapshot_attempts) = update.mount_snapshot_attempts {
                job.mount_snapshot_attempts = mount_snapshot_attempts;
            }
            if let Some(planning_attempts) = update.planning_attempts {
                job.planning_attempts = planning_attempts;
            }
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

    fn set_transfer_mount_snapshot(&self, update: TransferMountSnapshotUpdate) -> FsResult<bool> {
        self.transaction(|tx| {
            let mut job = match select_job_by_id_for_update(tx, &update.job_id)? {
                Some(job)
                    if job.run_id == update.run_id
                        && job.owner == update.owner
                        && job.lease_epoch == update.lease_epoch
                        && job.lease_expire_at > update.now_ms
                        && !job.state.is_terminal() =>
                {
                    job
                }
                _ => return Ok(false),
            };
            if !job.mount_snapshot_json.trim().is_empty() && job.mount_snapshot_json != "{}" {
                return Ok(job.mount_snapshot_json == update.mount_snapshot_json);
            }
            job.mount_snapshot_json = update.mount_snapshot_json;
            job.cluster_snapshot_version = update.cluster_snapshot_version;
            job.updated_at = update.now_ms;
            update_job(tx, &job)
        })
    }

    fn insert_tasks(&self, tasks: Vec<TransferTaskRecord>) -> FsResult<()> {
        ensure_task_transaction_bound(tasks.len())?;
        self.transaction(|tx| {
            for task in &tasks {
                insert_task(tx, task)?;
            }
            Ok(())
        })
    }

    fn append_planned_tasks(&self, update: TransferPlannedTasks) -> FsResult<bool> {
        ensure_task_transaction_bound(update.tasks.len())?;
        self.transaction(|tx| {
            match select_job_by_id_for_update(tx, &update.job_id)? {
                Some(job)
                    if job.run_id == update.run_id
                        && job.owner == update.owner
                        && job.lease_epoch == update.lease_epoch
                        && job.lease_expire_at > update.now_ms
                        && job.state == TransferState::Planning => {}
                _ => return Ok(false),
            }
            for task in &update.tasks {
                insert_task(tx, task)?;
            }
            Ok(true)
        })
    }

    fn persist_planned_tasks(&self, update: TransferPlannedTasks) -> FsResult<bool> {
        ensure_task_transaction_bound(update.tasks.len())?;
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

    fn has_transfer_tasks(&self, job_id: &str, run_id: u64) -> FsResult<bool> {
        let run_id = db_u64(run_id)?;
        self.conn()?
            .query_opt(
                "select 1 from transfer_tasks where job_id = $1 and run_id = $2 limit 1",
                &[&job_id, &run_id],
            )
            .map(|row| row.is_some())
            .map_err(postgres_err)
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
            if update.bump_retry {
                task.retry_count = task.retry_count.saturating_add(1);
            }
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

    fn select_inflight_by_worker(&self) -> FsResult<Vec<WorkerInflight>> {
        let mut conn = self.conn()?;
        let rows = conn
            .query(
                "select worker_id, count(*), coalesce(sum(progress_total_size), 0),
                        count(*) filter (where progress_total_size > 0)
             from transfer_tasks where state = 2
             group by worker_id",
                &[],
            )
            .map_err(postgres_err)?;
        Ok(rows
            .iter()
            .map(|row| WorkerInflight {
                worker_id: u32::try_from(row.get::<_, i64>(0)).unwrap_or(0),
                task_count: u64::try_from(row.get::<_, i64>(1)).unwrap_or(u64::MAX),
                estimated_bytes: row.get::<_, i64>(2),
                unknown_bytes_count: u64::try_from(
                    row.get::<_, i64>(1).saturating_sub(row.get::<_, i64>(3)),
                )
                .unwrap_or(0),
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
        let run_id_value = db_u64(run_id)?;
        let lease_epoch_value = db_u64(lease_epoch)?;
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
                let attempt_id = db_u64(candidate.attempt_id)?;
                let retry_count = task.retry_count as i32;
                let record_json = json(&task)?;
                let updated = tx
                    .execute(
                        "update transfer_tasks
                         set state = $1, retry_count = $2, progress_updated_at = $3,
                             progress_message = $4, record_json = $5, updated_at = $6
                         where job_id = $7 and run_id = $8 and task_id = $9 and state = 2
                           and attempt_id = $10 and worker_id = $11 and worker_session_id = $12
                           and stale_deadline_at = $13 and stale_deadline_at < $6
                           and exists (
                               select 1 from transfer_jobs
                               where job_id = $7 and run_id = $8 and owner = $14
                                 and lease_epoch = $15 and lease_expire_at > $6
                           )",
                        &[
                            &(task.state as i32),
                            &retry_count,
                            &task.progress.update_time,
                            &task.progress.message,
                            &record_json,
                            &task.updated_at,
                            &job_id,
                            &run_id_value,
                            &task.task_id,
                            &attempt_id,
                            &i64::from(candidate.worker_id),
                            &candidate.worker_session_id,
                            &candidate.stale_deadline_at,
                            &owner,
                            &lease_epoch_value,
                        ],
                    )
                    .map_err(postgres_err)?
                    > 0;
                if updated {
                    stale.push(StaleTaskAttempt { task });
                }
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
                "select record_json, state, attempt_id, worker_id, worker_session_id,
                        stale_deadline_at, report_target_json, retry_count, attempt_started_at,
                        last_report_at, progress_loaded_size, progress_total_size,
                        progress_updated_at, progress_message, updated_at
                 from transfer_tasks
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

    fn list_failed_tasks(&self, job_id: &str, run_id: u64) -> FsResult<Vec<TransferTaskRecord>> {
        let mut conn = self.conn()?;
        select_tasks(
            &mut *conn,
            job_id,
            run_id,
            Some(TransferTaskState::Failed),
            None,
        )
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
            task.progress.loaded_size = start
                .resume_loaded_size
                .clamp(0, task.progress.total_size.max(0));
            task.progress.update_time = start.now_ms;
            update_task(tx, &task)
        })
    }

    fn get_task_attempt_started_at(
        &self,
        job_id: &str,
        run_id: u64,
        task_id: &str,
    ) -> FsResult<Option<i64>> {
        let run_id = db_u64(run_id)?;
        let row: Option<i64> = self
            .conn()?
            .query_opt(
                "select attempt_started_at from transfer_tasks where job_id = $1 and run_id = $2 and task_id = $3",
                &[&job_id, &run_id, &task_id],
            )
            .map_err(postgres_err)?
            .map(|row| row.get(0));
        Ok(row)
    }

    fn update_task_report(&self, report: TransferTaskReport) -> FsResult<bool> {
        let run_id = db_u64(report.run_id)?;
        let attempt_id = db_u64(report.attempt_id)?;
        let loaded = match report.state {
            TransferTaskState::Running => report.progress.loaded_size.max(0),
            _ => report.progress.loaded_size,
        };
        let loaded_expr = match report.state {
            TransferTaskState::Running => "greatest(progress_loaded_size, $2)",
            _ => "$2",
        };
        let loaded_json_expr = match report.state {
            TransferTaskState::Running => {
                "greatest((record_json::jsonb->'progress'->>'loaded_size')::bigint, $2::bigint)"
            }
            _ => "$2::bigint",
        };
        let sql = format!(
            "update transfer_tasks
             set state = $1, progress_loaded_size = {loaded_expr}, progress_total_size = $3,
                 progress_updated_at = $4, progress_message = $5, last_report_at = $6,
                 stale_deadline_at = $7,
                 record_json = jsonb_set(
                       jsonb_set(
                        jsonb_set(
                            jsonb_set(
                                jsonb_set(
                                    jsonb_set(
                                        jsonb_set(
                                            jsonb_set(
                                                jsonb_set(
                                                    jsonb_set(
                                                        jsonb_set(record_json::jsonb, '{{state}}', to_jsonb(case $1::integer
                                                        when 1 then 'Pending'::text
                                                        when 2 then 'Running'::text
                                                        when 3 then 'Completed'::text
                                                        when 4 then 'Failed'::text
                                                        when 5 then 'Canceled'::text
                                                        when 6 then 'Stale'::text
                                                        else null::text
                                                    end)),
                                                    '{{progress,loaded_size}}', to_jsonb({loaded_json_expr})),
                                                '{{progress,total_size}}', to_jsonb($3::bigint)),
                                            '{{progress,update_time}}', to_jsonb($4::bigint)),
                                        '{{progress,message}}', to_jsonb($5::text)),
                                    '{{last_report_at}}', to_jsonb($6::bigint)),
                                '{{stale_deadline_at}}', to_jsonb($7::bigint)),
                            '{{updated_at}}', to_jsonb($8::bigint)),
                        '{{accepted_at_ms}}', to_jsonb($15::bigint)),
                    '{{first_byte_at_ms}}', to_jsonb($16::bigint)),
                '{{block_locations}}', $17::text::jsonb)::text,
                 updated_at = $8
             where job_id = $9 and run_id = $10 and task_id = $11 and attempt_id = $12
               and worker_id = $13 and worker_session_id = $14 and state in (1, 2)"
        );
        let block_locations_json = json(&report.block_locations)?;
        self.conn()?
            .execute(
                &sql,
                &[
                    &(report.state as i32),
                    &loaded,
                    &report.progress.total_size,
                    &report.progress.update_time,
                    &report.progress.message,
                    &report.now_ms,
                    &report.stale_deadline_at,
                    &report.now_ms,
                    &report.job_id,
                    &run_id,
                    &report.task_id,
                    &attempt_id,
                    &i64::from(report.worker_id),
                    &report.worker_session_id,
                    &report.accepted_at_ms,
                    &report.first_byte_at_ms,
                    &block_locations_json,
                ],
            )
            .map(|affected| affected > 0)
            .map_err(postgres_err)
    }

    fn update_job_progress_summary(
        &self,
        job_id: &str,
        run_id: u64,
        summary: TransferProgress,
        now_ms: i64,
    ) -> FsResult<()> {
        let run_id = db_u64(run_id)?;
        let summary_json = json(&summary)?;
        self.conn()?
            .execute(
                "update transfer_jobs
                 set record_json = jsonb_set(
                        jsonb_set(record_json::jsonb, '{summary}', $1::text::jsonb),
                        '{updated_at}', to_jsonb($2::bigint)
                     )::text,
                     updated_at = $2 where job_id = $3 and run_id = $4",
                &[&summary_json, &now_ms, &job_id, &run_id],
            )
            .map(|_| ())
            .map_err(postgres_err)
    }

    fn update_terminal_snapshot(
        &self,
        job_id: &str,
        run_id: u64,
        summary: TransferProgress,
        counts: TransferTaskCounts,
        now_ms: i64,
    ) -> FsResult<()> {
        let run_id = db_u64(run_id)?;
        let summary_json = json(&summary)?;
        let counts_json = json(&counts)?;
        self.conn()?
            .execute(
                "update transfer_jobs
                 set record_json = jsonb_set(
                        jsonb_set(
                            jsonb_set(record_json::jsonb, '{summary}', $1::text::jsonb),
                            '{terminal_summary}', $1::text::jsonb),
                        '{terminal_counts}', $2::text::jsonb)::text,
                     updated_at = $3
                 where job_id = $4 and run_id = $5
                   and (record_json::jsonb ->> 'terminal_summary') is null",
                &[&summary_json, &counts_json, &now_ms, &job_id, &run_id],
            )
            .map(|_| ())
            .map_err(postgres_err)
    }
}
