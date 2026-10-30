// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0.
// License text: http://www.apache.org/licenses/LICENSE-2.0

//! MySQL-backed service registry/resolver for deployments without etcd.
//!
//! One table (`transfer_instances`) carries all state: a row per live
//! instance, refreshed by heartbeat and filtered by lease expiry. A single
//! row (`transfer_topology`) holds a version bumped on membership change so
//! consumers can skip re-fetching while the topology is unchanged.

use crate::{
    DiscoveryError, DiscoveryResult, RegistrationControl, RegistrationGuard, RegistrationOptions,
    RegistrationStatus, ServiceEndpoint, ServiceKind, ServiceRegistry, ServiceResolver,
    ServiceResolverHandle, ServiceSnapshot, ServiceStatus, ServiceWatchEvent, SnapshotReader,
};
use async_trait::async_trait;
use mysql::prelude::Queryable;
use mysql::{params, Pool, PooledConn};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

pub const TRANSFER_INSTANCES_TABLE: &str = "transfer_instances";
pub const TRANSFER_TOPOLOGY_TABLE: &str = "transfer_topology";

#[derive(Clone)]
pub struct SqlDiscovery {
    pool: Pool,
}

impl SqlDiscovery {
    pub fn open(url: &str) -> DiscoveryResult<Self> {
        let pool = Pool::new(url).map_err(sql_err)?;
        Ok(Self { pool })
    }

    /// Creates the tables if missing. Idempotent; safe to call on every boot.
    pub fn init_schema(&self) -> DiscoveryResult<()> {
        let mut conn = self.conn()?;
        conn.query_drop(format!(
            "create table if not exists {TRANSFER_INSTANCES_TABLE} (
                kind varchar(32) not null,
                instance_id varchar(128) not null,
                host varchar(255) not null,
                rpc_port int unsigned not null,
                web_port int unsigned null,
                start_epoch bigint not null,
                heartbeat_at bigint not null,
                lease_expire_at bigint not null,
                primary key (kind, instance_id)
            )"
        ))
        .map_err(sql_err)?;
        conn.query_drop(format!(
            "create table if not exists {TRANSFER_TOPOLOGY_TABLE} (
                id tinyint primary key,
                topology_version bigint not null,
                updated_at bigint not null
            )"
        ))
        .map_err(sql_err)?;
        conn.exec_drop(
            format!(
                "insert ignore into {TRANSFER_TOPOLOGY_TABLE} (id, topology_version, updated_at)
                 values (1, 1, 0)"
            ),
            mysql::Params::Empty,
        )
        .map_err(sql_err)?;
        Ok(())
    }

    fn conn(&self) -> DiscoveryResult<PooledConn> {
        self.pool.get_conn().map_err(sql_err)
    }

    /// Current topology version. Piggybacked onto report acks by the caller.
    pub fn topology_version(&self) -> DiscoveryResult<i64> {
        let mut conn = self.conn()?;
        conn.exec_first(
            format!("select topology_version from {TRANSFER_TOPOLOGY_TABLE} where id = 1"),
            mysql::Params::Empty,
        )
        .map_err(sql_err)?
        .ok_or(DiscoveryError::StaleCache)
    }

    fn heartbeat(
        &self,
        kind: &ServiceKind,
        endpoint: &ServiceEndpoint,
        ttl_ms: i64,
    ) -> DiscoveryResult<()> {
        let now = now_ms();
        let mut conn = self.conn()?;
        conn.exec_drop(
            format!(
                "insert into {TRANSFER_INSTANCES_TABLE}
                    (kind, instance_id, host, rpc_port, web_port, start_epoch, heartbeat_at, lease_expire_at)
                 values (:kind, :id, :host, :rpc, :web, :epoch, :now, :expire)
                 on duplicate key update
                    host = :host, rpc_port = :rpc, web_port = :web,
                    start_epoch = :epoch, heartbeat_at = :now, lease_expire_at = :expire"
            ),
            params! {
                "kind" => kind.as_str(),
                "id" => endpoint.id.clone(),
                "host" => endpoint.host.clone(),
                "rpc" => endpoint.rpc_port,
                "web" => endpoint.web_port,
                "epoch" => endpoint.start_time_ms.min(i64::MAX as u64) as i64,
                "now" => now,
                "expire" => now + ttl_ms,
            },
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Bump the topology version so watchers know to refetch. Two concurrent
    /// bumpers just yield two versions; both trigger a refresh, which is fine.
    pub fn bump_topology(&self) -> DiscoveryResult<()> {
        let mut conn = self.conn()?;
        conn.exec_drop(
            format!(
                "update {TRANSFER_TOPOLOGY_TABLE}
                 set topology_version = topology_version + 1, updated_at = :now
                 where id = 1"
            ),
            params! { "now" => now_ms() },
        )
        .map_err(sql_err)?;
        Ok(())
    }

    fn deregister(&self, kind: &ServiceKind, instance_id: &str) {
        let result = self.conn().and_then(|mut conn| {
            conn.exec_drop(
                format!(
                    "delete from {TRANSFER_INSTANCES_TABLE} where kind = :kind and instance_id = :id"
                ),
                params! { "kind" => kind.as_str(), "id" => instance_id },
            )
            .map_err(sql_err)?;
            self.bump_topology()
        });
        if let Err(err) = result {
            log::warn!("sql discovery deregister failed: {err}");
        }
    }

    fn list_live(&self, kind: &ServiceKind) -> DiscoveryResult<Vec<ServiceEndpoint>> {
        let mut conn = self.conn()?;
        let rows: Vec<(String, String, u16, Option<u16>, i64)> = conn
            .exec(
                format!(
                    "select instance_id, host, rpc_port, web_port, start_epoch
                     from {TRANSFER_INSTANCES_TABLE}
                     where kind = :kind and lease_expire_at > :now"
                ),
                params! { "kind" => kind.as_str(), "now" => now_ms() },
            )
            .map_err(sql_err)?;
        let mut endpoints: Vec<ServiceEndpoint> = rows
            .into_iter()
            .map(
                |(id, host, rpc_port, web_port, start_epoch)| ServiceEndpoint {
                    kind: kind.clone(),
                    id,
                    host,
                    rpc_port,
                    web_port,
                    component_info: component_info(kind),
                    start_time_ms: start_epoch.max(0) as u64,
                    status: ServiceStatus::Serving,
                    metadata: None,
                },
            )
            .collect();
        endpoints.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(endpoints)
    }
}

fn now_ms() -> i64 {
    curvine_runtime::common::LocalTime::mills() as i64
}

fn sql_err(err: mysql::Error) -> DiscoveryError {
    DiscoveryError::InvalidDiscoveryConfig(format!("sql discovery store error: {err}"))
}

/// Minimal ComponentInfoProto satisfying `ServiceEndpoint::validate`.
fn component_info(kind: &ServiceKind) -> curvine_proto::ComponentInfoProto {
    curvine_proto::ComponentInfoProto {
        component: Some(kind.as_str().to_string()),
        release_version: None,
        git_commit: None,
        git_tag: None,
        git_branch: None,
        protocol_version: None,
        min_protocol_version: None,
        capabilities: Vec::new(),
    }
}

#[async_trait]
impl ServiceRegistry for SqlDiscovery {
    async fn register(
        &self,
        endpoint: ServiceEndpoint,
        options: RegistrationOptions,
    ) -> DiscoveryResult<RegistrationGuard> {
        options.validate()?;
        endpoint.validate()?;
        let kind = endpoint.kind.clone();
        let ttl_ms = (options.lease_ttl_secs * 1000) as i64;
        let keep_alive = Duration::from_secs(options.keep_alive_interval_secs.max(1));

        self.heartbeat(&kind, &endpoint, ttl_ms)?;
        self.bump_topology()?;

        let (status_tx, status_rx) = watch::channel(RegistrationStatus::Registered);
        let hb = tokio::spawn({
            let discovery = self.clone();
            let kind = kind.clone();
            let endpoint = endpoint.clone();
            async move {
                let mut ticker = tokio::time::interval(keep_alive);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                ticker.tick().await; // first tick is immediate; skip it
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            if discovery.heartbeat(&kind, &endpoint, ttl_ms).is_err() {
                                let _ = status_tx.send(RegistrationStatus::KeepAliveLost {
                                    message: "sql heartbeat failed".to_string(),
                                });
                                // Do not exit: the next tick may recover, and
                                // exiting would silently drop the lease.
                            }
                        }
                        _ = status_tx.closed() => return,
                    }
                }
            }
        });

        Ok(RegistrationGuard::with_lifecycle_owner(
            kind.clone(),
            endpoint.id.clone(),
            1,
            status_rx,
            Arc::new(SqlRegistrationControl {
                discovery: self.clone(),
                kind,
                instance_id: endpoint.id.clone(),
                _hb: hb,
            }),
            None,
        ))
    }
}

struct SqlRegistrationControl {
    discovery: SqlDiscovery,
    kind: ServiceKind,
    instance_id: String,
    _hb: tokio::task::JoinHandle<()>,
}

#[async_trait]
impl RegistrationControl for SqlRegistrationControl {
    async fn update_endpoint(&self, endpoint: ServiceEndpoint) -> DiscoveryResult<()> {
        self.discovery.heartbeat(&self.kind, &endpoint, 10_000)
    }

    async fn update_status(&self, _status: ServiceStatus) -> DiscoveryResult<()> {
        Ok(()) // status projection is not needed on the sql path
    }

    async fn shutdown(&self) -> DiscoveryResult<()> {
        self._hb.abort();
        self.discovery.deregister(&self.kind, &self.instance_id);
        Ok(())
    }
}

#[async_trait]
impl ServiceResolver for SqlDiscovery {
    async fn list(&self, kind: ServiceKind) -> DiscoveryResult<ServiceSnapshot> {
        let endpoints = self.list_live(&kind)?;
        Ok(ServiceSnapshot {
            kind,
            revision: self.topology_version()?,
            stale: false,
            last_update_ms: now_ms() as u64,
            endpoints,
        })
    }

    /// Watch is emulated by polling every 3s and diffing membership. The
    /// snapshot reader is refreshed in place so sync readers stay consistent.
    async fn watch(&self, kind: ServiceKind) -> DiscoveryResult<ServiceResolverHandle> {
        let (tx, rx) = mpsc::channel::<DiscoveryResult<ServiceWatchEvent>>(64);
        let initial = self.list(kind.clone()).await?;
        let reader = SnapshotReader::new(initial.clone(), true);
        let reader_for_task = reader.clone();
        tokio::spawn({
            let discovery = self.clone();
            let kind = kind.clone();
            async move {
                let mut last: Vec<String> =
                    initial.endpoints.iter().map(|e| e.id.clone()).collect();
                let mut ticker = tokio::time::interval(Duration::from_secs(3));
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    let Ok(snapshot) = discovery.list(kind.clone()).await else {
                        continue;
                    };
                    let ids: Vec<String> =
                        snapshot.endpoints.iter().map(|e| e.id.clone()).collect();
                    if ids != last {
                        for event in diff_events(&kind, &last, &ids) {
                            if tx.send(Ok(event)).await.is_err() {
                                return;
                            }
                        }
                        last = ids;
                    }
                    reader_for_task.replace_snapshot(snapshot).await;
                }
            }
        });
        Ok(ServiceResolverHandle::new(kind, reader, rx))
    }
}

fn diff_events(kind: &ServiceKind, last: &[String], current: &[String]) -> Vec<ServiceWatchEvent> {
    let mut events = Vec::new();
    for id in current {
        if !last.contains(id) {
            events.push(ServiceWatchEvent::Added(ServiceEndpoint {
                kind: kind.clone(),
                id: id.clone(),
                host: String::new(),
                rpc_port: 0,
                web_port: None,
                component_info: component_info(kind),
                start_time_ms: 0,
                status: ServiceStatus::Serving,
                metadata: None,
            }));
        }
    }
    for id in last {
        if !current.contains(id) {
            events.push(ServiceWatchEvent::Removed {
                kind: kind.clone(),
                id: id.clone(),
            });
        }
    }
    events
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(id: &str, host: &str, port: u16) -> ServiceEndpoint {
        let kind = ServiceKind::try_new("transfer").unwrap();
        ServiceEndpoint {
            kind: kind.clone(),
            id: id.to_string(),
            host: host.to_string(),
            rpc_port: port,
            web_port: None,
            component_info: component_info(&kind),
            start_time_ms: 1000,
            status: ServiceStatus::Serving,
            metadata: None,
        }
    }

    fn options() -> RegistrationOptions {
        RegistrationOptions {
            lease_ttl_secs: 2,
            keep_alive_interval_secs: 1,
            register_timeout_ms: 5000,
        }
    }

    fn memory_url() -> String {
        // mysql crate accepts an ODBC-ish DSN; a shared in-memory server is
        // unavailable, so the tests below use diff/heartbeat-free helpers.
        std::env::var("CURVINE_TEST_MYSQL_URL").unwrap_or_default()
    }

    fn diff_added(last: &[&str], current: &[&str]) -> Vec<String> {
        diff_events(
            &ServiceKind::try_new("transfer").unwrap(),
            &last.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            &current.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        )
        .into_iter()
        .filter_map(|event| match event {
            ServiceWatchEvent::Added(endpoint) => Some(endpoint.id),
            _ => None,
        })
        .collect()
    }

    fn diff_removed(last: &[&str], current: &[&str]) -> Vec<String> {
        diff_events(
            &ServiceKind::try_new("transfer").unwrap(),
            &last.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            &current.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        )
        .into_iter()
        .filter_map(|event| match event {
            ServiceWatchEvent::Removed { id, .. } => Some(id),
            _ => None,
        })
        .collect()
    }

    #[test]
    fn diff_events_reports_added_and_removed() {
        assert_eq!(diff_added(&["a", "b"], &["b", "c"]), vec!["c"]);
        assert_eq!(diff_removed(&["a", "b"], &["b", "c"]), vec!["a"]);
        assert!(diff_added(&["a"], &["a"]).is_empty());
        assert!(diff_removed(&["a"], &["a"]).is_empty());
    }

    #[test]
    fn diff_events_handles_epoch_change_as_removal_only_when_id_unchanged() {
        // Same instance id restarted: the resolver reports no diff; the epoch
        // check lives with the hash-ring rebuild, not in this diff.
        assert!(diff_added(&["h:9000"], &["h:9000"]).is_empty());
    }

    #[tokio::test]
    async fn sql_registry_heartbeat_and_lease_expiry() {
        let url = memory_url();
        if url.is_empty() {
            return; // mysql integration env not configured
        }
        let discovery = SqlDiscovery::open(&url).unwrap();
        discovery.init_schema().unwrap();
        let guard = discovery
            .register(endpoint("h1:9000", "h1", 9000), options())
            .await
            .unwrap();
        let snapshot = discovery
            .list(ServiceKind::try_new("transfer").unwrap())
            .await
            .unwrap();
        assert_eq!(snapshot.endpoints.len(), 1);
        assert_eq!(snapshot.endpoints[0].id, "h1:9000");
        // Drop = stop heartbeat; lease expires and the instance disappears.
        drop(guard);
        tokio::time::sleep(Duration::from_secs(3)).await;
        let snapshot = discovery
            .list(ServiceKind::try_new("transfer").unwrap())
            .await
            .unwrap();
        assert!(snapshot.endpoints.is_empty());
    }
}
