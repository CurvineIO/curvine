// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0.
// License text: http://www.apache.org/licenses/LICENSE-2.0

//! Where the router learns the live transfer instances from.
//!
//! `MySqlTopologySource` opens one short connection per fetch (workers carry
//! no MySQL pool) and reads the instance registry plus topology version.

use curvine_error::{FsError, FsResult};
use mysql::prelude::Queryable;
use mysql::{params, Pool};

/// (instance_id, rpc_addr, start_epoch) list plus the topology version.
pub type TopologySnapshot = (Vec<(String, String, u64)>, i64);

/// Where the router learns live instances from.
#[async_trait::async_trait]
pub trait TopologySource: Send + Sync {
    async fn fetch(&self) -> FsResult<TopologySnapshot>;
}

pub struct MySqlTopologySource {
    url: String,
}

impl MySqlTopologySource {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

#[async_trait::async_trait]
impl TopologySource for MySqlTopologySource {
    async fn fetch(&self) -> FsResult<TopologySnapshot> {
        let url = self.url.clone();
        let rows = tokio::task::spawn_blocking(move || fetch_sync(&url))
            .await
            .map_err(|err| FsError::common(format!("topology fetch join error: {err}")))??;
        Ok(rows)
    }
}

fn fetch_sync(url: &str) -> FsResult<TopologySnapshot> {
    let pool = Pool::new(url)
        .map_err(|err| FsError::common(format!("topology mysql pool error: {err}")))?;
    let mut conn = pool
        .get_conn()
        .map_err(|err| FsError::common(format!("topology mysql conn error: {err}")))?;

    let now = curvine_runtime::common::LocalTime::mills() as i64;
    let rows: Vec<(String, String, u16, i64)> = conn
        .exec(
            "select instance_id, host, rpc_port, start_epoch
             from transfer_instances
             where kind = 'transfer' and lease_expire_at > :now",
            params! { "now" => now },
        )
        .map_err(|err| FsError::common(format!("topology query error: {err}")))?;
    let version: i64 = conn
        .exec_first(
            "select topology_version from transfer_topology where id = 1",
            mysql::Params::Empty,
        )
        .map_err(|err| FsError::common(format!("topology version error: {err}")))?
        .unwrap_or(0);

    let endpoints = rows
        .into_iter()
        .map(|(id, host, port, epoch)| (id, format!("{host}:{port}"), epoch.max(0) as u64))
        .collect();
    Ok((endpoints, version))
}
