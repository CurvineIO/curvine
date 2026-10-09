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

use std::env;
use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Router};
use curvine_core_error::{err_box, CommonResult};
use curvine_error::FsResult;
use curvine_net::net::{InetAddr, NetUtils};
use curvine_rpc::server::ServerConf;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use log::{error, info};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::master::fs::MasterFilesystem;
use crate::master::{Master, MasterState};

#[derive(Clone)]
struct MasterMetricsHandler {
    fs: MasterFilesystem,
}

async fn metrics(Extension(handler): Extension<Arc<MasterMetricsHandler>>) -> FsResult<String> {
    let metrics = Master::get_metrics()?;
    Ok(metrics.text_output(handler.fs.clone())?)
}

async fn healthz(
    Extension(handler): Extension<Arc<MasterMetricsHandler>>,
) -> (StatusCode, &'static str) {
    health_status(handler.fs.master_monitor.journal_state())
}

async fn readyz(
    Extension(handler): Extension<Arc<MasterMetricsHandler>>,
) -> (StatusCode, &'static str) {
    ready_status(handler.fs.master_monitor.journal_state())
}

/// Liveness semantics: once the journal role is known the master is serving, so
/// both the active (leader) and the standby (follower) node report healthy.
fn health_status(state: MasterState) -> (StatusCode, &'static str) {
    match state {
        MasterState::Active | MasterState::Standby => (StatusCode::OK, "ok\n"),
        MasterState::SafeMode | MasterState::Exit => {
            (StatusCode::SERVICE_UNAVAILABLE, "master is not serving\n")
        }
    }
}

/// Readiness semantics: only the active (leader) master can serve metadata, so
/// a standby answers 503 and traffic should be routed to the leader instead.
fn ready_status(state: MasterState) -> (StatusCode, &'static str) {
    match state {
        MasterState::Active => (StatusCode::OK, "ok\n"),
        MasterState::Standby => (StatusCode::SERVICE_UNAVAILABLE, "not ready: standby\n"),
        MasterState::SafeMode | MasterState::Exit => (
            StatusCode::SERVICE_UNAVAILABLE,
            "not ready: master is not serving\n",
        ),
    }
}

fn metrics_router(handler: MasterMetricsHandler) -> Router {
    Router::new()
        .route("/metrics", get(metrics))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .layer(Extension(Arc::new(handler)))
}

/// Minimal HTTP server exposing Prometheus `/metrics` and the `/healthz`
/// (liveness) and `/readyz` (leader-only readiness) probe endpoints.
pub struct MasterMetricsServer {
    rt: Arc<Runtime>,
    fs: MasterFilesystem,
    conf: ServerConf,
    address: InetAddr,
}

impl MasterMetricsServer {
    pub fn with_rt(rt: Arc<Runtime>, conf: ServerConf, fs: MasterFilesystem) -> Self {
        let address = InetAddr::new(&conf.hostname, conf.port);
        Self {
            rt,
            fs,
            conf,
            address,
        }
    }

    pub fn start(self) {
        let rt = self.rt.clone();
        rt.spawn(async move {
            if let Err(err) = self.run().await {
                error!("Master metrics server error: {err}");
            }
        });
    }

    /// Spawns the metrics server and notifies once bind succeeds or fails.
    pub fn start_with_bind_notify(self) -> oneshot::Receiver<CommonResult<()>> {
        let (tx, rx) = oneshot::channel();
        let rt = self.rt.clone();
        rt.spawn(async move {
            match Self::bind_listener(&self).await {
                Ok(listener) => {
                    if tx.send(Ok(())).is_err() {
                        return;
                    }
                    if let Err(err) =
                        Self::serve(listener, self.fs, &self.conf, &self.address).await
                    {
                        error!("Master metrics server serve error: {err}");
                    }
                }
                Err(err) => {
                    let _ = tx.send(Err(err));
                }
            }
        });
        rx
    }

    async fn bind_listener(server: &Self) -> CommonResult<TcpListener> {
        match NetUtils::take_held_listener(server.address.port) {
            Some(std_listener) => {
                std_listener.set_nonblocking(true)?;
                Ok(TcpListener::from_std(std_listener)?)
            }
            None => Ok(TcpListener::bind(server.bind_addr()).await?),
        }
    }

    fn bind_addr(&self) -> String {
        let hostname =
            env::var("ORPC_BIND_HOSTNAME").unwrap_or_else(|_| self.address.hostname.clone());
        format!("{hostname}:{}", self.address.port)
    }

    async fn serve(
        listener: TcpListener,
        fs: MasterFilesystem,
        conf: &ServerConf,
        address: &InetAddr,
    ) -> CommonResult<()> {
        info!(
            "Master metrics server [{}] listening on {}",
            conf.name, address
        );
        let app = metrics_router(MasterMetricsHandler { fs });
        axum::serve(listener, app).await?;
        Ok(())
    }

    async fn run(self) -> CommonResult<()> {
        let listener = Self::bind_listener(&self).await?;
        Self::serve(listener, self.fs, &self.conf, &self.address).await
    }
}

pub async fn wait_for_metrics_bind(
    bind_rx: oneshot::Receiver<CommonResult<()>>,
    port: u16,
) -> CommonResult<()> {
    match timeout(Duration::from_secs(5), bind_rx).await {
        Ok(Ok(Ok(()))) => {
            info!("Master metrics server is listening on port {port}");
            Ok(())
        }
        Ok(Ok(Err(err))) => {
            error!("Master metrics server failed to bind on port {port}: {err}");
            Err(err)
        }
        Ok(Err(_)) => {
            error!("Master metrics server startup channel dropped before bind");
            err_box!("Master metrics server startup channel dropped before bind")
        }
        Err(_) => {
            error!("Master metrics server bind timed out after 5s on port {port}");
            err_box!("Master metrics server bind timed out after 5s on port {port}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::MasterMonitor;
    use curvine_raft::raft::RoleState;
    use curvine_runtime::sync::StateCtl;

    fn health_with_role(role: RoleState) -> (StatusCode, &'static str) {
        let monitor = MasterMonitor::new(StateCtl::new(role.into()), StateCtl::new(0));
        health_status(monitor.journal_state())
    }

    #[test]
    fn healthz_ok_for_active_and_standby() {
        assert_eq!(health_with_role(RoleState::Leader).0, StatusCode::OK);
        assert_eq!(health_with_role(RoleState::Follower).0, StatusCode::OK);
    }

    #[test]
    fn healthz_unavailable_before_role_and_after_exit() {
        let init = health_with_role(RoleState::Init);
        assert_eq!(init.0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(init.1, "master is not serving\n");

        assert_eq!(
            health_with_role(RoleState::Exit).0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    fn ready_with_role(role: RoleState) -> (StatusCode, &'static str) {
        let monitor = MasterMonitor::new(StateCtl::new(role.into()), StateCtl::new(0));
        ready_status(monitor.journal_state())
    }

    #[test]
    fn readyz_ok_only_on_leader() {
        let leader = ready_with_role(RoleState::Leader);
        assert_eq!(leader.0, StatusCode::OK);
        assert_eq!(leader.1, "ok\n");
    }

    #[test]
    fn readyz_unavailable_for_standby_and_exit() {
        let standby = ready_with_role(RoleState::Follower);
        assert_eq!(standby.0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(standby.1, "not ready: standby\n");

        let init = ready_with_role(RoleState::Init);
        assert_eq!(init.0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(init.1, "not ready: master is not serving\n");

        assert_eq!(
            ready_with_role(RoleState::Exit).0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
