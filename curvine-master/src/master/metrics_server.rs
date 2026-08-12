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
use crate::master::Master;

#[derive(Clone)]
struct MasterMetricsHandler {
    fs: MasterFilesystem,
}

async fn metrics(Extension(handler): Extension<Arc<MasterMetricsHandler>>) -> FsResult<String> {
    let metrics = Master::get_metrics()?;
    Ok(metrics.text_output(handler.fs.clone())?)
}

fn metrics_router(handler: MasterMetricsHandler) -> Router {
    Router::new()
        .route("/metrics", get(metrics))
        .layer(Extension(Arc::new(handler)))
}

/// Minimal HTTP server exposing only Prometheus `/metrics` for the master process.
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
