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
use std::path::Path;
use std::sync::Arc;

use axum::error_handling::HandleErrorLayer;
use axum::http::StatusCode;
use axum::Json;
use log::{error, info};
use orpc::io::net::{InetAddr, NetUtils};
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::server::{ServerConf, ServerMonitor, ServerStateListener};
use orpc::CommonResult;
use serde_json::json;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::TraceLayer;

use crate::router::{RouterHandler, TestHandler};

const WEBUI_DIR: &str = "webui";

pub trait WebHandlerService {
    type Item: RouterHandler + 'static;
    fn get_handler(&self) -> Self::Item;
}

pub struct WebServer<S> {
    rt: Arc<Runtime>,
    service: S,
    conf: ServerConf,
    address: InetAddr,
    monitor: ServerMonitor,
}

impl<S> WebServer<S>
where
    S: WebHandlerService + Send + Sync + 'static,
    S::Item: RouterHandler + Send + Sync + 'static,
{
    pub fn new(conf: ServerConf, service: S) -> Self {
        let address = InetAddr::new(&conf.hostname, conf.port);
        let rt = Arc::new(conf.create_runtime());
        Self {
            rt,
            service,
            conf,
            address,
            monitor: ServerMonitor::new(),
        }
    }

    pub fn with_rt(rt: Arc<Runtime>, conf: ServerConf, service: S) -> Self {
        let address = InetAddr::new(&conf.hostname, conf.port);
        Self {
            rt,
            service,
            conf,
            address,
            monitor: ServerMonitor::new(),
        }
    }

    pub fn bind_port(&self) -> u16 {
        self.address.port
    }

    pub fn server_name(&self) -> &str {
        &self.conf.name
    }

    pub fn bind_addr(&self) -> &InetAddr {
        &self.address
    }

    pub fn resolve_bind_addr(&self) -> String {
        self.get_bind_addr()
    }

    pub fn block_on_start(&self) {
        self.rt.block_on(async {
            if let Err(e) = self.run().await {
                error!(
                    "WebServer [{}] exited with error on address {}: {}",
                    self.conf.name,
                    self.get_bind_addr(),
                    e
                );
            }
        });
    }

    pub fn start(self) -> ServerStateListener {
        let rt = self.rt.clone();
        let listener = self.monitor.new_listener();
        rt.spawn(async move {
            Self::start0(self).await;
        });
        listener
    }

    pub async fn wait_bind(
        listener: &mut ServerStateListener,
        name: &str,
        bind_addr: &str,
    ) -> CommonResult<()> {
        use orpc::err_box;

        match listener.wait_startup().await {
            Ok(()) => Ok(()),
            Err(_) => err_box!(
                "WebServer [{}] failed to start on address {}",
                name,
                bind_addr
            ),
        }
    }

    async fn start0(server: Self) {
        let bind_addr = server.get_bind_addr();
        let listener = match server.bind_listener().await {
            Ok(listener) => listener,
            Err(e) => {
                error!(
                    "WebServer [{}] failed to bind address {}: {}",
                    server.conf.name, bind_addr, e
                );
                server.monitor.advance_shutdown();
                server.monitor.advance_stop();
                tokio::task::spawn_blocking(move || drop(server)).await.ok();
                return;
            }
        };

        info!(
            "WebServer [{}] start successfully, bind address: {}",
            server.conf.name, bind_addr
        );
        server.monitor.advance_running();

        if let Err(e) = server.serve_listener(listener).await {
            error!(
                "WebServer [{}] exited with error on address {}: {}",
                server.conf.name, bind_addr, e
            );
        }

        server.monitor.advance_shutdown();
        server.monitor.advance_stop();

        // Drop the server (and possibly its dedicated runtime) outside the async context
        // to avoid Tokio panics when WebServer::new() owns the only runtime reference.
        tokio::task::spawn_blocking(move || drop(server)).await.ok();
    }

    fn get_bind_addr(&self) -> String {
        let hostname = env::var("ORPC_BIND_HOSTNAME").unwrap_or(self.address.hostname.to_string());
        format!("{}:{}", hostname, self.address.port)
    }

    async fn bind_listener(&self) -> CommonResult<TcpListener> {
        // Prefer a pre-bound listener from the test port reservation map.
        // This eliminates the TOCTOU race between port discovery and actual bind
        // when parallel test processes (cargo nextest) run simultaneously.
        match NetUtils::take_held_listener(self.address.port) {
            Some(std_listener) => {
                std_listener.set_nonblocking(true)?;
                Ok(TcpListener::from_std(std_listener)?)
            }
            None => Ok(TcpListener::bind(self.get_bind_addr()).await?),
        }
    }

    async fn serve_listener(&self, listener: TcpListener) -> CommonResult<()> {
        let webui_path = Path::new(WEBUI_DIR);
        let serve_dir = ServeDir::new(webui_path)
            .not_found_service(ServeFile::new(webui_path.join("index.html")));
        let app = self
            .service
            .get_handler()
            .router()
            .fallback_service(serve_dir)
            .layer(
                ServiceBuilder::new()
                    .layer(TraceLayer::new_for_http())
                    .layer(HandleErrorLayer::new(|e| async move {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({"message": format!("internal server error: {e}")})),
                        )
                    })),
            );
        axum::serve(listener, app).await?;
        Ok(())
    }

    pub async fn run(&self) -> CommonResult<()> {
        let bind_addr = self.get_bind_addr();
        let listener = self.bind_listener().await?;
        info!(
            "WebServer [{}] start successfully, bind address: {}",
            self.conf.name, bind_addr
        );
        self.monitor.advance_running();
        self.serve_listener(listener).await
    }
}

#[allow(unused)]
struct TestWebService;

impl WebHandlerService for TestWebService {
    type Item = TestHandler;

    fn get_handler(&self) -> Self::Item {
        TestHandler {}
    }
}

#[test]
fn test() {
    use std::thread;
    use std::time::Duration;

    let service = TestWebService {};
    let mut conf = ServerConf::with_hostname("127.0.0.1", 9000);
    conf.name = "test".to_string();
    let web = WebServer::new(conf, service);
    let _listener = web.start();

    thread::sleep(Duration::from_millis(500));
}
