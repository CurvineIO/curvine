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
use orpc::io::net::InetAddr;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::server::ServerConf;
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
        }
    }

    pub fn with_rt(rt: Arc<Runtime>, conf: ServerConf, service: S) -> Self {
        let address = InetAddr::new(&conf.hostname, conf.port);
        Self {
            rt,
            service,
            conf,
            address,
        }
    }

    pub fn rt(&self) -> Arc<Runtime> {
        self.rt.clone()
    }

    pub fn block_on_start(&self) {
        self.rt.block_on(async {
            if let Err(e) = self.run().await {
                error!("WebServer connect error: {}", e);
            }
        });
    }

    pub fn start(self) {
        let rt = self.rt.clone();
        rt.spawn(async move {
            if let Err(e) = self.run().await {
                error!("WebServer connect error: {}", e);
            }
        });
    }

    fn get_bind_addr(&self) -> String {
        let hostname = env::var("ORPC_BIND_HOSTNAME").unwrap_or(self.address.hostname.to_string());
        format!("{}:{}", hostname, self.address.port)
    }

    pub async fn run(&self) -> CommonResult<()> {
        let listener = TcpListener::bind(self.get_bind_addr()).await?;
        info!(
            "WebServer [{}] start successfully, bind address: {}",
            self.conf.name, self.address,
        );
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

    // Start server in background instead of blocking
    web.start();

    // Wait a short time for server to start
    thread::sleep(Duration::from_millis(500));

    // Test completes - server continues running in background but test doesn't block
    // The server will be cleaned up when the runtime shuts down
}
