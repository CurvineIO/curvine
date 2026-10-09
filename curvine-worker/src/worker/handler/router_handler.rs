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

use std::sync::Arc;

use crate::worker::Worker;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use curvine_error::FsResult;
use curvine_fault::FaultHttpControl;
use curvine_rpc::server::ServerState;
use curvine_runtime::sync::StateCtl;
use curvine_web::router::RouterHandler;
use once_cell::sync::OnceCell;

#[derive(Clone)]
pub struct WorkerRouterHandler {
    pub(crate) fault_http: FaultHttpControl,
    pub(crate) server_state: Arc<OnceCell<StateCtl>>,
}

async fn metrics() -> FsResult<String> {
    let metrics = Worker::get_metrics()?;
    Ok(metrics.text_output()?)
}

async fn healthz(server_state: Arc<OnceCell<StateCtl>>) -> (StatusCode, &'static str) {
    if is_serving(&server_state) {
        (StatusCode::OK, "ok\n")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "worker is not running\n")
    }
}

fn is_serving(server_state: &OnceCell<StateCtl>) -> bool {
    server_state
        .get()
        .is_some_and(|ctl| ServerState::from(ctl.value()) == ServerState::Running)
}

impl RouterHandler for WorkerRouterHandler {
    fn router(&self) -> Router {
        let server_state = self.server_state.clone();
        let router = Router::new()
            .route("/metrics", get(metrics))
            .route("/healthz", get(move || healthz(server_state.clone())));

        self.fault_http.mount(router)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn healthz_ok_only_while_running() {
        let ctl = StateCtl::new(ServerState::Init.into());
        let cell = OnceCell::new();

        assert!(!is_serving(&cell));

        cell.set(ctl.read_only()).ok();
        assert!(!is_serving(&cell));

        ctl.set_state(ServerState::Running);
        assert!(is_serving(&cell));

        ctl.set_state(ServerState::Shutdown);
        assert!(!is_serving(&cell));

        ctl.set_state(ServerState::Stop);
        assert!(!is_serving(&cell));
    }
}
