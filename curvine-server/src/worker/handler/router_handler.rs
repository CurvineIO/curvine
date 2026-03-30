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

use axum::routing::get;
use axum::Router;

use curvine_web::router::RouterHandler;
#[cfg(feature = "heap-trace")]
use orpc::common::heap_trace::HeapTraceRuntime;

use crate::worker::Worker;

#[derive(Clone, Default)]
pub struct WorkerRouterHandler {
    #[cfg(feature = "heap-trace")]
    heap_trace: Option<Arc<HeapTraceRuntime>>,
}

impl WorkerRouterHandler {
    pub fn new(#[cfg(feature = "heap-trace")] heap_trace: Option<Arc<HeapTraceRuntime>>) -> Self {
        Self {
            #[cfg(feature = "heap-trace")]
            heap_trace,
        }
    }
}

async fn metrics() -> String {
    let metrics = Worker::get_metrics();
    metrics.text_output().unwrap()
}

impl RouterHandler for WorkerRouterHandler {
    fn router(&self) -> Router {
        let router = Router::new().route("/metrics", get(metrics));

        #[cfg(feature = "heap-trace")]
        let router = if let Some(runtime) = &self.heap_trace {
            router.merge(orpc::common::heap_trace::router(runtime.as_ref().clone()))
        } else {
            router
        };

        router
    }
}
