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
use curvine_runtime::sync::ErrorMonitor;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

pub(crate) struct CallCtx {
    path: String,
    timeout: Duration,
    err_monitor: Arc<ErrorMonitor<FsError>>,
    side: Side,
}

#[derive(Clone, Copy)]
enum Side {
    Read,
    Write,
}

impl Side {
    fn label(self) -> &'static str {
        match self {
            Side::Read => "read",
            Side::Write => "write",
        }
    }
}

impl CallCtx {
    pub(crate) fn reader(
        path: String,
        timeout: Duration,
        err_monitor: Arc<ErrorMonitor<FsError>>,
    ) -> Self {
        Self {
            path,
            timeout,
            err_monitor,
            side: Side::Read,
        }
    }

    pub(crate) fn writer(
        path: String,
        timeout: Duration,
        err_monitor: Arc<ErrorMonitor<FsError>>,
    ) -> Self {
        Self {
            path,
            timeout,
            err_monitor,
            side: Side::Write,
        }
    }

    pub(crate) fn monitor(&self) -> Arc<ErrorMonitor<FsError>> {
        self.err_monitor.clone()
    }

    pub(crate) fn check_error(&self, e: impl Into<FsError>) -> FsError {
        self.err_monitor.take_error().unwrap_or(e.into())
    }

    fn timeout_error(&self, op: &str) -> FsError {
        FsError::common(format!(
            "{} buffer task not responding after {:?}, path={}, op={}",
            self.side.label(),
            self.timeout,
            self.path,
            op
        ))
    }

    pub(crate) async fn call<T, F, E>(&self, op: &str, fun: F) -> FsResult<T>
    where
        F: Future<Output = Result<T, E>>,
        E: Into<FsError>,
    {
        match timeout(self.timeout, fun).await {
            Ok(v) => v.map_err(|e| self.check_error(e)),
            Err(_) => Err(self.timeout_error(op)),
        }
    }
}
