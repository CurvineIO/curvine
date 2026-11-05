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

use curvine_core_error::err_msg;
use curvine_error::{FsError, FsResult};
use curvine_runtime::sync::{AtomicBool, ErrorMonitor};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::timeout;

pub(crate) struct CallCtx {
    path: String,
    deadline: Option<Duration>,
    err_monitor: Arc<ErrorMonitor<FsError>>,
    side: Side,
    abandoned: AtomicBool,
    task: Option<JoinHandle<()>>,
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
    pub(crate) fn reader(path: String, err_monitor: Arc<ErrorMonitor<FsError>>) -> Self {
        Self {
            path,
            deadline: None,
            err_monitor,
            side: Side::Read,
            abandoned: AtomicBool::new(false),
            task: None,
        }
    }

    pub(crate) fn writer(
        path: String,
        timeout: Duration,
        err_monitor: Arc<ErrorMonitor<FsError>>,
        task: JoinHandle<()>,
    ) -> Self {
        Self {
            path,
            deadline: Some(timeout),
            err_monitor,
            side: Side::Write,
            abandoned: AtomicBool::new(false),
            task: Some(task),
        }
    }

    pub(crate) fn monitor(&self) -> Arc<ErrorMonitor<FsError>> {
        self.err_monitor.clone()
    }

    pub(crate) fn check_error(&self, e: impl Into<FsError>) -> FsError {
        self.err_monitor.take_error().unwrap_or(e.into())
    }

    fn deadline_text(&self) -> String {
        match self.deadline {
            Some(deadline) => format!("{deadline:?}"),
            None => "no deadline".to_string(),
        }
    }

    fn timeout_error(&self, op: &str) -> FsError {
        FsError::common(format!(
            "{} buffer task not responding after {}, path={}, op={}",
            self.side.label(),
            self.deadline_text(),
            self.path,
            op
        ))
    }

    fn abandoned_error(&self, op: &str) -> FsError {
        FsError::common(err_msg!(
            "{} buffer task not responding after {}, task abandoned, path={}, op={}",
            self.side.label(),
            self.deadline_text(),
            self.path,
            op
        ))
    }

    fn abandon(&self) {
        self.abandoned.set(true);
        if let Some(task) = &self.task {
            task.abort();
        }
    }

    pub(crate) async fn call<T, F, E>(&self, op: &str, fun: F) -> FsResult<T>
    where
        F: Future<Output = Result<T, E>>,
        E: Into<FsError>,
    {
        if self.abandoned.get() {
            return Err(self.abandoned_error(op));
        }

        let Some(deadline) = self.deadline else {
            return fun.await.map_err(|e| self.check_error(e));
        };

        match timeout(deadline, fun).await {
            Ok(v) => v.map_err(|e| self.check_error(e)),

            Err(_) => match self.err_monitor.take_error() {
                Some(background) => Err(background),
                None => {
                    self.abandon();
                    Err(self.timeout_error(op))
                }
            },
        }
    }
}
