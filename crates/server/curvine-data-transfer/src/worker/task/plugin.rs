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

//! Public contracts for worker-side job plugins.

use crate::worker::task::TaskContext;
use curvine_error::FsResult;
use curvine_model::JobTaskType;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub type JobFuture<'a> = Pin<Box<dyn Future<Output = FsResult<TaskOutcome>> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskOutcome {
    Completed,
    Failed { retryable: bool, message: String },
    Canceled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceClass {
    Network,
    Remote,
    DiskIo,
    Cpu,
}

pub trait JobExecutor: Send {
    fn run(&mut self) -> JobFuture<'_>;

    /// Whether the task context can be removed after `run` returns.
    ///
    /// A runner may retain its context when the final report could not be
    /// delivered and the scheduler must probe it later.
    fn should_remove(&self) -> bool {
        true
    }
}

pub trait JobPlugin: Send + Sync {
    /// Task types served by this implementation. Load and Export intentionally
    /// share one transfer implementation.
    fn task_types(&self) -> Vec<JobTaskType>;

    fn name(&self) -> &'static str;

    fn resource_class(&self) -> ResourceClass;

    /// Create an executor for a framework-owned task context.
    fn create_executor(&self, task: Arc<TaskContext>) -> FsResult<Box<dyn JobExecutor>>;
}
