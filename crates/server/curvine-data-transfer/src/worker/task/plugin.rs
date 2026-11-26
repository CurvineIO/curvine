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
//!
//! This module intentionally contains no concrete task implementation. A plugin
//! owns the interpretation of its payload and the executor it creates; the
//! worker runtime can depend on these contracts without depending on a
//! particular job type.

use super::TaskContext;
use curvine_error::FsResult;
use curvine_model::{TransferKind, WorkerTaskProgress};
use curvine_transfer_client::TransferClient;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// An owning, spawnable future returned by a job executor.
pub type JobFuture = Pin<Box<dyn Future<Output = FsResult<TaskExecution>> + Send + 'static>>;

/// Frozen notification returned by the data phase for delivery after copy permits are released.
pub struct TaskTerminalReport {
    pub progress: WorkerTaskProgress,
    pub client: Option<TransferClient>,
}

/// Outcome and cleanup decision after an executor has been consumed.
pub struct TaskExecution {
    pub outcome: TaskOutcome,
    /// Retain the context for a scheduler probe if the final report failed.
    pub remove_task: bool,
    pub terminal_report: Option<TaskTerminalReport>,
}

impl From<TaskOutcome> for TaskExecution {
    fn from(outcome: TaskOutcome) -> Self {
        Self {
            outcome,
            remove_task: true,
            terminal_report: None,
        }
    }
}

/// The framework-owned cancellation context for one attempt.
#[derive(Clone)]
pub struct TaskExecCtx {
    task: Arc<TaskContext>,
}

impl TaskExecCtx {
    pub fn new(task: Arc<TaskContext>) -> Self {
        Self { task }
    }

    /// True once this attempt has been canceled or superseded.
    pub fn is_cancelled(&self) -> bool {
        self.task.is_cancel()
    }
}

/// The result of executing a job task.
///
/// Task-specific result data is deliberately not part of this initial contract;
/// result transport will be added with the task-report extension.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskOutcome {
    /// The task completed successfully.
    Completed,
    /// The task failed. `retryable` tells the scheduler whether another
    /// attempt may be useful.
    Failed { retryable: bool, message: String },
    /// The task observed a cancellation request and stopped.
    Canceled,
}

/// The broad resource class used by a plugin.
///
/// Resource limits are not wired into the worker yet. Keeping the declaration
/// in the contract lets the runtime add admission policy without changing each
/// plugin's API later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceClass {
    Network,
    Remote,
    DiskIo,
    Cpu,
}

/// An executor for one decoded task.
///
/// The task payload is intentionally opaque to the framework. A plugin decodes
/// it and captures the resulting task-specific state in its executor.
pub trait JobExecutor: Send {
    /// Execute the task. Poll `ctx.is_cancelled()` before doing work and at
    /// bounded intervals while running; stop and return `TaskOutcome::Canceled`
    /// when cancellation is observed.
    fn run(self: Box<Self>, ctx: TaskExecCtx) -> JobFuture;
}

/// A worker-side implementation for one or more job types.
pub trait JobPlugin: Send + Sync {
    /// The wire-level task kinds handled by this plugin.
    fn task_kinds(&self) -> Vec<TransferKind>;

    /// A stable human-readable name used for diagnostics and metrics.
    fn name(&self) -> &'static str;

    /// The resource class declared by this plugin.
    fn resource_class(&self) -> ResourceClass;

    /// Create an executor for a framework-owned task context.
    fn create_executor(&self, task: Arc<TaskContext>) -> FsResult<Box<dyn JobExecutor>>;
}
