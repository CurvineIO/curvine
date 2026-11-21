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

use curvine_error::FsResult;
use curvine_model::TransferKind;
use std::future::Future;
use std::pin::Pin;

/// A boxed future returned by a job executor.
///
/// The future is allowed to borrow the executor. This keeps the contract
/// compatible with executors that hold state while avoiding a dependency on an
/// async-trait macro in the public interface.
pub type JobFuture<'a> = Pin<Box<dyn Future<Output = FsResult<TaskOutcome>> + Send + 'a>>;

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
    /// Execute the task and return its framework-level outcome.
    fn run(&mut self) -> JobFuture<'_>;
}

/// A worker-side implementation for one job type.
///
/// Implementations are responsible for validating and decoding `payload`.
/// Admission, cancellation, progress reporting and task persistence are kept
/// outside this initial contract and will be connected by later tasks.
pub trait JobPlugin: Send + Sync {
    /// The wire-level task kind handled by this plugin.
    fn task_kind(&self) -> TransferKind;

    /// A stable human-readable name used for diagnostics and metrics.
    fn name(&self) -> &'static str;

    /// The resource class declared by this plugin.
    fn resource_class(&self) -> ResourceClass;

    /// Validate and decode an opaque task payload into an executor.
    fn create_executor(&self, payload: &[u8]) -> FsResult<Box<dyn JobExecutor>>;
}
