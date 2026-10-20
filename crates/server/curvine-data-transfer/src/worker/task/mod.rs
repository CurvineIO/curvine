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

//! Worker side data loading module
//! Responsible for loading data from external storage systems (such as S3) to local storage

mod load_task_runner;
pub use self::load_task_runner::LoadTaskRunner;

mod task_progress_reporter;
pub use self::task_progress_reporter::ReportOutcome;

#[cfg(test)]
#[path = "tests/task_progress_reporter_test.rs"]
mod task_progress_reporter_test;

mod task_manager;
pub use self::task_manager::{TaskManager, WORKER_AT_CAPACITY_REJECT_PREFIX};

#[cfg(test)]
#[path = "tests/task_manager_test.rs"]
mod task_manager_test;

mod task_store;
pub use self::task_store::TaskStore;

mod task_context;
pub use self::task_context::TaskContext;

mod task_reaper;
pub use self::task_reaper::{reap_finished_tasks, EvictReason, TerminalRetention};

mod metrics;
pub use self::metrics::{
    record_metric, record_report_outcome, record_task_evicted, record_task_outcome,
    transfer_kind_label, WorkerTransferMetrics,
};
