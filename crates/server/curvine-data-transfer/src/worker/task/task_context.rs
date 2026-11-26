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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

use curvine_error::{FsError, FsResult};
use curvine_model::{TransferTaskInfo, TransferTaskResult, WorkerTaskProgress, WorkerTaskState};
use curvine_runtime::common::LocalTime;
use curvine_runtime::sync::StateCtl;
use tokio::sync::OwnedSemaphorePermit;

pub struct TaskContext {
    pub info: TransferTaskInfo,
    state: StateCtl,
    cancel_requested: AtomicBool,
    progress: Mutex<TaskProgress>,
    // Probe-only results remain charged until the last owner releases this context.
    probe_retention: parking_lot::Mutex<Option<OwnedSemaphorePermit>>,
}

#[derive(Default)]
struct TaskProgress {
    progress: WorkerTaskProgress,
    result: Option<TransferTaskResult>,
}

impl TaskContext {
    pub fn new(info: TransferTaskInfo) -> Self {
        Self {
            info,
            state: StateCtl::new(WorkerTaskState::Pending.into()),
            cancel_requested: AtomicBool::new(false),
            progress: Mutex::new(TaskProgress::default()),
            probe_retention: parking_lot::Mutex::new(None),
        }
    }

    pub(super) fn retain_for_probe(&self, permit: OwnedSemaphorePermit) {
        *self.probe_retention.lock() = Some(permit);
    }

    pub fn get_state(&self) -> WorkerTaskState {
        self.state.state()
    }

    fn progress_lock(&self) -> MutexGuard<'_, TaskProgress> {
        match self.progress.lock() {
            Ok(progress) => progress,
            Err(e) => {
                log::error!("fatal task progress lock poisoned: {}", e);
                std::process::abort();
            }
        }
    }

    // Every mutator holds progress_lock. A terminal transition freezes all fields
    // before probes or report producers can observe it, including timestamp/message.
    pub fn progress(&self) -> WorkerTaskProgress {
        self.progress_snapshot(&self.progress_lock().progress)
    }

    pub(crate) fn progress_with_result(&self) -> (WorkerTaskProgress, Option<TransferTaskResult>) {
        let lock = self.progress_lock();
        (self.progress_snapshot(&lock.progress), lock.result.clone())
    }

    fn progress_snapshot(&self, progress: &WorkerTaskProgress) -> WorkerTaskProgress {
        WorkerTaskProgress {
            state: self.get_state(),
            ..progress.clone()
        }
    }

    /// Keep a result for this attempt before its final report is sent, so a
    /// failed final report can still be answered by a worker query. A result
    /// is only accepted for progress that is still current for this attempt.
    pub(crate) fn publish_result(
        &self,
        progress: &WorkerTaskProgress,
        result: TransferTaskResult,
    ) -> FsResult<()> {
        let mut lock = self.progress_lock();
        let current = self.get_state();
        if (self.is_cancel() && progress.state != WorkerTaskState::Canceled)
            || (current.is_finish() && current != progress.state)
            || progress.update_time < lock.progress.update_time
        {
            return Err(FsError::common(
                "cannot publish a result for stale task progress",
            ));
        }
        self.state.set_state(progress.state);
        lock.progress = progress.clone();
        lock.result = Some(result);
        Ok(())
    }

    pub fn set_failed(&self, message: impl Into<String>) -> WorkerTaskProgress {
        let mut lock = self.progress_lock();
        if self.get_state().is_finish() {
            return self.progress_snapshot(&lock.progress);
        }
        self.state.set_state(WorkerTaskState::Failed);
        lock.result = None;
        lock.progress.message = message.into();
        lock.progress.update_time = LocalTime::mills() as i64;

        self.progress_snapshot(&lock.progress)
    }

    pub fn set_canceled(&self, message: impl Into<String>) -> WorkerTaskProgress {
        let mut lock = self.progress_lock();
        if self.get_state().is_finish() {
            return self.progress_snapshot(&lock.progress);
        }
        lock.result = None;
        self.cancel_requested.store(true, Ordering::Release);
        self.state.set_state(WorkerTaskState::Canceled);
        lock.progress.message = message.into();
        lock.progress.update_time = LocalTime::mills() as i64;

        self.progress_snapshot(&lock.progress)
    }

    pub fn request_cancel(&self, message: impl Into<String>) {
        let mut lock = self.progress_lock();
        if self.get_state().is_finish() {
            return;
        }
        self.cancel_requested.store(true, Ordering::Release);
        lock.progress.message = message.into();
        lock.progress.update_time = LocalTime::mills() as i64;
    }

    pub fn is_submit(&self) -> bool {
        self.get_state() <= WorkerTaskState::Loading
    }

    pub fn is_cancel(&self) -> bool {
        self.cancel_requested.load(Ordering::Acquire)
            || self.get_state() == WorkerTaskState::Canceled
    }

    pub fn update_state(&self, state: WorkerTaskState, message: impl Into<String>) {
        let mut lock = self.progress_lock();
        if self.get_state().is_finish() || (self.is_cancel() && state != WorkerTaskState::Canceled)
        {
            return;
        }
        if self.get_state() != state && state != WorkerTaskState::Completed {
            lock.result = None;
        }
        self.state.set_state(state);
        lock.progress.message = message.into();
        lock.progress.update_time = LocalTime::mills() as i64;
    }

    pub fn update_progress(
        &self,
        loaded_size: i64,
        total_size: i64,
        is_last: bool,
    ) -> WorkerTaskProgress {
        let mut lock = self.progress_lock();
        if self.get_state().is_finish() || self.is_cancel() {
            return self.progress_snapshot(&lock.progress);
        }

        lock.progress.loaded_size = loaded_size;
        lock.progress.total_size = total_size;
        lock.progress.update_time = LocalTime::mills() as i64;

        if is_last {
            lock.progress.message = "task completed successfully".into();
            self.state.set_state(WorkerTaskState::Completed);
        }

        self.progress_snapshot(&lock.progress)
    }
}
