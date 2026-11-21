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
use curvine_model::{JobTaskProgress, JobTaskState, LoadTaskInfo, TransferTaskResult};
use curvine_runtime::common::LocalTime;
use curvine_runtime::sync::StateCtl;

pub struct TaskContext {
    pub info: LoadTaskInfo,
    state: StateCtl,
    cancel_requested: AtomicBool,
    progress: Mutex<TaskProgress>,
}

#[derive(Default)]
struct TaskProgress {
    progress: JobTaskProgress,
    result: Option<TransferTaskResult>,
}

impl TaskContext {
    pub fn new(info: LoadTaskInfo) -> Self {
        Self {
            info,
            state: StateCtl::new(JobTaskState::Pending.into()),
            cancel_requested: AtomicBool::new(false),
            progress: Mutex::new(TaskProgress::default()),
        }
    }

    pub fn get_state(&self) -> JobTaskState {
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

    pub fn progress(&self) -> JobTaskProgress {
        self.progress_snapshot(&self.progress_lock().progress)
    }

    pub(crate) fn progress_with_result(&self) -> (JobTaskProgress, Option<TransferTaskResult>) {
        let lock = self.progress_lock();
        (self.progress_snapshot(&lock.progress), lock.result.clone())
    }

    fn progress_snapshot(&self, progress: &JobTaskProgress) -> JobTaskProgress {
        JobTaskProgress {
            state: self.get_state(),
            ..progress.clone()
        }
    }

    pub(crate) fn publish_result(
        &self,
        progress: &JobTaskProgress,
        result: TransferTaskResult,
    ) -> FsResult<()> {
        let mut lock = self.progress_lock();
        let current = self.get_state();
        if (self.is_cancel() && progress.state != JobTaskState::Canceled)
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

    pub fn set_failed(&self, message: impl Into<String>) -> JobTaskProgress {
        let mut lock = self.progress_lock();
        self.state.set_state(JobTaskState::Failed);
        lock.result = None;
        let lock = &mut lock.progress;
        lock.message = message.into();
        lock.update_time = LocalTime::mills() as i64;

        JobTaskProgress {
            state: self.get_state(),
            total_size: lock.total_size,
            loaded_size: lock.loaded_size,
            update_time: lock.update_time,
            message: lock.message.clone(),
        }
    }

    pub fn set_canceled(&self, message: impl Into<String>) -> JobTaskProgress {
        let mut lock = self.progress_lock();
        lock.result = None;
        let lock = &mut lock.progress;
        self.cancel_requested.store(true, Ordering::Release);
        self.state.set_state(JobTaskState::Canceled);
        lock.message = message.into();
        lock.update_time = LocalTime::mills() as i64;

        JobTaskProgress {
            state: self.get_state(),
            total_size: lock.total_size,
            loaded_size: lock.loaded_size,
            update_time: lock.update_time,
            message: lock.message.clone(),
        }
    }

    pub fn request_cancel(&self, message: impl Into<String>) {
        let lock = &mut self.progress_lock().progress;
        if self.get_state() == JobTaskState::Canceled {
            return;
        }
        self.cancel_requested.store(true, Ordering::Release);
        lock.message = message.into();
        lock.update_time = LocalTime::mills() as i64;
    }

    pub fn is_submit(&self) -> bool {
        self.get_state() <= JobTaskState::Loading
    }

    pub fn is_cancel(&self) -> bool {
        self.cancel_requested.load(Ordering::Acquire) || self.get_state() == JobTaskState::Canceled
    }

    pub fn update_state(&self, state: JobTaskState, message: impl Into<String>) {
        let mut lock = self.progress_lock();
        if self.is_cancel() && state != JobTaskState::Canceled {
            return;
        }
        if self.get_state() != state {
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
    ) -> JobTaskProgress {
        let mut lock = self.progress_lock();
        if self.is_cancel() {
            return self.progress_snapshot(&lock.progress);
        }

        lock.progress.loaded_size = loaded_size;
        lock.progress.total_size = total_size;
        lock.progress.update_time = LocalTime::mills() as i64;

        if is_last {
            if self.get_state() != JobTaskState::Completed {
                lock.result = None;
            }
            lock.progress.message = "task completed successfully".into();
            self.state.set_state(JobTaskState::Completed);
        }

        self.progress_snapshot(&lock.progress)
    }
}
