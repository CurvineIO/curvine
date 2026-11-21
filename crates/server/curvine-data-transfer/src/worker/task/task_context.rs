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

use curvine_model::{TransferTaskInfo, WorkerTaskProgress, WorkerTaskState};
use curvine_runtime::common::LocalTime;
use curvine_runtime::sync::StateCtl;

pub struct TaskContext {
    pub info: TransferTaskInfo,
    state: StateCtl,
    cancel_requested: AtomicBool,
    progress: Mutex<WorkerTaskProgress>,
}

impl TaskContext {
    pub fn new(info: TransferTaskInfo) -> Self {
        Self {
            info,
            state: StateCtl::new(WorkerTaskState::Pending.into()),
            cancel_requested: AtomicBool::new(false),
            progress: Mutex::new(WorkerTaskProgress::default()),
        }
    }

    pub fn get_state(&self) -> WorkerTaskState {
        self.state.state()
    }

    fn progress_lock(&self) -> MutexGuard<'_, WorkerTaskProgress> {
        match self.progress.lock() {
            Ok(progress) => progress,
            Err(e) => {
                log::error!("fatal task progress lock poisoned: {}", e);
                std::process::abort();
            }
        }
    }

    pub fn progress(&self) -> WorkerTaskProgress {
        let lock = self.progress_lock();
        WorkerTaskProgress {
            state: self.get_state(),
            total_size: lock.total_size,
            loaded_size: lock.loaded_size,
            update_time: lock.update_time,
            message: lock.message.clone(),
        }
    }

    pub fn set_failed(&self, message: impl Into<String>) -> WorkerTaskProgress {
        let mut lock = self.progress_lock();
        self.state.set_state(WorkerTaskState::Failed);
        lock.message = message.into();
        lock.update_time = LocalTime::mills() as i64;

        WorkerTaskProgress {
            state: self.get_state(),
            total_size: lock.total_size,
            loaded_size: lock.loaded_size,
            update_time: lock.update_time,
            message: lock.message.clone(),
        }
    }

    pub fn set_canceled(&self, message: impl Into<String>) -> WorkerTaskProgress {
        let mut lock = self.progress_lock();
        self.cancel_requested.store(true, Ordering::Release);
        self.state.set_state(WorkerTaskState::Canceled);
        lock.message = message.into();
        lock.update_time = LocalTime::mills() as i64;

        WorkerTaskProgress {
            state: self.get_state(),
            total_size: lock.total_size,
            loaded_size: lock.loaded_size,
            update_time: lock.update_time,
            message: lock.message.clone(),
        }
    }

    pub fn request_cancel(&self, message: impl Into<String>) {
        let mut lock = self.progress_lock();
        if self.get_state() == WorkerTaskState::Canceled {
            return;
        }
        self.cancel_requested.store(true, Ordering::Release);
        lock.message = message.into();
        lock.update_time = LocalTime::mills() as i64;
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
        if self.is_cancel() && state != WorkerTaskState::Canceled {
            return;
        }
        self.state.set_state(state);
        lock.message = message.into();
        lock.update_time = LocalTime::mills() as i64;
    }

    pub fn update_progress(
        &self,
        loaded_size: i64,
        total_size: i64,
        is_last: bool,
    ) -> WorkerTaskProgress {
        let mut lock = self.progress_lock();
        if self.is_cancel() {
            return WorkerTaskProgress {
                state: self.get_state(),
                total_size: lock.total_size,
                loaded_size: lock.loaded_size,
                update_time: lock.update_time,
                message: lock.message.clone(),
            };
        }

        lock.loaded_size = loaded_size;
        lock.total_size = total_size;
        lock.update_time = LocalTime::mills() as i64;

        if is_last {
            lock.message = "task completed successfully".into();
            self.state.set_state(WorkerTaskState::Completed);
        }

        WorkerTaskProgress {
            state: self.get_state(),
            total_size: lock.total_size,
            loaded_size: lock.loaded_size,
            update_time: lock.update_time,
            message: lock.message.clone(),
        }
    }
}
