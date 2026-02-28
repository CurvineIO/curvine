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

use std::sync::Mutex;

use curvine_common::state::{JobTaskProgress, JobTaskState, LoadTaskInfo};
use orpc::common::LocalTime;
use orpc::sync::StateCtl;

pub struct TaskContext {
    pub info: LoadTaskInfo,
    state: StateCtl,
    progress: Mutex<JobTaskProgress>,
}

impl TaskContext {
    pub fn new(info: LoadTaskInfo) -> Self {
        Self {
            info,
            state: StateCtl::new(JobTaskState::Pending.into()),
            progress: Mutex::new(JobTaskProgress::default()),
        }
    }

    pub fn get_state(&self) -> JobTaskState {
        self.state.state()
    }

    pub fn set_failed(&self, message: impl Into<String>) -> JobTaskProgress {
        let mut lock = self.progress.lock().unwrap();
        self.state.set_state(JobTaskState::Failed);
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

    pub fn is_submit(&self) -> bool {
        self.get_state() <= JobTaskState::Loading
    }

    pub fn is_cancel(&self) -> bool {
        self.get_state() == JobTaskState::Canceled
    }

    pub fn update_state(&self, state: JobTaskState, message: impl Into<String>) {
        let mut lock = self.progress.lock().unwrap();
        self.state.set_state(state);
        lock.message = message.into();
        lock.update_time = LocalTime::mills() as i64;
    }

    pub fn update_progress(&self, loaded_size: i64, total_size: i64, is_last: bool) -> JobTaskProgress {
        let mut lock = self.progress.lock().unwrap();

        lock.loaded_size = loaded_size;
        lock.total_size = total_size;
        lock.update_time = LocalTime::mills() as i64;

        if is_last {
            lock.message = "task completed successfully".into();
            self.state.set_state(JobTaskState::Completed);
        }

        JobTaskProgress {
            state: self.get_state(),
            total_size: lock.total_size,
            loaded_size: lock.loaded_size,
            update_time: lock.update_time,
            message: lock.message.clone(),
        }
    }
}
