use std::sync::Mutex;

use curvine_common::state::{LoadTaskInfo, WorkProgress, WorkState};
use orpc::common::LocalTime;
use orpc::sync::StateCtl;

pub struct TaskContext {
    pub info: LoadTaskInfo,
    state: StateCtl,
    progress: Mutex<WorkProgress>,
}

impl TaskContext {
    pub fn new(info: LoadTaskInfo) -> Self {
        Self {
            info,
            state: StateCtl::new(WorkState::Pending.into()),
            progress: Mutex::new(WorkProgress::default()),
        }
    }

    pub fn get_state(&self) -> WorkState {
        self.state.state()
    }

    pub fn set_failed(&self, message: impl Into<String>) -> WorkProgress {
        let mut lock = self.progress.lock().unwrap();
        self.state.set_state(WorkState::Failed);
        lock.message = message.into();
        lock.update_time = LocalTime::mills() as i64;

        WorkProgress {
            state: self.get_state(),
            total_size: lock.total_size,
            loaded_size: lock.loaded_size,
            update_time: lock.update_time,
            message: lock.message.clone(),
        }
    }

    pub fn is_submit(&self) -> bool {
        self.get_state() <= WorkState::Loading
    }

    pub fn is_cancel(&self) -> bool {
        self.get_state() == WorkState::Canceled
    }

    pub fn update_state(&self, state: WorkState, message: impl Into<String>) {
        let mut lock = self.progress.lock().unwrap();
        self.state.set_state(state);
        lock.message = message.into();
        lock.update_time = LocalTime::mills() as i64;
    }

    pub fn update_progress(&self, loaded_size: i64, total_size: i64) -> WorkProgress {
        let mut lock = self.progress.lock().unwrap();

        lock.loaded_size = loaded_size;
        lock.total_size = total_size;
        lock.update_time = LocalTime::mills() as i64;

        if loaded_size >= total_size {
            lock.message = "task completed successfully".into();
            self.state.set_state(WorkState::Completed);
        }

        WorkProgress {
            state: self.get_state(),
            total_size: lock.total_size,
            loaded_size: lock.loaded_size,
            update_time: lock.update_time,
            message: lock.message.clone(),
        }
    }
}
