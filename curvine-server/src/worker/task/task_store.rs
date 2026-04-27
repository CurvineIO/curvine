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

use std::ops::Deref;
use std::sync::Arc;

use curvine_common::state::LoadTaskInfo;
use orpc::sync::FastDashMap;

use crate::worker::task::TaskContext;

#[derive(Clone)]
pub struct TaskStore {
    tasks: Arc<FastDashMap<String, Arc<TaskContext>>>,
}

impl Default for TaskStore {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskStore {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(FastDashMap::default()),
        }
    }

    pub fn insert(&self, task: LoadTaskInfo) -> Arc<TaskContext> {
        let context = Arc::new(TaskContext::new(task));
        self.tasks
            .insert(context.info.task_id.clone(), context.clone());
        context
    }

    pub fn contains(&self, task_id: impl AsRef<str>) -> bool {
        self.tasks.contains_key(task_id.as_ref())
    }

    pub fn get_all_tasks(&self, job_id: impl AsRef<str>) -> Vec<Arc<TaskContext>> {
        self.tasks
            .iter()
            .filter(|x| x.info.job.job_id == job_id.as_ref())
            .map(|x| x.clone())
            .collect()
    }

    pub fn cancel(&self, job_id: impl AsRef<str>) -> Vec<Arc<TaskContext>> {
        let all_tasks = self.get_all_tasks(job_id);
        for context in all_tasks.iter() {
            let _ = self.tasks.remove(&context.info.task_id);
        }

        all_tasks
    }

    pub fn remove(&self, task_id: impl AsRef<str>) -> Option<Arc<TaskContext>> {
        self.tasks.remove(task_id.as_ref()).map(|x| x.1)
    }
}

impl Deref for TaskStore {
    type Target = FastDashMap<String, Arc<TaskContext>>;

    fn deref(&self) -> &Self::Target {
        &self.tasks
    }
}

#[cfg(test)]
mod supersede_tests {
    use super::TaskStore;
    use crate::worker::task::TaskContext;
    use curvine_common::state::WorkerAddress;
    use curvine_common::state::{
        JobTaskState, LoadJobInfo, LoadTaskInfo, MountInfo, StorageType, TtlAction,
    };
    use dashmap::mapref::entry::Entry;
    use std::sync::Arc;

    fn load_task(task_id: &str, job_id: &str) -> LoadTaskInfo {
        let job = LoadJobInfo {
            job_id: job_id.to_string(),
            source_path: "s".to_string(),
            target_path: "t".to_string(),
            block_size: 4096,
            replicas: 1,
            storage_type: StorageType::default(),
            ttl_ms: 0,
            ttl_action: TtlAction::default(),
            mount_info: MountInfo::default(),
            create_time: 0,
            overwrite: None,
        };
        LoadTaskInfo {
            job,
            task_id: task_id.to_string(),
            worker: WorkerAddress::default(),
            source_path: "s".to_string(),
            target_path: "t".to_string(),
            create_time: 0,
        }
    }

    /// Mirrors `TaskManager::submit_task`: supersede replaces the map entry; only
    /// `remove_if` matching the **current** `Arc` may delete (guards spawn epilogue).
    #[test]
    fn supersede_marks_old_canceled_remove_if_only_evicts_matching_runner() {
        let store = TaskStore::new();
        let tid = "same_task_id".to_string();

        let first = Arc::new(TaskContext::new(load_task(&tid, "job_a")));
        match store.entry(tid.clone()) {
            Entry::Vacant(v) => {
                v.insert(first.clone());
            }
            Entry::Occupied(_) => panic!("expected vacant"),
        }

        let second = Arc::new(TaskContext::new(load_task(&tid, "job_a")));
        match store.entry(tid.clone()) {
            Entry::Occupied(mut occ) => {
                let old = occ.insert(second.clone());
                assert!(Arc::ptr_eq(&old, &first));
                old.update_state(JobTaskState::Canceled, "superseded by new submit");
            }
            Entry::Vacant(_) => panic!("expected occupied"),
        }

        assert_eq!(first.get_state(), JobTaskState::Canceled);
        // Do not hold a `get()` Ref across `remove_if` on the same key (deadlocks: read vs write on one shard).
        assert!(Arc::ptr_eq(&*store.get(&tid).expect("slot"), &second));

        // Stale runner would call `remove_if` with `first`; must not clear the slot.
        assert!(store
            .remove_if(&tid, |_, ctx| Arc::ptr_eq(ctx, &first))
            .is_none());
        assert!(store.get(&tid).is_some());

        // Current runner removes with `second`.
        assert!(store
            .remove_if(&tid, |_, ctx| Arc::ptr_eq(ctx, &second))
            .is_some());
        assert!(store.get(&tid).is_none());
    }
}
