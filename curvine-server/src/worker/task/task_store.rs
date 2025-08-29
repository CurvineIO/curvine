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
