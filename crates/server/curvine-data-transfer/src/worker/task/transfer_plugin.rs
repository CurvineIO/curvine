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

use super::{
    JobExecutor, JobFuture, JobPlugin, ProgressReporter, ResourceClass, TaskContext, TaskOutcome,
};
use crate::common::UfsFactory;
use crate::worker::task::load_task_runner::LoadTaskRunner;
use curvine_client_core::file::CurvineFileSystem;
use curvine_error::FsResult;
use curvine_model::JobTaskType;
use std::sync::Arc;

/// The compatibility plugin for the existing Load/Export runner.
pub struct TransferPlugin {
    fs: CurvineFileSystem,
    factory: Arc<UfsFactory>,
    progress_interval_ms: u64,
    task_timeout_ms: u64,
}

impl TransferPlugin {
    pub fn new(
        fs: CurvineFileSystem,
        factory: Arc<UfsFactory>,
        progress_interval_ms: u64,
        task_timeout_ms: u64,
    ) -> Self {
        Self {
            fs,
            factory,
            progress_interval_ms,
            task_timeout_ms,
        }
    }
}

struct TransferExecutor {
    runner: Option<LoadTaskRunner>,
    remove_task: bool,
}

impl JobExecutor for TransferExecutor {
    fn run(&mut self) -> JobFuture<'_> {
        Box::pin(async move {
            let runner = self.runner.take().expect("transfer executor already run");
            self.remove_task = runner.run().await;
            Ok(TaskOutcome::Completed)
        })
    }

    fn should_remove(&self) -> bool {
        self.remove_task
    }
}

impl JobPlugin for TransferPlugin {
    fn task_types(&self) -> Vec<JobTaskType> {
        vec![JobTaskType::Load, JobTaskType::Export]
    }

    fn name(&self) -> &'static str {
        "transfer"
    }

    fn resource_class(&self) -> ResourceClass {
        ResourceClass::Network
    }

    fn create_executor(
        &self,
        task: Arc<TaskContext>,
        reporter: ProgressReporter,
    ) -> FsResult<Box<dyn JobExecutor>> {
        Ok(Box::new(TransferExecutor {
            runner: Some(LoadTaskRunner::new(
                task,
                self.fs.clone(),
                self.factory.clone(),
                reporter,
                self.progress_interval_ms,
                self.task_timeout_ms,
            )),
            remove_task: false,
        }))
    }
}
