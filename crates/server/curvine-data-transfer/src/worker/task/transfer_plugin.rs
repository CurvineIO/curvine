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
    JobExecutor, JobFuture, JobPlugin, ProgressReporter, ResourceClass, TaskContext, TaskExecCtx,
    TaskExecution, TaskOutcome,
};
use crate::common::UfsFactory;
use crate::worker::task::transfer_task_runner::TransferTaskRunner;
use curvine_client_core::file::CurvineFileSystem;
use curvine_error::FsResult;
use curvine_model::TransferKind;
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
    runner: TransferTaskRunner,
}

impl JobExecutor for TransferExecutor {
    fn run(self: Box<Self>, ctx: TaskExecCtx) -> JobFuture {
        Box::pin(async move {
            // The compatibility runner polls the same TaskContext while copying.
            let remove_task = self.runner.run().await;
            let outcome = if ctx.is_cancelled() {
                TaskOutcome::Canceled
            } else {
                TaskOutcome::Completed
            };
            Ok(TaskExecution {
                outcome,
                remove_task,
            })
        })
    }
}

impl JobPlugin for TransferPlugin {
    fn task_kinds(&self) -> Vec<TransferKind> {
        vec![TransferKind::Load, TransferKind::Export]
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
            runner: TransferTaskRunner::new(
                task,
                self.fs.clone(),
                self.factory.clone(),
                reporter,
                self.progress_interval_ms,
                self.task_timeout_ms,
            ),
        }))
    }
}
