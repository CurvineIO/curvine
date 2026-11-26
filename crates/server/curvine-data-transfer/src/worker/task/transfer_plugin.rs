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
    fn run(self: Box<Self>, _ctx: TaskExecCtx) -> JobFuture {
        Box::pin(async move {
            // The compatibility runner polls the same TaskContext while copying.
            Ok(self.runner.run().await)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::task::{JobPlugin, TaskExecCtx, TaskOutcome};
    use curvine_config::ClusterConf;
    use curvine_model::{
        MountInfo, StorageType, TransferJobInfo, TransferTaskInfo, TtlAction, WorkerAddress,
    };
    use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};

    fn failing_task() -> TransferTaskInfo {
        TransferTaskInfo {
            job: TransferJobInfo {
                job_id: "plugin-failure-job".to_string(),
                source_path: "file:///definitely-missing-source".to_string(),
                target_path: "file:///unused-target".to_string(),
                block_size: 4096,
                replicas: 1,
                storage_type: StorageType::default(),
                ttl_ms: 0,
                ttl_action: TtlAction::default(),
                mount_info: MountInfo::default(),
                create_time: 0,
                overwrite: Some(false),
            },
            task_id: "plugin-failure-task".to_string(),
            worker: WorkerAddress::default(),
            source_path: "file:///definitely-missing-source".to_string(),
            target_path: "file:///unused-target".to_string(),
            create_time: 0,
            source_read_plan_json: String::new(),
            transfer_report: None,
        }
    }

    #[test]
    fn plugin_reports_failed_copy_as_failed() {
        let rt = Arc::new(AsyncRuntime::single());
        let conf = ClusterConf::default();
        let fs = CurvineFileSystem::with_rt(conf.clone(), rt.clone()).unwrap();
        let factory = UfsFactory::with_rt(&conf.client, rt.clone());
        let task = Arc::new(TaskContext::new(failing_task()));
        let reporter = ProgressReporter::new(task.clone(), &fs, None, usize::MAX).unwrap();
        let plugin = TransferPlugin::new(fs, Arc::new(factory), 60_000, 60_000);
        let executor = plugin.create_executor(task.clone(), reporter).unwrap();

        let execution = rt
            .block_on(executor.run(TaskExecCtx::new(task)))
            .expect("plugin execution must return a task result");
        assert!(matches!(
            execution.outcome,
            TaskOutcome::Failed {
                retryable: false,
                ..
            }
        ));
        assert!(!matches!(execution.outcome, TaskOutcome::Completed));
    }
    #[test]
    fn plugin_returns_frozen_terminal_report_after_executor_is_consumed() {
        let rt = Arc::new(AsyncRuntime::single());
        let conf = ClusterConf::default();
        let fs = CurvineFileSystem::with_rt(conf.clone(), rt.clone()).unwrap();
        let factory = UfsFactory::with_rt(&conf.client, rt.clone());
        let plugin = TransferPlugin::new(fs.clone(), Arc::new(factory), 60_000, 60_000);
        let mut info = failing_task();
        info.transfer_report = Some(curvine_model::TransferTaskReportInfo {
            run_id: 1,
            attempt_id: 1,
            worker_id: 1,
            worker_session_id: "session".into(),
            report_target: String::new(),
            report_endpoints: vec![],
        });
        let task = Arc::new(TaskContext::new(info));
        // Cancel before copy: no network or data I/O is needed to freeze the result.
        task.set_canceled("cancel before execution");
        let default_client =
            curvine_transfer_client::TransferClient::with_context(&fs.fs_context()).unwrap();
        let reporter =
            ProgressReporter::new(task.clone(), &fs, Some(&default_client), usize::MAX).unwrap();
        let executor = plugin.create_executor(task.clone(), reporter).unwrap();
        let execution = rt
            .block_on(executor.run(TaskExecCtx::new(task.clone())))
            .unwrap();
        assert_eq!(execution.outcome, TaskOutcome::Canceled);
        let terminal = execution
            .terminal_report
            .expect("manager must receive the frozen report");
        assert_eq!(
            terminal.progress.state,
            curvine_model::WorkerTaskState::Canceled
        );
        assert!(
            terminal.client.is_some(),
            "the frozen report must keep the attempt's report route"
        );
        task.set_failed("later context mutation");
        assert_eq!(
            terminal.progress.state,
            curvine_model::WorkerTaskState::Canceled
        );
    }
}
