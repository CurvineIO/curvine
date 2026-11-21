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

use super::task_result_tests::sample_task;
use super::*;
use crate::worker::task::{JobFuture, JobPlugin, TaskOutcome};
use curvine_config::WorkerTaskClassLimit;
use curvine_error::FsError;
use std::time::Duration;
use tokio::sync::mpsc;

struct HeldPlugin {
    task_type: JobTaskType,
    class: ResourceClass,
    started: mpsc::UnboundedSender<String>,
    release: Arc<Semaphore>,
}

struct HeldExecutor {
    task_id: String,
    started: mpsc::UnboundedSender<String>,
    release: Arc<Semaphore>,
}

impl JobExecutor for HeldExecutor {
    fn run(&mut self) -> JobFuture<'_> {
        Box::pin(async move {
            self.started.send(self.task_id.clone()).unwrap();
            self.release.acquire().await.unwrap().forget();
            Ok(TaskOutcome::Completed)
        })
    }
}

impl JobPlugin for HeldPlugin {
    fn task_types(&self) -> Vec<JobTaskType> {
        vec![self.task_type]
    }

    fn name(&self) -> &'static str {
        "test-held-task"
    }

    fn resource_class(&self) -> ResourceClass {
        self.class
    }

    fn create_executor(
        &self,
        task: Arc<TaskContext>,
        _: ProgressReporter,
    ) -> FsResult<Box<dyn JobExecutor>> {
        if task.info.task_id == "factory-error" {
            return Err(FsError::common("test executor construction failed"));
        }
        Ok(Box::new(HeldExecutor {
            task_id: task.info.task_id.clone(),
            started: self.started.clone(),
            release: self.release.clone(),
        }))
    }
}

#[test]
fn saturated_class_does_not_block_other_class() {
    let rt = Arc::new(Runtime::single());
    let mut conf = ClusterConf::default();
    conf.job.worker_max_concurrent_tasks = 2;
    conf.job.worker_max_queued_tasks = 3;
    conf.job.worker_task_class_limits.network = WorkerTaskClassLimit {
        max_concurrent_tasks: Some(1),
        max_queued_tasks: Some(2),
    };
    conf.job.worker_task_class_limits.cpu.max_queued_tasks = Some(2);
    let mut manager = TaskManager::with_rt(rt.clone(), &conf, "session").unwrap();
    let (started, mut events) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    let mut registry = JobRegistry::new();
    for (task_type, class) in [
        (JobTaskType::Load, ResourceClass::Network),
        (JobTaskType::Export, ResourceClass::Cpu),
    ] {
        registry
            .register(HeldPlugin {
                task_type,
                class,
                started: started.clone(),
                release: release.clone(),
            })
            .unwrap();
    }
    manager.registry = Arc::new(registry);
    let submit = |task_type, id: &str, attempt| {
        let mut task = sample_task();
        task.task_id = id.into();
        let report = task.transfer_report.as_mut().unwrap();
        report.attempt_id = attempt;
        report.report_target = "transfer:9010".into();
        manager.submit_task_with_type(task_type, task)
    };
    rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), async {
            assert!(submit(JobTaskType::Load, "factory-error", 1).is_err());
            assert_eq!(manager.admitted.load(Ordering::Acquire), 0);
            assert!(
                submit(JobTaskType::Load, "network-running", 1)
                    .unwrap()
                    .accepted
            );
            assert_eq!(events.recv().await.as_deref(), Some("network-running"));
            assert!(
                submit(JobTaskType::Load, "network-waiting", 1)
                    .unwrap()
                    .accepted
            );
            // A retry of the same attempt is still acknowledged at capacity.
            assert!(
                submit(JobTaskType::Load, "network-running", 1)
                    .unwrap()
                    .accepted
            );
            let rejection = submit(JobTaskType::Load, "network-running", 2).unwrap();
            assert!(!rejection.accepted && rejection.retryable);
            assert!(rejection.reject_reason.contains("Network class queue full"));
            assert!(manager
                .query_transfer_task("job", "network-running", 1, 1, "session")
                .is_some());

            assert!(
                submit(JobTaskType::Export, "cpu-running", 1)
                    .unwrap()
                    .accepted
            );
            assert_eq!(events.recv().await.as_deref(), Some("cpu-running"));
            assert_eq!(manager.available_worker_task_permits(), 0);
            let rejection = submit(JobTaskType::Export, "cpu-extra", 1).unwrap();
            assert!(!rejection.accepted && rejection.retryable);
            assert!(rejection.reject_reason.starts_with("queue full:"));
            let (cpu_admitted, _) = manager.class_limits[&ResourceClass::Cpu]
                .queue
                .as_ref()
                .unwrap();
            assert_eq!(
                cpu_admitted.load(Ordering::Acquire),
                1,
                "global rejection rolled back the class reservation"
            );

            release.add_permits(3);
            assert_eq!(events.recv().await.as_deref(), Some("network-waiting"));
            while manager.admitted.load(Ordering::Acquire) > 0 {
                tokio::task::yield_now().await;
            }
            assert_eq!(cpu_admitted.load(Ordering::Acquire), 0);
            assert_eq!(manager.available_worker_task_permits(), 2);
        })
        .await
        .unwrap();
    });
}

#[tokio::test]
async fn defaults_equal_current_behavior() {
    let mut job = JobConf::default();
    job.init().unwrap();
    let limits = ClassLimits::from_job(&job);
    assert!(limits.is_empty());
    assert_eq!(
        job.worker_max_queued_tasks,
        job.worker_max_concurrent_tasks * 2
    );
    let admitted = Arc::new(AtomicUsize::new(0));
    let _slots: Vec<_> = (0..3)
        .map(|_| {
            admit_task(
                &admitted,
                3,
                ResourceClass::Network,
                limits.get(&ResourceClass::Network),
            )
            .unwrap_or_else(|_| panic!("slot available"))
        })
        .collect();
    assert!(admit_task(&admitted, 3, ResourceClass::Cpu, None).is_err());
    let global = Arc::new(Semaphore::new(1));
    let permit = acquire_execution_permits(global.clone(), None)
        .await
        .unwrap();
    assert!(permit.1.is_none());
    assert_eq!(global.available_permits(), 0);
    drop(permit);
    assert_eq!(global.available_permits(), 1);
}

#[test]
fn metrics_carry_task_type_label() {
    let metrics = TaskMetrics::get().unwrap();
    let _admitted = InFlightMetric::new(&metrics.admitted, JobTaskType::TestOnly);
    let running = InFlightMetric::new(&metrics.running, JobTaskType::TestOnly);
    let exported = Metrics::text_output().unwrap();
    assert!(exported.contains("worker_job_tasks_admitted{task_type=\"test_only\"} 1"));
    assert!(exported.contains("worker_job_tasks_running{task_type=\"test_only\"} 1"));
    drop(running);
    assert_eq!(metrics.running.with_label_values(&["test_only"]).get(), 0);
}
