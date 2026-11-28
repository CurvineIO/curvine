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

use super::task_result_tests::{sample_task, ReportRecorder};
use super::*;
use crate::worker::task::{JobFuture, JobPlugin, TaskExecCtx, TaskOutcome};
use curvine_config::WorkerTaskClassLimit;
use curvine_error::FsError;
use curvine_net::net::NetUtils;
use curvine_proto::TransferTaskStateProto;
use curvine_rpc::server::{RpcServer, ServerConf};
use std::time::Duration;
use tokio::sync::mpsc;

struct HeldPlugin {
    task_type: TransferKind,
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
    fn run(self: Box<Self>, _: TaskExecCtx) -> JobFuture {
        Box::pin(async move {
            self.started.send(self.task_id.clone()).unwrap();
            self.release.acquire().await.unwrap().forget();
            Ok(TaskOutcome::Completed.into())
        })
    }
}

impl JobPlugin for HeldPlugin {
    fn task_kinds(&self) -> Vec<TransferKind> {
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
        (TransferKind::Load, ResourceClass::Network),
        (TransferKind::Export, ResourceClass::Cpu),
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
        manager.submit_task_with_kind(task_type, task)
    };
    rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), async {
            assert!(submit(TransferKind::Load, "factory-error", 1).is_err());
            assert_eq!(manager.admitted.load(Ordering::Acquire), 0);
            assert!(
                submit(TransferKind::Load, "network-running", 1)
                    .unwrap()
                    .accepted
            );
            assert_eq!(events.recv().await.as_deref(), Some("network-running"));
            assert!(
                submit(TransferKind::Load, "network-waiting", 1)
                    .unwrap()
                    .accepted
            );
            // A retry of the same attempt is still acknowledged at capacity.
            assert!(
                submit(TransferKind::Load, "network-running", 1)
                    .unwrap()
                    .accepted
            );
            let rejection = submit(TransferKind::Load, "network-running", 2).unwrap();
            assert!(!rejection.accepted && rejection.retryable);
            assert!(rejection.reject_reason.contains("Network class queue full"));
            assert!(manager
                .query_transfer_task("job", "network-running", 1, 1, "session")
                .is_some());

            assert!(
                submit(TransferKind::Export, "cpu-running", 1)
                    .unwrap()
                    .accepted
            );
            assert_eq!(events.recv().await.as_deref(), Some("cpu-running"));
            assert_eq!(manager.available_worker_task_permits(), 0);
            let rejection = submit(TransferKind::Export, "cpu-extra", 1).unwrap();
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

#[test]
fn queued_cancellation_reports_without_waiting_for_class_or_global_capacity() {
    for wait_on_class in [true, false] {
        let rt = Arc::new(Runtime::single());
        let port = NetUtils::hold_available_port();
        let recorder = ReportRecorder::default();
        let server = RpcServer::with_rt(
            rt.clone(),
            ServerConf::with_hostname("127.0.0.1", port),
            recorder.clone(),
        );
        let mut listener = server.new_state_listener();
        let server_task = rt.spawn(async move { server.run().await });
        let mut conf = ClusterConf::default();
        conf.transfer.endpoints = vec![format!("127.0.0.1:{port}")];
        conf.job.worker_max_concurrent_tasks = if wait_on_class { 2 } else { 1 };
        conf.job.worker_max_queued_tasks = 2;
        conf.job.worker_task_class_limits.network = WorkerTaskClassLimit {
            max_concurrent_tasks: Some(1),
            max_queued_tasks: Some(2),
        };
        let mut manager = TaskManager::with_rt(rt.clone(), &conf, "session").unwrap();
        let (started, mut events) = mpsc::unbounded_channel();
        let release = Arc::new(Semaphore::new(0));
        let mut registry = JobRegistry::new();
        registry
            .register(HeldPlugin {
                task_type: TransferKind::Load,
                class: ResourceClass::Network,
                started,
                release: release.clone(),
            })
            .unwrap();
        manager.registry = Arc::new(registry);
        rt.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                listener.wait_startup().await.unwrap();
                // Hold the global permit ourselves for the global-wait case.
                // The task can then acquire a class permit and be canceled while
                // it owns that permit but still cannot run.
                let global_holder = if wait_on_class {
                    let mut holder = sample_task();
                    holder.task_id = "holder".into();
                    holder.job.job_id = "holder-job".into();
                    assert!(manager.submit_task(holder).unwrap().accepted);
                    assert_eq!(events.recv().await.as_deref(), Some("holder"));
                    None
                } else {
                    Some(
                        manager
                            .worker_task_semaphore
                            .clone()
                            .try_acquire_owned()
                            .unwrap(),
                    )
                };
                assert!(manager.submit_task(sample_task()).unwrap().accepted);
                let limits = &manager.class_limits[&ResourceClass::Network];
                let class = limits.concurrent.as_ref().unwrap();
                while class.available_permits() != 0 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(
                    manager.available_worker_task_permits(),
                    usize::from(wait_on_class)
                );
                tokio::task::yield_now().await;
                manager.cancel_job("job").unwrap();

                // A terminal RPC must be delivered and acknowledged without
                // unblocking the execution permit holder.
                while manager.tasks.contains("task") {
                    tokio::task::yield_now().await;
                }
                let remaining = usize::from(wait_on_class);
                // The task store entry is removed just before the retention
                // permit drops; allow the runner to finish that drop.
                let mut retained = Vec::new();
                while retained.len() < 2 - remaining {
                    if let Some(permit) = manager.terminal_recovery.reserve() {
                        retained.push(permit);
                    } else {
                        tokio::task::yield_now().await;
                    }
                }
                assert!(manager.terminal_recovery.reserve().is_none());
                assert_eq!(manager.admitted.load(Ordering::Acquire), remaining);
                assert_eq!(
                    limits.queue.as_ref().unwrap().0.load(Ordering::Acquire),
                    remaining
                );
                assert_eq!(class.available_permits(), usize::from(!wait_on_class));
                assert!(events.try_recv().is_err(), "canceled task must not run");
                let reports = recorder.0.lock().unwrap();
                assert_eq!(reports.len(), 1);
                assert_eq!(reports[0].task_id, "task");
                assert_eq!(
                    reports[0].state,
                    TransferTaskStateProto::TransferTaskCanceled as i32
                );
                drop(reports);
                drop(retained);
                drop(global_holder);
                release.add_permits(1);
            })
            .await
            .unwrap();
        });
        server_task.abort();
        let _ = rt.block_on(server_task);
    }
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
    let permit = acquire_execution_slots(global.clone(), None).await.unwrap();
    assert!(permit.1.is_none());
    assert_eq!(global.available_permits(), 0);
    drop(permit);
    assert_eq!(global.available_permits(), 1);
}

#[test]
fn reuses_exported_task_gauge_and_disables_incompatible_collectors() {
    let name = "test_worker_task_registered_gauge";
    let existing = Metrics::new_gauge_vec(name, "Existing task gauge", &["task_type"]).unwrap();
    let shared = task_gauge(name, "Unused fallback help").unwrap();
    let _guard = InFlightMetric::new(&shared, TransferKind::Load);
    assert_eq!(existing.with_label_values(&["load"]).get(), 1);
    assert!(Metrics::text_output()
        .unwrap()
        .contains("test_worker_task_registered_gauge{task_type=\"load\"} 1"));

    let name = "test_worker_task_wrong_label";
    Metrics::new_gauge_vec(name, "Incompatible task gauge", &["other"]).unwrap();
    assert!(task_gauge(name, "Task gauge").is_none());
    let name = "test_worker_task_wrong_type";
    Metrics::new_counter_vec(name, "Incompatible task counter", &["task_type"]).unwrap();
    assert!(task_gauge(name, "Task gauge").is_none());
}

#[test]
fn metrics_carry_task_type_label() {
    let admitted = Metrics::new_gauge_vec(
        "test_worker_job_tasks_admitted",
        "Test gauge for admitted tasks",
        &["task_type"],
    )
    .unwrap();
    let running = Metrics::new_gauge_vec(
        "test_worker_job_tasks_running",
        "Test gauge for running tasks",
        &["task_type"],
    )
    .unwrap();
    let _admitted_guard = InFlightMetric::new(&admitted, TransferKind::Export);
    let running_guard = InFlightMetric::new(&running, TransferKind::Export);
    let exported = Metrics::text_output().unwrap();
    assert!(exported.contains("test_worker_job_tasks_admitted{task_type=\"export\"} 1"));
    assert!(exported.contains("test_worker_job_tasks_running{task_type=\"export\"} 1"));
    drop(running_guard);
    assert_eq!(running.with_label_values(&["export"]).get(), 0);
}
