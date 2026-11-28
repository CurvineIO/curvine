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

use super::terminal_report::{deliver_terminal_report, ReportOutcome, ReportPolicy};
use super::{TaskContext, TaskStore};
use curvine_error::{FsError, FsResult};
use curvine_model::{TransferTaskResult, WorkerTaskProgress};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_transfer_client::TransferClient;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

const RECOVERY_INTERVAL: Duration = Duration::from_secs(5);

/// Reserved before accepting a task, retained through execution and notification.
/// Removing an old attempt must never remove its replacement from the task store.
pub(super) struct RetainedTask {
    pub context: Arc<TaskContext>,
    tasks: TaskStore,
    permit: Option<OwnedSemaphorePermit>,
    remove_on_drop: bool,
}

impl RetainedTask {
    pub fn new(context: Arc<TaskContext>, tasks: TaskStore, permit: OwnedSemaphorePermit) -> Self {
        Self {
            context,
            tasks,
            permit: Some(permit),
            remove_on_drop: true,
        }
    }

    /// Preserve the plugin contract for probe-only results without a terminal notification.
    pub fn keep_for_probe(mut self) {
        // The context owns only the permit, not this guard or TaskStore, avoiding
        // a reference cycle. Removal/replacement frees it after the last reader.
        if let Some(permit) = self.permit.take() {
            self.context.retain_for_probe(permit);
        }
        self.remove_on_drop = false;
    }

    pub fn is_current(&self) -> bool {
        self.tasks
            .get(&self.context.info.task_id)
            .is_some_and(|ctx| Arc::ptr_eq(ctx.value(), &self.context))
    }
}

impl Drop for RetainedTask {
    fn drop(&mut self) {
        if !self.remove_on_drop {
            return;
        }
        self.tasks.remove_if(&self.context.info.task_id, |_, ctx| {
            Arc::ptr_eq(ctx, &self.context)
        });
    }
}

pub(super) struct PendingReport {
    pub task: RetainedTask,
    pub progress: WorkerTaskProgress,
    pub result: Option<TransferTaskResult>,
    pub client: Option<TransferClient>,
}

impl PendingReport {
    pub fn send(&self) -> impl Future<Output = FsResult<bool>> + Send + 'static {
        let context = self.task.context.clone();
        let progress = self.progress.clone();
        let result = self.result.clone();
        let client = self.client.clone();
        async move {
            let client = client.ok_or_else(|| FsError::common("Missing transfer report client"))?;
            let info = context
                .info
                .transfer_report
                .as_ref()
                .ok_or_else(|| FsError::common("Missing transfer report identity"))?;
            client
                .report_task_with_result(
                    &context.info.job.job_id,
                    &context.info.task_id,
                    info,
                    progress,
                    result,
                )
                .await
        }
    }
}

/// One recovery consumer per manager. Permits bound executing, notifying and
/// retained results together, including entries still in the channel.
pub(super) struct TerminalRecovery {
    slots: Arc<Semaphore>,
    sender: mpsc::Sender<PendingReport>,
    stop: watch::Sender<()>,
}

impl TerminalRecovery {
    pub fn new(rt: &Runtime, limit: usize, active: Arc<Semaphore>) -> Self {
        let (sender, receiver) = mpsc::channel(limit);
        let (stop, stopped) = watch::channel(());
        rt.spawn(recover(
            receiver,
            stopped,
            active,
            RECOVERY_INTERVAL,
            PendingReport::send,
        ));
        Self {
            slots: Arc::new(Semaphore::new(limit)),
            sender,
            stop,
        }
    }

    pub fn reserve(&self) -> Option<OwnedSemaphorePermit> {
        self.slots.clone().try_acquire_owned().ok()
    }

    pub fn stopped(&self) -> watch::Receiver<()> {
        self.stop.subscribe()
    }

    pub fn sender(&self) -> mpsc::Sender<PendingReport> {
        self.sender.clone()
    }

    pub fn defer(sender: &mpsc::Sender<PendingReport>, report: PendingReport) {
        // Every entry already owns one of `limit` permits; a live receiver's
        // channel cannot be full when this additional entry owns a permit.
        if sender.try_send(report).is_err() {
            log::warn!("terminal recovery stopped before accepting a retained result");
        }
    }
}

async fn recover<F, Fut>(
    mut receiver: mpsc::Receiver<PendingReport>,
    mut stopped: watch::Receiver<()>,
    active: Arc<Semaphore>,
    interval: Duration,
    mut send: F,
) where
    F: FnMut(&PendingReport) -> Fut,
    Fut: Future<Output = FsResult<bool>>,
{
    let mut pending = VecDeque::<(Instant, PendingReport)>::new();
    loop {
        let deadline = pending
            .front()
            .map(|(due, _)| *due)
            .unwrap_or_else(|| Instant::now() + interval);
        tokio::select! {
            biased;
            _ = stopped.changed() => return,
            incoming = receiver.recv() => match incoming {
                Some(report) => pending.push_back((Instant::now() + interval, report)),
                None => return,
            },
            _ = tokio::time::sleep_until(deadline), if !pending.is_empty() => {
                let Some((_, report)) = pending.pop_front() else { continue; };
                if !report.task.is_current() { continue; }
                // One attempt per recovery round, with no copy permit held.
                // A single consumer also bounds recovery traffic during outages.
                let work = async {
                    let Ok(_permit) = active.acquire().await else {
                        return ReportOutcome::Interrupted;
                    };
                    deliver_terminal_report(
                        || send(&report),
                        || !report.task.is_current(),
                        ReportPolicy { attempts: 1, ..ReportPolicy::default() },
                    ).await
                };
                let outcome = tokio::select! {
                    biased;
                    _ = stopped.changed() => return,
                    result = tokio::time::timeout(Duration::from_secs(20), work) =>
                        result.unwrap_or(ReportOutcome::Exhausted),
                };
                if !matches!(outcome, ReportOutcome::Accepted | ReportOutcome::Rejected)
                    && report.task.is_current()
                {
                    pending.push_back((Instant::now() + interval, report));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::{
        MountInfo, StorageType, TransferJobInfo, TransferTaskInfo, TransferTaskReportInfo,
        TtlAction, WorkerAddress, WorkerTaskState,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn info(id: &str, attempt: u64) -> TransferTaskInfo {
        TransferTaskInfo {
            job: TransferJobInfo {
                job_id: "job".into(),
                source_path: "s".into(),
                target_path: "t".into(),
                block_size: 4096,
                replicas: 1,
                storage_type: StorageType::default(),
                ttl_ms: 0,
                ttl_action: TtlAction::default(),
                mount_info: MountInfo::default(),
                create_time: 0,
                overwrite: None,
            },
            task_id: id.into(),
            worker: WorkerAddress::default(),
            source_path: "s".into(),
            target_path: "t".into(),
            create_time: 0,
            source_read_plan_json: String::new(),
            transfer_report: Some(TransferTaskReportInfo {
                run_id: 1,
                attempt_id: attempt,
                worker_id: 7,
                worker_session_id: "session".into(),
                report_target: String::new(),
                report_endpoints: vec![],
            }),
        }
    }

    fn entry(queue: &TerminalRecovery, tasks: &TaskStore, id: &str, attempt: u64) -> PendingReport {
        let permit = queue
            .reserve()
            .expect("retention reserved before admission");
        let context = tasks.insert(info(id, attempt));
        let progress = context.update_progress(4096, 4096, true);
        PendingReport {
            task: RetainedTask::new(context, tasks.clone(), permit),
            progress,
            result: None,
            client: None,
        }
    }

    fn start<F, Fut>(limit: usize, send: F) -> (TerminalRecovery, tokio::task::JoinHandle<()>)
    where
        F: FnMut(&PendingReport) -> Fut + Send + 'static,
        Fut: Future<Output = FsResult<bool>> + Send + 'static,
    {
        let (sender, receiver) = mpsc::channel(limit);
        let (stop, stopped) = watch::channel(());
        let queue = TerminalRecovery {
            slots: Arc::new(Semaphore::new(limit)),
            sender,
            stop,
        };
        let handle = tokio::spawn(recover(
            receiver,
            stopped,
            Arc::new(Semaphore::new(1)),
            Duration::from_millis(1),
            send,
        ));
        (queue, handle)
    }

    async fn drained(queue: &TerminalRecovery, count: u32) {
        drop(
            tokio::time::timeout(Duration::from_secs(2), queue.slots.acquire_many(count))
                .await
                .expect("retained results did not drain")
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn review_regression_probe_only_contexts_hold_retention_until_removed() {
        let (queue, handle) = start(2, |_| async { panic!("probe-only entries must not send") });
        let tasks = TaskStore::new();
        for id in ["one", "two"] {
            entry(&queue, &tasks, id, 0).task.keep_for_probe();
        }
        assert_eq!(tasks.len(), 2);
        assert!(
            queue.reserve().is_none(),
            "retained contexts must remain charged"
        );
        let first = tasks.get("one").unwrap().clone();
        let removed = tasks.remove_if("one", |_, ctx| Arc::ptr_eq(ctx, &first));
        drop(removed);
        assert!(
            queue.reserve().is_none(),
            "an outstanding context reader still owns memory"
        );
        drop(first);
        assert_eq!(queue.slots.available_permits(), 1);
        drop(tasks);
        assert_eq!(
            queue.slots.available_permits(),
            2,
            "store teardown releases probe-only contexts"
        );
        drop(queue);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn review_regression_probe_only_replacement_keeps_new_attempt_charged() {
        let (queue, handle) = start(2, |_| async { Ok(true) });
        let tasks = TaskStore::new();
        let old = entry(&queue, &tasks, "same", 0);
        let old_context = old.task.context.clone();
        old.task.keep_for_probe();
        let newer = entry(&queue, &tasks, "same", 1);
        let newer_context = newer.task.context.clone();
        newer.task.keep_for_probe();
        assert_eq!(queue.slots.available_permits(), 0);
        assert!(tasks
            .remove_if("same", |_, ctx| Arc::ptr_eq(ctx, &old_context))
            .is_none());
        drop(old_context);
        assert_eq!(queue.slots.available_permits(), 1);
        assert!(Arc::ptr_eq(
            tasks.get("same").unwrap().value(),
            &newer_context
        ));
        drop(newer_context);
        drop(tasks);
        assert_eq!(queue.slots.available_permits(), 2);
        drop(queue);
        handle.await.unwrap();
    }

    #[test]
    fn review_regression_terminal_progress_is_immutable() {
        for state in [
            WorkerTaskState::Completed,
            WorkerTaskState::Failed,
            WorkerTaskState::Canceled,
            WorkerTaskState::PartialSuccess,
        ] {
            let context = TaskContext::new(info("one", 0));
            context.update_state(WorkerTaskState::Loading, "copying");
            context.update_progress(2048, 4096, false);
            match state {
                WorkerTaskState::Completed => {
                    context.update_progress(4096, 4096, true);
                }
                WorkerTaskState::Failed => {
                    context.set_failed("original failure");
                }
                WorkerTaskState::Canceled => {
                    context.set_canceled("original cancellation");
                }
                _ => context.update_state(state, "partial result"),
            }
            let frozen = serde_json::to_value(context.progress()).unwrap();
            context.request_cancel("late cancel");
            assert_eq!(serde_json::to_value(context.progress()).unwrap(), frozen);
            assert_eq!(
                serde_json::to_value(context.set_canceled("late supersede")).unwrap(),
                frozen
            );
            assert_eq!(
                serde_json::to_value(context.set_failed("late failure")).unwrap(),
                frozen
            );
            context.update_state(WorkerTaskState::Loading, "late running update");
            assert_eq!(
                serde_json::to_value(context.update_progress(0, 8192, false)).unwrap(),
                frozen
            );
            assert_eq!(
                serde_json::to_value(context.update_progress(8192, 8192, true)).unwrap(),
                frozen
            );
            assert_eq!(serde_json::to_value(context.progress()).unwrap(), frozen);
        }
    }

    #[test]
    fn review_regression_running_cancellation_still_reaches_terminal_state() {
        let context = TaskContext::new(info("one", 0));
        context.update_state(WorkerTaskState::Loading, "copying");
        context.update_progress(2048, 4096, false);
        context.request_cancel("stop copying");
        assert!(context.is_cancel());
        assert_eq!(context.get_state(), WorkerTaskState::Loading);
        assert_eq!(context.update_progress(4096, 4096, true).loaded_size, 2048);
        let canceled = context.set_canceled("copy aborted");
        assert_eq!(canceled.state, WorkerTaskState::Canceled);
        assert_eq!(canceled.loaded_size, 2048);
    }

    #[test]
    fn review_regression_competing_terminal_transitions_return_the_same_result() {
        let context = Arc::new(TaskContext::new(info("one", 0)));
        let barrier = Arc::new(std::sync::Barrier::new(2));
        std::thread::scope(|scope| {
            let a = scope.spawn(|| {
                barrier.wait();
                context.update_progress(4096, 4096, true)
            });
            let b = scope.spawn(|| {
                barrier.wait();
                context.set_failed("concurrent failure")
            });
            let first = serde_json::to_value(a.join().unwrap()).unwrap();
            let second = serde_json::to_value(b.join().unwrap()).unwrap();
            assert_eq!(first, second);
            assert_eq!(serde_json::to_value(context.progress()).unwrap(), first);
        });
    }

    #[tokio::test]
    async fn lost_ack_then_replay_reclaims_context_and_reopens_admission() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let saved = Arc::new(std::sync::Mutex::new(None));
        let master_result = saved.clone();
        let (queue, handle) = start(1, move |report| {
            let snapshot = serde_json::to_string(&report.progress).unwrap();
            let mut stored = master_result.lock().unwrap();
            if let Some(previous) = stored.as_ref() {
                assert_eq!(previous, &snapshot);
            } else {
                *stored = Some(snapshot);
            }
            let call = observed.fetch_add(1, Ordering::SeqCst);
            async move {
                if call == 0 {
                    Err(FsError::transfer_overloaded("ACK lost after persistence"))
                } else {
                    Ok(true)
                }
            }
        });
        let tasks = TaskStore::new();
        let report = entry(&queue, &tasks, "one", 0);
        assert!(queue.reserve().is_none());
        TerminalRecovery::defer(&queue.sender, report);
        drained(&queue, 1).await;
        assert!(!tasks.contains("one"));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        drop(queue);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn probe_persistence_does_not_delete_worker_result_before_ack() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = gate.clone();
        let ready = entered.clone();
        let (queue, handle) = start(1, move |_| {
            let gate = release.clone();
            let entered = ready.clone();
            async move {
                entered.notify_one();
                gate.notified().await;
                Ok(true)
            }
        });
        let tasks = TaskStore::new();
        let report = entry(&queue, &tasks, "one", 0);
        // Same read-only context access used by the scheduler query handler.
        let probed = tasks.get("one").unwrap().progress();
        assert_eq!(probed.state, WorkerTaskState::Completed);
        TerminalRecovery::defer(&queue.sender, report);
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        assert!(tasks.contains("one"));
        assert!(queue.reserve().is_none());
        gate.notify_one();
        drained(&queue, 1).await;
        assert!(!tasks.contains("one"));
        drop(queue);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn explicit_rejection_releases_but_local_error_retains() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let (queue, handle) = start(1, move |_| {
            let call = observed.fetch_add(1, Ordering::SeqCst);
            async move {
                if call == 0 {
                    Err(FsError::common("local configuration error"))
                } else {
                    Ok(false)
                }
            }
        });
        let tasks = TaskStore::new();
        TerminalRecovery::defer(&queue.sender, entry(&queue, &tasks, "one", 0));
        drained(&queue, 1).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(tasks.is_empty());
        drop(queue);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn superseded_entry_does_not_remove_replacement() {
        let (queue, handle) = start(2, |_| async { Ok(true) });
        let tasks = TaskStore::new();
        let old = entry(&queue, &tasks, "same", 0);
        let replacement = entry(&queue, &tasks, "same", 1);
        drop(old);
        assert!(replacement.task.is_current());
        assert_eq!(queue.slots.available_permits(), 1);
        drop(replacement);
        assert!(tasks.is_empty());
        drop(queue);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn unwinding_owner_releases_context_and_retention() {
        let (queue, handle) = start(1, |_| async { Ok(true) });
        let tasks = TaskStore::new();
        let report = entry(&queue, &tasks, "one", 0);
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _report = report;
            panic!("injected runner unwind");
        }));
        assert!(failed.is_err());
        assert!(tasks.is_empty());
        assert!(queue.reserve().is_some());
        drop(queue);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn cancellation_preserves_frozen_completion() {
        let (queue, handle) = start(1, |report| {
            assert_eq!(report.progress.state, WorkerTaskState::Completed);
            assert_eq!(report.progress.message, "task completed successfully");
            async { Ok(true) }
        });
        let tasks = TaskStore::new();
        let report = entry(&queue, &tasks, "one", 0);
        report.task.context.request_cancel("late cancellation");
        TerminalRecovery::defer(&queue.sender, report);
        drained(&queue, 1).await;
        assert!(tasks.is_empty());
        drop(queue);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_interrupts_inflight_and_drops_queued_results() {
        let started = Arc::new(tokio::sync::Notify::new());
        let notify = started.clone();
        let (queue, handle) = start(2, move |_| {
            notify.notify_one();
            std::future::pending::<FsResult<bool>>()
        });
        let tasks = TaskStore::new();
        let slots = queue.slots.clone();
        for id in ["one", "two"] {
            TerminalRecovery::defer(&queue.sender, entry(&queue, &tasks, id, 0));
        }
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .unwrap();
        assert!(queue.reserve().is_none());
        drop(queue);
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .unwrap()
            .unwrap();
        assert!(tasks.is_empty());
        assert_eq!(slots.available_permits(), 2);
    }

    #[tokio::test]
    async fn outage_bounds_all_results_and_restored_receiver_drains_them() {
        use std::sync::atomic::AtomicBool;
        let healthy = Arc::new(AtomicBool::new(false));
        let attempts = Arc::new(AtomicUsize::new(0));
        let state = healthy.clone();
        let calls = attempts.clone();
        let retried = Arc::new(tokio::sync::Notify::new());
        let notify = retried.clone();
        let (queue, handle) = start(3, move |_| {
            let ok = state.load(Ordering::SeqCst);
            if calls.fetch_add(1, Ordering::SeqCst) >= 8 {
                notify.notify_one();
            }
            async move {
                if ok {
                    Ok(true)
                } else {
                    Err(FsError::transfer_overloaded("offline"))
                }
            }
        });
        let tasks = TaskStore::new();
        for id in ["one", "two", "three"] {
            TerminalRecovery::defer(&queue.sender, entry(&queue, &tasks, id, 0));
        }
        tokio::time::timeout(Duration::from_secs(2), retried.notified())
            .await
            .unwrap();
        assert_eq!(tasks.len(), 3);
        assert!(queue.reserve().is_none());
        healthy.store(true, Ordering::SeqCst);
        drained(&queue, 3).await;
        assert!(tasks.is_empty());
        assert!(queue.reserve().is_some());
        drop(queue);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn stale_ack_cannot_remove_new_attempt() {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let notify = entered.clone();
        let gate = release.clone();
        let (queue, handle) = start(2, move |_| {
            let entered = notify.clone();
            let release = gate.clone();
            async move {
                entered.notify_one();
                release.notified().await;
                Ok(true)
            }
        });
        let tasks = TaskStore::new();
        TerminalRecovery::defer(&queue.sender, entry(&queue, &tasks, "same", 0));
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        let replacement = entry(&queue, &tasks, "same", 1);
        release.notify_one();
        drained(&queue, 1).await;
        assert!(replacement.task.is_current());
        assert_eq!(tasks.len(), 1);
        drop(replacement);
        drop(queue);
        handle.await.unwrap();
    }

    #[cfg(feature = "transfer")]
    #[tokio::test]
    async fn real_store_probe_then_recovery_ack_is_idempotent() {
        probe_then_ack(false, false).await;
    }

    #[cfg(feature = "transfer")]
    #[tokio::test]
    async fn review_regression_late_cancel_before_completed_probe_still_acks() {
        probe_then_ack(true, false).await;
    }

    #[cfg(feature = "transfer")]
    #[tokio::test]
    async fn review_regression_late_cancel_before_failed_probe_still_acks() {
        probe_then_ack(true, true).await;
    }

    #[cfg(feature = "transfer")]
    async fn probe_then_ack(late_cancel: bool, failed: bool) {
        use crate::transfer::{MemoryTransferStore, TransferStore};
        use curvine_model::{
            TransferJobRecord, TransferKind, TransferProgress, TransferState, TransferTaskRecord,
            TransferTaskReport, TransferTaskState,
        };
        let store = Arc::new(MemoryTransferStore::new());
        store
            .create_or_get_by_request_id(TransferJobRecord {
                job_key: "key".into(),
                job_id: "job".into(),
                run_id: 1,
                kind: TransferKind::Load,
                spec_version: 0,
                spec_blob: Vec::new(),
                source_path: "s".into(),
                target_path: "t".into(),
                command_json: "{}".into(),
                mount_snapshot_json: "{}".into(),
                secret_ref_json: "{}".into(),
                cluster_snapshot_version: 1,
                cv_metadata_epoch: None,
                state: TransferState::Running,
                owner: "owner".into(),
                lease_epoch: 1,
                lease_expire_at: 120000,
                cancel_requested: false,
                summary: TransferProgress::default(),
                client_request_id: "request".into(),
                submitter: "test".into(),
                tenant: "test".into(),
                created_at: 1,
                updated_at: 1,
            })
            .unwrap();
        store
            .insert_tasks(vec![TransferTaskRecord {
                job_id: "job".into(),
                run_id: 1,
                task_id: "one".into(),
                attempt_id: 0,
                spec_blob: Vec::new(),
                result_json: String::new(),
                source_path: "s".into(),
                target_path: "t".into(),
                worker_id: 7,
                worker_session_id: "session".into(),
                source_read_plan_json: "{}".into(),
                report_target_json: "{}".into(),
                state: TransferTaskState::Running,
                progress: TransferProgress::default(),
                retry_count: 0,
                attempt_started_at: 1,
                last_report_at: 1,
                stale_deadline_at: 60001,
                updated_at: 1,
            }])
            .unwrap();
        fn report(progress: &WorkerTaskProgress, now: i64) -> TransferTaskReport {
            TransferTaskReport {
                job_id: "job".into(),
                run_id: 1,
                task_id: "one".into(),
                attempt_id: 0,
                worker_id: 7,
                worker_session_id: "session".into(),
                state: match progress.state {
                    WorkerTaskState::Completed => TransferTaskState::Completed,
                    WorkerTaskState::Failed => TransferTaskState::Failed,
                    other => panic!("unexpected terminal state: {:?}", other),
                },
                progress: TransferProgress {
                    loaded_size: progress.loaded_size,
                    total_size: progress.total_size,
                    update_time: progress.update_time,
                    message: progress.message.clone(),
                    ..Default::default()
                },
                result_inline: None,
                result_ref: None,
                result_size: None,
                now_ms: now,
                stale_deadline_at: now + 60000,
            }
        }
        let receiver = store.clone();
        let (queue, handle) = start(1, move |pending| {
            let result = receiver.update_task_report(report(&pending.progress, 100));
            async move {
                assert!(
                    matches!(result, Ok(true)),
                    "exact recovery replay must be acknowledged"
                );
                result
            }
        });
        let tasks = TaskStore::new();
        let context = tasks.insert(info("one", 0));
        let progress = if failed {
            context.update_progress(4096, 4096, false);
            context.set_failed("original failure")
        } else {
            context.update_progress(4096, 4096, true)
        };
        let pending = PendingReport {
            task: RetainedTask::new(context.clone(), tasks.clone(), queue.reserve().unwrap()),
            progress,
            result: None,
            client: None,
        };
        let initial = deliver_terminal_report(
            || async { Err(FsError::transfer_overloaded("initial report unavailable")) },
            || false,
            ReportPolicy {
                attempts: 1,
                ..ReportPolicy::default()
            },
        )
        .await;
        assert_eq!(initial, ReportOutcome::Exhausted);
        if late_cancel {
            tasks.cancel("job");
        }
        assert_eq!(
            serde_json::to_value(context.progress()).unwrap(),
            serde_json::to_value(&pending.progress).unwrap()
        );
        assert!(store
            .update_task_report(report(&tasks.get("one").unwrap().progress(), 10))
            .unwrap());
        let persisted = store.get_transfer("job").unwrap().unwrap();
        assert_eq!(persisted.summary.loaded_size, 4096);
        assert!(tasks.contains("one"));
        TerminalRecovery::defer(&queue.sender, pending);
        drained(&queue, 1).await;
        assert!(tasks.is_empty());
        let after = store.get_transfer("job").unwrap().unwrap();
        assert_eq!(after.summary, persisted.summary);
        assert_eq!(after.updated_at, persisted.updated_at);
        drop(queue);
        handle.await.unwrap();
    }
}
