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

use crate::common::UfsFactory;
use crate::master::fs::MasterFilesystem;
use crate::master::job::job_runner::{
    LoadJobPrepareResult, QueuedLoadJob, QueuedLoadJobValidation,
};
use crate::master::{JobStore, LoadJobRunner, Master, MasterMetrics, MountManager};
use core::time::Duration;
use curvine_client::unified::MountValue;
use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::executor::ScheduledExecutor;
use curvine_common::fs::Path;
use curvine_common::state::{
    JobStatus, JobTaskProgress, JobTaskState, LoadJobCommand, LoadJobResult,
};
use curvine_common::FsResult;
use log::{debug, info, warn};
use orpc::common::LocalTime;
use orpc::runtime::{LoopTask, RpcRuntime, Runtime};
use orpc::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender};
use orpc::sync::AtomicCounter;
use orpc::{err_box, err_ext, CommonResult};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::{watch, Mutex};
use tokio::time::timeout;

struct QueuedLoadJobRequest {
    job_id: String,
    queued: QueuedLoadJob,
}

#[derive(Clone)]
struct LoadJobWorkerContext {
    jobs: JobStore,
    master_fs: MasterFilesystem,
    factory: Weak<UfsFactory>,
    job_max_files: usize,
    run_seq: Arc<AtomicCounter>,
    shutdown_rx: watch::Receiver<bool>,
    metrics: Option<&'static MasterMetrics>,
}

#[derive(Clone)]
struct LoadJobSubmitContext {
    jobs: JobStore,
    master_fs: MasterFilesystem,
    factory: Weak<UfsFactory>,
    job_max_files: usize,
    run_seq: Arc<AtomicCounter>,
    load_job_sender: AsyncSender<QueuedLoadJobRequest>,
    shutdown: Arc<AtomicBool>,
    metrics: Option<&'static MasterMetrics>,
}

impl LoadJobWorkerContext {
    fn create_runner(&self) -> Option<LoadJobRunner> {
        self.factory.upgrade().map(|factory| {
            LoadJobRunner::new(
                self.jobs.clone(),
                self.master_fs.clone(),
                factory,
                self.job_max_files,
                self.run_seq.clone(),
            )
        })
    }
}

impl LoadJobSubmitContext {
    fn create_runner(&self) -> Option<LoadJobRunner> {
        self.factory.upgrade().map(|factory| {
            LoadJobRunner::new(
                self.jobs.clone(),
                self.master_fs.clone(),
                factory,
                self.job_max_files,
                self.run_seq.clone(),
            )
        })
    }

    async fn forward_queued_load_job(&self, request: QueuedLoadJobRequest) {
        let job_id = request.job_id.clone();
        let run_id = request.queued.run_id;
        if self.shutdown.load(Ordering::SeqCst) {
            self.fail_queued_load_job(job_id, run_id, "load job manager is shutting down");
            return;
        }

        let Some(runner) = self.create_runner() else {
            self.fail_queued_load_job(job_id, run_id, "load job manager is shutting down");
            return;
        };
        let validation = runner.validate_queued_source(&request.queued).await;
        tokio::task::block_in_place(move || drop(runner));
        match validation {
            Ok(QueuedLoadJobValidation::Ready) => {}
            Ok(QueuedLoadJobValidation::Failed) => {
                self.finish_queued_load_job(true);
                return;
            }
            Ok(QueuedLoadJobValidation::Stale) => {
                self.finish_queued_load_job(false);
                return;
            }
            Err(err) => {
                warn!(
                    "queued load job {} source validation failed: {}",
                    job_id, err
                );
                self.finish_queued_load_job(true);
                return;
            }
        }

        if self.shutdown.load(Ordering::SeqCst) {
            self.fail_queued_load_job(job_id, run_id, "load job manager is shutting down");
            return;
        }

        if let Err(err) = self.load_job_sender.send(request).await {
            self.fail_queued_load_job(job_id, run_id, format!("queue load job failed: {}", err));
        }
    }

    fn fail_queued_load_job(&self, job_id: String, run_id: u64, message: impl Into<String>) {
        let message = message.into();
        warn!("queued load job {} skipped: {}", job_id, message);
        self.jobs
            .update_state_if_run(&job_id, run_id, JobTaskState::Failed, message);
        if let Some(metrics) = self.metrics {
            metrics.load_job_queue_len.dec();
            metrics.load_job_failure_count.inc();
        }
    }

    fn finish_queued_load_job(&self, failed: bool) {
        if let Some(metrics) = self.metrics {
            metrics.load_job_queue_len.dec();
            if failed {
                metrics.load_job_failure_count.inc();
            }
        }
    }
}

/// Load job manager
pub struct JobManager {
    rt: Arc<Runtime>,
    // Soft-isolates load planning from the master RPC/Raft runtime. Process-level
    // resources are still shared, so queue and concurrency limits remain required.
    background_rt: Arc<Runtime>,
    jobs: JobStore,
    master_fs: MasterFilesystem,
    factory: Arc<UfsFactory>,
    mount_manager: Arc<MountManager>,
    job_life_ttl: Duration,
    job_cleanup_ttl: Duration,
    failed_retry_interval: Duration,
    job_max_files: usize,
    run_seq: Arc<AtomicCounter>,
    submit_job_sender: AsyncSender<QueuedLoadJobRequest>,
    shutdown: Arc<AtomicBool>,
    shutdown_tx: watch::Sender<bool>,
    metrics: Option<&'static MasterMetrics>,
}

impl JobManager {
    pub fn from_cluster_conf(
        master_fs: MasterFilesystem,
        mount_manager: Arc<MountManager>,
        rt: Arc<Runtime>,
        conf: &ClusterConf,
    ) -> Self {
        let background_rt = Arc::new(Runtime::new(
            "master-load-job",
            conf.job.master_load_job_runtime_threads,
            conf.job.master_load_job_blocking_threads,
        ));
        let factory = Arc::new(UfsFactory::with_rt(&conf.client, background_rt.clone()));
        let jobs = JobStore::new();
        let run_seq = Arc::new(AtomicCounter::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let metrics = Master::get_metrics().ok();
        let (submit_job_sender, submit_job_receiver) =
            AsyncChannel::new(conf.job.master_max_pending_load_job_submits).split();
        let (load_job_sender, load_job_receiver) =
            AsyncChannel::new(conf.job.master_max_background_load_jobs).split();
        let load_job_worker_num = conf.job.master_max_concurrent_load_jobs;
        let submit_worker_num = conf.job.master_load_job_runtime_threads;

        Self::spawn_submit_workers(
            background_rt.clone(),
            submit_job_receiver,
            submit_worker_num,
            LoadJobSubmitContext {
                jobs: jobs.clone(),
                master_fs: master_fs.clone(),
                factory: Arc::downgrade(&factory),
                job_max_files: conf.job.job_max_files,
                run_seq: run_seq.clone(),
                load_job_sender: load_job_sender.clone(),
                shutdown: shutdown.clone(),
                metrics,
            },
        );

        Self::spawn_load_job_workers(
            background_rt.clone(),
            load_job_receiver,
            load_job_worker_num,
            LoadJobWorkerContext {
                jobs: jobs.clone(),
                master_fs: master_fs.clone(),
                factory: Arc::downgrade(&factory),
                job_max_files: conf.job.job_max_files,
                run_seq: run_seq.clone(),
                shutdown_rx,
                metrics,
            },
        );

        Self {
            rt,
            background_rt,
            jobs,
            master_fs,
            factory,
            mount_manager,
            job_life_ttl: conf.job.job_life_ttl,
            job_cleanup_ttl: conf.job.job_cleanup_ttl,
            failed_retry_interval: conf.job.master_failed_load_job_retry_interval,
            job_max_files: conf.job.job_max_files,
            run_seq,
            submit_job_sender,
            shutdown,
            shutdown_tx,
            metrics,
        }
    }

    fn spawn_submit_workers(
        rt: Arc<Runtime>,
        receiver: AsyncReceiver<QueuedLoadJobRequest>,
        workers: usize,
        context: LoadJobSubmitContext,
    ) {
        let receiver = Arc::new(Mutex::new(receiver));

        for _ in 0..workers {
            let receiver = receiver.clone();
            let context = context.clone();
            rt.spawn(async move {
                loop {
                    let request = {
                        let mut receiver = receiver.lock().await;
                        receiver.recv().await
                    };
                    let Some(request) = request else {
                        break;
                    };
                    context.forward_queued_load_job(request).await;
                }
            });
        }
    }

    fn spawn_load_job_workers(
        rt: Arc<Runtime>,
        receiver: AsyncReceiver<QueuedLoadJobRequest>,
        workers: usize,
        context: LoadJobWorkerContext,
    ) {
        let receiver = Arc::new(Mutex::new(receiver));

        for _ in 0..workers {
            let receiver = receiver.clone();
            let context = context.clone();
            rt.spawn(async move {
                let mut shutdown_rx = context.shutdown_rx.clone();
                loop {
                    let request = {
                        let mut receiver = receiver.lock().await;
                        if *shutdown_rx.borrow() {
                            match receiver.try_recv() {
                                Ok(Some(request)) => Some(request),
                                Ok(None) => None,
                                Err(err) => {
                                    debug!("load job receiver closed during shutdown: {}", err);
                                    None
                                }
                            }
                        } else {
                            tokio::select! {
                                request = receiver.recv() => request,
                                changed = shutdown_rx.changed() => {
                                    if changed.is_err() {
                                        None
                                    } else {
                                        match receiver.try_recv() {
                                            Ok(Some(request)) => Some(request),
                                            Ok(None) => None,
                                            Err(err) => {
                                                debug!("load job receiver closed during shutdown: {}", err);
                                                None
                                            }
                                        }
                                    }
                                },
                            }
                        }
                    };
                    let Some(request) = request else {
                        break;
                    };
                    let QueuedLoadJobRequest { job_id, queued } = request;
                    if let Some(metrics) = context.metrics {
                        metrics.load_job_queue_len.dec();
                        metrics.load_job_running_num.inc();
                    }
                    match context.create_runner() {
                        Some(runner) => {
                            let result = runner.run_queued_load_job(queued).await;
                            tokio::task::block_in_place(move || drop(runner));
                            if let Err(err) = result {
                                warn!("async load job {} failed: {}", job_id, err);
                                if let Some(metrics) = context.metrics {
                                    metrics.load_job_failure_count.inc();
                                }
                            }
                        }
                        None => {
                            let message = "load job manager is shutting down";
                            warn!("async load job {} skipped: {}", job_id, message);
                            let _ =
                                context
                                    .jobs
                                    .update_state(&job_id, JobTaskState::Failed, message);
                            if let Some(metrics) = context.metrics {
                                metrics.load_job_failure_count.inc();
                            }
                        }
                    }
                    if let Some(metrics) = context.metrics {
                        metrics.load_job_running_num.dec();
                    }
                }
            });
        }
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = self.shutdown_tx.send(true);
    }

    /// Start the job manager
    pub fn start(&self) -> CommonResult<()> {
        let cleanup_interval = self.job_cleanup_ttl.as_millis() as u64;
        let ttl_ms = self.job_life_ttl.as_millis() as i64;

        let executor = ScheduledExecutor::new("job_cleanup", cleanup_interval);
        executor.start(JobCleanupTask {
            jobs: self.jobs.clone(),
            ttl_ms,
        })?;

        info!("JobManager started");
        Ok(())
    }

    fn update_state(
        &self,
        job_id: &str,
        state: JobTaskState,
        message: impl Into<String>,
    ) -> FsResult<()> {
        self.jobs.update_state(job_id, state, message)
    }

    pub async fn wait_job_complete(
        &self,
        job_id: impl AsRef<str>,
        duration: Duration,
    ) -> FsResult<JobStatus> {
        timeout(duration, self.wait_job_complete0(job_id)).await?
    }

    async fn wait_job_complete0(&self, job_id: impl AsRef<str>) -> FsResult<JobStatus> {
        let job_id = job_id.as_ref();

        let mut listener = match self.jobs.get(job_id) {
            Some(job) => job.new_listener(),
            None => return err_ext!(FsError::job_not_found(job_id)),
        };

        let status = self.get_job_status(job_id)?;
        if status.state.is_finish() {
            return Ok(status);
        }

        loop {
            let next_state = JobTaskState::from(listener.next_state().await?);
            if next_state.is_finish() {
                return self.get_job_status(job_id);
            }
        }
    }

    pub fn get_job_status(&self, job_id: impl AsRef<str>) -> FsResult<JobStatus> {
        let job_id = job_id.as_ref();
        if let Some(job) = self.jobs.get(job_id) {
            Ok(JobStatus {
                job_id: job.info.job_id.clone(),
                state: job.state.state(),
                source_path: job.info.source_path.clone(),
                target_path: job.info.target_path.clone(),
                progress: job.progress.clone(),
            })
        } else {
            err_ext!(FsError::job_not_found(job_id))
        }
    }

    pub fn create_runner(&self) -> LoadJobRunner {
        LoadJobRunner::new(
            self.jobs.clone(),
            self.master_fs.clone(),
            self.factory.clone(),
            self.job_max_files,
            self.run_seq.clone(),
        )
    }

    pub fn get_mnt(&self, path: &Path) -> FsResult<Option<(Path, Arc<MountValue>)>> {
        if let Some(mnt) = self.mount_manager.get_mount_info(path)? {
            let mnt_value = self.factory.get_mnt(&mnt)?;
            let target_path = mnt_value.toggle_path(path)?;

            Ok(Some((target_path, mnt_value)))
        } else {
            Ok(None)
        }
    }

    pub fn rt(&self) -> &Runtime {
        &self.rt
    }

    pub fn background_rt(&self) -> &Runtime {
        &self.background_rt
    }

    /// See `LoadJobRunner::submit_load_task` for the concurrency contract: concurrent
    /// submits for the same path while a load is running return the **existing** run's
    /// result; the new command's options are not applied (first submitter wins).
    pub async fn submit_load_job(&self, command: LoadJobCommand) -> FsResult<LoadJobResult> {
        if self.shutdown.load(Ordering::SeqCst) {
            return err_box!("load job manager is shutting down");
        }

        let source_path = Path::from_str(&command.source_path)?;
        let mnt = if let Some(mnt) = self.mount_manager.get_mount_info(&source_path)? {
            mnt
        } else {
            return err_box!("Not found mount info for path: {}", source_path);
        };

        let job_runner = self.create_runner();
        if let Some(res) = job_runner.running_job_result_for_command(&command, &mnt)? {
            if let Some(metrics) = self.metrics {
                metrics.load_job_duplicate_count.inc();
            }
            return Ok(res);
        }

        let prepared = match job_runner.admit_load_job(command, mnt, self.failed_retry_interval)? {
            LoadJobPrepareResult::Done(result) => return Ok(result),
            LoadJobPrepareResult::Ready(prepared) => prepared,
        };

        let channel_permit = match self.submit_job_sender.try_reserve()? {
            Some(permit) => permit,
            None => {
                return Err(FsError::resource_exhausted(
                    "master load job submit queue is full; retry later",
                ))
            }
        };

        let (res, queued) = job_runner.commit_prepared_load_job(prepared)?;
        if let Some(queued) = queued {
            let job_id = res.job_id.clone();
            let request = QueuedLoadJobRequest {
                job_id: job_id.clone(),
                queued,
            };
            if let Some(metrics) = self.metrics {
                metrics.load_job_queue_len.inc();
                metrics.load_job_enqueue_count.inc();
            }
            channel_permit.send(request);
        }

        Ok(res)
    }

    /// Handle cancellation of tasks
    pub async fn cancel_job(&self, job_id: impl AsRef<str>) -> FsResult<()> {
        let job_id = job_id.as_ref();
        let assigned_workers = {
            if let Some(job) = self.jobs.get(job_id) {
                let state: JobTaskState = job.state.state();
                // Check whether it can be canceled
                if state == JobTaskState::Completed
                    || state == JobTaskState::Failed
                    || state == JobTaskState::Canceled
                {
                    info!(
                        "job {} is already in final state {:?}, source_path: {}, target_path: {}",
                        job_id, state, job.info.source_path, job.info.target_path
                    );
                    return Ok(());
                }

                job.assigned_workers.clone()
            } else {
                return err_ext!(FsError::job_not_found(job_id));
            }
        };

        self.update_state(job_id, JobTaskState::Canceled, "Canceling job by user")?;

        let job_runner = self.create_runner();
        job_runner.cancel_job(&job_id, assigned_workers).await?;

        Ok(())
    }

    pub fn update_progress(
        &self,
        job_id: impl AsRef<str>,
        task_id: impl AsRef<str>,
        progress: JobTaskProgress,
    ) -> FsResult<()> {
        self.jobs.update_progress(job_id, task_id, progress)
    }

    pub fn jobs(&self) -> &JobStore {
        &self.jobs
    }

    pub fn master_fs(&self) -> &MasterFilesystem {
        &self.master_fs
    }

    pub fn factory(&self) -> &Arc<UfsFactory> {
        &self.factory
    }
}

struct JobCleanupTask {
    jobs: JobStore,
    ttl_ms: i64,
}

impl LoopTask for JobCleanupTask {
    type Error = FsError;

    fn run(&self) -> Result<(), Self::Error> {
        // Collect tasks that need to be removed first
        let mut jobs_to_remove = vec![];
        let now = LocalTime::mills() as i64;
        for entry in self.jobs.iter() {
            let job = entry.value();
            if now > self.ttl_ms + job.info.create_time {
                jobs_to_remove.push(job.info.job_id.clone());
            }
        }

        for job_id in jobs_to_remove {
            if let Some(v) = self.jobs.remove(&job_id) {
                debug!("Removing expired job: {:?}", v.1.info);
            }
        }

        Ok(())
    }

    fn terminate(&self) -> bool {
        false
    }
}
