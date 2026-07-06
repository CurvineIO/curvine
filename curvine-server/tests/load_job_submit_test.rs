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

use curvine_common::conf::{ClientConf, ClusterConf, JournalConf, MasterConf};
use curvine_common::state::{
    JobTaskProgress, JobTaskState, LoadJobCommand, LoadJobInfo, LoadTaskInfo, MountInfo,
    MountOptions, StorageType, TtlAction, WorkerAddress, WorkerInfo,
};
use curvine_server::master::fs::MasterFilesystem;
use curvine_server::master::journal::JournalSystem;
use curvine_server::master::{JobContext, JobManager, JobStore, Master};
use curvine_server::worker::task::{TaskContext, TaskStore};
use orpc::common::Utils;
use orpc::runtime::{AsyncRuntime, RpcRuntime, Runtime};
use orpc::CommonResult;
use std::sync::Arc;
use std::time::Duration;

fn new_job_manager(name: &str) -> CommonResult<(Arc<JobManager>, Arc<Runtime>, String)> {
    Master::init_test_metrics();
    let test_name = format!("{}-{}", name, Utils::rand_id());

    let conf = ClusterConf {
        format_master: true,
        testing: true,
        master: MasterConf {
            meta_dir: Utils::test_sub_dir(format!("load-job-submit/meta-{}", test_name)),
            ..Default::default()
        },
        journal: JournalConf {
            enable: false,
            journal_dir: Utils::test_sub_dir(format!("load-job-submit/journal-{}", test_name)),
            ..Default::default()
        },
        ..Default::default()
    };

    let journal_system = JournalSystem::from_conf(&conf)?;
    let master_fs = MasterFilesystem::with_js(&conf, &journal_system);
    master_fs.add_test_worker(WorkerInfo::default());

    let mount_manager = journal_system.mount_manager();
    let rt = Arc::new(AsyncRuntime::single());
    let job_manager = Arc::new(JobManager::from_cluster_conf(
        master_fs,
        mount_manager.clone(),
        rt.clone(),
        &conf,
    ));

    let ufs_root_dir = Utils::test_sub_dir(format!("load-job-submit/ufs-{}", test_name));
    let ufs_root = format!("file://{}", ufs_root_dir);
    mount_manager.mount(None, "/mnt", &ufs_root, &MountOptions::builder().build())?;

    Ok((job_manager, rt, format!("{}/missing-file", ufs_root)))
}

fn load_task(task_id: &str, job_id: &str) -> LoadTaskInfo {
    let job = LoadJobInfo {
        job_id: job_id.to_string(),
        source_path: "file://source".to_string(),
        target_path: "/mnt/source".to_string(),
        replicas: 1,
        block_size: 4096,
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
        source_path: "file://source".to_string(),
        target_path: "/mnt/source".to_string(),
        create_time: 0,
    }
}

#[test]
fn submit_load_job_returns_before_ufs_planning() -> CommonResult<()> {
    let (job_manager, rt, missing_source) = new_job_manager("async-missing-source")?;

    rt.block_on(async move {
        let result = job_manager
            .submit_load_job(LoadJobCommand::builder(missing_source).build())
            .await?;

        assert!(!result.job_id.is_empty());
        assert_eq!(result.state, JobTaskState::Pending);

        for _ in 0..50 {
            let status = job_manager.get_job_status(&result.job_id)?;
            if status.state == JobTaskState::Failed {
                assert!(
                    !status.progress.message.is_empty(),
                    "failed job should keep a diagnostic message"
                );
                assert!(
                    status.progress.update_time > 0,
                    "failed job should update progress timestamp: {}",
                    status.progress.update_time
                );
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let status = job_manager.get_job_status(&result.job_id)?;
        panic!(
            "load job did not fail in background, state={:?}, message={}",
            status.state, status.progress.message
        );
    })
}

#[test]
fn empty_directory_load_completes_without_worker_reports() -> CommonResult<()> {
    let (job_manager, rt, missing_source) = new_job_manager("async-empty-dir")?;
    let source = missing_source.replace("/missing-file", "/empty-dir");
    let local_source = source
        .strip_prefix("file://")
        .expect("test UFS path uses file scheme");
    std::fs::create_dir_all(local_source)?;

    rt.block_on(async move {
        let result = job_manager
            .submit_load_job(LoadJobCommand::builder(source).build())
            .await?;

        assert!(!result.job_id.is_empty());
        assert_eq!(result.state, JobTaskState::Pending);

        for _ in 0..50 {
            let status = job_manager.get_job_status(&result.job_id)?;
            if status.state == JobTaskState::Completed {
                assert_eq!(status.progress.message, "No load tasks to dispatch");
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let status = job_manager.get_job_status(&result.job_id)?;
        panic!(
            "empty directory load did not complete, state={:?}, message={}",
            status.state, status.progress.message
        );
    })
}

#[test]
fn late_task_report_does_not_change_terminal_job_state() -> CommonResult<()> {
    let job_id = "job-terminal";
    let task_id = "job-terminal_run_1_task_0";
    let command = LoadJobCommand::builder("file://source").build();
    let mount = MountInfo::default();
    let mut job = JobContext::with_conf(
        &command,
        job_id.to_string(),
        "file://source".to_string(),
        "/mnt/source".to_string(),
        &mount,
        &ClientConf::default(),
        1,
    );
    job.add_task(load_task(task_id, job_id));

    let store = JobStore::new();
    store.insert(job_id.to_string(), job);
    store.update_state(job_id, JobTaskState::Failed, "dispatch failed")?;

    store.update_progress(
        job_id,
        task_id,
        JobTaskProgress {
            state: JobTaskState::Completed,
            loaded_size: 1,
            total_size: 1,
            update_time: 1,
            message: "late completed report".to_string(),
        },
    )?;

    let job = store.get(job_id).expect("job exists");
    assert_eq!(job.state.state::<JobTaskState>(), JobTaskState::Failed);
    assert_eq!(job.progress.message, "dispatch failed");
    Ok(())
}

#[test]
fn stale_task_report_for_running_job_is_ignored() -> CommonResult<()> {
    let job_id = "job-running-stale-report";
    let current_task_id = "job-running-stale-report_run_2_task_0";
    let stale_task_id = "job-running-stale-report_run_1_task_0";
    let command = LoadJobCommand::builder("file://source").build();
    let mount = MountInfo::default();
    let mut job = JobContext::with_conf(
        &command,
        job_id.to_string(),
        "file://source".to_string(),
        "/mnt/source".to_string(),
        &mount,
        &ClientConf::default(),
        2,
    );
    job.add_task(load_task(current_task_id, job_id));

    let store = JobStore::new();
    store.insert(job_id.to_string(), job);

    store.update_progress(
        job_id,
        stale_task_id,
        JobTaskProgress {
            state: JobTaskState::Completed,
            loaded_size: 1,
            total_size: 1,
            update_time: 1,
            message: "stale completed report".to_string(),
        },
    )?;

    let job = store.get(job_id).expect("job exists");
    assert_eq!(job.state.state::<JobTaskState>(), JobTaskState::Loading);
    assert_eq!(job.tasks.len(), 1);
    assert!(job.tasks.contains_key(current_task_id));
    Ok(())
}

#[test]
fn worker_cancel_marks_running_context_canceled() {
    let store = TaskStore::new();
    let context = store.insert(load_task("task-cancel", "job-cancel"));

    let canceled = store.cancel("job-cancel");

    assert_eq!(canceled.len(), 1);
    assert_eq!(context.get_state(), JobTaskState::Canceled);
    assert!(store.get("task-cancel").is_none());
}

#[test]
fn canceled_worker_context_ignores_late_completion_progress() {
    let context = TaskContext::new(load_task("task-cancel-progress", "job-cancel-progress"));
    context.update_state(JobTaskState::Canceled, "canceled by master");

    let progress = context.update_progress(1, 1, true);

    assert_eq!(progress.state, JobTaskState::Canceled);
    assert_eq!(context.get_state(), JobTaskState::Canceled);
}
