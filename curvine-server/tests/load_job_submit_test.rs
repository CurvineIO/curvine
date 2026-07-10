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
    MountOptions, OpenFlags, StorageType, TtlAction, WorkerAddress, WorkerInfo,
};
use curvine_common::utils::CommonUtils;
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
    let (job_manager, rt, missing_source, _) = new_job_manager_with_fs(name)?;
    Ok((job_manager, rt, missing_source))
}

fn new_job_manager_with_fs(
    name: &str,
) -> CommonResult<(Arc<JobManager>, Arc<Runtime>, String, MasterFilesystem)> {
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
    let master_fs_ref = master_fs.clone();

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

    Ok((
        job_manager,
        rt,
        format!("{}/missing-file", ufs_root),
        master_fs_ref,
    ))
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
fn fs_mode_cv_path_load_uses_ufs_source_when_metadata_is_ufs_only() -> CommonResult<()> {
    let (job_manager, rt, missing_source, master_fs) =
        new_job_manager_with_fs("fs-cv-load-ufs-only")?;
    let source = missing_source.replace("/missing-file", "/ufs-only-file");
    let local_source = source
        .strip_prefix("file://")
        .expect("test UFS path uses file scheme");
    if let Some(parent) = std::path::Path::new(local_source).parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(local_source, b"load-data")?;

    let cv_path = "/mnt/ufs-only-file";
    let source_path = curvine_common::fs::Path::from_str(&source)?;
    let (_, mount) = job_manager
        .get_mnt(&source_path)?
        .expect("test source should match mount");
    let sync_opts = mount.info.get_sync_opts(&ClientConf::default(), 1, 9);
    master_fs.create_with_opts(cv_path, sync_opts, OpenFlags::new_create())?;
    let expected_job_id = CommonUtils::create_job_id(&source);

    rt.block_on(async {
        let result = job_manager
            .submit_load_job(LoadJobCommand::builder(cv_path).build())
            .await?;

        assert_eq!(result.job_id, expected_job_id);
        assert_eq!(result.target_path, cv_path);

        let status = job_manager.get_job_status(&result.job_id)?;
        assert_eq!(status.source_path, source);
        assert_eq!(status.target_path, cv_path);
        Ok(())
    })
}

#[test]
fn cv_path_load_without_ufs_only_metadata_is_rejected() -> CommonResult<()> {
    let (job_manager, rt, _missing_source, master_fs) =
        new_job_manager_with_fs("cv-load-rejects-export")?;
    let cv_path = "/mnt/native-file";
    let create_opts = curvine_common::state::CreateFileOptsBuilder::new()
        .create_parent(true)
        .build();
    master_fs.create_with_opts(cv_path, create_opts, OpenFlags::new_create())?;

    rt.block_on(async {
        let err = job_manager
            .submit_load_job(LoadJobCommand::builder(cv_path).build())
            .await
            .expect_err("ordinary CV load should not fall back to CV-to-UFS export");
        assert!(
            err.to_string().contains("requires UFS-only metadata"),
            "unexpected error: {}",
            err
        );
        Ok(())
    })
}

#[test]
fn direct_export_task_keeps_cv_source_for_fs_mode_journal_sync() -> CommonResult<()> {
    let (job_manager, rt, _missing_source, master_fs) = new_job_manager_with_fs("direct-export")?;
    let cv_path = "/mnt/export-dir";
    master_fs.mkdir(cv_path, true)?;
    let expected_target = job_manager
        .get_mnt(&curvine_common::fs::Path::from_str(cv_path)?)?
        .expect("test CV path should match mount")
        .0
        .clone_uri();
    let (_, mount) = job_manager
        .get_mnt(&curvine_common::fs::Path::from_str(cv_path)?)?
        .expect("test CV path should match mount");

    let command = LoadJobCommand::builder(cv_path).build();
    let expected_job_id = CommonUtils::create_job_id(cv_path);

    rt.block_on(async {
        let result = job_manager
            .create_runner()
            .submit_export_task(command, mount.info.clone())
            .await?;

        assert_eq!(result.job_id, expected_job_id);
        assert_eq!(result.target_path, expected_target);
        assert_eq!(result.state, JobTaskState::Completed);
        Ok(())
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
