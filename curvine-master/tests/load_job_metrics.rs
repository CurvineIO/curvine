// Copyright 2026 OPPO.
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

//! Load job metrics recorded by the master job store (#224).
//!
//! The metrics are process-global, so this binary holds only these tests and
//! serializes them; every assertion compares exact deltas.

use curvine_config::ClientConf;
use curvine_error::FsResult;
use curvine_master::master::{JobContext, JobStore};
use curvine_master::Master;
use curvine_metrics::Metrics;
use curvine_model::{
    JobTaskProgress, JobTaskState, JobTaskType, LoadJobCommand, LoadTaskInfo, MountInfo,
    WorkerAddress,
};
use std::sync::{Mutex, MutexGuard};

static TEST_LOCK: Mutex<()> = Mutex::new(());

/// Serializes the tests and registers the master metrics before they are read.
fn serial() -> MutexGuard<'static, ()> {
    let guard = TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    Master::init_test_metrics();
    guard
}

fn job_context(
    job_id: &str,
    kind: JobTaskType,
    source_path: &str,
    target_path: &str,
) -> JobContext {
    JobContext::with_conf(
        &LoadJobCommand::builder(source_path).build(),
        job_id.to_string(),
        source_path.to_string(),
        target_path.to_string(),
        &MountInfo::default(),
        &ClientConf::default(),
        1,
        kind,
    )
}

fn load_task(job: &JobContext, task_id: &str) -> LoadTaskInfo {
    LoadTaskInfo {
        job: job.info.clone(),
        task_id: task_id.to_string(),
        worker: WorkerAddress::default(),
        source_path: format!("{}/{}", job.info.source_path, task_id),
        target_path: format!("{}/{}", job.info.target_path, task_id),
        create_time: 0,
        source_read_plan_json: String::new(),
        transfer_report: None,
    }
}

fn progress(state: JobTaskState, loaded_size: i64) -> JobTaskProgress {
    JobTaskProgress {
        state,
        loaded_size,
        total_size: 200,
        update_time: 1,
        message: String::new(),
    }
}

fn counter(name: &str, labels: &[&str]) -> i64 {
    Metrics::get(name)
        .unwrap()
        .try_into_counter_vec()
        .unwrap()
        .with_label_values(labels)
        .get()
}

fn finished(kind: &str, state: &str) -> i64 {
    counter("load_job_finished_total", &[kind, state])
}

fn finished_durations(kind: &str, state: &str) -> u64 {
    Metrics::get("load_job_duration_ms")
        .unwrap()
        .try_into_histogram_vec()
        .unwrap()
        .with_label_values(&[kind, state])
        .get_sample_count()
}

fn tasks_finished(kind: &str, state: &str) -> i64 {
    counter("load_task_finished_total", &[kind, state])
}

fn loaded_bytes(kind: &str) -> i64 {
    counter("load_task_loaded_bytes_total", &[kind])
}

#[test]
fn job_context_keeps_the_given_kind() {
    let _serial = serial();

    let load = job_context("kind-load", JobTaskType::Load, "s3://bucket/data", "/data");
    assert_eq!(load.kind, JobTaskType::Load);

    let export = job_context(
        "kind-export",
        JobTaskType::Export,
        "/data",
        "s3://bucket/data",
    );
    assert_eq!(export.kind, JobTaskType::Export);
}

#[test]
fn finished_runs_are_counted_once_by_kind_and_state() -> FsResult<()> {
    let _serial = serial();
    let store = JobStore::new();
    let load_failed = finished("load", "failed");
    let load_failed_durations = finished_durations("load", "failed");
    let load_canceled = finished("load", "canceled");
    let export_canceled = finished("export", "canceled");

    let job_id = "finished-load";
    store.insert(
        job_id.to_string(),
        job_context(job_id, JobTaskType::Load, "s3://bucket/load", "/load"),
    );
    store.update_state(job_id, JobTaskState::Loading, "planned")?;
    assert_eq!(finished("load", "failed"), load_failed);

    store.update_state(job_id, JobTaskState::Failed, "dispatch failed")?;
    assert_eq!(finished("load", "failed"), load_failed + 1);
    assert_eq!(
        finished_durations("load", "failed"),
        load_failed_durations + 1
    );

    // Moving between terminal states is not a second finish.
    store.update_state(job_id, JobTaskState::Canceled, "canceled late")?;
    assert_eq!(finished("load", "canceled"), load_canceled);
    assert_eq!(finished("load", "failed"), load_failed + 1);

    let job_id = "finished-export";
    let job = job_context(job_id, JobTaskType::Export, "/export", "s3://bucket/export");
    let run_id = job.run_id;
    store.insert(job_id.to_string(), job);
    assert!(store.update_state_if_run(job_id, run_id, JobTaskState::Canceled, "canceled"));
    assert_eq!(finished("export", "canceled"), export_canceled + 1);

    // A run that is no longer running is not updated, so it is not counted again.
    assert!(!store.update_state_if_run(job_id, run_id, JobTaskState::Failed, "late"));
    assert_eq!(finished("export", "canceled"), export_canceled + 1);
    Ok(())
}

#[test]
fn task_reports_count_copied_bytes_and_terminal_states() -> FsResult<()> {
    let _serial = serial();
    let store = JobStore::new();
    let bytes = loaded_bytes("export");
    let completed = tasks_finished("export", "completed");
    let failed = tasks_finished("export", "failed");
    let job_failed = finished("export", "failed");

    let job_id = "report-export";
    let mut job = job_context(job_id, JobTaskType::Export, "/report", "s3://bucket/report");
    let first = load_task(&job, "task-0");
    let second = load_task(&job, "task-1");
    job.add_task(first);
    job.add_task(second);
    store.insert(job_id.to_string(), job);

    store.update_progress(job_id, "task-0", progress(JobTaskState::Loading, 100))?;
    assert_eq!(loaded_bytes("export"), bytes + 100);

    // A retried task starts over; the drop is not subtracted.
    store.update_progress(job_id, "task-0", progress(JobTaskState::Loading, 60))?;
    assert_eq!(loaded_bytes("export"), bytes + 100);

    store.update_progress(job_id, "task-0", progress(JobTaskState::Completed, 200))?;
    assert_eq!(loaded_bytes("export"), bytes + 240);
    assert_eq!(tasks_finished("export", "completed"), completed + 1);

    // A repeated terminal report is not a second finish.
    store.update_progress(job_id, "task-0", progress(JobTaskState::Completed, 200))?;
    assert_eq!(loaded_bytes("export"), bytes + 240);
    assert_eq!(tasks_finished("export", "completed"), completed + 1);

    store.update_progress(job_id, "task-1", progress(JobTaskState::Failed, 0))?;
    assert_eq!(tasks_finished("export", "failed"), failed + 1);
    assert_eq!(finished("export", "failed"), job_failed + 1);

    // The job store ignores reports for a finished run.
    store.update_progress(job_id, "task-1", progress(JobTaskState::Completed, 200))?;
    assert_eq!(loaded_bytes("export"), bytes + 240);
    assert_eq!(tasks_finished("export", "completed"), completed + 1);
    assert_eq!(finished("export", "failed"), job_failed + 1);
    Ok(())
}
