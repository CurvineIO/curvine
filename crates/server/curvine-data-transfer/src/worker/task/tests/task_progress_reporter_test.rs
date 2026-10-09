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

use std::collections::HashMap;
use std::time::Duration;

use curvine_job_client::TransferTaskProgressReport;
use curvine_model::{JobTaskProgress, JobTaskState, TransferTaskReportInfo};

use super::task_progress_reporter::{
    insert_latest_progress, progress_report_batch_delay, progress_report_queue_capacity,
    ProgressReport,
};

fn progress_report(task_id: &str, loaded_size: i64) -> ProgressReport {
    ProgressReport {
        report: TransferTaskProgressReport {
            job_id: "job".to_string(),
            task_id: task_id.to_string(),
            info: TransferTaskReportInfo {
                run_id: 1,
                attempt_id: 1,
                worker_id: 7,
                worker_session_id: "session".to_string(),
                report_target: String::new(),
                report_endpoints: Vec::new(),
                attempt_started_at_ms: 0,
                resume_loaded_size: 0,
            },
            progress: JobTaskProgress {
                state: JobTaskState::Loading,
                loaded_size,
                total_size: 100,
                update_time: loaded_size,
                message: String::new(),
                accepted_at_ms: 0,
                first_byte_at_ms: 0,
            },
        },
        log_context: task_id.to_string(),
    }
}

#[test]
fn batch_coalesces_latest_progress_per_task_attempt() {
    let mut reports = HashMap::new();
    insert_latest_progress(&mut reports, progress_report("task-1", 10));
    insert_latest_progress(&mut reports, progress_report("task-2", 20));
    insert_latest_progress(&mut reports, progress_report("task-1", 30));

    assert_eq!(reports.len(), 2);
    assert_eq!(
        reports
            .values()
            .find(|report| report.report.task_id == "task-1")
            .unwrap()
            .report
            .progress
            .loaded_size,
        30
    );
}

#[test]
fn batch_delay_stays_within_progress_interval() {
    assert_eq!(
        progress_report_batch_delay(Duration::from_millis(8)),
        Duration::from_millis(8)
    );
    assert_eq!(
        progress_report_batch_delay(Duration::from_secs(10)),
        Duration::from_millis(25)
    );
}

#[test]
fn batch_queue_capacity_is_bounded() {
    assert_eq!(progress_report_queue_capacity(100), 1_024);
    assert_eq!(progress_report_queue_capacity(2_048), 2_048);
    assert_eq!(progress_report_queue_capacity(usize::MAX), 10_000);
}
