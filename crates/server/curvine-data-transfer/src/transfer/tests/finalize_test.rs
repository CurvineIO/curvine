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

//! Segment convergence decisions (design §14 T4): which targets may be
//! finalized, and which plan-time UFS identity convergence may stamp on them.
//!
//! Regression guard for the production defect where directory-loaded large
//! files were finalized without a `ufs_mtime`, leaving the cached copy
//! permanently unreadable (`cv_valid` rejects `ufs_mtime == 0`) and re-copied
//! by every incremental load.

use curvine_model::{TransferProgress, TransferTaskRecord, TransferTaskState};

use super::scheduler::converged_segment_targets;

fn segment_plan(mtime: i64) -> String {
    serde_json::json!({
        "segment": {
            "offset": 0,
            "length": 128,
            "total_len": 512,
            "block_index": 0,
            "source_mtime": mtime,
        }
    })
    .to_string()
}

fn task(target: &str, plan: String, state: TransferTaskState) -> TransferTaskRecord {
    TransferTaskRecord {
        job_id: "job".to_string(),
        run_id: 1,
        task_id: format!("{target}-{}", plan.len()),
        attempt_id: 1,
        source_path: "oss://bucket/source".to_string(),
        target_path: target.to_string(),
        worker_id: 1,
        worker_session_id: "s".to_string(),
        source_read_plan_json: plan,
        report_target_json: "{}".to_string(),
        state,
        progress: TransferProgress::default(),
        estimated_size: 0,
        retry_count: 0,
        replace_target: false,
        accepted_at_ms: 0,
        first_byte_at_ms: 0,
        attempt_started_at: 0,
        last_report_at: 0,
        stale_deadline_at: 0,
        updated_at: 0,
    }
}

#[test]
fn only_fully_completed_segment_targets_are_finalized() {
    let mtime = 1_789_595_866_000i64;
    let tasks = vec![
        // Completed pair: finalize, with the agreed plan-time mtime.
        task(
            "/t/ready",
            segment_plan(mtime),
            TransferTaskState::Completed,
        ),
        task(
            "/t/ready",
            segment_plan(mtime),
            TransferTaskState::Completed,
        ),
        // One segment still pending: must not finalize (holes would cement).
        task(
            "/t/partial",
            segment_plan(mtime),
            TransferTaskState::Completed,
        ),
        task(
            "/t/partial",
            segment_plan(mtime),
            TransferTaskState::Pending,
        ),
        // Batch and plain targets are not finalized by this path.
        task(
            "/t/batch",
            "{\"batch\":{\"batch\":[]}}".to_string(),
            TransferTaskState::Completed,
        ),
        task("/t/plain", String::new(), TransferTaskState::Completed),
    ];

    let targets = converged_segment_targets(&tasks);
    assert_eq!(targets, vec![("/t/ready".to_string(), Some(mtime))]);
}

#[test]
fn a_target_without_an_agreed_mtime_is_finalized_unstamped() {
    // The plan pins no observation (UFS fingerprint unavailable at planning):
    // convergence must still complete the file, but leave it cache-invalid
    // rather than claim a freshness nobody verified.
    let tasks = vec![
        task("/t/unknown", segment_plan(0), TransferTaskState::Completed),
        task("/t/unknown", segment_plan(0), TransferTaskState::Completed),
    ];
    assert_eq!(
        converged_segment_targets(&tasks),
        vec![("/t/unknown".to_string(), None)]
    );

    // Plans disagreeing means the source moved mid-copy: no single mtime
    // describes the cached bytes, so it stays unstamped too.
    let moved = vec![
        task("/t/moved", segment_plan(10), TransferTaskState::Completed),
        task("/t/moved", segment_plan(11), TransferTaskState::Completed),
    ];
    assert_eq!(
        converged_segment_targets(&moved),
        vec![("/t/moved".to_string(), None)]
    );
}

#[test]
fn group_manifests_contribute_their_plan_time_mtime() {
    let mtime = 1_789_595_866_000i64;
    let group = serde_json::json!({
        "segment_group": {
            "start_offset": 0,
            "end_offset": 256,
            "total_len": 512,
            "first_block_index": 0,
            "segment_bytes": 128,
            "source_len": 512,
            "source_mtime": mtime,
            "source_version": "",
        }
    })
    .to_string();
    let tasks = vec![task("/t/grouped", group, TransferTaskState::Completed)];
    assert_eq!(
        converged_segment_targets(&tasks),
        vec![("/t/grouped".to_string(), Some(mtime))]
    );
}
