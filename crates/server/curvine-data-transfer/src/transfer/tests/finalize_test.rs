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

use curvine_model::{
    ExtendedBlock, FileType, LocatedBlock, StorageType, TaskBlockLocation, TransferProgress,
    TransferTaskRecord, TransferTaskState, WorkerAddress,
};

use super::dispatch::{commit_block_for, converged_segment_targets, reported_block_locations};

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
        block_locations: Vec::new(),
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

/// Regression guard for the shell-file defect: segment tasks write straight to
/// workers and never report locations to the master, so the master layout query
/// for a pre-allocated block carries no locations at all. Finalize must commit
/// the worker ids the completing tasks reported instead of the (empty) ones
/// from the layout query, otherwise the file completes as an unreadable shell
/// (is_complete = true, every block location list empty).
#[test]
fn reported_task_locations_fill_empty_layout_blocks() {
    // Layout as the master sees it: pre-allocated blocks, no locations yet.
    let unlocated = located(1001, &[]);
    assert!(commit_block_for(&unlocated, None).locations.is_empty());

    let reported = [(1001i64, vec![7u32, 8, 9])].into_iter().collect();
    let commit = commit_block_for(&unlocated, Some(&reported));
    assert_eq!(commit.block_id, 1001);
    assert_eq!(commit.block_len, 128);
    let worker_ids: Vec<u32> = commit.locations.iter().map(|l| l.worker_id).collect();
    assert_eq!(worker_ids, vec![7, 8, 9]);
}

#[test]
fn layout_locations_take_precedence_over_reports() {
    // A block that the layout query already located (previously committed by
    // a client) must not be rewritten from stale task reports.
    let located = located(1002, &[7]);
    let reported = [(1002i64, vec![9])].into_iter().collect();
    let commit = commit_block_for(&located, Some(&reported));
    let worker_ids: Vec<u32> = commit.locations.iter().map(|l| l.worker_id).collect();
    assert_eq!(worker_ids, vec![7]);
}

#[test]
fn reported_block_locations_aggregate_completed_segment_tasks() {
    let tasks = vec![
        completed_with_locations(
            "/t/one",
            vec![TaskBlockLocation {
                block_id: 1,
                worker_ids: vec![5],
            }],
        ),
        task("/t/one", segment_plan(0), TransferTaskState::Completed),
        // Failed/pending/running reports never contribute.
        task("/t/two", segment_plan(0), TransferTaskState::Failed),
        // Non-segment plans never contribute.
        task("/t/plain", String::new(), TransferTaskState::Completed),
    ];
    let reported = reported_block_locations(&tasks);
    assert_eq!(reported.len(), 1);
    assert_eq!(reported.get("/t/one").unwrap().get(&1), Some(&vec![5u32]));
}

fn completed_with_locations(target: &str, locations: Vec<TaskBlockLocation>) -> TransferTaskRecord {
    let mut record = task(target, segment_plan(0), TransferTaskState::Completed);
    record.block_locations = locations;
    record
}

fn located(block_id: i64, worker_ids: &[u32]) -> LocatedBlock {
    LocatedBlock {
        block: ExtendedBlock {
            id: block_id,
            len: 128,
            storage_type: StorageType::Disk,
            file_type: FileType::File,
            alloc_opts: None,
        },
        locs: worker_ids
            .iter()
            .map(|worker_id| WorkerAddress {
                worker_id: *worker_id,
                hostname: format!("worker-{worker_id}"),
                ip_addr: format!("10.0.0.{worker_id}"),
                rpc_port: 8997,
                web_port: 9002,
            })
            .collect(),
        has_spdk: false,
    }
}
