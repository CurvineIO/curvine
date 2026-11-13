// Copyright 2026 OPPO
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

//! c2 F3: `SubtreeLocationPlan` budget and coverage unit contracts.
//!
//! One shared counter charges every actual block entry and every
//! worker-location entry against `SUBTREE_LOCATION_LIMIT`, deduplicating only
//! at lock-key level (never before counting). The bounded reads return either a
//! complete set or `None` - never a truncated success.
//!
//! These are unit tests of the plan type itself: store rows are seeded directly
//! through the existing store API, which is the unit fixture; the business-level
//! report/completion interleavings live in the location test files.

use super::super::subtree_write_plan::{SubtreeLocationPlan, SUBTREE_LOCATION_LIMIT};
use super::super::{InodeLockMode, InodeLockRequest};
use crate::master::fs::MasterFilesystem;
use crate::master::journal::JournalSystem;
use crate::master::meta::BlockMeta;
use crate::master::Master;
use curvine_config::ClusterConf;
use curvine_model::BlockLocation;
use curvine_runtime::common::Utils;

fn test_fs(name: &str) -> MasterFilesystem {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.change_test_meta_dir(format!("location-plan-{}-{}", name, Utils::rand_str(8)));
    let fs = JournalSystem::fs_only_for_test(&conf).expect("metadata filesystem");
    // add_block needs a live worker to place the block on.
    let mut worker = curvine_model::WorkerInfo::new(
        curvine_model::WorkerAddress {
            worker_id: 1,
            ..Default::default()
        },
        1,
    );
    worker.status = curvine_model::WorkerStatus::Live;
    worker.capacity = i64::MAX / 4;
    worker.available = i64::MAX / 4;
    fs.add_test_worker(worker);
    fs
}

/// Creates one file and returns (file_id, request) for it.
fn file_request(fs: &MasterFilesystem, path: &str) -> (i64, InodeLockRequest) {
    fs.create(path, true).expect("create file");
    let id = fs.file_status(path).expect("status").id;
    (id, InodeLockRequest::write(0, id))
}

/// Writes `workers.len()` location rows for one block through the store.
fn seed_locations(fs: &MasterFilesystem, block_id: i64, workers: &[u32]) {
    let handle = fs.fs_dir();
    let view = handle.view().expect("namespace view");
    for worker in workers {
        view.store
            .add_block_location(block_id, BlockLocation::with_id(*worker))
            .expect("seed location");
    }
}

/// Allocates one block on `path` without completing it (zero-length is fine for
/// the plan, which only walks block ids).
fn one_block(fs: &MasterFilesystem, path: &str) -> (i64, InodeLockRequest) {
    let (id, request) = file_request(fs, path);
    let block = fs
        .add_block(
            path,
            None,
            curvine_model::ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .expect("allocate a block");
    seed_locations(fs, block.id, &[1]);
    (id, request)
}

fn plan_of(fs: &MasterFilesystem, requests: &[InodeLockRequest]) -> Option<SubtreeLocationPlan> {
    let handle = fs.fs_dir();
    let view = handle.view().expect("namespace view");
    SubtreeLocationPlan::capture(&view, requests).expect("capture must not error")
}

#[test]
fn many_files_accumulating_past_the_budget_return_none() {
    let fs = test_fs("budget-accumulate");
    // Each file: 1 block + 1 location = 2 charged entries. (LIMIT/2 + 1) files
    // exceed the budget on the last file's block.
    let files = SUBTREE_LOCATION_LIMIT / 2 + 1;
    let mut requests = Vec::with_capacity(files);
    for index in 0..files {
        let path = format!("/f{index}");
        let (_, request) = one_block(&fs, &path);
        requests.push(request);
    }
    assert!(
        plan_of(&fs, &requests).is_none(),
        "{files} files x (1 block + 1 location) exceed the {SUBTREE_LOCATION_LIMIT} budget"
    );
}

#[test]
fn a_single_file_over_the_budget_returns_none() {
    let fs = test_fs("budget-single");
    let path = "/big";
    fs.create(path, true).expect("create file");
    let id = fs.file_status(path).expect("status").id;

    // `add_block` allocates "the next" block for a writer, so building a
    // thousand-block file through the public RPC is not a unit fixture. The
    // store API is the unit fixture here (as the review allows): rewrite the
    // inode with LIMIT+1 block metas, each with one location row.
    {
        let handle = fs.fs_dir();
        let view = handle.view().expect("namespace view");
        let mut inode = view
            .store
            .get_inode(id, None)
            .expect("inode")
            .expect("file row");
        let file = inode.as_file_mut().expect("file");
        let mut blocks = Vec::with_capacity(SUBTREE_LOCATION_LIMIT + 1);
        for seq in 1..=(SUBTREE_LOCATION_LIMIT + 1) as i64 {
            let block_id =
                crate::master::meta::InodeId::create_block_id(id, seq).expect("block id");
            blocks.push(BlockMeta::new(block_id, 4096));
        }
        file.blocks = blocks;
        let mut batch = view.store_ref().new_batch();
        batch.write_inode(&inode).expect("write the widened inode");
        batch.commit().expect("commit");
        for seq in 1..=(SUBTREE_LOCATION_LIMIT + 1) as i64 {
            let block_id =
                crate::master::meta::InodeId::create_block_id(id, seq).expect("block id");
            view.store
                .add_block_location(block_id, BlockLocation::with_id(1))
                .expect("seed location");
        }
    }

    let request = InodeLockRequest::write(0, id);
    let plan = plan_of(&fs, &[request]);
    assert!(
        plan.is_none(),
        "one file with {SUBTREE_LOCATION_LIMIT}+1 blocks (each +1 location) must \
         exceed the shared budget"
    );
    // The inode itself is fine; only the plan refuses.
    assert_eq!(fs.file_status(path).expect("status").id, id);
}

#[test]
fn one_block_with_many_location_rows_is_charged_per_row() {
    let fs = test_fs("budget-multirow");
    let (id, request) = one_block(&fs, "/m");
    // Two more worker rows on the SAME block: each row is one entry.
    seed_locations(
        &fs,
        {
            // Reuse the block id allocated by one_block: read it from the file.
            let handle = fs.fs_dir();
            let view = handle.view().expect("namespace view");
            view.store
                .get_inode(id, None)
                .expect("inode")
                .expect("file row")
                .as_file_ref()
                .expect("file")
                .block_ids()[0]
        },
        &[2, 3],
    );

    let plan = plan_of(&fs, &[request]).expect("three rows are well inside the budget");
    assert_eq!(plan.block_ids().len(), 1, "one block");
    assert!(
        plan.worker_ids().contains(&1)
            && plan.worker_ids().contains(&2)
            && plan.worker_ids().contains(&3),
        "every worker row must be sampled: {:?}",
        plan.worker_ids()
    );

    // Coverage sees the same rows.
    let handle = fs.fs_dir();
    let view = handle.view().expect("namespace view");
    assert!(
        SubtreeLocationPlan::covers(&plan, &view, &[request]).expect("covers"),
        "the unchanged set must be covered"
    );
}

#[test]
fn exactly_at_the_budget_still_produces_a_plan() {
    let fs = test_fs("budget-boundary");
    // LIMIT/2 files x 2 entries == LIMIT exactly: the last entry lands on the
    // boundary and must still be accepted.
    let files = SUBTREE_LOCATION_LIMIT / 2;
    let mut requests = Vec::with_capacity(files);
    for index in 0..files {
        let path = format!("/e{index}");
        let (_, request) = one_block(&fs, &path);
        requests.push(request);
    }
    let plan =
        plan_of(&fs, &requests).expect("a subtree exactly at the budget must produce a plan");
    assert_eq!(
        plan.block_ids().len(),
        files,
        "every file's block must be in the plan"
    );

    // Covers while unchanged; fails once a new location appears.
    let handle = fs.fs_dir();
    let view = handle.view().expect("namespace view");
    assert!(
        SubtreeLocationPlan::covers(&plan, &view, &requests).expect("covers"),
        "the unchanged set must be covered at the boundary"
    );
    seed_locations(&fs, plan.block_ids()[0], &[9]);
    assert!(
        !SubtreeLocationPlan::covers(&plan, &view, &requests).expect("covers"),
        "a worker row added after the sample must break coverage"
    );
}

#[test]
fn empty_and_zero_location_blocks_stay_planable() {
    let fs = test_fs("budget-empty");
    // A file with no blocks at all, and one with a block that has no location
    // rows: both contribute (almost) nothing to the budget.
    let (empty_id, empty_request) = file_request(&fs, "/no-blocks");
    let (rowless_id, rowless_request) = file_request(&fs, "/no-rows");
    fs.add_block(
        "/no-rows",
        None,
        curvine_model::ClientAddress::default(),
        vec![],
        vec![],
        0,
        None,
    )
    .expect("allocate a block without location rows");

    let requests = [empty_request, rowless_request];
    let plan = plan_of(&fs, &requests).expect("no rows means no budget pressure");
    // The rowless block is still sampled (its entry is charged), but no worker.
    assert!(
        plan.worker_ids().is_empty(),
        "no location rows means no workers: {:?}",
        plan.worker_ids()
    );
    let handle = fs.fs_dir();
    let view = handle.view().expect("namespace view");
    assert!(
        SubtreeLocationPlan::covers(&plan, &view, &requests).expect("covers"),
        "the empty set must be covered"
    );
    // Ids stay addressable: this is a plan over existing files only.
    assert_eq!(fs.file_status("/no-blocks").expect("status").id, empty_id);
    assert_eq!(fs.file_status("/no-rows").expect("status").id, rowless_id);
}
