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
use super::super::InodeLockRequest;
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

/// G4a: ONE block whose own location set exceeds the budget. The earlier case
/// with three rows proves per-row charging, not the over-limit rejection, so
/// this one puts 1025 rows on a single block.
#[test]
fn one_block_with_more_location_rows_than_the_budget_returns_none() {
    let fs = test_fs("budget-one-block-many-rows");
    let (id, request) = file_request(&fs, "/one");
    let block = fs
        .add_block(
            "/one",
            None,
            curvine_model::ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .expect("allocate a block");
    // SUBTREE_LOCATION_LIMIT + 1 rows on the SAME block.
    for worker in 1..=(SUBTREE_LOCATION_LIMIT as u32 + 1) {
        seed_locations(&fs, block.id, &[worker]);
    }
    let plan = plan_of(&fs, &[request]);
    assert!(
        plan.is_none(),
        "a single block with {} location rows must exceed the {SUBTREE_LOCATION_LIMIT} budget",
        SUBTREE_LOCATION_LIMIT + 1
    );
    // The inode and its block are still there; only the plan refuses.
    assert_eq!(fs.file_status("/one").expect("status").id, id);
}

/// G4b: the bounded reader itself, at a zero budget. An empty block must come
/// back as a complete empty set, a non-empty block must come back as over
/// limit - never as a truncated success.
#[test]
fn bounded_location_read_with_a_zero_budget_distinguishes_empty_from_over_limit() {
    let fs = test_fs("budget-zero");
    let (_, _request) = file_request(&fs, "/empty");
    let empty_block = fs
        .add_block(
            "/empty",
            None,
            curvine_model::ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .expect("allocate an empty block");
    let (_, _row_request) = file_request(&fs, "/with-row");
    let row_block = fs
        .add_block(
            "/with-row",
            None,
            curvine_model::ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .expect("allocate a block");
    seed_locations(&fs, row_block.id, &[1]);

    let handle = fs.fs_dir();
    let view = handle.view().expect("namespace view");
    let empty = view
        .store
        .get_locations_bounded(empty_block.id, 0)
        .expect("the bounded read must not error");
    assert_eq!(
        empty.as_ref().map(|locs| locs.len()),
        Some(0),
        "a zero budget on a block with no rows must return a complete empty set"
    );
    let over = view
        .store
        .get_locations_bounded(row_block.id, 0)
        .expect("the bounded read must not error");
    assert!(
        over.is_none(),
        "a zero budget on a block that has a row must exceed the limit, not truncate"
    );
}

// G4: `SubtreeLocationPlan::covers` against a changed live set (C2-FU-05 恢复).
/// G4c: the current block set grows after the sample. `covers` iterates the
/// inode's real blocks now, so a block that appeared after capture must make it
/// return false instead of silently passing.
#[test]
fn covers_rejects_a_block_added_after_the_sample() {
    let fs = test_fs("covers-block-grew");
    let (id, request) = one_block(&fs, "/grew");
    let plan = plan_of(&fs, &[request]).expect("one block is inside the budget");

    // Add a second block to the same inode after the sample.
    let block_id = {
        let handle = fs.fs_dir();
        let view = handle.view().expect("namespace view");
        let mut inode = view.store.get_inode(id, None).expect("inode").expect("row");
        let file = inode.as_file_mut().expect("file");
        let block_id = crate::master::meta::InodeId::create_block_id(id, 2).expect("block id");
        file.blocks.push(BlockMeta::new(block_id, 4096));
        let mut batch = view.store_ref().new_batch();
        batch.write_inode(&inode).expect("write the widened inode");
        batch.commit().expect("commit");
        view.store
            .add_block_location(block_id, BlockLocation::with_id(1))
            .expect("seed the new block's location");
        block_id
    };
    assert!(block_id > 0);

    let handle = fs.fs_dir();
    let view = handle.view().expect("namespace view");
    assert!(
        !SubtreeLocationPlan::covers(&plan, &view, &[request]).expect("covers"),
        "a block that appeared after the sample must break coverage"
    );
}

/// G4d: a worker that WAS sampled adds a location on a block the plan never saw.
/// The whole-call counter and the block membership check must both refuse it -
/// silently accepting would replay against a partially sampled set.
#[test]
fn covers_rejects_a_known_worker_expanding_onto_a_new_block() {
    let fs = test_fs("covers-cross-block");
    // Exactly at the budget: 512 blocks x (1 block entry + 1 location) = 1024.
    let files_blocks = SUBTREE_LOCATION_LIMIT / 2;
    let (id, request) = file_request(&fs, "/cross");
    {
        let handle = fs.fs_dir();
        let view = handle.view().expect("namespace view");
        let mut inode = view.store.get_inode(id, None).expect("inode").expect("row");
        let mut blocks = Vec::with_capacity(files_blocks);
        for seq in 1..=files_blocks as i64 {
            let block_id =
                crate::master::meta::InodeId::create_block_id(id, seq).expect("block id");
            blocks.push(BlockMeta::new(block_id, 4096));
        }
        inode.as_file_mut().expect("file").blocks = blocks;
        let mut batch = view.store_ref().new_batch();
        batch.write_inode(&inode).expect("write");
        batch.commit().expect("commit");
        for seq in 1..=files_blocks as i64 {
            let block_id =
                crate::master::meta::InodeId::create_block_id(id, seq).expect("block id");
            view.store
                .add_block_location(block_id, BlockLocation::with_id(1))
                .expect("seed location");
        }
    }
    let plan = plan_of(&fs, &[request]).expect("exactly at the budget still plans");
    assert_eq!(plan.block_ids().len(), files_blocks);

    // Worker 1 is already known to the plan; put it on one more block that the
    // plan never saw, so the set grows without introducing a new worker.
    let extra = {
        let handle = fs.fs_dir();
        let view = handle.view().expect("namespace view");
        let mut inode = view.store.get_inode(id, None).expect("inode").expect("row");
        let extra = crate::master::meta::InodeId::create_block_id(id, files_blocks as i64 + 1)
            .expect("block id");
        inode
            .as_file_mut()
            .expect("file")
            .blocks
            .push(BlockMeta::new(extra, 4096));
        let mut batch = view.store_ref().new_batch();
        batch.write_inode(&inode).expect("write");
        batch.commit().expect("commit");
        view.store
            .add_block_location(extra, BlockLocation::with_id(1))
            .expect("seed the extra block's location");
        extra
    };
    assert!(extra > 0);

    let handle = fs.fs_dir();
    let view = handle.view().expect("namespace view");
    assert!(
        !SubtreeLocationPlan::covers(&plan, &view, &[request]).expect("covers"),
        "a known worker on a block outside the sample must break coverage, not \
         pass because the worker id happens to be in the sampled set"
    );
}

/// G4e: the whole-call counter, isolated from the membership checks.
///
/// The 513-block case above already fails on block membership, so it cannot
/// tell whether the counter works at all. This one keeps BOTH sets identical to
/// the sample - same blocks, same workers (1 and 2 both appear) - and only grows
/// the number of rows by giving an existing worker-1 block a worker-2 row. The
/// row count goes 1024 -> 1025 while neither `block_ids` nor `worker_ids` gains
/// anything, so `false`/`None` can only come from the budget.
#[test]
fn covers_rejects_a_known_worker_growing_rows_on_an_existing_block() {
    let fs = test_fs("covers-row-growth");
    let files_blocks = SUBTREE_LOCATION_LIMIT / 2; // 512
    let (id, request) = file_request(&fs, "/rows");
    {
        let handle = fs.fs_dir();
        let view = handle.view().expect("namespace view");
        let mut inode = view.store.get_inode(id, None).expect("inode").expect("row");
        let mut blocks = Vec::with_capacity(files_blocks);
        for seq in 1..=files_blocks as i64 {
            let block_id =
                crate::master::meta::InodeId::create_block_id(id, seq).expect("block id");
            blocks.push(BlockMeta::new(block_id, 4096));
        }
        inode.as_file_mut().expect("file").blocks = blocks;
        let mut batch = view.store_ref().new_batch();
        batch.write_inode(&inode).expect("write");
        batch.commit().expect("commit");
        // One row per block, alternating workers so BOTH ids are sampled.
        for seq in 1..=files_blocks as i64 {
            let block_id =
                crate::master::meta::InodeId::create_block_id(id, seq).expect("block id");
            let worker = if seq % 2 == 0 { 1 } else { 2 };
            view.store
                .add_block_location(block_id, BlockLocation::with_id(worker))
                .expect("seed location");
        }
    }
    // 512 block entries + 512 location entries = exactly the budget.
    let plan = plan_of(&fs, &[request]).expect("exactly at the budget still plans");
    assert_eq!(plan.block_ids().len(), files_blocks);
    let sampled_workers = plan.worker_ids();
    assert!(
        sampled_workers.contains(&1) && sampled_workers.contains(&2),
        "fixture: both workers must already be sampled: {sampled_workers:?}"
    );

    // Grow the rows only: the block set and the worker set are unchanged.
    let grown_block = {
        let handle = fs.fs_dir();
        let view = handle.view().expect("namespace view");
        // seq 2 is worker 1; add worker 2 on the same block.
        let block_id = crate::master::meta::InodeId::create_block_id(id, 2).expect("block id");
        view.store
            .add_block_location(block_id, BlockLocation::with_id(2))
            .expect("add the second row");
        block_id
    };
    assert!(grown_block > 0);

    let handle = fs.fs_dir();
    let view = handle.view().expect("namespace view");
    assert!(
        !SubtreeLocationPlan::covers(&plan, &view, &[request]).expect("covers"),
        "1025 charged entries must break coverage even though every block and \
         every worker id is still inside the sample"
    );
    // `capture` sees the same 1025 entries and must refuse outright.
    assert!(
        SubtreeLocationPlan::capture(&view, &[request])
            .expect("capture must not error")
            .is_none(),
        "the same 1025-entry set must make capture return None"
    );
}
