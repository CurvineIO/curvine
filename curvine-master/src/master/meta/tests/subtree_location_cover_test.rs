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

//! G4: `SubtreeLocationPlan::covers` against a changed live set. The three
//! cases here isolate the three ways coverage may break - block membership,
//! worker-on-a-new-block, and the whole-call budget with both sets unchanged.

use super::*;

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
