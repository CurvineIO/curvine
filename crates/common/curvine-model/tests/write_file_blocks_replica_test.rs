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

use curvine_model::{
    BlockLocation, CommitBlock, ExtendedBlock, FileBlocks, FileStatus, FileType, LocatedBlock,
    StorageType, WorkerAddress, WriteFileBlocks,
};

fn blocks() -> WriteFileBlocks {
    let workers = (1..=3)
        .map(|worker_id| WorkerAddress {
            worker_id,
            ..WorkerAddress::default()
        })
        .collect();
    WriteFileBlocks::new(FileBlocks::new(
        FileStatus {
            len: 8,
            block_size: 8,
            ..FileStatus::default()
        },
        vec![LocatedBlock::new(
            ExtendedBlock::new(17, 8, StorageType::Disk, FileType::File),
            workers,
        )],
    ))
}
fn commit(workers: &[u32]) -> CommitBlock {
    CommitBlock {
        block_id: 17,
        block_len: 8,
        locations: workers
            .iter()
            .map(|id| BlockLocation::with_id(*id))
            .collect(),
    }
}
fn ids(blocks: &WriteFileBlocks) -> Vec<u32> {
    blocks
        .get_block_check(0)
        .unwrap()
        .1
        .locs
        .iter()
        .map(|l| l.worker_id)
        .collect()
}
#[test]
fn confirmed_commit_prunes_cached_locations_before_seek_reopen() {
    let mut blocks = blocks();
    blocks.add_commit(commit(&[2, 3])).unwrap();
    assert_eq!(ids(&blocks), vec![2, 3]);
    blocks.take_commit_blocks();
    assert_eq!(
        ids(&blocks),
        vec![2, 3],
        "draining publication does not restore stale workers"
    );
}
#[test]
fn restoring_pending_commit_preserves_confirmed_locations() {
    let mut blocks = blocks();
    blocks.add_commit(commit(&[2, 3])).unwrap();
    let commits = blocks.take_commit_blocks();
    for c in commits {
        blocks.add_commit(c).unwrap();
    }
    assert_eq!(ids(&blocks), vec![2, 3]);
    blocks.add_commit(commit(&[3])).unwrap();
    assert_eq!(ids(&blocks), vec![3]);
}
#[test]
fn fully_successful_commit_preserves_worker_addresses() {
    let mut blocks = blocks();
    let before = blocks.get_block_check(0).unwrap().1.locs;
    blocks.add_commit(commit(&[1, 2, 3])).unwrap();
    let after = blocks.get_block_check(0).unwrap().1.locs;
    assert_eq!(format!("{before:?}"), format!("{after:?}"));
}
