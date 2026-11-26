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
        expected_revision: None,
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

#[test]
fn acknowledged_add_block_commit_advances_cached_revision_once() {
    let mut file = blocks();
    let mut committed = commit(&[2, 3]);
    committed.expected_revision = Some(0);
    file.add_commit(committed.clone()).unwrap();
    file.acknowledge_commits(&[committed]).unwrap();
    let (_, lb) = file.get_block_check(1).unwrap();
    assert_eq!(lb.commit_revision, Some(1));
    let next = CommitBlock::from(&lb);
    assert_eq!(next.expected_revision, Some(1));
    file.acknowledge_commits(&[next]).unwrap();
    assert_eq!(file.get_block_check(1).unwrap().1.commit_revision, Some(2));
}

#[test]
fn commit_revision_is_wire_only_and_keeps_legacy_bincode_layout() {
    use curvine_model::ProtoUtils;
    use curvine_runtime::common::SerdeUtils;
    #[derive(serde::Serialize)]
    struct LegacyCommit {
        block_id: i64,
        block_len: i64,
        locations: Vec<BlockLocation>,
    }
    let mut current = commit(&[2, 3]);
    current.expected_revision = Some(12);
    let old = LegacyCommit {
        block_id: current.block_id,
        block_len: current.block_len,
        locations: current.locations.clone(),
    };
    let bytes = SerdeUtils::serialize(&old).unwrap();
    assert_eq!(SerdeUtils::serialize(&current).unwrap(), bytes);
    let decoded: CommitBlock = SerdeUtils::deserialize(&bytes).unwrap();
    assert_eq!(decoded.expected_revision, None);
    let pb = ProtoUtils::commit_block_to_pb(current);
    assert_eq!(
        ProtoUtils::commit_block_from_pb(pb).expected_revision,
        Some(12)
    );
    let mut located = blocks().get_block_check(0).unwrap().1;
    located.commit_revision = Some(12);
    let pb = ProtoUtils::located_block_to_pb(located);
    assert_eq!(
        ProtoUtils::located_block_from_pb(pb).commit_revision,
        Some(12)
    );
}
