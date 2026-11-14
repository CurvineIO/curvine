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

//! The inode id allocator must never regress across a master restart (#2064).
//!
//! Deletes (TTL eviction in production) remove high-id inodes from RocksDB, so
//! rebuilding the high-water mark from live inodes alone hands out ids that were
//! already used — and block ids are derived from inode ids. The checkpoint now
//! carries the allocator watermark, and restore keeps `max(live max, watermark)`.

use curvine_config::ClusterConf;
use curvine_master::master::fs::MasterFilesystem;
use curvine_master::master::journal::JournalSystem;
use curvine_master::master::meta::InodeId;
use curvine_master::Master;
use curvine_model::{BlockReportInfo, BlockReportList};
use curvine_raft::raft::RoleState;
use curvine_runtime::common::{FileUtils, Utils};

static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn conf(name: &str, format: bool) -> ClusterConf {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        format_master: format,
        ..Default::default()
    };
    conf.journal.enable = false;
    // Direct deletes (no staging under /.Trash) so inodes leave RocksDB at once.
    conf.master.delete_stage_min_children = usize::MAX;
    conf.master.meta_dir = Utils::test_sub_dir(format!(
        "inode-watermark/meta-{}-{}",
        name,
        Utils::rand_str(6)
    ));
    conf
}

fn filesystem(name: &str, format: bool) -> MasterFilesystem {
    let fs = JournalSystem::fs_only_for_test(&conf(name, format)).unwrap();
    fs.master_monitor.set_role_for_test(RoleState::Leader);
    fs
}

#[test]
fn inode_id_watermark_survives_restart_after_deletes() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    let fs = filesystem("source", true);
    for index in 0..40 {
        fs.mkdir(format!("/ttl-{index}"), false).unwrap();
    }
    let high_water = fs.last_inode_id();
    assert!(high_water >= 40, "creating 40 dirs must allocate ids");

    // TTL/eviction traffic: every high-id inode is gone, so the live max is back
    // at the root and says nothing about ids already handed out.
    for index in 0..40 {
        fs.delete(format!("/ttl-{index}"), true).unwrap();
    }
    assert!(fs.file_status("/ttl-0").is_err());

    let checkpoint = fs.fs_dir().read().create_checkpoint(1).unwrap();
    assert!(FileUtils::exists(&checkpoint));

    // Restart: a fresh metadata dir restored from the checkpoint.
    let restarted = filesystem("restarted", false);
    restarted
        .fs_dir()
        .write()
        .restore(&checkpoint, 0)
        .expect("restore from checkpoint");

    assert_eq!(
        restarted.last_inode_id(),
        high_water,
        "restore must not regress the allocator below the checkpointed watermark"
    );

    let created = restarted.mkdir("/after-restart", false).unwrap();
    assert!(
        created.id > high_water,
        "allocator handed out id {} after restarting at high-water mark {}",
        created.id,
        high_water
    );
}

// A block report naming a block whose inode id is above the allocator is the
// only witness left for ids that were allocated and deleted before an
// old-format checkpoint: they are in neither the live tree nor the journal tail.

fn block_report(worker_id: u32, full_report: bool, blocks: Vec<i64>) -> BlockReportList {
    let blocks: Vec<_> = blocks
        .into_iter()
        .map(|block_id| BlockReportInfo::with_deleted(block_id, 0))
        .collect();
    BlockReportList {
        report_id: 0,
        cluster_id: "curvine".into(),
        worker_id,
        full_report,
        total_len: blocks.len() as u64,
        blocks,
    }
}

#[test]
fn block_report_floor_survives_checkpoint_and_restart() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    let fs = filesystem("block-report", true);
    let stale_inode_id = fs.last_inode_id() + 1_000;
    let block_id = InodeId::create_block_id(stale_inode_id, 7).unwrap();

    let result = fs
        .block_report(block_report(200, false, vec![block_id]), None)
        .unwrap();
    assert!(result.delete_blocks.is_empty());
    assert_eq!(
        fs.last_inode_id(),
        stale_inode_id,
        "a reported block id must raise the allocator to its inode id"
    );

    let checkpoint = fs.fs_dir().read().create_checkpoint(1).unwrap();
    assert!(FileUtils::exists(&checkpoint));

    // The raise is memory-only, so the restart only sees it if the checkpoint
    // written afterwards persisted it.
    let restarted = filesystem("block-report-restarted", false);
    restarted
        .fs_dir()
        .write()
        .restore(&checkpoint, 0)
        .expect("restore from checkpoint");
    assert_eq!(
        restarted.last_inode_id(),
        stale_inode_id,
        "the next checkpoint must persist the block-report floor"
    );

    let created = restarted.mkdir("/after-report", false).unwrap();
    assert!(
        created.id > stale_inode_id,
        "allocator handed out id {} at or below the reported inode id {}",
        created.id,
        stale_inode_id
    );
}

#[test]
fn block_report_ignores_block_ids_without_an_inode_id() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    let fs = filesystem("block-report-underivable", true);
    fs.mkdir("/live", false).unwrap();
    let high_water = fs.last_inode_id();

    // A negative id masks to a huge inode id, and a tiny or root-level id never
    // belongs to a file block. None of them may move the allocator, and a report
    // at the current floor must leave it alone too.
    let blocks = vec![
        InodeId::create_block_id(high_water, 3).unwrap(),
        InodeId::create_block_id(high_water - 1, 0).unwrap(),
        InodeId::create_block_id(2, 0).unwrap(),
        -1,
    ];
    let result = fs
        .block_report(block_report(200, false, blocks), None)
        .unwrap();
    assert!(result.delete_blocks.is_empty());
    assert_eq!(
        fs.last_inode_id(),
        high_water,
        "reports at or below the allocator must not move it"
    );

    let created = fs.mkdir("/after-report", false).unwrap();
    assert_eq!(created.id, high_water + 1);
}

#[test]
fn full_block_report_raises_the_floor() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    let fs = filesystem("block-report-full", true);
    let stale_inode_id = fs.last_inode_id() + 500;
    let block_id = InodeId::create_block_id(stale_inode_id, 0).unwrap();

    fs.block_report(block_report(200, true, vec![block_id]), None)
        .unwrap();
    assert_eq!(
        fs.last_inode_id(),
        stale_inode_id,
        "a full report must raise the allocator like an incremental one"
    );
}
