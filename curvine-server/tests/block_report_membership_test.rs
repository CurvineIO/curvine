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

use curvine_config::{ClusterConf, MasterConf};
use curvine_core_error::CommonResult;
use curvine_model::{
    BlockLocation, BlockReportInfo, BlockReportList, BlockReportStatus, ClientAddress, CommitBlock,
    CreateFileOptsBuilder, LocatedBlock, OpenFlags, SetAttrOptsBuilder, StorageType, TtlAction,
    WorkerInfo,
};
use curvine_raft::conf::JournalConf;
use curvine_runtime::common::Utils;
use curvine_server::master::fs::MasterFilesystem;
use curvine_server::master::journal::JournalSystem;
use curvine_server::master::meta::InodeId;
use curvine_server::master::Master;
use std::sync::{Mutex, OnceLock};

// Filesystem initialization resets process-wide master metrics.
static SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn new_fs(name: &str) -> MasterFilesystem {
    Master::init_test_metrics();
    let conf = ClusterConf {
        format_master: true,
        testing: true,
        master: MasterConf {
            meta_dir: Utils::test_sub_dir(format!("block-report-membership/{name}")),
            ..Default::default()
        },
        journal: JournalConf {
            enable: false,
            journal_dir: Utils::test_sub_dir(format!(
                "block-report-membership/journal-{name}-{}",
                Utils::rand_str(6)
            )),
            ..Default::default()
        },
        ..Default::default()
    };
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.add_test_worker(WorkerInfo::default());
    fs
}

fn complete_block(fs: &MasterFilesystem, path: &str) -> CommonResult<LocatedBlock> {
    let client = ClientAddress::default();
    let block = fs.add_block(path, None, client.clone(), vec![], vec![], 0, None)?;
    fs.complete_file(
        path,
        None,
        128,
        vec![CommitBlock {
            block_id: block.block.id,
            block_len: 128,
            locations: vec![BlockLocation::with_id(block.locs[0].worker_id)],
        }],
        &client.client_name,
        false,
        None,
    )?;
    Ok(block)
}

fn report(
    fs: &MasterFilesystem,
    worker_id: u32,
    block_id: i64,
    status: BlockReportStatus,
    full_report: bool,
) -> CommonResult<Vec<i64>> {
    let result = fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id,
            full_report,
            total_len: 1,
            blocks: vec![BlockReportInfo::new(
                block_id,
                status,
                StorageType::Disk,
                128,
            )],
        },
        None,
    )?;
    Ok(result.delete_blocks)
}

#[test]
fn reports_reject_discarded_blocks_after_cache_invalidation() -> CommonResult<()> {
    let _serial = serial();
    let fs = new_fs("invalidated");
    let path = "/cached-file";
    fs.create_with_opts(
        path,
        CreateFileOptsBuilder::new()
            .ttl_action(TtlAction::Delete)
            .build(),
        OpenFlags::new_create(),
    )?;
    let block = complete_block(&fs, path)?;
    fs.set_attr(path, SetAttrOptsBuilder::new().ufs_mtime(12_345).build())?;
    let before = fs.file_status(path)?;
    assert!(before.cv_valid(None));
    fs.delete_locations(block.locs[0].worker_id)?;
    assert!(!fs.file_status(path)?.cv_valid(None));

    for (status, full_report) in [
        (BlockReportStatus::Finalized, false),
        (BlockReportStatus::Writing, true),
    ] {
        let rejected = report(
            &fs,
            block.locs[0].worker_id,
            block.block.id,
            status,
            full_report,
        )?;
        assert_eq!(rejected, vec![block.block.id]);
        assert!(fs
            .fs_dir
            .read()
            .get_block_locations(block.block.id)?
            .is_empty());
        let after = fs.file_status(path)?;
        assert_eq!(after.id, before.id);
        assert!(after.ufs_exists());
        assert!(!after.cv_valid(None));
    }
    Ok(())
}

#[test]
fn reports_reject_old_blocks_after_overwrite_preserving_current_blocks() -> CommonResult<()> {
    let _serial = serial();
    let fs = new_fs("overwritten");
    let path = "/overwritten-file";
    let before = fs.create(path, false)?;
    let old = complete_block(&fs, path)?;
    let after = fs.create(path, false)?;
    assert_eq!(before.id, after.id);
    let current = complete_block(&fs, path)?;
    assert_ne!(old.block.id, current.block.id);

    let rejected = report(
        &fs,
        old.locs[0].worker_id,
        old.block.id,
        BlockReportStatus::Finalized,
        false,
    )?;
    assert_eq!(rejected, vec![old.block.id]);
    assert!(fs
        .fs_dir
        .read()
        .get_block_locations(old.block.id)?
        .is_empty());
    let blocks = fs.get_block_locations(path)?;
    assert_eq!(blocks.block_locs.len(), 1);
    assert_eq!(blocks.block_locs[0].block.id, current.block.id);
    assert_eq!(blocks.block_locs[0].locs.len(), 1);
    Ok(())
}

#[test]
fn reports_accept_current_blocks_and_remove_deleted_locations() -> CommonResult<()> {
    let _serial = serial();
    let fs = new_fs("current");
    let path = "/current-file";
    fs.create(path, false)?;
    let block = complete_block(&fs, path)?;
    let mut replica = WorkerInfo::default();
    replica.address.worker_id = 101;
    fs.add_test_worker(replica);

    for (status, full_report) in [
        (BlockReportStatus::Finalized, true),
        (BlockReportStatus::Writing, false),
    ] {
        assert!(report(&fs, 101, block.block.id, status, full_report)?.is_empty());
        assert!(fs
            .fs_dir
            .read()
            .get_block_locations(block.block.id)?
            .iter()
            .any(|location| location.worker_id == 101));
        assert!(report(&fs, 101, block.block.id, BlockReportStatus::Deleted, false)?.is_empty());
        assert!(fs
            .fs_dir
            .read()
            .get_block_locations(block.block.id)?
            .iter()
            .all(|location| location.worker_id != 101));
    }
    Ok(())
}

#[test]
fn incremental_writing_report_defers_unattached_block_without_adding_location() -> CommonResult<()>
{
    let _serial = serial();
    let fs = new_fs("unattached-writing");
    let path = "/writing-file";
    let file = fs.create(path, false)?;
    let block_id = InodeId::create_block_id(file.id, 0)?;
    assert!(report(&fs, 100, block_id, BlockReportStatus::Writing, false)?.is_empty());
    assert!(fs.fs_dir.read().get_block_locations(block_id)?.is_empty());

    // Once allocation attaches the ID, the same writing report is valid.
    let block = fs.add_block(
        path,
        None,
        ClientAddress::default(),
        vec![],
        vec![],
        0,
        None,
    )?;
    assert_eq!(block.block.id, block_id);
    assert!(report(&fs, 100, block_id, BlockReportStatus::Writing, false)?.is_empty());
    assert_eq!(fs.fs_dir.read().get_block_locations(block_id)?.len(), 1);
    Ok(())
}
