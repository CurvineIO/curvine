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

use super::*;
use crate::master::fs::MasterFilesystem;
use crate::master::journal::JournalSystem;
use crate::master::Master;
use curvine_model::{
    ClientAddress, CreateFileOptsBuilder, OpenFlags, SetAttrOptsBuilder, StorageType, WorkerInfo,
};
use curvine_runtime::common::Utils;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const WAIT_LIMIT: Duration = Duration::from_secs(10);
const CACHE_PATH: &str = "/cached-file";

fn cached_file(name: &str) -> CommonResult<(MasterFilesystem, i64, u32)> {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.meta_dir = Utils::test_sub_dir(format!(
        "reported-blocks/shared-report-meta-{name}-{}",
        Utils::rand_str(6)
    ));
    conf.journal.journal_dir = Utils::test_sub_dir(format!(
        "reported-blocks/shared-report-journal-{name}-{}",
        Utils::rand_str(6)
    ));
    let fs = JournalSystem::fs_only_for_test(&conf)?;
    fs.add_test_worker(WorkerInfo::default());
    fs.create_with_opts(
        CACHE_PATH,
        CreateFileOptsBuilder::new()
            .ttl_action(TtlAction::Delete)
            .build(),
        OpenFlags::new_create(),
    )?;
    let client = ClientAddress::default();
    let block = fs.add_block(CACHE_PATH, None, client.clone(), vec![], vec![], 0, None)?;
    let id = block.block.id;
    let worker_id = block.locs[0].worker_id;
    fs.complete_file(
        CACHE_PATH,
        None,
        128,
        vec![CommitBlock {
            block_id: id,
            block_len: 128,
            locations: vec![BlockLocation::with_id(worker_id)],
        }],
        &client.client_name,
        false,
        None,
    )?;
    fs.set_attr(
        CACHE_PATH,
        SetAttrOptsBuilder::new().ufs_mtime(12_345).build(),
    )?;
    Ok((fs, id, worker_id))
}

fn report(id: i64, status: BlockReportStatus) -> BlockReportInfo {
    BlockReportInfo::new(id, status, StorageType::Disk, 128)
}

fn discard_cache(fs_dir: &mut FsDir, id: i64, worker_id: u32, overwrite: bool) -> CommonResult<()> {
    if overwrite {
        let path = InodePath::resolve(fs_dir.root_ptr(), CACHE_PATH, &fs_dir.store)?;
        let result = fs_dir.overwrite_file(&path, CreateFileOpts::with_create(false))?;
        assert!(result.blocks.contains_key(&id));
    } else {
        let removed = fs_dir.delete_locations(worker_id)?;
        assert!(removed.contains(&id));
        let result = fs_dir.invalidate_lost_cache_files(&removed)?;
        assert!(result.invalidated_block_ids.contains(&id));
        assert!(fs_dir.get_block_locations(id)?.is_empty());
    }
    // Overwrite detaches the block and returns worker cleanup records. Its
    // location remains until a subsequent authoritative or Deleted report.
    let inode = fs_dir.store.get_inode(InodeId::get_id(id), None)?.unwrap();
    assert!(!inode.as_file_ref()?.block_ids().contains(&id));
    Ok(())
}

#[test]
fn shared_report_commits_with_reader_before_competing_writer() -> CommonResult<()> {
    for overwrite in [false, true] {
        let (fs, id, worker_id) = cached_file(if overwrite {
            "reader-overwrite"
        } else {
            "reader-invalidate"
        })?;
        // Leave the inode's block current, but make the report's insertion
        // observable to the writer that runs after the report commits.
        fs.fs_dir
            .write()
            .block_report(vec![(false, id, BlockLocation::with_id(worker_id))])?;

        let (read_start_tx, read_start_rx) = mpsc::channel();
        let (read_done_tx, read_done_rx) = mpsc::channel();
        let (report_done_tx, report_done_rx) = mpsc::channel();
        let (write_start_tx, write_start_rx) = mpsc::channel();
        let (write_checked_tx, write_checked_rx) = mpsc::channel();

        thread::scope(|scope| -> CommonResult<()> {
            let reader_fs_dir = fs.fs_dir.clone();
            let reader = scope.spawn(move || -> CommonResult<()> {
                read_start_rx.recv_timeout(WAIT_LIMIT)?;
                let fs_dir = reader_fs_dir
                    .try_read_for(WAIT_LIMIT)
                    .expect("reader must run while the report holds a shared guard");
                let inode = fs_dir.store.get_inode(InodeId::get_id(id), None)?.unwrap();
                assert!(inode.as_file_ref()?.block_ids().contains(&id));
                assert!(fs_dir.get_block_locations(id)?.is_empty());
                read_done_tx.send(())?;
                // A report must commit without waiting for this reader to leave.
                report_done_rx.recv_timeout(WAIT_LIMIT)?;
                drop(fs_dir);
                Ok(())
            });
            let writer_fs_dir = fs.fs_dir.clone();
            let writer = scope.spawn(move || -> CommonResult<()> {
                write_start_rx.recv_timeout(WAIT_LIMIT)?;
                assert!(
                    writer_fs_dir.try_write().is_none(),
                    "a writer must not acquire the filesystem lock after report validation"
                );
                write_checked_tx.send(())?;
                let mut fs_dir = writer_fs_dir
                    .try_write_for(WAIT_LIMIT)
                    .expect("writer must run after the report and reader release their guards");
                assert_eq!(
                    fs_dir.get_block_locations(id)?.len(),
                    1,
                    "the report must commit before the competing writer runs"
                );
                discard_cache(&mut fs_dir, id, worker_id, overwrite)
            });

            let obsolete = FsDir::apply_reported_blocks_with_read_hook(
                &fs.fs_dir,
                worker_id,
                false,
                &mut BlockReportDiagnostics::default(),
                vec![report(id, BlockReportStatus::Finalized)],
                || {
                    read_start_tx.send(()).unwrap();
                    read_done_rx.recv_timeout(WAIT_LIMIT).unwrap();
                    write_start_tx.send(()).unwrap();
                    write_checked_rx.recv_timeout(WAIT_LIMIT).unwrap();
                },
            )?;
            report_done_tx.send(())?;
            assert!(obsolete.is_empty());
            reader.join().expect("reader thread panicked")?;
            writer.join().expect("writer thread panicked")?;
            Ok(())
        })?;
        // A report arriving after either writer must reject the detached ID.
        // For overwrite this also removes the location awaiting worker cleanup.
        let obsolete = FsDir::apply_reported_blocks_with_read(
            &fs.fs_dir,
            worker_id,
            false,
            &mut BlockReportDiagnostics::default(),
            vec![report(id, BlockReportStatus::Finalized)],
        )?;
        assert_eq!(obsolete, vec![id]);
        assert!(fs.fs_dir.read().get_block_locations(id)?.is_empty());
    }
    Ok(())
}

#[test]
fn shared_report_rejects_report_when_writer_discards_cache_first() -> CommonResult<()> {
    for overwrite in [false, true] {
        let (fs, id, worker_id) = cached_file(if overwrite {
            "writer-first-overwrite"
        } else {
            "writer-first-invalidate"
        })?;
        let stale_inode = fs
            .fs_dir
            .read()
            .store
            .get_inode(InodeId::get_id(id), None)?
            .unwrap();
        assert!(stale_inode.as_file_ref()?.block_ids().contains(&id));

        let mut writer = fs.fs_dir.write();
        let (report_waiting_tx, report_waiting_rx) = mpsc::channel();
        thread::scope(|scope| -> CommonResult<()> {
            let reporter_fs_dir = fs.fs_dir.clone();
            let reporter = scope.spawn(move || -> CommonResult<Vec<i64>> {
                assert!(reporter_fs_dir.try_read().is_none());
                report_waiting_tx.send(())?;
                let guard = reporter_fs_dir
                    .try_read_for(WAIT_LIMIT)
                    .expect("report must run after the writer releases its guard");
                // Release the timed probe before the report acquires its own guard.
                // The sole competing writer has finished by this point.
                drop(guard);
                Ok(FsDir::apply_reported_blocks_with_read(
                    &reporter_fs_dir,
                    worker_id,
                    false,
                    &mut BlockReportDiagnostics::default(),
                    vec![report(id, BlockReportStatus::Finalized)],
                )?)
            });
            report_waiting_rx.recv_timeout(WAIT_LIMIT)?;
            discard_cache(&mut writer, id, worker_id, overwrite)?;
            drop(writer);
            assert_eq!(reporter.join().expect("report thread panicked")?, vec![id]);
            Ok(())
        })?;
        assert!(fs.fs_dir.read().get_block_locations(id)?.is_empty());
    }
    Ok(())
}

#[test]
fn shared_report_preserves_mixed_status_order_for_duplicate_ids() -> CommonResult<()> {
    let (fs, id, worker_id) = cached_file("duplicate-status-order")?;
    for (statuses, expected_locations) in [
        (
            vec![BlockReportStatus::Finalized, BlockReportStatus::Deleted],
            0,
        ),
        (
            vec![
                BlockReportStatus::Deleted,
                BlockReportStatus::Writing,
                BlockReportStatus::Finalized,
            ],
            1,
        ),
    ] {
        let obsolete = FsDir::apply_reported_blocks_with_read(
            &fs.fs_dir,
            worker_id,
            false,
            &mut BlockReportDiagnostics::default(),
            statuses
                .into_iter()
                .map(|status| report(id, status))
                .collect(),
        )?;
        assert!(obsolete.is_empty());
        assert_eq!(
            fs.fs_dir.read().get_block_locations(id)?.len(),
            expected_locations
        );
    }
    Ok(())
}

#[test]
fn shared_report_leaves_unknown_incremental_writing_until_authoritative_report() -> CommonResult<()>
{
    let (fs, current_id, worker_id) = cached_file("unknown-writing")?;
    let unknown_id = InodeId::create_block_id(InodeId::get_id(current_id), 100)?;
    for (full_report, status, reject) in [
        (false, BlockReportStatus::Writing, false),
        (false, BlockReportStatus::Finalized, true),
        (true, BlockReportStatus::Writing, true),
    ] {
        // A location left from an older report must survive a non-authoritative
        // Writing report, but an authoritative report removes it.
        fs.fs_dir.write().block_report(vec![(
            true,
            unknown_id,
            BlockLocation::with_id(worker_id),
        )])?;
        let obsolete = FsDir::apply_reported_blocks_with_read(
            &fs.fs_dir,
            worker_id,
            full_report,
            &mut BlockReportDiagnostics::default(),
            vec![report(unknown_id, status)],
        )?;
        if reject {
            assert_eq!(obsolete, vec![unknown_id]);
            assert!(fs.fs_dir.read().get_block_locations(unknown_id)?.is_empty());
        } else {
            assert!(obsolete.is_empty());
            assert_eq!(fs.fs_dir.read().get_block_locations(unknown_id)?.len(), 1);
        }
        assert_eq!(fs.fs_dir.read().get_block_locations(current_id)?.len(), 1);
    }
    Ok(())
}
