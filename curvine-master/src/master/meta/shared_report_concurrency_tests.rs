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

use super::tests::{create_file_blocks, report_test_fs};
use super::*;
use curvine_model::StorageType;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const WAIT_LIMIT: Duration = Duration::from_secs(10);

fn report(id: i64, status: BlockReportStatus) -> BlockReportInfo {
    BlockReportInfo::new(id, status, StorageType::Disk, 128)
}

fn staged_report(
    fs_dir: &RwLock<FsDir>,
    worker_id: u32,
    blocks: Vec<BlockReportInfo>,
    prepared: mpsc::Sender<()>,
    commit: mpsc::Receiver<()>,
) -> CommonResult<Vec<i64>> {
    Ok(FsDir::apply_reported_blocks_with_read_hook(
        fs_dir,
        worker_id,
        false,
        &mut BlockReportDiagnostics::default(),
        blocks,
        || {
            prepared.send(()).unwrap();
            commit.recv_timeout(WAIT_LIMIT).unwrap();
        },
    )?)
}

fn assert_single_block_indexes(
    fs_dir: &FsDir,
    id: i64,
    expected_workers: &[u32],
) -> CommonResult<()> {
    let mut locations: Vec<_> = fs_dir
        .get_block_locations(id)?
        .into_iter()
        .map(|location| location.worker_id)
        .collect();
    locations.sort_unstable();
    assert_eq!(locations, expected_workers);
    for worker_id in [100, 101, 102] {
        let expected = if expected_workers.contains(&worker_id) {
            vec![id]
        } else {
            Vec::new()
        };
        assert_eq!(fs_dir.get_worker_block_ids(worker_id)?, expected);
    }
    Ok(())
}

#[test]
fn shared_reports_preserve_both_replicas_with_overlapping_preparation() -> CommonResult<()> {
    let fs = report_test_fs("shared-report-replicas");
    let id = create_file_blocks(&fs, "/file", 1)?[0];
    thread::scope(|scope| -> CommonResult<()> {
        let (prepared_tx, prepared_rx) = mpsc::channel();
        let mut reporters = Vec::new();
        for worker_id in [101, 102] {
            let fs_dir = fs.fs_dir.clone();
            let prepared = prepared_tx.clone();
            let (commit_tx, commit_rx) = mpsc::channel();
            let reporter = scope.spawn(move || {
                staged_report(
                    &fs_dir,
                    worker_id,
                    vec![report(id, BlockReportStatus::Finalized)],
                    prepared,
                    commit_rx,
                )
            });
            reporters.push((commit_tx, reporter));
        }
        drop(prepared_tx);
        for _ in 0..2 {
            prepared_rx.recv_timeout(WAIT_LIMIT)?;
        }
        assert_single_block_indexes(&fs.fs_dir.read(), id, &[100])?;

        for (index, (commit, reporter)) in reporters.into_iter().enumerate() {
            commit.send(())?;
            assert!(reporter
                .join()
                .expect("reporter thread panicked")?
                .is_empty());
            let expected: &[u32] = if index == 0 {
                &[100, 101]
            } else {
                &[100, 101, 102]
            };
            // The first commit completes while the other reporter still holds
            // its read guard and an unpublished batch for the same block.
            assert_single_block_indexes(&fs.fs_dir.read(), id, expected)?;
        }
        Ok(())
    })
}

#[test]
fn shared_reports_for_same_worker_follow_commit_order() -> CommonResult<()> {
    for add_first in [true, false] {
        let fs = report_test_fs("shared-report-commit-order");
        let id = create_file_blocks(&fs, "/file", 1)?[0];
        if !add_first {
            FsDir::apply_reported_blocks_with_read(
                &fs.fs_dir,
                101,
                false,
                &mut BlockReportDiagnostics::default(),
                vec![report(id, BlockReportStatus::Finalized)],
            )?;
        }
        let statuses = if add_first {
            [BlockReportStatus::Finalized, BlockReportStatus::Deleted]
        } else {
            [BlockReportStatus::Deleted, BlockReportStatus::Finalized]
        };

        thread::scope(|scope| -> CommonResult<()> {
            let (prepared_tx, prepared_rx) = mpsc::channel();
            let mut reporters = Vec::new();
            for status in statuses {
                let fs_dir = fs.fs_dir.clone();
                let prepared = prepared_tx.clone();
                let (commit_tx, commit_rx) = mpsc::channel();
                let reporter = scope.spawn(move || {
                    staged_report(&fs_dir, 101, vec![report(id, status)], prepared, commit_rx)
                });
                reporters.push((status, commit_tx, reporter));
            }
            drop(prepared_tx);
            for _ in 0..2 {
                prepared_rx.recv_timeout(WAIT_LIMIT)?;
            }
            let initial: &[u32] = if add_first { &[100] } else { &[100, 101] };
            assert_single_block_indexes(&fs.fs_dir.read(), id, initial)?;

            for (status, commit, reporter) in reporters {
                commit.send(())?;
                assert!(reporter
                    .join()
                    .expect("reporter thread panicked")?
                    .is_empty());
                let expected: &[u32] = if status == BlockReportStatus::Deleted {
                    &[100]
                } else {
                    &[100, 101]
                };
                assert_single_block_indexes(&fs.fs_dir.read(), id, expected)?;
            }
            Ok(())
        })?;
    }
    Ok(())
}

#[test]
fn checkpoint_waits_for_shared_report_and_restores_committed_indexes() -> CommonResult<()> {
    let fs = report_test_fs("shared-report-checkpoint");
    let restored = report_test_fs("shared-report-checkpoint-restored");
    let ids = create_file_blocks(&fs, "/file", 128)?;
    assert!(fs.fs_dir.read().get_rocks_store().db.conf().disable_wal);

    for (round, status) in [
        BlockReportStatus::Finalized,
        BlockReportStatus::Deleted,
        BlockReportStatus::Finalized,
        BlockReportStatus::Deleted,
    ]
    .into_iter()
    .enumerate()
    {
        let blocks = ids.iter().map(|&id| report(id, status)).collect();
        let checkpoint = thread::scope(|scope| -> CommonResult<String> {
            let (prepared_tx, prepared_rx) = mpsc::channel();
            let (commit_tx, commit_rx) = mpsc::channel();
            let fs_dir = fs.fs_dir.clone();
            let reporter =
                scope.spawn(move || staged_report(&fs_dir, 101, blocks, prepared_tx, commit_rx));
            prepared_rx.recv_timeout(WAIT_LIMIT)?;
            assert!(
                fs.fs_dir.try_write().is_none(),
                "checkpoint must not acquire exclusive access while a report is staged"
            );
            commit_tx.send(())?;
            assert!(reporter
                .join()
                .expect("reporter thread panicked")?
                .is_empty());
            let mut checkpoint_guard = fs
                .fs_dir
                .try_write_for(WAIT_LIMIT)
                .expect("checkpoint must acquire exclusive access after the report finishes");
            let checkpoint = checkpoint_guard.create_checkpoint(round as u64 + 1)?;
            drop(checkpoint_guard);
            Ok(checkpoint)
        })?;

        let mut live_worker_ids = fs.fs_dir.read().get_worker_block_ids(101)?;
        live_worker_ids.sort_unstable();
        if status == BlockReportStatus::Deleted {
            assert!(live_worker_ids.is_empty());
        } else {
            assert_eq!(live_worker_ids, ids);
        }

        // Inspect a restored, quiescent database: separate live reads need not
        // observe one snapshot when another shared report commits between them.
        let mut restored_dir = restored.fs_dir.write();
        restored_dir.restore(checkpoint, 0)?;
        let inode = restored_dir
            .store
            .get_inode(InodeId::get_id(ids[0]), None)?
            .unwrap();
        assert_eq!(inode.as_file_ref()?.block_ids(), ids);
        let mut original_worker_ids = restored_dir.get_worker_block_ids(100)?;
        original_worker_ids.sort_unstable();
        assert_eq!(original_worker_ids, ids);
        let mut restored_reporter_ids = restored_dir.get_worker_block_ids(101)?;
        restored_reporter_ids.sort_unstable();
        let expected_reporter_ids = if status == BlockReportStatus::Deleted {
            Vec::new()
        } else {
            ids.clone()
        };
        assert_eq!(restored_reporter_ids, expected_reporter_ids);
        for &id in &ids {
            let mut workers: Vec<_> = restored_dir
                .get_block_locations(id)?
                .into_iter()
                .map(|location| location.worker_id)
                .collect();
            workers.sort_unstable();
            let expected = if status == BlockReportStatus::Deleted {
                vec![100]
            } else {
                vec![100, 101]
            };
            assert_eq!(workers, expected, "checkpoint indexes disagree for {id}");
        }
    }
    Ok(())
}
