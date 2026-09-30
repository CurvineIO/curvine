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

use super::tests::{
    block_location_worker_ids, create_file_blocks, replace_inode_record, report_test_fs,
};
use super::*;
use crate::master::fs::{BlockReportResult, MasterFilesystem};
use crate::master::{SyncFsDir, SyncWorkerManager};
use curvine_model::{BlockReportList, StorageType};
use std::cell::RefCell;
use std::sync::Once;

struct CapturedWarning {
    level: log::Level,
    target: String,
    message: String,
    metadata_unlocked: bool,
    worker_manager_unlocked: bool,
}

struct LogCapture {
    fs_dir: SyncFsDir,
    worker_manager: SyncWorkerManager,
    warnings: Vec<CapturedWarning>,
}

thread_local! {
    static CAPTURE: RefCell<Option<LogCapture>> = const { RefCell::new(None) };
}

struct ReportLogger;

impl log::Log for ReportLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Warn
    }

    fn log(&self, record: &log::Record<'_>) {
        CAPTURE.with(|capture| {
            let mut capture = capture.borrow_mut();
            if let Some(capture) = capture.as_mut() {
                let message = record.args().to_string();
                if message.starts_with("block_report ") {
                    capture.warnings.push(CapturedWarning {
                        level: record.level(),
                        target: record.target().to_owned(),
                        message,
                        metadata_unlocked: capture.fs_dir.try_write().is_some(),
                        worker_manager_unlocked: capture.worker_manager.try_write().is_ok(),
                    });
                }
            }
        });
    }

    fn flush(&self) {}
}

struct CaptureGuard;

impl Drop for CaptureGuard {
    fn drop(&mut self) {
        CAPTURE.with(|capture| capture.borrow_mut().take());
    }
}

fn report(id: i64, status: BlockReportStatus) -> BlockReportInfo {
    BlockReportInfo::new(id, status, StorageType::Disk, 128)
}

fn capture_report(
    fs: &MasterFilesystem,
    full_report: bool,
    blocks: Vec<BlockReportInfo>,
) -> (FsResult<BlockReportResult>, Vec<String>) {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        log::set_logger(&ReportLogger).expect("install test log capture");
        log::set_max_level(log::LevelFilter::Warn);
    });
    CAPTURE.with(|capture| {
        let previous = capture.replace(Some(LogCapture {
            fs_dir: fs.fs_dir.clone(),
            worker_manager: fs.worker_manager.clone(),
            warnings: Vec::new(),
        }));
        assert!(previous.is_none());
    });
    let _capture_guard = CaptureGuard;
    let result = fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report,
            // These are partial full reports; no asynchronous reconciliation is needed.
            total_len: blocks.len() as u64 + 1,
            blocks,
        },
        None,
    );
    let capture = CAPTURE.with(|capture| capture.borrow_mut().take().unwrap());
    let messages = capture
        .warnings
        .into_iter()
        .map(|warning| {
            assert_eq!(warning.level, log::Level::Warn);
            assert_eq!(
                warning.target,
                "curvine_master::master::fs::master_filesystem"
            );
            assert!(
                warning.metadata_unlocked,
                "logging must release metadata protection"
            );
            assert!(
                warning.worker_manager_unlocked,
                "logging must release the worker manager"
            );
            warning.message
        })
        .collect();
    (result, messages)
}

#[test]
fn block_report_logs_original_warnings_and_separate_stale_summary() -> CommonResult<()> {
    let fs = report_test_fs("logging-mixed");
    let current = create_file_blocks(&fs, "/current", 1)?[0];
    let stale = InodeId::create_block_id(InodeId::get_id(current), 100)?;
    let missing = InodeId::create_block_id(10_000_000, 0)?;
    let directory = InodeId::create_block_id(fs.mkdir("/directory", false)?.id, 0)?;
    let (result, logs) = capture_report(
        &fs,
        false,
        vec![
            report(current, BlockReportStatus::Finalized),
            report(current, BlockReportStatus::Writing),
            report(missing, BlockReportStatus::Writing),
            report(directory, BlockReportStatus::Writing),
            report(stale, BlockReportStatus::Writing),
            report(missing, BlockReportStatus::Deleted),
            report(directory, BlockReportStatus::Deleted),
            report(stale, BlockReportStatus::Deleted),
            report(missing, BlockReportStatus::Finalized),
            report(directory, BlockReportStatus::Finalized),
            report(stale, BlockReportStatus::Finalized),
            report(missing, BlockReportStatus::Finalized),
            report(directory, BlockReportStatus::Finalized),
            report(stale, BlockReportStatus::Finalized),
        ],
    );
    assert_eq!(
        result?.delete_blocks,
        vec![missing, directory, stale, missing, directory, stale]
    );
    assert_eq!(
        logs,
        vec![
            format!("block_report deferred deletion for writing block {missing} on worker 100 because its inode is missing"),
            format!("block_report deferred deletion for writing block {directory} on worker 100 because its inode is not a file"),
            "block_report found 2 missing-inode and 2 non-file-inode blocks for worker 100; scheduling worker deletion".to_owned(),
            "block_report found 2 obsolete blocks in existing files for worker 100; scheduling worker deletion".to_owned(),
        ]
    );
    Ok(())
}

#[test]
fn block_report_logs_one_full_report_summary_across_chunks_and_duplicates() -> CommonResult<()> {
    let fs = report_test_fs("logging-chunks");
    let current = create_file_blocks(&fs, "/current", 1)?[0];
    let stale = InodeId::create_block_id(InodeId::get_id(current), 100)?;
    let missing = InodeId::create_block_id(10_000_000, 0)?;
    let directory = InodeId::create_block_id(fs.mkdir("/directory", false)?.id, 0)?;
    // Repeated IDs span the foreground processing boundary.
    let mut blocks = Vec::new();
    let pairs = MasterFilesystem::BLOCK_REPORT_CHUNK / 2;
    for _ in 0..pairs {
        blocks.push(report(missing, BlockReportStatus::Writing));
        blocks.push(report(directory, BlockReportStatus::Writing));
    }
    blocks.extend([
        report(missing, BlockReportStatus::Finalized),
        report(directory, BlockReportStatus::Finalized),
        report(stale, BlockReportStatus::Writing),
    ]);
    let (result, logs) = capture_report(&fs, true, blocks);
    assert_eq!(result?.delete_blocks.len(), pairs * 2 + 3);
    assert_eq!(
        logs,
        vec![
            format!("block_report found {} missing-inode and {} non-file-inode blocks for worker 100; scheduling worker deletion", pairs + 1, pairs + 1),
            "block_report found 1 obsolete blocks in existing files for worker 100; scheduling worker deletion".to_owned(),
        ]
    );
    Ok(())
}

#[test]
fn block_report_logs_inode_error_without_committing_deleted_locations() -> CommonResult<()> {
    let fs = report_test_fs("logging-error");
    let healthy = create_file_blocks(&fs, "/healthy", 1)?[0];
    let corrupt = create_file_blocks(&fs, "/corrupt", 1)?[0];
    let missing = InodeId::create_block_id(10_000_000, 0)?;
    let inode_id = InodeId::get_id(corrupt);
    let inode_bytes = replace_inode_record(&fs, inode_id, &[0xff])?;
    let blocks = || {
        vec![
            report(missing, BlockReportStatus::Writing),
            report(healthy, BlockReportStatus::Deleted),
            report(corrupt, BlockReportStatus::Deleted),
            report(missing, BlockReportStatus::Finalized),
            report(corrupt, BlockReportStatus::Finalized),
            report(corrupt, BlockReportStatus::Writing),
        ]
    };
    let (result, logs) = capture_report(&fs, false, blocks());
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("corrupt inode must fail the report"),
    };
    assert_eq!(
        logs,
        vec![
            format!("block_report deferred deletion for writing block {missing} on worker 100 because its inode is missing"),
            format!("block_report {:?}: {error}", report(corrupt, BlockReportStatus::Finalized)),
        ]
    );
    assert_eq!(block_location_worker_ids(&fs, healthy)?, vec![100]);
    assert_eq!(block_location_worker_ids(&fs, corrupt)?, vec![100]);

    replace_inode_record(&fs, inode_id, &inode_bytes)?;
    let (result, logs) = capture_report(&fs, false, blocks());
    assert_eq!(result?.delete_blocks, vec![missing]);
    assert_eq!(logs.len(), 2);
    assert!(block_location_worker_ids(&fs, healthy)?.is_empty());
    assert_eq!(block_location_worker_ids(&fs, corrupt)?, vec![100]);
    Ok(())
}

#[test]
fn block_report_deleted_only_does_not_read_or_log_corrupt_inode() -> CommonResult<()> {
    for full_report in [false, true] {
        let fs = report_test_fs("logging-deleted-corrupt");
        let id = create_file_blocks(&fs, "/corrupt", 1)?[0];
        let inode_id = InodeId::get_id(id);
        let inode_bytes = replace_inode_record(&fs, inode_id, &[0xff])?;
        let (result, logs) = capture_report(
            &fs,
            full_report,
            vec![
                report(id, BlockReportStatus::Deleted),
                report(id, BlockReportStatus::Deleted),
            ],
        );
        assert!(result?.delete_blocks.is_empty());
        assert!(logs.is_empty());
        assert!(block_location_worker_ids(&fs, id)?.is_empty());
        replace_inode_record(&fs, inode_id, &inode_bytes)?;
    }
    Ok(())
}

#[test]
fn block_report_error_summary_excludes_failed_chunk() -> CommonResult<()> {
    let fs = report_test_fs("logging-later-error");
    let corrupt = create_file_blocks(&fs, "/corrupt", 1)?[0];
    let inode_id = InodeId::get_id(corrupt);
    let inode_bytes = replace_inode_record(&fs, inode_id, &[0xff])?;
    let missing = InodeId::create_block_id(10_000_000, 0)?;
    let chunk_size = MasterFilesystem::BLOCK_REPORT_CHUNK;
    let mut blocks: Vec<_> = (0..chunk_size)
        .map(|_| report(missing, BlockReportStatus::Finalized))
        .collect();
    blocks.extend([
        report(missing, BlockReportStatus::Finalized),
        report(corrupt, BlockReportStatus::Finalized),
    ]);
    let (result, logs) = capture_report(&fs, false, blocks);
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("corrupt inode must fail the report"),
    };
    assert_eq!(
        logs,
        vec![
            format!("block_report {:?}: {error}", report(corrupt, BlockReportStatus::Finalized)),
            format!("block_report found {chunk_size} missing-inode and 0 non-file-inode blocks for worker 100; scheduling worker deletion"),
        ]
    );
    replace_inode_record(&fs, inode_id, &inode_bytes)?;
    Ok(())
}
