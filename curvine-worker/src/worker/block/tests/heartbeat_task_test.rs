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

use super::{BlockStore, HeartbeatTask};
use crate::worker::storage::Dataset;
use bytes::BytesMut;
use curvine_config::{ClusterConf, WorkerConf};
use curvine_core_error::CommonResult;
use curvine_io::DataSlice;
use curvine_model::{
    BlockReportStatus, DeleteBlockCmd, ExtendedBlock, FileType, StorageType, WorkerCommand,
};
use curvine_runtime::runtime::GroupExecutor;
use dashmap::{DashMap, DashSet};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

fn create_store() -> CommonResult<BlockStore> {
    let conf = ClusterConf {
        format_worker: true,
        worker: WorkerConf {
            dir_reserved: "0".to_string(),
            data_dir: vec!["[MEM:1KB]../testing/heartbeat-task".to_string()],
            ..WorkerConf::default()
        },
        ..ClusterConf::default()
    };
    BlockStore::new("test", &conf)
}

#[test]
fn report_is_emitted_only_after_delete_completes() -> CommonResult<()> {
    let store = create_store()?;
    let executor = Arc::new(GroupExecutor::new("heartbeat-task-test", 1, 2));
    let reports = Arc::new(DashMap::new());
    let pending = Arc::new(DashSet::new());
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    executor.spawn(move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    })?;
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    HeartbeatTask::delete_block_task(
        executor.clone(),
        store.clone(),
        vec![WorkerCommand::DeleteBlock(DeleteBlockCmd {
            blocks: vec![1],
        })],
        reports.clone(),
        pending.clone(),
    );

    // The master repeats DeleteBlock until it accepts the Deleted report.
    // A repeated command must not queue or count the same deletion twice.
    HeartbeatTask::delete_block_task(
        executor.clone(),
        store.clone(),
        vec![WorkerCommand::DeleteBlock(DeleteBlockCmd {
            blocks: vec![1],
        })],
        reports.clone(),
        pending.clone(),
    );

    assert!(reports.is_empty(), "report must wait for physical deletion");
    assert!(pending.contains(&1));
    assert_eq!(store.read()?.num_blocks_to_delete(), 1);
    release_tx.send(()).unwrap();

    let deadline = Instant::now() + Duration::from_secs(1);
    while !reports.contains_key(&1) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }

    let report = reports.get(&1).expect("missing deleted report");
    assert_eq!(report.status, BlockReportStatus::Deleted);
    assert_eq!(store.read()?.num_blocks_to_delete(), 0);
    assert!(
        pending.contains(&1),
        "pending state is cleared after master ACK"
    );
    // GroupExecutor's Drop waits for its worker threads; leaking this test-only
    // executor lets the process shutdown reclaim the idle thread deterministically.
    std::mem::forget(executor);
    Ok(())
}

/// A finalized, checksummed block whose data was then corrupted on disk.
fn corrupt_checksummed_block(name: &str, id: i64) -> CommonResult<(BlockStore, i64)> {
    let conf = ClusterConf {
        format_worker: true,
        worker: WorkerConf {
            dir_reserved: "0".to_string(),
            data_dir: vec![format!("[MEM:4MB]../testing/{name}")],
            block_checksum: true,
            block_checksum_verify: true,
            ..WorkerConf::default()
        },
        ..ClusterConf::default()
    };
    let store = BlockStore::new("test", &conf)?;
    let data: Vec<u8> = (0..100 * 1024).map(|i| (i % 251) as u8).collect();
    let mut block = ExtendedBlock::with_mem(id, "1MB")?;
    let meta = store.open_block(&block)?;
    let mut writer = store.open_writer(&meta, 0)?;
    writer.write_region(&DataSlice::Buffer(BytesMut::from(&data[..])))?;
    writer.flush()?;
    drop(writer);
    block.len = data.len() as i64;
    let finalized = store.finalize_block(&block)?;

    let path = store.short_circuit(&finalized)?.unwrap();
    let mut corrupted = data;
    corrupted[10] ^= 0xff;
    std::fs::write(&path, &corrupted)?;
    Ok((store, block.len))
}

/// Wait until every job queued on the single-threaded `executor` has run.
fn drain(executor: &GroupExecutor) {
    let (tx, rx) = mpsc::channel();
    executor.spawn(move || tx.send(()).unwrap()).unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
}

#[test]
fn corrupt_block_is_removed_and_reported_as_corrupted() -> CommonResult<()> {
    let (store, len) = corrupt_checksummed_block("heartbeat-task-corrupt", 7)?;
    store.report_corrupt(7, "test");

    let executor = Arc::new(GroupExecutor::new("heartbeat-corrupt-test", 1, 2));
    let reports = Arc::new(DashMap::new());
    HeartbeatTask::invalidate_corrupt_task(executor.clone(), store.clone(), reports.clone());
    drain(&executor);

    let (_, report) = reports.remove(&7).expect("missing corrupted report");
    assert_eq!(report.status, BlockReportStatus::Deleted);
    assert!(report.corrupted);
    assert_eq!(report.block_size, len);
    assert!(store.get_block(7).is_err());

    // The id stays fenced until master acknowledges the corrupted deletion.
    let block = ExtendedBlock::with_mem(7, "1MB")?;
    assert!(store.open_block(&block).is_err());
    let pending = DashSet::new();
    HeartbeatTask::acknowledge(&store, &pending, &[report]);
    store.open_block(&block)?;
    std::mem::forget(executor);
    Ok(())
}

#[test]
fn truncated_block_is_removed_and_reported_as_corrupted() -> CommonResult<()> {
    let conf = ClusterConf {
        format_worker: true,
        worker: WorkerConf {
            dir_reserved: "0".to_string(),
            data_dir: vec!["[MEM:4MB]../testing/heartbeat-task-truncated".to_string()],
            ..WorkerConf::default()
        },
        ..ClusterConf::default()
    };
    let store = BlockStore::new("test", &conf)?;
    let mut block = ExtendedBlock::with_mem(9, "1MB")?;
    let meta = store.open_block(&block)?;
    let mut writer = store.open_writer(&meta, 0)?;
    writer.write_region(&DataSlice::Buffer(BytesMut::from(&[9u8; 4096][..])))?;
    writer.flush()?;
    drop(writer);
    block.len = 4096;
    let finalized = store.finalize_block(&block)?;
    let path = store.short_circuit(&finalized)?.unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)?
        .set_len(100)?;

    // A failed read open is all it takes to queue the block.
    assert!(store.open_reader_by_id(9, 0, 4096).is_err());

    let executor = Arc::new(GroupExecutor::new("heartbeat-truncated-test", 1, 2));
    let reports = Arc::new(DashMap::new());
    HeartbeatTask::invalidate_corrupt_task(executor.clone(), store.clone(), reports.clone());
    drain(&executor);

    let (_, report) = reports.remove(&9).expect("missing corrupted report");
    assert_eq!(report.status, BlockReportStatus::Deleted);
    assert!(report.corrupted);
    assert_eq!(report.block_size, 4096);
    assert!(store.get_block(9).is_err());
    assert!(!std::path::Path::new(&path).exists());
    std::mem::forget(executor);
    Ok(())
}

#[test]
fn corrupt_block_held_by_a_writer_is_cleaned_up_once_released() -> CommonResult<()> {
    let (store, _) = corrupt_checksummed_block("heartbeat-task-corrupt-writer", 8)?;
    store.report_corrupt(8, "test");

    let executor = Arc::new(GroupExecutor::new("heartbeat-corrupt-writer-test", 1, 2));
    let reports = Arc::new(DashMap::new());
    let lease = store.ordinary_write_lease(8)?;
    HeartbeatTask::invalidate_corrupt_task(executor.clone(), store.clone(), reports.clone());
    drain(&executor);
    assert!(reports.is_empty(), "a block with a writer must be kept");
    assert!(store.get_block(8).is_ok());

    // The writer goes away without replacing the block. No new read reports
    // it, yet the next heartbeat still removes it.
    drop(lease);
    HeartbeatTask::invalidate_corrupt_task(executor.clone(), store.clone(), reports.clone());
    drain(&executor);
    let report = reports.get(&8).expect("requeued block was not cleaned up");
    assert!(report.corrupted);
    assert!(store.get_block(8).is_err());
    std::mem::forget(executor);
    Ok(())
}

/// A quarantined block cannot be removed until released. Repeated
/// DeleteBlock commands must not reschedule it on every heartbeat.
#[test]
fn quarantined_block_delete_is_parked_not_rescheduled() -> CommonResult<()> {
    let dataset = curvine_storage_local::BlockDataset::new_spdk_test_dataset(1, 1 << 30)?;
    let store = BlockStore::from_dataset_for_test(dataset);
    let block = ExtendedBlock::new(9, 4096, StorageType::SpdkDisk, FileType::File);
    store.open_block(&block)?;
    store.abort_block_uncertain(&block)?;
    assert!(store.get_raw_block_opt(9)?.unwrap().is_quarantined());

    let executor = Arc::new(GroupExecutor::new("heartbeat-task-quarantine", 1, 2));
    let reports = Arc::new(DashMap::new());
    let pending = Arc::new(DashSet::new());
    let cmd = || {
        vec![WorkerCommand::DeleteBlock(DeleteBlockCmd {
            blocks: vec![9],
        })]
    };

    // Heartbeat 1: the remove runs, is refused, and the id is parked.
    HeartbeatTask::delete_block_task(
        executor.clone(),
        store.clone(),
        cmd(),
        reports.clone(),
        pending.clone(),
    );
    drain(&executor);
    assert!(pending.contains(&9), "quarantined id must stay pending");
    assert!(reports.is_empty(), "must not report Deleted");
    assert_eq!(store.read()?.num_blocks_to_delete(), 0);

    // Heartbeat 2: same command; nothing must be scheduled again.
    HeartbeatTask::delete_block_task(
        executor.clone(),
        store.clone(),
        cmd(),
        reports.clone(),
        pending.clone(),
    );
    assert_eq!(
        store.read()?.num_blocks_to_delete(),
        0,
        "a parked quarantined block must not be rescheduled"
    );
    drain(&executor);
    assert!(reports.is_empty());
    assert!(store.get_raw_block_opt(9)?.unwrap().is_quarantined());
    std::mem::forget(executor);
    Ok(())
}
