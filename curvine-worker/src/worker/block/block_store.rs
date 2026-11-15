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

use crate::worker::block::{BlockMeta, BlockState};
use crate::worker::storage::{
    BlockDataset, BlockLayout, BlockReadContext, BlockTruncated, BlockWriteContext, ChecksumVerify,
    Dataset,
};
use crate::worker::Worker;
use curvine_config::ClusterConf;
use curvine_core_error::{CommonError, CommonResult};
use curvine_io::IOError;
use curvine_model::{ExtendedBlock, StorageInfo, StorageType};
use dashmap::{DashMap, DashSet};
use parking_lot::{Mutex, MutexGuard};
use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

const BLOCK_LOCK_STRIPES: usize = 256;

/// Read size for the full re-check of a block reported corrupt.
const REVERIFY_READ_LEN: i32 = 1024 * 1024;

#[derive(Default)]
struct BlockUse {
    ordinary_writers: usize,
    replication: Option<String>,
}

/// Keeps replication exclusive against active ordinary/batch/short-circuit RPC sessions
/// and deletion. A disconnected short-circuit FD must additionally be isolated by
/// unlinking its abandoned staging allocation before opening a replication successor.
/// The writer's file must be closed before releasing this lease.
pub(crate) struct BlockWriteLease {
    uses: Arc<Vec<Mutex<HashMap<i64, BlockUse>>>>,
    block_id: i64,
    replication: bool,
}

impl Drop for BlockWriteLease {
    fn drop(&mut self) {
        let mut uses = self.uses[self.block_id as u64 as usize % self.uses.len()].lock();
        if let Some(entry) = uses.get_mut(&self.block_id) {
            if self.replication {
                entry.replication = None;
            } else {
                entry.ordinary_writers -= 1;
            }
            if entry.replication.is_none() && entry.ordinary_writers == 0 {
                uses.remove(&self.block_id);
            }
        }
    }
}

/// Outcome of re-checking a block whose read failed checksum verification
/// or found the block file truncated.
#[derive(Debug)]
pub enum CorruptCheck {
    /// The re-check confirmed the damage; block and sidecar are gone.
    Removed(BlockMeta),
    /// The block was kept for good: it is already gone, or it is not
    /// truncated and has no sidecar or passed the second verification.
    Kept(String),
    /// The block cannot be checked yet because a writer holds it or it is
    /// not finalized. The caller requeues it for a later heartbeat.
    Retry(String),
}

/// Outcome of the lock-free verification of one block by the startup scan.
#[derive(Debug, PartialEq, Eq)]
pub enum ScanResult {
    /// Every chunk matched its sidecar.
    Verified,
    /// A chunk failed its CRC.
    Mismatch,
    /// The block has no sidecar and the policy reads it unverified.
    NoSidecar,
    /// The block has no sidecar but the policy requires one.
    Unverifiable,
    /// Not a finalized file block any more, or the scan stopped.
    Skipped,
}

/// The store's checksum policy rejects `meta` when it has no sidecar.
/// SPDK blocks never have one and are not checked.
fn requires_sidecar(verify: ChecksumVerify, meta: &BlockMeta) -> bool {
    verify == ChecksumVerify::RequireSidecar && meta.storage_type() != StorageType::SpdkDisk
}

#[derive(Clone)]
pub struct BlockStore {
    state: Arc<RwLock<BlockDataset>>,
    block_locks: Arc<Vec<Mutex<()>>>,
    uses: Arc<Vec<Mutex<HashMap<i64, BlockUse>>>>,
    replication_owner: Option<String>,
    /// Blocks whose read failed a chunk CRC or found the file truncated,
    /// waiting to be re-checked, with the number of re-checks that already
    /// failed with an error.
    corrupt_blocks: Arc<DashMap<i64, u32>>,
    /// Blocks removed as corrupt whose deletion master has not acknowledged.
    /// `open_block` refuses them: a replacement finalized before the report
    /// lands would otherwise lose its location to that stale report.
    fenced_blocks: Arc<DashSet<i64>>,
    /// Quarantined since the last heartbeat; reported so the master drops the location.
    quarantined_blocks: Arc<DashSet<i64>>,
    /// Test-only: forces the next finalize prepare to fail (one-shot).
    #[cfg(test)]
    force_transient_finalize_failure: Arc<std::sync::atomic::AtomicBool>,
    /// Test-only: forces the next abort to fail (one-shot).
    #[cfg(test)]
    force_transient_abort_failure: Arc<std::sync::atomic::AtomicBool>,
}

impl BlockStore {
    pub fn new(cluster_id: &str, conf: &ClusterConf) -> CommonResult<Self> {
        let dataset = BlockDataset::from_conf(cluster_id, conf)?;
        let block_store = BlockStore {
            state: Arc::new(RwLock::new(dataset)),
            uses: Arc::new(
                (0..BLOCK_LOCK_STRIPES)
                    .map(|_| Mutex::new(HashMap::new()))
                    .collect(),
            ),
            replication_owner: None,
            block_locks: Arc::new((0..BLOCK_LOCK_STRIPES).map(|_| Mutex::new(())).collect()),
            corrupt_blocks: Arc::new(DashMap::new()),
            fenced_blocks: Arc::new(DashSet::new()),
            quarantined_blocks: Arc::new(DashSet::new()),
            #[cfg(test)]
            force_transient_finalize_failure: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            force_transient_abort_failure: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        Ok(block_store)
    }

    /// Wraps a pre-built dataset, bypassing `ClusterConf` (for synthetic
    /// SPDK test datasets, which have no config-string form).
    #[cfg(test)]
    pub fn from_dataset_for_test(dataset: BlockDataset) -> Self {
        BlockStore {
            state: Arc::new(RwLock::new(dataset)),
            uses: Arc::new(
                (0..BLOCK_LOCK_STRIPES)
                    .map(|_| Mutex::new(HashMap::new()))
                    .collect(),
            ),
            replication_owner: None,
            block_locks: Arc::new((0..BLOCK_LOCK_STRIPES).map(|_| Mutex::new(())).collect()),
            corrupt_blocks: Arc::new(DashMap::new()),
            fenced_blocks: Arc::new(DashSet::new()),
            quarantined_blocks: Arc::new(DashSet::new()),
            force_transient_finalize_failure: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            force_transient_abort_failure: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_force_transient_finalize_failure_for_test(&self, enabled: bool) {
        self.force_transient_finalize_failure
            .store(enabled, std::sync::atomic::Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn set_force_transient_abort_failure_for_test(&self, enabled: bool) {
        self.force_transient_abort_failure
            .store(enabled, std::sync::atomic::Ordering::Release);
    }

    fn block_uses(&self, block_id: i64) -> &Mutex<HashMap<i64, BlockUse>> {
        &self.uses[block_id as u64 as usize % self.uses.len()]
    }

    /// Existing ordinary writer concurrency is unchanged; only replication is exclusive.
    pub(crate) fn ordinary_write_lease(
        &self,
        block_id: i64,
    ) -> CommonResult<Option<BlockWriteLease>> {
        if self.replication_owner.is_some() {
            return Ok(None);
        }
        let _lock = self.block_lock(block_id, "write_lease");
        self.check_replication_owner(block_id)?;
        self.block_uses(block_id)
            .lock()
            .entry(block_id)
            .or_default()
            .ordinary_writers += 1;
        Ok(Some(BlockWriteLease {
            uses: self.uses.clone(),
            block_id,
            replication: false,
        }))
    }

    pub(crate) fn replication_write_lease(
        &self,
        block_id: i64,
        token: &str,
    ) -> CommonResult<(Self, BlockWriteLease)> {
        let _lock = self.block_lock(block_id, "replication_lease");
        let mut uses = self.block_uses(block_id).lock();
        let entry = uses.entry(block_id).or_default();
        if entry.replication.is_some() || entry.ordinary_writers != 0 {
            return curvine_core_error::err_box!("block {} has an active writer", block_id);
        }
        entry.replication = Some(token.to_owned());
        let mut scoped = self.clone();
        scoped.replication_owner = Some(token.to_owned());
        Ok((
            scoped,
            BlockWriteLease {
                uses: self.uses.clone(),
                block_id,
                replication: true,
            },
        ))
    }

    /// True when more ordinary writers than the finalizing one hold a lease.
    /// Leases are acquired under the block stripe lock, so a caller holding
    /// that lock sees a stable count. A finalize without a lease (tests,
    /// replication) counts as one writer.
    fn has_other_ordinary_writers(&self, block_id: i64) -> bool {
        self.block_uses(block_id)
            .lock()
            .get(&block_id)
            .is_some_and(|entry| entry.ordinary_writers > 1)
    }

    // Called under the block stripe lock, so acquisition cannot race admission/removal.
    fn check_replication_owner(&self, block_id: i64) -> CommonResult<()> {
        let uses = self.block_uses(block_id).lock();
        let owner = uses
            .get(&block_id)
            .and_then(|entry| entry.replication.as_deref());
        if owner != self.replication_owner.as_deref() {
            return curvine_core_error::err_box!(
                "block {} is fenced by a replication writer",
                block_id
            );
        }
        Ok(())
    }

    pub(crate) fn write(&self) -> CommonResult<RwLockWriteGuard<'_, BlockDataset>> {
        match self.state.write() {
            Ok(state) => Ok(state),
            Err(e) => {
                log::error!("fatal block store write lock poisoned: {}", e);
                std::process::abort();
            }
        }
    }

    pub(crate) fn read(&self) -> CommonResult<RwLockReadGuard<'_, BlockDataset>> {
        match self.state.read() {
            Ok(state) => Ok(state),
            Err(e) => {
                log::error!("fatal block store read lock poisoned: {}", e);
                std::process::abort();
            }
        }
    }

    fn block_lock(&self, id: i64, operation: &str) -> MutexGuard<'_, ()> {
        let started = Instant::now();
        let index = (id as u64 as usize) % self.block_locks.len();
        let guard = self.block_locks[index].lock();
        self.observe_stripe_lock_wait(operation, started.elapsed());
        guard
    }

    fn with_dataset_write<T>(
        &self,
        operation: &str,
        action: impl FnOnce(&mut BlockDataset) -> CommonResult<T>,
    ) -> CommonResult<T> {
        let waiting = Instant::now();
        let mut state = self.write()?;
        let wait_elapsed = waiting.elapsed();
        let holding = Instant::now();
        let result = action(&mut state);
        let hold_elapsed = holding.elapsed();
        drop(state);

        self.observe_dataset_lock_wait(operation, wait_elapsed);
        self.observe_dataset_lock_hold(operation, hold_elapsed);
        result
    }

    fn observe_stripe_lock_wait(&self, operation: &str, elapsed: Duration) {
        if let Ok(metrics) = Worker::get_metrics() {
            metrics
                .block_store_stripe_lock_wait_us
                .with_label_values(&[operation])
                .observe(elapsed.as_micros() as f64);
        }
    }

    fn observe_dataset_lock_wait(&self, operation: &str, elapsed: Duration) {
        if let Ok(metrics) = Worker::get_metrics() {
            metrics
                .block_dataset_write_lock_wait_us
                .with_label_values(&[operation])
                .observe(elapsed.as_micros() as f64);
        }
    }

    fn observe_dataset_lock_hold(&self, operation: &str, elapsed: Duration) {
        if let Ok(metrics) = Worker::get_metrics() {
            metrics
                .block_dataset_write_lock_hold_us
                .with_label_values(&[operation])
                .observe(elapsed.as_micros() as f64);
        }
    }

    fn observe_file_layout_operation(&self, operation: &str, elapsed: Duration) {
        if let Ok(metrics) = Worker::get_metrics() {
            metrics
                .file_layout_operation_us
                .with_label_values(&[operation])
                .observe(elapsed.as_micros() as f64);
        }
    }

    pub fn open_block(&self, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        let _block_lock = self.block_lock(block.id, "open");
        self.check_replication_owner(block.id)?;
        if self.fenced_blocks.contains(&block.id) {
            return curvine_core_error::err_box!(
                "block {} was removed as corrupt; waiting for master to acknowledge the deletion",
                block.id
            );
        }
        let reservation =
            self.with_dataset_write("open", |state| state.reserve_file_open(block))?;

        let Some(reservation) = reservation else {
            return self.with_dataset_write("open", |state| state.open_block(block));
        };

        let started = Instant::now();
        let prepared = reservation.prepare(block);
        self.observe_file_layout_operation("open", started.elapsed());
        match prepared {
            Ok(meta) => self
                .with_dataset_write("open", |state| state.complete_file_open(&reservation, meta)),
            Err(error) => {
                if let Err(rollback) =
                    self.with_dataset_write("open", |state| state.rollback_file_open(&reservation))
                {
                    log::error!(
                        "failed to roll back block {} allocation after {}: {}",
                        block.id,
                        error,
                        rollback
                    );
                }
                Err(error)
            }
        }
    }

    pub fn finalize_block(&self, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        let _block_lock = self.block_lock(block.id, "finalize");
        self.check_replication_owner(block.id)?;
        let reservation =
            self.with_dataset_write("finalize", |state| state.reserve_file_finalize(block))?;

        let Some(mut reservation) = reservation else {
            return self.with_dataset_write("finalize", |state| state.finalize_block(block));
        };
        if self.has_other_ordinary_writers(block.id) {
            // Another session holds an open handle on the same staging inode
            // and may write after the checksum pass, including after publish;
            // its own finalize then short-circuits on the already finalized
            // block. A missing sidecar is safe, a stale one reads as corruption.
            log::warn!(
                "block {} finalized with concurrent writers, skipping checksum",
                block.id
            );
            reservation.skip_checksum();
        }

        let started = Instant::now();
        let plan = reservation.prepare(block.len);
        #[cfg(test)]
        let plan = if self
            .force_transient_finalize_failure
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            curvine_core_error::err_box!("injected transient finalize failure for test")
        } else {
            plan
        };
        self.observe_file_layout_operation("finalize", started.elapsed());
        let plan = match plan {
            Ok(plan) => plan,
            Err(error) => {
                if error.to_string().contains("length mismatch") {
                    // Deterministic mismatch: retrying can never succeed, so
                    // abort the block instead of looping forever.
                    if let Err(abort) =
                        self.with_dataset_write("finalize", |state| state.abort_block(block))
                    {
                        log::error!(
                            "failed to abort block {} after prepare error {}: {}; rollback to Writing",
                            block.id,
                            error,
                            abort
                        );
                        self.rollback_finalize_reservation(block.id, &abort, |state| {
                            state.rollback_file_finalize(&reservation)
                        });
                    }
                } else {
                    // Transient failure: release the reservation so the block
                    // returns to Writing and finalize can be retried.
                    self.rollback_finalize_reservation(block.id, &error, |state| {
                        state.rollback_file_finalize(&reservation)
                    });
                }
                return Err(error);
            }
        };

        match self.with_dataset_write("finalize", |state| {
            state.publish_file_finalize(&reservation, plan)
        }) {
            Ok((meta, sync)) => {
                // Durability only: run it outside the dataset lock, still under
                // the stripe lock.
                if let Some(sync) = sync {
                    sync.finish();
                }
                Ok(meta)
            }
            Err(error) => {
                self.rollback_finalize_reservation(block.id, &error, |state| {
                    state.rollback_file_finalize(&reservation)
                });
                Err(error)
            }
        }
    }

    /// Roll back a finalize reservation after a failed prepare or publish.
    /// Failures are logged while the caller keeps the original error.
    fn rollback_finalize_reservation(
        &self,
        block_id: i64,
        reason: &CommonError,
        rollback: impl FnOnce(&mut BlockDataset) -> CommonResult<()>,
    ) {
        if let Err(failed) = self.with_dataset_write("finalize", rollback) {
            log::error!(
                "failed to roll back block {} finalization after {}: {}",
                block_id,
                reason,
                failed
            );
        }
    }

    pub fn abort_block(&self, block: &ExtendedBlock) -> CommonResult<()> {
        self.abort_with(block, "abort", false)
    }

    /// Abort with an uncertain (timed-out) write/flush outcome; see `Dataset::abort_block_uncertain`.
    pub fn abort_block_uncertain(&self, block: &ExtendedBlock) -> CommonResult<()> {
        self.abort_with(block, "abort_uncertain", true)
    }

    /// Common abort path: stripe lock, replication-owner fence, then under one
    /// `with_dataset_write` skip `Finalized` blocks and dispatch to
    /// `abort_block` or `abort_block_uncertain`. Leases are not exclusive, so
    /// a late abort must never delete an acked replica. Reads raw meta.
    /// Missing blocks return `Ok(())`.
    ///
    /// Exception: an uncertain abort of an SPDK block is neither fenced nor
    /// skipped for `Finalized`. All writers share one extent
    /// (`BdevLayout::prepare_write`), so timed-out I/O may still land on a
    /// replica another writer finalized; quarantine only reserves the extent.
    fn abort_with(&self, block: &ExtendedBlock, op: &str, uncertain: bool) -> CommonResult<()> {
        let _block_lock = self.block_lock(block.id, op);
        // The stripe lock pins the storage type: open/finalize/remove take it too.
        let own_spdk_extent = uncertain
            && self
                .read()?
                .get_block(block.id)
                .is_some_and(|m| m.storage_type() == StorageType::SpdkDisk);
        if !own_spdk_extent {
            self.check_replication_owner(block.id)?;
        }
        #[cfg(test)]
        if self
            .force_transient_abort_failure
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            return curvine_core_error::err_box!("injected transient abort failure for test");
        }
        let result = self.with_dataset_write(op, |state| {
            if !own_spdk_extent && state.get_block(block.id).is_some_and(|m| m.is_final()) {
                return Ok(());
            }
            if uncertain {
                state.abort_block_uncertain(block)
            } else {
                state.abort_block(block)
            }
        });
        // Report even if persist failed: in-memory state is already Quarantined.
        if uncertain
            && self
                .get_raw_block_opt(block.id)
                .ok()
                .flatten()
                .is_some_and(|m| m.is_quarantined())
        {
            self.quarantined_blocks.insert(block.id);
        }
        result
    }

    pub(crate) fn abort_replication_write(&self, block_id: i64) -> CommonResult<()> {
        self.abort_with(
            &ExtendedBlock::with_id(block_id),
            "abort_replication",
            false,
        )
    }

    /// Aborts a batch-opened block unless it is already finalized; see
    /// [`BlockStore::abort_with`] for the check-and-abort contract.
    pub(crate) fn abort_batch_write(&self, block_id: i64) -> CommonResult<()> {
        self.abort_with(&ExtendedBlock::with_id(block_id), "abort_batch", false)
    }

    pub fn get_block(&self, id: i64) -> CommonResult<BlockMeta> {
        let state = self.read()?;
        let b = state
            .get_readable_block(id)
            .ok_or_else(|| curvine_core_error::err_msg!(format!("block {} not exists", id)))?;
        Ok(b.clone())
    }

    /// The raw pending meta, unlike the readable view (get_block()) which can
    /// return a stale committed_rewrites entry during a rewrite. Use this to
    /// check if a pending write/finalize is terminal.
    pub fn get_raw_block_opt(&self, id: i64) -> CommonResult<Option<BlockMeta>> {
        let state = self.read()?;
        Ok(state.get_block(id).cloned())
    }

    pub fn open_writer(&self, meta: &BlockMeta, off: i64) -> CommonResult<BlockWriteContext> {
        let (layout, dir) = {
            let state = self.read()?;
            state.layout_for(meta)?
        };
        layout.open_writer(&dir, meta, off).map_err(Into::into)
    }

    pub fn open_reader(
        &self,
        meta: &BlockMeta,
        off: i64,
        logical_len: i64,
    ) -> CommonResult<BlockReadContext> {
        // Hold the dataset read lock while opening, so a finalize publish
        // cannot pair the block file with another generation's sidecar.
        let state = self.read()?;
        let (layout, dir) = state.layout_for(meta)?;
        layout
            .open_reader(&dir, meta, off, logical_len, state.checksum_verify())
            .inspect_err(|e| self.report_if_truncated(meta, e))
            .map_err(Into::into)
    }

    /// Opens the currently readable generation while the dataset read lock is
    /// held, so a finalize publish cannot replace its path before the FD opens.
    /// `logical_len` may exceed the worker's physical bytes for sparse tails.
    pub fn open_reader_by_id(
        &self,
        id: i64,
        off: i64,
        logical_len: i64,
    ) -> CommonResult<(BlockMeta, BlockReadContext)> {
        self.open_reader_by_id_inner(id, off, Some(logical_len))
    }

    /// Opens the currently readable generation using its stored metadata length
    /// as the logical read boundary. This is intended for internal block copies
    /// that do not have a client-provided logical length.
    pub fn open_reader_by_id_at_stored_len(
        &self,
        id: i64,
        off: i64,
    ) -> CommonResult<(BlockMeta, BlockReadContext)> {
        self.open_reader_by_id_inner(id, off, None)
    }

    fn open_reader_by_id_inner(
        &self,
        id: i64,
        off: i64,
        logical_len: Option<i64>,
    ) -> CommonResult<(BlockMeta, BlockReadContext)> {
        let state = self.read()?;
        let meta = state
            .get_readable_block(id)
            .ok_or_else(|| curvine_core_error::err_msg!(format!("block {} not exists", id)))?
            .clone();
        let logical_len = logical_len.unwrap_or_else(|| meta.len());
        let (layout, dir) = state.layout_for(&meta)?;
        let reader = layout
            .open_reader(&dir, &meta, off, logical_len, state.checksum_verify())
            .inspect_err(|e| self.report_if_truncated(&meta, e))?;
        Ok((meta, reader))
    }

    /// Returns a local path only while no finalize publication is in progress.
    /// A finalizing file can be atomically renamed before a co-located client
    /// opens the returned path, so callers must use `open_reader_by_id` then.
    /// Also `None` while checksum verification is on: a client reading the
    /// file directly would bypass the check.
    pub fn short_circuit_by_id(&self, id: i64) -> CommonResult<Option<(BlockMeta, String, i64)>> {
        let state = self.read()?;
        if state.checksum_verify().is_enabled() {
            return Ok(None);
        }
        if matches!(
            state.get_block(id).map(BlockMeta::state),
            Some(BlockState::Finalizing)
        ) {
            return Ok(None);
        }
        let meta = state
            .get_readable_block(id)
            .ok_or_else(|| curvine_core_error::err_msg!(format!("block {} not exists", id)))?
            .clone();
        let (layout, dir) = state.layout_for(&meta)?;
        let Some(path) = layout.short_circuit(&dir, &meta)? else {
            return Ok(None);
        };
        let physical_len = std::fs::metadata(&path)?.len() as i64;
        Ok(Some((meta, path, physical_len)))
    }

    pub fn short_circuit(&self, meta: &BlockMeta) -> CommonResult<Option<String>> {
        let (layout, dir) = {
            let state = self.read()?;
            state.layout_for(meta)?
        };
        layout.short_circuit(&dir, meta)
    }

    pub fn worker_id(&self) -> CommonResult<u32> {
        let state = self.read()?;
        Ok(state.worker_id())
    }

    pub fn cluster_id(&self) -> CommonResult<String> {
        let state = self.read()?;
        Ok(state.cluster_id().to_string())
    }

    pub fn all_blocks(&self) -> CommonResult<Vec<BlockMeta>> {
        let state = self.read()?;
        Ok(state.all_blocks())
    }

    pub fn remove_block(&self, id: i64) -> CommonResult<()> {
        let _block_lock = self.block_lock(id, "remove");
        self.check_replication_owner(id)?;
        let block = ExtendedBlock::with_id(id);
        self.with_dataset_write("remove", |state| state.remove_block(&block))
    }

    // Asynchronously delete block.
    pub fn async_remove_block(&self, id: i64) -> CommonResult<Option<BlockMeta>> {
        let _block_lock = self.block_lock(id, "remove");
        if let Err(error) = self.check_replication_owner(id) {
            self.write()?.decrement_blocks_to_delete();
            return Err(error);
        }
        let removed = self.with_dataset_write("remove", |state| {
            let remove_result = match state.get_block(id) {
                Some(_) => state.remove_block_state_by_id(id).map(Some),
                None => Ok(None),
            };
            // Heartbeat increments this counter before scheduling the task.
            // Consume it even when metadata removal reports an error.
            state.decrement_blocks_to_delete();
            remove_result
        })?;

        let Some(removed) = removed else {
            return Ok(None);
        };

        if let Err(e) = removed.deallocate() {
            self.with_dataset_write("restore", |state| {
                state.restore_removed_block(removed);
                Ok(())
            })?;
            return Err(e);
        }

        removed.release();
        Ok(Some(removed.meta))
    }

    /// Record that a read of `id` failed a chunk CRC. The heartbeat re-checks
    /// the block later, so the read path never blocks on removal.
    pub fn report_corrupt(&self, id: i64, path: &str) {
        if self.corrupt_blocks.insert(id, 0).is_none() {
            log::error!(
                "block {} failed checksum verification, path {}; queued for invalidation",
                id,
                path
            );
        }
    }

    /// Queue a finalized block whose open failed because its file is shorter
    /// than the committed length. Like a CRC failure, the heartbeat re-checks
    /// it under the stripe lock before removing anything.
    fn report_if_truncated(&self, meta: &BlockMeta, error: &IOError) {
        if !meta.is_final() {
            return;
        }
        let Some(truncated) = BlockTruncated::from_error(error) else {
            return;
        };
        if self.corrupt_blocks.insert(meta.id, 0).is_none() {
            log::error!("{}; queued for invalidation", truncated);
        }
    }

    /// Drain the queued blocks with their count of failed re-checks.
    pub fn take_corrupt_blocks(&self) -> Vec<(i64, u32)> {
        let ids: Vec<i64> = self.corrupt_blocks.iter().map(|e| *e.key()).collect();
        ids.into_iter()
            .filter_map(|id| self.corrupt_blocks.remove(&id))
            .collect()
    }

    /// Drain blocks quarantined since the last call; removed/unquarantined ones are skipped.
    /// Removing the id before the lookup is safe: `get_raw_block_opt` cannot fail
    /// (`read()` aborts on poison; the rest is a map lookup).
    pub fn take_quarantined_blocks(&self) -> Vec<BlockMeta> {
        let ids: Vec<i64> = self.quarantined_blocks.iter().map(|e| *e.key()).collect();
        ids.into_iter()
            .filter_map(|id| {
                self.quarantined_blocks.remove(&id);
                self.get_raw_block_opt(id).ok().flatten()
            })
            .filter(|meta| meta.is_quarantined())
            .collect()
    }

    /// Verify a finalized block without the stripe lock, reading at most
    /// `read_len` bytes at a time and calling `on_read` with each length
    /// read, which may sleep to limit the rate or return false to stop. A
    /// read that fails its CRC is still passed to `on_read`, so it is
    /// counted and paced like the others.
    ///
    /// The reader opens the block file and its sidecar together under the
    /// dataset read lock, so a concurrent rewrite or delete cannot mix two
    /// generations. Finding a mismatch only reports it: removal re-checks
    /// the block under the lock.
    pub fn scan_block(
        &self,
        id: i64,
        read_len: i32,
        mut on_read: impl FnMut(i64) -> bool,
    ) -> CommonResult<ScanResult> {
        let (meta, required, mut reader) = {
            let state = self.read()?;
            let Some(meta) = state.get_block(id).cloned() else {
                return Ok(ScanResult::Skipped);
            };
            if *meta.state() != BlockState::Finalized
                || meta.storage_type() == StorageType::SpdkDisk
            {
                return Ok(ScanResult::Skipped);
            }
            let (layout, dir) = state.layout_for(&meta)?;
            let reader =
                layout.open_reader(&dir, &meta, 0, meta.len(), ChecksumVerify::AllowMissing)?;
            let required = requires_sidecar(state.checksum_verify(), &meta);
            (meta, required, reader)
        };
        if !reader.is_verified() {
            return Ok(if required {
                ScanResult::Unverifiable
            } else {
                ScanResult::NoSidecar
            });
        }
        while reader.block_pos() < meta.len() {
            let before = reader.block_pos();
            if let Err(e) = reader.read_region(false, read_len) {
                if reader.checksum_mismatch() {
                    on_read((read_len as i64).min(meta.len() - before));
                    return Ok(ScanResult::Mismatch);
                }
                return Err(e.into());
            }
            if !on_read(reader.block_pos() - before) {
                return Ok(ScanResult::Skipped);
            }
        }
        Ok(ScanResult::Verified)
    }

    /// Put a block back for the next heartbeat. A fresh read report that
    /// arrived meanwhile keeps its lower count.
    pub fn requeue_corrupt(&self, id: i64, failed_checks: u32) {
        self.corrupt_blocks
            .entry(id)
            .and_modify(|n| *n = (*n).min(failed_checks))
            .or_insert(failed_checks);
    }

    /// Master acknowledged the corrupted deletion of `id`; writers may
    /// recreate it again.
    pub fn unfence(&self, id: i64) {
        self.fenced_blocks.remove(&id);
    }

    /// Re-check a block reported corrupt and remove it with its sidecar only
    /// when the damage is confirmed: its file is still shorter than the
    /// committed length, or the whole block fails checksum verification again.
    /// Holding the stripe lock keeps finalize, abort and delete out while the
    /// block is read.
    pub fn invalidate_corrupt_block(&self, id: i64) -> CommonResult<CorruptCheck> {
        let _block_lock = self.block_lock(id, "invalidate");
        if self.block_uses(id).lock().contains_key(&id) {
            return Ok(CorruptCheck::Retry("block has an active writer".into()));
        }
        let (block_len, opened) = {
            let state = self.read()?;
            let Some(meta) = state.get_block(id).cloned() else {
                return Ok(CorruptCheck::Kept("block no longer exists".into()));
            };
            if *meta.state() != BlockState::Finalized {
                return Ok(CorruptCheck::Retry(format!(
                    "block is {:?}, not finalized",
                    meta.state()
                )));
            }
            let (layout, dir) = state.layout_for(&meta)?;
            let opened =
                layout.open_reader(&dir, &meta, 0, meta.len(), ChecksumVerify::AllowMissing);
            (meta.len(), opened)
        };

        // A truncated file is damage on its own; any other block must fail
        // a second full checksum verification.
        let truncation = match opened {
            Ok(reader) => {
                if !reader.is_verified() {
                    return Ok(CorruptCheck::Kept("block has no checksum sidecar".into()));
                }
                if !Self::reverify_finds_mismatch(reader, block_len)? {
                    return Ok(CorruptCheck::Kept(
                        "second verification found no mismatch".into(),
                    ));
                }
                None
            }
            Err(e) => match BlockTruncated::from_error(&e) {
                Some(truncated) => Some(truncated.to_string()),
                None => return Err(e.into()),
            },
        };

        let removed = self.with_dataset_write("invalidate", |state| {
            match state.get_block(id).map(BlockMeta::state) {
                Some(BlockState::Finalized) => state.remove_block_state_by_id(id).map(Some),
                _ => Ok(None),
            }
        })?;
        let Some(removed) = removed else {
            return Ok(CorruptCheck::Kept("block changed during re-check".into()));
        };
        if let Err(e) = removed.deallocate() {
            self.with_dataset_write("restore", |state| {
                state.restore_removed_block(removed);
                Ok(())
            })?;
            return Err(e);
        }
        removed.release();
        // Still under the stripe lock, so no open_block slips in between.
        self.fenced_blocks.insert(id);
        match truncation {
            Some(truncation) => {
                if let Ok(metrics) = Worker::get_metrics() {
                    metrics.truncated_invalidated_count.inc();
                }
                log::error!("removed block {}: {}", id, truncation);
            }
            None => {
                if let Ok(metrics) = Worker::get_metrics() {
                    metrics.checksum_invalidated_count.inc();
                }
                log::error!(
                    "removed block {} after it failed checksum verification twice",
                    id
                );
            }
        }
        Ok(CorruptCheck::Removed(removed.meta))
    }

    /// Read the whole block through its checksum and report whether a chunk
    /// fails its CRC. Other read errors are returned.
    fn reverify_finds_mismatch(mut reader: BlockReadContext, block_len: i64) -> CommonResult<bool> {
        while reader.block_pos() < block_len {
            if let Err(e) = reader.read_region(false, REVERIFY_READ_LEN) {
                if !reader.checksum_mismatch() {
                    return Err(e.into());
                }
                return Ok(true);
            }
        }
        Ok(false)
    }

    // Get all storage information and check whether the storage directory is normal.
    // If the directory is not normal, the storage will be marked as failed.
    // This method is called by the heartbeat thread and returns all storage information, including failed storage.
    pub fn get_and_check_storages(&self) -> CommonResult<Vec<StorageInfo>> {
        let state = self.read()?;
        Ok(state.get_and_check_storages())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::block::{BlockState, ChecksumScan, ScanStats};
    use crate::worker::storage::{Dataset, FileLayout};
    use curvine_config::WorkerConf;
    use curvine_io::DataSlice;
    use curvine_runtime::common::FileUtils;
    use std::io::Write;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Instant;

    fn create_store(name: &str) -> CommonResult<BlockStore> {
        create_store_with_capacity(name, "1KB")
    }

    fn create_store_with_capacity(name: &str, capacity: &str) -> CommonResult<BlockStore> {
        let conf = ClusterConf {
            format_worker: true,
            worker: WorkerConf {
                dir_reserved: "0".to_string(),
                data_dir: vec![format!("[MEM:{capacity}]../testing/block-store-{name}")],
                ..WorkerConf::default()
            },
            ..ClusterConf::default()
        };
        BlockStore::new("test", &conf)
    }

    fn write_block(path: &str, bytes: usize) -> CommonResult<()> {
        const CHUNK_SIZE: usize = 1024 * 1024;

        let mut file = std::fs::File::create(path)?;
        let chunk = vec![0_u8; CHUNK_SIZE];
        let mut remaining = bytes;
        while remaining > 0 {
            let len = remaining.min(CHUNK_SIZE);
            file.write_all(&chunk[..len])?;
            remaining -= len;
        }
        file.sync_all()?;
        Ok(())
    }

    fn finalized_block(store: &BlockStore) -> CommonResult<()> {
        let mut block = ExtendedBlock::with_id(1);
        block.len = 100;
        finalize_block(store, &block)?;
        Ok(())
    }

    fn replace_block_file_with_non_empty_dir(store: &BlockStore) -> CommonResult<String> {
        let meta = store.get_block(1)?;
        let path = store
            .short_circuit(&meta)?
            .expect("file layout must expose a local path");
        FileUtils::delete_path(&path, false)?;
        std::fs::create_dir(&path)?;
        std::fs::write(std::path::Path::new(&path).join("child"), b"data")?;
        Ok(path)
    }

    #[test]
    fn async_remove_missing_block_releases_delete_count() -> CommonResult<()> {
        let store = create_store("missing-block")?;
        store.read()?.increment_blocks_to_delete();

        assert!(store.async_remove_block(1)?.is_none());
        assert_eq!(store.read()?.num_blocks_to_delete(), 0);
        Ok(())
    }

    #[test]
    fn async_remove_invalid_dir_keeps_block_for_retry() -> CommonResult<()> {
        let store = create_store("invalid-dir")?;
        {
            let mut state = store.write()?;
            let mut meta = BlockMeta::new(1, 100, state.dir_iter().next().unwrap());
            meta.dir_id = u32::MAX;
            state.put_test_meta(meta);
            state.increment_blocks_to_delete();
        }

        assert!(store.async_remove_block(1).is_err());
        let state = store.read()?;
        assert!(state.get_block(1).is_some());
        assert_eq!(state.num_blocks_to_delete(), 0);
        Ok(())
    }

    #[test]
    fn async_remove_deallocate_error_keeps_block_for_retry() -> CommonResult<()> {
        let store = create_store("deallocate-error")?;
        finalized_block(&store)?;
        let path = replace_block_file_with_non_empty_dir(&store)?;
        store.read()?.increment_blocks_to_delete();

        let result = store.async_remove_block(1);
        assert!(result.is_err());
        {
            let state = store.read()?;
            assert!(state.get_block(1).is_some());
            assert_eq!(state.num_blocks_to_delete(), 0);
        }

        FileUtils::delete_path(path, true)?;
        store.read()?.increment_blocks_to_delete();
        assert!(store.async_remove_block(1)?.is_some());
        assert!(store.read()?.get_block(1).is_none());
        Ok(())
    }

    #[test]
    fn remove_deallocate_error_keeps_block_for_retry() -> CommonResult<()> {
        let store = create_store("remove-deallocate-error")?;
        finalized_block(&store)?;
        let path = replace_block_file_with_non_empty_dir(&store)?;

        let result = store.remove_block(1);
        assert!(result.is_err());
        assert!(store.read()?.get_block(1).is_some());

        FileUtils::delete_path(path, true)?;
        store.remove_block(1)?;
        assert!(store.read()?.get_block(1).is_none());
        Ok(())
    }

    fn finalize_block(store: &BlockStore, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        let writing = store.open_block(block)?;
        let path = store
            .short_circuit(&writing)?
            .expect("file layout must expose a local path");
        write_block(&path, block.len as usize)?;
        store.finalize_block(block)
    }

    fn read_bytes(reader: &mut BlockReadContext, len: i32) -> CommonResult<Vec<u8>> {
        let slice = reader.read_region(false, len)?;
        match slice {
            DataSlice::Bytes(bytes) => Ok(bytes.to_vec()),
            DataSlice::Buffer(bytes) => Ok(bytes.to_vec()),
            other => curvine_core_error::err_box!("unexpected test read slice: {:?}", other),
        }
    }

    #[test]
    fn failed_file_rewrite_restores_committed_block() -> CommonResult<()> {
        let store = create_store_with_capacity("rewrite-rollback", "16MB")?;
        let block = ExtendedBlock::with_mem(1, "1MB")?;
        let finalized = finalize_block(&store, &block)?;
        let available = store.read()?.available();
        let (_, dir) = store.read()?.layout_for(&finalized)?;
        let staging = BlockMeta::new(block.id, block.len, &dir);
        let staging_path = FileLayout::block_path(&dir, &staging)?;
        std::fs::create_dir(&staging_path)?;

        assert!(store.open_block(&block).is_err());
        let restored = store.get_block(block.id)?;
        assert!(restored.is_final());
        assert_eq!(restored.len(), finalized.len());
        assert_eq!(store.read()?.available(), available);

        FileUtils::delete_path(staging_path, true)?;
        Ok(())
    }

    #[test]
    fn file_rewrite_requires_capacity_for_committed_copy() -> CommonResult<()> {
        let store = create_store_with_capacity("rewrite-capacity", "1MB")?;
        let block = ExtendedBlock::with_mem(1, "1MB")?;
        let finalized = finalize_block(&store, &block)?;
        let available = store.read()?.available();

        assert!(store.open_block(&block).is_err());
        assert_eq!(store.get_block(block.id)?.state(), &BlockState::Finalized);
        assert_eq!(store.get_block(block.id)?.len(), finalized.len());
        assert_eq!(store.read()?.available(), available);
        Ok(())
    }

    #[test]
    fn failed_file_finalize_restores_writing_state() -> CommonResult<()> {
        let store = create_store_with_capacity("finalize-rollback", "16MB")?;
        let block = ExtendedBlock::with_mem(1, "1MB")?;
        let writing = store.open_block(&block)?;
        let staging_path = store
            .short_circuit(&writing)?
            .expect("file layout must expose a local path");
        write_block(&staging_path, block.len as usize)?;

        let (_, dir) = store.read()?.layout_for(&writing)?;
        let mut final_meta = writing.clone();
        final_meta.state = BlockState::Finalized;
        let active_path = FileLayout::block_path(&dir, &final_meta)?;
        std::fs::create_dir(&active_path)?;

        assert!(store.finalize_block(&block).is_err());
        assert_eq!(store.get_block(block.id)?.state(), &BlockState::Writing);

        FileUtils::delete_path(&active_path, true)?;
        assert!(store.finalize_block(&block)?.is_final());
        Ok(())
    }

    #[test]
    fn failed_rewrite_finalize_with_shorter_length_restores_committed_block() -> CommonResult<()> {
        let store = create_store_with_capacity("rewrite-shrink-abort", "16MB")?;
        let mut block = ExtendedBlock::with_mem(1, "50B")?;
        let finalized = finalize_block(&store, &block)?;
        assert!(finalized.is_final());
        let available = store.read()?.available();

        let rewriting = store.open_block(&block)?;
        assert_eq!(rewriting.state(), &BlockState::Writing);

        block.len = 20;
        let error = store.finalize_block(&block).unwrap_err();
        assert!(
            error.to_string().contains("length mismatch"),
            "expected a length mismatch prepare failure, got: {error}"
        );

        let restored = store.get_block(block.id)?;
        assert!(restored.is_final());
        assert_eq!(restored.len(), 50);
        let active_path = store
            .short_circuit(&restored)?
            .expect("file layout must expose a local path");
        assert_eq!(std::fs::metadata(&active_path)?.len(), 50);

        let (_, dir) = store.read()?.layout_for(&restored)?;
        let staging = BlockMeta::new(block.id, block.len, &dir);
        let staging_path = FileLayout::block_path(&dir, &staging)?;
        assert!(!staging_path.exists());
        assert_eq!(store.read()?.available(), available);
        Ok(())
    }

    #[test]
    fn failed_first_write_finalize_with_length_mismatch_aborts_block() -> CommonResult<()> {
        let store = create_store_with_capacity("first-write-abort", "16MB")?;
        let available = store.read()?.available();

        let block = ExtendedBlock::with_mem(1, "50B")?;
        let writing = store.open_block(&block)?;
        assert_eq!(writing.state(), &BlockState::Writing);
        let staging_path = store
            .short_circuit(&writing)?
            .expect("file layout must expose a local path");
        write_block(&staging_path, 30)?;

        let error = store.finalize_block(&block).unwrap_err();
        assert!(
            error.to_string().contains("length mismatch"),
            "expected a length mismatch prepare failure, got: {error}"
        );

        assert!(store.get_block(block.id).is_err());
        assert!(!std::path::Path::new(&staging_path).exists());
        assert_eq!(store.read()?.available(), available);
        Ok(())
    }

    #[test]
    fn finalizing_new_block_remains_reportable_and_readable() -> CommonResult<()> {
        let store = create_store_with_capacity("finalizing-report", "16MB")?;
        let block = ExtendedBlock::with_mem(1, "1MB")?;
        store.open_block(&block)?;

        let reservation = store
            .write()?
            .reserve_file_finalize(&block)?
            .expect("file block finalize must reserve");
        let report = store.all_blocks()?;
        assert_eq!(report.len(), 1);
        assert_eq!(report[0].state(), &BlockState::Finalizing);
        assert_eq!(store.get_block(block.id)?.state(), &BlockState::Finalizing);
        assert!(store.short_circuit_by_id(block.id)?.is_none());

        store.write()?.rollback_file_finalize(&reservation)?;
        Ok(())
    }

    #[test]
    fn short_circuit_reports_live_file_length() -> CommonResult<()> {
        let store = create_store_with_capacity("short-circuit-live-length", "16MB")?;
        let block = ExtendedBlock::with_mem(1, "50B")?;
        let finalized = finalize_block(&store, &block)?;
        let path = store
            .short_circuit(&finalized)?
            .expect("file layout must expose a local path");
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)?
            .set_len(20)?;

        let (meta, _, physical_len) = store
            .short_circuit_by_id(block.id)?
            .expect("finalized file must support short-circuit reads");
        assert_eq!(meta.len(), 50);
        assert_eq!(physical_len, 20);
        Ok(())
    }

    #[test]
    fn reader_opened_before_publish_keeps_committed_generation() -> CommonResult<()> {
        let store = create_store_with_capacity("finalize-reader-generation", "16MB")?;
        let block = ExtendedBlock::with_mem(1, "4B")?;
        let first = store.open_block(&block)?;
        let first_path = store
            .short_circuit(&first)?
            .expect("file layout must expose a local path");
        std::fs::write(first_path, b"old!")?;
        store.finalize_block(&block)?;

        let rewrite = store.open_block(&block)?;
        let rewrite_path = store
            .short_circuit(&rewrite)?
            .expect("file layout must expose a local path");
        std::fs::write(rewrite_path, b"new!")?;
        let reservation = store
            .write()?
            .reserve_file_finalize(&block)?
            .expect("rewrite finalize must reserve");
        let plan = reservation.prepare(block.len)?;

        let (_, mut committed_reader) = store.open_reader_by_id_at_stored_len(block.id, 0)?;
        store.write()?.publish_file_finalize(&reservation, plan)?;
        assert_eq!(read_bytes(&mut committed_reader, 4)?, b"old!");

        let (_, mut published_reader) = store.open_reader_by_id_at_stored_len(block.id, 0)?;
        assert_eq!(read_bytes(&mut published_reader, 4)?, b"new!");
        Ok(())
    }

    fn create_checksum_store(name: &str) -> CommonResult<BlockStore> {
        create_checksum_store_with(name, true, false)
    }

    fn create_checksum_store_with(
        name: &str,
        write: bool,
        verify: bool,
    ) -> CommonResult<BlockStore> {
        create_checksum_store_with_policy(name, write, verify, true)
    }

    fn create_checksum_store_with_policy(
        name: &str,
        write: bool,
        verify: bool,
        allow_missing: bool,
    ) -> CommonResult<BlockStore> {
        open_checksum_store(name, write, verify, allow_missing, true)
    }

    /// Open the store in `name`; `format` false keeps the blocks on disk, as
    /// a worker restart does.
    fn open_checksum_store(
        name: &str,
        write: bool,
        verify: bool,
        allow_missing: bool,
        format: bool,
    ) -> CommonResult<BlockStore> {
        let conf = ClusterConf {
            format_worker: format,
            worker: WorkerConf {
                dir_reserved: "0".to_string(),
                data_dir: vec![format!("[MEM:16MB]../testing/block-store-{name}")],
                block_checksum: write,
                block_checksum_verify: verify,
                block_checksum_allow_missing: allow_missing,
                ..WorkerConf::default()
            },
            ..ClusterConf::default()
        };
        BlockStore::new("test", &conf)
    }

    /// Write `data` through a worker writer and finalize it.
    fn finalize_with_data(store: &BlockStore, id: i64, data: &[u8]) -> CommonResult<BlockMeta> {
        let mut block = ExtendedBlock::with_mem(id, "1MB")?;
        let meta = store.open_block(&block)?;
        let mut writer = store.open_writer(&meta, 0)?;
        writer.write_region(&DataSlice::Buffer(bytes::BytesMut::from(data)))?;
        writer.flush()?;
        drop(writer);
        block.len = data.len() as i64;
        store.finalize_block(&block)
    }

    #[test]
    fn verified_reads_detect_corruption_and_disable_short_circuit() -> CommonResult<()> {
        let store = create_checksum_store_with("checksum-verify-read", true, true)?;
        // Three 64 KiB chunks with a partial tail.
        let len = 150 * 1024 + 17;
        let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let finalized = finalize_with_data(&store, 1, &data)?;
        assert!(store.short_circuit_by_id(finalized.id)?.is_none());

        let (_, mut reader) = store.open_reader_by_id(finalized.id, 100 * 1024, len as i64)?;
        assert!(!reader.supports_send_file());
        assert_eq!(
            read_bytes(&mut reader, 1024)?,
            &data[100 * 1024..101 * 1024]
        );

        // Flip one byte in chunk 1 [64 KiB, 128 KiB).
        let path = store.short_circuit(&finalized)?.unwrap();
        let mut corrupted = data.clone();
        corrupted[90 * 1024] ^= 0xff;
        std::fs::write(&path, &corrupted)?;

        let (_, mut reader) = store.open_reader_by_id(finalized.id, 0, len as i64)?;
        assert_eq!(read_bytes(&mut reader, 64 * 1024)?, &data[..64 * 1024]);
        let err = reader.read_region(false, 1).unwrap_err().to_string();
        assert!(err.contains("checksum mismatch at chunk 1"), "{err}");

        // Replication reads the source at its stored length with send_file.
        let (_, mut reader) = store.open_reader_by_id_at_stored_len(finalized.id, 64 * 1024)?;
        assert!(reader.read_region(true, 64 * 1024).is_err());
        Ok(())
    }

    /// Finalize a 3-chunk block with a checksum and return it with its path.
    fn finalize_checksummed(store: &BlockStore, id: i64) -> CommonResult<(BlockMeta, Vec<u8>)> {
        let len = 150 * 1024 + 17;
        let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let meta = finalize_with_data(store, id, &data)?;
        Ok((meta, data))
    }

    #[test]
    fn invalidate_removes_block_that_fails_again() -> CommonResult<()> {
        let store = create_checksum_store_with("checksum-invalidate", true, true)?;
        let (finalized, data) = finalize_checksummed(&store, 1)?;
        let path = store.short_circuit(&finalized)?.unwrap();
        let sidecar = FileLayout::checksum_path_of(std::path::Path::new(&path));
        assert!(sidecar.exists());

        let mut corrupted = data.clone();
        corrupted[90 * 1024] ^= 0xff;
        std::fs::write(&path, &corrupted)?;

        // The read path only queues the block, once per id.
        let (_, mut reader) =
            store.open_reader_by_id(finalized.id, 64 * 1024, data.len() as i64)?;
        assert!(reader.read_region(false, 1024).is_err());
        assert!(reader.checksum_mismatch());
        store.report_corrupt(finalized.id, reader.path());
        store.report_corrupt(finalized.id, reader.path());
        assert_eq!(store.take_corrupt_blocks(), vec![(finalized.id, 0)]);
        assert!(store.take_corrupt_blocks().is_empty());
        assert!(store.get_block(finalized.id).is_ok());

        match store.invalidate_corrupt_block(finalized.id)? {
            CorruptCheck::Removed(meta) => assert_eq!(meta.len, data.len() as i64),
            other => panic!("expected removal, got {other:?}"),
        }
        assert!(store.get_block(finalized.id).is_err());
        assert!(!std::path::Path::new(&path).exists());
        assert!(!sidecar.exists());

        // A second report for a block already removed is a no-op.
        assert!(matches!(
            store.invalidate_corrupt_block(finalized.id)?,
            CorruptCheck::Kept(_)
        ));

        // Until master acknowledges the deletion, the id cannot be recreated.
        let block = ExtendedBlock::with_mem(finalized.id, "1MB")?;
        let err = store.open_block(&block).unwrap_err();
        assert!(err.to_string().contains("removed as corrupt"), "{err}");
        store.unfence(finalized.id);
        store.open_block(&block)?;
        Ok(())
    }

    #[test]
    fn requeue_keeps_the_lower_failure_count() -> CommonResult<()> {
        let store = create_checksum_store_with("checksum-requeue", true, true)?;
        store.requeue_corrupt(5, 2);
        assert_eq!(store.take_corrupt_blocks(), vec![(5, 2)]);

        store.report_corrupt(5, "fresh read");
        store.requeue_corrupt(5, 2);
        assert_eq!(store.take_corrupt_blocks(), vec![(5, 0)]);
        Ok(())
    }

    #[test]
    fn invalidate_keeps_block_when_second_check_passes() -> CommonResult<()> {
        let store = create_checksum_store_with("checksum-invalidate-pass", true, true)?;
        let (finalized, data) = finalize_checksummed(&store, 1)?;
        store.report_corrupt(finalized.id, "transient");

        let reason = match store.invalidate_corrupt_block(finalized.id)? {
            CorruptCheck::Kept(reason) => reason,
            other => panic!("expected the block to be kept, got {other:?}"),
        };
        assert!(reason.contains("no mismatch"), "{reason}");
        let (_, mut reader) = store.open_reader_by_id(finalized.id, 0, data.len() as i64)?;
        assert_eq!(read_bytes(&mut reader, data.len() as i32)?, &data[..]);
        Ok(())
    }

    #[test]
    fn invalidate_keeps_blocks_it_cannot_confirm() -> CommonResult<()> {
        let store = create_checksum_store_with("checksum-invalidate-skip", true, true)?;
        let (finalized, data) = finalize_checksummed(&store, 1)?;
        let path = store.short_circuit(&finalized)?.unwrap();
        let mut corrupted = data.clone();
        corrupted[0] ^= 0xff;
        std::fs::write(&path, &corrupted)?;

        // An active writer may be replacing the corrupt generation.
        let lease = store.ordinary_write_lease(finalized.id)?;
        assert!(matches!(
            store.invalidate_corrupt_block(finalized.id)?,
            CorruptCheck::Retry(_)
        ));
        drop(lease);
        assert!(store.get_block(finalized.id).is_ok());

        // A block that is still being written has no sidecar to check.
        let writing = store.open_block(&ExtendedBlock::with_mem(2, "1MB")?)?;
        assert!(matches!(
            store.invalidate_corrupt_block(writing.id)?,
            CorruptCheck::Retry(_)
        ));

        // Without a sidecar the corruption cannot be confirmed.
        let plain = create_checksum_store_with("checksum-invalidate-nosidecar", false, true)?;
        let (unsummed, data) = finalize_checksummed(&plain, 1)?;
        let path = plain.short_circuit(&unsummed)?.unwrap();
        let mut corrupted = data.clone();
        corrupted[0] ^= 0xff;
        std::fs::write(&path, &corrupted)?;
        let reason = match plain.invalidate_corrupt_block(unsummed.id)? {
            CorruptCheck::Kept(reason) => reason,
            other => panic!("expected the block to be kept, got {other:?}"),
        };
        assert!(reason.contains("no checksum sidecar"), "{reason}");
        assert!(plain.get_block(unsummed.id).is_ok());
        Ok(())
    }

    /// Cut the file of a finalized block to `len` bytes behind the worker.
    fn truncate_block_file(store: &BlockStore, meta: &BlockMeta, len: u64) -> CommonResult<()> {
        let path = store.short_circuit(meta)?.unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)?
            .set_len(len)?;
        Ok(())
    }

    #[test]
    fn truncated_block_is_queued_and_removed() -> CommonResult<()> {
        // No sidecar: truncation is confirmed by the file length alone.
        let store = create_checksum_store_with("truncated-invalidate", false, false)?;
        let data = vec![5u8; 4096];
        let finalized = finalize_with_data(&store, 1, &data)?;
        let path = store.short_circuit(&finalized)?.unwrap();
        truncate_block_file(&store, &finalized, 1000)?;

        // Client reads and replication reads queue the block, once per id.
        let err = store
            .open_reader_by_id(finalized.id, 0, 4096)
            .err()
            .expect("a truncated block must not open");
        assert!(err.to_string().contains("is truncated"), "{err}");
        assert!(store
            .open_reader_by_id_at_stored_len(finalized.id, 0)
            .is_err());
        assert_eq!(store.take_corrupt_blocks(), vec![(finalized.id, 0)]);
        assert!(store.get_block(finalized.id).is_ok());

        match store.invalidate_corrupt_block(finalized.id)? {
            CorruptCheck::Removed(meta) => assert_eq!(meta.len, data.len() as i64),
            other => panic!("expected removal, got {other:?}"),
        }
        assert!(store.get_block(finalized.id).is_err());
        assert!(!std::path::Path::new(&path).exists());

        // Until master acknowledges the deletion, the id cannot be recreated.
        let block = ExtendedBlock::with_mem(finalized.id, "1MB")?;
        let err = store.open_block(&block).unwrap_err();
        assert!(err.to_string().contains("removed as corrupt"), "{err}");
        store.unfence(finalized.id);
        store.open_block(&block)?;
        Ok(())
    }

    #[test]
    fn truncated_checksummed_block_is_removed_with_its_sidecar() -> CommonResult<()> {
        let store = create_checksum_store_with("truncated-invalidate-sidecar", true, true)?;
        let (finalized, _) = finalize_checksummed(&store, 1)?;
        let path = store.short_circuit(&finalized)?.unwrap();
        let sidecar = FileLayout::checksum_path_of(std::path::Path::new(&path));
        assert!(sidecar.exists());
        truncate_block_file(&store, &finalized, 1000)?;

        assert!(store
            .open_reader_by_id(finalized.id, 0, finalized.len)
            .is_err());
        assert_eq!(store.take_corrupt_blocks(), vec![(finalized.id, 0)]);
        assert!(matches!(
            store.invalidate_corrupt_block(finalized.id)?,
            CorruptCheck::Removed(_)
        ));
        assert!(!std::path::Path::new(&path).exists());
        assert!(!sidecar.exists());
        Ok(())
    }

    #[test]
    fn only_truncated_finalized_blocks_are_queued() -> CommonResult<()> {
        let store = create_checksum_store_with("truncated-queue-only", false, false)?;
        let finalized = finalize_with_data(&store, 1, &[5u8; 4096])?;

        // An open that fails for another reason does not queue the block.
        assert!(store.open_reader_by_id(finalized.id, 4097, 4096).is_err());
        assert!(store.take_corrupt_blocks().is_empty());

        // A block being written holds fewer bytes than its reservation.
        let writing = store.open_block(&ExtendedBlock::with_mem(2, "1MB")?)?;
        let mut writer = store.open_writer(&writing, 0)?;
        writer.write_region(&DataSlice::Buffer(bytes::BytesMut::from(&[6u8; 100][..])))?;
        writer.flush()?;
        drop(writer);
        let (_, mut reader) = store.open_reader_by_id(writing.id, 0, 100)?;
        assert_eq!(read_bytes(&mut reader, 100)?, vec![6u8; 100]);
        assert!(store.take_corrupt_blocks().is_empty());
        Ok(())
    }

    #[test]
    fn truncated_block_restored_before_recheck_is_kept() -> CommonResult<()> {
        let store = create_checksum_store_with("truncated-restored", false, false)?;
        let data = vec![5u8; 4096];
        let finalized = finalize_with_data(&store, 1, &data)?;
        let path = store.short_circuit(&finalized)?.unwrap();
        truncate_block_file(&store, &finalized, 1000)?;
        assert!(store.open_reader_by_id(finalized.id, 0, 4096).is_err());
        assert_eq!(store.take_corrupt_blocks(), vec![(finalized.id, 0)]);

        // The re-check, not the failed read, decides: a file that holds its
        // committed length again is kept.
        std::fs::write(&path, &data)?;
        assert!(matches!(
            store.invalidate_corrupt_block(finalized.id)?,
            CorruptCheck::Kept(_)
        ));
        let (_, mut reader) = store.open_reader_by_id(finalized.id, 0, 4096)?;
        assert_eq!(read_bytes(&mut reader, 4096)?, data);
        Ok(())
    }

    #[test]
    fn unfinalized_blocks_are_read_without_verification() -> CommonResult<()> {
        // RequireSidecar rejects finalized blocks without a sidecar, but a
        // block that is still being written cannot have one yet.
        let store =
            create_checksum_store_with_policy("checksum-verify-staging", true, true, false)?;
        let block = ExtendedBlock::with_mem(1, "1MB")?;
        let meta = store.open_block(&block)?;
        let mut writer = store.open_writer(&meta, 0)?;
        writer.write_region(&DataSlice::Buffer(bytes::BytesMut::from(&[3u8; 4096][..])))?;
        writer.flush()?;
        drop(writer);

        let (read_meta, mut reader) = store.open_reader_by_id(block.id, 0, 4096)?;
        assert_eq!(read_meta.state(), &BlockState::Writing);
        assert!(!reader.is_verified());
        assert_eq!(read_bytes(&mut reader, 4096)?, vec![3u8; 4096]);

        // Finalize writes the staging sidecar outside the dataset lock; an
        // empty file is what a reader can see right after `File::create`.
        let staging_path = store.short_circuit(&meta)?.unwrap();
        let staging_checksum = FileLayout::checksum_path_of(std::path::Path::new(&staging_path));
        let reservation = store
            .write()?
            .reserve_file_finalize(&block)?
            .expect("file block finalize must reserve");
        std::fs::write(&staging_checksum, b"")?;
        let (read_meta, mut reader) = store.open_reader_by_id(block.id, 0, 4096)?;
        assert_eq!(read_meta.state(), &BlockState::Finalizing);
        assert!(!reader.is_verified());
        assert_eq!(read_bytes(&mut reader, 4096)?, vec![3u8; 4096]);
        store.write()?.rollback_file_finalize(&reservation)?;
        let _ = std::fs::remove_file(&staging_checksum);
        Ok(())
    }

    #[test]
    fn verify_off_keeps_short_circuit_and_send_file() -> CommonResult<()> {
        let store = create_checksum_store_with("checksum-verify-off", true, false)?;
        let finalized = finalize_with_data(&store, 1, &[7u8; 4096])?;
        assert!(store.short_circuit_by_id(finalized.id)?.is_some());
        let (_, reader) = store.open_reader_by_id(finalized.id, 0, 4096)?;
        assert!(!reader.is_verified());
        Ok(())
    }

    #[test]
    fn verify_without_write_reads_blocks_without_sidecar() -> CommonResult<()> {
        // Default policy allows blocks that have no sidecar.
        let store = create_checksum_store_with("checksum-verify-only", false, true)?;
        let finalized = finalize_with_data(&store, 1, &[5u8; 4096])?;
        let (_, mut reader) = store.open_reader_by_id(finalized.id, 0, 4096)?;
        assert!(!reader.is_verified());
        assert_eq!(read_bytes(&mut reader, 4096)?, vec![5u8; 4096]);
        Ok(())
    }

    fn assert_sidecar_matches(path: &str) -> CommonResult<()> {
        use crate::worker::storage::{BlockChecksum, DEFAULT_CHECKSUM_CHUNK_SIZE};
        let data = std::fs::read(path)?;
        let sidecar = std::fs::read(FileLayout::checksum_path_of(std::path::Path::new(path)))?;
        let expected = BlockChecksum::compute(DEFAULT_CHECKSUM_CHUNK_SIZE, &data)?;
        assert_eq!(BlockChecksum::decode(&sidecar)?, expected);
        Ok(())
    }

    #[test]
    fn finalize_checksums_worker_and_short_circuit_writes() -> CommonResult<()> {
        let store = create_checksum_store("checksum-write-paths")?;
        // More than two 64 KiB chunks with a partial tail.
        let len = 150 * 1024 + 17;

        // Worker write path with a seek back over already written bytes.
        let mut block = ExtendedBlock::with_mem(1, "1MB")?;
        let meta = store.open_block(&block)?;
        let mut writer = store.open_writer(&meta, 0)?;
        writer.write_region(&DataSlice::Buffer(bytes::BytesMut::from(
            &vec![1u8; len][..],
        )))?;
        writer.seek_to(70 * 1024)?;
        writer.write_region(&DataSlice::Buffer(bytes::BytesMut::from(&[9u8; 100][..])))?;
        writer.flush()?;
        drop(writer);
        block.len = len as i64;
        let finalized = store.finalize_block(&block)?;
        let path = store.short_circuit(&finalized)?.unwrap();
        assert_eq!(std::fs::read(&path)?[70 * 1024], 9);
        assert_sidecar_matches(&path)?;

        // Short-circuit path: the client writes the staging file directly.
        let mut block = ExtendedBlock::with_mem(2, "1MB")?;
        let meta = store.open_block(&block)?;
        let staging = store.short_circuit(&meta)?.unwrap();
        let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        std::fs::write(&staging, &data)?;
        block.len = len as i64;
        let finalized = store.finalize_block(&block)?;
        let path = store.short_circuit(&finalized)?.unwrap();
        assert_sidecar_matches(&path)?;

        // Removing the block removes its sidecar.
        store.remove_block(2)?;
        assert!(!FileLayout::checksum_path_of(std::path::Path::new(&path)).exists());
        Ok(())
    }

    #[test]
    fn concurrent_writers_finalize_without_checksum() -> CommonResult<()> {
        let store = create_checksum_store("checksum-concurrent-writers")?;
        let mut block = ExtendedBlock::with_mem(1, "1MB")?;
        let first = store.ordinary_write_lease(block.id)?;
        let second = store.ordinary_write_lease(block.id)?;
        let meta = store.open_block(&block)?;
        let staging = store.short_circuit(&meta)?.unwrap();
        std::fs::write(&staging, [1u8; 100])?;

        // The second session still holds the staging inode open and may write
        // after the checksum pass, so the first finalize must not publish one.
        let mut late = std::fs::OpenOptions::new().write(true).open(&staging)?;
        block.len = 100;
        let finalized = store.finalize_block(&block)?;
        let path = store.short_circuit(&finalized)?.unwrap();
        let sidecar = FileLayout::checksum_path_of(std::path::Path::new(&path));
        assert!(!sidecar.exists());
        drop(first);

        late.write_all(&[2u8; 100])?;
        drop(late);
        assert_eq!(store.finalize_block(&block)?.len(), 100);
        assert!(!sidecar.exists());
        drop(second);

        // A single remaining writer gets a sidecar again on rewrite.
        let _only = store.ordinary_write_lease(block.id)?;
        let rewrite = store.open_block(&block)?;
        std::fs::write(store.short_circuit(&rewrite)?.unwrap(), [3u8; 100])?;
        store.finalize_block(&block)?;
        assert_sidecar_matches(&path)?;
        Ok(())
    }

    #[test]
    fn length_mismatch_abort_leaves_no_checksum_sidecar() -> CommonResult<()> {
        let store = create_checksum_store("checksum-length-mismatch")?;
        let mut block = ExtendedBlock::with_mem(1, "1MB")?;
        let meta = store.open_block(&block)?;
        let staging = store.short_circuit(&meta)?.unwrap();
        std::fs::write(&staging, [4u8; 100])?;

        block.len = 101;
        assert!(store.finalize_block(&block).is_err());
        assert!(store.get_block(1).is_err());
        assert!(!std::path::Path::new(&staging).exists());
        assert!(!FileLayout::checksum_path_of(std::path::Path::new(&staging)).exists());
        Ok(())
    }

    #[test]
    fn stored_len_reader_reads_finalized_block() -> CommonResult<()> {
        let store = create_store_with_capacity("stored-len-reader", "16MB")?;
        let block = ExtendedBlock::with_mem(1, "4B")?;
        finalize_block(&store, &block)?;

        let (meta, mut reader) = store.open_reader_by_id_at_stored_len(block.id, 0)?;
        assert_eq!(meta.len(), block.len);
        assert_eq!(read_bytes(&mut reader, block.len as i32)?, vec![0; 4]);
        Ok(())
    }

    #[test]
    fn concurrent_open_of_same_block_keeps_single_writing_meta() -> CommonResult<()> {
        const WRITERS: usize = 4;

        let store = create_store_with_capacity("same-block-open", "16MB")?;
        let block = ExtendedBlock::with_mem(1, "1MB")?;
        let barrier = Arc::new(Barrier::new(WRITERS + 1));
        let mut workers = Vec::with_capacity(WRITERS);
        for _ in 0..WRITERS {
            let store = store.clone();
            let block = block.clone();
            let barrier = barrier.clone();
            workers.push(thread::spawn(move || {
                barrier.wait();
                store.open_block(&block)
            }));
        }

        barrier.wait();
        for worker in workers {
            let meta = worker.join().expect("open worker panicked")?;
            assert_eq!(meta.id(), block.id);
            assert_eq!(meta.state(), &BlockState::Writing);
        }

        assert_eq!(store.all_blocks()?.len(), 1);
        store.abort_block(&block)?;
        assert!(store.all_blocks()?.is_empty());
        Ok(())
    }

    #[test]
    #[ignore = "manual benchmark: run with --ignored --nocapture"]
    fn measure_concurrent_file_rewrites() -> CommonResult<()> {
        const BLOCKS: i64 = 4;
        const BLOCK_BYTES: usize = 32 * 1024 * 1024;

        let store = create_store_with_capacity("concurrent-rewrites", "1GB")?;
        let blocks: Vec<ExtendedBlock> = (0..BLOCKS)
            .map(|id| ExtendedBlock::with_mem(id, "32MB"))
            .collect::<CommonResult<_>>()?;

        for block in &blocks {
            let meta = store.open_block(block)?;
            let path = store
                .short_circuit(&meta)?
                .expect("file layout must expose a local path");
            write_block(&path, BLOCK_BYTES)?;
            store.finalize_block(block)?;
        }

        let barrier = Arc::new(Barrier::new(BLOCKS as usize + 1));
        let mut workers = Vec::with_capacity(BLOCKS as usize);
        for block in blocks.clone() {
            let store = store.clone();
            let barrier = barrier.clone();
            workers.push(thread::spawn(move || {
                barrier.wait();
                let started = Instant::now();
                let result = store.open_block(&block);
                let elapsed = started.elapsed();
                if result.is_ok() {
                    store.abort_block(&block)?;
                }
                Ok::<_, curvine_core_error::CommonError>(elapsed)
            }));
        }

        barrier.wait();
        let started = Instant::now();
        let elapsed: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().expect("rewrite worker panicked"))
            .collect::<CommonResult<_>>()?;
        let wall_clock = started.elapsed();
        let max_latency = elapsed
            .into_iter()
            .max()
            .expect("benchmark has at least one block");

        eprintln!(
            "CONCURRENT_FILE_REWRITE_BENCH blocks={BLOCKS} bytes_per_block={BLOCK_BYTES} wall_clock_us={} max_open_us={}",
            wall_clock.as_micros(),
            max_latency.as_micros(),
        );
        Ok(())
    }

    /// Overwrite the block file with zeros of the same length, the damage a
    /// reboot without sync can leave (#1813).
    fn zero_block(store: &BlockStore, meta: &BlockMeta) -> CommonResult<()> {
        let path = store.short_circuit(meta)?.unwrap();
        std::fs::write(&path, vec![0u8; meta.len() as usize])?;
        Ok(())
    }

    #[test]
    fn zeroed_block_after_restart_fails_verified_read() -> CommonResult<()> {
        let name = "checksum-restart-zeroed";
        let store = open_checksum_store(name, true, true, true, true)?;
        let (finalized, data) = finalize_checksummed(&store, 1)?;
        zero_block(&store, &finalized)?;
        drop(store);

        let store = open_checksum_store(name, true, true, true, false)?;
        assert!(store.get_block(finalized.id)?.is_final());
        let (_, mut reader) = store.open_reader_by_id(finalized.id, 0, data.len() as i64)?;
        assert!(reader.read_region(false, 64 * 1024).is_err());
        assert!(reader.checksum_mismatch());
        Ok(())
    }

    #[test]
    fn legacy_block_after_restart_follows_missing_sidecar_policy() -> CommonResult<()> {
        let name = "checksum-restart-legacy";
        // Written before checksums were enabled: no sidecar.
        let store = open_checksum_store(name, false, false, true, true)?;
        let data = vec![7u8; 100 * 1024];
        let legacy = finalize_with_data(&store, 1, &data)?;
        drop(store);

        let store = open_checksum_store(name, true, true, true, false)?;
        let (_, mut reader) = store.open_reader_by_id(legacy.id, 0, data.len() as i64)?;
        assert!(!reader.is_verified());
        assert_eq!(read_bytes(&mut reader, data.len() as i32)?, data);
        drop(store);

        let store = open_checksum_store(name, true, true, false, false)?;
        let Err(err) = store.open_reader_by_id(legacy.id, 0, data.len() as i64) else {
            panic!("a legacy block must be rejected when a sidecar is required");
        };
        assert!(err.to_string().contains("sidecar"), "{err}");
        Ok(())
    }

    /// Restart a store holding a healthy, a zeroed and a legacy block.
    fn restarted_store_for_scan(name: &str, allow_missing: bool) -> CommonResult<BlockStore> {
        let store = open_checksum_store(name, false, false, true, true)?;
        finalize_with_data(&store, 3, &[9u8; 1000])?;
        drop(store);

        let store = open_checksum_store(name, true, true, true, false)?;
        finalize_checksummed(&store, 1)?;
        let (zeroed, _) = finalize_checksummed(&store, 2)?;
        zero_block(&store, &zeroed)?;
        drop(store);

        open_checksum_store(name, true, true, allow_missing, false)
    }

    #[test]
    fn startup_scan_removes_zeroed_block_without_a_read() -> CommonResult<()> {
        let store = restarted_store_for_scan("checksum-scan-allow-missing", true)?;
        let stats = ChecksumScan::new(store.clone(), 0).run(|| false)?;
        assert_eq!(
            (stats.verified, stats.mismatched, stats.no_sidecar),
            (1, 1, 1),
            "{stats:?}"
        );
        assert_eq!(stats.unverifiable + stats.skipped + stats.failed, 0);
        // The mismatched read of the zeroed block counts too; the legacy
        // block has no sidecar and is not read.
        assert_eq!(stats.bytes, 2 * (150 * 1024 + 17), "{stats:?}");
        assert_eq!(store.take_corrupt_blocks(), vec![(2, 0)]);

        assert!(matches!(
            store.invalidate_corrupt_block(2)?,
            CorruptCheck::Removed(_)
        ));
        assert!(store.get_block(2).is_err());
        assert!(store.get_block(1)?.is_final());
        assert!(store.get_block(3)?.is_final());
        Ok(())
    }

    #[test]
    fn startup_scan_keeps_legacy_block_when_sidecar_is_required() -> CommonResult<()> {
        let store = restarted_store_for_scan("checksum-scan-require", false)?;
        let stats = ChecksumScan::new(store.clone(), 0).run(|| false)?;
        assert_eq!(
            (stats.verified, stats.mismatched, stats.unverifiable),
            (1, 1, 1),
            "{stats:?}"
        );
        // Only the CRC mismatch is queued: a missing sidecar does not prove
        // the data is wrong, and the block may be the only replica.
        assert_eq!(store.take_corrupt_blocks(), vec![(2, 0)]);
        assert!(matches!(
            store.invalidate_corrupt_block(2)?,
            CorruptCheck::Removed(_)
        ));
        assert!(store.get_block(1)?.is_final());
        assert!(store.get_block(3)?.is_final());
        Ok(())
    }

    #[test]
    fn startup_scan_keeps_concurrent_writer_block_when_sidecar_is_required() -> CommonResult<()> {
        let name = "checksum-scan-concurrent-writers";
        let store = open_checksum_store(name, true, true, true, true)?;
        let mut block = ExtendedBlock::with_mem(1, "1MB")?;
        let first = store.ordinary_write_lease(block.id)?;
        let second = store.ordinary_write_lease(block.id)?;
        let meta = store.open_block(&block)?;
        let data = vec![5u8; 100];
        std::fs::write(store.short_circuit(&meta)?.unwrap(), &data)?;
        block.len = data.len() as i64;
        let finalized = store.finalize_block(&block)?;
        let path = store.short_circuit(&finalized)?.unwrap();
        assert!(!FileLayout::checksum_path_of(std::path::Path::new(&path)).exists());
        drop((first, second));
        drop(store);

        // Concurrent writers skip the checksum on purpose; the data is good.
        let store = open_checksum_store(name, true, true, false, false)?;
        let stats = ChecksumScan::new(store.clone(), 0).run(|| false)?;
        assert_eq!(stats.unverifiable, 1, "{stats:?}");
        assert!(store.take_corrupt_blocks().is_empty());
        assert!(store.get_block(finalized.id)?.is_final());
        assert_eq!(std::fs::read(&path)?, data);
        Ok(())
    }

    #[test]
    fn startup_scan_stops_when_the_worker_stops() -> CommonResult<()> {
        let store = restarted_store_for_scan("checksum-scan-stop", true)?;
        let stats = ChecksumScan::new(store.clone(), 0).run(|| true)?;
        assert_eq!(stats, ScanStats::default());
        assert!(store.take_corrupt_blocks().is_empty());
        Ok(())
    }

    #[test]
    fn startup_scan_stops_during_a_throttled_read() -> CommonResult<()> {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::{Duration, Instant};

        let store = restarted_store_for_scan("checksum-scan-throttled-stop", true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let setter = stop.clone();
        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            setter.store(true, Ordering::Release);
        });
        // At 1 KiB/s the first 150 KiB block alone would wait about 150 s.
        let started = Instant::now();
        let stats = ChecksumScan::new(store.clone(), 1024).run(|| stop.load(Ordering::Acquire))?;
        handle.join().unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );
        // Block order is not fixed: the zeroed block may fail on its only
        // read before the stop is seen, but no healthy block can finish.
        assert_eq!(stats.verified, 0, "{stats:?}");
        assert!(stats.mismatched + stats.skipped >= 1, "{stats:?}");
        Ok(())
    }
}
