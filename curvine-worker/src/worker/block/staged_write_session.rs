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

use super::{BlockStore, BlockWriteLease, ExclusiveWriter, StagedVisibility};
use crate::worker::storage::{
    BlockChecksum, BlockLayoutKind, BlockMeta, Dataset, OwnedBlockWrite, StagedFileWriter,
};
use curvine_core_error::{err_box, CommonResult};
use curvine_io::{BlockIO, DataSlice, IOError, IOResult};
use curvine_model::{ExtendedBlock, StorageType};
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(test)]
static CHECKPOINT_COUNT: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static CHECKPOINT_NS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static FAST_COMPLETE_COUNT: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static FAST_COMPLETE_NS: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn staged_checkpoint_totals_for_benchmark() -> (u64, u64) {
    (
        CHECKPOINT_COUNT.load(Ordering::Relaxed),
        CHECKPOINT_NS.load(Ordering::Relaxed),
    )
}

#[cfg(test)]
pub(crate) fn staged_fast_complete_totals_for_benchmark() -> (u64, u64) {
    (
        FAST_COMPLETE_COUNT.load(Ordering::Relaxed),
        FAST_COMPLETE_NS.load(Ordering::Relaxed),
    )
}

/// One owner of a fresh file-backed block. Reads pin completed bytes while
/// recovery still uses the last checkpoint under the staging pathname.
/// A failed session can be cancelled but must never publish.
pub struct StagedWriteSession {
    store: BlockStore,
    block: ExtendedBlock,
    meta: BlockMeta,
    visible: StagedVisibility,
    writer: Option<StagedFileWriter>,
    lease: Option<BlockWriteLease>,
    extent: u64,
    checkpointed: bool,
    retained_on_failure: bool,
    pos: i64,
    path: String,
}

impl BlockStore {
    /// Try staged I/O for a buffered, remote file session. The caller must
    /// rule out short-circuit and splice. `None` means no staged write was
    /// started, so it is safe to use the existing path.
    pub fn try_open_staged_write(
        &self,
        block: &ExtendedBlock,
    ) -> CommonResult<Option<StagedWriteSession>> {
        if !self.file_io_backend.supports_staged_writes()
            || block.alloc_opts.is_some()
            || block.storage_type == StorageType::SpdkDisk
            || self.exclusive_owner.is_some()
        {
            return Ok(None);
        }
        let visible = StagedVisibility::default();
        let (store, lease, meta) = {
            let _lock = self.block_lock(block.id, "staged_write_lease");
            self.check_write_owner(block.id)?;
            self.check_open_fence(block.id)?;
            // A previous writer may still hold a short-circuit FD to an
            // existing staging inode. Start only with a fresh allocation.
            {
                let dataset = self.read()?;
                // Do not let a speculative backend selection consume the
                // chooser's turn and change placement on mixed workers.
                if dataset.get_block(block.id).is_some()
                    || dataset
                        .dir_iter()
                        .any(|dir| dir.storage_type() == StorageType::SpdkDisk)
                {
                    return Ok(None);
                }
            }
            let (store, lease) = self
                .claim_exclusive_writer(block.id, ExclusiveWriter::Staged(Arc::clone(&visible)))?;
            let Some(reservation) =
                store.with_dataset_write("open", |state| state.reserve_fresh_file_open(block))?
            else {
                return Ok(None);
            };
            let meta = store.finish_file_open(block, reservation)?;
            (store, lease, meta)
        };
        let writer = (|| {
            let (layout, dir) = store.read()?.layout_for(&meta)?;
            let BlockLayoutKind::File(layout) = layout else {
                return err_box!("block {} was not allocated on a file layout", block.id);
            };
            let path = crate::worker::storage::FileLayout::block_path(&dir, &meta)?;
            Ok((
                layout.open_staged_writer(&dir, &meta, &store.file_io_backend)?,
                path.to_string_lossy().into_owned(),
            ))
        })();
        let (writer, path) = match writer {
            Ok(writer) => writer,
            Err(error) => {
                if let Err(abort_error) = store.abort_block(block) {
                    log::warn!(
                        "failed to abort staging block {}: {}",
                        block.id,
                        abort_error
                    );
                }
                return Err(error);
            }
        };
        Ok(Some(StagedWriteSession {
            store,
            block: block.clone(),
            meta,
            visible,
            writer: Some(writer),
            lease: Some(lease),
            extent: 0,
            checkpointed: false,
            retained_on_failure: false,
            pos: 0,
            path,
        }))
    }
}

impl StagedWriteSession {
    pub fn meta(&self) -> &BlockMeta {
        &self.meta
    }

    /// Acknowledge only after the completed range has a readable view.
    pub fn write_all_at(&mut self, offset: u64, payload: &[u8]) -> CommonResult<()> {
        let Some(end) = offset.checked_add(payload.len() as u64) else {
            return err_box!("staged block {} write range overflow", self.block.id);
        };
        if end > self.meta.len() as u64 {
            return err_box!(
                "staged block {} write exceeds its reserved size",
                self.block.id
            );
        }
        let Some(writer) = self.writer.as_mut() else {
            return err_box!("staged block {} has no active writer", self.block.id);
        };
        let result = (|| {
            writer.write_all_at(offset, payload)?;
            if !payload.is_empty() {
                let view = writer.read_view()?;
                *self.visible.lock() = Some(view);
                self.extent = self.extent.max(end);
            }
            Ok(())
        })();
        if result.is_err() {
            self.abandon();
        }
        result
    }

    /// Make the current bytes durable; new readers can then open the staging
    /// file directly. Existing readers keep the file descriptor they pinned.
    pub fn checkpoint(&mut self) -> CommonResult<u64> {
        let Some(writer) = self.writer.as_mut() else {
            return err_box!("staged block {} has no active writer", self.block.id);
        };
        if self.checkpointed && self.visible.lock().is_none() {
            return Ok(self.extent);
        }
        #[cfg(test)]
        let start = std::time::Instant::now();
        let result = writer.checkpoint();
        #[cfg(test)]
        if result.is_ok() {
            CHECKPOINT_NS.fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            CHECKPOINT_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        match result {
            Ok(len) => {
                self.checkpointed = true;
                self.visible.lock().take();
                Ok(len)
            }
            Err(error) => {
                self.abandon();
                Err(error.into())
            }
        }
    }

    pub fn complete(&mut self, len: i64) -> CommonResult<BlockMeta> {
        self.complete_with_checksum(len, None)
    }

    fn complete_with_checksum(
        &mut self,
        len: i64,
        checksum: Option<BlockChecksum>,
    ) -> CommonResult<BlockMeta> {
        if u64::try_from(len) != Ok(self.extent) {
            return err_box!(
                "staged block {} length mismatch: expected {}, actual {}",
                self.block.id,
                len,
                self.extent
            );
        }
        let mut block = self.block.clone();
        block.len = len;
        let finalized = if !self.checkpointed && self.extent != 0 {
            let Some(writer) = &mut self.writer else {
                return err_box!("staged block {} has no active writer", self.block.id);
            };
            // Match the existing buffered Complete contract. No durable
            // checkpoint can be overwritten: the private inode only becomes
            // recoverable when the generation is published as finalized.
            #[cfg(test)]
            let start = std::time::Instant::now();
            let meta = self
                .store
                .finalize_buffered_staged_block(&block, checksum, writer)?;
            #[cfg(test)]
            {
                FAST_COMPLETE_NS.fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                FAST_COMPLETE_COUNT.fetch_add(1, Ordering::Relaxed);
            }
            meta
        } else {
            if self.checkpoint()? != self.extent {
                self.abandon();
                return err_box!("staged block {} checkpoint length changed", self.block.id);
            }
            self.store.finalize_block_with_checksum(&block, checksum)?
        };
        self.discard_writer();
        self.lease.take();
        Ok(finalized)
    }

    /// Cancel is explicit permission to discard the checkpoint. A disconnect
    /// instead leaves any acknowledged checkpoint intact for recovery.
    pub fn cancel(&mut self) -> CommonResult<()> {
        if self.retained_on_failure {
            return err_box!(
                "staged block {} is no longer owned; its checkpoint was retained",
                self.block.id
            );
        }
        self.discard_writer();
        if self.lease.is_some() {
            self.store.abort_block(&self.block)?;
            self.lease.take();
        }
        Ok(())
    }

    fn discard_writer(&mut self) {
        self.visible.lock().take();
        self.writer.take();
    }

    fn abandon(&mut self) {
        self.discard_writer();
        if self.lease.is_some() {
            self.retained_on_failure = self.checkpointed;
            if !self.checkpointed {
                if let Err(error) = self.store.abort_block(&self.block) {
                    self.retained_on_failure = true;
                    log::warn!(
                        "failed to reclaim staged block {}: {}",
                        self.block.id,
                        error
                    );
                }
            }
            if self.retained_on_failure {
                self.store.staged_handoffs.insert(self.block.id);
            }
            // The failed handle is closed even if its TCP connection stays
            // alive. Stale terminal frames cannot mutate a successor.
            self.lease.take();
        }
    }
}

impl BlockIO for StagedWriteSession {
    fn read_region(&mut self, _enable_send_file: bool, _len: i32) -> IOResult<DataSlice> {
        Err(IOError::from("staged block reads must use the block store"))
    }

    fn read_all(&mut self, _buf: &mut [u8]) -> IOResult<()> {
        Err(IOError::from("staged block reads must use the block store"))
    }

    fn write_region(&mut self, region: &DataSlice) -> IOResult<()> {
        if matches!(region, DataSlice::IOSlice(_)) {
            return Err(IOError::from("splice is not a buffered staged write"));
        }
        self.write_all(region.as_slice())
    }

    fn write_all(&mut self, buf: &[u8]) -> IOResult<()> {
        self.write_all_at(self.pos as u64, buf)?;
        self.pos += buf.len() as i64;
        Ok(())
    }

    fn flush(&mut self) -> IOResult<()> {
        if self.writer.is_none() {
            return Err(IOError::from("staged block has no active writer"));
        }
        Ok(())
    }

    fn sync_data(&mut self) -> IOResult<()> {
        self.checkpoint()?;
        Ok(())
    }

    fn seek(&mut self, pos: i64) -> IOResult<i64> {
        if !(0..=self.meta.len()).contains(&pos) {
            return Err(IOError::from("invalid staged write offset"));
        }
        self.pos = pos;
        Ok(pos)
    }

    fn pos(&self) -> i64 {
        self.pos
    }

    fn len(&self) -> i64 {
        self.extent as i64
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn resize(&mut self, _truncate: bool, _off: i64, _len: i64, _mode: i32) -> IOResult<()> {
        Err(IOError::from("resize requires the existing file backend"))
    }
}

impl OwnedBlockWrite for StagedWriteSession {
    fn finish(
        &mut self,
        len: i64,
        commit: bool,
        checksum: Option<BlockChecksum>,
    ) -> CommonResult<()> {
        if commit {
            self.complete_with_checksum(len, checksum)?;
        } else {
            self.cancel()?;
        }
        Ok(())
    }
}

impl Drop for StagedWriteSession {
    fn drop(&mut self) {
        self.abandon();
    }
}
