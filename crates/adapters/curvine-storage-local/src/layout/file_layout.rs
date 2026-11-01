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

use crate::layout::{validate_open_offset, BlockLayout};
use crate::{
    BlockChecksum, BlockMeta, BlockState, CHECKSUM_FILE_SUFFIX, DEFAULT_CHECKSUM_CHUNK_SIZE,
};
use crate::{BlockReadContext, BlockWriteContext, VfsDir};
use curvine_core_error::{err_box, try_err, CommonResult};
use curvine_io::{IOResult, LocalFile};
use curvine_model::ExtendedBlock;
#[cfg(test)]
use curvine_runtime::common::ByteUnit;
use curvine_runtime::common::FileUtils;
use std::collections::HashSet;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy)]
pub struct FileLayout;

const ACTIVE_DIR: &str = "active";
const STAGING_DIR: &str = "staging";

/// Work prepared outside the dataset lock, published under it.
///
/// When checksums are enabled, `staging_checksum` is a synced sidecar for the
/// staging block. Dropping an unpublished plan removes it, so every failed
/// finalize path (prepare error, lost reservation, rename error) leaves no
/// orphan sidecar behind.
pub struct FileFinalizePlan {
    final_meta: BlockMeta,
    staging_path: PathBuf,
    active_path: PathBuf,
    staging_checksum: Option<PathBuf>,
}

impl FileFinalizePlan {
    pub(crate) fn final_meta(&self) -> &BlockMeta {
        &self.final_meta
    }
}

impl Drop for FileFinalizePlan {
    fn drop(&mut self) {
        if let Some(path) = self.staging_checksum.take() {
            if let Err(e) = FileLayout::remove_if_exists(&path) {
                log::warn!(
                    "failed to remove unpublished checksum {}: {}",
                    path.display(),
                    e
                );
            }
        }
    }
}

impl FileLayout {
    fn active_dir(dir: &VfsDir) -> PathBuf {
        dir.base_path().join(ACTIVE_DIR)
    }

    fn staging_dir(dir: &VfsDir) -> PathBuf {
        dir.base_path().join(STAGING_DIR)
    }

    fn ensure_layout_dirs(dir: &VfsDir) -> CommonResult<()> {
        FileUtils::create_dir(Self::active_dir(dir), true)?;
        FileUtils::create_dir(Self::staging_dir(dir), true)?;
        Ok(())
    }

    fn block_dir(dir: &VfsDir, meta: &BlockMeta) -> CommonResult<PathBuf> {
        let path = match meta.state() {
            BlockState::Finalized => {
                let block_id = meta.id() as u64;
                let d1 = (block_id >> 48) & 0x1F;
                let d2 = (block_id >> 32) & 0x1F;
                Self::active_dir(dir)
                    .join(format!("b{}", d1))
                    .join(format!("b{}", d2))
            }
            BlockState::Writing
            | BlockState::Recovering
            | BlockState::Allocating
            | BlockState::Finalizing
            // Quarantined is SPDK-only; unreachable here but kept exhaustive.
            | BlockState::Quarantined => Self::staging_dir(dir),
        };

        if path.exists() {
            if !path.is_dir() {
                return err_box!("Path {} not a dir", path.to_string_lossy());
            }
        } else {
            try_err!(fs::create_dir_all(&path));
        }
        Ok(path)
    }

    pub fn block_path(dir: &VfsDir, meta: &BlockMeta) -> CommonResult<PathBuf> {
        Ok(Self::block_dir(dir, meta)?.join(meta.state().get_name(meta.id())))
    }

    /// Checksum sidecar path for a block file path.
    pub fn checksum_path_of(block_path: &Path) -> PathBuf {
        let mut name = OsString::from(block_path.as_os_str());
        name.push(CHECKSUM_FILE_SUFFIX);
        PathBuf::from(name)
    }

    fn is_checksum_file(file: &str) -> bool {
        file.ends_with(CHECKSUM_FILE_SUFFIX)
    }

    /// Returns whether a file was removed.
    fn remove_if_exists(path: &Path) -> std::io::Result<bool> {
        match fs::remove_file(path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Persist directory entry changes (unlink, rename) made in `path`'s parent.
    fn sync_parent_dir(path: &Path) -> std::io::Result<()> {
        match path.parent() {
            Some(parent) => File::open(parent)?.sync_all(),
            None => Ok(()),
        }
    }

    /// Compute the sidecar for `block_path` and write it, synced, next to it.
    /// `expected_len` guards against the file changing under the read.
    fn write_checksum(block_path: &Path, expected_len: i64) -> CommonResult<PathBuf> {
        let path = Self::checksum_path_of(block_path);
        let result = (|| -> CommonResult<()> {
            let sum =
                BlockChecksum::from_reader(DEFAULT_CHECKSUM_CHUNK_SIZE, File::open(block_path)?)?;
            if sum.data_len() != expected_len as u64 {
                return err_box!(
                    "checksum of {} covered {} bytes, expected {}",
                    block_path.display(),
                    sum.data_len(),
                    expected_len
                );
            }
            let mut file = File::create(&path)?;
            file.write_all(&sum.encode())?;
            file.sync_all()?;
            Ok(())
        })();
        if let Err(e) = result {
            let _ = Self::remove_if_exists(&path);
            return Err(e);
        }
        Ok(path)
    }

    /// `checksum` writes a synced sidecar for the staging file before publish.
    pub(crate) fn prepare_finalize(
        dir: &VfsDir,
        meta: &BlockMeta,
        committed_len: i64,
        checksum: bool,
    ) -> CommonResult<FileFinalizePlan> {
        let staging_path = Self::block_path(dir, meta)?;
        // Sparse seek-past-EOF resize can raise a committed block's logical
        // length on the master without rewriting worker bytes. A later partial
        // rewrite then completes with committed_len larger than the staging
        // file size. Only materialize that logical length for known rewrites;
        // first writes must still match the staging file exactly.
        let mut finalized_probe = meta.clone();
        finalized_probe.state = BlockState::Finalized;
        let active_path = Self::block_path(dir, &finalized_probe)?;
        let is_rewrite = active_path.exists();
        if is_rewrite && committed_len >= 0 {
            let current_len = staging_path.metadata()?.len() as i64;
            if committed_len > current_len {
                OpenOptions::new()
                    .write(true)
                    .open(&staging_path)?
                    .set_len(committed_len as u64)?;
            }
        }
        let final_meta = BlockMeta::with_final(meta, &staging_path)?;
        if final_meta.len() != committed_len {
            return err_box!(
                "Block {} length mismatch, expected: {}, actual: {}",
                meta.id(),
                committed_len,
                final_meta.len()
            );
        }

        let active_path = Self::block_path(dir, &final_meta)?;
        let staging_checksum = if checksum {
            Some(Self::write_checksum(&staging_path, committed_len)?)
        } else {
            None
        };
        Ok(FileFinalizePlan {
            final_meta,
            staging_path,
            active_path,
            staging_checksum,
        })
    }

    /// Publish order keeps a block from ever pairing with another generation's
    /// sidecar, in memory and after a crash:
    ///
    /// 1. Remove any active sidecar and, if one existed, sync the directory.
    ///    Always done, because a sidecar left by an earlier generation of this
    ///    block id (rewrite, or a failed sidecar delete) is stale either way.
    /// 2. Rename the block. With a new sidecar, sync the directory so the block
    ///    rename is durable before the sidecar can become visible.
    /// 3. Rename the new sidecar.
    ///
    /// Outcomes on failure: if step 1 fails, publish fails before the rename
    /// and rollback keeps the committed block and its sidecar. If step 2's
    /// rename fails, rollback keeps the committed block, but its sidecar is
    /// already gone and T5 handles the missing sidecar. If the sync in step 2
    /// or step 3 fails, the new block stays published without a sidecar.
    pub(crate) fn publish_finalize(mut plan: FileFinalizePlan) -> CommonResult<BlockMeta> {
        let active_checksum = Self::checksum_path_of(&plan.active_path);
        if Self::remove_if_exists(&active_checksum)? {
            Self::sync_parent_dir(&active_checksum)?;
        }
        FileUtils::rename(&plan.staging_path, &plan.active_path)?;
        if let Some(staging_checksum) = plan.staging_checksum.take() {
            let published = Self::sync_parent_dir(&plan.active_path)
                .and_then(|_| fs::rename(&staging_checksum, &active_checksum));
            if let Err(e) = published {
                // The block is already visible; keep it and publish without a
                // sidecar rather than failing a completed rename.
                log::warn!(
                    "block {} published without checksum, sidecar publish failed: {}",
                    plan.final_meta.id(),
                    e
                );
                let _ = Self::remove_if_exists(&staging_checksum);
            }
        }
        Ok(plan.final_meta.clone())
    }

    fn block_file(dir: &VfsDir, meta: &BlockMeta) -> CommonResult<String> {
        Ok(Self::block_path(dir, meta)?.to_string_lossy().to_string())
    }

    #[cfg(test)]
    pub(crate) fn write_test_data(dir: &VfsDir, meta: &BlockMeta, size: &str) -> CommonResult<()> {
        let bytes = ByteUnit::from_str(size)?.as_byte();
        LocalFile::write_string(
            Self::block_path(dir, meta)?,
            &"A".repeat(bytes as usize),
            true,
        )?;
        Ok(())
    }
}

impl BlockLayout for FileLayout {
    fn preserves_committed_on_write(&self) -> bool {
        true
    }

    fn allocate(&self, dir: &VfsDir, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        let meta = BlockMeta::with_tmp(block, dir);
        let file = Self::block_path(dir, &meta)?;
        OpenOptions::new().write(true).create_new(true).open(file)?;
        Ok(meta)
    }

    fn prepare_write(
        &self,
        dir: &VfsDir,
        meta: &BlockMeta,
        block: &ExtendedBlock,
    ) -> CommonResult<BlockMeta> {
        if block.len < 0 {
            return err_box!("Invalid file block size: {}", block.len);
        }
        let mut prepared = BlockMeta::new(meta.id(), block.len, dir);
        // A staging rewrite starts from the committed allocation and may grow, so charge the larger size.
        prepared.actual_len = meta.actual_len.max(block.len);

        if meta.is_final() {
            let committed_path = Self::block_path(dir, meta)?;
            let staging_path = Self::block_path(dir, &prepared)?;
            if staging_path.exists() {
                return err_box!(
                    "Cannot rewrite block {} because staging file {} already exists",
                    meta.id(),
                    staging_path.display()
                );
            }

            // BlockStore reserves this rewrite before the copy and serializes
            // same-block mutations. Existing readers keep using active while
            // the staging copy runs outside the dataset write lock.
            if let Err(e) = fs::copy(&committed_path, &staging_path) {
                let _ = fs::remove_file(&staging_path);
                return Err(e.into());
            }
        }

        Ok(prepared)
    }

    fn finalize(
        &self,
        dir: &VfsDir,
        meta: &BlockMeta,
        committed_len: i64,
        checksum: bool,
    ) -> CommonResult<BlockMeta> {
        let plan = Self::prepare_finalize(dir, meta, committed_len, checksum)?;
        Self::publish_finalize(plan)
    }

    fn scan(&self, dir: &VfsDir) -> CommonResult<Vec<BlockMeta>> {
        Self::ensure_layout_dirs(dir)?;
        let active_dir = FileUtils::list_files(Self::active_dir(dir), true)?;
        let staging_dir = FileUtils::list_files(Self::staging_dir(dir), true)?;

        let mut blocks = vec![];
        let mut active_ids = HashSet::new();
        let mut active_files = HashSet::new();
        let mut active_checksums = vec![];
        for file in active_dir {
            if Self::is_checksum_file(&file) {
                active_checksums.push(file);
            } else if let Ok(meta) = BlockMeta::from_file(&file, BlockState::Finalized, dir) {
                active_ids.insert(meta.id());
                active_files.insert(file);
                blocks.push(meta);
            }
        }
        // Remove sidecars whose block is gone (crash during deallocate).
        for file in active_checksums {
            let block = file.strip_suffix(CHECKSUM_FILE_SUFFIX).unwrap_or(&file);
            if !active_files.contains(block) {
                Self::remove_if_exists(Path::new(&file))?;
            }
        }

        for file in staging_dir {
            // Staging blocks restart as Recovering with unknown content, so
            // any staging sidecar is untrusted.
            if Self::is_checksum_file(&file) {
                Self::remove_if_exists(Path::new(&file))?;
                continue;
            }
            if let Ok(meta) = BlockMeta::from_file(&file, BlockState::Recovering, dir) {
                // A crash during a rewrite can leave the committed active file
                // and an incomplete staging copy. Prefer the published active
                // generation and discard staging before capacity is reserved.
                if active_ids.contains(&meta.id()) {
                    fs::remove_file(file)?;
                    continue;
                }
                blocks.push(meta);
            }
        }

        Ok(blocks)
    }

    // Filesystem blocks own no under-lock state; offset/file teardown is in deallocate().
    fn release(&self, _dir: &VfsDir, _meta: &BlockMeta) {}

    fn deallocate(&self, dir: &VfsDir, meta: &BlockMeta) -> CommonResult<()> {
        let path = Self::block_path(dir, meta)?;
        FileUtils::delete_path(&path, false)?;
        // The block is already gone, so a leftover sidecar is harmless here and
        // is removed by the next scan.
        let checksum = Self::checksum_path_of(&path);
        if let Err(e) = Self::remove_if_exists(&checksum) {
            log::warn!("failed to remove checksum {}: {}", checksum.display(), e);
        }
        Ok(())
    }

    fn open_writer(&self, dir: &VfsDir, meta: &BlockMeta, off: i64) -> IOResult<BlockWriteContext> {
        validate_open_offset(meta, off)?;
        let file = Self::block_file(dir, meta)?;
        let device = LocalFile::with_write_offset(file, false, off)?;
        BlockWriteContext::new(device, 0, meta.len, off)
    }

    fn open_reader(
        &self,
        dir: &VfsDir,
        meta: &BlockMeta,
        off: i64,
        logical_len: i64,
    ) -> IOResult<BlockReadContext> {
        let file = Self::block_file(dir, meta)?;
        // The opened file is the source of truth for bytes that can be read
        // physically, bounded by the logical length below.
        let inner = OpenOptions::new().read(true).open(&file)?;
        let device = LocalFile::from_file(&file, inner)?;
        if off < 0 || off > logical_len {
            return err_box!(
                "Invalid block offset: {}, block length: {}",
                off,
                logical_len
            );
        }
        // Only the logical range beyond the committed worker length may be a
        // sparse tail (master-side truncate up). Bytes the worker committed
        // must be present; a shorter file means the block was truncated or
        // damaged, and zero-filling it would return corrupted data as success.
        let committed_len = meta.len.min(logical_len);
        if device.len() < committed_len {
            return err_box!(
                "Block {} is truncated: committed length {}, file length {}",
                meta.id,
                committed_len,
                device.len()
            );
        }
        let physical_len = device.len().min(logical_len);
        BlockReadContext::with_physical(device, 0, logical_len, physical_len, off)
    }

    fn short_circuit(&self, dir: &VfsDir, meta: &BlockMeta) -> CommonResult<Option<String>> {
        Ok(Some(Self::block_file(dir, meta)?))
    }
}
