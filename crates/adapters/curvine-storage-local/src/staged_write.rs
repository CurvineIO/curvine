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

use crate::{write_watermark, BlockReadContext};
use curvine_io::{IOResult, LocalFile};
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::NamedTempFile;

pub(crate) const SCRATCH_PREFIX: &str = ".curvine-uring-staging-";
const ROLLBACK_SUFFIX: &str = ".uring-rollback";
const RETIRED_SUFFIX: &str = ".uring-retired";

pub(crate) fn is_scratch_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with(SCRATCH_PREFIX))
}

fn staging_parent(path: &Path) -> io::Result<&Path> {
    path.parent()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "staging file path has no parent"))
}

fn new_scratch(parent: &Path) -> io::Result<NamedTempFile> {
    tempfile::Builder::new()
        .prefix(SCRATCH_PREFIX)
        .tempfile_in(parent)
}

fn copy_extent(source: &mut File, scratch: &mut File, extent: u64) -> io::Result<()> {
    source.seek(SeekFrom::Start(0))?;
    if io::copy(source, scratch)? != extent {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "staging source changed during copy",
        ));
    }
    Ok(())
}

fn metadata_if_exists(path: &Path) -> io::Result<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(meta) => Ok(Some(meta)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn remove_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

fn same_inode(a: &fs::Metadata, b: &fs::Metadata) -> bool {
    (a.dev(), a.ino()) == (b.dev(), b.ino())
}

fn sync_watermark(path: &Path, extent: u64, parent: &File) -> IOResult<()> {
    if write_watermark::read(path) == Some(extent) {
        parent.sync_all()?;
    } else {
        // The watermark's directory sync also persists changes to the block names.
        write_watermark::write(path, extent)?;
    }
    Ok(())
}

struct CheckpointNames {
    rollback: PathBuf,
    retired: PathBuf,
}

impl CheckpointNames {
    fn new(path: &Path) -> Self {
        let with_suffix = |suffix: &str| {
            let mut name = path.as_os_str().to_os_string();
            name.push(suffix);
            PathBuf::from(name)
        };
        Self {
            rollback: with_suffix(ROLLBACK_SUFFIX),
            retired: with_suffix(RETIRED_SUFFIX),
        }
    }

    fn unresolved(&self) -> io::Result<bool> {
        Ok(metadata_if_exists(&self.rollback)?.is_some()
            || metadata_if_exists(&self.retired)?.is_some())
    }

    fn rearm(&self, parent: &File) -> io::Result<()> {
        if metadata_if_exists(&self.rollback)?.is_none() {
            // Keep the retired link until the old canonical inode and length
            // have been restored. Even if repair fails, a reader sees a fence.
            fs::hard_link(&self.retired, &self.rollback)?;
        }
        if metadata_if_exists(&self.retired)?.is_some() {
            // A previous rearm may have linked rollback but failed to sync it.
            // Make the rollback decision durable before changing canonical.
            parent.sync_all()?;
        }
        Ok(())
    }

    // Only after the chosen canonical inode AND its watermark are durable.
    // Rename rather than unlink: a failed decision sync must not drop the
    // last name of the old inode. On restart a surviving rollback name wins;
    // a retired name alone means the decision reached disk, not that a reply
    // reached the client. Live opens cannot make that inference after an EIO.
    fn retire(&self, parent: &File) -> io::Result<()> {
        let decision = fs::rename(&self.rollback, &self.retired).and_then(|()| parent.sync_all());
        if let Err(e) = decision {
            if let Err(rearm) = self.rearm(parent) {
                log::warn!("failed to rearm {}: {}", self.rollback.display(), rearm);
            }
            return Err(e);
        }
        // The absence of the authoritative name is durable. A crash may
        // resurrect only the non-authoritative link; no cleanup sync needed.
        // Live opens treat a surviving retired link as unresolved. Do not
        // acknowledge a checkpoint that they would subsequently reject.
        remove_if_exists(&self.retired)
    }

    /// Exclusive repair; readers can pin `rollback` during any partial repair.
    fn restore(&self, path: &Path, parent: &File) -> IOResult<()> {
        self.rearm(parent)?;
        let old = fs::symlink_metadata(&self.rollback)?;
        if !old.is_file() {
            return Err(io::Error::new(ErrorKind::InvalidData, "invalid checkpoint backup").into());
        }
        if !metadata_if_exists(path)?.is_some_and(|live| same_inode(&old, &live)) {
            // Don't consume the rollback name, even if canonical is missing.
            // A crash or an error between these calls leaves it usable.
            remove_if_exists(path)?;
            fs::hard_link(&self.rollback, path)?;
        }
        sync_watermark(path, old.len(), parent)?;
        // If rearmed, both names may link to the same inode; renaming one
        // over the other would do nothing. It is now safe to clear the alias.
        remove_if_exists(&self.retired)?;
        self.retire(parent)?;
        Ok(())
    }
}

fn unresolved_checkpoint() -> io::Error {
    io::Error::other("staging checkpoint requires repair")
}

pub(crate) fn checkpoint_unresolved(path: &Path) -> io::Result<bool> {
    CheckpointNames::new(path).unresolved()
}

pub(crate) fn require_resolved_checkpoint(path: &Path) -> io::Result<()> {
    if checkpoint_unresolved(path)? {
        return Err(unresolved_checkpoint());
    }
    Ok(())
}

pub(crate) fn open_checkpoint_reader(path: &Path) -> io::Result<File> {
    // Pin canonical before looking at the names. Pinning it after a negative
    // lookup could race with the installation of an uncommitted candidate.
    let canonical = File::open(path);
    let names = CheckpointNames::new(path);
    match File::open(&names.rollback) {
        Ok(backup) => return Ok(backup),
        Err(e) if e.kind() == ErrorKind::NotFound => (),
        Err(e) => return Err(e),
    }
    if metadata_if_exists(&names.retired)?.is_some() {
        return Err(unresolved_checkpoint());
    }
    // If repair replaced canonical and retired the marker between the open
    // and lookup, the pinned inode is not safe to serve.
    let canonical = canonical?;
    if !same_inode(&canonical.metadata()?, &fs::metadata(path)?) {
        return Err(unresolved_checkpoint());
    }
    Ok(canonical)
}

pub(crate) fn checkpoint_artifact_block(path: &Path) -> Option<PathBuf> {
    let name = path.file_name()?.to_str()?;
    let block = name
        .strip_suffix(ROLLBACK_SUFFIX)
        .or_else(|| name.strip_suffix(RETIRED_SUFFIX))?;
    crate::BlockState::check_file(block)?;
    Some(path.with_file_name(block))
}

/// Startup only, with no active writers. Resolve before applying watermarks.
pub(crate) fn recover_checkpoint_at_startup(path: &Path) -> IOResult<()> {
    let names = CheckpointNames::new(path);
    if metadata_if_exists(&names.rollback)?.is_some() {
        names.restore(path, &File::open(staging_parent(path)?)?)?;
    }
    remove_if_exists(&names.retired)?;
    Ok(())
}

/// Explicit abort/delete may discard the old inode, but must never leave an
/// unguarded candidate or resurrect a checkpoint after a successful delete.
pub(crate) fn discard_unresolved_checkpoint(path: &Path) -> io::Result<()> {
    let names = CheckpointNames::new(path);
    if names.unresolved()? {
        let parent = File::open(staging_parent(path)?)?;
        remove_if_exists(path)?;
        parent.sync_all()?;
        remove_if_exists(&names.retired)?;
        remove_if_exists(&names.rollback)?;
        parent.sync_all()?;
    }
    Ok(())
}

fn replace_checkpoint(
    path: &Path,
    names: &CheckpointNames,
    scratch: NamedTempFile,
    permissions: fs::Permissions,
    extent: u64,
    sync_install: impl FnOnce(&Path, u64, &File) -> IOResult<()>,
) -> IOResult<File> {
    scratch.as_file().set_permissions(permissions)?;
    scratch.as_file().sync_all()?;
    let parent = File::open(staging_parent(path)?)?;
    fs::hard_link(path, &names.rollback)?;
    let result = (|| -> IOResult<File> {
        // Neither the inode nor the watermark may change until the rollback
        // link is durable. The watermark update then also syncs the rename.
        parent.sync_all()?;
        let file = scratch.persist(path).map_err(|e| e.error)?;
        sync_install(path, extent, &parent)?;
        names.retire(&parent)?;
        Ok(file)
    })();
    if let Err(e) = &result {
        if let Err(restore) = names.restore(path, &parent) {
            log::warn!(
                "checkpoint {} failed ({}); rollback still requires repair: {}",
                path.display(),
                e,
                restore
            );
        }
    }
    result
}

fn recover_for_writer(path: &Path) -> IOResult<()> {
    let names = CheckpointNames::new(path);
    if metadata_if_exists(&names.rollback)?.is_some() {
        names.restore(path, &File::open(staging_parent(path)?)?)?;
    }
    require_resolved_checkpoint(path)?;
    Ok(())
}

fn complete_extent(path: &Path, meta: &fs::Metadata) -> io::Result<u64> {
    let extent = meta.len();
    if extent != 0 && write_watermark::read(path).is_none_or(|durable| durable < extent) {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "nonempty staging file has no complete durable checkpoint",
        ));
    }
    Ok(extent)
}

/// Move a complete checkpoint to a fresh inode before resuming in-place writes.
/// The caller holds the exclusive writer stripe and must keep the canonical
/// file unchanged while it is copied and replaced.
pub(crate) fn detach_checkpoint(path: &Path) -> IOResult<()> {
    // The watermark writer needs a nonempty parent even for a bare filename.
    let path = std::path::absolute(path)?;
    recover_for_writer(&path)?;
    let parent = staging_parent(&path)?;
    let dir = File::open(parent)?;
    let mut source = File::open(&path)?;
    let meta = source.metadata()?;
    let extent = complete_extent(&path, &meta)?;
    let mut scratch = new_scratch(parent)?;
    copy_extent(&mut source, scratch.as_file_mut(), extent)?;
    scratch.as_file().set_permissions(meta.permissions())?;
    scratch.as_file().sync_all()?;
    if write_watermark::read(&path) != Some(extent) {
        // Heal an ahead watermark left by an older worker's failed rename.
        write_watermark::write(&path, extent)?;
    }
    scratch.persist(&path).map_err(|e| e.error)?;
    // A failed sync needs no rollback: either inode has the same bytes and
    // length. Do not grant in-place writes until the detach is durable.
    dir.sync_all()?;
    Ok(())
}

/// A shared RPC payload can supply an owned handle for completion-based I/O;
/// a borrowed buffer is valid only until the synchronous call returns.
#[derive(Clone, Copy)]
pub enum StagedPayload<'a> {
    Shared(&'a bytes::Bytes),
    Borrowed(&'a [u8]),
}

impl AsRef<[u8]> for StagedPayload<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Shared(bytes) => bytes,
            Self::Borrowed(slice) => slice,
        }
    }
}

// Carry optional ring timings out of the callback so collecting samples isn't
// charged to write_call on just one backend. Zero-sized without profiling.
#[derive(Default)]
pub(crate) struct WriteReceipt {
    #[cfg(feature = "write-bench-profile")]
    pub(crate) ring: Option<(crate::write_bench_profile::RingReply, std::time::Instant)>,
}

#[cfg(not(feature = "write-bench-profile"))]
const _: () = assert!(std::mem::size_of::<WriteReceipt>() == 0);

pub(crate) type WriteAt =
    dyn Fn(&Arc<File>, u64, StagedPayload<'_>) -> io::Result<WriteReceipt> + Send + Sync;

/// Pinned, completed bytes of a Writing block. This is a read snapshot, not
/// a durable checkpoint or a finalized block with a checksum sidecar.
#[derive(Clone)]
pub struct StagedReadView {
    file: Arc<File>,
    path: Arc<Path>,
    physical_len: i64,
}

impl StagedReadView {
    /// Open from the pinned descriptor, even after the scratch file is renamed
    /// or unlinked. A logical tail beyond this snapshot reads as zeros.
    pub fn open_reader(&self, off: i64, logical_len: i64) -> IOResult<BlockReadContext> {
        // The path is only a diagnostic label. LocalFile's buffered reads
        // must use explicit offsets because try_clone shares the FD cursor.
        let mut device =
            LocalFile::from_file(&self.path.to_string_lossy(), self.file.try_clone()?)?;
        device.use_positional_reads();
        BlockReadContext::with_physical(
            device,
            0,
            logical_len,
            self.physical_len.min(logical_len),
            off,
        )
    }
}

struct WorkingFile {
    name: NamedTempFile,
    file: Arc<File>,
    // The latest view covers the largest prefix exposed on this inode.
    view: Option<StagedReadView>,
}

impl WorkingFile {
    fn read_view(&mut self, path: &Arc<Path>) -> IOResult<StagedReadView> {
        let meta = self.file.metadata()?;
        let physical_len = i64::try_from(meta.len()).map_err(|_| {
            io::Error::new(ErrorKind::InvalidData, "staging extent does not fit in i64")
        })?;
        if let Some(view) = &mut self.view {
            view.physical_len = physical_len;
            return Ok(view.clone());
        }

        // Open once per viewed inode, not once per append or reader. This must
        // be a separate, read-only open description: readers can seek without
        // disturbing io::copy on the writer's working or checkpoint handle.
        let file = File::open(self.name.path())?;
        let read_meta = file.metadata()?;
        if (read_meta.dev(), read_meta.ino()) != (meta.dev(), meta.ino()) {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "staging scratch file was replaced",
            )
            .into());
        }
        let view = StagedReadView {
            file: Arc::new(file),
            path: path.clone(),
            physical_len,
        };
        self.view = Some(view.clone());
        Ok(view)
    }
}

/// Positional writes to an isolated inode. The canonical staging pathname
/// stays on the last checkpoint, so a failed write below the watermark
/// cannot change bytes recovered from that pathname.
///
/// The caller owns this staging generation exclusively and must not change
/// its bytes through another handle, publish after an error, or acknowledge a
/// durable flush until `checkpoint` succeeds. Live readers use `read_view`
/// rather than opening the canonical staging pathname.
pub struct StagedFileWriter {
    path: Arc<Path>,
    published: File,
    working: Option<WorkingFile>,
    write_at: Box<WriteAt>,
    failed: bool,
    removed_placeholder: bool,
}

impl StagedFileWriter {
    #[cfg(test)]
    pub(crate) fn new(
        path: PathBuf,
        write_at: impl Fn(&Arc<File>, u64, &[u8]) -> io::Result<()> + Send + Sync + 'static,
    ) -> IOResult<Self> {
        Self::with_payload_writer(
            path,
            Box::new(move |file, offset, payload| {
                write_at(file, offset, payload.as_ref()).map(|()| WriteReceipt::default())
            }),
        )
    }

    pub(crate) fn with_payload_writer(path: PathBuf, write_at: Box<WriteAt>) -> IOResult<Self> {
        recover_for_writer(&path)?;
        let published = OpenOptions::new().read(true).write(true).open(&path)?;
        complete_extent(&path, &published.metadata()?)?;
        Ok(Self::from_open_file(path, published, write_at))
    }

    pub(crate) fn from_open_file(path: PathBuf, published: File, write_at: Box<WriteAt>) -> Self {
        Self {
            path: path.into(),
            published,
            working: None,
            write_at,
            failed: false,
            removed_placeholder: false,
        }
    }

    /// The canonical staging path, used for diagnostics by the writer session.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Write one payload at a validated block offset. Any I/O error poisons
    /// the interval; retrying a write can never make it publishable.
    pub fn write_all_at(&mut self, offset: u64, payload: &[u8]) -> IOResult<()> {
        self.write_payload_at(offset, StagedPayload::Borrowed(payload))
    }

    pub fn write_payload_at(&mut self, offset: u64, payload: StagedPayload<'_>) -> IOResult<()> {
        self.write_payload(offset, payload, false)
    }

    /// First write under a fresh allocation's exclusive lease, before any checkpoint.
    pub fn write_fresh_payload_at(
        &mut self,
        offset: u64,
        payload: StagedPayload<'_>,
    ) -> IOResult<()> {
        self.write_payload(offset, payload, true)
    }

    fn write_payload(
        &mut self,
        offset: u64,
        payload: StagedPayload<'_>,
        fresh: bool,
    ) -> IOResult<()> {
        self.check_healthy()?;
        if payload.as_ref().is_empty() {
            return Ok(());
        }
        let result = self.prepare_working(offset, fresh).and_then(|()| {
            let working = self.working.as_ref().expect("prepared working file");
            #[cfg(feature = "write-bench-profile")]
            let start = crate::write_bench_profile::start_write(payload.as_ref().len());
            let _receipt = (self.write_at)(&working.file, offset, payload)?;
            #[cfg(feature = "write-bench-profile")]
            if let Some(start) = start {
                crate::write_bench_profile::record_write_call(start.elapsed(), _receipt.ring);
            }
            Ok(())
        });
        if result.is_err() {
            self.poison();
        }
        result
    }

    /// Capture the completed extent after a successful nonempty `write_all_at`
    /// and before `checkpoint`. Only the returned view, not the writer, is
    /// needed to open readers after subsequent writes or writer drop.
    pub fn read_view(&mut self) -> IOResult<StagedReadView> {
        self.check_healthy()?;
        let working = self
            .working
            .as_mut()
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "no staging write to expose"))?;
        // The exclusive writer has no in-flight writes on a healthy inode.
        // If pinning fails, the attempted generation must not be checkpointed.
        let result = working.read_view(&self.path);
        if result.is_err() {
            self.poison();
        }
        result
    }

    /// The completed private inode of a fresh, never-checkpointed write.
    /// Borrow its path without releasing the `NamedTempFile`: a failed
    /// finalize must leave it available to this session, not to recovery.
    pub(crate) fn buffered_complete_source(&self, staging_path: &Path) -> IOResult<PathBuf> {
        self.check_healthy()?;
        let published = self.published.metadata()?;
        let placeholder_matches = match metadata_if_exists(staging_path)? {
            Some(live) => {
                !self.removed_placeholder && live.is_file() && same_inode(&published, &live)
            }
            None => self.removed_placeholder,
        };
        if staging_path != self.path.as_ref()
            || published.len() != 0
            || !placeholder_matches
            || metadata_if_exists(&write_watermark::path_of(staging_path))?.is_some()
        {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "buffered completion requires an unchanged, empty staging file without a watermark",
            )
            .into());
        }
        let working = self.working.as_ref().ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidInput,
                "no completed private write to publish",
            )
        })?;
        let source = working.name.path();
        let live = fs::symlink_metadata(source)?;
        if !live.is_file() || !same_inode(&live, &working.file.metadata()?) {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "staging scratch file was replaced",
            )
            .into());
        }
        working.file.set_permissions(published.permissions())?;
        Ok(source.to_path_buf())
    }

    pub(crate) fn persist_buffered_complete(
        &mut self,
        source: &Path,
        active: &Path,
    ) -> IOResult<()> {
        self.check_healthy()?;
        let working = self.working.as_ref().ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidInput,
                "no completed private write to publish",
            )
        })?;
        if working.name.path() != source {
            return Err(
                io::Error::new(ErrorKind::InvalidData, "buffered finalize source changed").into(),
            );
        }
        // Remove the empty placeholder before publication. A failed unlink
        // must not leave a successful commit that blocks the next rewrite.
        // Readers keep the private view until the dataset publishes the rename.
        if !self.removed_placeholder {
            fs::remove_file(&self.path)?;
            self.removed_placeholder = true;
        }
        let WorkingFile { name, file, view } = self.working.take().unwrap();
        match name.persist_noclobber(active) {
            Ok(_) => Ok(()),
            Err(error) => {
                self.working = Some(WorkingFile {
                    name: error.file,
                    file,
                    view,
                });
                Err(error.error.into())
            }
        }
    }

    /// Checkpoint the complete file extent, not the most recent write offset.
    pub fn checkpoint(&mut self) -> IOResult<u64> {
        self.checkpoint_with_sync(sync_watermark)
    }

    fn checkpoint_with_sync(
        &mut self,
        sync_install: impl FnOnce(&Path, u64, &File) -> IOResult<()>,
    ) -> IOResult<u64> {
        self.check_healthy()?;
        let result = self.checkpoint_inner(sync_install);
        if result.is_err() {
            self.poison();
        }
        result
    }

    fn check_healthy(&self) -> IOResult<()> {
        if self.failed {
            return Err(io::Error::other(
                "staging write interval failed; cannot reuse or publish it",
            )
            .into());
        }
        Ok(())
    }

    fn poison(&mut self) {
        self.failed = true;
        // The executor may still own the file after loss of its completion
        // channel. Unlinking a private inode cannot affect another generation.
        self.working.take();
    }

    fn prepare_working(&mut self, offset: u64, fresh: bool) -> IOResult<()> {
        if let Some(working) = &self.working {
            let visible_prefix = working
                .view
                .as_ref()
                .map_or(0, |view| view.physical_len as u64);
            if offset >= visible_prefix {
                return Ok(());
            }
        }
        // An overlapping write can fail after changing part of its range.
        // Fork before submitting it, leaving the visible prefix untouched.
        let parent = staging_parent(&self.path)?;
        let (source, fresh_placeholder) = match &mut self.working {
            Some(working) => (working.name.as_file_mut(), false),
            None => (&mut self.published, fresh),
        };
        let mut name = new_scratch(parent)?;
        let meta = source.metadata()?;
        // A newly created scratch file already has the contents of a verified
        // empty placeholder. Never infer freshness from length alone.
        if !(fresh_placeholder && meta.is_file() && meta.len() == 0) {
            copy_extent(source, name.as_file_mut(), meta.len())?;
        }
        let file = Arc::new(name.as_file().try_clone()?);
        self.working = Some(WorkingFile {
            name,
            file,
            view: None,
        });
        Ok(())
    }

    fn checkpoint_inner(
        &mut self,
        sync_install: impl FnOnce(&Path, u64, &File) -> IOResult<()>,
    ) -> IOResult<u64> {
        let names = CheckpointNames::new(&self.path);
        if names.unresolved()? {
            return Err(unresolved_checkpoint().into());
        }
        if self.removed_placeholder {
            // A failed buffered Complete removed only an empty, never-flushed
            // staging file. Re-create it if the owner requests a durable Flush.
            let permissions = self.published.metadata()?.permissions();
            let placeholder = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&self.path)?;
            placeholder.set_permissions(permissions)?;
            self.published = placeholder;
            self.removed_placeholder = false;
        }
        let old = self.published.metadata()?;
        let live = fs::metadata(&self.path)?;
        if !same_inode(&old, &live) || old.len() != live.len() {
            return Err(
                io::Error::new(ErrorKind::InvalidData, "staging checkpoint was replaced").into(),
            );
        }
        let Some(working) = self.working.take() else {
            self.published.sync_data()?;
            write_watermark::write(&self.path, old.len())?;
            return Ok(old.len());
        };
        let extent = working.file.metadata()?.len();
        if extent < old.len() {
            return Err(
                io::Error::new(ErrorKind::InvalidData, "staging checkpoint cannot shrink").into(),
            );
        }
        self.published = replace_checkpoint(
            &self.path,
            &names,
            working.name,
            old.permissions(),
            extent,
            sync_install,
        )?;
        Ok(extent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BlockLayout, BlockMeta, BlockState, ChecksumVerify, FileLayout, VfsDir};
    use curvine_core_error::CommonResult;
    use curvine_io::DataSlice;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{FileExt, PermissionsExt};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn fresh_staging_block() -> IOResult<(tempfile::TempDir, PathBuf)> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("blk_1");
        File::create(&path)?;
        Ok((dir, path))
    }

    fn staging_block(data: &[u8]) -> IOResult<(tempfile::TempDir, PathBuf)> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("blk_1");
        fs::write(&path, data)?;
        File::open(&path)?.sync_data()?;
        write_watermark::write(&path, data.len() as u64)?;
        Ok((dir, path))
    }

    fn read_bytes(reader: &mut BlockReadContext, len: i32) -> IOResult<Vec<u8>> {
        let DataSlice::Buffer(bytes) = reader.read_region(false, len)? else {
            panic!("expected buffered read");
        };
        Ok(bytes.to_vec())
    }

    fn read_view_bytes(view: &StagedReadView, len: i32) -> IOResult<Vec<u8>> {
        let mut reader = view.open_reader(0, len as i64)?;
        read_bytes(&mut reader, len)
    }

    #[test]
    fn sparse_first_write_does_not_read_the_empty_placeholder() -> IOResult<()> {
        let (_dir, path) = fresh_staging_block()?;
        let mut writer = StagedFileWriter::new(path.clone(), |file, offset, buf| {
            file.write_all_at(buf, offset)
        })?;
        // The checked copy rewinds this descriptor. A fresh write must not.
        writer.published.seek(SeekFrom::Start(9))?;
        writer.write_fresh_payload_at(3, StagedPayload::Borrowed(b"XY"))?;
        assert_eq!(writer.published.stream_position()?, 9);
        let view = writer.read_view()?;
        assert_eq!(read_view_bytes(&view, 5)?, b"\0\0\0XY");
        assert!(fs::read(&path)?.is_empty());
        assert_eq!(write_watermark::read(&path), None);

        assert_eq!(writer.checkpoint()?, 5);
        assert_eq!(fs::read(&path)?, b"\0\0\0XY");
        assert_eq!(write_watermark::read(&path), Some(5));
        Ok(())
    }

    #[test]
    fn first_write_checks_the_actual_source_before_skipping_the_copy() -> IOResult<()> {
        let (_dir, path) = staging_block(b"base")?;
        let mut writer = StagedFileWriter::new(path.clone(), |file, offset, buf| {
            file.write_all_at(buf, offset)
        })?;
        // Even a stale freshness hint must not discard a nonempty prefix.
        writer.write_fresh_payload_at(4, StagedPayload::Borrowed(b"!"))?;
        assert_eq!(read_view_bytes(&writer.read_view()?, 5)?, b"base!");
        drop(writer);
        assert_eq!(fs::read(&path)?, b"base");
        assert_eq!(write_watermark::read(&path), Some(4));
        Ok(())
    }

    #[test]
    fn views_keep_their_completed_prefix_across_append_and_overwrite() -> IOResult<()> {
        let (_dir, path) = staging_block(b"base")?;
        let mut writer = StagedFileWriter::new(path.clone(), |file, offset, buf| {
            file.write_all_at(buf, offset)
        })?;
        writer.write_all_at(4, b"12")?;
        let first = writer.read_view()?;
        let mut old_reader = first.open_reader(0, 8)?;
        assert!(!old_reader.is_verified());
        assert!(!old_reader.supports_send_file());
        assert_eq!(read_bytes(&mut old_reader, 4)?, b"base");

        writer.write_all_at(6, b"34")?;
        let second = writer.read_view()?;
        assert!(
            Arc::ptr_eq(&first.file, &second.file),
            "appends reuse the read descriptor"
        );
        assert_eq!(read_bytes(&mut old_reader, 4)?, b"12\0\0");
        // Even a new reader opened after the append sees the old physical cap.
        assert_eq!(read_view_bytes(&first, 8)?, b"base12\0\0");
        let mut second_reader = second.open_reader(0, 8)?;

        // This only overlaps the newer view, so we must protect its larger prefix.
        writer.write_all_at(6, b"XY")?;
        let third = writer.read_view()?;
        assert_ne!(second.file.metadata()?.ino(), third.file.metadata()?.ino());
        assert_eq!(read_bytes(&mut second_reader, 8)?, b"base1234");
        assert_eq!(read_view_bytes(&second, 8)?, b"base1234");
        assert_eq!(read_view_bytes(&third, 8)?, b"base12XY");
        assert_eq!(read_view_bytes(&third, 4)?, b"base");
        assert_eq!(fs::read(&path)?, b"base");
        Ok(())
    }

    #[test]
    fn partial_append_and_overwrite_do_not_change_existing_views() -> IOResult<()> {
        for offset in [6, 2] {
            let (dir, path) = staging_block(b"base")?;
            let mut writer = StagedFileWriter::new(path.clone(), |file, offset, buf| {
                if buf == b"XY" {
                    file.write_all_at(&buf[..1], offset)?;
                    return Err(io::Error::other("injected error after a partial write"));
                }
                file.write_all_at(buf, offset)
            })?;
            writer.write_all_at(4, b"12")?;
            let view = writer.read_view()?;
            let mut reader = view.open_reader(0, 8)?;

            let error = writer.write_all_at(offset, b"XY").unwrap_err();
            assert!(error
                .to_string()
                .contains("injected error after a partial write"));
            assert!(writer.read_view().is_err());
            assert!(writer.checkpoint().is_err());
            for entry in fs::read_dir(dir.path())? {
                assert!(!is_scratch_file(&entry?.path()));
            }
            drop(writer);
            assert_eq!(read_bytes(&mut reader, 8)?, b"base12\0\0");
            assert_eq!(read_view_bytes(&view, 8)?, b"base12\0\0");
            assert_eq!(write_watermark::read(&path), Some(4));
            assert_eq!(fs::read(&path)?, b"base");
        }
        Ok(())
    }

    #[test]
    fn pinned_readers_and_sendfile_coexist_with_checkpoint() -> IOResult<()> {
        let (_dir, path) = staging_block(b"abcd")?;
        let mut writer = StagedFileWriter::new(path.clone(), |file, offset, buf| {
            file.write_all_at(buf, offset)
        })?;
        writer.write_all_at(4, b"efgh")?;
        let view = writer.read_view()?;
        let working = writer.working.as_mut().unwrap();
        let scratch = working.name.path().to_path_buf();
        let copy_source = working.name.as_file_mut();
        copy_source.seek(SeekFrom::Start(7))?;
        let mut left = view.open_reader(1, 8)?;
        let mut right = view.clone().open_reader(4, 8)?;
        // Reader seeks cannot move the copy source, and buffered reads do not
        // depend on the cursor shared by the readers' duplicated descriptors.
        assert_eq!(copy_source.stream_position()?, 7);
        assert_eq!(read_bytes(&mut left, 2)?, b"bc");
        assert!(left.supports_send_file());
        let DataSlice::IOSlice(slice) = left.read_region(true, 2)? else {
            panic!("expected sendfile region");
        };
        assert_eq!(slice.off(), Some(3));

        assert_eq!(writer.checkpoint()?, 8);
        assert!(!scratch.exists());
        assert_eq!(read_bytes(&mut right, 2)?, b"ef");
        writer.published.seek(SeekFrom::Start(7))?;
        right.seek_to(2)?;
        assert_eq!(writer.published.stream_position()?, 7);
        writer.write_all_at(3, b"XY")?;
        assert_eq!(writer.checkpoint()?, 8);
        assert_eq!(fs::read(&path)?, b"abcXYfgh");
        drop(writer);

        assert_eq!(read_view_bytes(&view, 8)?, b"abcdefgh");
        assert_eq!(read_bytes(&mut left, 3)?, b"fgh");
        let target = tempfile::tempfile()?;
        let mut off = slice.off().unwrap();
        assert_eq!(
            curvine_sys::send_file(
                slice.raw_io(),
                target.as_raw_fd(),
                Some(&mut off),
                slice.len()
            )?,
            2
        );
        let mut sent = [0; 2];
        target.read_exact_at(&mut sent, 0)?;
        assert_eq!(&sent, b"de");
        Ok(())
    }

    #[test]
    fn refuses_to_adopt_an_unsynced_staging_tail() -> IOResult<()> {
        let (_dir, path) = staging_block(b"abcdefghij")?;
        OpenOptions::new().write(true).open(&path)?.set_len(12)?;
        let writer = StagedFileWriter::new(path.clone(), |file, offset, buf| {
            file.write_all_at(buf, offset)
        });
        assert!(writer.is_err());
        assert!(detach_checkpoint(&path).is_err());
        assert_eq!(fs::metadata(&path)?.len(), 12);
        assert_eq!(write_watermark::read(&path), Some(10));
        Ok(())
    }

    #[test]
    fn checkpoint_uses_extent_after_backward_overwrite() -> IOResult<()> {
        let (_dir, path) = staging_block(b"abcdefghij")?;
        let mut writer = StagedFileWriter::new(path.clone(), |file, offset, buf| {
            file.write_all_at(buf, offset)
        })?;
        writer.write_all_at(2, b"XY")?;
        assert_eq!(fs::read(&path)?, b"abcdefghij");
        assert_eq!(writer.checkpoint()?, 10);
        assert_eq!(write_watermark::read(&path), Some(10));
        assert_eq!(fs::read(&path)?, b"abXYefghij");

        writer.write_all_at(10, b"kl")?;
        assert_eq!(writer.checkpoint()?, 12);
        assert_eq!(fs::read(&path)?, b"abXYefghijkl");
        Ok(())
    }

    #[test]
    fn partial_overwrite_cannot_change_or_publish_checkpoint() -> IOResult<()> {
        let (_dir, path) = staging_block(b"abcdefghij")?;
        let calls = Arc::new(AtomicUsize::new(0));
        let mut writer = StagedFileWriter::new(path.clone(), {
            let calls = calls.clone();
            move |file, offset, buf| {
                calls.fetch_add(1, Ordering::Relaxed);
                file.write_all_at(&buf[..1], offset)?;
                Err(io::Error::other("injected error after a partial write"))
            }
        })?;
        let error = writer.write_all_at(2, b"XY").unwrap_err();
        assert!(error
            .to_string()
            .contains("injected error after a partial write"));
        assert!(writer.checkpoint().is_err());
        assert!(writer.write_all_at(2, b"XY").is_err());
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "failed data must not be retried"
        );
        drop(writer);
        assert_eq!(write_watermark::apply(&path)?, None);
        assert_eq!(write_watermark::read(&path), Some(10));
        assert_eq!(fs::read(&path)?, b"abcdefghij");
        Ok(())
    }

    #[test]
    fn post_rename_eio_restores_previous_checkpoint() -> IOResult<()> {
        const EIO: i32 = 5; // Linux errno; no libc dependency needed for the hook.
        let (_dir, path) = staging_block(b"base")?;
        let names = CheckpointNames::new(&path);
        let mut writer = StagedFileWriter::new(path.clone(), |file, offset, buf| {
            file.write_all_at(buf, offset)
        })?;
        writer.write_all_at(4, b"12")?;
        let pinned = writer.read_view()?;
        assert_eq!(writer.checkpoint()?, 6);
        let old_ino = fs::metadata(&path)?.ino();
        assert!(!names.unresolved()?);

        writer.write_all_at(1, b"XY")?;
        writer.write_all_at(6, b"34")?;
        assert_eq!(fs::read(&path)?, b"base12");
        assert_eq!(write_watermark::read(&path), Some(6));
        let err = writer
            .checkpoint_with_sync(|path, extent, parent| {
                assert_eq!(fs::read(path)?, b"bXYe1234");
                assert_eq!(write_watermark::read(path), Some(6));
                // Model an EIO at the install barrier whose directory changes
                // nonetheless reach disk. Before the fix they stayed published.
                sync_watermark(path, extent, parent)?;
                assert_eq!(write_watermark::read(path), Some(8));
                assert_eq!(fs::metadata(&names.rollback)?.ino(), old_ino);
                assert_eq!(open_checkpoint_reader(path)?.metadata()?.ino(), old_ino);
                Err(io::Error::from_raw_os_error(EIO).into())
            })
            .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(EIO));
        assert!(writer.checkpoint().is_err());
        drop(writer);

        assert_eq!(fs::metadata(&path)?.ino(), old_ino);
        assert_eq!(fs::read(&path)?, b"base12");
        assert_eq!(write_watermark::read(&path), Some(6));
        assert!(!names.unresolved()?);
        detach_checkpoint(&path)?;
        let legacy = OpenOptions::new().write(true).open(&path)?;
        assert_ne!(legacy.metadata()?.ino(), old_ino);
        legacy.write_all_at(b"XY", 1)?;
        assert_eq!(read_view_bytes(&pinned, 6)?, b"base12");
        Ok(())
    }

    #[test]
    fn scan_retries_rollback_before_advertising_staging_extent() -> CommonResult<()> {
        for missing_canonical in [false, true] {
            let root = tempfile::tempdir()?;
            let dir = VfsDir::from_str("", root.path().to_str().unwrap())?;
            let layout = FileLayout;
            let meta = BlockMeta::new(1, 8, &dir);
            let path = FileLayout::block_path(&dir, &meta)?;
            let names = CheckpointNames::new(&path);
            fs::write(&path, b"base")?;
            File::open(&path)?.sync_all()?;
            fs::hard_link(&path, &names.rollback)?;
            write_watermark::write(&path, 8)?;
            let old_ino = fs::metadata(&path)?.ino();
            // Simulate a crash after installation, or a repair that unlinked
            // the candidate but could not relink the backup at canonical.
            fs::remove_file(&path)?;
            if !missing_canonical {
                fs::write(&path, b"bXYe1234")?;
            }

            let mut reader = layout.open_reader(&dir, &meta, 0, 8, ChecksumVerify::Off)?;
            assert_eq!(read_bytes(&mut reader, 8)?, b"base\0\0\0\0");
            assert!(layout.short_circuit(&dir, &meta)?.is_none());
            assert!(layout.open_writer(&dir, &meta, 0).is_err());
            if !missing_canonical {
                assert_eq!(fs::read(&path)?, b"bXYe1234");
                // An exclusive repair that fails after relinking canonical
                // must keep the backup and still refuse in-place writes.
                let obstruction = path.with_file_name("blk_1.wmk.tmp");
                fs::create_dir(&obstruction)?;
                assert!(layout.detach_staged_checkpoint(&dir, &meta).is_err());
                assert_eq!(fs::metadata(&path)?.ino(), old_ino);
                assert_eq!(write_watermark::read(&path), Some(8));
                assert!(layout.open_writer(&dir, &meta, 0).is_err());
                fs::remove_dir(obstruction)?;
            }

            for _ in 0..2 {
                let blocks = layout.scan(&dir)?;
                assert_eq!(blocks.len(), 1);
                assert_eq!(blocks[0].state(), &BlockState::Recovering);
                assert_eq!(blocks[0].len(), 4);
                assert_eq!(fs::metadata(&path)?.ino(), old_ino);
                assert_eq!(fs::read(&path)?, b"base");
                assert_eq!(write_watermark::read(&path), Some(4));
                assert!(!names.unresolved()?);
            }
        }
        Ok(())
    }

    #[test]
    fn startup_ignores_a_retired_backup_after_a_durable_commit() -> IOResult<()> {
        let (_dir, path) = staging_block(b"base")?;
        let names = CheckpointNames::new(&path);
        let parent = File::open(staging_parent(&path)?)?;
        fs::hard_link(&path, &names.rollback)?;
        parent.sync_all()?;
        let mut scratch = NamedTempFile::new_in(staging_parent(&path)?)?;
        use std::io::Write;
        scratch.write_all(b"newbytes")?;
        scratch.as_file().sync_all()?;
        scratch.persist(&path).map_err(|e| e.error)?;
        write_watermark::write(&path, 8)?;
        // Simulate a crash before unlinking the last, non-authoritative name.
        fs::rename(&names.rollback, &names.retired)?;
        parent.sync_all()?;
        assert!(open_checkpoint_reader(&path).is_err());
        recover_checkpoint_at_startup(&path)?;
        assert!(!names.unresolved()?);
        assert_eq!(fs::read(&path)?, b"newbytes");
        assert_eq!(write_watermark::read(&path), Some(8));
        Ok(())
    }

    #[test]
    fn failed_checkpoint_rename_keeps_the_previous_pair() -> IOResult<()> {
        let (_dir, path) = staging_block(b"base")?;
        let old_ino = fs::metadata(&path)?.ino();
        let mut writer = StagedFileWriter::new(path.clone(), |file, offset, buf| {
            file.write_all_at(buf, offset)
        })?;
        writer.write_all_at(4, b"12")?;
        // The scratch descriptor still syncs, but persist cannot rename it.
        fs::remove_file(writer.working.as_ref().unwrap().name.path())?;
        assert_eq!(writer.checkpoint().unwrap_err().kind(), ErrorKind::NotFound);
        assert_eq!(fs::metadata(&path)?.ino(), old_ino);
        assert_eq!(fs::read(&path)?, b"base");
        assert_eq!(write_watermark::read(&path), Some(4));
        assert!(!checkpoint_unresolved(&path)?);
        Ok(())
    }

    #[test]
    fn checkpoint_metadata_failure_keeps_previous_inode() -> IOResult<()> {
        let (_dir, path) = staging_block(b"abcdefghij")?;
        // Obstruct the sidecar update after syncing the private inode.
        let obstruction = path.with_file_name("blk_1.wmk.tmp");
        fs::create_dir(&obstruction)?;
        let mut writer = StagedFileWriter::new(path.clone(), |file, offset, buf| {
            file.write_all_at(buf, offset)
        })?;
        writer.write_all_at(0, b"XY")?;
        writer.write_all_at(10, b"z")?;
        let view = writer.read_view()?;
        assert!(writer.checkpoint().is_err());
        fs::remove_dir(obstruction)?;
        assert!(writer.checkpoint().is_err());
        assert!(writer.write_all_at(0, b"XY").is_err());
        assert_eq!(read_view_bytes(&view, 11)?, b"XYcdefghijz");
        assert_eq!(write_watermark::read(&path), Some(10));
        assert_eq!(fs::read(&path)?, b"abcdefghij");
        Ok(())
    }

    #[test]
    fn old_writer_cannot_replace_a_successor_inode() -> IOResult<()> {
        let (_dir, path) = staging_block(b"abcdefghij")?;
        let mut writer = StagedFileWriter::new(path.clone(), |file, offset, buf| {
            file.write_all_at(buf, offset)
        })?;
        writer.write_all_at(10, b"XY")?;
        let successor = path.with_file_name("replacement");
        fs::write(&successor, b"ABCDEFGHIJ")?;
        fs::rename(successor, &path)?;
        assert!(writer.checkpoint().is_err());
        assert_eq!(fs::read(&path)?, b"ABCDEFGHIJ");
        assert_eq!(write_watermark::read(&path), Some(10));
        Ok(())
    }

    #[test]
    fn detach_checkpoint_preserves_pinned_view_and_repairs_watermark() -> IOResult<()> {
        let (_dir, path) = staging_block(b"abcd")?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640))?;
        let mut writer = StagedFileWriter::new(path.clone(), |file, offset, buf| {
            file.write_all_at(buf, offset)
        })?;
        writer.write_all_at(4, b"efgh")?;
        let view = writer.read_view()?;
        assert_eq!(writer.checkpoint()?, 8);
        drop(writer);
        let checkpoint_ino = fs::metadata(&path)?.ino();
        assert_eq!(view.file.metadata()?.ino(), checkpoint_ino);

        // Simulate a later checkpoint that updated the sidecar but failed to rename.
        write_watermark::write(&path, 12)?;
        detach_checkpoint(&path)?;
        let detached = fs::metadata(&path)?;
        assert_ne!(detached.ino(), checkpoint_ino);
        assert_eq!(detached.mode() & 0o777, 0o640);
        assert_eq!(write_watermark::read(&path), Some(8));
        assert_eq!(fs::read(&path)?, b"abcdefgh");

        OpenOptions::new()
            .write(true)
            .open(&path)?
            .write_all_at(b"XY", 2)?;
        assert_eq!(fs::read(&path)?, b"abXYefgh");
        assert_eq!(read_view_bytes(&view, 8)?, b"abcdefgh");
        Ok(())
    }

    #[test]
    fn partial_write_is_not_a_buffered_complete_source() -> IOResult<()> {
        let (_dir, path) = fresh_staging_block()?;
        let mut writer = StagedFileWriter::new(path.clone(), |file, offset, buf| {
            file.write_all_at(&buf[..2], offset)?;
            Err(io::Error::other(
                "injected write failure after a partial completion",
            ))
        })?;
        assert!(writer.write_all_at(0, b"partial").is_err());
        assert!(writer.buffered_complete_source(&path).is_err());
        assert!(writer.working.is_none());
        assert_eq!(fs::metadata(&path)?.len(), 0);
        assert!(write_watermark::read(&path).is_none());
        Ok(())
    }

    #[test]
    fn buffered_publish_collision_keeps_private_inode_for_retry_or_flush() -> IOResult<()> {
        for flush_after_failure in [false, true] {
            let (_dir, path) = fresh_staging_block()?;
            let active = path.with_file_name("active-block");
            let mut writer = StagedFileWriter::new(path.clone(), |file, offset, buf| {
                file.write_all_at(buf, offset)
            })?;
            writer.write_all_at(0, b"completed")?;
            let source = writer.buffered_complete_source(&path)?;
            fs::write(&active, b"unrelated generation")?;
            assert!(writer.persist_buffered_complete(&source, &active).is_err());
            assert!(!path.exists());
            assert_eq!(fs::read(&active)?, b"unrelated generation");
            assert_eq!(writer.buffered_complete_source(&path)?, source);
            if flush_after_failure {
                assert_eq!(writer.checkpoint()?, 9);
                assert_eq!(fs::read(&path)?, b"completed");
                assert_eq!(write_watermark::read(&path), Some(9));
            } else {
                fs::remove_file(&active)?;
                writer.persist_buffered_complete(&source, &active)?;
                assert!(!path.exists());
                assert!(!source.exists());
                assert_eq!(fs::read(&active)?, b"completed");
            }
        }
        Ok(())
    }

    #[test]
    fn buffered_complete_cannot_replace_durable_checkpoint() -> IOResult<()> {
        let (_dir, path) = fresh_staging_block()?;
        let mut writer = StagedFileWriter::new(path.clone(), |file, offset, buf| {
            file.write_all_at(buf, offset)
        })?;
        // Even an explicitly flushed empty block has a durable watermark.
        assert_eq!(writer.checkpoint()?, 0);
        writer.write_all_at(0, b"unflushed")?;
        assert!(writer.buffered_complete_source(&path).is_err());
        assert_eq!(fs::metadata(&path)?.len(), 0);
        assert_eq!(write_watermark::read(&path), Some(0));
        Ok(())
    }

    #[test]
    fn disconnect_drops_uncheckpointed_inode() -> IOResult<()> {
        let (_dir, path) = staging_block(b"abcdefghij")?;
        let mut writer = StagedFileWriter::new(path.clone(), |file, offset, buf| {
            file.write_all_at(buf, offset)
        })?;
        writer.write_all_at(0, b"XY")?;
        let view = writer.read_view()?;
        let scratch = writer.working.as_ref().unwrap().name.path().to_path_buf();
        assert!(is_scratch_file(&scratch));
        drop(writer);
        assert!(!scratch.exists());
        let mut reader = view.open_reader(0, 10)?;
        drop(view);
        assert_eq!(read_bytes(&mut reader, 10)?, b"XYcdefghij");
        assert_eq!(fs::read(&path)?, b"abcdefghij");
        Ok(())
    }
}
