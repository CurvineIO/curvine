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

use crate::fs::operator::{Read, Write};
use crate::fs::state::NodeState;
use crate::fs::{FuseReader, FuseWriter};
use crate::raw::fuse_abi::fuse_write_out;
use crate::session::FuseResponse;
use crate::{err_fuse, FuseError, FuseResult, FuseUtils};
use curvine_common::fs::{Path, StateReader, StateWriter};
use curvine_common::state::{CreateFileOptsBuilder, FileStatus, LockFlags, OpenFlags};
use log::warn;
use orpc::err_box;
use orpc::sync::AtomicCounter;
use orpc::sys::RawPtr;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;

/// Per-handle lock-owner bookkeeping.
///
/// POSIX locks are keyed by FUSE `lock_owner`. After `fork`, parent and child can
/// share one FUSE fh while using distinct lock_owners, so plock owners are a set.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct HandleLock {
    pub flock_owner_id: Option<u64>,
    /// Active POSIX lock_owners that acquired a lock through this handle.
    #[serde(default)]
    pub plock_owners: HashSet<u64>,
    /// Legacy single-owner field kept for state restore compatibility.
    #[serde(default)]
    plock_owner_id: Option<u64>,
}

impl HandleLock {
    /// Merge legacy `plock_owner_id` into `plock_owners` after deserialize.
    fn migrate_legacy_plock_owner(&mut self) {
        if let Some(owner) = self.plock_owner_id.take() {
            self.plock_owners.insert(owner);
        }
    }
}

pub struct BackendHandle {
    pub ino: u64,
    pub fh: u64,

    pub reader: Option<RawPtr<FuseReader>>,
    pub writer: Option<Arc<FuseWriter>>, // Writer uses Arc for global sharing
    /// Open-time file status snapshot. Guarded by a lock because the read path
    /// refreshes it in place after a dirty-read reopen (`read()` takes `&self`).
    status: std::sync::RwLock<FileStatus>,

    fh_locks: std::sync::Mutex<HandleLock>,

    read_ver: AtomicCounter,
}

impl BackendHandle {
    pub fn new(
        ino: u64,
        fh: u64,
        reader: Option<RawPtr<FuseReader>>,
        writer: Option<Arc<FuseWriter>>,
        status: FileStatus,
    ) -> Self {
        Self {
            ino,
            fh,
            reader,
            writer,
            status: std::sync::RwLock::new(status),
            fh_locks: std::sync::Mutex::new(HandleLock::default()),
            read_ver: AtomicCounter::new(0),
        }
    }

    /// Defensive upper bound for a single read/write request size.
    /// Same source as the `max_write` computed in `init`.
    fn max_io_size() -> u64 {
        FuseUtils::get_fuse_buf_size() as u64
    }

    /// Validate that a FUSE request offset (`u64`) fits in the signed `i64` the backend uses;
    /// an offset `> i64::MAX` would wrap to a negative position when cast `as i64`.
    fn check_offset(offset: u64) -> FuseResult<()> {
        if offset > i64::MAX as u64 {
            return err_fuse!(libc::EINVAL, "offset {} exceeds i64::MAX", offset);
        }
        Ok(())
    }

    /// Validate that write length fits in `fuse_write_out.size` without truncation.
    fn check_write_len(len: usize) -> FuseResult<()> {
        if len as u64 > u32::MAX as u64 {
            return err_fuse!(libc::EFBIG, "write len {} exceeds u32::MAX", len);
        }
        Ok(())
    }

    pub async fn read(
        &self,
        state: &NodeState,
        op: Read<'_>,
        reply: FuseResponse,
    ) -> FuseResult<()> {
        Self::check_offset(op.arg.offset)?;
        if op.arg.size as u64 > Self::max_io_size() {
            return err_fuse!(
                libc::EINVAL,
                "read size {} exceeds max {}",
                op.arg.size,
                Self::max_io_size()
            );
        }

        let reader = match &self.reader {
            Some(v) => v,
            None => return err_fuse!(libc::EIO),
        };

        if let Some(writer) = state.find_writer(self.ino).await {
            // `write_ver` advances only after a write/resize is successfully queued.
            // `enqueue_inflight` covers the brief window around send_queued_task so
            // dirty-read cannot publish past a Write that is mid-enqueue.
            // Concurrent fork writers (LTP ftest) can enqueue after we schedule a
            // flush; publish until the observed version is stable with no inflight
            // enqueues, then reopen once.
            //
            // Budget of 16: LTP fork-writer bursts typically stabilize within a
            // handful of flush rounds; sustained churn returns EAGAIN rather than
            // silently serving a stale view.
            const DIRTY_READ_FLUSH_BUDGET: usize = 16;
            if self.read_ver.get() != writer.write_ver() || writer.enqueue_inflight() != 0 {
                let mut published_ver = None;
                for _ in 0..DIRTY_READ_FLUSH_BUDGET {
                    if writer.enqueue_inflight() != 0 {
                        tokio::task::yield_now().await;
                    }
                    let ver_before = writer.write_ver();
                    if writer.enqueue_inflight() == 0 && self.read_ver.get() == ver_before {
                        published_ver = Some(ver_before);
                        break;
                    }
                    writer.flush(None).await?;
                    if writer.enqueue_inflight() == 0 && writer.write_ver() == ver_before {
                        published_ver = Some(ver_before);
                        break;
                    }
                }

                let Some(ver) = published_ver else {
                    warn!(
                        "dirty-read flush budget exhausted: ino={} write_ver={} read_ver={} inflight={}",
                        self.ino,
                        writer.write_ver(),
                        self.read_ver.get(),
                        writer.enqueue_inflight()
                    );
                    return err_fuse!(
                        libc::EAGAIN,
                        "dirty-read write_ver unstable after {} flushes",
                        DIRTY_READ_FLUSH_BUDGET
                    );
                };

                let path = reader.path().clone();
                let new_reader = state.new_reader(&path).await?;
                // Refresh status from the reopened reader before installing it.
                self.refresh_status(new_reader.status().clone());
                reader.replace(new_reader);
                self.read_ver.set(ver);
            }
        }

        reader.read(op, reply).await?;
        Ok(())
    }

    pub async fn write(&self, op: Write<'_>, reply: FuseResponse) -> FuseResult<()> {
        if op.data.is_empty() {
            // Zero-length writes must still reply so metrics finish normally.
            let res: FuseResult<fuse_write_out> = Ok(fuse_write_out {
                size: 0,
                padding: 0,
            });
            reply.send_rep(res).await?;
            return Ok(());
        }

        Self::check_offset(op.arg.offset)?;
        // The write reply reports the written length as `u32` (`fuse_write_out.size`);
        // reject anything that would truncate instead of silently under-reporting.
        Self::check_write_len(op.data.len())?;

        if let Some(writer) = &self.writer {
            writer
                .write(op.arg.offset as i64, op.data, Some(reply))
                .await?;
            Ok(())
        } else {
            err_fuse!(libc::EIO)
        }
    }

    pub async fn flush(&self, reply: Option<FuseResponse>) -> FuseResult<()> {
        if let Some(writer) = &self.writer {
            writer.flush(reply).await?;
        } else if let Some(reply) = reply {
            reply.send_rep(Ok::<(), FuseError>(())).await?;
        }
        Ok(())
    }

    pub async fn complete(&self, mut reply: Option<FuseResponse>) -> FuseResult<()> {
        if let Some(writer) = &self.writer {
            writer.complete(reply.take()).await?;
        }

        Ok(())
    }

    /// A clone of the current lock-guarded file status.
    pub fn status(&self) -> FileStatus {
        self.status.read().unwrap().clone()
    }

    /// Replace the open-time status snapshot after a dirty-read reopen.
    fn refresh_status(&self, status: FileStatus) {
        *self.status.write().unwrap() = status;
    }

    /// Record that `owner_id` holds a lock of `lock_flags` on this handle.
    pub fn add_lock(&self, lock_flags: LockFlags, owner_id: u64) {
        let mut fh_locks = self.fh_locks.lock().unwrap();
        fh_locks.migrate_legacy_plock_owner();

        match lock_flags {
            LockFlags::Plock => {
                fh_locks.plock_owners.insert(owner_id);
            }

            LockFlags::Flock => {
                fh_locks.flock_owner_id.get_or_insert(owner_id);
            }
        }
    }

    /// Remove and return one tracked owner for `typ`.
    ///
    /// For POSIX locks, prefer [`Self::take_plock_if_owner`] or
    /// [`Self::drain_plock_owners`] when the caller knows the owner set.
    pub fn remove_lock(&self, typ: LockFlags) -> Option<u64> {
        let mut fh_locks = self.fh_locks.lock().unwrap();
        fh_locks.migrate_legacy_plock_owner();

        match typ {
            LockFlags::Plock => {
                let owner = fh_locks.plock_owners.iter().copied().next()?;
                fh_locks.plock_owners.remove(&owner);
                Some(owner)
            }

            LockFlags::Flock => fh_locks.flock_owner_id.take(),
        }
    }

    /// Remove `owner_id` from the POSIX owner set when present.
    pub fn take_plock_if_owner(&self, owner_id: u64) -> Option<u64> {
        let mut fh_locks = self.fh_locks.lock().unwrap();
        fh_locks.migrate_legacy_plock_owner();
        if fh_locks.plock_owners.remove(&owner_id) {
            Some(owner_id)
        } else {
            None
        }
    }

    /// Drain every tracked POSIX lock_owner (used on handle release).
    pub fn drain_plock_owners(&self) -> Vec<u64> {
        let mut fh_locks = self.fh_locks.lock().unwrap();
        fh_locks.migrate_legacy_plock_owner();
        fh_locks.plock_owners.drain().collect()
    }

    pub async fn persist(&self, writer: &mut StateWriter) -> FuseResult<()> {
        self.complete(None).await?;

        writer.write_len(self.ino)?;
        writer.write_len(self.fh)?;
        writer.write_struct(&*self.status.read().unwrap())?;

        writer.write_struct(&self.writer.is_some())?;
        writer.write_struct(&self.reader.is_some())?;

        let mut locks = self.fh_locks.lock().unwrap();
        locks.migrate_legacy_plock_owner();
        writer.write_struct(&*locks)?;

        Ok(())
    }

    pub async fn restore(reader: &mut StateReader, state: &NodeState) -> FuseResult<Self> {
        let ino = reader.read_len()?;
        let fh = reader.read_len()?;
        let status: FileStatus = reader.read_struct()?;

        let has_writer: bool = reader.read_struct()?;
        let has_reader: bool = reader.read_struct()?;
        if !has_writer && !has_reader {
            return err_box!(
                "FileHandle has neither reader nor writer for ino={}, path={}",
                ino,
                status.path
            );
        }
        let mut locks: HandleLock = reader.read_struct()?;
        locks.migrate_legacy_plock_owner();

        let path = Path::from_str(&status.path)?;
        let writer = if has_writer {
            let opts = CreateFileOptsBuilder::with_conf(state.client_conf()).build();
            let writer = state
                .get_or_create_writer(ino, &path, OpenFlags::new_write_only(), opts)
                .await?;
            Some(writer)
        } else {
            None
        };

        let reader = if has_reader {
            let reader = state.new_reader(&path).await?;
            Some(RawPtr::from_owned(reader))
        } else {
            None
        };

        let handle = Self {
            ino,
            fh,
            reader,
            writer,
            status: std::sync::RwLock::new(status),

            fh_locks: std::sync::Mutex::new(locks),
            read_ver: AtomicCounter::new(0),
        };
        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::BackendHandle;

    // Reject offsets that would wrap negative when cast to i64.
    #[test]
    fn offset_over_i64_max_returns_einval() {
        assert!(BackendHandle::check_offset(0).is_ok());
        assert!(BackendHandle::check_offset(i64::MAX as u64).is_ok());

        let err = BackendHandle::check_offset(i64::MAX as u64 + 1)
            .expect_err("offset > i64::MAX must be rejected");
        assert_eq!(err.errno(), libc::EINVAL);

        let err =
            BackendHandle::check_offset(u64::MAX).expect_err("u64::MAX offset must be rejected");
        assert_eq!(err.errno(), libc::EINVAL);
    }

    // The defensive read/write size bound is the FUSE buffer size, matching the
    // `max_write` computed in `init`.
    #[test]
    fn max_io_size_matches_fuse_buf_size() {
        assert_eq!(
            BackendHandle::max_io_size(),
            crate::FuseUtils::get_fuse_buf_size() as u64
        );
    }

    // A write length that fits in the `u32` reply size passes; one that would
    // truncate (`> u32::MAX`) is rejected with EFBIG.
    #[test]
    fn write_len_over_u32_returns_efbig() {
        assert!(BackendHandle::check_write_len(0).is_ok());
        assert!(BackendHandle::check_write_len(u32::MAX as usize).is_ok());

        // usize is 64-bit on the supported targets, so u32::MAX + 1 is representable.
        let err = BackendHandle::check_write_len(u32::MAX as usize + 1)
            .expect_err("write len > u32::MAX must be rejected");
        assert_eq!(err.errno(), libc::EFBIG);
    }

    // After dirty-read reopen, `status()` must reflect the reopened file snapshot.
    #[test]
    fn refresh_status_updates_snapshot_through_shared_ref() {
        use curvine_common::state::FileStatus;

        let mut open_status = FileStatus::with_name(1, "f".to_string(), false);
        open_status.len = 100;
        open_status.mtime = 10;
        let handle = BackendHandle::new(1, 10, None, None, open_status);
        assert_eq!(handle.status().len, 100);
        assert_eq!(handle.status().mtime, 10);

        // Simulate the post-reopen refresh: a writer extended the file to 4096.
        let mut new_status = FileStatus::with_name(1, "f".to_string(), false);
        new_status.len = 4096;
        new_status.mtime = 20;
        // `handle` is an immutable binding — refresh_status takes `&self` and
        // mutates through the lock, exactly as the read path does.
        handle.refresh_status(new_status);

        assert_eq!(
            handle.status().len,
            4096,
            "status().len must reflect the reopened file, not the open-time 100"
        );
        assert_eq!(handle.status().mtime, 20);
    }

    #[test]
    fn plock_tracks_multiple_owners_on_shared_handle() {
        use curvine_common::state::{FileStatus, LockFlags};

        let handle = BackendHandle::new(
            1,
            10,
            None,
            None,
            FileStatus::with_name(1, "f".into(), false),
        );
        handle.add_lock(LockFlags::Plock, 11);
        handle.add_lock(LockFlags::Plock, 22);

        assert_eq!(handle.take_plock_if_owner(22), Some(22));
        assert_eq!(handle.take_plock_if_owner(22), None);
        assert_eq!(handle.take_plock_if_owner(11), Some(11));
        assert!(handle.drain_plock_owners().is_empty());
    }

    #[test]
    fn drain_plock_owners_returns_all_tracked_owners() {
        use curvine_common::state::{FileStatus, LockFlags};

        let handle = BackendHandle::new(
            1,
            10,
            None,
            None,
            FileStatus::with_name(1, "f".into(), false),
        );
        handle.add_lock(LockFlags::Plock, 7);
        handle.add_lock(LockFlags::Plock, 8);
        let mut owners = handle.drain_plock_owners();
        owners.sort_unstable();
        assert_eq!(owners, vec![7, 8]);
        assert!(handle.drain_plock_owners().is_empty());
    }

    #[test]
    fn legacy_single_plock_owner_migrates_into_set() {
        use super::HandleLock;
        use std::collections::HashSet;

        let mut locks = HandleLock {
            flock_owner_id: None,
            plock_owners: HashSet::new(),
            plock_owner_id: Some(99),
        };
        locks.migrate_legacy_plock_owner();
        assert!(locks.plock_owners.contains(&99));
        assert!(locks.plock_owner_id.is_none());
    }
}
