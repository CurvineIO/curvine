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
use orpc::err_box;
use orpc::sync::AtomicCounter;
use orpc::sys::RawPtr;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct HandleLock {
    pub flock_owner_id: Option<u64>,
    pub plock_owner_id: Option<u64>,
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

    /// Defensive upper bound for a single read/write request size. FUSE `size`
    /// is a `u32` from the kernel and is normally already capped by the
    /// `max_write`/`max_readahead` negotiated at init, but userspace still
    /// validates it so an abnormal request cannot drive an oversized backend IO.
    /// Same source as the `max_write` computed in `init`.
    fn max_io_size() -> u64 {
        FuseUtils::get_fuse_buf_size() as u64
    }

    /// Validate that a FUSE request offset (`u64`) fits in the signed `i64` the
    /// backend uses; an offset `> i64::MAX` would wrap to a negative position
    /// when cast `as i64`. Returns `EINVAL` instead of letting it reach the
    /// backend.
    fn check_offset(offset: u64) -> FuseResult<()> {
        if offset > i64::MAX as u64 {
            return err_fuse!(libc::EINVAL, "offset {} exceeds i64::MAX", offset);
        }
        Ok(())
    }

    /// Validate that a write length fits in the `u32` reported back to the kernel
    /// (`fuse_write_out.size`); a larger length would truncate and silently
    /// under-report the written byte count, so reject it with `EFBIG`.
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
            let writer_ver = writer.write_ver();
            if self.read_ver.get() != writer_ver {
                writer.flush(None).await?;

                let path = reader.path().clone();
                let new_reader = state.new_reader(&path).await?;
                // Refresh the handle's status snapshot from the freshly reopened
                // reader before installing it, so `status()` reflects the current
                // file (length/mtime) after a dirty-read reopen rather than the
                // stale open-time snapshot.
                self.refresh_status(new_reader.status().clone());
                reader.replace(new_reader);

                self.read_ver.set(writer_ver);
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

    /// A clone of the current file status. Returns an owned value (not a
    /// reference) because the status is lock-guarded: the read path can refresh
    /// it in place after a dirty-read reopen.
    pub fn status(&self) -> FileStatus {
        self.status.read().unwrap().clone()
    }

    /// Replace the open-time status snapshot with a fresh one. Called from the
    /// read path after a dirty-read reopen so `status()` no longer reports the
    /// stale open-time length/mtime.
    fn refresh_status(&self, status: FileStatus) {
        *self.status.write().unwrap() = status;
    }

    // Add lock, only save the owner_id of the first lock
    pub fn add_lock(&self, lock_flags: LockFlags, owner_id: u64) {
        let mut fh_locks = self.fh_locks.lock().unwrap();

        match lock_flags {
            LockFlags::Plock => {
                fh_locks.plock_owner_id.get_or_insert(owner_id);
            }

            LockFlags::Flock => {
                fh_locks.flock_owner_id.get_or_insert(owner_id);
            }
        }
    }

    // Remove lock, return owner_id
    pub fn remove_lock(&self, typ: LockFlags) -> Option<u64> {
        let mut fh_locks = self.fh_locks.lock().unwrap();

        match typ {
            LockFlags::Plock => fh_locks.plock_owner_id.take(),

            LockFlags::Flock => fh_locks.flock_owner_id.take(),
        }
    }

    pub async fn persist(&self, writer: &mut StateWriter) -> FuseResult<()> {
        self.complete(None).await?;

        writer.write_len(self.ino)?;
        writer.write_len(self.fh)?;
        writer.write_struct(&*self.status.read().unwrap())?;

        writer.write_struct(&self.writer.is_some())?;
        writer.write_struct(&self.reader.is_some())?;

        let locks = self.fh_locks.lock().unwrap();
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
        let locks: HandleLock = reader.read_struct()?;

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

    // An offset within i64 range passes; an offset > i64::MAX (which would wrap
    // negative when cast `as i64`) is rejected with EINVAL before it can reach
    // the backend.
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

    // After a dirty-read reopen the read path calls `refresh_status` (through a
    // shared `&self`); `status()` must then reflect the reopened file's
    // length/mtime, not the stale open-time snapshot.
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
}
