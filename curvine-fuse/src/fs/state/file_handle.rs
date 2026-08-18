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
use crate::fs::pcache::InodeCache;
use crate::fs::state::backend_handle::BackendHandle;
use crate::fs::state::cache_handle::CacheHandle;
use crate::fs::state::NodeState;
use crate::fs::{FuseReader, FuseWriter};
use crate::session::FuseResponse;
use crate::{err_fuse, FuseResult};
use curvine_core_error::err_box;
use curvine_fs_api::{StateReader, StateWriter};
use curvine_model::{CreateFileOpts, FileAllocOpts, FileStatus, LockFlags, OpenFlags};
use std::sync::Arc;

#[allow(clippy::large_enum_variant)]
pub enum FileHandle {
    Backend(BackendHandle),
    Cache(CacheHandle),
}

impl FileHandle {
    pub const TYPE_BACKEND: u8 = 0;
    pub const TYPE_CACHE: u8 = 1;

    pub fn new_backend(
        ino: u64,
        fh: u64,
        reader: Option<Arc<FuseReader>>,
        writer: Option<Arc<FuseWriter>>,
        status: FileStatus,
    ) -> Self {
        Self::Backend(BackendHandle::new(ino, fh, reader, writer, status))
    }

    pub fn new_cache(
        ino: u64,
        fh: u64,
        cache: Arc<InodeCache>,
        status: FileStatus,
        flags: OpenFlags,
        opts: CreateFileOpts,
    ) -> Self {
        Self::Cache(CacheHandle::new(ino, fh, cache, status, flags, opts))
    }

    pub fn ino(&self) -> u64 {
        match self {
            FileHandle::Backend(h) => h.ino,
            FileHandle::Cache(h) => h.ino,
        }
    }

    pub fn fh(&self) -> u64 {
        match self {
            FileHandle::Backend(h) => h.fh,
            FileHandle::Cache(h) => h.fh,
        }
    }

    pub fn status(&self) -> FileStatus {
        match self {
            FileHandle::Backend(h) => h.status(),
            FileHandle::Cache(h) => h.status().clone(),
        }
    }

    /// True when this handle can satisfy write-related side effects (backend writer or cache path).
    pub fn has_writer(&self) -> bool {
        match self {
            FileHandle::Backend(h) => h.writer.is_some(),
            FileHandle::Cache(h) => h.flags.write(),
        }
    }

    pub fn as_cache(&self) -> FuseResult<&CacheHandle> {
        match self {
            FileHandle::Cache(h) => Ok(h),
            _ => err_box!("not a cache handle"),
        }
    }

    pub async fn read(
        &self,
        state: &NodeState,
        op: Read<'_>,
        reply: FuseResponse,
    ) -> FuseResult<()> {
        match self {
            FileHandle::Backend(h) => h.read(state, op, reply).await,
            FileHandle::Cache(h) => h.read(state, op, reply).await,
        }
    }

    pub async fn write(&self, op: Write<'_>, reply: FuseResponse) -> FuseResult<()> {
        match self {
            FileHandle::Backend(h) => h.write(op, reply).await,
            FileHandle::Cache(h) => h.write(op, reply).await,
        }
    }

    pub async fn flush(&self, reply: Option<FuseResponse>) -> FuseResult<()> {
        match self {
            FileHandle::Backend(h) => h.flush(reply).await,
            FileHandle::Cache(h) => h.flush(reply).await,
        }
    }

    pub async fn complete(&self, reply: Option<FuseResponse>) -> FuseResult<()> {
        match self {
            FileHandle::Backend(h) => h.complete(reply).await,
            FileHandle::Cache(h) => h.complete(reply).await,
        }
    }

    pub fn add_lock(&self, lock_flags: LockFlags, owner_id: u64) {
        match self {
            FileHandle::Backend(h) => h.add_lock(lock_flags, owner_id),
            FileHandle::Cache(h) => h.add_lock(lock_flags, owner_id),
        }
    }

    pub fn remove_lock(&self, typ: LockFlags) -> Option<u64> {
        match self {
            FileHandle::Backend(h) => h.remove_lock(typ),
            FileHandle::Cache(h) => h.remove_lock(typ),
        }
    }

    pub fn take_plock_if_owner(&self, owner_id: u64) -> Option<u64> {
        match self {
            FileHandle::Backend(h) => h.take_plock_if_owner(owner_id),
            FileHandle::Cache(h) => h.take_plock_if_owner(owner_id),
        }
    }

    pub fn drain_plock_owners(&self) -> Vec<u64> {
        match self {
            FileHandle::Backend(h) => h.drain_plock_owners(),
            FileHandle::Cache(h) => h.drain_plock_owners(),
        }
    }

    pub async fn resize(&self, opts: FileAllocOpts) -> FuseResult<FileStatus> {
        match self {
            FileHandle::Backend(h) => {
                if let Some(writer) = &h.writer {
                    Ok(writer.resize(opts).await?)
                } else {
                    err_fuse!(libc::EACCES)
                }
            }

            FileHandle::Cache(h) => h.resize(opts).await,
        }
    }

    pub async fn persist(&self, writer: &mut StateWriter) -> FuseResult<()> {
        match self {
            FileHandle::Backend(h) => {
                writer.write_struct(&Self::TYPE_BACKEND)?;
                h.persist(writer).await
            }
            FileHandle::Cache(h) => {
                writer.write_struct(&Self::TYPE_CACHE)?;
                h.persist(writer).await
            }
        }
    }

    pub async fn restore(reader: &mut StateReader, state: &NodeState) -> FuseResult<Self> {
        let tag: u8 = reader.read_struct()?;
        match tag {
            Self::TYPE_BACKEND => Ok(FileHandle::Backend(
                BackendHandle::restore(reader, state).await?,
            )),

            Self::TYPE_CACHE => Ok(FileHandle::Cache(
                CacheHandle::restore(reader, state).await?,
            )),

            _ => err_box!("unknown FileHandle persist tag {}", tag),
        }
    }
}
