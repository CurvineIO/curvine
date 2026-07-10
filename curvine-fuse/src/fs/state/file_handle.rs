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
use crate::fs::state::backend_handle::BackendHandle;
use crate::fs::state::NodeState;
use crate::fs::{FuseReader, FuseWriter};
use crate::session::FuseResponse;
use crate::{err_fuse, FuseResult};
use curvine_common::fs::{StateReader, StateWriter};
use curvine_common::state::{FileAllocOpts, FileStatus, LockFlags};
use orpc::err_box;
use orpc::sys::RawPtr;
use std::sync::Arc;

pub enum FileHandle {
    Backend(BackendHandle),
}

impl FileHandle {
    pub const TYPE_BACKEND: u8 = 0;

    pub fn new_backend(
        ino: u64,
        fh: u64,
        reader: Option<RawPtr<FuseReader>>,
        writer: Option<Arc<FuseWriter>>,
        status: FileStatus,
    ) -> Self {
        Self::Backend(BackendHandle::new(ino, fh, reader, writer, status))
    }

    pub fn ino(&self) -> u64 {
        match self {
            FileHandle::Backend(h) => h.ino,
        }
    }

    pub fn fh(&self) -> u64 {
        match self {
            FileHandle::Backend(h) => h.fh,
        }
    }

    pub fn status(&self) -> &FileStatus {
        match self {
            FileHandle::Backend(h) => &h.status,
        }
    }

    /// True when this handle can satisfy write-related side effects (backend writer).
    pub fn has_writer(&self) -> bool {
        match self {
            FileHandle::Backend(h) => h.writer.is_some(),
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
        }
    }

    pub async fn write(&self, op: Write<'_>, reply: FuseResponse) -> FuseResult<()> {
        match self {
            FileHandle::Backend(h) => h.write(op, reply).await,
        }
    }

    pub async fn flush(&self, reply: Option<FuseResponse>) -> FuseResult<()> {
        match self {
            FileHandle::Backend(h) => h.flush(reply).await,
        }
    }

    pub async fn complete(&self, reply: Option<FuseResponse>) -> FuseResult<()> {
        match self {
            FileHandle::Backend(h) => h.complete(reply).await,
        }
    }

    pub fn add_lock(&self, lock_flags: LockFlags, owner_id: u64) {
        match self {
            FileHandle::Backend(h) => h.add_lock(lock_flags, owner_id),
        }
    }

    pub fn remove_lock(&self, typ: LockFlags) -> Option<u64> {
        match self {
            FileHandle::Backend(h) => h.remove_lock(typ),
        }
    }

    pub async fn resize(&self, opts: FileAllocOpts) -> FuseResult<()> {
        match self {
            FileHandle::Backend(h) => {
                if let Some(writer) = &h.writer {
                    writer.resize(opts).await?;
                    Ok(())
                } else {
                    err_fuse!(libc::EACCES)
                }
            }
        }
    }

    pub async fn persist(&self, writer: &mut StateWriter) -> FuseResult<()> {
        match self {
            FileHandle::Backend(h) => {
                writer.write_struct(&Self::TYPE_BACKEND)?;
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

            _ => err_box!("unknown FileHandle persist tag {}", tag),
        }
    }
}
