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
use crate::session::FuseResponse;
use crate::{err_fuse, FuseError, FuseResult};
use curvine_common::fs::{Path, StateReader, StateWriter};
use curvine_common::state::{CreateFileOptsBuilder, FileStatus, LockFlags, OpenFlags};
use orpc::err_box;
use orpc::sys::RawPtr;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Default, Serialize, Deserialize)]
struct HandleLock {
    flock_owner_id: Option<u64>,
    plock_owner_id: Option<u64>,
}

pub struct FileHandle {
    pub ino: u64,
    pub fh: u64,

    pub reader: Option<RawPtr<FuseReader>>,
    pub writer: Option<Arc<Mutex<FuseWriter>>>, // Writer uses Arc for global sharing
    pub status: FileStatus,

    fh_locks: std::sync::Mutex<HandleLock>,
}

impl FileHandle {
    pub fn new(
        ino: u64,
        fh: u64,
        reader: Option<RawPtr<FuseReader>>,
        writer: Option<Arc<Mutex<FuseWriter>>>,
        status: FileStatus,
    ) -> Self {
        Self {
            ino,
            fh,
            reader,
            writer,
            status,
            fh_locks: std::sync::Mutex::new(HandleLock::default()),
        }
    }

    pub async fn read(
        &self,
        state: &NodeState,
        op: Read<'_>,
        reply: FuseResponse,
    ) -> FuseResult<()> {
        let reader = match &self.reader {
            Some(v) => v,
            None => return err_fuse!(libc::EIO),
        };

        if op.arg.offset as i64 >= reader.len() {
            if let Some(writer) = state.find_writer(&op.header.nodeid) {
                {
                    writer.lock().await.flush(None).await?;
                }
                // TODO: Optimize by adding refresh interface to refresh block list
                let path = reader.path().clone();
                reader.as_mut().complete(None).await?;
                let new_reader = state.new_reader(&path).await?;
                reader.replace(new_reader);
            }
        }

        reader.as_mut().read(op, reply).await?;
        Ok(())
    }

    pub async fn write(&self, op: Write<'_>, reply: FuseResponse) -> FuseResult<()> {
        if op.data.is_empty() {
            return Ok(());
        }

        let lock = if let Some(lock) = &self.writer {
            lock
        } else {
            return err_fuse!(libc::EIO);
        };

        let mut writer = lock.lock().await;
        writer.write(op, reply).await?;
        Ok(())
    }

    pub async fn flush(&self, reply: Option<FuseResponse>) -> FuseResult<()> {
        if let Some(writer) = &self.writer {
            writer.lock().await.flush(reply).await?;
        } else if let Some(reply) = reply {
            reply.send_rep(Ok::<(), FuseError>(())).await?;
        }
        Ok(())
    }

    pub async fn complete(&self, mut reply: Option<FuseResponse>) -> FuseResult<()> {
        if let Some(writer) = &self.writer {
            if Arc::strong_count(writer) <= 1 {
                writer.lock().await.complete(reply.take()).await?;
            } else {
                writer.lock().await.flush(reply.take()).await?;
            }
        }
        if let Some(reader) = &self.reader {
            reader.as_mut().complete(reply.take()).await?;
        }
        Ok(())
    }

    pub fn status(&self) -> &FileStatus {
        &self.status
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
        writer.write_struct(&self.status)?;

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
                .new_writer(ino, &path, OpenFlags::new_write_only(), opts)
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
            status,
            fh_locks: std::sync::Mutex::new(locks),
        };
        Ok(handle)
    }
}
