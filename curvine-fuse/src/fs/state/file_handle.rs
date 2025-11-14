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
use curvine_common::state::FileStatus;
use orpc::sys::RawPtr;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct FileHandle {
    pub ino: u64,
    pub fh: u64,

    pub locks: u8,       // Lock status flags
    pub lock_owner: u64, // Owner ID of flock
    pub ofd_owner: u64,  // Owner ID of OFD lock

    pub reader: Option<RawPtr<FuseReader>>,
    pub writer: Option<Arc<Mutex<FuseWriter>>>, // Writer uses Arc for global sharing
    pub status: FileStatus,
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
            locks: 0,
            lock_owner: 0,
            ofd_owner: 0,
            reader,
            writer,
            status,
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

    pub async fn flush(&self, reply: FuseResponse) -> FuseResult<()> {
        if let Some(writer) = &self.writer {
            writer.lock().await.flush(Some(reply)).await?;
        } else {
            reply.send_rep(Ok::<(), FuseError>(())).await?;
        }
        Ok(())
    }

    pub async fn complete(&self, reply: FuseResponse) -> FuseResult<()> {
        let mut reply = Some(reply);
        if let Some(writer) = &self.writer {
            writer.lock().await.complete(reply.take()).await?;
        }
        if let Some(reader) = &self.reader {
            reader.as_mut().complete(reply.take()).await?;
        }
        Ok(())
    }

    pub fn status(&self) -> &FileStatus {
        &self.status
    }
}
