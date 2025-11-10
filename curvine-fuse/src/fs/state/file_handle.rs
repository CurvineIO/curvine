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
use crate::{err_fuse, FuseResult, FUSE_SUCCESS};
use curvine_client::unified::{UnifiedReader, UnifiedWriter};
use curvine_common::fs::{Reader, Writer};
use curvine_common::state::FileStatus;
use orpc::sys::{DataSlice, RawPtr};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct FileHandle {
    pub ino: u64,
    pub fh: u64,

    pub locks: u8,       // Lock status flags
    pub lock_owner: u64, // Owner ID of flock
    pub ofd_owner: u64,  // Owner ID of OFD lock

    pub reader: Option<RawPtr<UnifiedReader>>,
    pub writer: Option<Arc<Mutex<UnifiedWriter>>>, // Writer uses Arc for global sharing
}

impl FileHandle {
    pub fn new(
        ino: u64,
        fh: u64,
        reader: Option<RawPtr<UnifiedReader>>,
        writer: Option<Arc<Mutex<UnifiedWriter>>>,
    ) -> Self {
        Self {
            ino,
            fh,
            locks: 0,
            lock_owner: 0,
            ofd_owner: 0,
            reader,
            writer,
        }
    }

    pub async fn read(&self, state: &NodeState, op: Read<'_>) -> FuseResult<Vec<DataSlice>> {
        let reader = match &self.reader {
            Some(v) => v,
            None => return err_fuse!(libc::EIO),
        };

        if !reader.has_remaining() {
            if let Some(writer) = state.find_writer(&op.header.nodeid) {
                {
                    writer.lock().await.flush().await?;
                }
                // TODO: Optimize by adding refresh interface to refresh block list
                let path = reader.path().clone();
                reader.as_mut().complete().await?;
                let new_reader = state.new_reader(&path).await?;
                reader.replace(new_reader);
            }
        }

        let data = reader
            .as_mut()
            .fuse_read(op.arg.offset as i64, op.arg.size as usize)
            .await?;

        Ok(data)
    }

    pub async fn write(&self, op: Write<'_>) -> FuseResult<()> {
        let off = op.arg.offset;
        let len = op.data.len();

        let lock = if let Some(lock) = &self.writer {
            lock
        } else {
            return err_fuse!(libc::EIO);
        };

        let mut writer = lock.lock().await;

        // Only skip true zero-length writes
        if len == 0 {
            return err_fuse!(
                FUSE_SUCCESS,
                "Skip zero-length write to file {} offset={} size={}",
                writer.path(),
                off,
                len
            );
        }

        writer.seek(off as i64).await?;
        writer.fuse_write(DataSlice::bytes(op.data)).await?;
        Ok(())
    }

    pub async fn flush(&self) -> FuseResult<()> {
        if let Some(writer) = &self.writer {
            writer.lock().await.flush().await?;
        }
        Ok(())
    }

    pub async fn complete(&self) -> FuseResult<()> {
        if let Some(writer) = &self.writer {
            writer.lock().await.complete().await?;
        }
        if let Some(reader) = &self.reader {
            reader.as_mut().complete().await?;
        }
        Ok(())
    }

    pub async fn status(&self) -> FuseResult<FileStatus> {
        if let Some(writer) = &self.writer {
            return Ok(writer.lock().await.status().clone());
        } else {
            err_fuse!(libc::EIO)
        }
    }
}
