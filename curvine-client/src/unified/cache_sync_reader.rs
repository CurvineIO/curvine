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

use crate::file::{CurvineFileSystem, FsReader};
use curvine_common::fs::{Path, Reader};
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use log::info;
use orpc::sys::DataSlice;
use std::time::Duration;
use tokio::time;
use tokio::time::timeout;

pub struct CacheSyncReader {
    fs: CurvineFileSystem,
    inner: FsReader,
    check_interval_min: Duration,
    check_interval_max: Duration,
    log_ticks: u32,
    max_wait: Duration,
}

impl CacheSyncReader {
    pub async fn new(fs: &CurvineFileSystem, path: &Path) -> FsResult<Self> {
        let inner = fs.open(path).await?;
        let conf = &fs.conf().client;
        Ok(CacheSyncReader {
            fs: fs.clone(),
            inner,
            check_interval_min: conf.sync_check_interval_min,
            check_interval_max: conf.sync_check_interval_max,
            log_ticks: conf.sync_check_log_tick,
            max_wait: conf.max_sync_wait_timeout,
        })
    }

    pub async fn read_check_complete(&mut self) -> FsResult<DataSlice> {
        if !self.has_remaining() && !self.status().is_complete {
            let mut ticks = 0;
            loop {
                ticks += 1;
                let file_blocks = self.fs.get_block_locations(self.path()).await?;

                if file_blocks.len != self.len() || file_blocks.status.is_complete {
                    let mut reader = FsReader::new(
                        self.path().clone(),
                        self.fs.fs_context.clone(),
                        file_blocks,
                    )?;
                    reader.seek(self.inner.pos()).await?;

                    info!(
                        "file {} len change {} -> {}, complete: {}",
                        self.path(),
                        self.len(),
                        reader.len(),
                        reader.status().is_complete
                    );
                    self.inner = reader;
                    break;
                } else {
                    let sleep_time = self.check_interval_max.min(self.check_interval_min * ticks);
                    time::sleep(sleep_time).await;

                    if ticks % self.log_ticks == 0 {
                        info!("wait file {} complete", self.inner.path())
                    }
                }
            }
        }

        self.inner.read_chunk0().await
    }
}

impl Reader for CacheSyncReader {
    fn status(&self) -> &FileStatus {
        self.inner.status()
    }

    fn path(&self) -> &Path {
        self.inner.path()
    }

    fn len(&self) -> i64 {
        self.inner.len()
    }

    fn chunk_mut(&mut self) -> &mut DataSlice {
        self.inner.chunk_mut()
    }

    fn chunk_size(&self) -> usize {
        self.inner.chunk_size()
    }

    fn pos(&self) -> i64 {
        self.inner.pos()
    }

    fn pos_mut(&mut self) -> &mut i64 {
        self.inner.pos_mut()
    }

    async fn read_chunk0(&mut self) -> FsResult<DataSlice> {
        timeout(self.max_wait, self.read_check_complete()).await?
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        self.inner.seek(pos).await
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.inner.complete().await
    }
}
