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

use bytes::BytesMut;
use log::info;
use tracing::warn;

use curvine_common::fs::{Path, Writer};
use curvine_common::state::{FileAllocOpts, FileStatus, LoadJobResult, OpenFlags, WriteType};
use curvine_common::FsResult;
use orpc::sys::DataSlice;

use crate::file::FsWriter;
use crate::rpc::JobMasterClient;
use crate::unified::{MountValue, UnifiedFileSystem};

pub struct CacheSyncWriter {
    job_client: JobMasterClient,
    inner: FsWriter,
    write_type: WriteType,
    job_res: Option<LoadJobResult>,
    /// Whether seek or resize operations have been performed
    has_random_access: bool,
}

impl CacheSyncWriter {
    pub async fn new(
        fs: &UnifiedFileSystem,
        cv_path: &Path,
        mnt: &MountValue,
        flags: OpenFlags,
    ) -> FsResult<Self> {
        let write_type = mnt.info.write_type;

        let conf = &fs.conf().client;
        let opts = mnt.info.get_create_opts(conf);
        let inner = fs.cv().open_with_opts(cv_path, opts, flags).await?;
        let job_client = JobMasterClient::with_context(fs.fs_context());

        let writer = Self {
            job_client,
            inner,
            write_type,
            job_res: None,
            has_random_access: false,
        };
        Ok(writer)
    }

    pub async fn wait_job_complete(&self) -> FsResult<()> {
        if let Some(job_res) = &self.job_res {
            self.job_client
                .wait_job_complete(&job_res.job_id, true)
                .await
        } else {
            Ok(())
        }
    }
}

impl Writer for CacheSyncWriter {
    fn status(&self) -> &FileStatus {
        self.inner.status()
    }

    fn path(&self) -> &Path {
        self.inner.path()
    }

    fn pos(&self) -> i64 {
        self.inner.pos()
    }

    fn pos_mut(&mut self) -> &mut i64 {
        self.inner.pos_mut()
    }

    fn chunk_mut(&mut self) -> &mut BytesMut {
        self.inner.chunk_mut()
    }

    fn chunk_size(&self) -> usize {
        self.inner.chunk_size()
    }

    async fn write_chunk(&mut self, chunk: DataSlice) -> FsResult<i64> {
        if self.job_res.is_none() && !self.has_random_access {
            let job_res = self
                .job_client
                .submit_load(self.inner.path().clone_uri())
                .await?;
            info!(
                "submit(init) job successfully for {}, job id {}, target_path {}",
                self.inner.path(),
                job_res.job_id,
                job_res.target_path
            );

            self.job_res.replace(job_res);
        }
        self.inner.write_chunk(chunk).await
    }

    async fn flush(&mut self) -> FsResult<()> {
        self.inner.flush().await
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.inner.complete().await?;

        if self.job_res.is_none() {
            let job_res = self.job_client.submit_load(self.path().clone_uri()).await?;
            info!(
                "resubmit job successfully for {}, job id {}, target_path {}",
                self.path(),
                job_res.job_id,
                job_res.target_path
            );

            self.job_res.replace(job_res);
        }

        if matches!(self.write_type, WriteType::FsMode) {
            self.wait_job_complete().await?;
        }

        Ok(())
    }

    async fn cancel(&mut self) -> FsResult<()> {
        self.inner.cancel().await
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if self.pos() != pos {
            if let Some(job_res) = &self.job_res {
                if let Err(e) = self.job_client.cancel_job(&job_res.job_id).await {
                    warn!("cancel job {} failed: {}", job_res.job_id, e);
                } else {
                    info!("cancel(rand_write) job {} successfully", job_res.job_id);
                }
            }

            self.job_res.take();
            self.has_random_access = true;
        }

        self.inner.seek(pos).await
    }

    async fn resize(&mut self, opts: FileAllocOpts) -> FsResult<()> {
        if let Some(job_res) = &self.job_res {
            if let Err(e) = self.job_client.cancel_job(&job_res.job_id).await {
                warn!("cancel job {} failed: {}", job_res.job_id, e);
            } else {
                info!("cancel(resize) job {} successfully", job_res.job_id);
            }
            self.job_res.take();
        }
        self.has_random_access = true;

        self.inner.resize(opts).await
    }
}
