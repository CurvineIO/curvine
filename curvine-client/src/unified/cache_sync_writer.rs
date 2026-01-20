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
use curvine_common::state::{FileStatus, LoadJobResult, OpenFlags, WriteType};
use curvine_common::FsResult;
use orpc::err_box;
use orpc::sys::DataSlice;

use crate::file::FsWriter;
use crate::rpc::JobMasterClient;
use crate::unified::{MountValue, UnifiedFileSystem};

pub struct CacheSyncWriter {
    job_client: JobMasterClient,
    inner: FsWriter,
    write_type: WriteType,
    job_res: LoadJobResult,
    has_rand_write: bool,
}

impl CacheSyncWriter {
    pub async fn new(
        fs: &UnifiedFileSystem,
        cv_path: &Path,
        mnt: &MountValue,
        flags: OpenFlags,
    ) -> FsResult<Self> {
        let write_type = mnt.info.write_type;
        if !matches!(
            write_type,
            WriteType::AsyncThrough | WriteType::CacheThrough
        ) {
            return err_box!("write type must be either AsyncThrough or CacheThrough");
        }

        let conf = &fs.conf().client;
        let opts = mnt.info.get_create_opts(conf);
        let inner = fs.cv().open_with_opts(cv_path, opts, flags).await?;

        let job_client = JobMasterClient::with_context(fs.fs_context());
        let job_res = job_client.submit_load(cv_path.clone_uri()).await?;
        info!(
            "submit(init) job successfully for {}, job id {}, target_path {}",
            cv_path, job_res.job_id, job_res.target_path
        );

        let writer = Self {
            job_client,
            inner,
            write_type,
            job_res,
            has_rand_write: false,
        };
        Ok(writer)
    }

    pub async fn wait_job_complete(&self) -> FsResult<()> {
        self.job_client
            .wait_job_complete(&self.job_res.job_id, "cache-sync")
            .await
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
        self.inner.write_chunk(chunk).await
    }

    async fn flush(&mut self) -> FsResult<()> {
        self.inner.flush().await
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.inner.complete().await?;

        if self.has_rand_write {
            let job_res = self.job_client.submit_load(self.path().clone_uri()).await?;
            self.job_res = job_res;
            self.has_rand_write = false;
            info!(
                "resubmit(rand_write) job successfully for {}, job id {}, target_path {}",
                self.path(),
                self.job_res.job_id,
                self.job_res.target_path
            );
        }

        if matches!(self.write_type, WriteType::CacheThrough) {
            self.wait_job_complete().await?;
        }

        Ok(())
    }

    async fn cancel(&mut self) -> FsResult<()> {
        self.inner.cancel().await
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if self.pos() != pos && !self.has_rand_write {
            self.has_rand_write = true;
            if let Err(e) = self.job_client.cancel_job(&self.job_res.job_id).await {
                warn!("cancel job {} failed: {}", self.job_res.job_id, e);
            } else {
                info!(
                    "cancel(rand_write) job {} successfully",
                    self.job_res.job_id
                );
            }
        }

        self.inner.seek(pos).await
    }
}
