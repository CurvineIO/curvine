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
use curvine_common::state::{
    FileAllocOpts, FileStatus, LoadJobCommand, LoadJobResult, OpenFlags, SetAttrOptsBuilder,
    SyncLifecycleMarker, SyncLifecycleOwner, SyncLifecycleState, WriteType,
};
use curvine_common::utils::CommonUtils;
use curvine_common::FsResult;
use orpc::err_box;
use orpc::sys::DataSlice;

use crate::file::{CurvineFileSystem, FsWriter};
use crate::rpc::JobMasterClient;
use crate::unified::{MountValue, UnifiedFileSystem};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SourceSnapshot {
    id: i64,
    len: i64,
    mtime: i64,
}

impl SourceSnapshot {
    fn from_status(status: &FileStatus) -> Self {
        Self {
            id: status.id,
            len: status.len,
            mtime: status.mtime,
        }
    }
}

pub struct CacheSyncWriter {
    job_client: JobMasterClient,
    cv: CurvineFileSystem,
    inner: FsWriter,
    write_type: WriteType,
    job_res: Option<LoadJobResult>,
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

        let writer = Self {
            job_client,
            cv: fs.cv().clone(),
            inner,
            write_type,
            job_res: None,
        };
        Ok(writer)
    }

    pub async fn wait_job_complete(&self) -> FsResult<()> {
        if let Some(job_res) = &self.job_res {
            self.job_client
                .wait_job_complete(&job_res.job_id, "cache-sync")
                .await
        } else {
            Ok(())
        }
    }

    async fn latest_source_snapshot(&self) -> FsResult<SourceSnapshot> {
        let status = self.cv.get_status(self.inner.path()).await?;
        Ok(SourceSnapshot::from_status(&status))
    }

    async fn submit_publish_with_snapshot(
        &mut self,
        mark: &str,
        snapshot: SourceSnapshot,
    ) -> FsResult<()> {
        let source_generation =
            CommonUtils::source_generation(snapshot.id, snapshot.len, snapshot.mtime);
        let expected_job_id = CommonUtils::create_job_id_with_generation(
            self.inner.path().full_path(),
            &source_generation,
        );
        let pending_marker = SyncLifecycleMarker::with_owner(
            SyncLifecycleState::Pending,
            source_generation.clone(),
            expected_job_id.clone(),
            SyncLifecycleOwner::Publish,
        );
        self.set_sync_marker(&pending_marker, None).await?;
        let job_res = match self
            .job_client
            .submit_load_job(
                LoadJobCommand::publish_builder(self.inner.path().clone_uri())
                    .source_generation(source_generation.clone())
                    .expected_source_id(snapshot.id)
                    .expected_source_len(snapshot.len)
                    .expected_source_mtime(snapshot.mtime)
                    .build(),
            )
            .await
        {
            Ok(res) => res,
            Err(e) => {
                let aborted_marker = SyncLifecycleMarker::with_owner(
                    SyncLifecycleState::Aborted,
                    source_generation.clone(),
                    expected_job_id.clone(),
                    SyncLifecycleOwner::Publish,
                );
                if let Err(mark_err) = self
                    .set_sync_marker(&aborted_marker, Some(&pending_marker))
                    .await
                {
                    warn!(
                        "mark aborted sync state failed for {}: {}",
                        self.inner.path(),
                        mark_err
                    );
                }
                return Err(e);
            }
        };
        if job_res.job_id != expected_job_id {
            let marker = SyncLifecycleMarker::with_owner(
                SyncLifecycleState::Pending,
                source_generation.clone(),
                job_res.job_id.clone(),
                SyncLifecycleOwner::Publish,
            );
            self.set_sync_marker(&marker, Some(&pending_marker)).await?;
        }
        info!(
            "submit({}) job for {}, job id {}, target_path {}, generation={}, source(id={},len={},mtime={})",
            mark,
            self.inner.path(),
            job_res.job_id,
            job_res.target_path,
            source_generation,
            snapshot.id,
            snapshot.len,
            snapshot.mtime
        );

        self.job_res.replace(job_res);
        Ok(())
    }

    async fn set_sync_marker(
        &self,
        marker: &SyncLifecycleMarker,
        expect: Option<&SyncLifecycleMarker>,
    ) -> FsResult<()> {
        let mut builder = SetAttrOptsBuilder::new();
        for (k, v) in marker.as_expect_map() {
            builder = builder.add_x_attr(k, v);
        }
        if let Some(expect_marker) = expect {
            for (k, v) in expect_marker.as_expect_map() {
                builder = builder.expect_x_attr(k, v);
            }
        }
        let _ = self.cv.set_attr(self.inner.path(), builder.build()).await?;
        Ok(())
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

        if self.job_res.is_none() {
            let current_snapshot = self.latest_source_snapshot().await?;
            self.submit_publish_with_snapshot("complete", current_snapshot)
                .await?;
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
        if self.pos() != pos {
            if let Some(job_res) = &self.job_res {
                if let Err(e) = self.job_client.cancel_job(&job_res.job_id).await {
                    warn!("cancel job {} failed: {}", job_res.job_id, e);
                } else {
                    info!("cancel(rand_write) job {} successfully", job_res.job_id);
                }
            }

            self.job_res.take();
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

        self.inner.resize(opts).await
    }
}
