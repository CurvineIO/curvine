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

use crate::{UfsFileSystem, UfsWriter, UnifiedWriter};
use bytes::{Bytes, BytesMut};
use curvine_client_core::file::{CurvineFileSystem, FsWriter};
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{FileSystem, Path, Writer};
use curvine_io::DataSlice;
use curvine_model::{CreateFileOpts, FileAllocOpts, FileStatus, OpenFlags, SetAttrOpts};
use curvine_runtime::runtime::RpcRuntime;
use dashmap::DashSet;
use log::{debug, warn};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time;

const WRITE_CACHE_COMPLETE_TIMEOUT: Duration = Duration::from_secs(30);

enum MirrorTask {
    Write {
        pos: i64,
        data: Bytes,
    },
    Flush,
    Complete {
        opts: Option<SetAttrOpts>,
        tx: oneshot::Sender<FsResult<()>>,
    },
}

struct MirrorState {
    tx: mpsc::Sender<MirrorTask>,
    handle: JoinHandle<()>,
}

pub struct WriteCacheWriter {
    primary: UfsWriter,
    cv: CurvineFileSystem,
    ufs: UfsFileSystem,
    cv_path: Path,
    ufs_path: Path,
    path: Path,
    status: FileStatus,
    pos: i64,
    mirror_pos: i64,
    chunk: BytesMut,
    chunk_size: usize,
    mirror: Option<MirrorState>,
    pending: Arc<DashSet<String>>,
    pending_key: String,
}

impl WriteCacheWriter {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        primary: UnifiedWriter,
        cv: CurvineFileSystem,
        ufs: UfsFileSystem,
        cv_path: Path,
        ufs_path: Path,
        opts: CreateFileOpts,
        pending: Arc<DashSet<String>>,
    ) -> Result<Self, (UnifiedWriter, FsError)> {
        let primary = match UfsWriter::try_from_unified(primary) {
            Ok(primary) => primary,
            Err(writer) => {
                return Err((
                    *writer,
                    FsError::common("write cache requires a UFS writer primary"),
                ))
            }
        };
        let chunk_size = primary.chunk_size();
        let pos = primary.pos();
        let path = primary.path().clone();
        let status = primary.status().clone();

        let cv_flags = OpenFlags::new_write_only()
            .set_create(true)
            .set_overwrite(true);
        let cv_writer = match cv.open_with_opts(&cv_path, opts, cv_flags).await {
            Ok(writer) => writer,
            Err(e) => return Err((primary.into_unified(), e)),
        };

        let (tx, rx) = mpsc::channel(cv.conf().client.write_chunk_num.max(1));
        let pending_key = ufs_path.clone_uri();
        pending.insert(pending_key.clone());

        let handle = cv.fs_context_ref().rt().spawn(mirror_worker(
            cv.clone(),
            cv_path.clone(),
            pending.clone(),
            pending_key.clone(),
            cv_writer,
            rx,
        ));

        Ok(Self {
            primary,
            cv,
            ufs,
            cv_path,
            ufs_path,
            path,
            status,
            pos,
            mirror_pos: pos,
            chunk: BytesMut::with_capacity(chunk_size),
            chunk_size,
            mirror: Some(MirrorState { tx, handle }),
            pending,
            pending_key,
        })
    }

    fn mirror_active(&self) -> bool {
        self.mirror.is_some()
    }

    fn spawn_cleanup_cv(&self) {
        let cv = self.cv.clone();
        let cv_path = self.cv_path.clone();
        self.cv
            .fs_context_ref()
            .rt()
            .spawn(async move { cleanup_write_cache_file(cv, cv_path).await });
    }

    fn abandon_mirror(&mut self, reason: impl AsRef<str>) {
        if let Some(mirror) = self.mirror.take() {
            warn!(
                "disable write cache mirror for cv_path={}, ufs_path={}: {}",
                self.cv_path,
                self.ufs_path,
                reason.as_ref()
            );
            mirror.handle.abort();
            self.pending.remove(&self.pending_key);
            self.spawn_cleanup_cv();
        }
    }

    fn mirror_attr(&self, opts: Option<SetAttrOpts>, ufs_mtime: i64) -> Option<SetAttrOpts> {
        let mut opts = opts.unwrap_or_default();
        opts.ufs_mtime = Some(ufs_mtime);
        Some(opts)
    }

    async fn complete_mirror(&mut self, opts: Option<SetAttrOpts>) {
        let Some(mirror) = self.mirror.take() else {
            return;
        };

        let (tx, rx) = oneshot::channel();
        if let Err(e) = mirror.tx.try_send(MirrorTask::Complete { opts, tx }) {
            warn!(
                "failed to enqueue write cache complete for cv_path={}, ufs_path={}: {}",
                self.cv_path, self.ufs_path, e
            );
            mirror.handle.abort();
            self.pending.remove(&self.pending_key);
            self.spawn_cleanup_cv();
            return;
        }

        match time::timeout(WRITE_CACHE_COMPLETE_TIMEOUT, rx).await {
            Ok(Ok(Ok(()))) => {
                debug!(
                    "write cache mirror complete, cv_path={}, ufs_path={}",
                    self.cv_path, self.ufs_path
                );
                let _ = mirror.handle.await;
            }
            Ok(Ok(Err(e))) => {
                warn!(
                    "write cache complete failed for cv_path={}, ufs_path={}: {}",
                    self.cv_path, self.ufs_path, e
                );
            }
            Ok(Err(e)) => {
                warn!(
                    "write cache worker stopped before complete for cv_path={}, ufs_path={}: {}",
                    self.cv_path, self.ufs_path, e
                );
                self.pending.remove(&self.pending_key);
                self.spawn_cleanup_cv();
            }
            Err(_) => {
                warn!(
                    "write cache complete timeout after {:?}, cv_path={}, ufs_path={}",
                    WRITE_CACHE_COMPLETE_TIMEOUT, self.cv_path, self.ufs_path
                );
                mirror.handle.abort();
                self.pending.remove(&self.pending_key);
                self.spawn_cleanup_cv();
            }
        }
    }

    async fn enqueue_mirror_write(&mut self, pos: i64, data: Bytes) {
        let Some(mirror) = self.mirror.as_ref() else {
            return;
        };

        if let Err(e) = mirror.tx.try_send(MirrorTask::Write { pos, data }) {
            self.abandon_mirror(format!("failed to enqueue mirror write: {}", e));
        }
    }

    async fn enqueue_mirror_flush(&mut self) {
        let Some(mirror) = self.mirror.as_ref() else {
            return;
        };

        if let Err(e) = mirror.tx.try_send(MirrorTask::Flush) {
            self.abandon_mirror(format!("failed to enqueue mirror flush: {}", e));
        }
    }
}

async fn cleanup_write_cache_file(cv: CurvineFileSystem, cv_path: Path) {
    if let Err(e) = cv.delete(&cv_path, false).await {
        if !matches!(e, FsError::FileNotFound(_)) {
            warn!("failed to cleanup write cache mirror {}: {}", cv_path, e);
        }
    }
}

async fn mirror_worker(
    cv: CurvineFileSystem,
    cv_path: Path,
    pending: Arc<DashSet<String>>,
    pending_key: String,
    mut writer: FsWriter,
    mut rx: mpsc::Receiver<MirrorTask>,
) {
    let mut completed = false;

    while let Some(task) = rx.recv().await {
        match task {
            MirrorTask::Write { pos, data } => {
                if let Err(e) = writer.fuse_write(pos, DataSlice::Bytes(data)).await {
                    warn!("write cache mirror write failed for {}: {}", cv_path, e);
                    break;
                }
            }
            MirrorTask::Flush => {
                if let Err(e) = writer.flush().await {
                    warn!("write cache mirror flush failed for {}: {}", cv_path, e);
                    break;
                }
            }
            MirrorTask::Complete { opts, tx } => {
                let res = writer.complete_with_attr(opts).await;
                completed = res.is_ok();
                let _ = tx.send(res);
                break;
            }
        }
    }

    if !completed {
        let _ = writer.cancel().await;
        cleanup_write_cache_file(cv, cv_path).await;
    }

    pending.remove(&pending_key);
}

impl Writer for WriteCacheWriter {
    fn status(&self) -> &FileStatus {
        &self.status
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn pos(&self) -> i64 {
        self.pos
    }

    fn pos_mut(&mut self) -> &mut i64 {
        &mut self.pos
    }

    fn chunk_mut(&mut self) -> &mut BytesMut {
        &mut self.chunk
    }

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    async fn write_chunk(&mut self, chunk: DataSlice) -> FsResult<i64> {
        let data = chunk.to_bytes();
        if data.is_empty() {
            return Ok(0);
        }

        let len = data.len() as i64;
        let mirror_pos = self.mirror_pos;

        if let Err(e) = self
            .primary
            .async_write(DataSlice::Bytes(data.clone()))
            .await
        {
            self.abandon_mirror("ufs write failed");
            return Err(e);
        }

        if self.mirror_active() {
            self.enqueue_mirror_write(mirror_pos, data).await;
            self.mirror_pos += len;
        }

        Ok(len)
    }

    async fn flush(&mut self) -> FsResult<()> {
        self.flush_chunk().await?;
        if let Err(e) = self.primary.flush().await {
            self.abandon_mirror("ufs flush failed");
            return Err(e);
        }
        if self.mirror_active() {
            self.enqueue_mirror_flush().await;
        }
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.complete_with_attr(None).await
    }

    async fn complete_with_attr(&mut self, opts: Option<SetAttrOpts>) -> FsResult<()> {
        self.flush_chunk().await?;

        if let Err(e) = self.primary.complete_with_attr(opts.clone()).await {
            self.abandon_mirror("ufs complete failed");
            return Err(e);
        }

        if self.mirror_active() {
            match self.ufs.get_status(&self.ufs_path).await {
                Ok(status) => {
                    let opts = self.mirror_attr(opts, status.mtime);
                    self.complete_mirror(opts).await;
                }
                Err(e) => {
                    self.abandon_mirror(format!("failed to get ufs status after complete: {}", e));
                }
            }
        }

        Ok(())
    }

    async fn cancel(&mut self) -> FsResult<()> {
        self.chunk.clear();
        let res = self.primary.cancel().await;
        self.abandon_mirror("ufs writer canceled");
        res
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        self.flush_chunk().await?;
        if pos != self.pos {
            self.abandon_mirror(format!(
                "non-sequential write offset {}, expected {}",
                pos, self.pos
            ));
            self.mirror_pos = pos;
            self.primary.seek(pos).await?;
            self.pos = pos;
        }
        Ok(())
    }

    async fn resize(&mut self, opts: FileAllocOpts) -> FsResult<()> {
        self.flush_chunk().await?;
        self.abandon_mirror("writer resized");
        self.primary.resize(opts).await
    }
}
