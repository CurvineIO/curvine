// Copyright 2025 The Curvine Authors
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

use curvine_client::unified::{UnifiedFileSystem, UnifiedWriter};
use curvine_error::FsError;
use curvine_fs_api::{FileSystem, Path, Writer};
use curvine_model::OpenFlags;
use tokio::task::JoinHandle;

/// Owns a unique staging path and its writer until an atomic rename publishes
/// the file. The HTTP server may drop the request without a body read error.
pub(crate) struct StagedWrite(Option<PendingWrite>);

struct PendingWrite {
    fs: UnifiedFileSystem,
    path: Path,
    writer: Option<UnifiedWriter>,
}

impl StagedWrite {
    pub(crate) async fn create(fs: UnifiedFileSystem, path: Path) -> Result<Self, String> {
        // Do not drop an in-flight create: the backend may have allocated its
        // first block even if the request that started the RPC disappeared.
        // If the caller goes away, the task's returned guard is dropped.
        tokio::spawn(async move {
            let mut stage = Self(Some(PendingWrite {
                fs,
                path,
                writer: None,
            }));
            let pending = stage.0.as_mut().expect("new staging write");
            let opts = pending
                .fs
                .cv()
                .create_opts_builder()
                .create_parent(true)
                .build();
            let flags = OpenFlags::new_create().set_exclusive(true);
            let result = pending.fs.open_with_opts(&pending.path, opts, flags).await;
            match result {
                Ok(writer) => {
                    pending.writer = Some(writer);
                    Ok(stage)
                }
                Err(err @ FsError::FileAlreadyExists(_)) => {
                    // Do not unlink a staging name we did not create.
                    stage.0 = None;
                    Err(err.to_string())
                }
                Err(err) => {
                    let err = crate::s3::handlers::fs_error_text(err);
                    stage.finish(Err(err)).await
                }
            }
        })
        .await
        .map_err(|err| format!("Failed to join staging create: {err}"))?
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0.as_ref().expect("staging write is armed").path
    }

    pub(crate) fn writer(&mut self) -> &mut UnifiedWriter {
        self.0
            .as_mut()
            .and_then(|pending| pending.writer.as_mut())
            .expect("staging write must be open")
    }

    pub(crate) async fn complete(&mut self) -> Result<(), String> {
        self.writer()
            .complete()
            .await
            .map_err(|err| err.to_string())?;
        // The writer no longer has anything to cancel. A later metadata or
        // rename failure still needs to unlink the staging inode.
        self.0.as_mut().expect("staging write is armed").writer = None;
        Ok(())
    }

    pub(crate) async fn publish(&mut self, destination: &Path) -> Result<(), String> {
        let pending = self.0.as_ref().expect("staging write is armed");
        debug_assert!(pending.writer.is_none());
        let renamed = pending
            .fs
            .rename(&pending.path, destination)
            .await
            .map_err(crate::s3::handlers::fs_error_text)?;
        if !renamed {
            return Err("backend did not rename the staging file".to_string());
        }
        self.0 = None;
        Ok(())
    }

    pub(crate) async fn finish<T>(mut self, result: Result<T, String>) -> Result<T, String> {
        // On an ordinary error wait for cleanup before reporting the original
        // error. If this wait is cancelled, the cleanup task still runs.
        if let Some(cleanup) = self.start_cleanup() {
            if let Err(err) = cleanup.await {
                tracing::warn!("Failed to join staging cleanup: {err}");
            }
        }
        result
    }

    fn start_cleanup(&mut self) -> Option<JoinHandle<()>> {
        let PendingWrite { fs, path, writer } = self.0.take()?;
        Some(tokio::spawn(async move {
            if let Some(mut writer) = writer {
                if let Err(err) = writer.cancel().await {
                    tracing::warn!("Failed to cancel staging write {path}: {err}");
                }
            }
            // Permanently delete internal staging, never the destination or an
            // old part. The path can be absent after an ambiguous RPC result.
            if let Err(err) = fs.delete_with_opts(&path, false, true).await {
                if !matches!(err, FsError::FileNotFound(_)) {
                    tracing::warn!("Failed to purge staging file {path}: {err}");
                }
            }
        }))
    }
}

impl Drop for StagedWrite {
    fn drop(&mut self) {
        // Dropping a JoinHandle detaches, rather than aborts, the task.
        drop(self.start_cleanup());
    }
}
