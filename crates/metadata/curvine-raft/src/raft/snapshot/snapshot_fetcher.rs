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

use crate::proto::raft::{SnapshotData, SnapshotFileInfo};
use crate::raft::snapshot::DownloadJob;
use crate::raft::storage::AppStorage;
use crate::raft::{RaftClient, RaftError, RaftResult};
use curvine_config::JournalConf;
use curvine_core_error::err_box;
use curvine_runtime::common::{FileUtils, Utils};
use log::{debug, info};
use std::path::{Path, PathBuf};

pub enum SnapshotFetcher {
    Local(SnapshotData),
    // Files of another member's snapshot are readable from this host, but this
    // member must still own the copy it installs, so they are materialized into
    // the directory carried here before the snapshot is returned.
    OwnedCopy(SnapshotData, String),
    Remote(SnapshotData, DownloadJob),
}

impl SnapshotFetcher {
    pub fn new(
        node_id: u64,
        app_store: &impl AppStorage,
        conf: &JournalConf,
        client: RaftClient,
        mut data: SnapshotData,
    ) -> RaftResult<Self> {
        if node_id == data.node_id {
            if let Some(files) = data.files_data.as_ref() {
                if !Self::local_snapshot_ready(&files.dir, &files.files) {
                    return err_box!(
                        "local snapshot {} is incomplete under {}, it cannot be reused",
                        data.snapshot_id,
                        files.dir
                    );
                }
            }
            return Ok(Self::Local(data));
        }

        // The registered owner may be another member while its manifest files are
        // already readable from this host (shared filesystem, manual meta+journal
        // copies, reused volume). Adopting that directory would leave this member
        // advertising and installing a snapshot it does not own: the owner may
        // compact or delete it at any time, and this member could not serve its
        // own copy after a restart or after being promoted. Read the files locally
        // but materialize a private copy inside this member's own storage.
        // The manifest dir is the snapshot's own directory (named at creation,
        // e.g. ck-{millis}, not necessarily ck-{snapshot_id}), so readiness can
        // only be judged against the manifest itself.
        if let Some(files) = data.files_data.as_mut() {
            if Self::local_snapshot_ready(&files.dir, &files.files) {
                let dir = app_store.snapshot_dir(Utils::rand_id())?;
                if dir.trim().is_empty() {
                    // A store that cannot name a snapshot directory cannot own a
                    // copy, and must not fall back to reading another member's
                    // directory in place.
                    return err_box!(
                        "app storage returned an empty snapshot directory; it cannot own a copy of snapshot {}",
                        data.snapshot_id
                    );
                }
                info!(
                    "snapshot {} from node {} is readable at {}, materialize it under {} instead of adopting that directory",
                    data.snapshot_id, data.node_id, files.dir, dir
                );
                return Ok(Self::OwnedCopy(data, dir));
            }
        }

        let Some(files) = &data.files_data else {
            return err_box!("Not found snapshot data");
        };
        let dir = app_store.snapshot_dir(Utils::rand_id())?;
        let download_job = DownloadJob::new(data.node_id, dir, files.clone(), conf, client);
        Ok(SnapshotFetcher::Remote(data, download_job))
    }

    /// A local copy is reusable only when every manifest file exists with the
    /// expected length. mtime/ctime are deliberately not compared: copying the
    /// directory legitimately changes them. Anything missing falls back to the
    /// download path.
    fn local_snapshot_ready(dir: &str, files: &[SnapshotFileInfo]) -> bool {
        if dir.is_empty() || files.is_empty() {
            return false;
        }

        files.iter().all(|file| {
            let path = Path::new(dir).join(&file.path);
            matches!(FileUtils::metadata(&path), Ok(meta) if meta.len() == file.len)
        })
    }

    /// Materializes a snapshot that is already readable on this host into `dir`
    /// so the local member owns the files it installs. A checkpoint file is
    /// immutable once created, so it is hard linked where the filesystem allows
    /// it and copied otherwise. Sizes are verified against the manifest, and both
    /// the file bytes and every directory entry created here are synced before
    /// the copy can be published.
    fn materialize_snapshot(source: &str, dir: &str, files: &[SnapshotFileInfo]) -> RaftResult<()> {
        let dir = Path::new(dir);
        let mut synced_dirs: Vec<PathBuf> = Vec::new();
        Self::create_dirs_and_collect(dir, &mut synced_dirs)?;

        for file in files {
            let from = Path::new(source).join(&file.path);
            let to = dir.join(&file.path);
            if let Some(parent) = to.parent() {
                Self::create_dirs_and_collect(parent, &mut synced_dirs)?;
            }

            if let Err(e) = std::fs::hard_link(&from, &to) {
                // Cross-device paths and filesystems without hard links need a
                // real copy of the immutable checkpoint file.
                debug!(
                    "snapshot file {} cannot be linked ({}), copying it instead",
                    from.display(),
                    e
                );
                std::fs::copy(&from, &to)?;
                std::fs::File::open(&to)?.sync_all()?;
            }

            let copied = std::fs::metadata(&to)?.len();
            if copied != file.len {
                return err_box!(
                    "snapshot file {} copy length mismatch, expected {}, actual {}",
                    to.display(),
                    file.len,
                    copied
                );
            }
        }

        // A directory entry is durable only once its own directory is synced. The
        // collected list is deepest first, so syncing it in reverse makes every
        // parent entry durable before the directory that depends on it.
        for created in synced_dirs.iter().rev() {
            std::fs::File::open(created)?.sync_all()?;
        }

        Ok(())
    }

    /// Creates `dir` and records every directory this call created, plus the
    /// deepest directory that already existed because its entry list changed.
    fn create_dirs_and_collect(dir: &Path, synced: &mut Vec<PathBuf>) -> RaftResult<()> {
        if dir.as_os_str().is_empty() {
            return err_box!("cannot materialize a snapshot under an empty directory path");
        }
        if dir.is_dir() {
            Self::push_dir(synced, dir);
            return Ok(());
        }

        let mut missing: Vec<PathBuf> = Vec::new();
        let mut existing = dir.parent();
        while let Some(path) = existing {
            if path.as_os_str().is_empty() || path.is_dir() {
                break;
            }
            missing.push(path.to_path_buf());
            existing = path.parent();
        }

        FileUtils::create_dir(dir, true)?;
        Self::push_dir(synced, dir);
        for path in &missing {
            Self::push_dir(synced, path);
        }
        if let Some(parent) = existing {
            Self::push_dir(synced, parent);
        }
        Ok(())
    }

    fn push_dir(synced: &mut Vec<PathBuf>, dir: &Path) {
        if dir.as_os_str().is_empty() || synced.iter().any(|seen| seen == dir) {
            return;
        }
        synced.push(dir.to_path_buf());
    }

    pub async fn fetch(self) -> RaftResult<SnapshotData> {
        match self {
            SnapshotFetcher::Local(snapshot) => Ok(snapshot),

            SnapshotFetcher::OwnedCopy(mut data, dir) => {
                let (source, files) = match data.files_data.as_ref() {
                    Some(files) => (files.dir.clone(), files.files.clone()),
                    None => return err_box!("Not found snapshot data"),
                };

                let copy_dir = dir.clone();
                let materialized = tokio::task::spawn_blocking(move || {
                    Self::materialize_snapshot(&source, &copy_dir, &files)
                })
                .await
                .map_err(|e| RaftError::from(format!("snapshot copy task failed: {e}")))
                .and_then(|result| result);

                if let Err(e) = materialized {
                    let _ = FileUtils::delete_path(&dir, true);
                    return Err(e);
                }

                if let Some(files) = data.files_data.as_mut() {
                    files.dir = dir;
                }
                info!(
                    "snapshot {} materialized, this member now owns {}",
                    data.snapshot_id,
                    data.data_dir()
                );
                Ok(data)
            }
            SnapshotFetcher::Remote(mut data, mut download_job) => match data.files_data {
                None => err_box!("Not found snapshot data"),
                Some(ref mut files) => {
                    files.dir = download_job.run().await?;
                    Ok(data)
                }
            },
        }
    }
}
