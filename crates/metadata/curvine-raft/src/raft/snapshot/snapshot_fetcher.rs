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
use log::info;
use std::path::{Path, PathBuf};

pub enum SnapshotFetcher {
    Local(SnapshotData),
    // Files of another member's snapshot are readable from this host, but this
    // member must still own the copy it installs, so they are copied into the
    // directory carried here before the snapshot is returned.
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
                    // A store that cannot name a private snapshot directory cannot
                    // own a copy. Keep reading the files in place; the directory
                    // convention is only guaranteed by stores that implement one.
                    info!(
                        "snapshot {} from node {} is readable at {}, reused in place; the app store exposes no snapshot directory",
                        data.snapshot_id, data.node_id, files.dir
                    );
                    return Ok(Self::Local(data));
                }
                info!(
                    "snapshot {} from node {} is readable at {}, copy it to {} instead of adopting that directory",
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

    /// Copies a snapshot that is already readable on this host into `dir` so the
    /// local member owns the files it installs. Sizes are verified against the
    /// manifest, the bytes and their directory entries are synced before the copy
    /// can be published, and a partial copy is removed by the caller.
    fn copy_snapshot(source: &str, dir: &str, files: &[SnapshotFileInfo]) -> RaftResult<()> {
        let mut synced_dirs: Vec<PathBuf> = Vec::new();

        for file in files {
            let from = Path::new(source).join(&file.path);
            let to = Path::new(dir).join(&file.path);
            if let Some(parent) = to.parent() {
                FileUtils::create_dir(parent, true)?;
                if !synced_dirs.iter().any(|synced| synced == parent) {
                    synced_dirs.push(parent.to_path_buf());
                }
            }
            std::fs::copy(&from, &to)?;
            // The installed manifest and the trimmed journal will point at this
            // copy, so the bytes must survive a crash before it is published.
            std::fs::File::open(&to)?.sync_all()?;

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

        // Directory entries need their own sync, otherwise a crash can lose a
        // file that was already appended to the directory.
        for synced in synced_dirs.iter().rev() {
            std::fs::File::open(synced)?.sync_all()?;
        }

        Ok(())
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
                let copied = tokio::task::spawn_blocking(move || {
                    Self::copy_snapshot(&source, &copy_dir, &files)
                })
                .await
                .map_err(|e| RaftError::from(format!("snapshot copy task failed: {e}")))?;

                if let Err(e) = copied {
                    let _ = FileUtils::delete_path(&dir, true);
                    return Err(e);
                }

                if let Some(files) = data.files_data.as_mut() {
                    files.dir = dir;
                }
                info!(
                    "snapshot {} copied, this member now owns {}",
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
