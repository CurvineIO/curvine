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
use crate::raft::{RaftClient, RaftResult};
use curvine_config::JournalConf;
use curvine_core_error::err_box;
use curvine_runtime::common::{FileUtils, Utils};
use log::info;
use std::path::Path;

pub enum SnapshotFetcher {
    Local(SnapshotData),
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
            return Ok(Self::Local(data));
        }

        // The registered owner may be another member, but the snapshot files may
        // already be present locally (manual meta+journal copies, reused volume).
        // The manifest dir is the snapshot's own directory (named at creation,
        // e.g. ck-{millis}, not necessarily ck-{snapshot_id}), so a local copy
        // can only be judged against the manifest itself. Reusing the complete
        // local copy avoids a full download; the fetch layer records this member
        // as the owner afterwards.
        if let Some(files) = data.files_data.as_mut() {
            if Self::local_snapshot_ready(&files.dir, &files.files) {
                info!(
                    "snapshot {} already exists locally at {}, skip download from node {}",
                    data.snapshot_id, files.dir, data.node_id
                );
                return Ok(Self::Local(data));
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

    pub async fn fetch(self) -> RaftResult<SnapshotData> {
        match self {
            SnapshotFetcher::Local(snapshot) => Ok(snapshot),
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
