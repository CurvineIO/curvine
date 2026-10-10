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

use crate::proto::raft::SnapshotData;
use crate::raft::snapshot::DownloadJob;
use crate::raft::storage::AppStorage;
use crate::raft::{RaftClient, RaftResult};
use curvine_config::JournalConf;
use curvine_core_error::err_box;
use curvine_runtime::common::Utils;

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
        data: SnapshotData,
    ) -> RaftResult<Self> {
        if node_id == data.node_id {
            return Ok(Self::Local(data));
        }

        let Some(files) = &data.files_data else {
            return err_box!("Not found snapshot data");
        };
        let dir = app_store.snapshot_dir(Utils::rand_id())?;
        let download_job = DownloadJob::new(data.node_id, dir, files.clone(), conf, client);
        Ok(SnapshotFetcher::Remote(data, download_job))
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
