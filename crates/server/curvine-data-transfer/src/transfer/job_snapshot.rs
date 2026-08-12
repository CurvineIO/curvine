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

use crate::transfer::ClusterMetadataCache;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::Path;
use curvine_model::{MountInfo, TransferJobRecord};

pub fn job_mount_snapshot(
    job: &TransferJobRecord,
    cache: &ClusterMetadataCache,
) -> FsResult<MountInfo> {
    if !is_empty_snapshot(&job.mount_snapshot_json) {
        return serde_json::from_str(&job.mount_snapshot_json).map_err(|_| {
            FsError::common(format!(
                "Stored transfer mount snapshot for job {} is invalid",
                job.job_id
            ))
        });
    }

    let source = Path::from_str(&job.source_path)?;
    let target = Path::from_str(&job.target_path)?;
    cache.find_mount(job.kind, &source, &target)
}

fn is_empty_snapshot(value: &str) -> bool {
    let value = value.trim();
    value.is_empty() || value == "{}"
}
