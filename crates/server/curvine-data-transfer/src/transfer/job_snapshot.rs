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

use crate::transfer::{ClusterMetadataCache, MountSnapshotError};
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::Path;
use curvine_model::{MountInfo, TransferJobRecord};

pub fn job_mount_snapshot(
    job: &TransferJobRecord,
    cache: &ClusterMetadataCache,
) -> FsResult<MountInfo> {
    if let Some(mount) = parse_stored_mount_snapshot(job)? {
        return Ok(mount);
    }

    let source = Path::from_str(&job.source_path)?;
    let target = Path::from_str(&job.target_path)?;
    cache
        .find_mount_with_refresh(job.kind, &source, &target)
        .map(|snapshot| snapshot.mount)
        .map_err(MountSnapshotError::into_fs_error)
}

pub(crate) async fn job_mount_snapshot_with_version_async(
    job: &TransferJobRecord,
    cache: &ClusterMetadataCache,
) -> Result<(MountInfo, u64), MountSnapshotError> {
    if let Some(mount) = parse_stored_mount_snapshot(job).map_err(MountSnapshotError::Terminal)? {
        return Ok((mount, job.cluster_snapshot_version));
    }

    let job = job.clone();
    let cache = cache.clone();
    tokio::task::spawn_blocking(move || {
        let source = Path::from_str(&job.source_path)
            .map_err(|err| MountSnapshotError::Terminal(FsError::common(err.to_string())))?;
        let target = Path::from_str(&job.target_path)
            .map_err(|err| MountSnapshotError::Terminal(FsError::common(err.to_string())))?;
        cache
            .find_mount_with_refresh(job.kind, &source, &target)
            .map(|snapshot| (snapshot.mount, snapshot.version))
    })
    .await
    .map_err(|_| {
        MountSnapshotError::Retryable(FsError::common("Transfer mount snapshot task panicked"))
    })?
}

pub async fn job_mount_snapshot_async(
    job: &TransferJobRecord,
    cache: &ClusterMetadataCache,
) -> FsResult<MountInfo> {
    if let Some(mount) = parse_stored_mount_snapshot(job)? {
        return Ok(mount);
    }

    let job = job.clone();
    let cache = cache.clone();
    tokio::task::spawn_blocking(move || job_mount_snapshot(&job, &cache))
        .await
        .map_err(|_| FsError::common("Transfer mount snapshot task panicked"))?
}

fn parse_stored_mount_snapshot(job: &TransferJobRecord) -> FsResult<Option<MountInfo>> {
    if is_empty_snapshot(&job.mount_snapshot_json) {
        return Ok(None);
    }
    serde_json::from_str(&job.mount_snapshot_json)
        .map(Some)
        .map_err(|_| {
            FsError::common(format!(
                "Stored transfer mount snapshot for job {} is invalid",
                job.job_id
            ))
        })
}

fn is_empty_snapshot(value: &str) -> bool {
    let value = value.trim();
    value.is_empty() || value == "{}"
}
