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

//! `cv fs reconcile` — backfill exports for fs_mode files that never reached UFS.
//!
//! Async export is best effort: a submit that fails while Transfer is down
//! leaves the file in `Cv` with no UFS object. Free only releases verified
//! files, so nothing is lost — but a mount without TTL never gets a trigger to
//! retry, and an operator shutting Transfer down has no way to confirm UFS is
//! complete. This command supplies that trigger.
//!
//! It only enqueues exports for fs_mode mounts. It never releases anything or
//! waits for a copy; `cv free` and TTL reclaim Curvine data.

use curvine_core_error::CommonResult;
use curvine_fs_api::{FileSystem, Path};
use curvine_model::StorageState;
use curvine_unified_fs::UnifiedFileSystem;

#[derive(Debug)]
pub struct ReconcileCommand {
    pub path: String,
}

impl ReconcileCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        let root = Path::from_str(&self.path)?;

        let mut pending = vec![root];
        let mut scanned: u64 = 0;
        let mut enqueued: u64 = 0;
        let mut skipped: u64 = 0;
        let mut failed: u64 = 0;

        while let Some(dir) = pending.pop() {
            let entries = match client.list_status(&dir).await {
                Ok(entries) => entries,
                Err(err) => {
                    eprintln!("list {} failed: {}", dir.full_path(), err);
                    failed += 1;
                    continue;
                }
            };

            for status in entries {
                let Ok(path) = Path::from_str(&status.path) else {
                    eprintln!("skip unparsable path {}", status.path);
                    skipped += 1;
                    continue;
                };
                if status.is_dir {
                    pending.push(path);
                    continue;
                }

                scanned += 1;
                // `Both` already has a verified object; `Ufs` has no local copy.
                if status.storage_policy.state != StorageState::Cv || !status.is_complete {
                    skipped += 1;
                    continue;
                }

                match client.export_fs_mode_file(&path).await {
                    Ok(Some(job_id)) => {
                        enqueued += 1;
                        println!("enqueued {} as {}", path.full_path(), job_id);
                    }
                    // Not in an fs_mode mount, or changed since the listing.
                    Ok(None) => skipped += 1,
                    Err(err) => {
                        eprintln!("enqueue {} failed: {}", path.full_path(), err);
                        failed += 1;
                    }
                }
            }
        }

        println!(
            "scanned: {}, enqueued: {}, skipped: {}, failed: {}",
            scanned, enqueued, skipped, failed
        );

        if failed > 0 {
            return Err(format!("{} item(s) could not be reconciled", failed).into());
        }
        Ok(())
    }
}
