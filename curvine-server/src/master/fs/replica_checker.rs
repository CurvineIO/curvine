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

use crate::master::fs::MasterFilesystem;
use crate::master::meta::inode::InodeView;
use crate::master::MasterMonitor;
use curvine_common::error::FsError;
use curvine_common::FsResult;
use log::{info, warn};
use orpc::common::TimeSpent;
use orpc::runtime::LoopTask;
use orpc::CommonResult;

/// Periodically checks whether block replica counts meet the expected replicas configured on files.
pub struct ReplicaChecker {
    fs: MasterFilesystem,
    monitor: MasterMonitor,
}

impl ReplicaChecker {
    pub fn new(fs: MasterFilesystem, monitor: MasterMonitor) -> Self {
        Self { fs, monitor }
    }
}

impl LoopTask for ReplicaChecker {
    type Error = FsError;

    fn run(&self) -> FsResult<()> {
        // Only active master performs checks
        if !self.monitor.is_active() {
            return Ok(());
        }

        let spend = TimeSpent::new();

        // Access underlying rocks store for efficient full scan
        let fs_dir_lock = self.fs.fs_dir();
        let fs_dir = fs_dir_lock.read();
        let rocks = fs_dir.get_rocks_store();

        // Iterate all inodes; count under-replicated blocks
        let mut under_replicated_blocks: i64 = 0;
        let mut under_replicated_files: i64 = 0;
        let mut scanned_files: i64 = 0;
        let mut scanned_blocks: i64 = 0;

        let iter = match rocks.inodes_iter() {
            Ok(it) => it,
            Err(e) => {
                warn!("ReplicaChecker: failed to create inodes iterator: {}", e);
                return Ok(());
            }
        };

        for item in iter {
            let kv = match item {
                Ok(v) => v,
                Err(e) => {
                    warn!("ReplicaChecker: iterate inode error: {}", e);
                    continue;
                }
            };

            // Deserialize inode view
            let inode: InodeView =
                match curvine_common::utils::SerdeUtils::deserialize(kv.1.as_ref()) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("ReplicaChecker: deserialize inode error: {}", e);
                        continue;
                    }
                };

            // Only check files
            let file = match inode.as_file_ref() {
                Ok(f) => f,
                Err(_) => continue,
            };

            scanned_files += 1;

            // Skip files without blocks
            if file.blocks.is_empty() {
                continue;
            }

            let expected = file.replicas as usize;
            let mut file_under = false;

            for meta in &file.blocks {
                scanned_blocks += 1;
                let locs: CommonResult<Vec<curvine_common::state::BlockLocation>> =
                    rocks.get_locations(meta.id);
                let actual = match locs {
                    Ok(v) => v.len(),
                    Err(e) => {
                        warn!(
                            "ReplicaChecker: get locations failed for block {}: {}",
                            meta.id, e
                        );
                        0
                    }
                };

                if actual < expected {
                    under_replicated_blocks += 1;
                    file_under = true;
                    warn!(
                            "Under-replicated block detected: file_id={}, block_id={}, expected_replicas={}, actual_replicas={}",
                            file.id, meta.id, expected, actual
                        );
                }
            }

            if file_under {
                under_replicated_files += 1;
            }
        }

        info!(
            "ReplicaChecker summary: files_scanned={}, blocks_scanned={}, under_replicated_files={}, under_replicated_blocks={}, used={}ms",
            scanned_files,
            scanned_blocks,
            under_replicated_files,
            under_replicated_blocks,
            spend.used_ms()
        );

        Ok(())
    }

    fn terminate(&self) -> bool {
        self.monitor.is_stop()
    }
}
