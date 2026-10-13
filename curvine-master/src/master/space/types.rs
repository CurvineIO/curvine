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

use curvine_core_error::{err_box, CommonResult};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DirAgg {
    pub logical_bytes: u64,
    pub file_count: u64,
    pub directory_count: u64,
}

impl DirAgg {
    pub fn empty_dir() -> Self {
        Self {
            logical_bytes: 0,
            file_count: 0,
            directory_count: 1,
        }
    }

    pub fn add_file(&mut self, len: i64) -> CommonResult<()> {
        let len = len.max(0) as u64;
        self.logical_bytes = match self.logical_bytes.checked_add(len) {
            Some(v) => v,
            None => return err_box!("dir-space logical_bytes overflow"),
        };
        self.file_count = match self.file_count.checked_add(1) {
            Some(v) => v,
            None => return err_box!("dir-space file_count overflow"),
        };
        Ok(())
    }

    pub fn merge(&mut self, other: &DirAgg) -> CommonResult<()> {
        self.logical_bytes = match self.logical_bytes.checked_add(other.logical_bytes) {
            Some(v) => v,
            None => return err_box!("dir-space logical_bytes overflow"),
        };
        self.file_count = match self.file_count.checked_add(other.file_count) {
            Some(v) => v,
            None => return err_box!("dir-space file_count overflow"),
        };
        self.directory_count = match self.directory_count.checked_add(other.directory_count) {
            Some(v) => v,
            None => return err_box!("dir-space directory_count overflow"),
        };
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AggregationStats {
    pub anomaly_count: u64,
    pub degraded: bool,
}

impl AggregationStats {
    pub fn record_anomaly(&mut self, count: u64) {
        self.anomaly_count = self.anomaly_count.saturating_add(count);
        self.degraded = true;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanAbortReason {
    MaxDirs { limit: u64, observed: u64 },
    MaxDepth { limit: u32, observed: u32 },
    Overflow,
}

/// Where a served content-summary result came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotSource {
    /// Full-namespace snapshot built by the global scanner.
    Global,
    /// Snapshot rooted at a tracked point (mount or configured path).
    TrackedPoint,
    /// A single file resolved live from the inode tree (no snapshot behind it).
    LiveFile,
    /// Client-side recursive fallback built by the CLI.
    LiveRecursive,
}

impl SnapshotSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            SnapshotSource::Global => "GLOBAL",
            SnapshotSource::TrackedPoint => "TRACKED_POINT",
            SnapshotSource::LiveFile => "LIVE_FILE",
            SnapshotSource::LiveRecursive => "LIVE_RECURSIVE",
        }
    }
}

/// Metadata of one published immutable generation.
///
/// `generation` is assigned by the snapshot store inside the publish critical
/// section; scanner products carry a placeholder until they are published.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotMeta {
    pub generation: u64,
    pub started_ms: i64,
    pub finished_ms: i64,
    pub inode_count: u64,
    pub edge_count: u64,
    pub anomaly_count: u64,
    pub degraded: bool,
}

/// An immutable, fully built dir-space generation for one subtree.
///
/// `entries` maps every directory inode id in the subtree (excluding
/// `root_id`) to its recursive aggregate; the root's own aggregate is carried
/// by the scanner output and merged into the snapshot by the publisher.
#[derive(Debug, Clone)]
pub struct DirSpaceSnapshot {
    pub meta: SnapshotMeta,
    pub root_id: i64,
    pub source: SnapshotSource,
    pub entries: HashMap<i64, DirAgg>,
}
