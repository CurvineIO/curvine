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

//! Shared dir-space content summary model.
//!
//! Used by the master RPC handler, the client facades and the CLI so the
//! `curvine fs du` / `curvine fs count` commands no longer define private
//! duplicates. Snapshot metadata is only present for directory results that
//! were served from a published dir-space generation; file results and the
//! CLI live-recursive fallback carry `snapshot = None`.

/// Where a content summary result comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentSummarySource {
    /// Full-namespace snapshot built by the global scanner.
    Global,
    /// Snapshot built for one tracked point (mount or configured path).
    TrackedPoint,
    /// The path is a file: logical counts come from the live inode.
    LiveFile,
    /// The CLI fell back to a live recursive traversal.
    LiveRecursive,
    /// A source string this version does not understand.
    Unknown(String),
}

impl ContentSummarySource {
    pub fn as_str(&self) -> &'static str {
        match self {
            ContentSummarySource::Global => "GLOBAL",
            ContentSummarySource::TrackedPoint => "TRACKED_POINT",
            ContentSummarySource::LiveFile => "LIVE_FILE",
            ContentSummarySource::LiveRecursive => "LIVE_RECURSIVE",
            ContentSummarySource::Unknown(_) => "UNKNOWN",
        }
    }

    /// Unknown strings from newer peers map to `Unknown` instead of failing.
    pub fn from_str_lossy(value: &str) -> Self {
        match value {
            "GLOBAL" => ContentSummarySource::Global,
            "TRACKED_POINT" => ContentSummarySource::TrackedPoint,
            "LIVE_FILE" => ContentSummarySource::LiveFile,
            "LIVE_RECURSIVE" => ContentSummarySource::LiveRecursive,
            other => ContentSummarySource::Unknown(other.to_string()),
        }
    }
}

/// Metadata of the published snapshot generation a summary was served from.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContentSummarySnapshot {
    pub generation: u64,
    pub started_ms: i64,
    pub finished_ms: i64,
    pub anomaly_count: u64,
    pub degraded: bool,
}

/// Content summary of one path, shared by master RPC, client and CLI.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContentSummary {
    pub logical_bytes: u64,
    pub file_count: u64,
    pub directory_count: u64,
    pub source: Option<ContentSummarySource>,
    /// `Some` only when the result was served from a published snapshot.
    pub snapshot: Option<ContentSummarySnapshot>,
}

impl ContentSummary {
    /// Result for a file path: live inode data, no snapshot metadata.
    pub fn live_file(logical_bytes: u64) -> Self {
        ContentSummary {
            logical_bytes,
            file_count: 1,
            directory_count: 0,
            source: Some(ContentSummarySource::LiveFile),
            snapshot: None,
        }
    }

    /// Result of the CLI live recursive traversal.
    pub fn live_recursive(logical_bytes: u64, file_count: u64, directory_count: u64) -> Self {
        ContentSummary {
            logical_bytes,
            file_count,
            directory_count,
            source: Some(ContentSummarySource::LiveRecursive),
            snapshot: None,
        }
    }

    /// Result served from a published dir-space snapshot generation.
    pub fn from_snapshot(
        logical_bytes: u64,
        file_count: u64,
        directory_count: u64,
        source: ContentSummarySource,
        snapshot: ContentSummarySnapshot,
    ) -> Self {
        ContentSummary {
            logical_bytes,
            file_count,
            directory_count,
            source: Some(source),
            snapshot: Some(snapshot),
        }
    }

    pub fn is_degraded(&self) -> bool {
        self.snapshot.as_ref().map(|s| s.degraded).unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::ContentSummaryProto;
    use crate::ProtoUtils;

    #[test]
    fn content_summary_live_file_has_no_snapshot() {
        let summary = ContentSummary::live_file(1024);

        assert_eq!(summary.source, Some(ContentSummarySource::LiveFile));
        assert!(summary.snapshot.is_none());
        assert_eq!(summary.logical_bytes, 1024);
        assert_eq!(summary.file_count, 1);
        assert_eq!(summary.directory_count, 0);
        assert!(!summary.is_degraded());
    }

    #[test]
    fn content_summary_live_recursive_has_no_snapshot() {
        // The CLI live fallback is constructed locally and never crosses the
        // wire, so it does not require a proto round-trip.
        let summary = ContentSummary::live_recursive(4096, 4, 2);

        assert_eq!(summary.source, Some(ContentSummarySource::LiveRecursive));
        assert!(summary.snapshot.is_none());
        assert_eq!(summary.logical_bytes, 4096);
        assert_eq!(summary.file_count, 4);
        assert_eq!(summary.directory_count, 2);
    }

    #[test]
    fn content_summary_snapshot_round_trip() {
        let snapshot = ContentSummarySnapshot {
            generation: 42,
            started_ms: 1_000,
            finished_ms: 1_500,
            anomaly_count: 3,
            degraded: true,
        };
        let summary =
            ContentSummary::from_snapshot(8192, 8, 4, ContentSummarySource::TrackedPoint, snapshot);

        let pb = ProtoUtils::content_summary_to_pb(summary.clone());
        // Snapshot-carrying results must set generation/timestamp fields.
        assert_eq!(pb.generation, Some(42));
        assert_eq!(pb.started_ms, Some(1_000));
        assert_eq!(pb.finished_ms, Some(1_500));
        assert_eq!(pb.degraded, Some(true));

        let round_trip = ProtoUtils::content_summary_from_pb(pb);
        assert_eq!(round_trip, summary);
        assert_eq!(round_trip.source, Some(ContentSummarySource::TrackedPoint));
        assert_eq!(round_trip.snapshot.as_ref().unwrap().generation, 42);
        assert!(round_trip.is_degraded());
    }

    #[test]
    fn content_summary_global_round_trip_keeps_empty_unknown_source() {
        let snapshot = ContentSummarySnapshot {
            generation: 7,
            started_ms: 10,
            finished_ms: 20,
            anomaly_count: 0,
            degraded: false,
        };
        let summary =
            ContentSummary::from_snapshot(1, 1, 1, ContentSummarySource::Global, snapshot);

        let pb = ProtoUtils::content_summary_to_pb(summary.clone());
        assert_eq!(pb.source.as_deref(), Some("GLOBAL"));
        assert_eq!(ProtoUtils::content_summary_from_pb(pb), summary);
    }

    #[test]
    fn content_summary_unknown_source_does_not_panic() {
        let pb = ContentSummaryProto {
            logical_bytes: Some(1),
            source: Some("SOMETHING_NEWER".to_string()),
            ..Default::default()
        };

        let summary = ProtoUtils::content_summary_from_pb(pb);
        assert_eq!(
            summary.source,
            Some(ContentSummarySource::Unknown("SOMETHING_NEWER".to_string()))
        );
    }

    #[test]
    fn content_summary_missing_fields_default() {
        let summary = ProtoUtils::content_summary_from_pb(ContentSummaryProto::default());

        assert_eq!(summary.logical_bytes, 0);
        assert_eq!(summary.file_count, 0);
        assert_eq!(summary.directory_count, 0);
        assert!(summary.snapshot.is_none());
    }
}

/// Scan-round status published by the master dir-space coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirSpaceStatus {
    pub enabled: bool,
    pub scanning: bool,
    pub in_round: bool,
    pub phase: String,
    pub scope: String,
    pub current_path: String,
    pub last_outcome: String,
    pub last_finished_ms: i64,
    pub next_scan_at_ms: i64,
    pub paths_total: u32,
    pub paths_done: u32,
}
