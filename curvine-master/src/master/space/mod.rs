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

mod agg;
pub mod scan_lease;
pub mod scanner;
mod store;
mod types;

pub use agg::{AggregationOutput, SubtreeAggregator};
pub use scan_lease::{
    LeaseTicket, MetadataRestoreGuard, MetadataScanControl, MetadataScanLease, RestoreTicket,
};
pub use scanner::{GlobalScanner, ScanContext, ScanLimits, UnpublishedSnapshot};
pub use store::{DirSpaceQueryResult, DirSpaceSnapshotStore, SnapshotMiss};
pub use types::{
    AggregationStats, DirAgg, DirSpaceSnapshot, ScanAbortReason, SnapshotMeta, SnapshotSource,
};
