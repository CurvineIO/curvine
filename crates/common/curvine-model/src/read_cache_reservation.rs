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

use std::collections::BTreeMap;
use std::sync::Weak;

pub const READ_CACHE_PENDING_LIMIT: usize = 65_536;
pub const READ_CACHE_BATCH_LIMIT: usize = 4_096;
pub const READ_CACHE_ACK_MIN_MS: u64 = 60_000;

#[derive(Debug, Clone)]
pub(crate) struct CacheCredit {
    pub sequence: u64,
    pub lease: Weak<()>,
}

#[derive(Debug, Clone)]
pub(crate) struct CacheBatch {
    pub token: String,
    pub issued_at: u64,
    pub timeout_ms: u64,
    pub entries: BTreeMap<i64, CacheCredit>,
}

/// Optional Master-local proof. Losing it never releases reservation bytes or
/// changes registration. A reclamation watermark excludes every allocation
/// that might already have been paid for by an unattributed heartbeat delta.
#[derive(Debug, Clone, Default)]
pub struct ReadCacheReservation {
    pub(crate) epoch: Option<(u64, u64)>,
    pub(crate) snapshot_seq: u64,
    pub(crate) allocation_floor: u64,
    pub(crate) anonymous_tainted: bool,
    pub(crate) pending: BTreeMap<i64, CacheCredit>,
    pub(crate) inflight: Option<CacheBatch>,
}

impl ReadCacheReservation {
    pub(crate) fn discard(&mut self, sequence: u64, anonymous: bool) {
        self.allocation_floor = sequence;
        self.anonymous_tainted |= anonymous;
        // Stale pending entries are drained one bounded batch per heartbeat.
        // Avoid scanning the whole map on every net-delta reclamation.
        self.inflight = None;
    }
}
