// Copyright 2026 OPPO
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

//! The one genuinely repeated lock-request shape (C3 layout only).
//!
//! `set_attr_single_path_sharded`, `complete_file_sharded` and
//! `touch_atime_sharded` each built the same request list: every ancestor a
//! read, the target a write. Nothing else about them is common - `complete`
//! additionally takes location locks, `touch_atime` is best-effort, and
//! `set_attr` decides the root-retention flag for its own acquisition - so only
//! the construction is shared here. The callers still call
//! `lock_sharded_requests` themselves, and no plan/strategy type is introduced.

use super::*;

impl MasterFilesystem {
    /// Ancestor reads plus a target write for an already-resolved id chain.
    ///
    /// Pure construction: no locks are taken, no ordering or de-duplication is
    /// re-implemented from the lock pool, and the root-retention flag stays at
    /// the call site where each caller already knows whether its target is root.
    pub(super) fn ancestor_read_target_write_requests(
        ids: &[i64],
    ) -> smallvec::SmallVec<[InodeLockRequest; 8]> {
        let target_depth = ids.len() - 1;
        ids.iter()
            .enumerate()
            .map(|(depth, id)| {
                if depth == target_depth {
                    InodeLockRequest::write(depth, *id)
                } else {
                    InodeLockRequest::read(depth, *id)
                }
            })
            .collect()
    }
}
