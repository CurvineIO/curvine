// Copyright 2026 OPPO.
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

//! Guards the boundary between the metadata watermark and the independent UFS
//! replay watermark.
//!
//! A snapshot claims the whole applied prefix and lets the log drop it. If the
//! UFS replay has not consumed that prefix, publishing the snapshot deletes the
//! entries it still owes a copy. The replay then waits forever on a missing
//! entry, no further checkpoint is created, and the journal grows without
//! bound. These tests pin both halves of that contract.

use curvine_raft::proto::raft::{AppliedIndex, FsmState};

fn state(applied: u64, ufs_applied: u64) -> FsmState {
    let index = |index: u64| AppliedIndex {
        term: 1,
        index,
        op_id: index,
        rpc_id: 0,
    };
    FsmState {
        applied: index(applied),
        ufs_applied: index(ufs_applied),
    }
}

#[test]
fn ufs_replay_caught_up_requires_the_whole_applied_prefix() {
    assert!(
        state(0, 0).ufs_replay_caught_up(),
        "a fresh state is caught up"
    );
    assert!(state(100, 100).ufs_replay_caught_up());
    assert!(
        state(100, 101).ufs_replay_caught_up(),
        "a replay ahead of applied has nothing outstanding"
    );
    assert!(
        !state(100, 99).ufs_replay_caught_up(),
        "one unconsumed entry is still an outstanding obligation"
    );
    assert!(!state(11_238_709, 3_005_416).ufs_replay_caught_up());
}

#[test]
fn compact_never_passes_an_unsynced_prefix() {
    // A snapshot at `applied` may only drop what both watermarks passed.
    assert_eq!(state(100, 50).compact(), 49);
    assert_eq!(state(11_238_709, 3_005_416).compact(), 3_005_415);

    // Once caught up the bound is the applied prefix as usual.
    assert_eq!(state(100, 100).compact(), 99);
    assert_eq!(state(0, 0).compact(), 0);
}

#[test]
fn snapshot_publication_waits_for_the_ufs_replay() {
    // The two call sites (`gen_create_snapshot_job` and the leader snapshot
    // apply path) both gate on this predicate before materialising a snapshot.
    assert!(!state(7_028_510, 3_005_416).ufs_replay_caught_up());
    assert!(state(7_028_510, 7_028_510).ufs_replay_caught_up());
}
