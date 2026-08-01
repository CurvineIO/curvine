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

use curvine_common::state::{FileStatus, StoragePolicy, StorageState};

use super::planner::{needs_source_status_refresh, unchanged_load_target};

fn source_status(mtime: i64, len: i64) -> FileStatus {
    FileStatus {
        mtime,
        len,
        is_complete: true,
        ..Default::default()
    }
}

fn target_status(mtime: i64, len: i64) -> FileStatus {
    FileStatus {
        len,
        is_complete: true,
        storage_policy: StoragePolicy {
            ufs_mtime: mtime,
            state: StorageState::Both,
            ..Default::default()
        },
        ..Default::default()
    }
}

#[test]
fn unchanged_load_output_is_skipped_only_when_source_fingerprint_matches() {
    let source = source_status(10, 1024);
    let target = target_status(10, 1024);

    assert!(unchanged_load_target(&source, Some(&target)));
    assert!(!unchanged_load_target(
        &source_status(11, 1024),
        Some(&target)
    ));
    assert!(!unchanged_load_target(
        &source_status(10, 2048),
        Some(&target)
    ));
}

#[test]
fn incomplete_or_cv_missing_target_is_not_skipped() {
    let source = source_status(10, 1024);
    let mut incomplete = target_status(10, 1024);
    incomplete.is_complete = false;
    assert!(!unchanged_load_target(&source, Some(&incomplete)));

    let mut cv_missing = target_status(10, 1024);
    cv_missing.storage_policy.state = StorageState::Ufs;
    assert!(!unchanged_load_target(&source, Some(&cv_missing)));
}

#[test]
fn equal_size_timestamp_mismatch_refreshes_source_status_before_planning() {
    let target = target_status(10, 1024);

    assert!(needs_source_status_refresh(
        &source_status(11, 1024),
        Some(&target)
    ));
    assert!(!needs_source_status_refresh(
        &source_status(11, 2048),
        Some(&target)
    ));
}
