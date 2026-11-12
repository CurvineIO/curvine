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

//! The public link/symlink entries must route a sharded `Fallback` to the
//! exclusive owner body instead of reporting success without executing.

use super::*;
use curvine_model::state::FileType;

#[test]
fn link_fallback_surfaces_the_real_destination_conflict() {
    let fs = test_fs("link-fallback");
    fs.create("/src", true).unwrap();
    // Destination exists: the single-parent plan is not eligible, so the entry
    // escalates to the exclusive body. The real cold body must run and report
    // the conflict — not fabricate a Done(()) from a discarded Fallback.
    fs.create("/dst", true).unwrap();

    let err = fs.link("/src", "/dst").unwrap_err();
    assert!(
        matches!(err, FsError::FileAlreadyExists(_)),
        "link fallback must run the real cold body, got {err:?}"
    );
    assert_eq!(fs.file_status("/dst").unwrap().nlink, 1);
    assert_eq!(fs.file_status("/src").unwrap().nlink, 1);
}

#[test]
fn symlink_fallback_surfaces_the_real_destination_conflict() {
    let fs = test_fs("symlink-fallback");
    fs.create("/real", true).unwrap();
    fs.create("/link", true).unwrap();

    let err = fs
        .symlink_with_owner_group("/real", "/link", false, 0o644, None, None)
        .unwrap_err();
    assert!(
        matches!(err, FsError::FileAlreadyExists(_)),
        "symlink fallback must run the real cold body, got {err:?}"
    );
    assert_eq!(fs.file_status("/link").unwrap().file_type, FileType::File);
}

#[test]
fn rename_with_a_missing_path_reports_file_not_found() {
    let fs = test_fs("rename-missing");
    fs.mkdir("/p", true).unwrap();

    // Both paths miss their last component: the same-parent comparison used
    // to treat the shared parent id as "same inode" and return Ok(true).
    let err = fs
        .rename("/p/missing1", "/p/missing2", RenameFlags::empty())
        .unwrap_err();
    assert!(
        matches!(err, FsError::FileNotFound(_)),
        "renaming two missing paths must be FileNotFound, got {err:?}"
    );
    assert!(fs.file_status("/p").is_ok());

    // A missing source with an existing destination is also a real error.
    fs.create("/p/exists", true).unwrap();
    let err = fs
        .rename("/p/missing", "/p/exists", RenameFlags::empty())
        .unwrap_err();
    assert!(matches!(err, FsError::FileNotFound(_)), "got {err:?}");
    assert!(fs.file_status("/p/exists").is_ok());

    // Existing self-rename keeps the established bool semantics.
    assert!(fs
        .rename("/p/exists", "/p/exists", RenameFlags::empty())
        .unwrap());
}
