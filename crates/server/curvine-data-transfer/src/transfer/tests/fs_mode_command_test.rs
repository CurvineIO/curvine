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

//! Submit-time validation of fs_mode export batches.

use super::plan::validate_fs_mode_mapping;
use super::service::{validate_transfer_options, FS_MODE_BATCH_MAX_FILES};
use curvine_fs_api::Path;
use curvine_model::{FileGen, FsModeFile, MountInfo, TransferCommand, TransferKind, WriteType};

fn file(source: &str, target: &str) -> FsModeFile {
    FsModeFile {
        source: source.to_string(),
        target: target.to_string(),
        gen: FileGen::new(9001, 4096, 1000, 77),
    }
}

fn command(submitter: &str, files: Vec<FsModeFile>) -> TransferCommand {
    TransferCommand {
        kind: TransferKind::Export,
        source_path: "cv:///mnt".to_string(),
        target_path: "s3://bkt/mnt".to_string(),
        client_request_id: "req".to_string(),
        submitter: submitter.to_string(),
        tenant: String::new(),
        options: Default::default(),
        fs_mode_files: files,
    }
}

#[test]
fn a_well_formed_batch_passes() {
    let cmd = command("fs-mode:c1", vec![file("cv:///mnt/a", "s3://bkt/mnt/a")]);
    assert!(validate_transfer_options(&cmd).is_ok());
}

#[test]
fn fs_mode_submitter_and_files_must_appear_together() {
    // A plain command cannot borrow the scheduling exemption.
    assert!(validate_transfer_options(&command("fs-mode:c1", vec![])).is_err());
    // A batch cannot run under an ordinary submitter.
    let cmd = command("curvine-cli", vec![file("cv:///mnt/a", "s3://bkt/mnt/a")]);
    assert!(validate_transfer_options(&cmd).is_err());
    // Neither: unaffected.
    assert!(validate_transfer_options(&command("curvine-cli", vec![])).is_ok());
}

#[test]
fn batch_targets_must_match_the_actual_mount() {
    let mount = MountInfo {
        cv_path: "/mnt".into(),
        ufs_path: "s3://bkt/mnt".into(),
        mount_id: 1,
        write_type: WriteType::FsMode,
        ..Default::default()
    };
    let nested = MountInfo {
        cv_path: "/mnt/nested".into(),
        ufs_path: "s3://other/nested".into(),
        mount_id: 2,
        write_type: WriteType::FsMode,
        ..Default::default()
    };
    let mounts = vec![mount.clone(), nested];
    let source = Path::from_str("cv:///mnt/a").unwrap();
    assert!(validate_fs_mode_mapping(
        &mount,
        &mounts,
        &source,
        &Path::from_str("s3://bkt/mnt/a").unwrap()
    )
    .is_ok());
    assert!(validate_fs_mode_mapping(
        &mount,
        &mounts,
        &source,
        &Path::from_str("s3://bkt/private/a").unwrap()
    )
    .is_err());
    assert!(validate_fs_mode_mapping(
        &mount,
        &mounts,
        &Path::from_str("cv:///mnt/nested/a").unwrap(),
        &Path::from_str("s3://bkt/mnt/nested/a").unwrap()
    )
    .is_err());
}

#[test]
fn batch_is_export_only() {
    let mut cmd = command("fs-mode:c1", vec![file("cv:///mnt/a", "s3://bkt/mnt/a")]);
    cmd.kind = TransferKind::Load;
    assert!(validate_transfer_options(&cmd).is_err());
}

#[test]
fn batch_rejects_wrong_direction_and_oversize() {
    let flipped = command("fs-mode:c1", vec![file("s3://bkt/mnt/a", "cv:///mnt/a")]);
    assert!(validate_transfer_options(&flipped).is_err());

    let huge = command(
        "fs-mode:c1",
        (0..=FS_MODE_BATCH_MAX_FILES)
            .map(|_| file("cv:///mnt/a", "s3://bkt/mnt/a"))
            .collect(),
    );
    assert!(validate_transfer_options(&huge).is_err());
}
