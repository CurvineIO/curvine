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

//! Trust-boundary tests for directory metadata on cache_mode mounts.
//!
//! Covers docs/plans/2026-10-10-ufs-metadata-trust-boundary.md D1: directory
//! stat on read-only cache_mode mounts trusts the CV snapshot within TTL,
//! mirroring the file contract; files, read-write mounts, verify mounts and
//! expired entries keep the existing probe semantics.

use crate::unified::mount_cache::MountValue;
use crate::unified::unified_filesystem::{CacheValidity, UnifiedFileSystem};
use curvine_error::FsResult;
use curvine_fs_api::Path;
use curvine_model::{
    AccessMode, FileStatus, MountInfo, StoragePolicy, StorageState, TtlAction, WriteType,
};
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    AsyncRuntime::new("metadata-trust-test", 1, 1).block_on(fut)
}

fn ro_cache_mount() -> MountInfo {
    MountInfo {
        cv_path: "/bucket/model".to_string(),
        ufs_path: "s3://bucket/model".to_string(),
        mount_id: 1,
        properties: HashMap::new(),
        ttl_ms: 0,
        ttl_action: TtlAction::None,
        read_verify_ufs: false,
        storage_type: None,
        block_size: None,
        replicas: None,
        write_type: WriteType::CacheMode,
        provider: None,
        auto_cache: true,
        access_mode: AccessMode::ReadOnly,
        write_cache: false,
        ttl_type: Default::default(),
    }
}

fn mount_value(info: MountInfo) -> MountValue {
    MountValue::new(info).unwrap()
}

fn dir_status(mtime: i64, ttl_ms: i64) -> FileStatus {
    let mut status = FileStatus::with_name(579816926, "emb".to_string(), true);
    status.mtime = mtime;
    status.storage_policy = StoragePolicy {
        ttl_ms,
        ttl_action: if ttl_ms > 0 {
            TtlAction::Delete
        } else {
            TtlAction::None
        },
        ..Default::default()
    };
    status
}

fn file_status(ufs_mtime: i64) -> FileStatus {
    let mut status = FileStatus::with_name(42, "model.pt".to_string(), false);
    status.mtime = ufs_mtime;
    status.storage_policy.ufs_mtime = ufs_mtime;
    status.storage_policy.state = StorageState::Both;
    status
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_millis() as i64
}

async fn check(
    status: &FileStatus,
    ufs_path: &Path,
    mount: &MountValue,
) -> FsResult<CacheValidity> {
    UnifiedFileSystem::check_cache_validity(status, ufs_path, mount).await
}

#[test]
fn directory_on_read_only_cache_mode_mount_trusts_cv_snapshot() {
    block_on(async {
        let mount = mount_value(ro_cache_mount());
        let status = dir_status(now_millis(), 30 * 24 * 3600 * 1000);
        let ufs_path = Path::from_str("s3://bucket/model/emb").unwrap();

        match check(&status, &ufs_path, &mount).await.unwrap() {
            CacheValidity::Valid => {}
            other => panic!(
                "directory snapshot within TTL must be trusted, got {:?}",
                other
            ),
        }
    });
}

#[test]
fn directory_trust_is_rejected_after_ttl_expiry() {
    block_on(async {
        let mount = mount_value(ro_cache_mount());
        let created = now_millis() - 40 * 24 * 3600 * 1000;
        let status = dir_status(created, 30 * 24 * 3600 * 1000);
        let ufs_path = Path::from_str("s3://bucket/model/emb").unwrap();

        match check(&status, &ufs_path, &mount).await.unwrap() {
            CacheValidity::Invalid(None) => {}
            other => panic!("expired directory must fall back to probe, got {:?}", other),
        }
    });
}

#[test]
fn verify_enabled_mount_keeps_directory_probe_semantics() {
    block_on(async {
        // Use a real local dir so the verify probe resolves without network
        // backends; the assertion is that D1 does NOT trust the CV snapshot
        // and the UFS probe still runs (its status wins over CV).
        let tmp = std::env::temp_dir().join(format!(
            "cv-trust-verify-{}",
            curvine_runtime::common::Utils::rand_id()
        ));
        std::fs::create_dir_all(tmp.join("emb")).unwrap();

        let mut info = ro_cache_mount();
        info.read_verify_ufs = true;
        info.ufs_path = format!("file://{}", tmp.display());
        let mount = mount_value(info);
        let status = dir_status(now_millis(), 0);
        let ufs_path = Path::from_str(format!("{}/emb", tmp.display())).unwrap();

        // cv_valid(None) rejects ufs_mtime==0 (no UFS fingerprint is ever
        // stored for directories), so verify mounts keep probing exactly as
        // before D1. The probe returns the UFS status (dir exists locally).
        match check(&status, &ufs_path, &mount).await.unwrap() {
            CacheValidity::Invalid(Some(ufs_status)) => {
                assert!(ufs_status.is_dir);
            }
            other => panic!(
                "verify mount must keep probing directories, got {:?}",
                other
            ),
        }

        std::fs::remove_dir_all(&tmp).unwrap();
    });
}

#[test]
fn read_write_cache_mode_mount_keeps_directory_probe_semantics() {
    block_on(async {
        let mut info = ro_cache_mount();
        info.access_mode = AccessMode::ReadWrite;
        let mount = mount_value(info);
        let status = dir_status(now_millis(), 0);
        let ufs_path = Path::from_str("s3://bucket/model/emb").unwrap();

        match check(&status, &ufs_path, &mount).await.unwrap() {
            CacheValidity::Invalid(None) => {}
            other => panic!(
                "read-write mount must not trust directories, got {:?}",
                other
            ),
        }
    });
}

#[test]
fn only_read_only_cache_mode_mounts_are_eligible() {
    assert!(ro_cache_mount().is_read_only_cache_mode());

    let mut fs_mode = ro_cache_mount();
    fs_mode.write_type = WriteType::FsMode;
    assert!(!fs_mode.is_read_only_cache_mode());
}

#[test]
fn file_semantics_are_unchanged_for_read_only_cache_mode_mount() {
    block_on(async {
        let mount = mount_value(ro_cache_mount());
        let ufs_mtime = now_millis() - 1_000;
        let status = file_status(ufs_mtime);
        let ufs_path = Path::from_str("s3://bucket/model/model.pt").unwrap();

        match check(&status, &ufs_path, &mount).await.unwrap() {
            CacheValidity::Valid => {}
            other => panic!(
                "cached file with ufs fingerprint must stay valid, got {:?}",
                other
            ),
        }
    });
}

#[test]
fn directory_trust_does_not_relax_shared_cv_valid() {
    block_on(async {
        let status = dir_status(now_millis(), 0);
        assert!(
            !status.cv_valid(None),
            "Transfer must keep requiring a fingerprint"
        );
        let mount = mount_value(ro_cache_mount());
        let path = Path::from_str("s3://bucket/model/emb").unwrap();
        assert!(matches!(
            check(&status, &path, &mount).await.unwrap(),
            CacheValidity::Valid
        ));
    });
}

#[test]
fn ufs_only_directory_is_not_trusted() {
    block_on(async {
        let mut status = dir_status(now_millis(), 0);
        status.storage_policy.state = StorageState::Ufs;
        let mount = mount_value(ro_cache_mount());
        let path = Path::from_str("s3://bucket/model/emb").unwrap();
        assert!(matches!(
            check(&status, &path, &mount).await.unwrap(),
            CacheValidity::Invalid(None)
        ));
    });
}

#[test]
fn file_without_ufs_fingerprint_still_requires_a_probe() {
    block_on(async {
        let status = file_status(0);
        let mount = mount_value(ro_cache_mount());
        let path = Path::from_str("s3://bucket/model/model.pt").unwrap();
        assert!(matches!(
            check(&status, &path, &mount).await.unwrap(),
            CacheValidity::Invalid(None)
        ));
    });
}

#[test]
fn directory_without_ttl_keeps_the_existing_no_expiry_semantics() {
    block_on(async {
        let status = dir_status(0, 0);
        let mount = mount_value(ro_cache_mount());
        let path = Path::from_str("s3://bucket/model/emb").unwrap();
        assert!(matches!(
            check(&status, &path, &mount).await.unwrap(),
            CacheValidity::Valid
        ));
    });
}
