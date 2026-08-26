use curvine_model::{AccessMode, MountInfo, Provider, StorageType, TtlAction, TtlType, WriteType};
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Serialize)]
struct LegacyMountInfo {
    cv_path: String,
    ufs_path: String,
    mount_id: u32,
    properties: HashMap<String, String>,
    ttl_ms: i64,
    ttl_action: TtlAction,
    read_verify_ufs: bool,
    storage_type: Option<StorageType>,
    block_size: Option<i64>,
    replicas: Option<i32>,
    write_type: WriteType,
    provider: Option<Provider>,
}

#[derive(Debug, Serialize)]
struct LegacyMountInfoWithAutoCacheAccessMode {
    cv_path: String,
    ufs_path: String,
    mount_id: u32,
    properties: HashMap<String, String>,
    ttl_ms: i64,
    ttl_action: TtlAction,
    read_verify_ufs: bool,
    storage_type: Option<StorageType>,
    block_size: Option<i64>,
    replicas: Option<i32>,
    write_type: WriteType,
    provider: Option<Provider>,
    auto_cache: bool,
    access_mode: AccessMode,
}

/// Layout persisted between the `write_cache` addition and this PR's
/// `ttl_type` addition (i.e. the layout written by the PR base).
#[derive(Debug, Serialize)]
struct LegacyMountInfoWithWriteCache {
    cv_path: String,
    ufs_path: String,
    mount_id: u32,
    properties: HashMap<String, String>,
    ttl_ms: i64,
    ttl_action: TtlAction,
    read_verify_ufs: bool,
    storage_type: Option<StorageType>,
    block_size: Option<i64>,
    replicas: Option<i32>,
    write_type: WriteType,
    provider: Option<Provider>,
    auto_cache: bool,
    access_mode: AccessMode,
    write_cache: bool,
}

fn legacy_mount_info() -> LegacyMountInfo {
    let mut properties = HashMap::new();
    properties.insert("k".to_string(), "v".to_string());

    LegacyMountInfo {
        cv_path: "/acc-hdfs".to_string(),
        ufs_path: "oss://bucket/prefix".to_string(),
        mount_id: 7,
        properties,
        ttl_ms: 86_400_000,
        ttl_action: TtlAction::Delete,
        read_verify_ufs: true,
        storage_type: Some(StorageType::Disk),
        block_size: Some(134_217_728),
        replicas: Some(3),
        write_type: WriteType::CacheMode,
        provider: Some(Provider::OssHdfs),
    }
}

fn legacy_mount_info_with_auto_cache_access_mode() -> LegacyMountInfoWithAutoCacheAccessMode {
    let info = legacy_mount_info();
    LegacyMountInfoWithAutoCacheAccessMode {
        cv_path: info.cv_path,
        ufs_path: info.ufs_path,
        mount_id: info.mount_id,
        properties: info.properties,
        ttl_ms: info.ttl_ms,
        ttl_action: info.ttl_action,
        read_verify_ufs: info.read_verify_ufs,
        storage_type: info.storage_type,
        block_size: info.block_size,
        replicas: info.replicas,
        write_type: info.write_type,
        provider: info.provider,
        auto_cache: false,
        access_mode: AccessMode::ReadWrite,
    }
}

fn legacy_mount_info_with_write_cache() -> LegacyMountInfoWithWriteCache {
    let info = legacy_mount_info_with_auto_cache_access_mode();
    LegacyMountInfoWithWriteCache {
        cv_path: info.cv_path,
        ufs_path: info.ufs_path,
        mount_id: info.mount_id,
        properties: info.properties,
        ttl_ms: info.ttl_ms,
        ttl_action: info.ttl_action,
        read_verify_ufs: info.read_verify_ufs,
        storage_type: info.storage_type,
        block_size: info.block_size,
        replicas: info.replicas,
        write_type: info.write_type,
        provider: info.provider,
        auto_cache: info.auto_cache,
        access_mode: info.access_mode,
        write_cache: true,
    }
}

fn current_mount_info() -> MountInfo {
    let info = legacy_mount_info();
    MountInfo {
        cv_path: info.cv_path,
        ufs_path: info.ufs_path,
        mount_id: info.mount_id,
        properties: info.properties,
        ttl_ms: info.ttl_ms,
        ttl_action: info.ttl_action,
        read_verify_ufs: info.read_verify_ufs,
        storage_type: info.storage_type,
        block_size: info.block_size,
        replicas: info.replicas,
        write_type: info.write_type,
        provider: info.provider,
        auto_cache: true,
        access_mode: AccessMode::ReadOnly,
        write_cache: false,
        ttl_type: TtlType::Mtime,
    }
}

#[test]
fn decode_persisted_mount_info_accepts_legacy_bytes() {
    let bytes = bincode::serialize(&legacy_mount_info()).unwrap();
    let decoded = MountInfo::decode_persisted(&bytes).unwrap();

    assert_eq!(decoded.cv_path, "/acc-hdfs");
    assert_eq!(decoded.ufs_path, "oss://bucket/prefix");
    assert_eq!(decoded.mount_id, 7);
    assert_eq!(decoded.properties.get("k").map(String::as_str), Some("v"));
    assert_eq!(decoded.ttl_ms, 86_400_000);
    assert_eq!(decoded.ttl_action, TtlAction::Delete);
    assert!(decoded.read_verify_ufs);
    assert_eq!(decoded.storage_type, Some(StorageType::Disk));
    assert_eq!(decoded.block_size, Some(134_217_728));
    assert_eq!(decoded.replicas, Some(3));
    assert_eq!(decoded.write_type, WriteType::CacheMode);
    assert_eq!(decoded.provider, Some(Provider::OssHdfs));
    assert!(decoded.auto_cache);
    assert_eq!(decoded.access_mode, AccessMode::ReadOnly);
    assert!(!decoded.write_cache);
}

#[test]
fn decode_persisted_mount_info_accepts_previous_current_bytes() {
    let bytes = bincode::serialize(&legacy_mount_info_with_auto_cache_access_mode()).unwrap();
    let decoded = MountInfo::decode_persisted(&bytes).unwrap();

    assert!(!decoded.auto_cache);
    assert_eq!(decoded.access_mode, AccessMode::ReadWrite);
    assert!(!decoded.write_cache);
}

/// Payload written by the immediate parent of this PR: it ends at
/// `write_cache` and has no `ttl_type`. Both earlier legacy layouts reject
/// the trailing `write_cache` byte, so a dedicated fallback is required or
/// masters with existing mountpoints fail to restore the mount table.
#[test]
fn decode_persisted_mount_info_accepts_write_cache_era_bytes() {
    let bytes = bincode::serialize(&legacy_mount_info_with_write_cache()).unwrap();

    // Sanity check: the current struct alone hits EOF on these bytes.
    assert!(bincode::deserialize::<MountInfo>(&bytes).is_err());

    let decoded = MountInfo::decode_persisted(&bytes).unwrap();

    // All write_cache-era fields survive; only ttl_type is defaulted.
    assert_eq!(decoded.cv_path, "/acc-hdfs");
    assert_eq!(decoded.ufs_path, "oss://bucket/prefix");
    assert_eq!(decoded.mount_id, 7);
    assert_eq!(decoded.properties.get("k").map(String::as_str), Some("v"));
    assert_eq!(decoded.ttl_ms, 86_400_000);
    assert_eq!(decoded.ttl_action, TtlAction::Delete);
    assert!(decoded.read_verify_ufs);
    assert_eq!(decoded.storage_type, Some(StorageType::Disk));
    assert_eq!(decoded.block_size, Some(134_217_728));
    assert_eq!(decoded.replicas, Some(3));
    assert_eq!(decoded.write_type, WriteType::CacheMode);
    assert_eq!(decoded.provider, Some(Provider::OssHdfs));
    assert!(!decoded.auto_cache);
    assert_eq!(decoded.access_mode, AccessMode::ReadWrite);
    assert!(decoded.write_cache);
    assert_eq!(decoded.ttl_type, TtlType::Mtime);
}

#[test]
fn decode_persisted_mount_info_keeps_current_fields() {
    let mut current = current_mount_info();
    current.auto_cache = false;
    current.access_mode = AccessMode::ReadWrite;
    current.write_cache = true;

    let bytes = bincode::serialize(&current).unwrap();
    let decoded = MountInfo::decode_persisted(&bytes).unwrap();

    assert!(!decoded.auto_cache);
    assert_eq!(decoded.access_mode, AccessMode::ReadWrite);
    assert!(decoded.write_cache);
    assert_eq!(decoded.cv_path, current.cv_path);
    assert_eq!(decoded.ufs_path, current.ufs_path);
    assert_eq!(decoded.mount_id, current.mount_id);
}

#[test]
fn decode_persisted_mount_info_rejects_trailing_legacy_bytes() {
    let mut bytes = bincode::serialize(&legacy_mount_info()).unwrap();
    bytes.push(1);

    assert!(MountInfo::decode_persisted(&bytes).is_err());
}
