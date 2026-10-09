use curvine_model::{FileStatus, StorageState, TtlAction};

/// Browse (`GET /api/v1/fs/list`) serialises `FileStatus` directly. The expire
/// time and storage-location columns depend on these JSON field names staying
/// stable (`mtime`, `storage_policy.ttl_ms`, `ttl_action`, `state`).
#[test]
fn file_status_json_exposes_ttl_and_storage_state() {
    let mut status = FileStatus::with_name(1, "cached.log".to_string(), false);
    status.mtime = 1_700_000_000_000;
    status.storage_policy.ttl_ms = 86_400_000;
    status.storage_policy.ttl_action = TtlAction::Delete;
    status.storage_policy.state = StorageState::Both;

    let value = serde_json::to_value(&status).expect("FileStatus should serialise");
    let policy = value
        .get("storage_policy")
        .expect("storage_policy must be present");

    assert_eq!(
        value.get("mtime").and_then(|v| v.as_i64()),
        Some(1_700_000_000_000)
    );
    assert_eq!(
        policy.get("ttl_ms").and_then(|v| v.as_i64()),
        Some(86_400_000)
    );
    assert_eq!(
        policy.get("ttl_action").and_then(|v| v.as_str()),
        Some("Delete")
    );
    assert_eq!(policy.get("state").and_then(|v| v.as_str()), Some("Both"));
}

#[test]
fn file_status_is_expired_matches_mtime_plus_ttl() {
    let mut status = FileStatus::with_name(1, "stale.log".to_string(), false);
    status.mtime = 1;
    status.storage_policy.ttl_ms = 1;
    status.storage_policy.ttl_action = TtlAction::Delete;
    assert!(status.is_expired());

    status.storage_policy.ttl_action = TtlAction::None;
    assert!(!status.is_expired());

    status.storage_policy.ttl_action = TtlAction::Delete;
    status.storage_policy.ttl_ms = 0;
    assert!(!status.is_expired());
}
