use curvine_proto::{
    CompatibilityModeProto, ComponentInfoProto, GetFilesystemInfoRequest,
    GetFilesystemInfoResponse, ServerCompatibilityInfoProto,
};
use prost::Message;

fn sample_component_info() -> ComponentInfoProto {
    ComponentInfoProto {
        component: Some("master".to_string()),
        release_version: Some("0.4.0-alpha".to_string()),
        git_commit: Some("359fce7d982a15f09c3b4e0b2e62fee4229609dd".to_string()),
        git_tag: Some("v0.4.0-alpha".to_string()),
        git_branch: Some("main".to_string()),
        protocol_version: Some(1),
        min_protocol_version: Some(1),
        capabilities: vec!["transfer".to_string(), "batch-write".to_string()],
    }
}

/// A legacy client's view of GetFilesystemInfoRequest: no component_info field.
#[derive(Clone, PartialEq, ::prost::Message)]
struct LegacyGetFilesystemInfoRequest {}

/// A legacy client's view of GetFilesystemInfoResponse: business fields 1-10
/// (the worker list fields 11-14 are omitted since the compatibility-skip
/// behavior under test does not depend on them), no compatibility field on the
/// reserved 1000+ range.
#[derive(Clone, PartialEq, ::prost::Message)]
struct LegacyGetFilesystemInfoResponse {
    #[prost(string, required, tag = "1")]
    active_master: String,
    #[prost(string, repeated, tag = "2")]
    journal_nodes: Vec<String>,
    #[prost(int64, required, tag = "3")]
    inode_dir_num: i64,
    #[prost(int64, required, tag = "4")]
    inode_file_num: i64,
    #[prost(int64, required, tag = "5")]
    block_num: i64,
    #[prost(int64, required, tag = "6")]
    capacity: i64,
    #[prost(int64, required, tag = "7")]
    available: i64,
    #[prost(int64, required, tag = "8")]
    fs_used: i64,
    #[prost(int64, required, tag = "9")]
    non_fs_used: i64,
    #[prost(int64, required, tag = "10")]
    reserved_bytes: i64,
}

fn sample_response_with_compatibility() -> GetFilesystemInfoResponse {
    GetFilesystemInfoResponse {
        active_master: "master-0".to_string(),
        journal_nodes: vec!["master-0".to_string(), "master-1".to_string()],
        inode_dir_num: 10,
        inode_file_num: 20,
        block_num: 30,
        capacity: 1000,
        available: 500,
        fs_used: 300,
        non_fs_used: 200,
        reserved_bytes: 0,
        live_workers: vec![],
        blacklist_workers: vec![],
        decommission_workers: vec![],
        lost_workers: vec![],
        allocatable_capacity: Some(600),
        allocatable_available: Some(300),
        compatibility: Some(ServerCompatibilityInfoProto {
            server: sample_component_info(),
            min_worker_version: None,
            min_client_version: None,
            compatibility_mode: CompatibilityModeProto::Diagnose as i32,
            blocked_versions: vec![],
        }),
    }
}

#[test]
fn test_get_filesystem_info_request_component_info_round_trip() {
    // New client reports its own structured version on the reserved 1000+ range.
    let req = GetFilesystemInfoRequest {
        component_info: Some(sample_component_info()),
    };

    let encoded = req.encode_to_vec();
    let decoded = GetFilesystemInfoRequest::decode(encoded.as_slice()).unwrap();

    let info = decoded.component_info.unwrap();
    assert_eq!(info.component, Some("master".to_string()));
    assert_eq!(info.release_version, Some("0.4.0-alpha".to_string()));
    assert_eq!(info.protocol_version, Some(1));
    assert_eq!(info.min_protocol_version, Some(1));
    assert_eq!(info.capabilities.len(), 2);
}

#[test]
fn test_get_filesystem_info_request_legacy_empty_decodes() {
    // A legacy client sends an empty request (no component_info). The new
    // master must decode it fine and see no component info.
    let encoded = LegacyGetFilesystemInfoRequest {}.encode_to_vec();
    let decoded = GetFilesystemInfoRequest::decode(encoded.as_slice()).unwrap();
    assert!(decoded.component_info.is_none());
}

#[test]
fn test_get_filesystem_info_response_compatibility_round_trip() {
    // New master -> new client: full handshake round-trips.
    let rep = sample_response_with_compatibility();

    let encoded = rep.encode_to_vec();
    let decoded = GetFilesystemInfoResponse::decode(encoded.as_slice()).unwrap();

    assert_eq!(decoded.active_master, "master-0");
    assert_eq!(decoded.inode_file_num, 20);
    assert_eq!(decoded.allocatable_capacity, Some(600));
    assert_eq!(decoded.allocatable_available, Some(300));
    let compat = decoded.compatibility.unwrap();
    assert_eq!(compat.server.component, Some("master".to_string()));
    assert_eq!(
        compat.compatibility_mode,
        CompatibilityModeProto::Diagnose as i32
    );
    assert!(compat.min_worker_version.is_none());
    assert!(compat.min_client_version.is_none());
    assert!(compat.blocked_versions.is_empty());
}

#[test]
fn test_legacy_master_response_decodes_without_compatibility() {
    // Old master + new client: a legacy response with only business fields
    // (no compatibility) must decode; the client treats absence as legacy.
    let legacy = LegacyGetFilesystemInfoResponse {
        active_master: "old-master".to_string(),
        journal_nodes: vec!["old-master".to_string()],
        inode_dir_num: 1,
        inode_file_num: 2,
        block_num: 3,
        capacity: 100,
        available: 50,
        fs_used: 30,
        non_fs_used: 20,
        reserved_bytes: 0,
    };

    let encoded = legacy.encode_to_vec();
    let decoded = GetFilesystemInfoResponse::decode(encoded.as_slice()).unwrap();

    assert_eq!(decoded.active_master, "old-master");
    assert_eq!(decoded.inode_file_num, 2);
    assert!(decoded.compatibility.is_none());
    // Legacy master carries no allocatable fields; clients fall back to total.
    assert!(decoded.allocatable_capacity.is_none());
    assert!(decoded.allocatable_available.is_none());
}

#[test]
fn test_new_master_response_decoded_by_legacy_client() {
    // New master + old client: the legacy client's struct does not know the
    // compatibility field (1000); it must skip it and keep business fields.
    let rep = sample_response_with_compatibility();
    let encoded = rep.encode_to_vec();

    let legacy = LegacyGetFilesystemInfoResponse::decode(encoded.as_slice()).unwrap();

    assert_eq!(legacy.active_master, "master-0");
    assert_eq!(
        legacy.journal_nodes,
        vec!["master-0".to_string(), "master-1".to_string()]
    );
    assert_eq!(legacy.inode_dir_num, 10);
    assert_eq!(legacy.inode_file_num, 20);
    assert_eq!(legacy.block_num, 30);
    assert_eq!(legacy.capacity, 1000);
    assert_eq!(legacy.available, 500);
    assert_eq!(legacy.fs_used, 300);
    assert_eq!(legacy.non_fs_used, 200);
    assert_eq!(legacy.reserved_bytes, 0);
}

#[test]
fn test_legacy_client_request_with_new_field_ignored() {
    // Old master + new client: the new client's request carries component_info
    // on field 1000; a legacy master (struct without that field) must skip it.
    let req = GetFilesystemInfoRequest {
        component_info: Some(sample_component_info()),
    };
    let encoded = req.encode_to_vec();

    let legacy = LegacyGetFilesystemInfoRequest::decode(encoded.as_slice()).unwrap();
    assert_eq!(legacy, LegacyGetFilesystemInfoRequest {});
}
