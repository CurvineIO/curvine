use curvine_proto::{
    ComponentInfoProto, HeartbeatStatusProto, WorkerAddressProto, WorkerHeartbeatRequest,
    WorkerInfoProto,
};
use prost::Message;

fn sample_component_info() -> ComponentInfoProto {
    ComponentInfoProto {
        component: Some("worker".to_string()),
        release_version: Some("0.4.0-alpha".to_string()),
        git_commit: Some("359fce7d982a15f09c3b4e0b2e62fee4229609dd".to_string()),
        git_tag: Some("v0.4.0-alpha".to_string()),
        git_branch: Some("main".to_string()),
        protocol_version: Some(1),
        min_protocol_version: Some(1),
        capabilities: vec!["transfer".to_string(), "batch-write".to_string()],
    }
}

fn sample_worker_address(worker_id: u32) -> WorkerAddressProto {
    WorkerAddressProto {
        worker_id,
        hostname: "worker-host".to_string(),
        ip_addr: "127.0.0.1".to_string(),
        rpc_port: 1234,
        web_port: 5678,
    }
}

/// A legacy worker's view of WorkerHeartbeatRequest: all required business
/// fields a pre-change worker actually sent, no component_info on the
/// reserved 1000+ range.
#[derive(Clone, PartialEq, ::prost::Message)]
struct LegacyWorkerHeartbeatRequest {
    #[prost(string, required, tag = "1")]
    cluster_id: String,
    #[prost(uint32, required, tag = "2")]
    worker_id: u32,
    #[prost(int64, required, tag = "3")]
    fs_ctime: i64,
    #[prost(message, required, tag = "4")]
    address: WorkerAddressProto,
    #[prost(int32, required, tag = "5")]
    failed_dirs: i32,
    #[prost(enumeration = "HeartbeatStatusProto", required, tag = "6")]
    status: i32,
    #[prost(string, required, tag = "7")]
    software_version: String,
}

/// Append a raw varint field (field number, wire type 0) to a wire buffer.
fn push_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
}

/// A legacy master's view of WorkerInfoProto: all required business fields a
/// pre-change master actually sent, no component_info on the reserved 1000+
/// range.
#[derive(Clone, PartialEq, ::prost::Message)]
struct LegacyWorkerInfoProto {
    #[prost(message, required, tag = "1")]
    address: WorkerAddressProto,
    #[prost(int64, required, tag = "2")]
    capacity: i64,
    #[prost(int64, required, tag = "3")]
    available: i64,
    #[prost(int64, required, tag = "4")]
    fs_used: i64,
    #[prost(int64, required, tag = "5")]
    non_fs_used: i64,
    #[prost(uint64, required, tag = "6")]
    last_update: u64,
    #[prost(int64, required, tag = "8", default = "0")]
    reserved_bytes: i64,
    #[prost(string, optional, tag = "16", default = "")]
    software_version: Option<String>,
}

#[test]
fn test_worker_heartbeat_component_info_round_trip() {
    // New worker -> new master: the structured component info survives the
    // wire on the reserved 1000+ range of the heartbeat.
    let req = WorkerHeartbeatRequest {
        component_info: Some(sample_component_info()),
        ..Default::default()
    };

    let encoded = req.encode_to_vec();
    let decoded = WorkerHeartbeatRequest::decode(encoded.as_slice()).unwrap();

    let info = decoded.component_info.unwrap();
    assert_eq!(info.component, Some("worker".to_string()));
    assert_eq!(info.release_version, Some("0.4.0-alpha".to_string()));
    assert_eq!(info.protocol_version, Some(1));
    assert_eq!(info.min_protocol_version, Some(1));
    assert_eq!(info.capabilities.len(), 2);
}

#[test]
fn test_worker_heartbeat_legacy_empty_decodes() {
    // Old worker + new master: a legacy heartbeat without component_info must
    // decode; the master treats absence as a legacy/unknown peer.
    let legacy = LegacyWorkerHeartbeatRequest {
        cluster_id: "test-cluster".to_string(),
        worker_id: 7,
        fs_ctime: 123_456,
        address: sample_worker_address(7),
        failed_dirs: 0,
        status: HeartbeatStatusProto::Running as i32,
        software_version: "0.1.0".to_string(),
    };

    let encoded = legacy.encode_to_vec();
    let decoded = WorkerHeartbeatRequest::decode(encoded.as_slice()).unwrap();
    assert!(decoded.component_info.is_none());
    assert_eq!(decoded.software_version, "0.1.0");
    assert_eq!(decoded.worker_id, 7);
    assert_eq!(decoded.fs_ctime, 123_456);
    assert_eq!(decoded.address.worker_id, 7);
    assert_eq!(decoded.failed_dirs, 0);
    assert_eq!(decoded.status, HeartbeatStatusProto::Running as i32);
}

#[test]
fn test_worker_info_proto_component_info_round_trip() {
    // Master -> CLI: WorkerInfoProto carries the structured version so the
    // report command can display it.
    let proto = WorkerInfoProto {
        software_version: Some("0.1.0-test".to_string()),
        component_info: Some(sample_component_info()),
        ..Default::default()
    };

    let encoded = proto.encode_to_vec();
    let decoded = WorkerInfoProto::decode(encoded.as_slice()).unwrap();

    let info = decoded.component_info.unwrap();
    assert_eq!(info.component, Some("worker".to_string()));
    assert_eq!(info.release_version, Some("0.4.0-alpha".to_string()));
    assert_eq!(decoded.software_version.as_deref(), Some("0.1.0-test"));
}

#[test]
fn test_worker_info_proto_legacy_empty_decodes() {
    // Old master + new CLI: a legacy WorkerInfoProto without component_info
    // must decode; absence is displayed as legacy/unknown.
    let legacy = LegacyWorkerInfoProto {
        address: sample_worker_address(7),
        capacity: 100,
        available: 50,
        fs_used: 30,
        non_fs_used: 20,
        last_update: 1_700_000_000_000,
        reserved_bytes: 0,
        software_version: Some("0.1.0".to_string()),
    };

    let encoded = legacy.encode_to_vec();
    let decoded = WorkerInfoProto::decode(encoded.as_slice()).unwrap();
    assert!(decoded.component_info.is_none());
    assert_eq!(decoded.software_version.as_deref(), Some("0.1.0"));
    assert_eq!(decoded.address.worker_id, 7);
    assert_eq!(decoded.capacity, 100);
    assert_eq!(decoded.available, 50);
    assert_eq!(decoded.fs_used, 30);
    assert_eq!(decoded.non_fs_used, 20);
    assert_eq!(decoded.reserved_bytes, 0);
}

#[test]
fn test_worker_heartbeat_unknown_high_fields_are_skipped() {
    // Forward compatibility: a peer that sends fields this build does not
    // know about (e.g. a later reserved-range field beyond 1000) must be
    // decoded and those fields silently skipped — the same mechanism legacy
    // components rely on to ignore our new component_info field.
    let req = WorkerHeartbeatRequest {
        component_info: Some(sample_component_info()),
        ..Default::default()
    };

    let mut encoded = req.encode_to_vec();
    // Append an unknown length-delimited field 1001 (wire type 2).
    push_varint(&mut encoded, (1001 << 3) | 2);
    push_varint(&mut encoded, 1);
    encoded.push(0);

    let decoded = WorkerHeartbeatRequest::decode(encoded.as_slice()).unwrap();
    assert_eq!(decoded.component_info, req.component_info);
}
