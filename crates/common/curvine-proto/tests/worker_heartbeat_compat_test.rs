use curvine_proto::{ComponentInfoProto, WorkerHeartbeatRequest, WorkerInfoProto};
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

/// A legacy worker's view of WorkerHeartbeatRequest: business fields only, no
/// component_info on the reserved 1000+ range.
#[derive(Clone, PartialEq, ::prost::Message)]
struct LegacyWorkerHeartbeatRequest {
    #[prost(string, required, tag = "1")]
    cluster_id: String,
    #[prost(uint32, required, tag = "2")]
    worker_id: u32,
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

/// A legacy master's view of WorkerInfoProto: business fields only, no
/// component_info on the reserved 1000+ range.
#[derive(Clone, PartialEq, ::prost::Message)]
struct LegacyWorkerInfoProto {
    #[prost(string, required, tag = "16")]
    software_version: String,
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
        software_version: "0.1.0".to_string(),
    };

    let encoded = legacy.encode_to_vec();
    let decoded = WorkerHeartbeatRequest::decode(encoded.as_slice()).unwrap();
    assert!(decoded.component_info.is_none());
    assert_eq!(decoded.software_version, "0.1.0");
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
        software_version: "0.1.0".to_string(),
    };

    let encoded = legacy.encode_to_vec();
    let decoded = WorkerInfoProto::decode(encoded.as_slice()).unwrap();
    assert!(decoded.component_info.is_none());
    assert_eq!(decoded.software_version.as_deref(), Some("0.1.0"));
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
