use curvine_proto::{CompatibilityModeProto, ComponentInfoProto, ServerCompatibilityInfoProto};
use prost::Message;

fn sample_component_info() -> ComponentInfoProto {
    ComponentInfoProto {
        component: Some("worker".to_string()),
        release_version: Some("0.4.0-alpha".to_string()),
        git_commit: Some("24c848719b5b4fea74519d91cbe462bb49761b36".to_string()),
        git_tag: Some("v0.4.0-alpha".to_string()),
        git_branch: Some("main".to_string()),
        protocol_version: Some(1),
        min_protocol_version: Some(1),
        capabilities: vec![
            "short-circuit".to_string(),
            "batch-write".to_string(),
            "transfer".to_string(),
        ],
    }
}

fn sample_server_compatibility_info() -> ServerCompatibilityInfoProto {
    ServerCompatibilityInfoProto {
        server: sample_component_info(),
        min_worker_version: "0.2.0".to_string(),
        min_client_version: "0.2.0".to_string(),
        compatibility_mode: CompatibilityModeProto::Diagnose as i32,
        blocked_versions: vec!["0.2.5".to_string(), "0.2.6".to_string()],
    }
}

fn sample_server_compatibility_info_enforce() -> ServerCompatibilityInfoProto {
    ServerCompatibilityInfoProto {
        compatibility_mode: CompatibilityModeProto::Enforce as i32,
        ..sample_server_compatibility_info()
    }
}

/// Append a raw varint field (field number, wire type 0) to a wire buffer,
/// emulating a newer peer that sent fields this build does not know about.
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

/// Append an unknown length-delimited field (wire type 2) to a wire buffer.
fn push_unknown_len_delimited(buf: &mut Vec<u8>, field: u32, payload: &[u8]) {
    push_varint(buf, ((field as u64) << 3) | 2);
    push_varint(buf, payload.len() as u64);
    buf.extend_from_slice(payload);
}

/// Append an unknown fixed32 field (wire type 5) to a wire buffer.
fn push_unknown_fixed32(buf: &mut Vec<u8>, field: u32, value: u32) {
    push_varint(buf, ((field as u64) << 3) | 5);
    buf.extend_from_slice(&value.to_le_bytes());
}

#[test]
fn test_component_info_round_trip_full() {
    let info = sample_component_info();

    let encoded = info.encode_to_vec();
    let decoded = ComponentInfoProto::decode(encoded.as_slice()).unwrap();

    assert_eq!(decoded.component, Some("worker".to_string()));
    assert_eq!(decoded.release_version, Some("0.4.0-alpha".to_string()));
    assert_eq!(
        decoded.git_commit,
        Some("24c848719b5b4fea74519d91cbe462bb49761b36".to_string())
    );
    assert_eq!(decoded.git_tag, Some("v0.4.0-alpha".to_string()));
    assert_eq!(decoded.git_branch, Some("main".to_string()));
    assert_eq!(decoded.protocol_version, Some(1));
    assert_eq!(decoded.min_protocol_version, Some(1));
    assert_eq!(
        decoded.capabilities,
        vec![
            "short-circuit".to_string(),
            "batch-write".to_string(),
            "transfer".to_string()
        ]
    );
}

#[test]
fn test_component_info_round_trip_defaults() {
    // An empty component info must survive a round trip with all optional
    // fields unset (None), which is how a legacy/unknown peer is represented.
    let info = ComponentInfoProto {
        component: None,
        release_version: None,
        git_commit: None,
        git_tag: None,
        git_branch: None,
        protocol_version: None,
        min_protocol_version: None,
        capabilities: vec![],
    };

    let encoded = info.encode_to_vec();
    let decoded = ComponentInfoProto::decode(encoded.as_slice()).unwrap();

    assert_eq!(decoded, info);
    assert!(decoded.component.is_none());
    assert!(decoded.protocol_version.is_none());
    assert!(decoded.capabilities.is_empty());
}

#[test]
fn test_component_info_unknown_fields_ignored() {
    // A future peer may append cross-cutting metadata on high field numbers
    // (reserved 1000+/2000+ range). A build that does not know them must skip
    // them and keep all known fields intact (legacy compatibility).
    let mut encoded = sample_component_info().encode_to_vec();

    // Unknown varint field 1000 (protocol metadata range).
    push_varint(&mut encoded, 1000 << 3);
    push_varint(&mut encoded, 42);
    // Unknown length-delimited field 1001.
    push_unknown_len_delimited(&mut encoded, 1001, b"future-metadata");
    // Unknown fixed32 field 2000 (reserved infrastructure range).
    push_unknown_fixed32(&mut encoded, 2000, 0xdead_beef);

    let decoded = ComponentInfoProto::decode(encoded.as_slice()).unwrap();

    assert_eq!(decoded.component, Some("worker".to_string()));
    assert_eq!(decoded.release_version, Some("0.4.0-alpha".to_string()));
    assert_eq!(decoded.protocol_version, Some(1));
    assert_eq!(decoded.min_protocol_version, Some(1));
    assert_eq!(decoded.capabilities.len(), 3);

    // Known fields re-encode unchanged; the unknown fields are dropped.
    let re_encoded = decoded.encode_to_vec();
    let re_decoded = ComponentInfoProto::decode(re_encoded.as_slice()).unwrap();
    assert_eq!(re_decoded, sample_component_info());
}

#[test]
fn test_server_compatibility_info_round_trip_full() {
    let compat = sample_server_compatibility_info_enforce();

    let encoded = compat.encode_to_vec();
    let decoded = ServerCompatibilityInfoProto::decode(encoded.as_slice()).unwrap();

    assert_eq!(decoded.min_worker_version, "0.2.0".to_string());
    assert_eq!(decoded.min_client_version, "0.2.0".to_string());
    assert_eq!(
        decoded.compatibility_mode,
        CompatibilityModeProto::Enforce as i32
    );
    assert_eq!(
        decoded.blocked_versions,
        vec!["0.2.5".to_string(), "0.2.6".to_string()]
    );

    let server = &decoded.server;
    assert_eq!(server.component, Some("worker".to_string()));
    assert_eq!(server.release_version, Some("0.4.0-alpha".to_string()));
    assert_eq!(server.protocol_version, Some(1));
    assert_eq!(server.capabilities.len(), 3);
}

#[test]
fn test_server_compatibility_info_round_trip_defaults() {
    // Default contract: diagnose mode, empty version bounds, no blocklist.
    let compat = ServerCompatibilityInfoProto {
        server: ComponentInfoProto {
            component: Some("master".to_string()),
            release_version: Some("0.4.0-alpha".to_string()),
            ..Default::default()
        },
        min_worker_version: String::new(),
        min_client_version: String::new(),
        compatibility_mode: CompatibilityModeProto::Diagnose as i32,
        blocked_versions: vec![],
    };

    let encoded = compat.encode_to_vec();
    let decoded = ServerCompatibilityInfoProto::decode(encoded.as_slice()).unwrap();

    assert_eq!(
        decoded.compatibility_mode,
        CompatibilityModeProto::Diagnose as i32
    );
    assert!(decoded.min_worker_version.is_empty());
    assert!(decoded.blocked_versions.is_empty());
    assert_eq!(decoded.server.component, Some("master".to_string()));
}

#[test]
fn test_server_compatibility_info_unknown_fields_ignored() {
    // Emulate a future handshake response carrying extra high-range fields
    // both at the top level and nested inside the embedded component info.
    let mut compat = sample_server_compatibility_info();

    // Unknown fields inside the nested `server` message (field 1).
    let mut nested_encoded = compat.server.encode_to_vec();
    push_varint(&mut nested_encoded, 1000 << 3);
    push_varint(&mut nested_encoded, 7);
    compat.server = ComponentInfoProto::decode(nested_encoded.as_slice()).unwrap();

    let mut encoded = compat.encode_to_vec();
    // Unknown top-level varint field 1000 and length-delimited field 1001.
    push_varint(&mut encoded, 1000 << 3);
    push_varint(&mut encoded, 1);
    push_unknown_len_delimited(&mut encoded, 1001, b"future");

    let decoded = ServerCompatibilityInfoProto::decode(encoded.as_slice()).unwrap();

    assert_eq!(decoded.min_worker_version, "0.2.0".to_string());
    assert_eq!(decoded.min_client_version, "0.2.0".to_string());
    assert_eq!(
        decoded.compatibility_mode,
        CompatibilityModeProto::Diagnose as i32
    );
    let server = &decoded.server;
    assert_eq!(server.component, Some("worker".to_string()));
    assert_eq!(server.protocol_version, Some(1));
    assert_eq!(server.capabilities.len(), 3);
}

#[test]
fn test_compatibility_mode_enum_values() {
    // Wire contract: DIAGNOSE = 1, ENFORCE = 2 (no zero value is used so the
    // enum cannot be confused with an unset field).
    assert_eq!(CompatibilityModeProto::Diagnose as i32, 1);
    assert_eq!(CompatibilityModeProto::Enforce as i32, 2);
}
