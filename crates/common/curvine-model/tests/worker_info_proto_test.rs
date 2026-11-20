use curvine_model::{ProtoUtils, WorkerAddress, WorkerInfo};
use curvine_proto::ComponentInfoProto;
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct LegacyWorkerAddress {
    #[prost(uint32, required, tag = "1")]
    worker_id: u32,
    #[prost(string, required, tag = "2")]
    hostname: String,
    #[prost(string, required, tag = "3")]
    ip_addr: String,
    #[prost(uint32, required, tag = "4")]
    rpc_port: u32,
    #[prost(uint32, required, tag = "5")]
    web_port: u32,
}

#[test]
fn worker_control_port_round_trips_and_legacy_addresses_fall_back() {
    let address = WorkerAddress {
        worker_id: 42,
        hostname: "worker".into(),
        ip_addr: "127.0.0.1".into(),
        rpc_port: 8997,
        web_port: 9002,
        control_rpc_port: Some(9005),
    };
    let proto = ProtoUtils::worker_address_to_pb(&address);
    let wire = proto.encode_to_vec();
    let old_reader = LegacyWorkerAddress::decode(wire.as_slice()).unwrap();
    assert_eq!(old_reader.rpc_port, 8997);
    assert_eq!(old_reader.web_port, 9002);
    let decoded = curvine_proto::WorkerAddressProto::decode(wire.as_slice()).unwrap();
    let restored = ProtoUtils::worker_address_from_pb(&decoded);
    assert_eq!(restored.inet_addr().port, 8997);
    assert_eq!(restored.control_addr().port, 9005);

    let decoded =
        curvine_proto::WorkerAddressProto::decode(old_reader.encode_to_vec().as_slice()).unwrap();
    let restored = ProtoUtils::worker_address_from_pb(&decoded);
    assert_eq!(restored.control_rpc_port, None);
    assert_eq!(restored.control_addr().port, 8997);

    let old_json = r#"{"worker_id":42,"hostname":"worker","ip_addr":"127.0.0.1","rpc_port":8997,"web_port":9002}"#;
    let restored: WorkerAddress = serde_json::from_str(old_json).unwrap();
    assert_eq!(restored.control_rpc_port, None);
}

fn sample_component_info() -> ComponentInfoProto {
    ComponentInfoProto {
        component: Some("worker".to_string()),
        release_version: Some("0.4.0-alpha".to_string()),
        git_commit: Some("24c848719b5b4fea74519d91cbe462bb49761b36".to_string()),
        git_tag: Some("v0.4.0-alpha".to_string()),
        git_branch: Some("main".to_string()),
        protocol_version: Some(1),
        min_protocol_version: Some(1),
        capabilities: vec!["transfer".to_string()],
    }
}

#[test]
fn worker_info_proto_preserves_weight() {
    let worker = WorkerInfo {
        weight: 42,
        software_version: "0.1.0-test".to_string(),
        startup_time_ms: 123_456,
        ..Default::default()
    };

    let proto = ProtoUtils::worker_info_to_pb(worker);
    assert_eq!(proto.weight, Some(42));
    assert_eq!(proto.software_version.as_deref(), Some("0.1.0-test"));
    assert_eq!(proto.startup_time_ms, Some(123_456));

    let restored = ProtoUtils::worker_info_from_pb(vec![proto]);
    assert_eq!(restored[0].weight, 42);
    assert_eq!(restored[0].software_version, "0.1.0-test");
    assert_eq!(restored[0].startup_time_ms, 123_456);
}

#[test]
fn worker_info_proto_preserves_component_info() {
    let component_info = sample_component_info();
    let worker = WorkerInfo {
        weight: 42,
        software_version: "0.1.0-test".to_string(),
        component_info: Some(component_info.clone()),
        ..Default::default()
    };

    let proto = ProtoUtils::worker_info_to_pb(worker);
    assert_eq!(proto.component_info, Some(component_info));

    let restored = ProtoUtils::worker_info_from_pb(vec![proto]);
    assert_eq!(restored[0].component_info, Some(sample_component_info()));
}

#[test]
fn worker_info_proto_defaults_missing_component_info() {
    // A legacy worker carries no component_info; the round trip must keep it
    // None instead of inventing an empty payload.
    let worker = WorkerInfo {
        software_version: "0.1.0-test".to_string(),
        ..Default::default()
    };

    let proto = ProtoUtils::worker_info_to_pb(worker);
    assert!(proto.component_info.is_none());

    let restored = ProtoUtils::worker_info_from_pb(vec![proto]);
    assert!(restored[0].component_info.is_none());
}

#[test]
fn worker_info_proto_defaults_missing_weight() {
    let mut proto = ProtoUtils::worker_info_to_pb(WorkerInfo::default());
    proto.weight = None;
    proto.software_version = None;
    proto.startup_time_ms = None;

    let restored = ProtoUtils::worker_info_from_pb(vec![proto]);
    assert_eq!(restored[0].weight, WorkerInfo::default_weight());
    assert!(restored[0].software_version.is_empty());
    assert_eq!(restored[0].startup_time_ms, 0);
}
