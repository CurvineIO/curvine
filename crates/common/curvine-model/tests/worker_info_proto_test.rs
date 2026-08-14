use curvine_model::{ProtoUtils, WorkerInfo};
use curvine_proto::ComponentInfoProto;

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
