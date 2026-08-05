use curvine_model::{ProtoUtils, WorkerInfo};

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
