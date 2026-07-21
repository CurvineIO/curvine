use curvine_common::state::WorkerInfo;
use curvine_common::utils::ProtoUtils;

#[test]
fn worker_info_proto_preserves_weight() {
    let worker = WorkerInfo {
        weight: 42,
        ..Default::default()
    };

    let proto = ProtoUtils::worker_info_to_pb(worker);
    assert_eq!(proto.weight, Some(42));

    let restored = ProtoUtils::worker_info_from_pb(vec![proto]);
    assert_eq!(restored[0].weight, 42);
}

#[test]
fn worker_info_proto_defaults_missing_weight() {
    let mut proto = ProtoUtils::worker_info_to_pb(WorkerInfo::default());
    proto.weight = None;

    let restored = ProtoUtils::worker_info_from_pb(vec![proto]);
    assert_eq!(restored[0].weight, WorkerInfo::default_weight());
}
