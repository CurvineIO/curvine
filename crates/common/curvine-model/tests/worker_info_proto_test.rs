use curvine_model::{
    ProtoUtils, StorageInfo, StorageType, WorkerAddress, WorkerInfo, WorkerStatus,
};
use curvine_proto::ComponentInfoProto;

fn sample_worker() -> WorkerInfo {
    let mut worker = WorkerInfo::new(
        WorkerAddress {
            worker_id: 7,
            hostname: "worker-7".to_string(),
            ip_addr: "10.0.0.7".to_string(),
            rpc_port: 19998,
            ..Default::default()
        },
        WorkerInfo::default_weight(),
    );
    worker.last_update = 1_700_000_000_123;
    worker.block_num = 42;
    worker.status = WorkerStatus::Blacklist;
    worker.capacity = 1_000;
    worker.available = 600;
    worker.fs_used = 300;
    worker.non_fs_used = 100;
    worker.reserved_bytes = 50;
    worker.transfer_inflight_budget_bytes = 100 * 1024 * 1024 * 1024;
    worker.worker_max_concurrent_tasks = 64;
    worker.storage_map.insert(
        "disk-0".to_string(),
        StorageInfo {
            dir_id: 1,
            storage_id: "disk-0".to_string(),
            failed: false,
            capacity: 1_000,
            available: 600,
            fs_used: 300,
            non_fs_used: 100,
            reserved_bytes: 50,
            storage_type: StorageType::Disk,
            block_num: 42,
            dir_path: "/data".to_string(),
        },
    );
    worker
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
fn worker_info_proto_round_trip_preserves_heartbeat_and_blocks() {
    let worker = sample_worker();
    let pb = ProtoUtils::worker_info_to_pb(worker.clone());
    let restored = ProtoUtils::worker_info_from_pb(vec![pb])
        .pop()
        .expect("worker");

    assert_eq!(restored.last_update, worker.last_update);
    assert_eq!(restored.block_num, worker.block_num);
    assert_eq!(restored.status, worker.status);
    assert_eq!(restored.capacity, worker.capacity);
    assert_eq!(restored.available, worker.available);
    assert_eq!(restored.storage_map.len(), worker.storage_map.len());
    assert_eq!(restored.weight, worker.weight);
}

#[test]
fn worker_info_from_pb_derives_block_num_from_storage_map() {
    let mut worker = sample_worker();
    worker.block_num = 0;

    let mut pb = ProtoUtils::worker_info_to_pb(worker);
    pb.block_num = Some(0);

    let restored = ProtoUtils::worker_info_from_pb(vec![pb])
        .pop()
        .expect("worker");

    assert_eq!(restored.block_num, 42);
    assert_eq!(
        restored.transfer_inflight_budget_bytes,
        100 * 1024 * 1024 * 1024
    );
    assert_eq!(restored.worker_max_concurrent_tasks, 64);
}

#[test]
fn worker_info_from_pb_treats_missing_optional_fields_as_legacy_defaults() {
    let mut pb = ProtoUtils::worker_info_to_pb(sample_worker());
    pb.block_num = None;
    pb.status = None;
    pb.weight = None;
    pb.transfer_inflight_budget_bytes = None;
    pb.worker_max_concurrent_tasks = None;

    let restored = ProtoUtils::worker_info_from_pb(vec![pb])
        .pop()
        .expect("worker");

    assert_eq!(restored.status, WorkerStatus::Live);
    assert_eq!(restored.block_num, 42);
    assert_eq!(restored.weight, WorkerInfo::default_weight());
    assert_eq!(restored.transfer_inflight_budget_bytes, 0);
    assert_eq!(restored.worker_max_concurrent_tasks, 0);
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
