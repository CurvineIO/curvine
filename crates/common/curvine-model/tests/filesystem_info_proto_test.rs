use curvine_model::{FilesystemInfo, ProtoUtils};
use curvine_proto::GetFilesystemInfoResponse;

#[test]
fn filesystem_info_proto_round_trips_allocatable_fields() {
    let info = FilesystemInfo {
        capacity: 6000,
        available: 4800,
        allocatable_capacity: 1000,
        allocatable_available: 800,
        ..Default::default()
    };

    let pb = ProtoUtils::filesystem_info_to_pb(info.clone());
    assert_eq!(pb.allocatable_capacity, Some(1000));
    assert_eq!(pb.allocatable_available, Some(800));

    let restored = ProtoUtils::filesystem_info_from_pb(pb);
    assert_eq!(restored.capacity, 6000);
    assert_eq!(restored.available, 4800);
    assert_eq!(restored.allocatable_capacity, 1000);
    assert_eq!(restored.allocatable_available, 800);
}

#[test]
fn filesystem_info_proto_legacy_master_falls_back_to_total() {
    // A legacy master omits the allocatable fields (tags 15/16). The client
    // must fall back to the aggregate capacity/available so statfs never
    // reports zero free space against a mixed-version master.
    let legacy_pb = GetFilesystemInfoResponse {
        active_master: "old-master".to_string(),
        capacity: 6000,
        available: 4800,
        // allocatable_capacity / allocatable_available intentionally absent.
        ..Default::default()
    };

    let info = ProtoUtils::filesystem_info_from_pb(legacy_pb);
    assert_eq!(info.capacity, 6000);
    assert_eq!(info.available, 4800);
    assert_eq!(
        info.allocatable_capacity, 6000,
        "legacy master must fall back to total capacity"
    );
    assert_eq!(
        info.allocatable_available, 4800,
        "legacy master must fall back to total available"
    );
}

#[test]
fn filesystem_info_proto_zero_allocatable_is_preserved() {
    // A new master with no Live workers reports zero allocatable; this must
    // not be confused with the legacy-absent case (which falls back to total).
    let pb = ProtoUtils::filesystem_info_to_pb(FilesystemInfo {
        capacity: 5000,
        available: 4000,
        allocatable_capacity: 0,
        allocatable_available: 0,
        ..Default::default()
    });
    assert_eq!(pb.allocatable_capacity, Some(0));
    assert_eq!(pb.allocatable_available, Some(0));

    let info = ProtoUtils::filesystem_info_from_pb(pb);
    assert_eq!(info.allocatable_capacity, 0);
    assert_eq!(info.allocatable_available, 0);
}
