use curvine_model::{
    BlockReplicaDetail, FileBlockDetail, FileBlockDetails, FileStatus, ProtoUtils, StorageType,
    WorkerAddress, WorkerStatus,
};

#[test]
fn file_block_details_proto_preserves_actual_replica_storage() {
    let details = FileBlockDetails {
        status: FileStatus {
            path: "/data/file".to_string(),
            len: 128,
            ..Default::default()
        },
        blocks: vec![FileBlockDetail {
            block_id: 42,
            len: 128,
            offset: 0,
            replicas: vec![
                BlockReplicaDetail {
                    worker_id: 1,
                    storage_type: StorageType::SpdkDisk,
                    address: Some(WorkerAddress {
                        worker_id: 1,
                        hostname: "worker-a".to_string(),
                        ip_addr: "127.0.0.1".to_string(),
                        rpc_port: 50010,
                        web_port: 50011,
                    }),
                    state: WorkerStatus::Live,
                },
                BlockReplicaDetail {
                    worker_id: 2,
                    storage_type: StorageType::Disk,
                    address: None,
                    state: WorkerStatus::Unknown,
                },
            ],
        }],
    };

    let restored =
        ProtoUtils::file_block_details_from_pb(ProtoUtils::file_block_details_to_pb(details));

    assert_eq!(restored.status.path, "/data/file");
    assert_eq!(restored.blocks[0].block_id, 42);
    assert_eq!(restored.blocks[0].len, 128);
    assert_eq!(
        restored.blocks[0].replicas[0].storage_type,
        StorageType::SpdkDisk
    );
    assert_eq!(
        restored.blocks[0].replicas[0]
            .address
            .as_ref()
            .unwrap()
            .hostname,
        "worker-a"
    );
    assert_eq!(
        restored.blocks[0].replicas[1].storage_type,
        StorageType::Disk
    );
    assert!(restored.blocks[0].replicas[1].address.is_none());
    assert_eq!(restored.blocks[0].replicas[0].state, WorkerStatus::Live);
    assert_eq!(restored.blocks[0].replicas[1].state, WorkerStatus::Unknown);
}

#[test]
fn block_replica_detail_deserializes_missing_state_as_unknown() {
    let json = r#"
        {
            "worker_id": 2,
            "storage_type": "Disk",
            "address": null
        }
    "#;

    let replica: BlockReplicaDetail = serde_json::from_str(json).unwrap();

    assert_eq!(replica.worker_id, 2);
    assert_eq!(replica.storage_type, StorageType::Disk);
    assert!(replica.address.is_none());
    assert_eq!(replica.state, WorkerStatus::Unknown);
}
