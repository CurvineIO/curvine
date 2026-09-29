use curvine_model::{
    BlockReplicaDetail, FileBlockDetail, FileBlockDetails, FileStatus, ProtoUtils, StorageType,
    WorkerAddress,
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
                },
                BlockReplicaDetail {
                    worker_id: 2,
                    storage_type: StorageType::Disk,
                    address: None,
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
}
