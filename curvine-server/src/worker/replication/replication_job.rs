use curvine_common::proto::SumbitBlockReplicationRequest;
use curvine_common::state::{StorageType, WorkerAddress};

pub struct ReplicationJob {
    pub block_id: i64,
    pub target_worker_addr: WorkerAddress,
    pub storage_type: Option<StorageType>,
}

impl From<SumbitBlockReplicationRequest> for ReplicationJob {
    fn from(val: SumbitBlockReplicationRequest) -> Self {
        ReplicationJob {
            block_id: val.block_id,
            target_worker_addr: val.target_worker_info.into(),
            storage_type: None,
        }
    }
}

impl ReplicationJob {
    pub fn with_storage_type(&mut self, storage_type: StorageType) {
        self.storage_type = Some(storage_type);
    }
}
