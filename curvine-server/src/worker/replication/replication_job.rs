use curvine_common::proto::SumbitBlockReplicationRequest;
use curvine_common::state::WorkerAddress;

pub struct ReplicationJob {
    pub block_id: i64,
    pub target_worker_addr: WorkerAddress,
}

impl From<SumbitBlockReplicationRequest> for ReplicationJob {
    fn from(val: SumbitBlockReplicationRequest) -> Self {
        ReplicationJob {
            block_id: val.block_id,
            target_worker_addr: val.target_worker_info.into(),
        }
    }
}
