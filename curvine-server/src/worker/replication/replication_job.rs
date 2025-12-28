// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use curvine_common::proto::SubmitBlockReplicationRequest;
use curvine_common::state::{StorageType, WorkerAddress};
use curvine_common::utils::ProtoUtils;

pub struct ReplicationJob {
    pub block_id: i64,
    pub target_worker_addr: WorkerAddress,
    pub storage_type: Option<StorageType>,
}

impl From<SubmitBlockReplicationRequest> for ReplicationJob {
    fn from(val: SubmitBlockReplicationRequest) -> Self {
        ReplicationJob {
            block_id: val.block_id,
            target_worker_addr: ProtoUtils::worker_address_from_pb(&val.target_worker_info),
            storage_type: None,
        }
    }
}

impl ReplicationJob {
    pub fn with_storage_type(&mut self, storage_type: StorageType) {
        self.storage_type = Some(storage_type);
    }
}
