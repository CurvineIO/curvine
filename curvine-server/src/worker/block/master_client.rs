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

use crate::worker::block::{BlockMeta, BlockState};
use curvine_client::file::{FsClient, FsContext};
use curvine_common::fs::RpcCode;
use curvine_common::proto::*;
use curvine_common::state::{BlockReportInfo, HeartbeatStatus, StorageInfo, WorkerAddress};
use curvine_common::utils::ProtoUtils;
use orpc::CommonResult;
use std::sync::Arc;

//Worker and master communicate with the customer client.
// Use the synchronous client service.
// @todo Currently block reports and heartbeats use the same interface as the file system.
#[derive(Clone)]
pub struct MasterClient {
    pub(crate) fs_client: FsClient,
    pub(crate) cluster_id: String,
    pub(crate) worker_id: u32,
    pub(crate) worker_addr: WorkerAddress,
}

impl MasterClient {
    pub fn new(
        context: Arc<FsContext>,
        cluster_id: impl Into<String>,
        worker_id: u32,
        worker_addr: WorkerAddress,
    ) -> Self {
        // Directly reused file system client service.
        let fs_client = FsClient::new(context);
        Self {
            fs_client,
            cluster_id: cluster_id.into(),
            worker_id,
            worker_addr,
        }
    }

    // Send a heartbeat request, including registration, heartbeat, and offline.
    pub fn heartbeat(
        &self,
        status: HeartbeatStatus,
        storages: Vec<StorageInfo>,
    ) -> CommonResult<WorkerHeartbeatResponse> {
        // Get version from Cargo.toml
        let version_string = env!("CARGO_PKG_VERSION");

        let mut req = WorkerHeartbeatRequest {
            status: status.into(),
            cluster_id: self.cluster_id.clone(),
            worker_id: self.worker_id,
            address: ProtoUtils::worker_address_to_pb(&self.worker_addr),
            version: version_string.to_string(),
            ..Default::default()
        };
        for item in storages {
            req.storages.push(ProtoUtils::storage_info_to_pb(item));
        }

        let rep_header: WorkerHeartbeatResponse =
            self.fs_client.rpc_blocking(RpcCode::WorkerHeartbeat, req)?;

        Ok(rep_header)
    }

    pub fn full_block_report(
        &self,
        total_size: usize,
        blocks: &[BlockMeta],
    ) -> CommonResult<BlockReportListResponse> {
        let mut req = BlockReportListRequest {
            cluster_id: self.cluster_id.clone(),
            worker_id: self.worker_id,
            full_report: true,
            total_len: total_size as u64,
            blocks: vec![],
        };

        for block in blocks {
            let status = match block.state {
                BlockState::Finalized => BlockReportStatusProto::Finalized,
                BlockState::Writing => BlockReportStatusProto::Writing,
                BlockState::Recovering => BlockReportStatusProto::Writing,
            };
            let info = BlockReportInfoProto {
                id: block.id,
                status: status.into(),
                block_size: block.len,
                storage_type: block.storage_type().into(),
            };
            req.blocks.push(info)
        }

        let rep_header: BlockReportListResponse = self
            .fs_client
            .rpc_blocking(RpcCode::WorkerBlockReport, req)?;
        Ok(rep_header)
    }

    pub fn incr_block_report(
        &self,
        blocks: &[BlockReportInfo],
    ) -> CommonResult<BlockReportListResponse> {
        let mut req = BlockReportListRequest {
            cluster_id: self.cluster_id.clone(),
            worker_id: self.worker_id,
            full_report: false,
            total_len: blocks.len() as u64,
            blocks: vec![],
        };

        for block in blocks {
            let info = BlockReportInfoProto {
                id: block.id,
                status: block.status.into(),
                block_size: block.block_size,
                storage_type: block.storage_type.into(),
            };
            req.blocks.push(info);
        }

        let rep_header: BlockReportListResponse = self
            .fs_client
            .rpc_blocking(RpcCode::WorkerBlockReport, req)?;
        Ok(rep_header)
    }
}
