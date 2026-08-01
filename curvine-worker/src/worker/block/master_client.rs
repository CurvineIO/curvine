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
use curvine_client_core::file::{FsClient, FsContext};
use curvine_common::fs::RpcCode;
use curvine_common::proto::*;
use curvine_common::state::{
    BlockReportInfo, HeartbeatStatus, StorageInfo, TransferWorkerCapabilities, WorkerAddress,
};
use curvine_common::utils::ProtoUtils;
use curvine_common::version;
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
    pub(crate) worker_weight: u32,
    pub(crate) worker_session_id: String,
    pub(crate) software_version: String,
    pub(crate) startup_time_ms: u64,
}

impl MasterClient {
    pub fn new(
        context: Arc<FsContext>,
        cluster_id: impl Into<String>,
        worker_id: u32,
        worker_addr: WorkerAddress,
        worker_weight: u32,
        worker_session_id: impl Into<String>,
        startup_time_ms: u64,
    ) -> Self {
        // Directly reused file system client service.
        let fs_client = FsClient::new(context);
        Self {
            fs_client,
            cluster_id: cluster_id.into(),
            worker_id,
            worker_addr,
            worker_weight,
            worker_session_id: worker_session_id.into(),
            software_version: version::VERSION.to_string(),
            startup_time_ms,
        }
    }

    // Send a heartbeat request, including registration, heartbeat, and offline.
    pub fn heartbeat(
        &self,
        status: HeartbeatStatus,
        storages: Vec<StorageInfo>,
    ) -> CommonResult<WorkerHeartbeatResponse> {
        let transfer_capabilities = TransferWorkerCapabilities::current();
        let mut req = WorkerHeartbeatRequest {
            status: status.into(),
            cluster_id: self.cluster_id.clone(),
            worker_id: self.worker_id,
            address: ProtoUtils::worker_address_to_pb(&self.worker_addr),
            weight: Some(self.worker_weight),
            worker_session_id: Some(self.worker_session_id.clone()),
            transfer_task_submit: Some(transfer_capabilities.task_submit),
            transfer_report_target: Some(transfer_capabilities.report_target),
            transfer_query_task: Some(transfer_capabilities.query_task),
            transfer_attempt_safe_output: Some(transfer_capabilities.attempt_safe_output),
            transfer_source_read_plan: Some(transfer_capabilities.source_read_plan),
            software_version: self.software_version.clone(),
            fs_ctime: self.startup_time_ms as i64,
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
                BlockState::Writing
                | BlockState::Recovering
                | BlockState::Allocating
                | BlockState::Finalizing => BlockReportStatusProto::Writing,
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
