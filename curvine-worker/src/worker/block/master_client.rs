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

use crate::worker::block::worker_peer_fanout::{
    FullReportSnapshot, PeerMirrorRequest, PeerMirrorTransport, WorkerPeerFanout,
    WorkerPeerFanoutConfig,
};
use crate::worker::block::{BlockMeta, BlockState};
use curvine_client_core::file::{FsClient, FsContext};
use curvine_config::WorkerConf;
use curvine_core_error::{CommonError, CommonResult};
use curvine_error::FsResult;
use curvine_fs_api::RpcCode;
use curvine_model::ProtoUtils;
use curvine_model::{
    BlockReportInfo, HeartbeatStatus, StorageInfo, TransferWorkerCapabilities, WorkerAddress,
};
use curvine_net::net::NodeAddr;
use curvine_proto::*;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_sys::version;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) struct MasterClientConfig {
    pub(crate) cluster_id: String,
    pub(crate) worker_id: u32,
    pub(crate) worker_addr: WorkerAddress,
    pub(crate) worker_weight: u32,
    pub(crate) worker_session_id: String,
    pub(crate) startup_time_ms: u64,
    pub(crate) worker_conf: WorkerConf,
}

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
    /// Prebuilt structured component metadata reported on every heartbeat.
    /// Constant for the lifetime of the worker process, so it is built once
    /// and cloned into each request instead of re-allocated on the hot path.
    pub(crate) component_info: ComponentInfoProto,
    runtime: Arc<Runtime>,
    peer_fanout: Option<Arc<WorkerPeerFanout>>,
}

#[derive(Clone)]
struct FsClientPeerMirrorTransport {
    client: FsClient,
}

#[async_trait::async_trait]
impl PeerMirrorTransport for FsClientPeerMirrorTransport {
    fn master_peer_snapshot(&self) -> Vec<NodeAddr> {
        self.client.master_peer_snapshot()
    }

    fn master_peer_leader_id(&self) -> Option<u64> {
        self.client.master_peer_leader_id()
    }

    async fn send_to_peer(&self, peer_id: u64, request: PeerMirrorRequest) -> FsResult<()> {
        match request {
            PeerMirrorRequest::Heartbeat(request) => {
                let _: WorkerHeartbeatResponse = self
                    .client
                    .rpc_to_master_peer(peer_id, RpcCode::WorkerHeartbeat, *request)
                    .await?;
            }
            PeerMirrorRequest::BlockReport(request) => {
                let _: BlockReportListResponse = self
                    .client
                    .rpc_to_master_peer(peer_id, RpcCode::WorkerBlockReport, request)
                    .await?;
            }
        }
        // Mirror responses are deliberately discarded. Only the leader's
        // normal RPC response is allowed to issue worker commands.
        Ok(())
    }
}

impl MasterClient {
    pub(crate) fn new(context: Arc<FsContext>, config: MasterClientConfig) -> CommonResult<Self> {
        let MasterClientConfig {
            cluster_id,
            worker_id,
            worker_addr,
            worker_weight,
            worker_session_id,
            startup_time_ms,
            worker_conf,
        } = config;
        let runtime = context.clone_runtime();
        // Directly reused file system client service.
        let fs_client = FsClient::new(context);
        let component_info =
            ProtoUtils::component_version_to_pb(&version::component_version("worker"));
        let peer_fanout = if worker_conf.follower_reports_enabled {
            Some(
                WorkerPeerFanout::new(
                    runtime.clone(),
                    Arc::new(FsClientPeerMirrorTransport {
                        client: fs_client.clone(),
                    }),
                    WorkerPeerFanoutConfig::from(&worker_conf),
                )
                .map_err(|error| CommonError::from(error.to_string()))?,
            )
        } else {
            None
        };
        Ok(Self {
            fs_client,
            cluster_id,
            worker_id,
            worker_addr,
            worker_weight,
            worker_session_id,
            software_version: version::VERSION.to_string(),
            startup_time_ms,
            component_info,
            runtime,
            peer_fanout,
        })
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
            component_info: Some(self.component_info.clone()),
            ..Default::default()
        };
        for item in storages {
            req.storages.push(ProtoUtils::storage_info_to_pb(item));
        }

        let request = req.clone();
        let response = self.fs_client.rpc_blocking(RpcCode::WorkerHeartbeat, req);
        if let (Some(fanout), Ok(_)) = (&self.peer_fanout, &response) {
            fanout.publish_heartbeat(request);
        }
        if status == HeartbeatStatus::End {
            if let Some(fanout) = &self.peer_fanout {
                self.runtime.block_on(fanout.shutdown());
            }
        }
        let rep_header: WorkerHeartbeatResponse = response?;

        Ok(rep_header)
    }

    pub fn full_block_report(
        &self,
        report_id: u64,
        total_size: usize,
        blocks: &[BlockMeta],
    ) -> CommonResult<BlockReportListResponse> {
        let req = self.full_report_batch_request(report_id, total_size, blocks);

        let request = req.clone();
        let response = self.fs_client.rpc_blocking(RpcCode::WorkerBlockReport, req);
        if let (Some(fanout), Ok(_)) = (&self.peer_fanout, &response) {
            fanout.publish_block_report(request);
        }
        let rep_header: BlockReportListResponse = response?;
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
            report_id: None,
            worker_session_id: Some(self.worker_session_id.clone()),
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

        let request = req.clone();
        let response = self.fs_client.rpc_blocking(RpcCode::WorkerBlockReport, req);
        if let (Some(fanout), Ok(_)) = (&self.peer_fanout, &response) {
            fanout.publish_block_report(request);
        }
        let rep_header: BlockReportListResponse = response?;
        Ok(rep_header)
    }

    pub(crate) fn needs_follower_resync(&self) -> bool {
        self.peer_fanout
            .as_ref()
            .is_some_and(|fanout| fanout.needs_full_resync())
    }

    pub(crate) fn begin_initial_full_report(&self) {
        if let Some(fanout) = &self.peer_fanout {
            fanout.begin_initial_full_report();
        }
    }

    pub(crate) fn end_initial_full_report(&self) {
        if let Some(fanout) = &self.peer_fanout {
            fanout.end_initial_full_report();
        }
    }

    pub(crate) fn resync_follower_reports<F>(&self, build_reports: F) -> CommonResult<()>
    where
        F: FnOnce(usize) -> CommonResult<Option<FullReportSnapshot>>,
    {
        if let Some(fanout) = &self.peer_fanout {
            fanout.resync_with_snapshot(build_reports)?;
        }
        Ok(())
    }

    pub(crate) fn full_report_snapshot(
        &self,
        blocks: Vec<BlockMeta>,
        block_report_limit: usize,
    ) -> FullReportSnapshot {
        FullReportSnapshot::new(
            self.cluster_id.clone(),
            self.worker_id,
            self.worker_session_id.clone(),
            new_report_id(),
            blocks,
            block_report_limit,
        )
    }

    fn full_report_batch_request(
        &self,
        report_id: u64,
        total_size: usize,
        blocks: &[BlockMeta],
    ) -> BlockReportListRequest {
        let mut request = BlockReportListRequest {
            cluster_id: self.cluster_id.clone(),
            worker_id: self.worker_id,
            full_report: true,
            total_len: total_size as u64,
            report_id: Some(report_id),
            worker_session_id: Some(self.worker_session_id.clone()),
            blocks: Vec::with_capacity(blocks.len()),
        };
        request.blocks.extend(blocks.iter().map(|block| {
            let status = match block.state {
                BlockState::Finalized => BlockReportStatusProto::Finalized,
                BlockState::Writing
                | BlockState::Recovering
                | BlockState::Allocating
                | BlockState::Finalizing => BlockReportStatusProto::Writing,
            };
            BlockReportInfoProto {
                id: block.id,
                status: status.into(),
                block_size: block.len,
                storage_type: block.storage_type().into(),
            }
        }));
        request
    }
}

fn new_report_id() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(1)
        .max(1)
}
