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
use curvine_core_error::CommonResult;
use curvine_fs_api::RpcCode;
use curvine_model::ProtoUtils;
use curvine_model::{
    BlockReportInfo, HeartbeatStatus, StorageInfo, TransferWorkerCapabilities, WorkerAddress,
};
use curvine_proto::*;
use curvine_sys::version;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Set when this worker process has completed Start and Running heartbeats.
#[derive(Clone)]
pub struct MasterRegistration {
    start_ok: Arc<AtomicBool>,
    running_ok: Arc<AtomicBool>,
}

impl MasterRegistration {
    pub fn new() -> Self {
        Self {
            start_ok: Arc::new(AtomicBool::new(false)),
            running_ok: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn mark_start(&self) {
        self.start_ok.store(true, Ordering::Release);
    }

    pub fn mark_running(&self) {
        self.running_ok.store(true, Ordering::Release);
    }

    pub fn ready(&self) -> bool {
        self.start_ok.load(Ordering::Acquire) && self.running_ok.load(Ordering::Acquire)
    }
}

impl Default for MasterRegistration {
    fn default() -> Self {
        Self::new()
    }
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
    /// Runtime budget shared with TaskManager and reported on every heartbeat.
    pub(crate) transfer_inflight_budget_bytes: Arc<AtomicI64>,
    /// Runtime concurrency ceiling shared with TaskManager, reported on every
    /// heartbeat.
    pub(crate) worker_max_concurrent_tasks: Arc<AtomicUsize>,
    /// Prebuilt structured component metadata reported on every heartbeat.
    /// Constant for the lifetime of the worker process, so it is built once
    /// and cloned into each request instead of re-allocated on the hot path.
    pub(crate) component_info: ComponentInfoProto,
    registration: MasterRegistration,
}

impl MasterClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        context: Arc<FsContext>,
        cluster_id: impl Into<String>,
        worker_id: u32,
        worker_addr: WorkerAddress,
        worker_weight: u32,
        worker_session_id: impl Into<String>,
        startup_time_ms: u64,
        transfer_inflight_budget_bytes: Arc<AtomicI64>,
        worker_max_concurrent_tasks: Arc<AtomicUsize>,
        registration: MasterRegistration,
    ) -> Self {
        // Directly reused file system client service.
        let fs_client = FsClient::new(context);
        let component_info =
            ProtoUtils::component_version_to_pb(&version::component_version("worker"));
        Self {
            fs_client,
            cluster_id: cluster_id.into(),
            worker_id,
            worker_addr,
            worker_weight,
            worker_session_id: worker_session_id.into(),
            software_version: version::VERSION.to_string(),
            startup_time_ms,
            transfer_inflight_budget_bytes,
            worker_max_concurrent_tasks,
            component_info,
            registration,
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
            transfer_segment_group: Some(transfer_capabilities.segment_group),
            transfer_inflight_budget_bytes: Some(
                self.transfer_inflight_budget_bytes.load(Ordering::Acquire),
            ),
            worker_max_concurrent_tasks: Some(
                self.worker_max_concurrent_tasks.load(Ordering::Acquire) as i64,
            ),
            software_version: self.software_version.clone(),
            fs_ctime: self.startup_time_ms as i64,
            component_info: Some(self.component_info.clone()),
            ..Default::default()
        };
        for item in storages {
            req.storages.push(ProtoUtils::storage_info_to_pb(item));
        }

        let rep_header: WorkerHeartbeatResponse =
            if matches!(status, HeartbeatStatus::Start | HeartbeatStatus::Running) {
                self.fs_client
                    .rpc_blocking_all_masters(RpcCode::WorkerHeartbeat, req)?
            } else {
                self.fs_client.rpc_blocking(RpcCode::WorkerHeartbeat, req)?
            };
        match status {
            HeartbeatStatus::Start => self.registration.mark_start(),
            HeartbeatStatus::Running => self.registration.mark_running(),
            HeartbeatStatus::End => {}
        }

        Ok(rep_header)
    }

    pub fn full_block_report(
        &self,
        report_id: u64,
        total_size: usize,
        blocks: &[BlockMeta],
    ) -> CommonResult<BlockReportListResponse> {
        let mut req = BlockReportListRequest {
            cluster_id: self.cluster_id.clone(),
            worker_id: self.worker_id,
            worker_session_id: Some(self.worker_session_id.clone()),
            full_report: true,
            total_len: total_size as u64,
            report_id: Some(report_id),
            full_report_begin: None,
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

    /// Sends a harmless incremental empty request before taking the local full
    /// report snapshot. Older Masters ignore the optional field and reply
    /// without the optional acknowledgement, so callers can retain the legacy
    /// reporting path during a rolling upgrade.
    pub fn begin_full_block_report(&self, report_id: u64) -> CommonResult<BlockReportListResponse> {
        let req = BlockReportListRequest {
            cluster_id: self.cluster_id.clone(),
            worker_id: self.worker_id,
            worker_session_id: Some(self.worker_session_id.clone()),
            full_report: false,
            total_len: 0,
            report_id: Some(report_id),
            full_report_begin: Some(true),
            blocks: vec![],
        };
        Ok(self
            .fs_client
            .rpc_blocking(RpcCode::WorkerBlockReport, req)?)
    }

    pub fn incr_block_report(
        &self,
        blocks: &[BlockReportInfo],
    ) -> CommonResult<BlockReportListResponse> {
        let mut req = BlockReportListRequest {
            cluster_id: self.cluster_id.clone(),
            worker_id: self.worker_id,
            worker_session_id: Some(self.worker_session_id.clone()),
            full_report: false,
            total_len: blocks.len() as u64,
            report_id: None,
            full_report_begin: None,
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
