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

use std::sync::Arc;

use curvine_common::error::FsError;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    CancelTransferRequest, CancelTransferResponse, GetTransferStatusRequest,
    GetTransferStatusResponse, ListTransferTenantsRequest, ListTransferTenantsResponse,
    ListTransfersRequest, ListTransfersResponse, RetryTransferRequest, SubmitTransferRequest,
    SubmitTransferResponse, TransferProgressProto, TransferTaskReportRequest,
    TransferTaskReportResponse, WatchTransferRequest, WatchTransferResponse,
};
use curvine_common::state::{
    JobTaskProgress, JobTaskState, TransferCommand, TransferKind, TransferState,
    TransferTaskReportInfo,
};
use curvine_common::utils::SerdeUtils;
use curvine_common::FsResult;
use orpc::client::ClusterConnector;
use orpc::io::net::NodeAddr;
use orpc::runtime::Runtime;

use crate::file::FsContext;

const TRANSFER_PROTOCOL_VERSION: u32 = 1;

#[derive(Clone)]
pub struct TransferClient {
    connector: Arc<ClusterConnector>,
}

impl TransferClient {
    pub fn with_context(context: &Arc<FsContext>) -> FsResult<Self> {
        Self::with_endpoints(
            context.conf.client_rpc_conf(),
            context.clone_runtime(),
            transfer_endpoints(context),
        )
    }

    pub fn with_endpoint(context: &Arc<FsContext>, endpoint: impl Into<String>) -> FsResult<Self> {
        Self::with_report_endpoints(context, vec![endpoint.into()])
    }

    pub fn with_report_endpoints(
        context: &Arc<FsContext>,
        endpoints: Vec<String>,
    ) -> FsResult<Self> {
        Self::with_endpoints(
            context.conf.client_rpc_conf(),
            context.clone_runtime(),
            endpoints,
        )
    }

    pub fn with_rt(conf: &curvine_common::conf::ClusterConf, rt: Arc<Runtime>) -> FsResult<Self> {
        Self::with_endpoints(
            conf.client_rpc_conf(),
            rt,
            transfer_endpoints_from_conf(conf),
        )
    }

    fn with_endpoints(
        conf: orpc::client::ClientConf,
        rt: Arc<Runtime>,
        endpoints: Vec<String>,
    ) -> FsResult<Self> {
        let connector = ClusterConnector::with_rt(conf, rt);
        for endpoint in endpoints {
            connector.add_node(NodeAddr::from_str(endpoint)?)?;
        }
        Ok(Self {
            connector: Arc::new(connector),
        })
    }

    pub async fn submit(&self, command: TransferCommand) -> FsResult<SubmitTransferResponse> {
        let req = SubmitTransferRequest {
            kind: transfer_kind_code(command.kind),
            source_path: command.source_path.clone(),
            target_path: command.target_path.clone(),
            client_request_id: command.client_request_id.clone(),
            submitter: command.submitter.clone(),
            tenant: command.tenant.clone(),
            command: SerdeUtils::serialize_json(&command)?,
            protocol_version: Some(TRANSFER_PROTOCOL_VERSION),
        };
        self.connector
            .proto_rpc::<_, SubmitTransferResponse, FsError>(RpcCode::SubmitTransfer, req)
            .await
    }

    pub async fn status(&self, job_id: impl AsRef<str>) -> FsResult<GetTransferStatusResponse> {
        self.status_page(job_id, None, None).await
    }

    pub async fn retry(&self, job_id: impl AsRef<str>) -> FsResult<SubmitTransferResponse> {
        let req = RetryTransferRequest {
            job_id: job_id.as_ref().to_string(),
        };
        self.connector
            .proto_rpc::<_, SubmitTransferResponse, FsError>(RpcCode::RetryTransfer, req)
            .await
    }

    pub async fn status_page(
        &self,
        job_id: impl AsRef<str>,
        page_size: Option<u32>,
        page_token: Option<String>,
    ) -> FsResult<GetTransferStatusResponse> {
        let req = GetTransferStatusRequest {
            job_id: job_id.as_ref().to_string(),
            page_size,
            page_token,
        };
        self.connector
            .proto_rpc::<_, GetTransferStatusResponse, FsError>(RpcCode::GetTransferStatus, req)
            .await
    }

    pub async fn list(
        &self,
        kind: Option<TransferKind>,
        state: Option<TransferState>,
        submitter: Option<String>,
        tenant: Option<String>,
        page_size: Option<u32>,
        page_token: Option<String>,
    ) -> FsResult<ListTransfersResponse> {
        let req = ListTransfersRequest {
            kind: kind.map(transfer_kind_code),
            state: state.map(transfer_state_code),
            page_size,
            page_token,
            submitter,
            tenant,
        };
        self.connector
            .proto_rpc::<_, ListTransfersResponse, FsError>(RpcCode::ListTransfers, req)
            .await
    }

    pub async fn list_tenants(
        &self,
        page_size: Option<u32>,
        page_token: Option<String>,
    ) -> FsResult<ListTransferTenantsResponse> {
        let req = ListTransferTenantsRequest {
            page_size,
            page_token,
        };
        self.connector
            .proto_rpc::<_, ListTransferTenantsResponse, FsError>(RpcCode::ListTransferTenants, req)
            .await
    }

    pub async fn watch(
        &self,
        job_id: impl AsRef<str>,
        since_updated_at: Option<u64>,
        page_size: Option<u32>,
        page_token: Option<String>,
    ) -> FsResult<WatchTransferResponse> {
        let req = WatchTransferRequest {
            job_id: job_id.as_ref().to_string(),
            since_updated_at,
            page_size,
            page_token,
        };
        self.connector
            .proto_rpc::<_, WatchTransferResponse, FsError>(RpcCode::WatchTransfer, req)
            .await
    }

    pub async fn cancel(
        &self,
        job_id: impl AsRef<str>,
        run_id: Option<u64>,
    ) -> FsResult<CancelTransferResponse> {
        let req = CancelTransferRequest {
            job_id: job_id.as_ref().to_string(),
            run_id,
        };
        self.connector
            .proto_rpc::<_, CancelTransferResponse, FsError>(RpcCode::CancelTransfer, req)
            .await
    }

    pub async fn report_task(
        &self,
        job_id: impl AsRef<str>,
        task_id: impl AsRef<str>,
        info: &TransferTaskReportInfo,
        progress: JobTaskProgress,
    ) -> FsResult<bool> {
        let req = TransferTaskReportRequest {
            job_id: job_id.as_ref().to_string(),
            run_id: info.run_id,
            task_id: task_id.as_ref().to_string(),
            attempt_id: info.attempt_id,
            worker_id: info.worker_id,
            worker_session_id: info.worker_session_id.clone(),
            state: transfer_task_state_code(progress.state),
            progress: TransferProgressProto {
                loaded_size: progress.loaded_size,
                total_size: progress.total_size,
                update_time: progress.update_time,
                message: progress.message,
            },
            protocol_version: Some(TRANSFER_PROTOCOL_VERSION),
        };
        let response = self
            .connector
            .proto_rpc::<_, TransferTaskReportResponse, FsError>(RpcCode::ReportTransferTask, req)
            .await?;
        Ok(response.accepted)
    }
}

fn transfer_endpoints(context: &Arc<FsContext>) -> Vec<String> {
    transfer_endpoints_from_conf(&context.conf)
}

fn transfer_endpoints_from_conf(conf: &curvine_common::conf::ClusterConf) -> Vec<String> {
    if conf.transfer.endpoints.is_empty() {
        vec![format!(
            "{}:{}",
            conf.transfer.hostname, conf.transfer.rpc_port
        )]
    } else {
        conf.transfer.endpoints.clone()
    }
}

fn transfer_kind_code(kind: TransferKind) -> i32 {
    kind as i32
}

fn transfer_state_code(state: TransferState) -> i32 {
    state as i32
}

fn transfer_task_state_code(state: JobTaskState) -> i32 {
    match state {
        JobTaskState::Pending | JobTaskState::UNKNOWN => 1,
        JobTaskState::Loading => 2,
        JobTaskState::Completed => 3,
        JobTaskState::Failed => 4,
        JobTaskState::Canceled => 5,
    }
}
