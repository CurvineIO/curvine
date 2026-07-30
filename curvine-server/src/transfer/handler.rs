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

use curvine_common::error::FsError;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    CancelTransferRequest, CancelTransferResponse, GetTransferStatusRequest,
    GetTransferStatusResponse, ListTransferTenantsRequest, ListTransfersRequest,
    RetryTransferRequest, SubmitTransferRequest, SubmitTransferResponse, TransferTaskReportRequest,
    TransferTaskReportResponse, WatchTransferRequest, WatchTransferResponse,
};
use curvine_common::state::TransferState;
use curvine_common::FsResult;
use orpc::handler::MessageHandler;
use orpc::message::{Builder, Message};

use crate::transfer::{progress_to_proto, task_summary_to_proto, TransferService, TransferStore};

pub struct TransferHandler<S> {
    service: TransferService<S>,
}

impl<S> TransferHandler<S>
where
    S: TransferStore,
{
    pub fn new(service: TransferService<S>) -> Self {
        Self { service }
    }

    fn submit_transfer(&self, msg: &Message) -> FsResult<Message> {
        let req: SubmitTransferRequest = msg.parse_header()?;
        let job = self.service.submit_transfer(req)?;
        let response = SubmitTransferResponse {
            job_id: job.job_id,
            run_id: job.run_id,
            state: transfer_state_code(job.state),
        };
        Ok(Builder::success(msg).proto_header(response).build())
    }

    fn get_transfer_status(&self, msg: &Message) -> FsResult<Message> {
        let req: GetTransferStatusRequest = msg.parse_header()?;
        let (job, task_summary, tasks, next_page_token) =
            self.service
                .get_transfer_status(&req.job_id, req.page_size, req.page_token)?;
        let response = GetTransferStatusResponse {
            job_id: job.job_id,
            run_id: job.run_id,
            state: transfer_state_code(job.state),
            progress: progress_to_proto(job.summary),
            tasks,
            next_page_token,
            kind: Some(job.kind as i32),
            source_path: Some(job.source_path),
            target_path: Some(job.target_path),
            submitter: Some(job.submitter),
            tenant: Some(job.tenant),
            created_at: Some(job.created_at),
            updated_at: Some(job.updated_at),
            owner: Some(job.owner),
            lease_epoch: Some(job.lease_epoch),
            lease_expire_at: Some(job.lease_expire_at),
            cv_metadata_epoch: job.cv_metadata_epoch,
            task_summary: Some(task_summary_to_proto(task_summary)),
        };
        Ok(Builder::success(msg).proto_header(response).build())
    }

    fn retry_transfer(&self, msg: &Message) -> FsResult<Message> {
        let req: RetryTransferRequest = msg.parse_header()?;
        let job = self.service.retry_transfer(&req.job_id)?;
        let response = SubmitTransferResponse {
            job_id: job.job_id,
            run_id: job.run_id,
            state: transfer_state_code(job.state),
        };
        Ok(Builder::success(msg).proto_header(response).build())
    }

    fn list_transfers(&self, msg: &Message) -> FsResult<Message> {
        let req: ListTransfersRequest = msg.parse_header()?;
        let response = self.service.list_transfers(req)?;
        Ok(Builder::success(msg).proto_header(response).build())
    }

    fn list_transfer_tenants(&self, msg: &Message) -> FsResult<Message> {
        let req: ListTransferTenantsRequest = msg.parse_header()?;
        let response = self.service.list_tenant_summaries(req)?;
        Ok(Builder::success(msg).proto_header(response).build())
    }

    fn watch_transfer(&self, msg: &Message) -> FsResult<Message> {
        let req: WatchTransferRequest = msg.parse_header()?;
        let (job, task_summary, tasks, next_page_token, changed) = self.service.watch_transfer(
            &req.job_id,
            req.since_updated_at,
            req.page_size,
            req.page_token,
        )?;
        let response = WatchTransferResponse {
            job_id: job.job_id,
            run_id: job.run_id,
            state: transfer_state_code(job.state),
            progress: progress_to_proto(job.summary),
            tasks,
            next_page_token,
            updated_at: job.updated_at,
            changed,
            kind: Some(job.kind as i32),
            source_path: Some(job.source_path),
            target_path: Some(job.target_path),
            owner: Some(job.owner),
            lease_epoch: Some(job.lease_epoch),
            lease_expire_at: Some(job.lease_expire_at),
            cv_metadata_epoch: job.cv_metadata_epoch,
            task_summary: Some(task_summary_to_proto(task_summary)),
        };
        Ok(Builder::success(msg).proto_header(response).build())
    }

    fn cancel_transfer(&self, msg: &Message) -> FsResult<Message> {
        let req: CancelTransferRequest = msg.parse_header()?;
        let state = self.service.request_cancel(&req.job_id, req.run_id)?;
        let response = CancelTransferResponse {
            job_id: req.job_id,
            state: transfer_state_code(state),
        };
        Ok(Builder::success(msg).proto_header(response).build())
    }

    fn report_task(&self, msg: &Message) -> FsResult<Message> {
        let req: TransferTaskReportRequest = msg.parse_header()?;
        let accepted = self.service.report_task(req)?;
        let response = TransferTaskReportResponse { accepted };
        Ok(Builder::success(msg).proto_header(response).build())
    }
}

impl<S> MessageHandler for TransferHandler<S>
where
    S: TransferStore,
{
    type Error = FsError;

    fn handle(&self, msg: &Message) -> FsResult<Message> {
        match RpcCode::from(msg.code()) {
            RpcCode::SubmitTransfer => self.submit_transfer(msg),
            RpcCode::GetTransferStatus => self.get_transfer_status(msg),
            RpcCode::RetryTransfer => self.retry_transfer(msg),
            RpcCode::ListTransfers => self.list_transfers(msg),
            RpcCode::WatchTransfer => self.watch_transfer(msg),
            RpcCode::CancelTransfer => self.cancel_transfer(msg),
            RpcCode::ReportTransferTask => self.report_task(msg),
            RpcCode::ListTransferTenants => self.list_transfer_tenants(msg),
            code => Err(FsError::common(format!(
                "Unsupported transfer rpc code {}",
                code
            ))),
        }
    }
}

fn transfer_state_code(state: TransferState) -> i32 {
    state as i32
}
