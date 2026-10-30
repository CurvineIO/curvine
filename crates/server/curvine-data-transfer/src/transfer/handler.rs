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

use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::RpcCode;
use curvine_model::TransferState;
use curvine_proto::{
    CancelTransferRequest, CancelTransferResponse, GetTransferStatusRequest,
    GetTransferStatusResponse, ListTransferTenantsRequest, ListTransfersRequest,
    RetryTransferRequest, SubmitTransferRequest, SubmitTransferResponse,
    TransferTaskReportBatchRequest, TransferTaskReportBatchResponse, TransferTaskReportRequest,
    TransferTaskReportResponse, WatchTransferRequest, WatchTransferResponse,
};
use curvine_rpc::handler::MessageHandler;
use curvine_rpc::message::{Builder, Message};

use crate::transfer::{
    progress_to_proto, task_summary_to_proto, TransferMetrics, TransferService, TransferStore,
};

pub struct TransferHandler<S> {
    service: TransferService<S>,
    /// Present in discovery mode; relays status/watch to the job's owner.
    #[cfg(feature = "transfer-store-mysql")]
    forwarder: Option<std::sync::Arc<crate::transfer::Forwarder>>,
}

impl<S> TransferHandler<S>
where
    S: TransferStore,
{
    pub fn new(service: TransferService<S>) -> Self {
        Self {
            service,
            #[cfg(feature = "transfer-store-mysql")]
            forwarder: None,
        }
    }

    #[cfg(feature = "transfer-store-mysql")]
    pub fn with_forwarder(mut self, forwarder: std::sync::Arc<crate::transfer::Forwarder>) -> Self {
        self.forwarder = Some(forwarder);
        self
    }

    #[cfg(feature = "transfer-store-mysql")]
    /// Relays a status/watch request to the owning instance, if there is one.
    /// Returns None whenever the request should be served here: no forwarder,
    /// already forwarded (one hop only), no resolvable owner, or the relay
    /// failed. A failure to relay must never fail the caller's request.
    async fn relay<T, R>(&self, job_id: &str, code: RpcCode, req: T) -> Option<R>
    where
        T: prost::Message + Default,
        R: prost::Message + Default,
    {
        let forwarder = self.forwarder.as_ref()?;
        let addr = forwarder.relay_target(job_id)?;
        match forwarder.relay::<T, R>(&addr, code, req).await {
            Ok(response) => Some(response),
            Err(err) => {
                log::debug!(
                    "transfer relay to {} failed, serving locally instead: {}",
                    addr,
                    err
                );
                None
            }
        }
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
        let response = self.get_transfer_status_for(&req)?;
        Ok(Builder::success(msg).proto_header(response).build())
    }

    fn get_transfer_status_for(
        &self,
        req: &GetTransferStatusRequest,
    ) -> FsResult<GetTransferStatusResponse> {
        let (job, task_summary, tasks, next_page_token, task_details_expired) = self
            .service
            .get_transfer_status(&req.job_id, req.page_size, req.page_token.clone())?;
        Ok(GetTransferStatusResponse {
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
            task_details_expired: Some(task_details_expired),
        })
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
        let response = self.watch_transfer_for(&req)?;
        Ok(Builder::success(msg).proto_header(response).build())
    }

    fn watch_transfer_for(&self, req: &WatchTransferRequest) -> FsResult<WatchTransferResponse> {
        let (job, task_summary, tasks, next_page_token, changed, task_details_expired) =
            self.service.watch_transfer(
                &req.job_id,
                req.since_updated_at,
                req.page_size,
                req.page_token.clone(),
            )?;
        Ok(WatchTransferResponse {
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
            task_details_expired: Some(task_details_expired),
        })
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
        if let Ok(metrics) = TransferMetrics::get() {
            metrics.inc_report_rpc("single");
        }
        let req: TransferTaskReportRequest = msg.parse_header()?;
        let accepted = self.service.report_task(req)?;
        let response = TransferTaskReportResponse { accepted };
        Ok(Builder::success(msg).proto_header(response).build())
    }

    fn report_task_batch(&self, msg: &Message) -> FsResult<Message> {
        let req: TransferTaskReportBatchRequest = msg.parse_header()?;
        if let Ok(metrics) = TransferMetrics::get() {
            metrics.inc_report_rpc("batch");
            metrics.inc_report_batch_tasks(req.reports.len());
        }
        let accepted = self.service.report_tasks(req)?;
        // Piggyback the cached topology version so a worker only re-fetches the
        // instance list when membership actually changed.
        let response = TransferTaskReportBatchResponse {
            accepted,
            topology_version: Some(self.service.topology_version()),
        };
        Ok(Builder::success(msg).proto_header(response).build())
    }
}

impl<S> MessageHandler for TransferHandler<S>
where
    S: TransferStore,
{
    type Error = FsError;

    /// Status/watch move to the async path only when relaying is configured;
    /// everything else keeps running on the blocking pool as before.
    #[cfg(feature = "transfer-store-mysql")]
    fn is_sync(&self, msg: &Message) -> bool {
        if self.forwarder.is_none() {
            return true;
        }
        !matches!(
            RpcCode::from(msg.code()),
            RpcCode::GetTransferStatus | RpcCode::WatchTransfer
        )
    }

    #[cfg(feature = "transfer-store-mysql")]
    async fn async_handle(&self, msg: Message) -> FsResult<Message> {
        match RpcCode::from(msg.code()) {
            RpcCode::GetTransferStatus => {
                let mut req: GetTransferStatusRequest = msg.parse_header()?;
                if !crate::transfer::is_forwarded(req.forwarded) {
                    let relay = self
                        .relay::<_, GetTransferStatusResponse>(
                            &req.job_id,
                            RpcCode::GetTransferStatus,
                            GetTransferStatusRequest {
                                forwarded: Some(true),
                                ..req.clone()
                            },
                        )
                        .await;
                    if let Some(response) = relay {
                        return Ok(Builder::success(&msg).proto_header(response).build());
                    }
                }
                // Never relay twice, and never relay a request we already
                // answered from our own view.
                req.forwarded = Some(true);
                let response = self.get_transfer_status_for(&req)?;
                Ok(Builder::success(&msg).proto_header(response).build())
            }
            RpcCode::WatchTransfer => {
                let mut req: WatchTransferRequest = msg.parse_header()?;
                if !crate::transfer::is_forwarded(req.forwarded) {
                    let relay = self
                        .relay::<_, WatchTransferResponse>(
                            &req.job_id,
                            RpcCode::WatchTransfer,
                            WatchTransferRequest {
                                forwarded: Some(true),
                                ..req.clone()
                            },
                        )
                        .await;
                    if let Some(response) = relay {
                        return Ok(Builder::success(&msg).proto_header(response).build());
                    }
                }
                req.forwarded = Some(true);
                let response = self.watch_transfer_for(&req)?;
                Ok(Builder::success(&msg).proto_header(response).build())
            }
            code => Err(FsError::common(format!(
                "Unsupported async transfer rpc code {}",
                code
            ))),
        }
    }

    fn handle(&self, msg: &Message) -> FsResult<Message> {
        match RpcCode::from(msg.code()) {
            RpcCode::SubmitTransfer => self.submit_transfer(msg),
            RpcCode::GetTransferStatus => self.get_transfer_status(msg),
            RpcCode::RetryTransfer => self.retry_transfer(msg),
            RpcCode::ListTransfers => self.list_transfers(msg),
            RpcCode::WatchTransfer => self.watch_transfer(msg),
            RpcCode::CancelTransfer => self.cancel_transfer(msg),
            RpcCode::ReportTransferTask => self.report_task(msg),
            RpcCode::ReportTransferTaskBatch => self.report_task_batch(msg),
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
