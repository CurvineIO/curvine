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

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use curvine_config::ClusterConf;
use curvine_error::{FsError, FsResult};
use curvine_model::{
    JobTaskProgress, JobTaskState, TransferCommand, TransferKind, TransferState,
    TransferTaskReportInfo,
};
use curvine_net::net::NodeAddr;
use curvine_net::retry::TimeBondedRetryBuilder;
use curvine_proto::transfer_service_client::TransferServiceClient;
use curvine_proto::{
    CancelTransferRequest, CancelTransferResponse, GetTransferStatusRequest,
    GetTransferStatusResponse, ListTransferTenantsRequest, ListTransferTenantsResponse,
    ListTransfersRequest, ListTransfersResponse, RetryTransferRequest, SubmitTransferRequest,
    SubmitTransferResponse, TransferProgressProto, TransferTaskReportRequest,
    TransferTaskReportResponse, WatchTransferRequest, WatchTransferResponse,
};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use futures::{stream::FuturesUnordered, StreamExt};
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Response, Status};

use curvine_client_core::file::FsContext;

const TRANSFER_PROTOCOL_VERSION: u32 = 1;

#[derive(Clone)]
pub struct TransferClient {
    inner: Arc<TransferGrpcClient>,
}

struct TransferGrpcClient {
    endpoints: Vec<Endpoint>,
    channels: tokio::sync::OnceCell<Vec<Vec<Channel>>>,
    conn_size: usize,
    next_channel: AtomicUsize,
    preferred: AtomicUsize,
    retry: TimeBondedRetryBuilder,
    timeout: Duration,
    rt: Arc<Runtime>,
}

impl TransferClient {
    pub fn with_context(context: &Arc<FsContext>) -> FsResult<Self> {
        Self::with_endpoints(
            context.conf().client_rpc_conf(),
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
            context.conf().client_rpc_conf(),
            context.clone_runtime(),
            endpoints,
        )
    }

    pub fn with_rt(conf: &ClusterConf, rt: Arc<Runtime>) -> FsResult<Self> {
        Self::with_endpoints(
            conf.client_rpc_conf(),
            rt,
            transfer_endpoints_from_conf(conf),
        )
    }

    fn with_endpoints(
        conf: curvine_rpc::client::ClientConf,
        rt: Arc<Runtime>,
        endpoints: Vec<String>,
    ) -> FsResult<Self> {
        let mut grpc_endpoints = Vec::with_capacity(endpoints.len());
        let transport = curvine_rpc::grpc::TransportConfig {
            connect_timeout: Duration::from_millis(conf.conn_timeout_ms),
            request_timeout: Some(Duration::from_millis(conf.rpc_timeout_ms)),
            ..Default::default()
        };
        for endpoint in endpoints {
            let address = NodeAddr::from_str(endpoint)?;
            let uri = conf.grpc.uri(&address.addr.hostname, address.addr.port);
            let tls = conf
                .grpc
                .client_tls(&address.addr.hostname)
                .map_err(|e| FsError::common(e.to_string()))?;
            grpc_endpoints.push(
                transport
                    .endpoint(uri, tls.as_ref())
                    .map_err(|e| FsError::common(e.to_string()))?,
            );
        }
        Ok(Self {
            inner: Arc::new(TransferGrpcClient {
                endpoints: grpc_endpoints,
                channels: tokio::sync::OnceCell::new(),
                preferred: AtomicUsize::new(usize::MAX),
                conn_size: conf.conn_size.max(1),
                next_channel: AtomicUsize::new(0),
                retry: conf.io_retry_builder(),
                timeout: Duration::from_millis(conf.rpc_timeout_ms),
                rt,
            }),
        })
    }

    async fn call<Q, R, F, Fut>(&self, request: Q, call: F) -> FsResult<R>
    where
        Q: Clone,
        F: Fn(TransferServiceClient<Channel>, Q) -> Fut,
        Fut: Future<Output = Result<Response<R>, Status>>,
    {
        let channels = self
            .inner
            .channels
            .get_or_try_init(|| async {
                let endpoints = self.inner.endpoints.clone();
                let conn_size = self.inner.conn_size;
                self.inner
                    .rt
                    .spawn(async move {
                        endpoints
                            .iter()
                            .map(|endpoint| {
                                (0..conn_size).map(|_| endpoint.connect_lazy()).collect()
                            })
                            .collect()
                    })
                    .await
                    .map_err(|e| FsError::common(format!("transfer channel setup failed: {e}")))
            })
            .await?;
        if channels.is_empty() {
            return Err(FsError::common("no transfer endpoints configured"));
        }
        let mut last_error = None;
        let channel_index =
            self.inner.next_channel.fetch_add(1, Ordering::Relaxed) % self.inner.conn_size;
        let preferred = self.inner.preferred.load(Ordering::Relaxed);
        if preferred < channels.len() {
            match Self::attempt(
                &channels[preferred][channel_index],
                &request,
                &call,
                self.inner.timeout,
            )
            .await
            {
                Ok(response) => return Ok(response),
                Err((false, error)) => return Err(error),
                Err((true, error)) => last_error = Some(error),
            }
        }
        self.inner.preferred.store(usize::MAX, Ordering::Relaxed);
        let mut retry = self.inner.retry.build();
        while retry.attempt().await {
            let request = &request;
            let call = &call;
            let timeout = self.inner.timeout;
            let attempts = channels
                .iter()
                .enumerate()
                .map(|(index, pool)| async move {
                    (
                        index,
                        Self::attempt(&pool[channel_index], request, call, timeout).await,
                    )
                })
                .collect::<FuturesUnordered<_>>();
            futures::pin_mut!(attempts);
            while let Some((index, result)) = attempts.next().await {
                match result {
                    Ok(response) => {
                        self.inner.preferred.store(index, Ordering::Relaxed);
                        return Ok(response);
                    }
                    Err((false, error)) => {
                        self.inner.preferred.store(index, Ordering::Relaxed);
                        return Err(error);
                    }
                    Err((true, error)) => last_error = Some(error),
                }
            }
        }
        Err(last_error.unwrap_or_else(|| FsError::common("transfer RPC retry budget exhausted")))
    }

    async fn attempt<Q, R, F, Fut>(
        channel: &Channel,
        request: &Q,
        call: &F,
        timeout: Duration,
    ) -> Result<R, (bool, FsError)>
    where
        Q: Clone,
        F: Fn(TransferServiceClient<Channel>, Q) -> Fut,
        Fut: Future<Output = Result<Response<R>, Status>>,
    {
        let client = TransferServiceClient::new(channel.clone())
            .max_decoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES);
        match tokio::time::timeout(timeout, call(client, request.clone())).await {
            Ok(Ok(response)) => Ok(response.into_inner()),
            Ok(Err(status)) => {
                let transport_retry = status.details().is_empty()
                    && matches!(
                        status.code(),
                        Code::Unavailable
                            | Code::DeadlineExceeded
                            | Code::Cancelled
                            | Code::Internal
                            | Code::Unknown
                    );
                let error = curvine_error::grpc::from_status(status);
                Err((transport_retry || error.retry_master(), error))
            }
            Err(error) => Err((
                true,
                FsError::io(std::io::Error::new(std::io::ErrorKind::TimedOut, error)),
            )),
        }
    }

    pub async fn submit(&self, command: TransferCommand) -> FsResult<SubmitTransferResponse> {
        let req = SubmitTransferRequest {
            kind: transfer_kind_code(command.kind),
            source_path: command.source_path.clone(),
            target_path: command.target_path.clone(),
            client_request_id: command.client_request_id.clone(),
            submitter: command.submitter.clone(),
            tenant: command.tenant.clone(),
            command: serde_json::to_vec(&command).map_err(|e| FsError::common(e.to_string()))?,
            protocol_version: Some(TRANSFER_PROTOCOL_VERSION),
        };
        self.call(req, |mut client, req| async move {
            client.submit_transfer(req).await
        })
        .await
    }

    pub async fn status(&self, job_id: impl AsRef<str>) -> FsResult<GetTransferStatusResponse> {
        self.status_page(job_id, None, None).await
    }

    pub async fn retry(&self, job_id: impl AsRef<str>) -> FsResult<SubmitTransferResponse> {
        let req = RetryTransferRequest {
            job_id: job_id.as_ref().to_string(),
        };
        self.call(req, |mut client, req| async move {
            client.retry_transfer(req).await
        })
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
        self.call(req, |mut client, req| async move {
            client.get_transfer_status(req).await
        })
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
        self.call(req, |mut client, req| async move {
            client.list_transfers(req).await
        })
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
        self.call(req, |mut client, req| async move {
            client.list_transfer_tenants(req).await
        })
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
        self.call(req, |mut client, req| async move {
            client.watch_transfer(req).await
        })
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
        self.call(req, |mut client, req| async move {
            client.cancel_transfer(req).await
        })
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
        let response: TransferTaskReportResponse = self
            .call(req, |mut client, req| async move {
                client.report_transfer_task(req).await
            })
            .await?;
        Ok(response.accepted)
    }
}

fn transfer_endpoints(context: &Arc<FsContext>) -> Vec<String> {
    transfer_endpoints_from_conf(context.conf())
}

fn transfer_endpoints_from_conf(conf: &ClusterConf) -> Vec<String> {
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
        // Job-level PartialSuccess has no task-level counterpart; report as failed.
        JobTaskState::Failed | JobTaskState::PartialSuccess => 4,
        JobTaskState::Canceled => 5,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfer_probes_endpoints_concurrently_and_honors_pool_size() {
        let runtime = Arc::new(Runtime::single());
        let conf = curvine_rpc::client::ClientConf {
            conn_size: 3,
            ..Default::default()
        };
        let client = TransferClient::with_endpoints(
            conf,
            runtime.clone(),
            vec!["127.0.0.1:1".into(), "127.0.0.1:2".into()],
        )
        .unwrap();
        runtime.block_on(async {
            let calls = AtomicUsize::new(0);
            let result = tokio::time::timeout(
                Duration::from_millis(500),
                client.call((), |_, ()| {
                    let index = calls.fetch_add(1, Ordering::Relaxed);
                    async move {
                        if index == 0 {
                            std::future::pending().await
                        } else {
                            Ok(Response::new(42))
                        }
                    }
                }),
            )
            .await
            .expect("an unresponsive endpoint must not delay a healthy endpoint")
            .unwrap();
            assert_eq!(result, 42);
            let pools = client.inner.channels.get().unwrap();
            assert_eq!(pools.len(), 2);
            assert!(pools.iter().all(|pool| pool.len() == 3));
            assert_eq!(client.inner.preferred.load(Ordering::Relaxed), 1);
        });
    }
}
