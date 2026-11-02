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

use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use curvine_error::FsError;
use curvine_fs_api::RpcCode;
use curvine_proto::transfer_service_server::TransferService as TransferGrpcService;
use curvine_proto::*;
use curvine_rpc::handler::MessageHandler;
use curvine_rpc::message::Builder;
use prost::Message as ProtoMessage;
use tokio::sync::Notify;
use tonic::{Request, Response, Status};

use super::{TransferHandler, TransferService, TransferStore};

/// Typed gRPC front end for the same transfer operations used by the legacy
/// RPC server. Store access is synchronous and must not block a Tokio worker.
pub struct TransferGrpcHandler<S> {
    service: TransferService<S>,
    tracker: TransferRequestTracker,
}

#[derive(Default)]
struct TrackerState {
    closing: bool,
    active: usize,
}

#[derive(Default)]
struct TrackerInner {
    state: Mutex<TrackerState>,
    drained: Notify,
}

#[derive(Clone, Default)]
pub(crate) struct TransferRequestTracker(Arc<TrackerInner>);

struct RequestGuard(TransferRequestTracker);

impl Drop for RequestGuard {
    fn drop(&mut self) {
        let mut state = self.0 .0.state.lock().expect("transfer tracker poisoned");
        state.active -= 1;
        if state.active == 0 {
            self.0 .0.drained.notify_waiters();
        }
    }
}

impl TransferRequestTracker {
    fn register(&self) -> Option<RequestGuard> {
        let mut state = self.0.state.lock().expect("transfer tracker poisoned");
        if state.closing {
            return None;
        }
        state.active += 1;
        Some(RequestGuard(self.clone()))
    }

    pub(crate) fn shutdown(&self) {
        self.0
            .state
            .lock()
            .expect("transfer tracker poisoned")
            .closing = true;
    }

    pub(crate) async fn wait_drained(&self) {
        loop {
            let notified = self.0.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .0
                .state
                .lock()
                .expect("transfer tracker poisoned")
                .active
                == 0
            {
                return;
            }
            notified.await;
        }
    }
}

impl<S> TransferGrpcHandler<S>
where
    S: TransferStore,
{
    pub fn new(service: TransferService<S>) -> Self {
        Self::with_tracker(service, TransferRequestTracker::default())
    }

    pub(crate) fn with_tracker(
        service: TransferService<S>,
        tracker: TransferRequestTracker,
    ) -> Self {
        Self { service, tracker }
    }

    // This adapter implements tonic's Status-returning service contract.
    #[allow(clippy::result_large_err)]
    async fn dispatch<Q, R>(&self, code: RpcCode, request: Q) -> Result<Response<R>, Status>
    where
        Q: ProtoMessage + Debug + Send + 'static,
        R: ProtoMessage + Default + Send + 'static,
    {
        let guard = self
            .tracker
            .register()
            .ok_or_else(|| Status::unavailable("transfer service is shutting down"))?;
        let service = self.service.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let message = Builder::new_rpc(code).proto_header(request).build();
            let response = TransferHandler::new(service)
                .handle(&message)
                .map_err(|error| curvine_error::grpc::to_status(&error))?;
            response
                .check_error_ext::<FsError>()
                .map_err(|error| curvine_error::grpc::to_status(&error))?;
            let result = response.parse_header::<R>().map_err(|e| {
                Status::data_loss(format!("invalid transfer response payload: {e}"))
            })?;
            Ok(Response::new(result))
        })
        .await
        .map_err(|e| Status::internal(format!("transfer handler failed: {e}")))?
    }
}

#[tonic::async_trait]
impl<S> TransferGrpcService for TransferGrpcHandler<S>
where
    S: TransferStore,
{
    async fn submit_transfer(
        &self,
        request: Request<SubmitTransferRequest>,
    ) -> Result<Response<SubmitTransferResponse>, Status> {
        self.dispatch(RpcCode::SubmitTransfer, request.into_inner())
            .await
    }

    async fn get_transfer_status(
        &self,
        request: Request<GetTransferStatusRequest>,
    ) -> Result<Response<GetTransferStatusResponse>, Status> {
        self.dispatch(RpcCode::GetTransferStatus, request.into_inner())
            .await
    }

    async fn retry_transfer(
        &self,
        request: Request<RetryTransferRequest>,
    ) -> Result<Response<SubmitTransferResponse>, Status> {
        self.dispatch(RpcCode::RetryTransfer, request.into_inner())
            .await
    }

    async fn list_transfers(
        &self,
        request: Request<ListTransfersRequest>,
    ) -> Result<Response<ListTransfersResponse>, Status> {
        self.dispatch(RpcCode::ListTransfers, request.into_inner())
            .await
    }

    async fn list_transfer_tenants(
        &self,
        request: Request<ListTransferTenantsRequest>,
    ) -> Result<Response<ListTransferTenantsResponse>, Status> {
        self.dispatch(RpcCode::ListTransferTenants, request.into_inner())
            .await
    }

    async fn watch_transfer(
        &self,
        request: Request<WatchTransferRequest>,
    ) -> Result<Response<WatchTransferResponse>, Status> {
        self.dispatch(RpcCode::WatchTransfer, request.into_inner())
            .await
    }

    async fn cancel_transfer(
        &self,
        request: Request<CancelTransferRequest>,
    ) -> Result<Response<CancelTransferResponse>, Status> {
        self.dispatch(RpcCode::CancelTransfer, request.into_inner())
            .await
    }

    async fn report_transfer_task(
        &self,
        request: Request<TransferTaskReportRequest>,
    ) -> Result<Response<TransferTaskReportResponse>, Status> {
        self.dispatch(RpcCode::ReportTransferTask, request.into_inner())
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::transfer::MemoryTransferStore;
    use curvine_error::ErrorKind;
    use curvine_proto::transfer_service_client::TransferServiceClient;
    use curvine_proto::transfer_service_server::TransferServiceServer;

    #[tokio::test]
    async fn shutdown_waits_for_cancelled_blocking_store_work() {
        let tracker = TransferRequestTracker::default();
        let guard = tracker.register().unwrap();
        let (release, wait) = std::sync::mpsc::channel();
        let task = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            wait.recv().unwrap();
        });
        drop(task);
        tracker.shutdown();
        assert!(tracker.register().is_none());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), tracker.wait_drained())
                .await
                .is_err()
        );
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), tracker.wait_drained())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn grpc_submit_is_idempotent_and_preserves_domain_errors() {
        let store = Arc::new(MemoryTransferStore::new());
        let service = TransferGrpcHandler::new(TransferService::new(store));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = futures::stream::unfold(listener, |listener| async move {
            Some((listener.accept().await.map(|(stream, _)| stream), listener))
        });
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TransferServiceServer::new(service))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = stop_rx.await;
                })
                .await
                .unwrap();
        });
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = TransferServiceClient::new(channel);
        let request = SubmitTransferRequest {
            kind: curvine_model::TransferKind::Load as i32,
            source_path: "s3://bucket/source".into(),
            target_path: "/cv/target".into(),
            client_request_id: "stable-request".into(),
            submitter: "test".into(),
            tenant: "default".into(),
            command: Vec::new(),
            protocol_version: Some(1),
        };
        let first = client
            .submit_transfer(request.clone())
            .await
            .unwrap()
            .into_inner();
        let second = client.submit_transfer(request).await.unwrap().into_inner();
        assert_eq!(first.job_id, second.job_id);
        let status = client
            .get_transfer_status(GetTransferStatusRequest {
                job_id: "missing".into(),
                page_size: None,
                page_token: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(
            curvine_error::grpc::from_status(status).kind(),
            ErrorKind::JobNotFound
        ));
        // Exercise the production client as well as the generated client so
        // endpoint setup and status conversion are covered end to end.
        let mut conf = curvine_config::ClusterConf::format();
        conf.transfer.hostname = "127.0.0.1".into();
        conf.transfer.rpc_port = address.port();
        conf.transfer.endpoints.clear();
        let client_rt = Arc::new(curvine_runtime::runtime::Runtime::single());
        let transfer_client =
            curvine_job_client::TransferClient::with_rt(&conf, client_rt.clone()).unwrap();
        let error = transfer_client.status("missing").await.unwrap_err();
        assert!(matches!(error.kind(), ErrorKind::JobNotFound));
        drop(transfer_client);
        tokio::task::spawn_blocking(move || drop(client_rt))
            .await
            .unwrap();
        drop(client);
        let _ = stop_tx.send(());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn transfer_client_uses_mutual_tls() {
        let fixture = |name: &str| {
            format!(
                "{}/../../core/rpc/tests/fixtures/grpc_tls/{name}",
                env!("CARGO_MANIFEST_DIR")
            )
        };
        let grpc = curvine_rpc::grpc::GrpcConfig {
            tls_enabled: true,
            ca_cert_path: fixture("ca.pem"),
            server_cert_path: fixture("server.pem"),
            server_key_path: fixture("server.key"),
            require_client_auth: true,
            ..Default::default()
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = futures::stream::unfold(listener, |listener| async move {
            Some((listener.accept().await.map(|(stream, _)| stream), listener))
        });
        let service =
            TransferGrpcHandler::new(TransferService::new(Arc::new(MemoryTransferStore::new())));
        let mut server = curvine_rpc::grpc::TransportConfig::default()
            .server(grpc.server_tls().unwrap().as_ref())
            .unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let server_task = tokio::spawn(async move {
            server
                .add_service(TransferServiceServer::new(service))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        let mut conf = curvine_config::ClusterConf::format();
        conf.transfer.hostname = "127.0.0.1".into();
        conf.transfer.rpc_port = address.port();
        conf.transfer.endpoints.clear();
        conf.client.grpc = curvine_rpc::grpc::GrpcConfig {
            tls_enabled: true,
            ca_cert_path: fixture("ca.pem"),
            client_cert_path: fixture("client.pem"),
            client_key_path: fixture("client.key"),
            domain_name: "localhost".into(),
            ..Default::default()
        };
        let rt = Arc::new(curvine_runtime::runtime::Runtime::single());
        let client = curvine_job_client::TransferClient::with_rt(&conf, rt.clone()).unwrap();
        let error = client.status("missing").await.unwrap_err();
        assert!(matches!(error.kind(), ErrorKind::JobNotFound));
        drop(client);
        tokio::task::spawn_blocking(move || drop(rt)).await.unwrap();
        let _ = stop.send(());
        server_task.await.unwrap();
    }
}
