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

use crate::worker::grpc_data::StreamTracker;
use crate::worker::WorkerService;
use curvine_error::FsError;
use curvine_fs_api::RpcCode;
use curvine_net::net::ConnState;
use curvine_proto::{worker_control_service_server::WorkerControlService, *};
use curvine_rpc::{
    handler::{HandlerService, MessageHandler},
    message::Builder,
};
use curvine_runtime::runtime::RpcRuntime;
use prost::Message as ProtoMessage;
use std::fmt::Debug;
use tonic::{Request, Response, Status};

/// Typed gRPC adapter for task and replication control operations.
pub(crate) struct WorkerControlGrpcHandler {
    service: WorkerService,
    tracker: StreamTracker,
}

impl WorkerControlGrpcHandler {
    pub(crate) fn new(service: WorkerService, tracker: StreamTracker) -> Self {
        Self { service, tracker }
    }

    async fn dispatch<Q, R>(
        &self,
        code: RpcCode,
        request: Request<Q>,
    ) -> Result<Response<R>, Status>
    where
        Q: ProtoMessage + Debug + Send + 'static,
        R: ProtoMessage + Default + Send + 'static,
    {
        let id = curvine_rpc::grpc::request_id(&request)?;
        let guard = self
            .tracker
            .register()
            .ok_or_else(|| Status::unavailable("worker is shutting down"))?;
        let peer = request
            .remote_addr()
            .map(|address| ConnState::new(address.into(), Default::default()));
        let message = Builder::new_rpc(code)
            .req_id(id)
            .proto_header(request.into_inner())
            .build();
        let handler = self.service.get_message_handler(peer);
        let response = self
            .service
            .clone_rt()
            .spawn_blocking(move || {
                let _guard = guard;
                handler.handle(&message)
            })
            .await
            .map_err(|e| Status::internal(format!("worker handler failed: {e}")))?
            .map_err(|e| curvine_error::grpc::to_status(&e))?;
        response
            .check_error_ext::<FsError>()
            .map_err(|e| curvine_error::grpc::to_status(&e))?;
        let result = response
            .parse_header::<R>()
            .map_err(|e| Status::data_loss(format!("invalid worker response payload: {e}")))?;
        Ok(Response::new(result))
    }
}

#[tonic::async_trait]
impl WorkerControlService for WorkerControlGrpcHandler {
    async fn submit_task(
        &self,
        request: Request<SubmitTaskRequest>,
    ) -> Result<Response<SubmitTaskResponse>, Status> {
        self.dispatch(RpcCode::SubmitTask, request).await
    }

    async fn query_transfer_task(
        &self,
        request: Request<QueryTransferTaskRequest>,
    ) -> Result<Response<QueryTransferTaskResponse>, Status> {
        self.dispatch(RpcCode::QueryTransferTask, request).await
    }

    async fn cancel_job(
        &self,
        request: Request<CancelJobRequest>,
    ) -> Result<Response<CancelJobResponse>, Status> {
        self.dispatch(RpcCode::CancelJob, request).await
    }

    async fn submit_block_replication_job(
        &self,
        request: Request<SubmitBlockReplicationRequest>,
    ) -> Result<Response<SubmitBlockReplicationResponse>, Status> {
        self.dispatch(RpcCode::SubmitBlockReplicationJob, request)
            .await
    }

    async fn submit_fenced_replication(
        &self,
        request: Request<SubmitBlockReplicationRequest>,
    ) -> Result<Response<SubmitBlockReplicationResponse>, Status> {
        self.dispatch(RpcCode::SubmitFencedReplication, request)
            .await
    }

    async fn prepare_replication(
        &self,
        request: Request<PrepareReplicationRequest>,
    ) -> Result<Response<PrepareReplicationResponse>, Status> {
        self.dispatch(RpcCode::PrepareReplication, request).await
    }

    async fn reconcile_replication(
        &self,
        request: Request<ReconcileReplicationRequest>,
    ) -> Result<Response<ReconcileReplicationResponse>, Status> {
        self.dispatch(RpcCode::ReconcileReplication, request).await
    }
}
