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

use std::time::Duration;

use curvine_error::FsResult;
use curvine_fs_api::RpcCode;
use curvine_model::{JobTaskType, LoadTaskInfo};
use curvine_proto::worker_control_service_client::WorkerControlServiceClient;
use curvine_proto::*;
use curvine_runtime::common::SerdeUtils;
use curvine_runtime::common::Utils;
use prost::Message as PMessage;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct JobWorkerClient {
    client: Channel,
    timeout: Duration,
}

impl JobWorkerClient {
    pub(crate) fn with_grpc(channel: Channel, timeout: Duration) -> Self {
        Self {
            client: channel,
            timeout,
        }
    }

    pub async fn connect_grpc(host: &str, port: u16, timeout: Duration) -> FsResult<Self> {
        Self::connect_grpc_with_config(host, port, timeout, &Default::default()).await
    }

    pub async fn connect_grpc_with_config(
        host: &str,
        port: u16,
        timeout: Duration,
        grpc: &curvine_rpc::grpc::GrpcConfig,
    ) -> FsResult<Self> {
        let uri = grpc.uri(host, port);
        let tls = grpc
            .client_tls(host)
            .map_err(|e| curvine_error::FsError::common(e.to_string()))?;
        let transport = curvine_rpc::grpc::TransportConfig {
            connect_timeout: timeout,
            request_timeout: Some(timeout),
            ..Default::default()
        };
        let endpoint = transport
            .endpoint(uri, tls.as_ref())
            .map_err(|e| curvine_error::FsError::common(e.to_string()))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| curvine_error::FsError::io(std::io::Error::other(e)))?;
        Ok(Self::with_grpc(channel, timeout))
    }

    pub async fn rpc<T, R>(&self, code: RpcCode, header: T) -> FsResult<R>
    where
        T: PMessage + Default,
        R: PMessage + Default,
    {
        {
            let mut client = WorkerControlServiceClient::new(self.client.clone())
                .max_decoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES)
                .max_encoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES);
            let bytes = header.encode_to_vec();
            let id = Utils::req_id();
            macro_rules! call {
                ($method:ident, $request:ty) => {{
                    let request = <$request>::decode(bytes.as_slice())
                        .map_err(|e| curvine_error::FsError::common(e.to_string()))?;
                    let mut request = tonic::Request::new(request);
                    curvine_rpc::grpc::set_request_id(&mut request, id);
                    let response = tokio::time::timeout(self.timeout, client.$method(request))
                        .await
                        .map_err(|e| {
                            curvine_error::FsError::io(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                e,
                            ))
                        })?
                        .map_err(curvine_error::grpc::from_status)?
                        .into_inner();
                    R::decode(response.encode_to_vec().as_slice())
                        .map_err(|e| curvine_error::FsError::common(e.to_string()))
                }};
            }
            match code {
                RpcCode::SubmitTask => call!(submit_task, SubmitTaskRequest),
                RpcCode::CancelJob => call!(cancel_job, CancelJobRequest),
                RpcCode::QueryTransferTask => {
                    call!(query_transfer_task, QueryTransferTaskRequest)
                }
                RpcCode::PrepareReplication => {
                    call!(prepare_replication, PrepareReplicationRequest)
                }
                RpcCode::ReconcileReplication => {
                    call!(reconcile_replication, ReconcileReplicationRequest)
                }
                RpcCode::SubmitBlockReplicationJob => {
                    call!(submit_block_replication_job, SubmitBlockReplicationRequest)
                }
                RpcCode::SubmitFencedReplication => {
                    call!(submit_fenced_replication, SubmitBlockReplicationRequest)
                }
                _ => Err(curvine_error::FsError::unsupported(format!(
                    "unsupported worker control RPC: {code}"
                ))),
            }
        }
    }

    pub async fn submit_load_task_response(
        &self,
        task: LoadTaskInfo,
    ) -> FsResult<SubmitTaskResponse> {
        let request = SubmitTaskRequest {
            task_type: JobTaskType::Load.into(),
            task_command: SerdeUtils::serialize(&task)?,
        };

        self.rpc(RpcCode::SubmitTask, request).await
    }

    pub async fn submit_load_task(&self, task: LoadTaskInfo) -> FsResult<()> {
        let response = self.submit_load_task_response(task).await?;
        if !response.accepted.unwrap_or(true) {
            return Err(curvine_error::FsError::common(format!(
                "Worker rejected load task {}: {}",
                response.task_id,
                response
                    .reject_reason
                    .unwrap_or_else(|| "no reason provided".to_string())
            )));
        }
        Ok(())
    }

    pub async fn cancel_job(&self, job_id: impl AsRef<str>) -> FsResult<()> {
        let request = CancelJobRequest {
            job_id: job_id.as_ref().to_string(),
        };

        let _: CancelJobResponse = self.rpc(RpcCode::CancelJob, request).await?;
        Ok(())
    }

    pub async fn query_transfer_task(
        &self,
        job_id: impl AsRef<str>,
        run_id: u64,
        task_id: impl AsRef<str>,
        attempt_id: u64,
        worker_session_id: impl AsRef<str>,
    ) -> FsResult<QueryTransferTaskResponse> {
        let request = QueryTransferTaskRequest {
            job_id: job_id.as_ref().to_string(),
            run_id,
            task_id: task_id.as_ref().to_string(),
            attempt_id,
            worker_session_id: worker_session_id.as_ref().to_string(),
        };

        self.rpc(RpcCode::QueryTransferTask, request).await
    }
}

#[cfg(test)]
mod grpc_tests {
    use super::*;
    use curvine_proto::worker_control_service_server::{
        WorkerControlService, WorkerControlServiceServer,
    };
    use tonic::{Request, Response, Status};

    struct FakeWorker;

    #[tonic::async_trait]
    impl WorkerControlService for FakeWorker {
        async fn submit_task(
            &self,
            _: Request<SubmitTaskRequest>,
        ) -> Result<Response<SubmitTaskResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn query_transfer_task(
            &self,
            _: Request<QueryTransferTaskRequest>,
        ) -> Result<Response<QueryTransferTaskResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn cancel_job(
            &self,
            _: Request<CancelJobRequest>,
        ) -> Result<Response<CancelJobResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn submit_block_replication_job(
            &self,
            _: Request<SubmitBlockReplicationRequest>,
        ) -> Result<Response<SubmitBlockReplicationResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn submit_fenced_replication(
            &self,
            request: Request<SubmitBlockReplicationRequest>,
        ) -> Result<Response<SubmitBlockReplicationResponse>, Status> {
            curvine_rpc::grpc::request_id(&request)?;
            let request = request.into_inner();
            assert_eq!(request.target_token.as_deref(), Some("token"));
            Ok(Response::new(SubmitBlockReplicationResponse {
                success: true,
                message: None,
                attempt_id: request.attempt_id,
            }))
        }
        async fn prepare_replication(
            &self,
            request: Request<PrepareReplicationRequest>,
        ) -> Result<Response<PrepareReplicationResponse>, Status> {
            curvine_rpc::grpc::request_id(&request)?;
            assert_eq!(request.get_ref().attempt_id, "attempt");
            Ok(Response::new(PrepareReplicationResponse {
                token: "token".into(),
            }))
        }
        async fn reconcile_replication(
            &self,
            request: Request<ReconcileReplicationRequest>,
        ) -> Result<Response<ReconcileReplicationResponse>, Status> {
            curvine_rpc::grpc::request_id(&request)?;
            assert_eq!(request.get_ref().attempt_id, "attempt");
            Ok(Response::new(ReconcileReplicationResponse {
                storage_type: None,
            }))
        }
    }

    #[tokio::test]
    async fn replication_control_calls_use_typed_grpc_and_request_ids() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let incoming = futures::stream::unfold(listener, |listener| async move {
            Some((listener.accept().await.map(|(stream, _)| stream), listener))
        });
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(WorkerControlServiceServer::new(FakeWorker))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = stop_rx.await;
                })
                .await
                .unwrap();
        });
        let client = JobWorkerClient::connect_grpc("127.0.0.1", port, Duration::from_secs(2))
            .await
            .unwrap();
        let prepared: PrepareReplicationResponse = client
            .rpc(
                RpcCode::PrepareReplication,
                PrepareReplicationRequest {
                    block_id: 9,
                    attempt_id: "attempt".into(),
                    target_session_id: "session".into(),
                    lifetime_ms: 1000,
                },
            )
            .await
            .unwrap();
        let submitted: SubmitBlockReplicationResponse = client
            .rpc(
                RpcCode::SubmitFencedReplication,
                SubmitBlockReplicationRequest {
                    block_id: 9,
                    target_worker_info: WorkerAddressProto::default(),
                    attempt_id: Some("attempt".into()),
                    target_token: Some(prepared.token),
                    job_timeout_ms: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(submitted.attempt_id.as_deref(), Some("attempt"));
        let reconciled: ReconcileReplicationResponse = client
            .rpc(
                RpcCode::ReconcileReplication,
                ReconcileReplicationRequest {
                    block_id: 9,
                    attempt_id: "attempt".into(),
                    target_session_id: "session".into(),
                    release: true,
                },
            )
            .await
            .unwrap();
        assert_eq!(reconciled.storage_type, None);
        drop(client);
        let _ = stop_tx.send(());
        server.await.unwrap();
    }
}
