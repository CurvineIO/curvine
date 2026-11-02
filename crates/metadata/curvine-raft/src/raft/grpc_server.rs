// Copyright 2026 OPPO.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

//! Typed gRPC adapter for the existing Raft actor and snapshot reader.

use crate::proto::raft::{self, snapshot_download_chunk::Value};
use crate::raft::raft_server::RaftService;
use crate::raft::{grpc_error, RaftCode, RaftError};
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::{Builder, RequestStatus};
use futures::Stream;
use prost::Message as ProtoMessage;
use std::{pin::Pin, sync::Arc};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct RaftGrpcServer {
    service: Arc<RaftService>,
}

impl RaftGrpcServer {
    pub fn new(service: Arc<RaftService>) -> Self {
        Self { service }
    }

    async fn call<T: ProtoMessage, R: ProtoMessage + Default>(
        &self,
        request: Request<T>,
        code: RaftCode,
    ) -> Result<Response<R>, Status> {
        let id = curvine_rpc::grpc::request_id(&request)?;
        let message = Builder::new()
            .code(code)
            .request(RequestStatus::Open)
            .req_id(id)
            .proto_header(request.into_inner())
            .build();
        let handler = self.service.get_message_handler(None);
        let reply = handler
            .async_handle(message)
            .await
            .map_err(|e| grpc_error::to_status(&e))?;
        reply
            .check_error_ext::<RaftError>()
            .map_err(|e| grpc_error::to_status(&e))?;
        let header = reply
            .parse_header()
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(header))
    }
}

#[tonic::async_trait]
impl raft::raft_service_server::RaftService for RaftGrpcServer {
    async fn ping(
        &self,
        request: Request<raft::PingRequest>,
    ) -> Result<Response<raft::PingResponse>, Status> {
        self.call(request, RaftCode::Ping).await
    }
    async fn conf_change(
        &self,
        request: Request<raft::ConfChangeRequest>,
    ) -> Result<Response<raft::ConfChangeResponse>, Status> {
        self.call(request, RaftCode::ConfChange).await
    }
    async fn propose(
        &self,
        request: Request<raft::ProposeRequest>,
    ) -> Result<Response<raft::ProposeResponse>, Status> {
        self.call(request, RaftCode::Propose).await
    }
    async fn send_raft(
        &self,
        request: Request<raft::RaftRequest>,
    ) -> Result<Response<raft::RaftResponse>, Status> {
        self.call(request, RaftCode::Raft).await
    }
    async fn report_snapshot(
        &self,
        request: Request<raft::ReportSnapshotRequest>,
    ) -> Result<Response<raft::ReportSnapshotResponse>, Status> {
        self.call(request, RaftCode::ReportSnapshot).await
    }

    type DownloadSnapshotStream =
        Pin<Box<dyn Stream<Item = Result<raft::SnapshotDownloadChunk, Status>> + Send>>;

    async fn download_snapshot(
        &self,
        request: Request<raft::SnapshotDownloadRequest>,
    ) -> Result<Response<Self::DownloadSnapshotStream>, Status> {
        let id = curvine_rpc::grpc::request_id(&request)?;
        let header = request.into_inner();
        let handler = Arc::new(self.service.get_message_handler(None));
        let open = Builder::new()
            .code(RaftCode::SnapshotDownload)
            .request(RequestStatus::Open)
            .req_id(id)
            .seq_id(0)
            .proto_header(header)
            .build();
        let worker = handler.clone();
        let reply = tokio::task::spawn_blocking(move || worker.handle(&open))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|e| grpc_error::to_status(&e))?;
        reply
            .check_error_ext::<RaftError>()
            .map_err(|e| grpc_error::to_status(&e))?;
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let mut seq = 1i32;
            loop {
                if tx.is_closed() {
                    break;
                }
                let msg = Builder::new()
                    .code(RaftCode::SnapshotDownload)
                    .request(RequestStatus::Running)
                    .req_id(id)
                    .seq_id(seq)
                    .build();
                seq = match seq.checked_add(1) {
                    Some(n) => n,
                    None => break,
                };
                let worker = handler.clone();
                let result = tokio::task::spawn_blocking(move || worker.handle(&msg)).await;
                let response = match result {
                    Ok(Ok(reply)) => reply,
                    Ok(Err(e)) => {
                        let _ = tx.send(Err(grpc_error::to_status(&e))).await;
                        break;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                        break;
                    }
                };
                if let Err(e) = response.check_error_ext::<RaftError>() {
                    let _ = tx.send(Err(grpc_error::to_status(&e))).await;
                    break;
                }
                let value = if response.header_len() > 0 {
                    match response.parse_header::<raft::SnapshotDownloadResponse>() {
                        Ok(trailer) => Value::Trailer(trailer),
                        Err(e) => {
                            let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                            break;
                        }
                    }
                } else {
                    Value::Data(response.data.as_slice().to_vec())
                };
                let last = matches!(value, Value::Trailer(_));
                if tx
                    .send(Ok(raft::SnapshotDownloadChunk { value: Some(value) }))
                    .await
                    .is_err()
                    || last
                {
                    break;
                }
            }
        });
        let stream = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });
        Ok(Response::new(Box::pin(stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::raft::raft_service_server::RaftService as _;

    #[tokio::test]
    async fn ping_keeps_request_identity_and_uses_actor_response() {
        let mut service = RaftService::new(&crate::conf::JournalConf::default());
        let mut receiver = service.take_receiver().unwrap();
        let server = RaftGrpcServer::new(Arc::new(service));
        let actor = tokio::spawn(async move {
            let envelope = receiver.recv().await.unwrap();
            assert_eq!(envelope.msg.req_id(), 47);
            assert_eq!(RaftCode::from(envelope.msg.code()), RaftCode::Ping);
            let response = Builder::success(&envelope.msg)
                .proto_header(raft::PingResponse {
                    leader_id: 9,
                    group: vec![],
                })
                .build();
            envelope.send_with_log(Ok(response));
        });
        let mut request = Request::new(raft::PingRequest::default());
        curvine_rpc::grpc::set_request_id(&mut request, 47);
        assert_eq!(
            server.ping(request).await.unwrap().into_inner().leader_id,
            9
        );
        actor.await.unwrap();
    }
}
