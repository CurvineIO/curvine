// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use crate::proto::raft::{
    raft_service_client::RaftServiceClient, snapshot_download_chunk::Value, SnapshotDownloadChunk,
    SnapshotDownloadRequest, SnapshotDownloadResponse, SnapshotFileInfo,
};
use crate::raft::{RaftCode, RaftError, RaftResult};
use curvine_net::net::InetAddr;
use curvine_rpc::message::{Builder, Message, RequestStatus};
use curvine_runtime::common::Utils;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct SnapshotClient {
    dir: String,
    file: SnapshotFileInfo,
    req_id: i64,
    client: Mutex<RaftServiceClient<tonic::transport::Channel>>,
    stream: Mutex<Option<tonic::Streaming<SnapshotDownloadChunk>>>,
    remote_addr: InetAddr,
    timeout: Duration,
}

impl SnapshotClient {
    pub fn new_grpc(
        dir: impl Into<String>,
        file: SnapshotFileInfo,
        remote_addr: InetAddr,
        channel: tonic::transport::Channel,
        timeout: Duration,
    ) -> Self {
        Self {
            dir: dir.into(),
            file,
            req_id: Utils::req_id(),
            remote_addr,
            timeout,
            client: Mutex::new(
                RaftServiceClient::new(channel)
                    .max_decoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES),
            ),
            stream: Mutex::new(None),
        }
    }

    pub async fn open_file(
        &self,
        _seq_id: i32,
        offset: u64,
    ) -> RaftResult<SnapshotDownloadResponse> {
        let mut request = tonic::Request::new(SnapshotDownloadRequest {
            dir: self.dir.clone(),
            snapshot_file: self.file.clone(),
            offset,
        });
        curvine_rpc::grpc::set_request_id(&mut request, self.req_id);
        let call = async { self.client.lock().await.download_snapshot(request).await };
        let response = tokio::time::timeout(self.timeout, call)
            .await
            .map_err(|_| {
                RaftError::io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "snapshot open timed out",
                ))
            })?
            .map_err(crate::raft::grpc_error::from_status)?;
        *self.stream.lock().await = Some(response.into_inner());
        Ok(SnapshotDownloadResponse::default())
    }

    pub async fn read_data(&self, seq_id: i32) -> RaftResult<Message> {
        let mut guard = self.stream.lock().await;
        let incoming = guard
            .as_mut()
            .ok_or_else(|| RaftError::other("snapshot stream is not open".into()))?;
        let frame = tokio::time::timeout(self.timeout, incoming.message())
            .await
            .map_err(|_| {
                RaftError::io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "snapshot read timed out",
                ))
            })?
            .map_err(crate::raft::grpc_error::from_status)?
            .ok_or_else(|| {
                RaftError::other("snapshot stream ended before checksum trailer".into())
            })?;
        let request = Builder::new()
            .code(RaftCode::SnapshotDownload)
            .request(RequestStatus::Running)
            .req_id(self.req_id)
            .seq_id(seq_id)
            .build();
        match frame.value {
            Some(Value::Data(data)) => Ok(Builder::success(&request)
                .data(curvine_io::DataSlice::Buffer(data.as_slice().into()))
                .build()),
            Some(Value::Trailer(trailer)) => {
                Ok(Builder::success(&request).proto_header(trailer).build())
            }
            None => Err(RaftError::other("empty snapshot stream frame".into())),
        }
    }

    pub async fn close_file(&self, _seq_id: i32) -> RaftResult<()> {
        *self.stream.lock().await = None;
        Ok(())
    }

    pub fn file_len(&self) -> u64 {
        self.file.len
    }
    pub fn file_path(&self) -> &str {
        &self.file.path
    }
    pub fn remote_addr(&self) -> &InetAddr {
        &self.remote_addr
    }
}
