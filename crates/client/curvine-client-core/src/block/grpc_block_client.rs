// Copyright 2026 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

//! Client transport for the typed block data stream.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::block::adaptive_read::{AdaptiveReadWindow, MAX_AUTO_BYTES};
use curvine_error::{FsError, FsResult};
use curvine_fs_api::RpcCode;
use curvine_io::DataSlice;
use curvine_proto::block_data_request::Operation;
use curvine_proto::block_data_response::Result as OperationResult;
use curvine_proto::block_data_service_client::BlockDataServiceClient;
use curvine_proto::{BlockDataRequest, BlockDataResponse, ReadBlockChunk, WriteBlockChunk};
use curvine_rpc::message::{Builder, Message, RequestStatus};
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

struct Exchange {
    tx: mpsc::Sender<BlockDataRequest>,
    rx: tonic::Streaming<BlockDataResponse>,
    pending: VecDeque<PendingWrite>,
    read: Option<ReadWindow>,
}

struct ReadWindow {
    len: i64,
    next_offset: i64,
    delivered_offset: i64,
    chunk_size: usize,
    next_seq: i32,
    pending: VecDeque<PendingRead>,
}

struct PendingRead {
    seq: i32,
    sample: Option<(u64, Instant)>,
}

enum ReadPolicy {
    Fixed(usize),
    Adaptive(AdaptiveReadWindow),
}

impl ReadPolicy {
    fn window(&self, chunk_size: usize) -> usize {
        match self {
            Self::Fixed(window) => *window,
            Self::Adaptive(controller) => controller.window(chunk_size),
        }
    }

    fn budget(&self) -> usize {
        match self {
            Self::Fixed(_) => 32 * 1024 * 1024,
            Self::Adaptive(_) => MAX_AUTO_BYTES,
        }
    }

    fn end_stream(&mut self) {
        if let Self::Adaptive(controller) = self {
            controller.end_stream();
        }
    }

    fn fail(&mut self) {
        if let Self::Adaptive(controller) = self {
            controller.fail();
        }
    }
}

struct ClientState {
    stream: Option<Exchange>,
    // The pool is keyed by worker and keeps this client with its channel.
    // Learned state therefore survives stream resets, but never moves to a
    // different worker or survives destruction of this client.
    read_policy: ReadPolicy,
}

impl ReadWindow {
    fn sequence(&mut self, count: usize) -> FsResult<i32> {
        let seq = self.next_seq;
        self.next_seq = seq
            .checked_add(count as i32)
            .ok_or_else(|| FsError::common("block read sequence exhausted"))?;
        Ok(seq)
    }
}

struct PendingWrite {
    req_id: i64,
    seq_id: i32,
    deadline: tokio::time::Instant,
}

impl Exchange {
    async fn acknowledge_write(&mut self) -> FsResult<()> {
        let pending = self.pending.front().expect("pending write");
        let response = tokio::time::timeout_at(pending.deadline, self.rx.message())
            .await
            .map_err(FsError::from)?
            .map_err(curvine_error::grpc::from_status)?
            .ok_or_else(|| FsError::common("block stream closed without acknowledgement"))?;
        if response.req_id != pending.req_id || response.seq_id != pending.seq_id {
            return Err(FsError::common("block acknowledgement identity mismatch"));
        }
        let result = response
            .result
            .ok_or_else(|| FsError::common("missing block acknowledgement"))?;
        self.pending.pop_front();
        match result {
            OperationResult::Ack(true) => Ok(()),
            OperationResult::Error(detail) => Err(curvine_error::grpc::from_detail(&detail)
                .unwrap_or_else(|| FsError::common(detail.message))),
            _ => Err(FsError::common("unexpected block write acknowledgement")),
        }
    }

    async fn drain_writes(&mut self) -> FsResult<()> {
        while !self.pending.is_empty() {
            self.acknowledge_write().await?;
        }
        Ok(())
    }
}

/// An idle client holds an HTTP/2 channel; each block gets its own stream.
pub struct GrpcBlockClient {
    client: BlockDataServiceClient<Channel>,
    state: Mutex<ClientState>,
    timeout: Duration,
}

impl GrpcBlockClient {
    /// Returning a client to the idle pool must close an unfinished block
    /// stream so the next borrower cannot inherit its writer or reader.
    pub fn reset(&self) {
        if let Ok(mut state) = self.state.try_lock() {
            if state.stream.take().is_some() {
                state.read_policy.end_stream();
            }
        }
    }

    pub async fn connect(uri: String, timeout: Duration) -> FsResult<Self> {
        Self::connect_with_tls(uri, timeout, None).await
    }

    pub async fn connect_with_tls(
        uri: String,
        timeout: Duration,
        tls: Option<&curvine_rpc::grpc::ClientTls>,
    ) -> FsResult<Self> {
        let transport = curvine_rpc::grpc::TransportConfig {
            connect_timeout: timeout,
            request_timeout: None,
            ..Default::default()
        };
        let endpoint = transport
            .endpoint(uri, tls)
            .map_err(|e| FsError::common(e.to_string()))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| FsError::common(e.to_string()))?;
        Ok(Self {
            client: BlockDataServiceClient::new(channel)
                .max_decoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES)
                .max_encoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES),
            state: Mutex::new(ClientState {
                stream: None,
                read_policy: ReadPolicy::Fixed(1),
            }),
            timeout,
        })
    }

    /// Select a fixed window. As before, values outside 1..=32 are clamped.
    pub fn with_read_window(mut self, window: usize) -> Self {
        self.state.get_mut().read_policy = ReadPolicy::Fixed(window.clamp(1, 32));
        self
    }

    /// Select the experimental automatic read window for this connection.
    pub fn with_adaptive_read_window(mut self) -> Self {
        self.state.get_mut().read_policy = ReadPolicy::Adaptive(AdaptiveReadWindow::new());
        self
    }

    pub async fn rpc(&self, mut msg: Message, token: Option<&str>) -> FsResult<Message> {
        let mut frame = request_frame(&mut msg, token)?;
        let is_open = msg.request_status() == RequestStatus::Open;
        let is_done = matches!(
            msg.request_status(),
            RequestStatus::Complete | RequestStatus::Cancel
        );
        let mut state = self.state.lock().await;
        let ClientState {
            stream,
            read_policy,
        } = &mut *state;
        let exchange = async {
            if is_open {
                if stream.is_some() {
                    return Err(FsError::common("block stream is already open"));
                }
                let (tx, rx) = mpsc::channel(1);
                let mut client = self.client.clone();
                let response = client
                    .exchange(ReceiverStream::new(rx))
                    .await
                    .map_err(curvine_error::grpc::from_status)?;
                *stream = Some(Exchange {
                    tx,
                    rx: response.into_inner(),
                    pending: VecDeque::new(),
                    read: match frame.operation.as_ref() {
                        Some(Operation::OpenRead(open)) => Some(ReadWindow {
                            len: open.len,
                            next_offset: open.off,
                            delivered_offset: open.off,
                            chunk_size: open.chunk_size.max(1) as usize,
                            next_seq: frame.seq_id,
                            pending: VecDeque::new(),
                        }),
                        _ => None,
                    },
                });
            }
            let exchange = stream
                .as_mut()
                .ok_or_else(|| FsError::common("block stream is not open"))?;
            exchange.drain_writes().await?;
            if let Some(read) = &mut exchange.read {
                let chunk = match frame.operation.as_mut() {
                    Some(Operation::ReadChunk(chunk)) => Some(chunk),
                    _ => None,
                };
                let seek = chunk.as_ref().and_then(|chunk| chunk.header.as_ref());
                if chunk.is_none() || seek.is_some() {
                    // A seek or completion is a barrier: discard prefetched replies,
                    // but verify every identity before issuing a new operation.
                    while let Some(pending) = read.pending.front() {
                        let response = exchange
                            .rx
                            .message()
                            .await
                            .map_err(curvine_error::grpc::from_status)?
                            .ok_or_else(|| FsError::common("read stream closed while draining"))?;
                        if response.req_id != msg.req_id() || response.seq_id != pending.seq {
                            return Err(FsError::common("prefetched read identity mismatch"));
                        }
                        // Fixed windows have always discarded these replies
                        // after checking their identities. Only adaptive mode
                        // treats speculative failures as a reason to stop.
                        if matches!(&*read_policy, ReadPolicy::Adaptive(_)) {
                            match response.result {
                                Some(OperationResult::Data(_)) => (),
                                Some(OperationResult::Error(detail)) => {
                                    return Err(curvine_error::grpc::from_detail(&detail)
                                        .unwrap_or_else(|| FsError::common(detail.message)));
                                }
                                _ => {
                                    return Err(FsError::common(
                                        "unexpected prefetched read response",
                                    ))
                                }
                            }
                        }
                        read.pending.pop_front();
                    }
                    if let Some(header) = seek {
                        if let ReadPolicy::Adaptive(controller) = &mut *read_policy {
                            if header.offset != read.delivered_offset {
                                let eof_rewind = read.delivered_offset >= read.len
                                    && header.offset < read.delivered_offset;
                                controller.seek(eof_rewind);
                            }
                        }
                        read.next_offset = header.offset;
                        read.delivered_offset = header.offset;
                    }
                }
                if let Some(chunk) = chunk {
                    let remaining = read.len.saturating_sub(read.next_offset).max(0) as usize;
                    let window = read_policy.window(read.chunk_size);
                    if read.pending.len() < window && (remaining > 0 || read.pending.is_empty()) {
                        let available = remaining.div_ceil(read.chunk_size).max(1);
                        // Automatic mode bounds all pending chunks; fixed mode
                        // retains its existing per-burst budget and behavior.
                        let budget = (read_policy.budget() / read.chunk_size).max(1);
                        let count = (window - read.pending.len()).min(available).min(budget);
                        chunk.chunks = Some(count as u32);
                        let seq = read.next_seq;
                        let next_seq = seq
                            .checked_add(count as i32)
                            .ok_or_else(|| FsError::common("block read sequence exhausted"))?;
                        frame.seq_id = seq;
                        let sample = match &*read_policy {
                            ReadPolicy::Fixed(_) => None,
                            ReadPolicy::Adaptive(controller) => {
                                Some((controller.generation(), Instant::now()))
                            }
                        };
                        exchange
                            .tx
                            .send(frame)
                            .await
                            .map_err(|e| FsError::common(e.to_string()))?;
                        // No state is changed until the send succeeds, so a
                        // cancelled send cannot leave phantom queued reads.
                        read.next_seq = next_seq;
                        for index in 0..count {
                            read.pending.push_back(PendingRead {
                                seq: seq + index as i32,
                                sample,
                            });
                        }
                        read.next_offset = read
                            .next_offset
                            .saturating_add(count.saturating_mul(read.chunk_size) as i64)
                            .min(read.len);
                    }
                    let pending = read.pending.front().expect("scheduled read");
                    let mut response = exchange
                        .rx
                        .message()
                        .await
                        .map_err(curvine_error::grpc::from_status)?
                        .ok_or_else(|| FsError::common("read stream closed without response"))?;
                    if response.req_id != msg.req_id() || response.seq_id != pending.seq {
                        return Err(FsError::common("prefetched read identity mismatch"));
                    }
                    if let Some(OperationResult::Data(data)) = &response.result {
                        read.delivered_offset =
                            read.delivered_offset.saturating_add(data.len() as i64);
                        if let (Some((generation, sent)), ReadPolicy::Adaptive(controller)) =
                            (pending.sample, &mut *read_policy)
                        {
                            controller.observe(
                                generation,
                                sent,
                                Instant::now(),
                                data.len(),
                                AdaptiveReadWindow::max_window(read.chunk_size),
                            );
                        }
                    }
                    read.pending.pop_front();
                    response.seq_id = msg.seq_id();
                    return response_message(&msg, response);
                }
                frame.seq_id = read.sequence(1)?;
            }
            let wire_seq = frame.seq_id;
            exchange
                .tx
                .send(frame)
                .await
                .map_err(|e| FsError::common(e.to_string()))?;
            let mut response = exchange
                .rx
                .message()
                .await
                .map_err(curvine_error::grpc::from_status)?
                .ok_or_else(|| FsError::common("block stream closed without acknowledgement"))?;
            if response.seq_id != wire_seq {
                return Err(FsError::common("block acknowledgement identity mismatch"));
            }
            response.seq_id = msg.seq_id();
            response_message(&msg, response)
        };
        let result = match tokio::time::timeout(self.timeout, exchange).await {
            Ok(result) => result,
            Err(e) => Err(FsError::from(e)),
        };
        if result.is_err()
            || (RpcCode::from(msg.code()) == RpcCode::ReadBlock
                && result.as_ref().is_ok_and(|reply| !reply.is_success()))
            || (is_done && result.as_ref().is_ok_and(|reply| reply.is_success()))
            || (is_open && result.as_ref().is_ok_and(|reply| !reply.is_success()))
        {
            if result.is_err()
                || (RpcCode::from(msg.code()) == RpcCode::ReadBlock
                    && result.as_ref().is_ok_and(|reply| !reply.is_success()))
            {
                read_policy.fail();
            } else {
                read_policy.end_stream();
            }
            *stream = None;
        }
        result
    }

    /// Queue a data chunk while bounding the number of unacknowledged writes.
    /// A window of one preserves the synchronous per-chunk acknowledgement.
    pub async fn write_pipelined(
        &self,
        mut msg: Message,
        token: Option<&str>,
        window: usize,
    ) -> FsResult<()> {
        if window <= 1 {
            return self.rpc(msg, token).await?.check_error_ext::<FsError>();
        }
        let frame = request_frame(&mut msg, token)?;
        let mut state = self.state.lock().await;
        let stream = &mut state.stream;
        let result = async {
            let exchange = stream
                .as_mut()
                .ok_or_else(|| FsError::common("block stream is not open"))?;
            if exchange.pending.len() >= window {
                exchange.acknowledge_write().await?;
            }
            let deadline = tokio::time::Instant::now() + self.timeout;
            tokio::time::timeout_at(deadline, exchange.tx.send(frame))
                .await
                .map_err(FsError::from)?
                .map_err(|error| FsError::common(error.to_string()))?;
            exchange.pending.push_back(PendingWrite {
                req_id: msg.req_id(),
                seq_id: msg.seq_id(),
                deadline,
            });
            Ok(())
        }
        .await;
        if result.is_err() {
            *stream = None;
            state.read_policy.fail();
        }
        result
    }

    /// Wait for queued writes before changing the write position.
    pub async fn drain_writes(&self) -> FsResult<()> {
        let mut state = self.state.lock().await;
        let result = match state.stream.as_mut() {
            Some(exchange) => exchange.drain_writes().await,
            None => Err(FsError::common("block stream is not open")),
        };
        if result.is_err() {
            state.stream = None;
            state.read_policy.fail();
        }
        result
    }
}

fn request_frame(msg: &mut Message, token: Option<&str>) -> FsResult<BlockDataRequest> {
    let op = match (RpcCode::from(msg.code()), msg.request_status()) {
        (RpcCode::ReadBlock, RequestStatus::Open) => Operation::OpenRead(msg.parse_header()?),
        (RpcCode::ReadBlock, RequestStatus::Running) => Operation::ReadChunk(ReadBlockChunk {
            chunks: None,
            header: if msg.header_len() > 0 {
                Some(msg.parse_header()?)
            } else {
                None
            },
        }),
        (RpcCode::ReadBlock, RequestStatus::Complete) => {
            Operation::CompleteRead(msg.parse_header()?)
        }
        (RpcCode::WriteBlock, RequestStatus::Open) => Operation::OpenWrite(msg.parse_header()?),
        (RpcCode::WriteBlock, RequestStatus::Running) => {
            if matches!(msg.data, DataSlice::IOSlice(_)) {
                return Err(FsError::common(
                    "gRPC cannot encode a file-backed data slice",
                ));
            }
            Operation::WriteChunk(WriteBlockChunk {
                header: if msg.header_len() > 0 {
                    Some(msg.parse_header()?)
                } else {
                    None
                },
                data: if msg.data.is_empty() {
                    None
                } else {
                    Some(std::mem::replace(&mut msg.data, DataSlice::Empty).to_bytes())
                },
            })
        }
        (RpcCode::WriteBlock, RequestStatus::Complete) => {
            Operation::CompleteWrite(msg.parse_header()?)
        }
        (RpcCode::WriteBlock, RequestStatus::Cancel) => Operation::CancelWrite(msg.parse_header()?),
        (RpcCode::WriteBlocksBatch, RequestStatus::Open) => {
            Operation::OpenBatch(msg.parse_header()?)
        }
        (RpcCode::WriteBlocksBatch, RequestStatus::Running) => {
            Operation::WriteBatch(msg.parse_header()?)
        }
        (RpcCode::WriteBlocksBatch, RequestStatus::Complete) => {
            Operation::CompleteBatch(msg.parse_header()?)
        }
        (RpcCode::WriteBlocksBatch, RequestStatus::Cancel) => {
            Operation::CancelBatch(msg.parse_header()?)
        }
        _ => return Err(FsError::common("unsupported block stream operation")),
    };
    Ok(BlockDataRequest {
        req_id: msg.req_id(),
        seq_id: msg.seq_id(),
        replication_token: token.map(str::to_owned),
        operation: Some(op),
    })
}

fn response_message(request: &Message, response: BlockDataResponse) -> FsResult<Message> {
    if response.req_id != request.req_id() || response.seq_id != request.seq_id() {
        return Err(FsError::common("block acknowledgement identity mismatch"));
    }
    let result = response
        .result
        .ok_or_else(|| FsError::common("missing block acknowledgement"))?;
    let expected = match (RpcCode::from(request.code()), request.request_status()) {
        (RpcCode::ReadBlock, RequestStatus::Open) => matches!(
            result,
            OperationResult::OpenedRead(_) | OperationResult::Error(_)
        ),
        (RpcCode::WriteBlock, RequestStatus::Open) => matches!(
            result,
            OperationResult::OpenedWrite(_) | OperationResult::Error(_)
        ),
        (RpcCode::WriteBlocksBatch, RequestStatus::Open) => matches!(
            result,
            OperationResult::OpenedBatch(_) | OperationResult::Error(_)
        ),
        (RpcCode::ReadBlock, RequestStatus::Running) => {
            matches!(result, OperationResult::Data(_) | OperationResult::Error(_))
        }
        (RpcCode::WriteBlocksBatch, RequestStatus::Complete | RequestStatus::Cancel) => {
            matches!(
                result,
                OperationResult::CommittedBatch(_) | OperationResult::Error(_)
            )
        }
        (RpcCode::WriteBlocksBatch, RequestStatus::Running) => {
            matches!(
                result,
                OperationResult::WrittenBatch(_) | OperationResult::Error(_)
            )
        }
        _ => matches!(result, OperationResult::Ack(_) | OperationResult::Error(_)),
    };
    if !expected {
        return Err(FsError::common("unexpected block acknowledgement type"));
    }
    match result {
        OperationResult::OpenedRead(header) => {
            Ok(Builder::success(request).proto_header(header).build())
        }
        OperationResult::OpenedWrite(header) => {
            Ok(Builder::success(request).proto_header(header).build())
        }
        OperationResult::OpenedBatch(header) => {
            Ok(Builder::success(request).proto_header(header).build())
        }
        OperationResult::CommittedBatch(header) => {
            Ok(Builder::success(request).proto_header(header).build())
        }
        OperationResult::WrittenBatch(header) => {
            Ok(Builder::success(request).proto_header(header).build())
        }
        OperationResult::Data(data) => Ok(request.success_with_data(None, DataSlice::Bytes(data))),
        OperationResult::Ack(true) => Ok(request.success()),
        OperationResult::Ack(false) => Err(FsError::common("block operation was not acknowledged")),
        OperationResult::Error(error) => {
            let error = curvine_error::grpc::from_detail(&error)
                .unwrap_or_else(|| FsError::common(error.message));
            Ok(request.error_ext(&error))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_proto::block_data_service_server::{BlockDataService, BlockDataServiceServer};
    use curvine_proto::{BlockReadRequest, DataHeaderProto};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::sync::{oneshot, Notify};
    use tonic::Status;

    struct DelayedWrites {
        window: usize,
        reached: std::sync::Mutex<Option<oneshot::Sender<usize>>>,
        release: Arc<Notify>,
        fail_first: bool,
        committed: Arc<AtomicBool>,
    }

    #[tonic::async_trait]
    impl BlockDataService for DelayedWrites {
        type ExchangeStream =
            Pin<Box<dyn futures::Stream<Item = Result<BlockDataResponse, tonic::Status>> + Send>>;

        async fn exchange(
            &self,
            request: tonic::Request<tonic::Streaming<BlockDataRequest>>,
        ) -> Result<tonic::Response<Self::ExchangeStream>, tonic::Status> {
            let mut input = request.into_inner();
            let (tx, rx) = mpsc::channel(self.window + 2);
            let reached = self.reached.lock().unwrap().take();
            let release = self.release.clone();
            let window = self.window;
            let fail_first = self.fail_first;
            let committed = self.committed.clone();
            tokio::spawn(async move {
                let mut reached = reached;
                let mut pending = Vec::new();
                let mut released = false;
                while let Ok(Some(request)) = input.message().await {
                    let result = match request.operation {
                        Some(Operation::OpenWrite(_)) => {
                            OperationResult::OpenedWrite(Default::default())
                        }
                        Some(Operation::WriteChunk(_)) if !released => {
                            pending.push((request.req_id, request.seq_id));
                            if pending.len() < window {
                                continue;
                            }
                            if let Some(reached) = reached.take() {
                                let _ = reached.send(pending.len());
                            }
                            release.notified().await;
                            for (index, (req_id, seq_id)) in pending.drain(..).enumerate() {
                                let result = if fail_first && index == 0 {
                                    OperationResult::Error(curvine_error::grpc::to_detail(
                                        &FsError::disk_out_of_space("injected failure"),
                                    ))
                                } else {
                                    OperationResult::Ack(true)
                                };
                                if tx
                                    .send(Ok(BlockDataResponse {
                                        req_id,
                                        seq_id,
                                        result: Some(result),
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            released = true;
                            continue;
                        }
                        Some(Operation::CompleteWrite(_)) => {
                            committed.store(true, Ordering::SeqCst);
                            OperationResult::Ack(true)
                        }
                        Some(Operation::WriteChunk(_) | Operation::CancelWrite(_)) => {
                            OperationResult::Ack(true)
                        }
                        _ => return,
                    };
                    if tx
                        .send(Ok(BlockDataResponse {
                            req_id: request.req_id,
                            seq_id: request.seq_id,
                            result: Some(result),
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            });
            Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
        }
    }

    async fn delayed_client(
        window: usize,
        fail_first: bool,
    ) -> (
        GrpcBlockClient,
        oneshot::Receiver<usize>,
        Arc<Notify>,
        Arc<AtomicBool>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (reached_tx, reached_rx) = oneshot::channel();
        let release = Arc::new(Notify::new());
        let committed = Arc::new(AtomicBool::new(false));
        let service = DelayedWrites {
            window,
            reached: std::sync::Mutex::new(Some(reached_tx)),
            release: release.clone(),
            fail_first,
            committed: committed.clone(),
        };
        let server = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(BlockDataServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await;
        });
        let client = GrpcBlockClient::connect(format!("http://{addr}"), Duration::from_secs(5))
            .await
            .unwrap();
        let open = Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Open)
            .req_id(11)
            .seq_id(0)
            .proto_header(curvine_proto::BlockWriteRequest::default())
            .build();
        client.rpc(open, None).await.unwrap();
        (client, reached_rx, release, committed, server)
    }

    fn data_message(seq: i32) -> Message {
        Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Running)
            .req_id(11)
            .seq_id(seq)
            .data(DataSlice::Bytes(bytes::Bytes::from_static(b"chunk")))
            .build()
    }

    #[tokio::test]
    async fn write_windows_bound_in_flight_chunks_and_drain_before_commit() {
        for window in [8, 16, 32] {
            let (client, reached, release, committed, server) = delayed_client(window, false).await;
            for seq in 1..=window as i32 {
                client
                    .write_pipelined(data_message(seq), None, window)
                    .await
                    .unwrap();
            }
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), reached)
                    .await
                    .unwrap()
                    .unwrap(),
                window
            );
            assert!(tokio::time::timeout(
                Duration::from_millis(50),
                client.write_pipelined(data_message(window as i32 + 1), None, window)
            )
            .await
            .is_err());
            release.notify_one();
            client
                .write_pipelined(data_message(window as i32 + 1), None, window)
                .await
                .unwrap();
            let commit = Builder::new()
                .code(RpcCode::WriteBlock)
                .request(RequestStatus::Complete)
                .req_id(11)
                .seq_id(window as i32 + 2)
                .proto_header(curvine_proto::BlockWriteRequest::default())
                .build();
            client.rpc(commit, None).await.unwrap();
            assert!(committed.load(Ordering::SeqCst));
            server.abort();
        }
    }

    #[tokio::test]
    async fn queued_write_error_is_reported_before_commit() {
        let (client, reached, release, committed, server) = delayed_client(8, true).await;
        for seq in 1..=8 {
            client
                .write_pipelined(data_message(seq), None, 8)
                .await
                .unwrap();
        }
        reached.await.unwrap();
        release.notify_one();
        let commit = Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Complete)
            .req_id(11)
            .seq_id(9)
            .proto_header(curvine_proto::BlockWriteRequest::default())
            .build();
        let error = client.rpc(commit, None).await.unwrap_err();
        assert!(matches!(error, FsError::DiskOutOfSpace(_)));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!committed.load(Ordering::SeqCst));
        server.abort();
    }

    #[tokio::test]
    async fn window_one_waits_for_the_chunk_acknowledgement() {
        let (client, reached, release, _, server) = delayed_client(1, false).await;
        let writer =
            tokio::spawn(async move { client.write_pipelined(data_message(1), None, 1).await });
        assert_eq!(reached.await.unwrap(), 1);
        assert!(!writer.is_finished());
        release.notify_one();
        writer.await.unwrap().unwrap();
        server.abort();
    }

    struct EchoRead;

    #[tonic::async_trait]
    impl BlockDataService for EchoRead {
        type ExchangeStream =
            Pin<Box<dyn futures::Stream<Item = Result<BlockDataResponse, tonic::Status>> + Send>>;

        async fn exchange(
            &self,
            request: tonic::Request<tonic::Streaming<BlockDataRequest>>,
        ) -> Result<tonic::Response<Self::ExchangeStream>, tonic::Status> {
            let mut input = request.into_inner();
            let output = async_stream::try_stream! {
                while let Some(request) = input.message().await? {
                    let result = match request.operation {
                        Some(Operation::OpenRead(_)) => OperationResult::OpenedRead(Default::default()),
                        Some(Operation::ReadChunk(_)) => OperationResult::Data(bytes::Bytes::from_static(b"abc")),
                        Some(Operation::CompleteRead(_)) => OperationResult::Ack(true),
                        _ => Err(tonic::Status::invalid_argument("unexpected request"))?,
                    };
                    yield BlockDataResponse { req_id: request.req_id, seq_id: request.seq_id, result: Some(result) };
                }
            };
            Ok(tonic::Response::new(Box::pin(output)))
        }
    }

    #[tokio::test]
    async fn reads_over_a_real_bidirectional_stream() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(BlockDataServiceServer::new(EchoRead))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let client = GrpcBlockClient::connect(format!("http://{}", addr), Duration::from_secs(5))
            .await
            .unwrap();
        let msg = |seq_id, status| {
            Builder::new()
                .code(RpcCode::ReadBlock)
                .request(status)
                .req_id(9)
                .seq_id(seq_id)
        };
        client
            .rpc(
                msg(0, RequestStatus::Open)
                    .proto_header(BlockReadRequest::default())
                    .build(),
                None,
            )
            .await
            .unwrap();
        let data = client
            .rpc(msg(1, RequestStatus::Running).build(), None)
            .await
            .unwrap();
        assert_eq!(data.data.as_slice(), b"abc");
        client
            .rpc(
                msg(2, RequestStatus::Complete)
                    .proto_header(BlockReadRequest::default())
                    .build(),
                None,
            )
            .await
            .unwrap();
        task.abort();
    }

    #[test]
    fn read_seek_preserves_header_and_identity() {
        let header = DataHeaderProto {
            offset: 17,
            flush: false,
            is_last: false,
        };
        let mut message = Builder::new()
            .code(RpcCode::ReadBlock)
            .request(RequestStatus::Running)
            .req_id(31)
            .seq_id(4)
            .proto_header(header.clone())
            .build();
        let frame = request_frame(&mut message, None).unwrap();
        assert_eq!((frame.req_id, frame.seq_id), (31, 4));
        match frame.operation.unwrap() {
            Operation::ReadChunk(chunk) => assert_eq!(chunk.header, Some(header)),
            _ => panic!("unexpected operation"),
        }
    }

    type ReadCalls = Arc<std::sync::Mutex<Vec<(i32, u32, Option<i64>)>>>;
    struct BurstRead(ReadCalls);

    #[tonic::async_trait]
    impl BlockDataService for BurstRead {
        type ExchangeStream =
            Pin<Box<dyn futures::Stream<Item = Result<BlockDataResponse, Status>> + Send>>;
        async fn exchange(
            &self,
            request: tonic::Request<tonic::Streaming<BlockDataRequest>>,
        ) -> Result<tonic::Response<Self::ExchangeStream>, Status> {
            let mut input = request.into_inner();
            let calls = self.0.clone();
            let output = async_stream::try_stream! {
                let mut offset = 0;
                let mut next_seq = 0;
                while let Some(request) = input.message().await? {
                    if request.seq_id != next_seq { Err(Status::invalid_argument("sequence mismatch"))?; }
                    match request.operation {
                        Some(Operation::ReadChunk(chunk)) => {
                            let count = chunk.chunks.unwrap_or(1);
                            calls.lock().unwrap().push((request.seq_id, count, chunk.header.as_ref().map(|h| h.offset)));
                            if let Some(header) = chunk.header { offset = header.offset; }
                            for index in 0..count {
                                yield BlockDataResponse { req_id: request.req_id, seq_id: request.seq_id + index as i32,
                                    result: Some(OperationResult::Data(bytes::Bytes::from(vec![offset as u8; 3]))) };
                                offset += 3;
                                next_seq += 1;
                            }
                        }
                        Some(Operation::OpenRead(_)) => {
                            next_seq += 1;
                            yield BlockDataResponse { req_id: request.req_id, seq_id: request.seq_id,
                                result: Some(OperationResult::OpenedRead(Default::default())) };
                        }
                        Some(Operation::CompleteRead(_)) => {
                            next_seq += 1;
                            yield BlockDataResponse { req_id: request.req_id, seq_id: request.seq_id, result: Some(OperationResult::Ack(true)) };
                        }
                        _ => Err(Status::invalid_argument("unexpected operation"))?,
                    }
                }
            };
            Ok(tonic::Response::new(Box::pin(output)))
        }
    }

    #[tokio::test]
    async fn read_bursts_preserve_seek_completion_and_wire_identity() {
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(BlockDataServiceServer::new(BurstRead(calls.clone())))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let client = GrpcBlockClient::connect(format!("http://{addr}"), Duration::from_secs(5))
            .await
            .unwrap()
            .with_read_window(4);
        let msg = |seq, status| {
            Builder::new()
                .code(RpcCode::ReadBlock)
                .req_id(7)
                .seq_id(seq)
                .request(status)
        };
        client
            .rpc(
                msg(0, RequestStatus::Open)
                    .proto_header(BlockReadRequest {
                        len: 12,
                        chunk_size: 3,
                        ..Default::default()
                    })
                    .build(),
                None,
            )
            .await
            .unwrap();
        let first = client
            .rpc(msg(1, RequestStatus::Running).build(), None)
            .await
            .unwrap();
        assert_eq!(first.data.as_slice(), &[0; 3]);
        let seeked = client
            .rpc(
                msg(2, RequestStatus::Running)
                    .proto_header(DataHeaderProto {
                        offset: 6,
                        ..Default::default()
                    })
                    .build(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(seeked.data.as_slice(), &[6; 3]);
        let next = client
            .rpc(msg(3, RequestStatus::Running).build(), None)
            .await
            .unwrap();
        assert_eq!(next.data.as_slice(), &[9; 3]);
        client
            .rpc(
                msg(4, RequestStatus::Complete)
                    .proto_header(BlockReadRequest::default())
                    .build(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(*calls.lock().unwrap(), vec![(1, 4, None), (5, 2, Some(6))]);
        // Reopen then complete while replies are still prefetched.
        client
            .rpc(
                msg(0, RequestStatus::Open)
                    .proto_header(BlockReadRequest {
                        len: 12,
                        chunk_size: 3,
                        ..Default::default()
                    })
                    .build(),
                None,
            )
            .await
            .unwrap();
        client
            .rpc(msg(1, RequestStatus::Running).build(), None)
            .await
            .unwrap();
        client
            .rpc(
                msg(2, RequestStatus::Complete)
                    .proto_header(BlockReadRequest::default())
                    .build(),
                None,
            )
            .await
            .unwrap();
        task.abort();
    }

    #[tokio::test]
    async fn read_window_replenishes_before_previous_burst_is_empty() {
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(BlockDataServiceServer::new(BurstRead(calls.clone())))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let client = GrpcBlockClient::connect(format!("http://{addr}"), Duration::from_secs(5))
            .await
            .unwrap()
            .with_read_window(4);
        let msg = |seq, status| {
            Builder::new()
                .code(RpcCode::ReadBlock)
                .req_id(7)
                .seq_id(seq)
                .request(status)
        };
        client
            .rpc(
                msg(0, RequestStatus::Open)
                    .proto_header(BlockReadRequest {
                        len: 30,
                        chunk_size: 3,
                        ..Default::default()
                    })
                    .build(),
                None,
            )
            .await
            .unwrap();
        for seq in 1..=10 {
            let response = client
                .rpc(msg(seq, RequestStatus::Running).build(), None)
                .await
                .unwrap();
            assert_eq!(response.data.as_slice(), &[((seq - 1) * 3) as u8; 3]);
        }
        client
            .rpc(
                msg(11, RequestStatus::Complete)
                    .proto_header(BlockReadRequest::default())
                    .build(),
                None,
            )
            .await
            .unwrap();
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 7);
        assert_eq!(calls[0], (1, 4, None));
        assert_eq!(calls[1], (5, 1, None));
        assert!(calls[1..].iter().all(|(_, count, _)| *count == 1));
        task.abort();
    }

    // Seed an accepted window without relying on wall-clock/network timing in
    // the protocol tests. Leave window 4 unavailable during the trial.
    async fn seed_stable_window_two(client: &GrpcBlockClient) {
        let mut state = client.state.lock().await;
        let ReadPolicy::Adaptive(controller) = &mut state.read_policy else {
            panic!("expected adaptive read policy");
        };
        let origin = Instant::now();
        for (phase, interval) in [(0, 2000), (1, 1500)] {
            for i in 0..32u64 {
                let sent =
                    origin + Duration::from_secs(phase) + Duration::from_micros(interval * i);
                controller.observe(
                    controller.generation(),
                    sent,
                    sent + Duration::from_millis(1),
                    1024 * 1024,
                    2,
                );
            }
        }
        assert_eq!(controller.window(3), 2);
    }

    async fn seed_stable_window_four(client: &GrpcBlockClient) {
        let mut state = client.state.lock().await;
        let ReadPolicy::Adaptive(controller) = &mut state.read_policy else {
            panic!("expected adaptive read policy");
        };
        let origin = Instant::now();
        for (phase, interval) in [(0, 2000), (1, 1500), (2, 1100)] {
            for i in 0..32u64 {
                let sent =
                    origin + Duration::from_secs(phase) + Duration::from_micros(interval * i);
                controller.observe(
                    controller.generation(),
                    sent,
                    sent + Duration::from_millis(1),
                    1024 * 1024,
                    4,
                );
            }
        }
        assert_eq!(controller.window(3), 4);
    }

    fn read_request(seq: i32, status: RequestStatus) -> Builder {
        Builder::new()
            .code(RpcCode::ReadBlock)
            .req_id(7)
            .seq_id(seq)
            .request(status)
    }

    async fn open_test_read(client: &GrpcBlockClient, len: i64) {
        client
            .rpc(
                read_request(0, RequestStatus::Open)
                    .proto_header(BlockReadRequest {
                        len,
                        chunk_size: 3,
                        ..Default::default()
                    })
                    .build(),
                None,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn adaptive_seek_drains_pending_and_resets_window() {
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(BlockDataServiceServer::new(BurstRead(calls.clone())))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let client = GrpcBlockClient::connect(format!("http://{addr}"), Duration::from_secs(5))
            .await
            .unwrap()
            .with_adaptive_read_window();
        seed_stable_window_two(&client).await;
        open_test_read(&client, 30).await;
        let first = client
            .rpc(read_request(1, RequestStatus::Running).build(), None)
            .await
            .unwrap();
        assert_eq!(first.data.as_slice(), &[0; 3]);
        let seeked = client
            .rpc(
                read_request(2, RequestStatus::Running)
                    .proto_header(DataHeaderProto {
                        offset: 12,
                        ..Default::default()
                    })
                    .build(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(seeked.data.as_slice(), &[12; 3]);
        assert_eq!(*calls.lock().unwrap(), vec![(1, 2, None), (3, 1, Some(12))]);
        task.abort();
    }

    #[tokio::test]
    async fn adaptive_eof_rewind_and_pool_reset_keep_learned_window() {
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(BlockDataServiceServer::new(BurstRead(calls.clone())))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let client = GrpcBlockClient::connect(format!("http://{addr}"), Duration::from_secs(5))
            .await
            .unwrap()
            .with_adaptive_read_window();
        seed_stable_window_two(&client).await;
        open_test_read(&client, 6).await;
        for seq in 1..=2 {
            client
                .rpc(read_request(seq, RequestStatus::Running).build(), None)
                .await
                .unwrap();
        }
        client
            .rpc(
                read_request(3, RequestStatus::Running)
                    .proto_header(DataHeaderProto {
                        offset: 0,
                        ..Default::default()
                    })
                    .build(),
                None,
            )
            .await
            .unwrap();
        client.reset();
        open_test_read(&client, 6).await;
        client
            .rpc(read_request(1, RequestStatus::Running).build(), None)
            .await
            .unwrap();
        assert_eq!(
            *calls.lock().unwrap(),
            vec![(1, 2, None), (3, 2, Some(0)), (1, 2, None)]
        );
        task.abort();
    }

    #[tokio::test]
    async fn adaptive_prefetch_bounds_total_outstanding_for_different_chunk_sizes() {
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(BlockDataServiceServer::new(BurstRead(calls.clone())))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let client = GrpcBlockClient::connect(format!("http://{addr}"), Duration::from_secs(5))
            .await
            .unwrap()
            .with_adaptive_read_window();
        seed_stable_window_four(&client).await;
        for (chunk_size, max_pending) in
            [(1024 * 1024, 4), (2 * 1024 * 1024, 2), (5 * 1024 * 1024, 1)]
        {
            calls.lock().unwrap().clear();
            client
                .rpc(
                    read_request(0, RequestStatus::Open)
                        .proto_header(BlockReadRequest {
                            len: i64::from(chunk_size) * 8,
                            chunk_size,
                            ..Default::default()
                        })
                        .build(),
                    None,
                )
                .await
                .unwrap();
            for seq in 1..=3 {
                client
                    .rpc(read_request(seq, RequestStatus::Running).build(), None)
                    .await
                    .unwrap();
                let state = client.state.lock().await;
                let pending = &state
                    .stream
                    .as_ref()
                    .unwrap()
                    .read
                    .as_ref()
                    .unwrap()
                    .pending;
                // The response just returned occupied one slot while in flight.
                assert!(pending.len() < max_pending);
            }
            let calls = calls.lock().unwrap();
            // Requests from the just-reset stream may still reach the mock
            // server after the call log is cleared. The first request of the
            // newly opened stream is always wire sequence 1.
            let first = calls.iter().find(|(seq, _, _)| *seq == 1).unwrap();
            assert_eq!(first.1 as usize, max_pending);
            assert!(calls
                .iter()
                .filter(|(seq, _, _)| *seq != 1)
                .all(|(_, count, _)| *count == 1));
            drop(calls);
            client.reset();
        }
        task.abort();
    }

    struct BadPrefetch;

    #[tonic::async_trait]
    impl BlockDataService for BadPrefetch {
        type ExchangeStream =
            Pin<Box<dyn futures::Stream<Item = Result<BlockDataResponse, Status>> + Send>>;

        async fn exchange(
            &self,
            request: tonic::Request<tonic::Streaming<BlockDataRequest>>,
        ) -> Result<tonic::Response<Self::ExchangeStream>, Status> {
            let mut input = request.into_inner();
            let output = async_stream::try_stream! {
                while let Some(request) = input.message().await? {
                    match request.operation {
                        Some(Operation::OpenRead(_)) => yield BlockDataResponse {
                            req_id: request.req_id, seq_id: request.seq_id,
                            result: Some(OperationResult::OpenedRead(Default::default())),
                        },
                        Some(Operation::ReadChunk(_)) => {
                            yield BlockDataResponse { req_id: request.req_id, seq_id: request.seq_id,
                                result: Some(OperationResult::Data(bytes::Bytes::from_static(b"abc"))) };
                            yield BlockDataResponse { req_id: request.req_id, seq_id: request.seq_id + 1,
                                result: Some(OperationResult::Error(curvine_error::grpc::to_detail(
                                    &FsError::common("prefetch failed")))) };
                        },
                        Some(Operation::CompleteRead(_)) => yield BlockDataResponse {
                            req_id: request.req_id, seq_id: request.seq_id,
                            result: Some(OperationResult::Ack(true)),
                        },
                        _ => Err(Status::invalid_argument("unexpected operation"))?,
                    }
                }
            };
            Ok(tonic::Response::new(Box::pin(output)))
        }
    }

    #[tokio::test]
    async fn fixed_window_discards_speculative_errors_at_seek_and_completion() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(BlockDataServiceServer::new(BadPrefetch))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let client = GrpcBlockClient::connect(format!("http://{addr}"), Duration::from_secs(5))
            .await
            .unwrap()
            .with_read_window(2);
        open_test_read(&client, 30).await;
        client
            .rpc(read_request(1, RequestStatus::Running).build(), None)
            .await
            .unwrap();
        let seeked = client
            .rpc(
                read_request(2, RequestStatus::Running)
                    .proto_header(DataHeaderProto {
                        offset: 12,
                        ..Default::default()
                    })
                    .build(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(seeked.data.as_slice(), b"abc");
        let completed = client
            .rpc(
                read_request(3, RequestStatus::Complete)
                    .proto_header(BlockReadRequest::default())
                    .build(),
                None,
            )
            .await
            .unwrap();
        assert!(completed.is_success());
        task.abort();
    }

    #[tokio::test]
    async fn seek_rejects_error_in_drained_prefetch_and_disables_auto() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(BlockDataServiceServer::new(BadPrefetch))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let client = GrpcBlockClient::connect(format!("http://{addr}"), Duration::from_secs(5))
            .await
            .unwrap()
            .with_adaptive_read_window();
        seed_stable_window_two(&client).await;
        open_test_read(&client, 30).await;
        client
            .rpc(read_request(1, RequestStatus::Running).build(), None)
            .await
            .unwrap();
        let error = client
            .rpc(
                read_request(2, RequestStatus::Running)
                    .proto_header(DataHeaderProto {
                        offset: 12,
                        ..Default::default()
                    })
                    .build(),
                None,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("prefetch failed"));
        let state = client.state.lock().await;
        assert!(state.stream.is_none());
        assert_eq!(state.read_policy.window(3), 1);
        drop(state);
        // A failure permanently disables further growth on this client.
        let mut state = client.state.lock().await;
        let ReadPolicy::Adaptive(controller) = &mut state.read_policy else {
            panic!("expected auto")
        };
        let now = Instant::now();
        for i in 0..32u64 {
            let sent = now + Duration::from_millis(i);
            controller.observe(
                controller.generation(),
                sent,
                sent + Duration::from_micros(500),
                1024 * 1024,
                4,
            );
        }
        assert_eq!(controller.window(3), 1);
        task.abort();
    }

    struct BrokenRead {
        hang: bool,
    }

    #[tonic::async_trait]
    impl BlockDataService for BrokenRead {
        type ExchangeStream =
            Pin<Box<dyn futures::Stream<Item = Result<BlockDataResponse, Status>> + Send>>;

        async fn exchange(
            &self,
            request: tonic::Request<tonic::Streaming<BlockDataRequest>>,
        ) -> Result<tonic::Response<Self::ExchangeStream>, Status> {
            let mut input = request.into_inner();
            let hang = self.hang;
            let output = async_stream::try_stream! {
                while let Some(request) = input.message().await? {
                    match request.operation {
                        Some(Operation::OpenRead(_)) => yield BlockDataResponse {
                            req_id: request.req_id, seq_id: request.seq_id,
                            result: Some(OperationResult::OpenedRead(Default::default())),
                        },
                        Some(Operation::ReadChunk(_)) => {
                            if hang {
                                std::future::pending::<()>().await;
                            }
                            Err(Status::unavailable("read transport failed"))?;
                        },
                        _ => Err(Status::invalid_argument("unexpected operation"))?,
                    }
                }
            };
            Ok(tonic::Response::new(Box::pin(output)))
        }
    }

    #[tokio::test]
    async fn read_transport_errors_and_timeouts_disable_auto_for_the_connection() {
        for hang in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let task = tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(BlockDataServiceServer::new(BrokenRead { hang }))
                    .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
            );
            let mut client =
                GrpcBlockClient::connect(format!("http://{addr}"), Duration::from_secs(5))
                    .await
                    .unwrap()
                    .with_adaptive_read_window();
            client.timeout = Duration::from_millis(100);
            seed_stable_window_two(&client).await;
            open_test_read(&client, 30).await;
            assert!(client
                .rpc(read_request(1, RequestStatus::Running).build(), None)
                .await
                .is_err());
            let mut state = client.state.lock().await;
            assert!(state.stream.is_none());
            let ReadPolicy::Adaptive(controller) = &mut state.read_policy else {
                panic!("expected auto");
            };
            assert_eq!(controller.window(3), 1);
            let now = Instant::now();
            for i in 0..32u64 {
                let sent = now + Duration::from_millis(i);
                controller.observe(
                    controller.generation(),
                    sent,
                    sent + Duration::from_micros(500),
                    1024 * 1024,
                    4,
                );
            }
            assert_eq!(controller.window(3), 1);
            task.abort();
        }
    }

    #[test]
    fn payload_buffers_are_transferred_without_copying() {
        let payload = bytes::Bytes::from(vec![7u8; 4096]);
        let ptr = payload.as_ptr();
        let mut message = Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Running)
            .data(DataSlice::Bytes(payload))
            .build();
        let frame = request_frame(&mut message, None).unwrap();
        match frame.operation.unwrap() {
            Operation::WriteChunk(chunk) => assert_eq!(chunk.data.unwrap().as_ptr(), ptr),
            _ => panic!("unexpected operation"),
        }
        assert!(message.data.is_empty());

        let payload = bytes::Bytes::from(vec![3u8; 4096]);
        let ptr = payload.as_ptr();
        let request = Builder::new()
            .code(RpcCode::ReadBlock)
            .request(RequestStatus::Running)
            .build();
        let response = BlockDataResponse {
            req_id: request.req_id(),
            seq_id: request.seq_id(),
            result: Some(OperationResult::Data(payload)),
        };
        let message = response_message(&request, response).unwrap();
        assert_eq!(message.data.as_slice().as_ptr(), ptr);
        assert_eq!(message.data_bytes().unwrap().len(), 4096);
    }

    #[test]
    fn rejects_wrong_acknowledgement_type_and_identity() {
        let message = Builder::new()
            .code(RpcCode::ReadBlock)
            .request(RequestStatus::Open)
            .req_id(31)
            .seq_id(0)
            .proto_header(BlockReadRequest::default())
            .build();
        let wrong_kind = BlockDataResponse {
            req_id: 31,
            seq_id: 0,
            result: Some(OperationResult::Ack(true)),
        };
        assert!(response_message(&message, wrong_kind).is_err());
        let wrong_identity = BlockDataResponse {
            req_id: 32,
            seq_id: 0,
            result: Some(OperationResult::OpenedRead(Default::default())),
        };
        assert!(response_message(&message, wrong_identity).is_err());
    }

    #[test]
    fn batch_commit_keeps_individual_results() {
        let request = Builder::new()
            .code(RpcCode::WriteBlocksBatch)
            .request(RequestStatus::Complete)
            .req_id(31)
            .seq_id(2)
            .build();
        let response = BlockDataResponse {
            req_id: 31,
            seq_id: 2,
            result: Some(OperationResult::CommittedBatch(
                curvine_proto::BlocksBatchCommitResponse {
                    results: vec![true, false],
                },
            )),
        };
        let message = response_message(&request, response).unwrap();
        let decoded: curvine_proto::BlocksBatchCommitResponse = message.parse_header().unwrap();
        assert_eq!(decoded.results, [true, false]);
    }
}
