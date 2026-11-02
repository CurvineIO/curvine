// Copyright 2026 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

//! Typed, ordered block operations over a bounded bidirectional gRPC stream.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;

use curvine_error::FsError;
use curvine_fs_api::RpcCode;
use curvine_io::DataSlice;
use curvine_proto::block_data_request::Operation;
use curvine_proto::block_data_response::Result as OperationResult;
use curvine_proto::block_data_service_server::BlockDataService;
use curvine_proto::{BlockDataRequest, BlockDataResponse, ReplicationWriteRequest};
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::{Builder, Message, RequestStatus};
use futures::Stream;
use tokio::sync::{watch, Notify};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use super::WorkerService;

/// The service creates an independent stateful handler for each gRPC stream.
#[derive(Clone)]
pub struct GrpcBlockDataService {
    service: WorkerService,
    tracker: StreamTracker,
}

impl GrpcBlockDataService {
    pub fn new(service: WorkerService, tracker: StreamTracker) -> Self {
        Self { service, tracker }
    }
}

#[derive(Default)]
struct TrackerState {
    closing: bool,
    active: usize,
}

struct TrackerInner {
    state: Mutex<TrackerState>,
    stop: watch::Sender<bool>,
    drained: Notify,
}

/// Tracks stream tasks through their final blocking I/O and abort cleanup.
#[derive(Clone)]
pub struct StreamTracker(Arc<TrackerInner>);

impl Default for StreamTracker {
    fn default() -> Self {
        let (stop, _) = watch::channel(false);
        Self(Arc::new(TrackerInner {
            state: Mutex::new(TrackerState::default()),
            stop,
            drained: Notify::new(),
        }))
    }
}

pub(crate) struct StreamGuard(StreamTracker);

impl Drop for StreamGuard {
    fn drop(&mut self) {
        let mut state = self.0 .0.state.lock().expect("stream tracker poisoned");
        state.active -= 1;
        if state.active == 0 {
            self.0 .0.drained.notify_waiters();
        }
    }
}

impl StreamTracker {
    pub(crate) fn register(&self) -> Option<StreamGuard> {
        let mut state = self.0.state.lock().expect("stream tracker poisoned");
        if state.closing {
            return None;
        }
        state.active += 1;
        Some(StreamGuard(self.clone()))
    }

    pub fn shutdown(&self) {
        let mut state = self.0.state.lock().expect("stream tracker poisoned");
        state.closing = true;
        self.0.stop.send_replace(true);
    }

    pub async fn wait_drained(&self) {
        loop {
            let notified = self.0.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.0.state.lock().expect("stream tracker poisoned").active == 0 {
                return;
            }
            notified.await;
        }
    }
}

struct StreamState {
    req_id: i64,
    next_seq: i32,
    token: Option<String>,
    block_id: Option<i64>,
    mode: StreamMode,
    cleanup_write: Option<curvine_proto::BlockWriteRequest>,
    cleanup_batch: Option<curvine_proto::BlocksBatchWriteRequest>,
    opened: bool,
    read_chunk_size: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StreamMode {
    Read,
    Write,
    Batch,
}

impl StreamState {
    // Preserve tonic's Status-returning API.
    #[allow(clippy::result_large_err)]
    fn open(request: &BlockDataRequest) -> Result<Self, Status> {
        let (block_id, mode, cleanup_write) = match request.operation.as_ref() {
            Some(Operation::OpenRead(_)) | Some(Operation::OpenBatch(_)) => {
                if request.replication_token.is_some() {
                    return Err(Status::invalid_argument(
                        "replication token requires a block write",
                    ));
                }
                let mode = if matches!(request.operation.as_ref(), Some(Operation::OpenRead(_))) {
                    StreamMode::Read
                } else {
                    StreamMode::Batch
                };
                (None, mode, None)
            }
            Some(Operation::OpenWrite(open)) => {
                (Some(open.block.id), StreamMode::Write, Some(open.clone()))
            }
            _ => {
                return Err(Status::invalid_argument(
                    "first block operation must open a block",
                ))
            }
        };
        if request.replication_token.as_deref() == Some("") {
            return Err(Status::invalid_argument("empty replication token"));
        }
        Ok(Self {
            req_id: request.req_id,
            next_seq: request.seq_id,
            token: request.replication_token.clone(),
            block_id,
            mode,
            cleanup_write,
            cleanup_batch: match request.operation.as_ref() {
                Some(Operation::OpenBatch(open)) => Some(open.clone()),
                _ => None,
            },
            opened: false,
            read_chunk_size: match request.operation.as_ref() {
                Some(Operation::OpenRead(open)) => open.chunk_size.max(1) as usize,
                _ => 1,
            },
        })
    }

    // Preserve tonic's Status-returning API.
    #[allow(clippy::result_large_err)]
    fn validate(&mut self, request: &BlockDataRequest) -> Result<(), Status> {
        if request.req_id != self.req_id
            || request.seq_id != self.next_seq
            || request.replication_token != self.token
        {
            return Err(Status::invalid_argument(
                "block stream identity, sequence or token mismatch",
            ));
        }
        let valid = match request.operation.as_ref() {
            Some(Operation::OpenRead(_)) => !self.opened && self.mode == StreamMode::Read,
            Some(Operation::OpenWrite(_)) => !self.opened && self.mode == StreamMode::Write,
            Some(Operation::OpenBatch(_)) => !self.opened && self.mode == StreamMode::Batch,
            Some(Operation::ReadChunk(_) | Operation::CompleteRead(_)) => {
                self.opened && self.mode == StreamMode::Read
            }
            Some(Operation::WriteChunk(_)) => {
                self.opened && (self.mode == StreamMode::Write || self.mode == StreamMode::Batch)
            }
            Some(Operation::CompleteWrite(header) | Operation::CancelWrite(header)) => {
                self.opened
                    && self.mode == StreamMode::Write
                    && Some(header.block.id) == self.block_id
            }
            Some(
                Operation::WriteBatch(_) | Operation::CompleteBatch(_) | Operation::CancelBatch(_),
            ) => self.opened && self.mode == StreamMode::Batch,
            None => false,
        };
        if !valid {
            return Err(Status::invalid_argument(
                "operation does not match open block stream",
            ));
        }
        self.next_seq = self
            .next_seq
            .checked_add(1)
            .ok_or_else(|| Status::out_of_range("block sequence exhausted"))?;
        Ok(())
    }
}

// Preserve tonic's Status-returning API.
#[allow(clippy::result_large_err)]
fn into_message(request: BlockDataRequest, state: &StreamState) -> Result<(Message, bool), Status> {
    let mut builder = Builder::new().req_id(request.req_id).seq_id(request.seq_id);
    let operation = request
        .operation
        .ok_or_else(|| Status::invalid_argument("missing block operation"))?;
    let (code, status, done) = match operation {
        Operation::OpenRead(header) => {
            builder = builder.proto_header(header);
            (RpcCode::ReadBlock, RequestStatus::Open, false)
        }
        Operation::OpenWrite(header) => {
            builder = builder.proto_header(header);
            (RpcCode::WriteBlock, RequestStatus::Open, false)
        }
        Operation::ReadChunk(chunk) => {
            if let Some(header) = chunk.header {
                builder = builder.proto_header(header);
            }
            (RpcCode::ReadBlock, RequestStatus::Running, false)
        }
        Operation::WriteChunk(chunk) => {
            if let Some(header) = chunk.header {
                builder = builder.proto_header(header);
            }
            if let Some(data) = chunk.data {
                builder = builder.data(DataSlice::Bytes(data));
            }
            (RpcCode::WriteBlock, RequestStatus::Running, false)
        }
        Operation::CompleteRead(header) => {
            builder = builder.proto_header(header);
            (RpcCode::ReadBlock, RequestStatus::Complete, true)
        }
        Operation::CompleteWrite(header) => {
            builder = builder.proto_header(header);
            (RpcCode::WriteBlock, RequestStatus::Complete, true)
        }
        Operation::CancelWrite(header) => {
            builder = builder.proto_header(header);
            (RpcCode::WriteBlock, RequestStatus::Cancel, true)
        }
        Operation::OpenBatch(header) => {
            builder = builder.proto_header(header);
            (RpcCode::WriteBlocksBatch, RequestStatus::Open, false)
        }
        Operation::WriteBatch(header) => {
            builder = builder.proto_header(header);
            (RpcCode::WriteBlocksBatch, RequestStatus::Running, false)
        }
        Operation::CompleteBatch(header) => {
            builder = builder.proto_header(header);
            (RpcCode::WriteBlocksBatch, RequestStatus::Complete, true)
        }
        Operation::CancelBatch(header) => {
            builder = builder.proto_header(header);
            (RpcCode::WriteBlocksBatch, RequestStatus::Cancel, true)
        }
    };
    let msg = builder.code(code).request(status).build();
    if let Some(token) = &state.token {
        if code != RpcCode::WriteBlock {
            return Err(Status::invalid_argument(
                "replication stream only accepts block writes",
            ));
        }
        let envelope = ReplicationWriteRequest {
            block_id: state
                .block_id
                .ok_or_else(|| Status::invalid_argument("missing replication block"))?,
            token: token.clone(),
            header: msg.header.as_ref().map(|h| h.to_vec()).unwrap_or_default(),
        };
        let mut protocol = msg.protocol;
        protocol.code = RpcCode::WriteReplicationBlock.into();
        return Ok((
            Builder::protocol(protocol)
                .proto_header(envelope)
                .data(msg.data)
                .build(),
            done,
        ));
    }
    Ok((msg, done))
}

// Preserve tonic's Status-returning API.
#[allow(clippy::result_large_err)]
fn into_response(request: &Message, response: Message) -> Result<BlockDataResponse, Status> {
    let result = if !response.is_success() {
        let error = response
            .check_error_ext::<FsError>()
            .expect_err("error response");
        OperationResult::Error(curvine_error::grpc::to_detail(&error))
    } else {
        match (RpcCode::from(request.code()), request.request_status()) {
            (RpcCode::ReadBlock, RequestStatus::Open) => OperationResult::OpenedRead(
                response
                    .parse_header()
                    .map_err(|e| Status::internal(e.to_string()))?,
            ),
            (RpcCode::WriteBlock | RpcCode::WriteReplicationBlock, RequestStatus::Open) => {
                OperationResult::OpenedWrite(
                    response
                        .parse_header()
                        .map_err(|e| Status::internal(e.to_string()))?,
                )
            }
            (RpcCode::WriteBlocksBatch, RequestStatus::Open) => OperationResult::OpenedBatch(
                response
                    .parse_header()
                    .map_err(|e| Status::internal(e.to_string()))?,
            ),
            (RpcCode::WriteBlocksBatch, RequestStatus::Complete | RequestStatus::Cancel) => {
                OperationResult::CommittedBatch(
                    response
                        .parse_header()
                        .map_err(|e| Status::internal(e.to_string()))?,
                )
            }
            (RpcCode::WriteBlocksBatch, RequestStatus::Running) => OperationResult::WrittenBatch(
                response
                    .parse_header()
                    .map_err(|e| Status::internal(e.to_string()))?,
            ),
            (RpcCode::ReadBlock, RequestStatus::Running) => {
                OperationResult::Data(response.data.to_bytes())
            }
            _ => OperationResult::Ack(true),
        }
    };
    Ok(BlockDataResponse {
        req_id: request.req_id(),
        seq_id: request.seq_id(),
        result: Some(result),
    })
}

#[tonic::async_trait]
impl BlockDataService for GrpcBlockDataService {
    type ExchangeStream = Pin<Box<dyn Stream<Item = Result<BlockDataResponse, Status>> + Send>>;

    async fn exchange(
        &self,
        request: Request<tonic::Streaming<BlockDataRequest>>,
    ) -> Result<Response<Self::ExchangeStream>, Status> {
        let guard = self
            .tracker
            .register()
            .ok_or_else(|| Status::unavailable("worker is shutting down"))?;
        let mut stop = self.tracker.0.stop.subscribe();
        let peer = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_default();
        let mut inbound = request.into_inner();
        let mut handler = self.service.get_message_handler(None);
        handler.buffered_reads = true;
        handler.client_addr = peer;
        let handler = Arc::new(handler);
        // At most one response waits for transport backpressure; processing is sequential.
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let _guard = guard;
            let mut state: Option<StreamState> = None;
            'stream: loop {
                if *stop.borrow() {
                    break;
                }
                let frame = match tokio::select! { _ = tx.closed() => break, _ = stop.changed() => break, message = inbound.message() => message }
                {
                    Ok(Some(frame)) => frame,
                    Ok(None) => break,
                    Err(error) => {
                        tokio::select! { _ = stop.changed() => {}, _ = tx.send(Err(error)) => {} }
                        break;
                    }
                };
                let chunks = match frame.operation.as_ref() {
                    Some(Operation::ReadChunk(chunk)) => chunk.chunks.unwrap_or(1),
                    _ => 1,
                };
                if !(1..=32).contains(&chunks) {
                    let _ = tx
                        .send(Err(Status::invalid_argument(
                            "read burst must be in 1..=32",
                        )))
                        .await;
                    break;
                }
                let first_seq = frame.seq_id;
                // Amortize a blocking-pool handoff across a short sequential burst.
                // No blocking thread is retained while the stream is idle.
                let chunk_size = state.as_ref().map_or(1, |s| s.read_chunk_size);
                let batch_size = (8 * 1024 * 1024 / chunk_size).clamp(1, 4);
                for batch_start in (0..chunks).step_by(batch_size) {
                    let result = async {
                        let mut messages = Vec::with_capacity(batch_size);
                        for index in batch_start..chunks.min(batch_start + batch_size as u32) {
                            let mut frame = frame.clone();
                            frame.seq_id = first_seq
                                .checked_add(index as i32)
                                .ok_or_else(|| Status::out_of_range("read sequence exhausted"))?;
                            if index != 0 {
                                if let Some(Operation::ReadChunk(chunk)) = frame.operation.as_mut()
                                {
                                    chunk.header = None;
                                }
                            }
                            if state.is_none() {
                                state = Some(StreamState::open(&frame)?);
                            }
                            let stream_state = state.as_mut().expect("initialized");
                            stream_state.validate(&frame)?;
                            messages.push(into_message(frame, stream_state)?);
                        }
                        let worker = handler.clone();
                        let responses = tokio::task::spawn_blocking(move || {
                            messages
                                .into_iter()
                                .map(|(msg, done)| {
                                    let response = worker
                                        .handle(&msg)
                                        .unwrap_or_else(|error| msg.error_ext(&error));
                                    (msg, response, done)
                                })
                                .collect::<Vec<_>>()
                        })
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?;
                        let mut replies = Vec::with_capacity(responses.len());
                        for (msg, response, done) in responses {
                            let success = response.is_success();
                            if success && msg.request_status() == RequestStatus::Open {
                                state.as_mut().expect("initialized").opened = true;
                            }
                            if success && done {
                                state.as_mut().expect("initialized").cleanup_write = None;
                                state.as_mut().expect("initialized").cleanup_batch = None;
                            }
                            let terminate = (done && success)
                                || (!success && msg.request_status() == RequestStatus::Open);
                            replies.push((into_response(&msg, response)?, terminate));
                        }
                        Ok::<_, Status>(replies)
                    }
                    .await;
                    match result {
                        Ok(replies) => {
                            for (reply, done) in replies {
                                if tokio::select! { _ = stop.changed() => true, sent = tx.send(Ok(reply)) => sent.is_err() }
                                    || done
                                {
                                    break 'stream;
                                }
                            }
                        }
                        Err(error) => {
                            tokio::select! { _ = stop.changed() => {}, _ = tx.send(Err(error)) => {} }
                            break 'stream;
                        }
                    }
                }
            }
            // A replication attempt is owned by ReplicationTarget and must only
            // be revoked under its stripe lock. Ordinary writes are connection
            // scoped, so abort their staging block when the stream is lost.
            if let Some(state) = state {
                if state.opened && state.token.is_none() {
                    if let Some(header) = state.cleanup_write {
                        let cancel = Builder::new()
                            .code(RpcCode::WriteBlock)
                            .request(RequestStatus::Cancel)
                            .req_id(state.req_id)
                            .seq_id(state.next_seq)
                            .proto_header(header)
                            .build();
                        let worker = handler.clone();
                        let _ = tokio::task::spawn_blocking(move || worker.handle(&cancel)).await;
                    } else if let Some(open) = state.cleanup_batch {
                        let header = curvine_proto::BlocksBatchCommitRequest {
                            blocks: open.blocks,
                            off: open.off,
                            block_size: open.block_size,
                            req_id: state.req_id,
                            seq_id: state.next_seq,
                            cancel: true,
                            component_info: open.component_info,
                        };
                        let cancel = Builder::new()
                            .code(RpcCode::WriteBlocksBatch)
                            .request(RequestStatus::Cancel)
                            .req_id(state.req_id)
                            .seq_id(state.next_seq)
                            .proto_header(header)
                            .build();
                        let worker = handler.clone();
                        let _ = tokio::task::spawn_blocking(move || worker.handle(&cancel)).await;
                    }
                }
            }
            // Release file handles before reporting this task drained.
            let _ = tokio::task::spawn_blocking(move || drop(handler)).await;
            drop(_guard);
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_proto::{BlockReadRequest, BlockWriteRequest, ReadBlockChunk, WriteBlockChunk};

    fn frame(seq_id: i32, operation: Operation, token: Option<&str>) -> BlockDataRequest {
        BlockDataRequest {
            req_id: 42,
            seq_id,
            replication_token: token.map(str::to_string),
            operation: Some(operation),
        }
    }

    #[test]
    fn ordered_stream_rejects_wrong_id_sequence_and_operation() {
        let open = frame(0, Operation::OpenRead(BlockReadRequest::default()), None);
        let mut state = StreamState::open(&open).unwrap();
        state.validate(&open).unwrap();
        state.opened = true;
        let next = frame(
            1,
            Operation::ReadChunk(ReadBlockChunk {
                header: None,
                chunks: None,
            }),
            None,
        );
        state.validate(&next).unwrap();
        assert!(state.validate(&next).is_err());
        let write = frame(
            2,
            Operation::WriteChunk(WriteBlockChunk {
                header: None,
                data: None,
            }),
            None,
        );
        assert!(state.validate(&write).is_err());
    }

    #[test]
    fn replication_token_is_checked_and_every_write_is_fenced() {
        let mut header = BlockWriteRequest::default();
        header.block.id = 99;
        let open = frame(0, Operation::OpenWrite(header), Some("token"));
        let mut state = StreamState::open(&open).unwrap();
        state.validate(&open).unwrap();
        let (message, _) = into_message(open, &state).unwrap();
        assert_eq!(
            RpcCode::from(message.code()),
            RpcCode::WriteReplicationBlock
        );
        let envelope: ReplicationWriteRequest = message.parse_header().unwrap();
        assert_eq!(envelope.block_id, 99);
        assert_eq!(envelope.token, "token");
        state.opened = true;
        let bad = frame(
            1,
            Operation::WriteChunk(WriteBlockChunk {
                header: None,
                data: None,
            }),
            Some("stale"),
        );
        assert!(state.validate(&bad).is_err());
    }

    #[tokio::test]
    async fn shutdown_rejects_new_streams_and_waits_for_cleanup() {
        let tracker = StreamTracker::default();
        let guard = tracker.register().unwrap();
        tracker.shutdown();
        assert!(tracker.register().is_none());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), tracker.wait_drained())
                .await
                .is_err()
        );
        drop(guard);
        tokio::time::timeout(std::time::Duration::from_secs(1), tracker.wait_drained())
            .await
            .unwrap();
    }
}
