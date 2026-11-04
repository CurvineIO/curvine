// Copyright 2026 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use bytes::{Buf, Bytes};
use curvine_proto::{
    block_data_request::Operation, block_data_response::Result as BlockResult, BlockDataRequest,
    BlockDataResponse, WriteBlockChunk,
};
use futures::stream;
use http_body::Frame;
use http_body_util::StreamBody;
use prost::Message;
use std::convert::Infallible;
use tonic::codec::{DecodeBuf, Decoder, Streaming};
use tonic::codegen::http::{HeaderMap, HeaderValue, StatusCode};
use tonic::Status;
use tonic_prost::ProstDecoder;

// Observe the ordinary generated-service prost decoder. Its Bytes field must
// share the message buffer rather than allocate a second copy of the payload.
struct OwnershipCheckingDecoder<M> {
    inner: ProstDecoder<M>,
    payload: fn(&M) -> &Bytes,
}

impl<M: Message + Default> Decoder for OwnershipCheckingDecoder<M> {
    type Item = M;
    type Error = Status;

    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<M>, Status> {
        let start = buf.chunk().as_ptr() as usize;
        let end = start + buf.chunk().len();
        let decoded = self.inner.decode(buf)?;
        if let Some(message) = &decoded {
            let data = (self.payload)(message);
            let data_start = data.as_ptr() as usize;
            assert!(!data.is_empty());
            assert!(data_start >= start && data_start + data.len() <= end);
        }
        Ok(decoded)
    }
}

fn grpc_frame<M: Message>(message: &M) -> Vec<u8> {
    let message = message.encode_to_vec();
    let mut frame = Vec::with_capacity(5 + message.len());
    frame.push(0);
    frame.extend_from_slice(&(message.len() as u32).to_be_bytes());
    frame.extend_from_slice(&message);
    frame
}

// Split the first gRPC header and payload across HTTP body frames, then place
// the end of the first message and all of the second in the same frame.
fn fragmented_body<M: Message>(
    first: &M,
    second: &M,
    response: bool,
) -> impl http_body::Body<Data = Bytes, Error = Infallible> + Send + 'static {
    let mut wire = grpc_frame(first);
    let first_end = wire.len();
    wire.extend_from_slice(&grpc_frame(second));
    let boundaries = [0, 1, 3, 8, first_end - 3, wire.len()];
    let mut frames: Vec<_> = boundaries
        .windows(2)
        .map(|range| {
            Ok(Frame::data(Bytes::copy_from_slice(
                &wire[range[0]..range[1]],
            )))
        })
        .collect();
    if response {
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", HeaderValue::from_static("0"));
        frames.push(Ok(Frame::trailers(trailers)));
    }
    StreamBody::new(stream::iter(frames))
}

fn response_payload(message: &BlockDataResponse) -> &Bytes {
    match message.result.as_ref().unwrap() {
        BlockResult::Data(data) => data,
        _ => panic!("expected block data response"),
    }
}

fn request_payload(message: &BlockDataRequest) -> &Bytes {
    match message.operation.as_ref().unwrap() {
        Operation::WriteChunk(chunk) => chunk.data.as_ref().unwrap(),
        _ => panic!("expected write chunk request"),
    }
}

#[tokio::test]
async fn fragmented_coalesced_responses_keep_owned_payloads() {
    let first_data = Bytes::from(vec![0x37; 40 * 1024]);
    let second_data = Bytes::from(vec![0x9a; 32 * 1024]);
    let first = BlockDataResponse {
        req_id: 1,
        seq_id: 2,
        result: Some(BlockResult::Data(first_data.clone())),
    };
    let second = BlockDataResponse {
        req_id: 1,
        seq_id: 3,
        result: Some(BlockResult::Data(second_data.clone())),
    };
    let decoder = OwnershipCheckingDecoder {
        inner: ProstDecoder::default(),
        payload: response_payload,
    };
    let mut stream = Streaming::new_response(
        decoder,
        fragmented_body(&first, &second, true),
        StatusCode::OK,
        None,
        None,
    );
    let decoded_first = stream.message().await.unwrap().unwrap();
    let decoded_second = stream.message().await.unwrap().unwrap();
    assert!(stream.message().await.unwrap().is_none());
    drop(stream);
    assert_eq!(response_payload(&decoded_first), &first_data);
    assert_eq!(response_payload(&decoded_second), &second_data);
    assert_eq!((decoded_first.seq_id, decoded_second.seq_id), (2, 3));
}

#[tokio::test]
async fn fragmented_coalesced_write_chunks_keep_owned_payloads() {
    let first_data = Bytes::from(vec![0x42; 40 * 1024]);
    let second_data = Bytes::from(vec![0xe1; 32 * 1024]);
    let make_request = |seq_id, data| BlockDataRequest {
        req_id: 7,
        seq_id,
        replication_token: None,
        operation: Some(Operation::WriteChunk(WriteBlockChunk {
            header: None,
            data: Some(data),
        })),
    };
    let first = make_request(1, first_data.clone());
    let second = make_request(2, second_data.clone());
    let decoder = OwnershipCheckingDecoder {
        inner: ProstDecoder::default(),
        payload: request_payload,
    };
    let mut stream =
        Streaming::new_request(decoder, fragmented_body(&first, &second, false), None, None);
    let decoded_first = stream.message().await.unwrap().unwrap();
    let decoded_second = stream.message().await.unwrap().unwrap();
    assert!(stream.message().await.unwrap().is_none());
    drop(stream);
    assert_eq!(request_payload(&decoded_first), &first_data);
    assert_eq!(request_payload(&decoded_second), &second_data);
    assert_eq!((decoded_first.seq_id, decoded_second.seq_id), (1, 2));
}
