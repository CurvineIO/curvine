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

//! Real TCP tests for the serial worker receive deadline. Force the exact-read
//! path on every platform for protocol/header coverage; no splice syscall is
//! exercised by the header-only requests in the compatibility tests.

use super::RpcFrame;
use crate::handler::{Frame, StreamHandler, TestMessageHandler};
use crate::message::{Builder, Message, PROTOCOL_USIZE};
use crate::ServerConf;
use bytes::BytesMut;
use curvine_io::DataSlice;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout};

const DEADLINE: Duration = Duration::from_millis(30);
const WATCHDOG: Duration = Duration::from_secs(1);

async fn connected_pair(
    rt: Arc<Runtime>,
    exact_read: bool,
    close_idle: bool,
) -> (StreamHandler<RpcFrame, TestMessageHandler>, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (io, _) = listener.accept().await.unwrap();
    let conf = ServerConf {
        timeout_ms: DEADLINE.as_millis() as u64,
        close_idle,
        ..ServerConf::with_port(0)
    };
    let mut frame = RpcFrame::with_server(io, &conf);
    // Exercise the production exact-read implementation even on macOS.
    frame.enable_splice = exact_read;
    (
        StreamHandler::new(rt, frame, TestMessageHandler, &conf),
        peer,
    )
}

fn request_wire() -> BytesMut {
    let mut wire = BytesMut::new();
    Builder::new_rpc(1)
        .req_id(1964)
        .header(BytesMut::from(&b"request header!!"[..]))
        .data(DataSlice::Buffer(BytesMut::zeroed(16)))
        .build()
        .encode(&mut wire)
        .unwrap();
    wire
}

fn half_frame_must_close(prefix: usize, exact_read: bool, heartbeat: bool) {
    let rt = Arc::new(Runtime::new("half-frame-test", 1, 1));
    rt.block_on(async {
        let (mut handler, mut peer) = connected_pair(rt.clone(), exact_read, false).await;
        let mut wire = BytesMut::new();
        if heartbeat {
            Message::heartbeat().encode(&mut wire).unwrap();
        }
        wire.extend_from_slice(&request_wire()[..prefix]);
        peer.write_all(&wire).await.unwrap();
        let result = timeout(WATCHDOG, handler.run())
            .await
            .expect("a half-frame deadline must terminate, not restart receive");
        assert_eq!(result.unwrap_err().kind(), ErrorKind::ConnectionAborted);
        // RpcServer owns the handler in its connection task and drops it on
        // run() exit. Match that ownership boundary, including physical EOF.
        drop(handler);
        let mut byte = [0];
        let read = timeout(WATCHDOG, peer.read(&mut byte)).await.unwrap();
        assert!(
            matches!(read, Ok(0)) || read.is_err(),
            "peer must see shutdown"
        );
    });
}

#[test]
fn partial_protocol_timeout_closes_stream() {
    for exact_read in [false, true] {
        half_frame_must_close(8, exact_read, false);
    }
}

#[test]
fn partial_header_timeout_closes_stream() {
    for exact_read in [false, true] {
        // The entire protocol is consumed in one read. Progress must remain
        // visible while read_full waits for the remainder of the header.
        for prefix in [PROTOCOL_USIZE, PROTOCOL_USIZE + 8] {
            half_frame_must_close(prefix, exact_read, false);
        }
    }
}

#[test]
fn partial_data_timeout_closes_buffered_stream() {
    half_frame_must_close(PROTOCOL_USIZE + 16 + 8, false, false);
}

fn idle_connection_stays_usable(heartbeat: bool) {
    let rt = Arc::new(Runtime::new("idle-frame-test", 1, 1));
    rt.block_on(async {
        for exact_read in [false, true] {
            let (mut handler, mut peer) = connected_pair(rt.clone(), exact_read, false).await;
            if heartbeat {
                let mut wire = BytesMut::new();
                Message::heartbeat().encode(&mut wire).unwrap();
                peer.write_all(&wire).await.unwrap();
            }
            let run = handler.run();
            tokio::pin!(run);
            tokio::select! {
                result = &mut run => panic!("frame-boundary idle closed: {result:?}"),
                _ = sleep(DEADLINE * 4) => {},
            }
            let mut peer = RpcFrame::with_client(peer, 4096);
            for req_id in [1965, 1966] {
                peer.send(Builder::new_rpc(1).req_id(req_id).build()).await.unwrap();
                let response = timeout(WATCHDOG, async {
                    tokio::select! {
                        result = &mut run => panic!("idle connection cannot serve request: {result:?}"),
                        result = peer.receive() => result.unwrap(),
                    }
                }).await.unwrap();
                assert_eq!(response.req_id(), req_id);
                // A successful exact-read request must clear its progress
                // flag, not just the heartbeat fast path.
                tokio::select! {
                    result = &mut run => panic!("idle after request closed: {result:?}"),
                    _ = sleep(DEADLINE * 4) => {},
                }
            }
            drop(peer);
            timeout(WATCHDOG, &mut run).await.unwrap().unwrap();
        }
    });
}

#[test]
fn frame_boundary_idle_timeout_keeps_connection_usable() {
    idle_connection_stays_usable(false);
}

#[test]
fn complete_heartbeat_followed_by_idle_keeps_connection_usable() {
    idle_connection_stays_usable(true);
}

#[test]
fn close_idle_still_closes_an_empty_connection() {
    let rt = Arc::new(Runtime::new("close-idle-test", 1, 1));
    rt.block_on(async {
        for exact_read in [false, true] {
            let (mut handler, _peer) = connected_pair(rt.clone(), exact_read, true).await;
            timeout(WATCHDOG, handler.run()).await.unwrap().unwrap();
        }
    });
}

#[test]
fn cancelled_exact_read_cannot_parse_a_new_frame() {
    let rt = Arc::new(Runtime::new("cancel-exact-test", 1, 1));
    rt.block_on(async {
        for prefix in [8, PROTOCOL_USIZE, PROTOCOL_USIZE + 8] {
            let (mut handler, mut peer) = connected_pair(rt.clone(), true, false).await;
            peer.write_all(&request_wire()[..prefix]).await.unwrap();
            assert!(timeout(DEADLINE, handler.frame_mut().receive())
                .await
                .is_err());
            let result = timeout(WATCHDOG, handler.frame_mut().receive())
                .await
                .expect("cancelled exact-read parser must reject reuse immediately");
            assert_eq!(result.unwrap_err().kind(), ErrorKind::ConnectionAborted);
        }
    });
}

#[test]
fn heartbeat_then_partial_protocol_timeout_closes_stream() {
    for exact_read in [false, true] {
        half_frame_must_close(8, exact_read, true);
    }
}

#[test]
fn buffered_receive_preserves_bytes_on_cancellation() {
    let rt = Arc::new(Runtime::new("cancel-buffer-test", 1, 1));
    rt.block_on(async {
        for prefix in [8, PROTOCOL_USIZE + 8, PROTOCOL_USIZE + 16 + 8] {
            let (mut handler, mut peer) = connected_pair(rt.clone(), false, false).await;
            let wire = request_wire();
            peer.write_all(&wire[..prefix]).await.unwrap();
            assert!(timeout(DEADLINE, handler.frame_mut().receive())
                .await
                .is_err());
            peer.write_all(&wire[prefix..]).await.unwrap();
            let message = timeout(WATCHDOG, handler.frame_mut().receive())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(message.req_id(), 1964);
            assert_eq!(message.header_len(), 16);
            assert_eq!(message.data.len(), 16);
        }
    });
}

#[test]
fn complete_request_then_partial_protocol_timeout_closes_stream() {
    let rt = Arc::new(Runtime::new("coalesced-half-test", 1, 1));
    rt.block_on(async {
        for exact_read in [false, true] {
            let (mut handler, mut peer) = connected_pair(rt.clone(), exact_read, false).await;
            let mut wire = BytesMut::new();
            Builder::new_rpc(1)
                .req_id(1963)
                .build()
                .encode(&mut wire)
                .unwrap();
            wire.extend_from_slice(&request_wire()[..8]);
            peer.write_all(&wire).await.unwrap();
            {
                let run = handler.run();
                tokio::pin!(run);
                let mut peer_frame = RpcFrame::with_client(peer, 4096);
                let response = timeout(WATCHDOG, async {
                    tokio::select! {
                        result = &mut run => panic!("complete request was not served: {result:?}"),
                        result = peer_frame.receive() => result.unwrap(),
                    }
                })
                .await
                .unwrap();
                assert_eq!(response.req_id(), 1963);
                let result = timeout(WATCHDOG, &mut run).await.unwrap();
                assert_eq!(result.unwrap_err().kind(), ErrorKind::ConnectionAborted);
            }
        }
    });
}
