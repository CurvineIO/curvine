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

#![cfg(target_os = "linux")]

//! Kernel TCP_USER_TIMEOUT tests on isolated loopback sockets. No firewall,
//! routing, network namespace, elevated capability or external peer is needed.
//! The receive buffer is constrained before the handshake, then never drained,
//! so a large request fills the receive window and backpressures the sender.

use bytes::BytesMut;
use curvine_io::DataSlice;
use curvine_net::net::InetAddr;
use curvine_rpc::client::{ClientConf, RpcClient};
use curvine_rpc::handler::{Frame, RpcFrame};
use curvine_rpc::message::{Builder, Message};
use socket2::SockRef;
use std::io::ErrorKind;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const KERNEL_BUDGET_MS: u64 = 400;
const CONTROL_WINDOW: Duration = Duration::from_millis(1200);
const WATCHDOG: Duration = Duration::from_secs(5);

async fn stalled_pair(millis: u64) -> (RpcClient, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    // Setting this before accept/connect also constrains the initial TCP window;
    // changing only an accepted socket may leave a previously advertised window.
    SockRef::from(&listener).set_recv_buffer_size(4096).unwrap();
    let addr: InetAddr = listener.local_addr().unwrap().into();
    let conf = ClientConf {
        rpc_timeout_ms: millis,
        ..ClientConf::default()
    };
    let (client, peer) = timeout(WATCHDOG, async {
        tokio::join!(RpcClient::with_raw(&addr, &conf), listener.accept())
    })
    .await
    .unwrap();
    (client.unwrap(), peer.unwrap().0)
}

fn large_request() -> Message {
    Builder::new_rpc(1)
        .req_id(1964)
        .data(DataSlice::Buffer(BytesMut::zeroed(16 * 1024 * 1024)))
        .build()
}

#[tokio::test]
async fn zero_window_kernel_timeout_poisons_raw_connection() {
    let (client, _peer) = stalled_pair(KERNEL_BUDGET_MS).await;
    let start = Instant::now();
    // send() has no application deadline. This outer watchdog is only a test
    // failure bound: it must NOT be the source of the observed TimedOut error.
    let error = timeout(WATCHDOG, client.send(large_request()))
        .await
        .expect("kernel TCP_USER_TIMEOUT must expire before the test watchdog")
        .expect_err("a non-reading peer cannot accept the whole request");
    assert_eq!(error.kind(), ErrorKind::TimedOut);
    assert!(
        error.raw_error().raw_os_error().is_some(),
        "the timeout must come from the socket, not tokio::time::timeout"
    );
    eprintln!(
        "zero-window: kernel_budget={}ms elapsed={:?} socket_errno={:?}",
        KERNEL_BUDGET_MS,
        start.elapsed(),
        error.raw_error().raw_os_error()
    );
    assert!(client.is_closed());
    assert!(!client.is_reusable());
    assert!(client.send(Builder::new_rpc(1).build()).await.is_err());
}

#[tokio::test]
async fn zero_user_timeout_leaves_zero_window_write_pending() {
    let (client, _peer) = stalled_pair(0).await;
    let start = Instant::now();
    let result = timeout(CONTROL_WINDOW, client.send(large_request())).await;
    assert!(
        result.is_err(),
        "with TCP_USER_TIMEOUT=0 the kernel must not fail within the control window: {result:?}"
    );
    eprintln!(
        "zero-window control: kernel_budget=0ms application_watchdog={:?} elapsed={:?}",
        CONTROL_WINDOW,
        start.elapsed()
    );
    // The control watchdog now cancels send(). The in-flight guard must poison
    // the connection even though this time the kernel did not close it first.
    assert!(client.is_closed());
    assert!(!client.is_reusable());
}

#[tokio::test]
async fn acked_request_without_reply_still_needs_application_timeout() {
    let (client, peer) = stalled_pair(KERNEL_BUDGET_MS).await;
    let request = Builder::new_rpc(1).req_id(1965).seq_id(0).build();
    let mut frame = RpcFrame::with_client(peer, 4096);
    let (sent, received) = timeout(WATCHDOG, async {
        tokio::join!(client.send(request), frame.receive())
    })
    .await
    .unwrap();
    sent.unwrap();
    assert_eq!(received.unwrap().req_id(), 1965);
    assert!(client.is_active());
    let start = Instant::now();
    let error = timeout(WATCHDOG, client.timeout_receive(CONTROL_WINDOW, 1965, 0))
        .await
        .unwrap()
        .expect_err("the peer deliberately never replies");
    assert_eq!(error.kind(), ErrorKind::TimedOut);
    assert_eq!(error.raw_error().raw_os_error(), None);
    assert!(
        start.elapsed() >= CONTROL_WINDOW,
        "TCP_USER_TIMEOUT must not act as a reply/idle deadline"
    );
    eprintln!(
        "ACKed no-reply: kernel_budget={}ms application_budget={:?} elapsed={:?} socket_errno=None",
        KERNEL_BUDGET_MS,
        CONTROL_WINDOW,
        start.elapsed()
    );
    assert!(client.is_closed());
    assert!(!client.is_reusable());
}
