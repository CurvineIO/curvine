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

//! Send-side idle heartbeat for raw RPC connections.
//!
//! A raw data connection owns its frame exclusively and has no timer, so the
//! owning write path pokes it from wherever it waits for the next chunk. These
//! tests pin the properties that make that safe: an unexpired connection writes
//! nothing, an expired one writes exactly one frame, and the peer consumes that
//! frame instead of surfacing it as a message.

use curvine_rpc::client::ClientConf;
use curvine_rpc::handler::{Frame, RpcFrame};
use curvine_rpc::message::{Builder, Message, RequestStatus};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};

/// A normal request message, so a heartbeat can be distinguished from real work
/// on the same stream.
fn request() -> Message {
    Builder::new()
        .request(RequestStatus::Rpc)
        .new_req_id()
        .build()
}

/// Builds a connected client/server frame pair over loopback.
async fn frame_pair(heartbeat: Option<Duration>) -> (RpcFrame, RpcFrame) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    let client = TcpStream::connect(addr).await.expect("connect");
    let (server, _) = listener.accept().await.expect("accept");

    let client = RpcFrame::with_client(client, 8 * 1024).with_heartbeat(heartbeat);
    let server = RpcFrame::with_server(server, &Default::default());
    (client, server)
}

/// The heartbeat costs nothing on a connection that is actively used: a write
/// refreshes the clock, so the next poke stays silent.
#[tokio::test]
async fn heartbeat_is_skipped_while_the_connection_is_fresh() {
    let (mut client, _server) = frame_pair(Some(Duration::from_secs(60))).await;

    assert!(
        !client.heartbeat_if_idle().await.expect("heartbeat"),
        "a just-opened connection is not idle"
    );

    client.send(request()).await.expect("seed write");
    assert!(
        !client.heartbeat_if_idle().await.expect("heartbeat"),
        "a write refreshed the send clock"
    );
}

/// Once the configured silence elapses exactly one heartbeat goes out, and the
/// clock refresh makes the immediately following poke silent again.
#[tokio::test]
async fn heartbeat_fires_once_after_the_idle_period() {
    let (mut client, mut server) = frame_pair(Some(Duration::from_millis(20))).await;

    assert!(!client.heartbeat_if_idle().await.expect("fresh"));

    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(
        client.heartbeat_if_idle().await.expect("due"),
        "the idle period elapsed, a heartbeat is due"
    );
    assert!(
        !client.heartbeat_if_idle().await.expect("after beat"),
        "sending refreshed the clock"
    );

    // The peer swallows the heartbeat inside its read loop, so the first frame it
    // surfaces must be the real request sent after the beat, never the beat.
    client.send(request()).await.expect("trailing request");
    let received = tokio::time::timeout(Duration::from_secs(5), server.receive())
        .await
        .expect("server received a frame")
        .expect("decode");
    assert!(
        !received.is_heartbeat(),
        "a heartbeat must be consumed by the peer, not delivered as a message"
    );
}

/// A heartbeat must never disturb the stream for real traffic: interleaving one
/// between two requests leaves both requests decodable in order.
#[tokio::test]
async fn heartbeat_between_requests_keeps_the_stream_in_order() {
    let (mut client, mut server) = frame_pair(Some(Duration::from_millis(10))).await;

    let first = request();
    let first_id = first.req_id();
    let second = request();
    let second_id = second.req_id();

    client.send(first).await.expect("first");
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(client.heartbeat_if_idle().await.expect("beat"));
    client.send(second).await.expect("second");

    let one = server.receive().await.expect("first frame");
    let two = server.receive().await.expect("second frame");
    assert_eq!(one.req_id(), first_id);
    assert_eq!(two.req_id(), second_id);
}

/// Without a configured period the call is a permanent no-op, which is what keeps
/// every existing client's behaviour byte-identical until it opts in.
#[tokio::test]
async fn disabled_heartbeat_never_writes() {
    let (mut client, _server) = frame_pair(None).await;

    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!client.heartbeat_if_idle().await.expect("disabled"));
    assert!(!client.heartbeat_if_idle().await.expect("still disabled"));
}

/// A zero interval means "disabled", not "heartbeat on every call".
#[tokio::test]
async fn zero_interval_disables_the_heartbeat() {
    let (mut client, _server) = frame_pair(Some(Duration::ZERO)).await;

    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!client.heartbeat_if_idle().await.expect("zero interval"));
}

/// The period is derived only from an explicit opt-in.
#[test]
fn heartbeat_period_requires_an_explicit_opt_in() {
    let mut conf = ClientConf::default();
    assert_eq!(conf.heartbeat_period(), None);

    conf.heartbeat_interval_ms = 30_000;
    assert_eq!(conf.heartbeat_period(), Some(Duration::from_millis(30_000)));
}
