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

//! Server-side connection handling: `StreamHandler` (`is_stream() == true`)
//! versus `BufferHandler` (`is_stream() == false`).
//!
//! The probe handler sleeps for the number of milliseconds carried in the
//! request data and records the peak number of requests it ran at the same
//! time. All requests are pipelined over one `BufferClient` connection.

use bytes::BytesMut;
use curvine_core_error::{CommonErrorExt, CommonResultExt};
use curvine_io::DataSlice;
use curvine_net::net::{ConnState, InetAddr, NetUtils};
use curvine_rpc::client::{ClientConf, RpcClient};
use curvine_rpc::handler::{HandlerService, LimitConf, MessageHandler};
use curvine_rpc::message::{Builder, Message};
use curvine_rpc::server::{RpcServer, ServerConf};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Default)]
struct Probe {
    active: AtomicUsize,
    peak: AtomicUsize,
}

struct ProbeHandler {
    probe: Arc<Probe>,
}

impl MessageHandler for ProbeHandler {
    type Error = CommonErrorExt;

    // "async-panic" requests take the async path and panic there.
    fn is_sync(&self, msg: &Message) -> bool {
        msg.data_bytes() != Some(b"async-panic".as_slice())
    }

    async fn async_handle(&self, _msg: Message) -> CommonResultExt<Message> {
        panic!("probe async handler panic")
    }

    fn handle(&self, msg: &Message) -> CommonResultExt<Message> {
        let data = String::from_utf8_lossy(msg.data_bytes().unwrap_or_default()).to_string();
        if data == "panic" {
            panic!("probe handler panic");
        }
        let sleep_ms: u64 = data.parse().unwrap_or(0);

        let now = self.probe.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.probe.peak.fetch_max(now, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(sleep_ms));
        self.probe.active.fetch_sub(1, Ordering::SeqCst);

        // Echo the request data so the caller can check it got its own reply.
        let header = BytesMut::from(sleep_ms.to_string().as_bytes());
        Ok(msg.success_with_data(Some(header), DataSlice::Empty))
    }
}

struct ProbeService {
    probe: Arc<Probe>,
    stream: bool,
    limit: LimitConf,
}

impl HandlerService for ProbeService {
    type Item = ProbeHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        ProbeHandler {
            probe: self.probe.clone(),
        }
    }

    fn is_stream(&self) -> bool {
        self.stream
    }

    fn get_limit(&self) -> LimitConf {
        self.limit.clone()
    }
}

struct TestServer {
    rt: Arc<Runtime>,
    addr: InetAddr,
    probe: Arc<Probe>,
}

fn start_server(stream: bool, conn_limit: usize, global_limit: usize) -> TestServer {
    start_server_with(stream, conn_limit, global_limit, |_| {})
}

fn start_server_with(
    stream: bool,
    conn_limit: usize,
    global_limit: usize,
    configure: impl FnOnce(&mut ServerConf),
) -> TestServer {
    let probe = Arc::new(Probe::default());
    let service = ProbeService {
        probe: probe.clone(),
        stream,
        limit: LimitConf::new(conn_limit, global_limit),
    };

    let mut conf = ServerConf::with_hostname("127.0.0.1", NetUtils::hold_available_port());
    // Enough blocking threads that the pool is never the limiting factor.
    conf.worker_threads = 32;
    configure(&mut conf);
    let server = RpcServer::new(conf, service);
    let addr = server.bind_addr().clone();
    let rt = server.clone_rt();

    let mut listener = server.start();
    rt.block_on(listener.wait_startup())
        .expect("probe server must reach the Running state");

    TestServer { rt, addr, probe }
}

async fn connect(server: &TestServer) -> RpcClient {
    RpcClient::new(
        true,
        server.rt.clone(),
        &server.addr,
        &ClientConf::default(),
    )
    .await
    .expect("client must connect to the probe server")
}

/// Sends one request per entry of `sleeps_ms` concurrently on `client`, and
/// checks every response carries the value of its own request.
async fn send_all(client: &RpcClient, sleeps_ms: &[u64]) {
    let mut tasks = Vec::new();
    for &sleep_ms in sleeps_ms {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            let msg = Builder::new_rpc(1i8)
                .data(DataSlice::from_str(sleep_ms.to_string()))
                .build();
            let rep = client
                .timeout_rpc(Duration::from_secs(30), msg)
                .await
                .expect("probe handler must answer");
            let echoed = String::from_utf8_lossy(rep.header_bytes().unwrap()).to_string();
            assert_eq!(
                echoed,
                sleep_ms.to_string(),
                "response matched wrong request"
            );
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
}

#[test]
fn stream_handler_processes_one_request_at_a_time() {
    let server = start_server(true, 8, 4096);
    server.rt.block_on(async {
        let client = connect(&server).await;
        send_all(&client, &[100; 4]).await;
    });

    assert_eq!(server.probe.peak.load(Ordering::SeqCst), 1);
}

#[test]
fn buffer_handler_processes_requests_concurrently() {
    let server = start_server(false, 8, 4096);
    let start = Instant::now();
    server.rt.block_on(async {
        let client = connect(&server).await;
        send_all(&client, &[300; 4]).await;
    });
    let elapsed = start.elapsed();

    assert_eq!(server.probe.peak.load(Ordering::SeqCst), 4);
    // Serial processing would take at least 4 * 300ms.
    assert!(elapsed < Duration::from_millis(1200), "took {:?}", elapsed);
}

#[test]
fn buffer_handler_returns_out_of_order_responses_to_the_right_caller() {
    let server = start_server(false, 8, 4096);
    server.rt.block_on(async {
        let client = connect(&server).await;
        // Later requests finish first.
        send_all(&client, &[400, 300, 200, 100, 0]).await;
    });

    assert!(server.probe.peak.load(Ordering::SeqCst) > 1);
}

#[test]
fn buffer_handler_respects_connection_limit() {
    let server = start_server(false, 2, 4096);
    server.rt.block_on(async {
        let client = connect(&server).await;
        send_all(&client, &[100; 6]).await;
    });

    assert_eq!(server.probe.peak.load(Ordering::SeqCst), 2);
}

#[test]
fn buffer_handler_global_limit_is_shared_by_connections() {
    let server = start_server(false, 8, 3);
    server.rt.block_on(async {
        let c1 = connect(&server).await;
        let c2 = connect(&server).await;
        tokio::join!(send_all(&c1, &[100; 4]), send_all(&c2, &[100; 4]));
    });

    assert_eq!(server.probe.peak.load(Ordering::SeqCst), 3);
}

#[test]
fn buffer_handler_releases_global_permits_after_requests_finish() {
    // With only 2 global permits, the second connection can finish (instead
    // of timing out) only if the first connection released all its permits.
    let server = start_server(false, 8, 2);
    server.rt.block_on(async {
        let c1 = connect(&server).await;
        send_all(&c1, &[50; 4]).await;
        drop(c1);

        let c2 = connect(&server).await;
        send_all(&c2, &[50; 2]).await;
    });

    assert_eq!(server.probe.peak.load(Ordering::SeqCst), 2);
    assert_eq!(server.probe.active.load(Ordering::SeqCst), 0);
}

/// Sends `payload` (which makes the probe panic) and requires the call to fail
/// quickly because the server closed the connection. `client.rpc` has no
/// deadline of its own, so the outer timeout separates "connection closed"
/// from "call hung".
fn assert_panic_closes_connection(payload: &'static str) {
    let server = start_server(false, 8, 4096);
    server.rt.block_on(async {
        let client = connect(&server).await;
        let msg = Builder::new_rpc(1i8)
            .data(DataSlice::from_str(payload))
            .build();
        let res = tokio::time::timeout(Duration::from_secs(2), client.rpc(msg))
            .await
            .expect("panicking request must fail fast, not hang");
        assert!(res.is_err(), "panicking request must return an error");

        // A new connection is served normally.
        let client = connect(&server).await;
        send_all(&client, &[10; 2]).await;
    });
}

#[test]
fn buffer_handler_closes_connection_when_handler_panics() {
    assert_panic_closes_connection("panic");
}

#[test]
fn buffer_handler_closes_connection_when_async_handler_panics() {
    assert_panic_closes_connection("async-panic");
}

fn sleep_request(sleep_ms: u64) -> Message {
    Builder::new_rpc(1i8)
        .data(DataSlice::from_str(sleep_ms.to_string()))
        .build()
}

#[test]
fn buffer_handler_releases_permits_when_client_disconnects_mid_request() {
    // One global permit. The first client disconnects while its request is
    // still executing; the reply then has no reader. The second client can
    // run only if that path releases both permits.
    let server = start_server(false, 8, 1);
    server.rt.block_on(async {
        let c1 = connect(&server).await;
        let call = tokio::spawn(async move { c1.rpc(sleep_request(500)).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        call.abort();
        let _ = call.await;

        let c2 = connect(&server).await;
        tokio::time::timeout(Duration::from_secs(5), send_all(&c2, &[10; 2]))
            .await
            .expect("second connection must get the released permit");
    });

    assert_eq!(server.probe.peak.load(Ordering::SeqCst), 1);
    assert_eq!(server.probe.active.load(Ordering::SeqCst), 0);
}

#[test]
fn buffer_handler_closes_connection_when_permit_wait_times_out() {
    // One global permit held by a slow request on c1. A request on c2 waits
    // for that permit; after the server timeout (300ms) c2's connection must
    // close, well before the slow request finishes. Idle close is off so only
    // the permit deadline can close it.
    let server = start_server_with(false, 8, 1, |conf| {
        conf.timeout_ms = 300;
        conf.close_idle = false;
    });
    server.rt.block_on(async {
        let c1 = connect(&server).await;
        let slow = tokio::spawn(async move { c1.rpc(sleep_request(1500)).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let c2 = connect(&server).await;
        let res = tokio::time::timeout(Duration::from_millis(1000), c2.rpc(sleep_request(0)))
            .await
            .expect("waiting for a permit must not hang past the server timeout");
        assert!(res.is_err(), "undispatched request must fail");

        // The slow request on the other connection is unaffected.
        slow.await
            .unwrap()
            .expect("slow request must still complete");
    });
}
