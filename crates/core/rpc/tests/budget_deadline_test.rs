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

//! Regression for issue #1815: the master RPC retry budget must behave like a
//! deadline.
//!
//! `ClusterConnector::rpc` used to spend one full `rpc_timeout` on the leader
//! probe and then hand a fresh retry budget to the polling loop, so a call could
//! outlive `rpc_retry_max_duration_ms`. The peer below accepts the TCP
//! connection and never answers, which is what an unreachable control plane
//! looks like once the connection is up but the service does not respond.

use curvine_core_error::{CommonErrorExt, CommonResultExt};
use curvine_io::{DataSlice, IOError};
use curvine_net::net::{ConnState, NetUtils, NodeAddr};
use curvine_rpc::client::{ClientConf, ClusterConnector};
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::{Builder, Message, MessageBuilder};
use curvine_rpc::server::{RpcServer, ServerConf};
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The retry budget used by the budget test.
const BUDGET_MS: u64 = 1_500;
/// The per-request timeout used by the single-node test.
const RPC_TIMEOUT_MS: u64 = 700;
/// Everything above the configured bound but far below the unclamped timeout
/// below means the deadline was honoured.
const PATIENCE: Duration = Duration::from_secs(6);

/// Bind a listener that accepts connections and never writes a reply.
fn stalled_endpoint() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind a local stalled endpoint");
    let port = listener.local_addr().unwrap().port();

    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => held.push(stream),
                Err(_) => break,
            }
        }
    });

    port
}

fn connector(port: u16, budget_ms: u64, rpc_timeout_ms: u64) -> ClusterConnector {
    let conf = ClientConf {
        io_retry_max_duration_ms: budget_ms,
        io_retry_min_sleep_ms: 50,
        io_retry_max_sleep_ms: 200,
        // A single unclamped attempt would wait a minute on this stalled peer.
        rpc_timeout_ms,
        conn_timeout_ms: 1_000,
        ..Default::default()
    };

    let rt = Arc::new(AsyncRuntime::single());
    let connector = ClusterConnector::with_rt(conf, rt.clone());

    // Node 1 plays the leader so the leader probe is exercised first.
    connector
        .add_node(NodeAddr::new(1, "127.0.0.1", port))
        .unwrap();
    connector.change_leader(1);
    connector
}

#[test]
fn rpc_retry_budget_is_a_deadline() {
    let port = stalled_endpoint();
    let connector = connector(port, BUDGET_MS, 60_000);
    let rt = connector.clone_runtime();

    rt.block_on(async move {
        let msg = MessageBuilder::new_rpc(1i8).build();
        let start = Instant::now();
        let res = connector.rpc::<IOError>(msg).await;
        let elapsed = start.elapsed();

        let err = res.expect_err("a stalled master must not answer");
        assert!(
            elapsed < PATIENCE,
            "rpc() outlived its {} ms budget: {:?} ({})",
            BUDGET_MS,
            elapsed,
            err
        );
        assert!(
            elapsed >= Duration::from_millis(BUDGET_MS),
            "rpc() gave up before spending its budget: {:?}",
            elapsed
        );
    });
}

struct EchoHandler;

impl MessageHandler for EchoHandler {
    type Error = CommonErrorExt;

    fn handle(&self, msg: &Message) -> CommonResultExt<Message> {
        Ok(msg.success_with_data(None, DataSlice::Empty))
    }
}

struct EchoService;

impl HandlerService for EchoService {
    type Item = EchoHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        EchoHandler
    }
}

/// A stalled cached leader must not consume the whole budget: with a healthy
/// member available, `rpc()` must still fail over to it within the budget.
#[test]
fn stalled_leader_still_fails_over_within_budget() {
    let conf = ServerConf::with_hostname("127.0.0.1", NetUtils::hold_available_port());
    let server = RpcServer::new(conf, EchoService);
    let healthy_port = server.bind_addr().port;
    let server_rt = server.clone_rt();
    let mut listener = server.start();
    server_rt
        .block_on(listener.wait_startup())
        .expect("healthy master must reach the Running state");

    // The per-node timeout is longer than the budget, so an unclamped leader
    // probe would spend the whole budget on the stalled leader.
    let connector = connector(stalled_endpoint(), BUDGET_MS, 3_000);
    connector
        .add_node(NodeAddr::new(2, "127.0.0.1", healthy_port))
        .unwrap();
    let rt = connector.clone_runtime();

    rt.block_on(async move {
        let msg = MessageBuilder::new_rpc(1i8).build();
        let start = Instant::now();
        let res = connector.rpc::<IOError>(msg).await;
        let elapsed = start.elapsed();

        res.unwrap_or_else(|e| panic!("rpc() did not fail over after {elapsed:?}: {e}"));
        assert_eq!(
            connector.leader_id(),
            Some(2),
            "the healthy member must become the cached leader"
        );
        assert!(
            elapsed < Duration::from_millis(BUDGET_MS),
            "failover must finish within the {} ms budget: {:?}",
            BUDGET_MS,
            elapsed
        );
    });
}

/// One node call is bounded by `rpc_timeout_ms`, covering the connect handshake
/// as well as the request/reply.
#[test]
fn per_node_rpc_is_bounded_by_its_timeout() {
    let port = stalled_endpoint();
    let connector = connector(port, 60_000, RPC_TIMEOUT_MS);
    let rt = connector.clone_runtime();

    rt.block_on(async move {
        let msg = Builder::new_rpc(1i8).build();
        let start = Instant::now();
        let res = connector.timeout_rpc::<IOError>(1, msg).await;
        let elapsed = start.elapsed();

        let (_retry, err) = res.expect_err("a stalled master must not answer");
        assert!(
            elapsed < PATIENCE,
            "timeout_rpc() outlived its {} ms timeout: {:?} ({})",
            RPC_TIMEOUT_MS,
            elapsed,
            err
        );
        assert!(
            elapsed >= Duration::from_millis(RPC_TIMEOUT_MS),
            "timeout_rpc() gave up before its timeout: {:?}",
            elapsed
        );
    });
}
