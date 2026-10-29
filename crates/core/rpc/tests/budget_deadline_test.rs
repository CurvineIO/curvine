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

use curvine_io::IOError;
use curvine_net::net::NodeAddr;
use curvine_rpc::client::{ClientConf, ClusterConnector};
use curvine_rpc::message::{Builder, MessageBuilder};
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
