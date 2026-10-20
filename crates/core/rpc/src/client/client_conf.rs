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

use curvine_net::retry::{TimeBondedRetry, TimeBondedRetryBuilder};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::Runtime;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ClientConf {
    pub io_threads: usize,
    pub worker_threads: usize,
    pub buffer_size: usize,
    pub close_idle: bool,
    pub message_size: usize,

    // Set the connection timeout and retry policies on the client side.
    pub conn_retry_max_duration_ms: u64,
    pub conn_retry_min_sleep_ms: u64,
    pub conn_retry_max_sleep_ms: u64,

    // Set the read and write data timeout and retry policies.
    pub io_retry_max_duration_ms: u64,
    pub io_retry_min_sleep_ms: u64,
    pub io_retry_max_sleep_ms: u64,

    // The connection timeout time is 30s by default.
    // socket data read and write timeout time, default is 60s.
    // It is the timeout time of a connection or request, which has a certain relationship with the time of retrying the policy.
    pub conn_timeout_ms: u64,
    pub rpc_timeout_ms: u64,
    pub data_timeout_ms: u64,

    /// Upper bound on send-side silence for raw (non-buffered) connections.
    ///
    /// A raw client owns its frame exclusively and has no timer, so it cannot
    /// emit heartbeats on its own: the owning write path pokes the frame from
    /// wherever it waits for the next chunk. `None`/`0` disables the heartbeat
    /// and preserves the previous behaviour. When enabled it must stay well
    /// below the peer's idle timeout, otherwise the peer reaps the connection
    /// before a heartbeat can refresh it.
    pub heartbeat_interval_ms: u64,

    // How many connections can be used when connecting to share.
    pub conn_size: usize,

    pub use_libc: bool,
}

impl ClientConf {
    pub fn conn_retry_policy(&self) -> TimeBondedRetry {
        TimeBondedRetry::new(
            Duration::from_millis(self.conn_retry_max_duration_ms),
            Duration::from_millis(self.conn_retry_min_sleep_ms),
            Duration::from_millis(self.conn_retry_max_sleep_ms),
        )
    }

    pub fn io_retry_policy(&self) -> TimeBondedRetry {
        TimeBondedRetry::new(
            Duration::from_millis(self.io_retry_max_duration_ms),
            Duration::from_millis(self.io_retry_min_sleep_ms),
            Duration::from_millis(self.io_retry_max_sleep_ms),
        )
    }

    pub fn io_retry_builder(&self) -> TimeBondedRetryBuilder {
        TimeBondedRetryBuilder::new(
            Duration::from_millis(self.io_retry_max_duration_ms),
            Duration::from_millis(self.io_retry_min_sleep_ms),
            Duration::from_millis(self.io_retry_max_sleep_ms),
        )
    }

    /// Send-side idle period after which a heartbeat is due.
    ///
    /// Half the peer's idle timeout is the right ratio: two heartbeats land
    /// inside every peer window, so a single lost or delayed frame cannot get a
    /// healthy connection reaped.
    pub fn heartbeat_period(&self) -> Option<Duration> {
        (self.heartbeat_interval_ms > 0).then(|| Duration::from_millis(self.heartbeat_interval_ms))
    }
}

impl ClientConf {
    pub fn create_runtime(&self) -> Runtime {
        Runtime::new("rpc-client", self.io_threads, self.worker_threads)
    }
}

impl Default for ClientConf {
    fn default() -> Self {
        ClientConf {
            io_threads: 16,
            worker_threads: Utils::worker_threads(16),
            buffer_size: 128 * 1024,
            close_idle: false,
            message_size: 16,

            conn_retry_max_duration_ms: 60 * 1000,
            conn_retry_min_sleep_ms: 100,
            conn_retry_max_sleep_ms: 2 * 1000,

            // Retry budget is 2x the per-RPC timeout on purpose: the first
            // sweep pays the full per-node timeout to discover dead endpoints
            // (e.g. half-open pooled connections to restarted servers) and
            // ejects them; only a second sweep can then reconnect fast and
            // succeed. A budget equal to one timeout makes blackhole retries
            // exactly one sweep. See TimeBondedRetryBuilder.
            io_retry_max_duration_ms: 2 * 120 * 1000,
            io_retry_min_sleep_ms: 100,
            io_retry_max_sleep_ms: 10 * 1000,

            conn_timeout_ms: 30 * 1000,
            rpc_timeout_ms: 2 * 60 * 1000,
            data_timeout_ms: 2 * 60 * 1000,

            // Off by default: enabling it only matters for peers that reap
            // idle connections, and turning it on changes the wire behaviour of
            // every raw connection, so it stays an explicit opt-in.
            heartbeat_interval_ms: 0,

            conn_size: 1,

            use_libc: false,
        }
    }
}
