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

use crate::common::Utils;
use crate::io::retry::{TimeBondedRetry, TimeBondedRetryBuilder};
use crate::runtime::Runtime;
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

    // How many connections can be used when connecting to share.
    pub conn_size: usize,

    pub use_libc: bool,

    pub use_ucp: bool,
    pub ucp_tag_max_len: usize,
    pub ucp_small_use_tag: bool,
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

            conn_retry_max_duration_ms: 2 * 60 * 1000,
            conn_retry_min_sleep_ms: 300,
            conn_retry_max_sleep_ms: 10 * 1000,

            io_retry_max_duration_ms: 5 * 60 * 1000,
            io_retry_min_sleep_ms: 300,
            io_retry_max_sleep_ms: 30 * 1000,

            conn_timeout_ms: 30 * 1000,
            rpc_timeout_ms: 2 * 60 * 1000,
            data_timeout_ms: 5 * 60 * 1000,

            conn_size: 1,

            use_libc: false,

            use_ucp: false,
            ucp_tag_max_len: 4096,
            ucp_small_use_tag: false,
        }
    }
}
