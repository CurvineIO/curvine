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

use crate::conf::ClusterConf;
use crate::raft::RaftPeer;
use orpc::client::ClientConf;
use orpc::common::Utils;
use orpc::io::net::{InetAddr, NetUtils};
use orpc::runtime::Runtime;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::vec;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct JournalConf {
    // If you enable raft log synchronization, logs will not be synchronized,
    // which is equivalent to a stand-alone system.
    pub enable: bool,

    pub group_name: String,
    pub hostname: String,
    pub rpc_port: u16,

    pub io_threads: usize,
    pub worker_threads: usize,
    pub message_size: usize,

    // Master candidate node
    pub journal_addrs: Vec<RaftPeer>,

    // raft log storage configuration
    pub journal_dir: String,

    // Write enables debug and will print every log.
    pub writer_debug: bool,

    // The buffer queue size when journal is written, default is 200_000
    // The queue size is high in concurrency, which has a great impact on metadata performance. It can be set to 0 and use an unbounded queue.
    pub writer_channel_size: usize,
    pub writer_flush_batch_size: u64,
    pub writer_flush_batch_ms: u64,

    // Snapshot creation interval
    pub snapshot_interval: String,

    // How many entries are created after creating snapshots.
    pub snapshot_entries: u64,

    pub snapshot_read_chunk_size: usize,

    // Network configuration between raft node communications.
    // Set the client service connection timeout and retry policy.
    pub conn_retry_max_duration_ms: u64,
    pub conn_retry_min_sleep_ms: u64,
    pub conn_retry_max_sleep_ms: u64,

    // Set the read and write data timeout and retry policies.
    pub rpc_close_idle: bool,

    pub rpc_retry_max_duration_ms: u64,
    pub rpc_retry_min_sleep_ms: u64,
    pub rpc_retry_max_sleep_ms: u64,

    // The connection timeout time is 30s by default.
    // socket data read and write timeout time, default is 60s.
    // It is the timeout time of a connection or request, which has a certain relationship with the time of retrying the policy.
    pub conn_timeout_ms: u64,
    pub io_timeout_ms: u64,

    // How many connections can be used when connecting to share.
    pub conn_size: usize,

    // raft related configuration
    pub raft_poll_interval_ms: u64,
    pub raft_tick_interval_ms: u64,
    pub raft_election_tick: usize,
    pub raft_heartbeat_tick: usize,
    pub raft_min_election_ticks: usize,
    pub raft_max_election_ticks: usize,
    pub raft_check_quorum: bool,
    pub raft_max_size_per_msg: u64,
    pub raft_max_inflight_msgs: usize,
    pub raft_max_committed_size_per_ready: u64,

    // Raft requests to retry the cache configuration to prevent duplicate requests.
    pub raft_retry_cache_size: u64,
    pub raft_retry_cache_ttl: String,

    // The number of checkpoints saved.
    pub retain_checkpoint_num: usize,

    // Whether to ignore errors in the log replay process.
    pub ignore_replay_error: bool,
}

impl JournalConf {
    pub const DEFAULT_NODE_ID: u64 = 0;

    // Create a test configuration, which will also randomly select a server port.
    pub fn with_test() -> Self {
        let mut conf = Self::default();
        let port = NetUtils::get_available_port();
        conf.rpc_port = port;
        conf.journal_addrs = vec![RaftPeer::from_addr(&conf.hostname, port)];
        conf
    }

    pub fn create_runtime(&self) -> Arc<Runtime> {
        let rt = Runtime::new("raft-rpc", self.io_threads, self.worker_threads);
        Arc::new(rt)
    }

    pub fn local_addr(&self) -> InetAddr {
        InetAddr::new(self.hostname.clone(), self.rpc_port)
    }

    pub fn new_client_conf(&self) -> ClientConf {
        ClientConf {
            io_threads: self.io_threads,
            worker_threads: self.worker_threads,
            message_size: self.message_size,

            conn_retry_max_duration_ms: self.conn_retry_max_duration_ms,
            conn_retry_min_sleep_ms: self.conn_retry_min_sleep_ms,
            conn_retry_max_sleep_ms: self.conn_retry_max_sleep_ms,

            io_retry_max_duration_ms: self.rpc_retry_max_duration_ms,
            io_retry_min_sleep_ms: self.rpc_retry_min_sleep_ms,
            io_retry_max_sleep_ms: self.rpc_retry_max_sleep_ms,

            close_idle: self.rpc_close_idle,

            conn_timeout_ms: self.conn_timeout_ms,
            rpc_timeout_ms: self.io_timeout_ms,
            data_timeout_ms: self.io_timeout_ms,

            conn_size: self.conn_size,

            use_libc: false,
            ..Default::default()
        }
    }

    pub fn new_raft_conf(&self, id: u64, applied: u64) -> raft::Config {
        raft::Config {
            id,
            election_tick: self.raft_election_tick,
            heartbeat_tick: self.raft_heartbeat_tick,
            min_election_tick: self.raft_min_election_ticks,
            max_election_tick: self.raft_max_election_ticks,
            max_size_per_msg: self.raft_max_size_per_msg,
            max_inflight_msgs: self.raft_max_inflight_msgs,
            applied,
            max_committed_size_per_ready: self.raft_max_committed_size_per_ready,

            check_quorum: self.raft_check_quorum,
            skip_bcast_commit: true,
            pre_vote: true,
            ..Default::default()
        }
    }
}

impl Default for JournalConf {
    fn default() -> Self {
        let journal_dir = Utils::test_sub_dir("journal");
        let journal_addrs = vec![RaftPeer::new(
            1,
            ClusterConf::DEFAULT_HOSTNAME,
            ClusterConf::DEFAULT_RAFT_PORT,
        )];
        Self {
            enable: true,
            group_name: "raft-group".to_string(),
            hostname: ClusterConf::DEFAULT_HOSTNAME.to_string(),
            rpc_port: ClusterConf::DEFAULT_RAFT_PORT,
            io_threads: 8,
            worker_threads: 8,
            message_size: 200,
            journal_addrs,
            journal_dir,
            writer_debug: false,
            writer_channel_size: 0,
            writer_flush_batch_size: 1000,
            writer_flush_batch_ms: 100,
            snapshot_interval: "6h".to_string(),
            snapshot_entries: 100_000,
            snapshot_read_chunk_size: 1024 * 1024,

            conn_retry_max_duration_ms: 0,
            conn_retry_min_sleep_ms: 10 * 1000,
            conn_retry_max_sleep_ms: 10 * 1000,

            rpc_close_idle: false,
            rpc_retry_max_duration_ms: 60 * 1000,
            rpc_retry_min_sleep_ms: 20 * 1000,
            rpc_retry_max_sleep_ms: 20 * 1000,

            conn_timeout_ms: 30 * 1000,
            io_timeout_ms: 60 * 1000,

            conn_size: 1,

            raft_poll_interval_ms: 100,
            raft_tick_interval_ms: 1000,
            raft_election_tick: 10,
            raft_heartbeat_tick: 3,
            raft_min_election_ticks: 10,
            raft_max_election_ticks: 30,
            raft_check_quorum: true,
            raft_max_size_per_msg: 1024 * 1024,
            raft_max_inflight_msgs: 256,
            raft_max_committed_size_per_ready: 16 * 1024 * 1024,

            raft_retry_cache_size: 100_000,
            raft_retry_cache_ttl: "10m".to_string(),

            retain_checkpoint_num: 3,
            ignore_replay_error: false,
        }
    }
}
