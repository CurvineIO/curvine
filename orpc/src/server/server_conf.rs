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

use crate::io::net::{InetAddr, NetUtils};
use crate::runtime::Runtime;
use crate::sys::pipe::PipePool;
use std::sync::Arc;
use std::thread;

#[derive(Debug, Clone)]
pub struct ServerConf {
    pub name: String,

    pub io_threads: usize,
    pub worker_threads: usize,

    pub hostname: String,
    pub port: u16,
    pub buffer_size: usize,

    pub timout_ms: u64,
    pub keepalive: bool,

    pub close_idle: bool,

    pub enable_read_ahead: bool,
    pub read_ahead_len: u64,

    pub enable_splice: bool,
    // Pipe size
    pub pipe_buf_size: usize,
    // Initial number of pipeline resource pools
    pub pipe_pool_init_cap: usize,
    // Maximum number of pipeline resource pools
    pub pipe_pool_max_cap: usize,
    // In the pipeline resource pool, the pipeline idle recycling time.
    pub pipe_pool_idle_time: usize,

    pub enable_send_file: bool,

    pub use_ucp: bool,
    pub ucp_tag_max_len: usize,
    pub ucp_small_use_tag: bool,
}

impl ServerConf {
    pub fn with_hostname(hostname: impl Into<String>, port: u16) -> Self {
        let cpus = thread::available_parallelism().unwrap().get();
        ServerConf {
            name: "rpc-server".to_string(),
            io_threads: 32,
            worker_threads: cpus * 2,
            hostname: hostname.into(),
            port,
            buffer_size: 128 * 1024,
            timout_ms: 120_000,
            keepalive: true,
            close_idle: true,

            enable_read_ahead: true,
            read_ahead_len: 4 * 1024 * 1024,

            enable_splice: false,
            pipe_buf_size: 64 * 1024,
            pipe_pool_init_cap: 0,
            pipe_pool_max_cap: 2000,
            pipe_pool_idle_time: 0,

            enable_send_file: true,

            use_ucp: true,
            ucp_tag_max_len: 4096,
            ucp_small_use_tag: false,
        }
    }

    pub fn with_port(port: u16) -> Self {
        let hostname = NetUtils::get_hostname().unwrap_or("0.0.0.0".to_string());
        ServerConf::with_hostname(hostname, port)
    }

    pub fn create_runtime(&self) -> Runtime {
        Runtime::new(&self.name, self.io_threads, self.worker_threads)
    }

    pub fn bind_addr(&self) -> InetAddr {
        InetAddr::new(&self.hostname, self.port)
    }

    pub fn create_pipe_pool(&self) -> Option<Arc<PipePool>> {
        if self.enable_splice {
            let pool = PipePool::new(
                self.pipe_pool_init_cap,
                self.pipe_pool_max_cap,
                self.pipe_buf_size,
            );
            Some(Arc::new(pool))
        } else {
            None
        }
    }
}

impl Default for ServerConf {
    fn default() -> Self {
        let hostname = NetUtils::local_hostname();
        let port = NetUtils::get_available_port();
        Self::with_hostname(hostname, port)
    }
}
