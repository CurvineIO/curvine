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

use crate::block::BlockClient;
use crate::file::FsContext;
use curvine_common::state::{ExtendedBlock, WorkerAddress};
use log::{debug, info, warn};
use orpc::common::{LocalTime, TimeSpent, Utils};
use orpc::io::IOResult;
use orpc::sync::{AtomicLen, FastDashMap};
use std::collections::VecDeque;
use std::sync::Arc;

/// Block client connection pool for reusing connections to block storage workers.
///
/// ## Design Principles
///
/// **The pool does not limit the total number of connections**, but reduces connection overhead
/// by reusing connections:
/// - **Small file I/O**: Reuses idle connections from the pool when possible
/// - **Large file I/O**: Creates independent connections that are not pooled (file size >= `small_file_size`)
///
/// **The pool limits the number of idle connections** (default 128):
/// - When the number of idle connections reaches `max_idle_size`, newly returned connections
///   are directly released instead of being added to the pool
/// - This prevents the pool from consuming excessive resources while ensuring active connections
///   are not limited
///
/// ## How It Works
///
/// - Connections are organized by worker address, with each worker maintaining a queue of idle
///   connections using LIFO (Last In First Out) strategy
/// - Automatically cleans up idle connections that exceed `idle_time_ms`
/// - Thread-safe, using `FastDashMap` for concurrent access
///
pub struct BlockClientPool {
    enable: bool,
    pool: FastDashMap<WorkerAddress, VecDeque<BlockClient>>,
    id: u64,
    cur_idle_size: AtomicLen,
    max_idle_size: usize,
    small_file_size: i64,
    idle_time_ms: u64,
}

impl BlockClientPool {
    pub fn new(
        enable: bool,
        max_idle_size: usize,
        small_file_size: i64,
        idle_time_ms: u64,
    ) -> Self {
        let pool = FastDashMap::with_capacity(max_idle_size);
        let pool_instance = Self {
            enable,
            pool,
            id: Utils::unique_id(),
            cur_idle_size: AtomicLen::new(0),
            max_idle_size,
            small_file_size,
            idle_time_ms,
        };

        if enable {
            info!(
                "block client pool created: enable={}, max_idle_size={}, small_file_size={}B, idle_time_ms={}ms",
                pool_instance.enable,
                pool_instance.max_idle_size,
                pool_instance.small_file_size,
                pool_instance.idle_time_ms
            );
        }

        pool_instance
    }

    pub async fn acquire_write(
        self: &Arc<Self>,
        context: &FsContext,
        addr: &WorkerAddress,
        block: &ExtendedBlock,
    ) -> IOResult<BlockClient> {
        if !self.enable {
            return context.block_client(addr).await;
        }

        if let Some(opts) = &block.alloc_opts {
            if opts.len > self.small_file_size {
                return context.block_client(addr).await;
            }
        }

        self.acquire(context, addr).await
    }

    pub async fn acquire_read(
        self: &Arc<Self>,
        context: &FsContext,
        addr: &WorkerAddress,
        block: &ExtendedBlock,
    ) -> IOResult<BlockClient> {
        if !self.enable {
            return context.block_client(addr).await;
        }

        if block.len > self.small_file_size {
            return context.block_client(addr).await;
        }

        self.acquire(context, addr).await
    }

    async fn acquire(
        self: &Arc<Self>,
        context: &FsContext,
        addr: &WorkerAddress,
    ) -> IOResult<BlockClient> {
        let current_time = LocalTime::mills();

        let mut pool = self.pool.entry(addr.clone()).or_default();

        while let Some(mut client) = pool.pop_back() {
            let idle_duration = current_time.saturating_sub(client.uptime());
            self.cur_idle_size.decr();

            if idle_duration < self.idle_time_ms {
                debug!("acquiring connection for worker {} from pool", addr);
                return Ok(client);
            } else {
                debug!(
                    "connection for worker {} expired (idle={}ms), removing from pool",
                    addr, idle_duration
                );
                client.clear_pool();
            }
        }
        drop(pool);

        let mut client = context.block_client(addr).await?;
        client.set_pool(self.clone());

        Ok(client)
    }

    pub fn release(self: &Arc<Self>, mut client: BlockClient) {
        if !self.enable {
            return;
        }

        match client.pool() {
            Some(pool) if pool.id == self.id => {
                let addr = client.worker_addr().clone();

                if self.cur_idle_size.get() >= self.max_idle_size {
                    warn!(
                        "pool full(max={}), closing connection for worker {}",
                        self.max_idle_size,
                        client.worker_addr()
                    );
                    client.clear_pool();
                } else {
                    client.set_uptime();
                    let mut queue = self.pool.entry(addr.clone()).or_default();
                    queue.push_back(client);

                    self.cur_idle_size.incr();
                }
            }

            _ => {
                warn!(
                    "connection for worker {} does not belong to this pool, closing",
                    client.worker_addr()
                );
                // Clear pool reference to avoid infinite recursion in Drop
                client.clear_pool();
            }
        }
    }

    pub fn idle_conn(&self) -> usize {
        self.cur_idle_size.get()
    }

    pub fn clear_idle_conn(self: &Arc<Self>) {
        if !self.enable {
            return;
        }

        let current_time = LocalTime::mills();
        let spend = TimeSpent::new();
        let mut total_cleared = 0;
        let mut idle_count = 0;

        self.pool.retain(|_addr, queue| {
            let before_len = queue.len();

            let mut valid_clients = VecDeque::with_capacity(queue.len());
            while let Some(mut client) = queue.pop_front() {
                let idle_duration = current_time.saturating_sub(client.uptime());
                if idle_duration < self.idle_time_ms {
                    valid_clients.push_back(client);
                } else {
                    client.clear_pool();
                }
            }

            *queue = valid_clients;
            total_cleared += before_len - queue.len();
            idle_count += queue.len();

            !queue.is_empty()
        });

        if total_cleared > 0 {
            info!(
                "cleared {} idle connections (idle_time={}ms), cost {} ms",
                total_cleared,
                self.idle_time_ms,
                spend.used_ms()
            );
        }

        self.cur_idle_size.set(idle_count);
        FsContext::get_metrics()
            .block_idle_conn
            .set(idle_count as i64);
    }
}
