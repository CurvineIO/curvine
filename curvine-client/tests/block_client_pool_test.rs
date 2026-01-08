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

use curvine_client::block::BlockClientPool;
use curvine_client::file::FsContext;
use curvine_common::conf::ClusterConf;
use curvine_common::state::WorkerAddress;
use once_cell::sync::Lazy;
use orpc::runtime::Runtime;
use orpc::test::SimpleServer;
use std::sync::Arc;
use std::time::Duration;

// Global test server address, shared across all tests
// Server is started once and kept running in background thread
static TEST_SERVER: Lazy<(WorkerAddress, Arc<Runtime>)> = Lazy::new(|| {
    let server = SimpleServer::default(); // Uses random port
    let bind_addr = server.bind_addr();
    let worker_addr = WorkerAddress {
        worker_id: 1,
        hostname: bind_addr.hostname.clone(),
        ip_addr: bind_addr.hostname.clone(),
        rpc_port: bind_addr.port as u32,
        web_port: 8000,
    };
    let rt = server.new_rt();
    server.start(0); // Start server immediately (consumes server)
                     // Wait for server to start
    std::thread::sleep(Duration::from_millis(100));
    (worker_addr, rt)
});

fn create_test_pool(enable: bool, idle_time_ms: u64) -> Arc<BlockClientPool> {
    Arc::new(BlockClientPool::new(enable, 10, idle_time_ms))
}

fn get_test_server_addr() -> WorkerAddress {
    TEST_SERVER.0.clone()
}

fn create_test_context() -> FsContext {
    let mut conf = ClusterConf::default();
    conf.client.enable_block_conn_pool = true;
    conf.client.block_conn_idle_size = 10;
    conf.client.block_conn_idle_time = Duration::from_secs(30);

    let rt = TEST_SERVER.1.clone();
    FsContext::with_rt(conf, rt).unwrap()
}

#[tokio::test]
async fn test_writer_uses_pool() {
    let worker_addr = get_test_server_addr();
    let pool = create_test_pool(true, 30000);
    let context = Arc::new(create_test_context());

    // First acquire should create a new connection
    let client1 = pool.acquire_write(&context, &worker_addr).await;
    assert!(client1.is_ok());

    // Connection should have pool set
    let client1 = client1.unwrap();
    assert!(client1.pool().is_some());

    // Release connection back to pool
    pool.release(client1);

    // Second acquire should get connection from pool
    let client2 = pool.acquire_write(&context, &worker_addr).await;
    assert!(client2.is_ok());

    // Verify connection count (connection taken from pool, idle count is 0)
    assert_eq!(pool.idle_conn(), 0);
}

#[tokio::test]
async fn test_reader_uses_pool() {
    let worker_addr = get_test_server_addr();
    let pool = create_test_pool(true, 30000);
    let context = Arc::new(create_test_context());

    // First acquire should create a new connection
    let client1 = pool.acquire_read(&context, &worker_addr).await;
    assert!(client1.is_ok());

    // Connection should have pool set
    let client1 = client1.unwrap();
    assert!(client1.pool().is_some());

    // Release connection back to pool
    pool.release(client1);

    // Second acquire should get connection from pool
    let client2 = pool.acquire_read(&context, &worker_addr).await;
    assert!(client2.is_ok());

    // Verify connection count (connection taken from pool, idle count is 0)
    assert_eq!(pool.idle_conn(), 0);
}

#[tokio::test]
async fn test_expired_connection_not_acquired() {
    let worker_addr = get_test_server_addr();
    let pool = create_test_pool(true, 100); // 100ms idle time
    let context = Arc::new(create_test_context());

    // First acquire connection
    let client1 = pool.acquire_read(&context, &worker_addr).await.unwrap();

    // Release connection
    pool.release(client1);

    assert_eq!(pool.idle_conn(), 1);

    // Wait for connection to expire
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Acquire again, should create new connection (old connection expired)
    let client2 = pool.acquire_read(&context, &worker_addr).await;
    assert!(client2.is_ok());

    // Verify expired connection has been cleaned up
    assert_eq!(pool.idle_conn(), 0);
}

#[tokio::test]
async fn test_clear_idle_connections() {
    let worker_addr = get_test_server_addr();
    let pool = create_test_pool(true, 100); // 100ms idle time
    let context = Arc::new(create_test_context());

    // Create and release multiple connections
    let mut clients = vec![];
    for _ in 0..3 {
        let client = pool.acquire_read(&context, &worker_addr).await.unwrap();
        clients.push(client);
    }
    assert_eq!(pool.idle_conn(), 0);

    // Release all clients explicitly to avoid blocking in Drop
    for client in clients {
        drop(client);
    }

    assert_eq!(pool.idle_conn(), 3);

    // Wait for connections to expire
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Clear expired connections
    pool.clear_idle_conn();

    // Verify expired connections have been cleaned up
    assert_eq!(pool.idle_conn(), 0);
}

#[tokio::test]
async fn test_pool_acquire_release_count() {
    let worker_addr = get_test_server_addr();
    let pool = create_test_pool(true, 30000);
    let context = Arc::new(create_test_context());

    // Initial state
    assert_eq!(pool.idle_conn(), 0);

    // Acquire connection
    let client1 = pool.acquire_read(&context, &worker_addr).await.unwrap();
    assert_eq!(pool.idle_conn(), 0);

    // Release connection
    pool.release(client1);
    assert_eq!(pool.idle_conn(), 1);

    // Acquire again (from pool)
    let client2 = pool.acquire_read(&context, &worker_addr).await.unwrap();
    assert_eq!(pool.idle_conn(), 0);

    // Release again
    pool.release(client2);
    assert_eq!(pool.idle_conn(), 1);

    // Acquire multiple connections
    let client3 = pool.acquire_read(&context, &worker_addr).await.unwrap();
    let client4 = pool.acquire_read(&context, &worker_addr).await.unwrap();
    assert_eq!(pool.idle_conn(), 0);

    // Release multiple connections
    pool.release(client3);
    pool.release(client4);
    assert_eq!(pool.idle_conn(), 2);
}
