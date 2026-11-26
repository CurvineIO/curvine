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

use curvine_client_core::block::BlockClientPool;
use curvine_client_core::file::FsContext;
use curvine_config::ClusterConf;
use curvine_model::WorkerAddress;
use curvine_rpc::test::SimpleServer;
use curvine_runtime::runtime::Runtime;
use once_cell::sync::Lazy;
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
    conf.client.block_conn_idle_time_ms = 30_000;

    let rt = TEST_SERVER.1.clone();
    FsContext::with_rt(conf, rt).unwrap()
}

async fn wait_for_test_server_ready() {
    let worker_addr = get_test_server_addr();
    let context = create_test_context();
    for _ in 0..50 {
        if context.block_client(&worker_addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("Server failed to start within 5 seconds");
}

#[tokio::test]
async fn test_writer_uses_pool() {
    wait_for_test_server_ready().await;
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
    wait_for_test_server_ready().await;
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
    wait_for_test_server_ready().await;
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
    wait_for_test_server_ready().await;
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
    wait_for_test_server_ready().await;
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

/// Dropping a write pipeline's RPC future must not return its raw connection
/// to the pool, even when BlockClient itself is dropped normally afterwards.
#[tokio::test]
async fn test_cancelled_rpc_connection_not_returned_to_pool() {
    use curvine_rpc::handler::{Frame, RpcFrame};
    use curvine_rpc::message::Builder;
    use tokio::net::TcpListener;
    use tokio::time::timeout;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let worker_addr = WorkerAddress {
        worker_id: 2,
        hostname: addr.ip().to_string(),
        ip_addr: addr.ip().to_string(),
        rpc_port: addr.port() as u32,
        web_port: 8000,
    };
    let pool = create_test_pool(true, 30000);
    let context = create_test_context();
    let (client, peer) = tokio::join!(
        pool.acquire_write(&context, &worker_addr),
        listener.accept(),
    );
    let client = client.unwrap();
    let original_local_addr = peer.as_ref().unwrap().1;
    let mut frame = RpcFrame::with_client(peer.unwrap().0, 4096);
    {
        let rpc = client.rpc(Builder::new_rpc(1).req_id(52).seq_id(0).build());
        tokio::pin!(rpc);
        timeout(Duration::from_secs(2), async {
            tokio::select! {
                request = frame.receive() => { assert_eq!(request.unwrap().req_id(), 52); },
                result = &mut rpc => panic!("no reply was sent: {result:?}"),
            }
        })
        .await
        .unwrap();
    }
    assert!(!client.is_active());
    drop(client);
    assert_eq!(
        pool.idle_conn(),
        0,
        "cancelled connection must not return to the pool"
    );

    let (replacement, peer) = timeout(Duration::from_secs(2), async {
        tokio::join!(
            pool.acquire_write(&context, &worker_addr),
            listener.accept(),
        )
    })
    .await
    .expect("next acquisition must open a new TCP connection");
    let replacement = replacement.unwrap();
    let (replacement_peer, replacement_local_addr) = peer.unwrap();
    assert_ne!(replacement_local_addr, original_local_addr);
    assert!(replacement.is_active());
    drop(replacement);
    assert_eq!(
        pool.idle_conn(),
        1,
        "healthy replacement should still be reusable"
    );
    // Keep the peer open until after the pool liveness check; dropping it
    // earlier races FIN delivery against release() and invalidates the test.
    drop(replacement_peer);
}

/// A completed send can leave a response outstanding even when no receive
/// future has been polled. That connection must not be handed to a new reader.
#[tokio::test]
async fn test_sent_read_without_receive_not_returned_to_pool() {
    use curvine_rpc::handler::{Frame, RpcFrame};
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio::time::timeout;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let worker_addr = WorkerAddress {
        worker_id: 3,
        hostname: addr.ip().to_string(),
        ip_addr: addr.ip().to_string(),
        rpc_port: addr.port() as u32,
        web_port: 8000,
    };
    let pool = create_test_pool(true, 30000);
    let context = create_test_context();
    let (client, peer) =
        tokio::join!(pool.acquire_read(&context, &worker_addr), listener.accept(),);
    let client = client.unwrap();
    let (peer, original_local_addr) = peer.unwrap();
    let mut frame = RpcFrame::with_client(peer, 4096);
    timeout(Duration::from_secs(2), async {
        client.send_read(53, 0, None).await.unwrap();
        let request = frame.receive().await.unwrap();
        assert_eq!(request.req_id(), 53);
        assert_eq!(request.seq_id(), 0);
    })
    .await
    .unwrap();
    assert!(client.is_active(), "an outstanding read is still live");
    // Never construct or poll receive_read: a successful send alone must
    // make the connection ineligible for the idle pool.
    drop(client);
    assert_eq!(pool.idle_conn(), 0, "unread reply must prevent pooling");

    let mut byte = [0];
    let closed = timeout(Duration::from_secs(2), frame.io_mut().read(&mut byte))
        .await
        .expect("discarded connection must close at the peer");
    assert!(matches!(closed, Ok(0)) || closed.is_err());

    let (replacement, peer) = timeout(Duration::from_secs(2), async {
        tokio::join!(pool.acquire_read(&context, &worker_addr), listener.accept(),)
    })
    .await
    .expect("next acquisition must open a new TCP connection");
    let replacement = replacement.unwrap();
    let (replacement_peer, replacement_local_addr) = peer.unwrap();
    assert_ne!(replacement_local_addr, original_local_addr);
    drop(replacement);
    assert_eq!(pool.idle_conn(), 1, "idle replacement remains reusable");
    drop(replacement_peer);
}

/// Draining only part of a pipeline is insufficient, while receiving every
/// response must still allow a normal Drop to return the connection to its pool.
#[tokio::test]
async fn test_read_pipeline_pooling_requires_all_responses() {
    use curvine_rpc::handler::{Frame, RpcFrame};
    use curvine_rpc::message::Builder;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio::time::timeout;

    for received_count in [1, 2] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let worker_addr = WorkerAddress {
            worker_id: 4,
            hostname: addr.ip().to_string(),
            ip_addr: addr.ip().to_string(),
            rpc_port: addr.port() as u32,
            web_port: 8000,
        };
        let pool = create_test_pool(true, 30000);
        let context = create_test_context();
        let (client, peer) =
            tokio::join!(pool.acquire_read(&context, &worker_addr), listener.accept(),);
        let client = client.unwrap();
        let mut frame = RpcFrame::with_client(peer.unwrap().0, 4096);
        timeout(Duration::from_secs(2), async {
            for seq_id in 0..2 {
                client.send_read(54, seq_id, None).await.unwrap();
                let request = frame.receive().await.unwrap();
                assert_eq!(request.req_id(), 54);
                assert_eq!(request.seq_id(), seq_id);
                // Queue both replies, even when the client will abandon one.
                frame
                    .send(Builder::success(&request).build())
                    .await
                    .unwrap();
            }
            for seq_id in 0..received_count {
                client.receive_read(54, seq_id).await.unwrap();
                assert!(client.is_active());
                assert_eq!(client.is_reusable(), seq_id == 1);
            }
        })
        .await
        .unwrap();
        drop(client);
        if received_count == 1 {
            assert_eq!(pool.idle_conn(), 0, "one unread response prevents reuse");
            let mut byte = [0];
            let closed = timeout(Duration::from_secs(2), frame.io_mut().read(&mut byte))
                .await
                .expect("partly drained connection must close");
            assert!(matches!(closed, Ok(0)) || closed.is_err());
        } else {
            assert_eq!(pool.idle_conn(), 1, "fully drained pipeline is reusable");
            let reused = timeout(
                Duration::from_secs(2),
                pool.acquire_read(&context, &worker_addr),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(pool.idle_conn(), 0);
            timeout(Duration::from_secs(2), async {
                reused.send_read(55, 0, None).await.unwrap();
                // Same TCP peer proves that acquire reused the original socket.
                let request = frame.receive().await.unwrap();
                assert_eq!(request.req_id(), 55);
                frame
                    .send(Builder::success(&request).build())
                    .await
                    .unwrap();
                reused.receive_read(55, 0).await.unwrap();
            })
            .await
            .unwrap();
            drop(reused);
            assert_eq!(pool.idle_conn(), 1);
        }
    }
}
