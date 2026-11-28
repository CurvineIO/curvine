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

//! Exercise the production RpcServer -> StreamHandler -> WorkerHandler ownership
//! chain with real TCP and BlockStore leases. These tests use buffered receive;
//! Linux splice payload I/O and SPDK device teardown require separate validation.

use crate::worker::block::BlockStore;
use crate::worker::{Worker, WorkerService};
use bytes::BytesMut;
use curvine_config::{ClusterConf, WorkerConf};
use curvine_fs_api::RpcCode;
use curvine_io::DataSlice;
use curvine_model::{ExtendedBlock, FileType, ProtoUtils, StorageType};
use curvine_net::net::NetUtils;
use curvine_proto::{BlockWriteRequest, BlocksBatchWriteRequest};
use curvine_rpc::handler::{Frame, HandlerService, RpcFrame};
use curvine_rpc::message::{Builder, Message, RequestStatus, PROTOCOL_USIZE};
use curvine_rpc::server::{RpcServer, ServerConf};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{AsyncRuntime, JoinHandle, RpcRuntime};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

const DEADLINE: Duration = Duration::from_millis(100);
const WATCHDOG: Duration = Duration::from_secs(3);

// Abort the accept loop even when a test assertion panics. Connection tasks
// terminate independently when their peers drop or their receive deadline fires.
struct ServerTask(JoinHandle<()>);

impl Drop for ServerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn with_worker_server<F: Future<Output = ()>>(
    name: &str,
    close_idle: bool,
    test: impl FnOnce(u16, BlockStore) -> F,
) {
    let conf = ClusterConf {
        format_worker: true,
        worker: WorkerConf {
            io_threads: 1,
            worker_threads: 1,
            scheduler_threads: 1,
            executor_threads: 1,
            dir_reserved: "0".to_string(),
            data_dir: vec![format!(
                "[MEM:64KB]../testing/worker-connection-{name}-{}",
                Utils::uuid()
            )],
            ..WorkerConf::default()
        },
        ..ClusterConf::default()
    };
    // Initialize Worker metrics/config explicitly, independent of test order.
    // Constructing Worker does not start its RPC/web listeners or heartbeat.
    let _worker = Worker::with_conf(conf.clone()).unwrap();
    let rt = Arc::new(AsyncRuntime::new("worker-connection-test", 1, 1));
    let service = WorkerService::with_conf(&conf, rt.clone(), "test-session".to_string()).unwrap();
    let store = service.get_message_handler(None).store.clone();
    let port = NetUtils::hold_available_port();
    let server = RpcServer::with_rt(
        rt.clone(),
        ServerConf {
            timeout_ms: DEADLINE.as_millis() as u64,
            close_idle,
            enable_splice: false,
            ..ServerConf::with_hostname("127.0.0.1", port)
        },
        service,
    );
    let mut listener = server.new_state_listener();
    rt.block_on(async {
        let mut task = ServerTask(rt.spawn(async move { server.run().await.unwrap() }));
        timeout(WATCHDOG, listener.wait_running())
            .await
            .expect("RpcServer must bind")
            .unwrap();
        test(port, store).await;
        task.0.abort();
        let result = timeout(WATCHDOG, &mut task.0).await.unwrap();
        assert!(result.unwrap_err().is_cancelled());
    });
}

fn block() -> ExtendedBlock {
    ExtendedBlock::new(1964, 0, StorageType::Mem, FileType::File)
}

fn write_frame(status: RequestStatus, seq_id: i32, data: DataSlice) -> Message {
    Builder::new()
        .code(RpcCode::WriteBlock)
        .request(status)
        .req_id(1964)
        .seq_id(seq_id)
        .proto_header(BlockWriteRequest {
            block: ProtoUtils::extend_block_to_pb(block()),
            off: 0,
            block_size: 1024,
            short_circuit: false,
            client_name: "connection-lifecycle-test".to_string(),
            chunk_size: 1024,
            pipeline_stream: Vec::new(),
            component_info: None,
        })
        .data(data)
        .build()
}

async fn exchange(peer: &mut RpcFrame, request: Message) {
    let req_id = request.req_id();
    let seq_id = request.seq_id();
    timeout(WATCHDOG, peer.send(request))
        .await
        .unwrap()
        .unwrap();
    let reply = timeout(WATCHDOG, peer.receive()).await.unwrap().unwrap();
    assert!(!reply.is_empty(), "connection must still be usable");
    assert_eq!(reply.req_id(), req_id);
    assert_eq!(reply.seq_id(), seq_id);
    reply.check_error_ext::<curvine_error::FsError>().unwrap();
}

async fn open(port: u16, store: &BlockStore) -> RpcFrame {
    let stream = timeout(WATCHDOG, TcpStream::connect(("127.0.0.1", port)))
        .await
        .unwrap()
        .unwrap();
    stream.set_nodelay(true).unwrap();
    let mut peer = RpcFrame::with_client(stream, 4096);
    exchange(
        &mut peer,
        write_frame(RequestStatus::Open, 1, DataSlice::Empty),
    )
    .await;
    assert_writer_active(store);
    assert_pending(store);
    peer
}

fn assert_writer_active(store: &BlockStore) {
    assert_writer_active_for(store, block().id);
}

fn assert_writer_active_for(store: &BlockStore, id: i64) {
    let result = store.replication_write_lease(id, "probe");
    assert!(
        matches!(result, Err(ref error) if error.to_string().contains("active writer")),
        "the successful Open must hold an ordinary write lease"
    );
}

fn assert_pending(store: &BlockStore) {
    assert_pending_for(store, block().id);
}

fn assert_pending_for(store: &BlockStore, id: i64) {
    let meta = store
        .get_raw_block_opt(id)
        .unwrap()
        .expect("disconnect must preserve the pending block, not silently abort/delete it");
    assert!(
        !meta.is_final(),
        "disconnect must not silently commit a block"
    );
    let writer = store.open_writer(&meta, 0).unwrap();
    assert_eq!(
        writer.device_len(),
        0,
        "an incomplete frame must not write data"
    );
}

async fn assert_lease_released(store: &BlockStore) {
    assert_lease_released_for(store, block().id).await;
}

async fn assert_lease_released_for(store: &BlockStore, id: i64) {
    timeout(WATCHDOG, async {
        loop {
            match store.replication_write_lease(id, "probe") {
                Ok((_scoped, lease)) => {
                    drop(lease);
                    break;
                }
                Err(error) => {
                    assert!(error.to_string().contains("active writer"), "{error}");
                    sleep(Duration::from_millis(5)).await;
                }
            }
        }
    })
    .await
    .expect("the connection task must drop WorkerHandler and release its lease");
}

async fn assert_eof(peer: &mut RpcFrame) {
    let mut byte = [0];
    let len = timeout(WATCHDOG, peer.io_mut().read(&mut byte))
        .await
        .expect("the half-frame deadline must close the TCP connection")
        .unwrap();
    assert_eq!(len, 0, "the server must close without an unsolicited reply");
}

async fn assert_reconnect_works(port: u16, store: &BlockStore) {
    // Re-open the same pending block, proving the lease and admission path are
    // usable after teardown, rather than only observing a detached drop spy.
    let mut peer = open(port, store).await;
    exchange(
        &mut peer,
        write_frame(RequestStatus::Cancel, 2, DataSlice::Empty),
    )
    .await;
    assert_lease_released(store).await;
    assert!(store.get_raw_block_opt(block().id).unwrap().is_none());
}

fn half_frame_releases_lease(name: &str, prefix: usize, close_idle: bool) {
    with_worker_server(name, close_idle, |port, store| async move {
        let mut peer = open(port, &store).await;
        let mut wire = BytesMut::new();
        write_frame(
            RequestStatus::Running,
            2,
            DataSlice::Buffer(BytesMut::zeroed(16)),
        )
        .encode(&mut wire)
        .unwrap();
        assert!(prefix > 0 && prefix < wire.len());
        peer.io_mut().write_all(&wire[..prefix]).await.unwrap();
        assert_eof(&mut peer).await;
        assert_lease_released(&store).await;
        assert_pending(&store);
        assert_reconnect_works(port, &store).await;
    });
}

#[test]
fn half_protocol_timeout_releases_worker_write_lease() {
    half_frame_releases_lease("half-protocol", 1, false);
}

#[test]
fn full_protocol_missing_header_timeout_releases_worker_write_lease() {
    half_frame_releases_lease("missing-header", PROTOCOL_USIZE, false);
}

#[test]
fn half_header_timeout_releases_worker_write_lease() {
    half_frame_releases_lease("half-header", PROTOCOL_USIZE + 1, false);
}

#[test]
fn half_buffered_payload_timeout_releases_worker_write_lease() {
    let request = write_frame(
        RequestStatus::Running,
        2,
        DataSlice::Buffer(BytesMut::zeroed(16)),
    );
    let mut wire = BytesMut::new();
    request.encode(&mut wire).unwrap();
    half_frame_releases_lease("half-payload", wire.len() - 15, false);
}

#[test]
fn half_frame_with_close_idle_also_releases_worker_write_lease() {
    half_frame_releases_lease("half-frame-close-idle", 1, true);
}

#[test]
fn peer_eof_releases_worker_write_lease_without_committing() {
    with_worker_server("peer-eof", false, |port, store| async move {
        let peer = open(port, &store).await;
        drop(peer);
        assert_lease_released(&store).await;
        assert_pending(&store);
        assert_reconnect_works(port, &store).await;
    });
}

#[test]
fn close_idle_releases_worker_write_lease_without_committing() {
    with_worker_server("close-idle", true, |port, store| async move {
        let mut peer = open(port, &store).await;
        assert_eof(&mut peer).await;
        assert_lease_released(&store).await;
        assert_pending(&store);
        assert_reconnect_works(port, &store).await;
    });
}

#[test]
fn idle_connection_keeps_lease_until_successful_cancel() {
    with_worker_server("idle-cancel", false, |port, store| async move {
        let mut peer = open(port, &store).await;
        sleep(DEADLINE * 3).await;
        assert_writer_active(&store);
        assert_pending(&store);
        exchange(
            &mut peer,
            write_frame(RequestStatus::Cancel, 2, DataSlice::Empty),
        )
        .await;
        assert_lease_released(&store).await;
        assert!(store.get_raw_block_opt(block().id).unwrap().is_none());
    });
}

#[test]
fn successful_complete_releases_worker_write_lease_while_connection_stays_open() {
    with_worker_server("complete", false, |port, store| async move {
        let mut peer = open(port, &store).await;
        exchange(
            &mut peer,
            write_frame(RequestStatus::Complete, 2, DataSlice::Empty),
        )
        .await;
        assert_lease_released(&store).await;
        assert!(store
            .get_raw_block_opt(block().id)
            .unwrap()
            .unwrap()
            .is_final());
        // A new Open on the same TCP connection must create a fresh handler.
        exchange(
            &mut peer,
            write_frame(RequestStatus::Open, 3, DataSlice::Empty),
        )
        .await;
        assert_writer_active(&store);
        exchange(
            &mut peer,
            write_frame(RequestStatus::Cancel, 4, DataSlice::Empty),
        )
        .await;
        assert_lease_released(&store).await;
    });
}

#[test]
fn half_frame_timeout_releases_all_batch_worker_write_leases() {
    with_worker_server("batch-half-frame", false, |port, store| async move {
        let stream = timeout(WATCHDOG, TcpStream::connect(("127.0.0.1", port)))
            .await
            .unwrap()
            .unwrap();
        let mut peer = RpcFrame::with_client(stream, 4096);
        let blocks: Vec<_> = (1964..1967)
            .map(|id| ExtendedBlock::new(id, 0, StorageType::Mem, FileType::File))
            .collect();
        let request = Builder::new()
            .code(RpcCode::WriteBlocksBatch)
            .request(RequestStatus::Open)
            .req_id(1964)
            .seq_id(1)
            .proto_header(BlocksBatchWriteRequest {
                blocks: blocks
                    .iter()
                    .cloned()
                    .map(ProtoUtils::extend_block_to_pb)
                    .collect(),
                off: 0,
                block_size: 1024,
                req_id: 1964,
                seq_id: 1,
                chunk_size: 1024,
                short_circuit: false,
                client_name: "connection-lifecycle-test".to_string(),
                component_info: None,
            })
            .build();
        exchange(&mut peer, request).await;
        for block in &blocks {
            assert_writer_active_for(&store, block.id);
            assert_pending_for(&store, block.id);
        }
        let mut wire = BytesMut::new();
        write_frame(RequestStatus::Running, 2, DataSlice::Empty)
            .encode(&mut wire)
            .unwrap();
        peer.io_mut().write_all(&wire[..1]).await.unwrap();
        assert_eof(&mut peer).await;
        for block in &blocks {
            assert_lease_released_for(&store, block.id).await;
            assert_pending_for(&store, block.id);
        }
        assert_reconnect_works(port, &store).await;
    });
}
