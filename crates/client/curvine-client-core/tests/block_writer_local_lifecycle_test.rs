// Copyright 2026 OPPO.
// SPDX-License-Identifier: Apache-2.0

//! Exercise Local writer cancellation through its public API, with real RPC
//! opens and real file writes. One occupied blocking thread queues the operation
//! deterministically, without filesystem stalls, sleeps, or unsafe fault injection.

use curvine_client_core::block::BlockWriterLocal;
use curvine_client_core::file::FsContext;
use curvine_config::ClusterConf;
use curvine_fs_api::RpcCode;
use curvine_io::DataSlice;
use curvine_model::{ExtendedBlock, FileType, StorageType, WorkerAddress};
use curvine_net::net::{ConnState, NetUtils};
use curvine_proto::{BlockWriteRequest, BlockWriteResponse};
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::{Message, RequestStatus};
use curvine_rpc::server::{RpcServer, ServerConf};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use futures::poll;
use prost::Message as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

struct LocalService {
    next_connection: AtomicUsize,
    path: String,
    observed: Arc<Mutex<Vec<(usize, RequestStatus)>>>,
}

impl HandlerService for LocalService {
    type Item = LocalHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        LocalHandler {
            connection: self.next_connection.fetch_add(1, Ordering::SeqCst),
            path: self.path.clone(),
            observed: self.observed.clone(),
        }
    }
}

struct LocalHandler {
    connection: usize,
    path: String,
    observed: Arc<Mutex<Vec<(usize, RequestStatus)>>>,
}

impl MessageHandler for LocalHandler {
    type Error = curvine_core_error::CommonErrorExt;

    fn handle(&self, msg: &Message) -> Result<Message, Self::Error> {
        if RpcCode::from(msg.code()) == RpcCode::WriteBlock {
            let status = msg.request_status();
            self.observed
                .lock()
                .unwrap()
                .push((self.connection, status));
            if status == RequestStatus::Open {
                let request: BlockWriteRequest = msg.parse_header()?;
                let response = BlockWriteResponse {
                    id: request.block.id,
                    path: Some(self.path.clone()),
                    off: request.off,
                    block_size: request.block_size,
                    storage_type: StorageType::Disk.into(),
                    pipeline_status: None,
                };
                return Ok(msg.success_with_data(
                    Some(bytes::BytesMut::from(response.encode_to_vec().as_slice())),
                    DataSlice::empty(),
                ));
            }
        }
        Ok(msg.success_with_data(None, DataSlice::empty()))
    }
}

struct TestEnv {
    context: Arc<FsContext>,
    worker: WorkerAddress,
    observed: Arc<Mutex<Vec<(usize, RequestStatus)>>>,
    path: PathBuf,
}

impl TestEnv {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("curvine-local-write-{}", Utils::uuid()));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let rt = Arc::new(Runtime::new("local-write-lifecycle", 2, 1));
        let port = NetUtils::hold_available_port();
        // Isolate the blocked disk executor from the RPC server executor.
        // Otherwise the gate would also stall the server's message handling.
        let mut server_conf = ServerConf::with_hostname("127.0.0.1", port);
        server_conf.io_threads = 2;
        server_conf.worker_threads = 2;
        let server = RpcServer::new(
            server_conf,
            LocalService {
                next_connection: AtomicUsize::new(0),
                path: path.to_string_lossy().into_owned(),
                observed: observed.clone(),
            },
        );
        std::thread::spawn(move || server.block_on_start());
        let mut conf = ClusterConf::default();
        conf.client.conn_timeout_ms = 500;
        conf.client.conn_retry_max_duration_ms = 2000;
        conf.client.conn_retry_min_sleep_ms = 10;
        conf.client.conn_retry_max_sleep_ms = 50;
        Self {
            context: Arc::new(FsContext::with_rt(conf, rt).unwrap()),
            worker: WorkerAddress {
                worker_id: 1,
                hostname: "127.0.0.1".into(),
                ip_addr: "127.0.0.1".into(),
                rpc_port: u32::from(port),
                web_port: 1,
            },
            observed,
            path,
        }
    }

    async fn writer(&self) -> BlockWriterLocal {
        BlockWriterLocal::new(
            self.context.clone(),
            ExtendedBlock::new(1, 0, StorageType::Disk, FileType::File),
            self.worker.clone(),
            0,
            64,
        )
        .await
        .unwrap()
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Always release the occupied thread on assertion failure as well.
struct BlockingGate(Option<mpsc::Sender<()>>);

impl BlockingGate {
    async fn occupy(rt: &Runtime) -> Self {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        rt.spawn_blocking(move || {
            let _ = started_tx.send(());
            let _ = release_rx.recv_timeout(Duration::from_secs(5));
        });
        started_rx.await.unwrap();
        Self(Some(release_tx))
    }

    async fn drain(mut self, rt: &Runtime) {
        self.0.take().unwrap().send(()).unwrap();
        // A single blocking thread executes the queued write before this fence.
        rt.spawn_blocking(|| {}).await.unwrap();
    }
}

impl Drop for BlockingGate {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

#[test]
fn cancelled_local_write_rejects_reuse() {
    let env = TestEnv::new();
    let rt = env.context.clone_runtime();
    rt.block_on(async {
        let mut writer = env.writer().await;
        let gate = BlockingGate::occupy(&rt).await;
        let mut pending = Box::pin(writer.write(DataSlice::from_str("original")));
        assert!(poll!(&mut pending).is_pending());
        drop(pending);

        let reuse = writer.blocking_write(DataSlice::from_str("replacement"));
        // Drain while the original writer is alive: the red run must not cause UAF.
        gate.drain(&rt).await;
        assert!(reuse.is_err(), "cancelled local writer must reject reuse");
        assert_eq!(
            writer.pos(),
            0,
            "unacknowledged write must not update position"
        );
    });
}

#[test]
fn cancelled_local_write_owns_borrowed_input_and_cannot_commit() {
    let env = TestEnv::new();
    let rt = env.context.clone_runtime();
    rt.block_on(async {
        let mut writer = env.writer().await;
        let gate = BlockingGate::occupy(&rt).await;
        let mut input = b"original".to_vec();
        let mut pending = Box::pin(writer.write(DataSlice::mem_slice(&input)));
        assert!(poll!(&mut pending).is_pending());
        drop(pending);
        // Mutate (rather than free) the caller's buffer before the queued task
        // starts, so a missing ownership copy fails deterministically without UAF.
        input.fill(b'x');
        gate.drain(&rt).await;

        assert_eq!(std::fs::read(&env.path).unwrap(), b"original");
        assert!(writer.complete().await.is_err());
        assert!(writer.cancel().await.is_err());
        assert_eq!(
            env.observed.lock().unwrap().as_slice(),
            &[(0, RequestStatus::Open)]
        );
    });
}

#[test]
fn cancelled_local_flush_seek_and_complete_reject_reuse() {
    for operation in ["flush", "seek", "complete"] {
        let env = TestEnv::new();
        let rt = env.context.clone_runtime();
        rt.block_on(async {
            let mut writer = env.writer().await;
            writer.write(DataSlice::from_str("original")).await.unwrap();
            let gate = BlockingGate::occupy(&rt).await;
            let mut pending = Box::pin(async {
                match operation {
                    "flush" => writer.flush().await,
                    "seek" => writer.seek(3).await,
                    "complete" => writer.complete().await,
                    _ => unreachable!(),
                }
            });
            assert!(poll!(&mut pending).is_pending());
            drop(pending);
            gate.drain(&rt).await;

            assert_eq!(writer.pos(), 8, "{operation}: unacknowledged position");
            assert_eq!(writer.len(), 8);
            assert!(writer.blocking_write(DataSlice::from_str("bad")).is_err());
            assert!(writer.flush().await.is_err());
            assert!(writer.seek(1).await.is_err());
            assert!(writer.complete().await.is_err());
            assert!(writer.cancel().await.is_err());
            assert_eq!(
                env.observed.lock().unwrap().as_slice(),
                &[(0, RequestStatus::Open)]
            );
        });
    }
}

#[test]
fn dropped_cancelled_local_writer_does_not_lend_its_connection() {
    let env = TestEnv::new();
    let rt = env.context.clone_runtime();
    rt.block_on(async {
        let mut writer = env.writer().await;
        let gate = BlockingGate::occupy(&rt).await;
        let mut pending = Box::pin(writer.write(DataSlice::from_str("original")));
        assert!(poll!(&mut pending).is_pending());
        drop(pending);
        drop(writer);

        // The old blocking task still owns its file and RPC session. Acquire
        // the next writer's RPC session without queueing another local file
        // open on the intentionally occupied disk executor.
        let next_client = env.context.acquire_write(&env.worker).await.unwrap();
        let next_block = ExtendedBlock::new(2, 0, StorageType::Disk, FileType::File);
        let req_id = Utils::req_id();
        next_client
            .write_block(&next_block, 0, 64, req_id, 0, 64, true, Vec::new())
            .await
            .unwrap();
        next_client
            .write_commit(&next_block, 0, 64, req_id, 1, true)
            .await
            .unwrap();
        gate.drain(&rt).await;
        let observed = env.observed.lock().unwrap();
        let connections: Vec<_> = observed
            .iter()
            .filter(|(_, status)| *status == RequestStatus::Open)
            .map(|(connection, _)| *connection)
            .collect();
        assert_eq!(connections.len(), 2);
        assert_ne!(connections[0], connections[1]);
        assert_eq!(std::fs::read(&env.path).unwrap(), b"original");
    });
}

#[test]
fn local_writer_updates_position_and_commits_after_successful_operations() {
    let env = TestEnv::new();
    let rt = env.context.clone_runtime();
    rt.block_on(async {
        let mut writer = env.writer().await;
        writer.write(DataSlice::from_str("original")).await.unwrap();
        writer.flush().await.unwrap();
        assert_eq!((writer.pos(), writer.len(), writer.remaining()), (8, 8, 56));
        writer.seek(2).await.unwrap();
        assert_eq!((writer.pos(), writer.len()), (2, 8));
        writer.blocking_write(DataSlice::from_str("XX")).unwrap();
        assert_eq!((writer.pos(), writer.len()), (4, 8));
        writer.complete().await.unwrap();
        assert_eq!(std::fs::read(&env.path).unwrap(), b"orXXinal");
        drop(writer);

        // A successfully completed session remains poolable.
        let mut next_writer = env.writer().await;
        next_writer.cancel().await.unwrap();
        let observed = env.observed.lock().unwrap();
        let connections: Vec<_> = observed
            .iter()
            .filter(|(_, status)| *status == RequestStatus::Open)
            .map(|(connection, _)| *connection)
            .collect();
        assert_eq!(connections, [0, 0]);
        assert!(observed
            .iter()
            .any(|(_, status)| *status == RequestStatus::Complete));
    });
}

#[test]
fn cancelled_local_open_does_not_return_an_unfinished_session() {
    let env = TestEnv::new();
    let rt = env.context.clone_runtime();
    rt.block_on(async {
        let gate = BlockingGate::occupy(&rt).await;
        let result = tokio::time::timeout(Duration::from_millis(100), env.writer()).await;
        let cancelled = result.is_err();
        drop(result);
        gate.drain(&rt).await;
        assert!(
            cancelled,
            "local file open must run in the blocked disk executor"
        );
        assert_eq!(
            env.observed.lock().unwrap().as_slice(),
            &[(0, RequestStatus::Open)]
        );

        let mut next_writer = env.writer().await;
        next_writer.cancel().await.unwrap();
        let observed = env.observed.lock().unwrap();
        let connections: Vec<_> = observed
            .iter()
            .filter(|(_, status)| *status == RequestStatus::Open)
            .map(|(connection, _)| *connection)
            .collect();
        assert_eq!(
            connections,
            [0, 1],
            "cancelled Open must not return to the pool"
        );
    });
}

#[test]
fn local_open_failure_preserves_committed_replicas() {
    for (len, pos) in [(8, 0), (0, 8)] {
        let env = TestEnv::new();
        std::fs::create_dir(&env.path).unwrap();
        let rt = env.context.clone_runtime();
        let result = rt.block_on(BlockWriterLocal::new(
            env.context.clone(),
            ExtendedBlock::new(1, len, StorageType::Disk, FileType::File),
            env.worker.clone(),
            pos,
            64,
        ));
        std::fs::remove_dir(&env.path).unwrap();
        assert!(result.is_err());
        assert_eq!(
            env.observed.lock().unwrap().as_slice(),
            &[(0, RequestStatus::Open)],
            "a failed local reopen must not send Cancel for committed state",
        );
    }
}

#[test]
fn fresh_local_open_failure_cancels_worker_once() {
    let env = TestEnv::new();
    std::fs::create_dir(&env.path).unwrap();
    let rt = env.context.clone_runtime();
    let result = rt.block_on(BlockWriterLocal::new(
        env.context.clone(),
        ExtendedBlock::new(1, 0, StorageType::Disk, FileType::File),
        env.worker.clone(),
        0,
        64,
    ));
    std::fs::remove_dir(&env.path).unwrap();
    assert!(result.is_err());
    assert_eq!(
        env.observed.lock().unwrap().as_slice(),
        &[(0, RequestStatus::Open), (0, RequestStatus::Cancel)]
    );
}
