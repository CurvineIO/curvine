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

//! Replica aggregation exercised through BlockWriter and independent real RPC servers.

use curvine_client_core::block::BlockWriter;
use curvine_client_core::file::FsContext;
use curvine_config::ClusterConf;
use curvine_fs_api::RpcCode;
use curvine_io::DataSlice;
use curvine_model::{ExtendedBlock, FileType, LocatedBlock, StorageType, WorkerAddress};
use curvine_net::net::ConnState;
use curvine_proto::{BlockWriteRequest, BlockWriteResponse, DataHeaderProto};
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::{Message, RequestStatus};
use curvine_rpc::server::{RpcServer, ServerConf};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use prost::Message as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Open,
    Write,
    Flush,
    Complete,
    Cancel,
}

#[derive(Clone, Copy)]
enum Fault {
    Error,
    Delay(Duration),
}

#[derive(Clone, Default)]
struct Script {
    fault: Option<(Stage, Fault)>,
    observed: Arc<Mutex<Vec<Stage>>>,
    acked: Arc<Mutex<Vec<Stage>>>,
    data: Arc<Mutex<Vec<u8>>>,
    path: Option<PathBuf>,
    next_connection: Arc<AtomicUsize>,
    connections: Arc<Mutex<Vec<(Stage, usize)>>>,
}

impl Script {
    fn fail(stage: Stage) -> Self {
        Self {
            fault: Some((stage, Fault::Error)),
            ..Self::default()
        }
    }
    fn delay(stage: Stage, millis: u64) -> Self {
        Self {
            fault: Some((stage, Fault::Delay(Duration::from_millis(millis)))),
            ..Self::default()
        }
    }
    fn count(&self, stage: Stage) -> usize {
        self.observed
            .lock()
            .unwrap()
            .iter()
            .filter(|s| **s == stage)
            .count()
    }
}

struct ReplicaService(Script);
impl HandlerService for ReplicaService {
    type Item = ReplicaHandler;
    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        ReplicaHandler {
            script: self.0.clone(),
            connection: self.0.next_connection.fetch_add(1, Ordering::SeqCst),
            pos: Mutex::new(0),
        }
    }
}
struct ReplicaHandler {
    script: Script,
    connection: usize,
    pos: Mutex<usize>,
}
impl MessageHandler for ReplicaHandler {
    type Error = curvine_core_error::CommonErrorExt;
    fn is_sync(&self, _: &Message) -> bool {
        false
    }
    fn handle(&self, _: &Message) -> Result<Message, Self::Error> {
        unreachable!()
    }
    async fn async_handle(&self, msg: Message) -> Result<Message, Self::Error> {
        if RpcCode::from(msg.code()) != RpcCode::WriteBlock {
            return Ok(msg.success_with_data(None, DataSlice::empty()));
        }
        let stage = match msg.request_status() {
            RequestStatus::Open => Stage::Open,
            RequestStatus::Complete => Stage::Complete,
            RequestStatus::Cancel => Stage::Cancel,
            RequestStatus::Running if msg.data_len() == 0 => Stage::Flush,
            RequestStatus::Running => Stage::Write,
            status => panic!("unexpected status {status:?}"),
        };
        self.script.observed.lock().unwrap().push(stage);
        self.script
            .connections
            .lock()
            .unwrap()
            .push((stage, self.connection));
        if let Some((fault_stage, fault)) = self.script.fault {
            if fault_stage == stage {
                match fault {
                    Fault::Error => {
                        return curvine_core_error::err_box!("injected {:?} failure", stage)
                    }
                    Fault::Delay(delay) => tokio::time::sleep(delay).await,
                }
            }
        }
        if stage == Stage::Write {
            let mut pos = self.pos.lock().unwrap();
            if msg.header_bytes().is_some() {
                let header: DataHeaderProto = msg.parse_header()?;
                *pos = header.offset as usize;
            }
            let bytes = msg.data_bytes().expect("buffered RPC payload");
            let mut data = self.script.data.lock().unwrap();
            let new_len = data.len().max(*pos + bytes.len());
            data.resize(new_len, 0);
            data[*pos..*pos + bytes.len()].copy_from_slice(bytes);
            *pos += bytes.len();
        }
        self.script.acked.lock().unwrap().push(stage);
        let header = if stage == Stage::Open {
            let request: BlockWriteRequest = msg.parse_header()?;
            *self.pos.lock().unwrap() = request.off as usize;
            let response = BlockWriteResponse {
                id: request.block.id,
                path: self
                    .script
                    .path
                    .as_ref()
                    .map(|p| p.to_string_lossy().into_owned()),
                off: request.off,
                block_size: request.block_size,
                storage_type: StorageType::Disk.into(),
                pipeline_status: None,
            };
            Some(bytes::BytesMut::from(response.encode_to_vec().as_slice()))
        } else {
            None
        };
        Ok(msg.success_with_data(header, DataSlice::empty()))
    }
}

struct TestEnv {
    context: Arc<FsContext>,
    workers: Vec<WorkerAddress>,
    scripts: Vec<Script>,
}
impl TestEnv {
    async fn new(scripts: Vec<Script>) -> Self {
        Self::with_minimum(scripts, 1).await
    }
    async fn with_minimum(scripts: Vec<Script>, minimum: usize) -> Self {
        let mut workers = Vec::new();
        for (idx, script) in scripts.iter().enumerate() {
            let port = curvine_net::net::NetUtils::hold_available_port();
            let mut server_conf = ServerConf::with_hostname("127.0.0.1", port);
            server_conf.io_threads = 2;
            server_conf.worker_threads = 2;
            let server = RpcServer::new(server_conf, ReplicaService(script.clone()));
            std::thread::spawn(move || server.block_on_start());
            workers.push(WorkerAddress {
                worker_id: idx as u32 + 1,
                hostname: if script.path.is_some() {
                    "localhost".into()
                } else {
                    "127.0.0.1".into()
                },
                ip_addr: "127.0.0.1".into(),
                rpc_port: u32::from(port),
                web_port: 1,
            });
        }
        let mut conf = ClusterConf::default();
        conf.client.hostname = "localhost".into();
        conf.client.short_circuit = scripts.iter().any(|s| s.path.is_some());
        conf.client.write.min_replicas = minimum;
        conf.client.data_timeout_ms = 600;
        conf.client.rpc_timeout_ms = 2000;
        conf.client.conn_timeout_ms = 200;
        conf.client.conn_retry_max_duration_ms = 200;
        conf.client.conn_retry_min_sleep_ms = 10;
        conf.client.conn_retry_max_sleep_ms = 20;
        let context = Arc::new(
            FsContext::with_rt(conf, Arc::new(Runtime::new("replica-tests", 2, 1))).unwrap(),
        );
        for worker in &workers {
            for attempt in 0..50 {
                if context.block_client(worker).await.is_ok() {
                    break;
                }
                assert!(attempt < 49, "server never became ready");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        Self {
            context,
            workers,
            scripts,
        }
    }
    fn locate(&self) -> LocatedBlock {
        LocatedBlock::new(
            ExtendedBlock::new(17, 0, StorageType::Disk, FileType::File),
            self.workers.clone(),
        )
    }
    async fn writer(&self) -> BlockWriter {
        BlockWriter::new(self.context.clone(), self.locate(), 0, 64)
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn default_minimum_keeps_successfully_opened_replica() {
    let env = TestEnv::new(vec![Script::default(), Script::fail(Stage::Open)]).await;
    let mut writer = env.writer().await;
    writer.write(DataSlice::from_str("abcdefgh")).await.unwrap();
    let commit = writer.complete().await.unwrap();
    assert_eq!(commit.block_len, 8);
    assert_eq!(commit.locations.len(), 1);
    assert_eq!(commit.locations[0].worker_id, 1);
    assert_eq!(env.scripts[0].count(Stage::Cancel), 0);
    assert_eq!(*env.scripts[0].data.lock().unwrap(), b"abcdefgh");
}

#[tokio::test]
async fn failed_write_waits_for_slow_healthy_ack_and_never_reuses_failed_replica() {
    let env = TestEnv::new(vec![
        Script::fail(Stage::Write),
        Script::delay(Stage::Write, 150),
    ])
    .await;
    let mut writer = env.writer().await;
    writer.write(DataSlice::from_str("abcd")).await.unwrap();
    assert_eq!(writer.pos(), 4);
    assert!(env.scripts[1].acked.lock().unwrap().contains(&Stage::Write));
    writer.write(DataSlice::from_str("efgh")).await.unwrap();
    writer.flush().await.unwrap();
    let commit = writer.complete().await.unwrap();
    assert_eq!(commit.locations.len(), 1);
    assert_eq!(commit.locations[0].worker_id, 2);
    assert_eq!(env.scripts[0].count(Stage::Write), 1);
    assert_eq!(env.scripts[0].count(Stage::Complete), 0);
    assert_eq!(*env.scripts[1].data.lock().unwrap(), b"abcdefgh");
}

#[tokio::test]
async fn single_replica_stall_is_bounded_below_outer_budget() {
    let env = TestEnv::new(vec![Script::delay(Stage::Write, 2000), Script::default()]).await;
    let mut writer = env.writer().await;
    let start = Instant::now();
    writer.write(DataSlice::from_str("abcd")).await.unwrap();
    assert!(
        start.elapsed() < Duration::from_millis(450),
        "replica must expire at half the 600ms budget, elapsed={:?}",
        start.elapsed()
    );
    let commit = writer.complete().await.unwrap();
    assert_eq!(commit.locations.len(), 1);
    assert_eq!(commit.locations[0].worker_id, 2);
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        // The client owns an independent Tokio runtime. Its final owner must
        // be dropped off the async test executor (Tokio disallows blocking
        // runtime shutdown there), including when an assertion unwinds.
        let rt = self.context.clone_runtime();
        std::thread::spawn(move || {
            while Arc::strong_count(&rt) > 1 {
                std::thread::sleep(Duration::from_millis(10));
            }
            drop(rt);
        });
        for script in &self.scripts {
            if let Some(path) = &script.path {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

#[tokio::test]
async fn opening_stalled_replicas_uses_one_concurrent_budget() {
    let env = TestEnv::new(vec![
        Script::delay(Stage::Open, 2000),
        Script::delay(Stage::Open, 2000),
        Script::default(),
    ])
    .await;
    let start = Instant::now();
    let mut writer = env.writer().await;
    assert!(
        start.elapsed() < Duration::from_millis(450),
        "serial open timeouts exhausted the phase: {:?}",
        start.elapsed()
    );
    writer.write(DataSlice::from_str("abc")).await.unwrap();
    let commit = writer.complete().await.unwrap();
    assert_eq!(commit.locations.len(), 1);
    assert_eq!(commit.locations[0].worker_id, 3);
    for script in &env.scripts {
        assert_eq!(script.count(Stage::Open), 1);
    }
}

#[tokio::test]
async fn open_error_does_not_cancel_a_later_successful_open() {
    let env = TestEnv::new(vec![
        Script::fail(Stage::Open),
        Script::delay(Stage::Open, 150),
    ])
    .await;
    let mut writer = env.writer().await;
    assert!(env.scripts[1].acked.lock().unwrap().contains(&Stage::Open));
    writer.write(DataSlice::from_str("abc")).await.unwrap();
    assert_eq!(writer.complete().await.unwrap().locations[0].worker_id, 2);
    assert_eq!(env.scripts[1].count(Stage::Cancel), 0);
}

#[tokio::test]
async fn flush_errors_and_timeouts_prune_only_failed_replicas() {
    for fault in [Fault::Error, Fault::Delay(Duration::from_secs(2))] {
        let env = TestEnv::new(vec![
            Script {
                fault: Some((Stage::Flush, fault)),
                ..Script::default()
            },
            Script::delay(Stage::Flush, 100),
        ])
        .await;
        let mut writer = env.writer().await;
        writer.write(DataSlice::from_str("abc")).await.unwrap();
        let start = Instant::now();
        writer.flush().await.unwrap();
        assert!(start.elapsed() < Duration::from_millis(450));
        assert!(env.scripts[1].acked.lock().unwrap().contains(&Stage::Flush));
        writer.write(DataSlice::from_str("def")).await.unwrap();
        let commit = writer.complete().await.unwrap();
        assert_eq!(commit.block_len, 6);
        assert_eq!(commit.locations.len(), 1);
        assert_eq!(commit.locations[0].worker_id, 2);
        assert_eq!(env.scripts[0].count(Stage::Write), 1);
        assert_eq!(env.scripts[0].count(Stage::Complete), 0);
    }
}

#[tokio::test]
async fn complete_merges_errors_and_timeouts_before_publishing_locations() {
    for fault in [Fault::Error, Fault::Delay(Duration::from_secs(2))] {
        let env = TestEnv::new(vec![
            Script {
                fault: Some((Stage::Complete, fault)),
                ..Script::default()
            },
            Script::delay(Stage::Complete, 100),
        ])
        .await;
        let mut writer = env.writer().await;
        writer.write(DataSlice::from_str("abc")).await.unwrap();
        let start = Instant::now();
        let commit = writer.complete().await.unwrap();
        assert!(start.elapsed() < Duration::from_millis(450));
        assert!(env.scripts[1]
            .acked
            .lock()
            .unwrap()
            .contains(&Stage::Complete));
        assert_eq!(commit.locations.len(), 1);
        assert_eq!(commit.locations[0].worker_id, 2);
        assert_eq!(writer.complete().await.unwrap().locations.len(), 1);
        writer.cancel().await.unwrap();
        assert_eq!(
            env.scripts[1].count(Stage::Complete),
            1,
            "Complete must be idempotent"
        );
        assert_eq!(
            env.scripts[1].count(Stage::Cancel),
            0,
            "never abort an acknowledged commit"
        );
    }
}

#[tokio::test]
async fn two_of_three_healthy_replicas_meet_configured_minimum() {
    let env = TestEnv::with_minimum(
        vec![
            Script::fail(Stage::Write),
            Script::default(),
            Script::default(),
        ],
        2,
    )
    .await;
    let mut writer = env.writer().await;
    writer.write(DataSlice::from_str("abc")).await.unwrap();
    let commit = writer.complete().await.unwrap();
    assert_eq!(
        commit
            .locations
            .iter()
            .map(|l| l.worker_id)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
}

#[tokio::test]
async fn falling_below_minimum_is_terminal_and_preserves_acknowledged_position() {
    let env = TestEnv::with_minimum(
        vec![Script::fail(Stage::Write), Script::delay(Stage::Write, 100)],
        2,
    )
    .await;
    let mut writer = env.writer().await;
    let error = writer
        .write(DataSlice::from_str("abc"))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("required 2"), "{error}");
    assert!(
        env.scripts[1].acked.lock().unwrap().contains(&Stage::Write),
        "join_all must finish even when below minimum"
    );
    assert_eq!((writer.pos(), writer.len(), writer.remaining()), (0, 0, 64));
    assert!(!writer.has_remaining());
    assert!(writer.write(DataSlice::from_str("def")).await.is_err());
    assert!(writer.flush().await.is_err());
    assert!(writer.seek(1).await.is_err());
    assert!(writer.complete().await.is_err());
    assert_eq!(env.scripts[1].count(Stage::Complete), 0);
    writer.cancel().await.unwrap();
    assert_eq!(env.scripts[1].count(Stage::Cancel), 1);
}

#[tokio::test]
async fn losing_all_replicas_never_panics_or_returns_empty_success() {
    let env = TestEnv::new(vec![Script::fail(Stage::Write), Script::fail(Stage::Write)]).await;
    let mut writer = env.writer().await;
    assert!(writer.write(DataSlice::from_str("abc")).await.is_err());
    assert_eq!(
        (
            writer.pos(),
            writer.len(),
            writer.remaining(),
            writer.is_empty()
        ),
        (0, 0, 64, true)
    );
    assert!(!writer.has_remaining());
    assert!(writer.write(DataSlice::empty()).await.is_err());
    assert!(writer.complete().await.is_err());
    assert!(writer.flush().await.is_err());
    assert!(writer.seek(0).await.is_err());
    writer.cancel().await.unwrap();
}

#[tokio::test]
async fn all_open_failures_do_not_construct_an_empty_writer() {
    let env = TestEnv::new(vec![Script::fail(Stage::Open), Script::fail(Stage::Open)]).await;
    assert!(BlockWriter::new(env.context.clone(), env.locate(), 0, 64)
        .await
        .is_err());
    assert_eq!(env.scripts[0].count(Stage::Open), 1);
    assert_eq!(env.scripts[1].count(Stage::Open), 1);
}

#[tokio::test]
async fn invalid_or_unavailable_minimum_is_rejected_before_opening() {
    for min in [0, 2] {
        let env = TestEnv::with_minimum(vec![Script::default()], min).await;
        assert!(BlockWriter::new(env.context.clone(), env.locate(), 0, 64)
            .await
            .is_err());
        assert_eq!(env.scripts[0].count(Stage::Open), 0);
    }
}

#[tokio::test]
async fn complete_below_minimum_never_aborts_the_acknowledged_replica() {
    let env =
        TestEnv::with_minimum(vec![Script::default(), Script::fail(Stage::Complete)], 2).await;
    let mut writer = env.writer().await;
    writer.write(DataSlice::from_str("abc")).await.unwrap();
    assert!(writer.complete().await.is_err());
    writer.cancel().await.unwrap();
    assert_eq!(env.scripts[0].count(Stage::Complete), 1);
    assert_eq!(env.scripts[0].count(Stage::Cancel), 0);
}

#[tokio::test]
async fn interrupted_complete_can_publish_only_acknowledged_survivors_on_retry() {
    let env = TestEnv::new(vec![
        Script::default(),
        Script::delay(Stage::Complete, 2000),
    ])
    .await;
    let mut writer = env.writer().await;
    writer.write(DataSlice::from_str("abc")).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), writer.complete())
            .await
            .is_err()
    );
    assert!(env.scripts[0]
        .acked
        .lock()
        .unwrap()
        .contains(&Stage::Complete));
    let commit = writer.complete().await.unwrap();
    assert_eq!(commit.block_len, 3);
    assert_eq!(commit.locations.len(), 1);
    assert_eq!(commit.locations[0].worker_id, 1);
    writer.cancel().await.unwrap();
    assert_eq!(env.scripts[0].count(Stage::Complete), 1);
    assert_eq!(env.scripts[0].count(Stage::Cancel), 0);
    assert_eq!(env.scripts[1].count(Stage::Complete), 1);
}

#[tokio::test]
async fn interrupted_write_cannot_be_replayed_or_committed() {
    let env = TestEnv::new(vec![Script::default(), Script::delay(Stage::Write, 2000)]).await;
    let mut writer = env.writer().await;
    assert!(tokio::time::timeout(
        Duration::from_millis(100),
        writer.write(DataSlice::from_str("abc"))
    )
    .await
    .is_err());
    assert!(env.scripts[0].acked.lock().unwrap().contains(&Stage::Write));
    assert!(writer.write(DataSlice::from_str("abc")).await.is_err());
    assert!(writer.complete().await.is_err());
    assert_eq!(writer.pos(), 0);
    assert_eq!(env.scripts[0].count(Stage::Write), 1);
    assert_eq!(env.scripts[0].count(Stage::Complete), 0);
}

#[tokio::test]
async fn completed_writer_rejects_more_data_instead_of_counting_completed_as_live() {
    let env = TestEnv::new(vec![Script::default()]).await;
    let mut writer = env.writer().await;
    writer.complete().await.unwrap();
    assert!(writer.write(DataSlice::from_str("abc")).await.is_err());
    assert_eq!(env.scripts[0].count(Stage::Write), 0);
}

#[tokio::test]
async fn remote_seek_is_state_only_and_invalid_seek_does_not_prune() {
    let env = TestEnv::new(vec![Script::default(), Script::default()]).await;
    let mut writer = env.writer().await;
    writer.write(DataSlice::from_str("abcdefgh")).await.unwrap();
    assert!(writer.seek(-1).await.is_err());
    assert!(writer.seek(65).await.is_err());
    writer.seek(3).await.unwrap();
    assert_eq!((writer.pos(), writer.len()), (3, 8));
    writer.write(DataSlice::from_str("XYZ")).await.unwrap();
    let commit = writer.complete().await.unwrap();
    assert_eq!(commit.block_len, 8);
    assert_eq!(commit.locations.len(), 2);
    for script in &env.scripts {
        assert_eq!(*script.data.lock().unwrap(), b"abcXYZgh");
    }
}

struct BlockingGate(Option<std::sync::mpsc::Sender<()>>);
impl BlockingGate {
    async fn occupy(rt: &Runtime) -> Self {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        rt.spawn_blocking(move || {
            let _ = started_tx.send(());
            let _ = release_rx.recv_timeout(Duration::from_secs(5));
        });
        started_rx.await.unwrap();
        Self(Some(release_tx))
    }
    async fn drain(mut self, rt: &Runtime) {
        let _ = self.0.take().unwrap().send(());
        rt.spawn_blocking(|| ()).await.unwrap();
    }
}
impl Drop for BlockingGate {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

#[tokio::test]
async fn local_seek_timeout_drops_local_replica_and_preserves_remote_position() {
    let path = std::env::temp_dir().join(format!(
        "curvine-replica-seek-{}",
        curvine_runtime::common::Utils::uuid()
    ));
    let env = TestEnv::new(vec![
        Script {
            path: Some(path),
            ..Script::default()
        },
        Script::default(),
    ])
    .await;
    let mut writer = env.writer().await;
    writer.write(DataSlice::from_str("abcdefgh")).await.unwrap();
    let gate = BlockingGate::occupy(env.context.rt()).await;
    let start = Instant::now();
    writer.seek(3).await.unwrap();
    assert!(start.elapsed() < Duration::from_millis(450));
    assert_eq!((writer.pos(), writer.len()), (3, 8));
    gate.drain(env.context.rt()).await;
    writer.write(DataSlice::from_str("XYZ")).await.unwrap();
    let commit = writer.complete().await.unwrap();
    assert_eq!(commit.locations.len(), 1);
    assert_eq!(commit.locations[0].worker_id, 2);
    assert_eq!(env.scripts[0].count(Stage::Complete), 0);
    assert_eq!(*env.scripts[1].data.lock().unwrap(), b"abcXYZgh");
}

#[tokio::test]
async fn unfinished_remote_open_is_disconnected_instead_of_pooled() {
    let env = TestEnv::new(vec![Script::default()]).await;
    let writer = env.writer().await;
    drop(writer);
    let mut next = env.writer().await;
    let connections = env.scripts[0].connections.lock().unwrap().clone();
    let opens: Vec<_> = connections
        .iter()
        .filter(|(s, _)| *s == Stage::Open)
        .map(|(_, c)| *c)
        .collect();
    assert_eq!(opens.len(), 2);
    assert_ne!(opens[0], opens[1], "nonterminal lease was returned to pool");
    next.complete().await.unwrap();
}

#[tokio::test]
async fn blocked_local_open_can_degrade_without_waiting_for_disk_executor() {
    let path = std::env::temp_dir().join(format!(
        "curvine-replica-open-{}",
        curvine_runtime::common::Utils::uuid()
    ));
    let env = TestEnv::new(vec![
        Script {
            path: Some(path),
            ..Script::default()
        },
        Script::default(),
    ])
    .await;
    let gate = BlockingGate::occupy(env.context.rt()).await;
    let start = Instant::now();
    let mut writer = env.writer().await;
    assert!(start.elapsed() < Duration::from_millis(450));
    writer.write(DataSlice::from_str("abc")).await.unwrap();
    let commit = writer.complete().await.unwrap();
    assert_eq!(commit.locations.len(), 1);
    assert_eq!(commit.locations[0].worker_id, 2);
    assert_eq!(env.scripts[0].count(Stage::Complete), 0);
    assert_eq!(env.scripts[0].count(Stage::Cancel), 0);
    gate.drain(env.context.rt()).await;
}

#[tokio::test]
async fn blocked_local_write_flush_and_complete_degrade_to_remote_replica() {
    for stage in [Stage::Write, Stage::Flush, Stage::Complete] {
        let path = std::env::temp_dir().join(format!(
            "curvine-replica-disk-{}",
            curvine_runtime::common::Utils::uuid()
        ));
        let env = TestEnv::new(vec![
            Script {
                path: Some(path),
                ..Script::default()
            },
            Script::default(),
        ])
        .await;
        let mut writer = env.writer().await;
        writer.write(DataSlice::from_str("abc")).await.unwrap();
        let gate = BlockingGate::occupy(env.context.rt()).await;
        let start = Instant::now();
        let commit = match stage {
            Stage::Write => {
                writer.write(DataSlice::from_str("def")).await.unwrap();
                writer.complete().await.unwrap()
            }
            Stage::Flush => {
                writer.flush().await.unwrap();
                writer.complete().await.unwrap()
            }
            Stage::Complete => writer.complete().await.unwrap(),
            _ => unreachable!(),
        };
        assert!(
            start.elapsed() < Duration::from_millis(450),
            "{stage:?}: {:?}",
            start.elapsed()
        );
        assert_eq!(commit.locations.len(), 1);
        assert_eq!(commit.locations[0].worker_id, 2);
        assert_eq!(commit.block_len, if stage == Stage::Write { 6 } else { 3 });
        assert_eq!(env.scripts[0].count(Stage::Complete), 0);
        assert_eq!(env.scripts[0].count(Stage::Cancel), 0);
        gate.drain(env.context.rt()).await;
    }
}

#[tokio::test]
async fn below_minimum_open_attempts_all_cancels_and_preserves_primary_error() {
    let env = TestEnv::with_minimum(
        vec![
            Script::fail(Stage::Open),
            Script::delay(Stage::Cancel, 2000),
            Script::default(),
        ],
        3,
    )
    .await;
    let start = Instant::now();
    let error = match BlockWriter::new(env.context.clone(), env.locate(), 0, 64).await {
        Ok(_) => panic!("minimum was not met"),
        Err(e) => e.to_string(),
    };
    assert!(error.contains("injected Open failure"), "{error}");
    assert!(start.elapsed() < Duration::from_millis(450));
    assert_eq!(env.scripts[1].count(Stage::Cancel), 1);
    assert_eq!(env.scripts[2].count(Stage::Cancel), 1);
    assert!(env.scripts[2]
        .acked
        .lock()
        .unwrap()
        .contains(&Stage::Cancel));
}

#[tokio::test]
async fn cancelled_open_merge_disconnects_already_opened_remote_sessions() {
    let env = TestEnv::new(vec![Script::default(), Script::delay(Stage::Open, 2000)]).await;
    assert!(tokio::time::timeout(
        Duration::from_millis(100),
        BlockWriter::new(env.context.clone(), env.locate(), 0, 64)
    )
    .await
    .is_err());
    let mut locate = env.locate();
    locate.locs.truncate(1);
    let mut writer = BlockWriter::new(env.context.clone(), locate, 0, 64)
        .await
        .unwrap();
    let connections = env.scripts[0].connections.lock().unwrap().clone();
    let opens: Vec<_> = connections
        .iter()
        .filter(|(s, _)| *s == Stage::Open)
        .map(|(_, c)| *c)
        .collect();
    assert_eq!(opens.len(), 2);
    assert_ne!(opens[0], opens[1]);
    writer.complete().await.unwrap();
}

#[tokio::test]
async fn acknowledged_complete_still_allows_connection_pool_reuse() {
    let env = TestEnv::new(vec![Script::default()]).await;
    let mut writer = env.writer().await;
    writer.complete().await.unwrap();
    drop(writer);
    let mut next = env.writer().await;
    let connections = env.scripts[0].connections.lock().unwrap().clone();
    let opens: Vec<_> = connections
        .iter()
        .filter(|(s, _)| *s == Stage::Open)
        .map(|(_, c)| *c)
        .collect();
    assert_eq!(opens.len(), 2);
    assert_eq!(
        opens[0], opens[1],
        "finished sessions should remain poolable"
    );
    next.cancel().await.unwrap();
}
