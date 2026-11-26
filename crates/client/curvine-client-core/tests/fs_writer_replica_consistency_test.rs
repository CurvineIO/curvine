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

//! File-level replica continuity using real RPC transport and scripted peers.
//! The metadata peer is NOT a Master: real RocksDB publication is tested separately.

use curvine_client_core::file::{FsClient, FsContext, FsWriterBase};
use curvine_config::ClusterConf;
use curvine_fs_api::{Path, RpcCode};
use curvine_io::DataSlice;
use curvine_model::{
    CommitBlock, CreateFileOpts, ExtendedBlock, FileBlocks, FileStatus, FileType, LocatedBlock,
    OpenFlags, ProtoUtils, StorageType, WorkerAddress,
};
use curvine_net::net::ConnState;
use curvine_proto::{
    BlockWriteRequest, BlockWriteResponse, BlocksBatchWriteRequest, BlocksBatchWriteResponse,
    CompleteFileRequest, CompleteFileResponse, DataHeaderProto, FilesBatchWriteRequest,
    GetFilesystemInfoResponse, OpenFileResponse,
};
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::{Message, RequestStatus};
use curvine_rpc::server::{RpcServer, ServerConf};
use curvine_runtime::runtime::Runtime;
use prost::Message as _;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const IDS: [i64; 3] = [17, 18, 19];
const INITIAL: [&[u8]; 3] = [b"ABCDEFGH", b"ijklmnop", b"QRSTUVWX"];

#[derive(Clone)]
struct Replica {
    data: Arc<Mutex<HashMap<i64, Vec<u8>>>>,
    opens: Arc<Mutex<Vec<i64>>>,
    // One failed write on block 17, after which this peer recovers but its bytes
    // remain stale. A timeout deliberately never applies the abandoned write.
    fault: Arc<Mutex<Option<bool>>>,
    fail_resize: Arc<Mutex<bool>>,
    fail_batch_complete: Arc<Mutex<bool>>,
}
impl Replica {
    fn new(fault: Option<bool>) -> Self {
        Self {
            data: Arc::new(Mutex::new(
                IDS.into_iter()
                    .zip(INITIAL.iter().map(|v| v.to_vec()))
                    .collect(),
            )),
            opens: Arc::default(),
            fault: Arc::new(Mutex::new(fault)),
            fail_resize: Arc::default(),
            fail_batch_complete: Arc::default(),
        }
    }
}
struct ReplicaService(Replica);
impl HandlerService for ReplicaService {
    type Item = ReplicaHandler;
    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        ReplicaHandler {
            replica: self.0.clone(),
            session: Mutex::new((0, 0)),
            batch_session: Mutex::new(Vec::new()),
        }
    }
}
struct ReplicaHandler {
    replica: Replica,
    session: Mutex<(i64, usize)>,
    batch_session: Mutex<Vec<i64>>,
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
        if RpcCode::from(msg.code()) == RpcCode::WriteBlocksBatch {
            let response = match msg.request_status() {
                RequestStatus::Open => {
                    let req: BlocksBatchWriteRequest = msg.parse_header()?;
                    let ids: Vec<_> = req.blocks.iter().map(|block| block.id).collect();
                    self.replica.opens.lock().unwrap().extend(&ids);
                    *self.batch_session.lock().unwrap() = ids;
                    let responses = req
                        .blocks
                        .iter()
                        .map(|block| BlockWriteResponse {
                            id: block.id,
                            off: req.off,
                            block_size: req.block_size,
                            storage_type: StorageType::Disk.into(),
                            ..Default::default()
                        })
                        .collect();
                    Some(bytes::BytesMut::from(
                        BlocksBatchWriteResponse { responses }
                            .encode_to_vec()
                            .as_slice(),
                    ))
                }
                RequestStatus::Running => {
                    let req: FilesBatchWriteRequest = msg.parse_header()?;
                    let ids = self.batch_session.lock().unwrap();
                    assert_eq!(ids.len(), req.files.len());
                    for (id, file) in ids.iter().zip(req.files) {
                        self.replica.data.lock().unwrap().insert(*id, file.content);
                    }
                    None
                }
                RequestStatus::Complete => {
                    if *self.replica.fail_batch_complete.lock().unwrap() {
                        return curvine_core_error::err_box!("batch completion failure");
                    }
                    None
                }
                RequestStatus::Cancel => None,
                status => panic!("unexpected batch {status:?}"),
            };
            return Ok(msg.success_with_data(response, DataSlice::empty()));
        }
        assert_eq!(RpcCode::from(msg.code()), RpcCode::WriteBlock);
        let header = match msg.request_status() {
            RequestStatus::Open => {
                let req: BlockWriteRequest = msg.parse_header()?;
                if req.block.alloc_opts.is_some()
                    && std::mem::take(&mut *self.replica.fail_resize.lock().unwrap())
                {
                    return curvine_core_error::err_box!("one-shot resize open failure");
                }
                *self.session.lock().unwrap() = (req.block.id, req.off as usize);
                self.replica.opens.lock().unwrap().push(req.block.id);
                let rep = BlockWriteResponse {
                    path: None,
                    id: req.block.id,
                    off: req.off,
                    block_size: req.block_size,
                    storage_type: StorageType::Disk.into(),
                    pipeline_status: None,
                };
                Some(bytes::BytesMut::from(rep.encode_to_vec().as_slice()))
            }
            RequestStatus::Running => {
                let id = self.session.lock().unwrap().0;
                let fault = if id == IDS[0] && msg.data_len() > 0 {
                    self.replica.fault.lock().unwrap().take()
                } else {
                    None
                };
                if let Some(timeout) = fault {
                    if timeout {
                        tokio::time::sleep(Duration::from_millis(900)).await;
                    }
                    return curvine_core_error::err_box!("one-shot block write failure");
                }
                let mut session = self.session.lock().unwrap();
                if msg.header_bytes().is_some() {
                    let req: DataHeaderProto = msg.parse_header()?;
                    session.1 = req.offset as usize;
                }
                if msg.data_len() > 0 {
                    let bytes = msg.data_bytes().expect("RPC bytes");
                    let mut blocks = self.replica.data.lock().unwrap();
                    let data = blocks.get_mut(&id).expect("known block");
                    let end = session.1 + bytes.len();
                    data.resize(data.len().max(end), 0);
                    data[session.1..end].copy_from_slice(bytes);
                    session.1 = end;
                }
                None
            }
            RequestStatus::Complete | RequestStatus::Cancel => None,
            status => panic!("unexpected {status:?}"),
        };
        Ok(msg.success_with_data(header, DataSlice::empty()))
    }
}

#[derive(Clone)]
struct Metadata {
    blocks: Arc<Mutex<FileBlocks>>,
    commits: Arc<Mutex<Vec<Vec<CommitBlock>>>>,
}
struct MetadataService(Metadata);
impl HandlerService for MetadataService {
    type Item = Metadata;
    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        self.0.clone()
    }
}
impl MessageHandler for Metadata {
    type Error = curvine_core_error::CommonErrorExt;
    fn handle(&self, msg: &Message) -> Result<Message, Self::Error> {
        let encoded = match RpcCode::from(msg.code()) {
            RpcCode::GetFilesystemInfo => GetFilesystemInfoResponse::default().encode_to_vec(),
            RpcCode::OpenFile => OpenFileResponse {
                file_blocks: ProtoUtils::file_blocks_to_pb(self.blocks.lock().unwrap().clone()),
                first_block: None,
            }
            .encode_to_vec(),
            RpcCode::ResizeFile => {
                let mut blocks = self.blocks.lock().unwrap();
                blocks.block_locs[0].block.alloc_opts =
                    Some(curvine_model::FileAllocOpts::with_truncate(8));
                curvine_proto::FileResizeResponse {
                    file_blocks: ProtoUtils::file_blocks_to_pb(blocks.clone()),
                }
                .encode_to_vec()
            }
            RpcCode::CompleteFile => {
                let req: CompleteFileRequest = msg.parse_header()?;
                let commits: Vec<_> = req
                    .commit_blocks
                    .into_iter()
                    .map(ProtoUtils::commit_block_from_pb)
                    .collect();
                let mut blocks = self.blocks.lock().unwrap();
                // Contract oracle only. Real namespace/index atomicity is
                // separately verified using MasterFilesystem.
                for commit in &commits {
                    let lb = blocks
                        .block_locs
                        .iter()
                        .find(|lb| lb.id == commit.block_id)
                        .unwrap();
                    if commit.expected_revision != lb.commit_revision {
                        return curvine_core_error::err_box!("stale client commit");
                    }
                    if commit.locations.iter().any(|loc| {
                        !lb.locs
                            .iter()
                            .any(|worker| worker.worker_id == loc.worker_id)
                    }) {
                        return curvine_core_error::err_box!(
                            "client cannot restore excluded replica"
                        );
                    }
                }
                for commit in &commits {
                    let lb = blocks
                        .block_locs
                        .iter_mut()
                        .find(|lb| lb.id == commit.block_id)
                        .unwrap();
                    lb.locs.retain(|loc| {
                        commit
                            .locations
                            .iter()
                            .any(|c| c.worker_id == loc.worker_id)
                    });
                    lb.block.len = commit.block_len;
                    lb.block.alloc_opts = None;
                    lb.commit_revision = lb.commit_revision.map(|revision| revision + 1);
                }
                blocks.status.len = req.len;
                blocks.status.is_complete = !req.only_flush;
                self.commits.lock().unwrap().push(commits);
                CompleteFileResponse {
                    result: true,
                    file_blocks: req
                        .only_flush
                        .then(|| ProtoUtils::file_blocks_to_pb(blocks.clone())),
                }
                .encode_to_vec()
            }
            code => panic!("unexpected metadata RPC {code:?}"),
        };
        Ok(msg.success_with_data(
            Some(bytes::BytesMut::from(encoded.as_slice())),
            DataSlice::empty(),
        ))
    }
}

fn serve<S: HandlerService + Send + 'static>(service: S) -> u16 {
    let port = curvine_net::net::NetUtils::hold_available_port();
    let mut conf = ServerConf::with_hostname("127.0.0.1", port);
    conf.io_threads = 2;
    conf.worker_threads = 2;
    let server = RpcServer::new(conf, service);
    std::thread::spawn(move || server.block_on_start());
    port
}
struct Env {
    context: Arc<FsContext>,
    replicas: Vec<Replica>,
    metadata: Metadata,
}
impl Env {
    async fn new(cache: usize, timeout: bool) -> Self {
        Self::new_with_minimum(cache, timeout, 2).await
    }
    async fn new_with_minimum(cache: usize, timeout: bool, minimum: usize) -> Self {
        let replicas = vec![
            Replica::new(Some(timeout)),
            Replica::new(None),
            Replica::new(None),
        ];
        let workers: Vec<_> = replicas
            .iter()
            .enumerate()
            .map(|(i, replica)| WorkerAddress {
                worker_id: i as u32 + 1,
                hostname: "127.0.0.1".into(),
                ip_addr: "127.0.0.1".into(),
                rpc_port: serve(ReplicaService(replica.clone())) as u32,
                web_port: 1,
            })
            .collect();
        let blocks = FileBlocks::new(
            FileStatus {
                id: 7,
                path: "/replica-continuity".into(),
                len: 24,
                block_size: 8,
                replicas: 3,
                file_type: FileType::File,
                ..FileStatus::default()
            },
            IDS.into_iter()
                .map(|id| {
                    LocatedBlock::new(
                        ExtendedBlock::new(id, 8, StorageType::Disk, FileType::File),
                        workers.clone(),
                    )
                })
                .collect(),
        );
        let metadata = Metadata {
            blocks: Arc::new(Mutex::new(blocks)),
            commits: Arc::default(),
        };
        let port = serve(MetadataService(metadata.clone()));
        let mut conf = ClusterConf::default();
        conf.master.hostname = "127.0.0.1".into();
        conf.master.rpc_port = port;
        conf.client.short_circuit = false;
        conf.client.max_cache_block_handles = cache;
        conf.client.write.min_replicas = minimum;
        conf.client.data_timeout_ms = 600;
        conf.client.rpc_timeout_ms = 2000;
        conf.client.conn_timeout_ms = 200;
        conf.client.conn_retry_max_duration_ms = 200;
        conf.client.conn_retry_min_sleep_ms = 10;
        conf.client.conn_retry_max_sleep_ms = 20;
        let context = Arc::new(
            FsContext::with_rt(conf, Arc::new(Runtime::new("file-replica-tests", 2, 1))).unwrap(),
        );
        for worker in &workers {
            for attempt in 0..50 {
                if context.block_client(worker).await.is_ok() {
                    break;
                }
                assert!(attempt < 49, "server did not start");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        Self {
            context,
            replicas,
            metadata,
        }
    }
    fn writer(&self) -> FsWriterBase {
        FsWriterBase::new(
            self.context.clone(),
            Path::from_str("/replica-continuity").unwrap(),
            self.metadata.blocks.lock().unwrap().clone(),
            0,
        )
    }
    fn assert_file(&self, blocks: &FileBlocks, expected: &[u8]) {
        assert_eq!(blocks.status.len, expected.len() as i64);
        for (i, lb) in blocks.block_locs.iter().enumerate() {
            let ids: Vec<_> = lb.locs.iter().map(|loc| loc.worker_id).collect();
            assert_eq!(
                ids,
                if i == 0 { vec![2, 3] } else { vec![1, 2, 3] },
                "block {} advertised a stale replica; stale bytes={:?}",
                lb.id,
                self.replicas[0].data.lock().unwrap()[&lb.id]
            );
            for loc in &lb.locs {
                assert_eq!(
                    self.replicas[loc.worker_id as usize - 1]
                        .data
                        .lock()
                        .unwrap()[&lb.id],
                    expected[i * 8..(i + 1) * 8],
                    "block {} worker {}",
                    lb.id,
                    loc.worker_id
                );
            }
        }
    }
}
impl Drop for Env {
    fn drop(&mut self) {
        let rt = self.context.clone_runtime();
        std::thread::spawn(move || {
            while Arc::strong_count(&rt) > 1 {
                std::thread::sleep(Duration::from_millis(10));
            }
            drop(rt);
        });
    }
}

async fn rewrite_and_evict(env: &Env, writer: &mut FsWriterBase) {
    writer.seek(2).await.unwrap();
    writer.write(DataSlice::from_str("zz")).await.unwrap();
    // Cross-block write. Visit a third block to evict block 17 with cache=1.
    writer.seek(6).await.unwrap();
    writer.write(DataSlice::from_str("123456")).await.unwrap();
    writer.seek(16).await.unwrap();
    writer.write(DataSlice::from_str("!")).await.unwrap();
    assert!(env.replicas[0].fault.lock().unwrap().is_none());
}
async fn eviction_case(cache: usize, timeout: bool) {
    let env = Env::new(cache, timeout).await;
    let mut writer = env.writer();
    rewrite_and_evict(&env, &mut writer).await;
    let cached = writer.file_blocks();
    writer.seek(0).await.unwrap();
    writer.write(DataSlice::from_str("?")).await.unwrap();
    writer.complete().await.unwrap();
    let blocks = env.metadata.blocks.lock().unwrap().clone();
    env.assert_file(&blocks, b"?BzzEF123456mnop!RSTUVWX");
    assert_eq!(
        cached.block_locs[0]
            .locs
            .iter()
            .map(|l| l.worker_id)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(
        env.replicas[0]
            .opens
            .lock()
            .unwrap()
            .iter()
            .filter(|id| **id == IDS[0])
            .count(),
        1,
        "the recovered stale worker must not be reopened"
    );
    let commits = env.metadata.commits.lock().unwrap();
    let commit = commits
        .last()
        .unwrap()
        .iter()
        .find(|c| c.block_id == IDS[0])
        .unwrap();
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
async fn error_survivors_persist_across_zero_cache_reopen() {
    eviction_case(0, false).await;
}
#[tokio::test]
async fn timeout_survivors_persist_across_zero_cache_reopen() {
    eviction_case(0, true).await;
}
#[tokio::test]
async fn error_survivors_persist_across_fifo_eviction() {
    eviction_case(1, false).await;
}
#[tokio::test]
async fn timeout_survivors_persist_across_fifo_eviction() {
    eviction_case(1, true).await;
}

async fn publish_case(close: bool, timeout: bool) {
    let env = Env::new(1, timeout).await;
    let mut writer = env.writer();
    rewrite_and_evict(&env, &mut writer).await;
    if close {
        writer.complete().await.unwrap();
        drop(writer);
        // New writer constructed from independently returned metadata, not the
        // old writer's cache. Real Master persistence is a separate test layer.
        let path = Path::from_str("/replica-continuity").unwrap();
        let blocks = FsClient::new(env.context.clone())
            .open_with_opts(
                &path,
                CreateFileOpts::with_create(false),
                OpenFlags::new_read_write(),
            )
            .await
            .unwrap();
        writer = FsWriterBase::new(env.context.clone(), path, blocks, 0);
    } else {
        writer.flush().await.unwrap();
        env.assert_file(&writer.file_blocks(), b"ABzzEF123456mnop!RSTUVWX");
    }
    writer.seek(0).await.unwrap();
    writer.write(DataSlice::from_str("?")).await.unwrap();
    writer.complete().await.unwrap();
    env.assert_file(
        &env.metadata.blocks.lock().unwrap().clone(),
        b"?BzzEF123456mnop!RSTUVWX",
    );
    let commits = env.metadata.commits.lock().unwrap();
    assert_eq!(commits.len(), 2);
    for publication in commits.iter() {
        let commit = publication.iter().find(|c| c.block_id == IDS[0]).unwrap();
        assert_eq!(
            commit
                .locations
                .iter()
                .map(|l| l.worker_id)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
    }
}
#[tokio::test]
async fn error_survivors_persist_after_flush_refresh() {
    publish_case(false, false).await;
}
#[tokio::test]
async fn timeout_survivors_persist_after_flush_refresh() {
    publish_case(false, true).await;
}
#[tokio::test]
async fn error_survivors_persist_after_close_and_new_writer() {
    publish_case(true, false).await;
}
#[tokio::test]
async fn timeout_survivors_persist_after_close_and_new_writer() {
    publish_case(true, true).await;
}

#[tokio::test]
async fn degraded_resize_preserves_survivor_commit_through_close() {
    let env = Env::new(0, false).await;
    *env.replicas[0].fail_resize.lock().unwrap() = true;
    let mut writer = env.writer();
    writer
        .resize(curvine_model::FileAllocOpts::with_truncate(24))
        .await
        .unwrap();
    writer.complete().await.unwrap();
    let commits = env.metadata.commits.lock().unwrap();
    let commit = commits
        .last()
        .unwrap()
        .iter()
        .find(|c| c.block_id == IDS[0])
        .unwrap();
    assert_eq!(
        commit
            .locations
            .iter()
            .map(|l| l.worker_id)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(
        env.metadata.blocks.lock().unwrap().block_locs[0]
            .locs
            .iter()
            .map(|l| l.worker_id)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
}

#[tokio::test]
async fn batch_minimum_rejects_entire_mixed_batch_before_any_open() {
    let env = Env::new(0, false).await;
    let mut blocks = env.metadata.blocks.lock().unwrap().block_locs.clone();
    blocks[1].locs.truncate(1);
    let result =
        curvine_client_core::block::BatchBlockWriter::new(env.context.clone(), blocks).await;
    assert!(result.is_err());
    assert!(env
        .replicas
        .iter()
        .all(|r| r.opens.lock().unwrap().is_empty()));
}

#[tokio::test]
async fn batch_minimum_rejects_single_replica_before_open() {
    let env = Env::new(0, false).await;
    let mut block = env.metadata.blocks.lock().unwrap().block_locs[0].clone();
    block.locs.truncate(1);
    assert!(
        curvine_client_core::block::BatchBlockWriter::new(env.context.clone(), vec![block])
            .await
            .is_err()
    );
    assert!(env
        .replicas
        .iter()
        .all(|r| r.opens.lock().unwrap().is_empty()));
}

#[tokio::test]
async fn stale_second_handle_cannot_publish_recovered_partial_range_replica() {
    let env = Env::new(0, false).await;
    let mut first = env.writer();
    let mut stale = env.writer(); // Both cache version 0 and workers 1,2,3.
    first.seek(1).await.unwrap();
    first.write(DataSlice::from_str("zz")).await.unwrap();
    first.flush().await.unwrap(); // Worker 1 missed this range, version becomes 1.
    stale.seek(5).await.unwrap();
    stale.write(DataSlice::from_str("!!")).await.unwrap(); // Worker 1 recovers.
    assert!(stale.complete().await.is_err());
    let blocks = env.metadata.blocks.lock().unwrap();
    assert_eq!(blocks.block_locs[0].commit_revision, Some(1));
    assert_eq!(
        blocks.block_locs[0]
            .locs
            .iter()
            .map(|w| w.worker_id)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(env.replicas[0].data.lock().unwrap()[&17], b"ABCDE!!H");
    assert_eq!(env.replicas[1].data.lock().unwrap()[&17], b"AzzDE!!H");
    assert_eq!(env.replicas[2].data.lock().unwrap()[&17], b"AzzDE!!H");
    // The precondition protects advertisement, not all concurrent byte writes.
}

#[tokio::test]
async fn resize_below_minimum_fails_and_does_not_publish_survivors() {
    let env = Env::new_with_minimum(0, false, 3).await;
    *env.replicas[0].fail_resize.lock().unwrap() = true;
    let mut writer = env.writer();
    assert!(writer
        .resize(curvine_model::FileAllocOpts::with_truncate(24))
        .await
        .is_err());
    assert!(env.metadata.commits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn batch_minimum_zero_rejects_before_any_open() {
    let env = Env::new_with_minimum(0, false, 0).await;
    let blocks = env.metadata.blocks.lock().unwrap().block_locs.clone();
    assert!(
        curvine_client_core::block::BatchBlockWriter::new(env.context.clone(), blocks)
            .await
            .is_err()
    );
    assert!(env
        .replicas
        .iter()
        .all(|r| r.opens.lock().unwrap().is_empty()));
}

#[tokio::test]
async fn batch_at_minimum_writes_all_bytes_and_propagates_commit_revision() {
    let env = Env::new(0, false).await;
    let mut blocks = env.metadata.blocks.lock().unwrap().block_locs.clone();
    for (index, block) in blocks.iter_mut().enumerate() {
        block.locs.truncate(2);
        block.commit_revision = Some(index as u64 + 4);
    }
    let mut writer = curvine_client_core::block::BatchBlockWriter::new(env.context.clone(), blocks)
        .await
        .unwrap();
    let paths: Vec<_> = ["/a", "/b", "/c"]
        .iter()
        .map(|path| Path::from_str(path).unwrap())
        .collect();
    let contents = ["first", "second", "third"];
    let files: Vec<_> = paths.iter().zip(contents).collect();
    writer.write(&files).await.unwrap();
    let commits = writer.complete().await.unwrap();
    assert_eq!(commits.len(), 3);
    for (index, commit) in commits.iter().enumerate() {
        assert_eq!(commit.expected_revision, Some(index as u64 + 4));
        assert_eq!(commit.block_len, contents[index].len() as i64);
        assert_eq!(
            commit
                .locations
                .iter()
                .map(|l| l.worker_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        for replica in &env.replicas[..2] {
            assert_eq!(
                replica.data.lock().unwrap()[&IDS[index]],
                contents[index].as_bytes()
            );
        }
    }
    assert!(env.replicas[2].opens.lock().unwrap().is_empty());
}

#[tokio::test]
async fn batch_failure_still_requires_all_assigned_replicas() {
    let env = Env::new(0, false).await;
    *env.replicas[0].fail_batch_complete.lock().unwrap() = true;
    let blocks = env.metadata.blocks.lock().unwrap().block_locs.clone();
    let mut writer = curvine_client_core::block::BatchBlockWriter::new(env.context.clone(), blocks)
        .await
        .unwrap();
    // Two successes satisfy the configured minimum, but batch remains all-or-nothing.
    assert!(writer.complete().await.is_err());
    assert!(env.metadata.commits.lock().unwrap().is_empty());
}
