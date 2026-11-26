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

use curvine_client_core::file::{FsContext, FsWriter};
use curvine_config::ClusterConf;
use curvine_fs_api::{Path, RpcCode, Writer};
use curvine_model::{
    ExtendedBlock, FileBlocks, FileStatus, FileType, LocatedBlock, StorageType, WorkerAddress,
};
use curvine_proto::{BlockWriteRequest, BlockWriteResponse};
use curvine_rpc::handler::{Frame, RpcFrame};
use curvine_rpc::message::{Builder, RequestStatus};
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use futures::FutureExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

const CHUNK_SIZE: usize = 4096;

async fn record_data_frames(listener: TcpListener, total: usize) -> Vec<Vec<u8>> {
    let (socket, _) = listener.accept().await.unwrap();
    let mut peer = RpcFrame::with_client(socket, CHUNK_SIZE);
    let open = peer.receive().await.unwrap();
    assert_eq!(RpcCode::from(open.code()), RpcCode::WriteBlock);
    assert_eq!(open.request_status(), RequestStatus::Open);
    let request: BlockWriteRequest = open.parse_header().unwrap();
    let opened = BlockWriteResponse {
        block_size: request.block_size,
        storage_type: StorageType::Disk.into(),
        ..Default::default()
    };
    peer.send(Builder::success_with_header(&open, opened).build())
        .await
        .unwrap();

    let mut frames = Vec::new();
    let mut received = 0;
    while received < total {
        let data = peer.receive().await.unwrap();
        assert_eq!(RpcCode::from(data.code()), RpcCode::WriteBlock);
        assert_eq!(data.request_status(), RequestStatus::Running);
        assert!(!data.data.is_empty());
        received += data.data.len();
        frames.push(data.data.as_slice().to_vec());
        peer.send(Builder::success(&data).build()).await.unwrap();
    }
    frames
}

fn frames_at_depth(depth: usize, payload: &[u8]) -> Vec<Vec<u8>> {
    let rt = Arc::new(AsyncRuntime::current_thread("write-batch-test"));
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let worker = WorkerAddress {
            worker_id: 1,
            ip_addr: addr.ip().to_string(),
            hostname: addr.ip().to_string(),
            rpc_port: addr.port().into(),
            ..Default::default()
        };
        let block = ExtendedBlock::new(1, 0, StorageType::Disk, FileType::File);
        let status = FileStatus {
            block_size: 64 * 1024,
            replicas: 1,
            ..Default::default()
        };
        let mut conf = ClusterConf::default();
        conf.client.short_circuit = false;
        conf.client.write_chunk_size = CHUNK_SIZE;
        conf.client.write_chunk_num = 4;
        conf.client.write_batch_chunks = depth;
        let ctx = Arc::new(FsContext::with_rt(conf, rt.clone()).unwrap());
        let mut writer = FsWriter::with_first_block(
            ctx,
            Path::from_str("/batched").unwrap(),
            FileBlocks::new(status, vec![]),
            false,
            Some(LocatedBlock::new(block, vec![worker])),
        );

        // The dispatcher shares this current-thread runtime. Polling once queues all
        // four chunks before it can run, including the SDK's last buffered chunk.
        async {
            writer.write(payload).await.unwrap();
            writer.flush_chunk().await.unwrap();
        }
        .now_or_never()
        .expect("all four chunks must fit without yielding");

        tokio::time::timeout(Duration::from_secs(10), async {
            let frames = record_data_frames(listener, payload.len()).await;
            // A seek to the current position acknowledges the drained SDK queue.
            writer.seek(payload.len() as i64).await.unwrap();
            frames
        })
        .await
        .expect("worker must acknowledge all queued data")
    })
}

#[test]
fn sdk_dispatch_gathers_only_at_enabled_depth() {
    let chunks: Vec<_> = (0..4).map(|byte| vec![byte; CHUNK_SIZE]).collect();
    let payload = chunks.concat();
    let separate = frames_at_depth(1, &payload);
    assert_eq!(separate.len(), 4, "depth one sends each chunk separately");
    assert!(
        separate == chunks,
        "single-chunk frames changed the payload"
    );

    let gathered = frames_at_depth(4, &payload);
    assert_eq!(
        gathered.len(),
        1,
        "depth four gathers the four ready chunks"
    );
    assert!(gathered[0] == payload, "gathered frame changed the payload");
}
