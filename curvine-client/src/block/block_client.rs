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

#![allow(clippy::too_many_arguments)]

use crate::block::{
    BlockClientPool, BlockReadContext, CreateBatchBlockContext, CreateBlockContext,
};
use crate::file::FsContext;
use curvine_common::conf::ClientConf;
use curvine_common::error::FsError;
use curvine_common::fs::Path;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    BlockReadRequest, BlockReadResponse, BlockWriteRequest, BlockWriteResponse,
    BlocksBatchCommitRequest, BlocksBatchWriteRequest, BlocksBatchWriteResponse, DataHeaderProto,
    FileWriteData, FilesBatchWriteRequest,
};
use curvine_common::state::{ExtendedBlock, StorageType, WorkerAddress};
use curvine_common::utils::ProtoUtils;
use curvine_common::FsResult;
use orpc::client::RpcClient;
use orpc::common::LocalTime;
use orpc::error::ErrorExt;
use orpc::message::{Builder, Message, RequestStatus};
use orpc::sys::DataSlice;
use orpc::{try_option_ref, CommonResult};
use std::sync::Arc;
use std::time::Duration;

pub struct BlockClient {
    client: Option<RpcClient>,
    client_name: String,
    timeout: Duration,
    pool: Option<Arc<BlockClientPool>>,
    worker_addr: WorkerAddress,
    uptime: u64,
}

impl BlockClient {
    pub fn new(client: RpcClient, worker_addr: WorkerAddress, context: &FsContext) -> Self {
        Self {
            client: Some(client),
            client_name: context.clone_client_name(),
            timeout: Duration::from_millis(context.conf.client.data_timeout_ms),
            pool: None,
            worker_addr,
            uptime: LocalTime::mills(),
        }
    }

    pub fn set_pool(&mut self, pool: Arc<BlockClientPool>) {
        self.pool.replace(pool);
        self.uptime = LocalTime::mills();
    }

    pub fn clear_pool(&mut self) {
        self.pool.take();
    }

    pub fn worker_addr(&self) -> &WorkerAddress {
        &self.worker_addr
    }

    pub fn pool(&self) -> &Option<Arc<BlockClientPool>> {
        &self.pool
    }

    pub fn uptime(&self) -> u64 {
        self.uptime
    }

    pub fn set_uptime(&mut self) {
        self.uptime = LocalTime::mills();
    }

    pub async fn rpc(&self, msg: Message) -> FsResult<Message> {
        let client = try_option_ref!(self.client);
        let rep_msg = client.timeout_rpc(self.timeout, msg).await?;
        match rep_msg.check_error_ext::<FsError>() {
            Ok(_) => Ok(rep_msg),
            Err(e) => Err(e.ctx(format!("rpc failed to worker {}", self.worker_addr))),
        }
    }

    pub async fn write_block(
        &self,
        blk: &ExtendedBlock,
        off: i64,
        block_size: i64,
        req_id: i64,
        seq_id: i32,
        chunk_size: i32,
        short_circuit: bool,
    ) -> FsResult<CreateBlockContext> {
        let header = BlockWriteRequest {
            block: ProtoUtils::extend_block_to_pb(blk.clone()),
            off,
            block_size,
            short_circuit,
            client_name: self.client_name.to_string(),
            chunk_size,
        };

        let msg = Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Open)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(header)
            .build();

        let rep = self.rpc(msg).await?;
        let rep_header: BlockWriteResponse = rep.parse_header()?;

        let context = CreateBlockContext {
            id: rep_header.id,
            off: rep_header.off,
            block_size: rep_header.block_size,
            storage_type: StorageType::from(rep_header.storage_type),
            path: rep_header.path,
        };

        Ok(context)
    }

    pub async fn write_data(
        &self,
        buf: DataSlice,
        req_id: i64,
        seq_id: i32,
        header: Option<DataHeaderProto>,
    ) -> CommonResult<()> {
        let mut builder = Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Running)
            .req_id(req_id)
            .seq_id(seq_id)
            .data(buf);

        if let Some(header) = header {
            builder = builder.proto_header(header);
        }

        let msg = builder.build();
        let _ = self.rpc(msg).await?;
        Ok(())
    }

    pub async fn write_flush(&self, pos: i64, req_id: i64, seq_id: i32) -> CommonResult<()> {
        let header = DataHeaderProto {
            offset: pos,
            flush: true,
            is_last: false,
        };

        let msg = Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Running)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(header)
            .build();
        let _ = self.rpc(msg).await?;
        Ok(())
    }

    // Write complete
    pub async fn write_commit(
        &self,
        block: &ExtendedBlock,
        off: i64,
        block_size: i64,
        req_id: i64,
        seq_id: i32,
        cancel: bool,
    ) -> FsResult<()> {
        let header = BlockWriteRequest {
            block: ProtoUtils::extend_block_to_pb(block.clone()),
            off,
            block_size,
            client_name: self.client_name.to_string(),
            ..Default::default()
        };

        let status = if cancel {
            RequestStatus::Cancel
        } else {
            RequestStatus::Complete
        };

        let msg = Builder::new()
            .code(RpcCode::WriteBlock)
            .request(status)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(header)
            .build();

        let _ = self.rpc(msg).await?;
        Ok(())
    }

    // Open a block.
    pub async fn open_block(
        &self,
        conf: &ClientConf,
        block: &ExtendedBlock,
        off: i64,
        len: i64,
        req_id: i64,
        seq_id: i32,
        short_circuit: bool,
    ) -> FsResult<BlockReadContext> {
        let request = BlockReadRequest {
            id: block.id,
            off,
            len,
            chunk_size: conf.read_chunk_size as i32,
            short_circuit,
            enable_read_ahead: conf.enable_read_ahead,
            read_ahead_len: conf.read_ahead_len,
            drop_cache_len: conf.drop_cache_len,
        };

        let msg = Builder::new()
            .code(RpcCode::ReadBlock)
            .request(RequestStatus::Open)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(request)
            .build();

        let rep = self.rpc(msg).await?;
        let rep_header: BlockReadResponse = rep.parse_header()?;

        Ok(BlockReadContext::from_req(rep_header))
    }

    pub async fn read_commit(
        &self,
        block: &ExtendedBlock,
        req_id: i64,
        seq_id: i32,
    ) -> FsResult<()> {
        let request = BlockReadRequest {
            id: block.id,
            ..Default::default()
        };

        let msg = Builder::new()
            .code(RpcCode::ReadBlock)
            .request(RequestStatus::Complete)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(request)
            .build();

        let _ = self.rpc(msg).await?;
        Ok(())
    }

    pub async fn read_data(
        &self,
        req_id: i64,
        seq_id: i32,
        header: Option<DataHeaderProto>,
    ) -> FsResult<DataSlice> {
        let builder = Builder::new()
            .code(RpcCode::ReadBlock)
            .request(RequestStatus::Running)
            .req_id(req_id)
            .seq_id(seq_id);

        let msg = if let Some(header) = header {
            builder.proto_header(header).build()
        } else {
            builder.build()
        };

        let rep = self.rpc(msg).await?;
        Ok(rep.data)
    }
    pub async fn write_blocks_batch(
        &self,
        blocks: &[ExtendedBlock],
        off: i64,
        block_size: i64,
        req_id: i64,
        seq_id: i32,
        chunk_size: i32,
        short_circuit: bool,
    ) -> FsResult<CreateBatchBlockContext> {
        let blocks_pb: Vec<_> = blocks
            .iter()
            .map(|block| ProtoUtils::extend_block_to_pb(block.clone()))
            .collect();

        let req_header = BlocksBatchWriteRequest {
            blocks: blocks_pb,
            off,
            block_size,
            req_id,
            seq_id,
            chunk_size,
            short_circuit,
            client_name: self.client_name.to_string(),
        };

        let msg = Builder::new()
            .code(RpcCode::WriteBlocksBatch)
            .request(RequestStatus::Open)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(req_header)
            .build();

        let rep = self.rpc(msg).await?;
        let rep_header: BlocksBatchWriteResponse = rep.parse_header()?;
        let mut batch_context = CreateBatchBlockContext::new(req_id);

        for response in rep_header.responses {
            let context = CreateBlockContext {
                id: response.id,
                off: response.off,
                block_size: response.block_size,
                storage_type: StorageType::from(response.storage_type),
                path: response.path,
            };
            batch_context.push(context);
        }

        Ok(batch_context)
    }

    pub async fn write_commit_batch(
        &self,
        blocks: &[ExtendedBlock],
        off: i64,
        block_size: i64,
        req_id: i64,
        seq_id: i32,
        cancel: bool,
    ) -> FsResult<()> {
        // Convert blocks to protobuf
        let blocks_pb: Vec<_> = blocks
            .iter()
            .map(|block| ProtoUtils::extend_block_to_pb(block.clone()))
            .collect();

        let header = BlocksBatchCommitRequest {
            blocks: blocks_pb,
            off,
            block_size,
            req_id,
            seq_id,
            cancel,
        };

        let status = if cancel {
            RequestStatus::Cancel
        } else {
            RequestStatus::Complete
        };

        let msg = Builder::new()
            .code(RpcCode::WriteBlocksBatch)
            .request(status)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(header)
            .build();

        let _ = self.rpc(msg).await?;
        Ok(())
    }

    pub async fn write_files_batch(
        &self,
        files: &[(&Path, &str)],
        req_id: i64,
        seq_id: i32,
    ) -> CommonResult<()> {
        let file_data: Vec<_> = files
            .iter()
            .map(|(path, content)| FileWriteData {
                path: path.to_string(),
                content: content.as_bytes().to_vec(),
            })
            .collect();

        let header = FilesBatchWriteRequest {
            files: file_data,
            req_id,
            seq_id,
        };

        let msg = Builder::new()
            .code(RpcCode::WriteBlocksBatch)
            .request(RequestStatus::Running)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(header)
            .build();

        let _ = self.rpc(msg).await?;
        Ok(())
    }
}

impl Drop for BlockClient {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            if let Some(moved_client) = self.client.take() {
                let client = BlockClient {
                    client: Some(moved_client),
                    client_name: std::mem::take(&mut self.client_name),
                    timeout: self.timeout,
                    pool: Some(pool.clone()),
                    worker_addr: std::mem::take(&mut self.worker_addr),
                    uptime: self.uptime,
                };

                pool.release(client);
            }
        }
    }
}
