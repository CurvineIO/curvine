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

use crate::block::{BlockReadContext, CreateBlockContext};
use crate::file::FsContext;
use bytes::BytesMut;
use curvine_common::conf::ClientConf;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    BlockReadRequest, BlockReadResponse, BlockWriteRequest, BlockWriteResponse, DataHeaderProto,
};
use curvine_common::state::{ExtendedBlock, StorageType, WorkerAddress};
use curvine_common::utils::ProtoUtils;
use curvine_common::FsResult;
use orpc::client::RpcClient;
use orpc::message::{Builder, Message, RequestStatus};
use orpc::sys::DataSlice;
use orpc::{err_box, CommonResult};
use std::time::Duration;

pub struct BlockClient {
    client: RpcClient,
    client_name: String,
    timeout: Duration,
}

impl BlockClient {
    pub fn new(client: RpcClient, context: &FsContext) -> Self {
        Self {
            client,
            client_name: context.clone_client_name(),
            timeout: Duration::from_millis(context.conf.client.data_timeout_ms),
        }
    }

    pub async fn rpc(&self, msg: Message) -> FsResult<Message> {
        let rep_msg = self.client.timeout_rpc(self.timeout, msg).await?;
        if !rep_msg.is_success() {
            err_box!(rep_msg.to_error_msg())
        } else {
            Ok(rep_msg)
        }
    }

    pub async fn write_block(
        &self,
        blk: &ExtendedBlock,
        off: i64,
        len: i64,
        req_id: i64,
        seq_id: i32,
        chunk_size: i32,
        short_circuit: bool,
        pipeline: bool,
        locations: Vec<WorkerAddress>,
        location_index: i32,
    ) -> FsResult<CreateBlockContext> {
        let header = BlockWriteRequest {
            id: blk.id,
            off,
            len,
            file_type: blk.file_type.into(),
            storage_type: blk.storage_type.into(),
            short_circuit,
            client_name: self.client_name.to_string(),
            chunk_size,
            pipeline,
            locations: locations
                .iter()
                .map(|addr| ProtoUtils::worker_address_to_pb(addr))
                .collect(),
            location_index,
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
            len: rep_header.len,
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
        len: i64,
        req_id: i64,
        seq_id: i32,
        cancel: bool,
    ) -> FsResult<()> {
        let header = BlockWriteRequest {
            id: block.id,
            off,
            len,
            file_type: block.file_type.into(),
            storage_type: block.storage_type.into(),
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
    ) -> FsResult<BytesMut> {
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
        match rep.data {
            DataSlice::Buffer(v) => Ok(v),
            _ => err_box!("Unsupported type"),
        }
    }
}
