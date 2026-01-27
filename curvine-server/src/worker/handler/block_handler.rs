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

use crate::worker::block::BlockStore;
use crate::worker::handler::BlockHandler::{BatchWriter, PipelineWriter, Reader, Writer};
use crate::worker::handler::WriteContext;
use crate::worker::handler::{BatchWriteHandler, PipelineWriteHandler, ReadHandler, WriteHandler};
use curvine_client::file::FsContext;
use curvine_common::error::FsError;
use curvine_common::fs::RpcCode;
use curvine_common::FsResult;
use log::warn;
use orpc::error::ErrorExt;
use orpc::handler::MessageHandler;
use orpc::message::RequestStatus as ProtoRequestStatus;
use orpc::message::{Builder, Message, ResponseStatus};
use orpc::{err_box, CommonResult};
use std::sync::Arc;

pub enum BlockHandler {
    Writer(WriteHandler),
    PipelineWriter(PipelineWriteHandler),
    Reader(ReadHandler),
    BatchWriter(BatchWriteHandler),
}

impl BlockHandler {
    pub fn new(msg: &Message, store: BlockStore, fs_context: Arc<FsContext>) -> CommonResult<Self> {
        let code = RpcCode::from(msg.code());
        if code != RpcCode::WriteBlock {
            let handler = match code {
                RpcCode::ReadBlock => Reader(ReadHandler::new(store)),
                RpcCode::WriteBlocksBatch => BatchWriter(BatchWriteHandler::new(store, fs_context)),
                code => return err_box!("Unsupported request type: {:?}", code),
            };
            return Ok(handler);
        }

        if msg.request_status() == ProtoRequestStatus::Open {
            let ctx = WriteContext::from_req(msg)?;
            let pipeline_enabled = fs_context.cluster_conf().client.enable_pipeline_write;
            if ctx.has_pipeline_stream() && pipeline_enabled {
                let queue_size = fs_context.cluster_conf().worker.pipeline_queue_size;
                let pipeline_write_delay_ms =
                    fs_context.cluster_conf().worker.pipeline_write_delay_ms;
                return Ok(PipelineWriter(PipelineWriteHandler::new(
                    store,
                    fs_context,
                    crate::worker::Worker::get_metrics(),
                    queue_size,
                    pipeline_write_delay_ms,
                )));
            }
            if ctx.has_pipeline_stream() && !pipeline_enabled {
                warn!(
                    "BlockHandler::new_with_request: pipeline_stream ignored because pipeline write is disabled, block_id={}",
                    ctx.block.id
                );
            }
        }

        Ok(Writer(WriteHandler::new(store)))
    }
}

impl MessageHandler for BlockHandler {
    type Error = FsError;

    fn is_sync(&self, msg: &Message) -> bool {
        match self {
            Writer(h) => h.is_sync(msg),
            PipelineWriter(h) => h.is_sync(msg),
            Reader(_) => true,
            BatchWriter(_) => true,
        }
    }

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let result = match self {
            Writer(h) => h.handle(msg),
            PipelineWriter(h) => h.handle(msg),
            Reader(h) => h.handle(msg),
            BatchWriter(h) => h.handle(msg),
        };
        result.or_else(|e| Ok(msg.error_ext(&e)))
    }

    async fn async_handle(&mut self, msg: Message) -> FsResult<Message> {
        let error_ctx = (msg.code(), msg.request_status(), msg.req_id(), msg.seq_id());

        let result = match self {
            Writer(h) => h.async_handle(msg).await,
            PipelineWriter(h) => h.async_handle(msg).await,
            Reader(h) => h.async_handle(msg).await,
            BatchWriter(h) => h.async_handle(msg).await,
        };

        result.or_else(|e| {
            let err_msg = Builder::new()
                .code(error_ctx.0)
                .request(error_ctx.1)
                .response(ResponseStatus::Error)
                .req_id(error_ctx.2)
                .seq_id(error_ctx.3)
                .data(orpc::sys::DataSlice::Buffer(e.encode()))
                .build();
            Ok(err_msg)
        })
    }
}
