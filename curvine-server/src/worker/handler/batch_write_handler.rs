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
use crate::worker::handler::WriteContext;
use crate::worker::handler::WriteHandler;
use curvine_common::error::FsError;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    BlockWriteRequest, BlockWriteResponse, BlocksBatchCommitRequest, BlocksBatchCommitResponse,
    BlocksBatchWriteRequest, BlocksBatchWriteResponse, FilesBatchWriteRequest,
    FilesBatchWriteResponse,
};
use curvine_common::state::ExtendedBlock;
use curvine_common::utils::ProtoUtils;
use curvine_common::FsResult;
use orpc::err_box;
use orpc::handler::MessageHandler;
use orpc::io::LocalFile;
use orpc::message::{Builder, Message, RequestStatus};
use orpc::sys::DataSlice;

pub struct BatchWriteHandler {
    pub(crate) store: BlockStore,
    pub(crate) context: Option<Vec<WriteContext>>,
    pub(crate) file: Option<Vec<LocalFile>>,
    pub(crate) is_commit: bool,
    pub(crate) write_handler: WriteHandler,
}

impl BatchWriteHandler {
    pub fn new(store: BlockStore) -> Self {
        let store_clone = store.clone();
        Self {
            store,
            context: None,
            file: None,
            is_commit: false,
            write_handler: WriteHandler::new(store_clone),
        }
    }

    fn check_context(context: &WriteContext, msg: &Message) -> FsResult<()> {
        if context.req_id != msg.req_id() {
            return err_box!(
                "Request id mismatch, expected {}, actual {}",
                context.req_id,
                msg.req_id()
            );
        }
        Ok(())
    }

    fn commit_block(&self, block: &ExtendedBlock, commit: bool) -> FsResult<()> {
        if commit {
            self.store.finalize_block(block)?;
        } else {
            self.store.abort_block(block)?;
        }
        Ok(())
    }

    pub fn open_batch(&mut self, msg: &Message) -> FsResult<Message> {
        let header: BlocksBatchWriteRequest = msg.parse_header()?;
        let mut responses = Vec::with_capacity(header.blocks.len());

        // Initialize ONCE with capacity
        self.file = Some(Vec::with_capacity(header.blocks.len()));
        self.context = Some(Vec::with_capacity(header.blocks.len()));

        for (i, block_proto) in header.blocks.into_iter().enumerate() {
            let unique_req_id = msg.req_id() + i as i64;
            // Create a single BlockWriteRequest from the block
            let header = BlockWriteRequest {
                block: block_proto,
                off: header.off,
                block_size: header.block_size,
                short_circuit: header.short_circuit,
                client_name: header.client_name.clone(),
                chunk_size: header.chunk_size,
            };

            // Create single request message for each block
            let single_msg_req = Builder::new()
                .code(msg.code())
                .request(RequestStatus::Open)
                .req_id(unique_req_id)
                .seq_id(msg.seq_id())
                .proto_header(header)
                .build();

            let response = self.write_handler.open(&single_msg_req)?;
            let block_response: BlockWriteResponse = response.parse_header()?;
            responses.push(block_response);

            // Extract file and context from handler and store in batch vectors
            if let Some(file) = self.write_handler.file.take() {
                self.file.as_mut().unwrap().push(file);
            }
            if let Some(context) = self.write_handler.context.take() {
                self.context.as_mut().unwrap().push(context);
            }
        }
        let batch_response = BlocksBatchWriteResponse { responses };

        Ok(Builder::success(msg).proto_header(batch_response).build())
    }

    pub fn complete_batch(&mut self, msg: &Message, commit: bool) -> FsResult<Message> {
        // Parse the flattened batch request
        let header: BlocksBatchCommitRequest = msg.parse_header()?;
        let mut results = Vec::new();

        if self.is_commit {
            return if !msg.data.is_empty() {
                err_box!("The block has been committed and data cannot be written anymore.")
            } else {
                Ok(msg.success())
            };
        }

        // Process each block independently
        for (i, block_proto) in header.blocks.into_iter().enumerate() {
            if let Some(context) = self.context.take() {
                if context.len() > 1 {
                    Self::check_context(&context[i], msg)?;
                }
            }

            // Flush and close the file (same as complete)
            let file = self.file.take();
            if let Some(mut file) = file {
                if file.len() > 1 {
                    file[i].flush()?;
                    drop(file);
                }
            }

            // Create context manually for each block from block_proto
            let unique_req_id = msg.req_id() + i as i64;
            let context = WriteContext {
                block: ProtoUtils::extend_block_from_pb(block_proto),
                req_id: unique_req_id,
                chunk_size: header.block_size as i32,
                short_circuit: false,
                off: header.off,
                block_size: header.block_size,
            };

            // Validate block length (same as complete)
            if context.block.len > context.block_size {
                return err_box!(
                    "Invalid write offset: {}, block size: {}",
                    context.off,
                    context.block_size
                );
            }

            // Commit the block
            self.commit_block(&context.block, commit)?;
            results.push(true);
        }
        self.is_commit = true;
        let batch_response = BlocksBatchCommitResponse { results };

        Ok(Builder::success(msg).proto_header(batch_response).build())
    }

    pub fn write_batch(&mut self, msg: &Message) -> FsResult<Message> {
        let header: FilesBatchWriteRequest = msg.parse_header()?;
        let mut results = Vec::new();

        for (i, file_data) in header.files.iter().enumerate() {
            // Convert bytes to DataSlice
            let data_slice = DataSlice::Bytes(bytes::Bytes::from(file_data.clone().content));

            let unique_req_id = header.req_id + i as i64;
            // Create a temporary message for each file
            let single_msg = Builder::new()
                .code(RpcCode::WriteBlock)
                .request(RequestStatus::Running)
                .req_id(unique_req_id)
                .seq_id(header.seq_id)
                .data(data_slice)
                .build();

            // Transfer to handler
            let file = std::mem::take(&mut self.file.as_mut().unwrap()[i]);
            let context = std::mem::take(&mut self.context.as_mut().unwrap()[i]);

            self.write_handler.file = Some(file);
            self.write_handler.context = Some(context);
            
            let response = self.write_handler.write(&single_msg);

            // Transfer back
            let file = self.write_handler.file.take().unwrap();
            let context = self.write_handler.context.take().unwrap();
            
            self.file.as_mut().unwrap()[i] = file;
            self.context.as_mut().unwrap()[i] = context;

            results.push(response.is_ok());
        }

        let batch_response = FilesBatchWriteResponse { results };
        Ok(Builder::success(msg).proto_header(batch_response).build())
    }
}

impl MessageHandler for BatchWriteHandler {
    type Error = FsError;
    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let request_status = msg.request_status();
        match request_status {
            // batch operations
            RequestStatus::OpenBatch => self.open_batch(msg),
            RequestStatus::RunningBatch => self.write_batch(msg),
            RequestStatus::CompleteBatch => self.complete_batch(msg, true),
            RequestStatus::CancelBatch => self.complete_batch(msg, false),
            _ => err_box!("Unsupported request type"),
        }
    }
}
