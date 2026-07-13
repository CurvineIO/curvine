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
    BlocksBatchWriteRequest, BlocksBatchWriteResponse, DataHeaderProto, FilesBatchWriteRequest,
    FilesBatchWriteResponse,
};
use curvine_common::utils::ProtoUtils;
use curvine_common::FsResult;
use orpc::err_box;
use orpc::handler::MessageHandler;
use orpc::io::{BlockDevice, BlockIO};
use orpc::message::{Builder, Message, RequestStatus};
use orpc::sys::DataSlice;
use orpc::CommonResult;

pub struct BatchWriteHandler {
    pub(crate) store: BlockStore,
    pub(crate) context: Option<Vec<WriteContext>>,
    pub(crate) file: Option<Vec<Option<BlockDevice>>>,
    pub(crate) is_commit: bool,
    pub(crate) write_handler: WriteHandler,
}

impl BatchWriteHandler {
    pub fn new(store: BlockStore) -> CommonResult<Self> {
        let store_clone = store.clone();
        Ok(Self {
            store,
            context: None,
            file: None,
            is_commit: false,
            write_handler: WriteHandler::new(store_clone)?,
        })
    }

    fn check_context(context: &WriteContext, expected_req_id: i64) -> FsResult<()> {
        if context.req_id != expected_req_id {
            return err_box!(
                "Request id mismatch, expected {}, actual {}",
                context.req_id,
                expected_req_id
            );
        }
        Ok(())
    }

    fn abort_open_contexts(store: &BlockStore, contexts: &[WriteContext]) {
        for context in contexts {
            if let Err(e) = store.abort_block(&context.block) {
                log::warn!(
                    "failed to abort batch-opened block {} after open error: {}",
                    context.block.id,
                    e
                );
            }
        }
    }

    pub fn open_batch(&mut self, msg: &Message) -> FsResult<Message> {
        let header: BlocksBatchWriteRequest = msg.parse_header()?;
        let mut responses = Vec::with_capacity(header.blocks.len());
        let mut files = Vec::with_capacity(header.blocks.len());
        let mut contexts = Vec::with_capacity(header.blocks.len());
        self.is_commit = false;

        for (i, block_proto) in header.blocks.iter().cloned().enumerate() {
            let unique_req_id = msg.req_id() + i as i64;
            // Create a single BlockWriteRequest from the block
            let header = BlockWriteRequest {
                block: block_proto,
                off: header.off,
                block_size: header.block_size,
                short_circuit: header.short_circuit,
                client_name: header.client_name.clone(),
                chunk_size: header.chunk_size,
                pipeline_stream: Vec::new(),
            };

            // Create single request message for each block
            let single_msg_req = Builder::new()
                .code(msg.code())
                .request(RequestStatus::Open)
                .req_id(unique_req_id)
                .seq_id(msg.seq_id())
                .proto_header(header)
                .build();

            let response = match self.write_handler.open(&single_msg_req) {
                Ok(response) => response,
                Err(e) => {
                    Self::abort_open_contexts(&self.store, &contexts);
                    return Err(e);
                }
            };

            // Extract file and context from handler and store in batch vectors
            let Some(context) = self.write_handler.context.take() else {
                Self::abort_open_contexts(&self.store, &contexts);
                return err_box!(
                    "batch open did not create write context for block index {}",
                    i
                );
            };
            let block_response: BlockWriteResponse = match response.parse_header() {
                Ok(response) => response,
                Err(e) => {
                    if let Err(abort_err) = self.store.abort_block(&context.block) {
                        log::warn!(
                            "failed to abort batch-opened block {} after response parse error: {}",
                            context.block.id,
                            abort_err
                        );
                    }
                    Self::abort_open_contexts(&self.store, &contexts);
                    return Err(e.into());
                }
            };
            responses.push(block_response);
            files.push(self.write_handler.file.take());
            contexts.push(context);
        }
        self.file = Some(files);
        self.context = Some(contexts);
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

        let Some(contexts) = self.context.as_ref() else {
            return err_box!("batch commit without open context");
        };
        let Some(files) = self.file.as_mut() else {
            return err_box!("batch commit without open files");
        };
        if contexts.len() != header.blocks.len() || files.len() != header.blocks.len() {
            return err_box!(
                "batch commit block count mismatch: blocks={}, contexts={}, files={}",
                header.blocks.len(),
                contexts.len(),
                files.len()
            );
        }

        let mut commit_blocks = Vec::with_capacity(header.blocks.len());
        for (i, block_proto) in header.blocks.into_iter().enumerate() {
            let Some(context) = contexts.get(i) else {
                return err_box!("missing batch write context at index {}", i);
            };
            let expected_req_id = msg.req_id() + i as i64;
            Self::check_context(context, expected_req_id)?;

            let block = ProtoUtils::extend_block_from_pb(block_proto);
            if block.id != context.block.id
                || block.storage_type != context.block.storage_type
                || block.file_type != context.block.file_type
            {
                return err_box!(
                    "batch commit block mismatch at index {}: opened={:?}, committed={:?}",
                    i,
                    context.block,
                    block
                );
            }
            if block.len > context.block_size {
                return err_box!(
                    "Invalid write offset: {}, block size: {}",
                    block.len,
                    context.block_size
                );
            }
            commit_blocks.push(block);
        }

        for file in files.iter_mut() {
            if let Some(file) = file.as_mut() {
                file.flush()?;
            }
        }
        for file in files.iter_mut() {
            if let Some(file) = file.take() {
                drop(file);
            }
        }

        for block in commit_blocks {
            if commit {
                self.store.finalize_block(&block)?;
            } else {
                self.store.abort_block(&block)?;
            }
            results.push(true);
        }
        self.is_commit = true;
        let batch_response = BlocksBatchCommitResponse { results };

        Ok(Builder::success(msg).proto_header(batch_response).build())
    }

    pub fn write_batch(&mut self, msg: &Message) -> FsResult<Message> {
        let header: FilesBatchWriteRequest = msg.parse_header()?;

        // Use drain to extract elements while preserving the vector's allocated memory
        let Some(files_vec) = self.file.as_mut() else {
            return err_box!("batch write without open files");
        };
        let Some(contexts_vec) = self.context.as_mut() else {
            return err_box!("batch write without open context");
        };
        if header.files.len() != files_vec.len() || header.files.len() != contexts_vec.len() {
            return err_box!(
                "batch write file count mismatch: request={}, files={}, contexts={}",
                header.files.len(),
                files_vec.len(),
                contexts_vec.len()
            );
        }
        if files_vec.iter().any(Option::is_none) {
            return err_box!(
                "batch remote write received running data for short-circuit batch writer"
            );
        }

        let files_drain = std::mem::take(files_vec);
        let contexts_drain = std::mem::take(contexts_vec);

        // Process each file in order
        let mut files_iter = files_drain.into_iter();
        let mut contexts_iter = contexts_drain.into_iter();

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

            // Get the next file and context from iterators (preserves original order)
            let Some(file) = files_iter.next() else {
                return err_box!("missing batch write file at index {}", i);
            };
            let Some(file) = file else {
                return err_box!("batch remote write missing file state at index {}", i);
            };
            let Some(context) = contexts_iter.next() else {
                return err_box!("missing batch write context at index {}", i);
            };

            self.write_handler.file = Some(file);
            self.write_handler.context = Some(context);

            if let Err(e) = self.write_handler.write(&single_msg) {
                let Some(file) = self.write_handler.file.take() else {
                    return err_box!(
                        "batch write lost file state after write error at index {}",
                        i
                    );
                };
                let Some(context) = self.write_handler.context.take() else {
                    return err_box!(
                        "batch write lost context state after write error at index {}",
                        i
                    );
                };
                files_vec.push(Some(file));
                contexts_vec.push(context);
                files_vec.extend(files_iter);
                contexts_vec.extend(contexts_iter);
                return Err(e);
            }

            // Collect processed file and context back into the original vectors
            let Some(file) = self.write_handler.file.take() else {
                return err_box!("batch write lost file state at index {}", i);
            };
            let Some(context) = self.write_handler.context.take() else {
                return err_box!("batch write lost context state at index {}", i);
            };

            // Push back to reuse the pre-allocated capacity from open_batch
            files_vec.push(Some(file));
            contexts_vec.push(context);
        }

        let batch_response = FilesBatchWriteResponse {
            results: vec![true; header.files.len()],
        };
        Ok(Builder::success(msg).proto_header(batch_response).build())
    }

    pub fn flush_batch(&mut self, msg: &Message) -> FsResult<Message> {
        let header: DataHeaderProto = msg.parse_header()?;
        if !header.flush {
            return err_box!("batch writer received non-flush WriteBlock running request");
        }

        let Some(files) = self.file.as_mut() else {
            return err_box!("batch flush without open files");
        };
        let Some(contexts) = self.context.as_ref() else {
            return err_box!("batch flush without open context");
        };
        if files.len() != contexts.len() {
            return err_box!(
                "batch flush state mismatch: files={}, contexts={}",
                files.len(),
                contexts.len()
            );
        }
        for (i, (file, context)) in files.iter_mut().zip(contexts.iter()).enumerate() {
            let expected_req_id = msg.req_id() + i as i64;
            Self::check_context(context, expected_req_id)?;
            if let Some(file) = file.as_mut() {
                file.flush()?;
            }
        }

        Ok(msg.success())
    }
}

impl MessageHandler for BatchWriteHandler {
    type Error = FsError;
    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let request_status = msg.request_status();
        match request_status {
            // batch operations
            RequestStatus::Open => self.open_batch(msg),
            RequestStatus::Running if RpcCode::from(msg.code()) == RpcCode::WriteBlock => {
                self.flush_batch(msg)
            }
            RequestStatus::Running => self.write_batch(msg),
            RequestStatus::Complete => self.complete_batch(msg, true),
            RequestStatus::Cancel => self.complete_batch(msg, false),
            _ => err_box!("Unsupported request type"),
        }
    }
}
