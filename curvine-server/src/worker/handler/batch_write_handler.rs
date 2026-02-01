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

// use crate::master::meta::inode::SmallFileMeta;
use crate::worker::block::BlockStore;
use crate::worker::handler::WriteContext;
use crate::worker::handler::WriteHandler;
use curvine_client::block;
use curvine_common::error::FsError;
use curvine_common::fs::RpcCode;
use curvine_common::proto::ExtendedBlockProto;
use curvine_common::proto::{
    BlockWriteRequest, BlockWriteResponse, BlocksBatchCommitRequest, BlocksBatchCommitResponse,
    BlocksBatchWriteRequest, BlocksBatchWriteResponse, ContainerMetadata, FileWriteData,
    FilesBatchWriteRequest, FilesBatchWriteResponse, SmallFileMetaProto,
};
use curvine_common::state::ExtendedBlock;
use curvine_common::state::FileType;
use curvine_common::state::StorageType;
use curvine_common::utils::ProtoUtils;
use curvine_common::FsResult;
use orpc::common::Utils;
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
    // for small file containerization
    pub(crate) container_block_id: i64,
    pub(crate) container_path: String,
    pub(crate) container_files: Vec<SmallFileMetaProto>,
    pub(crate) container_name: String
}

impl BatchWriteHandler {
    pub fn new(store: BlockStore) -> Self {
        let store_clone = store.clone();
        println!("DEBUG at BatchWriteHandler, call new:()");
        Self {
            store,
            context: None,
            file: None,
            is_commit: false,
            write_handler: WriteHandler::new(store_clone),
            container_block_id: 0,
            container_path: String::new(),
            container_files: Vec::new(),
            container_name: String::new()
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
        println!("DEBUG at BatchWriteHanlder, start commit_block");
        if commit {
            self.store.finalize_block(block)?;
        } else {
            self.store.abort_block(block)?;
        }
        Ok(())
    }

    // pub fn open_batch(&mut self, msg: &Message) -> FsResult<Message> {
    //     let header: BlocksBatchWriteRequest = msg.parse_header()?;
    //     let mut responses = Vec::with_capacity(header.blocks.len());

    //     // Initialize ONCE with capacity
    //     self.file = Some(Vec::with_capacity(header.blocks.len()));
    //     self.context = Some(Vec::with_capacity(header.blocks.len()));

    //     for (i, block_proto) in header.blocks.into_iter().enumerate() {
    //         let unique_req_id = msg.req_id() + i as i64;
    //         // Create a single BlockWriteRequest from the block
    //         let header = BlockWriteRequest {
    //             block: block_proto,
    //             off: header.off,
    //             block_size: header.block_size,
    //             short_circuit: header.short_circuit,
    //             client_name: header.client_name.clone(),
    //             chunk_size: header.chunk_size,
    //         };

    //         // Create single request message for each block
    //         let single_msg_req = Builder::new()
    //             .code(msg.code())
    //             .request(RequestStatus::Open)
    //             .req_id(unique_req_id)
    //             .seq_id(msg.seq_id())
    //             .proto_header(header)
    //             .build();

    //         let response = self.write_handler.open(&single_msg_req)?;
    //         let block_response: BlockWriteResponse = response.parse_header()?;
    //         responses.push(block_response);

    //         // Extract file and context from handler and store in batch vectors
    //         if let Some(file) = self.write_handler.file.take() {
    //             self.file.as_mut().unwrap().push(file);
    //         }
    //         if let Some(context) = self.write_handler.context.take() {
    //             self.context.as_mut().unwrap().push(context);
    //         }
    //     }
    //     let batch_response = BlocksBatchWriteResponse { responses };

    //     Ok(Builder::success(msg).proto_header(batch_response).build())
    // }

    pub fn open_batch(&mut self, msg: &Message) -> FsResult<Message> {
        let header: BlocksBatchWriteRequest = msg.parse_header()?;
        println!(
            "DEBUG at BatchWriteHandler, open_batch, header.blocks: {:?}, block_size: {:?}",
            header.clone().blocks,
            header.clone().block_size
        );
        self.container_files = header.clone().files_metadata.unwrap().files;
        // All files are small - pack them into container blocks
        self.handle_small_files_batch(header.clone().blocks)?;

        let container_meta = ContainerMetadata {
            container_block_id: self.container_block_id,
            container_path: self.container_path.clone(),
            container_name: self.container_name.clone(),
            files: self.container_files.clone(),
        };

        let block_size = header.block_size;

        // Return single container block response instead of individual blocks
        let container_response = BlockWriteResponse {
            id: self.container_block_id,
            path: Some(self.container_path.clone()),
            off: 0,
            block_size: block_size,
            storage_type: StorageType::default() as i32,
        };

        let batch_response = BlocksBatchWriteResponse {
            responses: vec![container_response],
            container_meta: Some(container_meta),
        };

        Ok(Builder::success(msg).proto_header(batch_response).build())
    }

    fn handle_small_files_batch(&mut self, blocks: Vec<ExtendedBlockProto>) -> FsResult<()> {
        println!(
            "Debug at BatchWriteHandler, at handle_small_files_batch, blocks: {:?}",
            blocks
        );
        println!(
            "Debug at BatchWriteHandler, at handle_small_files_batch, self address: {:p}",
            (self as *const Self)
        );

        // Calculate total size needed for container
        let total_size: i64 = blocks.iter().map(|b| b.block_size).sum();

        let container_block = ExtendedBlock {
            id: self.container_block_id ,// Utils::unique_id() as i64,
            len: total_size,
            storage_type: StorageType::default(),
            file_type: FileType::Container, // New file type
            ..Default::default()
        };

        println!(
            "Debug at BatchWriteHandler, at handle_small_files_batch, container_block: {:?}",
            container_block
        );

        // Use existing open_block method like WriteHandler
        let container_meta = self.store.open_block(&container_block)?;
        let mut container_file = container_meta.create_writer(0, false)?;

        self.container_block_id = container_meta.id();
        self.container_path = container_file.path().to_string();
        // Store file metadata for later writes
        println!(
            "Debug at BatchWriteHandler, at handle_small_files_batch, blocks: {:?}",
            blocks
        );
        // self.container_files = blocks
        //     .iter()
        //     .enumerate()
        //     .map(|(i, block)| SmallFileMeta {
        //         offset: blocks[0..i].iter().map(|b| b.block_size).sum(),
        //         len: block.block_size,
        //         block_index: 0,
        //         mtime: 0,
        //     })
        //     .collect();

        self.file = Some(vec![container_file]);

        println!(
            "Debug at BatchWriteHandler, at handle_small_files_batch, self.container_files: {:?}",
            self.container_files
        );
        // // Create single container block
        // let container_path = format!("container_{}", Utils::unique_id());
        // // let container_file = self
        // //     .store
        // //     .create_container_block(&container_path, total_size)?;
        // let container_meta = self
        //     .store
        //     .create_container_block(&container_path, total_size)?;
        // let container_file = container_meta.create_writer(0, false)?;
        // // Store file metadata for later writes
        // self.container_files = blocks
        //     .iter()
        //     .enumerate()
        //     .map(|(i, block)| SmallFileMeta {
        //         offset: blocks[0..i].iter().map(|b| b.block_size).sum(),
        //         len: block.block_size,
        //         block_index: 0, // All in container block 0
        //         mtime: 0,       // will be updated later
        //     })
        //     .collect();

        // self.file = Some(vec![container_file]);
        // self.container_block_id = container_meta.id();

        Ok(())
    }

    // pub fn complete_batch(&mut self, msg: &Message, commit: bool) -> FsResult<Message> {
    //     println!("DEBUG at BatchWriteHandler: start complete_batch");
    //     // Parse the flattened batch request
    //     let header: BlocksBatchCommitRequest = msg.parse_header()?;
    //     let mut results: Vec<bool> = Vec::new();

    //     if self.is_commit {
    //         return if !msg.data.is_empty() {
    //             err_box!("The block has been committed and data cannot be written anymore.")
    //         } else {
    //             Ok(msg.success())
    //         };
    //     }

    //     // Process each block independently
    //     for (i, block_proto) in header.blocks.into_iter().enumerate() {
    //         if let Some(context) = self.context.take() {
    //             if context.len() > 1 {
    //                 Self::check_context(&context[i], msg)?;
    //             }
    //         }

    //         // Flush and close the file (same as complete)
    //         let file = self.file.take();
    //         if let Some(mut file) = file {
    //             if file.len() > 1 {
    //                 file[i].flush()?;
    //                 drop(file);
    //             }
    //         }

    //         // Create context manually for each block from block_proto
    //         let unique_req_id = msg.req_id() + i as i64;
    //         let context = WriteContext {
    //             block: ProtoUtils::extend_block_from_pb(block_proto),
    //             req_id: unique_req_id,
    //             chunk_size: header.block_size as i32,
    //             short_circuit: false,
    //             off: header.off,
    //             block_size: header.block_size,
    //         };

    //         // Validate block length (same as complete)
    //         if context.block.len > context.block_size {
    //             return err_box!(
    //                 "Invalid write offset: {}, block size: {}",
    //                 context.off,
    //                 context.block_size
    //             );
    //         }

    //         // Commit the block
    //         self.commit_block(&context.block, commit)?;
    //         results.push(true);
    //     }
    //     self.is_commit = true;
    //     let batch_response = BlocksBatchCommitResponse { results };

    //     Ok(Builder::success(msg).proto_header(batch_response).build())
    // }

    fn complete_container_batch(
        &mut self,
        blocks: &[ExtendedBlockProto],
        commit: bool,
    ) -> FsResult<()> {
        println!(
            "DEBUG at BatchWriteHanlder, start complete_container_batch with commit={:?}",
            commit
        );
        println!(
            "Debug at BatchWriteHandler,at complete_container_batch, self address: {:p}",
            (self as *const Self)
        );
        println!(
            "DEBUG at BatchWriteHanlder, at complete_container_batch, self.container_files: {:?}",
            self.container_files
        );
        println!(
            "DEBUG at BatchWriteHanlder, at complete_container_batch, self.file: {:?}",
            self.file
        );
        // Flush the container file
        if let Some(ref mut files) = self.file {
            if let Some(container_file) = files.first_mut() {
                println!(
                    "DEBUG at BatchWriteHanlder, start complete_container_batch flush = {:?}",
                    container_file.path()
                );
                container_file.flush()?;
            }
        }

        // Update container metadata with final file sizes
        // (This would be done if we need to update the container header)

        // Commit the container block
        if let Some(container_file_meta) = self.container_files.last() {
            let container_block = ExtendedBlock {
                id: self.container_block_id,
                len: container_file_meta.offset + container_file_meta.len,
                storage_type: StorageType::default(),
                file_type: FileType::Container,
                ..Default::default()
            };
            println!("DEBUG at BatchWriteHandler, at complete_container_batch, container_block: {:?}", container_block);
            self.commit_block(&container_block, commit)?;
        }

        Ok(())
    }

    pub fn complete_batch(&mut self, msg: &Message, commit: bool) -> FsResult<Message> {
        println!(
            "Debug at BatchWriteHandler,at complete_batch, self.file: {:?}",
            self.file
        );
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

        if let Some(ref container_meta) = header.container_meta {
            println!("DEBUG at BatchWriteHandler, at complete_batch, container_meta: {:?}", container_meta);
            self.container_block_id = container_meta.container_block_id;
            self.container_path = container_meta.container_path.clone();
            self.container_files = container_meta.files.clone();
        }

        // Container optimization: finalize single container block

        println!(
            "Debug at BatchWriteHandler,at complete_batch, blocks for complete_container_batch: {:?}",
            &header.blocks
        );
        self.complete_container_batch(&header.blocks, commit)?;
        results = vec![true; header.blocks.len()];
        let batch_response: BlocksBatchCommitResponse = BlocksBatchCommitResponse { results };

        Ok(Builder::success(msg).proto_header(batch_response).build())
    }

    // pub fn write_batch(&mut self, msg: &Message) -> FsResult<Message> {
    //     let header: FilesBatchWriteRequest = msg.parse_header()?;
    //     let mut results = Vec::new();

    //     for (i, file_data) in header.files.iter().enumerate() {
    //         // Convert bytes to DataSlice
    //         let data_slice = DataSlice::Bytes(bytes::Bytes::from(file_data.clone().content));

    //         let unique_req_id = header.req_id + i as i64;
    //         // Create a temporary message for each file
    //         let single_msg = Builder::new()
    //             .code(RpcCode::WriteBlock)
    //             .request(RequestStatus::Running)
    //             .req_id(unique_req_id)
    //             .seq_id(header.seq_id)
    //             .data(data_slice)
    //             .build();

    //         // Transfer to handler
    //         #[allow(clippy::mem_replace_with_default)]
    //         let file = std::mem::replace(
    //             &mut self.file.as_mut().unwrap()[i],
    //             LocalFile::place_holder(),
    //         );
    //         #[allow(clippy::mem_replace_with_default)]
    //         let context = std::mem::replace(
    //             &mut self.context.as_mut().unwrap()[i],
    //             WriteContext::place_holder(),
    //         );

    //         self.write_handler.file = Some(file);
    //         self.write_handler.context = Some(context);

    //         let response = self.write_handler.write(&single_msg);

    //         // Transfer back
    //         let file = self.write_handler.file.take().unwrap();
    //         let context = self.write_handler.context.take().unwrap();

    //         self.file.as_mut().unwrap()[i] = file;
    //         self.context.as_mut().unwrap()[i] = context;

    //         results.push(response.is_ok());
    //     }

    //     let batch_response = FilesBatchWriteResponse { results };
    //     Ok(Builder::success(msg).proto_header(batch_response).build())
    // }
    pub fn write_batch(&mut self, msg: &Message) -> FsResult<Message> {
        let header: FilesBatchWriteRequest = msg.parse_header()?;
        let mut results = Vec::new();

        if let Some(ref container_meta) = header.container_meta {
            println!(
                "DEBUG at BatchWriteHandler, at write_batch, container_meta= {:?}",
                container_meta
            );
            self.container_block_id = container_meta.container_block_id;
            self.container_path = container_meta.container_path.clone();
            self.container_files = container_meta.files.clone();
            self.container_name = container_meta.container_name.clone();
        }
        // Container optimization: write all files to single container
        self.write_container_batch(&header.files)?;

        results = vec![true; header.files.len()];

        let updated_meta = ContainerMetadata {
            container_block_id: self.container_block_id,
            container_path: self.container_path.clone(),
            container_name: self.container_name.clone(),
            files: self.container_files.clone(),
        };

        println!("DEBUG at BatchWriteHandler, updated_meta {:?}", updated_meta);

        let batch_response = FilesBatchWriteResponse {
            results,
            container_meta: Some(updated_meta),
        };
        Ok(Builder::success(msg).proto_header(batch_response).build())
    }

    fn write_container_batch(&mut self, files: &[FileWriteData]) -> FsResult<()> {
        let mut offset = 0;

        // Write all files to container sequentially
        for (i, file_data) in files.iter().enumerate() {
            // Update file metadata with actual offset
            if let Some(container_file) = &mut self.file {
                if let Some(file) = container_file.get_mut(0) {
                    file.write_all(&file_data.content)?;

                    // Update container_files metadata
                    if i < self.container_files.len() {
                        self.container_files[i].offset = offset;
                        self.container_files[i].len = file_data.content.len() as i64;
                        offset += file_data.content.len() as i64;
                    }
                }
            }
        }

        // update block len

        if let Some(files) = self.file.as_mut() {  
            if let Some(first_file) = files.first_mut() {  
                let current_pos = first_file.pos();  
                // This will update both the file system and the len field  
                first_file.resize(true, 0, current_pos, 0)?;  
            }  
        }

        println!(
            "DEBUG at BatchWriteHandler, at write_container_batch, self.file.path: {:?}",
            self.file.as_mut().unwrap().first()
        );

        println!(
            "DEBUG at BatchWriteHandler, at write_container_batch, self.file.path: {:?}",
            self.file.as_mut().unwrap().first()
        );

        Ok(())
    }
}

impl MessageHandler for BatchWriteHandler {
    type Error = FsError;
    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let request_status = msg.request_status();
        match request_status {
            // batch operations
            RequestStatus::Open => self.open_batch(msg),
            RequestStatus::Running => self.write_batch(msg),
            RequestStatus::Complete => self.complete_batch(msg, true),
            RequestStatus::Cancel => self.complete_batch(msg, false),
            _ => err_box!("Unsupported request type"),
        }
    }
}
