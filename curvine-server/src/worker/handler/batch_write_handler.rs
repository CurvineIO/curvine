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
use crate::worker::{Worker, WorkerMetrics};
use crate::worker::block::BlockStore;
use crate::worker::handler::WriteContext;
use crate::worker::handler::ContainerWriteContext;
use crate::worker::handler::WriteHandler;
use curvine_common::error::FsError;
use curvine_common::proto::ExtendedBlockProto;
use curvine_common::proto::{
    BlockWriteResponse, 
     BlocksBatchCommitResponse,
    ContainerMetadataProto,
    ContainerWriteRequest, ContainerWriteResponse, FileWriteDataProto, SmallFileMetaProto,
    ContainerBlockWriteRequest, ContainerBlockWriteResponse
};
use curvine_common::state::ExtendedBlock;
use curvine_common::state::FileType;
use curvine_common::state::StorageType;
use curvine_common::FsResult;
use curvine_common::utils::ProtoUtils;
use orpc::{err_box, try_option_mut};
use orpc::handler::MessageHandler;
use orpc::io::LocalFile;
use orpc::message::{Builder, Message, RequestStatus};

pub struct BatchWriteHandler {
    pub(crate) store: BlockStore,
    pub(crate) context: Option<ContainerWriteContext>,
    pub(crate) file: Option<Vec<LocalFile>>,
    pub(crate) is_commit: bool,
    pub(crate) _write_handler: WriteHandler,
    // for small file containerization
    pub(crate) container_block_id: i64,
    pub(crate) container_path: String,
    pub(crate) container_files: Vec<SmallFileMetaProto>,
    pub(crate) container_name: String,
    pub(crate) metrics: &'static WorkerMetrics,
}

impl BatchWriteHandler {
    pub fn new(store: BlockStore) -> Self {
        let store_clone = store.clone();
        println!("DEBUG at BatchWriteHandler, call new:()");
        let metrics = Worker::get_metrics();
        Self {
            store,
            context: None,
            file: None,
            is_commit: false,
            _write_handler: WriteHandler::new(store_clone),
            container_block_id: 0,
            container_path: String::new(),
            container_files: Vec::new(),
            container_name: String::new(),
            metrics
        }
    }

    fn check_context(context: &ContainerWriteContext, msg: &Message) -> FsResult<()> {
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

    pub fn open_batch(&mut self, msg: &Message) -> FsResult<Message> {
        println!("DEBUG at BatchWriteHandler, open_batch, start:");
        let context = ContainerWriteContext::from_req(msg)?;
        println!("DEBUG at BatchWriteHandler, open_batch, context: {:?}", context);
        if context.off > context.block_size {
            return err_box!(
                "Invalid write offset: {}, block size: {}",
                context.off,
                context.block_size
            );
        }

        // let header: ContainerBlockWriteRequest = msg.parse_header()?;
        // println!(
        //     "DEBUG at BatchWriteHandler, open_batch, header.blocks: {:?}, block_size: {:?}",
        //     header.clone().block,
        //     header.clone().block_size
        // );
        // self.container_files = header.clone().files_metadata.files;
        self.container_files = context.files_metadata.clone().files;
        // All files are small - pack them into container blocks
        // self.handle_small_files_batch(header.clone().block)?;


        // self.container_block_id = files_metadata.container_block_id;  
        // self.container_path = files_metadata.container_path.clone();  
        // self.container_files = files_metadata.files.clone();

        self.handle_small_files_batch(ProtoUtils::extend_block_to_pb(context.block.clone()))?;

        let container_meta = ContainerMetadataProto {
            container_block_id: self.container_block_id,
            container_path: self.container_path.clone(),
            container_name: self.container_name.clone(),
            files: self.container_files.clone(),
        };

        // let container_meta = ContainerMetadataProto {
        //     container_block_id: context.files_metadata.container_block_id,
        //     container_path: context.files_metadata.container_path.clone(),
        //     container_name: context.files_metadata.container_name.clone(),
        //     files: context.files_metadata.files.clone(),
        // };

        // let block_size = header.block_size;
        let block_size = context.block_size;

        // Return single container block response instead of individual blocks
        let container_response = BlockWriteResponse {
            id: self.container_block_id,
            path: Some(self.container_path.clone()),
            off: 0,
            block_size,
            storage_type: StorageType::default() as i32,
            pipeline_status: None,
        };

        let batch_response = ContainerBlockWriteResponse {
            responses: vec![container_response],
            container_meta,
        };
        
        let label = if context.short_circuit {
            "local"
        } else {
            "remote"
        };

        let _ = self.context.replace(context);
        self.metrics.write_blocks.with_label_values(&[label]).inc();

        println!("DEBUG at BatchWriteHandler, at open_batch, self.context {:?}", self.context);
        Ok(Builder::success(msg).proto_header(batch_response).build())
    }

    fn handle_small_files_batch(&mut self, block: ExtendedBlockProto) -> FsResult<()> {
        println!(
            "Debug at BatchWriteHandler, at handle_small_files_batch, blocks: {:?}",
            block
        );
        println!(
            "Debug at BatchWriteHandler, at handle_small_files_batch, self address: {:p}",
            (self as *const Self)
        );

        // Calculate total size needed for container
        let total_size: i64 = block.block_size;

        let container_block = ExtendedBlock {
            id: self.container_block_id, // Utils::unique_id() as i64,
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
        let container_file = container_meta.create_writer(0, false)?;
        
        println!("DEBUG at BatchWriteHandler, at handle_small_files_batch from {:?} to {:?}",self.container_block_id , container_meta.id());
        self.container_block_id = container_meta.id();
        self.container_path = container_file.path().to_string();
        // Store file metadata for later writes
        println!(
            "Debug at BatchWriteHandler, at handle_small_files_batch, block: {:?}",
            block
        );

        self.file = Some(vec![container_file]);

        println!(
            "Debug at BatchWriteHandler, at handle_small_files_batch, self.container_files: {:?}",
            self.container_files
        );

        Ok(())
    }

    fn complete_container_batch(
        &mut self,
        _block: &ExtendedBlock,
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
            println!(
                "DEBUG at BatchWriteHandler, at complete_container_batch, container_block: {:?}",
                container_block
            );
            self.commit_block(&container_block, commit)?;
        }

        Ok(())
    }

    pub fn complete_batch(&mut self, msg: &Message, commit: bool) -> FsResult<Message> {

        if let Some(context) = self.context.take() {
            Self::check_context(&context, msg)?;
        }
        let context = ContainerWriteContext::from_req(msg)?;


        // Parse the flattened batch request
        // let header: ContainerBlockWriteRequest = msg.parse_header()?;

        if self.is_commit {
            return if !msg.data.is_empty() {
                err_box!("The block has been committed and data cannot be written anymore.")
            } else {
                Ok(msg.success())
            };
        }

        let files_metadata = &context.files_metadata;  
        println!(  
            "DEBUG at BatchWriteHandler, at complete_batch, container_meta: {:?}",  
            files_metadata  
        );  
        self.container_block_id = files_metadata.container_block_id;  
        self.container_path = files_metadata.container_path.clone();  
        self.container_files = files_metadata.files.clone();

        // Container optimization: finalize single container block

        println!(
            "Debug at BatchWriteHandler,at complete_batch, blocks for complete_container_batch: {:?}",
            &context.block
        );
        self.complete_container_batch(&context.block, commit)?;
        Ok(msg.success())
    }

    
    pub fn write_batch(&mut self, msg: &Message) -> FsResult<Message> {
        println!("DEBUG at BatchWriteHandler, at write_batch, start",);
        let context = try_option_mut!(self.context);
        Self::check_context(context, msg)?;

        let header: ContainerWriteRequest = msg.parse_header()?;

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

        // let results = vec![true; header.files.len()];

        // let updated_meta = ContainerMetadataProto {
        //     container_block_id: self.container_block_id,
        //     container_path: self.container_path.clone(),
        //     container_name: self.container_name.clone(),
        //     files: self.container_files.clone(),
        // };

        // println!(
        //     "DEBUG at BatchWriteHandler, updated_meta {:?}",
        //     updated_meta
        // );

        Ok(msg.success())
    }

    fn write_container_batch(&mut self, files: &[FileWriteDataProto]) -> FsResult<()> {
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
