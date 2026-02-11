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
use crate::worker::handler::ContainerWriteContext;
use crate::worker::{Worker, WorkerMetrics};
use curvine_common::error::FsError;
use curvine_common::proto::ExtendedBlockProto;
use curvine_common::proto::{
    BlockWriteResponse, ContainerBlockWriteResponse, ContainerWriteRequest, FileWriteDataProto,
    SmallFileMetaProto,
};
use curvine_common::state::ExtendedBlock;
use curvine_common::state::FileType;
use curvine_common::state::StorageType;
use curvine_common::utils::ProtoUtils;
use curvine_common::FsResult;
use orpc::err_box;
use orpc::handler::MessageHandler;
use orpc::io::LocalFile;
use orpc::message::{Builder, Message, RequestStatus};

pub struct BatchWriteHandler {
    pub(crate) store: BlockStore,
    pub(crate) context: Option<ContainerWriteContext>,
    pub(crate) file: Option<LocalFile>,
    pub(crate) is_commit: bool,
    pub(crate) metrics: &'static WorkerMetrics,
}

impl BatchWriteHandler {
    pub fn new(store: BlockStore) -> Self {
        let metrics = Worker::get_metrics();
        Self {
            store,
            context: None,
            file: None,
            is_commit: false,
            metrics,
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
        if commit {
            self.store.finalize_block(block)?;
        } else {
            self.store.abort_block(block)?;
        }
        Ok(())
    }

    pub fn open_batch(&mut self, msg: &Message) -> FsResult<Message> {
        let mut context = ContainerWriteContext::from_req(msg)?;

        if context.off > context.block_size {
            return err_box!(
                "Invalid write offset: {}, block size: {}",
                context.off,
                context.block_size
            );
        }

        // Open container block
        self.handle_small_files_batch(ProtoUtils::extend_block_to_pb(context.block.clone()))?;

        // Update context with actual block ID and path from storage
        if let Some(ref local_file) = self.file {
            context.files_metadata.container_path = local_file.path().to_string();
        }

        let container_meta = context.files_metadata.clone();
        let block_size = context.block_size;

        let container_response = BlockWriteResponse {
            id: container_meta.container_block_id,
            path: Some(container_meta.container_path.clone()),
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
        self.metrics.write_blocks.with_label_values(&[label]).inc();

        // Store context for later use
        let _ = self.context.replace(context);

        Ok(Builder::success(msg).proto_header(batch_response).build())
    }

    fn handle_small_files_batch(&mut self, block: ExtendedBlockProto) -> FsResult<i64> {
        let total_size: i64 = block.block_size;

        let container_block = ExtendedBlock {
            id: block.id,
            len: total_size,
            storage_type: StorageType::default(),
            file_type: FileType::Container,
            ..Default::default()
        };

        let container_meta = self.store.open_block(&container_block)?;
        let container_file = container_meta.create_writer(0, false)?;

        let actual_block_id = container_meta.id();
        self.file = Some(container_file);

        Ok(actual_block_id)
    }

    fn complete_container_batch(
        &mut self,
        context: &ContainerWriteContext,
        commit: bool,
    ) -> FsResult<()> {
        // Flush the container file
        if let Some(ref mut container_local_file) = self.file {
            container_local_file.flush()?;
        }

        // Commit using context metadata
        if let Some(container_file_meta) = context.files_metadata.files.last() {
            let container_block = ExtendedBlock {
                id: context.files_metadata.container_block_id,
                len: container_file_meta.offset + container_file_meta.len,
                storage_type: StorageType::default(),
                file_type: FileType::Container,
                ..Default::default()
            };

            self.commit_block(&container_block, commit)?;
        }

        Ok(())
    }

    pub fn complete_batch(&mut self, msg: &Message, commit: bool) -> FsResult<Message> {
        if let Some(context) = self.context.take() {
            Self::check_context(&context, msg)?;
        }

        let context = ContainerWriteContext::from_req(msg)?;

        if self.is_commit {
            return if !msg.data.is_empty() {
                err_box!("The block has been committed and data cannot be written anymore.")
            } else {
                Ok(msg.success())
            };
        }

        // Use context directly instead of copying to self fields
        self.complete_container_batch(&context, commit)?;
        Ok(msg.success())
    }

    pub fn write_batch(&mut self, msg: &Message) -> FsResult<Message> {
        let mut context = self.context.take().ok_or_else(|| -> FsError {
            FsError::Common(orpc::error::ErrorImpl::with_source(
                "Container context not initialized".into(),
            ))
        })?;

        Self::check_context(&context, msg)?;

        let header: ContainerWriteRequest = msg.parse_header()?;

        // Update context metadata if provided in header
        if let Some(ref container_meta) = header.container_meta {
            context.files_metadata = container_meta.clone();
        }

        // Write files using context metadata
        self.write_container_batch(&header.files, &mut context.files_metadata.files)?;

        self.context = Some(context);

        Ok(msg.success())
    }

    fn write_container_batch(
        &mut self,
        files: &[FileWriteDataProto],
        container_files_meta: &mut [SmallFileMetaProto],
    ) -> FsResult<()> {
        let mut offset = 0;

        for (i, file_data) in files.iter().enumerate() {
            if let Some(container_file) = &mut self.file {
                container_file.write_all(&file_data.content)?;

                // Update metadata in context
                if i < container_files_meta.len() {
                    container_files_meta[i].offset = offset;
                    container_files_meta[i].len = file_data.content.len() as i64;
                    offset += file_data.content.len() as i64;
                }
            }
        }

        // Update block length
        if let Some(container_local_file) = self.file.as_mut() {
            let current_pos: i64 = container_local_file.pos();
            let _ = container_local_file.resize(true, 0, current_pos, 0);
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
