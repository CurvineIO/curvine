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

use crate::block::block_client::BlockClient;
use crate::file::FsContext;
use curvine_common::fs::Path;
use curvine_common::proto::{ContainerMetadataProto, SmallFileMetaProto};
use curvine_common::state::{ContainerStatus, ExtendedBlock, WorkerAddress};
use curvine_common::FsResult;
use orpc::common::Utils;
use orpc::err_box;

pub struct ContainerBlockWriterRemote {
    block: ExtendedBlock,
    worker_address: WorkerAddress,
    client: BlockClient,
    pos: i64,
    seq_id: i32,
    req_id: i64,
    block_size: i64,
    container_meta: Option<ContainerMetadataProto>,
}

impl ContainerBlockWriterRemote {
    pub async fn new(
        fs_context: &FsContext,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
        pos: i64,
        container_status: ContainerStatus,
        small_files_metadata: Vec<SmallFileMetaProto>,
    ) -> FsResult<Self> {
        // create a container block
        let req_id = Utils::req_id();
        let seq_id = 0;
        let block_size = fs_context.block_size();
        let client = fs_context.block_client(&worker_address).await?;
        let write_context = client
            .write_container_block(
                &block,
                0,
                block_size,
                req_id,
                fs_context.write_chunk_size() as i32,
                fs_context.write_chunk_size() as i32,
                false,
                container_status,
                small_files_metadata,
            )
            .await?;

        // Extract container metadata from response
        let container_meta = write_context.container_meta.clone();

        for context in &write_context.contexts {
            if block_size != context.block_size {
                return err_box!(
                    "Abnormal block size, expected length {}, actual length {}",
                    block_size,
                    context.block_size
                );
            }
        }

        let writer = Self {
            block,
            worker_address,
            client,
            pos,
            seq_id,
            req_id,
            block_size,
            container_meta,
        };

        Ok(writer)
    }

    fn next_seq_id(&mut self) -> i32 {
        self.seq_id += 1;
        self.seq_id
    }

    // Write data.
    pub async fn write(&mut self, files: &[(&Path, &str)]) -> FsResult<()> {
        let next_seq_id = self.next_seq_id();

        // Send all files in one RPC call
        self.client
            .write_container(files, self.req_id, next_seq_id, self.container_meta.clone())
            .await?;

        let file_len = files
            .iter()
            .map(|x: &(&Path, &str)| x.1.len())
            .sum::<usize>() as i64;
        if self.block.len < file_len {
            self.block.len = file_len
        }

        Ok(())
    }

    pub async fn flush(&mut self) -> FsResult<()> {
        let next_seq_id = self.next_seq_id();
        self.client
            .write_flush(self.pos, self.req_id, next_seq_id)
            .await?;

        Ok(())
    }

    // Write complete
    pub async fn complete(&mut self) -> FsResult<()> {
        let next_seq_id = self.next_seq_id();
        self.client
            .write_commit_container(
                &self.block,
                self.pos,
                self.block_size,
                self.req_id,
                next_seq_id,
                false,
                self.container_meta.take(),
            )
            .await?;

        Ok(())
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.worker_address
    }
}
