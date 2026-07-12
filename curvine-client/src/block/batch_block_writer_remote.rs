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
use curvine_common::state::{ExtendedBlock, WorkerAddress};
use curvine_common::FsResult;
use orpc::common::Utils;

pub struct BatchBlockWriterRemote {
    blocks: Vec<ExtendedBlock>,
    worker_address: WorkerAddress,
    client: BlockClient,
    pos: i64,
    seq_id: i32,
    req_id: i64,
    block_size: i64,
}

impl BatchBlockWriterRemote {
    pub async fn new(
        fs_context: &FsContext,
        blocks: Vec<ExtendedBlock>,
        worker_address: WorkerAddress,
        pos: i64,
    ) -> FsResult<(Self, Vec<bool>)> {
        let req_id = Utils::req_id();
        let seq_id = 0;
        let block_size = fs_context.block_size();

        let client = fs_context.block_client(&worker_address).await?;
        let write_context = match client
            .write_blocks_batch(
                &blocks,
                0,
                block_size,
                req_id,
                seq_id,
                fs_context.write_chunk_size() as i32,
                false,
            )
            .await
        {
            Ok(context) => context,
            Err(error) => {
                let cancels = vec![true; blocks.len()];
                if let Err(cancel_error) = client
                    .write_commit_batch(&blocks, pos, block_size, req_id, seq_id + 1, &cancels)
                    .await
                {
                    log::warn!(
                        "failed to cancel remote batch blocks after open error: {}",
                        cancel_error
                    );
                }
                return Err(error);
            }
        };

        let open_results = write_context
            .into_iter()
            .map(|result| match result {
                Ok(context) if block_size == context.block_size => true,
                Ok(context) => {
                    log::warn!(
                        "abnormal batch block size, expected {}, actual {}",
                        block_size,
                        context.block_size
                    );
                    false
                }
                Err(error) => {
                    log::warn!("failed to open remote batch block: {}", error);
                    false
                }
            })
            .collect();

        let writer = Self {
            blocks,
            worker_address,
            client,
            pos,
            seq_id,
            req_id,
            block_size,
        };

        Ok((writer, open_results))
    }

    fn next_seq_id(&mut self) -> i32 {
        self.seq_id += 1;
        self.seq_id
    }

    // Write data.
    pub async fn write(&mut self, files: &[(&Path, &str)]) -> FsResult<Vec<bool>> {
        let next_seq_id = self.next_seq_id();

        let results = self
            .client
            .write_files_batch(files, self.req_id, next_seq_id)
            .await?;

        for (i, ((_, content), success)) in files.iter().zip(results.iter()).enumerate() {
            if *success && i < self.blocks.len() {
                let file_len = content.len() as i64;
                self.blocks[i].len = file_len;
            }
        }
        Ok(results)
    }

    // Write complete
    pub async fn complete(&mut self, cancels: &[bool]) -> FsResult<Vec<bool>> {
        let next_seq_id = self.next_seq_id();
        self.client
            .write_commit_batch(
                &self.blocks,
                self.pos,
                self.block_size,
                self.req_id,
                next_seq_id,
                cancels,
            )
            .await
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.worker_address
    }
}
