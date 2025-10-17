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

use curvine_client::block::BlockWriterRemote;
use curvine_client::file::FsContext;
use curvine_common::state::{ExtendedBlock, WorkerAddress};
use curvine_common::FsResult;
use std::ops::{Deref, DerefMut};

pub struct WritePipeline {
    remote_worker_client: BlockWriterRemote,
}

impl WritePipeline {
    pub async fn new(
        fs_context: &FsContext,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
    ) -> FsResult<Self> {
        let client = BlockWriterRemote::new(fs_context, block, worker_address).await?;
        let handler = Self {
            remote_worker_client: client,
        };
        Ok(handler)
    }
}

impl Deref for WritePipeline {
    type Target = BlockWriterRemote;

    fn deref(&self) -> &Self::Target {
        &self.remote_worker_client
    }
}

impl DerefMut for WritePipeline {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.remote_worker_client
    }
}
