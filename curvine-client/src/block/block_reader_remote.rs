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

use crate::block::BlockClient;
use crate::file::FsContext;
use curvine_common::proto::DataHeaderProto;
use curvine_common::state::{ExtendedBlock, WorkerAddress};
use curvine_common::FsResult;
use orpc::common::Utils;
use orpc::err_box;
use orpc::sys::DataSlice;

pub struct BlockReaderRemote {
    client: Option<BlockClient>,
    block: ExtendedBlock,
    worker_address: WorkerAddress,
    pos: i64,
    len: i64,
    req_id: i64,
    seq_id: i32,
    header: Option<DataHeaderProto>,
}

impl BlockReaderRemote {
    pub async fn new(
        fs_context: &FsContext,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
        off: i64,
        len: i64,
    ) -> FsResult<Self> {
        let req_id = Utils::req_id();
        let seq_id = 0;

        let client = fs_context.acquire_read(&worker_address).await?;
        let _ = client
            .open_block(
                &fs_context.conf.client,
                &block,
                off,
                len,
                req_id,
                seq_id,
                false,
            )
            .await?;

        let reader = Self {
            client: Some(client),
            block,
            worker_address,
            pos: off,
            len,
            req_id,
            seq_id,
            header: None,
        };

        Ok(reader)
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        block: ExtendedBlock,
        worker_address: WorkerAddress,
        off: i64,
        len: i64,
    ) -> Self {
        Self {
            client: None,
            block,
            worker_address,
            pos: off,
            len,
            req_id: Utils::req_id(),
            seq_id: 0,
            header: None,
        }
    }

    fn next_seq_id(&mut self) -> i32 {
        self.seq_id += 1;
        self.seq_id
    }

    pub fn pos(&self) -> i64 {
        self.pos
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn remaining(&self) -> i64 {
        self.len - self.pos
    }

    pub fn seek(&mut self, pos: i64) -> FsResult<i64> {
        self.pos = pos;
        self.header = Some(DataHeaderProto {
            offset: pos,
            flush: false,
            is_last: false,
        });
        Ok(self.pos)
    }

    pub async fn read(&mut self) -> FsResult<DataSlice> {
        if self.remaining() <= 0 {
            return err_box!("No readable data");
        }

        let seq_id = self.next_seq_id();
        let header = self.header.take();
        let Some(client) = self.client.as_ref() else {
            return err_box!("No readable data");
        };
        let chunk = client.read_data(self.req_id, seq_id, header).await?;

        self.pos += chunk.len() as i64;
        Ok(chunk)
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        let next_seq_id = self.next_seq_id();
        let Some(client) = self.client.as_ref() else {
            return Ok(());
        };
        client
            .read_commit(&self.block, self.req_id, next_seq_id)
            .await
    }

    pub async fn abort(&mut self) {
        let next_seq_id = self.next_seq_id();
        let Some(mut client) = self.client.take() else {
            return;
        };
        let _ = client
            .read_commit(&self.block, self.req_id, next_seq_id)
            .await;
        client.clear_pool();
        self.header = None;
    }

    #[cfg(test)]
    pub(crate) fn set_test_client(&mut self, client: BlockClient) {
        self.client = Some(client);
    }

    pub fn block_id(&self) -> i64 {
        self.block.id
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.worker_address
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::BlockClientPool;
    use curvine_common::state::{FileType, StorageType};
    use std::sync::Arc;

    fn test_worker(worker_id: u32) -> WorkerAddress {
        WorkerAddress {
            worker_id,
            hostname: format!("worker-{}", worker_id),
            ip_addr: "127.0.0.1".to_string(),
            rpc_port: 8000 + worker_id,
            web_port: 9000 + worker_id,
        }
    }

    #[tokio::test]
    async fn aborted_remote_reader_does_not_repool_and_complete_is_noop() {
        let worker = test_worker(1);
        let block = ExtendedBlock::new(9, 4, StorageType::Disk, FileType::File);
        let mut reader = BlockReaderRemote::new_for_test(block, worker.clone(), 0, 4);
        let pool = Arc::new(BlockClientPool::new(true, 8, 60_000));
        let mut client = BlockClient::new_for_test(worker);
        client.set_pool(pool.clone());
        reader.set_test_client(client);

        assert_eq!(pool.idle_conn(), 0);
        assert_eq!(Arc::strong_count(&pool), 2);

        reader.abort().await;

        assert_eq!(pool.idle_conn(), 0);
        assert_eq!(Arc::strong_count(&pool), 1);
        reader
            .complete()
            .await
            .expect("aborted reader completion should be a no-op");
    }
}
