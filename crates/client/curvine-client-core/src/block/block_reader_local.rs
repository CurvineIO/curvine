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

use crate::block::{BlockClient, BlockReaderRemote};
use crate::file::FsContext;
use bytes::BytesMut;
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_io::LocalFile;
use curvine_io::{CacheManager, DataSlice, ReadAheadTask};
use curvine_model::{ExtendedBlock, WorkerAddress};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_sys::RawPtr;
use std::sync::Arc;

pub struct BlockReaderLocal {
    rt: Arc<Runtime>,
    client: BlockClient,
    os_cache: CacheManager,
    last_task: Option<ReadAheadTask>,
    block: ExtendedBlock,
    file: RawPtr<LocalFile>,
    worker_address: WorkerAddress,
    len: i64,
    req_id: i64,
    seq_id: i32,
    chunk: BytesMut,
    chunk_size: usize,
}

pub(crate) enum LocalReaderOpen {
    Local(BlockReaderLocal),
    Remote(BlockReaderRemote),
}

enum AdoptedRead<C, L, R> {
    Local { client: C, file: L },
    Remote(R),
}

trait ReadSessionCleanup {
    async fn finish_read(
        &mut self,
        block: &ExtendedBlock,
        req_id: i64,
        seq_id: i32,
    ) -> FsResult<()>;

    fn prevent_pool_reuse(&mut self);
}

impl ReadSessionCleanup for BlockClient {
    async fn finish_read(
        &mut self,
        block: &ExtendedBlock,
        req_id: i64,
        seq_id: i32,
    ) -> FsResult<()> {
        self.read_commit(block, req_id, seq_id).await
    }

    fn prevent_pool_reuse(&mut self) {
        self.clear_pool();
    }
}

async fn adopt_opened_session<C, L, R, E>(
    mut client: C,
    path: Option<String>,
    block: &ExtendedBlock,
    req_id: i64,
    seq_id: i32,
    open_local: impl FnOnce(&str) -> Result<L, E>,
    open_remote: impl FnOnce(C) -> R,
) -> Result<AdoptedRead<C, L, R>, E>
where
    C: ReadSessionCleanup,
{
    let Some(path) = path else {
        return Ok(AdoptedRead::Remote(open_remote(client)));
    };

    match open_local(&path) {
        Ok(file) => Ok(AdoptedRead::Local { client, file }),
        Err(e) => {
            // Release the Worker read context promptly, but never allow a failed
            // local open to return an uncertain session to the connection pool.
            let _ = client.finish_read(block, req_id, seq_id + 1).await;
            client.prevent_pool_reuse();
            Err(e)
        }
    }
}

impl BlockReaderLocal {
    pub(crate) async fn open(
        fs_context: Arc<FsContext>,
        block: ExtendedBlock,
        addr: WorkerAddress,
        off: i64,
        len: i64,
    ) -> FsResult<LocalReaderOpen> {
        let req_id = Utils::req_id();
        let seq_id = 0;

        let chunk_size = fs_context.read_chunk_size();
        let client = fs_context.acquire_read(&addr).await?;
        let read_context = client
            .open_block(
                &fs_context.conf.client,
                &block,
                off,
                len,
                req_id,
                seq_id,
                true,
            )
            .await?;

        let opened = adopt_opened_session(
            client,
            read_context.path,
            &block,
            req_id,
            seq_id,
            |path| LocalFile::with_read(path, off as u64),
            |client| {
                BlockReaderRemote::from_opened(
                    client,
                    block.clone(),
                    addr.clone(),
                    off,
                    len,
                    req_id,
                    seq_id,
                )
            },
        )
        .await?;

        let (client, file) = match opened {
            AdoptedRead::Local { client, file } => (client, file),
            // The worker can reject short-circuit mode when it must synthesize a
            // sparse logical tail. Reuse the already-open remote read session.
            AdoptedRead::Remote(reader) => return Ok(LocalReaderOpen::Remote(reader)),
        };

        let reader = Self {
            rt: fs_context.clone_runtime(),
            client,
            os_cache: fs_context.clone_os_cache(),
            last_task: None,
            block,
            file: RawPtr::from_owned(file),
            worker_address: addr.clone(),
            len,
            req_id,
            seq_id,
            chunk: BytesMut::with_capacity(chunk_size),
            chunk_size,
        };

        Ok(LocalReaderOpen::Local(reader))
    }

    fn next_seq_id(&mut self) -> i32 {
        self.seq_id += 1;
        self.seq_id
    }

    pub fn pos(&self) -> i64 {
        self.file.pos()
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn remaining(&self) -> i64 {
        self.len - self.file.pos()
    }

    pub fn seek(&mut self, pos: i64) -> FsResult<i64> {
        Ok(self.file.as_mut().seek(pos)?)
    }

    fn get_chunk(&mut self) -> FsResult<BytesMut> {
        let read_size = self.chunk_size.min(self.remaining() as usize);
        if read_size == 0 {
            return err_box!("No readable data");
        }

        self.chunk.reserve(read_size);
        unsafe {
            self.chunk.set_len(read_size);
        }
        Ok(self.chunk.split())
    }

    pub async fn read(&mut self) -> FsResult<DataSlice> {
        let mut chunk = self.get_chunk()?;
        let file = self.file.clone();

        // Perform read-out.
        self.last_task = file
            .as_mut()
            .read_ahead(&self.os_cache, self.last_task.take());

        let chunk = self
            .rt
            .spawn_blocking(move || {
                file.as_mut().read_all(&mut chunk)?;
                Ok::<BytesMut, FsError>(chunk)
            })
            .await??;
        Ok(DataSlice::buffer(chunk))
    }

    pub fn blocking_read(&mut self) -> FsResult<DataSlice> {
        let mut chunk = self.get_chunk()?;
        self.last_task = self
            .file
            .as_mut()
            .read_ahead(&self.os_cache, self.last_task.take());
        self.file.as_mut().read_all(&mut chunk)?;
        Ok(DataSlice::buffer(chunk))
    }

    // Reading is completed and the server needs to be notified.
    pub async fn complete(&mut self) -> FsResult<()> {
        let next_seq_id = self.next_seq_id();
        self.client
            .read_commit(&self.block, self.req_id, next_seq_id)
            .await?;
        Ok(())
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
    use super::{adopt_opened_session, AdoptedRead, ReadSessionCleanup};
    use curvine_error::{FsError, FsResult};
    use curvine_model::{ExtendedBlock, FileType, StorageType};

    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[derive(Debug)]
    struct FakeClient {
        id: u64,
        finish_called: Arc<AtomicBool>,
        prevent_pool_reuse_called: Arc<AtomicBool>,
        fail_finish: bool,
    }

    impl FakeClient {
        fn new(id: u64) -> Self {
            Self {
                id,
                finish_called: Arc::new(AtomicBool::new(false)),
                prevent_pool_reuse_called: Arc::new(AtomicBool::new(false)),
                fail_finish: false,
            }
        }
    }

    impl ReadSessionCleanup for FakeClient {
        async fn finish_read(
            &mut self,
            _block: &ExtendedBlock,
            _req_id: i64,
            _seq_id: i32,
        ) -> FsResult<()> {
            self.finish_called.store(true, Ordering::SeqCst);
            if self.fail_finish {
                Err(FsError::common("injected read_commit failure"))
            } else {
                Ok(())
            }
        }

        fn prevent_pool_reuse(&mut self) {
            self.prevent_pool_reuse_called.store(true, Ordering::SeqCst);
        }
    }

    fn test_block() -> ExtendedBlock {
        ExtendedBlock::new(1, 4096, StorageType::Disk, FileType::File)
    }

    #[tokio::test]
    async fn missing_short_circuit_path_adopts_the_opened_session() {
        let opened = adopt_opened_session(
            FakeClient::new(42),
            None,
            &test_block(),
            100,
            0,
            |_| -> Result<(), &'static str> { panic!("local open must not be attempted") },
            |client| client,
        )
        .await
        .unwrap();

        match opened {
            AdoptedRead::Remote(client) => {
                assert_eq!(client.id, 42);
                assert!(!client.finish_called.load(Ordering::SeqCst));
                assert!(!client.prevent_pool_reuse_called.load(Ordering::SeqCst));
            }
            AdoptedRead::Local { .. } => panic!("missing path must adopt remote mode"),
        }
    }

    #[tokio::test]
    async fn local_open_failure_finishes_session_and_prevents_pool_reuse() {
        let mut client = FakeClient::new(42);
        client.fail_finish = true;
        let finish_called = client.finish_called.clone();
        let prevent_pool_reuse_called = client.prevent_pool_reuse_called.clone();
        let result = adopt_opened_session(
            client,
            Some("/missing/block".to_string()),
            &test_block(),
            100,
            0,
            |_| -> Result<(), &'static str> { Err("injected local open failure") },
            |client| client,
        )
        .await;

        assert!(matches!(result, Err("injected local open failure")));
        assert!(finish_called.load(Ordering::SeqCst));
        assert!(prevent_pool_reuse_called.load(Ordering::SeqCst));
    }
}
