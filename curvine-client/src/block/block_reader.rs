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

use crate::block::block_reader::ReaderAdapter::{Hole, Local, Remote};
use crate::block::{BlockReaderHole, BlockReaderLocal, BlockReaderRemote};
use crate::file::{FsContext, ReadChunkKey};
use crate::p2p::ChunkId;
use bytes::Bytes;
use curvine_common::error::FsError;
use curvine_common::state::{ClientAddress, ExtendedBlock, LocatedBlock, WorkerAddress};
use curvine_common::FsResult;
use log::warn;
use orpc::common::Utils;
use orpc::error::ErrorExt;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::DataSlice;
use orpc::{err_box, CommonResult};
use std::sync::Arc;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

enum ReaderAdapter {
    Local(BlockReaderLocal),
    Remote(BlockReaderRemote),
    Hole(BlockReaderHole),
}

impl ReaderAdapter {
    async fn read(&mut self) -> FsResult<DataSlice> {
        match self {
            Local(r) => r.read().await,
            Remote(r) => r.read().await,
            Hole(r) => r.read(),
        }
    }

    #[allow(unused)]
    fn blocking_read(&mut self, rt: &Runtime) -> FsResult<DataSlice> {
        match self {
            Local(r) => r.blocking_read(),
            Remote(r) => rt.block_on(r.read()),
            Hole(r) => r.read(),
        }
    }

    async fn complete(&mut self) -> FsResult<()> {
        match self {
            Local(r) => r.complete().await,
            Remote(r) => r.complete().await,
            Hole(r) => r.complete(),
        }
    }

    async fn abort(&mut self) {
        match self {
            Local(_) | Hole(_) => {}
            Remote(r) => r.abort().await,
        }
    }

    fn remaining(&self) -> i64 {
        match self {
            Local(r) => r.remaining(),
            Remote(r) => r.remaining(),
            Hole(r) => r.remaining(),
        }
    }

    fn seek(&mut self, pos: i64) -> FsResult<i64> {
        match self {
            Local(r) => r.seek(pos),
            Remote(r) => r.seek(pos),
            Hole(r) => r.seek(pos),
        }
    }

    fn pos(&self) -> i64 {
        match self {
            Local(r) => r.pos(),
            Remote(r) => r.pos(),
            Hole(r) => r.pos(),
        }
    }

    fn len(&self) -> i64 {
        match self {
            Local(r) => r.len(),
            Remote(r) => r.len(),
            Hole(r) => r.len(),
        }
    }

    fn block_id(&self) -> i64 {
        match self {
            Local(r) => r.block_id(),
            Remote(r) => r.block_id(),
            Hole(r) => r.block_id(),
        }
    }

    fn worker_address(&self) -> &WorkerAddress {
        match self {
            Local(r) => r.worker_address(),
            Remote(r) => r.worker_address(),
            Hole(r) => r.worker_address(),
        }
    }
}

pub struct BlockReader {
    inner: ReaderAdapter,
    locs: Vec<WorkerAddress>,
    block: ExtendedBlock,
    file_id: i64,
    file_version_epoch: i64,
    file_mtime: i64,
    fs_context: Arc<FsContext>,
}

type ReadChunkFlight = (Arc<AsyncMutex<()>>, OwnedMutexGuard<()>);

impl BlockReader {
    pub async fn new(
        fs_context: Arc<FsContext>,
        located: LocatedBlock,
        off: i64,
        file_id: i64,
        file_version_epoch: i64,
        file_mtime: i64,
    ) -> CommonResult<Self> {
        let len = located.block.len;

        let locs = Self::sort_locs(
            located.locs,
            fs_context.conf.client.short_circuit,
            &fs_context.client_addr,
        )?;

        let adapter =
            Self::get_reader(&locs, located.block.clone(), fs_context.clone(), off, len).await?;

        let reader = Self {
            inner: adapter,
            locs,
            block: located.block,
            file_id,
            file_version_epoch,
            file_mtime,
            fs_context,
        };

        Ok(reader)
    }

    // Sort the worker replicas
    // 1. Local priority
    // 2. Other random, sharing stress
    fn sort_locs(
        mut locs: Vec<WorkerAddress>,
        short_circuit: bool,
        local_addr: &ClientAddress,
    ) -> FsResult<Vec<WorkerAddress>> {
        if locs.is_empty() {
            return Ok(vec![]);
        }

        Utils::shuffle(&mut locs);
        if !short_circuit {
            return Ok(locs);
        }

        let local = locs.iter().position(|x| x.hostname == local_addr.hostname);
        if let Some(index) = local {
            locs.swap(0, index);
        }

        Ok(locs)
    }

    async fn get_reader(
        locs: &[WorkerAddress],
        block: ExtendedBlock,
        fs_context: Arc<FsContext>,
        off: i64,
        len: i64,
    ) -> FsResult<ReaderAdapter> {
        if locs.is_empty() && block.alloc_opts.is_some() {
            let reader = BlockReaderHole::new(fs_context.clone(), block.clone(), off, len)?;
            return Ok(Hole(reader));
        }

        let short_circuit = fs_context.conf.client.short_circuit;
        for loc in locs {
            let short_circuit = short_circuit && fs_context.is_local_worker(loc);
            let res: FsResult<ReaderAdapter> = {
                if short_circuit {
                    let reader = BlockReaderLocal::new(
                        fs_context.clone(),
                        block.clone(),
                        loc.clone(),
                        off,
                        len,
                    )
                    .await?;
                    Ok(Local(reader))
                } else {
                    let reader =
                        BlockReaderRemote::new(&fs_context, block.clone(), loc.clone(), off, len)
                            .await?;
                    Ok(Remote(reader))
                }
            };
            match res {
                Ok(v) => return Ok(v),
                Err(e) => {
                    warn!("fail to create block reader for {}: {}", loc, e);
                }
            }
        }

        err_box!(
            "There is no available worker, locs: {:?}, failed workers: {:?}",
            locs,
            fs_context.get_failed_workers()
        )
    }

    // Based on network transmission efficiency considerations, the data size of the underlying tcp is fixed each time.
    pub async fn read(&mut self) -> FsResult<DataSlice> {
        if !self.has_remaining() {
            // end of block file
            return Ok(DataSlice::empty());
        }

        let read_key = ReadChunkKey::new(
            self.file_id,
            self.file_version_epoch,
            self.block.id,
            self.pos(),
        );
        if let Some(chunk) = self.try_read_chunk_cache(&read_key)? {
            return Ok(chunk);
        }

        let mut flight = self.acquire_read_chunk_flight(&read_key).await;
        if let Some(chunk) = self.try_read_chunk_cache(&read_key)? {
            self.release_read_chunk_flight(&read_key, &mut flight);
            return Ok(chunk);
        }

        let chunk_id = ChunkId::with_version(
            self.file_id,
            self.file_version_epoch,
            self.block.id,
            self.pos(),
        );
        let expect_len = self
            .remaining()
            .min(self.fs_context.read_chunk_size() as i64)
            .max(0) as usize;
        if let Some(chunk) = self
            .try_read_from_p2p(chunk_id, expect_len, &read_key)
            .await?
        {
            self.release_read_chunk_flight(&read_key, &mut flight);
            return Ok(chunk);
        }

        let chunk = self.read_from_worker(&read_key, chunk_id).await;
        self.release_read_chunk_flight(&read_key, &mut flight);
        chunk
    }

    pub fn blocking_read(&mut self, rt: &Runtime) -> FsResult<DataSlice> {
        if !self.has_remaining() {
            return Ok(DataSlice::empty()); // end of block file
        }
        rt.block_on(self.read())
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        if let Err(e) = self.inner.complete().await {
            warn!("fail to complete reader: {}", e);
        }
        Ok(())
    }

    pub fn remaining(&self) -> i64 {
        self.inner.remaining()
    }

    pub fn has_remaining(&self) -> bool {
        self.remaining() > 0
    }

    pub fn seek(&mut self, pos: i64) -> FsResult<()> {
        self.inner.seek(pos)?;
        Ok(())
    }

    pub fn pos(&self) -> i64 {
        self.inner.pos()
    }

    pub fn len(&self) -> i64 {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn block_id(&self) -> i64 {
        self.inner.block_id()
    }

    async fn acquire_read_chunk_flight(&self, read_key: &ReadChunkKey) -> Option<ReadChunkFlight> {
        if !self.fs_context.read_chunk_cache_enabled() {
            return None;
        }
        let lock = self.fs_context.read_chunk_flight_lock(read_key.clone());
        let guard = lock.clone().lock_owned().await;
        Some((lock, guard))
    }

    fn release_read_chunk_flight(
        &self,
        read_key: &ReadChunkKey,
        flight: &mut Option<ReadChunkFlight>,
    ) {
        if let Some((lock, guard)) = flight.take() {
            drop(guard);
            self.fs_context.cleanup_read_chunk_flight(read_key, &lock);
        }
    }

    fn try_read_chunk_cache(&mut self, read_key: &ReadChunkKey) -> FsResult<Option<DataSlice>> {
        let Some(cached) = self.fs_context.get_read_chunk_cache(read_key) else {
            return Ok(None);
        };
        self.advance_cached_position(cached.len())?;
        Ok(Some(DataSlice::bytes(cached)))
    }

    async fn try_read_from_p2p(
        &mut self,
        chunk_id: ChunkId,
        expect_len: usize,
        read_key: &ReadChunkKey,
    ) -> FsResult<Option<DataSlice>> {
        if expect_len == 0 || !matches!(&self.inner, Remote(_)) {
            return Ok(None);
        }
        let Some(service) = self.fs_context.p2p_service() else {
            return Ok(None);
        };
        if let Some(cached) = service
            .fetch_chunk(chunk_id, expect_len, Some(self.file_mtime))
            .await
        {
            self.advance_cached_position(cached.len())?;
            self.fs_context
                .put_read_chunk_cache(read_key.clone(), cached.clone());
            return Ok(Some(DataSlice::bytes(cached)));
        }
        if service.conf().fallback_worker_on_fail {
            Ok(None)
        } else {
            err_box!("p2p read miss and worker fallback is disabled")
        }
    }

    async fn read_from_worker(
        &mut self,
        read_key: &ReadChunkKey,
        chunk_id: ChunkId,
    ) -> FsResult<DataSlice> {
        loop {
            match self.inner.read().await {
                Ok(chunk) => {
                    if !chunk.is_empty() {
                        self.fs_context.on_worker_chunk_read(
                            read_key.clone(),
                            chunk_id,
                            Bytes::copy_from_slice(chunk.as_slice()),
                            self.file_mtime,
                        );
                    }
                    return Ok(chunk);
                }
                Err(e) => {
                    self.handle_worker_read_error(e).await?;
                }
            }
        }
    }

    async fn handle_worker_read_error(&mut self, e: FsError) -> FsResult<()> {
        if matches!(&self.inner, Hole(_)) || self.locs.is_empty() {
            return Err(e.ctx(format!(
                "failed to read block on {}",
                self.inner.worker_address()
            )));
        }

        let failed_addr = self.inner.worker_address().clone();
        warn!(
            "read data error block id {}, addr {}: {}",
            self.block_id(),
            failed_addr,
            e
        );
        self.inner.abort().await;
        self.locs.retain(|x| x != &failed_addr);
        if self.locs.is_empty() {
            return Err(e.ctx(format!("failed to read block on {}", failed_addr)));
        }
        self.inner = Self::get_reader(
            &self.locs,
            self.block.clone(),
            self.fs_context.clone(),
            self.pos(),
            self.len(),
        )
        .await?;
        Ok(())
    }

    fn advance_cached_position(&mut self, len: usize) -> FsResult<()> {
        let next_pos = self.pos() + len as i64;
        self.inner.seek(next_pos)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::block_reader::ReaderAdapter::Remote;
    use curvine_common::conf::ClusterConf;
    use curvine_common::error::FsError;
    use curvine_common::state::{
        FileAllocMode, FileAllocOpts, FileType, StorageType, WorkerAddress,
    };
    use once_cell::sync::Lazy;

    static TEST_RT: Lazy<Arc<Runtime>> = Lazy::new(|| {
        let conf = ClusterConf::default();
        Arc::new(conf.client_rpc_conf().create_runtime())
    });

    fn test_fs_context() -> Arc<FsContext> {
        let conf = ClusterConf::default();
        Arc::new(FsContext::with_rt(conf, TEST_RT.clone()).expect("fs context should build"))
    }

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
    async fn last_replica_read_failure_does_not_turn_allocated_block_into_hole() {
        let fs_context = test_fs_context();
        let worker = test_worker(1);
        let block = ExtendedBlock::with_alloc(
            7,
            4,
            StorageType::Disk,
            FileType::File,
            Some(FileAllocOpts::with_alloc(4, FileAllocMode::ZERO_RANGE)),
        );
        let remote = BlockReaderRemote::new_for_test(block.clone(), worker.clone(), 0, 4);
        let mut reader = BlockReader {
            inner: Remote(remote),
            locs: vec![worker],
            block,
            file_id: 11,
            file_version_epoch: 3,
            file_mtime: 17,
            fs_context,
        };

        let err = reader
            .handle_worker_read_error(FsError::common("boom"))
            .await
            .expect_err("last replica failure should surface as error");

        assert!(matches!(reader.inner, Remote(_)));
        assert!(err.to_string().contains("failed to read block"));
    }
}
