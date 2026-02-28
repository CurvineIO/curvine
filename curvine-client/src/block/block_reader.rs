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
use crate::client_metrics::{ReadFallbackReason, ReadSource};
use crate::file::{FsContext, ReadChunkKey};
use crate::p2p::{ChunkId, P2pReadTraceContext};
use bytes::Bytes;
use curvine_common::error::FsError;
use curvine_common::state::{ClientAddress, ExtendedBlock, LocatedBlock, WorkerAddress};
use curvine_common::FsResult;
use log::warn;
use orpc::common::{LocalTime, Utils};
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

    fn source_tag(&self) -> ReadSource {
        match self {
            Local(_) => ReadSource::WorkerLocal,
            Remote(_) => ReadSource::WorkerRemote,
            Hole(_) => ReadSource::Hole,
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
    tenant_id: Option<String>,
    job_id: Option<String>,
    fs_context: Arc<FsContext>,
}

type ReadChunkFlight = (Arc<AsyncMutex<()>>, OwnedMutexGuard<()>);

impl BlockReader {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        fs_context: Arc<FsContext>,
        located: LocatedBlock,
        off: i64,
        file_id: i64,
        file_version_epoch: i64,
        file_mtime: i64,
        tenant_id: Option<String>,
        job_id: Option<String>,
    ) -> CommonResult<Self> {
        let len = located.block.len;

        let locs = Self::sort_locs(
            located.locs,
            fs_context.conf.client.short_circuit,
            &fs_context.client_addr,
        )?;

        let adapter = Self::get_reader(
            &locs,
            located.block.clone(),
            fs_context.clone(),
            off,
            len,
            tenant_id.as_deref(),
            job_id.as_deref(),
        )
        .await?;

        let reader = Self {
            inner: adapter,
            locs,
            block: located.block,
            file_id,
            file_version_epoch,
            file_mtime,
            tenant_id,
            job_id,
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
        tenant_id: Option<&str>,
        job_id: Option<&str>,
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
                    FsContext::get_metrics().observe_read_fallback(
                        ReadFallbackReason::OpenReaderError,
                        tenant_id,
                        job_id,
                    );
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

    fn observe_read_source_metric(&self, source: ReadSource, bytes: usize, start_nanos: u128) {
        FsContext::get_metrics().observe_read_source(
            source,
            bytes,
            start_nanos,
            self.tenant_id.as_deref(),
            self.job_id.as_deref(),
        );
    }

    fn observe_read_fallback_metric(&self, reason: ReadFallbackReason) {
        FsContext::get_metrics().observe_read_fallback(
            reason,
            self.tenant_id.as_deref(),
            self.job_id.as_deref(),
        );
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
        let start_nanos = LocalTime::nanos();
        if let Some(cached) = self.fs_context.get_read_chunk_cache(read_key) {
            self.advance_cached_position(cached.len())?;
            self.observe_read_source_metric(ReadSource::LocalChunkCache, cached.len(), start_nanos);
            return Ok(Some(DataSlice::bytes(cached)));
        }
        Ok(None)
    }

    async fn acquire_read_chunk_flight(&self, read_key: &ReadChunkKey) -> Option<ReadChunkFlight> {
        if !self.fs_context.read_chunk_cache_enabled() {
            return None;
        }
        let lock = self.fs_context.read_chunk_flight_lock(read_key.clone());
        let guard = lock.clone().lock_owned().await;
        Some((lock, guard))
    }

    fn build_trace_context(
        &self,
        version_epoch: i64,
        block_id: i64,
        read_off: i64,
    ) -> P2pReadTraceContext {
        P2pReadTraceContext {
            trace_id: Some(format!(
                "{}:{}:{}:{}",
                self.file_id, version_epoch, block_id, read_off
            )),
            tenant_id: self.tenant_id.clone(),
            job_id: self.job_id.clone(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn try_read_from_p2p(
        &mut self,
        source: ReadSource,
        chunk_id: ChunkId,
        expect_len: usize,
        read_key: &ReadChunkKey,
        version_epoch: i64,
        block_id: i64,
        read_off: i64,
    ) -> FsResult<Option<DataSlice>> {
        if source == ReadSource::Hole || expect_len == 0 {
            return Ok(None);
        }
        let Some(p2p_service) = self.fs_context.p2p_service() else {
            return Ok(None);
        };
        let p2p_start = LocalTime::nanos();
        let expected_mtime = (self.file_mtime > 0).then_some(self.file_mtime);
        let trace_ctx = self.build_trace_context(version_epoch, block_id, read_off);
        if let Some(data) = p2p_service
            .fetch_chunk_with_context(chunk_id, expect_len, expected_mtime, Some(&trace_ctx))
            .await
        {
            self.advance_cached_position(data.len())?;
            self.fs_context
                .put_read_chunk_cache(read_key.clone(), data.clone());
            self.observe_read_source_metric(ReadSource::P2p, data.len(), p2p_start);
            return Ok(Some(DataSlice::bytes(data)));
        }
        if !p2p_service.conf().fallback_worker_on_fail {
            return err_box!("p2p read miss and worker fallback is disabled");
        }
        Ok(None)
    }

    fn normalize_worker_chunk(chunk: DataSlice) -> Bytes {
        match chunk.freeze() {
            DataSlice::Bytes(bytes) => bytes,
            DataSlice::Empty => Bytes::new(),
            other => Bytes::copy_from_slice(other.as_slice()),
        }
    }

    async fn read_from_worker(
        &mut self,
        source: ReadSource,
        read_key: &ReadChunkKey,
        chunk_id: ChunkId,
    ) -> FsResult<DataSlice> {
        let start = LocalTime::nanos();
        let bytes = Self::normalize_worker_chunk(self.inner.read().await?);
        if source != ReadSource::Hole {
            self.fs_context
                .put_read_chunk_cache(read_key.clone(), bytes.clone());
            if let Some(p2p_service) = self.fs_context.p2p_service() {
                if !p2p_service.publish_chunk(chunk_id, bytes.clone(), self.file_mtime) {
                    warn!(
                        "publish chunk to p2p failed, chunk=({}, {}, {}, {})",
                        chunk_id.file_id, chunk_id.version_epoch, chunk_id.block_id, chunk_id.off
                    );
                }
            }
        }
        self.observe_read_source_metric(source, bytes.len(), start);
        Ok(DataSlice::bytes(bytes))
    }

    async fn handle_worker_read_error(&mut self, e: FsError) -> FsResult<()> {
        if matches!(&self.inner, Hole(_)) || self.locs.is_empty() {
            let reason = if matches!(&self.inner, Hole(_)) {
                ReadFallbackReason::HoleReadError
            } else {
                ReadFallbackReason::AllWorkersFailed
            };
            self.observe_read_fallback_metric(reason);
            return Err(e.ctx(format!(
                "failed to read block on {}",
                self.inner.worker_address()
            )));
        }

        self.observe_read_fallback_metric(ReadFallbackReason::SwitchReplica);
        warn!(
            "read data error block id {}, addr {}: {}",
            self.block_id(),
            self.inner.worker_address(),
            e
        );
        self.locs.retain(|x| x != self.inner.worker_address());
        self.inner = Self::get_reader(
            &self.locs,
            self.block.clone(),
            self.fs_context.clone(),
            self.pos(),
            self.len(),
            self.tenant_id.as_deref(),
            self.job_id.as_deref(),
        )
        .await?;
        Ok(())
    }

    // Based on network transmission efficiency considerations, the data size of the underlying tcp is fixed each time.
    pub async fn read(&mut self) -> FsResult<DataSlice> {
        if !self.has_remaining() {
            return Ok(DataSlice::empty());
        }

        loop {
            let read_off = self.pos();
            let block_id = self.block_id();
            let version_epoch = self.file_version_epoch.max(0);
            let read_key = ReadChunkKey::new(self.file_id, version_epoch, block_id, read_off);
            let chunk_id = ChunkId::with_version(self.file_id, version_epoch, block_id, read_off);
            let source = self.inner.source_tag();
            let expect_len =
                self.remaining()
                    .max(0)
                    .min(self.fs_context.read_chunk_size() as i64) as usize;

            if let Some(data) = self.try_read_chunk_cache(&read_key)? {
                return Ok(data);
            }

            let mut flight = self.acquire_read_chunk_flight(&read_key).await;
            if flight.is_some() {
                if let Some(data) = self.try_read_chunk_cache(&read_key)? {
                    self.release_read_chunk_flight(&read_key, &mut flight);
                    return Ok(data);
                }
            }

            match self
                .try_read_from_p2p(
                    source,
                    chunk_id,
                    expect_len,
                    &read_key,
                    version_epoch,
                    block_id,
                    read_off,
                )
                .await
            {
                Ok(Some(data)) => {
                    self.release_read_chunk_flight(&read_key, &mut flight);
                    return Ok(data);
                }
                Ok(None) => {}
                Err(e) => {
                    self.release_read_chunk_flight(&read_key, &mut flight);
                    return Err(e);
                }
            }

            match self.read_from_worker(source, &read_key, chunk_id).await {
                Ok(v) => {
                    self.release_read_chunk_flight(&read_key, &mut flight);
                    return Ok(v);
                }
                Err(e) => {
                    self.release_read_chunk_flight(&read_key, &mut flight);
                    self.handle_worker_read_error(e).await?;
                }
            }
        }
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

    fn advance_cached_position(&mut self, read_len: usize) -> FsResult<()> {
        if read_len == 0 {
            return Ok(());
        }
        self.seek(self.pos() + read_len as i64)
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
}
