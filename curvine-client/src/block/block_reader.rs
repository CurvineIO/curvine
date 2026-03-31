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
use crate::file::{AcceleratedReadRequest, FsContext, ReadChunkFlight};
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

struct ReadPipelineInput {
    request: AcceleratedReadRequest,
}

enum ReadPipelineOutcome {
    Data(DataSlice),
    Retry,
}

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

    // Based on network transmission efficiency considerations, the data size of the underlying tcp is fixed each time.
    pub async fn read(&mut self) -> FsResult<DataSlice> {
        if !self.has_remaining() {
            return Ok(DataSlice::empty());
        }
        loop {
            let input = self.build_read_pipeline_input();
            match self.execute_read_pipeline(&input).await? {
                ReadPipelineOutcome::Data(data) => return Ok(data),
                ReadPipelineOutcome::Retry => continue,
            }
        }
    }

    pub fn blocking_read(&mut self, rt: &Runtime) -> FsResult<DataSlice> {
        if !self.has_remaining() {
            return Ok(DataSlice::empty());
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

    async fn acquire_read_chunk_flight(
        &self,
        request: &AcceleratedReadRequest,
    ) -> Option<ReadChunkFlight> {
        self.fs_context
            .read_accelerator()
            .acquire_flight(request)
            .await
    }

    fn release_read_chunk_flight(
        &self,
        request: &AcceleratedReadRequest,
        flight: &mut Option<ReadChunkFlight>,
    ) {
        self.fs_context
            .read_accelerator()
            .release_flight(request, flight);
    }

    fn observe_read_source_metric(&self, source: ReadSource, bytes: usize, start_nanos: u128) {
        self.observe_read_source_metric_with_adaptive(source, Some(source), bytes, start_nanos);
    }

    fn observe_read_source_metric_with_adaptive(
        &self,
        source: ReadSource,
        adaptive_source: Option<ReadSource>,
        bytes: usize,
        start_nanos: u128,
    ) {
        if let Some(adaptive_source) = adaptive_source {
            self.fs_context
                .read_accelerator()
                .observe_adaptive_read_latency(adaptive_source, start_nanos);
        }
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

    fn try_read_chunk_cache(
        &mut self,
        request: &AcceleratedReadRequest,
    ) -> FsResult<Option<DataSlice>> {
        let start = LocalTime::nanos();
        if let Some(cached) = self
            .fs_context
            .read_accelerator()
            .try_read_local_chunk(request)
        {
            self.advance_cached_position(cached.len())?;
            self.observe_read_source_metric(ReadSource::LocalChunkCache, cached.len(), start);
            return Ok(Some(DataSlice::bytes(cached)));
        }
        Ok(None)
    }

    async fn try_read_from_p2p_cached(
        &mut self,
        request: &AcceleratedReadRequest,
    ) -> FsResult<Option<DataSlice>> {
        let start = LocalTime::nanos();
        if let Some(data) = self
            .fs_context
            .read_accelerator()
            .try_read_cached_p2p(request)
            .await
        {
            self.advance_cached_position(data.len())?;
            self.observe_read_source_metric_with_adaptive(ReadSource::P2p, None, data.len(), start);
            return Ok(Some(DataSlice::bytes(data)));
        }
        Ok(None)
    }

    async fn try_read_from_p2p(
        &mut self,
        request: &AcceleratedReadRequest,
    ) -> FsResult<Option<DataSlice>> {
        let start = LocalTime::nanos();
        if let Some(hit) = self
            .fs_context
            .read_accelerator()
            .try_read_network_p2p(request)
            .await?
        {
            let (data, adaptive_source) = hit.into_parts();
            self.advance_cached_position(data.len())?;
            self.observe_read_source_metric_with_adaptive(
                ReadSource::P2p,
                adaptive_source,
                data.len(),
                start,
            );
            return Ok(Some(DataSlice::bytes(data)));
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

    async fn read_from_worker(&mut self, request: &AcceleratedReadRequest) -> FsResult<DataSlice> {
        let start = LocalTime::nanos();
        let bytes = Self::normalize_worker_chunk(self.inner.read().await?);
        if !bytes.is_empty() {
            self.fs_context
                .read_accelerator()
                .on_worker_chunk_read(request, bytes.clone());
        }
        self.observe_read_source_metric(request.source(), bytes.len(), start);
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
            self.observe_read_fallback_metric(ReadFallbackReason::AllWorkersFailed);
            return Err(e.ctx(format!("failed to read block on {}", failed_addr)));
        }
        self.observe_read_fallback_metric(ReadFallbackReason::SwitchReplica);
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

    fn build_read_pipeline_input(&self) -> ReadPipelineInput {
        let read_off = self.pos();
        let version_epoch = self.file_version_epoch.max(0);
        let block_id = self.block.id;
        ReadPipelineInput {
            request: AcceleratedReadRequest::new(
                self.file_id,
                version_epoch,
                block_id,
                read_off,
                self.remaining()
                    .min(self.fs_context.read_chunk_size() as i64)
                    .max(0) as usize,
                self.file_mtime,
                self.inner.source_tag(),
                self.tenant_id.clone(),
                self.job_id.clone(),
            ),
        }
    }

    async fn execute_read_pipeline(
        &mut self,
        input: &ReadPipelineInput,
    ) -> FsResult<ReadPipelineOutcome> {
        if let Some(data) = self.try_read_chunk_cache(&input.request)? {
            return Ok(ReadPipelineOutcome::Data(data));
        }
        if let Some(data) = self.try_read_from_p2p_cached(&input.request).await? {
            return Ok(ReadPipelineOutcome::Data(data));
        }

        let mut flight = self.acquire_read_chunk_flight(&input.request).await;
        if let Some(data) = self.try_read_chunk_cache(&input.request)? {
            self.release_read_chunk_flight(&input.request, &mut flight);
            return Ok(ReadPipelineOutcome::Data(data));
        }

        if let Some(data) = self.try_read_from_p2p(&input.request).await? {
            self.release_read_chunk_flight(&input.request, &mut flight);
            return Ok(ReadPipelineOutcome::Data(data));
        }

        match self.read_from_worker(&input.request).await {
            Ok(data) => {
                self.release_read_chunk_flight(&input.request, &mut flight);
                Ok(ReadPipelineOutcome::Data(data))
            }
            Err(e) => {
                self.release_read_chunk_flight(&input.request, &mut flight);
                self.handle_worker_read_error(e).await?;
                Ok(ReadPipelineOutcome::Retry)
            }
        }
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
    use crate::p2p::ChunkId;
    use curvine_common::conf::ClusterConf;
    use curvine_common::error::FsError;
    use curvine_common::state::{
        FileAllocMode, FileAllocOpts, FileType, StorageType, WorkerAddress,
    };
    use once_cell::sync::Lazy;
    use tokio::time::{sleep, timeout, Duration, Instant};

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

    async fn wait_until_cached(fs_context: &FsContext, timeout_budget: Duration) -> bool {
        let deadline = Instant::now() + timeout_budget;
        while Instant::now() < deadline {
            if fs_context
                .p2p_stats_snapshot()
                .is_some_and(|snapshot| snapshot.cached_chunks_count > 0)
            {
                return true;
            }
            sleep(Duration::from_millis(10)).await;
        }
        false
    }

    #[tokio::test]
    async fn cached_p2p_hit_bypasses_read_chunk_flight() {
        let mut conf = ClusterConf::default();
        conf.client.read_chunk_size = 4;
        conf.client.p2p.enable = true;
        let fs_context =
            Arc::new(FsContext::with_rt(conf, TEST_RT.clone()).expect("fs context should build"));
        let block = ExtendedBlock::new(7, 4, StorageType::Disk, FileType::File);
        let chunk_id = ChunkId::with_version(11, 3, block.id, 0);
        let mtime = 17;
        assert_eq!(fs_context.p2p_state(), Some(crate::p2p::P2pState::Running));
        assert!(fs_context.read_accelerator().publish_p2p_chunk(
            chunk_id,
            Bytes::from_static(b"ping"),
            mtime
        ));
        assert!(wait_until_cached(&fs_context, Duration::from_secs(1)).await);
        let request = AcceleratedReadRequest::new(
            11,
            3,
            block.id,
            0,
            4,
            mtime,
            ReadSource::WorkerRemote,
            None,
            None,
        );

        let _held_flight = fs_context.read_accelerator().acquire_flight(&request).await;

        let mut reader = BlockReader {
            inner: Hole(BlockReaderHole::new(fs_context.clone(), block.clone(), 0, 4).unwrap()),
            locs: Vec::new(),
            block: block.clone(),
            file_id: 11,
            file_version_epoch: 3,
            file_mtime: mtime,
            tenant_id: None,
            job_id: None,
            fs_context: fs_context.clone(),
        };
        let input = ReadPipelineInput { request };

        let outcome = timeout(
            Duration::from_millis(50),
            reader.execute_read_pipeline(&input),
        )
        .await
        .expect("cached p2p hit should not wait for read chunk flight")
        .expect("pipeline should succeed");
        let ReadPipelineOutcome::Data(data) = outcome else {
            panic!("expected cached data");
        };
        assert_eq!(data.as_slice(), b"ping");
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
            tenant_id: None,
            job_id: None,
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
