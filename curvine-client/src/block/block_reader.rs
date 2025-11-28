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
use crate::file::FsContext;
use curvine_common::state::{ClientAddress, ExtendedBlock, LocatedBlock, WorkerAddress};
use curvine_common::FsResult;
use log::warn;
use orpc::common::Utils;
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
}

pub struct BlockReader {
    inner: ReaderAdapter,
    locs: Vec<WorkerAddress>,
    block: ExtendedBlock,
    fs_context: Arc<FsContext>,
}

impl BlockReader {
    pub async fn new(
        fs_context: Arc<FsContext>,
        located: LocatedBlock,
        off: i64,
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
            if fs_context.is_failed_worker(loc) {
                continue;
            }

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
                    warn!(
                        "fail to create block reader for {}: {}",
                        loc.connect_addr(),
                        e
                    );
                    fs_context.add_failed_worker(loc);
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

        let res = self.inner.read().await;
        match res {
            _ if self.locs.is_empty() => res,

            Ok(v) => Ok(v),

            Err(e) => {
                warn!(
                    "Read data error block id {}, addr {}: {}",
                    self.block_id(),
                    self.inner.worker_address(),
                    e
                );
                self.fs_context
                    .add_failed_worker(self.inner.worker_address());

                let reader = Self::get_reader(
                    &self.locs,
                    self.block.clone(),
                    self.fs_context.clone(),
                    self.pos(),
                    self.len(),
                )
                .await?;

                self.inner = reader;
                self.inner.read().await
            }
        }
    }

    pub fn blocking_read(&mut self, rt: &Runtime) -> FsResult<DataSlice> {
        if !self.has_remaining() {
            return Ok(DataSlice::empty()); // end of block file
        }

        let res = self.inner.blocking_read(rt);

        match res {
            _ if self.locs.is_empty() => res,

            Ok(v) => Ok(v),

            Err(e) => {
                warn!(
                    "Read data error block id {}, addr {}: {}",
                    self.block_id(),
                    self.inner.worker_address(),
                    e
                );
                self.fs_context
                    .add_failed_worker(self.inner.worker_address());

                let reader = rt.block_on(Self::get_reader(
                    &self.locs,
                    self.block.clone(),
                    self.fs_context.clone(),
                    self.pos(),
                    self.len(),
                ))?;

                self.inner = reader;
                self.inner.blocking_read(rt)
            }
        }
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        self.inner.complete().await
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
}
