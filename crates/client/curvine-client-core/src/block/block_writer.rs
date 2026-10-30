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

#![allow(unused)]

use crate::block::block_writer::WriterAdapter::{Local, Remote};
use crate::block::{BlockWriterLocal, BlockWriterRemote};
use crate::file::FsContext;
use curvine_core_error::err_box;
use curvine_core_error::ErrorExt;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_io::DataSlice;
use curvine_model::{BlockLocation, CommitBlock, LocatedBlock, StorageType, WorkerAddress};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use futures::future::{join_all, try_join_all};
use log::warn;
use std::sync::Arc;

async fn finish_all_cancellations<I, F>(futures: I) -> FsResult<()>
where
    I: IntoIterator<Item = F>,
    F: std::future::Future<Output = FsResult<()>>,
{
    let mut first_error = None;
    for result in join_all(futures).await {
        if let Err(e) = result {
            if first_error.is_none() {
                first_error = Some(e);
            }
        }
    }

    match first_error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

// An adapter that was opened against one replica and can be cancelled again.
trait OpenedAdapter {
    fn worker_address(&self) -> &WorkerAddress;
    async fn cancel(&mut self) -> FsResult<()>;
}

impl OpenedAdapter for WriterAdapter {
    fn worker_address(&self) -> &WorkerAddress {
        WriterAdapter::worker_address(self)
    }

    async fn cancel(&mut self) -> FsResult<()> {
        WriterAdapter::cancel(self).await
    }
}

// Best-effort cancellation of the adapters already opened in this round.
// Every adapter is attempted; the first cancellation error is preserved,
// but it never masks the primary error from the caller.
async fn cancel_opened<T>(inners: &mut [T]) -> FsResult<()>
where
    T: OpenedAdapter,
{
    let futures = inners.iter_mut().map(|writer| {
        let worker_addr = writer.worker_address().clone();
        async move {
            writer
                .cancel()
                .await
                .map_err(|e| e.ctx(format!("failed to cancel block on {}", worker_addr)))
        }
    });
    finish_all_cancellations(futures).await
}

// Opens one replica at a time. If a later replica fails to open and
// `cancel_on_failure` is set, the adapters already opened in this round are
// cancelled best-effort and the original open error is returned. `open` is
// injectable for tests.
async fn open_all<T, F, Fut>(
    num_replicas: usize,
    cancel_on_failure: bool,
    mut open: F,
) -> FsResult<Vec<T>>
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = FsResult<T>>,
    T: OpenedAdapter,
{
    let mut inners = Vec::with_capacity(num_replicas);
    for idx in 0..num_replicas {
        match open(idx).await {
            Ok(adapter) => inners.push(adapter),
            Err(e) => {
                // Partial open failure: best-effort cancel the replicas opened
                // in this round so worker-side write state is reclaimed.
                // Cancellation errors are logged only; the original open
                // error remains the primary error returned to the caller.
                if cancel_on_failure {
                    if let Err(cancel_err) = cancel_opened(&mut inners).await {
                        warn!(
                            "failed to cancel {} replicas opened before open failure: {}",
                            inners.len(),
                            cancel_err
                        );
                    }
                }
                return Err(e);
            }
        }
    }
    Ok(inners)
}

enum WriterAdapter {
    Local(BlockWriterLocal),
    Remote(BlockWriterRemote),
}

impl WriterAdapter {
    fn worker_address(&self) -> &WorkerAddress {
        match self {
            Local(f) => f.worker_address(),
            Remote(f) => f.worker_address(),
        }
    }

    fn actual_storage_type(&self) -> StorageType {
        match self {
            Local(f) => f.actual_storage_type(),
            Remote(f) => f.actual_storage_type(),
        }
    }

    async fn write(&mut self, buf: DataSlice) -> FsResult<()> {
        match self {
            Local(f) => f.write(buf).await,
            Remote(f) => f.write(buf).await,
        }
    }

    fn blocking_write(&mut self, rt: &Runtime, buf: DataSlice) -> FsResult<()> {
        match self {
            Local(f) => f.blocking_write(buf.clone()),
            Remote(f) => rt.block_on(f.write(buf.clone())),
        }
    }

    async fn flush(&mut self) -> FsResult<()> {
        match self {
            Local(f) => f.flush().await,
            Remote(f) => f.flush().await,
        }
    }

    async fn complete(&mut self) -> FsResult<()> {
        match self {
            Local(f) => f.complete().await,
            Remote(f) => f.complete().await,
        }
    }

    async fn cancel(&mut self) -> FsResult<()> {
        match self {
            Local(f) => f.cancel().await,
            Remote(f) => f.cancel().await,
        }
    }

    fn remaining(&self) -> i64 {
        match self {
            Local(f) => f.remaining(),
            Remote(f) => f.remaining(),
        }
    }

    fn pos(&self) -> i64 {
        match self {
            Local(f) => f.pos(),
            Remote(f) => f.pos(),
        }
    }

    // Add seek support for WriterAdapter
    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        match self {
            Local(f) => f.seek(pos).await,
            Remote(f) => f.seek(pos).await,
        }
    }

    fn len(&self) -> i64 {
        match self {
            Local(f) => f.len(),
            Remote(f) => f.len(),
        }
    }

    // Create new WriterAdapter
    async fn new(
        fs_context: Arc<FsContext>,
        located_block: &LocatedBlock,
        worker_addr: &WorkerAddress,
        pos: i64,
        block_size: i64,
    ) -> FsResult<Self> {
        let conf = &fs_context.conf.client;
        // SPDK bypasses kernel — no local path for writes.
        // Use has_spdk (from worker-reported actual storage type)
        let short_circuit = conf.short_circuit
            && fs_context.is_local_worker(worker_addr)
            && !located_block.has_spdk;

        let adapter = if short_circuit {
            match BlockWriterLocal::new(
                fs_context.clone(),
                located_block.block.clone(),
                worker_addr.clone(),
                pos,
                block_size,
            )
            .await
            {
                Ok(writer) => Local(writer),
                Err(FsError::NoLocalPath(_)) => {
                    // Server has no local path for this block (e.g. SPDK bdev).
                    // BlockWriterLocal already aborted the open; retry via remote.
                    let writer = BlockWriterRemote::new(
                        &fs_context,
                        located_block.block.clone(),
                        worker_addr.clone(),
                        pos,
                        block_size,
                    )
                    .await?;
                    Remote(writer)
                }
                Err(e) => return Err(e),
            }
        } else {
            let writer = BlockWriterRemote::new(
                &fs_context,
                located_block.block.clone(),
                worker_addr.clone(),
                pos,
                block_size,
            )
            .await?;
            Remote(writer)
        };

        Ok(adapter)
    }
}

pub struct BlockWriter {
    inners: Vec<WriterAdapter>,
    locate: LocatedBlock,
    fs_context: Arc<FsContext>,
}

impl BlockWriter {
    pub async fn new(
        fs_context: Arc<FsContext>,
        locate: LocatedBlock,
        pos: i64,
        block_size: i64,
    ) -> FsResult<Self> {
        if locate.locs.is_empty() {
            return err_box!("There is no available worker");
        }

        // Reopening a committed block (the master reports len > 0, or the
        // write offset is non-zero): a cancel commit would abort the already
        // committed replicas on the workers. SPDK rewrites reuse the extent
        // without preserving the committed state (BdevLayout::prepare_write),
        // so aborting such a rewrite would free an extent the master still
        // references. Skip rollback cancellation for those and keep the
        // pre-existing drop-only behavior; newly allocated blocks (len == 0
        // and pos == 0) are still cancelled best-effort.
        let committed_reopen = locate.block.len > 0 || pos > 0;

        let inners = open_all(locate.locs.len(), !committed_reopen, |idx| {
            WriterAdapter::new(
                fs_context.clone(),
                &locate,
                &locate.locs[idx],
                pos,
                block_size,
            )
        })
        .await?;

        let writer = Self {
            inners,
            locate,
            fs_context,
        };
        Ok(writer)
    }

    pub async fn write(&mut self, chunk: DataSlice) -> FsResult<()> {
        let chunk = chunk.freeze();
        let futures = self.inners.iter_mut().map(|writer| {
            let chunk_clone = chunk.clone();
            async move {
                writer
                    .write(chunk_clone)
                    .await
                    .map_err(|e| (writer.worker_address().clone(), e))
            }
        });

        if let Err((worker_addr, e)) = try_join_all(futures).await {
            return Err(e.ctx(format!("failed to write block on {}", worker_addr)));
        }
        Ok(())
    }

    pub fn blocking_write(&mut self, rt: &Runtime, chunk: DataSlice) -> FsResult<()> {
        rt.block_on(self.write(chunk))
    }

    pub async fn flush(&mut self) -> FsResult<()> {
        let futures = self.inners.iter_mut().map(|writer| async move {
            writer
                .flush()
                .await
                .map_err(|e| (writer.worker_address().clone(), e))
        });

        if let Err((worker_addr, e)) = try_join_all(futures).await {
            return Err(e.ctx(format!("failed to flush block on {}", worker_addr)));
        }
        Ok(())
    }

    pub async fn complete(&mut self) -> FsResult<CommitBlock> {
        let futures = self.inners.iter_mut().map(|writer| async move {
            writer
                .complete()
                .await
                .map_err(|e| (writer.worker_address().clone(), e))
        });

        if let Err((worker_addr, e)) = try_join_all(futures).await {
            return Err(e.ctx(format!("failed to complete block on {}", worker_addr)));
        }
        Ok(self.to_commit_block())
    }

    pub async fn cancel(&mut self) -> FsResult<()> {
        cancel_opened(&mut self.inners).await
    }

    pub fn remaining(&self) -> i64 {
        self.inners[0].remaining()
    }

    pub fn has_remaining(&self) -> bool {
        self.inners[0].remaining() > 0
    }

    pub fn pos(&self) -> i64 {
        self.inners[0].pos()
    }

    pub fn len(&self) -> i64 {
        self.inners[0].len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // Get block ID for cache management
    pub fn block_id(&self) -> i64 {
        self.locate.block.id
    }

    // Implement seek support for random writes
    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 {
            return err_box!("Cannot seek to negative position: {}", pos);
        }

        let futures = self.inners.iter_mut().map(|writer| async move {
            writer
                .seek(pos)
                .await
                .map_err(|e| (writer.worker_address().clone(), e))
        });

        if let Err((worker_addr, e)) = try_join_all(futures).await {
            return Err(e.ctx(format!("failed to seek to worker {}", worker_addr)));
        }
        Ok(())
    }

    pub fn to_commit_block(&self) -> CommitBlock {
        let locs = self
            .inners
            .iter()
            .map(|writer| {
                BlockLocation::new(
                    writer.worker_address().worker_id,
                    writer.actual_storage_type(),
                )
            })
            .collect();

        CommitBlock {
            block_id: self.locate.block.id,
            block_len: self.len(),
            locations: locs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{cancel_opened, finish_all_cancellations, open_all, OpenedAdapter};
    use curvine_error::{FsError, FsResult};
    use curvine_model::{ExtendedBlock, FileType, LocatedBlock, StorageType, WorkerAddress};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn cancellation_attempts_all_futures_and_returns_an_error() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let futures = (0..3).map(|index| {
            let attempts = attempts.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                if index == 1 {
                    Err(FsError::common("injected cancellation failure"))
                } else {
                    Ok(())
                }
            }
        });

        assert!(finish_all_cancellations(futures).await.is_err());
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            3,
            "one failure must not prevent the remaining cancellations"
        );
    }

    #[derive(Clone, Debug)]
    struct ScriptedWriter {
        calls: Arc<AtomicUsize>,
        fail_cancel: bool,
        worker_id: u32,
    }

    impl ScriptedWriter {
        fn new(calls: Arc<AtomicUsize>, fail_cancel: bool, worker_id: u32) -> Self {
            Self {
                calls,
                fail_cancel,
                worker_id,
            }
        }
    }

    impl OpenedAdapter for ScriptedWriter {
        fn worker_address(&self) -> &WorkerAddress {
            // Return a stable address so the ctx wrapping in cancel_opened works.
            static ADDR: std::sync::OnceLock<WorkerAddress> = std::sync::OnceLock::new();
            ADDR.get_or_init(WorkerAddress::default)
        }

        async fn cancel(&mut self) -> FsResult<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_cancel {
                Err(FsError::common("injected cancellation failure"))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn failed_replica_open_cancels_adapters_opened_before_failure() {
        let calls = Arc::new(AtomicUsize::new(0));
        // Replica 0 opens fine, replica 1 fails to open: the adapter opened
        // for replica 0 must be cancelled exactly once and the original open
        // error of replica 1 must be returned.
        let script = vec![(false, false), (true, false)]; // (fail_open, fail_cancel)

        let err = open_all(2, true, |idx| {
            let script = script.clone();
            let calls = calls.clone();
            async move {
                let (fail_open, fail_cancel) = script[idx];
                if fail_open {
                    Err(FsError::common("injected open failure"))
                } else {
                    Ok(ScriptedWriter::new(calls, fail_cancel, idx as u32))
                }
            }
        })
        .await;

        assert!(err.is_err());
        let err_msg = err.unwrap_err().to_string();
        assert!(
            err_msg.contains("injected open failure"),
            "primary error must be the original open error, got: {err_msg}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the adapter opened before the failure must be cancelled exactly once"
        );
    }

    #[tokio::test]
    async fn failed_replica_open_with_failing_cancel_still_returns_open_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        // Replica 0 opens fine and its cancellation fails; replica 1 fails to
        // open. The caller must still get the original open error of replica 1.
        let script = vec![(false, true), (true, false)];

        let err = open_all(2, true, |idx| {
            let script = script.clone();
            let calls = calls.clone();
            async move {
                let (fail_open, fail_cancel) = script[idx];
                if fail_open {
                    Err(FsError::common("injected open failure"))
                } else {
                    Ok(ScriptedWriter::new(calls, fail_cancel, idx as u32))
                }
            }
        })
        .await;

        assert!(err.is_err());
        let err_msg = err.unwrap_err().to_string();
        assert!(
            err_msg.contains("injected open failure"),
            "primary error must be the original open error, got: {err_msg}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "cancel must still be attempted even when the original error wins"
        );
    }

    #[tokio::test]
    async fn successful_open_never_cancels_any_adapter() {
        let calls = Arc::new(AtomicUsize::new(0));

        let inners = open_all(3, true, |idx| {
            let calls = calls.clone();
            async move { Ok(ScriptedWriter::new(calls, false, idx as u32)) }
        })
        .await
        .unwrap();

        assert_eq!(inners.len(), 3);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "no cancellation when all replicas open successfully"
        );
    }

    #[tokio::test]
    async fn cancel_opened_attempts_all_adapters_and_preserves_first_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut inners = vec![
            ScriptedWriter::new(calls.clone(), false, 0),
            ScriptedWriter::new(calls.clone(), true, 1),
            ScriptedWriter::new(calls.clone(), false, 2),
        ];

        let result = cancel_opened(&mut inners).await;

        assert!(result.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "one failing cancel must not prevent the remaining cancellations"
        );
    }

    #[tokio::test]
    async fn committed_reopen_failure_does_not_cancel_opened_replicas() {
        let calls = Arc::new(AtomicUsize::new(0));
        // Reopening a committed block (len > 0): rollback must not cancel the
        // replicas opened in this round, otherwise an already committed
        // replica would be aborted on the workers (see the Copilot review on
        // PR #1918).
        let block = ExtendedBlock::new(1, 64, StorageType::Ssd, FileType::File);
        let block = ExtendedBlock { len: 64, ..block };
        let lb = LocatedBlock::new(block, vec![]);
        assert_eq!(lb.block.len, 64);

        // Drive open_all with cancel_on_failure = false directly: the
        // BlockWriter::new gate maps committed reopens to this flag.
        let err = open_all(2, false, |idx| {
            let calls = calls.clone();
            async move {
                if idx == 1 {
                    Err(FsError::common("injected open failure"))
                } else {
                    Ok(ScriptedWriter::new(calls, false, idx as u32))
                }
            }
        })
        .await;

        assert!(err.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "committed reopen must not cancel opened replicas"
        );
    }
}
