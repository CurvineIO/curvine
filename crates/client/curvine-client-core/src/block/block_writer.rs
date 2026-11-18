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

use crate::block::block_writer::WriterAdapter::{Local, Remote};
use crate::block::{BlockWriterLocal, BlockWriterRemote};
use crate::file::FsContext;
use curvine_core_error::err_box;
use curvine_core_error::ErrorExt;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_io::DataSlice;
use curvine_metrics::Gauge;
use curvine_model::{
    BlockLocation, CommitBlock, ExtendedBlock, LocatedBlock, StorageType, WorkerAddress,
};
use curvine_runtime::common::TimeSpent;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use futures::future::join_all;
use log::warn;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

// Whether a partial open failure may cancel/abort a block that already has
// worker-side write state. Reopening a committed block (the master reports
// len > 0, or the write offset is non-zero) must not be rolled back: a cancel
// commit would abort the already committed replicas on the workers. SPDK
// rewrites reuse the extent without preserving the committed state
// (BdevLayout::prepare_write), so aborting such a rewrite would free an
// extent the master still references. Newly allocated blocks (len == 0 and
// pos == 0) have no committed state and are cancelled best-effort.
pub(crate) fn rollback_on_failure(block: &ExtendedBlock, pos: i64) -> bool {
    block.len == 0 && pos == 0
}

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
async fn cancel_opened<T>(inners: &mut [T], budget: Duration) -> FsResult<()>
where
    T: OpenedAdapter,
{
    let futures = inners.iter_mut().map(|writer| {
        let worker_addr = writer.worker_address().clone();
        async move {
            timeout(budget, writer.cancel())
                .await
                .map_err(FsError::from)
                .and_then(|result| result)
                .map_err(|e| e.ctx(format!("failed to cancel block on {}", worker_addr)))
        }
    });
    finish_all_cancellations(futures).await
}

// Open all locations in one concurrent phase. A failed open only rolls back
// successful opens when the minimum cannot be met, and only for fresh blocks.
// Keep the injectable open seam for lifecycle/rollback regression tests.
async fn open_all<T, F, Fut>(
    num_replicas: usize,
    min_replicas: usize,
    budget: Duration,
    cancel_on_failure: bool,
    mut open: F,
) -> FsResult<Vec<T>>
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = FsResult<T>>,
    T: OpenedAdapter,
{
    let futures = (0..num_replicas).map(|idx| {
        let future = open(idx);
        async move {
            timeout(budget, future)
                .await
                .map_err(|e| {
                    FsError::from(e).ctx(format!(
                        "replica open timeout after {budget:?}, index={idx}"
                    ))
                })
                .and_then(|result| result)
        }
    });
    let mut inners = Vec::with_capacity(num_replicas);
    let mut first_error = None;
    for result in join_all(futures).await {
        match result {
            Ok(adapter) => inners.push(adapter),
            Err(e) => {
                warn!("discarding replica after open failure: {e}");
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
    }
    if inners.len() < min_replicas {
        if cancel_on_failure {
            if let Err(cancel_err) = cancel_opened(&mut inners, budget).await {
                warn!(
                    "failed to cancel {} opened replicas: {}",
                    inners.len(),
                    cancel_err
                );
            }
        }
        return Err(first_error
            .unwrap_or_else(|| FsError::common("no available replica"))
            .ctx(format!(
                "open block: healthy replicas {}, required {}",
                inners.len(),
                min_replicas
            )));
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

/// Keeps `client_write_inflight_requests` balanced when the enclosing future is
/// dropped. `CallCtx::abandon` aborts the buffer task mid-write, so a plain
/// `inc`/`dec` pair would leak one in-flight slot per aborted chunk.
struct InflightGuard<'a>(&'a Gauge);

impl<'a> InflightGuard<'a> {
    fn new(gauge: &'a Gauge) -> Self {
        gauge.inc();
        Self(gauge)
    }
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.0.dec();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplicaState {
    Active,
    // If a stage future is dropped, this state remains and prevents reuse.
    InFlight,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Write,
    Flush,
    Seek,
    Complete,
    Cancel,
}

struct ReplicaWriter {
    adapter: WriterAdapter,
    state: ReplicaState,
}

impl ReplicaWriter {
    async fn call(
        &mut self,
        stage: Stage,
        chunk: DataSlice,
        pos: i64,
        budget: Duration,
    ) -> FsResult<()> {
        match (self.state, stage) {
            // Retry an interrupted Complete merge without re-sending a commit.
            // In particular, cleanup must never abort an acknowledged replica.
            (ReplicaState::Completed, Stage::Complete | Stage::Cancel)
            | (ReplicaState::Cancelled, Stage::Cancel) => return Ok(()),
            (ReplicaState::Active, _) => (),
            _ => return err_box!("replica session cannot be reused after {:?}", self.state),
        }
        self.state = ReplicaState::InFlight;
        let operation = async {
            match stage {
                Stage::Write => self.adapter.write(chunk).await,
                Stage::Flush => self.adapter.flush().await,
                Stage::Seek => self.adapter.seek(pos).await,
                Stage::Complete => self.adapter.complete().await,
                Stage::Cancel => self.adapter.cancel().await,
            }
        };
        let result = timeout(budget, operation)
            .await
            .map_err(|e| {
                FsError::from(e).ctx(format!("replica {stage:?} timeout after {budget:?}"))
            })
            .and_then(|result| result)
            .map_err(|e| e.ctx(format!("{stage:?} on {}", self.adapter.worker_address())));
        self.state = if result.is_err() {
            ReplicaState::Failed
        } else {
            match stage {
                Stage::Complete => ReplicaState::Completed,
                Stage::Cancel => ReplicaState::Cancelled,
                _ => ReplicaState::Active,
            }
        };
        result
    }
}

pub struct BlockWriter {
    inners: Vec<ReplicaWriter>,
    locate: LocatedBlock,
    min_replicas: usize,
    replica_timeout: Duration,
    // Last merged, acknowledged state; safe even when every replica is lost.
    pos: i64,
    len: i64,
    block_size: i64,
    terminal_error: Option<String>,
    pending_stage: Option<Stage>,
}

impl BlockWriter {
    pub async fn new(
        fs_context: Arc<FsContext>,
        locate: LocatedBlock,
        pos: i64,
        block_size: i64,
    ) -> FsResult<Self> {
        let conf = &fs_context.conf.client;
        let min_replicas = conf.write.min_replicas;
        let replica_timeout = conf.write_replica_timeout();
        if min_replicas == 0 || locate.locs.len() < min_replicas {
            return err_box!(
                "client.write.min_replicas={}, available workers={}",
                min_replicas,
                locate.locs.len()
            );
        }
        if pos < 0 || pos > block_size {
            return err_box!(
                "Invalid block write position {}, capacity {}",
                pos,
                block_size
            );
        }
        let inners = open_all(
            locate.locs.len(),
            min_replicas,
            replica_timeout,
            rollback_on_failure(&locate.block, pos),
            |idx| {
                WriterAdapter::new(
                    fs_context.clone(),
                    &locate,
                    &locate.locs[idx],
                    pos,
                    block_size,
                )
            },
        )
        .await?
        .into_iter()
        .map(|adapter| ReplicaWriter {
            adapter,
            state: ReplicaState::Active,
        })
        .collect();
        let len = locate.block.len;
        Ok(Self {
            inners,
            locate,
            min_replicas,
            replica_timeout,
            pos,
            len,
            block_size,
            terminal_error: None,
            pending_stage: None,
        })
    }

    async fn run_stage(&mut self, stage: Stage, chunk: DataSlice, pos: i64) -> FsResult<()> {
        if let Some(error) = &self.terminal_error {
            return Err(FsError::common(error.clone()));
        }
        if let Some(pending) = self.pending_stage {
            // Only Complete can be resumed safely: acknowledged commits are
            // remembered individually. Replaying a cancelled write could append
            // the same bytes twice on replicas that already acknowledged it.
            if pending != Stage::Complete || stage != Stage::Complete {
                let error = format!(
                    "block {}: cancelled {pending:?} stage cannot be replayed",
                    self.block_id()
                );
                self.terminal_error = Some(error.clone());
                return Err(FsError::common(error));
            }
        }
        self.pending_stage = Some(stage);
        let budget = self.replica_timeout;
        let results = join_all(
            self.inners
                .iter_mut()
                .map(|writer| writer.call(stage, chunk.clone(), pos, budget)),
        )
        .await;
        self.pending_stage = None;
        let mut first_error = None;
        let succeeded: Vec<_> = results.iter().map(Result::is_ok).collect();
        for result in results {
            if let Err(e) = result {
                warn!(
                    "block {}: discarding replica after {stage:?}: {e}",
                    self.block_id()
                );
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
        // Drop failed sessions permanently. Their connection/local ownership
        // guards close rather than pool an unfinished or cancelled session.
        let mut succeeded = succeeded.into_iter();
        self.inners.retain(|_| succeeded.next().unwrap_or(false));
        if self.inners.len() < self.min_replicas {
            let error = first_error
                .unwrap_or_else(|| FsError::common("no available replica"))
                .ctx(format!(
                    "block {} {stage:?}: healthy replicas {}, required {}",
                    self.block_id(),
                    self.inners.len(),
                    self.min_replicas
                ));
            self.terminal_error = Some(error.to_string());
            return Err(error);
        }
        if let Some(writer) = self.inners.first() {
            self.pos = writer.adapter.pos();
            self.len = writer.adapter.len();
        }
        Ok(())
    }

    pub async fn write(&mut self, chunk: DataSlice) -> FsResult<()> {
        let metrics = FsContext::get_metrics();
        let started = TimeSpent::new();
        let inflight = InflightGuard::new(&metrics.write_inflight);
        let result = self.run_stage(Stage::Write, chunk.freeze(), self.pos).await;
        drop(inflight);
        metrics.write_backend_us.observe(started.used_us() as f64);
        result
    }

    pub fn blocking_write(&mut self, rt: &Runtime, chunk: DataSlice) -> FsResult<()> {
        rt.block_on(self.write(chunk))
    }

    pub async fn flush(&mut self) -> FsResult<()> {
        self.run_stage(Stage::Flush, DataSlice::empty(), self.pos)
            .await
    }

    pub async fn complete(&mut self) -> FsResult<CommitBlock> {
        self.run_stage(Stage::Complete, DataSlice::empty(), self.pos)
            .await?;
        Ok(self.to_commit_block())
    }

    pub async fn cancel(&mut self) -> FsResult<()> {
        // Cleanup ignores the minimum and any terminal stage error, but skips
        // acknowledged Complete sessions and never reuses interrupted ones.
        let budget = self.replica_timeout;
        let result = finish_all_cancellations(
            self.inners
                .iter_mut()
                .map(|writer| writer.call(Stage::Cancel, DataSlice::empty(), 0, budget)),
        )
        .await;
        self.terminal_error
            .get_or_insert_with(|| "block writer cancelled".to_string());
        result
    }

    pub fn remaining(&self) -> i64 {
        self.block_size - self.pos
    }
    pub fn has_remaining(&self) -> bool {
        self.terminal_error.is_none() && self.remaining() > 0
    }
    pub fn pos(&self) -> i64 {
        self.pos
    }
    pub fn len(&self) -> i64 {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn block_id(&self) -> i64 {
        self.locate.block.id
    }

    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 || pos > self.block_size {
            return err_box!(
                "Invalid seek position {}, block capacity {}",
                pos,
                self.block_size
            );
        }
        self.run_stage(Stage::Seek, DataSlice::empty(), pos).await
    }

    fn to_commit_block(&self) -> CommitBlock {
        let locations = self
            .inners
            .iter()
            .filter(|writer| writer.state == ReplicaState::Completed)
            .map(|writer| {
                BlockLocation::new(
                    writer.adapter.worker_address().worker_id,
                    writer.adapter.actual_storage_type(),
                )
            })
            .collect();
        CommitBlock {
            block_id: self.block_id(),
            block_len: self.len,
            locations,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        cancel_opened, finish_all_cancellations, open_all, rollback_on_failure, InflightGuard,
        OpenedAdapter,
    };
    use curvine_error::{FsError, FsResult};
    use curvine_metrics::Metrics;
    use curvine_model::{ExtendedBlock, FileType, LocatedBlock, StorageType, WorkerAddress};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn inflight_guard_releases_the_slot_on_drop() {
        let gauge = Metrics::new_gauge("test_inflight_guard_drop", "test gauge").unwrap();
        {
            let _guard = InflightGuard::new(&gauge);
            assert_eq!(gauge.get(), 1);
        }
        assert_eq!(gauge.get(), 0);
    }

    // The buffer task is aborted by CallCtx::abandon while a write is awaiting
    // its replicas; the drop guard must release the in-flight slot.
    #[tokio::test]
    async fn inflight_guard_releases_the_slot_when_the_task_is_aborted() {
        let gauge = Metrics::new_gauge("test_inflight_guard_abort", "test gauge").unwrap();

        let task = {
            let gauge = gauge.clone();
            tokio::spawn(async move {
                let _guard = InflightGuard::new(&gauge);
                std::future::pending::<()>().await;
            })
        };
        tokio::task::yield_now().await;
        assert_eq!(gauge.get(), 1);

        task.abort();
        let _ = task.await;
        assert_eq!(gauge.get(), 0);
    }

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
        address: WorkerAddress,
    }

    impl ScriptedWriter {
        fn new(calls: Arc<AtomicUsize>, fail_cancel: bool, worker_id: u32) -> Self {
            Self {
                calls,
                fail_cancel,
                address: WorkerAddress {
                    worker_id,
                    ..Default::default()
                },
            }
        }
    }

    impl OpenedAdapter for ScriptedWriter {
        fn worker_address(&self) -> &WorkerAddress {
            &self.address
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

        let err = open_all(2, 2, std::time::Duration::from_secs(1), true, |idx| {
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

        let err = open_all(2, 2, std::time::Duration::from_secs(1), true, |idx| {
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

        let inners = open_all(3, 1, std::time::Duration::from_secs(1), true, |idx| {
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

        let result = cancel_opened(&mut inners, std::time::Duration::from_secs(1)).await;

        assert!(result.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "one failing cancel must not prevent the remaining cancellations"
        );
    }

    #[test]
    fn rollback_on_failure_gates_on_freshly_allocated_blocks() {
        let new_block = LocatedBlock::new(
            ExtendedBlock::new(1, 0, StorageType::Ssd, FileType::File),
            vec![],
        );
        assert!(rollback_on_failure(&new_block, 0));
        // A non-zero write offset means the caller seeks into existing data.
        assert!(!rollback_on_failure(&new_block, 64));

        // The master reports committed blocks with len > 0 (see
        // acquire_new_block: newly allocated blocks always carry len 0).
        let committed = LocatedBlock::new(
            ExtendedBlock::new(1, 64, StorageType::Ssd, FileType::File),
            vec![],
        );
        assert!(!rollback_on_failure(&committed, 0));
        assert!(!rollback_on_failure(&committed, 64));
    }
}
