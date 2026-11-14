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

use crate::worker::block::{BlockStore, BlockWriteLease};
use crate::worker::handler::{DirWriteMetrics, WriteContext};
use crate::worker::storage::BlockWriteContext;
use crate::worker::{Worker, WorkerMetrics};
use curvine_core_error::{err_box, ternary, try_option_mut, CommonResult};
use curvine_error::{ErrorKind, FsError, FsResult};
use curvine_model::{ExtendedBlock, FileAllocMode};
use curvine_proto::{BlockWriteResponse, DataHeaderProto};
use curvine_rpc::message::{Builder, Message, RequestStatus};
use curvine_runtime::common::{ByteUnit, TimeSpent};
use log::{info, warn};
use std::mem;

/// Returns true if `e` is a "no storage space" admission rejection from the
/// worker storage layer. `CommonError` is a type-erased `Box<dyn Error>` whose
/// payload is a formatted string (see `curvine_core_error::err_box!`), so we
/// match on the stable substrings emitted by `VfsDataset`/`DirList`/
/// `RobinChoosingPolicy`:
///   - "Not enough space in storage dir ..." (VfsDataset / DirList)
///   - "Not enough {:?} storage capacity for ... bytes" (RobinChoosingPolicy)
pub(crate) fn is_no_storage_space(e: &curvine_core_error::CommonError) -> bool {
    let msg = e.to_string();
    msg.contains("Not enough space")
        || (msg.contains("Not enough") && msg.contains("storage capacity"))
}

fn map_storage_open_error(e: curvine_core_error::CommonError) -> FsError {
    if is_no_storage_space(&e) {
        FsError::disk_out_of_space(e.to_string())
    } else {
        e.into()
    }
}

/// How a block left the Writing state, as seen by the raw meta.
#[derive(Clone, Copy, Debug)]
enum Terminal {
    Removed,
    Quarantined,
    /// A committed generation survives (rollback or another writer's
    /// replica); this writer's data still did not land.
    Finalized,
}

impl Terminal {
    fn as_str(self) -> &'static str {
        match self {
            Terminal::Removed => "removed",
            Terminal::Quarantined => "quarantined",
            Terminal::Finalized => "finalized",
        }
    }
}

/// An abort that failed non-terminally and must be retried as-is on the
/// next Complete/Cancel, instead of falling through to commit_block().
/// A replay that lands answers Ok to Cancel and the original error to
/// Complete; the handler then refuses any later Complete.
pub(crate) struct PendingAbort {
    pub(crate) block: ExtendedBlock,
    /// Retry with abort_block_uncertain() instead of abort_block().
    pub(crate) uncertain: bool,
    /// Short tag for logs: why the abort was needed.
    pub(crate) reason: &'static str,
    /// The original failure, re-returned on every retry. Only `IO` keeps its kind;
    /// every other kind is answered as `Common` with the same message.
    /// TODO: store the error wire-encoded (`ErrorExt::encode`/`decode`) to preserve every kind.
    pub(crate) kind: ErrorKind,
    pub(crate) error: String,
}

impl PendingAbort {
    fn new(block: &ExtendedBlock, uncertain: bool, reason: &'static str, error: &FsError) -> Self {
        debug_assert!(
            matches!(error.kind(), ErrorKind::IO | ErrorKind::Common),
            "to_error() only round-trips IO and Common"
        );
        Self {
            block: block.clone(),
            uncertain,
            reason,
            kind: error.kind(),
            error: Self::source_message(error),
        }
    }

    /// The source message only, matching what ErrorEncoder puts on the wire;
    /// Display would append ": " plus context.
    fn source_message(error: &FsError) -> String {
        match error {
            FsError::IO(e) => e.source.to_string(),
            FsError::Common(e) => e.source.to_string(),
            other => other.to_string(),
        }
    }

    // Only IO vs Common is distinguished; the call sites produce no other kinds.
    fn to_error(&self) -> FsError {
        match self.kind {
            ErrorKind::IO => FsError::io(std::io::Error::other(self.error.clone())),
            _ => FsError::common(&self.error),
        }
    }
}

pub struct WriteHandler {
    pub(crate) store: BlockStore,
    pub(crate) context: Option<WriteContext>,
    pub(crate) file: Option<BlockWriteContext>,
    pub(crate) lease: Option<BlockWriteLease>,
    /// Handler is closed: by this writer's commit, or (with `aborted`) by
    /// Cancel or an error exit on a terminal block.
    pub(crate) is_commit: bool,
    pub(crate) io_slow_us: u64,
    pub(crate) metrics: &'static WorkerMetrics,
    pub(crate) client_addr: String,
    pub(crate) pending_abort: Option<PendingAbort>,
    /// Closed without this writer's commit (Cancel succeeded, an abort landed,
    /// or finalize was refused on a terminal block): refuse a later Complete.
    /// Per handler object: `WorkerHandler::handle` keeps the handler after an error reply so the retry lands here.
    pub(crate) aborted: bool,
}

impl WriteHandler {
    pub fn new(store: BlockStore, client_addr: String) -> CommonResult<Self> {
        let conf = Worker::get_conf()?;
        let metrics = Worker::get_metrics()?;
        Ok(Self {
            store,
            context: None,
            file: None,
            lease: None,
            is_commit: false,
            io_slow_us: conf.worker.io_slow_us(),
            metrics,
            client_addr,
            pending_abort: None,
            aborted: false,
        })
    }

    pub(crate) fn record_write_error(
        metrics: &WorkerMetrics,
        dir_id: u32,
        e: &impl std::fmt::Display,
    ) {
        let dir_id = dir_id.to_string();
        warn!("Write to storage dir {} failed: {}", dir_id, e);
        metrics
            .write_error_count
            .with_label_values(&[dir_id.as_str()])
            .inc();
    }

    pub fn resize(file: &mut BlockWriteContext, ctx: &WriteContext) -> FsResult<()> {
        let opts = if let Some(opts) = &ctx.block.alloc_opts {
            opts
        } else {
            return Ok(());
        };
        opts.validate()?;
        if opts.len > ctx.block_size {
            return err_box!(
                "Invalid resize operation: allocation size {} > block size {}",
                opts.len,
                ctx.block_size
            );
        }
        if !file.supports_resize() {
            return Ok(());
        }

        let mut mode = opts.mode;
        mode.remove(FileAllocMode::KEEP_SIZE);
        file.resize(opts.truncate, opts.off, opts.len, mode.bits())?;

        if opts.len != file.device_len() {
            return err_box!(
                "invalid resize file {} operation: resize {} != actual {}, opts={:?}",
                file.path(),
                opts.len,
                file.device_len(),
                opts
            );
        }

        Ok(())
    }

    pub fn open(&mut self, msg: &Message) -> FsResult<Message> {
        let mut context = WriteContext::from_req(msg)?;
        if context.off < 0 || context.off > context.block_size {
            return err_box!(
                "Invalid write offset: {}, block size: {}",
                context.off,
                context.block_size
            );
        }
        if self.lease.is_some() {
            return err_box!("previous write lease is still active");
        }
        let lease = self.store.ordinary_write_lease(context.block.id)?;
        self.is_commit = false;
        self.pending_abort = None;
        self.aborted = false;

        let open_block = ExtendedBlock {
            len: context.block_size,
            ..context.block.clone()
        };

        let meta = match self.store.open_block(&open_block) {
            Ok(m) => m,
            Err(e) => {
                // `CommonError` is a type-erased string error (see
                // curvine_core_error::err_box!), so match on the stable
                // rejection messages emitted by the storage layer.
                if is_no_storage_space(&e) {
                    self.metrics.disk_full_rejected_writes.inc();
                }
                return Err(map_storage_open_error(e));
            }
        };
        context.dir_id = meta.dir_id();
        context.dir_metrics = Some(DirWriteMetrics::new(self.metrics, context.dir_id));
        let mut file = match self.store.open_writer(&meta, context.off) {
            Ok(file) => file,
            Err(e) => {
                if let Err(abort_err) = self.store.abort_block(&context.block) {
                    log::warn!(
                        "failed to abort block {} after open_writer error: {}",
                        context.block.id,
                        abort_err
                    );
                }
                return Err(e.into());
            }
        };
        if let Err(e) = Self::resize(&mut file, &context) {
            drop(file);
            if let Err(abort_err) = self.store.abort_block(&context.block) {
                log::warn!(
                    "failed to abort block {} after resize error: {}",
                    context.block.id,
                    abort_err
                );
            }
            return Err(e);
        }

        // Same source of truth as the read path: layout decides short-circuit
        // eligibility and the local path (None for layouts without one, e.g. bdev).
        let sc_path = if context.short_circuit {
            match self.store.short_circuit(&meta) {
                Ok(path) => path,
                Err(e) => {
                    drop(file);
                    if let Err(abort_err) = self.store.abort_block(&context.block) {
                        log::warn!(
                            "failed to abort block {} after short_circuit error: {}",
                            context.block.id,
                            abort_err
                        );
                    }
                    return Err(e.into());
                }
            }
        } else {
            None
        };
        let is_short_circuit = sc_path.is_some();
        let (label, path, file) = if let Some(path) = sc_path {
            ("local", path, None)
        } else {
            ("remote", file.path().to_string(), Some(file))
        };

        let log_msg = format!(
            "Write {}-block start req_id: {}, path: {:?}, chunk_size: {}, off: {}, block_size: {}, client: {}",
            label,
            context.req_id,
            path,
            context.chunk_size,
            context.off,
            ByteUnit::byte_to_string(context.block_size as u64),
            self.client_addr
        );

        let response = BlockWriteResponse {
            id: meta.id,
            path: ternary!(is_short_circuit, Some(path), None),
            off: context.off,
            block_size: context.block_size,
            storage_type: meta.storage_type().into(),
            pipeline_status: None,
        };

        let _ = mem::replace(&mut self.file, file);
        let dir_id = context.dir_id.to_string();
        let _ = self.context.replace(context);
        self.lease = lease;

        self.metrics
            .write_blocks
            .with_label_values(&[dir_id.as_str(), label])
            .inc();

        info!("{}", log_msg);
        Ok(Builder::success(msg).proto_header(response).build())
    }

    fn check_context(context: &WriteContext, msg: &Message) -> FsResult<()> {
        if context.req_id != msg.req_id() {
            return err_box!(
                "Request id mismatch, expected {}, actual {}",
                context.req_id,
                msg.req_id()
            );
        }
        Ok(())
    }

    fn handle_data_header(
        file: &mut BlockWriteContext,
        context: &WriteContext,
        header: DataHeaderProto,
    ) -> FsResult<bool> {
        if header.flush {
            return Ok(true);
        }

        if header.offset < 0 || header.offset >= context.block_size {
            return err_box!(
                "Invalid seek offset: {}, block length: {}",
                header.offset,
                context.block_size
            );
        }

        file.seek_to(header.offset)?;
        Ok(false)
    }

    /// Quarantine as soon as a write is known uncertain, so the state lives in
    /// the dataset even if the client never sends Complete/Cancel.
    fn quarantine_if_uncertain(
        store: &BlockStore,
        block: &ExtendedBlock,
        file: &BlockWriteContext,
    ) {
        if !file.has_uncertain_writes() {
            return;
        }
        if let Err(e) = store.abort_block_uncertain(block) {
            warn!(
                "failed to quarantine block {} after uncertain write: {}",
                block.id, e
            );
        }
    }

    pub fn write(&mut self, msg: &Message) -> FsResult<Message> {
        let file = try_option_mut!(self.file);
        let context = try_option_mut!(self.context);
        Self::check_context(context, msg)?;

        let mut need_flush = false;
        if msg.header_len() > 0 {
            let header: DataHeaderProto = msg.parse_header()?;
            need_flush = Self::handle_data_header(file, context, header)?;
        }

        let data_len = msg.data_len() as i64;
        if data_len > 0 {
            let spend = TimeSpent::new();
            if let Err(e) = file.write_region(&msg.data) {
                Self::record_write_error(self.metrics, context.dir_id, &e);
                Self::quarantine_if_uncertain(&self.store, &context.block, file);
                return Err(e.into());
            }

            let used = spend.used_us();
            let dir_metrics = context.dir_metrics.as_ref();
            if used >= self.io_slow_us {
                warn!(
                    "Slow write data from disk cost: {}us (threshold={}us), dir: {}, path: {} ",
                    used,
                    self.io_slow_us,
                    context.dir_id,
                    file.path()
                );
                if let Some(dir_metrics) = dir_metrics {
                    dir_metrics.slow_write_count.inc();
                }
            }
            if let Some(dir_metrics) = dir_metrics {
                dir_metrics.write_bytes.inc_by(data_len);
                dir_metrics.write_time_us.inc_by(used as i64);
                dir_metrics.write_count.inc();
            }
        }

        if need_flush {
            if let Err(e) = file.flush() {
                Self::record_write_error(self.metrics, context.dir_id, &e);
                Self::quarantine_if_uncertain(&self.store, &context.block, file);
                return Err(e.into());
            }
        }

        Ok(msg.success())
    }

    fn commit_block(&self, block: &ExtendedBlock, commit: bool) -> FsResult<()> {
        if commit {
            self.store.finalize_block(block)?;
        } else {
            self.store.abort_block(block)?;
        }
        Ok(())
    }

    /// Raw-meta verdict after an abort/finalize attempt. `Some` is terminal
    /// (safe to latch is_commit); `None` means still Writing or unreadable,
    /// so stay retryable.
    fn terminal_state(&self, block_id: i64) -> Option<Terminal> {
        match self.store.get_raw_block_opt(block_id) {
            Ok(Some(meta)) if meta.is_quarantined() => Some(Terminal::Quarantined),
            Ok(Some(meta)) if meta.is_final() => Some(Terminal::Finalized),
            Ok(Some(_)) => None,
            Ok(None) => Some(Terminal::Removed),
            Err(_) => None,
        }
    }

    /// Close the handler with this writer's data aborted.
    fn close_aborted(&mut self) {
        self.is_commit = true;
        self.aborted = true;
        self.lease.take();
    }

    /// Abort (quarantine if `pending.uncertain`), then latch if terminal or
    /// park `pending` unchanged for the next retry. Returns the client error,
    /// never re-derived, so the message is stable across retries.
    fn abort_or_defer(&mut self, pending: PendingAbort) -> FsError {
        let abort_result = if pending.uncertain {
            self.store.abort_block_uncertain(&pending.block)
        } else {
            self.store.abort_block(&pending.block)
        };
        if let Err(abort_err) = abort_result {
            log::warn!(
                "failed to abort block {} after {}, client {}: {}",
                pending.block.id,
                pending.reason,
                self.client_addr,
                abort_err
            );
        }
        let error = pending.to_error();
        match self.terminal_state(pending.block.id) {
            Some(state) => {
                info!(
                    "abort of block {} after {} landed: {}, client {}",
                    pending.block.id,
                    pending.reason,
                    state.as_str(),
                    self.client_addr
                );
                self.close_aborted();
            }
            None => self.pending_abort = Some(pending),
        }
        error
    }

    pub fn complete(&mut self, msg: &Message, commit: bool) -> FsResult<Message> {
        if self.is_commit {
            return if !msg.data.is_empty() {
                if self.aborted {
                    err_box!("The block write was aborted and data cannot be written anymore.")
                } else {
                    err_box!("The block has been committed and data cannot be written anymore.")
                }
            } else if self.aborted && commit {
                // Closed without a commit: never let a Complete read as "committed".
                // Per handler only; after a successful close the next Complete gets a fresh handler.
                err_box!("The block write was aborted and cannot be committed.")
            } else {
                Ok(msg.success())
            };
        }

        // A prior abort failed non-terminally: self.file is already gone, so
        // retry only that abort. Never fall through to commit_block().
        if let Some(pending) = self.pending_abort.take() {
            let error = self.abort_or_defer(pending);
            // Terminal replay answers by intent: a Cancel got what it asked
            // for; a Complete did not.
            return if self.is_commit && !commit && msg.data.is_empty() {
                Ok(msg.success())
            } else {
                Err(error)
            };
        }

        let context = WriteContext::from_req(msg)?;
        if let Some(opened) = self.context.as_ref() {
            Self::check_context(opened, msg)?;
            // Commit/abort act on the header's block, so it must be the opened one.
            // In-tree clients always send it (BlockClient uses the writer's own
            // block); this only guards crafted frames on the unauthenticated data port.
            if opened.block.id != context.block.id {
                return err_box!(
                    "Block id mismatch, expected {}, actual {}",
                    opened.block.id,
                    context.block.id
                );
            }
        }
        let write_dir_id = self.context.take().map(|c| c.dir_id);

        let file = self.file.take();
        if let Some(mut file) = file {
            let flush_result = file.flush();
            // Sticky flag from a mid-stream timeout; read it even if flush is Ok.
            let uncertain = file.has_uncertain_writes();
            drop(file);
            if let Err(flush_err) = flush_result {
                if let Some(dir_id) = write_dir_id {
                    Self::record_write_error(self.metrics, dir_id, &flush_err);
                }
                let error: FsError = flush_err.into();
                let pending = PendingAbort::new(&context.block, uncertain, "flush error", &error);
                return Err(self.abort_or_defer(pending));
            }
            if uncertain {
                // Data may still be in flight: never free or publish this extent.
                let error = FsError::io(std::io::Error::other(format!(
                    "block {} write outcome uncertain, extent quarantined",
                    context.block.id
                )));
                let pending = PendingAbort::new(&context.block, true, "uncertain write", &error);
                return Err(self.abort_or_defer(pending));
            }
        }

        if context.block.len > context.block_size {
            let error = FsError::common(format!(
                "Invalid block length: {}, block size: {}",
                context.block.len, context.block_size
            ));
            let pending = PendingAbort::new(&context.block, false, "length check failed", &error);
            return Err(self.abort_or_defer(pending));
        }

        if let Err(commit_err) = self.commit_block(&context.block, commit) {
            log::warn!(
                "failed to commit block {} (commit={}): {}",
                context.block.id,
                commit,
                commit_err
            );
            // Only latch if truly terminal; stay retryable otherwise. Use the
            // raw pending meta, not the readable view: during a rewrite it can
            // mask a transient rollback.
            if self.terminal_state(context.block.id).is_some() {
                self.close_aborted();
                // Same rule as the replay: a Cancel that ended terminal got
                // what it asked for.
                if !commit && msg.data.is_empty() {
                    return Ok(msg.success());
                }
            }
            return Err(commit_err);
        }
        if commit {
            self.is_commit = true;
            self.lease.take();
        } else {
            self.close_aborted();
        }

        info!(
            "write block end for req_id {}, is commit: {}, off: {}, len: {}, client: {}",
            msg.req_id(),
            commit,
            context.off,
            context.block.len,
            self.client_addr
        );

        Ok(msg.success())
    }

    pub fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let request_status = msg.request_status();

        match request_status {
            RequestStatus::Open => self.open(msg),

            RequestStatus::Running => self.write(msg),

            RequestStatus::Complete => self.complete(msg, true),

            RequestStatus::Cancel => self.complete(msg, false),

            _ => err_box!("Unsupported request type"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{map_storage_open_error, WriteHandler};
    use crate::worker::block::BlockStore;
    use crate::worker::handler::WriteContext;
    use crate::worker::storage::{BlockWriteContext, Dataset};
    use crate::worker::Worker;
    use curvine_config::{ClusterConf, WorkerConf};
    use curvine_error::{ErrorKind, FsError, FsResult};
    use curvine_io::{BlockIO, DataSlice};
    use curvine_model::{ExtendedBlock, FileType, StorageType};
    use curvine_proto::DataHeaderProto;
    use curvine_rpc::message::{Builder, Message, RequestStatus};
    use std::sync::{Arc, Mutex};

    #[test]
    fn storage_capacity_rejection_maps_to_disk_out_of_space() {
        let error = curvine_core_error::err_msg!(
            "Not enough space in storage dir 1 for block 2 rewrite: need 20, available 10"
        );

        assert!(matches!(
            map_storage_open_error(error.into()),
            FsError::DiskOutOfSpace(_)
        ));
    }

    #[test]
    fn unrelated_storage_error_remains_common() {
        let error = curvine_core_error::err_msg!("failed to open staging file");

        assert!(matches!(
            map_storage_open_error(error.into()),
            FsError::Common(_)
        ));
    }

    // Same construction pattern already used by
    // curvine-worker/src/worker/block/block_store.rs (`create_store_with_capacity`)
    // and curvine-worker/src/worker/block/tests/heartbeat_task_test.rs
    // (`create_store`): a real BlockStore backed by an in-memory test data dir.
    fn create_test_store(name: &str) -> FsResult<BlockStore> {
        let conf = ClusterConf {
            format_worker: true,
            worker: WorkerConf {
                dir_reserved: "0".to_string(),
                data_dir: vec![format!("[MEM:1KB]../testing/write-handler-{name}")],
                ..WorkerConf::default()
            },
            ..ClusterConf::default()
        };
        // Initialize metrics/config explicitly; these tests must not depend on test order.
        let _worker = Worker::with_conf(conf.clone())?;
        BlockStore::new("test", &conf).map_err(FsError::from)
    }

    struct RecordingBlockIO {
        operations: Arc<Mutex<Vec<&'static str>>>,
    }

    impl BlockIO for RecordingBlockIO {
        fn read_region(
            &mut self,
            _enable_send_file: bool,
            _len: i32,
        ) -> curvine_io::IOResult<DataSlice> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "read not supported",
            )))
        }

        fn write_region(&mut self, _region: &DataSlice) -> curvine_io::IOResult<()> {
            self.operations.lock().unwrap().push("write");
            Ok(())
        }

        fn write_all(&mut self, _buf: &[u8]) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "write_all not supported",
            )))
        }

        fn read_all(&mut self, _buf: &mut [u8]) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "read_all not supported",
            )))
        }

        fn flush(&mut self) -> curvine_io::IOResult<()> {
            self.operations.lock().unwrap().push("flush");
            Ok(())
        }

        fn seek(&mut self, pos: i64) -> curvine_io::IOResult<i64> {
            Ok(pos)
        }

        fn pos(&self) -> i64 {
            0
        }

        fn len(&self) -> i64 {
            0
        }

        fn path(&self) -> &str {
            "recording"
        }

        fn resize(
            &mut self,
            _truncate: bool,
            _off: i64,
            _len: i64,
            _mode: i32,
        ) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "resize not supported",
            )))
        }
    }

    struct FailingFlushBlockIO;

    impl BlockIO for FailingFlushBlockIO {
        fn read_region(
            &mut self,
            _enable_send_file: bool,
            _len: i32,
        ) -> curvine_io::IOResult<DataSlice> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "read not supported",
            )))
        }

        fn write_region(&mut self, _region: &DataSlice) -> curvine_io::IOResult<()> {
            Ok(())
        }

        fn write_all(&mut self, _buf: &[u8]) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "write_all not supported",
            )))
        }

        fn read_all(&mut self, _buf: &mut [u8]) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "read_all not supported",
            )))
        }

        fn flush(&mut self) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "flush failed",
            )))
        }

        fn seek(&mut self, pos: i64) -> curvine_io::IOResult<i64> {
            Ok(pos)
        }

        fn pos(&self) -> i64 {
            0
        }

        fn len(&self) -> i64 {
            0
        }

        fn path(&self) -> &str {
            "failing-flush"
        }

        fn resize(
            &mut self,
            _truncate: bool,
            _off: i64,
            _len: i64,
            _mode: i32,
        ) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "resize not supported",
            )))
        }
    }

    /// Reports uncertain writes, to exercise `abort_block_uncertain` routing.
    struct FailingFlushUncertainBlockIO;

    impl BlockIO for FailingFlushUncertainBlockIO {
        fn read_region(
            &mut self,
            _enable_send_file: bool,
            _len: i32,
        ) -> curvine_io::IOResult<DataSlice> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "read not supported",
            )))
        }

        fn write_region(&mut self, _region: &DataSlice) -> curvine_io::IOResult<()> {
            Ok(())
        }

        fn write_all(&mut self, _buf: &[u8]) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "write_all not supported",
            )))
        }

        fn read_all(&mut self, _buf: &mut [u8]) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "read_all not supported",
            )))
        }

        fn flush(&mut self) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "flush timed out",
            )))
        }

        fn seek(&mut self, pos: i64) -> curvine_io::IOResult<i64> {
            Ok(pos)
        }

        fn pos(&self) -> i64 {
            0
        }

        fn len(&self) -> i64 {
            0
        }

        fn path(&self) -> &str {
            "failing-flush-uncertain"
        }

        fn resize(
            &mut self,
            _truncate: bool,
            _off: i64,
            _len: i64,
            _mode: i32,
        ) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "resize not supported",
            )))
        }

        fn has_uncertain_writes(&self) -> bool {
            true
        }
    }

    /// A mid-stream write timed out (sticky flag) but the later flush is Ok.
    struct UncertainFlushOkBlockIO;

    impl BlockIO for UncertainFlushOkBlockIO {
        fn read_region(
            &mut self,
            _enable_send_file: bool,
            _len: i32,
        ) -> curvine_io::IOResult<DataSlice> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "read not supported",
            )))
        }

        fn write_region(&mut self, _region: &DataSlice) -> curvine_io::IOResult<()> {
            Ok(())
        }

        fn write_all(&mut self, _buf: &[u8]) -> curvine_io::IOResult<()> {
            Ok(())
        }

        fn read_all(&mut self, _buf: &mut [u8]) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "read_all not supported",
            )))
        }

        fn flush(&mut self) -> curvine_io::IOResult<()> {
            Ok(())
        }

        fn seek(&mut self, pos: i64) -> curvine_io::IOResult<i64> {
            Ok(pos)
        }

        fn pos(&self) -> i64 {
            0
        }

        fn len(&self) -> i64 {
            0
        }

        fn path(&self) -> &str {
            "uncertain-flush-ok"
        }

        fn resize(
            &mut self,
            _truncate: bool,
            _off: i64,
            _len: i64,
            _mode: i32,
        ) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "resize not supported",
            )))
        }

        fn has_uncertain_writes(&self) -> bool {
            true
        }
    }

    /// A mid-stream write times out: write_region fails and the flag is set.
    struct UncertainWriteBlockIO;

    impl BlockIO for UncertainWriteBlockIO {
        fn read_region(
            &mut self,
            _enable_send_file: bool,
            _len: i32,
        ) -> curvine_io::IOResult<DataSlice> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "read not supported",
            )))
        }

        fn write_region(&mut self, _region: &DataSlice) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "write timed out",
            )))
        }

        fn write_all(&mut self, _buf: &[u8]) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "write timed out",
            )))
        }

        fn read_all(&mut self, _buf: &mut [u8]) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "read_all not supported",
            )))
        }

        fn flush(&mut self) -> curvine_io::IOResult<()> {
            Ok(())
        }

        fn seek(&mut self, pos: i64) -> curvine_io::IOResult<i64> {
            Ok(pos)
        }

        fn pos(&self) -> i64 {
            0
        }

        fn len(&self) -> i64 {
            0
        }

        fn path(&self) -> &str {
            "uncertain-write"
        }

        fn resize(
            &mut self,
            _truncate: bool,
            _off: i64,
            _len: i64,
            _mode: i32,
        ) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "resize not supported",
            )))
        }

        fn has_uncertain_writes(&self) -> bool {
            true
        }
    }

    /// SPDK store + handler over the given writer fake.
    fn spdk_handler<IO: BlockIO + 'static>(
        dir_tag: &str,
        block: &ExtendedBlock,
        io: IO,
    ) -> FsResult<(BlockStore, WriteHandler)> {
        let _worker = Worker::with_conf(ClusterConf {
            format_worker: true,
            worker: WorkerConf {
                data_dir: vec![format!("[MEM:1KB]../testing/{dir_tag}")],
                ..WorkerConf::default()
            },
            ..ClusterConf::default()
        })?;
        let dataset = curvine_storage_local::BlockDataset::new_spdk_test_dataset(1, 1 << 30)
            .map_err(FsError::from)?;
        let store = BlockStore::from_dataset_for_test(dataset);
        store.open_block(block).map_err(FsError::from)?;
        let handler = WriteHandler {
            store: store.clone(),
            context: Some(WriteContext {
                block: block.clone(),
                req_id: 1,
                chunk_size: 1024,
                short_circuit: false,
                off: 0,
                block_size: block.len,
                dir_id: 0,
                dir_metrics: None,
            }),
            file: Some(BlockWriteContext::new(io, 0, block.len, 0)?),
            lease: None,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };
        Ok((store, handler))
    }

    fn spdk_complete_msg(block: &ExtendedBlock) -> Message {
        Builder::new()
            .code(curvine_fs_api::RpcCode::WriteBlock)
            .request(RequestStatus::Complete)
            .req_id(1)
            .seq_id(1)
            .proto_header(curvine_proto::BlockWriteRequest {
                block: curvine_model::ProtoUtils::extend_block_to_pb(block.clone()),
                off: 0,
                block_size: block.len,
                short_circuit: false,
                client_name: "test".to_string(),
                chunk_size: 1024,
                pipeline_stream: Vec::new(),
                component_info: None,
            })
            .data(DataSlice::Empty)
            .build()
    }

    // Regression test for GH#1673: a data-bearing Running frame with flush=true
    // must write the payload before calling file.flush().
    #[test]
    fn flush_header_is_applied_after_payload_write() -> FsResult<()> {
        let block_size = 1024_i64;
        let operations = Arc::new(Mutex::new(Vec::new()));

        let file = BlockWriteContext::new(
            RecordingBlockIO {
                operations: operations.clone(),
            },
            0,
            block_size,
            0,
        )?;

        let context = WriteContext {
            block: ExtendedBlock::new(1, 0, StorageType::Disk, FileType::File),
            req_id: 1,
            chunk_size: 1024,
            short_circuit: false,
            off: 0,
            block_size,
            dir_id: 0,
            dir_metrics: None,
        };

        let store = create_test_store("flush-order")?;

        let mut handler = WriteHandler {
            store,
            context: Some(context),
            file: Some(file),
            lease: None,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };

        let header = DataHeaderProto {
            offset: block_size,
            flush: true,
            is_last: false,
        };

        let msg = Builder::new()
            .code(curvine_fs_api::RpcCode::WriteBlock)
            .request(RequestStatus::Running)
            .req_id(1)
            .seq_id(1)
            .proto_header(header)
            .data(DataSlice::Buffer(prost::bytes::BytesMut::from(
                &b"flush-test"[..],
            )))
            .build();

        let _: Message = handler.write(&msg)?;

        assert_eq!(
            operations.lock().unwrap().as_slice(),
            ["write", "flush"],
            "a data-bearing flush frame must write the payload before flushing"
        );

        Ok(())
    }

    // Regression test for GH#1673: a flush error surfaced by the underlying
    // BlockIO must propagate out of WriteHandler::write(), not be swallowed.
    #[test]
    fn flush_header_propagates_flush_error() -> FsResult<()> {
        let block_size = 1024_i64;

        let file = BlockWriteContext::new(FailingFlushBlockIO, 0, block_size, 0)?;

        let context = WriteContext {
            block: ExtendedBlock::new(1, 0, StorageType::Disk, FileType::File),
            req_id: 1,
            chunk_size: 1024,
            short_circuit: false,
            off: 0,
            block_size,
            dir_id: 4242,
            dir_metrics: None,
        };

        let store = create_test_store("flush-error")?;

        let mut handler = WriteHandler {
            store,
            context: Some(context),
            file: Some(file),
            lease: None,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };

        let header = DataHeaderProto {
            offset: block_size,
            flush: true,
            is_last: false,
        };

        let msg = Builder::new()
            .code(curvine_fs_api::RpcCode::WriteBlock)
            .request(RequestStatus::Running)
            .req_id(1)
            .seq_id(1)
            .proto_header(header)
            .data(DataSlice::Buffer(prost::bytes::BytesMut::from(
                &b"flush-test"[..],
            )))
            .build();

        let err = handler.write(&msg).unwrap_err();

        assert!(
            err.to_string().contains("flush failed"),
            "unexpected error: {err}"
        );

        // The failure must be attributed to the opened storage dir, not to the
        // default `dir_id` of a freshly built context.
        assert_eq!(
            handler
                .metrics
                .write_error_count
                .with_label_values(&["4242"])
                .get(),
            1,
            "a failed flush must increment write_error_count for its own dir_id"
        );

        Ok(())
    }

    // `open()` must attribute the writer to the directory that actually owns
    // the block, and resolve the per-dir counters from that label once.
    #[test]
    fn open_attributes_writer_to_block_storage_dir() -> FsResult<()> {
        let block_size = 512_i64;
        let store = create_test_store("open-dir-id")?;
        let block = ExtendedBlock::new(1, 0, StorageType::Mem, FileType::File);

        let mut handler = WriteHandler::new(store, "test".to_string())?;
        let req = curvine_proto::BlockWriteRequest {
            block: curvine_model::ProtoUtils::extend_block_to_pb(ExtendedBlock {
                len: block_size,
                ..block.clone()
            }),
            off: 0,
            block_size,
            short_circuit: false,
            client_name: "test".to_string(),
            chunk_size: block_size as i32,
            pipeline_stream: Vec::new(),
            component_info: None,
        };
        let msg = Builder::new()
            .code(curvine_fs_api::RpcCode::WriteBlock)
            .request(RequestStatus::Open)
            .req_id(1)
            .seq_id(0)
            .proto_header(req)
            .build();

        handler.open(&msg)?;

        let meta = handler.store.get_block(block.id).map_err(FsError::from)?;
        let context = handler
            .context
            .as_ref()
            .expect("open must keep the write context");
        assert_eq!(context.dir_id, meta.dir_id());
        assert!(
            context.dir_metrics.is_some(),
            "open must resolve the per-dir counters"
        );

        Ok(())
    }

    // On non-SPDK storage, `abort_block_uncertain` degrades to a plain
    // `abort_block`, so this only proves error propagation + cleanup, not
    // quarantine routing itself (see the SPDK-specific test below for that).
    #[test]
    fn complete_routes_uncertain_flush_failure_removes_block_on_non_spdk_storage() -> FsResult<()> {
        let block_size = 1024_i64;
        let block = ExtendedBlock::new(42, 0, StorageType::Disk, FileType::File);

        let store = create_test_store("complete-uncertain-flush")?;
        let meta = store.open_block(&block).map_err(FsError::from)?;
        assert!(store.get_block(meta.id).is_ok());

        let file = BlockWriteContext::new(FailingFlushUncertainBlockIO, 0, block_size, 0)?;
        let context = WriteContext {
            block: block.clone(),
            req_id: 1,
            chunk_size: 1024,
            short_circuit: false,
            off: 0,
            block_size,
            dir_id: 0,
            dir_metrics: None,
        };

        let mut handler = WriteHandler {
            store: store.clone(),
            context: Some(context),
            file: Some(file),
            lease: None,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };

        let msg = Builder::new()
            .code(curvine_fs_api::RpcCode::WriteBlock)
            .request(RequestStatus::Complete)
            .req_id(1)
            .seq_id(1)
            .proto_header(curvine_proto::BlockWriteRequest {
                block: curvine_model::ProtoUtils::extend_block_to_pb(block.clone()),
                off: 0,
                block_size,
                short_circuit: false,
                client_name: "test".to_string(),
                chunk_size: 1024,
                pipeline_stream: Vec::new(),
                component_info: None,
            })
            .data(DataSlice::Empty)
            .build();

        let err = handler.complete(&msg, true).unwrap_err();
        assert!(
            err.to_string().contains("flush timed out"),
            "unexpected error: {err}"
        );

        // Block must be cleaned up, not left dangling.
        assert!(
            store.get_block(meta.id).is_err(),
            "block must be cleaned up after uncertain flush failure"
        );

        Ok(())
    }

    // SPDK: uncertain flush must quarantine, not remove the block.
    // Detector itself is tested separately in curvine-storage-spdk::spdk_bdev.
    #[test]
    fn complete_routes_uncertain_flush_failure_to_quarantine_on_spdk() -> FsResult<()> {
        let block_size = 4096_i64;
        let block = ExtendedBlock::new(42, block_size, StorageType::SpdkDisk, FileType::File);

        // Just to init worker metrics; dataset/store below bypass this conf.
        let _worker = Worker::with_conf(ClusterConf {
            format_worker: true,
            worker: WorkerConf {
                data_dir: vec!["[MEM:1KB]../testing/write-handler-spdk-quarantine".to_string()],
                ..WorkerConf::default()
            },
            ..ClusterConf::default()
        })?;

        let dataset = curvine_storage_local::BlockDataset::new_spdk_test_dataset(1, 1 << 30)
            .map_err(FsError::from)?;
        let store = BlockStore::from_dataset_for_test(dataset);
        let meta = store.open_block(&block).map_err(FsError::from)?;
        assert!(store.get_block(meta.id).is_ok());

        let file = BlockWriteContext::new(FailingFlushUncertainBlockIO, 0, block_size, 0)?;
        let context = WriteContext {
            block: block.clone(),
            req_id: 1,
            chunk_size: 1024,
            short_circuit: false,
            off: 0,
            block_size,
            dir_id: 0,
            dir_metrics: None,
        };

        let mut handler = WriteHandler {
            store: store.clone(),
            context: Some(context),
            file: Some(file),
            lease: None,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };

        let msg = Builder::new()
            .code(curvine_fs_api::RpcCode::WriteBlock)
            .request(RequestStatus::Complete)
            .req_id(1)
            .seq_id(1)
            .proto_header(curvine_proto::BlockWriteRequest {
                block: curvine_model::ProtoUtils::extend_block_to_pb(block.clone()),
                off: 0,
                block_size,
                short_circuit: false,
                client_name: "test".to_string(),
                chunk_size: 1024,
                pipeline_stream: Vec::new(),
                component_info: None,
            })
            .data(DataSlice::Empty)
            .build();

        let err = handler.complete(&msg, true).unwrap_err();
        assert!(
            err.to_string().contains("flush timed out"),
            "unexpected error: {err}"
        );

        // Block must be quarantined, not removed: extent stays reserved.
        // Raw view: a quarantined block is deliberately not readable.
        let meta_after = store
            .get_raw_block_opt(meta.id)
            .map_err(FsError::from)?
            .expect("quarantined block must still exist");
        assert_eq!(
            meta_after.state(),
            &curvine_storage_local::BlockState::Quarantined,
            "uncertain flush failure on SPDK must quarantine the block, not remove it"
        );
        assert!(
            store.get_block(meta.id).is_err(),
            "quarantined block must not be readable"
        );

        Ok(())
    }

    /// Mid-stream write timed out, then Cancel with a successful flush: the
    /// sticky uncertain flag must still quarantine, not free the extent.
    #[test]
    fn cancel_after_uncertain_write_with_ok_flush_quarantines() -> FsResult<()> {
        let block = ExtendedBlock::new(50, 4096, StorageType::SpdkDisk, FileType::File);
        let (store, mut handler) = spdk_handler(
            "write-handler-cancel-uncertain-ok-flush",
            &block,
            UncertainFlushOkBlockIO,
        )?;
        let available_before = store.read().map_err(FsError::from)?.available();

        let err = handler
            .complete(&spdk_complete_msg(&block), false)
            .unwrap_err();
        assert!(err.to_string().contains("uncertain"), "{err}");

        assert_eq!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Quarantined),
            "Cancel after an uncertain write must quarantine, not free"
        );
        assert_eq!(
            store.read().map_err(FsError::from)?.available(),
            available_before,
            "quarantine must keep the extent reserved"
        );
        assert!(handler.is_commit, "quarantine is terminal: must latch");
        Ok(())
    }

    /// Same with Complete(commit=true): quarantine, never finalize.
    #[test]
    fn complete_after_uncertain_write_with_ok_flush_never_finalizes() -> FsResult<()> {
        let block = ExtendedBlock::new(51, 4096, StorageType::SpdkDisk, FileType::File);
        let (store, mut handler) = spdk_handler(
            "write-handler-commit-uncertain-ok-flush",
            &block,
            UncertainFlushOkBlockIO,
        )?;

        let err = handler
            .complete(&spdk_complete_msg(&block), true)
            .unwrap_err();
        assert!(err.to_string().contains("uncertain"), "{err}");

        assert_eq!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Quarantined),
            "Complete after an uncertain write must quarantine, not finalize"
        );
        assert!(handler.is_commit, "quarantine is terminal: must latch");
        Ok(())
    }

    fn spdk_data_frame() -> Message {
        Builder::new()
            .code(curvine_fs_api::RpcCode::WriteBlock)
            .request(RequestStatus::Running)
            .req_id(1)
            .seq_id(1)
            .proto_header(DataHeaderProto {
                offset: 0,
                flush: false,
                is_last: false,
            })
            .data(DataSlice::Buffer(prost::bytes::BytesMut::from(&b"x"[..])))
            .build()
    }

    /// A timed-out write must quarantine immediately, before any
    /// Complete/Cancel: the state must not depend on the client coming back.
    #[test]
    fn uncertain_write_quarantines_eagerly_before_complete() -> FsResult<()> {
        let block = ExtendedBlock::new(52, 4096, StorageType::SpdkDisk, FileType::File);
        let (store, mut handler) = spdk_handler(
            "write-handler-eager-quarantine",
            &block,
            UncertainWriteBlockIO,
        )?;
        let available_before = store.read().map_err(FsError::from)?.available();

        let err = handler.write(&spdk_data_frame()).unwrap_err();
        assert!(err.to_string().contains("write timed out"), "{err}");

        assert_eq!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Quarantined),
            "block must be quarantined as soon as the write is uncertain"
        );
        assert_eq!(
            store.read().map_err(FsError::from)?.available(),
            available_before,
            "extent must stay reserved"
        );

        // Client disconnects: handler is dropped. A later master delete must
        // still be refused, not free the extent.
        drop(handler);
        let remove_err = store.async_remove_block(block.id).unwrap_err();
        assert!(
            curvine_storage_local::is_quarantined_error(&remove_err),
            "{remove_err}"
        );
        assert_eq!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Quarantined),
        );
        Ok(())
    }

    /// Eager quarantine, then the client does send Cancel: idempotent, latches.
    #[test]
    fn cancel_after_eager_quarantine_is_idempotent() -> FsResult<()> {
        let block = ExtendedBlock::new(53, 4096, StorageType::SpdkDisk, FileType::File);
        let (store, mut handler) = spdk_handler(
            "write-handler-eager-then-cancel",
            &block,
            UncertainWriteBlockIO,
        )?;
        handler.write(&spdk_data_frame()).unwrap_err();

        let err = handler
            .complete(&spdk_complete_msg(&block), false)
            .unwrap_err();
        assert!(err.to_string().contains("uncertain"), "{err}");
        assert_eq!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Quarantined),
        );
        assert!(handler.is_commit, "must latch");
        Ok(())
    }

    // Regression: a re-entrant completion call must not force-release a quarantined block.
    #[test]
    fn complete_rejects_reentrant_call_after_uncertain_flush_quarantines_block() -> FsResult<()> {
        let block_size = 4096_i64;
        let block = ExtendedBlock::new(42, block_size, StorageType::SpdkDisk, FileType::File);

        let _worker = Worker::with_conf(ClusterConf {
            format_worker: true,
            worker: WorkerConf {
                data_dir: vec!["[MEM:1KB]../testing/write-handler-spdk-reentrancy".to_string()],
                ..WorkerConf::default()
            },
            ..ClusterConf::default()
        })?;

        let dataset = curvine_storage_local::BlockDataset::new_spdk_test_dataset(1, 1 << 30)
            .map_err(FsError::from)?;
        let store = BlockStore::from_dataset_for_test(dataset);
        let meta = store.open_block(&block).map_err(FsError::from)?;

        let file = BlockWriteContext::new(FailingFlushUncertainBlockIO, 0, block_size, 0)?;
        let context = WriteContext {
            block: block.clone(),
            req_id: 1,
            chunk_size: 1024,
            short_circuit: false,
            off: 0,
            block_size,
            dir_id: 0,
            dir_metrics: None,
        };

        let mut handler = WriteHandler {
            store: store.clone(),
            context: Some(context),
            file: Some(file),
            lease: None,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };

        let msg = Builder::new()
            .code(curvine_fs_api::RpcCode::WriteBlock)
            .request(RequestStatus::Complete)
            .req_id(1)
            .seq_id(1)
            .proto_header(curvine_proto::BlockWriteRequest {
                block: curvine_model::ProtoUtils::extend_block_to_pb(block.clone()),
                off: 0,
                block_size,
                short_circuit: false,
                client_name: "test".to_string(),
                chunk_size: 1024,
                pipeline_stream: Vec::new(),
                component_info: None,
            })
            .data(DataSlice::Empty)
            .build();

        // RPC 1: the client's normal completion attempt; flush times out,
        // uncertain -> quarantine.
        let err = handler.complete(&msg, true).unwrap_err();
        assert!(
            err.to_string().contains("flush timed out"),
            "unexpected error: {err}"
        );
        assert_eq!(
            store
                .get_raw_block_opt(meta.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Quarantined),
        );

        // RPC 2: simulates the client's cancel()-on-failure cleanup, reusing
        // the same handler instance on the same connection (commit=false).
        let cancel_msg = Builder::new()
            .code(curvine_fs_api::RpcCode::WriteBlock)
            .request(RequestStatus::Complete)
            .req_id(1)
            .seq_id(2)
            .proto_header(curvine_proto::BlockWriteRequest {
                block: curvine_model::ProtoUtils::extend_block_to_pb(block.clone()),
                off: 0,
                block_size,
                short_circuit: false,
                client_name: "test".to_string(),
                chunk_size: 1024,
                pipeline_stream: Vec::new(),
                component_info: None,
            })
            .data(DataSlice::Empty)
            .build();
        let second = handler.complete(&cancel_msg, false);
        // Must be an idempotent no-op success, not reach commit_block() again.
        assert!(
            second.is_ok(),
            "re-entrant call with empty data should be treated as an idempotent \
             no-op by the is_commit guard: {second:?}"
        );

        // Must still be quarantined: the re-entrant call must not have reached commit_block() again.
        assert_eq!(
            store
                .get_raw_block_opt(meta.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Quarantined),
            "re-entrant completion call must not force-release the quarantined extent"
        );

        Ok(())
    }

    // Regression test: commit_block() failing must still set is_commit/take the lease, forced deterministically via an out-of-band pre-finalize length mismatch.
    #[test]
    fn complete_guards_is_commit_and_lease_even_when_commit_block_itself_fails() -> FsResult<()> {
        let block_size = 1024_i64;
        let block = ExtendedBlock::new(42, block_size, StorageType::Disk, FileType::File);

        let store = create_test_store("complete-commit-block-fails")?;
        store.open_block(&block).map_err(FsError::from)?;

        // Pre-finalize with len=0 so the handler's own finalize below deterministically hits a length mismatch.
        let prefinalize_block = ExtendedBlock {
            len: 0,
            ..block.clone()
        };
        store
            .finalize_block(&prefinalize_block)
            .map_err(FsError::from)?;

        let handler_block = ExtendedBlock {
            len: block_size,
            ..block.clone()
        };

        // A flush that succeeds, to exercise the success-path commit_block() call.
        let file = BlockWriteContext::new(
            RecordingBlockIO {
                operations: Arc::new(Mutex::new(Vec::new())),
            },
            0,
            block_size,
            0,
        )?;
        let context = WriteContext {
            block: handler_block.clone(),
            req_id: 1,
            chunk_size: 1024,
            short_circuit: false,
            off: 0,
            block_size,
            dir_id: 0,
            dir_metrics: None,
        };

        let lease = store
            .ordinary_write_lease(handler_block.id)
            .map_err(FsError::from)?;

        let mut handler = WriteHandler {
            store: store.clone(),
            context: Some(context),
            file: Some(file),
            lease,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };

        let msg = Builder::new()
            .code(curvine_fs_api::RpcCode::WriteBlock)
            .request(RequestStatus::Complete)
            .req_id(1)
            .seq_id(1)
            .proto_header(curvine_proto::BlockWriteRequest {
                block: curvine_model::ProtoUtils::extend_block_to_pb(handler_block.clone()),
                off: 0,
                block_size,
                short_circuit: false,
                client_name: "test".to_string(),
                chunk_size: 1024,
                pipeline_stream: Vec::new(),
                component_info: None,
            })
            .data(DataSlice::Empty)
            .build();

        let err = handler.complete(&msg, true).unwrap_err();
        assert!(
            err.to_string().contains("length mismatch"),
            "unexpected error: {err}"
        );

        // The guard must be set even though commit_block() itself failed.
        assert!(
            handler.is_commit,
            "is_commit must be set to true even when commit_block() fails, \
             to prevent a re-entrant completion call from reaching it again"
        );
        assert!(
            handler.lease.is_none(),
            "the write lease must be released even when commit_block() fails"
        );

        // Rewrite: the mismatch rolled back to the committed generation, but
        // this write never landed, so a re-entrant Complete is refused and
        // only Cancel is an idempotent Ok.
        assert_eq!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Finalized)
        );
        assert!(handler.aborted);
        let second = handler.complete(&msg, true).unwrap_err();
        assert!(
            second
                .to_string()
                .contains("aborted and cannot be committed"),
            "{second}"
        );
        handler
            .complete(&msg, false)
            .expect("Cancel after an abort latch is Ok");

        Ok(())
    }

    /// A finalize mismatch removes a fresh block; the retried Complete must
    /// not read as "committed".
    #[test]
    fn complete_after_mismatch_removed_fresh_block_is_rejected() -> FsResult<()> {
        let block_size = 1024_i64;
        let block = ExtendedBlock::new(56, block_size, StorageType::Disk, FileType::File);
        let store = create_test_store("complete-mismatch-fresh-block")?;
        store.open_block(&block).map_err(FsError::from)?;

        // Flush succeeds but writes nothing: finalize sees 0 bytes vs len 1024.
        let mut handler = WriteHandler {
            store: store.clone(),
            context: Some(WriteContext {
                block: block.clone(),
                req_id: 1,
                chunk_size: 1024,
                short_circuit: false,
                off: 0,
                block_size,
                dir_id: 0,
                dir_metrics: None,
            }),
            file: Some(BlockWriteContext::new(
                RecordingBlockIO {
                    operations: Arc::new(Mutex::new(Vec::new())),
                },
                0,
                block_size,
                0,
            )?),
            lease: store
                .ordinary_write_lease(block.id)
                .map_err(FsError::from)?,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };
        let msg = complete_msg(&block, block_size, 1);

        handler.complete(&msg, true).unwrap_err();
        assert!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .is_none(),
            "mismatch on a fresh block must remove it"
        );
        assert!(handler.is_commit && handler.aborted);

        let second = handler.complete(&msg, true).unwrap_err();
        assert!(
            second
                .to_string()
                .contains("aborted and cannot be committed"),
            "{second}"
        );
        handler
            .complete(&msg, false)
            .expect("Cancel after an abort latch is Ok");
        Ok(())
    }

    // Transient finalize failure: not terminal, handler must stay open.
    #[test]
    fn complete_keeps_handler_open_after_transient_finalize_failure() -> FsResult<()> {
        // 0 len: RecordingBlockIO writes no real bytes, so the file stays
        // empty - avoids a real length mismatch masking the injected one.
        let block_size = 0_i64;
        let block = ExtendedBlock::new(43, block_size, StorageType::Disk, FileType::File);

        let store = create_test_store("complete-transient-finalize-failure")?;
        store.open_block(&block).map_err(FsError::from)?;

        let file = BlockWriteContext::new(
            RecordingBlockIO {
                operations: Arc::new(Mutex::new(Vec::new())),
            },
            0,
            block_size,
            0,
        )?;
        let context = WriteContext {
            block: block.clone(),
            req_id: 1,
            chunk_size: 1024,
            short_circuit: false,
            off: 0,
            block_size,
            dir_id: 0,
            dir_metrics: None,
        };

        let lease = store
            .ordinary_write_lease(block.id)
            .map_err(FsError::from)?;

        let mut handler = WriteHandler {
            store: store.clone(),
            context: Some(context),
            file: Some(file),
            lease,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };

        let msg = Builder::new()
            .code(curvine_fs_api::RpcCode::WriteBlock)
            .request(RequestStatus::Complete)
            .req_id(1)
            .seq_id(1)
            .proto_header(curvine_proto::BlockWriteRequest {
                block: curvine_model::ProtoUtils::extend_block_to_pb(block.clone()),
                off: 0,
                block_size,
                short_circuit: false,
                client_name: "test".to_string(),
                chunk_size: 1024,
                pipeline_stream: Vec::new(),
                component_info: None,
            })
            .data(DataSlice::Empty)
            .build();

        store.set_force_transient_finalize_failure_for_test(true);
        let err = handler.complete(&msg, true).unwrap_err();
        store.set_force_transient_finalize_failure_for_test(false);
        assert!(
            err.to_string()
                .contains("injected transient finalize failure"),
            "unexpected error: {err}"
        );

        // Must NOT be latched: the block rolled back to Writing, not terminal.
        assert!(
            !handler.is_commit,
            "is_commit must stay false after a transient (retryable) finalize failure"
        );
        assert!(
            handler.lease.is_some(),
            "the write lease must stay held after a transient finalize failure"
        );
        let meta = store.get_block(block.id).map_err(FsError::from)?;
        assert_eq!(
            meta.state(),
            &curvine_storage_local::BlockState::Writing,
            "block must be rolled back to Writing, not left in some other state"
        );

        // A retried Complete on the same handler must reach commit_block()
        // again (not report false success), and now succeed since the fault
        // injection is disabled.
        let retried = handler.complete(&msg, true);
        assert!(
            retried.is_ok(),
            "retried completion must actually finalize the block: {retried:?}"
        );
        assert!(
            handler.is_commit,
            "is_commit must be set once the retry actually finalizes the block"
        );
        assert!(
            handler.lease.is_none(),
            "the write lease must be released once the block is actually finalized"
        );
        let meta = store.get_block(block.id).map_err(FsError::from)?;
        assert!(
            meta.is_final(),
            "block must be finalized after the successful retry"
        );

        Ok(())
    }

    /// Same scenario, but on a rewrite of an already-finalized block: the
    /// terminal check must use the raw pending meta, not the readable
    /// (committed_rewrites) one, or a transient failure looks terminal.
    #[test]
    fn complete_keeps_handler_open_after_transient_finalize_failure_on_rewrite() -> FsResult<()> {
        let block_size = 0_i64;
        let block = ExtendedBlock::new(44, block_size, StorageType::Disk, FileType::File);

        let store = create_test_store("complete-transient-finalize-failure-rewrite")?;
        store.open_block(&block).map_err(FsError::from)?;

        // First write: finalize so the block has a committed generation.
        let first_lease = store
            .ordinary_write_lease(block.id)
            .map_err(FsError::from)?;
        let mut first_handler = WriteHandler {
            store: store.clone(),
            context: Some(WriteContext {
                block: block.clone(),
                req_id: 1,
                chunk_size: 1024,
                short_circuit: false,
                off: 0,
                block_size,
                dir_id: 0,
                dir_metrics: None,
            }),
            file: Some(BlockWriteContext::new(
                RecordingBlockIO {
                    operations: Arc::new(Mutex::new(Vec::new())),
                },
                0,
                block_size,
                0,
            )?),
            lease: first_lease,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };
        let first_msg = Builder::new()
            .code(curvine_fs_api::RpcCode::WriteBlock)
            .request(RequestStatus::Complete)
            .req_id(1)
            .seq_id(1)
            .proto_header(curvine_proto::BlockWriteRequest {
                block: curvine_model::ProtoUtils::extend_block_to_pb(block.clone()),
                off: 0,
                block_size,
                short_circuit: false,
                client_name: "test".to_string(),
                chunk_size: 1024,
                pipeline_stream: Vec::new(),
                component_info: None,
            })
            .data(DataSlice::Empty)
            .build();
        first_handler.complete(&first_msg, true)?;
        assert!(
            store.get_block(block.id).map_err(FsError::from)?.is_final(),
            "setup: first write must be finalized"
        );

        // Reopen for a rewrite: old copy moves to committed_rewrites.
        store.open_block(&block).map_err(FsError::from)?;
        assert_eq!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Writing),
            "setup: rewrite pending copy must be Writing"
        );
        assert!(
            store.get_block(block.id).map_err(FsError::from)?.is_final(),
            "setup: readable copy must still be the old Finalized generation"
        );

        let lease = store
            .ordinary_write_lease(block.id)
            .map_err(FsError::from)?;
        let mut handler = WriteHandler {
            store: store.clone(),
            context: Some(WriteContext {
                block: block.clone(),
                req_id: 2,
                chunk_size: 1024,
                short_circuit: false,
                off: 0,
                block_size,
                dir_id: 0,
                dir_metrics: None,
            }),
            file: Some(BlockWriteContext::new(
                RecordingBlockIO {
                    operations: Arc::new(Mutex::new(Vec::new())),
                },
                0,
                block_size,
                0,
            )?),
            lease,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };
        let msg = Builder::new()
            .code(curvine_fs_api::RpcCode::WriteBlock)
            .request(RequestStatus::Complete)
            .req_id(2)
            .seq_id(1)
            .proto_header(curvine_proto::BlockWriteRequest {
                block: curvine_model::ProtoUtils::extend_block_to_pb(block.clone()),
                off: 0,
                block_size,
                short_circuit: false,
                client_name: "test".to_string(),
                chunk_size: 1024,
                pipeline_stream: Vec::new(),
                component_info: None,
            })
            .data(DataSlice::Empty)
            .build();

        store.set_force_transient_finalize_failure_for_test(true);
        let err = handler.complete(&msg, true).unwrap_err();
        store.set_force_transient_finalize_failure_for_test(false);
        assert!(
            err.to_string()
                .contains("injected transient finalize failure"),
            "unexpected error: {err}"
        );

        // The fix: get_raw_block_opt() sees Writing, not the stale
        // Finalized entry the readable view resolves via committed_rewrites.
        assert!(
            !handler.is_commit,
            "is_commit must stay false after a transient finalize failure on a rewrite"
        );
        assert!(
            handler.lease.is_some(),
            "the write lease must stay held after a transient finalize failure on a rewrite"
        );
        assert_eq!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Writing),
            "pending copy must be rolled back to Writing, not left in some other state"
        );
        assert!(
            store.get_block(block.id).map_err(FsError::from)?.is_final(),
            "the readable copy stays the old Finalized generation while the rewrite is pending"
        );

        // Retry must actually finalize the rewrite now.
        let retried = handler.complete(&msg, true);
        assert!(
            retried.is_ok(),
            "retried completion must actually finalize the rewrite: {retried:?}"
        );
        assert!(
            handler.is_commit,
            "is_commit must be set once the retry actually finalizes the rewrite"
        );
        assert!(
            handler.lease.is_none(),
            "the write lease must be released once the rewrite is actually finalized"
        );
        // The rewrite must be published: pending copy finalized, no stale
        // committed_rewrites entry left for readers.
        assert_eq!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Finalized),
            "pending copy must be Finalized after the retry, not still Writing"
        );
        assert!(
            store.get_block(block.id).map_err(FsError::from)?.is_final(),
            "readers must see the finalized rewrite"
        );

        Ok(())
    }

    /// A transient abort failure must not latch is_commit; stay retryable.
    #[test]
    fn complete_keeps_handler_open_after_transient_abort_failure_on_flush_error() -> FsResult<()> {
        let block_size = 1024_i64;
        let block = ExtendedBlock::new(45, block_size, StorageType::Disk, FileType::File);

        let store = create_test_store("complete-transient-abort-failure-flush")?;
        store.open_block(&block).map_err(FsError::from)?;

        let file = BlockWriteContext::new(FailingFlushBlockIO, 0, block_size, 0)?;
        let context = WriteContext {
            block: block.clone(),
            req_id: 1,
            chunk_size: 1024,
            short_circuit: false,
            off: 0,
            block_size,
            dir_id: 0,
            dir_metrics: None,
        };
        let lease = store
            .ordinary_write_lease(block.id)
            .map_err(FsError::from)?;
        let mut handler = WriteHandler {
            store: store.clone(),
            context: Some(context),
            file: Some(file),
            lease,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };

        let msg = Builder::new()
            .code(curvine_fs_api::RpcCode::WriteBlock)
            .request(RequestStatus::Complete)
            .req_id(1)
            .seq_id(1)
            .proto_header(curvine_proto::BlockWriteRequest {
                block: curvine_model::ProtoUtils::extend_block_to_pb(block.clone()),
                off: 0,
                block_size,
                short_circuit: false,
                client_name: "test".to_string(),
                chunk_size: 1024,
                pipeline_stream: Vec::new(),
                component_info: None,
            })
            .data(DataSlice::Empty)
            .build();

        store.set_force_transient_abort_failure_for_test(true);
        let err = handler.complete(&msg, true).unwrap_err();
        store.set_force_transient_abort_failure_for_test(false);
        assert!(
            err.to_string().contains("flush failed"),
            "unexpected error: {err}"
        );

        assert!(
            !handler.is_commit,
            "is_commit must stay false after a transient abort failure"
        );
        assert!(
            handler.lease.is_some(),
            "the write lease must stay held after a transient abort failure"
        );
        assert_eq!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Writing),
            "block must remain Writing, not left in some other state"
        );

        // Cancel-on-failure cleanup, fault injection now disabled.
        let cancel_msg = Builder::new()
            .code(curvine_fs_api::RpcCode::WriteBlock)
            .request(RequestStatus::Complete)
            .req_id(1)
            .seq_id(2)
            .proto_header(curvine_proto::BlockWriteRequest {
                block: curvine_model::ProtoUtils::extend_block_to_pb(block.clone()),
                off: 0,
                block_size,
                short_circuit: false,
                client_name: "test".to_string(),
                chunk_size: 1024,
                pipeline_stream: Vec::new(),
                component_info: None,
            })
            .data(DataSlice::Empty)
            .build();
        // The abort is retried and now succeeds: a Cancel got what it asked
        // for, so the terminal replay answers Ok.
        handler
            .complete(&cancel_msg, false)
            .expect("terminal Cancel replay must report success");
        assert!(
            handler.is_commit,
            "is_commit must be set once the retry actually aborts the block"
        );
        assert!(
            handler.lease.is_none(),
            "the write lease must be released once the block is actually aborted"
        );
        assert!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .is_none(),
            "block must be fully removed after a successful abort"
        );

        Ok(())
    }

    /// Same scenario, but on the length-mismatch abort path.
    #[test]
    fn complete_keeps_handler_open_after_transient_abort_failure_on_length_mismatch() -> FsResult<()>
    {
        let block_size = 1024_i64;
        let block = ExtendedBlock::new(46, block_size, StorageType::Disk, FileType::File);
        // Oversized: triggers the length-check branch, not commit_block().
        let oversized_block = ExtendedBlock {
            len: block_size + 1,
            ..block.clone()
        };

        let store = create_test_store("complete-transient-abort-failure-length")?;
        store.open_block(&block).map_err(FsError::from)?;

        let file = BlockWriteContext::new(
            RecordingBlockIO {
                operations: Arc::new(Mutex::new(Vec::new())),
            },
            0,
            block_size,
            0,
        )?;
        let context = WriteContext {
            block: oversized_block.clone(),
            req_id: 1,
            chunk_size: 1024,
            short_circuit: false,
            off: 0,
            block_size,
            dir_id: 0,
            dir_metrics: None,
        };
        let lease = store
            .ordinary_write_lease(block.id)
            .map_err(FsError::from)?;
        let mut handler = WriteHandler {
            store: store.clone(),
            context: Some(context),
            file: Some(file),
            lease,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };

        let msg = Builder::new()
            .code(curvine_fs_api::RpcCode::WriteBlock)
            .request(RequestStatus::Complete)
            .req_id(1)
            .seq_id(1)
            .proto_header(curvine_proto::BlockWriteRequest {
                block: curvine_model::ProtoUtils::extend_block_to_pb(oversized_block.clone()),
                off: 0,
                block_size,
                short_circuit: false,
                client_name: "test".to_string(),
                chunk_size: 1024,
                pipeline_stream: Vec::new(),
                component_info: None,
            })
            .data(DataSlice::Empty)
            .build();

        store.set_force_transient_abort_failure_for_test(true);
        let err = handler.complete(&msg, true).unwrap_err();
        store.set_force_transient_abort_failure_for_test(false);
        assert!(
            err.to_string().contains("Invalid block length"),
            "unexpected error: {err}"
        );

        assert!(
            !handler.is_commit,
            "is_commit must stay false after a transient abort failure"
        );
        assert!(
            handler.lease.is_some(),
            "the write lease must stay held after a transient abort failure"
        );
        assert_eq!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Writing),
            "block must remain Writing, not left in some other state"
        );

        // Retry: abort now succeeds, same error but terminal this time.
        let retried = handler.complete(&msg, true);
        assert!(
            retried.is_err(),
            "the length-mismatch error is reported on every attempt"
        );
        assert!(
            handler.is_commit,
            "is_commit must be set once the retry actually aborts the block"
        );
        assert!(
            handler.lease.is_none(),
            "the write lease must be released once the block is actually aborted"
        );
        assert!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .is_none(),
            "block must be fully removed after a successful abort"
        );

        Ok(())
    }

    /// Complete/Cancel message for `block` with the given seq_id.
    fn complete_msg(block: &ExtendedBlock, block_size: i64, seq_id: i32) -> Message {
        Builder::new()
            .code(curvine_fs_api::RpcCode::WriteBlock)
            .request(RequestStatus::Complete)
            .req_id(1)
            .seq_id(seq_id)
            .proto_header(curvine_proto::BlockWriteRequest {
                block: curvine_model::ProtoUtils::extend_block_to_pb(block.clone()),
                off: 0,
                block_size,
                short_circuit: false,
                client_name: "test".to_string(),
                chunk_size: 1024,
                pipeline_stream: Vec::new(),
                component_info: None,
            })
            .data(DataSlice::Empty)
            .build()
    }

    /// Cancel retry after an uncertain flush whose quarantine-abort failed:
    /// the retry must re-run abort_block_uncertain (not plain abort_block
    /// via commit_block), so the extent ends Quarantined, not freed.
    #[test]
    fn cancel_retry_after_failed_uncertain_abort_quarantines_instead_of_freeing() -> FsResult<()> {
        let block_size = 4096_i64;
        let block = ExtendedBlock::new(47, block_size, StorageType::SpdkDisk, FileType::File);

        let _worker = Worker::with_conf(ClusterConf {
            format_worker: true,
            worker: WorkerConf {
                data_dir: vec!["[MEM:1KB]../testing/write-handler-pending-abort-cancel".to_string()],
                ..WorkerConf::default()
            },
            ..ClusterConf::default()
        })?;
        let dataset = curvine_storage_local::BlockDataset::new_spdk_test_dataset(1, 1 << 30)
            .map_err(FsError::from)?;
        let store = BlockStore::from_dataset_for_test(dataset);
        store.open_block(&block).map_err(FsError::from)?;
        let available_before = store.read().map_err(FsError::from)?.available();

        let mut handler = WriteHandler {
            store: store.clone(),
            context: Some(WriteContext {
                block: block.clone(),
                req_id: 1,
                chunk_size: 1024,
                short_circuit: false,
                off: 0,
                block_size,
                dir_id: 0,
                dir_metrics: None,
            }),
            file: Some(BlockWriteContext::new(
                FailingFlushUncertainBlockIO,
                0,
                block_size,
                0,
            )?),
            lease: None,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };

        // RPC 1: uncertain flush, and the quarantine-abort itself fails.
        store.set_force_transient_abort_failure_for_test(true);
        let err = handler
            .complete(&complete_msg(&block, block_size, 1), true)
            .unwrap_err();
        assert!(err.to_string().contains("flush timed out"), "{err}");
        assert!(!handler.is_commit, "must stay retryable");
        assert!(
            handler.pending_abort.is_some(),
            "must remember the pending abort"
        );
        assert_eq!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Writing),
        );

        // RPC 2: client Cancel. Must retry the uncertain abort, NOT fall
        // through to commit_block(_, false) -> plain abort_block. The
        // quarantine lands, so the Cancel is answered Ok.
        handler
            .complete(&complete_msg(&block, block_size, 2), false)
            .expect("terminal Cancel replay must report success");
        assert!(handler.is_commit, "quarantine is terminal: must latch");
        assert!(
            handler.pending_abort.is_none(),
            "pending abort must be cleared"
        );
        assert_eq!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Quarantined),
            "uncertain extent must be quarantined, not freed"
        );
        assert_eq!(
            store.read().map_err(FsError::from)?.available(),
            available_before,
            "quarantine must keep the extent reserved; available() must not grow"
        );

        Ok(())
    }

    /// Complete retry after a flush failure whose abort failed: the retry
    /// must re-run the abort, never finalize_block() on unflushed data.
    #[test]
    fn complete_retry_after_failed_abort_never_finalizes_unflushed_data() -> FsResult<()> {
        // len = 0 so the length check passes and only the flush failure
        // stands between the retry and finalize_block().
        let block_size = 0_i64;
        let block = ExtendedBlock::new(48, block_size, StorageType::Disk, FileType::File);

        let store = create_test_store("pending-abort-complete")?;
        store.open_block(&block).map_err(FsError::from)?;

        let lease = store
            .ordinary_write_lease(block.id)
            .map_err(FsError::from)?;
        let mut handler = WriteHandler {
            store: store.clone(),
            context: Some(WriteContext {
                block: block.clone(),
                req_id: 1,
                chunk_size: 1024,
                short_circuit: false,
                off: 0,
                block_size,
                dir_id: 0,
                dir_metrics: None,
            }),
            file: Some(BlockWriteContext::new(
                FailingFlushBlockIO,
                0,
                block_size,
                0,
            )?),
            lease,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };

        // RPC 1: flush fails, abort fails too.
        store.set_force_transient_abort_failure_for_test(true);
        let first = handler
            .complete(&complete_msg(&block, block_size, 1), true)
            .unwrap_err();
        assert!(first.to_string().contains("flush failed"), "{first}");
        assert!(matches!(first.kind(), ErrorKind::IO), "{:?}", first.kind());
        assert!(!handler.is_commit, "must stay retryable");
        assert!(
            handler.pending_abort.is_some(),
            "must remember the pending abort"
        );

        // RPC 2: client retries Complete(commit=true). Must retry the abort,
        // NOT reach commit_block(_, true) -> finalize_block().
        let err = handler
            .complete(&complete_msg(&block, block_size, 2), true)
            .unwrap_err();
        assert!(
            err.to_string().contains("flush failed"),
            "retry must re-report the original flush error, not succeed: {err}"
        );
        assert_eq!(
            err.kind() as i32,
            first.kind() as i32,
            "retry must keep the original error kind"
        );
        assert_eq!(
            err.to_string().matches("ERROR:").count(),
            first.to_string().matches("ERROR:").count(),
            "retry must not re-wrap the message: {err}"
        );
        assert!(handler.is_commit, "removed block is terminal: must latch");
        assert!(
            handler.lease.is_none(),
            "lease must be released once terminal"
        );
        assert!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .is_none(),
            "block must be aborted (removed), never Finalized"
        );

        Ok(())
    }

    /// Two non-terminal retries in a row: the deferred error must come back
    /// byte-identical each time, not grow a ": " per round trip.
    #[test]
    fn deferred_abort_error_message_is_stable_across_retries() -> FsResult<()> {
        let block_size = 0_i64;
        let block = ExtendedBlock::new(53, block_size, StorageType::Disk, FileType::File);

        let store = create_test_store("pending-abort-stable-message")?;
        store.open_block(&block).map_err(FsError::from)?;
        let mut handler = WriteHandler {
            store: store.clone(),
            context: Some(WriteContext {
                block: block.clone(),
                req_id: 1,
                chunk_size: 1024,
                short_circuit: false,
                off: 0,
                block_size,
                dir_id: 0,
                dir_metrics: None,
            }),
            file: Some(BlockWriteContext::new(
                FailingFlushBlockIO,
                0,
                block_size,
                0,
            )?),
            lease: store
                .ordinary_write_lease(block.id)
                .map_err(FsError::from)?,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };

        store.set_force_transient_abort_failure_for_test(true);
        let first = handler
            .complete(&complete_msg(&block, block_size, 1), false)
            .unwrap_err();
        // Stored as the wire message (source only), not Display with ": ".
        assert_eq!(
            handler.pending_abort.as_ref().unwrap().error,
            "flush failed"
        );
        store.set_force_transient_abort_failure_for_test(true);
        let second = handler
            .complete(&complete_msg(&block, block_size, 2), false)
            .unwrap_err();
        assert!(
            !handler.is_commit,
            "second abort also failed: stay retryable"
        );
        // Third abort succeeds: the Cancel is answered Ok and the handler latches.
        handler
            .complete(&complete_msg(&block, block_size, 3), false)
            .expect("terminal Cancel replay must report success");
        assert!(handler.is_commit, "third abort succeeded: latch");

        assert_eq!(second.to_string(), first.to_string());
        Ok(())
    }

    /// Disk block whose flush fails.
    fn failing_flush_handler(
        tag: &str,
        id: i64,
    ) -> FsResult<(BlockStore, ExtendedBlock, WriteHandler)> {
        let block_size = 0_i64;
        let block = ExtendedBlock::new(id, block_size, StorageType::Disk, FileType::File);
        let store = create_test_store(tag)?;
        store.open_block(&block).map_err(FsError::from)?;
        let handler = WriteHandler {
            store: store.clone(),
            context: Some(WriteContext {
                block: block.clone(),
                req_id: 1,
                chunk_size: 1024,
                short_circuit: false,
                off: 0,
                block_size,
                dir_id: 0,
                dir_metrics: None,
            }),
            file: Some(BlockWriteContext::new(
                FailingFlushBlockIO,
                0,
                block_size,
                0,
            )?),
            lease: store
                .ordinary_write_lease(block.id)
                .map_err(FsError::from)?,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };
        Ok((store, block, handler))
    }

    /// Terminal Cancel replay answers Ok.
    #[test]
    fn cancel_replay_that_reaches_terminal_returns_ok() -> FsResult<()> {
        let (store, block, mut handler) = failing_flush_handler("cancel-replay-terminal", 54)?;

        store.set_force_transient_abort_failure_for_test(true);
        handler
            .complete(&complete_msg(&block, 0, 1), false)
            .unwrap_err();
        assert!(handler.pending_abort.is_some());

        handler
            .complete(&complete_msg(&block, 0, 2), false)
            .expect("terminal Cancel replay must report success");

        assert!(handler.is_commit && handler.aborted);
        assert!(handler.lease.is_none());
        assert!(store
            .get_raw_block_opt(block.id)
            .map_err(FsError::from)?
            .is_none());
        Ok(())
    }

    /// Terminal Complete replay keeps its error; a later Complete is refused,
    /// a later Cancel is Ok.
    #[test]
    fn complete_after_abort_latch_is_rejected() -> FsResult<()> {
        let (store, block, mut handler) = failing_flush_handler("complete-after-abort-latch", 55)?;

        store.set_force_transient_abort_failure_for_test(true);
        let first = handler
            .complete(&complete_msg(&block, 0, 1), true)
            .unwrap_err();

        // Replay lands: block removed, but the caller asked to commit.
        let second = handler
            .complete(&complete_msg(&block, 0, 2), true)
            .unwrap_err();
        assert_eq!(second.to_string(), first.to_string());
        assert!(handler.is_commit && handler.aborted);

        // Third Complete must not take the is_commit Ok path.
        let third = handler
            .complete(&complete_msg(&block, 0, 3), true)
            .unwrap_err();
        assert!(
            third
                .to_string()
                .contains("aborted and cannot be committed"),
            "{third}"
        );

        // A Cancel on the same closed handler is idempotent Ok.
        handler
            .complete(&complete_msg(&block, 0, 4), false)
            .expect("Cancel after abort latch is Ok");
        Ok(())
    }

    /// A successful Cancel closes the handler without a commit: a later
    /// Complete is refused, a later Cancel is Ok.
    #[test]
    fn complete_after_successful_cancel_is_rejected() -> FsResult<()> {
        let block_size = 0_i64;
        let block = ExtendedBlock::new(58, block_size, StorageType::Disk, FileType::File);
        let store = create_test_store("complete-after-cancel")?;
        store.open_block(&block).map_err(FsError::from)?;
        let mut handler = WriteHandler {
            store: store.clone(),
            context: Some(WriteContext {
                block: block.clone(),
                req_id: 1,
                chunk_size: 1024,
                short_circuit: false,
                off: 0,
                block_size,
                dir_id: 0,
                dir_metrics: None,
            }),
            file: Some(BlockWriteContext::new(
                RecordingBlockIO {
                    operations: Arc::new(Mutex::new(Vec::new())),
                },
                0,
                block_size,
                0,
            )?),
            lease: store
                .ordinary_write_lease(block.id)
                .map_err(FsError::from)?,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };

        handler
            .complete(&complete_msg(&block, block_size, 1), false)
            .expect("Cancel is Ok");
        assert!(handler.is_commit && handler.aborted);
        assert!(store
            .get_raw_block_opt(block.id)
            .map_err(FsError::from)?
            .is_none());

        let err = handler
            .complete(&complete_msg(&block, block_size, 2), true)
            .unwrap_err();
        assert!(
            err.to_string().contains("aborted and cannot be committed"),
            "{err}"
        );
        handler
            .complete(&complete_msg(&block, block_size, 3), false)
            .expect("Cancel after Cancel is Ok");
        Ok(())
    }

    /// Rewrite flush failure restores the old generation, but this write
    /// never landed: a retried Complete is refused, Cancel is Ok.
    #[test]
    fn complete_retry_after_flush_failure_on_rewrite_is_refused() -> FsResult<()> {
        let block_size = 0_i64;
        let block = ExtendedBlock::new(57, block_size, StorageType::Disk, FileType::File);
        let store = create_test_store("rewrite-flush-failure-committed")?;
        store.open_block(&block).map_err(FsError::from)?;
        store.finalize_block(&block).map_err(FsError::from)?;

        // Reopen for rewrite; this writer's flush will fail.
        store.open_block(&block).map_err(FsError::from)?;
        let mut handler = WriteHandler {
            store: store.clone(),
            context: Some(WriteContext {
                block: block.clone(),
                req_id: 1,
                chunk_size: 1024,
                short_circuit: false,
                off: 0,
                block_size,
                dir_id: 0,
                dir_metrics: None,
            }),
            file: Some(BlockWriteContext::new(
                FailingFlushBlockIO,
                0,
                block_size,
                0,
            )?),
            lease: store
                .ordinary_write_lease(block.id)
                .map_err(FsError::from)?,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };

        let first = handler
            .complete(&complete_msg(&block, block_size, 1), true)
            .unwrap_err();
        assert!(first.to_string().contains("flush failed"), "{first}");
        assert_eq!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Finalized),
            "abort must restore the committed generation"
        );
        assert!(
            handler.is_commit,
            "Finalized is terminal: close the handler"
        );
        assert!(
            handler.aborted,
            "this write never landed: must be abort-latched"
        );

        let second = handler
            .complete(&complete_msg(&block, block_size, 2), true)
            .unwrap_err();
        assert!(
            second
                .to_string()
                .contains("aborted and cannot be committed"),
            "{second}"
        );
        handler
            .complete(&complete_msg(&block, block_size, 3), false)
            .expect("Cancel after an abort latch is Ok");
        Ok(())
    }

    /// Writer B finalizes the block between A's failed abort and A's retry;
    /// the retried abort must leave B's acked replica intact.
    #[test]
    fn complete_retry_after_failed_abort_preserves_replica_finalized_by_other_writer(
    ) -> FsResult<()> {
        let block_size = 0_i64;
        let block = ExtendedBlock::new(49, block_size, StorageType::Disk, FileType::File);

        let store = create_test_store("pending-abort-two-writers")?;
        store.open_block(&block).map_err(FsError::from)?;

        // Writer A: flush fails, abort fails too -> pending abort.
        let lease_a = store
            .ordinary_write_lease(block.id)
            .map_err(FsError::from)?;
        let mut handler_a = WriteHandler {
            store: store.clone(),
            context: Some(WriteContext {
                block: block.clone(),
                req_id: 1,
                chunk_size: 1024,
                short_circuit: false,
                off: 0,
                block_size,
                dir_id: 0,
                dir_metrics: None,
            }),
            file: Some(BlockWriteContext::new(
                FailingFlushBlockIO,
                0,
                block_size,
                0,
            )?),
            lease: lease_a,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "writer-a".to_string(),
            pending_abort: None,
            aborted: false,
        };
        store.set_force_transient_abort_failure_for_test(true);
        handler_a
            .complete(&complete_msg(&block, block_size, 1), true)
            .unwrap_err();
        assert!(handler_a.pending_abort.is_some(), "A must defer its abort");

        // Writer B: second ordinary lease is allowed; B finalizes the block.
        let lease_b = store
            .ordinary_write_lease(block.id)
            .map_err(FsError::from)?;
        assert!(lease_b.is_some(), "ordinary leases are not exclusive");
        store.open_block(&block).map_err(FsError::from)?;
        store.finalize_block(&block).map_err(FsError::from)?;
        assert!(
            store.get_block(block.id).map_err(FsError::from)?.is_final(),
            "setup: B's replica must be finalized"
        );

        // Writer A retries. The replayed abort must not touch B's replica.
        let err = handler_a
            .complete(&complete_msg(&block, block_size, 2), true)
            .unwrap_err();
        assert!(
            err.to_string().contains("flush failed"),
            "A must still get its original error: {err}"
        );
        assert_eq!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Finalized),
            "B's finalized replica must survive A's retried abort"
        );
        assert!(
            handler_a.is_commit,
            "finalized block is terminal: must latch"
        );
        assert!(handler_a.lease.is_none(), "A's lease must be released");

        Ok(())
    }

    /// Uncertain-path twin on SPDK: B reopened the same extent (BdevLayout
    /// reuses the offset) and finalized it, but A's timed-out I/O may still
    /// land there. A's replayed quarantine must win over B's Finalized.
    #[test]
    fn uncertain_retry_quarantines_shared_spdk_extent_even_after_other_writer_finalized(
    ) -> FsResult<()> {
        let block = ExtendedBlock::new(52, 4096, StorageType::SpdkDisk, FileType::File);
        let (store, mut handler_a) = spdk_handler(
            "pending-abort-two-writers-uncertain",
            &block,
            FailingFlushUncertainBlockIO,
        )?;
        handler_a.lease = store
            .ordinary_write_lease(block.id)
            .map_err(FsError::from)?;

        // Writer A: uncertain flush fails, quarantine fails too -> pending abort.
        store.set_force_transient_abort_failure_for_test(true);
        handler_a
            .complete(&spdk_complete_msg(&block), false)
            .unwrap_err();
        let pending = handler_a
            .pending_abort
            .as_ref()
            .expect("A must defer its abort");
        assert!(pending.uncertain, "deferred abort must stay uncertain");
        assert_eq!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Writing),
        );

        // Writer B: second ordinary lease is allowed; B finalizes the block.
        let lease_b = store
            .ordinary_write_lease(block.id)
            .map_err(FsError::from)?;
        assert!(lease_b.is_some(), "ordinary leases are not exclusive");
        store.open_block(&block).map_err(FsError::from)?;
        store.finalize_block(&block).map_err(FsError::from)?;
        let available = store.read().map_err(FsError::from)?.available();

        // Writer A retries. The shared extent is quarantined, not left
        // readable; the block is terminal, so A's Cancel is answered Ok.
        handler_a
            .complete(&spdk_complete_msg(&block), false)
            .expect("terminal Cancel replay must report success");
        assert_eq!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Quarantined),
            "A's in-flight I/O shares B's extent: it must be quarantined"
        );
        assert!(
            store.get_block(block.id).is_err(),
            "a quarantined extent must not be readable"
        );
        assert_eq!(
            store.read().map_err(FsError::from)?.available(),
            available,
            "quarantine must keep the extent reserved"
        );
        assert!(
            handler_a.is_commit,
            "quarantined block is terminal: must latch"
        );
        assert!(handler_a.lease.is_none(), "A's lease must be released");

        Ok(())
    }

    /// Disk control for the SPDK case above: generations do not share
    /// storage, so an uncertain abort still skips B's Finalized replica.
    #[test]
    fn uncertain_retry_on_disk_block_skips_replica_finalized_by_other_writer() -> FsResult<()> {
        let block_size = 0_i64;
        let block = ExtendedBlock::new(53, block_size, StorageType::Disk, FileType::File);
        let store = create_test_store("pending-abort-two-writers-uncertain-disk")?;
        store.open_block(&block).map_err(FsError::from)?;

        let mut handler_a = WriteHandler {
            store: store.clone(),
            context: Some(WriteContext {
                block: block.clone(),
                req_id: 1,
                chunk_size: 1024,
                short_circuit: false,
                off: 0,
                block_size,
                dir_id: 0,
                dir_metrics: None,
            }),
            file: Some(BlockWriteContext::new(
                FailingFlushUncertainBlockIO,
                0,
                block_size,
                0,
            )?),
            lease: store
                .ordinary_write_lease(block.id)
                .map_err(FsError::from)?,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "writer-a".to_string(),
            pending_abort: None,
            aborted: false,
        };
        store.set_force_transient_abort_failure_for_test(true);
        handler_a
            .complete(&complete_msg(&block, block_size, 1), true)
            .unwrap_err();
        let pending = handler_a
            .pending_abort
            .as_ref()
            .expect("A must defer its abort");
        assert!(pending.uncertain, "deferred abort must stay uncertain");

        // Writer B finalizes.
        let _lease_b = store
            .ordinary_write_lease(block.id)
            .map_err(FsError::from)?;
        store.open_block(&block).map_err(FsError::from)?;
        store.finalize_block(&block).map_err(FsError::from)?;

        let err = handler_a
            .complete(&complete_msg(&block, block_size, 2), true)
            .unwrap_err();
        assert!(err.to_string().contains("flush timed out"), "{err}");
        assert_eq!(
            store
                .get_raw_block_opt(block.id)
                .map_err(FsError::from)?
                .map(|m| *m.state()),
            Some(curvine_storage_local::BlockState::Finalized),
            "Disk fallback must still skip B's finalized replica"
        );
        assert!(handler_a.is_commit, "finalized block is terminal");
        Ok(())
    }

    /// A Complete whose header names a different block than the one opened
    /// is refused before any commit or abort runs on that block.
    #[test]
    fn complete_rejects_block_id_that_differs_from_opened_block() -> FsResult<()> {
        let block_size = 0_i64;
        let opened = ExtendedBlock::new(54, block_size, StorageType::Disk, FileType::File);
        let other = ExtendedBlock::new(55, block_size, StorageType::Disk, FileType::File);
        let store = create_test_store("complete-block-id-mismatch")?;
        store.open_block(&opened).map_err(FsError::from)?;
        store.open_block(&other).map_err(FsError::from)?;

        let mut handler = WriteHandler {
            store: store.clone(),
            context: Some(WriteContext {
                block: opened.clone(),
                req_id: 1,
                chunk_size: 1024,
                short_circuit: false,
                off: 0,
                block_size,
                dir_id: 0,
                dir_metrics: None,
            }),
            file: Some(BlockWriteContext::new(
                FailingFlushUncertainBlockIO,
                0,
                block_size,
                0,
            )?),
            lease: None,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "test".to_string(),
            pending_abort: None,
            aborted: false,
        };

        // Cancel naming block 55 with the opened writer's req_id.
        let err = handler
            .complete(&complete_msg(&other, block_size, 1), false)
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Block id mismatch, expected 54, actual 55"),
            "{err}"
        );
        for id in [opened.id, other.id] {
            assert_eq!(
                store
                    .get_raw_block_opt(id)
                    .map_err(FsError::from)?
                    .map(|m| *m.state()),
                Some(curvine_storage_local::BlockState::Writing),
                "block {id} must be untouched"
            );
        }
        assert!(handler.context.is_some(), "the opened context is kept");
        assert!(handler.file.is_some(), "the file is not flushed or dropped");
        assert!(!handler.is_commit);
        Ok(())
    }
}
