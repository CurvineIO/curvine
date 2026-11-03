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
use curvine_error::{FsError, FsResult};
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

pub struct WriteHandler {
    pub(crate) store: BlockStore,
    pub(crate) context: Option<WriteContext>,
    pub(crate) file: Option<BlockWriteContext>,
    pub(crate) lease: Option<BlockWriteLease>,
    pub(crate) is_commit: bool,
    pub(crate) io_slow_us: u64,
    pub(crate) metrics: &'static WorkerMetrics,
    pub(crate) client_addr: String,
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

    pub fn complete(&mut self, msg: &Message, commit: bool) -> FsResult<Message> {
        if self.is_commit {
            return if !msg.data.is_empty() {
                err_box!("The block has been committed and data cannot be written anymore.")
            } else {
                Ok(msg.success())
            };
        }

        let write_dir_id = self.context.as_ref().map(|c| c.dir_id);
        if let Some(context) = self.context.take() {
            Self::check_context(&context, msg)?;
        }
        let context = WriteContext::from_req(msg)?;

        let file = self.file.take();
        if let Some(mut file) = file {
            if let Err(flush_err) = file.flush() {
                drop(file);
                if let Some(dir_id) = write_dir_id {
                    Self::record_write_error(self.metrics, dir_id, &flush_err);
                }
                if let Err(abort_err) = self.store.abort_block(&context.block) {
                    log::warn!(
                        "failed to abort block {} after flush error: {}",
                        context.block.id,
                        abort_err
                    );
                }
                return Err(flush_err.into());
            }
            drop(file);
        }

        if context.block.len > context.block_size {
            if let Err(abort_err) = self.store.abort_block(&context.block) {
                log::warn!(
                    "failed to abort block {} after length check failed: {}",
                    context.block.id,
                    abort_err
                );
            }
            return err_box!(
                "Invalid block length: {}, block size: {}",
                context.block.len,
                context.block_size
            );
        }

        self.commit_block(&context.block, commit)?;
        self.is_commit = true;
        self.lease.take();

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
    use crate::worker::storage::BlockWriteContext;
    use crate::worker::Worker;
    use curvine_config::{ClusterConf, WorkerConf};
    use curvine_error::{FsError, FsResult};
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
}
