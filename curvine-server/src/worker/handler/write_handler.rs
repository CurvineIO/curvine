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

use crate::worker::block::BlockStore;
use crate::worker::handler::WriteContext;
use crate::worker::storage::BlockWriteContext;
use crate::worker::{Worker, WorkerMetrics};
use curvine_common::error::FsError;
use curvine_common::proto::{BlockWriteResponse, DataHeaderProto};
use curvine_common::state::{ExtendedBlock, FileAllocMode};
use curvine_common::FsResult;
use log::{info, warn};
use orpc::common::{ByteUnit, TimeSpent};
use orpc::handler::MessageHandler;
use orpc::message::{Builder, Message, RequestStatus};
use orpc::{err_box, ternary, try_option_mut, CommonResult};
use std::mem;

pub struct WriteHandler {
    pub(crate) store: BlockStore,
    pub(crate) context: Option<WriteContext>,
    pub(crate) file: Option<BlockWriteContext>,
    pub(crate) is_commit: bool,
    pub(crate) io_slow_us: u64,
    pub(crate) metrics: &'static WorkerMetrics,
}

impl WriteHandler {
    pub fn new(store: BlockStore) -> CommonResult<Self> {
        let conf = Worker::get_conf()?;
        let metrics = Worker::get_metrics()?;
        Ok(Self {
            store,
            context: None,
            file: None,
            is_commit: false,
            io_slow_us: conf.worker.io_slow_us(),
            metrics,
        })
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
        let context = WriteContext::from_req(msg)?;
        if context.off < 0 || context.off > context.block_size {
            return err_box!(
                "Invalid write offset: {}, block size: {}",
                context.off,
                context.block_size
            );
        }
        self.is_commit = false;

        let open_block = ExtendedBlock {
            len: context.block_size,
            ..context.block.clone()
        };

        let meta = self.store.open_block(&open_block)?;
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
            "Write {}-block start req_id: {}, path: {:?}, chunk_size: {}, off: {}, block_size: {}",
            label,
            context.req_id,
            path,
            context.chunk_size,
            context.off,
            ByteUnit::byte_to_string(context.block_size as u64)
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
        let _ = self.context.replace(context);

        self.metrics.write_blocks.with_label_values(&[label]).inc();

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

    pub fn write(&mut self, msg: &Message) -> FsResult<Message> {
        let file = try_option_mut!(self.file);
        let context = try_option_mut!(self.context);
        Self::check_context(context, msg)?;

        if msg.header_len() > 0 {
            let header: DataHeaderProto = msg.parse_header()?;
            if !header.flush {
                if header.offset < 0 || header.offset >= context.block_size {
                    return err_box!(
                        "Invalid seek offset: {}, block length: {}",
                        header.offset,
                        context.block_size
                    );
                }
                file.seek_to(header.offset)?;
            }
        }

        let data_len = msg.data_len() as i64;
        if data_len > 0 {
            let spend = TimeSpent::new();
            file.write_region(&msg.data)?;

            let used = spend.used_us();
            if used >= self.io_slow_us {
                warn!(
                    "Slow write data from disk cost: {}us (threshold={}us), path: {} ",
                    used,
                    self.io_slow_us,
                    file.path()
                );
            }
            self.metrics.write_bytes.inc_by(msg.data_len() as i64);
            self.metrics.write_time_us.inc_by(used as i64);
            self.metrics.write_count.inc();
        }

        Ok(msg.success())
    }

    pub(crate) fn complete_block(
        store: &BlockStore,
        file: Option<BlockWriteContext>,
        opened: Option<&WriteContext>,
        requested: &WriteContext,
        commit: bool,
    ) -> FsResult<()> {
        let opened_block = opened.map(|context| &context.block);
        let mut file = file;
        let result = (|| {
            if let Some(opened) = opened {
                if opened.req_id != requested.req_id {
                    return err_box!(
                        "Request id mismatch, expected {}, actual {}",
                        opened.req_id,
                        requested.req_id
                    );
                }
                if opened.block.id != requested.block.id
                    || opened.block.storage_type != requested.block.storage_type
                    || opened.block.file_type != requested.block.file_type
                {
                    return err_box!(
                        "write block mismatch: opened={:?}, completed={:?}",
                        opened.block,
                        requested.block
                    );
                }
            }

            if requested.block.len > requested.block_size {
                return err_box!(
                    "Invalid block length: {}, block size: {}",
                    requested.block.len,
                    requested.block_size
                );
            }

            if !commit {
                drop(file.take());
                store.abort_block(opened_block.unwrap_or(&requested.block))?;
                return Ok(());
            }

            if let Some(file) = file.as_mut() {
                file.flush()?;
            }
            drop(file.take());
            store.finalize_block(&requested.block)?;
            Ok(())
        })();

        if result.is_err() {
            drop(file.take());
            if let Some(opened_block) = opened_block {
                if let Err(abort_error) = store.abort_block(opened_block) {
                    warn!(
                        "failed to abort block {} after completion error: {}",
                        opened_block.id, abort_error
                    );
                }
            }
        }
        result
    }

    pub fn complete(&mut self, msg: &Message, commit: bool) -> FsResult<Message> {
        if self.is_commit {
            return if !msg.data.is_empty() {
                err_box!("The block has been committed and data cannot be written anymore.")
            } else {
                Ok(msg.success())
            };
        }

        let requested = match WriteContext::from_req(msg) {
            Ok(requested) => requested,
            Err(error) => {
                drop(self.file.take());
                if let Some(opened) = self.context.take() {
                    if let Err(abort_error) = self.store.abort_block(&opened.block) {
                        warn!(
                            "failed to abort block {} after invalid completion request: {}",
                            opened.block.id, abort_error
                        );
                    }
                }
                return Err(error);
            }
        };
        let opened = self.context.take();
        let file = self.file.take();
        Self::complete_block(&self.store, file, opened.as_ref(), &requested, commit)?;
        self.is_commit = true;

        info!(
            "write block end for req_id {}, is commit: {}, off: {}, len: {}",
            msg.req_id(),
            commit,
            requested.off,
            requested.block.len
        );

        Ok(msg.success())
    }
}

impl MessageHandler for WriteHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
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
