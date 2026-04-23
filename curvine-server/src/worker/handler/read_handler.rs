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
use crate::worker::handler::ReadContext;
use crate::worker::{Worker, WorkerMetrics};
use curvine_common::error::FsError;
use curvine_common::proto::{BlockReadResponse, DataHeaderProto};
use curvine_common::state::StorageType;
use curvine_common::FsResult;
use log::{info, warn};
use orpc::common::{ByteUnit, TimeSpent};
use orpc::handler::MessageHandler;
use orpc::io::{BlockDevice, BlockIO};
use orpc::message::{Builder, Message, RequestStatus};
use orpc::sys::{CacheManager, ReadAheadTask};
use orpc::{err_box, ternary, try_option_mut};
use std::mem;

pub struct ReadHandler {
    pub(crate) store: BlockStore,
    pub(crate) os_cache: CacheManager,
    pub(crate) context: Option<ReadContext>,
    pub(crate) file: Option<BlockDevice>,
    pub(crate) last_task: Option<ReadAheadTask>,
    pub(crate) io_slow_us: u64,
    pub(crate) enable_send_file: bool,
    pub(crate) metrics: &'static WorkerMetrics,
}

impl ReadHandler {
    pub const MAX_READ_AHEAD: i64 = 16 * 1024 * 1024;

    pub fn new(store: BlockStore) -> Self {
        let metrics = Worker::get_metrics();
        let conf = Worker::get_conf();
        Self {
            store,
            os_cache: CacheManager::with_place(),
            context: None,
            file: None,
            last_task: None,
            io_slow_us: conf.worker.io_slow_us(),
            enable_send_file: conf.worker.enable_send_file,
            metrics,
        }
    }

    pub fn open(&mut self, msg: &Message) -> FsResult<Message> {
        let mut context = ReadContext::from_req(msg)?;
        let meta = self.store.get_block(context.block_id)?;

        if context.off > meta.len {
            return err_box!(
                "The length of the requested data exceeds the maximum length of the block file, \
            request off {}, file len {}",
                context.off,
                meta.len
            );
        }

        if context.chunk_size <= 0 {
            return err_box!("chunk_size must be greater than 0");
        }

        if context.enable_read_ahead && context.read_ahead_len > Self::MAX_READ_AHEAD {
            return err_box!(
                "The pre-read size exceeds the maximum value allowed by the system.\
                 The current value is {}. The maximum allowed value is: {}",
                context.read_ahead_len,
                Self::MAX_READ_AHEAD
            );
        }

        // Check short-circuit before open. SPDK has no filesystem path.
        let is_short_circuit =
            context.short_circuit && meta.storage_type() != StorageType::SpdkDisk;
        let (label, path, file) = if is_short_circuit {
            let path = meta.get_block_file()?;
            ("local", path, None)
        } else {
            let file = meta.create_reader(context.off as u64)?;
            ("remote", file.path().to_string(), Some(file))
        };

        self.os_cache = CacheManager::new(
            context.enable_read_ahead,
            context.read_ahead_len,
            context.drop_cache_len,
            context.chunk_size as i64,
        );

        let log_msg = format!(
            "Read {}-block start req_id: {}, path: {:?}, chunk_size: {}, read len: {}, read_ahead: {}-{}",
            label,
            context.req_id,
            path,
            context.chunk_size,
            ByteUnit::byte_to_string(context.len as u64),
            self.os_cache.enable,
            self.os_cache.read_ahead_len
        );

        let response = BlockReadResponse {
            id: context.block_id,
            len: meta.len,
            path: ternary!(is_short_circuit, Some(path), None),
            storage_type: meta.storage_type().into(),
        };

        let _ = mem::replace(&mut self.file, file);
        context.bdev_offset = meta.bdev_offset;
        let _ = self.context.replace(context);

        self.metrics.read_blocks.with_label_values(&[label]).inc();
        info!("{}", log_msg);

        Ok(Builder::success(msg).proto_header(response).build())
    }

    fn check_context(context: &ReadContext, msg: &Message) -> FsResult<()> {
        if context.req_id != msg.req_id() {
            return err_box!(
                "Request id mismatch, expected {}, actual {}",
                context.req_id,
                msg.req_id()
            );
        }
        Ok(())
    }

    pub fn read(&mut self, msg: &Message) -> FsResult<Message> {
        let file = try_option_mut!(self.file);
        let context = try_option_mut!(self.context);

        if msg.header_len() > 0 {
            let header: DataHeaderProto = msg.parse_header()?;
            let abs_offset = if file.supports_short_circuit() {
                header.offset
            } else {
                context.bdev_offset + context.off + header.offset
            };
            if abs_offset != file.pos() {
                file.seek(abs_offset)?;
            }
        }

        let spend = TimeSpent::new();
        // OS page cache read-ahead is only available for local files
        if file.supports_read_ahead() {
            if let Some(local) = file.as_local_mut() {
                self.last_task = local.read_ahead(&self.os_cache, self.last_task.take());
            }
        }
        // SPDK bypasses kernel — sendfile unavailable
        let enable_send_file = self.enable_send_file && file.supports_send_file();
        let region = file.read_region(enable_send_file, context.chunk_size)?;
        // Post-read read-ahead for next chunk
        if file.supports_read_ahead() {
            if let Some(local) = file.as_local_mut() {
                self.last_task = local.read_ahead(&self.os_cache, self.last_task.take());
            }
        }
        let used = spend.used_us();

        if used >= self.io_slow_us {
            warn!(
                "Slow read data from disk cost: {}us (threshold={}us), path: {} ",
                used,
                self.io_slow_us,
                file.path()
            );
        }
        self.metrics.read_bytes.inc_by(region.len() as i64);
        self.metrics.read_time_us.inc_by(used as i64);
        self.metrics.read_count.inc();

        Ok(msg.success_with_data(None, region))
    }

    // Reading is completed and the file is closed.
    pub fn complete(&mut self, msg: &Message) -> FsResult<Message> {
        let _block_id = match &self.context {
            Some(v) => {
                //Remote reading
                Self::check_context(v, msg)?;
                v.block_id
            }

            None => {
                // Local short circuit reading, block information is in the header.
                // let c = ReadContext::from_req(msg)?;
                // c.block_id
                -1
            }
        };

        let file = self.file.take();
        drop(file);

        info!("Read block end for req_id {}", msg.req_id());
        Ok(msg.success())
    }
}

impl MessageHandler for ReadHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let request_status = msg.request_status();

        match request_status {
            RequestStatus::Open => self.open(msg),

            RequestStatus::Running => self.read(msg),

            RequestStatus::Complete => self.complete(msg),

            _ => err_box!("Unsupported request type"),
        }
    }
}
