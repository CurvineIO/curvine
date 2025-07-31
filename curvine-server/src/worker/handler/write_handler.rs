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
use crate::worker::{Worker, WorkerMetrics};
use curvine_common::error::FsError;
use curvine_common::proto::{BlockWriteResponse, DataHeaderProto};
use curvine_common::state::ExtendedBlock;
use curvine_common::FsResult;
use log::{info, warn};
use orpc::common::{ByteUnit, TimeSpent};
use orpc::handler::MessageHandler;
use orpc::io::LocalFile;
use orpc::message::{Builder, Message, RequestStatus};
use orpc::{err_box, ternary, try_option_mut};
use std::mem;

pub struct WriteHandler {
    pub(crate) store: BlockStore,
    pub(crate) context: Option<WriteContext>,
    pub(crate) file: Option<LocalFile>,
    pub(crate) is_commit: bool,
    pub(crate) io_slow_us: u64,
    pub(crate) metrics: &'static WorkerMetrics,
}

impl WriteHandler {
    pub fn new(store: BlockStore) -> Self {
        let conf = Worker::get_conf();
        let metrics = Worker::get_metrics();

        Self {
            store,
            context: None,
            file: None,
            is_commit: false,
            io_slow_us: conf.worker.io_slow_us(),
            metrics,
        }
    }

    pub fn open(&mut self, msg: &Message) -> FsResult<Message> {
        let context = WriteContext::from_req(msg)?;

        // When creating a block, there are 2 cases:
        // 1. In non-append mode, a new block file will be created.block's len = block_size
        // 2. Append mode, append data to the block.block len = block file size.
        let meta = if context.is_append {
            self.store.append_block(context.off, &context.block)?
        } else {
            self.store.create_block(&context.block)?
        };

        if context.off >= meta.len {
            return err_box!("The write start position exceeds the block size");
        }

        let file = meta.create_writer(context.is_append)?;
        if file.len() != context.off {
            return err_box!(
                "Append write initial length error, expected {}, actual {}",
                context.off,
                file.len()
            );
        }

        let (label, path, file) = if context.short_circuit {
            ("local", file.path().to_string(), None)
        } else {
            ("remote", file.path().to_string(), Some(file))
        };

        let log_msg = format!(
            "Write {}-block start req_id: {}, path: {:?}, off: {}, chunk_size: {}, block_size: {}",
            label,
            context.req_id,
            path,
            context.off,
            context.chunk_size,
            ByteUnit::byte_to_string(context.block.len as u64)
        );

        let response = BlockWriteResponse {
            id: meta.id,
            path: ternary!(context.short_circuit, Some(path), None),
            off: context.off,
            len: context.len,
            storage_type: meta.storage_type().into(),
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

        // Write existing data blocks.
        let data_len = msg.data_len() as i64;
        if data_len > 0 {
            if file.pos() + data_len > context.len {
                return err_box!("Writing data exceeds block_size");
            }

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
                self.metrics.write_slow_count.inc();
            }
            self.metrics.write_bytes.inc_by(msg.data_len() as i64);
            self.metrics.write_time_us.inc_by(used as i64);
        }

        // parse the header header and whether flush data is required.
        if msg.header_len() > 0 {
            let header: DataHeaderProto = msg.parse_header()?;
            if header.flush {
                file.flush()?;
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

        let block = match self.context.take() {
            Some(v) => {
                // Remote writing
                Self::check_context(&v, msg)?;
                v.block
            }

            None => {
                // Local short-circuit write, block information is in the header.
                let c = WriteContext::from_req(msg)?;
                c.block
            }
        };

        // flush and close the file.
        let file = self.file.take();
        if let Some(mut file) = file {
            file.flush()?;
            drop(file);
        }

        // Submit block.
        self.commit_block(&block, commit || block.is_stream())?;
        self.is_commit = true;

        info!(
            "write block end for req_id {}, is commit: {}",
            msg.req_id(),
            commit
        );

        Ok(msg.success())
    }
}

// If the write process suddenly interrupts, we will not delete the unwritten block.
/*impl Drop for WriteHandler {
    fn drop(&mut self) {
        if !self.is_commit {
            if let Err(e) = self.commit_block(false) {
                error!("Abort block {}", e);
            }
            self.is_commit = true;
        }
    }
}
*/

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
