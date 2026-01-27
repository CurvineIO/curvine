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
use crate::worker::handler::pipeline_writer::{ClientPipelineForwarder, PipelineStream};
use crate::worker::handler::WriteContext;
use crate::worker::handler::WriteHandler;
use crate::worker::WorkerMetrics;
use curvine_client::file::FsContext;
use curvine_common::error::FsError;
use curvine_common::proto::{BlockWriteResponse, DataHeaderProto, PipelineStatus};
use curvine_common::state::{ExtendedBlock, WorkerAddress};
use curvine_common::FsResult;
use log::{debug, info, warn};
use orpc::common::{ByteUnit, TimeSpent};
use orpc::io::LocalFile;
use orpc::message::{Builder, Message, RequestStatus};
use orpc::{err_box, ternary, try_option_mut};
use std::collections::HashSet;
use std::mem;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

pub struct PipelineWriteHandler {
    store: BlockStore,
    context: Option<WriteContext>,
    file: Option<LocalFile>,
    is_commit: bool,
    io_slow_us: u64,
    pipeline_write_delay_ms: u64,
    metrics: &'static WorkerMetrics,
    pipeline_stream: Option<PipelineStream>,
    forwarder: ClientPipelineForwarder,
    last_processed_seq: i32,
    processed_seq_ids: HashSet<(i64, i32)>,
    had_pipeline_stream: bool,
}

impl PipelineWriteHandler {
    pub fn new(
        store: BlockStore,
        fs_context: Arc<FsContext>,
        metrics: &'static WorkerMetrics,
        queue_size: usize,
        pipeline_write_delay_ms: u64,
    ) -> Self {
        Self {
            store,
            context: None,
            file: None,
            is_commit: false,
            io_slow_us: 0,
            pipeline_write_delay_ms,
            metrics,
            pipeline_stream: None,
            forwarder: ClientPipelineForwarder::new(fs_context, queue_size),
            last_processed_seq: -1,
            processed_seq_ids: HashSet::new(),
            had_pipeline_stream: false,
        }
    }

    pub fn set_io_slow_us(&mut self, io_slow_us: u64) {
        self.io_slow_us = io_slow_us;
    }

    pub fn is_sync(&self, msg: &Message) -> bool {
        if msg.request_status() == RequestStatus::Open {
            match WriteContext::from_req(msg) {
                Ok(ctx) => {
                    let has_pipeline_stream = ctx.has_pipeline_stream();
                    let result = !has_pipeline_stream;
                    if has_pipeline_stream && result {
                        warn!(
                            "PipelineWriteHandler::is_sync: has_pipeline_stream=true but sync=true! block_id={}",
                            ctx.block.id
                        );
                    }
                    return result;
                }
                Err(e) => {
                    warn!(
                        "PipelineWriteHandler::is_sync: failed to parse WriteContext: {}",
                        e
                    );
                }
            }
        }
        !self.had_pipeline_stream
    }

    pub async fn async_handle(&mut self, msg: Message) -> FsResult<Message> {
        let request_status = msg.request_status();
        match request_status {
            RequestStatus::Open => self.async_open(&msg).await,
            RequestStatus::Running => self.async_write(msg).await,
            RequestStatus::Complete => self.async_complete(&msg, true).await,
            RequestStatus::Cancel => self.async_complete(&msg, false).await,
            _ => err_box!("Unsupported request type"),
        }
    }

    pub async fn async_open(&mut self, msg: &Message) -> FsResult<Message> {
        let context = WriteContext::from_req(msg)?;

        debug!(
            "PipelineWriteHandler::async_open: block_id={}, pipeline_stream_count={}",
            context.block.id,
            context.pipeline_stream.len()
        );

        if context.off > context.block_size {
            return err_box!(
                "Invalid write offset: {}, block size: {}",
                context.off,
                context.block_size
            );
        }

        let open_block = ExtendedBlock {
            len: context.block_size,
            ..context.block.clone()
        };

        let meta = self.store.open_block(&open_block)?;
        let mut file = meta.create_writer(context.off, false)?;
        WriteHandler::resize(&mut file, &context)?;

        let (label, path, file) = if context.short_circuit {
            ("local", file.path().to_string(), None)
        } else {
            ("remote", file.path().to_string(), Some(file))
        };

        let mut pipeline_status = PipelineStatus {
            success: true,
            established_count: 1,
            failed_worker: None,
            error_message: None,
        };

        if context.has_pipeline_stream() {
            self.metrics.pipeline_establish_total.inc();
            match self.establish_pipeline_stream(&context).await {
                Ok(pipeline_stream) => {
                    pipeline_status.established_count += pipeline_stream.established_count();
                    self.pipeline_stream = Some(pipeline_stream);
                    self.had_pipeline_stream = true;
                    info!(
                        "PipelineWriteHandler::async_open: Pipeline established, block_id={}, workers={}",
                        context.block.id, pipeline_status.established_count
                    );
                }
                Err(e) => {
                    pipeline_status.success = false;
                    self.metrics.pipeline_establish_failed.inc();
                    pipeline_status.failed_worker = context.pipeline_stream.first().map(|w| {
                        curvine_common::proto::WorkerAddressProto {
                            worker_id: w.worker_id,
                            hostname: w.hostname.clone(),
                            ip_addr: w.ip_addr.clone(),
                            rpc_port: w.rpc_port,
                            web_port: w.web_port,
                        }
                    });
                    pipeline_status.error_message = Some(e.to_string());
                    warn!(
                        "PipelineWriteHandler::async_open: Pipeline establishment failed, block_id={}, error={}",
                        context.block.id,
                        e
                    );
                }
            }
        }

        let log_msg = format!(
            "Write {}-block start req_id: {}, path: {:?}, chunk_size: {}, off: {}, block_size: {}, pipeline_established: {}",
            label,
            context.req_id,
            path,
            context.chunk_size,
            context.off,
            ByteUnit::byte_to_string(context.block_size as u64),
            pipeline_status.established_count
        );

        let response = BlockWriteResponse {
            id: meta.id,
            path: ternary!(context.short_circuit, Some(path), None),
            off: context.off,
            block_size: context.block_size,
            storage_type: meta.storage_type().into(),
            pipeline_status: Some(pipeline_status),
        };

        let _ = mem::replace(&mut self.file, file);
        let _ = self.context.replace(context);

        self.metrics.write_blocks.with_label_values(&[label]).inc();

        info!("{}", log_msg);
        Ok(Builder::success(msg).proto_header(response).build())
    }

    pub async fn async_write(&mut self, mut msg: Message) -> FsResult<Message> {
        let seq_id = msg.seq_id();
        let req_id = msg.req_id();
        if seq_id >= 0
            && (self.is_duplicate_request(seq_id)
                || self.processed_seq_ids.contains(&(req_id, seq_id)))
        {
            return Ok(msg.success());
        }

        let context = try_option_mut!(self.context);
        Self::check_context(context, &msg)?;

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
                if let Some(file) = &mut self.file {
                    file.seek(header.offset)?;
                }
            }
        }

        let data_len = msg.data_len() as i64;
        if data_len > 0 {
            if self.pipeline_write_delay_ms > 0 && self.pipeline_stream.is_some() {
                sleep(Duration::from_millis(self.pipeline_write_delay_ms)).await;
            }
            let block_size = context.block_size;
            let file_pos = self.file.as_ref().map(|f| f.pos()).unwrap_or(0);

            if file_pos + data_len > block_size {
                return err_box!(
                    "Write range [{}, {}) exceeds block size {}",
                    file_pos,
                    file_pos + data_len,
                    block_size
                );
            }

            let data = std::mem::replace(&mut msg.data, orpc::sys::DataSlice::Empty).freeze();
            let io_slow_us = self.io_slow_us;

            let local_write_future = {
                if let Some(file) = self.file.take() {
                    let data_clone = data.clone();
                    Some(tokio::task::spawn_blocking(move || {
                        let mut file = file;
                        let spend = TimeSpent::new();
                        let result = file.write_region(&data_clone);
                        let used = spend.used_us();
                        let path = file.path().to_string();
                        (file, result, used, path)
                    }))
                } else {
                    None
                }
            };

            let forward_future = async {
                if let Some(ref mut pipeline_stream) = self.pipeline_stream {
                    pipeline_stream.write(data.clone()).await
                } else {
                    Ok(())
                }
            };

            let (local_result, forward_result) = if let Some(local_future) = local_write_future {
                let (local_res, forward_res) = tokio::join!(local_future, forward_future);
                (Some(local_res), forward_res)
            } else {
                (None, forward_future.await)
            };

            if let Some(local_res) = local_result {
                let (file, write_result, used, path) = local_res
                    .map_err(|e| FsError::from(format!("spawn_blocking failed: {}", e)))?;
                self.file = Some(file);
                write_result?;

                if used >= io_slow_us {
                    warn!(
                        "Slow write data from disk cost: {}us (threshold={}us), path: {}",
                        used, io_slow_us, path
                    );
                }
                self.metrics.write_time_us.inc_by(used as i64);
            }

            forward_result?;

            self.metrics.write_bytes.inc_by(data_len);
            self.metrics.write_count.inc();
        }

        self.update_last_seq(seq_id);
        if seq_id >= 0 {
            self.processed_seq_ids.insert((req_id, seq_id));
        }
        Ok(msg.success())
    }

    pub async fn async_complete(&mut self, msg: &Message, commit: bool) -> FsResult<Message> {
        if self.is_commit {
            return if !msg.data.is_empty() {
                err_box!("The block has been committed and data cannot be written anymore.")
            } else {
                Ok(msg.success())
            };
        }

        if let Some(context) = self.context.take() {
            Self::check_context(&context, msg)?;
        }
        let context = WriteContext::from_req(msg)?;

        let file = self.file.take();
        if let Some(mut file) = file {
            file.flush()?;
            drop(file);
        }

        if context.block.len > context.block_size {
            return err_box!(
                "Invalid write offset: {}, block size: {}",
                context.block.len,
                context.block_size
            );
        }

        if let Some(mut pipeline_stream) = self.pipeline_stream.take() {
            pipeline_stream.complete(commit).await?;
        }

        self.commit_block(&context.block, commit)?;
        self.is_commit = true;
        self.processed_seq_ids.clear();

        info!(
            "write block end for req_id {}, is commit: {}, off: {}, len: {}",
            msg.req_id(),
            commit,
            context.off,
            context.block.len
        );

        Ok(msg.success())
    }

    fn is_duplicate_request(&self, seq_id: i32) -> bool {
        seq_id >= 0 && seq_id <= self.last_processed_seq
    }

    fn update_last_seq(&mut self, seq_id: i32) {
        if seq_id > self.last_processed_seq {
            self.last_processed_seq = seq_id;
        }
    }

    fn commit_block(&self, block: &ExtendedBlock, commit: bool) -> FsResult<()> {
        if commit {
            self.store.finalize_block(block)?;
        } else {
            self.store.abort_block(block)?;
        }
        Ok(())
    }

    fn check_context(context: &WriteContext, msg: &Message) -> FsResult<()> {
        if msg.req_id() != context.req_id {
            return err_box!(
                "Invalid request id: current req_id {}, expected {}",
                msg.req_id(),
                context.req_id
            );
        }
        Ok(())
    }

    async fn establish_pipeline_stream(&self, ctx: &WriteContext) -> FsResult<PipelineStream> {
        let next_worker = ctx
            .pipeline_stream
            .first()
            .ok_or_else(|| FsError::common("No pipeline stream worker specified"))?;
        let remaining: Vec<WorkerAddress> = ctx.pipeline_stream[1..].to_vec();

        info!(
            "PipelineWriteHandler::establish_pipeline_stream: block_id={}, next_worker={}, remaining_count={}, remaining_workers={:?}",
            ctx.block.id,
            next_worker.worker_id,
            remaining.len(),
            remaining.iter().map(|w| w.worker_id).collect::<Vec<_>>()
        );

        let pipeline_stream = self.forwarder.connect(ctx).await?;

        info!(
            "PipelineWriteHandler::establish_pipeline_stream: SUCCESS block_id={}, next_worker={}",
            ctx.block.id, next_worker.worker_id
        );

        Ok(pipeline_stream)
    }

    pub fn handle(&mut self, _msg: &Message) -> FsResult<Message> {
        err_box!("PipelineWriteHandler only supports async_handle")
    }
}
