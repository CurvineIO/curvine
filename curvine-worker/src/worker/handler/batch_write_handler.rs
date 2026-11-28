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
use crate::worker::handler::WriteContext;
use crate::worker::handler::WriteHandler;
use crate::worker::storage::BlockWriteContext;
use crate::worker::WorkerMetrics;
use curvine_core_error::err_box;
use curvine_core_error::CommonResult;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::RpcCode;
use curvine_io::DataSlice;
use curvine_model::{ExtendedBlock, ProtoUtils};
use curvine_proto::{
    BlockWriteRequest, BlockWriteResponse, BlocksBatchCommitRequest, BlocksBatchCommitResponse,
    BlocksBatchWriteRequest, BlocksBatchWriteResponse, DataHeaderProto, ExtendedBlockProto,
    FilesBatchWriteRequest, FilesBatchWriteResponse,
};
use curvine_rpc::message::{Builder, Message, RequestStatus};
use std::collections::HashSet;

/// Authority to retry only a member whose explicit Cancel has not succeeded.
enum CancelMember {
    Owned(BlockWriteContext),
    Legacy {
        uncertain: bool,
        _lease: Option<BlockWriteLease>,
    },
}

pub struct BatchWriteHandler {
    pub(crate) context: Option<Vec<WriteContext>>,
    pub(crate) file: Option<Vec<Option<BlockWriteContext>>>,
    leases: Vec<Option<BlockWriteLease>>,
    pub(crate) is_commit: bool,
    /// This batch closed without publishing every block; refuse Complete.
    rolled_back: bool,
    /// During mixed Cancel, retain the original contexts for request identity
    /// and only the members still needing cancellation. None is a tombstone.
    pending_cancel: Vec<Option<CancelMember>>,
    pub(crate) write_handler: WriteHandler,
}

impl Drop for BatchWriteHandler {
    fn drop(&mut self) {
        self.file.take();
        self.write_handler.file.take();
        self.leases.clear();
    }
}

impl BatchWriteHandler {
    pub fn new(store: BlockStore, client_addr: String) -> CommonResult<Self> {
        Ok(Self {
            context: None,
            file: None,
            leases: Vec::new(),
            is_commit: false,
            rolled_back: false,
            pending_cancel: Vec::new(),
            write_handler: WriteHandler::new(store, client_addr)?,
        })
    }

    fn check_context(context: &WriteContext, expected_req_id: i64) -> FsResult<()> {
        if context.req_id != expected_req_id {
            return err_box!(
                "Request id mismatch, expected {}, actual {}",
                context.req_id,
                expected_req_id
            );
        }
        Ok(())
    }

    fn member_req_id(req_id: i64, index: usize) -> FsResult<i64> {
        req_id
            .checked_add(index as i64)
            .ok_or_else(|| FsError::common("batch request id overflow"))
    }

    fn is_owned_file(file: &Option<BlockWriteContext>) -> bool {
        file.as_ref().is_some_and(BlockWriteContext::is_owned)
    }

    fn has_owned_files(&self) -> bool {
        self.file
            .as_ref()
            .is_some_and(|files| files.iter().any(Self::is_owned_file))
    }

    fn is_uncertain(file: &BlockWriteContext) -> bool {
        !file.is_owned() && file.has_uncertain_writes()
    }

    fn uncertain_blocks(
        contexts: &[WriteContext],
        files: &[Option<BlockWriteContext>],
    ) -> HashSet<i64> {
        contexts
            .iter()
            .zip(files)
            .filter(|(_, file)| file.as_ref().is_some_and(Self::is_uncertain))
            .map(|(context, _)| context.block.id)
            .collect()
    }

    fn abort_legacy(
        store: &BlockStore,
        block: &ExtendedBlock,
        uncertain: bool,
    ) -> CommonResult<()> {
        if uncertain {
            store.abort_block_uncertain(block)
        } else {
            store.abort_block(block)
        }
    }

    fn take_legacy_file(file: &mut Option<BlockWriteContext>) -> Option<BlockWriteContext> {
        if Self::is_owned_file(file) {
            None
        } else {
            file.take()
        }
    }

    /// Drop all legacy handles before abort can return SPDK extents to the
    /// allocator; keep their leases until cleanup is done. Owned handles do
    /// their own cleanup on Drop and must never be aborted by block id.
    fn abort_uncommitted(
        contexts: &[WriteContext],
        files: &mut [Option<BlockWriteContext>],
        start: usize,
        abort: impl Fn(&ExtendedBlock, bool) -> CommonResult<()>,
    ) {
        // Record uncertainty before Drop. Classify by ID so an overlapping
        // legacy member cannot free an extent another member marked uncertain.
        let uncertain = Self::uncertain_blocks(contexts, files);
        for file in files.iter_mut() {
            drop(Self::take_legacy_file(file));
        }
        for (context, file) in contexts.iter().zip(files.iter()).skip(start) {
            if Self::is_owned_file(file) {
                continue;
            }
            if let Err(e) = abort(&context.block, uncertain.contains(&context.block.id)) {
                log::warn!(
                    "failed to abort batch block {} after batch failure: {}",
                    context.block.id,
                    e
                );
            }
        }
    }

    fn rollback_batch_from(&mut self, start: usize) {
        let mut files = self.file.take().unwrap_or_default();
        if let Some(contexts) = self.context.take() {
            Self::abort_uncommitted(&contexts, &mut files, start, |block, uncertain| {
                if uncertain {
                    self.write_handler.store.abort_block_uncertain(block)
                } else {
                    self.write_handler.store.abort_batch_write(block.id)
                }
            });
        }
        drop(files);
        self.leases.clear();
        self.is_commit = true;
        self.rolled_back = true;
    }

    fn rollback_batch(&mut self) {
        self.rollback_batch_from(0);
    }

    fn finish_running(&mut self, result: FsResult<Message>) -> FsResult<Message> {
        // A mixed batch may have advanced only a subset of its owners. Close
        // it on error; legacy-only batches keep their existing retry behavior.
        if result.is_err() {
            if self.has_owned_files() {
                self.rollback_batch();
            } else if let (Some(contexts), Some(files)) = (&self.context, &self.file) {
                // Preserve legacy Running retry behavior, but quarantine any
                // uncertain extent now, even if no terminal frame arrives.
                for id in Self::uncertain_blocks(contexts, files) {
                    if let Err(e) = self
                        .write_handler
                        .store
                        .abort_block_uncertain(&ExtendedBlock::with_id(id))
                    {
                        log::warn!("failed to quarantine batch block {}: {}", id, e);
                    }
                }
            }
        }
        result
    }

    pub fn open_batch(&mut self, msg: &Message) -> FsResult<Message> {
        let header: BlocksBatchWriteRequest = msg.parse_header()?;
        let mut responses = Vec::with_capacity(header.blocks.len());
        let mut files = Vec::with_capacity(header.blocks.len());
        let mut contexts = Vec::with_capacity(header.blocks.len());
        if !self.leases.is_empty() || !self.pending_cancel.is_empty() {
            return err_box!("previous batch write is still active");
        }
        let mut leases = Vec::with_capacity(header.blocks.len());
        let opened: FsResult<()> = (|| {
            for (i, block_proto) in header.blocks.into_iter().enumerate() {
                let unique_req_id = Self::member_req_id(msg.req_id(), i)?;
                let block_header = BlockWriteRequest {
                    block: block_proto,
                    off: header.off,
                    block_size: header.block_size,
                    short_circuit: header.short_circuit,
                    client_name: header.client_name.clone(),
                    chunk_size: header.chunk_size,
                    pipeline_stream: Vec::new(),
                    component_info: header.component_info.clone(),
                };
                let single_msg_req = Builder::new()
                    .code(msg.code())
                    .request(RequestStatus::Open)
                    .req_id(unique_req_id)
                    .seq_id(msg.seq_id())
                    .proto_header(block_header)
                    .build();
                let response = self.write_handler.open_remote(&single_msg_req)?;

                files.push(self.write_handler.file.take());
                leases.push(self.write_handler.lease.take()); // None when the file owns it.
                let Some(context) = self.write_handler.context.take() else {
                    return err_box!(
                        "batch open did not create write context for block index {}",
                        i
                    );
                };
                contexts.push(context);
                responses.push(response.parse_header::<BlockWriteResponse>()?);
            }
            Ok(())
        })();
        if let Err(e) = opened {
            Self::abort_uncommitted(&contexts, &mut files, 0, |block, uncertain| {
                Self::abort_legacy(&self.write_handler.store, block, uncertain)
            });
            drop(files);
            return Err(e);
        }
        self.is_commit = false;
        self.rolled_back = false;
        self.file = Some(files);
        self.leases = leases;
        self.context = Some(contexts);
        let batch_response = BlocksBatchWriteResponse { responses };

        Ok(Builder::success(msg).proto_header(batch_response).build())
    }

    /// Validates the commit request against the batch's open contexts.
    /// Returns the blocks to commit/abort in batch order.
    fn validate_commit_blocks(
        contexts: &[WriteContext],
        blocks: &[ExtendedBlockProto],
        req_id: i64,
    ) -> FsResult<Vec<ExtendedBlock>> {
        if contexts.len() != blocks.len() {
            return err_box!(
                "batch commit block count mismatch: blocks={}, contexts={}",
                blocks.len(),
                contexts.len()
            );
        }

        let mut commit_blocks = Vec::with_capacity(blocks.len());
        for (i, (block_proto, context)) in blocks.iter().zip(contexts).enumerate() {
            Self::check_context(context, Self::member_req_id(req_id, i)?)?;

            let block = ProtoUtils::extend_block_from_pb(block_proto.clone());
            if block.id != context.block.id
                || block.storage_type != context.block.storage_type
                || block.file_type != context.block.file_type
            {
                return err_box!(
                    "batch commit block mismatch at index {}: opened={:?}, committed={:?}",
                    i,
                    context.block,
                    block
                );
            }
            if block.len > context.block_size {
                return err_box!(
                    "Invalid block length: {}, block size: {}",
                    block.len,
                    context.block_size
                );
            }
            commit_blocks.push(block);
        }
        Ok(commit_blocks)
    }

    fn check_files(
        files: &mut [Option<BlockWriteContext>],
        contexts: &[WriteContext],
        metrics: &WorkerMetrics,
    ) -> FsResult<()> {
        for (file, context) in files.iter_mut().zip(contexts) {
            if let Some(file) = file.as_mut() {
                file.flush().inspect_err(|e| {
                    WriteHandler::record_write_error(metrics, context.dir_id, e);
                })?;
                // A successful flush does not clear a prior uncertain write.
                if Self::is_uncertain(file) {
                    let error = FsError::io(std::io::Error::other(format!(
                        "block {} write outcome uncertain",
                        context.block.id
                    )));
                    WriteHandler::record_write_error(metrics, context.dir_id, &error);
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    fn checkpoint_files(
        files: &mut [Option<BlockWriteContext>],
        contexts: &[WriteContext],
        metrics: &WorkerMetrics,
    ) -> FsResult<()> {
        for (file, context) in files.iter_mut().zip(contexts) {
            if let Some(file) = file.as_mut().filter(|file| file.is_owned()) {
                // For an owned file, sync_data checkpoints the stage and its
                // watermark. flush alone only checks session health.
                file.sync_data().inspect_err(|e| {
                    WriteHandler::record_write_error(metrics, context.dir_id, e);
                })?;
            }
        }
        Ok(())
    }

    fn complete_response(msg: &Message, num_blocks: usize) -> Message {
        Builder::success(msg)
            .proto_header(BlocksBatchCommitResponse {
                results: vec![true; num_blocks],
            })
            .build()
    }

    fn cancel_pending(&mut self, msg: &Message, blocks: &[ExtendedBlock]) -> FsResult<Message> {
        let mut first_error = None;
        for (slot, block) in self.pending_cancel.iter_mut().zip(blocks) {
            let result = match slot.as_mut() {
                Some(CancelMember::Owned(file)) => file
                    .finish_owned(block.len, false)
                    .unwrap_or_else(|| err_box!("missing owner for staged block {}", block.id)),
                Some(CancelMember::Legacy { uncertain, .. }) => {
                    Self::abort_legacy(&self.write_handler.store, block, *uncertain)
                }
                None => continue,
            };
            match result {
                Ok(()) => *slot = None,
                Err(e) => {
                    first_error.get_or_insert(e);
                }
            }
        }
        if let Some(e) = first_error {
            return Err(e.into());
        }
        self.pending_cancel.clear();
        self.context.take();
        Ok(Self::complete_response(msg, blocks.len()))
    }

    pub fn complete_batch(&mut self, msg: &Message, commit: bool) -> FsResult<Message> {
        let has_owned = self.has_owned_files();
        let cancelling = !self.pending_cancel.is_empty();
        if has_owned || cancelling {
            if let Some(context) = self.context.as_ref().and_then(|contexts| contexts.first()) {
                Self::check_context(context, msg.req_id())?;
            }
            if msg.header_len() == 0 || !msg.data.is_empty() {
                return err_box!("invalid staged batch terminal frame");
            }
        }
        let header: BlocksBatchCommitRequest = match msg.parse_header() {
            Ok(header) => header,
            Err(e) => {
                // Preserve legacy rollback on a malformed terminal, but do
                // not destroy a staged session (or pending Cancel) on decode.
                if !self.is_commit && !has_owned {
                    self.rollback_batch();
                }
                return Err(e.into());
            }
        };
        if (has_owned || cancelling) && header.req_id != msg.req_id() {
            return err_box!("batch terminal header request id mismatch");
        }
        if cancelling {
            if commit {
                return err_box!("batch cancel is pending; Complete cannot be retried");
            }
            let Some(contexts) = self.context.as_ref() else {
                return err_box!("batch cancel retry without open context");
            };
            let blocks = Self::validate_commit_blocks(contexts, &header.blocks, msg.req_id())?;
            return self.cancel_pending(msg, &blocks);
        }

        if self.is_commit {
            return if !msg.data.is_empty() {
                err_box!("The block has been committed and data cannot be written anymore.")
            } else if self.rolled_back && commit {
                // Checkpoints can survive rollback, but they are not committed.
                err_box!(
                    "batch write was rolled back after a previous failure; retry the batch write"
                )
            } else {
                Ok(msg.success())
            };
        }

        let Some(contexts) = self.context.as_ref() else {
            return err_box!("batch commit without open context");
        };
        let Some(files) = self.file.as_mut() else {
            return err_box!("batch commit without open files");
        };
        if files.len() != contexts.len() {
            let error = FsError::common("batch commit file state mismatch");
            self.rollback_batch();
            return Err(error);
        }
        let commit_blocks =
            match Self::validate_commit_blocks(contexts, &header.blocks, msg.req_id()) {
                Ok(blocks) => blocks,
                Err(e) => {
                    self.rollback_batch();
                    return Err(e);
                }
            };

        if !commit && has_owned {
            // Cancel must reach failed owners without flushing them. Snapshot
            // legacy uncertainty and drop legacy handles before any abort.
            let uncertain = Self::uncertain_blocks(contexts, files);
            let mut leases = std::mem::take(&mut self.leases).into_iter();
            let mut seen = HashSet::new();
            self.pending_cancel = std::mem::take(files)
                .into_iter()
                .zip(&commit_blocks)
                .map(|(file, block)| {
                    let lease = leases.next().flatten();
                    match file {
                        Some(file) if file.is_owned() => Some(CancelMember::Owned(file)),
                        // Legacy slots may overlap. Abort that ID only once.
                        _ if seen.insert(block.id) => Some(CancelMember::Legacy {
                            uncertain: uncertain.contains(&block.id),
                            _lease: lease,
                        }),
                        _ => None,
                    }
                })
                .collect();
            self.file.take();
            self.is_commit = true;
            self.rolled_back = true;
            return self.cancel_pending(msg, &commit_blocks);
        }

        // Validate every staged extent before finalizing any member. Legacy
        // layouts still check their actual length at finalize.
        if commit {
            for (block, file) in commit_blocks.iter().zip(files.iter()) {
                if let Some(file) = file.as_ref().filter(|file| file.is_owned()) {
                    let actual = file.device_len();
                    if block.len != actual {
                        let error = FsError::common(format!(
                            "staged block {} length mismatch: expected {}, actual {}",
                            block.id, block.len, actual
                        ));
                        self.rollback_batch();
                        return Err(error);
                    }
                }
            }
        }
        if let Err(e) = Self::check_files(files, contexts, self.write_handler.metrics) {
            self.rollback_batch();
            return Err(e);
        }
        // Do not checkpoint the whole batch here. Each owned finish commits
        // its own checkpoint; untouched siblings keep only their prior flushes.

        // Drop every legacy handle before finalizing/aborting. Keep their
        // checksum hints, and keep the owned handles for owner-scoped finish.
        let mut checksum_hints: Vec<_> = files
            .iter_mut()
            .map(|file| Self::take_legacy_file(file).and_then(|mut file| file.take_checksum()))
            .collect();
        let num_blocks = commit_blocks.len();
        for (i, block) in commit_blocks.into_iter().enumerate() {
            let result = if let Some(file) = files[i].as_mut() {
                file.finish_owned(block.len, commit)
                    .unwrap_or_else(|| err_box!("missing owner for staged block {}", block.id))
            } else if commit {
                self.write_handler
                    .store
                    .finalize_block_with_checksum(&block, checksum_hints[i].take())
                    .map(|_| ())
            } else {
                self.write_handler.store.abort_block(&block)
            };
            if let Err(e) = result {
                // Never abort earlier entries: another writer may have opened
                // a new generation since we finished them.
                self.rollback_batch_from(i);
                return Err(e.into());
            }
        }
        self.file.take();
        self.context.take();
        self.leases.clear();
        self.is_commit = true;
        Ok(Self::complete_response(msg, num_blocks))
    }

    fn validate_running_contexts(contexts: &[WriteContext], req_id: i64) -> FsResult<()> {
        for (i, context) in contexts.iter().enumerate() {
            Self::check_context(context, Self::member_req_id(req_id, i)?)?;
        }
        Ok(())
    }

    pub fn write_batch(&mut self, msg: &Message) -> FsResult<Message> {
        if msg.header_len() == 0 || !msg.data.is_empty() {
            return err_box!("batch Running must carry file data in its protobuf header only");
        }
        let header: FilesBatchWriteRequest = msg.parse_header()?;
        if header.req_id != msg.req_id() || header.seq_id != msg.seq_id() {
            return err_box!("batch Running header request or sequence id mismatch");
        }
        let Some(files_vec) = self.file.as_mut() else {
            return err_box!("batch write without open files");
        };
        let Some(contexts_vec) = self.context.as_mut() else {
            return err_box!("batch write without open context");
        };
        if header.files.len() != files_vec.len() || header.files.len() != contexts_vec.len() {
            return err_box!(
                "batch write file count mismatch: request={}, files={}, contexts={}",
                header.files.len(),
                files_vec.len(),
                contexts_vec.len()
            );
        }
        if files_vec.iter().any(Option::is_none) {
            return err_box!(
                "batch remote write received running data for short-circuit batch writer"
            );
        }

        // Only I/O on a validated Running frame can make the batch fatal.
        Self::validate_running_contexts(contexts_vec, msg.req_id())?;
        let file_count = header.files.len();

        let mut contexts =
            std::mem::replace(contexts_vec, Vec::with_capacity(file_count)).into_iter();
        for (i, ((slot, file_data), context)) in files_vec
            .iter_mut()
            .zip(header.files)
            .zip(contexts.by_ref())
            .enumerate()
        {
            // Take the decoded content; Bytes::from(Vec<u8>) does not copy.
            let single_msg = Builder::new()
                .code(RpcCode::WriteBlock)
                .request(RequestStatus::Running)
                .req_id(header.req_id + i as i64)
                .seq_id(header.seq_id)
                .data(DataSlice::Bytes(bytes::Bytes::from(file_data.content)))
                .build();

            self.write_handler.file = slot.take();
            self.write_handler.context = Some(context);
            let result = self.write_handler.write(&single_msg);

            // Restore the borrowed session even on error, so rollback can
            // distinguish owned and legacy files throughout the batch.
            *slot = self.write_handler.file.take();
            contexts_vec.extend(self.write_handler.context.take());
            if let Err(e) = result {
                contexts_vec.extend(contexts);
                return self.finish_running(Err(e));
            }
        }

        let batch_response = FilesBatchWriteResponse {
            results: vec![true; file_count],
        };
        Ok(Builder::success(msg).proto_header(batch_response).build())
    }

    pub fn flush_batch(&mut self, msg: &Message) -> FsResult<Message> {
        if !msg.data.is_empty() {
            return err_box!("batch flush alias cannot carry data");
        }
        let header: DataHeaderProto = msg.parse_header()?;
        if !header.flush {
            return err_box!("batch writer received non-flush WriteBlock running request");
        }

        let Some(files) = self.file.as_mut() else {
            return err_box!("batch flush without open files");
        };
        let Some(contexts) = self.context.as_ref() else {
            return err_box!("batch flush without open context");
        };
        if files.len() != contexts.len() {
            return err_box!(
                "batch flush state mismatch: files={}, contexts={}",
                files.len(),
                contexts.len()
            );
        }
        Self::validate_running_contexts(contexts, msg.req_id())?;
        // Drain/check every device before creating any new checkpoints.
        let metrics = self.write_handler.metrics;
        let result = Self::check_files(files, contexts, metrics)
            .and_then(|_| Self::checkpoint_files(files, contexts, metrics))
            .map(|()| msg.success());
        self.finish_running(result)
    }

    pub fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        match (RpcCode::from(msg.code()), msg.request_status()) {
            (RpcCode::WriteBlocksBatch, RequestStatus::Open) => self.open_batch(msg),
            (RpcCode::WriteBlock, RequestStatus::Running) => self.flush_batch(msg),
            (RpcCode::WriteBlocksBatch, RequestStatus::Running) => self.write_batch(msg),
            (RpcCode::WriteBlocksBatch, RequestStatus::Complete) => self.complete_batch(msg, true),
            (RpcCode::WriteBlocksBatch, RequestStatus::Cancel) => self.complete_batch(msg, false),
            _ => err_box!("Unsupported request type"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::block::BlockStore;
    use crate::worker::storage::BlockWriteContext;
    use crate::worker::Worker;
    use curvine_config::{ClusterConf, WorkerConf};
    use curvine_core_error::CommonResult;
    use curvine_fs_api::RpcCode;
    use curvine_io::{BlockIO, DataSlice};
    use curvine_model::{ExtendedBlock, FileType, StorageType};
    use curvine_proto::FileWriteData;
    use curvine_runtime::common::Utils;
    use curvine_storage_local::{BlockChecksum, Dataset, OwnedBlockWrite};
    use std::sync::{Arc, Mutex};

    fn store() -> CommonResult<BlockStore> {
        let conf = ClusterConf {
            format_worker: true,
            worker: WorkerConf {
                dir_reserved: "0".into(),
                data_dir: vec![format!(
                    "[MEM:4MB]../testing/batch-rollback-{}",
                    Utils::uuid()
                )],
                ..WorkerConf::default()
            },
            ..ClusterConf::default()
        };
        // The handler shares Worker metrics/config but does not start any listeners.
        let _worker = Worker::with_conf(conf.clone())?;
        BlockStore::new("test", &conf)
    }

    fn open_batch(handler: &mut BatchWriteHandler) -> FsResult<()> {
        let blocks: Vec<_> = (1..=3)
            .map(|id| {
                ProtoUtils::extend_block_to_pb(ExtendedBlock::new(
                    id,
                    4,
                    StorageType::Mem,
                    FileType::File,
                ))
            })
            .collect();
        let open = Builder::new()
            .code(RpcCode::WriteBlocksBatch)
            .request(RequestStatus::Open)
            .req_id(7)
            .proto_header(BlocksBatchWriteRequest {
                blocks,
                off: 0,
                block_size: 4,
                req_id: 7,
                seq_id: 0,
                chunk_size: 4,
                short_circuit: false,
                client_name: "test".into(),
                component_info: None,
            })
            .build();
        handler.open_batch(&open)?;
        Ok(())
    }

    fn commit_msg(block_ids: &[i64], req_id: i64) -> Message {
        terminal_msg(block_ids, req_id, false)
    }

    fn terminal_msg(block_ids: &[i64], req_id: i64, cancel: bool) -> Message {
        // No data was written, so the committed length is 0 (<= block_size 4).
        let blocks = block_ids
            .iter()
            .map(|id| {
                ProtoUtils::extend_block_to_pb(ExtendedBlock::new(
                    *id,
                    0,
                    StorageType::Mem,
                    FileType::File,
                ))
            })
            .collect();
        Builder::new()
            .code(RpcCode::WriteBlocksBatch)
            .request(if cancel {
                RequestStatus::Cancel
            } else {
                RequestStatus::Complete
            })
            .req_id(req_id)
            .proto_header(BlocksBatchCommitRequest {
                blocks,
                off: 0,
                block_size: 4,
                req_id,
                seq_id: 0,
                cancel,
                component_info: None,
            })
            .build()
    }

    fn write_msg(contents: &[&[u8]]) -> Message {
        Builder::new()
            .code(RpcCode::WriteBlocksBatch)
            .request(RequestStatus::Running)
            .req_id(7)
            .seq_id(1)
            .proto_header(FilesBatchWriteRequest {
                files: contents
                    .iter()
                    .map(|content| FileWriteData {
                        path: String::new(),
                        content: content.to_vec(),
                    })
                    .collect(),
                req_id: 7,
                seq_id: 1,
                ..Default::default()
            })
            .build()
    }

    fn flush_msg() -> Message {
        Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Running)
            .req_id(7)
            .proto_header(DataHeaderProto {
                flush: true,
                ..Default::default()
            })
            .build()
    }

    /// Replaces the batch's opened file for `index` with a write context whose
    /// `flush()` always fails, so `complete_batch` reaches the flush-error
    /// rollback branch.
    fn make_flush_fail(handler: &mut BatchWriteHandler, index: usize) -> FsResult<()> {
        let file = handler.file.as_mut().unwrap();
        let device = TestBlockIO {
            fail_flush: true,
            ..Default::default()
        };
        file[index] = Some(BlockWriteContext::new(device, 0, 4, 0)?);
        Ok(())
    }

    #[derive(Default)]
    struct TestBlockIO {
        fail_flush: bool,
        uncertain: bool,
        finish_failures: usize,
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    impl BlockIO for TestBlockIO {
        fn read_region(
            &mut self,
            _enable_send_file: bool,
            _len: i32,
        ) -> curvine_io::IOResult<DataSlice> {
            unreachable!()
        }

        fn write_region(&mut self, _region: &DataSlice) -> curvine_io::IOResult<()> {
            Ok(())
        }

        fn write_all(&mut self, _buf: &[u8]) -> curvine_io::IOResult<()> {
            unreachable!()
        }

        fn read_all(&mut self, _buf: &mut [u8]) -> curvine_io::IOResult<()> {
            unreachable!()
        }

        fn flush(&mut self) -> curvine_io::IOResult<()> {
            self.calls.lock().unwrap().push("flush");
            if self.fail_flush {
                Err(curvine_io::IOError::from("flush failed"))
            } else {
                Ok(())
            }
        }

        fn sync_data(&mut self) -> curvine_io::IOResult<()> {
            self.calls.lock().unwrap().push("checkpoint");
            Ok(())
        }

        fn has_uncertain_writes(&self) -> bool {
            self.uncertain
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
            "test-block-io"
        }

        fn resize(
            &mut self,
            _truncate: bool,
            _off: i64,
            _len: i64,
            _mode: i32,
        ) -> curvine_io::IOResult<()> {
            unreachable!()
        }
    }

    impl OwnedBlockWrite for TestBlockIO {
        fn finish(
            &mut self,
            _len: i64,
            commit: bool,
            _checksum: Option<BlockChecksum>,
        ) -> CommonResult<()> {
            self.calls
                .lock()
                .unwrap()
                .push(if commit { "complete" } else { "cancel" });
            if self.finish_failures > 0 {
                self.finish_failures -= 1;
                return err_box!("injected owned finish failure");
            }
            Ok(())
        }
    }

    fn install_owned(
        handler: &mut BatchWriteHandler,
        index: usize,
        device: TestBlockIO,
    ) -> FsResult<Arc<Mutex<Vec<&'static str>>>> {
        let calls = Arc::clone(&device.calls);
        handler.file.as_mut().unwrap()[index] = Some(BlockWriteContext::new_owned(device, 4, 0)?);
        handler.leases[index].take();
        Ok(calls)
    }

    #[test]
    fn mixed_cancel_retries_only_failed_members() -> CommonResult<()> {
        let store = store()?;
        let mut handler = BatchWriteHandler::new(store.clone(), "test".into())?;
        open_batch(&mut handler)?;
        let failed = install_owned(
            &mut handler,
            1,
            TestBlockIO {
                fail_flush: true, // Explicit Cancel must not check health first.
                finish_failures: 1,
                ..Default::default()
            },
        )?;
        let already_cancelled = install_owned(&mut handler, 2, TestBlockIO::default())?;

        let cancel = terminal_msg(&[1, 2, 3], 7, true);
        assert!(handler.handle(&cancel).is_err());
        assert!(store.get_block(1).is_err());
        // Reopen a legacy slot after its Cancel succeeded. A retry must not
        // abort the new writer or revisit a successfully cancelled owner.
        store.open_block(&ExtendedBlock::new(1, 4, StorageType::Mem, FileType::File))?;
        assert!(handler.handle(&commit_msg(&[1, 2, 3], 7)).is_err());
        assert!(handler.handle(&terminal_msg(&[1, 2, 3], 8, true)).is_err());
        assert!(handler.handle(&terminal_msg(&[1, 2, 4], 7, true)).is_err());
        assert_eq!(*failed.lock().unwrap(), ["cancel"]);

        let reply: BlocksBatchCommitResponse = handler.handle(&cancel)?.parse_header()?;
        assert_eq!(reply.results, [true; 3]);
        assert_eq!(*failed.lock().unwrap(), ["cancel", "cancel"]);
        assert_eq!(*already_cancelled.lock().unwrap(), ["cancel"]);
        assert!(store.get_block(1).is_ok());
        assert!(handler.handle(&commit_msg(&[1, 2, 3], 7)).is_err());
        Ok(())
    }

    #[test]
    fn failed_complete_does_not_checkpoint_untouched_owned_siblings() -> CommonResult<()> {
        let store = store()?;
        let mut handler = BatchWriteHandler::new(store.clone(), "test".into())?;
        open_batch(&mut handler)?;
        let first = install_owned(&mut handler, 0, TestBlockIO::default())?;
        let last = install_owned(&mut handler, 2, TestBlockIO::default())?;
        handler.handle(&flush_msg())?;
        store.abort_block(&ExtendedBlock::with_id(2))?;

        assert!(handler.handle(&commit_msg(&[1, 2, 3], 7)).is_err());
        assert_eq!(
            *first.lock().unwrap(),
            ["flush", "checkpoint", "flush", "complete"]
        );
        assert_eq!(*last.lock().unwrap(), ["flush", "checkpoint", "flush"]);
        Ok(())
    }

    #[test]
    fn invalid_running_frames_leave_owned_batch_open() -> CommonResult<()> {
        let mut handler = BatchWriteHandler::new(store()?, "test".into())?;
        open_batch(&mut handler)?;
        let owned = install_owned(&mut handler, 1, TestBlockIO::default())?;
        let running = write_msg(&[b"", b"", b""]);
        let mut wrong_req_id = write_msg(&[b"", b"", b""]);
        wrong_req_id.protocol.req_id += 1;
        let mut wrong_seq_id = write_msg(&[b"", b"", b""]);
        wrong_seq_id.protocol.seq_id += 1;
        let malformed = Builder::protocol(running.protocol)
            .header(bytes::BytesMut::from(&[0xff][..]))
            .build();
        let mut stale_flush = flush_msg();
        stale_flush.protocol.req_id += 1;
        let wrong_flush = Builder::protocol(flush_msg().protocol)
            .proto_header(DataHeaderProto::default())
            .build();
        for frame in [
            wrong_req_id,
            wrong_seq_id,
            malformed,
            stale_flush,
            wrong_flush,
        ] {
            assert!(handler.handle(&frame).is_err());
            assert!(!handler.is_commit);
        }
        assert!(owned.lock().unwrap().is_empty());
        handler.handle(&running)?;
        handler.handle(&flush_msg())?;
        assert_eq!(*owned.lock().unwrap(), ["flush", "checkpoint"]);
        Ok(())
    }

    #[test]
    fn complete_quarantines_sticky_spdk_uncertainty_after_successful_flush() -> CommonResult<()> {
        store()?; // Initialize the worker config/metrics without starting SPDK.
        let dataset = curvine_storage_local::BlockDataset::new_spdk_test_dataset(1, 1 << 30)?;
        let store = BlockStore::from_dataset_for_test(dataset);
        let block_size = 4096;
        let block = ExtendedBlock::new(1, block_size, StorageType::SpdkDisk, FileType::File);
        let meta = store.open_block(&block)?;
        let mut handler = BatchWriteHandler::new(store.clone(), "test".into())?;
        handler.leases.push(store.ordinary_write_lease(block.id)?);
        handler.context = Some(vec![WriteContext {
            block: block.clone(),
            req_id: 7,
            chunk_size: 4096,
            short_circuit: false,
            off: 0,
            block_size,
            dir_id: meta.dir_id(),
            dir_metrics: None,
        }]);
        handler.file = Some(vec![Some(BlockWriteContext::new(
            TestBlockIO {
                uncertain: true,
                ..Default::default()
            },
            0,
            block_size,
            0,
        )?)]);
        let complete = Builder::new()
            .code(RpcCode::WriteBlocksBatch)
            .request(RequestStatus::Complete)
            .req_id(7)
            .proto_header(BlocksBatchCommitRequest {
                blocks: vec![ProtoUtils::extend_block_to_pb(block)],
                block_size,
                req_id: 7,
                ..Default::default()
            })
            .build();
        assert!(handler
            .handle(&complete)
            .unwrap_err()
            .to_string()
            .contains("uncertain"));
        assert!(store.get_raw_block_opt(meta.id)?.unwrap().is_quarantined());
        Ok(())
    }

    #[test]
    fn complete_failure_aborts_all_uncommitted_siblings() -> CommonResult<()> {
        let store = store()?;
        let mut handler = BatchWriteHandler::new(store.clone(), "test".into())?;
        open_batch(&mut handler)?;
        assert!(store.get_block(1).is_ok());
        assert!(store.get_block(2).is_ok());
        assert!(store.get_block(3).is_ok());
        assert!(!handler.is_commit);

        // The commit request omits block 3: the count-mismatch guard fails and
        // the whole batch must be rolled back.
        let msg = commit_msg(&[1, 2], 9);
        assert!(handler.complete_batch(&msg, true).is_err());
        assert!(handler.is_commit);
        assert!(handler.context.is_none());
        assert!(handler.leases.is_empty());
        for id in 1..=3 {
            assert!(store.get_block(id).is_err(), "block {} must be aborted", id);
        }
        Ok(())
    }

    #[test]
    fn req_id_mismatch_failure_aborts_all_uncommitted_siblings() -> CommonResult<()> {
        let store = store()?;
        let mut handler = BatchWriteHandler::new(store.clone(), "test".into())?;
        open_batch(&mut handler)?;

        // Wrong req_id: per-block validation fails, triggering a full rollback.
        let msg = commit_msg(&[1, 2, 3], 42);
        assert!(handler.complete_batch(&msg, true).is_err());
        assert!(handler.is_commit);
        assert!(handler.context.is_none());
        for id in 1..=3 {
            assert!(store.get_block(id).is_err(), "block {} must be aborted", id);
        }
        Ok(())
    }

    // Regression test for the flush-error rollback branch: a middle block
    // whose flush fails must roll back the whole batch (triggered block and
    // all siblings), reset handler state, and reclaim staging allocations.
    #[test]
    fn flush_error_aborts_all_siblings_and_resets_handler() -> CommonResult<()> {
        let store = store()?;
        let mut handler = BatchWriteHandler::new(store.clone(), "test".into())?;
        open_batch(&mut handler)?;
        make_flush_fail(&mut handler, 1)?; // middle block (index 1)

        // Valid commit request so validation passes and the loop reaches flush.
        let msg = commit_msg(&[1, 2, 3], 7);
        assert!(handler.complete_batch(&msg, true).is_err());
        assert!(handler.rolled_back);
        assert!(handler.is_commit);
        assert!(handler.context.is_none());
        assert!(handler.leases.is_empty());
        for id in 1..=3 {
            assert!(store.get_block(id).is_err(), "block {} must be aborted", id);
        }
        Ok(())
    }

    #[test]
    fn running_write_failure_keeps_legacy_batch_slots_in_order() -> CommonResult<()> {
        let mut handler = BatchWriteHandler::new(store()?, "test".into())?;
        open_batch(&mut handler)?;

        assert!(handler.handle(&write_msg(&[b"a", b"12345", b"c"])).is_err());
        assert!(!handler.rolled_back);
        handler.handle(&write_msg(&[b"", b"b", b"c"]))?;
        for file in handler.file.as_ref().unwrap() {
            assert_eq!(file.as_ref().unwrap().block_pos(), 1);
        }
        Ok(())
    }

    #[test]
    fn running_flush_failure_does_not_rollback_legacy_only_batch() -> CommonResult<()> {
        let store = store()?;
        let mut handler = BatchWriteHandler::new(store.clone(), "test".into())?;
        open_batch(&mut handler)?;
        make_flush_fail(&mut handler, 1)?;

        assert!(handler.handle(&flush_msg()).is_err());
        assert!(!handler.is_commit);
        assert!(!handler.rolled_back);
        for id in 1..=3 {
            assert!(store.get_block(id).is_ok());
        }
        Ok(())
    }

    // Regression test for the finalize-error rollback branch: pre-abort block 2
    // so `finalize_block(2)` fails after block 1 was finalized. Block 1 must
    // stay final, block 3 must be aborted, handler state reset, and the
    // already-finalized block 1 survives (its staging was never opened).
    #[test]
    fn finalize_error_aborts_remaining_siblings_and_keeps_committed() -> CommonResult<()> {
        let store = store()?;
        let mut handler = BatchWriteHandler::new(store.clone(), "test".into())?;
        open_batch(&mut handler)?;

        // Abort block 2 behind the handler's back: it leaves the dataset, so
        // finalize_block(2) fails with "block 2 not exists" during commit.
        let block2 = ExtendedBlock::new(2, 0, StorageType::Mem, FileType::File);
        store.abort_block(&block2)?;
        assert!(store.get_block(2).is_err());

        let msg = commit_msg(&[1, 2, 3], 7);
        assert!(handler.complete_batch(&msg, true).is_err());
        assert!(handler.rolled_back);
        assert!(handler.context.is_none());
        assert!(handler.leases.is_empty());
        // Block 1 was finalized before the failure and must survive.
        assert!(store.get_block(1)?.is_final());
        // Block 3 was never committed and must be aborted by the rollback.
        assert!(store.get_block(3).is_err());
        Ok(())
    }

    // Regression test for the rewrite scenario (szbr00): a batch that
    // rewrites an already-finalized block on a file layout. The real rewrite
    // order is: finalize block 1 first, then open_batch over it — only then
    // does the dataset move the old meta into `committed_rewrites` and start
    // a new Writing generation. Rollback must abort that new generation,
    // release its reserved capacity, and keep the old committed copy.
    #[test]
    fn rewrite_rollback_aborts_new_generation_keeps_committed_copy() -> CommonResult<()> {
        let store = store()?;
        let mut handler = BatchWriteHandler::new(store.clone(), "test".into())?;

        // No batch is open yet: create block 1 and finalize it first, so the
        // batch later rewrites a committed generation.
        let available_before = store.read()?.available();
        let block1 = ExtendedBlock::new(1, 0, StorageType::Mem, FileType::File);
        store.open_block(&block1)?;
        store.finalize_block(&block1)?;
        assert!(store.read()?.get_block(1).unwrap().is_final());

        // Now open a 3-block batch over it: block 1 becomes a new Writing
        // generation backed by a staging copy of the committed file.
        open_batch(&mut handler)?;
        assert!(!store.read()?.get_block(1).unwrap().is_final());
        assert!(store.get_block(1)?.is_final());
        // The rewrite reserves capacity for the new generation.
        assert!(store.read()?.available() < available_before);

        // Trigger rollback via req_id mismatch. The old implementation's
        // `get_block(...).is_final()` guard read the readable view, which
        // reports the committed generation during a rewrite, and skipped
        // aborting block 1 — leaking the staging file and the reservation.
        let msg = commit_msg(&[1, 2, 3], 42);
        assert!(handler.complete_batch(&msg, true).is_err());
        assert!(handler.rolled_back);
        assert!(handler.context.is_none());
        assert!(handler.leases.is_empty());

        // The raw meta is the committed generation again (never deleted),
        // and the rewrite reservation was released.
        let raw = store
            .read()?
            .get_block(1)
            .cloned()
            .expect("block 1 meta must exist after rollback");
        assert!(raw.is_final());
        assert_eq!(store.read()?.available(), available_before);
        assert!(store.get_block(1)?.is_final());
        // Blocks 2/3 were never committed and must be aborted.
        assert!(store.get_block(2).is_err());
        assert!(store.get_block(3).is_err());
        Ok(())
    }

    // Regression test: a malformed protobuf commit header must roll back the
    // opened batch before returning the decode error.
    #[test]
    fn malformed_commit_header_rolls_back_batch() -> CommonResult<()> {
        let store = store()?;
        let mut handler = BatchWriteHandler::new(store.clone(), "test".into())?;
        open_batch(&mut handler)?;

        // Body with invalid protobuf wire data: header decode fails.
        let msg = Builder::new()
            .code(RpcCode::WriteBlocksBatch)
            .request(RequestStatus::Complete)
            .req_id(7)
            .data(DataSlice::Bytes(vec![0xFF, 0xFF, 0xFF, 0xFF].into()))
            .build();
        assert!(handler.complete_batch(&msg, true).is_err());
        assert!(handler.rolled_back);
        assert!(handler.is_commit);
        assert!(handler.context.is_none());
        for id in 1..=3 {
            assert!(store.get_block(id).is_err(), "block {} must be aborted", id);
        }
        Ok(())
    }

    // Regression test (solarAI00): after a successful commit, a malformed
    // Complete/Cancel header must NOT trigger a rollback. Every block is
    // already finalized, so rolling back would mark a committed batch as
    // rolled back and could abort a new Writing generation another writer
    // opened on the same block ids.
    #[test]
    fn malformed_header_after_success_does_not_rollback() -> CommonResult<()> {
        let store = store()?;
        let mut handler = BatchWriteHandler::new(store.clone(), "test".into())?;
        open_batch(&mut handler)?;

        let msg = commit_msg(&[1, 2, 3], 7);
        handler.complete_batch(&msg, true)?;
        assert!(handler.is_commit);
        assert!(!handler.rolled_back);

        // Malformed header on the committed handler: rejected, but the
        // committed state is untouched.
        let bad = Builder::new()
            .code(RpcCode::WriteBlocksBatch)
            .request(RequestStatus::Complete)
            .req_id(7)
            .data(DataSlice::Bytes(vec![0xFF, 0xFF, 0xFF, 0xFF].into()))
            .build();
        assert!(handler.complete_batch(&bad, true).is_err());
        assert!(handler.complete_batch(&bad, false).is_err());
        assert!(
            !handler.rolled_back,
            "committed batch must not be rolled back"
        );
        assert!(handler.is_commit);
        for id in 1..=3 {
            assert!(store.get_block(id)?.is_final(), "block {} stays final", id);
        }
        // Idempotent success is still reported after the malformed requests.
        handler.complete_batch(&msg, true)?;
        Ok(())
    }

    // After a rollback, a retried Complete must be rejected (the blocks are
    // gone) rather than misreported as a successful commit, while Cancel stays
    // idempotent and successful.
    #[test]
    fn complete_retry_rejected_after_rollback_cancel_stays_idempotent() -> CommonResult<()> {
        let store = store()?;
        let mut handler = BatchWriteHandler::new(store.clone(), "test".into())?;
        open_batch(&mut handler)?;

        // Roll back via wrong req_id.
        let bad = commit_msg(&[1, 2, 3], 42);
        assert!(handler.complete_batch(&bad, true).is_err());
        assert!(handler.rolled_back);

        // Same-connection retry of Complete must fail, not report success.
        let retry = commit_msg(&[1, 2, 3], 7);
        assert!(handler.complete_batch(&retry, true).is_err());
        assert!(handler.complete_batch(&retry, false).is_ok());
        Ok(())
    }

    #[test]
    fn successful_commit_still_works_after_fix() -> CommonResult<()> {
        let store = store()?;
        let mut handler = BatchWriteHandler::new(store.clone(), "test".into())?;
        open_batch(&mut handler)?;

        let msg = commit_msg(&[1, 2, 3], 7);
        handler.complete_batch(&msg, true)?;
        assert!(handler.is_commit);
        for id in 1..=3 {
            assert!(store.get_block(id)?.is_final());
        }
        // Repeated commit is idempotent.
        handler.complete_batch(&commit_msg(&[1, 2, 3], 7), true)?;
        Ok(())
    }
}
