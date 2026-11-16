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
use curvine_core_error::err_box;
use curvine_core_error::CommonResult;
use curvine_error::FsResult;
use curvine_fs_api::RpcCode;
use curvine_io::DataSlice;
use curvine_model::{ExtendedBlock, ProtoUtils};
use curvine_proto::{
    BlockWriteRequest, BlockWriteResponse, BlocksBatchCommitRequest, BlocksBatchCommitResponse,
    BlocksBatchWriteRequest, BlocksBatchWriteResponse, DataHeaderProto, ExtendedBlockProto,
    FilesBatchWriteRequest, FilesBatchWriteResponse,
};
use curvine_rpc::message::{Builder, Message, RequestStatus};

pub struct BatchWriteHandler {
    pub(crate) store: BlockStore,
    pub(crate) context: Option<Vec<WriteContext>>,
    pub(crate) file: Option<Vec<Option<BlockWriteContext>>>,
    leases: Vec<Option<BlockWriteLease>>,
    pub(crate) is_commit: bool,
    /// Set when the batch was rolled back after a fatal failure. Complete is
    /// then rejected (the blocks are gone, misreporting success would let
    /// `BlockClient::write_commit_batch()` treat an aborted batch as
    /// committed); Cancel stays idempotent and successful.
    rolled_back: bool,
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
        let store_clone = store.clone();
        Ok(Self {
            store,
            context: None,
            file: None,
            leases: Vec::new(),
            is_commit: false,
            rolled_back: false,
            write_handler: WriteHandler::new(store_clone, client_addr)?,
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

    /// Rolls back the batch's not-yet-committed blocks after a fatal
    /// `complete_batch` failure: blocks from `start` onward. Blocks before
    /// `start` were already committed (their finalize succeeded) and are
    /// left untouched — aborting them could delete a new generation another
    /// writer opened on the same block id after their commit. Each abort is
    /// best-effort — a failure is logged and never replaces the original
    /// error that caused the batch to fail.
    ///
    /// A block still in the committed, finalized generation is skipped (it
    /// is committed state, not batch staging), and a block being rewritten
    /// is handled atomically — the check and abort run under the same block
    /// stripe lock, so a concurrent finalize cannot slip between them and
    /// get its committed replica deleted. Missing blocks were already
    /// aborted and are fine too.
    ///
    /// The batch's write handles are dropped before any abort, matching the
    /// other abort paths (`open_batch` error, `WriteHandler::complete`):
    /// on SPDK the writer's Drop waits for in-flight I/O to drain, while
    /// `abort_block` returns the extent to the allocator, so freeing
    /// extents while writers are still alive could let a new allocation
    /// overwrite data still being written. Leases are kept until after the
    /// aborts so write fencing stays in place while blocks are aborted.
    ///
    /// State is reset (`context` / `file` / `leases` cleared, `is_commit =
    /// true`, `rolled_back = true`) so retried Complete/Cancel requests on
    /// this handler observe the rollback instead of misreporting the batch
    /// as committed.
    fn rollback_batch_from(&mut self, start: usize) {
        self.file.take();
        if let Some(contexts) = self.context.take() {
            for context in contexts.iter().skip(start) {
                if let Err(e) = self.store.abort_batch_write(context.block.id) {
                    log::warn!(
                        "failed to abort batch block {} after batch failure: {}",
                        context.block.id,
                        e
                    );
                }
            }
        }
        self.leases.clear();
        self.is_commit = true;
        self.rolled_back = true;
    }

    fn rollback_batch(&mut self) {
        self.rollback_batch_from(0);
    }

    fn abort_open_contexts(store: &BlockStore, contexts: &[WriteContext]) {
        for context in contexts {
            if let Err(e) = store.abort_block(&context.block) {
                log::warn!(
                    "failed to abort batch-opened block {} after open error: {}",
                    context.block.id,
                    e
                );
            }
        }
    }

    pub fn open_batch(&mut self, msg: &Message) -> FsResult<Message> {
        let header: BlocksBatchWriteRequest = msg.parse_header()?;
        let mut responses = Vec::with_capacity(header.blocks.len());
        let mut files = Vec::with_capacity(header.blocks.len());
        let mut contexts = Vec::with_capacity(header.blocks.len());
        if !self.leases.is_empty() {
            return err_box!("previous batch write is still active");
        }
        let mut leases = Vec::new();
        self.is_commit = false;

        for (i, block_proto) in header.blocks.iter().cloned().enumerate() {
            let unique_req_id = msg.req_id() + i as i64;
            // Create a single BlockWriteRequest from the block; propagate the
            // client's component_info so the per-block open carries the same
            // peer metadata as the outer batch frame. Named `block_header` (not
            // `header`) so it cannot be confused with the outer
            // BlocksBatchWriteRequest.
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

            // Create single request message for each block
            let single_msg_req = Builder::new()
                .code(msg.code())
                .request(RequestStatus::Open)
                .req_id(unique_req_id)
                .seq_id(msg.seq_id())
                .proto_header(block_header)
                .build();

            let response = match self.write_handler.open(&single_msg_req) {
                Ok(response) => response,
                Err(e) => {
                    files.clear();
                    Self::abort_open_contexts(&self.store, &contexts);
                    return Err(e);
                }
            };

            // Extract file and context from handler and store in batch vectors
            let Some(context) = self.write_handler.context.take() else {
                files.clear();
                Self::abort_open_contexts(&self.store, &contexts);
                return err_box!(
                    "batch open did not create write context for block index {}",
                    i
                );
            };
            let block_response: BlockWriteResponse = match response.parse_header() {
                Ok(response) => response,
                Err(e) => {
                    self.write_handler.file.take();
                    if let Err(abort_err) = self.store.abort_block(&context.block) {
                        log::warn!(
                            "failed to abort batch-opened block {} after response parse error: {}",
                            context.block.id,
                            abort_err
                        );
                    }
                    files.clear();
                    Self::abort_open_contexts(&self.store, &contexts);
                    return Err(e.into());
                }
            };
            responses.push(block_response);
            files.push(self.write_handler.file.take());
            contexts.push(context);
            leases.push(self.write_handler.lease.take());
        }
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
        files_len: usize,
        req_id: i64,
    ) -> FsResult<Vec<ExtendedBlock>> {
        if contexts.len() != blocks.len() || files_len != blocks.len() {
            return err_box!(
                "batch commit block count mismatch: blocks={}, contexts={}, files={}",
                blocks.len(),
                contexts.len(),
                files_len
            );
        }

        let mut commit_blocks = Vec::with_capacity(blocks.len());
        for (i, block_proto) in blocks.iter().enumerate() {
            let Some(context) = contexts.get(i) else {
                return err_box!("missing batch write context at index {}", i);
            };
            let expected_req_id = req_id + i as i64;
            Self::check_context(context, expected_req_id)?;

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

    pub fn complete_batch(&mut self, msg: &Message, commit: bool) -> FsResult<Message> {
        // Parse the flattened batch request. A malformed header must not be
        // returned before an OPEN batch is rolled back: the blocks would stay
        // in Writing and the staging allocations would never be reclaimed.
        // After a successful commit (`is_commit`) there is nothing left to
        // reclaim — every block is finalized — and rolling back would mark a
        // committed batch as rolled back and could abort a new generation
        // another writer opened on the same block ids.
        let header: BlocksBatchCommitRequest = match msg.parse_header() {
            Ok(header) => header,
            Err(e) => {
                if !self.is_commit {
                    self.rollback_batch();
                }
                return Err(e.into());
            }
        };
        let mut results = Vec::new();

        if self.is_commit {
            return if !msg.data.is_empty() {
                err_box!("The block has been committed and data cannot be written anymore.")
            } else if self.rolled_back && commit {
                // The batch was aborted by a previous failed Complete. Blocks
                // are gone, so reporting success would let the client treat an
                // aborted batch as committed. Cancel stays idempotent.
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
        let commit_blocks =
            match Self::validate_commit_blocks(contexts, &header.blocks, files.len(), msg.req_id())
            {
                Ok(blocks) => blocks,
                Err(e) => {
                    self.rollback_batch();
                    return Err(e);
                }
            };

        let flush_error = files.iter_mut().enumerate().find_map(|(i, file)| {
            let file = file.as_mut()?;
            file.flush().err().map(|e| (i, e))
        });
        if let Some((i, e)) = flush_error {
            if let Some(context) = self.context.as_ref().and_then(|c| c.get(i)) {
                WriteHandler::record_write_error(self.write_handler.metrics, context.dir_id, &e);
            }
            self.rollback_batch();
            return Err(e.into());
        }
        for file in files.iter_mut() {
            file.take();
        }

        for (i, block) in commit_blocks.into_iter().enumerate() {
            let result = if commit {
                self.store.finalize_block(&block).map(|_| ())
            } else {
                self.store.abort_block(&block)
            };
            if let Err(e) = result {
                // Blocks 0..i were already committed; a full rollback could
                // abort a new generation another writer opened on them since
                // their commit, so only the contexts from the failing index
                // onward are reclaimed. This still covers every block that
                // was never committed.
                self.rollback_batch_from(i);
                return Err(e.into());
            }
            results.push(true);
        }
        self.is_commit = true;
        self.leases.clear();
        let batch_response = BlocksBatchCommitResponse { results };

        Ok(Builder::success(msg).proto_header(batch_response).build())
    }

    pub fn write_batch(&mut self, msg: &Message) -> FsResult<Message> {
        let header: FilesBatchWriteRequest = msg.parse_header()?;

        // Use drain to extract elements while preserving the vector's allocated memory
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

        let files_drain = std::mem::take(files_vec);
        let contexts_drain = std::mem::take(contexts_vec);

        let mut files_iter = files_drain.into_iter();
        let mut contexts_iter = contexts_drain.into_iter();

        let file_count = header.files.len();
        for (i, file_data) in header.files.into_iter().enumerate() {
            // Take the decoded content; Bytes::from(Vec<u8>) does not copy.
            let data_slice = DataSlice::Bytes(bytes::Bytes::from(file_data.content));

            let unique_req_id = header.req_id + i as i64;
            // Create a temporary message for each file
            let single_msg = Builder::new()
                .code(RpcCode::WriteBlock)
                .request(RequestStatus::Running)
                .req_id(unique_req_id)
                .seq_id(header.seq_id)
                .data(data_slice)
                .build();

            // Get the next file and context from iterators (preserves original order)
            let Some(file) = files_iter.next() else {
                return err_box!("missing batch write file at index {}", i);
            };
            let Some(file) = file else {
                return err_box!("batch remote write missing file state at index {}", i);
            };
            let Some(context) = contexts_iter.next() else {
                return err_box!("missing batch write context at index {}", i);
            };

            self.write_handler.file = Some(file);
            self.write_handler.context = Some(context);

            if let Err(e) = self.write_handler.write(&single_msg) {
                let Some(file) = self.write_handler.file.take() else {
                    return err_box!(
                        "batch write lost file state after write error at index {}",
                        i
                    );
                };
                let Some(context) = self.write_handler.context.take() else {
                    return err_box!(
                        "batch write lost context state after write error at index {}",
                        i
                    );
                };
                files_vec.push(Some(file));
                contexts_vec.push(context);
                files_vec.extend(files_iter);
                contexts_vec.extend(contexts_iter);
                return Err(e);
            }

            let Some(file) = self.write_handler.file.take() else {
                return err_box!("batch write lost file state at index {}", i);
            };
            let Some(context) = self.write_handler.context.take() else {
                return err_box!("batch write lost context state at index {}", i);
            };

            files_vec.push(Some(file));
            contexts_vec.push(context);
        }

        let batch_response = FilesBatchWriteResponse {
            results: vec![true; file_count],
        };
        Ok(Builder::success(msg).proto_header(batch_response).build())
    }

    pub fn flush_batch(&mut self, msg: &Message) -> FsResult<Message> {
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
        for (i, (file, context)) in files.iter_mut().zip(contexts.iter()).enumerate() {
            let expected_req_id = msg.req_id() + i as i64;
            Self::check_context(context, expected_req_id)?;
            if let Some(file) = file.as_mut() {
                if let Err(e) = file.flush() {
                    WriteHandler::record_write_error(
                        self.write_handler.metrics,
                        context.dir_id,
                        &e,
                    );
                    return Err(e.into());
                }
            }
        }

        Ok(msg.success())
    }

    pub fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let request_status = msg.request_status();
        match request_status {
            // batch operations
            RequestStatus::Open => self.open_batch(msg),
            RequestStatus::Running if RpcCode::from(msg.code()) == RpcCode::WriteBlock => {
                self.flush_batch(msg)
            }
            RequestStatus::Running => self.write_batch(msg),
            RequestStatus::Complete => self.complete_batch(msg, true),
            RequestStatus::Cancel => self.complete_batch(msg, false),
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
    use curvine_runtime::common::Utils;
    use curvine_storage_local::Dataset;

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
            .request(RequestStatus::Complete)
            .req_id(req_id)
            .proto_header(BlocksBatchCommitRequest {
                blocks,
                off: 0,
                block_size: 4,
                req_id,
                seq_id: 0,
                cancel: false,
                component_info: None,
            })
            .build()
    }

    /// Replaces the batch's opened file for `index` with a write context whose
    /// `flush()` always fails, so `complete_batch` reaches the flush-error
    /// rollback branch.
    fn make_flush_fail(handler: &mut BatchWriteHandler, index: usize) -> FsResult<()> {
        let file = handler.file.as_mut().unwrap();
        file[index] = Some(BlockWriteContext::new(FailingFlushBlockIO, 0, 4, 0)?);
        Ok(())
    }

    struct FailingFlushBlockIO;

    impl BlockIO for FailingFlushBlockIO {
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
            unreachable!()
        }
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
