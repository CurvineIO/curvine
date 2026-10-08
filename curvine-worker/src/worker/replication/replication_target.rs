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
use crate::worker::handler::WriteHandler;
use curvine_core_error::err_box;
use curvine_error::FsResult;

use curvine_proto::{
    BlockWriteRequest, PrepareReplicationRequest, PrepareReplicationResponse,
    ReconcileReplicationRequest, ReconcileReplicationResponse, ReplicationWriteRequest,
};
use curvine_rpc::message::{Builder, Message, RequestStatus};
use curvine_runtime::common::Utils;
use curvine_runtime::sync::FastMutex;
use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

struct TargetAttempt {
    attempt_id: String,
    token: String,
    expires: Instant,
    writer: Option<WriteHandler>,
    storage_type: Option<i32>,
    retired: bool,
    store: BlockStore,
    _lease: BlockWriteLease,
}

/// The destination owns each writer, not its TCP connection. Revocation and every data/open/
/// complete/cancel operation use the SAME stripe lock. Revocation drops the file before aborting
/// it, including raw-device writers; a stale cancel can never abort a successor's allocation.
/// Tokens are destination-generated and never recreated, even by a delayed prepare RPC.
pub(crate) struct ReplicationTarget {
    store: BlockStore,
    session_id: String,
    attempts: Vec<FastMutex<HashMap<i64, TargetAttempt>>>,
    deadlines: FastMutex<BTreeSet<(Instant, i64, String)>>,
}

impl ReplicationTarget {
    pub(crate) fn new(store: BlockStore, session_id: String) -> Self {
        Self {
            store,
            session_id,
            deadlines: FastMutex::new(BTreeSet::new()),
            attempts: (0..256).map(|_| FastMutex::new(HashMap::new())).collect(),
        }
    }

    fn stripe(&self, block_id: i64) -> &FastMutex<HashMap<i64, TargetAttempt>> {
        &self.attempts[block_id as u64 as usize % self.attempts.len()]
    }

    fn check_session(&self, expected: &str) -> FsResult<()> {
        if self.session_id != expected {
            return err_box!("replication target session changed");
        }
        Ok(())
    }

    fn revoke(&self, block_id: i64, attempt: &mut TargetAttempt) -> FsResult<()> {
        attempt.retired = true;
        if let Some(mut writer) = attempt.writer.take() {
            // Close raw-device handles BEFORE releasing their allocations or lease.
            writer.file.take();
            if !writer.is_commit {
                if let Err(e) = attempt.store.abort_replication_write(block_id) {
                    attempt.writer = Some(writer);
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    pub(crate) fn prepare(
        &self,
        req: PrepareReplicationRequest,
    ) -> FsResult<PrepareReplicationResponse> {
        self.check_session(&req.target_session_id)?;
        let mut attempts = self.stripe(req.block_id).lock();
        if let Some(current) = attempts.get_mut(&req.block_id) {
            if current.expires > Instant::now() {
                if current.attempt_id == req.attempt_id {
                    return Ok(PrepareReplicationResponse {
                        token: current.token.clone(),
                    });
                }
                return err_box!("replication target still owns another attempt");
            }
            self.revoke(req.block_id, current)?;
            self.deadlines
                .lock()
                .remove(&(current.expires, req.block_id, current.token.clone()));
            attempts.remove(&req.block_id);
        }
        let token = Utils::uuid();
        let (store, lease) = self.store.replication_write_lease(req.block_id, &token)?;
        // A disconnected ordinary/short-circuit writer can leave a staging allocation.
        // Remove it before opening a successor. In particular, a short-circuit client
        // may still own an FD after its RPC session closes: unlinking its staging file
        // prevents that FD from modifying the new replication allocation.
        store.abort_replication_write(req.block_id)?;
        let expires =
            Instant::now() + Duration::from_millis(req.lifetime_ms.clamp(1000, 86_400_000));
        self.deadlines
            .lock()
            .insert((expires, req.block_id, token.clone()));
        attempts.insert(
            req.block_id,
            TargetAttempt {
                attempt_id: req.attempt_id,
                token: token.clone(),
                expires,
                writer: None,
                storage_type: None,
                retired: false,
                store,
                _lease: lease,
            },
        );
        Ok(PrepareReplicationResponse { token })
    }

    pub(crate) fn reconcile(
        &self,
        req: ReconcileReplicationRequest,
    ) -> FsResult<ReconcileReplicationResponse> {
        self.check_session(&req.target_session_id)?;
        let mut attempts = self.stripe(req.block_id).lock();
        let storage_type = if let Some(current) = attempts.get_mut(&req.block_id) {
            if current.attempt_id != req.attempt_id {
                // The old token is no longer authorized. Do not touch the new writer.
                return Ok(ReconcileReplicationResponse { storage_type: None });
            }
            self.revoke(req.block_id, current)?;
            let result = current.storage_type.filter(|_| {
                current
                    .store
                    .get_block(req.block_id)
                    .is_ok_and(|meta| meta.is_final())
            });
            // Preserve successful evidence and its exclusive lease until the Master has
            // committed metadata. A lost reconciliation response must be retryable.
            if req.release || result.is_none() {
                self.deadlines.lock().remove(&(
                    current.expires,
                    req.block_id,
                    current.token.clone(),
                ));
                attempts.remove(&req.block_id);
            }
            result
        } else {
            None
        };
        Ok(ReconcileReplicationResponse { storage_type })
    }

    pub(crate) fn write(&self, msg: &Message) -> FsResult<Message> {
        let req: ReplicationWriteRequest = msg.parse_header()?;
        let mut attempts = self.stripe(req.block_id).lock();
        let Some(attempt) = attempts.get_mut(&req.block_id) else {
            return err_box!("replication write token is retired");
        };
        if attempt.retired || attempt.token != req.token || attempt.expires <= Instant::now() {
            return err_box!("replication write token is stale or expired");
        }
        // Borrow the data; the envelope only copies the small protobuf header.
        let inner = Message::new(
            msg.protocol,
            (!req.header.is_empty()).then(|| req.header.as_slice().into()),
            curvine_io::DataSlice::mem_slice(msg.data.as_slice()),
        );
        if msg.request_status() != RequestStatus::Running {
            let header: BlockWriteRequest = inner.parse_header()?;
            if header.block.id != req.block_id
                || header.short_circuit
                || !header.pipeline_stream.is_empty()
            {
                return err_box!("invalid fenced replication write header");
            }
        }
        match msg.request_status() {
            RequestStatus::Open => {
                if attempt.writer.is_some() || attempt.storage_type.is_some() {
                    return err_box!("replication token has already opened a writer");
                }
                let mut writer = WriteHandler::new(attempt.store.clone(), "replication".into())?;
                let result = writer.handle(&inner);
                // Failed admission does not own an allocation. Never abort an existing
                // finalized block merely because opening this writer was rejected.
                if result.is_ok() {
                    attempt.writer = Some(writer);
                }
                result
            }
            RequestStatus::Running | RequestStatus::Complete => {
                let Some(writer) = attempt.writer.as_mut() else {
                    return err_box!("replication writer is not open");
                };
                let result = match writer.handle(&inner) {
                    Ok(result) => result,
                    Err(error) => {
                        let _ = self.revoke(req.block_id, attempt);
                        return Err(error);
                    }
                };
                if msg.request_status() == RequestStatus::Complete {
                    attempt.storage_type =
                        Some(attempt.store.get_block(req.block_id)?.storage_type().into());
                }
                Ok(result)
            }
            RequestStatus::Cancel => {
                self.revoke(req.block_id, attempt)?;
                // Keep the retired token inaccessible, without a per-attempt tombstone.
                self.deadlines.lock().remove(&(
                    attempt.expires,
                    req.block_id,
                    attempt.token.clone(),
                ));
                attempts.remove(&req.block_id);
                Ok(Builder::success(msg).build())
            }
            _ => err_box!("unsupported fenced replication operation"),
        }
    }

    pub(crate) fn reap(&self) {
        // A bounded due queue, not a full scan of every active/completed attempt per tick.
        for _ in 0..128 {
            let entry = {
                let mut deadlines = self.deadlines.lock();
                match deadlines.first() {
                    Some((due, _, _)) if *due <= Instant::now() => deadlines.pop_first(),
                    _ => None,
                }
            };
            let Some((_, block_id, token)) = entry else {
                break;
            };
            let mut attempts = self.stripe(block_id).lock();
            let Some(attempt) = attempts.get_mut(&block_id) else {
                continue;
            };
            if attempt.token != token {
                continue;
            }
            if self.revoke(block_id, attempt).is_ok() {
                attempts.remove(&block_id);
            } else {
                self.deadlines.lock().insert((
                    Instant::now() + Duration::from_secs(1),
                    block_id,
                    token,
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::storage::Dataset;
    use crate::worker::Worker;
    use curvine_config::{ClusterConf, WorkerConf};
    use curvine_core_error::CommonResult;
    use curvine_fs_api::RpcCode;
    use curvine_io::DataSlice;
    use curvine_model::{ExtendedBlock, ProtoUtils};
    use prost::Message as _;

    fn target() -> CommonResult<ReplicationTarget> {
        let conf = ClusterConf {
            format_worker: true,
            worker: WorkerConf {
                dir_reserved: "0".into(),
                data_dir: vec![format!(
                    "[MEM:4MB]../testing/replication-fence-{}",
                    Utils::uuid()
                )],
                ..WorkerConf::default()
            },
            ..ClusterConf::default()
        };
        // The handler shares Worker metrics/config but does not start any listeners.
        let _worker = Worker::with_conf(conf.clone())?;
        Ok(ReplicationTarget::new(
            BlockStore::new("test", &conf)?,
            "session".into(),
        ))
    }

    fn prepare(target: &ReplicationTarget, id: &str) -> FsResult<String> {
        Ok(target
            .prepare(PrepareReplicationRequest {
                block_id: 1,
                attempt_id: id.into(),
                target_session_id: "session".into(),
                lifetime_ms: 60_000,
            })?
            .token)
    }

    fn reconcile(target: &ReplicationTarget, id: &str, release: bool) -> FsResult<Option<i32>> {
        Ok(target
            .reconcile(ReconcileReplicationRequest {
                block_id: 1,
                attempt_id: id.into(),
                target_session_id: "session".into(),
                release,
            })?
            .storage_type)
    }

    fn frame(token: &str, status: RequestStatus, bytes: &[u8]) -> Message {
        let header = if status == RequestStatus::Running {
            Vec::new()
        } else {
            BlockWriteRequest {
                block: ProtoUtils::extend_block_to_pb(ExtendedBlock::with_mem(1, "4B").unwrap()),
                off: 0,
                block_size: 4,
                short_circuit: false,
                client_name: "test".into(),
                chunk_size: 4,
                pipeline_stream: Vec::new(),
                component_info: None,
            }
            .encode_to_vec()
        };
        Builder::new()
            .code(RpcCode::WriteReplicationBlock)
            .req_id(7)
            .seq_id(0)
            .request(status)
            .proto_header(ReplicationWriteRequest {
                block_id: 1,
                token: token.into(),
                header,
            })
            .data(DataSlice::Bytes(bytes.to_vec().into()))
            .build()
    }

    fn copy(target: &ReplicationTarget, token: &str) -> FsResult<()> {
        target.write(&frame(token, RequestStatus::Open, &[]))?;
        target.write(&frame(token, RequestStatus::Running, b"data"))?;
        target.write(&frame(token, RequestStatus::Complete, &[]))?;
        Ok(())
    }

    #[test]
    fn retired_requests_cannot_touch_successor_and_reconcile_is_repeatable() -> CommonResult<()> {
        let target = target()?;
        let old = prepare(&target, "old")?;
        target.write(&frame(&old, RequestStatus::Open, &[]))?;
        target.write(&frame(&old, RequestStatus::Running, b"old!"))?;
        assert_eq!(reconcile(&target, "old", false)?, None);
        let new = prepare(&target, "new")?;
        target.write(&frame(&new, RequestStatus::Open, &[]))?;
        for status in [
            RequestStatus::Open,
            RequestStatus::Running,
            RequestStatus::Complete,
            RequestStatus::Cancel,
        ] {
            assert!(target.write(&frame(&old, status, b"bad!")).is_err());
        }
        assert_eq!(reconcile(&target, "old", true)?, None);
        target.write(&frame(&new, RequestStatus::Running, b"data"))?;
        target.write(&frame(&new, RequestStatus::Complete, &[]))?;
        assert!(reconcile(&target, "new", false)?.is_some());
        // Model a lost response: the same request returns the same completion evidence.
        assert!(reconcile(&target, "new", false)?.is_some());
        assert!(target.store.remove_block(1).is_err());
        assert!(reconcile(&target, "new", true)?.is_some());
        let (_, mut reader) = target.store.open_reader_by_id_at_stored_len(1, 0)?;
        assert_eq!(reader.read_region(false, 4)?.as_slice(), b"data");
        Ok(())
    }

    #[test]
    fn delayed_prepare_and_old_session_never_resurrect_a_token() -> CommonResult<()> {
        let target = target()?;
        let old = prepare(&target, "same-id")?;
        assert_eq!(old, prepare(&target, "same-id")?);
        reconcile(&target, "same-id", false)?;
        let fresh = prepare(&target, "same-id")?;
        assert_ne!(old, fresh);
        assert!(target
            .write(&frame(&old, RequestStatus::Open, &[]))
            .is_err());
        assert!(target
            .prepare(PrepareReplicationRequest {
                block_id: 2,
                attempt_id: "old-master".into(),
                target_session_id: "previous-session".into(),
                lifetime_ms: 1000,
            })
            .is_err());
        Ok(())
    }

    #[test]
    fn ordinary_writers_and_deletion_cannot_overlap_a_replication_writer() -> CommonResult<()> {
        let target = target()?;
        let ordinary = target.store.ordinary_write_lease(1)?;
        assert!(prepare(&target, "replication").is_err());
        drop(ordinary);
        let token = prepare(&target, "replication")?;
        assert!(target.store.ordinary_write_lease(1).is_err());
        let block = ExtendedBlock::with_mem(1, "4B")?;
        assert!(target.store.open_block(&block).is_err());
        target.write(&frame(&token, RequestStatus::Open, &[]))?;
        target.write(&frame(&token, RequestStatus::Running, b"data"))?;
        assert!(target.store.abort_block(&block).is_err());
        assert!(target.store.finalize_block(&block).is_err());
        assert!(target.store.remove_block(1).is_err());
        target.store.read()?.increment_blocks_to_delete();
        assert!(target.store.async_remove_block(1).is_err());
        assert_eq!(target.store.read()?.num_blocks_to_delete(), 0);
        reconcile(&target, "replication", false)?;
        assert!(target.store.get_block(1).is_err());
        assert!(target.store.ordinary_write_lease(1)?.is_some());
        target.store.remove_block(1)?;
        Ok(())
    }

    #[test]
    fn ordinary_and_batch_handlers_hold_leases_until_their_files_close() -> CommonResult<()> {
        use crate::worker::handler::BatchWriteHandler;
        use curvine_proto::BlocksBatchWriteRequest;

        let target = target()?;
        let fenced = frame("unused", RequestStatus::Open, &[]);
        let envelope: ReplicationWriteRequest = fenced.parse_header()?;
        let header = BlockWriteRequest::decode(envelope.header.as_slice())?;
        let open = Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Open)
            .req_id(7)
            .proto_header(header.clone())
            .build();
        let mut ordinary = WriteHandler::new(target.store.clone(), "test".into())?;
        ordinary.open(&open)?;
        assert!(prepare(&target, "blocked-by-ordinary").is_err());
        drop(ordinary);
        prepare(&target, "after-ordinary")?;
        reconcile(&target, "after-ordinary", true)?;

        let mut batch = BatchWriteHandler::new(target.store.clone(), "test".into())?;
        let open = Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Open)
            .req_id(7)
            .proto_header(BlocksBatchWriteRequest {
                blocks: vec![header.block],
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
        batch.open_batch(&open)?;
        assert!(prepare(&target, "blocked-by-batch").is_err());
        drop(batch);
        let token = prepare(&target, "after-batch")?;
        copy(&target, &token)?;
        assert!(reconcile(&target, "after-batch", true)?.is_some());
        assert!(target.deadlines.lock().is_empty());
        Ok(())
    }

    #[test]
    fn disconnected_short_circuit_fd_cannot_modify_replication_successor() -> CommonResult<()> {
        use curvine_proto::BlockWriteResponse;
        use std::io::{Seek, SeekFrom, Write};

        let target = target()?;
        let envelope: ReplicationWriteRequest =
            frame("unused", RequestStatus::Open, &[]).parse_header()?;
        let mut header = BlockWriteRequest::decode(envelope.header.as_slice())?;
        header.short_circuit = true;
        let mut ordinary = WriteHandler::new(target.store.clone(), "test".into())?;
        let response: BlockWriteResponse = ordinary
            .open(
                &Builder::new()
                    .code(RpcCode::WriteBlock)
                    .request(RequestStatus::Open)
                    .req_id(7)
                    .proto_header(header)
                    .build(),
            )?
            .parse_header()?;
        let mut old_fd = std::fs::OpenOptions::new()
            .write(true)
            .open(response.path.unwrap())?;
        old_fd.write_all(b"old!")?;
        assert!(prepare(&target, "blocked").is_err());
        drop(ordinary); // Lose the RPC session without closing the client's local FD.
        let token = prepare(&target, "successor")?;
        copy(&target, &token)?;
        old_fd.seek(SeekFrom::Start(0))?;
        old_fd.write_all(b"bad!")?;
        old_fd.flush()?;
        assert!(reconcile(&target, "successor", true)?.is_some());
        let (_, mut reader) = target.store.open_reader_by_id_at_stored_len(1, 0)?;
        assert_eq!(reader.read_region(false, 4)?.as_slice(), b"data");
        Ok(())
    }

    #[test]
    fn failed_open_and_cancel_after_commit_preserve_existing_replica() -> CommonResult<()> {
        let target = target()?;
        let token = prepare(&target, "first")?;
        copy(&target, &token)?;
        target.write(&frame(&token, RequestStatus::Cancel, &[]))?;
        assert!(target.store.get_block(1)?.is_final());
        let next = prepare(&target, "rejected")?;
        let mut msg = frame(&next, RequestStatus::Open, &[]);
        let mut envelope: ReplicationWriteRequest = msg.parse_header()?;
        let mut header = BlockWriteRequest::decode(envelope.header.as_slice())?;
        header.block_size = -1;
        envelope.header = header.encode_to_vec();
        msg = Builder::protocol(msg.protocol)
            .proto_header(envelope)
            .build();
        assert!(target.write(&msg).is_err());
        reconcile(&target, "rejected", false)?;
        assert!(target.store.get_block(1)?.is_final());
        Ok(())
    }

    #[test]
    fn expiry_reaps_in_bounded_batches_and_does_not_reap_a_successor() -> CommonResult<()> {
        let target = target()?;
        let old = prepare(&target, "expired")?;
        target.write(&frame(&old, RequestStatus::Open, &[]))?;
        target
            .deadlines
            .lock()
            .insert((Instant::now(), 1, old.clone()));
        target.reap();
        assert!(target.store.get_block(1).is_err());
        let new = prepare(&target, "new")?;
        target.deadlines.lock().insert((Instant::now(), 1, old));
        target.reap();
        copy(&target, &new)?;
        for block_id in 2..260 {
            let token = target
                .prepare(PrepareReplicationRequest {
                    block_id,
                    attempt_id: block_id.to_string(),
                    target_session_id: "session".into(),
                    lifetime_ms: 1000,
                })?
                .token;
            target
                .deadlines
                .lock()
                .insert((Instant::now(), block_id, token));
        }
        let count = || {
            target
                .attempts
                .iter()
                .map(|s| s.lock().len())
                .sum::<usize>()
        };
        assert_eq!(count(), 259);
        target.reap();
        assert_eq!(count(), 131);
        target.reap();
        assert_eq!(count(), 3);
        target.reap();
        assert_eq!(count(), 1);
        Ok(())
    }
}
