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
use curvine_model::ExtendedBlock;
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
            // A timed-out write that failed to quarantine leaves its only evidence on
            // the file's sticky flag: read it before the handle is dropped.
            let uncertain = writer
                .pending_abort
                .as_ref()
                .is_some_and(|pending| pending.uncertain)
                || writer
                    .file
                    .as_ref()
                    .is_some_and(|f| f.has_uncertain_writes());
            // Close raw-device handles BEFORE releasing their allocations or lease.
            writer.file.take();
            if !writer.is_commit {
                let result = if uncertain {
                    attempt
                        .store
                        .abort_block_uncertain(&ExtendedBlock::with_id(block_id))
                } else {
                    attempt.store.abort_replication_write(block_id)
                };
                if let Err(e) = result {
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
                if current.writer.is_some() || current.storage_type.is_some() {
                    return err_box!("replication target still owns another attempt");
                }
                // Reconciliation can overtake a delayed Prepare. An unopened reservation
                // has no writer or completion evidence, so replace it under this same lock
                // instead of blocking repairs until expiry. Open racing this replacement
                // either wins first (and prevents replacement) or sees a stale token.
            }
            self.revoke(req.block_id, current)?;
            self.deadlines
                .lock()
                .remove(&(current.expires, req.block_id, current.token.clone()));
            attempts.remove(&req.block_id);
        }
        // Quarantined extent can't be freed or reopened: refuse before Open fails.
        if self
            .store
            .get_raw_block_opt(req.block_id)?
            .is_some_and(|meta| meta.is_quarantined())
        {
            return err_box!(
                "{}: block {} is quarantined on this worker",
                curvine_model::REPLICATION_TARGET_QUARANTINED,
                req.block_id
            );
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
    use crate::worker::block::BlockState;
    use crate::worker::storage::Dataset;
    use crate::worker::Worker;
    use curvine_config::{ClusterConf, WorkerConf};
    use curvine_core_error::CommonResult;
    use curvine_fs_api::RpcCode;
    use curvine_io::DataSlice;
    use curvine_model::{FileType, ProtoUtils, StorageType};
    use curvine_proto::DataHeaderProto;
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
    fn unopened_reservation_can_be_replaced_without_waiting_for_expiry() -> CommonResult<()> {
        let target = target()?;
        let old = prepare(&target, "old")?;
        assert_eq!(old, prepare(&target, "old")?);
        let new = prepare(&target, "new")?;
        assert_ne!(old, new);
        assert_eq!(target.deadlines.lock().len(), 1);
        for status in [
            RequestStatus::Open,
            RequestStatus::Running,
            RequestStatus::Complete,
            RequestStatus::Cancel,
        ] {
            assert!(target.write(&frame(&old, status, b"bad!")).is_err());
        }
        // Old cleanup must not revoke the replacement either.
        assert_eq!(reconcile(&target, "old", true)?, None);
        copy(&target, &new)?;
        assert!(reconcile(&target, "new", true)?.is_some());
        let (_, mut reader) = target.store.open_reader_by_id_at_stored_len(1, 0)?;
        assert_eq!(reader.read_region(false, 4)?.as_slice(), b"data");
        Ok(())
    }

    #[test]
    fn reconcile_before_late_prepare_does_not_block_successor() -> CommonResult<()> {
        let target = target()?;
        // The timed-out prepare has not arrived when reconciliation confirms absence.
        assert_eq!(reconcile(&target, "late", false)?, None);
        let late = prepare(&target, "late")?;
        let successor = prepare(&target, "successor")?;
        assert_ne!(late, successor);
        assert!(target
            .write(&frame(&late, RequestStatus::Open, &[]))
            .is_err());
        copy(&target, &successor)?;
        assert!(reconcile(&target, "successor", true)?.is_some());
        Ok(())
    }

    #[test]
    fn prepare_cannot_replace_open_writer_or_completion_evidence() -> CommonResult<()> {
        let target = target()?;
        let token = prepare(&target, "active")?;
        target.write(&frame(&token, RequestStatus::Open, &[]))?;
        assert!(prepare(&target, "successor").is_err());
        target.write(&frame(&token, RequestStatus::Running, b"data"))?;
        target.write(&frame(&token, RequestStatus::Complete, &[]))?;
        assert!(prepare(&target, "successor").is_err());
        assert!(reconcile(&target, "active", false)?.is_some());
        // Reconciliation closes the writer, but the completion evidence still owns the lease.
        assert!(target.stripe(1).lock().get(&1).unwrap().writer.is_none());
        assert!(prepare(&target, "successor").is_err());
        assert!(reconcile(&target, "active", true)?.is_some());
        let (_, mut reader) = target.store.open_reader_by_id_at_stored_len(1, 0)?;
        assert_eq!(reader.read_region(false, 4)?.as_slice(), b"data");
        Ok(())
    }

    #[test]
    fn failed_revocation_keeps_successor_fenced_until_cleanup_succeeds() -> CommonResult<()> {
        let target = target()?;
        let old = prepare(&target, "old")?;
        target.write(&frame(&old, RequestStatus::Open, &[]))?;
        let meta = target.store.get_block(1)?;
        let path = target.store.short_circuit(&meta)?.unwrap();
        // A non-empty directory makes file-backed allocation cleanup fail deterministically.
        std::fs::remove_file(&path)?;
        std::fs::create_dir(&path)?;
        std::fs::write(std::path::Path::new(&path).join("child"), b"blocked")?;
        assert!(reconcile(&target, "old", false).is_err());
        assert!(prepare(&target, "successor").is_err());
        assert!(target
            .write(&frame(&old, RequestStatus::Running, b"bad!"))
            .is_err());
        assert!(target.store.ordinary_write_lease(1).is_err());
        std::fs::remove_dir_all(&path)?;
        assert_eq!(reconcile(&target, "old", false)?, None);
        let new = prepare(&target, "successor")?;
        copy(&target, &new)?;
        assert!(reconcile(&target, "successor", true)?.is_some());
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

    /// An SPDK-backed target over a synthetic bdev; no real device needed.
    fn spdk_target() -> CommonResult<(ReplicationTarget, BlockStore)> {
        let conf = ClusterConf {
            format_worker: true,
            worker: WorkerConf {
                data_dir: vec![format!(
                    "[MEM:1KB]../testing/replication-spdk-{}",
                    Utils::uuid()
                )],
                ..WorkerConf::default()
            },
            ..ClusterConf::default()
        };
        let _worker = Worker::with_conf(conf)?;
        let dataset = curvine_storage_local::BlockDataset::new_spdk_test_dataset(1, 1 << 30)?;
        let store = BlockStore::from_dataset_for_test(dataset);
        Ok((
            ReplicationTarget::new(store.clone(), "session".into()),
            store,
        ))
    }

    fn raw_state(store: &BlockStore, id: i64) -> CommonResult<Option<BlockState>> {
        Ok(store.get_raw_block_opt(id)?.map(|m| *m.state()))
    }

    /// A stale SPDK block left at Writing is a certain failure: prepare()
    /// must free it so the successor can reopen. Quarantining it would
    /// brick the block id (open and remove both reject Quarantined).
    #[test]
    fn prepare_over_stale_spdk_writing_block_frees_it_and_successor_can_reopen() -> CommonResult<()>
    {
        let (target, store) = spdk_target()?;
        let block = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);
        store.open_block(&block)?;
        assert_eq!(raw_state(&store, 1)?, Some(BlockState::Writing));
        let available_before = store.read()?.available();

        prepare(&target, "successor")?;

        assert_eq!(
            raw_state(&store, 1)?,
            None,
            "a stale certain-failure SPDK block must be freed, not quarantined"
        );
        assert!(
            store.read()?.available() > available_before,
            "freeing must return the extent"
        );
        // The successor reopens via its own fenced store (the base store is
        // now fenced out by the new lease).
        {
            let mut attempts = target.stripe(1).lock();
            attempts.get_mut(&1).unwrap().store.open_block(&block)?;
        }
        assert_eq!(raw_state(&store, 1)?, Some(BlockState::Writing));
        Ok(())
    }

    /// Non-SPDK Writing blocks are still freed.
    #[test]
    fn prepare_still_frees_non_spdk_writing_block() -> CommonResult<()> {
        let target = target()?;
        let store = target.store.clone();
        store.open_block(&ExtendedBlock::with_mem(1, "4B")?)?;
        assert_eq!(raw_state(&store, 1)?, Some(BlockState::Writing));

        prepare(&target, "successor")?;

        assert_eq!(
            raw_state(&store, 1)?,
            None,
            "non-SPDK Writing block must still be freed"
        );
        Ok(())
    }

    /// revoke() after a failed uncertain abort must retry it, not plain-abort.
    /// The writer is injected: a synthetic SPDK dataset can't open a real bdev.
    #[test]
    fn revoke_after_failed_uncertain_abort_quarantines_instead_of_freeing() -> CommonResult<()> {
        let (target, store) = spdk_target()?;
        let token = prepare(&target, "a")?;
        let block = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);

        // Stand in for Open: allocate the block, attach a timing-out writer.
        {
            let mut attempts = target.stripe(1).lock();
            let attempt = attempts.get_mut(&1).unwrap();
            attempt.store.open_block(&block)?;
            attempt.writer = Some(WriteHandler {
                store: attempt.store.clone(),
                context: Some(crate::worker::handler::WriteContext {
                    block: block.clone(),
                    req_id: 7,
                    chunk_size: 4,
                    short_circuit: false,
                    off: 0,
                    block_size: 4096,
                    dir_id: 1,
                    dir_metrics: None,
                }),
                file: Some(crate::worker::storage::BlockWriteContext::new(
                    FailingFlushUncertainBlockIO,
                    0,
                    4096,
                    0,
                )?),
                lease: None,
                is_commit: false,
                io_slow_us: 0,
                metrics: Worker::get_metrics()?,
                client_addr: "test".into(),
                pending_abort: None,
                aborted: false,
            });
        }
        assert_eq!(raw_state(&store, 1)?, Some(BlockState::Writing));
        let available_before = store.read()?.available();

        // Fail the writer's own abort so revoke() sees a pending uncertain abort.
        store.set_force_transient_abort_failure_for_test(true);
        let err = target
            .write(&frame_for(&token, &block, RequestStatus::Complete))
            .unwrap_err();
        assert!(err.to_string().contains("flush timed out"), "{err}");

        assert_eq!(
            raw_state(&store, 1)?,
            Some(BlockState::Quarantined),
            "revoke must retry the uncertain abort and quarantine, not free"
        );
        assert_eq!(
            store.read()?.available(),
            available_before,
            "quarantine must keep the extent reserved"
        );
        Ok(())
    }

    /// A timed-out Running write parks no PendingAbort; revoke() must read
    /// the file's sticky flag before dropping it and quarantine, not free.
    #[test]
    fn revoke_reads_sticky_uncertain_flag_before_dropping_file() -> CommonResult<()> {
        let (target, store) = spdk_target()?;
        let token = prepare(&target, "a")?;
        let block = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);
        {
            let mut attempts = target.stripe(1).lock();
            let attempt = attempts.get_mut(&1).unwrap();
            attempt.store.open_block(&block)?;
            attempt.writer = Some(WriteHandler {
                store: attempt.store.clone(),
                context: Some(crate::worker::handler::WriteContext {
                    block: block.clone(),
                    req_id: 7,
                    chunk_size: 4,
                    short_circuit: false,
                    off: 0,
                    block_size: 4096,
                    dir_id: 1,
                    dir_metrics: None,
                }),
                file: Some(crate::worker::storage::BlockWriteContext::new(
                    FencedUncertainWriteBlockIO,
                    0,
                    4096,
                    0,
                )?),
                lease: None,
                is_commit: false,
                io_slow_us: 0,
                metrics: Worker::get_metrics()?,
                client_addr: "test".into(),
                pending_abort: None,
                aborted: false,
            });
        }
        let available_before = store.read()?.available();

        // write()'s own quarantine fails; revoke() must still pick the uncertain abort.
        store.set_force_transient_abort_failure_for_test(true);
        let err = target
            .write(&frame(&token, RequestStatus::Running, b"data"))
            .unwrap_err();
        assert!(err.to_string().contains("write timed out"), "{err}");

        assert_eq!(
            raw_state(&store, 1)?,
            Some(BlockState::Quarantined),
            "revoke must quarantine from the sticky flag, not plain-abort"
        );
        assert_eq!(
            store.read()?.available(),
            available_before,
            "extent must stay reserved"
        );
        Ok(())
    }

    /// With its lease gone, session fencing rejects the writer's store; the
    /// uncertain quarantine of its own SPDK extent must still succeed.
    #[test]
    fn uncertain_quarantine_succeeds_even_when_fencing_rejects() -> CommonResult<()> {
        let (_, store) = spdk_target()?;
        let block = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);
        store.open_block(&block)?;

        // Dropping the lease makes check_replication_owner reject this store.
        let (fenced, lease) = store.replication_write_lease(1, "stale-token")?;
        drop(lease);
        assert!(
            fenced.abort_replication_write(1).is_err(),
            "sanity: plain fenced abort must be rejected in this window"
        );

        fenced.abort_block_uncertain(&block)?;

        assert_eq!(
            raw_state(&store, 1)?,
            Some(BlockState::Quarantined),
            "uncertain quarantine must not be blocked by session fencing"
        );
        Ok(())
    }

    /// The non-SPDK fallback of an uncertain abort deletes the block, so a
    /// fenced-out store must be rejected: another session's pending
    /// allocation stays intact.
    #[test]
    fn fenced_uncertain_abort_on_disk_block_is_rejected() -> CommonResult<()> {
        let target = target()?;
        let store = target.store.clone();
        let block = ExtendedBlock::with_mem(1, "4B")?;

        // A stale session whose lease is gone, then the live owner opens the block.
        let (stale, lease) = store.replication_write_lease(1, "stale-token")?;
        drop(lease);
        let (owner, _owner_lease) = store.replication_write_lease(1, "owner-token")?;
        owner.open_block(&block)?;
        assert_eq!(raw_state(&store, 1)?, Some(BlockState::Writing));

        let err = stale.abort_block_uncertain(&block).unwrap_err();
        assert!(
            err.to_string().contains("fenced by a replication writer"),
            "{err}"
        );
        assert_eq!(
            raw_state(&store, 1)?,
            Some(BlockState::Writing),
            "the owner's pending block must not be deleted by a stale session"
        );
        Ok(())
    }

    /// A replication write times out after its lease is gone: write_region
    /// fails and the sticky uncertain flag is set.
    struct FencedUncertainWriteBlockIO;

    impl curvine_io::BlockIO for FencedUncertainWriteBlockIO {
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
            "fenced-uncertain-write"
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

    /// End to end on the real route: a replication writer whose lease has
    /// already been revoked hits a timed-out write. write() must still
    /// quarantine through the fenced store, and the successor's prepare()
    /// must then see Quarantined (never free it) and be refused open_block.
    #[test]
    fn fenced_replication_writer_quarantines_uncertain_write_and_successor_cannot_reuse(
    ) -> CommonResult<()> {
        let (target, store) = spdk_target()?;
        let block = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);

        // Writer A opens under its replication lease, then loses it (revoke/expiry).
        let (fenced, lease) = store.replication_write_lease(1, "stale-token")?;
        fenced.open_block(&block)?;
        drop(lease);
        let available = store.read()?.available();

        let mut writer_a = WriteHandler {
            store: fenced.clone(),
            context: Some(crate::worker::handler::WriteContext {
                block: block.clone(),
                req_id: 1,
                chunk_size: 1024,
                short_circuit: false,
                off: 0,
                block_size: block.len,
                dir_id: 0,
                dir_metrics: None,
            }),
            file: Some(crate::worker::storage::BlockWriteContext::new(
                FencedUncertainWriteBlockIO,
                0,
                block.len,
                0,
            )?),
            lease: None,
            is_commit: false,
            io_slow_us: 0,
            metrics: Worker::get_metrics()?,
            client_addr: "writer-a".into(),
            pending_abort: None,
            aborted: false,
        };
        let data = Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Running)
            .req_id(1)
            .seq_id(1)
            .proto_header(DataHeaderProto {
                offset: 0,
                flush: false,
                is_last: false,
            })
            .data(DataSlice::Buffer(prost::bytes::BytesMut::from(&b"x"[..])))
            .build();
        writer_a.write(&data).unwrap_err();

        assert_eq!(
            raw_state(&store, 1)?,
            Some(BlockState::Quarantined),
            "a fenced writer must still be able to quarantine its uncertain write"
        );
        assert_eq!(
            store.read()?.available(),
            available,
            "extent stays reserved"
        );

        // Successor: prepare() refused before any lease; extent stays quarantined.
        let err = prepare(&target, "successor").unwrap_err();
        assert!(err.to_string().contains("quarantined"), "{err}");
        assert!(
            target.stripe(1).lock().get(&1).is_none(),
            "a refused prepare must leave no reservation"
        );
        assert_eq!(raw_state(&store, 1)?, Some(BlockState::Quarantined));
        assert_eq!(store.read()?.available(), available);
        Ok(())
    }

    /// prepare() on a quarantined block fails fast with no lease or reservation.
    #[test]
    fn prepare_on_quarantined_block_is_rejected() -> CommonResult<()> {
        let (target, store) = spdk_target()?;
        let block = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);
        store.open_block(&block)?;
        store.abort_block_uncertain(&block)?;
        assert_eq!(raw_state(&store, 1)?, Some(BlockState::Quarantined));

        let err = prepare(&target, "a").unwrap_err();
        assert!(
            err.to_string()
                .contains(curvine_model::REPLICATION_TARGET_QUARANTINED),
            "{err}"
        );
        assert!(target.stripe(1).lock().get(&1).is_none());
        assert_eq!(raw_state(&store, 1)?, Some(BlockState::Quarantined));
        Ok(())
    }

    /// A routine Cancel on a certain-failure SPDK block must free it.
    #[test]
    fn cancel_on_certain_spdk_writing_block_frees_it() -> CommonResult<()> {
        let (target, store) = spdk_target()?;
        let token = prepare(&target, "a")?;
        let block = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);
        {
            let mut attempts = target.stripe(1).lock();
            let attempt = attempts.get_mut(&1).unwrap();
            attempt.store.open_block(&block)?;
            attempt.writer = Some(WriteHandler {
                store: attempt.store.clone(),
                context: None,
                file: None,
                lease: None,
                is_commit: false,
                io_slow_us: 0,
                metrics: Worker::get_metrics()?,
                client_addr: "test".into(),
                pending_abort: None,
                aborted: false,
            });
        }
        assert_eq!(raw_state(&store, 1)?, Some(BlockState::Writing));
        let available_before = store.read()?.available();

        target.write(&frame_for(&token, &block, RequestStatus::Cancel))?;

        assert_eq!(
            raw_state(&store, 1)?,
            None,
            "a certain-failure SPDK block must be freed on Cancel"
        );
        assert!(
            store.read()?.available() > available_before,
            "Cancel must return the extent"
        );
        Ok(())
    }

    /// Like `frame`, but for an explicit block (`frame` hardcodes a Mem block).
    fn frame_for(token: &str, block: &ExtendedBlock, status: RequestStatus) -> Message {
        let header = BlockWriteRequest {
            block: ProtoUtils::extend_block_to_pb(block.clone()),
            off: 0,
            block_size: block.len,
            short_circuit: false,
            client_name: "test".into(),
            chunk_size: 4,
            pipeline_stream: Vec::new(),
            component_info: None,
        }
        .encode_to_vec();
        Builder::new()
            .code(RpcCode::WriteReplicationBlock)
            .req_id(7)
            .seq_id(0)
            .request(status)
            .proto_header(ReplicationWriteRequest {
                block_id: block.id,
                token: token.into(),
                header,
            })
            .data(DataSlice::Empty)
            .build()
    }

    /// Reports uncertain writes with a timed-out flush.
    struct FailingFlushUncertainBlockIO;

    impl curvine_io::BlockIO for FailingFlushUncertainBlockIO {
        fn read_region(&mut self, _: bool, _: i32) -> curvine_io::IOResult<DataSlice> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "unsupported",
            )))
        }
        fn write_region(&mut self, _: &DataSlice) -> curvine_io::IOResult<()> {
            Ok(())
        }
        fn write_all(&mut self, _: &[u8]) -> curvine_io::IOResult<()> {
            Ok(())
        }
        fn read_all(&mut self, _: &mut [u8]) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "unsupported",
            )))
        }
        fn flush(&mut self) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "flush timed out",
            )))
        }
        fn has_uncertain_writes(&self) -> bool {
            true
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
        fn resize(&mut self, _: bool, _: i64, _: i64, _: i32) -> curvine_io::IOResult<()> {
            Err(curvine_io::IOError::new(std::io::Error::other(
                "unsupported",
            )))
        }
    }
}
