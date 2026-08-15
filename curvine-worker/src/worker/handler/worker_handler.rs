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
use crate::worker::handler::BlockHandler;
use crate::worker::replication::worker_replication_handler::WorkerReplicationHandler;
use crate::worker::task::TaskManager;
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::RpcCode;
use curvine_model::{CompatibilityPolicy, LoadTaskInfo};
use curvine_proto::*;
use curvine_rpc::handler::MessageHandler;
use curvine_rpc::message::{Builder, Message, RequestStatus, ResponseStatus};
use curvine_runtime::common::SerdeUtils;
use curvine_runtime::runtime::Runtime;
use curvine_runtime::sync::FastMutex;
use log::info;
use std::sync::Arc;

/// Connection-level peer metadata for a data connection, resolved exactly once
/// from the first data-plane open frame.
#[derive(Clone, Debug, PartialEq)]
pub enum ConnectionPeer {
    /// No data-plane open frame inspected yet.
    Unknown,
    /// The client never reported component info (legacy/unknown peer).
    Legacy,
    /// The client's structured component info, recorded from the first
    /// data-plane open frame that carried it.
    Known(ComponentInfoProto),
}

pub struct WorkerHandler {
    pub store: BlockStore,
    pub handler: FastMutex<Option<BlockHandler>>,
    pub task_manager: Arc<TaskManager>,
    pub rt: Arc<Runtime>,
    pub replication_handler: WorkerReplicationHandler,
    /// Connection-level peer metadata, resolved once from the first data-plane
    /// open frame on this connection. After resolution (Known or Legacy) the
    /// worker stops parsing headers for peer metadata, so at most one frame
    /// per connection pays the decode cost — the business handlers decode the
    /// same headers anyway. The worker only records the version here (T9);
    /// protocol negotiation on top of it happens in the client-worker
    /// compatibility layer (T10).
    pub connection_peer: FastMutex<ConnectionPeer>,
    /// Compatibility policy applied to client peers on this data connection.
    /// Built once per worker from `worker.compatibility` config; diagnose by
    /// default so old clients are never rejected without explicit
    /// configuration.
    pub compatibility_policy: CompatibilityPolicy,
}

impl MessageHandler for WorkerHandler {
    type Error = FsError;

    fn handle(&self, msg: &Message) -> FsResult<Message> {
        crate::fault_point! {
            sync,
            name: "worker.rpc.before_dispatch",
            description: "Before a Worker RPC is dispatched to its business handler",
            context: {
                "req_id" => msg.req_id(),
                "rpc_code" => msg.code() as i32,
                "request_status" => i8::from(msg.request_status()),
            },
            return_error: |fault| Err(FsError::common(fault.message)),
        }

        let code = RpcCode::from(msg.code());
        // Record the client's component info on this connection (best-effort;
        // running data frames carry no version payload and legacy clients omit
        // the field entirely). In enforce mode an incompatible peer is
        // rejected here, on the first data-plane open frame.
        self.record_peer_component_info(msg)?;

        match code {
            RpcCode::SubmitTask => self.task_submit(msg),

            RpcCode::QueryTransferTask => self.query_transfer_task(msg),

            RpcCode::CancelJob => self.cancel_job(msg),

            RpcCode::SubmitBlockReplicationJob => self.replication_handler.handle(msg),

            _ => {
                let mut handler = self.handler.lock();
                let h = self.get_handler(msg, &mut handler)?;
                let res = h.handle(msg);

                if res
                    .as_ref()
                    .is_ok_and(|msg| msg.response_status() == ResponseStatus::Success)
                    && matches!(
                        msg.request_status(),
                        RequestStatus::Cancel | RequestStatus::Complete
                    )
                {
                    let _ = handler.take();
                };

                res
            }
        }
    }
}

impl WorkerHandler {
    /// Whether the RPC code belongs to the block data plane (the only codes
    /// whose open frames carry peer metadata). Transfer/task control RPCs are
    /// excluded so they never resolve the connection peer.
    fn is_data_plane_code(code: RpcCode) -> bool {
        matches!(
            code,
            RpcCode::WriteBlock | RpcCode::ReadBlock | RpcCode::WriteBlocksBatch
        )
    }

    /// Decide the connection peer state for a frame, without mutating
    /// anything. Returns `None` when the frame does not participate in peer
    /// resolution (running/commit frames, non-data-plane RPCs); otherwise the
    /// resolved state — `Known` when the open frame carried `component_info`,
    /// `Legacy` when it did not. Pure, so the one-time resolution behavior is
    /// unit-testable without a full `WorkerHandler`.
    fn resolve_connection_peer(msg: &Message) -> Option<ConnectionPeer> {
        // Resolution happens on the data-plane open frame only: it is the
        // first frame a data connection carries and the only one guaranteed to
        // report peer metadata. Running frames carry payload data and commit
        // frames repeat metadata already seen on open, so they never resolve.
        if msg.request_status() != RequestStatus::Open
            || !Self::is_data_plane_code(RpcCode::from(msg.code()))
        {
            return None;
        }
        match Self::extract_component_info(msg) {
            Some(info) => Some(ConnectionPeer::Known(info)),
            None => Some(ConnectionPeer::Legacy),
        }
    }

    /// Extract the optional `component_info` a client attached to a data-plane
    /// request, based on the RPC code / request status pair that determines the
    /// wire message type. The client reports `component_info` on the open and
    /// commit frames only; running (data-carrying) frames use `DataHeaderProto`
    /// or `FilesBatchWriteRequest` (whose header can embed large file contents)
    /// and are deliberately skipped, so the hot path never decodes a large
    /// header just to read peer metadata.
    fn extract_component_info(msg: &Message) -> Option<ComponentInfoProto> {
        match (RpcCode::from(msg.code()), msg.request_status()) {
            (
                RpcCode::WriteBlock,
                RequestStatus::Open | RequestStatus::Complete | RequestStatus::Cancel,
            ) => msg
                .parse_header::<BlockWriteRequest>()
                .ok()
                .and_then(|req| req.component_info),
            (RpcCode::ReadBlock, RequestStatus::Open | RequestStatus::Complete) => msg
                .parse_header::<BlockReadRequest>()
                .ok()
                .and_then(|req| req.component_info),
            (RpcCode::WriteBlocksBatch, RequestStatus::Open) => msg
                .parse_header::<BlocksBatchWriteRequest>()
                .ok()
                .and_then(|req| req.component_info),
            (RpcCode::WriteBlocksBatch, RequestStatus::Complete | RequestStatus::Cancel) => msg
                .parse_header::<BlocksBatchCommitRequest>()
                .ok()
                .and_then(|req| req.component_info),
            _ => None,
        }
    }

    /// Resolve and record the client's `component_info` for this connection,
    /// then evaluate the resolved peer against this worker's compatibility
    /// policy. Resolution happens exactly once, from the first data-plane open
    /// frame: a new client carries `component_info` on it (`Known`), a legacy
    /// client does not (`Legacy`). After resolution the worker never parses
    /// another header for peer metadata, so at most one frame per connection
    /// pays the decode cost (the business handlers decode the same headers
    /// anyway).
    ///
    /// Compatibility handling (T10):
    /// - `diagnose` (default): record a warning for incompatible peers and
    ///   allow the request, so old clients are never rejected without explicit
    ///   configuration;
    /// - `enforce`: reject the first data-plane open frame with an explicit
    ///   error. The evaluation is skipped entirely when the policy cannot be
    ///   acted upon (default diagnose with no bounds/blocklist and a legacy
    ///   peer that reported nothing), mirroring the master's hot-path
    ///   avoidance.
    fn record_peer_component_info(&self, msg: &Message) -> FsResult<()> {
        let mut peer = self.connection_peer.lock();
        if !matches!(*peer, ConnectionPeer::Unknown) {
            return Ok(());
        }
        let Some(resolved) = Self::resolve_connection_peer(msg) else {
            return Ok(());
        };
        // Evaluate the resolved peer (Known or Legacy) against the policy
        // BEFORE committing it to the connection. Diagnose mode logs a warning
        // for incompatible peers and allows the request; enforce mode rejects
        // the first open frame. Evaluating first keeps the rejection airtight:
        // an enforce-rejected peer is never committed, so a retried frame is
        // resolved and evaluated again and stays rejected. The evaluation is
        // skipped when it could not change the outcome (default diagnose, no
        // bounds/blocklist, legacy peer with no component info) to avoid log
        // spam on every legacy connection.
        let peer_is_known = matches!(&resolved, ConnectionPeer::Known(_));
        if self.compatibility_policy.should_evaluate(peer_is_known) {
            Self::evaluate_peer(&self.compatibility_policy, &resolved)?;
        }
        match resolved {
            ConnectionPeer::Known(info) => {
                info!(
                    "peer component info on data connection: component={}, release_version={}, protocol_version={:?}, min_protocol_version={:?}, capabilities={:?}",
                    info.component.as_deref().unwrap_or("unknown"),
                    info.release_version.as_deref().unwrap_or("unknown"),
                    info.protocol_version,
                    info.min_protocol_version,
                    info.capabilities,
                );
                *peer = ConnectionPeer::Known(info);
            }
            ConnectionPeer::Legacy => *peer = ConnectionPeer::Legacy,
            ConnectionPeer::Unknown => unreachable!("resolution always yields Known or Legacy"),
        }
        Ok(())
    }

    /// Evaluate a resolved connection peer against the worker's compatibility
    /// policy. `diagnose` (default) logs a warning and allows the request;
    /// `enforce` rejects with an explicit error describing the actual peer
    /// version, the expected bound and the upgrade suggestion. Pure, so the
    /// diagnose/enforce decision is unit-testable without a full
    /// `WorkerHandler`. `Unknown` peers never evaluate.
    fn evaluate_peer(policy: &CompatibilityPolicy, peer: &ConnectionPeer) -> FsResult<()> {
        let info = match peer {
            ConnectionPeer::Known(info) => Some(info),
            ConnectionPeer::Legacy => None,
            ConnectionPeer::Unknown => return Ok(()),
        };
        let verdict = policy.check_client(info);
        if verdict.is_compatible() {
            return Ok(());
        }
        if verdict.rejects(policy.mode) {
            return err_box!(
                "client rejected by compatibility policy: {}; upgrade the client or set worker.compatibility.mode = \"diagnose\" to allow it",
                verdict.describe()
            );
        }
        log::warn!(
            "client peer on data connection incompatible (diagnose): {}",
            verdict.describe()
        );
        Ok(())
    }

    /// The component info recorded for the peer on this connection. `None`
    /// means the peer is unresolved or a legacy client that never reported
    /// component info on any request.
    pub fn peer_component_info(&self) -> Option<ComponentInfoProto> {
        match &*self.connection_peer.lock() {
            ConnectionPeer::Known(info) => Some(info.clone()),
            ConnectionPeer::Unknown | ConnectionPeer::Legacy => None,
        }
    }

    /// Capabilities declared by the peer on this connection, empty for
    /// unresolved or legacy clients. Feature-level negotiation token: a
    /// feature is enabled only when both the worker and the peer declare it.
    pub fn peer_capabilities(&self) -> Vec<String> {
        match &*self.connection_peer.lock() {
            ConnectionPeer::Known(info) => info.capabilities.clone(),
            ConnectionPeer::Unknown | ConnectionPeer::Legacy => Vec::new(),
        }
    }

    /// Whether the peer on this connection declared the given capability.
    pub fn peer_declares_capability(&self, feature: &str) -> bool {
        self.peer_capabilities().iter().any(|c| c == feature)
    }

    fn get_handler<'a>(
        &self,
        msg: &Message,
        handler: &'a mut Option<BlockHandler>,
    ) -> FsResult<&'a mut BlockHandler> {
        let code = RpcCode::from(msg.code());

        let need_new_handler = match msg.request_status() {
            RequestStatus::Open => true,
            _ => handler.is_none() || !Self::handler_matches_code(handler, code),
        };

        if need_new_handler {
            let _ = handler.replace(BlockHandler::new(code, self.store.clone())?);
        }

        match handler.as_mut() {
            None => err_box!("The request is not initialized"),
            Some(v) => Ok(v),
        }
    }

    // Check if the current handler type matches the request code
    fn handler_matches_code(handler: &Option<BlockHandler>, code: RpcCode) -> bool {
        matches!(
            (handler, code),
            (Some(BlockHandler::Writer(_)), RpcCode::WriteBlock)
                | (Some(BlockHandler::Reader(_)), RpcCode::ReadBlock)
                | (
                    Some(BlockHandler::BatchWriter(_)),
                    RpcCode::WriteBlocksBatch
                )
                | (Some(BlockHandler::BatchWriter(_)), RpcCode::WriteBlock)
        )
    }

    pub fn task_submit(&self, msg: &Message) -> FsResult<Message> {
        let req: SubmitTaskRequest = msg.parse_header()?;
        let task: LoadTaskInfo = SerdeUtils::deserialize(&req.task_command)?;
        let task_id = task.task_id.clone();

        let submit = self.task_manager.submit_task(task)?;
        let response = SubmitTaskResponse {
            task_id,
            accepted: Some(submit.accepted),
            reject_reason: Some(submit.reject_reason),
        };

        Ok(Builder::success(msg).proto_header(response).build())
    }

    pub fn cancel_job(&self, msg: &Message) -> FsResult<Message> {
        let req: CancelJobRequest = msg.parse_header()?;
        self.task_manager.cancel_job(req.job_id)?;
        Ok(msg.success())
    }

    pub fn query_transfer_task(&self, msg: &Message) -> FsResult<Message> {
        let req: QueryTransferTaskRequest = msg.parse_header()?;
        let progress = self.task_manager.query_transfer_task(
            &req.job_id,
            &req.task_id,
            req.run_id,
            req.attempt_id,
            &req.worker_session_id,
        )?;

        let response = match progress {
            Some(progress) => QueryTransferTaskResponse {
                found: true,
                state: transfer_task_state_code(progress.state),
                progress: TransferProgressProto {
                    loaded_size: progress.loaded_size,
                    total_size: progress.total_size,
                    update_time: progress.update_time,
                    message: progress.message,
                },
            },
            None => QueryTransferTaskResponse {
                found: false,
                state: TransferTaskStateProto::TransferTaskFailed.into(),
                progress: TransferProgressProto {
                    loaded_size: 0,
                    total_size: 0,
                    update_time: 0,
                    message: String::new(),
                },
            },
        };
        Ok(Builder::success(msg).proto_header(response).build())
    }
}

fn transfer_task_state_code(state: curvine_model::JobTaskState) -> i32 {
    match state {
        curvine_model::JobTaskState::Pending | curvine_model::JobTaskState::UNKNOWN => {
            TransferTaskStateProto::TransferTaskPending.into()
        }
        curvine_model::JobTaskState::Loading => TransferTaskStateProto::TransferTaskRunning.into(),
        curvine_model::JobTaskState::Completed => {
            TransferTaskStateProto::TransferTaskCompleted.into()
        }
        curvine_model::JobTaskState::Failed => TransferTaskStateProto::TransferTaskFailed.into(),
        curvine_model::JobTaskState::Canceled => {
            TransferTaskStateProto::TransferTaskCanceled.into()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockWriteRequest, ConnectionPeer, WorkerHandler};
    use curvine_fs_api::RpcCode;
    use curvine_proto::{
        BlockReadRequest, BlocksBatchCommitRequest, BlocksBatchWriteRequest, ComponentInfoProto,
        DataHeaderProto, ExtendedBlockProto, FileTypeProto, FileWriteData, FilesBatchWriteRequest,
        QueryTransferTaskRequest, StorageTypeProto,
    };
    use curvine_rpc::message::{Builder, RequestStatus};

    fn sample_component_info() -> ComponentInfoProto {
        ComponentInfoProto {
            component: Some("client".to_string()),
            release_version: Some("0.4.0-alpha".to_string()),
            git_commit: Some("359fce7d982a15f09c3b4e0b2e62fee4229609dd".to_string()),
            git_tag: Some("v0.4.0-alpha".to_string()),
            git_branch: Some("main".to_string()),
            protocol_version: Some(1),
            min_protocol_version: Some(1),
            capabilities: vec!["short-circuit".to_string(), "batch-write".to_string()],
        }
    }

    fn sample_block() -> ExtendedBlockProto {
        ExtendedBlockProto {
            id: 42,
            block_size: 4 * 1024 * 1024,
            storage_type: StorageTypeProto::Mem as i32,
            file_type: FileTypeProto::File as i32,
            alloc_opts: None,
        }
    }

    fn build_msg(
        code: RpcCode,
        status: RequestStatus,
        header: impl prost::Message,
    ) -> curvine_rpc::message::Message {
        Builder::new()
            .code(code)
            .request(status)
            .req_id(1)
            .seq_id(1)
            .proto_header(header)
            .build()
    }

    #[test]
    fn extracts_component_info_from_block_write_open_and_commit() {
        let header = BlockWriteRequest {
            block: sample_block(),
            off: 0,
            block_size: 4 * 1024 * 1024,
            short_circuit: true,
            client_name: "client-1".to_string(),
            chunk_size: 1 << 20,
            pipeline_stream: vec![],
            component_info: Some(sample_component_info()),
        };

        let open = build_msg(RpcCode::WriteBlock, RequestStatus::Open, header.clone());
        assert_eq!(
            WorkerHandler::extract_component_info(&open),
            Some(sample_component_info())
        );

        let commit = build_msg(RpcCode::WriteBlock, RequestStatus::Complete, header);
        assert_eq!(
            WorkerHandler::extract_component_info(&commit),
            Some(sample_component_info())
        );
    }

    #[test]
    fn extracts_component_info_from_block_read_open() {
        let header = BlockReadRequest {
            id: 42,
            off: 0,
            len: 4096,
            chunk_size: 1 << 20,
            short_circuit: false,
            enable_read_ahead: true,
            read_ahead_len: 4 * 1024 * 1024,
            drop_cache_len: 1 << 20,
            component_info: Some(sample_component_info()),
        };
        let open = build_msg(RpcCode::ReadBlock, RequestStatus::Open, header);
        assert_eq!(
            WorkerHandler::extract_component_info(&open),
            Some(sample_component_info())
        );
    }

    #[test]
    fn extracts_component_info_from_batch_frames() {
        let batch_open = BlocksBatchWriteRequest {
            blocks: vec![sample_block()],
            off: 0,
            block_size: 4 * 1024 * 1024,
            req_id: 7,
            seq_id: 1,
            chunk_size: 1 << 20,
            short_circuit: true,
            client_name: "client-1".to_string(),
            component_info: Some(sample_component_info()),
        };
        let open = build_msg(RpcCode::WriteBlocksBatch, RequestStatus::Open, batch_open);
        assert_eq!(
            WorkerHandler::extract_component_info(&open),
            Some(sample_component_info())
        );

        // Running frames carry file contents in the header and are never
        // parsed for peer metadata (peer cache is filled by open/commit
        // frames); the client omits component_info here, so extraction must
        // be None.
        let files_batch = FilesBatchWriteRequest {
            files: vec![FileWriteData {
                path: "/dir/a".to_string(),
                content: b"hello".to_vec(),
            }],
            req_id: 7,
            seq_id: 2,
            component_info: None,
        };
        let running = build_msg(
            RpcCode::WriteBlocksBatch,
            RequestStatus::Running,
            files_batch,
        );
        assert_eq!(WorkerHandler::extract_component_info(&running), None);

        let batch_commit = BlocksBatchCommitRequest {
            blocks: vec![sample_block()],
            off: 0,
            block_size: 4 * 1024 * 1024,
            req_id: 7,
            seq_id: 1,
            cancel: false,
            component_info: Some(sample_component_info()),
        };
        let complete = build_msg(
            RpcCode::WriteBlocksBatch,
            RequestStatus::Complete,
            batch_commit,
        );
        assert_eq!(
            WorkerHandler::extract_component_info(&complete),
            Some(sample_component_info())
        );
    }

    #[test]
    fn running_data_frames_carry_no_component_info() {
        // Running frames use DataHeaderProto (write/read data or batch flush)
        // and carry no component info; extraction must be None so the worker
        // does not spam the connection cache on the hot path.
        let header = DataHeaderProto {
            offset: 0,
            flush: false,
            is_last: false,
        };
        let running = build_msg(RpcCode::WriteBlock, RequestStatus::Running, header);
        assert_eq!(WorkerHandler::extract_component_info(&running), None);
    }

    #[test]
    fn legacy_client_without_component_info_extracts_none() {
        // Old client: no component_info on the reserved 1000+ range. The
        // worker records nothing and treats the peer as legacy/unknown.
        let header = BlockWriteRequest {
            block: sample_block(),
            off: 0,
            block_size: 4 * 1024 * 1024,
            short_circuit: true,
            client_name: "legacy-client".to_string(),
            chunk_size: 1 << 20,
            pipeline_stream: vec![],
            component_info: None,
        };
        let open = build_msg(RpcCode::WriteBlock, RequestStatus::Open, header);
        assert_eq!(WorkerHandler::extract_component_info(&open), None);
    }

    #[test]
    fn non_data_plane_messages_extract_none() {
        // Task / transfer control messages are outside the data plane; they
        // must not touch the connection peer cache.
        let header = QueryTransferTaskRequest {
            job_id: "job-1".to_string(),
            task_id: "task-1".to_string(),
            run_id: 0,
            attempt_id: 0,
            worker_session_id: String::new(),
        };
        let msg = build_msg(RpcCode::QueryTransferTask, RequestStatus::Open, header);
        assert_eq!(WorkerHandler::extract_component_info(&msg), None);
    }

    #[test]
    fn resolves_peer_from_data_plane_open_frames() {
        // New client: the first write-block open frame carries component_info
        // and resolves the connection peer to Known.
        let header = BlockWriteRequest {
            block: sample_block(),
            off: 0,
            block_size: 4 * 1024 * 1024,
            short_circuit: true,
            client_name: "client-1".to_string(),
            chunk_size: 1 << 20,
            pipeline_stream: vec![],
            component_info: Some(sample_component_info()),
        };
        let open = build_msg(RpcCode::WriteBlock, RequestStatus::Open, header);
        assert_eq!(
            WorkerHandler::resolve_connection_peer(&open),
            Some(ConnectionPeer::Known(sample_component_info()))
        );

        // Batch open carries component_info too.
        let batch_open = BlocksBatchWriteRequest {
            blocks: vec![sample_block()],
            off: 0,
            block_size: 4 * 1024 * 1024,
            req_id: 7,
            seq_id: 1,
            chunk_size: 1 << 20,
            short_circuit: true,
            client_name: "client-1".to_string(),
            component_info: Some(sample_component_info()),
        };
        let open = build_msg(RpcCode::WriteBlocksBatch, RequestStatus::Open, batch_open);
        assert_eq!(
            WorkerHandler::resolve_connection_peer(&open),
            Some(ConnectionPeer::Known(sample_component_info()))
        );
    }

    #[test]
    fn resolves_legacy_peer_from_open_frame_without_component_info() {
        // Legacy client: the first write-block open frame has no component_info
        // and resolves the connection peer to Legacy exactly once.
        let header = BlockWriteRequest {
            block: sample_block(),
            off: 0,
            block_size: 4 * 1024 * 1024,
            short_circuit: true,
            client_name: "legacy-client".to_string(),
            chunk_size: 1 << 20,
            pipeline_stream: vec![],
            component_info: None,
        };
        let open = build_msg(RpcCode::WriteBlock, RequestStatus::Open, header);
        assert_eq!(
            WorkerHandler::resolve_connection_peer(&open),
            Some(ConnectionPeer::Legacy)
        );
    }

    #[test]
    fn running_and_commit_frames_never_resolve_peer() {
        // Running frames (DataHeaderProto / FilesBatchWriteRequest) and commit
        // frames carry no peer metadata and must not resolve the connection,
        // so the worker never decodes payload-bearing headers for peer info.
        let running = build_msg(
            RpcCode::WriteBlock,
            RequestStatus::Running,
            DataHeaderProto {
                offset: 0,
                flush: false,
                is_last: false,
            },
        );
        assert_eq!(WorkerHandler::resolve_connection_peer(&running), None);

        let files_batch = FilesBatchWriteRequest {
            files: vec![FileWriteData {
                path: "/dir/a".to_string(),
                content: b"hello".to_vec(),
            }],
            req_id: 7,
            seq_id: 2,
            component_info: None,
        };
        let running = build_msg(
            RpcCode::WriteBlocksBatch,
            RequestStatus::Running,
            files_batch,
        );
        assert_eq!(WorkerHandler::resolve_connection_peer(&running), None);

        let commit = build_msg(
            RpcCode::WriteBlock,
            RequestStatus::Complete,
            BlockWriteRequest {
                block: sample_block(),
                off: 0,
                block_size: 4 * 1024 * 1024,
                short_circuit: false,
                client_name: "client-1".to_string(),
                chunk_size: 1 << 20,
                pipeline_stream: vec![],
                component_info: None,
            },
        );
        assert_eq!(WorkerHandler::resolve_connection_peer(&commit), None);
    }

    #[test]
    fn non_data_plane_rpcs_never_resolve_peer() {
        // Task / transfer control RPCs are outside the data plane and must not
        // touch the connection peer resolution.
        let header = QueryTransferTaskRequest {
            job_id: "job-1".to_string(),
            task_id: "task-1".to_string(),
            run_id: 0,
            attempt_id: 0,
            worker_session_id: String::new(),
        };
        let msg = build_msg(RpcCode::QueryTransferTask, RequestStatus::Open, header);
        assert_eq!(WorkerHandler::resolve_connection_peer(&msg), None);
    }

    // ---- T10: per-connection peer compatibility evaluation ----

    use curvine_model::{CompatibilityMode, CompatibilityPolicy};
    use curvine_sys::version::ReleaseVersion;

    fn version(v: &str) -> ReleaseVersion {
        ReleaseVersion::parse(v).unwrap()
    }

    fn client_info(release_version: &str, protocol_version: u32) -> ComponentInfoProto {
        ComponentInfoProto {
            component: Some("client".to_string()),
            release_version: Some(release_version.to_string()),
            git_commit: Some("abc".to_string()),
            git_tag: Some(String::new()),
            git_branch: Some("main".to_string()),
            protocol_version: Some(protocol_version),
            min_protocol_version: Some(protocol_version),
            capabilities: vec!["batch-write".to_string()],
        }
    }

    fn default_policy() -> CompatibilityPolicy {
        CompatibilityPolicy::default()
    }

    #[test]
    fn compatible_peer_is_allowed_under_default_policy() {
        // Default worker policy (diagnose, no bounds): a new client that
        // reports a compatible protocol version is accepted in both modes.
        let policy = default_policy();
        let peer = ConnectionPeer::Known(client_info("0.4.0-alpha", 1));
        assert!(WorkerHandler::evaluate_peer(&policy, &peer).is_ok());
        // Even under enforce, the default policy carries no bounds, so nothing
        // is rejected.
        let enforce = CompatibilityPolicy {
            mode: CompatibilityMode::Enforce,
            ..default_policy()
        };
        assert!(WorkerHandler::evaluate_peer(&enforce, &peer).is_ok());
    }

    #[test]
    fn protocol_mismatch_is_warned_in_diagnose_and_rejected_in_enforce() {
        // 协议不匹配: a client speaking protocol 2 against a worker that only
        // supports protocol 1.
        let policy = default_policy();
        let peer = ConnectionPeer::Known(client_info("0.4.0", 2));
        // diagnose: allowed (only a warning is logged).
        assert!(WorkerHandler::evaluate_peer(&policy, &peer).is_ok());
        // enforce: rejected with an explicit error describing the mismatch.
        let enforce = CompatibilityPolicy {
            mode: CompatibilityMode::Enforce,
            ..default_policy()
        };
        let err = WorkerHandler::evaluate_peer(&enforce, &peer).unwrap_err();
        assert!(err.to_string().contains("protocol version 2"));
        assert!(err.to_string().contains("diagnose"));
    }

    #[test]
    fn old_client_version_is_warned_in_diagnose_and_rejected_in_enforce() {
        // 旧 client: an explicitly configured min_client_version rejects an
        // older client only in enforce mode; diagnose allows it with a
        // warning.
        let policy = CompatibilityPolicy {
            mode: CompatibilityMode::Diagnose,
            min_client_version: Some(version("0.2.0")),
            ..default_policy()
        };
        let old_client = ConnectionPeer::Known(client_info("0.1.5", 1));
        assert!(WorkerHandler::evaluate_peer(&policy, &old_client).is_ok());

        let enforce = CompatibilityPolicy {
            mode: CompatibilityMode::Enforce,
            min_client_version: Some(version("0.2.0")),
            ..default_policy()
        };
        let err = WorkerHandler::evaluate_peer(&enforce, &old_client).unwrap_err();
        assert!(err.to_string().contains("0.1.5"));
        assert!(err.to_string().contains("0.2.0"));
    }

    #[test]
    fn legacy_peer_is_allowed_in_diagnose_and_rejected_in_enforce() {
        // 新 worker + 旧 client: a legacy client that never reported
        // component_info is allowed under diagnose (the default) and rejected
        // only when the worker is explicitly configured to enforce.
        let policy = default_policy();
        assert!(WorkerHandler::evaluate_peer(&policy, &ConnectionPeer::Legacy).is_ok());

        let enforce = CompatibilityPolicy {
            mode: CompatibilityMode::Enforce,
            ..default_policy()
        };
        let err = WorkerHandler::evaluate_peer(&enforce, &ConnectionPeer::Legacy).unwrap_err();
        assert!(err.to_string().contains("no component version info"));
    }

    #[test]
    fn blocked_version_is_always_rejected_regardless_of_mode() {
        // Blocked versions are an explicit operator backstop: rejected even in
        // diagnose mode.
        let policy = CompatibilityPolicy {
            blocked_versions: vec![version("0.2.5")],
            ..default_policy()
        };
        let blocked = ConnectionPeer::Known(client_info("0.2.5", 1));
        let err = WorkerHandler::evaluate_peer(&policy, &blocked).unwrap_err();
        assert!(err.to_string().contains("blocked"));

        let enforce = CompatibilityPolicy {
            mode: CompatibilityMode::Enforce,
            blocked_versions: vec![version("0.2.5")],
            ..default_policy()
        };
        assert!(WorkerHandler::evaluate_peer(&enforce, &blocked).is_err());
    }

    #[test]
    fn unknown_peer_never_evaluates() {
        let policy = default_policy();
        assert!(WorkerHandler::evaluate_peer(&policy, &ConnectionPeer::Unknown).is_ok());
    }
}
