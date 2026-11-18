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

use crate::master::fs::state::{monotonic_ms, SessionHeartbeat};
use crate::master::fs::{
    CompleteFileOptions, DeleteMode, FsRetryCache, MasterFilesystem, OperationStatus,
};
use crate::master::job::JobHandler;
use crate::master::meta::trash::TrashCursor;
use crate::master::meta::TrashListing;
use crate::master::replication::master_replication_handler::MasterReplicationHandler;
use crate::master::replication::master_replication_manager::MasterReplicationManager;
use crate::master::MountManager;
use crate::master::{Master, MasterMetrics, RpcContext};
use curvine_config::ClusterConf;
use curvine_core_error::{err_box, err_ext};
use curvine_error::{ErrorKind, FsError, FsResult};
use curvine_fs_api::Path;
use curvine_fs_api::RpcCode;
use curvine_model::ProtoUtils;
use curvine_model::{
    CompatibilityMode, CompatibilityPolicy, CompatibilityVerdict, CreateFileOpts, DeleteBlockCmd,
    DeleteResult, FileBlockDetails, FileBlocks, FileStatus, FilesystemInfo, FreeResult,
    HeartbeatStatus, ListOptions, OpenFlags, RenameFlags, RestoreTrashResult, WorkerCommand,
    WorkerInfo,
};
use curvine_net::net::ConnState;
use curvine_proto::*;
use curvine_raft::raft::FollowerReadRefusal;
use curvine_rpc::handler::MessageHandler;
use curvine_rpc::message::{Message, FOLLOWER_READ_SEQ_ID, WORKER_MIRROR_SEQ_ID};
use curvine_runtime::runtime::{GroupExecutor, Runtime};
use dashmap::DashMap;
use prost::Message as _;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use tokio::sync::oneshot;

pub struct MasterHandler {
    pub(crate) fs: MasterFilesystem,
    pub(crate) retry_cache: Option<FsRetryCache>,
    pub(crate) metrics: &'static MasterMetrics,
    pub(crate) audit_logging_enabled: bool,
    pub(crate) conn_state: Option<ConnState>,
    pub(crate) job_handler: JobHandler,
    pub(crate) mount_manager: Arc<MountManager>,
    pub(crate) control_rpc_executor: Arc<GroupExecutor>,
    pub(crate) replication_handler: Option<MasterReplicationHandler>,
    pub(crate) actor_rt: Arc<Runtime>,
    // Master's own version + compatibility contract, built once at startup.
    // GetFilesystemInfo backs statfs and is called frequently, so we reuse
    // this instead of recomputing component_version() on every call.
    master_compatibility: ServerCompatibilityInfoProto,
    // Compatibility policy derived from the master configuration. Used to
    // evaluate worker heartbeats and client handshakes with diagnose/enforce
    // semantics (lenient diagnose by default).
    compatibility_policy: CompatibilityPolicy,
    // Last compatibility verdict warned about per peer (worker id / client
    // address). Diagnose-mode warnings are deduped so a persistently
    // incompatible or legacy peer does not spam identical warnings on every
    // heartbeat or statfs call.
    compat_warned: DashMap<String, CompatibilityVerdict>,
}

impl MasterHandler {
    fn cache_reservation_ack_timeout_ms(heartbeat_interval_ms: u64) -> u64 {
        const ACK_HEARTBEAT_INTERVALS: u64 = 5;
        WorkerInfo::CACHE_RESERVATION_ACK_TIMEOUT_MS
            .max(heartbeat_interval_ms.saturating_mul(ACK_HEARTBEAT_INTERVALS))
    }

    fn is_follower_read_opt_in(msg: &Message) -> bool {
        msg.seq_id() == FOLLOWER_READ_SEQ_ID
    }

    fn is_worker_mirror_opt_in(msg: &Message) -> bool {
        msg.seq_id() == WORKER_MIRROR_SEQ_ID
    }

    fn is_worker_mirror_rpc(msg: &Message, code: RpcCode) -> bool {
        Self::is_worker_mirror_opt_in(msg)
            && matches!(code, RpcCode::WorkerHeartbeat | RpcCode::WorkerBlockReport)
    }

    fn active_worker_mirror_rpc_rejected(active: bool, msg: &Message, code: RpcCode) -> bool {
        active && Self::is_worker_mirror_rpc(msg, code)
    }

    fn is_follower_read_candidate(code: RpcCode) -> bool {
        matches!(
            code,
            RpcCode::FileStatus
                | RpcCode::ListStatus
                | RpcCode::Exists
                | RpcCode::GetBlockLocations
                | RpcCode::ListOptions
                | RpcCode::GetMountTable
                | RpcCode::GetMountInfo
                | RpcCode::GetFilesystemInfo
                | RpcCode::GetFileBlockDetails
                | RpcCode::ListTrash
                | RpcCode::GetCvMetadataSnapshotPage
                | RpcCode::OpenFile
        )
    }

    fn follower_read_denial_reason(
        enabled: bool,
        role: curvine_raft::raft::RoleState,
        marked_follower_read: bool,
        code: RpcCode,
        open_file_flags: Option<u32>,
    ) -> Option<&'static str> {
        if !enabled {
            return Some("disabled");
        }
        if role != curvine_raft::raft::RoleState::Follower {
            return Some("not_follower");
        }
        if !marked_follower_read {
            return Some("missing_opt_in");
        }
        if code.allows_follower_read(open_file_flags) {
            None
        } else if code == RpcCode::OpenFile {
            Some("open_file_not_read_only")
        } else {
            Some("not_allowlisted")
        }
    }

    fn follower_read_refusal_label(reason: FollowerReadRefusal) -> &'static str {
        match reason {
            FollowerReadRefusal::NotFollower => "not_follower",
            FollowerReadRefusal::ReplayNotReady => "replay_not_ready",
            FollowerReadRefusal::NoLeader => "no_leader",
            FollowerReadRefusal::NoCurrentTermLeaderContact => "no_current_term_contact",
            FollowerReadRefusal::StaleLeaderContact => "stale_leader_contact",
            FollowerReadRefusal::LeaderCommitUnknown => "leader_commit_unknown",
            FollowerReadRefusal::ApplyProgressUnknown => "apply_progress_unknown",
            FollowerReadRefusal::ApplyStalled => "apply_stalled",
            FollowerReadRefusal::ReplicationStalled => "replication_stalled",
            FollowerReadRefusal::CommitLag => "commit_lag",
        }
    }

    fn record_follower_read_accepted(metrics: &MasterMetrics, code: RpcCode) {
        let code_label = format!("{:?}", code);
        metrics
            .follower_read_accepted_total
            .with_label_values(&[&code_label])
            .inc();
    }

    fn record_follower_read_refused(metrics: &MasterMetrics, code: RpcCode, reason: &str) {
        let code_label = format!("{:?}", code);
        metrics
            .follower_read_refused_total
            .with_label_values(&[&code_label, reason])
            .inc();
    }

    fn follower_read(&self, ctx: &mut RpcContext<'_>, code: RpcCode) -> FsResult<Message> {
        match code {
            RpcCode::OpenFile => self.retry_check_open_file(ctx),
            RpcCode::FileStatus => self.file_status(ctx),
            RpcCode::ListStatus => self.list_status(ctx),
            RpcCode::Exists => self.exists(ctx),
            RpcCode::GetBlockLocations => self.get_block_locations(ctx),
            RpcCode::ListOptions => self.list_options(ctx),
            RpcCode::GetMountTable => self.get_mount_table(ctx),
            RpcCode::GetMountInfo => self.get_mount_info(ctx),
            _ => Err(FsError::not_leader(
                "RPC is not allowlisted for follower reads",
            )),
        }
    }

    fn passive_worker_rpc_allowed(
        worker_state_enabled: bool,
        follower_reads_enabled: bool,
        role: curvine_raft::raft::RoleState,
        marked_worker_mirror: bool,
        code: RpcCode,
    ) -> bool {
        (worker_state_enabled || follower_reads_enabled)
            && role == curvine_raft::raft::RoleState::Follower
            && marked_worker_mirror
            && matches!(code, RpcCode::WorkerHeartbeat | RpcCode::WorkerBlockReport)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        conf: &ClusterConf,
        fs: MasterFilesystem,
        retry_cache: Option<FsRetryCache>,
        conn_state: Option<ConnState>,
        mount_manager: Arc<MountManager>,
        job_handler: JobHandler,
        control_rpc_executor: Arc<GroupExecutor>,
        replication_manager: Arc<MasterReplicationManager>,
        actor_rt: Arc<Runtime>,
        metrics: &'static MasterMetrics,
    ) -> Self {
        metrics.active_connections.inc();
        // Build the master's compatibility payload once; GetFilesystemInfo can
        // be hot (statfs) and the underlying version metadata is immutable.
        let master_version = curvine_sys::version::component_version("master");
        // The advertised contract and the enforcement policy both come from
        // the configured compatibility section; defaults are lenient diagnose
        // with no bounds, so old components are never rejected by default.
        let compatibility_policy = conf.master.compatibility.to_policy();
        let master_compatibility =
            ProtoUtils::compatibility_to_pb(&master_version, &compatibility_policy);
        Self {
            fs,
            retry_cache,
            metrics,
            audit_logging_enabled: conf.master.audit_logging_enabled,
            conn_state,
            mount_manager,
            job_handler,
            control_rpc_executor,
            replication_handler: Some(MasterReplicationHandler::new(replication_manager)),
            actor_rt,
            master_compatibility,
            compatibility_policy,
            compat_warned: DashMap::new(),
        }
    }

    pub fn get_req_cache(&self, id: i64) -> Option<OperationStatus> {
        if let Some(ref c) = self.retry_cache {
            c.get(&id)
        } else {
            None
        }
    }

    pub fn set_req_cache<T>(&self, id: i64, res: FsResult<T>) -> FsResult<T> {
        if let Some(ref c) = self.retry_cache {
            c.set_status(id, res.is_ok())
        }
        res
    }

    pub fn check_is_retry(&self, id: i64) -> FsResult<bool> {
        if let Some(ref c) = self.retry_cache {
            c.check_is_retry(id)
        } else {
            Ok(false)
        }
    }

    pub fn mkdir(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: MkdirRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let opts = ProtoUtils::mkdir_opts_from_pb(header.opts);
        let status = self.fs.mkdir_with_opts(&header.path, opts)?;
        let rep_header = MkdirResponse {
            status: ProtoUtils::file_status_to_pb(status),
            ..Default::default()
        };
        ctx.response(rep_header)
    }

    fn create_file0(
        &self,
        req_id: i64,
        path: String,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileStatus> {
        if self.check_is_retry(req_id)? {
            // HDFS retries return the results of the last calculation
            // Alluxio Retry will re-query the status.
            // The same solution as alluxio is adopted here. In fact, the hdfs solution is better, but rust requires an additional memory copy to achieve it.
            // Re-querying the file status may cause the request to be unidempotent.
            return self.fs.file_status(&path);
        }

        let res = self.fs.create_with_opts(&path, opts, flags);
        self.set_req_cache(req_id, res)
    }

    pub fn retry_check_create_file(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: CreateFileRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let opts = ProtoUtils::create_opts_from_pb(header.opts);
        let flags = OpenFlags::new(header.flags);
        let path = header.path;
        let status = self.create_file0(ctx.msg.req_id(), path.clone(), opts, flags)?;
        let first_block = match header.first_block_client {
            Some(client) if status.len == 0 => self.fs.alloc_first_block(
                &path,
                status.id,
                ProtoUtils::client_address_from_pb(client),
            ),
            _ => None,
        };

        let rep_header = CreateFileResponse {
            file_status: ProtoUtils::file_status_to_pb(status),
            first_block: first_block.map(ProtoUtils::located_block_to_pb),
        };
        ctx.response(rep_header)
    }

    pub fn retry_check_open_file(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: OpenFileRequest = ctx.parse_header()?;

        let opts = ProtoUtils::create_opts_from_pb(header.opts);
        let flags = OpenFlags::new(header.flags);
        let audit_path = format!("{}:{}", flags.access_mark(), header.path);
        ctx.set_audit(Some(audit_path), None);

        let path = header.path;
        let file_blocks = self.open_file0(ctx.msg.req_id(), path.clone(), opts, flags)?;
        // Only an open that leaves the file empty is about to write block 0.
        let wants_first_block = !flags.read_only()
            && (flags.create() || flags.truncate())
            && file_blocks.status.len == 0
            && file_blocks.block_locs.is_empty();
        let first_block = match header.first_block_client {
            Some(client) if wants_first_block => self.fs.alloc_first_block(
                &path,
                file_blocks.status.id,
                ProtoUtils::client_address_from_pb(client),
            ),
            _ => None,
        };

        let rep_header = OpenFileResponse {
            file_blocks: ProtoUtils::file_blocks_to_pb(file_blocks),
            first_block: first_block.map(ProtoUtils::located_block_to_pb),
        };
        ctx.response(rep_header)
    }

    fn open_file0(
        &self,
        req_id: i64,
        path: String,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileBlocks> {
        if flags.read_only() {
            return self.get_block_locations_for_read(&path);
        }

        if self.check_is_retry(req_id)? {
            return self.fs.get_block_locations(&path);
        }

        let res = self.fs.open_file(path, opts, flags);
        self.set_req_cache(req_id, res)
    }

    fn get_block_locations_for_read(&self, path: &str) -> FsResult<FileBlocks> {
        if self.fs.master_monitor.is_active() {
            return self.fs.get_block_locations(path);
        }

        self.fs
            .follower_get_block_locations_if_ready(path)?
            .ok_or_else(|| FsError::not_leader("Follower block locations are not ready"))
    }

    pub fn file_status(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: GetFileStatusRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let status = self
            .fs
            .file_status_by_id(header.path.as_str(), header.inode_id)?;
        let rep_header = GetFileStatusResponse {
            status: ProtoUtils::file_status_to_pb(status),
        };

        ctx.response(rep_header)
    }

    pub fn exists(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: ExistsRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let exists = self.fs.exists(&header.path)?;
        let rep_header = ExistsResponse { exists };
        ctx.response(rep_header)
    }

    pub fn delete0(&self, req_id: i64, header: DeleteRequest) -> FsResult<DeleteResult> {
        if self.check_is_retry(req_id)? {
            return Ok(DeleteResult::default());
        }

        let path = Path::from_str(&header.path)?;
        let mount_info = self.mount_manager.get_mount_info(&path)?;
        if let Some(info) = &mount_info {
            if path.path() == info.cv_path {
                return err_box!("cannot delete mount point root: {}", info.cv_path);
            }
        }

        // Mounted deletes are UFS-first on the client, so the backing object is
        // already gone when this runs and a trash entry could never be restored.
        // Keep mounted deletes permanent until trash supports UFS routing; an
        // explicit client request to skip the trash is honoured either way.
        // Mounted deletes are also never staged for a bounded purge, because the
        // staging rename would be replayed against the UFS.
        let mode = if mount_info.is_some() {
            DeleteMode::Mounted
        } else if header.skip_trash() {
            DeleteMode::SkipTrash
        } else if header.async_delete() {
            DeleteMode::AsyncPurge
        } else {
            DeleteMode::Default
        };
        let res = self
            .fs
            .delete_with_mode(&header.path, header.recursive, mode);
        self.set_req_cache(req_id, res)
    }

    pub fn retry_check_delete(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: DeleteRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let res = self.delete0(ctx.msg.req_id(), header)?;
        let trashed = res.trashed;
        let purge_pending = res.purge_pending;
        let rep_header = DeleteResponse {
            res: Some(ProtoUtils::delete_res_to_pb(res)),
            trashed: Some(trashed),
            purge_pending: Some(purge_pending),
        };
        ctx.response(rep_header)
    }

    pub fn list_trash(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: ListTrashRequest = ctx.parse_header()?;
        let cursor = header
            .page_token
            .as_deref()
            .filter(|token| !token.is_empty())
            .map(parse_trash_page_token)
            .transpose()?;
        let page_size = header
            .page_size()
            .clamp(LIST_TRASH_MIN_PAGE_SIZE, LIST_TRASH_MAX_PAGE_SIZE)
            as usize;
        let listing = self.fs.list_trash_page(cursor.as_ref(), page_size)?;
        let (entries, next_page_token) = encode_trash_page(listing, LIST_TRASH_MAX_ENCODED_BYTES);
        ctx.response(ListTrashResponse {
            entries,
            next_page_token,
        })
    }

    pub fn restore_trash0(
        &self,
        req_id: i64,
        header: RestoreTrashRequest,
    ) -> FsResult<RestoreTrashResult> {
        if self.check_is_retry(req_id)? {
            // The original attempt already moved the entry and the retry cache
            // only records the operation status, so neither the effective
            // destination nor the restored status can be rebuilt.
            return Ok(RestoreTrashResult {
                dst: header.dst.clone(),
                status: None,
            });
        }

        // Resolve the destination before mutating anything: mounted
        // destination checks need the effective path even when the client
        // relies on the recorded origin.
        let dst = self.fs.restore_dst(&header.path, header.dst.as_deref())?;
        self.check_restore_dst_mount(&dst)?;

        let restored =
            self.fs
                .restore_trash(&header.path, header.dst.as_deref(), header.overwrite())?;
        let res = RestoreTrashResult {
            dst: Some(restored.dst),
            status: Some(restored.status),
        };
        self.set_req_cache(req_id, Ok(res))
    }

    // Cache-mode reads validate against UFS and never consult the CV namespace,
    // so restoring a CV-only entry there would leave it invisible. fs-mode reads
    // fall back to the CV blocks, so they remain valid.
    fn check_restore_dst_mount(&self, dst: &str) -> FsResult<()> {
        let path = Path::from_str(dst)?;
        let Some(info) = self.mount_manager.get_mount_info(&path)? else {
            return Ok(());
        };

        if path.path() == info.cv_path {
            return err_box!(
                "cannot restore trash onto mount point root: {}",
                info.cv_path
            );
        }
        if info.is_cache_mode() {
            return err_ext!(FsError::unsupported(format!(
                "restore trash into cache_mode mount: {}",
                dst
            )));
        }

        Ok(())
    }

    pub fn retry_check_restore_trash(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: RestoreTrashRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), header.dst.clone());

        let res = self.restore_trash0(ctx.msg.req_id(), header)?;
        ctx.response(ProtoUtils::restore_trash_res_to_pb(res))
    }

    pub fn empty_trash0(&self, req_id: i64, wait: bool) -> FsResult<(u64, DeleteResult)> {
        if self.check_is_retry(req_id)? {
            return Ok((0, DeleteResult::default()));
        }

        // Both modes stage the entries; only the wait mode blocks for the
        // reclamation, and both go through the same bounded purger.
        let res = self.fs.empty_trash0(wait).map(|(accepted, res)| {
            let res = if wait { res } else { DeleteResult::default() };
            (accepted, res)
        });
        self.set_req_cache(req_id, res)
    }

    pub fn retry_check_empty_trash(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: EmptyTrashRequest = ctx.parse_header()?;

        let (accepted, res) = self.empty_trash0(ctx.msg.req_id(), header.wait())?;
        ctx.response(EmptyTrashResponse {
            res: ProtoUtils::delete_res_to_pb(res),
            accepted: Some(accepted),
        })
    }

    pub fn retry_check_free(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: FreeRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let res = self.free0(ctx.msg.req_id(), header)?;
        ctx.response(FreeResponse {
            res: ProtoUtils::free_res_to_pb(res),
        })
    }

    pub fn free0(&self, req_id: i64, header: FreeRequest) -> FsResult<FreeResult> {
        if self.check_is_retry(req_id)? {
            return Ok(FreeResult::default());
        }

        let res = self.fs.free(&header.path, header.recursive);
        self.set_req_cache(req_id, res)
    }

    pub fn rename0(&self, req_id: i64, header: RenameRequest) -> FsResult<bool> {
        if self.check_is_retry(req_id)? {
            return Ok(true);
        }
        let flags = RenameFlags::new(header.flags);
        // Trash/restore moves carry master-internal semantics (they rewrite
        // the internal xattrs). Reject forged client requests instead of
        // silently masking the bits.
        if flags.intersects(RenameFlags::internal_mask()) {
            return err_ext!(FsError::invalid_argument(
                "rename flags contain a master-internal bit"
            ));
        }
        let res = self.fs.rename(&header.src, &header.dst, flags);
        self.set_req_cache(req_id, res)
    }

    pub fn retry_check_rename(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: RenameRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.src.to_string()), Some(header.dst.to_string()));

        let result = self.rename0(ctx.msg.req_id(), header)?;
        let rep_header = RenameResponse { result };
        ctx.response(rep_header)
    }

    pub fn list_status(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: ListStatusRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let list = Self::process_list_status(self.fs.clone(), header.path)?;
        let res = list
            .into_iter()
            .map(ProtoUtils::file_status_to_pb)
            .collect();

        let rep_header = ListStatusResponse { statuses: res };
        ctx.response(rep_header)
    }

    fn process_list_status(fs: MasterFilesystem, path: String) -> FsResult<Vec<FileStatus>> {
        fs.list_status(&path)
    }

    // The add block internally determines whether it is a retry request.
    pub fn add_block(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: AddBlockRequest = ctx.parse_header()?;
        ctx.set_audit(Some(req.path.to_string()), None);

        let path = req.path;
        let client_addr = ProtoUtils::client_address_from_pb(req.client_address);
        let commit_blocks = req
            .commit_blocks
            .into_iter()
            .map(ProtoUtils::commit_block_from_pb)
            .collect();

        let last_block = req.last_block.map(ProtoUtils::extend_block_from_pb);
        let located_block = self.fs.add_block(
            path,
            req.inode_id,
            client_addr,
            commit_blocks,
            req.exclude_workers,
            req.file_len,
            last_block,
        )?;
        let rep_header = ProtoUtils::located_block_to_pb(located_block);
        ctx.response(rep_header)
    }

    // Complete_file internally determines whether it is a retry request.
    pub fn complete_file(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: CompleteFileRequest = ctx.parse_header()?;

        let audit_path = if req.only_flush {
            format!("flush:{}", req.path)
        } else {
            format!("close:{}", req.path)
        };
        ctx.set_audit(Some(audit_path), None);

        let return_file_blocks = req.return_file_blocks.unwrap_or(true);
        let file_blocks = self.complete_file0(req, return_file_blocks)?;
        let rep_header = CompleteFileResponse {
            result: true,
            file_blocks: file_blocks.map(ProtoUtils::file_blocks_to_pb),
        };
        ctx.response(rep_header)
    }

    fn complete_file0(
        &self,
        req: CompleteFileRequest,
        return_file_blocks: bool,
    ) -> FsResult<Option<FileBlocks>> {
        let commit_blocks = req
            .commit_blocks
            .into_iter()
            .map(ProtoUtils::commit_block_from_pb)
            .collect();
        // `return_file_blocks` only matters for flush; a close never returns them.
        let set_attr_opts = if req.only_flush && !return_file_blocks {
            None
        } else {
            req.set_attr_opts.map(ProtoUtils::set_attr_opts_from_pb)
        };
        self.fs.complete_file_with_opts(
            req.path,
            req.inode_id,
            req.len,
            commit_blocks,
            req.client_name,
            CompleteFileOptions {
                only_flush: req.only_flush,
                return_file_blocks,
                set_attr_opts,
                release_block_id: req.release_block_id,
            },
        )
    }

    pub fn create_files_batch(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: CreateFilesBatchRequest = ctx.parse_header()?;

        let mut results = Vec::with_capacity(header.requests.len());
        for (index, req) in header.requests.into_iter().enumerate() {
            let opts = ProtoUtils::create_opts_from_pb(req.opts);
            let flags = OpenFlags::new(req.flags);

            // Generate unique req_id for each file in batch
            let unique_req_id = ctx.msg.req_id() + index as i64;
            let status = self.create_file0(unique_req_id, req.path, opts, flags)?;
            results.push(status);
        }

        let rep_header = CreateFilesBatchResponse {
            file_statuses: results
                .into_iter()
                .map(ProtoUtils::file_status_to_pb)
                .collect(),
        };
        ctx.response(rep_header)
    }

    pub fn add_blocks_batch(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: AddBlocksBatchRequest = ctx.parse_header()?;
        let mut results = Vec::with_capacity(header.requests.len());
        for req in header.requests {
            let path = req.path;
            let client_addr = ProtoUtils::client_address_from_pb(req.client_address);
            let commit_blocks = req
                .commit_blocks
                .into_iter()
                .map(ProtoUtils::commit_block_from_pb)
                .collect();

            let last_block = req.last_block.map(ProtoUtils::extend_block_from_pb);
            let located_block = self.fs.add_block(
                path,
                req.inode_id,
                client_addr,
                commit_blocks,
                req.exclude_workers,
                req.file_len,
                last_block,
            )?;
            results.push(ProtoUtils::located_block_to_pb(located_block));
        }

        let rep_header = AddBlocksBatchResponse { blocks: results };
        ctx.response(rep_header)
    }

    pub fn complete_files_batch(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: CompleteFilesBatchRequest = ctx.parse_header()?;

        let mut results = Vec::with_capacity(header.requests.len());
        for req in header.requests {
            let result = self.complete_file0(req, false).is_ok();
            results.push(result);
        }

        let rep_header = CompleteFilesBatchResponse { results };
        ctx.response(rep_header)
    }

    pub fn get_block_locations(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: GetBlockLocationsRequest = ctx.parse_header()?;
        ctx.set_audit(Some(req.path.to_string()), None);

        let blocks = self.get_block_locations_for_read(&req.path)?;
        let rep_header = GetBlockLocationsResponse {
            blocks: ProtoUtils::file_blocks_to_pb(blocks),
        };
        ctx.response(rep_header)
    }

    pub fn get_file_block_details(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: GetFileBlockDetailsRequest = ctx.parse_header()?;
        ctx.set_audit(Some(req.path.to_string()), None);

        let details = Self::process_get_file_block_details(self.fs.clone(), req.path)?;
        ctx.response(ProtoUtils::file_block_details_to_pb(details))
    }

    fn process_get_file_block_details(
        fs: MasterFilesystem,
        path: String,
    ) -> FsResult<FileBlockDetails> {
        fs.get_file_block_details(path)
    }

    async fn run_master_rpc_task<T, F>(executor: Arc<GroupExecutor>, task: F) -> FsResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> FsResult<T> + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        executor.try_spawn(move || {
            let result = panic::catch_unwind(AssertUnwindSafe(task))
                .unwrap_or_else(|_| err_box!("master control RPC task panicked"));
            let _ = tx.send(result);
        })?;
        rx.await?
    }

    pub fn get_filesystem_info(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: GetFilesystemInfoRequest = ctx.parse_header()?;
        // GetFilesystemInfo backs statfs and is called frequently. Only
        // evaluate the compatibility policy when the result can actually be
        // acted upon (enforce mode, configured bounds/blocklist, or the client
        // reported component_info); otherwise a legacy client would hit a
        // MissingInfo verdict and log a warning on every statfs call.
        if self
            .compatibility_policy
            .should_evaluate(req.component_info.is_some())
        {
            let verdict = self
                .compatibility_policy
                .check_client(req.component_info.as_ref());
            self.metrics.record_client_compat(
                self.client_ip(),
                req.component_info
                    .as_ref()
                    .map(|info| info.release_version.as_deref().unwrap_or_default()),
                Self::verdict_label(&verdict),
            );
            Self::check_peer_compatibility(
                "client",
                &format!("client:{}", self.client_ip()),
                &self.compat_warned,
                self.compatibility_policy.mode,
                verdict,
                self.metrics,
            )?;
        }
        let info = Self::process_get_filesystem_info(self.fs.clone())?;
        let rep_header = Self::build_filesystem_info_response(info, &self.master_compatibility);
        ctx.response(rep_header)
    }

    /// Build the GetFilesystemInfo response, attaching the master's own version
    /// and the default (lenient) compatibility contract on the reserved 1000+
    /// field range. Legacy clients that do not know the field simply skip it,
    /// so this never breaks older peers. The compatibility payload is built
    /// once at handler construction and reused across requests.
    fn build_filesystem_info_response(
        info: FilesystemInfo,
        master_compatibility: &ServerCompatibilityInfoProto,
    ) -> GetFilesystemInfoResponse {
        let mut rep_header = ProtoUtils::filesystem_info_to_pb(info);
        rep_header.compatibility = Some(master_compatibility.clone());
        rep_header
    }

    async fn async_get_cv_metadata_snapshot_page(
        &self,
        ctx: &mut RpcContext<'_>,
    ) -> FsResult<Message> {
        let req: GetCvMetadataSnapshotPageRequest = ctx.parse_header()?;
        ctx.set_audit(Some("cv-metadata-snapshot".to_string()), None);
        let fs = self.fs.clone();
        let response = Self::run_master_rpc_task(self.control_rpc_executor.clone(), move || {
            let page = fs.cv_metadata_snapshot_page(
                req.page_token,
                req.page_size.unwrap_or(10_000) as usize,
            )?;
            Ok(GetCvMetadataSnapshotPageResponse {
                entries: page
                    .entries
                    .into_iter()
                    .map(|entry| CvMetadataSnapshotEntryProto {
                        status: ProtoUtils::file_status_to_pb(entry.status),
                        blocks: entry.blocks.map(ProtoUtils::file_blocks_to_pb),
                    })
                    .collect(),
                next_page_token: page.next_page_token,
                epoch: page.epoch,
            })
        })
        .await?;
        ctx.response(response)
    }

    fn process_get_filesystem_info(fs: MasterFilesystem) -> FsResult<FilesystemInfo> {
        fs.filesystem_info()
    }

    pub fn worker_heartbeat(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: WorkerHeartbeatRequest = ctx.parse_header()?;
        // Evaluate the compatibility policy only when the result can actually
        // be acted upon (enforce mode, configured bounds/blocklist, or the
        // worker reported component_info); otherwise a legacy worker would hit
        // a MissingInfo verdict and log a warning on every heartbeat.
        if self
            .compatibility_policy
            .should_evaluate(header.component_info.is_some())
        {
            Self::check_peer_compatibility(
                "worker",
                &format!("worker:{}", header.worker_id),
                &self.compat_warned,
                self.compatibility_policy.mode,
                self.compatibility_policy
                    .check_worker(header.component_info.as_ref()),
                self.metrics,
            )?;
        }
        let rep_header = Self::process_worker_heartbeat(self.fs.clone(), header)?;
        ctx.response(rep_header)
    }

    /// Record worker liveness and storage information on a follower. The
    /// passive path deliberately returns no pending block deletion commands.
    fn worker_heartbeat_passive(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: WorkerHeartbeatRequest = ctx.parse_header()?;
        if self
            .compatibility_policy
            .should_evaluate(header.component_info.is_some())
        {
            Self::check_peer_compatibility(
                "worker",
                &format!("worker:{}", header.worker_id),
                &self.compat_warned,
                self.compatibility_policy.mode,
                self.compatibility_policy
                    .check_worker(header.component_info.as_ref()),
                self.metrics,
            )?;
        }

        Self::process_worker_heartbeat_passive(self.fs.clone(), header)?;
        ctx.response(WorkerHeartbeatResponse {
            cmds: Vec::new(),
            cache_reservation_fence: None,
            registration_required: None,
            full_report_token: None,
        })
    }

    fn process_worker_heartbeat_passive(
        fs: MasterFilesystem,
        header: WorkerHeartbeatRequest,
    ) -> FsResult<()> {
        let status = HeartbeatStatus::from(header.status);
        let address = ProtoUtils::worker_address_from_pb(&header.address);
        let worker_id = address.worker_id;
        let weight = header.weight.unwrap_or_else(WorkerInfo::default_weight);
        let worker_session_id = header.worker_session_id.unwrap_or_default();
        let cluster_id = header.cluster_id;
        let validate_cluster_id = cluster_id.clone();
        let validate_address = address.clone();
        let transfer_capabilities = curvine_model::TransferWorkerCapabilities {
            task_submit: header.transfer_task_submit.unwrap_or(false),
            report_target: header.transfer_report_target.unwrap_or(false),
            query_task: header.transfer_query_task.unwrap_or(false),
            attempt_safe_output: header.transfer_attempt_safe_output.unwrap_or(false),
            source_read_plan: header.transfer_source_read_plan.unwrap_or(false),
        };
        let software_version = header.software_version;
        let startup_time_ms = u64::try_from(header.fs_ctime).unwrap_or_default();
        let storages = ProtoUtils::storage_info_list_from_pb(header.storages);
        let component_info = header.component_info;

        fs.process_passive_worker_heartbeat(
            status,
            worker_id,
            |wm| {
                if status == HeartbeatStatus::Start {
                    wm.validate_passive_start(&validate_cluster_id, &validate_address)
                } else {
                    Ok(())
                }
            },
            |wm| {
                wm.heartbeat_passive(
                    &cluster_id,
                    status,
                    address,
                    weight,
                    worker_session_id,
                    transfer_capabilities,
                    software_version,
                    startup_time_ms,
                    storages,
                    component_info,
                )
            },
        )
    }

    /// Evaluate a compatibility verdict against the configured mode.
    ///
    /// - `diagnose` (default): log a warning for non-compatible peers and
    ///   allow the request, so old components are never rejected without
    ///   explicit configuration.
    /// - `enforce`: reject with an explicit error describing the actual peer
    ///   version, the expected bound and the upgrade suggestion.
    ///
    /// Diagnose-mode warnings are deduped per peer: a persistently
    /// incompatible or legacy peer (heartbeats run every few seconds, statfs
    /// every call) warns on the first occurrence and again only when its
    /// verdict changes, so repeated identical warnings do not flood
    /// operational logs.
    fn check_peer_compatibility(
        peer: &str,
        dedup_key: &str,
        warned: &DashMap<String, CompatibilityVerdict>,
        mode: CompatibilityMode,
        verdict: CompatibilityVerdict,
        metrics: &MasterMetrics,
    ) -> FsResult<()> {
        // Record the compatibility verdict as a metric. Only one verdict
        // label per peer is active at a time, so set the current label to 1
        // and clear every other label for the peer; otherwise a peer whose
        // verdict changes over time leaves stale series stuck at 1.
        let verdict_label = Self::verdict_label(&verdict);
        if let Some(worker_id) = dedup_key.strip_prefix("worker:") {
            for label in Self::VERDICT_LABELS {
                let active = if label == verdict_label { 1 } else { 0 };
                metrics
                    .compat_worker_verdict
                    .with_label_values(&[worker_id, label])
                    .set(active);
            }
        }
        // Client verdicts are not recorded here: client addresses are not
        // bounded by cluster size, so `get_filesystem_info` aggregates them by
        // release version through `MasterMetrics::record_client_compat`.

        if !verdict.rejects(mode) {
            if !verdict.is_compatible() {
                // Warn on the first occurrence and whenever the verdict
                // changes for this peer; suppress identical repeats.
                let changed = warned
                    .get(dedup_key)
                    .map(|last| *last != verdict)
                    .unwrap_or(true);
                if changed {
                    warned.insert(dedup_key.to_string(), verdict.clone());
                    log::warn!("{} compatibility: {}", peer, verdict.describe());
                }
            } else {
                // The peer is compatible again; forget the previous warning so
                // a future incompatibility is surfaced.
                warned.remove(dedup_key);
            }
            return Ok(());
        }
        // Enforce-mode rejection: record the counter and return the error.
        metrics
            .compat_enforce_rejected_total
            .with_label_values(&[peer, verdict_label])
            .inc();
        err_box!(
            "{} rejected by compatibility policy: {}; upgrade the {} or set master.compatibility.mode = \"diagnose\" to allow it",
            peer,
            verdict.describe(),
            peer
        )
    }

    /// All verdict label values for the compat_*_verdict gauge vectors, kept
    /// in sync with [`Self::verdict_label`]. Only one label per peer is active
    /// at a time: recording a verdict sets the current label to 1 and clears
    /// every other label for that peer.
    const VERDICT_LABELS: [&str; 6] = [
        "compatible",
        "missing_info",
        "blocked",
        "protocol_mismatch",
        "version_too_old",
        "version_unknown",
    ];

    /// Short human-readable label for a compatibility verdict, used as a
    /// Prometheus label value in compat_*_verdict and compat_enforce_rejected_total.
    fn verdict_label(verdict: &CompatibilityVerdict) -> &'static str {
        match verdict {
            CompatibilityVerdict::Compatible => "compatible",
            CompatibilityVerdict::MissingInfo => "missing_info",
            CompatibilityVerdict::Blocked(_) => "blocked",
            CompatibilityVerdict::ProtocolMismatch { .. } => "protocol_mismatch",
            CompatibilityVerdict::VersionTooOld { .. } => "version_too_old",
            CompatibilityVerdict::VersionUnknown { .. } => "version_unknown",
        }
    }

    fn process_worker_heartbeat(
        fs: MasterFilesystem,
        header: WorkerHeartbeatRequest,
    ) -> FsResult<WorkerHeartbeatResponse> {
        Self::process_worker_heartbeat_with_clock(
            fs,
            header,
            curvine_runtime::common::LocalTime::mills,
        )
    }

    fn process_worker_heartbeat_with_clock(
        fs: MasterFilesystem,
        header: WorkerHeartbeatRequest,
        clock: impl FnOnce() -> u64,
    ) -> FsResult<WorkerHeartbeatResponse> {
        Self::process_worker_heartbeat_with_clocks(
            fs,
            header,
            clock,
            crate::master::fs::state::monotonic_ms,
        )
    }

    fn process_worker_heartbeat_with_clocks(
        fs: MasterFilesystem,
        header: WorkerHeartbeatRequest,
        clock: impl FnOnce() -> u64,
        inventory_clock: impl FnOnce() -> u64,
    ) -> FsResult<WorkerHeartbeatResponse> {
        let status = HeartbeatStatus::from(header.status);
        let address = ProtoUtils::worker_address_from_pb(&header.address);
        // Worker weight comes from trusted administrator configuration. Preserve the
        // configured u32 value so the master does not silently alter allocation ratios.
        let weight = header.weight.unwrap_or_else(WorkerInfo::default_weight);
        // Registration and its pre-inventory baseline are atomic with namespace
        // changes. Match the established namespace -> worker-manager lock order.
        let registration_guard = matches!(status, HeartbeatStatus::Start | HeartbeatStatus::End)
            .then(|| fs.fs_dir.write());
        let mut wm = fs.worker_manager.write();
        wm.check_cluster_id(&header.cluster_id)?;
        let worker_id = address.worker_id;
        let session = header.worker_session_id.clone().unwrap_or_default();
        let seq = header.capacity_snapshot_seq.filter(|v| *v > 0);
        if matches!(status, HeartbeatStatus::Running) {
            wm.worker_map.ensure_worker_id_addr(&address)?;
            // A delayed legacy heartbeat may omit the snapshot fields entirely.
            // It must not replace an explicitly accepted session, even before
            // that session's first Running beat (or after it was marked lost).
            if seq.is_none()
                && wm
                    .worker_map
                    .workers
                    .get(&worker_id)
                    .or_else(|| wm.worker_map.lost_workers().get(&worker_id))
                    .is_some_and(|worker| {
                        worker.registration_accepted && session != worker.worker_session_id
                    })
            {
                return Err(FsError::in_progress_msg(format!(
                    "NotRegistered: worker {worker_id} must register its current session before sending heartbeats"
                )));
            }
            // Reject before updating liveness, capacity or commands. A promoted
            // follower may still know the previous process's registration.
            if seq.is_some()
                && (session.is_empty()
                    || wm
                        .worker_map
                        .workers
                        .get(&worker_id)
                        .is_none_or(|worker| session != worker.worker_session_id))
            {
                return Ok(WorkerHeartbeatResponse {
                    registration_required: Some(true),
                    ..Default::default()
                });
            }
            if let Some(worker) = wm.worker_map.workers.get_mut(&worker_id) {
                worker.adopt_cache_reservation_term(fs.master_monitor.term());
                // Do not overwrite capacity or acknowledge a fence with a delayed
                // request. Retries are harmless; the next fresh beat resumes it.
                if worker.cache_reservation_fence.snapshot_seq > 0
                    && seq.is_none_or(|v| v <= worker.cache_reservation_fence.snapshot_seq)
                {
                    return Ok(WorkerHeartbeatResponse::default());
                }
            }
        }
        let cmds = wm.heartbeat(
            &header.cluster_id,
            status,
            address,
            weight,
            header.worker_session_id.unwrap_or_default(),
            curvine_model::TransferWorkerCapabilities {
                task_submit: header.transfer_task_submit.unwrap_or(false),
                report_target: header.transfer_report_target.unwrap_or(false),
                query_task: header.transfer_query_task.unwrap_or(false),
                attempt_safe_output: header.transfer_attempt_safe_output.unwrap_or(false),
                source_read_plan: header.transfer_source_read_plan.unwrap_or(false),
            },
            header.software_version,
            u64::try_from(header.fs_ctime).unwrap_or_default(),
            ProtoUtils::storage_info_list_from_pb(header.storages),
            header.component_info,
        )?;
        let full_report_token = if let Some(fs_dir) = registration_guard.as_ref() {
            if status == HeartbeatStatus::Start {
                let token = fs.begin_full_block_report(fs_dir, worker_id, session.clone());
                if let Some(worker) = wm.worker_map.workers.get_mut(&worker_id) {
                    fs.start_inventory_recovery(worker, token.clone());
                }
                Some(token)
            } else {
                fs_dir.store.invalidate_location_report(worker_id);
                None
            }
        } else {
            None
        };
        let mut cache_reservation_fence = None;
        if matches!(status, HeartbeatStatus::Running) && !session.is_empty() {
            if let (Some(seq), Some(worker)) = (seq, wm.worker_map.workers.get_mut(&worker_id)) {
                worker.cache_reservation_fence.snapshot_seq = seq;
                // Capacity and liveness were already refreshed. Delete commands
                // remain pending until Deleted confirmation; the worker ignores
                // commands in a recovery response, so return an empty response.
                if fs.inventory_resync_due(worker, inventory_clock()) {
                    return Ok(WorkerHeartbeatResponse {
                        registration_required: Some(true),
                        ..Default::default()
                    });
                }
                cache_reservation_fence = worker.process_cache_reservations(
                    header.cache_reservation_fence_ack.as_deref(),
                    || curvine_runtime::common::Utils::rand_str(32),
                    clock(),
                    Self::cache_reservation_ack_timeout_ms(fs.conf.heartbeat_interval_ms()),
                );
            }
        }
        if let (Ok(metrics), Some(worker)) = (
            Master::get_metrics(),
            wm.worker_map.workers.get_mut(&worker_id),
        ) {
            metrics.record_cache_reservation_events(worker);
        }
        Ok(WorkerHeartbeatResponse {
            cmds: ProtoUtils::worker_cmd_to_pb(cmds),
            cache_reservation_fence,
            registration_required: None,
            full_report_token,
        })
    }

    pub fn block_report(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: BlockReportListRequest = ctx.parse_header()?;
        let cmds =
            Self::process_block_report(self.fs.clone(), self.replication_handler.clone(), header)?;
        let rep_header = BlockReportListResponse {
            cmds: ProtoUtils::worker_cmd_to_pb(cmds),
        };
        ctx.response(rep_header)
    }

    fn block_report_passive(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: BlockReportListRequest = ctx.parse_header()?;
        let worker_session_id = header.worker_session_id.clone();
        let list = ProtoUtils::block_report_list_from_pb(header);
        self.fs
            .passive_block_report(list, worker_session_id.as_deref())?;
        ctx.response(BlockReportListResponse { cmds: Vec::new() })
    }

    fn process_block_report(
        fs: MasterFilesystem,
        replication_handler: Option<MasterReplicationHandler>,
        header: BlockReportListRequest,
    ) -> FsResult<Vec<WorkerCommand>> {
        Self::process_block_report_with_clock(
            fs,
            replication_handler,
            header,
            curvine_runtime::common::LocalTime::mills,
        )
    }

    fn process_block_report_with_clock(
        fs: MasterFilesystem,
        replication_handler: Option<MasterReplicationHandler>,
        header: BlockReportListRequest,
        clock: impl Fn() -> u64,
    ) -> FsResult<Vec<WorkerCommand>> {
        // Keep the wire session before conversion to BlockReportList drops it.
        // Legacy reports retain their existing behavior but cannot renew liveness.
        let registration = if header.full_report {
            header
                .worker_session_id
                .as_ref()
                .filter(|s| !s.is_empty())
                .map(|session| (header.cluster_id.clone(), header.worker_id, session.clone()))
        } else {
            None
        };
        let renew = || -> FsResult<()> {
            if let Some((cluster_id, worker_id, session)) = &registration {
                let now = clock();
                fs.worker_manager
                    .write()
                    .renew_full_report_liveness(cluster_id, *worker_id, session, now)?;
            }
            Ok(())
        };
        // Renew on validated contact, including retries, without making the
        // worker allocatable. Do not hold the manager lock across report I/O.
        renew()?;
        let token = header.full_report_token.clone();
        let session = header.worker_session_id.clone();
        let list = ProtoUtils::block_report_list_from_pb(header);
        let result = fs.block_report_with_token(
            list,
            session.as_deref(),
            token.as_deref(),
            replication_handler,
        )?;
        // Processing may take time or race with restart/expiration. Revalidate
        // instead of refreshing a replacement session or resurrecting an old one.
        renew()?;

        if result.delete_blocks.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(vec![WorkerCommand::DeleteBlock(DeleteBlockCmd {
                blocks: result.delete_blocks,
            })])
        }
    }

    fn client_ip(&self) -> &str {
        match &self.conn_state {
            None => "",
            Some(v) => &v.remote_addr.hostname,
        }
    }

    pub fn clone_fs(&self) -> MasterFilesystem {
        self.fs.clone()
    }

    pub fn mount(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let request: MountRequest = ctx.parse_header()?;
        let mnt_opt = ProtoUtils::mount_options_from_pb(request.mount_options);

        ctx.set_audit(
            Some(request.cv_path.to_string()),
            Some(request.ufs_path.to_string()),
        );

        self.mount_manager
            .mount(None, &request.cv_path, &request.ufs_path, &mnt_opt)?;
        let rep_header = MountResponse::default();
        ctx.response(rep_header)
    }

    fn umount(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let request: UnMountRequest = ctx.parse_header()?;
        ctx.set_audit(Some(request.cv_path.to_string()), None);

        self.mount_manager.umount(&request.cv_path)?;
        let rep_header = UnMountResponse::default();
        ctx.response(rep_header)
    }

    fn get_mount_info(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let request: GetMountInfoRequest = ctx.parse_header()?;
        ctx.set_audit(Some(request.path.to_string()), None);

        let path = Path::from_str(request.path)?;
        let ret = self.mount_manager.get_mount_info(&path)?;
        let rep_header = GetMountInfoResponse {
            mount_info: ret.map(ProtoUtils::mount_info_to_pb),
        };
        ctx.response(rep_header)
    }

    fn get_mount_table(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let _: GetMountTableRequest = ctx.parse_header()?;
        let table = self.mount_manager.get_mount_table()?;

        let mount_table: Vec<MountInfoProto> = table
            .into_iter()
            .map(ProtoUtils::mount_info_to_pb)
            .collect();
        let rep_header = GetMountTableResponse { mount_table };
        ctx.response(rep_header)
    }

    fn set_attr_retry_check(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        if self.check_is_retry(ctx.msg.req_id())? {
            return ctx.response(SetAttrResponse::default());
        }

        let header: SetAttrRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let opts = ProtoUtils::set_attr_opts_from_pb(header.opts);
        let status = self.fs.set_attr(header.path, opts)?;

        ctx.response(SetAttrResponse {
            status: ProtoUtils::file_status_to_pb(status),
        })
    }

    fn symlink_retry_check(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: SymlinkRequest = ctx.parse_header()?;
        ctx.set_audit(
            Some(header.target.to_string()),
            Some(header.link.to_string()),
        );

        if self.check_is_retry(ctx.msg.req_id())? {
            return ctx.response(SymlinkResponse::default());
        }

        self.fs.symlink_with_owner_group(
            &header.target,
            &header.link,
            header.force,
            header.mode,
            header.owner,
            header.group,
        )?;

        ctx.response(SymlinkResponse::default())
    }

    fn client_heartbeat(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: ClientHeartbeatRequest = ctx.parse_header()?;
        let rep_header = Self::process_client_heartbeat(&self.fs, header, monotonic_ms())?;
        ctx.response(rep_header)
    }

    pub(crate) fn process_client_heartbeat(
        fs: &MasterFilesystem,
        header: ClientHeartbeatRequest,
        now: u64,
    ) -> FsResult<ClientHeartbeatResponse> {
        let address = ProtoUtils::client_address_from_pb(header.client);
        if address.client_name.is_empty() {
            return Err(FsError::invalid_argument(
                "client heartbeat requires a non-empty client_name",
            ));
        }

        let hb = SessionHeartbeat {
            address,
            heartbeat_interval_ms: header.heartbeat_interval_ms,
            pid: header.pid,
            mount_point: header.mount_point,
            client_version: header.client_version,
        };

        let client_name = hb.address.client_name.clone();
        let hostname = hb.address.hostname.clone();
        let mount_point = hb.mount_point.clone();
        let interval_ms = hb.heartbeat_interval_ms;

        // A demoted master must not own sessions: the lease epoch only advances
        // on the leader, and a standby heartbeat would write a session into an
        // epoch it never adopted (#1883 review).
        if !fs.master_monitor.is_active() {
            return Err(FsError::not_leader(
                "client heartbeat requires the active master",
            ));
        }

        // Adopt the term's lease epoch and write the session under one lock, so a
        // heartbeat can never land in an epoch it did not adopt first.
        let (new_session, session_timeout_ms) = fs.heartbeat_in_epoch(hb, now);

        if new_session {
            // The lease window is the master's, not the client's: a client (or a
            // modified SDK) reporting a slower interval than the lease allows would
            // flap between alive and expired, and the reaper could release its locks
            // while it is still working. Tell the operator once, on first contact.
            if interval_ms > 0 && interval_ms.saturating_mul(5) > session_timeout_ms {
                log::warn!(
                    "client {} reports a {}ms heartbeat interval, which is outside the \
                     {}ms session lease (expected about {}ms); the master enforces its own \
                     window, so this session may expire between heartbeats",
                    client_name,
                    interval_ms,
                    session_timeout_ms,
                    session_timeout_ms / 5
                );
            } else {
                log::info!(
                    "register client session {} from {} (mount point {:?}, lease {}ms)",
                    client_name,
                    hostname,
                    mount_point,
                    session_timeout_ms
                );
            }
        }

        Ok(ClientHeartbeatResponse {
            session_timeout_ms,
            new_session,
        })
    }

    fn metrics_report(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: MetricsReportRequest = ctx.parse_header()?;

        let metrics = ProtoUtils::metrics_report_from_pb(header.metrics);
        Master::get_metrics()?.metrics_report(metrics)?;

        ctx.response(MetricsReportResponse {})
    }

    fn create_leader_snapshot(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: CreateLeaderSnapshotRequest = ctx.parse_header()?;
        let force = req.force;
        let audit = if force {
            "leader-snapshot --force"
        } else {
            "leader-snapshot"
        };
        ctx.set_audit(Some(audit.to_string()), None);

        let info = self.fs.create_leader_snapshot(force)?;

        ctx.response(CreateLeaderSnapshotResponse {
            dir: info.dir,
            cost_ms: info.cost_ms,
        })
    }

    fn link_retry_check(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: LinkRequest = ctx.parse_header()?;
        ctx.set_audit(
            Some(header.src_path.to_string()),
            Some(header.dst_path.to_string()),
        );

        if self.check_is_retry(ctx.msg.req_id())? {
            return ctx.response(LinkResponse::default());
        }

        self.fs.link(&header.src_path, &header.dst_path)?;

        ctx.response(LinkResponse::default())
    }

    pub fn resize_file(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: FileResizeRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let file_blocks = self.fs.resize_by_id(
            &header.path,
            header.inode_id,
            ProtoUtils::file_alloc_opts_from_pb(header.opts),
        )?;
        let rep_header = FileResizeResponse {
            file_blocks: ProtoUtils::file_blocks_to_pb(file_blocks),
        };
        ctx.response(rep_header)
    }

    pub fn assign_worker(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: AssignWorkerRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let block = self.fs.assign_worker_by_id(
            &header.path,
            header.inode_id,
            ProtoUtils::extend_block_from_pb(header.block),
            ProtoUtils::client_address_from_pb(header.client_address),
            header.exclude_workers,
        )?;
        let rep_header = AssignWorkerResponse {
            block: ProtoUtils::located_block_to_pb(block),
        };
        ctx.response(rep_header)
    }

    pub fn get_lock(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: GetLockRequest = ctx.parse_header()?;
        let lock = ProtoUtils::file_lock_from_pb(header.lock);
        ctx.set_audit(Some(header.path.to_string()), None);

        let conflict = self.fs.get_lock(header.path, lock)?;
        let rep_header = GetLockResponse {
            conflict: conflict.map(ProtoUtils::file_lock_to_pb),
        };
        ctx.response(rep_header)
    }

    pub fn set_lock(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: SetLockRequest = ctx.parse_header()?;
        let lock = ProtoUtils::file_lock_from_pb(header.lock);

        let audit = format!(
            "[{:?}-{:?}]{}",
            lock.lock_flags, lock.lock_type, header.path
        );
        ctx.set_audit(Some(audit), None);

        let conflict = self.fs.set_lock(header.path, lock)?;
        let rep_header = SetLockResponse {
            conflict: conflict.map(ProtoUtils::file_lock_to_pb),
        };
        ctx.response(rep_header)
    }

    pub fn list_options(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: ListOptionsRequest = ctx.parse_header()?;
        if header.options.limit.unwrap_or(0) < 0 {
            return err_box!("list options limit must be greater than 0");
        }
        let opts = ProtoUtils::list_options_from_pb(header.options);
        let audit_path = format!("{}[{}]", header.path, opts);
        ctx.set_audit(Some(audit_path), None);

        let list = Self::process_list_options(self.fs.clone(), header.path, opts)?;
        let res = list
            .into_iter()
            .map(ProtoUtils::file_status_to_pb)
            .collect();
        let rep_header = ListOptionsResponse { statuses: res };
        ctx.response(rep_header)
    }

    fn process_list_options(
        fs: MasterFilesystem,
        path: String,
        opts: ListOptions,
    ) -> FsResult<Vec<FileStatus>> {
        fs.list_options(&path, opts)
    }

    fn uses_actor_rt(code: RpcCode) -> bool {
        matches!(
            code,
            RpcCode::WorkerHeartbeat | RpcCode::WorkerBlockReport | RpcCode::ClientHeartbeat
        )
    }

    fn record_rpc_queue_duration(&self, ctx: &RpcContext<'_>) {
        if let Some(queue_us) = ctx.queue_us {
            let executor = if Self::uses_actor_rt(ctx.code) {
                "actor"
            } else {
                "main"
            };
            self.metrics
                .rpc_queue_duration
                .with_label_values(&[executor])
                .observe(queue_us as f64);
        }
    }

    fn record_rpc_observability(&self, ctx: &RpcContext<'_>, response: &FsResult<Message>) {
        let used_us = ctx.spent.used_us();
        if self.audit_logging_enabled {
            ctx.audit_log(response, used_us, self.conn_state.as_ref());
        }

        let code_label = format!("{:?}", ctx.code);
        self.metrics.rpc_request_total_time.inc_by(used_us as i64);
        self.metrics.rpc_request_total_count.inc();

        self.metrics
            .operation_duration
            .with_label_values(&[&code_label])
            .observe(used_us as f64);
    }
}

// Keep each trash page well below the 16 MiB RPC frame limit even when every
// entry carries a long original path.
const LIST_TRASH_MAX_ENCODED_BYTES: usize = 8 * 1024 * 1024;
const LIST_TRASH_MIN_PAGE_SIZE: u32 = 100;
const LIST_TRASH_MAX_PAGE_SIZE: u32 = 10_000;

fn trash_page_token(entry: &TrashEntryProto) -> String {
    format!("{}:{}", entry.delete_ms, entry.path)
}

fn parse_trash_page_token(token: &str) -> FsResult<TrashCursor> {
    let invalid = || FsError::invalid_argument(format!("invalid trash page token: {}", token));
    let (delete_ms, path) = token.split_once(':').ok_or_else(invalid)?;
    let delete_ms = delete_ms.parse::<i64>().map_err(|_| invalid())?;
    if !path.starts_with('/') {
        return Err(invalid());
    }
    Ok(TrashCursor {
        delete_ms,
        path: path.to_string(),
    })
}

// Encode one page of already-resolved entries. The last consumed entry is the
// resume key when the byte budget cuts the page short; otherwise the listing's
// scan cursor is used so a fully consumed page is not rescanned.
fn encode_trash_page(
    listing: TrashListing,
    max_encoded_bytes: usize,
) -> (Vec<TrashEntryProto>, Option<String>) {
    let mut protos = Vec::with_capacity(listing.entries.len());
    let mut encoded_bytes = 0usize;
    // Token of the last entry consumed, whether it was returned or skipped. A
    // truncated page must resume after it, never before it.
    let mut consumed: Option<String> = None;

    for entry in listing.entries {
        let proto = ProtoUtils::trash_entry_to_pb(entry);
        let size = proto.encoded_len();
        if size > max_encoded_bytes {
            // The budget has to bound every entry, including the first one: an
            // entry that cannot fit any page would make the response
            // impossible to encode, and a client retrying the same token would
            // never get past it. Skip it and advance the cursor instead.
            log::warn!(
                "skipping trash entry {} ({} bytes) that exceeds the ListTrash page budget of {} bytes",
                proto.path,
                size,
                max_encoded_bytes
            );
            consumed = Some(trash_page_token(&proto));
            continue;
        }
        if encoded_bytes + size > max_encoded_bytes {
            return (protos, consumed);
        }
        encoded_bytes += size;
        consumed = Some(trash_page_token(&proto));
        protos.push(proto);
    }

    let next = listing
        .next
        .map(|cursor| format!("{}:{}", cursor.delete_ms, cursor.path));
    (protos, next)
}

impl Drop for MasterHandler {
    fn drop(&mut self) {
        self.metrics.active_connections.dec();
    }
}

impl MessageHandler for MasterHandler {
    type Error = FsError;

    fn is_sync(&self, msg: &Message) -> bool {
        let code = RpcCode::from(msg.code());
        !matches!(
            code,
            RpcCode::SubmitJob
                | RpcCode::GetJobStatus
                | RpcCode::CancelJob
                | RpcCode::ReportTask
                | RpcCode::GetCvMetadataSnapshotPage
        )
    }

    fn handle(&self, msg: &Message) -> FsResult<Message> {
        crate::fault_point! {
            sync,
            name: "master.rpc.before_sync_dispatch",
            description: "Before a synchronous Master RPC is dispatched to its business handler",
            context: {
                "req_id" => msg.req_id(),
                "rpc_code" => msg.code() as i32,
            },
            return_error: |fault| Ok(msg.error_ext(&FsError::common(fault.message))),
        }

        let mut rpc_context = RpcContext::new(msg);
        let ctx = &mut rpc_context;
        let code = RpcCode::from(msg.code());

        self.record_rpc_queue_duration(ctx);

        // Unified processing of all RPC requests (standby NotLeader uses the same
        // observability + error_ext conversion path as async_handle).
        let mut follower_read_snapshot = None;
        let active = self.fs.master_monitor.is_active();
        let mut response = if Self::active_worker_mirror_rpc_rejected(active, msg, code) {
            Err(FsError::not_leader(
                "Worker mirror RPCs are not authoritative on an active master",
            ))
        } else if !active {
            let readiness_snapshot = self.fs.master_monitor.follower_readiness_snapshot();
            let role = readiness_snapshot.role;
            if Self::passive_worker_rpc_allowed(
                self.fs.conf.follower_worker_state_enabled,
                self.fs.conf.follower_reads_enabled,
                role,
                Self::is_worker_mirror_rpc(msg, code),
                code,
            ) {
                match code {
                    RpcCode::WorkerHeartbeat => self.worker_heartbeat_passive(ctx),
                    RpcCode::WorkerBlockReport => self.block_report_passive(ctx),
                    _ => Err(FsError::not_leader_master(ctx.code, self.client_ip())),
                }
            } else {
                let open_file_flags = if code == RpcCode::OpenFile {
                    ctx.parse_header::<OpenFileRequest>()
                        .ok()
                        .map(|request| request.flags)
                } else {
                    None
                };
                let denial = Self::follower_read_denial_reason(
                    self.fs.conf.follower_reads_enabled,
                    role,
                    Self::is_follower_read_opt_in(msg),
                    code,
                    open_file_flags,
                );
                if let Some(reason) = denial {
                    if Self::is_follower_read_candidate(code) {
                        Self::record_follower_read_refused(self.metrics, code, reason);
                    }
                    Err(FsError::not_leader_master(ctx.code, self.client_ip()))
                } else if let Some(reason) = self.fs.follower_read_refusal(&readiness_snapshot) {
                    if Self::is_follower_read_candidate(code) {
                        Self::record_follower_read_refused(
                            self.metrics,
                            code,
                            Self::follower_read_refusal_label(reason),
                        );
                    }
                    Err(FsError::not_leader_master(ctx.code, self.client_ip()))
                } else {
                    follower_read_snapshot = Some(readiness_snapshot);
                    self.follower_read(ctx, code)
                }
            }
        } else {
            match code {
                // File system operation request
                RpcCode::Mkdir => self.mkdir(ctx),
                RpcCode::CreateFile => self.retry_check_create_file(ctx),
                RpcCode::OpenFile => self.retry_check_open_file(ctx),
                RpcCode::FileStatus => self.file_status(ctx),
                RpcCode::AddBlock => self.add_block(ctx),
                RpcCode::CompleteFile => self.complete_file(ctx),
                RpcCode::CreateFilesBatch => self.create_files_batch(ctx),
                RpcCode::AddBlocksBatch => self.add_blocks_batch(ctx),
                RpcCode::CompleteFilesBatch => self.complete_files_batch(ctx),
                RpcCode::Exists => self.exists(ctx),
                RpcCode::Delete => self.retry_check_delete(ctx),
                RpcCode::ListTrash => self.list_trash(ctx),
                RpcCode::RestoreTrash => self.retry_check_restore_trash(ctx),
                RpcCode::EmptyTrash => self.retry_check_empty_trash(ctx),
                RpcCode::Free => self.retry_check_free(ctx),
                RpcCode::Rename => self.retry_check_rename(ctx),
                RpcCode::ListStatus => self.list_status(ctx),
                RpcCode::ListOptions => self.list_options(ctx),
                RpcCode::GetBlockLocations => self.get_block_locations(ctx),
                RpcCode::GetFileBlockDetails => self.get_file_block_details(ctx),
                RpcCode::GetFilesystemInfo => self.get_filesystem_info(ctx),
                RpcCode::SetAttr => self.set_attr_retry_check(ctx),
                RpcCode::Symlink => self.symlink_retry_check(ctx),
                RpcCode::Link => self.link_retry_check(ctx),
                RpcCode::ResizeFile => self.resize_file(ctx),
                RpcCode::AssignWorker => self.assign_worker(ctx),
                RpcCode::GetLock => self.get_lock(ctx),
                RpcCode::SetLock => self.set_lock(ctx),

                RpcCode::Mount => self.mount(ctx),
                RpcCode::UnMount => self.umount(ctx),
                RpcCode::GetMountTable => self.get_mount_table(ctx),
                RpcCode::GetMountInfo => self.get_mount_info(ctx),

                RpcCode::MetricsReport => self.metrics_report(ctx),
                RpcCode::ClientHeartbeat => self.client_heartbeat(ctx),
                RpcCode::CreateLeaderSnapshot => self.create_leader_snapshot(ctx),

                // Worker related requests
                RpcCode::WorkerHeartbeat => self.worker_heartbeat(ctx),
                RpcCode::WorkerBlockReport => self.block_report(ctx),

                RpcCode::ReportBlockReplicationResult => {
                    if let Some(ref replication_service) = self.replication_handler {
                        return replication_service.handle(msg);
                    } else {
                        return Err(FsError::common("Replication service not initialized"));
                    }
                }

                // Unsupported request
                _ => err_box!("Unsupported operation"),
            }
        };

        if let Some(initial_snapshot) = follower_read_snapshot {
            let final_snapshot = self.fs.master_monitor.follower_readiness_snapshot();
            let refusal = if initial_snapshot.leadership_epoch != final_snapshot.leadership_epoch {
                Some("leadership_changed")
            } else {
                self.fs
                    .follower_read_refusal(&final_snapshot)
                    .map(Self::follower_read_refusal_label)
            };
            if let Some(reason) = refusal {
                Self::record_follower_read_refused(self.metrics, code, reason);
                response = Err(FsError::not_leader(format!(
                    "Follower read refused: {reason}"
                )));
            } else if response
                .as_ref()
                .err()
                .is_some_and(|error| matches!(error.kind(), ErrorKind::NotLeaderMaster))
            {
                Self::record_follower_read_refused(self.metrics, code, "block_locations_not_ready");
            } else if response.is_ok() {
                Self::record_follower_read_accepted(self.metrics, code);
            }
        }

        self.record_rpc_observability(ctx, &response);

        match response {
            Ok(v) => Ok(v),
            Err(e) => Ok(msg.error_ext(&e)),
        }
    }

    async fn async_handle(&self, msg: Message) -> FsResult<Message> {
        crate::fault_point! {
            async,
            name: "master.rpc.before_async_dispatch",
            description: "Before an asynchronous Master RPC is dispatched to its business handler",
            context: {
                "req_id" => msg.req_id(),
                "rpc_code" => msg.code() as i32,
            },
            return_error: |fault| async {
                Ok(msg.error_ext(&FsError::common(fault.message)))
            },
        }

        let mut rpc_context = RpcContext::new(&msg);
        let ctx = &mut rpc_context;
        let code = RpcCode::from(msg.code());

        let res = if !self.fs.master_monitor.is_active() {
            if Self::is_follower_read_candidate(code) {
                Self::record_follower_read_refused(self.metrics, code, "async_rpc_not_allowlisted");
            }
            Err(FsError::not_leader_master(ctx.code, self.client_ip()))
        } else {
            match code {
                RpcCode::SubmitJob => self.job_handler.submit_job(ctx).await,
                RpcCode::GetJobStatus => self.job_handler.get_load_status(ctx),
                RpcCode::CancelJob => self.job_handler.cancel_job(ctx).await,
                RpcCode::ReportTask => self.job_handler.task_report(ctx),
                RpcCode::GetCvMetadataSnapshotPage => {
                    self.async_get_cv_metadata_snapshot_page(ctx).await
                }

                v => err_box!("unsupported operation {:?}", v),
            }
        };

        self.record_rpc_observability(ctx, &res);

        match res {
            Ok(v) => Ok(v),
            Err(e) => Ok(msg.error_ext(&e)),
        }
    }

    // Worker heartbeats, worker block reports and client heartbeats are routed to
    // actor_rt; GetFilesystemInfo (statfs) deliberately runs on the main RPC
    // blocking pool so that FUSE/kubelet statfs bursts cannot delay heartbeats.
    // Client heartbeats belong there for the same reason: a main-pool burst longer
    // than `master.client_session_timeout` would expire every client session, and
    // once lock validity is session-aware that becomes a mass lock release.
    fn get_rt(&self, msg: &Message) -> Option<&Runtime> {
        let code = RpcCode::from(msg.code());
        if Self::uses_actor_rt(code) {
            Some(&self.actor_rt)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::journal::JournalSystem;
    use curvine_model::{TrashEntry, WorkerAddress};
    use curvine_rpc::message::{Builder, WORKER_MIRROR_SEQ_ID};
    use curvine_runtime::common::Utils;

    #[test]
    fn passive_worker_rpc_gate_requires_explicit_marker_and_allowlists_codes() {
        let follower = curvine_raft::raft::RoleState::Follower;
        let marked_request = Builder::new_rpc(RpcCode::WorkerHeartbeat as i8)
            .seq_id(WORKER_MIRROR_SEQ_ID)
            .build();
        let unmarked_request = Builder::new_rpc(RpcCode::WorkerHeartbeat as i8).build();
        assert!(MasterHandler::is_worker_mirror_opt_in(&marked_request));
        assert!(!MasterHandler::is_worker_mirror_opt_in(&unmarked_request));

        assert!(!MasterHandler::passive_worker_rpc_allowed(
            false,
            false,
            follower,
            true,
            RpcCode::WorkerHeartbeat
        ));
        assert!(!MasterHandler::passive_worker_rpc_allowed(
            true,
            false,
            follower,
            false,
            RpcCode::WorkerHeartbeat
        ));
        assert!(MasterHandler::passive_worker_rpc_allowed(
            true,
            false,
            follower,
            true,
            RpcCode::WorkerHeartbeat
        ));
        assert!(MasterHandler::passive_worker_rpc_allowed(
            false,
            true,
            follower,
            true,
            RpcCode::WorkerBlockReport
        ));
        assert!(!MasterHandler::passive_worker_rpc_allowed(
            true,
            true,
            follower,
            true,
            RpcCode::Mkdir
        ));
        assert!(!MasterHandler::passive_worker_rpc_allowed(
            true,
            true,
            curvine_raft::raft::RoleState::Leader,
            true,
            RpcCode::WorkerBlockReport
        ));
    }

    #[test]
    fn active_master_rejects_marked_worker_mirror_requests() {
        let marked_heartbeat = Builder::new_rpc(RpcCode::WorkerHeartbeat as i8)
            .seq_id(WORKER_MIRROR_SEQ_ID)
            .build();
        let marked_report = Builder::new_rpc(RpcCode::WorkerBlockReport as i8)
            .seq_id(WORKER_MIRROR_SEQ_ID)
            .build();
        let unmarked_heartbeat = Builder::new_rpc(RpcCode::WorkerHeartbeat as i8).build();
        let marked_mutation = Builder::new_rpc(RpcCode::Mkdir as i8)
            .seq_id(WORKER_MIRROR_SEQ_ID)
            .build();

        assert!(MasterHandler::active_worker_mirror_rpc_rejected(
            true,
            &marked_heartbeat,
            RpcCode::WorkerHeartbeat
        ));
        assert!(MasterHandler::active_worker_mirror_rpc_rejected(
            true,
            &marked_report,
            RpcCode::WorkerBlockReport
        ));
        assert!(!MasterHandler::active_worker_mirror_rpc_rejected(
            true,
            &unmarked_heartbeat,
            RpcCode::WorkerHeartbeat
        ));
        assert!(!MasterHandler::active_worker_mirror_rpc_rejected(
            false,
            &marked_heartbeat,
            RpcCode::WorkerHeartbeat
        ));
        assert!(!MasterHandler::active_worker_mirror_rpc_rejected(
            true,
            &marked_mutation,
            RpcCode::Mkdir
        ));
    }

    #[test]
    fn follower_read_gate_requires_request_marker_config_allowlist_and_read_only_open_file() {
        let follower = curvine_raft::raft::RoleState::Follower;
        let marked_request = Builder::new_rpc(RpcCode::FileStatus as i8)
            .seq_id(FOLLOWER_READ_SEQ_ID)
            .build();
        let unmarked_request = Builder::new_rpc(RpcCode::FileStatus as i8).build();
        let marked = MasterHandler::is_follower_read_opt_in(&marked_request);
        let unmarked = MasterHandler::is_follower_read_opt_in(&unmarked_request);
        assert!(marked);
        assert!(!unmarked);

        assert_eq!(
            MasterHandler::follower_read_denial_reason(
                false,
                follower,
                marked,
                RpcCode::FileStatus,
                None
            ),
            Some("disabled")
        );
        assert_eq!(
            MasterHandler::follower_read_denial_reason(
                true,
                curvine_raft::raft::RoleState::Leader,
                marked,
                RpcCode::FileStatus,
                None
            ),
            Some("not_follower")
        );
        assert_eq!(
            MasterHandler::follower_read_denial_reason(
                true,
                follower,
                marked,
                RpcCode::FileStatus,
                None
            ),
            None
        );
        assert_eq!(
            MasterHandler::follower_read_denial_reason(
                true,
                follower,
                unmarked,
                RpcCode::FileStatus,
                None
            ),
            Some("missing_opt_in")
        );
        assert_eq!(
            MasterHandler::follower_read_denial_reason(
                true,
                follower,
                marked,
                RpcCode::OpenFile,
                Some(OpenFlags::RDONLY.bits())
            ),
            None
        );
        assert_eq!(
            MasterHandler::follower_read_denial_reason(
                true,
                follower,
                unmarked,
                RpcCode::OpenFile,
                Some(OpenFlags::RDONLY.bits())
            ),
            Some("missing_opt_in")
        );
        assert_eq!(
            MasterHandler::follower_read_denial_reason(
                true,
                follower,
                marked,
                RpcCode::OpenFile,
                Some(OpenFlags::WRONLY.bits())
            ),
            Some("open_file_not_read_only")
        );
        assert_eq!(
            MasterHandler::follower_read_denial_reason(
                true,
                follower,
                marked,
                RpcCode::ListTrash,
                None
            ),
            Some("not_allowlisted")
        );
    }

    #[test]
    fn follower_read_metrics_have_bounded_rpc_and_reason_labels() {
        let metrics = test_metrics();
        let accepted_before = metrics
            .follower_read_accepted_total
            .with_label_values(&["FileStatus"])
            .get();
        let refused_before = metrics
            .follower_read_refused_total
            .with_label_values(&["FileStatus", "commit_lag"])
            .get();

        MasterHandler::record_follower_read_accepted(metrics, RpcCode::FileStatus);
        MasterHandler::record_follower_read_refused(metrics, RpcCode::FileStatus, "commit_lag");

        assert_eq!(
            metrics
                .follower_read_accepted_total
                .with_label_values(&["FileStatus"])
                .get(),
            accepted_before + 1
        );
        assert_eq!(
            metrics
                .follower_read_refused_total
                .with_label_values(&["FileStatus", "commit_lag"])
                .get(),
            refused_before + 1
        );
    }

    #[test]
    fn process_worker_heartbeat_stores_worker_report_fields() {
        Master::init_test_metrics();
        let test_name = Utils::rand_str(6);
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir =
            Utils::test_sub_dir(format!("master-handler-test/meta-{}", test_name));
        conf.journal.journal_dir =
            Utils::test_sub_dir(format!("master-handler-test/journal-{}", test_name));

        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        let address = WorkerAddress {
            worker_id: 7,
            hostname: "worker-host".to_string(),
            ip_addr: "127.0.0.1".to_string(),
            rpc_port: 1234,
            web_port: 5678,
        };
        let component_info = curvine_proto::ComponentInfoProto {
            component: Some("worker".to_string()),
            release_version: Some("0.4.0-alpha".to_string()),
            git_commit: Some("24c848719b5b4fea74519d91cbe462bb49761b36".to_string()),
            git_tag: Some("v0.4.0-alpha".to_string()),
            git_branch: Some("main".to_string()),
            protocol_version: Some(1),
            min_protocol_version: Some(1),
            capabilities: vec!["transfer".to_string()],
        };
        let header = WorkerHeartbeatRequest {
            status: HeartbeatStatus::Running.into(),
            cluster_id: conf.cluster_id.clone(),
            address: ProtoUtils::worker_address_to_pb(&address),
            software_version: "0.1.0-test".to_string(),
            fs_ctime: 123_456,
            component_info: Some(component_info.clone()),
            ..Default::default()
        };

        MasterHandler::process_worker_heartbeat(fs.clone(), header).unwrap();

        let info = fs.filesystem_info().unwrap();
        let worker = info
            .live_workers
            .iter()
            .find(|worker| worker.address.worker_id == address.worker_id)
            .unwrap();
        assert_eq!(worker.software_version, "0.1.0-test");
        assert_eq!(worker.startup_time_ms, 123_456);
        // Structured version metadata survives heartbeat -> WorkerInfo ->
        // WorkerInfoProto (filesystem_info) -> WorkerInfo round trip.
        assert_eq!(worker.component_info, Some(component_info));
    }

    fn client_heartbeat_req(client_name: &str) -> ClientHeartbeatRequest {
        ClientHeartbeatRequest {
            client: ClientAddressProto {
                client_name: client_name.to_string(),
                hostname: "client-host".to_string(),
                ip_addr: "127.0.0.1".to_string(),
                port: 0,
            },
            heartbeat_interval_ms: 12_000,
            pid: Some(4242),
            mount_point: Some("/mnt/curvine".to_string()),
            client_version: Some("0.1.0-test".to_string()),
        }
    }

    fn client_heartbeat_fs(test_name: &str) -> MasterFilesystem {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir =
            Utils::test_sub_dir(format!("master-handler-test/meta-{}", test_name));
        conf.journal.journal_dir =
            Utils::test_sub_dir(format!("master-handler-test/journal-{}", test_name));
        conf.master.client_session_timeout = "30s".to_string();
        conf.master.init().unwrap();
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        // `process_client_heartbeat` only serves the active master, so stand in
        // for raft leader election (the tests that exercise a standby set the
        // role themselves).
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);
        fs
    }

    #[test]
    fn process_client_heartbeat_registers_then_renews_session() {
        // Publishes the process-global session gauge (heartbeats reach
        // `MasterFilesystem::heartbeat_in_epoch`): serialize against the
        // session-reap tests' exact assertions (#1885 review).
        let _metric_serial = crate::master::Master::metric_test_serial();
        let fs = client_heartbeat_fs(&Utils::rand_str(6));

        let first =
            MasterHandler::process_client_heartbeat(&fs, client_heartbeat_req("c-1"), 1_000)
                .unwrap();
        assert!(first.new_session);
        assert_eq!(first.session_timeout_ms, 30_000);

        let second =
            MasterHandler::process_client_heartbeat(&fs, client_heartbeat_req("c-1"), 13_000)
                .unwrap();
        assert!(!second.new_session);

        // Clones of MasterFilesystem share the same session map.
        let shared = fs.clone();
        let sessions = shared.session_map.read();
        let s = sessions.get("c-1").unwrap();
        assert_eq!(s.started_at, 1_000);
        assert_eq!(s.last_heartbeat, 13_000);
        assert_eq!(s.expire_at, 43_000);
        assert_eq!(s.pid, Some(4242));
        assert_eq!(s.mount_point.as_deref(), Some("/mnt/curvine"));
        assert_eq!(s.address.hostname, "client-host");
        assert!(sessions.is_live("c-1", 43_000));
        assert!(!sessions.is_live("c-1", 43_001));
    }

    #[test]
    fn client_heartbeat_starts_a_new_lease_epoch_after_demotion() {
        use curvine_raft::raft::RoleState;

        // Publishes the process-global session gauge (#1885 review).
        let _metric_serial = crate::master::Master::metric_test_serial();
        let fs = client_heartbeat_fs(&Utils::rand_str(6));

        fs.master_monitor.set_role_for_test(RoleState::Leader);
        fs.master_monitor.set_term_for_test(1);
        MasterHandler::process_client_heartbeat(&fs, client_heartbeat_req("c-1"), 1_000).unwrap();
        MasterHandler::process_client_heartbeat(&fs, client_heartbeat_req("c-2"), 1_000).unwrap();
        assert_eq!(fs.session_map.read().created_at(), 1_000);
        assert_eq!(fs.session_map.read().len(), 2);

        // A demoted master must reject the heartbeat and leave the map untouched,
        // so it cannot own a lease in an epoch it never adopted.
        fs.master_monitor.set_role_for_test(RoleState::Follower);
        let err = MasterHandler::process_client_heartbeat(&fs, client_heartbeat_req("c-1"), 2_000)
            .expect_err("a standby must not renew a client session");
        assert!(
            matches!(err.kind(), ErrorKind::NotLeaderMaster),
            "a standby must answer NotLeaderMaster, got {:?}",
            err.kind()
        );
        assert_eq!(fs.session_map.read().created_at(), 1_000);
        assert_eq!(
            fs.session_map.read().get("c-1").unwrap().last_heartbeat,
            1_000,
            "the standby heartbeat must not renew the lease"
        );

        // Promotion carries a higher term: the previous term's sessions are gone
        // and the grace window restarts at promotion, not at process start.
        fs.master_monitor.set_role_for_test(RoleState::Leader);
        fs.master_monitor.set_term_for_test(2);
        let rep = MasterHandler::process_client_heartbeat(&fs, client_heartbeat_req("c-1"), 30_000)
            .unwrap();
        assert!(
            rep.new_session,
            "the new term must not inherit the old session"
        );

        let sessions = fs.session_map.read();
        assert_eq!(
            sessions.created_at(),
            30_000,
            "the lease epoch must restart when a new term is adopted"
        );
        assert_eq!(sessions.len(), 1);
        assert!(
            sessions.get("c-2").is_none(),
            "sessions from before the demotion must not survive the new term"
        );
    }

    #[test]
    fn process_client_heartbeat_lease_comes_from_the_master_timeout() {
        // Publishes the process-global session gauge (#1885 review).
        let _metric_serial = crate::master::Master::metric_test_serial();
        let fs = client_heartbeat_fs(&Utils::rand_str(6));

        // The client asks for a lease far longer than the master allows; the
        // enforced window must stay the master's `client_session_timeout`.
        let mut header = client_heartbeat_req("greedy-client");
        header.heartbeat_interval_ms = 3_600_000;

        let first = MasterHandler::process_client_heartbeat(&fs, header, 1_000).unwrap();
        assert_eq!(first.session_timeout_ms, 30_000);

        let sessions = fs.session_map.read();
        let s = sessions.get("greedy-client").unwrap();
        assert_eq!(
            s.expire_at - s.last_heartbeat,
            30_000,
            "a client-reported interval must never widen the enforced lease"
        );
        assert_eq!(
            s.heartbeat_interval_ms, 3_600_000,
            "the reported interval is still recorded for diagnostics"
        );
    }

    #[test]
    fn process_client_heartbeat_rejects_empty_client_name() {
        let fs = client_heartbeat_fs(&Utils::rand_str(6));

        let err = MasterHandler::process_client_heartbeat(&fs, client_heartbeat_req(""), 0)
            .expect_err("empty client_name must be rejected");
        assert!(err.to_string().contains("non-empty client_name"));
        assert!(fs.session_map.read().is_empty());
    }

    #[test]
    fn build_filesystem_info_response_attaches_master_compatibility() {
        let info = FilesystemInfo {
            active_master: "master-0".to_string(),
            inode_dir_num: 3,
            inode_file_num: 5,
            block_num: 7,
            capacity: 1000,
            available: 500,
            fs_used: 300,
            non_fs_used: 200,
            ..Default::default()
        };

        // Derive expectations from component_version("master") so the test stays
        // stable across BUILD_VERSION overrides and future protocol bumps.
        let master_version = curvine_sys::version::component_version("master");
        let master_compatibility = ProtoUtils::default_master_compatibility_to_pb(&master_version);

        let rep = MasterHandler::build_filesystem_info_response(info, &master_compatibility);

        assert_eq!(rep.active_master, "master-0");
        assert_eq!(rep.inode_file_num, 5);
        let compat = rep
            .compatibility
            .expect("master must advertise compatibility");
        assert_eq!(compat.server.component.as_deref(), Some("master"));
        assert_eq!(
            compat.server.release_version.as_deref(),
            Some(master_version.release_version.as_str())
        );
        assert_eq!(
            compat.server.protocol_version,
            Some(master_version.protocol_version)
        );
        assert_eq!(
            compat.compatibility_mode,
            CompatibilityModeProto::Diagnose as i32
        );
        assert!(compat.blocked_versions.is_empty());
    }

    #[test]
    fn diagnose_warnings_are_deduped_per_peer() {
        // A persistently incompatible worker must not re-log the same warning
        // on every heartbeat: only a verdict change emits a new warning.
        let policy = CompatibilityPolicy {
            mode: CompatibilityMode::Diagnose,
            min_worker_version: Some("0.2.0".parse().unwrap()),
            ..Default::default()
        };
        let warned = DashMap::new();
        let verdict = policy.check_worker(Some(&ComponentInfoProto {
            release_version: Some("0.1.0".to_string()),
            protocol_version: Some(1),
            ..sample_component_info()
        }));

        // First occurrence warns and records the verdict.
        assert!(MasterHandler::check_peer_compatibility(
            "worker",
            "worker:7",
            &warned,
            policy.mode,
            verdict.clone(),
            test_metrics()
        )
        .is_ok());
        assert!(warned.contains_key("worker:7"));

        // Identical verdict on a later heartbeat does not warn again but is
        // still allowed.
        assert!(MasterHandler::check_peer_compatibility(
            "worker",
            "worker:7",
            &warned,
            policy.mode,
            verdict.clone(),
            test_metrics()
        )
        .is_ok());

        // A different incompatible verdict for the same peer warns again.
        let different = CompatibilityVerdict::ProtocolMismatch {
            peer: 2,
            min: 1,
            max: 1,
        };
        assert!(MasterHandler::check_peer_compatibility(
            "worker",
            "worker:7",
            &warned,
            policy.mode,
            different.clone(),
            test_metrics()
        )
        .is_ok());
        assert_eq!(warned.get("worker:7").as_deref(), Some(&different));

        // A separate peer is tracked independently.
        assert!(!warned.contains_key("worker:8"));
    }

    #[test]
    fn diagnose_mode_allows_incompatible_worker_heartbeat() {
        // Configure a minimum worker version so the peer is genuinely
        // incompatible (below the bound): diagnose mode must still allow the
        // request (logging a warning) instead of rejecting it. A legacy worker
        // without component_info is also allowed.
        let policy = CompatibilityPolicy {
            mode: CompatibilityMode::Diagnose,
            min_worker_version: Some("0.2.0".parse().unwrap()),
            ..Default::default()
        };
        let incompatible = ComponentInfoProto {
            release_version: Some("0.1.0".to_string()),
            protocol_version: Some(1),
            ..sample_component_info()
        };
        let verdict = policy.check_worker(Some(&incompatible));
        assert_eq!(
            verdict,
            CompatibilityVerdict::VersionTooOld {
                peer: "0.1.0".to_string(),
                min: "0.2.0".to_string()
            }
        );
        let warned = DashMap::new();
        assert!(MasterHandler::check_peer_compatibility(
            "worker",
            "worker:7",
            &warned,
            policy.mode,
            verdict,
            test_metrics()
        )
        .is_ok());

        // Legacy worker (no component_info): MissingInfo, allowed in diagnose.
        let verdict = policy.check_worker(None);
        assert!(MasterHandler::check_peer_compatibility(
            "worker",
            "worker:7",
            &warned,
            policy.mode,
            verdict,
            test_metrics()
        )
        .is_ok());
    }

    #[test]
    fn enforce_mode_rejects_incompatible_worker_heartbeat() {
        let policy = CompatibilityPolicy {
            mode: CompatibilityMode::Enforce,
            min_worker_version: Some("0.2.0".parse().unwrap()),
            ..Default::default()
        };
        let incompatible = ComponentInfoProto {
            release_version: Some("0.1.0".to_string()),
            protocol_version: Some(1),
            ..sample_component_info()
        };
        let verdict = policy.check_worker(Some(&incompatible));
        let warned = DashMap::new();
        let err = MasterHandler::check_peer_compatibility(
            "worker",
            "worker:7",
            &warned,
            policy.mode,
            verdict,
            test_metrics(),
        )
        .unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("rejected by compatibility policy"), "{msg}");
        assert!(msg.contains("0.1.0"), "{msg}");
    }

    #[test]
    fn enforce_mode_rejects_legacy_client_without_component_info() {
        let policy = CompatibilityPolicy {
            mode: CompatibilityMode::Enforce,
            ..Default::default()
        };
        let verdict = policy.check_client(None);
        let warned = DashMap::new();
        let err = MasterHandler::check_peer_compatibility(
            "client",
            "client:127.0.0.1",
            &warned,
            policy.mode,
            verdict,
            test_metrics(),
        )
        .unwrap_err();
        assert!(format!("{}", err).contains("client rejected"));
    }

    #[test]
    fn compatibility_metrics_record_verdicts_and_rejections() {
        // Worker verdicts are recorded per peer in compat_worker_verdict and
        // enforce-mode rejections bump compat_enforce_rejected_total.
        let metrics = test_metrics();

        let policy = CompatibilityPolicy {
            mode: CompatibilityMode::Enforce,
            min_worker_version: Some("0.2.0".parse().unwrap()),
            ..Default::default()
        };
        let incompatible = ComponentInfoProto {
            release_version: Some("0.1.0".to_string()),
            protocol_version: Some(1),
            ..sample_component_info()
        };
        let verdict = policy.check_worker(Some(&incompatible));
        let warned = DashMap::new();

        // A compatible worker records the compatible verdict. Unique peer ids
        // keep this test isolated: metrics are process-global and other tests
        // also exercise worker:7 / worker:8 concurrently.
        MasterHandler::check_peer_compatibility(
            "worker",
            "worker:700",
            &warned,
            CompatibilityMode::Diagnose,
            CompatibilityVerdict::Compatible,
            metrics,
        )
        .unwrap();
        assert_eq!(
            metrics
                .compat_worker_verdict
                .with_label_values(&["700", "compatible"])
                .get(),
            1
        );

        // When a peer's verdict changes, the previous label must be cleared:
        // only one label per peer is active at a time.
        MasterHandler::check_peer_compatibility(
            "worker",
            "worker:700",
            &warned,
            CompatibilityMode::Diagnose,
            CompatibilityVerdict::VersionTooOld {
                peer: "0.1.0".to_string(),
                min: "0.2.0".to_string(),
            },
            metrics,
        )
        .unwrap();
        assert_eq!(
            metrics
                .compat_worker_verdict
                .with_label_values(&["700", "version_too_old"])
                .get(),
            1
        );
        assert_eq!(
            metrics
                .compat_worker_verdict
                .with_label_values(&["700", "compatible"])
                .get(),
            0
        );

        // An enforce-mode rejection (too-old worker) bumps the counter and
        // records the verdict gauge. A unique component label keeps this test
        // isolated from other tests that also land in the version_too_old
        // series (metrics are process-global and tests run in parallel).
        let before = metrics
            .compat_enforce_rejected_total
            .with_label_values(&["worker-test", "version_too_old"])
            .get();
        let err = MasterHandler::check_peer_compatibility(
            "worker-test",
            "worker:701",
            &warned,
            policy.mode,
            verdict,
            metrics,
        )
        .unwrap_err();
        assert!(format!("{}", err).contains("rejected by compatibility policy"));
        assert_eq!(
            metrics
                .compat_enforce_rejected_total
                .with_label_values(&["worker-test", "version_too_old"])
                .get(),
            before + 1
        );
        assert_eq!(
            metrics
                .compat_worker_verdict
                .with_label_values(&["701", "version_too_old"])
                .get(),
            1
        );

        // Client verdicts are aggregated by `record_client_compat`, not here.
        MasterHandler::check_peer_compatibility(
            "client",
            "client:10.0.0.1",
            &warned,
            CompatibilityMode::Diagnose,
            CompatibilityVerdict::MissingInfo,
            metrics,
        )
        .unwrap();
    }

    #[test]
    fn compatibility_to_pb_reflects_policy() {
        let policy = CompatibilityPolicy {
            mode: CompatibilityMode::Enforce,
            min_worker_version: Some("0.2.0".parse().unwrap()),
            blocked_versions: vec!["0.2.5".parse().unwrap()],
            ..Default::default()
        };
        let master_version = curvine_sys::version::component_version("master");
        let pb = ProtoUtils::compatibility_to_pb(&master_version, &policy);
        assert_eq!(
            pb.compatibility_mode,
            CompatibilityModeProto::Enforce as i32
        );
        assert_eq!(pb.min_worker_version.as_deref(), Some("0.2.0"));
        assert_eq!(pb.blocked_versions, vec!["0.2.5".to_string()]);
    }

    fn sample_component_info() -> ComponentInfoProto {
        ComponentInfoProto {
            component: Some("worker".to_string()),
            release_version: Some("0.4.0-alpha".to_string()),
            git_commit: Some("24c848719b5b4fea74519d91cbe462bb49761b36".to_string()),
            git_tag: Some("v0.4.0-alpha".to_string()),
            git_branch: Some("main".to_string()),
            protocol_version: Some(1),
            min_protocol_version: Some(1),
            capabilities: vec!["transfer".to_string()],
        }
    }

    /// Shared metrics instance for compatibility metric tests.
    fn test_metrics() -> &'static MasterMetrics {
        Master::init_test_metrics();
        Master::get_metrics().unwrap()
    }

    fn trash_entry(path: &str, delete_ms: i64) -> TrashEntry {
        TrashEntry {
            path: path.to_string(),
            origin: path.to_string(),
            delete_ms,
            status: FileStatus::default(),
        }
    }

    fn trash_paths(entries: &[TrashEntryProto]) -> Vec<&str> {
        entries.iter().map(|entry| entry.path.as_str()).collect()
    }

    #[test]
    fn encode_trash_page_carries_the_scan_cursor_only_when_more_entries_remain() {
        let listing = TrashListing {
            entries: vec![
                trash_entry("/.Trash/1-a", 10),
                trash_entry("/.Trash/2-a", 20),
            ],
            next: Some(TrashCursor {
                delete_ms: 20,
                path: "/.Trash/2-a".to_string(),
            }),
        };

        let (page, token) = encode_trash_page(listing, usize::MAX);
        assert_eq!(trash_paths(&page), vec!["/.Trash/1-a", "/.Trash/2-a"]);
        assert_eq!(token.as_deref(), Some("20:/.Trash/2-a"));

        let listing = TrashListing {
            entries: vec![trash_entry("/.Trash/3-a", 30)],
            next: None,
        };
        let (page, token) = encode_trash_page(listing, usize::MAX);
        assert_eq!(trash_paths(&page), vec!["/.Trash/3-a"]);
        assert!(token.is_none());
    }

    #[test]
    fn encode_trash_page_enforces_the_encoded_byte_budget() {
        let entries = vec![
            trash_entry("/.Trash/1-a", 10),
            trash_entry("/.Trash/2-a", 20),
        ];
        let first_size = ProtoUtils::trash_entry_to_pb(entries[0].clone()).encoded_len();

        let listing = TrashListing {
            entries,
            next: Some(TrashCursor {
                delete_ms: 20,
                path: "/.Trash/2-a".to_string(),
            }),
        };
        let (page, token) = encode_trash_page(listing, first_size);
        assert_eq!(trash_paths(&page), vec!["/.Trash/1-a"]);
        assert_eq!(token.as_deref(), Some("10:/.Trash/1-a"));
    }

    #[test]
    fn encode_trash_page_skips_entries_that_cannot_fit_any_page() {
        let normal = trash_entry("/.Trash/2-a", 20);
        let normal_size = ProtoUtils::trash_entry_to_pb(normal.clone()).encoded_len();
        let mut oversized = trash_entry("/.Trash/1-a", 10);
        oversized.origin = format!("/data/{}", "x".repeat(normal_size));

        // The first entry alone exceeds the budget: it is skipped, the page
        // still returns the entries that fit, and the cursor moves past it.
        let listing = TrashListing {
            entries: vec![oversized.clone(), normal.clone()],
            next: None,
        };
        let (page, token) = encode_trash_page(listing, normal_size);
        assert_eq!(trash_paths(&page), vec!["/.Trash/2-a"]);
        assert!(token.is_none());

        // A skipped entry must never be handed back as the first entry of the
        // next page: the resume key comes from the scan cursor.
        let listing = TrashListing {
            entries: vec![oversized],
            next: Some(TrashCursor {
                delete_ms: 10,
                path: "/.Trash/1-a".to_string(),
            }),
        };
        let (page, token) = encode_trash_page(listing, normal_size);
        assert!(page.is_empty());
        assert_eq!(token.as_deref(), Some("10:/.Trash/1-a"));
    }

    #[test]
    fn parse_trash_page_token_rejects_invalid_tokens() {
        for token in ["invalid", "10:no-leading-slash", "abc:/.Trash/1-a"] {
            assert!(
                parse_trash_page_token(token).is_err(),
                "token {token} should be rejected"
            );
        }

        let cursor = parse_trash_page_token("10:/.Trash/1-a").unwrap();
        assert_eq!(cursor.delete_ms, 10);
        assert_eq!(cursor.path, "/.Trash/1-a");
    }
    fn cache_fence_fixture() -> (MasterFilesystem, WorkerHeartbeatRequest) {
        Master::init_test_metrics();
        let name = Utils::rand_str(8);
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = Utils::test_sub_dir(format!("cache-fence/meta-{name}"));
        conf.journal.journal_dir = Utils::test_sub_dir(format!("cache-fence/journal-{name}"));
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        let header = WorkerHeartbeatRequest {
            cluster_id: conf.cluster_id.clone(),
            address: ProtoUtils::worker_address_to_pb(&WorkerAddress {
                worker_id: 7,
                hostname: "worker".into(),
                ip_addr: "127.0.0.1".into(),
                rpc_port: 8997,
                web_port: 9000,
            }),
            status: HeartbeatStatus::Running.into(),
            worker_session_id: Some("session".into()),
            capacity_snapshot_seq: Some(1),
            storages: vec![ProtoUtils::storage_info_to_pb(curvine_model::StorageInfo {
                capacity: 1 << 30,
                available: 1 << 30,
                storage_id: "disk".into(),
                ..Default::default()
            })],
            ..Default::default()
        };
        let mut start = header.clone();
        start.status = HeartbeatStatus::Start.into();
        start.capacity_snapshot_seq = None;
        MasterHandler::process_worker_heartbeat(fs.clone(), start).unwrap();
        MasterHandler::process_worker_heartbeat(fs.clone(), header.clone()).unwrap();
        (fs, header)
    }

    fn cache_fence_create(fs: &MasterFilesystem, path: &str, flush: bool, publish: bool) -> i64 {
        fs.create(path, true).unwrap();
        let block = fs
            .add_block(
                path,
                None,
                curvine_model::ClientAddress::default(),
                vec![],
                vec![],
                0,
                None,
            )
            .unwrap();
        let mut commit = curvine_model::CommitBlock::from(&block);
        commit.block_len = 4 << 20;
        fs.complete_file(path, None, 4 << 20, vec![commit], "", flush, None)
            .unwrap();
        if publish {
            fs.set_attr(
                path,
                curvine_model::SetAttrOptsBuilder::new()
                    .ufs_mtime(123)
                    .build(),
            )
            .unwrap();
        }
        block.id
    }

    fn cache_fence_credit(fs: &MasterFilesystem) -> i64 {
        fs.worker_manager
            .read()
            .get_worker(7)
            .unwrap()
            .scheduled_bytes
    }

    #[test]
    fn cache_fence_net_zero_capacity_requires_new_snapshot_and_only_closes_frozen_batch() {
        let (fs, mut h) = cache_fence_fixture();
        cache_fence_create(&fs, "/cache/a", false, true);
        assert_eq!(cache_fence_credit(&fs), 4 << 20);
        h.capacity_snapshot_seq = Some(2);
        let fence = MasterHandler::process_worker_heartbeat(fs.clone(), h.clone())
            .unwrap()
            .cache_reservation_fence
            .unwrap();
        cache_fence_create(&fs, "/cache/b", false, true);
        h.cache_reservation_fence_ack = Some(fence.clone());
        // Same/older snapshot, even with the right token, is not confirmation.
        MasterHandler::process_worker_heartbeat(fs.clone(), h.clone()).unwrap();
        assert_eq!(cache_fence_credit(&fs), 8 << 20);
        let mut stale = h.clone();
        stale.capacity_snapshot_seq = Some(1);
        stale.storages[0].available = 0;
        MasterHandler::process_worker_heartbeat(fs.clone(), stale).unwrap();
        assert_eq!(
            fs.worker_manager.read().get_worker(7).unwrap().available,
            1 << 30
        );
        h.capacity_snapshot_seq = Some(3);
        let second = MasterHandler::process_worker_heartbeat(fs.clone(), h.clone())
            .unwrap()
            .cache_reservation_fence
            .unwrap();
        assert_eq!(
            cache_fence_credit(&fs),
            4 << 20,
            "B completed after the first challenge"
        );
        h.capacity_snapshot_seq = Some(4);
        h.cache_reservation_fence_ack = Some(second);
        MasterHandler::process_worker_heartbeat(fs.clone(), h.clone()).unwrap();
        assert_eq!(cache_fence_credit(&fs), 0);
        h.capacity_snapshot_seq = Some(5);
        MasterHandler::process_worker_heartbeat(fs.clone(), h).unwrap();
        assert_eq!(cache_fence_credit(&fs), 0);
    }

    #[test]
    fn cache_fence_native_flush_reopen_and_legacy_are_excluded() {
        let (fs, mut h) = cache_fence_fixture();
        cache_fence_create(&fs, "/native", false, false);
        cache_fence_create(&fs, "/flushed", true, true);
        h.capacity_snapshot_seq = Some(2);
        assert!(
            MasterHandler::process_worker_heartbeat(fs.clone(), h.clone())
                .unwrap()
                .cache_reservation_fence
                .is_none()
        );
        cache_fence_create(&fs, "/cache", false, true);
        h.capacity_snapshot_seq = Some(3);
        let token = MasterHandler::process_worker_heartbeat(fs.clone(), h.clone())
            .unwrap()
            .cache_reservation_fence
            .unwrap();
        fs.open_file(
            "/cache",
            curvine_model::CreateFileOpts::with_create(false),
            curvine_model::OpenFlags::new_write_only(),
        )
        .unwrap();
        h.capacity_snapshot_seq = Some(4);
        h.cache_reservation_fence_ack = Some(token);
        MasterHandler::process_worker_heartbeat(fs.clone(), h).unwrap();
        assert_eq!(cache_fence_credit(&fs), 12 << 20);

        let (fs, mut h) = cache_fence_fixture();
        // A legacy process has its own session, not a downgrade mid-session.
        h.worker_session_id = Some("legacy-session".into());
        h.status = HeartbeatStatus::Start.into();
        h.capacity_snapshot_seq = None;
        MasterHandler::process_worker_heartbeat(fs.clone(), h.clone()).unwrap();
        h.status = HeartbeatStatus::Running.into();
        MasterHandler::process_worker_heartbeat(fs.clone(), h.clone()).unwrap();
        cache_fence_create(&fs, "/legacy-cache", false, true);
        assert!(MasterHandler::process_worker_heartbeat(fs.clone(), h)
            .unwrap()
            .cache_reservation_fence
            .is_none());
        assert_eq!(cache_fence_credit(&fs), 4 << 20);
    }

    fn cache_report_request(h: &WorkerHeartbeatRequest) -> BlockReportListRequest {
        BlockReportListRequest {
            cluster_id: h.cluster_id.clone(),
            worker_id: 7,
            full_report: true,
            report_id: Some(456),
            worker_session_id: h.worker_session_id.clone(),
            ..Default::default()
        }
    }

    #[test]
    fn inventory_token_is_issued_only_for_accepted_start_and_rejects_stale_reports() {
        let (fs, mut start) = cache_fence_fixture();
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);
        start.status = HeartbeatStatus::Start.into();
        start.capacity_snapshot_seq = None;
        let first = MasterHandler::process_worker_heartbeat(fs.clone(), start.clone()).unwrap();
        let token = first.full_report_token.unwrap();
        let mut report = cache_report_request(&start);
        report.full_report_token = Some(token.clone());

        let mut foreign = start.clone();
        foreign.cluster_id = "foreign".into();
        assert!(MasterHandler::process_worker_heartbeat(fs.clone(), foreign).is_err());
        // A rejected registration must not reset the valid round.
        MasterHandler::process_block_report(fs.clone(), None, report.clone()).unwrap();
        MasterHandler::process_block_report(fs.clone(), None, report.clone()).unwrap();
        let mut another_inventory = report.clone();
        another_inventory.report_id = Some(457);
        assert!(MasterHandler::process_block_report(fs.clone(), None, another_inventory).is_err());
        let second = MasterHandler::process_worker_heartbeat(fs.clone(), start).unwrap();
        assert_ne!(second.full_report_token.as_deref(), Some(token.as_str()));
        let error = MasterHandler::process_block_report(fs.clone(), None, report.clone())
            .err()
            .unwrap();
        assert!(error.to_string().starts_with("NotRegistered:"));
        report.full_report_token = second.full_report_token;
        MasterHandler::process_block_report(fs.clone(), None, report.clone()).unwrap();
        fs.master_monitor
            .set_term_for_test(fs.master_monitor.term() + 1);
        assert!(MasterHandler::process_block_report(fs, None, report).is_err());
    }

    #[test]
    fn cache_report_chunks_renew_provisional_liveness_without_enabling_allocation() {
        for new_session in [false, true] {
            let (fs, mut h) = cache_fence_fixture();
            let ids: Vec<_> = (0..3)
                .map(|i| cache_fence_create(&fs, &format!("/report-{i}"), false, true))
                .collect();
            h.status = HeartbeatStatus::Start.into();
            h.capacity_snapshot_seq = None;
            if new_session {
                h.worker_session_id = Some("restarted".into());
            }
            MasterHandler::process_worker_heartbeat(fs.clone(), h.clone()).unwrap();
            fs.worker_manager
                .write()
                .worker_map
                .workers
                .get_mut(&7)
                .unwrap()
                .last_update = 0;
            let credit = cache_fence_credit(&fs);
            let mut report = cache_report_request(&h);
            report.total_len = 3;
            for (i, id) in ids.into_iter().enumerate() {
                let now = (i as u64 + 1) * 800;
                report.blocks = vec![BlockReportInfoProto {
                    id,
                    status: curvine_model::BlockReportStatus::Finalized.into(),
                    block_size: 4 << 20,
                    storage_type: curvine_model::StorageType::Disk.into(),
                    ..Default::default()
                }];
                MasterHandler::process_block_report_with_clock(
                    fs.clone(),
                    None,
                    report.clone(),
                    || now,
                )
                .unwrap();
                let mut wm = fs.worker_manager.write();
                // Use the checker's actual final expiration check between chunks.
                // Total reporting exceeds 1000 ms, but each gap remains below it.
                assert!(wm
                    .remove_expired_worker_if_stale(7, now + 400, 0, 1000)
                    .is_none());
                let worker = wm.get_worker(7).unwrap();
                assert_eq!(worker.last_update, now);
                assert!(!worker.can_allocate(1), "reporting is not readiness");
                assert_eq!(worker.available, 1 << 30);
                assert_eq!(worker.scheduled_bytes, credit);
                assert_eq!(
                    worker.cache_reservation_fence.snapshot_seq,
                    if new_session { 0 } else { 1 }
                );
                assert_eq!(worker.cache_reservation_observation(now), (0, 0));
            }
            assert_eq!(fs.full_report_completed_round(7), Some(456));
            h.status = HeartbeatStatus::Running.into();
            h.capacity_snapshot_seq = Some(2);
            MasterHandler::process_worker_heartbeat(fs.clone(), h).unwrap();
            assert!(fs
                .worker_manager
                .read()
                .get_worker(7)
                .unwrap()
                .can_allocate(1));
        }
    }

    #[test]
    fn cache_report_stopped_progress_still_expires_and_late_chunks_cannot_revive() {
        let (fs, mut h) = cache_fence_fixture();
        h.status = HeartbeatStatus::Start.into();
        MasterHandler::process_worker_heartbeat(fs.clone(), h.clone()).unwrap();
        fs.worker_manager
            .write()
            .worker_map
            .workers
            .get_mut(&7)
            .unwrap()
            .last_update = 0;
        let mut report = cache_report_request(&h);
        report.total_len = 2;
        report.blocks = vec![BlockReportInfoProto {
            id: 9876,
            status: curvine_model::BlockReportStatus::Finalized.into(),
            ..Default::default()
        }];
        MasterHandler::process_block_report_with_clock(fs.clone(), None, report.clone(), || 800)
            .unwrap();
        {
            let mut wm = fs.worker_manager.write();
            assert!(wm
                .remove_expired_worker_if_stale(7, 1800, 0, 1000)
                .is_none());
            assert!(wm
                .remove_expired_worker_if_stale(7, 1801, 0, 1000)
                .is_some());
        }
        let error =
            MasterHandler::process_block_report_with_clock(fs.clone(), None, report, || 1900)
                .err()
                .expect("late report must re-register");
        assert!(error.to_string().contains("NotRegistered"));
        assert!(fs.worker_manager.read().get_worker(7).is_none());
    }

    #[test]
    fn cache_report_rejects_stale_session_and_foreign_cluster_before_processing() {
        let (fs, h) = cache_fence_fixture();
        cache_fence_create(&fs, "/protected", false, true);
        fs.worker_manager
            .write()
            .worker_map
            .workers
            .get_mut(&7)
            .unwrap()
            .last_update = 100;
        for stale_session in [true, false] {
            let mut report = cache_report_request(&h);
            if stale_session {
                report.worker_session_id = Some("previous-process".into());
            } else {
                report.cluster_id = "foreign-cluster".into();
            }
            assert!(MasterHandler::process_block_report_with_clock(
                fs.clone(),
                None,
                report,
                || 900
            )
            .is_err());
            let wm = fs.worker_manager.read();
            assert_eq!(wm.get_worker(7).unwrap().last_update, 100);
            assert_eq!(wm.get_worker(7).unwrap().scheduled_bytes, 4 << 20);
            assert_eq!(fs.full_report_completed_round(7), None);
            assert_eq!(fs.full_report_open_sessions(), 0);
        }
    }

    #[test]
    fn cache_report_legacy_and_incremental_reports_do_not_renew_registration() {
        let (fs, h) = cache_fence_fixture();
        fs.worker_manager
            .write()
            .worker_map
            .workers
            .get_mut(&7)
            .unwrap()
            .last_update = 100;
        for (full, session) in [
            (true, None),
            (true, Some(String::new())),
            (false, h.worker_session_id.clone()),
        ] {
            let mut report = cache_report_request(&h);
            report.full_report = full;
            report.worker_session_id = session;
            MasterHandler::process_block_report_with_clock(fs.clone(), None, report, || {
                panic!("legacy/incremental report must not enter renewal path")
            })
            .unwrap();
            assert_eq!(
                fs.worker_manager.read().get_worker(7).unwrap().last_update,
                100
            );
        }
    }

    #[test]
    fn cache_report_completion_revalidates_registration_after_restart_or_expiration() {
        for restart in [true, false] {
            let (fs, h) = cache_fence_fixture();
            fs.worker_manager
                .write()
                .worker_map
                .workers
                .get_mut(&7)
                .unwrap()
                .last_update = 0;
            let calls = std::cell::Cell::new(0);
            let result = MasterHandler::process_block_report_with_clock(
                fs.clone(),
                None,
                cache_report_request(&h),
                || {
                    let call = calls.get();
                    calls.set(call + 1);
                    if call == 0 {
                        return 800;
                    }
                    // The report has finished processing, but its session can
                    // change or expire before the completion-side renewal.
                    if restart {
                        let mut start = h.clone();
                        start.status = HeartbeatStatus::Start.into();
                        start.worker_session_id = Some("replacement".into());
                        MasterHandler::process_worker_heartbeat(fs.clone(), start).unwrap();
                        fs.worker_manager
                            .write()
                            .worker_map
                            .workers
                            .get_mut(&7)
                            .unwrap()
                            .last_update = 1500;
                    } else {
                        assert!(fs
                            .worker_manager
                            .write()
                            .remove_expired_worker_if_stale(7, 1801, 0, 1000)
                            .is_some());
                    }
                    1900
                },
            );
            assert!(result.err().unwrap().to_string().contains("NotRegistered"));
            let wm = fs.worker_manager.read();
            if restart {
                let worker = wm.get_worker(7).unwrap();
                assert_eq!(worker.worker_session_id, "replacement");
                assert_eq!(worker.last_update, 1500);
                assert!(!worker.can_allocate(1));
            } else {
                assert!(wm.get_worker(7).is_none());
            }
        }
    }

    #[test]
    fn cache_fence_start_retains_new_session_against_delayed_running() {
        let (fs, old) = cache_fence_fixture();
        let mut current = old.clone();
        current.worker_session_id = Some("new-session".into());
        current.status = HeartbeatStatus::Start.into();
        current.capacity_snapshot_seq = None;
        MasterHandler::process_worker_heartbeat(fs.clone(), current.clone()).unwrap();
        {
            let wm = fs.worker_manager.read();
            let worker = wm.get_worker(7).unwrap();
            assert_eq!(worker.worker_session_id, "new-session");
            assert!(!worker.is_live(), "Start alone must not enable allocation");
        }
        let mut delayed = old;
        delayed.capacity_snapshot_seq = Some(999);
        delayed.storages[0].available = 0;
        let rejected = MasterHandler::process_worker_heartbeat(fs.clone(), delayed).unwrap();
        assert_eq!(rejected.registration_required, Some(true));
        assert!(rejected.cmds.is_empty());
        assert!(rejected.cache_reservation_fence.is_none());
        {
            let wm = fs.worker_manager.read();
            let worker = wm.get_worker(7).unwrap();
            assert_eq!(worker.worker_session_id, "new-session");
            assert_eq!(worker.available, 1 << 30);
            assert_eq!(worker.cache_reservation_fence.snapshot_seq, 0);
            assert!(!worker.is_live());
        }
        current.status = HeartbeatStatus::Running.into();
        current.capacity_snapshot_seq = Some(1);
        assert_ne!(
            MasterHandler::process_worker_heartbeat(fs.clone(), current)
                .unwrap()
                .registration_required,
            Some(true)
        );
        assert!(fs.worker_manager.read().get_worker(7).unwrap().is_live());
    }

    #[test]
    fn cache_fence_promoted_stale_registration_recovers_with_start_and_full_report() {
        let (fs, mut current) = cache_fence_fixture();
        // A promoted follower retained the previous process's registration;
        // it missed the new process's mirrored Start and Running heartbeats.
        fs.master_monitor
            .set_term_for_test(fs.master_monitor.term() + 1);
        current.worker_session_id = Some("current-session".into());
        current.capacity_snapshot_seq = Some(50);
        assert_eq!(
            MasterHandler::process_worker_heartbeat(fs.clone(), current.clone())
                .unwrap()
                .registration_required,
            Some(true)
        );
        let mut start = current.clone();
        start.status = HeartbeatStatus::Start.into();
        start.capacity_snapshot_seq = None;
        for _ in 0..2 {
            // lost Start response: retry must be harmless
            MasterHandler::process_worker_heartbeat(fs.clone(), start.clone()).unwrap();
            assert!(!fs.worker_manager.read().get_worker(7).unwrap().is_live());
        }
        MasterHandler::process_block_report(
            fs.clone(),
            None,
            BlockReportListRequest {
                cluster_id: current.cluster_id.clone(),
                worker_id: 7,
                full_report: true,
                total_len: 0,
                report_id: Some(123),
                worker_session_id: current.worker_session_id.clone(),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(fs.full_report_completed_round(7), Some(123));
        current.capacity_snapshot_seq = Some(51);
        let response =
            MasterHandler::process_worker_heartbeat(fs.clone(), current.clone()).unwrap();
        assert_ne!(response.registration_required, Some(true));
        assert!(fs.worker_manager.read().get_worker(7).unwrap().is_live());
        cache_fence_create(&fs, "/recovered-cache", false, true);
        current.capacity_snapshot_seq = Some(52);
        let token = MasterHandler::process_worker_heartbeat(fs.clone(), current.clone())
            .unwrap()
            .cache_reservation_fence
            .unwrap();
        current.capacity_snapshot_seq = Some(53);
        current.cache_reservation_fence_ack = Some(token);
        MasterHandler::process_worker_heartbeat(fs.clone(), current).unwrap();
        assert_eq!(cache_fence_credit(&fs), 0);
    }

    #[test]
    fn cache_fence_legacy_running_cannot_replace_accepted_start() {
        for snapshot in [None, Some(0)] {
            for old_session in [None, Some(""), Some("old-session")] {
                let (fs, mut current) = cache_fence_fixture();
                fs.master_monitor
                    .set_role_for_test(curvine_raft::raft::RoleState::Leader);
                current.worker_session_id = Some("new-session".into());
                current.status = HeartbeatStatus::Start.into();
                current.capacity_snapshot_seq = None;
                let response =
                    MasterHandler::process_worker_heartbeat(fs.clone(), current.clone()).unwrap();
                let mut report = cache_report_request(&current);
                report.full_report_token = response.full_report_token;
                assert!(report.full_report_token.is_some());
                // The dangerous window: Start accepted, inventory not yet sent,
                // and no capacity sequence has been established for the new process.
                let before = fs.worker_manager.read().get_worker(7).unwrap().clone();
                let mut stale = current.clone();
                stale.status = HeartbeatStatus::Running.into();
                stale.worker_session_id = old_session.map(str::to_owned);
                stale.capacity_snapshot_seq = snapshot;
                stale.storages[0].available = 0;
                let err =
                    MasterHandler::process_worker_heartbeat(fs.clone(), stale.clone()).unwrap_err();
                assert!(err.to_string().starts_with("NotRegistered:"));
                {
                    let wm = fs.worker_manager.read();
                    let worker = wm.get_worker(7).unwrap();
                    assert_eq!(worker.worker_session_id, "new-session");
                    assert_eq!(worker.status, before.status);
                    assert_eq!(worker.last_update, before.last_update);
                    assert_eq!(worker.available, before.available);
                    assert_eq!(worker.scheduled_bytes, before.scheduled_bytes);
                    assert_eq!(worker.cache_reservation_fence.snapshot_seq, 0);
                    assert!(worker.registration_accepted);
                }
                MasterHandler::process_block_report(fs.clone(), None, report).unwrap();
                // Matching legacy Running is allowed, and keeps protection after
                // becoming live, being marked lost, and recovering with no Start.
                current.status = HeartbeatStatus::Running.into();
                for lost in [false, true, false] {
                    if lost {
                        fs.worker_manager
                            .write()
                            .worker_map
                            .remove_expired(7)
                            .unwrap();
                    }
                    assert!(
                        MasterHandler::process_worker_heartbeat(fs.clone(), stale.clone()).is_err()
                    );
                    MasterHandler::process_worker_heartbeat(fs.clone(), current.clone()).unwrap();
                    let wm = fs.worker_manager.read();
                    let worker = wm.get_worker(7).unwrap();
                    assert!(worker.is_live());
                    assert!(worker.registration_accepted);
                    assert_eq!(worker.worker_session_id, "new-session");
                }
            }
        }
    }

    #[test]
    fn cache_fence_legacy_auto_registration_without_start_remains_supported() {
        let (fs, mut h) = cache_fence_fixture();
        fs.worker_manager
            .write()
            .worker_map
            .remove(&ProtoUtils::worker_address_from_pb(&h.address));
        h.capacity_snapshot_seq = None;
        // With no accepted Start, old clients retain their auto-registration
        // behavior, including process changes and session-less heartbeats.
        for session in [Some("legacy-a"), Some("legacy-b"), None] {
            h.worker_session_id = session.map(str::to_owned);
            MasterHandler::process_worker_heartbeat(fs.clone(), h.clone()).unwrap();
            let wm = fs.worker_manager.read();
            let worker = wm.get_worker(7).unwrap();
            assert!(worker.is_live());
            assert!(!worker.registration_accepted);
            assert_eq!(worker.worker_session_id, session.unwrap_or_default());
        }
    }

    #[test]
    fn cache_fence_passive_start_protects_session_after_promotion() {
        use curvine_raft::raft::{FollowerReadinessMonitor, RoleState};
        use curvine_runtime::sync::StateCtl;

        let (mut fs, mut current) = cache_fence_fixture();
        let readiness = FollowerReadinessMonitor::default();
        readiness.observe_raft_state(RoleState::Follower, 1, 2, 0);
        fs.master_monitor = crate::master::MasterMonitor::with_follower_readiness(
            StateCtl::new(RoleState::Follower.into()),
            StateCtl::new(0),
            readiness,
        );
        current.status = HeartbeatStatus::Start.into();
        current.capacity_snapshot_seq = None;
        current.worker_session_id = Some("mirrored-session".into());
        MasterHandler::process_worker_heartbeat_passive(fs.clone(), current.clone()).unwrap();
        assert!(
            fs.worker_manager
                .read()
                .get_worker(7)
                .unwrap()
                .registration_accepted
        );
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);
        fs.master_monitor
            .set_term_for_test(fs.master_monitor.term() + 1);
        let mut stale = current.clone();
        stale.status = HeartbeatStatus::Running.into();
        stale.worker_session_id = Some("previous-session".into());
        assert!(MasterHandler::process_worker_heartbeat(fs.clone(), stale).is_err());
        current.status = HeartbeatStatus::Running.into();
        MasterHandler::process_worker_heartbeat(fs.clone(), current).unwrap();
        assert_eq!(
            fs.worker_manager
                .read()
                .get_worker(7)
                .unwrap()
                .worker_session_id,
            "mirrored-session"
        );
    }

    #[test]
    fn inventory_recovery_heartbeat_redelivery_keeps_liveness_capacity_and_delete_commands() {
        let (fs, mut h) = cache_fence_fixture();
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);
        fs.master_monitor
            .set_term_for_test(fs.master_monitor.term() + 1);
        fs.worker_manager.write().remove_block(7, 999);
        h.capacity_snapshot_seq = Some(2);
        h.storages[0].available -= 4 << 20;
        let beat = |h: WorkerHeartbeatRequest, now| {
            MasterHandler::process_worker_heartbeat_with_clocks(
                fs.clone(),
                h,
                curvine_runtime::common::LocalTime::mills,
                || now,
            )
            .unwrap()
        };
        let first = beat(h.clone(), 100);
        assert_eq!(first.registration_required, Some(true));
        assert!(first.cmds.is_empty());
        {
            let wm = fs.worker_manager.read();
            let worker = wm.get_worker(7).unwrap();
            assert_eq!(worker.available, (1 << 30) - (4 << 20));
            assert!(worker.is_live());
            assert_eq!(worker.cache_reservation_fence.snapshot_seq, 2);
        }
        // Lost response: the same snapshot is ignored, a fresh beat updates
        // liveness and preserves pending commands, and the due beat retries.
        assert_ne!(beat(h.clone(), 3_100).registration_required, Some(true));
        h.capacity_snapshot_seq = Some(3);
        let early = beat(h.clone(), 101);
        assert_ne!(early.registration_required, Some(true));
        assert!(!early.cmds.is_empty());
        h.capacity_snapshot_seq = Some(4);
        assert_eq!(beat(h.clone(), 3_100).registration_required, Some(true));
        h.capacity_snapshot_seq = Some(5);
        assert!(
            !beat(h.clone(), 3_101).cmds.is_empty(),
            "recovery replies must not drain deletion commands"
        );
        // A replacement process gets its own reporting round; stale recovery
        // requests and delayed heartbeats cannot overwrite it.
        let mut start = h.clone();
        start.status = HeartbeatStatus::Start.into();
        start.worker_session_id = Some("replacement".into());
        start.capacity_snapshot_seq = None;
        let response = MasterHandler::process_worker_heartbeat(fs.clone(), start.clone()).unwrap();
        let token = response.full_report_token.unwrap();
        let rejected = beat(h, 10_000);
        assert_eq!(rejected.registration_required, Some(true));
        let wm = fs.worker_manager.read();
        let worker = wm.get_worker(7).unwrap();
        assert_eq!(worker.worker_session_id, "replacement");
        assert!(worker
            .inventory_recovery
            .matches(fs.inventory_epoch(), &token));
    }

    #[test]
    fn inventory_recovery_promoted_matching_session_requires_fresh_inventory() {
        let (fs, mut h) = cache_fence_fixture();
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);
        let old_epoch = fs.inventory_epoch();
        {
            let mut wm = fs.worker_manager.write();
            let state = &mut wm
                .worker_map
                .workers
                .get_mut(&7)
                .unwrap()
                .inventory_recovery;
            state.start(old_epoch, "previous-leader".into(), 0, 3_000);
            state.reconciling(old_epoch, "previous-leader");
            state.complete(old_epoch, "previous-leader");
        }
        fs.master_monitor
            .set_term_for_test(fs.master_monitor.term() + 1);
        h.capacity_snapshot_seq = Some(2);
        assert_eq!(
            MasterHandler::process_worker_heartbeat(fs.clone(), h.clone())
                .unwrap()
                .registration_required,
            Some(true)
        );
        let mut start = h.clone();
        start.status = HeartbeatStatus::Start.into();
        start.capacity_snapshot_seq = None;
        let reply = MasterHandler::process_worker_heartbeat(fs.clone(), start.clone()).unwrap();
        let mut report = cache_report_request(&start);
        report.full_report_token = reply.full_report_token;
        MasterHandler::process_block_report(fs.clone(), None, report.clone()).unwrap();
        // Simulate a lost final-report response: the same round can be retried.
        MasterHandler::process_block_report(fs.clone(), None, report).unwrap();
        h.capacity_snapshot_seq = Some(3);
        assert_ne!(
            MasterHandler::process_worker_heartbeat(fs.clone(), h.clone())
                .unwrap()
                .registration_required,
            Some(true)
        );
        assert_eq!(
            fs.worker_manager
                .read()
                .get_worker(7)
                .unwrap()
                .worker_session_id,
            "session"
        );
    }

    #[test]
    fn cache_fence_ack_timeout_scales_with_configured_heartbeat() {
        for (interval, timeout) in [(1000, 60_000), (60_000, 300_000), (120_000, 600_000)] {
            for elapsed in [interval, timeout - 1, timeout] {
                let (mut fs, mut h) = cache_fence_fixture();
                let mut conf = (*fs.conf).clone();
                conf.heartbeat_interval = format!("{interval}ms");
                conf.worker_blacklist_interval = "10m".into();
                conf.worker_lost_interval = "20m".into();
                conf.init().unwrap();
                assert_eq!(conf.heartbeat_interval_ms(), interval);
                fs.conf = Arc::new(conf);
                cache_fence_create(&fs, "/slow-heartbeat-cache", false, true);
                h.capacity_snapshot_seq = Some(2);
                let token = MasterHandler::process_worker_heartbeat_with_clock(
                    fs.clone(),
                    h.clone(),
                    || 100,
                )
                .unwrap()
                .cache_reservation_fence
                .unwrap();
                h.capacity_snapshot_seq = Some(3);
                h.cache_reservation_fence_ack = Some(token);
                MasterHandler::process_worker_heartbeat_with_clock(fs.clone(), h, || 100 + elapsed)
                    .unwrap();
                assert_eq!(
                    cache_fence_credit(&fs),
                    if elapsed < timeout { 0 } else { 4 << 20 },
                    "interval={interval}, elapsed={elapsed}"
                );
            }
        }
        assert_eq!(
            MasterHandler::cache_reservation_ack_timeout_ms(u64::MAX),
            u64::MAX
        );
    }

    #[test]
    fn cache_fence_missing_registration_requires_start_before_running() {
        let (fs, h) = cache_fence_fixture();
        fs.worker_manager
            .write()
            .worker_map
            .remove(&ProtoUtils::worker_address_from_pb(&h.address));
        let response = MasterHandler::process_worker_heartbeat(fs.clone(), h.clone()).unwrap();
        assert_eq!(response.registration_required, Some(true));
        assert!(fs.worker_manager.read().get_worker(7).is_none());
        let mut start = h.clone();
        start.status = HeartbeatStatus::Start.into();
        start.capacity_snapshot_seq = None;
        MasterHandler::process_worker_heartbeat(fs.clone(), start).unwrap();
        assert_ne!(
            MasterHandler::process_worker_heartbeat(fs.clone(), h)
                .unwrap()
                .registration_required,
            Some(true)
        );
        assert!(fs.worker_manager.read().get_worker(7).unwrap().is_live());
    }

    #[test]
    fn cache_fence_same_session_reregistration_discards_token_but_preserves_sequence() {
        let (fs, mut h) = cache_fence_fixture();
        cache_fence_create(&fs, "/before-reregister", false, true);
        h.capacity_snapshot_seq = Some(2);
        let token = MasterHandler::process_worker_heartbeat(fs.clone(), h.clone())
            .unwrap()
            .cache_reservation_fence
            .unwrap();
        let mut start = h.clone();
        start.status = HeartbeatStatus::Start.into();
        start.capacity_snapshot_seq = None;
        for _ in 0..2 {
            MasterHandler::process_worker_heartbeat(fs.clone(), start.clone()).unwrap();
            let wm = fs.worker_manager.read();
            let worker = wm.get_worker(7).unwrap();
            assert!(!worker.is_live());
            assert_eq!(worker.scheduled_bytes, 4 << 20);
            assert_eq!(worker.cache_reservation_fence.snapshot_seq, 2);
        }
        h.cache_reservation_fence_ack = Some(token);
        MasterHandler::process_worker_heartbeat(fs.clone(), h.clone()).unwrap();
        assert!(
            !fs.worker_manager.read().get_worker(7).unwrap().is_live(),
            "replayed snapshot must not restore readiness"
        );
        h.capacity_snapshot_seq = Some(3);
        let response = MasterHandler::process_worker_heartbeat(fs.clone(), h).unwrap();
        assert!(response.cache_reservation_fence.is_none());
        assert_eq!(
            cache_fence_credit(&fs),
            4 << 20,
            "old token cannot release credit"
        );
        assert!(fs.worker_manager.read().get_worker(7).unwrap().is_live());
    }

    #[test]
    fn cache_fence_final_flush_completion_and_worker_restart() {
        let (fs, mut h) = cache_fence_fixture();
        cache_fence_create(&fs, "/cache", true, true);
        fs.complete_file("/cache", None, 4 << 20, vec![], "", false, None)
            .unwrap();
        h.capacity_snapshot_seq = Some(2);
        let old = MasterHandler::process_worker_heartbeat(fs.clone(), h.clone())
            .unwrap()
            .cache_reservation_fence
            .unwrap();
        h.worker_session_id = Some("different".into());
        h.capacity_snapshot_seq = Some(3);
        assert_eq!(
            MasterHandler::process_worker_heartbeat(fs.clone(), h.clone())
                .unwrap()
                .registration_required,
            Some(true)
        );
        assert_eq!(cache_fence_credit(&fs), 4 << 20);
        h.status = HeartbeatStatus::Start.into();
        h.capacity_snapshot_seq = None;
        MasterHandler::process_worker_heartbeat(fs.clone(), h.clone()).unwrap();
        h.status = HeartbeatStatus::Running.into();
        h.capacity_snapshot_seq = Some(1);
        MasterHandler::process_worker_heartbeat(fs.clone(), h.clone()).unwrap();
        cache_fence_create(&fs, "/new-cache", false, true);
        h.capacity_snapshot_seq = Some(2);
        h.cache_reservation_fence_ack = Some(old);
        MasterHandler::process_worker_heartbeat(fs.clone(), h).unwrap();
        assert_eq!(
            cache_fence_credit(&fs),
            4 << 20,
            "old session cannot release new credit"
        );
    }
    #[test]
    fn cache_fence_publication_after_rename_and_inline_completion() {
        let (fs, mut h) = cache_fence_fixture();
        cache_fence_create(&fs, "/tmp/load", false, false);
        fs.rename(
            "/tmp/load",
            "/tmp/published",
            curvine_model::RenameFlags::new(0),
        )
        .unwrap();
        fs.set_attr(
            "/tmp/published",
            curvine_model::SetAttrOptsBuilder::new()
                .ufs_mtime(42)
                .build(),
        )
        .unwrap();
        // A flush can publish metadata but only final completion can enroll it.
        cache_fence_create(&fs, "/inline", true, false);
        fs.complete_file(
            "/inline",
            None,
            4 << 20,
            vec![],
            "",
            false,
            Some(
                curvine_model::SetAttrOptsBuilder::new()
                    .ufs_mtime(43)
                    .build(),
            ),
        )
        .unwrap();
        h.capacity_snapshot_seq = Some(2);
        let token = MasterHandler::process_worker_heartbeat(fs.clone(), h.clone())
            .unwrap()
            .cache_reservation_fence
            .unwrap();
        h.capacity_snapshot_seq = Some(3);
        h.cache_reservation_fence_ack = Some(token);
        MasterHandler::process_worker_heartbeat(fs.clone(), h).unwrap();
        assert_eq!(cache_fence_credit(&fs), 0);
    }

    #[test]
    fn cache_fence_promotion_discards_old_batch_and_creation_eligibility() {
        let (fs, mut h) = cache_fence_fixture();
        cache_fence_create(&fs, "/old-cache", false, true);
        cache_fence_create(&fs, "/not-yet-published", false, false);
        h.capacity_snapshot_seq = Some(2);
        let token = MasterHandler::process_worker_heartbeat(fs.clone(), h.clone())
            .unwrap()
            .cache_reservation_fence
            .unwrap();
        fs.master_monitor
            .set_term_for_test(fs.master_monitor.term() + 1);
        fs.set_attr(
            "/not-yet-published",
            curvine_model::SetAttrOptsBuilder::new()
                .ufs_mtime(42)
                .build(),
        )
        .unwrap();
        h.capacity_snapshot_seq = Some(3);
        h.cache_reservation_fence_ack = Some(token);
        assert!(MasterHandler::process_worker_heartbeat(fs.clone(), h)
            .unwrap()
            .cache_reservation_fence
            .is_none());
        assert_eq!(cache_fence_credit(&fs), 8 << 20);
    }

    #[test]
    fn cache_fence_resize_cancels_sent_confirmation() {
        let (fs, mut h) = cache_fence_fixture();
        cache_fence_create(&fs, "/cache", false, true);
        h.capacity_snapshot_seq = Some(2);
        let token = MasterHandler::process_worker_heartbeat(fs.clone(), h.clone())
            .unwrap()
            .cache_reservation_fence
            .unwrap();
        fs.resize(
            "/cache",
            curvine_model::FileAllocOpts::with_truncate(2 << 20),
        )
        .unwrap();
        let before = cache_fence_credit(&fs);
        h.capacity_snapshot_seq = Some(3);
        h.cache_reservation_fence_ack = Some(token);
        MasterHandler::process_worker_heartbeat(fs.clone(), h).unwrap();
        assert_eq!(cache_fence_credit(&fs), before);
    }
    #[test]
    fn cache_fence_lost_challenge_and_ack_responses_recover_through_heartbeats() {
        let (fs, mut h) = cache_fence_fixture();
        cache_fence_create(&fs, "/cache/a", false, true);
        h.capacity_snapshot_seq = Some(2);
        let first = MasterHandler::process_worker_heartbeat(fs.clone(), h.clone())
            .unwrap()
            .cache_reservation_fence
            .unwrap();
        // First response is lost: the worker sends no acknowledgement next.
        cache_fence_create(&fs, "/cache/b", false, true);
        h.capacity_snapshot_seq = Some(3);
        let retry = MasterHandler::process_worker_heartbeat(fs.clone(), h.clone())
            .unwrap()
            .cache_reservation_fence
            .unwrap();
        assert_eq!(retry, first);
        assert_eq!(cache_fence_credit(&fs), 8 << 20);
        h.capacity_snapshot_seq = Some(4);
        h.cache_reservation_fence_ack = Some(first.clone());
        let second = MasterHandler::process_worker_heartbeat(fs.clone(), h.clone())
            .unwrap()
            .cache_reservation_fence
            .unwrap();
        assert_ne!(second, first);
        assert_eq!(cache_fence_credit(&fs), 4 << 20);
        // Response to the successful ack is lost. Worker repeats the old token.
        h.capacity_snapshot_seq = Some(5);
        assert_eq!(
            MasterHandler::process_worker_heartbeat(fs.clone(), h.clone())
                .unwrap()
                .cache_reservation_fence,
            Some(second.clone())
        );
        assert_eq!(cache_fence_credit(&fs), 4 << 20);
        h.capacity_snapshot_seq = Some(6);
        h.cache_reservation_fence_ack = Some(second);
        MasterHandler::process_worker_heartbeat(fs.clone(), h).unwrap();
        assert_eq!(cache_fence_credit(&fs), 0);
    }

    #[test]
    fn cache_fence_unwritten_c_is_protected_before_and_after_challenge() {
        for challenge_first in [false, true] {
            let (fs, mut h) = cache_fence_fixture();
            fs.create("/C", true).unwrap();
            fs.add_block(
                "/C",
                None,
                curvine_model::ClientAddress::default(),
                vec![],
                vec![],
                0,
                None,
            )
            .unwrap();
            let c_promised = cache_fence_credit(&fs);
            cache_fence_create(&fs, "/B", false, true);
            h.capacity_snapshot_seq = Some(2);
            if challenge_first {
                h.cache_reservation_fence_ack =
                    MasterHandler::process_worker_heartbeat(fs.clone(), h.clone())
                        .unwrap()
                        .cache_reservation_fence;
                assert!(h.cache_reservation_fence_ack.is_some());
                h.capacity_snapshot_seq = Some(3);
            }
            h.storages[0].available -= 4 << 20; // B's usage; C is still unwritten
            let response = MasterHandler::process_worker_heartbeat(fs.clone(), h.clone()).unwrap();
            assert!(response.cache_reservation_fence.is_none());
            assert_eq!(cache_fence_credit(&fs), c_promised);
            h.capacity_snapshot_seq = Some(4);
            MasterHandler::process_worker_heartbeat(fs.clone(), h.clone()).unwrap();
            let wm = fs.worker_manager.read();
            let worker = wm.get_worker(7).unwrap();
            assert_eq!(
                worker.allocatable_available(),
                (1 << 30) - (4 << 20) - c_promised
            );
        }
    }
    #[test]
    fn cache_fence_first_late_heartbeat_expires_before_ack() {
        let timeout = WorkerInfo::CACHE_RESERVATION_ACK_TIMEOUT_MS;
        for elapsed in [timeout - 1, timeout, timeout + 1000] {
            let (fs, mut h) = cache_fence_fixture();
            cache_fence_create(&fs, "/cache/a", false, true);
            h.capacity_snapshot_seq = Some(2);
            let token =
                MasterHandler::process_worker_heartbeat_with_clock(fs.clone(), h.clone(), || 100)
                    .unwrap()
                    .cache_reservation_fence
                    .unwrap();
            // A later completion is not part of the original immutable batch.
            cache_fence_create(&fs, "/cache/b", false, true);
            h.capacity_snapshot_seq = Some(3);
            h.cache_reservation_fence_ack = Some(token);
            // First heartbeat after issuance: availability is unchanged, and
            // the ten-minute reservation expiry has not elapsed.
            let next =
                MasterHandler::process_worker_heartbeat_with_clock(fs.clone(), h.clone(), || {
                    100 + elapsed
                })
                .unwrap()
                .cache_reservation_fence
                .unwrap();
            let residue = if elapsed >= timeout { 4 << 20 } else { 0 };
            assert_eq!(cache_fence_credit(&fs), residue + (4 << 20));
            h.capacity_snapshot_seq = Some(4);
            // Duplicate old ACK must neither confirm nor expire the new batch.
            let retry =
                MasterHandler::process_worker_heartbeat_with_clock(fs.clone(), h.clone(), || {
                    101 + elapsed
                })
                .unwrap()
                .cache_reservation_fence
                .unwrap();
            assert_eq!(retry, next);
            assert_eq!(cache_fence_credit(&fs), residue + (4 << 20));
            h.capacity_snapshot_seq = Some(5);
            h.cache_reservation_fence_ack = Some(next);
            let response =
                MasterHandler::process_worker_heartbeat_with_clock(fs.clone(), h, || 102 + elapsed)
                    .unwrap();
            assert!(response.cache_reservation_fence.is_none());
            assert_eq!(cache_fence_credit(&fs), residue);
        }
    }
}
