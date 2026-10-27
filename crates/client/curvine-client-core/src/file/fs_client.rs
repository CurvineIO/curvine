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

use crate::file::{FsContext, MasterHandshake, SessionMutationGuard};
use bytes::BytesMut;
use curvine_config::{ClientConf, ClusterConf, UfsConf, UfsConfBuilder};
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::{Path, RpcCode};
use curvine_model::ProtoUtils;
use curvine_model::*;
use curvine_proto::*;
use curvine_rpc::client::ClusterConnector;
use curvine_rpc::message::{BoxMessage, Message, MessageBuilder, FOLLOWER_READ_SEQ_ID};
use curvine_runtime::runtime::RpcRuntime;
use log::warn;
use prost::Message as PMessage;
use std::collections::LinkedList;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::time::Instant;

#[derive(Clone)]
pub struct FsClient {
    context: Arc<FsContext>,
    connector: Arc<ClusterConnector>,
    follower_peer_cursor: Arc<AtomicUsize>,
}

struct CompleteFileOptions {
    only_flush: bool,
    set_attr_opts: Option<SetAttrOpts>,
    return_file_blocks: bool,
}

// Keep path parsing for large batches bounded too. Larger or malformed batches
// fall back to pinning the full namespace for one sticky window.
const MAX_ENCODED_MUTATION_PATHS: usize = 1_024;
const MAX_ENCODED_MUTATION_PATH_BYTES: usize = 4 * 1_024;
const FOLLOWER_RPC_DEADLINE_FRACTION: u32 = 2;

/// RAII guard for a claimed one-time handshake report. Resets the claim when
/// dropped without being committed, so a cancelled or failed in-flight
/// `GetFilesystemInfo` future never permanently suppresses `component_info`
/// reporting.
struct HandshakeReportGuard<'a> {
    context: &'a FsContext,
    committed: bool,
}

impl HandshakeReportGuard<'_> {
    /// Mark the report as delivered; the one-time claim stays set.
    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for HandshakeReportGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.context.reset_handshake_report();
        }
    }
}

impl FsClient {
    pub fn new(context: Arc<FsContext>) -> Self {
        let connector = context.connector.clone();
        Self {
            context,
            connector,
            follower_peer_cursor: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn context(&self) -> &Arc<FsContext> {
        &self.context
    }

    pub fn conf(&self) -> &ClusterConf {
        &self.context.conf
    }

    fn mutation_guard<'a>(
        &self,
        paths: impl IntoIterator<Item = &'a Path>,
    ) -> Option<SessionMutationGuard> {
        self.context
            .conf
            .client
            .follower_reads_enabled
            .then(|| self.context.session_path_tracker().begin_mutation(paths))
    }

    fn encoded_mutation_guard(
        &self,
        paths: impl IntoIterator<Item = String>,
    ) -> Option<SessionMutationGuard> {
        if !self.context.conf.client.follower_reads_enabled {
            return None;
        }

        let mut parsed = Vec::new();
        let mut use_global_fallback = false;
        for path in paths {
            if parsed.len() >= MAX_ENCODED_MUTATION_PATHS {
                use_global_fallback = true;
                break;
            }
            if path.len() > MAX_ENCODED_MUTATION_PATH_BYTES {
                use_global_fallback = true;
                break;
            }
            match Path::from_str(path) {
                Ok(path) => parsed.push(path),
                Err(_) => {
                    use_global_fallback = true;
                    break;
                }
            }
        }
        if use_global_fallback {
            // The batch is too large to retain safely or contains a path that
            // the Path parser cannot interpret. Pin the full namespace instead
            // of dropping an uncertain mutation mark.
            parsed.clear();
            if let Ok(root) = Path::from_str("/") {
                parsed.push(root);
            }
        }
        Some(
            self.context
                .session_path_tracker()
                .begin_mutation(parsed.iter()),
        )
    }

    fn all_metadata_paths_mutation_guard(&self) -> Option<SessionMutationGuard> {
        if self.context.conf.client.follower_reads_enabled {
            if let Ok(root) = Path::from_str("/") {
                return Some(self.context.session_path_tracker().begin_mutation([&root]));
            }
        }
        None
    }

    pub async fn mkdir(&self, path: &Path, opts: MkdirOpts) -> FsResult<FileStatus> {
        let _mutation_guard = self.mutation_guard([path]);
        let header = MkdirRequest {
            path: path.encode(),
            opts: ProtoUtils::mkdir_opts_to_pb(opts),
        };

        let rep_header: MkdirResponse = self.rpc(RpcCode::Mkdir, header).await?;
        Ok(ProtoUtils::file_status_from_pb(rep_header.status))
    }

    pub async fn create(
        &self,
        path: &Path,
        create_parent: bool,
        overwrite: bool,
    ) -> FsResult<FileStatus> {
        let opts = CreateFileOptsBuilder::new()
            .create_parent(create_parent)
            .build();

        self.create_with_opts(path, opts, overwrite).await
    }

    pub async fn create_files_batch(
        &self,
        requests: Vec<(String, CreateFileOpts, OpenFlags)>,
    ) -> FsResult<Vec<FileStatus>> {
        let _mutation_guard =
            self.encoded_mutation_guard(requests.iter().map(|(path, _, _)| path.clone()));
        let pb_requests: Vec<CreateFileRequest> = requests
            .into_iter()
            .map(|(path, opts, flags)| CreateFileRequest {
                path,
                opts: ProtoUtils::create_opts_to_pb(opts, self.context.clone_client_name()),
                flags: flags.value(),
            })
            .collect();

        let header = CreateFilesBatchRequest {
            requests: pb_requests,
        };

        let rep: CreateFilesBatchResponse = self.rpc(RpcCode::CreateFilesBatch, header).await?;
        Ok(rep
            .file_statuses
            .into_iter()
            .map(ProtoUtils::file_status_from_pb)
            .collect())
    }

    pub async fn create_with_opts(
        &self,
        path: &Path,
        opts: CreateFileOpts,
        overwrite: bool,
    ) -> FsResult<FileStatus> {
        let _mutation_guard = self.mutation_guard([path]);
        let flags = OpenFlags::new_write_only()
            .set_create(true)
            .set_overwrite(overwrite);
        let header = CreateFileRequest {
            path: path.encode(),
            opts: ProtoUtils::create_opts_to_pb(opts, self.context.clone_client_name()),
            flags: flags.value(),
        };

        let rep_header: CreateFileResponse = self.rpc(RpcCode::CreateFile, header).await?;
        let status = ProtoUtils::file_status_from_pb(rep_header.file_status);
        Ok(status)
    }

    pub async fn open_with_opts(
        &self,
        path: &Path,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileBlocks> {
        // Any non-read-only access mode or status bit could create or mutate
        // metadata. Treat unknown flag bits conservatively as a write too.
        let _mutation_guard = if flags.bits() != OpenFlags::RDONLY.bits() {
            self.mutation_guard([path])
        } else {
            None
        };
        let header = OpenFileRequest {
            path: path.encode(),
            opts: ProtoUtils::create_opts_to_pb(opts, self.context.clone_client_name()),
            flags: flags.value(),
        };
        let rep_header: OpenFileResponse = self.rpc(RpcCode::OpenFile, header).await?;
        let status = ProtoUtils::file_blocks_from_pb(rep_header.file_blocks);
        Ok(status)
    }

    pub async fn file_status(&self, path: &Path) -> FsResult<FileStatus> {
        self.file_status0(path, None).await
    }

    pub async fn file_status_by_id(&self, path: &Path, inode_id: i64) -> FsResult<FileStatus> {
        self.file_status0(path, Some(inode_id)).await
    }

    async fn file_status0(&self, path: &Path, inode_id: Option<i64>) -> FsResult<FileStatus> {
        let header = GetFileStatusRequest {
            path: path.encode(),
            inode_id,
        };

        let rep_header: GetFileStatusResponse = self.rpc(RpcCode::FileStatus, header).await?;
        let status = ProtoUtils::file_status_from_pb(rep_header.status);
        Ok(status)
    }

    pub async fn file_status_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        let header = GetFileStatusRequest {
            path: path.encode(),
            inode_id: None,
        };
        self.rpc_bytes(RpcCode::FileStatus, header).await
    }

    pub async fn exists(&self, path: &Path) -> FsResult<bool> {
        let header = ExistsRequest {
            path: path.encode(),
        };

        let rep_header: ExistsResponse = self.rpc(RpcCode::Exists, header).await?;
        Ok(rep_header.exists)
    }

    pub async fn delete(&self, path: &Path, recursive: bool) -> FsResult<DeleteResult> {
        self.delete_with_opts(path, recursive, false).await
    }

    pub async fn delete_with_opts(
        &self,
        path: &Path,
        recursive: bool,
        skip_trash: bool,
    ) -> FsResult<DeleteResult> {
        let _mutation_guard = self.mutation_guard([path]);
        let header = DeleteRequest {
            path: path.encode(),
            recursive,
            skip_trash: Some(skip_trash),
        };

        let rep: DeleteResponse = self.rpc(RpcCode::Delete, header).await?;
        let trashed = rep.trashed();
        let mut res = ProtoUtils::delete_res_from_pb(rep.res.unwrap_or_default());
        res.trashed = trashed;
        Ok(res)
    }

    pub async fn list_trash_page(
        &self,
        page_token: Option<String>,
        page_size: Option<u32>,
    ) -> FsResult<(Vec<TrashEntry>, Option<String>)> {
        let req = ListTrashRequest {
            page_token,
            page_size,
        };
        let rep: ListTrashResponse = self.rpc(RpcCode::ListTrash, req).await?;
        let entries = rep
            .entries
            .into_iter()
            .map(ProtoUtils::trash_entry_from_pb)
            .collect();
        let next_page_token = rep.next_page_token.filter(|token| !token.is_empty());
        Ok((entries, next_page_token))
    }

    pub async fn list_trash(&self) -> FsResult<Vec<TrashEntry>> {
        let mut entries = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let (page, next_token) = self.list_trash_page(page_token, None).await?;
            entries.extend(page);
            match next_token {
                Some(token) => page_token = Some(token),
                None => return Ok(entries),
            }
        }
    }

    pub async fn restore_trash(
        &self,
        path: &Path,
        dst: Option<&Path>,
        overwrite: bool,
    ) -> FsResult<RestoreTrashResult> {
        let _mutation_guard = if let Some(destination) = dst {
            self.mutation_guard([path, destination])
        } else {
            // The request asks the master to restore the original path, which
            // is not present in this request. Keep namespace reads on the
            // leader until that destination can no longer be stale.
            self.all_metadata_paths_mutation_guard()
        };
        let req = RestoreTrashRequest {
            path: path.encode(),
            dst: dst.map(|value| value.encode()),
            overwrite: Some(overwrite),
        };
        let rep: RestoreTrashResponse = self.rpc(RpcCode::RestoreTrash, req).await?;
        Ok(ProtoUtils::restore_trash_res_from_pb(rep))
    }

    pub async fn empty_trash(&self) -> FsResult<DeleteResult> {
        // EmptyTrash has no path list. It can affect any namespace path that
        // was moved to trash, so pin reads globally for the sticky window.
        let _mutation_guard = self.all_metadata_paths_mutation_guard();
        let req = EmptyTrashRequest {};
        let rep: EmptyTrashResponse = self.rpc(RpcCode::EmptyTrash, req).await?;
        Ok(ProtoUtils::delete_res_from_pb(rep.res))
    }

    pub async fn free(&self, path: &Path, recursive: bool) -> FsResult<FreeResult> {
        let _mutation_guard = self.mutation_guard([path]);
        let header = FreeRequest {
            path: path.encode(),
            recursive,
        };

        let rep: FreeResponse = self.rpc(RpcCode::Free, header).await?;
        Ok(ProtoUtils::free_res_from_pb(rep.res))
    }

    pub async fn rename(&self, src: &Path, dst: &Path, flags: RenameFlags) -> FsResult<bool> {
        let _mutation_guard = self.mutation_guard([src, dst]);
        let header = RenameRequest {
            src: src.encode(),
            dst: dst.encode(),
            flags: flags.value(),
        };

        let rep_header: RenameResponse = self.rpc(RpcCode::Rename, header).await?;
        Ok(rep_header.result)
    }

    pub async fn list_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        let header = ListStatusRequest {
            path: path.encode(),
            need_location: false,
        };

        let rep_header: ListStatusResponse = self.rpc(RpcCode::ListStatus, header).await?;

        let res = rep_header
            .statuses
            .into_iter()
            .map(ProtoUtils::file_status_from_pb)
            .collect();

        Ok(res)
    }

    pub async fn list_options(
        &self,
        path: &Path,
        options: ListOptions,
    ) -> FsResult<Vec<FileStatus>> {
        let header = ListOptionsRequest {
            path: path.encode(),
            options: ProtoUtils::list_options_to_pb(options),
        };

        let rep_header: ListOptionsResponse = self.rpc(RpcCode::ListOptions, header).await?;

        let res = rep_header
            .statuses
            .into_iter()
            .map(ProtoUtils::file_status_from_pb)
            .collect();

        Ok(res)
    }

    pub async fn list_options_bytes(
        &self,
        path: &Path,
        options: ListOptions,
    ) -> FsResult<BytesMut> {
        let header = ListOptionsRequest {
            path: path.encode(),
            options: ProtoUtils::list_options_to_pb(options),
        };

        self.rpc_bytes(RpcCode::ListOptions, header).await
    }

    pub async fn list_status_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        let header = ListStatusRequest {
            path: path.encode(),
            need_location: false,
        };

        self.rpc_bytes(RpcCode::ListStatus, header).await
    }

    pub async fn list_files(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        let mut res = Vec::with_capacity(32);

        let mut stack = LinkedList::new();
        stack.push_back(path.clone());
        while let Some(item) = stack.pop_front() {
            let statuses = self.list_status(&item).await?;
            for item in statuses {
                if item.is_dir {
                    stack.push_back(Path::from_str(&item.path)?);
                } else {
                    res.push(item);
                }
            }
        }

        Ok(res)
    }

    pub async fn add_block(
        &self,
        path: &Path,
        commit_blocks: Vec<CommitBlock>,
        file_len: i64,
        last_block: Option<ExtendedBlock>,
    ) -> FsResult<LocatedBlock> {
        self.add_block0(path, None, commit_blocks, file_len, last_block)
            .await
    }

    pub async fn add_block_by_id(
        &self,
        path: &Path,
        id: i64,
        commit_blocks: Vec<CommitBlock>,
        file_len: i64,
        last_block: Option<ExtendedBlock>,
    ) -> FsResult<LocatedBlock> {
        self.add_block0(path, Some(id), commit_blocks, file_len, last_block)
            .await
    }

    async fn add_block0(
        &self,
        path: &Path,
        inode_id: Option<i64>,
        commit_blocks: Vec<CommitBlock>,
        file_len: i64,
        last_block: Option<ExtendedBlock>,
    ) -> FsResult<LocatedBlock> {
        let _mutation_guard = self.mutation_guard([path]);
        let commit_blocks = commit_blocks
            .into_iter()
            .map(|v| ProtoUtils::commit_block_to_pb(v.clone()))
            .collect();

        let header = AddBlockRequest {
            path: path.encode(),
            commit_blocks,
            exclude_workers: self.context.exclude_workers(),
            located: true,
            client_address: self.context.client_addr_pb(),
            file_len,
            last_block: last_block.map(ProtoUtils::extend_block_to_pb),
            inode_id,
        };

        let rep_header =
            FsContext::metrics_track("AddBlock", self.rpc(RpcCode::AddBlock, header)).await?;
        let locate_block = ProtoUtils::located_block_from_pb(rep_header);
        Ok(locate_block)
    }

    pub async fn add_blocks_batch(&self, requests: Vec<String>) -> FsResult<Vec<LocatedBlock>> {
        let _mutation_guard = self.encoded_mutation_guard(requests.iter().cloned());
        let pb_requests: Vec<AddBlockRequest> = requests
            .into_iter()
            .map(|path| {
                let commit_blocks: Vec<CommitBlockProto> = Vec::new();
                AddBlockRequest {
                    path,
                    commit_blocks,
                    exclude_workers: self.context.exclude_workers(),
                    located: true,
                    client_address: self.context.client_addr_pb(),
                    file_len: 0,
                    last_block: None,
                    inode_id: None,
                }
            })
            .collect();

        let header = AddBlocksBatchRequest {
            requests: pb_requests,
        };
        let rep: AddBlocksBatchResponse = self.rpc(RpcCode::AddBlocksBatch, header).await?;
        Ok(rep
            .blocks
            .into_iter()
            .map(ProtoUtils::located_block_from_pb)
            .collect())
    }

    pub async fn complete_file(
        &self,
        path: &Path,
        len: i64,
        commit_blocks: impl IntoIterator<Item = CommitBlock>,
        only_flush: bool,
        set_attr_opts: Option<SetAttrOpts>,
    ) -> FsResult<Option<FileBlocks>> {
        self.complete_file0(
            path,
            None,
            len,
            commit_blocks,
            CompleteFileOptions {
                only_flush,
                set_attr_opts,
                return_file_blocks: true,
            },
        )
        .await
    }

    pub async fn complete_file_by_id(
        &self,
        path: &Path,
        inode_id: i64,
        len: i64,
        commit_blocks: impl IntoIterator<Item = CommitBlock>,
        only_flush: bool,
        set_attr_opts: Option<SetAttrOpts>,
    ) -> FsResult<Option<FileBlocks>> {
        self.complete_file0(
            path,
            Some(inode_id),
            len,
            commit_blocks,
            CompleteFileOptions {
                only_flush,
                set_attr_opts,
                return_file_blocks: true,
            },
        )
        .await
    }

    pub(crate) async fn flush_file_by_id(
        &self,
        path: &Path,
        inode_id: i64,
        len: i64,
        commit_blocks: impl IntoIterator<Item = CommitBlock>,
    ) -> FsResult<()> {
        self.complete_file0(
            path,
            Some(inode_id),
            len,
            commit_blocks,
            CompleteFileOptions {
                only_flush: true,
                set_attr_opts: None,
                return_file_blocks: false,
            },
        )
        .await
        .map(|_| ())
    }

    // File writing is completed.
    async fn complete_file0(
        &self,
        path: &Path,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: impl IntoIterator<Item = CommitBlock>,
        options: CompleteFileOptions,
    ) -> FsResult<Option<FileBlocks>> {
        let _mutation_guard = self.mutation_guard([path]);
        let commit_blocks = commit_blocks
            .into_iter()
            .map(ProtoUtils::commit_block_to_pb)
            .collect();

        let header = CompleteFileRequest {
            path: path.encode(),
            len,
            client_name: self.context().clone_client_name(),
            commit_blocks,
            only_flush: options.only_flush,
            inode_id,
            set_attr_opts: options.set_attr_opts.map(ProtoUtils::set_attr_opts_to_pb),
            return_file_blocks: Some(options.return_file_blocks),
        };

        let operation = if options.only_flush {
            "Flush"
        } else {
            "Complete"
        };
        let rep: CompleteFileResponse =
            FsContext::metrics_track(operation, self.rpc(RpcCode::CompleteFile, header)).await?;

        Ok(rep.file_blocks.map(ProtoUtils::file_blocks_from_pb))
    }

    pub async fn complete_files_batch(
        &self,
        requests: Vec<(String, i64, Vec<CommitBlock>, String, bool)>,
    ) -> FsResult<Vec<bool>> {
        let _mutation_guard =
            self.encoded_mutation_guard(requests.iter().map(|(path, _, _, _, _)| path.clone()));
        let pb_requests: Vec<CompleteFileRequest> = requests
            .into_iter()
            .map(|(path, len, commit_blocks, client_name, only_flush)| {
                let commit_blocks = commit_blocks
                    .into_iter()
                    .map(ProtoUtils::commit_block_to_pb)
                    .collect();
                CompleteFileRequest {
                    path,
                    len,
                    client_name,
                    commit_blocks,
                    only_flush,
                    inode_id: None,
                    set_attr_opts: None,
                    return_file_blocks: Some(false),
                }
            })
            .collect();

        let header = CompleteFilesBatchRequest {
            requests: pb_requests,
        };

        let rep: CompleteFilesBatchResponse = self.rpc(RpcCode::CompleteFilesBatch, header).await?;
        Ok(rep.results)
    }

    pub async fn get_block_locations(&self, path: &Path) -> FsResult<FileBlocks> {
        let header = GetBlockLocationsRequest {
            path: path.encode(),
        };

        let rep: GetBlockLocationsResponse = self.rpc(RpcCode::GetBlockLocations, header).await?;
        let res = ProtoUtils::file_blocks_from_pb(rep.blocks);

        Ok(res)
    }

    pub async fn get_file_block_details(&self, path: &Path) -> FsResult<FileBlockDetails> {
        let header = GetFileBlockDetailsRequest {
            path: path.encode(),
        };
        let response: GetFileBlockDetailsResponse =
            self.rpc(RpcCode::GetFileBlockDetails, header).await?;
        Ok(ProtoUtils::file_block_details_from_pb(response))
    }

    pub async fn get_cv_metadata_snapshot_page(
        &self,
        page_token: Option<String>,
        page_size: Option<u32>,
    ) -> FsResult<GetCvMetadataSnapshotPageResponse> {
        let header = GetCvMetadataSnapshotPageRequest {
            page_token,
            page_size,
        };
        self.rpc(RpcCode::GetCvMetadataSnapshotPage, header).await
    }

    pub async fn get_filesystem_info(&self) -> FsResult<FilesystemInfo> {
        // Attach this client's component_info only on the first
        // GetFilesystemInfo per session (the handshake). GetFilesystemInfo
        // backs FUSE statfs and may be called frequently by the kernel, so
        // later calls omit the payload; the response is still parsed and
        // cached on every call. Legacy masters skip the unknown field and
        // legacy clients omit it entirely.
        let (header, mut guard) = self.get_filesystem_info_request();
        let rep: GetFilesystemInfoResponse =
            self.raw_rpc(RpcCode::GetFilesystemInfo, header).await?;
        // The report reached the master: keep the one-time claim. On error or
        // cancellation the guard's drop resets it so a later call can report.
        guard.commit();
        // Cache the master's advertised version / protocol / capabilities; a
        // master without a compatibility contract is recorded as legacy and
        // never rejected.
        self.context
            .set_master_handshake(MasterHandshake::from_response(&rep));
        let info = ProtoUtils::filesystem_info_from_pb(rep);
        // Cache the live-worker versions for the T10 client-side worker
        // pre-check before data connections are opened.
        self.context.set_live_workers(&info.live_workers);
        Ok(info)
    }

    /// Client-master version handshake: report this client's `component_info`
    /// and cache the master's advertised version / protocol / capabilities.
    /// Returns the cached handshake; a master without a compatibility
    /// contract is reported as a legacy peer and is never rejected.
    pub async fn handshake(&self) -> FsResult<MasterHandshake> {
        let _ = self.get_filesystem_info().await?;
        // Mark the one-time lazy handshake as done so the first ordinary RPC
        // does not re-run it (FUSE mount performs this eagerly at startup).
        self.context.mark_handshake_started();
        Ok(self.master_handshake())
    }

    /// Cached master handshake (version / protocol / capabilities). Before the
    /// first handshake and against legacy masters this reports a legacy peer,
    /// which is never rejected.
    pub fn master_handshake(&self) -> MasterHandshake {
        self.context.master_handshake()
    }

    /// Build the client's own `component_info` payload for the handshake.
    fn handshake_request_component_info() -> ComponentInfoProto {
        ProtoUtils::component_version_to_pb(&curvine_sys::version::component_version("client"))
    }

    /// Build a `GetFilesystemInfo` request, attaching this client's
    /// `component_info` only on the first call per session (the handshake).
    /// Both the typed and the raw-bytes RPC paths share this so every
    /// GetFilesystemInfo caller reports the client version exactly once;
    /// frequent statfs queries stay lean. The returned guard holds the claim
    /// and resets it on drop unless committed, so a cancelled or failed
    /// in-flight request never permanently suppresses reporting.
    fn get_filesystem_info_request(&self) -> (GetFilesystemInfoRequest, HandshakeReportGuard<'_>) {
        let report_component = self.context.claim_handshake_report();
        let header = if report_component {
            GetFilesystemInfoRequest {
                component_info: Some(Self::handshake_request_component_info()),
            }
        } else {
            GetFilesystemInfoRequest::default()
        };
        let guard = HandshakeReportGuard {
            context: &self.context,
            committed: !report_component,
        };
        (header, guard)
    }

    pub async fn get_filesystem_info_bytes(&self) -> FsResult<BytesMut> {
        let (header, mut guard) = self.get_filesystem_info_request();
        let bytes = self
            .raw_rpc_bytes(RpcCode::GetFilesystemInfo, header)
            .await?;
        guard.commit();
        // Decode the response so bytes-only callers (e.g. SDK paths) also
        // cache the master's compatibility contract instead of staying at the
        // default legacy handshake. Best-effort: the raw bytes are returned
        // regardless.
        if let Ok(rep) = GetFilesystemInfoResponse::decode(bytes.as_ref()) {
            self.context
                .set_master_handshake(MasterHandshake::from_response(&rep));
            // Cache the live-worker versions for the T10 client-side worker
            // pre-check before data connections are opened.
            let info = ProtoUtils::filesystem_info_from_pb(rep);
            self.context.set_live_workers(&info.live_workers);
        }
        Ok(bytes)
    }

    pub async fn mount(
        &self,
        ufs_path: &Path,
        cv_path: &Path,
        opts: MountOptions,
    ) -> FsResult<MountResponse> {
        let _mutation_guard = self.mutation_guard([ufs_path, cv_path]);
        let req = MountRequest {
            ufs_path: ufs_path.encode_uri(),
            cv_path: cv_path.encode(),
            mount_options: ProtoUtils::mount_options_to_pb(opts),
        };

        let rep: MountResponse = self.rpc(RpcCode::Mount, req).await?;
        Ok(rep)
    }

    pub async fn umount(&self, cv_path: &Path) -> FsResult<UnMountResponse> {
        let _mutation_guard = self.mutation_guard([cv_path]);
        let req = UnMountRequest {
            cv_path: cv_path.encode(),
        };

        let rep: UnMountResponse = self.rpc(RpcCode::UnMount, req).await?;
        Ok(rep)
    }

    pub async fn get_mount_info(&self, path: &Path) -> FsResult<Option<MountInfo>> {
        let req = GetMountInfoRequest {
            path: path.encode_uri(),
        };

        let rep: GetMountInfoResponse = self.rpc(RpcCode::GetMountInfo, req).await?;
        Ok(rep.mount_info.map(ProtoUtils::mount_info_from_pb))
    }

    pub async fn get_mount_info_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        let req = GetMountInfoRequest {
            path: path.encode_uri(),
        };

        let bytes = self.rpc_bytes(RpcCode::GetMountInfo, req).await?;
        Ok(bytes)
    }

    pub async fn get_ufs_conf(&self, ufs_path: &Path) -> FsResult<UfsConf> {
        let resp = self.get_mount_info(ufs_path).await?;
        let conf = match resp {
            Some(mount_point) => {
                let mut ufs_conf_builder = UfsConfBuilder::default();
                mount_point.properties.iter().for_each(|(k, v)| {
                    ufs_conf_builder.add_config(k, v);
                });
                ufs_conf_builder.build()
            }
            None => return err_box!("failed get {} config", ufs_path),
        };
        Ok(conf)
    }

    pub async fn get_mount_table(&self) -> FsResult<GetMountTableResponse> {
        let req = GetMountTableRequest {};
        let rep: GetMountTableResponse = self.rpc(RpcCode::GetMountTable, req).await?;
        Ok(rep)
    }

    pub async fn set_attr(&self, path: &Path, opts: SetAttrOpts) -> FsResult<FileStatus> {
        let _mutation_guard = self.mutation_guard([path]);
        let req = SetAttrRequest {
            path: path.encode(),
            opts: ProtoUtils::set_attr_opts_to_pb(opts),
        };
        let rep: SetAttrResponse = self.rpc(RpcCode::SetAttr, req).await?;
        Ok(ProtoUtils::file_status_from_pb(rep.status))
    }

    pub async fn symlink(&self, target: &str, link: &Path, force: bool) -> FsResult<()> {
        self.symlink_with_owner_group(target, link, force, None, None)
            .await
    }

    pub async fn symlink_with_owner_group(
        &self,
        target: &str,
        link: &Path,
        force: bool,
        owner: Option<String>,
        group: Option<String>,
    ) -> FsResult<()> {
        let _mutation_guard = self.mutation_guard([link]);
        let req = SymlinkRequest {
            target: target.to_string(),
            link: link.encode(),
            force,
            mode: ClientConf::DEFAULT_FILE_SYSTEM_MODE,
            owner,
            group,
        };
        let _: SymlinkResponse = self.rpc(RpcCode::Symlink, req).await?;
        Ok(())
    }

    pub async fn metrics_report(&self, metrics: Vec<MetricValue>) -> FsResult<()> {
        if metrics.is_empty() {
            return Ok(());
        }

        let req = MetricsReportRequest {
            instance: self.context.client_addr.ip_addr.clone(),
            source: "".to_string(),
            metrics: ProtoUtils::metrics_report_to_pb(metrics),
        };
        let _: MetricsReportResponse = self.rpc(RpcCode::MetricsReport, req).await?;
        Ok(())
    }

    pub async fn link(&self, src_path: &Path, dst_path: &Path) -> FsResult<()> {
        let _mutation_guard = self.mutation_guard([src_path, dst_path]);
        let req = LinkRequest {
            src_path: src_path.encode(),
            dst_path: dst_path.encode(),
        };
        let _: LinkResponse = self.rpc(RpcCode::Link, req).await?;
        Ok(())
    }

    pub async fn create_special_node(
        &self,
        path: &Path,
        opts: CreateFileOpts,
    ) -> FsResult<FileStatus> {
        let _mutation_guard = self.mutation_guard([path]);
        let flags = OpenFlags::new_create();
        let header = CreateFileRequest {
            path: path.encode(),
            opts: ProtoUtils::create_opts_to_pb(opts, self.context.clone_client_name()),
            flags: flags.value(),
        };

        let rep_header: CreateFileResponse = self.rpc(RpcCode::CreateFile, header).await?;
        Ok(ProtoUtils::file_status_from_pb(rep_header.file_status))
    }

    pub async fn resize(&self, path: &Path, alloc_opts: FileAllocOpts) -> FsResult<FileBlocks> {
        self.resize0(path, None, alloc_opts).await
    }

    pub async fn resize_by_id(
        &self,
        path: &Path,
        inode_id: i64,
        alloc_opts: FileAllocOpts,
    ) -> FsResult<FileBlocks> {
        self.resize0(path, Some(inode_id), alloc_opts).await
    }

    async fn resize0(
        &self,
        path: &Path,
        inode_id: Option<i64>,
        alloc_opts: FileAllocOpts,
    ) -> FsResult<FileBlocks> {
        let _mutation_guard = self.mutation_guard([path]);
        let req = FileResizeRequest {
            path: path.encode(),
            opts: ProtoUtils::file_alloc_opts_to_pb(alloc_opts),
            inode_id,
        };

        let rep: FileResizeResponse = self.rpc(RpcCode::ResizeFile, req).await?;
        Ok(ProtoUtils::file_blocks_from_pb(rep.file_blocks))
    }

    pub async fn assign_worker(&self, path: &Path, block: ExtendedBlock) -> FsResult<LocatedBlock> {
        self.assign_worker0(path, None, block).await
    }

    pub async fn assign_worker_by_id(
        &self,
        path: &Path,
        inode_id: i64,
        block: ExtendedBlock,
    ) -> FsResult<LocatedBlock> {
        self.assign_worker0(path, Some(inode_id), block).await
    }

    async fn assign_worker0(
        &self,
        path: &Path,
        inode_id: Option<i64>,
        block: ExtendedBlock,
    ) -> FsResult<LocatedBlock> {
        let _mutation_guard = self.mutation_guard([path]);
        let req = AssignWorkerRequest {
            path: path.encode(),
            block: ProtoUtils::extend_block_to_pb(block),
            exclude_workers: self.context.exclude_workers(),
            client_address: self.context.client_addr_pb(),
            inode_id,
        };

        let rep: AssignWorkerResponse = self.rpc(RpcCode::AssignWorker, req).await?;
        Ok(ProtoUtils::located_block_from_pb(rep.block))
    }

    pub async fn get_lock(&self, path: &Path, lock: FileLock) -> FsResult<Option<FileLock>> {
        let req = GetLockRequest {
            path: path.encode(),
            lock: ProtoUtils::file_lock_to_pb(lock),
        };
        let rep: GetLockResponse = self.rpc(RpcCode::GetLock, req).await?;
        Ok(rep.conflict.map(ProtoUtils::file_lock_from_pb))
    }

    pub async fn set_lock(&self, path: &Path, lock: FileLock) -> FsResult<Option<FileLock>> {
        let _mutation_guard = self.mutation_guard([path]);
        let req = SetLockRequest {
            path: path.encode(),
            lock: ProtoUtils::file_lock_to_pb(lock),
        };
        let rep: SetLockResponse = self.rpc(RpcCode::SetLock, req).await?;
        Ok(rep.conflict.map(ProtoUtils::file_lock_from_pb))
    }

    pub async fn rpc<T, R>(&self, code: RpcCode, header: T) -> FsResult<R>
    where
        T: PMessage + Default,
        R: PMessage + Default,
    {
        // Best-effort one-time handshake before the first ordinary master RPC
        // so every client path (CLI, SDK, data-transfer, direct
        // CurvineFileSystem/UnifiedFileSystem users) reports component_info
        // and caches the master's compatibility contract, not only FUSE
        // mount. GetFilesystemInfo is the handshake itself and must not
        // recurse.
        if code != RpcCode::GetFilesystemInfo {
            self.ensure_handshake().await;
        }
        let msg = MessageBuilder::new_rpc(code).proto_header(header).build();
        self.route_master_message(code, msg)
            .await?
            .parse_header()
            .map_err(FsError::from)
    }

    pub async fn rpc_bytes(&self, code: RpcCode, header: impl PMessage) -> FsResult<BytesMut> {
        if code != RpcCode::GetFilesystemInfo {
            self.ensure_handshake().await;
        }
        let msg = MessageBuilder::new_rpc(code).proto_header(header).build();
        let msg = self.route_master_message(code, msg).await?;
        match msg.header {
            None => Ok(BytesMut::new()),
            Some(bytes) => Ok(bytes),
        }
    }

    /// Raw typed master RPC without the lazy handshake guard; the handshake
    /// itself (GetFilesystemInfo) uses this to avoid recursing into
    /// [`Self::ensure_handshake`].
    async fn raw_rpc<T, R>(&self, code: RpcCode, header: T) -> FsResult<R>
    where
        T: PMessage + Default,
        R: PMessage + Default,
    {
        self.connector
            .proto_rpc::<T, R, FsError>(code, header)
            .await
    }

    /// Raw bytes master RPC without the lazy handshake guard; see
    /// [`Self::raw_rpc`].
    async fn raw_rpc_bytes(&self, code: RpcCode, header: impl PMessage) -> FsResult<BytesMut> {
        let msg = MessageBuilder::new_rpc(code).proto_header(header).build();

        let msg = self.connector.rpc::<FsError>(msg).await?;
        match msg.header {
            None => Ok(BytesMut::new()),
            Some(v) => Ok(v),
        }
    }

    /// Route requests through a follower only when the session opted in and
    /// the closed RPC classifier plus decoded path say the request is safe.
    /// Writes and non-allowlisted RPCs retain the connector's existing
    /// leader-routing and retry behavior.
    async fn route_master_message(&self, code: RpcCode, msg: Message) -> FsResult<Message> {
        if !self.context.conf.client.follower_reads_enabled || code == RpcCode::GetFilesystemInfo {
            return self.connector.rpc::<FsError>(msg).await;
        }

        let open_file_flags = if code == RpcCode::OpenFile {
            msg.header_bytes()
                .and_then(|bytes| OpenFileRequest::decode(bytes).ok())
                .map(|request| request.flags)
        } else {
            None
        };
        if !code.allows_follower_read(open_file_flags) {
            // Preserve the existing retry and leader discovery behavior for
            // writes and RPCs outside the read allowlist.
            return self.connector.rpc::<FsError>(msg).await;
        }

        let path = follower_read_path(code, &msg);
        let sticky = path
            .as_ref()
            .is_none_or(|path| self.context.session_path_tracker().is_sticky(path));
        let msg = msg.into_arc();

        let deadline = Instant::now() + self.connector.rpc_timeout();
        let leader_id = self.ensure_leader_until(deadline).await?;

        if !sticky {
            let followers: Vec<_> = self
                .connector
                .peer_snapshot()
                .into_iter()
                .filter(|peer| peer.id != leader_id)
                .collect();
            if !followers.is_empty() {
                let index =
                    self.follower_peer_cursor.fetch_add(1, Ordering::Relaxed) % followers.len();
                let peer = &followers[index];
                let remaining = deadline.saturating_duration_since(Instant::now());
                let follower_budget = remaining / FOLLOWER_RPC_DEADLINE_FRACTION;
                if !follower_budget.is_zero() {
                    if let Some(follower_msg) = follower_read_message(&msg) {
                        match tokio::time::timeout(
                            follower_budget,
                            self.connector.timeout_rpc::<FsError>(peer.id, follower_msg),
                        )
                        .await
                        {
                            Ok(Ok(response)) => return Ok(response),
                            Ok(Err((_, error))) => {
                                warn!(
                                    "follower read rpc({}) to node {} failed; falling back to leader: {}",
                                    msg.req_id(),
                                    peer.id,
                                    error
                                );
                            }
                            Err(error) => {
                                warn!(
                                    "follower read rpc({}) to node {} timed out; falling back to leader: {}",
                                    msg.req_id(),
                                    peer.id,
                                    error
                                );
                            }
                        }
                    }
                }
            }
        }

        self.send_to_leader_until(leader_id, msg, deadline).await
    }

    async fn ensure_leader_until(&self, deadline: Instant) -> FsResult<u64> {
        if let Some(leader_id) = self.context.connector.leader_id() {
            if self.context.connector.get_addr(leader_id).is_ok() {
                return Ok(leader_id);
            }
            self.context
                .connector
                .change_leader(ClusterConnector::DEFAULT_LEADER_ID);
        }

        self.probe_leader_until(deadline).await
    }

    /// GetFilesystemInfo is excluded from follower routing and is the
    /// leader-only probe used to establish or refresh the cached leader hint.
    async fn probe_leader_until(&self, deadline: Instant) -> FsResult<u64> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(FsError::common(
                "metadata RPC deadline expired before leader discovery",
            ));
        }

        let probe = MessageBuilder::new_rpc(RpcCode::GetFilesystemInfo)
            .proto_header(GetFilesystemInfoRequest::default())
            .build();
        tokio::time::timeout(remaining, self.connector.rpc::<FsError>(probe))
            .await
            .map_err(FsError::from)??;
        self.context
            .connector
            .leader_id()
            .ok_or_else(|| FsError::common("leader probe succeeded without a cached leader"))
    }

    async fn send_to_leader_until(
        &self,
        leader_id: u64,
        msg: BoxMessage,
        deadline: Instant,
    ) -> FsResult<Message> {
        match self
            .send_to_peer_until(leader_id, msg.clone(), deadline)
            .await
        {
            Ok(response) => Ok(response),
            Err((false, error)) => Err(error),
            Err((true, first_error)) => {
                // A prior leader hint can go stale between discovery and this
                // send. Refresh only with the leader-only probe; never retry
                // the eligible read through generic fanout.
                self.context
                    .connector
                    .change_leader(ClusterConnector::DEFAULT_LEADER_ID);
                let refreshed = self
                    .probe_leader_until(deadline)
                    .await
                    .map_err(|probe_error| {
                        warn!(
                            "leader-targeted rpc({}) failed ({}), and the leader probe failed: {}",
                            msg.req_id(),
                            first_error,
                            probe_error
                        );
                        probe_error
                    })?;
                self.send_to_peer_until(refreshed, msg, deadline)
                    .await
                    .map_err(|(_, error)| error)
            }
        }
    }

    async fn send_to_peer_until(
        &self,
        peer_id: u64,
        msg: BoxMessage,
        deadline: Instant,
    ) -> Result<Message, (bool, FsError)> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err((
                true,
                FsError::common("metadata RPC deadline expired before peer send"),
            ));
        }
        match tokio::time::timeout(
            remaining,
            self.context.connector.timeout_rpc::<FsError>(peer_id, msg),
        )
        .await
        {
            Ok(result) => result,
            Err(error) => Err((true, FsError::from(error))),
        }
    }

    /// Run the client-master handshake once per session before the first
    /// ordinary master RPC, best-effort: a failure only logs a warning and
    /// the caller's RPC proceeds with legacy assumptions (nothing is ever
    /// rejected by default). Skipped when a GetFilesystemInfo request already
    /// went out (the typed/bytes paths populate the same cache).
    async fn ensure_handshake(&self) {
        if self.context.handshake_started() || self.context.handshake_reported() {
            return;
        }
        let _guard = self.context.handshake_lock().lock().await;
        if self.context.handshake_started() || self.context.handshake_reported() {
            return;
        }
        self.context.mark_handshake_started();
        if let Err(e) = self.handshake().await {
            warn!("client-master handshake failed: {e}; continuing with legacy assumptions");
        }
    }

    pub fn rpc_blocking<T, R>(&self, code: RpcCode, header: T) -> FsResult<R>
    where
        T: PMessage + Default,
        R: PMessage + Default,
    {
        self.context.rt().block_on(self.rpc(code, header))
    }

    pub fn client_addr(&self) -> &ClientAddress {
        &self.context.client_addr
    }
}

fn follower_read_path(code: RpcCode, msg: &Message) -> Option<Path> {
    if code == RpcCode::GetMountTable {
        return Path::from_str("/").ok();
    }

    let header = msg.header_bytes()?;
    let path = match code {
        RpcCode::OpenFile => OpenFileRequest::decode(header).ok()?.path,
        RpcCode::FileStatus => GetFileStatusRequest::decode(header).ok()?.path,
        RpcCode::Exists => ExistsRequest::decode(header).ok()?.path,
        RpcCode::ListStatus => ListStatusRequest::decode(header).ok()?.path,
        RpcCode::ListOptions => ListOptionsRequest::decode(header).ok()?.path,
        RpcCode::GetBlockLocations => GetBlockLocationsRequest::decode(header).ok()?.path,
        RpcCode::GetMountInfo => GetMountInfoRequest::decode(header).ok()?.path,
        _ => return None,
    };
    let path = Path::from_str(path).ok()?;
    if matches!(code, RpcCode::ListStatus | RpcCode::ListOptions)
        && path.path().contains(['*', '?', '[', '{', '\\'])
    {
        // ListStatus resolves glob patterns to multiple paths. A component
        // prefix check cannot prove which mutations that result set includes,
        // so keep glob listings on the leader for read-after-write safety.
        return None;
    }
    if path.path().is_empty() {
        Path::from_str("/").ok()
    } else {
        Some(path)
    }
}

/// Build the one-shot request variant that lets a follower distinguish an
/// explicitly opted-in read from the same RPC sent to the leader. Metadata
/// read requests carry their protobuf only in the header; fail closed if a
/// future caller attaches a data body that cannot be copied here.
fn follower_read_message(msg: &Message) -> Option<Message> {
    if msg.data_len() != 0 {
        return None;
    }

    let mut builder = MessageBuilder::protocol(msg.protocol).seq_id(FOLLOWER_READ_SEQ_ID);
    if let Some(header) = &msg.header {
        builder = builder.header(header.clone());
    }
    Some(builder.build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_config::ClusterConf;
    use curvine_core_error::CommonErrorExt;
    use curvine_net::net::{ConnState, InetAddr, NetUtils, NodeAddr};
    use curvine_proto::GetFilesystemInfoRequest;
    use curvine_rpc::handler::{HandlerService, MessageHandler};
    use curvine_rpc::message::INIT_SEQ_ID;
    use curvine_rpc::server::{RpcServer, ServerConf};
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn mutation_test_context(follower_reads_enabled: bool) -> Arc<FsContext> {
        mutation_test_context_with_retry(follower_reads_enabled, 1_000, 1, 1)
    }

    fn mutation_test_context_with_retry(
        follower_reads_enabled: bool,
        sticky_ms: u64,
        retry_max_ms: u64,
        retry_sleep_ms: u64,
    ) -> Arc<FsContext> {
        let mut conf = ClusterConf::default();
        conf.client.follower_reads_enabled = follower_reads_enabled;
        conf.client.follower_write_sticky_ms = sticky_ms;
        conf.client.master_addrs = vec![InetAddr::new("127.0.0.1", 0)];
        conf.client.conn_retry_max_duration_ms = 1;
        conf.client.conn_retry_min_sleep_ms = 1;
        conf.client.conn_retry_max_sleep_ms = 1;
        conf.client.rpc_retry_max_duration_ms = retry_max_ms;
        conf.client.rpc_retry_min_sleep_ms = retry_sleep_ms;
        conf.client.rpc_retry_max_sleep_ms = retry_sleep_ms;
        conf.client.rpc_timeout_ms = 1;
        conf.client.init().unwrap();

        let rpc_runtime = Arc::new(conf.client.client_rpc_conf().create_runtime());
        let context = Arc::new(FsContext::with_rt(conf, rpc_runtime).unwrap());
        context.mark_handshake_started();
        context
    }

    fn path(value: &str) -> Path {
        Path::from_str(value).unwrap()
    }

    const FAKE_LEADER: usize = 0;
    const FAKE_FOLLOWER_SUCCESS: usize = 1;
    const FAKE_FOLLOWER_REFUSE: usize = 2;

    #[derive(Default)]
    struct RpcCounts {
        filesystem_info: AtomicUsize,
        exists: AtomicUsize,
        open_file: AtomicUsize,
        delete: AtomicUsize,
        mount_table: AtomicUsize,
        list_status: AtomicUsize,
        follower_seq_id: AtomicUsize,
        init_seq_id: AtomicUsize,
    }

    impl RpcCounts {
        fn record(&self, msg: &Message) {
            match msg.seq_id() {
                FOLLOWER_READ_SEQ_ID => self.follower_seq_id.fetch_add(1, Ordering::SeqCst),
                INIT_SEQ_ID => self.init_seq_id.fetch_add(1, Ordering::SeqCst),
                _ => 0,
            };

            let code = RpcCode::from(msg.code());
            let counter = match code {
                RpcCode::GetFilesystemInfo => &self.filesystem_info,
                RpcCode::Exists => &self.exists,
                RpcCode::OpenFile => &self.open_file,
                RpcCode::Delete => &self.delete,
                RpcCode::GetMountTable => &self.mount_table,
                RpcCode::ListStatus => &self.list_status,
                _ => return,
            };
            counter.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct FakeRpcState {
        mode: AtomicUsize,
        require_follower_marker: AtomicBool,
        counts: RpcCounts,
    }

    impl FakeRpcState {
        fn new(mode: usize) -> Arc<Self> {
            Arc::new(Self {
                mode: AtomicUsize::new(mode),
                require_follower_marker: AtomicBool::new(true),
                counts: RpcCounts::default(),
            })
        }
    }

    struct FakeRpcService {
        state: Arc<FakeRpcState>,
    }

    struct FakeRpcHandler {
        state: Arc<FakeRpcState>,
    }

    impl MessageHandler for FakeRpcHandler {
        type Error = CommonErrorExt;

        fn handle(&self, msg: &Message) -> Result<Message, Self::Error> {
            let code = RpcCode::from(msg.code());
            self.state.counts.record(msg);
            let mode = self.state.mode.load(Ordering::SeqCst);

            if code == RpcCode::GetFilesystemInfo {
                return if mode == FAKE_LEADER {
                    Ok(msg.success())
                } else {
                    Ok(msg.error_ext(&FsError::not_leader("fake follower")))
                };
            }
            if mode == FAKE_FOLLOWER_REFUSE {
                return Ok(msg.error_ext(&FsError::not_leader("fake follower")));
            }
            if mode != FAKE_LEADER
                && self.state.require_follower_marker.load(Ordering::SeqCst)
                && msg.seq_id() != FOLLOWER_READ_SEQ_ID
            {
                return Ok(msg.error_ext(&FsError::not_leader("unmarked read is leader-only")));
            }
            if code == RpcCode::Exists {
                let response = ExistsResponse {
                    exists: mode == FAKE_FOLLOWER_SUCCESS,
                };
                let bytes = response.encode_to_vec();
                return Ok(msg.success_with_header(BytesMut::from(bytes.as_slice())));
            }

            Ok(msg.success())
        }
    }

    impl HandlerService for FakeRpcService {
        type Item = FakeRpcHandler;

        fn get_message_handler(&self, _conn_state: Option<ConnState>) -> Self::Item {
            FakeRpcHandler {
                state: self.state.clone(),
            }
        }
    }

    async fn start_fake_rpc_server(state: Arc<FakeRpcState>) -> InetAddr {
        let port = NetUtils::hold_available_port();
        let mut conf = ServerConf::with_hostname("127.0.0.1", port);
        conf.io_threads = 1;
        conf.worker_threads = 1;
        let server = RpcServer::new(conf, FakeRpcService { state });
        let address = server.bind_addr().clone();
        let mut listener = RpcServer::run_server(server);
        listener.wait_startup().await.unwrap();
        address
    }

    fn follower_route_context(
        enabled: bool,
        sticky_ms: u64,
        leader_addr: &InetAddr,
    ) -> (Arc<FsContext>, Arc<curvine_runtime::runtime::Runtime>) {
        let mut conf = ClusterConf::default();
        conf.client.follower_reads_enabled = enabled;
        conf.client.follower_write_sticky_ms = sticky_ms;
        conf.client.master_addrs = vec![leader_addr.clone()];
        conf.client.conn_retry_max_duration_ms = 100;
        conf.client.conn_retry_min_sleep_ms = 1;
        conf.client.conn_retry_max_sleep_ms = 5;
        conf.client.conn_timeout_ms = 100;
        conf.client.rpc_retry_max_duration_ms = 800;
        conf.client.rpc_retry_min_sleep_ms = 2;
        conf.client.rpc_retry_max_sleep_ms = 20;
        conf.client.rpc_timeout_ms = 800;
        conf.client.init().unwrap();

        let runtime = Arc::new(conf.client.client_rpc_conf().create_runtime());
        let context = Arc::new(FsContext::with_rt(conf, runtime.clone()).unwrap());
        context.mark_handshake_started();
        (context, runtime)
    }

    fn peer(id: u64, address: &InetAddr) -> NodeAddr {
        NodeAddr::new(id, address.hostname.clone(), address.port)
    }

    #[test]
    fn handshake_request_carries_client_component_info() {
        // A new client reports its own structured version during the
        // handshake so the master can diagnose mixed-version clusters.
        let info = FsClient::handshake_request_component_info();
        let req = GetFilesystemInfoRequest {
            component_info: Some(info),
        };

        let encoded = req.encode_to_vec();
        let decoded = GetFilesystemInfoRequest::decode(encoded.as_slice()).unwrap();
        let decoded_info = decoded.component_info.unwrap();
        assert_eq!(decoded_info.component.as_deref(), Some("client"));
        assert_eq!(decoded_info.protocol_version, Some(1));
        assert_eq!(decoded_info.min_protocol_version, Some(1));
    }

    #[test]
    fn handshake_request_is_backward_compatible() {
        // The request only adds an optional field on the reserved 1000+ range:
        // a legacy master's view of the message (no component_info) must
        // decode the payload and ignore the unknown field.
        #[derive(Clone, PartialEq, ::prost::Message)]
        struct LegacyGetFilesystemInfoRequest {}

        let req = GetFilesystemInfoRequest {
            component_info: Some(FsClient::handshake_request_component_info()),
        };
        let encoded = req.encode_to_vec();

        let legacy = LegacyGetFilesystemInfoRequest::decode(encoded.as_slice()).unwrap();
        assert_eq!(legacy, LegacyGetFilesystemInfoRequest {});
    }

    #[test]
    fn failed_mutation_calls_mark_paths_before_rpc_and_share_session_state() {
        let context = mutation_test_context(true);
        let client = FsClient::new(context.clone());
        let cloned_client = client.clone();
        let runtime = tokio::runtime::Runtime::new().unwrap();

        runtime.block_on(async {
            let create_path = path("/sticky/create");
            assert!(client
                .create_with_opts(&create_path, CreateFileOptsBuilder::new().build(), false,)
                .await
                .is_err());
            assert!(cloned_client
                .context()
                .session_path_tracker()
                .is_sticky(&path("/sticky/create/child")));
            assert!(context.session_path_tracker().is_sticky(&path("/sticky")));

            let read_only_path = path("/reads/remain-eligible");
            assert!(client
                .open_with_opts(
                    &read_only_path,
                    CreateFileOptsBuilder::new().build(),
                    OpenFlags::new_read_only(),
                )
                .await
                .is_err());
            assert!(!context.session_path_tracker().is_sticky(&read_only_path));

            let append_path = path("/sticky/append");
            assert!(client
                .open_with_opts(
                    &append_path,
                    CreateFileOptsBuilder::new().build(),
                    OpenFlags::new_append(),
                )
                .await
                .is_err());
            assert!(context
                .session_path_tracker()
                .is_sticky(&path("/sticky/append/child")));

            let rename_src = path("/rename/source/tree");
            let rename_dst = path("/rename/destination/tree");
            assert!(client
                .rename(&rename_src, &rename_dst, RenameFlags::empty())
                .await
                .is_err());
            assert!(context
                .session_path_tracker()
                .is_sticky(&path("/rename/source/tree/child")));
            assert!(context
                .session_path_tracker()
                .is_sticky(&path("/rename/destination/tree/child")));
            assert!(context
                .session_path_tracker()
                .is_sticky(&path("/rename/source")));
            assert!(context
                .session_path_tracker()
                .is_sticky(&path("/rename/destination")));

            let batch_path = path("/batch/create/file");
            assert!(client
                .create_files_batch(vec![(
                    batch_path.encode(),
                    CreateFileOptsBuilder::new().build(),
                    OpenFlags::new_write_only().set_create(true),
                )])
                .await
                .is_err());
            assert!(context
                .session_path_tracker()
                .is_sticky(&path("/batch/create/file/child")));

            let complete_path = path("/batch/complete/file");
            assert!(client
                .complete_files_batch(vec![(
                    complete_path.encode(),
                    0,
                    Vec::new(),
                    context.clone_client_name(),
                    false,
                )])
                .await
                .is_err());
            assert!(context
                .session_path_tracker()
                .is_sticky(&path("/batch/complete/file/child")));

            assert!(client.empty_trash().await.is_err());
            assert!(context
                .session_path_tracker()
                .is_sticky(&path("/unrelated/path")));
        });
    }

    #[test]
    fn disabled_follower_reads_do_not_create_sticky_state() {
        let context = mutation_test_context(false);
        let client = FsClient::new(context.clone());
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let path = path("/static/leader-only");

        assert!(runtime
            .block_on(client.mkdir(&path, MkdirOpts::with_create(false)))
            .is_err());
        assert!(!context.session_path_tracker().is_sticky(&path));
    }

    #[test]
    fn slow_failed_mutation_restarts_sticky_ttl_after_rpc_finishes() {
        let context = mutation_test_context_with_retry(true, 50, 100, 20);
        let client = FsClient::new(context.clone());
        let target = path("/slow/failure");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let started = std::time::Instant::now();

        assert!(runtime
            .block_on(client.mkdir(&target, MkdirOpts::with_create(false)))
            .is_err());
        assert!(started.elapsed() >= std::time::Duration::from_millis(60));
        assert!(context
            .session_path_tracker()
            .is_sticky(&path("/slow/failure/child")));
    }

    #[test]
    fn cancelling_inflight_mutation_refreshes_sticky_ttl() {
        let context = mutation_test_context_with_retry(true, 50, 500, 100);
        let client = FsClient::new(context.clone());
        let target = path("/cancelled/write");
        let runtime = tokio::runtime::Runtime::new().unwrap();

        let outcome = runtime.block_on(async {
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                client.mkdir(&target, MkdirOpts::with_create(false)),
            )
            .await
        });
        assert!(
            outcome.is_err(),
            "the RPC should still be retrying at timeout"
        );
        assert!(context
            .session_path_tracker()
            .is_sticky(&path("/cancelled/write/child")));
    }

    #[test]
    fn context_rejects_unvalidated_follower_reads_without_sticky_ttl() {
        let mut conf = ClusterConf::default();
        conf.client.follower_reads_enabled = true;
        conf.client.follower_write_sticky_ms = 0;
        let rpc_runtime = Arc::new(conf.client.client_rpc_conf().create_runtime());

        let error = FsContext::with_rt(conf, rpc_runtime).err().unwrap();
        assert!(error
            .to_string()
            .contains("follower_write_sticky_ms must be greater than zero"));
    }

    #[test]
    fn follower_routing_falls_back_safely_and_never_changes_leader_on_read_success() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let keep_context_runtimes = Arc::new(Mutex::new(Vec::<
            Arc<curvine_runtime::runtime::Runtime>,
        >::new()));
        let keep_context_runtimes_for_test = keep_context_runtimes.clone();

        runtime.block_on(async move {
            let leader_state = FakeRpcState::new(FAKE_LEADER);
            let follower_state = FakeRpcState::new(FAKE_FOLLOWER_SUCCESS);
            let leader_addr = start_fake_rpc_server(leader_state.clone()).await;
            let follower_addr = start_fake_rpc_server(follower_state.clone()).await;
            let leader = peer(1, &leader_addr);
            let follower = peer(2, &follower_addr);

            let (context, context_runtime) = follower_route_context(true, 250, &leader_addr);
            keep_context_runtimes_for_test
                .lock()
                .unwrap()
                .push(context_runtime);
            context
                .connector
                .reconcile_nodes(vec![leader.clone(), follower.clone()])
                .unwrap();
            context.connector.change_leader(leader.id);
            let client = FsClient::new(context.clone());

            // A successful follower response is returned directly and never
            // changes the cached leader hint.
            assert!(client.exists(&path("/fresh/file")).await.unwrap());
            assert_eq!(context.connector.leader_id(), Some(leader.id));
            assert_eq!(follower_state.counts.exists.load(Ordering::SeqCst), 1);
            assert_eq!(leader_state.counts.exists.load(Ordering::SeqCst), 0);
            assert_eq!(
                follower_state.counts.follower_seq_id.load(Ordering::SeqCst),
                1
            );
            assert_eq!(
                leader_state.counts.follower_seq_id.load(Ordering::SeqCst),
                0
            );

            let bytes = client
                .rpc_bytes(
                    RpcCode::Exists,
                    ExistsRequest {
                        path: path("/fresh/bytes").encode(),
                    },
                )
                .await
                .unwrap();
            assert!(ExistsResponse::decode(bytes.as_ref()).unwrap().exists);
            assert_eq!(follower_state.counts.exists.load(Ordering::SeqCst), 2);
            assert_eq!(leader_state.counts.exists.load(Ordering::SeqCst), 0);
            assert_eq!(
                follower_state.counts.follower_seq_id.load(Ordering::SeqCst),
                2
            );

            // Follower refusal is retryable for this client path and falls back to
            // a targeted leader request with the original, unmarked sequence ID.
            follower_state
                .mode
                .store(FAKE_FOLLOWER_REFUSE, Ordering::SeqCst);
            let follower_marked_before =
                follower_state.counts.follower_seq_id.load(Ordering::SeqCst);
            let leader_init_before = leader_state.counts.init_seq_id.load(Ordering::SeqCst);
            assert!(!client.exists(&path("/refused/file")).await.unwrap());
            assert_eq!(context.connector.leader_id(), Some(leader.id));
            assert_eq!(follower_state.counts.exists.load(Ordering::SeqCst), 3);
            assert_eq!(leader_state.counts.exists.load(Ordering::SeqCst), 1);
            assert_eq!(
                follower_state.counts.follower_seq_id.load(Ordering::SeqCst),
                follower_marked_before + 1
            );
            assert_eq!(
                leader_state.counts.init_seq_id.load(Ordering::SeqCst),
                leader_init_before + 1
            );

            // If a cached leader hint is stale, fallback probes with the
            // leader-only GetFilesystemInfo RPC and retries the read directly to
            // the newly discovered leader. The original read is never broadcast.
            let (stale_hint, stale_hint_runtime) = follower_route_context(true, 250, &leader_addr);
            keep_context_runtimes_for_test
                .lock()
                .unwrap()
                .push(stale_hint_runtime);
            stale_hint
                .connector
                .reconcile_nodes(vec![
                    peer(1, &follower_addr),
                    peer(2, &leader_addr),
                    peer(3, &follower_addr),
                ])
                .unwrap();
            stale_hint.connector.change_leader(1);
            let stale_client = FsClient::new(stale_hint.clone());
            stale_client
                .follower_peer_cursor
                .store(1, Ordering::Relaxed);
            assert!(!stale_client
                .exists(&path("/leader-changed/file"))
                .await
                .unwrap());
            assert_eq!(stale_hint.connector.leader_id(), Some(2));

            // One unavailable follower attempt is bounded to half of the RPC
            // deadline, leaving time for the leader fallback.
            let unavailable_port = NetUtils::get_available_port();
            context
                .connector
                .reconcile_nodes(vec![
                    leader.clone(),
                    NodeAddr::new(3, "127.0.0.1", unavailable_port),
                ])
                .unwrap();
            context.connector.change_leader(leader.id);
            assert!(!client.exists(&path("/unavailable/file")).await.unwrap());
            assert_eq!(leader_state.counts.exists.load(Ordering::SeqCst), 3);

            // Read-only OpenFile is eligible; write flags and mutation RPCs stay
            // leader-targeted even when the follower would answer successfully.
            context
                .connector
                .reconcile_nodes(vec![leader.clone(), follower.clone()])
                .unwrap();
            context.connector.change_leader(leader.id);
            follower_state
                .mode
                .store(FAKE_FOLLOWER_SUCCESS, Ordering::SeqCst);
            let read_open = OpenFileRequest {
                path: path("/open/read").encode(),
                opts: CreateFileOptsProto::default(),
                flags: OpenFlags::RDONLY.bits(),
            };
            client
                .rpc::<OpenFileRequest, OpenFileResponse>(RpcCode::OpenFile, read_open)
                .await
                .unwrap();
            assert_eq!(follower_state.counts.open_file.load(Ordering::SeqCst), 1);
            assert_eq!(leader_state.counts.open_file.load(Ordering::SeqCst), 0);

            let write_open = OpenFileRequest {
                path: path("/open/write").encode(),
                opts: CreateFileOptsProto::default(),
                flags: OpenFlags::new_write_only().bits(),
            };
            client
                .rpc::<OpenFileRequest, OpenFileResponse>(RpcCode::OpenFile, write_open)
                .await
                .unwrap();
            let delete = DeleteRequest {
                path: path("/mutating/delete").encode(),
                recursive: false,
                ..Default::default()
            };
            client
                .rpc::<DeleteRequest, DeleteResponse>(RpcCode::Delete, delete)
                .await
                .unwrap();
            assert_eq!(follower_state.counts.open_file.load(Ordering::SeqCst), 1);
            assert_eq!(follower_state.counts.delete.load(Ordering::SeqCst), 0);
            assert_eq!(leader_state.counts.open_file.load(Ordering::SeqCst), 1);
            assert_eq!(leader_state.counts.delete.load(Ordering::SeqCst), 1);

            // A mutation path and the global mount table stay leader-pinned until
            // the configured sticky window expires.
            context
                .session_path_tracker()
                .mark_mutation(&path("/sticky/tree"));
            let follower_before_sticky = follower_state.counts.exists.load(Ordering::SeqCst);
            let follower_marker_before_sticky =
                follower_state.counts.follower_seq_id.load(Ordering::SeqCst);
            assert!(!client.exists(&path("/sticky/tree/child")).await.unwrap());
            client
                .rpc::<GetMountTableRequest, GetMountTableResponse>(
                    RpcCode::GetMountTable,
                    GetMountTableRequest::default(),
                )
                .await
                .unwrap();
            client
                .rpc::<ListStatusRequest, ListStatusResponse>(
                    RpcCode::ListStatus,
                    ListStatusRequest {
                        path: String::new(),
                        need_location: false,
                    },
                )
                .await
                .unwrap();
            assert_eq!(
                follower_state.counts.exists.load(Ordering::SeqCst),
                follower_before_sticky
            );
            assert_eq!(
                follower_state.counts.follower_seq_id.load(Ordering::SeqCst),
                follower_marker_before_sticky
            );
            assert_eq!(follower_state.counts.mount_table.load(Ordering::SeqCst), 0);
            assert_eq!(follower_state.counts.list_status.load(Ordering::SeqCst), 0);

            client
                .mkdir(&path("/glob/child"), MkdirOpts::with_create(false))
                .await
                .unwrap();
            let leader_lists_before_glob = leader_state.counts.list_status.load(Ordering::SeqCst);
            let follower_lists_before_glob =
                follower_state.counts.list_status.load(Ordering::SeqCst);
            client
                .rpc::<ListStatusRequest, ListStatusResponse>(
                    RpcCode::ListStatus,
                    ListStatusRequest {
                        path: "/glob/*".to_string(),
                        need_location: false,
                    },
                )
                .await
                .unwrap();
            assert_eq!(
                leader_state.counts.list_status.load(Ordering::SeqCst),
                leader_lists_before_glob + 1
            );
            assert_eq!(
                follower_state.counts.list_status.load(Ordering::SeqCst),
                follower_lists_before_glob
            );

            tokio::time::sleep(Duration::from_millis(260)).await;
            assert!(client.exists(&path("/sticky/tree/child")).await.unwrap());
            assert_eq!(
                follower_state.counts.exists.load(Ordering::SeqCst),
                follower_before_sticky + 1
            );

            // A cached leader can become a follower after failover. Sticky
            // reads stay unmarked, so that old leader refuses them and the
            // client refreshes the leader hint before retrying the read.
            let (stale_sticky, stale_sticky_runtime) =
                follower_route_context(true, 250, &leader_addr);
            keep_context_runtimes_for_test
                .lock()
                .unwrap()
                .push(stale_sticky_runtime);
            stale_sticky
                .connector
                .reconcile_nodes(vec![peer(1, &follower_addr), peer(2, &leader_addr)])
                .unwrap();
            stale_sticky.connector.change_leader(1);
            stale_sticky
                .session_path_tracker()
                .mark_mutation(&path("/sticky-after-failover"));
            let old_follower_marked_before =
                follower_state.counts.follower_seq_id.load(Ordering::SeqCst);
            let old_follower_exists_before = follower_state.counts.exists.load(Ordering::SeqCst);
            let new_leader_exists_before = leader_state.counts.exists.load(Ordering::SeqCst);
            assert!(!FsClient::new(stale_sticky.clone())
                .exists(&path("/sticky-after-failover"))
                .await
                .unwrap());
            assert_eq!(stale_sticky.connector.leader_id(), Some(2));
            assert_eq!(
                follower_state.counts.exists.load(Ordering::SeqCst),
                old_follower_exists_before + 1
            );
            assert_eq!(
                follower_state.counts.follower_seq_id.load(Ordering::SeqCst),
                old_follower_marked_before
            );
            assert_eq!(
                leader_state.counts.exists.load(Ordering::SeqCst),
                new_leader_exists_before + 1
            );

            // With the feature off, existing leader routing is unchanged.
            let (disabled, disabled_runtime) = follower_route_context(false, 250, &leader_addr);
            keep_context_runtimes_for_test
                .lock()
                .unwrap()
                .push(disabled_runtime);
            disabled
                .connector
                .reconcile_nodes(vec![leader.clone(), follower.clone()])
                .unwrap();
            disabled.connector.change_leader(leader.id);
            let disabled_client = FsClient::new(disabled);
            let follower_before_disabled = follower_state.counts.exists.load(Ordering::SeqCst);
            assert!(!disabled_client.exists(&path("/disabled")).await.unwrap());
            assert_eq!(
                follower_state.counts.exists.load(Ordering::SeqCst),
                follower_before_disabled
            );

            // Unknown leader state first probes GetFilesystemInfo, then routes
            // the eligible request to a follower without caching its success.
            let (no_hint, no_hint_runtime) = follower_route_context(true, 250, &leader_addr);
            keep_context_runtimes_for_test
                .lock()
                .unwrap()
                .push(no_hint_runtime);
            no_hint
                .connector
                .reconcile_nodes(vec![leader.clone(), follower.clone()])
                .unwrap();
            let no_hint_client = FsClient::new(no_hint.clone());
            let leader_probe_before = leader_state.counts.filesystem_info.load(Ordering::SeqCst);
            let follower_probe_before =
                follower_state.counts.filesystem_info.load(Ordering::SeqCst);
            let follower_read_before = follower_state.counts.exists.load(Ordering::SeqCst);
            assert!(no_hint_client.exists(&path("/no-hint")).await.unwrap());
            assert_eq!(no_hint.connector.leader_id(), Some(leader.id));
            assert!(
                leader_state.counts.filesystem_info.load(Ordering::SeqCst) > leader_probe_before
            );
            assert!(
                follower_state.counts.filesystem_info.load(Ordering::SeqCst)
                    > follower_probe_before
            );
            assert_eq!(
                follower_state.counts.exists.load(Ordering::SeqCst),
                follower_read_before + 1
            );
        });

        // Keep each FsContext's nested RPC runtime alive until after the
        // async test body returns, then release it on this synchronous test
        // thread to avoid Tokio's blocking-shutdown panic.
        drop(keep_context_runtimes);
        drop(runtime);
    }
}
