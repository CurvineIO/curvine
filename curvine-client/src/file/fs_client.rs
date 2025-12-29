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

use crate::file::FsContext;
use bytes::BytesMut;
use curvine_common::conf::{ClientConf, UfsConf, UfsConfBuilder};
use curvine_common::error::FsError;
use curvine_common::fs::{Path, RpcCode};
use curvine_common::proto::*;
use curvine_common::state::*;
use curvine_common::utils::ProtoUtils;
use curvine_common::FsResult;
use orpc::client::ClusterConnector;
use orpc::err_box;
use orpc::message::MessageBuilder;
use orpc::runtime::RpcRuntime;
use prost::Message as PMessage;
use std::collections::LinkedList;
use std::sync::Arc;

#[derive(Clone)]
pub struct FsClient {
    context: Arc<FsContext>,
    connector: Arc<ClusterConnector>,
}

impl FsClient {
    pub fn new(context: Arc<FsContext>) -> Self {
        let connector = context.connector.clone();
        Self { context, connector }
    }

    pub fn context(&self) -> Arc<FsContext> {
        self.context.clone()
    }

    pub async fn mkdir(&self, path: &Path, opts: MkdirOpts) -> FsResult<FileStatus> {
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
        let pb_requests: Vec<CreateFileRequest> = requests
            .into_iter()
            .map(|(path, opts, flags)| CreateFileRequest {
                path,
                opts: ProtoUtils::create_opts_to_pb(opts, self.context.clone_client_name()),
                flags: flags.value(),
            })
            .collect();

        println!(
            "DEBUG: at create_files_batch pb_requests: {:?}",
            pb_requests
        );
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
        let header = GetFileStatusRequest {
            path: path.encode(),
        };

        let rep_header: GetFileStatusResponse = self.rpc(RpcCode::FileStatus, header).await?;
        let status = ProtoUtils::file_status_from_pb(rep_header.status);
        Ok(status)
    }

    pub async fn file_status_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        let header = GetFileStatusRequest {
            path: path.encode(),
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

    pub async fn delete(&self, path: &Path, recursive: bool) -> FsResult<()> {
        let header = DeleteRequest {
            path: path.encode(),
            recursive,
        };

        let _: DeleteResponse = self.rpc(RpcCode::Delete, header).await?;
        Ok(())
    }

    pub async fn rename(&self, src: &Path, dst: &Path) -> FsResult<bool> {
        let header = RenameRequest {
            src: src.encode(),
            dst: dst.encode(),
            flags: RenameFlags::empty().value(),
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
        };

        let rep_header = self.rpc(RpcCode::AddBlock, header).await?;
        let locate_block = ProtoUtils::located_block_from_pb(rep_header);
        Ok(locate_block)
    }

    pub async fn add_blocks_batch(
        &self,
        requests: Vec<(String, Vec<CommitBlock>, i64, Option<ExtendedBlock>)>,
    ) -> FsResult<Vec<LocatedBlock>> {
        let pb_requests: Vec<AddBlockRequest> = requests
            .into_iter()
            .map(|(path, commit_blocks, file_len, last_block)| {
                let commit_blocks = commit_blocks
                    .into_iter()
                    .map(ProtoUtils::commit_block_to_pb)
                    .collect();

                AddBlockRequest {
                    path,
                    commit_blocks,
                    exclude_workers: self.context.exclude_workers(),
                    located: true,
                    client_address: self.context.client_addr_pb(),
                    file_len,
                    last_block: last_block.map(ProtoUtils::extend_block_to_pb),
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
    // File writing is completed.
    pub async fn complete_file(
        &self,
        path: &Path,
        len: i64,
        commit_blocks: impl IntoIterator<Item = CommitBlock>,
        only_flush: bool,
    ) -> FsResult<Option<FileBlocks>> {
        let commit_blocks = commit_blocks
            .into_iter()
            .map(ProtoUtils::commit_block_to_pb)
            .collect();

        let header = CompleteFileRequest {
            path: path.encode(),
            len,
            client_name: self.context().clone_client_name(),
            commit_blocks,
            only_flush,
        };

        let rep: CompleteFileResponse = self.rpc(RpcCode::CompleteFile, header).await?;
        Ok(rep.file_blocks.map(ProtoUtils::file_blocks_from_pb))
    }

    pub async fn complete_files_batch(
        &self,
        requests: Vec<(String, i64, Vec<CommitBlock>, String, bool)>,
    ) -> FsResult<Vec<bool>> {
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
                }
            })
            .collect();

        println!("pb_requests: {:?}", pb_requests);
        let header = CompleteFilesBatchRequest {
            requests: pb_requests,
        };

        let rep: CompleteFilesBatchResponse = self.rpc(RpcCode::CompleteFilesBatch, header).await?;
        println!("rep from complete files batch: {:?}", rep);
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

    pub async fn get_master_info(&self) -> FsResult<MasterInfo> {
        let header = GetMasterInfoRequest::default();
        let rep: GetMasterInfoResponse = self.rpc(RpcCode::GetMasterInfo, header).await?;
        let res = ProtoUtils::master_info_from_pb(rep);
        Ok(res)
    }

    pub async fn get_master_info_bytes(&self) -> FsResult<BytesMut> {
        let header = GetMasterInfoRequest::default();
        self.rpc_bytes(RpcCode::GetMasterInfo, header).await
    }

    pub async fn mount(
        &self,
        ufs_path: &Path,
        cv_path: &Path,
        opts: MountOptions,
    ) -> FsResult<MountResponse> {
        let req = MountRequest {
            ufs_path: ufs_path.encode_uri(),
            cv_path: cv_path.encode(),
            mount_options: ProtoUtils::mount_options_to_pb(opts),
        };

        let rep: MountResponse = self.rpc(RpcCode::Mount, req).await?;
        Ok(rep)
    }

    pub async fn umount(&self, cv_path: &Path) -> FsResult<UnMountResponse> {
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
        let req = SetAttrRequest {
            path: path.encode(),
            opts: ProtoUtils::set_attr_opts_to_pb(opts),
        };
        let rep: SetAttrResponse = self.rpc(RpcCode::SetAttr, req).await?;
        Ok(ProtoUtils::file_status_from_pb(rep.status))
    }

    pub async fn symlink(&self, target: &str, link: &Path, force: bool) -> FsResult<()> {
        let req = SymlinkRequest {
            target: target.to_string(),
            link: link.encode(),
            force,
            mode: ClientConf::DEFAULT_FILE_SYSTEM_MODE,
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
        let req = LinkRequest {
            src_path: src_path.encode(),
            dst_path: dst_path.encode(),
        };
        let _: LinkResponse = self.rpc(RpcCode::Link, req).await?;
        Ok(())
    }

    pub async fn resize(&self, path: &Path, alloc_opts: FileAllocOpts) -> FsResult<FileBlocks> {
        let req = FileResizeRequest {
            path: path.encode(),
            opts: ProtoUtils::file_alloc_opts_to_pb(alloc_opts),
        };

        let rep: FileResizeResponse = self.rpc(RpcCode::ResizeFile, req).await?;
        Ok(ProtoUtils::file_blocks_from_pb(rep.file_blocks))
    }

    pub async fn assign_worker(&self, path: &Path, block: ExtendedBlock) -> FsResult<LocatedBlock> {
        let req = AssignWorkerRequest {
            path: path.encode(),
            block: ProtoUtils::extend_block_to_pb(block),
            exclude_workers: self.context.exclude_workers(),
            client_address: self.context.client_addr_pb(),
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
        self.connector
            .proto_rpc::<T, R, FsError>(code, header)
            .await
    }

    pub async fn rpc_bytes(&self, code: RpcCode, header: impl PMessage) -> FsResult<BytesMut> {
        let msg = MessageBuilder::new_rpc(code).proto_header(header).build();

        let msg = self.connector.rpc::<FsError>(msg).await?;
        match msg.header {
            None => Ok(BytesMut::new()),
            Some(v) => Ok(v),
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
