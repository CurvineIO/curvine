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
use orpc::message::MessageBuilder;
use orpc::runtime::RpcRuntime;
use orpc::{err_box, ternary};
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

    pub async fn mkdir(&self, path: &Path, opts: MkdirOpts) -> FsResult<bool> {
        let header = MkdirRequest {
            path: path.encode(),
            opts: ProtoUtils::mkdir_opts_to_pb(opts),
        };

        let rep_header: MkdirResponse = self.rpc(RpcCode::Mkdir, header).await?;
        Ok(rep_header.flag)
    }

    pub async fn create(&self, path: &Path, create_parent: bool) -> FsResult<FileStatus> {
        let opts = CreateFileOptsBuilder::new()
            .create_parent(create_parent)
            .build();

        self.create_with_opts(path, opts).await
    }

    pub async fn create_with_opts(
        &self,
        path: &Path,
        opts: CreateFileOpts,
    ) -> FsResult<FileStatus> {
        let header = CreateFileRequest {
            path: path.encode(),
            opts: ProtoUtils::create_opts_to_pb(opts, self.context.clone_client_name()),
        };

        let rep_header: CreateFileResponse = self.rpc(RpcCode::CreateFile, header).await?;
        let status = ProtoUtils::file_status_from_pb(rep_header.file_status);
        Ok(status)
    }

    pub async fn append(&self, path: &Path, opts: CreateFileOpts) -> FsResult<LastBlockStatus> {
        let header = AppendFileRequest {
            path: path.encode(),
            opts: ProtoUtils::create_opts_to_pb(opts, self.context.clone_client_name()),
        };
        let rep_header: AppendFileResponse = self.rpc(RpcCode::AppendFile, header).await?;
        let status = LastBlockStatus {
            file_status: ProtoUtils::file_status_from_pb(rep_header.file_status),
            last_block: rep_header.last_block.map(ProtoUtils::located_block_from_pb),
        };
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
        previous: Option<CommitBlock>,
        local_addr: &ClientAddress,
    ) -> FsResult<LocatedBlock> {
        let previous = previous.map(|v| ProtoUtils::commit_block_to_pb(v.clone()));

        let exclude_workers = {
            self.context()
                .failed_workers
                .iter()
                .map(|x| ProtoUtils::worker_address_to_pb(&x.1))
                .collect()
        };

        let header = AddBlockRequest {
            path: path.encode(),
            previous,
            exclude_workers,
            located: true,
            client_address: ClientAddressProto {
                client_name: local_addr.client_name.to_owned(),
                hostname: local_addr.hostname.to_owned(),
                ip_addr: local_addr.ip_addr.to_owned(),
                port: local_addr.port,
            },
        };

        let rep_header = self.rpc(RpcCode::AddBlock, header).await?;
        let locate_block = ProtoUtils::located_block_from_pb(rep_header);
        Ok(locate_block)
    }

    // File writing is completed.
    pub async fn complete_file(
        &self,
        path: &Path,
        len: i64,
        last: Option<CommitBlock>,
    ) -> FsResult<()> {
        let last = last.map(|v| ProtoUtils::commit_block_to_pb(v.clone()));
        let header = CompleteFileRequest {
            path: path.encode(),
            len,
            client_name: self.context().clone_client_name(),
            last,
        };

        let _: CompleteFileResponse = self.rpc(RpcCode::CompleteFile, header).await?;
        Ok(())
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
        ufs_path: &str,
        curvine_path: &str,
        mount_opt: MountOptions,
    ) -> FsResult<MountResponse> {
        // Create a request
        let req = MountRequest {
            ufs_path: ufs_path.to_string(),
            curvine_path: curvine_path.to_string(),
            mount_options: Some(mount_opt),
        };

        let rep: MountResponse = self.rpc(RpcCode::Mount, req).await?;
        Ok(rep)
    }

    pub async fn umount(&self, mnt_path: &str) -> FsResult<UnMountResponse> {
        //TODO umount options
        let req = UnMountRequest {
            curvine_path: mnt_path.to_string(),
            unmount_options: None,
        };

        let rep: UnMountResponse = self.rpc(RpcCode::UnMount, req).await?;
        Ok(rep)
    }

    pub async fn get_mount_point(&self, ufs_path: &str) -> FsResult<GetMountPointInfoResponse> {
        //TODO umount options
        let req = GetMountPointInfoRequest {
            ufs_path: Some(ufs_path.to_string()),
            curvine_path: None, //havn't inuse
        };

        let rep: GetMountPointInfoResponse = self.rpc(RpcCode::GetMountInfo, req).await?;
        Ok(rep)
    }

    pub async fn get_ufs_conf(&self, ufs_path: &str) -> FsResult<UfsConf> {
        let resp = self.get_mount_point(ufs_path).await?;
        let conf = match resp.mount_point {
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

    pub async fn get_all_mounts(&self) -> FsResult<Vec<MountInfo>> {
        let req = GetMountTableRequest {};
        let rep: GetMountTableResponse = self.rpc(RpcCode::GetMountTable, req).await?;

        let mut res = Vec::with_capacity(rep.mount_points.len());
        for item in rep.mount_points {
            let mount_info = MountInfo {
                mount_id: item.mount_id,
                curvine_path: item.curvine_path,
                ufs_path: item.ufs_path,
                properties: item.properties,
                auto_cache: item.auto_cache,
                cache_ttl_secs: item.cache_ttl_secs,
                consistency_strategy: curvine_common::state::ConsistencyStrategy::Always,
            };
            res.push(mount_info);
        }

        Ok(res)
    }

    // async cache loading task
    pub async fn async_cache(
        &self,
        path: &Path,
        ttl: Option<String>,
        recursive: bool,
    ) -> FsResult<CacheJobResult> {
        let req = LoadJobRequest {
            path: path.full_path().to_owned(),
            ttl,
            recursive: ternary!(recursive, Some(recursive), None),
        };

        let rep: LoadJobResponse = self.rpc(RpcCode::SubmitLoadJob, req).await?;
        Ok(CacheJobResult {
            job_id: rep.job_id,
            target_path: rep.target_path,
        })
    }

    pub async fn set_attr(&self, path: &Path, opts: SetAttrOpts) -> FsResult<()> {
        let req = SetAttrRequest {
            path: path.encode(),
            opts: ProtoUtils::set_attr_opts_to_pb(opts),
        };
        let _: SetAttrResponse = self.rpc(RpcCode::SetAttr, req).await?;
        Ok(())
    }

    pub async fn symlink(&self, target: &Path, link: &Path, force: bool) -> FsResult<()> {
        let req = SymlinkRequest {
            target: target.encode(),
            link: link.encode(),
            force,
            mode: ClientConf::DEFAULT_FILE_SYSTEM_MODE,
        };
        let _: SymlinkResponse = self.rpc(RpcCode::Symlink, req).await?;
        Ok(())
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
