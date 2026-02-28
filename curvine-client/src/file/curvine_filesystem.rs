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

use crate::block::BatchBlockWriter;
use crate::file::{FsClient, FsContext, FsReader, FsWriter, FsWriterBase};
use crate::p2p::P2pStatsSnapshot;
use crate::ClientMetrics;
use bytes::BytesMut;
use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::fs::{Path, Reader, Writer};
use curvine_common::state::CommitBlock;
use curvine_common::state::{
    CreateFileOpts, CreateFileOptsBuilder, FileAllocOpts, FileBlocks, FileLock, FileStatus,
    MasterInfo, MkdirOpts, MkdirOptsBuilder, MountInfo, MountOptions, MountType, OpenFlags,
    SetAttrOpts,
};
use curvine_common::utils::ProtoUtils;
use curvine_common::version::GIT_VERSION;
use curvine_common::FsResult;
use log::info;
use log::warn;
use orpc::client::ClientConf;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::{err_box, err_ext};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

#[derive(Clone)]
pub struct CurvineFileSystem {
    pub(crate) fs_context: Arc<FsContext>,
    pub(crate) fs_client: Arc<FsClient>,
}

impl CurvineFileSystem {
    pub fn with_rt(conf: ClusterConf, rt: Arc<Runtime>) -> FsResult<Self> {
        let fs_context = Arc::new(FsContext::with_rt(conf, rt.clone())?);
        let fs_client = FsClient::new(fs_context.clone());
        let fs = Self {
            fs_context,
            fs_client: Arc::new(fs_client),
        };

        FsContext::start_clean_task(fs.clone(), fs.fs_context.block_pool.clone());

        let c = &fs.conf().client;
        info!(
            "Create new filesystem, git version: {}, masters: {}, threads: {}-{}, \
            buffer(rw): {}-{}, conn timeout(ms): {}-{}, rpc timeout(ms): {}-{}, data timeout(ms): {}",
            GIT_VERSION,
            fs.conf().masters_string(),
            rt.io_threads(),
            rt.worker_threads(),
            c.read_chunk_size,
            c.write_chunk_size,
            c.conn_timeout_ms,
            c.conn_retry_max_duration_ms,
            c.rpc_timeout_ms,
            c.rpc_retry_max_duration_ms,
            c.data_timeout_ms
        );

        Ok(fs)
    }

    pub fn conf(&self) -> &ClusterConf {
        &self.fs_context.conf
    }

    pub fn rpc_conf(&self) -> &ClientConf {
        self.fs_context.rpc_conf()
    }

    pub async fn mkdir_with_opts(&self, path: &Path, opts: MkdirOpts) -> FsResult<FileStatus> {
        self.fs_client.mkdir(path, opts).await
    }

    pub async fn mkdir(&self, path: &Path, create_parent: bool) -> FsResult<bool> {
        let opts = MkdirOptsBuilder::with_conf(&self.fs_context.conf.client)
            .create_parent(create_parent)
            .build();
        match self.mkdir_with_opts(path, opts).await {
            Ok(_) => Ok(true),
            Err(e) => {
                if matches!(e, FsError::FileAlreadyExists(_)) {
                    Ok(false)
                } else {
                    Err(e)
                }
            }
        }
    }

    pub async fn create_with_opts(
        &self,
        path: &Path,
        opts: CreateFileOpts,
        overwrite: bool,
    ) -> FsResult<FsWriter> {
        let status = self
            .fs_client
            .create_with_opts(path, opts, overwrite)
            .await?;
        let file_blocks = FileBlocks::new(status, vec![]);
        let writer = FsWriter::create(self.fs_context.clone(), path.clone(), file_blocks);
        Ok(writer)
    }

    pub fn create_opts_builder(&self) -> CreateFileOptsBuilder {
        CreateFileOptsBuilder::with_conf(&self.fs_context.conf.client)
            .client_name(self.fs_context.clone_client_name())
    }

    pub async fn create(&self, path: &Path, overwrite: bool) -> FsResult<FsWriter> {
        let opts = self.create_opts_builder().create_parent(true).build();
        self.create_with_opts(path, opts, overwrite).await
    }

    pub async fn append(&self, path: &Path) -> FsResult<FsWriter> {
        let opts = self.create_opts_builder().create_parent(false).build();
        let flags = OpenFlags::new_append().set_create(true);
        self.open_with_opts(path, opts, flags).await
    }

    pub async fn exists(&self, path: &Path) -> FsResult<bool> {
        self.fs_client.exists(path).await
    }

    fn check_read_status(path: &Path, file_blocks: &FileBlocks) -> FsResult<()> {
        // if !file_blocks.status.is_complete {
        //     return err_box!("Cannot read from {} because it is incomplete.", path);
        // }

        if file_blocks.status.is_expired() {
            return err_ext!(FsError::file_expired(path.path()));
        }

        Ok(())
    }

    pub async fn open(&self, path: &Path) -> FsResult<FsReader> {
        let file_blocks = self.fs_client.get_block_locations(path).await?;
        Self::check_read_status(path, &file_blocks)?;

        let reader = FsReader::new(path.clone(), self.fs_context.clone(), file_blocks)?;
        Ok(reader)
    }

    pub async fn open_for_write(&self, path: &Path, overwrite: bool) -> FsResult<FsWriter> {
        let create_opts = self.create_opts_builder().create_parent(true).build();
        let flags = OpenFlags::new_write_only()
            .set_create(true)
            .set_overwrite(overwrite);
        self.open_with_opts(path, create_opts, flags).await
    }

    pub async fn open_with_opts(
        &self,
        path: &Path,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FsWriter> {
        let file_block = self.fs_client.open_with_opts(path, opts, flags).await?;
        let writer = FsWriter::new(
            self.fs_context.clone(),
            path.clone(),
            file_block,
            flags.append(),
        );
        Ok(writer)
    }

    pub async fn rename(&self, src: &Path, dst: &Path) -> FsResult<bool> {
        self.fs_client.rename(src, dst).await
    }

    pub async fn delete(&self, path: &Path, recursive: bool) -> FsResult<()> {
        self.fs_client.delete(path, recursive).await
    }

    pub async fn get_status(&self, path: &Path) -> FsResult<FileStatus> {
        self.fs_client.file_status(path).await
    }

    pub async fn get_status_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        self.fs_client.file_status_bytes(path).await
    }

    pub async fn list_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        self.fs_client.list_status(path).await
    }

    pub async fn list_status_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        self.fs_client.list_status_bytes(path).await
    }

    pub async fn list_files(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        self.fs_client.list_files(path).await
    }

    pub async fn get_block_locations(&self, path: &Path) -> FsResult<FileBlocks> {
        self.fs_client.get_block_locations(path).await
    }

    pub async fn get_master_info(&self) -> FsResult<MasterInfo> {
        self.fs_client.get_master_info().await
    }

    pub async fn get_master_info_bytes(&self) -> FsResult<BytesMut> {
        self.fs_client.get_master_info_bytes().await
    }

    pub async fn get_p2p_runtime_policy(
        &self,
    ) -> FsResult<(u64, Vec<String>, Vec<String>, String)> {
        self.fs_client.get_p2p_runtime_policy().await
    }

    pub async fn get_mount_table(&self) -> FsResult<Vec<MountInfo>> {
        let res = self.fs_client.get_mount_table().await?;
        let table = res
            .mount_table
            .into_iter()
            .map(ProtoUtils::mount_info_from_pb)
            .collect();

        Ok(table)
    }

    pub async fn mount(&self, ufs_path: &Path, cv_path: &Path, opts: MountOptions) -> FsResult<()> {
        if !opts.update && ufs_path.scheme().is_none() {
            return err_box!("ufs path {} invalid must be start with schema://", ufs_path);
        }
        if cv_path.is_root() {
            return err_box!("mount path can not be root");
        }

        if !opts.update
            && opts.mount_type == MountType::Cst
            && ufs_path.authority_path() != cv_path.path()
        {
            return err_box!(
                "for Cst mount type, the ufs path and the cv path must be identical. \
                     current ufs path: {},  current cv path: {}",
                ufs_path.authority_path(),
                cv_path.path()
            );
        }

        self.fs_client.mount(ufs_path, cv_path, opts).await?;
        Ok(())
    }

    pub async fn umount(&self, cv_path: &Path) -> FsResult<()> {
        self.fs_client.umount(cv_path).await?;
        Ok(())
    }

    pub async fn set_attr(&self, path: &Path, opts: SetAttrOpts) -> FsResult<FileStatus> {
        self.fs_client.set_attr(path, opts).await
    }

    pub async fn symlink(&self, target: &str, link: &Path, force: bool) -> FsResult<()> {
        self.fs_client.symlink(target, link, force).await
    }

    pub async fn link(&self, src_path: &Path, dst_path: &Path) -> FsResult<()> {
        self.fs_client.link(src_path, dst_path).await
    }

    pub async fn get_mount_info(&self, path: &Path) -> FsResult<Option<MountInfo>> {
        self.fs_client.get_mount_info(path).await
    }

    pub async fn get_mount_info_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        self.fs_client.get_mount_info_bytes(path).await
    }

    pub async fn resize(&self, path: &Path, opts: FileAllocOpts) -> FsResult<()> {
        let create_opts = self.create_opts_builder().create_parent(true).build();
        let flags = OpenFlags::new_write_only()
            .set_create(true)
            .set_overwrite(false);
        let file_blocks = self
            .fs_client
            .open_with_opts(path, create_opts, flags)
            .await?;

        let mut writer = FsWriterBase::new(self.fs_context.clone(), path.clone(), file_blocks, 0);
        writer.resize(opts).await?;
        writer.complete().await?;

        Ok(())
    }

    pub async fn get_lock(&self, path: &Path, lock: FileLock) -> FsResult<Option<FileLock>> {
        self.fs_client.get_lock(path, lock).await
    }

    pub async fn set_lock(&self, path: &Path, lock: FileLock) -> FsResult<Option<FileLock>> {
        self.fs_client.set_lock(path, lock).await
    }

    pub fn clone_runtime(&self) -> Arc<Runtime> {
        self.fs_context.clone_runtime()
    }

    pub fn fs_client(&self) -> Arc<FsClient> {
        self.fs_client.clone()
    }

    pub fn fs_context(&self) -> Arc<FsContext> {
        self.fs_context.clone()
    }

    pub async fn read_string(&self, path: &Path) -> FsResult<String> {
        let mut reader = self.open(path).await?;

        let len = reader.len() as usize;
        let mut buf = BytesMut::zeroed(len);

        reader.read_full(&mut buf).await?;
        reader.complete().await?;
        Ok(String::from_utf8_lossy(&buf).to_string())
    }

    pub async fn metrics_report(&self) -> FsResult<()> {
        let metrics = ClientMetrics::encode()?;
        self.fs_client.metrics_report(metrics).await
    }

    pub async fn sync_p2p_runtime_policy(&self) -> FsResult<()> {
        let Some(p2p_service) = self.fs_context.p2p_service() else {
            return Ok(());
        };
        if !p2p_service.is_enabled() {
            return Ok(());
        }

        let (version, peer_whitelist, tenant_whitelist, signature) =
            self.fs_client.get_p2p_runtime_policy().await?;
        if !p2p_service
            .sync_runtime_policy_from_master(version, peer_whitelist, tenant_whitelist, signature)
            .await
        {
            return err_box!("failed to apply p2p runtime policy from master");
        }
        Ok(())
    }

    pub fn sync_p2p_metrics(&self) {
        let (service_id, snapshot) = if let Some(p2p_service) = self.fs_context.p2p_service() {
            (p2p_service.peer_id().to_string(), p2p_service.snapshot())
        } else {
            ("disabled".to_string(), P2pStatsSnapshot::default())
        };
        FsContext::get_metrics().sync_p2p_snapshot(&service_id, &snapshot);
    }

    pub async fn write_string(&self, path: &Path, str: impl AsRef<str>) -> FsResult<()> {
        let mut writer = self.create(path, true).await?;
        writer.write(str.as_ref().as_bytes()).await?;
        writer.complete().await?;
        Ok(())
    }

    pub async fn append_string(&self, path: &Path, str: impl AsRef<str>) -> FsResult<()> {
        let mut writer = self.append(path).await?;
        writer.write(str.as_ref().as_bytes()).await?;
        writer.complete().await?;
        Ok(())
    }

    // close fs, report metrics
    pub async fn cleanup(&self) {
        let res = timeout(
            Duration::from_secs(self.conf().client.close_timeout_secs),
            self.metrics_report(),
        )
        .await;
        if let Err(e) = res {
            warn!("close {}", e);
        }
    }

    pub async fn write_batch_string(&self, files: &[(Path, &str)]) -> FsResult<()> {
        let chunk_size = self.fs_context().write_chunk_size();
        let mut batch = Vec::with_capacity(files.len());
        let mut batch_memory = 0;

        for (path, content) in files.iter() {
            let content_size: usize = content.len();

            if content_size >= chunk_size {
                self.write_string(path, content.to_string()).await?;
                continue;
            }

            if batch_memory + content_size > chunk_size {
                self.handle_batch_files(&batch).await?;
                batch.clear();
                batch_memory = 0;
            }

            batch.push((path, content));
            batch_memory += content_size;
        }

        // Final flush
        if !batch.is_empty() {
            self.handle_batch_files(&batch).await?;
        }

        Ok(())
    }

    async fn handle_batch_files(&self, files: &[(&Path, &str)]) -> FsResult<()> {
        if files.is_empty() {
            return Ok(());
        }

        // Step 1: Batch create files
        let mut create_requests = Vec::with_capacity(files.len());
        for (path, _) in files {
            let opts = self.create_opts_builder().create_parent(true).build();
            let flags = OpenFlags::new_write_only()
                .set_create(true)
                .set_overwrite(true);
            create_requests.push((path.encode(), opts, flags));
        }

        let file_statuses = self.fs_client().create_files_batch(create_requests).await?;

        // Step 2: Batch allocate blocks
        let mut add_block_requests = Vec::with_capacity(file_statuses.len());
        for ((path, _content), _status) in files.iter().zip(file_statuses.iter()) {
            add_block_requests.push(path.encode());
        }

        let allocated_blocks: Vec<curvine_common::state::LocatedBlock> = self
            .fs_client()
            .add_blocks_batch(add_block_requests)
            .await?;

        // assert if allocated_blocks is not smae with add_block_requests
        let mut batch_writer =
            BatchBlockWriter::new(self.fs_context.clone(), allocated_blocks).await?;

        // Write all data (no flushing yet)
        batch_writer.write(files).await?;

        // Step 4: Complete all files at worker side
        let commit_blocks = batch_writer.complete().await?;

        // Step 5: Batch complete at master side
        let mut complete_requests: Vec<(String, i64, Vec<CommitBlock>, String, bool)> =
            Vec::with_capacity(files.len());
        for ((path, content), commit_block) in files.iter().zip(commit_blocks.iter()) {
            complete_requests.push((
                path.encode(),
                content.len() as i64,
                vec![commit_block.clone()],
                self.fs_context().clone_client_name(),
                false,
            ));
        }
        self.fs_client()
            .complete_files_batch(complete_requests)
            .await?;
        Ok(())
    }
}
