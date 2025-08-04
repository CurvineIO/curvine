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

use crate::file::{FsClient, FsContext, FsReader, FsReaderBase, FsWriter, FsWriterBase};
use bytes::BytesMut;
use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::fs::{Path, Reader, Writer};
use curvine_common::state::{
    CacheJobResult, CreateFileOpts, CreateFileOptsBuilder, FileBlocks, FileStatus, MasterInfo,
    MkdirOpts, MkdirOptsBuilder, MountInfo, SetAttrOpts,
};
use curvine_common::version::GIT_VERSION;
use curvine_common::FsResult;
use log::info;
use orpc::client::ClientConf;
use orpc::err_ext;
use orpc::runtime::{RpcRuntime, Runtime};
use std::sync::Arc;

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

    pub async fn mkdir_with_opts(&self, path: &Path, opts: MkdirOpts) -> FsResult<bool> {
        self.fs_client.mkdir(path, opts).await
    }

    pub async fn mkdir(&self, path: &Path, create_parent: bool) -> FsResult<bool> {
        let opts = MkdirOptsBuilder::with_conf(&self.fs_context.conf.client)
            .create_parent(create_parent)
            .build();
        self.mkdir_with_opts(path, opts).await
    }

    pub async fn create_with_opts(&self, path: &Path, opts: CreateFileOpts) -> FsResult<FsWriter> {
        let status = self.fs_client.create_with_opts(path, opts).await?;
        let writer = FsWriter::create(self.fs_context.clone(), path.clone(), status);
        Ok(writer)
    }

    fn create_opts_builder(&self) -> CreateFileOptsBuilder {
        CreateFileOptsBuilder::with_conf(&self.fs_context.conf.client)
            .client_name(self.fs_context.clone_client_name())
    }

    pub async fn create(&self, path: &Path, overwrite: bool) -> FsResult<FsWriter> {
        let opts = self
            .create_opts_builder()
            .create(true)
            .overwrite(overwrite)
            .append(false)
            .create_parent(true)
            .build();
        self.create_with_opts(path, opts).await
    }

    pub async fn append(&self, path: &Path) -> FsResult<FsWriter> {
        let opts = self
            .create_opts_builder()
            .create(false)
            .overwrite(false)
            .append(true)
            .create_parent(false)
            .build();

        let status = self.fs_client.append(path, opts).await?;
        let writer = FsWriter::append(self.fs_context.clone(), path.clone(), status);
        Ok(writer)
    }

    // Create an FsWriterBase,
    // FsWriterBase writes data directly to the network without any optimization,
    // which may be required by model scenarios, such as custom write optimization, etc.
    pub async fn append_direct(&self, path: &Path, create: bool) -> FsResult<FsWriterBase> {
        let opts = self
            .create_opts_builder()
            .create(create)
            .overwrite(false)
            .append(true)
            .create_parent(true)
            .build();

        let status = self.fs_client.append(path, opts).await?;
        let writer = FsWriterBase::new(
            self.fs_context.clone(),
            path.clone(),
            status.file_status,
            status.last_block,
        );
        Ok(writer)
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

    pub async fn open_direct(&self, path: &Path) -> FsResult<FsReaderBase> {
        let file_blocks = self.fs_client.get_block_locations(path).await?;
        Self::check_read_status(path, &file_blocks)?;

        let reader = FsReaderBase::new(path.clone(), self.fs_context.clone(), file_blocks);
        Ok(reader)
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

    pub async fn async_cache(
        &self,
        path: &Path,
        ttl: Option<String>,
        recursive: bool,
    ) -> FsResult<CacheJobResult> {
        self.fs_client.async_cache(path, ttl, recursive).await
    }

    pub async fn get_all_mounts(&self) -> FsResult<Vec<MountInfo>> {
        self.fs_client.get_all_mounts().await
    }

    pub async fn set_attr(&self, path: &Path, opts: SetAttrOpts) -> FsResult<()> {
        self.fs_client.set_attr(path, opts).await
    }

    pub async fn symlink(&self, target: &Path, link: &Path, force: bool) -> FsResult<()> {
        self.fs_client.symlink(target, link, force).await
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
}
