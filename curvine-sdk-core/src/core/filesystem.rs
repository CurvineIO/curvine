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

use crate::core::Session;
use crate::{LibFsReader, LibFsWriter};
use bytes::BytesMut;
use curvine_config::ClusterConf;
use curvine_error::FsResult;
use curvine_fs_api::{FileSystem, Path};
use curvine_model::{FreeResult, MountInfo, MountOptions};
use orpc_rpc::runtime::RpcRuntime;

pub struct LibFilesystem {
    session: Session,
}

impl LibFilesystem {
    pub fn new(conf: ClusterConf) -> FsResult<Self> {
        Ok(Self {
            session: Session::from_cluster_conf(conf)?,
        })
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    fn rt(&self) -> &std::sync::Arc<orpc_rpc::runtime::Runtime> {
        self.session.runtime()
    }

    fn inner(&self) -> &curvine_client::unified::UnifiedFileSystem {
        self.session.unified()
    }

    pub fn mkdir(&self, path: impl AsRef<str>, create_parent: bool) -> FsResult<bool> {
        let path = Path::from_str(path)?;
        self.rt()
            .block_on(async { self.inner().mkdir(&path, create_parent).await })
    }

    pub fn rename(&self, src: impl AsRef<str>, dst: impl AsRef<str>) -> FsResult<bool> {
        let src_path = Path::from_str(src)?;
        let dst_path = Path::from_str(dst)?;

        self.rt()
            .block_on(async { self.inner().rename(&src_path, &dst_path).await })
    }

    pub fn delete(&self, path: impl AsRef<str>, recursive: bool) -> FsResult<()> {
        let path = Path::from_str(path)?;
        self.rt()
            .block_on(async { self.inner().delete(&path, recursive).await })
    }

    pub fn get_status(&self, path: impl AsRef<str>) -> FsResult<BytesMut> {
        let path = Path::from_str(path)?;
        self.rt()
            .block_on(async { self.inner().get_status_bytes(&path).await })
    }

    pub fn list_status(&self, path: impl AsRef<str>) -> FsResult<BytesMut> {
        let path = Path::from_str(path)?;
        self.rt()
            .block_on(async { self.inner().list_status_bytes(&path).await })
    }

    pub fn open(&self, path: impl AsRef<str>) -> FsResult<LibFsReader> {
        let path = Path::from_str(path)?;
        let reader = self
            .rt()
            .block_on(async { self.inner().open(&path).await })?;
        Ok(LibFsReader::new(self.rt().clone(), reader))
    }

    pub fn create(&self, path: impl AsRef<str>, overwrite: bool) -> FsResult<LibFsWriter> {
        let path = Path::from_str(path)?;
        let writer = self
            .rt()
            .block_on(async { self.inner().create(&path, overwrite).await })?;
        Ok(LibFsWriter::new(
            self.rt().clone(),
            writer,
            self.inner().conf(),
        ))
    }

    pub fn append(&self, path: impl AsRef<str>) -> FsResult<LibFsWriter> {
        let path = Path::from_str(path)?;
        let writer = self
            .rt()
            .block_on(async { self.inner().append(&path).await })?;
        Ok(LibFsWriter::new(
            self.rt().clone(),
            writer,
            self.inner().conf(),
        ))
    }

    pub fn get_master_info(&self) -> FsResult<BytesMut> {
        self.rt()
            .block_on(async { self.inner().get_master_info_bytes().await })
    }

    pub fn get_mount_info(&self, path: impl AsRef<str>) -> FsResult<BytesMut> {
        let path = Path::from_str(path)?;
        self.rt()
            .block_on(async { self.inner().get_mount_info_bytes(&path).await })
    }

    pub fn mount(
        &self,
        ufs_path: impl AsRef<str>,
        cv_path: impl AsRef<str>,
        opts: MountOptions,
    ) -> FsResult<()> {
        let ufs_path = Path::from_str(ufs_path)?;
        let cv_path = Path::from_str(cv_path)?;
        self.rt()
            .block_on(async { self.inner().mount(&ufs_path, &cv_path, opts).await })
    }

    pub fn umount(&self, cv_path: impl AsRef<str>) -> FsResult<()> {
        let cv_path = Path::from_str(cv_path)?;
        self.rt()
            .block_on(async { self.inner().umount(&cv_path).await })
    }

    pub fn get_mount_table(&self) -> FsResult<Vec<MountInfo>> {
        self.rt()
            .block_on(async { self.inner().get_mount_table().await })
    }

    pub fn toggle_path(&self, path: impl AsRef<str>, check_cache: bool) -> FsResult<Option<Path>> {
        let path = Path::from_str(path)?;
        self.rt()
            .block_on(async { self.inner().toggle_path(&path, check_cache).await })
    }

    pub fn free(&self, path: impl AsRef<str>, recursive: bool) -> FsResult<FreeResult> {
        let path = Path::from_str(path)?;
        self.rt()
            .block_on(async { self.inner().free(&path, recursive).await })
    }

    pub fn cleanup(&self) {
        self.rt().block_on(self.inner().cleanup())
    }
}
