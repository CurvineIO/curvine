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

#![allow(unused)]

use curvine_common::error::FsError;
use curvine_common::fs::{CurvineURI, FileSystem, Path};
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use std::sync::Arc;
use curvine_client::unified::{UfsFileSystem, UnifiedReader, UnifiedWriter};
use curvine_ufs::fs::ufs_context::UFSContext;

#[derive(Clone)]
pub struct UfsClient {
    fs: Arc<UfsFileSystem>,
    context: Arc<UFSContext>,
}

impl UfsClient {
    pub fn new(context: Arc<UFSContext>) -> FsResult<Self> {
        let fs = Self::create_filesystem(context.clone())?;
        Ok(UfsClient { fs, context: context.clone() })
    }
    
    fn create_filesystem(context: Arc<UFSContext>) -> FsResult<Arc<UfsFileSystem>> {
        match context.get_scheme() {
            Some("s3") => {
                let s3_fs = UfsFileSystem::with_scheme(context.get_scheme(), context.conf().get_config().clone())?;
                Ok(Arc::new(s3_fs))
            }
            Some("oss") => {
                let oss_fs = UfsFileSystem::with_scheme(context.get_scheme(), context.conf().get_config().clone())?;
                Ok(Arc::new(oss_fs))
            }
            Some("file") => {
                Err(FsError::unsupported("Local filesystem"))
            }
            Some(_) => Err(FsError::unsupported("storage scheme")),
            None => Err(FsError::unsupported("Missing storage scheme")),
        }
    }
    
    pub async fn is_dir(&self, path: &CurvineURI) -> FsResult<bool> {
        self.fs.get_status(path).await.map(|status| status.is_dir)
    }

    pub async fn list_dir(
        &self,
        path: &CurvineURI,
        recursive: bool,
    ) -> FsResult<Vec<String>> {
        self.list_dir_impl(path, recursive).await
    }

    async fn list_dir_impl(
        &self,
        path: &CurvineURI,
        recursive: bool,
    ) -> FsResult<Vec<String>> {
        let status = self.fs.list_status(path).await?;
        let mut result = vec![];

        for file_status in status {
            result.push(file_status.path.clone());

            if recursive && file_status.is_dir {
                let sub_path = Path::from_str(&file_status.path)?;
                let sub_files = Box::pin(self.list_dir_impl(&sub_path, true)).await?;
                result.extend(sub_files);
            }
        }

        Ok(result)
    }

    pub async fn open(&self, uri: &CurvineURI) -> FsResult<UnifiedReader> {
        self.fs.open(uri).await
    }
    pub async fn exists(&self, path: &CurvineURI) -> FsResult<bool> {
        self.fs.exists(path).await
    }

    pub async fn delete(&self, path: &CurvineURI, recursive: bool) -> FsResult<()> {
        self.fs.delete(path, recursive).await
    }

    pub async fn rename(&self, src: &CurvineURI, dst: &CurvineURI) -> FsResult<bool> {
        self.fs.rename(src, dst).await
    }

    pub async fn create(&self, uri: &CurvineURI, overwrite: bool) -> FsResult<UnifiedWriter> {
        self.fs.create(uri, overwrite).await
    }

    pub async fn get_file_status(&self, path: &CurvineURI) -> FsResult<FileStatus> {
        self.fs.get_status(path).await
    }
}
