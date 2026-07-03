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
use bytes::BytesMut;
use curvine_client::unified::{UnifiedReader, UnifiedWriter};
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::FreeResult;
use curvine_common::FsResult;
use std::sync::Arc;

#[derive(Clone)]
pub struct FileSystemClient {
    session: Arc<Session>,
}

impl FileSystemClient {
    pub(crate) fn new(session: Arc<Session>) -> Self {
        Self { session }
    }

    fn unified(&self) -> &curvine_client::unified::UnifiedFileSystem {
        self.session.unified()
    }

    pub async fn mkdir(&self, path: impl AsRef<str>, create_parent: bool) -> FsResult<bool> {
        let path = Path::from_str(path)?;
        self.unified().mkdir(&path, create_parent).await
    }

    pub async fn rename(&self, src: impl AsRef<str>, dst: impl AsRef<str>) -> FsResult<bool> {
        let src_path = Path::from_str(src)?;
        let dst_path = Path::from_str(dst)?;
        self.unified().rename(&src_path, &dst_path).await
    }

    pub async fn delete(&self, path: impl AsRef<str>, recursive: bool) -> FsResult<()> {
        let path = Path::from_str(path)?;
        self.unified().delete(&path, recursive).await
    }

    pub async fn get_status(&self, path: impl AsRef<str>) -> FsResult<BytesMut> {
        let path = Path::from_str(path)?;
        self.unified().get_status_bytes(&path).await
    }

    pub async fn list_status(&self, path: impl AsRef<str>) -> FsResult<BytesMut> {
        let path = Path::from_str(path)?;
        self.unified().list_status_bytes(&path).await
    }

    pub async fn open(&self, path: impl AsRef<str>) -> FsResult<UnifiedReader> {
        let path = Path::from_str(path)?;
        self.unified().open(&path).await
    }

    pub async fn create(&self, path: impl AsRef<str>, overwrite: bool) -> FsResult<UnifiedWriter> {
        let path = Path::from_str(path)?;
        self.unified().create(&path, overwrite).await
    }

    pub async fn append(&self, path: impl AsRef<str>) -> FsResult<UnifiedWriter> {
        let path = Path::from_str(path)?;
        self.unified().append(&path).await
    }

    pub async fn free(&self, path: impl AsRef<str>, recursive: bool) -> FsResult<FreeResult> {
        let path = Path::from_str(path)?;
        self.unified().free(&path, recursive).await
    }

    pub async fn cleanup(&self) {
        self.unified().cleanup().await
    }
}
