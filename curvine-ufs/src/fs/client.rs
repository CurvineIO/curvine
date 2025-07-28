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

use super::ufs_context::UFSContext;
use crate::fs::buffer_transfer::{AsyncChunkReader, AsyncChunkWriter};
use crate::fs::factory::FileSystemFactory;
use crate::fs::filesystem::FileSystem;
use curvine_common::fs::CurvineURI;
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use std::sync::Arc;

/// Unified file system client
///
/// Encapsulate access operations to different file systems and provide a unified interface
#[derive(Clone)]
pub struct UfsClient {
    fs: Arc<dyn FileSystem>,
    context: Arc<UFSContext>,
}

impl UfsClient {
    /// Create a new file system client
    pub async fn new(context: Arc<UFSContext>) -> Self {
        let fs = FileSystemFactory::create_filesystem(context.clone())
            .await
            .unwrap();
        UfsClient {
            fs,
            context: context.clone(),
        }
    }

    /// Determine whether the path is a directory
    pub async fn is_directory(&self, path: &CurvineURI) -> FsResult<bool> {
        self.fs.is_directory(path).await
    }

    /// List the contents of the directory
    ///
    /// Parameters:
    /// -path: directory path
    /// -config: Configuration information
    /// -recursive: Whether to recursively list subdirectory content, default to false
    pub async fn list_directory(
        &self,
        path: &CurvineURI,
        recursive: bool,
    ) -> FsResult<Vec<String>> {
        self.fs.list_directory(path, recursive).await
    }

    /// Create an external storage reader
    pub async fn open(&self, uri: &CurvineURI) -> FsResult<Box<dyn AsyncChunkReader>> {
        self.fs.open(uri).await
    }

    /// Check if the file or directory exists
    pub async fn exists(&self, path: &CurvineURI) -> FsResult<bool> {
        self.fs.exists(path).await
    }

    /// Create a directory
    pub async fn create_directory(&self, path: &CurvineURI) -> FsResult<()> {
        self.fs.create_directory(path).await
    }

    /// Delete a file or directory
    pub async fn delete(&self, path: &CurvineURI, recursive: bool) -> FsResult<()> {
        self.fs.delete(path, recursive).await
    }

    /// Rename a file or directory
    pub async fn rename(&self, src: &CurvineURI, dst: &CurvineURI) -> FsResult<()> {
        self.fs.rename(src, dst).await
    }

    // Create a write file
    pub async fn create(&self, uri: &CurvineURI) -> FsResult<Box<dyn AsyncChunkWriter>> {
        self.fs.create(uri).await
    }
    pub async fn get_file_status(&self, path: &CurvineURI) -> FsResult<Option<FileStatus>> {
        self.fs.get_file_status(path).await
    }
}
