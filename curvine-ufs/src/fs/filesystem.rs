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

use crate::fs::buffer_transfer::{AsyncChunkReader, AsyncChunkWriter};
use curvine_common::fs::CurvineURI;
use curvine_common::state::FileStatus;
use curvine_common::FsResult;

/// File system abstract interface
/// Send: The implementation of FileSystem can be safely passed between threads. For example, you can move the FileSystem implementation from one thread to another.
/// Sync: The implementation of FileSystem can be shared among multiple threads. For example, you can pass an immutable reference to FileSystem to multiple threads without causing data race.
/// Define a unified file system operation interface, and different storage systems need to implement this trait
#[async_trait::async_trait]
pub trait FileSystem: Send + Sync {
    // Open returns the reader interface
    async fn open(&self, path: &CurvineURI) -> FsResult<Box<dyn AsyncChunkReader>>;

    /// Determine whether the path is a directory
    async fn is_directory(&self, path: &CurvineURI) -> FsResult<bool>;

    /// List the contents of the directory
    ///
    /// Parameters:
    /// -path: directory path
    /// -config: Configuration information
    /// -recursive: Whether to recursively list subdirectory content, default to false
    async fn list_directory(&self, uri: &CurvineURI, recursive: bool) -> FsResult<Vec<String>>;

    /// Check if the file or directory exists
    async fn exists(&self, uri: &CurvineURI) -> FsResult<bool>;

    /// Create a directory
    async fn create_directory(&self, uri: &CurvineURI) -> FsResult<()>;

    /// Delete a file or directory
    async fn delete(&self, uri: &CurvineURI, recursive: bool) -> FsResult<()>;

    /// Rename a file or directory
    async fn rename(&self, src: &CurvineURI, dst: &CurvineURI) -> FsResult<()>;

    /// The create interface returns the writer
    async fn create(&self, uri: &CurvineURI) -> FsResult<Box<dyn AsyncChunkWriter>>;

    async fn mount(&self, src: &CurvineURI, dst: &CurvineURI) -> FsResult<()>;

    async fn get_file_status(&self, path: &CurvineURI) -> FsResult<Option<FileStatus>>;
}
