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

use crate::fs::filesystem::FileSystem;
use crate::fs::s3_filesystem::S3FileSystem;
use crate::fs::ufs_context::UFSContext;
use curvine_common::error::FsError;
use curvine_common::FsResult;
use std::sync::Arc;

/// Main functions:
/// -Unified processing of configuration parameters of different storage systems
#[derive(Clone)]
pub struct FileSystemFactory;

impl Default for FileSystemFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl FileSystemFactory {
    pub fn new() -> Self {
        FileSystemFactory {}
    }

    pub async fn create_filesystem(context: Arc<UFSContext>) -> FsResult<Arc<dyn FileSystem>> {
        match context.get_scheme() {
            Some("s3") => {
                let s3_fs = S3FileSystem::new(context)?;
                Ok(Arc::new(s3_fs))
            }
            Some("oss") => {
                // Since OSS is compatible with S3 protocol, S3FileSystem can be reused
                let oss_fs = S3FileSystem::new(context)?;
                Ok(Arc::new(oss_fs))
            }
            Some("file") => {
                // The local file system has not yet been implemented, and an error is returned
                Err(FsError::unsupported("Local filesystem"))
            }
            Some(_) => Err(FsError::unsupported("storage scheme")),
            None => Err(FsError::unsupported("Missing storage scheme")),
        }
    }
}
