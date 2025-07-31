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

use crate::common::ufs_manager::UfsManager;
use axum::async_trait;
use bytes::Bytes;
use curvine_client::file::{FsClient, FsWriter};
use curvine_common::fs::Path;
use curvine_common::fs::{CurvineURI, Writer};
use curvine_common::state::{CreateFileOptsBuilder, TtlAction};
use curvine_common::FsResult;
use curvine_ufs::fs::buffer_transfer::{AsyncChunkReader, AsyncChunkWriter};
use orpc::CommonResult;
use std::io::Result as IoResult;
use std::io::Error;
use std::sync::Arc;

/// External storage client
///
/// Responsible for reading data from external storage (such as S3, OSS, etc.) and writing to the Curvine file system
pub struct UfsConnector {
    /// Source path
    source_path: String,
    /// Storage factory
    fs_client: Arc<FsClient>,
    /// UFS Manager
    ufs_manager: UfsManager,
}

impl UfsConnector {
    /// Create a new external storage client
    pub fn new(source_path: String, fs_client: Arc<FsClient>) -> Self {
        let ufs_manager = UfsManager::new(fs_client.clone());
        Self {
            source_path,
            fs_client: fs_client.clone(),
            ufs_manager,
        }
    }

    pub(crate) async fn get_file_size(&mut self) -> CommonResult<u64> {
        let reader = self.create_reader().await?;
        Ok(reader.content_length())
    }

    /// Create an external storage reader
    pub async fn create_reader(&mut self) -> FsResult<Box<dyn AsyncChunkReader + Send + 'static>> {
        let uri = CurvineURI::new(&self.source_path)?;
        let ufs_client = self.ufs_manager.get_client(&uri).await?;
        let reader = ufs_client.open(&uri).await?;
        Ok(reader)
    }

    /// Create a Curvine file system writer
    pub async fn create_curvine_writer(
        &mut self,
        ufs_mtime: i64,
        ttl_ms: Option<i64>,
        ttl_action: Option<i32>,
    ) -> FsResult<Box<dyn AsyncChunkWriter + Send + 'static>> {
        // Create a target path object
        let uri = CurvineURI::new(&self.source_path)?;
        let target_path = self.ufs_manager.get_curvine_path(&uri).await?.into();
        let writer = CurvineFsWriter::new(
            self.fs_client.clone(),
            target_path,
            true,
            ufs_mtime,
            ttl_ms,
            ttl_action,
        )
        .await;
        Ok(Box::new(writer))
    }
}

/// Curvine file system writer implements AsyncChunkWriter
pub struct CurvineFsWriter {
    writer: FsWriter,
}

impl CurvineFsWriter {
    /// Creates a new CurvineFsWriter with optional TTL settings
    pub async fn new(
        fs_client: Arc<FsClient>,
        path: Path,
        overwrite: bool,
        ufs_mtime: i64,
        ttl_ms: Option<i64>,
        ttl_action: Option<i32>,
    ) -> Self {
        let fs_context = fs_client.context();
        let client_conf = fs_context.cluster_conf().client;

        let mut opts_builder = CreateFileOptsBuilder::with_conf(&client_conf)
            .create_parent(true)
            .overwrite(overwrite)
            .ufs_mtime(ufs_mtime);

        if let Some(ttl_ms_value) = ttl_ms {
            opts_builder = opts_builder.ttl_ms(ttl_ms_value);
        }

        if let Some(ttl_action_value) = ttl_action {
            let action = TtlAction::from(ttl_action_value);
            opts_builder = opts_builder.ttl_action(action);
        }

        let opts = opts_builder.build();
        let status = fs_client.create_with_opts(&path, opts).await.unwrap();
        let writer = FsWriter::create(Arc::clone(&fs_client.context()), path, status);

        Self { writer }
    }
}

#[async_trait]
impl AsyncChunkWriter for CurvineFsWriter {
    /// Write Bytes data to stream
    async fn write_chunk(&mut self, data: Bytes) -> IoResult<()> {
        self.writer
            .write(data.as_ref())
            .await
            .map_err(|e| Error::other(format!("Curvine write error: {}", e)))
    }

    /// Refresh the buffer
    async fn flush(&mut self) -> IoResult<()> {
        self.writer
            .complete()
            .await
            .map_err(|e| Error::other(format!("Curvine complete error: {}", e)))
    }
}
