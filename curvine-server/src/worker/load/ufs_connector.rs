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
use bytes::{Bytes, BytesMut};
use curvine_client::file::{FsClient, FsWriter};
use curvine_client::unified::UnifiedReader;
use curvine_common::fs::{CurvineURI, Path, Reader, Writer};
use curvine_common::proto::MountPointInfo;
use curvine_common::state::{CreateFileOptsBuilder, TtlAction};
use curvine_common::FsResult;
use curvine_ufs::fs::{AsyncChunkReader, AsyncChunkWriter};
use orpc::sys::DataSlice;
use orpc::CommonResult;
use std::io::Error;
use std::io::Result as IoResult;
use std::sync::Arc;

pub struct UfsConnector {
    source_path: String,
    fs_client: Arc<FsClient>,
    ufs_manager: UfsManager,
}

impl UfsConnector {
    pub async fn new(source_path: String, fs_client: Arc<FsClient>) -> CommonResult<Self> {
        let mut ufs_manager = UfsManager::new(fs_client.clone());
        // ufs connector will be created by the worker side,
        // and all the mount table should be retrieved from master.
        ufs_manager.sync_mount_table().await?;
        Ok(Self {
            source_path,
            fs_client: fs_client.clone(),
            ufs_manager,
        })
    }

    pub(crate) async fn get_file_size(&mut self) -> CommonResult<u64> {
        let reader = self.create_reader().await?;
        Ok(reader.content_length())
    }

    pub async fn create_reader(&mut self) -> FsResult<Box<dyn AsyncChunkReader + Send + 'static>> {
        let uri = CurvineURI::new(&self.source_path)?;
        let ufs_client = self.ufs_manager.get_client(&uri).await?;
        let reader = ufs_client.open(&uri).await?;
        let status = ufs_client.get_file_status(&uri).await?;
        Ok(Box::new(S3ReaderWrapper::new(reader, status.mtime)))
    }

    pub async fn create_curvine_writer(
        &mut self,
        ufs_mtime: i64,
        ttl_ms: Option<i64>,
        ttl_action: Option<i32>,
    ) -> FsResult<Box<dyn AsyncChunkWriter + Send + 'static>> {
        let uri = CurvineURI::new(&self.source_path)?;
        let mount_point_info = self.ufs_manager.get_mount_point_with_uri(&uri).await?;
        let target_path = self.ufs_manager.get_curvine_path(&uri).await?.into();
        let writer = CurvineFsWriter::new(
            self.fs_client.clone(),
            target_path,
            true,
            ufs_mtime,
            ttl_ms,
            ttl_action,
            mount_point_info,
        )
        .await;
        Ok(Box::new(writer))
    }
}

pub struct S3ReaderWrapper {
    reader: UnifiedReader,
    mtime: i64,
}

impl S3ReaderWrapper {
    pub fn new(reader: UnifiedReader, mtime: i64) -> Self {
        Self { reader, mtime }
    }
}

#[async_trait]
impl AsyncChunkReader for S3ReaderWrapper {
    async fn read_chunk(&mut self, buf: &mut BytesMut) -> IoResult<usize> {
        match self.reader.read_chunk0().await {
            Ok(DataSlice::Bytes(bytes)) => {
                let len = bytes.len();
                buf.extend_from_slice(&bytes);
                Ok(len)
            }
            Ok(DataSlice::Buffer(bytes_mut)) => {
                let len = bytes_mut.len();
                buf.extend_from_slice(&bytes_mut);
                Ok(len)
            }
            Ok(DataSlice::IOSlice(_io_slice)) => {
                // IOSlice doesn't support direct byte access, treat as empty for now
                // In a real implementation, you might need to handle this differently
                Ok(0)
            }
            Ok(DataSlice::MemSlice(mem_slice)) => {
                let data = mem_slice.as_slice();
                let len = data.len();
                buf.extend_from_slice(data);
                Ok(len)
            }
            Ok(DataSlice::Empty) => Ok(0),
            Err(e) => Err(Error::other(format!("S3 read error: {}", e))),
        }
    }

    fn content_length(&self) -> u64 {
        self.reader.len() as u64
    }

    fn mtime(&self) -> i64 {
        self.mtime
    }
    async fn read(&mut self, offset: u64, length: u64) -> IoResult<Bytes> {
        // Seek to the specified offset
        if let Err(e) = self.reader.seek(offset as i64).await {
            return Err(Error::other(format!("S3 seek error: {}", e)));
        }

        let mut result = BytesMut::new();
        let mut remaining = length;

        while remaining > 0 {
            match self.reader.read_chunk0().await {
                Ok(DataSlice::Bytes(bytes)) => {
                    let chunk_len = bytes.len() as u64;
                    if chunk_len <= remaining {
                        result.extend_from_slice(&bytes);
                        remaining -= chunk_len;
                    } else {
                        result.extend_from_slice(&bytes[..remaining as usize]);
                        remaining = 0;
                    }
                }
                Ok(DataSlice::Buffer(bytes_mut)) => {
                    let chunk_len = bytes_mut.len() as u64;
                    if chunk_len <= remaining {
                        result.extend_from_slice(&bytes_mut);
                        remaining -= chunk_len;
                    } else {
                        result.extend_from_slice(bytes_mut[..remaining as usize].as_ref());
                        remaining = 0;
                    }
                }
                Ok(DataSlice::IOSlice(_io_slice)) => {
                    // IOSlice doesn't support direct byte access, skip for now
                    // In a real implementation, you might need to handle this differently
                    break;
                }
                Ok(DataSlice::MemSlice(mem_slice)) => {
                    let data = mem_slice.as_slice();
                    let chunk_len = data.len() as u64;
                    if chunk_len <= remaining {
                        result.extend_from_slice(data);
                        remaining -= chunk_len;
                    } else {
                        result.extend_from_slice(&data[..remaining as usize]);
                        remaining = 0;
                    }
                }
                Ok(DataSlice::Empty) => break,
                Err(e) => return Err(Error::other(format!("S3 read error: {}", e))),
            }
        }

        Ok(result.freeze())
    }
}

pub struct CurvineFsWriter {
    writer: FsWriter,
}

impl CurvineFsWriter {
    pub async fn new(
        fs_client: Arc<FsClient>,
        path: Path,
        overwrite: bool,
        ufs_mtime: i64,
        ttl_ms: Option<i64>,
        ttl_action: Option<i32>,
        mount_point_info: MountPointInfo,
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

        opts_builder = opts_builder
            .block_size(mount_point_info.block_size as i64)
            .storage_type(mount_point_info.storage_type.into())
            .replicas(mount_point_info.replicas as i32);

        let opts = opts_builder.build();
        let status = fs_client.create_with_opts(&path, opts).await.unwrap();
        let writer = FsWriter::create(Arc::clone(&fs_client.context()), path, status);

        Self { writer }
    }
}

#[async_trait]
impl AsyncChunkWriter for CurvineFsWriter {
    async fn write_chunk(&mut self, data: Bytes) -> IoResult<()> {
        self.writer
            .write(data.as_ref())
            .await
            .map_err(|e| Error::other(format!("Curvine write error: {}", e)))
    }

    async fn flush(&mut self) -> IoResult<()> {
        self.writer
            .complete()
            .await
            .map_err(|e| Error::other(format!("Curvine complete error: {}", e)))
    }
}
