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

use crate::err_ufs;
use crate::fs::buffer_transfer::{AsyncChunkReader, AsyncChunkWriter};
use crate::fs::filesystem::FileSystem;
use crate::fs::ufs_context::UFSContext;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use curvine_common::error::FsError;
use curvine_common::fs::CurvineURI;
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use dashmap::DashMap;
use futures::StreamExt;
use opendal::services::{Azblob, Gcs};
use opendal::{layers::LoggingLayer, Operator};
use std::io::{Error, ErrorKind, Result as IoResult};
use std::sync::Arc;

/// OpenDAL file system implementation
#[derive(Clone)]
pub struct OpendalFileSystem {
    operator_cache: Arc<DashMap<String, Operator>>,
    context: Arc<UFSContext>,
}

impl OpendalFileSystem {
    /// Create a new OpenDAL filesystem (for S3, GCS, Azure, etc.)
    pub fn new(context: Arc<UFSContext>) -> FsResult<Self> {
        Ok(Self {
            operator_cache: Arc::new(DashMap::new()),
            context,
        })
    }

    fn get_path(&self, uri: &CurvineURI) -> FsResult<String> {
        if uri.is_root() {
            Ok("/".to_string())
        } else {
            Ok(uri.path().trim_start_matches('/').to_string())
        }
    }

    async fn get_operator(&self, uri: &CurvineURI) -> FsResult<Operator> {
        let scheme = self
            .context
            .get_scheme()
            .ok_or_else(|| FsError::invalid_path(uri.full_path(), "Missing scheme"))?;

        let bucket_or_container = uri
            .authority()
            .ok_or_else(|| {
                FsError::invalid_path(uri.full_path(), "URI missing bucket/container name")
            })?
            .to_string();

        // Check cache first
        if let Some(op) = self.operator_cache.get(&bucket_or_container) {
            return Ok(op.clone());
        }

        // Create new operator
        let operator = match scheme {
            "gcs" | "gs" => {
                let config = self.context.s3a_config();
                let mut builder = Gcs::default();

                builder = builder.bucket(&bucket_or_container);

                if let Some(service_account) = config.get("fs.gcs.service_account") {
                    builder = builder.credential(service_account);
                }
                if let Some(endpoint) = config.get("fs.gcs.endpoint") {
                    builder = builder.endpoint(endpoint);
                }

                Operator::new(builder)
                    .map_err(|e| FsError::common(format!("Failed to create GCS operator: {}", e)))?
                    .layer(LoggingLayer::default())
                    .finish()
            }
            "azblob" => {
                let config = self.context.s3a_config();
                let mut builder = Azblob::default();

                builder = builder.container(&bucket_or_container);

                if let Some(account_name) = config.get("fs.azure.account_name") {
                    builder = builder.account_name(account_name);
                }
                if let Some(account_key) = config.get("fs.azure.account_key") {
                    builder = builder.account_key(account_key);
                }
                if let Some(endpoint) = config.get("fs.azure.endpoint") {
                    builder = builder.endpoint(endpoint);
                }

                Operator::new(builder)
                    .map_err(|e| {
                        FsError::common(format!("Failed to create Azure operator: {}", e))
                    })?
                    .layer(LoggingLayer::default())
                    .finish()
            }
            _ => {
                return Err(FsError::unsupported(format!(
                    "Unsupported scheme: {}",
                    scheme
                )));
            }
        };

        // Cache the operator
        self.operator_cache
            .insert(bucket_or_container.clone(), operator.clone());

        Ok(operator)
    }
}

#[async_trait]
impl FileSystem for OpendalFileSystem {
    async fn open(&self, path: &CurvineURI) -> FsResult<Box<dyn AsyncChunkReader>> {
        let operator = self.get_operator(path).await?;
        let path_str = self.get_path(path)?;
        let reader = OpendalChunkReader::new(path, operator, path_str).await?;
        Ok(Box::new(reader))
    }

    async fn is_directory(&self, path: &CurvineURI) -> FsResult<bool> {
        let operator = self.get_operator(path).await?;
        let path_str = self.get_path(path)?;
        match operator.stat(&path_str).await {
            Ok(metadata) => Ok(metadata.is_dir()),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(false),
            Err(e) => err_ufs!(e),
        }
    }

    async fn list_directory(&self, uri: &CurvineURI, recursive: bool) -> FsResult<Vec<String>> {
        let operator = self.get_operator(uri).await?;
        let path_str = self.get_path(uri)?;
        let mut entries = Vec::new();

        let list_result = if recursive {
            operator.list_with(&path_str).recursive(true).await
        } else {
            operator.list(&path_str).await
        }
        .map_err(|e| FsError::common(format!("Failed to list directory: {}", e)))?;

        for entry in list_result {
            // Build full URI for the entry
            let full_uri = format!(
                "{}://{}/{}",
                uri.scheme().unwrap_or(""),
                uri.authority().unwrap_or(""),
                entry.path()
            );
            entries.push(full_uri);
        }

        Ok(entries)
    }

    async fn exists(&self, uri: &CurvineURI) -> FsResult<bool> {
        let operator = self.get_operator(uri).await?;
        let path_str = self.get_path(uri)?;
        match operator.stat(&path_str).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(false),
            Err(e) => err_ufs!(e),
        }
    }

    async fn create_directory(&self, uri: &CurvineURI) -> FsResult<()> {
        let operator = self.get_operator(uri).await?;
        let path_str = self.get_path(uri)?;
        operator
            .create_dir(&path_str)
            .await
            .map_err(|e| FsError::common(format!("Failed to create directory: {}", e)))?;
        Ok(())
    }

    async fn delete(&self, uri: &CurvineURI, recursive: bool) -> FsResult<()> {
        let operator = self.get_operator(uri).await?;
        let path_str = self.get_path(uri)?;

        if recursive && self.is_directory(uri).await? {
            operator.remove_all(&path_str).await
        } else {
            operator.delete(&path_str).await
        }
        .map_err(|e| FsError::common(format!("Failed to delete: {}", e)))?;

        Ok(())
    }

    async fn rename(&self, src: &CurvineURI, dst: &CurvineURI) -> FsResult<()> {
        // For rename, both URIs should be in the same bucket/container
        let src_operator = self.get_operator(src).await?;
        let src_path = self.get_path(src)?;
        let dst_path = self.get_path(dst)?;

        src_operator
            .rename(&src_path, &dst_path)
            .await
            .map_err(|e| FsError::common(format!("Failed to rename: {}", e)))?;
        Ok(())
    }

    async fn create(&self, uri: &CurvineURI) -> FsResult<Box<dyn AsyncChunkWriter>> {
        let operator = self.get_operator(uri).await?;
        let path_str = self.get_path(uri)?;
        let writer = OpendalChunkWriter::new(operator, path_str);
        Ok(Box::new(writer))
    }

    async fn mount(&self, _src: &CurvineURI, _dst: &CurvineURI) -> FsResult<()> {
        // Not supported for cloud storage
        Err(FsError::unsupported("Mount operation not supported"))
    }

    async fn get_file_status(&self, path: &CurvineURI) -> FsResult<Option<FileStatus>> {
        let operator = self.get_operator(path).await?;
        let path_str = self.get_path(path)?;

        match operator.stat(&path_str).await {
            Ok(metadata) => {
                let status = FileStatus {
                    path: path.full_path().to_owned(),
                    name: path.name().to_owned(),
                    is_dir: metadata.is_dir(),
                    mtime: metadata
                        .last_modified()
                        .map(|t| t.timestamp_millis())
                        .unwrap_or(0),
                    is_complete: true,
                    len: metadata.content_length() as i64,
                    replicas: 1,
                    block_size: 4 * 1024 * 1024,
                    ..Default::default()
                };
                Ok(Some(status))
            }
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(None),
            Err(e) => err_ufs!(e),
        }
    }
}

/// OpenDAL chunk reader implementation
pub struct OpendalChunkReader {
    operator: Operator,
    path: String,
    content_length: u64,
    mtime: i64,
    current_offset: u64,
    // Stream for chunked reads
    byte_stream: Option<opendal::FuturesBytesStream>,
}

impl OpendalChunkReader {
    async fn new(_uri: &CurvineURI, operator: Operator, path: String) -> IoResult<Self> {
        let metadata = operator
            .stat(&path)
            .await
            .map_err(|e| Error::new(ErrorKind::Other, format!("Failed to stat file: {}", e)))?;

        let content_length = metadata.content_length();
        let mtime = metadata
            .last_modified()
            .map(|t| t.timestamp_millis())
            .unwrap_or(0);

        Ok(Self {
            operator,
            path,
            content_length,
            mtime,
            current_offset: 0,
            byte_stream: None,
        })
    }

    /// Initialize the data stream
    async fn init_stream(&mut self) -> IoResult<()> {
        if self.byte_stream.is_none() {
            let reader = self
                .operator
                .reader_with(&self.path)
                // TODO: allow set via user configuration.
                .chunk(8 * 1024 * 1024) // 8MB chunks for efficient reading
                .concurrent(4) // 4 concurrent reads for better performance
                .await
                .map_err(|e| {
                    Error::new(ErrorKind::Other, format!("Failed to create reader: {}", e))
                })?;

            self.byte_stream = Some(
                reader
                    .into_bytes_stream(..self.content_length)
                    .await
                    .map_err(|e| {
                        Error::new(ErrorKind::Other, format!("Failed to create stream: {}", e))
                    })?,
            );
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncChunkReader for OpendalChunkReader {
    async fn read_chunk(&mut self, buf: &mut BytesMut) -> IoResult<usize> {
        // Initialize the stream if not already done
        if self.byte_stream.is_none() {
            self.init_stream().await?;
        }

        if let Some(stream) = &mut self.byte_stream {
            // Use next() to get the next chunk of data
            if let Some(chunk_result) = stream.next().await {
                match chunk_result {
                    Ok(chunk) => {
                        let bytes_read = chunk.len();
                        buf.extend_from_slice(&chunk);
                        self.current_offset += bytes_read as u64;
                        Ok(bytes_read)
                    }
                    Err(e) => Err(Error::new(
                        ErrorKind::Other,
                        format!("Failed to read chunk: {}", e),
                    )),
                }
            } else {
                // Stream has ended
                self.byte_stream = None;
                Ok(0)
            }
        } else {
            Err(Error::new(ErrorKind::Other, "Stream not initialized"))
        }
    }

    fn content_length(&self) -> u64 {
        self.content_length
    }

    fn mtime(&self) -> i64 {
        self.mtime
    }

    async fn read(&mut self, offset: u64, length: u64) -> IoResult<Bytes> {
        let end = (offset + length).min(self.content_length);
        let range = offset..end;

        self.operator
            .read_with(&self.path)
            .range(range)
            .await
            .map(|buffer| buffer.to_bytes())
            .map_err(|e| Error::new(ErrorKind::Other, format!("Failed to read: {}", e)))
    }
}

/// OpenDAL chunk writer implementation
pub struct OpendalChunkWriter {
    operator: Operator,
    path: String,
    writer: Option<opendal::Writer>,
}

impl OpendalChunkWriter {
    fn new(operator: Operator, path: String) -> Self {
        Self {
            operator,
            path,
            writer: None,
        }
    }
}

#[async_trait]
impl AsyncChunkWriter for OpendalChunkWriter {
    async fn write_chunk(&mut self, data: Bytes) -> IoResult<()> {
        if self.writer.is_none() {
            self.writer = Some(
                self.operator
                    .writer_with(&self.path)
                    // TODO: allow set via user configuration.
                    .chunk(8 * 1024 * 1024)
                    .concurrent(4)
                    .await
                    .map_err(|e| {
                        Error::new(ErrorKind::Other, format!("Failed to create writer: {}", e))
                    })?,
            );
        }

        let writer = self.writer.as_mut().unwrap();
        writer
            .write(data)
            .await
            .map_err(|e| Error::new(ErrorKind::Other, format!("Failed to write: {}", e)))?;

        Ok(())
    }

    async fn flush(&mut self) -> IoResult<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer
                .close()
                .await
                .map_err(|e| Error::new(ErrorKind::Other, format!("Failed to flush: {}", e)))?;
        }
        Ok(())
    }
}
