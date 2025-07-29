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

use bytes::BytesMut;
use curvine_common::conf::UfsConf;
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use futures::StreamExt;
use opendal::services::{Azblob, Gcs, S3};
use opendal::{layers::LoggingLayer, Operator};
use orpc::sys::DataSlice;

/// OpenDAL Reader implementation
pub struct OpendalReader {
    operator: Operator,
    path: Path,
    object_path: String,
    length: i64,
    pos: i64,
    chunk: DataSlice,
    chunk_size: usize,
    byte_stream: Option<opendal::FuturesBytesStream>,
}

impl Reader for OpendalReader {
    fn path(&self) -> &Path {
        &self.path
    }

    fn len(&self) -> i64 {
        self.length
    }

    fn chunk_mut(&mut self) -> &mut DataSlice {
        &mut self.chunk
    }

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn pos(&self) -> i64 {
        self.pos
    }

    fn pos_mut(&mut self) -> &mut i64 {
        &mut self.pos
    }

    async fn read_chunk0(&mut self) -> FsResult<DataSlice> {
        // Initialize stream if needed
        if self.byte_stream.is_none() {
            let reader = self
                .operator
                .reader_with(&self.object_path)
                .chunk(self.chunk_size)
                .concurrent(4)
                .await
                .map_err(|e| FsError::common(format!("Failed to create reader: {}", e)))?;

            self.byte_stream = Some(
                reader
                    .into_bytes_stream(..self.length as u64)
                    .await
                    .map_err(|e| FsError::common(format!("Failed to create stream: {}", e)))?,
            );
        }

        if let Some(stream) = &mut self.byte_stream {
            if let Some(chunk_result) = stream.next().await {
                match chunk_result {
                    Ok(chunk) => Ok(DataSlice::Bytes(chunk)),
                    Err(e) => Err(FsError::common(format!("Failed to read chunk: {}", e))),
                }
            } else {
                Ok(DataSlice::Empty)
            }
        } else {
            Ok(DataSlice::Empty)
        }
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 || pos > self.length {
            return Err(FsError::common("Invalid seek position"));
        }

        // If seeking backward or forward significantly, reset the stream
        if pos < self.pos || pos > self.pos + (self.chunk_size as i64 * 2) {
            self.byte_stream = None;
            self.chunk = DataSlice::Empty;

            // Create new stream starting from the seek position
            let reader = self
                .operator
                .reader_with(&self.object_path)
                .chunk(self.chunk_size)
                .concurrent(4)
                .await
                .map_err(|e| FsError::common(format!("Failed to create reader: {}", e)))?;

            self.byte_stream = Some(
                reader
                    .into_bytes_stream(pos as u64..self.length as u64)
                    .await
                    .map_err(|e| FsError::common(format!("Failed to create stream: {}", e)))?,
            );
        } else {
            // Skip forward in the current stream
            while self.pos < pos {
                let skip_bytes = (pos - self.pos).min(self.chunk_size as i64) as usize;
                if self.chunk.is_empty() {
                    self.chunk = self.read_chunk0().await?;
                }
                if self.chunk.is_empty() {
                    break;
                }
                let actual_skip = skip_bytes.min(self.chunk.len());
                self.chunk.advance(actual_skip);
                self.pos += actual_skip as i64;
            }
        }

        self.pos = pos;
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.byte_stream = None;
        self.chunk = DataSlice::Empty;
        Ok(())
    }
}

/// OpenDAL Writer implementation
pub struct OpendalWriter {
    operator: Operator,
    path: Path,
    object_path: String,
    status: FileStatus,
    pos: i64,
    chunk: BytesMut,
    chunk_size: usize,
    writer: Option<opendal::Writer>,
}

impl Writer for OpendalWriter {
    fn status(&self) -> &FileStatus {
        &self.status
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn pos(&self) -> i64 {
        self.pos
    }

    fn pos_mut(&mut self) -> &mut i64 {
        &mut self.pos
    }

    fn chunk_mut(&mut self) -> &mut BytesMut {
        &mut self.chunk
    }

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    async fn write_chunk(&mut self, chunk: DataSlice) -> FsResult<i64> {
        if self.writer.is_none() {
            self.writer = Some(
                self.operator
                    .writer(&self.object_path)
                    .await
                    .map_err(|e| FsError::common(format!("Failed to create writer: {}", e)))?,
            );
        }

        let data = match chunk {
            DataSlice::Empty => return Ok(0),
            DataSlice::Bytes(bytes) => bytes,
            DataSlice::Buffer(buf) => buf.freeze(),
            DataSlice::IOSlice(_) => {
                return Err(FsError::common("IOSlice not supported for writing"))
            }
            DataSlice::MemSlice(_) => {
                return Err(FsError::common("MemSlice not supported for writing"))
            }
        };

        let len = data.len() as i64;

        let writer = self.writer.as_mut().unwrap();
        writer
            .write(data)
            .await
            .map_err(|e| FsError::common(format!("Failed to write: {}", e)))?;

        Ok(len)
    }

    async fn flush(&mut self) -> FsResult<()> {
        self.flush_chunk().await?;
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.flush().await?;

        if let Some(writer) = self.writer.as_mut() {
            writer
                .close()
                .await
                .map_err(|e| FsError::common(format!("Failed to close writer: {}", e)))?;
        }

        self.writer = None;
        Ok(())
    }

    async fn cancel(&mut self) -> FsResult<()> {
        self.writer = None;
        Ok(())
    }
}

/// OpenDAL file system implementation
#[derive(Clone)]
pub struct OpendalFileSystem {
    operator: Operator,
    scheme: String,
    bucket_or_container: String,
}

impl OpendalFileSystem {
    pub fn new(path: &Path, conf: UfsConf) -> FsResult<Self> {
        let scheme = path
            .scheme()
            .ok_or_else(|| FsError::invalid_path(path.full_path(), "Missing scheme"))?;

        let bucket_or_container = path
            .authority()
            .ok_or_else(|| {
                FsError::invalid_path(path.full_path(), "URI missing bucket/container name")
            })?
            .to_string();

        let operator = match scheme {
            "s3" => {
                let mut builder = S3::default();
                builder = builder.bucket(&bucket_or_container);

                if let Some(endpoint) = conf.get("fs.s3a.endpoint") {
                    builder = builder.endpoint(endpoint);
                }
                if let Some(region) = conf.get("fs.s3a.region") {
                    builder = builder.region(region);
                }
                if let Some(access_key) = conf.get("fs.s3a.access.key") {
                    builder = builder.access_key_id(access_key);
                }
                if let Some(secret_key) = conf.get("fs.s3a.secret.key") {
                    builder = builder.secret_access_key(secret_key);
                }

                Operator::new(builder)
                    .map_err(|e| FsError::common(format!("Failed to create S3 operator: {}", e)))?
                    .layer(LoggingLayer::default())
                    .finish()
            }
            "gcs" | "gs" => {
                let mut builder = Gcs::default();
                builder = builder.bucket(&bucket_or_container);

                if let Some(service_account) = conf.get("fs.gcs.service_account") {
                    builder = builder.credential(service_account);
                }
                if let Some(endpoint) = conf.get("fs.gcs.endpoint") {
                    builder = builder.endpoint(endpoint);
                }

                Operator::new(builder)
                    .map_err(|e| FsError::common(format!("Failed to create GCS operator: {}", e)))?
                    .layer(LoggingLayer::default())
                    .finish()
            }
            "azblob" => {
                let mut builder = Azblob::default();
                builder = builder.container(&bucket_or_container);

                if let Some(account_name) = conf.get("fs.azure.account_name") {
                    builder = builder.account_name(account_name);
                }
                if let Some(account_key) = conf.get("fs.azure.account_key") {
                    builder = builder.account_key(account_key);
                }
                if let Some(endpoint) = conf.get("fs.azure.endpoint") {
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

        Ok(Self {
            operator,
            scheme: scheme.to_string(),
            bucket_or_container,
        })
    }

    fn get_object_path(&self, path: &Path) -> FsResult<String> {
        if path.is_root() {
            Ok("/".to_string())
        } else {
            // Remove the scheme and bucket from the path
            let full_path = path.path();
            Ok(full_path.trim_start_matches('/').to_string())
        }
    }
}

impl FileSystem<OpendalWriter, OpendalReader, UfsConf> for OpendalFileSystem {
    fn conf(&self) -> &UfsConf {
        // OpenDAL doesn't store conf as all configuration is already applied to the operator
        // This is a limitation of the current design, but it's acceptable for OpenDAL
        static EMPTY_CONF: std::sync::OnceLock<UfsConf> = std::sync::OnceLock::new();
        EMPTY_CONF.get_or_init(UfsConf::new)
    }

    async fn mkdir(&self, path: &Path, _create_parent: bool) -> FsResult<bool> {
        let object_path = self.get_object_path(path)?;

        self.operator
            .create_dir(&object_path)
            .await
            .map_err(|e| FsError::common(format!("Failed to create directory: {}", e)))?;

        Ok(true)
    }

    async fn create(&self, path: &Path, _overwrite: bool) -> FsResult<OpendalWriter> {
        let object_path = self.get_object_path(path)?;

        let status = FileStatus {
            path: path.full_path().to_owned(),
            name: path.name().to_owned(),
            is_dir: false,
            mtime: 0,
            is_complete: false,
            len: 0,
            replicas: 1,
            block_size: 4 * 1024 * 1024,
            ..Default::default()
        };

        Ok(OpendalWriter {
            operator: self.operator.clone(),
            path: path.clone(),
            object_path,
            status,
            pos: 0,
            chunk: BytesMut::with_capacity(8 * 1024 * 1024),
            chunk_size: 8 * 1024 * 1024,
            writer: None,
        })
    }

    async fn append(&self, path: &Path) -> FsResult<OpendalWriter> {
        // OpenDAL doesn't support append for most backends
        // For now, return an error
        Err(FsError::unsupported(format!(
            "Append operation not supported for {}",
            path.full_path()
        )))
    }

    async fn exists(&self, path: &Path) -> FsResult<bool> {
        let object_path = self.get_object_path(path)?;

        match self.operator.stat(&object_path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(FsError::common(format!("Failed to check existence: {}", e))),
        }
    }

    async fn open(&self, path: &Path) -> FsResult<OpendalReader> {
        let object_path = self.get_object_path(path)?;

        let metadata = self
            .operator
            .stat(&object_path)
            .await
            .map_err(|e| FsError::common(format!("Failed to stat file: {}", e)))?;

        Ok(OpendalReader {
            operator: self.operator.clone(),
            path: path.clone(),
            object_path,
            length: metadata.content_length() as i64,
            pos: 0,
            chunk: DataSlice::Empty,
            chunk_size: 8 * 1024 * 1024,
            byte_stream: None,
        })
    }

    async fn rename(&self, src: &Path, dst: &Path) -> FsResult<bool> {
        let src_path = self.get_object_path(src)?;
        let dst_path = self.get_object_path(dst)?;

        self.operator
            .rename(&src_path, &dst_path)
            .await
            .map_err(|e| FsError::common(format!("Failed to rename: {}", e)))?;

        Ok(true)
    }

    async fn delete(&self, path: &Path, recursive: bool) -> FsResult<()> {
        let object_path = self.get_object_path(path)?;

        if recursive {
            // Check if it's a directory
            match self.operator.stat(&object_path).await {
                Ok(metadata) if metadata.is_dir() => self.operator.remove_all(&object_path).await,
                _ => self.operator.delete(&object_path).await,
            }
        } else {
            self.operator.delete(&object_path).await
        }
        .map_err(|e| FsError::common(format!("Failed to delete: {}", e)))?;

        Ok(())
    }

    async fn get_status(&self, path: &Path) -> FsResult<FileStatus> {
        let object_path = self.get_object_path(path)?;

        let metadata = self
            .operator
            .stat(&object_path)
            .await
            .map_err(|e| FsError::common(format!("Failed to stat: {}", e)))?;

        Ok(FileStatus {
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
        })
    }

    async fn list_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        let object_path = self.get_object_path(path)?;

        let list_result = self
            .operator
            .list(&object_path)
            .await
            .map_err(|e| FsError::common(format!("Failed to list directory: {}", e)))?;

        let mut statuses = Vec::new();
        for entry in list_result {
            let entry_path = format!(
                "{}://{}/{}",
                self.scheme,
                self.bucket_or_container,
                entry.path()
            );

            let metadata = entry.metadata();
            statuses.push(FileStatus {
                path: entry_path.clone(),
                name: entry.name().to_owned(),
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
            });
        }

        Ok(statuses)
    }
}
