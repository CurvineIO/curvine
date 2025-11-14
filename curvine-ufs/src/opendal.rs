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

use crate::OpendalConf;
use crate::{err_ufs, UfsUtils};
use bytes::BytesMut;
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use curvine_common::state::{FileStatus, FileType, SetAttrOpts};
use curvine_common::FsResult;
use futures::StreamExt;
use opendal::services::*;
use opendal::{
    layers::{LoggingLayer, RetryLayer, TimeoutLayer},
    Metadata, Operator,
};
use orpc::sys::DataSlice;
use std::collections::HashMap;
use std::time::Duration;

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
    status: FileStatus,
}

impl Reader for OpendalReader {
    fn status(&self) -> &FileStatus {
        &self.status
    }

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
            DataSlice::IOSlice(_) | DataSlice::MemSlice(_) => {
                // For IOSlice and MemSlice, we need to copy the data
                // This is acceptable since OpenDAL will handle the actual I/O efficiently
                let slice = chunk.as_slice();
                bytes::Bytes::copy_from_slice(slice)
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
    fn add_stability_layers(
        base_op: Operator,
        conf: &HashMap<String, String>,
    ) -> FsResult<Operator> {
        let opendal_conf = OpendalConf::from_map(conf)
            .map_err(|e| FsError::common(format!("Failed to parse OpenDAL config: {}", e)))?;

        let total_timeout_ms = opendal_conf.total_timeout_ms();

        let op = base_op
            .layer(LoggingLayer::default())
            .layer(TimeoutLayer::new().with_io_timeout(Duration::from_millis(total_timeout_ms)))
            .layer(
                RetryLayer::new()
                    .with_min_delay(Duration::from_millis(opendal_conf.retry_interval_ms))
                    .with_max_delay(Duration::from_millis(opendal_conf.retry_max_delay_ms))
                    .with_max_times(opendal_conf.retry_times as usize)
                    .with_factor(2.0)
                    .with_jitter(),
            );

        Ok(op)
    }

    pub fn new(path: &Path, conf: HashMap<String, String>) -> FsResult<Self> {
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
            #[cfg(feature = "opendal-hdfs")]
            "hdfs" | "oss" => {
                use crate::jni::{register_jvm, JVM};

                register_jvm();

                let _ = JVM.get_or_init().map_err(|e| {
                    FsError::common(format!("Failed to initialize JVM for HDFS: {}", e))
                })?;

                let mut builder = Hdfs::default();

                let namenode = if let Some(namenode_config) = conf.get("hdfs.namenode") {
                    namenode_config.clone()
                } else {
                    format!("hdfs://{}", bucket_or_container)
                };

                builder = builder.name_node(&namenode);

                let root_path = conf.get("hdfs.root").map(|s| s.as_str()).unwrap_or("/");
                builder = builder.root(root_path);

                let hdfs_user = conf
                    .get("hdfs.user")
                    .cloned()
                    .or_else(|| std::env::var("HADOOP_USER_NAME").ok())
                    .or_else(|| std::env::var("USER").ok());

                if let Some(user) = hdfs_user {
                    builder = builder.user(&user);
                }

                if let Some(ccache) = conf.get("hdfs.kerberos.ccache") {
                    builder = builder.kerberos_ticket_cache_path(ccache);
                } else if let Ok(ccache) = std::env::var("KRB5CCNAME") {
                    builder = builder.kerberos_ticket_cache_path(&ccache);
                }

                if let Some(krb5_conf) = conf.get("hdfs.kerberos.krb5_conf") {
                    std::env::set_var("KRB5_CONFIG", krb5_conf);
                }

                if conf
                    .get("hdfs.atomic_write_dir")
                    .map(|s| s == "true")
                    .unwrap_or(false)
                {
                    let atomic_dir = format!("{}/atomic_write_dir", root_path);
                    builder = builder.atomic_write_dir(&atomic_dir);
                }

                let base_op = Operator::new(builder)
                    .map_err(|e| FsError::common(format!("Failed to create HDFS operator: {}", e)))?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            #[cfg(feature = "opendal-webhdfs")]
            "webhdfs" => {
                let mut builder = Webhdfs::default();

                let endpoint = if let Some(endpoint_config) = conf.get("webhdfs.endpoint") {
                    endpoint_config.clone()
                } else {
                    format!("http://{}", bucket_or_container)
                };

                builder = builder.endpoint(&endpoint);

                let root_path = conf.get("webhdfs.root").map(|s| s.as_str()).unwrap_or("/");
                builder = builder.root(root_path);

                let atomic_dir = format!("{}/atomic_write_dir", root_path);
                builder = builder.atomic_write_dir(&atomic_dir);

                let base_op = Operator::new(builder)
                    .map_err(|e| {
                        FsError::common(format!("Failed to create WebHDFS operator: {}", e))
                    })?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            #[cfg(feature = "opendal-s3")]
            "s3" | "s3a" => {
                let mut builder = S3::default();
                builder = builder.bucket(&bucket_or_container);

                if let Some(endpoint) = conf.get("s3.endpoint_url") {
                    builder = builder.endpoint(endpoint);
                }
                if let Some(region) = conf.get("s3.region_name") {
                    builder = builder.region(region);
                }
                if let Some(access_key) = conf.get("s3.credentials.access") {
                    builder = builder.access_key_id(access_key);
                }
                if let Some(secret_key) = conf.get("s3.credentials.secret") {
                    builder = builder.secret_access_key(secret_key);
                }

                let base_op = Operator::new(builder)
                    .map_err(|e| FsError::common(format!("Failed to create S3 operator: {}", e)))?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            #[cfg(feature = "opendal-oss")]
            "oss" => {
                let mut builder = Oss::default();
                builder = builder.bucket(&bucket_or_container);

                if let Some(endpoint) = conf.get("oss.endpoint_url") {
                    builder = builder.endpoint(endpoint);
                }
                if let Some(access_key) = conf.get("oss.credentials.access") {
                    builder = builder.access_key_id(access_key);
                }
                if let Some(secret_key) = conf.get("oss.credentials.secret") {
                    builder = builder.secret_access_key(secret_key);
                }

                let base_op = Operator::new(builder)
                    .map_err(|e| FsError::common(format!("Failed to create OSS operator: {}", e)))?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            #[cfg(feature = "opendal-gcs")]
            "gcs" | "gs" => {
                let mut builder = Gcs::default();
                builder = builder.bucket(&bucket_or_container);

                if let Some(service_account) = conf.get("gcs.service_account") {
                    builder = builder.credential(service_account);
                }
                if let Some(endpoint) = conf.get("gcs.endpoint_url") {
                    builder = builder.endpoint(endpoint);
                }

                let base_op = Operator::new(builder)
                    .map_err(|e| FsError::common(format!("Failed to create GCS operator: {}", e)))?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            #[cfg(feature = "opendal-azblob")]
            "azblob" => {
                let mut builder = Azblob::default();
                builder = builder.container(&bucket_or_container);

                if let Some(account_name) = conf.get("azure.account_name") {
                    builder = builder.account_name(account_name);
                }
                if let Some(account_key) = conf.get("azure.account_key") {
                    builder = builder.account_key(account_key);
                }
                if let Some(endpoint) = conf.get("azure.endpoint_url") {
                    builder = builder.endpoint(endpoint);
                }

                let base_op = Operator::new(builder)
                    .map_err(|e| {
                        FsError::common(format!("Failed to create Azure operator: {}", e))
                    })?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            #[cfg(feature = "opendal-cos")]
            "cos" => {
                let mut builder = Cos::default();
                builder = builder.bucket(&bucket_or_container);

                if let Some(endpoint) = conf.get("cos.endpoint_url") {
                    builder = builder.endpoint(endpoint);
                }
                if let Some(access_key) = conf.get("cos.credentials.access") {
                    builder = builder.secret_id(access_key);
                }
                if let Some(secret_key) = conf.get("cos.credentials.secret") {
                    builder = builder.secret_key(secret_key);
                }

                let base_op = Operator::new(builder)
                    .map_err(|e| FsError::common(format!("Failed to create COS operator: {}", e)))?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
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
            let full_path = path.path();
            let trimmed = full_path.trim_start_matches('/');
            if trimmed.is_empty() {
                Ok("/".to_string())
            } else {
                Ok(trimmed.to_string())
            }
        }
    }

    async fn process_entry(
        &self,
        entry: opendal::Entry,
        parent_normalized: &str,
    ) -> Option<FileStatus> {
        let raw_path = format!(
            "{}://{}/{}",
            self.scheme,
            self.bucket_or_container,
            entry.path()
        );
        let entry_path = Path::from_str(&raw_path)
            .ok()?
            .normalize_uri()
            .unwrap_or(raw_path.clone());

        let metadata = entry.metadata();

        // Critical: Skip self-references to prevent infinite recursion
        if metadata.is_dir() && entry_path == parent_normalized {
            log::warn!(
                "Filtered self-reference: '{}' points to parent '{}'",
                entry_path,
                parent_normalized
            );
            return None;
        }

        // Get metadata with HDFS stat optimization
        let (mtime, content_length) =
            if self.scheme == "hdfs" && metadata.is_dir() && metadata.last_modified().is_none() {
                self.get_hdfs_metadata(&entry)
                    .await
                    .unwrap_or((946684800000, metadata.content_length() as i64))
            } else {
                (
                    metadata
                        .last_modified()
                        .map(|t| t.timestamp_millis())
                        .unwrap_or(946684800000),
                    metadata.content_length() as i64,
                )
            };

        // Clean directory names (remove trailing slash)
        let name = entry.name();
        let cleaned_name = if metadata.is_dir() && name.ends_with('/') {
            &name[..name.len() - 1]
        } else {
            name
        };

        Some(FileStatus {
            path: entry_path,
            name: cleaned_name.to_owned(),
            is_dir: metadata.is_dir(),
            file_type: if metadata.is_dir() {
                FileType::Dir
            } else {
                FileType::File
            },
            mtime,
            len: content_length,
            is_complete: true,
            replicas: 1,
            block_size: 4 * 1024 * 1024,
            ..Default::default()
        })
    }

    async fn get_hdfs_metadata(&self, entry: &opendal::Entry) -> Option<(i64, i64)> {
        self.operator.stat(entry.path()).await.ok().map(|stat| {
            (
                stat.last_modified()
                    .map(|t| t.timestamp_millis())
                    .unwrap_or(946684800000),
                stat.content_length() as i64,
            )
        })
    }

    pub fn write_status(path: &Path) -> FileStatus {
        FileStatus {
            path: path.full_path().to_owned(),
            name: path.name().to_owned(),
            is_dir: false,
            is_complete: false,
            replicas: 1,
            block_size: 4 * 1024 * 1024,
            file_type: FileType::File,
            ..Default::default()
        }
    }

    pub fn read_status(path: &Path, metadata: Metadata) -> FileStatus {
        FileStatus {
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
        }
    }
}

impl FileSystem<OpendalWriter, OpendalReader> for OpendalFileSystem {
    async fn mkdir(&self, path: &Path, _create_parent: bool) -> FsResult<bool> {
        let mut object_path = self.get_object_path(path)?;
        // Ensure directory paths end with '/' for WebHDFS
        if !object_path.ends_with('/') && !object_path.is_empty() {
            object_path.push('/');
        }
        // Debug: Creating directory

        self.operator
            .create_dir(&object_path)
            .await
            .map_err(|e| FsError::common(format!("Failed to create directory: {}", e)))?;

        Ok(true)
    }

    async fn create(&self, path: &Path, _overwrite: bool) -> FsResult<OpendalWriter> {
        let object_path = self.get_object_path(path)?;
        // Debug: Creating file

        let status = Self::write_status(path);
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
        let status = Self::read_status(path, metadata);

        Ok(OpendalReader {
            operator: self.operator.clone(),
            path: path.clone(),
            object_path,
            length: status.len,
            pos: 0,
            chunk: DataSlice::Empty,
            chunk_size: 8 * 1024 * 1024,
            byte_stream: None,
            status,
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

        let metadata = match self.operator.stat(&object_path).await {
            Ok(m) => m,
            Err(e) => {
                // Map backend NotFound to FsError::FileNotFound so FUSE can return ENOENT
                if e.kind() == opendal::ErrorKind::NotFound {
                    return Err(FsError::file_not_found(path.full_path()));
                }
                return Err(FsError::common(format!("Failed to stat: {}", e)));
            }
        };

        Ok(FileStatus {
            path: path.full_path().to_owned(),
            name: path.name().to_owned(),
            is_dir: metadata.is_dir(),
            mtime: metadata
                .last_modified()
                .map(|t| t.timestamp_millis())
                .unwrap_or(946684800000),
            is_complete: true,
            len: metadata.content_length() as i64,
            replicas: 1,
            block_size: 4 * 1024 * 1024,
            file_type: if metadata.is_dir() {
                FileType::Dir
            } else {
                FileType::File
            },
            ..Default::default()
        })
    }

    async fn list_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        // Build object path with HDFS trailing slash (needed for proper directory listing)
        let mut object_path = self.get_object_path(path)?;
        if self.scheme == "hdfs" && !object_path.ends_with('/') && !object_path.is_empty() {
            object_path.push('/');
        }

        let list_result = self
            .operator
            .list(&object_path)
            .await
            .map_err(|e| FsError::common(format!("Failed to list directory: {}", e)))?;

        // Get parent path for self-reference filtering
        let parent_normalized = path
            .normalize_uri()
            .unwrap_or_else(|| path.full_path().to_string());

        let mut statuses = Vec::new();
        for entry in list_result {
            if let Some(status) = self.process_entry(entry, &parent_normalized).await {
                statuses.push(status);
            }
        }

        Ok(statuses)
    }

    async fn set_attr(&self, _path: &Path, _opts: SetAttrOpts) -> FsResult<()> {
        err_ufs!("SetAttr operation is not supported by OpenDAL file system")
    }
}
