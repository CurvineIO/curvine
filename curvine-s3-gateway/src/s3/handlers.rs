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

use super::types::{PutContext, PutOperation};
use super::ListObjectContent;
use super::ListObjectHandler;
use super::ListObjectOption;
use super::ListObjectVersionsHandler;
use super::ListObjectVersionsOption;
use super::ListObjectVersionsResult;
use super::ObjectVersion;
use super::PutObjectHandler;
use super::PutObjectOption;
use crate::s3::error::Error;
use crate::s3::s3_api::HeadHandler;
use crate::s3::s3_api::HeadObjectResult;
use crate::utils::s3_utils::{
    file_status_to_head_object_result, file_status_to_list_object_content,
};
use chrono;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use curvine_common::state::FileType;
use curvine_common::FsResult;
use orpc::runtime::AsyncRuntime;

use std::sync::OnceLock;
use tokio::io::AsyncWriteExt;
use tracing;
use uuid;
pub struct S3Handlers {
    pub fs: UnifiedFileSystem,
    pub region: String,

    /// Temporary directory for all PUT operations (multipart and regular)
    /// Configurable path where all temporary files for PUT operations are stored
    pub put_temp_dir: String,

    /// Shared async runtime for executing blocking file system operations
    /// Prevents blocking the main async executor with CPU-intensive tasks
    pub rt: std::sync::Arc<AsyncRuntime>,
}

impl S3Handlers {
    /// Create a new S3Handlers instance
    ///
    /// Initializes the S3 handlers with the necessary dependencies for
    /// handling S3 operations against the Curvine file system.
    ///
    /// # Arguments
    ///
    /// * `fs` - The unified filesystem instance for storage operations
    /// * `region` - The S3 region identifier to report in responses
    /// * `put_temp_dir` - Temporary directory path for all PUT operations
    /// * `rt` - Shared runtime for scheduling internal blocking tasks
    ///
    /// # Returns
    ///
    /// * `Self` - Configured S3 handlers ready to process requests
    ///
    /// # Design Notes
    ///
    /// The handlers are designed to be cloneable and thread-safe, allowing
    /// the same instance to handle multiple concurrent requests efficiently.
    pub fn new(
        fs: UnifiedFileSystem,
        region: String,
        put_temp_dir: String,
        rt: std::sync::Arc<AsyncRuntime>,
    ) -> Self {
        tracing::debug!(
            "Creating new S3Handlers with region: {}, put_temp_dir: {}",
            region,
            put_temp_dir
        );
        Self {
            fs,
            region,
            put_temp_dir,
            rt,
        }
    }

    fn cv_object_path(&self, bucket: &str, key: &str) -> FsResult<Path> {
        tracing::debug!("Converting S3 path: s3://{}/{}", bucket, key);

        if bucket.is_empty() || key.is_empty() {
            tracing::warn!("Invalid S3 path: bucket or key is empty");
            return Err(curvine_common::error::FsError::invalid_path(
                "",
                "bucket or key is empty",
            ));
        }

        if bucket.contains('/') {
            tracing::warn!(
                "Invalid bucket name '{}': contains invalid characters",
                bucket
            );
            return Err(curvine_common::error::FsError::invalid_path(
                bucket,
                "contains invalid characters",
            ));
        }

        let path = format!("/{bucket}/{key}");
        tracing::debug!("Mapped S3 path to Curvine path: {}", path);
        Ok(Path::from_str(&path)?)
    }

    fn cv_bucket_path(&self, bucket: &str) -> FsResult<Path> {
        tracing::debug!("Converting S3 bucket: s3://{}", bucket);

        if bucket.is_empty() {
            tracing::warn!("Invalid bucket name: bucket name is empty");
            return Err(curvine_common::error::FsError::invalid_path(
                "",
                "bucket name is empty",
            ));
        }

        if bucket.contains('/') {
            tracing::warn!(
                "Invalid bucket name '{}': contains invalid characters",
                bucket
            );
            return Err(curvine_common::error::FsError::invalid_path(
                bucket,
                "contains invalid characters",
            ));
        }

        let path = format!("/{bucket}");
        tracing::debug!("Mapped S3 bucket to Curvine path: {}", path);
        Ok(Path::from_str(&path)?)
    }
}

#[async_trait::async_trait]
impl HeadHandler for S3Handlers {
    async fn lookup(&self, bucket: &str, object: &str) -> Result<Option<HeadObjectResult>, Error> {
        tracing::info!("HEAD request for s3://{}/{}", bucket, object);

        let path = match self.cv_object_path(bucket, object) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    "Failed to convert S3 path s3://{}/{}: {}",
                    bucket,
                    object,
                    e
                );
                return Ok(None);
            }
        };

        let fs = self.fs.clone();
        let object_name = object.to_string();

        let res = fs.get_status(&path).await;

        match res {
            Ok(st) if st.file_type == FileType::File => {
                tracing::debug!("Found file at path: {}, size: {}", path, st.len);

                let head = file_status_to_head_object_result(&st, &object_name);

                Ok(Some(head))
            }
            Ok(st) => {
                tracing::debug!("Path exists but is not a file: {:?}", st.file_type);
                Ok(None)
            }
            Err(e) => {
                tracing::warn!("Failed to get status for path {}: {}", path, e);
                Ok(None)
            }
        }
    }
}

#[async_trait::async_trait]
impl crate::s3::s3_api::GetObjectHandler for S3Handlers {
    fn handle<'a>(
        &'a self,
        bucket: &str,
        object: &str,
        _opt: crate::s3::s3_api::GetObjectOption,
        out: tokio::sync::Mutex<
            std::pin::Pin<Box<dyn 'a + Send + crate::utils::io::PollWrite + Unpin>>,
        >,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<(), String>>>> {
        let fs = self.fs.clone();
        let path = self.cv_object_path(bucket, object);
        let bucket = bucket.to_string();
        let object = object.to_string();

        Box::pin(async move {
            if let Some(start) = _opt.range_start {
                if let Some(end) = _opt.range_end {
                    tracing::info!(
                        "GET object s3://{}/{} with range: bytes={}-{}",
                        bucket,
                        object,
                        start,
                        end
                    );
                } else {
                    tracing::info!(
                        "GET object s3://{}/{} with range: bytes={}-",
                        bucket,
                        object,
                        start
                    );
                }
            } else {
                tracing::info!("GET object s3://{}/{}", bucket, object);
            }

            let path = path.map_err(|e| {
                tracing::error!(
                    "Failed to convert S3 path s3://{}/{}: {}",
                    bucket,
                    object,
                    e
                );
                e.to_string()
            })?;

            let mut reader = fs.open(&path).await.map_err(|e| {
                tracing::error!("Failed to open file at path {}: {}", path, e);
                e.to_string()
            })?;

            let (seek_pos, bytes_to_read) = if let Some(range_end) = _opt.range_end {
                if range_end > u64::MAX / 2 {
                    let suffix_len = u64::MAX - range_end;
                    let file_size = reader.remaining().max(0) as u64;
                    if suffix_len > file_size {
                        (None, None)
                    } else {
                        let start_pos = file_size - suffix_len;
                        tracing::debug!(
                            "Suffix range request: seeking to {} for last {} bytes",
                            start_pos,
                            suffix_len
                        );
                        (Some(start_pos), Some(suffix_len))
                    }
                } else {
                    let start = _opt.range_start.unwrap_or(0);
                    let bytes = range_end - start + 1;
                    tracing::debug!(
                        "Normal range request: seeking to {} for {} bytes",
                        start,
                        bytes
                    );
                    (Some(start), Some(bytes))
                }
            } else if let Some(start) = _opt.range_start {
                tracing::debug!("Open-ended range request: seeking to {}", start);
                (Some(start), None)
            } else {
                (None, None)
            };

            if let Some(pos) = seek_pos {
                reader.seek(pos as i64).await.map_err(|e| {
                    tracing::error!("Failed to seek to position {}: {}", pos, e);
                    e.to_string()
                })?;
            }

            let remaining_bytes = bytes_to_read;

            log::debug!(
                "GetObject: range_bytes={:?}, reader.remaining()={}",
                remaining_bytes,
                reader.remaining()
            );

            let target_read = if let Some(range_bytes) = remaining_bytes {
                range_bytes
            } else {
                reader.remaining().max(0) as u64
            };

            log::debug!("GetObject: will read {target_read} bytes directly");

            // Configurable chunk size: default 1MB, supports 512KB/1MB/2MB via env
            fn get_chunk_size() -> usize {
                static SZ: OnceLock<usize> = OnceLock::new();
                *SZ.get_or_init(|| {
                    match std::env::var("CURVINE_S3_GET_CHUNK_MB")
                        .ok()
                        .and_then(|v| v.parse::<usize>().ok())
                    {
                        Some(2) => 2 * 1024 * 1024,
                        Some(1) => 1 * 1024 * 1024,
                        Some(0) => 512 * 1024, // treat 0 as 512KB for convenience
                        Some(_) => 1 * 1024 * 1024,
                        None => 1 * 1024 * 1024,
                    }
                })
            }
            let chunk_size_conf = get_chunk_size();
            let mut total_read = 0u64;
            let mut remaining_to_read = target_read;

            // Dual-buffer prefetch: read ahead multiple chunks, write while reading next
            fn get_prefetch_depth() -> usize {
                std::env::var("CURVINE_S3_GET_PREFETCH_DEPTH")
                    .ok()
                    .and_then(|v| v.parse::<usize>().ok())
                    .map(|n| n.clamp(1, 3))
                    .unwrap_or(2)
            }
            let prefetch_depth = get_prefetch_depth();

            let mut guard = out.lock().await;

            // Pre-read multiple chunks into buffer queue for overlapped I/O
            let mut buffer_queue: Vec<Vec<u8>> = Vec::with_capacity(prefetch_depth);
            
            // Initial prefetch: read first few chunks
            for _ in 0..prefetch_depth {
                if remaining_to_read == 0 {
                    break;
                }
                
                let chunk_size = std::cmp::min(chunk_size_conf, remaining_to_read as usize);
                let mut buffer = vec![0u8; chunk_size];
                
                let bytes_read = reader
                    .read_full(&mut buffer[..chunk_size])
                    .await
                    .map_err(|e| e.to_string())?;

                if bytes_read == 0 {
                    break;
                }
                
                buffer.truncate(bytes_read);
                buffer_queue.push(buffer);
                remaining_to_read -= bytes_read as u64;
            }

            // Process buffered chunks with read-ahead
            while !buffer_queue.is_empty() {
                // Take first buffer from queue
                let current_buffer = buffer_queue.remove(0);
                let bytes_to_write = current_buffer.len();
                
                // Try to read next chunk while we have current buffer (overlapped I/O)
                if remaining_to_read > 0 && buffer_queue.len() < prefetch_depth {
                    let next_chunk_size = std::cmp::min(chunk_size_conf, remaining_to_read as usize);
                    let mut next_buffer = vec![0u8; next_chunk_size];
                    
                    let bytes_read = reader
                        .read_full(&mut next_buffer[..next_chunk_size])
                        .await
                        .map_err(|e| e.to_string())?;
                    
                    if bytes_read > 0 {
                        next_buffer.truncate(bytes_read);
                        buffer_queue.push(next_buffer);
                        remaining_to_read -= bytes_read as u64;
                    }
                }
                
                // Write current buffer using zero-copy Vec->Bytes conversion when possible
                guard.poll_write_vec(current_buffer).await.map_err(|e| {
                    tracing::error!("Failed to write chunk to output: {}", e);
                    e.to_string()
                })?;

                total_read += bytes_to_write as u64;

                if total_read % (chunk_size_conf as u64 * 16) == 0 {
                    log::trace!(
                        "GetObject: streamed {} KB (total: {} KB)",
                        bytes_to_write / 1024,
                        total_read / 1024
                    );
                }
            }

            drop(guard);

            log::debug!(
                "GetObject: streaming completed, total bytes: {}",
                total_read
            );

            if let Err(e) = reader.complete().await {
                tracing::warn!("Failed to complete reader cleanup: {}", e);
            }

            tracing::info!(
                "GET object s3://{}/{} completed, total bytes: {}",
                bucket,
                object,
                total_read
            );
            Ok(())
        })
    }
}

#[async_trait::async_trait]
impl PutObjectHandler for S3Handlers {
    async fn handle(
        &self,
        _opt: &PutObjectOption,
        bucket: &str,
        object: &str,
        body: &mut (dyn crate::utils::io::PollRead + Unpin + Send),
    ) -> Result<(), String> {
        let context = PutContext::new(
            self.fs.clone(),
            self.rt.clone(),
            bucket.to_string(),
            object.to_string(),
            self.cv_object_path(bucket, object),
        );

        PutOperation::execute(context, body).await
    }
}

#[async_trait::async_trait]
impl crate::s3::s3_api::DeleteObjectHandler for S3Handlers {
    async fn handle(
        &self,
        _opt: &crate::s3::s3_api::DeleteObjectOption,
        object: &str,
    ) -> Result<(), String> {
        // Clone necessary data for async block
        let fs = self.fs.clone();
        let object = object.to_string();
        let path = Path::from_str(format!("/{object}"));

        let path = path.map_err(|e| e.to_string())?;
        match fs.delete(&path, false).await {
            Ok(_) => Ok(()),
            Err(e) => {
                let msg = e.to_string();
                // Treat not-found as success to be S3 compatible (idempotent)
                if msg.contains("No such file")
                    || msg.contains("not exists")
                    || msg.contains("not found")
                {
                    Ok(())
                } else {
                    Err(msg)
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl crate::s3::s3_api::CreateBucketHandler for S3Handlers {
    async fn handle(
        &self,
        _opt: &crate::s3::s3_api::CreateBucketOption,
        bucket: &str,
    ) -> Result<(), String> {
        let fs = self.fs.clone();
        let bucket = bucket.to_string();
        let path = self.cv_bucket_path(&bucket);

        let path = path.map_err(|e| e.to_string())?;

        if fs.get_status(&path).await.is_ok() {
            return Err("BucketAlreadyExists".to_string());
        }

        fs.mkdir(&path, true).await.map_err(|e| e.to_string())?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::s3::s3_api::DeleteBucketHandler for S3Handlers {
    async fn handle(
        &self,
        _opt: &crate::s3::s3_api::DeleteBucketOption,
        bucket: &str,
    ) -> Result<(), String> {
        let fs = self.fs.clone();
        let bucket = bucket.to_string();
        let path = self.cv_bucket_path(&bucket);

        let path = path.map_err(|e| e.to_string())?;
        fs.delete(&path, false).await.map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl crate::s3::s3_api::ListBucketHandler for S3Handlers {
    async fn handle(
        &self,
        _opt: &crate::s3::s3_api::ListBucketsOption,
    ) -> Result<Vec<crate::s3::s3_api::Bucket>, String> {
        let mut buckets = vec![];
        let root = Path::from_str("/").map_err(|e| e.to_string())?;
        let list = self
            .fs
            .list_status(&root)
            .await
            .map_err(|e| e.to_string())?;

        for st in list {
            if st.is_dir {
                let creation_date = if st.mtime > 0 {
                    chrono::DateTime::from_timestamp(st.mtime / 1000, 0)
                        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
                        .unwrap_or_else(|| {
                            chrono::Utc::now()
                                .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                                .to_string()
                        })
                } else {
                    chrono::Utc::now()
                        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                        .to_string()
                };

                buckets.push(crate::s3::s3_api::Bucket {
                    name: st.name,
                    creation_date,
                    bucket_region: self.region.clone(),
                });
            }
        }
        Ok(buckets)
    }
}

#[async_trait::async_trait]
impl crate::s3::s3_api::GetBucketLocationHandler for S3Handlers {
    async fn handle(&self, _loc: Option<&str>) -> Result<Option<&'static str>, ()> {
        match self.region.as_str() {
            "us-east-1" => Ok(Some("us-east-1")),
            "us-west-1" => Ok(Some("us-west-1")),
            "us-west-2" => Ok(Some("us-west-2")),
            "eu-west-1" => Ok(Some("eu-west-1")),
            "eu-central-1" => Ok(Some("eu-central-1")),
            "ap-southeast-1" => Ok(Some("ap-southeast-1")),
            "ap-northeast-1" => Ok(Some("ap-northeast-1")),
            _ => {
                tracing::warn!(
                    "Unsupported region '{}', defaulting to us-east-1",
                    self.region
                );
                Ok(Some("us-east-1"))
            }
        }
    }
}

#[async_trait::async_trait]
impl crate::s3::s3_api::MultiUploadObjectHandler for S3Handlers {
    fn handle_create_session<'a>(
        &'a self,
        _bucket: &'a str,
        _key: &'a str,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<String, ()>>>> {
        Box::pin(async move {
            let upload_id = uuid::Uuid::new_v4().to_string();
            Ok(upload_id)
        })
    }

    fn handle_upload_part<'a>(
        &'a self,
        _bucket: &'a str,
        _key: &'a str,
        upload_id: &'a str,
        part_number: u32,
        body: &'a mut (dyn tokio::io::AsyncRead + Unpin + Send),
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<String, ()>>>> {
        Box::pin(async move {
            use bytes::BytesMut;
            use tokio::io::AsyncReadExt;

            // Create temporary directory for this upload session using configured path
            let dir = format!("{}/{}", self.put_temp_dir, upload_id);
            let _ = tokio::fs::create_dir_all(&dir).await;

            let path = format!("{dir}/{part_number}");
            let mut file = match tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .await
            {
                Ok(f) => f,
                Err(_) => return Err(()),
            };

            let mut hasher = md5::Context::new();
            let mut total_data = BytesMut::new();

            let mut temp_buf = vec![0u8; 1024 * 1024]; // 1MB buffer
            loop {
                let n = match body.read(&mut temp_buf).await {
                    Ok(n) => n,
                    Err(_) => return Err(()),
                };
                if n == 0 {
                    break;
                }

                let actual_data = &temp_buf[..n];
                hasher.consume(actual_data);
                total_data.extend_from_slice(actual_data);
            }

            if file.write_all(&total_data).await.is_err() {
                return Err(());
            }

            let digest = hasher.compute();
            Ok(format!("\"{digest:x}\""))
        })
    }

    fn handle_complete<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        upload_id: &'a str,
        data: &'a [(&'a str, u32)],
        _opts: crate::s3::s3_api::MultiUploadObjectCompleteOption,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<String, ()>>>> {
        let fs = self.fs.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let path_res = self.cv_object_path(&bucket, &key);

        Box::pin(async move {
            use tokio::io::AsyncReadExt;

            let final_path = match path_res {
                Ok(p) => p,
                Err(_) => return Err(()),
            };

            let mut writer = match fs.create(&final_path, true).await {
                Ok(w) => w,
                Err(_) => return Err(()),
            };

            // Prepare temporary directory path using configured multipart temp directory
            let put_temp_dir = self.put_temp_dir.clone();
            let dir = format!("{put_temp_dir}/{upload_id}");

            let mut part_list = data.to_vec();
            part_list.sort_by_key(|(_, n)| *n);

            for (_, num) in part_list {
                let path = format!("{dir}/{num}");
                let mut file = match tokio::fs::OpenOptions::new().read(true).open(&path).await {
                    Ok(f) => f,
                    Err(_) => return Err(()),
                };

                let mut buf = [0u8; 1024 * 1024]; // 1MB buffer
                loop {
                    let n = match file.read(&mut buf).await {
                        Ok(n) => n,
                        Err(_) => return Err(()),
                    };
                    if n == 0 {
                        break;
                    }

                    if writer.write(&buf[..n]).await.is_err() {
                        return Err(());
                    }
                }
            }

            if writer.complete().await.is_err() {
                return Err(());
            }

            let _ = tokio::fs::remove_dir_all(&dir).await;

            Ok("etag-not-computed".to_string())
        })
    }

    fn handle_abort<'a>(
        &'a self,
        _bucket: &'a str,
        _key: &'a str,
        upload_id: &'a str,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<(), ()>>>> {
        Box::pin(async move {
            let dir = format!("/tmp/curvine-multipart/{}", upload_id);
            let _ = tokio::fs::remove_dir_all(&dir).await;
            Ok(())
        })
    }
}

#[async_trait::async_trait]
impl ListObjectHandler for S3Handlers {
    async fn handle(
        &self,
        opt: &ListObjectOption,
        bucket: &str,
    ) -> Result<Vec<ListObjectContent>, String> {
        let bkt_path = self.cv_bucket_path(bucket).map_err(|e| e.to_string())?;

        let root = bkt_path;

        let list = self
            .fs
            .list_status(&root)
            .await
            .map_err(|e| e.to_string())?;
        let mut contents = Vec::new();

        for st in list {
            if st.is_dir {
                continue;
            }

            let key = st.name.clone();

            if let Some(pref) = &opt.prefix {
                if !key.starts_with(pref) {
                    continue;
                }
            }

            contents.push(file_status_to_list_object_content(&st, key));
        }

        Ok(contents)
    }
}

/// Implementation of ListObjectVersionsHandler trait for object versions listing
///
/// Provides S3 ListObjectVersions functionality by treating each file as a single version.
/// Since Curvine may not support true versioning, we simulate versioning by using
/// file metadata to create version information.
#[async_trait::async_trait]
impl ListObjectVersionsHandler for S3Handlers {
    /// Handle LIST object versions request
    ///
    /// Lists object versions within a specified bucket. Since Curvine doesn't support
    /// true versioning, each file is treated as a single "latest" version.
    ///
    /// # Arguments
    ///
    /// * `opt` - List object versions options including prefix and pagination parameters
    /// * `bucket` - S3 bucket name to list object versions from
    ///
    /// # Returns
    ///
    /// * `Future<Result<ListObjectVersionsResult, String>>` - Structured response with versions
    ///
    /// # Version Simulation Strategy
    ///
    /// - Each file is treated as its own latest version
    /// - Version ID is generated from file metadata (mtime + size)
    /// - All versions are marked as `IsLatest=true`
    /// - No delete markers are generated (since no versioning)
    ///
    /// # S3 Compatibility
    ///
    /// This implementation provides enough compatibility for tools like s3-benchmark
    /// to work correctly, even without true versioning support.
    async fn handle(
        &self,
        opt: &ListObjectVersionsOption,
        bucket: &str,
    ) -> Result<ListObjectVersionsResult, String> {
        tracing::info!("ListObjectVersions request for bucket: {}", bucket);

        // Convert bucket name to file system path
        let bkt_path = self.cv_bucket_path(bucket).map_err(|e| e.to_string())?;

        // List directory contents
        let list = self
            .fs
            .list_status(&bkt_path)
            .await
            .map_err(|e| e.to_string())?;

        let mut versions = Vec::new();

        // Process each file system entry
        for st in list {
            if st.is_dir {
                // Skip directories for object versions listing
                continue;
            }

            // Object key is just the file name
            let key = st.name.clone();

            // Apply prefix filtering if specified
            if let Some(prefix) = &opt.prefix {
                if !key.starts_with(prefix) {
                    continue;
                }
            }

            // Generate a version ID based on file metadata
            // Use mtime + size to create a reasonably unique version identifier
            let version_id = format!("{}-{}", st.mtime, st.len);

            // Convert file status to ObjectVersion
            let version = ObjectVersion {
                key: key.clone(),
                version_id,
                is_latest: true, // Always latest since we don't support true versioning
                last_modified: crate::utils::s3_utils::format_s3_timestamp(st.mtime)
                    .unwrap_or_else(|| {
                        chrono::Utc::now()
                            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                            .to_string()
                    }),
                etag: crate::utils::s3_utils::generate_etag(&st),
                size: st.len as u64,
                storage_class: Some(crate::utils::s3_utils::map_storage_class(
                    &st.storage_policy.storage_type,
                )),
                owner: Some(crate::utils::s3_utils::create_owner_info(&st)),
            };

            versions.push(version);
        }

        // Apply max_keys limit if specified
        if let Some(max_keys) = opt.max_keys {
            if max_keys > 0 && versions.len() > max_keys as usize {
                versions.truncate(max_keys as usize);
            }
        }

        // Create the response structure
        let result = ListObjectVersionsResult {
            xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
            name: bucket.to_string(),
            prefix: opt.prefix.clone(),
            key_marker: opt.key_marker.clone(),
            version_id_marker: opt.version_id_marker.clone(),
            next_key_marker: None, // TODO: Implement pagination if needed
            next_version_id_marker: None,
            max_keys: opt.max_keys.map(|k| k as u32),
            is_truncated: false, // TODO: Set to true if results are truncated
            versions,
            delete_markers: Vec::new(), // No delete markers since no versioning
        };

        tracing::info!(
            "ListObjectVersions completed for bucket: {}, found {} versions",
            bucket,
            result.versions.len()
        );

        Ok(result)
    }
}
