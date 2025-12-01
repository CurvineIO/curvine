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
use crate::s3::error_code::Error;
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
use std::fs;
use tracing;
use uuid;

#[derive(Clone)]
pub struct S3Handlers {
    pub fs: UnifiedFileSystem,
    pub region: String,
    pub put_temp_dir: String,
    pub rt: std::sync::Arc<AsyncRuntime>,
    pub get_chunk_size_bytes: usize,
}

impl S3Handlers {
    pub fn new(
        fs: UnifiedFileSystem,
        region: String,
        put_temp_dir: String,
        rt: std::sync::Arc<AsyncRuntime>,
        get_chunk_size_mb: f32,
    ) -> Self {
        let get_chunk_size_bytes = (get_chunk_size_mb * 1024.0 * 1024.0) as usize;

        let put_temp_path = std::path::Path::new(&put_temp_dir);
        if !put_temp_path.exists() {
            tracing::debug!("Creating put_temp_dir: {}", put_temp_dir);
            if let Err(e) = fs::create_dir_all(put_temp_path) {
                tracing::error!("Failed to create put_temp_dir {}: {}", put_temp_dir, e);
            }
        }

        tracing::debug!(
            "Creating new S3Handlers with region: {}, put_temp_dir: {}, GET optimizations: chunk_size={}MB",
            region, put_temp_dir, get_chunk_size_mb
        );

        Self {
            fs,
            region,
            put_temp_dir,
            rt,
            get_chunk_size_bytes: get_chunk_size_bytes.clamp(512 * 1024, 4 * 1024 * 1024),
        }
    }

    #[inline]
    pub async fn handle_list_buckets(
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

    #[inline]
    pub async fn handle_get_bucket_location(
        &self,
        _loc: Option<&str>,
    ) -> Result<Option<&'static str>, ()> {
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

    pub fn cv_object_path(&self, bucket: &str, key: &str) -> FsResult<Path> {
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

impl HeadHandler for S3Handlers {
    fn lookup(
        &self,
        bucket: &str,
        object: &str,
    ) -> impl std::future::Future<Output = Result<Option<HeadObjectResult>, Error>> + Send {
        let bucket = bucket.to_string();
        let object = object.to_string();
        let this = self.clone();

        async move {
            tracing::info!("HEAD request for s3://{}/{}", bucket, object);

            let path = match this.cv_object_path(&bucket, &object) {
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

            let fs = this.fs.clone();
            let object_name = object.clone();

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
}

impl crate::s3::s3_api::GetObjectHandler for S3Handlers {
    fn handle<'a>(
        &'a self,
        bucket: &str,
        object: &str,
        opt: crate::s3::s3_api::GetObjectOption,
        out: &'a mut (dyn crate::utils::io::PollWrite + Unpin + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        let fs = self.fs.clone();
        let path = self.cv_object_path(bucket, object);
        let bucket = bucket.to_string();
        let object = object.to_string();
        let chunk_size = self.get_chunk_size_bytes;

        Box::pin(async move {
            if let Some(start) = opt.range_start {
                if let Some(end) = opt.range_end {
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

            let (seek_pos, bytes_to_read) = if let Some(range_end) = opt.range_end {
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
                    let start = opt.range_start.unwrap_or(0);
                    let bytes = range_end - start + 1;
                    tracing::debug!(
                        "Normal range request: seeking to {} for {} bytes",
                        start,
                        bytes
                    );
                    (Some(start), Some(bytes))
                }
            } else if let Some(start) = opt.range_start {
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

            let target_read = bytes_to_read.unwrap_or(reader.remaining().max(0) as u64);
            log::debug!("GetObject: will read {target_read} bytes directly");

            let chunk_size = if target_read <= 64 * 1024 {
                std::cmp::min(chunk_size, target_read as usize).max(4 * 1024)
            } else if target_read <= 1024 * 1024 {
                std::cmp::min(chunk_size, 256 * 1024)
            } else {
                chunk_size
            };

            let mut total_read = 0u64;
            let mut remaining_to_read = target_read;

            while remaining_to_read > 0 {
                let read_size = std::cmp::min(chunk_size, remaining_to_read as usize);
                let mut buffer = vec![0u8; read_size];

                let bytes_read = reader
                    .read_full(&mut buffer[..read_size])
                    .await
                    .map_err(|e| e.to_string())?;

                if bytes_read == 0 {
                    break;
                }

                buffer.truncate(bytes_read);

                out.poll_write_vec(buffer).await.map_err(|e| {
                    tracing::error!("Failed to write chunk to output: {}", e);
                    e.to_string()
                })?;

                total_read += bytes_read as u64;
                remaining_to_read -= bytes_read as u64;
            }

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

impl PutObjectHandler for S3Handlers {
    fn handle(
        &self,
        _opt: PutObjectOption,
        bucket: String,
        object: String,
        mut body: crate::utils::io::PollReaderEnum,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send {
        let context = PutContext::new(
            self.fs.clone(),
            self.rt.clone(),
            bucket.clone(),
            object.clone(),
            self.cv_object_path(&bucket, &object),
        );

        async move { PutOperation::execute(context, &mut body).await }
    }
}

impl crate::s3::s3_api::DeleteObjectHandler for S3Handlers {
    fn handle(
        &self,
        _opt: &crate::s3::s3_api::DeleteObjectOption,
        object: &str,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send {
        let fs = self.fs.clone();
        let object = object.to_string();

        async move {
            let path = Path::from_str(format!("/{object}")).map_err(|e| e.to_string())?;
            match fs.delete(&path, false).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    let msg = e.to_string();
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
}

impl crate::s3::s3_api::CreateBucketHandler for S3Handlers {
    fn handle(
        &self,
        _opt: &crate::s3::s3_api::CreateBucketOption,
        bucket: &str,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send {
        let this = self.clone();
        let bucket = bucket.to_string();

        async move {
            let fs = this.fs.clone();
            let path = this.cv_bucket_path(&bucket).map_err(|e| e.to_string())?;

            if fs.get_status(&path).await.is_ok() {
                return Err("BucketAlreadyExists".to_string());
            }

            fs.mkdir(&path, true).await.map_err(|e| e.to_string())?;
            Ok(())
        }
    }
}

impl crate::s3::s3_api::DeleteBucketHandler for S3Handlers {
    fn handle(
        &self,
        _opt: &crate::s3::s3_api::DeleteBucketOption,
        bucket: &str,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send {
        let fs = self.fs.clone();
        let bucket = bucket.to_string();
        let path = self.cv_bucket_path(&bucket);

        async move {
            let path = path.map_err(|e| e.to_string())?;
            fs.delete(&path, false).await.map_err(|e| e.to_string())
        }
    }
}

impl crate::s3::s3_api::ListBucketHandler for S3Handlers {
    fn handle(
        &self,
        opt: &crate::s3::s3_api::ListBucketsOption,
    ) -> impl std::future::Future<Output = Result<Vec<crate::s3::s3_api::Bucket>, String>> + Send
    {
        self.handle_list_buckets(opt)
    }
}

impl crate::s3::s3_api::GetBucketLocationHandler for S3Handlers {
    fn handle(
        &self,
        loc: Option<&str>,
    ) -> impl std::future::Future<Output = Result<Option<&'static str>, ()>> + Send {
        self.handle_get_bucket_location(loc)
    }
}

#[async_trait::async_trait]
impl crate::s3::s3_api::MultiUploadObjectHandler for S3Handlers {
    async fn handle_create_session(&self, _bucket: String, _key: String) -> Result<String, ()> {
        let upload_id = uuid::Uuid::new_v4().to_string();
        Ok(upload_id)
    }

    async fn handle_upload_part(
        &self,
        _bucket: String,
        _key: String,
        upload_id: String,
        part_number: u32,
        mut body: crate::utils::io::AsyncReadEnum,
    ) -> Result<String, ()> {
        use bytes::BytesMut;
        use tokio::io::AsyncWriteExt;

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
    }

    async fn handle_complete(
        &self,
        bucket: String,
        key: String,
        upload_id: String,
        data: Vec<(String, u32)>,
        _opts: crate::s3::s3_api::MultiUploadObjectCompleteOption,
    ) -> Result<String, ()> {
        use tokio::io::AsyncReadExt;

        let final_path = match self.cv_object_path(&bucket, &key) {
            Ok(p) => p,
            Err(_) => return Err(()),
        };

        let mut writer = match self.fs.create(&final_path, true).await {
            Ok(w) => w,
            Err(_) => return Err(()),
        };

        let dir = format!("{}/{}", self.put_temp_dir, upload_id);

        let mut part_list = data;
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
    }

    async fn handle_abort(
        &self,
        _bucket: String,
        _key: String,
        upload_id: String,
    ) -> Result<(), ()> {
        let dir = format!("{}/{}", self.put_temp_dir, upload_id);
        let _ = tokio::fs::remove_dir_all(&dir).await;
        Ok(())
    }
}

impl ListObjectHandler for S3Handlers {
    fn handle(
        &self,
        opt: &ListObjectOption,
        bucket: &str,
    ) -> impl std::future::Future<Output = Result<Vec<ListObjectContent>, String>> + Send {
        let this = self.clone();
        let opt = opt.clone();
        let bucket = bucket.to_string();

        async move {
            let bkt_path = this.cv_bucket_path(&bucket).map_err(|e| e.to_string())?;

            // S3 prefix semantics: prefix is for filtering, not path construction
            // Only treat prefix as directory path if it ends with '/'
            let (list_path, prefix_dir) = if let Some(prefix) = &opt.prefix {
                if prefix.ends_with('/') {
                    // Prefix is a directory path (e.g., "folder/")
                    let dir_path = prefix.trim_end_matches('/');
                    if dir_path.is_empty() {
                        (bkt_path.clone(), String::new())
                    } else {
                        let full_path =
                            Path::from_str(format!("{}/{}", bkt_path, dir_path).as_str())
                                .map_err(|e| e.to_string())?;
                        (full_path, dir_path.to_string())
                    }
                } else if prefix.contains('/') {
                    // Prefix contains directory component (e.g., "folder/file")
                    // List the parent directory and filter by full prefix
                    let last_slash = prefix.rfind('/').unwrap();
                    let dir_part = &prefix[..last_slash];
                    if dir_part.is_empty() {
                        (bkt_path.clone(), String::new())
                    } else {
                        let full_path =
                            Path::from_str(format!("{}/{}", bkt_path, dir_part).as_str())
                                .map_err(|e| e.to_string())?;
                        (full_path, dir_part.to_string())
                    }
                } else {
                    // Prefix is just a filename prefix (e.g., "small")
                    // List bucket root and filter by prefix
                    (bkt_path.clone(), String::new())
                }
            } else {
                (bkt_path.clone(), String::new())
            };

            let list = this
                .fs
                .list_status(&list_path)
                .await
                .map_err(|e| e.to_string())?;

            let mut contents = Vec::new();

            for st in list {
                // Build the S3 key from file status
                let key = if prefix_dir.is_empty() {
                    st.name.clone()
                } else {
                    format!("{}/{}", prefix_dir, st.name)
                };

                // Apply prefix filter
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
}

impl ListObjectVersionsHandler for S3Handlers {
    fn handle(
        &self,
        opt: &ListObjectVersionsOption,
        bucket: &str,
    ) -> impl std::future::Future<Output = Result<ListObjectVersionsResult, String>> + Send {
        let this = self.clone();
        let opt = opt.clone();
        let bucket = bucket.to_string();

        async move {
            tracing::info!("ListObjectVersions request for bucket: {}", bucket);

            let bkt_path = this.cv_bucket_path(&bucket).map_err(|e| e.to_string())?;

            let list = this
                .fs
                .list_status(&bkt_path)
                .await
                .map_err(|e| e.to_string())?;

            let mut versions = Vec::new();

            for st in list {
                if st.is_dir {
                    continue;
                }

                let key = st.name.clone();

                if let Some(prefix) = &opt.prefix {
                    if !key.starts_with(prefix) {
                        continue;
                    }
                }

                let version_id = format!("{}-{}", st.mtime, st.len);

                let version = ObjectVersion {
                    key: key.clone(),
                    version_id,
                    is_latest: true,
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

            if let Some(max_keys) = opt.max_keys {
                if max_keys > 0 && versions.len() > max_keys as usize {
                    versions.truncate(max_keys as usize);
                }
            }

            let result = ListObjectVersionsResult {
                xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
                name: bucket.to_string(),
                prefix: opt.prefix.clone(),
                key_marker: opt.key_marker.clone(),
                version_id_marker: opt.version_id_marker.clone(),
                next_key_marker: None,
                next_version_id_marker: None,
                max_keys: opt.max_keys.map(|k| k as u32),
                is_truncated: false,
                versions,
                delete_markers: Vec::new(),
            };

            tracing::info!(
                "ListObjectVersions completed for bucket: {}, found {} versions",
                bucket,
                result.versions.len()
            );

            Ok(result)
        }
    }
}
