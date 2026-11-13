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
use crate::utils::s3_utils::file_status_to_head_object_result;
use crate::utils::temp_storage::{PartMeta, TempStorageEnum};
use chrono;
use curvine_client::unified::UnifiedFileSystem;
use curvine_error::FsResult;
use curvine_fs_api::{FileSystem, Path, Reader, Writer};
use curvine_model::FileType;
use curvine_runtime::runtime::AsyncRuntime;
use std::sync::Arc;
use tracing;
use uuid;

/// Whether an `ETag` header value is strong (`"x"`) rather than weak (`W/"x"`).
///
/// RFC 9110 §8.8.3: only a strong validator may satisfy `If-Match`.
fn is_strong(tag: &str) -> bool {
    // The weakness prefix is case-insensitive per RFC 9110 §8.8.3.
    !tag.trim_start()
        .get(..2)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("W/"))
}

/// The opaque value of an `ETag`, without the weakness prefix and quotes.
///
/// S3 also accepts an unquoted value, and comparison is case-insensitive
/// because digests are compared as text.
fn etag_value(tag: &str) -> String {
    tag.trim()
        .trim_start_matches("W/")
        .trim_matches('"')
        .to_ascii_lowercase()
}

/// Conditional request headers, as received.
#[derive(Debug, Default, Clone)]
pub struct Preconditions {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<String>,
    pub if_unmodified_since: Option<String>,
    /// `true` when evaluating `x-amz-copy-source-if-*` against the source.
    pub is_copy_source: bool,
}

impl Preconditions {
    pub fn is_empty(&self) -> bool {
        self.if_match.is_none()
            && self.if_none_match.is_none()
            && self.if_modified_since.is_none()
            && self.if_unmodified_since.is_none()
    }

    /// Read the conditional headers from a request.
    ///
    /// `copy_source` selects the `x-amz-copy-source-if-*` family, which
    /// `CopyObject` applies to the source object rather than the destination.
    pub fn from_headers<H: crate::auth::sig_v4::VHeader>(header: &H, copy_source: bool) -> Self {
        let prefix = if copy_source {
            "x-amz-copy-source-"
        } else {
            ""
        };
        let get = |name: &str| header.get_header(&format!("{prefix}{name}"));
        Self {
            if_match: get("if-match"),
            if_none_match: get("if-none-match"),
            if_modified_since: get("if-modified-since"),
            if_unmodified_since: get("if-unmodified-since"),
            is_copy_source: copy_source,
        }
    }
}

/// Truncate a millisecond timestamp to whole seconds.
///
/// An HTTP-date carries no sub-second precision, so comparing a millisecond
/// mtime against one must compare at the same granularity. Without this a
/// client that echoes the exact `Last-Modified` it was given fails its own
/// `If-Unmodified-Since`.
fn to_whole_seconds(timestamp_ms: i64) -> i64 {
    timestamp_ms.div_euclid(1000) * 1000
}

/// Parse an HTTP-date, accepting the three forms RFC 9110 permits.
fn parse_http_date(value: &str) -> Option<i64> {
    for format in [
        "%a, %d %b %Y %H:%M:%S GMT",
        "%A, %d-%b-%y %H:%M:%S GMT",
        "%a %b %e %H:%M:%S %Y",
    ] {
        if let Ok(parsed) = chrono::NaiveDateTime::parse_from_str(value.trim(), format) {
            return Some(to_whole_seconds(parsed.and_utc().timestamp_millis()));
        }
    }
    chrono::DateTime::parse_from_rfc2822(value.trim())
        .ok()
        .map(|dt| to_whole_seconds(dt.timestamp_millis()))
}

/// Evaluate conditional headers per RFC 9110 §13.2.2.
///
/// Order is fixed by the RFC:
/// 1. `If-Match` — strong comparison; failure is 412.
/// 2. `If-Unmodified-Since` — ignored when `If-Match` is present.
/// 3. `If-None-Match` — weak comparison; a match is 304.
/// 4. `If-Modified-Since` — ignored when `If-None-Match` is present.
pub fn evaluate_precondition_headers(
    headers: &Preconditions,
    etag: Option<&str>,
    last_modified_ms: Option<i64>,
) -> Option<u16> {
    let has_if_match = headers.if_match.is_some();
    let has_if_none_match = headers.if_none_match.is_some();

    if let Some(header) = headers.if_match.as_deref() {
        let satisfied = match etag {
            None => false,
            Some(current) => header.split(',').any(|tag| {
                let tag = tag.trim();
                tag == "*" || (is_strong(tag) && etag_value(tag) == etag_value(current))
            }),
        };
        if !satisfied {
            return Some(412);
        }
    }

    if !has_if_match {
        if let Some(raw) = headers.if_unmodified_since.as_deref() {
            if let Some(limit) = parse_http_date(raw) {
                match last_modified_ms {
                    Some(mtime) if to_whole_seconds(mtime) > limit => return Some(412),
                    None => return Some(412),
                    _ => {}
                }
            }
        }
    }

    if let Some(header) = headers.if_none_match.as_deref() {
        let matched = match etag {
            None => false,
            Some(current) => header.split(',').any(|tag| {
                let tag = tag.trim();
                tag == "*" || etag_value(tag) == etag_value(current)
            }),
        };
        if matched {
            return Some(304);
        }
    }

    if !has_if_none_match {
        if let Some(raw) = headers.if_modified_since.as_deref() {
            if let Some(limit) = parse_http_date(raw) {
                if let Some(mtime) = last_modified_ms {
                    if to_whole_seconds(mtime) <= limit {
                        return Some(304);
                    }
                }
            }
        }
    }

    None
}

/// Error text the S3 API layer maps to `NoSuchBucket` (404).
pub const NO_SUCH_BUCKET: &str = "NoSuchBucket: the specified bucket does not exist";
/// Error text the S3 API layer maps to `BucketNotEmpty` (409).
pub const BUCKET_NOT_EMPTY: &str = "BucketNotEmpty: the bucket you tried to delete is not empty";

#[derive(Clone)]
pub struct S3Handlers {
    pub fs: UnifiedFileSystem,
    pub region: String,
    pub temp_storage: Arc<TempStorageEnum>,
    pub rt: std::sync::Arc<AsyncRuntime>,
    pub get_chunk_size_bytes: usize,
}

impl S3Handlers {
    pub fn new(
        fs: UnifiedFileSystem,
        region: String,
        temp_storage: Arc<TempStorageEnum>,
        rt: std::sync::Arc<AsyncRuntime>,
        get_chunk_size_mb: f32,
    ) -> Self {
        let get_chunk_size_bytes = (get_chunk_size_mb * 1024.0 * 1024.0) as usize;

        tracing::debug!(
            "Creating new S3Handlers with region: {}, temp_storage: {} ({}), GET optimizations: chunk_size={}MB",
            region,
            temp_storage.config().temp_dir,
            temp_storage.storage_type(),
            get_chunk_size_mb
        );

        Self {
            fs,
            region,
            temp_storage,
            rt,
            get_chunk_size_bytes: get_chunk_size_bytes.clamp(512 * 1024, 4 * 1024 * 1024),
        }
    }

    /// Evaluate the conditional request headers for one object.
    ///
    /// Implements RFC 9110 §13.2.2 evaluation order and the S3 extensions:
    /// `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`.
    /// Returns the status a conditional request must answer with (412 or 304),
    /// or `None` when it should be served normally.
    pub async fn evaluate_preconditions(
        &self,
        path: &Path,
        headers: &Preconditions,
    ) -> Option<u16> {
        if headers.is_empty() {
            return None;
        }
        let status = self.fs.get_status(path).await.ok()?;
        let current_etag = crate::utils::s3_utils::generate_etag(&status);

        evaluate_precondition_headers(headers, Some(&current_etag), Some(status.mtime))
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
    ) -> Result<Option<&'static str>, String> {
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
            return Err(curvine_error::FsError::invalid_path(
                "",
                "bucket or key is empty",
            ));
        }

        if bucket.contains('/') {
            tracing::warn!(
                "Invalid bucket name '{}': contains invalid characters",
                bucket
            );
            return Err(curvine_error::FsError::invalid_path(
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
            return Err(curvine_error::FsError::invalid_path(
                "",
                "bucket name is empty",
            ));
        }

        if bucket.contains('/') {
            tracing::warn!(
                "Invalid bucket name '{}': contains invalid characters",
                bucket
            );
            return Err(curvine_error::FsError::invalid_path(
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
        opt: PutObjectOption,
        bucket: String,
        object: String,
        mut body: crate::utils::io::PollReaderEnum,
    ) -> impl std::future::Future<Output = Result<String, String>> + Send {
        let fs = self.fs.clone();
        let path = self.cv_object_path(&bucket, &object);
        let context = PutContext::new(
            self.fs.clone(),
            self.rt.clone(),
            bucket.clone(),
            object.clone(),
            self.cv_object_path(&bucket, &object),
        );

        async move {
            // The staging prefix is reserved; a PUT cannot create an object
            // that listings would have to hide.
            reject_reserved_key(&object)?;

            // S3 applies `If-Match` / `If-None-Match` to the DESTINATION on
            // PutObject. Without this an `If-Match: <old>` overwrite silently
            // clobbered a concurrently-replaced object, which is the lost
            // update the GET path already guards against. `If-None-Match: *`
            // is the "create only if absent" form.
            let dest_conditions = crate::s3::handlers::Preconditions {
                if_match: opt.if_match.clone(),
                if_none_match: opt.if_none_match.clone(),
                ..Default::default()
            };
            if !dest_conditions.is_empty() {
                let dest = path.as_ref().map_err(|e| e.to_string())?.clone();
                let existing = fs.get_status(&dest).await.ok();
                let etag = existing.as_ref().map(crate::utils::s3_utils::generate_etag);
                if crate::s3::handlers::evaluate_precondition_headers(
                    &dest_conditions,
                    etag.as_deref(),
                    existing.as_ref().map(|st| st.mtime),
                )
                .is_some()
                {
                    return Err("PreconditionFailed: destination condition not met".to_string());
                }
            }

            // Metadata is written to the staging inode, so the rename publishes
            // body and metadata as one step. Writing it afterwards left a
            // window where the object answered HEAD with the fallback ETag, and
            // a failed `set_attr` reported 500 for an object already visible.
            let metadata = crate::utils::s3_utils::S3Metadata::from_put_option(&opt);
            let expect_length = opt
                .content_length
                .and_then(|v| usize::try_from(v).ok())
                .filter(|v| *v > 0);
            let md5 = PutOperation::execute(
                context,
                &mut body,
                metadata,
                crate::s3::types::BodyExpect {
                    length: expect_length,
                    content_md5: opt.content_md5.as_deref(),
                },
            )
            .await?;
            let _ = path;

            // Report what HEAD/GET will report for the same object.
            Ok(format!("\"{md5}\""))
        }
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
                    let _ = &e;
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

            // S3 requires DeleteBucket to fail on a missing bucket (404) and on
            // a non-empty one (409). Passing straight to `delete` answered 500
            // for a non-empty bucket and left the client guessing.
            let status = fs.get_status(&path).await.map_err(|e| e.to_string())?;
            if !status.is_dir {
                return Err(NO_SUCH_BUCKET.to_string());
            }
            if status.children_num > 0 {
                return Err(BUCKET_NOT_EMPTY.to_string());
            }

            fs.delete(&path, false)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
    }
}

impl S3Handlers {
    /// Server-side copy, the operation behind `x-amz-copy-source`.
    ///
    /// Without this branch a copy request fell through to the normal PUT path,
    /// which has no body: it created a zero-byte object under the destination
    /// key and answered success, silently destroying whatever was there.
    pub async fn copy_object(
        &self,
        source_bucket: &str,
        source_key: &str,
        dest_bucket: &str,
        dest_key: &str,
        source_conditions: &Preconditions,
    ) -> Result<String, String> {
        use curvine_fs_api::Reader;

        let src = self
            .cv_object_path(source_bucket, source_key)
            .map_err(|e| format!("Invalid copy source: {e}"))?;
        let dst = self
            .cv_object_path(dest_bucket, dest_key)
            .map_err(|e| format!("Invalid copy destination: {e}"))?;
        // The staging prefix is reserved; a copy cannot create an object that
        // listings would have to hide.
        reject_reserved_key(dest_key)?;

        let status = self.fs.get_status(&src).await.map_err(|_| {
            format!("NoSuchKey: copy source {source_bucket}/{source_key} does not exist")
        })?;

        // `x-amz-copy-source-if-*` constrains the SOURCE object, not the
        // destination, so the conditions are evaluated against `src`.
        if let Some(code) = self.evaluate_preconditions(&src, source_conditions).await {
            return Err(format!(
                "PreconditionFailed: copy source condition returned {code}"
            ));
        }

        // Copy through the same staging-then-rename path a PUT uses, so a
        // failed copy cannot truncate an existing destination object.
        let staging_id = uuid::Uuid::new_v4().to_string();
        let staging = {
            let parent = dst
                .parent()
                .map_err(|e| format!("Invalid destination parent: {e}"))?
                .ok_or_else(|| format!("Destination {dst} has no parent"))?;
            Path::from_str(format!("{parent}/.curvine-s3-copy-{staging_id}"))
                .map_err(|e| format!("Invalid staging path: {e}"))?
        };

        let mut reader = self.fs.open(&src).await.map_err(|e| e.to_string())?;
        let mut writer = self
            .fs
            .create(&staging, true)
            .await
            .map_err(|e| format!("Failed to create copy staging object: {e}"))?;

        let mut md5 = md5::Context::new();
        loop {
            let mut buf = vec![0u8; self.get_chunk_size_bytes];
            let n = match reader.read(&mut buf).await {
                Ok(n) => n,
                Err(e) => {
                    let _ = writer.cancel().await;
                    let _ = self.fs.delete(&staging, false).await;
                    return Err(format!("Failed to read copy source: {e}"));
                }
            };
            if n == 0 {
                break;
            }
            buf.truncate(n);
            md5.consume(&buf);
            if let Err(e) = writer.write(&buf).await {
                let _ = writer.cancel().await;
                let _ = self.fs.delete(&staging, false).await;
                return Err(format!("Failed to write copy destination: {e}"));
            }
        }

        if let Err(e) = writer.complete().await {
            let _ = self.fs.delete(&staging, false).await;
            return Err(format!("Failed to complete copy destination: {e}"));
        }
        if let Err(e) = self.fs.rename(&staging, &dst).await {
            let _ = self.fs.delete(&staging, false).await;
            return Err(format!("Failed to publish copy destination: {e}"));
        }

        let etag = format!("{:x}", md5.compute());
        // Copy inherits the source's Content-Type and user metadata; the
        // upload path records them the same way.
        let mut opts = curvine_model::SetAttrOptsBuilder::new().add_x_attr(
            crate::utils::s3_utils::XATTR_S3_ETAG,
            etag.as_bytes().to_vec(),
        );
        if let Some(ct) = status
            .x_attr
            .get(crate::utils::s3_utils::XATTR_S3_CONTENT_TYPE)
        {
            opts = opts.add_x_attr(crate::utils::s3_utils::XATTR_S3_CONTENT_TYPE, ct.clone());
        }
        for (k, v) in &status.x_attr {
            if k.starts_with(crate::utils::s3_utils::XATTR_S3_META_PREFIX) {
                opts = opts.add_x_attr(k.clone(), v.clone());
            }
        }
        if let Err(e) = self.fs.set_attr(&dst, opts.build()).await {
            tracing::warn!("Failed to record copy metadata for {dst}: {e}");
        }

        Ok(etag)
    }
}

/// Whether two ETag strings denote the same strong validator.
///
/// Quoting and a weak prefix are presentation; the digest is what must match.
fn same_etag(left: &str, right: &str) -> bool {
    fn value(tag: &str) -> String {
        tag.trim()
            .trim_start_matches("W/")
            .trim_matches('"')
            .to_ascii_lowercase()
    }
    value(left) == value(right)
}

/// S3 minimum size for a part other than the last one.
const MIN_PART_BYTES: usize = 5 << 20;

/// Key segment reserved for gateway staging artefacts.
///
/// Every staging object (`PutOperation`, CopyObject, multipart assembly) is a
/// sibling named `.curvine-s3-*`, and listings filter that prefix out. Reserving
/// it on the write paths is what makes the filter sound: no user object can
/// carry the prefix, so hiding it can never hide user data.
const STAGING_KEY_PREFIX: &str = ".curvine-s3-";

/// Whether a listed entry is a gateway staging artefact rather than an object.
///
/// Sound because the namespace is reserved on every write path; ListObjects and
/// ListObjectVersions share this predicate so neither can expose staging.
pub(super) fn is_staging_name(name: &str) -> bool {
    name.starts_with(STAGING_KEY_PREFIX)
}

/// Reject any key with a segment inside the reserved staging namespace.
///
/// Every segment is checked, not just the last: creating `a/.curvine-s3-x/b`
/// also creates the `.curvine-s3-x` directory, and listings of `a` would then
/// hide that entry. Rejecting it is what keeps the listing filter sound.
fn reject_reserved_key(key: &str) -> Result<(), String> {
    if key
        .split('/')
        .any(|segment| segment.starts_with(STAGING_KEY_PREFIX))
    {
        return Err(format!(
            "InvalidArgument: object keys may not contain a segment starting with {STAGING_KEY_PREFIX}, which is reserved for gateway staging"
        ));
    }
    Ok(())
}

/// S3 requires a Complete request to list parts in strictly ascending
/// part-number order, without duplicates.
///
/// Sorting the list first used to silently accept both, so the request is
/// validated in the order it arrived.
fn validate_part_order(parts: &[(String, u32)]) -> Result<(), String> {
    for window in parts.windows(2) {
        if window[1].1 <= window[0].1 {
            return Err(format!(
                "InvalidPartOrder: part numbers must be strictly ascending (got {} then {})",
                window[0].1, window[1].1
            ));
        }
    }
    if parts.is_empty() {
        return Err("InvalidPart: no parts listed".to_string());
    }
    Ok(())
}

/// Check every listed part against the record staged with its bytes.
///
/// One record per part decides existence, ETag match and the non-final
/// minimum size, so all three read from the same source of truth: the record
/// published atomically with the part (`PartMeta`).
fn validate_staged_parts(
    parts: &[(String, u32)],
    staged: &[(u32, PartMeta)],
) -> Result<(), String> {
    let count = staged.len();
    for (index, ((etag, part_num), (_, recorded))) in parts.iter().zip(staged).enumerate() {
        if !same_etag(&recorded.etag, etag) {
            return Err(format!(
                "InvalidPart: part {part_num} etag does not match the staged part"
            ));
        }
        // S3 requires every part except the last to be at least 5 MiB; a
        // single-part upload is the last part and is exempt.
        if index + 1 < count && recorded.size < MIN_PART_BYTES {
            return Err(format!(
                "EntityTooSmall: part {part_num} is {} bytes, below the 5 MiB minimum for a non-final part",
                recorded.size
            ));
        }
    }
    Ok(())
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
    ) -> impl std::future::Future<Output = Result<Option<&'static str>, String>> + Send {
        self.handle_get_bucket_location(loc)
    }
}

#[async_trait::async_trait]
impl crate::s3::s3_api::MultiUploadObjectHandler for S3Handlers {
    async fn handle_create_session(&self, _bucket: String, _key: String) -> Result<String, ()> {
        let upload_id = uuid::Uuid::new_v4().to_string();

        // Pre-create the upload session directory
        if let Err(e) = self.temp_storage.create_dir(&upload_id).await {
            tracing::error!("Failed to create upload session directory: {}", e);
            return Err(());
        }

        tracing::debug!(
            "Created multipart upload session: {} (storage: {})",
            upload_id,
            self.temp_storage.storage_type()
        );
        Ok(upload_id)
    }

    async fn handle_upload_part<R: crate::utils::io::PollRead + Send>(
        &self,
        _bucket: String,
        _key: String,
        upload_id: String,
        part_number: u32,
        body: &mut R,
        expect: crate::utils::temp_storage::StageExpect<'_>,
    ) -> Result<String, ()> {
        // Streaming write: a part can be up to the S3 5 GiB limit, so it is
        // hashed and written chunk by chunk instead of buffered in memory.
        match self
            .temp_storage
            .write_part_stream(&upload_id, part_number, body, expect)
            .await
        {
            Ok(etag) => {
                tracing::debug!(
                    "Uploaded part {} for upload_id {} (storage: {})",
                    part_number,
                    upload_id,
                    self.temp_storage.storage_type()
                );
                Ok(etag)
            }
            Err(e) => {
                tracing::error!(
                    "Failed to write part {} for upload_id {}: {}",
                    part_number,
                    upload_id,
                    e
                );
                Err(())
            }
        }
    }

    async fn handle_complete(
        &self,
        bucket: String,
        key: String,
        upload_id: String,
        data: Vec<(String, u32)>,
        _opts: crate::s3::s3_api::MultiUploadObjectCompleteOption,
    ) -> Result<String, String> {
        let final_path = match self.cv_object_path(&bucket, &key) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("Invalid object path: {}", e);
                return Err(format!("Invalid object path: {e}"));
            }
        };

        validate_part_order(&data)?;
        let part_list = data;

        // The staging prefix is reserved, so Complete cannot publish there.
        reject_reserved_key(&key)?;

        // An unknown, aborted or already-completed upload id is a 404, not a
        // part error: without this check a missing part record inside a dead
        // session would be misreported as InvalidPart (400).
        if !self.temp_storage.upload_exists(&upload_id).await {
            return Err(format!("NoSuchUpload: upload {upload_id} does not exist"));
        }

        // Validate the upload before touching the destination. Creating the
        // final object first meant a bad upload id, or a part that is not
        // staged, still left a zero-byte object under the destination key —
        // destroying any previous object and reporting failure afterwards.
        //
        // The part ETAG is compared with the record the upload left next to the
        // part, so a request naming a different digest than the one staged is
        // rejected here, not discovered while streaming.
        let mut staged_parts = Vec::with_capacity(part_list.len());
        for (_, part_num) in part_list.iter() {
            let recorded = self
                .temp_storage
                .part_meta(&upload_id, *part_num)
                .await
                .ok_or_else(|| {
                    // No record means the part was never uploaded under this
                    // upload id, or was uploaded by an older build. This also
                    // covers a part whose bytes were lost: the record is
                    // published with them.
                    format!("InvalidPart: part {part_num} of upload {upload_id} is not staged")
                })?;
            staged_parts.push((*part_num, recorded));
        }
        validate_staged_parts(&part_list, &staged_parts)?;

        // Stage the assembled object beside the destination and publish it with
        // a rename, so the destination is only ever the previous object or the
        // complete new one.
        let staging_id = uuid::Uuid::new_v4().to_string();
        let staging = match final_path.parent() {
            Ok(Some(parent)) => Path::from_str(format!("{parent}/.curvine-s3-mp-{staging_id}"))
                .map_err(|e| format!("Invalid staging path: {e}"))?,
            _ => return Err("Destination has no parent".to_string()),
        };

        let mut writer = match self.fs.create(&staging, true).await {
            Ok(w) => w,
            Err(e) => {
                tracing::error!("Failed to create final file: {}", e);
                return Err(format!("Failed to create final file: {e}"));
            }
        };

        // Stream each part directly to final file (memory-efficient)
        // This avoids loading entire parts into memory
        let mut part_etags: Vec<String> = Vec::with_capacity(part_list.len());
        for (etag, part_num) in part_list.iter() {
            if let Err(e) = self
                .temp_storage
                .read_part_stream(&upload_id, *part_num, etag, &mut writer)
                .await
            {
                tracing::error!(
                    "Failed to stream part {} for upload_id {}: {}",
                    part_num,
                    upload_id,
                    e
                );
                // Cleanup on failure. The staged parts are deliberately kept:
                // a rejected Complete must be retryable with a corrected list,
                // and deleting them would force the client to re-upload.
                let _ = writer.cancel().await;
                let _ = self.fs.delete(&staging, false).await;
                // Pass the staging error through unchanged: an ETag mismatch
                // surfaces here when a part changed between preflight and
                // assembly and its `InvalidPart:` prefix must reach the mapping
                // as a 400, while a genuine I/O failure stays a 500.
                return Err(e);
            }
            part_etags.push(etag.trim_matches('"').to_ascii_lowercase());
        }

        if let Err(e) = writer.complete().await {
            tracing::error!("Failed to complete final file write: {}", e);
            // Cleanup on failure. Parts stay staged so the client can retry.
            let _ = self.fs.delete(&staging, false).await;
            return Err(format!("Failed to complete final file write: {e}"));
        }

        // S3 multipart ETag: md5 of the concatenated part MD5s, plus `-N`.
        // The response used to be the literal "etag-not-computed", so no client
        // could validate a multipart upload.
        let multipart_etag = multipart_etag(&part_etags);

        // Record the ETag on the staging inode, so the rename below publishes
        // body and ETag together. Writing it afterwards left the object
        // answering HEAD with the fallback ETag if `set_attr` failed, and the
        // response ETag could differ from HEAD permanently.
        if let Err(e) = self
            .fs
            .set_attr(
                &staging,
                curvine_model::SetAttrOptsBuilder::new()
                    .add_x_attr(
                        crate::utils::s3_utils::XATTR_S3_ETAG,
                        multipart_etag.trim_matches('"').as_bytes().to_vec(),
                    )
                    .build(),
            )
            .await
        {
            let _ = self.fs.delete(&staging, false).await;
            return Err(format!("Failed to record multipart etag: {e}"));
        }

        // Publish: the destination is only ever replaced by a complete object
        // that already carries its metadata.
        if let Err(e) = self.fs.rename(&staging, &final_path).await {
            let _ = self.fs.delete(&staging, false).await;
            return Err(format!("Failed to publish assembled object: {e}"));
        }

        // Cleanup temp files after successful completion
        if let Err(e) = self.temp_storage.cleanup(&upload_id).await {
            tracing::warn!(
                "Failed to cleanup temp files for upload_id {}: {}",
                upload_id,
                e
            );
        }

        tracing::info!(
            "Completed multipart upload for s3://{}/{} (upload_id: {}, parts: {})",
            bucket,
            key,
            upload_id,
            part_etags.len()
        );

        Ok(multipart_etag)
    }

    async fn handle_abort(
        &self,
        _bucket: String,
        _key: String,
        upload_id: String,
    ) -> Result<(), ()> {
        // Cleanup temp files on abort
        if let Err(e) = self.temp_storage.cleanup(&upload_id).await {
            tracing::warn!(
                "Failed to cleanup temp files on abort for upload_id {}: {}",
                upload_id,
                e
            );
        }

        tracing::debug!("Aborted multipart upload: {}", upload_id);
        Ok(())
    }
}

impl ListObjectHandler for S3Handlers {
    fn handle(
        &self,
        opt: &ListObjectOption,
        bucket: &str,
    ) -> impl std::future::Future<Output = Result<super::listing::ListObjectPage, String>> + Send
    {
        let this = self.clone();
        let opt = opt.clone();
        let bucket = bucket.to_string();

        async move {
            let bkt_path = this.cv_bucket_path(&bucket).map_err(|e| e.to_string())?;

            // A missing bucket must be an error, not an empty listing: S3
            // answers 404, and clients rely on it to detect a wrong bucket
            // instead of concluding the bucket is empty.
            let bucket_status = this.fs.get_status(&bkt_path).await.map_err(|e| match e {
                curvine_error::FsError::FileNotFound(_) => NO_SUCH_BUCKET.to_string(),
                other => other.to_string(),
            })?;
            if !bucket_status.is_dir {
                return Err(NO_SUCH_BUCKET.to_string());
            }

            let dirs = BucketDirs {
                fs: &this.fs,
                bucket: &bkt_path,
            };
            super::listing::list_objects(&opt, &dirs).await
        }
    }
}

/// Bucket-relative directory reads for ListObjects. Directory names come from
/// filesystem statuses, never from the client's prefix or continuation token.
struct BucketDirs<'a> {
    fs: &'a UnifiedFileSystem,
    bucket: &'a Path,
}

impl BucketDirs<'_> {
    fn path(&self, dir: &str) -> FsResult<Path> {
        if dir.is_empty() {
            return Ok(self.bucket.clone());
        }
        Ok(Path::from_str(format!("{}/{}", self.bucket, dir))?)
    }
}

impl super::listing::DirSource for BucketDirs<'_> {
    async fn list(&self, dir: &str) -> FsResult<Vec<curvine_model::FileStatus>> {
        self.fs.list_status(&self.path(dir)?).await
    }

    async fn stream(&self, dir: &str) -> FsResult<curvine_fs_api::ListStream> {
        let path = self.path(dir)?;
        let page = curvine_model::ListOptions::with_limit(32);
        match self.fs.list_stream(&path, page).await {
            // Backends without paged listing report it as a plain error.
            Err(curvine_error::FsError::Common(_)) => Ok(self.fs.list_status(&path).await?.into()),
            other => other,
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
                // Same rule as ListObjects: staging objects are internal state.
                if is_staging_name(&st.name) {
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

/// S3 multipart ETag: `hex(md5(concat(part-md5-bytes)))-<part count>`.
///
/// Each part's ETag is already the hex MD5 of that part, so the digest is
/// computed over the decoded bytes of those hex strings, exactly as S3 does.
fn multipart_etag(part_etags: &[String]) -> String {
    let mut ctx = md5::Context::new();
    for etag in part_etags {
        match hex::decode(etag) {
            Ok(bytes) => ctx.consume(&bytes),
            // A part ETag that is not a hex MD5 cannot be part of a spec ETag;
            // hashing its text keeps the value deterministic.
            Err(_) => ctx.consume(etag.as_bytes()),
        }
    }
    format!("\"{:x}-{}\"", ctx.compute(), part_etags.len())
}

#[cfg(test)]
mod multipart_etag_tests {
    use super::multipart_etag;

    /// S3 defines a multipart ETag as md5 over the part MD5 bytes, suffixed
    /// with the part count. Two uploads of identical parts must agree.
    #[test]
    fn multipart_etag_is_spec_shaped_and_deterministic() {
        let parts = vec!["d41d8cd98f00b204e9800998ecf8427e".to_string()];
        let etag = multipart_etag(&parts);
        // Shaped like "<32 hex>-<count>", wrapped in quotes.
        assert!(etag.starts_with('"'), "{etag}");
        assert!(etag.ends_with("-1\""), "{etag}");
        assert_eq!(etag, multipart_etag(&parts));
    }

    #[test]
    fn multipart_etag_changes_with_content_and_count() {
        let a = multipart_etag(&["aaaa".to_string(), "bbbb".to_string()]);
        let b = multipart_etag(&["aaaa".to_string(), "cccc".to_string()]);
        let c = multipart_etag(&["aaaa".to_string()]);
        assert_ne!(a, b, "different part digests must differ");
        assert_ne!(a, c, "different part counts must differ");
    }

    /// A completed upload used to return the literal "etag-not-computed".
    #[test]
    fn multipart_etag_is_not_a_placeholder() {
        let etag = multipart_etag(&["d41d8cd98f00b204e9800998ecf8427e".to_string()]);
        assert!(!etag.contains("not-computed"), "{etag}");
    }
}

#[cfg(test)]
mod precondition_tests {
    use super::{
        etag_value, evaluate_precondition_headers, is_strong, parse_http_date, Preconditions,
    };

    const ETAG: &str = "\"a826b09d5f9122902dbaef2a70d3e48f\"";
    // 2026-11-04T09:00:00Z
    const MTIME: i64 = 1_793_782_800_000;

    fn http_date(ms: i64) -> String {
        chrono::DateTime::from_timestamp(ms / 1000, 0)
            .unwrap()
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string()
    }

    fn eval(
        if_match: Option<&str>,
        if_none_match: Option<&str>,
        if_modified_since: Option<&str>,
        if_unmodified_since: Option<&str>,
    ) -> Option<u16> {
        evaluate_precondition_headers(
            &Preconditions {
                if_match: if_match.map(str::to_string),
                if_none_match: if_none_match.map(str::to_string),
                if_modified_since: if_modified_since.map(str::to_string),
                if_unmodified_since: if_unmodified_since.map(str::to_string),
                is_copy_source: false,
            },
            Some(ETAG),
            Some(MTIME),
        )
    }

    #[test]
    fn unconditional_requests_pass() {
        assert_eq!(eval(None, None, None, None), None);
    }

    // ---- If-Match: strong comparison (RFC 9110 8.8.3) ----
    #[test]
    fn if_match_requires_a_strong_match() {
        assert_eq!(eval(Some(ETAG), None, None, None), None);
        // An unquoted value is still a strong validator in S3.
        assert_eq!(
            eval(Some("a826b09d5f9122902dbaef2a70d3e48f"), None, None, None),
            None
        );
        // A weak validator never satisfies If-Match.
        assert_eq!(
            eval(Some(&format!("W/{ETAG}")), None, None, None),
            Some(412)
        );
        assert_eq!(eval(Some("\"deadbeef\""), None, None, None), Some(412));
        // A list matches if any member matches.
        assert_eq!(
            eval(Some(&format!("\"nope\", {ETAG}")), None, None, None),
            None
        );
        assert_eq!(eval(Some("*"), None, None, None), None);
    }

    // ---- If-None-Match: weak comparison ----
    #[test]
    fn if_none_match_uses_weak_comparison() {
        assert_eq!(eval(None, Some(ETAG), None, None), Some(304));
        // Weak and strong forms compare equal here.
        assert_eq!(
            eval(None, Some(&format!("W/{ETAG}")), None, None),
            Some(304)
        );
        assert_eq!(eval(None, Some("\"other\""), None, None), None);
        assert_eq!(eval(None, Some("*"), None, None), Some(304));
    }

    // ---- Time conditions ----
    #[test]
    fn if_modified_since_returns_304_when_not_modified() {
        // mtime == limit: not modified since the client's copy.
        assert_eq!(eval(None, None, Some(&http_date(MTIME)), None), Some(304));
        // Object newer than the limit: serve it.
        assert_eq!(
            eval(None, None, Some(&http_date(MTIME - 60_000)), None),
            None
        );
    }

    #[test]
    fn if_unmodified_since_returns_412_when_modified() {
        assert_eq!(
            eval(None, None, None, Some(&http_date(MTIME - 60_000))),
            Some(412)
        );
        assert_eq!(eval(None, None, None, Some(&http_date(MTIME))), None);
    }

    /// RFC 9110 13.2.2: a present If-Match overrides If-Unmodified-Since,
    /// and a present If-None-Match overrides If-Modified-Since.
    #[test]
    fn etag_conditions_take_precedence_over_dates() {
        // If-Match passes, so If-Unmodified-Since must be ignored.
        assert_eq!(
            eval(Some(ETAG), None, None, Some(&http_date(MTIME - 60_000))),
            None
        );
        // If-None-Match does not match, so If-Modified-Since must be ignored.
        assert_eq!(
            eval(None, Some("\"other\""), Some(&http_date(MTIME)), None),
            None
        );
    }

    #[test]
    fn unparsable_dates_are_ignored() {
        assert_eq!(eval(None, None, Some("not a date"), None), None);
        assert_eq!(eval(None, None, None, Some("nonsense")), None);
    }

    #[test]
    fn http_dates_parse_in_their_permitted_forms() {
        assert!(parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT").is_some());
        assert!(parse_http_date("Sunday, 06-Nov-94 08:49:37 GMT").is_some());
        assert!(parse_http_date("Sun Nov  6 08:49:37 1994").is_some());
        assert!(parse_http_date("2026-11-04T09:00:00Z").is_none());
    }

    /// A missing object cannot satisfy If-Match, and must not answer 304.
    #[test]
    fn missing_object_fails_if_match_but_not_if_none_match() {
        let cond = |im: Option<&str>, inm: Option<&str>| Preconditions {
            if_match: im.map(str::to_string),
            if_none_match: inm.map(str::to_string),
            ..Default::default()
        };
        assert_eq!(
            evaluate_precondition_headers(&cond(Some(ETAG), None), None, None),
            Some(412)
        );
        assert_eq!(
            evaluate_precondition_headers(&cond(None, Some(ETAG)), None, None),
            None
        );
    }

    #[test]
    fn weakness_and_quoting_are_normalised() {
        assert!(is_strong("\"abc\""));
        assert!(is_strong("abc"));
        assert!(!is_strong("W/\"abc\""));
        assert!(!is_strong("w/\"abc\""));
        for form in ["\"a826\"", "a826", "W/\"a826\"", "\"A826\""] {
            assert_eq!(etag_value(form), "a826", "{form}");
        }
    }
}

#[cfg(test)]
mod put_destination_condition_tests {
    use crate::s3::handlers::{evaluate_precondition_headers, Preconditions};

    fn cond(if_match: Option<&str>, if_none_match: Option<&str>) -> Preconditions {
        Preconditions {
            if_match: if_match.map(str::to_string),
            if_none_match: if_none_match.map(str::to_string),
            ..Default::default()
        }
    }

    /// PutObject applies the destination conditions; a stale `If-Match` must be
    /// refused so a concurrent replacement is not silently overwritten.
    #[test]
    fn put_destination_conditions_are_enforced() {
        let etag = "\"abc123\"";
        assert_eq!(
            evaluate_precondition_headers(&cond(Some("\"old\""), None), Some(etag), None),
            Some(412)
        );
        assert_eq!(
            evaluate_precondition_headers(&cond(Some(etag), None), Some(etag), None),
            None
        );
        // `If-None-Match: *` refuses to overwrite an existing object.
        assert_eq!(
            evaluate_precondition_headers(&cond(None, Some("*")), Some(etag), None),
            Some(304)
        );
        assert_eq!(
            evaluate_precondition_headers(&cond(None, Some("*")), None, None),
            None
        );
    }
}

#[cfg(test)]
mod part_validation_tests {
    use super::{
        reject_reserved_key, same_etag, validate_part_order, validate_staged_parts, PartMeta,
        MIN_PART_BYTES, STAGING_KEY_PREFIX,
    };

    fn parts(numbers: &[u32]) -> Vec<(String, u32)> {
        numbers
            .iter()
            .map(|n| (format!("\"etag{n}\""), *n))
            .collect()
    }

    fn staged(numbers_and_sizes: &[(u32, usize)]) -> Vec<(u32, PartMeta)> {
        numbers_and_sizes
            .iter()
            .map(|(n, size)| {
                (
                    *n,
                    PartMeta {
                        etag: format!("etag{n}"),
                        size: *size,
                    },
                )
            })
            .collect()
    }

    /// The production order check, not a test-local copy of it.
    #[test]
    fn part_order_requires_strictly_ascending_numbers() {
        assert!(validate_part_order(&parts(&[1, 2, 3])).is_ok());
        assert!(validate_part_order(&parts(&[3])).is_ok());

        let duplicate = validate_part_order(&parts(&[1, 1])).unwrap_err();
        assert!(duplicate.starts_with("InvalidPartOrder:"), "{duplicate}");

        let descending = validate_part_order(&parts(&[2, 1])).unwrap_err();
        assert!(descending.starts_with("InvalidPartOrder:"), "{descending}");

        let empty = validate_part_order(&parts(&[])).unwrap_err();
        assert!(empty.starts_with("InvalidPart:"), "{empty}");
    }

    /// The production size check: an undersized non-final part is refused, the
    /// final part is exempt, and a missing part is caught here rather than
    /// during assembly.
    #[test]
    fn non_final_parts_must_reach_the_minimum() {
        let big = MIN_PART_BYTES;
        let ok = validate_staged_parts(&parts(&[1, 2]), &staged(&[(1, big), (2, 1)]));
        assert!(ok.is_ok(), "a small final part is allowed: {ok:?}");

        let small_non_final =
            validate_staged_parts(&parts(&[1, 2]), &staged(&[(1, big - 1), (2, 1)])).unwrap_err();
        assert!(
            small_non_final.starts_with("EntityTooSmall:"),
            "{small_non_final}"
        );

        // A single part is also the final part, so it is exempt.
        assert!(validate_staged_parts(&parts(&[1]), &staged(&[(1, 1)])).is_ok());
    }

    #[test]
    fn staged_part_etags_must_match_the_request() {
        let mismatch =
            validate_staged_parts(&parts(&[1]), &staged(&[(2, MIN_PART_BYTES)])).unwrap_err();
        assert!(mismatch.starts_with("InvalidPart:"), "{mismatch}");
    }

    #[test]
    fn etags_compare_as_digests() {
        assert!(same_etag("abc123", "\"abc123\""));
        assert!(same_etag("W/\"ABC123\"", "abc123"));
        assert!(!same_etag("abc123", "other"));
    }

    /// Reserving the prefix is what makes hiding staging objects from listings
    /// sound: no accepted key can collide with it.
    #[test]
    fn the_staging_prefix_is_reserved_on_write_paths() {
        for key in [
            ".curvine-s3-report",
            "folder/.curvine-s3-mp-abc",
            format!("{STAGING_KEY_PREFIX}staging-1").as_str(),
            // Only checking the last segment used to accept these, and the
            // parent directory then vanished from listings.
            ".curvine-s3-x/file",
            "folder/.curvine-s3-x/file",
            "a/.curvine-s3-staging-1/b/c",
        ] {
            let err = reject_reserved_key(key).unwrap_err();
            assert!(err.starts_with("InvalidArgument:"), "{key}: {err}");
        }
        for key in [
            "report",
            "folder/file",
            "folder/.hidden",
            ".curvine-s3",
            "folder/.curvine-s3",
        ] {
            assert!(reject_reserved_key(key).is_ok(), "{key} must be accepted");
        }
    }
}
