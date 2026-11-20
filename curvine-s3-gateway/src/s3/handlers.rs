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
use crate::s3::s3_api::HeadObjectResult;
use crate::utils::s3_utils::file_status_to_head_object_result;
use crate::utils::signed_spool::SignedSpool;
use crate::utils::temp_storage::{PartMeta, TempStorageEnum};
use chrono;
use curvine_client::unified::UnifiedFileSystem;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{FileSystem, Path, Writer};
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

/// Answer to a write refused by a read-only mount. The message is fixed: the
/// message of a 4xx answer is sent to the client, and the filesystem error
/// names internal paths.
const READ_ONLY_DENIED: &str = "AccessDenied: the path is on a read-only mount";

/// Error text for a filesystem error at a handler exit. A read-only
/// `cache_mode` mount refuses writes by policy (`FsError::ReadOnly`, from
/// `UnifiedFileSystem::check_mount_write` for create, set_attr, mkdir, rename
/// and delete), which S3 reports as `AccessDenied` (403); as a 500 it was
/// masked as `InternalError` and SDKs retried it. Other errors keep their text,
/// and so their status.
pub(crate) fn fs_error_text(e: FsError) -> String {
    match e {
        FsError::ReadOnly(_) => READ_ONLY_DENIED.to_string(),
        e => e.to_string(),
    }
}

/// [`fs_error_text`] for the exits that prefix the error with what failed.
pub(crate) fn fs_error_text_with(context: &str, e: FsError) -> String {
    match e {
        FsError::ReadOnly(_) => READ_ONLY_DENIED.to_string(),
        e => format!("{context}: {e}"),
    }
}

/// Directory markers below `dir`, deepest first.
///
/// Iterative on purpose: an async recursion over a deep tree would have to box
/// every level. Any file below `dir` means the bucket is not empty, which is
/// reported as `BucketNotEmpty`.
async fn empty_directory_markers(fs: &UnifiedFileSystem, dir: &Path) -> Result<Vec<Path>, String> {
    let mut markers = Vec::new();
    let mut pending = vec![dir.clone()];
    while let Some(current) = pending.pop() {
        let children = fs.list_status(&current).await.map_err(|e| e.to_string())?;
        for child in children {
            if !child.is_dir {
                return Err(BUCKET_NOT_EMPTY.to_string());
            }
            let child_path = Path::from_str(&child.path).map_err(|e| e.to_string())?;
            pending.push(child_path.clone());
            markers.push(child_path);
        }
    }
    // Deepest first: a parent can only be removed once its children are gone.
    markers.sort_by_key(|marker| std::cmp::Reverse(marker.full_path().len()));
    Ok(markers)
}

#[derive(Clone)]
pub struct S3Handlers {
    pub fs: UnifiedFileSystem,
    pub region: String,
    pub temp_storage: Arc<TempStorageEnum>,
    pub rt: std::sync::Arc<AsyncRuntime>,
    pub get_chunk_size_bytes: usize,
    pub(crate) signed_spool: Arc<SignedSpool>,
}

impl S3Handlers {
    pub fn new(
        fs: UnifiedFileSystem,
        region: String,
        temp_storage: Arc<TempStorageEnum>,
        rt: std::sync::Arc<AsyncRuntime>,
        get_chunk_size_mb: f32,
    ) -> Self {
        let defaults = curvine_config::S3GatewayConf::default();
        let signed_spool = SignedSpool::new(
            defaults.signed_chunk_spool_dir,
            defaults.signed_chunk_spool_max_bytes,
        );
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
            signed_spool,
        }
    }

    pub(crate) fn with_signed_spool(mut self, spool: Arc<SignedSpool>) -> Self {
        self.signed_spool = spool;
        self
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

    /// The region GetBucketLocation reports for `bucket`: the gateway's
    /// configured region, once the bucket is known to exist. S3 answers
    /// `NoSuchBucket` (404) for a missing bucket.
    #[inline]
    pub async fn handle_get_bucket_location(&self, bucket: &str) -> Result<String, String> {
        let path = self
            .cv_bucket_path(bucket)
            .map_err(|_| NO_SUCH_BUCKET.to_string())?;
        match self.fs.get_status(&path).await {
            Ok(status) if status.is_dir => Ok(self.region.clone()),
            Ok(_) | Err(FsError::FileNotFound(_)) => Err(NO_SUCH_BUCKET.to_string()),
            Err(e) => Err(format!("Failed to stat bucket: {e}")),
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

impl S3Handlers {
    pub(crate) async fn list_multipart_uploads(
        &self,
        bucket: &str,
        opt: super::multipart_listing::UploadListOptions,
    ) -> Result<super::dto::ListMultipartUploadsResult, String> {
        let path = self
            .cv_bucket_path(bucket)
            .map_err(|_| NO_SUCH_BUCKET.to_string())?;
        let status = self.fs.get_status(&path).await.map_err(|e| match e {
            curvine_error::FsError::FileNotFound(_) => NO_SUCH_BUCKET.to_string(),
            other => format!("Failed to stat bucket: {other}"),
        })?;
        if !status.is_dir {
            return Err(NO_SUCH_BUCKET.to_string());
        }
        let uploads = if opt.max_uploads == 0 {
            Vec::new()
        } else {
            self.temp_storage.list_uploads(bucket).await?
        };
        Ok(super::multipart_listing::page_uploads(bucket, opt, uploads))
    }
}

impl S3Handlers {
    pub(crate) async fn lookup_object(
        &self,
        bucket: &str,
        object: &str,
    ) -> Option<(HeadObjectResult, i64)> {
        let path = self.cv_object_path(bucket, object).ok()?;
        let st = self.fs.get_status(&path).await.ok()?;
        let head = if st.file_type == FileType::File && !object.ends_with('/') {
            file_status_to_head_object_result(&st, object)
        } else if crate::utils::s3_utils::is_directory_marker(&st, object) {
            HeadObjectResult {
                content_length: Some(0),
                last_modified: crate::utils::s3_utils::format_http_timestamp(st.mtime),
                accept_ranges: Some("bytes".to_string()),
                etag: Some(format!("\"{}\"", crate::utils::s3_utils::EMPTY_MD5)),
                content_type: Some(crate::utils::s3_utils::DIRECTORY_CONTENT_TYPE.to_string()),
                ..Default::default()
            }
        } else {
            return None;
        };
        Some((head, st.mtime))
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

        let this = self.clone();

        async move {
            // The staging prefix is reserved; a PUT cannot create an object
            // that listings would have to hide.
            if let Err(err) = reject_reserved_key(&object) {
                return Err(discard_body(&mut body, err).await);
            }

            // S3 requires the bucket to exist. The staging write creates its
            // parents, so without this check a typo'd bucket name succeeded and
            // the bucket then showed up in ListBuckets.
            let bucket_path = this.cv_bucket_path(&bucket).map_err(|e| e.to_string())?;
            match fs.get_status(&bucket_path).await {
                Ok(st) if st.is_dir => {}
                _ => return Err(discard_body(&mut body, NO_SUCH_BUCKET.to_string()).await),
            }

            // A key ending in '/' is a directory marker: OpenDAL `create_dir`,
            // the AWS console and Hadoop S3A all write one. Storing it as a
            // file named without the slash made every later PUT under that
            // prefix fail with "Parent path is not a directory".
            if object.ends_with('/') {
                if !crate::utils::s3_utils::is_directory_marker_key(&object) {
                    let err = "InvalidArgument: empty path component in directory marker key";
                    return Err(discard_body(&mut body, err.to_string()).await);
                }
                // A directory marker carries no body; storing one would put
                // bytes under a key that stands for a directory.
                if opt.content_length.unwrap_or(0) > 0 {
                    let err =
                        "UnexpectedContent: a directory marker must not carry a body".to_string();
                    return Err(discard_body(&mut body, err).await);
                }
                let dir = this
                    .cv_object_path(&bucket, &object)
                    .map_err(|e| e.to_string())?;
                // A backend may return false for an existing path. Only a
                // directory (including an implicit prefix) can take a marker.
                if !fs.mkdir(&dir, true).await.map_err(fs_error_text)?
                    && !fs.get_status(&dir).await.map_err(|e| e.to_string())?.is_dir
                {
                    return Err(
                        "InvalidArgument: a file already occupies the directory marker path"
                            .to_string(),
                    );
                }
                let marker = curvine_model::SetAttrOptsBuilder::new()
                    .add_x_attr(crate::utils::s3_utils::XATTR_S3_DIR_MARKER, b"1".to_vec())
                    .build();
                fs.set_attr(&dir, marker).await.map_err(fs_error_text)?;
                return Ok(format!("\"{}\"", crate::utils::s3_utils::EMPTY_MD5));
            }

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
                    let err = "PreconditionFailed: destination condition not met".to_string();
                    return Err(discard_body(&mut body, err).await);
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
            let Some((_, key)) = object.split_once('/') else {
                return Ok(());
            };
            if key.is_empty()
                || (key.ends_with('/') && !crate::utils::s3_utils::is_directory_marker_key(key))
            {
                return Ok(());
            }
            let path = Path::from_str(format!("/{object}")).map_err(|e| e.to_string())?;
            // `key/` is a zero-byte object in S3: deleting it removes the
            // marker and, when nothing else lives under the prefix, the
            // directory itself. It never removes the keys below.
            let st = match fs.get_status(&path).await {
                Ok(st) => st,
                Err(FsError::FileNotFound(_)) => return Ok(()),
                Err(e) => return Err(fs_error_text(e)),
            };
            if st.is_dir != key.ends_with('/')
                || (st.is_dir && !crate::utils::s3_utils::is_directory_marker(&st, key))
            {
                // Not the addressed S3 object: answer 204 and leave `path` alone.
                return Ok(());
            }
            match fs.delete(&path, false).await {
                Ok(_) | Err(FsError::FileNotFound(_)) => Ok(()),
                Err(FsError::DirNotEmpty(_)) if st.is_dir => {
                    // Children keep the prefix alive; only the marker goes away.
                    let opts = curvine_model::SetAttrOptsBuilder::new()
                        .remove_x_attr(crate::utils::s3_utils::XATTR_S3_DIR_MARKER)
                        .build();
                    match fs.set_attr(&path, opts).await {
                        Ok(_) | Err(FsError::FileNotFound(_)) => Ok(()),
                        Err(e) => Err(fs_error_text(e)),
                    }
                }
                Err(e) => Err(fs_error_text(e)),
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

            fs.mkdir(&path, true).await.map_err(fs_error_text)?;
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
            // Objects nest arbitrarily deep, so the bucket is empty only when
            // no file remains anywhere below it. `children_num` only counts the
            // first level and kept a bucket undeletable once its objects were
            // removed but their parent directories were not.
            //
            // The markers are then removed deepest-first WITHOUT `recursive`:
            // a non-recursive delete of a directory only succeeds while it is
            // empty, so an object that lands after the walk fails the delete
            // and is reported as BucketNotEmpty instead of being removed by a
            // recursive delete the client never asked for.
            let markers = empty_directory_markers(&fs, &path).await?;
            for marker in markers {
                if fs.delete(&marker, false).await.is_err() {
                    return Err(BUCKET_NOT_EMPTY.to_string());
                }
            }
            fs.delete(&path, false)
                .await
                .map(|_| ())
                .map_err(fs_error_text)
        }
    }
}

impl S3Handlers {
    /// UploadPartCopy: stage `src` (or its `x-amz-copy-source-range` span) as
    /// part `part_number` of `upload_id`, returning the quoted `"<md5>"` ETag.
    pub async fn upload_part_copy(
        &self,
        (source_bucket, source_key): (&str, &str),
        (dest_bucket, dest_key): (&str, &str),
        upload_id: &str,
        part_number: u32,
        range: Option<&str>,
        source_conditions: &Preconditions,
    ) -> Result<String, String> {
        use curvine_fs_api::Reader;

        reject_reserved_key(dest_key)?;
        self.cv_object_path(dest_bucket, dest_key)
            .map_err(|e| format!("InvalidArgument: invalid copy destination: {e}"))?;
        for bucket in [dest_bucket, source_bucket] {
            let path = self
                .cv_bucket_path(bucket)
                .map_err(|_| NO_SUCH_BUCKET.to_string())?;
            if !self
                .fs
                .get_status(&path)
                .await
                .map_err(|e| copy_lookup_error(e, NO_SUCH_BUCKET))?
                .is_dir
            {
                return Err(NO_SUCH_BUCKET.to_string());
            }
        }
        self.temp_storage
            .validate_session(upload_id, dest_bucket, dest_key)
            .await?;
        let missing =
            || format!("NoSuchKey: copy source {source_bucket}/{source_key} does not exist");
        let src = self
            .cv_object_path(source_bucket, source_key)
            .map_err(|_| missing())?;
        let status = self
            .fs
            .get_status(&src)
            .await
            .map_err(|e| copy_lookup_error(e, &missing()))?;
        if status.is_dir {
            return Err(missing());
        }
        let etag = crate::utils::s3_utils::generate_etag(&status);
        if let Some(code) =
            evaluate_precondition_headers(source_conditions, Some(&etag), Some(status.mtime))
        {
            return Err(format!(
                "PreconditionFailed: copy source condition returned {code}"
            ));
        }
        let size = status.len.max(0) as u64;
        let (start, length) = match range {
            Some(range) => copy_source_range(range, size)?,
            None => (0, size),
        };
        if length > MAX_PART_BYTES {
            return Err(format!(
                "EntityTooLarge: part size {length} exceeds the 5 GiB maximum"
            ));
        }

        let mut reader = self.fs.open(&src).await.map_err(|e| e.to_string())?;
        if start > 0 {
            reader
                .seek(start as i64)
                .await
                .map_err(|e| format!("Failed to seek copy source: {e}"))?;
        }
        let mut body = CopySourceBody {
            reader,
            left: length,
            chunk: self.get_chunk_size_bytes,
        };
        let expect = crate::utils::temp_storage::StageExpect {
            sha256_hex: None,
            content_length: Some(length as usize),
        };
        self.temp_storage
            .write_part_stream(upload_id, part_number, &mut body, expect)
            .await
    }

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
        replacement: Option<&crate::utils::s3_utils::S3Metadata>,
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
            .map_err(|e| fs_error_text_with("Failed to create copy staging object", e))?;

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

        // Attach the metadata while the copy is still staging, so the rename
        // below publishes body and metadata together. Writing it afterwards
        // left a window where HEAD answered without Content-Type, and a failed
        // write only logged for an object that was already visible.
        let etag = format!("{:x}", md5.compute());
        match replacement {
            // REPLACE: the request carries the metadata and the source's is
            // dropped, which is how a client rewrites Content-Type or tags
            // without re-uploading the body.
            Some(metadata) => {
                if let Err(e) = metadata.write_to(&self.fs, &staging, &etag).await {
                    let _ = self.fs.delete(&staging, false).await;
                    return Err(format!("Failed to record replaced copy metadata: {e}"));
                }
            }
            // COPY: the destination inherits the source's Content-Type and user
            // metadata; the upload path records them the same way.
            None => {
                let mut opts = curvine_model::SetAttrOptsBuilder::new().add_x_attr(
                    crate::utils::s3_utils::XATTR_S3_ETAG,
                    etag.as_bytes().to_vec(),
                );
                // The destination keeps the source metadata: inherit every
                // `s3.*` attribute except the ETag this copy recomputes. A
                // hard-coded list silently dropped headers added later (for
                // example Content-Encoding), which serves gzip bytes as
                // identity.
                for (key, value) in &status.x_attr {
                    if key.starts_with(crate::utils::s3_utils::XATTR_S3_PREFIX)
                        && key != crate::utils::s3_utils::XATTR_S3_ETAG
                        && key != crate::utils::s3_utils::XATTR_S3_COMPLETED_UPLOAD
                        && !key.starts_with(crate::utils::s3_utils::XATTR_S3_META_PREFIX)
                    {
                        opts = opts.add_x_attr(key.clone(), value.clone());
                    }
                }
                for (key, value) in &status.x_attr {
                    if key.starts_with(crate::utils::s3_utils::XATTR_S3_META_PREFIX) {
                        opts = opts.add_x_attr(key.clone(), value.clone());
                    }
                }
                if let Err(e) = self.fs.set_attr(&staging, opts.build()).await {
                    let _ = self.fs.delete(&staging, false).await;
                    return Err(fs_error_text_with("Failed to record copy metadata", e));
                }
            }
        }

        // Publish. `rename` replaces the destination, so the object only ever
        // holds the previous body or the complete new one with its metadata.
        if let Err(e) = self.fs.rename(&staging, &dst).await {
            let _ = self.fs.delete(&staging, false).await;
            return Err(fs_error_text_with("Failed to publish copy destination", e));
        }

        Ok(etag)
    }

    /// ListParts: `GET /{bucket}/{key}?uploadId=...`.
    pub async fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number_marker: Option<&str>,
        max_parts: Option<&str>,
    ) -> Result<crate::s3::dto::ListPartsResult, String> {
        let marker = parse_nonneg_i32(part_number_marker, "part-number-marker")?.unwrap_or(0);
        let max_parts = parse_nonneg_i32(max_parts, "max-parts")?
            .unwrap_or(1000)
            .min(1000);

        reject_reserved_key(key)?;
        self.cv_object_path(bucket, key)
            .map_err(|e| format!("InvalidArgument: invalid object path: {e}"))?;
        let bkt_path = self
            .cv_bucket_path(bucket)
            .map_err(|_| NO_SUCH_BUCKET.to_string())?;
        let bkt_status = self.fs.get_status(&bkt_path).await.map_err(|e| match e {
            curvine_error::FsError::FileNotFound(_) => NO_SUCH_BUCKET.to_string(),
            other => format!("Failed to stat bucket: {other}"),
        })?;
        if !bkt_status.is_dir {
            return Err(NO_SUCH_BUCKET.to_string());
        }
        self.temp_storage
            .validate_session(upload_id, bucket, key)
            .await?;

        let (parts, is_truncated) = self
            .temp_storage
            .list_parts(upload_id, marker, max_parts as usize)
            .await?;
        let next_part_number_marker = is_truncated
            .then(|| parts.last().map(|p| p.part_number))
            .flatten();
        Ok(crate::s3::dto::ListPartsResult {
            xmlns: crate::s3::dto::S3_NAMESPACE.to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id: upload_id.to_string(),
            initiator: crate::s3::dto::Owner::default(),
            owner: crate::s3::dto::Owner::default(),
            storage_class: "STANDARD".to_string(),
            part_number_marker: marker,
            next_part_number_marker,
            max_parts,
            is_truncated,
            parts,
        })
    }
}

fn parse_nonneg_i32(raw: Option<&str>, name: &str) -> Result<Option<u32>, String> {
    let Some(raw) = raw.filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    raw.bytes()
        .all(|b| b.is_ascii_digit())
        .then(|| raw.parse::<i32>().ok().map(|n| n as u32))
        .flatten()
        .map(Some)
        .ok_or_else(|| {
            format!("InvalidArgument: Argument {name} must be an integer between 0 and 2147483647")
        })
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

/// S3 maximum size for a single part.
const MAX_PART_BYTES: u64 = 5 << 30;

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

/// How much of a rejected PUT body is read and discarded before the error is
/// returned. Within it the client reads the error and the connection stays
/// open; past it the connection is closed as before, instead of reading an
/// arbitrarily large body that will not be stored.
const EARLY_REJECT_DRAIN_LIMIT: u64 = 64 * 1024 * 1024;

/// Read and discard the rest of `body`, up to [`EARLY_REJECT_DRAIN_LIMIT`]
/// bytes, then hand back `err`.
///
/// A PUT rejected before its body is read used to drop the body unread. hyper
/// then closes the connection with request bytes still in the socket, the
/// kernel answers them with a reset, and a client that sends without
/// `Expect: 100-continue` gets a broken pipe instead of the error (#2186).
async fn discard_body(body: &mut crate::utils::io::PollReaderEnum, err: String) -> String {
    let mut left = EARLY_REJECT_DRAIN_LIMIT;
    while left > 0 {
        match body.poll_read().await {
            Ok(Some(chunk)) => left = left.saturating_sub(chunk.len() as u64),
            Ok(None) | Err(_) => break,
        }
    }
    err
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
        // The protocol calls an empty Complete payload MalformedXML, not a
        // part error: nothing was listed to validate.
        return Err("MalformedXML: no parts listed".to_string());
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

/// Fixed-size `<upload_id>:<sha256>` receipt stored in `s3.completed_upload`
/// so a retried Complete can check both `upload_id` and the `(part_num, etag)`
/// list without storing unbounded part lists in inode xattrs.
fn encode_completed_upload(upload_id: &str, parts: &[(String, u32)]) -> Vec<u8> {
    format!("{upload_id}:{}", completed_parts_digest(parts)).into_bytes()
}

fn completed_parts_digest(parts: &[(String, u32)]) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    for (etag, part_num) in parts {
        let clean = etag
            .trim()
            .trim_start_matches("W/")
            .trim_matches('"')
            .to_ascii_lowercase();
        hasher.update(part_num.to_le_bytes());
        hasher.update(b":");
        hasher.update(clean.as_bytes());
        hasher.update(b"\n");
    }
    hex::encode(hasher.finalize())
}

/// Replay a `CompleteMultipartUpload` whose session directory was already
/// cleaned up (#2184): same upload and parts returns the committed ETag,
/// different parts returns `InvalidPart`, anything else returns `NoSuchUpload`.
fn replay_completed_upload(
    recorded: Option<&[u8]>,
    stored_etag: Option<&[u8]>,
    upload_id: &str,
    parts: &[(String, u32)],
) -> Result<String, String> {
    let missing = || format!("NoSuchUpload: upload {upload_id} does not exist");
    let text = recorded
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .ok_or_else(missing)?;
    let (recorded_id, recorded_digest) = text.rsplit_once(':').ok_or_else(missing)?;
    if recorded_id != upload_id {
        return Err(missing());
    }
    let etag = stored_etag
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .ok_or_else(missing)?;
    if recorded_digest != completed_parts_digest(parts) {
        return Err(format!(
            "InvalidPart: parts do not match completed upload {upload_id}"
        ));
    }
    Ok(format!("\"{etag}\""))
}

/// Only an absent inode is a 404; RPC and storage failures remain retryable.
fn copy_lookup_error(err: curvine_error::FsError, missing: &str) -> String {
    match err {
        curvine_error::FsError::FileNotFound(_) => missing.to_string(),
        other => format!("Failed to stat copy path: {other}"),
    }
}

/// The `(start, length)` an `x-amz-copy-source-range` selects from a source of
/// `size` bytes. S3 accepts only `bytes=first-last` with both ends present.
fn copy_source_range(range: &str, size: u64) -> Result<(u64, u64), String> {
    let invalid = || format!("InvalidArgument: invalid x-amz-copy-source-range {range:?}");
    let (first, last) = range
        .strip_prefix("bytes=")
        .and_then(|span| span.split_once('-'))
        .ok_or_else(invalid)?;
    let number = |s: &str| {
        (!s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
            .then(|| s.parse::<u64>().ok())
            .flatten()
    };
    let (Some(first), Some(last)) = (number(first), number(last)) else {
        return Err(invalid());
    };
    if first > last {
        return Err(invalid());
    }
    if last >= size {
        return Err(format!(
            "InvalidRange: x-amz-copy-source-range {range:?} is outside the {size}-byte source"
        ));
    }
    Ok((first, last - first + 1))
}

/// The copied span of a source object, read as a part body.
struct CopySourceBody {
    reader: curvine_client::unified::UnifiedReader,
    left: u64,
    chunk: usize,
}

#[async_trait::async_trait]
impl crate::utils::io::PollRead for CopySourceBody {
    async fn poll_read(&mut self) -> Result<Option<Vec<u8>>, String> {
        use curvine_fs_api::Reader;
        if self.left == 0 {
            return Ok(None);
        }
        let mut buf = vec![0u8; self.chunk.min(self.left.min(usize::MAX as u64) as usize)];
        let n = self
            .reader
            .read(&mut buf)
            .await
            .map_err(|e| format!("Failed to read copy source: {e}"))?;
        if n == 0 {
            return Err("Failed to read copy source: it ended early".to_string());
        }
        buf.truncate(n);
        self.left -= n as u64;
        Ok(Some(buf))
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

#[async_trait::async_trait]
impl crate::s3::s3_api::MultiUploadObjectHandler for S3Handlers {
    async fn handle_create_session(&self, bucket: String, key: String) -> Result<String, String> {
        let upload_id = uuid::Uuid::new_v4().to_string();
        self.temp_storage
            .create_session(&upload_id, &bucket, &key)
            .await?;

        tracing::debug!(
            "Created multipart upload session: {} (storage: {})",
            upload_id,
            self.temp_storage.storage_type()
        );
        Ok(upload_id)
    }

    async fn handle_upload_part<R: crate::utils::io::PollRead + Send>(
        &self,
        bucket: String,
        key: String,
        upload_id: String,
        part_number: u32,
        body: &mut R,
        expect: crate::utils::temp_storage::StageExpect<'_>,
    ) -> Result<String, String> {
        // The signed request path must match the session that owns the part.
        // An existing upload id for another key is not a write authorization.
        self.temp_storage
            .validate_session(&upload_id, &bucket, &key)
            .await?;
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
                Err(e)
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

        // An unknown, aborted or already-completed upload id is a 404, unless
        // the current object at `final_path` was published by `upload_id` (#2184).
        if !self.temp_storage.upload_exists(&upload_id).await {
            let status = self.fs.get_status(&final_path).await.ok();
            let recorded = status
                .as_ref()
                .and_then(|st| {
                    st.x_attr
                        .get(crate::utils::s3_utils::XATTR_S3_COMPLETED_UPLOAD)
                })
                .map(Vec::as_slice);
            let stored_etag = status
                .as_ref()
                .and_then(|st| st.x_attr.get(crate::utils::s3_utils::XATTR_S3_ETAG))
                .map(Vec::as_slice);
            return replay_completed_upload(recorded, stored_etag, &upload_id, &part_list);
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
                return Err(fs_error_text_with("Failed to create final file", e));
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
                    .add_x_attr(
                        crate::utils::s3_utils::XATTR_S3_COMPLETED_UPLOAD,
                        encode_completed_upload(&upload_id, &part_list),
                    )
                    .build(),
            )
            .await
        {
            let _ = self.fs.delete(&staging, false).await;
            return Err(fs_error_text_with("Failed to record multipart etag", e));
        }

        // Publish: the destination is only ever replaced by a complete object
        // that already carries its metadata.
        if let Err(e) = self.fs.rename(&staging, &final_path).await {
            let _ = self.fs.delete(&staging, false).await;
            return Err(fs_error_text_with("Failed to publish assembled object", e));
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
    ) -> Result<(), String> {
        // Like Complete: an unknown, aborted or already-completed upload id is
        // a 404 NoSuchUpload, so SDKs stop retrying a dead session.
        if !self.temp_storage.upload_exists(&upload_id).await {
            return Err(format!("NoSuchUpload: upload {upload_id} does not exist"));
        }
        // Cleanup temp files on abort
        if let Err(e) = self.temp_storage.cleanup(&upload_id).await {
            tracing::warn!(
                "Failed to cleanup temp files on abort for upload_id {}: {}",
                upload_id,
                e
            );
            return Err(e);
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

            // This backend keeps a single version per key, so versions use the
            // same ordered key traversal as ListObjectsV2 (#2095): every object
            // becomes one latest version, `Delimiter` groups into common
            // prefixes and `key-marker` pages exactly like `start-after`. Real
            // version history and delete markers do not exist here.
            let list_opt = opt.to_list_option();
            let dirs = BucketDirs {
                fs: &this.fs,
                bucket: &bkt_path,
            };
            let page = super::listing::list_objects(&list_opt, &dirs).await?;

            // Unversioned buckets report the S3 "null" version id.
            let versions = page
                .contents
                .into_iter()
                .map(|obj| ObjectVersion {
                    key: obj.key,
                    version_id: "null".to_string(),
                    is_latest: true,
                    last_modified: obj.last_modified.unwrap_or_else(|| {
                        chrono::Utc::now()
                            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                            .to_string()
                    }),
                    etag: obj.etag.unwrap_or_default(),
                    size: obj.size,
                    storage_class: obj.storage_class,
                    owner: obj.owner,
                })
                .collect();

            let result = ListObjectVersionsResult {
                xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
                name: bucket.to_string(),
                prefix: opt.prefix.clone(),
                key_marker: opt.key_marker.clone(),
                version_id_marker: opt.version_id_marker.clone(),
                // Resume at the first key of the next page; with one version
                // per key no version-id marker is needed.
                next_key_marker: page.next_marker,
                next_version_id_marker: None,
                max_keys: opt.max_keys.map(|k| k as u32),
                delimiter: opt.delimiter.clone(),
                encoding_type: opt.encoding_type.clone(),
                is_truncated: page.is_truncated,
                versions,
                delete_markers: Vec::new(),
                common_prefixes: page.common_prefixes,
            };

            tracing::info!(
                "ListObjectVersions completed for bucket: {}, found {} versions, {} common prefixes",
                bucket,
                result.versions.len(),
                result.common_prefixes.len()
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
mod delete_error_tests {
    use super::{fs_error_text, fs_error_text_with};
    use crate::s3::s3_api::status_for_error;
    use curvine_error::FsError;

    #[test]
    fn a_read_only_mount_is_access_denied() {
        let text = fs_error_text(FsError::read_only_msg("Delete on a read-only mount"));
        assert!(text.starts_with("AccessDenied:"), "{text}");
        assert_eq!(status_for_error(&text), 403);
        // Other failures stay server errors.
        let text = fs_error_text(FsError::common("rpc failed"));
        assert_eq!(status_for_error(&text), 500);
    }

    /// Every write exit, not only delete: PutObject, CopyObject and
    /// CompleteMultipartUpload on a read-only mount answer 403 as well.
    #[test]
    fn a_read_only_write_is_access_denied() {
        let e = FsError::read_only("/s3/bucket/key");
        let text = fs_error_text_with("Failed to publish copy destination", e);
        assert!(text.starts_with("AccessDenied:"), "{text}");
        assert_eq!(status_for_error(&text), 403);
        // The 403 message reaches the client; the internal path stays out.
        assert!(!text.contains("/s3/bucket/key"), "{text}");

        let text = fs_error_text_with("Failed to create final file", FsError::common("io"));
        assert!(text.starts_with("Failed to create final file: io"));
        assert_eq!(status_for_error(&text), 500);
    }
}

#[cfg(test)]
mod list_parts_tests {
    use super::parse_nonneg_i32;
    use crate::s3::dto::{ListPartsResult, Owner, Part, S3_NAMESPACE};

    #[test]
    fn list_parts_parameters_and_xml_follow_s3_shape() {
        assert_eq!(parse_nonneg_i32(None, "max-parts"), Ok(None));
        assert_eq!(parse_nonneg_i32(Some(""), "max-parts"), Ok(None));
        assert_eq!(parse_nonneg_i32(Some("0"), "max-parts"), Ok(Some(0)));
        assert_eq!(parse_nonneg_i32(Some("25"), "max-parts"), Ok(Some(25)));
        for bad in ["-1", "+1", "abc", "2147483648"] {
            assert!(parse_nonneg_i32(Some(bad), "max-parts")
                .unwrap_err()
                .starts_with("InvalidArgument:"));
        }

        let mut doc = ListPartsResult {
            xmlns: S3_NAMESPACE.to_string(),
            bucket: "bkt".to_string(),
            key: "a b/c<&>".to_string(),
            upload_id: "u1".to_string(),
            initiator: Owner::default(),
            owner: Owner::default(),
            storage_class: "STANDARD".to_string(),
            part_number_marker: 1,
            next_part_number_marker: Some(2),
            max_parts: 1,
            is_truncated: true,
            parts: vec![Part::staged(
                2,
                "0123456789abcdef0123456789abcdef",
                5242880,
                "2026-11-17T00:00:00.000Z".to_string(),
            )],
        };
        let xml = quick_xml::se::to_string(&doc).expect("serialize ListPartsResult");
        for needle in [
            r#"<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#,
            "<Bucket>bkt</Bucket>",
            // ListParts has no encoding-type: the key is XML-escaped only.
            "<Key>a b/c&lt;&amp;&gt;</Key>",
            "<UploadId>u1</UploadId>",
            "<StorageClass>STANDARD</StorageClass>",
            "<PartNumberMarker>1</PartNumberMarker>",
            "<NextPartNumberMarker>2</NextPartNumberMarker>",
            "<MaxParts>1</MaxParts>",
            "<IsTruncated>true</IsTruncated>",
            "<Part><PartNumber>2</PartNumber>",
            // XML text escapes the quotes; parsers return `"<md5>"`.
            "<ETag>&quot;0123456789abcdef0123456789abcdef&quot;</ETag><Size>5242880</Size></Part>",
        ] {
            assert!(xml.contains(needle), "missing {needle} in {xml}");
        }
        assert!(!xml.contains("Checksum"), "{xml}");

        doc.is_truncated = false;
        doc.next_part_number_marker = None;
        let final_xml = quick_xml::se::to_string(&doc).expect("serialize final ListPartsResult");
        assert!(!final_xml.contains("NextPartNumberMarker"), "{final_xml}");
    }
}

#[cfg(test)]
mod discard_body_tests {
    use super::{discard_body, EARLY_REJECT_DRAIN_LIMIT};
    use crate::utils::io::{PollRead, PollReaderEnum};

    const MIB: usize = 1024 * 1024;

    /// A body of `chunks` 1 MiB chunks that records how many were read.
    struct Chunks {
        left: usize,
        read: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl PollRead for Chunks {
        async fn poll_read(&mut self) -> Result<Option<Vec<u8>>, String> {
            if self.left == 0 {
                return Ok(None);
            }
            self.left -= 1;
            self.read.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Some(vec![0; MIB]))
        }
    }

    async fn chunks_read(chunks: usize) -> usize {
        let read = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut body: PollReaderEnum = Box::new(Chunks {
            left: chunks,
            read: read.clone(),
        });
        let err = discard_body(&mut body, "PreconditionFailed: x".to_string()).await;
        assert_eq!(err, "PreconditionFailed: x");
        read.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// A rejected body within the limit is read to the end, so hyper can keep
    /// the connection and the client reads the error (#2186); a larger one is
    /// read only up to the limit.
    #[tokio::test]
    async fn rejected_bodies_are_drained_up_to_the_limit() {
        assert_eq!(chunks_read(16).await, 16);
        let limit_chunks = EARLY_REJECT_DRAIN_LIMIT as usize / MIB;
        assert_eq!(chunks_read(limit_chunks + 16).await, limit_chunks);
    }
}

#[cfg(test)]
mod part_validation_tests {
    use super::{
        copy_lookup_error, copy_source_range, encode_completed_upload, reject_reserved_key,
        replay_completed_upload, same_etag, validate_part_order, validate_staged_parts, PartMeta,
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

        // The protocol names an empty Complete payload MalformedXML.
        let empty = validate_part_order(&parts(&[])).unwrap_err();
        assert!(empty.starts_with("MalformedXML:"), "{empty}");
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

    #[test]
    fn completed_upload_replays_matching_parts_and_rejects_mismatches() {
        let committed = vec![("\"aa\"".to_string(), 1), ("\"bb\"".to_string(), 2)];
        let rec = encode_completed_upload("u1", &committed);
        let etag = b"deadbeef-2";

        // Identical parts (regardless of quotes/case) replay the committed ETag.
        let unquoted = vec![("AA".to_string(), 1), ("bb".to_string(), 2)];
        assert_eq!(
            replay_completed_upload(Some(&rec), Some(etag), "u1", &unquoted).unwrap(),
            "\"deadbeef-2\""
        );
        // Wrong ETag or wrong part number on the same upload is InvalidPart.
        for bad in [
            vec![("\"00\"".to_string(), 1), ("\"bb\"".to_string(), 2)],
            vec![("\"aa\"".to_string(), 1), ("\"bb\"".to_string(), 3)],
            vec![("\"aa\"".to_string(), 1)],
        ] {
            let err = replay_completed_upload(Some(&rec), Some(etag), "u1", &bad).unwrap_err();
            assert!(err.starts_with("InvalidPart:"), "{err}");
        }
        // Unknown upload id or an object without a receipt stays NoSuchUpload.
        for err in [
            replay_completed_upload(Some(&rec), Some(etag), "u2", &committed).unwrap_err(),
            replay_completed_upload(None, Some(etag), "u1", &committed).unwrap_err(),
        ] {
            assert!(err.starts_with("NoSuchUpload:"), "{err}");
        }
    }

    #[test]
    fn copy_source_range_and_lookup_errors() {
        use crate::s3::s3_api::status_for_error;
        use curvine_error::FsError;

        assert_eq!(
            copy_lookup_error(FsError::file_not_found("/x"), "NoSuchKey: x"),
            "NoSuchKey: x"
        );
        assert_eq!(
            status_for_error(&copy_lookup_error(FsError::common("rpc"), "NoSuchKey: x")),
            500
        );
        assert_eq!(copy_source_range("bytes=0-4", 5), Ok((0, 5)));
        assert_eq!(copy_source_range("bytes=2-2", 5), Ok((2, 1)));
        assert!(copy_source_range("bytes=0-21", 5)
            .unwrap_err()
            .starts_with("InvalidRange:"));
        for r in [
            "0-2",
            "bytes=0",
            "bytes=a-b",
            "bytes=0-b",
            "bytes=0-",
            "bytes=0-2,3-4",
            "bytes=3-1",
        ] {
            assert!(
                copy_source_range(r, 5)
                    .unwrap_err()
                    .starts_with("InvalidArgument:"),
                "{r}"
            );
        }
    }
}
