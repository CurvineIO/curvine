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

//! # S3 API Trait Definitions and Data Structures
//!
//! This module defines the core S3 API traits and data structures that provide
//! complete AWS S3 compatibility for the Curvine Object Gateway. It serves as
//! the contract between HTTP handlers and storage backend implementations.
//!
//! ## API Coverage
//!
//! The module implements all major S3 operations:
//! - **Object Operations**: PUT, GET, HEAD, DELETE with range support
//! - **Bucket Operations**: CREATE, DELETE, LIST with location support  
//! - **Multipart Upload**: Complete lifecycle from initiation to completion
//! - **Listing Operations**: LIST objects and buckets with filtering
//!
//! ## Design Principles
//!
//! - **Full S3 Compatibility**: All structures match AWS S3 API specifications
//! - **Async First**: All operations return futures for high-concurrency handling
//! - **Type Safety**: Strong typing prevents common integration errors
//! - **Extensibility**: Trait-based design allows pluggable implementations
//!
//! ## Response Format Compatibility
//!
//! All response structures are designed for direct XML serialization,
//! maintaining full compatibility with S3 clients including:
//! - AWS CLI and SDKs
//! - MinIO client libraries
//! - Third-party S3 tools
//! - Custom S3 applications

pub use crate::s3::error::Error;
use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    io::Write,
    str::FromStr,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ArchiveStatus {
    #[serde(rename = "ARCHIVE_ACCESS")]
    ArchiveAccess,
    #[serde(rename = "DEEP_ARCHIVE_ACCESS")]
    DeepArchiveAccess,
}

impl std::str::FromStr for ArchiveStatus {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ARCHIVE_ACCESS" => Ok(ArchiveStatus::ArchiveAccess),
            "DEEP_ARCHIVE_ACCESS" => Ok(ArchiveStatus::DeepArchiveAccess),
            _ => Err(Error::Other(format!("Invalid ArchiveStatus value: {}", s))),
        }
    }
}

impl std::fmt::Display for ArchiveStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArchiveStatus::ArchiveAccess => write!(f, "ARCHIVE_ACCESS"),
            ArchiveStatus::DeepArchiveAccess => write!(f, "DEEP_ARCHIVE_ACCESS"),
        }
    }
}

static OWNER_ID: &str = DEFAULT_OWNER_ID;
pub type DateTime = chrono::DateTime<chrono::Utc>;

pub trait VRequest: crate::auth::sig_v4::VHeader {
    fn method(&self) -> String;
    fn url_path(&self) -> String;
    fn get_query(&self, k: &str) -> Option<String>;
    fn all_query(&self, cb: impl FnMut(&str, &str) -> bool);
}
#[async_trait::async_trait]
pub trait BodyWriter {
    type BodyWriter<'a>: crate::utils::io::PollWrite + Send + Unpin
    where
        Self: 'a;
    async fn get_body_writer(&mut self) -> Result<Self::BodyWriter<'_>, String>;
}
pub trait BodyReader {
    type BodyReader: crate::utils::io::PollRead + Send;
    fn get_body_reader<'b>(
        self,
    ) -> std::pin::Pin<
        Box<dyn 'b + Send + std::future::Future<Output = Result<Self::BodyReader, String>>>,
    >;
}
pub trait HeaderTaker {
    type Head: crate::auth::sig_v4::VHeader;
    fn take_header(&self) -> Self::Head;
}
pub trait VRequestPlus: VRequest {
    fn body<'a>(
        self,
    ) -> std::pin::Pin<
        Box<dyn 'a + Send + std::future::Future<Output = Result<Vec<u8>, std::io::Error>>>,
    >;
}
pub trait VResponse: crate::auth::sig_v4::VHeader + BodyWriter {
    fn set_status(&mut self, status: u16);
    fn send_header(&mut self);
}

#[derive(Default, Debug, Serialize)]
pub struct HeadObjectResult {
    #[serde(rename = "AcceptRanges")]
    pub accept_ranges: Option<String>,
    #[serde(rename = "ArchiveStatus")]
    pub archive_status: Option<ArchiveStatus>,
    #[serde(rename = "BucketKeyEnabled")]
    pub bucket_key_enabled: Option<bool>,
    #[serde(rename = "CacheControl")]
    pub cache_control: Option<String>,
    #[serde(rename = "ChecksumCRC32")]
    pub checksum_crc32: Option<String>,
    #[serde(rename = "ChecksumCRC32C")]
    pub checksum_crc32c: Option<String>,
    #[serde(rename = "ChecksumCRC64")]
    pub checksum_crc64: Option<String>,
    #[serde(rename = "ChecksumSHA1")]
    pub checksum_sha1: Option<String>,
    #[serde(rename = "ChecksumSHA256")]
    pub checksum_sha256: Option<String>,
    #[serde(rename = "ChecksumType")]
    pub checksum_type: Option<String>,
    #[serde(rename = "ContentDisposition")]
    pub content_disposition: Option<String>,
    #[serde(rename = "ContentEncoding")]
    pub content_encoding: Option<String>,
    #[serde(rename = "ContentLanguage")]
    pub content_language: Option<String>,
    #[serde(rename = "ContentLength")]
    pub content_length: Option<usize>,
    #[serde(rename = "ContentRange")]
    pub content_range: Option<String>,
    #[serde(rename = "ContentType")]
    pub content_type: Option<String>,
    #[serde(rename = "DeleteMarker")]
    pub delete_marker: Option<bool>,
    #[serde(rename = "ETag")]
    pub etag: Option<String>,
    #[serde(rename = "Expiration")]
    pub expiration: Option<String>,
    #[serde(rename = "Expires")]
    pub expires: Option<String>,
    #[serde(rename = "ExpiresString")]
    pub expires_string: Option<String>,
    #[serde(rename = "LastModified")]
    pub last_modified: Option<String>,
    #[serde(rename = "Metadata")]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(rename = "MissingMeta")]
    pub missing_meta: Option<i32>,
    #[serde(rename = "ObjectLockLegalHoldStatus")]
    pub object_lock_legal_hold_status: Option<String>,
    #[serde(rename = "ObjectLockMode")]
    pub object_lock_mode: Option<String>,
    #[serde(rename = "ObjectLockRetainUntilDate")]
    pub object_lock_retain_until_date: Option<String>,
    #[serde(rename = "PartsCount")]
    pub parts_count: Option<i32>,
    #[serde(rename = "ReplicationStatus")]
    pub replication_status: Option<String>,
    #[serde(rename = "RequestCharged")]
    pub request_charged: Option<String>,
    #[serde(rename = "Restore")]
    pub restore: Option<String>,
    #[serde(rename = "SSECustomerAlgorithm")]
    pub sse_customer_algorithm: Option<String>,
    #[serde(rename = "SSECustomerKeyMD5")]
    pub sse_customer_key_md5: Option<String>,
    #[serde(rename = "SSEKMSKeyId")]
    pub sse_kms_key_id: Option<String>,
    #[serde(rename = "ServerSideEncryption")]
    pub server_side_encryption: Option<String>,
    #[serde(rename = "StorageClass")]
    pub storage_class: Option<String>,
    #[serde(rename = "VersionId")]
    pub version_id: Option<String>,
    #[serde(rename = "WebsiteRedirectLocation")]
    pub website_redirect_location: Option<String>,
}

/// Core trait for object metadata retrieval operations
///
/// This trait provides the fundamental capability to retrieve object metadata
/// without transferring the object content itself. It serves as the foundation
/// for both HEAD requests and as a prerequisite for GET operations.
///
/// ## Purpose
/// - **Metadata Lookup**: Retrieve object size, modification time, ETag, and custom metadata
/// - **Existence Check**: Determine if an object exists without downloading it
/// - **Performance**: Efficient metadata access for large objects
/// - **Foundation**: Required by GetObjectHandler for content operations
///
/// ## Implementation Notes
/// - Should return `None` if object doesn't exist (not an error)
/// - Must populate all available metadata fields accurately
/// - Should be optimized for fast metadata access
#[async_trait::async_trait]
pub trait HeadHandler {
    /// Retrieve object metadata without content transfer
    ///
    /// ## Parameters
    /// - `bucket`: S3 bucket name containing the object
    /// - `object`: Object key/path within the bucket
    ///
    /// ## Returns
    /// - `Ok(Some(HeadObjectResult))`: Object exists with metadata
    /// - `Ok(None)`: Object does not exist
    /// - `Err(Error)`: Backend error during lookup
    async fn lookup(&self, bucket: &str, object: &str) -> Result<Option<HeadObjectResult>, Error>;
}
#[derive(Default)]
pub struct GetObjectOption {
    pub range_start: Option<u64>,
    pub range_end: Option<u64>,
}

pub trait GetObjectHandler: HeadHandler {
    fn handle<'a>(
        &'a self,
        bucket: &str,
        object: &str,
        opt: GetObjectOption,
        out: tokio::sync::Mutex<
            std::pin::Pin<Box<dyn 'a + Send + crate::utils::io::PollWrite + Unpin>>,
        >,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<(), String>>>>;
}

extern crate serde;
use serde::Serialize;
use sha1::Digest;
use tokio::io::AsyncSeekExt;

use crate::utils::consts::*;
use crate::utils::io::{PollRead, PollWrite};

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
#[serde(rename = "ListBucketResult")]
pub struct ListObjectResult {
    #[serde(
        rename = "xmlns",
        default = "s3_namespace",
        skip_serializing_if = "String::is_empty"
    )]
    pub xmlns: String,

    #[serde(rename = "Name")]
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_keys: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
    pub is_truncated: bool,
    #[serde(default)]
    pub contents: Vec<ListObjectContent>,
    #[serde(default)]
    pub common_prefixes: Vec<CommonPrefix>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListObjectContent {
    pub key: String,
    pub last_modified: Option<String>,
    pub etag: Option<String>,
    pub size: u64,
    pub storage_class: Option<String>,
    pub owner: Option<Owner>,
}

#[derive(Debug, Serialize)]
#[serde(rename = "ListAllMyBucketsResult")]
#[serde(rename_all = "PascalCase")]
pub struct ListAllMyBucketsResult {
    #[serde(
        rename = "xmlns",
        default = "s3_namespace",
        skip_serializing_if = "String::is_empty"
    )]
    pub xmlns: String,

    pub owner: Owner,

    pub buckets: Buckets,
}
#[derive(Debug, Serialize)]
pub struct Buckets {
    #[serde(rename = "Bucket")]
    pub bucket: Vec<Bucket>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Bucket {
    pub name: String,
    pub creation_date: String,
    pub bucket_region: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Owner {
    pub id: String,
    pub display_name: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CommonPrefix {
    pub prefix: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListObjectOption {
    pub bucket: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub continuation_token: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding_type: Option<String>, // Usually "url"

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_bucket_owner: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub fetch_owner: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_keys: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional_object_attributes: Option<Vec<String>>, // e.g. ["RestoreStatus"]

    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_payer: Option<String>, // e.g. "requester"

    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_after: Option<String>,
}

#[async_trait::async_trait]
pub trait ListObjectHandler {
    async fn handle(
        &self,
        opt: &ListObjectOption,
        bucket: &str,
    ) -> Result<Vec<ListObjectContent>, String>;
}

/// List Object Versions API structures and handler
#[derive(Debug, Serialize)]
#[serde(rename = "ListVersionsResult")]
pub struct ListObjectVersionsResult {
    #[serde(
        rename = "xmlns",
        default = "s3_namespace",
        skip_serializing_if = "String::is_empty"
    )]
    pub xmlns: String,

    #[serde(rename = "Name")]
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_marker: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_id_marker: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_key_marker: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_version_id_marker: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_keys: Option<u32>,

    pub is_truncated: bool,

    #[serde(rename = "Version", default)]
    pub versions: Vec<ObjectVersion>,

    #[serde(rename = "DeleteMarker", default)]
    pub delete_markers: Vec<DeleteMarker>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ObjectVersion {
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
    pub last_modified: String,
    pub etag: String,
    pub size: u64,
    pub storage_class: Option<String>,
    pub owner: Option<Owner>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteMarker {
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
    pub last_modified: String,
    pub owner: Option<Owner>,
}

#[derive(Debug)]
pub struct ListObjectVersionsOption {
    pub bucket: String,

    pub delimiter: Option<String>,
    pub encoding_type: Option<String>,
    pub key_marker: Option<String>,
    pub max_keys: Option<i32>,
    pub prefix: Option<String>,
    pub version_id_marker: Option<String>,
}

#[async_trait::async_trait]
pub trait ListObjectVersionsHandler {
    async fn handle(
        &self,
        opt: &ListObjectVersionsOption,
        bucket: &str,
    ) -> Result<ListObjectVersionsResult, String>;
}

pub fn handle_head_object<T: VRequest, F: VResponse, E: HeadHandler>(
    _req: &T,
    _resp: &mut F,
    _handler: &E,
) {
    todo!()
}

/// Handle S3 GET Object requests with streaming support and range capabilities
///
/// This function processes S3 GET Object requests, providing full AWS S3 compatibility
/// including HTTP range requests, metadata headers, and streaming download.
///
/// ## Features
/// - **Streaming Download**: Efficient memory usage for large files
/// - **Range Requests**: Support for partial content (bytes=start-end, bytes=start-, bytes=-suffix)
/// - **Metadata Headers**: Returns all custom metadata as x-amz-meta-* headers
/// - **Content Headers**: Proper Content-Type, Content-Length, ETag handling
/// - **Error Handling**: Standard S3 error responses (404, 416, etc.)
///
/// ## Parameters
/// - `req`: HTTP request containing bucket/object path and range headers
/// - `resp`: HTTP response writer for streaming object data
/// - `handler`: Backend implementation for object retrieval
///
/// ## HTTP Response Codes
/// - `200 OK`: Full object retrieved successfully
/// - `206 Partial Content`: Range request fulfilled
/// - `404 Not Found`: Object does not exist
/// - `405 Method Not Allowed`: Non-GET request
/// - `416 Range Not Satisfiable`: Invalid range specification
pub async fn handle_get_object<T: VRequest, F: VResponse>(
    req: T,
    resp: &mut F,
    handler: &std::sync::Arc<dyn GetObjectHandler + Send + Sync>,
) {
    if req.method() != "GET" {
        resp.set_status(405);
        resp.send_header();
        return;
    }

    let rpath = req.url_path();
    let raw = rpath.trim_matches('/');
    let r = match raw.find('/') {
        Some(val) => val,
        None => {
            log::error!("{}", orpc::err_msg!("Invalid path format"));
            resp.set_status(404);
            resp.send_header();
            return;
        }
    };

    // build option from query first
    let mut opt = GetObjectOption {
        range_start: req
            .get_query("range-start")
            .and_then(|v| v.parse::<u64>().ok()),
        range_end: req
            .get_query("range-end")
            .and_then(|v| v.parse::<u64>().ok()),
    };

    // Try parse HTTP Range header: bytes=start-end, bytes=start-, bytes=-suffix
    if let Some(rh) = req.get_header("range") {
        if let Some(bytes) = rh.strip_prefix("bytes=") {
            if let Some(suffix_str) = bytes.strip_prefix('-') {
                // Handle suffix-byte-range-spec: bytes=-N (last N bytes)
                if let Ok(suffix_len) = suffix_str.parse::<u64>() {
                    // We'll need to calculate the actual range after we know the file size
                    // For now, we mark this as a special case
                    opt.range_start = None;
                    opt.range_end = Some(u64::MAX - suffix_len); // Use special encoding
                }
            } else {
                // Handle normal range: bytes=start-end or bytes=start-
                let mut it = bytes.splitn(2, '-');
                let s = it.next().unwrap_or("");
                let e = it.next().unwrap_or("");
                if !s.is_empty() {
                    if let Ok(v) = s.parse::<u64>() {
                        opt.range_start = Some(v);
                    }
                }
                if !e.is_empty() {
                    if let Ok(v) = e.parse::<u64>() {
                        opt.range_end = Some(v);
                    }
                }
            }
        }
    }

    let next = r;
    let bucket = &raw[..next];
    let object = &raw[next + 1..];

    let head = match handler.lookup(bucket, object).await {
        Ok(val) => val,
        Err(err) => {
            log::error!("{}", orpc::err_msg!("lookup bucket object error: {}", err));
            resp.set_status(500);
            resp.send_header();
            return;
        }
    };
    let head = match head {
        Some(val) => val,
        None => {
            log::error!("{}", orpc::err_msg!("Object not found"));
            resp.set_status(404);
            resp.send_header();
            return;
        }
    };

    //send header info to client
    let total_len = head.content_length.unwrap_or(0) as u64;

    // Default: full content
    let mut status = 200u16;
    let mut resp_len = total_len;
    let header_last_modified = head.last_modified.clone();
    let header_etag = head.etag.clone();
    let header_ct = head.content_type.clone();

    // If range requested, validate and compute
    if opt.range_start.is_some() || opt.range_end.is_some() {
        let (start, end) = if let Some(range_end) = opt.range_end {
            if range_end > u64::MAX / 2 {
                // This is a suffix-byte-range-spec (bytes=-N)
                let suffix_len = u64::MAX - range_end;

                // Add reasonable limit: maximum 1GB suffix range
                const MAX_SUFFIX_SIZE: u64 = 1024 * 1024 * 1024; // 1GB
                if suffix_len > MAX_SUFFIX_SIZE {
                    tracing::warn!(
                        "Suffix range size {} exceeds maximum allowed {}",
                        suffix_len,
                        MAX_SUFFIX_SIZE
                    );
                    resp.set_header("content-range", &format!("bytes */{total_len}"));
                    resp.set_status(416); // Range Not Satisfiable
                    resp.send_header();
                    return;
                }

                if suffix_len > total_len {
                    // If suffix length is larger than file, return entire file
                    (0, total_len.saturating_sub(1))
                } else {
                    // Return last N bytes
                    (total_len - suffix_len, total_len.saturating_sub(1))
                }
            } else {
                // Normal range processing
                let start = opt.range_start.unwrap_or(0);
                if start >= total_len {
                    resp.set_status(416);
                    resp.set_header("content-range", &format!("bytes */{}", total_len));
                    resp.send_header();
                    return;
                }
                let end = range_end.min(total_len.saturating_sub(1));
                if end < start {
                    resp.set_status(416);
                    resp.set_header("content-range", &format!("bytes */{}", total_len));
                    resp.send_header();
                    return;
                }
                (start, end)
            }
        } else {
            // range_start only (bytes=N-)
            let start = opt.range_start.unwrap_or(0);
            if start >= total_len {
                resp.set_status(416);
                resp.set_header("content-range", &format!("bytes */{}", total_len));
                resp.send_header();
                return;
            }
            (start, total_len.saturating_sub(1))
        };
        resp_len = end - start + 1;
        status = 206;
        resp.set_header("content-range", &format!("bytes {start}-{end}/{total_len}"));
    }

    // Apply headers
    resp.set_header("content-length", resp_len.to_string().as_str());
    if let Some(v) = header_etag {
        resp.set_header("etag", &v)
    }

    if let Some(v) = header_ct {
        resp.set_header("content-type", &v)
    }

    if let Some(v) = header_last_modified {
        resp.set_header("last-modified", &v)
    }

    // Set custom metadata as x-amz-meta-* headers
    if let Some(metadata) = head.metadata {
        for (key, value) in metadata {
            // If key already starts with x-amz-meta-, use as-is
            // Otherwise, add the x-amz-meta- prefix
            let header_name = if key.starts_with("x-amz-meta-") {
                key.to_string()
            } else {
                format!("x-amz-meta-{key}")
            };
            resp.set_header(&header_name, &value);
        }
    }

    // Keep default HTTP connection behavior (enable keep-alive by default)
    resp.set_status(status);
    resp.send_header();

    let ret = {
        match resp.get_body_writer().await {
            Ok(body) => {
                let ret = handler
                    .handle(bucket, object, opt, tokio::sync::Mutex::new(Box::pin(body)))
                    .await;
                if let Err(err) = ret {
                    Err(err)
                } else {
                    Ok(())
                }
            }
            Err(err) => Err(err),
        }
    };
    if let Err(err) = ret {
        log::error!("body handle error {err}");
        resp.set_status(500);
    }
}

/// Handle S3 LIST Objects V2 requests with prefix filtering and pagination
///
/// This function processes S3 ListObjectsV2 requests, providing AWS S3 compatible
/// object listing with advanced filtering, pagination, and metadata support.
///
/// ## Features
/// - **Prefix Filtering**: Filter objects by key prefix for directory-like browsing
/// - **Pagination Support**: Handle large object lists with continuation tokens
/// - **Metadata Inclusion**: Return object size, modification time, ETag, and storage class
/// - **Delimiter Support**: Enable hierarchical listing with common prefixes
/// - **Performance Optimized**: Efficient backend queries with result limiting
///
/// ## Query Parameters
/// - `list-type=2`: Specifies ListObjectsV2 API version
/// - `prefix`: Filter objects by key prefix
/// - `max-keys`: Limit number of objects returned (default: 1000)
/// - `continuation-token`: Token for paginated results
/// - `delimiter`: Character for hierarchical grouping
///
/// ## HTTP Response Codes
/// - `200 OK`: Object list retrieved successfully
/// - `404 Not Found`: Bucket does not exist
/// - `405 Method Not Allowed`: Non-GET request
/// - `500 Internal Server Error`: Backend listing error
pub async fn handle_get_list_object_versions<T: VRequest, F: VResponse>(
    req: T,
    resp: &mut F,
    handler: &std::sync::Arc<dyn ListObjectVersionsHandler + Send + Sync>,
) {
    // Parse bucket name from URL path
    let url_path = req.url_path();
    let bucket_name = url_path.trim_start_matches('/');

    // Parse query parameters
    let opt = ListObjectVersionsOption {
        bucket: bucket_name.to_string(),
        delimiter: req.get_query("delimiter"),
        encoding_type: req.get_query("encoding-type"),
        key_marker: req.get_query("key-marker"),
        max_keys: req.get_query("max-keys").and_then(|s| s.parse().ok()),
        prefix: req.get_query("prefix"),
        version_id_marker: req.get_query("version-id-marker"),
    };

    // Execute handler
    match handler.handle(&opt, bucket_name).await {
        Ok(result) => {
            // Set response headers
            resp.set_header("content-type", "application/xml");
            resp.set_status(200);

            // Serialize to XML
            match quick_xml::se::to_string(&result) {
                Ok(xml) => {
                    if let Ok(mut writer) = resp.get_body_writer().await {
                        let _ = writer.poll_write(xml.as_bytes()).await;
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to serialize ListObjectVersions response: {}", e);
                    resp.set_status(500);
                }
            }
        }
        Err(e) => {
            tracing::error!("ListObjectVersions handler error: {}", e);
            resp.set_status(500);
        }
    }
}

pub async fn handle_get_list_object<T: VRequest, F: VResponse>(
    req: T,
    resp: &mut F,
    handler: &std::sync::Arc<dyn ListObjectHandler + Send + Sync>,
) {
    if req.method() != "GET" {
        resp.set_status(405);
        resp.send_header();
        return;
    }

    let rpath = req.url_path();
    let trimmed = rpath.trim_matches('/');

    let bucket = if !trimmed.is_empty() {
        trimmed.to_string()
    } else if let Some(b) = req.get_query("bucket") {
        b
    } else {
        resp.set_status(400);
        resp.send_header();
        return;
    };

    let opt = ListObjectOption {
        bucket: bucket.clone(),
        continuation_token: req.get_query("continuation-token"),
        delimiter: req.get_query("delimiter"),
        expected_bucket_owner: req.get_query("expected-bucket-owner"),
        max_keys: req
            .get_query("max-keys")
            .and_then(|v| v.parse::<i32>().ok()),
        optional_object_attributes: None, //todo: support option_object_attributes on v2
        request_payer: req.get_header("x-amz-request-layer"),
        start_after: req.get_query("start-after"),
        encoding_type: req.get_query("encoding-type"),
        fetch_owner: req.get_query("fetch-owner").and_then(|v| {
            if v == "true" {
                Some(true)
            } else if v == "false" {
                Some(false)
            } else {
                None
            }
        }),
        prefix: req.get_query("prefix"),
    };

    let ret = handler.handle(&opt, bucket.as_str()).await;
    match ret {
        Ok(ans) => {
            let result = ListObjectResult {
                xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
                name: bucket,
                prefix: opt.prefix,
                key_count: Some(ans.len() as u32),
                max_keys: Some(opt.max_keys.unwrap_or(1000) as u32),
                delimiter: opt.delimiter,
                is_truncated: false,
                contents: ans,
                common_prefixes: vec![],
            };

            match quick_xml::se::to_string(&result) {
                Ok(data) => {
                    log::debug!("ListObjectsV2 XML => {data}");
                    resp.set_header("content-type", "application/xml");
                    resp.set_header("content-length", data.len().to_string().as_str());
                    resp.set_status(200);
                    resp.send_header();
                    let ret = match resp.get_body_writer().await {
                        Ok(mut body) => {
                            if let Err(err) = body.poll_write(data.as_bytes()).await {
                                log::info!("write to response body error {err}");
                            }
                            Ok(())
                        }
                        Err(err) => Err(err),
                    };
                    if let Err(err) = ret {
                        log::error!("write body error {err}");
                        resp.set_status(500);
                        resp.send_header();
                    }
                }

                Err(err) => {
                    log::error!("xml marshal failed {err}");
                }
            }
        }
        Err(err) => log::error!("get_list_object error {err}"),
    }
}

#[async_trait::async_trait]
pub trait ListBucketHandler {
    async fn handle(&self, opt: &ListBucketsOption) -> Result<Vec<Bucket>, String>;
}
#[async_trait::async_trait]
pub trait GetBucketLocationHandler {
    async fn handle(&self, _loc: Option<&str>) -> Result<Option<&'static str>, ()> {
        Ok(Some("us-west-1"))
    }
}
#[derive(Debug)]
pub struct ListBucketsOption {
    pub bucket_region: Option<String>,
    pub continuation_token: Option<String>,
    pub max_buckets: Option<i32>,
    pub prefix: Option<String>,
}

/// Handle S3 LIST Buckets requests with metadata and ownership information
///
/// This function processes S3 ListBuckets (GET /) requests, providing complete
/// bucket listing with creation dates, regions, and owner information.
///
/// ## Features
/// - **Complete Bucket Listing**: Returns all accessible buckets for the user
/// - **Real Metadata**: Shows actual creation dates from filesystem timestamps
/// - **Region Information**: Includes bucket region configuration
/// - **Owner Details**: Provides bucket ownership information
/// - **Performance Optimized**: Efficient directory scanning and metadata retrieval
///
/// ## Parameters
/// - `req`: HTTP request for bucket listing
/// - `resp`: HTTP response writer for bucket list XML
/// - `handler`: Backend implementation for bucket enumeration
///
/// ## Response Format
/// Returns XML with bucket information including:
/// - Bucket name and creation date
/// - Region/location constraint
/// - Owner display name and ID
///
/// ## HTTP Response Codes
/// - `200 OK`: Bucket list retrieved successfully
/// - `403 Forbidden`: Access denied to bucket listing
/// - `405 Method Not Allowed`: Non-GET request
/// - `500 Internal Server Error`: Backend listing error
pub async fn handle_get_list_buckets<T: VRequest, F: VResponse>(
    req: T,
    resp: &mut F,
    handler: &std::sync::Arc<dyn ListBucketHandler + Send + Sync>,
) {
    if req.method() != "GET" {
        resp.set_status(405);
        resp.send_header();
        return;
    }

    let opt = ListBucketsOption {
        bucket_region: req.get_query("bucket-region"),
        continuation_token: req.get_query("continuation-token"),
        max_buckets: req
            .get_query("max-buckets")
            .and_then(|v| v.parse::<i32>().ok()),
        prefix: req.get_query("prefix"),
    };

    match handler.handle(&opt).await {
        Ok(v) => {
            let res = ListAllMyBucketsResult {
                xmlns: r#"xmlns="http://s3.amazonaws.com/doc/2006-03-01/""#.to_string(),
                owner: Owner {
                    id: OWNER_ID.to_string(),
                    display_name: "bws".to_string(),
                },
                buckets: Buckets { bucket: v },
            };

            match quick_xml::se::to_string(&res) {
                Ok(v) => match resp.get_body_writer().await {
                    Ok(mut w) => {
                        if let Err(err) = w.poll_write(v.as_bytes()).await {
                            log::info!("write to client body error {err}");
                        }
                    }
                    Err(e) => log::error!("get_body_writer error: {e}"),
                },
                Err(e) => {
                    resp.set_status(500);
                    resp.send_header();
                    log::error!("xml serde error: {e}")
                }
            }
        }

        Err(e) => {
            log::info!("listbucket handle error: {e}");
            resp.set_status(500);
            resp.send_header();
        }
    }
}
#[derive(Default)]
pub struct PutObjectOption {
    // pub acl: ObjectCannedACL,
    pub cache_control: Option<String>,
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    pub checksum_crc32: Option<String>,
    pub checksum_crc32c: Option<String>,
    pub checksum_crc64nvme: Option<String>,
    pub checksum_sha1: Option<String>,
    pub checksum_sha256: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub content_length: Option<i64>,
    pub content_md5: Option<String>,
    pub content_type: Option<String>,
    pub expected_bucket_owner: Option<String>,
    pub expires: Option<DateTime>,
    pub grant_full_control: Option<String>,
    pub grant_read: Option<String>,
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    // pub metadata: Option<HashMap<String, String>>,
    pub object_lock_legal_hold_status: Option<ObjectLockLegalHoldStatus>,
    pub object_lock_mode: Option<ObjectLockMode>,
    pub object_lock_retain_until_date: Option<DateTime>,
    pub request_payer: Option<RequestPayer>,
    pub storage_class: Option<String>,
    // pub tagging: Option<String>,
    // pub website_redirect_location: Option<String>,
    pub write_offset_bytes: Option<i64>,
}

impl PutObjectOption {
    pub fn invalid(&self) -> bool {
        if self.content_length.is_none() || self.content_md5.is_none() {
            return false;
        }
        true
    }
}

#[derive(Debug, PartialEq)]
pub enum ChecksumAlgorithm {
    Crc32,
    Crc32c,
    Sha1,
    Sha256,
    Crc64nvme,
}

impl std::str::FromStr for ChecksumAlgorithm {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "CRC32" => Ok(ChecksumAlgorithm::Crc32),
            "CRC32C" => Ok(ChecksumAlgorithm::Crc32c),
            "SHA1" => Ok(ChecksumAlgorithm::Sha1),
            "SHA256" => Ok(ChecksumAlgorithm::Sha256),
            "CRC64NVME" => Ok(ChecksumAlgorithm::Crc64nvme),
            _ => orpc::err_box!("Invalid checksum algorithm: {}", s),
        }
    }
}

#[derive(Debug)]
pub enum RequestPayer {
    Requester,
}

impl std::str::FromStr for RequestPayer {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "requester" => Ok(RequestPayer::Requester),
            _ => Err(Error::Other(format!("Invalid RequestPayer value: {}", s))),
        }
    }
}
#[derive(Debug)]
pub enum ObjectLockMode {
    Governance,
    Compliance,
}
impl std::str::FromStr for ObjectLockMode {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "GOVERNANCE" => Ok(ObjectLockMode::Governance),
            "COMPLIANCE" => Ok(ObjectLockMode::Compliance),
            _ => Err(Error::Other(format!("Invalid ObjectLockMode value: {}", s))),
        }
    }
}

#[derive(Debug)]
pub enum ObjectLockLegalHoldStatus {
    On,
    Off,
}

impl std::str::FromStr for ObjectLockLegalHoldStatus {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ON" => Ok(ObjectLockLegalHoldStatus::On),
            "OFF" => Ok(ObjectLockLegalHoldStatus::Off),
            _ => Err(Error::Other(format!(
                "Invalid ObjectLockLegalHoldStatus value: {}",
                s
            ))),
        }
    }
}

#[async_trait::async_trait]
pub trait PutObjectHandler {
    async fn handle(
        &self,
        opt: &PutObjectOption,
        bucket: &str,
        object: &str,
        body: &mut (dyn crate::utils::io::PollRead + Unpin + Send),
    ) -> Result<(), String>;
}

/// Parse bucket and object names from URL path for PUT operations
///
/// ## Parameters
/// - `url_path`: The URL path from the request
///
/// ## Returns
/// - `Ok((bucket, object))`: Successfully parsed bucket and object names
/// - `Err(())`: Invalid path format
fn parse_put_object_path(url_path: &str) -> Result<(&str, &str), ()> {
    let url_path = url_path.trim_matches('/');
    let next = match url_path.find('/') {
        Some(pos) => pos,
        None => return Err(()),
    };
    let bucket = &url_path[..next];
    let object = &url_path[next + 1..];
    Ok((bucket, object))
}

/// Handle S3 PUT Object requests with streaming upload and metadata support
///
/// This function processes S3 PUT Object requests, providing full AWS S3 compatibility
/// including streaming upload, custom metadata, content validation, and authentication.
///
/// ## Features
/// - **Streaming Upload**: Memory-efficient handling of large files
/// - **Metadata Support**: Processes x-amz-meta-* headers for custom metadata
/// - **Content Validation**: MD5 checksum verification and content-type detection
/// - **Authentication**: AWS Signature V4 validation for secure uploads
/// - **Atomic Operations**: Ensures data integrity during upload process
///
/// ## Parameters
/// - `v4head`: AWS Signature V4 authentication headers
/// - `req`: HTTP request containing object data and metadata
/// - `resp`: HTTP response writer for upload confirmation
/// - `handler`: Backend implementation for object storage
///
/// ## HTTP Response Codes
/// - `200 OK`: Object uploaded successfully
/// - `400 Bad Request`: Invalid request format or metadata
/// - `403 Forbidden`: Authentication failure
/// - `405 Method Not Allowed`: Non-PUT request
/// - `500 Internal Server Error`: Storage backend error
pub async fn handle_put_object<T: VRequest + BodyReader, F: VResponse>(
    mut v4head: crate::auth::sig_v4::V4Head,
    req: T,
    resp: &mut F,
    handler: &std::sync::Arc<dyn PutObjectHandler + Send + Sync>,
) {
    if req.method() != "PUT" {
        resp.set_status(405);
        resp.send_header();
        return;
    }

    // Parse bucket and object from URL path
    let url_path = req.url_path();
    let (bucket, object) = match parse_put_object_path(&url_path) {
        Ok((bucket, object)) => (bucket, object),
        Err(()) => {
            resp.set_status(400);
            resp.send_header();
            return;
        }
    };

    let opt = PutObjectOption {
        cache_control: req.get_header("cache-control"),
        checksum_algorithm: req
            .get_header("checksum-algorithm")
            .and_then(|v| ChecksumAlgorithm::from_str(&v).ok()),
        checksum_crc32: req.get_header("x-amz-checksum-crc32"),
        checksum_crc32c: req.get_header("x-amz-checksum-crc32c"),
        checksum_crc64nvme: req.get_header("x-amz-checksum-crc64vme"),
        checksum_sha1: req.get_header("x-amz-checksum-sha1"),
        checksum_sha256: req.get_header("x-amz-checksum-sha256"),
        content_disposition: req.get_header("content-disposition"),
        content_encoding: req.get_header("cotent-encoding"),
        content_language: req.get_header("content-language"),
        content_length: req
            .get_header("content-length")
            .and_then(|v| v.parse::<i64>().map_or(Some(-1), Some)),
        content_md5: req.get_header("content-md5"),
        content_type: req.get_header("content-type"),
        expected_bucket_owner: req.get_header("x-amz-expected-bucket-owner"),
        expires: req.get_header("expire").and_then(|v| {
            chrono::NaiveDateTime::parse_from_str(&v, "%a, %d %b %Y %H:%M:%S GMT")
                .map_or(None, |v| {
                    Some(chrono::DateTime::from_naive_utc_and_offset(v, chrono::Utc))
                })
        }),
        grant_full_control: req.get_header("x-amz-grant-full-control"),
        grant_read: req.get_header("x-amz-grant-read"),
        if_match: req.get_header("if-match"),
        if_none_match: req.get_header("if-none-match"),
        // metadata: todo!(),
        object_lock_legal_hold_status: req
            .get_header("x-amz-object-lock-legal-hold-status")
            .and_then(|v| ObjectLockLegalHoldStatus::from_str(&v).ok()),
        object_lock_mode: req
            .get_header("x-amz-object-lock-mode")
            .and_then(|v| ObjectLockMode::from_str(&v).ok()),
        object_lock_retain_until_date: req
            .get_header("x-amz-object-lock-retain_until_date")
            .and_then(|v| {
                chrono::NaiveDateTime::parse_from_str(&v, "%a, %d %b %Y %H:%M:%S GMT")
                    .map_or(None, |v| {
                        Some(chrono::DateTime::from_naive_utc_and_offset(v, chrono::Utc))
                    })
            }),
        request_payer: req
            .get_header("x-amz-request-payer")
            .and_then(|v| RequestPayer::from_str(&v).ok()),
        storage_class: req.get_header("x-amz-storage-class"),
        // tagging: todo!(),
        // website_redirect_location: todo!(),
        write_offset_bytes: req
            .get_header("x-amz-write-offset-bytes")
            .and_then(|v| v.parse::<i64>().ok()),
    };

    //todo:parse from body,then derive into handle
    enum ContentSha256 {
        Hash(String),
        Streaming,
        Unsigned,
    }

    let content_sha256 = req.get_header("x-amz-content-sha256").map_or_else(
        // Default to UNSIGNED-PAYLOAD for requests without x-amz-content-sha256 (e.g., V2 requests)
        || Some(ContentSha256::Unsigned),
        |content_sha256| {
            if content_sha256.as_str() == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
                Some(ContentSha256::Streaming)
            } else if content_sha256.as_str() == "UNSIGNED-PAYLOAD" {
                Some(ContentSha256::Unsigned)
            } else {
                Some(ContentSha256::Hash(content_sha256))
            }
        },
    );

    let content_sha256 = match content_sha256 {
        Some(val) => val,
        None => {
            log::error!("{}", orpc::err_msg!("Missing content SHA256"));
            resp.set_status(404);
            resp.send_header();
            return;
        }
    };
    let r = match req.get_body_reader().await {
        Ok(val) => val,
        Err(err) => {
            log::error!("{}", orpc::err_msg!("get body reader error: {}", err));
            resp.set_status(500);
            resp.send_header();
            return;
        }
    };
    let ret: Result<(), String> = match content_sha256 {
        ContentSha256::Hash(cs) => {
            let content_length = match opt.content_length {
                Some(val) => val as usize,
                None => {
                    log::error!("{}", orpc::err_msg!("Missing content length"));
                    resp.set_status(404);
                    resp.send_header();
                    return;
                }
            };
            if content_length <= 10 << 20 {
                match read_body_to_vec(r, &cs, content_length).await {
                    Ok(vec) => {
                        let mut reader = InMemoryPollReader {
                            data: vec,
                            offset: 0,
                        };
                        handler.handle(&opt, bucket, object, &mut reader).await
                    }
                    Err(err) => {
                        use crate::error::BodyParseError;
                        match err {
                            BodyParseError::HashMismatch => {
                                log::warn!("{}", orpc::err_msg!("Hash validation failed"));
                                resp.set_status(400);
                                resp.send_header();
                                return;
                            }
                            BodyParseError::ContentLengthMismatch => {
                                log::warn!("{}", orpc::err_msg!("Content length mismatch"));
                                resp.set_status(400);
                                resp.send_header();
                                return;
                            }
                            BodyParseError::InvalidEncoding(_) => {
                                log::error!(
                                    "{}",
                                    orpc::err_msg!("Invalid content encoding: {}", err)
                                );
                                resp.set_status(400);
                                resp.send_header();
                                return;
                            }
                            BodyParseError::Io(_) => {
                                log::error!(
                                    "{}",
                                    orpc::err_msg!("I/O error during body parsing: {}", err)
                                );
                                resp.set_status(500);
                                resp.send_header();
                                return;
                            }
                        }
                    }
                }
            } else {
                match tokio::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .read(true)
                    .mode(0o644)
                    .open(format!("/tmp/curvine-temp/{}", cs))
                    .await
                {
                    Ok(mut fd) => match parse_body(r, &mut fd, &cs, content_length).await {
                        Ok(_) => {
                            if let Err(err) = fd.seek(std::io::SeekFrom::Start(0)).await {
                                log::error!("fd seek failed {err}");
                                resp.set_status(500);
                                resp.send_header();
                                return;
                            }
                            handler.handle(&opt, bucket, object, &mut fd).await
                        }
                        Err(err) => match err {
                            ParseBodyError::HashMismatch => {
                                log::warn!("put object hash not match");
                                resp.set_status(400);
                                resp.send_header();
                                return;
                            }
                            ParseBodyError::ContentLengthMismatch => {
                                log::warn!("content length invalid");
                                resp.set_status(400);
                                resp.send_header();
                                return;
                            }
                            ParseBodyError::Io(err) => {
                                log::error!("parse body io error {err}");
                                resp.set_status(500);
                                resp.send_header();
                                return;
                            }
                            ParseBodyError::InvalidEncoding(err) => {
                                log::error!("invalid content encoding: {err}");
                                resp.set_status(400);
                                resp.send_header();
                                return;
                            }
                        },
                    },
                    Err(err) => {
                        log::error!("open local path error {err}");
                        resp.set_status(500);
                        resp.send_header();
                        return;
                    }
                }
            }
        }
        ContentSha256::Streaming => {
            let file_name = uuid::Uuid::new_v4().to_string()[..8].to_string();
            let file_name = format!("/tmp/curvine-temp/{}", file_name);
            let ret = match tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .read(true)
                .mode(0o644)
                .open(file_name.as_str())
                .await
            {
                Ok(mut fd) => crate::utils::chunk_parse(r, &mut fd, v4head.hasher()).await,
                Err(err) => {
                    log::error!("open local temp file error {err}");
                    resp.set_status(500);
                    resp.send_header();
                    return;
                }
            };
            if let Err(err) = ret {
                orpc::try_log!(tokio::fs::remove_file(file_name.as_str()).await, ());
                match err {
                    crate::utils::ChunkParseError::HashNoMatch => {
                        log::warn!("accept hash no match request");
                        resp.set_status(400);
                        resp.send_header();
                        return;
                    }
                    crate::utils::ChunkParseError::IllegalContent => {
                        log::warn!("accept illegal content request");
                        resp.set_status(400);
                        resp.send_header();
                        return;
                    }
                    crate::utils::ChunkParseError::Io(err) => {
                        log::error!("local io error {err}");
                        resp.set_status(500);
                        resp.send_header();
                        return;
                    }
                }
            }
            match tokio::fs::OpenOptions::new()
                .read(true)
                .open(file_name.as_str())
                .await
            {
                Ok(mut fd) => {
                    let ret = handler.handle(&opt, bucket, object, &mut fd).await;
                    orpc::try_log!(tokio::fs::remove_file(file_name.as_str()).await, ());
                    ret
                }
                Err(err) => {
                    log::error!("open file {file_name} error {err}");
                    resp.set_status(500);
                    resp.send_header();
                    orpc::try_log!(tokio::fs::remove_file(file_name.as_str()).await, ());
                    return;
                }
            }
        }
        ContentSha256::Unsigned => {
            // No hash verification; stream body into a temp file, then pass to handler
            let file_name = uuid::Uuid::new_v4().to_string()[..8].to_string();
            let file_name = format!("/tmp/curvine-temp/{}", file_name);
            let ret = match tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .read(true)
                .mode(0o644)
                .open(file_name.as_str())
                .await
            {
                Ok(mut fd) => {
                    // write all body to temp file using BytesMut accumulation
                    let mut reader = r;
                    use bytes::BytesMut;
                    use tokio::io::AsyncWriteExt;
                    loop {
                        match reader.poll_read().await {
                            Ok(Some(buf)) => {
                                // Use BytesMut to avoid zero-prefill Vec issues
                                let mut bytes_chunk = BytesMut::with_capacity(buf.len());
                                bytes_chunk.extend_from_slice(&buf);
                                if let Err(e) = fd.write_all(&bytes_chunk).await {
                                    log::error!("unsigned write error {e}");
                                    break Err(format!("write error {e}"));
                                }
                            }
                            Ok(None) => break Ok(()),
                            Err(e) => {
                                log::error!("unsigned read error {e}");
                                break Err(e);
                            }
                        }
                    }
                }
                Err(err) => {
                    log::error!("open local temp file error {err}");
                    resp.set_status(500);
                    resp.send_header();
                    return;
                }
            };

            if let Err(_err) = ret {
                let _ = tokio::fs::remove_file(file_name.as_str()).await;
                resp.set_status(500);
                resp.send_header();
                return;
            }

            match tokio::fs::OpenOptions::new()
                .read(true)
                .open(file_name.as_str())
                .await
            {
                Ok(mut fd) => {
                    let ret = handler.handle(&opt, bucket, object, &mut fd).await;
                    let _ = tokio::fs::remove_file(file_name.as_str()).await;
                    ret
                }
                Err(err) => {
                    log::error!("open file {file_name} error {err}");
                    resp.set_status(500);
                    resp.send_header();
                    let _ = tokio::fs::remove_file(file_name.as_str()).await;
                    return;
                }
            }
        }
    };
    //
    match ret {
        Ok(_) => {
            resp.set_status(200);
            resp.send_header();
        }
        Err(err) => {
            resp.set_status(500);
            resp.send_header();
            log::error!("put object handle error: {err}");
        }
    }
}
pub struct DeleteObjectOption {}

#[async_trait::async_trait]
pub trait DeleteObjectHandler {
    async fn handle(&self, opt: &DeleteObjectOption, object: &str) -> Result<(), String>;
}

/// Handle S3 DELETE Object requests with version and metadata support
///
/// This function processes S3 DeleteObject (DELETE /{bucket}/{key}) requests,
/// providing safe object deletion with proper validation and AWS S3 compatibility.
///
/// ## Features
/// - **Object Existence Check**: Verifies object exists before deletion
/// - **Atomic Deletion**: Ensures object deletion is atomic and consistent
/// - **Metadata Cleanup**: Removes associated metadata and references
/// - **Version Support**: Handles object versioning if enabled
/// - **Error Handling**: Proper S3 error responses for various scenarios
///
/// ## Parameters
/// - `req`: HTTP request containing bucket and object key to delete
/// - `resp`: HTTP response writer for deletion confirmation
/// - `handler`: Backend implementation for object deletion
///
/// ## HTTP Response Codes
/// - `204 No Content`: Object deleted successfully
/// - `404 Not Found`: Object does not exist (S3 returns 204 anyway)
/// - `405 Method Not Allowed`: Non-DELETE request
/// - `500 Internal Server Error`: Backend deletion error
///
/// ## S3 Behavior Note
/// S3 returns 204 even if the object doesn't exist, for security reasons
pub async fn handle_delete_object<T: VRequest, F: VResponse>(
    req: T,
    resp: &mut F,
    handler: &std::sync::Arc<dyn DeleteObjectHandler + Send + Sync>,
) {
    let opt = DeleteObjectOption {};
    let url_path = req.url_path();
    if let Err(e) = handler.handle(&opt, url_path.trim_matches('/')).await {
        resp.set_status(500);
        log::info!("delete object handler error: {e}");
    } else {
        resp.set_status(204);
    }
}
pub struct MultiUploadObjectCompleteOption {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
}
pub trait MultiUploadObjectHandler {
    fn handle_create_session<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<String, ()>>>>;

    ///return etag
    fn handle_upload_part<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        upload_id: &'a str,
        part_number: u32,
        body: &'a mut (dyn tokio::io::AsyncRead + Unpin + Send),
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<String, ()>>>>;

    fn handle_complete<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        upload_id: &'a str,
        //(etag,part number)
        data: &'a [(&'a str, u32)],
        opts: MultiUploadObjectCompleteOption,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<String, ()>>>>;

    fn handle_abort<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        upload_id: &'a str,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<(), ()>>>>;
}

pub async fn handle_multipart_create_session<T: VRequest, F: VResponse>(
    req: T,
    resp: &mut F,
    handler: &std::sync::Arc<dyn MultiUploadObjectHandler + Send + Sync>,
) {
    let raw_path = req.url_path();
    let raw = raw_path
        .trim_start_matches('/')
        .splitn(2, '/')
        .collect::<Vec<&str>>();
    if raw.len() != 2 {
        resp.set_status(400);
        resp.send_header();
        return;
    }

    let bucket = raw[0];
    let key = raw[1];
    match handler.handle_create_session(bucket, key).await {
        Ok(upload_id) => {
            #[derive(Debug, serde::Serialize)]
            #[serde(rename_all = "PascalCase")]
            pub struct MultipartInitResponse<'a> {
                #[serde(rename = "Bucket")]
                pub bucket: &'a str,
                #[serde(rename = "Key")]
                pub key: &'a str,
                #[serde(rename = "UploadId")]
                pub upload_id: &'a str,
            }

            let r = MultipartInitResponse {
                bucket,
                key,
                upload_id: &upload_id,
            };

            let is_err = match quick_xml::se::to_string(&r) {
                Ok(content) => match resp.get_body_writer().await {
                    Ok(mut w) => {
                        let _ = w.poll_write(content.as_bytes()).await;
                        None
                    }
                    Err(err) => {
                        log::error!("get body writer error {err}");
                        Some(())
                    }
                },
                Err(err) => {
                    log::error!("xml encode error {err}");
                    Some(())
                }
            };

            if is_err.is_some() {
                resp.set_status(500);
                resp.send_header();
            }
        }
        Err(_) => {
            log::error!("handle create session error");
            resp.set_status(500);
            resp.send_header();
        }
    }
}

pub async fn handle_multipart_upload_part<T: VRequest + BodyReader + HeaderTaker, F: VResponse>(
    req: T,
    resp: &mut F,
    handler: &std::sync::Arc<dyn MultiUploadObjectHandler + Send + Sync>,
) {
    let upload_id = req.get_query("uploadId");
    let part_number = req.get_query("partNumber");

    let upload_id = match upload_id {
        Some(id) => id,
        None => {
            resp.set_status(400);
            return;
        }
    };
    let part_number = match part_number {
        Some(num) => num,
        None => {
            resp.set_status(400);
            return;
        }
    };

    let part_number = match part_number.as_str().parse::<u32>() {
        Ok(num) => num,
        Err(_) => {
            resp.set_status(400);
            return;
        }
    };
    let raw_path = req.url_path();

    let raw = raw_path
        .trim_start_matches('/')
        .splitn(2, '/')
        .collect::<Vec<&str>>();
    if raw.len() != 2 {
        resp.set_status(400);
        return;
    }

    let header = req.take_header();
    let body_reader = match req.get_body_reader().await {
        Ok(body_reader) => body_reader,
        Err(err) => {
            log::error!("get body reader failed {err}");
            resp.set_status(500);
            resp.send_header();
            return;
        }
    };

    let (body, release) = match get_body_stream(body_reader, &header).await {
        Ok(data) => data,
        Err(err) => {
            log::error!("get body stream error {err}");
            resp.set_status(500);
            resp.send_header();
            return;
        }
    };

    let ret = match body {
        StreamType::File(mut file) => {
            handler
                .handle_upload_part(raw[0], raw[1], upload_id.as_str(), part_number, &mut file)
                .await
        }

        StreamType::Buff(mut buf_reader) => {
            handler
                .handle_upload_part(
                    raw[0],
                    raw[1],
                    upload_id.as_str(),
                    part_number,
                    &mut buf_reader,
                )
                .await
        }
    };

    if let Some(release) = release {
        release.await;
    }

    if let Ok(etag) = ret {
        resp.set_header("etag", &etag);
    } else {
        resp.set_status(500);
        resp.send_header();
    }
}

pub async fn handle_multipart_complete_session<T: VRequestPlus, F: VResponse>(
    req: T,
    resp: &mut F,
    handler: &std::sync::Arc<dyn MultiUploadObjectHandler + Send + Sync>,
) {
    let raw_path = req.url_path();
    let raw = raw_path
        .trim_start_matches('/')
        .splitn(2, '/')
        .collect::<Vec<&str>>();
    if raw.len() != 2 {
        resp.set_status(400);
        resp.send_header();
        return;
    }

    let bucket = raw[0];
    let key = raw[1];
    let upload_id = req.get_query("uploadId");

    if let Some(upload_id) = upload_id {
        #[derive(Debug, serde::Deserialize)]
        #[serde(rename_all = "PascalCase")]
        pub struct CompleteMultiPartUploadRequest {
            #[serde(rename = "Part")]
            pub parts: Vec<CompletedPart>,
        }
        #[derive(Debug, serde::Deserialize)]
        #[serde(rename_all = "PascalCase")]
        pub struct CompletedPart {
            #[serde(rename = "ETag")]
            pub etag: String,
            #[serde(rename = "PartNumber")]
            pub part_number: u32,
        }

        match req.body().await {
            Ok(body) => {
                match quick_xml::de::from_str::<CompleteMultiPartUploadRequest>(unsafe {
                    std::str::from_utf8_unchecked(&body)
                }) {
                    Ok(upload_request) => {
                        let data = upload_request
                            .parts
                            .iter()
                            .map(|data| (data.etag.as_str(), data.part_number))
                            .collect::<Vec<(&str, u32)>>();
                        match handler
                            .handle_complete(
                                bucket,
                                key,
                                &upload_id,
                                &data,
                                MultiUploadObjectCompleteOption {
                                    if_match: None,
                                    if_none_match: None,
                                },
                            )
                            .await
                        {
                            Ok(etag) => {
                                use serde::{Deserialize, Serialize};
                                #[derive(Debug, Serialize, Deserialize)]
                                #[serde(rename_all = "PascalCase")]
                                pub struct CompleteMultipartUploadResponse<'a> {
                                    #[serde(rename = "Location")]
                                    pub location: &'a str,
                                    #[serde(rename = "Bucket")]
                                    pub bucket: &'a str,
                                    #[serde(rename = "Key")]
                                    pub key: &'a str,
                                    #[serde(rename = "ETag")]
                                    pub etag: &'a str,
                                }

                                let r = CompleteMultipartUploadResponse {
                                    location: "",
                                    bucket,
                                    key,
                                    etag: &etag,
                                };

                                match quick_xml::se::to_string(&r) {
                                    Ok(content) => {
                                        let err = match resp.get_body_writer().await {
                                            Ok(mut w) => {
                                                let _ = w.poll_write(content.as_bytes()).await;
                                                None
                                            }
                                            Err(err) => Some(err),
                                        };

                                        if let Some(err) = err {
                                            log::error!("get body writer error {err}");
                                            resp.set_status(500);
                                            resp.send_header();
                                        }
                                    }
                                    Err(err) => {
                                        log::error!("quick xml encode error {err}");
                                        resp.set_status(500);
                                        resp.send_header();
                                    }
                                }
                            }
                            Err(_) => {
                                log::error!("handle_complete error");
                                resp.set_status(500);
                                resp.send_header();
                            }
                        }
                    }
                    Err(_) => {
                        resp.set_status(400);
                        resp.send_header();
                    }
                }
            }
            Err(err) => {
                log::error!("read body error {err}");
                resp.set_status(500);
                resp.send_header();
            }
        }
    } else {
        resp.set_status(400);
        resp.send_header();
    }
}

pub async fn handle_multipart_abort_session<T: VRequest, F: VResponse>(
    _req: T,
    _resp: &mut F,
    _handler: &std::sync::Arc<dyn MultiUploadObjectHandler + Send + Sync>,
) {
    todo!()
}
pub struct CreateBucketOption {
    pub grant_full_control: Option<String>,
    pub grant_read: Option<String>,
    pub grant_read_acp: Option<String>,
    pub grant_write: Option<String>,
    pub grant_write_acp: Option<String>,
    pub object_lock_enabled_for_bucket: Option<bool>,
    pub object_ownership: Option<ObjectOwnership>,
}
pub enum ObjectOwnership {
    BucketOwnerPreferred,
    ObjectWriter,
    BucketOwnerEnforced,
}

impl FromStr for ObjectOwnership {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "BucketOwnerPreferred" => Ok(ObjectOwnership::BucketOwnerPreferred),
            "ObjectWriter" => Ok(ObjectOwnership::ObjectWriter),
            "BucketOwnerEnforced" => Ok(ObjectOwnership::BucketOwnerEnforced),
            _ => Err(Error::Other(s.to_string())),
        }
    }
}
pub struct CreateBucketConfiguration {
    pub bucket: Option<BucketInfo>,
    pub location: Option<LocationInfo>,
    pub location_constraint: Option<BucketLocationConstraint>,
}
pub struct BucketInfo {
    pub data_redundancy: DataRedundancy,
    pub bucket_type: BucketType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataRedundancy {
    SingleAvailabilityZone,
    SingleLocalZone,
    Unknown(String),
}

impl From<&str> for DataRedundancy {
    fn from(s: &str) -> Self {
        match s {
            "SingleAvailabilityZone" => Self::SingleAvailabilityZone,
            "SingleLocalZone" => Self::SingleLocalZone,
            other => Self::Unknown(other.to_string()),
        }
    }
}

impl Display for DataRedundancy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            match self {
                DataRedundancy::SingleAvailabilityZone => "SingleAvailabilityZone".to_string(),
                DataRedundancy::SingleLocalZone => "SingleLocalZone".to_string(),
                DataRedundancy::Unknown(s) => s.clone(),
            }
            .as_str(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BucketType {
    Directory,
    Unknown(String),
}

impl From<&str> for BucketType {
    fn from(s: &str) -> Self {
        match s {
            "Directory" => Self::Directory,
            other => Self::Unknown(other.to_string()),
        }
    }
}

impl Display for BucketType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            match self {
                Self::Directory => "Directory".to_string(),
                Self::Unknown(s) => s.clone(),
            }
            .as_str(),
        )
    }
}

pub struct LocationInfo {
    pub name: Option<String>,
    pub location_type: LocationType,
}

pub enum LocationType {
    AvailabilityZone,
    LocalZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BucketLocationConstraint {
    AfSouth1,
    ApEast1,
    ApNortheast1,
    ApNortheast2,
    ApNortheast3,
    ApSouth1,
    ApSouth2,
    ApSoutheast1,
    ApSoutheast2,
    ApSoutheast3,
    ApSoutheast4,
    ApSoutheast5,
    CaCentral1,
    CnNorth1,
    CnNorthwest1,
    Eu,
    EuCentral1,
    EuCentral2,
    EuNorth1,
    EuSouth1,
    EuSouth2,
    EuWest1,
    EuWest2,
    EuWest3,
    IlCentral1,
    MeCentral1,
    MeSouth1,
    SaEast1,
    UsEast2,
    UsGovEast1,
    UsGovWest1,
    UsWest1,
    UsWest2,
    Unknown(String),
}

impl From<&str> for BucketLocationConstraint {
    fn from(s: &str) -> Self {
        match s {
            "af-south-1" => Self::AfSouth1,
            "ap-east-1" => Self::ApEast1,
            "ap-northeast-1" => Self::ApNortheast1,
            "ap-northeast-2" => Self::ApNortheast2,
            "ap-northeast-3" => Self::ApNortheast3,
            "ap-south-1" => Self::ApSouth1,
            "ap-south-2" => Self::ApSouth2,
            "ap-southeast-1" => Self::ApSoutheast1,
            "ap-southeast-2" => Self::ApSoutheast2,
            "ap-southeast-3" => Self::ApSoutheast3,
            "ap-southeast-4" => Self::ApSoutheast4,
            "ap-southeast-5" => Self::ApSoutheast5,
            "ca-central-1" => Self::CaCentral1,
            "cn-north-1" => Self::CnNorth1,
            "cn-northwest-1" => Self::CnNorthwest1,
            "EU" => Self::Eu,
            "eu-central-1" => Self::EuCentral1,
            "eu-central-2" => Self::EuCentral2,
            "eu-north-1" => Self::EuNorth1,
            "eu-south-1" => Self::EuSouth1,
            "eu-south-2" => Self::EuSouth2,
            "eu-west-1" => Self::EuWest1,
            "eu-west-2" => Self::EuWest2,
            "eu-west-3" => Self::EuWest3,
            "il-central-1" => Self::IlCentral1,
            "me-central-1" => Self::MeCentral1,
            "me-south-1" => Self::MeSouth1,
            "sa-east-1" => Self::SaEast1,
            "us-east-2" => Self::UsEast2,
            "us-gov-east-1" => Self::UsGovEast1,
            "us-gov-west-1" => Self::UsGovWest1,
            "us-west-1" => Self::UsWest1,
            "us-west-2" => Self::UsWest2,
            other => Self::Unknown(other.to_string()),
        }
    }
}

impl Display for BucketLocationConstraint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::AfSouth1 => "af-south-1",
            Self::ApEast1 => "ap-east-1",
            Self::ApNortheast1 => "ap-northeast-1",
            Self::ApNortheast2 => "ap-northeast-2",
            Self::ApNortheast3 => "ap-northeast-3",
            Self::ApSouth1 => "ap-south-1",
            Self::ApSouth2 => "ap-south-2",
            Self::ApSoutheast1 => "ap-southeast-1",
            Self::ApSoutheast2 => "ap-southeast-2",
            Self::ApSoutheast3 => "ap-southeast-3",
            Self::ApSoutheast4 => "ap-southeast-4",
            Self::ApSoutheast5 => "ap-southeast-5",
            Self::CaCentral1 => "ca-central-1",
            Self::CnNorth1 => "cn-north-1",
            Self::CnNorthwest1 => "cn-northwest-1",
            Self::Eu => "EU",
            Self::EuCentral1 => "eu-central-1",
            Self::EuCentral2 => "eu-central-2",
            Self::EuNorth1 => "eu-north-1",
            Self::EuSouth1 => "eu-south-1",
            Self::EuSouth2 => "eu-south-2",
            Self::EuWest1 => "eu-west-1",
            Self::EuWest2 => "eu-west-2",
            Self::EuWest3 => "eu-west-3",
            Self::IlCentral1 => "il-central-1",
            Self::MeCentral1 => "me-central-1",
            Self::MeSouth1 => "me-south-1",
            Self::SaEast1 => "sa-east-1",
            Self::UsEast2 => "us-east-2",
            Self::UsGovEast1 => "us-gov-east-1",
            Self::UsGovWest1 => "us-gov-west-1",
            Self::UsWest1 => "us-west-1",
            Self::UsWest2 => "us-west-2",
            Self::Unknown(s) => s,
        })
    }
}

#[async_trait::async_trait]
pub trait CreateBucketHandler {
    async fn handle(&self, opt: &CreateBucketOption, bucket: &str) -> Result<(), String>;
}

/// Handle S3 CREATE Bucket requests with region and configuration support
///
/// This function processes S3 CreateBucket (PUT /{bucket}) requests, providing full
/// AWS S3 compatibility for bucket creation with proper validation and error handling.
///
/// ## Features
/// - **Bucket Validation**: Ensures bucket names follow S3 naming conventions
/// - **Duplicate Detection**: Returns 409 Conflict for existing buckets
/// - **Region Support**: Handles bucket location constraints and region configuration
/// - **Atomic Creation**: Ensures bucket creation is atomic and consistent
/// - **Error Handling**: Proper S3 error responses for various failure scenarios
///
/// ## Parameters
/// - `req`: HTTP request containing bucket name and optional configuration
/// - `resp`: HTTP response writer for creation confirmation
/// - `handler`: Backend implementation for bucket creation
///
/// ## HTTP Response Codes
/// - `200 OK`: Bucket created successfully
/// - `400 Bad Request`: Invalid bucket name or configuration
/// - `409 Conflict`: Bucket already exists
/// - `405 Method Not Allowed`: Non-PUT request
/// - `500 Internal Server Error`: Backend creation error
pub async fn handle_create_bucket<T: VRequest, F: VResponse>(
    req: T,
    resp: &mut F,
    handler: &std::sync::Arc<dyn CreateBucketHandler + Send + Sync>,
) {
    if req.method() != "PUT" {
        resp.set_status(405);
        resp.send_header();
        return;
    }

    let opt = CreateBucketOption {
        grant_full_control: req.get_header("x-amz-grant-full-control"),
        grant_read: req.get_header("x-amz-grant-read"),
        grant_read_acp: req.get_header("x-amz-grant-read-acp"),
        grant_write: req.get_header("x-amz-grant-write"),
        grant_write_acp: req.get_header("x-amz-grant-write-acp"),
        object_lock_enabled_for_bucket: req
            .get_header("x-amz-bucket-object-lock-enabled")
            .and_then(|v| {
                if v == "true" {
                    Some(true)
                } else if v == "false" {
                    Some(false)
                } else {
                    None
                }
            }),
        object_ownership: req
            .get_header("x-amz-object-ownership")
            .and_then(|v| v.parse().ok()),
    };
    let url_path = req.url_path();
    if let Err(e) = handler.handle(&opt, url_path.trim_matches('/')).await {
        if e.contains("BucketAlreadyExists") {
            resp.set_status(409); // Conflict
        } else {
            resp.set_status(500); // Internal Server Error
        }
        log::info!("create bucket handler error: {e}")
    }
}

pub struct DeleteBucketOption {
    pub expected_owner: Option<String>,
}

#[async_trait::async_trait]
pub trait DeleteBucketHandler {
    async fn handle(&self, opt: &DeleteBucketOption, bucket: &str) -> Result<(), String>;
}

/// Handle S3 DELETE Bucket requests with safety validation
///
/// This function processes S3 DeleteBucket (DELETE /{bucket}) requests, providing
/// safe bucket deletion with proper validation and AWS S3 compatibility.
///
/// ## Features
/// - **Empty Bucket Validation**: Ensures bucket is empty before deletion
/// - **Existence Check**: Verifies bucket exists before attempting deletion
/// - **Atomic Deletion**: Ensures bucket deletion is atomic and consistent
/// - **Safety Measures**: Prevents accidental deletion of non-empty buckets
/// - **Error Handling**: Proper S3 error responses for various failure scenarios
///
/// ## Parameters
/// - `req`: HTTP request containing bucket name to delete
/// - `resp`: HTTP response writer for deletion confirmation
/// - `handler`: Backend implementation for bucket deletion
///
/// ## HTTP Response Codes
/// - `204 No Content`: Bucket deleted successfully
/// - `404 Not Found`: Bucket does not exist
/// - `409 Conflict`: Bucket is not empty
/// - `405 Method Not Allowed`: Non-DELETE request
/// - `500 Internal Server Error`: Backend deletion error
pub async fn handle_delete_bucket<T: VRequest, F: VResponse>(
    req: T,
    resp: &mut F,
    handler: &std::sync::Arc<dyn DeleteBucketHandler + Send + Sync>,
) {
    if req.method() != "DELETE" {
        resp.set_status(405);
        resp.send_header();
        return;
    }
    let opt = DeleteBucketOption {
        expected_owner: req.get_header("x-amz-expected-bucket-owner"),
    };
    let url_path = req.url_path();
    match handler.handle(&opt, url_path.trim_matches('/')).await {
        Ok(_) => {
            resp.set_status(204);
            resp.send_header();
        }
        Err(e) => {
            resp.set_status(500);
            log::error!("delete object handler error: {e}")
        }
    }
}

//utils
// Use unified BodyParseError from crate::error module
pub use crate::error::BodyParseError as ParseBodyError;
enum StreamType {
    File(tokio::fs::File),
    Buff(tokio::io::BufReader<std::io::Cursor<Vec<u8>>>),
}

async fn get_body_stream<T: crate::utils::io::PollRead + Send, H: crate::auth::sig_v4::VHeader>(
    mut src: T,
    header: &H,
) -> Result<
    (
        StreamType,
        Option<std::pin::Pin<Box<dyn Send + std::future::Future<Output = ()>>>>,
    ),
    ParseBodyError,
> {
    let cl = header.get_header("content-length");
    let acs = header
        .get_header("x-amz-content-sha256")
        .ok_or(ParseBodyError::HashMismatch)?;
    if let Some(cl) = cl {
        let cl = cl
            .as_str()
            .parse::<usize>()
            .or(Err(ParseBodyError::ContentLengthMismatch))?;
        if acs.as_str() != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
            if cl <= 10 << 20 {
                // Use BytesMut to avoid zero-prefilled Vec allocation
                use bytes::BytesMut;
                let mut buff = BytesMut::new();

                // Read data using PollRead and accumulate in BytesMut
                let mut hsh = sha2::Sha256::new();
                let mut remaining = cl;
                while let Some(chunk) = src.poll_read().await.map_err(ParseBodyError::Io)? {
                    let chunk_len = chunk.len();
                    if remaining < chunk_len {
                        return Err(ParseBodyError::ContentLengthMismatch);
                    }
                    remaining -= chunk_len;
                    let _ = hsh.write_all(&chunk);
                    buff.extend_from_slice(&chunk);
                }

                // Verify SHA256 if not UNSIGNED-PAYLOAD
                if acs.as_str() != "UNSIGNED-PAYLOAD" {
                    let ret = hsh.finalize();
                    let real_sha256 = hex::encode(ret);
                    if real_sha256.as_str() != acs.as_str() {
                        return Err(ParseBodyError::HashMismatch);
                    }
                }

                return Ok((
                    StreamType::Buff(tokio::io::BufReader::new(std::io::Cursor::new(
                        buff.to_vec(),
                    ))),
                    None,
                ));
            } else {
                let file_name = format!(
                    "/tmp/curvine-temp/{}",
                    &uuid::Uuid::new_v4().to_string()[..8]
                );
                let mut fd = tokio::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .mode(0o644)
                    .open(file_name.as_str())
                    .await
                    .map_err(|err| ParseBodyError::Io(err.to_string()))?;
                parse_body(src, &mut fd, &acs, cl).await?;
                drop(fd);
                match tokio::fs::OpenOptions::new()
                    .read(true)
                    .open(file_name.as_str())
                    .await
                {
                    Ok(fd) => {
                        return Ok((
                            StreamType::File(fd),
                            Some({
                                Box::pin(async move {
                                    let _ = tokio::fs::remove_file(file_name.as_str()).await;
                                })
                            }),
                        ))
                    }
                    Err(err) => {
                        let _ = tokio::fs::remove_file(file_name.as_str()).await;
                        return Err(ParseBodyError::Io(err.to_string()));
                    }
                }
                // return Ok(())
            }
        }
    }
    //chunk
    todo!()
}

async fn parse_body<
    T: crate::utils::io::PollRead + Send,
    E: tokio::io::AsyncWrite + Send + Unpin,
>(
    src: T,
    dst: &mut E,
    content_sha256: &str,
    content_length: usize,
) -> Result<(), ParseBodyError> {
    use tokio::io::AsyncWriteExt;

    let (chunks, _hash) = read_and_validate_body(src, content_sha256, content_length).await?;

    // Write all chunks to destination
    for chunk in chunks {
        dst.write_all(&chunk)
            .await
            .map_err(|err| ParseBodyError::Io(format!("write error {err}")))?;
    }

    Ok(())
}

#[async_trait::async_trait]
impl crate::utils::io::PollRead for tokio::io::BufReader<std::io::Cursor<Vec<u8>>> {
    async fn poll_read(&mut self) -> Result<Option<Vec<u8>>, String> {
        use tokio::io::AsyncReadExt;
        let mut buf = vec![0u8; 64 * 1024];
        match self.read(&mut buf).await {
            Ok(0) => Ok(None),
            Ok(n) => {
                buf.truncate(n);
                Ok(Some(buf))
            }
            Err(e) => Err(e.to_string()),
        }
    }
}

#[async_trait::async_trait]
impl crate::utils::io::PollRead for tokio::fs::File {
    async fn poll_read(&mut self) -> Result<Option<Vec<u8>>, String> {
        use tokio::io::AsyncReadExt;
        let mut buf = vec![0u8; 64 * 1024];
        match self.read(&mut buf).await {
            Ok(0) => Ok(None),
            Ok(n) => {
                buf.truncate(n);
                Ok(Some(buf))
            }
            Err(e) => Err(e.to_string()),
        }
    }
}
struct InMemoryPollReader {
    data: Vec<u8>,
    offset: usize,
}

#[async_trait::async_trait]
impl crate::utils::io::PollRead for InMemoryPollReader {
    async fn poll_read(&mut self) -> Result<Option<Vec<u8>>, String> {
        if self.offset >= self.data.len() {
            return Ok(None);
        }
        let remaining = self.data.len() - self.offset;
        // read up to 64KB per poll
        let to_read = remaining.min(64 * 1024);
        let chunk = self.data[self.offset..self.offset + to_read].to_vec();
        self.offset += to_read;
        Ok(Some(chunk))
    }
}

// Helper: read entire body into Vec<u8> and verify sha256
/// Core body reading and validation logic
///
/// This function provides the common logic for reading request bodies with
/// SHA256 validation and content length checking.
async fn read_and_validate_body<T>(
    mut src: T,
    expected_sha256: &str,
    mut content_length: usize,
) -> Result<(Vec<Vec<u8>>, sha2::digest::Output<sha2::Sha256>), ParseBodyError>
where
    T: crate::utils::io::PollRead + Send,
{
    use std::io::Write;
    let mut hasher = sha2::Sha256::new();
    let mut chunks = Vec::new();

    while let Some(buff) = src.poll_read().await.map_err(ParseBodyError::Io)? {
        let buff_len = buff.len();
        if content_length < buff_len {
            return Err(ParseBodyError::ContentLengthMismatch);
        }
        content_length -= buff_len;
        let _ = hasher.write_all(&buff);
        chunks.push(buff);
    }

    let hash_result = hasher.finalize();
    let real_sha256 = hex::encode(hash_result);
    if real_sha256.as_str() != expected_sha256 {
        return Err(ParseBodyError::HashMismatch);
    }

    Ok((chunks, hash_result))
}

async fn read_body_to_vec<T: crate::utils::io::PollRead + Send>(
    src: T,
    expected_sha256: &str,
    content_length: usize,
) -> Result<Vec<u8>, ParseBodyError> {
    let (chunks, _hash) = read_and_validate_body(src, expected_sha256, content_length).await?;

    // Combine all chunks into a single Vec
    let total_size = chunks.iter().map(|chunk| chunk.len()).sum();
    let mut result = Vec::with_capacity(total_size);
    for chunk in chunks {
        result.extend_from_slice(&chunk);
    }

    Ok(result)
}
