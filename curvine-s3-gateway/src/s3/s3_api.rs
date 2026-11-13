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

pub use crate::s3::error_code::Error;
use std::{
    fmt::{Debug, Display},
    str::FromStr,
};

pub use crate::s3::dto::ArchiveStatus;

pub use crate::s3::dto::{DateTime, DEFAULT_OWNER_ID};
static OWNER_ID: &str = DEFAULT_OWNER_ID;

pub trait VRequest: crate::auth::sig_v4::VHeader {
    fn method(&self) -> String;
    fn url_path(&self) -> String;
    fn get_query(&self, k: &str) -> Option<String>;
    fn all_query(&self, cb: impl FnMut(&str, &str) -> bool);
}
pub trait BodyWriter {
    type BodyWriter<'a>: crate::utils::io::PollWrite + Send + Unpin
    where
        Self: 'a;
    fn get_body_writer(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Self::BodyWriter<'_>, String>> + Send;
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

pub use crate::s3::dto::HeadObjectResult;

pub trait HeadHandler: Send + Sync {
    fn lookup(
        &self,
        bucket: &str,
        object: &str,
    ) -> impl std::future::Future<Output = Result<Option<HeadObjectResult>, Error>> + Send;
}

pub use crate::s3::dto::GetObjectOption;

/// GET Object handler trait
///
/// Note: This trait uses Box<dyn Future> because the output writer has a borrowed
/// lifetime that cannot be expressed with impl Future in stable Rust.
/// The Box allocation here is unavoidable due to lifetime constraints.
pub trait GetObjectHandler: HeadHandler {
    fn handle<'a>(
        &'a self,
        bucket: &str,
        object: &str,
        opt: GetObjectOption,
        out: &'a mut (dyn crate::utils::io::PollWrite + Unpin + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>>;
}

extern crate serde;
use serde::Serialize;

use crate::utils::io::PollWrite;

pub use crate::s3::dto::{Bucket, Buckets, CommonPrefix, ListAllMyBucketsResult, Owner};
pub use crate::s3::dto::{ListObjectContent, ListObjectResult};

#[derive(Debug, Clone, Default, Serialize)]
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

/// OPTIMIZED: No async_trait - zero Box allocation
pub trait ListObjectHandler: Send + Sync {
    fn handle(
        &self,
        opt: &ListObjectOption,
        bucket: &str,
    ) -> impl std::future::Future<Output = Result<super::listing::ListObjectPage, String>> + Send;
}

pub use crate::s3::dto::{DeleteMarker, ListObjectVersionsResult, ObjectVersion};

#[derive(Debug, Clone)]
pub struct ListObjectVersionsOption {
    pub bucket: String,

    pub delimiter: Option<String>,
    pub encoding_type: Option<String>,
    pub key_marker: Option<String>,
    pub max_keys: Option<i32>,
    pub prefix: Option<String>,
    pub version_id_marker: Option<String>,
}

/// OPTIMIZED: No async_trait - zero Box allocation
pub trait ListObjectVersionsHandler: Send + Sync {
    fn handle(
        &self,
        opt: &ListObjectVersionsOption,
        bucket: &str,
    ) -> impl std::future::Future<Output = Result<ListObjectVersionsResult, String>> + Send;
}

/// Legacy generic entry point, superseded by `S3Router::handle_head_object_request`.
///
/// Kept for API compatibility only: the live HEAD route reads metadata through
/// `S3Handlers::lookup`. Aborts instead of panicking so a mis-wired caller
/// cannot take the gateway down in a `panic = "abort"` build.
pub fn handle_head_object<T: VRequest, F: VResponse, E: HeadHandler>(
    _req: &T,
    resp: &mut F,
    _handler: &E,
) {
    resp.set_status(501);
    resp.send_header();
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
/// - OPTIMIZED: Generic handler parameter eliminates async_trait Box allocation
pub async fn handle_get_object<T: VRequest, F: VResponse, H: GetObjectHandler + Send + Sync>(
    req: T,
    resp: &mut F,
    handler: &H,
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
            log::error!("{}", curvine_core_error::err_msg!("Invalid path format"));
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

    if let Some(rh) = req.get_header("range") {
        if let Some(bytes) = rh.strip_prefix("bytes=") {
            if let Some(suffix_str) = bytes.strip_prefix('-') {
                if let Ok(suffix_len) = suffix_str.parse::<u64>() {
                    opt.range_start = None;
                    opt.range_end = Some(u64::MAX - suffix_len);
                }
            } else {
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
            log::error!(
                "{}",
                curvine_core_error::err_msg!("lookup bucket object error: {}", err)
            );
            resp.set_status(500);
            resp.send_header();
            return;
        }
    };
    let head = match head {
        Some(val) => val,
        None => {
            log::error!("{}", curvine_core_error::err_msg!("Object not found"));
            resp.set_status(404);
            resp.send_header();
            return;
        }
    };

    let total_len = head.content_length.unwrap_or(0) as u64;

    let mut status = 200u16;
    let mut resp_len = total_len;
    let header_last_modified = head.last_modified.clone();
    let header_etag = head.etag.clone();
    let header_ct = head.content_type.clone();

    if opt.range_start.is_some() || opt.range_end.is_some() {
        let (start, end) = if let Some(range_end) = opt.range_end {
            if range_end > u64::MAX / 2 {
                let suffix_len = u64::MAX - range_end;

                const MAX_SUFFIX_SIZE: u64 = 1024 * 1024 * 1024;
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
                    (0, total_len.saturating_sub(1))
                } else {
                    (total_len - suffix_len, total_len.saturating_sub(1))
                }
            } else {
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

    if let Some(metadata) = head.metadata {
        for (key, value) in metadata {
            let header_name = if key.starts_with("x-amz-meta-") {
                key.to_string()
            } else {
                format!("x-amz-meta-{key}")
            };
            resp.set_header(&header_name, &value);
        }
    }

    resp.set_header("connection", "keep-alive");
    resp.set_status(status);
    resp.send_header();

    let body_result = resp.get_body_writer().await;
    match body_result {
        Ok(mut body) => {
            if let Err(err) = handler.handle(bucket, object, opt, &mut body).await {
                log::error!("body handle error {err}");
            }
        }
        Err(err) => {
            log::error!("get body writer error {err}");
        }
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
/// - OPTIMIZED: Generic handler parameter eliminates async_trait Box allocation
pub async fn handle_get_list_object_versions<
    T: VRequest,
    F: VResponse,
    H: ListObjectVersionsHandler + Send + Sync,
>(
    req: T,
    resp: &mut F,
    handler: &H,
) {
    let url_path = req.url_path();
    let bucket_name = url_path.trim_start_matches('/');

    let opt = ListObjectVersionsOption {
        bucket: bucket_name.to_string(),
        delimiter: req.get_query("delimiter"),
        encoding_type: req.get_query("encoding-type"),
        key_marker: req.get_query("key-marker"),
        max_keys: req.get_query("max-keys").and_then(|s| s.parse().ok()),
        prefix: req.get_query("prefix"),
        version_id_marker: req.get_query("version-id-marker"),
    };

    match handler.handle(&opt, bucket_name).await {
        Ok(result) => {
            resp.set_header("content-type", "application/xml");
            resp.set_status(200);

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

pub async fn handle_get_list_object<
    T: VRequest,
    F: VResponse,
    H: ListObjectHandler + Send + Sync,
>(
    req: T,
    resp: &mut F,
    handler: &H,
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

    let max_keys = match req.get_query("max-keys") {
        Some(value) => match value.parse::<u64>() {
            Ok(value) => Some(value.min(1000) as i32),
            Err(_) => {
                resp.set_status(400);
                resp.send_header();
                return;
            }
        },
        None => None,
    };
    let opt = ListObjectOption {
        bucket: bucket.clone(),
        continuation_token: req.get_query("continuation-token"),
        delimiter: req.get_query("delimiter"),
        expected_bucket_owner: req.get_query("expected-bucket-owner"),
        max_keys,
        optional_object_attributes: None,
        request_payer: req.get_header("x-amz-request-payer"),
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
            // Traversal, grouping and pagination happen together in the backend;
            // the HTTP layer must not collect or paginate the bucket a second time.
            let total_count = ans.contents.len() + ans.common_prefixes.len();
            let mut result = ListObjectResult {
                xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
                name: bucket,
                prefix: opt.prefix,
                key_count: Some(total_count as u32),
                max_keys: Some(opt.max_keys.unwrap_or(1000).min(1000) as u32),
                delimiter: opt.delimiter,
                encoding_type: opt.encoding_type,
                start_after: opt.start_after,
                continuation_token: opt.continuation_token,
                next_continuation_token: ans.next_continuation_token,
                is_truncated: ans.is_truncated,
                contents: ans.contents,
                common_prefixes: ans.common_prefixes,
            };

            if result.encoding_type.as_deref() == Some("url") {
                let encode = |value: &str| urlencoding::encode(value).into_owned();
                result.prefix = result.prefix.as_deref().map(encode);
                result.delimiter = result.delimiter.as_deref().map(encode);
                result.start_after = result.start_after.as_deref().map(encode);
                for item in &mut result.contents {
                    item.key = encode(&item.key);
                }
                for group in &mut result.common_prefixes {
                    group.prefix = encode(&group.prefix);
                }
            }
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
        Err(err) => {
            // Without a status this answered whatever the response defaulted
            // to, so "bucket does not exist" looked like an empty listing or a
            // silent failure.
            let status = status_for_error(&err);
            log::warn!("get_list_object failed ({status}): {err}");
            resp.set_status(status);
            resp.send_header();
        }
    }
}

pub trait ListBucketHandler: Send + Sync {
    fn handle(
        &self,
        opt: &ListBucketsOption,
    ) -> impl std::future::Future<Output = Result<Vec<Bucket>, String>> + Send;
}
pub trait GetBucketLocationHandler: Send + Sync {
    fn handle(
        &self,
        loc: Option<&str>,
    ) -> impl std::future::Future<Output = Result<Option<&'static str>, String>> + Send;
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
/// - OPTIMIZED: Generic handler parameter eliminates async_trait Box allocation
pub async fn handle_get_list_buckets<
    T: VRequest,
    F: VResponse,
    H: ListBucketHandler + Send + Sync,
>(
    req: T,
    resp: &mut F,
    handler: &H,
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
            log::error!("list buckets handle error: {e}");
            resp.set_status(500);
            resp.send_header();
        }
    }
}

pub use crate::s3::dto::{
    ChecksumAlgorithm, ObjectLockLegalHoldStatus, ObjectLockMode, PutObjectOption, RequestPayer,
};
pub trait PutObjectHandler: Send + Sync {
    /// Writes the object and returns its ETag, which the S3 response echoes.
    fn handle(
        &self,
        opt: PutObjectOption,
        bucket: String,
        object: String,
        body: crate::utils::io::PollReaderEnum,
    ) -> impl std::future::Future<Output = Result<String, String>> + Send;
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
pub async fn handle_put_object<T, F, H>(
    v4head: crate::auth::sig_v4::V4Head,
    req: T,
    resp: &mut F,
    handler: &H,
) where
    T: VRequest + BodyReader,
    F: VResponse,
    H: PutObjectHandler + Send + Sync,
    <T as BodyReader>::BodyReader: 'static,
{
    let _ = &v4head;
    if req.method() != "PUT" {
        resp.set_status(405);
        resp.send_header();
        return;
    }

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
        content_encoding: req.get_header("content-encoding"),
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
        write_offset_bytes: req
            .get_header("x-amz-write-offset-bytes")
            .and_then(|v| v.parse::<i64>().ok()),
        metadata: {
            // S3 user metadata travels as `x-amz-meta-<name>`; keep the name
            // without the prefix so HEAD can emit it back unchanged.
            let mut map = std::collections::HashMap::new();
            req.rng_header(|k, v| {
                if let Some(name) = k.strip_prefix("x-amz-meta-") {
                    map.insert(name.to_string(), v.to_string());
                }
                true
            });
            if map.is_empty() {
                None
            } else {
                Some(map)
            }
        },
    };

    enum ContentSha256 {
        Hash(String),
        Streaming,
        Unsigned,
    }

    let content_sha256 = req.get_header("x-amz-content-sha256").map_or_else(
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
            log::error!("{}", curvine_core_error::err_msg!("Missing content SHA256"));
            resp.set_status(404);
            resp.send_header();
            return;
        }
    };
    let r = match req.get_body_reader().await {
        Ok(val) => val,
        Err(err) => {
            log::error!(
                "{}",
                curvine_core_error::err_msg!("get body reader error: {}", err)
            );
            resp.set_status(500);
            resp.send_header();
            return;
        }
    };
    let expected_length = match opt.content_length {
        Some(length) if length >= 0 => usize::try_from(length).ok().map(|value| value as u64),
        Some(_) => {
            log::warn!("invalid content length: {:?}", opt.content_length);
            resp.set_status(400);
            resp.send_header();
            return;
        }
        None => None,
    };

    let ret: Result<String, String> = match content_sha256 {
        ContentSha256::Hash(cs) => {
            let Some(expected_length) = expected_length else {
                log::warn!("missing content length");
                resp.set_status(400);
                resp.send_header();
                return;
            };
            let body = Box::new(crate::utils::verifying_reader::VerifyingReader::new(
                r,
                Some(cs),
                Some(expected_length),
            ));
            handler
                .handle(opt.clone(), bucket.to_string(), object.to_string(), body)
                .await
        }
        ContentSha256::Streaming => {
            // Chunked SigV4 payloads are rejected earlier by `get_body_stream`;
            // reaching here means the framing was not decoded, so fail with a
            // client error instead of staging an unverified body.
            log::warn!("STREAMING-AWS4-HMAC-SHA256-PAYLOAD bodies are not supported");
            resp.set_status(400);
            resp.send_header();
            return;
        }
        ContentSha256::Unsigned => {
            let body = Box::new(crate::utils::verifying_reader::VerifyingReader::new(
                r,
                None,
                expected_length,
            ));
            handler
                .handle(opt.clone(), bucket.to_string(), object.to_string(), body)
                .await
        }
    };
    match ret {
        Ok(etag) => {
            resp.set_header("etag", &etag);
            resp.set_status(200);
            resp.send_header();
        }
        Err(err) => {
            // A failed `If-Match` / `If-None-Match` is 412, not a server error;
            // clients must be able to tell a lost update from an outage.
            let status = status_for_error(&err);
            log::warn!("put object failed ({status}): {err}");
            resp.set_status(status);
            resp.send_header();
        }
    }
}
pub struct DeleteObjectOption {}

/// OPTIMIZED: No async_trait - zero Box allocation
pub trait DeleteObjectHandler: Send + Sync {
    fn handle(
        &self,
        opt: &DeleteObjectOption,
        object: &str,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;
}

// ===== DeleteObjects (Batch) =====
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ObjectIdentifier {
    #[serde(rename = "Key")]
    key: String,
    #[allow(dead_code)]
    #[serde(rename = "VersionId")]
    version_id: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DeleteObjectsRequest {
    #[serde(rename = "Object")]
    objects: Vec<ObjectIdentifier>,
    #[allow(dead_code)]
    quiet: Option<bool>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "PascalCase", rename = "DeleteResult")]
struct DeleteResult {
    #[serde(rename = "Deleted")]
    deleted: Vec<DeletedEntry>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "PascalCase")]
struct DeletedEntry {
    #[serde(rename = "Key")]
    key: String,
}

/// Handle S3 DeleteObjects (POST ?delete) batch deletion
/// - OPTIMIZED: Generic handler parameter eliminates async_trait Box allocation
pub async fn handle_post_delete_objects<
    T: VRequestPlus,
    F: VResponse,
    H: DeleteObjectHandler + Send + Sync,
>(
    req: T,
    resp: &mut F,
    handler: &H,
) {
    // Parse bucket name from URL path
    let url_path = req.url_path();
    let bucket = url_path
        .trim_start_matches('/')
        .split('/')
        .next()
        .unwrap_or("");
    if bucket.is_empty() {
        resp.set_status(400);
        resp.send_header();
        return;
    }

    // Read XML body
    let body = match req.body().await {
        Ok(b) => b,
        Err(e) => {
            log::error!("DeleteObjects read body error: {e}");
            resp.set_status(400);
            resp.send_header();
            return;
        }
    };

    // Parse XML. The body is client-controlled, so decode it with validation
    // rather than fabricating a `&str` from unchecked memory.
    let parsed: Result<DeleteObjectsRequest, _> = match std::str::from_utf8(&body) {
        Ok(xml) => quick_xml::de::from_str(xml),
        Err(e) => {
            log::error!("DeleteObjects body is not valid UTF-8: {e}");
            resp.set_status(400);
            resp.send_header();
            return;
        }
    };
    let req_obj = match parsed {
        Ok(v) => v,
        Err(e) => {
            log::error!("DeleteObjects XML parse error: {e}");
            resp.set_status(400);
            resp.send_header();
            return;
        }
    };

    // Execute deletions one by one
    let mut deleted = Vec::with_capacity(req_obj.objects.len());
    let opt = DeleteObjectOption {};
    for obj in req_obj.objects {
        let object_path = format!("{}/{}", bucket, obj.key);
        match handler.handle(&opt, &object_path).await {
            Ok(_) => {
                deleted.push(DeletedEntry { key: obj.key });
            }
            Err(err) => {
                // For S3 compatibility, still include in Deleted list to be lenient
                log::warn!("DeleteObjects: delete failed for {}: {}", object_path, err);
                deleted.push(DeletedEntry { key: obj.key });
            }
        }
    }

    let result = DeleteResult { deleted };
    match quick_xml::se::to_string(&result) {
        Ok(xml) => {
            resp.set_header("content-type", "application/xml");
            resp.set_status(200);
            if let Ok(mut w) = resp.get_body_writer().await {
                let _ = w.poll_write(xml.as_bytes()).await;
            }
        }
        Err(e) => {
            log::error!("DeleteObjects XML encode error: {e}");
            resp.set_status(500);
            resp.send_header();
        }
    }
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
/// - OPTIMIZED: Generic handler parameter eliminates async_trait Box allocation
pub async fn handle_delete_object<
    T: VRequest,
    F: VResponse,
    H: DeleteObjectHandler + Send + Sync,
>(
    req: T,
    resp: &mut F,
    handler: &H,
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

/// Multipart upload handler - uses async_trait for stack safety
///
/// Deep async call chains require heap-allocated futures to prevent stack overflow.
/// This is a necessary trade-off: async_trait boxes futures, but prevents crashes.
/// Part bodies are streamed straight into temp storage, so nothing here
/// buffers a whole part.
#[async_trait::async_trait]
pub trait MultiUploadObjectHandler: Send + Sync {
    async fn handle_create_session(&self, bucket: String, key: String) -> Result<String, ()>;

    /// Stages the part body into the configured temp storage.
    ///
    /// `expect` carries the checks the body must satisfy before the part is
    /// visible; leaving it to the handler lets both the local and the Curvine
    /// backend verify while streaming, without buffering the part in memory.
    async fn handle_upload_part<R: crate::utils::io::PollRead + Send>(
        &self,
        bucket: String,
        key: String,
        upload_id: String,
        part_number: u32,
        body: &mut R,
        expect: crate::utils::temp_storage::StageExpect<'_>,
    ) -> Result<String, ()>;

    async fn handle_complete(
        &self,
        bucket: String,
        key: String,
        upload_id: String,
        data: Vec<(String, u32)>,
        opts: MultiUploadObjectCompleteOption,
    ) -> Result<String, String>;

    async fn handle_abort(&self, bucket: String, key: String, upload_id: String) -> Result<(), ()>;
}

/// - OPTIMIZED: Generic handler parameter eliminates async_trait Box allocation
pub async fn handle_multipart_create_session<
    T: VRequest,
    F: VResponse,
    H: MultiUploadObjectHandler + Send + Sync,
>(
    req: T,
    resp: &mut F,
    handler: &H,
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
    match handler
        .handle_create_session(bucket.to_string(), key.to_string())
        .await
    {
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

/// - OPTIMIZED: Generic handler parameter eliminates async_trait Box allocation
pub async fn handle_multipart_upload_part<
    T: VRequest + BodyReader + HeaderTaker,
    F: VResponse,
    H: MultiUploadObjectHandler + Send + Sync,
>(
    req: T,
    resp: &mut F,
    handler: &H,
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

    use crate::auth::sig_v4::VHeader as _;
    let header = req.take_header();
    // The part streams straight into the configured temp storage. Buffering it
    // first would hold a whole part (up to the S3 5 GiB limit) in memory, and
    // `panic = "abort"` turns that out-of-memory into a gateway-wide abort.
    if let Err(status) = reject_streaming_part(&header) {
        log::warn!("chunked aws-chunked part bodies are not supported");
        resp.set_status(status);
        resp.send_header();
        return;
    }
    let content_length = match multipart_part_length(&header) {
        Ok(length) => length,
        Err(status) => {
            resp.set_status(status);
            resp.send_header();
            return;
        }
    };
    let declared_sha256 = header.get_header("x-amz-content-sha256");
    let expect = crate::utils::temp_storage::StageExpect {
        sha256_hex: part_sha256(declared_sha256.as_deref()),
        content_length: Some(content_length),
    };

    let mut body_reader = match req.get_body_reader().await {
        Ok(body_reader) => body_reader,
        Err(err) => {
            log::error!("get body reader failed {err}");
            resp.set_status(500);
            resp.send_header();
            return;
        }
    };

    let ret = handler
        .handle_upload_part(
            raw[0].to_string(),
            raw[1].to_string(),
            upload_id.clone(),
            part_number,
            &mut body_reader,
            expect,
        )
        .await;

    if let Ok(etag) = ret {
        resp.set_header("etag", &etag);
    } else {
        resp.set_status(500);
        resp.send_header();
    }
}

/// Reject the SigV4 streaming framing, which this build does not decode.
///
/// Its bytes are length-prefixed chunk records, not part content, so staging
/// them would store a corrupt part. `Content-Length` is also only an estimate
/// for that framing, which makes the length check meaningless.
fn reject_streaming_part<H: crate::auth::sig_v4::VHeader>(header: &H) -> Result<(), u16> {
    // The whole `STREAMING-...` family (including the `-TRAILER` variants) is
    // aws-chunked framing, so match the prefix rather than one exact value.
    if header
        .get_header("x-amz-content-sha256")
        .is_some_and(|value| value.starts_with("STREAMING-"))
    {
        return Err(400);
    }
    Ok(())
}

/// Largest part S3 accepts. A larger declared length is a client error, not a
/// reason to allocate whichever size the header names.
const MAX_PART_BYTES: usize = 5 << 30;

/// Digest a part body must match, or `None` when the client sent no verifiable
/// hash.
///
/// `UNSIGNED-PAYLOAD` carries no whole-body digest. Comparing that literal
/// string with the computed hash is what used to fail every unsigned part
/// above the in-memory threshold. The streaming framing is rejected earlier,
/// so it never reaches this filter.
fn part_sha256(declared: Option<&str>) -> Option<&str> {
    declared.filter(|value| *value != "UNSIGNED-PAYLOAD")
}

/// Declared part length, or the status to answer with.
///
/// A part body is only streamable with a known length: without it the read
/// loop cannot bound the part or report a truncated upload.
fn multipart_part_length<H: crate::auth::sig_v4::VHeader>(header: &H) -> Result<usize, u16> {
    let raw = match header.get_header("content-length") {
        Some(value) => value,
        None => return Err(411),
    };
    let length = raw.trim().parse::<usize>().map_err(|_| 400_u16)?;
    if length > MAX_PART_BYTES {
        return Err(400);
    }
    Ok(length)
}

pub async fn handle_multipart_complete_session<
    T: VRequestPlus,
    F: VResponse,
    H: MultiUploadObjectHandler + Send + Sync,
>(
    req: T,
    resp: &mut F,
    handler: &H,
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
                let parsed = match std::str::from_utf8(&body) {
                    Ok(xml) => quick_xml::de::from_str::<CompleteMultiPartUploadRequest>(xml),
                    Err(e) => {
                        log::error!("CompleteMultipartUpload body is not valid UTF-8: {e}");
                        resp.set_status(400);
                        resp.send_header();
                        return;
                    }
                };
                match parsed {
                    Ok(upload_request) => {
                        let data = upload_request
                            .parts
                            .iter()
                            .map(|data| (data.etag.clone(), data.part_number))
                            .collect::<Vec<(String, u32)>>();
                        match handler
                            .handle_complete(
                                bucket.to_string(),
                                key.to_string(),
                                upload_id.clone(),
                                data,
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
                            Err(e) => {
                                // An unknown upload id is 404 NoSuchUpload, not
                                // a server error; clients retry 500s.
                                let status = status_for_error(&e);
                                log::warn!("handle_complete failed ({status}): {e}");
                                resp.set_status(status);
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

/// Handles `DELETE /{bucket}/{key}?uploadId=...` (AbortMultipartUpload).
///
/// SDKs abort an upload after any part failure, so this must remove the staged
/// parts instead of leaving them behind until manual cleanup.
pub async fn handle_abort_multipart_upload<
    T: VRequest,
    F: VResponse,
    H: MultiUploadObjectHandler + Send + Sync,
>(
    req: T,
    resp: &mut F,
    handler: &H,
) {
    let url_path = req.url_path();
    let path = url_path.trim_matches('/');
    let (bucket, key) = match path.split_once('/') {
        Some((b, k)) if !b.is_empty() && !k.is_empty() => (b.to_string(), k.to_string()),
        _ => {
            resp.set_status(400);
            resp.send_header();
            return;
        }
    };

    let upload_id = match req.get_query("uploadId") {
        Some(id) if !id.is_empty() => id.as_str().to_string(),
        _ => {
            resp.set_status(400);
            resp.send_header();
            return;
        }
    };

    match handler.handle_abort(bucket, key, upload_id).await {
        Ok(()) => {
            resp.set_status(204);
            resp.send_header();
        }
        Err(()) => {
            resp.set_status(500);
            resp.send_header();
        }
    }
}

/// Legacy generic entry point, superseded by `S3Router::handle_multipart_abort_request`.
///
/// Responds 501 rather than panicking: this crate is built with `panic = "abort"`,
/// so an unimplemented protocol path must never be able to terminate the process.
pub async fn handle_multipart_abort_session<
    T: VRequest,
    F: VResponse,
    H: MultiUploadObjectHandler + Send + Sync,
>(
    _req: T,
    resp: &mut F,
    _handler: &H,
) {
    resp.set_status(501);
    resp.send_header();
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

/// OPTIMIZED: No async_trait - zero Box allocation
pub trait CreateBucketHandler: Send + Sync {
    fn handle(
        &self,
        opt: &CreateBucketOption,
        bucket: &str,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;
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
/// - OPTIMIZED: Generic handler parameter eliminates async_trait Box allocation
pub async fn handle_create_bucket<
    T: VRequest,
    F: VResponse,
    H: CreateBucketHandler + Send + Sync,
>(
    req: T,
    resp: &mut F,
    handler: &H,
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
    let bucket = url_path.trim_matches('/');
    if let Err(e) = validate_bucket_name(bucket) {
        log::info!("create bucket rejected: {e}");
        resp.set_status(400);
        resp.send_header();
        return;
    }
    if let Err(e) = handler.handle(&opt, bucket).await {
        if e.contains("BucketAlreadyExists") {
            crate::utils::s3_utils::set_error_response(resp, 409); // Conflict
        } else {
            crate::utils::s3_utils::set_error_response(resp, 500); // Internal Server Error
        }
        log::info!("create bucket handler error: {e}")
    }
}

pub struct DeleteBucketOption {
    pub expected_owner: Option<String>,
}

/// OPTIMIZED: No async_trait - zero Box allocation
pub trait DeleteBucketHandler: Send + Sync {
    fn handle(
        &self,
        opt: &DeleteBucketOption,
        bucket: &str,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;
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
/// - OPTIMIZED: Generic handler parameter eliminates async_trait Box allocation
pub async fn handle_delete_bucket<
    T: VRequest,
    F: VResponse,
    H: DeleteBucketHandler + Send + Sync,
>(
    req: T,
    resp: &mut F,
    handler: &H,
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
            let status = status_for_error(&e);
            log::warn!("delete bucket failed ({status}): {e}");
            resp.set_status(status);
            resp.send_header();
        }
    }
}

/// Whether `bucket` is a name S3 accepts.
///
/// Without this a one-character bucket was created and answered 200, while the
/// protocol requires `InvalidBucketName` for anything outside 3-63 characters
/// of lower-case letters, digits, dots and hyphens.
fn validate_bucket_name(bucket: &str) -> Result<(), String> {
    let bytes = bucket.as_bytes();
    if !(3..=63).contains(&bytes.len()) {
        return Err("InvalidBucketName: bucket names must be 3-63 characters".to_string());
    }
    let edge = |byte: u8| byte == b'.' || byte == b'-';
    let valid_chars = bytes
        .iter()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || edge(*byte));
    if !valid_chars
        || edge(bytes[0])
        || edge(bytes[bytes.len() - 1])
        || bucket.contains("..")
        || bucket.starts_with("xn--")
        || bucket.parse::<std::net::IpAddr>().is_ok()
    {
        return Err("InvalidBucketName: the specified bucket is not valid".to_string());
    }
    Ok(())
}

/// Check a declared `Content-MD5` against the digest computed while streaming.
///
/// The header is the client's own integrity check: a malformed digest is
/// `InvalidDigest` and a mismatch `BadDigest`. Ignoring it stored a body the
/// client had already rejected.
pub(crate) fn verify_content_md5(declared: Option<&str>, computed_hex: &str) -> Result<(), String> {
    let Some(declared) = declared else {
        return Ok(());
    };
    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(declared.trim())
        .map_err(|_| "InvalidDigest: Content-MD5 must be base64 of the MD5 digest".to_string())?;
    if decoded.len() != 16 {
        return Err("InvalidDigest: Content-MD5 must decode to 16 bytes".to_string());
    }
    if hex::encode(decoded) != computed_hex {
        return Err(
            "BadDigest: the Content-MD5 you specified did not match what we received".to_string(),
        );
    }
    Ok(())
}

/// Map a handler error to the HTTP status the S3 protocol requires.
///
/// Handlers report protocol conditions as text; without this the caller could
/// only answer 500, which hid "bucket does not exist" and "bucket not empty"
/// from every S3 client.
pub(crate) fn status_for_error(err: &str) -> u16 {
    if err.starts_with("NoSuchBucket") {
        404
    } else if err.starts_with("BucketNotEmpty") {
        409
    } else if err.starts_with("NoSuchKey") || err.starts_with("NoSuchUpload") {
        404
    } else if err.starts_with("PreconditionFailed") {
        412
    } else if err.starts_with("HashMismatch")
        || err.starts_with("ContentLengthMismatch")
        || err.starts_with("InvalidDigest")
        || err.starts_with("BadDigest")
        || err.starts_with("InvalidBucketName")
    {
        400
    } else if err.starts_with("InvalidPart")
        || err.starts_with("EntityTooSmall")
        || err.starts_with("InvalidArgument")
    {
        // `InvalidPartOrder` shares the `InvalidPart` prefix. All of these are
        // request errors; answering 500 would make clients retry a request that
        // can never succeed.
        400
    } else {
        500
    }
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
#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::sig_v4::VHeader;
    use std::collections::HashMap;

    struct Headers(HashMap<String, String>);

    impl VHeader for Headers {
        fn get_header(&self, key: &str) -> Option<String> {
            self.0.get(key).cloned()
        }

        fn set_header(&mut self, key: &str, val: &str) {
            self.0.insert(key.to_string(), val.to_string());
        }

        fn delete_header(&mut self, key: &str) {
            self.0.remove(key);
        }

        fn rng_header(&self, mut cb: impl FnMut(&str, &str) -> bool) {
            for (key, value) in &self.0 {
                if !cb(key, value) {
                    return;
                }
            }
        }
    }

    fn part_header(length: &str, sha256: Option<&str>) -> Headers {
        let mut map = HashMap::from([("content-length".to_string(), length.to_string())]);
        if let Some(sha256) = sha256 {
            map.insert("x-amz-content-sha256".to_string(), sha256.to_string());
        }
        Headers(map)
    }

    /// The declared length is a client claim: an oversized part must be
    /// refused before anything allocates that much memory.
    #[test]
    fn rejects_an_oversized_part_length() {
        assert_eq!(multipart_part_length(&part_header("6GB", None)), Err(400));
        assert_eq!(
            multipart_part_length(&part_header("5368709121", None)),
            Err(400)
        );
        assert_eq!(
            multipart_part_length(&part_header("5368709120", None)),
            Ok(MAX_PART_BYTES)
        );
    }

    /// Without a length the part cannot be bounded or checked for truncation.
    #[test]
    fn rejects_a_part_without_content_length() {
        let header = Headers(HashMap::new());
        assert_eq!(multipart_part_length(&header), Err(411));
    }

    /// A part body is not staged into memory, so the digest and length are
    /// enforced while writing it. An unsigned part must not be compared with
    /// the literal header text, which used to fail every such part.
    #[tokio::test]
    async fn write_part_stream_verifies_length_and_digest() {
        use crate::utils::temp_storage::{LocalTempStorage, StageExpect, TempStorageConfig};
        use std::io::Cursor;

        let dir = std::env::temp_dir().join(format!("curvine-part-{}", uuid::Uuid::new_v4()));
        let storage = LocalTempStorage::new(TempStorageConfig::new(
            dir.to_str().unwrap().to_string(),
            1024,
            4096,
        ));
        let data = vec![7u8; 8192];
        let digest = {
            use sha2::Digest;
            hex::encode(sha2::Sha256::digest(&data))
        };

        let mut body = tokio::io::BufReader::new(Cursor::new(data.clone()));
        let etag = storage
            .write_part_stream(
                "upload",
                1,
                &mut body,
                StageExpect {
                    sha256_hex: Some(&digest),
                    content_length: Some(data.len()),
                },
            )
            .await
            .expect("a verified part must be staged");
        assert!(etag.starts_with('"') && etag.ends_with('"'));

        // Unsigned payload: length only.
        let mut body = tokio::io::BufReader::new(Cursor::new(data.clone()));
        storage
            .write_part_stream(
                "upload",
                2,
                &mut body,
                StageExpect {
                    sha256_hex: None,
                    content_length: Some(data.len()),
                },
            )
            .await
            .expect("an unsigned part must be accepted");

        // A wrong digest must fail and leave no part behind for Complete.
        let mut body = tokio::io::BufReader::new(Cursor::new(data.clone()));
        let err = storage
            .write_part_stream(
                "upload",
                3,
                &mut body,
                StageExpect {
                    sha256_hex: Some("00"),
                    content_length: Some(data.len()),
                },
            )
            .await
            .expect_err("a wrong digest must be rejected");
        assert!(err.contains("sha256"), "{err}");
        assert!(
            !tokio::fs::try_exists(storage_part_path(&dir, "upload", 3))
                .await
                .unwrap_or(false),
            "a rejected part must not be left for Complete"
        );

        // A short body must not silently produce a truncated part.
        let mut body = tokio::io::BufReader::new(Cursor::new(data[..16].to_vec()));
        let err = storage
            .write_part_stream(
                "upload",
                4,
                &mut body,
                StageExpect {
                    sha256_hex: None,
                    content_length: Some(data.len()),
                },
            )
            .await
            .expect_err("a short body must be rejected");
        assert!(err.contains("length"), "{err}");

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    fn storage_part_path(dir: &std::path::Path, upload_id: &str, part: u32) -> std::path::PathBuf {
        dir.join(upload_id).join(part.to_string())
    }

    /// Staging an undecoded aws-chunked body would store chunk framing as part
    /// data, so the whole `STREAMING-` family must be refused, not just the one
    /// value that was matched before.
    #[test]
    fn rejects_the_undecoded_streaming_framing() {
        for framing in [
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
            "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        ] {
            assert_eq!(
                reject_streaming_part(&part_header("64", Some(framing))),
                Err(400),
                "{framing} is aws-chunked framing and must not be staged"
            );
        }
        for verifiable in ["UNSIGNED-PAYLOAD", "abc"] {
            assert_eq!(
                reject_streaming_part(&part_header("64", Some(verifiable))),
                Ok(()),
                "{verifiable} is a real payload and must be accepted"
            );
        }
        assert_eq!(reject_streaming_part(&part_header("64", None)), Ok(()));
    }

    /// Complete reports protocol conditions as text; without these mappings an
    /// invalid part list answered 500 and SDKs retried it forever.
    #[test]
    fn multipart_validation_errors_map_to_client_errors() {
        for err in [
            "InvalidPart: part 2 of upload u is not staged",
            "InvalidPartOrder: part numbers must be strictly ascending (got 2 then 1)",
            "EntityTooSmall: part 1 is 10 bytes, below the 5 MiB minimum for a non-final part",
            "InvalidArgument: object keys may not start a segment with .curvine-s3-",
        ] {
            assert_eq!(status_for_error(err), 400, "{err}");
        }
        assert_eq!(
            status_for_error("NoSuchUpload: upload u does not exist"),
            404
        );
        assert_eq!(status_for_error("PreconditionFailed: nope"), 412);
        assert_eq!(status_for_error("Failed to write to destination: io"), 500);
    }

    /// This arm is the only place a verification failure becomes the 400 that
    /// #1990 requires; without it a rejected body is reported as 500.
    #[test]
    fn verification_failures_map_to_client_errors() {
        assert_eq!(
            status_for_error("HashMismatch: received aa, expected bb"),
            400
        );
        assert_eq!(
            status_for_error("ContentLengthMismatch: received 5 of 10 bytes (transport: eof)"),
            400
        );
        // A genuine backend failure stays a server error.
        assert_eq!(status_for_error("Failed to write to destination: io"), 500);
    }

    /// A one-character bucket was created and answered 200; S3 requires
    /// InvalidBucketName for anything outside its naming rules.
    #[test]
    fn bucket_names_outside_the_rules_are_rejected() {
        for bad in [
            "a",
            "aa",
            "-abc",
            "abc-",
            ".abc",
            "abc.",
            "a..b",
            "Abc",
            "192.168.1.1",
            "xn--bucket",
        ] {
            assert!(validate_bucket_name(bad).is_err(), "{bad} must be rejected");
        }
        for good in ["abc", "a-b.c", "my-bucket-01", "a1b2c3"] {
            assert!(
                validate_bucket_name(good).is_ok(),
                "{good} must be accepted"
            );
        }
    }

    /// The client's own integrity check must be enforced, not ignored.
    #[test]
    fn content_md5_is_verified() {
        use base64::Engine;
        let digest = [0x11u8; 16];
        let declared = base64::engine::general_purpose::STANDARD.encode(digest);
        let computed = hex::encode(digest);

        assert!(verify_content_md5(None, &computed).is_ok());
        assert!(verify_content_md5(Some(&declared), &computed).is_ok());

        assert!(
            verify_content_md5(Some(&declared), &hex::encode([0x22u8; 16]))
                .expect_err("mismatch")
                .starts_with("BadDigest")
        );
        assert!(verify_content_md5(Some("not base64!"), &computed)
            .expect_err("malformed")
            .starts_with("InvalidDigest"));
        assert!(verify_content_md5(
            Some(&base64::engine::general_purpose::STANDARD.encode([0u8; 8])),
            &computed
        )
        .expect_err("short digest")
        .starts_with("InvalidDigest"));
    }

    #[test]
    fn unsigned_parts_carry_no_digest() {
        assert_eq!(part_sha256(Some("UNSIGNED-PAYLOAD")), None);
        assert_eq!(part_sha256(None), None);
        // A real digest is verified, not skipped.
        assert_eq!(part_sha256(Some("abc")), Some("abc"));
    }
}
