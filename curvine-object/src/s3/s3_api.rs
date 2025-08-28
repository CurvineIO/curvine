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

static OWNER_ID: &str = "ffffffffffffffff";
pub type DateTime = chrono::DateTime<chrono::Utc>;

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
    fn get_body_writer<'b>(
        &'b mut self,
    ) -> std::pin::Pin<
        Box<dyn 'b + Send + std::future::Future<Output = Result<Self::BodyWriter<'b>, String>>>,
    >;
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
    pub archive_status: Option<String>, // 可用枚举替代
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
    pub expires: Option<String>, // 原是 `time.Time`，可转换为 ISO8601 字符串
    #[serde(rename = "ExpiresString")]
    pub expires_string: Option<String>,
    #[serde(rename = "LastModified")]
    pub last_modified: Option<String>, // 可考虑使用 chrono::DateTime 类型
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

#[async_trait::async_trait]
pub trait HeadHandler {
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

pub trait ListObjectHandler {
    fn handle<'a>(
        &'a self,
        opt: &'a ListObjectOption,
        bucket: &'a str,
    ) -> std::pin::Pin<
        Box<dyn 'a + Send + std::future::Future<Output = Result<Vec<ListObjectContent>, String>>>,
    >;
}

pub fn handle_head_object<T: VRequest, F: VResponse, E: HeadHandler>(
    _req: &T,
    _resp: &mut F,
    _handler: &E,
) {
    todo!()
}

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
    let r = raw.find('/');
    if r.is_none() {
        resp.set_status(400);
        resp.send_header();
        return;
    }

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
            if bytes.starts_with('-') {
                // Handle suffix-byte-range-spec: bytes=-N (last N bytes)
                if let Ok(suffix_len) = bytes[1..].parse::<u64>() {
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

    let next = r.unwrap();
    let bucket = &raw[..next];
    let object = &raw[next + 1..];

    let head = handler.lookup(bucket, object).await;
    if let Err(e) = head {
        log::error!("lookup {bucket} {object} error: {e}");
        resp.set_status(500);
        resp.send_header();
        return;
    }
    let head = head.unwrap();
    if head.is_none() {
        log::info!("not found {bucket} {object}");
        resp.set_status(404);
        resp.send_header();
        return;
    }
    //send header info to client
    let head = head.unwrap();
    let total_len = head.content_length.unwrap_or(0) as u64;

    // Default: full content
    let mut status = 200u16;
    let mut resp_len = total_len;
    let mut header_last_modified = head.last_modified.clone();
    let mut header_etag = head.etag.clone();
    let mut header_ct = head.content_type.clone();

    // If range requested, validate and compute
    if opt.range_start.is_some() || opt.range_end.is_some() {
        let (start, end) = if let Some(range_end) = opt.range_end {
            if range_end > u64::MAX - 1000000 {
                // This is a suffix-byte-range-spec (bytes=-N)
                let suffix_len = u64::MAX - range_end;
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
        resp.set_header(
            "content-range",
            &format!("bytes {}-{}/{}", start, end, total_len),
        );
    }

    // Apply headers
    resp.set_header("content-length", resp_len.to_string().as_str());
    if let Some(v) = header_etag.take() {
        resp.set_header("etag", &v)
    }
    if let Some(v) = header_ct.take() {
        resp.set_header("content-type", &v)
    }
    if let Some(v) = header_last_modified.take() {
        resp.set_header("last-modified", &v)
    }
    // Fix for leading zeros: force connection close for GET responses
    resp.set_header("Connection", "close");
    //
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

//query list-type=2, return ListObjectResult
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
                    log::debug!("ListObjectsV2 XML => {}", data);
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
                        return;
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

pub trait ListBucketHandler {
    fn handle<'a>(
        &'a self,
        opt: &'a ListBucketsOption,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<Vec<Bucket>, String>>>>;
}
pub trait GetBucketLocationHandler {
    fn handle<'a>(
        &'a self,
        _loc: Option<&'a str>,
    ) -> std::pin::Pin<
        Box<dyn 'a + Send + std::future::Future<Output = Result<Option<&'static str>, ()>>>,
    > {
        Box::pin(async { Ok(Some("us-west-1")) })
    }
}
#[derive(Debug)]
pub struct ListBucketsOption {
    pub bucket_region: Option<String>,
    pub continuation_token: Option<String>,
    pub max_buckets: Option<i32>,
    pub prefix: Option<String>,
}
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
        if self.content_length.is_none() {
            return false;
        } else if self.content_md5.is_none() {
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
            _ => Err(format!("Invalid checksum algorithm: {}", s)),
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
    let url_path = req.url_path();
    let url_path = url_path.trim_matches('/');
    let ret = url_path.find('/');
    if ret.is_none() {
        resp.set_status(400);
        resp.send_header();
        return;
    }
    let next = ret.unwrap();
    let bucket = &url_path[..next];
    let object = &url_path[next + 1..];
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
        || None,
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
    if content_sha256.is_none() {
        resp.set_status(403);
        return;
    }
    let content_sha256 = content_sha256.unwrap();
    let ret = req.get_body_reader().await;
    if let Err(err) = ret {
        resp.set_status(500);
        resp.send_header();
        log::error!("get body reader error: {err}");
        return;
    }
    let r = ret.unwrap();
    let ret: Result<(), String> = match content_sha256 {
        ContentSha256::Hash(cs) => {
            if opt.content_length.is_none() {
                resp.set_status(403);
                resp.send_header();
                return;
            }
            let content_length = opt.content_length.unwrap() as usize;
            if content_length <= 10 << 20 {
                match read_body_to_vec(r, &cs, content_length).await {
                    Ok(vec) => {
                        let mut reader = InMemoryPollReader {
                            data: vec,
                            offset: 0,
                        };
                        handler.handle(&opt, bucket, object, &mut reader).await
                    }
                    Err(err) => match err {
                        ParseBodyError::HashNoMatch => {
                            log::warn!("put object hash not match");
                            resp.set_status(400);
                            resp.send_header();
                            return;
                        }
                        ParseBodyError::ContentLengthIncorrect => {
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
                    },
                }
            } else {
                match tokio::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .read(true)
                    .mode(0o644)
                    .open(format!(".sys_bws/{}", cs))
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
                            ParseBodyError::HashNoMatch => {
                                log::warn!("put object hash not match");
                                resp.set_status(400);
                                resp.send_header();
                                return;
                            }
                            ParseBodyError::ContentLengthIncorrect => {
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
            let file_name = format!(".sys_bws/{}", file_name);
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
                tokio::fs::remove_file(file_name.as_str())
                    .await
                    .unwrap_or_else(|err| log::error!("remove file {file_name} error {err}"));
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
                    tokio::fs::remove_file(file_name.as_str())
                        .await
                        .unwrap_or_else(|err| log::error!("remove file {file_name} error {err}"));
                    ret
                }
                Err(err) => {
                    log::error!("open file {file_name} error {err}");
                    resp.set_status(500);
                    resp.send_header();
                    tokio::fs::remove_file(file_name.as_str())
                        .await
                        .unwrap_or_else(|err| log::error!("remove file {file_name} error {err}"));
                    return;
                }
            }
        }
        ContentSha256::Unsigned => {
            // No hash verification; stream body into a temp file, then pass to handler
            let file_name = uuid::Uuid::new_v4().to_string()[..8].to_string();
            let file_name = format!(".sys_bws/{}", file_name);
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

    if upload_id.is_none() || part_number.is_none() {
        resp.set_status(400);
    } else {
        let ret = u32::from_str_radix(part_number.unwrap().as_str(), 10);
        if ret.is_err() {
            resp.set_status(400);
            return;
        }
        let part_number = ret.unwrap();
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
                    .handle_upload_part(
                        raw[0],
                        raw[1],
                        upload_id.unwrap().as_str(),
                        part_number,
                        &mut file,
                    )
                    .await
            }
            StreamType::Buff(mut buf_reader) => {
                handler
                    .handle_upload_part(
                        raw[0],
                        raw[1],
                        upload_id.unwrap().as_str(),
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
#[derive(Debug)]
enum ParseBodyError {
    HashNoMatch,
    ContentLengthIncorrect,
    Io(String),
}
impl Display for ParseBodyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ParseBodyError::HashNoMatch => "hash no match",
            ParseBodyError::ContentLengthIncorrect => "content length incorrect",
            ParseBodyError::Io(err) => err.as_str(),
        })
    }
}
impl std::error::Error for ParseBodyError {}
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
        .ok_or(ParseBodyError::HashNoMatch)?;
    if let Some(cl) = cl {
        let cl = cl
            .as_str()
            .parse::<usize>()
            .or(Err(ParseBodyError::ContentLengthIncorrect))?;
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
                        return Err(ParseBodyError::ContentLengthIncorrect);
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
                        return Err(ParseBodyError::HashNoMatch);
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
                    ".sys_bws/{}",
                    uuid::Uuid::new_v4().to_string()[..8].to_string()
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
    mut src: T,
    dst: &mut E,
    content_sha256: &str,
    mut content_length: usize,
) -> Result<(), ParseBodyError> {
    use tokio::io::AsyncWriteExt;
    let mut hsh = sha2::Sha256::new();
    // if content length > 10MB, it will store on disk instead memory
    while let Some(buff) = src.poll_read().await.map_err(ParseBodyError::Io)? {
        let buff_len = buff.len();
        if content_length < buff_len {
            return Err(ParseBodyError::ContentLengthIncorrect);
        }
        content_length -= buff_len;
        let _ = hsh.write_all(&buff);
        dst.write_all(&buff)
            .await
            .map_err(|err| ParseBodyError::Io(format!("write error {err}")))?;
    }
    let ret = hsh.finalize();
    let real_sha256 = hex::encode(ret);
    if real_sha256.as_str() != content_sha256 {
        Err(ParseBodyError::HashNoMatch)
    } else {
        Ok(())
    }
}

impl crate::utils::io::PollRead for tokio::io::BufReader<std::io::Cursor<Vec<u8>>> {
    fn poll_read<'a>(
        &'a mut self,
    ) -> std::pin::Pin<
        Box<dyn 'a + Send + std::future::Future<Output = Result<Option<Vec<u8>>, String>>>,
    > {
        Box::pin(async move {
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
        })
    }
}

impl crate::utils::io::PollRead for tokio::fs::File {
    fn poll_read<'a>(
        &'a mut self,
    ) -> std::pin::Pin<
        Box<dyn 'a + Send + std::future::Future<Output = Result<Option<Vec<u8>>, String>>>,
    > {
        Box::pin(async move {
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
        })
    }
}
struct InMemoryPollReader {
    data: Vec<u8>,
    offset: usize,
}

impl crate::utils::io::PollRead for InMemoryPollReader {
    fn poll_read<'a>(
        &'a mut self,
    ) -> std::pin::Pin<
        Box<dyn 'a + Send + std::future::Future<Output = Result<Option<Vec<u8>>, String>>>,
    > {
        Box::pin(async move {
            if self.offset >= self.data.len() {
                return Ok(None);
            }
            let remaining = self.data.len() - self.offset;
            // read up to 64KB per poll
            let to_read = remaining.min(64 * 1024);
            let chunk = self.data[self.offset..self.offset + to_read].to_vec();
            self.offset += to_read;
            Ok(Some(chunk))
        })
    }
}

// Helper: read entire body into Vec<u8> and verify sha256
async fn read_body_to_vec<T: crate::utils::io::PollRead + Send>(
    mut src: T,
    expected_sha256: &str,
    mut content_length: usize,
) -> Result<Vec<u8>, ParseBodyError> {
    use bytes::BytesMut;
    let mut hasher = sha2::Sha256::new();
    let mut out = BytesMut::with_capacity(content_length);
    while let Some(buff) = src.poll_read().await.map_err(ParseBodyError::Io)? {
        let buff_len = buff.len();
        if content_length < buff_len {
            return Err(ParseBodyError::ContentLengthIncorrect);
        }
        content_length -= buff_len;
        let _ = std::io::Write::write_all(&mut hasher, &buff);
        out.extend_from_slice(&buff);
    }
    let real_sha256 = hex::encode(hasher.finalize());
    if real_sha256.as_str() != expected_sha256 {
        Err(ParseBodyError::HashNoMatch)
    } else {
        Ok(out.freeze().to_vec())
    }
}
