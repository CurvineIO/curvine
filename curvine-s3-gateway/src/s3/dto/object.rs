// Copyright 2025 OPPO.
// Licensed under the Apache License, Version 2.0

//! Object-related S3 DTOs

use serde::Serialize;
use std::collections::HashMap;

use super::common::{
    ArchiveStatus, ChecksumAlgorithm, CommonPrefix, DateTime, ObjectLockLegalHoldStatus,
    ObjectLockMode, Owner, RequestPayer, S3_NAMESPACE,
};

/// HEAD Object response
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

/// GET Object options
#[derive(Default, Debug, Clone)]
pub struct GetObjectOption {
    pub range_start: Option<u64>,
    pub range_end: Option<u64>,
}

/// PUT Object options
#[derive(Default, Clone, Debug)]
pub struct PutObjectOption {
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
    pub object_lock_legal_hold_status: Option<ObjectLockLegalHoldStatus>,
    pub object_lock_mode: Option<ObjectLockMode>,
    pub object_lock_retain_until_date: Option<DateTime>,
    pub request_payer: Option<RequestPayer>,
    pub storage_class: Option<String>,
    pub write_offset_bytes: Option<i64>,
}

impl PutObjectOption {
    /// Check if required fields are present
    pub fn is_valid(&self) -> bool {
        self.content_length.is_some()
    }
}

/// PUT Object result
#[derive(Default, Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutObjectResult {
    pub etag: Option<String>,
    pub version_id: Option<String>,
    pub checksum_crc32: Option<String>,
    pub checksum_crc32c: Option<String>,
    pub checksum_sha1: Option<String>,
    pub checksum_sha256: Option<String>,
}

/// List Objects V2 result
#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
#[serde(rename = "ListBucketResult")]
pub struct ListObjectResult {
    #[serde(rename = "xmlns", skip_serializing_if = "String::is_empty")]
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

impl Default for ListObjectResult {
    fn default() -> Self {
        Self {
            xmlns: S3_NAMESPACE.to_string(),
            name: String::new(),
            prefix: None,
            key_count: None,
            max_keys: None,
            delimiter: None,
            is_truncated: false,
            contents: Vec::new(),
            common_prefixes: Vec::new(),
        }
    }
}

/// Single object in list result
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListObjectContent {
    pub key: String,
    pub last_modified: Option<String>,
    pub etag: Option<String>,
    pub size: u64,
    pub storage_class: Option<String>,
    pub owner: Option<Owner>,
}

/// List Objects options
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListObjectOption {
    pub bucket: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub continuation_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_bucket_owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fetch_owner: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_keys: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional_object_attributes: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_payer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_after: Option<String>,
}

/// List Object Versions result
#[derive(Debug, Serialize)]
#[serde(rename = "ListVersionsResult")]
pub struct ListObjectVersionsResult {
    #[serde(rename = "xmlns", skip_serializing_if = "String::is_empty")]
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

/// Object version info
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

/// Delete marker info
#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteMarker {
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
    pub last_modified: String,
    pub owner: Option<Owner>,
}

/// List Object Versions options
#[derive(Debug, Clone, Default)]
pub struct ListObjectVersionsOption {
    pub bucket: String,
    pub delimiter: Option<String>,
    pub encoding_type: Option<String>,
    pub key_marker: Option<String>,
    pub max_keys: Option<i32>,
    pub prefix: Option<String>,
    pub version_id_marker: Option<String>,
}

/// Delete Object result
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteObjectResult {
    pub delete_marker: Option<bool>,
    pub version_id: Option<String>,
}

/// Delete Objects (batch) request
#[derive(Debug, Clone)]
pub struct DeleteObjectsRequest {
    pub bucket: String,
    pub objects: Vec<ObjectIdentifier>,
    pub quiet: bool,
}

/// Object identifier for delete operations
#[derive(Debug, Clone)]
pub struct ObjectIdentifier {
    pub key: String,
    pub version_id: Option<String>,
}

/// Delete Objects result
#[derive(Debug, Serialize)]
#[serde(rename = "DeleteResult")]
pub struct DeleteObjectsResult {
    #[serde(rename = "Deleted", default)]
    pub deleted: Vec<DeletedObject>,
    #[serde(rename = "Error", default)]
    pub errors: Vec<DeleteError>,
}

/// Successfully deleted object
#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeletedObject {
    pub key: String,
    pub version_id: Option<String>,
    pub delete_marker: Option<bool>,
    pub delete_marker_version_id: Option<String>,
}

/// Delete error
#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteError {
    pub key: String,
    pub version_id: Option<String>,
    pub code: String,
    pub message: String,
}
