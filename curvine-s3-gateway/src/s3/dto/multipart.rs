// Copyright 2025 OPPO.
// Licensed under the Apache License, Version 2.0

//! Multipart upload related S3 DTOs

use serde::{Deserialize, Serialize};

use super::common::Owner;

/// Initiate Multipart Upload result
#[derive(Debug, Serialize)]
#[serde(rename = "InitiateMultipartUploadResult")]
#[serde(rename_all = "PascalCase")]
pub struct InitiateMultipartUploadResult {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
}

/// Upload Part result
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct UploadPartResult {
    pub etag: String,
    pub checksum_crc32: Option<String>,
    pub checksum_crc32c: Option<String>,
    pub checksum_sha1: Option<String>,
    pub checksum_sha256: Option<String>,
}

/// Complete Multipart Upload request body
#[derive(Debug, Deserialize)]
#[serde(rename = "CompleteMultipartUpload")]
pub struct CompleteMultipartUploadRequest {
    #[serde(rename = "Part", default)]
    pub parts: Vec<CompletedPart>,
}

/// Completed part info
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CompletedPart {
    pub part_number: u32,
    #[serde(rename = "ETag")]
    pub etag: String,
    pub checksum_crc32: Option<String>,
    pub checksum_crc32c: Option<String>,
    pub checksum_sha1: Option<String>,
    pub checksum_sha256: Option<String>,
}

/// Complete Multipart Upload result
#[derive(Debug, Serialize)]
#[serde(rename = "CompleteMultipartUploadResult")]
#[serde(rename_all = "PascalCase")]
pub struct CompleteMultipartUploadResult {
    pub location: String,
    pub bucket: String,
    pub key: String,
    #[serde(rename = "ETag")]
    pub etag: String,
    pub checksum_crc32: Option<String>,
    pub checksum_crc32c: Option<String>,
    pub checksum_sha1: Option<String>,
    pub checksum_sha256: Option<String>,
}

/// Complete Multipart Upload options
#[derive(Debug, Default)]
pub struct MultiUploadObjectCompleteOption {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
}

/// List Multipart Uploads result
#[derive(Debug, Serialize)]
#[serde(rename = "ListMultipartUploadsResult")]
#[serde(rename_all = "PascalCase")]
pub struct ListMultipartUploadsResult {
    pub bucket: String,
    pub key_marker: Option<String>,
    pub upload_id_marker: Option<String>,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<String>,
    pub max_uploads: u32,
    pub is_truncated: bool,
    #[serde(rename = "Upload", default)]
    pub uploads: Vec<MultipartUpload>,
}

/// Multipart upload info
#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct MultipartUpload {
    pub key: String,
    pub upload_id: String,
    pub initiator: Option<Owner>,
    pub owner: Option<Owner>,
    pub storage_class: Option<String>,
    pub initiated: String,
}

/// List Parts result
#[derive(Debug, Serialize)]
#[serde(rename = "ListPartsResult")]
#[serde(rename_all = "PascalCase")]
pub struct ListPartsResult {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub part_number_marker: Option<u32>,
    pub next_part_number_marker: Option<u32>,
    pub max_parts: u32,
    pub is_truncated: bool,
    #[serde(rename = "Part", default)]
    pub parts: Vec<Part>,
}

/// Part info
#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Part {
    pub part_number: u32,
    pub last_modified: String,
    #[serde(rename = "ETag")]
    pub etag: String,
    pub size: u64,
    pub checksum_crc32: Option<String>,
    pub checksum_crc32c: Option<String>,
    pub checksum_sha1: Option<String>,
    pub checksum_sha256: Option<String>,
}

/// Copy Part result
#[derive(Debug, Serialize)]
#[serde(rename = "CopyPartResult")]
#[serde(rename_all = "PascalCase")]
pub struct CopyPartResult {
    #[serde(rename = "ETag")]
    pub etag: String,
    pub last_modified: String,
    pub checksum_crc32: Option<String>,
    pub checksum_crc32c: Option<String>,
    pub checksum_sha1: Option<String>,
    pub checksum_sha256: Option<String>,
}
