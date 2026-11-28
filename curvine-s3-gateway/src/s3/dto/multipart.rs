// Copyright 2025 OPPO.
// Licensed under the Apache License, Version 2.0

//! Multipart upload related S3 DTOs

use serde::{Deserialize, Serialize};

use super::common::{CommonPrefix, Owner};

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
    #[serde(rename = "@xmlns")]
    pub xmlns: &'static str,
    pub bucket: String,
    pub key_marker: String,
    pub upload_id_marker: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_key_marker: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_upload_id_marker: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding_type: Option<String>,
    pub max_uploads: u32,
    pub is_truncated: bool,
    #[serde(rename = "Upload", skip_serializing_if = "Vec::is_empty")]
    pub uploads: Vec<MultipartUpload>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub common_prefixes: Vec<CommonPrefix>,
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
    #[serde(rename = "@xmlns")]
    pub xmlns: String,
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub initiator: Owner,
    pub owner: Owner,
    pub storage_class: String,
    pub part_number_marker: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_part_number_marker: Option<u32>,
    pub max_parts: u32,
    pub is_truncated: bool,
    #[serde(rename = "Part", default, skip_serializing_if = "Vec::is_empty")]
    pub parts: Vec<Part>,
}

/// Part info
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Part {
    pub part_number: u32,
    pub last_modified: String,
    #[serde(rename = "ETag")]
    pub etag: String,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum_crc32: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum_crc32c: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum_sha1: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum_sha256: Option<String>,
}

impl Part {
    pub fn staged(part_number: u32, etag_hex: &str, size: usize, last_modified: String) -> Self {
        Self {
            part_number,
            last_modified,
            etag: format!("\"{etag_hex}\""),
            size: size as u64,
            checksum_crc32: None,
            checksum_crc32c: None,
            checksum_sha1: None,
            checksum_sha256: None,
        }
    }
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
