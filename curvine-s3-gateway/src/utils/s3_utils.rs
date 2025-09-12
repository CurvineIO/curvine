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

//! S3 API utilities
//!
//! This module provides utility functions for S3 API operations including:
//! - File converter utilities for S3 API
//! - Response header generation
//! - S3-compatible identifiers

use crate::s3::s3_api::{HeadObjectResult, ListObjectContent, Owner};
use curvine_common::state::{FileStatus, StorageType, TtlAction};
use std::collections::HashMap;

pub fn format_s3_timestamp(timestamp_ms: i64) -> Option<String> {
    chrono::DateTime::from_timestamp(timestamp_ms / 1000, 0)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string())
}

pub fn format_http_timestamp(timestamp_ms: i64) -> Option<String> {
    chrono::DateTime::from_timestamp(timestamp_ms / 1000, 0)
        .map(|dt| dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
}

pub fn map_storage_class(storage_type: &StorageType) -> String {
    match storage_type.as_str_name() {
        "MEM" => "REDUCED_REDUNDANCY",
        "SSD" => "STANDARD_IA",
        "HDD" => "STANDARD",
        "UFS" => "GLACIER",
        "DISK" => "STANDARD",
        _ => "STANDARD",
    }
    .to_string()
}

/// Infer content type from file extension
pub fn infer_content_type(object_name: &str) -> String {
    match object_name.rsplit('.').next() {
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("png") => "image/png",
        Some("gif") => "image/gif",
        Some("pdf") => "application/pdf",
        Some("txt") => "text/plain",
        Some("html") => "text/html",
        Some("json") => "application/json",
        Some("xml") => "application/xml",
        Some("zip") => "application/zip",
        Some("mp4") => "video/mp4",
        Some("mp3") => "audio/mpeg",
        _ => "application/octet-stream",
    }
    .to_string()
}

/// Generate ETag from file status
pub fn generate_etag(file_status: &FileStatus) -> String {
    format!("\"{:x}-{:x}\"", file_status.id, file_status.mtime)
}

/// Create owner information from file status
pub fn create_owner_info(file_status: &FileStatus) -> Owner {
    Owner {
        id: file_status.owner.clone(),
        display_name: file_status.owner.clone(),
    }
}

/// Create metadata map from file status
pub fn create_metadata_map(file_status: &FileStatus) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    metadata.insert("curvine-file-id".to_string(), file_status.id.to_string());
    metadata.insert(
        "curvine-replicas".to_string(),
        file_status.replicas.to_string(),
    );
    metadata.insert(
        "curvine-block-size".to_string(),
        file_status.block_size.to_string(),
    );
    metadata.insert("curvine-owner".to_string(), file_status.owner.clone());
    metadata.insert("curvine-group".to_string(), file_status.group.clone());
    metadata.insert(
        "curvine-mode".to_string(),
        format!("{:o}", file_status.mode),
    );
    metadata.insert(
        "curvine-complete".to_string(),
        file_status.is_complete.to_string(),
    );

    // Add atime if different from mtime
    if file_status.atime != file_status.mtime {
        if let Some(atime_str) =
            chrono::DateTime::from_timestamp(file_status.atime / 1000, 0).map(|dt| dt.to_rfc3339())
        {
            metadata.insert("curvine-atime".to_string(), atime_str);
        }
    }

    // Add UFS mtime if available and different
    if file_status.storage_policy.ufs_mtime > 0
        && file_status.storage_policy.ufs_mtime != file_status.mtime
    {
        if let Some(ufs_mtime_str) =
            chrono::DateTime::from_timestamp(file_status.storage_policy.ufs_mtime / 1000, 0)
                .map(|dt| dt.to_rfc3339())
        {
            metadata.insert("curvine-ufs-mtime".to_string(), ufs_mtime_str);
        }
    }

    // Add custom attributes from x_attr
    for (key, value) in &file_status.x_attr {
        if let Ok(value_str) = String::from_utf8(value.clone()) {
            metadata.insert(
                format!("{}{}", "x-amz-meta-", key.to_lowercase()),
                value_str,
            );
        }
    }

    metadata
}

/// Fill TTL/Expiration information for HeadObjectResult
pub fn fill_ttl_info(head: &mut HeadObjectResult, file_status: &FileStatus) {
    if file_status.storage_policy.ttl_ms > 0 {
        let expires_timestamp = file_status.atime + file_status.storage_policy.ttl_ms;
        head.expires =
            chrono::DateTime::from_timestamp(expires_timestamp / 1000, 0).map(|dt| dt.to_rfc3339());

        // Set expiration rule info
        head.expiration = Some(
            match file_status.storage_policy.ttl_action {
                TtlAction::Delete => "expiry-date=\"{}\" rule-id=\"ttl-delete\"",
                TtlAction::Move => "expiry-date=\"{}\" rule-id=\"ttl-move\"",
                TtlAction::Ufs => "expiry-date=\"{}\" rule-id=\"ttl-ufs\"",
                _ => "rule-id=\"no-expiry\"",
            }
            .to_string(),
        );
    }
}

/// Convert FileStatus to complete HeadObjectResult
/// Note: For HTTP headers, use format_http_timestamp() to convert the last_modified field
pub fn file_status_to_head_object_result(
    file_status: &FileStatus,
    object_name: &str,
) -> HeadObjectResult {
    let mut head = HeadObjectResult {
        content_length: Some(file_status.len as usize),
        // Use HTTP timestamp format for Last-Modified header
        last_modified: format_http_timestamp(file_status.mtime),
        accept_ranges: Some("bytes".to_string()),
        etag: Some(generate_etag(file_status)),
        storage_class: Some(map_storage_class(&file_status.storage_policy.storage_type)),
        content_type: Some(infer_content_type(object_name)),
        ..Default::default()
    };

    // TTL/Expiration information
    fill_ttl_info(&mut head, file_status);

    // Metadata from file attributes and properties
    head.metadata = Some(create_metadata_map(file_status));

    head
}

/// Convert FileStatus to ListObjectContent
pub fn file_status_to_list_object_content(
    file_status: &FileStatus,
    key: String,
) -> ListObjectContent {
    ListObjectContent {
        key,
        last_modified: format_s3_timestamp(file_status.mtime),
        etag: Some(generate_etag(file_status)),
        size: file_status.len as u64,
        storage_class: Some(map_storage_class(&file_status.storage_policy.storage_type)),
        owner: Some(create_owner_info(file_status)),
    }
}

// S3 Response Utilities

/// Generate a unique request ID for S3 API responses
///
/// Creates a unique identifier for each request, used in x-amz-request-id header.
/// Format: 16-character hexadecimal string based on timestamp and thread ID.
pub fn generate_request_id() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    std::time::SystemTime::now().hash(&mut hasher);
    std::thread::current().id().hash(&mut hasher);
    format!("{:016X}", hasher.finish())
}

/// Generate a stable host ID for S3 API responses
///
/// Creates a consistent identifier for the gateway instance, used in x-amz-id-2 header.
/// The host ID remains stable throughout the gateway process lifetime but is unique
/// per process instance.
///
/// Format: 32-character hexadecimal string based on stable identifiers:
/// - Service name ("curvine-s3-gateway")
/// - Hostname (from environment variables)
/// - Process ID (for uniqueness within same host)
pub fn generate_host_id() -> String {
    use std::sync::OnceLock;

    static HOST_ID: OnceLock<String> = OnceLock::new();
    HOST_ID
        .get_or_init(|| {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            let mut hasher = DefaultHasher::new();

            // Use stable identifiers for consistent host ID
            "curvine-s3-gateway".hash(&mut hasher);

            // Try to get hostname, fallback to localhost
            let hostname = std::env::var("HOSTNAME")
                .or_else(|_| std::env::var("COMPUTERNAME"))
                .unwrap_or_else(|_| "localhost".to_string());
            hostname.hash(&mut hasher);

            // Use process ID for uniqueness within same host
            std::process::id().hash(&mut hasher);

            let hash = hasher.finish();

            // Generate a base64-like string similar to AWS S3 host IDs
            format!(
                "{:016X}{:016X}",
                hash,
                hash.wrapping_mul(0x9e3779b97f4a7c15)
            )
        })
        .clone()
}

/// Set error status with S3-standard headers (request-id and host-id)
///
/// Helper function to set HTTP status code along with required S3 response headers.
/// Automatically adds:
/// - x-amz-request-id: Unique request identifier
/// - x-amz-id-2: Stable host identifier  
/// - content-type: application/xml (for error responses)
///
/// # Arguments
/// * `resp` - Mutable reference to response object implementing VResponse
/// * `status` - HTTP status code to set
pub fn set_error_response<F: crate::s3::s3_api::VResponse>(resp: &mut F, status: u16) {
    resp.set_status(status);
    resp.set_header("x-amz-request-id", &generate_request_id());
    resp.set_header("x-amz-id-2", &generate_host_id());
    if status >= 400 {
        resp.set_header("content-type", "application/xml");
    }
}
