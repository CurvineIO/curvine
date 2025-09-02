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

//! File converter utilities for S3 API
//! Converts Curvine FileStatus to S3 object representations

use crate::s3::s3_api::{HeadObjectResult, ListObjectContent, Owner};
use curvine_common::state::{FileStatus, StorageType, TtlAction};
use std::collections::HashMap;

pub fn format_s3_timestamp(timestamp_ms: i64) -> Option<String> {
    chrono::DateTime::from_timestamp(timestamp_ms / 1000, 0)
        .map(|dt| dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
}

/// Map Curvine storage type to S3 storage class
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
pub fn file_status_to_head_object_result(
    file_status: &FileStatus,
    object_name: &str,
) -> HeadObjectResult {
    let mut head = HeadObjectResult {
        content_length: Some(file_status.len as usize),
        last_modified: format_s3_timestamp(file_status.mtime),
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
