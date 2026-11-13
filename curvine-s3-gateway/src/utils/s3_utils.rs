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
use curvine_model::{FileStatus, StorageType, TtlAction};

pub fn format_s3_timestamp(timestamp_ms: i64) -> Option<String> {
    chrono::DateTime::from_timestamp(timestamp_ms / 1000, 0)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string())
}

/// Format a millisecond timestamp as an HTTP-date.
///
/// `from_timestamp` takes (seconds, nanoseconds), so the sub-second part is
/// carried through instead of being discarded. Dropping it made the emitted
/// `Last-Modified` earlier than the real mtime, so a client echoing it back in
/// `If-Unmodified-Since` failed its own precondition.
pub fn format_http_timestamp(timestamp_ms: i64) -> Option<String> {
    let secs = timestamp_ms.div_euclid(1000);
    let nanos = (timestamp_ms.rem_euclid(1000) * 1_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, nanos)
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
/// S3 ETag for an object.
///
/// S3 clients treat the ETag as a content identifier: two objects with equal
/// bytes must share one, and changing the bytes must change it. The previous
/// value was `"{id:x}-{mtime:x}"`, which broke both properties (`s3fs`,
/// `rclone` and incremental sync all rely on it).
///
/// A multipart ETag is `md5(concat(part-md5s))-N`, which cannot be derived from
/// a completed object without its part boundaries, so the stored digest is used
/// verbatim when present. Otherwise the ETag falls back to the object id, which
/// is at least stable for a given object.
pub fn generate_etag(file_status: &FileStatus) -> String {
    match file_status.x_attr.get(XATTR_S3_ETAG) {
        Some(digest) => format!("\"{}\"", String::from_utf8_lossy(digest)),
        None => format!("\"{:x}\"", file_status.id),
    }
}

/// S3 metadata keys stored in the object's extended attributes.
///
/// Extended attributes are the only per-object key/value store that survives a
/// commit without a proto change, so the gateway keeps the S3-visible metadata
/// there instead of dropping it.
pub const XATTR_S3_ETAG: &str = "s3.etag";
pub const XATTR_S3_CONTENT_TYPE: &str = "s3.content_type";
/// User metadata is stored one key per attribute with this prefix.
pub const XATTR_S3_META_PREFIX: &str = "s3.meta.";

/// Content type recorded by the upload, or a guess from the key.
pub fn effective_content_type(file_status: &FileStatus, object_name: &str) -> String {
    match file_status.x_attr.get(XATTR_S3_CONTENT_TYPE) {
        Some(value) => String::from_utf8_lossy(value).to_string(),
        None => infer_content_type(object_name),
    }
}

/// User metadata supplied on upload, in S3 `x-amz-meta-*` form.
pub fn user_metadata(file_status: &FileStatus) -> std::collections::BTreeMap<String, String> {
    let mut out = std::collections::BTreeMap::new();
    for (key, value) in &file_status.x_attr {
        if let Some(name) = key.strip_prefix(XATTR_S3_META_PREFIX) {
            out.insert(name.to_string(), String::from_utf8_lossy(value).to_string());
        }
    }
    out
}

/// Create owner information from file status
pub fn create_owner_info(file_status: &FileStatus) -> Owner {
    Owner {
        id: file_status.owner.clone(),
        display_name: file_status.owner.clone(),
    }
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
                TtlAction::Free => "expiry-date=\"{}\" rule-id=\"ttl-free\"",
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
        content_type: Some(effective_content_type(file_status, object_name)),
        ..Default::default()
    };

    // TTL/Expiration information
    fill_ttl_info(&mut head, file_status);

    // Only metadata the client uploaded; internal attributes (file id, owner,
    // mode, replicas, other xattrs) are not S3 user metadata.
    head.metadata = Some(user_metadata(file_status).into_iter().collect());

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

#[cfg(test)]
mod etag_tests {
    use super::*;

    fn status_with(id: i64, mtime: i64, x_attr: Vec<(&str, &str)>) -> FileStatus {
        let mut s = FileStatus {
            id,
            mtime,
            ..Default::default()
        };
        for (k, v) in x_attr {
            s.x_attr.insert(k.to_string(), v.as_bytes().to_vec());
        }
        s
    }

    /// Two objects with the same bytes must share an ETag, which is what S3
    /// clients use to decide whether a transfer is needed.
    #[test]
    fn equal_content_yields_equal_etag() {
        let a = status_with(100, 1_700_000_000, vec![(XATTR_S3_ETAG, "abc123")]);
        let b = status_with(200, 1_700_000_999, vec![(XATTR_S3_ETAG, "abc123")]);
        assert_eq!(generate_etag(&a), generate_etag(&b));
        assert_eq!(generate_etag(&a), "\"abc123\"");
    }

    /// Changing the bytes must change the ETag. The old id+mtime form changed
    /// on every write regardless of content, and could also collide.
    #[test]
    fn different_content_yields_different_etag() {
        let a = status_with(100, 1_700_000_000, vec![(XATTR_S3_ETAG, "aaaa")]);
        let b = status_with(100, 1_700_000_000, vec![(XATTR_S3_ETAG, "bbbb")]);
        assert_ne!(generate_etag(&a), generate_etag(&b));
    }

    /// Content-Type must survive the upload instead of falling back to the
    /// extension guess.
    #[test]
    fn recorded_content_type_wins() {
        let s = status_with(1, 0, vec![(XATTR_S3_CONTENT_TYPE, "text/html")]);
        assert_eq!(effective_content_type(&s, "page.bin"), "text/html");
    }

    #[test]
    fn user_metadata_is_returned_without_its_prefix() {
        let s = status_with(
            1,
            0,
            vec![
                ("s3.meta.foo", "bar"),
                ("s3.meta.baz", "qux"),
                (XATTR_S3_ETAG, "ignored"),
            ],
        );
        let meta = user_metadata(&s);
        assert_eq!(meta.get("foo").map(String::as_str), Some("bar"));
        assert_eq!(meta.get("baz").map(String::as_str), Some("qux"));
        assert!(!meta.contains_key("etag"));
    }
}

#[cfg(test)]
mod metadata_visibility_tests {
    use super::*;

    /// HEAD/GET return exactly the uploaded user metadata, nothing internal.
    #[test]
    fn only_user_metadata_is_exposed() {
        let mut status = FileStatus {
            id: 42,
            owner: "root".into(),
            ..Default::default()
        };
        let meta = |s: &FileStatus| file_status_to_head_object_result(s, "k").metadata;
        assert_eq!(meta(&status), Some(Default::default()));
        for (k, v) in [("s3.etag", "a"), ("s3.meta.owner", "me"), ("raw", "x")] {
            status.x_attr.insert(k.into(), v.into());
        }
        let expected = [("owner".to_string(), "me".to_string())];
        assert_eq!(meta(&status), Some(expected.into()));
    }
}

/// S3-visible metadata an upload carries, written to the object's xattrs.
///
/// Kept as a value so the write can happen on the staging inode before the
/// publishing rename, instead of after the object is already visible.
#[derive(Debug, Clone, Default)]
pub struct S3Metadata {
    pub content_type: Option<String>,
    pub user_metadata: Vec<(String, String)>,
}

impl S3Metadata {
    pub fn from_put_option(opt: &crate::s3::dto::PutObjectOption) -> Self {
        Self {
            content_type: opt.content_type.clone(),
            user_metadata: opt
                .metadata
                .as_ref()
                .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or_default(),
        }
    }

    /// Write ETag, Content-Type and user metadata onto `path`.
    pub async fn write_to(
        &self,
        fs: &curvine_client::unified::UnifiedFileSystem,
        path: &curvine_fs_api::Path,
        etag: &str,
    ) -> Result<(), String> {
        use curvine_fs_api::FileSystem;
        use curvine_model::SetAttrOptsBuilder;

        let mut builder =
            SetAttrOptsBuilder::new().add_x_attr(XATTR_S3_ETAG, etag.as_bytes().to_vec());
        if let Some(ct) = self.content_type.as_deref() {
            builder = builder.add_x_attr(XATTR_S3_CONTENT_TYPE, ct.as_bytes().to_vec());
        }
        for (name, value) in &self.user_metadata {
            builder = builder.add_x_attr(
                format!("{XATTR_S3_META_PREFIX}{name}"),
                value.as_bytes().to_vec(),
            );
        }

        fs.set_attr(path, builder.build())
            .await
            .map_err(|e| format!("Failed to store S3 metadata for {path}: {e}"))
    }
}
