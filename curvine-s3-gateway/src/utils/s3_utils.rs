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
/// Standard object headers S3 stores and echoes back on HEAD/GET. They are
/// metadata, not user metadata: clients read them from the response headers,
/// never from `x-amz-meta-*`.
pub const XATTR_S3_CACHE_CONTROL: &str = "s3.cache_control";
pub const XATTR_S3_CONTENT_DISPOSITION: &str = "s3.content_disposition";
pub const XATTR_S3_CONTENT_ENCODING: &str = "s3.content_encoding";
pub const XATTR_S3_CONTENT_LANGUAGE: &str = "s3.content_language";
pub const XATTR_S3_EXPIRES: &str = "s3.expires";
/// User metadata is stored one key per attribute with this prefix.
pub const XATTR_S3_META_PREFIX: &str = "s3.meta.";

/// Content type recorded by the upload, or a guess from the key.
pub fn effective_content_type(file_status: &FileStatus, object_name: &str) -> String {
    match file_status.x_attr.get(XATTR_S3_CONTENT_TYPE) {
        Some(value) => String::from_utf8_lossy(value).to_string(),
        None => infer_content_type(object_name),
    }
}

/// A stored standard object header, if the upload set one.
pub fn stored_header(file_status: &FileStatus, key: &str) -> Option<String> {
    file_status
        .x_attr
        .get(key)
        .map(|value| String::from_utf8_lossy(value).to_string())
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
        // The TTL is reported through `x-amz-expiration` only. `Expires` must
        // be an HTTP-date; an RFC 3339 value there makes every cache treat the
        // object as already stale (RFC 9111 5.3), and the TTL moves with each
        // read because it is `atime + ttl_ms`.
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

    // Standard object headers the client set on upload. They are response
    // headers, not user metadata, so they must not come back as x-amz-meta-*.
    head.cache_control = stored_header(file_status, XATTR_S3_CACHE_CONTROL);
    head.content_disposition = stored_header(file_status, XATTR_S3_CONTENT_DISPOSITION);
    head.content_encoding = stored_header(file_status, XATTR_S3_CONTENT_ENCODING);
    head.content_language = stored_header(file_status, XATTR_S3_CONTENT_LANGUAGE);
    // `expires` is the uploaded header only: the Curvine TTL has its own
    // channel in `x-amz-expiration`, and an RFC 3339 value in `Expires` makes
    // caches treat the object as already stale.
    head.expires = stored_header(file_status, XATTR_S3_EXPIRES);
    head.expires_string = head.expires.clone();

    // Only metadata the client uploaded; internal attributes (file id, owner,
    // mode, replicas, other xattrs) are not S3 user metadata.
    head.metadata = Some(user_metadata(file_status).into_iter().collect());

    head
}

/// Standard object headers a HEAD/GET response must echo.
///
/// One list for both paths: the router serves HEAD and GET from different
/// functions, and duplicating the mapping let them drift apart.
pub fn object_response_headers(head: &HeadObjectResult) -> Vec<(&'static str, String)> {
    [
        ("cache-control", head.cache_control.as_deref()),
        ("content-disposition", head.content_disposition.as_deref()),
        ("content-encoding", head.content_encoding.as_deref()),
        ("content-language", head.content_language.as_deref()),
        ("expires", head.expires.as_deref()),
    ]
    .into_iter()
    .filter_map(|(name, value)| value.map(|v| (name, v.to_string())))
    .collect()
}

/// The `response-*` parameters a GET may override.
const RESPONSE_OVERRIDE_PARAMETERS: [&str; 6] = [
    "response-cache-control",
    "response-content-disposition",
    "response-content-encoding",
    "response-content-language",
    "response-expires",
    "response-content-type",
];

/// The first `response-*` parameter whose value is not a valid header value.
///
/// A query value is percent-decoded before it reaches the response builder, so
/// `%0D%0A` would carry a CR/LF into `HeaderValue`. `Builder::header` only
/// reports that error from `body()`, which panics under `panic = "abort"`.
fn invalid_override_parameter<Q: Fn(&str) -> Option<String>>(query: Q) -> Option<&'static str> {
    RESPONSE_OVERRIDE_PARAMETERS.into_iter().find(|parameter| {
        query(parameter).is_some_and(|value| axum::http::HeaderValue::from_str(&value).is_err())
    })
}

/// Every header a GET must emit for `head`, with the `response-*` overrides
/// applied.
///
/// This is the whole response-header decision in one place, so it can be tested
/// without a filesystem: an invalid override is reported by name, and a valid
/// one replaces the stored value instead of adding a second one.
pub fn get_response_headers<Q: Fn(&str) -> Option<String>>(
    head: &HeadObjectResult,
    query: Q,
) -> Result<Vec<(&'static str, String)>, &'static str> {
    if let Some(parameter) = invalid_override_parameter(&query) {
        return Err(parameter);
    }
    let mut headers = object_response_headers(head);
    apply_response_overrides(&mut headers, &query);
    // `content-type` already has a stored value, so it is resolved here rather
    // than in the override list: emitting both would send two values and every
    // client would read the stored one.
    if let Some(value) = query("response-content-type").or_else(|| head.content_type.clone()) {
        headers.retain(|(name, _)| *name != "content-type");
        headers.push(("content-type", value));
    }
    Ok(headers)
}

/// Apply the `response-*` overrides a GET may carry for one response.
///
/// The stored header is replaced, not appended: two values for the same header
/// name would be rejected by strict clients.
pub fn apply_response_overrides<Q: Fn(&str) -> Option<String>>(
    headers: &mut Vec<(&'static str, String)>,
    query: Q,
) {
    // `content-type` is resolved by the caller: it already emits one and
    // appending a second value would leave the override unreadable.
    for (name, parameter) in [
        ("cache-control", "response-cache-control"),
        ("content-disposition", "response-content-disposition"),
        ("content-encoding", "response-content-encoding"),
        ("content-language", "response-content-language"),
        ("expires", "response-expires"),
    ] {
        if let Some(value) = query(parameter) {
            headers.retain(|(existing, _)| *existing != name);
            headers.push((name, value));
        }
    }
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

    /// Cache-Control, Content-Encoding and friends are response headers, not
    /// user metadata: they come back as headers and never as x-amz-meta-*.
    #[test]
    fn standard_object_headers_are_echoed_not_user_metadata() {
        let s = status_with(
            100,
            1_700_000_000,
            vec![
                (XATTR_S3_CACHE_CONTROL, "max-age=60"),
                (XATTR_S3_CONTENT_ENCODING, "gzip"),
                (XATTR_S3_CONTENT_DISPOSITION, "attachment; filename=a.bin"),
                (XATTR_S3_EXPIRES, "Wed, 21 Oct 2026 07:28:00 GMT"),
            ],
        );

        let head = file_status_to_head_object_result(&s, "a.bin");

        assert_eq!(head.cache_control.as_deref(), Some("max-age=60"));
        assert_eq!(head.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(
            head.content_disposition.as_deref(),
            Some("attachment; filename=a.bin")
        );
        assert_eq!(
            head.expires.as_deref(),
            Some("Wed, 21 Oct 2026 07:28:00 GMT")
        );
        assert_eq!(head.expires_string, head.expires);

        let metadata = head.metadata.expect("metadata map");
        assert!(!metadata.contains_key("cache-control"), "{metadata:?}");
        assert!(!metadata.contains_key("content-encoding"), "{metadata:?}");
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

    fn head_with(content_type: Option<&str>) -> HeadObjectResult {
        HeadObjectResult {
            content_type: content_type.map(str::to_string),
            ..Default::default()
        }
    }

    /// `%0D%0A` reaches the builder percent-decoded; without this check
    /// `body()` panics and `panic = "abort"` takes the gateway down.
    #[test]
    fn crlf_in_a_response_override_is_rejected() {
        let head = head_with(None);
        let parameter = get_response_headers(&head, |name| {
            (name == "response-cache-control").then(|| "a\r\nb".to_string())
        })
        .expect_err("CR/LF must be rejected");
        assert_eq!(parameter, "response-cache-control");
    }

    #[test]
    fn response_content_type_replaces_the_stored_value() {
        let head = head_with(Some("application/octet-stream"));
        let headers = get_response_headers(&head, |name| {
            (name == "response-content-type").then(|| "audio/ogg".to_string())
        })
        .expect("valid override");

        let content_types: Vec<_> = headers
            .iter()
            .filter(|(name, _)| *name == "content-type")
            .collect();
        assert_eq!(content_types.len(), 1, "{headers:?}");
        assert_eq!(content_types[0].1, "audio/ogg");
    }

    #[test]
    fn a_stored_header_is_replaced_not_duplicated() {
        let mut head = head_with(None);
        head.cache_control = Some("max-age=60".to_string());
        let headers = get_response_headers(&head, |name| {
            (name == "response-cache-control").then(|| "no-store".to_string())
        })
        .expect("valid override");

        let cache_control: Vec<_> = headers
            .iter()
            .filter(|(name, _)| *name == "cache-control")
            .collect();
        assert_eq!(cache_control.len(), 1, "{headers:?}");
        assert_eq!(cache_control[0].1, "no-store");
    }

    #[test]
    fn a_plain_get_emits_the_stored_headers() {
        let mut head = head_with(Some("text/plain"));
        head.content_encoding = Some("gzip".to_string());
        let headers = get_response_headers(&head, |_| None).expect("no overrides");

        assert!(headers
            .iter()
            .any(|(name, value)| *name == "content-encoding" && value == "gzip"));
        assert!(headers
            .iter()
            .any(|(name, value)| *name == "content-type" && value == "text/plain"));
    }

    /// `content-type` is resolved by the caller before the response is built.
    /// If the override list also emitted it, the response would carry two
    /// values and clients would read the stored one.
    #[test]
    fn content_type_is_not_emitted_by_the_override_list() {
        let mut headers = vec![("cache-control", "max-age=1".to_string())];
        apply_response_overrides(&mut headers, |name| {
            (name == "response-content-type").then(|| "audio/ogg".to_string())
        });
        assert!(!headers.iter().any(|(name, _)| *name == "content-type"));
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
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub expires: Option<String>,
    pub user_metadata: Vec<(String, String)>,
}

impl S3Metadata {
    pub fn from_put_option(opt: &crate::s3::dto::PutObjectOption) -> Self {
        Self {
            content_type: opt.content_type.clone(),
            cache_control: opt.cache_control.clone(),
            content_disposition: opt.content_disposition.clone(),
            content_encoding: opt.content_encoding.clone(),
            content_language: opt.content_language.clone(),
            // Stored in HTTP-date form so HEAD echoes back what the client sent.
            expires: opt
                .expires
                .map(|value| value.format("%a, %d %b %Y %H:%M:%S GMT").to_string()),
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
        for (key, value) in [
            (XATTR_S3_CONTENT_TYPE, self.content_type.as_deref()),
            (XATTR_S3_CACHE_CONTROL, self.cache_control.as_deref()),
            (
                XATTR_S3_CONTENT_DISPOSITION,
                self.content_disposition.as_deref(),
            ),
            (XATTR_S3_CONTENT_ENCODING, self.content_encoding.as_deref()),
            (XATTR_S3_CONTENT_LANGUAGE, self.content_language.as_deref()),
            (XATTR_S3_EXPIRES, self.expires.as_deref()),
        ] {
            if let Some(value) = value {
                builder = builder.add_x_attr(key, value.as_bytes().to_vec());
            }
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
