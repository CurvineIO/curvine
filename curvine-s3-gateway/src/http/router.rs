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

//! S3 API request router - OPTIMIZED for zero dynamic dispatch
//!
//! This module handles routing of S3 API requests based on HTTP methods
//! and provides dedicated handlers for each type of S3 operation.
//!
//! ## Performance Optimization
//!
//! This router has been optimized to eliminate async trait dynamic dispatch:
//! - Previously: Each handler was `Arc<dyn Trait>`, causing Box allocation per call
//! - Now: Single `Arc<S3Handlers>` with direct method calls, zero allocation overhead
//!
//! The S3Router follows a hierarchical approach:
//! 1. Route by HTTP method (PUT, GET, DELETE, HEAD, POST)
//! 2. Determine operation type based on URL path and query parameters
//! 3. Delegate to specific operation handlers (direct calls, no trait objects)
//! 4. Handle errors and responses uniformly

use crate::auth::sig_v4::VHeader;
use crate::http::axum::{Request, Response};
use crate::s3::handlers::{evaluate_precondition_headers, Preconditions, S3Handlers};
use crate::s3::s3_api::*;

/// Whether a bucket-level GET is a ListObjects V1 request.
///
/// V1 is the only listing without `list-type`, so it is identified by the
/// listing parameters it shares with V2, or by having no query at all. Every
/// other bucket sub-resource (`?location`, `?versions`, `?uploads`, ...) is
/// dispatched further down the same handler, even when it carries parameters
/// like `prefix` or `encoding-type` that a listing also accepts.
fn is_list_objects_v1_request(req: &Request) -> bool {
    // A listing names a bucket, never a key. Without this check every plain
    // object GET (`/{bucket}/{key}`, no query string) was classified as a V1
    // listing and answered 400 instead of the object.
    if req.url_path().trim_matches('/').split_once('/').is_some() {
        return false;
    }
    // A bucket-level request naming another sub-resource is not a listing.
    // Sub-resources are checked first because SDKs send listing-shaped
    // parameters with them: a versions request carries `prefix`, `delimiter`
    // and `encoding-type=url`, and used to be answered as V1.
    const SUB_RESOURCES: [&str; 10] = [
        "location",
        "versions",
        "uploads",
        "uploadId",
        "acl",
        "tagging",
        "cors",
        "policy",
        "delete",
        "object-lock",
    ];
    if SUB_RESOURCES.iter().any(|key| req.get_query(key).is_some()) {
        return false;
    }
    // Any remaining bucket-level GET is a V1 listing: it carries listing
    // parameters, or only X-Amz-* signing parameters when presigned.
    true
}
use axum::http::StatusCode;
use axum::response::IntoResponse;
use std::sync::Arc;

#[inline]
fn get_handlers(req: &axum::extract::Request<axum::body::Body>) -> Option<Arc<S3Handlers>> {
    req.extensions().get::<Arc<S3Handlers>>().cloned()
}

pub struct S3Router;

/// S3 XML error response for the router's own error exits, so SDKs read
/// `Error.Code` instead of a bare status (the auth middleware and handlers
/// answer through the same helpers).
fn s3_error(err: &str) -> axum::response::Response {
    let status = crate::s3::s3_api::status_for_error(err);
    let request_id = crate::utils::s3_utils::generate_request_id();
    let body = crate::s3::s3_api::error_xml(err, &request_id);
    axum::response::Response::builder()
        .status(status)
        .header("content-type", "application/xml")
        .header("x-amz-request-id", &request_id)
        .body(axum::body::Body::from(body))
        .unwrap()
}

/// GetBucketLocation body. S3 reports `us-east-1` as an empty
/// LocationConstraint, which SDKs read back as us-east-1, and every other
/// region by name.
fn location_constraint_response(region: &str) -> axum::response::Response {
    let region = if region == "us-east-1" { "" } else { region };
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LocationConstraint xmlns=\"{}\">{}</LocationConstraint>",
        crate::s3::dto::S3_NAMESPACE,
        quick_xml::escape::escape(region)
    );
    axum::response::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .header(
            "x-amz-request-id",
            crate::utils::s3_utils::generate_request_id(),
        )
        .body(axum::body::Body::from(body))
        .unwrap()
}

/// CopyObject / UploadPartCopy answer: the root element in the S3 namespace,
/// the quoted ETag and the time of the copy.
fn copy_result_xml(root: &str, quoted_etag: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<{root} xmlns=\"{}\">\
<ETag>{quoted_etag}</ETag>\
<LastModified>{}</LastModified>\
</{root}>",
        crate::s3::dto::S3_NAMESPACE,
        chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ")
    )
}

/// S3 sub-resources that no handler serves.
///
/// Without this list a request naming one of them fell through to the handler
/// of another operation: `PUT /{bucket}/{key}?tagging` (PutObjectTagging)
/// reached PutObject and replaced the object with the tagging document,
/// `DELETE /{bucket}/{key}?tagging` reached DeleteObject, `DELETE /{bucket}?cors`
/// reached DeleteBucket, and `GET /{bucket}?versioning` was answered as a listing.
///
/// A Signature V2 request reaches this check only if the gateway signs the same
/// sub-resources as the client. What the gateway signs follows the clients
/// (`INCLUDED_QUERY_PARAMS` in `auth/sig_v2.rs`), so adding a name here does not
/// change it.
const UNSUPPORTED_SUB_RESOURCES: &[&str] = &[
    "accelerate",
    "acl",
    "analytics",
    "attributes",
    "cors",
    "encryption",
    "intelligent-tiering",
    "inventory",
    "legal-hold",
    "lifecycle",
    "logging",
    "metrics",
    "notification",
    "object-lock",
    "ownershipControls",
    "policy",
    "policyStatus",
    "publicAccessBlock",
    "replication",
    "requestPayment",
    "restore",
    "retention",
    "select",
    "tagging",
    "torrent",
    "versioning",
    "website",
];

/// The first parameter of `query` that names an unsupported sub-resource.
fn unsupported_sub_resource(query: Option<&str>) -> Option<&'static str> {
    query?
        .split('&')
        .map(|pair| pair.split_once('=').map_or(pair, |(key, _)| key))
        .find_map(|key| {
            UNSUPPORTED_SUB_RESOURCES
                .iter()
                .copied()
                .find(|s| *s == key)
        })
}

impl S3Router {
    pub async fn route(req: axum::extract::Request<axum::body::Body>) -> axum::response::Response {
        if let Some(sub_resource) = unsupported_sub_resource(req.uri().query()) {
            return s3_error(&format!("NotImplemented: {sub_resource} is not supported"));
        }
        match *req.method() {
            axum::http::Method::PUT => Self::handle_put_request(req).await,
            axum::http::Method::GET => Self::handle_get_request(req).await,
            axum::http::Method::DELETE => Self::handle_delete_request(req).await,
            axum::http::Method::HEAD => Self::handle_head_request(req).await,
            axum::http::Method::POST => Self::handle_post_request(req).await,
            _ => {
                s3_error("MethodNotAllowed: the request method is not supported for this resource")
            }
        }
    }

    async fn handle_put_request(
        req: axum::extract::Request<axum::body::Body>,
    ) -> axum::response::Response {
        // OPTIMIZED: Single concrete type extraction instead of multiple trait objects
        let handlers = match get_handlers(&req) {
            Some(h) => h,
            None => {
                tracing::error!("S3Handlers not found in request extensions");
                return s3_error("failed to load the request handlers");
            }
        };
        let authenticated = req
            .extensions()
            .get::<crate::auth::sig_v4::V4Head>()
            .is_some();
        let chunk_auth = req
            .extensions()
            .get::<crate::utils::signed_chunk_reader::ChunkAuth>()
            .cloned();

        let path = req.uri().path();
        let rpath = path
            .trim_start_matches('/')
            .splitn(2, '/')
            .collect::<Vec<&str>>();

        let rpath_len = rpath.len();
        if rpath_len == 0 {
            log::info!("args length invalid");
            return s3_error("InvalidArgument: the path must be /bucket/key");
        }

        let is_create_bkt = rpath_len == 1 || (rpath_len == 2 && rpath[1].is_empty());
        let req = Request::from(req);
        let is_copy = req.get_header("x-amz-copy-source").is_some();
        if (is_copy || is_create_bkt)
            && req
                .get_header("x-amz-content-sha256")
                .is_some_and(|mode| mode.starts_with("STREAMING-"))
        {
            // These routes do not consume the upload body. Never mutate state
            // for an aws-chunked request without processing its signed framing.
            return s3_error(
                "InvalidRequest: aws-chunked is only supported for PutObject and UploadPart",
            );
        }

        if is_create_bkt {
            Self::handle_create_bucket_request(req, &handlers).await
        } else {
            let xid = req.get_query("x-id");
            let upload_id = req.get_query("uploadId");
            let part_number = req.get_query("partNumber");
            // Match on either query parameter so an incomplete multipart PUT
            // returns 400 instead of falling through to CopyObject/PutObject.
            let is_upload_part = matches!(xid.as_deref(), Some("UploadPart" | "UploadPartCopy"))
                || upload_id.is_some()
                || part_number.is_some();

            if is_upload_part && is_copy {
                Self::handle_upload_part_copy_request(req, &handlers).await
            } else if is_upload_part {
                Self::handle_multipart_upload_part_request(req, &handlers, chunk_auth).await
            } else if is_copy {
                // Server-side copy. Routing it to the PUT handler created a
                // zero-byte object because a copy request carries no body.
                Self::handle_copy_object_request(req, &handlers).await
            } else {
                if !authenticated {
                    return s3_error("AccessDenied: the request did not pass authentication");
                }
                Self::handle_put_object_request(req, &handlers, chunk_auth).await
            }
        }
    }

    async fn handle_get_request(
        req: axum::extract::Request<axum::body::Body>,
    ) -> axum::response::Response {
        if req.uri().path().starts_with("/probe-bsign") {
            return (StatusCode::OK, b"").into_response();
        }

        // OPTIMIZED: Single concrete type extraction
        let handlers = match get_handlers(&req) {
            Some(h) => h,
            None => {
                tracing::error!("S3Handlers not found in request extensions");
                return s3_error("failed to load the request handlers");
            }
        };

        let req = Request::from(req);
        if matches!(req.get_query("x-id").as_deref(), Some("ListParts"))
            || req.get_query("uploadId").is_some()
        {
            return Self::handle_list_parts_request(req, &handlers).await;
        }
        let url_path = req.url_path();
        if matches!(
            req.get_query("x-id").as_deref(),
            Some("ListMultipartUploads")
        ) || req.get_query("uploads").is_some()
        {
            return Self::handle_list_multipart_uploads_request(req, &handlers).await;
        }

        if let Some(lt) = req.get_query("list-type") {
            if lt == "2" {
                if url_path.trim_start_matches('/').is_empty() {
                    return Self::handle_list_buckets_request(req, &handlers).await;
                } else {
                    return Self::handle_list_objects_request(req, &handlers).await;
                }
            }
        } else if url_path.trim_start_matches('/').is_empty() {
            return Self::handle_list_buckets_request(req, &handlers).await;
        } else if is_list_objects_v1_request(&req) {
            // `GET /{bucket}` with listing parameters, or with no query at all,
            // is ListObjects V1. Legacy SDKs still call it; answering it as a
            // GetObject on the bucket failed every such listing.
            return Self::handle_list_objects_v1_request(req, &handlers).await;
        }

        if req.get_query("location").is_some() {
            return Self::handle_get_bucket_location_request(req, &handlers).await;
        }

        // Handle object versions listing requests
        if req.get_query("versions").is_some() {
            return Self::handle_list_object_versions_request(req, &handlers).await;
        }

        // Default to object download - uses optimized stream_get_object
        Self::handle_get_object_request(req, &handlers).await
    }

    async fn handle_delete_request(
        req: axum::extract::Request<axum::body::Body>,
    ) -> axum::response::Response {
        let path = req.uri().path().trim_start_matches('/');
        if path.is_empty() {
            return s3_error("InvalidArgument: the path must be /bucket/key");
        }

        // OPTIMIZED: Single concrete type extraction
        let handlers = match get_handlers(&req) {
            Some(h) => h,
            None => {
                tracing::error!("S3Handlers not found in request extensions");
                return s3_error("failed to load the request handlers");
            }
        };

        let rr = path.split("/").collect::<Vec<&str>>();
        let rr_len = rr.len();

        if rr_len == 1 || (rr_len == 2 && rr[1].is_empty()) {
            return Self::handle_delete_bucket_request(Request::from(req), &handlers).await;
        }

        let req = Request::from(req);
        // `DELETE /{bucket}/{key}?uploadId=...` is AbortMultipartUpload, not a
        // plain object delete: SDKs abort after any part failure.
        if req.get_query("uploadId").is_some() {
            return Self::handle_multipart_abort_request(req, &handlers).await;
        }
        Self::handle_delete_object_request(req, &handlers).await
    }

    async fn handle_head_request(
        req: axum::extract::Request<axum::body::Body>,
    ) -> axum::response::Response {
        // OPTIMIZED: Single concrete type extraction
        let handlers = match get_handlers(&req) {
            Some(h) => h,
            None => {
                tracing::error!("S3Handlers not found in request extensions");
                return (StatusCode::INTERNAL_SERVER_ERROR, b"").into_response();
            }
        };

        let req = Request::from(req);
        let raw_path = req.url_path();

        let args = raw_path
            .trim_start_matches('/')
            .splitn(2, '/')
            .collect::<Vec<&str>>();

        if args.len() == 1 {
            Self::handle_head_bucket_request(args[0], &handlers).await
        } else if args.len() != 2 {
            (StatusCode::BAD_REQUEST, b"").into_response()
        } else {
            Self::handle_head_object_request(req, args[0], args[1], &handlers).await
        }
    }

    async fn handle_post_request(
        req: axum::extract::Request<axum::body::Body>,
    ) -> axum::response::Response {
        // OPTIMIZED: Single concrete type extraction
        let handlers = match get_handlers(&req) {
            Some(h) => h,
            None => {
                tracing::error!("S3Handlers not found in request extensions");
                return s3_error("failed to load the request handlers");
            }
        };

        // Handle DeleteObjects (POST ?delete)
        if req
            .uri()
            .query()
            .map(|q| q.contains("delete"))
            .unwrap_or(false)
        {
            let mut resp = Response::default();
            crate::s3::s3_api::handle_post_delete_objects(
                Request::from(req),
                &mut resp,
                handlers.as_ref(),
            )
            .await;
            return resp.into();
        }

        // Multipart-related POSTs
        let query_string = req.uri().query();
        let is_create_session = if let Some(query) = query_string {
            query.contains("uploads=") || query.contains("uploads")
        } else {
            false
        };

        let req = Request::from(req);
        if is_create_session {
            Self::handle_multipart_create_session_request(req, &handlers).await
        } else if req.get_query("uploadId").is_some() {
            Self::handle_multipart_complete_session_request(req, &handlers).await
        } else {
            s3_error("InvalidArgument: the request must carry uploads or uploadId")
        }
    }

    // =========================================================================
    // OPTIMIZED HANDLER METHODS - All use concrete Arc<S3Handlers> type
    // Previously: Each method took Arc<dyn Trait>, causing Box allocation per call
    // Now: Direct method calls on concrete type, zero dynamic dispatch overhead
    // =========================================================================

    async fn handle_create_bucket_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        let mut resp = Response::default();
        handle_create_bucket(req, &mut resp, handlers.as_ref()).await;
        resp.into()
    }

    async fn handle_multipart_upload_part_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
        chunk_auth: Option<crate::utils::signed_chunk_reader::ChunkAuth>,
    ) -> axum::response::Response {
        let mut resp = Response::default();
        handle_upload_part_with_streaming(
            req,
            &mut resp,
            handlers.as_ref(),
            chunk_auth.map(|auth| (auth, &handlers.signed_spool)),
        )
        .await;
        resp.into()
    }

    /// `PUT ?uploadId&partNumber` with `x-amz-copy-source`: UploadPartCopy.
    async fn handle_upload_part_copy_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        use crate::s3::s3_api::VRequest;

        let dest_path = req.url_path();
        let Some((dst_bucket, dst_key)) = dest_path
            .trim_start_matches('/')
            .split_once('/')
            .filter(|(b, k)| !b.is_empty() && !k.is_empty())
        else {
            return copy_error_response("InvalidArgument: the path must be /bucket/key");
        };
        let raw = req.get_header("x-amz-copy-source").unwrap_or_default();
        let Some((src_bucket, src_key)) = parse_copy_source(&raw) else {
            return copy_error_response("InvalidArgument: invalid x-amz-copy-source");
        };
        let Some(upload_id) = req.get_query("uploadId").filter(|s| !s.is_empty()) else {
            return copy_error_response("InvalidArgument: uploadId is required");
        };
        let part_number = req.get_query("partNumber").unwrap_or_default();
        let part_number = match parse_part_number(&part_number) {
            Ok(n) => n,
            Err(err) => return copy_error_response(err),
        };
        let range = req.get_header("x-amz-copy-source-range");
        let conds = crate::s3::handlers::Preconditions::from_headers(&req, true);
        match handlers
            .upload_part_copy(
                (&src_bucket, &src_key),
                (dst_bucket, dst_key),
                &upload_id,
                part_number,
                range.as_deref(),
                &conds,
            )
            .await
        {
            Ok(etag) => {
                // `write_part_stream` already returns a quoted `"<md5>"` ETag.
                let body = copy_result_xml("CopyPartResult", &etag);
                (
                    StatusCode::OK,
                    [(axum::http::header::CONTENT_TYPE, "application/xml")],
                    body,
                )
                    .into_response()
            }
            Err(e) => {
                log::warn!("upload part copy failed: {e}");
                copy_error_response(&e)
            }
        }
    }

    /// `PUT` with `x-amz-copy-source`: copy an existing object server-side.
    async fn handle_copy_object_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        use crate::s3::s3_api::VRequest;

        // "bucket/key" or "/bucket/key", URL-encoded, optionally carrying
        // "?versionId=..." which this backend does not version.
        let raw = match req.get_header("x-amz-copy-source") {
            Some(value) => value,
            None => return s3_error("InvalidArgument: the x-amz-copy-source header is required"),
        };
        let source = raw
            .split('?')
            .next()
            .unwrap_or(&raw)
            .trim_start_matches('/');
        let decoded = urlencoding::decode(source)
            .map(|cow| cow.into_owned())
            .unwrap_or_else(|_| source.to_string());
        let (src_bucket, src_key) = match decoded.split_once('/') {
            Some((b, k)) if !b.is_empty() && !k.is_empty() => (b, k),
            _ => {
                log::warn!("invalid x-amz-copy-source: {raw}");
                return s3_error(
                    "InvalidArgument: the x-amz-copy-source header must be /bucket/key",
                );
            }
        };

        let dest_path = req.url_path();
        let dest = dest_path
            .trim_start_matches('/')
            .splitn(2, '/')
            .collect::<Vec<&str>>();
        if dest.len() != 2 || dest[1].is_empty() {
            return s3_error("InvalidArgument: the path must be /bucket/key");
        }

        // `x-amz-metadata-directive: REPLACE` replaces the destination metadata
        // with the request's; COPY (the default) inherits the source's.
        let replace = req
            .get_header("x-amz-metadata-directive")
            .is_some_and(|value| value.eq_ignore_ascii_case("REPLACE"));
        // A copy onto itself only makes sense when it replaces metadata.
        // Without REPLACE it is a request error, not a no-op.
        if src_bucket == dest[0] && src_key == dest[1] && !replace {
            log::warn!("copy object onto itself without REPLACE: {src_bucket}/{src_key}");
            return s3_error("InvalidArgument: this copy request is illegal because it is trying to copy an object to itself without changing the object's metadata");
        }
        let replacement = replace.then(|| crate::utils::s3_utils::S3Metadata::from_headers(&req));

        let source_conditions = crate::s3::handlers::Preconditions::from_headers(&req, true);
        match handlers
            .copy_object(
                src_bucket,
                src_key,
                dest[0],
                dest[1],
                &source_conditions,
                replacement.as_ref(),
            )
            .await
        {
            Ok(etag) => {
                let body = copy_result_xml("CopyObjectResult", &format!("\"{etag}\""));
                (
                    StatusCode::OK,
                    [(axum::http::header::CONTENT_TYPE, "application/xml")],
                    body,
                )
                    .into_response()
            }
            Err(e) => {
                // Map every protocol status the handler may report; a missing
                // arm silently degraded 412 to 500.
                let status = copy_error_status(&e);
                log::warn!("copy object failed ({status}): {e}");
                let request_id = crate::utils::s3_utils::generate_request_id();
                let body = crate::s3::s3_api::error_xml(&e, &request_id);
                let headers = [
                    ("content-type", "application/xml"),
                    ("x-amz-request-id", request_id.as_str()),
                ];
                (status, headers, body).into_response()
            }
        }
    }

    async fn handle_put_object_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
        chunk_auth: Option<crate::utils::signed_chunk_reader::ChunkAuth>,
    ) -> axum::response::Response {
        let mut resp = Response::default();
        handle_put_object_with_streaming(
            req,
            &mut resp,
            handlers.as_ref(),
            chunk_auth.map(|auth| (auth, &handlers.signed_spool)),
        )
        .await;
        resp.into()
    }

    async fn handle_list_buckets_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        log::info!("is list buckets");
        let mut resp = Response::default();
        handle_get_list_buckets(req, &mut resp, handlers.as_ref()).await;
        resp.into()
    }

    async fn handle_list_objects_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        let mut resp = Response::default();
        handle_get_list_object(req, &mut resp, handlers.as_ref()).await;
        resp.into()
    }

    async fn handle_list_objects_v1_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        let mut resp = Response::default();
        handle_get_list_object_v1(req, &mut resp, handlers.as_ref()).await;
        resp.into()
    }

    /// Handle object versions listing request - OPTIMIZED with concrete type
    async fn handle_list_object_versions_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        let mut resp = Response::default();
        crate::s3::s3_api::handle_get_list_object_versions(req, &mut resp, handlers.as_ref()).await;
        resp.into()
    }

    /// `GET /{bucket}?uploads`: ListMultipartUploads.
    async fn handle_list_multipart_uploads_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        let path = req.url_path();
        let trimmed = path.trim_start_matches('/');
        let bucket = trimmed.strip_suffix('/').unwrap_or(trimmed);
        if bucket.is_empty() || bucket.contains('/') {
            return list_parts_error_response(
                "InvalidArgument: ListMultipartUploads requires a bucket path",
            );
        }
        if req.get_query("uploads").is_none() {
            return list_parts_error_response("InvalidArgument: uploads subresource is required");
        }
        let options = match crate::s3::multipart_listing::UploadListOptions::from_request(&req) {
            Ok(options) => options,
            Err(e) => return list_parts_error_response(&e),
        };
        match handlers
            .list_multipart_uploads(bucket, options)
            .await
            .and_then(|doc| quick_xml::se::to_string(&doc).map_err(|e| format!("xml: {e}")))
        {
            Ok(xml) => (
                StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, "application/xml")],
                format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>{xml}"),
            )
                .into_response(),
            Err(e) => {
                log::warn!("list multipart uploads failed: {e}");
                list_parts_error_response(&e)
            }
        }
    }

    async fn handle_get_bucket_location_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        let url_path = req.url_path();
        let bucket = url_path.trim_matches('/');
        match handlers.handle_get_bucket_location(bucket).await {
            Ok(region) => location_constraint_response(&region),
            Err(e) => {
                log::warn!("get bucket location error: {e}");
                s3_error(&e)
            }
        }
    }

    async fn handle_get_object_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        // Direct call to optimized streaming function
        crate::http::axum::stream_get_object(req, handlers).await
    }

    async fn handle_delete_bucket_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        let mut resp = Response::default();
        handle_delete_bucket(req, &mut resp, handlers.as_ref()).await;
        resp.into()
    }

    async fn handle_delete_object_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        let mut resp = Response::default();
        handle_delete_object(req, &mut resp, handlers.as_ref()).await;
        resp.into()
    }

    async fn handle_head_bucket_request(
        bucket_name: &str,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        // Direct method call - no trait object
        let opt = crate::s3::ListBucketsOption {
            bucket_region: None,
            continuation_token: None,
            max_buckets: None,
            prefix: None,
        };
        match handlers.handle_list_buckets(&opt).await {
            Ok(buckets) => {
                let exists = buckets.iter().any(|b| b.name == bucket_name);
                if exists {
                    (StatusCode::OK, b"").into_response()
                } else {
                    (StatusCode::NOT_FOUND, b"").into_response()
                }
            }
            Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, b"").into_response(),
        }
    }

    async fn handle_head_object_request(
        req: Request,
        bucket: &str,
        object: &str,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        let Some((head, mtime)) = handlers.lookup_object(bucket, object).await else {
            return (StatusCode::NOT_FOUND, b"").into_response();
        };
        let conditions = Preconditions::from_headers(&req, false);
        if let Some(code) =
            evaluate_precondition_headers(&conditions, head.etag.as_deref(), Some(mtime))
        {
            return crate::http::axum::precondition_response(
                "HEAD",
                code,
                head.etag.as_deref(),
                mtime,
            );
        }

        let mut resp = Response::default();

        if let Some(v) = head.content_length {
            resp.set_header("content-length", v.to_string().as_str())
        }
        if let Some(v) = &head.etag {
            resp.set_header("etag", v);
        }
        if let Some(v) = &head.last_modified {
            resp.set_header("last-modified", v);
        }

        // Standard object headers stored on upload plus any
        // `response-*` overrides, shared with GET.
        let object_headers =
            match crate::utils::s3_utils::get_response_headers(&head, |name| req.get_query(name)) {
                Ok(headers) => headers,
                Err(parameter) => {
                    tracing::warn!("rejected HEAD with invalid {parameter} value");
                    return (StatusCode::BAD_REQUEST, b"").into_response();
                }
            };
        for (name, value) in object_headers {
            resp.set_header(name, &value);
        }

        if let Some(metadata) = head.metadata {
            for (key, value) in metadata {
                let header_name = if key.starts_with("x-amz-meta-") {
                    key
                } else {
                    format!("x-amz-meta-{}", key)
                };
                resp.set_header(&header_name, &value);
            }
        }

        resp.set_header("Connection", "close");
        resp.set_header("X-Head-Response", "true");
        resp.set_status(200);
        resp.send_header();
        resp.into()
    }

    async fn handle_multipart_create_session_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        let mut resp = Response::default();
        handle_multipart_create_session(req, &mut resp, handlers.as_ref()).await;
        resp.into()
    }

    async fn handle_multipart_complete_session_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        let mut resp = Response::default();
        handle_multipart_complete_session(req, &mut resp, handlers.as_ref()).await;
        resp.into()
    }

    async fn handle_multipart_abort_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        let mut resp = Response::default();
        crate::s3::s3_api::handle_abort_multipart_upload(req, &mut resp, handlers.as_ref()).await;
        resp.into()
    }

    /// `GET /{bucket}/{key}?uploadId=...`: ListParts.
    async fn handle_list_parts_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        let path = req.url_path();
        let Some((bucket, key)) = path
            .trim_start_matches('/')
            .split_once('/')
            .filter(|(b, k)| !b.is_empty() && !k.is_empty())
        else {
            return list_parts_error_response("InvalidArgument: the path must be /bucket/key");
        };
        let Some(upload_id) = req.get_query("uploadId") else {
            return list_parts_error_response("InvalidArgument: uploadId is required");
        };
        let marker = req.get_query("part-number-marker");
        let max = req.get_query("max-parts");
        match handlers
            .list_parts(bucket, key, &upload_id, marker.as_deref(), max.as_deref())
            .await
            .and_then(|doc| quick_xml::se::to_string(&doc).map_err(|e| format!("xml: {e}")))
        {
            Ok(xml) => (
                StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, "application/xml")],
                format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>{xml}"),
            )
                .into_response(),
            Err(e) => {
                log::warn!("list parts failed: {e}");
                list_parts_error_response(&e)
            }
        }
    }
}

fn list_parts_error_response(err: &str) -> axum::response::Response {
    let request_id = crate::utils::s3_utils::generate_request_id();
    let body = crate::s3::s3_api::error_xml(err, &request_id);
    let headers = [
        ("content-type", "application/xml"),
        ("x-amz-request-id", request_id.as_str()),
    ];
    (copy_error_status(err), headers, body).into_response()
}

/// HTTP status for a `copy_object` failure.
///
/// Delegates to the protocol mapping instead of re-deriving a subset: a local
/// match answered 500 for every code it did not list, which turned a 400 (a
/// reserved staging key, for example) into a retryable server error.
fn copy_error_status(err: &str) -> StatusCode {
    StatusCode::from_u16(crate::s3::s3_api::status_for_error(err))
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
}

/// S3 XML error response for an UploadPartCopy failure.
fn copy_error_response(err: &str) -> axum::response::Response {
    let request_id = crate::utils::s3_utils::generate_request_id();
    let body = crate::s3::s3_api::error_xml(err, &request_id);
    let headers = [
        ("content-type", "application/xml"),
        ("x-amz-request-id", request_id.as_str()),
    ];
    (copy_error_status(err), headers, body).into_response()
}

/// `x-amz-copy-source` as `(bucket, key)`: "bucket/key" or "/bucket/key",
/// URL-encoded, optionally carrying "?versionId=...".
fn parse_copy_source(raw: &str) -> Option<(String, String)> {
    let source = raw.split('?').next().unwrap_or(raw).trim_start_matches('/');
    // `urlencoding::decode` leaves malformed `%` escapes untouched; reject them.
    if source.split('%').skip(1).any(|s| {
        s.as_bytes()
            .get(..2)
            .is_none_or(|h| !h[0].is_ascii_hexdigit() || !h[1].is_ascii_hexdigit())
    }) {
        return None;
    }
    let decoded = urlencoding::decode(source).ok()?;
    let (bucket, key) = decoded.split_once('/')?;
    (!bucket.is_empty() && !key.is_empty()).then(|| (bucket.to_string(), key.to_string()))
}

#[cfg(test)]
mod copy_result_tests {
    use super::copy_result_xml;
    use crate::s3::dto::S3_NAMESPACE;

    #[test]
    fn copy_results_carry_the_namespace() {
        for root in ["CopyObjectResult", "CopyPartResult"] {
            let xml = copy_result_xml(root, "\"abc\"");
            let open = format!("<{root} xmlns=\"{S3_NAMESPACE}\">");
            assert!(xml.contains(&open), "{xml}");
            assert!(xml.contains("<ETag>\"abc\"</ETag>"), "{xml}");
            assert!(xml.ends_with(&format!("</{root}>")), "{xml}");
        }
    }
}

#[cfg(test)]
mod location_tests {
    use super::location_constraint_response;

    async fn body(region: &str) -> String {
        let response = location_constraint_response(region);
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["content-type"], "application/xml");
        let body = response.into_body();
        let bytes = axum::body::to_bytes(body, 4096).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    /// S3 reports us-east-1 as an empty constraint and every other region by
    /// name, including regions the handler used to replace with us-east-1.
    #[tokio::test]
    async fn location_constraint_names_the_region_as_s3_does() {
        let open = r#"<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#;
        let empty = body("us-east-1").await;
        let expected = format!("{open}</LocationConstraint>");
        assert!(empty.ends_with(&expected), "{empty}");
        for region in ["eu-central-1", "us-east-2", "cn-north-1"] {
            let xml = body(region).await;
            let expected = format!("{open}{region}</LocationConstraint>");
            assert!(xml.ends_with(&expected), "{xml}");
        }
    }
}

#[cfg(test)]
mod copy_status_tests {
    use super::{copy_error_status, parse_copy_source};
    use axum::http::StatusCode;

    #[test]
    fn copy_source_decodes_once_and_rejects_malformed_escapes() {
        assert_eq!(
            parse_copy_source("/bucket/key%25ZZ?versionId=null"),
            Some(("bucket".into(), "key%ZZ".into()))
        );
        assert_eq!(
            parse_copy_source("bucket/a%2Fb%20c"),
            Some(("bucket".into(), "a/b c".into()))
        );
        for raw in ["b/k%ZZ", "b/k%", "b/k%A", "b/%FF", "bucket", "/bucket/"] {
            assert!(parse_copy_source(raw).is_none(), "{raw}");
        }
    }

    #[test]
    fn copy_errors_use_the_protocol_mapping() {
        assert_eq!(
            copy_error_status("NoSuchKey: copy source b/k does not exist"),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            copy_error_status("PreconditionFailed: copy source condition returned 412"),
            StatusCode::PRECONDITION_FAILED
        );
        // A reserved staging destination must reach the client as a 400.
        assert_eq!(
            copy_error_status("InvalidArgument: object keys may not contain a segment"),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            copy_error_status("Failed to write copy destination: io"),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}

#[cfg(test)]
mod sub_resource_tests {
    use super::{unsupported_sub_resource, S3Router};
    use axum::body::Body;
    use axum::http::{Method, StatusCode};

    #[test]
    fn unsupported_sub_resources_are_found_in_any_position() {
        for (query, sub_resource) in [
            ("tagging", "tagging"),
            ("tagging=", "tagging"),
            ("versionId=3&tagging", "tagging"),
            ("acl", "acl"),
            ("versioning", "versioning"),
            ("cors", "cors"),
            ("object-lock", "object-lock"),
            ("select&select-type=2", "select"),
        ] {
            assert_eq!(
                unsupported_sub_resource(Some(query)),
                Some(sub_resource),
                "{query}"
            );
        }
    }

    #[test]
    fn supported_queries_pass() {
        assert_eq!(unsupported_sub_resource(None), None);
        for query in [
            "",
            "list-type=2&prefix=a%2F",
            "location",
            "versions",
            "uploads",
            "uploadId=u1&partNumber=2",
            "delete",
            "versionId=null",
            "response-content-type=text%2Fplain",
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=a%2Fb&X-Amz-Signature=x",
            // A sub-resource name used as a value is not a sub-resource.
            "prefix=tagging",
        ] {
            assert_eq!(unsupported_sub_resource(Some(query)), None, "{query}");
        }
    }

    async fn status_of(method: Method, uri: &str) -> StatusCode {
        let req = axum::extract::Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::from("<Tagging><TagSet/></Tagging>"))
            .expect("build request");
        S3Router::route(req).await.status()
    }

    /// These requests used to reach PutObject, DeleteObject, DeleteBucket,
    /// CreateBucket and ListObjects V1. They now stop before any handler runs,
    /// so none is registered here.
    #[tokio::test]
    async fn unsupported_sub_resources_answer_not_implemented() {
        for (method, uri) in [
            (Method::PUT, "/bucket/key?tagging"),
            (Method::DELETE, "/bucket/key?tagging"),
            (Method::PUT, "/bucket/key?acl"),
            (Method::DELETE, "/bucket?cors"),
            (Method::PUT, "/bucket?versioning"),
            (Method::GET, "/bucket?versioning"),
        ] {
            let status = status_of(method.clone(), uri).await;
            assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{method} {uri}");
        }
    }

    /// Supported requests still reach dispatch. With no handlers registered
    /// they fail there, but never with NotImplemented.
    #[tokio::test]
    async fn supported_requests_still_reach_dispatch() {
        for (method, uri) in [
            (Method::GET, "/bucket?location"),
            (Method::PUT, "/bucket/key"),
            (Method::POST, "/bucket?delete"),
        ] {
            let status = status_of(method.clone(), uri).await;
            assert_ne!(status, StatusCode::NOT_IMPLEMENTED, "{method} {uri}");
        }
    }
}

#[cfg(test)]
mod list_v1_tests {
    use super::{is_list_objects_v1_request, Request};
    use axum::body::Body;

    fn request(uri: &str) -> Request {
        axum::extract::Request::builder()
            .uri(uri)
            .body(Body::empty())
            .expect("build request")
            .into()
    }

    #[test]
    fn a_bare_bucket_get_is_a_v1_listing() {
        assert!(is_list_objects_v1_request(&request("/bucket")));
    }

    #[test]
    fn v1_listing_parameters_select_v1() {
        for uri in [
            "/bucket?marker=a",
            "/bucket?prefix=x/",
            "/bucket?delimiter=/",
            "/bucket?max-keys=2",
            "/bucket?encoding-type=url",
        ] {
            assert!(is_list_objects_v1_request(&request(uri)), "{uri}");
        }
    }

    /// An object GET has no query string either, so the path depth is what
    /// separates it from a bucket listing.
    #[test]
    fn object_paths_are_never_v1_listings() {
        for uri in [
            "/bucket/some/key",
            "/bucket/some/key?prefix=x",
            "/bucket/key/",
        ] {
            assert!(!is_list_objects_v1_request(&request(uri)), "{uri}");
        }
    }

    /// A presigned listing carries only X-Amz-* signing parameters.
    #[test]
    fn presigned_bucket_listings_are_v1() {
        assert!(is_list_objects_v1_request(&request(
            "/bucket?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=a%2Fb&X-Amz-Signature=x"
        )));
    }

    #[test]
    fn bucket_sub_resources_are_not_v1_listings() {
        for uri in [
            "/bucket?location",
            "/bucket?versions",
            "/bucket?uploads",
            "/bucket?acl",
            // #2100: SDKs send listing parameters along with `?versions`.
            "/bucket?versions=&encoding-type=url",
            "/bucket?versions&prefix=nested/&delimiter=/",
            "/bucket?versions&max-keys=2&key-marker=a",
        ] {
            assert!(!is_list_objects_v1_request(&request(uri)), "{uri}");
        }
    }
}
