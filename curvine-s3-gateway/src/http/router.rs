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

use crate::http::axum::{Request, Response};
use crate::s3::handlers::S3Handlers;
use crate::s3::s3_api::*;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use std::sync::Arc;

#[inline]
fn get_handlers(req: &axum::extract::Request<axum::body::Body>) -> Option<Arc<S3Handlers>> {
    req.extensions().get::<Arc<S3Handlers>>().cloned()
}

pub struct S3Router;

impl S3Router {
    pub async fn route(req: axum::extract::Request<axum::body::Body>) -> axum::response::Response {
        match *req.method() {
            axum::http::Method::PUT => Self::handle_put_request(req).await,
            axum::http::Method::GET => Self::handle_get_request(req).await,
            axum::http::Method::DELETE => Self::handle_delete_request(req).await,
            axum::http::Method::HEAD => Self::handle_head_request(req).await,
            axum::http::Method::POST => Self::handle_post_request(req).await,
            _ => (StatusCode::METHOD_NOT_ALLOWED, b"").into_response(),
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
                return (StatusCode::INTERNAL_SERVER_ERROR, b"").into_response();
            }
        };
        let v4head = req
            .extensions()
            .get::<crate::auth::sig_v4::V4Head>()
            .cloned();

        let path = req.uri().path();
        let rpath = path
            .trim_start_matches('/')
            .splitn(2, '/')
            .collect::<Vec<&str>>();

        let rpath_len = rpath.len();
        if rpath_len == 0 {
            log::info!("args length invalid");
            return (StatusCode::BAD_REQUEST, b"").into_response();
        }

        let is_create_bkt = rpath_len == 1 || (rpath_len == 2 && rpath[1].is_empty());
        let req = Request::from(req);

        if is_create_bkt {
            Self::handle_create_bucket_request(req, &handlers).await
        } else {
            let xid = req.get_query("x-id");
            let upload_id = req.get_query("uploadId");
            let part_number = req.get_query("partNumber");

            let is_upload_part = (xid.is_some() && xid.as_ref().unwrap().as_str() == "UploadPart")
                || (upload_id.is_some() && part_number.is_some());

            if is_upload_part {
                Self::handle_multipart_upload_part_request(req, &handlers).await
            } else {
                Self::handle_put_object_request(req, &handlers, v4head).await
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
                return (StatusCode::INTERNAL_SERVER_ERROR, b"").into_response();
            }
        };

        let req = Request::from(req);
        let url_path = req.url_path();

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
        }

        if let Some(loc) = req.get_query("location") {
            return Self::handle_get_bucket_location_request(req, &handlers, loc).await;
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
            return (StatusCode::BAD_REQUEST, b"").into_response();
        }

        // OPTIMIZED: Single concrete type extraction
        let handlers = match get_handlers(&req) {
            Some(h) => h,
            None => {
                tracing::error!("S3Handlers not found in request extensions");
                return (StatusCode::INTERNAL_SERVER_ERROR, b"").into_response();
            }
        };

        let rr = path.split("/").collect::<Vec<&str>>();
        let rr_len = rr.len();

        if rr_len == 1 || (rr_len == 2 && rr[1].is_empty()) {
            Self::handle_delete_bucket_request(Request::from(req), &handlers).await
        } else {
            Self::handle_delete_object_request(Request::from(req), &handlers).await
        }
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
            Self::handle_head_object_request(args[0], args[1], &handlers).await
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
                return (StatusCode::INTERNAL_SERVER_ERROR, b"").into_response();
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
            (StatusCode::BAD_REQUEST, b"").into_response()
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
    ) -> axum::response::Response {
        let mut resp = Response::default();
        handle_multipart_upload_part(req, &mut resp, handlers.as_ref()).await;
        resp.into()
    }

    async fn handle_put_object_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
        v4head: Option<crate::auth::sig_v4::V4Head>,
    ) -> axum::response::Response {
        if v4head.is_none() {
            tracing::warn!(
                "V4Head is None in handle_put_object_request - authentication may have failed"
            );
            return (StatusCode::FORBIDDEN, b"").into_response();
        }
        let mut resp = Response::default();
        handle_put_object(v4head.unwrap(), req, &mut resp, handlers.as_ref()).await;
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

    /// Handle object versions listing request - OPTIMIZED with concrete type
    async fn handle_list_object_versions_request(
        req: Request,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        let mut resp = Response::default();
        crate::s3::s3_api::handle_get_list_object_versions(req, &mut resp, handlers.as_ref()).await;
        resp.into()
    }

    async fn handle_get_bucket_location_request(
        _req: Request,
        handlers: &Arc<S3Handlers>,
        loc: String,
    ) -> axum::response::Response {
        // Direct method call - no trait object indirection
        match handlers
            .handle_get_bucket_location(if loc.is_empty() { None } else { Some(&loc) })
            .await
        {
            Ok(location) => {
                let xml_content = match location {
                    Some(loc) => {
                        format!("<LocationConstraint>{loc}</LocationConstraint>")
                    }
                    None => "<LocationConstraint></LocationConstraint>".to_string(),
                };
                (StatusCode::OK, xml_content).into_response()
            }
            Err(_) => {
                log::error!("get bucket location error");
                (StatusCode::INTERNAL_SERVER_ERROR, b"").into_response()
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
        bucket: &str,
        object: &str,
        handlers: &Arc<S3Handlers>,
    ) -> axum::response::Response {
        // Direct method call - no trait object indirection
        match handlers.lookup(bucket, object).await {
            Ok(metadata) => match metadata {
                Some(head) => {
                    use crate::auth::sig_v4::VHeader;
                    let mut resp = Response::default();

                    if let Some(v) = head.content_length {
                        resp.set_header("content-length", v.to_string().as_str())
                    }
                    if let Some(v) = head.etag {
                        resp.set_header("etag", &v);
                    }
                    if let Some(v) = head.content_type {
                        resp.set_header("content-type", &v);
                    }
                    if let Some(v) = head.last_modified {
                        resp.set_header("last-modified", &v);
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
                None => (StatusCode::NOT_FOUND, b"").into_response(),
            },
            Err(err) => {
                log::error!("lookup object metadata error {err}");
                (StatusCode::INTERNAL_SERVER_ERROR, b"").into_response()
            }
        }
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
}
