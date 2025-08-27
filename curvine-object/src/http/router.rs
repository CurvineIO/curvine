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

//! S3 API request router
//!
//! This module handles routing of S3 API requests based on HTTP methods
//! and provides dedicated handlers for each type of S3 operation.

use crate::http::axum::{Request, Response};
use crate::s3::s3_api::*;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use std::sync::Arc;

/// S3 request router that delegates requests to appropriate handlers
pub struct S3Router;

impl S3Router {
    /// Route S3 request based on HTTP method
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

    /// Handle PUT requests (object upload, bucket creation, multipart upload)
    async fn handle_put_request(
        req: axum::extract::Request<axum::body::Body>,
    ) -> axum::response::Response {
        let multipart_obj = req
            .extensions()
            .get::<Arc<dyn MultiUploadObjectHandler + Send + Sync>>()
            .cloned();
        let put_obj = req
            .extensions()
            .get::<Arc<dyn PutObjectHandler + Sync + Send>>()
            .cloned();
        let create_bkt_obj = req
            .extensions()
            .get::<Arc<dyn CreateBucketHandler + Sync + Send>>()
            .cloned();
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
            Self::handle_create_bucket_request(req, create_bkt_obj).await
        } else {
            let xid = req.get_query("x-id");
            let upload_id = req.get_query("uploadId");
            let part_number = req.get_query("partNumber");

            // Check if this is an UploadPart request
            let is_upload_part = (xid.is_some() && xid.as_ref().unwrap().as_str() == "UploadPart")
                || (upload_id.is_some() && part_number.is_some());

            if is_upload_part {
                Self::handle_multipart_upload_part_request(req, multipart_obj).await
            } else {
                Self::handle_put_object_request(req, put_obj, v4head).await
            }
        }
    }

    /// Handle GET requests (object download, bucket listing, object listing)
    async fn handle_get_request(
        req: axum::extract::Request<axum::body::Body>,
    ) -> axum::response::Response {
        // Handle probe endpoint
        if req.uri().path().starts_with("/probe-bsign") {
            return (StatusCode::OK, b"").into_response();
        }

        let get_obj = req
            .extensions()
            .get::<Arc<dyn GetObjectHandler + Send + Sync>>()
            .cloned();
        let listbkt_obj = req
            .extensions()
            .get::<Arc<dyn ListBucketHandler + Send + Sync>>()
            .cloned();
        let listobj_obj = req
            .extensions()
            .get::<Arc<dyn ListObjectHandler + Send + Sync>>()
            .cloned();
        let getbkt_loc_obj = req
            .extensions()
            .get::<Arc<dyn GetBucketLocationHandler + Send + Sync>>()
            .cloned();

        let req = Request::from(req);
        let url_path = req.url_path();

        // Handle list operations
        if let Some(lt) = req.get_query("list-type") {
            if lt == "2" {
                if url_path.trim_start_matches('/').is_empty() {
                    return Self::handle_list_buckets_request(req, listbkt_obj).await;
                } else {
                    return Self::handle_list_objects_request(req, listobj_obj).await;
                }
            }
        } else if url_path.trim_start_matches('/').is_empty() {
            // root listing without list-type param => ListBuckets
            return Self::handle_list_buckets_request(req, listbkt_obj).await;
        }

        // Handle bucket location requests
        if let Some(loc) = req.get_query("location") {
            return Self::handle_get_bucket_location_request(req, getbkt_loc_obj, loc).await;
        }

        // Handle object download
        Self::handle_get_object_request(req, get_obj).await
    }

    /// Handle DELETE requests (object deletion, bucket deletion)
    async fn handle_delete_request(
        req: axum::extract::Request<axum::body::Body>,
    ) -> axum::response::Response {
        let path = req.uri().path().trim_start_matches('/');
        if path.is_empty() {
            return (StatusCode::BAD_REQUEST, b"").into_response();
        }

        let rr = path.split("/").collect::<Vec<&str>>();
        let rr_len = rr.len();

        if rr_len == 1 || (rr_len == 2 && rr[1].is_empty()) {
            Self::handle_delete_bucket_request(req).await
        } else {
            Self::handle_delete_object_request(req).await
        }
    }

    /// Handle HEAD requests (object metadata, bucket existence check)
    async fn handle_head_request(
        req: axum::extract::Request<axum::body::Body>,
    ) -> axum::response::Response {
        let head_obj = req
            .extensions()
            .get::<Arc<dyn HeadHandler + Sync + Send>>()
            .cloned();
        let listbkt_obj = req
            .extensions()
            .get::<Arc<dyn ListBucketHandler + Sync + Send>>()
            .cloned();

        if head_obj.is_none() {
            log::warn!("not open head features");
            return (StatusCode::INTERNAL_SERVER_ERROR, b"").into_response();
        }

        let head_obj = head_obj.unwrap();
        let req = Request::from(req);
        let raw_path = req.url_path();
        let args = raw_path
            .trim_start_matches('/')
            .splitn(2, '/')
            .collect::<Vec<&str>>();

        if args.len() == 1 {
            Self::handle_head_bucket_request(args[0], listbkt_obj).await
        } else if args.len() != 2 {
            (StatusCode::BAD_REQUEST, b"").into_response()
        } else {
            Self::handle_head_object_request(args[0], args[1], head_obj).await
        }
    }

    /// Handle POST requests (multipart upload session management)
    async fn handle_post_request(
        req: axum::extract::Request<axum::body::Body>,
    ) -> axum::response::Response {
        let multipart_obj = req
            .extensions()
            .get::<Arc<dyn MultiUploadObjectHandler + Send + Sync>>()
            .cloned();

        match multipart_obj {
            Some(multipart_obj) => {
                let query_string = req.uri().query();
                let is_create_session = if let Some(query) = query_string {
                    let has_uploads = query.contains("uploads=") || query.contains("uploads");
                    has_uploads
                } else {
                    false
                };

                let req = Request::from(req);
                if is_create_session {
                    Self::handle_multipart_create_session_request(req, multipart_obj).await
                } else if req.get_query("uploadId").is_some() {
                    Self::handle_multipart_complete_session_request(req, multipart_obj).await
                } else {
                    (StatusCode::BAD_REQUEST, b"").into_response()
                }
            }
            None => {
                log::warn!("not open multipart object features");
                (StatusCode::INTERNAL_SERVER_ERROR, b"").into_response()
            }
        }
    }

    // === Helper methods for specific operations ===

    /// Handle bucket creation
    async fn handle_create_bucket_request(
        req: Request,
        create_bkt_obj: Option<Arc<dyn CreateBucketHandler + Sync + Send>>,
    ) -> axum::response::Response {
        match create_bkt_obj {
            Some(create_bkt_obj) => {
                let mut resp = Response::default();
                handle_create_bucket(req, &mut resp, &create_bkt_obj).await;
                resp.into()
            }
            None => {
                log::warn!("not open create bucket method");
                (StatusCode::FORBIDDEN, b"").into_response()
            }
        }
    }

    /// Handle multipart upload part
    async fn handle_multipart_upload_part_request(
        req: Request,
        multipart_obj: Option<Arc<dyn MultiUploadObjectHandler + Send + Sync>>,
    ) -> axum::response::Response {
        match multipart_obj {
            Some(multipart_obj) => {
                let mut resp = Response::default();
                handle_multipart_upload_part(req, &mut resp, &multipart_obj).await;
                resp.into()
            }
            None => (StatusCode::INTERNAL_SERVER_ERROR, b"").into_response(),
        }
    }

    /// Handle object upload
    async fn handle_put_object_request(
        req: Request,
        put_obj: Option<Arc<dyn PutObjectHandler + Sync + Send>>,
        v4head: Option<crate::auth::sig_v4::V4Head>,
    ) -> axum::response::Response {
        match put_obj {
            Some(put_obj) => {
                let mut resp = Response::default();
                handle_put_object(v4head.unwrap(), req, &mut resp, &put_obj).await;
                resp.into()
            }
            None => {
                log::warn!("not open put object method");
                (StatusCode::FORBIDDEN, b"").into_response()
            }
        }
    }

    /// Handle bucket listing
    async fn handle_list_buckets_request(
        req: Request,
        listbkt_obj: Option<Arc<dyn ListBucketHandler + Send + Sync>>,
    ) -> axum::response::Response {
        log::info!("is list buckets");
        match listbkt_obj {
            Some(listbkt_obj) => {
                let mut resp = Response::default();
                handle_get_list_buckets(req, &mut resp, &listbkt_obj).await;
                resp.into()
            }
            None => {
                log::warn!("not open list buckets method");
                (StatusCode::FORBIDDEN, b"").into_response()
            }
        }
    }

    /// Handle object listing
    async fn handle_list_objects_request(
        req: Request,
        listobj_obj: Option<Arc<dyn ListObjectHandler + Send + Sync>>,
    ) -> axum::response::Response {
        log::info!("is list objects");
        match listobj_obj {
            Some(listobj_obj) => {
                let mut resp = Response::default();
                handle_get_list_object(req, &mut resp, &listobj_obj).await;
                resp.into()
            }
            None => {
                log::warn!("not open list objects method");
                (StatusCode::FORBIDDEN, b"").into_response()
            }
        }
    }

    /// Handle bucket location retrieval - simplified to avoid private module access
    async fn handle_get_bucket_location_request(
        _req: Request,
        getbkt_loc_obj: Option<Arc<dyn GetBucketLocationHandler + Send + Sync>>,
        loc: String,
    ) -> axum::response::Response {
        match getbkt_loc_obj {
            Some(bkt) => {
                match bkt
                    .handle(if loc.is_empty() { None } else { Some(&loc) })
                    .await
                {
                    Ok(location) => {
                        // Simple XML response without complex struct
                        let xml_content = match location {
                            Some(loc) => {
                                format!("<LocationConstraint>{}</LocationConstraint>", loc)
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
            None => {
                log::warn!("not open get bucket location method");
                (StatusCode::FORBIDDEN, b"").into_response()
            }
        }
    }

    /// Handle object download
    async fn handle_get_object_request(
        req: Request,
        get_obj: Option<Arc<dyn GetObjectHandler + Send + Sync>>,
    ) -> axum::response::Response {
        match get_obj {
            Some(obj) => {
                let mut resp = Response::default();
                handle_get_object(req, &mut resp, &obj).await;
                resp.into()
            }
            None => {
                log::warn!("not open get object method");
                (StatusCode::FORBIDDEN, b"").into_response()
            }
        }
    }

    /// Handle bucket deletion
    async fn handle_delete_bucket_request(
        req: axum::extract::Request<axum::body::Body>,
    ) -> axum::response::Response {
        match req
            .extensions()
            .get::<Arc<dyn DeleteBucketHandler + Send + Sync>>()
            .cloned()
        {
            Some(delete_bkt_obj) => {
                let mut resp = Response::default();
                handle_delete_bucket(Request::from(req), &mut resp, &delete_bkt_obj).await;
                resp.into()
            }
            None => {
                log::warn!("not open get delete bucket method");
                (StatusCode::FORBIDDEN, b"").into_response()
            }
        }
    }

    /// Handle object deletion
    async fn handle_delete_object_request(
        req: axum::extract::Request<axum::body::Body>,
    ) -> axum::response::Response {
        match req
            .extensions()
            .get::<Arc<dyn DeleteObjectHandler + Send + Sync>>()
            .cloned()
        {
            Some(delete_obj_obj) => {
                let mut resp = Response::default();
                handle_delete_object(Request::from(req), &mut resp, &delete_obj_obj).await;
                resp.into()
            }
            None => {
                log::warn!("not open get delete object method");
                (StatusCode::FORBIDDEN, b"").into_response()
            }
        }
    }

    /// Handle bucket existence check (HEAD bucket)
    async fn handle_head_bucket_request(
        bucket_name: &str,
        listbkt_obj: Option<Arc<dyn ListBucketHandler + Sync + Send>>,
    ) -> axum::response::Response {
        if let Some(listbkt_obj) = listbkt_obj {
            let opt = crate::s3::ListBucketsOption {
                bucket_region: None,
                continuation_token: None,
                max_buckets: None,
                prefix: None,
            };
            match listbkt_obj.handle(&opt).await {
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
        } else {
            (StatusCode::FORBIDDEN, b"").into_response()
        }
    }

    /// Handle object metadata retrieval (HEAD object)
    async fn handle_head_object_request(
        bucket: &str,
        object: &str,
        head_obj: Arc<dyn HeadHandler + Sync + Send>,
    ) -> axum::response::Response {
        match head_obj.lookup(bucket, object).await {
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
                    // Fix for leading zeros: prevent HEAD response mixing with GET response
                    resp.set_header("Connection", "close");
                    // Keep original content-length for S3 compatibility
                    resp.set_header("X-Head-Response", "true"); // Mark as HEAD response
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

    /// Handle multipart upload session creation
    async fn handle_multipart_create_session_request(
        req: Request,
        multipart_obj: Arc<dyn MultiUploadObjectHandler + Send + Sync>,
    ) -> axum::response::Response {
        let mut resp = Response::default();
        handle_multipart_create_session(req, &mut resp, &multipart_obj).await;
        resp.into()
    }

    /// Handle multipart upload session completion
    async fn handle_multipart_complete_session_request(
        req: Request,
        multipart_obj: Arc<dyn MultiUploadObjectHandler + Send + Sync>,
    ) -> axum::response::Response {
        let mut resp = Response::default();
        handle_multipart_complete_session(req, &mut resp, &multipart_obj).await;
        resp.into()
    }
}
