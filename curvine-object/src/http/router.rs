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
//!
//! The S3Router follows a hierarchical approach:
//! 1. Route by HTTP method (PUT, GET, DELETE, HEAD, POST)
//! 2. Determine operation type based on URL path and query parameters
//! 3. Delegate to specific operation handlers
//! 4. Handle errors and responses uniformly

use crate::http::axum::{Request, Response};
use crate::s3::s3_api::*;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use std::sync::Arc;

/// S3 request router that delegates requests to appropriate handlers
///
/// This is the main entry point for all S3 API requests. It analyzes the HTTP method
/// and URL path to determine the appropriate S3 operation and routes the request
/// to the corresponding handler method.
pub struct S3Router;

impl S3Router {
    /// Route S3 request based on HTTP method
    ///
    /// This is the main routing entry point that analyzes the incoming HTTP request
    /// and dispatches it to the appropriate method-specific handler.
    ///
    /// # Arguments
    /// * `req` - The incoming HTTP request with Axum body
    ///
    /// # Returns
    /// * `axum::response::Response` - HTTP response for the request
    ///
    /// # Supported HTTP Methods
    /// * PUT - Object upload, bucket creation, multipart upload parts
    /// * GET - Object download, bucket/object listing, bucket location
    /// * DELETE - Object/bucket deletion
    /// * HEAD - Object metadata, bucket existence check
    /// * POST - Multipart upload session management
    ///
    /// # Error Handling
    /// Returns METHOD_NOT_ALLOWED (405) for unsupported HTTP methods
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
    ///
    /// Analyzes the URL path to determine the type of PUT operation:
    /// - Single path segment: Bucket creation
    /// - Two path segments with multipart query params: Multipart upload part
    /// - Two path segments without multipart params: Object upload
    ///
    /// # Arguments
    /// * `req` - The HTTP PUT request
    ///
    /// # Returns
    /// * `axum::response::Response` - Response for the PUT operation
    ///
    /// # Path Analysis
    /// * `/bucket` or `/bucket/` - Create bucket operation
    /// * `/bucket/object?uploadId=xyz&partNumber=1` - Multipart upload part
    /// * `/bucket/object` - Regular object upload
    ///
    /// # Error Handling
    /// Returns BAD_REQUEST (400) if path parsing fails or arguments are invalid
    async fn handle_put_request(
        req: axum::extract::Request<axum::body::Body>,
    ) -> axum::response::Response {
        // Extract required handlers from request extensions
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

        // Parse URL path to determine operation type
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

        // Check if this is a bucket creation request
        let is_create_bkt = rpath_len == 1 || (rpath_len == 2 && rpath[1].is_empty());
        let req = Request::from(req);

        if is_create_bkt {
            Self::handle_create_bucket_request(req, create_bkt_obj).await
        } else {
            // Analyze query parameters to distinguish between multipart and regular upload
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
    ///
    /// Analyzes URL path and query parameters to determine the GET operation:
    /// - Root path with list-type=2: Bucket or object listing
    /// - Query parameter "location": Bucket location request
    /// - /probe-bsign: Health check endpoint
    /// - Object path: Object download with optional range support
    ///
    /// # Arguments
    /// * `req` - The HTTP GET request
    ///
    /// # Returns
    /// * `axum::response::Response` - Response for the GET operation
    ///
    /// # Supported Operations
    /// * Object download with Range header support (HTTP 206 Partial Content)
    /// * Bucket listing (ListBuckets API)
    /// * Object listing (ListObjectsV2 API)
    /// * Bucket location retrieval
    /// * Health check probe endpoint
    ///
    /// # Query Parameters
    /// * `list-type=2` - Triggers S3 ListObjectsV2 API
    /// * `location` - Triggers GetBucketLocation API
    /// * Range headers are automatically processed for partial downloads
    async fn handle_get_request(
        req: axum::extract::Request<axum::body::Body>,
    ) -> axum::response::Response {
        // Handle probe endpoint for health checks
        if req.uri().path().starts_with("/probe-bsign") {
            return (StatusCode::OK, b"").into_response();
        }

        // Extract required handlers from request extensions
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

        // Handle list operations based on list-type query parameter
        if let Some(lt) = req.get_query("list-type") {
            if lt == "2" {
                if url_path.trim_start_matches('/').is_empty() {
                    return Self::handle_list_buckets_request(req, listbkt_obj).await;
                } else {
                    return Self::handle_list_objects_request(req, listobj_obj).await;
                }
            }
        } else if url_path.trim_start_matches('/').is_empty() {
            // Root listing without list-type param defaults to ListBuckets
            return Self::handle_list_buckets_request(req, listbkt_obj).await;
        }

        // Handle bucket location requests
        if let Some(loc) = req.get_query("location") {
            return Self::handle_get_bucket_location_request(req, getbkt_loc_obj, loc).await;
        }

        // Default to object download
        Self::handle_get_object_request(req, get_obj).await
    }

    /// Handle DELETE requests (object deletion, bucket deletion)
    ///
    /// Analyzes the URL path to determine whether to delete an object or bucket:
    /// - Single path segment: Delete bucket
    /// - Multiple path segments: Delete object
    ///
    /// # Arguments
    /// * `req` - The HTTP DELETE request
    ///
    /// # Returns
    /// * `axum::response::Response` - Response for the DELETE operation
    ///
    /// # Path Analysis
    /// * `/bucket` or `/bucket/` - Delete bucket operation
    /// * `/bucket/object/path` - Delete object operation
    ///
    /// # Error Handling
    /// Returns BAD_REQUEST (400) if path is empty or invalid
    /// S3-compatible: Returns success even if object/bucket doesn't exist
    async fn handle_delete_request(
        req: axum::extract::Request<axum::body::Body>,
    ) -> axum::response::Response {
        let path = req.uri().path().trim_start_matches('/');
        if path.is_empty() {
            return (StatusCode::BAD_REQUEST, b"").into_response();
        }

        // Split path to determine if it's bucket or object deletion
        let rr = path.split("/").collect::<Vec<&str>>();
        let rr_len = rr.len();

        if rr_len == 1 || (rr_len == 2 && rr[1].is_empty()) {
            Self::handle_delete_bucket_request(req).await
        } else {
            Self::handle_delete_object_request(req).await
        }
    }

    /// Handle HEAD requests (object metadata, bucket existence check)
    ///
    /// Analyzes the URL path to determine HEAD operation type:
    /// - Single path segment: Check bucket existence
    /// - Two path segments: Get object metadata
    ///
    /// # Arguments
    /// * `req` - The HTTP HEAD request
    ///
    /// # Returns
    /// * `axum::response::Response` - Response with headers but no body
    ///
    /// # HEAD Response Headers
    /// For objects: content-length, etag, content-type, last-modified
    /// For buckets: Basic existence check (200 OK or 404 Not Found)
    ///
    /// # Error Handling
    /// Returns INTERNAL_SERVER_ERROR (500) if HEAD handler is not configured
    /// Returns BAD_REQUEST (400) for invalid path structures
    /// Returns NOT_FOUND (404) if object/bucket doesn't exist
    async fn handle_head_request(
        req: axum::extract::Request<axum::body::Body>,
    ) -> axum::response::Response {
        // Extract HEAD handler from request extensions
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

        // Parse path segments to determine operation type
        let args = raw_path
            .trim_start_matches('/')
            .splitn(2, '/')
            .collect::<Vec<&str>>();

        if args.len() == 1 {
            // HEAD bucket - check existence
            Self::handle_head_bucket_request(args[0], listbkt_obj).await
        } else if args.len() != 2 {
            (StatusCode::BAD_REQUEST, b"").into_response()
        } else {
            // HEAD object - get metadata
            Self::handle_head_object_request(args[0], args[1], head_obj).await
        }
    }

    /// Handle POST requests (multipart upload session management)
    ///
    /// Analyzes query parameters to determine multipart upload operation:
    /// - "uploads" query parameter: Create multipart upload session
    /// - "uploadId" query parameter: Complete multipart upload session
    ///
    /// # Arguments
    /// * `req` - The HTTP POST request
    ///
    /// # Returns
    /// * `axum::response::Response` - Response for the POST operation
    ///
    /// # Multipart Upload Flow
    /// 1. POST with ?uploads - Creates upload session, returns uploadId
    /// 2. PUT with ?uploadId&partNumber=N - Upload individual parts
    /// 3. POST with ?uploadId - Complete upload with part list
    ///
    /// # Error Handling
    /// Returns INTERNAL_SERVER_ERROR (500) if multipart handler not configured
    /// Returns BAD_REQUEST (400) for invalid query parameters
    async fn handle_post_request(
        req: axum::extract::Request<axum::body::Body>,
    ) -> axum::response::Response {
        // Extract multipart upload handler from request extensions
        let multipart_obj = req
            .extensions()
            .get::<Arc<dyn MultiUploadObjectHandler + Send + Sync>>()
            .cloned();

        match multipart_obj {
            Some(multipart_obj) => {
                // Analyze query string to determine multipart operation
                let query_string = req.uri().query();
                let is_create_session = if let Some(query) = query_string {
                    let has_uploads = query.contains("uploads=") || query.contains("uploads");
                    has_uploads
                } else {
                    false
                };

                let req = Request::from(req);
                if is_create_session {
                    // Create new multipart upload session
                    Self::handle_multipart_create_session_request(req, multipart_obj).await
                } else if req.get_query("uploadId").is_some() {
                    // Complete existing multipart upload session
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

    /// Handle bucket creation request
    ///
    /// Creates a new S3 bucket using the provided bucket handler.
    ///
    /// # Arguments
    /// * `req` - The processed request object
    /// * `create_bkt_obj` - Optional bucket creation handler
    ///
    /// # Returns
    /// * `axum::response::Response` - Success or error response
    ///
    /// # S3 Compatibility
    /// - Returns 200 OK on successful bucket creation
    /// - Returns 403 Forbidden if bucket creation is disabled
    /// - Supports standard S3 bucket creation headers and options
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

    /// Handle multipart upload part request
    ///
    /// Processes a single part of a multipart upload operation.
    /// Each part is identified by uploadId and partNumber query parameters.
    ///
    /// # Arguments
    /// * `req` - The processed request object
    /// * `multipart_obj` - Optional multipart upload handler
    ///
    /// # Returns
    /// * `axum::response::Response` - Success with ETag or error response
    ///
    /// # Multipart Upload Details
    /// - Each part gets a unique ETag for integrity verification
    /// - Part numbers must be between 1 and 10,000
    /// - Parts can be uploaded in any order
    /// - Minimum part size is 5MB (except for the last part)
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

    /// Handle object upload request
    ///
    /// Processes regular object upload (non-multipart) with full S3 compatibility.
    /// Supports various content types, checksums, and metadata.
    ///
    /// # Arguments
    /// * `req` - The processed request object
    /// * `put_obj` - Optional object upload handler
    /// * `v4head` - S3 V4 signature verification data
    ///
    /// # Returns
    /// * `axum::response::Response` - Success or error response
    ///
    /// # Features Supported
    /// - S3 V4 signature verification
    /// - Content-MD5 and SHA256 checksums
    /// - Custom metadata headers
    /// - Storage class specification
    /// - Content-Type detection and override
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

    /// Handle bucket listing request
    ///
    /// Returns a list of all buckets accessible to the requesting user.
    /// Implements S3 ListBuckets API with proper XML formatting.
    ///
    /// # Arguments
    /// * `req` - The processed request object
    /// * `listbkt_obj` - Optional bucket listing handler
    ///
    /// # Returns
    /// * `axum::response::Response` - XML-formatted bucket list or error
    ///
    /// # Response Format
    /// Returns XML document with ListAllMyBucketsResult structure:
    /// - Owner information
    /// - Bucket names and creation dates
    /// - Region information for each bucket
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

    /// Handle object listing request
    ///
    /// Returns a list of objects within a specified bucket.
    /// Implements S3 ListObjectsV2 API with pagination and filtering support.
    ///
    /// # Arguments
    /// * `req` - The processed request object
    /// * `listobj_obj` - Optional object listing handler
    ///
    /// # Returns
    /// * `axum::response::Response` - XML-formatted object list or error
    ///
    /// # Query Parameters Supported
    /// - prefix: Filter objects by key prefix
    /// - delimiter: Group keys by delimiter
    /// - max-keys: Limit number of objects returned
    /// - continuation-token: Pagination support
    /// - start-after: Start listing after specific key
    async fn handle_list_objects_request(
        req: Request,
        listobj_obj: Option<Arc<dyn ListObjectHandler + Send + Sync>>,
    ) -> axum::response::Response {
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

    /// Handle bucket location retrieval request
    ///
    /// Returns the AWS region where the bucket is located.
    /// Simplified implementation that avoids complex XML structure dependencies.
    ///
    /// # Arguments
    /// * `_req` - The processed request object (unused in current implementation)
    /// * `getbkt_loc_obj` - Optional bucket location handler
    /// * `loc` - Location query parameter value
    ///
    /// # Returns
    /// * `axum::response::Response` - XML-formatted location or error
    ///
    /// # Implementation Note
    /// Uses simple XML string formatting instead of complex struct serialization
    /// to avoid private module access issues with LocationConstraint.
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

    /// Handle object download request
    ///
    /// Downloads object content with support for Range requests (partial downloads).
    /// Implements full S3 GetObject API compatibility.
    ///
    /// # Arguments
    /// * `req` - The processed request object
    /// * `get_obj` - Optional object download handler
    ///
    /// # Returns
    /// * `axum::response::Response` - Object content or error
    ///
    /// # Features Supported
    /// - Full object download
    /// - Range requests (HTTP 206 Partial Content)
    /// - Proper Content-Type detection
    /// - ETag and Last-Modified headers
    /// - Content-Length header for both full and partial downloads
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

    /// Handle bucket deletion request
    ///
    /// Deletes an empty bucket. S3-compatible implementation that prevents
    /// deletion of non-empty buckets.
    ///
    /// # Arguments
    /// * `req` - The raw HTTP request (needed for extension access)
    ///
    /// # Returns
    /// * `axum::response::Response` - Success (204 No Content) or error
    ///
    /// # S3 Compatibility Notes
    /// - Returns 204 No Content on successful deletion
    /// - Returns 409 Conflict if bucket is not empty
    /// - Returns 404 Not Found if bucket doesn't exist
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

    /// Handle object deletion request
    ///
    /// Deletes a specific object from a bucket. S3-compatible implementation
    /// that returns success even if the object doesn't exist.
    ///
    /// # Arguments
    /// * `req` - The raw HTTP request (needed for extension access)
    ///
    /// # Returns
    /// * `axum::response::Response` - Success (204 No Content) or error
    ///
    /// # S3 Compatibility Notes
    /// - Returns 204 No Content on successful deletion
    /// - Returns 204 No Content even if object doesn't exist (idempotent)
    /// - Supports versioned object deletion through query parameters
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
    ///
    /// Checks if a bucket exists and is accessible to the requesting user.
    /// Returns only HTTP status code without response body.
    ///
    /// # Arguments
    /// * `bucket_name` - Name of the bucket to check
    /// * `listbkt_obj` - Optional bucket listing handler for existence verification
    ///
    /// # Returns
    /// * `axum::response::Response` - 200 OK if exists, 404 Not Found if not
    ///
    /// # Implementation Details
    /// Uses the ListBuckets handler to verify bucket existence rather than
    /// a dedicated HEAD bucket handler. This ensures consistency with
    /// the bucket listing functionality.
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
    ///
    /// Returns object metadata as HTTP headers without the object content.
    /// Essential for S3 compatibility and object existence verification.
    ///
    /// # Arguments
    /// * `bucket` - Name of the bucket containing the object
    /// * `object` - Name/key of the object
    /// * `head_obj` - Object metadata handler
    ///
    /// # Returns
    /// * `axum::response::Response` - Headers with metadata or error
    ///
    /// # Response Headers Included
    /// - content-length: Object size in bytes
    /// - etag: Object ETag for integrity verification
    /// - content-type: MIME type of the object
    /// - last-modified: Object modification timestamp
    /// - Connection: close (prevents response mixing issues)
    /// - X-Head-Response: true (debug marker)
    ///
    /// # Error Responses
    /// - 404 Not Found: Object doesn't exist
    /// - 500 Internal Server Error: Metadata lookup failed
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

                    // Set standard S3 object metadata headers
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
    ///
    /// Initiates a new multipart upload session for large objects.
    /// Returns an uploadId that must be used for subsequent part uploads.
    ///
    /// # Arguments
    /// * `req` - The processed request object
    /// * `multipart_obj` - Multipart upload handler
    ///
    /// # Returns
    /// * `axum::response::Response` - XML response with uploadId or error
    ///
    /// # Response Format
    /// Returns XML document with InitiateMultipartUploadResult:
    /// - Bucket name
    /// - Object key
    /// - Upload ID for subsequent operations
    ///
    /// # Usage Flow
    /// 1. Client calls this endpoint to get uploadId
    /// 2. Client uploads parts using PUT with uploadId and partNumber
    /// 3. Client completes upload using POST with uploadId and part list
    async fn handle_multipart_create_session_request(
        req: Request,
        multipart_obj: Arc<dyn MultiUploadObjectHandler + Send + Sync>,
    ) -> axum::response::Response {
        let mut resp = Response::default();
        handle_multipart_create_session(req, &mut resp, &multipart_obj).await;
        resp.into()
    }

    /// Handle multipart upload session completion
    ///
    /// Completes a multipart upload by combining all uploaded parts into
    /// a single object. Requires a list of part numbers and their ETags.
    ///
    /// # Arguments
    /// * `req` - The processed request object containing part list
    /// * `multipart_obj` - Multipart upload handler
    ///
    /// # Returns
    /// * `axum::response::Response` - XML response with completion details or error
    ///
    /// # Request Body Format
    /// Expects XML document with CompleteMultipartUpload structure:
    /// - Array of Part elements with PartNumber and ETag
    /// - Parts can be listed in any order (will be sorted internally)
    ///
    /// # Response Format
    /// Returns XML document with CompleteMultipartUploadResult:
    /// - Location: Object URL
    /// - Bucket and Key: Object location
    /// - ETag: Final object ETag
    ///
    /// # Error Conditions
    /// - Invalid XML format in request body
    /// - Missing or invalid part ETags
    /// - Parts not found or corrupted
    async fn handle_multipart_complete_session_request(
        req: Request,
        multipart_obj: Arc<dyn MultiUploadObjectHandler + Send + Sync>,
    ) -> axum::response::Response {
        let mut resp = Response::default();
        handle_multipart_complete_session(req, &mut resp, &multipart_obj).await;
        resp.into()
    }
}
