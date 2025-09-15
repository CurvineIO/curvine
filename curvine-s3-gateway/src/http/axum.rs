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

use std::collections::HashMap;

use axum::response::IntoResponse;
use futures::StreamExt;

use crate::auth::{sig_v4, AccesskeyStore};
use crate::s3::s3_api::VRequest;
use crate::s3::VRequestPlus;
use axum::body;
pub struct Request {
    request: axum::http::Request<axum::body::Body>,
    query: Option<HashMap<String, String>>,
}

impl Request {
    /// Get access to the request extensions
    pub fn extensions(&self) -> &axum::http::Extensions {
        self.request.extensions()
    }
}

impl From<Request> for axum::extract::Request {
    fn from(val: Request) -> Self {
        val.request
    }
}

impl From<axum::extract::Request> for Request {
    fn from(value: axum::extract::Request) -> Self {
        let query = value.uri().query().map(|query| {
            query
                .split("&")
                .map(|item| {
                    item.find("=")
                        .map_or((item.to_string(), "".to_string()), |pos| {
                            (item[..pos].to_string(), item[pos + 1..].to_string())
                        })
                })
                .collect::<std::collections::HashMap<String, String>>()
        });
        Self {
            request: value,
            query,
        }
    }
}

impl sig_v4::VHeader for Request {
    fn get_header(&self, key: &str) -> Option<String> {
        self.request
            .headers()
            .get(key)
            .and_then(|value| value.to_str().ok().map(|value| value.to_string()))
    }

    fn set_header(&mut self, key: &str, val: &str) {
        let key: axum::http::HeaderName = key.to_string().parse().unwrap();
        self.request.headers_mut().insert(key, val.parse().unwrap());
    }

    fn delete_header(&mut self, key: &str) {
        self.request.headers_mut().remove(key);
    }

    fn rng_header(&self, mut cb: impl FnMut(&str, &str) -> bool) {
        for (k, v) in self.request.headers().iter() {
            if !cb(k.as_str(), unsafe {
                std::str::from_utf8_unchecked(v.as_bytes())
            }) {
                return;
            }
        }
    }
}

impl VRequest for Request {
    fn method(&self) -> String {
        self.request.method().as_str().to_string()
    }

    fn url_path(&self) -> String {
        self.request.uri().path().to_string()
    }

    fn get_query(&self, k: &str) -> Option<String> {
        self.query
            .as_ref()
            .and_then(|query| query.get(k))
            .map(|v| v.to_string())
    }

    fn all_query(&self, mut cb: impl FnMut(&str, &str) -> bool) {
        self.query
            .as_ref()
            .map(|query| query.iter().all(|(k, v)| cb(k, v)));
    }
}

impl VRequestPlus for Request {
    fn body<'a>(
        self,
    ) -> std::pin::Pin<
        Box<dyn 'a + Send + std::future::Future<Output = Result<Vec<u8>, std::io::Error>>>,
    > {
        Box::pin(async move {
            let mut bodystream = self.request.into_body().into_data_stream();
            let mut ret = Vec::new();
            while let Some(bodystream) = bodystream.next().await {
                let bytes = bodystream.map_err(std::io::Error::other)?;
                ret.extend_from_slice(bytes.iter().as_slice());
            }
            Ok(ret)
        })
    }
}
pub struct BodyReader(body::BodyDataStream);

#[async_trait::async_trait]
impl crate::utils::io::PollRead for BodyReader {
    async fn poll_read(&mut self) -> Result<Option<Vec<u8>>, String> {
        let data = self.0.next().await;
        match data {
            Some(ret) => match ret {
                Ok(ret) => {
                    let vec_data = ret.to_vec();
                    // Reduce noisy logs in hot path
                    Ok(Some(vec_data))
                }
                Err(err) => Err(err.to_string()),
            },
            None => {
                log::debug!("HTTP-BODY-CHUNK: END OF STREAM");
                Ok(None)
            }
        }
    }
}

impl crate::s3::s3_api::BodyReader for Request {
    type BodyReader = BodyReader;

    fn get_body_reader<'b>(
        self,
    ) -> std::pin::Pin<
        Box<dyn 'b + Send + std::future::Future<Output = Result<Self::BodyReader, String>>>,
    > {
        Box::pin(async move {
            let ret: axum::body::Body = self.request.into_body();
            Ok(BodyReader(ret.into_data_stream()))
        })
    }
}
pub struct HeaderWarp(axum::http::HeaderMap);

impl sig_v4::VHeader for HeaderWarp {
    fn get_header(&self, key: &str) -> Option<String> {
        self.0
            .get(key)
            .and_then(|value| value.to_str().ok().map(|value| value.to_string()))
    }

    fn set_header(&mut self, key: &str, val: &str) {
        let key: axum::http::HeaderName = key.to_string().parse().unwrap();
        self.0.insert(key, val.parse().unwrap());
    }

    fn delete_header(&mut self, key: &str) {
        self.0.remove(key);
    }

    fn rng_header(&self, mut cb: impl FnMut(&str, &str) -> bool) {
        for (k, v) in self.0.iter() {
            if !cb(k.as_str(), unsafe {
                std::str::from_utf8_unchecked(v.as_bytes())
            }) {
                return;
            }
        }
    }
}

impl crate::s3::s3_api::HeaderTaker for Request {
    type Head = HeaderWarp;

    fn take_header(&self) -> Self::Head {
        HeaderWarp(self.request.headers().clone())
    }
}
#[derive(Default)]
pub struct Response {
    status: u16,
    headers: axum::http::HeaderMap,
    body: Vec<u8>,
}

impl From<Response> for axum::response::Response {
    fn from(val: Response) -> Self {
        let mut respbuilder = axum::response::Response::builder().status(if val.status == 0 {
            200
        } else {
            val.status
        });
        if !val.headers.is_empty() {
            if let Some(header) = respbuilder.headers_mut() {
                *header = val.headers;
            }
        }
        let raw = val.body;
        // log::info!("Response body length: {}", raw.len());
        respbuilder.body(raw.into()).unwrap()
    }
}

impl sig_v4::VHeader for Response {
    fn get_header(&self, key: &str) -> Option<String> {
        self.headers
            .get(key)
            .and_then(|v| v.to_str().ok().map(|v| v.to_string()))
    }

    fn set_header(&mut self, key: &str, val: &str) {
        log::debug!("set header {key} {val}");
        self.headers.insert(
            key.to_string().parse::<axum::http::HeaderName>().unwrap(),
            val.parse().unwrap(),
        );
    }

    fn delete_header(&mut self, key: &str) {
        self.headers.remove(key);
    }

    fn rng_header(&self, mut cb: impl FnMut(&str, &str) -> bool) {
        self.headers.iter().all(|(k, v)| {
            cb(k.as_str(), unsafe {
                std::str::from_utf8_unchecked(v.as_bytes())
            })
        });
    }
}
pub struct BodyWriter<'a>(&'a mut Vec<u8>);
#[async_trait::async_trait]
impl<'b> crate::utils::io::PollWrite for BodyWriter<'b> {
    async fn poll_write(&mut self, buff: &[u8]) -> Result<usize, std::io::Error> {
        // append to body buffer
        self.0.extend_from_slice(buff);
        Ok(buff.len())
    }
}

#[async_trait::async_trait]
impl crate::s3::s3_api::BodyWriter for Response {
    type BodyWriter<'a>
        = BodyWriter<'a>
    where
        Self: 'a;

    async fn get_body_writer(&mut self) -> Result<Self::BodyWriter<'_>, String> {
        Ok(BodyWriter(&mut self.body))
    }
}

impl crate::s3::s3_api::VResponse for Response {
    fn set_status(&mut self, status: u16) {
        self.status = status;
    }

    fn send_header(&mut self) {}
}

/// Optimized streaming GET object handler using ReaderStream (similar to s3s approach)
/// This avoids the MPSC channel overhead and provides better performance
pub async fn stream_get_object_optimized(
    req: super::axum::Request,
    handlers: &crate::s3::handlers::S3Handlers,
) -> axum::response::Response {
    use crate::s3::s3_api::{GetObjectOption, VRequest};
    use curvine_common::fs::{FileSystem, Reader};

    // Parse bucket and object from URL path
    let url_path = req.url_path();
    let path_parts: Vec<&str> = url_path.trim_start_matches('/').splitn(2, '/').collect();
    if path_parts.len() != 2 {
        return axum::response::Response::builder()
            .status(400)
            .body(axum::body::Body::from("Invalid path"))
            .unwrap();
    }

    let bucket = path_parts[0];
    let object = path_parts[1];

    // Parse range header if present
    use crate::auth::sig_v4::VHeader;
    let range_header = req.get_header("range");
    let (range_start, range_end) = if let Some(range_str) = range_header {
        parse_range_header(&range_str)
    } else {
        (None, None)
    };

    let opt = GetObjectOption {
        range_start,
        range_end,
    };

    // Get the curvine path
    let cv_path = match handlers.cv_object_path(bucket, object) {
        Ok(path) => path,
        Err(e) => {
            tracing::error!(
                "Failed to convert S3 path s3://{}/{}: {}",
                bucket,
                object,
                e
            );
            return axum::response::Response::builder()
                .status(404)
                .body(axum::body::Body::from("Not Found"))
                .unwrap();
        }
    };

    // Open the file
    let mut reader = match handlers.fs.open(&cv_path).await {
        Ok(reader) => reader,
        Err(e) => {
            tracing::error!("Failed to open file at path {}: {}", cv_path, e);
            return axum::response::Response::builder()
                .status(404)
                .body(axum::body::Body::from("Not Found"))
                .unwrap();
        }
    };

    // Handle range requests
    let file_size = reader.remaining().max(0) as u64;
    let (content_length, content_range, status_code) = if let Some(range_end) = opt.range_end {
        if range_end > u64::MAX / 2 {
            // Suffix range request
            let suffix_len = u64::MAX - range_end;
            if suffix_len > file_size {
                (file_size, None, 200)
            } else {
                let start_pos = file_size - suffix_len;
                if let Err(e) = reader.seek(start_pos as i64).await {
                    tracing::error!("Failed to seek to position {}: {}", start_pos, e);
                    return axum::response::Response::builder()
                        .status(500)
                        .body(axum::body::Body::from("Internal Server Error"))
                        .unwrap();
                }
                let content_range = format!("bytes {}-{}/{}", start_pos, file_size - 1, file_size);
                (suffix_len, Some(content_range), 206)
            }
        } else {
            // Normal range request
            let start = opt.range_start.unwrap_or(0);
            let end = std::cmp::min(range_end, file_size - 1);
            let length = end - start + 1;

            if let Err(e) = reader.seek(start as i64).await {
                tracing::error!("Failed to seek to position {}: {}", start, e);
                return axum::response::Response::builder()
                    .status(500)
                    .body(axum::body::Body::from("Internal Server Error"))
                    .unwrap();
            }

            let content_range = format!("bytes {}-{}/{}", start, end, file_size);
            (length, Some(content_range), 206)
        }
    } else if let Some(start) = opt.range_start {
        // Open-ended range request
        if let Err(e) = reader.seek(start as i64).await {
            tracing::error!("Failed to seek to position {}: {}", start, e);
            return axum::response::Response::builder()
                .status(500)
                .body(axum::body::Body::from("Internal Server Error"))
                .unwrap();
        }
        let length = file_size - start;
        let content_range = format!("bytes {}-{}/{}", start, file_size - 1, file_size);
        (length, Some(content_range), 206)
    } else {
        // Full file
        (file_size, None, 200)
    };

    // Create stream directly from curvine reader with optimized parameters
    let stream_adapter =
        CurvineStreamAdapter::new(reader, content_length, handlers.get_chunk_size_bytes);
    let body_stream = stream_adapter.into_stream();

    // Build response
    let mut builder = axum::response::Response::builder()
        .status(status_code)
        .header("accept-ranges", "bytes")
        .header("content-length", content_length.to_string());

    if let Some(content_range) = content_range {
        builder = builder.header("content-range", content_range);
    }

    // Add S3-compatible headers
    builder = builder
        .header(
            "x-amz-request-id",
            crate::utils::s3_utils::generate_request_id(),
        )
        .header("x-amz-id-2", crate::utils::s3_utils::generate_host_id());

    let body = axum::body::Body::from_stream(body_stream);
    builder.body(body).unwrap()
}

pub async fn handle_fn(
    req: axum::extract::Request<axum::body::Body>,
    _next: axum::middleware::Next,
) -> axum::response::Response {
    crate::http::router::S3Router::route(req).await
}

pub async fn handle_authorization_middleware(
    req: axum::extract::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> impl axum::response::IntoResponse {
    let ret = req
        .extensions()
        .get::<crate::auth::AccessKeyStoreEnum>()
        .cloned();

    let ak_store = match ret {
        Some(ret) => ret,
        None => {
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, b"").into_response();
        }
    };

    let req = Request::from(req);

    // Try AWS Signature V4 first, then fall back to V2
    let auth_result = crate::auth::sig_v4::extract_args(&req);

    if auth_result.is_err() {
        // Try AWS Signature V2 authentication
        match crate::auth::sig_v2::extract_v2_args(&req) {
            Ok(v2_args) => {
                tracing::debug!(
                    "Using AWS Signature V2 authentication for access key: {}",
                    v2_args.access_key
                );

                // Get secret key for V2 authentication
                let secretkey = match ak_store.get(&v2_args.access_key).await {
                    Ok(secretkey) => {
                        if secretkey.is_none() {
                            tracing::warn!("V2 auth: access key not found: {}", v2_args.access_key);
                            return (axum::http::StatusCode::FORBIDDEN, b"").into_response();
                        }
                        secretkey.unwrap()
                    }
                    Err(_) => {
                        tracing::error!("V2 auth: failed to retrieve secret key");
                        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, b"")
                            .into_response();
                    }
                };

                // Parse query parameters for V2 signature verification
                let mut query_params = std::collections::HashMap::new();
                if let Some(query_str) = req.request.uri().query() {
                    for pair in query_str.split('&') {
                        if let Some(eq_pos) = pair.find('=') {
                            let key = &pair[..eq_pos];
                            let value = &pair[eq_pos + 1..];
                            query_params.insert(key.to_string(), value.to_string());
                        } else {
                            // Handle key without value (like "uploads")
                            query_params.insert(pair.to_string(), String::new());
                        }
                    }
                }

                // Verify V2 signature
                if let Err(e) = crate::auth::sig_v2::verify_v2_signature(
                    &v2_args,
                    &secretkey,
                    req.method().as_str(),
                    req.url_path().as_str(),
                    &query_params,
                    &req,
                ) {
                    tracing::warn!("V2 signature verification failed: {}", e);
                    return (axum::http::StatusCode::FORBIDDEN, b"").into_response();
                }

                // Create a V4Head for V2 requests to satisfy middleware expectations
                // This is a compatibility layer - V2 auth is complete but we need V4Head for handlers
                let dummy_ksigning = [0u8; 32]; // Dummy signing key for V2 compatibility
                let dummy_hasher = crate::auth::sig_v4::HmacSha256CircleHasher::new(
                    dummy_ksigning,
                    "v2-signature".to_string(),
                    v2_args.date.clone(),
                    "us-east-1".to_string(),
                );
                let v4head = crate::auth::sig_v4::V4Head::new(
                    v2_args.signature.clone(),
                    "us-east-1".to_string(),
                    v2_args.access_key.clone(),
                    dummy_hasher,
                );

                let mut axum_req: axum::http::Request<axum::body::Body> = req.into();
                axum_req.extensions_mut().insert(v4head);

                tracing::debug!("V2 authentication successful, proceeding to next middleware");
                return next.run(axum_req).await;
            }
            Err(e) => {
                tracing::warn!(
                    "Authentication failed: neither V4 nor V2 signature found. V2 error: {:?}. Request method: {}, URI: {}",
                    e, req.method(), req.url_path()
                );
                return (axum::http::StatusCode::BAD_REQUEST, b"").into_response();
            }
        }
    }

    // Continue with V4 authentication
    let base_arg = auth_result.unwrap();

    // Get raw query string for signature calculation
    let mut query = Vec::new();
    if let Some(query_str) = req.request.uri().query() {
        // Parse query string manually to preserve original encoding
        for pair in query_str.split('&') {
            if let Some(eq_pos) = pair.find('=') {
                let key = &pair[..eq_pos];
                let value = &pair[eq_pos + 1..];
                query.push(crate::utils::BaseKv {
                    key: key.to_string(),
                    val: value.to_string(),
                });
            } else {
                // Handle key without value (like "uploads")
                query.push(crate::utils::BaseKv {
                    key: pair.to_string(),
                    val: String::new(),
                });
            }
        }
    }

    let secretkey = match ak_store.get(&base_arg.access_key).await {
        Ok(secretkey) => {
            if secretkey.is_none() {
                return (axum::http::StatusCode::FORBIDDEN, b"").into_response();
            }
            secretkey.unwrap()
        }
        Err(_) => {
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, b"").into_response();
        }
    };

    let ret = crate::auth::sig_v4::get_v4_signature(
        &req,
        req.method().as_str(),
        &base_arg.region,
        &base_arg.service,
        req.url_path().as_str(),
        &secretkey,
        &base_arg.content_hash,
        &base_arg.signed_headers,
        query,
    );

    let circle_hasher = match ret {
        Ok((sig, circle_hasher)) => {
            if sig != base_arg.signature {
                return (axum::http::StatusCode::FORBIDDEN, b"").into_response();
            }
            circle_hasher
        }
        Err(_) => {
            return (axum::http::StatusCode::FORBIDDEN, b"").into_response();
        }
    };

    let v4head = sig_v4::V4Head::new(
        base_arg.signature,
        base_arg.region,
        base_arg.access_key,
        circle_hasher,
    );

    let mut req: axum::http::Request<axum::body::Body> = req.into();
    req.extensions_mut().insert(v4head);
    next.run(req).await
}
mod bucket {
    #[derive(serde::Serialize, Debug)]
    #[serde(rename = "LocationConstraint", rename_all = "PascalCase")]
    pub struct LocationConstraint {
        #[serde(rename = "$value")]
        region: String,

        #[serde(rename = "xmlns")]
        _xmlns: &'static str,
    }

    impl LocationConstraint {
        #[allow(dead_code)]
        pub fn new<T: Into<String>>(region: T) -> Self {
            Self {
                region: region.into(),
                _xmlns: "http://s3.amazonaws.com/doc/2026-03-01/",
            }
        }
    }
}

/// Simple and efficient adapter that converts curvine Reader to HTTP stream
/// Focuses on optimal chunk size and efficient data conversion, similar to s3s approach
struct CurvineStreamAdapter {
    reader: curvine_client::unified::UnifiedReader,
    remaining_bytes: u64,
    chunk_size: usize,
}

impl CurvineStreamAdapter {
    fn new(
        reader: curvine_client::unified::UnifiedReader,
        content_length: u64,
        chunk_size_bytes: usize,
    ) -> Self {
        let chunk_size = if content_length <= 64 * 1024 {
            std::cmp::min(chunk_size_bytes, content_length as usize).max(4 * 1024)
        } else if content_length <= 1024 * 1024 {
            std::cmp::min(chunk_size_bytes, 256 * 1024)
        } else {
            chunk_size_bytes
        };

        Self {
            reader,
            remaining_bytes: content_length,
            chunk_size,
        }
    }

    /// Create a simple, efficient stream - inspired by s3s but optimized for curvine
    /// No complex prefetching, just optimal chunk size and efficient data conversion
    fn into_stream(
        mut self,
    ) -> impl futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send {
        async_stream::stream! {
            use curvine_common::fs::Reader;

            // Simple streaming approach - similar to s3s but optimized for curvine
            while self.remaining_bytes > 0 {
                let read_size = std::cmp::min(self.chunk_size, self.remaining_bytes as usize);

                match self.reader.async_read(Some(read_size)).await {
                    Ok(data_slice) => {
                        if data_slice.is_empty() {
                            break;
                        }

                        let bytes_read = data_slice.len();
                        self.remaining_bytes = self.remaining_bytes.saturating_sub(bytes_read as u64);

                        // Efficient DataSlice to Bytes conversion
                        // This is more efficient than s3s because we avoid extra buffering
                        let mut buffer = vec![0u8; bytes_read];
                        let mut data_slice_copy = data_slice;
                        data_slice_copy.copy_to_slice(&mut buffer);
                        let bytes = bytes::Bytes::from(buffer);

                        yield Ok(bytes);
                    }
                    Err(e) => {
                        yield Err(std::io::Error::new(std::io::ErrorKind::Other, e));
                        break;
                    }
                }
            }
        }
    }
}

/// Parse HTTP Range header
/// Returns (range_start, range_end) where range_end follows curvine's convention
fn parse_range_header(range_str: &str) -> (Option<u64>, Option<u64>) {
    if !range_str.starts_with("bytes=") {
        return (None, None);
    }

    let range_spec = &range_str[6..]; // Remove "bytes=" prefix
    let parts: Vec<&str> = range_spec.split('-').collect();

    if parts.len() != 2 {
        return (None, None);
    }

    match (parts[0].parse::<u64>(), parts[1].parse::<u64>()) {
        (Ok(start), Ok(end)) => {
            // Normal range: bytes=0-1023
            (Some(start), Some(end))
        }
        (Ok(start), Err(_)) => {
            // Open-ended range: bytes=1024-
            (Some(start), None)
        }
        (Err(_), Ok(suffix_len)) => {
            // Suffix range: bytes=-1024
            // Convert to curvine's convention: range_end = u64::MAX - suffix_len
            (None, Some(u64::MAX - suffix_len))
        }
        _ => (None, None),
    }
}
