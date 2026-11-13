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

    /// Raw, still percent-encoded request path, exactly as sent by the client.
    /// SigV4/V2 canonical requests must sign this value, not the decoded one.
    fn encoded_path(&self) -> &str {
        self.request.uri().path()
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
                    item.find("=").map_or(
                        (
                            urlencoding::decode(item)
                                .unwrap_or_else(|_| item.into())
                                .to_string(),
                            "".to_string(),
                        ),
                        |pos| {
                            let key = urlencoding::decode(&item[..pos])
                                .unwrap_or_else(|_| item[..pos].into())
                                .to_string();
                            let value = urlencoding::decode(&item[pos + 1..])
                                .unwrap_or_else(|_| item[pos + 1..].into())
                                .to_string();
                            (key, value)
                        },
                    )
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
            // Header values are raw bytes; skip the non-UTF-8 ones instead of
            // producing a `&str` from unchecked memory (UB, and a panic-abort
            // build turns it into a process crash).
            let Ok(value) = v.to_str() else {
                log::warn!("skip header {} with non-UTF-8 value", k.as_str());
                continue;
            };
            if !cb(k.as_str(), value) {
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
        // Object keys arrive percent-encoded (e.g. Hive-style `dt%3D2026-11-01`);
        // decode so they map to the real Curvine path.
        let raw = self.request.uri().path();
        urlencoding::decode(raw)
            .map(|s| s.into_owned())
            .unwrap_or_else(|_| raw.to_string())
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
pub struct BodyReader(pub(crate) body::BodyDataStream);

impl BodyReader {
    /// Direct implementation - used by both trait impl and enum dispatch
    /// This avoids async_trait boxing when called directly
    #[inline]
    pub async fn poll_read_impl(&mut self) -> Result<Option<Vec<u8>>, String> {
        let data = self.0.next().await;
        match data {
            Some(ret) => match ret {
                Ok(ret) => {
                    let vec_data = ret.to_vec();
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

#[async_trait::async_trait]
impl crate::utils::io::PollRead for BodyReader {
    async fn poll_read(&mut self) -> Result<Option<Vec<u8>>, String> {
        self.poll_read_impl().await
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
            let Ok(value) = v.to_str() else {
                log::warn!("skip header {} with non-UTF-8 value", k.as_str());
                continue;
            };
            if !cb(k.as_str(), value) {
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
            let Ok(value) = v.to_str() else {
                log::warn!("skip header {} with non-UTF-8 value", k.as_str());
                return true;
            };
            cb(k.as_str(), value)
        });
    }
}
pub struct BodyWriter<'a>(&'a mut Vec<u8>);
#[async_trait::async_trait]
impl<'b> crate::utils::io::PollWrite for BodyWriter<'b> {
    async fn poll_write(&mut self, buff: &[u8]) -> Result<usize, std::io::Error> {
        self.0.extend_from_slice(buff);
        Ok(buff.len())
    }
}

/// OPTIMIZED: No async_trait - zero allocation
impl crate::s3::s3_api::BodyWriter for Response {
    type BodyWriter<'a>
        = BodyWriter<'a>
    where
        Self: 'a;

    fn get_body_writer(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Self::BodyWriter<'_>, String>> + Send {
        let writer = BodyWriter(&mut self.body);
        async move { Ok(writer) }
    }
}

impl crate::s3::s3_api::VResponse for Response {
    fn set_status(&mut self, status: u16) {
        self.status = status;
    }

    fn send_header(&mut self) {}
}

/// This avoids the MPSC channel overhead and provides better performance
pub async fn stream_get_object(
    req: super::axum::Request,
    handlers: &crate::s3::handlers::S3Handlers,
) -> axum::response::Response {
    use crate::s3::s3_api::{GetObjectOption, VRequest};
    use curvine_fs_api::{FileSystem, Reader};

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

    use crate::auth::sig_v4::VHeader;
    // Parse the Range header once, with the validating parser that also
    // rejects unsatisfiable ranges (`Range::to_byte_range` returns `None`).
    let requested_range = match req.get_header("range") {
        Some(range_str) => match crate::s3::types::Range::parse(&range_str) {
            Ok(range) => Some(range),
            Err(e) => {
                tracing::warn!("invalid Range header {range_str:?}: {e}");
                return axum::response::Response::builder()
                    .status(400)
                    .body(axum::body::Body::from("Invalid Range header"))
                    .unwrap();
            }
        },
        None => None,
    };
    let opt = GetObjectOption {
        range_start: None,
        range_end: None,
    };
    let _ = &opt;

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

    let file_size = reader.remaining().max(0) as u64;

    // Conditional request evaluation. S3 clients use If-Match / If-None-Match
    // for optimistic concurrency and cache validation; ignoring them silently
    // acknowledged writes against a stale ETag.
    // Conditional request evaluation: all four RFC 9110 headers.
    let preconditions = crate::s3::handlers::Preconditions::from_headers(&req, false);
    if let Some(status) = handlers
        .evaluate_preconditions(&cv_path, &preconditions)
        .await
    {
        return axum::response::Response::builder()
            .status(status)
            .header(
                "x-amz-request-id",
                crate::utils::s3_utils::generate_request_id(),
            )
            .body(axum::body::Body::empty())
            .unwrap();
    }

    // `to_byte_range` clamps the end, validates the start and returns `None`
    // for a range that cannot be satisfied, so no subtraction below can
    // underflow (the previous arithmetic wrapped to ~2^64 in release builds).
    let byte_range = requested_range
        .as_ref()
        .map(|range| range.to_byte_range(file_size));

    let (content_length, content_range, status_code) = match byte_range {
        Some(Some((start, length))) => {
            if let Err(e) = reader.seek(start as i64).await {
                tracing::error!("Failed to seek to position {}: {}", start, e);
                return axum::response::Response::builder()
                    .status(500)
                    .body(axum::body::Body::from("Internal Server Error"))
                    .unwrap();
            }
            let end = start + length - 1;
            (
                length,
                Some(format!("bytes {}-{}/{}", start, end, file_size)),
                206,
            )
        }
        Some(None) => {
            // Unsatisfiable: an empty object or a start beyond the end.
            return axum::response::Response::builder()
                .status(416)
                .header("content-range", format!("bytes */{}", file_size))
                .header("accept-ranges", "bytes")
                .header(
                    "x-amz-request-id",
                    crate::utils::s3_utils::generate_request_id(),
                )
                .header("x-amz-id-2", crate::utils::s3_utils::generate_host_id())
                .body(axum::body::Body::empty())
                .unwrap();
        }
        None => (file_size, None, 200),
    };

    // Reuse the HEAD metadata mapping so GET and HEAD agree on a response for
    // the same object (ETag, Content-Type, Last-Modified, x-amz-meta-*).
    let head = crate::utils::s3_utils::file_status_to_head_object_result(reader.status(), object);

    let stream_adapter =
        CurvineStreamAdapter::new(reader, content_length, handlers.get_chunk_size_bytes);
    let body_stream = stream_adapter.into_stream();

    let mut builder = axum::response::Response::builder()
        .status(status_code)
        .header("accept-ranges", "bytes")
        .header("content-length", content_length.to_string());

    if let Some(content_range) = content_range {
        builder = builder.header("content-range", content_range);
    }
    if let Some(etag) = head.etag.as_deref() {
        builder = builder.header("etag", etag);
    }
    if let Some(content_type) = head.content_type.as_deref() {
        builder = builder.header("content-type", content_type);
    }
    if let Some(last_modified) = head.last_modified.as_deref() {
        builder = builder.header("last-modified", last_modified);
    }
    if let Some(metadata) = head.metadata.as_ref() {
        for (key, value) in metadata {
            let header_name = if key.starts_with("x-amz-meta-") {
                key.clone()
            } else {
                format!("x-amz-meta-{}", key)
            };
            builder = builder.header(header_name, value);
        }
    }

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

    // Store original extensions before conversion
    let original_extensions = req.extensions().clone();
    let req = Request::from(req);

    // Presigned URL: SigV4 carried in the query instead of an Authorization header.
    let raw_query = req.request.uri().query().unwrap_or_default().to_string();
    let presigned = match sig_v4::VHeader::get_header(&req, "authorization") {
        None => sig_v4::extract_presigned(&raw_query),
        Some(_) => None,
    };
    if let Some(presigned) = presigned {
        let forbidden = || (axum::http::StatusCode::FORBIDDEN, b"").into_response();
        let Ok(p) = presigned else {
            return forbidden();
        };
        let secret = match ak_store.get(&p.access_key).await {
            Ok(Some(secret)) => secret,
            Ok(None) => return forbidden(),
            Err(_) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, b"").into_response(),
        };
        let now = chrono::Utc::now().timestamp();
        let path = req.encoded_path();
        let header = |name: &str| sig_v4::VHeader::get_header(&req, name);
        let method = req.method().to_string();
        let hasher =
            match sig_v4::verify_presigned(&p, &method, path, &raw_query, &secret, header, now) {
                Ok(hasher) => hasher,
                Err(e) => {
                    // Never log the URL: its query is a bearer credential.
                    tracing::warn!("presigned request rejected: {e}");
                    return forbidden();
                }
            };
        let v4head = sig_v4::V4Head::new(p.signature, p.region, p.access_key, hasher);
        let mut axum_req: axum::http::Request<axum::body::Body> = req.into();
        *axum_req.extensions_mut() = original_extensions;
        axum_req.extensions_mut().insert(v4head);
        return next.run(axum_req).await;
    }

    let auth_result = crate::auth::sig_v4::extract_args(&req);

    if auth_result.is_err() {
        match crate::auth::sig_v2::extract_v2_args(&req) {
            Ok(v2_args) => {
                tracing::debug!(
                    "Using AWS Signature V2 authentication for access key: {}",
                    v2_args.access_key
                );

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

                let mut query_params = std::collections::HashMap::new();
                if let Some(query_str) = req.request.uri().query() {
                    for pair in query_str.split('&') {
                        if let Some(eq_pos) = pair.find('=') {
                            let key = &pair[..eq_pos];
                            let value = &pair[eq_pos + 1..];
                            query_params.insert(key.to_string(), value.to_string());
                        } else {
                            query_params.insert(pair.to_string(), String::new());
                        }
                    }
                }

                if let Err(e) = crate::auth::sig_v2::verify_v2_signature(
                    &v2_args,
                    &secretkey,
                    req.method().as_str(),
                    req.encoded_path(),
                    &query_params,
                    &req,
                ) {
                    tracing::warn!("V2 signature verification failed: {}", e);
                    return (axum::http::StatusCode::FORBIDDEN, b"").into_response();
                }

                let dummy_ksigning = [0u8; 32];
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
                // Restore original extensions and add V4Head
                *axum_req.extensions_mut() = original_extensions;
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

    let base_arg = auth_result.unwrap();

    let mut query = Vec::new();
    if let Some(query_str) = req.request.uri().query() {
        for pair in query_str.split('&') {
            if let Some(eq_pos) = pair.find('=') {
                let key = &pair[..eq_pos];
                let value = &pair[eq_pos + 1..];
                query.push(crate::utils::BaseKv {
                    key: key.to_string(),
                    val: value.to_string(),
                });
            } else {
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
        // SigV4 canonical URI must use the encoded path as sent by the client
        req.encoded_path(),
        &secretkey,
        &base_arg.content_hash,
        &base_arg.signed_headers,
        query,
    );

    let circle_hasher = match ret {
        Ok((sig, circle_hasher)) => {
            // Constant-time, like the V2 path: a byte-wise early exit leaks how
            // much of a guess was correct.
            if !crate::auth::sig_v2::constant_time_eq(sig.as_bytes(), base_arg.signature.as_bytes())
            {
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

    let mut axum_req: axum::http::Request<axum::body::Body> = req.into();
    // Restore original extensions and add V4Head
    *axum_req.extensions_mut() = original_extensions;
    axum_req.extensions_mut().insert(v4head);
    next.run(axum_req).await
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

    fn into_stream(
        mut self,
    ) -> impl futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send {
        async_stream::stream! {
            use curvine_fs_api::Reader;
            use curvine_io::DataSlice;

            while self.remaining_bytes > 0 {
                let read_size = std::cmp::min(self.chunk_size, self.remaining_bytes as usize);

                match self.reader.async_read(Some(read_size)).await {
                    Ok(data_slice) => {
                        if data_slice.is_empty() {
                            break;
                        }

                        let bytes_read = data_slice.len();
                        self.remaining_bytes = self.remaining_bytes.saturating_sub(bytes_read as u64);

                        let bytes = match data_slice {
                            DataSlice::Bytes(bytes) => bytes,
                            DataSlice::Buffer(buf) => buf.freeze(),
                            DataSlice::IOSlice(_) | DataSlice::MemSlice(_) => {
                                bytes::Bytes::copy_from_slice(data_slice.as_slice())
                            },
                            DataSlice::Empty => bytes::Bytes::new(),
                        };

                        yield Ok(bytes);
                    }
                    Err(e) => {
                        yield Err(std::io::Error::other(e));
                        break;
                    }
                }
            }

            if let Err(e) = self.reader.complete().await {
                tracing::warn!("Failed to complete reader cleanup in stream adapter: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::s3_api::VRequest;

    fn build(uri: &str) -> Request {
        let req = axum::http::Request::builder()
            .method("GET")
            .uri(uri)
            .body(axum::body::Body::empty())
            .unwrap();
        Request::from(req)
    }

    #[test]
    fn url_path_decodes_percent_encoded_object_key() {
        // AWS SDKs encode '=' in Hive/Iceberg partition directories as %3D
        let req = build("/warehouse/db/tbl/data/dt%3D2026-11-01/part-0.parquet");
        assert_eq!(
            req.url_path(),
            "/warehouse/db/tbl/data/dt=2026-11-01/part-0.parquet"
        );
    }

    #[test]
    fn url_path_decodes_space_and_literal_percent() {
        let req = build("/bucket/a%20b/100%25.txt");
        assert_eq!(req.url_path(), "/bucket/a b/100%.txt");
    }

    #[test]
    fn url_path_keeps_unencoded_path() {
        let req = build("/bucket/dir/file.txt");
        assert_eq!(req.url_path(), "/bucket/dir/file.txt");
    }

    #[test]
    fn encoded_path_stays_encoded_for_signing() {
        // `encoded_path()` is what both SigV4 and V2 signing call sites use;
        // it must return the path exactly as the client sent it, unlike `url_path()`.
        let req = build("/warehouse/dt%3D2026-11-01/f.parquet");
        assert_eq!(req.encoded_path(), "/warehouse/dt%3D2026-11-01/f.parquet");
    }
}
