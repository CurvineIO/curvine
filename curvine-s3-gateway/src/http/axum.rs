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

use futures::StreamExt;

use crate::auth::{sig_v4, AccesskeyStore};
use crate::s3::s3_api::VRequest;
use crate::s3::VRequestPlus;
use axum::body;
use axum::http::HeaderValue;
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

    /// Query parameters as a Signature V2 client signs them.
    ///
    /// The path is signed as sent (see `encoded_path`), but the values in the
    /// canonicalized resource are signed decoded: `GetObject` with
    /// `ResponseContentType="text/plain"` is sent as
    /// `response-content-type=text%2Fplain` and signed as
    /// `response-content-type=text/plain` by botocore, the AWS SDKs for Java
    /// and JavaScript, and minio-go. The MinIO server decodes the query before
    /// verifying for the same reason.
    ///
    /// The values come from `urlencoding::decode`, which is not form decoding:
    /// a literal `+` stays `+` and `%2B` decodes to `+`, as botocore's
    /// `unquote` and JavaScript's `decodeURIComponent` decode them.
    fn v2_signed_query(&self) -> HashMap<String, String> {
        self.query.clone().unwrap_or_default()
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

/// S3 XML error response so SDKs read `Error.Code` instead of a bare status;
/// HEAD answers with the same status but no body, as the protocol requires.
fn s3_error_response(method: &str, err: &str) -> axum::response::Response {
    let status = crate::s3::s3_api::status_for_error(err);
    let request_id = crate::utils::s3_utils::generate_request_id();
    let builder = axum::response::Response::builder()
        .status(status)
        .header("x-amz-request-id", &request_id);
    if method == "HEAD" {
        return builder.body(axum::body::Body::empty()).unwrap();
    }
    let body = crate::s3::s3_api::error_xml(err, &request_id);
    builder
        .header("content-type", "application/xml")
        .body(axum::body::Body::from(body))
        .unwrap()
}

/// This avoids the MPSC channel overhead and provides better performance
/// 404 with the S3 `NoSuchKey` XML body, so SDKs can tell a missing key.
fn no_such_key() -> axum::response::Response {
    let request_id = crate::utils::s3_utils::generate_request_id();
    let body =
        crate::s3::s3_api::error_xml("NoSuchKey: The specified key does not exist.", &request_id);
    axum::response::Response::builder()
        .status(404)
        .header("content-type", "application/xml")
        .header("x-amz-request-id", request_id)
        .body(axum::body::Body::from(body))
        .unwrap()
}

/// The answer to a GET or HEAD whose precondition headers did not hold. A 412
/// carries the `PreconditionFailed` XML body (none for HEAD), as PutObject and
/// CopyObject already do, so SDKs read `Error.Code`. A 304 has no body but
/// repeats the validators: RFC 9110 section 15.4.5 requires the `ETag` a 200
/// would have sent, and S3 also sends `Last-Modified`.
pub(crate) fn precondition_response(
    method: &str,
    status: u16,
    etag: Option<&str>,
    mtime_ms: i64,
) -> axum::response::Response {
    if status == 412 {
        return s3_error_response(method, "PreconditionFailed: object condition not met");
    }
    let mut builder = axum::response::Response::builder()
        .status(status)
        .header("accept-ranges", "bytes")
        .header(
            "x-amz-request-id",
            crate::utils::s3_utils::generate_request_id(),
        );
    if let Some(etag) = etag {
        builder = builder.header("etag", etag);
    }
    if let Some(last_modified) = crate::utils::s3_utils::format_http_timestamp(mtime_ms) {
        builder = builder.header("last-modified", last_modified);
    }
    builder.body(axum::body::Body::empty()).unwrap()
}

/// 416 for a range the object cannot satisfy: the `InvalidRange` XML body, and
/// `Content-Range: bytes */<size>` so the client learns the object size.
fn range_not_satisfiable(method: &str, file_size: u64) -> axum::response::Response {
    let mut response = s3_error_response(
        method,
        "InvalidRange: the requested range is not satisfiable",
    );
    let headers = response.headers_mut();
    let content_range = format!("bytes */{file_size}");
    headers.insert("content-range", content_range.parse().unwrap());
    headers.insert("accept-ranges", HeaderValue::from_static("bytes"));
    let host_id = crate::utils::s3_utils::generate_host_id();
    headers.insert("x-amz-id-2", host_id.parse().unwrap());
    response
}

pub async fn stream_get_object(
    req: super::axum::Request,
    handlers: &crate::s3::handlers::S3Handlers,
) -> axum::response::Response {
    use crate::s3::s3_api::VRequest;
    use curvine_fs_api::{FileSystem, Reader};

    let url_path = req.url_path();
    let path_parts: Vec<&str> = url_path.trim_start_matches('/').splitn(2, '/').collect();
    if path_parts.len() != 2 {
        return s3_error_response(
            req.method().as_str(),
            "InvalidArgument: the path must be /bucket/key",
        );
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
                return s3_error_response(
                    req.method().as_str(),
                    "InvalidArgument: the Range header could not be parsed",
                );
            }
        },
        None => None,
    };
    let cv_path = match handlers.cv_object_path(bucket, object) {
        Ok(path) => path,
        Err(e) => {
            tracing::error!(
                "Failed to convert S3 path s3://{}/{}: {}",
                bucket,
                object,
                e
            );
            return no_such_key();
        }
    };

    if object.ends_with('/') {
        if !crate::utils::s3_utils::is_directory_marker_key(object) {
            return no_such_key();
        }
        let status = match handlers.fs.get_status(&cv_path).await {
            Ok(st) if crate::utils::s3_utils::is_directory_marker(&st, object) => st,
            Ok(_) | Err(curvine_error::FsError::FileNotFound(_)) => return no_such_key(),
            Err(e) => {
                tracing::error!("Failed to stat directory marker at path {}: {}", cv_path, e);
                return s3_error_response(req.method().as_str(), "failed to open the object");
            }
        };
        let conditions = crate::s3::handlers::Preconditions::from_headers(&req, false);
        let etag = format!("\"{}\"", crate::utils::s3_utils::EMPTY_MD5);
        if let Some(code) = crate::s3::handlers::evaluate_precondition_headers(
            &conditions,
            Some(&etag),
            Some(status.mtime),
        ) {
            return precondition_response(req.method().as_str(), code, Some(&etag), status.mtime);
        }
        let mut response = axum::response::Response::builder().status(200);
        for (name, value) in crate::utils::s3_utils::directory_marker_headers(status.mtime) {
            response = response.header(name, value);
        }
        return response
            .header(
                "x-amz-request-id",
                crate::utils::s3_utils::generate_request_id(),
            )
            .header("x-amz-id-2", crate::utils::s3_utils::generate_host_id())
            .body(axum::body::Body::empty())
            .unwrap();
    }

    let mut reader = match handlers.fs.open(&cv_path).await {
        Ok(reader) => reader,
        Err(curvine_error::FsError::FileNotFound(_)) => return no_such_key(),
        Err(e) => {
            if handlers
                .fs
                .get_status(&cv_path)
                .await
                .is_ok_and(|st| st.is_dir)
            {
                return no_such_key();
            }
            // Only a missing key or directory prefix is NoSuchKey; every other
            // open failure is a server error, so clients do not conclude the
            // object is gone.
            tracing::error!("Failed to open file at path {}: {}", cv_path, e);
            return s3_error_response(req.method().as_str(), "failed to open the object");
        }
    };

    let file_size = reader.remaining().max(0) as u64;

    // Conditional request evaluation. S3 clients use If-Match / If-None-Match
    // for optimistic concurrency and cache validation; ignoring them silently
    // acknowledged writes against a stale ETag.
    // Conditional request evaluation: all four RFC 9110 headers.
    let preconditions = crate::s3::handlers::Preconditions::from_headers(&req, false);
    // Evaluate against the status the open returned: the validators then
    // describe the object being served, without a second metadata round trip.
    let etag = crate::utils::s3_utils::generate_etag(reader.status());
    let mtime = reader.status().mtime;
    if let Some(status) =
        crate::s3::handlers::evaluate_precondition_headers(&preconditions, Some(&etag), Some(mtime))
    {
        return precondition_response(req.method().as_str(), status, Some(&etag), mtime);
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
                return s3_error_response(req.method().as_str(), "failed to seek in the object");
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
            return range_not_satisfiable(req.method().as_str(), file_size);
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
    // Resolve Content-Type once, before the builder is touched: `header()`
    // appends to the HeaderMap, so emitting the stored type and then the
    // `response-content-type` override would send two values and every client
    // would read the stored one.
    if let Some(last_modified) = head.last_modified.as_deref() {
        builder = builder.header("last-modified", last_modified);
    }
    // Standard object headers stored on upload plus the `response-*` overrides
    // this GET may carry. One decision function, shared with HEAD, and it
    // rejects overrides that are not valid header values before the builder
    // sees them.
    let object_headers =
        match crate::utils::s3_utils::get_response_headers(&head, |name| req.get_query(name)) {
            Ok(headers) => headers,
            Err(parameter) => {
                tracing::warn!("rejected GET with invalid {parameter} value");
                return s3_error_response(
                    req.method().as_str(),
                    "InvalidArgument: response override is not a valid header value",
                );
            }
        };
    for (name, value) in object_headers {
        builder = builder.header(name, value);
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
    // Errors below answer with S3 XML bodies so SDKs read `Error.Code`;
    // HEAD must stay bodyless, so the method travels with every rejection.
    let method = req.method().as_str().to_string();
    let ret = req
        .extensions()
        .get::<crate::auth::AccessKeyStoreEnum>()
        .cloned();

    let ak_store = match ret {
        Some(ret) => ret,
        None => {
            return s3_error_response(&method, "failed to load the access-key store");
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
        let Ok(p) = presigned else {
            return s3_error_response(&method, "AccessDenied: the presigned request is invalid");
        };
        let secret = match ak_store.get(&p.access_key).await {
            Ok(Some(secret)) => secret,
            Ok(None) => {
                return s3_error_response(
                    &method,
                    "InvalidAccessKeyId: the AWS access key Id you provided does not exist in our records",
                );
            }
            Err(_) => return s3_error_response(&method, "failed to load the access-key store"),
        };
        let now = chrono::Utc::now().timestamp();
        let path = req.encoded_path();
        let header = |name: &str| sig_v4::VHeader::get_header(&req, name);
        let method = req.method().to_string();
        let hasher = match sig_v4::verify_presigned(
            &p, &method, path, &raw_query, &secret, header, now,
        ) {
            Ok(hasher) => hasher,
            Err(e) => {
                // Never log the URL: its query is a bearer credential.
                tracing::warn!("presigned request rejected: {e}");
                return s3_error_response(
                        &method,
                        "SignatureDoesNotMatch: the request signature we calculated does not match the signature you provided",
                    );
            }
        };
        // A presigned URL signs only the headers it lists; an unsigned
        // `x-amz-copy-source` would turn a write URL into "copy any object the
        // gateway can read", so a copy must have signed that header.
        let copy_source = sig_v4::VHeader::get_header(&req, "x-amz-copy-source");
        if !sig_v4::presigned_copy_allowed(&p, copy_source.as_deref()) {
            tracing::warn!("presigned request carries an unsigned x-amz-copy-source");
            return s3_error_response(
                &method,
                "AccessDenied: the presigned request carries an unsigned x-amz-copy-source",
            );
        }
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
                            return s3_error_response(
                                &method,
                                "InvalidAccessKeyId: the AWS access key Id you provided does not exist in our records",
                            );
                        }
                        secretkey.unwrap()
                    }
                    Err(_) => {
                        tracing::error!("V2 auth: failed to retrieve secret key");
                        return s3_error_response(&method, "failed to load the access-key store");
                    }
                };

                let query_params = req.v2_signed_query();

                if let Err(e) = crate::auth::sig_v2::verify_v2_signature(
                    &v2_args,
                    &secretkey,
                    req.method().as_str(),
                    req.encoded_path(),
                    &query_params,
                    &req,
                ) {
                    tracing::warn!("V2 signature verification failed: {}", e);
                    return s3_error_response(
                        &method,
                        "SignatureDoesNotMatch: the request signature we calculated does not match the signature you provided",
                    );
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
                return s3_error_response(
                    &method,
                    "AccessDenied: the request does not carry a recognizable signature",
                );
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
                return s3_error_response(
                    &method,
                    "InvalidAccessKeyId: the AWS access key Id you provided does not exist in our records",
                );
            }
            secretkey.unwrap()
        }
        Err(_) => {
            return s3_error_response(&method, "failed to load the access-key store");
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
                return s3_error_response(
                    &method,
                    "SignatureDoesNotMatch: the request signature we calculated does not match the signature you provided",
                );
            }
            circle_hasher
        }
        Err(_) => {
            return s3_error_response(
                &method,
                "SignatureDoesNotMatch: the request signature we calculated does not match the signature you provided",
            );
        }
    };

    use crate::utils::signed_chunk_reader::{
        ChunkAuth, SIGNED_CHUNKS, SIGNED_TRAILER, UNSIGNED_TRAILER,
    };
    let chunk_auth = match base_arg.content_hash.as_str() {
        SIGNED_CHUNKS | SIGNED_TRAILER => Some(ChunkAuth::Signed(circle_hasher.clone())),
        UNSIGNED_TRAILER => Some(ChunkAuth::Unsigned),
        _ => None,
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
    if let Some(chunk_auth) = chunk_auth {
        axum_req.extensions_mut().insert(chunk_auth);
    }
    next.run(axum_req).await
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
    use crate::auth::sig_v2::{calculate_signature, verify_v2_signature, V2Args};
    use crate::auth::sig_v4::VHeader;
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

    #[test]
    fn v2_signed_query_decodes_values() {
        let req = build("/b/k?response-content-type=text%2Fplain&acl");
        let query = req.v2_signed_query();
        assert_eq!(query["response-content-type"], "text/plain");
        assert_eq!(query["acl"], "");
    }

    #[test]
    fn v2_signed_query_keeps_a_literal_plus() {
        let req = build("/b/k?response-content-type=a+b&response-expires=%2B");
        let query = req.v2_signed_query();
        assert_eq!(query["response-content-type"], "a+b");
        assert_eq!(query["response-expires"], "+");
    }

    #[test]
    fn v2_accepts_a_response_override_signed_like_the_aws_sdks() {
        // What an AWS SDK signs for `GetObject` with
        // `ResponseContentType="text/plain"`: the path as sent, the value decoded.
        const DATE: &str = "Wed, 18 Nov 2026 12:00:00 GMT";
        let signed = format!("GET\n\n\n{DATE}\n/b/k?response-content-type=text/plain");
        let args = V2Args {
            access_key: "ak".to_string(),
            signature: calculate_signature("sk", &signed).unwrap(),
            date: DATE.to_string(),
        };
        let mut req = build("/b/k?response-content-type=text%2Fplain");
        req.set_header("date", DATE);
        let path = req.encoded_path();
        let query = req.v2_signed_query();
        let verified = verify_v2_signature(&args, "sk", "GET", path, &query, &req);
        assert!(verified.is_ok(), "{verified:?}");

        // The value as sent, which the gateway used to sign, does not match.
        let mut raw: HashMap<String, String> = HashMap::new();
        raw.insert("response-content-type".into(), "text%2Fplain".into());
        let rejected = verify_v2_signature(&args, "sk", "GET", path, &raw, &req);
        assert!(rejected.is_err());
    }

    async fn body_text(response: axum::response::Response) -> String {
        let body = response.into_body();
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    /// SDKs read `Error.Code` from the body. A bare 416 made boto3 report the
    /// code as "416" instead of `InvalidRange`.
    #[tokio::test]
    async fn an_unsatisfiable_range_answers_invalid_range_xml() {
        let response = range_not_satisfiable("GET", 11);
        assert_eq!(response.status(), 416);
        assert_eq!(response.headers()["content-range"], "bytes */11");
        assert_eq!(response.headers()["content-type"], "application/xml");
        let body = body_text(response).await;
        assert!(body.contains("<Code>InvalidRange</Code>"), "{body}");
    }

    const ETAG: &str = "\"abc\"";
    /// 2026-11-19T00:00:00Z
    const MTIME: i64 = 1_795_046_400_000;

    #[tokio::test]
    async fn a_failed_precondition_answers_precondition_failed_xml() {
        let failed = precondition_response("GET", 412, Some(ETAG), MTIME);
        assert_eq!(failed.status(), 412);
        assert_eq!(failed.headers()["content-type"], "application/xml");
        let body = body_text(failed).await;
        assert!(body.contains("<Code>PreconditionFailed</Code>"), "{body}");

        // HEAD gets the same status without a body.
        let head = precondition_response("HEAD", 412, Some(ETAG), MTIME);
        assert_eq!(head.status(), 412);
        assert!(body_text(head).await.is_empty());
    }

    /// RFC 9110 section 15.4.5: a 304 repeats the `ETag` a 200 would send, so
    /// a cache can refresh the entry it revalidated.
    #[tokio::test]
    async fn not_modified_repeats_the_validators() {
        for method in ["GET", "HEAD"] {
            let response = precondition_response(method, 304, Some(ETAG), MTIME);
            assert_eq!(response.status(), 304);
            let headers = response.headers();
            assert_eq!(headers["etag"], ETAG);
            let last_modified = "Thu, 19 Nov 2026 00:00:00 GMT";
            assert_eq!(headers["last-modified"], last_modified);
            assert!(body_text(response).await.is_empty());
        }
    }
}
