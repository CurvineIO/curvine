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
use futures::stream::StreamExt as _; // for map on ReceiverStream

use crate::auth::{sig_v4, AccesskeyStore};
use crate::s3::s3_api::VRequest;
use crate::s3::VRequestPlus;
use axum::body;
pub struct Request {
    request: axum::http::Request<axum::body::Body>,
    query: Option<HashMap<String, String>>,
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
                    // DEBUG: Log body data for debugging
                    log::debug!("HTTP-BODY-CHUNK: {} bytes", vec_data.len());
                    if !vec_data.is_empty() {
                        log::debug!(
                            "HTTP-BODY-HEX: {:?}",
                            vec_data
                                .iter()
                                .take(64)
                                .map(|b| format!("{b:02x}"))
                                .collect::<Vec<_>>()
                                .join(" ")
                        );
                    }
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

// Streaming VResponse for high-throughput GET object
struct StreamingResponse {
    status: std::sync::Arc<tokio::sync::Mutex<u16>>,
    headers: std::sync::Arc<tokio::sync::Mutex<axum::http::HeaderMap>>,
    header_ready_tx: Option<tokio::sync::oneshot::Sender<()>>,
    tx: tokio::sync::mpsc::Sender<bytes::Bytes>,
}

struct StreamBodyWriter {
    tx: tokio::sync::mpsc::Sender<bytes::Bytes>,
}

#[async_trait::async_trait]
impl crate::utils::io::PollWrite for StreamBodyWriter {
    async fn poll_write(&mut self, buff: &[u8]) -> Result<usize, std::io::Error> {
        if buff.is_empty() {
            return Ok(0);
        }
        // Copy into Bytes and send; channel provides backpressure.
        let bytes = bytes::Bytes::copy_from_slice(buff);
        self.tx
            .send(bytes)
            .await
            .map_err(|e| std::io::Error::other(format!("stream send error: {}", e)))?;
        Ok(buff.len())
    }
}

#[async_trait::async_trait]
impl crate::s3::s3_api::BodyWriter for StreamingResponse {
    type BodyWriter<'a> = StreamBodyWriter where Self: 'a;

    async fn get_body_writer(&mut self) -> Result<Self::BodyWriter<'_>, String> {
        Ok(StreamBodyWriter { tx: self.tx.clone() })
    }
}

impl crate::s3::s3_api::VResponse for StreamingResponse {
    fn set_status(&mut self, status: u16) {
        let mut_guard = self.status.clone();
        // fire-and-forget lock update
        let _ = futures::executor::block_on(async move {
            let mut st = mut_guard.lock().await;
            *st = status;
        });
    }

    fn send_header(&mut self) {
        if let Some(tx) = self.header_ready_tx.take() {
            let _ = tx.send(());
        }
    }
}

impl crate::auth::sig_v4::VHeader for StreamingResponse {
    fn get_header(&self, key: &str) -> Option<String> {
        let headers = futures::executor::block_on(self.headers.lock());
        headers
            .get(key)
            .and_then(|v| v.to_str().ok().map(|s| s.to_string()))
    }

    fn set_header(&mut self, key: &str, val: &str) {
        let mut headers = futures::executor::block_on(self.headers.lock());
        let name: axum::http::HeaderName = key.to_string().parse().unwrap();
        headers.insert(name, val.parse().unwrap());
    }

    fn delete_header(&mut self, key: &str) {
        let mut headers = futures::executor::block_on(self.headers.lock());
        headers.remove(key);
    }

    fn rng_header(&self, mut cb: impl FnMut(&str, &str) -> bool) {
        let headers = futures::executor::block_on(self.headers.lock());
        for (k, v) in headers.iter() {
            if !cb(k.as_str(), unsafe { std::str::from_utf8_unchecked(v.as_bytes()) }) {
                return;
            }
        }
    }
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
