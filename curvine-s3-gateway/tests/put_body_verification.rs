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

//! `handle_put_object` wiring: the request body must reach the handler through
//! the verifier.
//!
//! `VerifyingReader` is covered on its own and `status_for_error` is covered on
//! its own, but neither proves the PUT path actually connects them. Replacing
//! `Some(cs)` with `None` in `handle_put_object` turns off SHA-256 checking for
//! every signed PUT and keeps every other gateway test green; these cases fail
//! on exactly that change.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use curvine_s3_gateway::auth::sig_v4::{HmacSha256CircleHasher, V4Head, VHeader};
use curvine_s3_gateway::s3::s3_api::{
    handle_put_object, BodyReader, BodyWriter, PutObjectHandler, PutObjectOption, VRequest,
    VResponse,
};
use curvine_s3_gateway::utils::io::{PollRead, PollWrite};
use sha2::{Digest, Sha256};

#[derive(Clone, Default)]
struct Headers(HashMap<String, String>);

impl VHeader for Headers {
    fn get_header(&self, key: &str) -> Option<String> {
        self.0.get(key).cloned()
    }

    fn set_header(&mut self, key: &str, val: &str) {
        self.0.insert(key.to_string(), val.to_string());
    }

    fn delete_header(&mut self, key: &str) {
        self.0.remove(key);
    }

    fn rng_header(&self, mut cb: impl FnMut(&str, &str) -> bool) {
        for (key, value) in &self.0 {
            if !cb(key, value) {
                return;
            }
        }
    }
}

/// Body backed by a fixed byte vector, delivered as one chunk.
struct MockReader {
    chunks: VecDeque<Vec<u8>>,
}

#[async_trait::async_trait]
impl PollRead for MockReader {
    async fn poll_read(&mut self) -> Result<Option<Vec<u8>>, String> {
        Ok(self.chunks.pop_front())
    }
}

struct MockRequest {
    headers: Headers,
    body: Vec<u8>,
}

impl VHeader for MockRequest {
    fn get_header(&self, key: &str) -> Option<String> {
        self.headers.get_header(key)
    }

    fn set_header(&mut self, key: &str, val: &str) {
        self.headers.set_header(key, val);
    }

    fn delete_header(&mut self, key: &str) {
        self.headers.delete_header(key);
    }

    fn rng_header(&self, cb: impl FnMut(&str, &str) -> bool) {
        self.headers.rng_header(cb);
    }
}

impl VRequest for MockRequest {
    fn method(&self) -> String {
        "PUT".to_string()
    }

    fn url_path(&self) -> String {
        "/bucket/object".to_string()
    }

    fn get_query(&self, _k: &str) -> Option<String> {
        None
    }

    fn all_query(&self, _cb: impl FnMut(&str, &str) -> bool) {}
}

impl BodyReader for MockRequest {
    type BodyReader = MockReader;

    fn get_body_reader<'b>(
        self,
    ) -> Pin<Box<dyn 'b + Send + Future<Output = Result<Self::BodyReader, String>>>> {
        Box::pin(async move {
            Ok(MockReader {
                chunks: VecDeque::from([self.body]),
            })
        })
    }
}

struct VecWriter(Vec<u8>);

#[async_trait::async_trait]
impl PollWrite for VecWriter {
    async fn poll_write(&mut self, buff: &[u8]) -> Result<usize, std::io::Error> {
        self.0.extend_from_slice(buff);
        Ok(buff.len())
    }
}

struct MockResponse {
    headers: Headers,
    status: u16,
}

impl VHeader for MockResponse {
    fn get_header(&self, key: &str) -> Option<String> {
        self.headers.get_header(key)
    }

    fn set_header(&mut self, key: &str, val: &str) {
        self.headers.set_header(key, val);
    }

    fn delete_header(&mut self, key: &str) {
        self.headers.delete_header(key);
    }

    fn rng_header(&self, cb: impl FnMut(&str, &str) -> bool) {
        self.headers.rng_header(cb);
    }
}

impl BodyWriter for MockResponse {
    type BodyWriter<'a>
        = VecWriter
    where
        Self: 'a;

    async fn get_body_writer(&mut self) -> Result<Self::BodyWriter<'_>, String> {
        Ok(VecWriter(Vec::new()))
    }
}

impl VResponse for MockResponse {
    fn set_status(&mut self, status: u16) {
        self.status = status;
    }

    fn send_header(&mut self) {}
}

/// Drains the body it is given and records it, so a rejection is visible as an
/// empty recording: the real handler writes the staging object only after the
/// body completes.
#[derive(Clone, Default)]
struct RecordingHandler {
    stored: Arc<Mutex<Vec<u8>>>,
}

impl PutObjectHandler for RecordingHandler {
    fn handle(
        &self,
        _opt: PutObjectOption,
        _bucket: String,
        _object: String,
        mut body: Box<dyn PollRead + Send>,
    ) -> impl Future<Output = Result<String, String>> + Send {
        let stored = self.stored.clone();
        async move {
            let mut all = Vec::new();
            while let Some(chunk) = body.poll_read().await? {
                all.extend_from_slice(&chunk);
            }
            *stored.lock().unwrap() = all;
            Ok("\"etag\"".to_string())
        }
    }
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::default();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

fn v4head() -> V4Head {
    V4Head::new(
        "signature".to_string(),
        "us-east-1".to_string(),
        "access".to_string(),
        HmacSha256CircleHasher::new([0u8; 32], String::new(), String::new(), String::new()),
    )
}

async fn put(headers: &[(&str, &str)], body: &[u8]) -> (u16, Option<String>, Vec<u8>) {
    let mut map = HashMap::new();
    for (key, value) in headers {
        map.insert((*key).to_string(), (*value).to_string());
    }
    let req = MockRequest {
        headers: Headers(map),
        body: body.to_vec(),
    };
    let mut resp = MockResponse {
        headers: Headers::default(),
        status: 0,
    };
    let handler = RecordingHandler::default();
    handle_put_object(v4head(), req, &mut resp, &handler).await;
    let stored = handler.stored.lock().unwrap().clone();
    (resp.status, resp.headers.get_header("etag"), stored)
}

/// The signed path must verify the digest end to end: the bytes the handler
/// stores are the bytes that were hashed.
#[tokio::test]
async fn a_matching_hash_and_length_reach_the_handler() {
    let body = b"payload-bytes".to_vec();
    let hash = sha256_hex(&body);
    let length = body.len().to_string();
    let (status, etag, stored) = put(
        &[
            ("x-amz-content-sha256", hash.as_str()),
            ("content-length", length.as_str()),
        ],
        &body,
    )
    .await;

    assert_eq!(status, 200);
    assert_eq!(etag.as_deref(), Some("\"etag\""));
    assert_eq!(stored, body);
}

/// A wrong digest must answer 400 and publish nothing. If the `Some(cs)` wiring
/// in `handle_put_object` were `None`, the body would be accepted and this
/// assertion fails.
#[tokio::test]
async fn a_wrong_sha256_is_rejected_before_anything_is_stored() {
    let body = b"payload-bytes".to_vec();
    let length = body.len().to_string();
    let (status, etag, stored) = put(
        &[
            ("x-amz-content-sha256", &"0".repeat(64)),
            ("content-length", length.as_str()),
        ],
        &body,
    )
    .await;

    assert_eq!(status, 400, "a hash mismatch must be a client error");
    assert!(etag.is_none());
    assert!(stored.is_empty(), "a rejected body must not be stored");
}

/// A body shorter than its declared `Content-Length` is the #1990 truncation
/// case: 400, and nothing stored.
#[tokio::test]
async fn a_short_signed_body_is_rejected() {
    let body = b"short".to_vec();
    let hash = sha256_hex(&body);
    let (status, _, stored) = put(
        &[
            ("x-amz-content-sha256", hash.as_str()),
            ("content-length", "1024"),
        ],
        &body,
    )
    .await;

    assert_eq!(status, 400);
    assert!(stored.is_empty());
}

/// `UNSIGNED-PAYLOAD` has no digest to compare but still carries a length, and
/// a truncated unsigned body must not be published either.
#[tokio::test]
async fn a_short_unsigned_body_is_rejected() {
    let (status, _, stored) = put(
        &[
            ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
            ("content-length", "1024"),
        ],
        b"short",
    )
    .await;

    assert_eq!(status, 400);
    assert!(stored.is_empty());
}

/// An unsigned body of the declared length is legitimate and must still be
/// delivered: the length check is not a hash requirement.
#[tokio::test]
async fn a_matching_unsigned_body_reaches_the_handler() {
    let body = b"unsigned-body".to_vec();
    let length = body.len().to_string();
    let (status, _, stored) = put(
        &[
            ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
            ("content-length", length.as_str()),
        ],
        &body,
    )
    .await;

    assert_eq!(status, 200);
    assert_eq!(stored, body);
}
