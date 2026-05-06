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

use axum::extract::Request;
use axum::http::HeaderMap;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::auth_store::{AccessKeyStore, AccessKeyStoreEnum};
use crate::sig_v4::{self, CanonicalQueryPair, V4Head, VHeader, SIGV4_SERVICE_S3VECTORS};

pub struct AuthHttpCtx<'a> {
    headers: &'a HeaderMap,
    method: &'a str,
    path: &'a str,
}

impl<'a> AuthHttpCtx<'a> {
    #[must_use]
    pub fn new(headers: &'a HeaderMap, method: &'a str, path: &'a str) -> Self {
        Self {
            headers,
            method,
            path,
        }
    }
}

pub fn parse_query_pairs(raw: &str) -> Vec<CanonicalQueryPair> {
    raw.split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            pair.find('=').map_or(
                CanonicalQueryPair {
                    key: pair.to_string(),
                    val: String::new(),
                },
                |pos| CanonicalQueryPair {
                    key: pair[..pos].to_string(),
                    val: pair[pos + 1..].to_string(),
                },
            )
        })
        .collect()
}

impl VHeader for AuthHttpCtx<'_> {
    fn get_header(&self, key: &str) -> Option<String> {
        self.headers
            .get(key)
            .and_then(|value| value.to_str().ok().map(str::to_owned))
    }

    fn set_header(&mut self, _: &str, _: &str) {}

    fn delete_header(&mut self, _: &str) {}

    fn rng_header(&self, mut cb: impl FnMut(&str, &str) -> bool) {
        for (k, v) in self.headers.iter() {
            let Ok(val) = v.to_str() else {
                continue;
            };
            if !cb(k.as_str(), val) {
                return;
            }
        }
    }
}

pub async fn vectors_sig_v4_middleware(req: Request, next: Next) -> Response {
    let store = match req.extensions().get::<AccessKeyStoreEnum>().cloned() {
        Some(s) => s,
        None => {
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "credential store not configured",
            )
                .into_response();
        }
    };

    let mut query_pairs = req.uri().query().map(parse_query_pairs).unwrap_or_default();

    let ctx = AuthHttpCtx::new(req.headers(), req.method().as_str(), req.uri().path());

    let base_arg = match sig_v4::extract_args(&ctx) {
        Ok(a) => a,
        Err(e) => {
            tracing::warn!("vector gateway SigV4 parse failed: {e}");
            return axum::http::StatusCode::BAD_REQUEST.into_response();
        }
    };

    if base_arg.service != SIGV4_SERVICE_S3VECTORS {
        tracing::warn!(
            "vector gateway rejecting credential scope service '{}' (expected '{}')",
            base_arg.service,
            SIGV4_SERVICE_S3VECTORS
        );
        return axum::http::StatusCode::FORBIDDEN.into_response();
    }

    let secretkey = match store.get(&base_arg.access_key).await {
        Ok(secretkey) => match secretkey {
            Some(s) => s,
            None => {
                tracing::warn!("unknown access key on vector gateway");
                return axum::http::StatusCode::FORBIDDEN.into_response();
            }
        },
        Err(e) => {
            tracing::warn!("vector gateway credential store error: {e}");
            return axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let circle_hasher = match sig_v4::get_v4_signature(
        &ctx,
        ctx.method,
        &base_arg.region,
        &base_arg.service,
        ctx.path,
        &secretkey,
        &base_arg.content_hash,
        &base_arg.signed_headers,
        std::mem::take(&mut query_pairs),
    ) {
        Ok((sig, circle_hasher)) => {
            if sig != base_arg.signature {
                tracing::warn!("vector gateway SigV4 mismatch");
                return axum::http::StatusCode::FORBIDDEN.into_response();
            }
            circle_hasher
        }
        Err(e) => {
            tracing::warn!("vector gateway SigV4 compute failed: {e}");
            return axum::http::StatusCode::FORBIDDEN.into_response();
        }
    };

    let v4head = V4Head::new(
        base_arg.signature,
        base_arg.region,
        base_arg.access_key,
        circle_hasher,
    );

    let mut req = req;
    req.extensions_mut().insert(v4head);
    next.run(req).await
}
