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

//! AWS SigV4 primitives shared via [`curvine_vector_gateway::sig_v4`], with S3-gateway error mapping.
//!
//! Call sites must enforce that the credential scope `service` matches the gateway (for example
//! only `"s3"` for this crate); [`extract_args`] does not validate service by itself.

pub use curvine_vector_gateway::sig_v4::{BaseArgs, HmacSha256CircleHasher, V4Head, VHeader};
pub use curvine_vector_gateway::sig_v4::{SIGV4_SERVICE_S3, SIGV4_SERVICE_S3VECTORS};

use curvine_vector_gateway::sig_v4::{self as inner, CanonicalQueryPair};

pub fn extract_args<R: VHeader>(r: &R) -> Result<BaseArgs, crate::error::AuthError> {
    inner::extract_args(r).map_err(|e| match e {
        inner::SigV4ExtractError::MissingAuthorizationHeader => {
            crate::error::AuthError::MissingAuthHeader
        }
        inner::SigV4ExtractError::InvalidFormat(msg) => crate::error::AuthError::InvalidFormat(msg),
    })
}

#[allow(clippy::too_many_arguments)]
pub fn get_v4_signature<T: VHeader, S: ToString>(
    req: &T,
    method: &str,
    region: &str,
    service: &str,
    url_path: &str,
    secretkey: &str,
    content_hash: &str,
    signed_headers: &[S],
    query: Vec<crate::utils::BaseKv<String, String>>,
) -> crate::utils::GenericResult<(String, HmacSha256CircleHasher)> {
    let query: Vec<CanonicalQueryPair> = query
        .into_iter()
        .map(|kv| CanonicalQueryPair {
            key: kv.key,
            val: kv.val,
        })
        .collect();
    inner::get_v4_signature(
        req,
        method,
        region,
        service,
        url_path,
        secretkey,
        content_hash,
        signed_headers,
        query,
    )
    .map_err(|e| e.to_string())
}
