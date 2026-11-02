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

extern crate hmac;
use self::hmac::{Hmac, Mac};
use base64::Engine;
use std::collections::HashMap;

use crate::auth::sig_v4::VHeader;
use crate::utils::GenericResult;

/// AWS Signature V2 authentication arguments
#[derive(Debug)]
pub struct V2Args {
    pub access_key: String,
    pub signature: String,
    pub date: String,
}

/// AWS Signature V2 authentication error
#[derive(Debug)]
pub struct V2AuthError(pub String);

/// Extract AWS Signature V2 authentication arguments from request headers
///
/// Parses the Authorization header in format: "AWS access_key:signature"
/// Also extracts the Date or X-Amz-Date header for signature validation.
///
/// # Arguments
/// * `r` - Request object implementing VHeader trait
///
/// # Returns
/// * `Ok(V2Args)` - Successfully parsed authentication arguments
/// * `Err(V2AuthError)` - Missing or invalid headers
pub fn extract_v2_args<R: VHeader>(r: &R) -> Result<V2Args, V2AuthError> {
    // Get Authorization header
    let authorization = r
        .get_header("authorization")
        .ok_or(V2AuthError("Missing authorization header".to_string()))?;

    let authorization = authorization.trim();

    // Parse "AWS access_key:signature" format
    if !authorization.starts_with("AWS ") {
        return Err(V2AuthError(
            "Invalid authorization header format - expected AWS v2".to_string(),
        ));
    }

    let auth_parts = authorization[4..].splitn(2, ':').collect::<Vec<&str>>();
    if auth_parts.len() != 2 {
        return Err(V2AuthError(
            "Invalid authorization header format - missing colon".to_string(),
        ));
    }

    let access_key = auth_parts[0].to_string();
    let signature = auth_parts[1].to_string();

    // Get date from Date or X-Amz-Date header (check Date first, as per s3s implementation)
    let date = r
        .get_header("date")
        .or_else(|| r.get_header("x-amz-date"))
        .ok_or(V2AuthError("Missing date header".to_string()))?;

    Ok(V2Args {
        access_key,
        signature,
        date,
    })
}

/// Query parameters that should be included in the canonicalized resource
/// Based on AWS S3 documentation and s3s implementation
const INCLUDED_QUERY_PARAMS: &[&str] = &[
    "acl",
    "delete",
    "lifecycle",
    "location",
    "logging",
    "notification",
    "partNumber",
    "policy",
    "requestPayment",
    "response-cache-control",
    "response-content-disposition",
    "response-content-encoding",
    "response-content-language",
    "response-content-type",
    "response-expires",
    "uploadId",
    "uploads",
    "versionId",
    "versioning",
    "versions",
    "website",
];

/// Create the string to sign for AWS Signature V2
///
/// Implements the AWS S3 REST authentication string-to-sign algorithm:
/// StringToSign = HTTP-Verb + "\n" +
///                Content-MD5 + "\n" +
///                Content-Type + "\n" +
///                Date + "\n" +
///                CanonicalizedAmzHeaders +
///                CanonicalizedResource
///
/// # Arguments
/// * `method` - HTTP method (GET, PUT, POST, DELETE, etc.)
/// * `uri_path` - Request URI path
/// * `query_params` - Query parameters as key-value pairs
/// * `headers` - Request headers implementing VHeader trait
///
/// # Returns
/// * String to sign for signature calculation
pub fn create_string_to_sign<R: VHeader>(
    method: &str,
    uri_path: &str,
    query_params: &HashMap<String, String>,
    headers: &R,
) -> String {
    let mut string_to_sign = String::with_capacity(256);

    // HTTP-Verb
    string_to_sign.push_str(method);
    string_to_sign.push('\n');

    // Content-MD5
    if let Some(content_md5) = headers.get_header("content-md5") {
        string_to_sign.push_str(&content_md5);
    }
    string_to_sign.push('\n');

    // Content-Type
    if let Some(content_type) = headers.get_header("content-type") {
        string_to_sign.push_str(&content_type);
    }
    string_to_sign.push('\n');

    // Date - use empty string if x-amz-date is present
    let mut date = headers.get_header("date").unwrap_or_default();
    if headers.get_header("x-amz-date").is_some() {
        date = String::new();
    }
    string_to_sign.push_str(&date);
    string_to_sign.push('\n');

    // CanonicalizedAmzHeaders
    let canonicalized_amz_headers = canonicalize_amz_headers(headers);
    string_to_sign.push_str(&canonicalized_amz_headers);

    // CanonicalizedResource
    string_to_sign.push_str(uri_path);

    // Add included query parameters
    let mut included_params = Vec::new();
    for param in INCLUDED_QUERY_PARAMS {
        if let Some(value) = query_params.get(*param) {
            if value.is_empty() {
                included_params.push(param.to_string());
            } else {
                included_params.push(format!("{}={}", param, value));
            }
        }
    }

    if !included_params.is_empty() {
        string_to_sign.push('?');
        string_to_sign.push_str(&included_params.join("&"));
    }

    string_to_sign
}

/// Calculate AWS Signature V2 signature
///
/// Computes HMAC-SHA1 signature for AWS Signature V2 authentication.
/// Based on the s3s library implementation.
///
/// # Arguments
/// * `secret_key` - AWS secret access key
/// * `string_to_sign` - The canonicalized string to sign
///
/// # Returns
/// * Base64-encoded HMAC-SHA1 signature
pub fn calculate_signature(secret_key: &str, string_to_sign: &str) -> GenericResult<String> {
    // Compute HMAC-SHA1 signature
    let hmac_key =
        Hmac::<sha1::Sha1>::new_from_slice(secret_key.as_bytes()).map_err(|e| e.to_string())?;
    let mut hmac_key = hmac_key;
    hmac_key.update(string_to_sign.as_bytes());
    let signature_bytes = hmac_key.finalize().into_bytes();

    // Encode as Base64
    let signature = base64::engine::general_purpose::STANDARD.encode(signature_bytes);
    Ok(signature)
}

/// Build canonicalized AMZ headers string for Signature V2
///
/// Processes all x-amz-* headers according to AWS specification:
/// 1. Convert header names to lowercase
/// 2. Sort headers lexicographically
/// 3. Combine headers with same name (comma-separated)
/// 4. Remove excess whitespace
///
/// Based on the s3s library implementation.
///
/// # Arguments
/// * `headers` - Request headers implementing VHeader trait
///
/// # Returns
/// * Canonicalized AMZ headers string (may be empty)
pub fn canonicalize_amz_headers<R: VHeader>(headers: &R) -> String {
    let mut amz_headers: Vec<(String, Vec<String>)> = Vec::new();
    let mut header_map: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    // Collect all x-amz-* headers
    headers.rng_header(|name, value| {
        let name_lower = name.to_lowercase();
        if name_lower.starts_with("x-amz-") {
            header_map
                .entry(name_lower)
                .or_default()
                .push(value.trim().to_string());
        }
        true // Continue iteration
    });

    if header_map.is_empty() {
        return String::new();
    }

    // Convert to sorted vector
    for (name, values) in header_map {
        amz_headers.push((name, values));
    }

    // Sort by header name
    amz_headers.sort_by(|a, b| a.0.cmp(&b.0));

    // Build canonicalized string
    let mut result = String::new();
    for (name, values) in amz_headers {
        result.push_str(&name);
        result.push(':');
        result.push_str(&values.join(","));
        result.push('\n');
    }

    result
}

/// Verify AWS Signature V2 authentication
///
/// Complete V2 signature verification implementing the full AWS specification.
///
/// # Arguments
/// * `v2_args` - Parsed V2 authentication arguments
/// * `secret_key` - Secret key for the access key
/// * `method` - HTTP method
/// * `uri_path` - Request URI path
/// * `query_params` - Query parameters
/// * `headers` - Request headers
///
/// # Returns
/// * `Ok(())` - Signature is valid
/// * `Err(String)` - Signature verification failed
pub fn verify_v2_signature<R: VHeader>(
    v2_args: &V2Args,
    secret_key: &str,
    method: &str,
    uri_path: &str,
    query_params: &HashMap<String, String>,
    headers: &R,
) -> GenericResult<()> {
    // Create string to sign
    let string_to_sign = create_string_to_sign(method, uri_path, query_params, headers);

    // Calculate expected signature
    let expected_signature = calculate_signature(secret_key, &string_to_sign)?;

    // Compare signatures
    if v2_args.signature != expected_signature {
        tracing::warn!(
            "V2 signature mismatch - provided: {}, expected: {}",
            v2_args.signature,
            expected_signature
        );
        return Err("Signature mismatch".to_string());
    }

    tracing::debug!("V2 signature verification successful");
    Ok(())
}
