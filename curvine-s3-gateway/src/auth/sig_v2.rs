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

    // V2 has no signed payload hash, so the date is the only replay bound. The
    // header is echoed into the string to sign, which means an accepted stale
    // request stays valid forever; reject it here, the same way the V4 path
    // does.
    validate_v2_date(&date).map_err(V2AuthError)?;

    Ok(V2Args {
        access_key,
        signature,
        date,
    })
}

/// Validate the V2 `Date` / `x-amz-date` header against the accepted skew.
///
/// V2 clients send either RFC 1123 (`Date`) or the SigV4-style compact stamp
/// (`x-amz-date`); both forms are accepted, everything else is rejected.
pub fn validate_v2_date(date: &str) -> GenericResult<()> {
    let signed_at = chrono::DateTime::parse_from_rfc2822(date)
        .map(|parsed| parsed.timestamp())
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(date, "%Y%m%dT%H%M%SZ")
                .map(|parsed| parsed.and_utc().timestamp())
        })
        .map_err(|e| format!("invalid date header: {e}"))?;
    let skew = chrono::Utc::now().timestamp() - signed_at;
    if skew.abs() > crate::auth::sig_v4::MAX_CLOCK_SKEW_SECS {
        return Err(format!("date header outside the accepted skew: {skew}s"));
    }
    Ok(())
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

/// Compare two byte strings without an early exit.
///
/// Shared with the V4 path so both signature schemes are compared the same way.
pub(crate) fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    left.ct_eq(right).into()
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

    // Constant-time compare: a byte-wise early exit leaks how much of a guess
    // was correct, which is enough to recover a signature one byte at a time.
    if !constant_time_eq(v2_args.signature.as_bytes(), expected_signature.as_bytes()) {
        // Never log either signature. `expected` is HMAC-SHA1(secret, a string
        // the caller chooses), so logging it would let anyone with read access
        // to the gateway log mint a valid signature for their own request.
        tracing::warn!(
            "V2 signature mismatch for access key {}",
            v2_args.access_key
        );
        return Err("Signature mismatch".to_string());
    }

    tracing::debug!("V2 signature verification successful");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Minimal header map so the V2 path can be exercised without a request.
    struct Headers(HashMap<String, String>);

    impl VHeader for Headers {
        fn get_header(&self, key: &str) -> Option<String> {
            self.0.get(key).map(ToString::to_string)
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

    fn headers(pairs: &[(&str, &str)]) -> Headers {
        Headers(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    fn rfc2822(offset_secs: i64) -> String {
        let when = chrono::Utc::now() + chrono::Duration::seconds(offset_secs);
        when.to_rfc2822()
    }

    #[test]
    fn rejects_a_stale_date_header() {
        // V2 carries no payload hash, so an accepted old request replays forever.
        let stale = rfc2822(-(crate::auth::sig_v4::MAX_CLOCK_SKEW_SECS + 60));
        let err = validate_v2_date(&stale).unwrap_err();
        assert!(err.contains("skew"), "{err}");

        let header = headers(&[
            ("authorization", "AWS ak:signature"),
            ("date", stale.as_str()),
        ]);
        assert!(extract_v2_args(&header).is_err());
    }

    #[test]
    fn accepts_current_date_formats() {
        assert!(validate_v2_date(&rfc2822(0)).is_ok());
        assert!(validate_v2_date(&rfc2822(-30)).is_ok());
        let compact = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        assert!(validate_v2_date(&compact).is_ok());
    }

    #[test]
    fn rejects_malformed_dates_without_panicking() {
        for bad in ["", "short", "2025-01-01", "not a date", "20251301T000000Z"] {
            assert!(validate_v2_date(bad).is_err(), "expected {bad:?} rejected");
        }
    }

    #[test]
    fn accepts_a_valid_signature_and_rejects_a_forged_one() {
        let secret = "secret";
        let header = headers(&[
            ("date", rfc2822(0).as_str()),
            ("content-type", "text/plain"),
        ]);
        let string_to_sign = create_string_to_sign("GET", "/bucket/key", &HashMap::new(), &header);
        let valid = V2Args {
            access_key: "ak".to_string(),
            signature: calculate_signature(secret, &string_to_sign).unwrap(),
            date: header.get_header("date").unwrap(),
        };
        assert!(verify_v2_signature(
            &valid,
            secret,
            "GET",
            "/bucket/key",
            &HashMap::new(),
            &header
        )
        .is_ok());

        let forged = V2Args {
            signature: "AAAA".to_string(),
            ..valid
        };
        assert!(verify_v2_signature(
            &forged,
            secret,
            "GET",
            "/bucket/key",
            &HashMap::new(),
            &header
        )
        .is_err());
    }
}
