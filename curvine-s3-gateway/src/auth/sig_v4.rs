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

use std::io::Write;
extern crate hmac;
extern crate sha1;
use self::hmac::{Hmac, Mac};
use self::sha1::Digest;

pub trait VHeader {
    fn get_header(&self, key: &str) -> Option<String>;
    fn set_header(&mut self, key: &str, val: &str);
    fn delete_header(&mut self, key: &str);
    fn rng_header(&self, cb: impl FnMut(&str, &str) -> bool);
}

use crate::s3::error_code::Error;
use crate::utils::consts::*;
use crate::utils::GenericResult;
#[derive(Debug)]
pub struct BaseArgs {
    pub region: String,
    pub service: String,
    pub access_key: String,
    pub content_hash: String,
    pub signed_headers: Vec<String>,
    pub signature: String,
    pub date: String,
}

pub use crate::error::AuthError;

pub fn extract_args<R: VHeader>(r: &R) -> Result<BaseArgs, AuthError> {
    let authorization = r.get_header("authorization");
    let authorization = authorization.ok_or(AuthError::MissingAuthHeader)?;

    let authorization = authorization.trim();
    let heads = authorization.splitn(2, ' ').collect::<Vec<&str>>();
    if heads.len() != 2 {
        return Err(AuthError::InvalidFormat(
            "Invalid authorization header format".to_string(),
        ));
    }

    match heads[0] {
        "AWS4-HMAC-SHA256" => {
            let heads = heads[1].split(',').collect::<Vec<&str>>();
            if heads.len() != 3 {
                return Err(AuthError::InvalidFormat(
                    "Invalid authorization header format".to_string(),
                ));
            }
            let mut credential = None;
            let mut signed_headers = None;
            let mut signature = None;
            for head in heads {
                let heads = head.trim().splitn(2, '=').collect::<Vec<&str>>();
                if heads.len() != 2 {
                    return Err(AuthError::InvalidFormat(
                        "Invalid authorization header format".to_string(),
                    ));
                }
                match heads[0] {
                    "Credential" => {
                        let heads = heads[1].split('/').collect::<Vec<&str>>();
                        if heads.len() != 5 {
                            return Err(AuthError::InvalidFormat(
                                "Invalid authorization header format".to_string(),
                            ));
                        }
                        credential = Some((heads[0], heads[1], heads[2], heads[3], heads[4]));
                    }
                    "SignedHeaders" => {
                        signed_headers = Some(heads[1].split(';').collect::<Vec<&str>>());
                    }
                    "Signature" => {
                        signature = Some(heads[1]);
                    }
                    _ => {
                        return Err(AuthError::InvalidFormat(
                            "Invalid authorization header format".to_string(),
                        ));
                    }
                }
            }

            if signature.is_none() || signed_headers.is_none() || credential.is_none() {
                return Err(AuthError::InvalidFormat(
                    "Invalid authorization header format".to_string(),
                ));
            }

            let signature = signature.unwrap();
            let signed_headers = signed_headers.unwrap();
            let credential = credential.unwrap();

            let content_hash =
                r.get_header("x-amz-content-sha256")
                    .ok_or(AuthError::InvalidFormat(
                        "Missing content hash header".to_string(),
                    ))?;

            // Special handling for common S3 content hash values
            let normalized_content_hash = match content_hash.as_str() {
                "UNSIGNED-PAYLOAD" => content_hash,
                EMPTY_PAYLOAD_HASH => content_hash,
                _ => content_hash,
            };

            let result = BaseArgs {
                content_hash: normalized_content_hash,
                region: credential.2.to_string(),
                service: credential.3.to_string(),
                access_key: credential.0.to_string(),
                signed_headers: signed_headers.into_iter().map(|v| v.to_string()).collect(),
                signature: signature.to_string(),
                date: credential.1.to_string(),
            };

            Ok(result)
        }
        _ => Err(AuthError::InvalidFormat(
            "Invalid authorization header format".to_string(),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
/// Maximum accepted clock skew between the client and this gateway.
/// Matches the AWS SigV4 guidance (15 minutes).
pub const MAX_CLOCK_SKEW_SECS: i64 = 15 * 60;

/// Validates the `x-amz-date` header (`YYYYMMDD'T'HHMMSS'Z'`).
///
/// Returns an error for a malformed value or one outside
/// [`MAX_CLOCK_SKEW_SECS`], so a captured request cannot be replayed
/// indefinitely and no caller slices a too-short string.
fn validate_xamz_date(xamz_date: &str) -> GenericResult<()> {
    let skew = chrono::Utc::now().timestamp() - signed_at(xamz_date)?;
    if skew.abs() > MAX_CLOCK_SKEW_SECS {
        return Err(format!("x-amz-date outside the accepted skew: {skew}s"));
    }
    Ok(())
}

/// Seconds since the epoch of an `x-amz-date` value (`YYYYMMDD'T'HHMMSS'Z'`).
fn signed_at(xamz_date: &str) -> GenericResult<i64> {
    let bytes = xamz_date.as_bytes();
    if bytes.len() != 16 || bytes[8] != b'T' || bytes[15] != b'Z' {
        return Err(format!("invalid x-amz-date format: {xamz_date}"));
    }
    let date = chrono::NaiveDate::parse_from_str(&xamz_date[..8], "%Y%m%d")
        .map_err(|e| format!("invalid x-amz-date: {e}"))?;
    let time = chrono::NaiveTime::parse_from_str(&xamz_date[9..15], "%H%M%S")
        .map_err(|e| format!("invalid x-amz-date: {e}"))?;
    Ok(date.and_time(time).and_utc().timestamp())
}

/// Longest lifetime SigV4 allows for a presigned URL (7 days).
const MAX_PRESIGN_SECS: i64 = 7 * 24 * 3600;

/// Query-string (presigned URL) SigV4 authentication.
pub struct Presigned {
    pub access_key: String,
    pub region: String,
    pub signature: String,
    date: String,
    expires: i64,
    signed_headers: Vec<String>,
}

fn query_pairs(query: &str) -> impl Iterator<Item = (String, String)> + '_ {
    let decode =
        |s: &str| urlencoding::decode(s).map_or_else(|_| s.to_string(), |c| c.into_owned());
    query.split('&').filter(|kv| !kv.is_empty()).map(move |kv| {
        let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
        (decode(k), decode(v))
    })
}

/// URI encoding as SigV4 signs it: unreserved bytes kept, uppercase hex.
fn aws_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Parse presigned-URL authentication; `None` when the query carries none.
pub fn extract_presigned(query: &str) -> Option<Result<Presigned, AuthError>> {
    let mut auth = std::collections::HashMap::new();
    for (k, v) in query_pairs(query).filter(|(k, _)| k.starts_with("X-Amz-")) {
        // A repeated authentication parameter is ambiguous: reject it.
        if auth.insert(k, v).is_some() {
            return Some(Err(AuthError::InvalidFormat(
                "duplicate X-Amz parameter".into(),
            )));
        }
    }
    auth.get("X-Amz-Signature")?;
    let bad = |m: &str| Some(Err(AuthError::InvalidFormat(m.to_string())));
    let get = |k: &str| auth.get(k).map(String::as_str).unwrap_or_default();
    if auth.contains_key("X-Amz-Security-Token") {
        return bad("session tokens are not supported");
    }
    let credential: Vec<&str> = get("X-Amz-Credential").split('/').collect();
    let date = get("X-Amz-Date");
    let expires = get("X-Amz-Expires").parse::<i64>().unwrap_or(0);
    let signed_headers: Vec<String> = get("X-Amz-SignedHeaders")
        .split(';')
        .map(str::to_string)
        .collect();
    if get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256"
        || credential.len() != 5
        || credential[3] != "s3"
        || credential[4] != "aws4_request"
        || date.get(..8) != Some(credential[1])
        || !(1..=MAX_PRESIGN_SECS).contains(&expires)
        || !signed_headers.iter().any(|h| h == "host")
    {
        return bad("invalid presigned URL parameters");
    }
    Some(Ok(Presigned {
        access_key: credential[0].to_string(),
        region: credential[2].to_string(),
        signature: get("X-Amz-Signature").to_string(),
        date: date.to_string(),
        expires,
        signed_headers,
    }))
}

/// SigV4 canonical header value: trimmed, with internal whitespace runs
/// collapsed to one space, as SDKs sign it.
fn canonical_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Verify a presigned GET/HEAD at `now`. Unlike header signatures it is valid
/// for its whole `X-Amz-Expires` lifetime, with clock-skew tolerance only for
/// a signing time in the future.
pub fn verify_presigned(
    p: &Presigned,
    method: &str,
    encoded_path: &str,
    query: &str,
    secret: &str,
    header: impl Fn(&str) -> Option<String>,
    now: i64,
) -> GenericResult<HmacSha256CircleHasher> {
    if method != "GET" && method != "HEAD" {
        return Err("presigned URLs are only accepted for GET and HEAD".into());
    }
    let start = signed_at(&p.date)?;
    if now + MAX_CLOCK_SKEW_SECS < start || now > start + p.expires {
        return Err("presigned URL is not valid at this time".into());
    }
    let mut pairs: Vec<(String, String)> = query_pairs(query)
        .filter(|(k, _)| k != "X-Amz-Signature")
        .map(|(k, v)| (aws_encode(&k), aws_encode(&v)))
        .collect();
    pairs.sort();
    let canonical_query: Vec<String> = pairs.iter().map(|(k, v)| format!("{k}={v}")).collect();
    let mut canonical_headers = String::new();
    for name in &p.signed_headers {
        let value = header(name).ok_or("a signed header is missing")?;
        canonical_headers.push_str(&format!("{name}:{}\n", canonical_value(&value)));
    }
    let canonical_request = format!(
        "{method}\n{encoded_path}\n{}\n{canonical_headers}\n{}\nUNSIGNED-PAYLOAD",
        canonical_query.join("&"),
        p.signed_headers.join(";")
    );
    let scope = format!("{}/{}/s3/aws4_request", &p.date[..8], p.region);
    let digest = hex::encode(sha2::Sha256::digest(canonical_request.as_bytes()));
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{}\n{scope}\n{digest}", p.date);
    let ksign = get_v4_ksigning(secret, &p.region, &p.date)?;
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&ksign).map_err(|e| e.to_string())?;
    mac.update(string_to_sign.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());
    if !crate::auth::sig_v2::constant_time_eq(signature.as_bytes(), p.signature.as_bytes()) {
        return Err("presigned URL signature does not match".into());
    }
    Ok(HmacSha256CircleHasher::new(
        ksign,
        signature,
        p.date.clone(),
        p.region.clone(),
    ))
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
    mut query: Vec<crate::utils::BaseKv<String, String>>,
) -> GenericResult<(String, HmacSha256CircleHasher)> {
    let xamz_date = req.get_header("x-amz-date");
    if xamz_date.is_none() {
        return Err(Error::Illegal.to_string());
    }
    let xamz_date = xamz_date.unwrap();
    // Reject stale or malformed timestamps before any HMAC work: without this
    // a captured signed request replays forever, and slicing `[..8]` below
    // would panic (process abort) on a short header.
    validate_xamz_date(&xamz_date)?;

    // Build canonical headers
    let ans: Vec<String> = signed_headers
        .iter()
        .map(|v| {
            let header_name = v.to_string().to_lowercase(); // Ensure lowercase
            let val = req.get_header(&header_name);
            match val {
                Some(val) => {
                    let canonical_header = format!("{}:{}", header_name, val.trim());
                    canonical_header
                }
                None => {
                    let canonical_header = format!("{header_name}:");
                    canonical_header
                }
            }
        })
        .collect();

    // Sort and encode query parameters
    query.sort_by(|a, b| a.key.cmp(&b.key));
    let query_strings: Vec<String> = query
        .iter()
        .map(|v| {
            // Query parameters are already in their original encoded form
            // Always include the equals sign for consistency with AWS
            let query_param = format!("{}={}", v.key, v.val);
            query_param
        })
        .collect();

    let canonical_query_string = query_strings.join("&");

    let canonical_headers = ans.join("\n");

    let signed_headers_string = signed_headers
        .iter()
        .map(|v| v.to_string().to_lowercase())
        .collect::<Vec<String>>()
        .join(";");

    // Build canonical request
    let canonical_request = format!(
        "{method}\n{url_path}\n{canonical_query_string}\n{canonical_headers}\n\n{signed_headers_string}\n{content_hash}"
    );

    let ksign = get_v4_ksigning(secretkey, region, &xamz_date)?;
    let buff: &[u8] = &ksign;

    let mut hsh = sha2::Sha256::default();
    let _ = hsh.write_all(canonical_request.as_bytes());
    let ans = hsh.finalize();
    let canonical_hsh = hex::encode(ans);

    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}/{}/{}/aws4_request\n{}",
        xamz_date,
        &xamz_date[..8],
        region,
        service,
        canonical_hsh
    );

    // println!("{tosign}");
    let ret = Hmac::<sha2::Sha256>::new_from_slice(buff);
    if let Err(err) = ret {
        return Err(err.to_string());
    }
    let mut hsh = ret.unwrap();
    hsh.update(string_to_sign.as_bytes());
    let ans = hsh.finalize().into_bytes();
    let signature = hex::encode(ans);

    Ok((
        signature.clone(),
        HmacSha256CircleHasher::new(ksign, signature, xamz_date, region.to_string()),
    ))
}

fn get_v4_ksigning(secretkey: &str, region: &str, xamz_date: &str) -> GenericResult<[u8; 32]> {
    let mut ksign = [0u8; 32];
    circle_hmac_sha256(
        format!("{}{}", "AWS4", secretkey).as_str(),
        vec![
            &xamz_date.as_bytes()[..8],
            region.as_bytes(),
            "s3".as_bytes(),
            "aws4_request".as_bytes(),
        ]
        .as_slice(),
        &mut ksign,
    )?;
    Ok(ksign)
}

fn circle_hmac_sha256(initkey: &str, values: &[&[u8]], target: &mut [u8]) -> GenericResult<()> {
    let ret = Hmac::<sha2::Sha256>::new_from_slice(initkey.as_bytes());
    if let Err(err) = ret {
        return Err(err.to_string());
    }
    let mut hsh = ret.unwrap();
    hsh.update(values[0]);
    let mut next = hsh.finalize().into_bytes();
    for value in values.iter().skip(1) {
        match Hmac::<sha2::Sha256>::new_from_slice(&next) {
            Ok(mut hsh) => {
                hsh.update(value);
                next = hsh.finalize().into_bytes();
            }
            Err(err) => {
                return Err(err.to_string());
            }
        }
    }
    target[0..next.len()].copy_from_slice(&next);
    Ok(())
}

#[derive(Clone)]
pub struct HmacSha256CircleHasher {
    ksigning: [u8; 32],
    last_hash: String,
    xamz_date: String,
    region: String,
    date: String,
}

impl HmacSha256CircleHasher {
    pub fn new(ksigning: [u8; 32], lasthash: String, xamz_date: String, region: String) -> Self {
        // Callers pass a header value that may be malformed; take a checked
        // prefix so a short date cannot panic in a `panic = "abort"` build.
        let date = xamz_date.get(..8).unwrap_or_default().to_string();
        Self {
            ksigning,
            last_hash: lasthash,
            region,
            xamz_date,
            date,
        }
    }
    pub fn next(&mut self, curr_hsh: &str) -> Result<String, Error> {
        let ans = Hmac::<sha2::Sha256>::new_from_slice(&self.ksigning);
        if let Err(_err) = ans {
            return Err(Error::Illegal);
        }
        let mut hsh = ans.unwrap();
        let tosign = format!(
            "{}\n{}\n{}/{}/{}/{}\n{}\n{}\n{}",
            "AWS4-HMAC-SHA256-PAYLOAD",
            self.xamz_date,
            self.date,
            self.region,
            "s3",
            "aws4_request",
            self.last_hash,
            EMPTY_PAYLOAD_HASH,
            curr_hsh
        );
        hsh.update(tosign.as_bytes());
        let ans = hsh.finalize().into_bytes();
        let ans = hex::encode(ans);
        self.last_hash = ans.clone();
        Ok(ans)
    }
}

#[derive(Clone)]
pub struct V4Head {
    signature: String,
    region: String,
    accesskey: String,
    circle_hasher: HmacSha256CircleHasher,
}

impl V4Head {
    pub fn new(
        signature: String,
        region: String,
        accesskey: String,
        hasher: HmacSha256CircleHasher,
    ) -> Self {
        Self {
            signature,
            region,
            accesskey,
            circle_hasher: hasher,
        }
    }
    pub fn signature(&self) -> &str {
        &self.signature
    }
    pub fn region(&self) -> &str {
        &self.region
    }
    pub fn accesskey(&self) -> &str {
        &self.accesskey
    }
    pub fn hasher(&mut self) -> &mut HmacSha256CircleHasher {
        &mut self.circle_hasher
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn stamp(offset_secs: i64) -> String {
        let now = Utc::now() + chrono::Duration::seconds(offset_secs);
        now.format("%Y%m%dT%H%M%SZ").to_string()
    }

    #[test]
    fn accepts_a_current_signature_date() {
        assert!(validate_xamz_date(&stamp(0)).is_ok());
        assert!(validate_xamz_date(&stamp(-60)).is_ok());
    }

    #[test]
    fn rejects_a_stale_signature_date() {
        // A captured request replayed after the skew window must be rejected.
        let err = validate_xamz_date(&stamp(-(MAX_CLOCK_SKEW_SECS + 60))).unwrap_err();
        assert!(err.contains("skew"), "{err}");
        assert!(validate_xamz_date(&stamp(MAX_CLOCK_SKEW_SECS + 60)).is_err());
    }

    #[test]
    fn rejects_malformed_signature_dates_without_panicking() {
        for bad in [
            "",
            "short",
            "2025",
            "20250101",
            "20250101T0000Z",
            "20250101T000000",
            "2025-01-01T00:00:00Z",
            "20251301T000000Z",
            "20250101T999999Z",
        ] {
            assert!(
                validate_xamz_date(bad).is_err(),
                "expected {bad:?} to be rejected"
            );
        }
    }

    /// AWS "query parameters" example: GET examplebucket/test.txt, 24h URL.
    #[test]
    fn verifies_presigned_urls_against_the_aws_example() {
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404";
        let secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let host = |k: &str| (k == "host").then(|| "examplebucket.s3.amazonaws.com".to_string());
        let p = extract_presigned(query).unwrap().unwrap();
        let at = signed_at("20130524T010000Z").unwrap();
        assert!(verify_presigned(&p, "GET", "/test.txt", query, secret, host, at).is_ok());
        assert!(verify_presigned(&p, "GET", "/other.txt", query, secret, host, at).is_err());
        assert!(verify_presigned(&p, "PUT", "/test.txt", query, secret, host, at).is_err());
        // A GET URL replayed as HEAD signs a different canonical method.
        assert!(verify_presigned(&p, "HEAD", "/test.txt", query, secret, host, at).is_err());
        assert_eq!(canonical_value(" \"a\",\t  \"b\" "), "\"a\", \"b\"");
        let expired = at + 86400;
        assert!(verify_presigned(&p, "GET", "/test.txt", query, secret, host, expired).is_err());
        let longer = query.replace("Expires=86400", "Expires=604801");
        assert!(extract_presigned(&longer).unwrap().is_err());
        assert!(extract_presigned("list-type=2").is_none());
    }

    #[test]
    fn hasher_tolerates_a_short_date_header() {
        // Reached after only an access-key match, so it must not panic.
        let hasher =
            HmacSha256CircleHasher::new([0u8; 32], "sig".into(), "short".into(), "r".into());
        assert_eq!(hasher.date, "");
    }
}
