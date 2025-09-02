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

use crate::s3::error::Error;
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
            let mut singed_headers = None;
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
                        singed_headers = Some(heads[1].split(';').collect::<Vec<&str>>());
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

            if signature.is_none() || singed_headers.is_none() || credential.is_none() {
                return Err(AuthError::InvalidFormat(
                    "Invalid authorization header format".to_string(),
                ));
            }

            let signature = signature.unwrap();
            let singed_headers = singed_headers.unwrap();
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
                signed_headers: singed_headers.into_iter().map(|v| v.to_string()).collect(),
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
        Self {
            ksigning,
            last_hash: lasthash,
            region,
            xamz_date: xamz_date.clone(),
            date: xamz_date[..8].to_string(),
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
