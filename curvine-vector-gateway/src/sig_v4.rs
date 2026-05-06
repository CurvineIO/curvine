use std::io::Write;

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const SIGV4_SERVICE_S3: &str = "s3";
pub const SIGV4_SERVICE_S3VECTORS: &str = "s3vectors";

pub trait VHeader {
    fn get_header(&self, key: &str) -> Option<String>;
    fn set_header(&mut self, key: &str, val: &str);
    fn delete_header(&mut self, key: &str);
    fn rng_header(&self, cb: impl FnMut(&str, &str) -> bool);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalQueryPair {
    pub key: String,
    pub val: String,
}

#[derive(Debug, Clone)]
pub struct BaseArgs {
    pub region: String,
    pub service: String,
    pub access_key: String,
    pub content_hash: String,
    pub signed_headers: Vec<String>,
    pub signature: String,
    pub date: String,
}

#[derive(Debug, Error)]
pub enum SigV4ExtractError {
    #[error("missing Authorization header")]
    MissingAuthorizationHeader,
    #[error("invalid Authorization header: {0}")]
    InvalidFormat(String),
}

pub fn extract_args<R: VHeader>(r: &R) -> Result<BaseArgs, SigV4ExtractError> {
    let authorization = r
        .get_header("authorization")
        .ok_or(SigV4ExtractError::MissingAuthorizationHeader)?;

    let authorization = authorization.trim();
    let heads = authorization.splitn(2, ' ').collect::<Vec<&str>>();
    if heads.len() != 2 {
        return Err(SigV4ExtractError::InvalidFormat(
            "authorization scheme split".into(),
        ));
    }

    match heads[0] {
        "AWS4-HMAC-SHA256" => {
            let heads = heads[1].split(',').collect::<Vec<&str>>();
            if heads.len() != 3 {
                return Err(SigV4ExtractError::InvalidFormat(
                    "AWS4-HMAC-SHA256 parameters".into(),
                ));
            }

            let mut credential = None;
            let mut signed_headers = None;
            let mut signature = None;

            for head in heads {
                let heads = head.trim().splitn(2, '=').collect::<Vec<&str>>();
                if heads.len() != 2 {
                    return Err(SigV4ExtractError::InvalidFormat("key=value pair".into()));
                }
                match heads[0] {
                    "Credential" => {
                        let heads = heads[1].split('/').collect::<Vec<&str>>();
                        if heads.len() != 5 {
                            return Err(SigV4ExtractError::InvalidFormat(
                                "credential scope".into(),
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
                        return Err(SigV4ExtractError::InvalidFormat(
                            "unknown authorization parameter".into(),
                        ));
                    }
                }
            }

            let signature = signature
                .ok_or_else(|| SigV4ExtractError::InvalidFormat("missing Signature".into()))?;
            let signed_headers = signed_headers
                .ok_or_else(|| SigV4ExtractError::InvalidFormat("missing SignedHeaders".into()))?;
            let credential = credential
                .ok_or_else(|| SigV4ExtractError::InvalidFormat("missing Credential".into()))?;

            let content_hash = r.get_header("x-amz-content-sha256").ok_or_else(|| {
                SigV4ExtractError::InvalidFormat("missing x-amz-content-sha256".into())
            })?;

            let x_amz_date = r
                .get_header("x-amz-date")
                .ok_or_else(|| SigV4ExtractError::InvalidFormat("missing x-amz-date".into()))?;
            validate_x_amz_date_iso8601_strict(&x_amz_date)
                .map_err(SigV4ExtractError::InvalidFormat)?;
            if credential.1.len() != 8 || !x_amz_date.starts_with(credential.1) {
                return Err(SigV4ExtractError::InvalidFormat(
                    "x-amz-date does not match Credential scope date".into(),
                ));
            }

            Ok(BaseArgs {
                content_hash,
                region: credential.2.to_string(),
                service: credential.3.to_string(),
                access_key: credential.0.to_string(),
                signed_headers: signed_headers.into_iter().map(|v| v.to_string()).collect(),
                signature: signature.to_string(),
                date: credential.1.to_string(),
            })
        }
        _ => Err(SigV4ExtractError::InvalidFormat(
            "unsupported authorization scheme".into(),
        )),
    }
}

#[derive(Debug, Error)]
pub enum SigV4SignatureError {
    #[error("hmac initialization failed: {0}")]
    Hmac(String),
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
    mut query: Vec<CanonicalQueryPair>,
) -> Result<(String, HmacSha256CircleHasher), SigV4SignatureError> {
    let x_amz_date = req
        .get_header("x-amz-date")
        .ok_or_else(|| SigV4SignatureError::Hmac("missing x-amz-date".into()))?;
    validate_x_amz_date_iso8601_strict(&x_amz_date).map_err(SigV4SignatureError::Hmac)?;

    let canonical_headers = signed_headers
        .iter()
        .map(|name| {
            let header_name = name.to_string().to_lowercase();
            let value = req.get_header(&header_name).ok_or_else(|| {
                SigV4SignatureError::Hmac(format!("missing signed header: {header_name}"))
            })?;
            Ok::<String, SigV4SignatureError>(format!("{header_name}:{}", value.trim()))
        })
        .collect::<Result<Vec<String>, SigV4SignatureError>>()?
        .join("\n");

    query.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| a.val.cmp(&b.val)));
    let canonical_query_string = query
        .iter()
        .map(|kv| format!("{}={}", kv.key, kv.val))
        .collect::<Vec<String>>()
        .join("&");

    let signed_headers_string = signed_headers
        .iter()
        .map(|v| v.to_string().to_lowercase())
        .collect::<Vec<String>>()
        .join(";");

    let canonical_request = format!(
        "{method}\n{url_path}\n{canonical_query_string}\n{canonical_headers}\n\n{signed_headers_string}\n{content_hash}"
    );

    let ksigning = get_v4_ksigning(secretkey, region, service, x_amz_date.as_str())?;

    let canonical_hash = {
        let mut hasher = Sha256::default();
        let _ = hasher.write_all(canonical_request.as_bytes());
        hex::encode(hasher.finalize())
    };

    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{x_amz_date}\n{}/{}/{}/aws4_request\n{canonical_hash}",
        &x_amz_date[..8],
        region,
        service,
    );

    let mut hasher = Hmac::<Sha256>::new_from_slice(&ksigning)
        .map_err(|e| SigV4SignatureError::Hmac(e.to_string()))?;
    hasher.update(string_to_sign.as_bytes());
    let signature = hex::encode(hasher.finalize().into_bytes());

    Ok((
        signature.clone(),
        HmacSha256CircleHasher::new(
            ksigning,
            signature,
            x_amz_date,
            region.to_string(),
            service.to_string(),
        ),
    ))
}

fn validate_x_amz_date_iso8601_strict(x_amz_date: &str) -> Result<(), String> {
    let b = x_amz_date.as_bytes();
    if b.len() != 16 {
        return Err(format!("invalid x-amz-date length (got {})", b.len()));
    }
    if b[8] != b'T' || b[15] != b'Z' {
        return Err("invalid x-amz-date layout".into());
    }
    if !(0..8).chain(9..15).all(|i| b[i].is_ascii_digit()) {
        return Err("invalid x-amz-date digits".into());
    }
    Ok(())
}

fn get_v4_ksigning(
    secretkey: &str,
    region: &str,
    service: &str,
    x_amz_date: &str,
) -> Result<[u8; 32], SigV4SignatureError> {
    validate_x_amz_date_iso8601_strict(x_amz_date).map_err(SigV4SignatureError::Hmac)?;
    let mut target = [0u8; 32];
    circle_hmac_sha256(
        format!("AWS4{}", secretkey).as_str(),
        &[
            &x_amz_date.as_bytes()[..8],
            region.as_bytes(),
            service.as_bytes(),
            b"aws4_request",
        ],
        &mut target,
    )?;
    Ok(target)
}

fn circle_hmac_sha256(
    init_key: &str,
    values: &[&[u8]],
    target: &mut [u8],
) -> Result<(), SigV4SignatureError> {
    let mut hasher = Hmac::<Sha256>::new_from_slice(init_key.as_bytes())
        .map_err(|e| SigV4SignatureError::Hmac(e.to_string()))?;
    hasher.update(values[0]);
    let mut next = hasher.finalize().into_bytes();
    for value in values.iter().skip(1) {
        let mut hasher = Hmac::<Sha256>::new_from_slice(&next)
            .map_err(|e| SigV4SignatureError::Hmac(e.to_string()))?;
        hasher.update(value);
        next = hasher.finalize().into_bytes();
    }
    target[0..next.len()].copy_from_slice(&next);
    Ok(())
}

#[derive(Clone)]
pub struct HmacSha256CircleHasher {
    ksigning: [u8; 32],
    last_hash: String,
    x_amz_date: String,
    region: String,
    service: String,
    date: String,
}

impl HmacSha256CircleHasher {
    pub fn new(
        ksigning: [u8; 32],
        last_hash: String,
        x_amz_date: String,
        region: String,
        service: String,
    ) -> Self {
        let date_prefix = x_amz_date
            .get(..8)
            .filter(|s| s.is_ascii())
            .unwrap_or("19700101")
            .to_string();
        Self {
            ksigning,
            last_hash,
            x_amz_date,
            region,
            service,
            date: date_prefix,
        }
    }

    pub fn next(&mut self, curr_hash: &str) -> Result<String, SigV4SignatureError> {
        let mut hasher = Hmac::<Sha256>::new_from_slice(&self.ksigning)
            .map_err(|e| SigV4SignatureError::Hmac(e.to_string()))?;
        let to_sign = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}/{}/{}/aws4_request\n{}\n{}\n{}",
            self.x_amz_date,
            self.date,
            self.region,
            self.service,
            self.last_hash,
            EMPTY_PAYLOAD_HASH,
            curr_hash
        );
        hasher.update(to_sign.as_bytes());
        let out = hex::encode(hasher.finalize().into_bytes());
        self.last_hash.clone_from(&out);
        Ok(out)
    }
}

#[derive(Clone)]
pub struct V4Head {
    signature: String,
    region: String,
    access_key: String,
    circle_hasher: HmacSha256CircleHasher,
}

impl V4Head {
    pub fn new(
        signature: String,
        region: String,
        access_key: String,
        hasher: HmacSha256CircleHasher,
    ) -> Self {
        Self {
            signature,
            region,
            access_key,
            circle_hasher: hasher,
        }
    }

    pub fn signature(&self) -> &str {
        &self.signature
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    pub fn access_key(&self) -> &str {
        &self.access_key
    }

    pub fn hasher(&mut self) -> &mut HmacSha256CircleHasher {
        &mut self.circle_hasher
    }
}

pub const EMPTY_PAYLOAD_HASH: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
