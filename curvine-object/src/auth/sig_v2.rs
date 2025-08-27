extern crate hmac;
use self::hmac::{Hmac, Mac};
use base64::Engine;

use crate::utils::GenericResult;
#[allow(dead_code)]
fn get_v2_signature(
    secretkey: &str,
    method: &str,
    url_path: &str,
    content_type: &str,
    date: &str,
) -> GenericResult<String> {
    let tosign = format!("{method}\n\n{content_type}\n{date}\n{url_path}");
    let hsh = Hmac::<sha1::Sha1>::new_from_slice(secretkey.as_bytes());
    if let Err(err) = hsh {
        return Err(err.to_string());
    }
    let mut hsh = hsh.unwrap();
    hsh.update(tosign.as_bytes());
    let ans = base64::engine::general_purpose::STANDARD.encode(hsh.finalize().into_bytes());
    Ok(ans)
}
