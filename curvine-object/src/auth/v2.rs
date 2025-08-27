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
#[cfg(test)]
mod v2_test {
    use crate::utils::GenericResult;
    #[test]
    fn v2_signature_test() -> GenericResult<()> {
        let signature = super::get_v2_signature(
            "root12345",
            "GET",
            "/test/hello.txt",
            "application/text",
            "Mon, 07 Apr 2025 09:20:53 +0000",
        )?;
        assert!(
            signature == "ZDG7mtEBYOSLC8PfKoz9iHR23fk=",
            "expect ZDG7mtEBYOSLC8PfKoz9iHR23fk=,get {signature}"
        );
        Ok(())
    }
}
