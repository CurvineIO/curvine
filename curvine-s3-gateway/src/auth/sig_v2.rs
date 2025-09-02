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
