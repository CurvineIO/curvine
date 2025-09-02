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

use serde::{Deserialize, Serialize};
use std::fmt;
use subtle::ConstantTimeEq;
use zeroize::Zeroize;
#[derive(Clone)]
pub struct SecretKey(Box<str>);

impl SecretKey {
    fn new(s: impl Into<Box<str>>) -> Self {
        Self(s.into())
    }

    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Drop for SecretKey {
    fn drop(&mut self) {
        self.zeroize();
    }
}

impl Zeroize for SecretKey {
    fn zeroize(&mut self) {
        unsafe {
            let bytes = self.0.as_bytes_mut();
            bytes.zeroize();
        }
    }
}

impl ConstantTimeEq for SecretKey {
    fn ct_eq(&self, other: &Self) -> subtle::Choice {
        self.0.as_bytes().ct_eq(other.0.as_bytes())
    }
}

impl PartialEq for SecretKey {
    fn eq(&self, other: &Self) -> bool {
        self.ct_eq(other).into()
    }
}

impl Eq for SecretKey {}

impl From<String> for SecretKey {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<Box<str>> for SecretKey {
    fn from(value: Box<str>) -> Self {
        Self::new(value)
    }
}

impl From<&str> for SecretKey {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

const PLACEHOLDER: &str = "[SENSITIVE-SECRET-KEY]";

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SecretKey").field(&PLACEHOLDER).finish()
    }
}

impl<'de> Deserialize<'de> for SecretKey {
    fn deserialize<D>(deserializer: D) -> Result<SecretKey, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <String as Deserialize>::deserialize(deserializer).map(SecretKey::from)
    }
}

impl Serialize for SecretKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        <str as Serialize>::serialize(&self.0, serializer)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: SecretKey,
}

impl Credentials {
    pub fn new(access_key: String, secret_key: impl Into<SecretKey>) -> Self {
        Self {
            access_key,
            secret_key: secret_key.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secret_key_creation() {
        let secret = SecretKey::from("test-secret");
        assert_eq!(secret.expose(), "test-secret");
        assert_eq!(secret.len(), 11);
        assert!(!secret.is_empty());
    }

    #[test]
    fn test_secret_key_equality() {
        let secret1 = SecretKey::from("same-secret");
        let secret2 = SecretKey::from("same-secret");
        let secret3 = SecretKey::from("different-secret");

        assert_eq!(secret1, secret2);
        assert_ne!(secret1, secret3);
    }

    #[test]
    fn test_secret_key_debug() {
        let secret = SecretKey::from("super-secret");
        let debug_str = format!("{:?}", secret);
        assert!(debug_str.contains(PLACEHOLDER));
        assert!(!debug_str.contains("super-secret"));
    }

    #[test]
    fn test_secret_key_serialization() {
        let secret = SecretKey::from("secret-key");
        let json = serde_json::to_string(&secret).unwrap();
        assert!(json.contains(PLACEHOLDER));
        assert!(!json.contains("secret-key"));
    }

    #[test]
    fn test_credentials() {
        let creds = Credentials::new("AKIATEST".to_string(), "secret123");
        assert_eq!(creds.access_key, "AKIATEST");
        assert_eq!(creds.secret_key.expose(), "secret123");
    }
}
