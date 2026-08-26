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

use curvine_core_error::{err_box, CommonError};
use num_enum::{FromPrimitive, IntoPrimitive};
use serde::{Deserialize, Serialize};

/// Base timestamp used to compute TTL expiration.
///
/// - `Mtime`: expiration = last-modified time + ttl_ms (legacy behaviour).
/// - `Atime`: expiration = last-accessed time + ttl_ms. The master refreshes
///   `atime` on read opens for files whose storage policy selects this mode,
///   so actively-read files keep surviving while cold ones are reclaimed.
#[repr(i32)]
#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    PartialEq,
    IntoPrimitive,
    FromPrimitive,
    Eq,
    Default,
    Hash,
)]
pub enum TtlType {
    #[default]
    Mtime = 0,
    Atime = 1,
}

impl TtlType {
    pub fn is_atime(&self) -> bool {
        matches!(self, TtlType::Atime)
    }

    /// Encode for the internal `INTERNAL_TTL_TYPE_XATTR` attribute.
    pub fn to_xattr_value(self) -> Vec<u8> {
        vec![i32::from(self) as u8]
    }

    /// Decode from the internal xattr value; absent/unknown means `Mtime`.
    pub fn from_xattr_value(value: Option<&[u8]>) -> Self {
        match value {
            Some(v) if v.first() == Some(&1) => TtlType::Atime,
            _ => TtlType::Mtime,
        }
    }

    /// Sync the internal `INTERNAL_TTL_TYPE_XATTR` attribute with this
    /// policy. Atime inserts the marker; Mtime removes any stale marker so a
    /// mount (or builder) override always wins over a previously-stamped
    /// value, e.g. an atime marker coming from the client conf when merging
    /// create opts of an mtime mount.
    pub fn apply_to_xattrs(self, x_attrs: &mut std::collections::HashMap<String, Vec<u8>>) {
        let key = crate::file_status::INTERNAL_TTL_TYPE_XATTR.to_string();
        if self.is_atime() {
            x_attrs.insert(key, self.to_xattr_value());
        } else {
            x_attrs.remove(&key);
        }
    }
}

impl TryFrom<&str> for TtlType {
    type Error = CommonError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let typ = match value.to_lowercase().as_str() {
            "mtime" => TtlType::Mtime,
            "atime" => TtlType::Atime,
            _ => return err_box!("invalid ttl type: {}", value),
        };

        Ok(typ)
    }
}

impl From<TtlType> for &str {
    fn from(value: TtlType) -> Self {
        match value {
            TtlType::Mtime => "mtime",
            TtlType::Atime => "atime",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_from_str() {
        assert_eq!(TtlType::try_from("mtime").unwrap(), TtlType::Mtime);
        assert_eq!(TtlType::try_from("atime").unwrap(), TtlType::Atime);
        assert_eq!(TtlType::try_from("ATIME").unwrap(), TtlType::Atime);
        assert!(TtlType::try_from("ctime").is_err());
    }

    #[test]
    fn test_default() {
        assert_eq!(TtlType::default(), TtlType::Mtime);
        assert!(!TtlType::Mtime.is_atime());
        assert!(TtlType::Atime.is_atime());
    }

    #[test]
    fn test_int_roundtrip() {
        assert_eq!(TtlType::from(0_i32), TtlType::Mtime);
        assert_eq!(TtlType::from(1_i32), TtlType::Atime);
        assert_eq!(i32::from(TtlType::Mtime), 0);
        assert_eq!(i32::from(TtlType::Atime), 1);
    }

    #[test]
    fn test_apply_to_xattrs_insert_and_clear() {
        let mut x_attrs = std::collections::HashMap::new();
        let key = crate::file_status::INTERNAL_TTL_TYPE_XATTR.to_string();

        TtlType::Atime.apply_to_xattrs(&mut x_attrs);
        assert_eq!(x_attrs.get(&key).map(|v| v.as_slice()), Some(&[1u8][..]));

        // Overriding back to mtime must clear the stale atime marker.
        TtlType::Mtime.apply_to_xattrs(&mut x_attrs);
        assert!(!x_attrs.contains_key(&key));

        // Removing again is still fine (no key present).
        TtlType::Mtime.apply_to_xattrs(&mut x_attrs);
        assert!(!x_attrs.contains_key(&key));
    }
}
