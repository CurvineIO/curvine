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

use curvine_codec::{Codec, Decode, Encode};
use curvine_core_error::CommonResult;

/// Schema versions understood by the metadata codec. The discriminant is the
/// version stamped into the persisted record header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum MetadataVersion {
    V1 = 1,
}

impl MetadataVersion {
    /// Version written by the current binary.
    pub(crate) const CURRENT: Self = Self::V1;

    pub(crate) const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Serializes metadata records with the `curvine-codec` persisted layout.
///
/// This is a namespace for associated functions only; the schema version is
/// selected statically via [`MetadataVersion`], so no codec instance is needed.
pub(crate) struct MetadataCodec;

impl MetadataCodec {
    /// Encodes with the version written by the current binary.
    pub(crate) fn encode<T: Encode + ?Sized>(value: &T) -> CommonResult<Vec<u8>> {
        Ok(Codec::encode(MetadataVersion::CURRENT.as_u8(), value)?)
    }

    /// Decodes one metadata record. The schema version is read from the record
    /// header and validated by the codec, so no version is passed in here.
    pub(crate) fn decode<T: Decode>(bytes: &[u8]) -> CommonResult<T> {
        Ok(Codec::decode(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::MountInfo;

    #[test]
    fn encode_stamps_current_version_and_round_trips() {
        let info = MountInfo {
            cv_path: "/data".to_string(),
            ufs_path: "oss://bucket/data".to_string(),
            mount_id: 1,
            ..Default::default()
        };

        let bytes = MetadataCodec::encode(&info).unwrap();
        assert_eq!(
            bytes[0],
            Codec::make_header(MetadataVersion::CURRENT.as_u8()).unwrap()
        );

        let decoded: MountInfo = MetadataCodec::decode(&bytes).unwrap();
        assert_eq!(decoded, info);
    }

    #[test]
    fn decode_rejects_newer_schema_version() {
        // Version 2 header with no payload.
        let err = MetadataCodec::decode::<MountInfo>(&[0x82]).unwrap_err();
        assert!(err.to_string().contains("newer than supported"));
    }

    #[test]
    fn current_version_is_one() {
        assert_eq!(MetadataVersion::CURRENT.as_u8(), 1);
    }
}
