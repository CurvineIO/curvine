// Copyright 2026 OPPO.
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

use crate::{
    CodecError, CodecResult, Reader, Writer, CURRENT_VERSION, INIT_CAPACITY, MAGIC, MAX_VERSION,
};

pub trait Encode {
    fn encode(&self, writer: &mut Writer) -> CodecResult<()>;

    #[doc(hidden)]
    #[inline]
    fn blob_view(_items: &[Self]) -> Option<&[u8]>
    where
        Self: Sized,
    {
        None
    }
}

pub trait Decode: Sized {
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self>;

    #[doc(hidden)]
    #[inline]
    fn decode_vec(_reader: &mut Reader<'_>) -> CodecResult<Option<Vec<Self>>> {
        Ok(None)
    }
}

pub struct Codec;

impl Codec {
    pub fn encode<T: Encode + ?Sized>(version: u8, value: &T) -> CodecResult<Vec<u8>> {
        let mut writer = Writer::with_capacity(version, INIT_CAPACITY)?;
        let header = Self::make_header(writer.version())?;
        writer.put_u8(header)?;
        value.encode(&mut writer)?;
        Ok(writer.into_inner())
    }

    pub fn encode_unversioned<T: Encode + ?Sized>(value: &T) -> CodecResult<Vec<u8>> {
        let mut writer = Writer::with_capacity(CURRENT_VERSION, INIT_CAPACITY)?;
        value.encode(&mut writer)?;
        Ok(writer.into_inner())
    }

    pub fn decode<T: Decode>(bytes: &[u8]) -> CodecResult<T> {
        let version = Self::parse_version(bytes)?;
        let mut reader = Reader::new(&bytes[1..], version)?;
        let value = T::decode(&mut reader)?;
        Self::ensure_no_trailing(&reader, bytes.len())?;
        Ok(value)
    }

    pub fn parse_version(bytes: &[u8]) -> CodecResult<u8> {
        match bytes.first() {
            Some(&header) => Self::parse_header(header),
            None => Err(CodecError::decode("empty input")),
        }
    }

    pub fn make_header(version: u8) -> CodecResult<u8> {
        if let Some(msg) = Self::version_range_error(version) {
            return Err(CodecError::encode(msg));
        }
        Ok((MAGIC << 6) | version)
    }

    #[cold]
    #[inline(never)]
    fn version_range_msg(version: u8) -> String {
        format!(
            "schema version {} is out of range 1..={}",
            version, MAX_VERSION
        )
    }

    #[inline]
    pub(crate) fn version_range_error(version: u8) -> Option<String> {
        if version == 0 || version > MAX_VERSION {
            Some(Self::version_range_msg(version))
        } else {
            None
        }
    }

    pub fn parse_header(header: u8) -> CodecResult<u8> {
        if (header >> 6) != MAGIC {
            return Err(CodecError::decode(format!(
                "bad magic {:#04b} in header byte {:#04x}, expected {:#04b}",
                header >> 6,
                header,
                MAGIC
            )));
        }
        let version = header & MAX_VERSION;
        if version == 0 {
            return Err(CodecError::decode(format!(
                "invalid schema version 0 in header byte {:#04x}",
                header
            )));
        }
        if version > CURRENT_VERSION {
            return Err(CodecError::decode(format!(
                "schema version {} is newer than supported {}",
                version, CURRENT_VERSION
            )));
        }
        Ok(version)
    }

    pub fn is_codec_format(bytes: &[u8]) -> bool {
        !bytes.is_empty() && (bytes[0] >> 6) == MAGIC && (bytes[0] & MAX_VERSION) != 0
    }

    #[cold]
    #[inline(never)]
    fn err_trailing(remaining: usize, input_len: usize) -> CodecError {
        CodecError::decode(format!(
            "{} trailing bytes after value (input {} bytes)",
            remaining, input_len
        ))
    }

    #[inline]
    fn ensure_no_trailing(reader: &Reader<'_>, input_len: usize) -> CodecResult<()> {
        if !reader.is_empty() {
            return Err(Self::err_trailing(reader.remaining(), input_len));
        }
        Ok(())
    }

    #[cold]
    #[inline(never)]
    pub(crate) fn err_end_of_input(pos: usize, need: usize, remaining: usize) -> CodecError {
        CodecError::decode(format!(
            "unexpected end of input at offset {}: need {} bytes, {} remaining",
            pos, need, remaining
        ))
    }

    #[cold]
    #[inline(never)]
    pub(crate) fn err_invalid_bool(value: u8, offset: usize) -> CodecError {
        CodecError::decode(format!(
            "invalid bool byte {:#04x} at offset {}",
            value, offset
        ))
    }

    #[cold]
    #[inline(never)]
    pub(crate) fn err_invalid_utf8(offset: usize, err: std::str::Utf8Error) -> CodecError {
        CodecError::decode(format!(
            "invalid utf-8 string at offset {}: {}",
            offset, err
        ))
    }

    #[cold]
    #[inline(never)]
    pub(crate) fn err_bytes_len(len: usize, offset: usize, remaining: usize) -> CodecError {
        CodecError::decode(format!(
            "byte sequence length {} at offset {} exceeds remaining {} bytes",
            len, offset, remaining
        ))
    }

    #[cold]
    #[inline(never)]
    pub(crate) fn err_container_len(len: usize, offset: usize, remaining: usize) -> CodecError {
        CodecError::decode(format!(
            "container length {} at offset {} exceeds remaining {} bytes",
            len, offset, remaining
        ))
    }

    #[cold]
    #[inline(never)]
    pub(crate) fn err_len_exceeds_u32(len: usize) -> CodecError {
        CodecError::encode(format!("length {} exceeds u32 length prefix", len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trip() {
        for version in 1..=MAX_VERSION {
            let header = Codec::make_header(version).unwrap();
            assert_eq!(header >> 6, MAGIC);
            if version <= CURRENT_VERSION {
                assert_eq!(Codec::parse_header(header).unwrap(), version);
            } else {
                assert!(Codec::parse_header(header).is_err());
            }
        }
        assert_eq!(Codec::make_header(CURRENT_VERSION).unwrap(), 0x81);
    }

    #[test]
    fn header_rejects_bad_version() {
        assert!(Codec::make_header(0).is_err());
        assert!(Codec::make_header(MAX_VERSION + 1).is_err());
    }

    #[test]
    fn parse_header_rejects_bad_magic() {
        // Top bits 0b00/0b01/0b11 are not the codec magic.
        for header in [0x00, 0x01, 0x40, 0x41, 0xC0, 0xC1, 0xFF] {
            assert!(
                Codec::parse_header(header).is_err(),
                "header {:#04x}",
                header
            );
        }
    }

    #[test]
    fn parse_header_rejects_zero_version() {
        // 0b10_000000 packs the magic with version 0.
        assert!(Codec::parse_header(0b10_000000).is_err());
    }

    #[test]
    fn sniff_distinguishes_codec_from_plain_bytes() {
        assert!(Codec::is_codec_format(&[0x81]));
        assert!(Codec::is_codec_format(&[0x81, 0x00]));
        assert!(!Codec::is_codec_format(&[]));
        // Small values (legacy enum tags, short string length prefixes).
        assert!(!Codec::is_codec_format(&[0x00]));
        assert!(!Codec::is_codec_format(&[0x01]));
        assert!(!Codec::is_codec_format(&[0x02]));
        assert!(!Codec::is_codec_format(&[0x40]));
        // Different magic quadrant.
        assert!(!Codec::is_codec_format(&[0xC0]));
        assert!(!Codec::is_codec_format(&[0xC1]));
        // Magic ok but version 0 is not a valid codec record.
        assert!(!Codec::is_codec_format(&[0b10_000000]));
    }

    #[test]
    fn codec_round_trip_scalars() {
        let bytes = Codec::encode(CURRENT_VERSION, &0x0102_0304_0506_0708u64).unwrap();
        assert_eq!(bytes[0], (MAGIC << 6) | CURRENT_VERSION);
        assert_eq!(
            &bytes[1..],
            &[0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]
        );
        let value: u64 = Codec::decode(&bytes).unwrap();
        assert_eq!(value, 0x0102_0304_0506_0708);
    }

    #[test]
    fn codec_decode_rejects_trailing_bytes() {
        // Header + one-byte payload + one extra byte.
        let bytes = [0x81, 0x01, 0x02];
        let result: CodecResult<u8> = Codec::decode(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn codec_decode_rejects_empty_input() {
        let result: CodecResult<u8> = Codec::decode(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn round_trip_stamps_header_byte() {
        let value = String::from("curvine");
        let bytes = Codec::encode(CURRENT_VERSION, &value).unwrap();
        assert_eq!(bytes[0], 0x81);
        assert_eq!(bytes[1], 0x07);
        let decoded: String = Codec::decode(&bytes).unwrap();
        assert_eq!(decoded, "curvine");
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let bytes = [0x01, 0x00, 0x00, 0x00, 0x00];
        let result: CodecResult<u32> = Codec::decode(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn parse_version_reads_header() {
        assert_eq!(Codec::parse_version(&[0x81]).unwrap(), CURRENT_VERSION);
        assert!(Codec::parse_version(&[]).is_err());
        assert!(Codec::parse_version(&[0x01]).is_err());
    }

    #[test]
    fn newer_version_sniffs_as_codec_then_fails_loudly() {
        // Version 2: the sniff must return true so dispatch routes the record
        // into the codec branch...
        assert!(Codec::is_codec_format(&[0x82]));
        // ...where the downgrade guard rejects it instead of misreading it as
        // legacy data.
        let result: CodecResult<u8> = Codec::decode(&[0x82, 0x00]);
        let err = result.unwrap_err();
        assert!(err.to_string().contains("newer than supported"));
    }

    #[test]
    fn decode_rejects_header_only_payload() {
        // Header byte but no payload bytes for a fixed-width type.
        let result: CodecResult<u64> = Codec::decode(&[0x81]);
        assert!(result.is_err());
    }

    #[test]
    fn encode_requires_explicit_version() {
        let bytes = Codec::encode(2, &7u16).unwrap();
        assert_eq!(bytes[0], (MAGIC << 6) | 2);
        assert_eq!(&bytes[1..], &[0x07, 0x00]);
        // Same version contract as the header constructor.
        assert!(Codec::encode(0, &7u16).is_err());
    }

    #[test]
    fn encode_unversioned_writes_no_header() {
        let bare = Codec::encode_unversioned(&7u16).unwrap();
        assert_eq!(bare.as_slice(), &[0x07, 0x00]);

        let framed = Codec::encode(CURRENT_VERSION, &7u16).unwrap();
        assert_eq!(&framed[1..], bare.as_slice());

        // Headerless bytes are not a record: `decode` requires the header.
        assert!(Codec::decode::<u16>(&bare).is_err());
    }
}
