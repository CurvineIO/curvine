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

use crate::error::CodecError;
use crate::{Codec, CodecResult};

pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
    version: u8,
}

impl<'a> Reader<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8], version: u8) -> CodecResult<Self> {
        if let Some(msg) = Codec::version_range_error(version) {
            return Err(CodecError::decode(msg));
        }
        Ok(Self {
            buf,
            pos: 0,
            version,
        })
    }

    #[inline(always)]
    pub fn version(&self) -> u8 {
        self.version
    }

    #[inline(always)]
    pub fn has_version(&self, version: u8) -> bool {
        self.version >= version
    }

    #[inline(always)]
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    #[inline(always)]
    pub fn position(&self) -> usize {
        self.pos
    }

    #[inline(always)]
    fn take(&mut self, n: usize) -> CodecResult<&'a [u8]> {
        let remaining = self.remaining();
        if remaining < n {
            return Err(Codec::err_end_of_input(self.pos, n, remaining));
        }
        let start = self.pos;
        self.pos += n;
        Ok(&self.buf[start..self.pos])
    }

    #[inline(always)]
    fn take_array<const N: usize>(&mut self) -> CodecResult<[u8; N]> {
        let bytes = self.take(N)?;
        match <[u8; N]>::try_from(bytes) {
            Ok(array) => Ok(array),
            Err(_) => Err(CodecError::decode("internal: slice length mismatch")),
        }
    }

    #[inline(always)]
    pub fn get_u8(&mut self) -> CodecResult<u8> {
        Ok(self.take(1)?[0])
    }

    #[inline(always)]
    pub fn get_i8(&mut self) -> CodecResult<i8> {
        Ok(self.take(1)?[0] as i8)
    }

    #[inline(always)]
    pub fn get_u16(&mut self) -> CodecResult<u16> {
        Ok(u16::from_le_bytes(self.take_array()?))
    }

    #[inline(always)]
    pub fn get_i16(&mut self) -> CodecResult<i16> {
        Ok(i16::from_le_bytes(self.take_array()?))
    }

    #[inline(always)]
    pub fn get_u32(&mut self) -> CodecResult<u32> {
        Ok(u32::from_le_bytes(self.take_array()?))
    }

    #[inline(always)]
    pub fn get_i32(&mut self) -> CodecResult<i32> {
        Ok(i32::from_le_bytes(self.take_array()?))
    }

    #[inline(always)]
    pub fn get_u64(&mut self) -> CodecResult<u64> {
        Ok(u64::from_le_bytes(self.take_array()?))
    }

    #[inline(always)]
    pub fn get_i64(&mut self) -> CodecResult<i64> {
        Ok(i64::from_le_bytes(self.take_array()?))
    }

    #[inline(always)]
    pub fn get_f32(&mut self) -> CodecResult<f32> {
        Ok(f32::from_bits(self.get_u32()?))
    }

    #[inline(always)]
    pub fn get_f64(&mut self) -> CodecResult<f64> {
        Ok(f64::from_bits(self.get_u64()?))
    }

    /// Read a bool; only byte values `0` and `1` are accepted.
    #[inline(always)]
    pub fn get_bool(&mut self) -> CodecResult<bool> {
        let offset = self.pos;
        match self.get_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(Codec::err_invalid_bool(value, offset)),
        }
    }

    #[inline(always)]
    pub fn get_str(&mut self) -> CodecResult<String> {
        let start = self.pos;
        let bytes = self.get_bytes()?;
        match String::from_utf8(bytes) {
            Ok(value) => Ok(value),
            Err(e) => Err(Codec::err_invalid_utf8(start, e.utf8_error())),
        }
    }

    #[inline(always)]
    pub fn get_bytes(&mut self) -> CodecResult<Vec<u8>> {
        let start = self.pos;
        let len = self.get_u32()? as usize;
        let remaining = self.remaining();
        if len > remaining {
            return Err(Codec::err_bytes_len(len, start, remaining));
        }

        let end = self.pos + len;
        let bytes = &self.buf[self.pos..end];
        self.pos = end;
        Ok(bytes.to_vec())
    }

    #[inline(always)]
    pub fn take_len(&mut self) -> CodecResult<usize> {
        let start = self.pos;
        let len = self.get_u32()? as usize;
        let remaining = self.remaining();
        if len > remaining {
            return Err(Codec::err_container_len(len, start, remaining));
        }
        Ok(len)
    }

    #[inline(always)]
    pub fn take_slice(&mut self, n: usize) -> CodecResult<&'a [u8]> {
        self.take(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_round_trip_against_writer() {
        let mut writer = crate::Writer::new(1).unwrap();
        writer.put_u8(0xAB).unwrap();
        writer.put_i8(-3).unwrap();
        writer.put_u16(0x0102).unwrap();
        writer.put_i16(-2).unwrap();
        writer.put_u32(0x0102_0304).unwrap();
        writer.put_i32(-2).unwrap();
        writer.put_u64(0x0102_0304_0506_0708).unwrap();
        writer.put_i64(-2).unwrap();
        writer.put_f32(1.5).unwrap();
        writer.put_f64(-2.5).unwrap();
        writer.put_bool(true).unwrap();

        let bytes = writer.into_inner();
        let mut reader = Reader::new(&bytes, 1).unwrap();
        assert_eq!(reader.get_u8().unwrap(), 0xAB);
        assert_eq!(reader.get_i8().unwrap(), -3);
        assert_eq!(reader.get_u16().unwrap(), 0x0102);
        assert_eq!(reader.get_i16().unwrap(), -2);
        assert_eq!(reader.get_u32().unwrap(), 0x0102_0304);
        assert_eq!(reader.get_i32().unwrap(), -2);
        assert_eq!(reader.get_u64().unwrap(), 0x0102_0304_0506_0708);
        assert_eq!(reader.get_i64().unwrap(), -2);
        assert_eq!(reader.get_f32().unwrap(), 1.5);
        assert_eq!(reader.get_f64().unwrap(), -2.5);
        assert!(reader.get_bool().unwrap());
        assert!(reader.is_empty());
        assert_eq!(reader.position(), bytes.len());
    }

    #[test]
    fn truncated_input_reports_offset() {
        let bytes = [0x01, 0x02];
        let mut reader = Reader::new(&bytes, 1).unwrap();
        assert!(reader.get_u32().is_err());

        let mut reader = Reader::new(&bytes, 1).unwrap();
        reader.get_u8().unwrap();
        let err = reader.get_u16().unwrap_err();
        assert!(err.to_string().contains("offset 1"));
    }

    #[test]
    fn bool_rejects_invalid_byte() {
        let bytes = [0x02];
        let mut reader = Reader::new(&bytes, 1).unwrap();
        let err = reader.get_bool().unwrap_err();
        assert!(err.to_string().contains("invalid bool"));
    }

    #[test]
    fn get_str_round_trip_and_utf8_check() {
        let mut writer = crate::Writer::new(1).unwrap();
        writer.put_str("curvine-元数据").unwrap();
        let bytes = writer.into_inner();
        let mut reader = Reader::new(&bytes, 1).unwrap();
        assert_eq!(reader.get_str().unwrap(), "curvine-元数据");

        // Invalid UTF-8 inside a well-formed length prefix.
        let bad = [0x02, 0x00, 0x00, 0x00, 0xFF, 0xFE];
        let mut reader = Reader::new(&bad, 1).unwrap();
        let err = reader.get_str().unwrap_err();
        assert!(err.to_string().contains("invalid utf-8"));
    }

    #[test]
    fn length_prefix_capped_against_remaining() {
        // Claims 0xFFFF_FFFF bytes but only a few remain: must fail before
        // any large allocation.
        let bytes = [0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x02];
        let mut reader = Reader::new(&bytes, 1).unwrap();
        assert!(reader.get_bytes().is_err());
        let mut reader = Reader::new(&bytes, 1).unwrap();
        assert!(reader.take_len().is_err());
    }

    #[test]
    fn take_slice_reads_pinned_length() {
        let bytes = [0xAA, 0xBB, 0xCC];
        let mut reader = Reader::new(&bytes, 1).unwrap();
        assert_eq!(reader.take_slice(2).unwrap(), &[0xAA, 0xBB]);
        assert_eq!(reader.take_slice(1).unwrap(), &[0xCC]);
        assert!(reader.take_slice(1).is_err());
    }

    #[test]
    fn version_context() {
        let bytes = [0x00];
        assert_eq!(Reader::new(&bytes, 2).unwrap().version(), 2);
        assert!(Reader::new(&bytes, 0).is_err());
        assert!(Reader::new(&bytes, crate::MAX_VERSION + 1).is_err());
    }

    #[test]
    fn has_version_gates_from_threshold() {
        let bytes = [0x00];
        assert!(Reader::new(&bytes, 2).unwrap().has_version(2));
        assert!(Reader::new(&bytes, 3).unwrap().has_version(2));
        assert!(!Reader::new(&bytes, 1).unwrap().has_version(2));
    }
}
