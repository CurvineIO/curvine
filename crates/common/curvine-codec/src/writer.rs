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

pub struct Writer {
    buf: Vec<u8>,
    version: u8,
}

impl Writer {
    #[inline]
    pub fn new(version: u8) -> CodecResult<Self> {
        Self::with_capacity(version, 0)
    }

    pub fn with_capacity(version: u8, capacity: usize) -> CodecResult<Self> {
        if let Some(msg) = Codec::version_range_error(version) {
            return Err(CodecError::encode(msg));
        }
        Ok(Self {
            buf: Vec::with_capacity(capacity),
            version,
        })
    }

    #[inline(always)]
    pub fn version(&self) -> u8 {
        self.version
    }

    #[inline]
    pub fn into_inner(self) -> Vec<u8> {
        self.buf
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    #[inline(always)]
    pub fn reserve(&mut self, additional: usize) {
        self.buf.reserve(additional);
    }

    #[inline(always)]
    pub fn put_u8(&mut self, value: u8) -> CodecResult<()> {
        self.buf.push(value);
        Ok(())
    }

    #[inline(always)]
    pub fn put_i8(&mut self, value: i8) -> CodecResult<()> {
        self.buf.push(value as u8);
        Ok(())
    }

    #[inline(always)]
    pub fn put_u16(&mut self, value: u16) -> CodecResult<()> {
        self.buf.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    #[inline(always)]
    pub fn put_i16(&mut self, value: i16) -> CodecResult<()> {
        self.buf.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    #[inline(always)]
    pub fn put_u32(&mut self, value: u32) -> CodecResult<()> {
        self.buf.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    #[inline(always)]
    pub fn put_i32(&mut self, value: i32) -> CodecResult<()> {
        self.buf.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    #[inline(always)]
    pub fn put_u64(&mut self, value: u64) -> CodecResult<()> {
        self.buf.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    #[inline(always)]
    pub fn put_i64(&mut self, value: i64) -> CodecResult<()> {
        self.buf.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    #[inline(always)]
    pub fn put_f32(&mut self, value: f32) -> CodecResult<()> {
        self.buf.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    #[inline(always)]
    pub fn put_f64(&mut self, value: f64) -> CodecResult<()> {
        self.buf.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    #[inline(always)]
    pub fn put_bool(&mut self, value: bool) -> CodecResult<()> {
        self.buf.push(value as u8);
        Ok(())
    }

    #[inline(always)]
    pub fn put_str(&mut self, value: &str) -> CodecResult<()> {
        self.put_bytes(value.as_bytes())
    }

    #[inline(always)]
    pub fn put_bytes(&mut self, value: &[u8]) -> CodecResult<()> {
        self.put_len(value.len())?;
        self.buf.extend_from_slice(value);
        Ok(())
    }

    #[inline(always)]
    pub fn put_slice(&mut self, value: &[u8]) -> CodecResult<()> {
        self.buf.extend_from_slice(value);
        Ok(())
    }

    #[inline(always)]
    pub fn put_len(&mut self, len: usize) -> CodecResult<()> {
        if len > u32::MAX as usize {
            return Err(Codec::err_len_exceeds_u32(len));
        }
        self.buf.extend_from_slice(&(len as u32).to_le_bytes());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_rejects_out_of_range_version() {
        assert!(Writer::new(0).is_err());
        assert!(Writer::new(crate::MAX_VERSION + 1).is_err());
        assert_eq!(Writer::new(7).unwrap().version(), 7);
    }

    #[test]
    fn put_primitives_little_endian() {
        let mut writer = Writer::new(1).unwrap();
        writer.put_u16(0x0102).unwrap();
        writer.put_u32(0x0102_0304).unwrap();
        writer.put_u64(0x0102_0304_0506_0708).unwrap();
        writer.put_i16(-2).unwrap();
        writer.put_i32(-2).unwrap();
        writer.put_i64(-2).unwrap();
        let bytes = writer.into_inner();
        assert_eq!(bytes.as_slice()[..2], [0x02, 0x01]);
        assert_eq!(bytes.as_slice()[2..6], [0x04, 0x03, 0x02, 0x01]);
        assert_eq!(
            bytes.as_slice()[6..14],
            [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]
        );
        // Two's complement little-endian.
        assert_eq!(bytes.as_slice()[14..16], [0xFE, 0xFF]);
        assert_eq!(bytes.as_slice()[16..20], [0xFE, 0xFF, 0xFF, 0xFF]);
        assert_eq!(
            bytes.as_slice()[20..28],
            [0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
        );
    }

    #[test]
    fn put_str_writes_u32_prefix_and_utf8() {
        let mut writer = Writer::new(1).unwrap();
        writer.put_str("cv").unwrap();
        let bytes = writer.into_inner();
        assert_eq!(bytes.as_slice(), &[0x02, 0x00, 0x00, 0x00, b'c', b'v']);

        let mut writer = Writer::new(1).unwrap();
        writer.put_str("").unwrap();
        assert_eq!(writer.into_inner().as_slice(), &[0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn put_bool_writes_one_byte() {
        let mut writer = Writer::new(1).unwrap();
        writer.put_bool(true).unwrap();
        writer.put_bool(false).unwrap();
        assert_eq!(writer.into_inner().as_slice(), &[0x01, 0x00]);
    }

    #[test]
    fn put_slice_has_no_prefix() {
        let mut writer = Writer::new(1).unwrap();
        writer.put_slice(&[0xAA, 0xBB]).unwrap();
        assert_eq!(writer.into_inner().as_slice(), &[0xAA, 0xBB]);
    }

    // 64-bit only: on 32-bit targets `u32::MAX + 1` does not fit in usize.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn put_len_rejects_length_beyond_u32_prefix() {
        let mut writer = Writer::new(1).unwrap();
        assert!(writer.put_len(u32::MAX as usize + 1).is_err());
        // The maximum representable length is still accepted.
        assert!(writer.put_len(u32::MAX as usize).is_ok());
    }
}
