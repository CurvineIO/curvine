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

use crate::FuseResult;
use orpc::err_box;
use orpc::sys::FFIUtils;
use std::ffi::OsStr;
use std::mem::size_of;

// fuse requests the decoder.
pub struct FuseDecoder<'a> {
    buf: &'a [u8],
}

impl<'a> FuseDecoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        FuseDecoder { buf }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // Get a structure
    pub fn get_struct<T>(&mut self) -> FuseResult<&'a T> {
        let bytes = self.get_slice(size_of::<T>())?;
        Self::parse(bytes)
    }

    pub fn parse<T>(bytes: &[u8]) -> FuseResult<&T> {
        if bytes.len() != size_of::<T>() {
            return err_box!("Byte array length and type size mismatch");
        }

        let ptr = unsafe { (bytes.as_ptr() as *const T).as_ref() };
        match ptr {
            None => err_box!("Not found data"),
            Some(v) => Ok(v),
        }
    }

    pub fn get_slice(&mut self, len: usize) -> FuseResult<&'a [u8]> {
        if len > self.buf.len() {
            return err_box!("Not found data");
        }
        let bytes = &self.buf[..len];
        self.buf = &self.buf[len..];
        Ok(bytes)
    }

    pub fn get_struct_vec<T>(&mut self, count: usize) -> FuseResult<Vec<&'a T>> {
        if count == 0 {
            return Ok(vec![]);
        }

        let mut res = Vec::with_capacity(count);
        for _ in 0..count {
            let item: &'a T = self.get_struct()?;
            res.push(item);
        }
        Ok(res)
    }

    pub fn get_os_str(&mut self) -> FuseResult<&'a OsStr> {
        let len = match self.buf.iter().position(|&c| c == 0) {
            None => return err_box!("Not found data"),
            Some(v) => v,
        };

        let bytes = self.get_slice(len)?;
        let _zero = self.get_slice(1)?;
        Ok(FFIUtils::get_os_str(bytes))
    }

    /// Fetch a slice of all remaining bytes.
    pub fn get_all(&mut self) -> FuseResult<&'a [u8]> {
        let bytes = self.buf;
        self.buf = &[];
        Ok(bytes)
    }

    /// Get exactly the specified number of bytes (for FUSE_SETXATTR value)
    pub fn get_bytes(&mut self, size: usize) -> FuseResult<&'a [u8]> {
        if size > self.buf.len() {
            return err_box!("Not enough data for specified size");
        }
        self.get_slice(size)
    }
}

#[cfg(test)]
pub mod tests {
    use crate::session::fuse_decoder::FuseDecoder;
    use orpc::CommonResult;

    const DATA: [u8; 10] = [0x66, 0x6f, 0x6f, 0x00, 0x62, 0x61, 0x72, 0x00, 0x62, 0x61];

    #[test]
    fn string() -> CommonResult<()> {
        let mut coder = FuseDecoder::new(&DATA);

        let str1 = coder.get_os_str()?;
        assert_eq!(str1, "foo");

        let str2 = coder.get_os_str().unwrap();
        assert_eq!(str2, "bar");

        assert_eq!(coder.len(), 2);

        Ok(())
    }
}
