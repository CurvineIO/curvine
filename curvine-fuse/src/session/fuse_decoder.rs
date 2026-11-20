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

use crate::raw::fuse_abi::FuseAbiPod;
use crate::FuseResult;
use curvine_core_error::err_box;
use curvine_sys::FFIUtils;
use std::ffi::OsStr;
use std::mem::{align_of, size_of};

// Decoder for FUSE request buffers.
pub(crate) struct FuseDecoder<'a> {
    buf: &'a [u8],
}

impl<'a> FuseDecoder<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        FuseDecoder { buf }
    }

    pub(crate) fn len(&self) -> usize {
        self.buf.len()
    }

    pub(crate) fn get_struct<T: FuseAbiPod>(&mut self) -> FuseResult<&'a T> {
        let bytes = self.get_slice(size_of::<T>())?;
        Self::parse(bytes)
    }

    /// View `bytes` as `T`.
    ///
    /// Length, zero-size and alignment preconditions are checked here; the
    /// `FuseAbiPod` bound guarantees every bit pattern is a valid `T`, so the
    /// cast cannot produce an invalid value.
    pub(crate) fn parse<T: FuseAbiPod>(bytes: &[u8]) -> FuseResult<&T> {
        let size = size_of::<T>();
        if size == 0 {
            return err_box!("Zero-sized FUSE ABI type is invalid");
        }
        if bytes.len() != size {
            return err_box!("Byte array length and type size mismatch");
        }

        let ptr = bytes.as_ptr();
        if !(ptr as usize).is_multiple_of(align_of::<T>()) {
            return err_box!("Unaligned FUSE ABI structure");
        }

        // SAFETY: `T: FuseAbiPod` guarantees every bit pattern is valid, the
        // length matches `size_of::<T>()`, the pointer is aligned, and the
        // returned reference borrows from `bytes`.
        Ok(unsafe { &*ptr.cast::<T>() })
    }

    pub(crate) fn get_slice(&mut self, len: usize) -> FuseResult<&'a [u8]> {
        if len > self.buf.len() {
            return err_box!("Not found data");
        }
        let bytes = &self.buf[..len];
        self.buf = &self.buf[len..];
        Ok(bytes)
    }

    pub(crate) fn get_struct_vec<T: FuseAbiPod>(&mut self, count: usize) -> FuseResult<Vec<&'a T>> {
        if count == 0 {
            return Ok(vec![]);
        }

        let item_size = size_of::<T>();
        if item_size == 0 {
            return err_box!("Zero-sized FUSE ABI type is invalid");
        }

        let max_count = self.buf.len() / item_size;
        if count > max_count {
            return err_box!(
                "Not enough data for {} structures: requested {}, available {}",
                std::any::type_name::<T>(),
                count,
                max_count
            );
        }

        let mut res = Vec::with_capacity(count);
        for _ in 0..count {
            let item: &'a T = self.get_struct()?;
            res.push(item);
        }
        Ok(res)
    }

    pub(crate) fn get_os_str(&mut self) -> FuseResult<&'a OsStr> {
        let len = match self.buf.iter().position(|&c| c == 0) {
            None => return err_box!("Not found data"),
            Some(v) => v,
        };

        let bytes = self.get_slice(len)?;
        let _zero = self.get_slice(1)?;
        Ok(FFIUtils::get_os_str(bytes))
    }

    /// Fetch a slice of all remaining bytes.
    pub(crate) fn get_all(&mut self) -> FuseResult<&'a [u8]> {
        let bytes = self.buf;
        self.buf = &[];
        Ok(bytes)
    }

    /// Get exactly the specified number of bytes (for FUSE_SETXATTR value)
    pub(crate) fn get_bytes(&mut self, size: usize) -> FuseResult<&'a [u8]> {
        if size > self.buf.len() {
            return err_box!("Not enough data for specified size");
        }
        self.get_slice(size)
    }

    pub(crate) fn ensure_empty(&self) -> FuseResult<()> {
        if self.buf.is_empty() {
            Ok(())
        } else {
            err_box!("Unexpected trailing request data: {} bytes", self.buf.len())
        }
    }
}

#[cfg(test)]
pub mod tests {
    use crate::raw::fuse_abi::fuse_in_header;
    use crate::session::fuse_decoder::FuseDecoder;
    use curvine_core_error::CommonResult;
    use std::mem::{align_of, size_of};

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

    #[test]
    fn ensure_empty_rejects_remaining_bytes() {
        let mut decoder = FuseDecoder::new(&DATA);
        decoder.get_slice(DATA.len() - 1).unwrap();

        let err = decoder.ensure_empty().unwrap_err();
        assert!(err.to_string().contains("1 bytes"));
    }

    #[test]
    fn rejects_unaligned_fuse_header() {
        let size = size_of::<fuse_in_header>();
        let align = align_of::<fuse_in_header>();
        assert!(align > 1, "test requires an over-aligned struct");

        let backing = vec![0u8; size + align];
        let base = backing.as_ptr() as usize;
        let offset = (1..align)
            .find(|o| !(base + o).is_multiple_of(align))
            .unwrap();
        let bytes = &backing[offset..offset + size];

        let err = FuseDecoder::parse::<fuse_in_header>(bytes).unwrap_err();
        assert!(err.to_string().contains("Unaligned"));
    }

    #[test]
    fn accepts_aligned_fuse_header() {
        let header = fuse_in_header::default();
        let bytes = unsafe {
            std::slice::from_raw_parts(
                (&header as *const fuse_in_header).cast::<u8>(),
                size_of::<fuse_in_header>(),
            )
        };
        assert!(FuseDecoder::parse::<fuse_in_header>(bytes).is_ok());
    }

    #[repr(C)]
    #[derive(Clone, Copy, Debug)]
    struct TestZst;

    // SAFETY: test-only marker type; a ZST carries no bit patterns.
    unsafe impl crate::raw::fuse_abi::FuseAbiPod for TestZst {}

    #[test]
    fn rejects_zero_sized_abi_type() {
        let err = FuseDecoder::parse::<TestZst>(&[]).unwrap_err();
        assert!(err.to_string().contains("Zero-sized"));

        let mut decoder = FuseDecoder::new(&[]);
        let err = decoder.get_struct_vec::<TestZst>(1).unwrap_err();
        assert!(err.to_string().contains("Zero-sized"));

        let mut decoder = FuseDecoder::new(&[]);
        assert!(decoder
            .get_struct_vec::<TestZst>(0)
            .expect("zero count is a no-op")
            .is_empty());
    }

    #[test]
    fn get_struct_vec_rejects_short_input() {
        let mut decoder = FuseDecoder::new(&[]);
        let err = decoder.get_struct_vec::<fuse_in_header>(1).unwrap_err();
        assert!(err.to_string().contains("Not enough data"));
    }
}
