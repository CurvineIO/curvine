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

use bytes::Bytes;
use std::fmt::{Debug, Formatter};
use std::ops::{Deref, DerefMut};

/// Buffer address alignment required for O_DIRECT I/O. The hardware/logical
/// block requirement is 512 bytes; 4096 is the safe page-granularity choice.
pub const DIRECT_IO_ALIGN: usize = 4096;

/// A heap buffer whose base address is aligned to [`DIRECT_IO_ALIGN`], suitable
/// as the DMA target of an O_DIRECT read.
///
/// Backed by an over-allocated `Vec<u8>`; the exposed region is the aligned
/// sub-range `[off, off + len)` inside the vector. Keeping ownership in a
/// `Vec` lets [`AlignedBuf::into_bytes`] convert to [`Bytes`] without copying:
/// `Bytes::from(Vec<u8>)` adopts the allocation and `slice` is a view.
pub struct AlignedBuf {
    inner: Vec<u8>,
    off: usize,
    len: usize,
}

impl AlignedBuf {
    /// Allocate `len` bytes with a [`DIRECT_IO_ALIGN`]-aligned base address.
    ///
    /// Storage is zero-initialized (`vec![0; n]` lowers to `alloc_zeroed`,
    /// which serves page-granular buffers from pre-zeroed pages), so every
    /// byte reachable through the safe accessors is initialized even before
    /// a read fills the window. Short-read cropping still happens at the
    /// consumer (`DirectReadPlan::crop_bounds`): zeroed padding must never be
    /// exposed as file data.
    pub fn alloc(len: usize) -> Self {
        if len == 0 {
            return Self {
                inner: Vec::new(),
                off: 0,
                len: 0,
            };
        }

        // Over-allocate by align - 1 so an aligned `len`-byte window always fits.
        let inner: Vec<u8> = vec![0u8; len + DIRECT_IO_ALIGN - 1];
        let off = inner.as_ptr().align_offset(DIRECT_IO_ALIGN);
        assert!(
            off != usize::MAX && off + len <= inner.len(),
            "{DIRECT_IO_ALIGN}-aligned window must fit the allocation"
        );

        Self { inner, off, len }
    }

    /// Number of exposed (aligned) bytes.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Alignment guarantee of the buffer base address.
    pub fn align(&self) -> usize {
        DIRECT_IO_ALIGN
    }

    /// The full aligned region as a mutable slice, to be filled by the caller
    /// (e.g. via pread).
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.inner[self.off..self.off + self.len]
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.inner[self.off..self.off + self.len]
    }

    /// Zero-copy conversion into [`Bytes`]: the underlying allocation is
    /// adopted by the returned handle, which is a view over the aligned region.
    ///
    /// Bytes the caller did not fill with a read are zeros (see
    /// [`AlignedBuf::alloc`]); the caller is responsible for cropping to the
    /// range a read actually covered.
    pub fn into_bytes(self) -> Bytes {
        let bytes = Bytes::from(self.inner);
        bytes.slice(self.off..self.off + self.len)
    }
}

impl Deref for AlignedBuf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for AlignedBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_bytes_mut()
    }
}

impl Debug for AlignedBuf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlignedBuf")
            .field("len", &self.len)
            .field("off", &self.off)
            .field("align", &DIRECT_IO_ALIGN)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{AlignedBuf, DIRECT_IO_ALIGN};

    #[test]
    fn alloc_base_address_is_4k_aligned() {
        for len in [1usize, 512, 4095, 4096, 4097, 10000, 1 << 20] {
            let buf = AlignedBuf::alloc(len);
            assert_eq!(buf.len(), len);
            assert_eq!(buf.align(), DIRECT_IO_ALIGN);
            assert_eq!(
                buf.as_slice().as_ptr() as usize % DIRECT_IO_ALIGN,
                0,
                "base address must be {DIRECT_IO_ALIGN}-aligned for len={len}"
            );
            assert_eq!(buf.as_slice().len(), len);
        }
    }

    #[test]
    fn zero_len_alloc_is_empty() {
        let buf = AlignedBuf::alloc(0);
        assert!(buf.is_empty());
        assert!(buf.as_slice().is_empty());
        assert!(buf.into_bytes().is_empty());
    }

    // The safe accessors (`as_slice`, `into_bytes`, Deref) are reachable
    // before any read fills the buffer, so the storage must be initialized
    // at allocation time.
    #[test]
    fn alloc_storage_is_zero_initialized() {
        for len in [1usize, 4096, 10000] {
            let buf = AlignedBuf::alloc(len);
            assert!(buf.as_slice().iter().all(|&b| b == 0), "len={len}");
            let bytes = buf.into_bytes();
            assert!(bytes.iter().all(|&b| b == 0), "len={len}");
        }
    }

    #[test]
    fn write_read_roundtrip_through_as_bytes_mut() {
        let mut buf = AlignedBuf::alloc(8192);
        for (i, b) in buf.as_bytes_mut().iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        assert_eq!(buf.as_slice()[0], 0);
        assert_eq!(buf.as_slice()[251], 0);
        assert_eq!(buf.as_slice()[250], 250);
        // Deref/DerefMut indexing.
        assert_eq!(buf[500], (500 % 251) as u8);
        buf[500] = 7;
        assert_eq!(buf.as_slice()[500], 7);
    }

    #[test]
    fn into_bytes_is_zero_copy_and_preserves_contents() {
        let mut buf = AlignedBuf::alloc(10000);
        for (i, b) in buf.as_bytes_mut().iter_mut().enumerate() {
            *b = (i % 253) as u8;
        }
        let expected: Vec<u8> = (0..10000usize).map(|i| (i % 253) as u8).collect();
        let ptr = buf.as_slice().as_ptr();

        let bytes = buf.into_bytes();
        assert_eq!(bytes.len(), 10000);
        assert_eq!(&bytes[..], &expected[..]);
        assert_eq!(
            bytes.as_ptr(),
            ptr,
            "into_bytes must not copy: base pointer must be unchanged"
        );
    }

    #[test]
    fn send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AlignedBuf>();
    }
}
