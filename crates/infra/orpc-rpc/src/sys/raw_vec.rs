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

use crate::sys::RawPtr;
use bytes::BytesMut;
use std::fmt::{Debug, Formatter};

/// and other languages use shared memory to communicate.
/// Usually, an external language passes a pointer memory block, wrapped in RawVec to read and write this memory in rust.
#[derive(Clone)]
pub struct RawVec {
    ptr: RawPtr<u8>,
    len: usize,
}

impl RawVec {
    pub fn new(ptr: *const u8, len: usize) -> Self {
        Self {
            ptr: RawPtr::from_raw(ptr),
            len,
        }
    }

    pub fn from_raw(ptr: *mut u8, len: usize) -> Self {
        Self::new(ptr, len)
    }

    pub fn from_slice(slice: &[u8]) -> Self {
        Self::new(slice.as_ptr(), slice.len())
    }

    pub fn from_bytes_mut(buf: &BytesMut) -> Self {
        Self::new(buf.as_ptr(), buf.capacity())
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_mut_ptr(), self.len) }
    }

    pub fn split_to(&mut self, at: usize) -> Self {
        assert!(
            at <= self.len,
            "split_to out of bounds: {:?} <= {:?}",
            at,
            self.len,
        );

        let res = RawVec::from_raw(self.ptr.as_mut_ptr(), at);
        self.ptr.add(at);
        self.len -= at;

        res
    }

    pub fn split_off(&mut self, at: usize) -> Self {
        assert!(
            at <= self.len,
            "split_off out of bounds: {:?} <= {:?}",
            at,
            self.len,
        );

        let other = unsafe { self.ptr.as_mut_ptr().add(at) };
        let res = RawVec::from_raw(other, self.len - at);
        self.len = at;

        res
    }

    pub fn copy_to_slice(&mut self, dst: &mut [u8]) {
        assert!(
            self.len >= dst.len(),
            "copy_to_slice out of bounds: the len is {} but advancing by {}",
            self.len,
            dst.len(),
        );

        let src = self.split_to(dst.len());
        dst.copy_from_slice(src.as_slice());
    }

    pub fn advance(&mut self, cnt: usize) {
        let _ = self.split_to(cnt);
    }

    pub fn clear(&mut self) {
        let _ = self.split_to(self.len);
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }
}

impl Debug for RawVec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ptr = {}, len = {}",
            self.ptr.as_mut_ptr() as usize,
            self.len
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::sys::RawVec;
    use bytes::BytesMut;

    #[test]
    fn split_to() {
        let vec = Vec::from("abc123");
        let mut raw_vec = RawVec::from_slice(&vec);

        let split = raw_vec.split_to(3);

        println!("split {}", String::from_utf8_lossy(split.as_slice()));
        println!("now {}", String::from_utf8_lossy(raw_vec.as_slice()));

        assert_eq!(split.as_slice(), Vec::from("abc"));
        assert_eq!(raw_vec.as_slice(), Vec::from("123"));
    }

    #[test]
    fn split_off() {
        let vec = Vec::from("abc123");
        let mut raw_vec = RawVec::from_slice(&vec);

        let split = raw_vec.split_off(3);

        println!("split {}", String::from_utf8_lossy(split.as_slice()));
        println!("now {}", String::from_utf8_lossy(raw_vec.as_slice()));

        assert_eq!(split.as_slice(), Vec::from("123"));
        assert_eq!(raw_vec.as_slice(), Vec::from("abc"));
    }

    #[test]
    fn copy_to_slice() {
        let vec = Vec::from("abc123");
        let mut raw_vec = RawVec::from_slice(&vec);

        let mut buf = BytesMut::zeroed(3);
        raw_vec.copy_to_slice(&mut buf);

        println!("buf {}", String::from_utf8_lossy(&buf));
        println!("now {}", String::from_utf8_lossy(raw_vec.as_slice()));

        assert_eq!(&buf[..], Vec::from("abc"));
        assert_eq!(raw_vec.as_slice(), Vec::from("123"));
    }

    #[test]
    fn advance() {
        let vec = Vec::from("abc123");
        let mut raw_vec = RawVec::from_slice(&vec);

        raw_vec.advance(3);
        assert_eq!(raw_vec.as_slice(), Vec::from("123"));
    }

    #[test]
    fn clear() {
        let vec = Vec::from("abc123");
        let mut raw_vec = RawVec::from_slice(&vec);

        raw_vec.clear();
        assert_eq!(raw_vec.len, 0);
    }
}
