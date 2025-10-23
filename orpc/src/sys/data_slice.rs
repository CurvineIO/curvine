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

#![allow(unused, clippy::should_implement_trait)]

use crate::handler::RpcFrame;
use crate::io::{IOResult, LocalFile};
use crate::sys;
use crate::sys::DataSlice::{Buffer, Bytes, Empty, IOSlice, MemSlice};
use crate::sys::{RawIO, RawIOSlice, RawVec};
use crate::CommonResult;
use bytes::{Buf, Bytes as TBytes, BytesMut};
use std::fs::File;

/// A data fragment, which represents a portion of the data on top of an IO stream.
/// In many cases, in order to reduce memory copying, data reading and writing will not occur when creating data fragments.
/// The read and write operation of the data only occurs when it is necessary to read and write data to an exception io stream, which is usually done directly by the kernel.
///
/// Based on the issue of rust ownership, passing native io objects is quite complicated, and the unsafe method is used here:
/// 1. For Linux systems, the fd is saved, that is, the file descriptor.
/// 2. For other systems, bytes, that is, data byte streams, there is no possibility of io optimization.
#[derive(Debug)]
pub enum DataSlice {
    Empty,

    Buffer(BytesMut),

    // io data slicing
    IOSlice(RawIOSlice),

    // Memory data slicing
    MemSlice(RawVec),

    Bytes(TBytes),
}

impl DataSlice {
    pub fn empty() -> Self {
        Empty
    }

    pub fn mem_slice(slice: &[u8]) -> Self {
        MemSlice(RawVec::from_slice(slice))
    }

    pub fn buffer(buffer: BytesMut) -> Self {
        Buffer(buffer)
    }

    pub fn bytes(bytes: TBytes) -> Self {
        Bytes(bytes)
    }

    pub fn io_slice(raw: RawIO, off: Option<i64>, len: usize) -> Self {
        IOSlice(RawIOSlice::new(raw, off, len))
    }

    // Get the length of the data segment.
    pub fn len(&self) -> usize {
        match self {
            Empty => 0,
            Buffer(s) => s.len(),
            IOSlice(s) => s.len(),
            MemSlice(s) => s.len(),
            Bytes(s) => s.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // Create a network data fragment.
    pub async fn from_frame(frame: &mut RpcFrame, enable_splice: bool, len: i32) -> IOResult<Self> {
        if len <= 0 {
            return Ok(Empty);
        }

        #[cfg(not(target_os = "linux"))]
        {
            let buf = frame.read_full(len).await?;
            Ok(Buffer(buf))
        }

        #[cfg(target_os = "linux")]
        {
            if enable_splice {
                let fd = sys::get_raw_io(frame)?;
                Ok(IOSlice(RawIOSlice::new(fd, None, len as usize)))
            } else {
                let buf = frame.read_full(len).await?;
                Ok(Buffer(buf))
            }
        }
    }

    // Create a file data fragment.
    // Usually created in business threads, it is a synchronization function.
    pub fn from_file(
        file: &mut LocalFile,
        enable_send_file: bool,
        off: Option<i64>,
        len: i32,
    ) -> CommonResult<Self> {
        if len <= 0 {
            return Ok(Empty);
        }

        #[cfg(not(target_os = "linux"))]
        {
            let buf = file.read_full(off, len as usize)?;
            Ok(Buffer(buf))
        }

        #[cfg(target_os = "linux")]
        {
            if enable_send_file {
                let fd = sys::get_raw_io(file)?;
                Ok(IOSlice(RawIOSlice::new(fd, off, len as usize)))
            } else {
                let buf = file.read_full(off, len as usize)?;
                Ok(Buffer(buf))
            }
        }
    }

    pub fn to_error_msg(&self) -> String {
        let slice: &[u8] = match self {
            Buffer(buf) => buf,
            Bytes(bytes) => bytes,
            _ => b"",
        };
        String::from_utf8_lossy(slice).to_string()
    }

    pub fn from_str<T: AsRef<str>>(str: T) -> Self {
        let bytes = BytesMut::from(str.as_ref());
        Buffer(bytes)
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Empty => &[],
            Buffer(s) => s,
            IOSlice(s) => s.as_slice(),
            MemSlice(s) => s.as_slice(),
            Bytes(s) => s,
        }
    }

    pub fn split_to(&mut self, at: usize) -> DataSlice {
        match self {
            Empty => Empty,
            Buffer(s) => Buffer(s.split_to(at)),
            IOSlice(s) => IOSlice(s.split_to()),
            MemSlice(s) => MemSlice(s.split_to(at)),
            Bytes(s) => Bytes(s.split_to(at)),
        }
    }

    pub fn split_off(&mut self, at: usize) -> DataSlice {
        match self {
            Empty => Empty,
            Buffer(s) => Buffer(s.split_off(at)),
            IOSlice(s) => IOSlice(s.split_off()),
            MemSlice(s) => MemSlice(s.split_off(at)),
            Bytes(s) => Bytes(s.split_off(at)),
        }
    }

    pub fn copy_to_slice(&mut self, dst: &mut [u8]) {
        match self {
            Empty => (),
            Buffer(s) => s.copy_to_slice(dst),
            IOSlice(s) => s.copy_to_slice(),
            MemSlice(s) => s.copy_to_slice(dst),
            Bytes(s) => s.copy_to_slice(dst),
        }
    }

    pub fn advance(&mut self, cnt: usize) {
        match self {
            Empty => (),
            Buffer(s) => s.advance(cnt),
            IOSlice(s) => s.advance(),
            MemSlice(s) => s.advance(cnt),
            Bytes(s) => s.advance(cnt),
        }
    }

    pub fn clear(&mut self) {
        match self {
            Empty => (),
            Buffer(s) => s.clear(),
            IOSlice(s) => s.clear(),
            MemSlice(s) => s.clear(),
            Bytes(s) => s.clear(),
        }
    }

    pub fn freeze(self) -> Self {
        match self {
            Buffer(s) => Bytes(s.freeze()),
            _ => self,
        }
    }

    pub fn as_ptr(&self) -> *const u8 {
        match self {
            Empty => std::ptr::null() as *const _,
            Buffer(s) => s.as_ptr(),
            IOSlice(s) => panic!("Not support IOSlice"),
            MemSlice(s) => s.as_ptr(),
            Bytes(s) => s.as_ptr(),
        }
    }
}

impl Clone for DataSlice {
    #[inline]
    fn clone(&self) -> Self {
        match self {
            Empty => Empty,
            Bytes(v) => Bytes(v.clone()),
            MemSlice(v) => MemSlice(v.clone()),
            IOSlice(v) => IOSlice(v.clone()),
            // Buffer requires memory copy, which has additional overhead and does not allow clone
            Buffer(_) => panic!("Not support"),
        }
    }
}

impl From<&str> for DataSlice {
    fn from(value: &str) -> Self {
        Buffer(BytesMut::from(value.as_bytes()))
    }
}

impl From<String> for DataSlice {
    fn from(value: String) -> Self {
        let mut bytes = BytesMut::new();
        bytes.extend_from_slice(value.as_bytes());
        Buffer(bytes)
    }
}
