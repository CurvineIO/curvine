//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::ops::{Deref, DerefMut};

use bytes::BytesMut;

/// Reusable [`BytesMut`] wrapper for per-frame reads and protocol encoding on the write side.
///
/// [`take_exact`](Self::take_exact) may retain spare capacity after [`BytesMut::split_to`]; when
/// the tail’s capacity exceeds `buf_size * MAX_FACTOR`, the tail is discarded to cap growth.
///
/// # `buf_size` and zero
///
/// `buf_size` is the baseline used for that shrink threshold (and e.g. [`RpcFrame::into_tokio_frame`](crate::handler::RpcFrame::into_tokio_frame) capacity).
/// If `buf_size` is `0`, it is replaced with [`DEFAULT_BUF_SIZE`] (8 KiB) so shrink logic and
/// downstream sizing behave sensibly instead of treating every non‑empty tail as over‑capacity.
pub struct FrameBuf {
    inner: BytesMut,
    buf_size: usize,
}

impl FrameBuf {
    pub const MAX_FACTOR: usize = 4;

    /// Default effective `buf_size` when the caller passes `0` ([`new`](Self::new)).
    pub const DEFAULT_BUF_SIZE: usize = 8 * 1024;

    /// Creates an empty buffer. If `buf_size` is `0`, uses [`DEFAULT_BUF_SIZE`] (8 KiB).
    pub fn new(buf_size: usize) -> Self {
        let buf_size = if buf_size == 0 {
            Self::DEFAULT_BUF_SIZE
        } else {
            buf_size
        };
        Self {
            inner: BytesMut::new(),
            buf_size,
        }
    }

    /// Effective baseline size (after applying the `0` → 8 KiB default in [`new`](Self::new)).
    pub fn buf_size(&self) -> usize {
        self.buf_size
    }

    pub fn take_exact(&mut self, len: usize) -> BytesMut {
        self.inner.reserve(len);
        unsafe {
            self.inner.set_len(len);
        }
        let buf = self.inner.split_to(len);

        if self.inner.capacity() > self.buf_size.saturating_mul(Self::MAX_FACTOR) {
            self.inner = BytesMut::new();
        }

        buf
    }

    pub fn into_inner(self) -> BytesMut {
        self.inner
    }
}

impl Clone for FrameBuf {
    fn clone(&self) -> Self {
        Self {
            inner: BytesMut::new(),
            buf_size: self.buf_size,
        }
    }
}

impl Deref for FrameBuf {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for FrameBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
