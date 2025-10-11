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

use std::ops::Deref;
use crate::sys::RawVec;
use crate::ucp::bindings::ucp_rkey_buffer_release;

// 可以访问远程内存区域（包含access key）
pub struct RKeyBuffer {
    inner: RawVec
}

impl RKeyBuffer {
    pub fn new(inner: RawVec) -> Self {
        Self {
            inner,
        }
    }
}

impl Deref for RKeyBuffer {
    type Target = RawVec;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Drop for RKeyBuffer {
    fn drop(&mut self) {
        unsafe {
            ucp_rkey_buffer_release(self.inner.as_ptr() as _)
        }
    }
}