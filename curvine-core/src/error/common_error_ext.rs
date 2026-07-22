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

use crate::error::ErrorExt;
use crate::io::IOError;
use crate::CommonError;
use bytes::{BufMut, BytesMut};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

pub struct CommonErrorExt(CommonError);

impl Debug for CommonErrorExt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Display for CommonErrorExt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for CommonErrorExt {}

impl ErrorExt for CommonErrorExt {
    fn ctx(self, ctx: impl Into<String>) -> Self {
        let error = format!("{}: {}", self.0, ctx.into());
        Self(error.into())
    }

    fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_slice(self.0.to_string().as_bytes());
        buf
    }

    fn decode(byte: BytesMut) -> Self {
        let error = String::from_utf8_lossy(&byte).to_string();
        Self(error.into())
    }
}

impl From<String> for CommonErrorExt {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl From<CommonError> for CommonErrorExt {
    fn from(value: CommonError) -> Self {
        Self(value)
    }
}

impl From<IOError> for CommonErrorExt {
    fn from(value: IOError) -> Self {
        Self(value.into())
    }
}
