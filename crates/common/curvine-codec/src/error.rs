// Copyright 2026 OPPO.
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

use std::fmt;

#[derive(Debug)]
pub enum CodecErrorKind {
    Encode(String),
    Decode(String),
}

impl fmt::Display for CodecErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecErrorKind::Encode(msg) => write!(f, "codec encode error: {msg}"),
            CodecErrorKind::Decode(msg) => write!(f, "codec decode error: {msg}"),
        }
    }
}

#[derive(Debug)]
pub struct CodecError(Box<CodecErrorKind>);

impl CodecError {
    pub fn encode(msg: impl Into<String>) -> Self {
        Self(Box::new(CodecErrorKind::Encode(msg.into())))
    }

    pub fn decode(msg: impl Into<String>) -> Self {
        Self(Box::new(CodecErrorKind::Decode(msg.into())))
    }

    pub fn kind(&self) -> &CodecErrorKind {
        &self.0
    }

    #[cold]
    #[inline(never)]
    pub fn invalid_tag(type_name: &'static str, raw: i64) -> Self {
        Self::decode(format!("invalid {} tag {}", type_name, raw))
    }

    #[cold]
    #[inline(never)]
    pub fn invalid_bits(type_name: &'static str, bits: i64) -> Self {
        Self::decode(format!("invalid {} bits {:#x}", type_name, bits))
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for CodecError {}
