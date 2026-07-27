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

use crate::error::ErrorEncoder;
use bytes::BytesMut;
use serde::Serialize;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Deref;

/// Encapsulated error data provides more error information for troubleshooting.
/// Currently, there are three mainstream rust error libraries:
/// 1. anyhow, any type error, simple to use, the error type will be lost.
/// 2. thiserror, you can use enumeration to customize error types, and you will not lose error types.
/// 3. sanfu, similar to thisError, can convert errors through the context method.
///
/// The above three libraries have unified defects:
/// 1. The rust error cannot be serialized. In the cs mode, the client service may not be able to obtain the error type.
/// 2. The error is missing context, otherwise the location of the error will increase the problem's troubleshooting.
///
/// Based on solving these problems, a set of error handling frameworks is designed, and its use will be more complicated.
pub struct ErrorImpl<E, D = ()> {
    // Original error type
    pub source: E,

    // The wrong online article currently records the wrong code location.
    pub ctx: Vec<String>,

    // Some errors may require carrying data to facilitate the caller to proceed with the next step of processing.
    pub data: Option<D>,
}

impl<E, D> ErrorImpl<E, D>
where
    E: Error,
{
    pub fn new(source: E, data: Option<D>) -> Self {
        Self {
            source,
            ctx: vec![],
            data,
        }
    }

    pub fn with_source(source: E) -> Self {
        Self::new(source, None)
    }

    pub fn with_data(source: E, data: D) -> Self {
        Self::new(source, Some(data))
    }

    pub fn ctx(mut self, ctx: impl Into<String>) -> Self {
        self.ctx.push(ctx.into());
        self
    }
}

impl<E, D> Display for ErrorImpl<E, D>
where
    E: Error,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.source, self.ctx.join("\n"))
    }
}

impl<E, D> Debug for ErrorImpl<E, D>
where
    E: Error,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]{}", self.ctx.join("\n"), self.source)
    }
}

// Serialize any error type.
// Most Errors cannot be serialized, and all Errors will become strings and serialized.
impl<E, D> ErrorImpl<E, D>
where
    E: Error,
    D: Serialize,
{
    pub fn encode<K: Into<i32>>(&self, kind: K) -> BytesMut {
        ErrorEncoder::encode(kind.into(), self)
    }
}

impl<E, D> Deref for ErrorImpl<E, D> {
    type Target = E;

    fn deref(&self) -> &Self::Target {
        &self.source
    }
}
