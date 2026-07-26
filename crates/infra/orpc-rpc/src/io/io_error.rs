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

use crate::error::{ErrorExt, ErrorImpl};
use crate::CommonError;
use bytes::BytesMut;
use std::error::Error;
use std::ffi::NulError;
use std::io;
use std::io::ErrorKind;
use std::num::ParseIntError;
use std::ops::Deref;
use std::str::Utf8Error;
use thiserror::Error as ThisError;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError;
use tokio::task::JoinError;
use tokio::time::error::Elapsed;

// Error returned by the operating system.
#[derive(Debug, ThisError)]
#[error("{inner}")]
pub struct IOError {
    pub(crate) inner: ErrorImpl<io::Error, ()>,
}

impl IOError {
    pub fn new(error: io::Error) -> Self {
        Self {
            inner: ErrorImpl::with_source(error),
        }
    }

    pub fn conn_reset() -> Self {
        let error = io::Error::new(ErrorKind::ConnectionReset, "Connection closed");
        Self::new(error)
    }

    pub fn with_msg(error: io::Error, msg: impl AsRef<str>) -> Self {
        let msg = format!("{}: {}", msg.as_ref(), error);
        let new_error = io::Error::new(error.kind(), msg);
        Self::new(new_error)
    }

    pub fn with_opt_msg(error: Option<Self>, msg: impl AsRef<str>) -> Self {
        match error {
            None => Self::create("No errors found"),
            Some(e) => Self::with_msg(e.into_raw(), msg),
        }
    }

    pub fn with_ctx(error: io::Error, ctx: impl Into<String>) -> Self {
        let inner = ErrorImpl::with_source(error).ctx(ctx);
        Self { inner }
    }

    pub fn create<E>(error: E) -> Self
    where
        E: Into<Box<dyn Error + Send + Sync>>,
    {
        Self::new(io::Error::other(error))
    }

    pub fn create_ctx<E>(error: E, ctx: impl Into<String>) -> Self
    where
        E: Into<Box<dyn Error + Send + Sync>>,
    {
        let error = io::Error::other(error);
        Self::with_ctx(error, ctx)
    }

    pub fn is_would_block(&self) -> bool {
        self.kind() == ErrorKind::WouldBlock
    }

    // To determine whether this error needs to be tried again.
    pub fn need_retry(err: &io::Error) -> bool {
        err.kind() == ErrorKind::WouldBlock
    }

    pub fn into_raw(self) -> io::Error {
        self.inner.source
    }

    pub fn raw_error(&self) -> &io::Error {
        &self.inner.source
    }
}

impl Deref for IOError {
    type Target = io::Error;

    fn deref(&self) -> &Self::Target {
        &self.inner.source
    }
}

impl ErrorExt for IOError {
    fn ctx(self, ctx: impl Into<String>) -> Self {
        let inner = self.inner.ctx(ctx);
        Self { inner }
    }

    fn encode(&self) -> BytesMut {
        todo!()
    }

    fn decode(_byte: BytesMut) -> Self {
        todo!()
    }
}

impl From<io::Error> for IOError {
    fn from(err: io::Error) -> Self {
        Self::new(err)
    }
}

impl From<&str> for IOError {
    fn from(value: &str) -> Self {
        Self::create(value)
    }
}

impl From<String> for IOError {
    fn from(value: String) -> Self {
        Self::create(value)
    }
}

impl From<CommonError> for IOError {
    fn from(value: CommonError) -> Self {
        Self::create(value)
    }
}

impl From<Elapsed> for IOError {
    fn from(value: Elapsed) -> Self {
        let error = io::Error::new(ErrorKind::TimedOut, value);
        Self::new(error)
    }
}

impl From<ParseIntError> for IOError {
    fn from(value: ParseIntError) -> Self {
        Self::create(value)
    }
}

impl<T> From<SendError<T>> for IOError {
    fn from(value: SendError<T>) -> Self {
        Self::create(value.to_string())
    }
}

impl From<RecvError> for IOError {
    fn from(value: RecvError) -> Self {
        Self::create(value)
    }
}

impl From<JoinError> for IOError {
    fn from(value: JoinError) -> Self {
        Self::create(value)
    }
}

impl From<Utf8Error> for IOError {
    fn from(value: Utf8Error) -> Self {
        Self::create(value)
    }
}

impl From<NulError> for IOError {
    fn from(value: NulError) -> Self {
        Self::create(value)
    }
}

impl<T> From<std::sync::mpsc::SendError<T>> for IOError {
    fn from(value: std::sync::mpsc::SendError<T>) -> Self {
        Self::create(value.to_string())
    }
}

impl From<toml::de::Error> for IOError {
    fn from(value: toml::de::Error) -> Self {
        Self::create(value)
    }
}

impl From<toml::ser::Error> for IOError {
    fn from(value: toml::ser::Error) -> Self {
        Self::create(value)
    }
}
