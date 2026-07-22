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

use crate::CommonError;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

// String error type.
pub struct StringError(String);

impl StringError {
    pub fn new<T: Error>(error: T) -> Self {
        Self(error.to_string())
    }
}

impl Display for StringError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Debug for StringError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for StringError {}

impl From<String> for StringError {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for StringError {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<std::io::Error> for StringError {
    fn from(value: std::io::Error) -> Self {
        Self(value.to_string())
    }
}

impl From<CommonError> for StringError {
    fn from(value: CommonError) -> Self {
        Self(value.to_string())
    }
}
