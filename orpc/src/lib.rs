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

use crate::error::CommonErrorExt;

pub mod client;
pub mod common;
pub mod error;
pub mod handler;
pub mod io;
pub mod macros;
pub mod message;
pub mod runtime;
pub mod server;
pub mod sync;
pub mod sys;
pub mod test;

pub type CommonError = Box<dyn std::error::Error + Send + Sync>;

pub type CommonResult<T> = Result<T, CommonError>;

pub type CommonResultExt<T> = Result<T, CommonErrorExt>;
