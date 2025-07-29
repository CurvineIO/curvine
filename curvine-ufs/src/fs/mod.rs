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

pub mod aws_utils;
pub mod buffer_transfer;
pub mod channel_transfer;
pub mod client;
pub mod factory;
pub mod filesystem;
mod opendal_filesystem;
pub mod s3;
pub mod s3_conf;
mod s3_filesystem;
pub mod ufs_context;

pub use crate::fs::buffer_transfer::{AsyncChunkReader, AsyncChunkWriter, ProgressCallback};
pub use client::UfsClient;
pub use orpc::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender};
