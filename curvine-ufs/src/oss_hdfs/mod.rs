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

mod callback_ctx;
mod ffi;
mod oss_hdfs_filesystem;
mod oss_hdfs_reader;
mod oss_hdfs_writer;

use curvine_common::error::FsError;
use curvine_common::FsResult;
use orpc::error::ErrorExt;

use self::ffi::{jindo_last_error, JindoStatus};

pub use self::oss_hdfs_filesystem::OssHdfsFileSystem;
pub use self::oss_hdfs_reader::OssHdfsReader;
pub use self::oss_hdfs_writer::OssHdfsWriter;

pub const SCHEME: &str = "oss";

pub(crate) fn jindo_error(status: JindoStatus, operation: &str, err: Option<String>) -> FsError {
    let detail = err.unwrap_or_else(jindo_last_error);
    match status {
        JindoStatus::FileNotFound => FsError::file_not_found(detail).ctx(operation),
        _ => FsError::common(format!("{}: {}", operation, detail)),
    }
}

pub(crate) fn check_jindo_status(
    status: JindoStatus,
    operation: &str,
    err: Option<String>,
) -> FsResult<()> {
    match status {
        JindoStatus::Ok => Ok(()),
        _ => Err(jindo_error(status, operation, err)),
    }
}
