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

pub mod fs;

// Native S3 implementation
#[cfg(feature = "s3")]
pub mod s3;

// OpenDAL implementations
#[cfg(feature = "opendal")]
pub mod opendal;

mod ufs_utils;
pub use self::ufs_utils::UfsUtils;

pub const FOLDER_SUFFIX: &str = "/";

#[macro_export]
macro_rules! err_ufs {
    ($e:expr) => ({
        Err(orpc::err_msg!($e).into())
    });

    ($f:tt, $($arg:expr),+) => ({
        orpc::err_box!(format!($f, $($arg),+))
    });
}
