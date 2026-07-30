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

pub mod java_abi;

mod java_utils;
pub use self::java_utils::JavaUtils;

mod java_filesystem;
pub use self::java_filesystem::JavaFilesystem;

pub const SUCCESS: i64 = 0;

// If it fails, return the error code.
#[macro_export]
macro_rules! java_err {
    ($env:expr, $expr:expr) => {{
        match $expr {
            Err(e) => {
                // The return value of java abi is always i64.
                $crate::java::JavaUtils::throw(&mut $env, &e);
                return e.libc_kind();
            }

            Ok(res) => res,
        }
    }};

    ($env:expr, $expr:expr, $final:expr) => {{
        match $expr {
            Err(e) => {
                $final;
                $crate::java::JavaUtils::throw(&mut $env, &e);
                return e.libc_kind();
            }

            Ok(res) => {
                $final;
                res
            }
        }
    }};
}

// If it fails, return null.
#[macro_export]
macro_rules! java_err2 {
    ($env:expr, $expr:expr) => {{
        match $expr {
            Err(e) => {
                $crate::java::JavaUtils::throw(&mut $env, &e);
                return jni::objects::JObject::null().into_raw();
            }

            Ok(res) => res,
        }
    }};
}
