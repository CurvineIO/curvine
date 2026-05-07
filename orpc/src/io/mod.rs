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

pub mod net;
pub mod retry;

mod local_file;
pub use self::local_file::LocalFile;

pub mod io_error;
pub use self::io_error::IOError;

pub type IOResult<T> = Result<T, IOError>;

pub mod block_io;
pub use self::block_io::{BlockDevice, BlockIO};

#[cfg(feature = "spdk")]
pub mod spdk_env;

#[cfg(feature = "spdk")]
pub use spdk_env::{BdevInfo, NvmeTarget, SpdkConf};

#[cfg(not(feature = "spdk"))]
pub mod spdk_stub;

#[cfg(not(feature = "spdk"))]
pub use spdk_stub::{BdevInfo, NvmeTarget, SpdkConf};

#[cfg(all(test, feature = "spdk"))]
mod spdk_bdev_test;

#[cfg(feature = "spdk")]
pub mod spdk_ffi;

#[cfg(feature = "spdk")]
pub mod spdk_bdev;
#[cfg(feature = "spdk")]
pub use spdk_bdev::SpdkBdev;

#[cfg(feature = "spdk")]
pub mod spdk_poller;

#[cfg(not(feature = "spdk"))]
pub mod spdk_poller_stub;
