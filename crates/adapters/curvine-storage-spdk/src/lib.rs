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

pub use orpc::io::{NvmeTarget, SpdkConf};

#[cfg(feature = "spdk")]
pub mod spdk_bdev;
#[cfg(feature = "spdk")]
pub mod spdk_env;
#[cfg(feature = "spdk")]
pub mod spdk_ffi;
#[cfg(feature = "spdk")]
pub mod spdk_poller;

#[cfg(all(test, feature = "spdk"))]
mod spdk_bdev_test;

#[cfg(feature = "spdk")]
pub use spdk_bdev::SpdkBdev;
#[cfg(feature = "spdk")]
pub use spdk_env::{BdevInfo, SpdkEnv, SpdkEnvState};
#[cfg(feature = "spdk")]
pub use spdk_poller::{CtrlHandle, PollerConfig};
