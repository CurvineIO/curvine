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

//! Hot-parent background scenario fixtures.
//!
//! Shape: `/bench/hot/parent` is the contended hot parent. A deep chain under
//! it is the foreground target, so foreground ancestor read locks and
//! background parent write locks contend on the same subtree.
//!
//! All path helpers are called before the barrier; the timed region holds one
//! `MasterFilesystem` call only.

use curvine_error::FsResult;
use curvine_master::master::fs::MasterFilesystem;

const HOT_ROOT: &str = "/bench/hot";
const HOT_PROBE: &str = "probe";

pub fn hot_parent_dir() -> String {
    format!("{HOT_ROOT}/parent")
}

pub fn hot_deep_dir() -> String {
    let mut path = hot_parent_dir();
    path.push_str("/deep");
    for depth in 0..crate::background::HOT_PARENT_DEPTH {
        path.push_str(&format!("/d{depth}"));
    }
    path
}

pub fn prepare_hot_parent(fs: &MasterFilesystem) -> FsResult<()> {
    let deep = hot_deep_dir();
    fs.mkdir(&deep, true)?;
    fs.create(format!("{deep}/{HOT_PROBE}"), false)?;
    Ok(())
}

/// One background cycle on the hot parent: create then delete. Both calls are
/// timed together as one background operation, because the background is an
/// offered load, not a per-call measurement. `path` is built by the caller
/// outside the timed region.
pub fn hot_parent_cycle(fs: &MasterFilesystem, path: &str) -> FsResult<()> {
    fs.create(path, false)?;
    fs.delete(path, false)?;
    Ok(())
}

pub fn hot_parent_status(fs: &MasterFilesystem, probe: &str) -> FsResult<()> {
    fs.file_status(probe)?;
    Ok(())
}

pub fn hot_parent_create(fs: &MasterFilesystem, path: &str) -> FsResult<()> {
    fs.create(path, false)?;
    Ok(())
}
