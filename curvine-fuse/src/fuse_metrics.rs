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

use once_cell::sync::OnceCell;

use orpc::common::{Gauge, Metrics as m};
use orpc::CommonResult;

static FUSE_METRICS: OnceCell<FuseMetrics> = OnceCell::new();

pub struct FuseMetrics {
    pub inode_num: Gauge,
    pub file_handle_num: Gauge,
    pub dir_handle_num: Gauge,
}

impl FuseMetrics {
    pub fn ensure_init() -> CommonResult<()> {
        FUSE_METRICS.get_or_try_init(Self::new)?;
        Ok(())
    }

    pub fn get() -> &'static Self {
        FUSE_METRICS
            .get()
            .expect("FuseMetrics not initialized; call ensure_init from CurvineFileSystem::new")
    }

    fn new() -> CommonResult<Self> {
        Ok(Self {
            inode_num: m::new_gauge("inode_num", "FUSE inode count in dcache")?,
            file_handle_num: m::new_gauge("file_handle_num", "FUSE open file handle count")?,
            dir_handle_num: m::new_gauge("dir_handle_num", "FUSE open directory handle count")?,
        })
    }
}
