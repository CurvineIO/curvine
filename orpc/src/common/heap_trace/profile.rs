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

use crate::CommonResult;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

const PROFILE_FORMAT: &str = "pprof";

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct HeapTraceProfile {
    pub format: String,
    pub payload: Vec<u8>,
}

impl HeapTraceProfile {
    pub fn new(payload: Vec<u8>) -> Self {
        Self {
            format: PROFILE_FORMAT.to_string(),
            payload,
        }
    }
}

pub fn read_profile(raw_profile_path: &Path) -> CommonResult<HeapTraceProfile> {
    let payload = fs::read(raw_profile_path)?;
    Ok(HeapTraceProfile::new(payload))
}

#[cfg(feature = "heap-trace")]
pub fn dump_profile(raw_profile_path: &Path) -> CommonResult<()> {
    use std::ffi::CString;
    use tikv_jemalloc_ctl::profiling::prof;
    use tikv_jemalloc_ctl::{epoch, raw};

    static PROF_DUMP: &[u8] = b"prof.dump\0";

    epoch::advance().map_err(|err| err.to_string())?;

    if !prof::read().map_err(|err| err.to_string())? {
        return Err("jemalloc heap profiling is disabled".into());
    }

    let dump_path = CString::new(raw_profile_path.to_string_lossy().into_owned())
        .map_err(|err| err.to_string())?;
    unsafe {
        raw::write(PROF_DUMP, dump_path.as_ptr()).map_err(|err| err.to_string())?;
    }
    Ok(())
}

#[cfg(not(feature = "heap-trace"))]
pub fn dump_profile(_raw_profile_path: &Path) -> CommonResult<()> {
    Ok(())
}
