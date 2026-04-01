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
    use tikv_jemalloc_ctl::{epoch, raw};

    static PROF_DUMP: &[u8] = b"prof.dump\0";

    epoch::advance().map_err(|err| format!("epoch advance failed: {}", err))?;

    // Debug: check opt.prof and prof.active states
    let (opt_prof, prof_active): (bool, bool) = unsafe {
        let opt_prof = raw::read(b"opt.prof\0")
            .map_err(|err| format!("read opt.prof failed: {}", err))?;
        let prof_active = raw::read(b"prof.active\0")
            .map_err(|err| format!("read prof.active failed: {}", err))?;
        (opt_prof, prof_active)
    };

    log::debug!("jemalloc profiling state: opt.prof={}, prof.active={}", opt_prof, prof_active);

    if !opt_prof {
        return Err("jemalloc opt.prof is false - profiling was not enabled at startup. Check MALLOC_CONF=prof:true,prof_active:true".into());
    }

    if !prof_active {
        return Err(format!(
            "jemalloc prof.active is false (opt.prof={}). Profiling may have been disabled or MALLOC_CONF was not set before process start.",
            opt_prof
        ).into());
    }

    let dump_path = CString::new(raw_profile_path.to_string_lossy().into_owned())
        .map_err(|err| err.to_string())?;
    unsafe {
        raw::write(PROF_DUMP, dump_path.as_ptr()).map_err(|err| err.to_string())?;
    }
    Ok(())
}

#[cfg(not(feature = "heap-trace"))]
pub fn dump_profile(raw_profile_path: &Path) -> CommonResult<()> {
    if let Some(parent) = raw_profile_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(raw_profile_path, [])?;
    Ok(())
}

#[cfg(all(test, not(feature = "heap-trace")))]
mod tests {
    use super::{dump_profile, read_profile};
    use tempfile::TempDir;

    #[test]
    fn dump_profile_stub_creates_readable_placeholder_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("heap.raw.pb.gz");

        dump_profile(&path).unwrap();
        let profile = read_profile(&path).unwrap();

        assert_eq!(profile.format, "pprof");
        assert!(profile.payload.is_empty());
    }
}
