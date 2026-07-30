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

use crate::sys::{self, SysResult};
use log::warn;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct FsStats {
    path: PathBuf,
    device_id: u64,
}

impl FsStats {
    pub fn new<P: AsRef<str>>(path: P) -> Self {
        let path = PathBuf::from(path.as_ref());
        let device_id = sys::get_device_id(path.as_path());
        Self { path, device_id }
    }

    pub fn exists(&self) -> bool {
        self.path.exists()
    }

    pub fn is_dir(&self) -> bool {
        self.path.is_dir()
    }

    pub fn is_file(&self) -> bool {
        self.path.is_file()
    }

    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    pub fn device_id(&self) -> u64 {
        self.device_id
    }

    pub fn len(&self) -> u64 {
        match self.path.metadata() {
            Err(_) => 0,
            Ok(m) => m.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn total_space(&self) -> u64 {
        Self::space_or_zero(fs2::total_space(self.path()))
    }

    pub fn free_space(&self) -> u64 {
        Self::space_or_zero(fs2::free_space(self.path()))
    }

    pub fn available_space(&self) -> u64 {
        Self::space_or_zero(fs2::available_space(self.path()))
    }

    pub fn used_space(&self) -> u64 {
        let used = self.total_space().checked_sub(self.free_space());
        used.unwrap_or(0)
    }

    pub fn check_dir(&self) -> SysResult<()> {
        let m = self.path.metadata()?;

        if !m.is_dir() {
            return sys_error!(
                std::io::ErrorKind::NotADirectory,
                "Not a directory: {:?}",
                self.path()
            );
        }

        if m.permissions().readonly() {
            return sys_error!(
                std::io::ErrorKind::PermissionDenied,
                "Directory is not writable: {:?}",
                self.path()
            );
        }

        let new_device_id = sys::get_device_id(self.path());
        if new_device_id != self.device_id {
            warn!("The device ID associated with the directory {:?} has changed. Previous device ID: {},\
             Current device ID: {}", self.path(), self.device_id, new_device_id)
        }

        Ok(())
    }

    fn space_or_zero(result: SysResult<u64>) -> u64 {
        match result {
            Ok(value) => value,
            Err(error) => {
                warn!("{}", error);
                0
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::sys::FsStats;

    #[test]
    pub fn space() {
        let fs = FsStats::new("../target");
        println!(
            "path = {:?}, total_space = {}, free_space = {}, available_space = {}, len = {}",
            fs.path(),
            fs.total_space(),
            fs.free_space(),
            fs.available_space(),
            fs.len(),
        )
    }
}
