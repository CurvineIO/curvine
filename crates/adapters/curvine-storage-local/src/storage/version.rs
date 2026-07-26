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

use orpc::common::{LocalTime, Utils};
use orpc::io::LocalFile;
use orpc::{err_box, try_err, CommonResult};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;

// Store directory version information
#[derive(Serialize, Deserialize, Clone)]
pub struct StorageVersion {
    pub(crate) cluster_id: String,
    pub(crate) worker_id: u32,
    pub(crate) storage_id: String,
    pub(crate) dir_id: u32,
    pub(crate) time_ms: u64,
}

impl StorageVersion {
    pub fn with_cluster<T: AsRef<str>>(id: T) -> Self {
        Self {
            cluster_id: id.as_ref().to_string(),
            ..Default::default()
        }
    }

    // Read the version information of the storage directory.
    pub fn read_version<T: AsRef<str>>(path: T, cluster_id: T) -> CommonResult<StorageVersion> {
        let path = PathBuf::from_str(path.as_ref())?;
        if !path.exists() {
            try_err!(fs::create_dir_all(path.as_path()));
        } else if path.is_file() {
            return err_box!("{:?} not a dir", path);
        }

        // Try loading the configuration file.
        let file = path.join("version");
        let ver: Option<StorageVersion> = LocalFile::read_toml(file.as_path())?;

        let mut version = match ver {
            None => {
                let storage_id = Utils::uuid();
                let dir_id = Utils::crc32(storage_id.as_bytes());
                StorageVersion {
                    cluster_id: cluster_id.as_ref().to_owned(),
                    worker_id: Utils::machine_id(),
                    storage_id,
                    dir_id,
                    time_ms: 0,
                }
            }
            Some(v) => v,
        };

        version.time_ms = LocalTime::mills();

        Ok(version)
    }
}

impl Default for StorageVersion {
    fn default() -> Self {
        let storage_id = Utils::uuid();
        let dir_id = Utils::crc32(storage_id.as_bytes());
        Self {
            cluster_id: Utils::rand_str(8),
            worker_id: Utils::machine_id(),
            storage_id,
            dir_id,
            time_ms: LocalTime::mills(),
        }
    }
}
