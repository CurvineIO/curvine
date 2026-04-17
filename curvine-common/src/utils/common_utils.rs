//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::fs::Path;
use crate::state::{FileStatus, FileType};
use crate::FsResult;
use log::info;
use orpc::common::Utils;
use orpc::io::LocalFile;
use orpc::{err_msg, ternary, CommonResult};
use std::collections::HashMap;
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct CommonUtils;

impl CommonUtils {
    pub const JOB_ID_PREFIX: &'static str = "job_";
    pub const CURVINE_STATE_FILE: &'static str = "CURVINE_STATE_FILE";

    pub fn create_job_id(source: impl AsRef<str>) -> String {
        format!("{}{}", Self::JOB_ID_PREFIX, Utils::md5(source))
    }

    pub fn reload_param(env: HashMap<String, String>) -> CommonResult<()> {
        let exe_path = std::env::current_exe()
            .map_err(|e| err_msg!("failed to get current executable path: {}", e))?;
        let args: Vec<String> = std::env::args().collect();

        info!(
            "reloading: executing {:?} with args: {:?}",
            exe_path,
            &args[1..]
        );

        let mut cmd = Command::new(&exe_path);
        cmd.args(&args[1..]);
        cmd.stdin(Stdio::inherit());
        cmd.stdout(Stdio::inherit());
        cmd.stderr(Stdio::inherit());

        for (k, v) in env.clone() {
            cmd.env(k, v);
        }

        let _child = cmd
            .spawn()
            .map_err(|e| err_msg!("Failed to spawn new process: {}", e))?;

        info!("reload: new process spawned successfully");
        Ok(())
    }

    fn to_epoch_ms(ts: std::io::Result<SystemTime>) -> i64 {
        ts.ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    pub fn metadata_to_file_status(path: &Path, meta: &std::fs::Metadata) -> FileStatus {
        FileStatus {
            path: path.full_path().to_owned(),
            name: path.name().to_owned(),
            is_dir: meta.is_dir(),
            mtime: Self::to_epoch_ms(meta.modified()),
            atime: Self::to_epoch_ms(meta.accessed()),
            children_num: 0,
            is_complete: true,
            len: meta.len() as i64,
            replicas: 1,
            block_size: 512,
            file_type: ternary!(meta.is_dir(), FileType::Dir, FileType::File),
            mode: 0o777,
            ..Default::default()
        }
    }

    pub fn file_to_status(path: &Path, file: &LocalFile) -> FsResult<FileStatus> {
        let meta = file.metadata()?;
        Ok(Self::metadata_to_file_status(path, &meta))
    }
}
