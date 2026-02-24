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

use log::info;
use orpc::common::Utils;
use orpc::{err_msg, CommonResult};
use std::collections::HashMap;
use std::process::{Command, Stdio};

pub struct CommonUtils;

impl CommonUtils {
    pub const JOB_ID_PREFIX: &'static str = "job_";
    pub const CURVINE_STATE_FILE: &'static str = "CURVINE_STATE_FILE";

    pub fn create_job_id(source: impl AsRef<str>) -> String {
        format!("{}{}", Self::JOB_ID_PREFIX, Utils::md5(source))
    }

    pub fn create_job_id_with_generation(
        source: impl AsRef<str>,
        source_generation: impl AsRef<str>,
    ) -> String {
        format!(
            "{}{}",
            Self::JOB_ID_PREFIX,
            Utils::md5(format!(
                "{}#{}",
                source.as_ref(),
                source_generation.as_ref()
            ))
        )
    }

    pub fn source_generation(
        id: impl std::fmt::Display,
        len: impl std::fmt::Display,
        mtime: impl std::fmt::Display,
    ) -> String {
        Utils::md5(format!("{}:{}:{}", id, len, mtime))
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
}
