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

use crate::common::Utils;
use crate::CommonResult;
use log::info;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};

pub struct CommonUtils;

impl CommonUtils {
    pub const JOB_ID_PREFIX: &'static str = "job_";

    pub fn create_job_id(source: impl AsRef<str>) -> String {
        format!("{}{}", Self::JOB_ID_PREFIX, Utils::md5(source))
    }

    pub fn reload_param(env: HashMap<String, String>) -> CommonResult<()> {
        let exe_path = std::env::current_exe()
            .map_err(|e| err_msg!("failed to get current executable path: {}", e))?;
        let exe_path = Self::reload_exe_path(exe_path);
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

    /// The binary to start for a reload.
    ///
    /// On Linux `current_exe()` reads `/proc/self/exe`. After the file this
    /// process was started from is replaced, as an upgrade does, the kernel
    /// reports it as `<path> (deleted)`, which does not exist. The successor
    /// must be the binary now at `<path>`.
    fn reload_exe_path(exe: PathBuf) -> PathBuf {
        match exe.as_os_str().as_bytes().strip_suffix(b" (deleted)") {
            Some(path) => PathBuf::from(OsStr::from_bytes(path)),
            None => exe,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CommonUtils;
    use std::path::PathBuf;

    #[test]
    fn reload_exe_path_drops_the_deleted_suffix() {
        assert_eq!(
            CommonUtils::reload_exe_path(PathBuf::from("/opt/curvine/lib/curvine-fuse (deleted)")),
            PathBuf::from("/opt/curvine/lib/curvine-fuse")
        );
    }

    #[test]
    fn reload_exe_path_keeps_a_live_path() {
        for path in [
            "/opt/curvine/lib/curvine-fuse",
            "/opt/curvine (deleted)/lib/curvine-fuse",
            "/opt/curvine/lib/curvine-fuse (deleted).bak",
        ] {
            assert_eq!(
                CommonUtils::reload_exe_path(PathBuf::from(path)),
                PathBuf::from(path)
            );
        }
    }

    const HOLD_ENV: &str = "CURVINE_RELOAD_EXE_TEST_HOLD";

    // A process for `replaced_binary_resolves_to_the_new_file` to inspect: run
    // with HOLD_ENV set, it stays up until its stdin closes. It does nothing in
    // a normal test run.
    #[test]
    fn hold_until_stdin_closes() {
        if std::env::var_os(HOLD_ENV).is_some() {
            let _ = std::io::Read::read_to_end(&mut std::io::stdin(), &mut Vec::new());
        }
    }

    // The kernel behavior this relies on: once the file a running binary was
    // started from is replaced, /proc/<pid>/exe reads "<path> (deleted)".
    #[cfg(target_os = "linux")]
    #[test]
    fn replaced_binary_resolves_to_the_new_file() {
        use std::fs;
        use std::process::{Command, Stdio};

        let test_bin = std::env::current_exe().unwrap();
        let dir = std::env::temp_dir().join(format!("reload-exe-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let exe = dir.join("old-binary");
        fs::copy(&test_bin, &exe).unwrap();
        let mut child = Command::new(&exe)
            .args([
                "--exact",
                "common::common_utils::tests::hold_until_stdin_closes",
            ])
            .env(HOLD_ENV, "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        // Replace the file the way an upgrade does: write a new one, rename it over.
        let staged = dir.join("new-binary");
        fs::copy(&test_bin, &staged).unwrap();
        fs::rename(&staged, &exe).unwrap();
        let link = fs::read_link(format!("/proc/{}/exe", child.id()));

        drop(child.stdin.take());
        let _ = child.wait();
        let link = link.unwrap();
        assert_eq!(link, PathBuf::from(format!("{} (deleted)", exe.display())));
        let resolved = CommonUtils::reload_exe_path(link);
        assert_eq!(resolved, exe);
        assert!(resolved.is_file());
        let _ = fs::remove_dir_all(&dir);
    }
}
