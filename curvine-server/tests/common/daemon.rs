// Copyright 2026 OPPO.
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

use std::fs::{self, File};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub struct TestDirectory(pub PathBuf);

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub struct Daemon {
    child: Child,
    pub log_path: PathBuf,
}

impl Daemon {
    pub fn spawn(service: &str, config_path: &std::path::Path, directory: &TestDirectory) -> Self {
        let log_path = directory.0.join(format!(
            "{service}-{}-process.log",
            config_path.file_stem().unwrap().to_string_lossy()
        ));
        let log = File::create(&log_path).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_curvine-server"))
            .args(["--service", service, "--conf"])
            .arg(config_path)
            .env_remove("CURVINE_MASTER_HOSTNAME")
            .env_remove("CURVINE_JOURNAL_HOSTNAME")
            .env_remove("CURVINE_WORKER_HOSTNAME")
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        Self { child, log_path }
    }

    pub fn assert_running(&mut self) {
        if let Some(status) = self.child.try_wait().unwrap() {
            panic!(
                "daemon exited with {status}: {}",
                fs::read_to_string(&self.log_path).unwrap_or_default()
            );
        }
    }

    pub async fn terminate(&mut self) {
        self.assert_running();
        assert!(Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .unwrap()
            .success());
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(
                    status.success(),
                    "graceful shutdown failed: {status}: {}",
                    fs::read_to_string(&self.log_path).unwrap_or_default()
                );
                return;
            }
            assert!(Instant::now() < deadline, "daemon shutdown timed out");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Ok(log) = fs::read_to_string(&self.log_path) {
                let mut tail: Vec<_> = log.lines().rev().take(20).collect();
                tail.reverse();
                eprintln!("daemon {} log tail:\n{}", self.child.id(), tail.join("\n"));
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
