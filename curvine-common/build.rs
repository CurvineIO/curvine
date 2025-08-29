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

use std::process::Command;
use std::{env, fs, str};

fn main() {
    let src = vec![
        "common.proto",
        "master.proto",
        "worker.proto",
        "job.proto",
        "mount.proto",
    ];

    let base = env::var("OUT_DIR").unwrap_or_else(|_| ".".to_string());
    let output = format!("{}/protos", base);
    fs::create_dir_all(&output).unwrap();

    let mut build = prost_build::Config::new();
    build.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");

    build
        .out_dir(&output)
        .compile_protos(&src, &["proto/"])
        .unwrap();

    let src = vec!["raft.proto"];
    let mut build = prost_build::Config::new();
    build
        .out_dir(&output)
        .extern_path(".eraftpb", "::raft::eraftpb")
        .compile_protos(&src, &["proto/", ""])
        .unwrap();

    // Build version number file
    let ver_file = format!("{}/version.rs", base);
    let commit = get_git_head_commit();
    let commit_str = format!("pub static GIT_VERSION: &str = \"{}\";", commit);
    fs::write(ver_file, commit_str).unwrap();
}

fn get_git_head_commit() -> String {
    let output = Command::new("git")
        .arg("rev-parse")
        .arg("--short")
        .arg("HEAD")
        .output();

    if let Ok(v) = output {
        if v.status.success() {
            str::from_utf8(&v.stdout)
                .unwrap_or("unknown")
                .trim()
                .to_string()
        } else {
            "unknown".to_string()
        }
    } else {
        "unknown".to_string()
    }
}
