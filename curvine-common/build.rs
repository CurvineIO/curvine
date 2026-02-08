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
        "replication.proto",
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
    let pkg_version = env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "unknown".to_string());
    let git_tag = get_git_tag();
    let git_branch = get_git_branch();

    // Build the source info: prefer tag over branch
    let source_info = if !git_tag.is_empty() && git_tag != "unknown" {
        format!("tag: {}", git_tag)
    } else if !git_branch.is_empty() && git_branch != "unknown" {
        format!("branch: {}", git_branch)
    } else {
        String::new()
    };

    // Build full version string
    let full_version = if !source_info.is_empty() {
        format!("{} (commit: {}, {})", pkg_version, commit, source_info)
    } else {
        format!("{} (commit: {})", pkg_version, commit)
    };

    let version_content = format!(
        r#"/// Git commit ID (short)
pub static GIT_VERSION: &str = "{}";

/// Package version from Cargo.toml
pub static PKG_VERSION: &str = "{}";

/// Git tag (if built from a tag)
pub static GIT_TAG: &str = "{}";

/// Git branch (if not built from a tag)
pub static GIT_BRANCH: &str = "{}";

/// Full version string: "version (commit: commit-id, tag/branch: name)"
pub static VERSION: &str = "{}";
"#,
        commit, pkg_version, git_tag, git_branch, full_version
    );

    fs::write(ver_file, version_content).unwrap();
}

fn get_git_head_commit() -> String {
    run_git_command(&["rev-parse", "--short", "HEAD"])
}

fn get_git_tag() -> String {
    // Try to get exact tag at HEAD
    let tag = run_git_command(&["describe", "--tags", "--exact-match", "HEAD"]);
    if !tag.is_empty() && tag != "unknown" {
        return tag;
    }
    String::new()
}

fn get_git_branch() -> String {
    let branch = run_git_command(&["rev-parse", "--abbrev-ref", "HEAD"]);
    // Skip if it's HEAD (detached HEAD state, like in CI)
    if branch == "HEAD" {
        return String::new();
    }
    branch
}

fn run_git_command(args: &[&str]) -> String {
    let output = Command::new("git").args(args).output();

    if let Ok(v) = output {
        if v.status.success() {
            return str::from_utf8(&v.stdout)
                .unwrap_or("unknown")
                .trim()
                .to_string();
        }
    }
    "unknown".to_string()
}
