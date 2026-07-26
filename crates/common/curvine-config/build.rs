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

use std::path::{Path, PathBuf};
use std::process::Command;
use std::{env, fs, str};

fn main() {
    emit_git_rerun_if_changed();

    let commit = run_git_command(&["rev-parse", "--short", "HEAD"]);
    let pkg_version = env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "unknown".to_string());
    let git_tag = get_git_tag();
    let git_branch = get_git_branch();

    let source_info = if !git_tag.is_empty() && git_tag != "unknown" {
        format!("tag: {}", git_tag)
    } else if !git_branch.is_empty() && git_branch != "unknown" {
        format!("branch: {}", git_branch)
    } else {
        String::new()
    };

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

    let base = env::var("OUT_DIR").unwrap_or_else(|_| ".".to_string());
    fs::write(format!("{base}/version.rs"), version_content).unwrap();
}

fn emit_git_rerun_if_changed() {
    let Some(head_path) = git_path("HEAD") else {
        return;
    };
    println!("cargo:rerun-if-changed={}", head_path.display());

    if let Ok(contents) = fs::read_to_string(&head_path) {
        if let Some(git_ref) = contents.strip_prefix("ref: ") {
            let git_ref = git_ref.trim();
            if let Some(ref_path) = git_path(git_ref) {
                println!("cargo:rerun-if-changed={}", ref_path.display());
            }
        }
    }

    if let Some(packed_refs) = git_path("packed-refs") {
        println!("cargo:rerun-if-changed={}", packed_refs.display());
    }
}

fn git_path(path: &str) -> Option<PathBuf> {
    let output = Command::new("git")
        .args(["rev-parse", "--git-path", path])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let raw = str::from_utf8(&output.stdout).ok()?.trim();
    if raw.is_empty() {
        return None;
    }
    let path = Path::new(raw);
    if path.is_absolute() {
        Some(path.to_path_buf())
    } else {
        let manifest_dir = env::var_os("CARGO_MANIFEST_DIR")?;
        Some(PathBuf::from(manifest_dir).join(path))
    }
}

fn get_git_tag() -> String {
    let tag = run_git_command(&["describe", "--tags", "--exact-match", "HEAD"]);
    if !tag.is_empty() && tag != "unknown" {
        return tag;
    }
    String::new()
}

fn get_git_branch() -> String {
    let branch = run_git_command(&["rev-parse", "--abbrev-ref", "HEAD"]);
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
