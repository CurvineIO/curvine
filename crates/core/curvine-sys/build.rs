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
    println!("cargo:rerun-if-env-changed=BUILD_VERSION");

    let base = env::var("OUT_DIR").unwrap_or_else(|_| ".".to_string());
    let ver_file = format!("{}/version.rs", base);
    let commit = get_git_head_commit();
    let build_version = env::var("BUILD_VERSION").ok().filter(|v| !v.is_empty());
    let pkg_version = build_version
        .clone()
        .unwrap_or_else(|| env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "unknown".to_string()));
    let git_tag = get_git_tag();
    let git_branch = get_git_branch();
    let git_describe = get_git_describe();

    // Build the source info: prefer tag over branch
    let source_info = if !git_tag.is_empty() && git_tag != "unknown" {
        format!("tag: {}", git_tag)
    } else if !git_branch.is_empty() && git_branch != "unknown" {
        format!("branch: {}", git_branch)
    } else {
        String::new()
    };

    // Build full version string
    let full_version = if let Some(build_version) = build_version {
        build_version
    } else if !source_info.is_empty() {
        format!("{} (commit: {}, {})", pkg_version, commit, source_info)
    } else {
        format!("{} (commit: {})", pkg_version, commit)
    };

    let version_content = format!(
        r#"/// Git commit ID (short)
pub static GIT_VERSION: &str = "{}";

/// Package version from Cargo.toml, or BUILD_VERSION when provided
pub static PKG_VERSION: &str = "{}";

/// Git tag (if built from a tag)
pub static GIT_TAG: &str = "{}";

/// Git branch (if not built from a tag)
pub static GIT_BRANCH: &str = "{}";

/// Full version string. BUILD_VERSION overrides the package-derived string.
pub static VERSION: &str = "{}";

/// Raw `git describe --tags --always --dirty` output captured at build time.
pub static GIT_DESCRIBE: &str = "{}";
"#,
        commit, pkg_version, git_tag, git_branch, full_version, git_describe
    );

    fs::write(ver_file, version_content).unwrap();
}

/// Tell Cargo to re-run this build script when Git HEAD (or the branch it
/// points at) changes. Uses `git rev-parse --git-path` so git worktrees work
/// (literal `.git/HEAD` is a file there, not a directory).
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

    // After `git pack-refs`, the loose ref may be absent and tip updates only
    // touch packed-refs; watch it so incremental builds still refresh VERSION.
    if let Some(packed_refs) = git_path("packed-refs") {
        println!("cargo:rerun-if-changed={}", packed_refs.display());
    }

    // `GIT_DESCRIBE` also depends on tags. Loose tags are not covered by the
    // HEAD/packed-refs watches: creating, moving, or deleting a local tag
    // changes `git describe` output without touching those files. Watch the
    // `refs/tags` directory (mtime covers create/move/delete) and every
    // currently loose tag file (covers `git tag -f` content rewrites).
    if let Some(tags_dir) = git_path("refs/tags") {
        println!("cargo:rerun-if-changed={}", tags_dir.display());
    }
    for tag in list_loose_tags() {
        if let Some(tag_path) = git_path(&format!("refs/tags/{}", tag)) {
            println!("cargo:rerun-if-changed={}", tag_path.display());
        }
    }
}

/// Names of the tags currently stored under `refs/tags`. Empty when git is
/// unavailable or the command fails.
fn list_loose_tags() -> Vec<String> {
    let output = match Command::new("git")
        .args(["for-each-ref", "--format=%(refname:short)", "refs/tags"])
        .output()
    {
        Ok(output) if output.status.success() => output,
        _ => return Vec::new(),
    };
    let stdout = match str::from_utf8(&output.stdout) {
        Ok(stdout) => stdout,
        Err(_) => return Vec::new(),
    };
    stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
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
        // `git rev-parse --git-path` may return a cwd-relative path; resolve
        // against the package directory so Cargo can watch it reliably.
        let manifest_dir = env::var_os("CARGO_MANIFEST_DIR")?;
        Some(PathBuf::from(manifest_dir).join(path))
    }
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

fn get_git_describe() -> String {
    // Prefer annotated tags, fall back to the short commit when no tag exists,
    // and append `-dirty` when the working tree has uncommitted changes.
    run_git_command(&["describe", "--tags", "--always", "--dirty"])
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
