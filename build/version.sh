#!/usr/bin/env bash

#
# Copyright 2025 OPPO.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Read the version declared in [workspace.package]. The key can be indented,
# so a plain `grep '^version ='` is not reliable for every Cargo.toml layout.
get_workspace_version() {
  local cargo_toml="${1:?Cargo.toml path required}"
  awk '
    /^\[workspace\.package\]/ { in_workspace = 1; next }
    /^\[/ { in_workspace = 0 }
    in_workspace && /^[[:space:]]*version[[:space:]]*=/ {
      sub(/^[[:space:]]*version[[:space:]]*=[[:space:]]*"/, "")
      sub(/".*/, "")
      print
      exit
    }
  ' "$cargo_toml"
}

# BUILD_VERSION is the release/CI override. Without it, fall back to the
# Cargo workspace version.
#
# This is the RELEASE version (what compatibility checks and `--version` use),
# not the package-name version. Use get_package_version for artifact names.
get_build_version() {
  local cargo_toml="${1:?Cargo.toml path required}"
  if [ -n "${BUILD_VERSION:-}" ]; then
    printf '%s\n' "$BUILD_VERSION"
    return 0
  fi
  local version
  if ! version="$(get_workspace_version "$cargo_toml")"; then
    echo "failed to read workspace version from $cargo_toml" >&2
    return 1
  fi
  if [ -z "$version" ]; then
    echo "failed to resolve [workspace.package].version from $cargo_toml" >&2
    return 1
  fi
  printf '%s\n' "$version"
}

# ---------------------------------------------------------------------------
# Git-derived package version
#
# Artifact/package names must identify WHERE the build came from, exactly like
# the binaries' `--version` output does (tag when HEAD is a tagged commit,
# branch otherwise, always with the short commit). The Cargo workspace version
# is a hand-maintained release number, so it must not drive the package name:
# every build between two releases would otherwise be called after the last
# release version.
#
# Resolution order for get_package_version:
#   BUILD_VERSION override  ->  git (tag | branch) + short commit [-dirty]
#                           ->  Cargo workspace version (non-git source tree)
# ---------------------------------------------------------------------------

# Package names must stay filesystem-safe, so anything outside the portable
# set (e.g. the `/` in `codex/fix-1243` or `release/1.0`) becomes `-`.
sanitize_version_component() {
  printf '%s' "$1" \
    | sed -e 's/[^A-Za-z0-9._-]/-/g' -e 's/--*/-/g' -e 's/^-//' -e 's/-$//'
}

# Run git in a repository directory and print trimmed stdout. Returns 1 when
# git is unavailable or the command fails, so callers can fall back instead of
# baking an error message into a package name.
git_capture() {
  local repo_dir="${1:?repo dir required}"
  shift
  if ! command -v git >/dev/null 2>&1; then
    return 1
  fi
  local out
  if ! out="$(git -C "$repo_dir" "$@" 2>/dev/null)"; then
    return 1
  fi
  printf '%s' "$out"
}

# Short commit of HEAD (empty when not a git checkout).
get_git_short_commit() {
  local repo_dir="${1:?repo dir required}"
  local commit
  commit="$(git_capture "$repo_dir" rev-parse --short HEAD)" || return 1
  [ -n "$commit" ] || return 1
  printf '%s\n' "$commit"
}

# Tag pointing exactly at HEAD. Empty when HEAD is untagged (or in a detached
# HEAD that no tag covers), matching the `tag`-over-`branch` priority used by
# the component `--version` display.
get_git_head_tag() {
  local repo_dir="${1:?repo dir required}"
  git_capture "$repo_dir" describe --tags --exact-match HEAD
}

# Current branch name; empty for a detached HEAD (git prints `HEAD`).
get_git_head_branch() {
  local repo_dir="${1:?repo dir required}"
  local branch
  branch="$(git_capture "$repo_dir" rev-parse --abbrev-ref HEAD)" || return 1
  if [ -z "$branch" ] || [ "$branch" = "HEAD" ]; then
    return 1
  fi
  printf '%s\n' "$branch"
}

# True when tracked files differ from HEAD. Uses `describe --dirty` for the
# same signal the binaries embed in `GIT_DESCRIBE`, so package name and
# `--version` agree on what "dirty" means (untracked files do not count).
git_is_dirty() {
  local repo_dir="${1:?repo dir required}"
  local described
  described="$(git_capture "$repo_dir" describe --tags --always --dirty)" || return 1
  case "$described" in
    *-dirty) return 0 ;;
    *) return 1 ;;
  esac
}

# Version derived from the git checkout at $repo_dir:
#   tagged HEAD   -> <tag>-<commit>        e.g. 0.5.4-alpha-488e9da36
#   on a branch   -> <branch>-<commit>     e.g. pr1823-488e9da36
#   detached HEAD -> <commit>
# plus a `-dirty` suffix for an uncommitted working tree.
# Returns 1 (and prints nothing) when the tree is not a git checkout.
get_git_package_version() {
  local repo_dir="${1:?repo dir required}"
  local commit tag branch version

  commit="$(get_git_short_commit "$repo_dir")" || return 1

  # Every probe must tolerate failure explicitly: under `set -e` (dash in
  # particular) a bare `x="$(failing)"` aborts the function before it prints,
  # which would silently demote the package name to the Cargo version.
  tag="$(get_git_head_tag "$repo_dir")" || tag=""
  if [ -n "$tag" ]; then
    version="$(sanitize_version_component "${tag#v}")-$commit"
  elif branch="$(get_git_head_branch "$repo_dir")"; then
    version="$(sanitize_version_component "$branch")-$commit"
  else
    version="$commit"
  fi

  if git_is_dirty "$repo_dir"; then
    version="$version-dirty"
  fi

  printf '%s\n' "$version"
}

# The version stamped into artifact and package names.
#
# BUILD_VERSION still wins (release CI pins the tag version so released
# artifacts keep a clean semver name); otherwise the git identity is used, and
# only a source tree without git falls back to the Cargo workspace version.
get_package_version() {
  local cargo_toml="${1:?Cargo.toml path required}"
  local repo_dir="${2:-}"
  if [ -z "$repo_dir" ]; then
    repo_dir="$(cd "$(dirname "$cargo_toml")" && pwd)"
  fi

  if [ -n "${BUILD_VERSION:-}" ]; then
    printf '%s\n' "$BUILD_VERSION"
    return 0
  fi

  local git_version
  if git_version="$(get_git_package_version "$repo_dir")" && [ -n "$git_version" ]; then
    printf '%s\n' "$git_version"
    return 0
  fi

  get_build_version "$cargo_toml"
}

# Strip the leading `v` from a release tag. Tags are the release anchor, so
# v0.4.0-alpha becomes the release version 0.4.0-alpha.
tag_to_release_version() {
  local tag="${1:?tag required}"
  printf '%s\n' "${tag#v}"
}

# Overwrite the version declared in [workspace.package]. Used by the release
# build to drive Python SDK packaging (maturin with dynamic version reads the
# Cargo workspace version). Local builds without BUILD_VERSION never call this.
set_workspace_version() {
  local cargo_toml="${1:?Cargo.toml path required}"
  local version="${2:?version required}"
  local current
  current="$(get_workspace_version "$cargo_toml")" || {
    echo "failed to read workspace version from $cargo_toml" >&2
    return 1
  }
  if [ -z "$current" ]; then
    echo "failed to resolve [workspace.package].version from $cargo_toml" >&2
    return 1
  fi
  if [ "$current" = "$version" ]; then
    return 0
  fi
  if ! awk -v new_version="$version" '
    /^\[[^]]*\]/ { in_workspace = ($0 ~ /^\[workspace\.package\]/) ? 1 : 0 }
    in_workspace && /^[[:space:]]*version[[:space:]]*=/ {
      sub(/^[[:space:]]*version[[:space:]]*=[[:space:]]*".*"/, "version = \"" new_version "\"")
      in_workspace = 0
    }
    { print }
  ' "$cargo_toml" > "${cargo_toml}.tmp"; then
    rm -f "${cargo_toml}.tmp"
    echo "failed to update workspace version in $cargo_toml" >&2
    return 1
  fi
  mv "${cargo_toml}.tmp" "$cargo_toml"
  local actual
  if ! actual="$(get_workspace_version "$cargo_toml")"; then
    echo "failed to verify workspace version after update in $cargo_toml" >&2
    return 1
  fi
  if [ "$actual" != "$version" ]; then
    echo "expected workspace version $version after update, got $actual" >&2
    return 1
  fi
}

# Read the Maven project version. The project version is the <version> element
# that directly follows the project's <artifactId> in pom.xml (dependency
# versions appear later and must not be picked up).
get_pom_version() {
  local pom="${1:?pom.xml path required}"
  awk '
    /<artifactId>/ { if (!seen_artifact) { seen_artifact = 1; artifact_line = NR } }
    seen_artifact && !done && NR > artifact_line && /<version>[^<]*<\/version>/ {
      line = $0
      sub(/.*<version>/, "", line)
      sub(/<\/version>.*/, "", line)
      print line
      exit
    }
  ' "$pom"
}

# Overwrite the Maven project version in pom.xml. The release CI injects the
# tag version here because Java package versions are not maintained by hand.
set_pom_version() {
  local pom="${1:?pom.xml path required}"
  local version="${2:?version required}"
  local current
  current="$(get_pom_version "$pom")" || {
    echo "failed to read pom version from $pom" >&2
    return 1
  }
  if [ -z "$current" ]; then
    echo "failed to resolve project version from $pom" >&2
    return 1
  fi
  if [ "$current" = "$version" ]; then
    return 0
  fi
  if ! awk -v new_version="$version" '
    /<artifactId>/ { if (!seen_artifact) { seen_artifact = 1; artifact_line = NR } }
    seen_artifact && !done && NR > artifact_line && /<version>[^<]*<\/version>/ {
      sub(/<version>[^<]*<\/version>/, "<version>" new_version "</version>")
      done = 1
    }
    { print }
  ' "$pom" > "${pom}.tmp"; then
    rm -f "${pom}.tmp"
    echo "failed to update pom version in $pom" >&2
    return 1
  fi
  mv "${pom}.tmp" "$pom"
  local actual
  if ! actual="$(get_pom_version "$pom")"; then
    echo "failed to verify pom version after update in $pom" >&2
    return 1
  fi
  if [ "$actual" != "$version" ]; then
    echo "expected pom version $version after update, got $actual" >&2
    return 1
  fi
}

# Read the Python SDK package version from pyproject.toml. Since 1.x, maturin
# derives the wheel version from Cargo when the project declares
# `dynamic = ["version"]`; in that case no static [project].version exists and
# this returns an empty string (the version is inherited from the workspace).
get_python_version() {
  local pyproject="${1:?pyproject.toml path required}"
  awk '
    /^\[project\]/ { in_project = 1; next }
    /^\[/ { in_project = 0 }
    in_project && /^version[[:space:]]*=/ {
      sub(/^[[:space:]]*version[[:space:]]*=[[:space:]]*"/, "")
      sub(/".*/, "")
      print
      exit
    }
  ' "$pyproject"
}

# Overwrite the static [project].version in pyproject.toml. Only meaningful for
# projects with a pinned version; the Curvine Python SDK uses dynamic version,
# so the release flow drives it through set_workspace_version instead.
set_python_version() {
  local pyproject="${1:?pyproject.toml path required}"
  local version="${2:?version required}"
  local current
  current="$(get_python_version "$pyproject")" || {
    echo "failed to read python version from $pyproject" >&2
    return 1
  }
  if [ "$current" = "$version" ]; then
    return 0
  fi
  if ! awk -v new_version="$version" '
    /^\[[^]]*\]/ { in_project = ($0 ~ /^\[project\]/) ? 1 : 0 }
    in_project && /^version[[:space:]]*=/ {
      sub(/^[[:space:]]*version[[:space:]]*=[[:space:]]*".*/, "version = \"" new_version "\"")
      in_project = 0
    }
    { print }
  ' "$pyproject" > "${pyproject}.tmp"; then
    rm -f "${pyproject}.tmp"
    echo "failed to update python version in $pyproject" >&2
    return 1
  fi
  mv "${pyproject}.tmp" "$pyproject"
  local actual
  if ! actual="$(get_python_version "$pyproject")"; then
    echo "failed to verify python version after update in $pyproject" >&2
    return 1
  fi
  if [ "$actual" != "$version" ]; then
    echo "expected python version $version after update, got $actual" >&2
    return 1
  fi
}
