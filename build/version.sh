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
# Cargo workspace version so local builds still produce a stable package name.
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
  awk -v new_version="$version" '
    /^\[[^]]*\]/ { in_workspace = ($0 ~ /^\[workspace\.package\]/) ? 1 : 0 }
    in_workspace && /^[[:space:]]*version[[:space:]]*=/ {
      sub(/^[[:space:]]*version[[:space:]]*=[[:space:]]*".*"/, "version = \"" new_version "\"")
      in_workspace = 0
    }
    { print }
  ' "$cargo_toml" > "${cargo_toml}.tmp" && mv "${cargo_toml}.tmp" "$cargo_toml"
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
  awk -v new_version="$version" '
    /<artifactId>/ { if (!seen_artifact) { seen_artifact = 1; artifact_line = NR } }
    seen_artifact && !done && NR > artifact_line && /<version>[^<]*<\/version>/ {
      sub(/<version>[^<]*<\/version>/, "<version>" new_version "</version>")
      done = 1
    }
    { print }
  ' "$pom" > "${pom}.tmp" && mv "${pom}.tmp" "$pom"
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
  awk -v new_version="$version" '
    /^\[[^]]*\]/ { in_project = ($0 ~ /^\[project\]/) ? 1 : 0 }
    in_project && /^version[[:space:]]*=/ {
      sub(/^[[:space:]]*version[[:space:]]*=[[:space:]]*".*/, "version = \"" new_version "\"")
      in_project = 0
    }
    { print }
  ' "$pyproject" > "${pyproject}.tmp" && mv "${pyproject}.tmp" "$pyproject"
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
