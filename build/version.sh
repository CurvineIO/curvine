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
