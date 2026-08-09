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

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/version.sh"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

cat > "$TMP_DIR/Cargo.toml" <<'EOF'
[workspace.package]
  version = "0.2.0"
EOF

actual="$(get_build_version "$TMP_DIR/Cargo.toml")"
if [ "$actual" != "0.2.0" ]; then
  echo "expected workspace version 0.2.0, got $actual" >&2
  exit 1
fi

BUILD_VERSION="0.3.0-alpha" actual="$(get_build_version "$TMP_DIR/Cargo.toml")"
if [ "$actual" != "0.3.0-alpha" ]; then
  echo "expected BUILD_VERSION override 0.3.0-alpha, got $actual" >&2
  exit 1
fi

echo "workspace version resolution: ok"
