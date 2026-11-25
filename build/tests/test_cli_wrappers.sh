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

# bin/cv and bin/dfs must work from a CURVINE_HOME whose path holds a space,
# as the service launchers do, and pass every argument through unchanged.
#
# The harness stages both wrappers under such a path with stub curvine-cli
# and java commands that print what they were given.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT
HOME_DIR="$TMP_DIR/curvine home"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

mkdir -p "$HOME_DIR/bin" "$HOME_DIR/conf" "$HOME_DIR/lib" "$TMP_DIR/stubs"
cp "$ROOT/bin/cv" "$ROOT/bin/dfs" "$HOME_DIR/bin/"
cp "$ROOT/../etc/curvine-env.sh" "$HOME_DIR/conf/"
unset CURVINE_HOME CURVINE_CONF_DIR CURVINE_CONF_FILE CURVINE_LOG_DIR
: > "$HOME_DIR/lib/curvine-hadoop-a-shade.jar"
: > "$HOME_DIR/lib/curvine-hadoop-b-shade.jar"

cat > "$HOME_DIR/lib/curvine-cli" <<'STUB'
#!/usr/bin/env bash
printf 'curvine-cli'; printf ' [%s]' "$@"; printf '\n'
STUB
chmod +x "$HOME_DIR/lib/curvine-cli"

cat > "$TMP_DIR/stubs/java" <<'STUB'
#!/usr/bin/env bash
if [ "$1" = "-version" ]; then
  echo 'openjdk version "17.0.2" 2022-01-18' >&2
  exit 0
fi
printf 'CLASSPATH=%s\n' "$CLASSPATH"
printf 'java'; printf ' [%s]' "$@"; printf '\n'
STUB
chmod +x "$TMP_DIR/stubs/java"

output="$(bash "$HOME_DIR/bin/cv" fs -ls "/a b" 2>&1)" || fail "cv failed: $output"
[[ "$output" == "curvine-cli [fs] [-ls] [/a b]" ]] || fail "cv passed the wrong command: $output"

output="$(bash "$HOME_DIR/bin/dfs" load "/a b" 2>&1)" || fail "dfs (Rust client) failed: $output"
[[ "$output" == "curvine-cli [load] [/a b]" ]] || fail "dfs passed the wrong command: $output"

output="$(PATH="$TMP_DIR/stubs:$PATH" bash "$HOME_DIR/bin/dfs" fs -ls "/a b" 2>&1)" \
  || fail "dfs (Java client) failed: $output"
expected_cp="$HOME_DIR/lib/curvine-hadoop-a-shade.jar:$HOME_DIR/lib/curvine-hadoop-b-shade.jar"
[[ "$output" == *"CLASSPATH=$expected_cp"* ]] || fail "dfs built the wrong CLASSPATH: $output"
[[ "$output" == *"[io.curvine.CurvineShell] [fs] [-ls] [/a b]"* ]] || fail "dfs passed the wrong Java command: $output"

echo "PASS: cv and dfs work from a CURVINE_HOME with a space"
