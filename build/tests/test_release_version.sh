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

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# --- tag_to_release_version -------------------------------------------------
actual="$(tag_to_release_version "v0.4.0-alpha")"
[ "$actual" = "0.4.0-alpha" ] || fail "tag_to_release_version v0.4.0-alpha, got $actual"
actual="$(tag_to_release_version "v0.2.0")"
[ "$actual" = "0.2.0" ] || fail "tag_to_release_version v0.2.0, got $actual"

# --- set_workspace_version --------------------------------------------------
cat > "$TMP_DIR/Cargo.toml" <<'EOF'
[workspace.package]
  version = "0.2.0"
EOF

set_workspace_version "$TMP_DIR/Cargo.toml" "0.4.0-alpha"
actual="$(get_workspace_version "$TMP_DIR/Cargo.toml")"
[ "$actual" = "0.4.0-alpha" ] || fail "set_workspace_version result, got $actual"
# Empty version must be rejected (subshell: ${":?} terminates the shell)
if ( set +e; set_workspace_version "$TMP_DIR/Cargo.toml" "" ) >/dev/null 2>&1; then
  fail "expected empty version to fail"
fi

# --- get_pom_version / set_pom_version --------------------------------------
cat > "$TMP_DIR/pom.xml" <<'EOF'
<project>
  <groupId>io.curvine</groupId>
  <artifactId>curvine-hadoop</artifactId>
  <version>0.2.0</version>
  <dependencies>
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-api</artifactId>
      <version>1.7.25</version>
    </dependency>
  </dependencies>
</project>
EOF

actual="$(get_pom_version "$TMP_DIR/pom.xml")"
[ "$actual" = "0.2.0" ] || fail "get_pom_version, got $actual"

set_pom_version "$TMP_DIR/pom.xml" "0.3.1-rc1"
actual="$(get_pom_version "$TMP_DIR/pom.xml")"
[ "$actual" = "0.3.1-rc1" ] || fail "set_pom_version result, got $actual"
# Dependency version must be untouched
grep -q "<version>1.7.25</version>" "$TMP_DIR/pom.xml" || fail "dependency version was modified"

# --- get_python_version / set_python_version --------------------------------
cat > "$TMP_DIR/pyproject-static.toml" <<'EOF'
[project]
name = "curvine_libsdk"
version = "0.1.0"
EOF

actual="$(get_python_version "$TMP_DIR/pyproject-static.toml")"
[ "$actual" = "0.1.0" ] || fail "get_python_version, got $actual"

set_python_version "$TMP_DIR/pyproject-static.toml" "0.4.0-alpha"
actual="$(get_python_version "$TMP_DIR/pyproject-static.toml")"
[ "$actual" = "0.4.0-alpha" ] || fail "set_python_version result, got $actual"

# Dynamic pyproject has no static version to report
cat > "$TMP_DIR/pyproject-dynamic.toml" <<'EOF'
[project]
name = "curvine_libsdk"
dynamic = ["version"]
EOF
actual="$(get_python_version "$TMP_DIR/pyproject-dynamic.toml")"
[ -z "$actual" ] || fail "dynamic pyproject should have no static version, got $actual"

# --- validate-release-version.sh (end to end) --------------------------------
VALIDATE="$ROOT/validate-release-version.sh"
cat > "$TMP_DIR/release-Cargo.toml" <<'EOF'
[workspace.package]
version = "0.4.0-alpha"
EOF

# Re-align the fixtures mutated by the helper tests above
set_pom_version "$TMP_DIR/pom.xml" "0.4.0-alpha"
set_python_version "$TMP_DIR/pyproject-static.toml" "0.4.0-alpha"

# PASS: tag matches Cargo
"$VALIDATE" "v0.4.0-alpha" --cargo "$TMP_DIR/release-Cargo.toml" >/dev/null 2>&1 \
  || fail "validate should pass for matching tag"

# PASS: with pom and pyproject already aligned
"$VALIDATE" "v0.4.0-alpha" \
  --cargo "$TMP_DIR/release-Cargo.toml" \
  --pom "$TMP_DIR/pom.xml" \
  --pyproject "$TMP_DIR/pyproject-static.toml" >/dev/null 2>&1 \
  || fail "validate should pass for aligned pom and pyproject"

# FAIL: tag does not match Cargo
if "$VALIDATE" "v1.2.3" --cargo "$TMP_DIR/release-Cargo.toml" >/dev/null 2>&1; then
  fail "validate should fail for mismatched tag"
fi

# FAIL: invalid tag shape
if "$VALIDATE" "1.2.3" --cargo "$TMP_DIR/release-Cargo.toml" >/dev/null 2>&1; then
  fail "validate should fail for tag without v prefix"
fi
if "$VALIDATE" "vabc" --cargo "$TMP_DIR/release-Cargo.toml" >/dev/null 2>&1; then
  fail "validate should fail for non-semver tag"
fi

# FAIL: pom not aligned
set_pom_version "$TMP_DIR/pom.xml" "0.3.1-rc1"
if "$VALIDATE" "v0.4.0-alpha" \
  --cargo "$TMP_DIR/release-Cargo.toml" \
  --pom "$TMP_DIR/pom.xml" >/dev/null 2>&1; then
  fail "validate should fail when pom version differs from tag"
fi

# PASS: dynamic pyproject still validates (version inherited from workspace)
"$VALIDATE" "v0.4.0-alpha" \
  --cargo "$TMP_DIR/release-Cargo.toml" \
  --pyproject "$TMP_DIR/pyproject-dynamic.toml" >/dev/null 2>&1 \
  || fail "validate should pass for dynamic pyproject"

# FAIL: static pyproject not aligned
set_python_version "$TMP_DIR/pyproject-static.toml" "0.1.0"
if "$VALIDATE" "v0.4.0-alpha" \
  --cargo "$TMP_DIR/release-Cargo.toml" \
  --pyproject "$TMP_DIR/pyproject-static.toml" >/dev/null 2>&1; then
  fail "validate should fail when pyproject version differs from tag"
fi

# --- dry-run output ---------------------------------------------------------
OUT="$("$VALIDATE" "v0.4.0-alpha" --cargo "$TMP_DIR/release-Cargo.toml" --dry-run)"
echo "$OUT" | grep -q "BUILD_VERSION=0.4.0-alpha" || fail "dry-run should list BUILD_VERSION"
echo "$OUT" | grep -q "set_workspace_version" || fail "dry-run should list workspace injection"

echo "release version validation: ok"