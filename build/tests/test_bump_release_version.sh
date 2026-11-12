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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUMP="$ROOT/build/bump-release-version.sh"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

write_cargo() {
  cat > "$1" <<'EOF'
[workspace]
members = []

[workspace.package]
version = "0.2.2"
edition = "2021"
EOF
}

write_pom() {
  cat > "$1" <<'EOF'
<project>
  <groupId>io.curvine</groupId>
  <artifactId>curvine-hadoop</artifactId>
  <version>0.2.2</version>
  <dependencies>
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-api</artifactId>
      <version>1.7.25</version>
    </dependency>
  </dependencies>
</project>
EOF
}

checksum() {
  cksum "$1" | awk '{print $1 $2}'
}

# --- updates Cargo.toml and pom.xml, accepts a v-prefixed input -------------
write_cargo "$TMP_DIR/Cargo.toml"
write_pom "$TMP_DIR/pom.xml"

bash "$BUMP" v0.6.1-alpha --cargo "$TMP_DIR/Cargo.toml" --pom "$TMP_DIR/pom.xml" --skip-lock >/dev/null
grep -q '^version = "0.6.1-alpha"$' "$TMP_DIR/Cargo.toml" || fail "Cargo version was not bumped"
grep -q '<version>0.6.1-alpha</version>' "$TMP_DIR/pom.xml" || fail "pom version was not bumped"
grep -q '<version>1.7.25</version>' "$TMP_DIR/pom.xml" || fail "dependency version was modified"

# --- idempotent -------------------------------------------------------------
cargo_sum="$(checksum "$TMP_DIR/Cargo.toml")"
pom_sum="$(checksum "$TMP_DIR/pom.xml")"
bash "$BUMP" 0.6.1-alpha --cargo "$TMP_DIR/Cargo.toml" --pom "$TMP_DIR/pom.xml" --skip-lock >/dev/null
[ "$cargo_sum" = "$(checksum "$TMP_DIR/Cargo.toml")" ] || fail "unchanged Cargo.toml was rewritten"
[ "$pom_sum" = "$(checksum "$TMP_DIR/pom.xml")" ] || fail "unchanged pom.xml was rewritten"

# --- dry-run does not modify files -----------------------------------------
cargo_sum="$(checksum "$TMP_DIR/Cargo.toml")"
pom_sum="$(checksum "$TMP_DIR/pom.xml")"
bash "$BUMP" 0.7.0 --cargo "$TMP_DIR/Cargo.toml" --pom "$TMP_DIR/pom.xml" --skip-lock --dry-run >/dev/null
[ "$cargo_sum" = "$(checksum "$TMP_DIR/Cargo.toml")" ] || fail "dry-run modified Cargo.toml"
[ "$pom_sum" = "$(checksum "$TMP_DIR/pom.xml")" ] || fail "dry-run modified pom.xml"

# --- invalid versions are rejected before any file changes ------------------
for invalid in "0.7" "latest" "v0.7.0.1" "0.7.0-" "01.7.0" "0.7.0-01" "0.7.0-alpha..1"; do
  cargo_sum="$(checksum "$TMP_DIR/Cargo.toml")"
  if bash "$BUMP" "$invalid" --cargo "$TMP_DIR/Cargo.toml" --pom "$TMP_DIR/pom.xml" --skip-lock >/dev/null 2>&1; then
    fail "expected invalid version '$invalid' to fail"
  fi
  [ "$cargo_sum" = "$(checksum "$TMP_DIR/Cargo.toml")" ] || fail "invalid version '$invalid' modified Cargo.toml"
done

# --- --skip-pom only touches Cargo.toml ------------------------------------
write_cargo "$TMP_DIR/Cargo.toml"
write_pom "$TMP_DIR/pom.xml"
bash "$BUMP" 0.8.0 --cargo "$TMP_DIR/Cargo.toml" --pom "$TMP_DIR/pom.xml" --skip-pom --skip-lock >/dev/null
grep -q '^version = "0.8.0"$' "$TMP_DIR/Cargo.toml" || fail "Cargo version was not bumped with --skip-pom"
grep -q '<version>0.2.2</version>' "$TMP_DIR/pom.xml" || fail "--skip-pom modified pom.xml"

# --- Cargo.lock: only inheriting workspace members are bumped --------------
WS="$TMP_DIR/ws"
mkdir -p "$WS/a" "$WS/b" "$WS/tests"
cat > "$WS/Cargo.toml" <<'EOF'
[workspace]
members = [
  "a",
  "b",
  "tests",
]

[workspace.package]
version = "0.2.2"
EOF
cat > "$WS/a/Cargo.toml" <<'EOF'
[package]
name = "curvine-a"
version.workspace = true
EOF
cat > "$WS/b/Cargo.toml" <<'EOF'
[package]
name = "curvine-b"
version.workspace = true
EOF
cat > "$WS/tests/Cargo.toml" <<'EOF'
[package]
name = "curvine-tests"
version = "0.1.0"
EOF
cat > "$WS/Cargo.lock" <<'EOF'
version = 4

[[package]]
name = "ahash"
version = "0.8.12"
source = "registry+https://github.com/rust-lang/crates.io-index"

[[package]]
name = "curvine-a"
version = "0.2.2"
dependencies = [
 "ahash 0.8.12",
]

[[package]]
name = "curvine-b"
version = "0.2.2"

[[package]]
name = "curvine-tests"
version = "0.1.0"
dependencies = [
 "ahash 0.8.12",
]
EOF

write_pom "$TMP_DIR/pom.xml"
bash "$BUMP" 0.9.0 --cargo "$WS/Cargo.toml" --pom "$TMP_DIR/pom.xml" >/dev/null
grep -A1 '^name = "curvine-a"$' "$WS/Cargo.lock" | grep -q '^version = "0.9.0"$' || fail "curvine-a was not bumped in Cargo.lock"
grep -A1 '^name = "curvine-b"$' "$WS/Cargo.lock" | grep -q '^version = "0.9.0"$' || fail "curvine-b was not bumped in Cargo.lock"
grep -A1 '^name = "curvine-tests"$' "$WS/Cargo.lock" | grep -q '^version = "0.1.0"$' || fail "explicit-version package was bumped in Cargo.lock"
grep -A1 '^name = "ahash"$' "$WS/Cargo.lock" | grep -q '^version = "0.8.12"$' || fail "unrelated dependency was bumped in Cargo.lock"
grep -q '"ahash 0.8.12"' "$WS/Cargo.lock" || fail "dependency reference was rewritten in Cargo.lock"

lock_sum="$(checksum "$WS/Cargo.lock")"
bash "$BUMP" 0.9.0 --cargo "$WS/Cargo.toml" --pom "$TMP_DIR/pom.xml" >/dev/null
[ "$lock_sum" = "$(checksum "$WS/Cargo.lock")" ] || fail "unchanged Cargo.lock was rewritten"

# --- a rerun without --skip-lock rejects a lockfile a --skip-lock run left stale
bash "$BUMP" 0.10.0 --cargo "$WS/Cargo.toml" --pom "$TMP_DIR/pom.xml" --skip-lock >/dev/null
if bash "$BUMP" 0.10.0 --cargo "$WS/Cargo.toml" --pom "$TMP_DIR/pom.xml" >/dev/null 2>&1; then
  fail "expected a stale Cargo.lock to fail after a --skip-lock bump"
fi
grep -A1 '^name = "curvine-a"$' "$WS/Cargo.lock" | grep -q '^version = "0.9.0"$' || fail "the stale lockfile check rewrote Cargo.lock"

# --- missing inputs are usage errors ---------------------------------------
if bash "$BUMP" 0.8.0 --cargo "$TMP_DIR/Cargo.toml" --pom "$TMP_DIR/missing.xml" --skip-lock >/dev/null 2>&1; then
  fail "expected a missing pom to fail"
fi
if bash "$BUMP" 0.8.0 --cargo "$TMP_DIR/Cargo.toml" --pom "$TMP_DIR/pom.xml" >/dev/null 2>&1; then
  fail "expected a missing Cargo.lock to fail"
fi

echo "PASS test_bump_release_version"
