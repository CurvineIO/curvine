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

# Package-name version resolution.
#
# A package name must say where the build came from, matching what the
# binaries' `--version` reports: the tag when HEAD is tagged, otherwise the
# branch, always with the short commit, plus `-dirty` for an uncommitted tree.
# The Cargo workspace version is a hand-maintained release number and must not
# leak into package names (it used to, so every build was called after the last
# release, e.g. curvine-0.2.2-x86_64-centos8.zip).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/version.sh"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# Git identity for fixture commits; keep it local to the test.
git_init() {
  local dir="$1"
  git -C "$dir" init -q
  git -C "$dir" config user.email "test@curvine.invalid"
  git -C "$dir" config user.name "curvine test"
  git -C "$dir" config commit.gpgsign false
}

git_commit() {
  local dir="$1" file="$2" content="$3"
  printf '%s\n' "$content" > "$dir/$file"
  git -C "$dir" add "$file"
  git -C "$dir" commit -q -m "commit $content"
}

write_cargo() {
  local dir="$1" version="$2"
  cat > "$dir/Cargo.toml" <<EOF
[workspace.package]
version = "$version"
edition = "2021"
license = "Apache-2.0"
EOF
}

# --- sanitize_version_component ---------------------------------------------
actual="$(sanitize_version_component 'codex/fix-1243')"
[ "$actual" = "codex-fix-1243" ] || fail "slash should become dash, got $actual"
actual="$(sanitize_version_component 'release/1.0')"
[ "$actual" = "release-1.0" ] || fail "release/1.0 should become release-1.0, got $actual"
actual="$(sanitize_version_component 'a//b')"
[ "$actual" = "a-b" ] || fail "repeated separators should collapse, got $actual"
actual="$(sanitize_version_component 'v0.5.4-alpha')"
[ "$actual" = "v0.5.4-alpha" ] || fail "semver-ish names must pass through, got $actual"
actual="$(sanitize_version_component '  spaced name  ')"
[ "$actual" = "spaced-name" ] || fail "spaces should become dashes, got $actual"

# --- untagged branch HEAD ---------------------------------------------------
REPO="$TMP_DIR/branch-repo"
mkdir -p "$REPO"
git_init "$REPO"
write_cargo "$REPO" "0.2.2"
git_commit "$REPO" f.txt "one"
git -C "$REPO" checkout -q -b pr1823

commit="$(git -C "$REPO" rev-parse --short HEAD)"
actual="$(get_package_version "$REPO/Cargo.toml" "$REPO")"
[ "$actual" = "pr1823-$commit" ] \
  || fail "branch build should be named pr1823-$commit, got $actual"
# The Cargo version must not appear at all.
case "$actual" in
  *0.2.2*) fail "package version must not fall back to the Cargo version: $actual" ;;
esac
# The alias without an explicit repo dir must resolve to the Cargo.toml dir.
actual="$(get_package_version "$REPO/Cargo.toml")"
[ "$actual" = "pr1823-$commit" ] \
  || fail "repo dir should default to the Cargo.toml directory, got $actual"

# --- tagged HEAD: the tag wins over the branch ------------------------------
git -C "$REPO" tag "v0.5.4-alpha"
actual="$(get_package_version "$REPO/Cargo.toml" "$REPO")"
[ "$actual" = "0.5.4-alpha-$commit" ] \
  || fail "tagged HEAD should be named after the tag (v stripped), got $actual"

# A tag that is not on HEAD must be ignored.
git_commit "$REPO" f.txt "two"
commit2="$(git -C "$REPO" rev-parse --short HEAD)"
actual="$(get_package_version "$REPO/Cargo.toml" "$REPO")"
[ "$actual" = "pr1823-$commit2" ] \
  || fail "a tag behind HEAD must not be used, got $actual"

# --- lightweight tag on HEAD is also honoured ------------------------------
git -C "$REPO" tag "0.6.0"
actual="$(get_package_version "$REPO/Cargo.toml" "$REPO")"
[ "$actual" = "0.6.0-$commit2" ] \
  || fail "lightweight tag without v prefix should be used as-is, got $actual"

# --- dirty working tree ----------------------------------------------------
printf 'dirty\n' > "$REPO/f.txt"
actual="$(get_package_version "$REPO/Cargo.toml" "$REPO")"
[ "$actual" = "0.6.0-$commit2-dirty" ] \
  || fail "uncommitted tracked change should append -dirty, got $actual"

git -C "$REPO" checkout -q -- f.txt
actual="$(get_package_version "$REPO/Cargo.toml" "$REPO")"
[ "$actual" = "0.6.0-$commit2" ] \
  || fail "clean tree must not carry -dirty, got $actual"

# Untracked files are not dirty, matching `git describe --dirty`.
printf 'scratch\n' > "$REPO/untracked.txt"
actual="$(get_package_version "$REPO/Cargo.toml" "$REPO")"
case "$actual" in
  *-dirty) fail "untracked files must not mark the build dirty: $actual" ;;
esac
rm -f "$REPO/untracked.txt"

# --- detached HEAD without a covering tag ----------------------------------
DETACHED="$TMP_DIR/detached-repo"
mkdir -p "$DETACHED"
git_init "$DETACHED"
write_cargo "$DETACHED" "0.2.2"
git_commit "$DETACHED" f.txt "one"
git_commit "$DETACHED" f.txt "two"
git -C "$DETACHED" checkout -q --detach HEAD~1
commit="$(git -C "$DETACHED" rev-parse --short HEAD)"
actual="$(get_package_version "$DETACHED/Cargo.toml" "$DETACHED")"
[ "$actual" = "$commit" ] \
  || fail "detached HEAD should fall back to the commit only, got $actual"

# --- BUILD_VERSION override wins -------------------------------------------
actual="$(BUILD_VERSION="0.5.4-alpha" get_package_version "$REPO/Cargo.toml" "$REPO")"
[ "$actual" = "0.5.4-alpha" ] \
  || fail "BUILD_VERSION must override the git identity, got $actual"
# An empty override is not an override.
actual="$(BUILD_VERSION="" get_package_version "$REPO/Cargo.toml" "$REPO")"
[ "$actual" = "0.6.0-$commit2" ] \
  || fail "empty BUILD_VERSION must still use git identity, got $actual"

# --- no git checkout: fall back to the Cargo version -----------------------
NOGIT="$TMP_DIR/nogit"
mkdir -p "$NOGIT"
write_cargo "$NOGIT" "0.2.2"
actual="$(get_package_version "$NOGIT/Cargo.toml" "$NOGIT")"
[ "$actual" = "0.2.2" ] \
  || fail "a source tree without git should use the Cargo version, got $actual"

# A missing Cargo version still fails loudly instead of producing a nameless
# package.
cat > "$NOGIT/broken.toml" <<'EOF'
[workspace.package]
name = "curvine"
EOF
if actual="$(get_package_version "$NOGIT/broken.toml" "$NOGIT" 2>/dev/null)"; then
  fail "a missing workspace version must fail, got $actual"
fi

# --- get_build_version is unchanged ---------------------------------------
# The release version stays Cargo/BUILD_VERSION-derived: it is what the
# binaries report and what compatibility checks compare.
actual="$(get_build_version "$NOGIT/Cargo.toml")"
[ "$actual" = "0.2.2" ] || fail "get_build_version should still read Cargo, got $actual"
actual="$(BUILD_VERSION="9.9.9" get_build_version "$NOGIT/Cargo.toml")"
[ "$actual" = "9.9.9" ] || fail "get_build_version should honour BUILD_VERSION, got $actual"
# In a git checkout the release version is still the Cargo version.
actual="$(get_build_version "$REPO/Cargo.toml")"
[ "$actual" = "0.2.2" ] || fail "release version must not become git-derived, got $actual"

# --- the resolver must work under POSIX sh + set -e ------------------------
# build.sh is invoked as `sh build/build.sh`, and /bin/sh is dash on Debian
# and Ubuntu. Under `set -e` an unguarded failing probe aborts the function
# silently and demotes the package name to the Cargo version.
#
# The regression this guards is specifically the UNTAGGED path: `git describe
# --exact-match HEAD` exits non-zero when HEAD has no tag, so a bare
# `tag="$(...)"` kills the function under dash and the package name silently
# becomes the Cargo version. A tagged fixture would hide the bug entirely,
# hence the dedicated untagged repo below.
SH_REPO="$TMP_DIR/sh-untagged-repo"
mkdir -p "$SH_REPO"
git_init "$SH_REPO"
write_cargo "$SH_REPO" "0.2.2"
git_commit "$SH_REPO" f.txt "one"
git -C "$SH_REPO" checkout -q -b fix/package-name
sh_commit="$(git -C "$SH_REPO" rev-parse --short HEAD)"

# Precondition: this repo must really be untagged, otherwise the test below
# would pass for the wrong reason.
if git -C "$SH_REPO" describe --tags --exact-match HEAD >/dev/null 2>&1; then
  fail "sh fixture must be untagged"
fi

expected="fix-package-name-$sh_commit"
actual="$(get_package_version "$SH_REPO/Cargo.toml" "$SH_REPO")"
[ "$actual" = "$expected" ] \
  || fail "untagged branch should be $expected, got $actual"

SH_BIN="/bin/sh"
if [ -x "$SH_BIN" ]; then
  for case_dir in "$SH_REPO" "$REPO" "$NOGIT"; do
    expected="$(get_package_version "$case_dir/Cargo.toml" "$case_dir")"
    actual="$("$SH_BIN" -c '
      set -e
      . "$1"
      get_package_version "$2" "$3"
    ' sh "$ROOT/version.sh" "$case_dir/Cargo.toml" "$case_dir")" \
      || fail "$case_dir: resolver failed under $SH_BIN"
    [ "$actual" = "$expected" ] \
      || fail "$case_dir: $SH_BIN gave $actual, bash gave $expected"
  done

  # The untagged case must not silently fall back to the Cargo version.
  actual="$("$SH_BIN" -c '
    set -e
    . "$1"
    get_package_version "$2" "$3"
  ' sh "$ROOT/version.sh" "$SH_REPO/Cargo.toml" "$SH_REPO")"
  case "$actual" in
    *0.2.2*) fail "$SH_BIN fell back to the Cargo version: $actual" ;;
  esac
fi

echo "package version resolution: ok"
