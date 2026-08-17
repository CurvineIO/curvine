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

# Release version consistency gate.
#
# The git tag (vX.Y.Z[-pre]) is the release anchor. Before artifacts are
# built and published, the tag must match the version declared in the Cargo
# workspace ([workspace.package].version). Java pom and Python pyproject
# versions are injected from the tag by the release CI, so they are optional
# checks here: --pom requires them to already equal the release version while
# --pyproject accepts a dynamic version that inherits the workspace.
#
# Usage:
#   build/validate-release-version.sh <tag> [options]
#
# Options:
#   --cargo PATH       Cargo.toml to check (default: <repo root>/Cargo.toml)
#   --pom PATH         also require the Java pom.xml project version to match
#   --pyproject PATH   also check the Python pyproject.toml version
#   --dry-run          print the injection plan (what the release CI would
#                      apply) without modifying any file
#
# Exit codes: 0 = consistent, 1 = mismatch or invalid tag, 2 = usage/argument
# error or missing input file.

set -euo pipefail

FS_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$FS_HOME/build/version.sh"

CARGO_TOML="${CARGO_TOML:-$FS_HOME/Cargo.toml}"
POM_PATH=""
PYPROJECT_PATH=""
DRY_RUN=0

usage() {
  sed -n 's/^# \{0,1\}//p' "${BASH_SOURCE[0]}" | sed -n '/^Usage:/,/^Exit codes:/p'
}

while [ $# -gt 0 ]; do
  case "$1" in
    --cargo)
      CARGO_TOML="${2:?--cargo requires a path}"
      shift 2
      ;;
    --cargo=*)
      CARGO_TOML="${1#*=}"
      shift
      ;;
    --pom)
      POM_PATH="${2:?--pom requires a path}"
      shift 2
      ;;
    --pom=*)
      POM_PATH="${1#*=}"
      shift
      ;;
    --pyproject)
      PYPROJECT_PATH="${2:?--pyproject requires a path}"
      shift 2
      ;;
    --pyproject=*)
      PYPROJECT_PATH="${1#*=}"
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    -*)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
    *)
      if [ -n "${TAG:-}" ]; then
        echo "unexpected positional argument: $1" >&2
        usage >&2
        exit 2
      fi
      TAG="$1"
      shift
      ;;
  esac
done

if [ -z "${TAG:-}" ]; then
  echo "error: a release tag is required (e.g. v0.4.0-alpha)" >&2
  usage >&2
  exit 2
fi

# --- tag shape -------------------------------------------------------------
if [[ ! "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "error: invalid release tag '$TAG' (expected vX.Y.Z[-pre], e.g. v0.4.0-alpha)" >&2
  exit 1
fi

RELEASE_VERSION="$(tag_to_release_version "$TAG")"
failures=0

# --- Cargo workspace version (primary, hand-maintained source) ------------
if [ ! -f "$CARGO_TOML" ]; then
  echo "error: Cargo.toml not found: $CARGO_TOML" >&2
  exit 2
fi

CARGO_VERSION="$(get_workspace_version "$CARGO_TOML")"
if [ -z "$CARGO_VERSION" ]; then
  echo "error: could not read [workspace.package].version from $CARGO_TOML" >&2
  exit 1
fi

if [ "$CARGO_VERSION" != "$RELEASE_VERSION" ]; then
  echo "FAIL tag/Cargo mismatch: tag=$TAG, release version=$RELEASE_VERSION, Cargo version=$CARGO_VERSION" >&2
  failures=1
fi

# --- Java pom (optional; injected by the release CI) -----------------------
POM_VERSION=""
if [ -n "$POM_PATH" ]; then
  if [ ! -f "$POM_PATH" ]; then
    echo "error: pom.xml not found: $POM_PATH" >&2
    exit 2
  fi
  POM_VERSION="$(get_pom_version "$POM_PATH")"
  if [ "$POM_VERSION" != "$RELEASE_VERSION" ]; then
    echo "FAIL tag/pom mismatch: release version=$RELEASE_VERSION, pom version=$POM_VERSION" >&2
    failures=1
  fi
fi

# --- Python pyproject (optional; dynamic inherits the workspace) -----------
PYTHON_VERSION=""
if [ -n "$PYPROJECT_PATH" ]; then
  if [ ! -f "$PYPROJECT_PATH" ]; then
    echo "error: pyproject.toml not found: $PYPROJECT_PATH" >&2
    exit 2
  fi
  PYTHON_VERSION="$(get_python_version "$PYPROJECT_PATH")"
  if [ -z "$PYTHON_VERSION" ]; then
    echo "note: pyproject version is dynamic, wheel version inherits the Cargo workspace version" >&2
  elif [ "$PYTHON_VERSION" != "$RELEASE_VERSION" ]; then
    echo "FAIL tag/python mismatch: release version=$RELEASE_VERSION, pyproject version=$PYTHON_VERSION" >&2
    failures=1
  fi
fi

if [ "$failures" -ne 0 ]; then
  echo "release version validation failed for tag $TAG" >&2
  exit 1
fi

echo "PASS release tag $TAG is consistent (release version $RELEASE_VERSION)"

# --- dry-run: show what the release CI will inject -------------------------
if [ "$DRY_RUN" -eq 1 ]; then
  echo
  echo "Injection plan (dry-run, no files changed):"
  echo "  BUILD_VERSION=$RELEASE_VERSION"
  echo "    -> binaries (curvine-sys build.rs), build-version file"
  echo "  set_workspace_version $CARGO_TOML $RELEASE_VERSION"
  echo "    -> Python SDK wheel version (maturin dynamic version)"
  if [ -n "$POM_PATH" ]; then
    echo "  set_pom_version $POM_PATH $RELEASE_VERSION"
    echo "    -> Java SDK jar version"
  fi
fi