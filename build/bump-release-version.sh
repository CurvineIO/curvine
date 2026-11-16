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

# Release version bump helper.
#
# The release tag (vX.Y.Z[-pre]) is the release anchor. This script moves the
# hand-maintained version declarations to the release version so the tag can be
# created from the bump commit and the release CI consistency gate passes.
#
# Usage:
#   build/bump-release-version.sh <version|vX.Y.Z[-pre]> [options]
#
# Options:
#   --cargo PATH   Cargo.toml to update (default: <repo root>/Cargo.toml)
#   --pom PATH     Java pom.xml to update (default:
#                  <repo root>/curvine-libsdk/java/pom.xml)
#   --skip-pom     do not update the Java pom
#   --skip-lock    do not refresh Cargo.lock
#   --dry-run      print the plan without modifying files
#
# Exit codes: 0 = updated or already current, 1 = invalid version or
# verification failure, 2 = usage error or missing input file.

set -euo pipefail

FS_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$FS_HOME/build/version.sh"

CARGO_TOML="$FS_HOME/Cargo.toml"
POM_PATH="$FS_HOME/curvine-libsdk/java/pom.xml"
SKIP_POM=0
SKIP_LOCK=0
DRY_RUN=0
VERSION=""

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
    --skip-pom)
      SKIP_POM=1
      shift
      ;;
    --skip-lock)
      SKIP_LOCK=1
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
      if [ -n "$VERSION" ]; then
        echo "unexpected positional argument: $1" >&2
        usage >&2
        exit 2
      fi
      VERSION="$1"
      shift
      ;;
  esac
done

if [ -z "$VERSION" ]; then
  echo "error: a release version is required (e.g. 0.6.1-alpha)" >&2
  usage >&2
  exit 2
fi

VERSION="$(normalize_release_version "$VERSION")"

if [ ! -f "$CARGO_TOML" ]; then
  echo "error: Cargo.toml not found: $CARGO_TOML" >&2
  exit 2
fi

if [ "$SKIP_POM" -eq 0 ] && [ ! -f "$POM_PATH" ]; then
  echo "error: pom.xml not found: $POM_PATH (pass --skip-pom to skip it)" >&2
  exit 2
fi

LOCK_PATH="$(dirname "$CARGO_TOML")/Cargo.lock"
if [ "$SKIP_LOCK" -eq 0 ] && [ ! -f "$LOCK_PATH" ]; then
  echo "error: Cargo.lock not found: $LOCK_PATH (pass --skip-lock to skip it)" >&2
  exit 2
fi

CURRENT_VERSION="$(get_workspace_version "$CARGO_TOML")"
if [ -z "$CURRENT_VERSION" ]; then
  echo "error: could not read [workspace.package].version from $CARGO_TOML" >&2
  exit 1
fi

CURRENT_POM_VERSION=""
if [ "$SKIP_POM" -eq 0 ]; then
  CURRENT_POM_VERSION="$(get_pom_version "$POM_PATH")"
  if [ -z "$CURRENT_POM_VERSION" ]; then
    echo "error: could not read the project version from $POM_PATH" >&2
    exit 1
  fi
fi

if [ "$DRY_RUN" -eq 1 ]; then
  echo "Release version bump plan (dry-run, no files changed):"
  echo "  Cargo.toml: $CURRENT_VERSION -> $VERSION"
  if [ "$SKIP_LOCK" -eq 0 ]; then
    echo "  Cargo.lock: workspace package versions -> $VERSION"
  fi
  if [ "$SKIP_POM" -eq 0 ]; then
    echo "  $POM_PATH: $CURRENT_POM_VERSION -> $VERSION"
  fi
  exit 0
fi

set_workspace_version "$CARGO_TOML" "$VERSION"

if [ "$SKIP_POM" -eq 0 ]; then
  set_pom_version "$POM_PATH" "$VERSION"
fi

if [ "$SKIP_LOCK" -eq 0 ]; then
  set_lockfile_workspace_version "$CARGO_TOML" "$LOCK_PATH" \
    "$CURRENT_VERSION" "$VERSION"
fi

VALIDATE_ARGS=("v$VERSION" "--cargo" "$CARGO_TOML")
if [ "$SKIP_POM" -eq 0 ]; then
  VALIDATE_ARGS+=("--pom" "$POM_PATH")
fi
bash "$FS_HOME/build/validate-release-version.sh" "${VALIDATE_ARGS[@]}"

echo "Release version applied:"
if [ "$CURRENT_VERSION" = "$VERSION" ]; then
  echo "  Cargo.toml: already at $VERSION"
else
  echo "  Cargo.toml: $CURRENT_VERSION -> $VERSION"
fi
if [ "$SKIP_LOCK" -eq 0 ]; then
  echo "  Cargo.lock: workspace package versions -> $VERSION"
fi
if [ "$SKIP_POM" -eq 0 ]; then
  if [ "$CURRENT_POM_VERSION" = "$VERSION" ]; then
    echo "  $POM_PATH: already at $VERSION"
  else
    echo "  $POM_PATH: $CURRENT_POM_VERSION -> $VERSION"
  fi
fi
