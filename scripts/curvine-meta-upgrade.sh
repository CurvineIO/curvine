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

# Offline conversion of master metadata written by a pre-codec build.
#
# Runs the CLI as `curvine-cli meta-upgrade ...`, so it behaves exactly like any
# other subcommand. The subcommand is compiled into the CLI unconditionally:
#
#   cargo build -p curvine-cli --release
#
# Usage:
#   scripts/curvine-meta-upgrade.sh --out-dir /data/curvine-upgrade --dry-run
#   scripts/curvine-meta-upgrade.sh --out-dir /data/curvine-upgrade
#
# The source directories come from the cluster config, so only --out-dir is
# normally needed. Stop the whole cluster first; the sources are only read.
#
# The converted journal snapshot is rewritten to restore from the configured
# master.meta_dir (and to name this member), so install the output at the
# configured directories by rename, backing up the live ones first:
#   mv $META_DIR $META_DIR.bak;     mv $OUT/meta $META_DIR
#   mv $JOURNAL_DIR $JOURNAL_DIR.bak; mv $OUT/journal $JOURNAL_DIR

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CV_BIN="${CURVINE_CLI:-}"
if [[ -z "${CV_BIN}" ]]; then
    if [[ -n "${CURVINE_HOME:-}" && -x "${CURVINE_HOME}/bin/cv" ]]; then
        CV_BIN="${CURVINE_HOME}/bin/cv"
    else
        CV_BIN="${SCRIPT_DIR}/../target/release/curvine-cli"
        [[ -x "${CV_BIN}" ]] || CV_BIN="${SCRIPT_DIR}/../target/debug/curvine-cli"
    fi
fi

if [[ ! -x "${CV_BIN}" ]]; then
    echo "curvine CLI not found: ${CV_BIN}" >&2
    echo "build it with: cargo build -p curvine-cli --release" >&2
    exit 1
fi

exec "${CV_BIN}" meta-upgrade "$@"
