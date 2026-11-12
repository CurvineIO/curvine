#!/bin/bash

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

BIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"; pwd)"
MNT_PATH=/curvine-fuse

is_mountpoint() {
    local path="$1"
    local mountinfo="${2:-/proc/self/mountinfo}"
    local mount_id parent major_minor root mount_path mount_options

    while read -r mount_id parent major_minor root mount_path mount_options; do
        # Linux mountinfo escapes path whitespace and backslashes as octal
        # sequences. Decode in-place so trailing newlines remain lossless.
        mount_path=${mount_path//\\040/ }
        mount_path=${mount_path//\\011/$'\t'}
        mount_path=${mount_path//\\012/$'\n'}
        mount_path=${mount_path//\\134/$'\\'}
        if [[ "$mount_path" == "$path" ]]; then
            return 0
        fi
    done < "$mountinfo"

    return 1
}

prepare_mount_path() {
    if is_mountpoint "$MNT_PATH"; then
        echo "Refusing to touch mounted path ${MNT_PATH}. Stop or unmount the existing FUSE mount first." >&2
        return 1
    fi

    if [ -e "$MNT_PATH" ] && [ ! -d "$MNT_PATH" ]; then
        echo "FUSE mount path exists but is not a directory: ${MNT_PATH}" >&2
        return 1
    fi

    mkdir -p "$MNT_PATH"
}

# One mount per process: the systemd unit only cleans up MNT_PATH itself, not
# the mnt-0, mnt-1, ... submounts created by fuse.mnt_number > 1.
reject_multiple_mounts() {
    local i arg value
    for ((i = 0; i < ${#PARAMS[@]}; i++)); do
        arg=${PARAMS[$i]}
        if [[ "$arg" == --mnt-number=* ]]; then
            value="${arg#*=}"
        elif [[ "$arg" == "--mnt-number" ]]; then
            value=${PARAMS[$((i + 1))]:-}
        else
            continue
        fi
        if [[ "$value" != "1" ]]; then
            echo "run supports a single mount, got --mnt-number ${value}" >&2
            return 1
        fi
    done
}

main() {
    local arg
    MNT_PATH=/curvine-fuse

    for arg in "$@"; do
        if [[ "$arg" == "--help" ]] || [[ "$arg" == "-h" ]]; then
            . "${BIN_DIR}/../conf/curvine-env.sh"
            "${CURVINE_HOME}/lib/curvine-fuse" --help
            exit 0
        fi
        if [[ "$arg" == "--version-json" ]] || [[ "$arg" == "--version" ]] || [[ "$arg" == "-V" ]]; then
            . "${BIN_DIR}/../conf/curvine-env.sh"
            "${CURVINE_HOME}/lib/curvine-fuse" "$arg"
            exit 0
        fi
    done

    ACTION=${1:-}
    shift || true
    PARAMS=("$@")

    local i
    for ((i = 0; i < ${#PARAMS[@]}; i++)); do
        arg=${PARAMS[$i]}
        if [[ "$arg" == --mnt-path=* ]]; then
            MNT_PATH="${arg#*=}"
        elif [[ "$arg" == "--mnt-path" ]] && (( i + 1 < ${#PARAMS[@]} )); then
            MNT_PATH=${PARAMS[$((i + 1))]}
        fi
    done

    case "$ACTION" in
        start)
            prepare_mount_path || exit 1
            echo "Starting curvine-fuse with arguments: ${PARAMS[*]}"
            exec "${BIN_DIR}/launch-process.sh" fuse start "${PARAMS[@]}"
            ;;
        restart)
            "${BIN_DIR}/launch-process.sh" fuse stop "${PARAMS[@]}" || exit $?
            sleep 1
            prepare_mount_path || exit 1
            echo "Starting curvine-fuse with arguments: ${PARAMS[*]}"
            exec "${BIN_DIR}/launch-process.sh" fuse start "${PARAMS[@]}"
            ;;
        run)
            # Foreground mode used by the systemd units (systemd/README.md).
            reject_multiple_mounts || exit 1
            prepare_mount_path || exit 1
            # Tells the unit's ExecStopPost= that the path is ours to detach.
            if [ -n "${RUNTIME_DIRECTORY:-}" ]; then
                : > "${RUNTIME_DIRECTORY}/mount-path-ready" || exit 1
            fi
            exec "${BIN_DIR}/launch-process.sh" fuse run "${PARAMS[@]}"
            ;;
        stop|reload)
            exec "${BIN_DIR}/launch-process.sh" fuse "$ACTION" "${PARAMS[@]}"
            ;;
        *)
            echo "Usage: $0 [start|stop|restart|reload|run] [FUSE options]" >&2
            exit 1
            ;;
    esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
