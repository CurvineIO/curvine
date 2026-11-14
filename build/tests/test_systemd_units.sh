#!/usr/bin/env bash

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

# Checks the foreground "run" action of the launch scripts and the systemd unit
# templates rendered by curvine-systemd.sh, against a staged dist layout with
# stub binaries. Needs no root and no running systemd.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
# The scripts default the configuration file from CURVINE_HOME only when unset.
unset CURVINE_CONF_FILE

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

# Stage the dist layout produced by build.sh: bin/, conf/, lib/, systemd/.
stage_home() {
    local home=$1
    mkdir -p "$home/bin" "$home/conf" "$home/lib" "$home/systemd"
    cp "$ROOT"/build/bin/* "$home/bin/"
    chmod +x "$home"/bin/*
    cp "$ROOT/etc/curvine-env.sh" "$home/conf/"
    cp -R "$ROOT/build/systemd/." "$home/systemd/"
    # Stub binaries report their PID and arguments, one per line.
    local bin
    for bin in curvine-server curvine-fuse; do
        cat > "$home/lib/$bin" <<'EOF'
#!/usr/bin/env bash
echo "pid=$$"
printf 'arg=%s\n' "$@"
EOF
        chmod +x "$home/lib/$bin"
    done
}

HOME_DIR="$WORK/curvine"
stage_home "$HOME_DIR"

# run must exec the binary in place of the script: same PID, conf appended.
out=$(bash -c 'echo "shell=$$"; exec "$1" run' _ "$HOME_DIR/bin/curvine-master.sh")
shell_pid=$(sed -n 's/^shell=//p' <<< "$out")
[[ "$(sed -n 's/^pid=//p' <<< "$out")" == "$shell_pid" ]] || fail "master run did not exec the server: $out"
grep -qx 'arg=--service' <<< "$out" || fail "master run lost --service: $out"
grep -qx 'arg=master' <<< "$out" || fail "master run lost the service name: $out"
grep -qx "arg=$HOME_DIR/conf/curvine-cluster.toml" <<< "$out" || fail "master run lost --conf: $out"
grep -qx 'arg=worker' <<< "$("$HOME_DIR/bin/curvine-worker.sh" run)" || fail "worker run did not start the worker"
[[ ! -e "$HOME_DIR/master.pid" ]] || fail "run must not write a PID file"

# The FUSE run action prepares the mount path and passes the options through.
MNT="$WORK/mnt/fuse"
out=$(bash -c 'echo "shell=$$"; exec "$1" run --mnt-path "$2" -d' _ "$HOME_DIR/bin/curvine-fuse.sh" "$MNT")
shell_pid=$(sed -n 's/^shell=//p' <<< "$out")
[[ "$(sed -n 's/^pid=//p' <<< "$out")" == "$shell_pid" ]] || fail "fuse run did not exec curvine-fuse: $out"
[[ -d "$MNT" ]] || fail "fuse run did not create the mount path"
grep -qx "arg=$MNT" <<< "$out" || fail "fuse run lost --mnt-path: $out"
grep -qx 'arg=-d' <<< "$out" || fail "fuse run lost extra options: $out"
grep -qx "arg=$HOME_DIR/conf/curvine-cluster.toml" <<< "$out" || fail "fuse run did not default --conf: $out"
mkdir -p "$WORK/rt"
RUNTIME_DIRECTORY="$WORK/rt" "$HOME_DIR/bin/curvine-fuse.sh" run --mnt-path "$MNT" --mnt-number 1 > /dev/null
[[ -e "$WORK/rt/mount-path-ready" ]] || fail "fuse run did not leave the mount marker"
# Several mounts per unit are rejected before anything is prepared.
for bad in "--mnt-number 2" "--mnt-number=2"; do
    rm -f "$WORK/rt/mount-path-ready"
    # shellcheck disable=SC2086
    if RUNTIME_DIRECTORY="$WORK/rt" "$HOME_DIR/bin/curvine-fuse.sh" run --mnt-path "$MNT" $bad > /dev/null 2>&1; then
        fail "fuse run accepted $bad"
    fi
    [[ ! -e "$WORK/rt/mount-path-ready" ]] || fail "rejected start left the mount marker ($bad)"
done

# Render the units into a staging directory.
UNIT_DIR="$WORK/units"
"$HOME_DIR/bin/curvine-systemd.sh" install --user "$(id -un)" --unit-dir "$UNIT_DIR" > /dev/null
for unit in curvine-master.service curvine-worker.service curvine-fuse@.service; do
    file="$UNIT_DIR/$unit"
    [[ -f "$file" ]] || fail "$unit was not rendered"
    if grep -n '@CURVINE_[A-Z]*@' "$file"; then
        fail "$unit still has template placeholders"
    fi
    grep -qx "User=$(id -un)" "$file" || fail "$unit has the wrong User="
    grep -qx "Group=$(id -gn)" "$file" || fail "$unit has the wrong Group="
    grep -qx "WorkingDirectory=$HOME_DIR" "$file" || fail "$unit has the wrong WorkingDirectory="
done
grep -qx "ExecStart=$HOME_DIR/bin/curvine-master.sh run" "$UNIT_DIR/curvine-master.service" \
    || fail "master unit has the wrong ExecStart="
grep -qx "ExecStart=$HOME_DIR/bin/curvine-worker.sh run" "$UNIT_DIR/curvine-worker.service" \
    || fail "worker unit has the wrong ExecStart="
grep -qx "ExecStart=$HOME_DIR/bin/curvine-fuse.sh run --mnt-path %f --mnt-number 1 \$CURVINE_FUSE_OPTS" "$UNIT_DIR/curvine-fuse@.service" \
    || fail "fuse unit has the wrong ExecStart="
grep -q '^ExecStopPost=.*mount-path-ready' "$UNIT_DIR/curvine-fuse@.service" \
    || fail "fuse unit cleanup is not gated on the mount marker"

if command -v systemd-analyze > /dev/null 2>&1; then
    # verify exits 0 on unknown keys or bad values and only warns, so any output
    # about the rendered units is a failure. It also warns about unrelated units
    # on the host, so only lines that mention the three units are considered.
    # The FUSE template is checked through an instance.
    verify_out=$(systemd-analyze verify "$UNIT_DIR/curvine-master.service" "$UNIT_DIR/curvine-worker.service" \
        "$UNIT_DIR/curvine-fuse@curvine\\x2dfuse.service" 2>&1 || true)
    verify_out=$(grep -E 'curvine-(master|worker)\.service|curvine-fuse@' <<< "$verify_out" || true)
    [[ -z "$verify_out" ]] || fail "systemd-analyze verify reported problems: $verify_out"
fi

"$HOME_DIR/bin/curvine-systemd.sh" uninstall --unit-dir "$UNIT_DIR" > /dev/null
[[ -z "$(ls -A "$UNIT_DIR")" ]] || fail "uninstall left unit files behind"

# Paths systemd would split or interpret are rejected.
SPACE_HOME="$WORK/with space"
stage_home "$SPACE_HOME"
if "$SPACE_HOME/bin/curvine-systemd.sh" install --unit-dir "$WORK/space-units" 2> /dev/null; then
    fail "install accepted a CURVINE_HOME with whitespace"
fi
[[ ! -e "$WORK/space-units/curvine-master.service" ]] || fail "rejected install still wrote units"

echo "curvine systemd units: ok"
