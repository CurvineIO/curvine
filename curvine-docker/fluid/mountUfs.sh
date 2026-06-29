#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="${FLUID_RUNTIME_CONFIG_PATH:-/etc/fluid/config/config.json}"
CURVINE_HOME="${CURVINE_HOME:-/app/curvine}"
CURVINE_CONF_FILE="${CURVINE_CONF_FILE:-${CURVINE_HOME}/conf/curvine-cluster.toml}"
CV_BIN="${CURVINE_CLI:-${CURVINE_HOME}/bin/cv}"

log() {
  echo "$@" >&2
}

if [ ! -f "$CONFIG_PATH" ]; then
  log "Fluid runtime config not found: $CONFIG_PATH"
  printf '{"mounted":[]}\n'
  exit 0
fi

python3 - "$CONFIG_PATH" "$CV_BIN" "$CURVINE_CONF_FILE" <<'PY'
import json
import os
import shutil
import subprocess
import sys

config_path, cv_bin, conf_file = sys.argv[1:4]


def eprint(*args):
    print(*args, file=sys.stderr)


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        content = f.read().strip()
    if not content:
        return {}
    data = json.loads(content)
    if isinstance(data, str):
        data = json.loads(data)
    if not isinstance(data, dict):
        raise ValueError(f"runtime config must be a JSON object, got {type(data).__name__}")
    return data


def read_secret(value):
    if value and os.path.isfile(value):
        with open(value, "r", encoding="utf-8") as f:
            return f.read().strip()
    return value


def options_for_mount(mount):
    options = dict(mount.get("options") or {})
    encrypt_options = mount.get("encryptOptions") or {}

    if isinstance(encrypt_options, list):
        for item in encrypt_options:
            name = item.get("name")
            value_from = item.get("valueFrom") or {}
            secret_key_ref = value_from.get("secretKeyRef") or {}
            if name and secret_key_ref.get("path"):
                encrypt_options[name] = secret_key_ref["path"]
    if isinstance(encrypt_options, dict):
        for key, value in encrypt_options.items():
            options[key] = read_secret(value)

    params = []
    key_map = {
        "endpoint_url": "s3.endpoint_url",
        "region_name": "s3.region_name",
        "path_style": "s3.force.path.style",
        "access": "s3.credentials.access",
        "secret": "s3.credentials.secret",
        "access-key": "s3.credentials.access",
        "secret-key": "s3.credentials.secret",
    }
    for key, conf_key in key_map.items():
        value = options.get(key)
        if value:
            params.extend(["-c", f"{conf_key}={value}"])
    return params


def cv_path_for_mount(mount):
    path = mount.get("path")
    if path:
        return path if path.startswith("/") else f"/{path}"
    name = mount.get("name")
    if name:
        return name if str(name).startswith("/") else f"/{name}"
    mount_point = mount.get("mountPoint") or ""
    if "://" in mount_point:
        suffix = mount_point.split("://", 1)[1].split("/", 1)
        if len(suffix) == 2 and suffix[1]:
            return f"/{suffix[1].strip('/')}"
    return "/"


def is_curvine_native_mount(mount_point):
    return str(mount_point).startswith(("curvine://", "curvinefs://"))


def run_cv(args):
    cmd = [cv_bin, *args, "--conf", conf_file]
    eprint("Running:", " ".join(cmd))
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def parse_mounted_paths(output):
    paths = []
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        cols = [col.strip() for col in stripped.strip("|").split("|")]
        if len(cols) < 2 or cols[0] == "ID" or cols[1] == "Curvine Path":
            continue
        if cols[1]:
            paths.append(cols[1])
    return paths


config = load_config(config_path)
mounts = config.get("mounts") or []
mounted = []

if not shutil.which(cv_bin) and not os.path.exists(cv_bin):
    for mount in mounts:
        mount_point = mount.get("mountPoint")
        if mount_point:
            if is_curvine_native_mount(mount_point):
                mounted.append(cv_path_for_mount(mount))
            else:
                mounted.append(mount_point)
    print(json.dumps({"mounted": mounted}, separators=(",", ":")))
    sys.exit(0)

for mount in mounts:
    mount_point = mount.get("mountPoint")
    if not mount_point:
        eprint("Skipping mount without mountPoint:", mount)
        continue
    cv_path = cv_path_for_mount(mount)
    if is_curvine_native_mount(mount_point):
        eprint(f"Skipping native Curvine mountPoint {mount_point}; reporting {cv_path}")
        mounted.append(cv_path)
        continue
    params = options_for_mount(mount)

    current = run_cv(["mount"])
    if current.returncode == 0 and (mount_point in current.stdout or cv_path in current.stdout):
        mounted.append(cv_path)
        continue

    command = [
        "mount",
        mount_point,
        cv_path,
        "--check-path-consist",
        "false",
        *params,
    ]
    result = run_cv(command)
    if result.returncode != 0:
        eprint(result.stdout)
        eprint(result.stderr)
        raise SystemExit(result.returncode)
    mounted.append(cv_path)

current = run_cv(["mount"])
if current.returncode == 0:
    mounted = parse_mounted_paths(current.stdout) or mounted
elif current.stderr:
    eprint(current.stderr)

print(json.dumps({"mounted": mounted}, separators=(",", ":")))
PY
