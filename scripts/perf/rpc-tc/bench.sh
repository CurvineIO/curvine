#!/usr/bin/env bash
# End-to-end remote-read comparison run using real Curvine master and worker.
set -euo pipefail
if [[ $# != 5 ]]; then
    echo 'Usage: sudo bench.sh orpc|grpc SERVER_BINARY CLIENT_BINARY REVISION RESULT_DIRECTORY' >&2
    exit 2
fi
[[ $EUID == 0 ]] || { echo 'Run as root (network namespaces and tc require privileges).' >&2; exit 1; }
case $1 in orpc|grpc) transport=$1;; *) echo 'First argument must be orpc or grpc' >&2; exit 2;; esac
[[ -x $2 && -x $3 ]] || { echo 'Server and client binaries must be executable' >&2; exit 2; }
[[ -n $4 ]] || { echo 'Revision must be nonempty' >&2; exit 2; }
server=$(realpath "$2")
client=$(realpath "$3")
mkdir -p "$5"
result_dir=$(cd "$5" && pwd -P)
[[ ! -e $result_dir/cluster && ! -e $result_dir/config.toml && ! -e $result_dir/server.log ]] || {
    echo 'Result directory has existing benchmark data; use a fresh directory' >&2; exit 2;
}
mkdir "$result_dir/cluster"
# The unique result directory owns all data that format_master/worker may remove.
# Both servers share this config and run on the server side of the veth pair.
python3 - "$result_dir" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
q = lambda value: json.dumps(str(value), ensure_ascii=True)
config = f'''format_master = true
format_worker = true
[master]
hostname = "192.0.2.1"
rpc_port = 8995
meta_dir = {q(root / "cluster" / "meta")}
log = {{ level = "error", log_dir = "stdout" }}
[journal]
enable = false
hostname = "192.0.2.1"
journal_addrs = [{{ id = 1, hostname = "192.0.2.1", port = 8996 }}]
journal_dir = {q(root / "cluster" / "journal")}
[worker]
hostname = "192.0.2.1"
rpc_port = 8997
dir_reserved = "0"
data_dir = [{q("[DISK]" + str(root / "cluster" / "data"))}]
log = {{ level = "error", log_dir = "stdout" }}
[client]
hostname = "192.0.2.2"
master_addrs = [{{ hostname = "192.0.2.1", port = 8995 }}]
'''
(root / "config.toml").write_text(config, encoding="utf-8")
PY
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
unset CURVINE_MASTER_HOSTNAME CURVINE_WORKER_HOSTNAME CURVINE_CLIENT_HOSTNAME \
    CURVINE_MDS_HOSTNAME CURVINE_TRANSFER_HOSTNAME ORPC_BIND_HOSTNAME
printf -v server_command 'exec %q %q %q >> %q 2>&1' "$script_dir/server.sh" "$server" "$result_dir/config.toml" "$result_dir/server.log"
printf -v client_command 'exec %q' "$client"
export RPC_TC_CONF="$result_dir/config.toml" RPC_TC_RESULT_DIR="$result_dir"
export RPC_TC_TRANSPORT="$transport" RPC_TC_REVISION="$4" RPC_TC_READY_PORT=8997
RPC_TC_SERVER_SHA256=$(sha256sum "$server" | cut -d' ' -f1)
RPC_TC_CLIENT_SHA256=$(sha256sum "$client" | cut -d' ' -f1)
export RPC_TC_SERVER_SHA256 RPC_TC_CLIENT_SHA256
exec "$script_dir/run.sh" "$server_command" "$client_command"
