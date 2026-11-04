#!/usr/bin/env bash
# Diagnostic only: wrap the benchmark server binary, capture its worker's
# network namespace before the worker starts, and then run the real binary.
# Never use packet capture in a performance acceptance measurement.
set -euo pipefail

: "${RPC_TC_CAPTURE_REAL_SERVER:?set to the real curvine-server binary}"
: "${RPC_TC_CAPTURE_PCAP:?set to a new absolute pcap path}"
[[ -x $RPC_TC_CAPTURE_REAL_SERVER && $RPC_TC_CAPTURE_PCAP == /* ]] || {
    echo 'Capture requires an executable real server and absolute pcap path' >&2
    exit 2
}

service=''
previous=''
for arg in "$@"; do
    if [[ $previous == --service ]]; then service=$arg; break; fi
    previous=$arg
done
if [[ $service != worker ]]; then
    exec "$RPC_TC_CAPTURE_REAL_SERVER" "$@"
fi

log="${RPC_TC_CAPTURE_PCAP}.log"
[[ ! -e $RPC_TC_CAPTURE_PCAP && ! -e $log ]] || {
    echo 'Refusing to overwrite capture or log' >&2
    exit 2
}
mapfile -t interfaces < <(ip -o -4 addr show | awk -v addr="${RPC_TC_SERVER_ADDR:-192.0.2.1}" '{ split($4, parts, "/"); if (parts[1] == addr) print $2 }')
[[ ${#interfaces[@]} == 1 ]] || { echo 'Cannot uniquely identify server veth' >&2; exit 2; }
server_if=${interfaces[0]%%@*}
tcpdump -i "$server_if" -s 0 -U -c "${RPC_TC_CAPTURE_PACKETS:-4096}" \
    -w "$RPC_TC_CAPTURE_PCAP" 'tcp port 8997' 2>"$log" &
capture_pid=$!
ready=0
for ((attempt=0; attempt<100; attempt++)); do
    if grep -q 'listening on' "$log"; then ready=1; break; fi
    if ! kill -0 "$capture_pid" 2>/dev/null; then break; fi
    sleep 0.05
done
if (( ! ready )); then
    kill "$capture_pid" 2>/dev/null || true
    wait "$capture_pid" 2>/dev/null || true
    echo "tcpdump did not become ready; see $log" >&2
    exit 1
fi
exec "$RPC_TC_CAPTURE_REAL_SERVER" "$@"
