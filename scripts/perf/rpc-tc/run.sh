#!/usr/bin/env bash
# Run a server and client over a shaped, isolated veth link.
set -euo pipefail

if [[ $# != 2 ]]; then
    echo "Usage: sudo $0 'SERVER_COMMAND' 'CLIENT_COMMAND'" >&2
    exit 2
fi
if [[ $EUID != 0 ]]; then
    echo 'Run as root (network namespaces and tc require privileges).' >&2
    exit 1
fi
for tool in ip tc; do
    command -v "$tool" >/dev/null || { echo "Missing $tool" >&2; exit 1; }
done

server_ns="cvs-$$"
client_ns="cvc-$$"
server_if="cvs$$"
client_if="cvc$$"
server_pid=''
client_pid=''
server_created=0
client_created=0
link_created=0
cleanup() {
    # Stop all processes still attached to namespaces we created.
    for ns in "$server_ns" "$client_ns"; do
        if [[ $ns == "$server_ns" && $server_created == 1 ]] || [[ $ns == "$client_ns" && $client_created == 1 ]]; then
            while read -r pid; do [[ -z $pid ]] || kill "$pid" 2>/dev/null || true; done < <(ip netns pids "$ns" 2>/dev/null)
        fi
    done
    if [[ -n $server_pid ]]; then
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    if [[ -n $client_pid ]]; then
        kill "$client_pid" 2>/dev/null || true
        wait "$client_pid" 2>/dev/null || true
    fi
    if (( server_created )); then ip netns del "$server_ns" 2>/dev/null || true; fi
    if (( client_created )); then ip netns del "$client_ns" 2>/dev/null || true; fi
    if (( link_created )); then ip link del "$server_if" 2>/dev/null || true; fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

ip netns add "$server_ns"
server_created=1
ip netns add "$client_ns"
client_created=1
ip link add "$server_if" type veth peer name "$client_if"
link_created=1
ip link set "$server_if" netns "$server_ns"
ip link set "$client_if" netns "$client_ns"
ip -n "$server_ns" addr add 192.0.2.1/24 dev "$server_if"
ip -n "$client_ns" addr add 192.0.2.2/24 dev "$client_if"
for ns in "$server_ns" "$client_ns"; do ip -n "$ns" link set lo up; done
ip -n "$server_ns" link set "$server_if" up
ip -n "$client_ns" link set "$client_if" up
# Each direction has its own egress qdisc; DELAY is one-way latency.
for pair in "$server_ns:$server_if" "$client_ns:$client_if"; do
    ip netns exec "${pair%%:*}" tc qdisc add dev "${pair#*:}" root netem \
        delay "${DELAY:-1ms}" rate "${RATE:-1gbit}"
done

export RPC_TC_SERVER_ADDR=192.0.2.1 RPC_TC_CLIENT_ADDR=192.0.2.2
ip netns exec "$server_ns" bash -c "$1" &
server_pid=$!
# An optional TCP probe provides a bounded startup wait. It opens a real connection.
if [[ -n ${RPC_TC_READY_PORT:-} ]]; then
    ready=0
    for ((attempt=0; attempt<100; attempt++)); do
        if ! kill -0 "$server_pid" 2>/dev/null; then
            echo 'Server exited before becoming ready' >&2
            wait "$server_pid" || true
            exit 1
        fi
        if ip netns exec "$client_ns" bash -c 'exec 3<>/dev/tcp/192.0.2.1/"$RPC_TC_READY_PORT"' 2>/dev/null; then
            ready=1
            break
        fi
        sleep 0.1
    done
    if (( ! ready )); then echo 'Server readiness timed out' >&2; exit 1; fi
else
    kill -0 "$server_pid"
fi
printf 'rate=%s delay_per_direction=%s\n' "${RATE:-1gbit}" "${DELAY:-1ms}" >&2
if [[ -z ${RPC_TC_RESULT_DIR:-} ]]; then
    ip netns exec "$client_ns" bash -c "$2"
    exit
fi

# The workload signals readiness only after the dataset and warmup are done.
# Snapshot all persistent server-namespace processes around the timed reads.
case ${RPC_TC_TRANSPORT:-} in orpc|grpc) ;; *) echo 'Set RPC_TC_TRANSPORT to orpc or grpc' >&2; exit 2;; esac
[[ -n ${RPC_TC_REVISION:-} ]] || { echo 'Set RPC_TC_REVISION to the measured revision' >&2; exit 2; }
command -v python3 >/dev/null || { echo 'Missing python3' >&2; exit 1; }
[[ -d $RPC_TC_RESULT_DIR ]] || { echo 'RPC_TC_RESULT_DIR must be an existing empty directory' >&2; exit 2; }
RPC_TC_MEASURE_DIR=$(cd "$RPC_TC_RESULT_DIR" && pwd -P)
for name in ready go client.json server-before.json server-after.json result.json; do
    [[ ! -e $RPC_TC_MEASURE_DIR/$name ]] || { echo "Refusing to overwrite $RPC_TC_MEASURE_DIR/$name" >&2; exit 2; }
done
export RPC_TC_MEASURE_DIR
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
ip netns exec "$client_ns" bash -c "$2" &
client_pid=$!
ready=0
for ((attempt=0; attempt<1200; attempt++)); do
    if [[ -f $RPC_TC_MEASURE_DIR/ready ]]; then ready=1; break; fi
    if ! kill -0 "$client_pid" 2>/dev/null; then
        echo 'Client exited before measurement was ready' >&2
        wait "$client_pid" || true
        exit 1
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then echo 'Server exited before measurement' >&2; exit 1; fi
    sleep 0.1
done
(( ready )) || { echo 'Client measurement readiness timed out' >&2; exit 1; }
python3 "$script_dir/measure.py" snapshot "$server_ns" "$RPC_TC_MEASURE_DIR/server-before.json"
touch "$RPC_TC_MEASURE_DIR/go"
if ! wait "$client_pid"; then echo 'Client workload failed' >&2; exit 1; fi
client_pid=''
python3 "$script_dir/measure.py" snapshot "$server_ns" "$RPC_TC_MEASURE_DIR/server-after.json"
python3 "$script_dir/measure.py" finish "$RPC_TC_MEASURE_DIR/client.json" \
    "$RPC_TC_MEASURE_DIR/server-before.json" "$RPC_TC_MEASURE_DIR/server-after.json" \
    "$RPC_TC_MEASURE_DIR/result.json" "${RATE:-1gbit}" "${DELAY:-1ms}" \
    "$RPC_TC_TRANSPORT" "$RPC_TC_REVISION"
printf 'result=%s\n' "$RPC_TC_MEASURE_DIR/result.json"
