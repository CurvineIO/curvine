#!/usr/bin/env bash
# Run the real master and worker in the server namespace.
set -euo pipefail
if [[ $# != 2 ]]; then echo 'Usage: server.sh /absolute/path/curvine-server /absolute/path/config.toml' >&2; exit 2; fi
server=$1
conf=$2
master=''
worker=''
cleanup() {
    [[ -z $worker ]] || kill "$worker" 2>/dev/null || true
    [[ -z $master ]] || kill "$master" 2>/dev/null || true
    [[ -z $worker ]] || wait "$worker" 2>/dev/null || true
    [[ -z $master ]] || wait "$master" 2>/dev/null || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
"$server" --service master --conf "$conf" &
master=$!
"$server" --service worker --conf "$conf" &
worker=$!
# Either service exiting makes the measurement invalid.
set +e
wait -n "$master" "$worker"
status=$?
set -e
echo "master or worker exited during benchmark (status $status)" >&2
exit 1
