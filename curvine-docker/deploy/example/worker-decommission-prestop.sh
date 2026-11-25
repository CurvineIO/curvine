#!/bin/sh
set -eu

master_web="${CURVINE_MASTER_WEB:?set CURVINE_MASTER_WEB to the leader-aware Master web host:port}"
host="${CURVINE_DRAIN_HOST:-${HOSTNAME:?set CURVINE_DRAIN_HOST to the Worker registration hostname}}"
timeout_seconds="${CURVINE_DRAIN_TIMEOUT_SECONDS:-1500}"
poll_seconds="${CURVINE_DRAIN_POLL_SECONDS:-10}"
case "$timeout_seconds" in
  ''|*[!0-9]*) echo "CURVINE_DRAIN_TIMEOUT_SECONDS must be an integer" >&2; exit 2 ;;
esac
case "$poll_seconds" in
  ''|*[!0-9]*) echo "CURVINE_DRAIN_POLL_SECONDS must be an integer" >&2; exit 2 ;;
esac
if [ "$timeout_seconds" -le 0 ] || [ "$poll_seconds" -le 0 ]; then
  echo "drain timeout and poll interval must be positive" >&2
  exit 2
fi
deadline=$(( $(date +%s) + timeout_seconds ))

remaining_seconds() {
  remaining=$(( deadline - $(date +%s) ))
  [ "$remaining" -gt 0 ] || return 1
  printf '%s' "$remaining"
}

url="http://${master_web}"
echo "requesting Curvine decommission for ${host}"
remaining=$(remaining_seconds) || {
  echo "drain deadline expired before committing decommission intent" >&2
  exit 1
}
if ! curl --max-time "$remaining" --fail --silent --show-error --get "${url}/add-dcm" \
    --data-urlencode "workers=${host}" >/dev/null; then
  echo "could not commit decommission intent; keep the Worker in service" >&2
  exit 1
fi

while :; do
  remaining=$(remaining_seconds) || {
    echo "${host}: drain timeout; do not treat this Pod as safely drained" >&2
    exit 1
  }
  body=$(curl --max-time "$remaining" --fail --silent --show-error --get "${url}/api/decommission" \
    --data-urlencode "host=${host}") || {
      echo "could not read drain status; termination is not confirmed safe" >&2
      exit 1
    }
  # A response that arrived at or after the deadline is never accepted as safe.
  remaining=$(remaining_seconds) || {
    echo "${host}: drain timeout; do not treat this Pod as safely drained" >&2
    exit 1
  }
  state=$(printf '%s' "$body" | jq -r '.[0].state // "Unknown"')
  echo "${host}: state=${state}"
  if [ "$state" = Drained ]; then
    echo "${host}: drain completed; termination may proceed"
    exit 0
  fi
  if [ "$state" = Blocked ] || [ "$state" = Disabled ] || [ "$state" = Paused ]; then
    echo "${host}: state=${state}; waiting without declaring it safe"
  fi
  remaining=$(remaining_seconds) || {
    echo "${host}: drain timeout; do not treat this Pod as safely drained" >&2
    exit 1
  }
  sleep_for="$poll_seconds"
  if [ "$sleep_for" -gt "$remaining" ]; then
    sleep_for="$remaining"
  fi
  sleep "$sleep_for"
done
