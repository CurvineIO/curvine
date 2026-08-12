#!/usr/bin/env bash
set -euo pipefail

CURVINE_HOME="${CURVINE_HOME:-/app/curvine}"
CURVINE_CONF_FILE="${CURVINE_CONF_FILE:-${CURVINE_HOME}/conf/curvine-cluster.toml}"
CV_BIN="${CURVINE_CLI:-${CURVINE_HOME}/bin/cv}"

if [ ! -x "$CV_BIN" ]; then
  echo "Curvine CLI not found or not executable: $CV_BIN" >&2
  exit 1
fi

exec "$CV_BIN" report fluid-summary --conf "$CURVINE_CONF_FILE"
