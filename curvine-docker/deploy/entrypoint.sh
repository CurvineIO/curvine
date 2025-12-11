#!/bin/bash

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

set -euo pipefail

# Start the curvine service process
# Service type: master, worker
SERVER_TYPE=${1:-master}

# Operation type: start, stop, restart
ACTION_TYPE=${2:-start}

echo "SERVER_TYPE: $SERVER_TYPE, ACTION_TYPE: $ACTION_TYPE"

set_hosts() {
  # Print pod info for debugging (Kubernetes CoreDNS handles DNS resolution, no need to modify /etc/hosts)
  echo "POD_IP: ${POD_IP:-}"
  echo "POD_NAMESPACE: ${POD_NAMESPACE:-}"
  echo "POD_CLUSTER_DOMAIN: ${POD_CLUSTER_DOMAIN:-}"
}

# Container-specific service management logic
# This avoids modifying launch-process.sh which is used in non-container environments

start_service() {
  local SERVICE_NAME=$1

  if [ -z "$CURVINE_CONF_FILE" ]; then
    export CURVINE_CONF_FILE=${CURVINE_HOME}/conf/curvine-cluster.toml
  fi

  local LOG_DIR=${CURVINE_HOME}/logs
  local OUT_FILE=${LOG_DIR}/${SERVICE_NAME}.out
  mkdir -p "${LOG_DIR}"

  cd "${CURVINE_HOME}"

  echo "Starting ${SERVICE_NAME} service..."

  # Start the service with tee to output to both stdout and file
  "${CURVINE_HOME}/lib/curvine-server" \
    --service "${SERVICE_NAME}" \
    --conf "${CURVINE_CONF_FILE}" \
    2>&1 | tee "${OUT_FILE}" &

  local TEE_PID=$!
  sleep 3

  # First check if tee process is still running
  # If tee exited, it means curvine-server failed quickly
  if ! kill -0 "${TEE_PID}" 2>/dev/null; then
    wait "${TEE_PID}" 2>/dev/null
    local EXIT_CODE=$?
    echo "${SERVICE_NAME} start fail - process exited during startup with code ${EXIT_CODE}"
    exit 1
  fi

  # Verify curvine-server process is running
  # Use full path and service name for precise matching
  local ACTUAL_PID
  ACTUAL_PID=$(pgrep -n -f "${CURVINE_HOME}/lib/curvine-server[[:space:]].*--service[[:space:]]${SERVICE_NAME}" || true)
  if [[ -n "${ACTUAL_PID}" ]] && kill -0 "${ACTUAL_PID}" 2>/dev/null; then
    echo "${SERVICE_NAME} start success, pid=${ACTUAL_PID} (tee_pid=${TEE_PID})"

    # Wait for tee process to keep container alive
    # When curvine-server exits, tee exits, and container terminates gracefully
    wait "${TEE_PID}"
    local EXIT_CODE=$?
    echo "${SERVICE_NAME} process exited with code ${EXIT_CODE}"
    exit "${EXIT_CODE}"
  else
    echo "${SERVICE_NAME} start fail - process not found after startup check"
    # Kill tee if it's still running
    kill "${TEE_PID}" 2>/dev/null || true
    exit 1
  fi
}

stop_service() {
  local SERVICE_NAME=$1

  echo "Stopping ${SERVICE_NAME} service gracefully..."

  # Find all curvine-server processes for the given service
  local PIDS
  PIDS=$(pgrep -f "${CURVINE_HOME}/lib/curvine-server[[:space:]].*--service[[:space:]]${SERVICE_NAME}" || true)

  if [[ -z "${PIDS}" ]]; then
    echo "No running ${SERVICE_NAME} service to stop"
    return 0
  fi

  echo "Found ${SERVICE_NAME} PIDs: ${PIDS}"

  # Send SIGTERM first
  read -r -a PID_ARRAY <<< "${PIDS}"
  if [[ ${#PID_ARRAY[@]} -gt 0 ]]; then
    kill "${PID_ARRAY[@]}" 2>/dev/null || true
  fi

  # Wait up to ~15s for graceful shutdown
  local TIMEOUT=15
  local INTERVAL=3
  local ELAPSED=0

  while [[ ${ELAPSED} -lt ${TIMEOUT} ]]; do
    sleep "${INTERVAL}"
    ELAPSED=$((ELAPSED + INTERVAL))

    local ALIVE
    ALIVE=$(pgrep -f "${CURVINE_HOME}/lib/curvine-server[[:space:]].*--service[[:space:]]${SERVICE_NAME}" || true)
    if [[ -z "${ALIVE}" ]]; then
      echo "${SERVICE_NAME} stopped gracefully"
      return 0
    fi

    echo "Waiting for ${SERVICE_NAME} to stop gracefully (elapsed=${ELAPSED}s)"
  done

  # Force kill if still running
  local STILL_ALIVE
  STILL_ALIVE=$(pgrep -f "${CURVINE_HOME}/lib/curvine-server[[:space:]].*--service[[:space:]]${SERVICE_NAME}" || true)
  if [[ -n "${STILL_ALIVE}" ]]; then
    echo "${SERVICE_NAME} did not stop gracefully after ${TIMEOUT}s, killing with SIGKILL"
    read -r -a STILL_ARRAY <<< "${STILL_ALIVE}"
    kill -9 "${STILL_ARRAY[@]}" 2>/dev/null || true
  fi
}

restart_service() {
  local SERVICE_NAME=$1
  stop_service "${SERVICE_NAME}"
  start_service "${SERVICE_NAME}"
}

set_hosts

case "$SERVER_TYPE" in
  (master|worker)
    case "$ACTION_TYPE" in
      start)
        start_service "$SERVER_TYPE"
        ;;
      stop)
        stop_service "$SERVER_TYPE"
        ;;
      restart)
        restart_service "$SERVER_TYPE"
        ;;
      *)
        echo "Unsupported action: $ACTION_TYPE (expected: start|stop|restart)"
        exit 1
        ;;
    esac
    ;;

  (all)
    echo "Container does not support 'all' mode. Use separate master and worker containers."
    exit 1
    ;;

  (*)
    exec "$@"
    ;;
esac

