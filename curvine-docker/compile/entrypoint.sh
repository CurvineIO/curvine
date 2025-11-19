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

# Start the curvine service process
# Service type: master, worker, all
SERVER_TYPE=$1

# Operation type: start, stop, restart
ACTION_TYPE=$2

if [ -z "$SERVER_TYPE" ]; then
  SERVER_TYPE="all"
fi

if [ -z "$ACTION_TYPE" ]; then
  ACTION_TYPE="start"
fi

echo "SERVER_TYPE: $SERVER_TYPE, ACTION_TYPE: $ACTION_TYPE"

set_hosts() {
  if [[ $(grep -c $HOSTNAME /etc/hosts) = '0' ]]; then
    echo "$POD_IP $HOSTNAME" >> /etc/hosts
  fi

  echo "POD_IP: ${POD_IP}"
  echo "POD_NAMESPACE: ${POD_NAMESPACE}"
  echo "POD_CLUSTER_DOMAIN: ${POD_CLUSTER_DOMAIN}"
  if [[ -z "$POD_IP" || -z "$POD_NAMESPACE" || -z "$POD_CLUSTER_DOMAIN" ]]; then
    echo "missing env, POD_IP: $POD_IP, POD_NAMESPACE: $POD_NAMESPACE, POD_CLUSTER_DOMAIN: $POD_CLUSTER_DOMAIN"
    return 0
  fi
  name="${POD_IP//./-}.${POD_NAMESPACE//_/-}.pod.${POD_CLUSTER_DOMAIN}"
  echo "$(cat /etc/hosts | sed s/"$POD_IP"/"$POD_IP $name"/g)" >/etc/hosts
  echo 'export PS1="[\u@\H \W]\\$ "' >>/etc/bashrc
}

# Container-specific startup logic
# This avoids modifying launch-process.sh which is used in non-container environments
start_service() {
  local SERVICE_NAME=$1
  
  if [ -z "$CURVINE_CONF_FILE" ]; then
    export CURVINE_CONF_FILE=${CURVINE_HOME}/conf/curvine-cluster.toml
  fi
  
  local LOG_DIR=${CURVINE_HOME}/logs
  local OUT_FILE=${LOG_DIR}/${SERVICE_NAME}.out
  mkdir -p ${LOG_DIR}
  
  cd ${CURVINE_HOME}
  
  echo "Starting ${SERVICE_NAME} service..."
  
  # Start the service with tee to output to both stdout and file
  ${CURVINE_HOME}/lib/curvine-server \
    --service ${SERVICE_NAME} \
    --conf ${CURVINE_CONF_FILE} \
    2>&1 | tee ${OUT_FILE} &
  
  local TEE_PID=$!
  sleep 3
  
  # First check if tee process is still running
  # If tee exited, it means curvine-server failed quickly
  if ! kill -0 ${TEE_PID} 2>/dev/null; then
    wait ${TEE_PID} 2>/dev/null
    local EXIT_CODE=$?
    echo "${SERVICE_NAME} start fail - process exited during startup with code ${EXIT_CODE}"
    exit 1
  fi
  
  # Verify curvine-server process is running
  # Use full path and service name for precise matching
  local ACTUAL_PID=$(pgrep -n -f "${CURVINE_HOME}/lib/curvine-server.*--service ${SERVICE_NAME}")
  if [[ -n "${ACTUAL_PID}" ]] && kill -0 ${ACTUAL_PID} 2>/dev/null; then
    echo "${SERVICE_NAME} start success, pid=${ACTUAL_PID} (tee_pid=${TEE_PID})"
    
    # Wait for tee process to keep container alive
    # When curvine-server exits, tee exits, and container terminates gracefully
    wait ${TEE_PID}
    local EXIT_CODE=$?
    echo "${SERVICE_NAME} process exited with code ${EXIT_CODE}"
    exit ${EXIT_CODE}
  else
    echo "${SERVICE_NAME} start fail - process not found after startup check"
    # Kill tee if it's still running
    kill ${TEE_PID} 2>/dev/null || true
    exit 1
  fi
}

set_hosts

# Only handle start action in container
if [ "$ACTION_TYPE" != "start" ]; then
  echo "Container only supports start action, got: $ACTION_TYPE"
  exit 1
fi

case "$SERVER_TYPE" in
    (master|worker)
      start_service "$SERVER_TYPE"
      ;;
    
    (all)
      echo "Container does not support 'all' mode. Use separate master and worker containers."
      exit 1
      ;;

    (*)
      exec "$@"
      ;;
esac

