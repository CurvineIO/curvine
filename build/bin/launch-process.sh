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

. "$(cd "`dirname "$0"`"; pwd)"/../conf/curvine-env.sh

SERVICE_NAME=$1
ACTION=$2
PARAMS="${@:3}"

PID_FILE=${CURVINE_HOME}/${SERVICE_NAME}.pid
GRACEFULLY_TIMEOUT=15
RELOAD_LOCK_FILE=${CURVINE_HOME}/${SERVICE_NAME}.reload.lock

LOG_DIR=${CURVINE_HOME}/logs
OUT_FILE=${LOG_DIR}/${SERVICE_NAME}.out


if [ -z "$CURVINE_CONF_FILE" ]; then
  export CURVINE_CONF_FILE=$CURVINE_HOME/conf/curvine-cluster.toml
fi

mkdir -p ${LOG_DIR}

cd ${CURVINE_HOME}

check() {
  if [ -f "${PID_FILE}" ]; then
    local PID=`cat ${PID_FILE}`
    if kill -0 ${PID} > /dev/null 2>&1; then
      echo "${SERVICE_NAME} running, pid=${PID}. Please execute stop first"
      exit 1
    fi
  fi
}

start() {
    check

    name="unknown"
    if [[ "$SERVICE_NAME" = "worker" ]] || [[ "$SERVICE_NAME" = "master" ]]; then
      name="curvine-server"
      nohup ${CURVINE_HOME}/lib/curvine-server \
      --service ${SERVICE_NAME} \
      --conf ${CURVINE_CONF_FILE} \
      > ${OUT_FILE} 2>&1 < /dev/null  &
     elif [[ "$SERVICE_NAME" = "fuse" ]]; then
       name="curvine-fuse"
       nohup ${CURVINE_HOME}/lib/curvine-fuse \
       $PARAMS \
       --conf ${CURVINE_CONF_FILE} \
       > ${OUT_FILE} 2>&1 < /dev/null  &
    else
       echo "Unknown service"
       exit
    fi

    NEW_PID=$!
    sleep 3

    if [[ $(ps -p "${NEW_PID}" -o comm=) =~ $name ]]; then
        echo ${NEW_PID} > ${PID_FILE}
        echo "${SERVICE_NAME} start success, pid=${NEW_PID}"
    else
      echo "${SERVICE_NAME} start fail"
    fi

    head ${OUT_FILE}
}

waitPid() {
  PID=$1
  n=`expr ${GRACEFULLY_TIMEOUT} / 3`
  i=0
  while [ $i -le $n ]; do
     if kill -0 ${PID} > /dev/null 2>&1; then
       echo "`date +"%Y-%m-%d %H:%M:%S"` wait ${SERVICE_NAME} stop gracefully"
       sleep 3
      else
        break
     fi
     let i++
  done
}

stop() {
  if [ -f "${PID_FILE}" ]; then
    local PID=`cat ${PID_FILE}`
    if kill -0 $PID > /dev/null 2>&1; then
      echo "stopping ${SERVICE_NAME}"

      kill ${PID} && rm -f "PID_FILE"
      waitPid $PID;

      if kill -0 $PID > /dev/null 2>&1; then
        echo "shuffle worker: ${SERVICE_NAME} did not stop gracefully after $GRACEFULLY_TIMEOUT seconds: killing with kill -9"
        kill -9 $PID
      else
        echo "${SERVICE_NAME} stop gracefully"
      fi
    else
      echo "no ${SERVICE_NAME} to stop"
    fi
  else
    echo "Not found ${SERVICE_NAME} pid file"
  fi
}

reload() {
  # Prevent concurrent reload operations
  if [ -f "${RELOAD_LOCK_FILE}" ]; then
    local LOCK_PID=`cat ${RELOAD_LOCK_FILE} 2>/dev/null`
    if kill -0 $LOCK_PID > /dev/null 2>&1; then
      echo "reload already in progress (lock held by pid ${LOCK_PID})"
      exit 1
    else
      # Stale lock file, remove it
      rm -f ${RELOAD_LOCK_FILE}
    fi
  fi
  
  # Create lock file
  echo $$ > ${RELOAD_LOCK_FILE}
  trap "rm -f ${RELOAD_LOCK_FILE}" EXIT
  
  if [ -f "${PID_FILE}" ]; then
    local OLD_PID=`cat ${PID_FILE}`
    if kill -0 $OLD_PID > /dev/null 2>&1; then
      echo "reloading ${SERVICE_NAME} (sending SIGUSR1 to pid ${OLD_PID})"
      kill -USR1 ${OLD_PID}
      if [ $? -ne 0 ]; then
        echo "failed to send reload signal to ${SERVICE_NAME}"
        exit 1
      fi
      
      echo "${SERVICE_NAME} reload signal sent successfully, waiting for old process to exit and new process to start..."

      # Wait for old process to exit (graceful shutdown)
      # Old process will persist state and spawn new process before exiting
      waitPid $OLD_PID
      
      # Wait a bit for new process to start
      sleep 3

      # Find new process - simple approach: find any running curvine-fuse process
      # Since old process has exited, any found process should be the new one
      # Take the last one as it's the newest process
      local NEW_PID=""
      if command -v pgrep > /dev/null 2>&1; then
        # Get all curvine-fuse PIDs, exclude old PID, take the last one (newest)
        NEW_PID=$(pgrep -f "curvine-fuse" 2>/dev/null | grep -v "^${OLD_PID}$" | tail -1)
      else
        NEW_PID=$(ps aux | grep "curvine-fuse" | awk '{print $2}' | grep -v "^${OLD_PID}$" | tail -1)
      fi
      
      # Verify and update PID file
      if [ -n "$NEW_PID" ] && kill -0 $NEW_PID > /dev/null 2>&1; then
        # Verify it's actually curvine-fuse
        if ps -p $NEW_PID -o comm= 2>/dev/null | grep -q "curvine-fuse"; then
          echo $NEW_PID > ${PID_FILE}
          echo "${SERVICE_NAME} reloaded successfully, new pid=${NEW_PID}"
          exit 0
        fi
      fi

      # If we get here, failed to find new process
      echo "error: could not find new ${SERVICE_NAME} process after ${max_retries} seconds"
      echo "please check ${SERVICE_NAME} status manually"
      exit 1
    else
      echo "${SERVICE_NAME} is not running (pid ${OLD_PID} not found)"
      exit 1
    fi
  else
    echo "Not found ${SERVICE_NAME} pid file"
    exit 1
  fi
}

run() {
case $1 in
    "start")
        start
        ;;
    "stop")
        stop
        ;;
    "restart")
        stop
        sleep 1
        start
        ;;
    "reload")
        if [[ "$SERVICE_NAME" != "fuse" ]]; then
          echo "reload is only supported for fuse service, ${SERVICE_NAME} does not support reload"
          exit 1
        fi
        reload
        ;;
     *)
        if [[ "$SERVICE_NAME" = "fuse" ]]; then
          echo "Usage: [start|stop|restart|reload]"
        else
          echo "Usage: [start|stop|restart]"
        fi
        ;;
esac
}

run ${ACTION}

