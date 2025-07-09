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
     *)
        echo "Use age [start|stop|restart]"
        ;;
esac
}

run ${ACTION}

