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

RUN_CMD=${CURVINE_HOME}/curvine/bin/launch-process.sh

set_hosts

case "$SERVER_TYPE" in
    (all)
      $RUN_CMD master $ACTION_TYPE
      $RUN_CMD worker $ACTION_TYPE
      ;;

    (master)
      $RUN_CMD master $ACTION_TYPE
      ;;

    (worker)
      $RUN_CMD worker $ACTION_TYPE
      ;;

    (fluid-thin-runtime)
      python3 $CURVINE_HOME/fluid-config-parse.py && $CURVINE_HOME/mount-curvine.sh
      ;;

     (*)
      exec "$@"
      ;;
esac

tail -f /dev/null &
wait || :
