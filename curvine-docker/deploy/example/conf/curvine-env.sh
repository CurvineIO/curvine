#!/bin/bash

export CURVINE_HOME="$(cd "$(dirname "$0")"/..; pwd)"


export CURVINE_MASTER_HOSTNAME=$(hostname)

export CURVINE_WORKER_HOSTNAME=$(hostname -i)

export CURVINE_CLIENT_HOSTNAME=$(hostname -i)

export ORPC_BIND_HOSTNAME=0.0.0.0

export CURVINE_CONF_FILE=${CURVINE_HOME}/conf/curvine-cluster.toml