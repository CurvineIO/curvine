#!/bin/bash

export CURVINE_HOME="$(cd "$(dirname "$0")"/..; pwd)"

# Production / multi-node example.
# Set the cluster NIC (same value on every node). Ops must ensure /etc/hosts
# contains "<interface_ip> <hostname>" on each machine.
# For local single-node, leave CURVINE_INTERFACE empty and use localhost
# defaults from the packaged etc/curvine-env.sh instead.
export CURVINE_INTERFACE=${CURVINE_INTERFACE:-eth0}

LOCAL_HOSTNAME=$(hostname 2>/dev/null || true)
if [ -z "$LOCAL_HOSTNAME" ] || [ "$LOCAL_HOSTNAME" = "localhost" ]; then
    echo "ERROR: CURVINE_INTERFACE=$CURVINE_INTERFACE requires a real hostname" >&2
    exit 1
fi

IFACE_IP=$(ip -4 -o addr show dev "$CURVINE_INTERFACE" 2>/dev/null \
    | awk '{print $4}' | cut -d/ -f1 | head -n1)
if [ -z "$IFACE_IP" ]; then
    echo "ERROR: cannot read IPv4 from interface '$CURVINE_INTERFACE'" >&2
    exit 1
fi

RESOLVED_IP=$(getent ahostsv4 "$LOCAL_HOSTNAME" 2>/dev/null | awk '{print $1; exit}')
if [ -z "$RESOLVED_IP" ] || [[ "$RESOLVED_IP" == 127.* ]]; then
    RESOLVED_IP=$(getent hosts "$LOCAL_HOSTNAME" 2>/dev/null | awk '{print $1; exit}')
fi
if [ -z "$RESOLVED_IP" ] || [[ "$RESOLVED_IP" == 127.* ]]; then
    echo "ERROR: failed to resolve hostname '$LOCAL_HOSTNAME'. Ensure /etc/hosts contains: $IFACE_IP $LOCAL_HOSTNAME" >&2
    exit 1
fi
if [ "$RESOLVED_IP" != "$IFACE_IP" ]; then
    echo "ERROR: hostname '$LOCAL_HOSTNAME' resolved to '$RESOLVED_IP' but $CURVINE_INTERFACE has '$IFACE_IP'" >&2
    exit 1
fi

export CURVINE_MASTER_HOSTNAME=${CURVINE_MASTER_HOSTNAME:-$LOCAL_HOSTNAME}
export CURVINE_WORKER_HOSTNAME=${CURVINE_WORKER_HOSTNAME:-$RESOLVED_IP}
export CURVINE_CLIENT_HOSTNAME=${CURVINE_CLIENT_HOSTNAME:-$RESOLVED_IP}

export CURVINE_CONF_FILE=${CURVINE_HOME}/conf/curvine-cluster.toml
