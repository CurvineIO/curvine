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

export CURVINE_HOME="$(cd "$(dirname "$0")"/..; pwd)"

# ---------------------------------------------------------------------------
# Hostname / advertise address
#
# Local / single-node (default):
#   Leave CURVINE_INTERFACE empty. Master/worker/client default to localhost so
#   auto-detected unreachable NICs cannot break local FUSE writes (see #1134).
#
# Production / multi-node:
#   1. Set CURVINE_INTERFACE to the cluster NIC, e.g. eth0 (same value on every
#      node; no per-machine hostname/IP env overrides required).
#   2. Ensure /etc/hosts on each node maps that NIC's IP to the node hostname:
#        <eth0_ip>  <hostname>
#   3. This script resolves $(hostname) to an IPv4 address and uses it as the
#      worker/client advertise address. Master uses the hostname (must match
#      journal_addrs). Resolution failure or a mismatch vs the NIC IP is fatal.
#
# Explicit CURVINE_MASTER_HOSTNAME / CURVINE_WORKER_HOSTNAME /
# CURVINE_CLIENT_HOSTNAME always win over the defaults below.
# ---------------------------------------------------------------------------

# Cluster NIC name for production advertise-IP resolution. Empty = local/dev.
CURVINE_INTERFACE=${CURVINE_INTERFACE:-}

_curvine_env_fail() {
    echo "ERROR: curvine-env.sh: $*" >&2
    # Sourced by start scripts: exit aborts cluster startup instead of
    # continuing with a wrong advertise address.
    exit 1
}

# IPv4 address configured on the given network interface, or empty.
_curvine_interface_ipv4() {
    local iface="$1"
    local os_name ip

    os_name=$(uname 2>/dev/null || echo unknown)
    case "$os_name" in
        Linux)
            if command -v ip >/dev/null 2>&1; then
                ip=$(ip -4 -o addr show dev "$iface" 2>/dev/null \
                    | awk '{print $4}' | cut -d/ -f1 | head -n1)
            fi
            if [ -z "$ip" ] && command -v ifconfig >/dev/null 2>&1; then
                ip=$(ifconfig "$iface" 2>/dev/null \
                    | awk '/inet / { print $2; exit }')
                ip=${ip#addr:}
            fi
            ;;
        Darwin)
            if command -v ipconfig >/dev/null 2>&1; then
                ip=$(ipconfig getifaddr "$iface" 2>/dev/null || true)
            fi
            if [ -z "$ip" ] && command -v ifconfig >/dev/null 2>&1; then
                ip=$(ifconfig "$iface" 2>/dev/null \
                    | awk '/inet / { print $2; exit }')
            fi
            ;;
        *)
            ip=
            ;;
    esac
    echo "$ip"
}

# Resolve hostname to a non-loopback IPv4 via /etc/hosts / system resolver.
_curvine_resolve_hostname_ipv4() {
    local host="$1"
    local ip

    if command -v getent >/dev/null 2>&1; then
        while read -r ip _; do
            case "$ip" in
                ""|127.*|0.0.0.0) continue ;;
                *:*) continue ;;
                *)
                    echo "$ip"
                    return 0
                    ;;
            esac
        done < <(getent ahostsv4 "$host" 2>/dev/null | awk '{print $1}')

        while read -r ip _; do
            case "$ip" in
                ""|127.*|0.0.0.0) continue ;;
                *:*) continue ;;
                *)
                    echo "$ip"
                    return 0
                    ;;
            esac
        done < <(getent hosts "$host" 2>/dev/null | awk '{print $1}')
    fi

    if command -v python3 >/dev/null 2>&1; then
        ip=$(python3 -c '
import socket, sys
host = sys.argv[1]
try:
    for info in socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM):
        addr = info[4][0]
        if not addr.startswith("127."):
            print(addr)
            raise SystemExit(0)
except Exception:
    raise SystemExit(1)
' "$host" 2>/dev/null || true)
        if [ -n "$ip" ]; then
            echo "$ip"
            return 0
        fi
    fi

    return 1
}

if [ -n "${CURVINE_MASTER_HOSTNAME:-}" ] \
    && [ -n "${CURVINE_WORKER_HOSTNAME:-}" ] \
    && [ -n "${CURVINE_CLIENT_HOSTNAME:-}" ]; then
    # All advertise hostnames already provided; skip detection.
    LOCAL_HOSTNAME=$CURVINE_MASTER_HOSTNAME
    LOCAL_IP=$CURVINE_WORKER_HOSTNAME
elif [ -n "$CURVINE_INTERFACE" ]; then
    LOCAL_HOSTNAME=$(hostname 2>/dev/null || true)
    if [ -z "$LOCAL_HOSTNAME" ] || [ "$LOCAL_HOSTNAME" = "localhost" ]; then
        _curvine_env_fail \
            "CURVINE_INTERFACE=$CURVINE_INTERFACE requires a real hostname; got '${LOCAL_HOSTNAME:-<empty>}'"
    fi

    IFACE_IP=$(_curvine_interface_ipv4 "$CURVINE_INTERFACE")
    if [ -z "$IFACE_IP" ]; then
        _curvine_env_fail \
            "cannot read IPv4 address from interface '$CURVINE_INTERFACE' (check NIC name and that it is up)"
    fi

    if ! LOCAL_IP=$(_curvine_resolve_hostname_ipv4 "$LOCAL_HOSTNAME"); then
        _curvine_env_fail \
            "failed to resolve hostname '$LOCAL_HOSTNAME' to a non-loopback IPv4. Ensure /etc/hosts contains: $IFACE_IP $LOCAL_HOSTNAME"
    fi

    case "$LOCAL_IP" in
        127.*|0.0.0.0|localhost)
            _curvine_env_fail \
                "hostname '$LOCAL_HOSTNAME' resolved to loopback '$LOCAL_IP'; production must not advertise localhost. Fix /etc/hosts to: $IFACE_IP $LOCAL_HOSTNAME"
            ;;
    esac

    if [ "$LOCAL_IP" != "$IFACE_IP" ]; then
        _curvine_env_fail \
            "hostname '$LOCAL_HOSTNAME' resolved to '$LOCAL_IP' but interface '$CURVINE_INTERFACE' has '$IFACE_IP'. Align /etc/hosts with the NIC IP: $IFACE_IP $LOCAL_HOSTNAME"
    fi
else
    # Safe local/dev defaults (do not auto-detect NICs).
    LOCAL_HOSTNAME=localhost
    LOCAL_IP=localhost
fi

# master bound host name
export CURVINE_MASTER_HOSTNAME=${CURVINE_MASTER_HOSTNAME:-$LOCAL_HOSTNAME}

# worker bound host name / advertise address
export CURVINE_WORKER_HOSTNAME=${CURVINE_WORKER_HOSTNAME:-$LOCAL_IP}

# The client hostname is used to determine whether the worker and client are on the same machine.
export CURVINE_CLIENT_HOSTNAME=${CURVINE_CLIENT_HOSTNAME:-$LOCAL_IP}

export ORPC_BIND_HOSTNAME=0.0.0.0

export CURVINE_CONF_FILE=${CURVINE_CONF_FILE:-$CURVINE_HOME/conf/curvine-cluster.toml}
