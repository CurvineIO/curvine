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

# Install or remove the systemd units that run Curvine services in the
# foreground under systemd supervision (see systemd/README.md).

set -euo pipefail

. "$(cd "$(dirname "$0")"; pwd)"/../conf/curvine-env.sh

DEFAULT_UNIT_DIR=/etc/systemd/system
TEMPLATE_DIR=${CURVINE_HOME}/systemd
UNITS=(curvine-master.service curvine-worker.service curvine-fuse@.service)

usage() {
  cat <<EOF
Usage: $0 install [--user USER] [--group GROUP] [--unit-dir DIR]
       $0 uninstall [--unit-dir DIR]

install    Render ${UNITS[*]}
           for CURVINE_HOME=${CURVINE_HOME} into the unit directory and reload systemd.
           Units are not enabled or started.
uninstall  Stop and disable the Curvine units, including every curvine-fuse@ instance,
           remove the unit files and reload systemd.

Options:
  --user USER     Account the services run as (default: root).
  --group GROUP   Group the services run as (default: primary group of USER).
  --unit-dir DIR  Unit directory (default: ${DEFAULT_UNIT_DIR}). With any other directory
                  the files are only written or removed and systemctl is not called,
                  which is useful for packaging.
EOF
}

die() {
  echo "$*" >&2
  exit 1
}

# systemctl is only driven for the live unit directory; a custom --unit-dir is a
# staging area for packaging and must not touch the running system.
manage_systemd() {
  [[ "${UNIT_DIR}" == "${DEFAULT_UNIT_DIR}" ]] && command -v systemctl > /dev/null 2>&1
}

check_home() {
  # The path is written verbatim into ExecStart= and WorkingDirectory=, where
  # whitespace splits arguments, '%' starts a systemd specifier and quotes or
  # backslashes are interpreted.
  if [[ "${CURVINE_HOME}" =~ [[:space:]%\\\"\'] ]]; then
    die "CURVINE_HOME '${CURVINE_HOME}' contains whitespace, '%', quotes or backslashes; install Curvine under a plain path first"
  fi
  local unit
  for unit in "${UNITS[@]}"; do
    [[ -f "${TEMPLATE_DIR}/${unit}.in" ]] || die "Missing unit template ${TEMPLATE_DIR}/${unit}.in"
  done
}

render() {
  local template=$1 target=$2 content
  content=$(< "${template}")
  # Quoted replacements keep '&' literal on bash 5.2+ (patsub_replacement).
  content=${content//@CURVINE_HOME@/"${CURVINE_HOME}"}
  content=${content//@CURVINE_USER@/"${SERVICE_USER}"}
  content=${content//@CURVINE_GROUP@/"${SERVICE_GROUP}"}
  printf '%s\n' "${content}" > "${target}"
  chmod 0644 "${target}"
}

install_units() {
  check_home
  id -u "${SERVICE_USER}" > /dev/null 2>&1 || die "User '${SERVICE_USER}' does not exist"
  if [[ -z "${SERVICE_GROUP}" ]]; then
    SERVICE_GROUP=$(id -gn "${SERVICE_USER}")
  elif command -v getent > /dev/null 2>&1 && ! getent group "${SERVICE_GROUP}" > /dev/null; then
    die "Group '${SERVICE_GROUP}' does not exist"
  fi
  mkdir -p "${UNIT_DIR}"
  [[ -w "${UNIT_DIR}" ]] || die "${UNIT_DIR} is not writable; run as root or pass --unit-dir"

  local unit
  for unit in "${UNITS[@]}"; do
    render "${TEMPLATE_DIR}/${unit}.in" "${UNIT_DIR}/${unit}"
    echo "Installed ${UNIT_DIR}/${unit}"
  done

  if manage_systemd; then
    systemctl daemon-reload
  fi

  cat <<EOF

Units installed for CURVINE_HOME=${CURVINE_HOME}, running as ${SERVICE_USER}:${SERVICE_GROUP}.
Enable and start the services this host runs, for example:
  systemctl enable --now curvine-master.service
  systemctl enable --now curvine-worker.service
  systemctl enable --now "\$(systemd-escape --path --template=curvine-fuse@.service /curvine-fuse)"
EOF
}

uninstall_units() {
  local unit
  if manage_systemd; then
    local units=(curvine-master.service curvine-worker.service) instance
    # Loaded curvine-fuse@ instances, running or not.
    while read -r instance _; do
      [[ -n "${instance}" ]] && units+=("${instance}")
    done < <(systemctl list-units --all --plain --no-legend 'curvine-fuse@*.service' 2> /dev/null)
    for unit in "${units[@]}"; do
      systemctl disable --now "${unit}" 2> /dev/null || true
    done
    # Removes the enablement symlinks of every instance of the template.
    systemctl disable curvine-fuse@.service 2> /dev/null || true
  fi

  for unit in "${UNITS[@]}"; do
    if [[ -f "${UNIT_DIR}/${unit}" ]]; then
      rm -f "${UNIT_DIR}/${unit}"
      echo "Removed ${UNIT_DIR}/${unit}"
    fi
  done

  if manage_systemd; then
    systemctl daemon-reload
    systemctl reset-failed 'curvine-*' 2> /dev/null || true
  fi
}

ACTION=${1:-}
[[ $# -gt 0 ]] && shift

SERVICE_USER=root
SERVICE_GROUP=
UNIT_DIR=${DEFAULT_UNIT_DIR}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --user)
      [[ $# -ge 2 ]] || die "--user requires a value"
      SERVICE_USER=$2
      shift 2
      ;;
    --group)
      [[ $# -ge 2 ]] || die "--group requires a value"
      SERVICE_GROUP=$2
      shift 2
      ;;
    --unit-dir)
      [[ $# -ge 2 ]] || die "--unit-dir requires a value"
      UNIT_DIR=${2%/}
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      exit 1
      ;;
  esac
done

case "${ACTION}" in
  install)
    install_units
    ;;
  uninstall)
    uninstall_units
    ;;
  -h|--help)
    usage
    ;;
  *)
    usage >&2
    exit 1
    ;;
esac
