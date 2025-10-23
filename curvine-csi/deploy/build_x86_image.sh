#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "Building from ${ROOT_DIR}"

IMAGE_NAME=${IMAGE_NAME:-curvine/csi}
IMAGE_TAG=${IMAGE_TAG:-latest}
PLATFORM=${PLATFORM:-linux/amd64}
GOPROXY=${GOPROXY:-https://proxy.golang.org}

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

DOCKER_ARGS=(
  --platform "${PLATFORM}"
  --build-arg "GOPROXY=${GOPROXY}"
  -t "${IMAGE_NAME}:${IMAGE_TAG}"
  -f "${ROOT_DIR}/curvine-csi/Dockerfile"
  "${ROOT_DIR}"
)

echo "Building ${IMAGE_NAME}:${IMAGE_TAG} for ${PLATFORM}"
docker build "${DOCKER_ARGS[@]}"
