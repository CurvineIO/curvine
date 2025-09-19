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

# Simple build using Docker with volume mounts
# This script builds curvine using the builder stage only

set -e

# Get the project root directory (one level up from curvine-docker)
PROJECT_ROOT="$(cd "$(dirname "$0")/.."; pwd)"
DOCKER_DIR="$(cd "$(dirname "$0")"; pwd)"

# Create build directories on host
BUILD_OUTPUT_DIR="$PROJECT_ROOT/docker-build-output"
BUILD_CACHE_DIR="$PROJECT_ROOT/docker-build-cache"

echo "Project root: $PROJECT_ROOT"
echo "Build output directory: $BUILD_OUTPUT_DIR"
echo "Build cache directory: $BUILD_CACHE_DIR"

# Clean and create build directories
rm -rf "$BUILD_OUTPUT_DIR"
mkdir -p "$BUILD_OUTPUT_DIR" "$BUILD_CACHE_DIR"

# Build the builder image if it doesn't exist
if ! docker image inspect curvine-builder:latest >/dev/null 2>&1; then
    echo "Building the curvine builder image..."
    docker build -t curvine-builder:latest -f "$DOCKER_DIR/Dockerfile" --target builder "$PROJECT_ROOT"
fi

# Run the build process with volume mounts
echo "Running build process with volume mounts..."
docker run --rm \
    -v "$PROJECT_ROOT:/workspace" \
    -v "$BUILD_OUTPUT_DIR:/build-output" \
    -v "$BUILD_CACHE_DIR:/build-cache" \
    curvine-builder:latest \
    bash -c "
        cd /workspace &&
        # Build using the default script (without --zip to preserve directory structure)
        ./build/build.sh
        echo 'Copying build artifacts to output directory...'
        cp -r /workspace/build/dist/* /build-output/
        echo 'Build completed! Artifacts are in /build-output'
        ls -la /build-output
    "

echo ""
echo "Build completed successfully!"
echo "Build artifacts are available in: $BUILD_OUTPUT_DIR"
echo "Build cache is available in: $BUILD_CACHE_DIR"