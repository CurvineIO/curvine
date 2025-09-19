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

# Build curvine using Docker with volume mounts
# This script demonstrates how to build curvine using the new Dockerfile with volume mapping

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
rm -rf "$BUILD_OUTPUT_DIR" "$BUILD_CACHE_DIR"
mkdir -p "$BUILD_OUTPUT_DIR" "$BUILD_CACHE_DIR"

# Step 1: Build the builder image
echo "Building the curvine builder image..."
docker build -t curvine-builder:latest -f "$DOCKER_DIR/Dockerfile" --target builder "$PROJECT_ROOT"

# Step 2: Run the build process with volume mounts
echo "Running build process with volume mounts..."
docker run --rm \
    -v "$PROJECT_ROOT:/workspace:ro" \
    -v "$BUILD_OUTPUT_DIR:/build-output" \
    -v "$BUILD_CACHE_DIR:/build-cache" \
    curvine-builder:latest \
    bash -c "
        cd /workspace &&
        # Modify build script to output to /build-output instead of build/dist
        DIST_DIR=/build-output ./build/build.sh
    "

# Step 3: Build the runtime image
echo "Building the curvine runtime image..."
# Copy build output to a temporary location for Docker context
cp -r "$BUILD_OUTPUT_DIR" "$DOCKER_DIR/build-output"

# Build the runtime image
docker build -t curvine:latest -f "$DOCKER_DIR/Dockerfile" "$DOCKER_DIR"

# Clean up temporary build output
rm -rf "$DOCKER_DIR/build-output"

echo "Build completed successfully!"
echo "Images created:"
echo "  - curvine-builder:latest (build environment)"
echo "  - curvine:latest (runtime)"
echo ""
echo "Build artifacts are available in: $BUILD_OUTPUT_DIR"
echo "Build cache is available in: $BUILD_CACHE_DIR"
