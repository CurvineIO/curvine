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

# Build curvine runtime image from build artifacts
# This script should be run after build-simple.sh

set -e

# Get the project root directory (one level up from curvine-docker)
PROJECT_ROOT="$(cd "$(dirname "$0")/.."; pwd)"
DOCKER_DIR="$(cd "$(dirname "$0")"; pwd)"

# Check if build output exists
BUILD_OUTPUT_DIR="$PROJECT_ROOT/docker-build-output"
if [ ! -d "$BUILD_OUTPUT_DIR" ]; then
    echo "Error: Build output directory not found: $BUILD_OUTPUT_DIR"
    echo "Please run ./curvine-docker/build-simple.sh first"
    exit 1
fi

# Check build output content
echo "Checking build output content..."
ls -la "$BUILD_OUTPUT_DIR"

# Check if we have directory structure or just zip files
if [ ! -d "$BUILD_OUTPUT_DIR/bin" ] && [ ! -d "$BUILD_OUTPUT_DIR/lib" ]; then
    echo "Warning: Expected directory structure (bin/, lib/, conf/) not found in build output"
    echo "This might be because build was run with --zip flag"
    echo ""
    echo "Current build output contents:"
    ls -la "$BUILD_OUTPUT_DIR"
    echo ""
    echo "Please run ./curvine-docker/build-simple.sh again to generate the correct directory structure"
    exit 1
fi

# Get image tag from command line or use default
TAG="latest"
if [ $# -eq 1 ]; then
    TAG=$1
fi

echo "Building curvine runtime image..."
echo "Build output directory: $BUILD_OUTPUT_DIR"
echo "Image tag: curvine:$TAG"

# Build the runtime image using the existing Dockerfile stage 2
# Dockerfile directly uses docker-build-output/ directory
echo "Building Docker image using curvine-docker/Dockerfile stage 2..."
docker build -t curvine:$TAG --target runtime -f "$DOCKER_DIR/Dockerfile" "$PROJECT_ROOT"

echo ""
echo "✅ Runtime image built successfully!"
echo "Image: curvine:$TAG"
echo ""
echo "You can now run the image with:"
echo "  docker run -it --rm curvine:$TAG"
echo ""
echo "Or run specific services:"
echo "  docker run -it --rm curvine:$TAG master start    # Run master only"
echo "  docker run -it --rm curvine:$TAG worker start    # Run worker only"
echo "  docker run -it --rm curvine:$TAG all start       # Run both (default)"
