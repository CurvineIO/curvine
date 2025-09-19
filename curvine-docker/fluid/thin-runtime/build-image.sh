#!/bin/bash

set -e

# Configuration
CURVINE_BUILD_PATH="../../../../build/dist"
IMAGE_NAME="fluid-cloudnative/curvine-thinruntime"
IMAGE_TAG="v1.0.0"

# Check if build directory exists
if [ ! -d "$CURVINE_BUILD_PATH" ]; then
    echo "Error: Build directory not found at $CURVINE_BUILD_PATH"
    echo "Please run 'make build' or 'cargo build' first to generate the build artifacts"
    exit 1
fi

# Check if essential files exist
if [ ! -f "$CURVINE_BUILD_PATH/lib/curvine-fuse" ]; then
    echo "Error: curvine-fuse binary not found at $CURVINE_BUILD_PATH/lib/curvine-fuse"
    echo "Please ensure curvine-fuse is built and available in the build directory"
    exit 1
fi

echo "Found curvine build directory: $CURVINE_BUILD_PATH"

# Create build context
BUILD_DIR=$(mktemp -d)
echo "Creating build context in: $BUILD_DIR"

# Copy Dockerfile and fluid config parser
cp Dockerfile "$BUILD_DIR/"
cp fluid-config-parse.py "$BUILD_DIR/"

# Copy entire build directory
echo "Copying curvine build artifacts..."
cp -r "$CURVINE_BUILD_PATH" "$BUILD_DIR/build"

# Build Docker image
echo "Building Docker image: $IMAGE_NAME:$IMAGE_TAG"
docker build -t "$IMAGE_NAME:$IMAGE_TAG" "$BUILD_DIR"

# Clean up
rm -rf "$BUILD_DIR"

echo "Docker image built successfully: $IMAGE_NAME:$IMAGE_TAG"
echo ""
echo "Build artifacts copied from: $CURVINE_BUILD_PATH"
echo ""
echo "To test the image:"
echo "docker run --rm --privileged $IMAGE_NAME:$IMAGE_TAG"
echo ""
echo "To push the image:"
echo "docker push $IMAGE_NAME:$IMAGE_TAG" 