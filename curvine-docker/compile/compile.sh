#!/bin/bash

# Build the curvine-compile image
docker buildx build --platform linux/amd64 -t curvine/curvine-compile:latest -f Dockerfile_rocky9 .