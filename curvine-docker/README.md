# Curvine Docker Build System

This directory contains Docker configuration files and scripts for building and running Curvine.

## File Description

- `Dockerfile` - Multi-stage Dockerfile with volume mount support
- `build-simple.sh` - Simple build script using volume mounts, generates build artifacts only
- `build-runtime.sh` - Build runtime image based on build artifacts
- `build-with-volumes.sh` - Complete build script that generates final runtime image
- `deploy/` - Deployment related files
- `compile/` - Build environment related files

## Usage

### Method 1: Step-by-step Build (Recommended)

This method separates compilation and runtime image building for easier development and debugging:

**Step 1: Build Artifacts**
```bash
# Run from project root
cd /path/to/curvine
./curvine-docker/build-simple.sh
```

After building:
- Build artifacts will be saved in `docker-build-output/` directory
- Build cache will be saved in `docker-build-cache/` directory (reused in subsequent builds)
  - `target/` - Rust/Cargo compilation cache
  - `.cargo/` - Cargo dependency cache
  - `.m2/repository/` - Maven dependency cache
  - `.npm/` - npm package cache

**Step 2: Build Runtime Image**
```bash
# Build runtime image based on build artifacts
./curvine-docker/build-runtime.sh

# Or specify custom image tag
./curvine-docker/build-runtime.sh v1.0.0
```

This will use the second stage of `curvine-docker/Dockerfile` to directly build the `curvine:latest` runtime image (or specified tag).

### Method 2: Complete Build

This method builds the final runtime Docker image in one step:

```bash
# Run from project root
cd /path/to/curvine
./curvine-docker/build-with-volumes.sh
```

This will create two images:
- `curvine-builder:latest` - Build environment image
- `curvine:latest` - Runtime image

### Method 3: Manual Step-by-step Build

You can also manually execute the build process:

```bash
# 1. Build the builder image
docker build -t curvine-builder:latest -f curvine-docker/Dockerfile --target builder .

# 2. Create output directories
mkdir -p docker-build-output docker-build-cache

# 3. Run the build
docker run --rm \
    -v $(pwd):/workspace \
    -v $(pwd)/docker-build-output:/build-output \
    -v $(pwd)/docker-build-cache:/build-cache \
    curvine-builder:latest \
    bash -c "cd /workspace && ./build/build.sh && cp -r build/dist/* /build-output/"

# 4. Build runtime image
./curvine-docker/build-runtime.sh

# Or manually build runtime image directly
docker build -t curvine:latest --target runtime -f curvine-docker/Dockerfile .
```

## Directory Structure

Directory structure after building:

```
curvine/
├── docker-build-output/          # Build artifacts output directory
│   ├── bin/                      # Executable files
│   ├── lib/                      # Library files
│   ├── conf/                     # Configuration files
│   ├── webui/                    # Web UI files (if built)
│   └── build-version             # Version information
├── docker-build-cache/           # Build cache directory
│   ├── target/                   # Rust/Cargo compilation cache
│   ├── .cargo/                   # Cargo dependency cache
│   ├── .m2/repository/           # Maven dependency cache
│   └── .npm/                     # npm package cache
└── curvine-docker/
    ├── Dockerfile
    ├── build-simple.sh
    └── build-with-volumes.sh
```

## Running the Image

After building, you can run the curvine container:

```bash
# Run all services (default)
docker run -it --rm curvine:latest

# Run master service only
docker run -it --rm curvine:latest master start

# Run worker service only
docker run -it --rm curvine:latest worker start

# Run in background with port mapping
docker run -d -p 8995:8995 -p 9000:9000 curvine:latest
```

## Advantages

Advantages of using this build approach:

1. **Multi-language build cache reuse**: `docker-build-cache/` directory persists across builds, supporting Rust, Java(Maven), Node.js(npm) caches, significantly accelerating subsequent builds
2. **Accessible artifacts**: Build artifacts are directly saved in host directory, convenient for use and debugging
3. **Environment consistency**: Build environment is fully containerized, ensuring consistent build results
4. **Flexibility**: Can choose to build only or generate runtime image as well
5. **Separation of concerns**: Compilation and runtime environments are separated, facilitating debugging and deployment
6. **Unified configuration**: Runtime image directly uses the second stage of main Dockerfile, avoiding duplicate configuration

## Cleanup

Clean up build artifacts and cache:

```bash
rm -rf docker-build-output docker-build-cache
```

Clean up Docker images:

```bash
docker rmi curvine-builder:latest curvine:latest
```
