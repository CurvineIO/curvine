#!/bin/bash

set -e

# Configuration
CURVINE_BUILD_PATH="../../build/dist"
IMAGE_NAME="curvine"
IMAGE_TAG="latest"

# Color output functions
print_info() {
    echo -e "\033[34m[INFO]\033[0m $1"
}

print_success() {
    echo -e "\033[32m[SUCCESS]\033[0m $1"
}

print_warning() {
    echo -e "\033[33m[WARNING]\033[0m $1"
}

print_error() {
    echo -e "\033[31m[ERROR]\033[0m $1"
}

# Interactive build mode selection
select_build_mode() {
    echo "==========================================="
    echo "    Curvine Native K8s Image Builder"
    echo "==========================================="
    echo ""
    echo "Please select build mode:"
    echo ""
    echo "1) Build from pre-built binaries (Recommended)"
    echo "   - Use pre-compiled binary files"
    echo "   - Fastest build speed"
    echo "   - Requires running 'make build' or 'cargo build' first"
    echo ""
    echo "2) Build from local workspace"
    echo "   - Copy local source code to container for compilation"
    echo "   - Suitable for testing local changes"
    echo "   - Longer build time, requires complete compilation environment"
    echo ""
    echo "3) Exit"
    echo ""
    read -p "Please enter your choice (1-3): " choice
    
    case $choice in
        1)
            BUILD_MODE="binary"
            print_info "Selected: Build from pre-built binaries"
            ;;
        2)
            BUILD_MODE="local"
            print_info "Selected: Build from local workspace"
            ;;
        3)
            print_info "Build cancelled"
            exit 0
            ;;
        *)
            print_error "Invalid choice, please rerun the script"
            exit 1
            ;;
    esac
    echo ""
}

# Function to build from pre-built binaries
build_from_binaries() {
    print_info "Starting build from pre-built binaries..."
    
    # Check if build directory exists
    if [ ! -d "$CURVINE_BUILD_PATH" ]; then
        print_error "Build directory not found: $CURVINE_BUILD_PATH"
        print_info "Please run 'make build' or 'cargo build' first to generate build artifacts"
        exit 1
    fi
    
    # Check if essential files exist
    if [ ! -f "$CURVINE_BUILD_PATH/bin/curvine-server" ] && [ ! -f "$CURVINE_BUILD_PATH/lib/curvine-server" ]; then
        print_warning "curvine-server binary not found in $CURVINE_BUILD_PATH"
        print_info "Please ensure Curvine is built and available in the build directory"
        exit 1
    fi
    
    print_success "Found build directory: $CURVINE_BUILD_PATH"
    
    # Get script directory
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
    
    # Create build context with proper error handling
    BUILD_DIR=$(mktemp -d -t curvine-binary-build-XXXXXX)
    if [ ! -d "$BUILD_DIR" ]; then
        print_error "Failed to create temporary build directory"
        exit 1
    fi
    print_info "Creating build context: $BUILD_DIR"
    
    # Copy Dockerfile for binary builds
    cp "$SCRIPT_DIR/Dockerfile.binary" "$BUILD_DIR/Dockerfile"
    
    # Copy entrypoint script (use compile-specific entrypoint for K8s environment support)
    if [ -f "$SCRIPT_DIR/entrypoint.sh" ]; then
        cp "$SCRIPT_DIR/entrypoint.sh" "$BUILD_DIR/"
        print_info "Using compile-specific entrypoint.sh"
    else
        print_error "entrypoint.sh not found at $SCRIPT_DIR/entrypoint.sh"
        exit 1
    fi
    
    # Copy build artifacts: flatten build/dist/* into build/ directory structure
    print_info "Copying build artifacts..."
    mkdir -p "$BUILD_DIR/build"
    cp -r "$PROJECT_ROOT/$CURVINE_BUILD_PATH"/* "$BUILD_DIR/build/"
    
    # Build Docker image
    print_info "Building Docker image: $IMAGE_NAME:$IMAGE_TAG"
    docker build -t "$IMAGE_NAME:$IMAGE_TAG" "$BUILD_DIR"
    
    # Clean up
    rm -rf "$BUILD_DIR"
    
    print_success "Binary build completed!"
}

# Function to build from local workspace
build_from_local() {
    print_info "Starting build from local workspace..."
    
    # Navigate to project root
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
    
    print_info "Project root directory: $PROJECT_ROOT"
    
    # Create build context with proper error handling
    BUILD_DIR=$(mktemp -d -t curvine-source-build-XXXXXX)
    if [ ! -d "$BUILD_DIR" ]; then
        print_error "Failed to create temporary build directory"
        exit 1
    fi
    print_info "Creating build context: $BUILD_DIR"
    
    # Copy Dockerfile for source build
    cp "$SCRIPT_DIR/Dockerfile" "$BUILD_DIR/"
    
    # Copy entrypoint script (use compile-specific entrypoint.sh, similar to thin-runtime)
    if [ -f "$SCRIPT_DIR/entrypoint.sh" ]; then
        cp "$SCRIPT_DIR/entrypoint.sh" "$BUILD_DIR/"
        print_info "Using compile-specific entrypoint.sh"
    elif [ -f "$PROJECT_ROOT/curvine-docker/deploy/entrypoint.sh" ]; then
        cp "$PROJECT_ROOT/curvine-docker/deploy/entrypoint.sh" "$BUILD_DIR/"
        print_warning "Using deploy/entrypoint.sh as fallback"
    else
        print_error "entrypoint.sh not found"
        exit 1
    fi
    
    # Copy curvine-docker/compile directory for build configs
    # Copy the entire compile directory to match thin-runtime pattern
    cp -r "$SCRIPT_DIR" "$BUILD_DIR/compile"
    
    # Copy curvine project source code
    print_info "Copying Curvine project source code..."
    mkdir -p "$BUILD_DIR/workspace"
    
    # Copy curvine directories (the actual source code)
    for dir in curvine-cli curvine-client curvine-common curvine-server curvine-libsdk curvine-tests curvine-fuse curvine-web curvine-ufs curvine-s3-gateway curvine-kube orpc; do
        if [ -d "$PROJECT_ROOT/$dir" ]; then
            print_info "Copying $dir..."
            cp -r "$PROJECT_ROOT/$dir" "$BUILD_DIR/workspace/"
        fi
    done
    
    # Copy root-level build files (essential for the build process)
    cp "$PROJECT_ROOT/Cargo.toml" "$BUILD_DIR/workspace/" 2>/dev/null || true
    cp "$PROJECT_ROOT/Cargo.lock" "$BUILD_DIR/workspace/" 2>/dev/null || true
    cp "$PROJECT_ROOT/Makefile" "$BUILD_DIR/workspace/" 2>/dev/null || true
    cp "$PROJECT_ROOT/rust-toolchain.toml" "$BUILD_DIR/workspace/" 2>/dev/null || true
    cp "$PROJECT_ROOT/rustfmt.toml" "$BUILD_DIR/workspace/" 2>/dev/null || true
    cp "$PROJECT_ROOT/clippy.toml" "$BUILD_DIR/workspace/" 2>/dev/null || true

    # Copy build directory so that `make all` works inside the Docker image
    if [ -d "$PROJECT_ROOT/build" ]; then
        print_info "Copying build directory..."
        cp -r "$PROJECT_ROOT/build" "$BUILD_DIR/workspace/"
    fi

    # Copy etc directory for configurations
    if [ -d "$PROJECT_ROOT/etc" ]; then
        cp -r "$PROJECT_ROOT/etc" "$BUILD_DIR/workspace/"
    fi
    
    # Build Docker image with source code
    print_info "Building Docker image (with source compilation): $IMAGE_NAME:$IMAGE_TAG"
    print_warning "Note: Source build requires longer time, please be patient..."
    
    # Build with increased shared memory (for RocksDB compilation)
    docker build --shm-size=2g -t "$IMAGE_NAME:$IMAGE_TAG" "$BUILD_DIR"
    
    # Clean up
    rm -rf "$BUILD_DIR"
    
    print_success "Local workspace build completed!"
    print_info "Note: This build uses your local source code, not a fresh Git clone"
}

# Main execution
main() {
    # Check if docker is available
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    # Check if docker daemon is running
    if ! docker info &> /dev/null; then
        print_error "Docker daemon is not running, please start Docker"
        exit 1
    fi
    
    # Interactive mode selection
    select_build_mode
    
    # Execute selected build mode
    case $BUILD_MODE in
        "binary")
            build_from_binaries
            ;;
        "local")
            build_from_local
            ;;
    esac
    
    echo ""
    print_success "Docker image built successfully: $IMAGE_NAME:$IMAGE_TAG"
    echo ""
    echo "Test the image:"
    echo "docker run --rm $IMAGE_NAME:$IMAGE_TAG master start"
    echo ""
    echo "Push the image:"
    echo "docker push $IMAGE_NAME:$IMAGE_TAG"
    echo ""
    echo "Load into minikube:"
    echo "minikube image load $IMAGE_NAME:$IMAGE_TAG"
    echo ""
}

# Check for help flag
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    echo "Curvine Native K8s Docker Build Tool"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help     Show this help message"
    echo ""
    echo "Build modes:"
    echo "  1. Build from pre-built binaries (Recommended)"
    echo "  2. Build from local workspace"
    echo ""
    echo "Examples:"
    echo "  $0                    # Interactive build mode selection"
    echo ""
    exit 0
fi

# Run main function
main
