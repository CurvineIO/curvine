#!/bin/bash

set -e

# Configuration
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

# Interactive platform selection
select_platform() {
    echo "==========================================="
    echo "    Curvine Native K8s Image Builder"
    echo "==========================================="
    echo ""
    echo "Please select build platform:"
    echo ""
    echo "1) Ubuntu 24.04"
    echo "   - Build from local workspace with Ubuntu 24.04 base image"
    echo "   - Copy local source code to container for compilation"
    echo ""
    echo "2) Rocky Linux 9"
    echo "   - Build from local workspace with Rocky Linux 9 base image"
    echo "   - Copy local source code to container for compilation"
    echo ""
    echo "3) Exit"
    echo ""
    read -p "Please enter your choice (1-3): " choice
    
    case $choice in
        1)
            DOCKERFILE_NAME="Dockerfile_ubuntu24"
            PLATFORM_NAME="Ubuntu 24.04"
            print_info "Selected: Ubuntu 24.04"
            ;;
        2)
            DOCKERFILE_NAME="Dockerfile_rocky9"
            PLATFORM_NAME="Rocky Linux 9"
            print_info "Selected: Rocky Linux 9"
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

# Function to build Docker image from local workspace
build_image() {
    print_info "Starting build from local workspace ($PLATFORM_NAME)..."
    
    # Get script directory and project root
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
    
    print_info "Project root directory: $PROJECT_ROOT"
    
    # Validate Dockerfile exists
    if [ ! -f "$SCRIPT_DIR/$DOCKERFILE_NAME" ]; then
        print_error "Dockerfile not found: $SCRIPT_DIR/$DOCKERFILE_NAME"
        exit 1
    fi
    
    # Create build context
    BUILD_DIR=$(mktemp -d -t curvine-build-XXXXXX)
    if [ ! -d "$BUILD_DIR" ]; then
        print_error "Failed to create temporary build directory"
        exit 1
    fi
    print_info "Creating build context: $BUILD_DIR"
    
    # Copy Dockerfile
    cp "$SCRIPT_DIR/$DOCKERFILE_NAME" "$BUILD_DIR/Dockerfile"
    print_info "Using $DOCKERFILE_NAME"
    
    # Copy entrypoint script
    if [ -f "$SCRIPT_DIR/entrypoint.sh" ]; then
        cp "$SCRIPT_DIR/entrypoint.sh" "$BUILD_DIR/"
        print_info "Copied entrypoint.sh"
    else
        print_error "entrypoint.sh not found at $SCRIPT_DIR/entrypoint.sh"
        rm -rf "$BUILD_DIR"
        exit 1
    fi
    
    # Copy build config files (settings.xml and config) for Dockerfile build stage
    # Dockerfile expects these files in a 'compile' directory (COPY compile/ /build-config/)
    mkdir -p "$BUILD_DIR/compile"
    
    if [ -f "$SCRIPT_DIR/settings.xml" ]; then
        cp "$SCRIPT_DIR/settings.xml" "$BUILD_DIR/compile/"
        print_info "Copied settings.xml"
    else
        print_error "settings.xml not found at $SCRIPT_DIR/settings.xml"
        rm -rf "$BUILD_DIR"
        exit 1
    fi
    
    if [ -f "$SCRIPT_DIR/config" ]; then
        cp "$SCRIPT_DIR/config" "$BUILD_DIR/compile/"
        print_info "Copied config"
    else
        print_error "config not found at $SCRIPT_DIR/config"
        rm -rf "$BUILD_DIR"
        exit 1
    fi
    
    # Copy source code
    print_info "Copying Curvine project source code..."
    mkdir -p "$BUILD_DIR/workspace"
    
    # Copy curvine directories
    for dir in curvine-cli curvine-client curvine-common curvine-server curvine-libsdk curvine-tests curvine-fuse curvine-web curvine-ufs curvine-s3-gateway curvine-kube orpc; do
        if [ -d "$PROJECT_ROOT/$dir" ]; then
            print_info "Copying $dir..."
            cp -r "$PROJECT_ROOT/$dir" "$BUILD_DIR/workspace/"
        fi
    done
    
    # Copy root-level build files
    cp "$PROJECT_ROOT/Cargo.toml" "$BUILD_DIR/workspace/" 2>/dev/null || true
    cp "$PROJECT_ROOT/Cargo.lock" "$BUILD_DIR/workspace/" 2>/dev/null || true
    cp "$PROJECT_ROOT/Makefile" "$BUILD_DIR/workspace/" 2>/dev/null || true
    cp "$PROJECT_ROOT/rust-toolchain.toml" "$BUILD_DIR/workspace/" 2>/dev/null || true
    cp "$PROJECT_ROOT/rustfmt.toml" "$BUILD_DIR/workspace/" 2>/dev/null || true
    cp "$PROJECT_ROOT/clippy.toml" "$BUILD_DIR/workspace/" 2>/dev/null || true

    # Copy build directory if exists
    if [ -d "$PROJECT_ROOT/build" ]; then
        print_info "Copying build directory..."
        cp -r "$PROJECT_ROOT/build" "$BUILD_DIR/workspace/"
    fi

    # Copy etc directory if exists
    if [ -d "$PROJECT_ROOT/etc" ]; then
        cp -r "$PROJECT_ROOT/etc" "$BUILD_DIR/workspace/"
    fi
    
    # Build Docker image
    print_info "Building Docker image: $IMAGE_NAME:$IMAGE_TAG"
    print_warning "Note: Source build requires longer time, please be patient..."
    
    # Build with increased shared memory (for RocksDB compilation)
    if ! docker build --shm-size=2g -t "$IMAGE_NAME:$IMAGE_TAG" "$BUILD_DIR"; then
        print_error "Docker build failed"
        rm -rf "$BUILD_DIR"
        exit 1
    fi
    
    # Clean up
    rm -rf "$BUILD_DIR"
    
    print_success "Build completed!"
    print_info "Note: This build uses your local source code"
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
    
    # Interactive platform selection
    select_platform
    
    # Build image
    build_image
    
    echo ""
    print_success "Docker image built successfully: $IMAGE_NAME:$IMAGE_TAG"
    echo ""
    echo "Test the image:"
    echo "  docker run --rm $IMAGE_NAME:$IMAGE_TAG master start"
    echo ""
    echo "Push the image:"
    echo "  docker push $IMAGE_NAME:$IMAGE_TAG"
    echo ""
    echo "Load into minikube:"
    echo "  minikube image load $IMAGE_NAME:$IMAGE_TAG"
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
    echo "Build platforms:"
    echo "  1. Ubuntu 24.04"
    echo "  2. Rocky Linux 9"
    echo ""
    echo "Examples:"
    echo "  $0                    # Interactive platform selection"
    echo ""
    exit 0
fi

# Run main function
main
