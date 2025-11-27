#!/bin/bash

set -e

# Configuration
IMAGE_NAME="curvine"
IMAGE_TAG="latest"
DOCKERFILE_NAME=""
PLATFORM_NAME=""

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
    # If DOCKERFILE_NAME is already set (non-interactive mode), skip selection
    if [ -n "$DOCKERFILE_NAME" ]; then
        print_info "Using platform: $PLATFORM_NAME"
        return 0
    fi
    
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
    print_info "Using Dockerfile: $DOCKERFILE_NAME"
    
    # Validate Dockerfile exists
    if [ ! -f "$SCRIPT_DIR/$DOCKERFILE_NAME" ]; then
        print_error "Dockerfile not found: $SCRIPT_DIR/$DOCKERFILE_NAME"
        exit 1
    fi
    
    # Validate .dockerignore exists
    if [ ! -f "$PROJECT_ROOT/.dockerignore" ]; then
        print_warning ".dockerignore not found, build context may be large"
    fi
    
    # Build Docker image directly from project root
    print_info "Building Docker image: $IMAGE_NAME:$IMAGE_TAG"
    print_warning "Note: Source build requires longer time, please be patient..."
    
    # Build with project root as context
    # .dockerignore will filter out unnecessary files
    if ! docker build \
        --shm-size=2g \
        -f "$SCRIPT_DIR/$DOCKERFILE_NAME" \
        -t "$IMAGE_NAME:$IMAGE_TAG" \
        "$PROJECT_ROOT"; then
        print_error "Docker build failed"
        exit 1
    fi
    
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

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -h|--help)
                echo "Curvine Native K8s Docker Build Tool"
                echo ""
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  -h, --help          Show this help message"
                echo "  -p, --platform      Specify platform: ubuntu24 or rocky9"
                echo "  -t, --tag           Specify image tag (default: latest)"
                echo "  -n, --name          Specify image name (default: curvine)"
                echo ""
                echo "Build platforms:"
                echo "  ubuntu24   - Ubuntu 24.04"
                echo "  rocky9     - Rocky Linux 9"
                echo ""
                echo "Examples:"
                echo "  $0                                    # Interactive platform selection"
                echo "  $0 --platform rocky9                  # Non-interactive build with Rocky9"
                echo "  $0 -p ubuntu24 -t v1.0.0              # Build Ubuntu24 with custom tag"
                echo "  $0 -p rocky9 -n myapp -t latest       # Build with custom name and tag"
                echo ""
                exit 0
                ;;
            -p|--platform)
                case "$2" in
                    ubuntu24)
                        DOCKERFILE_NAME="Dockerfile_ubuntu24"
                        PLATFORM_NAME="Ubuntu 24.04"
                        ;;
                    rocky9)
                        DOCKERFILE_NAME="Dockerfile_rocky9"
                        PLATFORM_NAME="Rocky Linux 9"
                        ;;
                    *)
                        print_error "Invalid platform: $2. Use 'ubuntu24' or 'rocky9'"
                        exit 1
                        ;;
                esac
                shift 2
                ;;
            -t|--tag)
                IMAGE_TAG="$2"
                shift 2
                ;;
            -n|--name)
                IMAGE_NAME="$2"
                shift 2
                ;;
            *)
                print_error "Unknown option: $1"
                echo "Use -h or --help for usage information"
                exit 1
                ;;
        esac
    done
}

# Parse arguments and run main function
parse_args "$@"
main
