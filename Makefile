.PHONY: help check-env format format-csi build cargo docker-build docker-build-compile docker-compile docker-build-fluid-cache docker-build-fluid-thin docker-build-fluid all dist dist-only

# Default target when running 'make' without arguments
.DEFAULT_GOAL := help

# Detect system type and set shell accordingly
SYSTEM_TYPE := $(shell uname -s)
IS_UBUNTU := $(shell grep -q -i ubuntu /etc/os-release 2>/dev/null && echo 1 || echo 0)

# Set shell command based on system type
SHELL_CMD := sh
ifeq ($(IS_UBUNTU),1)
  # Ubuntu system - use bash
  SHELL_CMD := bash
endif

# Default target - show help
help:
	@echo "Curvine Build System - Available Commands:"
	@echo ""
	@echo "Environment:"
	@echo "  make check-env                   - Check build environment dependencies"
	@echo ""
	@echo "Building:"
	@echo "  make build ARGS='<args>'         - Build with specific arguments passed to build.sh"
	@echo "  make all                         - Same as 'make build'"
	@echo "  make dist                        - Build and create distribution package (tar.gz)"
	@echo "  make dist-only                   - Create distribution package without building"
	@echo "  make format                      - Format Rust code using pre-commit hooks"
	@echo "  make format-csi                  - Format curvine-csi Go code"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-build                - Build runtime Docker image from source (compile + package)"
	@echo "  make docker-build-compile         - Build compilation Docker image (interactive)"
	@echo "  make docker-compile               - Compile code in Docker container (output to local build/dist)"
	@echo ""
	@echo "Fluid (Kubernetes):"
	@echo "  make docker-build-fluid          - Build unified Fluid Docker image (supports both cache-runtime and thin-runtime)"
	@echo ""
	@echo "CSI (Container Storage Interface):"
	@echo "  make curvine-csi                 - Build curvine-csi Docker image"
	@echo ""
	@echo "Other:"
	@echo "  make cargo ARGS='<args>'         - Run arbitrary cargo commands"
	@echo "  make help                        - Show this help message"
	@echo ""
	@echo "Parameters:"
	@echo "  ARGS='<args>'         - Additional arguments to pass to build.sh"
	@echo "  RELEASE_VERSION='...' - Version string for distribution packages (optional)"
	@echo ""
	@echo "Examples:"
	@echo "  make build                                  - Build entire project in release mode"
	@echo "  make build ARGS='-d'                       - Build entire project in debug mode"
	@echo "  make build ARGS='-p server -p client'       - Build only server and client components"
	@echo "  make build ARGS='-p object'                  - Build S3 object gateway"
	@echo "  make build ARGS='--package core --ufs s3'   - Build core packages with S3 native SDK"
	@echo "  make build ARGS='--skip-java-sdk'               - Build all packages except Java SDK"
	@echo "  make build-hdfs                             - Build with HDFS support (native + WebHDFS)"
	@echo "  make build-webhdfs                          - Build with WebHDFS support only"
	@echo "  make dist                                   - Build and create distribution package"
	@echo "  RELEASE_VERSION=v1.0.0 make dist           - Build and package with specific version"
	@echo "  make cargo ARGS='test --verbose'            - Run cargo test with verbose output"
	@echo "  make curvine-csi                            - Build curvine-csi Docker image"
	@echo "  make docker-build-fluid                  - Build unified Fluid Docker image"

# 1. Check build environment dependencies
check-env:
	@if echo "$(ARGS)" | grep -q -- '--skip-java-sdk'; then \
		$(SHELL_CMD) build/check-env.sh --skip-java-sdk; \
	else \
		$(SHELL_CMD) build/check-env.sh; \
	fi

# 2. Format the project
format:
	$(SHELL_CMD) build/pre-commit.sh

# 2.1. Format curvine-csi Go code
format-csi:
	$(SHELL_CMD) build/format-csi.sh

# 3. Build and package the project (depends on environment check and format)
build: check-env
	$(SHELL_CMD) build/build.sh $(ARGS)

# 4. Other modules through cargo command
cargo:
	cargo $(ARGS)

# 5. Build runtime Docker image from source (compile + package into image)
docker-build:
	@echo "Building runtime Docker image from source..."
	@bash curvine-docker/deploy/build-image.sh

# 6. Build compilation Docker image (interactive)
docker-build-compile:
	@echo "Please select the system type to build compilation image:"
	@echo "1) Rocky Linux 9"
	@echo "2) Ubuntu 22.04"
	@read -p "Enter your choice (1 or 2): " choice; \
	case $$choice in \
		1) \
			echo "Building Rocky Linux 9 compilation image..."; \
			docker build -t curvine/curvine-compile:latest -f curvine-docker/compile/Dockerfile_rocky9 curvine-docker/compile ;; \
		2) \
			echo "Building Ubuntu 22.04 compilation image..."; \
			docker build -t curvine/curvine-compile:latest -f curvine-docker/compile/Dockerfile_ubuntu22 curvine-docker/compile ;; \
		*) \
			echo "Invalid option!" ;; \
	esac

# 7. Compile code in Docker container (output to local build/dist)
docker-compile:
	@echo "Compiling code in Docker container..."
	docker run --rm --entrypoint="" -v $(PWD):/workspace -w /workspace curvine/curvine-compile:build-cached bash -c "make all"

# 7.1. Build Fluid CacheRuntime Docker image
docker-build-fluid-cache:
	@echo "Building Fluid CacheRuntime Docker image..."
	@bash curvine-docker/fluid/cache-runtime/build-image.sh

# 7.2. Build Fluid ThinRuntime Docker image
docker-build-fluid-thin:
	@echo "Building Fluid ThinRuntime Docker image..."
	@bash curvine-docker/fluid/thin-runtime/build-image.sh

# 7.3. Build unified Fluid Docker image
docker-build-fluid:
	@echo "Building unified Fluid Docker image..."
	@echo "This image supports both cache-runtime and thin-runtime modes"
	@if ! docker image inspect ghcr.io/curvineio/curvine:latest >/dev/null 2>&1; then \
		echo "Warning: Base image ghcr.io/curvineio/curvine:latest not found locally."; \
		echo "Attempting to pull from registry..."; \
		docker pull ghcr.io/curvineio/curvine:latest || \
		(echo "Error: Failed to pull base image. Please build it first with 'make docker-build' or pull from registry." && exit 1); \
	fi
	@cd curvine-docker/fluid && docker build -f Dockerfile -t curvine-fluid:latest .
	@echo "✓ Unified Fluid Docker image built successfully: curvine-fluid:latest"
	@echo ""
	@echo "Usage examples:"
	@echo "  # CacheRuntime mode:"
	@echo "  docker run -e FLUID_RUNTIME_COMPONENT_TYPE=master curvine-fluid:latest master start"
	@echo ""
	@echo "  # ThinRuntime mode:"
	@echo "  docker run curvine-fluid:latest fluid-thin-runtime"

# 8. CSI (Container Storage Interface) target
.PHONY: curvine-csi

# Build curvine-csi Docker image
curvine-csi:
	@echo "Building curvine-csi Docker image..."
	docker build --build-arg GOPROXY=https://goproxy.cn,direct -t curvine-csi:latest -f curvine-csi/Dockerfile .

# Tag and push curvine-csi image to private registry
.PHONY: curvine-csi-push
curvine-csi-push: curvine-csi
	@echo "Tagging and pushing curvine-csi image to private registry..."
	docker tag curvine-csi:latest curvineio/curvine-csi:latest
	docker push curvineio/curvine-csi:latest
	@echo "✓ Image pushed successfully: curvineio/curvine-csi:latest"

# Quick iteration build for CSI development (only rebuilds CSI binary)
# Prerequisite: curvine-csi:latest must exist (run 'make curvine-csi' first)
.PHONY: curvine-csi-quick
curvine-csi-quick:
	@echo "Quick building curvine-csi (CSI binary only)..."
	@if ! docker image inspect curvine-csi:latest >/dev/null 2>&1; then \
		echo "Error: Base image curvine-csi:latest not found. Please run 'make curvine-csi' first."; \
		exit 1; \
	fi
	@echo "Building CSI binary locally..."
	@cd curvine-csi && GOPROXY=https://goproxy.cn,direct go build -o csi-binary main.go || exit 1
	@echo "Building Docker image with new CSI binary..."
	docker build -t curvine-csi:latest -f curvine-csi/Dockerfile.quick .
	@rm -f curvine-csi/csi-binary

# Quick iteration build and push
.PHONY: curvine-csi-quick-push
curvine-csi-quick-push: curvine-csi-quick
	@echo "Tagging and pushing quick-built curvine-csi image to private registry..."
	docker tag curvine-csi:latest curvineio/curvine-csi:latest
	docker push curvineio/curvine-csi:latest
	@echo "✓ Quick-built image pushed successfully: curvineio/curvine-csi:latest"

# 7. HDFS-specific builds
.PHONY: build-hdfs build-webhdfs setup-hdfs

# Build with HDFS support (native HDFS + WebHDFS)
build-hdfs: check-env
	@echo "Building Curvine with HDFS support..."
	$(SHELL_CMD) build/build.sh --ufs opendal-hdfs --ufs opendal-webhdfs $(ARGS)

# 8. All in one
all: build

# 9. Distribution packaging
dist: all
	@$(MAKE) dist-only

dist-only:
	@echo "Creating distribution package..."
	@if [ ! -d "build/dist" ]; then \
		echo "Error: build/dist directory not found. Please run 'make all' first."; \
		exit 1; \
	fi
	@# Get version from environment variable only
	@PLATFORM=$$(uname -s | tr '[:upper:]' '[:lower:]'); \
	ARCH=$$(uname -m); \
	if [ -n "$$RELEASE_VERSION" ]; then \
		VERSION="$$RELEASE_VERSION"; \
		echo "Using provided version: $$VERSION"; \
		if [ -n "$$GITHUB_ACTIONS" ]; then \
			DIST_NAME="curvine-$${VERSION}-$${PLATFORM}-$${ARCH}"; \
			echo "GitHub Actions detected - using clean naming"; \
		else \
			TIMESTAMP=$$(date +%Y%m%d-%H%M%S); \
			DIST_NAME="curvine-$${VERSION}-$${PLATFORM}-$${ARCH}-$${TIMESTAMP}"; \
			echo "Local build - adding timestamp"; \
		fi; \
	else \
		echo "No version provided via RELEASE_VERSION environment variable"; \
		if [ -n "$$GITHUB_ACTIONS" ]; then \
			DIST_NAME="curvine-$${PLATFORM}-$${ARCH}"; \
			echo "GitHub Actions detected - no version in package name"; \
		else \
			TIMESTAMP=$$(date +%Y%m%d-%H%M%S); \
			DIST_NAME="curvine-$${PLATFORM}-$${ARCH}-$${TIMESTAMP}"; \
			echo "Local build - no version, adding timestamp"; \
		fi; \
	fi; \
	echo "Packaging as: $${DIST_NAME}.tar.gz"; \
	cd build/dist && tar -czf "../../$${DIST_NAME}.tar.gz" . && cd ../..; \
	echo "Distribution package created: $${DIST_NAME}.tar.gz"; \
	ls -lh "$${DIST_NAME}.tar.gz"