.PHONY: help check-env format format-csi build cargo docker-build docker-build-compile docker-compile docker-build-fluid-cache docker-build-fluid-thin docker-build-fluid docker-build-spdk-target docker-compose-spdk docker-compose-spdk-down all dist dist-only

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

FLUID_IMAGE ?= curvine-fluid:latest
FLUID_BASE_IMAGE_TAG ?= latest

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
	@echo "                                    FLUID_IMAGE=curvine-fluid:latest FLUID_BASE_IMAGE_TAG=latest"
	@echo ""
	@echo "CSI (Container Storage Interface):"
	@echo "  make curvine-csi                 - Build curvine-csi Docker image"
	@echo "  make curvine-csi-quick           - Quick rebuild CSI + curvine-cli + curvine-fuse (Dockerfile.quick)"
	@echo "  make curvine-csi-quick-push      - Quick build and push to CSI_QUICK_REGISTRY (default localhost:5000)"
	@echo ""
	@echo "SPDK NVMe-oF (Storage Performance Development Kit):"
	@echo "  make docker-build-spdk-target    - Build SPDK NVMe-oF target Docker image"
	@echo "  make docker-compose-spdk         - Start SPDK target + initiator stack (dev/test)"
	@echo "  make docker-compose-spdk-down    - Stop SPDK target + initiator stack"

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
	@echo "  make build ARGS='--skip-java-sdk'            - Build all packages except Java SDK"
	@echo "  make build ARGS='--skip-python-sdk'          - Build all packages except Python SDK"
	@echo "  make build ARGS='-p java -p python'          - Build both Java and Python SDKs"
	@echo "                               (Python wheel: build/dist/lib/curvine_libsdk-*.whl)"
	@echo "  make build-hdfs                             - Build with HDFS support (native + WebHDFS)"
	@echo "  make build-webhdfs                          - Build with WebHDFS support only"
	@echo "  make dist                                   - Build and create distribution package"
	@echo "  RELEASE_VERSION=v1.0.0 make dist           - Build and package with specific version"
	@echo "  make cargo ARGS='test --verbose'            - Run cargo test with verbose output"
	@echo "  make curvine-csi                            - Build curvine-csi Docker image"
	@echo "  make docker-build-fluid                  - Build unified Fluid Docker image"
	@echo "  make docker-build-spdk-target             - Build SPDK NVMe-oF target image"

	@echo "  make docker-compose-spdk                  - Start SPDK dev/test stack"

# 1. Check build environment dependencies
check-env:
	$(SHELL_CMD) build/check-env.sh $(filter --skip-java-sdk --skip-python-sdk,$(ARGS))

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
docker-build-fluid-cache: docker-build-fluid

# 7.2. Build Fluid ThinRuntime Docker image
docker-build-fluid-thin: docker-build-fluid

# 7.3. Build unified Fluid Docker image
docker-build-fluid:
	@echo "Building unified Fluid Docker image..."
	@echo "This image supports both cache-runtime and thin-runtime modes"
	@if ! docker image inspect ghcr.io/curvineio/curvine:$(FLUID_BASE_IMAGE_TAG) >/dev/null 2>&1; then \
		echo "Warning: Base image ghcr.io/curvineio/curvine:$(FLUID_BASE_IMAGE_TAG) not found locally."; \
		echo "Attempting to pull from registry..."; \
		docker pull ghcr.io/curvineio/curvine:$(FLUID_BASE_IMAGE_TAG) || \
		(echo "Error: Failed to pull base image. Please build it first with 'make docker-build' or pull from registry." && exit 1); \
	fi
	@docker build \
		--build-arg BASE_IMAGE_TAG=$(FLUID_BASE_IMAGE_TAG) \
		-f curvine-docker/fluid/Dockerfile \
		-t $(FLUID_IMAGE) \
		curvine-docker/fluid
	@echo "✓ Unified Fluid Docker image built successfully: $(FLUID_IMAGE)"
	@echo ""
	@echo "Usage examples:"
	@echo "  # CacheRuntime mode:"
	@echo "  docker run -e FLUID_RUNTIME_COMPONENT_TYPE=master $(FLUID_IMAGE) master start"
	@echo ""
	@echo "  # ThinRuntime mode:"
	@echo "  docker run $(FLUID_IMAGE) fluid-thin-runtime"

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

# Quick iteration build for CSI development (CSI + curvine-cli + curvine-fuse)
# Prerequisite: ghcr.io/curvineio/curvine-csi:latest base image (runtime libs only)
.PHONY: curvine-csi-quick
# Override for private registry, e.g. make curvine-csi-quick-push CSI_QUICK_REGISTRY=registry.example.com:5000
CSI_QUICK_REGISTRY ?= localhost:5000
curvine-csi-quick:
	@echo "Quick building curvine-csi (CSI + curvine-cli + curvine-fuse)..."
	@if ! docker image inspect ghcr.io/curvineio/curvine-csi:latest >/dev/null 2>&1; then \
		echo "Error: Base image ghcr.io/curvineio/curvine-csi:latest not found."; \
		exit 1; \
	fi
	@echo "Building curvine-cli and curvine-fuse..."
	@cargo build --release -p curvine-cli -p curvine-fuse || exit 1
	@mkdir -p curvine-csi/quick-build
	@cp target/release/curvine-cli target/release/curvine-fuse curvine-csi/quick-build/
	@echo "Building CSI binary locally..."
	@cd curvine-csi && GOPROXY=https://goproxy.cn,direct go build -o csi-binary main.go || exit 1
	@echo "Building Docker image with local binaries..."
	docker build -t curvine-csi:latest -f curvine-csi/Dockerfile.quick .
	docker tag curvine-csi:latest $(CSI_QUICK_REGISTRY)/curvine-csi:latest
	@rm -f curvine-csi/csi-binary
	@rm -rf curvine-csi/quick-build
	@echo "✓ Quick image: curvine-csi:latest and $(CSI_QUICK_REGISTRY)/curvine-csi:latest"

# Quick iteration build and push to local registry
.PHONY: curvine-csi-quick-push
curvine-csi-quick-push: curvine-csi-quick
	@echo "Pushing quick-built curvine-csi image to $(CSI_QUICK_REGISTRY)..."
	docker push $(CSI_QUICK_REGISTRY)/curvine-csi:latest
	@echo "✓ Quick-built image pushed: $(CSI_QUICK_REGISTRY)/curvine-csi:latest"

# 9. SPDK NVMe-oF builds
.PHONY: docker-build-spdk-target docker-compose-spdk docker-compose-spdk-down

# Build SPDK NVMe-oF target Docker image
docker-build-spdk-target:
	@echo "Building SPDK NVMe-oF target Docker image..."
	docker build \
		--target target-runtime \
		-f curvine-docker/deploy/Dockerfile_rocky9 \
		-t curvine-spdk-target:latest \
		.
	@echo "✓ SPDK target image built: curvine-spdk-target:latest"

# Start SPDK target + initiator stack via docker compose (dev/test only)
docker-compose-spdk:
	@echo "Starting SPDK target + initiator stack (dev/test)..."
	@echo "Note: This runs both target and initiator on the same machine."
	@echo "      In production, deploy them on separate physical machines."
	@echo ""
	@echo "Prerequisites:"
	@echo "  sudo sysctl -w vm.nr_hugepages=4096"
	@echo "  sudo modprobe uio_pci_generic"
	@echo "  sudo modprobe nvme nvme-fabrics nvme-tcp"
	@echo ""
	cd curvine-docker/deploy/spdk && docker compose up --build -d
	@echo "✓ SPDK stack started. Check status with: docker compose -f curvine-docker/deploy/spdk/docker-compose.yml ps"

# Stop SPDK target + initiator stack
docker-compose-spdk-down:
	@echo "Stopping SPDK target + initiator stack..."
	cd curvine-docker/deploy/spdk && docker compose down
	@echo "✓ SPDK stack stopped."

# 10. HDFS-specific builds
.PHONY: build-hdfs build-webhdfs setup-hdfs

# Build with HDFS support (native HDFS + WebHDFS)
build-hdfs: check-env
	@echo "Building Curvine with HDFS support..."
	$(SHELL_CMD) build/build.sh --ufs opendal-hdfs --ufs opendal-webhdfs $(ARGS)

# 11. All in one
all: build

# 12. Distribution packaging
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
