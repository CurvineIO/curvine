.PHONY: help check-env format build cargo docker-build docker-build-img docker-build-cached all dist dist-only

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
	@echo "  make format                      - Format code using pre-commit hooks"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-build                - Build using Docker compilation image"
	@echo "  make docker-build-cached         - Build using cached Docker compilation image"
	@echo "  make docker-build-img            - Build compilation Docker image (interactive)"
	@echo ""
	@echo "CSI (Container Storage Interface):"
	@echo "  make csi-build                   - Build curvine-csi Go binary"
	@echo "  make csi-run                     - Run curvine-csi from source"
	@echo "  make csi-docker-build            - Build curvine-csi Docker image"
	@echo "  make csi-docker-push             - Push curvine-csi Docker image"
	@echo "  make csi-docker                  - Build and push curvine-csi Docker image"
	@echo "  make csi-docker-fast             - Build curvine-csi Docker image quickly (no push)"
	@echo "  make csi-fmt                     - Format curvine-csi Go code"
	@echo "  make csi-vet                     - Run go vet on curvine-csi code"
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
	@echo "  make build-hdfs                             - Build with HDFS support (native + WebHDFS)"
	@echo "  make build-webhdfs                          - Build with WebHDFS support only"
	@echo "  make dist                                   - Build and create distribution package"
	@echo "  RELEASE_VERSION=v1.0.0 make dist           - Build and package with specific version"
	@echo "  make cargo ARGS='test --verbose'            - Run cargo test with verbose output"
	@echo "  make csi-docker-fast                        - Build curvine-csi Docker image quickly"

# 1. Check build environment dependencies
check-env:
	$(SHELL_CMD) build/check-env.sh

# 2. Format the project
format:
	$(SHELL_CMD) build/pre-commit.sh

# 3. Build and package the project (depends on environment check and format)
build: check-env
	$(SHELL_CMD) build/build.sh $(ARGS)

# 4. Other modules through cargo command
cargo:
	cargo $(ARGS)

# 5. Build through docker compilation image
docker-build:
	docker run --rm --entrypoint="" -v $(PWD):/workspace -w /workspace curvine/curvine-compile:latest bash -c "make all"

docker-build-cached:
	docker run --rm --entrypoint="" -v $(PWD):/workspace -w /workspace curvine/curvine-compile:build-cached bash -c "make all"

# 6. Build compilation image under curvine-docker
docker-build-img:
	@echo "Please select the system type to build:"
	@echo "1) Rocky Linux 9"
	@echo "2) Ubuntu 22.04"
	@read -p "Enter your choice (1 or 2): " choice; \
	case $$choice in \
		1) \
			echo "Building Rocky Linux 9 compilation image..."; \
			docker build -t curvine-build -f curvine-docker/compile/Dockerfile_rocky9 curvine-docker/compile ;; \
		2) \
			echo "Building Ubuntu 22.04 compilation image..."; \
			docker build -t curvine-build -f curvine-docker/compile/Dockerfile_ubuntu22 curvine-docker/compile ;; \
		*) \
			echo "Invalid option!" ;; \
	esac

# 8. CSI (Container Storage Interface) targets - delegate to curvine-csi/Makefile
.PHONY: csi-build csi-run csi-fmt csi-vet csi-docker-build csi-docker-push csi-docker csi-docker-fast

# Build curvine-csi Go binary
csi-build:
	@echo "Building curvine-csi..."
	cd curvine-csi && go fmt ./... && go vet ./... && go build -o bin/csi main.go

# Run curvine-csi from source
csi-run:
	@echo "Running curvine-csi..."
	cd curvine-csi && go fmt ./... && go vet ./... && go run ./main.go

# Format curvine-csi Go code
csi-fmt:
	@echo "Formatting curvine-csi Go code..."
	cd curvine-csi && go fmt ./...

# Run go vet on curvine-csi code
csi-vet:
	@echo "Running go vet on curvine-csi code..."
	cd curvine-csi && go vet ./...

# Build curvine-csi Docker image (from root context)
csi-docker-build:
	@echo "Building curvine-csi Docker image..."
	docker build --build-arg GOPROXY=https://goproxy.cn,direct -t curvine-csi:latest -f curvine-csi/Dockerfile .

# Push curvine-csi Docker image
csi-docker-push:
	@echo "Pushing curvine-csi Docker image..."
	docker push curvine-csi:latest

# Build and push curvine-csi Docker image
csi-docker:
	@echo "Building and pushing curvine-csi Docker image..."
	docker build --build-arg GOPROXY=https://goproxy.cn,direct -t curvine-csi:latest -f curvine-csi/Dockerfile .
	docker push curvine-csi:latest

# Build curvine-csi Docker image quickly (no push)
csi-docker-fast:
	@echo "Building curvine-csi Docker image (fast)..."
	docker build --build-arg GOPROXY=https://goproxy.cn,direct -t curvine-csi:latest -f curvine-csi/Dockerfile .

# 7. HDFS-specific builds
.PHONY: build-hdfs build-webhdfs setup-hdfs

# Build with HDFS support (native HDFS + WebHDFS)
build-hdfs: check-env
	@echo "Building Curvine with HDFS support..."
	$(SHELL_CMD) build/build.sh --features opendal-hdfs,opendal-webhdfs,jni $(ARGS)

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
	cd build && tar -czf "../$${DIST_NAME}.tar.gz" dist/; \
	echo "Distribution package created: $${DIST_NAME}.tar.gz"; \
	ls -lh "../$${DIST_NAME}.tar.gz"