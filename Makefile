.PHONY: help check-env format build cargo docker-build docker-build-img docker-build-cached fuse server cli ufs all

# Build mode configuration (debug or release)
MODE ?= release

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
	@echo "  make build [MODE=debug|release]  - Check environment, format and build the entire project (default: release)"
	@echo "  make all                         - Same as 'make build'"
	@echo "  make format                      - Format code using pre-commit hooks"
	@echo ""
	@echo "Individual Components:"
	@echo "  make fuse [MODE=debug|release]   - Build curvine-fuse component only"
	@echo "  make server [MODE=debug|release] - Build curvine-server component only"
	@echo "  make cli [MODE=debug|release]    - Build curvine-cli component only"
	@echo "  make ufs [MODE=debug|release]    - Build curvine-ufs component only"
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
	@echo "  MODE=debug     - Build in debug mode (default, faster compilation)"
	@echo "  MODE=release   - Build in release mode (optimized, slower compilation)"
	@echo ""
	@echo "Examples:"
	@echo "  make build                       - Build entire project in debug mode"
	@echo "  make build MODE=release          - Build entire project in release mode"
	@echo "  make server MODE=release         - Build only server component in release mode"
	@echo "  make cargo ARGS='test --verbose' - Run cargo test with verbose output"
	@echo "  make csi-docker-fast             - Build curvine-csi Docker image quickly"

# 1. Check build environment dependencies
check-env:
	$(SHELL_CMD) build/check-env.sh

# 2. Format the project
format:
	$(SHELL_CMD) build/pre-commit.sh

# 3. Build and package the project (depends on environment check and format)
build: check-env format
	$(SHELL_CMD) build/build.sh $(MODE)

# 4. Other modules through cargo command
cargo:
	cargo $(ARGS)

# 5. Build through docker compilation image
docker-build:
	docker run --rm -v $(PWD):/workspace -w /workspace curvine/curvine-compile:latest make all

docker-build-cached:
	docker run --rm -v $(PWD):/workspace -w /workspace curvine/curvine-compile:build-cached make all

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

CARGO_FLAGS :=
ifeq ($(MODE),release)
  CARGO_FLAGS := --release
endif

# Build curvine-fuse
fuse: check-env
	@if [ -n "$$CURVINE_FUSE_FEATURE" ]; then \
		echo "Building curvine-fuse with feature: $$CURVINE_FUSE_FEATURE"; \
		cargo build -p curvine-fuse --features "$$CURVINE_FUSE_FEATURE" $(CARGO_FLAGS); \
	else \
		echo "FUSE not available, skipping curvine-fuse build"; \
		exit 1; \
	fi

# Build curvine-server
server:
	cargo build -p curvine-server $(CARGO_FLAGS)

# Build curvine-cli
cli:
	cargo build -p curvine-cli $(CARGO_FLAGS)

# Build curvine-ufs
ufs:
	cargo build -p curvine-ufs $(CARGO_FLAGS)

# 7. All in one
all: build