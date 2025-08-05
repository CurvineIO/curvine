.PHONY: help format build cargo docker-build docker-build-img docker-build-cached fuse server cli ufs all

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
	@echo "Building:"
	@echo "  make build [MODE=debug|release]  - Format and build the entire project (default: release)"
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

# 1. Format the project
format:
	$(SHELL_CMD) build/pre-commit.sh

# 2. Build and package the project (depends on format)
build: format
	$(SHELL_CMD) build/build.sh $(MODE)

# 3. Other modules through cargo command
cargo:
	cargo $(ARGS)

# 4. Build through docker compilation image
docker-build:
	docker run --rm -v $(PWD):/workspace -w /workspace curvine/curvine-compile:latest make all

docker-build-cached:
	docker run --rm -v $(PWD):/workspace -w /workspace curvine/curvine-compile:build-cached make all

# 5. Build compilation image under curvine-docker
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

CARGO_FLAGS :=
ifeq ($(MODE),release)
  CARGO_FLAGS := --release
endif

# Build curvine-fuse
fuse:
	cargo build -p curvine-fuse $(CARGO_FLAGS)

# Build curvine-server
server:
	cargo build -p curvine-server $(CARGO_FLAGS)

# Build curvine-cli
cli:
	cargo build -p curvine-cli $(CARGO_FLAGS)

# Build curvine-ufs
ufs:
	cargo build -p curvine-ufs $(CARGO_FLAGS)

# 6. All in one
all: build