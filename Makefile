.PHONY: format build cargo docker-build

# Detect system type and set shell accordingly
SYSTEM_TYPE := $(shell uname -s)
IS_UBUNTU := $(shell grep -q -i ubuntu /etc/os-release 2>/dev/null && echo 1 || echo 0)

# Set shell command based on system type
SHELL_CMD := sh
ifeq ($(IS_UBUNTU),1)
  # Ubuntu system - use bash
  SHELL_CMD := bash
endif

# 1. Format the project
format:
	$(SHELL_CMD) build/pre-commit.sh

# 2. Build and package the project (depends on format)
build: format
	$(SHELL_CMD) build/build.sh

# 3. Other modules through cargo command
cargo:
	cargo $(ARGS)

# 4. Build compilation image under curvine-docker
docker-build:
	docker build -t curvine-build -f curvine-docker/deploy/Dockerfile.build curvine-docker

MODE ?= debug

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

# 5. All in one
all: build