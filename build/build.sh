#!/usr/bin/env bash

#
# Copyright 2025 OPPO.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

# curvine package command
# ./build, debug mode
# ./build release, release mode.
FS_HOME="$(cd "`dirname "$0"`/.."; pwd)"

# Check if cargo is available
if ! command -v cargo &> /dev/null; then
    echo "Error: cargo is not installed or not in PATH" >&2
    exit 1
fi

get_arch_name() {
    arch=$(uname -m)
    case $arch in
        x86_64)
            echo "x86_64"
            ;;
        i386 | i686)
            echo "x86_32"
            ;;
        aarch64 | arm64)
             echo "aarch_64"
            ;;
        armv7l | armv6l)
            echo "aarch_32"
            ;;
        *)
            echo "unknown"
            ;;
    esac
}

get_os_version() {
  if [ -f "/etc/os-release" ]; then
    id=$(grep -E '^ID=' /etc/os-release | cut -d= -f2- | tr -d '"')
    ver=$(grep ^VERSION_ID= /etc/os-release | cut -d '"' -f 2| cut -d '.' -f 1)
    echo $id$ver
  elif [[ "$OSTYPE" == "darwin"* ]]; then
    echo "mac"
  else
    echo "unknown"
  fi
}

get_fuse_version() {
  if command -v fusermount3 > /dev/null 2>&1; then
      echo "fuse3"
  elif command -v fusermount > /dev/null 2>&1; then
      echo "fuse2"
  else
      echo ""  # No FUSE available
  fi
}

# maturin is a Rust tool; install via cargo so builds do not depend on pip mirrors or venv pip.
ensure_maturin_cmd() {
  if command -v maturin >/dev/null 2>&1; then
    MATURIN_CMD=(maturin)
    return 0
  fi
  if [ -x "${HOME}/.cargo/bin/maturin" ]; then
    MATURIN_CMD=("${HOME}/.cargo/bin/maturin")
    return 0
  fi

  echo "maturin not found in PATH; installing via cargo ..."
  cargo install maturin --locked
  if command -v maturin >/dev/null 2>&1; then
    MATURIN_CMD=(maturin)
    return 0
  fi
  if [ -x "${HOME}/.cargo/bin/maturin" ]; then
    MATURIN_CMD=("${HOME}/.cargo/bin/maturin")
    return 0
  fi

  echo "Error: maturin install via cargo failed." >&2
  return 1
}

print_help() {
  echo "Usage: $0 [options]"
  echo
  echo "Options:"
  echo "  -p, --package PACKAGE  Package to build (can be specified multiple times, default: all)"
  echo "                        Available packages:"
  echo "                          - core: includes server, client, and cli"
  echo "                          - server: server component"
  echo "                          - client: client component"
  echo "                          - cli: command line interface"
  echo "                          - web: web interface"
  echo "                          - fuse: FUSE filesystem"
  echo "                          - java: Java SDK"
  echo "                          - python: Python SDK"
  echo "                          - rust-sdk: Rust SDK (rlib + cdylib)"
  echo "                          - tests: test suite and benchmarks"
  echo "                          - all: all packages"
  echo
  echo "  -u, --ufs TYPE        UFS storage type (can be specified multiple times, default: opendal-s3)"
  echo "                        Available types:"
  echo "                          - opendal-s3: OpenDAL S3"
  echo "                          - opendal-oss: OpenDAL OSS"
  echo "                          - opendal-azblob: OpenDAL Azure Blob"
  echo "                          - opendal-gcs: OpenDAL GCS"
  echo "                          - opendal-hdfs: OpenDAL HDFS (native, includes JNI)"
  echo "                          - opendal-webhdfs: OpenDAL WebHDFS"
  echo "                          - oss-hdfs: OSS-HDFS (JindoSDK)"
  echo
  echo "  -d, --debug           Build in debug mode (default: release mode)"
  echo "  -f, --features LIST   Comma-separated list of extra features to enable"
  echo "  -z, --zip             Create zip archive"
  echo "  --spdk                Enable SPDK NVMe-oF initiator support for the server-native build (TCP transport)"
  echo "  --spdk-rdma           Enable SPDK NVMe-oF initiator support for the server-native build (TCP + RDMA transport)"
  echo "  --spdk-dir PATH       Path to pre-built SPDK installation (default: /opt/spdk or \$SPDK_DIR)"
  echo "  --skip-java-sdk        Skip Java SDK compilation (useful for Docker builds)"
  echo "  --skip-python-sdk      Skip Python SDK compilation (useful for Docker builds)"
  echo "  -h, --help            Show this help message"
  echo
  echo "Examples:"
  echo "  $0                                      # Build all packages in release mode with opendal-s3"
  echo "  $0 --package core --ufs s3             # Build core packages with server, client and cli"
  echo "  $0 -p web --package fuse --debug       # Build web and fuse in debug mode"
  echo "  $0 --package all --ufs opendal-s3 -z   # Build all packages with OpenDAL S3 and create zip"
  echo "  $0 --ufs opendal-hdfs --ufs opendal-webhdfs  # Build with HDFS support"
  echo "  $0 --ufs oss-hdfs                         # Build with OSS-HDFS support (JindoSDK)"
  echo "  $0 --features jni --package client     # Build client with JNI support"
  echo "  $0 -p server --spdk                     # Build server-native artifacts with SPDK TCP initiator support"
  echo "  $0 -p server --spdk-rdma                # Build server-native artifacts with SPDK RDMA initiator support"
  echo "  $0 -p core --spdk-rdma --spdk-dir /opt/spdk"
  echo "                                          # Build server-native SPDK/RDMA separately from client/CLI artifacts"
  echo "  $0 --skip-java-sdk                      # Build all packages except Java SDK"
  echo "  $0 --skip-python-sdk                    # Build all packages except Python SDK"
  echo "  $0 -p java -p python                    # Build both Java and Python SDKs"
}

# Create a version file.
GIT_VERSION="unknown"
if command -v git &> /dev/null && git rev-parse --git-dir &> /dev/null; then
    GIT_VERSION=$(git rev-parse --short HEAD)
fi

# Get the necessary environment parameters
ARCH_NAME=$(get_arch_name)
OS_VERSION=$(get_os_version)
FUSE_VERSION=$(get_fuse_version)
CURVINE_VERSION=$(grep '^version =' "$FS_HOME/Cargo.toml" | sed 's/^version = "\(.*\)"/\1/')

# Package Directory
DIST_DIR="$FS_HOME/build/dist"
DIST_ZIP=curvine-${CURVINE_VERSION}-${ARCH_NAME}-${OS_VERSION}.zip

# Process command parameters
PROFILE="--release"
declare -a PACKAGES=("all")  # Default to build all packages
declare -a UFS_TYPES=("opendal-s3")  # Default UFS type
declare -a EXTRA_FEATURES=()  # From -f only; --alloc is merged into FEATURES later
ALLOC=jemalloc
CRATE_ZIP=""
SKIP_JAVA_SDK=0    # Flag to skip Java SDK compilation
SKIP_PYTHON_SDK=0  # Flag to skip Python SDK compilation
ENABLE_SPDK=0      # Flag to enable SPDK TCP initiator support
ENABLE_SPDK_RDMA=0 # Flag to enable SPDK RDMA initiator support
SPDK_DIR="${SPDK_DIR:-}"  # Path to pre-built SPDK installation

# Parse command line arguments. Avoid external getopt: BSD getopt silently
# rewrites GNU-style long options differently and can make local runs build all.
require_option_value() {
  local opt="$1"
  if [ $# -lt 2 ] || [ -z "$2" ]; then
    echo "Error: ${opt} requires a value" >&2
    print_help >&2
    exit 1
  fi
}

add_package_arg() {
  if [ ${#PACKAGES[@]} -eq 1 ] && [ "${PACKAGES[0]}" = "all" ]; then
    PACKAGES=()
  fi
  PACKAGES+=("$1")
}

add_features_arg() {
  local feature
  local -a FEATURE_ARRAY=()
  IFS=',' read -ra FEATURE_ARRAY <<< "$1"
  for feature in "${FEATURE_ARRAY[@]}"; do
    EXTRA_FEATURES+=("$feature")
  done
}

while [ $# -gt 0 ]; do
  case "$1" in
    -p|--package)
      require_option_value "$1" "${2:-}"
      add_package_arg "$2"
      shift 2
      ;;
    --package=*)
      add_package_arg "${1#*=}"
      shift
      ;;
    -u|--ufs)
      require_option_value "$1" "${2:-}"
      UFS_TYPES+=("$2")
      shift 2
      ;;
    --ufs=*)
      UFS_TYPES+=("${1#*=}")
      shift
      ;;
    -f|--features)
      require_option_value "$1" "${2:-}"
      add_features_arg "$2"
      shift 2
      ;;
    --features=*)
      add_features_arg "${1#*=}"
      shift
      ;;
    -a|--alloc)
      require_option_value "$1" "${2:-}"
      ALLOC="$2"
      shift 2
      ;;
    --alloc=*)
      ALLOC="${1#*=}"
      shift
      ;;
    -d|--debug)
      PROFILE=""
      shift
      ;;
    -z|--zip)
      CRATE_ZIP="zip"
      shift
      ;;
    --spdk)
      ENABLE_SPDK=1
      shift
      ;;
    --spdk-rdma)
      ENABLE_SPDK_RDMA=1
      shift
      ;;
    --spdk-dir)
      require_option_value "$1" "${2:-}"
      SPDK_DIR="$2"
      shift 2
      ;;
    --spdk-dir=*)
      SPDK_DIR="${1#*=}"
      shift
      ;;
    --skip-java-sdk)
      SKIP_JAVA_SDK=1
      shift
      ;;
    --skip-python-sdk)
      SKIP_PYTHON_SDK=1
      shift
      ;;
    -h|--help)
      print_help
      exit 0
      ;;
    --)
      shift
      if [ $# -gt 0 ]; then
        echo "Error: unexpected positional arguments: $*" >&2
        print_help >&2
        exit 1
      fi
      break
      ;;
    *)
      echo "Unknown argument: $1" >&2
      print_help
      exit 1
      ;;
  esac
done

# Set target directory based on PROFILE
# TARGET_DIR is used for file paths (release or debug)
if [ -z "$PROFILE" ]; then
  TARGET_DIR="debug"
else
  TARGET_DIR="release"
fi

# Check if "all" is specified along with other packages
for pkg in "${PACKAGES[@]}"; do
  if [ "$pkg" = "all" ] && [ ${#PACKAGES[@]} -gt 1 ]; then
    echo "Error: 'all' cannot be combined with other packages" >&2
    exit 1
  fi
done

# Handle core package
if [[ " ${PACKAGES[@]} " =~ " core " ]]; then
  # Replace core with its components
  PACKAGES=("${PACKAGES[@]/core/}")
  PACKAGES+=("server" "client" "cli")
fi

# Export UFS types as comma-separated string
CURVINE_UFS_TYPE=$(IFS=,; echo "${UFS_TYPES[*]}")
SERVER_NATIVE_PROFILE="none"
if [ $ENABLE_SPDK_RDMA -eq 1 ]; then
  SERVER_NATIVE_PROFILE="spdk-rdma"
elif [ $ENABLE_SPDK -eq 1 ]; then
  SERVER_NATIVE_PROFILE="spdk"
fi

# Create necessary directories
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"/conf
mkdir -p "$DIST_DIR"/bin
mkdir -p "$DIST_DIR"/lib
mkdir -p "$DIST_DIR"/tests


# Copy configuration files and directories.
cp -R "$FS_HOME"/etc/. "$DIST_DIR"/conf/

cp "$FS_HOME"/build/bin/* "$DIST_DIR"/bin
chmod +x "$DIST_DIR"/bin/*

# Copy tests (including scripts directory)
cp -R "$FS_HOME"/build/tests/. "$DIST_DIR"/tests/

# Ensure test scripts are executable (avoid failing on empty globs)
chmod +x "$DIST_DIR"/tests/*.sh "$DIST_DIR"/tests/scripts/* 2>/dev/null || true


# Write version file
echo "commit=$GIT_VERSION" > "$DIST_DIR"/build-version
echo "os=${OS_VERSION}_$ARCH_NAME" >> "$DIST_DIR"/build-version
echo "fuse=${FUSE_VERSION:-none}" >> "$DIST_DIR"/build-version
echo "version=$CURVINE_VERSION" >> "$DIST_DIR"/build-version
echo "ufs_types=${CURVINE_UFS_TYPE}" >> "$DIST_DIR"/build-version
echo "server_native_profile=${SERVER_NATIVE_PROFILE}" >> "$DIST_DIR"/build-version


# Check if a package should be built
should_build_package() {
  local package=$1
  if [[ " ${PACKAGES[@]} " =~ " all " ]]; then
    return 0
  fi
  if [[ " ${PACKAGES[@]} " =~ " $package " ]]; then
    return 0
  fi
  return 1
}

BUILD_JAVA_SDK=0
if should_build_package "java" && [ $SKIP_JAVA_SDK -eq 0 ]; then
  BUILD_JAVA_SDK=1
fi

BUILD_PYTHON_SDK=0
if should_build_package "python" && [ $SKIP_PYTHON_SDK -eq 0 ]; then
  BUILD_PYTHON_SDK=1
fi

BUILD_RUST_SDK=0
if should_build_package "rust-sdk"; then
  BUILD_RUST_SDK=1
fi

# Collect rust packages by release profile. Keep server-native features out of
# client entrypoints so Cargo feature unification cannot leak native links.
declare -a SERVER_RUST_BUILD_ARGS=()
declare -a CLIENT_RUST_BUILD_ARGS=()
declare -a CLI_RUST_BUILD_ARGS=()
declare -a TEST_RUST_BUILD_ARGS=()
declare -a COPY_TARGETS=()
declare -a CLIENT_ARTIFACT_CHECK_TARGETS=()

# Add required packages
if should_build_package "server"; then
  SERVER_RUST_BUILD_ARGS+=("-p" "curvine-server")
  COPY_TARGETS+=("curvine-server")
fi

if should_build_package "client"; then
  CLIENT_RUST_BUILD_ARGS+=("-p" "curvine-client")
  # COPY_TARGETS+=("curvine-client")
fi

if should_build_package "cli"; then
  CLI_RUST_BUILD_ARGS+=("-p" "curvine-cli")
  COPY_TARGETS+=("curvine-cli")
  CLIENT_ARTIFACT_CHECK_TARGETS+=("curvine-cli")
fi

# Add optional rust packages
if should_build_package "fuse" && [ -n "$FUSE_VERSION" ]; then
  CLIENT_RUST_BUILD_ARGS+=("-p" "curvine-fuse")
  COPY_TARGETS+=("curvine-fuse")
  CLIENT_ARTIFACT_CHECK_TARGETS+=("curvine-fuse")
fi

if should_build_package "tests"; then
  TEST_RUST_BUILD_ARGS+=("-p" "curvine-tests")
  COPY_TARGETS+=("curvine-bench")
fi

declare -a FEATURES=()
declare -a CLIENT_SKIPPED_NATIVE_UFS=()

validate_feature_name() {
  local feature="$1"
  case "$feature" in
    *[!a-zA-Z0-9_./-]*|"")
      echo "Error: invalid feature value: ${feature}" >&2
      return 1
      ;;
  esac
}

add_feature() {
  local feature="$1"
  local existing
  validate_feature_name "$feature" || exit 1
  for existing in "${FEATURES[@]}"; do
    if [ "$existing" = "$feature" ]; then
      return 0
    fi
  done
  FEATURES+=("$feature")
}

remember_skipped_native_ufs() {
  local ufs="$1"
  local existing
  for existing in "${CLIENT_SKIPPED_NATIVE_UFS[@]}"; do
    if [ "$existing" = "$ufs" ]; then
      return 0
    fi
  done
  CLIENT_SKIPPED_NATIVE_UFS+=("$ufs")
}

is_client_native_ufs() {
  case "$1" in
    oss-hdfs|opendal-hdfs|opendal-hdfs-native)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

append_ufs_feature() {
  local scope="$1"
  local ufs="$2"

  case "$ufs" in
    *[!a-zA-Z0-9_-]*|"")
      echo "Error: invalid --ufs value: ${ufs}" >&2
      exit 1
      ;;
  esac

  if [ "$scope" = "client-safe" ] && is_client_native_ufs "$ufs"; then
    remember_skipped_native_ufs "$ufs"
    return 0
  fi

  case "$ufs" in
    oss-hdfs)
      add_feature "curvine-ufs/oss-hdfs"
      add_feature "curvine-client/oss-hdfs"
      ;;
    opendal-hdfs)
      add_feature "curvine-ufs/opendal-hdfs"
      add_feature "curvine-client/opendal-hdfs"
      add_feature "curvine-ufs/jni"
      add_feature "curvine-server/jni"
      ;;
    opendal-webhdfs)
      add_feature "curvine-ufs/opendal-webhdfs"
      add_feature "curvine-client/opendal-webhdfs"
      ;;
    *)
      add_feature "curvine-client/$ufs"
      ;;
  esac
}

append_ufs_features() {
  local scope="$1"
  local ufs
  for ufs in "${UFS_TYPES[@]}"; do
    append_ufs_feature "$scope" "$ufs"
  done
}

append_extra_features() {
  local scope="$1"
  local feature
  for feature in "${EXTRA_FEATURES[@]}"; do
    validate_feature_name "$feature" || exit 1
    case "$feature" in
      oss-hdfs|opendal-hdfs|opendal-webhdfs|opendal-cos|opendal-hdfs-native)
        append_ufs_feature "$scope" "$feature"
        ;;
      jni|curvine-server/jni|curvine-ufs/jni)
        if [ "$scope" = "server-native" ] || [ "$scope" = "tests" ]; then
          add_feature "curvine-ufs/jni"
          add_feature "curvine-server/jni"
        else
          remember_skipped_native_ufs "jni"
        fi
        ;;
      spdk|curvine-server/spdk)
        if [ "$scope" = "server-native" ] || [ "$scope" = "tests" ]; then
          add_feature "curvine-server/spdk"
        fi
        ;;
      spdk-rdma|curvine-server/spdk-rdma)
        if [ "$scope" = "server-native" ] || [ "$scope" = "tests" ]; then
          add_feature "curvine-server/spdk-rdma"
        fi
        ;;
      orpc/spdk|orpc/spdk-rdma|curvine-ufs/oss-hdfs|curvine-ufs/opendal-hdfs|curvine-ufs/opendal-hdfs-native|curvine-client/oss-hdfs|curvine-client/opendal-hdfs|curvine-client/opendal-hdfs-native)
        if [ "$scope" = "server-native" ] || [ "$scope" = "tests" ]; then
          add_feature "$feature"
        else
          remember_skipped_native_ufs "$feature"
        fi
        ;;
      curvine-server/*)
        if [ "$scope" = "server-native" ] || [ "$scope" = "tests" ]; then
          add_feature "$feature"
        fi
        ;;
      *)
        add_feature "$feature"
        ;;
    esac
  done
}

append_alloc_feature() {
  add_feature "curvine-common/${ALLOC}"
}

feature_list() {
  local IFS=,
  echo "$*"
}

build_rust_packages() {
  local label="$1"
  shift
  local -a build_args=()
  while [ $# -gt 0 ] && [ "$1" != "--" ]; do
    build_args+=("$1")
    shift
  done
  if [ $# -eq 0 ]; then
    echo "Internal error: missing feature separator for ${label}" >&2
    exit 1
  fi
  shift
  local -a features=("$@")

  if [ ${#build_args[@]} -eq 0 ]; then
    return 0
  fi

  local -a build_cmd=(cargo build)
  if [ -n "$PROFILE" ]; then
    build_cmd+=("$PROFILE")
  fi
  build_cmd+=("${build_args[@]}")
  if [ ${#features[@]} -gt 0 ]; then
    build_cmd+=(--no-default-features --features "$(feature_list "${features[@]}")")
  fi

  echo "Building ${label} crates with command: ${build_cmd[*]}"
  "${build_cmd[@]}"
}

artifact_dynamic_entries() {
  local inspector="$1"
  local artifact="$2"

  case "$inspector" in
    readelf)
      readelf -d "$artifact" 2>/dev/null | grep -E 'NEEDED|RPATH|RUNPATH' || true
      ;;
    llvm-readelf)
      llvm-readelf -d "$artifact" 2>/dev/null | grep -E 'NEEDED|RPATH|RUNPATH' || true
      ;;
    otool)
      otool -L "$artifact" 2>/dev/null || true
      ;;
  esac
}

check_minimal_artifact_deps() {
  local label="$1"
  local artifact="$2"

  if [ ! -e "$artifact" ]; then
    return 0
  fi

  local inspector=""
  if command -v readelf >/dev/null 2>&1 && readelf -h "$artifact" >/dev/null 2>&1; then
    inspector="readelf"
  elif command -v llvm-readelf >/dev/null 2>&1 && llvm-readelf -h "$artifact" >/dev/null 2>&1; then
    inspector="llvm-readelf"
  elif command -v otool >/dev/null 2>&1 && otool -hv "$artifact" >/dev/null 2>&1; then
    inspector="otool"
  else
    echo "Warn: no readelf/llvm-readelf/otool inspector found; skipping runtime dependency check for ${label}" >&2
    return 0
  fi

  if [ -z "$inspector" ]; then
    echo "Skipping runtime dependency check for non-ELF artifact: ${label}"
    return 0
  fi

  local needed
  needed="$(artifact_dynamic_entries "$inspector" "$artifact")"
  if grep -E 'libibverbs\.so|librdmacm\.so|libspdk|librte_|libjindosdk|libhdfs|libjvm|libjli' <<<"$needed" >/dev/null; then
    echo "Error: ${label} contains native server/storage runtime dependencies forbidden for minimal client artifacts:" >&2
    echo "$needed" >&2
    exit 1
  fi
  echo "OK runtime deps: ${label}"
}

# Join libsdk SDK feature + UFS passthrough features (forwarded to curvine-client).
libsdk_features() {
  local sdk_feature="$1"
  local alloc_feature="$2"
  local features="${alloc_feature},${sdk_feature}"
  local ufs
  for ufs in "${UFS_TYPES[@]}"; do
    case "$ufs" in
      *[!a-zA-Z0-9_-]*|"")
        echo "Error: invalid --ufs value: ${ufs}" >&2
        return 1
        ;;
    esac
    if is_client_native_ufs "$ufs"; then
      remember_skipped_native_ufs "$ufs"
      continue
    fi
    features="${features},${ufs}"
  done
  echo "$features"
}

build_curvine_libsdk() {
  local sdk_feature="$1"
  local features
  features="$(libsdk_features "$sdk_feature" "curvine-common/system")" || exit 1
  # Invoke cargo via argv (no eval) so --ufs values cannot inject shell metacharacters.
  local -a sdk_cmd=(cargo build)
  if [ -n "$PROFILE" ]; then
    sdk_cmd+=("$PROFILE")
  fi
  sdk_cmd+=(-p curvine-libsdk --no-default-features --features "$features")
  echo "Building curvine-libsdk with features: ${features}"
  echo "Build command: ${sdk_cmd[*]}"
  "${sdk_cmd[@]}"
}

# Check FUSE availability if needed
if should_build_package "fuse" || [[ " ${PACKAGES[@]} " =~ " all " ]]; then
  if [ -z "$FUSE_VERSION" ]; then
    echo "Warn: FUSE package requested but FUSE is not available on this system" >&2
  fi
fi

FEATURES=()
append_ufs_features "server-native"
append_extra_features "server-native"
append_alloc_feature
if [ $ENABLE_SPDK -eq 1 ]; then
  add_feature "curvine-server/spdk"
  echo "Enabling SPDK NVMe-oF initiator support (TCP transport)"
fi
if [ $ENABLE_SPDK_RDMA -eq 1 ]; then
  add_feature "curvine-server/spdk-rdma"
  echo "Enabling SPDK NVMe-oF initiator support (TCP + RDMA transport)"
fi
SERVER_FEATURES=("${FEATURES[@]}")

FEATURES=()
if [ ${#CLIENT_RUST_BUILD_ARGS[@]} -gt 0 ]; then
  if [[ " ${CLIENT_RUST_BUILD_ARGS[@]} " =~ " -p curvine-fuse " ]] && [ -n "$FUSE_VERSION" ]; then
    add_feature "curvine-fuse/$FUSE_VERSION"
  fi
  append_ufs_features "client-safe"
fi
append_extra_features "client-safe"
append_alloc_feature
CLIENT_FEATURES=("${FEATURES[@]}")

FEATURES=()
append_extra_features "cli-minimal"
append_alloc_feature
CLI_FEATURES=("${FEATURES[@]}")

FEATURES=()
append_ufs_features "tests"
append_extra_features "tests"
append_alloc_feature
if [ $ENABLE_SPDK -eq 1 ]; then
  add_feature "curvine-server/spdk"
fi
if [ $ENABLE_SPDK_RDMA -eq 1 ]; then
  add_feature "curvine-server/spdk-rdma"
fi
TEST_FEATURES=("${FEATURES[@]}")

if [ ${#CLIENT_SKIPPED_NATIVE_UFS[@]} -gt 0 ]; then
  echo "Client-safe/minimal artifacts skip native server/storage features: ${CLIENT_SKIPPED_NATIVE_UFS[*]}"
fi

# Set SPDK_DIR environment variable for build.rs
if [ -n "$SPDK_DIR" ]; then
  export SPDK_DIR
  echo "Using SPDK_DIR=${SPDK_DIR}"
elif [ -d "/opt/spdk" ]; then
  export SPDK_DIR="/opt/spdk"
  echo "Using default SPDK_DIR=/opt/spdk"
fi

# Skip cargo build when no non-SDK rust package was selected
if [ ${#SERVER_RUST_BUILD_ARGS[@]} -eq 0 ] && [ ${#CLIENT_RUST_BUILD_ARGS[@]} -eq 0 ] && [ ${#CLI_RUST_BUILD_ARGS[@]} -eq 0 ] && [ ${#TEST_RUST_BUILD_ARGS[@]} -eq 0 ]; then
  echo "No non-SDK rust packages selected, skipping workspace cargo build..."
else
  build_rust_packages "server-native" "${SERVER_RUST_BUILD_ARGS[@]}" -- "${SERVER_FEATURES[@]}"
  build_rust_packages "tests" "${TEST_RUST_BUILD_ARGS[@]}" -- "${TEST_FEATURES[@]}"
  build_rust_packages "client-safe" "${CLIENT_RUST_BUILD_ARGS[@]}" -- "${CLIENT_FEATURES[@]}"
  build_rust_packages "cli-minimal" "${CLI_RUST_BUILD_ARGS[@]}" -- "${CLI_FEATURES[@]}"
fi

if [ $BUILD_JAVA_SDK -eq 1 ]; then
  build_curvine_libsdk "java-sdk"
  # Copy JNI native before Python SDK build (if any) overwrites target/.
  mkdir -p "$FS_HOME"/curvine-libsdk/java/native
  if [ -e "$FS_HOME/target/${TARGET_DIR}/curvine_libsdk.dll" ]; then
    cp -f "$FS_HOME/target/${TARGET_DIR}/curvine_libsdk.dll" "$FS_HOME/curvine-libsdk/java/native/curvine_libsdk.dll"
  elif [ -e "$FS_HOME/target/${TARGET_DIR}/libcurvine_libsdk.so" ]; then
    cp -f "$FS_HOME/target/${TARGET_DIR}/libcurvine_libsdk.so" "$FS_HOME/curvine-libsdk/java/native/libcurvine_libsdk_${OS_VERSION}_$ARCH_NAME.so"
  fi
fi

if [ $BUILD_RUST_SDK -eq 1 ]; then
  build_curvine_libsdk "rust-sdk"
  mkdir -p "$DIST_DIR"/lib
  if [ -e "$FS_HOME/target/${TARGET_DIR}/curvine_libsdk.dll" ]; then
    cp -f "$FS_HOME/target/${TARGET_DIR}/curvine_libsdk.dll" "$DIST_DIR/lib/curvine_libsdk.dll"
  elif [ -e "$FS_HOME/target/${TARGET_DIR}/libcurvine_libsdk.so" ]; then
    cp -f "$FS_HOME/target/${TARGET_DIR}/libcurvine_libsdk.so" \
      "$DIST_DIR/lib/libcurvine_libsdk_${OS_VERSION}_$ARCH_NAME.so"
  fi
fi

# Build optional non-rust packages
if should_build_package "web"; then
  echo "Building WebUI..."
  cd "$FS_HOME"/curvine-web/webui
  npm install
  npm run build
  mv "$FS_HOME"/curvine-web/webui/dist "$DIST_DIR"/webui
fi

if [ $BUILD_JAVA_SDK -eq 1 ]; then
  # Native library was copied immediately after the java-sdk cargo build.
  # Build java package
  cd "$FS_HOME"/curvine-libsdk/java
  mvn protobuf:compile package -DskipTests -P${TARGET_DIR}
  if [ $? -ne 0 ]; then
    echo "Java build failed. Exiting..."
    exit 1
  fi
  cp "$FS_HOME"/curvine-libsdk/java/target/curvine-hadoop-*.jar "$DIST_DIR"/lib
fi

if [ $BUILD_PYTHON_SDK -eq 1 ]; then
  if ! command -v protoc >/dev/null 2>&1; then
    echo "Error: protoc is required to build the Python SDK wheel. Install Protobuf 3+." >&2
    exit 1
  fi

  PYTHON3_BIN=""
  if command -v python3 >/dev/null 2>&1; then
    PYTHON3_BIN=python3
  elif command -v python >/dev/null 2>&1; then
    PYTHON3_BIN=python
  else
    echo "Error: python3 (or python) is required to build the Python SDK wheel." >&2
    exit 1
  fi

  # Isolated venv for the wheel interpreter (no manual activation; works under sh and bash).
  PYTHON_SDK_VENV="${CURVINE_PYTHON_SDK_VENV:-$FS_HOME/build/.venv-python-sdk}"
  if [ -d "$PYTHON_SDK_VENV" ]; then
    if [ ! -f "$PYTHON_SDK_VENV/pyvenv.cfg" ]; then
      echo "Error: ${PYTHON_SDK_VENV} exists but is not a Python venv (missing pyvenv.cfg)." >&2
      exit 1
    fi
  else
    echo "Creating Python SDK build venv at ${PYTHON_SDK_VENV} ..."
    "$PYTHON3_BIN" -m venv "$PYTHON_SDK_VENV" || {
      echo "Error: $PYTHON3_BIN -m venv failed (install python3-venv on Debian/Ubuntu)." >&2
      exit 1
    }
  fi

  ensure_maturin_cmd || exit 1

  PROTO_DIR="$FS_HOME/curvine-common/proto"
  PY_SDK_PY="$FS_HOME/curvine-libsdk/python"
  PROTO_PKG="$PY_SDK_PY/curvine_libsdk/_proto"
  echo "Generating Python protobuf stubs into curvine_libsdk/python/curvine_libsdk/_proto/..."
  mkdir -p "$PROTO_PKG"
  protoc -I"$PROTO_DIR" --python_out="$PROTO_PKG" "$PROTO_DIR"/*.proto
  for f in "$PROTO_PKG"/*_pb2.py; do
    if [ -f "$f" ]; then
      sed -i -E 's/^import ([A-Za-z0-9_]+_pb2)( as .*)$/from . import \1\2/' "$f"
    fi
  done

  MATURIN_RELEASE=()
  if [ -n "$PROFILE" ]; then
    MATURIN_RELEASE=(--release)
  fi

  # Use native 'linux' tag + skip auditwheel repair by default so wheels install on a wide
  # range of glibc baselines. (Strict manylinux_2_34-style tags often break uv/pip on older
  # manylinux_2_32-class hosts.) For PyPI uploads, set e.g. CURVINE_MATURIN_COMPATIBILITY=pypi
  # and CURVINE_MATURIN_AUDITWHEEL=repair.
  MATURIN_COMPAT="${CURVINE_MATURIN_COMPATIBILITY:-linux}"
  MATURIN_AUDIT="${CURVINE_MATURIN_AUDITWHEEL:-skip}"

  echo "Building Python wheel (maturin) into ${DIST_DIR}/lib ..."
  cd "$FS_HOME/curvine-libsdk"
  PY_SDK_FEATURES="$(libsdk_features "python-sdk" "curvine-common/${ALLOC}")" || exit 1
  echo "maturin features: ${PY_SDK_FEATURES}"
  "${MATURIN_CMD[@]}" build --no-default-features \
    --features "${PY_SDK_FEATURES}" \
    "${MATURIN_RELEASE[@]}" \
    --interpreter "$PYTHON_SDK_VENV/bin/python" \
    --compatibility "$MATURIN_COMPAT" \
    --auditwheel "$MATURIN_AUDIT" \
    --out "$DIST_DIR/lib"
  if [ $? -ne 0 ]; then
    echo "maturin build failed. Exiting..."
    exit 1
  fi

  # Optional legacy native artifacts (same binary as inside the wheel).
  mkdir -p "$FS_HOME"/curvine-libsdk/python/native
  if [ -e "$FS_HOME/target/${TARGET_DIR}/curvine_libsdk.dll" ]; then
    cp -f "$FS_HOME/target/${TARGET_DIR}/curvine_libsdk.dll" \
      "$FS_HOME/curvine-libsdk/python/native/curvine_libsdk_python.dll"
    cp -f "$FS_HOME/target/${TARGET_DIR}/curvine_libsdk.dll" \
      "$DIST_DIR/lib/curvine_libsdk_python.dll"
  elif [ -e "$FS_HOME/target/${TARGET_DIR}/libcurvine_libsdk.so" ]; then
    cp -f "$FS_HOME/target/${TARGET_DIR}/libcurvine_libsdk.so" \
      "$FS_HOME/curvine-libsdk/python/native/libcurvine_libsdk_python_${OS_VERSION}_$ARCH_NAME.so"
    cp -f "$FS_HOME/target/${TARGET_DIR}/libcurvine_libsdk.so" \
      "$DIST_DIR/lib/libcurvine_libsdk_python_${OS_VERSION}_$ARCH_NAME.so"
  fi
fi

# Copy workspace binaries after all cargo builds (workspace + JNI libsdk + maturin's Rust build).
echo "Copying Rust binaries into ${DIST_DIR}/lib ..."
for target in "${COPY_TARGETS[@]}"; do
  cp -f "$FS_HOME"/target/${TARGET_DIR}/${target} "$DIST_DIR"/lib
done

echo "Checking minimal client artifact runtime dependencies ..."
for target in "${CLIENT_ARTIFACT_CHECK_TARGETS[@]}"; do
  check_minimal_artifact_deps "$target" "$DIST_DIR/lib/$target"
done
for sdk_artifact in "$DIST_DIR"/lib/libcurvine_libsdk*.so "$DIST_DIR"/lib/curvine_libsdk*.dll; do
  if [ -e "$sdk_artifact" ]; then
    check_minimal_artifact_deps "$(basename "$sdk_artifact")" "$sdk_artifact"
  fi
done

# create zip
cd "$DIST_DIR"
if [[ ${CRATE_ZIP} = "zip" ]]; then
  zip -m -r "$DIST_ZIP" *
  echo "build success, file: $DIST_DIR/$DIST_ZIP"
else
    echo "build success, dir: $DIST_DIR"
fi
