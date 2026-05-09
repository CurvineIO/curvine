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

# Parse command line arguments
TEMP=$(getopt -o p:u:f:a:dzhv --long package:,ufs:,features:,alloc:,debug,zip,skip-java-sdk,skip-python-sdk,help -n "$0" -- "$@")
if [ $? != 0 ] ; then print_help ; exit 1 ; fi

eval set -- "$TEMP"

while true ; do
  case "$1" in
    -p|--package)
      # If this is the first -p argument, clear the default "all"
      if [ ${#PACKAGES[@]} -eq 1 ] && [ "${PACKAGES[0]}" = "all" ]; then
        PACKAGES=()
      fi
      PACKAGES+=("$2")
      shift 2
      ;;
    -u|--ufs)
      UFS_TYPES+=("$2")
      shift 2
      ;;
    -f|--features)
      # Parse comma-separated features
      IFS=',' read -ra FEATURE_ARRAY <<< "$2"
      for feature in "${FEATURE_ARRAY[@]}"; do
        EXTRA_FEATURES+=("$feature")
      done
      shift 2
      ;;
    -a|--alloc)
      ALLOC="$2"
      shift 2
      ;;
    -d|--debug)
      PROFILE=""
      shift
      ;;
    -z|--zip)
      CRATE_ZIP="zip"
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
      break
      ;;
    *)
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

# Create necessary directories
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"/conf
mkdir -p "$DIST_DIR"/bin
mkdir -p "$DIST_DIR"/lib
mkdir -p "$DIST_DIR"/tests


# Copy configuration files and bin
cp "$FS_HOME"/etc/* "$DIST_DIR"/conf

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

# Collect all rust packages to build
declare -a RUST_BUILD_ARGS=()
declare -a COPY_TARGETS=()

# Add required packages
if should_build_package "server"; then
  RUST_BUILD_ARGS+=("-p" "curvine-server")
  COPY_TARGETS+=("curvine-server")
fi

if should_build_package "client"; then
  RUST_BUILD_ARGS+=("-p" "curvine-client")
  # COPY_TARGETS+=("curvine-client")
fi

if should_build_package "cli"; then
  RUST_BUILD_ARGS+=("-p" "curvine-cli")
  COPY_TARGETS+=("curvine-cli")
fi

# Add optional rust packages
if should_build_package "fuse" && [ -n "$FUSE_VERSION" ]; then
  RUST_BUILD_ARGS+=("-p" "curvine-fuse")
  COPY_TARGETS+=("curvine-fuse")
fi

if should_build_package "tests"; then
  RUST_BUILD_ARGS+=("-p" "curvine-tests")
  COPY_TARGETS+=("curvine-bench")
fi

<<<<<<< HEAD
=======
# Add S3 object gateway package
if should_build_package "object" || [[ " ${PACKAGES[@]} " =~ " all " ]]; then
  RUST_BUILD_ARGS+=("-p" "curvine-s3-gateway")
  COPY_TARGETS+=("curvine-s3-gateway")
fi

build_curvine_libsdk() {
  local sdk_feature="$1"
  local sdk_cmd="cargo build $PROFILE -p curvine-libsdk --no-default-features --features curvine-common/${ALLOC},${sdk_feature}"
  echo "Building curvine-libsdk with feature: ${sdk_feature}"
  echo "Build command: ${sdk_cmd}"
  eval "$sdk_cmd"
}

>>>>>>> b8a002c (build: ship Python SDK wheel in dist and libsdk fixes)
# Base command
cmd="cargo build $PROFILE"

# Add package arguments if any
if [ ${#RUST_BUILD_ARGS[@]} -gt 0 ]; then
  cmd="$cmd ${RUST_BUILD_ARGS[@]}"
fi

# Collect all features
declare -a FEATURES=()

# Check FUSE availability if needed
if [[ " ${RUST_BUILD_ARGS[@]} " =~ " -p curvine-fuse " ]] || [[ " ${PACKAGES[@]} " =~ " all " ]]; then
  if [ -z "$FUSE_VERSION" ]; then
    echo "Warn: FUSE package requested but FUSE is not available on this system" >&2
  fi
fi

# Add features based on what we're actually building
if [ ${#RUST_BUILD_ARGS[@]} -gt 0 ]; then
  # Add FUSE features if we're building fuse
  if [[ " ${RUST_BUILD_ARGS[@]} " =~ " -p curvine-fuse " ]]; then
    FEATURES+=("curvine-fuse/$FUSE_VERSION")
    # FUSE depends on curvine-client, so we need to add UFS features for client
    # to enable OSS and other storage backend support in fuse
    for ufs in "${UFS_TYPES[@]}"; do
      case $ufs in
        oss-hdfs)
          # OSS uses JindoSDK. curvine-client/oss-hdfs already includes curvine-ufs/oss-hdfs,
          # but we specify both explicitly for clarity and to ensure all packages can use it
          FEATURES+=("curvine-ufs/oss-hdfs")
          FEATURES+=("curvine-client/oss-hdfs")
          ;;
        opendal-hdfs)
          # HDFS native support requires JNI
          FEATURES+=("curvine-ufs/opendal-hdfs")
          FEATURES+=("curvine-client/opendal-hdfs")
          FEATURES+=("curvine-ufs/jni")
          FEATURES+=("curvine-server/jni")
          ;;
        opendal-webhdfs)
          # WebHDFS support (no JNI required)
          FEATURES+=("curvine-ufs/opendal-webhdfs")
          FEATURES+=("curvine-client/opendal-webhdfs")
          ;;
        *)
          FEATURES+=("curvine-client/$ufs")
          ;;
      esac
    done
  fi

  # Add UFS features if we're building client
  if [[ " ${RUST_BUILD_ARGS[@]} " =~ " -p curvine-client " ]]; then
    for ufs in "${UFS_TYPES[@]}"; do
      case $ufs in
        oss-hdfs)
          # OSS uses JindoSDK. curvine-client/oss-hdfs already includes curvine-ufs/oss-hdfs,
          # but we specify both explicitly for clarity and to ensure all packages can use it
          FEATURES+=("curvine-ufs/oss-hdfs")
          FEATURES+=("curvine-client/oss-hdfs")
          ;;
        opendal-hdfs)
          # HDFS native support requires JNI
          FEATURES+=("curvine-ufs/opendal-hdfs")
          FEATURES+=("curvine-client/opendal-hdfs")
          FEATURES+=("curvine-ufs/jni")
          FEATURES+=("curvine-server/jni")
          ;;
        opendal-webhdfs)
          # WebHDFS support (no JNI required)
          FEATURES+=("curvine-ufs/opendal-webhdfs")
          FEATURES+=("curvine-client/opendal-webhdfs")
          ;;
        *)
          FEATURES+=("curvine-client/$ufs")
          ;;
      esac
    done
  fi
else
  # If building all packages, add all relevant features
  FEATURES+=("curvine-fuse/$FUSE_VERSION")  # FUSE check already done above
  for ufs in "${UFS_TYPES[@]}"; do
    case $ufs in
      oss-hdfs)
        # OSS uses JindoSDK. curvine-client/oss-hdfs already includes curvine-ufs/oss-hdfs,
        # but we specify both explicitly for clarity and to ensure all packages can use it
        FEATURES+=("curvine-ufs/oss-hdfs")
        FEATURES+=("curvine-client/oss-hdfs")
        ;;
      opendal-hdfs)
        # HDFS native support requires JNI
        FEATURES+=("curvine-ufs/opendal-hdfs")
        FEATURES+=("curvine-client/opendal-hdfs")
        FEATURES+=("curvine-ufs/jni")
        FEATURES+=("curvine-server/jni")
        ;;
      opendal-webhdfs)
        # WebHDFS support (no JNI required)
        FEATURES+=("curvine-ufs/opendal-webhdfs")
        FEATURES+=("curvine-client/opendal-webhdfs")
        ;;
      *)
        FEATURES+=("curvine-client/$ufs")
        ;;
    esac
  done
fi

# Add extra features if specified
if [ ${#EXTRA_FEATURES[@]} -gt 0 ]; then
  for feature in "${EXTRA_FEATURES[@]}"; do
    case $feature in
      opendal-hdfs)
        # HDFS features need to be added to the correct packages
        FEATURES+=("curvine-ufs/opendal-hdfs")
        FEATURES+=("curvine-client/opendal-hdfs")
        ;;
      opendal-webhdfs)
        # WebHDFS features need to be added to the correct packages
        FEATURES+=("curvine-ufs/opendal-webhdfs")
        FEATURES+=("curvine-client/opendal-webhdfs")
        ;;
      opendal-cos)
        # COS features need to be added to the correct packages
        FEATURES+=("curvine-ufs/opendal-cos")
        FEATURES+=("curvine-client/opendal-cos")
        ;;
      oss-hdfs)
        # OSS-HDFS features need to be added to the correct packages
        # curvine-client/oss-hdfs already includes curvine-ufs/oss-hdfs,
        # but we specify both explicitly for clarity and to ensure all packages can use it
        FEATURES+=("curvine-ufs/oss-hdfs")
        FEATURES+=("curvine-client/oss-hdfs")
        ;;
      jni)
        # JNI features need to be added to curvine-ufs and curvine-server
        FEATURES+=("curvine-ufs/jni")
        FEATURES+=("curvine-server/jni")
        ;;
      *)
        # For other features, add as-is (might be package-specific)
        FEATURES+=("$feature")
        ;;
    esac
  done
fi

# Append --alloc as a workspace feature: curvine-common/{jemalloc|mimalloc} → cargo --features
FEATURES+=("curvine-common/${ALLOC}")

# Add features to command if any
if [ ${#FEATURES[@]} -gt 0 ]; then
  # Join features with comma for --features
  IFS=, eval 'FEATURE_LIST="${FEATURES[*]}"'
  cmd="$cmd --no-default-features --features $FEATURE_LIST"
fi

# Skip cargo build when no non-SDK rust package was selected
if [ ${#RUST_BUILD_ARGS[@]} -eq 0 ]; then
  echo "No non-SDK rust packages selected, skipping workspace cargo build..."
else
  echo "Building crates with command: $cmd"
  eval "$cmd"

  if [ $? -ne 0 ]; then
    echo "Cargo build failed. Exiting..."
    exit 1
  fi
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

  if ! command -v python3 >/dev/null 2>&1; then
    echo "Error: python3 is required to build the Python SDK wheel." >&2
    exit 1
  fi
  if ! python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)' 2>/dev/null; then
    echo "Error: Python 3.8+ is required to build the Python SDK wheel (abi3-py38)." >&2
    exit 1
  fi

  # Isolated venv for maturin (no manual activation; works under sh and bash).
  PYTHON_SDK_VENV="${CURVINE_PYTHON_SDK_VENV:-$FS_HOME/build/.venv-python-sdk}"
  PY_SDK_REQ="$FS_HOME/build/requirements-python-sdk.txt"
  if [ ! -f "$PY_SDK_REQ" ]; then
    echo "Error: missing $PY_SDK_REQ" >&2
    exit 1
  fi

  if [ ! -d "$PYTHON_SDK_VENV" ]; then
    echo "Creating Python SDK build venv at ${PYTHON_SDK_VENV} ..."
    python3 -m venv "$PYTHON_SDK_VENV" || {
      echo "Error: python3 -m venv failed (install python3-venv on Debian/Ubuntu)." >&2
      exit 1
    }
  fi

  # Minimal venvs may lack pip; ensure it exists before installing maturin.
  if ! "$PYTHON_SDK_VENV/bin/python" -m pip --version >/dev/null 2>&1; then
    echo "Bootstrapping pip in Python SDK build venv (ensurepip) ..."
    "$PYTHON_SDK_VENV/bin/python" -m ensurepip --upgrade || {
      echo "Error: pip is not available and ensurepip failed (install python3-venv)." >&2
      exit 1
    }
  fi

  echo "Installing / updating Python SDK build dependencies (maturin) ..."
  "$PYTHON_SDK_VENV/bin/python" -m pip install -q --upgrade pip
  "$PYTHON_SDK_VENV/bin/python" -m pip install -q -r "$PY_SDK_REQ"

  MATURIN_CMD=("$PYTHON_SDK_VENV/bin/python" -m maturin)

  PROTO_DIR="$FS_HOME/curvine-common/proto"
  PY_SDK_PY="$FS_HOME/curvine-libsdk/python"
  echo "Generating Python protobuf stubs into curvine-libsdk/python..."
  protoc -I"$PROTO_DIR" --python_out="$PY_SDK_PY" "$PROTO_DIR"/*.proto

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
  "${MATURIN_CMD[@]}" build --no-default-features \
    --features "curvine-common/${ALLOC},python-sdk" \
    "${MATURIN_RELEASE[@]}" \
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

# create zip
cd "$DIST_DIR"
if [[ ${CRATE_ZIP} = "zip" ]]; then
  zip -m -r "$DIST_ZIP" *
  echo "build success, file: $DIST_DIR/$DIST_ZIP"
else
    echo "build success, dir: $DIST_DIR"
fi
