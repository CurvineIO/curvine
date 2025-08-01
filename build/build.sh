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
  else
      echo "fuse2"
  fi
}

# curvine package command
# ./build, debug mode
# ./build release, release mode.
FS_HOME="$(cd "`dirname "$0"`/.."; pwd)"

# Create a version file.
GIT_VERSION=$(git rev-parse HEAD)
if [[ -z "${GIT_VERSION}" ]]; then
  GIT_VERSION=unknown
fi

# Get the necessary environment parameters
ARCH_NAME=$(get_arch_name)
OS_VERSION=$(get_os_version)
FUSE_VERSION=$(get_fuse_version)
CURVINE_VERSION=$(grep '^version =' "$FS_HOME/Cargo.toml" | sed 's/^version = "\(.*\)"/\1/')

# Package Directory
DIST_DIR="$FS_HOME/build/dist/"
DIST_ZIP=curvine-${CURVINE_VERSION}-${ARCH_NAME}-${OS_VERSION}.zip

# Process command parameters
PROFILE=$1
if [ -z "$PROFILE" ] || [ "$PROFILE" = "-r" ]; then
    PROFILE="release"
fi
CRATE_ZIP=$2

# Compile rust
if [[ "${PROFILE}" = "release" ]]; then
  cargo build --features curvine-fuse/"$FUSE_VERSION" --release
else
  cargo build --features curvine-fuse/"$FUSE_VERSION"
fi
if [ $? -ne 0 ]; then
    echo "Cargo build failed. Exiting..."
    exit 1
fi

# Compile webui
echo "Start build WebUI..."
cd "$FS_HOME"/curvine-web/webui
npm install
npm run build


# Create necessary directories
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"/conf
mkdir -p "$DIST_DIR"/bin
mkdir -p "$DIST_DIR"/lib
mkdir -p "$FS_HOME"/curvine-libsdk/java/native

# Copy files
cp "$FS_HOME"/target/${PROFILE}/curvine-server "$DIST_DIR"/lib
cp "$FS_HOME"/target/${PROFILE}/curvine-fuse "$DIST_DIR"/lib
cp "$FS_HOME"/target/${PROFILE}/curvine-client "$DIST_DIR"/lib
cp "$FS_HOME"/target/${PROFILE}/curvine-bench "$DIST_DIR"/lib
cp "$FS_HOME"/etc/* "$DIST_DIR"/conf
cp "$FS_HOME"/build/bin/* "$DIST_DIR"/bin
cp "$FS_HOME"/target/${PROFILE}/curvine-cli "$DIST_DIR"/lib
mv "$FS_HOME"/curvine-web/webui/dist "$DIST_DIR"/webui

# Write to version file
echo "commit=$GIT_VERSION" > "$DIST_DIR"/build-version
echo "os=${OS_VERSION}_$ARCH_NAME" >> "$DIST_DIR"/build-version
echo "fuse=$FUSE_VERSION" >> "$DIST_DIR"/build-version
echo "version=$CURVINE_VERSION" >> "$DIST_DIR"/build-version

chmod +x "$DIST_DIR"/bin/*

# Handle java native.
if [ -e "$FS_HOME/target/${PROFILE}/curvine_libsdk.dll" ]; then
  cp -f "$FS_HOME/target/${PROFILE}/curvine_libsdk.dll" "$FS_HOME/curvine-libsdk/java/native/curvine_libsdk.dll"
elif [ -e "$FS_HOME/target/${PROFILE}/libcurvine_libsdk.so" ]; then
  cp -f "$FS_HOME/target/${PROFILE}/libcurvine_libsdk.so" "$FS_HOME/curvine-libsdk/java/native/libcurvine_libsdk_${OS_VERSION}_$ARCH_NAME.so"
fi

# Build java client service package.
cd "$FS_HOME"/curvine-libsdk/java
mvn clean protobuf:compile package -DskipTests -P${PROFILE}
if [ $? -ne 0 ]; then
    echo "Java build failed. Exiting..."
    exit 1
fi
cp "$FS_HOME"/curvine-libsdk/java/target/curvine-hadoop-*.jar "$DIST_DIR"/lib


# create zip
cd "$DIST_DIR"
if [[ ${CRATE_ZIP} = "zip" ]]; then
  zip -m -r "$DIST_ZIP" *
  echo "build success, file: $DIST_DIR/$DIST_ZIP"
else
    echo "build success, dir: $DIST_DIR"
fi
