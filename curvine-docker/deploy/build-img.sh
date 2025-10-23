#!/bin/bash

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

# Build the curvine deployment image
rm -rf curvine*.zip
cp ../../build/dist/curvine*.zip .

# Check for curvine*.zip
if ls curvine*.zip> /dev/null 2>&1; then
    echo "Found curvine*.zip in current directory"
else
    echo "Error: curvine*.zip not found in current directory or ../../build/dist"
    exit 1
fi

rm -rf dist
mkdir -p dist
unzip -o curvine*.zip -d dist

tag=latest
if [ $# -eq 0 ];then
  echo "lack image tag, usage: sh build latest; now set to $tag"
else
  tag=$1
fi

docker buildx build --no-cache --platform linux/amd64  -t curvine:$tag -f Dockerfile_rocky9 .

echo "build image curvine:$tag"