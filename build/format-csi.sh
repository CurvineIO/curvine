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

# Check Go formatting and run go vet in curvine-csi
if [ -d "curvine-csi" ]; then
  echo "Checking curvine-csi Go code..."
  cd curvine-csi || exit 1
  if ! unformatted=$(gofmt -l .); then
    echo "Error: gofmt failed"
    exit 1
  fi
  if [ -n "$unformatted" ]; then
    echo "Error: these files are not gofmt-formatted (run 'gofmt -w' on them):"
    echo "$unformatted"
    exit 1
  fi
  if ! go vet ./...; then
    echo "Error: go vet failed"
    exit 1
  fi
  cd ..
  echo "✓ curvine-csi Go code is formatted and vetted"
else
  echo "Error: curvine-csi directory not found"
  exit 1
fi
