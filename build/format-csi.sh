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

# Format Go code in curvine-csi
if [ -d "curvine-csi" ]; then
  echo "Formatting curvine-csi Go code..."
  cd curvine-csi && go fmt ./... && go vet ./...
  if [ $? -ne 0 ]; then
    echo "Error: Go format/vet check failed"
    exit 1
  fi
  cd ..
  echo "✓ curvine-csi Go code formatted successfully"
else
  echo "Error: curvine-csi directory not found"
  exit 1
fi
