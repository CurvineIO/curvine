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

# Run all tests

CURVINE_HOME="$(cd "`dirname "$0"`/.."; pwd)"
cd $CURVINE_HOME

RUN_CLIPPY=false
CLIPPY_LEVEL="deny"
RUN_INTEGRATION=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    -l|--level)
      CLIPPY_LEVEL="$2"
      shift 2
      ;;
    -c|--clippy)
      RUN_CLIPPY=true
      shift
      ;;
    -i|--integration)
      RUN_INTEGRATION=true
      shift
      ;;
    *)
      echo "Usage: $0 [--level warn|deny|allow] [--clippy] [--integration]"
      echo "  --integration  also run the curvine-tests suites, one test at a time (slow: each suite starts its own clusters)."
      echo "                 Default features only: fault-injection, cosn and lancedb tests are not included."
      exit 1
      ;;
  esac
done

# step1: Check the format
if ! cargo fmt -- --check ; then
  echo "Code is not formatted"
  exit 1
fi

# step2: Check code style and potential problems
# run: cargo clippy --release --all-targets -- --deny warnings
# fix: cargo clippy --release --fix --all-targets --allow-dirty --allow-staged --allow-no-vcs -- --deny warnings

if [ "$RUN_CLIPPY" = true ]; then
  if ! cargo clippy --release  --all-targets -- --$CLIPPY_LEVEL warnings; then
    echo "Clippy check failed"
    exit 1
  fi
fi

# step3: Run the unit tests of the default workspace members
if ! cargo test --release; then
  echo "run-tests failed!"
  exit 1
fi

# step4: Run the integration tests. curvine-tests is opt-in (not in default-members),
# and its suites start their own in-process clusters, so no external cluster is needed.
# Tests in one binary share process-global state (e.g. UFS_TEST_PATH), so run them
# one at a time, as CI does.
if [ "$RUN_INTEGRATION" = true ]; then
  if ! cargo test --release -p curvine-tests -- --test-threads=1; then
    echo "run-tests failed!"
    exit 1
  fi
fi

echo "run-tests success!"
