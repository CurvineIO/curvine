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

pkill -9 -f "test_cluster"

RUN_CLIPPY=false
CLIPPY_LEVEL="deny"

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
    *)
      echo "Usage: $0 [--level warn|deny|allow] [--clippy]"
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

# step3：Start the test cluster
nohup cargo run --release --example test_cluster &
SERVER_PID=$!

sleep 3

if kill -0 ${SERVER_PID} > /dev/null 2>&1; then
  echo "test_cluster start success, pid=${SERVER_PID}"
else
  echo "test_cluster start fail"
  kill -9 $SERVER_PID
  exit 1
fi


# step4: Run all tests
if ! cargo test --release; then
  echo "run-tests failed!"
  kill -9 $SERVER_PID
  exit 1
fi

kill -9 $SERVER_PID
echo "run-tests success!"
