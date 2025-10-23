#!/bin/bash
ROOT_DIR=$(cd ../../$(dirname $0); pwd)
echo "ROOT_DIR: ${ROOT_DIR}"

rm -rf build/dist/*

# Parse command line arguments
ZIP_FLAG=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --zip)
      ZIP_FLAG="--zip"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--zip]"
      exit 1
      ;;
  esac
done

docker run -it --rm --name curvine-compile \
  -u root --privileged=true \
  -v ${ROOT_DIR}:/workspace \
  -w /workspace \
  --network host \
  curvine/curvine-compile:latest "build/build.sh ${ZIP_FLAG} --package all"