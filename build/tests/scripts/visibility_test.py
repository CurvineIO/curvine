#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Visibility test for file size and content after pwrite.

Usage:
    python3 visibility_test.py --dir /curvine-fuse
"""

import argparse
import os
import sys

WRITE_SIZE = 1024
DEFAULT_DIR = "/curvine-fuse"
TEST_FILE = "visibility-test"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Visibility test for file size and content after pwrite",
    )
    parser.add_argument("--dir", default=DEFAULT_DIR, help="Directory for test files")
    args = parser.parse_args()

    path = os.path.join(os.path.abspath(args.dir), TEST_FILE)
    data = b"A" * WRITE_SIZE

    try:
        os.unlink(path)
    except FileNotFoundError:
        pass

    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        if os.pwrite(fd, data, 0) != WRITE_SIZE:
            print("ERROR: pwrite short write", file=sys.stderr)
            return 1

        size = os.fstat(fd).st_size
        print(f"size={size} (expect {WRITE_SIZE})")
        if size != WRITE_SIZE:
            print("ERROR: size mismatch", file=sys.stderr)
            return 1

        read_buf = os.pread(fd, WRITE_SIZE, 0)
        if read_buf != data:
            print("ERROR: read content mismatch", file=sys.stderr)
            return 1

        print("PASS")
        return 0
    finally:
        os.close(fd)


if __name__ == "__main__":
    sys.exit(main())
