#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Visibility tests for pwrite/pread behavior on FUSE mounts.

Includes:
1) basic file size/content visibility after pwrite
2) pread visibility strategies after extending pwrite (ported from repro_pread_then_pwrite.py)

Usage:
    python3 visibility_test.py --dir /curvine-fuse --mode all
"""

from __future__ import annotations

import argparse
import os
import sys

WRITE_SIZE = 1024
DEFAULT_DIR = "/curvine-fuse"
BASIC_TEST_FILE = "visibility-test"

PP_TEST_FILE = "pp.bin"
# Values from issue #852 repro (repro_pc_boundary.c / repro_pread_then_pwrite.c):
# first write establishes reader at BUF1_LEN; extending pwrite starts at OFF2
# with a sparse hole [BUF1_LEN, OFF2).
BUF1_LEN = 1167
OFF2 = 1425
BUF2_LEN = 3882
EXPECT = OFF2 + BUF2_LEN  # 5307

_EXPECTED_PREAD = (
    b"A" * BUF1_LEN
    + b"\x00" * (OFF2 - BUF1_LEN)
    + b"B" * BUF2_LEN
)


def run_basic_visibility(base_dir: str) -> int:
    path = os.path.join(base_dir, BASIC_TEST_FILE)
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
        print(f"[basic] size={size} (expect {WRITE_SIZE})")
        if size != WRITE_SIZE:
            print("ERROR: size mismatch", file=sys.stderr)
            return 1

        read_buf = os.pread(fd, WRITE_SIZE, 0)
        if read_buf != data:
            print("ERROR: read content mismatch", file=sys.stderr)
            return 1

        print("[basic] PASS")
        return 0
    finally:
        os.close(fd)


def run_pread_strategy(
        name: str,
        file_path: str,
        *,
        do_fsync: bool,
        do_lseek: bool,
        do_reopen: bool,
        do_fstat: bool,
) -> int:
    try:
        os.unlink(file_path)
    except FileNotFoundError:
        pass

    b1 = b"A" * BUF1_LEN
    b2 = b"B" * BUF2_LEN

    fd = os.open(file_path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        os.pwrite(fd, b1, 0)
        os.pread(fd, BUF1_LEN, 0)  # establish page cache
        os.pwrite(fd, b2, OFF2)  # extending pwrite

        if do_fsync:
            os.fsync(fd)
        if do_lseek:
            os.lseek(fd, 0, os.SEEK_END)
        if do_fstat:
            os.fstat(fd)
        if do_reopen:
            os.close(fd)
            fd = -1
            fd = os.open(file_path, os.O_RDWR)

        data = os.pread(fd, EXPECT, 0)
        ok = len(data) == EXPECT and data == _EXPECTED_PREAD
        print(
            f"[pread] {name}: pread={len(data)} (expect {EXPECT}) "
            f"{'OK' if ok else 'FAIL'}"
        )
        if not ok and len(data) == EXPECT:
            print("[pread] content mismatch (stale or wrong bytes)", file=sys.stderr)
        return 0 if ok else 1
    finally:
        if fd >= 0:
            os.close(fd)
        try:
            os.unlink(file_path)
        except FileNotFoundError:
            pass


def run_pread_visibility(base_dir: str) -> int:
    file_path = os.path.join(base_dir, PP_TEST_FILE)
    cases = [
        (
            "A. baseline (no flush)",
            dict(do_fsync=False, do_lseek=False, do_reopen=False, do_fstat=False),
        ),
        (
            "B. fsync between",
            dict(do_fsync=True, do_lseek=False, do_reopen=False, do_fstat=False),
        ),
        (
            "C. lseek SEEK_END between",
            dict(do_fsync=False, do_lseek=True, do_reopen=False, do_fstat=False),
        ),
        (
            "D. close+reopen between",
            dict(do_fsync=False, do_lseek=False, do_reopen=True, do_fstat=False),
        ),
        (
            "E. fstat between",
            dict(do_fsync=False, do_lseek=False, do_reopen=False, do_fstat=True),
        ),
        (
            "F. fsync + lseek + fstat",
            dict(do_fsync=True, do_lseek=True, do_reopen=False, do_fstat=True),
        ),
    ]

    fail = 0
    for name, kwargs in cases:
        fail += run_pread_strategy(name, file_path, **kwargs)
    passed = len(cases) - fail
    if fail == 0:
        print(f"[pread] {passed}/{len(cases)} PASS")
    else:
        print(f"[pread] {fail}/{len(cases)} FAIL")
    return 1 if fail else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Visibility tests for pwrite/pread on FUSE mounts",
    )
    parser.add_argument("--dir", default=DEFAULT_DIR, help="Directory for test files")
    parser.add_argument(
        "--mode",
        choices=["basic", "pread", "all"],
        default="all",
        help="Select which visibility suite to run",
    )
    args = parser.parse_args()

    base_dir = os.path.abspath(args.dir)
    os.makedirs(base_dir, exist_ok=True)

    fail = 0
    if args.mode in ("basic", "all"):
        fail += run_basic_visibility(base_dir)
    if args.mode in ("pread", "all"):
        fail += run_pread_visibility(base_dir)

    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
