#!/usr/bin/env python3
#  // Copyright 2025 OPPO.
#  //
#  // Licensed under the Apache License, Version 2.0 (the "License");
#  // you may not use this file except in compliance with the License.
#  // You may obtain a copy of the License at
#  //
#  //     http://www.apache.org/licenses/LICENSE-2.0
#  //
#  // Unless required by applicable law or agreed to in writing, software
#  // distributed under the License is distributed on an "AS IS" BASIS,
#  // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  // See the License for the specific language governing permissions and
#  // limitations under the License.

"""
POSIX rename(2) regression tests for Curvine FUSE mounts.

Covers the same cases as curvine-server/tests/master_fs_test.rs::rename.

Usage:
    python3 rename_test.py --dir /curvine-fuse/fuse-test
"""

from __future__ import annotations

import argparse
import errno
import os
import shutil
import sys
from typing import Callable

DEFAULT_DIR = "/curvine-fuse/fuse-test"


def case_dir(root: str, name: str) -> str:
    path = os.path.join(root, name)
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)
    return path


def touch(path: str, content: str = "x") -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def expect_ok(name: str, fn: Callable[[], None]) -> int:
    try:
        fn()
        print(f"  PASS: {name}")
        return 0
    except Exception as exc:  # noqa: BLE001 — test harness
        print(f"  FAIL: {name} — unexpected error: {exc}", file=sys.stderr)
        return 1


def expect_errno(name: str, fn: Callable[[], None], expected: int) -> int:
    try:
        fn()
        print(
            f"  FAIL: {name} — expected errno {expected} ({errno.errorcode.get(expected, '?')}), "
            "but rename succeeded",
            file=sys.stderr,
        )
        return 1
    except OSError as exc:
        if exc.errno == expected:
            print(f"  PASS: {name} (errno={expected} {errno.errorcode.get(expected, '?')})")
            return 0
        print(
            f"  FAIL: {name} — expected errno {expected}, got {exc.errno}: {exc}",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"  FAIL: {name} — expected OSError, got {exc}", file=sys.stderr)
        return 1


def test_basic_file_rename(root: str) -> int:
    d = case_dir(root, "basic_file")
    src, dst = os.path.join(d, "a.txt"), os.path.join(d, "b.txt")
    touch(src)

    def run() -> None:
        os.rename(src, dst)
        if os.path.exists(src) or not os.path.isfile(dst):
            raise OSError("rename did not move file")

    return expect_ok("basic file rename", run)


def test_basic_dir_rename(root: str) -> int:
    d = case_dir(root, "basic_dir")
    src, dst = os.path.join(d, "src"), os.path.join(d, "dst")
    os.makedirs(src)
    touch(os.path.join(src, "child.txt"))

    def run() -> None:
        os.rename(src, dst)
        if os.path.exists(src) or not os.path.isdir(dst):
            raise OSError("rename did not move directory")
        if not os.path.isfile(os.path.join(dst, "child.txt")):
            raise OSError("child missing after directory rename")

    return expect_ok("basic directory rename (children preserved)", run)


def test_file_to_existing_dir(root: str) -> int:
    d = case_dir(root, "file_to_dir")
    file_path = os.path.join(d, "1.log")
    dir_path = os.path.join(d, "b")
    touch(file_path)
    os.makedirs(dir_path)

    def run() -> None:
        os.rename(file_path, dir_path)

    rc = expect_errno("file -> existing directory", run, errno.EISDIR)
    if rc:
        return rc
    if not os.path.isfile(file_path) or not os.path.isdir(dir_path):
        print("  FAIL: file -> dir — source or destination state corrupted", file=sys.stderr)
        return 1
    if os.path.exists(os.path.join(dir_path, "1.log")):
        print("  FAIL: file -> dir — file was moved into directory", file=sys.stderr)
        return 1
    return 0


def test_file_overwrite_file(root: str) -> int:
    d = case_dir(root, "file_over_file")
    old_path = os.path.join(d, "old.log")
    new_path = os.path.join(d, "new.log")
    touch(old_path, "old")
    touch(new_path, "new")

    def run() -> None:
        os.rename(old_path, new_path)
        if os.path.exists(old_path) or not os.path.isfile(new_path):
            raise OSError("file overwrite rename failed")

    return expect_ok("file -> existing file (overwrite)", run)


def test_dir_overwrite_empty_dir(root: str) -> int:
    d = case_dir(root, "dir_over_empty")
    src_dir = os.path.join(d, "src_dir")
    dst_dir = os.path.join(d, "empty_dir")
    os.makedirs(src_dir)
    touch(os.path.join(src_dir, "child.txt"))
    os.makedirs(dst_dir)

    def run() -> None:
        os.rename(src_dir, dst_dir)
        if os.path.exists(src_dir) or not os.path.isdir(dst_dir):
            raise OSError("directory overwrite failed")
        if not os.path.isfile(os.path.join(dst_dir, "child.txt")):
            raise OSError("child missing after empty-dir overwrite")

    return expect_ok("directory -> empty directory (overwrite)", run)


def test_dir_to_nonempty_dir(root: str) -> int:
    d = case_dir(root, "dir_to_nonempty")
    src_dir = os.path.join(d, "rename_src")
    dst_dir = os.path.join(d, "rename_dst")
    os.makedirs(src_dir)
    touch(os.path.join(src_dir, "file.txt"))
    os.makedirs(dst_dir)
    touch(os.path.join(dst_dir, "keep.txt"))

    def run() -> None:
        os.rename(src_dir, dst_dir)

    rc = expect_errno("directory -> non-empty directory", run, errno.ENOTEMPTY)
    if rc:
        return rc
    if not os.path.isdir(src_dir) or not os.path.isfile(os.path.join(dst_dir, "keep.txt")):
        print("  FAIL: dir -> non-empty dir — tree state corrupted", file=sys.stderr)
        return 1
    return 0


def test_dir_to_file(root: str) -> int:
    d = case_dir(root, "dir_to_file")
    dir_path = os.path.join(d, "dir_src")
    file_path = os.path.join(d, "file_dst")
    os.makedirs(dir_path)
    touch(file_path)

    def run() -> None:
        os.rename(dir_path, file_path)

    rc = expect_errno("directory -> existing file", run, errno.ENOTDIR)
    if rc:
        return rc
    if not os.path.isdir(dir_path) or not os.path.isfile(file_path):
        print("  FAIL: dir -> file — source or destination state corrupted", file=sys.stderr)
        return 1
    return 0


def test_rename_into_subdir(root: str) -> int:
    d = case_dir(root, "into_subdir")
    parent = os.path.join(d, "rename_parent")
    child = os.path.join(parent, "child")
    os.makedirs(parent)

    def run() -> None:
        os.rename(parent, child)

    rc = expect_errno("directory -> path under itself", run, errno.EINVAL)
    if rc:
        return rc
    if not os.path.isdir(parent) or os.path.exists(child):
        print("  FAIL: rename into subdir — tree state corrupted", file=sys.stderr)
        return 1
    return 0


def test_same_path_noop(root: str) -> int:
    d = case_dir(root, "same_path")
    path = os.path.join(d, "same.txt")
    touch(path, "data")

    def run() -> None:
        os.rename(path, path)
        if not os.path.isfile(path):
            raise OSError("same-path rename removed file")

    return expect_ok("rename src to itself (no-op)", run)


def test_same_path_noop_empty_dir(root: str) -> int:
    d = case_dir(root, "same_path_empty_dir")
    path = os.path.join(d, "empty_dir")
    os.makedirs(path)

    def run() -> None:
        os.rename(path, path)
        if not os.path.isdir(path):
            raise OSError("same-path rename removed empty directory")

    return expect_ok("rename empty directory to itself (no-op)", run)


def test_same_path_noop_nonempty_dir(root: str) -> int:
    d = case_dir(root, "same_path_full_dir")
    path = os.path.join(d, "full_dir")
    os.makedirs(path)
    touch(os.path.join(path, "child.txt"))

    def run() -> None:
        os.rename(path, path)
        if not os.path.isdir(path):
            raise OSError("same-path rename removed non-empty directory")
        if not os.path.isfile(os.path.join(path, "child.txt")):
            raise OSError("same-path rename lost directory children")

    return expect_ok("rename non-empty directory to itself (no-op)", run)


def test_missing_source(root: str) -> int:
    d = case_dir(root, "missing_src")
    src = os.path.join(d, "missing.txt")
    dst = os.path.join(d, "dst.txt")

    def run() -> None:
        os.rename(src, dst)

    return expect_errno("rename missing source", run, errno.ENOENT)


TESTS: list[tuple[str, Callable[[str], int]]] = [
    ("1", test_basic_file_rename),
    ("2", test_basic_dir_rename),
    ("3", test_file_to_existing_dir),
    ("4", test_file_overwrite_file),
    ("5", test_dir_overwrite_empty_dir),
    ("6", test_dir_to_nonempty_dir),
    ("7", test_dir_to_file),
    ("8", test_rename_into_subdir),
    ("9", test_same_path_noop),
    ("10", test_same_path_noop_empty_dir),
    ("11", test_same_path_noop_nonempty_dir),
    ("12", test_missing_source),
]


def main() -> int:
    parser = argparse.ArgumentParser(description="POSIX rename regression tests")
    parser.add_argument("--dir", default=DEFAULT_DIR, help="Base test directory")
    args = parser.parse_args()

    root = os.path.abspath(args.dir)
    os.makedirs(root, exist_ok=True)

    print(f"Running rename tests under {root}\n")

    fail = 0
    for num, test_fn in TESTS:
        print(f"[case {num}] {test_fn.__name__}:")
        fail += test_fn(root)
        print()

    total = len(TESTS)
    passed = total - fail
    print(f"Summary: {passed}/{total} passed, {fail}/{total} failed")
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
