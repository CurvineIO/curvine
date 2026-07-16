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
Regression test for lookup of a name longer than the FUSE name limit.

Curvine FUSE advertises a 255-byte name limit. Lookup operations on a single
path component longer than that must fail with ENAMETOOLONG instead of falling
through to a backend lookup and returning ENOENT.
"""

import argparse
import errno
import os
import shutil
import sys
from typing import Callable

DEFAULT_DIR = "/curvine-fuse/fuse-test"
LONG_NAME = "a" * 256


def case_dir(root: str, name: str) -> str:
    path = os.path.join(root, name)
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)
    return path


def is_mount_path(path: str) -> bool:
    probe = os.path.abspath(path)
    while probe != "/":
        if os.path.ismount(probe):
            return True
        probe = os.path.dirname(probe)
    return False


def expect_enametoolong(name: str, fn: Callable[[], None]) -> int:
    try:
        fn()
    except OSError as exc:
        if exc.errno == errno.ENAMETOOLONG:
            print(f"  PASS: {name}")
            return 0
        print(
            f"  FAIL: {name}: expected ENAMETOOLONG, got errno={exc.errno} "
            f"({errno.errorcode.get(exc.errno, '?')}): {exc}",
            file=sys.stderr,
        )
        return 1

    print(f"  FAIL: {name}: operation unexpectedly succeeded", file=sys.stderr)
    return 1


def test_chdir_long_name(workdir: str) -> int:
    def run() -> None:
        old_cwd = os.getcwd()
        try:
            os.chdir(workdir)
            os.chdir(LONG_NAME)
        finally:
            os.chdir(old_cwd)

    return expect_enametoolong("chdir on 256-byte name returns ENAMETOOLONG", run)


def test_open_long_name(workdir: str) -> int:
    def run() -> None:
        os.open(os.path.join(workdir, LONG_NAME), os.O_RDWR)

    return expect_enametoolong("open on 256-byte name returns ENAMETOOLONG", run)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="lookup ENAMETOOLONG regression test",
    )
    parser.add_argument(
        "--dir",
        default=DEFAULT_DIR,
        help="Base test directory on the Curvine FUSE mount",
    )
    parser.add_argument(
        "--allow-non-mount",
        action="store_true",
        help="Skip mount check; for local debugging only",
    )
    args = parser.parse_args()

    root = os.path.abspath(args.dir)
    os.makedirs(root, exist_ok=True)

    if not args.allow_non_mount and not is_mount_path(root):
        print(f"FAIL: {root} is not under a mount point", file=sys.stderr)
        return 1

    workdir = case_dir(root, "lookup_enametoolong")
    failures = test_chdir_long_name(workdir)
    failures += test_open_long_name(workdir)

    if failures:
        print(f"FAIL: lookup ENAMETOOLONG regression ({failures} failures)")
        return 1

    print("PASS: lookup ENAMETOOLONG regression")
    return 0


if __name__ == "__main__":
    sys.exit(main())
