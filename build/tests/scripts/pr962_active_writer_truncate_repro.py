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
Regression test for PR #962.

The failing sequence is:
  1. open(path, O_RDWR|O_CREAT|O_TRUNC)
  2. sparse pwrite through that fd, leaving an active FuseWriter
  3. truncate(path, len) through the path API, which reaches FUSE with fh == 0
  4. read from the same open fd

Before PR #962, and again after the dcache refactor dropped the fallback,
step 3 bypassed the active writer and resized the backend directly. The open
fd could then observe stale metadata and read(2) returned EIO.
"""

import argparse
import errno
import os
import shutil
import sys

DEFAULT_DIR = "/curvine-fuse/fuse-test"
SPARSE_OFFSET = 128 * 1024
MARKER = b"curvine-pr962-active-writer\n"


def safe_close(fd: int) -> None:
    try:
        os.close(fd)
    except OSError:
        pass


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


def run_case(root: str) -> None:
    workdir = case_dir(root, "pr962_active_writer_truncate")
    target = os.path.join(workdir, "sparse-file")
    fd = os.open(target, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        written = os.pwrite(fd, MARKER, SPARSE_OFFSET)
        if written != len(MARKER):
            raise OSError(errno.EIO, f"short pwrite: {written}/{len(MARKER)}")

        expected_len = SPARSE_OFFSET + len(MARKER)
        os.truncate(target, expected_len)

        if os.fstat(fd).st_size != expected_len:
            raise OSError(errno.EIO, "same fd saw stale size after path truncate")

        os.lseek(fd, SPARSE_OFFSET, os.SEEK_SET)
        got = os.read(fd, len(MARKER))
        if got != MARKER:
            raise OSError(errno.EIO, f"same fd read mismatch: {got!r}")

        os.lseek(fd, 0, os.SEEK_SET)
        hole = os.read(fd, 4096)
        if hole != b"\0" * 4096:
            raise OSError(errno.EIO, "sparse hole did not read as zeros")
    finally:
        safe_close(fd)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="PR #962 active-writer truncate regression test",
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

    try:
        run_case(root)
    except OSError as exc:
        print(
            f"FAIL: PR #962 active-writer truncate regression: "
            f"errno={exc.errno} ({errno.errorcode.get(exc.errno, '?')}): {exc}",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL: PR #962 active-writer truncate regression: {exc}", file=sys.stderr)
        return 1

    print("PASS: PR #962 active-writer truncate regression")
    return 0


if __name__ == "__main__":
    sys.exit(main())
