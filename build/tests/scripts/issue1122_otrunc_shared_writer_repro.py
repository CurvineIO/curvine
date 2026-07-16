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
Regression test for issue #1122.

curvine-fuse advertised FUSE_ATOMIC_O_TRUNC in init() (via a blanket
`out_flags |= op.arg.flags`) even though `open` never truncates. With that
capability advertised, the kernel skips the follow-up SETATTR(size=0). When a
writer for the inode already exists, `get_or_create_writer` returns the shared
writer and discards the second open's flags, so O_TRUNC is silently lost.

The failing sequence (multi-fd) is:
  1. create an existing file of N (>0) bytes
  2. open(path, O_WRONLY)                    -> establishes + holds a writer
  3. open(path, O_WRONLY|O_TRUNC) on same ino -> shared writer ignores O_TRUNC
  4. the file is NOT truncated (size stays N)

After the fix (stop advertising FUSE_ATOMIC_O_TRUNC), the kernel falls back to
the open + SETATTR(size=0) path handled by set_attr -> fs_resize, and the file
is correctly truncated to 0.

A single-fd O_TRUNC case is included as a control: it passes both before and
after the fix, because the writer is freshly created with the O_TRUNC flag.
"""

import argparse
import errno
import os
import sys

DEFAULT_DIR = "/curvine-fuse/fuse-test"
FILE_SIZE = 100


def safe_close(fd: int) -> None:
    try:
        os.close(fd)
    except OSError:
        pass


def is_mount_path(path: str) -> bool:
    probe = os.path.abspath(path)
    while probe != "/":
        if os.path.ismount(probe):
            return True
        probe = os.path.dirname(probe)
    return False


def make_file(path: str, n: int) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.write(fd, b"X" * n)
    finally:
        safe_close(fd)


def file_size(path: str) -> int:
    return os.stat(path).st_size


def case_single_fd_trunc(root: str) -> None:
    """Control: single-fd O_TRUNC on an existing file must truncate to 0."""
    target = os.path.join(root, "issue1122_otrunc_single")
    make_file(target, FILE_SIZE)
    fd = os.open(target, os.O_WRONLY | os.O_TRUNC)
    safe_close(fd)
    after = file_size(target)
    if after != 0:
        raise OSError(
            errno.EIO,
            f"single-fd O_TRUNC did not truncate: size={after} (expected 0)",
        )


def case_multi_fd_trunc(root: str) -> None:
    """Bug scenario: O_TRUNC on an ino that already has an open writer."""
    target = os.path.join(root, "issue1122_otrunc_multi")
    make_file(target, FILE_SIZE)
    fd1 = os.open(target, os.O_WRONLY)  # establish + hold a writer for the ino
    try:
        fd2 = os.open(target, os.O_WRONLY | os.O_TRUNC)  # 2nd open w/ O_TRUNC
        try:
            size_via_fd2 = os.fstat(fd2).st_size
        finally:
            safe_close(fd2)
    finally:
        safe_close(fd1)
    after = file_size(target)
    if after != 0 or size_via_fd2 != 0:
        raise OSError(
            errno.EIO,
            f"multi-fd O_TRUNC did not truncate: size={after} "
            f"fstat(fd2)={size_via_fd2} (expected 0)",
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="issue #1122 shared-writer O_TRUNC regression test",
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
        case_single_fd_trunc(root)
        case_multi_fd_trunc(root)
    except OSError as exc:
        print(
            f"FAIL: issue #1122 shared-writer O_TRUNC regression: "
            f"errno={exc.errno} ({errno.errorcode.get(exc.errno, '?')}): {exc}",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL: issue #1122 shared-writer O_TRUNC regression: {exc}", file=sys.stderr)
        return 1

    print("PASS: issue #1122 shared-writer O_TRUNC regression")
    return 0


if __name__ == "__main__":
    sys.exit(main())
