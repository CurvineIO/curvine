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
Regression test for open(path, O_CREAT | O_RDONLY, mode).

O_CREAT is independent from the access mode: opening a missing file with
O_CREAT|O_RDONLY must create the file and return a read-only fd. Curvine used
to route FUSE CREATE through the read-only reader path before creating the
inode, so the operation failed with ENOENT.
"""

import argparse
import errno
import os
import shutil
import sys

DEFAULT_DIR = "/curvine-fuse/fuse-test"


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
    workdir = case_dir(root, "open_creat_rdonly")
    target = os.path.join(workdir, "created-readonly")

    fd = os.open(target, os.O_CREAT | os.O_RDONLY, 0o644)
    try:
        st = os.fstat(fd)
        if not os.path.isfile(target):
            raise OSError(errno.ENOENT, "created file is not visible")
        if (st.st_mode & 0o777) != 0o644:
            raise OSError(errno.EIO, f"created mode is {st.st_mode & 0o777:o}, expected 644")
        try:
            os.write(fd, b"x")
        except OSError as exc:
            if exc.errno != errno.EBADF:
                raise OSError(exc.errno, f"write on read-only fd returned {exc}") from exc
        else:
            raise OSError(errno.EIO, "write on read-only fd unexpectedly succeeded")
    finally:
        safe_close(fd)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="open(O_CREAT|O_RDONLY) regression test",
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
            f"FAIL: open(O_CREAT|O_RDONLY) regression: "
            f"errno={exc.errno} ({errno.errorcode.get(exc.errno, '?')}): {exc}",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL: open(O_CREAT|O_RDONLY) regression: {exc}", file=sys.stderr)
        return 1

    print("PASS: open(O_CREAT|O_RDONLY) regression")
    return 0


if __name__ == "__main__":
    sys.exit(main())
