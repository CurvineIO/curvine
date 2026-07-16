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
Regression test for symlink owner/group metadata.

Symlinks created through FUSE must store the caller uid/gid. Otherwise lstat()
falls back to the configured FUSE uid/gid instead of reporting the creator.
"""

import argparse
import errno
import os
import shutil
import sys

DEFAULT_DIR = "/curvine-fuse/fuse-test"


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
    workdir = case_dir(root, "symlink_owner")
    target = os.path.join(workdir, "target")
    link = os.path.join(workdir, "link")

    fd = os.open(target, os.O_CREAT | os.O_TRUNC | os.O_RDWR, 0o644)
    os.close(fd)
    os.symlink("target", link)

    st = os.lstat(link)
    if st.st_uid != os.geteuid() or st.st_gid != os.getegid():
        raise OSError(
            errno.EIO,
            f"symlink uid:gid is {st.st_uid}:{st.st_gid}, "
            f"expected {os.geteuid()}:{os.getegid()}",
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="symlink owner/group regression test",
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
            f"FAIL: symlink owner/group: errno={exc.errno} "
            f"({errno.errorcode.get(exc.errno, '?')}): {exc}",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL: symlink owner/group: {exc}", file=sys.stderr)
        return 1

    print("PASS: symlink owner/group")
    return 0


if __name__ == "__main__":
    sys.exit(main())
