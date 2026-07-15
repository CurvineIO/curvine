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
Regression test for file group inheritance in setgid directories.

When a regular file is created inside a directory with the setgid bit set, the
new file must inherit the parent directory's group instead of the process group.
"""

import argparse
import errno
import grp
import os
import pwd
import shutil
import stat
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
    parent_gid = grp.getgrnam("bin").gr_gid
    nobody = pwd.getpwnam("nobody")

    workdir = case_dir(root, "setgid_group_inherit")
    parent = os.path.join(workdir, "parent")
    child = os.path.join(parent, "child")

    os.mkdir(parent, 0o777)
    os.chown(parent, 0, parent_gid)
    os.chmod(parent, 0o2777)

    parent_stat = os.stat(parent)
    if parent_stat.st_gid != parent_gid or not (parent_stat.st_mode & stat.S_ISGID):
        raise OSError(
            errno.EIO,
            f"bad parent metadata gid={parent_stat.st_gid}, mode={stat.S_IMODE(parent_stat.st_mode):o}",
        )

    old_euid = os.geteuid()
    old_egid = os.getegid()
    fd = -1
    try:
        os.setegid(nobody.pw_gid)
        os.seteuid(nobody.pw_uid)
        fd = os.open(child, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o777)
    finally:
        os.seteuid(old_euid)
        os.setegid(old_egid)
        if fd >= 0:
            os.close(fd)

    child_stat = os.stat(child)
    if child_stat.st_gid != parent_gid:
        raise OSError(
            errno.EIO,
            f"child gid is {child_stat.st_gid}, expected parent gid {parent_gid}",
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="setgid directory group inheritance regression test",
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

    if os.geteuid() != 0:
        print("SKIP: setgid group inheritance test requires root")
        return 0

    try:
        grp.getgrnam("bin")
        pwd.getpwnam("nobody")
    except KeyError as exc:
        print(f"SKIP: required account is missing: {exc}")
        return 0

    root = os.path.abspath(args.dir)
    os.makedirs(root, exist_ok=True)

    if not args.allow_non_mount and not is_mount_path(root):
        print(f"FAIL: {root} is not under a mount point", file=sys.stderr)
        return 1

    try:
        run_case(root)
    except OSError as exc:
        print(
            f"FAIL: setgid group inheritance: errno={exc.errno} "
            f"({errno.errorcode.get(exc.errno, '?')}): {exc}",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL: setgid group inheritance: {exc}", file=sys.stderr)
        return 1

    print("PASS: setgid group inheritance")
    return 0


if __name__ == "__main__":
    sys.exit(main())
