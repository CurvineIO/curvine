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
setgid directory GID inheritance missing on Curvine FUSE — reproducer for LTP failures.

Root cause: when a directory has the S_ISGID bit set, new files and subdirectories
created inside it must inherit the parent directory's group ID (and directories
must also inherit S_ISGID).  Curvine assigns the creating process's effective GID
instead, so stat() reports the wrong st_gid.

Covered LTP cases (each line is one minimal repro scenario):
  creat08 — creat(2) in S_ISGID directory as nobody must yield parent dir gid (TFAIL block2)
  open10  — open(O_CREAT|O_EXCL|O_RDWR) in S_ISGID directory as nobody must yield parent gid
  mkdir02 — mkdir(2) in S_ISGID directory must inherit parent gid and S_ISGID (TFAIL)

Usage:
    python3 curvine_ltp_setgid_gid_repro.py --dir /curvine-fuse/fuse-test
    python3 curvine_ltp_setgid_gid_repro.py --dir /curvine-fuse/fuse-test --allow-non-mount
"""

import argparse
import ctypes
import errno
import grp
import os
import platform
import pwd
import shutil
import stat
import sys
from typing import Callable

DEFAULT_DIR = "/curvine-fuse/fuse-test"

AT_FDCWD = -100
O_CREAT = 0o100
O_EXCL = 0o200
O_RDWR = 2
O_WRONLY = 1
O_TRUNC = 0o1000

MODE_RWX = 0o777
MODE_SGID_DIR = stat.S_ISGID | MODE_RWX


def expect_ok(name: str, fn: Callable[[], None]) -> int:
    try:
        fn()
        print(f"  PASS: {name}")
        return 0
    except OSError as exc:
        print(
            f"  FAIL: {name} — unexpected OSError errno={exc.errno} "
            f"({errno.errorcode.get(exc.errno, '?')}): {exc}",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"  FAIL: {name} — unexpected error: {exc}", file=sys.stderr)
        return 1


def expect_errno(name: str, fn: Callable[[], None], expected: int) -> int:
    try:
        fn()
        print(
            f"  FAIL: {name} — expected errno {expected} "
            f"({errno.errorcode.get(expected, '?')}), but syscall succeeded",
            file=sys.stderr,
        )
        return 1
    except OSError as exc:
        if exc.errno == expected:
            print(
                f"  PASS: {name} (errno={expected} "
                f"{errno.errorcode.get(expected, '?')})"
            )
            return 0
        print(
            f"  FAIL: {name} — expected errno {expected}, got {exc.errno}: {exc}",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"  FAIL: {name} — expected OSError, got {exc}", file=sys.stderr)
        return 1


def skip(name: str, reason: str) -> int:
    print(f"  SKIP: {name} — {reason}")
    return 0


def case_dir(root: str, name: str) -> str:
    path = os.path.join(root, name)
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)
    return path


def _syscall_nr_open() -> int:
    table = {
        "x86_64": 2,
        "aarch64": 56,
    }
    machine = platform.machine()
    if machine not in table:
        raise OSError(
            errno.ENOSYS,
            f"unsupported architecture for open: {machine}",
        )
    return table[machine]


def _syscall_nr_creat() -> int:
    table = {
        "x86_64": 85,
        "aarch64": 56,
    }
    machine = platform.machine()
    if machine not in table:
        raise OSError(
            errno.ENOSYS,
            f"unsupported architecture for creat: {machine}",
        )
    return table[machine]


def sys_creat(path: str, mode: int) -> int:
    """creat(path, mode) via raw syscall (openat on aarch64)."""
    libc = ctypes.CDLL(None, use_errno=True)
    syscall_fn = libc.syscall
    syscall_fn.restype = ctypes.c_long

    path_buf = path.encode()
    if platform.machine() == "x86_64":
        ret = int(
            syscall_fn(
                _syscall_nr_creat(),
                ctypes.c_char_p(path_buf),
                mode,
            )
        )
    else:
        flags = O_CREAT | O_WRONLY | O_TRUNC
        ret = int(
            syscall_fn(
                _syscall_nr_creat(),
                AT_FDCWD,
                ctypes.c_char_p(path_buf),
                flags,
                mode,
            )
        )
    if ret < 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))
    return ret


def sys_open(path: str, flags: int, mode: int = 0) -> int:
    """open(path, flags, mode) via raw syscall (openat on aarch64)."""
    libc = ctypes.CDLL(None, use_errno=True)
    syscall_fn = libc.syscall
    syscall_fn.restype = ctypes.c_long

    path_buf = path.encode()
    if platform.machine() == "x86_64":
        ret = int(
            syscall_fn(
                _syscall_nr_open(),
                ctypes.c_char_p(path_buf),
                flags,
                mode,
            )
        )
    else:
        ret = int(
            syscall_fn(
                _syscall_nr_open(),
                AT_FDCWD,
                ctypes.c_char_p(path_buf),
                flags,
                mode,
            )
        )
    if ret < 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))
    return ret


def _get_free_gid(skip: int) -> int:
    for gid in range(1, 32768):
        if gid == skip:
            continue
        try:
            grp.getgrgid(gid)
        except KeyError:
            return gid
    raise OSError(errno.EDEADLK, "no unused group ID found")


def _raise_gid_mismatch(actual: int, expected: int, label: str) -> None:
    raise OSError(
        errno.EINVAL,
        f"{label}: gid got {actual}, expected {expected}",
    )


def _make_setgid_parent(
    workdir: str,
    owner_uid: int,
    owner_gid: int,
) -> tuple[str, int]:
    free_gid = _get_free_gid(owner_gid)
    parent = os.path.join(workdir, "setgid_parent")
    os.mkdir(parent, MODE_RWX)
    os.chown(parent, owner_uid, free_gid)
    os.chmod(parent, MODE_SGID_DIR)
    parent_stat = os.stat(parent)
    if not (parent_stat.st_mode & stat.S_ISGID):
        raise OSError(errno.EINVAL, "S_ISGID not set on parent directory")
    if parent_stat.st_gid != free_gid:
        _raise_gid_mismatch(
            parent_stat.st_gid,
            free_gid,
            "parent directory after chown/chmod",
        )
    return parent, free_gid


def _drop_to_nobody(nobody_uid: int, nobody_gid: int) -> None:
    ruid, _, suid = os.getresuid()
    rgid, _, sgid = os.getresgid()
    os.setresgid(rgid, nobody_gid, sgid)
    os.setresuid(ruid, nobody_uid, suid)


def _restore_effective_ids(orig_euid: int, orig_egid: int) -> None:
    ruid, _, suid = os.getresuid()
    rgid, _, sgid = os.getresgid()
    os.setresuid(ruid, orig_euid, suid)
    os.setresgid(rgid, orig_egid, sgid)


def test_creat_in_setgid_dir_inherits_parent_gid(root: str) -> int:
    # Related LTP cases: creat08
    # creat(2) in an S_ISGID directory must assign the parent directory gid, not the process gid.
    if os.geteuid() != 0:
        return skip(
            "creat in setgid dir inherits parent gid (creat08)",
            "requires root",
        )

    workdir = case_dir(root, "creat_setgid_gid")
    nobody = pwd.getpwnam("nobody")
    parent, expected_gid = _make_setgid_parent(
        workdir,
        nobody.pw_uid,
        nobody.pw_gid,
    )
    target = os.path.join(parent, "nosetgid")
    orig_euid = os.geteuid()
    orig_egid = os.getegid()

    def run_creat_inherit_gid() -> None:
        _drop_to_nobody(nobody.pw_uid, nobody.pw_gid)
        fd = sys_creat(target, MODE_RWX)
        os.close(fd)
        child_stat = os.stat(target)
        if child_stat.st_gid != expected_gid:
            _raise_gid_mismatch(
                child_stat.st_gid,
                expected_gid,
                "creat in S_ISGID directory",
            )

    try:
        return expect_ok(
            "creat(2) in S_ISGID dir yields parent gid (creat08 block2)",
            run_creat_inherit_gid,
        )
    finally:
        _restore_effective_ids(orig_euid, orig_egid)


def test_open_creat_in_setgid_dir_inherits_parent_gid(root: str) -> int:
    # Related LTP cases: open10
    # open(O_CREAT) in an S_ISGID directory must assign the parent directory gid, not the process gid.
    if os.geteuid() != 0:
        return skip(
            "open O_CREAT in setgid dir inherits parent gid (open10)",
            "requires root",
        )

    workdir = case_dir(root, "open_setgid_gid")
    nobody = pwd.getpwnam("nobody")
    parent, expected_gid = _make_setgid_parent(
        workdir,
        nobody.pw_uid,
        nobody.pw_gid,
    )
    target = os.path.join(parent, "nosetgid")
    orig_euid = os.geteuid()
    orig_egid = os.getegid()
    open_flags = O_CREAT | O_EXCL | O_RDWR

    def run_open_creat_inherit_gid() -> None:
        _drop_to_nobody(nobody.pw_uid, nobody.pw_gid)
        fd = sys_open(target, open_flags, MODE_RWX)
        os.close(fd)
        child_stat = os.stat(target)
        if child_stat.st_gid != expected_gid:
            _raise_gid_mismatch(
                child_stat.st_gid,
                expected_gid,
                "open(O_CREAT) in S_ISGID directory",
            )

    try:
        return expect_ok(
            "open(O_CREAT|O_EXCL|O_RDWR) in S_ISGID dir yields parent gid (open10 block2)",
            run_open_creat_inherit_gid,
        )
    finally:
        _restore_effective_ids(orig_euid, orig_egid)


def test_mkdir_in_setgid_dir_inherits_parent_gid_and_sgid(root: str) -> int:
    # Related LTP cases: mkdir02
    # mkdir(2) in an S_ISGID directory must inherit parent gid and the S_ISGID bit.
    if os.geteuid() != 0:
        return skip(
            "mkdir in setgid dir inherits gid and S_ISGID (mkdir02)",
            "requires root",
        )

    workdir = case_dir(root, "mkdir_setgid_gid")
    nobody = pwd.getpwnam("nobody")
    parent, expected_gid = _make_setgid_parent(
        workdir,
        os.getuid(),
        nobody.pw_gid,
    )
    child = os.path.join(parent, "testdir2")
    orig_euid = os.geteuid()
    orig_egid = os.getegid()

    def run_mkdir_inherit_gid() -> None:
        _drop_to_nobody(nobody.pw_uid, nobody.pw_gid)
        os.mkdir(child, MODE_RWX)
        child_stat = os.stat(child)
        if child_stat.st_gid != expected_gid:
            _raise_gid_mismatch(
                child_stat.st_gid,
                expected_gid,
                "mkdir in S_ISGID directory",
            )
        if not (child_stat.st_mode & stat.S_ISGID):
            raise OSError(
                errno.EINVAL,
                "mkdir in S_ISGID directory: S_ISGID not inherited",
            )

    try:
        return expect_ok(
            "mkdir(2) in S_ISGID dir inherits parent gid and S_ISGID (mkdir02)",
            run_mkdir_inherit_gid,
        )
    finally:
        _restore_effective_ids(orig_euid, orig_egid)


TESTS: list[tuple[str, str, Callable[..., int]]] = [
    (
        "1",
        "creat(2) in S_ISGID dir inherits parent gid (creat08)",
        test_creat_in_setgid_dir_inherits_parent_gid,
    ),
    (
        "2",
        "open(O_CREAT) in S_ISGID dir inherits parent gid (open10)",
        test_open_creat_in_setgid_dir_inherits_parent_gid,
    ),
    (
        "3",
        "mkdir(2) in S_ISGID dir inherits parent gid and S_ISGID (mkdir02)",
        test_mkdir_in_setgid_dir_inherits_parent_gid_and_sgid,
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="setgid directory GID inheritance missing on Curvine FUSE — reproducer",
    )
    parser.add_argument(
        "--dir",
        default=DEFAULT_DIR,
        help="Base test directory (must be on the Curvine FUSE mount)",
    )
    parser.add_argument(
        "--allow-non-mount",
        action="store_true",
        help="Skip FUSE mount check; for local debugging only (default: False)",
    )
    args = parser.parse_args()

    if not sys.platform.startswith("linux"):
        print("SKIP: Linux-only syscalls", file=sys.stderr)
        return 0

    if os.geteuid() != 0:
        print("WARNING: not running as root", file=sys.stderr)

    root = os.path.abspath(args.dir)
    os.makedirs(root, exist_ok=True)

    if args.allow_non_mount:
        print(
            "WARNING: --allow-non-mount set; results may not reflect Curvine FUSE behaviour",
            file=sys.stderr,
        )

    print(f"Running setgid-gid reproducer under {root}\n")

    fail = 0
    for num, title, test_fn in TESTS:
        print(f"[case {num}] {title}:")
        fail += test_fn(root)
        print()

    total = len(TESTS)
    passed = total - fail
    print(f"Summary: {passed}/{total} passed, {fail}/{total} failed")
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
