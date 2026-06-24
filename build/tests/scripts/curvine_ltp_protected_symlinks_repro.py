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
fs.protected_symlinks policy not enforced on Curvine FUSE — reproducer for LTP failures.

Root cause: in sticky, world-writable directories the kernel blocks the directory
owner from following symlinks they do not own (fs.protected_symlinks=1).  Curvine
does not apply this check: the dir owner can open others' symlinks when it should
get EACCES, and incorrectly blocks the dir owner from opening their own symlinks.

Covered LTP cases (each line is one minimal repro scenario):
  prot_hsymlinks — root-owned sticky tmp_root: root must not open hsym's symlink (TFAIL)
  prot_hsymlinks — hsym-owned sticky tmp_hsym: hsym must open own symlink (TFAIL)

Usage:
    python3 curvine_ltp_protected_symlinks_repro.py --dir /curvine-fuse/fuse-test
    python3 curvine_ltp_protected_symlinks_repro.py --dir /curvine-fuse/fuse-test --allow-non-mount
"""

import argparse
import ctypes
import errno
import os
import platform
import pwd
import shutil
import stat
import sys
from typing import Callable

DEFAULT_DIR = "/curvine-fuse/fuse-test"

AT_FDCWD = -100
O_RDONLY = 0

MODE_WORLD_WRITABLE = stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO
MODE_STICKY_WORLD = MODE_WORLD_WRITABLE | stat.S_ISVTX

TEST_USER = "hsym"


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


def sys_open(path: str, flags: int, mode: int = 0) -> int:
    """open(path, flags[, mode]) via raw syscall (openat on aarch64)."""
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


def _get_test_user() -> pwd.struct_passwd:
    try:
        return pwd.getpwnam(TEST_USER)
    except KeyError as exc:
        raise OSError(
            errno.ENOENT,
            f"test user {TEST_USER!r} not found (LTP prot_hsymlinks creates it)",
        ) from exc


def _drop_to_user(uid: int, gid: int) -> None:
    ruid, _, suid = os.getresuid()
    rgid, _, sgid = os.getresgid()
    os.setgroups([])
    os.setresgid(rgid, gid, sgid)
    os.setresuid(ruid, uid, suid)


def _restore_effective_ids(orig_euid: int, orig_egid: int) -> None:
    ruid, _, suid = os.getresuid()
    rgid, _, sgid = os.getresgid()
    os.setresuid(ruid, orig_euid, suid)
    os.setresgid(rgid, orig_egid, sgid)


def _make_sticky_subdir(parent: str, name: str, owner_uid: int, owner_gid: int) -> str:
    path = os.path.join(parent, name)
    os.makedirs(path, MODE_WORLD_WRITABLE)
    os.chown(path, owner_uid, owner_gid)
    os.chmod(path, MODE_STICKY_WORLD)
    return path


def test_open_other_symlink_in_sticky_dir_rejects_dir_owner(root: str) -> int:
    # Related LTP cases: prot_hsymlinks
    # Sticky dir owner must not follow another user's symlink (fs.protected_symlinks).
    if os.geteuid() != 0:
        return skip(
            "open other symlink in sticky dir rejects dir owner (prot_hsymlinks)",
            "requires root",
        )

    try:
        hsym = _get_test_user()
    except OSError as exc:
        return skip(
            "open other symlink in sticky dir rejects dir owner (prot_hsymlinks)",
            str(exc),
        )

    workdir = case_dir(root, "prot_slink_root_sticky")
    target = os.path.join(workdir, "root.hs")
    with open(target, "wb"):
        pass

    sticky_root = _make_sticky_subdir(workdir, "tmp_root", 0, 0)
    link = os.path.join(sticky_root, "link")

    orig_euid = os.geteuid()
    orig_egid = os.getegid()
    try:
        _drop_to_user(hsym.pw_uid, hsym.pw_gid)
        os.symlink(target, link)
    finally:
        _restore_effective_ids(orig_euid, orig_egid)

    def run_root_open_other_symlink() -> None:
        fd = sys_open(link, O_RDONLY)
        os.close(fd)

    return expect_errno(
        "open(2) on others' symlink in root-owned sticky dir yields EACCES (prot_hsymlinks link_4)",
        run_root_open_other_symlink,
        errno.EACCES,
    )


def test_open_own_symlink_in_sticky_dir_succeeds_for_dir_owner(root: str) -> int:
    # Related LTP cases: prot_hsymlinks
    # Sticky dir owner may follow symlinks they own (fs.protected_symlinks exception).
    if os.geteuid() != 0:
        return skip(
            "open own symlink in sticky dir succeeds for dir owner (prot_hsymlinks)",
            "requires root",
        )

    try:
        hsym = _get_test_user()
    except OSError as exc:
        return skip(
            "open own symlink in sticky dir succeeds for dir owner (prot_hsymlinks)",
            str(exc),
        )

    workdir = case_dir(root, "prot_slink_hsym_sticky")
    target = os.path.join(workdir, "root.hs")
    with open(target, "wb"):
        pass

    sticky_hsym = _make_sticky_subdir(workdir, "tmp_hsym", hsym.pw_uid, hsym.pw_gid)
    link = os.path.join(sticky_hsym, "link")

    orig_euid = os.geteuid()
    orig_egid = os.getegid()
    try:
        _drop_to_user(hsym.pw_uid, hsym.pw_gid)
        os.symlink(target, link)
    finally:
        _restore_effective_ids(orig_euid, orig_egid)

    def run_hsym_open_own_symlink() -> None:
        _drop_to_user(hsym.pw_uid, hsym.pw_gid)
        try:
            fd = sys_open(link, O_RDONLY)
            os.close(fd)
        finally:
            _restore_effective_ids(orig_euid, orig_egid)

    return expect_ok(
        "open(2) on own symlink in hsym-owned sticky dir succeeds (prot_hsymlinks link_6)",
        run_hsym_open_own_symlink,
    )


TESTS: list[tuple[str, str, Callable[..., int]]] = [
    (
        "1",
        "sticky dir owner cannot open another user's symlink (prot_hsymlinks link_4)",
        test_open_other_symlink_in_sticky_dir_rejects_dir_owner,
    ),
    (
        "2",
        "sticky dir owner can open own symlink (prot_hsymlinks link_6)",
        test_open_own_symlink_in_sticky_dir_succeeds_for_dir_owner,
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="fs.protected_symlinks policy missing on Curvine FUSE — reproducer",
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

    print(f"Running protected-symlinks reproducer under {root}\n")

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
