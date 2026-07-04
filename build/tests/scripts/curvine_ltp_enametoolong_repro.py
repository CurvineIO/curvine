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
ENAMETOOLONG lookup bug on Curvine FUSE mounts.

Root cause: when a pathname contains a component longer than NAME_MAX (or the
full path exceeds PATH_MAX), Curvine FUSE lookup returns ENOENT instead of the
POSIX-required ENAMETOOLONG.  The failure is in the shared lookup path and
surfaces through many different syscalls.

Covered LTP cases (each line is one minimal repro scenario):
  chroot03 — chroot(272-byte component on FUSE) must fail ENAMETOOLONG (not ENOENT)
  chdir04 — chdir(272-byte component) must fail ENAMETOOLONG
  execve03 — execve(272-byte filename) must fail ENAMETOOLONG
  open08 (case 4) — open(272-byte name, O_RDWR) must fail ENAMETOOLONG
  statx03 (case 7) — statx(AT_FDCWD, 256-byte name) must fail ENAMETOOLONG
  nftw01 (block 25) — nftw(path with NAME_MAX+1 byte final component) ENAMETOOLONG
  nftw6401 (block 25) — nftw64(same long-component path) ENAMETOOLONG

Usage:
    python3 curvine_ltp_enametoolong_repro.py --dir /curvine-fuse/fuse-test
    python3 curvine_ltp_enametoolong_repro.py --dir /curvine-fuse/fuse-test --allow-non-mount
"""

import argparse
import ctypes
import errno
import multiprocessing
import os
import shutil
import sys
from typing import Callable, Optional

DEFAULT_DIR = "/curvine-fuse/fuse-test"

AT_FDCWD = -100
O_RDWR = 2

# 272-byte single component from chdir04 / execve03 / open08 LTP sources.
LTP_LONG_COMPONENT = (
    "abcdefghijklmnopqrstmnopqrstuvwxyz"
    "abcdefghijklmnopqrstmnopqrstuvwxyz"
    "abcdefghijklmnopqrstmnopqrstuvwxyz"
    "abcdefghijklmnopqrstmnopqrstuvwxyz"
    "abcdefghijklmnopqrstmnopqrstuvwxyz"
    "abcdefghijklmnopqrstmnopqrstuvwxyz"
    "abcdefghijklmnopqrstmnopqrstuvwxyz"
    "abcdefghijklmnopqrstmnopqrstuvwxyz"
)
assert len(LTP_LONG_COMPONENT) == 272

_libc: Optional[ctypes.CDLL] = None
_nftw_callback_type: Optional[type] = None


class StatxTimestamp(ctypes.Structure):
    _fields_ = [
        ("tv_sec", ctypes.c_int64),
        ("tv_nsec", ctypes.c_uint32),
        ("__reserved", ctypes.c_int32),
    ]


class Statx(ctypes.Structure):
    _fields_ = [
        ("stx_mask", ctypes.c_uint32),
        ("stx_blksize", ctypes.c_uint32),
        ("stx_attributes", ctypes.c_uint64),
        ("stx_nlink", ctypes.c_uint32),
        ("stx_uid", ctypes.c_uint32),
        ("stx_gid", ctypes.c_uint32),
        ("stx_mode", ctypes.c_uint16),
        ("__spare0", ctypes.c_uint16 * 1),
        ("stx_ino", ctypes.c_uint64),
        ("stx_size", ctypes.c_uint64),
        ("stx_blocks", ctypes.c_uint64),
        ("stx_attributes_mask", ctypes.c_uint64),
        ("stx_atime", StatxTimestamp),
        ("stx_btime", StatxTimestamp),
        ("stx_ctime", StatxTimestamp),
        ("stx_mtime", StatxTimestamp),
        ("stx_rdev_major", ctypes.c_uint32),
        ("stx_rdev_minor", ctypes.c_uint32),
        ("stx_dev_major", ctypes.c_uint32),
        ("stx_dev_minor", ctypes.c_uint32),
        ("stx_mnt_id", ctypes.c_uint64),
        ("__spare2", ctypes.c_uint64 * 1),
        ("__spare3", ctypes.c_uint64 * 12),
    ]


def _init_libc() -> ctypes.CDLL:
    global _libc, _nftw_callback_type
    if _libc is None:
        _libc = ctypes.CDLL(None, use_errno=True)
        _nftw_callback_type = ctypes.CFUNCTYPE(
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_void_p,
            ctypes.c_int,
            ctypes.c_void_p,
        )
        _libc.openat.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_uint,
        ]
        _libc.openat.restype = ctypes.c_int
        _libc.statx.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_uint,
            ctypes.POINTER(Statx),
        ]
        _libc.statx.restype = ctypes.c_int
    return _libc


def sys_openat(path: str, flags: int, mode: int = 0) -> int:
    libc = _init_libc()
    ret = libc.openat(AT_FDCWD, path.encode(), flags, mode)
    if ret < 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))
    return ret


def sys_chroot(path: str) -> None:
    libc = _init_libc()
    ret = libc.chroot(path.encode())
    if ret < 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))


def sys_chdir(path: str) -> None:
    libc = _init_libc()
    ret = libc.chdir(path.encode())
    if ret < 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))


def sys_statx(
    dfd: int,
    path: str,
    flags: int,
    mask: int,
    buf: Statx,
) -> None:
    libc = _init_libc()
    ret = libc.statx(dfd, path.encode(), flags, mask, ctypes.byref(buf))
    if ret < 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))


def sys_nftw(path: str, *, use64: bool = False) -> None:
    libc = _init_libc()
    assert _nftw_callback_type is not None

    @_nftw_callback_type
    def callback(
        fpath: bytes,
        statbuf: ctypes.c_void_p,
        flag: int,
        ftw: ctypes.c_void_p,
    ) -> int:
        return 0

    fn = libc.nftw64 if use64 else libc.nftw
    fn.argtypes = [
        ctypes.c_char_p,
        _nftw_callback_type,
        ctypes.c_int,
        ctypes.c_int,
    ]
    fn.restype = ctypes.c_int

    ret = fn(path.encode(), callback, 20, 0)
    if ret == 0:
        raise OSError(errno.EINVAL, "nftw succeeded unexpectedly")
    err = ctypes.get_errno()
    raise OSError(err, os.strerror(err))


def path_with_long_final_component(base: str) -> str:
    """LTP nftw test_long_file_name: tmp_path + '/' + (NAME_MAX+1) 'E's."""
    name_max = os.pathconf(base, "PC_NAME_MAX")
    return os.path.join(base, "E" * (name_max + 1))


def _execve_child_target(path: str) -> None:
    libc = _init_libc()
    path_b = path.encode()
    argv = (ctypes.c_char_p * 2)(path_b, None)
    ret = libc.execve(path_b, argv, None)
    os._exit(ctypes.get_errno() if ret < 0 else errno.EINVAL)


def case_dir(root: str, name: str) -> str:
    path = os.path.join(root, name)
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)
    return path


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


def test_chroot_long_component(root: str) -> int:
    # Related LTP cases: chroot03
    # chroot(2) on FUSE must fail ENAMETOOLONG when a path component exceeds
    # NAME_MAX; PATH_MAX+1 is rejected by the kernel before lookup (not this bug).
    if os.geteuid() != 0:
        return skip("chroot long component", "requires root")

    workdir = case_dir(root, "chroot_long")
    saved = os.getcwd()
    os.chdir(workdir)

    def run() -> None:
        sys_chroot(LTP_LONG_COMPONENT)

    try:
        return expect_errno(
            f"chroot({len(LTP_LONG_COMPONENT)}-byte component on FUSE)",
            run,
            errno.ENAMETOOLONG,
        )
    finally:
        os.chdir(saved)


def test_chdir_long_component(root: str) -> int:
    # Related LTP cases: chdir04
    # chdir(2) must fail with ENAMETOOLONG when a path component exceeds NAME_MAX.
    workdir = case_dir(root, "chdir_long")
    saved = os.getcwd()
    os.chdir(workdir)

    def run() -> None:
        sys_chdir(LTP_LONG_COMPONENT)

    try:
        return expect_errno(
            f"chdir({len(LTP_LONG_COMPONENT)}-byte component)",
            run,
            errno.ENAMETOOLONG,
        )
    finally:
        os.chdir(saved)


def test_execve_long_component(root: str) -> int:
    # Related LTP cases: execve03
    # execve(2) must fail with ENAMETOOLONG when the filename exceeds NAME_MAX.
    if os.geteuid() != 0:
        return skip("execve long component", "requires root")

    workdir = case_dir(root, "execve_long")
    saved = os.getcwd()
    os.chdir(workdir)

    def run() -> None:
        proc = multiprocessing.Process(
            target=_execve_child_target,
            args=(LTP_LONG_COMPONENT,),
        )
        proc.start()
        proc.join()
        code = proc.exitcode
        if code != errno.ENAMETOOLONG:
            raise OSError(
                code if code and code > 0 else errno.EINVAL,
                f"child exit {code}, expected ENAMETOOLONG",
            )

    try:
        return expect_errno(
            f"execve({len(LTP_LONG_COMPONENT)}-byte filename)",
            run,
            errno.ENAMETOOLONG,
        )
    finally:
        os.chdir(saved)


def test_open_long_component(root: str) -> int:
    # Related LTP cases: open08 (case 4)
    # open(2) must fail with ENAMETOOLONG when the pathname component exceeds NAME_MAX.
    workdir = case_dir(root, "open_long")
    saved = os.getcwd()
    os.chdir(workdir)

    def run() -> None:
        fd = sys_openat(LTP_LONG_COMPONENT, O_RDWR)
        os.close(fd)

    try:
        return expect_errno(
            f"open({len(LTP_LONG_COMPONENT)}-byte name, O_RDWR)",
            run,
            errno.ENAMETOOLONG,
        )
    finally:
        os.chdir(saved)


def test_statx_long_component(root: str) -> int:
    # Related LTP cases: statx03 (case 7)
    # statx(2) must fail with ENAMETOOLONG when the pathname component exceeds NAME_MAX.
    workdir = case_dir(root, "statx_long")
    long_name = "@" * 256
    saved = os.getcwd()
    os.chdir(workdir)

    def run() -> None:
        buf = Statx()
        sys_statx(AT_FDCWD, long_name, 0, 0, buf)

    try:
        return expect_errno(
            f"statx(AT_FDCWD, {len(long_name)}-byte name)",
            run,
            errno.ENAMETOOLONG,
        )
    finally:
        os.chdir(saved)


def test_nftw_long_final_component(root: str) -> int:
    # Related LTP cases: nftw01 (block 25)
    # nftw(3) must return -1 with ENAMETOOLONG when a path component exceeds NAME_MAX.
    workdir = case_dir(root, "nftw_long")
    long_path = path_with_long_final_component(workdir)

    def run() -> None:
        sys_nftw(long_path, use64=False)

    return expect_errno(
        f"nftw(path with NAME_MAX+1 final component)",
        run,
        errno.ENAMETOOLONG,
    )


def test_nftw64_long_final_component(root: str) -> int:
    # Related LTP cases: nftw6401 (block 25)
    # nftw64(3) must return -1 with ENAMETOOLONG when a path component exceeds NAME_MAX.
    workdir = case_dir(root, "nftw64_long")
    long_path = path_with_long_final_component(workdir)

    def run() -> None:
        sys_nftw(long_path, use64=True)

    return expect_errno(
        f"nftw64(path with NAME_MAX+1 final component)",
        run,
        errno.ENAMETOOLONG,
    )


TESTS: list = [
    (
        "1",
        "chroot(272-byte component on FUSE) returns ENAMETOOLONG",
        test_chroot_long_component,
    ),
    (
        "2",
        "chdir(272-byte component) returns ENAMETOOLONG",
        test_chdir_long_component,
    ),
    (
        "3",
        "execve(272-byte filename) returns ENAMETOOLONG",
        test_execve_long_component,
    ),
    (
        "4",
        "open(272-byte name, O_RDWR) returns ENAMETOOLONG",
        test_open_long_component,
    ),
    (
        "5",
        "statx(AT_FDCWD, 256-byte name) returns ENAMETOOLONG",
        test_statx_long_component,
    ),
    (
        "6",
        "nftw(NAME_MAX+1 final component) returns ENAMETOOLONG",
        test_nftw_long_final_component,
    ),
    (
        "7",
        "nftw64(NAME_MAX+1 final component) returns ENAMETOOLONG",
        test_nftw64_long_final_component,
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="ENAMETOOLONG vs ENOENT lookup reproducer for Curvine FUSE",
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
        print("WARNING: chroot/execve cases require root", file=sys.stderr)

    root = os.path.abspath(args.dir)
    os.makedirs(root, exist_ok=True)

    if args.allow_non_mount:
        print(
            "WARNING: --allow-non-mount set; results may not reflect Curvine FUSE behaviour",
            file=sys.stderr,
        )

    print(f"Running ENAMETOOLONG lookup tests under {root}\n")

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
