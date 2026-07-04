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
security.*/system.* extended attribute namespace unsupported on Curvine FUSE.

Root cause: security.* namespace xattrs (e.g. security.ltptest1/2) set via
setxattr(2)/fsetxattr(2)/lsetxattr(2) do not appear in listxattr(2)/
flistxattr(2)/llistxattr(2) results.  FS_IOC_SETFLAGS for immutable/append-only
flags returns ENOSYS during setxattr03 setup.

Covered LTP cases (each line is one minimal repro scenario):
  listxattr01   — setxattr(security.ltptest1) then listxattr must list that name (TFAIL missing)
  flistxattr01  — fsetxattr(security.ltptest1) then flistxattr must list that name (TFAIL missing)
  llistxattr01  — lsetxattr on symlink(security.ltptest2) then llistxattr must list it (TFAIL missing)
  setxattr03    — FS_IOC_SETFLAGS(FS_IMMUTABLE_FL) in setup must succeed (TBROK ENOSYS)

Usage:
    python3 curvine_ltp_security_system_xattr_repro.py --dir /curvine-fuse/fuse-test
    python3 curvine_ltp_security_system_xattr_repro.py --dir /curvine-fuse/fuse-test --allow-non-mount
"""

import argparse
import ctypes
import errno
import os
import platform
import shutil
import sys
from typing import Callable

DEFAULT_DIR = "/curvine-fuse/fuse-test"

XATTR_CREATE = 0x1

SECURITY_KEY1 = "security.ltptest1"
SECURITY_KEY2 = "security.ltptest2"
VALUE = b"test"

FS_IOC_GETFLAGS = 0x80086601
FS_IOC_SETFLAGS = 0x40086602
FS_IMMUTABLE_FL = 0x00000010


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


def _syscall_nr(name: str) -> int:
    table = {
        "setxattr": {"x86_64": 188, "aarch64": 5},
        "lsetxattr": {"x86_64": 189, "aarch64": 6},
        "fsetxattr": {"x86_64": 190, "aarch64": 7},
        "listxattr": {"x86_64": 194, "aarch64": 11},
        "llistxattr": {"x86_64": 195, "aarch64": 12},
        "flistxattr": {"x86_64": 196, "aarch64": 13},
    }
    machine = platform.machine()
    if name not in table or machine not in table[name]:
        raise OSError(
            errno.ENOSYS,
            f"unsupported architecture for {name}: {machine}",
        )
    return table[name][machine]


def _raw_syscall(nr: int, *args: int) -> int:
    libc = ctypes.CDLL(None, use_errno=True)
    syscall_fn = libc.syscall
    syscall_fn.restype = ctypes.c_long
    ret = int(syscall_fn(nr, *args))
    if ret < 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))
    return ret


def _ioctl(fd: int, request: int, arg: ctypes.c_void_p) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    ioctl_fn = libc.ioctl
    ioctl_fn.restype = ctypes.c_int
    ret = int(ioctl_fn(fd, request, arg))
    if ret < 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))


def sys_setxattr(path: str, name: str, value: bytes, flags: int = 0) -> None:
    """setxattr(path, name, value, size, flags) via raw syscall."""
    _raw_syscall(
        _syscall_nr("setxattr"),
        ctypes.c_char_p(path.encode()),
        ctypes.c_char_p(name.encode()),
        ctypes.c_char_p(value),
        len(value),
        flags,
    )


def sys_lsetxattr(path: str, name: str, value: bytes, flags: int = 0) -> None:
    """lsetxattr(path, name, value, size, flags) via raw syscall."""
    _raw_syscall(
        _syscall_nr("lsetxattr"),
        ctypes.c_char_p(path.encode()),
        ctypes.c_char_p(name.encode()),
        ctypes.c_char_p(value),
        len(value),
        flags,
    )


def sys_fsetxattr(fd: int, name: str, value: bytes, flags: int = 0) -> None:
    """fsetxattr(fd, name, value, size, flags) via raw syscall."""
    _raw_syscall(
        _syscall_nr("fsetxattr"),
        fd,
        ctypes.c_char_p(name.encode()),
        ctypes.c_char_p(value),
        len(value),
        flags,
    )


def sys_listxattr(path: str, buf: ctypes.Array, size: int) -> int:
    """listxattr(path, list, size) via raw syscall."""
    return _raw_syscall(
        _syscall_nr("listxattr"),
        ctypes.c_char_p(path.encode()),
        ctypes.cast(buf, ctypes.c_void_p),
        size,
    )


def sys_llistxattr(path: str, buf: ctypes.Array, size: int) -> int:
    """llistxattr(path, list, size) via raw syscall."""
    return _raw_syscall(
        _syscall_nr("llistxattr"),
        ctypes.c_char_p(path.encode()),
        ctypes.cast(buf, ctypes.c_void_p),
        size,
    )


def sys_flistxattr(fd: int, buf: ctypes.Array, size: int) -> int:
    """flistxattr(fd, list, size) via raw syscall."""
    return _raw_syscall(
        _syscall_nr("flistxattr"),
        fd,
        ctypes.cast(buf, ctypes.c_void_p),
        size,
    )


def _xattr_names_from_buffer(buf: bytes, listed: int) -> list[str]:
    names: list[str] = []
    i = 0
    while i < listed:
        end = buf.find(b"\x00", i, listed)
        if end == -1:
            break
        if end > i:
            names.append(buf[i:end].decode())
        i = end + 1
    return names


def test_listxattr_includes_security_xattr(root: str) -> int:
    # Related LTP cases: listxattr01
    # listxattr(2) must include security.* names set via setxattr(2) on the file.
    if os.geteuid() != 0:
        return skip("listxattr lists security xattr", "requires root")

    workdir = case_dir(root, "listxattr_security")
    target = os.path.join(workdir, "testfile")
    fd = os.open(target, os.O_CREAT | os.O_RDWR, 0o644)
    os.close(fd)
    sys_setxattr(target, SECURITY_KEY1, VALUE, flags=XATTR_CREATE)

    def run() -> None:
        buf = ctypes.create_string_buffer(128)
        ret = sys_listxattr(target, buf, 128)
        names = _xattr_names_from_buffer(bytes(buf[:ret]), ret)
        if SECURITY_KEY1 not in names:
            raise OSError(
                errno.ENODATA,
                f"missing attribute {SECURITY_KEY1} in listxattr result {names}",
            )

    return expect_ok(
        "setxattr(security.ltptest1) then listxattr lists that name (listxattr01)",
        run,
    )


def test_flistxattr_includes_security_xattr(root: str) -> int:
    # Related LTP cases: flistxattr01
    # flistxattr(2) must include security.* names set via fsetxattr(2) on the fd.
    if os.geteuid() != 0:
        return skip("flistxattr lists security xattr", "requires root")

    workdir = case_dir(root, "flistxattr_security")
    target = os.path.join(workdir, "testfile")
    fd = os.open(target, os.O_CREAT | os.O_RDWR, 0o644)
    sys_fsetxattr(fd, SECURITY_KEY1, VALUE, flags=XATTR_CREATE)

    def run() -> None:
        buf = ctypes.create_string_buffer(128)
        ret = sys_flistxattr(fd, buf, 128)
        names = _xattr_names_from_buffer(bytes(buf[:ret]), ret)
        if SECURITY_KEY1 not in names:
            raise OSError(
                errno.ENODATA,
                f"missing attribute {SECURITY_KEY1} in flistxattr result {names}",
            )

    try:
        return expect_ok(
            "fsetxattr(security.ltptest1) then flistxattr lists that name (flistxattr01)",
            run,
        )
    finally:
        os.close(fd)


def test_llistxattr_includes_symlink_security_xattr(root: str) -> int:
    # Related LTP cases: llistxattr01
    # llistxattr(2) on a symlink must list security.* set on the link via lsetxattr(2).
    if os.geteuid() != 0:
        return skip("llistxattr lists symlink security xattr", "requires root")

    workdir = case_dir(root, "llistxattr_security")
    testfile = os.path.join(workdir, "testfile")
    symlink = os.path.join(workdir, "symlink")
    fd = os.open(testfile, os.O_CREAT | os.O_RDWR, 0o644)
    os.close(fd)
    os.symlink("testfile", symlink)
    sys_lsetxattr(testfile, SECURITY_KEY1, VALUE, flags=XATTR_CREATE)
    sys_lsetxattr(symlink, SECURITY_KEY2, VALUE, flags=XATTR_CREATE)

    def run() -> None:
        buf = ctypes.create_string_buffer(128)
        ret = sys_llistxattr(symlink, buf, 128)
        names = _xattr_names_from_buffer(bytes(buf[:ret]), ret)
        if SECURITY_KEY2 not in names:
            raise OSError(
                errno.ENODATA,
                f"missing attribute {SECURITY_KEY2} in llistxattr result {names}",
            )

    return expect_ok(
        "lsetxattr on symlink(security.ltptest2) then llistxattr lists it (llistxattr01)",
        run,
    )


def test_fs_ioc_setflags_immutable_supported(root: str) -> int:
    # Related LTP cases: setxattr03
    # FS_IOC_SETFLAGS(2) with FS_IMMUTABLE_FL must succeed during immutable setup.
    if os.geteuid() != 0:
        return skip("FS_IOC_SETFLAGS immutable", "requires root")

    workdir = case_dir(root, "setxattr03_immutable_setup")
    target = os.path.join(workdir, "setxattr03immutable")
    fd = os.open(target, os.O_CREAT | os.O_RDWR, 0o644)

    def run() -> None:
        flags = ctypes.c_int(0)
        _ioctl(fd, FS_IOC_GETFLAGS, ctypes.byref(flags))
        flags.value |= FS_IMMUTABLE_FL
        _ioctl(fd, FS_IOC_SETFLAGS, ctypes.byref(flags))

    try:
        return expect_ok(
            "FS_IOC_SETFLAGS(FS_IMMUTABLE_FL) succeeds on FUSE file (setxattr03 setup)",
            run,
        )
    finally:
        os.close(fd)


TESTS: list[tuple[str, str, Callable[..., int]]] = [
    (
        "1",
        "listxattr lists security.ltptest1 after setxattr (listxattr01)",
        test_listxattr_includes_security_xattr,
    ),
    (
        "2",
        "flistxattr lists security.ltptest1 after fsetxattr (flistxattr01)",
        test_flistxattr_includes_security_xattr,
    ),
    (
        "3",
        "llistxattr lists security.ltptest2 on symlink after lsetxattr (llistxattr01)",
        test_llistxattr_includes_symlink_security_xattr,
    ),
    (
        "4",
        "FS_IOC_SETFLAGS immutable flag supported (setxattr03 setup)",
        test_fs_ioc_setflags_immutable_supported,
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="security.*/system.* xattr namespace unsupported — reproducer",
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

    print(f"Running security-system-xattr reproducer under {root}\n")

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
